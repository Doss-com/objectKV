//! Pinned `SlateDB` adaptation layer for objectKV.
//!
//! This crate starts with the externally versioned write seam. Explicit reads
//! at a caller-selected version remain blocked on a small upstream `SlateDB` API.

use okv_model::{ApplyOutcome, CommitBatch, Mutation, Version};
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};
use std::error::Error;
use std::fmt::{Display, Formatter};

/// `SlateDB` source revision compiled by this adapter.
pub const SLATEDB_REVISION: &str = "e0161973d8d7ffdede7c44725729838811674e99";

const COMMIT_KEY_PREFIX: &[u8] = b"\x00okv/commit/";
const LATEST_VERSION_KEY: &[u8] = b"\x00okv/latest-version";
const USER_KEY_PREFIX: u8 = 1;

/// Errors produced at the objectKV to `SlateDB` boundary.
#[derive(Debug, Eq, PartialEq)]
pub enum AdapterError {
    Backend(String),
    ConflictingReplay { version: Version },
    InvalidBatch(String),
    NonMonotonicVersion { latest: Version, attempted: Version },
    UnsupportedGeneration { generation: u64 },
    UnsupportedRangeClear,
    ZeroVersion,
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(message) => write!(formatter, "SlateDB error: {message}"),
            Self::ConflictingReplay { version } => write!(
                formatter,
                "version {version} was replayed with different mutations"
            ),
            Self::InvalidBatch(message) => write!(formatter, "invalid commit batch: {message}"),
            Self::NonMonotonicVersion { latest, attempted } => write!(
                formatter,
                "version {attempted} cannot follow version {latest}"
            ),
            Self::UnsupportedGeneration { generation } => write!(
                formatter,
                "SlateDB adapter does not yet support logical generation {generation}"
            ),
            Self::UnsupportedRangeClear => {
                formatter.write_str("SlateDB adapter does not yet support atomic range clear")
            }
            Self::ZeroVersion => formatter.write_str("version zero is reserved"),
        }
    }
}

impl Error for AdapterError {}

/// A `SlateDB` instance using objectKV's commit-version and replay contract.
pub struct SlateEngine {
    db: Db,
}

impl SlateEngine {
    #[must_use]
    pub const fn new(db: Db) -> Self {
        Self { db }
    }

    /// Apply one atomic commit at its caller-assigned version.
    ///
    /// A private commit record makes exact replay idempotent even after later
    /// commits. User keys are namespaced so commit records cannot collide with
    /// application data.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for zero/non-monotonic versions, conflicting
    /// replay, or a `SlateDB` failure.
    pub async fn apply(&self, batch: CommitBatch) -> Result<ApplyOutcome, AdapterError> {
        if batch.version == Version::ZERO {
            return Err(AdapterError::ZeroVersion);
        }
        if batch.version.generation() != 0 {
            return Err(AdapterError::UnsupportedGeneration {
                generation: batch.version.generation(),
            });
        }
        if batch
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, Mutation::ClearRange { .. }))
        {
            return Err(AdapterError::UnsupportedRangeClear);
        }

        let commit_key = commit_key(batch.version);
        let fingerprint = batch
            .fingerprint()
            .map_err(|error| AdapterError::InvalidBatch(error.to_string()))?;
        if let Some(outcome) = self
            .replay_outcome(&commit_key, &fingerprint, batch.version)
            .await?
        {
            return outcome;
        }

        let latest = self.latest_version().await?;
        if batch.version <= latest {
            return Err(AdapterError::NonMonotonicVersion {
                latest,
                attempted: batch.version,
            });
        }

        let mut write_batch = WriteBatch::new();
        for mutation in batch.mutations {
            match mutation {
                Mutation::Set { key, value } => write_batch.put(user_key(&key), value),
                Mutation::Clear { key } => write_batch.delete(user_key(&key)),
                Mutation::ClearRange { .. } => unreachable!("rejected before write construction"),
            }
        }
        write_batch.put(&commit_key, fingerprint);
        write_batch.put(LATEST_VERSION_KEY, batch.version.to_be_bytes());

        let options = WriteOptions {
            seqnum: batch.version.get(),
        };
        match self.db.write_with_options(write_batch, &options).await {
            Ok(handle) => {
                debug_assert_eq!(handle.seqnum(), batch.version.get());
                Ok(ApplyOutcome::Applied)
            }
            Err(error) => {
                // A concurrent writer may have won the same version between
                // the replay check and the serialized SlateDB write.
                if let Some(outcome) = self
                    .replay_outcome(&commit_key, &fingerprint, batch.version)
                    .await?
                {
                    return outcome;
                }
                let latest = self.latest_version().await?;
                if batch.version <= latest {
                    return Err(AdapterError::NonMonotonicVersion {
                        latest,
                        attempted: batch.version,
                    });
                }
                Err(AdapterError::Backend(error.to_string()))
            }
        }
    }

    /// Return the latest applied objectKV version.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the private version record is malformed or
    /// `SlateDB` cannot serve the read.
    pub async fn latest_version(&self) -> Result<Version, AdapterError> {
        let encoded = self
            .db
            .get(LATEST_VERSION_KEY)
            .await
            .map_err(|error| backend(&error))?;
        let Some(encoded) = encoded else {
            return Ok(Version::ZERO);
        };
        let encoded: [u8; 16] = encoded.as_ref().try_into().map_err(|_| {
            AdapterError::Backend("private latest-version record is not 16 bytes".to_owned())
        })?;
        Ok(Version::from_be_bytes(encoded))
    }

    /// Read the latest value for a user key.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SlateDB` cannot serve the read.
    pub async fn get_latest(&self, key: &[u8]) -> Result<Option<Vec<u8>>, AdapterError> {
        self.db
            .get(user_key(key))
            .await
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(|error| backend(&error))
    }

    /// Close the underlying `SlateDB` instance.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SlateDB` cannot close cleanly.
    pub async fn close(&self) -> Result<(), AdapterError> {
        self.db.close().await.map_err(|error| backend(&error))
    }

    async fn replay_outcome(
        &self,
        commit_key: &[u8],
        fingerprint: &[u8],
        version: Version,
    ) -> Result<Option<Result<ApplyOutcome, AdapterError>>, AdapterError> {
        let existing = self
            .db
            .get(commit_key)
            .await
            .map_err(|error| backend(&error))?;
        Ok(existing.map(|existing| {
            if existing.as_ref() == fingerprint {
                Ok(ApplyOutcome::AlreadyApplied)
            } else {
                Err(AdapterError::ConflictingReplay { version })
            }
        }))
    }
}

fn backend(error: &slatedb::Error) -> AdapterError {
    AdapterError::Backend(error.to_string())
}

fn commit_key(version: Version) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_KEY_PREFIX.len() + 16);
    key.extend_from_slice(COMMIT_KEY_PREFIX);
    key.extend_from_slice(&version.to_be_bytes());
    key
}

fn user_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(USER_KEY_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}

#[cfg(test)]
mod tests {
    use super::{AdapterError, SlateEngine, SLATEDB_REVISION};
    use okv_model::{ApplyOutcome, CommitBatch, CommitIdentity, KeyRange, Mutation, Version};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;
    use slatedb::Db;
    use std::sync::Arc;

    async fn engine(name: &str) -> SlateEngine {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open(format!("okv-slate/{name}"), store)
            .await
            .expect("open SlateDB");
        SlateEngine::new(db)
    }

    fn set(version: u64, key: &[u8], value: &[u8]) -> CommitBatch {
        CommitBatch {
            version: Version::new(version),
            identity: CommitIdentity::for_test(version),
            mutations: vec![Mutation::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }
    }

    #[tokio::test]
    async fn applies_externally_assigned_versions_with_gaps() {
        let engine = engine("external-version").await;

        assert_eq!(
            engine.apply(set(42, b"k", b"v")).await,
            Ok(ApplyOutcome::Applied)
        );
        assert_eq!(engine.latest_version().await, Ok(Version::new(42)));
        assert_eq!(engine.get_latest(b"k").await, Ok(Some(b"v".to_vec())));

        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn accepts_exact_replay_after_later_commits() {
        let engine = engine("exact-replay").await;
        let first = set(10, b"k", b"one");

        assert_eq!(engine.apply(first.clone()).await, Ok(ApplyOutcome::Applied));
        assert_eq!(
            engine.apply(set(20, b"k", b"two")).await,
            Ok(ApplyOutcome::Applied)
        );
        assert_eq!(engine.apply(first).await, Ok(ApplyOutcome::AlreadyApplied));

        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn rejects_conflicting_replay() {
        let engine = engine("conflicting-replay").await;

        engine
            .apply(set(10, b"k", b"one"))
            .await
            .expect("initial apply");
        assert_eq!(
            engine.apply(set(10, b"k", b"different")).await,
            Err(AdapterError::ConflictingReplay {
                version: Version::new(10)
            })
        );

        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn rejects_unknown_non_monotonic_version() {
        let engine = engine("non-monotonic").await;

        engine
            .apply(set(10, b"k", b"one"))
            .await
            .expect("initial apply");
        assert_eq!(
            engine.apply(set(5, b"other", b"value")).await,
            Err(AdapterError::NonMonotonicVersion {
                latest: Version::new(10),
                attempted: Version::new(5)
            })
        );

        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn rejects_unimplemented_logical_generation_and_range_clear() {
        let engine = engine("unsupported-contract").await;
        let mut generated = set(1, b"k", b"v");
        generated.version = Version::from_parts(1, 1);
        assert_eq!(
            engine.apply(generated).await,
            Err(AdapterError::UnsupportedGeneration { generation: 1 })
        );
        assert_eq!(
            engine
                .apply(CommitBatch {
                    version: Version::new(1),
                    identity: CommitIdentity::for_test(1),
                    mutations: vec![Mutation::ClearRange {
                        range: KeyRange::new(b"a".to_vec(), b"z".to_vec()).expect("range"),
                    }],
                })
                .await,
            Err(AdapterError::UnsupportedRangeClear)
        );
        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn concurrent_lower_version_cannot_replace_higher_version() {
        let engine = engine("concurrent-monotonic").await;
        let (high, low) = tokio::join!(
            engine.apply(set(20, b"high", b"twenty")),
            engine.apply(set(10, b"low", b"ten"))
        );
        assert_eq!(high, Ok(ApplyOutcome::Applied));
        assert!(
            low == Ok(ApplyOutcome::Applied)
                || low
                    == Err(AdapterError::NonMonotonicVersion {
                        latest: Version::new(20),
                        attempted: Version::new(10),
                    })
        );
        assert_eq!(engine.latest_version().await, Ok(Version::new(20)));
        engine.close().await.expect("close SlateDB");
    }

    #[test]
    fn pins_the_observed_revision() {
        assert_eq!(SLATEDB_REVISION.len(), 40);
    }
}
