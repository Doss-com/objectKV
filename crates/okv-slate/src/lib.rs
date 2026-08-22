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
const USER_KEY_PREFIX: u8 = 1;
const FINGERPRINT_VERSION: &[u8] = b"okv-commit-v1";

/// Errors produced at the objectKV to `SlateDB` boundary.
#[derive(Debug, Eq, PartialEq)]
pub enum AdapterError {
    Backend(String),
    ConflictingReplay { version: Version },
    NonMonotonicVersion { latest: Version, attempted: Version },
    ZeroVersion,
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(message) => write!(formatter, "SlateDB error: {message}"),
            Self::ConflictingReplay { version } => write!(
                formatter,
                "version {} was replayed with different mutations",
                version.get()
            ),
            Self::NonMonotonicVersion { latest, attempted } => write!(
                formatter,
                "version {} cannot follow version {}",
                attempted.get(),
                latest.get()
            ),
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

        let commit_key = commit_key(batch.version);
        let fingerprint = fingerprint(&batch);
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
            }
        }
        write_batch.put(&commit_key, &fingerprint);

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
                Err(AdapterError::Backend(error.to_string()))
            }
        }
    }

    /// Return the latest applied objectKV version.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SlateDB` cannot create a snapshot.
    pub async fn latest_version(&self) -> Result<Version, AdapterError> {
        self.db
            .snapshot()
            .await
            .map(|snapshot| Version::new(snapshot.seq()))
            .map_err(|error| backend(&error))
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
    let mut key = Vec::with_capacity(COMMIT_KEY_PREFIX.len() + 8);
    key.extend_from_slice(COMMIT_KEY_PREFIX);
    key.extend_from_slice(&version.get().to_be_bytes());
    key
}

fn user_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(USER_KEY_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}

fn fingerprint(batch: &CommitBatch) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(FINGERPRINT_VERSION);
    push_len(&mut encoded, batch.mutations.len());
    for mutation in &batch.mutations {
        match mutation {
            Mutation::Set { key, value } => {
                encoded.push(1);
                push_bytes(&mut encoded, key);
                push_bytes(&mut encoded, value);
            }
            Mutation::Clear { key } => {
                encoded.push(2);
                push_bytes(&mut encoded, key);
            }
        }
    }
    encoded
}

fn push_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    push_len(target, bytes.len());
    target.extend_from_slice(bytes);
}

fn push_len(target: &mut Vec<u8>, len: usize) {
    let len = u64::try_from(len).expect("usize always fits in u64 on supported targets");
    target.extend_from_slice(&len.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::{AdapterError, SlateEngine, SLATEDB_REVISION};
    use okv_model::{ApplyOutcome, CommitBatch, Mutation, Version};
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

    #[test]
    fn pins_the_observed_revision() {
        assert_eq!(SLATEDB_REVISION.len(), 40);
    }
}
