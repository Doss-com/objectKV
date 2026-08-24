//! Pinned `SlateDB` adaptation layer for objectKV.
//!
//! The adapter applies externally versioned writes and owns an MVCC physical
//! key encoding for exact point and ordered range reads at a caller-selected
//! version. Range tombstones and history collection remain explicit gaps.

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use okv_model::{ApplyOutcome, CommitBatch, Mutation, Row, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::config::{DbReaderOptions, WriteOptions};
use slatedb::db_cache::DbCache;
use slatedb::{Db, DbIterator, DbReader, DbReaderMode, WriteBatch};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

mod kv_runtime_density;
mod manifest_bound_store;
mod mvcc_gc_curve;
mod mvcc_retention;
mod phase0;
mod snapshot_read_curve;

pub use kv_runtime_density::{
    run_kv_runtime_density_worker, KvRuntimeDensityMode, KvRuntimeDensityReceipt,
    KvRuntimeDensityResourceSample, KvRuntimeDensityTopology, KvRuntimeDensityWorkerConfig,
};
pub use mvcc_gc_curve::{
    inspect_latest_physical_manifest, run_authorized_mvcc_gc_curve_worker,
    run_authorized_mvcc_gc_curve_worker_at_root, run_mvcc_gc_curve_worker,
    verify_physical_manifest_on_local_root, MvccGcAuthorizedCurveReceipt,
    MvccGcCollectionAuthorization, MvccGcCollectionRequest, MvccGcCurveConfig, MvccGcCurveMode,
    MvccGcCurveReceipt, MvccGcPhysicalManifestReceipt, MvccGcPhysicalObjectReceipt,
};
pub use mvcc_retention::{
    MvccHistoryFilterMode, MvccHistoryFilterStatsSnapshot, MvccHistoryFilterSupplier,
    MvccRetentionFloor,
};

pub use phase0::{
    run_phase0_compaction_contract, run_phase0_compaction_coordinator_process_node,
    run_phase0_compaction_reclaim_contract, run_phase0_compaction_worker_process_node,
    run_phase0_coordinator_fencing_contract, run_phase0_coordinator_recovery_contract,
    run_phase0_filesystem_contract, run_phase0_minio_compaction_contract,
    run_phase0_orphan_gc_contract, CountingStore, IoCounters, Phase0CompactionConfig,
    Phase0CompactionCoordinatorProcessConfig, Phase0CompactionMode, Phase0CompactionReclaimConfig,
    Phase0CompactionReclaimMode, Phase0CompactionReclaimReport, Phase0CompactionReclaimSeedReport,
    Phase0CompactionReport, Phase0CompactionSeedReport, Phase0CompactionWorkerProcessConfig,
    Phase0Config, Phase0CoordinatorFencingConfig, Phase0CoordinatorFencingMode,
    Phase0CoordinatorFencingReport, Phase0CoordinatorFencingSeedReport,
    Phase0CoordinatorRecoveryConfig, Phase0CoordinatorRecoveryMode,
    Phase0CoordinatorRecoveryReport, Phase0CoordinatorRecoverySeedReport, Phase0Gate,
    Phase0IoDelta, Phase0Mode, Phase0OrphanGcConfig, Phase0OrphanGcMode, Phase0OrphanGcReport,
    Phase0OrphanGcSeedReport, Phase0PhaseReport, Phase0PhysicalProfile, Phase0PhysicalReceipt,
    Phase0Report, Phase0SeedReport,
};
pub use snapshot_read_curve::{
    run_snapshot_read_curve_worker, SnapshotReadCurveConfig, SnapshotReadCurveMode,
    SnapshotReadCurveReceipt, SnapshotReadTargetReceipt,
};

/// `SlateDB` source revision compiled by this adapter.
pub const SLATEDB_REVISION: &str = "e0161973d8d7ffdede7c44725729838811674e99";

const COMMIT_KEY_PREFIX: &[u8] = b"\x00okv/commit/";
const LATEST_VERSION_KEY: &[u8] = b"\x00okv/latest-version";
const USER_KEY_PREFIX: u8 = 1;
const KEY_ESCAPE: u8 = 0;
const KEY_ESCAPED_ZERO: u8 = 0xff;
const VALUE_TOMBSTONE: u8 = 0;
const VALUE_SET: u8 = 1;

/// Errors produced at the objectKV to `SlateDB` boundary.
#[derive(Debug, Eq, PartialEq)]
pub enum AdapterError {
    AuthorityManifestMismatch {
        key: String,
    },
    Backend(String),
    ConflictingReplay {
        version: Version,
    },
    CorruptMvcc(String),
    InvalidBatch(String),
    InvalidScanRange {
        start: Vec<u8>,
        end: Vec<u8>,
    },
    NonMonotonicVersion {
        latest: Version,
        attempted: Version,
    },
    RetentionFloorRegression {
        current: Version,
        attempted: Version,
    },
    SnapshotExpired {
        requested: Version,
        minimum: Version,
    },
    SnapshotUnavailable {
        requested: Version,
        applied: Version,
    },
    UnsupportedGeneration {
        generation: u64,
    },
    UnsupportedRangeClear,
    ZeroVersion,
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AuthorityManifestMismatch { key } => {
                write!(formatter, "authority manifest identity mismatch for {key}")
            }
            Self::Backend(message) => write!(formatter, "SlateDB error: {message}"),
            Self::ConflictingReplay { version } => write!(
                formatter,
                "version {version} was replayed with different mutations"
            ),
            Self::CorruptMvcc(message) => write!(formatter, "corrupt MVCC entry: {message}"),
            Self::InvalidBatch(message) => write!(formatter, "invalid commit batch: {message}"),
            Self::InvalidScanRange { start, end } => write!(
                formatter,
                "scan start {start:?} must be smaller than end {end:?}"
            ),
            Self::NonMonotonicVersion { latest, attempted } => write!(
                formatter,
                "version {attempted} cannot follow version {latest}"
            ),
            Self::RetentionFloorRegression { current, attempted } => write!(
                formatter,
                "minimum-readable version {attempted} cannot precede current floor {current}"
            ),
            Self::SnapshotExpired { requested, minimum } => write!(
                formatter,
                "snapshot {requested} precedes minimum-readable version {minimum}"
            ),
            Self::SnapshotUnavailable { requested, applied } => write!(
                formatter,
                "snapshot {requested} is unavailable at applied frontier {applied}"
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

/// Exact immutable manifest identity selected by the publication authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityManifestReference {
    pub key: String,
    pub length: u64,
    pub sha256: String,
}

/// Read-only objectKV MVCC view pinned to one authority-selected `SlateDB`
/// manifest rather than the engine's internal latest manifest.
pub struct AuthorityBoundSlateReader {
    reader: DbReader,
    bound_manifest: String,
}

/// Ordered visible-row cursor over one authority-selected immutable manifest.
pub struct AuthorityBoundSlateScan {
    iterator: DbIterator,
    physical_end: Vec<u8>,
    read_version: Version,
    finished: bool,
}

impl AuthorityBoundSlateScan {
    /// Advance to the next visible user row at the selected version.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for malformed MVCC state or an underlying
    /// iterator failure.
    pub async fn next(&mut self) -> Result<Option<Row>, AdapterError> {
        if self.finished {
            return Ok(None);
        }
        while let Some(entry) = self
            .iterator
            .next()
            .await
            .map_err(|error| backend(&error))?
        {
            let (user_key, version) = decode_user_version_key(entry.key.as_ref())?;
            if version > self.read_version {
                continue;
            }
            let value = decode_value(entry.value.as_ref())?;
            let next_user = user_key_successor(&user_key);
            let reached_end = next_user >= self.physical_end;
            if reached_end {
                self.finished = true;
            } else {
                self.iterator
                    .seek(next_user)
                    .await
                    .map_err(|error| backend(&error))?;
            }
            if let Some(value) = value {
                return Ok(Some((user_key, value)));
            }
            if reached_end {
                return Ok(None);
            }
        }
        self.finished = true;
        Ok(None)
    }
}

impl AuthorityBoundSlateReader {
    /// Open a read-only view whose manifest listing stops at the exact physical
    /// root selected by the objectKV authority.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the manifest key is not a `SlateDB` manifest
    /// path or the reader cannot open the selected physical state.
    pub async fn open(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        manifest: &AuthorityManifestReference,
        seed: u64,
    ) -> Result<Self, AdapterError> {
        Self::open_inner(database_path, store, manifest, seed, None).await
    }

    /// Open an authority-selected view with a caller-owned decoded block cache.
    /// The cache may be shared across every Range Engine in one KV Runtime.
    ///
    /// # Errors
    ///
    /// Returns the same identity and reader errors as [`Self::open`].
    pub async fn open_with_cache(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        manifest: &AuthorityManifestReference,
        seed: u64,
        cache: Arc<dyn DbCache>,
    ) -> Result<Self, AdapterError> {
        Self::open_inner(database_path, store, manifest, seed, Some(cache)).await
    }

    async fn open_inner(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        manifest: &AuthorityManifestReference,
        seed: u64,
        cache: Option<Arc<dyn DbCache>>,
    ) -> Result<Self, AdapterError> {
        let bytes = store
            .get(&object_store::path::Path::from(manifest.key.as_str()))
            .await
            .map_err(|error| AdapterError::Backend(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| AdapterError::Backend(error.to_string()))?;
        let exact_identity = u64::try_from(bytes.len()).unwrap_or(u64::MAX) == manifest.length
            && format!("{:x}", Sha256::digest(&bytes)) == manifest.sha256;
        if !exact_identity {
            return Err(AdapterError::AuthorityManifestMismatch {
                key: manifest.key.clone(),
            });
        }
        let bound_store = Arc::new(
            manifest_bound_store::ManifestBoundStore::new(store, database_path, &manifest.key)
                .map_err(AdapterError::Backend)?,
        );
        let options = DbReaderOptions {
            skip_wal_replay: true,
            ..DbReaderOptions::default()
        };
        let builder = DbReader::builder(database_path, bound_store)
            .with_reader_mode(DbReaderMode::FollowLatest)
            .with_options(options)
            .with_seed(seed);
        let builder = match cache {
            Some(cache) => builder.with_db_cache(cache),
            None => builder,
        };
        let reader = builder.build().await.map_err(|error| backend(&error))?;
        Ok(Self {
            reader,
            bound_manifest: manifest.key.clone(),
        })
    }

    /// Return the exact manifest key used to construct this view.
    #[must_use]
    pub fn bound_manifest(&self) -> &str {
        &self.bound_manifest
    }

    /// Return the latest applied objectKV version visible in the selected
    /// physical manifest.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for malformed private state or a reader error.
    pub async fn latest_version(&self) -> Result<Version, AdapterError> {
        let encoded = self
            .reader
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

    /// Read at one external version while enforcing an authority-supplied
    /// minimum-readable version.
    ///
    /// # Errors
    ///
    /// Returns an expired or unavailable snapshot error, malformed MVCC state,
    /// or a reader error.
    pub async fn get_at_retained(
        &self,
        key: &[u8],
        read_version: Version,
        minimum_readable_version: Version,
    ) -> Result<Option<Vec<u8>>, AdapterError> {
        self.require_readable(read_version, minimum_readable_version)
            .await?;
        let prefix = user_key_prefix(key);
        let start = complemented_version(read_version);
        let mut iterator = self
            .reader
            .scan_prefix(prefix, start.as_slice()..)
            .await
            .map_err(|error| backend(&error))?;
        let Some(entry) = iterator.next().await.map_err(|error| backend(&error))? else {
            return Ok(None);
        };
        decode_value(entry.value.as_ref())
    }

    /// Scan one half-open user-key interval through the selected physical
    /// manifest.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for an invalid interval, expired or unavailable
    /// snapshot, malformed MVCC state, or a reader error.
    pub async fn scan_at_retained(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: Version,
        minimum_readable_version: Version,
        limit: usize,
    ) -> Result<Vec<Row>, AdapterError> {
        if start >= end {
            return Err(AdapterError::InvalidScanRange {
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
        if limit == 0 {
            self.require_readable(read_version, minimum_readable_version)
                .await?;
            return Ok(Vec::new());
        }
        let mut cursor = self
            .scan_cursor_at_retained(start, end, read_version, minimum_readable_version)
            .await?;
        let mut rows = Vec::with_capacity(limit.min(1_024));
        while let Some(row) = cursor.next().await? {
            rows.push(row);
            if rows.len() == limit {
                break;
            }
        }
        Ok(rows)
    }

    /// Open an ordered visible-row cursor without materializing the requested
    /// range in memory.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for an invalid interval, unavailable snapshot,
    /// or underlying reader failure.
    pub async fn scan_cursor_at_retained(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: Version,
        minimum_readable_version: Version,
    ) -> Result<AuthorityBoundSlateScan, AdapterError> {
        if start >= end {
            return Err(AdapterError::InvalidScanRange {
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
        self.require_readable(read_version, minimum_readable_version)
            .await?;
        let physical_start = user_key_prefix(start);
        let physical_end = user_key_prefix(end);
        let iterator = self
            .reader
            .scan(physical_start..physical_end.clone())
            .await
            .map_err(|error| backend(&error))?;
        Ok(AuthorityBoundSlateScan {
            iterator,
            physical_end,
            read_version,
            finished: false,
        })
    }

    /// Close the bound reader.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when the reader cannot close cleanly.
    pub async fn close(&self) -> Result<(), AdapterError> {
        self.reader.close().await.map_err(|error| backend(&error))
    }

    async fn require_readable(
        &self,
        requested: Version,
        minimum: Version,
    ) -> Result<(), AdapterError> {
        if requested < minimum {
            return Err(AdapterError::SnapshotExpired { requested, minimum });
        }
        let applied = self.latest_version().await?;
        if requested > applied {
            return Err(AdapterError::SnapshotUnavailable { requested, applied });
        }
        Ok(())
    }
}

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
                Mutation::Set { key, value } => {
                    write_batch.put(user_version_key(&key, batch.version), encode_set(&value));
                }
                Mutation::Clear { key } => {
                    write_batch.put(user_version_key(&key, batch.version), [VALUE_TOMBSTONE]);
                }
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
        let latest = self.latest_version().await?;
        self.get_visible(key, latest).await
    }

    /// Read the newest value at or below an exact external version.
    ///
    /// The adapter refuses a version above its applied frontier. A point clear
    /// is returned as an absent value while older physical history remains
    /// present.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError::SnapshotUnavailable`] when the requested
    /// version is ahead of this engine, or an adapter error for malformed MVCC
    /// state and `SlateDB` failures.
    pub async fn get_at(
        &self,
        key: &[u8],
        read_version: Version,
    ) -> Result<Option<Vec<u8>>, AdapterError> {
        self.get_at_retained(key, read_version, Version::ZERO).await
    }

    /// Read at an exact version while enforcing an authority-supplied
    /// minimum-readable version.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError::SnapshotExpired`] below the supplied floor,
    /// [`AdapterError::SnapshotUnavailable`] above the applied frontier, or an
    /// adapter error for malformed state and `SlateDB` failures.
    pub async fn get_at_retained(
        &self,
        key: &[u8],
        read_version: Version,
        minimum_readable_version: Version,
    ) -> Result<Option<Vec<u8>>, AdapterError> {
        self.require_readable(read_version, minimum_readable_version)
            .await?;
        self.get_visible(key, read_version).await
    }

    /// Scan one half-open user-key interval at an exact external version.
    ///
    /// Rows are returned once each in ascending raw user-key order. The limit
    /// counts visible rows, not physical MVCC entries.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for an invalid interval, an unavailable
    /// snapshot, malformed MVCC state, or a `SlateDB` failure.
    pub async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: Version,
        limit: usize,
    ) -> Result<Vec<Row>, AdapterError> {
        self.scan_at_retained(start, end, read_version, Version::ZERO, limit)
            .await
    }

    /// Scan one half-open user-key interval with an authority-supplied
    /// minimum-readable version.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for an invalid interval, an expired or
    /// unavailable snapshot, malformed MVCC state, or a `SlateDB` failure.
    pub async fn scan_at_retained(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: Version,
        minimum_readable_version: Version,
        limit: usize,
    ) -> Result<Vec<Row>, AdapterError> {
        if start >= end {
            return Err(AdapterError::InvalidScanRange {
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
        self.require_readable(read_version, minimum_readable_version)
            .await?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let physical_start = user_key_prefix(start);
        let physical_end = user_key_prefix(end);
        let mut iterator = self
            .db
            .scan(physical_start..physical_end.clone())
            .await
            .map_err(|error| backend(&error))?;
        let mut rows = Vec::with_capacity(limit.min(1_024));

        while let Some(entry) = iterator.next().await.map_err(|error| backend(&error))? {
            let (user_key, version) = decode_user_version_key(entry.key.as_ref())?;
            if version > read_version {
                continue;
            }

            if let Some(value) = decode_value(entry.value.as_ref())? {
                rows.push((user_key.clone(), value));
                if rows.len() == limit {
                    break;
                }
            }
            let next_user = user_key_successor(&user_key);
            if next_user >= physical_end {
                break;
            }
            iterator
                .seek(next_user)
                .await
                .map_err(|error| backend(&error))?;
        }
        Ok(rows)
    }

    /// Flush the current objectKV MVCC memtable into immutable object state.
    ///
    /// # Errors
    ///
    /// Returns an adapter error when `SlateDB` cannot complete the flush.
    pub async fn flush(&self) -> Result<(), AdapterError> {
        self.db.flush().await.map_err(|error| backend(&error))
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

    async fn require_applied(&self, requested: Version) -> Result<(), AdapterError> {
        let applied = self.latest_version().await?;
        if requested > applied {
            return Err(AdapterError::SnapshotUnavailable { requested, applied });
        }
        Ok(())
    }

    async fn require_readable(
        &self,
        requested: Version,
        minimum: Version,
    ) -> Result<(), AdapterError> {
        if requested < minimum {
            return Err(AdapterError::SnapshotExpired { requested, minimum });
        }
        self.require_applied(requested).await
    }

    async fn get_visible(
        &self,
        key: &[u8],
        read_version: Version,
    ) -> Result<Option<Vec<u8>>, AdapterError> {
        let prefix = user_key_prefix(key);
        let start = complemented_version(read_version);
        let mut iterator = self
            .db
            .scan_prefix(prefix, start.as_slice()..)
            .await
            .map_err(|error| backend(&error))?;
        let Some(entry) = iterator.next().await.map_err(|error| backend(&error))? else {
            return Ok(None);
        };
        decode_value(entry.value.as_ref())
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

fn user_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 3);
    encoded.push(USER_KEY_PREFIX);
    for byte in key {
        if *byte == KEY_ESCAPE {
            encoded.extend_from_slice(&[KEY_ESCAPE, KEY_ESCAPED_ZERO]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[KEY_ESCAPE, KEY_ESCAPE]);
    encoded
}

fn user_version_key(key: &[u8], version: Version) -> Vec<u8> {
    let mut encoded = user_key_prefix(key);
    encoded.extend_from_slice(&complemented_version(version));
    encoded
}

fn user_key_successor(key: &[u8]) -> Vec<u8> {
    let mut successor = user_key_prefix(key);
    let terminator = successor
        .last_mut()
        .expect("user key prefix always contains a terminator");
    *terminator = terminator.saturating_add(1);
    successor
}

fn complemented_version(version: Version) -> [u8; 16] {
    let mut encoded = version.to_be_bytes();
    for byte in &mut encoded {
        *byte = !*byte;
    }
    encoded
}

fn decode_user_version_key(encoded: &[u8]) -> Result<(Vec<u8>, Version), AdapterError> {
    if encoded.first() != Some(&USER_KEY_PREFIX) {
        return Err(AdapterError::CorruptMvcc(
            "user entry has the wrong namespace".to_owned(),
        ));
    }

    let mut user_key = Vec::new();
    let mut offset = 1_usize;
    loop {
        let Some(byte) = encoded.get(offset).copied() else {
            return Err(AdapterError::CorruptMvcc(
                "user key has no terminator".to_owned(),
            ));
        };
        if byte != KEY_ESCAPE {
            user_key.push(byte);
            offset = offset.saturating_add(1);
            continue;
        }

        let Some(next) = encoded.get(offset.saturating_add(1)).copied() else {
            return Err(AdapterError::CorruptMvcc(
                "user key ends inside an escape".to_owned(),
            ));
        };
        match next {
            KEY_ESCAPE => {
                offset = offset.saturating_add(2);
                break;
            }
            KEY_ESCAPED_ZERO => {
                user_key.push(KEY_ESCAPE);
                offset = offset.saturating_add(2);
            }
            other => {
                return Err(AdapterError::CorruptMvcc(format!(
                    "user key has unknown escape {other:#04x}"
                )));
            }
        }
    }

    let complemented: [u8; 16] = encoded
        .get(offset..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            AdapterError::CorruptMvcc("user entry does not end in one 16-byte version".to_owned())
        })?;
    let version = complemented.map(|byte| !byte);
    Ok((user_key, Version::from_be_bytes(version)))
}

fn encode_set(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(value.len().saturating_add(1));
    encoded.push(VALUE_SET);
    encoded.extend_from_slice(value);
    encoded
}

fn decode_value(encoded: &[u8]) -> Result<Option<Vec<u8>>, AdapterError> {
    match encoded.split_first() {
        Some((&VALUE_TOMBSTONE, [])) => Ok(None),
        Some((&VALUE_TOMBSTONE, _)) => Err(AdapterError::CorruptMvcc(
            "point tombstone carries trailing bytes".to_owned(),
        )),
        Some((&VALUE_SET, value)) => Ok(Some(value.to_vec())),
        Some((&tag, _)) => Err(AdapterError::CorruptMvcc(format!(
            "unknown value tag {tag:#04x}"
        ))),
        None => Err(AdapterError::CorruptMvcc(
            "value envelope is empty".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_user_version_key, user_key_prefix, user_version_key, AdapterError,
        AuthorityBoundSlateReader, AuthorityManifestReference, SlateEngine, SLATEDB_REVISION,
    };
    use okv_model::{ApplyOutcome, CommitBatch, CommitIdentity, KeyRange, Mutation, Version};
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStore, ObjectStoreExt};
    use slatedb::{admin::Admin, Db};
    use std::sync::Arc;

    async fn engine(name: &str) -> SlateEngine {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open(format!("okv-slate/{name}"), store)
            .await
            .expect("open SlateDB");
        SlateEngine::new(db)
    }

    #[tokio::test]
    async fn authority_bound_reader_keeps_the_selected_manifest_visible() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "okv-slate/authority-bound-reader";
        let db = Db::open(path, Arc::clone(&store))
            .await
            .expect("open writer");
        let engine = SlateEngine::new(db);
        engine
            .apply(set(1, b"key", b"version-1"))
            .await
            .expect("apply version one");
        engine.flush().await.expect("flush version one");
        engine.close().await.expect("close version-one writer");
        let admin = Admin::builder(path, Arc::clone(&store)).build();
        let first_manifest = admin
            .read_manifest(None)
            .await
            .expect("read first manifest")
            .expect("first manifest exists");
        let first_manifest_key = format!("{path}/manifest/{:020}.manifest", first_manifest.id());
        let first_manifest_bytes = store
            .get(&slatedb::object_store::path::Path::from(
                first_manifest_key.as_str(),
            ))
            .await
            .expect("read first manifest object")
            .bytes()
            .await
            .expect("read first manifest bytes");
        let first_manifest_reference = AuthorityManifestReference {
            key: first_manifest_key.clone(),
            length: u64::try_from(first_manifest_bytes.len()).expect("manifest length fits u64"),
            sha256: format!("{:x}", Sha256::digest(&first_manifest_bytes)),
        };
        let mut forged_reference = first_manifest_reference.clone();
        forged_reference.sha256 = "00".repeat(32);
        assert!(matches!(
            AuthorityBoundSlateReader::open(path, Arc::clone(&store), &forged_reference, 1102,)
                .await,
            Err(AdapterError::AuthorityManifestMismatch { .. })
        ));

        let reopened = Db::open(path, Arc::clone(&store))
            .await
            .expect("reopen writer");
        let engine = SlateEngine::new(reopened);
        engine
            .apply(set(2, b"key", b"version-2"))
            .await
            .expect("apply version two");
        engine.flush().await.expect("flush version two");
        engine.close().await.expect("close version-two writer");
        let second_manifest = admin
            .read_manifest(None)
            .await
            .expect("read second manifest")
            .expect("second manifest exists");
        assert!(second_manifest.id() > first_manifest.id());

        let reader = AuthorityBoundSlateReader::open(
            path,
            Arc::clone(&store),
            &first_manifest_reference,
            1103,
        )
        .await
        .expect("open authority-bound reader");
        assert_eq!(reader.bound_manifest(), first_manifest_key);
        assert_eq!(
            reader.latest_version().await.expect("read bound frontier"),
            Version::new(1)
        );
        assert_eq!(
            reader
                .get_at_retained(b"key", Version::new(1), Version::new(1))
                .await
                .expect("read version one"),
            Some(b"version-1".to_vec())
        );
        let mut scan = reader
            .scan_cursor_at_retained(b"a", b"z", Version::new(1), Version::new(1))
            .await
            .expect("open authority-bound scan");
        assert_eq!(
            scan.next().await.expect("read authority-bound row"),
            Some((b"key".to_vec(), b"version-1".to_vec()))
        );
        assert_eq!(scan.next().await.expect("finish scan"), None);
        assert_eq!(
            scan.next().await.expect("finished scan stays finished"),
            None
        );
        assert!(matches!(
            reader
                .get_at_retained(b"key", Version::new(2), Version::new(1))
                .await,
            Err(AdapterError::SnapshotUnavailable { .. })
        ));
        reader.close().await.expect("close bound reader");
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
    async fn retained_reads_reject_expired_and_future_snapshots() {
        let engine = engine("retained-read-bounds").await;
        engine
            .apply(set(10, b"k", b"ten"))
            .await
            .expect("apply version ten");
        engine
            .apply(set(20, b"k", b"twenty"))
            .await
            .expect("apply version twenty");

        assert_eq!(
            engine
                .get_at_retained(b"k", Version::new(9), Version::new(10))
                .await,
            Err(AdapterError::SnapshotExpired {
                requested: Version::new(9),
                minimum: Version::new(10),
            })
        );
        assert_eq!(
            engine
                .get_at_retained(b"k", Version::new(10), Version::new(10))
                .await,
            Ok(Some(b"ten".to_vec()))
        );
        assert_eq!(
            engine
                .scan_at_retained(b"a", b"z", Version::new(21), Version::new(10), 10,)
                .await,
            Err(AdapterError::SnapshotUnavailable {
                requested: Version::new(21),
                applied: Version::new(20),
            })
        );
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

    #[tokio::test]
    async fn concurrent_identical_version_has_one_apply_and_one_replay() {
        let engine = engine("concurrent-identical").await;
        let batch = set(10, b"k", b"value");
        let (left, right) = tokio::join!(engine.apply(batch.clone()), engine.apply(batch));
        assert!(
            (left == Ok(ApplyOutcome::Applied) && right == Ok(ApplyOutcome::AlreadyApplied))
                || (right == Ok(ApplyOutcome::Applied) && left == Ok(ApplyOutcome::AlreadyApplied))
        );
        assert_eq!(engine.latest_version().await, Ok(Version::new(10)));
        engine.close().await.expect("close SlateDB");
    }

    #[tokio::test]
    async fn concurrent_conflicting_version_has_one_winner() {
        let engine = engine("concurrent-conflicting").await;
        let (left, right) = tokio::join!(
            engine.apply(set(10, b"k", b"left")),
            engine.apply(set(10, b"k", b"right"))
        );
        let conflict = Err(AdapterError::ConflictingReplay {
            version: Version::new(10),
        });
        assert!(
            (left == Ok(ApplyOutcome::Applied) && right == conflict)
                || (right == Ok(ApplyOutcome::Applied) && left == conflict)
        );
        assert_eq!(engine.latest_version().await, Ok(Version::new(10)));
        assert!(matches!(
            engine.get_latest(b"k").await,
            Ok(Some(value)) if value == b"left" || value == b"right"
        ));
        engine.close().await.expect("close SlateDB");
    }

    #[test]
    fn pins_the_observed_revision() {
        assert_eq!(SLATEDB_REVISION.len(), 40);
    }

    #[test]
    fn mvcc_key_codec_preserves_binary_user_order_and_descending_versions() {
        let keys = [b"a".as_slice(), b"a\0z".as_slice(), b"aa".as_slice()];
        let encoded = keys.map(user_key_prefix);
        assert!(encoded[0] < encoded[1]);
        assert!(encoded[1] < encoded[2]);

        let newer = user_version_key(b"a\0z", Version::new(20));
        let older = user_version_key(b"a\0z", Version::new(10));
        assert!(newer < older);
        assert_eq!(
            decode_user_version_key(&newer),
            Ok((b"a\0z".to_vec(), Version::new(20)))
        );
    }

    #[test]
    fn mvcc_key_decoder_rejects_malformed_entries() {
        assert!(matches!(
            decode_user_version_key(&[1, b'a', 0]),
            Err(AdapterError::CorruptMvcc(_))
        ));
        assert!(matches!(
            decode_user_version_key(&[1, b'a', 0, 1]),
            Err(AdapterError::CorruptMvcc(_))
        ));
        assert!(matches!(
            decode_user_version_key(&[1, b'a', 0, 0, 1, 2]),
            Err(AdapterError::CorruptMvcc(_))
        ));
    }
}
