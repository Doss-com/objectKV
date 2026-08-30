//! Disposable `RocksDB` point-serving image for objectKV.

use okv::{
    ReadOutcome, ResidentActivationRequest, ResidentAdvanceRequest, ResidentEngineReceipt,
    ResidentMutation, ResidentRangeBounds, ResidentRangeEngine, ResidentSnapshot,
    ResidentTransactionRecord, ServingImage, ServingImageReceipt, ServingImageRecord, StreamCursor,
};
use rocksdb::statistics::Ticker;
use rocksdb::{
    AsColumnFamilyRef, BlockBasedOptions, Cache, ColumnFamilyDescriptor, Direction, IteratorMode,
    Options, WriteBatch, WriteOptions, DB,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const TOMBSTONE_TAG: u8 = 0;
const VALUE_TAG: u8 = 1;
const ABSENT_TAG: u8 = 2;
const INSTALL_BATCH_RECORDS: usize = 4_096;
const DEFAULT_RESIDENT_BLOCK_CACHE_BYTES: u64 = 128 * 1_024 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveImage {
    generation: u64,
    covered_through: u64,
    records: u64,
    local_bytes: u64,
}

/// Empty or completely activated `RocksDB` image on disposable local media.
pub struct RocksDbServingImage {
    database: DB,
    root: PathBuf,
    max_local_bytes: u64,
    active: Option<ActiveImage>,
}

impl Debug for RocksDbServingImage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RocksDbServingImage")
            .field("root", &self.root)
            .field("max_local_bytes", &self.max_local_bytes)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl RocksDbServingImage {
    /// Open one empty provider rooted on disposable local media.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is non-empty, the byte budget is zero, or
    /// `RocksDB` cannot open the image.
    pub fn open(root: &Path, max_local_bytes: u64) -> Result<Self, String> {
        if max_local_bytes == 0 {
            return Err("RocksDB serving image requires a positive local-byte budget".to_owned());
        }
        if root.exists() {
            let mut entries = fs::read_dir(root)
                .map_err(|error| format!("read RocksDB serving root: {error}"))?;
            if entries
                .next()
                .transpose()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("RocksDB serving image requires an empty root".to_owned());
            }
        } else {
            fs::create_dir_all(root)
                .map_err(|error| format!("create RocksDB serving root: {error}"))?;
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.optimize_for_point_lookup(128);
        options.set_max_open_files(256);
        let database = DB::open(&options, root)
            .map_err(|error| format!("open RocksDB serving image: {error}"))?;
        Ok(Self {
            database,
            root: root.to_path_buf(),
            max_local_bytes,
            active: None,
        })
    }
}

impl ServingImage for RocksDbServingImage {
    fn activate(
        &mut self,
        generation: u64,
        covered_through: u64,
        records: Vec<ServingImageRecord>,
    ) -> Result<ServingImageReceipt, String> {
        if generation == 0 || covered_through == 0 || records.is_empty() {
            return Err("RocksDB serving activation requires non-zero complete state".to_owned());
        }
        if self.active.is_some() {
            return Err("RocksDB serving image is already active".to_owned());
        }
        let mut previous_key: Option<&[u8]> = None;
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        let mut batch = WriteBatch::default();
        for (index, record) in records.iter().enumerate() {
            if record.key.is_empty()
                || previous_key.is_some_and(|previous| previous >= record.key.as_slice())
            {
                return Err("RocksDB serving records must have unique ordered keys".to_owned());
            }
            previous_key = Some(&record.key);
            let encoded = encode_value(record.value.as_deref());
            batch.put(&record.key, encoded);
            if index % INSTALL_BATCH_RECORDS == INSTALL_BATCH_RECORDS - 1 {
                self.database
                    .write_opt(batch, &write_options)
                    .map_err(|error| format!("install RocksDB serving batch: {error}"))?;
                batch = WriteBatch::default();
            }
        }
        if !batch.is_empty() {
            self.database
                .write_opt(batch, &write_options)
                .map_err(|error| format!("install RocksDB serving batch: {error}"))?;
        }
        self.database
            .flush()
            .map_err(|error| format!("flush RocksDB serving image: {error}"))?;
        let local_bytes = database_directory_bytes(&self.database, &self.root)?;
        if local_bytes > self.max_local_bytes {
            return Err(format!(
                "RocksDB serving image uses {local_bytes} bytes above its {} byte budget",
                self.max_local_bytes
            ));
        }
        let records = u64::try_from(records.len()).unwrap_or(u64::MAX);
        self.active = Some(ActiveImage {
            generation,
            covered_through,
            records,
            local_bytes,
        });
        Ok(ServingImageReceipt {
            provider: "rocksdb-11.8.1".to_owned(),
            generation,
            covered_through,
            records,
            local_bytes,
        })
    }

    fn get(
        &self,
        generation: u64,
        covered_through: u64,
        key: &[u8],
    ) -> Result<ReadOutcome, String> {
        let active = self
            .active
            .ok_or_else(|| "RocksDB serving image is not active".to_owned())?;
        if active.generation != generation || active.covered_through != covered_through {
            return Err("RocksDB serving image generation or coverage is stale".to_owned());
        }
        let value = self
            .database
            .get_pinned(key)
            .map_err(|error| format!("read RocksDB serving image: {error}"))?;
        value.map_or(Ok(ReadOutcome::Absent), |value| {
            decode_value(value.as_ref())
        })
    }
}

const HISTORY_CF: &str = "history";
const METADATA_CF: &str = "metadata";
/// Provider identity for the sparse post-object-frontier resident format.
pub const RESIDENT_PROVIDER: &str = "rocksdb-11.8.1-native-resident-v2";
/// Persisted resident-engine format written by this provider.
pub const RESIDENT_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug)]
struct NativeActiveImage {
    generation: u64,
    object_root: String,
    object_durable_version: u64,
    applied: StreamCursor,
    owned_range: ResidentRangeBounds,
    object_first_key: Vec<u8>,
    object_last_key: Vec<u8>,
    records: u64,
    local_bytes: u64,
}

#[derive(Default)]
struct NativeEngineState {
    active: Option<NativeActiveImage>,
    known_keys: BTreeSet<Vec<u8>>,
    history_seeded_keys: BTreeSet<Vec<u8>>,
    failed: bool,
}

/// `RocksDB` implementation of the transition-verified resident data plane.
pub struct RocksDbResidentRangeEngine {
    database: DB,
    root: PathBuf,
    max_local_bytes: u64,
    block_cache_bytes: u64,
    direct_reads: bool,
    block_cache: Cache,
    statistics: Options,
    state: Mutex<NativeEngineState>,
    transition_epoch: AtomicU64,
}

/// One cumulative `RocksDB` counter snapshot for a resident range engine.
///
/// Evaluators subtract two snapshots around a measured window. Cache capacity,
/// usage, and pinned usage are gauges and must not be subtracted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RocksDbResidentMetrics {
    pub database_count: u64,
    pub block_cache_count: u64,
    pub implicit_block_cache_count: u64,
    pub column_family_count: u64,
    pub metadata_cache_disabled: bool,
    pub block_cache_capacity_bytes: u64,
    pub block_cache_usage_bytes: u64,
    pub block_cache_pinned_usage_bytes: u64,
    pub direct_reads: bool,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub block_cache_data_hits: u64,
    pub block_cache_data_misses: u64,
    pub block_cache_bytes_read: u64,
    pub bytes_read: u64,
    pub read_amp_useful_bytes: u64,
    pub read_amp_total_bytes: u64,
    pub flush_write_bytes: u64,
    pub compaction_read_bytes: u64,
    pub compaction_write_bytes: u64,
}

impl Debug for RocksDbResidentRangeEngine {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RocksDbResidentRangeEngine")
            .field("root", &self.root)
            .field("max_local_bytes", &self.max_local_bytes)
            .field("block_cache_bytes", &self.block_cache_bytes)
            .field("direct_reads", &self.direct_reads)
            .field(
                "transition_epoch",
                &self.transition_epoch.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl RocksDbResidentRangeEngine {
    /// Open one empty native resident engine on disposable local media.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-empty root, zero byte budget, or `RocksDB`
    /// open failure.
    pub fn open(root: &Path, max_local_bytes: u64) -> Result<Self, String> {
        Self::open_with_block_cache(root, max_local_bytes, DEFAULT_RESIDENT_BLOCK_CACHE_BYTES)
    }

    /// Open one empty native resident engine with an explicit shared block
    /// cache budget.
    ///
    /// The default head and MVCC history column families share this cache and
    /// one `RocksDB` statistics object. This makes cache use and read
    /// amplification observable at the complete resident-engine boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-empty root, a zero local or cache budget, a
    /// cache budget that cannot fit in `usize`, or a `RocksDB` open failure.
    pub fn open_with_block_cache(
        root: &Path,
        max_local_bytes: u64,
        block_cache_bytes: u64,
    ) -> Result<Self, String> {
        Self::open_with_block_cache_and_direct_reads(
            root,
            max_local_bytes,
            block_cache_bytes,
            false,
        )
    }

    /// Open one empty native resident engine with an explicit shared block
    /// cache and an explicit operating-system page-cache treatment.
    ///
    /// `direct_reads` applies `RocksDB` direct I/O to table-file reads only.
    /// Flushes and compactions retain the portable buffered-I/O default.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::open_with_block_cache`].
    pub fn open_with_block_cache_and_direct_reads(
        root: &Path,
        max_local_bytes: u64,
        block_cache_bytes: u64,
        direct_reads: bool,
    ) -> Result<Self, String> {
        if max_local_bytes == 0 {
            return Err("native resident engine requires a positive local-byte budget".to_owned());
        }
        if block_cache_bytes == 0 {
            return Err("native resident engine requires a positive block-cache budget".to_owned());
        }
        let cache_capacity = usize::try_from(block_cache_bytes)
            .map_err(|_| "native resident block-cache budget exceeds usize".to_owned())?;
        require_empty_root(root)?;
        let block_cache = Cache::new_lru_cache(cache_capacity);
        let point_options = measured_point_options(&block_cache, direct_reads);
        let mut database_options = point_options.clone();
        database_options.create_missing_column_families(true);
        let database = DB::open_cf_descriptors(
            &database_options,
            root,
            [
                ColumnFamilyDescriptor::new("default", point_options.clone()),
                ColumnFamilyDescriptor::new(HISTORY_CF, point_options.clone()),
                ColumnFamilyDescriptor::new(METADATA_CF, uncached_metadata_options()),
            ],
        )
        .map_err(|error| format!("open native resident RocksDB: {error}"))?;
        Ok(Self {
            database,
            root: root.to_path_buf(),
            max_local_bytes,
            block_cache_bytes,
            direct_reads,
            block_cache,
            statistics: point_options,
            state: Mutex::new(NativeEngineState::default()),
            transition_epoch: AtomicU64::new(0),
        })
    }

    /// Capture cumulative `RocksDB` counters and current shared-cache gauges.
    #[must_use]
    pub fn metrics(&self) -> RocksDbResidentMetrics {
        RocksDbResidentMetrics {
            database_count: 1,
            block_cache_count: 1,
            implicit_block_cache_count: 0,
            column_family_count: 3,
            metadata_cache_disabled: true,
            block_cache_capacity_bytes: self.block_cache_bytes,
            block_cache_usage_bytes: usize_as_u64(self.block_cache.get_usage()),
            block_cache_pinned_usage_bytes: usize_as_u64(self.block_cache.get_pinned_usage()),
            direct_reads: self.direct_reads,
            block_cache_hits: self.statistics.get_ticker_count(Ticker::BlockCacheHit),
            block_cache_misses: self.statistics.get_ticker_count(Ticker::BlockCacheMiss),
            block_cache_data_hits: self.statistics.get_ticker_count(Ticker::BlockCacheDataHit),
            block_cache_data_misses: self.statistics.get_ticker_count(Ticker::BlockCacheDataMiss),
            block_cache_bytes_read: self
                .statistics
                .get_ticker_count(Ticker::BlockCacheBytesRead),
            bytes_read: self.statistics.get_ticker_count(Ticker::BytesRead),
            read_amp_useful_bytes: self
                .statistics
                .get_ticker_count(Ticker::ReadAmpEstimateUsefulBytes),
            read_amp_total_bytes: self
                .statistics
                .get_ticker_count(Ticker::ReadAmpTotalReadBytes),
            flush_write_bytes: self.statistics.get_ticker_count(Ticker::FlushWriteBytes),
            compaction_read_bytes: self.statistics.get_ticker_count(Ticker::CompactReadBytes),
            compaction_write_bytes: self.statistics.get_ticker_count(Ticker::CompactWriteBytes),
        }
    }

    /// Evict every unpinned block-cache entry while preserving the configured
    /// capacity. This is an evaluator boundary used between independent
    /// workload samples on one immutable local fixture.
    ///
    /// # Errors
    ///
    /// Returns an error when any unpinned cache bytes survive eviction.
    pub fn reset_block_cache(&self) -> Result<(), String> {
        let capacity = usize::try_from(self.block_cache_bytes)
            .map_err(|_| "native resident block-cache budget exceeds usize".to_owned())?;
        let mut cache = self.block_cache.clone();
        cache.set_capacity(0);
        let remaining = cache.get_usage();
        let pinned = cache.get_pinned_usage();
        cache.set_capacity(capacity);
        if remaining > pinned {
            return Err(format!(
                "native resident block cache retained {} unpinned bytes after reset",
                remaining.saturating_sub(pinned)
            ));
        }
        Ok(())
    }

    fn history_get(
        &self,
        key: &[u8],
        read_version: u64,
        object_durable_version: u64,
    ) -> Result<ReadOutcome, String> {
        let history = self
            .database
            .cf_handle(HISTORY_CF)
            .ok_or_else(|| "native resident history column family is absent".to_owned())?;
        let prefix = history_prefix(key)?;
        let iterator = self
            .database
            .iterator_cf(history, IteratorMode::From(&prefix, Direction::Forward));
        let mut saw_history = false;
        for item in iterator {
            let (encoded_key, encoded_value) =
                item.map_err(|error| format!("read resident history: {error}"))?;
            if !encoded_key.starts_with(&prefix) {
                break;
            }
            saw_history = true;
            let commit_version = decode_history_commit(&encoded_key, prefix.len())?;
            if commit_version < object_durable_version {
                return Err("resident history precedes the object frontier".to_owned());
            }
            if commit_version <= read_version {
                return decode_history_value(&encoded_value);
            }
        }
        if saw_history {
            Err("resident history is missing its object-frontier seed".to_owned())
        } else {
            self.head_get(key)
        }
    }

    fn head_get(&self, key: &[u8]) -> Result<ReadOutcome, String> {
        self.database
            .get(key)
            .map_err(|error| format!("read native resident head: {error}"))?
            .map_or(Ok(ReadOutcome::Absent), decode_owned_value)
    }
}

impl ResidentRangeEngine for RocksDbResidentRangeEngine {
    fn activate(
        &self,
        request: ResidentActivationRequest,
    ) -> Result<ResidentEngineReceipt, String> {
        validate_activation(&request)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "native resident engine state lock is poisoned".to_owned())?;
        if state.failed {
            return Err("native resident engine previously failed closed".to_owned());
        }
        if state.active.is_some() {
            return Err("native resident engine is already active".to_owned());
        }
        let stable_epoch = begin_transition(&self.transition_epoch)?;
        let result = (|| {
            let metadata = self
                .database
                .cf_handle(METADATA_CF)
                .ok_or_else(|| "native resident metadata column family is absent".to_owned())?;
            let mut batch = WriteBatch::default();
            let mut known_keys = BTreeSet::new();
            for record in &request.records {
                let encoded = encode_value(record.value.as_deref());
                batch.put(&record.key, &encoded);
                known_keys.insert(record.key.clone());
            }
            let mut active = NativeActiveImage {
                generation: request.generation,
                object_root: request.object_root,
                object_durable_version: request.object_durable_version,
                applied: StreamCursor::after_complete_version(request.object_durable_version),
                owned_range: request.owned_range,
                object_first_key: request.object_first_key,
                object_last_key: request.object_last_key,
                records: u64::try_from(known_keys.len()).unwrap_or(u64::MAX),
                local_bytes: 0,
            };
            batch.put_cf(metadata, b"active", encode_metadata(&active)?);
            install_and_flush_base(&self.database, batch)?;
            active.local_bytes = database_directory_bytes(&self.database, &self.root)?;
            if active.local_bytes > self.max_local_bytes {
                return Err(format!(
                    "native resident engine uses {} bytes above its {} byte budget",
                    active.local_bytes, self.max_local_bytes
                ));
            }
            Ok((active, known_keys))
        })();
        finish_transition(&self.transition_epoch, stable_epoch);
        match result {
            Ok((active, known_keys)) => {
                state.known_keys = known_keys;
                state.history_seeded_keys.clear();
                state.active = Some(active.clone());
                Ok(native_receipt(&active))
            }
            Err(error) => {
                state.failed = true;
                Err(error)
            }
        }
    }

    fn advance(&self, request: ResidentAdvanceRequest) -> Result<ResidentEngineReceipt, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "native resident engine state lock is poisoned".to_owned())?;
        if state.failed {
            return Err("native resident engine previously failed closed".to_owned());
        }
        let active = state
            .active
            .clone()
            .ok_or_else(|| "native resident engine is not active".to_owned())?;
        validate_advance(&active, &request)?;
        if request.start == request.end && request.records.is_empty() {
            return Ok(native_receipt(&active));
        }
        let stable_epoch = begin_transition(&self.transition_epoch)?;
        let result = (|| {
            let history = self
                .database
                .cf_handle(HISTORY_CF)
                .ok_or_else(|| "native resident history column family is absent".to_owned())?;
            let metadata = self
                .database
                .cf_handle(METADATA_CF)
                .ok_or_else(|| "native resident metadata column family is absent".to_owned())?;
            let mut batch = WriteBatch::default();
            let mut known_keys = state.known_keys.clone();
            let mut history_seeded_keys = state.history_seeded_keys.clone();
            for transaction in &request.records {
                apply_transaction(
                    &self.database,
                    &mut batch,
                    history,
                    &mut known_keys,
                    &mut history_seeded_keys,
                    active.object_durable_version,
                    transaction,
                )?;
            }
            let mut advanced = NativeActiveImage {
                applied: request.end,
                records: u64::try_from(known_keys.len()).unwrap_or(u64::MAX),
                local_bytes: 0,
                ..active
            };
            batch.put_cf(metadata, b"active", encode_metadata(&advanced)?);
            apply_disposable_advance(&self.database, batch)?;
            advanced.local_bytes = database_directory_bytes(&self.database, &self.root)?;
            if advanced.local_bytes > self.max_local_bytes {
                return Err(format!(
                    "native resident engine uses {} bytes above its {} byte budget",
                    advanced.local_bytes, self.max_local_bytes
                ));
            }
            Ok((advanced, known_keys, history_seeded_keys))
        })();
        finish_transition(&self.transition_epoch, stable_epoch);
        match result {
            Ok((advanced, known_keys, history_seeded_keys)) => {
                state.known_keys = known_keys;
                state.history_seeded_keys = history_seeded_keys;
                state.active = Some(advanced.clone());
                Ok(native_receipt(&advanced))
            }
            Err(error) => {
                state.failed = true;
                Err(error)
            }
        }
    }

    fn snapshot(
        self: Arc<Self>,
        generation: u64,
        read_version: u64,
    ) -> Result<Box<dyn ResidentSnapshot>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "native resident engine state lock is poisoned".to_owned())?;
        if state.failed {
            return Err("native resident engine previously failed closed".to_owned());
        }
        let active = state
            .active
            .as_ref()
            .ok_or_else(|| "native resident engine is not active".to_owned())?;
        if generation != active.generation {
            return Err("native resident snapshot generation is stale".to_owned());
        }
        if active.applied.batch_order.is_some()
            || read_version < active.object_durable_version
            || read_version > active.applied.commit_version
        {
            return Err("native resident snapshot version is outside complete coverage".to_owned());
        }
        let binding = active.clone();
        let bound_epoch = self.transition_epoch.load(Ordering::Acquire);
        if bound_epoch % 2 != 0 {
            return Err("native resident engine is advancing".to_owned());
        }
        drop(state);
        Ok(Box::new(RocksDbResidentSnapshot {
            engine: self,
            binding,
            read_version,
            bound_epoch,
        }))
    }

    fn receipt(&self) -> Result<ResidentEngineReceipt, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "native resident engine state lock is poisoned".to_owned())?;
        if state.failed {
            return Err("native resident engine previously failed closed".to_owned());
        }
        state
            .active
            .as_ref()
            .map(native_receipt)
            .ok_or_else(|| "native resident engine is not active".to_owned())
    }
}

struct RocksDbResidentSnapshot {
    engine: Arc<RocksDbResidentRangeEngine>,
    binding: NativeActiveImage,
    read_version: u64,
    bound_epoch: u64,
}

impl Debug for RocksDbResidentSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RocksDbResidentSnapshot")
            .field("generation", &self.binding.generation)
            .field("read_version", &self.read_version)
            .field("bound_epoch", &self.bound_epoch)
            .finish_non_exhaustive()
    }
}

impl ResidentSnapshot for RocksDbResidentSnapshot {
    fn get(&self, key: &[u8]) -> Result<ReadOutcome, String> {
        if !self.binding.owned_range.contains(key) {
            return Ok(ReadOutcome::Absent);
        }
        if self.read_version == self.binding.applied.commit_version {
            let before = self.engine.transition_epoch.load(Ordering::Acquire);
            if before == self.bound_epoch && before % 2 == 0 {
                let outcome = self.engine.head_get(key)?;
                let after = self.engine.transition_epoch.load(Ordering::Acquire);
                if after == before {
                    return Ok(outcome);
                }
            }
        }
        self.engine
            .history_get(key, self.read_version, self.binding.object_durable_version)
    }
}

#[derive(Serialize)]
struct PersistedNativeMetadata<'a> {
    format_version: u16,
    generation: u64,
    object_root: &'a str,
    object_durable_version: u64,
    applied: StreamCursor,
    owned_range: &'a ResidentRangeBounds,
    object_first_key: &'a [u8],
    object_last_key: &'a [u8],
    records: u64,
}

fn encode_metadata(active: &NativeActiveImage) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&PersistedNativeMetadata {
        format_version: u16::try_from(RESIDENT_FORMAT_VERSION)
            .expect("resident format version fits persisted metadata"),
        generation: active.generation,
        object_root: &active.object_root,
        object_durable_version: active.object_durable_version,
        applied: active.applied,
        owned_range: &active.owned_range,
        object_first_key: &active.object_first_key,
        object_last_key: &active.object_last_key,
        records: active.records,
    })
    .map_err(|error| format!("encode native resident metadata: {error}"))
}

fn native_receipt(active: &NativeActiveImage) -> ResidentEngineReceipt {
    ResidentEngineReceipt {
        provider: RESIDENT_PROVIDER.to_owned(),
        generation: active.generation,
        object_root: active.object_root.clone(),
        object_durable_version: active.object_durable_version,
        applied: active.applied,
        owned_range: active.owned_range.clone(),
        object_first_key: active.object_first_key.clone(),
        object_last_key: active.object_last_key.clone(),
        records: active.records,
        local_bytes: active.local_bytes,
    }
}

fn validate_activation(request: &ResidentActivationRequest) -> Result<(), String> {
    if request.generation == 0
        || request.object_root.is_empty()
        || request.object_durable_version == 0
        || request.object_first_key.is_empty()
        || request.object_first_key > request.object_last_key
        || matches!(
            (&request.owned_range.start, &request.owned_range.end),
            (Some(start), Some(end)) if start >= end
        )
        || request.records.is_empty()
    {
        return Err("native resident activation requires complete non-zero metadata".to_owned());
    }
    let mut previous: Option<&[u8]> = None;
    for record in &request.records {
        if record.key.is_empty()
            || record.key < request.object_first_key
            || record.key > request.object_last_key
            || !request.owned_range.contains(&record.key)
            || previous.is_some_and(|key| key >= record.key.as_slice())
        {
            return Err("native resident activation records are not one ordered range".to_owned());
        }
        previous = Some(&record.key);
    }
    Ok(())
}

fn validate_advance(
    active: &NativeActiveImage,
    request: &ResidentAdvanceRequest,
) -> Result<(), String> {
    if request.generation != active.generation
        || request.start != active.applied
        || request.target_version < request.end.commit_version
    {
        return Err("native resident advancement generation or cursor is stale".to_owned());
    }
    if request.end.batch_order.is_none() && request.end.commit_version != request.target_version {
        return Err("complete native resident advancement must close at its target".to_owned());
    }
    let mut cursor = request.start;
    for record in &request.records {
        if !stamp_after_cursor(record.commit_version, record.batch_order, cursor)
            || record.commit_version > request.target_version
        {
            return Err("native resident transaction records regress or exceed target".to_owned());
        }
        cursor = StreamCursor {
            commit_version: record.commit_version,
            batch_order: Some(record.batch_order),
        };
    }
    if let Some(end_order) = request.end.batch_order {
        if cursor.commit_version != request.end.commit_version
            || cursor.batch_order != Some(end_order)
        {
            return Err(
                "partial native resident advancement does not end at its final record".to_owned(),
            );
        }
    } else if request.records.is_empty() && request.start != request.end {
        return Err("native resident advancement cannot move without retained records".to_owned());
    }
    Ok(())
}

fn stamp_after_cursor(commit_version: u64, batch_order: u16, cursor: StreamCursor) -> bool {
    commit_version > cursor.commit_version
        || (commit_version == cursor.commit_version
            && cursor.batch_order.is_some_and(|order| batch_order > order))
}

fn apply_transaction(
    database: &DB,
    batch: &mut WriteBatch,
    history: &impl AsColumnFamilyRef,
    known_keys: &mut BTreeSet<Vec<u8>>,
    history_seeded_keys: &mut BTreeSet<Vec<u8>>,
    object_durable_version: u64,
    transaction: &ResidentTransactionRecord,
) -> Result<(), String> {
    for (ordinal, mutation) in transaction.mutations.iter().enumerate() {
        let ordinal = u32::try_from(ordinal)
            .map_err(|_| "resident mutation ordinal exceeds u32".to_owned())?;
        let (key, value) = match mutation {
            ResidentMutation::Set { key, value } => (key, Some(value.as_slice())),
            ResidentMutation::Clear { key } => (key, None),
            ResidentMutation::ClearRange { range } => {
                let cleared = known_keys
                    .iter()
                    .filter(|key| range.contains(key))
                    .cloned()
                    .collect::<Vec<_>>();
                for key in cleared {
                    put_native_action(
                        database,
                        batch,
                        history,
                        history_seeded_keys,
                        object_durable_version,
                        NativeAction {
                            key: &key,
                            commit_version: transaction.commit_version,
                            batch_order: transaction.batch_order,
                            ordinal,
                            value: None,
                        },
                    )?;
                }
                continue;
            }
        };
        put_native_action(
            database,
            batch,
            history,
            history_seeded_keys,
            object_durable_version,
            NativeAction {
                key,
                commit_version: transaction.commit_version,
                batch_order: transaction.batch_order,
                ordinal,
                value,
            },
        )?;
        known_keys.insert(key.clone());
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct NativeAction<'a> {
    key: &'a [u8],
    commit_version: u64,
    batch_order: u16,
    ordinal: u32,
    value: Option<&'a [u8]>,
}

fn put_native_action(
    database: &DB,
    batch: &mut WriteBatch,
    history: &impl AsColumnFamilyRef,
    history_seeded_keys: &mut BTreeSet<Vec<u8>>,
    object_durable_version: u64,
    action: NativeAction<'_>,
) -> Result<(), String> {
    seed_history_if_needed(
        database,
        batch,
        history,
        history_seeded_keys,
        action.key,
        object_durable_version,
    )?;
    let encoded = encode_value(action.value);
    batch.put(action.key, &encoded);
    batch.put_cf(
        history,
        history_key(
            action.key,
            action.commit_version,
            action.batch_order,
            action.ordinal,
        )?,
        encoded,
    );
    Ok(())
}

fn seed_history_if_needed(
    database: &DB,
    batch: &mut WriteBatch,
    history: &impl AsColumnFamilyRef,
    history_seeded_keys: &mut BTreeSet<Vec<u8>>,
    key: &[u8],
    object_durable_version: u64,
) -> Result<(), String> {
    if !history_seeded_keys.insert(key.to_vec()) {
        return Ok(());
    }
    let outcome = database
        .get(key)
        .map_err(|error| format!("read native resident head before first mutation: {error}"))?
        .map_or(Ok(ReadOutcome::Absent), decode_owned_value)?;
    batch.put_cf(
        history,
        history_key(key, object_durable_version, 0, 0)?,
        encode_history_value(&outcome),
    );
    Ok(())
}

fn history_prefix(key: &[u8]) -> Result<Vec<u8>, String> {
    let length = u32::try_from(key.len()).map_err(|_| "resident key exceeds u32".to_owned())?;
    let mut encoded = Vec::with_capacity(4 + key.len());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(key);
    Ok(encoded)
}

fn history_key(
    key: &[u8],
    commit_version: u64,
    batch_order: u16,
    ordinal: u32,
) -> Result<Vec<u8>, String> {
    let mut encoded = history_prefix(key)?;
    encoded.extend_from_slice(&(!commit_version).to_be_bytes());
    encoded.extend_from_slice(&(!batch_order).to_be_bytes());
    encoded.extend_from_slice(&(!ordinal).to_be_bytes());
    Ok(encoded)
}

fn decode_history_commit(key: &[u8], prefix_len: usize) -> Result<u64, String> {
    let encoded = key
        .get(prefix_len..prefix_len.saturating_add(8))
        .ok_or_else(|| "resident history key is truncated".to_owned())?;
    let inverted = u64::from_be_bytes(
        encoded
            .try_into()
            .map_err(|_| "resident history version is truncated".to_owned())?,
    );
    Ok(!inverted)
}

fn point_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.optimize_for_point_lookup(128);
    options.set_max_open_files(256);
    options
}

/// Construct the exact measured point-read options shared by native and direct
/// T27 subjects.
#[must_use]
pub fn measured_point_options(block_cache: &Cache, direct_reads: bool) -> Options {
    let mut options = point_options();
    options.set_use_direct_reads(direct_reads);
    options.enable_statistics();
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(block_cache);
    options.set_block_based_table_factory(&table);
    options
}

fn uncached_metadata_options() -> Options {
    let mut options = point_options();
    let mut table = BlockBasedOptions::default();
    table.disable_cache();
    options.set_block_based_table_factory(&table);
    options
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn require_empty_root(root: &Path) -> Result<(), String> {
    if root.exists() {
        let mut entries =
            fs::read_dir(root).map_err(|error| format!("read resident root: {error}"))?;
        if entries
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("native resident engine requires an empty root".to_owned());
        }
    } else {
        fs::create_dir_all(root).map_err(|error| format!("create resident root: {error}"))?;
    }
    Ok(())
}

fn write_without_wal(database: &DB, batch: WriteBatch) -> Result<(), String> {
    let mut options = WriteOptions::default();
    options.disable_wal(true);
    database
        .write_opt(batch, &options)
        .map_err(|error| format!("write native resident batch: {error}"))
}

fn install_and_flush_base(database: &DB, batch: WriteBatch) -> Result<(), String> {
    write_without_wal(database, batch)?;
    database
        .flush()
        .map_err(|error| format!("flush native resident head: {error}"))?;
    let history = database
        .cf_handle(HISTORY_CF)
        .ok_or_else(|| "native resident history column family is missing".to_owned())?;
    database
        .flush_cf(history)
        .map_err(|error| format!("flush native resident history: {error}"))?;
    let metadata = database
        .cf_handle(METADATA_CF)
        .ok_or_else(|| "native resident metadata column family is missing".to_owned())?;
    database
        .flush_cf(metadata)
        .map_err(|error| format!("flush native resident metadata: {error}"))
}

fn apply_disposable_advance(database: &DB, batch: WriteBatch) -> Result<(), String> {
    // The replicated txLog, not this disposable RocksDB image, owns durability.
    // Keep the recent tail in RocksDB's mutable path and let its normal
    // memtable/compaction policy decide when to create another SST. Forcing a
    // flush after every catch-up page creates one extra L0 probe for nearly
    // every untouched point read.
    write_without_wal(database, batch)
}

fn begin_transition(epoch: &AtomicU64) -> Result<u64, String> {
    let stable = epoch.load(Ordering::Acquire);
    if stable % 2 != 0 || stable > u64::MAX - 2 {
        return Err("native resident transition epoch is unavailable".to_owned());
    }
    epoch.store(stable + 1, Ordering::Release);
    Ok(stable)
}

fn finish_transition(epoch: &AtomicU64, stable: u64) {
    epoch.store(stable + 2, Ordering::Release);
}

fn encode_value(value: Option<&[u8]>) -> Vec<u8> {
    value.map_or_else(
        || vec![TOMBSTONE_TAG],
        |value| {
            let mut encoded = Vec::with_capacity(value.len().saturating_add(1));
            encoded.extend_from_slice(value);
            encoded.push(VALUE_TAG);
            encoded
        },
    )
}

fn decode_value(value: &[u8]) -> Result<ReadOutcome, String> {
    match value.split_last() {
        Some((&TOMBSTONE_TAG, [])) => Ok(ReadOutcome::Tombstone),
        Some((&VALUE_TAG, value)) => Ok(ReadOutcome::Value(value.to_vec())),
        _ => Err("RocksDB serving image value framing is invalid".to_owned()),
    }
}

fn encode_history_value(outcome: &ReadOutcome) -> Vec<u8> {
    match outcome {
        ReadOutcome::Absent => vec![ABSENT_TAG],
        ReadOutcome::Tombstone => encode_value(None),
        ReadOutcome::Value(value) => encode_value(Some(value)),
    }
}

fn decode_history_value(value: &[u8]) -> Result<ReadOutcome, String> {
    match value {
        [ABSENT_TAG] => Ok(ReadOutcome::Absent),
        _ => decode_value(value),
    }
}

fn decode_owned_value(mut value: Vec<u8>) -> Result<ReadOutcome, String> {
    match value.pop() {
        Some(TOMBSTONE_TAG) if value.is_empty() => Ok(ReadOutcome::Tombstone),
        Some(VALUE_TAG) => Ok(ReadOutcome::Value(value)),
        _ => Err("RocksDB serving image value framing is invalid".to_owned()),
    }
}

fn directory_bytes(root: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("read RocksDB serving directory: {error}"))?
        {
            let entry = entry.map_err(|error| {
                format!(
                    "read RocksDB serving entry below {}: {error}",
                    directory.display()
                )
            })?;
            match directory_entry(&entry)? {
                DirectoryEntry::Missing | DirectoryEntry::Other => {}
                DirectoryEntry::Symlink => {
                    return Err("RocksDB serving root contains a symlink".to_owned());
                }
                DirectoryEntry::Directory(path) => pending.push(path),
                DirectoryEntry::File(bytes) => total = total.saturating_add(bytes),
            }
        }
    }
    Ok(total)
}

#[derive(Debug, Eq, PartialEq)]
enum DirectoryEntry {
    Missing,
    Symlink,
    Directory(PathBuf),
    File(u64),
    Other,
}

fn directory_entry(entry: &fs::DirEntry) -> Result<DirectoryEntry, String> {
    let entry_path = entry.path();
    let file_type = match entry.file_type() {
        Ok(file_type) => file_type,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DirectoryEntry::Missing);
        }
        Err(error) => {
            return Err(format!(
                "read RocksDB serving entry type {}: {error}",
                entry_path.display()
            ));
        }
    };
    if file_type.is_symlink() {
        return Ok(DirectoryEntry::Symlink);
    }
    if file_type.is_dir() {
        return Ok(DirectoryEntry::Directory(entry_path));
    }
    if file_type.is_file() {
        return match entry.metadata() {
            Ok(metadata) => Ok(DirectoryEntry::File(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(DirectoryEntry::Missing)
            }
            Err(error) => Err(format!(
                "read RocksDB serving entry metadata {}: {error}",
                entry_path.display()
            )),
        };
    }
    Ok(DirectoryEntry::Other)
}

fn database_directory_bytes(database: &DB, root: &Path) -> Result<u64, String> {
    database
        .disable_file_deletions()
        .map_err(|error| format!("pause RocksDB file deletion for byte accounting: {error}"))?;
    let measured = directory_bytes(root);
    let resumed = database
        .enable_file_deletions()
        .map_err(|error| format!("resume RocksDB file deletion after byte accounting: {error}"));
    match (measured, resumed) {
        (Ok(bytes), Ok(())) => Ok(bytes),
        (Err(measure_error), Ok(())) => Err(measure_error),
        (Ok(_), Err(resume_error)) => Err(resume_error),
        (Err(measure_error), Err(resume_error)) => Err(format!(
            "{measure_error}; additionally failed to resume file deletion: {resume_error}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_history_value, directory_entry, history_key, history_prefix, DirectoryEntry,
        RocksDbResidentRangeEngine, RocksDbServingImage, HISTORY_CF,
    };
    use okv::{
        ReadOutcome, ResidentActivationRequest, ResidentAdvanceRequest, ResidentMutation,
        ResidentRangeBounds, ResidentRangeEngine, ResidentTransactionRecord, ServingImage,
        ServingImageRecord, StreamCursor,
    };
    use std::{fs, sync::Arc};
    use tempfile::TempDir;

    #[test]
    fn byte_accounting_ignores_an_entry_deleted_after_directory_read() {
        let root = TempDir::new().expect("temporary accounting root");
        let transient = root.path().join("transient.log");
        fs::write(&transient, b"obsolete").expect("write transient entry");
        let entry = fs::read_dir(root.path())
            .expect("read accounting root")
            .next()
            .expect("transient entry")
            .expect("read transient entry");
        fs::remove_file(&transient).expect("remove transient entry");
        assert_eq!(
            directory_entry(&entry).expect("ignore an entry deleted during accounting"),
            DirectoryEntry::Missing
        );
    }

    #[test]
    fn activates_exact_values_tombstones_and_absence() {
        let root = TempDir::new().expect("temporary serving root");
        let mut image =
            RocksDbServingImage::open(root.path(), 32 * 1_024 * 1_024).expect("open empty image");
        let receipt = image
            .activate(
                7,
                11,
                vec![
                    ServingImageRecord {
                        key: b"a".to_vec(),
                        value: Some(b"a11".to_vec()),
                    },
                    ServingImageRecord {
                        key: b"b".to_vec(),
                        value: None,
                    },
                ],
            )
            .expect("activate image");
        assert_eq!(receipt.records, 2);
        assert_eq!(
            image.get(7, 11, b"a").expect("read value"),
            ReadOutcome::Value(b"a11".to_vec())
        );
        assert_eq!(
            image.get(7, 11, b"b").expect("read tombstone"),
            ReadOutcome::Tombstone
        );
        assert_eq!(
            image.get(7, 11, b"c").expect("read absence"),
            ReadOutcome::Absent
        );
    }

    #[test]
    fn stale_generation_fails_closed() {
        let root = TempDir::new().expect("temporary serving root");
        let mut image =
            RocksDbServingImage::open(root.path(), 32 * 1_024 * 1_024).expect("open empty image");
        image
            .activate(
                7,
                11,
                vec![ServingImageRecord {
                    key: b"a".to_vec(),
                    value: Some(b"a11".to_vec()),
                }],
            )
            .expect("activate image");
        assert!(image.get(8, 11, b"a").is_err());
    }

    fn native_engine() -> (TempDir, Arc<RocksDbResidentRangeEngine>) {
        let root = TempDir::new().expect("temporary native resident root");
        let engine = Arc::new(
            RocksDbResidentRangeEngine::open(root.path(), 64 * 1_024 * 1_024)
                .expect("open native resident engine"),
        );
        engine
            .activate(ResidentActivationRequest {
                generation: 7,
                object_root: "manifest/sha256/base".to_owned(),
                object_durable_version: 11,
                owned_range: ResidentRangeBounds::default(),
                object_first_key: b"a".to_vec(),
                object_last_key: b"z".to_vec(),
                records: vec![
                    ServingImageRecord {
                        key: b"a".to_vec(),
                        value: Some(b"a11".to_vec()),
                    },
                    ServingImageRecord {
                        key: b"k".to_vec(),
                        value: Some(b"k11".to_vec()),
                    },
                    ServingImageRecord {
                        key: b"z".to_vec(),
                        value: Some(b"z11".to_vec()),
                    },
                ],
            })
            .expect("activate native resident engine");
        (root, engine)
    }

    #[test]
    fn native_receipt_identifies_sparse_format_v2() {
        let (_root, engine) = native_engine();
        assert_eq!(
            engine.receipt().expect("read native receipt").provider,
            "rocksdb-11.8.1-native-resident-v2"
        );
    }

    #[test]
    fn native_activation_materializes_no_history_rows() {
        let (_root, engine) = native_engine();
        let history = engine
            .database
            .cf_handle(HISTORY_CF)
            .expect("history column family");
        let history_rows = engine
            .database
            .iterator_cf(history, rocksdb::IteratorMode::Start)
            .count();
        assert_eq!(history_rows, 0);
    }

    #[test]
    fn native_sparse_history_seeds_once_and_preserves_intermediate_versions() {
        let (_root, engine) = native_engine();
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![
                        ResidentMutation::Set {
                            key: b"a".to_vec(),
                            value: b"a12".to_vec(),
                        },
                        ResidentMutation::Set {
                            key: b"zz".to_vec(),
                            value: b"zz12".to_vec(),
                        },
                    ],
                }],
            })
            .expect("apply first sparse-history version");
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(12),
                end: StreamCursor::after_complete_version(13),
                target_version: 13,
                records: vec![ResidentTransactionRecord {
                    commit_version: 13,
                    batch_order: 0,
                    mutations: vec![
                        ResidentMutation::Set {
                            key: b"a".to_vec(),
                            value: b"a13".to_vec(),
                        },
                        ResidentMutation::Clear {
                            key: b"zz".to_vec(),
                        },
                    ],
                }],
            })
            .expect("apply second sparse-history version");

        let at_11 = engine.clone().snapshot(7, 11).expect("snapshot at O");
        let at_12 = engine.clone().snapshot(7, 12).expect("snapshot at 12");
        let at_13 = engine.clone().snapshot(7, 13).expect("snapshot at 13");
        assert_eq!(
            at_11.get(b"a").expect("a at O"),
            ReadOutcome::Value(b"a11".to_vec())
        );
        assert_eq!(at_11.get(b"zz").expect("zz at O"), ReadOutcome::Absent);
        assert_eq!(
            at_12.get(b"a").expect("a at 12"),
            ReadOutcome::Value(b"a12".to_vec())
        );
        assert_eq!(
            at_12.get(b"zz").expect("zz at 12"),
            ReadOutcome::Value(b"zz12".to_vec())
        );
        assert_eq!(
            at_13.get(b"a").expect("a at 13"),
            ReadOutcome::Value(b"a13".to_vec())
        );
        assert_eq!(at_13.get(b"zz").expect("zz at 13"), ReadOutcome::Tombstone);

        let history = engine
            .database
            .cf_handle(HISTORY_CF)
            .expect("history column family");
        for key in [b"a".as_slice(), b"zz".as_slice()] {
            let prefix = history_prefix(key).expect("history prefix");
            let rows = engine
                .database
                .iterator_cf(
                    history,
                    rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                )
                .map_while(Result::ok)
                .take_while(|(encoded_key, _)| encoded_key.starts_with(&prefix))
                .count();
            assert_eq!(rows, 3, "one frontier seed plus two mutations");
        }
    }

    #[test]
    fn resident_history_format_fixtures_remain_compatible() {
        let v1: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/resident-history-v1.json"))
                .expect("parse resident history v1 fixture");
        let v2: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/resident-history-v2.json"))
                .expect("parse resident history v2 fixture");
        assert_eq!(v1["format_version"], 1);
        assert_eq!(v2["format_version"], 2);
        assert_eq!(
            history_key(b"a", 11, 0, 0).expect("encode v1 history key"),
            decode_hex(v1["history_key_hex"].as_str().expect("v1 key hex"))
        );
        assert_eq!(
            decode_history_value(&decode_hex(
                v1["value_outcome_hex"].as_str().expect("v1 value hex")
            ))
            .expect("decode v1 value"),
            ReadOutcome::Value(decode_hex(
                v1["value_hex"].as_str().expect("v1 raw value hex")
            ))
        );
        assert_eq!(
            decode_history_value(&decode_hex(
                v1["tombstone_outcome_hex"]
                    .as_str()
                    .expect("v1 tombstone hex")
            ))
            .expect("decode v1 tombstone"),
            ReadOutcome::Tombstone
        );
        assert_eq!(
            decode_history_value(&decode_hex(
                v2["absent_outcome_hex"].as_str().expect("v2 absence hex")
            ))
            .expect("decode v2 absence"),
            ReadOutcome::Absent
        );
    }

    #[test]
    fn changed_key_without_frontier_seed_fails_closed() {
        let (_root, engine) = native_engine();
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![ResidentMutation::Set {
                        key: b"a".to_vec(),
                        value: b"a12".to_vec(),
                    }],
                }],
            })
            .expect("apply changed key");
        let history = engine
            .database
            .cf_handle(HISTORY_CF)
            .expect("history column family");
        engine
            .database
            .delete_cf(
                history,
                history_key(b"a", 11, 0, 0).expect("frontier seed key"),
            )
            .expect("remove frontier seed to simulate corruption");

        let at_11 = engine.clone().snapshot(7, 11).expect("snapshot at O");
        let error = at_11
            .get(b"a")
            .expect_err("changed key without frontier seed must fail");
        assert!(error.contains("object-frontier seed"));
    }

    #[test]
    fn first_touch_preserves_base_tombstones_and_range_clear_history() {
        let root = TempDir::new().expect("temporary native resident root");
        let engine = Arc::new(
            RocksDbResidentRangeEngine::open(root.path(), 64 * 1_024 * 1_024)
                .expect("open native resident engine"),
        );
        engine
            .activate(ResidentActivationRequest {
                generation: 7,
                object_root: "manifest/sha256/tombstone-range".to_owned(),
                object_durable_version: 11,
                owned_range: ResidentRangeBounds::default(),
                object_first_key: b"a".to_vec(),
                object_last_key: b"z".to_vec(),
                records: vec![
                    ServingImageRecord {
                        key: b"a".to_vec(),
                        value: Some(b"a11".to_vec()),
                    },
                    ServingImageRecord {
                        key: b"b".to_vec(),
                        value: None,
                    },
                    ServingImageRecord {
                        key: b"k".to_vec(),
                        value: Some(b"k11".to_vec()),
                    },
                    ServingImageRecord {
                        key: b"z".to_vec(),
                        value: Some(b"z11".to_vec()),
                    },
                ],
            })
            .expect("activate tombstone fixture");
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![
                        ResidentMutation::ClearRange {
                            range: okv::ResidentKeyRange {
                                start: b"a".to_vec(),
                                end: b"z".to_vec(),
                            },
                        },
                        ResidentMutation::Set {
                            key: b"b".to_vec(),
                            value: b"b12".to_vec(),
                        },
                    ],
                }],
            })
            .expect("apply range clear and replacement");

        let at_11 = engine.clone().snapshot(7, 11).expect("snapshot at O");
        let at_12 = engine.clone().snapshot(7, 12).expect("snapshot at 12");
        assert_eq!(
            at_11.get(b"a").expect("a at O"),
            ReadOutcome::Value(b"a11".to_vec())
        );
        assert_eq!(at_11.get(b"b").expect("b at O"), ReadOutcome::Tombstone);
        assert_eq!(
            at_11.get(b"k").expect("k at O"),
            ReadOutcome::Value(b"k11".to_vec())
        );
        assert_eq!(at_12.get(b"a").expect("a at 12"), ReadOutcome::Tombstone);
        assert_eq!(
            at_12.get(b"b").expect("b at 12"),
            ReadOutcome::Value(b"b12".to_vec())
        );
        assert_eq!(at_12.get(b"k").expect("k at 12"), ReadOutcome::Tombstone);
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0, "hex fixture must contain whole bytes");
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("hex fixture is UTF-8");
                u8::from_str_radix(pair, 16).expect("hex fixture contains one byte")
            })
            .collect()
    }

    #[test]
    fn native_engine_exposes_explicit_cache_and_read_counters() {
        let root = TempDir::new().expect("temporary native resident root");
        let engine = Arc::new(
            RocksDbResidentRangeEngine::open_with_block_cache(
                root.path(),
                64 * 1_024 * 1_024,
                2 * 1_024 * 1_024,
            )
            .expect("open native resident engine with explicit cache"),
        );
        engine
            .activate(ResidentActivationRequest {
                generation: 7,
                object_root: "manifest/sha256/cache-metrics".to_owned(),
                object_durable_version: 11,
                owned_range: ResidentRangeBounds::default(),
                object_first_key: b"a".to_vec(),
                object_last_key: b"z".to_vec(),
                records: vec![ServingImageRecord {
                    key: b"a".to_vec(),
                    value: Some(vec![42; 4_096]),
                }],
            })
            .expect("activate native resident engine");
        let before = engine.metrics();
        let snapshot = engine.clone().snapshot(7, 11).expect("bind snapshot");
        for _ in 0..32 {
            assert_eq!(
                snapshot.get(b"a").expect("read measured value"),
                ReadOutcome::Value(vec![42; 4_096])
            );
        }
        let after = engine.metrics();
        assert_eq!(after.database_count, 1);
        assert_eq!(after.block_cache_count, 1);
        assert_eq!(after.implicit_block_cache_count, 0);
        assert_eq!(after.column_family_count, 3);
        assert!(after.metadata_cache_disabled);
        assert_eq!(after.block_cache_capacity_bytes, 2 * 1_024 * 1_024);
        assert!(!after.direct_reads);
        assert!(after.block_cache_usage_bytes <= after.block_cache_capacity_bytes);
        assert!(after.block_cache_hits >= before.block_cache_hits);
        assert!(after.block_cache_misses >= before.block_cache_misses);
        assert!(after.bytes_read >= before.bytes_read);
        engine
            .reset_block_cache()
            .expect("evict measured block cache");
        let reset = engine.metrics();
        assert!(reset.block_cache_usage_bytes <= reset.block_cache_pinned_usage_bytes);
    }

    #[test]
    fn native_engine_exposes_direct_read_page_cache_treatment() {
        let root = TempDir::new().expect("temporary native resident root");
        let engine = RocksDbResidentRangeEngine::open_with_block_cache_and_direct_reads(
            root.path(),
            64 * 1_024 * 1_024,
            2 * 1_024 * 1_024,
            true,
        )
        .expect("open native resident engine with direct reads");

        assert!(engine.metrics().direct_reads);
    }

    #[test]
    fn native_engine_rejects_zero_cache_budget() {
        let root = TempDir::new().expect("temporary native resident root");
        assert!(RocksDbResidentRangeEngine::open_with_block_cache(
            root.path(),
            64 * 1_024 * 1_024,
            0,
        )
        .is_err());
    }

    #[test]
    fn native_snapshot_remains_exact_after_live_advancement() {
        let (_root, engine) = native_engine();
        let old = engine.clone().snapshot(7, 11).expect("bind old snapshot");
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![
                        ResidentMutation::Set {
                            key: b"a".to_vec(),
                            value: b"a12".to_vec(),
                        },
                        ResidentMutation::Clear { key: b"k".to_vec() },
                        ResidentMutation::Set {
                            key: b"zz".to_vec(),
                            value: b"tail-insert".to_vec(),
                        },
                    ],
                }],
            })
            .expect("advance native resident engine");
        let current = engine
            .clone()
            .snapshot(7, 12)
            .expect("bind current snapshot");
        assert_eq!(
            current.get(b"a").expect("read current value"),
            ReadOutcome::Value(b"a12".to_vec())
        );
        assert_eq!(
            current.get(b"k").expect("read current tombstone"),
            ReadOutcome::Tombstone
        );
        assert_eq!(
            current.get(b"zz").expect("read tail insert"),
            ReadOutcome::Value(b"tail-insert".to_vec())
        );
        assert_eq!(
            old.get(b"a").expect("read pinned old value"),
            ReadOutcome::Value(b"a11".to_vec())
        );
        assert_eq!(
            old.get(b"k").expect("read pinned old value"),
            ReadOutcome::Value(b"k11".to_vec())
        );
        assert_eq!(
            old.get(b"zz").expect("read old tail absence"),
            ReadOutcome::Absent
        );
    }

    #[test]
    fn latest_snapshot_avoids_one_sst_probe_per_read_after_small_tail() {
        const READS: u64 = 256;

        let root = TempDir::new().expect("temporary native resident root");
        let engine = Arc::new(
            RocksDbResidentRangeEngine::open_with_block_cache(
                root.path(),
                64 * 1_024 * 1_024,
                2 * 1_024 * 1_024,
            )
            .expect("open native resident engine"),
        );
        let records = (0..1_024)
            .map(|index| ServingImageRecord {
                key: format!("k/{index:04}").into_bytes(),
                value: Some(vec![42; 1_024]),
            })
            .collect::<Vec<_>>();
        engine
            .activate(ResidentActivationRequest {
                generation: 7,
                object_root: "manifest/sha256/read-amplification".to_owned(),
                object_durable_version: 11,
                owned_range: ResidentRangeBounds::default(),
                object_first_key: b"k/0000".to_vec(),
                object_last_key: b"k/1023".to_vec(),
                records,
            })
            .expect("activate native resident engine");
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![ResidentMutation::Set {
                        key: b"k/0000".to_vec(),
                        value: vec![43; 1_024],
                    }],
                }],
            })
            .expect("advance native resident engine");
        let snapshot = engine
            .clone()
            .snapshot(7, 12)
            .expect("bind current snapshot");
        for _ in 0..128 {
            assert_eq!(
                snapshot.get(b"k/0512").expect("warm resident read"),
                ReadOutcome::Value(vec![42; 1_024])
            );
        }
        let before = engine.metrics();
        for _ in 0..READS {
            assert_eq!(
                snapshot.get(b"k/0512").expect("measured resident read"),
                ReadOutcome::Value(vec![42; 1_024])
            );
        }
        let after = engine.metrics();
        let cache_lookups = after
            .block_cache_hits
            .saturating_sub(before.block_cache_hits)
            .saturating_add(
                after
                    .block_cache_misses
                    .saturating_sub(before.block_cache_misses),
            );
        assert_eq!(cache_lookups, READS);
    }

    #[test]
    fn native_advancement_preserves_batch_and_mutation_order() {
        let (_root, engine) = native_engine();
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor::after_complete_version(11),
                end: StreamCursor {
                    commit_version: 12,
                    batch_order: Some(0),
                },
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 0,
                    mutations: vec![ResidentMutation::Set {
                        key: b"k".to_vec(),
                        value: b"first".to_vec(),
                    }],
                }],
            })
            .expect("apply first transaction page");
        engine
            .advance(ResidentAdvanceRequest {
                generation: 7,
                start: StreamCursor {
                    commit_version: 12,
                    batch_order: Some(0),
                },
                end: StreamCursor::after_complete_version(12),
                target_version: 12,
                records: vec![ResidentTransactionRecord {
                    commit_version: 12,
                    batch_order: 1,
                    mutations: vec![
                        ResidentMutation::ClearRange {
                            range: okv::ResidentKeyRange {
                                start: b"a".to_vec(),
                                end: b"z".to_vec(),
                            },
                        },
                        ResidentMutation::Set {
                            key: b"k".to_vec(),
                            value: b"after".to_vec(),
                        },
                    ],
                }],
            })
            .expect("apply second transaction page");
        let snapshot = engine
            .clone()
            .snapshot(7, 12)
            .expect("bind complete batch snapshot");
        assert_eq!(
            snapshot.get(b"a").expect("read range tombstone"),
            ReadOutcome::Tombstone
        );
        assert_eq!(
            snapshot.get(b"k").expect("read point after clear"),
            ReadOutcome::Value(b"after".to_vec())
        );
        assert!(engine.clone().snapshot(8, 12).is_err());
    }
}
