//! Disposable `RocksDB` point-serving image for objectKV.

use okv::{
    ReadOutcome, ResidentActivationRequest, ResidentAdvanceRequest, ResidentEngineReceipt,
    ResidentMutation, ResidentRangeBounds, ResidentRangeEngine, ResidentSnapshot,
    ResidentTransactionRecord, ServingImage, ServingImageReceipt, ServingImageRecord, StreamCursor,
};
use rocksdb::{
    AsColumnFamilyRef, ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions, DB,
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
const INSTALL_BATCH_RECORDS: usize = 4_096;

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
const RESIDENT_PROVIDER: &str = "rocksdb-11.8.1-native-resident-v1";

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
    failed: bool,
}

/// `RocksDB` implementation of the transition-verified resident data plane.
pub struct RocksDbResidentRangeEngine {
    database: DB,
    root: PathBuf,
    max_local_bytes: u64,
    state: Mutex<NativeEngineState>,
    transition_epoch: AtomicU64,
}

impl Debug for RocksDbResidentRangeEngine {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RocksDbResidentRangeEngine")
            .field("root", &self.root)
            .field("max_local_bytes", &self.max_local_bytes)
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
        if max_local_bytes == 0 {
            return Err("native resident engine requires a positive local-byte budget".to_owned());
        }
        require_empty_root(root)?;
        let mut database_options = point_options();
        database_options.create_missing_column_families(true);
        let database = DB::open_cf_descriptors(
            &database_options,
            root,
            [
                ColumnFamilyDescriptor::new("default", point_options()),
                ColumnFamilyDescriptor::new(HISTORY_CF, point_options()),
                ColumnFamilyDescriptor::new(METADATA_CF, Options::default()),
            ],
        )
        .map_err(|error| format!("open native resident RocksDB: {error}"))?;
        Ok(Self {
            database,
            root: root.to_path_buf(),
            max_local_bytes,
            state: Mutex::new(NativeEngineState::default()),
            transition_epoch: AtomicU64::new(0),
        })
    }

    fn history_get(&self, key: &[u8], read_version: u64) -> Result<ReadOutcome, String> {
        let history = self
            .database
            .cf_handle(HISTORY_CF)
            .ok_or_else(|| "native resident history column family is absent".to_owned())?;
        let prefix = history_prefix(key)?;
        let iterator = self
            .database
            .iterator_cf(history, IteratorMode::From(&prefix, Direction::Forward));
        for item in iterator {
            let (encoded_key, encoded_value) =
                item.map_err(|error| format!("read resident history: {error}"))?;
            if !encoded_key.starts_with(&prefix) {
                break;
            }
            let commit_version = decode_history_commit(&encoded_key, prefix.len())?;
            if commit_version <= read_version {
                return decode_value(&encoded_value);
            }
        }
        Ok(ReadOutcome::Absent)
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
            let history = self
                .database
                .cf_handle(HISTORY_CF)
                .ok_or_else(|| "native resident history column family is absent".to_owned())?;
            let metadata = self
                .database
                .cf_handle(METADATA_CF)
                .ok_or_else(|| "native resident metadata column family is absent".to_owned())?;
            let mut batch = WriteBatch::default();
            let mut known_keys = BTreeSet::new();
            for record in &request.records {
                let encoded = encode_value(record.value.as_deref());
                batch.put(&record.key, &encoded);
                batch.put_cf(
                    history,
                    history_key(&record.key, request.object_durable_version, 0, 0)?,
                    encoded,
                );
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
            write_and_flush(&self.database, batch)?;
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
            for transaction in &request.records {
                apply_transaction(&mut batch, history, &mut known_keys, transaction)?;
            }
            let mut advanced = NativeActiveImage {
                applied: request.end,
                records: u64::try_from(known_keys.len()).unwrap_or(u64::MAX),
                local_bytes: 0,
                ..active
            };
            batch.put_cf(metadata, b"active", encode_metadata(&advanced)?);
            write_and_flush(&self.database, batch)?;
            advanced.local_bytes = database_directory_bytes(&self.database, &self.root)?;
            if advanced.local_bytes > self.max_local_bytes {
                return Err(format!(
                    "native resident engine uses {} bytes above its {} byte budget",
                    advanced.local_bytes, self.max_local_bytes
                ));
            }
            Ok((advanced, known_keys))
        })();
        finish_transition(&self.transition_epoch, stable_epoch);
        match result {
            Ok((advanced, known_keys)) => {
                state.known_keys = known_keys;
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
        self.engine.history_get(key, self.read_version)
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
        format_version: 1,
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
    batch: &mut WriteBatch,
    history: &impl AsColumnFamilyRef,
    known_keys: &mut BTreeSet<Vec<u8>>,
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
                        batch,
                        history,
                        &key,
                        transaction.commit_version,
                        transaction.batch_order,
                        ordinal,
                        None,
                    )?;
                }
                continue;
            }
        };
        put_native_action(
            batch,
            history,
            key,
            transaction.commit_version,
            transaction.batch_order,
            ordinal,
            value,
        )?;
        known_keys.insert(key.clone());
    }
    Ok(())
}

fn put_native_action(
    batch: &mut WriteBatch,
    history: &impl AsColumnFamilyRef,
    key: &[u8],
    commit_version: u64,
    batch_order: u16,
    ordinal: u32,
    value: Option<&[u8]>,
) -> Result<(), String> {
    let encoded = encode_value(value);
    batch.put(key, &encoded);
    batch.put_cf(
        history,
        history_key(key, commit_version, batch_order, ordinal)?,
        encoded,
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

fn write_and_flush(database: &DB, batch: WriteBatch) -> Result<(), String> {
    let mut options = WriteOptions::default();
    options.disable_wal(true);
    database
        .write_opt(batch, &options)
        .map_err(|error| format!("write native resident batch: {error}"))?;
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
            let entry = entry.map_err(|error| error.to_string())?;
            let file_type = entry.file_type().map_err(|error| error.to_string())?;
            if file_type.is_symlink() {
                return Err("RocksDB serving root contains a symlink".to_owned());
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                total = total
                    .saturating_add(entry.metadata().map_err(|error| error.to_string())?.len());
            }
        }
    }
    Ok(total)
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
    use super::{RocksDbResidentRangeEngine, RocksDbServingImage};
    use okv::{
        ReadOutcome, ResidentActivationRequest, ResidentAdvanceRequest, ResidentMutation,
        ResidentRangeBounds, ResidentRangeEngine, ResidentTransactionRecord, ServingImage,
        ServingImageRecord, StreamCursor,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

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
