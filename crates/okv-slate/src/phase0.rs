use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult, UploadPart,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use slatedb::{Db, WriteBatch};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::SLATEDB_REVISION;

const STORE_KIND: &str = "filesystem";

/// Fixed inputs for the `SlateDB` Phase 0 filesystem incumbent.
#[derive(Clone, Debug)]
pub struct Phase0Config {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub point_reads_per_seed: usize,
    pub scan_rows_per_seed: usize,
    pub seeds: Vec<u64>,
}

/// Correct execution or the suite's deliberate cache-state poison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0Mode {
    Correct,
    ReuseWarmDbForReopen,
}

/// One hard-gate observation from the baseline.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0Gate {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

/// Object-store calls and bytes observed during one phase.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Phase0IoDelta {
    pub successful_requests: BTreeMap<String, u64>,
    pub failed_requests: BTreeMap<String, u64>,
    pub read_bytes: BTreeMap<String, u64>,
    pub written_bytes: BTreeMap<String, u64>,
}

impl Phase0IoDelta {
    #[must_use]
    pub fn request_total(&self) -> u64 {
        self.successful_requests
            .values()
            .chain(self.failed_requests.values())
            .sum()
    }

    #[must_use]
    pub fn read_byte_total(&self) -> u64 {
        self.read_bytes.values().sum()
    }

    #[must_use]
    pub fn written_byte_total(&self) -> u64 {
        self.written_bytes.values().sum()
    }
}

/// Timings and backend I/O for one logical baseline phase.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0PhaseReport {
    pub phase: String,
    pub logical_operations: u64,
    pub elapsed_seconds: f64,
    pub io: Phase0IoDelta,
}

/// Evidence produced by one deterministic seed.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0SeedReport {
    pub seed: u64,
    pub total_io: Phase0IoDelta,
    pub ingest: Phase0PhaseReport,
    pub warm_point: Phase0PhaseReport,
    pub ordered_scan: Phase0PhaseReport,
    pub reopen_first_correct_read_seconds: f64,
    pub reopen: Phase0PhaseReport,
    pub cold_point: Phase0PhaseReport,
}

/// Full frozen-contract report returned to `okv-eval`.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0Report {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub receipt_digest: String,
    pub repeated_receipt_digest: String,
    pub seeds: Vec<Phase0SeedReport>,
    pub gates: Vec<Phase0Gate>,
}

impl Phase0Report {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
}

/// Execute RFC-0021 against a fresh local filesystem object store.
///
/// # Errors
///
/// Returns an error when `SlateDB` or the local object-store setup cannot
/// complete. Logical and cache-state violations are returned as failed gates.
pub async fn run_phase0_filesystem_contract(
    config: &Phase0Config,
    mode: Phase0Mode,
) -> Result<Phase0Report, String> {
    validate_config(config)?;
    let receipt_digest = oracle_receipt(config);
    let repeated_receipt_digest = oracle_receipt(config);
    let mut reports = Vec::with_capacity(config.seeds.len());
    let mut exact_dataset_after_flush = true;
    let mut warm_point_reads_exact = true;
    let mut ordered_scan_exact = true;
    let mut cold_point_reads_exact = true;
    let mut empty_cache_reopen_exact = true;
    let mut object_io_accounted = true;

    for seed in &config.seeds {
        let outcome = run_seed(config, *seed, mode).await?;
        exact_dataset_after_flush &= outcome.check("exact_dataset_after_flush");
        warm_point_reads_exact &= outcome.check("warm_point_reads_exact");
        ordered_scan_exact &= outcome.check("ordered_scan_exact");
        cold_point_reads_exact &= outcome.check("cold_point_reads_exact");
        empty_cache_reopen_exact &= outcome.check("empty_cache_reopen_exact");
        object_io_accounted &= outcome.check("object_io_accounted");
        reports.push(outcome.report);
    }

    let fresh_db_cache_on_reopen = mode == Phase0Mode::Correct;
    let deterministic_oracle_digest_repeated = receipt_digest == repeated_receipt_digest;
    let gates = vec![
        gate(
            "exact_dataset_after_flush",
            exact_dataset_after_flush,
            "fixed post-flush point samples equal the independent dataset oracle",
        ),
        gate(
            "warm_point_reads_exact",
            warm_point_reads_exact,
            "warm point reads equal the independent dataset oracle",
        ),
        gate(
            "ordered_scan_exact",
            ordered_scan_exact,
            "the bounded scan is exact, ordered, and complete",
        ),
        gate(
            "cold_point_reads_exact",
            cold_point_reads_exact,
            "point reads after cache replacement equal the dataset oracle",
        ),
        gate(
            "empty_cache_reopen_exact",
            empty_cache_reopen_exact,
            "the first read after reopen returns the exact expected value",
        ),
        gate(
            "fresh_db_cache_on_reopen",
            fresh_db_cache_on_reopen,
            if fresh_db_cache_on_reopen {
                "the timed reopen used a newly constructed SlateDB instance"
            } else {
                "negative control reused the warm SlateDB instance"
            },
        ),
        gate(
            "object_io_accounted",
            object_io_accounted,
            "backend writes and reads produced request and byte evidence",
        ),
        gate(
            "deterministic_oracle_digest_repeated",
            deterministic_oracle_digest_repeated,
            "two independent oracle passes produced the same logical receipt",
        ),
    ];

    Ok(Phase0Report {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0Mode::Correct => "correct",
            Phase0Mode::ReuseWarmDbForReopen => "reuse_warm_db_for_reopen",
        }
        .to_owned(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        receipt_digest,
        repeated_receipt_digest,
        seeds: reports,
        gates,
    })
}

struct SeedOutcome {
    report: Phase0SeedReport,
    checks: BTreeMap<&'static str, bool>,
}

impl SeedOutcome {
    fn check(&self, id: &'static str) -> bool {
        self.checks.get(id).copied().unwrap_or(false)
    }
}

#[allow(clippy::too_many_lines)]
async fn run_seed(
    config: &Phase0Config,
    seed: u64,
    mode: Phase0Mode,
) -> Result<SeedOutcome, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-slate-")
        .tempdir()
        .map_err(|error| format!("create Phase 0 root: {error}"))?;
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open local object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("seed-{seed:016x}");
    let db = Db::builder(db_path.as_str(), Arc::clone(&store))
        .build()
        .await
        .map_err(|error| format!("open SlateDB seed {seed}: {error}"))?;

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let mut batch = WriteBatch::new();
    for ordinal in 0..config.key_count {
        batch.put(
            key_for(seed, ordinal),
            value_for(config.logical_bytes, config.key_count, seed, ordinal),
        );
    }
    db.write(batch)
        .await
        .map_err(|error| format!("write seed {seed}: {error}"))?;
    db.flush()
        .await
        .map_err(|error| format!("flush seed {seed}: {error}"))?;
    let ingest_elapsed = ingest_started.elapsed().as_secs_f64();
    let ingest_io = counters.snapshot().difference(&before_ingest);
    let sample_ordinals = point_ordinals(config, seed);
    let exact_dataset_after_flush = check_points(&db, config, seed, &sample_ordinals).await?;

    check_points(&db, config, seed, &sample_ordinals).await?;
    let before_warm = counters.snapshot();
    let warm_started = Instant::now();
    let warm_point_reads_exact = check_points(&db, config, seed, &sample_ordinals).await?;
    let warm_elapsed = warm_started.elapsed().as_secs_f64();
    let warm_io = counters.snapshot().difference(&before_warm);

    let scan_start = config.key_count / 3;
    let scan_count = u64::try_from(config.scan_rows_per_seed)
        .map_err(|error| format!("scan row count does not fit u64: {error}"))?
        .min(config.key_count - scan_start);
    let before_scan = counters.snapshot();
    let scan_started = Instant::now();
    let ordered_scan_exact = check_scan(&db, config, seed, scan_start, scan_count).await?;
    let scan_elapsed = scan_started.elapsed().as_secs_f64();
    let scan_io = counters.snapshot().difference(&before_scan);

    let first_ordinal = sample_ordinals[0];
    let first_key = key_for(seed, first_ordinal);
    let first_value = value_for(config.logical_bytes, config.key_count, seed, first_ordinal);
    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let active_db = if mode == Phase0Mode::Correct {
        db.close()
            .await
            .map_err(|error| format!("close SlateDB seed {seed}: {error}"))?;
        Db::builder(db_path.as_str(), Arc::clone(&store))
            .build()
            .await
            .map_err(|error| format!("reopen SlateDB seed {seed}: {error}"))?
    } else {
        db
    };
    let first_observed = active_db
        .get(&first_key)
        .await
        .map_err(|error| format!("first reopened read seed {seed}: {error}"))?;
    let empty_cache_reopen_exact = first_observed.as_deref() == Some(first_value.as_slice());
    let reopen_elapsed = reopen_started.elapsed().as_secs_f64();
    let reopen_io = counters.snapshot().difference(&before_reopen);

    let cold_ordinals = &sample_ordinals[1..];
    let before_cold = counters.snapshot();
    let cold_started = Instant::now();
    let cold_point_reads_exact = check_points(&active_db, config, seed, cold_ordinals).await?;
    let cold_elapsed = cold_started.elapsed().as_secs_f64();
    let cold_io = counters.snapshot().difference(&before_cold);
    active_db
        .close()
        .await
        .map_err(|error| format!("close reopened SlateDB seed {seed}: {error}"))?;
    let total_io = counters.snapshot().difference(&IoSnapshot::default());

    let read_io_accounted = (reopen_io.read_byte_total() + cold_io.read_byte_total()) > 0;
    let object_io_accounted = ingest_io.request_total() > 0
        && ingest_io.written_byte_total() > 0
        && (read_io_accounted || mode == Phase0Mode::ReuseWarmDbForReopen);
    Ok(SeedOutcome {
        report: Phase0SeedReport {
            seed,
            total_io,
            ingest: phase("ingest", config.key_count, ingest_elapsed, ingest_io),
            warm_point: phase(
                "warm-point",
                sample_ordinals.len() as u64,
                warm_elapsed,
                warm_io,
            ),
            ordered_scan: phase("ordered-scan", scan_count, scan_elapsed, scan_io),
            reopen_first_correct_read_seconds: reopen_elapsed,
            reopen: phase("reopen", 1, reopen_elapsed, reopen_io),
            cold_point: phase(
                "cold-point",
                cold_ordinals.len() as u64,
                cold_elapsed,
                cold_io,
            ),
        },
        checks: BTreeMap::from([
            ("exact_dataset_after_flush", exact_dataset_after_flush),
            ("warm_point_reads_exact", warm_point_reads_exact),
            ("ordered_scan_exact", ordered_scan_exact),
            ("cold_point_reads_exact", cold_point_reads_exact),
            ("empty_cache_reopen_exact", empty_cache_reopen_exact),
            ("object_io_accounted", object_io_accounted),
        ]),
    })
}

fn validate_config(config: &Phase0Config) -> Result<(), String> {
    if config.logical_bytes == 0 {
        return Err("Phase 0 logical_bytes must be greater than zero".to_owned());
    }
    if config.key_count < 2 {
        return Err("Phase 0 key_count must be at least two".to_owned());
    }
    if config.point_reads_per_seed < 2 {
        return Err("Phase 0 point_reads_per_seed must be at least two".to_owned());
    }
    if config.scan_rows_per_seed == 0 {
        return Err("Phase 0 scan_rows_per_seed must be greater than zero".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("Phase 0 requires at least one seed".to_owned());
    }
    Ok(())
}

fn gate(id: &str, passed: bool, detail: &str) -> Phase0Gate {
    Phase0Gate {
        id: id.to_owned(),
        passed,
        detail: detail.to_owned(),
    }
}

fn phase(
    name: &str,
    logical_operations: u64,
    elapsed_seconds: f64,
    io: Phase0IoDelta,
) -> Phase0PhaseReport {
    Phase0PhaseReport {
        phase: name.to_owned(),
        logical_operations,
        elapsed_seconds,
        io,
    }
}

async fn check_points(
    db: &Db,
    config: &Phase0Config,
    seed: u64,
    ordinals: &[u64],
) -> Result<bool, String> {
    for ordinal in ordinals {
        let observed = db
            .get(key_for(seed, *ordinal))
            .await
            .map_err(|error| format!("point read seed {seed} ordinal {ordinal}: {error}"))?;
        let expected = value_for(config.logical_bytes, config.key_count, seed, *ordinal);
        if observed.as_deref() != Some(expected.as_slice()) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn check_scan(
    db: &Db,
    config: &Phase0Config,
    seed: u64,
    start: u64,
    count: u64,
) -> Result<bool, String> {
    let end = start + count;
    let mut iterator = db
        .scan(key_for(seed, start)..key_for(seed, end))
        .await
        .map_err(|error| format!("scan seed {seed}: {error}"))?;
    for ordinal in start..end {
        let Some(row) = iterator
            .next()
            .await
            .map_err(|error| format!("scan next seed {seed}: {error}"))?
        else {
            return Ok(false);
        };
        if row.key.as_ref() != key_for(seed, ordinal).as_slice()
            || row.value.as_ref()
                != value_for(config.logical_bytes, config.key_count, seed, ordinal).as_slice()
        {
            return Ok(false);
        }
    }
    iterator
        .next()
        .await
        .map(|row| row.is_none())
        .map_err(|error| format!("scan exhaustion seed {seed}: {error}"))
}

fn point_ordinals(config: &Phase0Config, seed: u64) -> Vec<u64> {
    (0..config.point_reads_per_seed)
        .map(|index| {
            let index = u64::try_from(index).expect("usize fits u64 on supported targets");
            seed.wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(index.wrapping_mul(1_442_695_040_888_963_407))
                % config.key_count
        })
        .collect()
}

fn key_for(seed: u64, ordinal: u64) -> Vec<u8> {
    format!("k/{seed:016x}/{ordinal:016x}").into_bytes()
}

fn value_for(logical_bytes: u64, key_count: u64, seed: u64, ordinal: u64) -> Vec<u8> {
    let base = logical_bytes / key_count;
    let remainder = logical_bytes % key_count;
    let length = base + u64::from(ordinal < remainder);
    let length = usize::try_from(length).expect("configured value length fits usize");
    let mut value = Vec::with_capacity(length);
    let mut block = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"okv-phase0-value-v1");
        hasher.update(seed.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(block.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length - value.len();
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        block += 1;
    }
    value
}

fn oracle_receipt(config: &Phase0Config) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-phase0-filesystem-receipt-v1");
    hasher.update(SLATEDB_REVISION.as_bytes());
    hasher.update(config.logical_bytes.to_be_bytes());
    hasher.update(config.key_count.to_be_bytes());
    hasher.update(config.point_reads_per_seed.to_be_bytes());
    hasher.update(config.scan_rows_per_seed.to_be_bytes());
    for seed in &config.seeds {
        hasher.update(seed.to_be_bytes());
        for ordinal in 0..config.key_count {
            hasher.update(key_for(*seed, ordinal));
            hasher.update(value_for(
                config.logical_bytes,
                config.key_count,
                *seed,
                ordinal,
            ));
        }
        for ordinal in point_ordinals(config, *seed) {
            hasher.update(ordinal.to_be_bytes());
        }
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Clone, Debug, Default)]
struct IoSnapshot {
    successful_requests: BTreeMap<String, u64>,
    failed_requests: BTreeMap<String, u64>,
    read_bytes: BTreeMap<String, u64>,
    written_bytes: BTreeMap<String, u64>,
}

impl IoSnapshot {
    fn difference(&self, earlier: &Self) -> Phase0IoDelta {
        Phase0IoDelta {
            successful_requests: subtract_maps(
                &self.successful_requests,
                &earlier.successful_requests,
            ),
            failed_requests: subtract_maps(&self.failed_requests, &earlier.failed_requests),
            read_bytes: subtract_maps(&self.read_bytes, &earlier.read_bytes),
            written_bytes: subtract_maps(&self.written_bytes, &earlier.written_bytes),
        }
    }
}

fn subtract_maps(
    current: &BTreeMap<String, u64>,
    earlier: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    current
        .iter()
        .filter_map(|(key, value)| {
            let delta = value.saturating_sub(*earlier.get(key).unwrap_or(&0));
            (delta > 0).then(|| (key.clone(), delta))
        })
        .collect()
}

#[derive(Debug, Default)]
struct IoCounters {
    snapshot: Mutex<IoSnapshot>,
}

impl IoCounters {
    fn snapshot(&self) -> IoSnapshot {
        self.snapshot
            .lock()
            .expect("I/O counter lock poisoned")
            .clone()
    }

    fn request(&self, api: &str, succeeded: bool) {
        let mut snapshot = self.snapshot.lock().expect("I/O counter lock poisoned");
        let requests = if succeeded {
            &mut snapshot.successful_requests
        } else {
            &mut snapshot.failed_requests
        };
        *requests.entry(api.to_owned()).or_default() += 1;
    }

    fn bytes_read(&self, api: &str, bytes: u64) {
        let mut snapshot = self.snapshot.lock().expect("I/O counter lock poisoned");
        *snapshot.read_bytes.entry(api.to_owned()).or_default() += bytes;
    }

    fn bytes_written(&self, api: &str, bytes: u64) {
        let mut snapshot = self.snapshot.lock().expect("I/O counter lock poisoned");
        *snapshot.written_bytes.entry(api.to_owned()).or_default() += bytes;
    }
}

struct CountingStore<T> {
    inner: Arc<T>,
    counters: Arc<IoCounters>,
}

impl<T> CountingStore<T> {
    fn new(inner: T, counters: Arc<IoCounters>) -> Self {
        Self {
            inner: Arc::new(inner),
            counters,
        }
    }
}

impl<T: ObjectStore> Display for CountingStore<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CountingStore({})", self.inner)
    }
}

impl<T: ObjectStore> Debug for CountingStore<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CountingStore").finish()
    }
}

struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<IoCounters>,
}

impl Debug for CountingUpload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CountingUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = data.content_length() as u64;
        let counters = Arc::clone(&self.counters);
        self.inner
            .put_part(data)
            .map(move |result| {
                counters.request("multipart_part", result.is_ok());
                if result.is_ok() {
                    counters.bytes_written("multipart_part", bytes);
                }
                result
            })
            .boxed()
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        let result = self.inner.complete().await;
        self.counters.request("multipart_complete", result.is_ok());
        result
    }

    async fn abort(&mut self) -> StoreResult<()> {
        let result = self.inner.abort().await;
        self.counters.request("multipart_abort", result.is_ok());
        result
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl<T: ObjectStore> ObjectStore for CountingStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, options).await;
        self.counters.request("put", result.is_ok());
        if result.is_ok() {
            self.counters.bytes_written("put", bytes);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let result = self.inner.put_multipart_opts(location, options).await;
        self.counters.request("multipart_init", result.is_ok());
        result.map(|inner| {
            Box::new(CountingUpload {
                inner,
                counters: Arc::clone(&self.counters),
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let api = if options.head {
            "head"
        } else if options.range.is_some() {
            "get_range"
        } else {
            "get"
        };
        let result = self.inner.get_opts(location, options).await;
        self.counters.request(api, result.is_ok());
        if let Ok(value) = &result {
            self.counters
                .bytes_read(api, value.range.end - value.range.start);
        }
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await;
        self.counters.request("get_ranges", result.is_ok());
        if let Ok(values) = &result {
            self.counters.bytes_read(
                "get_ranges",
                values.iter().map(|value| value.len() as u64).sum(),
            );
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        let inner = Arc::clone(&self.inner);
        let counters = Arc::clone(&self.counters);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let counters = Arc::clone(&counters);
                async move {
                    let location = location?;
                    let result = inner.delete(&location).await;
                    counters.request("delete", result.is_ok());
                    result.map(|()| location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.counters.request("list", true);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.counters.request("list_with_offset", true);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let result = self.inner.list_with_delimiter(prefix).await;
        self.counters.request("list_with_delimiter", result.is_ok());
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        let result = self.inner.copy_opts(from, to, options).await;
        self.counters.request("copy", result.is_ok());
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        let result = self.inner.rename_opts(from, to, options).await;
        self.counters.request("rename", result.is_ok());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{run_phase0_filesystem_contract, Phase0Config, Phase0Mode};

    fn config() -> Phase0Config {
        Phase0Config {
            logical_bytes: 65_536,
            key_count: 64,
            point_reads_per_seed: 8,
            scan_rows_per_seed: 10,
            seeds: vec![1103],
        }
    }

    #[tokio::test]
    async fn filesystem_contract_passes() {
        let report = run_phase0_filesystem_contract(&config(), Phase0Mode::Correct)
            .await
            .expect("run contract");
        assert!(report.passed(), "failed gates: {:?}", report.gates);
        assert_eq!(report.receipt_digest, report.repeated_receipt_digest);
        assert!(report.seeds[0].ingest.io.written_byte_total() > 0);
        assert!(report.seeds[0].reopen.io.read_byte_total() > 0);
    }

    #[tokio::test]
    async fn warm_reopen_negative_control_fails_only_cache_state() {
        let report = run_phase0_filesystem_contract(&config(), Phase0Mode::ReuseWarmDbForReopen)
            .await
            .expect("run negative contract");
        let failed: Vec<&str> = report
            .gates
            .iter()
            .filter(|gate| !gate.passed)
            .map(|gate| gate.id.as_str())
            .collect();
        assert_eq!(failed, vec!["fresh_db_cache_on_reopen"]);
    }
}
