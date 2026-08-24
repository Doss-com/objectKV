//! Physical density probe for one disposable KV Runtime process.

use crate::phase0::{CountingStore, IoCounters, Phase0IoDelta};
use crate::SLATEDB_REVISION;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::{Settings, SstBlockSize};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::{Db, WriteBatch};
use std::path::Path;
#[cfg(target_os = "macos")]
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const RANGES_PER_BATCH: usize = 64;

struct ResourceProbe {
    pid: Pid,
    system: System,
}

impl ResourceProbe {
    fn new() -> Self {
        Self {
            pid: Pid::from_u32(std::process::id()),
            system: System::new(),
        }
    }

    fn resident_memory_bytes(&mut self) -> u64 {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_memory(),
        );
        self.system
            .process(self.pid)
            .map_or(0, sysinfo::Process::memory)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum KvRuntimeDensityTopology {
    OneDbLogicalRanges,
    ManyDbSharedCache,
    ManyDbPrivateCache,
}

impl KvRuntimeDensityTopology {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::OneDbLogicalRanges => "one-db-logical-ranges",
            Self::ManyDbSharedCache => "many-db-shared-cache",
            Self::ManyDbPrivateCache => "many-db-private-cache",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KvRuntimeDensityMode {
    Correct,
    SubstituteAccountedRss,
    ClaimPrivateCachesAreShared,
    ReuseWarmHandle,
    OmitSafetyReceipt,
}

impl KvRuntimeDensityMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::SubstituteAccountedRss => "substitute_accounted_rss",
            Self::ClaimPrivateCachesAreShared => "claim_private_caches_are_shared",
            Self::ReuseWarmHandle => "reuse_warm_handle",
            Self::OmitSafetyReceipt => "omit_safety_receipt",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvRuntimeDensityWorkerConfig {
    pub topology: KvRuntimeDensityTopology,
    pub target_range_engines: usize,
    pub seed: u64,
    pub max_rss_bytes: u64,
    pub timeout_millis: u64,
    pub decoded_cache_bytes: u64,
    pub nvme_cache_bytes: usize,
    pub nvme_part_bytes: usize,
    pub nvme_open_file_handles: usize,
    pub keys_per_range: usize,
    pub value_bytes: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvRuntimeDensityResourceSample {
    pub rss_bytes: u64,
    pub runtime_tasks: usize,
    pub os_threads: u64,
    pub open_file_descriptors: u64,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KvRuntimeDensityReceipt {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub physical_profile: String,
    pub topology: String,
    pub mode: String,
    pub target_range_engines: usize,
    pub completed_range_engines: usize,
    pub database_instances: usize,
    pub decoded_cache_instances: usize,
    pub nvme_cache_instances: usize,
    pub baseline: KvRuntimeDensityResourceSample,
    pub resident: KvRuntimeDensityResourceSample,
    pub after_close: KvRuntimeDensityResourceSample,
    pub peak_rss_bytes: u64,
    pub incremental_peak_rss_bytes: u64,
    pub physical_rss_probe_supported: bool,
    pub runtime_task_probe_supported: bool,
    pub thread_probe_supported: bool,
    pub file_descriptor_probe_supported: bool,
    pub object_wal_enabled: bool,
    pub automatic_flush_enabled: bool,
    pub embedded_compactor: bool,
    pub embedded_garbage_collector: bool,
    pub sst_block_size_bytes: u64,
    pub min_filter_keys: u32,
    pub completed_range_reads_exact: bool,
    pub empty_ram_and_nvme_reopen_executed: bool,
    pub safety_bounds_checked: bool,
    pub stop_reason: String,
    pub object_io: Phase0IoDelta,
    pub object_files: u64,
    pub object_file_bytes: u64,
    pub nvme_cache_files: u64,
    pub nvme_cache_file_bytes: u64,
    pub initial_open_seconds: f64,
    pub write_flush_seconds: f64,
    pub warm_point_p50_seconds: f64,
    pub warm_point_p99_seconds: f64,
    pub empty_cache_rebuild_seconds: f64,
    pub cold_point_p50_seconds: f64,
    pub cold_point_p99_seconds: f64,
    pub semantic_receipt_sha256: String,
}

/// Execute one physical density subject inside the current process.
///
/// # Errors
///
/// Returns an error when the configuration is invalid, temporary storage
/// cannot be created, `SlateDB` cannot complete its lifecycle, or an exact read
/// differs from the deterministic value oracle.
#[allow(clippy::too_many_lines)]
pub async fn run_kv_runtime_density_worker(
    config: &KvRuntimeDensityWorkerConfig,
    mode: KvRuntimeDensityMode,
) -> Result<KvRuntimeDensityReceipt, String> {
    validate_config(config)?;

    let started = Instant::now();
    let mut resource_probe = ResourceProbe::new();
    let baseline = resource_sample(&mut resource_probe);
    let root = tempfile::Builder::new()
        .prefix("okv-kv-runtime-density-")
        .tempdir()
        .map_err(|error| format!("create density root: {error}"))?;
    let object_root = root.path().join("objects");
    let warm_cache_root = root.path().join("nvme-warm");
    let cold_cache_root = root.path().join("nvme-cold");
    std::fs::create_dir_all(&object_root)
        .map_err(|error| format!("create object root: {error}"))?;
    std::fs::create_dir_all(&warm_cache_root)
        .map_err(|error| format!("create warm cache root: {error}"))?;
    std::fs::create_dir_all(&cold_cache_root)
        .map_err(|error| format!("create cold cache root: {error}"))?;

    let local = LocalFileSystem::new_with_prefix(&object_root)
        .map_err(|error| format!("open density object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let raw_store: Arc<dyn ObjectStore> =
        Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let warm_store = cached_store(config, &warm_cache_root, Arc::clone(&raw_store)).await?;
    let mut databases = Vec::new();
    let mut warm_caches = Vec::new();
    let mut completed = 0_usize;
    let mut initial_open_seconds = 0.0;
    let mut write_flush_seconds = 0.0;
    let mut peak_rss_bytes = baseline.rss_bytes;
    let mut stop_reason = "none".to_owned();

    match config.topology {
        KvRuntimeDensityTopology::OneDbLogicalRanges => {
            let cache = decoded_cache(config.decoded_cache_bytes);
            warm_caches.push(Arc::clone(&cache));
            let open_started = Instant::now();
            databases.push(
                build_db(
                    &database_path(config.topology, 0),
                    Arc::clone(&warm_store),
                    cache,
                    database_seed(config, 0),
                )
                .await?,
            );
            initial_open_seconds += open_started.elapsed().as_secs_f64();

            while completed < config.target_range_engines {
                let batch_end = completed
                    .saturating_add(RANGES_PER_BATCH)
                    .min(config.target_range_engines);
                let mut batch = WriteBatch::new();
                for range in completed..batch_end {
                    put_range(&mut batch, config, range);
                }
                let write_started = Instant::now();
                databases[0]
                    .write(batch)
                    .await
                    .map_err(|error| format!("write logical-range density batch: {error}"))?;
                write_flush_seconds += write_started.elapsed().as_secs_f64();
                completed = batch_end;
                let sample = resource_sample(&mut resource_probe);
                peak_rss_bytes = peak_rss_bytes.max(sample.rss_bytes);
                if let Some(reason) = safety_stop(&sample, started, config) {
                    reason.clone_into(&mut stop_reason);
                    break;
                }
            }

            let flush_started = Instant::now();
            databases[0]
                .flush()
                .await
                .map_err(|error| format!("flush logical-range density database: {error}"))?;
            write_flush_seconds += flush_started.elapsed().as_secs_f64();
        }
        KvRuntimeDensityTopology::ManyDbSharedCache
        | KvRuntimeDensityTopology::ManyDbPrivateCache => {
            let shared_cache = (config.topology == KvRuntimeDensityTopology::ManyDbSharedCache)
                .then(|| decoded_cache(config.decoded_cache_bytes));
            if let Some(cache) = &shared_cache {
                warm_caches.push(Arc::clone(cache));
            }

            for range in 0..config.target_range_engines {
                let cache = if let Some(cache) = &shared_cache {
                    Arc::clone(cache)
                } else {
                    let cache = decoded_cache(config.decoded_cache_bytes);
                    warm_caches.push(Arc::clone(&cache));
                    cache
                };
                let open_started = Instant::now();
                let database = build_db(
                    &database_path(config.topology, range),
                    Arc::clone(&warm_store),
                    cache,
                    database_seed(config, range),
                )
                .await?;
                initial_open_seconds += open_started.elapsed().as_secs_f64();

                let mut batch = WriteBatch::new();
                put_range(&mut batch, config, range);
                let write_started = Instant::now();
                database
                    .write(batch)
                    .await
                    .map_err(|error| format!("write database for range {range}: {error}"))?;
                database
                    .flush()
                    .await
                    .map_err(|error| format!("flush database for range {range}: {error}"))?;
                write_flush_seconds += write_started.elapsed().as_secs_f64();

                databases.push(database);
                completed = completed.saturating_add(1);
                let sample = resource_sample(&mut resource_probe);
                peak_rss_bytes = peak_rss_bytes.max(sample.rss_bytes);
                if let Some(reason) = safety_stop(&sample, started, config) {
                    reason.clone_into(&mut stop_reason);
                    break;
                }
            }
        }
    }

    let database_instances = databases.len();
    let actual_decoded_cache_instances = warm_caches.len();
    let (warm_exact, warm_latencies) = read_completed_ranges(&databases, config, completed).await?;
    let resident = resource_sample(&mut resource_probe);
    peak_rss_bytes = peak_rss_bytes.max(resident.rss_bytes);
    let safety_bounds_checked = mode != KvRuntimeDensityMode::OmitSafetyReceipt;
    if stop_reason == "none" {
        if let Some(reason) = safety_stop(&resident, started, config) {
            reason.clone_into(&mut stop_reason);
        }
    }
    let (warm_cache_files, warm_cache_file_bytes) = file_inventory(&warm_cache_root)?;
    let mut warm_databases = Some(databases);
    let mut remaining_warm_caches = Some(warm_caches);

    let rebuild_started = Instant::now();
    let (cold_exact, cold_latencies, empty_ram_and_nvme_reopen_executed) =
        if mode == KvRuntimeDensityMode::ReuseWarmHandle {
            let (exact, latencies) = read_completed_ranges(
                warm_databases.as_deref().unwrap_or_default(),
                config,
                completed,
            )
            .await?;
            (exact, latencies, false)
        } else {
            let databases = warm_databases.take().unwrap_or_default();
            let caches = remaining_warm_caches.take().unwrap_or_default();
            close_databases_and_caches(&databases, &caches, "warm").await?;
            drop(databases);
            drop(caches);
            drop(warm_store);

            let cold_store = cached_store(config, &cold_cache_root, Arc::clone(&raw_store)).await?;
            let (reopened, cold_caches) = reopen_databases(config, completed, cold_store).await?;
            let (exact, latencies) = read_completed_ranges(&reopened, config, completed).await?;
            let cold_resident = resource_sample(&mut resource_probe);
            peak_rss_bytes = peak_rss_bytes.max(cold_resident.rss_bytes);
            close_databases_and_caches(&reopened, &cold_caches, "cold").await?;
            (exact, latencies, true)
        };
    let empty_cache_rebuild_seconds = rebuild_started.elapsed().as_secs_f64();
    if mode == KvRuntimeDensityMode::ReuseWarmHandle {
        close_databases_and_caches(
            warm_databases.as_deref().unwrap_or_default(),
            remaining_warm_caches.as_deref().unwrap_or_default(),
            "reused warm",
        )
        .await?;
    }

    let after_close = resource_sample(&mut resource_probe);
    let (object_files, object_file_bytes) = file_inventory(&object_root)?;
    let (cold_cache_files, cold_cache_file_bytes) = file_inventory(&cold_cache_root)?;
    let nvme_cache_files = warm_cache_files.max(cold_cache_files);
    let nvme_cache_file_bytes = warm_cache_file_bytes.max(cold_cache_file_bytes);
    peak_rss_bytes = peak_rss_bytes.max(after_close.rss_bytes);
    let mut physical_rss_probe_supported = peak_rss_bytes > 0;
    if mode == KvRuntimeDensityMode::SubstituteAccountedRss {
        peak_rss_bytes = 4_608;
        physical_rss_probe_supported = false;
    }
    let completed_range_reads_exact = warm_exact && cold_exact;
    let (warm_point_p50_seconds, warm_point_p99_seconds) = percentiles(&warm_latencies);
    let (cold_point_p50_seconds, cold_point_p99_seconds) = percentiles(&cold_latencies);
    let semantic_receipt_sha256 = semantic_digest(
        config,
        completed,
        completed_range_reads_exact,
        empty_ram_and_nvme_reopen_executed,
        &stop_reason,
    );
    let decoded_cache_instances = if mode == KvRuntimeDensityMode::ClaimPrivateCachesAreShared
        && config.topology == KvRuntimeDensityTopology::ManyDbPrivateCache
    {
        1
    } else {
        actual_decoded_cache_instances
    };

    Ok(KvRuntimeDensityReceipt {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        physical_profile: "objectkv-serving-v1".to_owned(),
        topology: config.topology.id().to_owned(),
        mode: mode.id().to_owned(),
        target_range_engines: config.target_range_engines,
        completed_range_engines: completed,
        database_instances,
        decoded_cache_instances,
        nvme_cache_instances: 1,
        baseline: baseline.clone(),
        resident,
        after_close,
        peak_rss_bytes,
        incremental_peak_rss_bytes: peak_rss_bytes.saturating_sub(baseline.rss_bytes),
        physical_rss_probe_supported,
        runtime_task_probe_supported: true,
        thread_probe_supported: baseline.os_threads > 0,
        file_descriptor_probe_supported: baseline.open_file_descriptors > 0,
        object_wal_enabled: false,
        automatic_flush_enabled: false,
        embedded_compactor: false,
        embedded_garbage_collector: false,
        sst_block_size_bytes: 65_536,
        min_filter_keys: 1,
        completed_range_reads_exact,
        empty_ram_and_nvme_reopen_executed,
        safety_bounds_checked,
        stop_reason,
        object_io: counters.total(),
        object_files,
        object_file_bytes,
        nvme_cache_files,
        nvme_cache_file_bytes,
        initial_open_seconds,
        write_flush_seconds,
        warm_point_p50_seconds,
        warm_point_p99_seconds,
        empty_cache_rebuild_seconds,
        cold_point_p50_seconds,
        cold_point_p99_seconds,
        semantic_receipt_sha256,
    })
}

fn validate_config(config: &KvRuntimeDensityWorkerConfig) -> Result<(), String> {
    if config.target_range_engines == 0 {
        return Err("physical density target must be positive".to_owned());
    }
    if config.max_rss_bytes == 0 || config.timeout_millis == 0 {
        return Err("physical density safety bounds must be positive".to_owned());
    }
    if config.decoded_cache_bytes == 0
        || config.nvme_cache_bytes == 0
        || config.nvme_part_bytes == 0
        || config.nvme_open_file_handles == 0
    {
        return Err("physical density cache bounds must be positive".to_owned());
    }
    if !config.nvme_part_bytes.is_multiple_of(1_024) {
        return Err("physical density NVMe part must be KiB aligned".to_owned());
    }
    if config.keys_per_range == 0 || config.value_bytes == 0 {
        return Err("physical density logical inputs must be positive".to_owned());
    }
    Ok(())
}

fn serving_settings() -> Settings {
    Settings {
        flush_interval: None,
        wal_enabled: false,
        min_filter_keys: 1,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

fn decoded_cache(capacity: u64) -> Arc<dyn DbCache> {
    Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: capacity,
        time_to_live: None,
        time_to_idle: None,
    }))
}

async fn cached_store(
    config: &KvRuntimeDensityWorkerConfig,
    root: &Path,
    raw_store: Arc<dyn ObjectStore>,
) -> Result<Arc<dyn ObjectStore>, String> {
    CachedObjectStore::builder(root, raw_store)
        .with_max_cache_size_bytes(Some(config.nvme_cache_bytes))
        .with_part_size_bytes(config.nvme_part_bytes)
        .with_cache_on_flush(true)
        .with_scan_interval(None)
        .with_max_open_file_handles(config.nvme_open_file_handles)
        .build()
        .await
        .map(|store| store as Arc<dyn ObjectStore>)
        .map_err(|error| format!("build shared NVMe cache: {error}"))
}

async fn build_db(
    path: &str,
    store: Arc<dyn ObjectStore>,
    cache: Arc<dyn DbCache>,
    seed: u64,
) -> Result<Db, String> {
    Db::builder(path, store)
        .with_settings(serving_settings())
        .with_seed(seed)
        .with_db_cache(cache)
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .build()
        .await
        .map_err(|error| format!("open physical density SlateDB: {error}"))
}

fn put_range(batch: &mut WriteBatch, config: &KvRuntimeDensityWorkerConfig, range: usize) {
    for key_ordinal in 0..config.keys_per_range {
        batch.put(
            key_for(range, key_ordinal),
            value_for(config.seed, range, key_ordinal, config.value_bytes),
        );
    }
}

fn safety_stop<'a>(
    sample: &KvRuntimeDensityResourceSample,
    started: Instant,
    config: &'a KvRuntimeDensityWorkerConfig,
) -> Option<&'a str> {
    if sample.rss_bytes > config.max_rss_bytes {
        Some("rss-limit")
    } else if started.elapsed().as_millis() > u128::from(config.timeout_millis) {
        Some("time-limit")
    } else {
        None
    }
}

fn database_path(topology: KvRuntimeDensityTopology, range: usize) -> String {
    match topology {
        KvRuntimeDensityTopology::OneDbLogicalRanges => "kv-runtime".to_owned(),
        KvRuntimeDensityTopology::ManyDbSharedCache
        | KvRuntimeDensityTopology::ManyDbPrivateCache => {
            format!("kv-runtime/range-{range:016x}")
        }
    }
}

fn database_seed(config: &KvRuntimeDensityWorkerConfig, range: usize) -> u64 {
    match config.topology {
        KvRuntimeDensityTopology::OneDbLogicalRanges => config.seed,
        KvRuntimeDensityTopology::ManyDbSharedCache
        | KvRuntimeDensityTopology::ManyDbPrivateCache => {
            config.seed
                ^ u64::try_from(range)
                    .unwrap_or(u64::MAX)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        }
    }
}

async fn reopen_databases(
    config: &KvRuntimeDensityWorkerConfig,
    completed: usize,
    store: Arc<dyn ObjectStore>,
) -> Result<(Vec<Db>, Vec<Arc<dyn DbCache>>), String> {
    let mut databases = Vec::new();
    let mut caches = Vec::new();
    match config.topology {
        KvRuntimeDensityTopology::OneDbLogicalRanges => {
            let cache = decoded_cache(config.decoded_cache_bytes);
            databases.push(
                build_db(
                    &database_path(config.topology, 0),
                    store,
                    Arc::clone(&cache),
                    database_seed(config, 0),
                )
                .await?,
            );
            caches.push(cache);
        }
        KvRuntimeDensityTopology::ManyDbSharedCache => {
            let cache = decoded_cache(config.decoded_cache_bytes);
            for range in 0..completed {
                databases.push(
                    build_db(
                        &database_path(config.topology, range),
                        Arc::clone(&store),
                        Arc::clone(&cache),
                        database_seed(config, range),
                    )
                    .await?,
                );
            }
            caches.push(cache);
        }
        KvRuntimeDensityTopology::ManyDbPrivateCache => {
            for range in 0..completed {
                let cache = decoded_cache(config.decoded_cache_bytes);
                databases.push(
                    build_db(
                        &database_path(config.topology, range),
                        Arc::clone(&store),
                        Arc::clone(&cache),
                        database_seed(config, range),
                    )
                    .await?,
                );
                caches.push(cache);
            }
        }
    }
    Ok((databases, caches))
}

async fn close_databases_and_caches(
    databases: &[Db],
    caches: &[Arc<dyn DbCache>],
    phase: &str,
) -> Result<(), String> {
    for (index, database) in databases.iter().enumerate() {
        database
            .close()
            .await
            .map_err(|error| format!("close {phase} density database {index}: {error}"))?;
    }
    for (index, cache) in caches.iter().enumerate() {
        cache
            .close()
            .await
            .map_err(|error| format!("close {phase} decoded cache {index}: {error}"))?;
    }
    Ok(())
}

async fn read_completed_ranges(
    databases: &[Db],
    config: &KvRuntimeDensityWorkerConfig,
    completed: usize,
) -> Result<(bool, Vec<f64>), String> {
    let mut exact = true;
    let mut latencies = Vec::with_capacity(completed);
    for range in 0..completed {
        let database = match config.topology {
            KvRuntimeDensityTopology::OneDbLogicalRanges => &databases[0],
            KvRuntimeDensityTopology::ManyDbSharedCache
            | KvRuntimeDensityTopology::ManyDbPrivateCache => &databases[range],
        };
        let started = Instant::now();
        let observed = database
            .get(key_for(range, 0))
            .await
            .map_err(|error| format!("read physical density range {range}: {error}"))?;
        latencies.push(started.elapsed().as_secs_f64());
        let expected = value_for(config.seed, range, 0, config.value_bytes);
        exact &= observed.as_deref() == Some(expected.as_slice());
    }
    Ok((exact, latencies))
}

fn key_for(range: usize, ordinal: usize) -> Vec<u8> {
    format!("range/{range:016x}/key/{ordinal:08x}").into_bytes()
}

fn value_for(seed: u64, range: usize, ordinal: usize, length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut block = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"okv-kv-runtime-density-value-v1");
        hasher.update(seed.to_be_bytes());
        hasher.update(range.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(block.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length - value.len();
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        block = block.saturating_add(1);
    }
    value
}

fn percentiles(samples: &[f64]) -> (f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let p50 = sorted[(sorted.len() - 1) / 2];
    let p99_index = ((sorted.len() - 1) * 99).div_ceil(100);
    (p50, sorted[p99_index])
}

fn file_inventory(root: &Path) -> Result<(u64, u64), String> {
    let mut files = 0_u64;
    let mut bytes = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .map_err(|error| format!("read inventory {}: {error}", directory.display()))?
        {
            let entry = entry.map_err(|error| format!("read inventory entry: {error}"))?;
            let file_type = entry
                .file_type()
                .map_err(|error| format!("read inventory file type: {error}"))?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files = files.saturating_add(1);
                bytes = bytes.saturating_add(
                    entry
                        .metadata()
                        .map_err(|error| format!("read inventory metadata: {error}"))?
                        .len(),
                );
            }
        }
    }
    Ok((files, bytes))
}

fn resource_sample(probe: &mut ResourceProbe) -> KvRuntimeDensityResourceSample {
    KvRuntimeDensityResourceSample {
        rss_bytes: probe.resident_memory_bytes(),
        runtime_tasks: tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        os_threads: process_thread_count(),
        open_file_descriptors: open_file_descriptor_count(),
    }
}

#[cfg(target_os = "linux")]
fn process_thread_count() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Threads:"))?
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn process_thread_count() -> u64 {
    Command::new("ps")
        .args(["-M", "-p", &std::process::id().to_string(), "-o", "pid="])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or(0, |output| {
            u64::try_from(
                output
                    .stdout
                    .split(|byte| *byte == b'\n')
                    .count()
                    .saturating_sub(1),
            )
            .unwrap_or(u64::MAX)
        })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_thread_count() -> u64 {
    0
}

fn open_file_descriptor_count() -> u64 {
    ["/proc/self/fd", "/dev/fd"]
        .into_iter()
        .find_map(|path| {
            std::fs::read_dir(path)
                .ok()
                .map(|entries| u64::try_from(entries.count()).unwrap_or(u64::MAX))
        })
        .unwrap_or(0)
}

fn semantic_digest(
    config: &KvRuntimeDensityWorkerConfig,
    completed: usize,
    exact: bool,
    reopened: bool,
    stop_reason: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-kv-runtime-density-semantic-v1");
    hasher.update(SLATEDB_REVISION.as_bytes());
    hasher.update(config.topology.id().as_bytes());
    hasher.update(config.target_range_engines.to_be_bytes());
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.keys_per_range.to_be_bytes());
    hasher.update(config.value_bytes.to_be_bytes());
    hasher.update(completed.to_be_bytes());
    hasher.update([u8::from(exact), u8::from(reopened)]);
    hasher.update(stop_reason.as_bytes());
    format!("{:x}", hasher.finalize())
}
