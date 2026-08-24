//! RFC-0058 exact-version read amplification worker.

use crate::phase0::{CountingStore, IoCounters, Phase0IoDelta};
use crate::{AdapterError, SlateEngine, SLATEDB_REVISION};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::{Settings, SstBlockSize};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::Db;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const DATABASE_PATH: &str = "kv-runtime";
const POINT_SAMPLE_LIMIT: usize = 32;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReadCurveMode {
    Correct,
    LatestOnly,
    SkipPointTombstone,
    OverstateAppliedFrontier,
    LengthPrefixUserKeys,
}

impl SnapshotReadCurveMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::LatestOnly => "latest_only",
            Self::SkipPointTombstone => "skip_point_tombstone",
            Self::OverstateAppliedFrontier => "overstate_applied_frontier",
            Self::LengthPrefixUserKeys => "length_prefix_user_keys",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotReadCurveConfig {
    pub version_depth: u64,
    pub key_count: usize,
    pub value_bytes: usize,
    pub seed: u64,
    pub max_rss_bytes: u64,
    pub timeout_millis: u64,
    pub decoded_cache_bytes: u64,
    pub nvme_cache_bytes: usize,
    pub nvme_part_bytes: usize,
    pub nvme_open_file_handles: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotReadTargetReceipt {
    pub class: String,
    pub read_version: u64,
    pub point_samples: usize,
    pub warm_point_p50_seconds: f64,
    pub warm_point_p99_seconds: f64,
    pub cold_point_p50_seconds: f64,
    pub cold_point_p99_seconds: f64,
    pub warm_scan_seconds: f64,
    pub cold_scan_seconds: f64,
    pub warm_scan_rows: usize,
    pub cold_scan_rows: usize,
    pub warm_point_io: Phase0IoDelta,
    pub cold_point_io: Phase0IoDelta,
    pub warm_scan_io: Phase0IoDelta,
    pub cold_scan_io: Phase0IoDelta,
    pub point_reads_exact: bool,
    pub ordered_scans_exact: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotReadCurveReceipt {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub physical_profile: String,
    pub mode: String,
    pub seed: u64,
    pub version_depth: u64,
    pub key_count: usize,
    pub value_bytes: usize,
    pub actual_applied_frontier: u64,
    pub claimed_applied_frontier: u64,
    pub targets: Vec<SnapshotReadTargetReceipt>,
    pub tombstone_exact: bool,
    pub future_frontier_refused: bool,
    pub binary_key_order_exact: bool,
    pub close_reopen_exact: bool,
    pub safety_bounds_held: bool,
    pub peak_rss_bytes: u64,
    pub object_files: u64,
    pub object_file_bytes: u64,
    pub physical_bytes_per_live_byte: f64,
    pub ingest_flush_seconds: f64,
    pub total_elapsed_seconds: f64,
    pub total_io: Phase0IoDelta,
    pub semantic_receipt_sha256: String,
}

/// Execute one RFC-0058 subject in the current child process.
///
/// # Errors
///
/// Returns an error for invalid bounds, object-store failures, or a `SlateDB`
/// lifecycle failure. Semantic violations are returned in the receipt so the
/// controller can prove each negative subject is detected.
#[allow(clippy::too_many_lines)]
pub async fn run_snapshot_read_curve_worker(
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
) -> Result<SnapshotReadCurveReceipt, String> {
    validate_config(config)?;
    let started = Instant::now();
    let root = tempfile::Builder::new()
        .prefix("okv-snapshot-read-curve-")
        .tempdir()
        .map_err(|error| format!("create snapshot-read root: {error}"))?;
    let object_root = root.path().join("objects");
    let warm_cache_root = root.path().join("nvme-warm");
    std::fs::create_dir_all(&object_root)
        .map_err(|error| format!("create snapshot-read object root: {error}"))?;
    std::fs::create_dir_all(&warm_cache_root)
        .map_err(|error| format!("create snapshot-read warm cache root: {error}"))?;

    let local = LocalFileSystem::new_with_prefix(&object_root)
        .map_err(|error| format!("open snapshot-read object root: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let raw_store: Arc<dyn ObjectStore> =
        Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let warm_store = cached_store(config, &warm_cache_root, Arc::clone(&raw_store)).await?;
    let warm_cache = decoded_cache(config.decoded_cache_bytes);
    let warm_engine = build_engine(config, warm_store, Arc::clone(&warm_cache)).await?;

    let ingest_started = Instant::now();
    for sequence in 1..=config.version_depth {
        warm_engine
            .apply(commit_for(config, sequence))
            .await
            .map_err(|error| format!("apply snapshot-read version {sequence}: {error}"))?;
    }
    warm_engine
        .flush()
        .await
        .map_err(|error| format!("flush snapshot-read history: {error}"))?;
    let ingest_flush_seconds = ingest_started.elapsed().as_secs_f64();

    let target_versions = target_versions(config.version_depth);
    let mut targets = Vec::with_capacity(target_versions.len());
    for (class, read_version) in &target_versions {
        let before_point = counters.total();
        let (warm_point_exact, warm_point_latencies) =
            measure_points(&warm_engine, config, mode, *read_version).await?;
        let warm_point_io = counters.total().difference_since(&before_point);
        let before_scan = counters.total();
        let scan_started = Instant::now();
        let (warm_scan_exact, warm_scan_rows) =
            measure_scan(&warm_engine, config, mode, *read_version).await?;
        let warm_scan_seconds = scan_started.elapsed().as_secs_f64();
        let warm_scan_io = counters.total().difference_since(&before_scan);
        let (warm_point_p50_seconds, warm_point_p99_seconds) = percentiles(&warm_point_latencies);

        targets.push(SnapshotReadTargetReceipt {
            class: (*class).to_owned(),
            read_version: *read_version,
            point_samples: warm_point_latencies.len(),
            warm_point_p50_seconds,
            warm_point_p99_seconds,
            cold_point_p50_seconds: 0.0,
            cold_point_p99_seconds: 0.0,
            warm_scan_seconds,
            cold_scan_seconds: 0.0,
            warm_scan_rows,
            cold_scan_rows: 0,
            warm_point_io,
            cold_point_io: Phase0IoDelta::default(),
            warm_scan_io,
            cold_scan_io: Phase0IoDelta::default(),
            point_reads_exact: warm_point_exact,
            ordered_scans_exact: warm_scan_exact,
        });
    }

    let tombstone_exact = measure_tombstone(&warm_engine, config, mode).await?;
    let actual_applied_frontier = config.version_depth;
    let claimed_applied_frontier = if mode == SnapshotReadCurveMode::OverstateAppliedFrontier {
        actual_applied_frontier.saturating_add(1)
    } else {
        actual_applied_frontier
    };
    let future_frontier_refused = if mode == SnapshotReadCurveMode::OverstateAppliedFrontier {
        false
    } else {
        matches!(
            warm_engine
                .get_at(
                    &curve_key(0),
                    Version::new(actual_applied_frontier.saturating_add(1))
                )
                .await,
            Err(AdapterError::SnapshotUnavailable { .. })
        )
    };
    let binary_key_order_exact = if mode == SnapshotReadCurveMode::LengthPrefixUserKeys {
        broken_length_prefix_order_is_exact()
    } else {
        measure_binary_order(&warm_engine, config.version_depth).await?
    };

    warm_engine
        .close()
        .await
        .map_err(|error| format!("close warm snapshot-read engine: {error}"))?;
    warm_cache
        .close()
        .await
        .map_err(|error| format!("close warm snapshot-read decoded cache: {error}"))?;

    for target in &mut targets {
        let cold_point_root = root
            .path()
            .join(format!("nvme-cold-{}-point", target.class));
        std::fs::create_dir_all(&cold_point_root)
            .map_err(|error| format!("create cold point cache root: {error}"))?;
        let cold_point_store =
            cached_store(config, &cold_point_root, Arc::clone(&raw_store)).await?;
        let cold_point_cache = decoded_cache(config.decoded_cache_bytes);
        let cold_point_engine =
            build_engine(config, cold_point_store, Arc::clone(&cold_point_cache)).await?;
        let before_point = counters.total();
        let (cold_point_exact, cold_point_latencies) =
            measure_points(&cold_point_engine, config, mode, target.read_version).await?;
        target.cold_point_io = counters.total().difference_since(&before_point);
        (target.cold_point_p50_seconds, target.cold_point_p99_seconds) =
            percentiles(&cold_point_latencies);
        cold_point_engine
            .close()
            .await
            .map_err(|error| format!("close cold point engine: {error}"))?;
        cold_point_cache
            .close()
            .await
            .map_err(|error| format!("close cold point cache: {error}"))?;

        let cold_scan_root = root.path().join(format!("nvme-cold-{}-scan", target.class));
        std::fs::create_dir_all(&cold_scan_root)
            .map_err(|error| format!("create cold scan cache root: {error}"))?;
        let cold_scan_store = cached_store(config, &cold_scan_root, Arc::clone(&raw_store)).await?;
        let cold_scan_cache = decoded_cache(config.decoded_cache_bytes);
        let cold_scan_engine =
            build_engine(config, cold_scan_store, Arc::clone(&cold_scan_cache)).await?;
        let before_scan = counters.total();
        let scan_started = Instant::now();
        let (cold_scan_exact, cold_scan_rows) =
            measure_scan(&cold_scan_engine, config, mode, target.read_version).await?;
        target.cold_scan_seconds = scan_started.elapsed().as_secs_f64();
        target.cold_scan_rows = cold_scan_rows;
        target.cold_scan_io = counters.total().difference_since(&before_scan);
        target.point_reads_exact &= cold_point_exact;
        target.ordered_scans_exact &= cold_scan_exact;
        cold_scan_engine
            .close()
            .await
            .map_err(|error| format!("close cold scan engine: {error}"))?;
        cold_scan_cache
            .close()
            .await
            .map_err(|error| format!("close cold scan cache: {error}"))?;
    }

    let close_reopen_exact = targets
        .iter()
        .all(|target| target.point_reads_exact && target.ordered_scans_exact);
    let total_elapsed_seconds = started.elapsed().as_secs_f64();
    let peak_rss_bytes = resident_memory_bytes();
    let safety_bounds_held = peak_rss_bytes <= config.max_rss_bytes
        && started.elapsed().as_millis() <= u128::from(config.timeout_millis);
    let (object_files, object_file_bytes) = file_inventory(&object_root)?;
    let live_bytes = config
        .key_count
        .saturating_sub(1)
        .saturating_mul(config.value_bytes);
    let physical_bytes_per_live_byte = if live_bytes == 0 {
        0.0
    } else {
        f64::from(u32::try_from(object_file_bytes).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(live_bytes).unwrap_or(u32::MAX))
    };
    let semantic_receipt_sha256 = semantic_digest(
        config,
        mode,
        &targets,
        tombstone_exact,
        future_frontier_refused,
        binary_key_order_exact,
        close_reopen_exact,
    );

    Ok(SnapshotReadCurveReceipt {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        physical_profile: "objectkv-serving-v1".to_owned(),
        mode: mode.id().to_owned(),
        seed: config.seed,
        version_depth: config.version_depth,
        key_count: config.key_count,
        value_bytes: config.value_bytes,
        actual_applied_frontier,
        claimed_applied_frontier,
        targets,
        tombstone_exact,
        future_frontier_refused,
        binary_key_order_exact,
        close_reopen_exact,
        safety_bounds_held,
        peak_rss_bytes,
        object_files,
        object_file_bytes,
        physical_bytes_per_live_byte,
        ingest_flush_seconds,
        total_elapsed_seconds,
        total_io: counters.total(),
        semantic_receipt_sha256,
    })
}

fn validate_config(config: &SnapshotReadCurveConfig) -> Result<(), String> {
    if config.version_depth == 0 || config.key_count < 2 || config.value_bytes == 0 {
        return Err(
            "snapshot-read logical inputs must be positive and include two keys".to_owned(),
        );
    }
    if config.max_rss_bytes == 0
        || config.timeout_millis == 0
        || config.decoded_cache_bytes == 0
        || config.nvme_cache_bytes == 0
        || config.nvme_part_bytes == 0
        || config.nvme_open_file_handles == 0
    {
        return Err("snapshot-read resource bounds must be positive".to_owned());
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
    config: &SnapshotReadCurveConfig,
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
        .map_err(|error| format!("build snapshot-read NVMe cache: {error}"))
}

async fn build_engine(
    config: &SnapshotReadCurveConfig,
    store: Arc<dyn ObjectStore>,
    cache: Arc<dyn DbCache>,
) -> Result<SlateEngine, String> {
    Db::builder(DATABASE_PATH, store)
        .with_settings(serving_settings())
        .with_seed(config.seed)
        .with_db_cache(cache)
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| format!("open snapshot-read SlateDB: {error}"))
}

fn commit_for(config: &SnapshotReadCurveConfig, sequence: u64) -> CommitBatch {
    let mut mutations = Vec::with_capacity(config.key_count.saturating_add(3));
    for ordinal in 0..config.key_count {
        if ordinal == config.key_count - 1 && sequence == config.version_depth {
            mutations.push(Mutation::Clear {
                key: curve_key(ordinal),
            });
        } else {
            mutations.push(Mutation::Set {
                key: curve_key(ordinal),
                value: value_for(config, ordinal, sequence),
            });
        }
    }
    if sequence == 1 {
        for key in [b"b".as_slice(), b"b\0z".as_slice(), b"ba".as_slice()] {
            mutations.push(Mutation::Set {
                key: key.to_vec(),
                value: key.to_vec(),
            });
        }
    }
    CommitBatch {
        version: Version::new(sequence),
        identity: CommitIdentity::for_test(sequence),
        mutations,
    }
}

fn target_versions(depth: u64) -> Vec<(&'static str, u64)> {
    vec![
        ("latest", depth),
        ("near-latest", depth.saturating_sub(8).max(1)),
        ("oldest", 1),
    ]
}

async fn measure_points(
    engine: &SlateEngine,
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
    requested: u64,
) -> Result<(bool, Vec<f64>), String> {
    let sample_count = POINT_SAMPLE_LIMIT.min(config.key_count.saturating_sub(1));
    let mut exact = true;
    let mut latencies = Vec::with_capacity(sample_count);
    for sample in 0..sample_count {
        let ordinal = sample.saturating_mul(config.key_count.saturating_sub(1)) / sample_count;
        let started = Instant::now();
        let observed =
            if mode == SnapshotReadCurveMode::LatestOnly && requested < config.version_depth {
                engine.get_latest(&curve_key(ordinal)).await
            } else {
                engine
                    .get_at(&curve_key(ordinal), Version::new(requested))
                    .await
            }
            .map_err(|error| format!("measure point read: {error}"))?;
        latencies.push(started.elapsed().as_secs_f64());
        exact &= observed == Some(value_for(config, ordinal, requested));
    }
    Ok((exact, latencies))
}

async fn measure_scan(
    engine: &SlateEngine,
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
    requested: u64,
) -> Result<(bool, usize), String> {
    let effective = if mode == SnapshotReadCurveMode::LatestOnly && requested < config.version_depth
    {
        config.version_depth
    } else {
        requested
    };
    let rows = engine
        .scan_at(b"k/", b"k0", Version::new(effective), config.key_count)
        .await
        .map_err(|error| format!("measure ordered scan: {error}"))?;
    let expected_rows = if requested == config.version_depth {
        config.key_count.saturating_sub(1)
    } else {
        config.key_count
    };
    let exact = rows.len() == expected_rows
        && rows.iter().enumerate().all(|(ordinal, (key, value))| {
            *key == curve_key(ordinal) && *value == value_for(config, ordinal, requested)
        });
    Ok((exact, rows.len()))
}

async fn measure_tombstone(
    engine: &SlateEngine,
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
) -> Result<bool, String> {
    let key = curve_key(config.key_count - 1);
    let observed = if mode == SnapshotReadCurveMode::SkipPointTombstone {
        engine
            .get_at(&key, Version::new(config.version_depth.saturating_sub(1)))
            .await
    } else {
        engine
            .get_at(&key, Version::new(config.version_depth))
            .await
    }
    .map_err(|error| format!("measure point tombstone: {error}"))?;
    Ok(observed.is_none())
}

async fn measure_binary_order(engine: &SlateEngine, version: u64) -> Result<bool, String> {
    let rows = engine
        .scan_at(b"b", b"c", Version::new(version), 3)
        .await
        .map_err(|error| format!("measure binary user-key order: {error}"))?;
    let observed = rows.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
    Ok(observed == [b"b".to_vec(), b"b\0z".to_vec(), b"ba".to_vec()])
}

fn broken_length_prefix_order_is_exact() -> bool {
    let logical = [b"b".to_vec(), b"b\0z".to_vec(), b"ba".to_vec()];
    let mut encoded = logical
        .iter()
        .map(|key| {
            let mut encoded = (key.len() as u64).to_be_bytes().to_vec();
            encoded.extend_from_slice(key);
            (encoded, key.clone())
        })
        .collect::<Vec<_>>();
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    encoded.into_iter().map(|(_, key)| key).collect::<Vec<_>>() == logical
}

fn curve_key(ordinal: usize) -> Vec<u8> {
    let mut key = b"k/".to_vec();
    key.extend_from_slice(&u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    key
}

fn value_for(config: &SnapshotReadCurveConfig, ordinal: usize, version: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(config.value_bytes);
    let mut block = 0_u64;
    while value.len() < config.value_bytes {
        let mut hasher = Sha256::new();
        hasher.update(b"okv-snapshot-read-value-v1");
        hasher.update(config.seed.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(version.to_be_bytes());
        hasher.update(block.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = config.value_bytes - value.len();
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
    let p99 = sorted[((sorted.len() - 1) * 99).div_ceil(100)];
    (p50, p99)
}

fn resident_memory_bytes() -> u64 {
    let pid = Pid::from_u32(std::process::id());
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, sysinfo::Process::memory)
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
                .map_err(|error| format!("read inventory type: {error}"))?;
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

#[allow(clippy::fn_params_excessive_bools)]
fn semantic_digest(
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
    targets: &[SnapshotReadTargetReceipt],
    tombstone_exact: bool,
    future_frontier_refused: bool,
    binary_key_order_exact: bool,
    close_reopen_exact: bool,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-snapshot-read-receipt-v1");
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.version_depth.to_be_bytes());
    hasher.update(config.key_count.to_be_bytes());
    hasher.update(config.value_bytes.to_be_bytes());
    hasher.update(mode.id().as_bytes());
    for target in targets {
        hasher.update(target.class.as_bytes());
        hasher.update(target.read_version.to_be_bytes());
        hasher.update([u8::from(target.point_reads_exact)]);
        hasher.update([u8::from(target.ordered_scans_exact)]);
        hasher.update(target.warm_scan_rows.to_be_bytes());
        hasher.update(target.cold_scan_rows.to_be_bytes());
    }
    hasher.update([
        u8::from(tombstone_exact),
        u8::from(future_frontier_refused),
        u8::from(binary_key_order_exact),
        u8::from(close_reopen_exact),
    ]);
    format!("{:x}", hasher.finalize())
}
