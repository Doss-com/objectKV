//! Process-isolated shared-cache eviction contract for many logical ranges.

use crate::range_serving_view::{AuthorityBoundRangeView, AuthorityRangeRoot};
use futures_util::TryStreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_slate::{
    inspect_latest_physical_manifest, AuthorityManifestReference, CountingStore, IoCounters,
    Phase0IoDelta, SlateEngine,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::Settings;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::Db;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const FORMAT_VERSION: u16 = 1;
const TARGET_VERSION: u64 = 1;

/// One deliberately unsafe shared-cache eviction subject.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheEvictionMode {
    #[default]
    Correct,
    DisablePhysicalBound,
    SkipReread,
    AcceptWrongValue,
}

impl RangeCacheEvictionMode {
    /// Stable mode name for eval receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DisablePhysicalBound => "disable_physical_bound",
            Self::SkipReread => "skip_reread",
            Self::AcceptWrongValue => "accept_wrong_value",
        }
    }
}

/// Backing object store used by the eviction worker.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheEvictionBackend {
    #[default]
    Local,
    Gcs,
}

impl RangeCacheEvictionBackend {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Gcs => "gcs",
        }
    }
}

/// Configuration for one disposable multi-range eviction worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheEvictionConfig {
    pub backend: RangeCacheEvictionBackend,
    pub range_count: usize,
    pub keys_per_range: usize,
    pub value_bytes: usize,
    pub cache_limit_bytes: usize,
    pub cache_part_bytes: usize,
    pub decoded_cache_bytes: u64,
    pub seed: u64,
    pub mode: RangeCacheEvictionMode,
}

/// Stable worker receipt for one shared-cache eviction run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheReadOutcome {
    Exact,
    Wrong,
}

impl RangeCacheReadOutcome {
    #[must_use]
    pub const fn is_exact(self) -> bool {
        matches!(self, Self::Exact)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheBoundOutcome {
    Held,
    Exceeded,
}

impl RangeCacheBoundOutcome {
    #[must_use]
    pub const fn held(self) -> bool {
        matches!(self, Self::Held)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheRefillOutcome {
    Observed,
    Missing,
}

impl RangeCacheRefillOutcome {
    #[must_use]
    pub const fn observed(self) -> bool {
        matches!(self, Self::Observed)
    }
}

/// Stable worker receipt for one shared-cache eviction run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheEvictionReceipt {
    pub format_version: u16,
    pub mode: RangeCacheEvictionMode,
    pub backend: RangeCacheEvictionBackend,
    pub seed: u64,
    pub range_count: usize,
    pub first_pass_ranges: usize,
    pub reread_ranges: usize,
    pub shared_cache_roots: u64,
    pub cache_limit_bytes: u64,
    pub settled_cache_bytes: u64,
    pub settled_cache_parts: u64,
    pub first_pass_backend_get_ranges: u64,
    pub reread_backend_get_ranges: u64,
    pub reread_backend_bytes: u64,
    pub scratch_objects_deleted: u64,
    pub first_pass: RangeCacheReadOutcome,
    pub reread: RangeCacheReadOutcome,
    pub cache_bound: RangeCacheBoundOutcome,
    pub eviction_refill: RangeCacheRefillOutcome,
    pub trace_sha256: String,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64) -> Result<Self, String> {
        let root = tempfile::Builder::new()
            .prefix(&format!("okv-range-cache-eviction-{seed}-"))
            .tempdir()
            .map_err(|error| error.to_string())?
            .keep();
        Ok(Self(root))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-range-cache-eviction-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Build one immutable base, force many logical ranges through one bounded
/// persistent cache, then reread every range with fresh decoded RAM.
///
/// # Errors
///
/// Returns an error when fixture construction or the storage path cannot run.
/// Semantic disagreements remain explicit in the receipt.
#[allow(clippy::too_many_lines)]
pub async fn run_range_cache_eviction_worker(
    config: &RangeCacheEvictionConfig,
) -> Result<RangeCacheEvictionReceipt, String> {
    validate_config(config)?;
    let root = TempRoot::new(config.seed)?;
    let object_root = root.0.join("objects");
    let cache_root = root.0.join("shared-nvme");
    let raw_backend = build_backing_store(config, &object_root)?;
    let counters = Arc::new(IoCounters::default());
    let backend: Arc<dyn ObjectStore> =
        Arc::new(CountingStore::new(raw_backend, Arc::clone(&counters)));
    let database_path = "range-cache-eviction";
    let engine = build_engine(database_path, Arc::clone(&backend), config.seed).await?;
    let mutations = fixture_mutations(config);
    engine
        .apply(CommitBatch {
            version: Version::new(TARGET_VERSION),
            identity: CommitIdentity::for_test(config.seed.max(1)),
            mutations,
        })
        .await
        .map_err(|error| error.to_string())?;
    engine.flush().await.map_err(|error| error.to_string())?;
    let physical =
        inspect_latest_physical_manifest(Arc::clone(&backend), database_path, config.seed ^ 0xe711)
            .await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let authority_root = AuthorityRangeRoot {
        cell_id: [0x51; 16],
        tenant_id: [0x71; 16],
        generation: 1,
        manifest: AuthorityManifestReference {
            key: physical.manifest.key,
            length: physical.manifest.length,
            sha256: physical.manifest.sha256,
        },
        covered_through: TARGET_VERSION,
        minimum_readable_version: TARGET_VERSION,
        log_chain_sha256: [0; 32],
    };
    let cached_store = build_cached_store(&cache_root, Arc::clone(&backend), config).await?;
    let before_first = counters.total();
    let first_view = open_view(
        database_path,
        Arc::clone(&cached_store),
        authority_root.clone(),
        config,
        config.seed ^ 0xe712,
    )
    .await?;
    let mut first_pass_exact = true;
    for range in 0..config.range_count {
        first_pass_exact &= scan_range_exact(&first_view, config, range).await?;
    }
    first_view
        .close()
        .await
        .map_err(|error| error.to_string())?;
    let after_first = counters.total();
    let first_pass_io = after_first.difference_since(&before_first);
    if config.mode == RangeCacheEvictionMode::DisablePhysicalBound {
        let _ = cache_inventory(&cache_root)?;
    } else {
        let _ = wait_for_cache_bound(&cache_root, config.cache_limit_bytes).await?;
    }

    let before_reread = counters.total();
    let mut reread_ranges = 0_usize;
    let mut reread_exact = true;
    if config.mode != RangeCacheEvictionMode::SkipReread {
        let reread_view = open_view(
            database_path,
            Arc::clone(&cached_store),
            authority_root,
            config,
            config.seed ^ 0xe713,
        )
        .await?;
        for range in (0..config.range_count).rev() {
            reread_exact &= scan_range_exact(&reread_view, config, range).await?;
            reread_ranges = reread_ranges.saturating_add(1);
        }
        reread_view
            .close()
            .await
            .map_err(|error| error.to_string())?;
    }
    if config.mode == RangeCacheEvictionMode::AcceptWrongValue {
        reread_exact = false;
    }
    let after_reread = counters.total();
    let reread_io = after_reread.difference_since(&before_reread);
    let inventory = if config.mode == RangeCacheEvictionMode::DisablePhysicalBound {
        cache_inventory(&cache_root)?
    } else {
        wait_for_cache_bound(&cache_root, config.cache_limit_bytes).await?
    };
    let bounded_subject = config.mode != RangeCacheEvictionMode::DisablePhysicalBound;
    let cache_bound_held = bounded_subject && inventory.bytes <= config.cache_limit_bytes as u64;
    let reread_get_ranges = successful_get_ranges(&reread_io);
    let eviction_refill_observed =
        cache_bound_held && reread_ranges == config.range_count && reread_get_ranges > 0;
    let semantic = (
        FORMAT_VERSION,
        config.mode,
        config.backend,
        config.seed,
        config.range_count,
        config.keys_per_range,
        first_pass_exact,
        reread_exact,
        reread_ranges,
        cache_bound_held,
        eviction_refill_observed,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    let scratch_objects_deleted = if config.backend == RangeCacheEvictionBackend::Gcs {
        cleanup_remote_scratch(Arc::clone(&backend)).await?
    } else {
        0
    };
    Ok(RangeCacheEvictionReceipt {
        format_version: FORMAT_VERSION,
        mode: config.mode,
        backend: config.backend,
        seed: config.seed,
        range_count: config.range_count,
        first_pass_ranges: config.range_count,
        reread_ranges,
        shared_cache_roots: 1,
        cache_limit_bytes: u64::try_from(config.cache_limit_bytes).unwrap_or(u64::MAX),
        settled_cache_bytes: inventory.bytes,
        settled_cache_parts: inventory.parts,
        first_pass_backend_get_ranges: successful_get_ranges(&first_pass_io),
        reread_backend_get_ranges: reread_get_ranges,
        reread_backend_bytes: reread_io.read_byte_total(),
        scratch_objects_deleted,
        first_pass: if first_pass_exact {
            RangeCacheReadOutcome::Exact
        } else {
            RangeCacheReadOutcome::Wrong
        },
        reread: if reread_exact {
            RangeCacheReadOutcome::Exact
        } else {
            RangeCacheReadOutcome::Wrong
        },
        cache_bound: if cache_bound_held {
            RangeCacheBoundOutcome::Held
        } else {
            RangeCacheBoundOutcome::Exceeded
        },
        eviction_refill: if eviction_refill_observed {
            RangeCacheRefillOutcome::Observed
        } else {
            RangeCacheRefillOutcome::Missing
        },
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn validate_config(config: &RangeCacheEvictionConfig) -> Result<(), String> {
    if config.range_count < 2
        || config.keys_per_range == 0
        || config.value_bytes < 1_024
        || config.cache_limit_bytes < config.cache_part_bytes.saturating_mul(2)
        || config.cache_part_bytes == 0
        || !config.cache_part_bytes.is_multiple_of(1_024)
        || config.decoded_cache_bytes == 0
    {
        return Err("multi-range eviction requires at least two ranges, nonzero keys, 1 KiB values, a cache of at least two aligned parts, and decoded cache capacity".to_owned());
    }
    Ok(())
}

fn build_backing_store(
    config: &RangeCacheEvictionConfig,
    local_root: &Path,
) -> Result<Arc<dyn ObjectStore>, String> {
    match config.backend {
        RangeCacheEvictionBackend::Local => {
            fs::create_dir_all(local_root).map_err(|error| error.to_string())?;
            LocalFileSystem::new_with_prefix(local_root)
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .map_err(|error| error.to_string())
        }
        RangeCacheEvictionBackend::Gcs => {
            let bucket = std::env::var("OKV_GCS_BUCKET")
                .map_err(|_| "GCS eviction profile requires OKV_GCS_BUCKET".to_owned())?;
            let store = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| error.to_string())?;
            let prefix = format!(
                "scratch/range-cache-eviction/{}/{}/{}",
                config.mode.id(),
                config.seed,
                Uuid::new_v4()
            );
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        }
    }
}

async fn cleanup_remote_scratch(store: Arc<dyn ObjectStore>) -> Result<u64, String> {
    let mut objects = store.list(None);
    let mut locations = Vec::new();
    while let Some(meta) = objects
        .try_next()
        .await
        .map_err(|error| error.to_string())?
    {
        locations.push(meta.location);
    }
    drop(objects);
    let mut deleted = 0_u64;
    for location in locations {
        store
            .delete(&location)
            .await
            .map_err(|error| error.to_string())?;
        deleted = deleted.saturating_add(1);
    }
    Ok(deleted)
}

async fn build_engine(
    database_path: &str,
    store: Arc<dyn ObjectStore>,
    seed: u64,
) -> Result<SlateEngine, String> {
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(database_path, store)
        .with_settings(settings)
        .with_seed(seed ^ 0xe700)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| error.to_string())
}

async fn build_cached_store(
    root: &Path,
    backend: Arc<dyn ObjectStore>,
    config: &RangeCacheEvictionConfig,
) -> Result<Arc<dyn ObjectStore>, String> {
    let maximum = (config.mode != RangeCacheEvictionMode::DisablePhysicalBound)
        .then_some(config.cache_limit_bytes);
    CachedObjectStore::builder(root, backend)
        .with_max_cache_size_bytes(maximum)
        .with_part_size_bytes(config.cache_part_bytes)
        .with_cache_on_flush(false)
        .with_scan_interval(None)
        .with_max_open_file_handles(16)
        .build()
        .await
        .map(|store| store as Arc<dyn ObjectStore>)
        .map_err(|error| error.to_string())
}

async fn open_view(
    database_path: &str,
    store: Arc<dyn ObjectStore>,
    root: AuthorityRangeRoot,
    config: &RangeCacheEvictionConfig,
    seed: u64,
) -> Result<AuthorityBoundRangeView, String> {
    let decoded: Arc<dyn DbCache> = Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: config.decoded_cache_bytes,
        time_to_live: None,
        time_to_idle: None,
    }));
    AuthorityBoundRangeView::open_with_cache(
        database_path,
        store,
        root,
        TARGET_VERSION,
        Vec::new(),
        &BTreeMap::new(),
        seed,
        decoded,
    )
    .await
    .map_err(|error| error.to_string())
}

fn fixture_mutations(config: &RangeCacheEvictionConfig) -> Vec<Mutation> {
    let mut mutations =
        Vec::with_capacity(config.range_count.saturating_mul(config.keys_per_range));
    for range in 0..config.range_count {
        for ordinal in 0..config.keys_per_range {
            mutations.push(Mutation::Set {
                key: range_key(range, ordinal),
                value: range_value(config, range, ordinal),
            });
        }
    }
    mutations
}

async fn scan_range_exact(
    view: &AuthorityBoundRangeView,
    config: &RangeCacheEvictionConfig,
    range: usize,
) -> Result<bool, String> {
    let observed = view
        .scan_at(
            &range_start(range),
            &range_end(range),
            TARGET_VERSION,
            config.keys_per_range,
        )
        .await
        .map_err(|error| error.to_string())?;
    let expected = (0..config.keys_per_range)
        .map(|ordinal| {
            (
                range_key(range, ordinal),
                range_value(config, range, ordinal),
            )
        })
        .collect::<Vec<_>>();
    Ok(observed == expected)
}

fn range_key(range: usize, ordinal: usize) -> Vec<u8> {
    format!("r/{range:05}/{ordinal:05}").into_bytes()
}

fn range_start(range: usize) -> Vec<u8> {
    format!("r/{range:05}/").into_bytes()
}

fn range_end(range: usize) -> Vec<u8> {
    format!("r/{range:05}/~").into_bytes()
}

fn range_value(config: &RangeCacheEvictionConfig, range: usize, ordinal: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(config.value_bytes);
    let mut block = 0_u64;
    while value.len() < config.value_bytes {
        let mut hasher = Sha256::new();
        hasher.update(config.seed.to_be_bytes());
        hasher.update(range.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(block.to_be_bytes());
        value.extend_from_slice(&hasher.finalize());
        block = block.saturating_add(1);
    }
    value.truncate(config.value_bytes);
    value
}

#[derive(Clone, Copy)]
struct CacheInventory {
    bytes: u64,
    parts: u64,
}

async fn wait_for_cache_bound(root: &Path, limit: usize) -> Result<CacheInventory, String> {
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let inventory = cache_inventory(root)?;
        if inventory.bytes <= limit || tokio::time::Instant::now() >= deadline {
            return Ok(inventory);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn cache_inventory(root: &Path) -> Result<CacheInventory, String> {
    let mut files = Vec::new();
    collect_files(root, &mut files).map_err(|error| error.to_string())?;
    let mut bytes = 0_u64;
    let mut parts = 0_u64;
    for path in files {
        bytes = bytes.saturating_add(
            fs::metadata(&path)
                .map_err(|error| error.to_string())?
                .len(),
        );
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("_part"))
        {
            parts = parts.saturating_add(1);
        }
    }
    Ok(CacheInventory { bytes, parts })
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_files(&entry.path(), files)?;
        } else if file_type.is_file() {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn successful_get_ranges(io: &Phase0IoDelta) -> u64 {
    io.successful_requests
        .get("get_range")
        .copied()
        .unwrap_or(0)
}
