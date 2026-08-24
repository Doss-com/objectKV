//! Isolated performance curve for the authority-bound base plus certified tail.

use crate::{
    bind_provider_physical_manifest, promote_provider_bound_persistent_range_base,
    provider_closure_digest, AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord,
    PersistentRangeBaseDescriptor, ProviderBoundObjectStore, ProviderBoundReadStats, ProviderKind,
    RevisionToken,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_consensus::{
    sign_tagged_log_statement, tagged_log_public_key, CellLogSetMember, CellLogSetPolicy,
    CellMutation, CellTaggedLogCertificate, CellTaggedLogStatement, RequestIdentity,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
use okv_slate::{
    inspect_latest_physical_manifest, AuthorityManifestReference, CountingStore, IoCounters,
    MvccGcPhysicalManifestReceipt, Phase0IoDelta, SlateEngine, SLATEDB_REVISION,
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
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const DATABASE_PATH: &str = "range-serving-curve";
const CELL_ID: [u8; 16] = [0x51; 16];
const TENANT_ID: [u8; 16] = [0x71; 16];
const GENERATION: u64 = 1;
const LOG_SET_ID: u16 = 10;
const BASE_BATCH_KEYS: usize = 256;
const GCS_SCRATCH_PREFIX: &str = "scratch/provider-bound-range/";

/// Physical object backend used by one isolated range-serving worker.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingObjectBackend {
    #[default]
    Local,
    Gcs,
}

impl RangeServingObjectBackend {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Gcs => "gcs",
        }
    }
}

/// Physical cache path under test for one disposable Range Engine.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingCacheMode {
    Raw,
    SharedRamNvme,
    MetadataReopen,
    NvmeReopen,
}

impl RangeServingCacheMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::SharedRamNvme => "shared_ram_nvme",
            Self::MetadataReopen => "metadata_reopen",
            Self::NvmeReopen => "nvme_reopen",
        }
    }
}

/// Provider identity mode for the range-serving worker.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingProviderMode {
    #[default]
    Unbound,
    Correct,
    ChangedGeneration,
    SameBytesNewGeneration,
    MissingRevision,
    ChangedBytes,
    ChangedNamespace,
    SkipRevisionEnforcement,
}

impl RangeServingProviderMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Unbound => "unbound",
            Self::Correct => "correct",
            Self::ChangedGeneration => "changed_generation",
            Self::SameBytesNewGeneration => "same_bytes_new_generation",
            Self::MissingRevision => "missing_revision",
            Self::ChangedBytes => "changed_bytes",
            Self::ChangedNamespace => "changed_namespace",
            Self::SkipRevisionEnforcement => "skip_revision_enforcement",
        }
    }

    const fn enabled(self) -> bool {
        !matches!(self, Self::Unbound)
    }
}

/// One isolated curve point. Cache state is process-cold with an OS-warm local
/// filesystem, then warm within the same immutable view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingCurveConfig {
    pub base_key_count: usize,
    pub value_bytes: usize,
    pub tail_records: usize,
    pub point_samples: usize,
    pub scan_rows: usize,
    pub max_rss_bytes: u64,
    pub cache_mode: RangeServingCacheMode,
    pub decoded_cache_bytes: u64,
    pub nvme_cache_bytes: usize,
    pub nvme_part_bytes: usize,
    pub nvme_open_file_handles: usize,
    #[serde(default)]
    pub provider_mode: RangeServingProviderMode,
    #[serde(default)]
    pub object_backend: RangeServingObjectBackend,
    #[serde(default)]
    pub scratch_prefix: Option<String>,
    #[serde(default)]
    pub warmup_reads: usize,
    #[serde(default)]
    pub measured_reads: usize,
    pub seed: u64,
}

/// Stable measurements and semantic checks from one fresh worker process.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RangeServingCurveReceipt {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub cache_model: String,
    pub cache_mode: String,
    pub seed: u64,
    pub base_key_count: usize,
    pub value_bytes: usize,
    pub base_frontier: u64,
    pub tail_records: usize,
    pub point_samples: usize,
    pub target_version: u64,
    pub base_logical_bytes: u64,
    pub ingest_flush_seconds: f64,
    pub view_open_seconds: f64,
    pub base_open_seconds: f64,
    pub tail_auth_seconds: f64,
    pub first_point_seconds: f64,
    pub warm_point_p50_seconds: f64,
    pub warm_point_p99_seconds: f64,
    pub scan_seconds: f64,
    pub scan_rows: usize,
    pub scan_rows_per_second: f64,
    pub first_point_exact: bool,
    pub warm_points_exact: bool,
    pub ordered_scan_exact: bool,
    pub authenticated_tail_records: u64,
    pub cache_prepare_io: Phase0IoDelta,
    pub open_io: Phase0IoDelta,
    pub first_point_io: Phase0IoDelta,
    pub fill_point_io: Phase0IoDelta,
    pub warm_point_io: Phase0IoDelta,
    pub scan_io: Phase0IoDelta,
    pub total_io: Phase0IoDelta,
    pub provider_mode: String,
    pub object_backend: String,
    pub provider_closure_sha256: String,
    pub provider_get_requests: u64,
    pub provider_revision_checks: u64,
    pub provider_refused_requests: u64,
    pub provider_read_bytes: u64,
    pub unversioned_fallbacks: u64,
    pub scratch_objects_deleted: u64,
    pub scratch_cleanup_complete: bool,
    pub peak_rss_bytes: u64,
    pub safety_bounds_held: bool,
    pub semantic_receipt_sha256: String,
}

/// Build and measure one authority-selected object base plus real certified
/// txLog suffix in the current process.
///
/// # Errors
///
/// Returns an error for invalid bounds, storage failures, or failed serving
/// construction. Read disagreements remain explicit booleans in the receipt.
#[allow(clippy::too_many_lines)]
pub async fn run_range_serving_curve_worker(
    config: &RangeServingCurveConfig,
) -> Result<RangeServingCurveReceipt, String> {
    validate_config(config)?;
    let root = tempfile::Builder::new()
        .prefix("okv-range-serving-curve-")
        .tempdir()
        .map_err(|error| format!("create range-serving curve root: {error}"))?;
    let object_root = root.path().join("objects");
    let nvme_root = root.path().join("nvme-cache");
    let (raw_store, provider_kind, provider_namespace) = build_object_store(config, &object_root)?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> =
        Arc::new(CountingStore::new(raw_store, Arc::clone(&counters)));
    let result = Box::pin(run_range_serving_curve_inner(
        config,
        &nvme_root,
        Arc::clone(&store),
        Arc::clone(&counters),
        provider_kind,
        &provider_namespace,
    ))
    .await;
    let cleanup = cleanup_scratch(config, Arc::clone(&store)).await;
    match (result, cleanup) {
        (Ok(mut receipt), Ok(deleted)) => {
            receipt.scratch_objects_deleted = deleted;
            receipt.scratch_cleanup_complete = true;
            Ok(receipt)
        }
        (Ok(_), Err(cleanup_error)) => Err(format!(
            "range-serving curve scratch cleanup failed: {cleanup_error}"
        )),
        (Err(error), Ok(_)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(format!(
            "{error}; range-serving curve scratch cleanup also failed: {cleanup_error}"
        )),
    }
}

#[allow(clippy::too_many_lines)]
async fn run_range_serving_curve_inner(
    config: &RangeServingCurveConfig,
    nvme_root: &Path,
    store: Arc<dyn ObjectStore>,
    counters: Arc<IoCounters>,
    provider_kind: ProviderKind,
    provider_namespace: &str,
) -> Result<RangeServingCurveReceipt, String> {
    let engine = build_engine(Arc::clone(&store), config.seed).await?;
    let mut oracle = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut prior_chain = [0_u8; 32];
    let ingest_started = Instant::now();
    let mut base_frontier = 0_u64;
    for (batch_index, first) in (0..config.base_key_count)
        .step_by(BASE_BATCH_KEYS)
        .enumerate()
    {
        let sequence = u64::try_from(batch_index + 1).map_err(|error| error.to_string())?;
        let end = first
            .saturating_add(BASE_BATCH_KEYS)
            .min(config.base_key_count);
        let mutations = (first..end)
            .map(|ordinal| {
                let key = key_for(ordinal);
                let value = value_for(config, ordinal, 0);
                oracle.insert(key.clone(), value.clone());
                CellMutation::Set { key, value }
            })
            .collect::<Vec<_>>();
        let envelope = envelope(sequence, prior_chain, &mutations)?;
        prior_chain = Sha256::digest(envelope.encode()).into();
        engine
            .apply(slate_batch(&envelope)?)
            .await
            .map_err(|error| format!("apply curve base batch at version {sequence}: {error}"))?;
        base_frontier = sequence;
    }
    engine
        .flush()
        .await
        .map_err(|error| format!("flush curve base: {error}"))?;
    let ingest_flush_seconds = ingest_started.elapsed().as_secs_f64();
    let physical =
        inspect_latest_physical_manifest(Arc::clone(&store), DATABASE_PATH, config.seed ^ 0x6a11)
            .await?;
    engine
        .close()
        .await
        .map_err(|error| format!("close curve writer: {error}"))?;

    let manifest = AuthorityManifestReference {
        key: physical.manifest.key.clone(),
        length: physical.manifest.length,
        sha256: physical.manifest.sha256.clone(),
    };
    let range_root = AuthorityRangeRoot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        manifest,
        covered_through: base_frontier,
        minimum_readable_version: 1,
        log_chain_sha256: prior_chain,
    };
    let (policy, signing_seeds) = log_policy()?;
    let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
    let mut records = Vec::with_capacity(config.tail_records);
    for tail_index in 0..config.tail_records {
        let sequence =
            base_frontier.saturating_add(u64::try_from(tail_index + 1).unwrap_or(u64::MAX));
        let ordinal = tail_ordinal(config.seed, tail_index, config.base_key_count);
        let key = key_for(ordinal);
        let value = value_for(config, ordinal, sequence);
        oracle.insert(key.clone(), value.clone());
        let commit = envelope(sequence, prior_chain, &[CellMutation::Set { key, value }])?;
        prior_chain = Sha256::digest(commit.encode()).into();
        records.push(certified_record(&commit, &policy, &signing_seeds)?);
    }
    let target_version =
        base_frontier.saturating_add(u64::try_from(config.tail_records).unwrap_or(u64::MAX));
    let (warmup_ordinals, measured_ordinals) = point_workload_ordinals(config);
    let (source_store, provider_store, provider_closure_sha256, unversioned_fallbacks) =
        prepare_provider_store(
            config,
            Arc::clone(&store),
            &physical,
            &range_root,
            provider_kind,
            provider_namespace,
        )
        .await?;
    let before_cache_prepare = counters.total();
    let (serving_store, decoded_cache) = match config.cache_mode {
        RangeServingCacheMode::Raw => (Arc::clone(&source_store), None),
        RangeServingCacheMode::SharedRamNvme => (
            build_nvme_store(nvme_root, Arc::clone(&source_store), config).await?,
            Some(new_decoded_cache(config)),
        ),
        RangeServingCacheMode::MetadataReopen => {
            let prepare_store =
                build_nvme_store(nvme_root, Arc::clone(&source_store), config).await?;
            let prepare_view = AuthorityBoundRangeView::open_with_cache(
                DATABASE_PATH,
                prepare_store,
                range_root.clone(),
                target_version,
                records.clone(),
                &policies,
                config.seed ^ 0x5e11,
                new_decoded_cache(config),
            )
            .await
            .map_err(|error| format!("open metadata cache preparation view: {error}"))?;
            prepare_view
                .close()
                .await
                .map_err(|error| format!("close metadata cache preparation view: {error}"))?;
            (
                build_nvme_store(nvme_root, Arc::clone(&source_store), config).await?,
                Some(new_decoded_cache(config)),
            )
        }
        RangeServingCacheMode::NvmeReopen => {
            let prepare_store =
                build_nvme_store(nvme_root, Arc::clone(&source_store), config).await?;
            let prepare_view = AuthorityBoundRangeView::open_with_cache(
                DATABASE_PATH,
                prepare_store,
                range_root.clone(),
                target_version,
                records.clone(),
                &policies,
                config.seed ^ 0x6e11,
                new_decoded_cache(config),
            )
            .await
            .map_err(|error| format!("open NVMe cache preparation view: {error}"))?;
            for ordinal in &warmup_ordinals {
                let observed = prepare_view
                    .get_at(&key_for(*ordinal), target_version)
                    .await
                    .map_err(|error| format!("prepare NVMe point cache: {error}"))?;
                if observed.as_ref() != oracle.get(&key_for(*ordinal)) {
                    return Err("NVMe cache preparation point disagrees with oracle".to_owned());
                }
            }
            let prepared_scan = prepare_view
                .scan_at(b"k/", b"k0", target_version, config.scan_rows)
                .await
                .map_err(|error| format!("prepare NVMe scan cache: {error}"))?;
            let expected_scan = oracle
                .iter()
                .take(config.scan_rows)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            if prepared_scan != expected_scan {
                return Err("NVMe cache preparation scan disagrees with oracle".to_owned());
            }
            prepare_view
                .close()
                .await
                .map_err(|error| format!("close NVMe cache preparation view: {error}"))?;
            (
                build_nvme_store(nvme_root, Arc::clone(&source_store), config).await?,
                Some(new_decoded_cache(config)),
            )
        }
    };
    let after_cache_prepare = counters.total();
    let cache_prepare_io = after_cache_prepare.difference_since(&before_cache_prepare);
    let before_open = after_cache_prepare;
    let open_started = Instant::now();
    let view = match decoded_cache {
        Some(cache) => {
            AuthorityBoundRangeView::open_with_cache(
                DATABASE_PATH,
                serving_store,
                range_root,
                target_version,
                records,
                &policies,
                config.seed ^ 0x7e11,
                cache,
            )
            .await
        }
        None => {
            AuthorityBoundRangeView::open(
                DATABASE_PATH,
                serving_store,
                range_root,
                target_version,
                records,
                &policies,
                config.seed ^ 0x7e11,
            )
            .await
        }
    }
    .map_err(|error| format!("open range-serving curve view: {error}"))?;
    let view_open_seconds = open_started.elapsed().as_secs_f64();
    let after_open = counters.total();

    let first_ordinal = measured_ordinals[0];
    let first_started = Instant::now();
    let first_value = view
        .get_at(&key_for(first_ordinal), target_version)
        .await
        .map_err(|error| format!("first curve point read: {error}"))?;
    let first_point_seconds = first_started.elapsed().as_secs_f64();
    let first_point_exact = first_value.as_ref() == oracle.get(&key_for(first_ordinal));
    let after_first = counters.total();

    let mut warm_points_exact = true;
    for ordinal in &warmup_ordinals {
        let observed = view
            .get_at(&key_for(*ordinal), target_version)
            .await
            .map_err(|error| format!("fill curve point cache: {error}"))?;
        warm_points_exact &= observed.as_ref() == oracle.get(&key_for(*ordinal));
    }
    let after_fill = counters.total();

    let mut warm_seconds = Vec::with_capacity(measured_ordinals.len());
    for ordinal in measured_ordinals {
        let started = Instant::now();
        let observed = view
            .get_at(&key_for(ordinal), target_version)
            .await
            .map_err(|error| format!("warm curve point read: {error}"))?;
        warm_seconds.push(started.elapsed().as_secs_f64());
        warm_points_exact &= observed.as_ref() == oracle.get(&key_for(ordinal));
    }
    let after_warm = counters.total();

    let scan_started = Instant::now();
    let observed_scan = view
        .scan_at(b"k/", b"k0", target_version, config.scan_rows)
        .await
        .map_err(|error| format!("curve ordered scan: {error}"))?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let expected_scan = oracle
        .iter()
        .take(config.scan_rows)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let ordered_scan_exact = observed_scan == expected_scan;
    let after_scan = counters.total();
    view.close()
        .await
        .map_err(|error| format!("close curve view: {error}"))?;

    warm_seconds.sort_by(f64::total_cmp);
    let provider_stats = provider_store
        .as_ref()
        .map_or_else(ProviderBoundReadStats::default, |store| store.stats());
    let provider_read_bytes = after_scan
        .difference_since(&before_cache_prepare)
        .read_byte_total();
    let peak_rss_bytes = resident_memory_bytes();
    let safety_bounds_held = peak_rss_bytes > 0 && peak_rss_bytes <= config.max_rss_bytes;
    let base_logical_bytes = u64::try_from(config.base_key_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(config.value_bytes).unwrap_or(u64::MAX));
    let semantic_receipt_sha256 = semantic_receipt(
        config,
        base_frontier,
        target_version,
        first_point_exact,
        warm_points_exact,
        ordered_scan_exact,
    );
    Ok(RangeServingCurveReceipt {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        cache_model: match (config.cache_mode, config.object_backend) {
            (RangeServingCacheMode::Raw, RangeServingObjectBackend::Local) => {
                "process-cold-os-warm-local-filesystem"
            }
            (RangeServingCacheMode::Raw, RangeServingObjectBackend::Gcs) => {
                "process-cold-gcs-origin"
            }
            (RangeServingCacheMode::SharedRamNvme, RangeServingObjectBackend::Local) => {
                "process-cold-shared-ram-nvme-os-warm-local-filesystem"
            }
            (RangeServingCacheMode::SharedRamNvme, RangeServingObjectBackend::Gcs) => {
                "process-cold-shared-ram-nvme-gcs-origin"
            }
            (RangeServingCacheMode::MetadataReopen, RangeServingObjectBackend::Local) => {
                "metadata-prepared-data-cold-decoded-ram-cold-os-warm-local-filesystem"
            }
            (RangeServingCacheMode::MetadataReopen, RangeServingObjectBackend::Gcs) => {
                "metadata-prepared-data-cold-decoded-ram-cold-gcs-origin"
            }
            (RangeServingCacheMode::NvmeReopen, RangeServingObjectBackend::Local) => {
                "decoded-ram-cold-nvme-reopen-os-warm-local-filesystem"
            }
            (RangeServingCacheMode::NvmeReopen, RangeServingObjectBackend::Gcs) => {
                "decoded-ram-cold-nvme-reopen-gcs-origin"
            }
        }
        .to_owned(),
        cache_mode: config.cache_mode.id().to_owned(),
        seed: config.seed,
        base_key_count: config.base_key_count,
        value_bytes: config.value_bytes,
        base_frontier,
        tail_records: config.tail_records,
        point_samples: warm_seconds.len(),
        target_version,
        base_logical_bytes,
        ingest_flush_seconds,
        view_open_seconds,
        base_open_seconds: view.base_open_seconds(),
        tail_auth_seconds: view.tail_auth_seconds(),
        first_point_seconds,
        warm_point_p50_seconds: percentile(&warm_seconds, 50),
        warm_point_p99_seconds: percentile(&warm_seconds, 99),
        scan_seconds,
        scan_rows: observed_scan.len(),
        scan_rows_per_second: f64::from(u32::try_from(observed_scan.len()).unwrap_or(u32::MAX))
            / scan_seconds.max(f64::EPSILON),
        first_point_exact,
        warm_points_exact,
        ordered_scan_exact,
        authenticated_tail_records: view.authenticated_tail_records(),
        cache_prepare_io,
        open_io: after_open.difference_since(&before_open),
        first_point_io: after_first.difference_since(&after_open),
        fill_point_io: after_fill.difference_since(&after_first),
        warm_point_io: after_warm.difference_since(&after_fill),
        scan_io: after_scan.difference_since(&after_warm),
        total_io: after_scan,
        provider_mode: config.provider_mode.id().to_owned(),
        object_backend: config.object_backend.id().to_owned(),
        provider_closure_sha256,
        provider_get_requests: provider_stats.get_requests,
        provider_revision_checks: provider_stats.revision_checks,
        provider_refused_requests: provider_stats.refused_requests,
        provider_read_bytes,
        unversioned_fallbacks,
        scratch_objects_deleted: 0,
        scratch_cleanup_complete: config.object_backend == RangeServingObjectBackend::Local,
        peak_rss_bytes,
        safety_bounds_held,
        semantic_receipt_sha256,
    })
}

fn validate_config(config: &RangeServingCurveConfig) -> Result<(), String> {
    if config.base_key_count == 0
        || config.value_bytes < 16
        || config.point_samples < 2
        || config.scan_rows == 0
        || config.scan_rows > config.base_key_count
        || config.max_rss_bytes == 0
        || config.decoded_cache_bytes == 0
        || config.nvme_cache_bytes == 0
        || config.nvme_part_bytes == 0
        || config.nvme_open_file_handles == 0
    {
        return Err("range-serving curve requires nonzero base/RSS, value_bytes >= 16, at least two points, and a bounded nonzero scan".to_owned());
    }
    match (config.object_backend, config.scratch_prefix.as_deref()) {
        (RangeServingObjectBackend::Local, None) => {}
        (RangeServingObjectBackend::Gcs, Some(prefix)) if valid_gcs_scratch_prefix(prefix) => {}
        (RangeServingObjectBackend::Local, Some(_)) => {
            return Err(
                "local range-serving curve must not declare a remote scratch prefix".to_owned(),
            )
        }
        (RangeServingObjectBackend::Gcs, _) => {
            return Err("GCS range-serving curve requires a guarded scratch prefix".to_owned())
        }
    }
    Ok(())
}

fn valid_gcs_scratch_prefix(prefix: &str) -> bool {
    prefix.starts_with(GCS_SCRATCH_PREFIX)
        && prefix.len() > GCS_SCRATCH_PREFIX.len()
        && prefix.len() <= 512
        && !prefix.contains("..")
        && !prefix.contains("//")
        && !prefix.ends_with('/')
}

fn build_object_store(
    config: &RangeServingCurveConfig,
    local_root: &Path,
) -> Result<(Arc<dyn ObjectStore>, ProviderKind, String), String> {
    match config.object_backend {
        RangeServingObjectBackend::Local => {
            fs::create_dir_all(local_root)
                .map_err(|error| format!("create curve object root: {error}"))?;
            let local = LocalFileSystem::new_with_prefix(local_root)
                .map_err(|error| format!("open curve object root: {error}"))?;
            Ok((
                Arc::new(local),
                ProviderKind::VersionedTest,
                "local-versioned-fixture".to_owned(),
            ))
        }
        RangeServingObjectBackend::Gcs => {
            let project = std::env::var("OKV_GCP_PROJECT")
                .map_err(|_| "GCS range-serving curve requires OKV_GCP_PROJECT".to_owned())?;
            let bucket = std::env::var("OKV_GCS_BUCKET")
                .map_err(|_| "GCS range-serving curve requires OKV_GCS_BUCKET".to_owned())?;
            if project.trim().is_empty() || bucket.trim().is_empty() {
                return Err(
                    "GCS range-serving curve requires nonempty project and bucket".to_owned(),
                );
            }
            let prefix = config
                .scratch_prefix
                .as_deref()
                .ok_or_else(|| "GCS range-serving curve requires a scratch prefix".to_owned())?;
            let store = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(&bucket)
                .build()
                .map_err(|error| format!("build GCS range-serving store: {error}"))?;
            let namespace = format!("gs://{bucket}/{prefix}");
            Ok((
                Arc::new(PrefixStore::new(store, prefix)),
                ProviderKind::Gcs,
                namespace,
            ))
        }
    }
}

async fn cleanup_scratch(
    config: &RangeServingCurveConfig,
    store: Arc<dyn ObjectStore>,
) -> Result<u64, String> {
    if config.object_backend == RangeServingObjectBackend::Local {
        return Ok(0);
    }
    let mut objects = store.list(None);
    let mut locations = Vec::new();
    while let Some(meta) = objects
        .try_next()
        .await
        .map_err(|error| format!("list GCS scratch objects: {error}"))?
    {
        locations.push(meta.location);
    }
    drop(objects);
    let mut deleted = 0_u64;
    for location in locations {
        store
            .delete(&location)
            .await
            .map_err(|error| format!("delete GCS scratch object {location}: {error}"))?;
        deleted = deleted.saturating_add(1);
    }
    let remaining = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| format!("verify GCS scratch cleanup: {error}"))?;
    if !remaining.is_empty() {
        return Err(format!(
            "GCS scratch cleanup left {} live objects",
            remaining.len()
        ));
    }
    Ok(deleted)
}

/// Remove any live objects under a validated GCS range-serving scratch prefix.
///
/// This is the controller fallback when a child process is killed before its
/// normal cleanup path runs.
///
/// # Errors
///
/// Returns an error for invalid scope, missing configuration, or cleanup
/// failure.
pub async fn cleanup_range_serving_curve_gcs_scratch(
    config: &RangeServingCurveConfig,
) -> Result<u64, String> {
    validate_config(config)?;
    if config.object_backend != RangeServingObjectBackend::Gcs {
        return Ok(0);
    }
    let scratch_root = tempfile::Builder::new()
        .prefix("okv-range-serving-cleanup-")
        .tempdir()
        .map_err(|error| format!("create range-serving cleanup root: {error}"))?;
    let (store, _, _) = build_object_store(config, scratch_root.path())?;
    cleanup_scratch(config, store).await
}

async fn prepare_provider_store(
    config: &RangeServingCurveConfig,
    store: Arc<dyn ObjectStore>,
    physical: &MvccGcPhysicalManifestReceipt,
    range_root: &AuthorityRangeRoot,
    provider_kind: ProviderKind,
    provider_namespace: &str,
) -> Result<
    (
        Arc<dyn ObjectStore>,
        Option<Arc<ProviderBoundObjectStore>>,
        String,
        u64,
    ),
    String,
> {
    if !config.provider_mode.enabled() {
        return Ok((store, None, String::new(), 0));
    }
    let mut provider = bind_provider_physical_manifest(
        Arc::clone(&store),
        provider_kind,
        provider_namespace,
        physical,
    )
    .await?;
    match config.provider_mode {
        RangeServingProviderMode::ChangedGeneration => {
            if let Some(version) = &mut provider.manifest.revision.version {
                version.push_str("-changed");
            } else if let Some(e_tag) = &mut provider.manifest.revision.e_tag {
                e_tag.push_str("-changed");
            }
            provider.closure_sha256 = provider_closure_digest(&provider);
        }
        RangeServingProviderMode::MissingRevision => {
            provider.manifest.revision.e_tag = None;
            provider.manifest.revision.version = None;
            provider.closure_sha256 = provider_closure_digest(&provider);
        }
        RangeServingProviderMode::SameBytesNewGeneration => {
            let revision = overwrite_first_live_sst(Arc::clone(&store), &provider, false).await?;
            provider
                .live_ssts
                .first_mut()
                .ok_or_else(|| "provider closure has no live SST to revise".to_owned())?
                .revision = revision;
        }
        RangeServingProviderMode::ChangedBytes => {
            let revision = overwrite_first_live_sst(Arc::clone(&store), &provider, true).await?;
            provider
                .live_ssts
                .first_mut()
                .ok_or_else(|| "provider closure has no live SST to revise".to_owned())?
                .revision = revision;
        }
        RangeServingProviderMode::SkipRevisionEnforcement => {
            let _ = overwrite_first_live_sst(Arc::clone(&store), &provider, false).await?;
        }
        RangeServingProviderMode::Unbound
        | RangeServingProviderMode::Correct
        | RangeServingProviderMode::ChangedNamespace => {}
    }
    let base = PersistentRangeBaseDescriptor {
        format_version: 1,
        database_path: DATABASE_PATH.to_owned(),
        root: range_root.clone(),
        physical: physical.clone(),
    };
    let descriptor = promote_provider_bound_persistent_range_base(&base, provider)?;
    let closure_sha256 = descriptor.root.provider_closure_sha256.clone();
    if config.provider_mode == RangeServingProviderMode::SkipRevisionEnforcement {
        return Ok((store, None, closure_sha256, 1));
    }
    let active_namespace = if config.provider_mode == RangeServingProviderMode::ChangedNamespace {
        format!("changed-{provider_namespace}")
    } else {
        provider_namespace.to_owned()
    };
    let bound = Arc::new(ProviderBoundObjectStore::new(
        store,
        provider_kind,
        &active_namespace,
        &descriptor.provider,
    )?);
    let source: Arc<dyn ObjectStore> = bound.clone();
    Ok((source, Some(bound), closure_sha256, 0))
}

async fn overwrite_first_live_sst(
    store: Arc<dyn ObjectStore>,
    provider: &crate::ProviderBoundPhysicalManifestReceipt,
    change_bytes: bool,
) -> Result<RevisionToken, String> {
    let object = provider
        .live_ssts
        .first()
        .ok_or_else(|| "provider closure has no live SST to overwrite".to_owned())?;
    let path = object_store::path::Path::from(object.key.clone());
    let result = store
        .get(&path)
        .await
        .map_err(|error| format!("read provider control SST: {error}"))?;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| format!("read provider control SST body: {error}"))?;
    let replacement = if change_bytes {
        let mut replacement = bytes.to_vec();
        let first = replacement
            .first_mut()
            .ok_or_else(|| "provider control SST is empty".to_owned())?;
        *first ^= 0xff;
        Bytes::from(replacement)
    } else {
        bytes
    };
    store
        .put(&path, replacement.into())
        .await
        .map_err(|error| format!("overwrite provider control SST: {error}"))?;
    let meta = store
        .head(&path)
        .await
        .map_err(|error| format!("read overwritten provider control identity: {error}"))?;
    let revision = RevisionToken {
        e_tag: meta.e_tag,
        version: meta.version,
    };
    if revision.e_tag.is_none() && revision.version.is_none() {
        return Err("overwritten provider control SST has no revision".to_owned());
    }
    Ok(revision)
}

async fn build_nvme_store(
    nvme_root: &Path,
    store: Arc<dyn ObjectStore>,
    config: &RangeServingCurveConfig,
) -> Result<Arc<dyn ObjectStore>, String> {
    CachedObjectStore::builder(nvme_root, store)
        .with_max_cache_size_bytes(Some(config.nvme_cache_bytes))
        .with_part_size_bytes(config.nvme_part_bytes)
        .with_cache_on_flush(false)
        .with_scan_interval(None)
        .with_max_open_file_handles(config.nvme_open_file_handles)
        .build()
        .await
        .map(|store| store as Arc<dyn ObjectStore>)
        .map_err(|error| format!("build range-serving shared NVMe cache: {error}"))
}

fn new_decoded_cache(config: &RangeServingCurveConfig) -> Arc<dyn DbCache> {
    Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: config.decoded_cache_bytes,
        time_to_live: None,
        time_to_idle: None,
    }))
}

async fn build_engine(store: Arc<dyn ObjectStore>, seed: u64) -> Result<SlateEngine, String> {
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(DATABASE_PATH, store)
        .with_settings(settings)
        .with_seed(seed ^ 0x51a7_e000)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| error.to_string())
}

fn envelope(
    sequence: u64,
    previous_log_chain: [u8; 32],
    mutations: &[CellMutation],
) -> Result<CommitEnvelope, String> {
    let mut client_id = [0_u8; 16];
    client_id[8..].copy_from_slice(&sequence.to_be_bytes());
    Ok(CommitEnvelope::from_parts(CommitEnvelopeParts {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        version: Version::from_parts(GENERATION, sequence),
        log_index: sequence,
        client_id,
        request_id: sequence,
        resolver_set_id: [0x33; 16],
        read_conflicts: Vec::new(),
        write_conflicts: Vec::new(),
        canonical_mutations: serde_json::to_vec(mutations).map_err(|error| error.to_string())?,
        required_resolvers: vec![1],
        required_log_tags: vec![LOG_SET_ID],
        previous_log_chain,
    }))
}

fn slate_batch(envelope: &CommitEnvelope) -> Result<CommitBatch, String> {
    let (client_id, request_id) = envelope.client_identity();
    let mutations: Vec<CellMutation> = serde_json::from_slice(envelope.canonical_mutations())
        .map_err(|error| error.to_string())?;
    let mutations = mutations
        .into_iter()
        .map(|mutation| match mutation {
            CellMutation::Clear { key } => Mutation::Clear { key },
            CellMutation::Set { key, value } => Mutation::Set { key, value },
        })
        .collect();
    Ok(CommitBatch {
        version: Version::new(envelope.version().sequence()),
        identity: CommitIdentity::new(client_id, request_id),
        mutations,
    })
}

fn log_policy() -> Result<(CellLogSetPolicy, BTreeMap<u64, Vec<u8>>), String> {
    let seeds = BTreeMap::from([
        (101, vec![0x11; 32]),
        (102, vec![0x22; 32]),
        (103, vec![0x33; 32]),
    ]);
    let members = seeds
        .iter()
        .map(|(node_id, seed)| {
            tagged_log_public_key(seed)
                .map(|public_key| CellLogSetMember {
                    node_id: *node_id,
                    public_key,
                })
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        CellLogSetPolicy {
            format_version: 1,
            generation: GENERATION,
            policy_epoch: 1,
            log_set_id: LOG_SET_ID,
            quorum_size: 2,
            ratekeeper_soft_limit_bytes: u64::MAX,
            members,
        },
        seeds,
    ))
}

fn certified_record(
    envelope: &CommitEnvelope,
    policy: &CellLogSetPolicy,
    seeds: &BTreeMap<u64, Vec<u8>>,
) -> Result<CertifiedTxLogRecord, String> {
    let encoded = envelope.encode();
    let (encoded_client_id, request_id) = envelope.client_identity();
    let mut client_id = [0_u8; 8];
    client_id.copy_from_slice(&encoded_client_id[8..]);
    let statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        transaction_identity: RequestIdentity {
            client_id: u64::from_be_bytes(client_id),
            request_id,
        },
        commit_sequence: envelope.version().sequence(),
        log_set_id: LOG_SET_ID,
        policy_epoch: policy.policy_epoch,
        envelope_sha256: Sha256::digest(&encoded).into(),
        durable_position: envelope.version().sequence(),
    };
    let attestations = seeds
        .iter()
        .take(2)
        .map(|(node_id, seed)| {
            sign_tagged_log_statement(*node_id, seed, &statement).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CertifiedTxLogRecord {
        envelope: encoded,
        certificates: vec![CellTaggedLogCertificate {
            statement,
            attestations,
        }],
    })
}

fn key_for(ordinal: usize) -> Vec<u8> {
    format!("k/{ordinal:016x}").into_bytes()
}

fn value_for(config: &RangeServingCurveConfig, ordinal: usize, sequence: u64) -> Vec<u8> {
    let mut value = vec![0_u8; config.value_bytes];
    value[..8].copy_from_slice(&u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    value[8..16].copy_from_slice(&sequence.to_be_bytes());
    for (index, byte) in value[16..].iter_mut().enumerate() {
        *byte = config
            .seed
            .wrapping_add(u64::try_from(ordinal).unwrap_or(u64::MAX))
            .wrapping_add(u64::try_from(index).unwrap_or(u64::MAX))
            .to_le_bytes()[0];
    }
    value
}

fn tail_ordinal(seed: u64, tail_index: usize, key_count: usize) -> usize {
    let seed = usize::try_from(seed).unwrap_or(usize::MAX);
    seed.wrapping_add(tail_index.wrapping_mul(7_919)) % key_count
}

fn point_workload_ordinals(config: &RangeServingCurveConfig) -> (Vec<usize>, Vec<usize>) {
    let warmup_count = if config.warmup_reads == 0 {
        config.point_samples
    } else {
        config.warmup_reads
    };
    let measured_count = if config.measured_reads == 0 {
        config.point_samples
    } else {
        config.measured_reads
    };
    let warmup = (0..warmup_count)
        .map(|sample| tail_ordinal(config.seed ^ 0x91, sample, config.base_key_count))
        .collect::<Vec<_>>();
    let measured = if config.measured_reads == 0 {
        warmup.clone()
    } else {
        (0..measured_count)
            .map(|sample| warmup[sample % warmup.len()])
            .collect()
    };
    (warmup, measured)
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    let index = sorted.len().saturating_sub(1).saturating_mul(percentile) / 100;
    sorted[index]
}

fn resident_memory_bytes() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, sysinfo::Process::memory)
}

fn semantic_receipt(
    config: &RangeServingCurveConfig,
    base_frontier: u64,
    target_version: u64,
    first_exact: bool,
    warm_exact: bool,
    scan_exact: bool,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-range-serving-curve-v1");
    hasher.update(config.base_key_count.to_be_bytes());
    hasher.update(config.value_bytes.to_be_bytes());
    hasher.update(config.tail_records.to_be_bytes());
    hasher.update(config.point_samples.to_be_bytes());
    hasher.update(config.warmup_reads.to_be_bytes());
    hasher.update(config.measured_reads.to_be_bytes());
    hasher.update(config.scan_rows.to_be_bytes());
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.cache_mode.id().as_bytes());
    hasher.update(config.provider_mode.id().as_bytes());
    hasher.update(base_frontier.to_be_bytes());
    hasher.update(target_version.to_be_bytes());
    hasher.update([
        u8::from(first_exact),
        u8::from(warm_exact),
        u8::from(scan_exact),
    ]);
    format!("{:x}", hasher.finalize())
}
