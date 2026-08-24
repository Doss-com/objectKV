//! RFC-0059 retained-window physical compaction curve.

use crate::phase0::{CountingStore, IoCounters, Phase0IoDelta};
use crate::{
    AdapterError, AuthorityBoundSlateReader, AuthorityManifestReference, MvccHistoryFilterMode,
    MvccHistoryFilterStatsSnapshot, MvccHistoryFilterSupplier, MvccRetentionFloor, SlateEngine,
    SLATEDB_REVISION,
};
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::Admin;
use slatedb::config::{CompactionWorkerOptions, CompactorOptions, Settings, SstBlockSize};
use slatedb::manifest::VersionedManifest;
use slatedb::{CompactionWorkerBuilder, Db, PathResolver};
use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio_util::sync::CancellationToken;

const DATABASE_PATH: &str = "kv-runtime";
const POINT_SAMPLES: usize = 32;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MvccGcCurveMode {
    Correct,
    IgnoreLeaseFloor,
    DropFloorAnchor,
    DropTombstoneAnchor,
    ReloadFloorDuringJob,
    ClaimCollectionWithoutPublication,
}

impl MvccGcCurveMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::IgnoreLeaseFloor => "ignore_lease_floor",
            Self::DropFloorAnchor => "drop_floor_anchor",
            Self::DropTombstoneAnchor => "drop_tombstone_anchor",
            Self::ReloadFloorDuringJob => "reload_floor_during_job",
            Self::ClaimCollectionWithoutPublication => "claim_collection_without_publication",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcCurveConfig {
    pub history_depth: u64,
    pub retained_versions: u64,
    pub flush_stride: u64,
    pub key_count: usize,
    pub value_bytes: usize,
    pub seed: u64,
    pub timeout_millis: u64,
    pub max_rss_bytes: u64,
}

/// Exact identity of one physical object read back after compaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcPhysicalObjectReceipt {
    pub key: String,
    pub length: u64,
    pub sha256: String,
}

impl MvccGcPhysicalObjectReceipt {
    fn is_valid(&self) -> bool {
        !self.key.is_empty()
            && self.length > 0
            && self.sha256.len() == 64
            && self
                .sha256
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    }
}

/// Exact manifest object and live SST closure for one `SlateDB` manifest ID.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcPhysicalManifestReceipt {
    pub manifest_id: u64,
    pub manifest: MvccGcPhysicalObjectReceipt,
    pub live_ssts: Vec<MvccGcPhysicalObjectReceipt>,
    pub closure_sha256: String,
}

impl MvccGcPhysicalManifestReceipt {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        if self.manifest_id == 0 || !self.manifest.is_valid() || self.live_ssts.is_empty() {
            return false;
        }
        let unique = self
            .live_ssts
            .iter()
            .map(|object| object.key.as_str())
            .collect::<BTreeSet<_>>();
        unique.len() == self.live_ssts.len()
            && !unique.contains(self.manifest.key.as_str())
            && self
                .live_ssts
                .iter()
                .all(MvccGcPhysicalObjectReceipt::is_valid)
            && self.closure_sha256
                == physical_closure_digest(self.manifest_id, &self.manifest, &self.live_ssts)
    }
}

/// Physical input known before a collection worker starts compacting.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcCollectionRequest {
    pub frozen_floor: u64,
    pub input_manifest: MvccGcPhysicalManifestReceipt,
}

/// Authority fields that must be issued between input discovery and compaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcCollectionAuthorization {
    pub job_id: String,
    pub owner_generation: u64,
    pub authority_term: u64,
    pub authority_index: u64,
    pub frozen_floor: u64,
    pub input_manifest: MvccGcPhysicalObjectReceipt,
    pub destination_root: String,
    pub range_map_epoch: u64,
    pub expected_collected_through: u64,
    pub output_namespace: String,
}

impl MvccGcCollectionAuthorization {
    fn matches_request(&self, request: &MvccGcCollectionRequest) -> bool {
        !self.job_id.is_empty()
            && self.owner_generation != 0
            && self.authority_index != 0
            && self.frozen_floor == request.frozen_floor
            && self.input_manifest == request.input_manifest.manifest
            && !self.destination_root.is_empty()
            && self.range_map_epoch != 0
            && self.output_namespace.ends_with('/')
    }
}

/// Physical collector output bound to an authorization obtained before work.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MvccGcAuthorizedCurveReceipt {
    pub authorization: MvccGcCollectionAuthorization,
    pub physical: MvccGcCurveReceipt,
    pub binding_sha256: String,
}

impl MvccGcAuthorizedCurveReceipt {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        let request = MvccGcCollectionRequest {
            frozen_floor: self.physical.floor_version,
            input_manifest: self.physical.input_physical_manifest.clone(),
        };
        self.authorization.matches_request(&request)
            && self.physical.anomaly_count() == 0
            && output_namespace_contains(&self.authorization, &self.physical)
            && self.binding_sha256 == authorized_binding_digest(&self.authorization, &self.physical)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MvccGcCurveReceipt {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub physical_profile: String,
    pub mode: String,
    pub seed: u64,
    pub history_depth: u64,
    pub retained_versions: u64,
    pub flush_stride: u64,
    pub floor_version: u64,
    pub filter_floor_version: u64,
    pub claimed_collected_through: u64,
    pub input_physical_manifest: MvccGcPhysicalManifestReceipt,
    pub output_physical_manifest: MvccGcPhysicalManifestReceipt,
    pub authority_bound_input_reads_exact: bool,
    pub authority_bound_output_reads_exact: bool,
    pub key_count: usize,
    pub value_bytes: usize,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub publication_completed: bool,
    pub floor_advanced_mid_job: bool,
    pub pre_compaction_live_sst_bytes: u64,
    pub post_compaction_live_sst_bytes: u64,
    pub live_byte_reduction_fraction: f64,
    pub post_compaction_bytes_per_retained_logical_byte: f64,
    pub filter_stats: MvccHistoryFilterStatsSnapshot,
    pub compaction_seconds: f64,
    pub compaction_io: Phase0IoDelta,
    pub cold_point_p50_seconds: f64,
    pub cold_point_p99_seconds: f64,
    pub cold_point_io: Phase0IoDelta,
    pub cold_scan_seconds: f64,
    pub cold_scan_rows: usize,
    pub cold_scan_io: Phase0IoDelta,
    pub floor_point_exact: bool,
    pub floor_scan_exact: bool,
    pub latest_point_exact: bool,
    pub latest_scan_exact: bool,
    pub tombstone_anchor_exact: bool,
    pub expired_snapshot_refused: bool,
    pub future_snapshot_refused: bool,
    pub close_reopen_exact: bool,
    pub safety_bounds_held: bool,
    pub peak_rss_bytes: u64,
    pub total_elapsed_seconds: f64,
    pub semantic_receipt_sha256: String,
}

impl MvccGcCurveReceipt {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        [
            !self.publication_completed,
            self.claimed_collected_through > 0 && !self.publication_completed,
            !self.input_physical_manifest.is_valid(),
            !self.output_physical_manifest.is_valid(),
            !self.authority_bound_input_reads_exact,
            !self.authority_bound_output_reads_exact,
            self.publication_completed
                && self.input_physical_manifest.manifest == self.output_physical_manifest.manifest,
            self.floor_advanced_mid_job,
            self.filter_floor_version != self.floor_version,
            self.final_l0_ssts != 0,
            self.filter_stats.dropped_older_entries == 0,
            self.post_compaction_live_sst_bytes >= self.pre_compaction_live_sst_bytes,
            !self.floor_point_exact,
            !self.floor_scan_exact,
            !self.latest_point_exact,
            !self.latest_scan_exact,
            !self.tombstone_anchor_exact,
            !self.expired_snapshot_refused,
            !self.future_snapshot_refused,
            !self.close_reopen_exact,
            !self.safety_bounds_held,
        ]
        .into_iter()
        .map(u64::from)
        .sum()
    }
}

/// Execute one retained-window physical compaction point in the current child
/// process.
///
/// # Errors
///
/// Returns an error for invalid inputs, object-store failures, `SlateDB`
/// lifecycle failures, or a compaction that does not publish before timeout.
#[allow(clippy::too_many_lines)]
pub async fn run_mvcc_gc_curve_worker(
    config: &MvccGcCurveConfig,
    mode: MvccGcCurveMode,
) -> Result<MvccGcCurveReceipt, String> {
    let (physical, authorization) =
        run_mvcc_gc_curve_worker_internal(config, mode, None, |_| async { Ok(None) }).await?;
    if authorization.is_some() {
        return Err("unrequested MVCC GC authorization was returned".to_owned());
    }
    Ok(physical)
}

/// Execute physical collection only after an asynchronous authority callback
/// binds the discovered input manifest and frozen floor.
///
/// # Errors
///
/// Returns an error when input preparation, authorization, compaction, physical
/// receipt construction, or exact authorization binding fails.
pub async fn run_authorized_mvcc_gc_curve_worker<A, F>(
    config: &MvccGcCurveConfig,
    mode: MvccGcCurveMode,
    authorize: A,
) -> Result<MvccGcAuthorizedCurveReceipt, String>
where
    A: FnOnce(MvccGcCollectionRequest) -> F,
    F: Future<Output = Result<MvccGcCollectionAuthorization, String>>,
{
    let (physical, authorization) =
        run_mvcc_gc_curve_worker_internal(config, mode, None, |request| async move {
            authorize(request).await.map(Some)
        })
        .await?;
    build_authorized_receipt(physical, authorization)
}

/// Execute authorized physical collection in a caller-owned local object root.
///
/// The caller owns the root lifetime, so a controller can verify and publish
/// the physical closure after the collector process exits.
///
/// # Errors
///
/// Returns an error when the root cannot be opened, input preparation,
/// authorization, compaction, physical receipt construction, or exact binding
/// fails.
pub async fn run_authorized_mvcc_gc_curve_worker_at_root<A, F>(
    config: &MvccGcCurveConfig,
    mode: MvccGcCurveMode,
    object_root: &Path,
    authorize: A,
) -> Result<MvccGcAuthorizedCurveReceipt, String>
where
    A: FnOnce(MvccGcCollectionRequest) -> F,
    F: Future<Output = Result<MvccGcCollectionAuthorization, String>>,
{
    let (physical, authorization) =
        run_mvcc_gc_curve_worker_internal(config, mode, Some(object_root), |request| async move {
            authorize(request).await.map(Some)
        })
        .await?;
    build_authorized_receipt(physical, authorization)
}

fn build_authorized_receipt(
    physical: MvccGcCurveReceipt,
    authorization: Option<MvccGcCollectionAuthorization>,
) -> Result<MvccGcAuthorizedCurveReceipt, String> {
    let authorization = authorization
        .ok_or_else(|| "authorized MVCC GC worker returned no authorization".to_owned())?;
    let receipt = MvccGcAuthorizedCurveReceipt {
        binding_sha256: authorized_binding_digest(&authorization, &physical),
        authorization,
        physical,
    };
    if !receipt.is_valid() {
        return Err("authorized MVCC GC receipt failed exact binding".to_owned());
    }
    Ok(receipt)
}

#[allow(clippy::too_many_lines)]
async fn run_mvcc_gc_curve_worker_internal<A, F>(
    config: &MvccGcCurveConfig,
    mode: MvccGcCurveMode,
    object_root: Option<&Path>,
    authorize: A,
) -> Result<(MvccGcCurveReceipt, Option<MvccGcCollectionAuthorization>), String>
where
    A: FnOnce(MvccGcCollectionRequest) -> F,
    F: Future<Output = Result<Option<MvccGcCollectionAuthorization>, String>>,
{
    validate(config)?;
    let started = Instant::now();
    let floor_version = config
        .history_depth
        .saturating_sub(config.retained_versions)
        .saturating_add(1);
    let floor = Version::new(floor_version);
    let temporary_root = if object_root.is_none() {
        Some(
            tempfile::Builder::new()
                .prefix("okv-mvcc-gc-curve-")
                .tempdir()
                .map_err(|error| format!("create MVCC GC root: {error}"))?,
        )
    } else {
        None
    };
    let root_path = object_root.map_or_else(
        || {
            temporary_root
                .as_ref()
                .map_or_else(PathBuf::new, |root| root.path().to_path_buf())
        },
        Path::to_path_buf,
    );
    std::fs::create_dir_all(&root_path)
        .map_err(|error| format!("create MVCC GC object root: {error}"))?;
    let local = LocalFileSystem::new_with_prefix(&root_path)
        .map_err(|error| format!("open MVCC GC object root: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));

    let writer = build_engine(config, Arc::clone(&store), config.seed).await?;
    for sequence in 1..=config.history_depth {
        writer
            .apply(commit_for(config, sequence, floor_version))
            .await
            .map_err(|error| format!("apply MVCC GC version {sequence}: {error}"))?;
        if sequence % config.flush_stride == 0 || sequence == config.history_depth {
            writer
                .flush()
                .await
                .map_err(|error| format!("flush MVCC GC version {sequence}: {error}"))?;
        }
    }
    writer
        .close()
        .await
        .map_err(|error| format!("close MVCC GC writer: {error}"))?;

    let filter_floor_version = if mode == MvccGcCurveMode::IgnoreLeaseFloor {
        floor_version.saturating_add(1).min(config.history_depth)
    } else {
        floor_version
    };
    let filter_floor = Arc::new(
        MvccRetentionFloor::new(Version::new(filter_floor_version))
            .map_err(|error| format!("create MVCC retention floor: {error}"))?,
    );
    let filter_mode = match mode {
        MvccGcCurveMode::DropFloorAnchor => MvccHistoryFilterMode::DropFloorAnchor,
        MvccGcCurveMode::DropTombstoneAnchor => MvccHistoryFilterMode::DropTombstoneAnchor,
        MvccGcCurveMode::ReloadFloorDuringJob => MvccHistoryFilterMode::ReloadFloorDuringJob,
        MvccGcCurveMode::Correct
        | MvccGcCurveMode::IgnoreLeaseFloor
        | MvccGcCurveMode::ClaimCollectionWithoutPublication => MvccHistoryFilterMode::Correct,
    };
    let supplier = MvccHistoryFilterSupplier::with_mode(Arc::clone(&filter_floor), filter_mode);
    let observer = Admin::builder(DATABASE_PATH, Arc::clone(&store))
        .with_seed(config.seed ^ 0x0b5e_7a11)
        .with_compaction_filter_supplier(Arc::new(supplier.clone()))
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read initial MVCC GC manifest: {error}"))?
        .ok_or_else(|| "missing initial MVCC GC manifest".to_owned())?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;
    let pre_compaction_live_sst_bytes = live_sst_bytes(&initial_manifest);
    let input_physical_manifest =
        physical_manifest_receipt(Arc::clone(&store), DATABASE_PATH, &initial_manifest).await?;
    let collection_request = MvccGcCollectionRequest {
        frozen_floor: floor_version,
        input_manifest: input_physical_manifest.clone(),
    };
    let authorization = authorize(collection_request.clone()).await?;
    if authorization
        .as_ref()
        .is_some_and(|issued| !issued.matches_request(&collection_request))
    {
        return Err("MVCC GC authorization does not match prepared input".to_owned());
    }

    let worker = CompactionWorkerBuilder::new(DATABASE_PATH, Arc::clone(&store))
        .with_seed(config.seed ^ 0xa11c_e55e)
        .with_options(CompactionWorkerOptions {
            max_concurrent_compactions: 1,
            compactions_poll_interval: Duration::from_millis(20),
            heartbeat_interval: Duration::from_millis(40),
            max_subcompactions: 1,
            min_filter_keys: 1,
            ..CompactionWorkerOptions::default()
        })
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .with_compaction_filter_supplier(Arc::new(supplier.clone()))
        .build()
        .await
        .map_err(|error| format!("build MVCC GC worker: {error}"))?;
    let coordinator = Admin::builder(DATABASE_PATH, Arc::clone(&store))
        .with_seed(config.seed ^ 0xc001_d1a7)
        .with_compaction_filter_supplier(Arc::new(supplier.clone()))
        .build();
    let compaction_before = counters.total();
    let compaction_started = Instant::now();
    let mut floor_advanced_mid_job = false;
    let (final_manifest, publication_completed) =
        if mode == MvccGcCurveMode::ClaimCollectionWithoutPublication {
            (initial_manifest.clone(), false)
        } else {
            let coordinator_cancel = CancellationToken::new();
            let coordinator_cancel_task = coordinator_cancel.clone();
            let coordinator_task = tokio::spawn(async move {
                coordinator
                    .run_compactor_with_options(
                        coordinator_cancel_task,
                        CompactorOptions {
                            worker: None,
                            max_concurrent_compactions: 1,
                            poll_interval: Duration::from_millis(20),
                            commit_compacted_interval: Duration::from_millis(20),
                            worker_heartbeat_timeout: Duration::from_secs(2),
                            ..CompactorOptions::default()
                        },
                    )
                    .await
            });
            let worker_cancel = CancellationToken::new();
            let worker_cancel_task = worker_cancel.clone();
            let worker_task = tokio::spawn(async move {
                tokio::select! {
                    result = worker.run() => result,
                    () = worker_cancel_task.cancelled() => worker.stop().await,
                }
            });
            let timeout = Duration::from_millis(config.timeout_millis);
            let manifest = loop {
                if compaction_started.elapsed() >= timeout {
                    coordinator_cancel.cancel();
                    worker_cancel.cancel();
                    let _ = coordinator_task.await;
                    let _ = worker_task.await;
                    return Err(format!(
                        "MVCC GC compaction exceeded {} ms",
                        config.timeout_millis
                    ));
                }
                if mode == MvccGcCurveMode::ReloadFloorDuringJob
                    && !floor_advanced_mid_job
                    && supplier.stats().inspected_user_entries >= config.history_depth
                {
                    let advanced = filter_floor_version
                        .saturating_add(config.retained_versions.max(1))
                        .min(config.history_depth);
                    floor_advanced_mid_job = filter_floor
                        .advance(Version::new(advanced))
                        .map_err(|error| format!("advance live filter floor: {error}"))?;
                }
                let manifest = observer
                    .read_manifest(None)
                    .await
                    .map_err(|error| format!("poll MVCC GC manifest: {error}"))?
                    .ok_or_else(|| "missing polled MVCC GC manifest".to_owned())?;
                if manifest.l0().is_empty()
                    && !manifest.compacted().is_empty()
                    && supplier.stats().dropped_older_entries > 0
                {
                    break manifest;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            };
            coordinator_cancel.cancel();
            worker_cancel.cancel();
            let coordinator_result = coordinator_task
                .await
                .map_err(|error| format!("join MVCC GC coordinator: {error}"))?;
            coordinator_result.map_err(|error| format!("stop MVCC GC coordinator: {error}"))?;
            let worker_result = worker_task
                .await
                .map_err(|error| format!("join MVCC GC worker: {error}"))?;
            worker_result.map_err(|error| format!("stop MVCC GC worker: {error}"))?;
            (manifest, true)
        };
    let compaction_seconds = if publication_completed {
        compaction_started.elapsed().as_secs_f64()
    } else {
        0.0
    };
    let compaction_io = counters.total().difference_since(&compaction_before);
    let post_compaction_live_sst_bytes = live_sst_bytes(&final_manifest);
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let output_physical_manifest =
        physical_manifest_receipt(Arc::clone(&store), DATABASE_PATH, &final_manifest).await?;
    let authority_bound_input_reads_exact = verify_authority_bound_view(
        Arc::clone(&store),
        &input_physical_manifest.manifest,
        config,
        floor,
        config.seed ^ 0xb01d_0001,
    )
    .await?;
    let authority_bound_output_reads_exact = verify_authority_bound_view(
        Arc::clone(&store),
        &output_physical_manifest.manifest,
        config,
        floor,
        config.seed ^ 0xb01d_0002,
    )
    .await?;

    let semantic = build_engine(config, Arc::clone(&store), config.seed ^ 0x55aa).await?;
    let floor_point_exact = verify_points(&semantic, config, floor_version, floor).await?;
    let floor_scan_exact = verify_scan(&semantic, config, floor_version, floor).await?;
    let latest_point_exact = verify_points(&semantic, config, config.history_depth, floor).await?;
    let latest_scan_exact = verify_scan(&semantic, config, config.history_depth, floor).await?;
    let tombstone_anchor_exact = semantic
        .get_at_retained(&curve_key(config.key_count - 1), floor, floor)
        .await
        .map_err(|error| format!("read tombstone anchor: {error}"))?
        .is_none();
    let expired_snapshot_refused = floor_version == 0
        || matches!(
            semantic
                .get_at_retained(
                    &curve_key(0),
                    Version::new(floor_version.saturating_sub(1)),
                    floor,
                )
                .await,
            Err(AdapterError::SnapshotExpired { .. })
        );
    let future_snapshot_refused = matches!(
        semantic
            .get_at_retained(
                &curve_key(0),
                Version::new(config.history_depth.saturating_add(1)),
                floor,
            )
            .await,
        Err(AdapterError::SnapshotUnavailable { .. })
    );
    semantic
        .close()
        .await
        .map_err(|error| format!("close semantic MVCC GC reader: {error}"))?;

    let point_reader = build_engine(config, Arc::clone(&store), config.seed ^ 0x91).await?;
    let point_before = counters.total();
    let (point_exact, point_latencies) =
        measure_cold_points(&point_reader, config, floor_version, floor).await?;
    let cold_point_io = counters.total().difference_since(&point_before);
    point_reader
        .close()
        .await
        .map_err(|error| format!("close cold point MVCC GC reader: {error}"))?;
    let (cold_point_p50_seconds, cold_point_p99_seconds) = percentiles(&point_latencies);

    let scan_reader = build_engine(config, Arc::clone(&store), config.seed ^ 0x92).await?;
    let scan_before = counters.total();
    let cold_scan_started = Instant::now();
    let (scan_exact, cold_scan_rows) =
        measured_scan(&scan_reader, config, floor_version, floor).await?;
    let cold_scan_seconds = cold_scan_started.elapsed().as_secs_f64();
    let cold_scan_io = counters.total().difference_since(&scan_before);
    scan_reader
        .close()
        .await
        .map_err(|error| format!("close cold scan MVCC GC reader: {error}"))?;

    let retained_logical_bytes = config
        .key_count
        .saturating_mul(config.value_bytes)
        .saturating_mul(usize::try_from(config.retained_versions).unwrap_or(usize::MAX));
    let post_compaction_bytes_per_retained_logical_byte = if retained_logical_bytes == 0 {
        0.0
    } else {
        ratio(
            post_compaction_live_sst_bytes,
            retained_logical_bytes as u64,
        )
    };
    let live_byte_reduction_fraction = if pre_compaction_live_sst_bytes == 0 {
        0.0
    } else {
        1.0 - ratio(
            post_compaction_live_sst_bytes,
            pre_compaction_live_sst_bytes,
        )
    };
    let peak_rss_bytes = resident_memory_bytes();
    let total_elapsed_seconds = started.elapsed().as_secs_f64();
    let safety_bounds_held = peak_rss_bytes <= config.max_rss_bytes
        && started.elapsed().as_millis() <= u128::from(config.timeout_millis).saturating_mul(3);
    let close_reopen_exact = authority_bound_input_reads_exact
        && authority_bound_output_reads_exact
        && floor_point_exact
        && floor_scan_exact
        && latest_point_exact
        && latest_scan_exact
        && point_exact
        && scan_exact;
    let filter_stats = supplier.stats();
    let semantic_receipt_sha256 = semantic_digest(
        config,
        floor_version,
        post_compaction_live_sst_bytes,
        filter_stats,
        close_reopen_exact,
        tombstone_anchor_exact,
    );

    let receipt = MvccGcCurveReceipt {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        physical_profile: "objectkv-serving-v1+mvcc-gc-v1".to_owned(),
        mode: mode.id().to_owned(),
        seed: config.seed,
        history_depth: config.history_depth,
        retained_versions: config.retained_versions,
        flush_stride: config.flush_stride,
        floor_version,
        filter_floor_version,
        claimed_collected_through: floor_version,
        input_physical_manifest,
        output_physical_manifest,
        authority_bound_input_reads_exact,
        authority_bound_output_reads_exact,
        key_count: config.key_count,
        value_bytes: config.value_bytes,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        publication_completed,
        floor_advanced_mid_job,
        pre_compaction_live_sst_bytes,
        post_compaction_live_sst_bytes,
        live_byte_reduction_fraction,
        post_compaction_bytes_per_retained_logical_byte,
        filter_stats,
        compaction_seconds,
        compaction_io,
        cold_point_p50_seconds,
        cold_point_p99_seconds,
        cold_point_io,
        cold_scan_seconds,
        cold_scan_rows,
        cold_scan_io,
        floor_point_exact,
        floor_scan_exact,
        latest_point_exact,
        latest_scan_exact,
        tombstone_anchor_exact,
        expired_snapshot_refused,
        future_snapshot_refused,
        close_reopen_exact,
        safety_bounds_held,
        peak_rss_bytes,
        total_elapsed_seconds,
        semantic_receipt_sha256,
    };
    if authorization
        .as_ref()
        .is_some_and(|issued| !output_namespace_contains(issued, &receipt))
    {
        return Err("MVCC GC output escaped the authorized namespace".to_owned());
    }
    Ok((receipt, authorization))
}

fn validate(config: &MvccGcCurveConfig) -> Result<(), String> {
    if config.history_depth == 0
        || config.retained_versions == 0
        || config.retained_versions > config.history_depth
    {
        return Err("retained versions must be within nonzero history depth".to_owned());
    }
    if config.flush_stride == 0
        || config.key_count < 2
        || config.value_bytes == 0
        || config.timeout_millis == 0
        || config.max_rss_bytes == 0
    {
        return Err("MVCC GC workload and safety bounds must be positive".to_owned());
    }
    Ok(())
}

fn settings() -> Settings {
    Settings {
        flush_interval: None,
        wal_enabled: false,
        min_filter_keys: 1,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

async fn build_engine(
    _config: &MvccGcCurveConfig,
    store: Arc<dyn ObjectStore>,
    seed: u64,
) -> Result<SlateEngine, String> {
    Db::builder(DATABASE_PATH, store)
        .with_settings(settings())
        .with_seed(seed)
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| format!("open MVCC GC SlateDB: {error}"))
}

fn commit_for(config: &MvccGcCurveConfig, sequence: u64, floor: u64) -> CommitBatch {
    let mut mutations = Vec::with_capacity(config.key_count);
    for ordinal in 0..config.key_count {
        if ordinal == config.key_count - 1 && sequence == floor {
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
    CommitBatch {
        version: Version::new(sequence),
        identity: CommitIdentity::for_test(sequence),
        mutations,
    }
}

fn curve_key(ordinal: usize) -> Vec<u8> {
    format!("gc-key-{ordinal:08x}").into_bytes()
}

fn value_for(config: &MvccGcCurveConfig, ordinal: usize, sequence: u64) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-mvcc-gc-value-v1");
    hasher.update(config.seed.to_be_bytes());
    hasher.update((ordinal as u64).to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    let digest = hasher.finalize();
    (0..config.value_bytes)
        .map(|offset| digest[offset % digest.len()])
        .collect()
}

fn expected_rows(config: &MvccGcCurveConfig, sequence: u64, floor: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..config.key_count)
        .filter(|ordinal| !(*ordinal == config.key_count - 1 && sequence == floor))
        .map(|ordinal| (curve_key(ordinal), value_for(config, ordinal, sequence)))
        .collect()
}

async fn verify_authority_bound_view(
    store: Arc<dyn ObjectStore>,
    manifest: &MvccGcPhysicalObjectReceipt,
    config: &MvccGcCurveConfig,
    floor: Version,
    seed: u64,
) -> Result<bool, String> {
    let reference = AuthorityManifestReference {
        key: manifest.key.clone(),
        length: manifest.length,
        sha256: manifest.sha256.clone(),
    };
    let reader = AuthorityBoundSlateReader::open(DATABASE_PATH, store, &reference, seed)
        .await
        .map_err(|error| format!("open authority-bound MVCC view: {error}"))?;
    let mut exact = reader.bound_manifest() == manifest.key
        && reader
            .latest_version()
            .await
            .map_err(|error| format!("read authority-bound frontier: {error}"))?
            == Version::new(config.history_depth);
    for sequence in [floor.sequence(), config.history_depth] {
        for ordinal in sample_ordinals(config.key_count) {
            let observed = reader
                .get_at_retained(&curve_key(ordinal), Version::new(sequence), floor)
                .await
                .map_err(|error| format!("read authority-bound point: {error}"))?;
            let expected = if ordinal == config.key_count - 1 && sequence == floor.sequence() {
                None
            } else {
                Some(value_for(config, ordinal, sequence))
            };
            exact &= observed == expected;
        }
        let rows = reader
            .scan_at_retained(
                b"gc-key-00000000",
                b"gc-key-ffffffffz",
                Version::new(sequence),
                floor,
                config.key_count,
            )
            .await
            .map_err(|error| format!("scan authority-bound view: {error}"))?;
        exact &= rows == expected_rows(config, sequence, floor.sequence());
    }
    reader
        .close()
        .await
        .map_err(|error| format!("close authority-bound MVCC view: {error}"))?;
    Ok(exact)
}

async fn verify_points(
    engine: &SlateEngine,
    config: &MvccGcCurveConfig,
    sequence: u64,
    floor: Version,
) -> Result<bool, String> {
    for ordinal in sample_ordinals(config.key_count) {
        let observed = engine
            .get_at_retained(&curve_key(ordinal), Version::new(sequence), floor)
            .await
            .map_err(|error| format!("verify MVCC GC point: {error}"))?;
        let expected = if ordinal == config.key_count - 1 && sequence == floor.sequence() {
            None
        } else {
            Some(value_for(config, ordinal, sequence))
        };
        if observed != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn verify_scan(
    engine: &SlateEngine,
    config: &MvccGcCurveConfig,
    sequence: u64,
    floor: Version,
) -> Result<bool, String> {
    let observed = engine
        .scan_at_retained(
            b"gc-key-00000000",
            b"gc-key-ffffffffz",
            Version::new(sequence),
            floor,
            config.key_count,
        )
        .await
        .map_err(|error| format!("verify MVCC GC scan: {error}"))?;
    Ok(observed == expected_rows(config, sequence, floor.sequence()))
}

async fn measure_cold_points(
    engine: &SlateEngine,
    config: &MvccGcCurveConfig,
    sequence: u64,
    floor: Version,
) -> Result<(bool, Vec<f64>), String> {
    let mut exact = true;
    let mut latencies = Vec::with_capacity(POINT_SAMPLES);
    for ordinal in sample_ordinals(config.key_count) {
        let started = Instant::now();
        let observed = engine
            .get_at_retained(&curve_key(ordinal), Version::new(sequence), floor)
            .await
            .map_err(|error| format!("measure MVCC GC point: {error}"))?;
        latencies.push(started.elapsed().as_secs_f64());
        let expected = if ordinal == config.key_count - 1 && sequence == floor.sequence() {
            None
        } else {
            Some(value_for(config, ordinal, sequence))
        };
        exact &= observed == expected;
    }
    Ok((exact, latencies))
}

async fn measured_scan(
    engine: &SlateEngine,
    config: &MvccGcCurveConfig,
    sequence: u64,
    floor: Version,
) -> Result<(bool, usize), String> {
    let rows = engine
        .scan_at_retained(
            b"gc-key-00000000",
            b"gc-key-ffffffffz",
            Version::new(sequence),
            floor,
            config.key_count,
        )
        .await
        .map_err(|error| format!("measure MVCC GC scan: {error}"))?;
    let exact = rows == expected_rows(config, sequence, floor.sequence());
    Ok((exact, rows.len()))
}

fn sample_ordinals(key_count: usize) -> Vec<usize> {
    let sample_count = POINT_SAMPLES.min(key_count);
    let mut ordinals = (0..sample_count)
        .map(|index| index.saturating_mul(key_count) / sample_count)
        .collect::<Vec<_>>();
    if let Some(last) = ordinals.last_mut() {
        *last = key_count - 1;
    }
    ordinals
}

fn live_sst_bytes(manifest: &VersionedManifest) -> u64 {
    let l0 = manifest
        .l0()
        .iter()
        .map(slatedb::manifest::SsTableView::estimate_size)
        .sum::<u64>();
    let compacted = manifest
        .compacted()
        .iter()
        .map(slatedb::manifest::SortedRun::estimate_size)
        .sum::<u64>();
    l0.saturating_add(compacted)
}

async fn physical_manifest_receipt(
    store: Arc<dyn ObjectStore>,
    database_path: &str,
    manifest: &VersionedManifest,
) -> Result<MvccGcPhysicalManifestReceipt, String> {
    let manifest_path = object_store::path::Path::from(format!(
        "{database_path}/manifest/{:020}.manifest",
        manifest.id()
    ));
    let manifest_object = physical_object_receipt(Arc::clone(&store), &manifest_path).await?;
    let resolver = PathResolver::new(database_path, manifest);
    let mut live_paths = BTreeSet::new();
    for view in manifest.l0() {
        live_paths.insert(resolver.sst_path(&view.sst.id));
    }
    for run in manifest.compacted() {
        for view in run.sst_views() {
            live_paths.insert(resolver.sst_path(&view.sst.id));
        }
    }
    let mut live_ssts = Vec::with_capacity(live_paths.len());
    for path in live_paths {
        live_ssts.push(physical_object_receipt(Arc::clone(&store), &path).await?);
    }
    let closure_sha256 = physical_closure_digest(manifest.id(), &manifest_object, &live_ssts);
    let receipt = MvccGcPhysicalManifestReceipt {
        manifest_id: manifest.id(),
        manifest: manifest_object,
        live_ssts,
        closure_sha256,
    };
    if !receipt.is_valid() {
        return Err(format!(
            "invalid physical manifest receipt for SlateDB manifest {}",
            manifest.id()
        ));
    }
    Ok(receipt)
}

/// Inspect the exact physical closure of the latest `SlateDB` manifest.
///
/// # Errors
///
/// Returns an error when the database has no manifest or any manifest or live
/// SST object cannot be read and hashed exactly.
pub async fn inspect_latest_physical_manifest(
    store: Arc<dyn ObjectStore>,
    database_path: &str,
    seed: u64,
) -> Result<MvccGcPhysicalManifestReceipt, String> {
    let admin = Admin::builder(database_path, Arc::clone(&store))
        .with_seed(seed)
        .build();
    let manifest = admin
        .read_manifest(None)
        .await
        .map_err(|error| format!("read latest SlateDB manifest: {error}"))?
        .ok_or_else(|| "SlateDB database has no physical manifest".to_owned())?;
    physical_manifest_receipt(store, database_path, &manifest).await
}

/// Re-read one complete physical manifest closure from a local object root.
///
/// # Errors
///
/// Returns an error when the receipt is malformed, an object is absent, or any
/// object identity differs from the collector receipt.
pub async fn verify_physical_manifest_on_local_root(
    object_root: &Path,
    expected: &MvccGcPhysicalManifestReceipt,
) -> Result<(), String> {
    if !expected.is_valid() {
        return Err("cannot verify a malformed physical manifest receipt".to_owned());
    }
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(object_root)
            .map_err(|error| format!("open physical verification root: {error}"))?,
    );
    for object in std::iter::once(&expected.manifest).chain(expected.live_ssts.iter()) {
        let observed = physical_object_receipt(
            Arc::clone(&store),
            &object_store::path::Path::from(object.key.clone()),
        )
        .await?;
        if observed != *object {
            return Err(format!(
                "physical object {} differs after collector exit",
                object.key
            ));
        }
    }
    Ok(())
}

async fn physical_object_receipt(
    store: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
) -> Result<MvccGcPhysicalObjectReceipt, String> {
    let result = store
        .get(path)
        .await
        .map_err(|error| format!("read physical object {path}: {error}"))?;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| format!("read physical object body {path}: {error}"))?;
    Ok(MvccGcPhysicalObjectReceipt {
        key: path.to_string(),
        length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    })
}

fn physical_closure_digest(
    manifest_id: u64,
    manifest: &MvccGcPhysicalObjectReceipt,
    live_ssts: &[MvccGcPhysicalObjectReceipt],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-mvcc-gc-physical-closure-v1");
    hasher.update(manifest_id.to_be_bytes());
    hash_physical_object(&mut hasher, manifest);
    for object in live_ssts {
        hash_physical_object(&mut hasher, object);
    }
    format!("{:x}", hasher.finalize())
}

fn hash_physical_object(hasher: &mut Sha256, object: &MvccGcPhysicalObjectReceipt) {
    hasher.update((object.key.len() as u64).to_be_bytes());
    hasher.update(object.key.as_bytes());
    hasher.update(object.length.to_be_bytes());
    hasher.update(object.sha256.as_bytes());
}

fn output_namespace_contains(
    authorization: &MvccGcCollectionAuthorization,
    physical: &MvccGcCurveReceipt,
) -> bool {
    physical
        .output_physical_manifest
        .manifest
        .key
        .starts_with(&authorization.output_namespace)
        && physical
            .output_physical_manifest
            .live_ssts
            .iter()
            .all(|object| object.key.starts_with(&authorization.output_namespace))
}

fn authorized_binding_digest(
    authorization: &MvccGcCollectionAuthorization,
    physical: &MvccGcCurveReceipt,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-authorized-mvcc-gc-receipt-v1");
    hash_string(&mut hasher, &authorization.job_id);
    hasher.update(authorization.owner_generation.to_be_bytes());
    hasher.update(authorization.authority_term.to_be_bytes());
    hasher.update(authorization.authority_index.to_be_bytes());
    hasher.update(authorization.frozen_floor.to_be_bytes());
    hash_physical_object(&mut hasher, &authorization.input_manifest);
    hash_string(&mut hasher, &authorization.destination_root);
    hasher.update(authorization.range_map_epoch.to_be_bytes());
    hasher.update(authorization.expected_collected_through.to_be_bytes());
    hash_string(&mut hasher, &authorization.output_namespace);
    hash_string(&mut hasher, &physical.semantic_receipt_sha256);
    hash_string(
        &mut hasher,
        &physical.output_physical_manifest.closure_sha256,
    );
    format!("{:x}", hasher.finalize())
}

fn hash_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn percentiles(samples: &[f64]) -> (f64, f64) {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return (0.0, 0.0);
    }
    let p50 = sorted[(sorted.len() - 1) / 2];
    let p99_index = ((sorted.len() - 1) * 99).div_ceil(100);
    (p50, sorted[p99_index])
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    f64::from(u32::try_from(numerator).unwrap_or(u32::MAX))
        / f64::from(u32::try_from(denominator).unwrap_or(u32::MAX))
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

fn semantic_digest(
    config: &MvccGcCurveConfig,
    floor: u64,
    post_bytes: u64,
    stats: MvccHistoryFilterStatsSnapshot,
    close_reopen_exact: bool,
    tombstone_exact: bool,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-mvcc-gc-receipt-v1");
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.history_depth.to_be_bytes());
    hasher.update(config.retained_versions.to_be_bytes());
    hasher.update(floor.to_be_bytes());
    hasher.update(post_bytes.to_be_bytes());
    hasher.update(stats.inspected_user_entries.to_be_bytes());
    hasher.update(stats.kept_newer_entries.to_be_bytes());
    hasher.update(stats.kept_floor_anchors.to_be_bytes());
    hasher.update(stats.dropped_older_entries.to_be_bytes());
    hasher.update([u8::from(close_reopen_exact), u8::from(tombstone_exact)]);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::{
        run_authorized_mvcc_gc_curve_worker, run_mvcc_gc_curve_worker,
        MvccGcCollectionAuthorization, MvccGcCurveConfig, MvccGcCurveMode,
    };

    #[tokio::test]
    async fn real_compaction_converges_to_the_retained_window() {
        let receipt = run_mvcc_gc_curve_worker(&config(), MvccGcCurveMode::Correct)
            .await
            .expect("run MVCC GC curve");
        assert_eq!(receipt.floor_version, 13);
        assert!(receipt.post_compaction_live_sst_bytes < receipt.pre_compaction_live_sst_bytes);
        assert!(receipt.filter_stats.dropped_older_entries > 0);
        assert!(receipt.floor_point_exact);
        assert!(receipt.floor_scan_exact);
        assert!(receipt.latest_point_exact, "{receipt:#?}");
        assert!(receipt.latest_scan_exact);
        assert!(receipt.tombstone_anchor_exact);
        assert!(receipt.expired_snapshot_refused);
        assert!(receipt.future_snapshot_refused);
        assert!(receipt.close_reopen_exact);
        assert!(receipt.safety_bounds_held);
        assert!(receipt.input_physical_manifest.is_valid());
        assert!(receipt.output_physical_manifest.is_valid());
        assert!(receipt.authority_bound_input_reads_exact);
        assert!(receipt.authority_bound_output_reads_exact);
        assert_ne!(
            receipt.input_physical_manifest.manifest,
            receipt.output_physical_manifest.manifest
        );
        assert_eq!(receipt.anomaly_count(), 0);
    }

    #[tokio::test]
    async fn every_unsafe_collection_subject_is_detected() {
        for mode in [
            MvccGcCurveMode::IgnoreLeaseFloor,
            MvccGcCurveMode::DropFloorAnchor,
            MvccGcCurveMode::DropTombstoneAnchor,
            MvccGcCurveMode::ReloadFloorDuringJob,
            MvccGcCurveMode::ClaimCollectionWithoutPublication,
        ] {
            let receipt = run_mvcc_gc_curve_worker(&config(), mode)
                .await
                .unwrap_or_else(|error| panic!("run {mode:?}: {error}"));
            assert!(
                receipt.anomaly_count() > 0,
                "unsafe mode {mode:?} was not detected: {receipt:#?}"
            );
        }
    }

    #[tokio::test]
    async fn authorization_is_issued_after_input_discovery_and_before_compaction() {
        let receipt = run_authorized_mvcc_gc_curve_worker(
            &config(),
            MvccGcCurveMode::Correct,
            |request| async move {
                Ok(MvccGcCollectionAuthorization {
                    job_id: "j1".to_owned(),
                    owner_generation: 7,
                    authority_term: 3,
                    authority_index: 9,
                    frozen_floor: request.frozen_floor,
                    input_manifest: request.input_manifest.manifest,
                    destination_root: "cell-root".to_owned(),
                    range_map_epoch: 9,
                    expected_collected_through: 0,
                    output_namespace: "kv-runtime/".to_owned(),
                })
            },
        )
        .await
        .expect("run authorized physical collector");
        assert!(receipt.is_valid());
        assert_eq!(
            receipt.authorization.input_manifest,
            receipt.physical.input_physical_manifest.manifest
        );
        assert_eq!(
            receipt.authorization.frozen_floor,
            receipt.physical.filter_floor_version
        );
    }

    fn config() -> MvccGcCurveConfig {
        MvccGcCurveConfig {
            history_depth: 16,
            retained_versions: 4,
            flush_stride: 2,
            key_count: 32,
            value_bytes: 64,
            seed: 1103,
            timeout_millis: 10_000,
            max_rss_bytes: 1_073_741_824,
        }
    }
}
