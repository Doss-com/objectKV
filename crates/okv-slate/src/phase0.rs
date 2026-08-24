use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult, UploadPart,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::Admin;
use slatedb::compactor::{Compaction, CompactionStatus};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings, SstBlockSize,
};
use slatedb::{CompactionWorkerBuilder, Db, DbBuilder, WriteBatch};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

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
    pub physical_profile: Phase0PhysicalProfile,
}

/// Frozen physical configuration selected for one Phase 0 run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0PhysicalProfile {
    /// Pinned `SlateDB` defaults used by RFC-0021 and RFC-0022.
    SlateDbDefaultV1,
    /// Serving-worker configuration tested by the one-time RFC-0024 pass.
    ObjectKvServingV1,
}

impl Phase0PhysicalProfile {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::SlateDbDefaultV1 => "slatedb-default-v1",
            Self::ObjectKvServingV1 => "objectkv-serving-v1",
        }
    }

    fn settings(self) -> Settings {
        let mut settings = Settings::default();
        if self == Self::ObjectKvServingV1 {
            settings.flush_interval = None;
            settings.wal_enabled = false;
            settings.min_filter_keys = 1;
            settings.compactor_options = None;
            settings.garbage_collector_options = None;
        }
        settings
    }

    fn configure<P: Into<Path>>(self, builder: DbBuilder<P>, seed: u64) -> DbBuilder<P> {
        let builder = builder.with_seed(seed).with_settings(self.settings());
        match self {
            Self::SlateDbDefaultV1 => builder,
            Self::ObjectKvServingV1 => builder.with_sst_block_size(SstBlockSize::Block64Kib),
        }
    }

    #[must_use]
    fn receipt(self) -> Phase0PhysicalReceipt {
        let settings = self.settings();
        Phase0PhysicalReceipt {
            id: self.id().to_owned(),
            object_wal_enabled: settings.wal_enabled,
            automatic_flush_interval_millis: settings.flush_interval.map(|interval| {
                u64::try_from(interval.as_millis())
                    .expect("configured flush interval milliseconds fit u64")
            }),
            sst_block_size_bytes: match self {
                Self::SlateDbDefaultV1 => SstBlockSize::Block4Kib.as_bytes() as u64,
                Self::ObjectKvServingV1 => SstBlockSize::Block64Kib.as_bytes() as u64,
            },
            min_filter_keys: settings.min_filter_keys,
            l0_sst_size_bytes: settings.l0_sst_size_bytes as u64,
            embedded_compactor: settings.compactor_options.is_some(),
            embedded_garbage_collector: settings.garbage_collector_options.is_some(),
        }
    }
}

/// Exact storage settings carried in the raw physical receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Phase0PhysicalReceipt {
    pub id: String,
    pub object_wal_enabled: bool,
    pub automatic_flush_interval_millis: Option<u64>,
    pub sst_block_size_bytes: u64,
    pub min_filter_keys: u32,
    pub l0_sst_size_bytes: u64,
    pub embedded_compactor: bool,
    pub embedded_garbage_collector: bool,
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
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
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

    #[must_use]
    pub fn difference_since(&self, earlier: &Self) -> Self {
        Self {
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
    pub initial_open: Phase0PhaseReport,
    pub ingest: Phase0PhaseReport,
    pub post_flush_verify: Phase0PhaseReport,
    pub warm_cache_prime: Phase0PhaseReport,
    pub warm_point: Phase0PhaseReport,
    pub ordered_scan: Phase0PhaseReport,
    pub reopen_first_correct_read_seconds: f64,
    pub close_before_reopen: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub cold_point: Phase0PhaseReport,
    pub final_close: Phase0PhaseReport,
}

/// Full frozen-contract report returned to `okv-eval`.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0Report {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub receipt_digest: String,
    pub repeated_receipt_digest: String,
    pub seeds: Vec<Phase0SeedReport>,
    pub gates: Vec<Phase0Gate>,
}

/// Fixed inputs for the local separate-role compaction falsifier.
#[derive(Clone, Debug)]
pub struct Phase0CompactionConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub flush_count: u64,
    pub seeds: Vec<u64>,
    pub timeout_millis: u64,
}

/// Correct execution or the deliberate missing-worker poison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0CompactionMode {
    Correct,
    SkipExternalWorker,
}

/// Manifest and I/O evidence for one deterministic compaction seed.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CompactionSeedReport {
    pub seed: u64,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub coordinator_embedded_worker: bool,
    pub worker_sst_block_size_bytes: u64,
    pub worker_min_filter_keys: u32,
    pub roles_completed_cleanly: bool,
    pub exact_dataset_after_compaction: bool,
    pub total_io: Phase0IoDelta,
    pub initial_open: Phase0PhaseReport,
    pub ingest_and_flush: Phase0PhaseReport,
    pub close_before_maintenance: Phase0PhaseReport,
    pub maintenance: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub full_verify: Phase0PhaseReport,
    pub final_close: Phase0PhaseReport,
    pub reopen_first_correct_read_seconds: f64,
    pub maintenance_write_amplification: f64,
}

/// Full report for the separate coordinator and compaction-worker contract.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CompactionReport {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub flush_count: u64,
    pub seeds: Vec<Phase0CompactionSeedReport>,
    pub gates: Vec<Phase0Gate>,
}

/// Inputs passed to one real standalone compaction-worker process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Phase0CompactionWorkerProcessConfig {
    pub object_root: String,
    pub db_path: String,
    pub seed: u64,
}

/// Inputs passed to one real standalone compaction-coordinator process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Phase0CompactionCoordinatorProcessConfig {
    pub object_root: String,
    pub db_path: String,
    pub seed: u64,
    pub poll_interval_millis: u64,
    pub commit_interval_millis: u64,
    pub worker_heartbeat_timeout_millis: u64,
}

/// Fixed inputs for the overwrite and worker-process reclaim falsifier.
#[derive(Clone, Debug)]
pub struct Phase0CompactionReclaimConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<u64>,
    pub claim_timeout_millis: u64,
    pub reclaim_timeout_millis: u64,
    pub completion_timeout_millis: u64,
    pub worker_binary: PathBuf,
}

/// Correct worker replacement or the deliberate missing-replacement poison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0CompactionReclaimMode {
    Correct,
    SkipReplacementWorker,
}

/// One real-process worker reclaim receipt.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CompactionReclaimSeedReport {
    pub seed: u64,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub first_worker_id: Option<String>,
    pub replacement_worker_id: Option<String>,
    pub first_worker_claimed_running: bool,
    pub first_worker_killed: bool,
    pub stale_claim_reclaimed: bool,
    pub replacement_completed: bool,
    pub coordinator_completed_cleanly: bool,
    pub exact_latest_overwrite_after_reclaim: bool,
    pub kill_to_completion_seconds: f64,
    pub reopen_first_correct_read_seconds: f64,
    pub ingest: Phase0PhaseReport,
    pub reclaim: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub full_verify: Phase0PhaseReport,
    pub total_io_observed_by_controller: Phase0IoDelta,
}

/// Full overwrite and real worker-process reclaim report.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CompactionReclaimReport {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<Phase0CompactionReclaimSeedReport>,
    pub gates: Vec<Phase0Gate>,
}

/// Fixed inputs for the coordinator death and output adoption falsifier.
#[derive(Clone, Debug)]
pub struct Phase0CoordinatorRecoveryConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<u64>,
    pub compacted_timeout_millis: u64,
    pub completion_timeout_millis: u64,
    pub process_binary: PathBuf,
}

/// Correct coordinator replacement or the deliberate missing replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0CoordinatorRecoveryMode {
    Correct,
    SkipCoordinatorRestart,
}

/// One real-process coordinator recovery receipt.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CoordinatorRecoverySeedReport {
    pub seed: u64,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub first_coordinator_pid: u32,
    pub replacement_coordinator_pid: Option<u32>,
    pub compacted_job_id: Option<String>,
    pub compacted_output_ssts: Vec<String>,
    pub first_coordinator_killed: bool,
    pub compacted_output_persisted_before_kill: bool,
    pub manifest_unchanged_before_restart: bool,
    pub replacement_committed_existing_output: bool,
    pub exact_latest_overwrite_after_recovery: bool,
    pub kill_to_completion_seconds: f64,
    pub reopen_first_correct_read_seconds: f64,
    pub ingest: Phase0PhaseReport,
    pub coordinator_recovery: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub full_verify: Phase0PhaseReport,
    pub total_io_observed_by_controller: Phase0IoDelta,
}

/// Full coordinator death and output adoption report.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CoordinatorRecoveryReport {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<Phase0CoordinatorRecoverySeedReport>,
    pub gates: Vec<Phase0Gate>,
}

impl Phase0CoordinatorRecoveryReport {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
}

/// Fixed inputs for the concurrent compaction-coordinator fencing falsifier.
#[derive(Clone, Debug)]
pub struct Phase0CoordinatorFencingConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<u64>,
    pub fencing_timeout_millis: u64,
    pub completion_timeout_millis: u64,
    pub process_binary: PathBuf,
}

/// Correct epoch fencing or the control that kills the stale coordinator externally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0CoordinatorFencingMode {
    Correct,
    KillStaleCoordinator,
}

/// One real-process concurrent coordinator fencing receipt.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CoordinatorFencingSeedReport {
    pub seed: u64,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub compactor_epoch_before: u64,
    pub first_compactor_epoch: u64,
    pub second_compactor_epoch: u64,
    pub first_coordinator_pid: u32,
    pub second_coordinator_pid: u32,
    pub first_coordinator_self_fenced: bool,
    pub first_coordinator_killed_by_controller: bool,
    pub second_coordinator_active_at_completion: bool,
    pub second_coordinator_completed_compaction: bool,
    pub exact_latest_overwrite_after_fencing: bool,
    pub epoch_advance_to_first_seconds: f64,
    pub epoch_advance_to_second_seconds: f64,
    pub second_epoch_to_first_exit_seconds: f64,
    pub ingest: Phase0PhaseReport,
    pub coordinator_fencing: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub full_verify: Phase0PhaseReport,
    pub total_io_observed_by_controller: Phase0IoDelta,
}

/// Full concurrent coordinator fencing report.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0CoordinatorFencingReport {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<Phase0CoordinatorFencingSeedReport>,
    pub gates: Vec<Phase0Gate>,
}

impl Phase0CoordinatorFencingReport {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
}

/// Fixed inputs for active-output preservation and true-orphan collection.
#[derive(Clone, Debug)]
pub struct Phase0OrphanGcConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<u64>,
    pub compacted_timeout_millis: u64,
    pub completion_timeout_millis: u64,
    pub process_binary: PathBuf,
}

/// Correct deletion or the dry-run control that leaves a true orphan present.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0OrphanGcMode {
    Correct,
    DryRunOrphanDeletion,
}

/// One real-object active-output and true-orphan GC receipt.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct Phase0OrphanGcSeedReport {
    pub seed: u64,
    pub initial_l0_ssts: u64,
    pub final_l0_ssts: u64,
    pub final_sorted_runs: u64,
    pub active_output_paths: Vec<String>,
    pub active_compacted_output_persisted: bool,
    pub active_output_survived_gc: bool,
    pub active_output_committed_after_gc: bool,
    pub orphan_path: String,
    pub orphan_created: bool,
    pub orphan_deleted: bool,
    pub exact_latest_overwrite_after_gc: bool,
    pub active_gc_seconds: f64,
    pub orphan_gc_seconds: f64,
    pub ingest: Phase0PhaseReport,
    pub garbage_collection: Phase0PhaseReport,
    pub reopen_open: Phase0PhaseReport,
    pub first_correct_read: Phase0PhaseReport,
    pub full_verify: Phase0PhaseReport,
    pub total_io_observed_by_controller: Phase0IoDelta,
}

/// Full active-output and true-orphan GC report.
#[derive(Clone, Debug, Serialize)]
pub struct Phase0OrphanGcReport {
    pub contract_version: u32,
    pub slatedb_revision: String,
    pub store: String,
    pub mode: String,
    pub physical: Phase0PhysicalReceipt,
    pub logical_bytes: u64,
    pub key_count: u64,
    pub overwrite_rounds: u64,
    pub seeds: Vec<Phase0OrphanGcSeedReport>,
    pub gates: Vec<Phase0Gate>,
}

impl Phase0OrphanGcReport {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
}

impl Phase0CompactionReclaimReport {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
}

impl Phase0CompactionReport {
    #[must_use]
    pub fn anomaly_count(&self) -> u64 {
        self.gates.iter().filter(|gate| !gate.passed).count() as u64
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.gates.iter().all(|gate| gate.passed)
    }
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
    let mut gates = vec![
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

    gates.extend(physical_gates(config.physical_profile, mode, &reports));

    Ok(Phase0Report {
        contract_version: 3,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0Mode::Correct => "correct",
            Phase0Mode::ReuseWarmDbForReopen => "reuse_warm_db_for_reopen",
        }
        .to_owned(),
        physical: config.physical_profile.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        receipt_digest,
        repeated_receipt_digest,
        seeds: reports,
        gates,
    })
}

/// Execute a local separate-role compaction contract against immutable SST objects.
///
/// The serving writer has no embedded compactor. A coordinator with no embedded
/// worker schedules compaction, while a separately built worker rewrites SSTs
/// with the same block and filter geometry as the serving writer.
///
/// # Errors
///
/// Returns an error when the local object store, `SlateDB` roles, or exact-read
/// checks cannot execute. Expected contract violations are returned as gates.
#[allow(clippy::too_many_lines)]
pub async fn run_phase0_compaction_contract(
    config: &Phase0CompactionConfig,
    mode: Phase0CompactionMode,
) -> Result<Phase0CompactionReport, String> {
    validate_compaction_config(config)?;
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        reports.push(run_compaction_seed(config, *seed, mode).await?);
    }

    Ok(compaction_report(config, mode, STORE_KIND, reports))
}

/// Execute the separate-role compaction contract through a real S3-compatible
/// object store. Each workload receives a unique object prefix so the proof
/// never depends on deleting or reusing remote state.
///
/// # Errors
///
/// Returns an error when required `OKV_S3_*` configuration is absent, the
/// S3-compatible store cannot be built, or the shared compaction contract fails
/// to execute. Logical and physical violations are returned as failed gates.
pub async fn run_phase0_minio_compaction_contract(
    config: &Phase0CompactionConfig,
    mode: Phase0CompactionMode,
    namespace: &str,
) -> Result<Phase0CompactionReport, String> {
    validate_compaction_config(config)?;
    let namespace = sanitize_object_namespace(namespace)?;
    let endpoint = required_env("OKV_S3_ENDPOINT")?;
    let bucket = required_env("OKV_S3_BUCKET")?;
    let access_key = required_env("OKV_S3_ACCESS_KEY_ID")?;
    let secret_key = required_env("OKV_S3_SECRET_ACCESS_KEY")?;
    let region = std::env::var("OKV_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        let remote = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_endpoint(&endpoint)
            .with_access_key_id(&access_key)
            .with_secret_access_key(&secret_key)
            .with_region(&region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map_err(|error| format!("build S3-compatible compaction store: {error}"))?;
        let counters = Arc::new(IoCounters::default());
        let store: Arc<dyn ObjectStore> =
            Arc::new(CountingStore::new(remote, Arc::clone(&counters)));
        let db_path = format!("phase0-minio/{namespace}/seed-{seed:016x}");
        reports.push(
            run_compaction_seed_on_store(config, *seed, mode, store, counters, db_path).await?,
        );
    }

    Ok(compaction_report(config, mode, "minio-s3", reports))
}

#[allow(clippy::too_many_lines)]
fn compaction_report(
    config: &Phase0CompactionConfig,
    mode: Phase0CompactionMode,
    store_kind: &str,
    reports: Vec<Phase0CompactionSeedReport>,
) -> Phase0CompactionReport {
    let initial_l0_materialized = reports
        .iter()
        .all(|report| report.initial_l0_ssts >= config.flush_count);
    let separate_roles_completed = mode == Phase0CompactionMode::Correct
        && reports
            .iter()
            .all(|report| !report.coordinator_embedded_worker && report.roles_completed_cleanly);
    let l0_reduced = reports
        .iter()
        .all(|report| report.final_l0_ssts < report.initial_l0_ssts);
    let sorted_run_created = reports.iter().all(|report| report.final_sorted_runs > 0);
    let exact_dataset_after_compaction = reports
        .iter()
        .all(|report| report.exact_dataset_after_compaction);
    let maintenance_io_accounted = reports.iter().all(|report| {
        report.maintenance.io.read_byte_total() > 0
            && report.maintenance.io.written_byte_total() > 0
    });
    let serving_reads_do_not_write = reports.iter().all(|report| {
        report.first_correct_read.io.written_byte_total()
            + report.full_verify.io.written_byte_total()
            == 0
    });
    let max_open_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let gates = vec![
        gate(
            "initial_l0_flushes_materialized",
            initial_l0_materialized,
            &format!(
                "every seed exposed at least {} L0 SSTs before maintenance",
                config.flush_count
            ),
        ),
        gate(
            "separate_compaction_roles_completed",
            separate_roles_completed,
            "a coordinator with no embedded worker and a separately built worker stopped cleanly",
        ),
        gate(
            "external_compaction_reduced_l0",
            l0_reduced,
            "standalone maintenance reduced the visible L0 SST count",
        ),
        gate(
            "external_compaction_created_sorted_run",
            sorted_run_created,
            "standalone maintenance committed at least one compacted sorted run",
        ),
        gate(
            "exact_dataset_after_external_compaction",
            exact_dataset_after_compaction,
            "a fresh serving instance scanned every key and value exactly after maintenance",
        ),
        gate(
            "maintenance_object_io_accounted",
            maintenance_io_accounted,
            "standalone maintenance produced separately measured object reads and writes",
        ),
        gate(
            "serving_reads_do_not_write",
            serving_reads_do_not_write,
            "first point read and full verification emitted no object writes; writable-handle open metadata remains separately measured",
        ),
        gate(
            "metadata_bounded_reopen_after_compaction",
            max_open_bytes <= 1_048_576,
            &format!("fresh-instance open read {max_open_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget_after_compaction",
            max_first_point_requests <= 8,
            &format!(
                "first correct point read used {max_first_point_requests} requests; ceiling is 8"
            ),
        ),
        gate(
            "first_cold_point_byte_budget_after_compaction",
            max_first_point_bytes <= 524_288,
            &format!(
                "first correct point read fetched {max_first_point_bytes} bytes; ceiling is 524288"
            ),
        ),
    ];

    Phase0CompactionReport {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: store_kind.to_owned(),
        mode: match mode {
            Phase0CompactionMode::Correct => "correct",
            Phase0CompactionMode::SkipExternalWorker => "skip_external_worker",
        }
        .to_owned(),
        physical: Phase0PhysicalProfile::ObjectKvServingV1.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        flush_count: config.flush_count,
        seeds: reports,
        gates,
    }
}

/// Run one standalone compaction worker until its process is terminated.
///
/// # Errors
///
/// Returns an error when the child cannot open the shared filesystem object
/// root, build the format-compatible worker, or run its event loop.
pub async fn run_phase0_compaction_worker_process_node(
    config: Phase0CompactionWorkerProcessConfig,
) -> Result<(), String> {
    let local = LocalFileSystem::new_with_prefix(&config.object_root)
        .map_err(|error| format!("open worker object root: {error}"))?;
    let store: Arc<dyn ObjectStore> = Arc::new(local);
    let worker_options = CompactionWorkerOptions {
        max_concurrent_compactions: 1,
        compactions_poll_interval: Duration::from_millis(25),
        heartbeat_interval: Duration::from_millis(50),
        max_subcompactions: 1,
        min_filter_keys: 1,
        ..CompactionWorkerOptions::default()
    };
    let worker = CompactionWorkerBuilder::new(config.db_path.as_str(), store)
        .with_seed(config.seed)
        .with_options(worker_options)
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .build()
        .await
        .map_err(|error| format!("build process compaction worker: {error}"))?;
    worker
        .run()
        .await
        .map_err(|error| format!("run process compaction worker: {error}"))
}

/// Run one standalone compaction coordinator until its process is terminated.
///
/// # Errors
///
/// Returns an error when the child cannot open the shared filesystem object
/// root or run the coordinator event loop.
pub async fn run_phase0_compaction_coordinator_process_node(
    config: Phase0CompactionCoordinatorProcessConfig,
) -> Result<(), String> {
    let local = LocalFileSystem::new_with_prefix(&config.object_root)
        .map_err(|error| format!("open coordinator object root: {error}"))?;
    let store: Arc<dyn ObjectStore> = Arc::new(local);
    let coordinator = Admin::builder(config.db_path.as_str(), store)
        .with_seed(config.seed)
        .build();
    let options = CompactorOptions {
        worker: None,
        max_concurrent_compactions: 1,
        poll_interval: Duration::from_millis(config.poll_interval_millis),
        commit_compacted_interval: Duration::from_millis(config.commit_interval_millis),
        worker_heartbeat_timeout: Duration::from_millis(config.worker_heartbeat_timeout_millis),
        ..CompactorOptions::default()
    };
    coordinator
        .run_compactor_with_options(CancellationToken::new(), options)
        .await
        .map_err(|error| format!("run process compaction coordinator: {error}"))
}

/// Execute overwrite compaction through a killed and reclaimed worker process.
///
/// # Errors
///
/// Returns an error when the dataset, process controller, or `SlateDB` roles
/// cannot execute. Expected reclaim violations are returned as failed gates.
#[allow(clippy::too_many_lines)]
pub async fn run_phase0_compaction_reclaim_contract(
    config: &Phase0CompactionReclaimConfig,
    mode: Phase0CompactionReclaimMode,
) -> Result<Phase0CompactionReclaimReport, String> {
    validate_reclaim_config(config)?;
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        reports.push(run_reclaim_seed(config, *seed, mode).await?);
    }
    let initial_overwrite_l0_materialized = reports
        .iter()
        .all(|report| report.initial_l0_ssts >= config.overwrite_rounds);
    let first_worker_claimed_running = reports
        .iter()
        .all(|report| report.first_worker_claimed_running);
    let first_worker_killed = reports.iter().all(|report| report.first_worker_killed);
    let stale_claim_reclaimed = reports.iter().all(|report| report.stale_claim_reclaimed);
    let replacement_identity_fresh = reports.iter().all(|report| {
        report.first_worker_id.is_some()
            && report.replacement_worker_id.is_some()
            && report.first_worker_id != report.replacement_worker_id
    });
    let replacement_completed = mode == Phase0CompactionReclaimMode::Correct
        && reports.iter().all(|report| {
            report.replacement_completed
                && report.coordinator_completed_cleanly
                && report.final_l0_ssts < report.initial_l0_ssts
                && report.final_sorted_runs > 0
        });
    let exact_latest_overwrite_after_reclaim = reports
        .iter()
        .all(|report| report.exact_latest_overwrite_after_reclaim);
    let max_open_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let gates = vec![
        gate(
            "overwrite_l0_rounds_materialized",
            initial_overwrite_l0_materialized,
            &format!(
                "every seed exposed at least {} overlapping L0 SSTs",
                config.overwrite_rounds
            ),
        ),
        gate(
            "first_worker_claimed_running_job",
            first_worker_claimed_running,
            "the first OS worker process persisted a Running claim before termination",
        ),
        gate(
            "first_worker_process_killed",
            first_worker_killed,
            "the controller terminated and reaped the claimed worker process",
        ),
        gate(
            "stale_worker_claim_reclaimed",
            stale_claim_reclaimed,
            "the coordinator reset the silent Running claim to unowned Scheduled",
        ),
        gate(
            "replacement_worker_identity_fresh",
            replacement_identity_fresh,
            "the replacement process claimed the job with a different worker identity",
        ),
        gate(
            "replacement_worker_completed_compaction",
            replacement_completed,
            "the replacement completed the reclaimed job and the coordinator committed its sorted run",
        ),
        gate(
            "exact_latest_overwrite_after_reclaim",
            exact_latest_overwrite_after_reclaim,
            "a fresh serving handle returned only the latest overwrite for every key",
        ),
        gate(
            "metadata_bounded_reopen_after_worker_reclaim",
            max_open_bytes <= 1_048_576,
            &format!("fresh-instance open read {max_open_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget_after_worker_reclaim",
            max_first_point_requests <= 8,
            &format!(
                "first correct point read used {max_first_point_requests} requests; ceiling is 8"
            ),
        ),
        gate(
            "first_cold_point_byte_budget_after_worker_reclaim",
            max_first_point_bytes <= 524_288,
            &format!(
                "first correct point read fetched {max_first_point_bytes} bytes; ceiling is 524288"
            ),
        ),
    ];

    Ok(Phase0CompactionReclaimReport {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0CompactionReclaimMode::Correct => "correct",
            Phase0CompactionReclaimMode::SkipReplacementWorker => "skip_replacement_worker",
        }
        .to_owned(),
        physical: Phase0PhysicalProfile::ObjectKvServingV1.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        overwrite_rounds: config.overwrite_rounds,
        seeds: reports,
        gates,
    })
}

/// Execute overwrite compaction through coordinator process death after the
/// worker persists its output but before the manifest commits it.
///
/// # Errors
///
/// Returns an error when the dataset, process controller, or `SlateDB` roles
/// cannot execute. Expected recovery violations are returned as failed gates.
#[allow(clippy::too_many_lines)]
pub async fn run_phase0_coordinator_recovery_contract(
    config: &Phase0CoordinatorRecoveryConfig,
    mode: Phase0CoordinatorRecoveryMode,
) -> Result<Phase0CoordinatorRecoveryReport, String> {
    validate_coordinator_recovery_config(config)?;
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        reports.push(run_coordinator_recovery_seed(config, *seed, mode).await?);
    }
    let initial_overwrite_l0_materialized = reports
        .iter()
        .all(|report| report.initial_l0_ssts >= config.overwrite_rounds);
    let compacted_output_persisted = reports
        .iter()
        .all(|report| report.compacted_output_persisted_before_kill);
    let first_coordinator_killed = reports.iter().all(|report| report.first_coordinator_killed);
    let manifest_unchanged_before_restart = reports
        .iter()
        .all(|report| report.manifest_unchanged_before_restart);
    let replacement_identity_fresh = mode == Phase0CoordinatorRecoveryMode::Correct
        && reports.iter().all(|report| {
            report
                .replacement_coordinator_pid
                .is_some_and(|pid| pid != report.first_coordinator_pid)
        });
    let replacement_committed_existing_output = mode == Phase0CoordinatorRecoveryMode::Correct
        && reports.iter().all(|report| {
            report.replacement_committed_existing_output
                && report.final_l0_ssts < report.initial_l0_ssts
                && report.final_sorted_runs > 0
        });
    let exact_latest_overwrite_after_recovery = reports
        .iter()
        .all(|report| report.exact_latest_overwrite_after_recovery);
    let max_open_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let gates = vec![
        gate(
            "coordinator_recovery_overwrite_l0_materialized",
            initial_overwrite_l0_materialized,
            &format!(
                "every seed exposed at least {} overlapping L0 SSTs",
                config.overwrite_rounds
            ),
        ),
        gate(
            "compacted_output_persisted_before_coordinator_kill",
            compacted_output_persisted,
            "the worker persisted a Compacted job with output SST identities before coordinator death",
        ),
        gate(
            "first_coordinator_process_killed",
            first_coordinator_killed,
            "the controller terminated and reaped the first coordinator process",
        ),
        gate(
            "manifest_unchanged_before_coordinator_restart",
            manifest_unchanged_before_restart,
            "the first coordinator died before publishing the compacted output in the manifest",
        ),
        gate(
            "replacement_coordinator_identity_fresh",
            replacement_identity_fresh,
            "the replacement coordinator used a distinct operating-system process",
        ),
        gate(
            "replacement_coordinator_committed_existing_output",
            replacement_committed_existing_output,
            "the replacement committed the already persisted output without a replacement worker",
        ),
        gate(
            "exact_latest_overwrite_after_coordinator_recovery",
            exact_latest_overwrite_after_recovery,
            "a fresh serving handle returned only the latest overwrite for every key",
        ),
        gate(
            "metadata_bounded_reopen_after_coordinator_recovery",
            max_open_bytes <= 1_048_576,
            &format!("fresh-instance open read {max_open_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget_after_coordinator_recovery",
            max_first_point_requests <= 8,
            &format!(
                "first correct point read used {max_first_point_requests} requests; ceiling is 8"
            ),
        ),
        gate(
            "first_cold_point_byte_budget_after_coordinator_recovery",
            max_first_point_bytes <= 524_288,
            &format!(
                "first correct point read fetched {max_first_point_bytes} bytes; ceiling is 524288"
            ),
        ),
    ];

    Ok(Phase0CoordinatorRecoveryReport {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0CoordinatorRecoveryMode::Correct => "correct",
            Phase0CoordinatorRecoveryMode::SkipCoordinatorRestart => "skip_coordinator_restart",
        }
        .to_owned(),
        physical: Phase0PhysicalProfile::ObjectKvServingV1.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        overwrite_rounds: config.overwrite_rounds,
        seeds: reports,
        gates,
    })
}

#[allow(clippy::too_many_lines)]
async fn run_coordinator_recovery_seed(
    config: &Phase0CoordinatorRecoveryConfig,
    seed: u64,
    mode: Phase0CoordinatorRecoveryMode,
) -> Result<Phase0CoordinatorRecoverySeedReport, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-coordinator-recovery-")
        .tempdir()
        .map_err(|error| format!("create coordinator recovery root: {error}"))?;
    let object_root = root
        .path()
        .to_str()
        .ok_or_else(|| "coordinator recovery root is not UTF-8".to_owned())?
        .to_owned();
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open coordinator recovery object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("coordinator-recovery-seed-{seed:016x}");
    let profile = Phase0PhysicalProfile::ObjectKvServingV1;

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let db = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open coordinator recovery writer seed {seed}: {error}"))?;
    for round in 0..config.overwrite_rounds {
        let mut batch = WriteBatch::new();
        for ordinal in 0..config.key_count {
            batch.put(
                key_for(seed, ordinal),
                overwrite_value_for(config.logical_bytes, config.key_count, seed, round, ordinal),
            );
        }
        db.write(batch).await.map_err(|error| {
            format!("write coordinator recovery seed {seed} round {round}: {error}")
        })?;
        db.flush().await.map_err(|error| {
            format!("flush coordinator recovery seed {seed} round {round}: {error}")
        })?;
    }
    db.close()
        .await
        .map_err(|error| format!("close coordinator recovery writer seed {seed}: {error}"))?;
    let ingest = phase(
        "overwrite-ingest",
        config.key_count * config.overwrite_rounds,
        ingest_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_ingest),
    );

    let observer = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0x0b5e_7a11)
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read coordinator recovery manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing coordinator recovery manifest seed {seed}"))?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;

    let before_recovery = counters.snapshot();
    let recovery_started = Instant::now();
    let mut first_coordinator = CoordinatorProcess::spawn(
        &config.process_binary,
        &Phase0CompactionCoordinatorProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0xc001_d1a7,
            poll_interval_millis: 1_000,
            commit_interval_millis: 1_000,
            worker_heartbeat_timeout_millis: 5_000,
        },
    )?;
    let first_coordinator_pid = first_coordinator.id();
    let mut worker = WorkerProcess::spawn(
        &config.process_binary,
        &Phase0CompactionWorkerProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0xa11c_e55e,
        },
    )?;
    let compacted_timeout = Duration::from_millis(config.compacted_timeout_millis);
    let compacted_started = Instant::now();
    let mut compacted_job = None;
    while compacted_started.elapsed() < compacted_timeout {
        if let Some(compaction) = newest_compaction(&observer).await? {
            if compaction.status() == CompactionStatus::Compacted {
                compacted_job = Some(compaction);
                break;
            }
            if compaction.status() == CompactionStatus::Completed {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let compacted_job_id = compacted_job.as_ref().map(|job| job.id().to_string());
    let compacted_output_ssts = compacted_job
        .as_ref()
        .map(Compaction::output_ssts)
        .unwrap_or_default()
        .iter()
        .map(|sst| format!("{:?}", sst.id))
        .collect::<Vec<_>>();
    let compacted_output_persisted_before_kill = !compacted_output_ssts.is_empty();
    let first_coordinator_killed = first_coordinator.terminate();
    let kill_started = Instant::now();
    worker.terminate();

    let manifest_before_restart = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read pre-restart manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing pre-restart manifest seed {seed}"))?;
    let manifest_unchanged_before_restart = manifest_before_restart.l0().len()
        == initial_manifest.l0().len()
        && manifest_before_restart.compacted().len() == initial_manifest.compacted().len();

    let mut replacement_process = if mode == Phase0CoordinatorRecoveryMode::Correct {
        Some(CoordinatorProcess::spawn(
            &config.process_binary,
            &Phase0CompactionCoordinatorProcessConfig {
                object_root,
                db_path: db_path.clone(),
                seed: seed ^ 0x5ec0_0002,
                poll_interval_millis: 25,
                commit_interval_millis: 25,
                worker_heartbeat_timeout_millis: 5_000,
            },
        )?)
    } else {
        None
    };
    let replacement_coordinator_pid = replacement_process.as_ref().map(CoordinatorProcess::id);
    let completion_timeout = Duration::from_millis(config.completion_timeout_millis);
    let completion_started = Instant::now();
    let mut replacement_committed_existing_output = false;
    if replacement_process.is_some() {
        while completion_started.elapsed() < completion_timeout {
            let manifest = observer
                .read_manifest(None)
                .await
                .map_err(|error| format!("poll replacement manifest seed {seed}: {error}"))?
                .ok_or_else(|| format!("missing replacement manifest seed {seed}"))?;
            let final_output_ssts = manifest
                .compacted()
                .iter()
                .flat_map(|run| run.sst_views().iter())
                .map(|view| format!("{:?}", view.sst.id))
                .collect::<Vec<_>>();
            if manifest.l0().len() < initial_manifest.l0().len()
                && !manifest.compacted().is_empty()
                && final_output_ssts == compacted_output_ssts
            {
                replacement_committed_existing_output = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    if let Some(process) = &mut replacement_process {
        process.terminate();
    }
    let kill_to_completion_seconds = if replacement_committed_existing_output {
        kill_started.elapsed().as_secs_f64()
    } else {
        0.0
    };
    let final_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read final coordinator recovery manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing final coordinator recovery manifest seed {seed}"))?;
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let coordinator_recovery = phase(
        "coordinator-kill-output-adoption",
        u64::from(first_coordinator_killed) + u64::from(replacement_committed_existing_output),
        recovery_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_recovery),
    );

    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let reopened = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("reopen coordinator recovery DB seed {seed}: {error}"))?;
    let reopen_open = phase(
        "reopen-open",
        1,
        reopen_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reopen),
    );
    let latest_round = config.overwrite_rounds - 1;
    let first_ordinal = seed % config.key_count;
    let before_first = counters.snapshot();
    let first_started = Instant::now();
    let first_value_observed = reopened
        .get(key_for(seed, first_ordinal))
        .await
        .map_err(|error| format!("first coordinator recovery read seed {seed}: {error}"))?;
    let expected = overwrite_value_for(
        config.logical_bytes,
        config.key_count,
        seed,
        latest_round,
        first_ordinal,
    );
    let first_exact = first_value_observed.as_deref() == Some(expected.as_slice());
    let first_correct_read = phase(
        "first-correct-read",
        1,
        first_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_first),
    );
    let reopen_first_correct_read_seconds =
        reopen_open.elapsed_seconds + first_correct_read.elapsed_seconds;
    let before_verify = counters.snapshot();
    let verify_started = Instant::now();
    let exact_latest_overwrite_after_recovery = first_exact
        && check_overwrite_scan_for_shape(
            &reopened,
            config.logical_bytes,
            config.key_count,
            seed,
            latest_round,
        )
        .await?;
    let full_verify = phase(
        "full-overwrite-verify",
        config.key_count,
        verify_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_verify),
    );
    reopened
        .close()
        .await
        .map_err(|error| format!("close coordinator recovery DB seed {seed}: {error}"))?;
    let total_io_observed_by_controller = counters.snapshot().difference(&IoSnapshot::default());

    Ok(Phase0CoordinatorRecoverySeedReport {
        seed,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        first_coordinator_pid,
        replacement_coordinator_pid,
        compacted_job_id,
        compacted_output_ssts,
        first_coordinator_killed,
        compacted_output_persisted_before_kill,
        manifest_unchanged_before_restart,
        replacement_committed_existing_output,
        exact_latest_overwrite_after_recovery,
        kill_to_completion_seconds,
        reopen_first_correct_read_seconds,
        ingest,
        coordinator_recovery,
        reopen_open,
        first_correct_read,
        full_verify,
        total_io_observed_by_controller,
    })
}

/// Execute compaction while two coordinator processes contend for one authority epoch.
///
/// # Errors
///
/// Returns an error when the dataset, process controller, or `SlateDB` roles
/// cannot execute. Expected fencing violations are returned as failed gates.
#[allow(clippy::too_many_lines)]
pub async fn run_phase0_coordinator_fencing_contract(
    config: &Phase0CoordinatorFencingConfig,
    mode: Phase0CoordinatorFencingMode,
) -> Result<Phase0CoordinatorFencingReport, String> {
    validate_coordinator_fencing_config(config)?;
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        reports.push(run_coordinator_fencing_seed(config, *seed, mode).await?);
    }
    let initial_overwrite_l0_materialized = reports
        .iter()
        .all(|report| report.initial_l0_ssts >= config.overwrite_rounds);
    let first_epoch_advanced = reports
        .iter()
        .all(|report| report.first_compactor_epoch > report.compactor_epoch_before);
    let second_epoch_advanced = reports
        .iter()
        .all(|report| report.second_compactor_epoch > report.first_compactor_epoch);
    let stale_coordinator_self_fenced = mode == Phase0CoordinatorFencingMode::Correct
        && reports.iter().all(|report| {
            report.first_coordinator_self_fenced && !report.first_coordinator_killed_by_controller
        });
    let successor_remained_authoritative = reports.iter().all(|report| {
        report.second_coordinator_active_at_completion
            && report.second_coordinator_completed_compaction
            && report.final_l0_ssts < report.initial_l0_ssts
            && report.final_sorted_runs > 0
    });
    let exact_latest_overwrite_after_fencing = reports
        .iter()
        .all(|report| report.exact_latest_overwrite_after_fencing);
    let max_open_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let gates = vec![
        gate(
            "coordinator_fencing_overwrite_l0_materialized",
            initial_overwrite_l0_materialized,
            &format!(
                "every seed exposed at least {} overlapping L0 SSTs",
                config.overwrite_rounds
            ),
        ),
        gate(
            "first_coordinator_epoch_advanced",
            first_epoch_advanced,
            "the first coordinator acquired an epoch and persisted compaction state",
        ),
        gate(
            "second_coordinator_epoch_advanced",
            second_epoch_advanced,
            "the second live coordinator acquired a strictly newer authority epoch",
        ),
        gate(
            "stale_coordinator_self_fenced",
            stale_coordinator_self_fenced,
            "the older live coordinator exited on its stale epoch without controller termination",
        ),
        gate(
            "successor_coordinator_completed_compaction",
            successor_remained_authoritative,
            "the newer coordinator remained live and committed compaction through its epoch",
        ),
        gate(
            "exact_latest_overwrite_after_coordinator_fencing",
            exact_latest_overwrite_after_fencing,
            "a fresh serving handle returned only the latest overwrite for every key",
        ),
        gate(
            "metadata_bounded_reopen_after_coordinator_fencing",
            max_open_bytes <= 1_048_576,
            &format!("fresh-instance open read {max_open_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget_after_coordinator_fencing",
            max_first_point_requests <= 8,
            &format!(
                "first correct point read used {max_first_point_requests} requests; ceiling is 8"
            ),
        ),
        gate(
            "first_cold_point_byte_budget_after_coordinator_fencing",
            max_first_point_bytes <= 524_288,
            &format!(
                "first correct point read fetched {max_first_point_bytes} bytes; ceiling is 524288"
            ),
        ),
    ];

    Ok(Phase0CoordinatorFencingReport {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0CoordinatorFencingMode::Correct => "correct",
            Phase0CoordinatorFencingMode::KillStaleCoordinator => "kill_stale_coordinator",
        }
        .to_owned(),
        physical: Phase0PhysicalProfile::ObjectKvServingV1.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        overwrite_rounds: config.overwrite_rounds,
        seeds: reports,
        gates,
    })
}

#[allow(clippy::too_many_lines)]
async fn run_coordinator_fencing_seed(
    config: &Phase0CoordinatorFencingConfig,
    seed: u64,
    mode: Phase0CoordinatorFencingMode,
) -> Result<Phase0CoordinatorFencingSeedReport, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-coordinator-fencing-")
        .tempdir()
        .map_err(|error| format!("create coordinator fencing root: {error}"))?;
    let object_root = root
        .path()
        .to_str()
        .ok_or_else(|| "coordinator fencing root is not UTF-8".to_owned())?
        .to_owned();
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open coordinator fencing object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("coordinator-fencing-seed-{seed:016x}");
    let profile = Phase0PhysicalProfile::ObjectKvServingV1;

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let db = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open coordinator fencing writer seed {seed}: {error}"))?;
    for round in 0..config.overwrite_rounds {
        let mut batch = WriteBatch::new();
        for ordinal in 0..config.key_count {
            batch.put(
                key_for(seed, ordinal),
                overwrite_value_for(config.logical_bytes, config.key_count, seed, round, ordinal),
            );
        }
        db.write(batch).await.map_err(|error| {
            format!("write coordinator fencing seed {seed} round {round}: {error}")
        })?;
        db.flush().await.map_err(|error| {
            format!("flush coordinator fencing seed {seed} round {round}: {error}")
        })?;
    }
    db.close()
        .await
        .map_err(|error| format!("close coordinator fencing writer seed {seed}: {error}"))?;
    let ingest = phase(
        "overwrite-ingest",
        config.key_count * config.overwrite_rounds,
        ingest_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_ingest),
    );

    let observer = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0x0b5e_7a11)
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read coordinator fencing manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing coordinator fencing manifest seed {seed}"))?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;
    let compactor_epoch_before = initial_manifest.compactor_epoch();

    let before_fencing = counters.snapshot();
    let fencing_started = Instant::now();
    let mut first_coordinator = CoordinatorProcess::spawn(
        &config.process_binary,
        &Phase0CompactionCoordinatorProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0xc001_d1a7,
            poll_interval_millis: 25,
            commit_interval_millis: 25,
            worker_heartbeat_timeout_millis: 5_000,
        },
    )?;
    let first_coordinator_pid = first_coordinator.id();
    let first_epoch_started = Instant::now();
    let first_compactor_epoch = wait_for_compactor_epoch(
        &observer,
        compactor_epoch_before,
        Duration::from_millis(config.fencing_timeout_millis),
    )
    .await?;
    let epoch_advance_to_first_seconds = first_epoch_started.elapsed().as_secs_f64();

    let scheduled_started = Instant::now();
    while scheduled_started.elapsed() < Duration::from_millis(config.fencing_timeout_millis) {
        if newest_compaction(&observer).await?.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if newest_compaction(&observer).await?.is_none() {
        return Err(format!(
            "first coordinator did not persist compaction state for seed {seed}"
        ));
    }

    let second_epoch_started = Instant::now();
    let mut second_coordinator = CoordinatorProcess::spawn(
        &config.process_binary,
        &Phase0CompactionCoordinatorProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0x5ec0_0002,
            poll_interval_millis: 25,
            commit_interval_millis: 25,
            worker_heartbeat_timeout_millis: 5_000,
        },
    )?;
    let second_coordinator_pid = second_coordinator.id();
    let second_compactor_epoch = wait_for_compactor_epoch(
        &observer,
        first_compactor_epoch,
        Duration::from_millis(config.fencing_timeout_millis),
    )
    .await?;
    let epoch_advance_to_second_seconds = second_epoch_started.elapsed().as_secs_f64();

    let stale_exit_started = Instant::now();
    let mut first_coordinator_self_fenced = false;
    let mut first_coordinator_killed_by_controller = false;
    if mode == Phase0CoordinatorFencingMode::Correct {
        while stale_exit_started.elapsed() < Duration::from_millis(config.fencing_timeout_millis) {
            if first_coordinator.exited() {
                first_coordinator_self_fenced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if !first_coordinator_self_fenced {
            first_coordinator_killed_by_controller = first_coordinator.terminate();
        }
    } else {
        first_coordinator_killed_by_controller = first_coordinator.terminate();
    }
    let second_epoch_to_first_exit_seconds = stale_exit_started.elapsed().as_secs_f64();

    let mut worker = WorkerProcess::spawn(
        &config.process_binary,
        &Phase0CompactionWorkerProcessConfig {
            object_root,
            db_path: db_path.clone(),
            seed: seed ^ 0xa11c_e55e,
        },
    )?;
    let completion_started = Instant::now();
    let mut second_coordinator_completed_compaction = false;
    while completion_started.elapsed() < Duration::from_millis(config.completion_timeout_millis) {
        let manifest = observer
            .read_manifest(None)
            .await
            .map_err(|error| format!("poll fenced coordinator manifest seed {seed}: {error}"))?
            .ok_or_else(|| format!("missing fenced coordinator manifest seed {seed}"))?;
        if manifest.compactor_epoch() == second_compactor_epoch
            && manifest.l0().len() < initial_manifest.l0().len()
            && !manifest.compacted().is_empty()
        {
            second_coordinator_completed_compaction = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let second_coordinator_active_at_completion = !second_coordinator.exited();
    worker.terminate();
    second_coordinator.terminate();
    let final_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read final coordinator fencing manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing final coordinator fencing manifest seed {seed}"))?;
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let coordinator_fencing = phase(
        "concurrent-coordinator-epoch-fencing",
        u64::from(first_coordinator_self_fenced)
            + u64::from(second_coordinator_completed_compaction),
        fencing_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_fencing),
    );

    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let reopened = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("reopen coordinator fencing DB seed {seed}: {error}"))?;
    let reopen_open = phase(
        "reopen-open",
        1,
        reopen_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reopen),
    );
    let latest_round = config.overwrite_rounds - 1;
    let first_ordinal = seed % config.key_count;
    let before_first = counters.snapshot();
    let first_started = Instant::now();
    let first_value_observed = reopened
        .get(key_for(seed, first_ordinal))
        .await
        .map_err(|error| format!("first coordinator fencing read seed {seed}: {error}"))?;
    let expected = overwrite_value_for(
        config.logical_bytes,
        config.key_count,
        seed,
        latest_round,
        first_ordinal,
    );
    let first_exact = first_value_observed.as_deref() == Some(expected.as_slice());
    let first_correct_read = phase(
        "first-correct-read",
        1,
        first_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_first),
    );
    let before_verify = counters.snapshot();
    let verify_started = Instant::now();
    let exact_latest_overwrite_after_fencing = first_exact
        && check_overwrite_scan_for_shape(
            &reopened,
            config.logical_bytes,
            config.key_count,
            seed,
            latest_round,
        )
        .await?;
    let full_verify = phase(
        "full-overwrite-verify",
        config.key_count,
        verify_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_verify),
    );
    reopened
        .close()
        .await
        .map_err(|error| format!("close coordinator fencing DB seed {seed}: {error}"))?;
    let total_io_observed_by_controller = counters.snapshot().difference(&IoSnapshot::default());

    Ok(Phase0CoordinatorFencingSeedReport {
        seed,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        compactor_epoch_before,
        first_compactor_epoch,
        second_compactor_epoch,
        first_coordinator_pid,
        second_coordinator_pid,
        first_coordinator_self_fenced,
        first_coordinator_killed_by_controller,
        second_coordinator_active_at_completion,
        second_coordinator_completed_compaction,
        exact_latest_overwrite_after_fencing,
        epoch_advance_to_first_seconds,
        epoch_advance_to_second_seconds,
        second_epoch_to_first_exit_seconds,
        ingest,
        coordinator_fencing,
        reopen_open,
        first_correct_read,
        full_verify,
        total_io_observed_by_controller,
    })
}

/// Preserve active worker output, then collect one aged unreferenced SST.
///
/// # Errors
///
/// Returns an error when the dataset, process controller, object inventory, or
/// garbage collector cannot execute. Expected deletion violations are failed gates.
#[allow(clippy::too_many_lines)]
pub async fn run_phase0_orphan_gc_contract(
    config: &Phase0OrphanGcConfig,
    mode: Phase0OrphanGcMode,
) -> Result<Phase0OrphanGcReport, String> {
    validate_orphan_gc_config(config)?;
    let mut reports = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        reports.push(run_orphan_gc_seed(config, *seed, mode).await?);
    }
    let initial_overwrite_l0_materialized = reports
        .iter()
        .all(|report| report.initial_l0_ssts >= config.overwrite_rounds);
    let active_output_persisted = reports
        .iter()
        .all(|report| report.active_compacted_output_persisted);
    let active_output_preserved = reports
        .iter()
        .all(|report| report.active_output_survived_gc);
    let active_output_committed = reports
        .iter()
        .all(|report| report.active_output_committed_after_gc);
    let orphan_created = reports.iter().all(|report| report.orphan_created);
    let orphan_deleted =
        mode == Phase0OrphanGcMode::Correct && reports.iter().all(|report| report.orphan_deleted);
    let exact_latest_overwrite_after_gc = reports
        .iter()
        .all(|report| report.exact_latest_overwrite_after_gc);
    let max_open_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let max_first_point_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let gates = vec![
        gate(
            "orphan_gc_overwrite_l0_materialized",
            initial_overwrite_l0_materialized,
            &format!(
                "every seed exposed at least {} overlapping L0 SSTs",
                config.overwrite_rounds
            ),
        ),
        gate(
            "active_compacted_output_persisted",
            active_output_persisted,
            "the worker persisted final output while its job remained active",
        ),
        gate(
            "active_compacted_output_survived_gc",
            active_output_preserved,
            "garbage collection retained every output protected by active compaction state",
        ),
        gate(
            "preserved_output_committed_after_gc",
            active_output_committed,
            "a replacement coordinator committed the exact preserved output",
        ),
        gate(
            "aged_unreferenced_sst_created",
            orphan_created,
            "the controller created one old compacted SST absent from every authority root",
        ),
        gate(
            "aged_unreferenced_sst_deleted",
            orphan_deleted,
            "garbage collection deleted the aged SST absent from manifests and compaction jobs",
        ),
        gate(
            "exact_latest_overwrite_after_orphan_gc",
            exact_latest_overwrite_after_gc,
            "a fresh serving handle returned only the latest overwrite for every key",
        ),
        gate(
            "metadata_bounded_reopen_after_orphan_gc",
            max_open_bytes <= 1_048_576,
            &format!("fresh-instance open read {max_open_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget_after_orphan_gc",
            max_first_point_requests <= 8,
            &format!(
                "first correct point read used {max_first_point_requests} requests; ceiling is 8"
            ),
        ),
        gate(
            "first_cold_point_byte_budget_after_orphan_gc",
            max_first_point_bytes <= 524_288,
            &format!(
                "first correct point read fetched {max_first_point_bytes} bytes; ceiling is 524288"
            ),
        ),
    ];

    Ok(Phase0OrphanGcReport {
        contract_version: 1,
        slatedb_revision: SLATEDB_REVISION.to_owned(),
        store: STORE_KIND.to_owned(),
        mode: match mode {
            Phase0OrphanGcMode::Correct => "correct",
            Phase0OrphanGcMode::DryRunOrphanDeletion => "dry_run_orphan_deletion",
        }
        .to_owned(),
        physical: Phase0PhysicalProfile::ObjectKvServingV1.receipt(),
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        overwrite_rounds: config.overwrite_rounds,
        seeds: reports,
        gates,
    })
}

#[allow(clippy::too_many_lines)]
async fn run_orphan_gc_seed(
    config: &Phase0OrphanGcConfig,
    seed: u64,
    mode: Phase0OrphanGcMode,
) -> Result<Phase0OrphanGcSeedReport, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-orphan-gc-")
        .tempdir()
        .map_err(|error| format!("create orphan GC root: {error}"))?;
    let object_root = root
        .path()
        .to_str()
        .ok_or_else(|| "orphan GC root is not UTF-8".to_owned())?
        .to_owned();
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open orphan GC object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("orphan-gc-seed-{seed:016x}");
    let profile = Phase0PhysicalProfile::ObjectKvServingV1;

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let db = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open orphan GC writer seed {seed}: {error}"))?;
    for round in 0..config.overwrite_rounds {
        let mut batch = WriteBatch::new();
        for ordinal in 0..config.key_count {
            batch.put(
                key_for(seed, ordinal),
                overwrite_value_for(config.logical_bytes, config.key_count, seed, round, ordinal),
            );
        }
        db.write(batch)
            .await
            .map_err(|error| format!("write orphan GC seed {seed} round {round}: {error}"))?;
        db.flush()
            .await
            .map_err(|error| format!("flush orphan GC seed {seed} round {round}: {error}"))?;
    }
    db.close()
        .await
        .map_err(|error| format!("close orphan GC writer seed {seed}: {error}"))?;
    let ingest = phase(
        "overwrite-ingest",
        config.key_count * config.overwrite_rounds,
        ingest_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_ingest),
    );

    let observer = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0x0b5e_7a11)
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read orphan GC manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing orphan GC manifest seed {seed}"))?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;

    let before_gc = counters.snapshot();
    let gc_started = Instant::now();
    let mut first_coordinator = CoordinatorProcess::spawn(
        &config.process_binary,
        &Phase0CompactionCoordinatorProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0xc001_d1a7,
            poll_interval_millis: 1_000,
            commit_interval_millis: 1_000,
            worker_heartbeat_timeout_millis: 5_000,
        },
    )?;
    let mut worker = WorkerProcess::spawn(
        &config.process_binary,
        &Phase0CompactionWorkerProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0xa11c_e55e,
        },
    )?;
    let compacted_started = Instant::now();
    let mut active_compacted_output_persisted = false;
    while compacted_started.elapsed() < Duration::from_millis(config.compacted_timeout_millis) {
        if newest_compaction(&observer)
            .await?
            .is_some_and(|compaction| {
                compaction.status() == CompactionStatus::Compacted
                    && !compaction.output_ssts().is_empty()
            })
        {
            active_compacted_output_persisted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    first_coordinator.terminate();
    worker.terminate();

    let compacted_prefix = format!("{db_path}/compacted");
    let active_output_paths = list_object_paths(&store, &compacted_prefix).await?;
    active_compacted_output_persisted &= !active_output_paths.is_empty();
    let active_gc_started = Instant::now();
    observer
        .run_gc_once(compacted_only_gc_options(false))
        .await
        .map_err(|error| format!("run active-output GC seed {seed}: {error}"))?;
    let active_gc_seconds = active_gc_started.elapsed().as_secs_f64();
    let mut active_output_survived_gc = true;
    for output in &active_output_paths {
        active_output_survived_gc &= store.head(&Path::from(output.as_str())).await.is_ok();
    }

    let mut replacement = CoordinatorProcess::spawn(
        &config.process_binary,
        &Phase0CompactionCoordinatorProcessConfig {
            object_root: object_root.clone(),
            db_path: db_path.clone(),
            seed: seed ^ 0x5ec0_0002,
            poll_interval_millis: 25,
            commit_interval_millis: 25,
            worker_heartbeat_timeout_millis: 5_000,
        },
    )?;
    let completion_started = Instant::now();
    let mut active_output_committed_after_gc = false;
    while completion_started.elapsed() < Duration::from_millis(config.completion_timeout_millis) {
        let manifest = observer
            .read_manifest(None)
            .await
            .map_err(|error| format!("poll post-GC manifest seed {seed}: {error}"))?
            .ok_or_else(|| format!("missing post-GC manifest seed {seed}"))?;
        if manifest.l0().len() < initial_manifest.l0().len() && !manifest.compacted().is_empty() {
            active_output_committed_after_gc = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    replacement.terminate();

    let orphan_path = format!("{db_path}/compacted/00000000000000000000000001.sst");
    store
        .put(
            &Path::from(orphan_path.as_str()),
            Bytes::from_static(b"okv-aged-unreferenced-sst").into(),
        )
        .await
        .map_err(|error| format!("create aged orphan seed {seed}: {error}"))?;
    let orphan_created = store.head(&Path::from(orphan_path.as_str())).await.is_ok();
    let orphan_gc_started = Instant::now();
    observer
        .run_gc_once(compacted_only_gc_options(
            mode == Phase0OrphanGcMode::DryRunOrphanDeletion,
        ))
        .await
        .map_err(|error| format!("run true-orphan GC seed {seed}: {error}"))?;
    let orphan_gc_seconds = orphan_gc_started.elapsed().as_secs_f64();
    let orphan_deleted = store.head(&Path::from(orphan_path.as_str())).await.is_err();

    let final_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read final orphan GC manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing final orphan GC manifest seed {seed}"))?;
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let garbage_collection = phase(
        "active-output-and-aged-orphan-gc",
        2,
        gc_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_gc),
    );

    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let reopened = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("reopen orphan GC DB seed {seed}: {error}"))?;
    let reopen_open = phase(
        "reopen-open",
        1,
        reopen_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reopen),
    );
    let latest_round = config.overwrite_rounds - 1;
    let first_ordinal = seed % config.key_count;
    let before_first = counters.snapshot();
    let first_started = Instant::now();
    let first_value_observed = reopened
        .get(key_for(seed, first_ordinal))
        .await
        .map_err(|error| format!("first orphan GC read seed {seed}: {error}"))?;
    let expected = overwrite_value_for(
        config.logical_bytes,
        config.key_count,
        seed,
        latest_round,
        first_ordinal,
    );
    let first_exact = first_value_observed.as_deref() == Some(expected.as_slice());
    let first_correct_read = phase(
        "first-correct-read",
        1,
        first_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_first),
    );
    let before_verify = counters.snapshot();
    let verify_started = Instant::now();
    let exact_latest_overwrite_after_gc = first_exact
        && check_overwrite_scan_for_shape(
            &reopened,
            config.logical_bytes,
            config.key_count,
            seed,
            latest_round,
        )
        .await?;
    let full_verify = phase(
        "full-overwrite-verify",
        config.key_count,
        verify_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_verify),
    );
    reopened
        .close()
        .await
        .map_err(|error| format!("close orphan GC DB seed {seed}: {error}"))?;
    let total_io_observed_by_controller = counters.snapshot().difference(&IoSnapshot::default());

    Ok(Phase0OrphanGcSeedReport {
        seed,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        active_output_paths,
        active_compacted_output_persisted,
        active_output_survived_gc,
        active_output_committed_after_gc,
        orphan_path,
        orphan_created,
        orphan_deleted,
        exact_latest_overwrite_after_gc,
        active_gc_seconds,
        orphan_gc_seconds,
        ingest,
        garbage_collection,
        reopen_open,
        first_correct_read,
        full_verify,
        total_io_observed_by_controller,
    })
}

async fn list_object_paths(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let prefix = Path::from(prefix);
    let mut objects = store.list(Some(&prefix));
    let mut paths = Vec::new();
    while let Some(object) = objects.next().await {
        let object = object.map_err(|error| format!("list {prefix}: {error}"))?;
        paths.push(object.location.to_string());
    }
    paths.sort();
    Ok(paths)
}

fn compacted_only_gc_options(dry_run: bool) -> GarbageCollectorOptions {
    GarbageCollectorOptions {
        manifest_options: None,
        wal_options: None,
        wal_fence_options: None,
        compacted_options: Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::ZERO,
            dry_run,
        }),
        compactions_options: None,
        detach_options: None,
        metric_level: None,
        boundary_files_enabled: false,
        object_store_max_retries: Some(3),
    }
}

#[allow(clippy::too_many_lines)]
async fn run_reclaim_seed(
    config: &Phase0CompactionReclaimConfig,
    seed: u64,
    mode: Phase0CompactionReclaimMode,
) -> Result<Phase0CompactionReclaimSeedReport, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-compaction-reclaim-")
        .tempdir()
        .map_err(|error| format!("create compaction reclaim root: {error}"))?;
    let object_root = root
        .path()
        .to_str()
        .ok_or_else(|| "compaction reclaim root is not UTF-8".to_owned())?
        .to_owned();
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open reclaim object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("reclaim-seed-{seed:016x}");
    let profile = Phase0PhysicalProfile::ObjectKvServingV1;

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let db = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open reclaim writer seed {seed}: {error}"))?;
    for round in 0..config.overwrite_rounds {
        let mut batch = WriteBatch::new();
        for ordinal in 0..config.key_count {
            batch.put(
                key_for(seed, ordinal),
                overwrite_value_for(config.logical_bytes, config.key_count, seed, round, ordinal),
            );
        }
        db.write(batch)
            .await
            .map_err(|error| format!("write reclaim seed {seed} round {round}: {error}"))?;
        db.flush()
            .await
            .map_err(|error| format!("flush reclaim seed {seed} round {round}: {error}"))?;
    }
    db.close()
        .await
        .map_err(|error| format!("close reclaim writer seed {seed}: {error}"))?;
    let ingest = phase(
        "overwrite-ingest",
        config.key_count * config.overwrite_rounds,
        ingest_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_ingest),
    );

    let observer = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0x0b5e_7a11)
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read reclaim initial manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing reclaim initial manifest seed {seed}"))?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;

    let before_reclaim = counters.snapshot();
    let reclaim_started = Instant::now();
    let coordinator = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0xc001_d1a7)
        .build();
    let coordinator_options = CompactorOptions {
        worker: None,
        max_concurrent_compactions: 1,
        poll_interval: Duration::from_millis(25),
        commit_compacted_interval: Duration::from_millis(25),
        worker_heartbeat_timeout: Duration::from_millis(250),
        ..CompactorOptions::default()
    };
    let coordinator_cancel = CancellationToken::new();
    let coordinator_cancel_task = coordinator_cancel.clone();
    let coordinator_task = tokio::spawn(async move {
        coordinator
            .run_compactor_with_options(coordinator_cancel_task, coordinator_options)
            .await
    });

    let first_process_config = Phase0CompactionWorkerProcessConfig {
        object_root: object_root.clone(),
        db_path: db_path.clone(),
        seed: seed ^ 0xf175_7001,
    };
    let mut first_process = WorkerProcess::spawn(&config.worker_binary, &first_process_config)?;
    let claim_timeout = Duration::from_millis(config.claim_timeout_millis);
    let claim_started = Instant::now();
    let mut claimed_compaction: Option<Compaction> = None;
    while claim_started.elapsed() < claim_timeout {
        if let Some(compaction) = newest_compaction(&observer).await? {
            if compaction.status() == CompactionStatus::Running && compaction.worker().is_some() {
                claimed_compaction = Some(compaction);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let first_worker_id = claimed_compaction
        .as_ref()
        .and_then(Compaction::worker)
        .map(|worker| worker.worker_id.clone());
    let compaction_id = claimed_compaction.as_ref().map(Compaction::id);
    let first_worker_claimed_running = compaction_id.is_some();
    if first_worker_claimed_running {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let first_worker_killed = first_process.terminate();
    let kill_started = Instant::now();

    let reclaim_timeout = Duration::from_millis(config.reclaim_timeout_millis);
    let reclaim_wait_started = Instant::now();
    let mut stale_claim_reclaimed = false;
    while reclaim_wait_started.elapsed() < reclaim_timeout {
        if let Some(id) = compaction_id {
            if let Some(compaction) = observer
                .read_compaction(id, None)
                .await
                .map_err(|error| format!("read reclaimed compaction seed {seed}: {error}"))?
            {
                if compaction.status() == CompactionStatus::Scheduled
                    && compaction.worker().is_none()
                {
                    stale_claim_reclaimed = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut replacement_process = if mode == Phase0CompactionReclaimMode::Correct {
        Some(WorkerProcess::spawn(
            &config.worker_binary,
            &Phase0CompactionWorkerProcessConfig {
                object_root,
                db_path: db_path.clone(),
                seed: seed ^ 0x5ec0_0002,
            },
        )?)
    } else {
        None
    };
    let completion_timeout = Duration::from_millis(config.completion_timeout_millis);
    let completion_started = Instant::now();
    let mut replacement_worker_id = None;
    let mut replacement_completed = false;
    if replacement_process.is_some() {
        while completion_started.elapsed() < completion_timeout {
            if let Some(id) = compaction_id {
                if let Some(compaction) = observer
                    .read_compaction(id, None)
                    .await
                    .map_err(|error| format!("read replacement compaction seed {seed}: {error}"))?
                {
                    if let Some(worker) = compaction.worker() {
                        replacement_worker_id = Some(worker.worker_id.clone());
                    }
                    if compaction.status() == CompactionStatus::Completed {
                        let manifest = observer
                            .read_manifest(None)
                            .await
                            .map_err(|error| {
                                format!("poll replacement manifest seed {seed}: {error}")
                            })?
                            .ok_or_else(|| format!("missing replacement manifest seed {seed}"))?;
                        if manifest.l0().len() < initial_manifest.l0().len()
                            && !manifest.compacted().is_empty()
                        {
                            replacement_completed = true;
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    if let Some(process) = &mut replacement_process {
        process.terminate();
    }
    coordinator_cancel.cancel();
    let coordinator_completed_cleanly = matches!(coordinator_task.await, Ok(Ok(())));
    let kill_to_completion_seconds = if replacement_completed {
        kill_started.elapsed().as_secs_f64()
    } else {
        0.0
    };

    let final_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read reclaim final manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing reclaim final manifest seed {seed}"))?;
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let reclaim = phase(
        "worker-kill-reclaim-complete",
        u64::from(first_worker_killed) + u64::from(replacement_completed),
        reclaim_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reclaim),
    );

    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let reopened = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("reopen reclaimed DB seed {seed}: {error}"))?;
    let reopen_open = phase(
        "reopen-open",
        1,
        reopen_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reopen),
    );
    let latest_round = config.overwrite_rounds - 1;
    let first_ordinal = seed % config.key_count;
    let before_first = counters.snapshot();
    let first_started = Instant::now();
    let first_value_observed = reopened
        .get(key_for(seed, first_ordinal))
        .await
        .map_err(|error| format!("first reclaimed read seed {seed}: {error}"))?;
    let expected = overwrite_value_for(
        config.logical_bytes,
        config.key_count,
        seed,
        latest_round,
        first_ordinal,
    );
    let first_exact = first_value_observed.as_deref() == Some(expected.as_slice());
    let first_correct_read = phase(
        "first-correct-read",
        1,
        first_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_first),
    );
    let reopen_first_correct_read_seconds =
        reopen_open.elapsed_seconds + first_correct_read.elapsed_seconds;
    let before_verify = counters.snapshot();
    let verify_started = Instant::now();
    let exact_latest_overwrite_after_reclaim =
        first_exact && check_overwrite_scan(&reopened, config, seed, latest_round).await?;
    let full_verify = phase(
        "full-overwrite-verify",
        config.key_count,
        verify_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_verify),
    );
    reopened
        .close()
        .await
        .map_err(|error| format!("close reclaimed DB seed {seed}: {error}"))?;
    let total_io_observed_by_controller = counters.snapshot().difference(&IoSnapshot::default());

    Ok(Phase0CompactionReclaimSeedReport {
        seed,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        first_worker_id,
        replacement_worker_id,
        first_worker_claimed_running,
        first_worker_killed,
        stale_claim_reclaimed,
        replacement_completed,
        coordinator_completed_cleanly,
        exact_latest_overwrite_after_reclaim,
        kill_to_completion_seconds,
        reopen_first_correct_read_seconds,
        ingest,
        reclaim,
        reopen_open,
        first_correct_read,
        full_verify,
        total_io_observed_by_controller,
    })
}

async fn newest_compaction(admin: &Admin) -> Result<Option<Compaction>, String> {
    let state = admin
        .read_compactions(None)
        .await
        .map_err(|error| format!("read latest compactions: {error}"))?;
    Ok(state.and_then(|state| {
        state
            .recent_compactions()
            .max_by_key(|compaction| compaction.id())
            .cloned()
    }))
}

async fn check_overwrite_scan(
    db: &Db,
    config: &Phase0CompactionReclaimConfig,
    seed: u64,
    round: u64,
) -> Result<bool, String> {
    check_overwrite_scan_for_shape(db, config.logical_bytes, config.key_count, seed, round).await
}

async fn check_overwrite_scan_for_shape(
    db: &Db,
    logical_bytes: u64,
    key_count: u64,
    seed: u64,
    round: u64,
) -> Result<bool, String> {
    let mut iterator = db
        .scan(key_for(seed, 0)..key_for(seed, key_count))
        .await
        .map_err(|error| format!("scan reclaimed overwrites seed {seed}: {error}"))?;
    for ordinal in 0..key_count {
        let Some(row) = iterator
            .next()
            .await
            .map_err(|error| format!("scan reclaimed next seed {seed}: {error}"))?
        else {
            return Ok(false);
        };
        if row.key.as_ref() != key_for(seed, ordinal).as_slice()
            || row.value.as_ref()
                != overwrite_value_for(logical_bytes, key_count, seed, round, ordinal).as_slice()
        {
            return Ok(false);
        }
    }
    iterator
        .next()
        .await
        .map(|row| row.is_none())
        .map_err(|error| format!("scan reclaimed exhaustion seed {seed}: {error}"))
}

fn overwrite_value_for(
    logical_bytes: u64,
    key_count: u64,
    seed: u64,
    round: u64,
    ordinal: u64,
) -> Vec<u8> {
    let base = logical_bytes / key_count;
    let remainder = logical_bytes % key_count;
    let length = base + u64::from(ordinal < remainder);
    let length = usize::try_from(length).expect("configured overwrite value length fits usize");
    let mut value = Vec::with_capacity(length);
    let mut block = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"okv-phase0-overwrite-value-v1");
        hasher.update(seed.to_be_bytes());
        hasher.update(round.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(block.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length - value.len();
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        block += 1;
    }
    value
}

fn validate_reclaim_config(config: &Phase0CompactionReclaimConfig) -> Result<(), String> {
    if config.logical_bytes < config.key_count {
        return Err("reclaim logical_bytes must be at least key_count".to_owned());
    }
    if config.key_count < 2 {
        return Err("reclaim key_count must be at least two".to_owned());
    }
    if config.overwrite_rounds < 4 {
        return Err("reclaim overwrite_rounds must be at least four".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("reclaim contract requires at least one seed".to_owned());
    }
    if config.claim_timeout_millis == 0
        || config.reclaim_timeout_millis == 0
        || config.completion_timeout_millis == 0
    {
        return Err("reclaim timeouts must be greater than zero".to_owned());
    }
    if !config.worker_binary.is_file() {
        return Err(format!(
            "reclaim worker binary does not exist: {}",
            config.worker_binary.display()
        ));
    }
    Ok(())
}

fn validate_coordinator_recovery_config(
    config: &Phase0CoordinatorRecoveryConfig,
) -> Result<(), String> {
    if config.logical_bytes < config.key_count {
        return Err("coordinator recovery logical_bytes must be at least key_count".to_owned());
    }
    if config.key_count < 2 {
        return Err("coordinator recovery key_count must be at least two".to_owned());
    }
    if config.overwrite_rounds < 4 {
        return Err("coordinator recovery overwrite_rounds must be at least four".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("coordinator recovery contract requires at least one seed".to_owned());
    }
    if config.compacted_timeout_millis == 0 || config.completion_timeout_millis == 0 {
        return Err("coordinator recovery timeouts must be greater than zero".to_owned());
    }
    if !config.process_binary.is_file() {
        return Err(format!(
            "coordinator recovery process binary does not exist: {}",
            config.process_binary.display()
        ));
    }
    Ok(())
}

fn validate_coordinator_fencing_config(
    config: &Phase0CoordinatorFencingConfig,
) -> Result<(), String> {
    if config.logical_bytes < config.key_count {
        return Err("coordinator fencing logical_bytes must be at least key_count".to_owned());
    }
    if config.key_count < 2 {
        return Err("coordinator fencing key_count must be at least two".to_owned());
    }
    if config.overwrite_rounds < 4 {
        return Err("coordinator fencing overwrite_rounds must be at least four".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("coordinator fencing contract requires at least one seed".to_owned());
    }
    if config.fencing_timeout_millis == 0 || config.completion_timeout_millis == 0 {
        return Err("coordinator fencing timeouts must be greater than zero".to_owned());
    }
    if !config.process_binary.is_file() {
        return Err(format!(
            "coordinator fencing process binary does not exist: {}",
            config.process_binary.display()
        ));
    }
    Ok(())
}

fn validate_orphan_gc_config(config: &Phase0OrphanGcConfig) -> Result<(), String> {
    if config.logical_bytes < config.key_count {
        return Err("orphan GC logical_bytes must be at least key_count".to_owned());
    }
    if config.key_count < 2 {
        return Err("orphan GC key_count must be at least two".to_owned());
    }
    if config.overwrite_rounds < 4 {
        return Err("orphan GC overwrite_rounds must be at least four".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("orphan GC contract requires at least one seed".to_owned());
    }
    if config.compacted_timeout_millis == 0 || config.completion_timeout_millis == 0 {
        return Err("orphan GC timeouts must be greater than zero".to_owned());
    }
    if !config.process_binary.is_file() {
        return Err(format!(
            "orphan GC process binary does not exist: {}",
            config.process_binary.display()
        ));
    }
    Ok(())
}

async fn wait_for_compactor_epoch(
    observer: &Admin,
    after_epoch: u64,
    timeout: Duration,
) -> Result<u64, String> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let manifest = observer
            .read_manifest(None)
            .await
            .map_err(|error| format!("read compactor epoch: {error}"))?
            .ok_or_else(|| "missing manifest while reading compactor epoch".to_owned())?;
        if manifest.compactor_epoch() > after_epoch {
            return Ok(manifest.compactor_epoch());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(format!(
        "compactor epoch did not advance beyond {after_epoch} within {} ms",
        timeout.as_millis()
    ))
}

struct CoordinatorProcess {
    child: Child,
}

impl CoordinatorProcess {
    fn spawn(
        binary: &std::path::Path,
        config: &Phase0CompactionCoordinatorProcessConfig,
    ) -> Result<Self, String> {
        let config_json = serde_json::to_string(config)
            .map_err(|error| format!("serialize coordinator process config: {error}"))?;
        let child = Command::new(binary)
            .arg("slate-compaction-coordinator-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("spawn compaction coordinator process: {error}"))?;
        Ok(Self { child })
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn exited(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(Some(_)))
    }

    fn terminate(&mut self) -> bool {
        match self.child.try_wait() {
            Ok(Some(_)) | Err(_) => false,
            Ok(None) => {
                let killed = self.child.kill().is_ok();
                let reaped = self.child.wait().is_ok();
                killed && reaped
            }
        }
    }
}

impl Drop for CoordinatorProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct WorkerProcess {
    child: Child,
}

impl WorkerProcess {
    fn spawn(
        binary: &std::path::Path,
        config: &Phase0CompactionWorkerProcessConfig,
    ) -> Result<Self, String> {
        let config_json = serde_json::to_string(config)
            .map_err(|error| format!("serialize worker process config: {error}"))?;
        let child = Command::new(binary)
            .arg("slate-compaction-worker-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("spawn compaction worker process: {error}"))?;
        Ok(Self { child })
    }

    fn terminate(&mut self) -> bool {
        match self.child.try_wait() {
            Ok(Some(_)) | Err(_) => false,
            Ok(None) => {
                let killed = self.child.kill().is_ok();
                let reaped = self.child.wait().is_ok();
                killed && reaped
            }
        }
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_compaction_seed(
    config: &Phase0CompactionConfig,
    seed: u64,
    mode: Phase0CompactionMode,
) -> Result<Phase0CompactionSeedReport, String> {
    let root = tempfile::Builder::new()
        .prefix("okv-phase0-compaction-")
        .tempdir()
        .map_err(|error| format!("create compaction root: {error}"))?;
    let local = LocalFileSystem::new_with_prefix(root.path())
        .map_err(|error| format!("open compaction object store: {error}"))?;
    let counters = Arc::new(IoCounters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let db_path = format!("compaction-seed-{seed:016x}");

    run_compaction_seed_on_store(config, seed, mode, store, counters, db_path).await
}

#[allow(clippy::too_many_lines)]
async fn run_compaction_seed_on_store(
    config: &Phase0CompactionConfig,
    seed: u64,
    mode: Phase0CompactionMode,
    store: Arc<dyn ObjectStore>,
    counters: Arc<IoCounters>,
    db_path: String,
) -> Result<Phase0CompactionSeedReport, String> {
    let profile = Phase0PhysicalProfile::ObjectKvServingV1;
    let phase0_config = Phase0Config {
        logical_bytes: config.logical_bytes,
        key_count: config.key_count,
        point_reads_per_seed: 2,
        scan_rows_per_seed: 1,
        seeds: vec![seed],
        physical_profile: profile,
    };

    let before_initial_open = counters.snapshot();
    let initial_open_started = Instant::now();
    let db = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open compaction writer seed {seed}: {error}"))?;
    let initial_open = phase(
        "initial-open",
        1,
        initial_open_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_initial_open),
    );

    let before_ingest = counters.snapshot();
    let ingest_started = Instant::now();
    let keys_per_flush = config.key_count / config.flush_count;
    for flush_index in 0..config.flush_count {
        let start = flush_index * keys_per_flush;
        let end = start + keys_per_flush;
        let mut batch = WriteBatch::new();
        for ordinal in start..end {
            batch.put(
                key_for(seed, ordinal),
                value_for(config.logical_bytes, config.key_count, seed, ordinal),
            );
        }
        db.write(batch).await.map_err(|error| {
            format!("write compaction seed {seed} flush {flush_index}: {error}")
        })?;
        db.flush().await.map_err(|error| {
            format!("flush compaction seed {seed} flush {flush_index}: {error}")
        })?;
    }
    let ingest_and_flush = phase(
        "ingest-and-flush",
        config.key_count,
        ingest_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_ingest),
    );

    let before_close = counters.snapshot();
    let close_started = Instant::now();
    db.close()
        .await
        .map_err(|error| format!("close compaction writer seed {seed}: {error}"))?;
    let close_before_maintenance = phase(
        "close-before-maintenance",
        1,
        close_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_close),
    );

    let observer = Admin::builder(db_path.as_str(), Arc::clone(&store))
        .with_seed(seed ^ 0x0b5e_7a11)
        .build();
    let initial_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read initial compaction manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing initial compaction manifest seed {seed}"))?;
    let initial_l0_ssts = initial_manifest.l0().len() as u64;

    let before_maintenance = counters.snapshot();
    let maintenance_started = Instant::now();
    let mut roles_completed_cleanly = false;
    if mode == Phase0CompactionMode::Correct {
        let coordinator = Admin::builder(db_path.as_str(), Arc::clone(&store))
            .with_seed(seed ^ 0xc001_d1a7)
            .build();
        let coordinator_options = CompactorOptions {
            worker: None,
            max_concurrent_compactions: 1,
            poll_interval: Duration::from_millis(50),
            commit_compacted_interval: Duration::from_millis(50),
            worker_heartbeat_timeout: Duration::from_secs(5),
            ..CompactorOptions::default()
        };

        let worker_options = CompactionWorkerOptions {
            max_concurrent_compactions: 1,
            compactions_poll_interval: Duration::from_millis(50),
            heartbeat_interval: Duration::from_millis(100),
            max_subcompactions: 1,
            min_filter_keys: 1,
            ..CompactionWorkerOptions::default()
        };
        let worker = CompactionWorkerBuilder::new(db_path.as_str(), Arc::clone(&store))
            .with_seed(seed ^ 0xa11c_e55e)
            .with_options(worker_options)
            .with_sst_block_size(SstBlockSize::Block64Kib)
            .build()
            .await
            .map_err(|error| format!("build separate compaction worker seed {seed}: {error}"))?;

        let coordinator_cancel = CancellationToken::new();
        let coordinator_cancel_task = coordinator_cancel.clone();
        let coordinator_task = tokio::spawn(async move {
            coordinator
                .run_compactor_with_options(coordinator_cancel_task, coordinator_options)
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
        while maintenance_started.elapsed() < timeout {
            let manifest = observer
                .read_manifest(None)
                .await
                .map_err(|error| format!("poll compaction manifest seed {seed}: {error}"))?
                .ok_or_else(|| format!("missing polled compaction manifest seed {seed}"))?;
            if manifest.l0().len() < initial_manifest.l0().len() && !manifest.compacted().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        coordinator_cancel.cancel();
        worker_cancel.cancel();
        let coordinator_result = coordinator_task.await;
        let worker_result = worker_task.await;
        roles_completed_cleanly =
            matches!(coordinator_result, Ok(Ok(()))) && matches!(worker_result, Ok(Ok(())));
    }

    let final_manifest = observer
        .read_manifest(None)
        .await
        .map_err(|error| format!("read final compaction manifest seed {seed}: {error}"))?
        .ok_or_else(|| format!("missing final compaction manifest seed {seed}"))?;
    let final_l0_ssts = final_manifest.l0().len() as u64;
    let final_sorted_runs = final_manifest.compacted().len() as u64;
    let maintenance_io = counters.snapshot().difference(&before_maintenance);
    let maintenance = phase(
        "separate-role-maintenance",
        initial_l0_ssts.saturating_sub(final_l0_ssts),
        maintenance_started.elapsed().as_secs_f64(),
        maintenance_io.clone(),
    );

    let before_reopen = counters.snapshot();
    let reopen_started = Instant::now();
    let reopened = profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("reopen compacted DB seed {seed}: {error}"))?;
    let reopen_open = phase(
        "reopen-open",
        1,
        reopen_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_reopen),
    );

    let first_ordinal = seed % config.key_count;
    let first_key = key_for(seed, first_ordinal);
    let first_expected = value_for(config.logical_bytes, config.key_count, seed, first_ordinal);
    let before_first_read = counters.snapshot();
    let first_read_started = Instant::now();
    let first_observed = reopened
        .get(&first_key)
        .await
        .map_err(|error| format!("first compacted read seed {seed}: {error}"))?;
    let first_read_exact = first_observed.as_deref() == Some(first_expected.as_slice());
    let first_correct_read = phase(
        "first-correct-read",
        1,
        first_read_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_first_read),
    );
    let reopen_first_correct_read_seconds =
        reopen_open.elapsed_seconds + first_correct_read.elapsed_seconds;

    let before_verify = counters.snapshot();
    let verify_started = Instant::now();
    let exact_dataset_after_compaction = first_read_exact
        && check_scan(&reopened, &phase0_config, seed, 0, config.key_count).await?;
    let full_verify = phase(
        "full-verify",
        config.key_count,
        verify_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_verify),
    );

    let before_final_close = counters.snapshot();
    let final_close_started = Instant::now();
    reopened
        .close()
        .await
        .map_err(|error| format!("close compacted DB seed {seed}: {error}"))?;
    let final_close = phase(
        "final-close",
        1,
        final_close_started.elapsed().as_secs_f64(),
        counters.snapshot().difference(&before_final_close),
    );
    let total_io = counters.snapshot().difference(&IoSnapshot::default());
    let maintenance_write_amplification =
        write_amplification(maintenance_io.written_byte_total(), config.logical_bytes);

    Ok(Phase0CompactionSeedReport {
        seed,
        initial_l0_ssts,
        final_l0_ssts,
        final_sorted_runs,
        coordinator_embedded_worker: false,
        worker_sst_block_size_bytes: SstBlockSize::Block64Kib.as_bytes() as u64,
        worker_min_filter_keys: 1,
        roles_completed_cleanly,
        exact_dataset_after_compaction,
        total_io,
        initial_open,
        ingest_and_flush,
        close_before_maintenance,
        maintenance,
        reopen_open,
        first_correct_read,
        full_verify,
        final_close,
        reopen_first_correct_read_seconds,
        maintenance_write_amplification,
    })
}

fn required_env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("required environment variable {name} is not set"))
}

fn sanitize_object_namespace(namespace: &str) -> Result<String, String> {
    let sanitized: String = namespace
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect();
    if sanitized.is_empty() || sanitized.len() > 192 {
        return Err("object namespace must contain 1 to 192 characters".to_owned());
    }
    Ok(sanitized)
}

#[allow(clippy::cast_precision_loss)]
fn write_amplification(written_bytes: u64, logical_bytes: u64) -> f64 {
    written_bytes as f64 / logical_bytes as f64
}

fn validate_compaction_config(config: &Phase0CompactionConfig) -> Result<(), String> {
    if config.logical_bytes < config.key_count {
        return Err("compaction logical_bytes must be at least key_count".to_owned());
    }
    if config.flush_count < 4 {
        return Err("compaction flush_count must be at least four".to_owned());
    }
    if config.key_count < config.flush_count || config.key_count % config.flush_count != 0 {
        return Err("compaction key_count must be a multiple of flush_count".to_owned());
    }
    if config.seeds.is_empty() {
        return Err("compaction contract requires at least one seed".to_owned());
    }
    if config.timeout_millis == 0 {
        return Err("compaction timeout_millis must be greater than zero".to_owned());
    }
    Ok(())
}

fn physical_gates(
    profile: Phase0PhysicalProfile,
    mode: Phase0Mode,
    reports: &[Phase0SeedReport],
) -> Vec<Phase0Gate> {
    if profile != Phase0PhysicalProfile::ObjectKvServingV1 || mode != Phase0Mode::Correct {
        return Vec::new();
    }
    let reopen_open_read_bytes = reports
        .iter()
        .map(|report| report.reopen_open.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    let first_point_requests = reports
        .iter()
        .map(|report| report.first_correct_read.io.request_total())
        .max()
        .unwrap_or(u64::MAX);
    let first_point_read_bytes = reports
        .iter()
        .map(|report| report.first_correct_read.io.read_byte_total())
        .max()
        .unwrap_or(u64::MAX);
    vec![
        gate(
            "metadata_bounded_reopen",
            reopen_open_read_bytes <= 1_048_576,
            &format!("fresh-instance open read {reopen_open_read_bytes} bytes; ceiling is 1048576"),
        ),
        gate(
            "first_cold_point_request_budget",
            first_point_requests <= 8,
            &format!("first correct point read used {first_point_requests} requests; ceiling is 8"),
        ),
        gate(
            "first_cold_point_byte_budget",
            first_point_read_bytes <= 524_288,
            &format!(
                "first correct point read fetched {first_point_read_bytes} bytes; ceiling is 524288"
            ),
        ),
    ]
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
    let before_initial_open = counters.snapshot();
    let initial_open_started = Instant::now();
    let db = config
        .physical_profile
        .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
        .build()
        .await
        .map_err(|error| format!("open SlateDB seed {seed}: {error}"))?;
    let initial_open_elapsed = initial_open_started.elapsed().as_secs_f64();
    let initial_open_io = counters.snapshot().difference(&before_initial_open);

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
    let before_post_flush_verify = counters.snapshot();
    let post_flush_verify_started = Instant::now();
    let exact_dataset_after_flush = check_points(&db, config, seed, &sample_ordinals).await?;
    let post_flush_verify_elapsed = post_flush_verify_started.elapsed().as_secs_f64();
    let post_flush_verify_io = counters.snapshot().difference(&before_post_flush_verify);

    let before_warm_cache_prime = counters.snapshot();
    let warm_cache_prime_started = Instant::now();
    check_points(&db, config, seed, &sample_ordinals).await?;
    let warm_cache_prime_elapsed = warm_cache_prime_started.elapsed().as_secs_f64();
    let warm_cache_prime_io = counters.snapshot().difference(&before_warm_cache_prime);
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
    let before_close = counters.snapshot();
    let close_started = Instant::now();
    let active_db = if mode == Phase0Mode::Correct {
        db.close()
            .await
            .map_err(|error| format!("close SlateDB seed {seed}: {error}"))?;
        let close_elapsed = close_started.elapsed().as_secs_f64();
        let close_io = counters.snapshot().difference(&before_close);

        let before_open = counters.snapshot();
        let open_started = Instant::now();
        let reopened = config
            .physical_profile
            .configure(Db::builder(db_path.as_str(), Arc::clone(&store)), seed)
            .build()
            .await
            .map_err(|error| format!("reopen SlateDB seed {seed}: {error}"))?;
        let open_elapsed = open_started.elapsed().as_secs_f64();
        let open_io = counters.snapshot().difference(&before_open);
        (
            reopened,
            phase("close-before-reopen", 1, close_elapsed, close_io),
            phase("reopen-open", 1, open_elapsed, open_io),
        )
    } else {
        (
            db,
            phase("close-before-reopen", 0, 0.0, Phase0IoDelta::default()),
            phase("reopen-open", 0, 0.0, Phase0IoDelta::default()),
        )
    };
    let (active_db, close_before_reopen, reopen_open) = active_db;

    let before_first_read = counters.snapshot();
    let first_read_started = Instant::now();
    let first_observed = active_db
        .get(&first_key)
        .await
        .map_err(|error| format!("first reopened read seed {seed}: {error}"))?;
    let empty_cache_reopen_exact = first_observed.as_deref() == Some(first_value.as_slice());
    let first_read_elapsed = first_read_started.elapsed().as_secs_f64();
    let first_read_io = counters.snapshot().difference(&before_first_read);
    let reopen_first_correct_read_seconds = reopen_open.elapsed_seconds + first_read_elapsed;

    let cold_ordinals = &sample_ordinals[1..];
    let before_cold = counters.snapshot();
    let cold_started = Instant::now();
    let cold_point_reads_exact = check_points(&active_db, config, seed, cold_ordinals).await?;
    let cold_elapsed = cold_started.elapsed().as_secs_f64();
    let cold_io = counters.snapshot().difference(&before_cold);
    let before_final_close = counters.snapshot();
    let final_close_started = Instant::now();
    active_db
        .close()
        .await
        .map_err(|error| format!("close reopened SlateDB seed {seed}: {error}"))?;
    let final_close_elapsed = final_close_started.elapsed().as_secs_f64();
    let final_close_io = counters.snapshot().difference(&before_final_close);
    let total_io = counters.snapshot().difference(&IoSnapshot::default());

    let read_io_accounted = (reopen_open.io.read_byte_total()
        + first_read_io.read_byte_total()
        + cold_io.read_byte_total())
        > 0;
    let object_io_accounted = ingest_io.request_total() > 0
        && ingest_io.written_byte_total() > 0
        && (read_io_accounted || mode == Phase0Mode::ReuseWarmDbForReopen);
    Ok(SeedOutcome {
        report: Phase0SeedReport {
            seed,
            total_io,
            initial_open: phase("initial-open", 1, initial_open_elapsed, initial_open_io),
            ingest: phase("ingest", config.key_count, ingest_elapsed, ingest_io),
            post_flush_verify: phase(
                "post-flush-verify",
                sample_ordinals.len() as u64,
                post_flush_verify_elapsed,
                post_flush_verify_io,
            ),
            warm_cache_prime: phase(
                "warm-cache-prime",
                sample_ordinals.len() as u64,
                warm_cache_prime_elapsed,
                warm_cache_prime_io,
            ),
            warm_point: phase(
                "warm-point",
                sample_ordinals.len() as u64,
                warm_elapsed,
                warm_io,
            ),
            ordered_scan: phase("ordered-scan", scan_count, scan_elapsed, scan_io),
            reopen_first_correct_read_seconds,
            close_before_reopen,
            reopen_open,
            first_correct_read: phase("first-correct-read", 1, first_read_elapsed, first_read_io),
            cold_point: phase(
                "cold-point",
                cold_ordinals.len() as u64,
                cold_elapsed,
                cold_io,
            ),
            final_close: phase("final-close", 1, final_close_elapsed, final_close_io),
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

/// Process-local object-store counters used by physical evaluation workers.
#[derive(Debug, Default)]
pub struct IoCounters {
    snapshot: Mutex<IoSnapshot>,
}

impl IoCounters {
    #[must_use]
    pub fn total(&self) -> Phase0IoDelta {
        self.snapshot().difference(&IoSnapshot::default())
    }

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

/// Transparent object-store wrapper that accounts for requests and bytes.
pub struct CountingStore<T> {
    inner: Arc<T>,
    counters: Arc<IoCounters>,
}

impl<T> CountingStore<T> {
    #[must_use]
    pub fn new(inner: T, counters: Arc<IoCounters>) -> Self {
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
    use super::{
        run_phase0_compaction_contract, run_phase0_filesystem_contract, Phase0CompactionConfig,
        Phase0CompactionMode, Phase0Config, Phase0Mode, Phase0PhysicalProfile,
    };

    fn config() -> Phase0Config {
        Phase0Config {
            logical_bytes: 65_536,
            key_count: 64,
            point_reads_per_seed: 8,
            scan_rows_per_seed: 10,
            seeds: vec![1103],
            physical_profile: Phase0PhysicalProfile::SlateDbDefaultV1,
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
        assert!(report.seeds[0].reopen_open.io.read_byte_total() > 0);
        let expected_reopen = report.seeds[0].reopen_open.elapsed_seconds
            + report.seeds[0].first_correct_read.elapsed_seconds;
        assert!(
            (report.seeds[0].reopen_first_correct_read_seconds - expected_reopen).abs()
                < f64::EPSILON
        );
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

    #[tokio::test]
    async fn objectkv_serving_profile_meets_reopen_and_point_budgets() {
        let mut config = config();
        config.physical_profile = Phase0PhysicalProfile::ObjectKvServingV1;
        let report = run_phase0_filesystem_contract(&config, Phase0Mode::Correct)
            .await
            .expect("run tuned contract");
        assert!(report.passed(), "failed gates: {:?}", report.gates);
        assert_eq!(report.physical.id, "objectkv-serving-v1");
        assert!(!report.physical.object_wal_enabled);
        assert!(!report.physical.embedded_compactor);
        assert!(!report.physical.embedded_garbage_collector);
    }

    #[tokio::test]
    async fn separate_compaction_roles_preserve_exact_serving_reads() {
        let report = run_phase0_compaction_contract(
            &Phase0CompactionConfig {
                logical_bytes: 262_144,
                key_count: 256,
                flush_count: 4,
                seeds: vec![1103],
                timeout_millis: 10_000,
            },
            Phase0CompactionMode::Correct,
        )
        .await
        .expect("run separate compaction contract");
        assert!(report.passed(), "failed gates: {:?}", report.gates);
        assert_eq!(report.seeds[0].worker_sst_block_size_bytes, 65_536);
        assert!(!report.seeds[0].coordinator_embedded_worker);
    }

    #[tokio::test]
    async fn missing_external_worker_is_detected() {
        let report = run_phase0_compaction_contract(
            &Phase0CompactionConfig {
                logical_bytes: 262_144,
                key_count: 256,
                flush_count: 4,
                seeds: vec![1103],
                timeout_millis: 100,
            },
            Phase0CompactionMode::SkipExternalWorker,
        )
        .await
        .expect("run missing-worker control");
        let failed: Vec<&str> = report
            .gates
            .iter()
            .filter(|gate| !gate.passed)
            .map(|gate| gate.id.as_str())
            .collect();
        assert!(failed.contains(&"separate_compaction_roles_completed"));
        assert!(failed.contains(&"external_compaction_reduced_l0"));
        assert!(failed.contains(&"external_compaction_created_sorted_run"));
        assert!(failed.contains(&"maintenance_object_io_accounted"));
        assert!(report.seeds[0].exact_dataset_after_compaction);
    }
}
