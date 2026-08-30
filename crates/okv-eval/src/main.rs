use chrono::Utc;
use clap::{Parser, Subcommand};
use nix::fcntl::{Flock, FlockArg};
use okv_consensus::{
    run_generation_process_contract, run_process_node, run_publication_process_contract,
    run_raft_cluster_contract, run_raft_process_contract, run_raft_storage_contract,
    run_transaction_machine_contract, run_transaction_process_contract, GenerationProcessMode,
    ProcessNodeConfig, ProcessReadOperation, ProcessTransactionResult, PublicationProcessMode,
    RaftClusterMode, RaftProcessMode, RaftStorageMode, TransactionMachineConfig,
    TransactionMutation, TransactionProcessMode, TransactionProcessReport,
};
use okv_eval::authority_state_scale::{
    run_authority_state_scale_contract, AuthorityStateScaleMode, AuthorityStateScaleProfile,
    AuthorityStateScaleReport,
};
use okv_eval::cold_read::{
    run_cold_read_profile, run_empty_worker_profile, ColdReadMode, ColdReadProfile, ColdReadReport,
    EmptyWorkerMode, EmptyWorkerReport,
};
use okv_eval::commit_group::{
    run_commit_group_contract, CommitGroupMode, CommitGroupProfile, CommitGroupReport,
};
use okv_eval::commit_proxy::{
    run_commit_proxy_contract, CommitProxyMode, CommitProxyProfile, CommitProxyReport,
};
use okv_eval::commit_proxy_object_frontier::{
    run_commit_proxy_object_frontier_contract, CommitProxyObjectFrontierMode,
    CommitProxyObjectFrontierProfile, CommitProxyObjectFrontierReport,
};
use okv_eval::comparison::{compare_results, validate_comparison_receipt};
use okv_eval::config::{
    contract_hash, load_suite, validate_workload_selection, BudgetKind, DatasetConfig, LoadedSuite,
    MetricRegistry, ProfileConfig, TelemetryConfig, WorkloadConfig,
};
use okv_eval::fixture_anchor::{
    run_fixture_anchor_contract, FixtureAnchorMode, FixtureAnchorReport,
};
use okv_eval::frontiered_process_snapshot::{
    run_frontiered_process_snapshot_contract, FrontieredProcessSnapshotMode,
    FrontieredProcessSnapshotProfile, FrontieredProcessSnapshotReport,
};
use okv_eval::object_fixture::{
    decode_fixture_placement_locator, open_generation_pinned_fixture_lazy,
    prepare_fixture_placement, run_object_fixture_contract, FixturePlacementBuildEnvelopeV1,
    ObjectFixtureMode, ObjectFixtureProfile, ObjectFixtureReport,
};
use okv_eval::object_frontier::{
    run_object_frontier_contract, ObjectFrontierMode, ObjectFrontierReport,
};
use okv_eval::process_snapshot_compaction::{
    run_process_snapshot_compaction_contract, ProcessSnapshotCompactionMode,
    ProcessSnapshotCompactionProfile,
};
use okv_eval::program::{load_program, plan_program};
use okv_eval::provider_attempt::ProviderAttemptBackend;
#[cfg(feature = "resident-rocksdb")]
use okv_eval::resident::{run_resident_profile, ResidentMode, ResidentProfile, ResidentReport};
use okv_eval::result::{
    median, median_absolute_deviation, statistic_value, validate_result, BudgetResult, EvalResult,
    GateStatus, HardGateResult, PrimaryMetricResult, ProfileIdentity, Verdict,
};
use okv_eval::serving_recovery::{
    run_serving_recovery_contract, run_serving_recovery_node, ServingRecoveryMode,
    ServingRecoveryProcessConfig, ServingRecoveryProfile, ServingRecoveryReport,
};
use okv_eval::serving_recovery_openraft::{
    run_openraft_serving_recovery_contract,
    run_openraft_serving_recovery_contract_from_fixture_placement,
    run_openraft_serving_recovery_contract_from_object_fixture,
    run_openraft_serving_recovery_contract_from_object_fixture_locator,
    run_openraft_serving_recovery_contract_with_hot_reads, run_openraft_serving_recovery_node,
    OpenRaftHotReadAccessPattern, OpenRaftHotReadNegativeControl, OpenRaftHotReadProfile,
    OpenRaftHotReadReport, OpenRaftHotReadSampleReport, OpenRaftHotReadStorageReport,
    OpenRaftHotReadSubject, OpenRaftServingObjectBackend, OpenRaftServingProcessConfig,
    OpenRaftServingRecoveryMode, OpenRaftServingRecoveryReport, NATIVE_RESIDENT_PROVIDER,
};
use okv_eval::staged_txlog_process::{
    run_staged_txlog_node, run_staged_txlog_process_contract, StagedTxLogNodeConfig,
    StagedTxLogProcessMode,
};
use okv_eval::storage_layout::{
    run_columnar_cache_admission_contract, run_columnar_cache_admission_contract_on_backend,
    run_columnar_datafusion_contract_with_scan_fetch, run_storage_layout_contract,
    run_storage_layout_pair_contract, run_storage_layout_pair_contract_on_backend,
    ColumnarCacheAdmissionMode, ColumnarCacheAdmissionReport, ColumnarDataFusionMode,
    ColumnarDataFusionReport, StorageLayoutMode, StorageLayoutProfile, StorageLayoutReport,
};
use okv_eval::t27_plan::{
    build_t27_execution_incarnation, build_t27_execution_plan, decode_t27_execution_plan,
    derive_t27_expected_identity, verify_t27_plan_poison, verify_t27_position_poison,
    T27AccessPatternV1, T27AggregateRunReceiptV1, T27ExecutionEnvelopeV1, T27PlanPoisonV1,
    T27PlanProfileV1, T27PlanRunReceiptV1, T27PlanSubjectV1, T27PositionObservationV1,
    T27PositionPoisonV1, T27PositionReceiptV1, T27StratumEvidenceV1, T27StratumRunReceiptV1,
};
use okv_eval::t28_cold_point::{
    T28CacheState, T28PointExecutionReceiptV1, T28PointPlanV2, T28PointSubject,
    T28ReaderPlanIdentityV1,
};
use okv_eval::t28_iam::T28ReaderIamReceiptV1;
use okv_eval::telemetry::{MetricRecorder, RunResource, Telemetry, TelemetryFlushReport};
use okv_eval::transaction_batch::{
    run_transaction_batch_contract, TransactionBatchMode, TransactionBatchProfile,
    TransactionBatchReport,
};
use okv_history_oracle::{
    check_history, KeyRange as OracleKeyRange, KeyValue as OracleKeyValue,
    ObservedValue as OracleObservedValue, ReadOperation as OracleReadOperation,
    TransactionHistoryV1, TransactionOutcome as OracleTransactionOutcome,
    TransactionRecord as OracleTransactionRecord, HISTORY_SCHEMA_VERSION,
};
use okv_htap::{
    run_physical_overlay_contract, run_streaming_overlay_contract, PhysicalOverlayMode,
    StreamingOverlayMode,
};
use okv_model::{
    run_differential_history, run_htap_contract, run_publication_gc_contract, ApplyOutcome,
    CommitBatch, CommitIdentity, DifferentialMode, HtapContractMode, Model, Mutation,
    PublicationGcMode, Version,
};
use okv_object::{
    filesystem_backend, gcs_backend_from_env, gcs_backend_from_env_no_retries, memory_backend,
    minio_backend_from_env, prefixed_backend, read_planned_point, run_conformance,
    run_publication_adapter_contract, run_publication_publisher_manifest_recovery_contract,
    run_publication_publisher_manifest_recovery_node, run_publication_publisher_process_contract,
    run_publication_publisher_process_node, run_publication_publisher_publish_recovery_contract,
    run_publication_publisher_publish_recovery_node,
    run_publication_publisher_put_recovery_contract, run_publication_publisher_put_recovery_node,
    validate_conformance_report, CaseStatus, ConformanceOptions, ConformanceProfile,
    PublicationAdapterMode, PublisherManifestRecoveryMode, PublisherManifestRecoveryProcessConfig,
    PublisherProcessConfig, PublisherProcessMode, PublisherPublishRecoveryMode,
    PublisherPublishRecoveryProcessConfig, PublisherPutRecoveryMode,
    PublisherPutRecoveryProcessConfig,
};
use okv_sim::{
    run_commit_contract, run_generation_fencing, run_persisted_wal_contract,
    run_serializability_history, run_staged_txlog_contract, CommitContractMode, PersistedWalMode,
    SerializabilityMode, StagedTxLogMode,
};
use okv_slate::{
    run_phase0_filesystem_contract, Phase0Config, Phase0IoDelta, Phase0Mode, Phase0PhaseReport,
    Phase0Report,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, info_span};
use uuid::Uuid;

struct Measurement {
    metric: &'static str,
    value: f64,
    attributes: BTreeMap<String, String>,
}

struct WorkloadExecution {
    error: Option<String>,
    measurements: Vec<Measurement>,
    hard_gates: Vec<HardGateResult>,
    budget_units: f64,
    artifact_refs: Vec<String>,
    secondary_metrics: BTreeMap<String, f64>,
}

struct TransactionContractRun {
    process: TransactionProcessReport,
    topology_sha256: Option<String>,
    machine_report: Option<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct T27PositionExecutionOutputV1 {
    receipt: T27PositionReceiptV1,
    raw_report: OpenRaftServingRecoveryReport,
}

struct T27HostLease {
    file: Flock<File>,
    lease_id: String,
    acquired_at: String,
    path: PathBuf,
}

struct T27ControllerRunOutcome {
    plan: okv_eval::t27_plan::T27ExecutionPlanV1,
    receipts: Vec<T27PositionReceiptV1>,
    started_at: String,
    finished_at: String,
    lease_id: String,
    lease_acquired_at: String,
    lease_released_at: String,
    telemetry_endpoint_sha256: String,
    telemetry_flush: TelemetryFlushReport,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct T27AggregateManifestV1 {
    schema_version: u32,
    bundles: Vec<T27AggregateBundleV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct T27AggregateBundleV1 {
    plan: PathBuf,
    expected_plan_sha256: String,
    stratum_receipt: PathBuf,
    position_receipts: Vec<PathBuf>,
}

struct T27LoadedAggregateBundleV1 {
    plan: okv_eval::t27_plan::T27ExecutionPlanV1,
    receipt: T27StratumRunReceiptV1,
    positions: Vec<T27PositionReceiptV1>,
}

impl WorkloadExecution {
    fn passed(&self) -> bool {
        self.error.is_none()
    }
}

#[derive(Debug, Parser)]
#[command(name = "okv-eval", version, about = "objectKV evaluation runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run the in-memory model smoke check without suite orchestration.
    Smoke,
    /// Validate a suite, metric registry, and result schema as one contract.
    ValidateSuite {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
    },
    /// Validate a product-level graph of suites, controls, and requirement claims.
    ValidateProgram {
        #[arg(default_value = "evals/programs/objectkv-golden-path-v1.toml")]
        program: PathBuf,
    },
    /// Compare one result to the exact control declared by a program gate.
    CompareResults {
        #[arg(default_value = "evals/programs/objectkv-product-thesis-v1.toml")]
        program: PathBuf,
        #[arg(long)]
        gate: String,
        #[arg(long)]
        candidate: PathBuf,
        #[arg(long)]
        control: PathBuf,
        #[arg(long, default_value = "evals/schema/result.schema.json")]
        result_schema: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Resolve a product-level evaluation program without running its gates.
    PlanProgram {
        #[arg(default_value = "evals/programs/objectkv-golden-path-v1.toml")]
        program: PathBuf,
    },
    /// List the metrics exposed by a validated suite.
    ListMetrics {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
    },
    /// Print the deterministic execution plan without running a workload.
    Plan {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
        #[arg(long, default_value = "dev")]
        profile: String,
    },
    /// Run one registered workload and emit a schema-validated result.
    Run {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
        #[arg(long, default_value = "dev")]
        profile: String,
        #[arg(long, default_value = "model-smoke")]
        workload: String,
        #[arg(long, default_value = "model")]
        backend: String,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Pair candidate and control runs in one alternating benchmark batch.
        #[arg(long)]
        batch_id: Option<String>,
        /// Permit a diagnostic run whose source tree is not reproducible.
        #[arg(long)]
        allow_dirty: bool,
    },
    /// Emit one canonical real-process trace without suite orchestration.
    RaftProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical cell-generation takeover trace without suite orchestration.
    GenerationProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit the combined generation, routing, and publication incarnation trace.
    ProviderIncarnationTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical publication-authority process trace without suite orchestration.
    PublicationProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical publisher prepare/restart trace without suite orchestration.
    PublicationPublisherProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical publisher ambiguous-PUT recovery trace.
    PublicationPublisherPutRecoveryTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical publisher ambiguous-manifest recovery trace.
    PublicationPublisherManifestRecoveryTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical publisher lost-Publish-response recovery trace.
    PublicationPublisherPublishRecoveryTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical replicated transaction history trace.
    TransactionProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit the same replicated transaction trace on externally managed machines.
    TransactionMachineTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
        #[arg(long)]
        config: PathBuf,
    },
    /// Emit one replacement `ServingWorker` recovery report without suite orchestration.
    ServingRecoveryProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 1_024)]
        key_count: u64,
        #[arg(long, default_value_t = 1_016)]
        value_bytes: usize,
        #[arg(long, default_value_t = 262_144)]
        row_object_target_bytes: usize,
        #[arg(long, default_value_t = 32_768)]
        row_object_block_bytes: usize,
    },
    /// Emit one `OpenRaft` retained-stream replacement-worker report.
    ServingRecoveryOpenRaftTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 128)]
        key_count: u64,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 32_768)]
        row_object_target_bytes: usize,
        #[arg(long, default_value_t = 4_096)]
        row_object_block_bytes: usize,
    },
    /// Emit one authenticated object-frontier and physical txLog-pop report.
    ObjectFrontierTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
    },
    /// Emit one bounded concurrent commit-path report.
    CommitGroupTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 512)]
        transaction_count: u64,
        #[arg(long, default_value_t = 32)]
        max_in_flight: usize,
    },
    /// Emit one explicit transaction-batch entry report.
    TransactionBatchTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value_t = 512)]
        transaction_count: u64,
        #[arg(long, default_value_t = 16)]
        transactions_per_batch: usize,
    },
    /// Emit one independent-request commit-proxy batching report.
    CommitProxyTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "saturated_candidate")]
        mode: String,
        #[arg(long, default_value_t = 1_024)]
        transaction_count: u64,
        #[arg(long, default_value_t = 64)]
        concurrent_clients: usize,
        #[arg(long, default_value_t = 16)]
        max_batch_items: usize,
    },
    /// Emit one concurrent commit-proxy and authenticated object-frontier report.
    CommitProxyObjectFrontierTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "quarter_conflict_candidate")]
        mode: String,
        #[arg(long, default_value_t = 512)]
        prefix_transaction_count: u64,
        #[arg(long, default_value_t = 1_024)]
        suffix_transaction_count: u64,
    },
    /// Emit one durable process-snapshot and physical journal-compaction report.
    ProcessSnapshotCompactionTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 1_024)]
        transaction_count: u64,
        #[arg(long, default_value_t = 32)]
        transactions_per_batch: usize,
    },
    /// Emit the RFC-0044 canonical empty-anchor determinism report.
    FixtureAnchorTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 20)]
        fresh_authorities: usize,
    },
    /// Emit the RFC-0044 content-addressed fixture and resident identity report.
    ObjectFixtureTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "candidate")]
        mode: String,
        #[arg(long, default_value_t = 4_096)]
        key_count: u64,
        #[arg(long, default_value_t = 1_024)]
        value_bytes: usize,
        #[arg(long, default_value_t = 1_048_576)]
        target_object_bytes: usize,
        #[arg(long, default_value_t = 65_536)]
        target_block_bytes: usize,
    },
    /// Prepare one immutable, generation-pinned GCS fixture in its own invocation.
    ObjectFixturePrepareGcs {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value_t = 1)]
        base_version: u64,
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        source_sha256: String,
        #[arg(long)]
        suite_sha256: String,
        #[arg(long)]
        binary_sha256: String,
        #[arg(long)]
        cargo_lock_sha256: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 1_048_576)]
        key_count: u64,
        #[arg(long, default_value_t = 1_024)]
        value_bytes: usize,
        #[arg(long, default_value_t = 8_388_608)]
        target_object_bytes: usize,
        #[arg(long, default_value_t = 65_536)]
        target_block_bytes: usize,
    },
    /// Consume one required-existing GCS fixture from a fresh process tree.
    ObjectFixtureConsumeGcs {
        #[arg(long)]
        trace_seed: u64,
        #[arg(long)]
        locator: PathBuf,
        #[arg(long)]
        expected_envelope_sha256: String,
        #[arg(long, default_value = "native_snapshot")]
        subject: String,
        #[arg(long, default_value_t = 256)]
        warmup_operations: usize,
        #[arg(long, default_value_t = 1_024)]
        measured_operations: usize,
        #[arg(long, default_value_t = 1)]
        concurrent_clients: usize,
        #[arg(long, default_value_t = 134_217_728)]
        max_local_bytes: u64,
        #[arg(long, default_value_t = 16_777_216)]
        block_cache_bytes: u64,
        #[arg(long, default_value_t = false)]
        direct_reads: bool,
    },
    /// Seal T28 point ranges from one generation-pinned GCS fixture.
    T28PlanBuildGcs {
        #[arg(long)]
        locator: PathBuf,
        #[arg(long)]
        expected_envelope_sha256: String,
        #[arg(long)]
        iam_receipt: PathBuf,
        #[arg(long)]
        expected_iam_receipt_sha256: String,
        #[arg(long, default_value = "metadata_warm_data_cold")]
        cache_state: String,
        #[arg(long = "key-id", value_delimiter = ',', num_args = 1..)]
        key_ids: Vec<u64>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Execute one sealed T28 data range and emit its provider trace.
    T28PointReadGcs {
        #[arg(long)]
        locator: PathBuf,
        #[arg(long)]
        expected_envelope_sha256: String,
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        iam_receipt: PathBuf,
        #[arg(long)]
        expected_iam_receipt_sha256: String,
        #[arg(long, default_value_t = 0)]
        ordinal: u64,
        #[arg(long, default_value = "candidate")]
        subject: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Build one immutable T27 ABBA plan from a verified fixture placement.
    T27PlanBuild {
        #[arg(long)]
        locator: PathBuf,
        #[arg(long)]
        expected_envelope_sha256: String,
        #[arg(long, default_value = "preflight_64_mib")]
        profile: String,
        #[arg(long)]
        runtime_source_sha256: String,
        #[arg(long)]
        runtime_cargo_lock: PathBuf,
        #[arg(long)]
        machine_receipt: PathBuf,
        #[arg(long)]
        scratch_root: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Bind one authenticated T27 workload plan to replacement infrastructure.
    T27PlanIncarnateGcs {
        #[arg(long)]
        source_plan: PathBuf,
        #[arg(long)]
        expected_source_plan_sha256: String,
        #[arg(long)]
        runtime_executable: PathBuf,
        #[arg(long)]
        runtime_cargo_lock: PathBuf,
        #[arg(long)]
        machine_receipt: PathBuf,
        #[arg(long)]
        scratch_root: PathBuf,
        #[arg(long)]
        plan_output: PathBuf,
        #[arg(long)]
        receipt_output: PathBuf,
    },
    /// Build and verify one sealed corruption of an authenticated T27 plan.
    T27PlanPoisonCheck {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        poison: String,
        #[arg(long)]
        poisoned_plan_output: PathBuf,
        #[arg(long)]
        receipt_output: PathBuf,
    },
    /// Build and verify one sealed corruption of a T27 position receipt.
    T27PositionPoisonCheck {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        position_receipt: PathBuf,
        #[arg(long)]
        poison: String,
        #[arg(long)]
        poisoned_receipt_output: PathBuf,
        #[arg(long)]
        receipt_output: PathBuf,
    },
    /// Execute every T27 position sequentially in a fresh process.
    T27PlanRunGcs {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        runtime_cargo_lock: PathBuf,
        #[arg(long)]
        machine_receipt: PathBuf,
        #[arg(long)]
        scratch_root: PathBuf,
        #[arg(long)]
        output_dir: PathBuf,
    },
    /// Execute one complete T27 stratum as an authenticated resumable unit.
    T27StratumRunGcs {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        stratum_id: String,
        #[arg(long)]
        runtime_cargo_lock: PathBuf,
        #[arg(long)]
        machine_receipt: PathBuf,
        #[arg(long)]
        scratch_root: PathBuf,
        #[arg(long)]
        output_dir: PathBuf,
    },
    /// Validate every sealed T27 stratum and emit the final aggregate receipt.
    T27AggregateRun {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Execute one T27 position. Only the plan controller should invoke this.
    #[command(hide = true)]
    T27PlanPositionGcs {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        expected_plan_sha256: String,
        #[arg(long)]
        ordinal: u64,
        #[arg(long)]
        controller_id: String,
        #[arg(long)]
        worker_id: String,
        #[arg(long)]
        runtime_cargo_lock: PathBuf,
        #[arg(long)]
        machine_receipt: PathBuf,
        #[arg(long)]
        scratch_root: PathBuf,
        #[arg(long)]
        lease_id: String,
    },
    /// Build one actual resident process from the RFC-0044 object fixture.
    ObjectFixtureResidentTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "native_snapshot")]
        subject: String,
        #[arg(long, default_value_t = false)]
        regenerate_control_poison: bool,
        #[arg(long, default_value_t = 4_096)]
        key_count: u64,
        #[arg(long, default_value_t = 1_024)]
        value_bytes: usize,
        #[arg(long, default_value_t = 1_048_576)]
        target_object_bytes: usize,
        #[arg(long, default_value_t = 65_536)]
        target_block_bytes: usize,
    },
    /// Emit one four-cycle frontiered process-snapshot report.
    FrontieredProcessSnapshotTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "aligned_r_q_o_candidate")]
        mode: String,
    },
    /// Emit one same-history immutable storage-layout diagnostic report.
    StorageLayoutTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "indexed_row_object_control")]
        mode: String,
        #[arg(long, default_value_t = 1_024)]
        key_count: u64,
        #[arg(long, default_value_t = 256)]
        point_operations: usize,
    },
    /// Internal entrypoint used by the real-process consensus controller.
    #[command(hide = true)]
    ConsensusNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the dedicated publisher controller.
    #[command(hide = true)]
    PublisherNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the ambiguous-PUT publisher controller.
    #[command(hide = true)]
    PublisherPutNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the ambiguous-manifest publisher controller.
    #[command(hide = true)]
    PublisherManifestNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the lost-Publish-response controller.
    #[command(hide = true)]
    PublisherPublishNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the replacement `ServingWorker` controller.
    #[command(hide = true)]
    ServingRecoveryNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the G4.4 replacement-worker controller.
    #[command(hide = true)]
    ServingRecoveryOpenRaftNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the L1 staged txLog controller.
    #[command(name = "staged-txlog-node", hide = true)]
    StagedTxLogNode {
        #[arg(long)]
        config_json: String,
    },
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("okv-eval: {error}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
fn execute(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Commands::Smoke => {
            run_model_smoke()?;
            println!("{{\"schema_version\":1,\"suite\":\"smoke\",\"status\":\"pass\",\"correctness_failures\":0}}");
        }
        Commands::ValidateSuite { suite } => {
            let loaded = load_suite(&suite)?;
            println!(
                "validated suite {} with {} metrics, {} lanes, {} workloads, and {} profiles",
                loaded.suite.id,
                loaded.registry.metrics.len(),
                loaded.suite.lanes.len(),
                loaded.suite.workloads.len(),
                loaded.suite.profiles.len()
            );
        }
        Commands::ValidateProgram { program } => {
            let loaded = load_program(&program)?;
            let gate_count: usize = loaded
                .program
                .phases
                .iter()
                .map(|phase| phase.gates.len())
                .sum();
            println!(
                "validated program {} with {} phases and {} gates",
                loaded.program.id,
                loaded.program.phases.len(),
                gate_count
            );
        }
        Commands::PlanProgram { program } => {
            let loaded = load_program(&program)?;
            println!("{}", serde_json::to_string_pretty(&plan_program(&loaded)?)?);
        }
        Commands::CompareResults {
            program,
            gate,
            candidate,
            control,
            result_schema,
            output,
        } => {
            let loaded = load_program(&program)?;
            let receipt = compare_results(&loaded, &gate, &candidate, &control, &result_schema)?;
            let value = serde_json::to_value(&receipt)?;
            validate_comparison_receipt(&loaded.comparison_schema_path, &value)?;
            let bytes = serde_json::to_vec_pretty(&value)?;
            if let Some(path) = output {
                fs::write(path, &bytes)?;
            }
            println!("{}", String::from_utf8(bytes)?);
        }
        Commands::ListMetrics { suite } => list_metrics(&load_suite(&suite)?),
        Commands::Plan { suite, profile } => print_plan(&load_suite(&suite)?, &profile)?,
        Commands::Run {
            suite,
            profile,
            workload,
            backend,
            output,
            batch_id,
            allow_dirty,
        } => run_suite(
            &suite,
            &profile,
            &workload,
            &backend,
            output.as_deref(),
            batch_id.as_deref(),
            allow_dirty,
        )?,
        Commands::RaftProcessTrace { seed, mode } => {
            let mode = parse_raft_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_raft_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::GenerationProcessTrace { seed, mode } => {
            let mode = parse_generation_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_generation_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ProviderIncarnationTrace { seed, mode } => {
            let mode = match mode.as_str() {
                "correct" | "none" => {
                    okv_eval::provider_incarnation::ProviderIncarnationMode::Correct
                }
                "accept_stale_source_incarnation" => okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation,
                other => return Err(std::io::Error::other(format!("unknown provider incarnation mode {other}")).into()),
            };
            let executable = std::env::current_exe()?;
            let report = okv_eval::provider_incarnation::run_provider_incarnation_contract(
                seed,
                mode,
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationProcessTrace { seed, mode } => {
            let mode = parse_publication_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_publication_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationPublisherProcessTrace { seed, mode } => {
            let mode = parse_publisher_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_publication_publisher_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationPublisherPutRecoveryTrace { seed, mode } => {
            let mode = parse_publisher_put_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_publication_publisher_put_recovery_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationPublisherManifestRecoveryTrace { seed, mode } => {
            let mode =
                parse_publisher_manifest_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report =
                run_publication_publisher_manifest_recovery_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationPublisherPublishRecoveryTrace { seed, mode } => {
            let mode =
                parse_publisher_publish_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report =
                run_publication_publisher_publish_recovery_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::TransactionProcessTrace { seed, mode } => {
            let mode = parse_transaction_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_transaction_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::TransactionMachineTrace { seed, mode, config } => {
            let mode = parse_transaction_process_mode(&mode).map_err(std::io::Error::other)?;
            let config = serde_json::from_slice::<TransactionMachineConfig>(&fs::read(config)?)?;
            let report = run_transaction_machine_contract(seed, mode, config)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ServingRecoveryProcessTrace {
            seed,
            mode,
            key_count,
            value_bytes,
            row_object_target_bytes,
            row_object_block_bytes,
        } => {
            let mode = parse_serving_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_serving_recovery_contract(
                seed,
                mode,
                &ServingRecoveryProfile {
                    key_count,
                    value_bytes,
                    target_object_bytes: row_object_target_bytes,
                    target_block_bytes: row_object_block_bytes,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ServingRecoveryOpenRaftTrace {
            seed,
            mode,
            key_count,
            value_bytes,
            row_object_target_bytes,
            row_object_block_bytes,
        } => {
            let mode =
                parse_openraft_serving_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_openraft_serving_recovery_contract(
                seed,
                mode,
                &ServingRecoveryProfile {
                    key_count,
                    value_bytes,
                    target_object_bytes: row_object_target_bytes,
                    target_block_bytes: row_object_block_bytes,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ObjectFrontierTrace { seed, mode } => {
            let mode = parse_object_frontier_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_object_frontier_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CommitGroupTrace {
            seed,
            mode,
            transaction_count,
            max_in_flight,
        } => {
            let mode = parse_commit_group_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_commit_group_contract(
                seed,
                mode,
                &CommitGroupProfile {
                    live_keys: 256,
                    value_bytes: 128,
                    transaction_count,
                    candidate_max_in_flight: max_in_flight,
                    control_max_in_flight: 1,
                    candidate_min_transactions_per_second: 200,
                    candidate_min_entries_per_append: 4,
                    candidate_max_commit_p99_micros: 250_000,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::TransactionBatchTrace {
            seed,
            transaction_count,
            transactions_per_batch,
        } => {
            let executable = std::env::current_exe()?;
            let report = run_transaction_batch_contract(
                seed,
                TransactionBatchMode::Candidate,
                &TransactionBatchProfile {
                    live_keys: 256,
                    value_bytes: 128,
                    transaction_count,
                    transactions_per_batch,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CommitProxyTrace {
            seed,
            mode,
            transaction_count,
            concurrent_clients,
            max_batch_items,
        } => {
            let mode = match mode.as_str() {
                "saturated_candidate" => CommitProxyMode::SaturatedCandidate,
                "admission_knee_control" => CommitProxyMode::AdmissionKneeControl,
                "sparse_arrival_control" => CommitProxyMode::SparseArrivalControl,
                "byte_bound_control" => CommitProxyMode::ByteBoundControl,
                "overload_control" => CommitProxyMode::OverloadControl,
                "oversized_item_poison" => CommitProxyMode::OversizedItemPoison,
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown commit-proxy mode {other}"
                    ))
                    .into());
                }
            };
            let executable = std::env::current_exe()?;
            let report = run_commit_proxy_contract(
                seed,
                mode,
                &CommitProxyProfile {
                    transaction_count,
                    value_bytes: 128,
                    concurrent_clients,
                    admission_knee_clients: 32,
                    max_batch_items,
                    max_entry_bytes: 262_144,
                    max_batch_delay_micros: 2_000,
                    queue_capacity: 2_048,
                    sparse_transaction_count: 32,
                    byte_control_transaction_count: 128,
                    byte_control_value_bytes: 8_192,
                    byte_control_max_entry_bytes: 131_072,
                    overload_transaction_count: 512,
                    overload_queue_capacity: max_batch_items,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CommitProxyObjectFrontierTrace {
            seed,
            mode,
            prefix_transaction_count,
            suffix_transaction_count,
        } => {
            let mode =
                parse_commit_proxy_object_frontier_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_commit_proxy_object_frontier_contract(
                seed,
                mode,
                &CommitProxyObjectFrontierProfile {
                    prefix_transaction_count,
                    suffix_transaction_count,
                    value_bytes: 128,
                    concurrent_clients: 64,
                    max_batch_items: 32,
                    max_entry_bytes: 262_144,
                    max_batch_delay_micros: 2_000,
                    queue_capacity: 2_048,
                    hot_key_count: 64,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ProcessSnapshotCompactionTrace {
            seed,
            mode,
            transaction_count,
            transactions_per_batch,
        } => {
            let mode = match mode.as_str() {
                "candidate" => ProcessSnapshotCompactionMode::Candidate,
                "purge_before_snapshot_poison" => {
                    ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison
                }
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown process snapshot-compaction mode {other}"
                    ))
                    .into());
                }
            };
            let executable = std::env::current_exe()?;
            let report = run_process_snapshot_compaction_contract(
                seed,
                mode,
                &ProcessSnapshotCompactionProfile {
                    transaction_count,
                    transactions_per_batch,
                    live_keys: 256,
                    value_bytes: 128,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::FixtureAnchorTrace {
            seed,
            mode,
            fresh_authorities,
        } => {
            let mode = parse_fixture_anchor_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_fixture_anchor_contract(seed, mode, fresh_authorities, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ObjectFixtureTrace {
            seed,
            mode,
            key_count,
            value_bytes,
            target_object_bytes,
            target_block_bytes,
        } => {
            let mode = parse_object_fixture_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_object_fixture_contract(
                seed,
                mode,
                &ObjectFixtureProfile {
                    key_count,
                    value_bytes,
                    target_object_bytes,
                    target_block_bytes,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ObjectFixturePrepareGcs {
            seed,
            base_version,
            prefix,
            source_sha256,
            suite_sha256,
            binary_sha256,
            cargo_lock_sha256,
            output,
            key_count,
            value_bytes,
            target_object_bytes,
            target_block_bytes,
        } => {
            let bucket = std::env::var("OKV_GCS_BUCKET")?;
            let backend = prefixed_backend(gcs_backend_from_env()?, prefix.clone())?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let locator = runtime.block_on(prepare_fixture_placement(
                seed,
                &ObjectFixtureProfile {
                    key_count,
                    value_bytes,
                    target_object_bytes,
                    target_block_bytes,
                },
                base_version,
                backend,
                &FixturePlacementBuildEnvelopeV1 {
                    bucket,
                    prefix,
                    source_sha256,
                    suite_sha256,
                    binary_sha256,
                    cargo_lock_sha256,
                },
            ))?;
            let bytes = serde_json::to_vec_pretty(&locator)?;
            fs::write(output, &bytes)?;
            println!("{}", String::from_utf8(bytes)?);
        }
        Commands::ObjectFixtureConsumeGcs {
            trace_seed,
            locator,
            expected_envelope_sha256,
            subject,
            warmup_operations,
            measured_operations,
            concurrent_clients,
            max_local_bytes,
            block_cache_bytes,
            direct_reads,
        } => {
            let placement =
                decode_fixture_placement_locator(&fs::read(locator)?, &expected_envelope_sha256)?;
            let subject = match subject.as_str() {
                "native_snapshot" => OpenRaftHotReadSubject::NativeSnapshot,
                "direct_owned_rocksdb" => OpenRaftHotReadSubject::DirectOwnedRocksdb,
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown object-fixture resident subject {other}"
                    ))
                    .into());
                }
            };
            let profile = ServingRecoveryProfile {
                key_count: placement.key_count,
                value_bytes: usize::try_from(placement.value_bytes)?,
                target_object_bytes: usize::try_from(placement.target_object_bytes)?,
                target_block_bytes: usize::try_from(placement.target_block_bytes)?,
            };
            let executable = std::env::current_exe()?;
            let report = run_openraft_serving_recovery_contract_from_fixture_placement(
                trace_seed,
                &profile,
                2,
                &executable,
                &placement,
                OpenRaftHotReadProfile {
                    subject,
                    seed: trace_seed,
                    key_count: placement.key_count,
                    value_bytes: profile.value_bytes,
                    warmup_operations,
                    measured_operations,
                    concurrent_clients,
                    access_pattern: OpenRaftHotReadAccessPattern::Hotset80_20,
                    max_local_bytes,
                    block_cache_bytes,
                    direct_reads,
                    sample_count: 1,
                    negative_control: None,
                },
                false,
                true,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::T28PlanBuildGcs {
            locator,
            expected_envelope_sha256,
            iam_receipt,
            expected_iam_receipt_sha256,
            cache_state,
            key_ids,
            output,
        } => {
            let placement =
                decode_fixture_placement_locator(&fs::read(locator)?, &expected_envelope_sha256)?;
            let iam = T28ReaderIamReceiptV1::decode(
                &fs::read(iam_receipt)?,
                &expected_iam_receipt_sha256,
            )?;
            let reader_identity =
                T28ReaderPlanIdentityV1::from_receipt(&iam, &expected_iam_receipt_sha256)?;
            let configured_bucket = std::env::var("OKV_GCS_BUCKET")?;
            if configured_bucket != placement.bucket {
                return Err(std::io::Error::other(
                    "T28 configured GCS bucket differs from fixture placement",
                )
                .into());
            }
            if reader_identity.bucket != placement.bucket {
                return Err(std::io::Error::other(
                    "T28 IAM receipt bucket differs from fixture placement",
                )
                .into());
            }
            let cache_state = match cache_state.as_str() {
                "empty_reader" => T28CacheState::EmptyReader,
                "metadata_warm_data_cold" => T28CacheState::MetadataWarmDataCold,
                other => {
                    return Err(
                        std::io::Error::other(format!("unknown T28 cache state {other}")).into(),
                    );
                }
            };
            let backend =
                prefixed_backend(gcs_backend_from_env_no_retries()?, placement.prefix.clone())?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let reader = runtime.block_on(open_generation_pinned_fixture_lazy(
                backend,
                &placement,
                &expected_envelope_sha256,
                &placement.fixture.fixture_id,
                placement.base_version,
            ))?;
            let points = runtime.block_on(async {
                let mut points = Vec::with_capacity(key_ids.len());
                for key_id in key_ids {
                    if key_id >= placement.key_count {
                        return Err("T28 key ID is outside the fixture".to_owned());
                    }
                    let point = reader.plan_point(&key_id.to_be_bytes()).await?;
                    points.push((key_id, point));
                }
                Ok::<_, String>(points)
            })?;
            let plan = T28PointPlanV2::seal(&placement, reader_identity, cache_state, points)?;
            let bytes = serde_json::to_vec_pretty(&plan)?;
            write_new_file(&output, &bytes)?;
            println!("{}", String::from_utf8(bytes)?);
        }
        Commands::T28PointReadGcs {
            locator,
            expected_envelope_sha256,
            plan,
            expected_plan_sha256,
            iam_receipt,
            expected_iam_receipt_sha256,
            ordinal,
            subject,
            output,
        } => {
            let placement =
                decode_fixture_placement_locator(&fs::read(locator)?, &expected_envelope_sha256)?;
            let iam = T28ReaderIamReceiptV1::decode(
                &fs::read(iam_receipt)?,
                &expected_iam_receipt_sha256,
            )?;
            let reader_identity =
                T28ReaderPlanIdentityV1::from_receipt(&iam, &expected_iam_receipt_sha256)?;
            let plan = T28PointPlanV2::decode(
                &fs::read(plan)?,
                &expected_plan_sha256,
                &placement,
                &reader_identity,
            )?;
            if plan.cache_state != T28CacheState::MetadataWarmDataCold {
                return Err(std::io::Error::other(
                    "T28 point command currently requires metadata_warm_data_cold",
                )
                .into());
            }
            let subject = match subject.as_str() {
                "candidate" => T28PointSubject::Candidate,
                "raw_range_control" => T28PointSubject::RawRangeControl,
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown T28 point subject {other}"
                    ))
                    .into());
                }
            };
            let operation = plan
                .operations
                .get(usize::try_from(ordinal)?)
                .ok_or_else(|| std::io::Error::other("T28 operation ordinal is absent"))?;
            let configured_bucket = std::env::var("OKV_GCS_BUCKET")?;
            if configured_bucket != placement.bucket {
                return Err(std::io::Error::other(
                    "T28 configured GCS bucket differs from fixture placement",
                )
                .into());
            }
            let backend =
                prefixed_backend(gcs_backend_from_env_no_retries()?, placement.prefix.clone())?;
            let observed = Arc::new(ProviderAttemptBackend::new(backend, subject.id())?);
            let reader_backend: Arc<dyn okv_object::Backend> = observed.clone();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let reader = runtime.block_on(open_generation_pinned_fixture_lazy(
                reader_backend,
                &placement,
                &expected_envelope_sha256,
                &placement.fixture.fixture_id,
                placement.base_version,
            ))?;
            if subject == T28PointSubject::Candidate {
                let derived = runtime.block_on(reader.plan_point(&operation.point.key))?;
                if derived != operation.point {
                    return Err(std::io::Error::other(
                        "T28 candidate derived a range different from the sealed plan",
                    )
                    .into());
                }
            }
            observed.clear_events();
            let started = Instant::now();
            let read = match subject {
                T28PointSubject::Candidate => {
                    runtime.block_on(reader.read_planned_point(&operation.point))?
                }
                T28PointSubject::RawRangeControl => runtime.block_on(read_planned_point(
                    observed.as_ref(),
                    &operation.point.data_key,
                    None,
                    &operation.point.block,
                    &operation.point.key,
                    operation.point.read_version,
                ))?,
            };
            let elapsed_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let receipt = T28PointExecutionReceiptV1::new(
                &plan,
                ordinal,
                subject,
                "gcs",
                elapsed_nanos,
                &read,
                observed.events(),
            )?;
            let bytes = serde_json::to_vec_pretty(&receipt)?;
            if let Some(output) = output {
                write_new_file(&output, &bytes)?;
            }
            println!("{}", String::from_utf8(bytes)?);
        }
        Commands::T27PlanBuild {
            locator,
            expected_envelope_sha256,
            profile,
            runtime_source_sha256,
            runtime_cargo_lock,
            machine_receipt,
            scratch_root,
            output,
        } => {
            let placement =
                decode_fixture_placement_locator(&fs::read(locator)?, &expected_envelope_sha256)?;
            let profile = parse_t27_plan_profile(&profile).map_err(std::io::Error::other)?;
            let execution = capture_t27_execution_envelope(
                &runtime_source_sha256,
                &runtime_cargo_lock,
                &machine_receipt,
                &scratch_root,
            )?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let expected = runtime.block_on(derive_t27_expected_identity(
                &placement,
                profile,
                &std::env::current_exe()?,
            ))?;
            let plan = build_t27_execution_plan(&placement, profile, execution, expected)?;
            let bytes = serde_json::to_vec_pretty(&plan)?;
            write_new_file(&output, &bytes)?;
            println!("{}", String::from_utf8(bytes)?);
        }
        Commands::T27PlanIncarnateGcs {
            source_plan,
            expected_source_plan_sha256,
            runtime_executable,
            runtime_cargo_lock,
            machine_receipt,
            scratch_root,
            plan_output,
            receipt_output,
        } => {
            let source =
                decode_t27_execution_plan(&fs::read(source_plan)?, &expected_source_plan_sha256)?;
            let execution = capture_t27_execution_envelope_for_executable(
                &source.execution.runtime_source_sha256,
                &runtime_executable,
                &runtime_cargo_lock,
                &machine_receipt,
                &scratch_root,
            )?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let expected = runtime.block_on(derive_t27_expected_identity(
                &source.fixture,
                source.profile,
                &runtime_executable,
            ))?;
            if expected != source.expected {
                return Err(std::io::Error::other(
                    "T27 execution incarnation exact-open changed the semantic oracle",
                )
                .into());
            }
            let (incarnated, receipt) = build_t27_execution_incarnation(&source, execution)?;
            write_new_file(&plan_output, &serde_json::to_vec_pretty(&incarnated)?)?;
            let receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
            write_new_file(&receipt_output, &receipt_bytes)?;
            println!("{}", String::from_utf8(receipt_bytes)?);
        }
        Commands::T27PlanPoisonCheck {
            plan,
            expected_plan_sha256,
            poison,
            poisoned_plan_output,
            receipt_output,
        } => {
            let plan = decode_t27_execution_plan(&fs::read(plan)?, &expected_plan_sha256)?;
            let poison = poison
                .parse::<T27PlanPoisonV1>()
                .map_err(std::io::Error::other)?;
            let (poisoned_plan_bytes, receipt) = verify_t27_plan_poison(&plan, poison)?;
            write_new_file(&poisoned_plan_output, &poisoned_plan_bytes)?;
            let receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
            write_new_file(&receipt_output, &receipt_bytes)?;
            println!("{}", String::from_utf8(receipt_bytes)?);
        }
        Commands::T27PositionPoisonCheck {
            plan,
            expected_plan_sha256,
            position_receipt,
            poison,
            poisoned_receipt_output,
            receipt_output,
        } => {
            let plan = decode_t27_execution_plan(&fs::read(plan)?, &expected_plan_sha256)?;
            let source: T27PositionReceiptV1 =
                serde_json::from_slice(&fs::read(position_receipt)?)?;
            source.validate(&plan).map_err(std::io::Error::other)?;
            let poison = poison
                .parse::<T27PositionPoisonV1>()
                .map_err(std::io::Error::other)?;
            let (poisoned_receipt_bytes, receipt) =
                verify_t27_position_poison(&plan, &source, poison)?;
            write_new_file(&poisoned_receipt_output, &poisoned_receipt_bytes)?;
            let receipt_bytes = serde_json::to_vec_pretty(&receipt)?;
            write_new_file(&receipt_output, &receipt_bytes)?;
            println!("{}", String::from_utf8(receipt_bytes)?);
        }
        Commands::T27PlanRunGcs {
            plan,
            expected_plan_sha256,
            runtime_cargo_lock,
            machine_receipt,
            scratch_root,
            output_dir,
        } => {
            let receipt = run_t27_plan_controller(
                &plan,
                &expected_plan_sha256,
                &runtime_cargo_lock,
                &machine_receipt,
                &scratch_root,
                &output_dir,
            )?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::T27StratumRunGcs {
            plan,
            expected_plan_sha256,
            stratum_id,
            runtime_cargo_lock,
            machine_receipt,
            scratch_root,
            output_dir,
        } => {
            let receipt = run_t27_stratum_controller(
                &plan,
                &expected_plan_sha256,
                &stratum_id,
                &runtime_cargo_lock,
                &machine_receipt,
                &scratch_root,
                &output_dir,
            )?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::T27AggregateRun { manifest, output } => {
            let receipt = run_t27_aggregate(&manifest, &output)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::T27PlanPositionGcs {
            plan,
            expected_plan_sha256,
            ordinal,
            controller_id,
            worker_id,
            runtime_cargo_lock,
            machine_receipt,
            scratch_root,
            lease_id,
        } => {
            let receipt = run_t27_plan_position(
                &plan,
                &expected_plan_sha256,
                ordinal,
                controller_id,
                worker_id,
                &runtime_cargo_lock,
                &machine_receipt,
                &scratch_root,
                &lease_id,
            )?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::ObjectFixtureResidentTrace {
            seed,
            subject,
            regenerate_control_poison,
            key_count,
            value_bytes,
            target_object_bytes,
            target_block_bytes,
        } => {
            let subject = match subject.as_str() {
                "native_snapshot" => OpenRaftHotReadSubject::NativeSnapshot,
                "direct_owned_rocksdb" => OpenRaftHotReadSubject::DirectOwnedRocksdb,
                other => {
                    return Err(std::io::Error::other(format!(
                        "unknown object-fixture resident subject {other}"
                    ))
                    .into());
                }
            };
            let executable = std::env::current_exe()?;
            let report = run_openraft_serving_recovery_contract_from_object_fixture(
                seed,
                &ServingRecoveryProfile {
                    key_count,
                    value_bytes,
                    target_object_bytes,
                    target_block_bytes,
                },
                2,
                &executable,
                OpenRaftServingObjectBackend::LocalFilesystem,
                OpenRaftHotReadProfile {
                    subject,
                    seed,
                    key_count,
                    value_bytes,
                    warmup_operations: 256,
                    measured_operations: 1_024,
                    concurrent_clients: 1,
                    access_pattern: OpenRaftHotReadAccessPattern::Hotset80_20,
                    max_local_bytes: 128 * 1_024 * 1_024,
                    block_cache_bytes: 16 * 1_024 * 1_024,
                    direct_reads: false,
                    sample_count: 1,
                    negative_control: None,
                },
                regenerate_control_poison,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::FrontieredProcessSnapshotTrace { seed, mode } => {
            let mode =
                parse_frontiered_process_snapshot_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_frontiered_process_snapshot_contract(
                seed,
                mode,
                &FrontieredProcessSnapshotProfile {
                    frontier_cycles: 4,
                    transactions_per_cycle: 256,
                    transactions_per_batch: 32,
                    live_keys: 256,
                    value_bytes: 128,
                    retry_window: 64,
                    max_physical_amplification: 8.0,
                    max_snapshot_growth_ratio: 1.25,
                },
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::StorageLayoutTrace {
            seed,
            mode,
            key_count,
            point_operations,
        } => {
            let mode = parse_storage_layout_mode(&mode).map_err(std::io::Error::other)?;
            let report = run_storage_layout_contract(
                mode,
                &StorageLayoutProfile {
                    key_count,
                    canonical_live_row_bytes: 512,
                    opaque_payload_bytes: 480,
                    base_version: 1,
                    delta_cycles: 4,
                    update_fraction: 0.125,
                    delete_fraction: 0.01,
                    point_operations,
                    target_run_object_bytes: 8_388_608,
                    row_block_bytes: 65_536,
                    columnar_block_rows: 1_024,
                    overlay_cache_bytes: 16_777_216,
                    seeds: vec![seed],
                    repeats: 1,
                },
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ConsensusNode { config_json } => {
            let config = serde_json::from_str::<ProcessNodeConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_process_node(config))?;
        }
        Commands::PublisherNode { config_json } => {
            let config = serde_json::from_str::<PublisherProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_publication_publisher_process_node(config))?;
        }
        Commands::PublisherPutNode { config_json } => {
            let config = serde_json::from_str::<PublisherPutRecoveryProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_publication_publisher_put_recovery_node(config))?;
        }
        Commands::PublisherManifestNode { config_json } => {
            let config =
                serde_json::from_str::<PublisherManifestRecoveryProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_publication_publisher_manifest_recovery_node(config))?;
        }
        Commands::PublisherPublishNode { config_json } => {
            let config =
                serde_json::from_str::<PublisherPublishRecoveryProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_publication_publisher_publish_recovery_node(config))?;
        }
        Commands::ServingRecoveryNode { config_json } => {
            let config = serde_json::from_str::<ServingRecoveryProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let report = runtime.block_on(run_serving_recovery_node(config))?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ServingRecoveryOpenRaftNode { config_json } => {
            let config = serde_json::from_str::<OpenRaftServingProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let report = runtime.block_on(run_openraft_serving_recovery_node(config))?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::StagedTxLogNode { config_json } => {
            let config = serde_json::from_str::<StagedTxLogNodeConfig>(&config_json)?;
            run_staged_txlog_node(config).map_err(std::io::Error::other)?;
        }
    }
    Ok(())
}

fn list_metrics(loaded: &LoadedSuite) {
    for metric in &loaded.registry.metrics {
        println!(
            "{}\t{}\t{:?}\t{}",
            metric.id, metric.otel_name, metric.kind, metric.unit
        );
    }
}

#[derive(Serialize)]
struct ExecutionPlan<'a> {
    suite: &'a str,
    status: &'a str,
    profile: &'a str,
    repeats: u32,
    budget_kind: BudgetKind,
    budget_limit: f64,
    workloads: Vec<&'a str>,
    lanes: Vec<&'a str>,
    required_signals: &'a [String],
    telemetry_required: bool,
}

fn print_plan(loaded: &LoadedSuite, profile_id: &str) -> Result<(), Box<dyn Error>> {
    let profile = loaded
        .suite
        .profiles
        .get(profile_id)
        .ok_or_else(|| format!("unknown profile {profile_id}"))?;
    let plan = ExecutionPlan {
        suite: &loaded.suite.id,
        status: &loaded.suite.status,
        profile: profile_id,
        repeats: profile.repeats,
        budget_kind: profile.budget_kind,
        budget_limit: profile.budget_limit,
        workloads: loaded
            .suite
            .workloads
            .iter()
            .map(|workload| workload.id.as_str())
            .collect(),
        lanes: loaded
            .suite
            .lanes
            .iter()
            .map(|lane| lane.id.as_str())
            .collect(),
        required_signals: &loaded.suite.telemetry.required_signals,
        telemetry_required: loaded
            .suite
            .telemetry
            .required_for_profiles
            .iter()
            .any(|required| required == profile_id),
    };
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_suite(
    suite_path: &Path,
    profile_id: &str,
    workload_id: &str,
    backend: &str,
    output: Option<&Path>,
    batch_id: Option<&str>,
    allow_dirty: bool,
) -> Result<(), Box<dyn Error>> {
    let source_dirty = git_is_dirty()?;
    if !allow_dirty && source_dirty {
        return Err("source tree is dirty; commit the candidate or pass --allow-dirty for a non-comparable diagnostic run".into());
    }
    let loaded = load_suite(suite_path)?;
    let profile = loaded
        .suite
        .profiles
        .get(profile_id)
        .ok_or_else(|| format!("unknown profile {profile_id}"))?;
    if let Some(expected_backend) = profile
        .parameters
        .get("backend")
        .and_then(toml::Value::as_str)
    {
        if expected_backend != backend {
            return Err(format!(
                "profile {profile_id} requires backend {expected_backend}, received {backend}"
            )
            .into());
        }
    }
    let workload = loaded
        .suite
        .workloads
        .iter()
        .find(|candidate| candidate.id == workload_id)
        .ok_or_else(|| format!("unknown workload {workload_id}"))?;
    validate_workload_selection(profile, workload)?;
    let lane = loaded
        .suite
        .lanes
        .iter()
        .find(|candidate| candidate.id == workload.lane)
        .ok_or_else(|| format!("unknown lane {}", workload.lane))?;
    let primary_definition = loaded
        .registry
        .metrics
        .iter()
        .find(|metric| metric.id == lane.primary_metric)
        .ok_or_else(|| format!("unknown primary metric {}", lane.primary_metric))?;
    let (machine_identity, machine_receipt_path) = resolve_machine_identity(suite_path, profile)?;

    let run_id = Uuid::new_v4().to_string();
    let batch_id = batch_id
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| run_id.clone(), ToOwned::to_owned);
    let mut candidate_commit = git_revision("HEAD")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    if source_dirty {
        candidate_commit.push_str("+dirty");
    }
    let parent_commit = git_revision("HEAD^")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    let suite_hash = contract_hash(&loaded)?;
    let profile_hash = sha256(toml::to_string(profile)?.as_bytes());
    let workload_profile_hash = profile
        .workload_profile
        .as_ref()
        .map(toml::to_string)
        .transpose()?
        .map(|value| sha256(value.as_bytes()));
    let lockfile_hash = sha256(&fs::read("Cargo.lock")?);
    let resource = RunResource {
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        environment: profile_id.to_owned(),
        run_id: run_id.clone(),
        batch_id: batch_id.clone(),
        suite_id: loaded.suite.id.clone(),
        suite_hash: suite_hash.clone(),
        profile_id: profile_id.to_owned(),
        profile_hash: profile_hash.clone(),
        candidate_commit: candidate_commit.clone(),
        backend: backend.to_owned(),
    };
    let telemetry = Telemetry::init(&loaded.suite.telemetry, profile_id, &resource)?;
    let mut recorder = telemetry.recorder(&loaded.registry);
    let run_span = info_span!(
        "okv.eval.run",
        run.id = %run_id,
        batch.id = %batch_id,
        suite.id = %loaded.suite.id,
        profile.id = %profile_id,
        workload.id = %workload.id,
        lane.id = %lane.id,
        backend = %backend
    );
    let run_guard = run_span.enter();
    info!(
        telemetry.enabled = telemetry.enabled(),
        "evaluation started"
    );

    let dataset = dataset_config(&loaded, profile_id);
    let seeds = dataset
        .map(|dataset| dataset.seeds.clone())
        .unwrap_or_default();
    let started = Instant::now();
    let workload_execution = execute_workload(
        workload,
        &run_id,
        &candidate_commit,
        &seeds,
        backend,
        dataset,
        profile,
    );
    let elapsed = started.elapsed().as_secs_f64();
    for measurement in &workload_execution.measurements {
        recorder.record(
            measurement.metric,
            measurement.value,
            measurement.attributes.clone(),
        )?;
    }
    let execution_passed = workload_execution.passed();
    let semantic_failures = workload_execution
        .measurements
        .iter()
        .filter(|measurement| measurement.metric == "correctness.anomalies")
        .map(|measurement| measurement.value)
        .sum::<f64>();
    let semantic_failures = if workload_execution
        .measurements
        .iter()
        .any(|measurement| measurement.metric == "correctness.anomalies")
    {
        semantic_failures
    } else {
        f64::from(!execution_passed)
    };
    let semantic_correctness_passed = semantic_failures == 0.0;
    recorder.record(
        "operation.duration",
        elapsed,
        attributes(&[
            ("lane", &lane.id),
            ("workload", &workload.id),
            ("operation", &workload.operation),
            ("backend", backend),
            ("result", if execution_passed { "pass" } else { "fail" }),
        ]),
    )?;
    recorder.record(
        "correctness.failures",
        semantic_failures,
        attributes(&[("lane", &lane.id), ("workload", &workload.id)]),
    )?;

    let samples = recorder.samples(&lane.primary_metric).to_vec();
    let samples = if samples.is_empty() && lane.primary_metric == "correctness.failures" {
        vec![semantic_failures]
    } else {
        samples
    };
    if samples.is_empty() {
        if let Some(error) = workload_execution.error.as_deref() {
            return Err(format!(
                "workload {} failed before recording primary metric {}: {error}",
                workload.id, lane.primary_metric
            )
            .into());
        }
        return Err(format!(
            "workload {} did not record primary metric {}",
            workload.id, lane.primary_metric
        )
        .into());
    }
    let sample_median = median(&samples);
    let selected_value = statistic_value(&samples, &lane.statistic)?;
    let budget_observed = match profile.budget_kind {
        BudgetKind::Seconds => elapsed,
        BudgetKind::Events | BudgetKind::Operations => workload_execution.budget_units,
    };
    let budget_passed = budget_observed <= profile.budget_limit;
    let verdict = if !execution_passed || !budget_passed {
        Verdict::Discard
    } else if source_dirty {
        Verdict::Inconclusive
    } else {
        Verdict::Keep
    };
    let reason = workload_execution.error.unwrap_or_else(|| {
        if !budget_passed {
            format!(
                "budget exceeded: observed {budget_observed}, limit {}",
                profile.budget_limit
            )
        } else if source_dirty {
            "diagnostic dirty-tree run; hard gates passed but the result is not comparable"
                .to_owned()
        } else {
            "all configured hard gates passed".to_owned()
        }
    });
    let result = EvalResult {
        schema_version: 1,
        run_id,
        batch_id,
        created_at: Utc::now().to_rfc3339(),
        lane: lane.id.clone(),
        suite: loaded.suite.id.clone(),
        suite_hash,
        profile: ProfileIdentity {
            id: profile_id.to_owned(),
            hash: profile_hash,
            evidence_class: profile.evidence_class,
            workload_profile_hash,
            machine: machine_identity,
            rustc: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
            lockfile_hash,
        },
        candidate_commit,
        parent_commit,
        backend: backend.to_owned(),
        seeds,
        budget: BudgetResult {
            kind: profile.budget_kind,
            limit: profile.budget_limit,
            observed: budget_observed,
        },
        hard_gates: {
            let mut gates = vec![
                HardGateResult {
                    id: "correctness_failures".to_owned(),
                    status: if semantic_correctness_passed {
                        GateStatus::Pass
                    } else {
                        GateStatus::Fail
                    },
                    detail: None,
                },
                HardGateResult {
                    id: "workload_execution".to_owned(),
                    status: if execution_passed {
                        GateStatus::Pass
                    } else {
                        GateStatus::Fail
                    },
                    detail: None,
                },
                HardGateResult {
                    id: "budget_must_hold".to_owned(),
                    status: if budget_passed {
                        GateStatus::Pass
                    } else {
                        GateStatus::Fail
                    },
                    detail: None,
                },
                HardGateResult {
                    id: "schema_valid".to_owned(),
                    status: GateStatus::Pass,
                    detail: None,
                },
            ];
            gates.extend(workload_execution.hard_gates);
            gates
        },
        primary_metric: PrimaryMetricResult {
            name: primary_definition.otel_name.clone(),
            unit: primary_definition.unit.clone(),
            direction: lane.direction,
            statistic: lane.statistic.clone(),
            value: selected_value,
            mad: median_absolute_deviation(&samples, sample_median),
            median: sample_median,
            samples,
            incumbent_median: None,
        },
        secondary_metrics: {
            let mut metrics = workload_execution.secondary_metrics;
            metrics.insert("operation.duration.median".to_owned(), elapsed);
            metrics
        },
        verdict,
        reason,
        artifact_refs: {
            let mut refs = workload_execution.artifact_refs;
            if let Some(path) = output {
                refs.push(path.display().to_string());
            }
            if let Some(path) = machine_receipt_path {
                refs.push(path.display().to_string());
            }
            refs
        },
    };
    let value = serde_json::to_value(&result)?;
    validate_result(&loaded.result_schema_path, &value)?;
    let rendered = serde_json::to_string_pretty(&value)?;
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{rendered}\n"))?;
    }
    println!("{rendered}");
    info!(verdict = ?result.verdict, "evaluation finished");
    drop(run_guard);
    drop(run_span);
    let telemetry_flush = telemetry.shutdown();
    if !telemetry_flush.all_succeeded() {
        return Err(std::io::Error::other(
            "evaluation telemetry did not flush and shut down all required signal exporters",
        )
        .into());
    }
    Ok(())
}

fn resolve_machine_identity(
    suite_path: &Path,
    profile: &ProfileConfig,
) -> Result<(String, Option<PathBuf>), Box<dyn Error>> {
    let Some(receipt_env) = profile
        .parameters
        .get("machine_receipt_env")
        .and_then(toml::Value::as_str)
    else {
        return Ok((
            command_output("uname", &["-m"]).unwrap_or_else(|| "unknown".to_owned()),
            None,
        ));
    };
    let receipt_path = PathBuf::from(std::env::var(receipt_env).map_err(|_| {
        std::io::Error::other(format!(
            "profile requires a machine receipt; set {receipt_env}"
        ))
    })?);
    let receipt_bytes = fs::read(&receipt_path)?;
    let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes)?;
    let schema_relative = profile
        .parameters
        .get("machine_receipt_schema")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| {
            std::io::Error::other(
                "profile with machine_receipt_env must set machine_receipt_schema",
            )
        })?;
    let schema_path = suite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(schema_relative);
    validate_result(&schema_path, &receipt)?;
    if let Some(expected_interface) = profile
        .parameters
        .get("required_hot_scratch_interface")
        .and_then(toml::Value::as_str)
    {
        let observed_interface = receipt
            .pointer("/runner/hot_scratch/interface")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                std::io::Error::other("machine receipt has no runner hot-scratch interface")
            })?;
        if observed_interface != expected_interface {
            return Err(std::io::Error::other(format!(
                "profile requires hot-scratch interface {expected_interface}, receipt has {observed_interface}"
            ))
            .into());
        }
        let scratch_env = profile
            .parameters
            .get("serving_scratch_env")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| {
                std::io::Error::other(
                    "profile with required_hot_scratch_interface must set serving_scratch_env",
                )
            })?;
        let configured_scratch = PathBuf::from(std::env::var(scratch_env).map_err(|_| {
            std::io::Error::other(format!(
                "profile requires a serving scratch root; set {scratch_env}"
            ))
        })?);
        let receipt_mount = PathBuf::from(
            receipt
                .pointer("/runner/hot_scratch/mount")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    std::io::Error::other("machine receipt has no runner hot-scratch mount")
                })?,
        );
        if !configured_scratch.starts_with(&receipt_mount) {
            return Err(std::io::Error::other(format!(
                "serving scratch {} is not below receipt hot mount {}",
                configured_scratch.display(),
                receipt_mount.display()
            ))
            .into());
        }
    }
    Ok((
        format!("receipt-sha256:{}", sha256(&receipt_bytes)),
        Some(receipt_path),
    ))
}

fn execute_workload(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    match workload.operation.as_str() {
        "model_smoke" => execution_from_result(run_model_smoke()),
        "provider_semantic_preflight" => run_provider_semantic_preflight(workload, backend),
        "foundationdb_objectkv_logical_lifecycle" => {
            run_foundationdb_logical_lifecycle(workload, run_id, backend, dataset, profile)
        }
        "foundationdb_objectkv_media_loss_lifecycle" => {
            run_foundationdb_media_loss_lifecycle(workload, backend, dataset, profile)
        }
        "provider_incarnation_authority_contract" => {
            run_provider_incarnation_authority(workload, seeds, backend)
        }
        "foundationdb_objectkv_provider_incarnation" => {
            run_foundationdb_provider_incarnation(workload, seeds, backend, profile)
        }
        "deterministic_generation_recovery" => {
            run_generation_recovery(workload, candidate_commit, seeds)
        }
        "commit_envelope_contract" => run_commit_envelope_contract(workload, seeds),
        "staged_txlog_contract" => run_staged_txlog_protocol(workload, seeds, backend),
        "staged_txlog_process_contract" => run_staged_txlog_process(workload, seeds, backend),
        "strict_serializability_contract" => run_strict_serializability_contract(workload, seeds),
        "transaction_process_serializability_contract" => {
            run_transaction_process_serializability(workload, seeds, backend)
        }
        "persisted_wal_contract" => run_persisted_wal(workload, seeds, backend),
        "raft_storage_contract" => run_raft_storage(workload, seeds, backend),
        "raft_cluster_contract" => run_raft_cluster(workload, seeds, backend),
        "raft_process_contract" => run_raft_process(workload, seeds, backend),
        "generation_process_contract" => run_generation_process(workload, seeds, backend),
        "publication_authority_process_contract" => {
            run_publication_process(workload, seeds, backend)
        }
        "publication_publisher_process_contract" => {
            run_publication_publisher_process(workload, seeds, backend)
        }
        "publication_publisher_put_recovery_contract" => {
            run_publication_publisher_put_recovery(workload, seeds, backend)
        }
        "publication_publisher_manifest_recovery_contract" => {
            run_publication_publisher_manifest_recovery(workload, seeds, backend)
        }
        "publication_publisher_publish_recovery_contract" => {
            run_publication_publisher_publish_recovery(workload, seeds, backend)
        }
        "htap_exactness_contract" => run_htap_exactness_contract(workload, seeds, backend),
        "htap_physical_contract" => run_htap_physical_contract(workload, seeds, backend),
        "htap_streaming_contract" => run_htap_streaming_contract(workload, seeds, backend),
        "columnar_range_datafusion_contract" => {
            run_columnar_range_datafusion(workload, seeds, backend, dataset, profile)
        }
        "columnar_cache_admission_contract" => {
            run_columnar_cache_admission(workload, run_id, seeds, backend, dataset, profile)
        }
        "object_publication_gc_contract" => {
            run_object_publication_gc_contract(workload, seeds, backend)
        }
        "object_publication_adapter_contract" => {
            run_object_publication_adapter_contract(workload, seeds, backend)
        }
        "model_differential_history" => run_model_differential(workload, seeds),
        "object_store_conformance" => run_object_store_conformance(workload, backend),
        "resident_hot_path" | "direct_rocksdb_hot_control" | "direct_rocksdb_owned_hot_control" => {
            run_resident_hot_path(workload, seeds, backend, dataset, profile)
        }
        "elastic_cold_point" | "indexed_object_reader_control" => {
            run_elastic_cold_point(workload, seeds, backend, dataset, profile)
        }
        "empty_worker_recovery" | "full_local_restore_control" => {
            run_empty_worker_recovery(workload, seeds, backend, dataset, profile)
        }
        "serving_worker_process_recovery_contract" => {
            run_serving_worker_process_recovery(workload, seeds, backend, dataset, profile)
        }
        "serving_worker_openraft_recovery_contract" => {
            run_openraft_serving_worker_recovery(workload, run_id, seeds, backend, dataset, profile)
        }
        "transaction_authority_state_scale_contract"
        | "transaction_authority_split_state_scale_contract" => {
            run_transaction_authority_state_scale(workload, seeds, backend, dataset, profile)
        }
        "authenticated_object_frontier_contract" => {
            run_authenticated_object_frontier(workload, seeds, backend)
        }
        "commit_group_contract" => run_commit_group(workload, seeds, backend, dataset, profile),
        "transaction_batch_contract" => {
            run_transaction_batch(workload, seeds, backend, dataset, profile)
        }
        "commit_proxy_contract" => run_commit_proxy(workload, seeds, backend, profile),
        "commit_proxy_object_frontier_contract" => {
            run_commit_proxy_object_frontier(workload, seeds, backend, profile)
        }
        "process_snapshot_compaction_contract" => {
            run_process_snapshot_compaction(workload, seeds, backend, dataset, profile)
        }
        "object_fixture_anchor_contract" => {
            run_object_fixture_anchor(workload, seeds, backend, profile)
        }
        "object_fixture_contract" => {
            run_object_fixture_contract_workload(workload, seeds, backend, dataset, profile)
        }
        "object_fixture_resident_process_contract" => {
            run_object_fixture_resident_process(workload, seeds, backend, dataset, profile)
        }
        "object_fixture_gcs_preflight_contract" => {
            run_object_fixture_gcs_preflight(workload, seeds, backend, dataset, profile)
        }
        "frontiered_process_snapshot_contract" => {
            run_frontiered_process_snapshot(workload, seeds, backend, dataset, profile)
        }
        "storage_layout_diagnostic" => {
            run_storage_layout(workload, run_id, seeds, backend, dataset, profile)
        }
        "slatedb_phase0_filesystem_contract" => run_slatedb_phase0_filesystem(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        operation => execution_from_result(Err(format!(
            "operation {operation} is declared but has no runner implementation"
        ))),
    }
}

#[allow(clippy::too_many_lines)]
fn run_transaction_authority_state_scale(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "data-openraft-local-process";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "transaction-authority scale requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "transaction-authority scale requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.key_count == 0 {
        return execution_from_result(Err(
            "transaction-authority scale requires live keys and fixed seeds".to_owned(),
        ));
    }
    let checkpoints = match profile
        .parameters
        .get("commit_checkpoints")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "authority-state profile requires commit_checkpoints".to_owned())
        .and_then(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_integer()
                        .ok_or_else(|| "commit checkpoints must be integers".to_owned())
                        .and_then(|value| {
                            u64::try_from(value)
                                .map_err(|error| format!("invalid commit checkpoint: {error}"))
                        })
                })
                .collect::<Result<Vec<_>, _>>()
        }) {
        Ok(checkpoints) => checkpoints,
        Err(error) => return execution_from_result(Err(error)),
    };
    let integer = |name: &str| -> Result<u64, String> {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("authority-state profile requires integer {name}"))
            .and_then(|value| {
                u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
            })
    };
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(128);
    let max_projected_growth_ratio = match integer("max_projected_growth_ratio") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let scale_profile = AuthorityStateScaleProfile {
        live_keys: dataset.key_count,
        value_bytes,
        commit_checkpoints: checkpoints,
        max_projected_growth_ratio,
    };
    let split_contract = workload.operation == "transaction_authority_split_state_scale_contract";
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let subject = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str);
    let mode = match (split_contract, negative_control, subject) {
        (true, Some("serving_only_accounting"), _) => {
            AuthorityStateScaleMode::ServingOnlyAccountingPoison
        }
        (true, None, Some("object_frontier_only")) => {
            AuthorityStateScaleMode::IdealStreamPopProjection
        }
        (true, None, None) => AuthorityStateScaleMode::AlignedFrontiersProjection,
        (false, Some("retained_only_accounting"), _) => {
            AuthorityStateScaleMode::RetainedOnlyAccountingPoison
        }
        (false, None, Some("no_pop")) => AuthorityStateScaleMode::NoPopControl,
        (false, None, None) => AuthorityStateScaleMode::IdealStreamPopProjection,
        (_, Some(other), _) => {
            return execution_from_result(Err(format!(
                "unknown authority-state negative control {other}"
            )))
        }
        (_, None, Some(other)) => {
            return execution_from_result(Err(format!("unknown authority-state subject {other}")))
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_authority_state_scale_contract(*seed, mode, &scale_profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay =
        match run_authority_state_scale_contract(seeds[0], mode, &scale_profile, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
    transaction_authority_state_scale_execution(workload, backend, mode, &reports, &replay)
}

#[allow(clippy::too_many_lines)]
fn transaction_authority_state_scale_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: AuthorityStateScaleMode,
    reports: &[AuthorityStateScaleReport],
    replay: &AuthorityStateScaleReport,
) -> WorkloadExecution {
    let process_boundary = reports.iter().all(|report| report.authority_processes == 3);
    let projection_non_mutating = reports.iter().all(|report| report.projection_non_mutating);
    let complete_accounting = reports.iter().all(|report| report.accounting_complete);
    let exact_replay = reports.first().is_some_and(|first| {
        first.structural_sha256 == replay.structural_sha256
            && first.checkpoints == replay.checkpoints
    });
    let bounded_state = reports.iter().all(|report| report.bounded_state);
    let anomaly_count = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();
    let poison_detected = match mode {
        AuthorityStateScaleMode::RetainedOnlyAccountingPoison
        | AuthorityStateScaleMode::ServingOnlyAccountingPoison => {
            !complete_accounting && anomaly_count > 0
        }
        AuthorityStateScaleMode::AlignedFrontiersProjection
        | AuthorityStateScaleMode::IdealStreamPopProjection
        | AuthorityStateScaleMode::NoPopControl => complete_accounting && anomaly_count == 0,
    };
    let expired_retry_rejected = reports.iter().all(|report| report.expired_retry_rejected);
    let error = if !process_boundary || !projection_non_mutating || !exact_replay {
        Some("transaction-authority accounting violated its process or replay contract".to_owned())
    } else if !poison_detected {
        Some("transaction-authority incomplete-accounting poison was not detected".to_owned())
    } else if matches!(
        mode,
        AuthorityStateScaleMode::RetainedOnlyAccountingPoison
            | AuthorityStateScaleMode::ServingOnlyAccountingPoison
    ) {
        Some(format!(
            "incomplete authority accounting omitted state at {anomaly_count} checkpoints"
        ))
    } else if matches!(
        mode,
        AuthorityStateScaleMode::AlignedFrontiersProjection
            | AuthorityStateScaleMode::IdealStreamPopProjection
    ) && !bounded_state
    {
        Some(format!(
            "complete authority state remains lifetime-commit-sized after ideal stream pop; ratios={:?}",
            reports
                .iter()
                .map(|report| report.growth_ratio)
                .collect::<Vec<_>>()
        ))
    } else {
        None
    };
    let mode_name = match mode {
        AuthorityStateScaleMode::AlignedFrontiersProjection => "aligned_frontiers_projection",
        AuthorityStateScaleMode::IdealStreamPopProjection => "ideal_stream_pop_projection",
        AuthorityStateScaleMode::NoPopControl => "no_pop_control",
        AuthorityStateScaleMode::RetainedOnlyAccountingPoison => "retained_only_accounting_poison",
        AuthorityStateScaleMode::ServingOnlyAccountingPoison => "serving_only_accounting_poison",
    };
    let mut measurements = Vec::new();
    for report in reports {
        measurements.push(Measurement {
            metric: "authority.snapshot_growth_ratio",
            value: report.growth_ratio,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("projection", mode_name),
            ]),
        });
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: resident_count_as_f64(report.correctness_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "complete-authority-state-v1"),
                (
                    "anomaly.class",
                    if report.correctness_anomalies == 0 {
                        "none"
                    } else {
                        "incomplete_accounting"
                    },
                ),
            ]),
        });
        for checkpoint in &report.checkpoints {
            let checkpoint_name = format!("c{}", checkpoint.commits);
            for (projection, bytes) in [
                ("actual", checkpoint.stats.snapshot_bytes),
                ("stream_popped", checkpoint.stats.projected_snapshot_bytes),
                ("selected", checkpoint.selected_snapshot_bytes),
            ] {
                measurements.push(Measurement {
                    metric: "authority.snapshot_bytes",
                    value: resident_count_as_f64(bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("projection", projection),
                        ("checkpoint.class", &checkpoint_name),
                    ]),
                });
            }
            for (component, bytes) in [
                (
                    "transaction_authority",
                    checkpoint.stats.transaction_authority_bytes,
                ),
                ("serving_state", checkpoint.stats.serving_state_bytes),
                ("resolver_state", checkpoint.stats.resolver_state_bytes),
                (
                    "transaction_retry_state",
                    checkpoint.stats.transaction_retry_state_bytes,
                ),
                (
                    "transaction_frontier_state",
                    checkpoint.stats.transaction_frontier_state_bytes,
                ),
                (
                    "retained_transactions",
                    checkpoint.stats.retained_transactions_bytes,
                ),
                ("durable_outcomes", checkpoint.stats.durable_outcomes_bytes),
                (
                    "request_fingerprints",
                    checkpoint.stats.request_fingerprints_bytes,
                ),
            ] {
                measurements.push(Measurement {
                    metric: "authority.state_component_bytes",
                    value: resident_count_as_f64(bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("component", component),
                        ("checkpoint.class", &checkpoint_name),
                    ]),
                });
            }
        }
    }

    let first = reports.first();
    let mut secondary_metrics = BTreeMap::new();
    if let Some(first) = first {
        for checkpoint in &first.checkpoints {
            secondary_metrics.insert(
                format!("authority.actual_snapshot_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.snapshot_bytes),
            );
            secondary_metrics.insert(
                format!(
                    "authority.stream_popped_snapshot_bytes.c{}",
                    checkpoint.commits
                ),
                resident_count_as_f64(checkpoint.stats.projected_snapshot_bytes),
            );
            secondary_metrics.insert(
                format!("authority.txlog_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.retained_transactions_bytes),
            );
            secondary_metrics.insert(
                format!("authority.transaction_state_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.transaction_authority_bytes),
            );
            secondary_metrics.insert(
                format!("authority.serving_state_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.serving_state_bytes),
            );
            secondary_metrics.insert(
                format!("authority.resolver_state_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.resolver_state_bytes),
            );
            secondary_metrics.insert(
                format!(
                    "authority.transaction_retry_state_bytes.c{}",
                    checkpoint.commits
                ),
                resident_count_as_f64(checkpoint.stats.transaction_retry_state_bytes),
            );
            secondary_metrics.insert(
                format!(
                    "authority.transaction_frontier_state_bytes.c{}",
                    checkpoint.commits
                ),
                resident_count_as_f64(checkpoint.stats.transaction_frontier_state_bytes),
            );
            secondary_metrics.insert(
                format!("authority.outcome_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.durable_outcomes_bytes),
            );
            secondary_metrics.insert(
                format!("authority.fingerprint_bytes.c{}", checkpoint.commits),
                resident_count_as_f64(checkpoint.stats.request_fingerprints_bytes),
            );
        }
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(process_boundary),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "ideal_projection_does_not_mutate_retained_stream".to_owned(),
                status: gate_status(projection_non_mutating),
                detail: first.map(|report| {
                    format!(
                        "final_retained_records={}",
                        report
                            .checkpoints
                            .last()
                            .map_or(0, |checkpoint| checkpoint.stats.retained_records)
                    )
                }),
            },
            HardGateResult {
                id: "complete_authority_state_accounted".to_owned(),
                status: gate_status(complete_accounting),
                detail: Some(format!("mode={mode_name}")),
            },
            HardGateResult {
                id: "fresh_controller_replay_is_exact".to_owned(),
                status: gate_status(exact_replay),
                detail: first.map(|report| report.structural_sha256.clone()),
            },
            HardGateResult {
                id: "expired_retry_fails_closed".to_owned(),
                status: gate_status(expired_retry_rejected),
                detail: Some(format!("mode={mode_name}")),
            },
            HardGateResult {
                id: "projected_snapshot_growth_at_most_2x".to_owned(),
                status: gate_status(
                    !matches!(
                        mode,
                        AuthorityStateScaleMode::AlignedFrontiersProjection
                            | AuthorityStateScaleMode::IdealStreamPopProjection
                    ) || bounded_state,
                ),
                detail: Some(format!(
                    "ratios={:?}",
                    reports
                        .iter()
                        .map(|report| report.growth_ratio)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "retained_only_accounting_poison_detected".to_owned(),
                status: gate_status(poison_detected),
                detail: Some(format!("anomalies={anomaly_count}")),
            },
        ],
        budget_units: reports
            .iter()
            .filter_map(|report| report.checkpoints.last())
            .map(|checkpoint| resident_count_as_f64(checkpoint.commits))
            .sum(),
        artifact_refs: vec![format!("okv-eval://authority-state-scale-v1/{mode_name}")],
        secondary_metrics,
    }
}

fn run_elastic_cold_point(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    if !backend.ends_with("local-fs") {
        return execution_from_result(Err(format!(
            "cold point-read runner requires a local-fs backend, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(
            Err("cold point-read workload requires a dataset".to_owned()),
        );
    };
    let parameter = |name: &str| -> Result<usize, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("cold point-read profile requires integer {name}"))?;
        usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1_024);
    let cold_profile = match (
        parameter("measurement_operations"),
        parameter("row_object_target_bytes"),
        parameter("row_object_block_bytes"),
    ) {
        (Ok(operations_per_repeat), Ok(target_object_bytes), Ok(target_block_bytes)) => {
            ColdReadProfile {
                key_count: dataset.key_count,
                value_bytes,
                operations_per_repeat,
                repeats: profile.repeats,
                seeds: seeds.to_vec(),
                target_object_bytes,
                target_block_bytes,
            }
        }
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return execution_from_result(Err(error));
        }
    };
    let mode = if workload.operation == "indexed_object_reader_control" {
        ColdReadMode::DirectControl
    } else if workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        == Some("scan_object_on_point_read")
    {
        ColdReadMode::ScanObjectPoison
    } else {
        ColdReadMode::Candidate
    };
    let report = match run_cold_read_profile(mode, &cold_profile) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    cold_read_execution(workload, backend, profile, mode, &report)
}

#[allow(clippy::too_many_lines)]
fn cold_read_execution(
    workload: &WorkloadConfig,
    backend: &str,
    profile: &ProfileConfig,
    mode: ColdReadMode,
    report: &ColdReadReport,
) -> WorkloadExecution {
    let correctness_passed = report.correctness_failures == 0;
    let indexed_requests_passed = report.data_range_requests == report.operations;
    let no_full_gets_passed = report.full_data_requests == 0;
    let no_list_passed = report.list_requests == 0;
    let manifest_warmup_passed = report.manifest_warmup_requests == 1
        && report.manifest_warmup_bytes == report.manifest_bytes;
    let index_warmup_passed = report.index_warmup_requests == report.segment_count
        && report.index_warmup_bytes == report.index_bytes;
    let local_budget = profile
        .parameters
        .get("local_byte_budget")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u64::try_from(value).ok());
    let metadata_bytes = report.manifest_bytes.saturating_add(report.index_bytes);
    let metadata_budget_passed = local_budget.is_some_and(|budget| metadata_bytes <= budget);
    let bounded_bytes_passed =
        report.data_response_bytes <= report.operations.saturating_mul(report.max_block_bytes);
    let error = if !correctness_passed {
        Some(format!(
            "{} cold point reads failed validation",
            report.correctness_failures
        ))
    } else if !indexed_requests_passed {
        Some(format!(
            "cold reads used {} range requests for {} operations",
            report.data_range_requests, report.operations
        ))
    } else if !no_full_gets_passed {
        Some(format!(
            "cold reads issued {} complete-object GETs",
            report.full_data_requests
        ))
    } else if !no_list_passed {
        Some(format!(
            "cold reads issued {} LIST requests",
            report.list_requests
        ))
    } else if !manifest_warmup_passed {
        Some(format!(
            "manifest warmup issued {} requests and returned {} of {} bytes",
            report.manifest_warmup_requests, report.manifest_warmup_bytes, report.manifest_bytes
        ))
    } else if !index_warmup_passed {
        Some(format!(
            "index warmup issued {} requests for {} segments and returned {} of {} bytes",
            report.index_warmup_requests,
            report.segment_count,
            report.index_warmup_bytes,
            report.index_bytes
        ))
    } else if !metadata_budget_passed {
        Some(format!(
            "cached row metadata {} exceeded local budget {}",
            metadata_bytes,
            local_budget.unwrap_or_default()
        ))
    } else if !bounded_bytes_passed {
        Some(format!(
            "cold reads transferred {} bytes beyond the block bound",
            report.data_response_bytes
        ))
    } else {
        None
    };
    let mode_name = match mode {
        ColdReadMode::Candidate => "candidate",
        ColdReadMode::DirectControl => "direct_control",
        ColdReadMode::ScanObjectPoison => "scan_object_poison",
    };
    WorkloadExecution {
        error,
        measurements: vec![
            Measurement {
                metric: "object_store.requests",
                value: report.requests_per_operation(),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "row_object"),
                    ("api", "data_get_per_operation"),
                    ("result", "attempted"),
                ]),
            },
            Measurement {
                metric: "object_store.bytes",
                value: resident_count_as_f64(report.data_response_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "row_object"),
                    ("direction", "read"),
                    ("api", "data_get"),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: resident_count_as_f64(report.correctness_failures),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "indexed-row-point-v1"),
                    (
                        "anomaly.class",
                        if correctness_passed {
                            "none"
                        } else {
                            "value_mismatch"
                        },
                    ),
                ]),
            },
        ],
        hard_gates: vec![
            HardGateResult {
                id: "exact_cold_point_reads".to_owned(),
                status: gate_status(correctness_passed),
                detail: Some(format!("mode={mode_name}")),
            },
            HardGateResult {
                id: "one_indexed_data_range_get".to_owned(),
                status: gate_status(indexed_requests_passed),
                detail: Some(format!(
                    "range_gets={},operations={}",
                    report.data_range_requests, report.operations
                )),
            },
            HardGateResult {
                id: "no_complete_object_gets".to_owned(),
                status: gate_status(no_full_gets_passed),
                detail: Some(report.full_data_requests.to_string()),
            },
            HardGateResult {
                id: "no_list_dependency".to_owned(),
                status: gate_status(no_list_passed),
                detail: Some(report.list_requests.to_string()),
            },
            HardGateResult {
                id: "single_manifest_warmup".to_owned(),
                status: gate_status(manifest_warmup_passed),
                detail: Some(format!(
                    "requests={},bytes={}",
                    report.manifest_warmup_requests, report.manifest_warmup_bytes
                )),
            },
            HardGateResult {
                id: "one_index_warmup_per_segment".to_owned(),
                status: gate_status(index_warmup_passed),
                detail: Some(format!(
                    "requests={},segments={},bytes={}",
                    report.index_warmup_requests, report.segment_count, report.index_warmup_bytes
                )),
            },
            HardGateResult {
                id: "cached_metadata_within_local_budget".to_owned(),
                status: gate_status(metadata_budget_passed),
                detail: Some(format!(
                    "actual={},budget={}",
                    metadata_bytes,
                    local_budget.unwrap_or_default()
                )),
            },
            HardGateResult {
                id: "bounded_row_block_bytes".to_owned(),
                status: gate_status(bounded_bytes_passed),
                detail: Some(format!(
                    "actual={},max_per_read={}",
                    report.data_response_bytes, report.max_block_bytes
                )),
            },
        ],
        budget_units: resident_count_as_f64(report.operations),
        artifact_refs: vec![format!("okv-eval://cold-object-row-v1/{mode_name}")],
        secondary_metrics: cold_read_secondary_metrics(report, profile.repeats),
    }
}

fn cold_read_secondary_metrics(report: &ColdReadReport, repeats: u32) -> BTreeMap<String, f64> {
    let median_latency = |select: fn(&okv_eval::cold_read::ColdReadSample) -> u64| {
        let values = report
            .samples
            .iter()
            .map(|sample| resident_count_as_f64(select(sample)))
            .collect::<Vec<_>>();
        median(&values)
    };
    BTreeMap::from([
        (
            "cold.latency_ns.p50".to_owned(),
            median_latency(|sample| sample.latency_ns_p50),
        ),
        (
            "cold.latency_ns.p99".to_owned(),
            median_latency(|sample| sample.latency_ns_p99),
        ),
        (
            "cold.requests_per_operation".to_owned(),
            report.requests_per_operation(),
        ),
        (
            "cold.bytes_per_operation".to_owned(),
            report.bytes_per_operation(),
        ),
        (
            "cold.data_object_bytes".to_owned(),
            resident_count_as_f64(report.data_object_bytes),
        ),
        (
            "cold.max_data_object_bytes".to_owned(),
            resident_count_as_f64(report.max_data_object_bytes),
        ),
        (
            "cold.segment_count".to_owned(),
            resident_count_as_f64(report.segment_count),
        ),
        (
            "cold.manifest_bytes".to_owned(),
            resident_count_as_f64(report.manifest_bytes),
        ),
        (
            "cold.index_bytes".to_owned(),
            resident_count_as_f64(report.index_bytes),
        ),
        (
            "cold.max_index_bytes".to_owned(),
            resident_count_as_f64(report.max_index_bytes),
        ),
        (
            "cold.metadata_bytes".to_owned(),
            resident_count_as_f64(report.manifest_bytes.saturating_add(report.index_bytes)),
        ),
        (
            "cold.block_count".to_owned(),
            resident_count_as_f64(report.block_count),
        ),
        (
            "cold.max_block_bytes".to_owned(),
            resident_count_as_f64(report.max_block_bytes),
        ),
        ("cold.profile_repeats".to_owned(), f64::from(repeats)),
    ])
}

fn run_empty_worker_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    if !backend.contains("local-fs") && backend != "full-local-restore" {
        return execution_from_result(Err(format!(
            "empty-worker runner requires a local filesystem backend, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err("empty-worker workload requires a dataset".to_owned()));
    };
    let parameter = |name: &str| -> Result<usize, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("empty-worker profile requires integer {name}"))?;
        usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1_024);
    let profile_config = match (
        parameter("row_object_target_bytes"),
        parameter("row_object_block_bytes"),
    ) {
        (Ok(target_object_bytes), Ok(target_block_bytes)) => ColdReadProfile {
            key_count: dataset.key_count,
            value_bytes,
            operations_per_repeat: 1,
            repeats: profile.repeats,
            seeds: seeds.to_vec(),
            target_object_bytes,
            target_block_bytes,
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = if workload.operation == "full_local_restore_control" {
        EmptyWorkerMode::FullHydrationControl
    } else if workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        == Some("hydrate_full_range_before_read")
    {
        EmptyWorkerMode::FullHydrationPoison
    } else {
        EmptyWorkerMode::LazyCandidate
    };
    let report = match run_empty_worker_profile(mode, &profile_config) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    empty_worker_execution(workload, backend, mode, &report)
}

#[allow(clippy::too_many_lines)]
fn empty_worker_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: EmptyWorkerMode,
    report: &EmptyWorkerReport,
) -> WorkloadExecution {
    let correctness_passed = report.correctness_failures == 0;
    let no_list_passed = report
        .samples
        .iter()
        .all(|sample| sample.list_requests == 0);
    let lazy_manifest_passed = report
        .samples
        .iter()
        .all(|sample| sample.manifest_requests == 1);
    let lazy_index_passed = report
        .samples
        .iter()
        .all(|sample| sample.index_requests == 1);
    let lazy_data_passed = report.samples.iter().all(|sample| {
        sample.data_range_requests == 1
            && sample.data_full_requests == 0
            && sample.hydrated_data_objects == 0
    });
    let lazy_bytes_passed = report.samples.iter().all(|sample| {
        sample.total_response_bytes
            <= report
                .manifest_bytes
                .saturating_add(report.max_index_bytes)
                .saturating_add(report.max_block_bytes)
    });
    let full_hydration_passed = report.samples.iter().all(|sample| {
        sample.manifest_requests == 1
            && sample.index_requests == report.segment_count
            && sample.data_range_requests == 0
            && sample.data_full_requests == report.segment_count
            && sample.hydrated_data_objects == report.segment_count
            && sample.index_response_bytes == report.index_bytes
            && sample.data_response_bytes == report.data_object_bytes
    });
    let expected_path_passed = match mode {
        EmptyWorkerMode::LazyCandidate | EmptyWorkerMode::FullHydrationPoison => {
            lazy_manifest_passed && lazy_index_passed && lazy_data_passed && lazy_bytes_passed
        }
        EmptyWorkerMode::FullHydrationControl => full_hydration_passed,
    };
    let error = if !correctness_passed {
        Some(format!(
            "{} empty-worker first reads failed validation",
            report.correctness_failures
        ))
    } else if !no_list_passed {
        Some("empty-worker recovery used LIST".to_owned())
    } else if !expected_path_passed {
        Some(match mode {
            EmptyWorkerMode::FullHydrationPoison => {
                "empty worker hydrated the complete range before first read".to_owned()
            }
            EmptyWorkerMode::LazyCandidate => {
                "empty worker exceeded the selected metadata and block path".to_owned()
            }
            EmptyWorkerMode::FullHydrationControl => {
                "full-restore control did not hydrate the complete closure".to_owned()
            }
        })
    } else {
        None
    };
    let mode_name = match mode {
        EmptyWorkerMode::LazyCandidate => "lazy_candidate",
        EmptyWorkerMode::FullHydrationControl => "full_hydration_control",
        EmptyWorkerMode::FullHydrationPoison => "full_hydration_poison",
    };
    let mut measurements = report
        .samples
        .iter()
        .map(|sample| Measurement {
            metric: "recovery.first_correct_read_duration",
            value: sample.first_read_seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", "row-object"),
                ("result", "attempted"),
            ]),
        })
        .collect::<Vec<_>>();
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: resident_count_as_f64(report.correctness_failures),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "empty-worker-row-point-v1"),
            (
                "anomaly.class",
                if correctness_passed {
                    "none"
                } else {
                    "value_mismatch"
                },
            ),
        ]),
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "exact_first_read".to_owned(),
            status: gate_status(correctness_passed),
            detail: Some(format!("samples={}", report.samples.len())),
        },
        HardGateResult {
            id: "no_list_dependency".to_owned(),
            status: gate_status(no_list_passed),
            detail: Some("exact named manifest root".to_owned()),
        },
    ];
    if mode == EmptyWorkerMode::FullHydrationControl {
        hard_gates.push(HardGateResult {
            id: "complete_closure_hydrated".to_owned(),
            status: gate_status(full_hydration_passed),
            detail: Some(format!(
                "segments={},data_bytes={}",
                report.segment_count, report.data_object_bytes
            )),
        });
    } else {
        hard_gates.extend([
            HardGateResult {
                id: "one_manifest_get".to_owned(),
                status: gate_status(lazy_manifest_passed),
                detail: Some(report.manifest_bytes.to_string()),
            },
            HardGateResult {
                id: "one_selected_index_get".to_owned(),
                status: gate_status(lazy_index_passed),
                detail: Some(format!("closure_segments={}", report.segment_count)),
            },
            HardGateResult {
                id: "one_data_range_get_without_hydration".to_owned(),
                status: gate_status(lazy_data_passed),
                detail: Some(format!("closure_bytes={}", report.data_object_bytes)),
            },
            HardGateResult {
                id: "selected_metadata_and_block_byte_bound".to_owned(),
                status: gate_status(lazy_bytes_passed),
                detail: Some(format!(
                    "bound={}",
                    report
                        .manifest_bytes
                        .saturating_add(report.max_index_bytes)
                        .saturating_add(report.max_block_bytes)
                )),
            },
        ]);
    }
    let durations = report
        .samples
        .iter()
        .map(|sample| sample.first_read_seconds)
        .collect::<Vec<_>>();
    let response_bytes = report
        .samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.total_response_bytes))
        .collect::<Vec<_>>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: durations.iter().sum(),
        artifact_refs: vec![format!("okv-eval://empty-worker-row-v1/{mode_name}")],
        secondary_metrics: BTreeMap::from([
            (
                "recovery.first_read_seconds.median".to_owned(),
                median(&durations),
            ),
            (
                "recovery.response_bytes.median".to_owned(),
                median(&response_bytes),
            ),
            (
                "recovery.manifest_bytes".to_owned(),
                resident_count_as_f64(report.manifest_bytes),
            ),
            (
                "recovery.index_closure_bytes".to_owned(),
                resident_count_as_f64(report.index_bytes),
            ),
            (
                "recovery.data_closure_bytes".to_owned(),
                resident_count_as_f64(report.data_object_bytes),
            ),
            (
                "recovery.segment_count".to_owned(),
                resident_count_as_f64(report.segment_count),
            ),
            (
                "recovery.max_index_bytes".to_owned(),
                resident_count_as_f64(report.max_index_bytes),
            ),
            (
                "recovery.max_block_bytes".to_owned(),
                resident_count_as_f64(report.max_block_bytes),
            ),
        ]),
    }
}

fn run_openraft_serving_worker_recovery(
    workload: &WorkloadConfig,
    run_id: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const LOCAL_BACKEND: &str = "object-store-local-fs+authority-openraft+data-openraft";
    const SSD_BACKEND: &str =
        "object-store-local-fs+authority-openraft+data-openraft+rocksdb-local-fs";
    const NATIVE_SSD_LOCAL_BACKEND: &str =
        "object-store-local-fs+authority-openraft+data-openraft+rocksdb-native-resident-local-fs";
    const SSD_GCS_BACKEND: &str =
        "object-store-gcs+authority-openraft-local-process+data-openraft-local-process+rocksdb-nvme";
    const NATIVE_SSD_GCS_BACKEND: &str =
        "object-store-gcs+authority-openraft-local-process+data-openraft-local-process+rocksdb-native-resident-nvme";
    const GCS_BACKEND: &str =
        "object-store-gcs+authority-openraft-local-process+data-openraft-local-process";
    if !matches!(
        backend,
        LOCAL_BACKEND
            | SSD_BACKEND
            | NATIVE_SSD_LOCAL_BACKEND
            | SSD_GCS_BACKEND
            | NATIVE_SSD_GCS_BACKEND
            | GCS_BACKEND
    ) {
        return execution_from_result(Err(format!(
            "OpenRaft serving recovery requires {LOCAL_BACKEND}, {SSD_BACKEND}, {NATIVE_SSD_LOCAL_BACKEND}, {SSD_GCS_BACKEND}, {NATIVE_SSD_GCS_BACKEND}, or {GCS_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "OpenRaft serving recovery requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.key_count < 32 {
        return execution_from_result(Err(
            "OpenRaft serving recovery requires at least 32 keys and fixed seeds".to_owned(),
        ));
    }
    let integer = |name: &str| -> Result<usize, String> {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("OpenRaft serving profile requires integer {name}"))
            .and_then(|value| {
                usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
            })
    };
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(128);
    let recovery_profile = match (
        integer("row_object_target_bytes"),
        integer("row_object_block_bytes"),
    ) {
        (Ok(target_object_bytes), Ok(target_block_bytes)) => ServingRecoveryProfile {
            key_count: dataset.key_count,
            value_bytes,
            target_object_bytes,
            target_block_bytes,
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let hot_read_negative_control = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        Some("mismatched_block_cache") => {
            Some(OpenRaftHotReadNegativeControl::MismatchedBlockCache)
        }
        Some("counter_reset") => Some(OpenRaftHotReadNegativeControl::CounterReset),
        Some("skip_concurrent_catchup") | None => None,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown OpenRaft serving recovery negative control {other}"
            )))
        }
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        Some("skip_concurrent_catchup") => OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison,
        Some("mismatched_block_cache" | "counter_reset") => {
            OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        }
        Some(_) => unreachable!("negative control was validated above"),
        None if workload
            .parameters
            .get("subject")
            .and_then(toml::Value::as_str)
            == Some("integrated_kernel_native_rocksdb") =>
        {
            OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        }
        None if workload
            .parameters
            .get("subject")
            .and_then(toml::Value::as_str)
            == Some("integrated_kernel_rocksdb") =>
        {
            OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
        }
        None if workload
            .parameters
            .get("subject")
            .and_then(toml::Value::as_str)
            == Some("integrated_kernel") =>
        {
            OpenRaftServingRecoveryMode::IntegratedKernelCandidate
        }
        None if workload
            .parameters
            .get("subject")
            .and_then(toml::Value::as_str)
            == Some("full_hydration") =>
        {
            OpenRaftServingRecoveryMode::FullHydrationControl
        }
        None => OpenRaftServingRecoveryMode::Candidate,
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let page_records = match integer("retained_stream_page_records") {
        Ok(value) => match u32::try_from(value) {
            Ok(value) => value,
            Err(error) => return execution_from_result(Err(error.to_string())),
        },
        Err(error) => return execution_from_result(Err(error)),
    };
    let hot_read_operations = if matches!(
        mode,
        OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
    ) {
        match (
            integer("hot_read_warmup_operations"),
            integer("hot_read_operations"),
        ) {
            (Ok(warmup), Ok(measured)) if warmup > 0 && measured > 0 => Some((warmup, measured)),
            (Ok(_), Ok(_)) => {
                return execution_from_result(Err(
                    "hot-read operation counts must be positive".to_owned()
                ))
            }
            (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
        }
    } else {
        None
    };
    let hot_read_concurrent_clients = match workload
        .parameters
        .get("concurrent_clients")
        .or_else(|| profile.parameters.get("concurrent_clients"))
        .and_then(toml::Value::as_integer)
    {
        Some(value) => match usize::try_from(value) {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                return execution_from_result(Err(
                    "hot-read concurrent clients must be positive".to_owned()
                ))
            }
            Err(error) => return execution_from_result(Err(error.to_string())),
        },
        None => 1,
    };
    let hot_read_subject = match workload
        .parameters
        .get("hot_read_subject")
        .and_then(toml::Value::as_str)
    {
        None | Some("native_snapshot") => OpenRaftHotReadSubject::NativeSnapshot,
        Some("direct_owned_rocksdb") => OpenRaftHotReadSubject::DirectOwnedRocksdb,
        Some(other) => {
            return execution_from_result(Err(format!("unknown OpenRaft hot-read subject {other}")))
        }
    };
    let hot_read_access_pattern = match workload
        .parameters
        .get("distribution")
        .and_then(toml::Value::as_str)
    {
        None | Some("hotset-80-20") => OpenRaftHotReadAccessPattern::Hotset80_20,
        Some("zipf-0.8") => OpenRaftHotReadAccessPattern::Zipf0_8,
        Some("zipf-1.4") => OpenRaftHotReadAccessPattern::Zipf1_4,
        Some("zipf-2.0") => OpenRaftHotReadAccessPattern::Zipf2_0,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown OpenRaft hot-read access pattern {other}"
            )))
        }
    };
    let hot_read_resource_limit = |name: &str, default: u64| -> Result<u64, String> {
        let declared = profile
            .workload_profile
            .as_ref()
            .and_then(|workload_profile| workload_profile.resource_limits.get(name).copied())
            .or_else(|| {
                profile
                    .parameters
                    .get(name)
                    .and_then(toml::Value::as_integer)
                    .and_then(|value| u64::try_from(value).ok())
            })
            .unwrap_or(default);
        if declared == 0 {
            Err(format!("hot-read resource limit {name} must be positive"))
        } else {
            Ok(declared)
        }
    };
    let hot_read_max_local_bytes = match hot_read_resource_limit("local_bytes", 128 * 1_024 * 1_024)
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let declared_hot_read_block_cache_bytes =
        match hot_read_resource_limit("block_cache_bytes", 128 * 1_024 * 1_024) {
            Ok(value) => value,
            Err(error) => return execution_from_result(Err(error)),
        };
    let hot_read_block_cache_bytes = if hot_read_negative_control
        == Some(OpenRaftHotReadNegativeControl::MismatchedBlockCache)
    {
        declared_hot_read_block_cache_bytes.saturating_mul(2)
    } else {
        declared_hot_read_block_cache_bytes
    };
    let reuse_fixture_across_repeats = profile
        .parameters
        .get("reuse_fixture_across_repeats")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let hot_read_direct_reads = profile
        .parameters
        .get("direct_reads")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let fixture_runs = if reuse_fixture_across_repeats {
        1
    } else {
        profile.repeats
    };
    let samples_per_fixture = if reuse_fixture_across_repeats {
        usize::try_from(profile.repeats).unwrap_or(usize::MAX)
    } else {
        1
    };
    let require_linux_process_metrics = profile
        .parameters
        .get("require_linux_process_metrics")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let mut reports = Vec::with_capacity(
        seeds
            .len()
            .saturating_mul(usize::try_from(fixture_runs).unwrap_or(usize::MAX)),
    );
    for repeat in 0..fixture_runs {
        for (index, seed) in seeds.iter().enumerate() {
            let object_backend = serving_recovery_object_backend(
                backend,
                run_id,
                &format!("primary-{repeat}-{index}"),
            );
            let hot_read = hot_read_operations.map(|(warmup_operations, measured_operations)| {
                OpenRaftHotReadProfile {
                    subject: hot_read_subject,
                    seed: *seed,
                    key_count: recovery_profile.key_count,
                    value_bytes: recovery_profile.value_bytes,
                    warmup_operations,
                    measured_operations,
                    concurrent_clients: hot_read_concurrent_clients,
                    access_pattern: hot_read_access_pattern,
                    max_local_bytes: hot_read_max_local_bytes,
                    block_cache_bytes: hot_read_block_cache_bytes,
                    direct_reads: hot_read_direct_reads,
                    sample_count: samples_per_fixture,
                    negative_control: hot_read_negative_control,
                }
            });
            match run_openraft_serving_recovery_contract_with_hot_reads(
                *seed,
                mode,
                &recovery_profile,
                page_records,
                &executable,
                object_backend,
                hot_read,
            ) {
                Ok(report) => reports.push(report),
                Err(error) => return execution_from_result(Err(error)),
            }
        }
    }
    let replay_hot_read = hot_read_operations.map(|_| OpenRaftHotReadProfile {
        subject: hot_read_subject,
        seed: seeds[0],
        key_count: recovery_profile.key_count,
        value_bytes: recovery_profile.value_bytes,
        warmup_operations: hot_read_concurrent_clients,
        measured_operations: hot_read_concurrent_clients,
        concurrent_clients: hot_read_concurrent_clients,
        access_pattern: hot_read_access_pattern,
        max_local_bytes: hot_read_max_local_bytes,
        block_cache_bytes: declared_hot_read_block_cache_bytes,
        direct_reads: hot_read_direct_reads,
        sample_count: 1,
        negative_control: None,
    });
    let replay = match run_openraft_serving_recovery_contract_with_hot_reads(
        seeds[0],
        mode,
        &recovery_profile,
        page_records,
        &executable,
        serving_recovery_object_backend(backend, run_id, "replay"),
        replay_hot_read,
    ) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    openraft_serving_recovery_execution(
        workload,
        backend,
        mode,
        &reports,
        &replay,
        OpenRaftHotReadEvaluationContract {
            declared_block_cache_bytes: declared_hot_read_block_cache_bytes,
            declared_direct_reads: hot_read_direct_reads,
            samples_per_fixture,
            reuse_fixture_across_repeats,
            require_linux_process_metrics,
            negative_control: hot_read_negative_control,
        },
    )
}

fn serving_recovery_object_backend(
    backend: &str,
    run_id: &str,
    invocation: &str,
) -> OpenRaftServingObjectBackend {
    if backend.starts_with("object-store-gcs+") {
        OpenRaftServingObjectBackend::Gcs {
            prefix: format!("scratch/single-range-kernel/{run_id}/{invocation}"),
        }
    } else {
        OpenRaftServingObjectBackend::LocalFilesystem
    }
}

fn first_hot_read_clients(reports: &[OpenRaftServingRecoveryReport]) -> Option<u64> {
    reports
        .first()
        .and_then(|report| report.process.hot_read.as_ref())
        .map(|hot_read| hot_read.concurrent_clients)
}

fn hot_storage_values(
    reports: &[OpenRaftServingRecoveryReport],
    value: impl Fn(&OpenRaftHotReadStorageReport) -> f64,
) -> Vec<f64> {
    reports
        .iter()
        .flat_map(|report| {
            report
                .process
                .hot_read
                .iter()
                .flat_map(|hot_read| hot_read.samples.iter().map(|sample| value(&sample.storage)))
        })
        .collect()
}

fn hot_read_samples(
    reports: &[OpenRaftServingRecoveryReport],
) -> Vec<&OpenRaftHotReadSampleReport> {
    reports
        .iter()
        .flat_map(|report| {
            report
                .process
                .hot_read
                .iter()
                .flat_map(|hot_read| &hot_read.samples)
        })
        .collect()
}

fn hot_read_summary_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    hot_read: &OpenRaftHotReadReport,
) -> Vec<Measurement> {
    vec![
        Measurement {
            metric: "operation.throughput",
            value: hot_read.operations_per_second,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("operation", "single_range_resident_point_read"),
                ("backend", backend),
            ]),
        },
        Measurement {
            metric: "operation.duration",
            value: resident_count_as_f64(hot_read.latency_ns_p99) / 1_000_000_000.0,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("operation", "single_range_resident_point_read"),
                ("backend", backend),
                ("result", "success"),
            ]),
        },
        Measurement {
            metric: "object_store.requests",
            value: resident_count_as_f64(hot_read.object_requests),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("store", "object_base"),
                ("api", "get"),
                ("result", "attempted"),
                ("backend", backend),
            ]),
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn hot_read_sample_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    sample: &OpenRaftHotReadSampleReport,
) -> Vec<Measurement> {
    let sample_id = sample.sample.to_string();
    let common = |extra: &[(&str, &str)]| {
        let mut values = vec![
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("sample", sample_id.as_str()),
        ];
        values.extend_from_slice(extra);
        attributes(&values)
    };
    let storage = &sample.storage;
    let process = &sample.process;
    vec![
        Measurement {
            metric: "operation.throughput",
            value: sample.operations_per_second,
            attributes: common(&[("operation", "single_range_resident_point_read")]),
        },
        Measurement {
            metric: "operation.duration",
            value: resident_count_as_f64(sample.latency_ns_p99) / 1_000_000_000.0,
            attributes: common(&[
                ("operation", "single_range_resident_point_read"),
                ("result", "success"),
            ]),
        },
        Measurement {
            metric: "object_store.requests",
            value: resident_count_as_f64(sample.object_requests),
            attributes: common(&[
                ("store", "object_base"),
                ("api", "get"),
                ("result", "attempted"),
            ]),
        },
        Measurement {
            metric: "cache.hit_ratio",
            value: storage.block_cache_hit_ratio,
            attributes: common(&[("cache.tier", "rocksdb-block-cache")]),
        },
        Measurement {
            metric: "serving.local_bytes",
            value: resident_count_as_f64(storage.block_cache_usage_bytes),
            attributes: common(&[
                ("cache.tier", "rocksdb-block-cache"),
                ("range.class", "single-range"),
            ]),
        },
        Measurement {
            metric: "rocksdb.cache_requests",
            value: resident_count_as_f64(storage.block_cache_hits),
            attributes: common(&[("block.class", "all"), ("result", "hit")]),
        },
        Measurement {
            metric: "rocksdb.cache_requests",
            value: resident_count_as_f64(storage.block_cache_misses),
            attributes: common(&[("block.class", "all"), ("result", "miss")]),
        },
        Measurement {
            metric: "rocksdb.read_bytes",
            value: resident_count_as_f64(storage.bytes_read),
            attributes: common(&[("counter.class", "bytes-read")]),
        },
        Measurement {
            metric: "rocksdb.read_bytes",
            value: resident_count_as_f64(storage.block_cache_bytes_read),
            attributes: common(&[("counter.class", "block-cache-bytes-read")]),
        },
        Measurement {
            metric: "rocksdb.read_bytes",
            value: resident_count_as_f64(storage.read_amp_useful_bytes),
            attributes: common(&[("counter.class", "read-amp-useful")]),
        },
        Measurement {
            metric: "rocksdb.read_bytes",
            value: resident_count_as_f64(storage.read_amp_total_bytes),
            attributes: common(&[("counter.class", "read-amp-total")]),
        },
        Measurement {
            metric: "rocksdb.read_amplification",
            value: storage.read_amplification_ratio,
            attributes: common(&[]),
        },
        Measurement {
            metric: "compute.cpu_duration",
            value: resident_count_as_f64(process.user_cpu_nanoseconds) / 1_000_000_000.0,
            attributes: common(&[("component", "serving-worker"), ("cpu.class", "user")]),
        },
        Measurement {
            metric: "compute.cpu_duration",
            value: resident_count_as_f64(process.system_cpu_nanoseconds) / 1_000_000_000.0,
            attributes: common(&[("component", "serving-worker"), ("cpu.class", "system")]),
        },
        Measurement {
            metric: "compute.cpu_duration",
            value: resident_count_as_f64(process.total_cpu_nanoseconds) / 1_000_000_000.0,
            attributes: common(&[("component", "serving-worker"), ("cpu.class", "total")]),
        },
        Measurement {
            metric: "process.cpu_per_operation",
            value: process.cpu_nanoseconds_per_read,
            attributes: common(&[("operation", "single_range_resident_point_read")]),
        },
        Measurement {
            metric: "memory.resident_bytes",
            value: resident_count_as_f64(process.rss_before_warmup_bytes),
            attributes: common(&[("component", "serving-worker"), ("phase", "before-warmup")]),
        },
        Measurement {
            metric: "memory.resident_bytes",
            value: resident_count_as_f64(process.rss_after_warmup_bytes),
            attributes: common(&[("component", "serving-worker"), ("phase", "after-warmup")]),
        },
        Measurement {
            metric: "memory.resident_bytes",
            value: resident_count_as_f64(process.rss_after_measurement_bytes),
            attributes: common(&[
                ("component", "serving-worker"),
                ("phase", "after-measurement"),
            ]),
        },
        Measurement {
            metric: "memory.resident_bytes",
            value: resident_count_as_f64(process.peak_rss_bytes),
            attributes: common(&[("component", "serving-worker"), ("phase", "peak")]),
        },
        Measurement {
            metric: "process.io_bytes",
            value: resident_count_as_f64(process.logical_read_bytes),
            attributes: common(&[("io.class", "logical-read")]),
        },
        Measurement {
            metric: "process.io_bytes",
            value: resident_count_as_f64(process.logical_write_bytes),
            attributes: common(&[("io.class", "logical-write")]),
        },
        Measurement {
            metric: "process.io_bytes",
            value: resident_count_as_f64(process.physical_read_bytes),
            attributes: common(&[("io.class", "physical-read")]),
        },
        Measurement {
            metric: "process.io_bytes",
            value: resident_count_as_f64(process.physical_write_bytes),
            attributes: common(&[("io.class", "physical-write")]),
        },
        Measurement {
            metric: "network.bytes",
            value: resident_count_as_f64(process.host_network_rx_bytes),
            attributes: common(&[
                ("peer.class", "host-network-namespace"),
                ("direction", "receive"),
            ]),
        },
        Measurement {
            metric: "network.bytes",
            value: resident_count_as_f64(process.host_network_tx_bytes),
            attributes: common(&[
                ("peer.class", "host-network-namespace"),
                ("direction", "transmit"),
            ]),
        },
    ]
}

#[derive(Clone, Copy)]
struct OpenRaftHotReadEvaluationContract {
    declared_block_cache_bytes: u64,
    declared_direct_reads: bool,
    samples_per_fixture: usize,
    reuse_fixture_across_repeats: bool,
    require_linux_process_metrics: bool,
    negative_control: Option<OpenRaftHotReadNegativeControl>,
}

#[allow(clippy::too_many_lines)]
fn openraft_serving_recovery_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: OpenRaftServingRecoveryMode,
    reports: &[OpenRaftServingRecoveryReport],
    replay: &OpenRaftServingRecoveryReport,
    contract: OpenRaftHotReadEvaluationContract,
) -> WorkloadExecution {
    let OpenRaftHotReadEvaluationContract {
        declared_block_cache_bytes,
        declared_direct_reads,
        samples_per_fixture,
        reuse_fixture_across_repeats,
        require_linux_process_metrics,
        negative_control,
    } = contract;
    let anomaly_count = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let process_boundary = reports.iter().all(|report| {
        report.authority_processes == 6
            && report.worker_process_starts == 2
            && report.worker_process_kills == 1
            && report.empty_scratch_restarts == 1
    });
    let two_round_catchup = reports.iter().all(|report| {
        report.concurrent_commits == 4
            && report.process.catchup_rounds == 2
            && report.process.initial_records_applied == 3
            && report.process.concurrent_records_observed == 4
            && report.process.initial_target_version < report.process.activation_target_version
    });
    let expected_apply = reports.iter().all(|report| match mode {
        OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison => {
            report.process.concurrent_records_applied == 0
        }
        OpenRaftServingRecoveryMode::Candidate
        | OpenRaftServingRecoveryMode::IntegratedKernelCandidate
        | OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
        | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        | OpenRaftServingRecoveryMode::FullHydrationControl => {
            report.process.concurrent_records_applied == 4
        }
    });
    let standalone_direct_control = mode
        == OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        && !reports.is_empty()
        && reports.iter().all(|report| {
            report.process.hot_read.as_ref().is_some_and(|hot_read| {
                hot_read.subject == OpenRaftHotReadSubject::DirectOwnedRocksdb
            })
        });
    let minimum_txlog_reads = if standalone_direct_control { 2 } else { 4 };
    let journal_independent = reports.iter().all(|report| {
        report.process.physical_wal_path_accesses == 0
            && report.process.txlog_read_requests >= minimum_txlog_reads
            && report.process.txlog_response_payload_bytes > 0
    });
    let authority_stable = reports.iter().all(|report| {
        report.process.generation_sandwich_stable
            && report.process.generation == 7
            && report.process.logical_txlog_root == "wal-g7"
            && report.process.manifest_authoritative
    });
    let no_list = reports
        .iter()
        .all(|report| report.process.list_requests == 0);
    let lazy_path = reports.iter().all(|report| {
        report.process.manifest_requests == 1
            && report.process.index_requests == 1
            && report.process.data_range_requests == 1
            && report.process.data_full_requests == 0
    });
    let full_path = reports.iter().all(|report| {
        report.process.manifest_requests == 1
            && report.process.index_requests == report.process.row_segment_count
            && report.process.data_range_requests == 0
            && report.process.data_full_requests == report.process.row_segment_count
    });
    let expected_path = match mode {
        OpenRaftServingRecoveryMode::FullHydrationControl
        | OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
        | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate => full_path,
        OpenRaftServingRecoveryMode::Candidate
        | OpenRaftServingRecoveryMode::IntegratedKernelCandidate
        | OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison => lazy_path,
    };
    let integrated_kernel = matches!(
        mode,
        OpenRaftServingRecoveryMode::IntegratedKernelCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
    );
    let batch_cursor_exact = !integrated_kernel
        || reports
            .iter()
            .all(|report| report.process.batch_cursor_resumes >= 2);
    let serving_image_exact = mode != OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
        || reports.iter().all(|report| {
            report.process.serving_image_provider.as_deref() == Some("rocksdb-11.8.1")
                && report.process.serving_image_records > 0
                && report.process.serving_image_local_bytes > 0
                && report.process.serving_image_local_bytes <= 128 * 1_024 * 1_024
        });
    let resident_engine_exact = mode
        != OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        || reports.iter().all(|report| {
            report
                .process
                .hot_read
                .as_ref()
                .is_some_and(|hot_read| match hot_read.subject {
                    OpenRaftHotReadSubject::NativeSnapshot => {
                        report.process.resident_engine_provider.as_deref()
                            == Some(NATIVE_RESIDENT_PROVIDER)
                            && report.process.resident_engine_records > 0
                            && report.process.resident_engine_local_bytes > 0
                            && report.process.resident_engine_local_bytes
                                <= hot_read.max_local_bytes
                            && report.process.resident_engine_applied_version
                                == report.process.activation_target_version
                    }
                    OpenRaftHotReadSubject::DirectOwnedRocksdb => {
                        report.process.resident_engine_provider.is_none()
                            && report.process.resident_engine_records == 0
                            && report.process.resident_engine_local_bytes == 0
                            && report.process.resident_engine_applied_version == 0
                    }
                })
        });
    let resident_hot_mode = matches!(
        mode,
        OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
    );
    let hot_samples = hot_read_samples(reports);
    let fixture_reuse_exact = !resident_hot_mode
        || (samples_per_fixture > 0
            && reports.iter().all(|report| {
                report.process.hot_read.as_ref().is_some_and(|hot_read| {
                    hot_read.samples.len() == samples_per_fixture
                        && hot_read.samples.iter().enumerate().all(|(index, sample)| {
                            sample.sample == u64::try_from(index).unwrap_or(u64::MAX)
                        })
                })
            })
            && (!reuse_fixture_across_repeats || samples_per_fixture > 1));
    let hot_read_exact = !resident_hot_mode
        || (!hot_samples.is_empty()
            && reports.iter().all(|report| {
                report.process.hot_read.as_ref().is_some_and(|hot_read| {
                    hot_read.measured_operations > 0
                        && hot_read.concurrent_clients > 0
                        && hot_read.concurrent_clients <= 256
                        && hot_read.trace_sha256.len() == 64
                        && hot_read.correctness_failures == 0
                        && hot_read.object_requests == 0
                        && hot_read.operations_per_second.is_finite()
                        && hot_read.operations_per_second > 0.0
                })
            })
            && hot_samples.iter().all(|sample| {
                sample.correctness_failures == 0
                    && sample.object_requests == 0
                    && sample.operations_per_second.is_finite()
                    && sample.operations_per_second > 0.0
                    && sample.latency_ns_max >= sample.latency_ns_p999
            }));
    let hot_read_storage_exact = mode
        != OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
        || (!hot_samples.is_empty()
            && hot_samples.iter().all(|sample| {
                sample.storage.block_cache_capacity_bytes == declared_block_cache_bytes
                    && sample.storage.direct_reads == declared_direct_reads
                    && sample.storage.block_cache_usage_bytes
                        <= declared_block_cache_bytes
                            .saturating_mul(105)
                            .saturating_div(100)
                    && sample.storage.block_cache_hit_ratio.is_finite()
                    && sample.storage.read_amplification_ratio.is_finite()
            }));
    let measured_counter_deltas_exact = !resident_hot_mode
        || (!hot_samples.is_empty() && hot_samples.iter().all(|sample| sample.counter_delta_valid));
    let process_metrics_exact = !resident_hot_mode
        || (!hot_samples.is_empty()
            && hot_samples.iter().all(|sample| {
                sample.process.process_cpu_supported
                    && sample.process.cpu_nanoseconds_per_read.is_finite()
                    && sample.process.cpu_nanoseconds_per_read > 0.0
                    && (!require_linux_process_metrics || sample.process.linux_proc_supported)
            }));
    let hot_read_clients_exact = first_hot_read_clients(reports).is_some_and(|clients| {
        clients > 0
            && clients <= 256
            && reports.iter().all(|report| {
                report
                    .process
                    .hot_read
                    .as_ref()
                    .is_some_and(|hot_read| hot_read.concurrent_clients == clients)
            })
    });
    let exact_values = anomaly_count == 0 && reports.iter().all(|report| report.exact_replay);
    let error = if !exact_values {
        Some(format!(
            "OpenRaft replacement returned {anomaly_count} incorrect held-out reads"
        ))
    } else if !process_boundary {
        Some("OpenRaft replacement did not cross the frozen process boundary".to_owned())
    } else if !two_round_catchup || !expected_apply || !batch_cursor_exact {
        Some("OpenRaft replacement did not execute the frozen two-round catch-up".to_owned())
    } else if !journal_independent {
        Some("OpenRaft replacement did not use the logical retained-stream API".to_owned())
    } else if !authority_stable
        || !no_list
        || !expected_path
        || !serving_image_exact
        || !resident_engine_exact
    {
        Some("OpenRaft replacement exceeded its authoritative object-read path".to_owned())
    } else if !hot_read_exact || (resident_hot_mode && !hot_read_clients_exact) {
        Some("resident hot-read window did not preserve its declared execution contract".to_owned())
    } else if !hot_read_storage_exact {
        Some("resident hot-read window did not preserve its declared block-cache budget".to_owned())
    } else if !measured_counter_deltas_exact {
        Some("resident hot-read counters were not valid measured-window deltas".to_owned())
    } else if !process_metrics_exact {
        Some("resident hot-read process resources were not attributable".to_owned())
    } else if !fixture_reuse_exact {
        Some("resident fixture was not reused across the declared samples".to_owned())
    } else if !exact_replay {
        Some("fresh-process OpenRaft recovery changed its semantic digest".to_owned())
    } else if negative_control.is_some() {
        Some("OpenRaft negative control was not rejected".to_owned())
    } else {
        None
    };
    let measurements = reports
        .iter()
        .flat_map(|report| {
            let mut measurements = vec![
                Measurement {
                    metric: "recovery.first_correct_read_duration",
                    value: report.process.first_read_seconds,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("dataset.class", "row-object-plus-openraft-retained-stream"),
                        ("result", "attempted"),
                    ]),
                },
                Measurement {
                    metric: "correctness.anomalies",
                    value: resident_count_as_f64(report.correctness_anomalies),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "object-base-plus-openraft-tail-v1"),
                        (
                            "anomaly.class",
                            if report.correctness_anomalies == 0 {
                                "none"
                            } else {
                                "recovery_value_mismatch"
                            },
                        ),
                    ]),
                },
            ];
            if let Some(hot_read) = report.process.hot_read.as_ref() {
                if hot_read.samples.is_empty() {
                    measurements.extend(hot_read_summary_measurements(workload, backend, hot_read));
                } else {
                    for sample in &hot_read.samples {
                        measurements
                            .extend(hot_read_sample_measurements(workload, backend, sample));
                    }
                }
            }
            measurements
        })
        .collect::<Vec<_>>();
    let first = reports.first();
    let mut hard_gates = vec![
        HardGateResult {
            id: "versionstamp_cursor_preserves_shared_commit_version".to_owned(),
            status: gate_status(batch_cursor_exact),
            detail: first.map(|report| {
                format!(
                    "batch_cursor_resumes={}",
                    report.process.batch_cursor_resumes
                )
            }),
        },
        HardGateResult {
            id: "six_authority_processes_and_empty_replacement".to_owned(),
            status: gate_status(process_boundary),
            detail: Some("publication=3,data=3,workers=2,kills=1".to_owned()),
        },
        HardGateResult {
            id: "two_frozen_catchup_rounds_while_commits_continue".to_owned(),
            status: gate_status(two_round_catchup),
            detail: first.map(|report| {
                format!(
                    "initial={},activation={},concurrent={}",
                    report.process.initial_target_version,
                    report.process.activation_target_version,
                    report.concurrent_commits
                )
            }),
        },
        HardGateResult {
            id: "required_concurrent_suffix_applied".to_owned(),
            status: gate_status(
                expected_apply && mode != OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison,
            ),
            detail: first.map(|report| {
                format!(
                    "applied={}/{}",
                    report.process.concurrent_records_applied,
                    report.process.concurrent_records_observed
                )
            }),
        },
        HardGateResult {
            id: "no_physical_raft_journal_access".to_owned(),
            status: gate_status(journal_independent),
            detail: first.map(|report| {
                format!(
                    "stream_requests={},payload_bytes={},path_accesses={}",
                    report.process.txlog_read_requests,
                    report.process.txlog_response_payload_bytes,
                    report.process.physical_wal_path_accesses
                )
            }),
        },
        HardGateResult {
            id: "exact_set_clear_insert_and_clear_range".to_owned(),
            status: gate_status(exact_values),
            detail: Some(format!("anomalies={anomaly_count}")),
        },
        HardGateResult {
            id: "bounded_named_object_path".to_owned(),
            status: gate_status(authority_stable && no_list && expected_path),
            detail: first.map(|report| {
                format!(
                    "manifest={},index={},range={},full={},list={}",
                    report.process.manifest_requests,
                    report.process.index_requests,
                    report.process.data_range_requests,
                    report.process.data_full_requests,
                    report.process.list_requests
                )
            }),
        },
        HardGateResult {
            id: "fresh_process_semantic_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: first.map(|report| report.semantic_sha256.clone()),
        },
    ];
    if mode == OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate {
        hard_gates.push(HardGateResult {
            id: "bounded_rocksdb_serving_image_activated".to_owned(),
            status: gate_status(serving_image_exact),
            detail: first.map(|report| {
                format!(
                    "provider={},records={},local_bytes={}",
                    report
                        .process
                        .serving_image_provider
                        .as_deref()
                        .unwrap_or("absent"),
                    report.process.serving_image_records,
                    report.process.serving_image_local_bytes
                )
            }),
        });
        hard_gates.push(HardGateResult {
            id: "exact_public_kernel_hot_reads".to_owned(),
            status: gate_status(hot_read_exact),
            detail: first.and_then(|report| {
                report.process.hot_read.as_ref().map(|hot_read| {
                    format!(
                        "operations={},trace={},pattern={},failures={},p99_ns={}",
                        hot_read.measured_operations,
                        hot_read.trace_sha256,
                        hot_read.access_pattern.id(),
                        hot_read.correctness_failures,
                        hot_read.latency_ns_p99
                    )
                })
            }),
        });
        hard_gates.push(HardGateResult {
            id: "post_activation_reads_issue_zero_object_operations".to_owned(),
            status: gate_status(hot_read_exact),
            detail: first.and_then(|report| {
                report
                    .process
                    .hot_read
                    .as_ref()
                    .map(|hot_read| hot_read.object_requests.to_string())
            }),
        });
    }
    if mode == OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate {
        let hot_read_subject = first
            .and_then(|report| report.process.hot_read.as_ref())
            .map(|hot_read| hot_read.subject)
            .unwrap_or_default();
        let (read_gate, object_gate, ownership_gate, ownership_detail) = match hot_read_subject {
            OpenRaftHotReadSubject::NativeSnapshot => (
                "exact_bound_native_snapshot_hot_reads",
                "bound_native_snapshot_issues_zero_object_operations",
                "native_snapshot_returns_owned_value",
                "ReadOutcome::Value(Vec<u8>)",
            ),
            OpenRaftHotReadSubject::DirectOwnedRocksdb => (
                "exact_matched_topology_direct_rocksdb_hot_reads",
                "matched_topology_direct_rocksdb_issues_zero_object_operations",
                "matched_topology_direct_rocksdb_returns_owned_value",
                "DB::get -> Option<Vec<u8>>",
            ),
        };
        let resident_topology_gate = match hot_read_subject {
            OpenRaftHotReadSubject::NativeSnapshot => {
                "native_resident_engine_activated_and_advanced"
            }
            OpenRaftHotReadSubject::DirectOwnedRocksdb => {
                "direct_control_has_zero_native_resident_engines"
            }
        };
        hard_gates.push(HardGateResult {
            id: resident_topology_gate.to_owned(),
            status: gate_status(resident_engine_exact),
            detail: first.map(|report| {
                format!(
                    "provider={},records={},local_bytes={},applied={}",
                    report
                        .process
                        .resident_engine_provider
                        .as_deref()
                        .unwrap_or("absent"),
                    report.process.resident_engine_records,
                    report.process.resident_engine_local_bytes,
                    report.process.resident_engine_applied_version
                )
            }),
        });
        hard_gates.push(HardGateResult {
            id: read_gate.to_owned(),
            status: gate_status(hot_read_exact),
            detail: first.and_then(|report| {
                report.process.hot_read.as_ref().map(|hot_read| {
                    format!(
                        "operations={},failures={},p99_ns={}",
                        hot_read.measured_operations,
                        hot_read.correctness_failures,
                        hot_read.latency_ns_p99
                    )
                })
            }),
        });
        hard_gates.push(HardGateResult {
            id: object_gate.to_owned(),
            status: gate_status(hot_read_exact),
            detail: first.and_then(|report| {
                report
                    .process
                    .hot_read
                    .as_ref()
                    .map(|hot_read| hot_read.object_requests.to_string())
            }),
        });
        hard_gates.push(HardGateResult {
            id: ownership_gate.to_owned(),
            status: gate_status(true),
            detail: Some(ownership_detail.to_owned()),
        });
        hard_gates.push(HardGateResult {
            id: "bounded_concurrent_hot_read_clients_executed".to_owned(),
            status: gate_status(hot_read_clients_exact),
            detail: first_hot_read_clients(reports).map(|clients| clients.to_string()),
        });
        hard_gates.push(HardGateResult {
            id: "measured_window_rocksdb_cache_counters".to_owned(),
            status: gate_status(hot_read_storage_exact),
            detail: first.and_then(|report| {
                report
                    .process
                    .hot_read
                    .as_ref()
                    .and_then(|hot_read| hot_read.storage.as_ref())
                    .map(|storage| {
                        format!(
                            "capacity={},usage={},hits={},misses={},bytes_read={},amp={}",
                            storage.block_cache_capacity_bytes,
                            storage.block_cache_usage_bytes,
                            storage.block_cache_hits,
                            storage.block_cache_misses,
                            storage.bytes_read,
                            storage.read_amplification_ratio
                        )
                    })
            }),
        });
        hard_gates.push(HardGateResult {
            id: "rocksdb_direct_read_mode_matches_profile".to_owned(),
            status: gate_status(hot_read_storage_exact),
            detail: Some(format!("direct_reads={declared_direct_reads}")),
        });
        hard_gates.push(HardGateResult {
            id: "fixture_reused_across_independent_samples".to_owned(),
            status: gate_status(fixture_reuse_exact),
            detail: Some(format!(
                "fixture_runs={},samples_per_fixture={},reuse={reuse_fixture_across_repeats}",
                reports.len(),
                samples_per_fixture
            )),
        });
        hard_gates.push(HardGateResult {
            id: "measured_window_counter_deltas_valid".to_owned(),
            status: gate_status(measured_counter_deltas_exact),
            detail: Some(format!(
                "valid={}/{}",
                hot_samples
                    .iter()
                    .filter(|sample| sample.counter_delta_valid)
                    .count(),
                hot_samples.len()
            )),
        });
        hard_gates.push(HardGateResult {
            id: "process_cpu_and_linux_io_attributed".to_owned(),
            status: gate_status(process_metrics_exact),
            detail: Some(format!(
                "process_cpu={}/{},linux_proc={}/{},linux_required={require_linux_process_metrics}",
                hot_samples
                    .iter()
                    .filter(|sample| sample.process.process_cpu_supported)
                    .count(),
                hot_samples.len(),
                hot_samples
                    .iter()
                    .filter(|sample| sample.process.linux_proc_supported)
                    .count(),
                hot_samples.len()
            )),
        });
    }
    let durations = reports
        .iter()
        .map(|report| report.process.first_read_seconds)
        .collect::<Vec<_>>();
    let object_bytes = reports
        .iter()
        .map(|report| resident_count_as_f64(report.process.total_object_response_bytes))
        .collect::<Vec<_>>();
    let stream_bytes = reports
        .iter()
        .map(|report| resident_count_as_f64(report.process.txlog_response_payload_bytes))
        .collect::<Vec<_>>();
    let hot_throughput = hot_samples
        .iter()
        .map(|sample| sample.operations_per_second)
        .collect::<Vec<_>>();
    let hot_p99_ns = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.latency_ns_p99))
        .collect::<Vec<_>>();
    let hot_p50_ns = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.latency_ns_p50))
        .collect::<Vec<_>>();
    let hot_p95_ns = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.latency_ns_p95))
        .collect::<Vec<_>>();
    let hot_p99_9_ns = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.latency_ns_p999))
        .collect::<Vec<_>>();
    let hot_concurrent_clients = reports
        .iter()
        .filter_map(|report| {
            report
                .process
                .hot_read
                .as_ref()
                .map(|hot_read| resident_count_as_f64(hot_read.concurrent_clients))
        })
        .collect::<Vec<_>>();
    let hot_cache_capacity_bytes = hot_storage_values(reports, |storage| {
        resident_count_as_f64(storage.block_cache_capacity_bytes)
    });
    let hot_cache_usage_bytes = hot_storage_values(reports, |storage| {
        resident_count_as_f64(storage.block_cache_usage_bytes)
    });
    let hot_cache_hit_ratio = hot_storage_values(reports, |storage| storage.block_cache_hit_ratio);
    let hot_bytes_read =
        hot_storage_values(reports, |storage| resident_count_as_f64(storage.bytes_read));
    let hot_read_amplification =
        hot_storage_values(reports, |storage| storage.read_amplification_ratio);
    let hot_cpu_ns_per_read = hot_samples
        .iter()
        .map(|sample| sample.process.cpu_nanoseconds_per_read)
        .collect::<Vec<_>>();
    let measured_operations_per_sample = reports
        .first()
        .and_then(|report| report.process.hot_read.as_ref())
        .map_or(0, |hot_read| hot_read.measured_operations);
    let hot_physical_read_bytes_per_read = hot_samples
        .iter()
        .map(|sample| {
            ratio_as_f64(
                sample.process.physical_read_bytes,
                measured_operations_per_sample,
            )
        })
        .collect::<Vec<_>>();
    let hot_logical_read_bytes_per_read = hot_samples
        .iter()
        .map(|sample| {
            ratio_as_f64(
                sample.process.logical_read_bytes,
                measured_operations_per_sample,
            )
        })
        .collect::<Vec<_>>();
    let hot_rss_after_measurement = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.process.rss_after_measurement_bytes))
        .collect::<Vec<_>>();
    let hot_peak_rss = hot_samples
        .iter()
        .map(|sample| resident_count_as_f64(sample.process.peak_rss_bytes))
        .collect::<Vec<_>>();
    let mut secondary_metrics = BTreeMap::from([
        (
            "serving_recovery_openraft.first_read_seconds.median".to_owned(),
            median(&durations),
        ),
        (
            "serving_recovery_openraft.object_response_bytes.median".to_owned(),
            median(&object_bytes),
        ),
        (
            "serving_recovery_openraft.txlog_payload_bytes.median".to_owned(),
            median(&stream_bytes),
        ),
        (
            "serving_recovery_openraft.correctness_anomalies".to_owned(),
            resident_count_as_f64(anomaly_count),
        ),
        (
            "serving_recovery_openraft.exact_replay".to_owned(),
            if exact_replay { 1.0 } else { 0.0 },
        ),
    ]);
    if !hot_throughput.is_empty() {
        secondary_metrics.insert(
            "single_range.hot_read_throughput.median".to_owned(),
            median(&hot_throughput),
        );
        secondary_metrics.insert(
            "single_range.hot_read_p50_ns.median".to_owned(),
            median(&hot_p50_ns),
        );
        secondary_metrics.insert(
            "single_range.hot_read_p95_ns.median".to_owned(),
            median(&hot_p95_ns),
        );
        secondary_metrics.insert(
            "single_range.hot_read_p99_ns.median".to_owned(),
            median(&hot_p99_ns),
        );
        secondary_metrics.insert(
            "single_range.hot_read_p999_ns.median".to_owned(),
            median(&hot_p99_9_ns),
        );
        secondary_metrics.insert(
            "single_range.hot_read_concurrent_clients.median".to_owned(),
            median(&hot_concurrent_clients),
        );
        secondary_metrics.insert(
            "single_range.fixture_runs".to_owned(),
            usize_as_f64(reports.len()),
        );
        secondary_metrics.insert(
            "single_range.measured_samples".to_owned(),
            usize_as_f64(hot_samples.len()),
        );
        secondary_metrics.insert(
            "single_range.process_cpu_ns_per_read.median".to_owned(),
            median(&hot_cpu_ns_per_read),
        );
        secondary_metrics.insert(
            "single_range.process_physical_read_bytes_per_read.median".to_owned(),
            median(&hot_physical_read_bytes_per_read),
        );
        secondary_metrics.insert(
            "single_range.process_logical_read_bytes_per_read.median".to_owned(),
            median(&hot_logical_read_bytes_per_read),
        );
        secondary_metrics.insert(
            "single_range.process_rss_after_measurement_bytes.median".to_owned(),
            median(&hot_rss_after_measurement),
        );
        secondary_metrics.insert(
            "single_range.process_peak_rss_bytes.maximum".to_owned(),
            hot_peak_rss.iter().copied().reduce(f64::max).unwrap_or(0.0),
        );
        if !hot_cache_capacity_bytes.is_empty() {
            secondary_metrics.insert(
                "single_range.block_cache_capacity_bytes.median".to_owned(),
                median(&hot_cache_capacity_bytes),
            );
            secondary_metrics.insert(
                "single_range.block_cache_usage_bytes.median".to_owned(),
                median(&hot_cache_usage_bytes),
            );
            secondary_metrics.insert(
                "single_range.block_cache_hit_ratio.median".to_owned(),
                median(&hot_cache_hit_ratio),
            );
            secondary_metrics.insert(
                "single_range.rocksdb_bytes_read.median".to_owned(),
                median(&hot_bytes_read),
            );
            secondary_metrics.insert(
                "single_range.rocksdb_read_amplification.median".to_owned(),
                median(&hot_read_amplification),
            );
        }
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: if hot_throughput.is_empty() {
            durations.iter().sum()
        } else {
            reports
                .iter()
                .filter_map(|report| report.process.hot_read.as_ref())
                .map(|hot_read| {
                    resident_count_as_f64(hot_read.measured_operations)
                        * usize_as_f64(hot_read.samples.len())
                })
                .sum()
        },
        artifact_refs: reports
            .iter()
            .map(|report| {
                let trace_sha256 = report
                    .process
                    .hot_read
                    .as_ref()
                    .map_or("no-hot-read", |hot_read| hot_read.trace_sha256.as_str());
                format!(
                    "okv-eval://serving-recovery-openraft-v1/{}/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256,
                    trace_sha256
                )
            })
            .collect(),
        secondary_metrics,
    }
}

fn run_serving_worker_process_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "object-store-local-fs+authority-openraft+quorum-wal-files";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "serving process recovery requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(
            Err("serving process recovery requires a dataset".to_owned()),
        );
    };
    if seeds.is_empty() || dataset.key_count == 0 {
        return execution_from_result(Err(
            "serving process recovery requires keys and fixed seeds".to_owned(),
        ));
    }
    let parameter = |name: &str| -> Result<usize, String> {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("serving recovery profile requires integer {name}"))
            .and_then(|value| {
                usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
            })
    };
    let inferred_value_bytes = dataset
        .logical_bytes
        .checked_div(dataset.key_count)
        .and_then(|bytes| bytes.checked_sub(8))
        .and_then(|bytes| usize::try_from(bytes).ok())
        .unwrap_or(1_024)
        .max(16);
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(inferred_value_bytes);
    let recovery_profile = match (
        parameter("row_object_target_bytes"),
        parameter("row_object_block_bytes"),
    ) {
        (Ok(target_object_bytes), Ok(target_block_bytes)) => ServingRecoveryProfile {
            key_count: dataset.key_count,
            value_bytes,
            target_object_bytes,
            target_block_bytes,
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        Some("skip_tail_replay") => ServingRecoveryMode::SkipTailPoison,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown serving recovery negative control {other}"
            )))
        }
        None if workload
            .parameters
            .get("subject")
            .and_then(toml::Value::as_str)
            == Some("full_hydration") =>
        {
            ServingRecoveryMode::FullHydrationControl
        }
        None => ServingRecoveryMode::Candidate,
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_serving_recovery_contract(*seed, mode, &recovery_profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_serving_recovery_contract(seeds[0], mode, &recovery_profile, &executable)
    {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    serving_recovery_execution(workload, backend, mode, &reports, &replay)
}

#[allow(clippy::too_many_lines)]
fn serving_recovery_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: ServingRecoveryMode,
    reports: &[ServingRecoveryReport],
    replay: &ServingRecoveryReport,
) -> WorkloadExecution {
    let anomaly_count = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let process_boundary = reports.iter().all(|report| {
        report.authority_processes == 3
            && report.worker_process_starts == 2
            && report.worker_process_kills == 1
            && report.empty_scratch_restarts == 1
            && report.process.scratch_was_empty
    });
    let authority_stable = reports.iter().all(|report| {
        report.process.generation_sandwich_stable
            && report.process.generation == 7
            && report.process.transaction_system_id == "tx-g7"
            && report.process.logical_wal_root == "wal-g7"
            && report.process.manifest_authoritative
    });
    let durable_tail = reports.iter().all(|report| {
        report.process.txlog_records_recovered >= 4
            && report.process.txlog_tail_records == 3
            && report.process.txlog_physical_bytes > 0
    });
    let tail_applied = reports.iter().all(|report| match mode {
        ServingRecoveryMode::SkipTailPoison => report.process.txlog_tail_records_applied == 0,
        ServingRecoveryMode::Candidate | ServingRecoveryMode::FullHydrationControl => {
            report.process.txlog_tail_records_applied == report.process.txlog_tail_records
        }
    });
    let no_list = reports
        .iter()
        .all(|report| report.process.list_requests == 0);
    let lazy_path = reports.iter().all(|report| {
        report.process.manifest_requests == 1
            && report.process.index_requests == 1
            && report.process.data_range_requests == 1
            && report.process.data_full_requests == 0
            && report.process.total_object_response_bytes
                <= report
                    .process
                    .manifest_response_bytes
                    .saturating_add(report.process.index_response_bytes)
                    .saturating_add(report.process.data_response_bytes)
    });
    let full_path = reports.iter().all(|report| {
        report.process.manifest_requests == 1
            && report.process.index_requests == report.process.row_segment_count
            && report.process.data_range_requests == 0
            && report.process.data_full_requests == report.process.row_segment_count
            && report.process.index_response_bytes == report.process.row_index_closure_bytes
            && report.process.data_response_bytes == report.process.row_data_closure_bytes
    });
    let exact_values = reports.iter().all(|report| {
        report.correctness_anomalies == 0
            && report.exact_base_read
            && report.exact_tail_update
            && report.exact_tail_delete
            && report.exact_tail_insert
    });
    let expected_path = match mode {
        ServingRecoveryMode::Candidate | ServingRecoveryMode::SkipTailPoison => lazy_path,
        ServingRecoveryMode::FullHydrationControl => full_path,
    };
    let error = if !exact_values {
        Some(format!(
            "serving replacement returned {anomaly_count} incorrect base or tail reads"
        ))
    } else if !process_boundary {
        Some("serving replacement did not cross the frozen process boundary".to_owned())
    } else if !authority_stable {
        Some("serving replacement did not open one stable authoritative root".to_owned())
    } else if !durable_tail || !tail_applied {
        Some("serving replacement did not reconstruct the required txLog suffix".to_owned())
    } else if !no_list {
        Some("serving replacement used object LIST for recovery".to_owned())
    } else if !expected_path {
        Some(match mode {
            ServingRecoveryMode::Candidate => {
                "serving replacement exceeded the selected manifest/index/block path".to_owned()
            }
            ServingRecoveryMode::FullHydrationControl => {
                "full-hydration control did not read the complete row closure".to_owned()
            }
            ServingRecoveryMode::SkipTailPoison => {
                "skip-tail poison changed the frozen object-read path".to_owned()
            }
        })
    } else if !exact_replay {
        Some("fresh-process serving recovery changed its semantic digest".to_owned())
    } else {
        None
    };

    let mut measurements = reports
        .iter()
        .map(|report| Measurement {
            metric: "recovery.first_correct_read_duration",
            value: report.first_correct_read_seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", "row-object-plus-txlog"),
                ("result", "attempted"),
            ]),
        })
        .collect::<Vec<_>>();
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: resident_count_as_f64(anomaly_count),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "object-base-plus-point-tail-v1"),
            (
                "anomaly.class",
                if anomaly_count == 0 {
                    "none"
                } else {
                    "recovery_value_mismatch"
                },
            ),
        ]),
    });

    let first = reports.first();
    let hard_gates = vec![
        HardGateResult {
            id: "exact_base_and_tail_reads".to_owned(),
            status: gate_status(exact_values),
            detail: Some(format!("anomalies={anomaly_count}")),
        },
        HardGateResult {
            id: "real_process_kill_and_empty_replacement".to_owned(),
            status: gate_status(process_boundary),
            detail: Some("authority=3,workers=2,kills=1".to_owned()),
        },
        HardGateResult {
            id: "stable_generation_publication_generation_read".to_owned(),
            status: gate_status(authority_stable),
            detail: Some("generation=7,transaction_system=tx-g7,wal_root=wal-g7".to_owned()),
        },
        HardGateResult {
            id: "non_empty_quorum_txlog_suffix".to_owned(),
            status: gate_status(durable_tail),
            detail: first.map(|report| {
                format!(
                    "records={},tail={},physical_bytes={}",
                    report.process.txlog_records_recovered,
                    report.process.txlog_tail_records,
                    report.process.txlog_physical_bytes
                )
            }),
        },
        HardGateResult {
            id: "required_tail_replayed".to_owned(),
            status: gate_status(tail_applied && mode != ServingRecoveryMode::SkipTailPoison),
            detail: first.map(|report| {
                format!(
                    "applied={}/{}",
                    report.process.txlog_tail_records_applied, report.process.txlog_tail_records
                )
            }),
        },
        HardGateResult {
            id: "no_object_list".to_owned(),
            status: gate_status(no_list),
            detail: Some("named authoritative root".to_owned()),
        },
        HardGateResult {
            id: if mode == ServingRecoveryMode::FullHydrationControl {
                "complete_row_closure_hydrated"
            } else {
                "one_manifest_index_and_block_path"
            }
            .to_owned(),
            status: gate_status(expected_path),
            detail: first.map(|report| {
                format!(
                    "manifest={},index={},range={},full={},response_bytes={}",
                    report.process.manifest_requests,
                    report.process.index_requests,
                    report.process.data_range_requests,
                    report.process.data_full_requests,
                    report.process.total_object_response_bytes
                )
            }),
        },
        HardGateResult {
            id: "fresh_process_semantic_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: first.map(|report| report.semantic_sha256.clone()),
        },
    ];
    let durations = reports
        .iter()
        .map(|report| report.first_correct_read_seconds)
        .collect::<Vec<_>>();
    let response_bytes = reports
        .iter()
        .map(|report| resident_count_as_f64(report.process.total_object_response_bytes))
        .collect::<Vec<_>>();
    let txlog_bytes = reports
        .iter()
        .map(|report| resident_count_as_f64(report.process.txlog_physical_bytes))
        .collect::<Vec<_>>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: durations.iter().sum(),
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://serving-recovery-process-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "serving_recovery.first_read_seconds.median".to_owned(),
                median(&durations),
            ),
            (
                "serving_recovery.object_response_bytes.median".to_owned(),
                median(&response_bytes),
            ),
            (
                "serving_recovery.txlog_physical_bytes.median".to_owned(),
                median(&txlog_bytes),
            ),
            (
                "serving_recovery.correctness_anomalies".to_owned(),
                resident_count_as_f64(anomaly_count),
            ),
            (
                "serving_recovery.exact_replay".to_owned(),
                if exact_replay { 1.0 } else { 0.0 },
            ),
            (
                "serving_recovery.row_data_closure_bytes".to_owned(),
                first.map_or(0.0, |report| {
                    resident_count_as_f64(report.process.row_data_closure_bytes)
                }),
            ),
            (
                "serving_recovery.row_segment_count".to_owned(),
                first.map_or(0.0, |report| {
                    resident_count_as_f64(report.process.row_segment_count)
                }),
            ),
        ]),
    }
}

#[cfg(not(feature = "resident-rocksdb"))]
fn run_resident_hot_path(
    _workload: &WorkloadConfig,
    _seeds: &[u64],
    _backend: &str,
    _dataset: Option<&DatasetConfig>,
    _profile: &ProfileConfig,
) -> WorkloadExecution {
    execution_from_result(Err(
        "resident RocksDB workloads require --features resident-rocksdb".to_owned(),
    ))
}

#[cfg(feature = "resident-rocksdb")]
fn run_resident_hot_path(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "resident RocksDB workload requires a dataset".to_owned()
        ));
    };
    let parameter = |name: &str| -> Result<usize, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("resident profile requires integer {name}"))?;
        usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let value_bytes = workload
        .parameters
        .get("value_bytes")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1_024);
    let resident_profile = match (
        usize::try_from(dataset.key_count),
        parameter("measurement_operations"),
        parameter("warmup_operations"),
    ) {
        (Ok(key_count), Ok(operations_per_repeat), Ok(warmup_operations)) => ResidentProfile {
            key_count: u64::try_from(key_count).unwrap_or(u64::MAX),
            value_bytes,
            operations_per_repeat,
            warmup_operations,
            repeats: profile.repeats,
            seeds: seeds.to_vec(),
        },
        (Err(error), _, _) => {
            return execution_from_result(Err(format!("invalid resident key count: {error}")));
        }
        (_, Err(error), _) | (_, _, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = if workload.operation == "direct_rocksdb_hot_control" {
        ResidentMode::DirectControl
    } else if workload.operation == "direct_rocksdb_owned_hot_control" {
        ResidentMode::DirectOwnedControl
    } else if workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        == Some("allow_object_read_after_warmup")
    {
        ResidentMode::ObjectFallbackPoison
    } else {
        ResidentMode::Candidate
    };
    let report = match run_resident_profile(mode, &resident_profile) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    resident_execution(workload, backend, profile, mode, &report)
}

#[cfg(feature = "resident-rocksdb")]
fn resident_execution(
    workload: &WorkloadConfig,
    backend: &str,
    profile: &ProfileConfig,
    mode: ResidentMode,
    report: &ResidentReport,
) -> WorkloadExecution {
    let local_budget = profile
        .parameters
        .get("local_byte_budget")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u64::try_from(value).ok());
    let local_budget_passed = local_budget.is_none_or(|budget| report.max_local_bytes <= budget);
    let object_gate_passed = report.object_fallbacks == 0;
    let correctness_passed = report.correctness_failures == 0;
    let error = if !correctness_passed {
        Some(format!(
            "{} resident point reads failed validation",
            report.correctness_failures
        ))
    } else if !object_gate_passed {
        Some(format!(
            "{} object fallback attempts occurred after resident warmup",
            report.object_fallbacks
        ))
    } else if !local_budget_passed {
        Some(format!(
            "resident local bytes {} exceeded budget {}",
            report.max_local_bytes,
            local_budget.unwrap_or_default()
        ))
    } else {
        None
    };
    let mode_name = match mode {
        ResidentMode::Candidate => "candidate",
        ResidentMode::DirectControl => "direct_control",
        ResidentMode::DirectOwnedControl => "direct_owned_control",
        ResidentMode::ObjectFallbackPoison => "object_fallback_poison",
    };
    WorkloadExecution {
        error,
        measurements: resident_measurements(workload, backend, report),
        hard_gates: resident_hard_gates(
            report,
            mode_name,
            local_budget,
            correctness_passed,
            object_gate_passed,
            local_budget_passed,
        ),
        budget_units: resident_count_as_f64(report.operations),
        artifact_refs: vec![format!("okv-eval://resident-hot-path-v1/{mode_name}")],
        secondary_metrics: resident_secondary_metrics(report),
    }
}

#[cfg(feature = "resident-rocksdb")]
fn resident_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    report: &ResidentReport,
) -> Vec<Measurement> {
    let mut measurements = report
        .samples
        .iter()
        .map(|sample| Measurement {
            metric: "operation.throughput",
            value: sample.operations_per_second,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("operation", "resident_point_read"),
                ("backend", backend),
            ]),
        })
        .collect::<Vec<_>>();
    measurements.extend([
        Measurement {
            metric: "object_store.requests",
            value: resident_count_as_f64(report.object_fallbacks),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "object_base"),
                ("api", "get"),
                ("result", "attempted"),
            ]),
        },
        Measurement {
            metric: "cache.hit_ratio",
            value: report.cache_hit_ratio(),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("cache.tier", "resident_range"),
                ("backend", backend),
            ]),
        },
        Measurement {
            metric: "serving.local_bytes",
            value: resident_count_as_f64(report.max_local_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("cache.tier", "rocksdb"),
                ("range.class", "complete_resident"),
            ]),
        },
        Measurement {
            metric: "correctness.anomalies",
            value: resident_count_as_f64(report.correctness_failures),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "resident-point-read-v1"),
                (
                    "anomaly.class",
                    if report.correctness_failures == 0 {
                        "none"
                    } else {
                        "value_mismatch"
                    },
                ),
            ]),
        },
    ]);
    measurements
}

#[cfg(feature = "resident-rocksdb")]
fn resident_hard_gates(
    report: &ResidentReport,
    mode_name: &str,
    local_budget: Option<u64>,
    correctness_passed: bool,
    object_gate_passed: bool,
    local_budget_passed: bool,
) -> Vec<HardGateResult> {
    let mut gates = vec![
        HardGateResult {
            id: "exact_resident_point_reads".to_owned(),
            status: gate_status(correctness_passed),
            detail: Some(format!("mode={mode_name}")),
        },
        HardGateResult {
            id: "zero_object_requests_after_warmup".to_owned(),
            status: gate_status(object_gate_passed),
            detail: Some(report.object_fallbacks.to_string()),
        },
        HardGateResult {
            id: "resident_local_byte_budget".to_owned(),
            status: gate_status(local_budget_passed),
            detail: local_budget
                .map(|budget| format!("actual={},budget={budget}", report.max_local_bytes)),
        },
    ];
    if mode_name == "direct_owned_control" {
        gates.push(HardGateResult {
            id: "owned_value_result".to_owned(),
            status: gate_status(true),
            detail: Some("rocksdb_get_vec".to_owned()),
        });
    }
    gates
}

#[cfg(feature = "resident-rocksdb")]
fn resident_secondary_metrics(report: &ResidentReport) -> BTreeMap<String, f64> {
    let latency_median = |select: fn(&okv_eval::resident::ResidentSample) -> u64| {
        let values = report
            .samples
            .iter()
            .map(|sample| resident_count_as_f64(select(sample)))
            .collect::<Vec<_>>();
        median(&values)
    };
    BTreeMap::from([
        (
            "resident.latency_ns.p50".to_owned(),
            latency_median(|sample| sample.latency_ns_p50),
        ),
        (
            "resident.latency_ns.p95".to_owned(),
            latency_median(|sample| sample.latency_ns_p95),
        ),
        (
            "resident.latency_ns.p99".to_owned(),
            latency_median(|sample| sample.latency_ns_p99),
        ),
        (
            "resident.latency_ns.p999".to_owned(),
            latency_median(|sample| sample.latency_ns_p999),
        ),
        (
            "resident.object_fallbacks".to_owned(),
            resident_count_as_f64(report.object_fallbacks),
        ),
        (
            "resident.max_local_bytes".to_owned(),
            resident_count_as_f64(report.max_local_bytes),
        ),
    ])
}

#[allow(clippy::cast_precision_loss)]
fn resident_count_as_f64(value: u64) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn usize_as_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn ratio_as_f64(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn run_slatedb_phase0_filesystem(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB Phase 0 workload requires a dataset for the selected profile".to_owned(),
        ));
    };
    let parameter = |name: &str| -> Result<usize, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("SlateDB Phase 0 profile requires integer {name}"))?;
        usize::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let config = match (
        parameter("point_reads_per_seed"),
        parameter("scan_rows_per_seed"),
    ) {
        (Ok(point_reads_per_seed), Ok(scan_rows_per_seed)) => Phase0Config {
            logical_bytes: dataset.logical_bytes,
            key_count: dataset.key_count,
            point_reads_per_seed,
            scan_rows_per_seed,
            seeds: dataset.seeds.clone(),
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0Mode::Correct,
        Some("reuse_warm_db_for_reopen") => Phase0Mode::ReuseWarmDbForReopen,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB Phase 0 negative control {other}"
            )));
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return execution_from_result(Err(format!("build SlateDB Phase 0 runtime: {error}")));
        }
    };
    let report = match runtime.block_on(run_phase0_filesystem_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0Report,
) -> WorkloadExecution {
    const LANE: &str = "slatedb-filesystem-incumbent";
    const ORACLE: &str = "deterministic-slate-phase0-filesystem-v1";
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut total_io = Phase0IoDelta::default();
    let dataset_class = format!("local-fs-{}-bytes", report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "recovery.first_correct_read_duration",
            value: seed.reopen_first_correct_read_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", &dataset_class),
                ("result", if report.passed() { "pass" } else { "fail" }),
            ]),
        });
        for phase in [
            &seed.initial_open,
            &seed.ingest,
            &seed.post_flush_verify,
            &seed.warm_cache_prime,
            &seed.warm_point,
            &seed.ordered_scan,
            &seed.close_before_reopen,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.cold_point,
            &seed.final_close,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, phase);
            total_operations += phase.logical_operations;
        }
        merge_counts(
            &mut total_io.successful_requests,
            &seed.total_io.successful_requests,
        );
        merge_counts(
            &mut total_io.failed_requests,
            &seed.total_io.failed_requests,
        );
        merge_counts(&mut total_io.read_bytes, &seed.total_io.read_bytes);
        merge_counts(&mut total_io.written_bytes, &seed.total_io.written_bytes);
    }
    for (api, value) in &total_io.successful_requests {
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "filesystem"),
                ("api", api),
                ("result", "success"),
            ]),
        });
    }
    for (api, value) in &total_io.failed_requests {
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "filesystem"),
                ("api", api),
                ("result", "error"),
            ]),
        });
    }
    for (api, value) in &total_io.read_bytes {
        measurements.push(Measurement {
            metric: "object_store.bytes",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "filesystem"),
                ("direction", "read"),
                ("api", api),
            ]),
        });
    }
    for (api, value) in &total_io.written_bytes {
        measurements.push(Measurement {
            metric: "object_store.bytes",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "filesystem"),
                ("direction", "write"),
                ("api", api),
            ]),
        });
    }
    let anomalies = report.anomaly_count();
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", LANE),
            ("workload", &workload.id),
            ("oracle", ORACLE),
            ("anomaly.class", "hard-gate"),
        ]),
    });
    let hard_gates: Vec<HardGateResult> = std::iter::once(HardGateResult {
        id: "correctness_anomalies".to_owned(),
        status: if anomalies == 0 {
            GateStatus::Pass
        } else {
            GateStatus::Fail
        },
        detail: Some(format!("{anomalies} failed frozen contract gates")),
    })
    .chain(report.gates.iter().map(|gate| HardGateResult {
        id: gate.id.clone(),
        status: if gate.passed {
            GateStatus::Pass
        } else {
            GateStatus::Fail
        },
        detail: Some(gate.detail.clone()),
    }))
    .collect();
    let failed_gates: Vec<&str> = report
        .gates
        .iter()
        .filter(|gate| !gate.passed)
        .map(|gate| gate.id.as_str())
        .collect();
    let artifact_path = phase0_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_phase0_artifact(&artifact_path, report);
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB Phase 0 failed gates: {}",
            failed_gates.join(", ")
        ))
    };
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(total_operations),
        artifact_refs: artifact_result
            .is_ok()
            .then(|| artifact_path.display().to_string())
            .into_iter()
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "phase0.object_store.requests.total".to_owned(),
                bounded_count(total_io.request_total()),
            ),
            (
                "phase0.object_store.read_bytes.total".to_owned(),
                bounded_count(total_io.read_byte_total()),
            ),
            (
                "phase0.object_store.written_bytes.total".to_owned(),
                bounded_count(total_io.written_byte_total()),
            ),
            (
                "phase0.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
        ]),
    }
}

fn add_phase0_phase_measurements(
    measurements: &mut Vec<Measurement>,
    workload: &WorkloadConfig,
    backend: &str,
    phase: &Phase0PhaseReport,
) {
    const LANE: &str = "slatedb-filesystem-incumbent";
    let throughput = if phase.elapsed_seconds > 0.0 {
        bounded_count(phase.logical_operations) / phase.elapsed_seconds
    } else {
        0.0
    };
    measurements.push(Measurement {
        metric: "operation.throughput",
        value: throughput,
        attributes: attributes(&[
            ("lane", LANE),
            ("workload", &workload.id),
            ("operation", &phase.phase),
            ("backend", backend),
        ]),
    });
}

fn merge_counts(target: &mut BTreeMap<String, u64>, source: &BTreeMap<String, u64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += value;
    }
}

fn phase0_artifact_path(run_id: &str, candidate_commit: &str, report: &Phase0Report) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "phase0-slate-filesystem-{candidate}-{run}-{}.json",
        report.mode
    ))
}

fn write_phase0_artifact(path: &Path, report: &Phase0Report) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "Phase 0 artifact path has no parent".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create Phase 0 artifact directory: {error}"))?;
    let rendered = serde_json::to_string_pretty(report)
        .map_err(|error| format!("serialize Phase 0 report: {error}"))?;
    fs::write(path, format!("{rendered}\n"))
        .map_err(|error| format!("write Phase 0 artifact: {error}"))
}

#[allow(clippy::too_many_lines)]
fn run_persisted_wal(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "persisted WAL workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PersistedWalMode::Correct,
        Some("ram_only_dedup") => PersistedWalMode::RamOnlyDedup,
        Some("ack_before_quorum") => PersistedWalMode::AckBeforeQuorum,
        Some("trust_single_replica") => PersistedWalMode::TrustSingleReplica,
        Some("accept_torn_as_commit") => PersistedWalMode::AcceptTornAsCommit,
        Some("skip_log_chain_validation") => PersistedWalMode::SkipLogChainValidation,
        Some("ignore_complete_corruption") => PersistedWalMode::IgnoreCompleteCorruption,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown persisted WAL negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut quorum_appends = 0_u64;
    let mut recovered_records = 0_u64;
    let mut reopened_wals = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut leader_only_attempts = 0_u64;
    let mut torn_tail_replicas = 0_u64;
    let mut corruption_failures = 0_u64;
    let mut physical_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_persisted_wal_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_persisted_wal_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        quorum_appends = quorum_appends.saturating_add(first.quorum_appends);
        recovered_records = recovered_records.saturating_add(first.recovered_records);
        reopened_wals = reopened_wals.saturating_add(first.reopened_wals);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        leader_only_attempts = leader_only_attempts.saturating_add(first.leader_only_attempts);
        torn_tail_replicas = torn_tail_replicas.saturating_add(first.torn_tail_replicas);
        corruption_failures = corruption_failures.saturating_add(first.corruption_failures);
        physical_bytes = physical_bytes.saturating_add(first.physical_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "okv-persisted-wal-contract-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "persisted_wal" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.quorum_appends),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "cell-commit-contract"),
                    ("result", "quorum-fsynced"),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.physical_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "local-three-file"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "fresh-open-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-persisted-wal://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(7);
    let semantic_operations_exercised = event_count == expected_events
        && quorum_appends > 0
        && recovered_records > 0
        && reopened_wals > 0
        && recovered_outcomes > 0
        && leader_only_attempts > 0
        && torn_tail_replicas > 0
        && corruption_failures > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "persisted WAL gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "persisted_wal.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "persisted_wal.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, quorum_appends={quorum_appends}, recovered_records={recovered_records}, reopened={reopened_wals}, outcomes={recovered_outcomes}, leader_only={leader_only_attempts}, torn={torn_tail_replicas}, corruption_failures={corruption_failures}"
                )),
            },
            HardGateResult {
                id: "persisted_wal.contract_agreement".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("wal.contract.events".to_owned(), bounded_count(event_count)),
            (
                "wal.contract.quorum_appends".to_owned(),
                bounded_count(quorum_appends),
            ),
            (
                "wal.contract.recovered_records".to_owned(),
                bounded_count(recovered_records),
            ),
            (
                "wal.contract.reopened".to_owned(),
                bounded_count(reopened_wals),
            ),
            (
                "wal.contract.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
            (
                "wal.contract.leader_only_attempts".to_owned(),
                bounded_count(leader_only_attempts),
            ),
            (
                "wal.contract.torn_tail_replicas".to_owned(),
                bounded_count(torn_tail_replicas),
            ),
            (
                "wal.contract.corruption_failures".to_owned(),
                bounded_count(corruption_failures),
            ),
            (
                "wal.contract.physical_bytes".to_owned(),
                bounded_count(physical_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_storage(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft storage workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => RaftStorageMode::Correct,
        Some("ram_only_vote") => RaftStorageMode::RamOnlyVote,
        Some("ram_only_committed") => RaftStorageMode::RamOnlyCommitted,
        Some("ignore_conflict_truncate") => RaftStorageMode::IgnoreConflictTruncate,
        Some("ignore_purge") => RaftStorageMode::IgnorePurge,
        Some("accept_log_gap") => RaftStorageMode::AcceptLogGap,
        Some("ignore_complete_corruption") => RaftStorageMode::IgnoreCompleteCorruption,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown Raft storage negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut reopened_stores = 0_u64;
    let mut durable_votes = 0_u64;
    let mut durable_committed_positions = 0_u64;
    let mut appended_entries = 0_u64;
    let mut conflict_truncations = 0_u64;
    let mut purged_prefixes = 0_u64;
    let mut rejected_log_gaps = 0_u64;
    let mut torn_tail_repairs = 0_u64;
    let mut corruption_failures = 0_u64;
    let mut physical_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_storage_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_storage_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        reopened_stores = reopened_stores.saturating_add(first.reopened_stores);
        durable_votes = durable_votes.saturating_add(first.durable_votes);
        durable_committed_positions =
            durable_committed_positions.saturating_add(first.durable_committed_positions);
        appended_entries = appended_entries.saturating_add(first.appended_entries);
        conflict_truncations = conflict_truncations.saturating_add(first.conflict_truncations);
        purged_prefixes = purged_prefixes.saturating_add(first.purged_prefixes);
        rejected_log_gaps = rejected_log_gaps.saturating_add(first.rejected_log_gaps);
        torn_tail_repairs = torn_tail_repairs.saturating_add(first.torn_tail_repairs);
        corruption_failures = corruption_failures.saturating_add(first.corruption_failures);
        physical_bytes = physical_bytes.saturating_add(first.physical_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-storage-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_storage" }),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.physical_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "per-node-journal"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "stable-log-reopen"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-storage://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(8);
    let semantic_operations_exercised = event_count == expected_events
        && reopened_stores > 0
        && durable_votes > 0
        && durable_committed_positions > 0
        && appended_entries > 0
        && conflict_truncations > 0
        && purged_prefixes > 0
        && torn_tail_repairs > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft storage gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_storage.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_storage.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, reopened={reopened_stores}, votes={durable_votes}, committed={durable_committed_positions}, appended={appended_entries}, truncations={conflict_truncations}, purges={purged_prefixes}, gap_rejections={rejected_log_gaps}, torn_repairs={torn_tail_repairs}, corruption_failures={corruption_failures}"
                )),
            },
            HardGateResult {
                id: "raft_storage.contract_agreement".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("raft_storage.events".to_owned(), bounded_count(event_count)),
            (
                "raft_storage.reopened".to_owned(),
                bounded_count(reopened_stores),
            ),
            (
                "raft_storage.durable_votes".to_owned(),
                bounded_count(durable_votes),
            ),
            (
                "raft_storage.durable_committed".to_owned(),
                bounded_count(durable_committed_positions),
            ),
            (
                "raft_storage.appended_entries".to_owned(),
                bounded_count(appended_entries),
            ),
            (
                "raft_storage.conflict_truncations".to_owned(),
                bounded_count(conflict_truncations),
            ),
            (
                "raft_storage.purged_prefixes".to_owned(),
                bounded_count(purged_prefixes),
            ),
            (
                "raft_storage.rejected_log_gaps".to_owned(),
                bounded_count(rejected_log_gaps),
            ),
            (
                "raft_storage.torn_tail_repairs".to_owned(),
                bounded_count(torn_tail_repairs),
            ),
            (
                "raft_storage.corruption_failures".to_owned(),
                bounded_count(corruption_failures),
            ),
            (
                "raft_storage.physical_bytes".to_owned(),
                bounded_count(physical_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_cluster(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft cluster workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => RaftClusterMode::Correct,
        Some("acknowledge_before_quorum") => RaftClusterMode::AcknowledgeBeforeQuorum,
        Some("skip_successor_election") => RaftClusterMode::SkipSuccessorElection,
        Some("skip_restart_catchup") => RaftClusterMode::SkipRestartCatchup,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown Raft cluster negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut committed_writes = 0_u64;
    let mut elections = 0_u64;
    let mut stale_write_attempts = 0_u64;
    let mut stale_write_acks = 0_u64;
    let mut partitions = 0_u64;
    let mut repairs = 0_u64;
    let mut simulated_crashes = 0_u64;
    let mut simulated_bounces = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_cluster_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_cluster_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        committed_writes = committed_writes.saturating_add(first.committed_writes);
        elections = elections.saturating_add(first.elections);
        stale_write_attempts = stale_write_attempts.saturating_add(first.stale_write_attempts);
        stale_write_acks = stale_write_acks.saturating_add(first.stale_write_acks);
        partitions = partitions.saturating_add(first.partitions);
        repairs = repairs.saturating_add(first.repairs);
        simulated_crashes = simulated_crashes.saturating_add(first.simulated_crashes);
        simulated_bounces = simulated_bounces.saturating_add(first.simulated_bounces);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-cluster-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_cluster" }),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "openraft-cell-v0"),
                    ("result", if exact { "quorum-applied" } else { "rejected" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "leader-failover-and-bounce"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-cluster://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(8)
        && partitions == seed_count.saturating_mul(2)
        && repairs == seed_count.saturating_mul(2)
        && simulated_crashes == seed_count
        && simulated_bounces == seed_count
        && stale_write_attempts == seed_count;
    let expected_success_path = mode != RaftClusterMode::Correct
        || (committed_writes == seed_count.saturating_mul(3)
            && elections == seed_count.saturating_mul(3)
            && stale_write_acks == 0
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft cluster gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_cluster.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_cluster.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, commits={committed_writes}, elections={elections}, stale_attempts={stale_write_attempts}, stale_acks={stale_write_acks}, partitions={partitions}, repairs={repairs}, crashes={simulated_crashes}, bounces={simulated_bounces}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "raft_cluster.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("raft_cluster.checks".to_owned(), bounded_count(check_count)),
            (
                "raft_cluster.committed_writes".to_owned(),
                bounded_count(committed_writes),
            ),
            (
                "raft_cluster.elections".to_owned(),
                bounded_count(elections),
            ),
            (
                "raft_cluster.stale_write_attempts".to_owned(),
                bounded_count(stale_write_attempts),
            ),
            (
                "raft_cluster.stale_write_acks".to_owned(),
                bounded_count(stale_write_acks),
            ),
            (
                "raft_cluster.partitions".to_owned(),
                bounded_count(partitions),
            ),
            ("raft_cluster.repairs".to_owned(), bounded_count(repairs)),
            (
                "raft_cluster.simulated_crashes".to_owned(),
                bounded_count(simulated_crashes),
            ),
            (
                "raft_cluster.simulated_bounces".to_owned(),
                bounded_count(simulated_bounces),
            ),
            (
                "raft_cluster.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

fn parse_raft_process_mode(value: &str) -> Result<RaftProcessMode, String> {
    match value {
        "correct" | "none" => Ok(RaftProcessMode::Correct),
        "disable_dedup" => Ok(RaftProcessMode::DisableDedup),
        "acknowledge_before_quorum" => Ok(RaftProcessMode::AcknowledgeBeforeQuorum),
        "skip_killed_node_restart" => Ok(RaftProcessMode::SkipKilledNodeRestart),
        other => Err(format!("unknown Raft process mode {other}")),
    }
}

fn parse_transaction_process_mode(value: &str) -> Result<TransactionProcessMode, String> {
    match value {
        "correct" | "none" => Ok(TransactionProcessMode::Correct),
        "accept_conflicts" => Ok(TransactionProcessMode::AcceptConflicts),
        "partial_apply" => Ok(TransactionProcessMode::PartialApply),
        other => Err(format!("unknown transaction process mode {other}")),
    }
}

fn parse_generation_process_mode(value: &str) -> Result<GenerationProcessMode, String> {
    match value {
        "correct" | "none" => Ok(GenerationProcessMode::Correct),
        "bypass_stale_commit_fence" => Ok(GenerationProcessMode::BypassStaleCommitFence),
        "accept_write_during_recovery" => Ok(GenerationProcessMode::AcceptWriteDuringRecovery),
        "accept_competing_recovery" => Ok(GenerationProcessMode::AcceptCompetingRecovery),
        "activate_without_recovery_proof" => {
            Ok(GenerationProcessMode::ActivateWithoutRecoveryProof)
        }
        "accept_single_signer_fence" => Ok(GenerationProcessMode::AcceptSingleSignerFence),
        "accept_tampered_fence_position" => Ok(GenerationProcessMode::AcceptTamperedFencePosition),
        "accept_duplicate_recovery_signer" => {
            Ok(GenerationProcessMode::AcceptDuplicateRecoverySigner)
        }
        "accept_stale_recovery_certificate" => {
            Ok(GenerationProcessMode::AcceptStaleRecoveryCertificate)
        }
        "accept_wrong_recovery_membership" => {
            Ok(GenerationProcessMode::AcceptWrongRecoveryMembership)
        }
        other => Err(format!("unknown generation process mode {other}")),
    }
}

fn parse_publication_process_mode(value: &str) -> Result<PublicationProcessMode, String> {
    match value {
        "correct" | "none" => Ok(PublicationProcessMode::Correct),
        "bypass_generation_fence" => Ok(PublicationProcessMode::BypassGenerationFence),
        "publish_without_intent" => Ok(PublicationProcessMode::PublishWithoutIntent),
        "ignore_root_epoch" => Ok(PublicationProcessMode::IgnoreRootEpoch),
        "ignore_delete_reservation" => Ok(PublicationProcessMode::IgnoreDeleteReservation),
        "disable_request_dedup" => Ok(PublicationProcessMode::DisableRequestDedup),
        "acknowledge_before_quorum" => Ok(PublicationProcessMode::AcknowledgeBeforeQuorum),
        "stale_expected_root" => Ok(PublicationProcessMode::StaleExpectedRoot),
        "local_stale_outcome_read" => Ok(PublicationProcessMode::LocalStaleOutcomeRead),
        "cross_generation_intent_publish" => {
            Ok(PublicationProcessMode::CrossGenerationIntentPublish)
        }
        "retire_by_plan_key_only" => Ok(PublicationProcessMode::RetireByPlanKeyOnly),
        other => Err(format!("unknown publication process mode {other}")),
    }
}

fn parse_publisher_process_mode(value: &str) -> Result<PublisherProcessMode, String> {
    match value {
        "correct" | "none" => Ok(PublisherProcessMode::Correct),
        "upload_before_prepare_ack" => Ok(PublisherProcessMode::UploadBeforePrepareAck),
        other => Err(format!("unknown publisher process mode {other}")),
    }
}

fn parse_publisher_put_recovery_mode(value: &str) -> Result<PublisherPutRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(PublisherPutRecoveryMode::Correct),
        "publish_partial_closure" => Ok(PublisherPutRecoveryMode::PublishPartialClosure),
        other => Err(format!("unknown publisher PUT recovery mode {other}")),
    }
}

fn parse_publisher_manifest_recovery_mode(
    value: &str,
) -> Result<PublisherManifestRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(PublisherManifestRecoveryMode::Correct),
        "trust_manifest_without_closure" => {
            Ok(PublisherManifestRecoveryMode::TrustManifestWithoutClosure)
        }
        other => Err(format!("unknown publisher manifest recovery mode {other}")),
    }
}

fn parse_publisher_publish_recovery_mode(
    value: &str,
) -> Result<PublisherPublishRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(PublisherPublishRecoveryMode::Correct),
        "convergence_only_duplicate_publish" => {
            Ok(PublisherPublishRecoveryMode::ConvergenceOnlyDuplicatePublish)
        }
        other => Err(format!("unknown publisher Publish recovery mode {other}")),
    }
}

fn parse_serving_recovery_mode(value: &str) -> Result<ServingRecoveryMode, String> {
    match value {
        "candidate" | "correct" => Ok(ServingRecoveryMode::Candidate),
        "full_hydration_control" => Ok(ServingRecoveryMode::FullHydrationControl),
        "skip_tail_poison" | "skip_tail_replay" => Ok(ServingRecoveryMode::SkipTailPoison),
        other => Err(format!("unknown serving recovery mode {other}")),
    }
}

fn parse_openraft_serving_recovery_mode(
    value: &str,
) -> Result<OpenRaftServingRecoveryMode, String> {
    match value {
        "candidate" | "correct" => Ok(OpenRaftServingRecoveryMode::Candidate),
        "integrated_kernel_candidate" | "integrated_kernel" => {
            Ok(OpenRaftServingRecoveryMode::IntegratedKernelCandidate)
        }
        "integrated_kernel_rocksdb_candidate" | "integrated_kernel_rocksdb" => {
            Ok(OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate)
        }
        "integrated_kernel_native_rocksdb_candidate" | "integrated_kernel_native_rocksdb" => {
            Ok(OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate)
        }
        "full_hydration_control" => Ok(OpenRaftServingRecoveryMode::FullHydrationControl),
        "skip_concurrent_catchup_poison" | "skip_concurrent_catchup" => {
            Ok(OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison)
        }
        other => Err(format!("unknown OpenRaft serving recovery mode {other}")),
    }
}

fn parse_t27_plan_profile(value: &str) -> Result<T27PlanProfileV1, String> {
    match value {
        "preflight_64_mib" | "preflight" => Ok(T27PlanProfileV1::Preflight64Mib),
        "admission_1_gib" | "admission" => Ok(T27PlanProfileV1::Admission1Gib),
        other => Err(format!("unknown T27 plan profile {other}")),
    }
}

fn capture_t27_execution_envelope(
    runtime_source_sha256: &str,
    runtime_cargo_lock: &Path,
    machine_receipt_path: &Path,
    scratch_root: &Path,
) -> Result<T27ExecutionEnvelopeV1, Box<dyn Error>> {
    capture_t27_execution_envelope_for_executable(
        runtime_source_sha256,
        &std::env::current_exe()?,
        runtime_cargo_lock,
        machine_receipt_path,
        scratch_root,
    )
}

fn capture_t27_execution_envelope_for_executable(
    runtime_source_sha256: &str,
    runtime_executable: &Path,
    runtime_cargo_lock: &Path,
    machine_receipt_path: &Path,
    scratch_root: &Path,
) -> Result<T27ExecutionEnvelopeV1, Box<dyn Error>> {
    fs::create_dir_all(scratch_root)?;
    let scratch_root = scratch_root.canonicalize()?;
    let machine_receipt_bytes = fs::read(machine_receipt_path)?;
    let machine_receipt: serde_json::Value = serde_json::from_slice(&machine_receipt_bytes)?;
    if machine_receipt.pointer("/checks/runner_ready") != Some(&serde_json::Value::Bool(true))
        || machine_receipt.pointer("/checks/binary_sha256_recorded")
            != Some(&serde_json::Value::Bool(true))
    {
        return Err(std::io::Error::other("T27 machine receipt is not runner-ready").into());
    }
    let runner = machine_receipt
        .get("runner")
        .ok_or_else(|| std::io::Error::other("T27 machine receipt omits runner identity"))?;
    let machine_instance_id = runner
        .get("instance_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| std::io::Error::other("T27 machine receipt omits instance ID"))?
        .to_owned();
    let infrastructure_lease_expires_epoch = runner
        .get("lease_expires_epoch")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| std::io::Error::other("T27 machine receipt omits lease expiry"))?
        .parse::<u64>()?;
    require_t27_infrastructure_lease(infrastructure_lease_expires_epoch)?;
    let runtime_executable_sha256 = sha256(&fs::read(runtime_executable)?);
    let declared_binary_sha256 = runner
        .get("binary_sha256")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| std::io::Error::other("T27 machine receipt omits runner binary SHA"))?;
    if declared_binary_sha256 != runtime_executable_sha256 {
        return Err(std::io::Error::other(
            "T27 current executable differs from the machine receipt",
        )
        .into());
    }
    let hot_scratch = runner
        .get("hot_scratch")
        .ok_or_else(|| std::io::Error::other("T27 machine receipt omits hot scratch"))?;
    if hot_scratch
        .get("interface")
        .and_then(serde_json::Value::as_str)
        != Some("nvme")
    {
        return Err(std::io::Error::other("T27 hot scratch is not NVMe").into());
    }
    let declared_mount = PathBuf::from(
        hot_scratch
            .get("mount")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| std::io::Error::other("T27 hot scratch omits its mount"))?,
    )
    .canonicalize()?;
    if !scratch_root.starts_with(&declared_mount) {
        return Err(std::io::Error::other(
            "T27 scratch root is not backed by the declared hot-scratch mount",
        )
        .into());
    }
    let findmnt = Command::new("findmnt")
        .arg("--json")
        .arg("--target")
        .arg(&scratch_root)
        .arg("--output")
        .arg("SOURCE,UUID,FSTYPE,TARGET,MAJ:MIN")
        .output()?;
    if !findmnt.status.success() {
        return Err(std::io::Error::other("findmnt failed for T27 scratch root").into());
    }
    let mounts: serde_json::Value = serde_json::from_slice(&findmnt.stdout)?;
    let mount = mounts
        .pointer("/filesystems/0")
        .ok_or_else(|| std::io::Error::other("findmnt omitted the T27 scratch filesystem"))?;
    let mount_field = |name: &str| -> Result<String, std::io::Error> {
        mount
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| std::io::Error::other(format!("findmnt omitted T27 field {name}")))
    };
    let scratch_mount_source = mount_field("source")?;
    let scratch_filesystem_uuid = mount_field("uuid")?;
    let scratch_filesystem_type = mount_field("fstype")?;
    let scratch_mount_point = mount_field("target")?;
    let scratch_major_minor = mount_field("maj:min")?;
    if Path::new(&scratch_mount_point).canonicalize()? != declared_mount {
        return Err(
            std::io::Error::other("T27 observed mount differs from the machine receipt").into(),
        );
    }
    let linux_boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    let host_lease_path = canonical_t27_host_lease_path(
        &machine_instance_id,
        &scratch_filesystem_uuid,
        &scratch_major_minor,
    );
    let execution = T27ExecutionEnvelopeV1 {
        runtime_source_sha256: runtime_source_sha256.to_owned(),
        runtime_executable_sha256,
        runtime_cargo_lock_sha256: sha256(&fs::read(runtime_cargo_lock)?),
        machine_receipt_sha256: sha256(&machine_receipt_bytes),
        machine_instance_id,
        infrastructure_lease_expires_epoch,
        linux_boot_id,
        scratch_root: scratch_root.display().to_string(),
        scratch_mount_point,
        scratch_mount_source: scratch_mount_source.clone(),
        scratch_filesystem_type,
        scratch_major_minor,
        scratch_device_number: fs::metadata(&scratch_root)?.dev(),
        scratch_filesystem_uuid,
        scratch_block_device: scratch_mount_source,
        host_lease_path: host_lease_path.display().to_string(),
    };
    execution.validate()?;
    Ok(execution)
}

fn canonical_t27_host_lease_path(
    machine_instance_id: &str,
    scratch_filesystem_uuid: &str,
    scratch_major_minor: &str,
) -> PathBuf {
    let identity =
        format!("{machine_instance_id}\0{scratch_filesystem_uuid}\0{scratch_major_minor}");
    let digest = sha256(identity.as_bytes());
    PathBuf::from("/var/lib/objectkv/receipts").join(format!("t27-host-{}.lock", &digest[..16]))
}

fn require_t27_infrastructure_lease(expires_epoch: u64) -> Result<(), Box<dyn Error>> {
    let now = u64::try_from(Utc::now().timestamp())?;
    if expires_epoch <= now {
        return Err(std::io::Error::other("T27 infrastructure lease has expired").into());
    }
    Ok(())
}

fn t27_access_pattern(value: T27AccessPatternV1) -> OpenRaftHotReadAccessPattern {
    match value {
        T27AccessPatternV1::Zipf0_8 => OpenRaftHotReadAccessPattern::Zipf0_8,
        T27AccessPatternV1::Zipf1_4 => OpenRaftHotReadAccessPattern::Zipf1_4,
        T27AccessPatternV1::Zipf2_0 => OpenRaftHotReadAccessPattern::Zipf2_0,
    }
}

fn t27_subject(value: T27PlanSubjectV1) -> OpenRaftHotReadSubject {
    match value {
        T27PlanSubjectV1::NativeSnapshot => OpenRaftHotReadSubject::NativeSnapshot,
        T27PlanSubjectV1::DirectOwnedRocksdb => OpenRaftHotReadSubject::DirectOwnedRocksdb,
    }
}

fn t27_observed_subject(value: OpenRaftHotReadSubject) -> T27PlanSubjectV1 {
    match value {
        OpenRaftHotReadSubject::NativeSnapshot => T27PlanSubjectV1::NativeSnapshot,
        OpenRaftHotReadSubject::DirectOwnedRocksdb => T27PlanSubjectV1::DirectOwnedRocksdb,
    }
}

fn run_t27_plan_position(
    plan_path: &Path,
    expected_plan_sha256: &str,
    ordinal: u64,
    controller_id: String,
    worker_id: String,
    runtime_cargo_lock: &Path,
    machine_receipt: &Path,
    scratch_root: &Path,
    lease_id: &str,
) -> Result<T27PositionExecutionOutputV1, Box<dyn Error>> {
    Uuid::parse_str(&controller_id)?;
    Uuid::parse_str(&worker_id)?;
    Uuid::parse_str(lease_id)?;
    let plan = decode_t27_execution_plan(&fs::read(plan_path)?, expected_plan_sha256)?;
    let observed_execution = capture_t27_execution_envelope(
        &plan.execution.runtime_source_sha256,
        runtime_cargo_lock,
        machine_receipt,
        scratch_root,
    )?;
    if observed_execution != plan.execution {
        return Err(std::io::Error::other("T27 position execution envelope mismatch").into());
    }
    validate_t27_host_lease(&plan, &controller_id, lease_id)?;
    let position = plan
        .positions
        .get(usize::try_from(ordinal)?)
        .ok_or_else(|| std::io::Error::other("T27 plan position is absent"))?
        .clone();
    if position.ordinal != ordinal {
        return Err(std::io::Error::other("T27 plan ordinal is not canonical").into());
    }
    let started_at = Utc::now().to_rfc3339();
    let profile = ServingRecoveryProfile {
        key_count: plan.fixture.key_count,
        value_bytes: usize::try_from(plan.fixture.value_bytes)?,
        target_object_bytes: usize::try_from(plan.fixture.target_object_bytes)?,
        target_block_bytes: usize::try_from(plan.fixture.target_block_bytes)?,
    };
    let executable = std::env::current_exe()?;
    let report = run_openraft_serving_recovery_contract_from_fixture_placement(
        position.trace_seed,
        &profile,
        2,
        &executable,
        &plan.fixture,
        OpenRaftHotReadProfile {
            subject: t27_subject(position.subject),
            seed: position.trace_seed,
            key_count: plan.fixture.key_count,
            value_bytes: profile.value_bytes,
            warmup_operations: usize::try_from(position.warmup_operations)?,
            measured_operations: usize::try_from(position.measured_operations)?,
            concurrent_clients: usize::try_from(position.concurrent_clients)?,
            access_pattern: t27_access_pattern(position.access_pattern),
            max_local_bytes: position.max_local_bytes,
            block_cache_bytes: position.block_cache_bytes,
            direct_reads: position.direct_reads,
            sample_count: 1,
            negative_control: None,
        },
        false,
        false,
    )?;
    let hot_read = report
        .process
        .hot_read
        .as_ref()
        .ok_or_else(|| std::io::Error::other("T27 position omitted hot-read evidence"))?;
    if hot_read.subject != t27_subject(position.subject)
        || hot_read.access_pattern != t27_access_pattern(position.access_pattern)
        || hot_read.concurrent_clients != position.concurrent_clients
        || hot_read.max_local_bytes != position.max_local_bytes
        || hot_read.warmup_operations != position.warmup_operations
        || hot_read.measured_operations != position.measured_operations
    {
        return Err(
            std::io::Error::other("T27 position report differs from the immutable plan").into(),
        );
    }
    let sample = hot_read
        .samples
        .first()
        .ok_or_else(|| std::io::Error::other("T27 position omitted its measured sample"))?;
    let image = report
        .process
        .object_fixture_image
        .as_ref()
        .ok_or_else(|| std::io::Error::other("T27 position omitted fixture-image evidence"))?;
    let raw_report_bytes = serde_json::to_vec(&report)?;
    let receipt = T27PositionReceiptV1::new(
        &plan,
        &position,
        controller_id,
        lease_id.to_owned(),
        worker_id,
        std::process::id(),
        started_at,
        Utc::now().to_rfc3339(),
        T27PositionObservationV1 {
            execution: observed_execution,
            measured_worker_process_id: report.process.worker_process.process_id,
            measured_worker_linux_boot_id: report
                .process
                .worker_process
                .linux_boot_id
                .clone()
                .ok_or_else(|| std::io::Error::other("T27 worker omitted Linux boot ID"))?,
            measured_worker_start_ticks: report
                .process
                .worker_process
                .linux_process_start_ticks
                .ok_or_else(|| std::io::Error::other("T27 worker omitted process start ticks"))?,
            fixture_id: image.fixture_id.clone(),
            image_provider: image.provider.clone(),
            runtime_resident_provider: report.process.resident_engine_provider.clone(),
            runtime_serving_image_provider: report.process.serving_image_provider.clone(),
            tail_sha256: image.tail_sha256.clone(),
            resident_logical_sha256: image.resident_logical_sha256.clone(),
            report_semantic_sha256: report.semantic_sha256.clone(),
            trace_sha256: hot_read.trace_sha256.clone(),
            subject: t27_observed_subject(hot_read.subject),
            effective_engine_options_sha256: sample.storage.effective_engine_options_sha256.clone(),
            engine_topology: sample.storage.engine_topology.clone(),
            database_count: sample.storage.database_count,
            block_cache_count: sample.storage.block_cache_count,
            implicit_block_cache_count: sample.storage.implicit_block_cache_count,
            column_family_count: sample.storage.column_family_count,
            metadata_cache_disabled: sample.storage.metadata_cache_disabled,
            direct_reads: sample.storage.direct_reads,
            block_cache_capacity_bytes: sample.storage.block_cache_capacity_bytes,
            block_cache_usage_bytes: sample.storage.block_cache_usage_bytes,
            block_cache_misses: sample.storage.block_cache_misses,
            resident_image_local_bytes: image.local_bytes,
            operations_per_second: sample.operations_per_second,
            latency_ns_p99: sample.latency_ns_p99,
            cpu_nanoseconds_per_read: sample.process.cpu_nanoseconds_per_read,
            physical_read_bytes: sample.process.physical_read_bytes,
            read_amplification_ratio: sample.storage.read_amplification_ratio,
            flush_write_bytes: sample.storage.flush_write_bytes,
            compaction_read_bytes: sample.storage.compaction_read_bytes,
            compaction_write_bytes: sample.storage.compaction_write_bytes,
            correctness_failures: sample.correctness_failures,
            object_requests: sample.object_requests,
            scratch_was_empty: report.process.scratch_was_empty && image.scratch_was_empty,
            process_cpu_supported: sample.process.process_cpu_supported,
            linux_proc_supported: sample.process.linux_proc_supported,
            raw_report_sha256: sha256(&raw_report_bytes),
        },
    );
    receipt.validate(&plan)?;
    Ok(T27PositionExecutionOutputV1 {
        receipt,
        raw_report: report,
    })
}

#[allow(clippy::too_many_lines)]
fn run_t27_positions_controller(
    plan_path: &Path,
    expected_plan_sha256: &str,
    stratum_id: Option<&str>,
    runtime_cargo_lock: &Path,
    machine_receipt: &Path,
    scratch_root: &Path,
    output_dir: &Path,
) -> Result<T27ControllerRunOutcome, Box<dyn Error>> {
    let plan = decode_t27_execution_plan(&fs::read(plan_path)?, expected_plan_sha256)?;
    let positions = plan
        .positions
        .iter()
        .filter(|position| stratum_id.is_none_or(|value| position.stratum_id == value))
        .collect::<Vec<_>>();
    if positions.is_empty() {
        return Err(std::io::Error::other("T27 stratum is absent from the immutable plan").into());
    }
    let observed_execution = capture_t27_execution_envelope(
        &plan.execution.runtime_source_sha256,
        runtime_cargo_lock,
        machine_receipt,
        scratch_root,
    )?;
    if observed_execution != plan.execution {
        return Err(std::io::Error::other("T27 controller execution envelope mismatch").into());
    }
    let controller_id = Uuid::new_v4().to_string();
    let host_lease = acquire_t27_host_lease(&plan, &controller_id)?;
    let telemetry_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            std::io::Error::other(
                "T27 requires OTEL_EXPORTER_OTLP_ENDPOINT for logs, metrics, and traces",
            )
        })?;
    let telemetry_endpoint_sha256 = sha256(telemetry_endpoint.as_bytes());
    let telemetry_config = TelemetryConfig {
        protocol: "otlp-http".to_owned(),
        endpoint_env: "OTEL_EXPORTER_OTLP_ENDPOINT".to_owned(),
        required_signals: vec!["logs".to_owned(), "metrics".to_owned(), "traces".to_owned()],
        required_for_profiles: vec!["t27".to_owned()],
    };
    let metric_registry: MetricRegistry =
        toml::from_str(include_str!("../../../evals/metrics.toml"))?;
    let profile_id = stratum_id.map_or_else(|| "t27".to_owned(), |value| format!("t27-{value}"));
    let telemetry_resource = RunResource {
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        environment: "objectkv-dev-gcs".to_owned(),
        run_id: controller_id.clone(),
        batch_id: controller_id.clone(),
        suite_id: "t27-native-vs-direct-rocksdb".to_owned(),
        suite_hash: plan.plan_sha256.clone(),
        profile_id: profile_id.clone(),
        profile_hash: plan.plan_sha256.clone(),
        candidate_commit: plan.execution.runtime_source_sha256.clone(),
        backend: "gcs+nvme+rocksdb".to_owned(),
    };
    let telemetry = Telemetry::init(&telemetry_config, "t27", &telemetry_resource)?;
    let mut telemetry_recorder = telemetry.recorder(&metric_registry);
    let run_span = info_span!(
        "okv.eval.t27.run",
        run.id = %controller_id,
        plan.sha256 = %plan.plan_sha256,
        stratum.id = stratum_id.unwrap_or("all"),
        profile = ?plan.profile
    );
    let run_guard = run_span.enter();
    info!("T27 fresh-process plan started");
    fs::create_dir(output_dir)?;
    let started_at = Utc::now().to_rfc3339();
    let lease = serde_json::to_vec_pretty(&serde_json::json!({
        "schema_version": 1,
        "controller_id": &controller_id,
        "lease_id": &host_lease.lease_id,
        "host_lease_path": &host_lease.path,
        "plan_sha256": &plan.plan_sha256,
        "stratum_id": stratum_id,
        "started_at": &started_at,
        "process_id": std::process::id()
    }))?;
    write_new_file(&output_dir.join("controller-lease.json"), &lease)?;
    let executable = std::env::current_exe()?;
    let mut receipts = Vec::with_capacity(positions.len());
    for position in positions {
        let worker_id = Uuid::new_v4().to_string();
        let position_span = info_span!(
            "okv.eval.t27.position",
            position.ordinal = position.ordinal,
            position.stratum = %position.stratum_id,
            position.subject = ?position.subject,
            worker.id = %worker_id
        );
        let _position_guard = position_span.enter();
        let child = Command::new(&executable)
            .arg("t27-plan-position-gcs")
            .arg("--plan")
            .arg(plan_path)
            .arg("--expected-plan-sha256")
            .arg(expected_plan_sha256)
            .arg("--ordinal")
            .arg(position.ordinal.to_string())
            .arg("--controller-id")
            .arg(&controller_id)
            .arg("--worker-id")
            .arg(&worker_id)
            .arg("--runtime-cargo-lock")
            .arg(runtime_cargo_lock)
            .arg("--machine-receipt")
            .arg(machine_receipt)
            .arg("--scratch-root")
            .arg(scratch_root)
            .arg("--lease-id")
            .arg(&host_lease.lease_id)
            .env("OKV_EVAL_SERVING_SCRATCH_ROOT", scratch_root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let process_id = child.id();
        let output = child.wait_with_output()?;
        if !output.status.success() {
            let failure_path =
                output_dir.join(format!("position-{:03}-failure.txt", position.ordinal));
            let mut failure = output.stdout;
            failure.extend_from_slice(&output.stderr);
            write_new_file(&failure_path, &failure)?;
            return Err(std::io::Error::other(format!(
                "T27 position {} failed; evidence is at {}",
                position.ordinal,
                failure_path.display()
            ))
            .into());
        }
        let position_output: T27PositionExecutionOutputV1 = serde_json::from_slice(&output.stdout)?;
        let receipt = position_output.receipt;
        receipt.validate(&plan)?;
        if receipt.controller_id != controller_id
            || receipt.worker_id != worker_id
            || receipt.wrapper_process_id != process_id
            || receipt.lease_id != host_lease.lease_id
        {
            return Err(std::io::Error::other(format!(
                "T27 position {} process identity mismatch",
                position.ordinal
            ))
            .into());
        }
        let raw_report_bytes = serde_json::to_vec(&position_output.raw_report)?;
        if sha256(&raw_report_bytes) != receipt.raw_report_sha256 {
            return Err(std::io::Error::other(format!(
                "T27 position {} raw report digest mismatch",
                position.ordinal
            ))
            .into());
        }
        let raw_report_path = output_dir.join(format!("position-{:03}.raw.json", position.ordinal));
        write_new_file(&raw_report_path, &raw_report_bytes)?;
        let receipt_path = output_dir.join(format!("position-{:03}.json", position.ordinal));
        write_new_file(&receipt_path, &serde_json::to_vec_pretty(&receipt)?)?;
        record_t27_position_telemetry(&mut telemetry_recorder, &receipt)?;
        info!(
            operations_per_second = receipt.operations_per_second,
            latency_ns_p99 = receipt.latency_ns_p99,
            block_cache_misses = receipt.block_cache_misses,
            physical_read_bytes = receipt.physical_read_bytes,
            "T27 fresh-process position recorded"
        );
        receipts.push(receipt);
        require_t27_infrastructure_lease(plan.execution.infrastructure_lease_expires_epoch)?;
        validate_t27_host_lease(&plan, &controller_id, &host_lease.lease_id)?;
    }
    info!("T27 fresh-process measurements completed; flushing telemetry exporters");
    drop(telemetry_recorder);
    drop(run_guard);
    let telemetry_flush = telemetry.shutdown();
    let finished_at = Utc::now().to_rfc3339();
    let lease_id = host_lease.lease_id.clone();
    let lease_acquired_at = host_lease.acquired_at.clone();
    let lease_released_at = host_lease.release()?;
    Ok(T27ControllerRunOutcome {
        plan,
        receipts,
        started_at,
        finished_at,
        lease_id,
        lease_acquired_at,
        lease_released_at,
        telemetry_endpoint_sha256,
        telemetry_flush,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_t27_plan_controller(
    plan_path: &Path,
    expected_plan_sha256: &str,
    runtime_cargo_lock: &Path,
    machine_receipt: &Path,
    scratch_root: &Path,
    output_dir: &Path,
) -> Result<T27PlanRunReceiptV1, Box<dyn Error>> {
    let outcome = run_t27_positions_controller(
        plan_path,
        expected_plan_sha256,
        None,
        runtime_cargo_lock,
        machine_receipt,
        scratch_root,
        output_dir,
    )?;
    let run_receipt = T27PlanRunReceiptV1::new(
        &outcome.plan,
        &outcome.receipts,
        outcome.started_at,
        outcome.finished_at,
        outcome.lease_id,
        outcome.lease_acquired_at,
        outcome.lease_released_at,
        outcome.telemetry_endpoint_sha256,
        outcome.telemetry_flush,
    )?;
    write_new_file(
        &output_dir.join("plan-run.json"),
        &serde_json::to_vec_pretty(&run_receipt)?,
    )?;
    let passed = run_receipt.passed;
    if !passed {
        return Err(std::io::Error::other(format!(
            "T27 plan failed one or more ABBA or telemetry admission gates; sealed evidence is at {}",
            output_dir.join("plan-run.json").display()
        ))
        .into());
    }
    Ok(run_receipt)
}

#[allow(clippy::too_many_arguments)]
fn run_t27_stratum_controller(
    plan_path: &Path,
    expected_plan_sha256: &str,
    stratum_id: &str,
    runtime_cargo_lock: &Path,
    machine_receipt: &Path,
    scratch_root: &Path,
    output_dir: &Path,
) -> Result<T27StratumRunReceiptV1, Box<dyn Error>> {
    let outcome = run_t27_positions_controller(
        plan_path,
        expected_plan_sha256,
        Some(stratum_id),
        runtime_cargo_lock,
        machine_receipt,
        scratch_root,
        output_dir,
    )?;
    let run_receipt = T27StratumRunReceiptV1::new(
        &outcome.plan,
        stratum_id.to_owned(),
        &outcome.receipts,
        outcome.started_at,
        outcome.finished_at,
        outcome.lease_id,
        outcome.lease_acquired_at,
        outcome.lease_released_at,
        outcome.telemetry_endpoint_sha256,
        outcome.telemetry_flush,
    )?;
    write_new_file(
        &output_dir.join("stratum-run.json"),
        &serde_json::to_vec_pretty(&run_receipt)?,
    )?;
    if !run_receipt.passed {
        return Err(std::io::Error::other(format!(
            "T27 stratum failed one or more ABBA or telemetry admission gates; sealed evidence is at {}",
            output_dir.join("stratum-run.json").display()
        ))
        .into());
    }
    Ok(run_receipt)
}

fn run_t27_aggregate(
    manifest_path: &Path,
    output: &Path,
) -> Result<T27AggregateRunReceiptV1, Box<dyn Error>> {
    let manifest: T27AggregateManifestV1 = serde_json::from_slice(&fs::read(manifest_path)?)?;
    if manifest.schema_version != 1 {
        return Err(std::io::Error::other("unsupported T27 aggregate manifest version").into());
    }
    let base = manifest_path
        .parent()
        .ok_or_else(|| std::io::Error::other("T27 aggregate manifest has no parent directory"))?;
    let resolve = |path: &Path| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            base.join(path)
        }
    };
    let mut bundles = Vec::with_capacity(manifest.bundles.len());
    for bundle in manifest.bundles {
        let plan = decode_t27_execution_plan(
            &fs::read(resolve(&bundle.plan))?,
            &bundle.expected_plan_sha256,
        )?;
        let receipt: T27StratumRunReceiptV1 =
            serde_json::from_slice(&fs::read(resolve(&bundle.stratum_receipt))?)?;
        let mut positions = Vec::with_capacity(bundle.position_receipts.len());
        for path in &bundle.position_receipts {
            positions.push(serde_json::from_slice::<T27PositionReceiptV1>(&fs::read(
                resolve(path),
            )?)?);
        }
        bundles.push(T27LoadedAggregateBundleV1 {
            plan,
            receipt,
            positions,
        });
    }
    let evidence = bundles
        .iter()
        .map(|bundle| T27StratumEvidenceV1 {
            plan: &bundle.plan,
            receipt: &bundle.receipt,
            positions: &bundle.positions,
        })
        .collect::<Vec<_>>();
    let receipt = T27AggregateRunReceiptV1::new(&evidence)?;
    write_new_file(output, &serde_json::to_vec_pretty(&receipt)?)?;
    if !receipt.passed {
        return Err(std::io::Error::other(format!(
            "T27 aggregate contains a failed stratum; sealed evidence is at {}",
            output.display()
        ))
        .into());
    }
    Ok(receipt)
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn record_t27_position_telemetry(
    recorder: &mut MetricRecorder,
    receipt: &T27PositionReceiptV1,
) -> Result<(), Box<dyn Error>> {
    let sample = receipt.position.ordinal.to_string();
    let workload = match receipt.observed_subject {
        T27PlanSubjectV1::NativeSnapshot => "t27-native-snapshot",
        T27PlanSubjectV1::DirectOwnedRocksdb => "t27-direct-owned-rocksdb",
    };
    let lane = "cache-pressure";
    let backend = "gcs+nvme+rocksdb";
    let operation = "resident-point-read";
    recorder.record(
        "operation.throughput",
        receipt.operations_per_second,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("operation", operation),
            ("backend", backend),
            ("sample", &sample),
        ]),
    )?;
    recorder.record(
        "operation.duration",
        receipt.latency_ns_p99 as f64 / 1_000_000_000.0,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("operation", operation),
            ("backend", backend),
            ("result", "success"),
            ("sample", &sample),
        ]),
    )?;
    recorder.record(
        "process.cpu_per_operation",
        receipt.cpu_nanoseconds_per_read,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("backend", backend),
            ("operation", operation),
            ("sample", &sample),
        ]),
    )?;
    recorder.record(
        "process.io_bytes",
        receipt.physical_read_bytes as f64,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("backend", backend),
            ("io.class", "physical-read"),
            ("sample", &sample),
        ]),
    )?;
    recorder.record(
        "rocksdb.cache_requests",
        receipt.block_cache_misses as f64,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("backend", backend),
            ("block.class", "all"),
            ("result", "miss"),
            ("sample", &sample),
        ]),
    )?;
    recorder.record(
        "rocksdb.read_amplification",
        receipt.read_amplification_ratio,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("backend", backend),
            ("sample", &sample),
        ]),
    )?;
    if let Some(local_bytes) = receipt.resident_image_local_bytes {
        recorder.record(
            "serving.local_bytes",
            local_bytes as f64,
            t27_local_bytes_attributes(lane, workload, backend, &sample),
        )?;
    }
    recorder.record(
        "correctness.failures",
        receipt.correctness_failures as f64,
        attributes(&[("lane", lane), ("workload", workload)]),
    )?;
    recorder.record(
        "object_store.requests",
        receipt.object_requests as f64,
        attributes(&[
            ("lane", lane),
            ("workload", workload),
            ("backend", backend),
            ("store", "object-base"),
            ("api", "get"),
            ("result", "attempted"),
            ("sample", &sample),
        ]),
    )?;
    Ok(())
}

fn t27_local_bytes_attributes(
    lane: &str,
    workload: &str,
    backend: &str,
    sample: &str,
) -> BTreeMap<String, String> {
    attributes(&[
        ("lane", lane),
        ("workload", workload),
        ("backend", backend),
        ("cache.tier", "nvme-rocksdb"),
        ("range.class", "complete-resident"),
        ("sample", sample),
    ])
}

fn acquire_t27_host_lease(
    plan: &okv_eval::t27_plan::T27ExecutionPlanV1,
    controller_id: &str,
) -> Result<T27HostLease, Box<dyn Error>> {
    acquire_t27_host_lease_at(
        Path::new(&plan.execution.host_lease_path),
        plan.execution.infrastructure_lease_expires_epoch,
        &plan.plan_sha256,
        &plan.execution.linux_boot_id,
        controller_id,
    )
}

fn acquire_t27_host_lease_at(
    path: &Path,
    infrastructure_lease_expires_epoch: u64,
    plan_sha256: &str,
    linux_boot_id: &str,
    controller_id: &str,
) -> Result<T27HostLease, Box<dyn Error>> {
    require_t27_infrastructure_lease(infrastructure_lease_expires_epoch)?;
    let path = path.to_path_buf();
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("T27 host lease has no parent directory"))?;
    if !parent.is_dir() {
        return Err(std::io::Error::other("T27 host lease parent does not exist").into());
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let mut file = Flock::lock(file, FlockArg::LockExclusiveNonblock)
        .map_err(|(_, error)| std::io::Error::other(format!("T27 host lease is busy: {error}")))?;
    let lease_id = Uuid::new_v4().to_string();
    let acquired_at = Utc::now().to_rfc3339();
    let body = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "status": "active",
        "lease_id": &lease_id,
        "controller_id": controller_id,
        "controller_process_id": std::process::id(),
        "plan_sha256": plan_sha256,
        "linux_boot_id": linux_boot_id,
        "acquired_at": &acquired_at
    }))?;
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&body)?;
    file.sync_all()?;
    Ok(T27HostLease {
        file,
        lease_id,
        acquired_at,
        path,
    })
}

fn validate_t27_host_lease(
    plan: &okv_eval::t27_plan::T27ExecutionPlanV1,
    controller_id: &str,
    lease_id: &str,
) -> Result<(), Box<dyn Error>> {
    require_t27_infrastructure_lease(plan.execution.infrastructure_lease_expires_epoch)?;
    let path = Path::new(&plan.execution.host_lease_path);
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    if value.get("status").and_then(serde_json::Value::as_str) != Some("active")
        || value.get("lease_id").and_then(serde_json::Value::as_str) != Some(lease_id)
        || value
            .get("controller_id")
            .and_then(serde_json::Value::as_str)
            != Some(controller_id)
        || value.get("plan_sha256").and_then(serde_json::Value::as_str)
            != Some(plan.plan_sha256.as_str())
        || value
            .get("linux_boot_id")
            .and_then(serde_json::Value::as_str)
            != Some(plan.execution.linux_boot_id.as_str())
    {
        return Err(std::io::Error::other("T27 host lease identity mismatch").into());
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(unexpected_lock) => {
            drop(unexpected_lock);
            Err(std::io::Error::other("T27 host lease is not actively locked").into())
        }
        Err((_file, error)) if error == nix::errno::Errno::EWOULDBLOCK => Ok(()),
        Err((_file, error)) => Err(std::io::Error::other(format!(
            "T27 host lease could not be validated: {error}"
        ))
        .into()),
    }
}

impl T27HostLease {
    fn release(mut self) -> Result<String, Box<dyn Error>> {
        let released_at = Utc::now().to_rfc3339();
        let body = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "status": "released",
            "lease_id": &self.lease_id,
            "released_at": &released_at
        }))?;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&body)?;
        self.file.sync_all()?;
        drop(self.file);
        Ok(released_at)
    }
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn parse_object_frontier_mode(value: &str) -> Result<ObjectFrontierMode, String> {
    match value {
        "candidate" | "correct" => Ok(ObjectFrontierMode::Candidate),
        "missing_pending_control" | "missing_pending" => {
            Ok(ObjectFrontierMode::MissingPendingControl)
        }
        "forged_coverage_control" | "forged_coverage" => {
            Ok(ObjectFrontierMode::ForgedCoverageControl)
        }
        "subquorum_control" | "subquorum" => Ok(ObjectFrontierMode::SubquorumControl),
        other => Err(format!("unknown object-frontier mode {other}")),
    }
}

fn parse_fixture_anchor_mode(value: &str) -> Result<FixtureAnchorMode, String> {
    match value {
        "candidate" => Ok(FixtureAnchorMode::Candidate),
        "second_identity_bypass_poison" => Ok(FixtureAnchorMode::SecondIdentityBypassPoison),
        other => Err(format!("unknown fixture-anchor mode {other}")),
    }
}

fn parse_object_fixture_mode(value: &str) -> Result<ObjectFixtureMode, String> {
    match value {
        "candidate" => Ok(ObjectFixtureMode::Candidate),
        "corrupt_descriptor_poison" => Ok(ObjectFixtureMode::CorruptDescriptorPoison),
        "mutated_anchor_poison" => Ok(ObjectFixtureMode::MutatedAnchorPoison),
        "tail_mismatch_poison" => Ok(ObjectFixtureMode::TailMismatchPoison),
        "shared_mutable_image_poison" => Ok(ObjectFixtureMode::SharedMutableImagePoison),
        other => Err(format!("unknown object-fixture mode {other}")),
    }
}

fn parse_commit_proxy_object_frontier_mode(
    value: &str,
) -> Result<CommitProxyObjectFrontierMode, String> {
    match value {
        "quarter_conflict_candidate" | "candidate" => {
            Ok(CommitProxyObjectFrontierMode::QuarterConflictCandidate)
        }
        "no_conflict_control" | "no_conflict" => {
            Ok(CommitProxyObjectFrontierMode::NoConflictControl)
        }
        "high_conflict_control" | "high_conflict" => {
            Ok(CommitProxyObjectFrontierMode::HighConflictControl)
        }
        "one_entry_same_durability_control" | "one_entry" => {
            Ok(CommitProxyObjectFrontierMode::OneEntrySameDurabilityControl)
        }
        "moving_frontier_poison" | "moving_frontier" => {
            Ok(CommitProxyObjectFrontierMode::MovingFrontierPoison)
        }
        "premature_pop_poison" | "premature_pop" => {
            Ok(CommitProxyObjectFrontierMode::PrematurePopPoison)
        }
        other => Err(format!("unknown commit-proxy object-frontier mode {other}")),
    }
}

fn parse_frontiered_process_snapshot_mode(
    value: &str,
) -> Result<FrontieredProcessSnapshotMode, String> {
    match value {
        "aligned_r_q_o_candidate" => Ok(FrontieredProcessSnapshotMode::AlignedRqoCandidate),
        "no_retry_frontier_control" => Ok(FrontieredProcessSnapshotMode::NoRetryFrontierControl),
        "omit_retry_and_recovery_state_from_accounting" | "accounting_poison" => {
            Ok(FrontieredProcessSnapshotMode::AccountingPoison)
        }
        other => Err(format!(
            "unknown frontiered process-snapshot subject {other}"
        )),
    }
}

fn parse_storage_layout_mode(value: &str) -> Result<StorageLayoutMode, String> {
    match value {
        "indexed_row_object_control" => Ok(StorageLayoutMode::IndexedRowObjectControl),
        "indexed_parquet_control" => Ok(StorageLayoutMode::IndexedParquetControl),
        "coalesced_parquet_candidate" => Ok(StorageLayoutMode::CoalescedParquetCandidate),
        "split_projection_sidecar_candidate" => {
            Ok(StorageLayoutMode::SplitProjectionSidecarCandidate)
        }
        "hybrid_columnar_candidate" => Ok(StorageLayoutMode::HybridColumnarCandidate),
        "columnar_range_overlay_candidate" => Ok(StorageLayoutMode::ColumnarRangeOverlayCandidate),
        "scan_complete_parquet_object_for_point" => {
            Ok(StorageLayoutMode::ParquetFullFilePointPoison)
        }
        "omit_row_capsule_bytes" => Ok(StorageLayoutMode::HybridAccountingPoison),
        "apply_predicate_before_invalidation" => Ok(StorageLayoutMode::ColumnarInvalidationPoison),
        other => Err(format!("unknown storage-layout subject {other}")),
    }
}

fn parse_commit_group_mode(value: &str) -> Result<CommitGroupMode, String> {
    match value {
        "candidate" | "correct" => Ok(CommitGroupMode::Candidate),
        "sequential_control" | "control" => Ok(CommitGroupMode::SequentialControl),
        "acknowledge_before_quorum" | "early_ack_poison" => Ok(CommitGroupMode::EarlyAckPoison),
        other => Err(format!("unknown commit-group mode {other}")),
    }
}

fn run_commit_group(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "data-openraft-local-process+stable-journal";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "commit-group runner requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err("commit-group runner requires a dataset".to_owned()));
    };
    if seeds.is_empty() || dataset.key_count == 0 {
        return execution_from_result(Err(
            "commit-group runner requires fixed seeds and live keys".to_owned(),
        ));
    }
    let integer = |name: &str| -> Result<u64, String> {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("commit-group profile requires integer {name}"))
    };
    let float = |name: &str| -> Result<f64, String> {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("commit-group profile requires float {name}"))
    };
    let max_p99 = match float("candidate_max_commit_p99_seconds") {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "candidate_max_commit_p99_seconds must be finite and positive".to_owned(),
            ));
        }
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile = match (|| {
        Ok::<_, String>(CommitGroupProfile {
            live_keys: dataset.key_count,
            value_bytes: usize::try_from(integer("value_bytes")?)
                .map_err(|error| error.to_string())?,
            transaction_count: integer("transaction_count")?,
            candidate_max_in_flight: usize::try_from(integer("candidate_max_in_flight")?)
                .map_err(|error| error.to_string())?,
            control_max_in_flight: usize::try_from(integer("control_max_in_flight")?)
                .map_err(|error| error.to_string())?,
            candidate_min_transactions_per_second: integer(
                "candidate_min_transactions_per_second",
            )?,
            candidate_min_entries_per_append: integer("candidate_min_entries_per_append")?,
            candidate_max_commit_p99_micros: u64::try_from(
                std::time::Duration::from_secs_f64(max_p99).as_micros(),
            )
            .map_err(|error| error.to_string())?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let mode = match parse_commit_group_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_commit_group_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_commit_group_contract(seeds[0], mode, &profile, &executable) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    commit_group_execution(
        workload,
        backend,
        mode,
        &profile,
        &reports,
        &replay,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_lines)]
fn commit_group_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: CommitGroupMode,
    profile: &CommitGroupProfile,
    reports: &[CommitGroupReport],
    replay: &CommitGroupReport,
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| report.authority_processes == 3);
    let release_build = reports.iter().all(|report| report.release_build);
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let normal_exact = reports.iter().all(|report| {
        report.committed_count == profile.transaction_count
            && report.commit_versions_unique_and_increasing
            && report.retained_stream_complete
            && report.exact_final_values
            && report.exact_retry
            && report.leader_failover_exact
            && report.restarted_voter_exact
            && report.correctness_anomalies == 0
    });
    let poison_detected = reports.iter().all(|report| {
        report.early_ack_observed
            && report.early_ack_missing_after_quorum_recovery
            && report.correctness_anomalies > 0
    });
    let min_throughput = resident_count_as_f64(profile.candidate_min_transactions_per_second);
    let min_entries_per_append = resident_count_as_f64(profile.candidate_min_entries_per_append);
    let max_p99 = resident_count_as_f64(profile.candidate_max_commit_p99_micros) / 1_000_000.0;
    let candidate_throughput = mode != CommitGroupMode::Candidate
        || reports
            .iter()
            .all(|report| report.transactions_per_second >= min_throughput);
    let candidate_batching = mode != CommitGroupMode::Candidate
        || reports
            .iter()
            .all(|report| report.median_entries_per_append >= min_entries_per_append);
    let candidate_latency = mode != CommitGroupMode::Candidate
        || reports
            .iter()
            .all(|report| report.commit_p99_seconds <= max_p99);
    let error = if !topology_exact {
        Some("commit-group runner did not start three authority processes".to_owned())
    } else if !release_build {
        Some("commit-group runner requires a release executable".to_owned())
    } else if mode == CommitGroupMode::EarlyAckPoison {
        if poison_detected {
            Some("early acknowledgement was absent from recovered quorum state".to_owned())
        } else {
            Some("early-ack poison was not detected".to_owned())
        }
    } else if !normal_exact {
        Some("commit-group correctness or recovery contract failed".to_owned())
    } else if !candidate_throughput || !candidate_batching || !candidate_latency {
        Some("commit-group candidate missed a frozen performance gate".to_owned())
    } else if !exact_replay {
        Some("commit-group fresh-controller semantic replay changed".to_owned())
    } else {
        None
    };
    let mut measurements = Vec::new();
    for report in reports {
        let window_class = mode.id();
        measurements.extend([
            Measurement {
                metric: "commit.throughput",
                value: report.transactions_per_second,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", window_class),
                ]),
            },
            Measurement {
                metric: "commit.latency",
                value: report.commit_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", window_class),
                    (
                        "result",
                        if report.correctness_anomalies == 0 {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: resident_count_as_f64(report.correctness_anomalies),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "commit-group-v1"),
                    (
                        "anomaly.class",
                        if report.correctness_anomalies == 0 {
                            "none"
                        } else {
                            "acknowledged_without_quorum_recovery"
                        },
                    ),
                ]),
            },
        ]);
        for node in &report.node_io {
            let node_class = format!("voter-{}", node.node_id);
            measurements.push(Measurement {
                metric: "raft_log.entries_per_append",
                value: node.entries_per_append,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("node.class", &node_class),
                ]),
            });
            for (io_class, seconds) in [
                ("append", node.append_durable_seconds),
                ("committed_marker", node.committed_durable_seconds),
            ] {
                measurements.push(Measurement {
                    metric: "raft_log.durable_io_duration",
                    value: seconds,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("node.class", &node_class),
                        ("io.class", io_class),
                    ]),
                });
            }
        }
    }
    let first = reports.first();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "strict_serializable_recovery_contract".to_owned(),
                status: gate_status(mode == CommitGroupMode::EarlyAckPoison || normal_exact),
                detail: first.map(|report| {
                    format!(
                        "committed={},retained={},retry={},failover={},restart={}",
                        report.committed_count,
                        report.retained_stream_complete,
                        report.exact_retry,
                        report.leader_failover_exact,
                        report.restarted_voter_exact
                    )
                }),
            },
            HardGateResult {
                id: "candidate_min_transactions_per_second".to_owned(),
                status: gate_status(candidate_throughput),
                detail: Some(format!(
                    "minimum={min_throughput},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_min_entries_per_append".to_owned(),
                status: gate_status(candidate_batching),
                detail: Some(format!(
                    "minimum={min_entries_per_append},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.median_entries_per_append)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_max_commit_p99_seconds".to_owned(),
                status: gate_status(candidate_latency),
                detail: Some(format!(
                    "maximum={max_p99},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.commit_p99_seconds)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "early_ack_poison_detected".to_owned(),
                status: gate_status(mode != CommitGroupMode::EarlyAckPoison || poison_detected),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "fresh_controller_semantic_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: first.map(|report| report.semantic_sha256.clone()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://commit-group-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "commit_group.throughput.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "commit_group.p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "commit_group.entries_per_append.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.median_entries_per_append)
                        .collect::<Vec<_>>(),
                ),
            ),
            ("commit_group.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_commit_proxy_object_frontier(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "object-store-local-fs+publication-openraft+data-openraft+stable-journal";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "commit-proxy object-frontier requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "commit-proxy object-frontier requires fixed seeds".to_owned()
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("commit-proxy object-frontier profile requires integer {name}"))
    };
    let float = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("commit-proxy object-frontier profile requires float {name}"))
    };
    let usize_parameter = |name: &str| {
        integer(name).and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    };
    let profile = match (|| {
        Ok::<_, String>(CommitProxyObjectFrontierProfile {
            prefix_transaction_count: integer("prefix_transaction_count")?,
            suffix_transaction_count: integer("suffix_transaction_count")?,
            value_bytes: usize_parameter("value_bytes")?,
            concurrent_clients: usize_parameter("concurrent_clients")?,
            max_batch_items: usize_parameter("max_batch_items")?,
            max_entry_bytes: usize_parameter("max_entry_bytes")?,
            max_batch_delay_micros: integer("max_batch_delay_micros")?,
            queue_capacity: usize_parameter("queue_capacity")?,
            hot_key_count: integer("hot_key_count")?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("quarter_conflict_candidate");
    let mode = match parse_commit_proxy_object_frontier_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_commit_proxy_object_frontier_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let candidate_min_throughput = match integer("candidate_min_resolved_per_second") {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let no_conflict_min_throughput = match integer("no_conflict_min_resolved_per_second") {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let candidate_min_density = match integer("candidate_min_logical_transactions_per_append") {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let candidate_max_p99 = match float("candidate_max_commit_p99_seconds") {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "candidate_max_commit_p99_seconds must be finite and positive".to_owned(),
            ));
        }
        Err(error) => return execution_from_result(Err(error)),
    };
    let max_frontier_seconds = match float("max_frontier_protocol_seconds") {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "max_frontier_protocol_seconds must be finite and positive".to_owned(),
            ));
        }
        Err(error) => return execution_from_result(Err(error)),
    };
    commit_proxy_object_frontier_execution(
        workload,
        backend,
        mode,
        &reports,
        candidate_min_throughput,
        no_conflict_min_throughput,
        candidate_min_density,
        candidate_max_p99,
        max_frontier_seconds,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn commit_proxy_object_frontier_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: CommitProxyObjectFrontierMode,
    reports: &[CommitProxyObjectFrontierReport],
    candidate_min_throughput: f64,
    no_conflict_min_throughput: f64,
    candidate_min_density: f64,
    candidate_max_p99: f64,
    max_frontier_seconds: f64,
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| {
        report.publication_authority_processes == 3 && report.data_authority_processes == 3
    });
    let release_build = reports.iter().all(|report| report.release_build);
    let exact = reports
        .iter()
        .all(|report| report.correctness_anomalies == 0);
    let no_backpressure = reports
        .iter()
        .all(|report| report.suffix_backpressure_rejections == 0);
    let foreground_is_object_free = reports
        .iter()
        .all(|report| report.foreground_object_requests == 0);
    let observed_overlap = reports
        .iter()
        .all(|report| report.suffix_resolutions_before_activation > 0);
    let frontier_bounded = reports
        .iter()
        .all(|report| report.frontier_protocol_seconds <= max_frontier_seconds);
    let candidate_performance = mode != CommitProxyObjectFrontierMode::QuarterConflictCandidate
        || reports.iter().all(|report| {
            report.resolved_outcomes_per_second >= candidate_min_throughput
                && report.commit_p99_seconds <= candidate_max_p99
                && report.leader_logical_transactions_per_append >= candidate_min_density
        });
    let no_conflict_performance = mode != CommitProxyObjectFrontierMode::NoConflictControl
        || reports
            .iter()
            .all(|report| report.resolved_outcomes_per_second >= no_conflict_min_throughput);
    let positive_exact = !matches!(
        mode,
        CommitProxyObjectFrontierMode::QuarterConflictCandidate
            | CommitProxyObjectFrontierMode::NoConflictControl
            | CommitProxyObjectFrontierMode::HighConflictControl
            | CommitProxyObjectFrontierMode::OneEntrySameDurabilityControl
    ) || reports.iter().all(|report| {
        report.pending_frontier_protected
            && report.closure_validated
            && report.physical_pop_applied
            && report.persisted_retention_floor == report.object_version
            && report.retained_suffix_strictly_newer
            && report.frontier_activation_accepted
            && report.active_frontier_exact
            && report.object_plus_suffix_reconstruction_exact
            && report.data_leader_failover_exact
            && report.publication_leader_failover_exact
            && report.restarted_data_voter_exact
            && report.fresh_controller_reconstruction_exact
    });
    let poison_exact = match mode {
        CommitProxyObjectFrontierMode::MovingFrontierPoison => reports.iter().all(|report| {
            report.moving_frontier_poison_detected
                && report.poison_prefix_retained
                && !report.physical_pop_applied
                && report.persisted_retention_floor == 0
        }),
        CommitProxyObjectFrontierMode::PrematurePopPoison => reports.iter().all(|report| {
            report.premature_pop_poison_detected
                && report.poison_prefix_retained
                && !report.physical_pop_applied
                && report.persisted_retention_floor == 0
        }),
        CommitProxyObjectFrontierMode::QuarterConflictCandidate
        | CommitProxyObjectFrontierMode::NoConflictControl
        | CommitProxyObjectFrontierMode::HighConflictControl
        | CommitProxyObjectFrontierMode::OneEntrySameDurabilityControl => true,
    };
    let error = if !topology_exact {
        Some("G4.10b did not start both frozen three-process quorums".to_owned())
    } else if !release_build {
        Some("G4.10b measured suites require a release executable".to_owned())
    } else if !exact || !positive_exact || !poison_exact {
        Some("G4.10b correctness, recovery, or poison contract failed".to_owned())
    } else if !foreground_is_object_free || !observed_overlap || !frontier_bounded {
        Some("G4.10b separation, overlap, or frontier-duration gate failed".to_owned())
    } else if !no_backpressure {
        Some("G4.10b admitted work encountered foreground backpressure".to_owned())
    } else if !candidate_performance || !no_conflict_performance {
        Some("G4.10b subject missed a frozen absolute performance gate".to_owned())
    } else {
        None
    };

    let mut measurements = Vec::new();
    for report in reports {
        let mean_batch_items = if report.batcher.batches == 0 {
            0.0
        } else {
            resident_count_as_f64(report.batcher.resolved_items)
                / resident_count_as_f64(report.batcher.batches)
        };
        measurements.extend([
            Measurement {
                metric: "commit.throughput",
                value: report.resolved_outcomes_per_second,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                ]),
            },
            Measurement {
                metric: "commit.latency",
                value: report.commit_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                    (
                        "result",
                        if report.correctness_anomalies == 0 {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "commit.logical_transactions_per_append",
                value: report.leader_logical_transactions_per_append,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("node.class", "data-voter-201"),
                ]),
            },
            Measurement {
                metric: "commit.batch_items",
                value: mean_batch_items,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("closure", mode.id()),
                ]),
            },
            Measurement {
                metric: "commit.backpressure_rejections",
                value: resident_count_as_f64(report.suffix_backpressure_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    (
                        "result",
                        if report.suffix_backpressure_rejections == 0 {
                            "none"
                        } else {
                            "queue_full"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.conflicts",
                value: resident_count_as_f64(report.suffix_conflict_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict_serializable"),
                    ("conflict.kind", mode.id()),
                ]),
            },
            Measurement {
                metric: "object_frontier.protocol_duration",
                value: report.frontier_protocol_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("mode", mode.id()),
                    (
                        "result",
                        if report.correctness_anomalies == 0 {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: resident_count_as_f64(report.correctness_anomalies),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "commit-proxy-object-frontier-v1"),
                    (
                        "anomaly.class",
                        if report.correctness_anomalies == 0 {
                            "none"
                        } else {
                            "composition_contract"
                        },
                    ),
                ]),
            },
        ]);
    }
    let throughputs = reports
        .iter()
        .map(|report| report.resolved_outcomes_per_second)
        .collect::<Vec<_>>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_publication_and_three_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("publication=3,data=3".to_owned()),
            },
            HardGateResult {
                id: "exact_object_plus_suffix_recovery".to_owned(),
                status: gate_status(exact && positive_exact),
                detail: Some(mode.id().to_owned()),
            },
            HardGateResult {
                id: "foreground_object_requests_zero".to_owned(),
                status: gate_status(foreground_is_object_free),
                detail: Some("commit path owns no object backend".to_owned()),
            },
            HardGateResult {
                id: "concurrent_suffix_progress_observed".to_owned(),
                status: gate_status(observed_overlap),
                detail: reports.first().map(|report| {
                    format!(
                        "resolved_before_activation={}",
                        report.suffix_resolutions_before_activation
                    )
                }),
            },
            HardGateResult {
                id: "frontier_protocol_bounded".to_owned(),
                status: gate_status(frontier_bounded),
                detail: Some(format!("limit_seconds={max_frontier_seconds}")),
            },
            HardGateResult {
                id: "mode_specific_performance".to_owned(),
                status: gate_status(candidate_performance && no_conflict_performance),
                detail: Some(mode.id().to_owned()),
            },
            HardGateResult {
                id: "unsafe_poison_rejected_before_pop".to_owned(),
                status: gate_status(poison_exact),
                detail: Some(mode.id().to_owned()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://commit-proxy-object-frontier-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "commit_proxy_object_frontier.throughput.median".to_owned(),
                median(&throughputs),
            ),
            (
                "commit_proxy_object_frontier.p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "commit_proxy_object_frontier.append_density.minimum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.leader_logical_transactions_per_append)
                    .reduce(f64::min)
                    .unwrap_or(0.0),
            ),
            (
                "commit_proxy_object_frontier.frontier_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.frontier_protocol_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "commit_proxy_object_frontier.wall_seconds".to_owned(),
                wall_seconds,
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_commit_proxy(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "data-openraft-local-process+stable-journal";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "commit proxy requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err("commit proxy requires fixed seeds".to_owned()));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("commit-proxy profile requires integer {name}"))
    };
    let float = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("commit-proxy profile requires float {name}"))
    };
    let usize_parameter = |name: &str| {
        integer(name).and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    };
    let profile = match (|| {
        Ok::<_, String>(CommitProxyProfile {
            transaction_count: integer("transaction_count")?,
            value_bytes: usize_parameter("value_bytes")?,
            concurrent_clients: usize_parameter("candidate_concurrent_clients")?,
            admission_knee_clients: usize_parameter("admission_knee_clients")?,
            max_batch_items: usize_parameter("max_batch_items")?,
            max_entry_bytes: usize_parameter("max_entry_bytes")?,
            max_batch_delay_micros: integer("max_batch_delay_micros")?,
            queue_capacity: usize_parameter("queue_capacity")?,
            sparse_transaction_count: integer("sparse_transaction_count")?,
            byte_control_transaction_count: integer("byte_control_transaction_count")?,
            byte_control_value_bytes: usize_parameter("byte_control_value_bytes")?,
            byte_control_max_entry_bytes: usize_parameter("byte_control_max_entry_bytes")?,
            overload_transaction_count: integer("overload_transaction_count")?,
            overload_queue_capacity: usize_parameter("overload_queue_capacity")?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("saturated_candidate");
    let mode = match mode_name {
        "saturated_candidate" => CommitProxyMode::SaturatedCandidate,
        "admission_knee_control" => CommitProxyMode::AdmissionKneeControl,
        "sparse_arrival_control" => CommitProxyMode::SparseArrivalControl,
        "byte_bound_control" => CommitProxyMode::ByteBoundControl,
        "overload_control" => CommitProxyMode::OverloadControl,
        "oversized_item_poison" => CommitProxyMode::OversizedItemPoison,
        other => {
            return execution_from_result(Err(format!("unknown commit-proxy subject {other}")));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_commit_proxy_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_commit_proxy_contract(seeds[0], mode, &profile, &executable) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    let min_throughput = match integer("candidate_min_transactions_per_second") {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let min_transactions_per_append = match integer("candidate_min_logical_transactions_per_append")
    {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let max_p99 = match float("candidate_max_commit_p99_seconds") {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "candidate_max_commit_p99_seconds must be finite and positive".to_owned(),
            ));
        }
        Err(error) => return execution_from_result(Err(error)),
    };
    commit_proxy_execution(
        workload,
        backend,
        mode,
        &profile,
        &reports,
        &replay,
        min_throughput,
        min_transactions_per_append,
        max_p99,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn commit_proxy_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: CommitProxyMode,
    profile: &CommitProxyProfile,
    reports: &[CommitProxyReport],
    replay: &CommitProxyReport,
    min_throughput: f64,
    min_transactions_per_append: f64,
    max_p99: f64,
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| report.authority_processes == 3);
    let release_build = reports.iter().all(|report| report.release_build);
    let exact_replay = mode == CommitProxyMode::OverloadControl
        || reports.first().is_some_and(|first| {
            first.semantic_sha256 == replay.semantic_sha256
                && first.correctness_anomalies == replay.correctness_anomalies
        });
    let exact = reports
        .iter()
        .all(|report| report.correctness_anomalies == 0);
    let entry_bytes_bounded = reports.iter().all(|report| {
        report.batcher.max_observed_entry_bytes
            <= u64::try_from(match mode {
                CommitProxyMode::ByteBoundControl => profile.byte_control_max_entry_bytes,
                _ => profile.max_entry_bytes,
            })
            .unwrap_or(u64::MAX)
    });
    let candidate_throughput = mode != CommitProxyMode::SaturatedCandidate
        || reports
            .iter()
            .all(|report| report.transactions_per_second >= min_throughput);
    let candidate_append_density = mode != CommitProxyMode::SaturatedCandidate
        || reports.iter().all(|report| {
            report.leader_logical_transactions_per_append >= min_transactions_per_append
        });
    let candidate_latency = mode != CommitProxyMode::SaturatedCandidate
        || reports
            .iter()
            .all(|report| report.commit_p99_seconds <= max_p99);
    let knee_latency = mode != CommitProxyMode::AdmissionKneeControl
        || reports
            .iter()
            .all(|report| report.commit_p99_seconds <= max_p99);
    let sparse_exact = mode != CommitProxyMode::SparseArrivalControl
        || reports.iter().all(|report| {
            report.batcher.delay_bound_closures > 0
                && report.batcher.max_observed_batch_items == 1
                && report.commit_p99_seconds <= max_p99
        });
    let byte_exact = mode != CommitProxyMode::ByteBoundControl
        || reports.iter().all(|report| {
            report.batcher.byte_bound_closures > 0 && report.batcher.max_observed_batch_items >= 4
        });
    let overload_exact = mode != CommitProxyMode::OverloadControl
        || reports.iter().all(|report| report.overload_was_explicit);
    let oversized_detected = mode != CommitProxyMode::OversizedItemPoison
        || reports
            .iter()
            .all(|report| report.oversized_rejected_before_mutation);
    let error = if !topology_exact {
        Some("commit-proxy runner did not start three authority processes".to_owned())
    } else if !release_build {
        Some("commit-proxy runner requires a release executable".to_owned())
    } else if mode == CommitProxyMode::OversizedItemPoison {
        Some(if oversized_detected {
            "oversized transaction was rejected before admission and mutation".to_owned()
        } else {
            "oversized transaction poison was not detected".to_owned()
        })
    } else if !exact || !entry_bytes_bounded || !exact_replay {
        Some("commit-proxy correctness, byte bound, or replay contract failed".to_owned())
    } else if !candidate_throughput || !candidate_append_density || !candidate_latency {
        Some("commit-proxy saturated candidate missed a frozen performance gate".to_owned())
    } else if !knee_latency || !sparse_exact || !byte_exact || !overload_exact {
        Some("commit-proxy policy control missed its frozen gate".to_owned())
    } else {
        None
    };
    let negative_control = mode == CommitProxyMode::OversizedItemPoison;
    let anomaly_count = if negative_control {
        resident_count_as_f64(u64::try_from(reports.len()).unwrap_or(u64::MAX))
    } else {
        resident_count_as_f64(
            reports
                .iter()
                .map(|report| report.correctness_anomalies)
                .sum(),
        )
    };
    let mut measurements = Vec::new();
    for report in reports {
        measurements.extend([
            Measurement {
                metric: "commit.throughput",
                value: report.transactions_per_second,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                ]),
            },
            Measurement {
                metric: "commit.latency",
                value: report.commit_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                    ("result", if negative_control { "fail" } else { "pass" }),
                ]),
            },
            Measurement {
                metric: "commit.logical_transactions_per_append",
                value: report.leader_logical_transactions_per_append,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("node.class", "voter-201"),
                ]),
            },
            Measurement {
                metric: "commit.batch_items",
                value: report.mean_batch_items,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("closure", mode.id()),
                ]),
            },
            Measurement {
                metric: "commit.batch_entry_bytes",
                value: resident_count_as_f64(report.batcher.max_observed_entry_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("wire.version", "v2"),
                ]),
            },
            Measurement {
                metric: "commit.backpressure_rejections",
                value: resident_count_as_f64(report.backpressure_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    (
                        "result",
                        if report.backpressure_rejections > 0 {
                            "rejected"
                        } else {
                            "accepted"
                        },
                    ),
                ]),
            },
        ]);
    }
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: anomaly_count,
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "commit-proxy-v1"),
            (
                "anomaly.class",
                if negative_control { mode.id() } else { "none" },
            ),
        ]),
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "commit_proxy_correctness_contract".to_owned(),
                status: gate_status(exact),
                detail: reports.first().map(|report| {
                    format!(
                        "attempted={},accepted={},resolved={},anomalies={}",
                        report.attempted_count,
                        report.accepted_count,
                        report.resolved_count,
                        report.correctness_anomalies
                    )
                }),
            },
            HardGateResult {
                id: "entry_byte_bound".to_owned(),
                status: gate_status(entry_bytes_bounded),
                detail: reports.first().map(|report| {
                    format!("max_observed={}", report.batcher.max_observed_entry_bytes)
                }),
            },
            HardGateResult {
                id: "candidate_min_transactions_per_second".to_owned(),
                status: gate_status(candidate_throughput),
                detail: Some(format!(
                    "minimum={min_throughput},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_min_logical_transactions_per_append".to_owned(),
                status: gate_status(candidate_append_density),
                detail: Some(format!(
                    "minimum={min_transactions_per_append},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.leader_logical_transactions_per_append)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_max_commit_p99_seconds".to_owned(),
                status: gate_status(candidate_latency),
                detail: Some(format!(
                    "maximum={max_p99},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.commit_p99_seconds)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "admission_knee_max_commit_p99_seconds".to_owned(),
                status: gate_status(knee_latency),
                detail: Some(format!("maximum={max_p99}")),
            },
            HardGateResult {
                id: "sparse_delay_closure".to_owned(),
                status: gate_status(sparse_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "byte_bound_closure".to_owned(),
                status: gate_status(byte_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "explicit_overload_backpressure".to_owned(),
                status: gate_status(overload_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "oversized_item_poison_detected".to_owned(),
                status: gate_status(oversized_detected),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "fresh_controller_semantic_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: reports.first().map(|report| report.semantic_sha256.clone()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://commit-proxy-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "commit_proxy.throughput.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "commit_proxy.p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "commit_proxy.batch_items.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.mean_batch_items)
                        .collect::<Vec<_>>(),
                ),
            ),
            ("commit_proxy.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_transaction_batch(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "data-openraft-local-process+stable-journal";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "transaction batch requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err("transaction batch requires fixed seeds".to_owned()));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err("transaction batch requires a dataset".to_owned()));
    };
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("transaction-batch profile requires integer {name}"))
    };
    let float = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("transaction-batch profile requires float {name}"))
    };
    let max_p99 = match float("candidate_max_commit_p99_seconds") {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "candidate_max_commit_p99_seconds must be finite and positive".to_owned(),
            ));
        }
        Err(error) => return execution_from_result(Err(error)),
    };
    let batch_profile = match (|| {
        Ok::<_, String>(TransactionBatchProfile {
            live_keys: dataset.key_count,
            value_bytes: usize::try_from(integer("value_bytes")?)
                .map_err(|error| error.to_string())?,
            transaction_count: integer("transaction_count")?,
            transactions_per_batch: usize::try_from(integer("candidate_transactions_per_batch")?)
                .map_err(|error| error.to_string())?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    if mode_name == "single_entry_control" {
        let control_window = match integer("control_max_in_flight")
            .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
        {
            Ok(value) => value,
            Err(error) => return execution_from_result(Err(error)),
        };
        let control_profile = CommitGroupProfile {
            live_keys: batch_profile.live_keys,
            value_bytes: batch_profile.value_bytes,
            transaction_count: batch_profile.transaction_count,
            candidate_max_in_flight: control_window,
            control_max_in_flight: 1,
            candidate_min_transactions_per_second: 1,
            candidate_min_entries_per_append: 1,
            candidate_max_commit_p99_micros: 10_000_000,
        };
        let mut reports = Vec::with_capacity(seeds.len());
        for seed in seeds {
            match run_commit_group_contract(
                *seed,
                CommitGroupMode::Candidate,
                &control_profile,
                &executable,
            ) {
                Ok(report) => reports.push(report),
                Err(error) => return execution_from_result(Err(error)),
            }
        }
        let replay = match run_commit_group_contract(
            seeds[0],
            CommitGroupMode::Candidate,
            &control_profile,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        return transaction_batch_single_entry_control_execution(
            workload,
            backend,
            &control_profile,
            &reports,
            &replay,
            started.elapsed().as_secs_f64(),
        );
    }
    let mode = match mode_name {
        "candidate" => TransactionBatchMode::Candidate,
        "duplicate_identity_control" => TransactionBatchMode::DuplicateIdentityControl,
        "early_ack_poison" => TransactionBatchMode::EarlyAckPoison,
        other => {
            return execution_from_result(Err(format!(
                "unknown transaction-batch subject {other}"
            )));
        }
    };
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_transaction_batch_contract(*seed, mode, &batch_profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_transaction_batch_contract(seeds[0], mode, &batch_profile, &executable) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    let min_throughput = match integer("candidate_min_transactions_per_second") {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    let min_transactions_per_append = match integer("candidate_min_logical_transactions_per_append")
    {
        Ok(value) => resident_count_as_f64(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    transaction_batch_execution(
        workload,
        backend,
        mode,
        &batch_profile,
        &reports,
        &replay,
        min_throughput,
        min_transactions_per_append,
        max_p99,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn transaction_batch_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: TransactionBatchMode,
    profile: &TransactionBatchProfile,
    reports: &[TransactionBatchReport],
    replay: &TransactionBatchReport,
    min_throughput: f64,
    min_transactions_per_append: f64,
    max_p99: f64,
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| report.authority_processes == 3);
    let release_build = reports.iter().all(|report| report.release_build);
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let candidate_exact = reports.iter().all(|report| {
        report.committed_count == profile.transaction_count
            && report.versionstamps_unique_and_increasing
            && report.shared_versions_and_contiguous_orders
            && report.retained_stream_complete
            && report.exact_final_values
            && report.exact_individual_retry
            && report.exact_batch_retry
            && report.in_batch_conflict_detected
            && report.duplicate_identity_rejected_before_mutation
            && report.leader_failover_exact
            && report.restarted_voter_exact
            && report.correctness_anomalies == 0
    });
    let duplicate_detected = reports
        .iter()
        .all(|report| report.duplicate_identity_rejected_before_mutation);
    let poison_detected = reports.iter().all(|report| {
        report.early_ack_observed
            && report.early_ack_missing_after_quorum_recovery
            && report.correctness_anomalies > 0
    });
    let candidate_throughput = mode != TransactionBatchMode::Candidate
        || reports
            .iter()
            .all(|report| report.transactions_per_second >= min_throughput);
    let candidate_append_density = mode != TransactionBatchMode::Candidate
        || reports.iter().all(|report| {
            report.leader_logical_transactions_per_append >= min_transactions_per_append
        });
    let candidate_latency = mode != TransactionBatchMode::Candidate
        || reports
            .iter()
            .all(|report| report.commit_p99_seconds <= max_p99);
    let error = if !topology_exact {
        Some("transaction-batch runner did not start three authority processes".to_owned())
    } else if !release_build {
        Some("transaction-batch runner requires a release executable".to_owned())
    } else if mode == TransactionBatchMode::DuplicateIdentityControl {
        Some(if duplicate_detected {
            "duplicate transaction identity was rejected before mutation".to_owned()
        } else {
            "duplicate transaction identity control was not detected".to_owned()
        })
    } else if mode == TransactionBatchMode::EarlyAckPoison {
        Some(if poison_detected {
            "early batch acknowledgement was absent from recovered quorum state".to_owned()
        } else {
            "early-ack transaction-batch poison was not detected".to_owned()
        })
    } else if !candidate_exact {
        Some("transaction-batch correctness or recovery contract failed".to_owned())
    } else if !candidate_throughput || !candidate_append_density || !candidate_latency {
        Some("transaction-batch candidate missed a frozen performance gate".to_owned())
    } else if !exact_replay {
        Some("transaction-batch fresh-controller semantic replay changed".to_owned())
    } else {
        None
    };
    let negative_control = mode != TransactionBatchMode::Candidate;
    let anomaly_count = if negative_control {
        resident_count_as_f64(u64::try_from(reports.len()).unwrap_or(u64::MAX))
    } else {
        resident_count_as_f64(
            reports
                .iter()
                .map(|report| report.correctness_anomalies)
                .sum(),
        )
    };
    let mut measurements = Vec::new();
    for report in reports {
        measurements.extend([
            Measurement {
                metric: "commit.throughput",
                value: report.transactions_per_second,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                ]),
            },
            Measurement {
                metric: "commit.latency",
                value: report.commit_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", mode.id()),
                    ("result", if negative_control { "fail" } else { "pass" }),
                ]),
            },
            Measurement {
                metric: "commit.logical_transactions_per_append",
                value: report.leader_logical_transactions_per_append,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("node.class", "voter-201"),
                ]),
            },
        ]);
    }
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: anomaly_count,
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "transaction-batch-v1"),
            (
                "anomaly.class",
                if negative_control { mode.id() } else { "none" },
            ),
        ]),
    });
    let first = reports.first();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "transaction_batch_recovery_contract".to_owned(),
                status: gate_status(mode != TransactionBatchMode::Candidate || candidate_exact),
                detail: first.map(|report| {
                    format!(
                        "committed={},versionstamps={},stream={},retry={},failover={},restart={}",
                        report.committed_count,
                        report.versionstamps_unique_and_increasing,
                        report.retained_stream_complete,
                        report.exact_individual_retry && report.exact_batch_retry,
                        report.leader_failover_exact,
                        report.restarted_voter_exact
                    )
                }),
            },
            HardGateResult {
                id: "candidate_min_transactions_per_second".to_owned(),
                status: gate_status(candidate_throughput),
                detail: Some(format!(
                    "minimum={min_throughput},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_min_logical_transactions_per_append".to_owned(),
                status: gate_status(candidate_append_density),
                detail: Some(format!(
                    "minimum={min_transactions_per_append},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.leader_logical_transactions_per_append)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "candidate_max_commit_p99_seconds".to_owned(),
                status: gate_status(candidate_latency),
                detail: Some(format!(
                    "maximum={max_p99},observed={:?}",
                    reports
                        .iter()
                        .map(|report| report.commit_p99_seconds)
                        .collect::<Vec<_>>()
                )),
            },
            HardGateResult {
                id: "duplicate_identity_control_detected".to_owned(),
                status: gate_status(
                    mode != TransactionBatchMode::DuplicateIdentityControl || duplicate_detected,
                ),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "early_ack_poison_detected".to_owned(),
                status: gate_status(
                    mode != TransactionBatchMode::EarlyAckPoison || poison_detected,
                ),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "fresh_controller_semantic_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: first.map(|report| report.semantic_sha256.clone()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://transaction-batch-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "transaction_batch.throughput.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "transaction_batch.p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "transaction_batch.logical_transactions_per_append.minimum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.leader_logical_transactions_per_append)
                    .fold(f64::MAX, f64::min),
            ),
            ("transaction_batch.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn transaction_batch_single_entry_control_execution(
    workload: &WorkloadConfig,
    backend: &str,
    profile: &CommitGroupProfile,
    reports: &[CommitGroupReport],
    replay: &CommitGroupReport,
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| report.authority_processes == 3);
    let release_build = reports.iter().all(|report| report.release_build);
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let exact = reports.iter().all(|report| {
        report.committed_count == profile.transaction_count
            && report.commit_versions_unique_and_increasing
            && report.retained_stream_complete
            && report.exact_final_values
            && report.exact_retry
            && report.leader_failover_exact
            && report.restarted_voter_exact
            && report.correctness_anomalies == 0
    });
    let error = (!topology_exact || !release_build || !exact || !exact_replay)
        .then(|| "single-entry control violated its recovery contract".to_owned());
    let mut measurements = Vec::new();
    for report in reports {
        measurements.extend([
            Measurement {
                metric: "commit.throughput",
                value: report.transactions_per_second,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", "single_entry_control"),
                ]),
            },
            Measurement {
                metric: "commit.latency",
                value: report.commit_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum_sync_all"),
                    ("window.class", "single_entry_control"),
                    ("result", "pass"),
                ]),
            },
            Measurement {
                metric: "commit.logical_transactions_per_append",
                value: 1.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("node.class", "voter-201"),
                ]),
            },
        ]);
    }
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: resident_count_as_f64(
            reports
                .iter()
                .map(|report| report.correctness_anomalies)
                .sum(),
        ),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "transaction-batch-v1"),
            ("anomaly.class", "none"),
        ]),
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "single_entry_recovery_contract".to_owned(),
                status: gate_status(exact),
                detail: Some(format!("transactions={}", profile.transaction_count)),
            },
            HardGateResult {
                id: "fresh_controller_semantic_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: reports.first().map(|report| report.semantic_sha256.clone()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://transaction-batch-v1/single-entry-control/{}/{}",
                    report.seed, report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "transaction_batch.throughput.median".to_owned(),
                median(
                    &reports
                        .iter()
                        .map(|report| report.transactions_per_second)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "transaction_batch.p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| report.commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "transaction_batch.logical_transactions_per_append.minimum".to_owned(),
                1.0,
            ),
            ("transaction_batch.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

fn run_authenticated_object_frontier(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "object-store-local-fs+authority-openraft+data-openraft";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "authenticated object frontier requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "authenticated object frontier requires fixed seeds".to_owned()
        ));
    }
    let mode_name = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let mode = match parse_object_frontier_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_object_frontier_contract(*seed, mode, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_object_frontier_contract(seeds[0], mode, &executable) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    object_frontier_execution(
        workload,
        backend,
        mode,
        &reports,
        &replay,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_lines)]
fn run_object_fixture_anchor(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "data-openraft-local-process+fixture-anchor";
    const REQUIRED_FRESH_AUTHORITIES: usize = 20;
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "object fixture anchor requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err("object fixture anchor requires fixed seeds".to_owned()));
    }
    let fresh_authorities = match profile_config
        .parameters
        .get("fresh_authorities")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
    {
        Some(REQUIRED_FRESH_AUTHORITIES) => REQUIRED_FRESH_AUTHORITIES,
        Some(other) => {
            return execution_from_result(Err(format!(
                "object fixture anchor requires exactly {REQUIRED_FRESH_AUTHORITIES} fresh authorities, got {other}"
            )));
        }
        None => {
            return execution_from_result(Err(
                "object fixture anchor profile requires integer fresh_authorities".to_owned(),
            ));
        }
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let mode = match parse_fixture_anchor_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_fixture_anchor_contract(*seed, mode, fresh_authorities, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    fixture_anchor_execution(
        workload,
        backend,
        mode,
        fresh_authorities,
        &reports,
        started.elapsed().as_secs_f64(),
    )
}

fn fixture_anchor_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: FixtureAnchorMode,
    fresh_authorities: usize,
    reports: &[FixtureAnchorReport],
    wall_seconds: f64,
) -> WorkloadExecution {
    let fresh_authorities_u64 = u64::try_from(fresh_authorities).unwrap_or(u64::MAX);
    let release_build = reports.iter().all(|report| report.release_build);
    let candidate_observations_exact = mode != FixtureAnchorMode::Candidate
        || reports.iter().all(|report| {
            report.requested_fresh_authorities == fresh_authorities_u64
                && report.authority_processes_started == fresh_authorities_u64.saturating_mul(3)
                && report.observations.len() == fresh_authorities
                && report.anchor_version_stable
                && report.correctness_anomalies == 0
        });
    let one_empty_anchor_record = mode != FixtureAnchorMode::Candidate
        || reports.iter().all(|report| {
            report
                .observations
                .iter()
                .all(|observation| observation.anchor_records == 1)
        });
    let zero_anchor_mutations = mode != FixtureAnchorMode::Candidate
        || reports.iter().all(|report| {
            report
                .observations
                .iter()
                .all(|observation| observation.anchor_mutations == 0)
        });
    let zero_live_keys = reports.iter().all(|report| {
        report
            .observations
            .iter()
            .all(|observation| observation.live_keys == 0)
    });
    let lost_response_retry_exact = reports.iter().all(|report| {
        report.observations.iter().all(|observation| {
            observation.lost_response_observed && observation.exact_retry_returned_original
        })
    });
    let changed_identity_guard_rejected = reports.iter().all(|report| {
        report
            .observations
            .iter()
            .all(|observation| observation.second_identity_guard_rejected)
    });
    let poison_detected = mode != FixtureAnchorMode::SecondIdentityBypassPoison
        || reports.iter().all(|report| {
            report.second_identity_bypass_detected && report.correctness_anomalies == 0
        });
    let exact = candidate_observations_exact
        && one_empty_anchor_record
        && zero_anchor_mutations
        && zero_live_keys
        && lost_response_retry_exact
        && changed_identity_guard_rejected
        && poison_detected;
    let error = if !exact {
        Some("object fixture anchor correctness gate failed".to_owned())
    } else if !release_build {
        Some("object fixture anchor measured subject requires a release build".to_owned())
    } else {
        None
    };
    let anomaly_count = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();

    WorkloadExecution {
        error,
        measurements: vec![
            Measurement {
                metric: "operation.duration",
                value: wall_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "fixture-anchor-contract"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: resident_count_as_f64(anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "object-fixture-anchor-v1"),
                    (
                        "anomaly.class",
                        if anomaly_count == 0 {
                            "none"
                        } else {
                            "fixture-anchor"
                        },
                    ),
                ]),
            },
        ],
        hard_gates: vec![
            HardGateResult {
                id: "twenty_fresh_authorities_share_anchor".to_owned(),
                status: gate_status(candidate_observations_exact),
                detail: Some(format!(
                    "mode={},fresh_authorities={fresh_authorities}",
                    mode.id()
                )),
            },
            HardGateResult {
                id: "one_empty_anchor_record_per_authority".to_owned(),
                status: gate_status(one_empty_anchor_record),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "anchor_mutations_zero".to_owned(),
                status: gate_status(zero_anchor_mutations),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "anchor_live_keys_zero".to_owned(),
                status: gate_status(zero_live_keys),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "exact_retry_after_lost_response".to_owned(),
                status: gate_status(lost_response_retry_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "changed_identity_guard_rejected".to_owned(),
                status: gate_status(changed_identity_guard_rejected),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "second_identity_bypass_poison_detected".to_owned(),
                status: gate_status(poison_detected),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://object-fixture-anchor-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "fixture_anchor.fresh_authorities".to_owned(),
                resident_count_as_f64(fresh_authorities_u64),
            ),
            (
                "fixture_anchor.authority_processes_started".to_owned(),
                reports
                    .iter()
                    .map(|report| resident_count_as_f64(report.authority_processes_started))
                    .sum(),
            ),
            ("fixture_anchor.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_fixture_contract_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "object-store-local-fs+data-openraft-local-process+fixture-contract";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "object fixture contract requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "object fixture contract requires a fixed dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.logical_bytes == 0 || dataset.key_count == 0 {
        return execution_from_result(Err(
            "object fixture contract requires fixed seeds and a non-empty dataset".to_owned(),
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("object fixture profile requires integer {name}"))
    };
    let profile = match (|| {
        Ok::<_, String>(ObjectFixtureProfile {
            key_count: dataset.key_count,
            value_bytes: usize::try_from(integer("value_bytes")?)
                .map_err(|error| error.to_string())?,
            target_object_bytes: usize::try_from(integer("target_object_bytes")?)
                .map_err(|error| error.to_string())?,
            target_block_bytes: usize::try_from(integer("target_block_bytes")?)
                .map_err(|error| error.to_string())?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let generated_logical_bytes = profile
        .key_count
        .saturating_mul(u64::try_from(profile.value_bytes).unwrap_or(u64::MAX));
    if generated_logical_bytes != dataset.logical_bytes {
        return execution_from_result(Err(format!(
            "object fixture profile generates {generated_logical_bytes} bytes, dataset declares {}",
            dataset.logical_bytes
        )));
    }
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let mode = match parse_object_fixture_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_object_fixture_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    object_fixture_execution(
        workload,
        backend,
        mode,
        &profile,
        &reports,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_lines)]
fn object_fixture_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: ObjectFixtureMode,
    profile: &ObjectFixtureProfile,
    reports: &[ObjectFixtureReport],
    wall_seconds: f64,
) -> WorkloadExecution {
    let release_build = reports.iter().all(|report| report.release_build);
    let one_empty_anchor_record = reports.iter().all(|report| {
        report.base_anchor_version == 2
            && report.anchor_txlog_records == 1
            && report.anchor_txlog_mutations == 0
            && report.anchor_live_keys == 0
    });
    let zero_base_values_in_txlog = reports.iter().all(|report| {
        report.base_value_txlog_records == 0 && report.base_value_txlog_mutation_bytes == 0
    });
    let closure_exact_at_anchor = reports.iter().all(|report| {
        report.decoded_base_records == profile.key_count
            && report.all_base_records_at_anchor
            && report.all_segment_versions_at_anchor
            && report.object_count >= 3
            && report.object_bytes > 0
    });
    let descriptor_deterministic = reports.iter().all(|report| report.descriptor_deterministic);
    let immutable_put_reuse_verified = reports
        .iter()
        .all(|report| !report.fixture_reused && report.immutable_put_reuse_verified);
    let canonical_tail_exact = reports
        .iter()
        .all(|report| report.tail_records == 7 && report.tail_exact);
    let resident_images_distinct = reports.iter().all(|report| report.resident_images_distinct);
    let resident_logical_image_equal = reports
        .iter()
        .all(|report| report.resident_logical_images_equal);
    let fresh_subject_roots = reports.iter().all(|report| report.subject_roots_distinct);
    let corrupt_descriptor_poison_detected = mode != ObjectFixtureMode::CorruptDescriptorPoison
        || reports.iter().all(|report| report.poison_detected);
    let mutated_anchor_poison_detected = mode != ObjectFixtureMode::MutatedAnchorPoison
        || reports.iter().all(|report| report.poison_detected);
    let tail_mismatch_poison_detected = mode != ObjectFixtureMode::TailMismatchPoison
        || reports.iter().all(|report| report.poison_detected);
    let shared_mutable_image_poison_detected = mode != ObjectFixtureMode::SharedMutableImagePoison
        || reports.iter().all(|report| report.poison_detected);
    let anomaly_count = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();
    let exact = one_empty_anchor_record
        && zero_base_values_in_txlog
        && closure_exact_at_anchor
        && descriptor_deterministic
        && immutable_put_reuse_verified
        && canonical_tail_exact
        && resident_images_distinct
        && resident_logical_image_equal
        && fresh_subject_roots
        && corrupt_descriptor_poison_detected
        && mutated_anchor_poison_detected
        && tail_mismatch_poison_detected
        && shared_mutable_image_poison_detected
        && anomaly_count == 0;
    let error = if !exact {
        Some("object fixture content-addressing or resident identity gate failed".to_owned())
    } else if !release_build {
        Some("object fixture measured subject requires a release build".to_owned())
    } else {
        None
    };
    let gate = |id: &str, passed: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(passed),
        detail: Some(format!("mode={}", mode.id())),
    };
    WorkloadExecution {
        error,
        measurements: vec![
            Measurement {
                metric: "operation.duration",
                value: wall_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-fixture-contract"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: resident_count_as_f64(anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "object-fixture-contract-v1"),
                    (
                        "anomaly.class",
                        if anomaly_count == 0 {
                            "none"
                        } else {
                            "object-fixture"
                        },
                    ),
                ]),
            },
        ],
        hard_gates: vec![
            gate("one_empty_anchor_record", one_empty_anchor_record),
            gate("zero_base_values_in_txlog", zero_base_values_in_txlog),
            gate("closure_exact_at_anchor", closure_exact_at_anchor),
            gate("descriptor_deterministic", descriptor_deterministic),
            gate("immutable_put_reuse_verified", immutable_put_reuse_verified),
            gate("canonical_tail_exact", canonical_tail_exact),
            gate("resident_images_distinct", resident_images_distinct),
            gate("resident_logical_image_equal", resident_logical_image_equal),
            gate("fresh_subject_roots", fresh_subject_roots),
            gate(
                "corrupt_descriptor_poison_detected",
                corrupt_descriptor_poison_detected,
            ),
            gate(
                "mutated_anchor_poison_detected",
                mutated_anchor_poison_detected,
            ),
            gate(
                "tail_mismatch_poison_detected",
                tail_mismatch_poison_detected,
            ),
            gate(
                "shared_mutable_image_poison_detected",
                shared_mutable_image_poison_detected,
            ),
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://object-fixture-contract-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "object_fixture.logical_bytes".to_owned(),
                resident_count_as_f64(
                    profile
                        .key_count
                        .saturating_mul(u64::try_from(profile.value_bytes).unwrap_or(u64::MAX)),
                ),
            ),
            (
                "object_fixture.object_bytes".to_owned(),
                reports
                    .iter()
                    .map(|report| resident_count_as_f64(report.object_bytes))
                    .sum(),
            ),
            (
                "object_fixture.verification_seconds".to_owned(),
                reports
                    .iter()
                    .map(|report| report.fixture_verification_seconds)
                    .sum(),
            ),
            ("object_fixture.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_fixture_resident_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "object-store-local-fs+data-openraft-local-process+rocksdb-resident";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "object fixture resident process requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "object fixture resident process requires a fixed dataset".to_owned(),
        ));
    };
    if seeds.is_empty() || dataset.logical_bytes == 0 || dataset.key_count == 0 {
        return execution_from_result(Err(
            "object fixture resident process requires fixed seeds and data".to_owned(),
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("object fixture resident profile requires integer {name}"))
    };
    let value_bytes = match integer("value_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let target_object_bytes = match integer("target_object_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let target_block_bytes = match integer("target_block_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    if dataset
        .key_count
        .saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX))
        != dataset.logical_bytes
    {
        return execution_from_result(Err(
            "object fixture resident profile disagrees with dataset bytes".to_owned(),
        ));
    }
    let recovery_profile = ServingRecoveryProfile {
        key_count: dataset.key_count,
        value_bytes,
        target_object_bytes,
        target_block_bytes,
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let poison = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        == Some("regenerated_control_poison");
    let hot_profile = |seed: u64, subject| OpenRaftHotReadProfile {
        subject,
        seed,
        key_count: dataset.key_count,
        value_bytes,
        warmup_operations: 256,
        measured_operations: 1_024,
        concurrent_clients: 1,
        access_pattern: OpenRaftHotReadAccessPattern::Hotset80_20,
        max_local_bytes: 128 * 1_024 * 1_024,
        block_cache_bytes: 16 * 1_024 * 1_024,
        direct_reads: false,
        sample_count: 1,
        negative_control: None,
    };
    let started = Instant::now();
    let mut pairs = Vec::new();
    let mut poison_detected = true;
    for seed in seeds {
        if poison {
            let outcome = run_openraft_serving_recovery_contract_from_object_fixture(
                *seed,
                &recovery_profile,
                2,
                &executable,
                OpenRaftServingObjectBackend::LocalFilesystem,
                hot_profile(*seed, OpenRaftHotReadSubject::DirectOwnedRocksdb),
                true,
            );
            poison_detected &= outcome.is_err_and(|error| {
                error.contains("regenerated control diverges from verified object fixture")
            });
        } else {
            let native = match run_openraft_serving_recovery_contract_from_object_fixture(
                *seed,
                &recovery_profile,
                2,
                &executable,
                OpenRaftServingObjectBackend::LocalFilesystem,
                hot_profile(*seed, OpenRaftHotReadSubject::NativeSnapshot),
                false,
            ) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
            let control = match run_openraft_serving_recovery_contract_from_object_fixture(
                *seed,
                &recovery_profile,
                2,
                &executable,
                OpenRaftServingObjectBackend::LocalFilesystem,
                hot_profile(*seed, OpenRaftHotReadSubject::DirectOwnedRocksdb),
                false,
            ) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
            pairs.push((native, control));
        }
    }
    let candidate_checks = pairs.iter().map(|(native, control)| {
        let native_image = native.process.object_fixture_image.as_ref();
        let control_image = control.process.object_fixture_image.as_ref();
        let native_hot = native.process.hot_read.as_ref();
        let control_hot = control.process.hot_read.as_ref();
        (
            native.exact_replay && control.exact_replay,
            native_image.is_some_and(|image| image.scratch_was_empty)
                && control_image.is_some_and(|image| image.scratch_was_empty),
            native_image
                .zip(control_image)
                .is_some_and(|(native, control)| native.fixture_id == control.fixture_id),
            native_image
                .zip(control_image)
                .is_some_and(|(native, control)| native.tail_sha256 == control.tail_sha256),
            native_image
                .zip(control_image)
                .is_some_and(|(native, control)| {
                    native.resident_image_id != control.resident_image_id
                }),
            native_image
                .zip(control_image)
                .is_some_and(|(native, control)| {
                    native.resident_logical_sha256 == control.resident_logical_sha256
                }),
            native_image.is_some_and(|image| image.local_bytes > 0)
                && control_image.is_some_and(|image| image.local_bytes > 0),
            native_hot.is_some_and(|hot| hot.object_requests == 0)
                && control_hot.is_some_and(|hot| hot.object_requests == 0),
        )
    });
    let mut recovery_exact = true;
    let mut fresh_processes = true;
    let mut same_fixture = true;
    let mut same_tail = true;
    let mut images_distinct = true;
    let mut logical_equal = true;
    let mut images_nonzero = true;
    let mut zero_object_reads = true;
    for checks in candidate_checks {
        recovery_exact &= checks.0;
        fresh_processes &= checks.1;
        same_fixture &= checks.2;
        same_tail &= checks.3;
        images_distinct &= checks.4;
        logical_equal &= checks.5;
        images_nonzero &= checks.6;
        zero_object_reads &= checks.7;
    }
    let release_build = !cfg!(debug_assertions);
    let exact = poison_detected
        && (poison
            || (recovery_exact
                && fresh_processes
                && same_fixture
                && same_tail
                && images_distinct
                && logical_equal
                && images_nonzero
                && zero_object_reads))
        && release_build;
    let gate = |id: &str, passed: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(passed),
        detail: Some(format!("poison={poison}")),
    };
    WorkloadExecution {
        error: (!exact).then(|| "object fixture resident process gate failed".to_owned()),
        measurements: vec![
            Measurement {
                metric: "operation.duration",
                value: started.elapsed().as_secs_f64(),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-fixture-resident-process-contract"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "correctness.anomalies",
                value: if exact { 0.0 } else { 1.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "object-fixture-resident-process-v1"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "object-fixture-resident-process"
                        },
                    ),
                ]),
            },
        ],
        hard_gates: vec![
            gate(
                "object_fixture_worker_recovery_exact",
                poison || recovery_exact,
            ),
            gate("fresh_subject_processes", poison || fresh_processes),
            gate("same_fixture_identity", poison || same_fixture),
            gate("same_tail_identity", poison || same_tail),
            gate("resident_images_distinct", poison || images_distinct),
            gate("resident_logical_image_equal", poison || logical_equal),
            gate("resident_images_nonzero", poison || images_nonzero),
            gate(
                "post_activation_zero_object_requests",
                poison || zero_object_reads,
            ),
            gate("regenerated_control_poison_detected", poison_detected),
            gate("release_build", release_build),
        ],
        budget_units: started.elapsed().as_secs_f64(),
        artifact_refs: pairs
            .iter()
            .flat_map(|(native, control)| {
                [
                    format!(
                        "okv-eval://object-fixture-resident-process-v1/native/{}",
                        native.semantic_sha256
                    ),
                    format!(
                        "okv-eval://object-fixture-resident-process-v1/control/{}",
                        control.semantic_sha256
                    ),
                ]
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "object_fixture_resident.native_local_bytes".to_owned(),
                pairs
                    .iter()
                    .filter_map(|(native, _)| native.process.object_fixture_image.as_ref())
                    .map(|image| resident_count_as_f64(image.local_bytes))
                    .sum(),
            ),
            (
                "object_fixture_resident.control_local_bytes".to_owned(),
                pairs
                    .iter()
                    .filter_map(|(_, control)| control.process.object_fixture_image.as_ref())
                    .map(|image| resident_count_as_f64(image.local_bytes))
                    .sum(),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_fixture_gcs_preflight(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "object-store-gcs+data-openraft-local-process+rocksdb-resident+persisted-fixture";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "object fixture GCS preflight requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "object fixture GCS preflight requires a fixed dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.logical_bytes == 0 || dataset.key_count == 0 {
        return execution_from_result(Err(
            "object fixture GCS preflight requires fixed seeds and data".to_owned(),
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("object fixture GCS profile requires integer {name}"))
    };
    let value_bytes = match integer("value_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let target_object_bytes = match integer("target_object_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let target_block_bytes = match integer("target_block_bytes")
        .and_then(|value| usize::try_from(value).map_err(|error| error.to_string()))
    {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let max_local_bytes = match integer("max_local_bytes") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let block_cache_bytes = match integer("block_cache_bytes") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let minimum_reopens = match integer("minimum_exact_descriptor_reopens") {
        Ok(value) => usize::try_from(value).unwrap_or(usize::MAX),
        Err(error) => return execution_from_result(Err(error)),
    };
    let max_scratch_ratio = match profile_config
        .parameters
        .get("max_transaction_authority_scratch_ratio")
        .and_then(toml::Value::as_float)
    {
        Some(value) if value.is_finite() && value > 0.0 => value,
        _ => {
            return execution_from_result(Err(
                "object fixture GCS profile requires a positive scratch ratio".to_owned(),
            ));
        }
    };
    let prefix_env = match profile_config
        .parameters
        .get("gcs_prefix_env")
        .and_then(toml::Value::as_str)
    {
        Some(value) if !value.is_empty() => value,
        _ => {
            return execution_from_result(Err(
                "object fixture GCS profile requires gcs_prefix_env".to_owned(),
            ));
        }
    };
    let prefix = match std::env::var(prefix_env) {
        Ok(value)
            if !value.is_empty()
                && !value.starts_with('/')
                && !value.contains("..")
                && !value.contains("://") =>
        {
            value
        }
        _ => {
            return execution_from_result(Err(format!(
                "{prefix_env} must contain a non-empty provider-relative GCS prefix"
            )));
        }
    };
    if dataset
        .key_count
        .saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX))
        != dataset.logical_bytes
    {
        return execution_from_result(Err(
            "object fixture GCS profile disagrees with dataset bytes".to_owned(),
        ));
    }
    let poison = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        == Some("reuse_bypass_poison");
    let recovery_profile = ServingRecoveryProfile {
        key_count: dataset.key_count,
        value_bytes,
        target_object_bytes,
        target_block_bytes,
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let order = [
        OpenRaftHotReadSubject::NativeSnapshot,
        OpenRaftHotReadSubject::DirectOwnedRocksdb,
        OpenRaftHotReadSubject::DirectOwnedRocksdb,
        OpenRaftHotReadSubject::NativeSnapshot,
    ];
    let mut reports = Vec::with_capacity(seeds.len().saturating_mul(order.len()));
    for seed in seeds {
        let mut locator = None;
        for (ordinal, subject) in order.iter().copied().enumerate() {
            let existing = if ordinal == 0 || (poison && ordinal == 1) {
                None
            } else {
                locator.clone()
            };
            let report = match run_openraft_serving_recovery_contract_from_object_fixture_locator(
                *seed,
                &recovery_profile,
                2,
                &executable,
                OpenRaftServingObjectBackend::Gcs {
                    prefix: prefix.clone(),
                },
                OpenRaftHotReadProfile {
                    subject,
                    seed: *seed,
                    key_count: dataset.key_count,
                    value_bytes,
                    warmup_operations: 256,
                    measured_operations: 1_024,
                    concurrent_clients: 1,
                    access_pattern: OpenRaftHotReadAccessPattern::Hotset80_20,
                    max_local_bytes,
                    block_cache_bytes,
                    direct_reads: false,
                    sample_count: 1,
                    negative_control: None,
                },
                false,
                existing,
            ) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
            let Some(setup) = report.object_fixture_setup.as_ref() else {
                return execution_from_result(Err(
                    "object fixture GCS run omitted setup evidence".to_owned()
                ));
            };
            if locator.is_none() {
                locator = Some(setup.locator.clone());
            }
            reports.push(report);
        }
    }

    let setups = reports
        .iter()
        .filter_map(|report| report.object_fixture_setup.as_ref())
        .collect::<Vec<_>>();
    let images = reports
        .iter()
        .filter_map(|report| report.process.object_fixture_image.as_ref())
        .collect::<Vec<_>>();
    let expected_reports = seeds.len().saturating_mul(order.len());
    let one_fixture_identity = setups.len() == expected_reports
        && images.len() == expected_reports
        && setups
            .first()
            .is_some_and(|first| setups.iter().all(|setup| setup.locator == first.locator))
        && setups.first().is_some_and(|first| {
            images
                .iter()
                .all(|image| image.fixture_id == first.locator.fixture_id)
        });
    let one_tail_identity = images.first().is_some_and(|first| {
        images
            .iter()
            .all(|image| image.tail_sha256 == first.tail_sha256)
    });
    let abba_subject_order = reports.len() == expected_reports
        && reports.chunks_exact(order.len()).all(|chunk| {
            chunk.iter().zip(order).all(|(report, expected)| {
                report
                    .process
                    .object_fixture_image
                    .as_ref()
                    .is_some_and(|image| image.subject == expected)
            })
        });
    let fresh_subject_processes = reports.iter().all(|report| {
        report.worker_process_starts == 2
            && report.worker_process_kills == 1
            && report.empty_scratch_restarts == 1
            && report
                .process
                .object_fixture_image
                .as_ref()
                .is_some_and(|image| image.scratch_was_empty)
    });
    let exact_reopens = setups
        .iter()
        .filter(|setup| setup.fixture_opened_existing)
        .count();
    let minimum_three_exact_descriptor_reopens = exact_reopens >= minimum_reopens
        && setups
            .chunks_exact(order.len())
            .all(|chunk| chunk.iter().skip(1).all(|setup| setup.fixture_reused));
    let one_empty_anchor_per_authority = setups.iter().all(|setup| {
        setup.base_anchor_version == 2
            && setup.anchor_txlog_records == 1
            && setup.anchor_txlog_mutations == 0
    });
    let zero_base_values_in_txlog = setups.iter().all(|setup| {
        setup.base_value_txlog_records == 0 && setup.base_value_txlog_mutation_bytes == 0
    });
    let complete_closure_at_anchor = setups.iter().all(|setup| {
        setup.decoded_base_records == dataset.key_count
            && setup.all_base_records_at_anchor
            && setup.fixture_object_count >= 3
            && setup.fixture_object_bytes > 0
    });
    let max_setup_seconds = setups
        .iter()
        .map(|setup| setup.fixture_setup_seconds)
        .fold(0.0_f64, f64::max);
    let setup_within_300_seconds = max_setup_seconds <= 300.0;
    let max_scratch_bytes = setups
        .iter()
        .map(|setup| setup.transaction_authority_physical_bytes)
        .max()
        .unwrap_or(u64::MAX);
    let observed_scratch_ratio =
        resident_count_as_f64(max_scratch_bytes) / resident_count_as_f64(dataset.logical_bytes);
    let transaction_authority_scratch_at_most_0_25x = observed_scratch_ratio <= max_scratch_ratio;
    let resident_logical_images_equal = images.first().is_some_and(|first| {
        images
            .iter()
            .all(|image| image.resident_logical_sha256 == first.resident_logical_sha256)
    });
    let post_activation_zero_object_requests = reports.iter().all(|report| {
        report
            .process
            .hot_read
            .as_ref()
            .is_some_and(|hot| hot.object_requests == 0 && hot.correctness_failures == 0)
    });
    let recovery_exact = reports.iter().all(|report| report.exact_replay);
    let reuse_bypass_poison_detected = !poison || !minimum_three_exact_descriptor_reopens;
    let release_build = !cfg!(debug_assertions);
    let candidate_exact = recovery_exact
        && one_fixture_identity
        && one_tail_identity
        && abba_subject_order
        && fresh_subject_processes
        && minimum_three_exact_descriptor_reopens
        && one_empty_anchor_per_authority
        && zero_base_values_in_txlog
        && complete_closure_at_anchor
        && setup_within_300_seconds
        && transaction_authority_scratch_at_most_0_25x
        && resident_logical_images_equal
        && post_activation_zero_object_requests;
    let exact = release_build
        && if poison {
            reuse_bypass_poison_detected
        } else {
            candidate_exact
        };
    let gate = |id: &str, passed: bool| {
        HardGateResult {
        id: id.to_owned(),
        status: gate_status(passed),
        detail: Some(format!(
            "poison={poison},prefix={prefix},reopens={exact_reopens},max_setup_seconds={max_setup_seconds:.6},scratch_ratio={observed_scratch_ratio:.6}"
        )),
    }
    };
    let mut measurements = reports
        .iter()
        .enumerate()
        .filter_map(|(ordinal, report)| {
            let setup = report.object_fixture_setup.as_ref()?;
            let subject = report
                .process
                .object_fixture_image
                .as_ref()
                .map_or("absent", |image| match image.subject {
                    OpenRaftHotReadSubject::NativeSnapshot => "native_snapshot",
                    OpenRaftHotReadSubject::DirectOwnedRocksdb => "direct_owned_rocksdb",
                });
            let ordinal = ordinal.to_string();
            Some(Measurement {
                metric: "object_fixture.setup_seconds",
                value: setup.fixture_setup_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-fixture-gcs-preflight"),
                    ("backend", backend),
                    ("subject", subject),
                    ("ordinal", &ordinal),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            })
        })
        .collect::<Vec<_>>();
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: if exact { 0.0 } else { 1.0 },
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "object-fixture-gcs-preflight-v1"),
            (
                "anomaly.class",
                if exact {
                    "none"
                } else {
                    "object-fixture-gcs-preflight"
                },
            ),
        ]),
    });
    WorkloadExecution {
        error: (!exact).then(|| "object fixture GCS preflight gate failed".to_owned()),
        measurements,
        hard_gates: vec![
            gate("gcs_backend_exact", true),
            gate("one_fixture_identity", poison || one_fixture_identity),
            gate("one_tail_identity", poison || one_tail_identity),
            gate("abba_subject_order", poison || abba_subject_order),
            gate("fresh_subject_processes", poison || fresh_subject_processes),
            gate(
                "minimum_three_exact_descriptor_reopens",
                poison || minimum_three_exact_descriptor_reopens,
            ),
            gate(
                "one_empty_anchor_per_authority",
                poison || one_empty_anchor_per_authority,
            ),
            gate(
                "zero_base_values_in_txlog",
                poison || zero_base_values_in_txlog,
            ),
            gate(
                "complete_closure_at_anchor",
                poison || complete_closure_at_anchor,
            ),
            gate(
                "setup_within_300_seconds",
                poison || setup_within_300_seconds,
            ),
            gate(
                "transaction_authority_scratch_at_most_0_25x",
                poison || transaction_authority_scratch_at_most_0_25x,
            ),
            gate(
                "resident_logical_images_equal",
                poison || resident_logical_images_equal,
            ),
            gate(
                "post_activation_zero_object_requests",
                poison || post_activation_zero_object_requests,
            ),
            gate("reuse_bypass_poison_detected", reuse_bypass_poison_detected),
            gate("release_build", release_build),
        ],
        budget_units: max_setup_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://object-fixture-gcs-preflight-v1/{}",
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "object_fixture.gcs_exact_descriptor_reopens".to_owned(),
                resident_count_as_f64(u64::try_from(exact_reopens).unwrap_or(u64::MAX)),
            ),
            (
                "object_fixture.transaction_authority_scratch_ratio_max".to_owned(),
                observed_scratch_ratio,
            ),
            (
                "object_fixture.transaction_authority_physical_bytes_max".to_owned(),
                resident_count_as_f64(max_scratch_bytes),
            ),
            (
                "object_fixture.fixture_request_bytes_total".to_owned(),
                setups.iter().fold(0.0, |total, setup| {
                    total + resident_count_as_f64(setup.fixture_object_request_bytes)
                }),
            ),
            (
                "object_fixture.fixture_response_bytes_total".to_owned(),
                setups.iter().fold(0.0, |total, setup| {
                    total + resident_count_as_f64(setup.fixture_object_response_bytes)
                }),
            ),
        ]),
    }
}

fn run_process_snapshot_compaction(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "data-openraft-local-process+durable-state-snapshot+canonical-journal";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "process snapshot compaction requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "process snapshot compaction requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.logical_bytes == 0 {
        return execution_from_result(Err(
            "process snapshot compaction requires fixed seeds and logical bytes".to_owned(),
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("process snapshot-compaction profile requires integer {name}"))
    };
    let profile = match (|| {
        Ok::<_, String>(ProcessSnapshotCompactionProfile {
            transaction_count: integer("transaction_count")?,
            transactions_per_batch: usize::try_from(integer("transactions_per_batch")?)
                .map_err(|error| error.to_string())?,
            live_keys: integer("live_keys")?,
            value_bytes: usize::try_from(integer("value_bytes")?)
                .map_err(|error| error.to_string())?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("candidate");
    let mode = match mode_name {
        "candidate" => ProcessSnapshotCompactionMode::Candidate,
        "purge_before_snapshot_poison" => ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison,
        other => {
            return execution_from_result(Err(format!(
                "unknown process snapshot-compaction subject {other}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_process_snapshot_compaction_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let topology_exact = reports.iter().all(|report| report.authority_processes == 3);
    let release_build = reports.iter().all(|report| report.release_build);
    let candidate_exact = mode != ProcessSnapshotCompactionMode::Candidate
        || reports.iter().all(|report| {
            report.committed_count == profile.transaction_count
                && report.all_snapshots_cover_purge
                && report.all_journals_reclaimed_bytes
                && report.full_quorum_restart_exact
                && report.retained_stream_restart_exact
                && report.exact_retry_after_restart
                && report.suffix_commit_after_restart
                && report.correctness_anomalies == 0
        });
    let poison_exact = mode != ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison
        || reports.iter().all(|report| {
            report.purge_before_snapshot_rejected
                && report.poison_journal_unchanged
                && report.poison_restart_exact
                && report.correctness_anomalies == 0
        });
    let exact = topology_exact && candidate_exact && poison_exact;
    let error = if !exact {
        Some("process snapshot-compaction correctness gate failed".to_owned())
    } else if !release_build {
        Some("process snapshot-compaction measured subject requires a release build".to_owned())
    } else {
        None
    };
    let logical_bytes = resident_count_as_f64(dataset.logical_bytes);
    let mut measurements = Vec::new();
    for report in &reports {
        measurements.extend([
            Measurement {
                metric: "storage.amplification",
                value: resident_count_as_f64(report.durable_bytes_after) / logical_bytes,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("format", "okvs1+okvr1"),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: resident_count_as_f64(report.journal_bytes_after),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "three-process-local"),
                    (
                        "fault",
                        if mode == ProcessSnapshotCompactionMode::Candidate {
                            "none"
                        } else {
                            "purge-before-snapshot"
                        },
                    ),
                ]),
            },
        ]);
    }
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: resident_count_as_f64(
            reports
                .iter()
                .map(|report| report.correctness_anomalies)
                .sum(),
        ),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "process-snapshot-compaction-v1"),
            (
                "anomaly.class",
                if mode == ProcessSnapshotCompactionMode::Candidate {
                    "none"
                } else {
                    "purge-before-snapshot"
                },
            ),
        ]),
    });
    let wall_seconds = started.elapsed().as_secs_f64();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("authority_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "durable_snapshot_and_compaction_exact".to_owned(),
                status: gate_status(candidate_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "purge_before_snapshot_poison_detected".to_owned(),
                status: gate_status(poison_exact),
                detail: Some(format!("mode={}", mode.id())),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://process-snapshot-compaction-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "process_snapshot.journal_bytes_before.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| resident_count_as_f64(report.journal_bytes_before))
                    .fold(0.0_f64, f64::max),
            ),
            (
                "process_snapshot.journal_bytes_after.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| resident_count_as_f64(report.journal_bytes_after))
                    .fold(0.0_f64, f64::max),
            ),
            (
                "process_snapshot.snapshot_bytes.maximum".to_owned(),
                reports
                    .iter()
                    .map(|report| resident_count_as_f64(report.snapshot_bytes))
                    .fold(0.0_f64, f64::max),
            ),
            ("process_snapshot.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_frontiered_process_snapshot(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "local-fs+publication-openraft+data-openraft+frontiered-durable-snapshot";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "frontiered process snapshot requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "frontiered process snapshot requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.logical_bytes == 0 {
        return execution_from_result(Err(
            "frontiered process snapshot requires fixed seeds and logical bytes".to_owned(),
        ));
    }
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("frontiered process-snapshot profile requires integer {name}"))
    };
    let float = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("frontiered process-snapshot profile requires float {name}"))
    };
    let profile = match (|| {
        Ok::<_, String>(FrontieredProcessSnapshotProfile {
            frontier_cycles: integer("frontier_cycles")?,
            transactions_per_cycle: integer("transactions_per_cycle")?,
            transactions_per_batch: usize::try_from(integer("transactions_per_batch")?)
                .map_err(|error| error.to_string())?,
            live_keys: integer("live_keys")?,
            value_bytes: usize::try_from(integer("value_bytes")?)
                .map_err(|error| error.to_string())?,
            retry_window: integer("retry_window")?,
            max_physical_amplification: float("max_physical_amplification")?,
            max_snapshot_growth_ratio: float("max_snapshot_growth_ratio")?,
        })
    })() {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile_logical_bytes = profile
        .live_keys
        .saturating_mul(u64::try_from(profile.value_bytes).unwrap_or(u64::MAX));
    if profile_logical_bytes != dataset.logical_bytes {
        return execution_from_result(Err(format!(
            "frontiered process snapshot dataset declares {} logical bytes, profile owns {profile_logical_bytes}",
            dataset.logical_bytes
        )));
    }
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("aligned_r_q_o_candidate");
    let mode = match parse_frontiered_process_snapshot_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let started = Instant::now();
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_frontiered_process_snapshot_contract(*seed, mode, &profile, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    frontiered_process_snapshot_execution(
        workload,
        backend,
        mode,
        &profile,
        &reports,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::too_many_lines)]
fn frontiered_process_snapshot_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: FrontieredProcessSnapshotMode,
    profile: &FrontieredProcessSnapshotProfile,
    reports: &[FrontieredProcessSnapshotReport],
    wall_seconds: f64,
) -> WorkloadExecution {
    let topology_exact = reports.iter().all(|report| {
        report.data_authority_processes == 3 && report.publication_authority_processes == 3
    });
    let release_build = reports.iter().all(|report| report.release_build);
    let four_cycles = reports
        .iter()
        .all(|report| report.frontier_cycles == 4 && report.complete_frontier_cycles == 4);
    let semantic_exact = reports
        .iter()
        .all(|report| report.correctness_anomalies == 0);
    let authenticated_frontiers = reports.iter().all(|report| {
        report.cycles.iter().all(|cycle| {
            cycle.frontier_attestation_after_restart_exact
                && cycle.publication_retry_after_restart_exact
        })
    });
    let resolver_exact = reports.iter().all(|report| {
        report
            .cycles
            .iter()
            .all(|cycle| cycle.resolver_floor == cycle.object_version)
    });
    let retry_window_exact = reports.iter().all(|report| {
        if mode == FrontieredProcessSnapshotMode::NoRetryFrontierControl {
            report.no_retry_frontier_control_detected
        } else {
            report.cycles.iter().all(|cycle| {
                cycle.retained_retry_outcomes == profile.retry_window
                    && cycle.retained_retry_fingerprints == profile.retry_window
            })
        }
    });
    let expired_retry_exact = reports.iter().all(|report| {
        report
            .cycles
            .iter()
            .all(|cycle| cycle.expired_retry_rejected_without_mutation)
    });
    let retained_retry_exact = reports
        .iter()
        .all(|report| report.cycles.iter().all(|cycle| cycle.retained_retry_exact));
    let snapshots_cover_purge = reports.iter().all(|report| {
        report
            .cycles
            .iter()
            .all(|cycle| cycle.snapshot_covers_purge)
    });
    let journals_reclaim = reports
        .iter()
        .all(|report| report.cycles.iter().all(|cycle| cycle.journals_reclaimed));
    let restart_exact = reports.iter().all(|report| {
        report
            .cycles
            .iter()
            .all(|cycle| cycle.full_quorum_restart_exact)
    });
    let object_exact = reports.iter().all(|report| {
        report.final_object_plus_suffix_exact
            && report.cycles.iter().all(|cycle| {
                cycle.object_state_after_restart_exact
                    && cycle.object_plus_suffix_after_restart_exact
            })
    });
    let suffix_exact = reports
        .iter()
        .all(|report| report.suffix_commit_after_final_restart);
    let candidate_amplification_bounded = mode
        != FrontieredProcessSnapshotMode::AlignedRqoCandidate
        || reports.iter().all(|report| {
            report.maximum_actual_physical_amplification <= profile.max_physical_amplification
        });
    let candidate_growth_bounded = mode != FrontieredProcessSnapshotMode::AlignedRqoCandidate
        || reports
            .iter()
            .all(|report| report.snapshot_growth_ratio <= profile.max_snapshot_growth_ratio);
    let candidate_bounded = candidate_amplification_bounded && candidate_growth_bounded;
    let no_retry_control = mode != FrontieredProcessSnapshotMode::NoRetryFrontierControl
        || reports
            .iter()
            .all(|report| report.no_retry_frontier_control_detected);
    let accounting_poison = mode != FrontieredProcessSnapshotMode::AccountingPoison
        || reports
            .iter()
            .all(|report| report.accounting_poison_detected);
    let exact = topology_exact
        && four_cycles
        && semantic_exact
        && authenticated_frontiers
        && resolver_exact
        && retry_window_exact
        && expired_retry_exact
        && retained_retry_exact
        && snapshots_cover_purge
        && journals_reclaim
        && restart_exact
        && object_exact
        && suffix_exact
        && no_retry_control
        && accounting_poison;
    let error = if !exact {
        Some("frontiered process-snapshot correctness or control gate failed".to_owned())
    } else if !release_build {
        Some("frontiered process-snapshot measured subject requires a release build".to_owned())
    } else if !candidate_bounded {
        Some(format!(
            "frontiered process-snapshot candidate exceeded the frozen {:.3}x media or {:.3}x growth gate",
            profile.max_physical_amplification, profile.max_snapshot_growth_ratio
        ))
    } else {
        None
    };
    let measurements = reports
        .iter()
        .flat_map(|report| {
            [
                Measurement {
                    metric: "storage.amplification",
                    value: report.maximum_physical_amplification,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("format", "okvs1+okvr1+object-frontier"),
                    ]),
                },
                Measurement {
                    metric: "authority.snapshot_growth_ratio",
                    value: report.snapshot_growth_ratio,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("projection", "six-process-cell"),
                    ]),
                },
                Measurement {
                    metric: "wal.retained_bytes",
                    value: report.cycles.last().map_or(0.0, |cycle| {
                        resident_count_as_f64(cycle.journal_bytes_after)
                    }),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("topology", "six-process-local"),
                        ("fault", mode.id()),
                    ]),
                },
            ]
        })
        .chain(std::iter::once(Measurement {
            metric: "correctness.anomalies",
            value: resident_count_as_f64(
                reports
                    .iter()
                    .map(|report| report.correctness_anomalies)
                    .sum(),
            ),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "frontiered-process-snapshot-v1"),
                ("anomaly.class", mode.id()),
            ]),
        }))
        .collect();
    let maximum_actual_amplification = reports
        .iter()
        .map(|report| report.maximum_actual_physical_amplification)
        .fold(0.0_f64, f64::max);
    let maximum_growth = reports
        .iter()
        .map(|report| report.snapshot_growth_ratio)
        .fold(0.0_f64, f64::max);
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_real_openraft_data_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("data_processes=3".to_owned()),
            },
            HardGateResult {
                id: "three_real_openraft_publication_processes".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("publication_processes=3".to_owned()),
            },
            HardGateResult {
                id: "release_build".to_owned(),
                status: gate_status(release_build),
                detail: Some("build_profile=release".to_owned()),
            },
            HardGateResult {
                id: "four_complete_frontier_cycles".to_owned(),
                status: gate_status(four_cycles),
                detail: Some("frontier_cycles=4".to_owned()),
            },
            HardGateResult {
                id: "frozen_object_frontiers_are_authenticated".to_owned(),
                status: gate_status(authenticated_frontiers),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "resolver_frontier_equals_cycle_commit_version".to_owned(),
                status: gate_status(resolver_exact),
                detail: None,
            },
            HardGateResult {
                id: "retry_window_is_exactly_64_requests".to_owned(),
                status: gate_status(retry_window_exact),
                detail: Some(format!("retry_window={}", profile.retry_window)),
            },
            HardGateResult {
                id: "expired_retry_is_rejected_without_mutation".to_owned(),
                status: gate_status(expired_retry_exact),
                detail: None,
            },
            HardGateResult {
                id: "retained_retry_replays_exactly".to_owned(),
                status: gate_status(retained_retry_exact),
                detail: None,
            },
            HardGateResult {
                id: "durable_snapshot_covers_purge".to_owned(),
                status: gate_status(snapshots_cover_purge),
                detail: None,
            },
            HardGateResult {
                id: "physical_journals_reclaim_bytes".to_owned(),
                status: gate_status(journals_reclaim),
                detail: None,
            },
            HardGateResult {
                id: "full_quorum_restart_is_exact_each_cycle".to_owned(),
                status: gate_status(restart_exact),
                detail: None,
            },
            HardGateResult {
                id: "object_plus_suffix_reconstruction_is_exact".to_owned(),
                status: gate_status(object_exact),
                detail: None,
            },
            HardGateResult {
                id: "suffix_commit_after_final_restart".to_owned(),
                status: gate_status(suffix_exact),
                detail: None,
            },
            HardGateResult {
                id: "max_physical_amplification".to_owned(),
                status: gate_status(candidate_amplification_bounded),
                detail: Some(format!(
                    "actual_max={maximum_actual_amplification:.6}, limit={:.6}",
                    profile.max_physical_amplification
                )),
            },
            HardGateResult {
                id: "max_snapshot_growth_ratio".to_owned(),
                status: gate_status(candidate_growth_bounded),
                detail: Some(format!(
                    "actual_max={maximum_growth:.6}, limit={:.6}",
                    profile.max_snapshot_growth_ratio
                )),
            },
            HardGateResult {
                id: "bounded_lifetime_media_curve".to_owned(),
                status: gate_status(candidate_bounded && no_retry_control),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "accounting_poison_detected".to_owned(),
                status: gate_status(accounting_poison),
                detail: Some(format!("mode={}", mode.id())),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://frontiered-process-snapshot-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "frontiered_snapshot.actual_physical_amplification.maximum".to_owned(),
                maximum_actual_amplification,
            ),
            (
                "frontiered_snapshot.snapshot_growth_ratio.maximum".to_owned(),
                maximum_growth,
            ),
            (
                "frontiered_snapshot.batch_commit_p99_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .flat_map(|report| &report.cycles)
                    .map(|cycle| cycle.batch_commit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "frontiered_snapshot.maintenance_seconds.maximum".to_owned(),
                reports
                    .iter()
                    .flat_map(|report| &report.cycles)
                    .map(|cycle| cycle.maintenance_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            ("frontiered_snapshot.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[derive(Clone, Copy, Debug)]
struct StorageLayoutAdmissionThresholds {
    point_request_ratio_max: f64,
    point_bytes_ratio_max: f64,
    point_p99_ratio_max: f64,
    scan_throughput_ratio_min: f64,
    storage_amplification_ratio_max: f64,
    compaction_write_amplification_ratio_max: f64,
    resident_index_ratio_max: f64,
}

#[derive(Clone, Copy, Debug)]
struct StorageLayoutComparison {
    same_histories: bool,
    point_request_ratio_max: f64,
    point_bytes_ratio_max: f64,
    point_p99_ratio: f64,
    scan_throughput_ratio: f64,
    storage_amplification_ratio_max: f64,
    compaction_write_amplification_ratio_max: f64,
    resident_index_ratio_max: f64,
}

impl StorageLayoutComparison {
    fn passes(self, thresholds: StorageLayoutAdmissionThresholds) -> bool {
        self.same_histories
            && self.point_request_ratio_max <= thresholds.point_request_ratio_max
            && self.point_bytes_ratio_max <= thresholds.point_bytes_ratio_max
            && self.point_p99_ratio <= thresholds.point_p99_ratio_max
            && self.scan_throughput_ratio >= thresholds.scan_throughput_ratio_min
            && self.storage_amplification_ratio_max <= thresholds.storage_amplification_ratio_max
            && self.compaction_write_amplification_ratio_max
                <= thresholds.compaction_write_amplification_ratio_max
            && self.resident_index_ratio_max <= thresholds.resident_index_ratio_max
    }
}

fn storage_layout_admission_thresholds(
    profile: &ProfileConfig,
) -> Result<StorageLayoutAdmissionThresholds, String> {
    let value = |name: &str| {
        profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| format!("storage-layout admission requires positive float {name}"))
    };
    Ok(StorageLayoutAdmissionThresholds {
        point_request_ratio_max: value("admission_point_request_ratio_max")?,
        point_bytes_ratio_max: value("admission_point_bytes_ratio_max")?,
        point_p99_ratio_max: value("admission_point_p99_ratio_max")?,
        scan_throughput_ratio_min: value("admission_scan_throughput_ratio_min")?,
        storage_amplification_ratio_max: value("admission_storage_amplification_ratio_max")?,
        compaction_write_amplification_ratio_max: value(
            "admission_compaction_write_amplification_ratio_max",
        )?,
        resident_index_ratio_max: value("admission_resident_index_ratio_max")?,
    })
}

#[allow(clippy::cast_precision_loss)]
fn storage_layout_comparison(
    candidate: &StorageLayoutReport,
    baseline: &StorageLayoutReport,
) -> Result<StorageLayoutComparison, String> {
    if candidate.samples.is_empty() || candidate.samples.len() != baseline.samples.len() {
        return Err("storage-layout candidate and baseline samples do not align".to_owned());
    }
    let pairs = candidate.samples.iter().zip(&baseline.samples);
    let same_histories = pairs.clone().all(|(candidate, baseline)| {
        candidate.seed == baseline.seed
            && candidate.repeat == baseline.repeat
            && candidate.canonical_history_sha256 == baseline.canonical_history_sha256
    });
    let max_ratio =
        |numerator: fn(&okv_eval::storage_layout::StorageLayoutSample) -> f64,
         denominator: fn(&okv_eval::storage_layout::StorageLayoutSample) -> f64| {
            candidate
                .samples
                .iter()
                .zip(&baseline.samples)
                .map(|(candidate, baseline)| {
                    storage_layout_ratio(numerator(candidate), denominator(baseline))
                })
                .fold(0.0_f64, f64::max)
        };
    let candidate_p99 = candidate
        .samples
        .iter()
        .map(|sample| sample.point_latency_ns_p99 as f64)
        .collect::<Vec<_>>();
    let baseline_p99 = baseline
        .samples
        .iter()
        .map(|sample| sample.point_latency_ns_p99 as f64)
        .collect::<Vec<_>>();
    let candidate_scan = candidate
        .samples
        .iter()
        .map(|sample| sample.scan_rows_per_second)
        .collect::<Vec<_>>();
    let baseline_scan = baseline
        .samples
        .iter()
        .map(|sample| sample.scan_rows_per_second)
        .collect::<Vec<_>>();
    Ok(StorageLayoutComparison {
        same_histories,
        point_request_ratio_max: max_ratio(
            |sample| sample.point_requests as f64 / sample.point_operations.max(1) as f64,
            |sample| sample.point_requests as f64 / sample.point_operations.max(1) as f64,
        ),
        point_bytes_ratio_max: max_ratio(
            |sample| sample.point_response_bytes as f64 / sample.point_operations.max(1) as f64,
            |sample| sample.point_response_bytes as f64 / sample.point_operations.max(1) as f64,
        ),
        point_p99_ratio: storage_layout_ratio(median(&candidate_p99), median(&baseline_p99)),
        scan_throughput_ratio: storage_layout_ratio(
            median(&candidate_scan),
            median(&baseline_scan),
        ),
        storage_amplification_ratio_max: max_ratio(
            |sample| sample.storage_amplification,
            |sample| sample.storage_amplification,
        ),
        compaction_write_amplification_ratio_max: max_ratio(
            |sample| sample.compaction_write_amplification,
            |sample| sample.compaction_write_amplification,
        ),
        resident_index_ratio_max: max_ratio(
            |sample| sample.resident_index_bytes as f64,
            |sample| sample.resident_index_bytes as f64,
        ),
    })
}

fn storage_layout_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        f64::INFINITY
    }
}

fn storage_layout_profile(
    dataset: &DatasetConfig,
    seeds: &[u64],
    profile_config: &ProfileConfig,
) -> Result<StorageLayoutProfile, String> {
    let integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| format!("storage-layout profile requires integer {name}"))
    };
    let float = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_float)
            .ok_or_else(|| format!("storage-layout profile requires float {name}"))
    };
    let optional_integer = |name: &str| {
        profile_config
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .and_then(|value| u64::try_from(value).ok())
    };
    let profile = StorageLayoutProfile {
        key_count: integer("key_count")?,
        canonical_live_row_bytes: usize::try_from(integer("canonical_live_row_bytes")?)
            .map_err(|error| error.to_string())?,
        opaque_payload_bytes: usize::try_from(integer("opaque_payload_bytes")?)
            .map_err(|error| error.to_string())?,
        base_version: integer("base_version")?,
        delta_cycles: integer("delta_cycles")?,
        update_fraction: float("update_fraction")?,
        delete_fraction: float("delete_fraction")?,
        point_operations: usize::try_from(integer("point_operations")?)
            .map_err(|error| error.to_string())?,
        target_run_object_bytes: usize::try_from(integer("target_run_object_bytes")?)
            .map_err(|error| error.to_string())?,
        row_block_bytes: usize::try_from(integer("row_block_bytes")?)
            .map_err(|error| error.to_string())?,
        columnar_block_rows: usize::try_from(integer("columnar_block_rows")?)
            .map_err(|error| error.to_string())?,
        overlay_cache_bytes: usize::try_from(
            optional_integer("overlay_cache_bytes")
                .unwrap_or(integer("target_run_object_bytes")?.saturating_mul(2)),
        )
        .map_err(|error| error.to_string())?,
        seeds: seeds.to_vec(),
        repeats: profile_config.repeats,
    };
    if profile.key_count != dataset.key_count
        || profile
            .key_count
            .saturating_mul(u64::try_from(profile.canonical_live_row_bytes).unwrap_or(u64::MAX))
            != dataset.logical_bytes
    {
        return Err("storage-layout dataset and profile identity differ".to_owned());
    }
    Ok(profile)
}

fn run_columnar_range_datafusion(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "datafusion+local-fs-range-stripes";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "columnar DataFusion source requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "columnar DataFusion source requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.key_count == 0 {
        return execution_from_result(Err(
            "columnar DataFusion source requires fixed seeds and keys".to_owned(),
        ));
    }
    let profile = match storage_layout_profile(dataset, seeds, profile_config) {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => ColumnarDataFusionMode::Correct,
        Some("payload_prefetch") => ColumnarDataFusionMode::PayloadPrefetchPoison,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown columnar DataFusion negative control {other}"
            )));
        }
    };
    let scan_fetch_target_bytes = workload
        .parameters
        .get("scan_fetch_target_bytes")
        .or_else(|| profile_config.parameters.get("scan_fetch_target_bytes"))
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    let started = Instant::now();
    let report = match run_columnar_datafusion_contract_with_scan_fetch(
        mode,
        &profile,
        scan_fetch_target_bytes,
    ) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    columnar_datafusion_execution(
        workload,
        backend,
        mode,
        &profile,
        &report,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn columnar_datafusion_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: ColumnarDataFusionMode,
    profile: &StorageLayoutProfile,
    report: &ColumnarDataFusionReport,
    wall_seconds: f64,
) -> WorkloadExecution {
    let query_exact = report.samples.iter().all(|sample| {
        sample.query_anomalies == 0
            && sample.expected_groups > 0
            && sample.expected_groups == sample.result_groups
            && sample.source_rows > 0
    });
    let projection_pushdown = report.samples.iter().all(|sample| {
        sample.scan_plans > 0
            && sample.scan_plans == sample.projection_pushdown_plans
            && sample.source_stripes > 0
    });
    let incremental_stripes = report.samples.iter().all(|sample| {
        sample.source_stripes > 1
            && sample.source_batches == sample.source_stripes
            && sample.object_requests
                == sample
                    .projection_fetch_requests
                    .saturating_add(sample.opaque_payload_requests)
    });
    let range_io_exact = report.samples.iter().all(|sample| {
        sample.full_object_requests == 0
            && sample.list_requests == 0
            && sample.checksum_covered_ranges
    });
    let source_memory_bounded = report.samples.iter().all(|sample| {
        let fetch_limit = sample
            .scan_fetch_target_bytes
            .max(sample.maximum_projection_stripe_bytes);
        sample.peak_batch_rows > 0
            && sample.peak_batch_rows
                <= u64::try_from(profile.columnar_block_rows).unwrap_or(u64::MAX)
            && sample.peak_batch_bytes > 0
            && sample.peak_fetch_bytes > 0
            && sample.peak_fetch_bytes <= fetch_limit
    });
    let payload_gate = match mode {
        ColumnarDataFusionMode::Correct => report.samples.iter().all(|sample| {
            sample.opaque_payload_requests == 0
                && sample.opaque_payload_response_bytes == 0
                && sample.object_response_bytes == sample.projection_bytes
        }),
        ColumnarDataFusionMode::PayloadPrefetchPoison => report.samples.iter().all(|sample| {
            sample.poison_detected
                && sample.opaque_payload_requests > 0
                && sample.opaque_payload_response_bytes > 0
                && sample.object_response_bytes > sample.projection_bytes
        }),
    };
    let throughput_observed = report
        .samples
        .iter()
        .all(|sample| sample.query_seconds > 0.0 && sample.source_rows_per_second.is_finite());
    let passed = query_exact
        && projection_pushdown
        && incremental_stripes
        && range_io_exact
        && source_memory_bounded
        && payload_gate
        && throughput_observed;
    let measurements = report
        .samples
        .iter()
        .flat_map(|sample| {
            [
                Measurement {
                    metric: "correctness.anomalies",
                    value: bounded_count(sample.query_anomalies),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "columnar-range-datafusion-v1"),
                        ("anomaly.class", mode.id()),
                    ]),
                },
                Measurement {
                    metric: "query.result_exact",
                    value: if sample.query_anomalies == 0 {
                        1.0
                    } else {
                        0.0
                    },
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("query.class", "c5_projection_aggregate"),
                        ("backend", backend),
                        ("oracle", "columnar-range-datafusion-v1"),
                    ]),
                },
                Measurement {
                    metric: "operation.throughput",
                    value: sample.source_rows_per_second,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "datafusion_source_row"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "object_store.requests",
                    value: bounded_count(sample.object_requests),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", "filesystem-observed"),
                        ("api", "get.range"),
                        ("result", "ok"),
                    ]),
                },
                Measurement {
                    metric: "object_store.bytes",
                    value: bounded_count(sample.object_response_bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", "filesystem-observed"),
                        ("direction", "read"),
                        ("api", "get.range"),
                    ]),
                },
                Measurement {
                    metric: "htap.peak_memory",
                    value: bounded_count(sample.peak_batch_bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("query.class", "c5_projection_aggregate"),
                        ("backend", backend),
                        ("merge.kind", "range-stripe-source"),
                    ]),
                },
            ]
        })
        .collect();
    WorkloadExecution {
        error: (!passed).then(|| {
            format!(
                "columnar DataFusion gate failed: mode={}, exact={query_exact}, projection={projection_pushdown}, incremental={incremental_stripes}, range_io={range_io_exact}, memory={source_memory_bounded}, payload={payload_gate}, throughput={throughput_observed}",
                mode.id()
            )
        }),
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "columnar_datafusion.query_result_exact".to_owned(),
                status: gate_status(query_exact),
                detail: None,
            },
            HardGateResult {
                id: "columnar_datafusion.projection_pushdown_reaches_source".to_owned(),
                status: gate_status(projection_pushdown),
                detail: None,
            },
            HardGateResult {
                id: "columnar_datafusion.one_incremental_batch_per_stripe".to_owned(),
                status: gate_status(incremental_stripes),
                detail: None,
            },
            HardGateResult {
                id: "columnar_datafusion.range_reads_are_checksum_covered".to_owned(),
                status: gate_status(range_io_exact),
                detail: None,
            },
            HardGateResult {
                id: "columnar_datafusion.source_fetch_and_batch_buffers_are_bounded".to_owned(),
                status: gate_status(source_memory_bounded),
                detail: Some(format!(
                    "maximum_rows_per_batch={}",
                    profile.columnar_block_rows
                )),
            },
            HardGateResult {
                id: "columnar_datafusion.projected_scan_avoids_payload".to_owned(),
                status: gate_status(payload_gate),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "columnar_datafusion.throughput_is_observed".to_owned(),
                status: gate_status(throughput_observed),
                detail: None,
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: report
            .samples
            .iter()
            .map(|sample| {
                format!(
                    "okv-eval://columnar-range-datafusion-v1/{}/{}/{}/{}",
                    mode.id(),
                    sample.seed,
                    sample.repeat,
                    sample.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "columnar_datafusion.rows_per_second.median".to_owned(),
                median(
                    &report
                        .samples
                        .iter()
                        .map(|sample| sample.source_rows_per_second)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "columnar_datafusion.object_requests.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.object_requests as f64)
                    .sum(),
            ),
            (
                "columnar_datafusion.projection_fetch_requests.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.projection_fetch_requests as f64)
                    .sum(),
            ),
            (
                "columnar_datafusion.object_bytes.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.object_response_bytes as f64)
                    .sum(),
            ),
            (
                "columnar_datafusion.payload_requests.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.opaque_payload_requests as f64)
                    .sum(),
            ),
            (
                "columnar_datafusion.peak_batch_bytes.maximum".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.peak_batch_bytes as f64)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "columnar_datafusion.peak_fetch_bytes.maximum".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.peak_fetch_bytes as f64)
                    .fold(0.0_f64, f64::max),
            ),
            ("columnar_datafusion.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::cast_precision_loss)]
fn run_columnar_cache_admission(
    workload: &WorkloadConfig,
    run_id: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const LOCAL_BACKEND: &str = "columnar-cache+local-fs-range-stripes";
    const GCS_BACKEND: &str = "columnar-cache+gcs-range-stripes";
    if !matches!(backend, LOCAL_BACKEND | GCS_BACKEND) {
        return execution_from_result(Err(format!(
            "columnar cache admission requires {LOCAL_BACKEND} or {GCS_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(
            Err("columnar cache admission requires a dataset".to_owned()),
        );
    };
    let profile = match storage_layout_profile(dataset, seeds, profile_config) {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("cache_admission")
        .and_then(toml::Value::as_str)
        .unwrap_or("ghost_two_chance")
    {
        "full_admit" => ColumnarCacheAdmissionMode::FullAdmit,
        "never_admit_control" => ColumnarCacheAdmissionMode::NeverAdmitControl,
        "ghost_two_chance" => ColumnarCacheAdmissionMode::GhostTwoChance,
        other => {
            return execution_from_result(Err(format!(
                "unknown columnar cache admission mode {other}"
            )));
        }
    };
    let cache_ratio_percent = workload
        .parameters
        .get("cache_ratio_percent")
        .or_else(|| profile_config.parameters.get("cache_ratio_percent"))
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(20);
    let zipf_alpha = workload
        .parameters
        .get("zipf_alpha")
        .or_else(|| profile_config.parameters.get("zipf_alpha"))
        .and_then(|value| {
            value
                .as_float()
                .or_else(|| value.as_integer().map(|v| v as f64))
        })
        .unwrap_or(1.4);
    let started = Instant::now();
    let report_result = match backend {
        LOCAL_BACKEND => {
            run_columnar_cache_admission_contract(mode, &profile, cache_ratio_percent, zipf_alpha)
        }
        GCS_BACKEND => gcs_backend_from_env()
            .map_err(|error| error.to_string())
            .and_then(|object_backend| {
                let root_prefix = format!("objectkv/evals/columnar-cache/{run_id}");
                run_columnar_cache_admission_contract_on_backend(
                    mode,
                    &profile,
                    cache_ratio_percent,
                    zipf_alpha,
                    &object_backend,
                    &root_prefix,
                )
            }),
        _ => unreachable!("columnar cache backend was validated above"),
    };
    let report = match report_result {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    columnar_cache_admission_execution(
        workload,
        backend,
        mode,
        &report,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn columnar_cache_admission_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: ColumnarCacheAdmissionMode,
    report: &ColumnarCacheAdmissionReport,
    wall_seconds: f64,
) -> WorkloadExecution {
    let exact = report.samples.iter().all(|sample| {
        sample.point_anomalies == 0
            && sample.point_operations > 0
            && sample.pre_scan_hit_ratio.is_finite()
            && sample.post_scan_hit_ratio.is_finite()
    });
    let capacity_bounded = report
        .samples
        .iter()
        .all(|sample| sample.capacity_bytes > 0 && sample.resident_bytes <= sample.capacity_bytes);
    let pollution_exercised = report
        .samples
        .iter()
        .all(|sample| sample.pollution_object_requests > 0);
    let admission_state = report.samples.iter().all(|sample| match mode {
        ColumnarCacheAdmissionMode::FullAdmit => sample.resident_bytes > 0,
        ColumnarCacheAdmissionMode::NeverAdmitControl => {
            sample.resident_bytes == 0 && sample.post_scan_hit_ratio == 0.0
        }
        ColumnarCacheAdmissionMode::GhostTwoChance => {
            sample.resident_bytes > 0 && sample.ghost_entries > 0
        }
    });
    let mut traces = BTreeMap::new();
    let trace_stable = report.samples.iter().all(|sample| {
        traces
            .entry(sample.seed)
            .or_insert_with(|| sample.trace_sha256.clone())
            == &sample.trace_sha256
    });
    let passed =
        exact && capacity_bounded && pollution_exercised && admission_state && trace_stable;
    let measurements = report
        .samples
        .iter()
        .flat_map(|sample| {
            [
                Measurement {
                    metric: "correctness.anomalies",
                    value: bounded_count(sample.point_anomalies),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "columnar-cache-admission-v1"),
                        ("anomaly.class", mode.id()),
                    ]),
                },
                Measurement {
                    metric: "cache.hit_ratio",
                    value: sample.post_scan_hit_ratio,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("cache.tier", "range-engine-post-scan"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "object_store.requests",
                    value: bounded_count(sample.post_scan_object_requests),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", "filesystem-observed"),
                        ("api", "get.range"),
                        ("result", "ok"),
                    ]),
                },
                Measurement {
                    metric: "object_store.bytes",
                    value: bounded_count(sample.post_scan_response_bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", "filesystem-observed"),
                        ("direction", "read"),
                        ("api", "get.range"),
                    ]),
                },
                Measurement {
                    metric: "memory.resident_bytes",
                    value: bounded_count(sample.resident_bytes),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("component", "range-engine-cache"),
                        ("backend", backend),
                    ]),
                },
            ]
        })
        .collect();
    WorkloadExecution {
        error: (!passed).then(|| {
            format!(
                "columnar cache-admission gate failed: mode={}, exact={exact}, capacity={capacity_bounded}, pollution={pollution_exercised}, admission={admission_state}, replay={trace_stable}",
                mode.id()
            )
        }),
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "columnar_cache.point_results_are_exact".to_owned(),
                status: gate_status(exact),
                detail: None,
            },
            HardGateResult {
                id: "columnar_cache.capacity_is_bounded".to_owned(),
                status: gate_status(capacity_bounded),
                detail: Some(format!("cache_ratio_percent={}", report.cache_ratio_percent)),
            },
            HardGateResult {
                id: "columnar_cache.scan_pollution_is_exercised".to_owned(),
                status: gate_status(pollution_exercised),
                detail: None,
            },
            HardGateResult {
                id: "columnar_cache.admission_state_matches_policy".to_owned(),
                status: gate_status(admission_state),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "columnar_cache.trace_is_repeatable".to_owned(),
                status: gate_status(trace_stable),
                detail: Some(format!("zipf_alpha={}", report.zipf_alpha)),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: report
            .samples
            .iter()
            .map(|sample| {
                format!(
                    "okv-eval://columnar-cache-admission-v1/{}/{}/{}/{}",
                    mode.id(),
                    sample.seed,
                    sample.repeat,
                    sample.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "columnar_cache.pre_scan_hit_ratio.median".to_owned(),
                median(
                    &report
                        .samples
                        .iter()
                        .map(|sample| sample.pre_scan_hit_ratio)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "columnar_cache.post_scan_hit_ratio.median".to_owned(),
                median(
                    &report
                        .samples
                        .iter()
                        .map(|sample| sample.post_scan_hit_ratio)
                        .collect::<Vec<_>>(),
                ),
            ),
            (
                "columnar_cache.post_scan_requests.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.post_scan_object_requests as f64)
                    .sum(),
            ),
            (
                "columnar_cache.pollution_requests.total".to_owned(),
                report
                    .samples
                    .iter()
                    .map(|sample| sample.pollution_object_requests as f64)
                    .sum(),
            ),
            ("columnar_cache.wall_seconds".to_owned(), wall_seconds),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_storage_layout(
    workload: &WorkloadConfig,
    run_id: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile_config: &ProfileConfig,
) -> WorkloadExecution {
    const LOCAL_BACKEND: &str = "local-fs+observed-range-reads";
    const GCS_BACKEND: &str = "gcs+observed-range-reads";
    if !matches!(backend, LOCAL_BACKEND | GCS_BACKEND) {
        return execution_from_result(Err(format!(
            "storage-layout diagnostic requires {LOCAL_BACKEND} or {GCS_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "storage-layout diagnostic requires a dataset".to_owned()
        ));
    };
    if seeds.is_empty() || dataset.key_count == 0 {
        return execution_from_result(Err(
            "storage-layout diagnostic requires fixed seeds and keys".to_owned(),
        ));
    }
    let profile = match storage_layout_profile(dataset, seeds, profile_config) {
        Ok(profile) => profile,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_name = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("subject"))
        .and_then(toml::Value::as_str)
        .unwrap_or("indexed_row_object_control");
    if mode_name == "vortex_random_access_candidate" {
        return execution_from_result(Err(
            "Vortex diagnostic requires the isolated Rust 1.95 helper; the Rust 1.88 workspace does not claim this subject"
                .to_owned(),
        ));
    }
    let mode = match parse_storage_layout_mode(mode_name) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let baseline_mode = match workload
        .parameters
        .get("baseline_subject")
        .and_then(toml::Value::as_str)
    {
        Some(name) => match parse_storage_layout_mode(name) {
            Ok(baseline) if baseline != mode => Some(baseline),
            Ok(_) => {
                return execution_from_result(Err(
                    "storage-layout baseline must differ from the candidate".to_owned(),
                ));
            }
            Err(error) => return execution_from_result(Err(error)),
        },
        None => None,
    };
    let thresholds = match baseline_mode {
        Some(_) => match storage_layout_admission_thresholds(profile_config) {
            Ok(thresholds) => Some(thresholds),
            Err(error) => return execution_from_result(Err(error)),
        },
        None => None,
    };
    let started = Instant::now();
    let (report, baseline_report) = match (backend, baseline_mode) {
        (LOCAL_BACKEND, Some(baseline_mode)) => {
            match run_storage_layout_pair_contract(mode, baseline_mode, &profile) {
                Ok((report, baseline)) => (report, Some(baseline)),
                Err(error) => return execution_from_result(Err(error)),
            }
        }
        (LOCAL_BACKEND, None) => match run_storage_layout_contract(mode, &profile) {
            Ok(report) => (report, None),
            Err(error) => return execution_from_result(Err(error)),
        },
        (GCS_BACKEND, Some(baseline_mode)) => {
            let object_backend = match gcs_backend_from_env() {
                Ok(object_backend) => object_backend,
                Err(error) => return execution_from_result(Err(error.to_string())),
            };
            let root_prefix = format!("objectkv/evals/storage-layout/{run_id}");
            match run_storage_layout_pair_contract_on_backend(
                mode,
                baseline_mode,
                &profile,
                &object_backend,
                &root_prefix,
            ) {
                Ok((report, baseline)) => (report, Some(baseline)),
                Err(error) => return execution_from_result(Err(error)),
            }
        }
        (GCS_BACKEND, None) => {
            return execution_from_result(Err(
                "GCS storage-layout evaluation requires a same-durability baseline subject"
                    .to_owned(),
            ));
        }
        _ => unreachable!("storage-layout backend was validated above"),
    };
    let comparison = match baseline_report.as_ref() {
        Some(baseline) => match storage_layout_comparison(&report, baseline) {
            Ok(comparison) => Some(comparison),
            Err(error) => return execution_from_result(Err(error)),
        },
        None => None,
    };
    storage_layout_execution(
        workload,
        backend,
        mode,
        &report,
        comparison,
        thresholds,
        started.elapsed().as_secs_f64(),
    )
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn storage_layout_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: StorageLayoutMode,
    report: &StorageLayoutReport,
    comparison: Option<StorageLayoutComparison>,
    thresholds: Option<StorageLayoutAdmissionThresholds>,
    wall_seconds: f64,
) -> WorkloadExecution {
    let observed_store = if backend == "gcs+observed-range-reads" {
        "gcs-observed"
    } else {
        "filesystem-observed"
    };
    let is_poison = matches!(
        mode,
        StorageLayoutMode::ParquetFullFilePointPoison
            | StorageLayoutMode::HybridAccountingPoison
            | StorageLayoutMode::ColumnarInvalidationPoison
    );
    let logical_exact = report.samples.iter().all(|sample| {
        sample.point_anomalies == 0
            && sample.scan_anomalies == 0
            && sample.warm_point_anomalies == 0
            && sample.restart_anomalies == 0
            && sample.post_compaction_sha256 == sample.canonical_history_sha256
    });
    let manifest_exact = report
        .samples
        .iter()
        .all(|sample| sample.active_manifest_complete);
    let no_list = report
        .samples
        .iter()
        .all(|sample| sample.list_requests == 0);
    let checksum_covered = report
        .samples
        .iter()
        .all(|sample| sample.checksum_covered_ranges);
    let branch_exact = report
        .samples
        .iter()
        .all(|sample| sample.branch_reused_immutable_runs);
    let media_exact = report.samples.iter().all(|sample| {
        if mode == StorageLayoutMode::HybridAccountingPoison {
            sample.poison_detected
        } else {
            sample.accounting_anomalies == 0
        }
    });
    let bounded_point = report.samples.iter().all(|sample| {
        if mode == StorageLayoutMode::ParquetFullFilePointPoison {
            sample.poison_detected
        } else {
            sample.point_full_object_requests == 0
        }
    });
    let poison_detected = !is_poison || report.samples.iter().all(|sample| sample.poison_detected);
    let warm_overlay_exact = mode != StorageLayoutMode::ColumnarRangeOverlayCandidate
        || report
            .samples
            .iter()
            .all(|sample| sample.warm_point_operations > 0 && sample.warm_point_requests == 0);
    let projection_scan_exact = mode != StorageLayoutMode::ColumnarRangeOverlayCandidate
        || report
            .samples
            .iter()
            .all(|sample| sample.scan_opaque_payload_bytes == 0);
    let restart_exact = mode != StorageLayoutMode::ColumnarRangeOverlayCandidate
        || report
            .samples
            .iter()
            .all(|sample| sample.restart_anomalies == 0 && sample.restart_requests > 0);
    let overlay_bounded = mode != StorageLayoutMode::ColumnarRangeOverlayCandidate
        || report.samples.iter().all(|sample| {
            sample.overlay_capacity_bytes > 0
                && sample.overlay_resident_bytes <= sample.overlay_capacity_bytes
        });
    let admission_exact = comparison
        .zip(thresholds)
        .is_none_or(|(comparison, thresholds)| comparison.passes(thresholds));
    let exact = logical_exact
        && manifest_exact
        && no_list
        && checksum_covered
        && branch_exact
        && media_exact
        && bounded_point
        && poison_detected
        && warm_overlay_exact
        && projection_scan_exact
        && restart_exact
        && overlay_bounded
        && admission_exact;
    let error = (!exact).then(|| "storage-layout correctness or control gate failed".to_owned());

    let measurements = report
        .samples
        .iter()
        .flat_map(|sample| {
            let operations = sample.point_operations.max(1) as f64;
            [
                Measurement {
                    metric: "storage.amplification",
                    value: sample.storage_amplification,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("format", &sample.subject),
                    ]),
                },
                Measurement {
                    metric: "object_store.requests",
                    value: sample.point_requests as f64 / operations,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", observed_store),
                        ("api", "point_get_per_operation"),
                        ("result", "ok"),
                    ]),
                },
                Measurement {
                    metric: "object_store.bytes",
                    value: sample.point_response_bytes as f64 / operations,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("store", observed_store),
                        ("direction", "read_per_point"),
                        ("api", "get"),
                    ]),
                },
                Measurement {
                    metric: "operation.throughput",
                    value: sample.scan_rows_per_second,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "projected_scan"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "compaction.write_amplification",
                    value: sample.compaction_write_amplification,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("compaction.kind", "base+deltas+full-history"),
                    ]),
                },
                Measurement {
                    metric: "memory.resident_bytes",
                    value: sample.resident_index_bytes as f64,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("component", "manifest+primary-index"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "branch.incremental_bytes",
                    value: sample.branch_incremental_bytes as f64,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("branch.kind", "shared-immutable-runs"),
                    ]),
                },
            ]
        })
        .chain(std::iter::once(Measurement {
            metric: "correctness.anomalies",
            value: report
                .samples
                .iter()
                .map(okv_eval::storage_layout::StorageLayoutSample::correctness_anomalies)
                .sum::<u64>() as f64,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "typed-mvcc-storage-layout-history-v1"),
                ("anomaly.class", mode.subject()),
            ]),
        }))
        .collect();
    let mut hard_gates = vec![
        HardGateResult {
            id: "same_history_and_post_compaction_digest".to_owned(),
            status: gate_status(logical_exact),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "one_active_manifest_has_complete_closure".to_owned(),
            status: gate_status(manifest_exact),
            detail: None,
        },
        HardGateResult {
            id: "no_list_dependency".to_owned(),
            status: gate_status(no_list),
            detail: None,
        },
        HardGateResult {
            id: "fetched_ranges_are_checksum_covered".to_owned(),
            status: gate_status(checksum_covered),
            detail: None,
        },
        HardGateResult {
            id: "complete_media_accounting".to_owned(),
            status: gate_status(media_exact),
            detail: None,
        },
        HardGateResult {
            id: "point_read_never_scans_complete_object".to_owned(),
            status: gate_status(bounded_point),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "branch_reuses_unchanged_runs".to_owned(),
            status: gate_status(branch_exact),
            detail: None,
        },
        HardGateResult {
            id: "negative_control_detected".to_owned(),
            status: gate_status(poison_detected),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "warm_overlay_replays_without_object_io".to_owned(),
            status: gate_status(warm_overlay_exact),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "projected_scan_fetches_no_opaque_payload".to_owned(),
            status: gate_status(projection_scan_exact),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "empty_overlay_restart_reconstructs_exact_history".to_owned(),
            status: gate_status(restart_exact),
            detail: Some(format!("subject={}", mode.subject())),
        },
        HardGateResult {
            id: "overlay_cache_respects_byte_bound".to_owned(),
            status: gate_status(overlay_bounded),
            detail: Some(format!("subject={}", mode.subject())),
        },
    ];
    let mut secondary_metrics = BTreeMap::from([
        (
            "storage_layout.point_p99_ns.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.point_latency_ns_p99 as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.point_response_bytes_per_operation.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| {
                    sample.point_response_bytes as f64 / sample.point_operations.max(1) as f64
                })
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.actual_stored_bytes.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.stored_bytes as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.overlay_fill_requests.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.overlay_fill_requests as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.overlay_resident_bytes.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.overlay_resident_bytes as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.overlay_capacity_bytes.minimum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.overlay_capacity_bytes as f64)
                .reduce(f64::min)
                .unwrap_or(0.0),
        ),
        (
            "storage_layout.warm_point_requests.total".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.warm_point_requests as f64)
                .sum(),
        ),
        (
            "storage_layout.warm_point_p99_ns.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.warm_point_latency_ns_p99 as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.scan_opaque_payload_bytes.total".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.scan_opaque_payload_bytes as f64)
                .sum(),
        ),
        (
            "storage_layout.restart_requests.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.restart_requests as f64)
                .fold(0.0_f64, f64::max),
        ),
        (
            "storage_layout.restart_response_bytes.maximum".to_owned(),
            report
                .samples
                .iter()
                .map(|sample| sample.restart_response_bytes as f64)
                .fold(0.0_f64, f64::max),
        ),
        ("storage_layout.wall_seconds".to_owned(), wall_seconds),
    ]);
    if let Some((comparison, thresholds)) = comparison.zip(thresholds) {
        let admission_gates = [
            (
                "admission_same_histories",
                comparison.same_histories,
                format!("same_histories={}", comparison.same_histories),
            ),
            (
                "admission_point_request_ratio",
                comparison.point_request_ratio_max <= thresholds.point_request_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.point_request_ratio_max, thresholds.point_request_ratio_max
                ),
            ),
            (
                "admission_point_bytes_ratio",
                comparison.point_bytes_ratio_max <= thresholds.point_bytes_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.point_bytes_ratio_max, thresholds.point_bytes_ratio_max
                ),
            ),
            (
                "admission_point_p99_ratio",
                comparison.point_p99_ratio <= thresholds.point_p99_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.point_p99_ratio, thresholds.point_p99_ratio_max
                ),
            ),
            (
                "admission_scan_throughput_ratio",
                comparison.scan_throughput_ratio >= thresholds.scan_throughput_ratio_min,
                format!(
                    "observed={}, min={}",
                    comparison.scan_throughput_ratio, thresholds.scan_throughput_ratio_min
                ),
            ),
            (
                "admission_storage_amplification_ratio",
                comparison.storage_amplification_ratio_max
                    <= thresholds.storage_amplification_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.storage_amplification_ratio_max,
                    thresholds.storage_amplification_ratio_max
                ),
            ),
            (
                "admission_compaction_write_amplification_ratio",
                comparison.compaction_write_amplification_ratio_max
                    <= thresholds.compaction_write_amplification_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.compaction_write_amplification_ratio_max,
                    thresholds.compaction_write_amplification_ratio_max
                ),
            ),
            (
                "admission_resident_index_ratio",
                comparison.resident_index_ratio_max <= thresholds.resident_index_ratio_max,
                format!(
                    "observed={}, max={}",
                    comparison.resident_index_ratio_max, thresholds.resident_index_ratio_max
                ),
            ),
        ];
        hard_gates.extend(
            admission_gates
                .into_iter()
                .map(|(id, passed, detail)| HardGateResult {
                    id: id.to_owned(),
                    status: gate_status(passed),
                    detail: Some(detail),
                }),
        );
        secondary_metrics.extend([
            (
                "storage_layout.admission.point_request_ratio.maximum".to_owned(),
                comparison.point_request_ratio_max,
            ),
            (
                "storage_layout.admission.point_bytes_ratio.maximum".to_owned(),
                comparison.point_bytes_ratio_max,
            ),
            (
                "storage_layout.admission.point_p99_ratio".to_owned(),
                comparison.point_p99_ratio,
            ),
            (
                "storage_layout.admission.scan_throughput_ratio".to_owned(),
                comparison.scan_throughput_ratio,
            ),
            (
                "storage_layout.admission.storage_amplification_ratio.maximum".to_owned(),
                comparison.storage_amplification_ratio_max,
            ),
            (
                "storage_layout.admission.compaction_write_amplification_ratio.maximum".to_owned(),
                comparison.compaction_write_amplification_ratio_max,
            ),
            (
                "storage_layout.admission.resident_index_ratio.maximum".to_owned(),
                comparison.resident_index_ratio_max,
            ),
        ]);
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: wall_seconds,
        artifact_refs: report
            .samples
            .iter()
            .map(|sample| {
                format!(
                    "okv-eval://storage-layout-v1/{}/{}/repeat-{}/{}",
                    sample.subject, sample.seed, sample.repeat, sample.canonical_history_sha256
                )
            })
            .collect(),
        secondary_metrics,
    }
}

#[allow(clippy::too_many_lines)]
fn object_frontier_execution(
    workload: &WorkloadConfig,
    backend: &str,
    mode: ObjectFrontierMode,
    reports: &[ObjectFrontierReport],
    replay: &ObjectFrontierReport,
    wall_seconds: f64,
) -> WorkloadExecution {
    let anomalies = reports
        .iter()
        .map(|report| report.correctness_anomalies)
        .sum::<u64>();
    let exact_replay = reports.first().is_some_and(|first| {
        first.semantic_sha256 == replay.semantic_sha256
            && first.correctness_anomalies == replay.correctness_anomalies
    });
    let topology_exact = reports
        .iter()
        .all(|report| report.authority_processes == 3 && report.data_processes == 3);
    let control_rejected = reports
        .iter()
        .all(|report| report.unsafe_transition_rejected);
    let candidate_exact = reports.iter().all(|report| {
        report.pending_frontier_protected
            && !report.pending_frontier_retained
            && report.closure_validated
            && report.physical_pop_applied
            && report.popped_records == report.retained_records_before
            && report.retained_records_after == 0
            && report.persisted_retention_floor == report.requested_frontier
            && report.stale_cursor_rejected
            && report.exact_pop_retry
            && report.certificate_signers >= 2
            && report.activation_accepted
            && report.active_frontier_exact
            && report.data_leader_failover
            && report.authority_leader_failover
            && report.restarted_data_voter
            && report.recovered_state_exact
    });
    let missing_pending_exact = reports.iter().all(|report| {
        !report.pending_frontier_protected
            && report.closure_validated
            && !report.physical_pop_applied
            && report.retained_records_after == report.retained_records_before
            && report.unsafe_transition_rejected
    });
    let forged_coverage_exact = reports.iter().all(|report| {
        report.pending_frontier_protected
            && report.pending_frontier_retained
            && !report.closure_validated
            && !report.physical_pop_applied
            && report.retained_records_after == report.retained_records_before
            && report.unsafe_transition_rejected
    });
    let subquorum_exact = reports.iter().all(|report| {
        report.pending_frontier_protected
            && report.pending_frontier_retained
            && report.closure_validated
            && report.physical_pop_applied
            && report.retained_records_after == 0
            && report.stale_cursor_rejected
            && report.certificate_signers >= 2
            && !report.activation_accepted
            && !report.active_frontier_exact
            && report.unsafe_transition_rejected
            && report.recovered_state_exact
    });
    let mode_exact = match mode {
        ObjectFrontierMode::Candidate => candidate_exact,
        ObjectFrontierMode::MissingPendingControl => missing_pending_exact,
        ObjectFrontierMode::ForgedCoverageControl => forged_coverage_exact,
        ObjectFrontierMode::SubquorumControl => subquorum_exact,
    };
    let error = if anomalies != 0 {
        Some(format!(
            "authenticated object frontier returned {anomalies} correctness anomalies"
        ))
    } else if !topology_exact {
        Some("authenticated object frontier did not start both three-voter quorums".to_owned())
    } else if !mode_exact {
        Some(format!(
            "authenticated object frontier mode {} violated its frozen contract",
            mode.id()
        ))
    } else if mode != ObjectFrontierMode::Candidate && !control_rejected {
        Some("authenticated object-frontier negative control was not rejected".to_owned())
    } else if !exact_replay {
        Some("authenticated object-frontier fresh replay changed semantic state".to_owned())
    } else {
        None
    };
    let phase_seconds = reports
        .iter()
        .map(|report| {
            report.prepare_seconds
                + report.validation_seconds
                + report.pop_seconds
                + report.certificate_seconds
                + report.activation_seconds
                + report.recovery_seconds
        })
        .collect::<Vec<_>>();
    let pop_seconds = reports
        .iter()
        .map(|report| report.pop_seconds)
        .collect::<Vec<_>>();
    let closure_bytes = reports
        .iter()
        .map(|report| resident_count_as_f64(report.closure_bytes))
        .collect::<Vec<_>>();
    let measurements = reports
        .iter()
        .flat_map(|report| {
            let protocol_seconds = report.prepare_seconds
                + report.validation_seconds
                + report.pop_seconds
                + report.certificate_seconds
                + report.activation_seconds
                + report.recovery_seconds;
            [
                Measurement {
                    metric: "object_frontier.protocol_duration",
                    value: protocol_seconds,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("mode", mode.id()),
                        (
                            "result",
                            if report.correctness_anomalies == 0 {
                                "pass"
                            } else {
                                "fail"
                            },
                        ),
                    ]),
                },
                Measurement {
                    metric: "correctness.anomalies",
                    value: resident_count_as_f64(report.correctness_anomalies),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "authenticated-object-frontier-v1"),
                        (
                            "anomaly.class",
                            if report.correctness_anomalies == 0 {
                                "none"
                            } else {
                                "object_frontier_contract"
                            },
                        ),
                    ]),
                },
            ]
        })
        .collect::<Vec<_>>();
    let first = reports.first();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "three_publication_and_three_data_voters".to_owned(),
                status: gate_status(topology_exact),
                detail: Some("publication=3,data=3".to_owned()),
            },
            HardGateResult {
                id: "mode_specific_safe_transition".to_owned(),
                status: gate_status(mode_exact),
                detail: Some(mode.id().to_owned()),
            },
            HardGateResult {
                id: "unsafe_control_rejected".to_owned(),
                status: gate_status(mode == ObjectFrontierMode::Candidate || control_rejected),
                detail: Some(format!("mode={}", mode.id())),
            },
            HardGateResult {
                id: "physical_pop_and_exact_object_recovery".to_owned(),
                status: gate_status(match mode {
                    ObjectFrontierMode::Candidate | ObjectFrontierMode::SubquorumControl => reports
                        .iter()
                        .all(|report| report.physical_pop_applied && report.recovered_state_exact),
                    ObjectFrontierMode::MissingPendingControl
                    | ObjectFrontierMode::ForgedCoverageControl => {
                        reports.iter().all(|report| !report.physical_pop_applied)
                    }
                }),
                detail: first.map(|report| {
                    format!(
                        "popped={},floor={},remaining={}",
                        report.popped_records,
                        report.persisted_retention_floor,
                        report.retained_records_after
                    )
                }),
            },
            HardGateResult {
                id: "fresh_controller_semantic_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: first.map(|report| report.semantic_sha256.clone()),
            },
        ],
        budget_units: wall_seconds,
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-eval://authenticated-object-frontier-v1/{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.semantic_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "object_frontier.protocol_seconds.median".to_owned(),
                median(&phase_seconds),
            ),
            (
                "object_frontier.pop_seconds.median".to_owned(),
                median(&pop_seconds),
            ),
            (
                "object_frontier.closure_bytes.median".to_owned(),
                median(&closure_bytes),
            ),
            ("object_frontier.wall_seconds".to_owned(), wall_seconds),
            (
                "object_frontier.correctness_anomalies".to_owned(),
                resident_count_as_f64(anomalies),
            ),
            (
                "object_frontier.exact_replay".to_owned(),
                if exact_replay { 1.0 } else { 0.0 },
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_generation_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "generation process workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_generation_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut data_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut generation_preparations = 0_u64;
    let mut generation_reservations = 0_u64;
    let mut generation_activations = 0_u64;
    let mut committed_data_writes = 0_u64;
    let mut fenced_commit_attempts = 0_u64;
    let mut fenced_commit_rejections = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_generation_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_generation_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        data_process_starts = data_process_starts.saturating_add(first.data_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        generation_preparations =
            generation_preparations.saturating_add(first.generation_preparations);
        generation_reservations =
            generation_reservations.saturating_add(first.generation_reservations);
        generation_activations =
            generation_activations.saturating_add(first.generation_activations);
        committed_data_writes = committed_data_writes.saturating_add(first.committed_data_writes);
        fenced_commit_attempts =
            fenced_commit_attempts.saturating_add(first.fenced_commit_attempts);
        fenced_commit_rejections =
            fenced_commit_rejections.saturating_add(first.fenced_commit_rejections);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_generation_two_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "generation-takeover-process-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "generation_takeover" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_data_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-generation"),
                    ("result", if exact { "committed" } else { "unsafe-control" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "cell-generation-takeover"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-generation-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_preparations = if mode == GenerationProcessMode::AcceptCompetingRecovery {
        2
    } else {
        1
    };
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(16)
        && authority_process_starts == seed_count.saturating_mul(3)
        && data_process_starts == seed_count.saturating_mul(6)
        && process_kills == seed_count
        && authority_failovers == seed_count
        && learner_additions == seed_count.saturating_mul(3)
        && membership_changes == seed_count
        && generation_preparations == seed_count.saturating_mul(expected_preparations)
        && generation_reservations == seed_count
        && generation_activations == seed_count
        && fenced_commit_attempts == seed_count.saturating_mul(4);
    let expected_success_path = mode != GenerationProcessMode::Correct
        || (committed_data_writes == seed_count.saturating_mul(2)
            && fenced_commit_rejections == seed_count.saturating_mul(4)
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "generation takeover gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "generation_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "generation_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, authority_starts={authority_process_starts}, data_starts={data_process_starts}, kills={process_kills}, authority_failovers={authority_failovers}, learners={learner_additions}, membership_changes={membership_changes}, preparations={generation_preparations}, reservations={generation_reservations}, activations={generation_activations}, data_commits={committed_data_writes}, fence_attempts={fenced_commit_attempts}, fence_rejections={fenced_commit_rejections}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "generation_process.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("generation_process.checks".to_owned(), bounded_count(check_count)),
            (
                "generation_process.authority_failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "generation_process.membership_changes".to_owned(),
                bounded_count(membership_changes),
            ),
            (
                "generation_process.committed_data_writes".to_owned(),
                bounded_count(committed_data_writes),
            ),
            (
                "generation_process.fenced_commit_rejections".to_owned(),
                bounded_count(fenced_commit_rejections),
            ),
            (
                "generation_process.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_publication_publisher_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "publisher process workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_publisher_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut publisher_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut object_puts = 0_u64;
    let mut publication_writes = 0_u64;
    let mut empty_scratch_restarts = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_publication_publisher_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_publication_publisher_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        publisher_process_starts =
            publisher_process_starts.saturating_add(first.publisher_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        object_puts = object_puts.saturating_add(first.object_puts);
        publication_writes = publication_writes.saturating_add(first.publication_writes);
        empty_scratch_restarts =
            empty_scratch_restarts.saturating_add(first.empty_scratch_restarts);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "publisher-prepare-restart-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "publisher_ordering" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publisher-prepare-restart"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publisher-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(10)
        && authority_process_starts == seed_count.saturating_mul(3)
        && if mode == PublisherProcessMode::Correct {
            publisher_process_starts == seed_count.saturating_mul(2)
                && process_kills == seed_count
                && object_puts == seed_count.saturating_mul(3)
                && publication_writes == seed_count.saturating_mul(3)
                && empty_scratch_restarts == seed_count
        } else {
            publisher_process_starts == seed_count
                && process_kills == seed_count
                && object_puts == seed_count
                && publication_writes == 0
                && empty_scratch_restarts == 0
        };
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "publisher process gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "publisher_process.exact_fresh_process_replay".to_owned(),
            status: if exact_replay {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: None,
        },
        HardGateResult {
            id: "publisher_process.semantic_operations_exercised".to_owned(),
            status: if semantic_operations_exercised {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: Some(format!(
                "checks={check_count}, authority_starts={authority_process_starts}, publisher_starts={publisher_process_starts}, kills={process_kills}, object_puts={object_puts}, publication_writes={publication_writes}, empty_scratch_restarts={empty_scratch_restarts}"
            )),
        },
        HardGateResult {
            id: "publisher_process.contract_agreement".to_owned(),
            status: if anomaly_count == 0 {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: mismatch_details.first().cloned(),
        },
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: id.clone(),
        status: if *passed {
            GateStatus::Pass
        } else {
            GateStatus::Fail
        },
        detail: None,
    }));

    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publisher_process.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publisher_process.authority_starts".to_owned(),
                bounded_count(authority_process_starts),
            ),
            (
                "publisher_process.publisher_starts".to_owned(),
                bounded_count(publisher_process_starts),
            ),
            (
                "publisher_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "publisher_process.object_puts".to_owned(),
                bounded_count(object_puts),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_publication_publisher_put_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "publisher PUT recovery workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_publisher_put_recovery_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut publisher_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut put_attempts = 0_u64;
    let mut object_effects = 0_u64;
    let mut injected_unknown_responses = 0_u64;
    let mut existing_object_recoveries = 0_u64;
    let mut named_verification_reads = 0_u64;
    let mut publication_command_attempts = 0_u64;
    let mut empty_scratch_restarts = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_publication_publisher_put_recovery_contract(*seed, mode, &executable)
        {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_publication_publisher_put_recovery_contract(*seed, mode, &executable)
        {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        publisher_process_starts =
            publisher_process_starts.saturating_add(first.publisher_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        put_attempts = put_attempts.saturating_add(first.put_attempts);
        object_effects = object_effects.saturating_add(first.object_effects);
        injected_unknown_responses =
            injected_unknown_responses.saturating_add(first.injected_unknown_responses);
        existing_object_recoveries =
            existing_object_recoveries.saturating_add(first.existing_object_recoveries);
        named_verification_reads =
            named_verification_reads.saturating_add(first.named_verification_reads);
        publication_command_attempts =
            publication_command_attempts.saturating_add(first.publication_command_attempts);
        empty_scratch_restarts =
            empty_scratch_restarts.saturating_add(first.empty_scratch_restarts);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "publisher-ambiguous-put-restart-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "publication_closure" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publisher-ambiguous-put-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publisher-put-recovery://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let common_operations = check_count == seed_count.saturating_mul(12)
        && authority_process_starts == seed_count.saturating_mul(3)
        && publisher_process_starts == seed_count.saturating_mul(2)
        && process_kills == seed_count
        && injected_unknown_responses == seed_count
        && publication_command_attempts == seed_count.saturating_mul(3)
        && empty_scratch_restarts == seed_count;
    let mode_operations = if mode == PublisherPutRecoveryMode::Correct {
        put_attempts == seed_count.saturating_mul(4)
            && object_effects == seed_count.saturating_mul(3)
            && existing_object_recoveries == seed_count
            && named_verification_reads == seed_count.saturating_mul(6)
    } else {
        put_attempts == seed_count.saturating_mul(2)
            && object_effects == seed_count.saturating_mul(2)
            && existing_object_recoveries == 0
            && named_verification_reads == seed_count
    };
    let semantic_operations_exercised = common_operations && mode_operations;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "publisher PUT recovery gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "publisher_put_recovery.exact_fresh_process_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: None,
        },
        HardGateResult {
            id: "publisher_put_recovery.semantic_operations_exercised".to_owned(),
            status: gate_status(semantic_operations_exercised),
            detail: Some(format!(
                "checks={check_count}, authority_starts={authority_process_starts}, publisher_starts={publisher_process_starts}, kills={process_kills}, put_attempts={put_attempts}, effects={object_effects}, unknown_responses={injected_unknown_responses}, existing_recoveries={existing_object_recoveries}, named_reads={named_verification_reads}, publication_attempts={publication_command_attempts}, empty_scratch_restarts={empty_scratch_restarts}"
            )),
        },
        HardGateResult {
            id: "publisher_put_recovery.contract_agreement".to_owned(),
            status: gate_status(anomaly_count == 0),
            detail: mismatch_details.first().cloned(),
        },
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: id.clone(),
        status: gate_status(*passed),
        detail: None,
    }));

    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publisher_put_recovery.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publisher_put_recovery.authority_starts".to_owned(),
                bounded_count(authority_process_starts),
            ),
            (
                "publisher_put_recovery.publisher_starts".to_owned(),
                bounded_count(publisher_process_starts),
            ),
            (
                "publisher_put_recovery.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "publisher_put_recovery.put_attempts".to_owned(),
                bounded_count(put_attempts),
            ),
            (
                "publisher_put_recovery.object_effects".to_owned(),
                bounded_count(object_effects),
            ),
            (
                "publisher_put_recovery.unknown_responses".to_owned(),
                bounded_count(injected_unknown_responses),
            ),
            (
                "publisher_put_recovery.existing_recoveries".to_owned(),
                bounded_count(existing_object_recoveries),
            ),
            (
                "publisher_put_recovery.named_reads".to_owned(),
                bounded_count(named_verification_reads),
            ),
            (
                "publisher_put_recovery.publication_attempts".to_owned(),
                bounded_count(publication_command_attempts),
            ),
            (
                "publisher_put_recovery.empty_scratch_restarts".to_owned(),
                bounded_count(empty_scratch_restarts),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_publication_publisher_manifest_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "publisher manifest recovery workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_publisher_manifest_recovery_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut publisher_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut put_attempts = 0_u64;
    let mut object_effects = 0_u64;
    let mut injected_unknown_responses = 0_u64;
    let mut existing_object_recoveries = 0_u64;
    let mut named_verification_reads = 0_u64;
    let mut publication_command_attempts = 0_u64;
    let mut empty_scratch_restarts = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first =
            match run_publication_publisher_manifest_recovery_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        let second =
            match run_publication_publisher_manifest_recovery_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        publisher_process_starts =
            publisher_process_starts.saturating_add(first.publisher_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        put_attempts = put_attempts.saturating_add(first.put_attempts);
        object_effects = object_effects.saturating_add(first.object_effects);
        injected_unknown_responses =
            injected_unknown_responses.saturating_add(first.injected_unknown_responses);
        existing_object_recoveries =
            existing_object_recoveries.saturating_add(first.existing_object_recoveries);
        named_verification_reads =
            named_verification_reads.saturating_add(first.named_verification_reads);
        publication_command_attempts =
            publication_command_attempts.saturating_add(first.publication_command_attempts);
        empty_scratch_restarts =
            empty_scratch_restarts.saturating_add(first.empty_scratch_restarts);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "publisher-ambiguous-manifest-restart-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "publication_closure" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publisher-ambiguous-manifest-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publisher-manifest-recovery://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let common_operations = check_count == seed_count.saturating_mul(13)
        && authority_process_starts == seed_count.saturating_mul(3)
        && publisher_process_starts == seed_count.saturating_mul(2)
        && process_kills == seed_count
        && injected_unknown_responses == seed_count
        && publication_command_attempts == seed_count.saturating_mul(3)
        && empty_scratch_restarts == seed_count;
    let mode_operations = if mode == PublisherManifestRecoveryMode::Correct {
        put_attempts == seed_count.saturating_mul(6)
            && object_effects == seed_count.saturating_mul(3)
            && existing_object_recoveries == seed_count.saturating_mul(3)
            && named_verification_reads == seed_count.saturating_mul(8)
    } else {
        put_attempts == seed_count.saturating_mul(3)
            && object_effects == seed_count.saturating_mul(2)
            && existing_object_recoveries == seed_count
            && named_verification_reads == seed_count.saturating_mul(2)
    };
    let semantic_operations_exercised = common_operations && mode_operations;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "publisher manifest recovery gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "publisher_manifest_recovery.exact_fresh_process_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: None,
        },
        HardGateResult {
            id: "publisher_manifest_recovery.semantic_operations_exercised".to_owned(),
            status: gate_status(semantic_operations_exercised),
            detail: Some(format!(
                "checks={check_count}, authority_starts={authority_process_starts}, publisher_starts={publisher_process_starts}, kills={process_kills}, put_attempts={put_attempts}, effects={object_effects}, unknown_responses={injected_unknown_responses}, existing_recoveries={existing_object_recoveries}, named_reads={named_verification_reads}, publication_attempts={publication_command_attempts}, empty_scratch_restarts={empty_scratch_restarts}"
            )),
        },
        HardGateResult {
            id: "publisher_manifest_recovery.contract_agreement".to_owned(),
            status: gate_status(anomaly_count == 0),
            detail: mismatch_details.first().cloned(),
        },
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: id.clone(),
        status: gate_status(*passed),
        detail: None,
    }));

    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publisher_manifest_recovery.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publisher_manifest_recovery.authority_starts".to_owned(),
                bounded_count(authority_process_starts),
            ),
            (
                "publisher_manifest_recovery.publisher_starts".to_owned(),
                bounded_count(publisher_process_starts),
            ),
            (
                "publisher_manifest_recovery.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "publisher_manifest_recovery.put_attempts".to_owned(),
                bounded_count(put_attempts),
            ),
            (
                "publisher_manifest_recovery.object_effects".to_owned(),
                bounded_count(object_effects),
            ),
            (
                "publisher_manifest_recovery.unknown_responses".to_owned(),
                bounded_count(injected_unknown_responses),
            ),
            (
                "publisher_manifest_recovery.existing_recoveries".to_owned(),
                bounded_count(existing_object_recoveries),
            ),
            (
                "publisher_manifest_recovery.named_reads".to_owned(),
                bounded_count(named_verification_reads),
            ),
            (
                "publisher_manifest_recovery.publication_attempts".to_owned(),
                bounded_count(publication_command_attempts),
            ),
            (
                "publisher_manifest_recovery.empty_scratch_restarts".to_owned(),
                bounded_count(empty_scratch_restarts),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_publication_publisher_publish_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "publisher Publish recovery workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_publisher_publish_recovery_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut publisher_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut object_put_attempts = 0_u64;
    let mut object_effects = 0_u64;
    let mut named_verification_reads = 0_u64;
    let mut publish_command_attempts = 0_u64;
    let mut publish_applies = 0_u64;
    let mut dropped_publish_replies = 0_u64;
    let mut recovered_publish_outcomes = 0_u64;
    let mut exact_outcome_replays = 0_u64;
    let mut empty_scratch_restarts = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first =
            match run_publication_publisher_publish_recovery_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        let second =
            match run_publication_publisher_publish_recovery_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        publisher_process_starts =
            publisher_process_starts.saturating_add(first.publisher_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        object_put_attempts = object_put_attempts.saturating_add(first.object_put_attempts);
        object_effects = object_effects.saturating_add(first.object_effects);
        named_verification_reads =
            named_verification_reads.saturating_add(first.named_verification_reads);
        publish_command_attempts =
            publish_command_attempts.saturating_add(first.publish_command_attempts);
        publish_applies = publish_applies.saturating_add(first.publish_applies);
        dropped_publish_replies =
            dropped_publish_replies.saturating_add(first.dropped_publish_replies);
        recovered_publish_outcomes =
            recovered_publish_outcomes.saturating_add(first.recovered_publish_outcomes);
        exact_outcome_replays = exact_outcome_replays.saturating_add(first.exact_outcome_replays);
        empty_scratch_restarts =
            empty_scratch_restarts.saturating_add(first.empty_scratch_restarts);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "publisher-lost-publish-response-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "authority_outcome" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publisher-lost-publish-response-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publisher-publish-recovery://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let common_operations = check_count == seed_count.saturating_mul(14)
        && authority_process_starts == seed_count.saturating_mul(3)
        && publisher_process_starts == seed_count.saturating_mul(2)
        && process_kills == seed_count.saturating_mul(2)
        && authority_failovers == seed_count
        && object_put_attempts == seed_count.saturating_mul(3)
        && object_effects == seed_count.saturating_mul(3)
        && named_verification_reads == seed_count.saturating_mul(15)
        && publish_command_attempts == seed_count.saturating_mul(2)
        && dropped_publish_replies == seed_count
        && empty_scratch_restarts == seed_count;
    let mode_operations = if mode == PublisherPublishRecoveryMode::Correct {
        publish_applies == seed_count
            && recovered_publish_outcomes == seed_count
            && exact_outcome_replays == seed_count
    } else {
        publish_applies == seed_count.saturating_mul(2)
            && recovered_publish_outcomes == 0
            && exact_outcome_replays == 0
    };
    let semantic_operations_exercised = common_operations && mode_operations;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "publisher Publish recovery gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "publisher_publish_recovery.exact_fresh_process_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: None,
        },
        HardGateResult {
            id: "publisher_publish_recovery.semantic_operations_exercised".to_owned(),
            status: gate_status(semantic_operations_exercised),
            detail: Some(format!(
                "checks={check_count}, authority_starts={authority_process_starts}, publisher_starts={publisher_process_starts}, kills={process_kills}, failovers={authority_failovers}, object_put_attempts={object_put_attempts}, effects={object_effects}, named_reads={named_verification_reads}, publish_attempts={publish_command_attempts}, publish_applies={publish_applies}, dropped_replies={dropped_publish_replies}, recovered_outcomes={recovered_publish_outcomes}, exact_replays={exact_outcome_replays}, empty_scratch_restarts={empty_scratch_restarts}"
            )),
        },
        HardGateResult {
            id: "publisher_publish_recovery.contract_agreement".to_owned(),
            status: gate_status(anomaly_count == 0),
            detail: mismatch_details.first().cloned(),
        },
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: id.clone(),
        status: gate_status(*passed),
        detail: None,
    }));

    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publisher_publish_recovery.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publisher_publish_recovery.authority_starts".to_owned(),
                bounded_count(authority_process_starts),
            ),
            (
                "publisher_publish_recovery.publisher_starts".to_owned(),
                bounded_count(publisher_process_starts),
            ),
            (
                "publisher_publish_recovery.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "publisher_publish_recovery.authority_failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "publisher_publish_recovery.object_put_attempts".to_owned(),
                bounded_count(object_put_attempts),
            ),
            (
                "publisher_publish_recovery.object_effects".to_owned(),
                bounded_count(object_effects),
            ),
            (
                "publisher_publish_recovery.named_reads".to_owned(),
                bounded_count(named_verification_reads),
            ),
            (
                "publisher_publish_recovery.publish_attempts".to_owned(),
                bounded_count(publish_command_attempts),
            ),
            (
                "publisher_publish_recovery.publish_applies".to_owned(),
                bounded_count(publish_applies),
            ),
            (
                "publisher_publish_recovery.dropped_replies".to_owned(),
                bounded_count(dropped_publish_replies),
            ),
            (
                "publisher_publish_recovery.recovered_outcomes".to_owned(),
                bounded_count(recovered_publish_outcomes),
            ),
            (
                "publisher_publish_recovery.exact_outcome_replays".to_owned(),
                bounded_count(exact_outcome_replays),
            ),
            (
                "publisher_publish_recovery.empty_scratch_restarts".to_owned(),
                bounded_count(empty_scratch_restarts),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_publication_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "publication process workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_publication_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut publication_writes = 0_u64;
    let mut generation_transitions = 0_u64;
    let mut dropped_replies = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut duplicate_retries = 0_u64;
    let mut deletion_reservations = 0_u64;
    let mut restarted_nodes = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_publication_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_publication_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        publication_writes = publication_writes.saturating_add(first.publication_writes);
        generation_transitions =
            generation_transitions.saturating_add(first.generation_transitions);
        dropped_replies = dropped_replies.saturating_add(first.dropped_replies);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        duplicate_retries = duplicate_retries.saturating_add(first.duplicate_retries);
        deletion_reservations = deletion_reservations.saturating_add(first.deletion_reservations);
        restarted_nodes = restarted_nodes.saturating_add(first.restarted_nodes);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "publication-authority-process-v1"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "publication_authority"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publication-authority-failover"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publication-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(24)
        && authority_process_starts >= seed_count.saturating_mul(9)
        && process_kills >= seed_count.saturating_mul(6)
        && authority_failovers == seed_count.saturating_mul(2)
        && publication_writes == seed_count.saturating_mul(23)
        && generation_transitions == seed_count
        && dropped_replies == seed_count
        && recovered_outcomes == seed_count
        && duplicate_retries == seed_count
        && deletion_reservations == seed_count
        && restarted_nodes >= seed_count.saturating_mul(5);
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "publication authority gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "publication_process.exact_fresh_process_replay".to_owned(),
            status: if exact_replay {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: None,
        },
        HardGateResult {
            id: "publication_process.semantic_operations_exercised".to_owned(),
            status: if semantic_operations_exercised {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: Some(format!(
                "checks={check_count}, authority_starts={authority_process_starts}, kills={process_kills}, failovers={authority_failovers}, publication_writes={publication_writes}, generation_transitions={generation_transitions}, dropped_replies={dropped_replies}, recovered_outcomes={recovered_outcomes}, duplicate_retries={duplicate_retries}, delete_reservations={deletion_reservations}, restarted_nodes={restarted_nodes}"
            )),
        },
        HardGateResult {
            id: "publication_process.contract_agreement".to_owned(),
            status: if anomaly_count == 0 {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: mismatch_details.first().cloned(),
        },
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: id.clone(),
        status: if *passed {
            GateStatus::Pass
        } else {
            GateStatus::Fail
        },
        detail: None,
    }));

    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publication_process.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publication_process.failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "publication_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "publication_process.publication_writes".to_owned(),
                bounded_count(publication_writes),
            ),
            (
                "publication_process.restarted_nodes".to_owned(),
                bounded_count(restarted_nodes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_process(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft process workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_raft_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut committed_writes = 0_u64;
    let mut elections = 0_u64;
    let mut process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut dropped_replies = 0_u64;
    let mut duplicate_retries = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        committed_writes = committed_writes.saturating_add(first.committed_writes);
        elections = elections.saturating_add(first.elections);
        process_starts = process_starts.saturating_add(first.process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        dropped_replies = dropped_replies.saturating_add(first.dropped_replies);
        duplicate_retries = duplicate_retries.saturating_add(first.duplicate_retries);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-process-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_process" }),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "openraft-cell-v0"),
                    (
                        "result",
                        if exact {
                            "replay-deduplicated"
                        } else {
                            "rejected"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "lost-reply-process-failover-restart"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_counts = match mode {
        RaftProcessMode::Correct | RaftProcessMode::DisableDedup => (4, 1, 1, 1),
        RaftProcessMode::AcknowledgeBeforeQuorum => (6, 3, 0, 0),
        RaftProcessMode::SkipKilledNodeRestart => (3, 1, 1, 1),
    };
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(8)
        && process_starts == seed_count.saturating_mul(expected_counts.0)
        && process_kills == seed_count.saturating_mul(expected_counts.1)
        && dropped_replies == seed_count.saturating_mul(expected_counts.2)
        && duplicate_retries == seed_count.saturating_mul(expected_counts.3);
    let expected_success_path = mode != RaftProcessMode::Correct
        || (committed_writes == seed_count.saturating_mul(3)
            && elections == seed_count.saturating_mul(2)
            && recovered_outcomes == seed_count.saturating_mul(3)
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft process gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, commits={committed_writes}, elections={elections}, starts={process_starts}, kills={process_kills}, dropped_replies={dropped_replies}, retries={duplicate_retries}, recovered_outcomes={recovered_outcomes}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "raft_process.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("raft_process.checks".to_owned(), bounded_count(check_count)),
            (
                "raft_process.committed_writes".to_owned(),
                bounded_count(committed_writes),
            ),
            (
                "raft_process.elections".to_owned(),
                bounded_count(elections),
            ),
            (
                "raft_process.process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "raft_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "raft_process.dropped_replies".to_owned(),
                bounded_count(dropped_replies),
            ),
            (
                "raft_process.duplicate_retries".to_owned(),
                bounded_count(duplicate_retries),
            ),
            (
                "raft_process.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
            (
                "raft_process.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_htap_exactness_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "HTAP exactness workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => HtapContractMode::Correct,
        Some("pushdown_poison") => HtapContractMode::PushdownPoison,
        Some("schema_partition_move") => HtapContractMode::SchemaPartitionMove,
        Some("wal_pop_conflation") => HtapContractMode::WalPopConflation,
        Some("lease_gc_race") => HtapContractMode::LeaseGcRace,
        Some("certificate_toctou") => HtapContractMode::CertificateToctou,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown HTAP exactness negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut exact_checks = 0_u64;
    let mut tail_rows = 0_u64;
    let mut tail_bytes = 0_u64;
    let mut peak_memory_bytes = 0_u64;
    let mut spill_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_htap_contract(*seed, mode);
        let second = run_htap_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        exact_checks = exact_checks.saturating_add(first.exact_checks);
        tail_rows = tail_rows.saturating_add(first.tail_rows);
        tail_bytes = tail_bytes.saturating_add(first.tail_bytes);
        peak_memory_bytes = peak_memory_bytes.max(first.peak_memory_bytes);
        spill_bytes = spill_bytes.saturating_add(first.spill_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "okv-htap-row-oracle-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "htap_exactness" },
                    ),
                ]),
            },
            Measurement {
                metric: "query.result_exact",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("oracle", "okv-htap-row-oracle-v1"),
                ]),
            },
            Measurement {
                metric: "htap.tail_rows",
                value: bounded_count(first.tail_rows),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("base.format", "model-row"),
                ]),
            },
            Measurement {
                metric: "htap.tail_bytes",
                value: bounded_count(first.tail_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("base.format", "model-row"),
                ]),
            },
            Measurement {
                metric: "htap.peak_memory",
                value: bounded_count(first.peak_memory_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("merge.kind", "ordered-model"),
                ]),
            },
            Measurement {
                metric: "htap.spill_bytes",
                value: bounded_count(first.spill_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("merge.kind", "ordered-model"),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-htap-contract://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(6);
    let semantic_operations_exercised = event_count == expected_events
        && exact_checks > 0
        && tail_rows > 0
        && tail_bytes > 0
        && peak_memory_bytes > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "HTAP exactness gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "htap.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "htap.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, exact_checks={exact_checks}, tail_rows={tail_rows}, tail_bytes={tail_bytes}, peak_memory_bytes={peak_memory_bytes}, spill_bytes={spill_bytes}"
                )),
            },
            HardGateResult {
                id: "htap.exact_result".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("htap.contract.events".to_owned(), bounded_count(event_count)),
            (
                "htap.contract.exact_checks".to_owned(),
                bounded_count(exact_checks),
            ),
            ("htap.tail_rows".to_owned(), bounded_count(tail_rows)),
            ("htap.tail_bytes".to_owned(), bounded_count(tail_bytes)),
            (
                "htap.peak_memory_bytes".to_owned(),
                bounded_count(peak_memory_bytes),
            ),
            ("htap.spill_bytes".to_owned(), bounded_count(spill_bytes)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_htap_physical_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "HTAP physical workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PhysicalOverlayMode::Correct,
        Some("pushdown_before_invalidation") => PhysicalOverlayMode::PushdownBeforeInvalidation,
        Some("partition_local_reduction") => PhysicalOverlayMode::PartitionLocalReduction,
        Some("project_primary_key_early") => PhysicalOverlayMode::ProjectPrimaryKeyEarly,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown HTAP physical negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut base_rows = 0_u64;
    let mut tail_rows = 0_u64;
    let mut output_rows = 0_u64;
    let mut tail_bytes = 0_u64;
    let mut parquet_bytes = 0_u64;
    let mut materialized_bytes = 0_u64;
    let mut exact_replay = true;
    let mut parquet_round_trip = true;
    let mut arrow_tail_complete = true;
    let mut invalidation_precedes_filter = true;
    let mut partition_move_is_logical_identity = true;
    let mut hidden_primary_key_survives_projection = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_physical_overlay_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_physical_overlay_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_checks);
        base_rows = base_rows.saturating_add(first.base_rows);
        tail_rows = tail_rows.saturating_add(first.tail_rows);
        output_rows = output_rows.saturating_add(first.output_rows);
        tail_bytes = tail_bytes.saturating_add(first.tail_bytes);
        parquet_bytes = parquet_bytes.saturating_add(first.parquet_bytes);
        materialized_bytes = materialized_bytes.max(first.materialized_bytes);
        parquet_round_trip &= first.parquet_round_trip;
        arrow_tail_complete &= first.arrow_tail_complete;
        invalidation_precedes_filter &= first.invalidation_precedes_filter;
        partition_move_is_logical_identity &= first.partition_move_is_logical_identity;
        hidden_primary_key_survives_projection &= first.hidden_primary_key_survives_projection;
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "zebradb-datafusion-overlay-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "physical_overlay" },
                    ),
                ]),
            },
            Measurement {
                metric: "query.result_exact",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_base_plus_tail"),
                    ("backend", backend),
                    ("oracle", "zebradb-datafusion-overlay-v1"),
                ]),
            },
            Measurement {
                metric: "htap.tail_rows",
                value: bounded_count(first.tail_rows),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_base_plus_tail"),
                    ("backend", backend),
                    ("base.format", "parquet"),
                ]),
            },
            Measurement {
                metric: "htap.tail_bytes",
                value: bounded_count(first.tail_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_base_plus_tail"),
                    ("backend", backend),
                    ("base.format", "parquet"),
                ]),
            },
            Measurement {
                metric: "htap.peak_memory",
                value: bounded_count(first.materialized_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_base_plus_tail"),
                    ("backend", backend),
                    ("merge.kind", "materialized-correctness-adapter"),
                ]),
            },
            Measurement {
                metric: "htap.spill_bytes",
                value: 0.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_base_plus_tail"),
                    ("backend", backend),
                    ("merge.kind", "materialized-correctness-adapter"),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-htap-physical://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(4);
    let semantic_operations_exercised = event_count == expected_events
        && base_rows > 0
        && tail_rows > 0
        && output_rows > 0
        && tail_bytes > 0
        && parquet_bytes > 0
        && materialized_bytes > 0;
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && parquet_round_trip
        && arrow_tail_complete
        && invalidation_precedes_filter
        && partition_move_is_logical_identity
        && hidden_primary_key_survives_projection;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "HTAP physical gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "htap_physical.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "htap_physical.semantic_operations_exercised".to_owned(),
                status: gate_status(semantic_operations_exercised),
                detail: Some(format!(
                    "events={event_count}, base_rows={base_rows}, tail_rows={tail_rows}, output_rows={output_rows}, tail_bytes={tail_bytes}, parquet_bytes={parquet_bytes}, materialized_bytes={materialized_bytes}"
                )),
            },
            HardGateResult {
                id: "htap_physical.exact_result".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "htap_physical.parquet_round_trip".to_owned(),
                status: gate_status(parquet_round_trip),
                detail: None,
            },
            HardGateResult {
                id: "htap_physical.arrow_tail_complete".to_owned(),
                status: gate_status(arrow_tail_complete),
                detail: None,
            },
            HardGateResult {
                id: "htap_physical.invalidation_precedes_filter".to_owned(),
                status: gate_status(invalidation_precedes_filter),
                detail: None,
            },
            HardGateResult {
                id: "htap_physical.partition_move_is_logical_identity".to_owned(),
                status: gate_status(partition_move_is_logical_identity),
                detail: None,
            },
            HardGateResult {
                id: "htap_physical.hidden_primary_key_survives_projection".to_owned(),
                status: gate_status(hidden_primary_key_survives_projection),
                detail: None,
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("htap_physical.events".to_owned(), bounded_count(event_count)),
            ("htap_physical.base_rows".to_owned(), bounded_count(base_rows)),
            ("htap_physical.tail_rows".to_owned(), bounded_count(tail_rows)),
            (
                "htap_physical.output_rows".to_owned(),
                bounded_count(output_rows),
            ),
            ("htap_physical.tail_bytes".to_owned(), bounded_count(tail_bytes)),
            (
                "htap_physical.parquet_bytes".to_owned(),
                bounded_count(parquet_bytes),
            ),
            (
                "htap_physical.materialized_bytes".to_owned(),
                bounded_count(materialized_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_htap_streaming_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "HTAP streaming workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => StreamingOverlayMode::Correct,
        Some("materialize_inputs") => StreamingOverlayMode::MaterializeInputs,
        Some("reset_group_at_batch_boundary") => StreamingOverlayMode::ResetGroupAtBatchBoundary,
        Some("start_tail_at_max_watermark") => StreamingOverlayMode::StartTailAtMaximumWatermark,
        Some("rebase_continuation_target") => StreamingOverlayMode::RebaseContinuationTarget,
        Some("accept_unsorted_input") => StreamingOverlayMode::AcceptUnsortedInput,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown HTAP streaming negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut base_rows = 0_u64;
    let mut tail_rows = 0_u64;
    let mut output_rows = 0_u64;
    let mut input_batches = 0_u64;
    let mut output_batches = 0_u64;
    let mut tail_bytes = 0_u64;
    let mut parquet_bytes = 0_u64;
    let mut peak_buffered_rows = 0_u64;
    let mut peak_buffered_bytes = 0_u64;
    let mut maximum_group_rows = 0_u64;
    let mut materialized_input_rows = 0_u64;
    let mut spill_bytes = 0_u64;
    let mut exact_replay = true;
    let mut parquet_round_trip = true;
    let mut arrow_tail_complete = true;
    let mut incremental_emission = true;
    let mut input_order_validated = true;
    let mut batch_boundary_groups_preserved = true;
    let mut independent_watermarks = true;
    let mut continuation_target_bound = true;
    let mut buffer_bound_holds = true;
    let mut output_order_declared = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_streaming_overlay_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_streaming_overlay_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_checks);
        base_rows = base_rows.saturating_add(first.base_rows);
        tail_rows = tail_rows.saturating_add(first.tail_rows);
        output_rows = output_rows.saturating_add(first.output_rows);
        input_batches = input_batches.saturating_add(first.input_batches);
        output_batches = output_batches.saturating_add(first.output_batches);
        tail_bytes = tail_bytes.saturating_add(first.tail_bytes);
        parquet_bytes = parquet_bytes.saturating_add(first.parquet_bytes);
        peak_buffered_rows = peak_buffered_rows.max(first.peak_buffered_rows);
        peak_buffered_bytes = peak_buffered_bytes.max(first.peak_buffered_bytes);
        maximum_group_rows = maximum_group_rows.max(first.maximum_group_rows_observed);
        materialized_input_rows =
            materialized_input_rows.saturating_add(first.materialized_input_rows);
        spill_bytes = spill_bytes.saturating_add(first.spill_bytes);
        parquet_round_trip &= first.parquet_round_trip;
        arrow_tail_complete &= first.arrow_tail_complete;
        incremental_emission &= first.incremental_emission;
        input_order_validated &= first.input_order_validated;
        batch_boundary_groups_preserved &= first.batch_boundary_groups_preserved;
        independent_watermarks &= first.independent_watermarks;
        continuation_target_bound &= first.continuation_target_bound;
        buffer_bound_holds &= first.buffer_bound_holds;
        output_order_declared &= first.output_order_declared;
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        let merge_kind = if first.materialized_input_rows == 0 {
            "ordered-streaming"
        } else {
            "materialized-negative-control"
        };
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "zebradb-datafusion-streaming-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "streaming_overlay" },
                    ),
                ]),
            },
            Measurement {
                metric: "query.result_exact",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_streaming_base_plus_tail"),
                    ("backend", backend),
                    ("oracle", "zebradb-datafusion-streaming-v1"),
                ]),
            },
            Measurement {
                metric: "htap.tail_rows",
                value: bounded_count(first.tail_rows),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_streaming_base_plus_tail"),
                    ("backend", backend),
                    ("base.format", "parquet"),
                ]),
            },
            Measurement {
                metric: "htap.tail_bytes",
                value: bounded_count(first.tail_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_streaming_base_plus_tail"),
                    ("backend", backend),
                    ("base.format", "parquet"),
                ]),
            },
            Measurement {
                metric: "htap.peak_memory",
                value: bounded_count(first.peak_buffered_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_streaming_base_plus_tail"),
                    ("backend", backend),
                    ("merge.kind", merge_kind),
                ]),
            },
            Measurement {
                metric: "htap.spill_bytes",
                value: bounded_count(first.spill_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "datafusion_streaming_base_plus_tail"),
                    ("backend", backend),
                    ("merge.kind", merge_kind),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-htap-streaming://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(8);
    let semantic_operations_exercised = event_count == expected_events
        && base_rows > 0
        && tail_rows > 0
        && output_rows > 0
        && input_batches > 0
        && output_batches > 0
        && tail_bytes > 0
        && parquet_bytes > 0
        && peak_buffered_bytes > 0;
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && parquet_round_trip
        && arrow_tail_complete
        && incremental_emission
        && input_order_validated
        && batch_boundary_groups_preserved
        && independent_watermarks
        && continuation_target_bound
        && buffer_bound_holds
        && output_order_declared;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "HTAP streaming gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "htap_streaming.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.semantic_operations_exercised".to_owned(),
                status: gate_status(semantic_operations_exercised),
                detail: Some(format!(
                    "events={event_count}, base_rows={base_rows}, tail_rows={tail_rows}, output_rows={output_rows}, input_batches={input_batches}, output_batches={output_batches}, peak_buffered_rows={peak_buffered_rows}, peak_buffered_bytes={peak_buffered_bytes}, maximum_group_rows={maximum_group_rows}, materialized_rows={materialized_input_rows}"
                )),
            },
            HardGateResult {
                id: "htap_streaming.exact_result".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "htap_streaming.parquet_round_trip".to_owned(),
                status: gate_status(parquet_round_trip),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.arrow_tail_complete".to_owned(),
                status: gate_status(arrow_tail_complete),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.incremental_emission".to_owned(),
                status: gate_status(incremental_emission),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.input_order_validated".to_owned(),
                status: gate_status(input_order_validated),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.batch_boundary_groups_preserved".to_owned(),
                status: gate_status(batch_boundary_groups_preserved),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.independent_watermarks".to_owned(),
                status: gate_status(independent_watermarks),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.continuation_target_bound".to_owned(),
                status: gate_status(continuation_target_bound),
                detail: None,
            },
            HardGateResult {
                id: "htap_streaming.buffer_bound_holds".to_owned(),
                status: gate_status(buffer_bound_holds),
                detail: Some(format!(
                    "peak_rows={peak_buffered_rows}, peak_bytes={peak_buffered_bytes}, maximum_group_rows={maximum_group_rows}, materialized_rows={materialized_input_rows}"
                )),
            },
            HardGateResult {
                id: "htap_streaming.output_order_declared".to_owned(),
                status: gate_status(output_order_declared),
                detail: None,
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "htap_streaming.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "htap_streaming.base_rows".to_owned(),
                bounded_count(base_rows),
            ),
            (
                "htap_streaming.tail_rows".to_owned(),
                bounded_count(tail_rows),
            ),
            (
                "htap_streaming.output_rows".to_owned(),
                bounded_count(output_rows),
            ),
            (
                "htap_streaming.input_batches".to_owned(),
                bounded_count(input_batches),
            ),
            (
                "htap_streaming.output_batches".to_owned(),
                bounded_count(output_batches),
            ),
            (
                "htap_streaming.tail_bytes".to_owned(),
                bounded_count(tail_bytes),
            ),
            (
                "htap_streaming.parquet_bytes".to_owned(),
                bounded_count(parquet_bytes),
            ),
            (
                "htap_streaming.peak_buffered_rows".to_owned(),
                bounded_count(peak_buffered_rows),
            ),
            (
                "htap_streaming.peak_buffered_bytes".to_owned(),
                bounded_count(peak_buffered_bytes),
            ),
            (
                "htap_streaming.maximum_group_rows".to_owned(),
                bounded_count(maximum_group_rows),
            ),
            (
                "htap_streaming.materialized_input_rows".to_owned(),
                bounded_count(materialized_input_rows),
            ),
            (
                "htap_streaming.spill_bytes".to_owned(),
                bounded_count(spill_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_publication_adapter_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "object publication adapter workload requires at least one seed".to_owned(),
        ));
    }
    if backend != "object-store-local-fs+authority-quorum-fs" {
        return execution_from_result(Err(format!(
            "object publication adapter requires object-store-local-fs+authority-quorum-fs, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PublicationAdapterMode::Correct,
        Some("publish_root_before_verify") => PublicationAdapterMode::PublishRootBeforeVerify,
        Some("omit_durable_intent") => PublicationAdapterMode::OmitDurableIntent,
        Some("forget_unknown_object_outcome") => PublicationAdapterMode::ForgetUnknownObjectOutcome,
        Some("ram_only_authority") => PublicationAdapterMode::RamOnlyAuthority,
        Some("trust_list_for_liveness") => PublicationAdapterMode::TrustListForLiveness,
        Some("delete_without_revalidation") => PublicationAdapterMode::DeleteWithoutRevalidation,
        Some("delete_without_reservation") => PublicationAdapterMode::DeleteWithoutReservation,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown object publication adapter negative control {other}"
            )));
        }
    };

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return execution_from_result(Err(format!(
                "failed to create publication adapter runtime: {error}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut publication_intents = 0_u64;
    let mut published_roots = 0_u64;
    let mut authority_reopens = 0_u64;
    let mut authority_records = 0_u64;
    let mut unknown_object_outcomes = 0_u64;
    let mut unknown_authority_outcomes = 0_u64;
    let mut unknown_delete_outcomes = 0_u64;
    let mut complete_marks = 0_u64;
    let mut incomplete_marks = 0_u64;
    let mut deferred_deletes = 0_u64;
    let mut delete_reservations = 0_u64;
    let mut blocked_publications = 0_u64;
    let mut reclaimed_objects = 0_u64;
    let mut recreated_objects = 0_u64;
    let mut object_requests = 0_u64;
    let mut object_bytes_written = 0_u64;
    let mut object_bytes_read = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first_root = std::env::temp_dir().join(format!(
            "okv-publication-adapter-eval-first-{seed}-{}",
            Uuid::new_v4()
        ));
        let second_root = std::env::temp_dir().join(format!(
            "okv-publication-adapter-eval-second-{seed}-{}",
            Uuid::new_v4()
        ));
        let first = runtime.block_on(run_publication_adapter_contract(&first_root, *seed, mode));
        let second = runtime.block_on(run_publication_adapter_contract(&second_root, *seed, mode));
        let _ = fs::remove_dir_all(&first_root);
        let _ = fs::remove_dir_all(&second_root);

        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        publication_intents = publication_intents.saturating_add(first.publication_intents);
        published_roots = published_roots.saturating_add(first.published_roots);
        authority_reopens = authority_reopens.saturating_add(first.authority_reopens);
        authority_records = authority_records.saturating_add(first.authority_records);
        unknown_object_outcomes =
            unknown_object_outcomes.saturating_add(first.verified_unknown_object_outcomes);
        unknown_authority_outcomes =
            unknown_authority_outcomes.saturating_add(first.verified_unknown_authority_outcomes);
        unknown_delete_outcomes =
            unknown_delete_outcomes.saturating_add(first.verified_unknown_delete_outcomes);
        complete_marks = complete_marks.saturating_add(first.complete_marks);
        incomplete_marks = incomplete_marks.saturating_add(first.incomplete_marks);
        deferred_deletes = deferred_deletes.saturating_add(first.deferred_deletes);
        delete_reservations = delete_reservations.saturating_add(first.delete_reservations);
        blocked_publications = blocked_publications.saturating_add(first.blocked_publications);
        reclaimed_objects = reclaimed_objects.saturating_add(first.reclaimed_objects);
        recreated_objects = recreated_objects.saturating_add(first.recreated_objects);
        object_requests = object_requests.saturating_add(first.object_requests);
        object_bytes_written = object_bytes_written.saturating_add(first.object_bytes_written);
        object_bytes_read = object_bytes_read.saturating_add(first.object_bytes_read);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "object-publication-adapter-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "publication_adapter" },
                    ),
                ]),
            },
            Measurement {
                metric: "object_store.requests",
                value: bounded_count(first.object_requests),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("api", "publication-adapter"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "object_store.bytes",
                value: bounded_count(first.object_bytes_written),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("direction", "write"),
                    ("api", "publication-adapter"),
                ]),
            },
            Measurement {
                metric: "object_store.bytes",
                value: bounded_count(first.object_bytes_read),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("direction", "read"),
                    ("api", "publication-adapter"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publish-mark-reserve-delete"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publication-adapter://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_checks = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(16);
    let physical_boundaries_exercised = check_count == expected_checks
        && publication_intents > 0
        && published_roots > 0
        && authority_reopens > 0
        && complete_marks > 0
        && incomplete_marks > 0
        && object_requests > 0
        && object_bytes_written > 0
        && object_bytes_read > 0;
    let passed = anomaly_count == 0 && exact_replay && physical_boundaries_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "object publication adapter gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, physical_boundaries_exercised={physical_boundaries_exercised}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "publication_adapter.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "publication_adapter.physical_boundaries_exercised".to_owned(),
                status: gate_status(physical_boundaries_exercised),
                detail: Some(format!(
                    "checks={check_count}, intents={publication_intents}, roots={published_roots}, authority_reopens={authority_reopens}, authority_records={authority_records}, object_unknown={unknown_object_outcomes}, authority_unknown={unknown_authority_outcomes}, delete_unknown={unknown_delete_outcomes}, complete_marks={complete_marks}, incomplete_marks={incomplete_marks}, deferred={deferred_deletes}, reservations={delete_reservations}, blocked={blocked_publications}, reclaimed={reclaimed_objects}, recreated={recreated_objects}, requests={object_requests}"
                )),
            },
            HardGateResult {
                id: "publication_adapter.contract_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "publication_adapter.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "publication_adapter.publication_intents".to_owned(),
                bounded_count(publication_intents),
            ),
            (
                "publication_adapter.published_roots".to_owned(),
                bounded_count(published_roots),
            ),
            (
                "publication_adapter.authority_reopens".to_owned(),
                bounded_count(authority_reopens),
            ),
            (
                "publication_adapter.authority_records".to_owned(),
                bounded_count(authority_records),
            ),
            (
                "publication_adapter.verified_unknown_object_outcomes".to_owned(),
                bounded_count(unknown_object_outcomes),
            ),
            (
                "publication_adapter.verified_unknown_authority_outcomes".to_owned(),
                bounded_count(unknown_authority_outcomes),
            ),
            (
                "publication_adapter.verified_unknown_delete_outcomes".to_owned(),
                bounded_count(unknown_delete_outcomes),
            ),
            (
                "publication_adapter.complete_marks".to_owned(),
                bounded_count(complete_marks),
            ),
            (
                "publication_adapter.incomplete_marks".to_owned(),
                bounded_count(incomplete_marks),
            ),
            (
                "publication_adapter.deferred_deletes".to_owned(),
                bounded_count(deferred_deletes),
            ),
            (
                "publication_adapter.delete_reservations".to_owned(),
                bounded_count(delete_reservations),
            ),
            (
                "publication_adapter.blocked_publications".to_owned(),
                bounded_count(blocked_publications),
            ),
            (
                "publication_adapter.reclaimed_objects".to_owned(),
                bounded_count(reclaimed_objects),
            ),
            (
                "publication_adapter.recreated_objects".to_owned(),
                bounded_count(recreated_objects),
            ),
            (
                "publication_adapter.object_requests".to_owned(),
                bounded_count(object_requests),
            ),
            (
                "publication_adapter.object_bytes_written".to_owned(),
                bounded_count(object_bytes_written),
            ),
            (
                "publication_adapter.object_bytes_read".to_owned(),
                bounded_count(object_bytes_read),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_publication_gc_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "object publication and GC workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PublicationGcMode::Correct,
        Some("publish_pointer_before_blocks") => PublicationGcMode::PublishPointerBeforeBlocks,
        Some("omit_publication_intent") => PublicationGcMode::OmitPublicationIntent,
        Some("trust_accounting_counter") => PublicationGcMode::TrustAccountingCounter,
        Some("trust_list_for_liveness") => PublicationGcMode::TrustListForLiveness,
        Some("continue_incomplete_mark") => PublicationGcMode::ContinueIncompleteMark,
        Some("delete_without_revalidation") => PublicationGcMode::DeleteWithoutRevalidation,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown object publication and GC negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut exact_checks = 0_u64;
    let mut publication_intents = 0_u64;
    let mut published_roots = 0_u64;
    let mut verified_unknown_outcomes = 0_u64;
    let mut complete_marks = 0_u64;
    let mut incomplete_marks = 0_u64;
    let mut drifted_counters = 0_u64;
    let mut stale_list_observations = 0_u64;
    let mut deferred_deletes = 0_u64;
    let mut reclaimed_objects = 0_u64;
    let mut object_requests = 0_u64;
    let mut object_bytes_written = 0_u64;
    let mut physical_bytes = 0_u64;
    let mut live_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = run_publication_gc_contract(*seed, mode);
        let second = run_publication_gc_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        exact_checks = exact_checks.saturating_add(first.exact_checks);
        publication_intents = publication_intents.saturating_add(first.publication_intents);
        published_roots = published_roots.saturating_add(first.published_roots);
        verified_unknown_outcomes =
            verified_unknown_outcomes.saturating_add(first.verified_unknown_outcomes);
        complete_marks = complete_marks.saturating_add(first.complete_marks);
        incomplete_marks = incomplete_marks.saturating_add(first.incomplete_marks);
        drifted_counters = drifted_counters.saturating_add(first.drifted_counters);
        stale_list_observations =
            stale_list_observations.saturating_add(first.stale_list_observations);
        deferred_deletes = deferred_deletes.saturating_add(first.deferred_deletes);
        reclaimed_objects = reclaimed_objects.saturating_add(first.reclaimed_objects);
        object_requests = object_requests.saturating_add(first.object_requests);
        object_bytes_written = object_bytes_written.saturating_add(first.object_bytes_written);
        physical_bytes = physical_bytes.saturating_add(first.physical_bytes);
        live_bytes = live_bytes.saturating_add(first.live_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        let amplification = if first.live_bytes == 0 {
            bounded_count(first.physical_bytes)
        } else {
            bounded_count(first.physical_bytes) / bounded_count(first.live_bytes)
        };
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "object-publication-gc-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "publication_gc" },
                    ),
                ]),
            },
            Measurement {
                metric: "object_store.requests",
                value: bounded_count(first.object_requests),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "deterministic-model"),
                    ("api", "publication-gc"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "object_store.bytes",
                value: bounded_count(first.object_bytes_written),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "deterministic-model"),
                    ("direction", "write"),
                    ("api", "publication-gc"),
                ]),
            },
            Measurement {
                metric: "storage.amplification",
                value: amplification,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("format", "content-addressed-model"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "publish-mark-sweep"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publication-gc://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(7);
    let semantic_operations_exercised = event_count == expected_events
        && exact_checks == expected_events
        && publication_intents > 0
        && published_roots > 0
        && verified_unknown_outcomes > 0
        && complete_marks > 0
        && incomplete_marks > 0
        && drifted_counters > 0
        && stale_list_observations > 0
        && object_requests > 0
        && object_bytes_written > 0
        && physical_bytes > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "object publication and GC gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "publication_gc.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "publication_gc.semantic_operations_exercised".to_owned(),
                status: gate_status(semantic_operations_exercised),
                detail: Some(format!(
                    "events={event_count}, intents={publication_intents}, roots={published_roots}, unknown_outcomes={verified_unknown_outcomes}, complete_marks={complete_marks}, incomplete_marks={incomplete_marks}, drifted_counters={drifted_counters}, stale_lists={stale_list_observations}, deferred={deferred_deletes}, reclaimed={reclaimed_objects}, requests={object_requests}"
                )),
            },
            HardGateResult {
                id: "publication_gc.contract_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("publication_gc.events".to_owned(), bounded_count(event_count)),
            (
                "publication_gc.intents".to_owned(),
                bounded_count(publication_intents),
            ),
            (
                "publication_gc.published_roots".to_owned(),
                bounded_count(published_roots),
            ),
            (
                "publication_gc.verified_unknown_outcomes".to_owned(),
                bounded_count(verified_unknown_outcomes),
            ),
            (
                "publication_gc.complete_marks".to_owned(),
                bounded_count(complete_marks),
            ),
            (
                "publication_gc.incomplete_marks".to_owned(),
                bounded_count(incomplete_marks),
            ),
            (
                "publication_gc.deferred_deletes".to_owned(),
                bounded_count(deferred_deletes),
            ),
            (
                "publication_gc.reclaimed_objects".to_owned(),
                bounded_count(reclaimed_objects),
            ),
            (
                "publication_gc.object_requests".to_owned(),
                bounded_count(object_requests),
            ),
            (
                "publication_gc.object_bytes_written".to_owned(),
                bounded_count(object_bytes_written),
            ),
            (
                "publication_gc.physical_bytes".to_owned(),
                bounded_count(physical_bytes),
            ),
            (
                "publication_gc.live_bytes".to_owned(),
                bounded_count(live_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_staged_txlog_protocol(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "staged txLog protocol workload requires at least one seed".to_owned(),
        ));
    }
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match negative_control {
        None => StagedTxLogMode::Correct,
        Some("ack_one_copy") => StagedTxLogMode::AckOneCopy,
        Some("accept_stale_epoch") => StagedTxLogMode::AcceptStaleEpoch,
        Some("overwrite_acknowledged_suffix") => StagedTxLogMode::OverwriteAcknowledgedSuffix,
        Some("publish_uncommitted_segment") => StagedTxLogMode::PublishUncommittedSegment,
        Some("trust_object_list") => StagedTxLogMode::TrustObjectList,
        Some("unbounded_queue") => StagedTxLogMode::UnboundedQueue,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown staged txLog negative control {other}"
            )));
        }
    };
    let subject = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str);
    if mode == StagedTxLogMode::Correct && subject != Some("protocol-model") {
        return execution_from_result(Err(format!(
            "staged txLog subject {} is not code complete; only the L0 protocol-model subject is runnable",
            subject.unwrap_or("missing")
        )));
    }
    if backend != "sim-model" {
        return execution_from_result(Err(format!(
            "staged txLog L0 requires backend sim-model, got {backend}"
        )));
    }

    let mut reports = Vec::new();
    let mut exact_replay = true;
    for seed in seeds {
        let first = run_staged_txlog_contract(*seed, mode);
        let second = run_staged_txlog_contract(*seed, mode);
        exact_replay &= first == second;
        reports.push(first);
    }

    let anomaly_count = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let event_count = reports
        .iter()
        .map(|report| report.executed_steps)
        .sum::<u64>();
    let acknowledged_appends = reports
        .iter()
        .map(|report| report.acknowledged_appends)
        .sum::<u64>();
    let recovered_unknown_outcomes = reports
        .iter()
        .map(|report| report.recovered_unknown_outcomes)
        .sum::<u64>();
    let writer_takeovers = reports
        .iter()
        .map(|report| report.writer_takeovers)
        .sum::<u64>();
    let repaired_records = reports
        .iter()
        .map(|report| report.repaired_records)
        .sum::<u64>();
    let stale_writer_rejections = reports
        .iter()
        .map(|report| report.stale_writer_rejections)
        .sum::<u64>();
    let conflicting_retries_rejected = reports
        .iter()
        .map(|report| report.conflicting_retries_rejected)
        .sum::<u64>();
    let published_segments = reports
        .iter()
        .map(|report| report.published_segments)
        .sum::<u64>();
    let orphan_objects_ignored = reports
        .iter()
        .map(|report| report.orphan_objects_ignored)
        .sum::<u64>();
    let bounded_queue_rejections = reports
        .iter()
        .map(|report| report.bounded_queue_rejections)
        .sum::<u64>();
    let candidate_semantics_exercised = acknowledged_appends > 0
        && recovered_unknown_outcomes > 0
        && writer_takeovers > 0
        && repaired_records > 0
        && stale_writer_rejections > 0
        && conflicting_retries_rejected > 0
        && published_segments > 0
        && orphan_objects_ignored > 0
        && bounded_queue_rejections > 0;
    let poison_detected = mode != StagedTxLogMode::Correct
        && reports.iter().all(|report| {
            report.anomaly_count == 1
                && report.first_mismatch_step.is_some()
                && report.first_mismatch.is_some()
        });
    let candidate_passed = mode == StagedTxLogMode::Correct
        && anomaly_count == 0
        && exact_replay
        && candidate_semantics_exercised;
    let error = if mode == StagedTxLogMode::Correct {
        (!candidate_passed).then(|| {
            let detail = reports
                .iter()
                .find_map(|report| report.first_mismatch.clone())
                .unwrap_or_else(|| "no mismatch detail".to_owned());
            format!(
                "staged txLog protocol gate failed: anomalies={anomaly_count}, exact_replay={exact_replay}, candidate_semantics={candidate_semantics_exercised}; {detail}"
            )
        })
    } else {
        Some(if poison_detected {
            format!("staged txLog negative control {} was detected", mode.id())
        } else {
            format!(
                "staged txLog negative control {} escaped detection",
                mode.id()
            )
        })
    };
    let measurements = reports
        .iter()
        .flat_map(|report| {
            [
                Measurement {
                    metric: "correctness.anomalies",
                    value: bounded_count(report.anomaly_count),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "staged-txlog-protocol-v1"),
                        (
                            "anomaly.class",
                            negative_control.unwrap_or(if report.anomaly_count == 0 {
                                "none"
                            } else {
                                "candidate"
                            }),
                        ),
                    ]),
                },
                Measurement {
                    metric: "availability.success_ratio",
                    value: if report.anomaly_count == 0 { 1.0 } else { 0.0 },
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "staged-txlog-protocol"),
                        ("fault", mode.id()),
                        ("backend", backend),
                    ]),
                },
            ]
        })
        .collect::<Vec<_>>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "staged_txlog.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog.candidate_semantics_exercised".to_owned(),
                status: gate_status(
                    mode != StagedTxLogMode::Correct || candidate_semantics_exercised,
                ),
                detail: Some(format!(
                    "acks={acknowledged_appends}, unknowns={recovered_unknown_outcomes}, takeovers={writer_takeovers}, repairs={repaired_records}, stale_rejections={stale_writer_rejections}, retry_rejections={conflicting_retries_rejected}, segments={published_segments}, ignored_orphans={orphan_objects_ignored}, queue_rejections={bounded_queue_rejections}"
                )),
            },
            HardGateResult {
                id: "staged_txlog.contract_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: reports
                    .iter()
                    .find_map(|report| report.first_mismatch.clone()),
            },
            HardGateResult {
                id: "staged_txlog.negative_control_detected".to_owned(),
                status: gate_status(mode == StagedTxLogMode::Correct || poison_detected),
                detail: negative_control.map(|control| format!("control={control}")),
            },
            HardGateResult {
                id: "staged_txlog.l0_scope_only".to_owned(),
                status: GateStatus::Pass,
                detail: Some(
                    "deterministic protocol model; no process, media, latency, or transaction-commit claim"
                        .to_owned(),
                ),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-staged-txlog://{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "staged_txlog.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "staged_txlog.acknowledged_appends".to_owned(),
                bounded_count(acknowledged_appends),
            ),
            (
                "staged_txlog.recovered_unknown_outcomes".to_owned(),
                bounded_count(recovered_unknown_outcomes),
            ),
            (
                "staged_txlog.writer_takeovers".to_owned(),
                bounded_count(writer_takeovers),
            ),
            (
                "staged_txlog.repaired_records".to_owned(),
                bounded_count(repaired_records),
            ),
            (
                "staged_txlog.stale_writer_rejections".to_owned(),
                bounded_count(stale_writer_rejections),
            ),
            (
                "staged_txlog.conflicting_retries_rejected".to_owned(),
                bounded_count(conflicting_retries_rejected),
            ),
            (
                "staged_txlog.published_segments".to_owned(),
                bounded_count(published_segments),
            ),
            (
                "staged_txlog.orphan_objects_ignored".to_owned(),
                bounded_count(orphan_objects_ignored),
            ),
            (
                "staged_txlog.bounded_queue_rejections".to_owned(),
                bounded_count(bounded_queue_rejections),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_staged_txlog_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "staged txLog process workload requires at least one seed".to_owned(),
        ));
    }
    if backend != "local-process" {
        return execution_from_result(Err(format!(
            "staged txLog L1 requires backend local-process, got {backend}"
        )));
    }
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match negative_control {
        None => StagedTxLogProcessMode::Correct,
        Some("ack_before_sync") => StagedTxLogProcessMode::AckBeforeSync,
        Some("accept_stale_epoch") => StagedTxLogProcessMode::AcceptStaleEpoch,
        Some("node_specific_segment_bytes") => StagedTxLogProcessMode::NodeSpecificSegmentBytes,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown staged txLog process negative control {other}"
            )));
        }
    };
    let subject = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str);
    if mode == StagedTxLogProcessMode::Correct && subject != Some("three-process-quorum-nvme") {
        return execution_from_result(Err(format!(
            "staged txLog L1 subject must be three-process-quorum-nvme, got {}",
            subject.unwrap_or("missing")
        )));
    }
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_staged_txlog_process_contract(*seed, mode, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => {
                return execution_from_result(Err(format!(
                    "staged txLog L1 seed {seed} could not execute: {error}"
                )));
            }
        }
    }

    let anomaly_count = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let process_starts = reports
        .iter()
        .map(|report| report.process_starts)
        .sum::<u64>();
    let process_kills = reports
        .iter()
        .map(|report| report.process_kills)
        .sum::<u64>();
    let acknowledged_appends = reports
        .iter()
        .map(|report| report.acknowledged_appends)
        .sum::<u64>();
    let network_append_requests = reports
        .iter()
        .map(|report| report.network_append_requests)
        .sum::<u64>();
    let acknowledged_record_loss = reports
        .iter()
        .map(|report| report.acknowledged_record_loss)
        .sum::<u64>();
    let object_operations = reports
        .iter()
        .map(|report| report.object_operations)
        .sum::<u64>();
    let topology_exact = reports.iter().all(|report| {
        report.node_count == 3
            && report.write_quorum == 2
            && report.distinct_roots == 3
            && report.distinct_listeners == 3
            && report
                .initial_process_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3
            && report
                .restart_process_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3
            && report.process_starts == 6
            && report.process_kills == 6
    });
    let durable_quorum = reports.iter().all(|report| {
        report.append_samples.len() >= 3
            && report
                .append_samples
                .iter()
                .all(|sample| sample.durable_acknowledgements >= 2)
    });
    let exact_retry = reports
        .iter()
        .all(|report| report.exact_retry_no_physical_effect);
    let recovery_exact = reports.iter().all(|report| {
        report.acknowledged_record_loss == 0
            && report.consecutive_recovery
            && report.exact_prefix_nodes >= 2
    });
    let torn_repair_exact = reports.iter().all(|report| report.torn_tail_repairs == 1);
    let stale_writer_fenced = reports
        .iter()
        .all(|report| report.stale_writer_rejections == 3 && report.stale_writer_mutations == 0);
    let segment_identity_exact = reports
        .iter()
        .all(|report| report.segment_bytes_identical && report.segment_digests.len() == 3);
    let no_object_operations = object_operations == 0;
    let bounded_physical_bytes = reports.iter().all(|report| report.bounded_physical_bytes);
    let poison_detected = mode != StagedTxLogProcessMode::Correct
        && reports
            .iter()
            .all(|report| report.anomaly_count == 1 && report.first_mismatch.is_some());
    let candidate_passed = mode == StagedTxLogProcessMode::Correct
        && anomaly_count == 0
        && topology_exact
        && durable_quorum
        && exact_retry
        && recovery_exact
        && torn_repair_exact
        && stale_writer_fenced
        && segment_identity_exact
        && no_object_operations
        && bounded_physical_bytes;
    let error = if mode == StagedTxLogProcessMode::Correct {
        (!candidate_passed).then(|| {
            format!(
                "staged txLog L1 gate failed: anomalies={anomaly_count}, topology={topology_exact}, quorum={durable_quorum}, retry={exact_retry}, recovery={recovery_exact}, torn_repair={torn_repair_exact}, fencing={stale_writer_fenced}, segment_identity={segment_identity_exact}, no_objects={no_object_operations}, bounded={bounded_physical_bytes}; {}",
                reports
                    .iter()
                    .find_map(|report| report.first_mismatch.clone())
                    .unwrap_or_else(|| "no mismatch detail".to_owned())
            )
        })
    } else {
        Some(if poison_detected {
            format!(
                "staged txLog L1 negative control {} was detected",
                mode.id()
            )
        } else {
            format!(
                "staged txLog L1 negative control {} escaped detection",
                mode.id()
            )
        })
    };

    let mut measurements = Vec::new();
    for report in &reports {
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(report.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "staged-txlog-process-l1-v1"),
                (
                    "anomaly.class",
                    negative_control.unwrap_or(if report.anomaly_count == 0 {
                        "none"
                    } else {
                        "candidate"
                    }),
                ),
            ]),
        });
        measurements.push(Measurement {
            metric: "availability.success_ratio",
            value: if report.anomaly_count == 0 { 1.0 } else { 0.0 },
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("operation", "staged-txlog-process-l1"),
                ("fault", mode.id()),
                ("backend", backend),
            ]),
        });
        for sample in &report.append_samples {
            let record_class = format!("{}B", sample.payload_bytes);
            let result = if sample.durable_acknowledgements >= 2 {
                "quorum"
            } else {
                "failed"
            };
            measurements.push(Measurement {
                metric: "log.append.ack_duration",
                value: sample.quorum_duration_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("durability", "quorum-nvme-local-process"),
                    ("record.class", &record_class),
                    ("load.class", "l1-diagnostic"),
                    ("result", result),
                ]),
            });
        }
        measurements.push(Measurement {
            metric: "log.staging.bytes",
            value: bounded_count(report.max_node_physical_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("tier", "local-journal"),
                ("sample", "maximum"),
            ]),
        });
        for bytes in &report.segment_bytes {
            measurements.push(Measurement {
                metric: "log.segment.bytes",
                value: bounded_count(*bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("segment.class", "l1-preview"),
                    ("partial", "false"),
                ]),
            });
        }
    }

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "staged_txlog_process.distinct_process_topology".to_owned(),
                status: gate_status(topology_exact),
                detail: Some(format!(
                    "starts={process_starts}, kills={process_kills}, seeds={}",
                    seeds.len()
                )),
            },
            HardGateResult {
                id: "staged_txlog_process.durable_write_quorum".to_owned(),
                status: gate_status(durable_quorum),
                detail: Some(format!(
                    "acknowledged_appends={acknowledged_appends}, network_append_requests={network_append_requests}"
                )),
            },
            HardGateResult {
                id: "staged_txlog_process.exact_retry_no_physical_effect".to_owned(),
                status: gate_status(exact_retry),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog_process.restart_recovery".to_owned(),
                status: gate_status(recovery_exact),
                detail: Some(format!(
                    "acknowledged_record_loss={acknowledged_record_loss}"
                )),
            },
            HardGateResult {
                id: "staged_txlog_process.torn_tail_repair".to_owned(),
                status: gate_status(torn_repair_exact),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog_process.stale_writer_fenced".to_owned(),
                status: gate_status(stale_writer_fenced),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog_process.segment_identity".to_owned(),
                status: gate_status(segment_identity_exact),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog_process.no_object_operations".to_owned(),
                status: gate_status(no_object_operations),
                detail: Some(format!("object_operations={object_operations}")),
            },
            HardGateResult {
                id: "staged_txlog_process.bounded_physical_bytes".to_owned(),
                status: gate_status(bounded_physical_bytes),
                detail: None,
            },
            HardGateResult {
                id: "staged_txlog_process.negative_control_detected".to_owned(),
                status: gate_status(mode == StagedTxLogProcessMode::Correct || poison_detected),
                detail: negative_control.map(|control| format!("control={control}")),
            },
            HardGateResult {
                id: "staged_txlog_process.l1_scope_only".to_owned(),
                status: GateStatus::Pass,
                detail: Some(
                    "one-host process mechanism; append timing is diagnostic and does not claim independent-machine latency"
                        .to_owned(),
                ),
            },
        ],
        budget_units: bounded_count(
            reports
                .iter()
                .map(|report| report.executed_checks)
                .sum::<u64>(),
        ),
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-staged-txlog-process://{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "staged_txlog_process.process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "staged_txlog_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "staged_txlog_process.acknowledged_appends".to_owned(),
                bounded_count(acknowledged_appends),
            ),
            (
                "staged_txlog_process.network_append_requests".to_owned(),
                bounded_count(network_append_requests),
            ),
            (
                "staged_txlog_process.acknowledged_record_loss".to_owned(),
                bounded_count(acknowledged_record_loss),
            ),
            (
                "staged_txlog_process.object_operations".to_owned(),
                bounded_count(object_operations),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_commit_envelope_contract(workload: &WorkloadConfig, seeds: &[u64]) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "commit envelope workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => CommitContractMode::Correct,
        Some("ram_only_dedup") => CommitContractMode::RamOnlyDedup,
        Some("accept_conflicting_retry") => CommitContractMode::AcceptConflictingRetry,
        Some("accept_partial_resolver") => CommitContractMode::AcceptPartialResolver,
        Some("omit_required_log_tag") => CommitContractMode::OmitRequiredLogTag,
        Some("accept_stale_generation") => CommitContractMode::AcceptStaleGeneration,
        Some("ack_before_quorum") => CommitContractMode::AckBeforeQuorum,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown commit contract negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut acknowledged_commits = 0_u64;
    let mut recovered_commits = 0_u64;
    let mut retry_count = 0_u64;
    let mut leader_only_attempts = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_commit_contract(*seed, mode);
        let second = run_commit_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        acknowledged_commits = acknowledged_commits.saturating_add(first.acknowledged_commits);
        recovered_commits = recovered_commits.saturating_add(first.recovered_commits);
        retry_count = retry_count.saturating_add(first.retry_count);
        leader_only_attempts = leader_only_attempts.saturating_add(first.leader_only_attempts);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "okv-cell-commit-contract-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        "commit_contract"
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-commit-contract://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let semantic_operations_exercised = acknowledged_commits > 0
        && recovered_commits > 0
        && retry_count > 0
        && leader_only_attempts > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "commit contract gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "commit.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "commit.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "acknowledged={acknowledged_commits}, recovered={recovered_commits}, retries={retry_count}, leader_only={leader_only_attempts}"
                )),
            },
            HardGateResult {
                id: "commit.contract_agreement".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "commit.contract.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "commit.contract.acknowledged".to_owned(),
                bounded_count(acknowledged_commits),
            ),
            (
                "commit.contract.recovered".to_owned(),
                bounded_count(recovered_commits),
            ),
            (
                "commit.contract.retries".to_owned(),
                bounded_count(retry_count),
            ),
            (
                "commit.contract.leader_only_attempts".to_owned(),
                bounded_count(leader_only_attempts),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_strict_serializability_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "strict-serializability workload requires at least one seed".to_owned(),
        ));
    }
    let transactions = workload
        .parameters
        .get("transactions")
        .and_then(toml::Value::as_integer)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1_000);
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => SerializabilityMode::Correct,
        Some("accept_point_conflict") => SerializabilityMode::AcceptPointConflict,
        Some("accept_range_phantom") => SerializabilityMode::AcceptRangePhantom,
        Some("partial_commit") => SerializabilityMode::PartialCommit,
        Some("omit_read_conflict") => SerializabilityMode::OmitReadConflict,
        Some("omit_write_conflict") => SerializabilityMode::OmitWriteConflict,
        Some("stale_read_version") => SerializabilityMode::StaleReadVersion,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown strict-serializability negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut transaction_count = 0_u64;
    let mut committed_count = 0_u64;
    let mut aborted_count = 0_u64;
    let mut multi_range_count = 0_u64;
    let mut point_read_count = 0_u64;
    let mut range_read_count = 0_u64;
    let mut exact_replay = true;
    let mut first_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first_history = run_serializability_history(*seed, transactions, mode);
        let second_history = run_serializability_history(*seed, transactions, mode);
        exact_replay &= first_history == second_history;
        let report = check_history(&first_history);
        let second_report = check_history(&second_history);
        exact_replay &= report == second_report;
        let per_seed_anomalies = u64::try_from(report.anomalies.len()).unwrap_or(u64::MAX);
        anomaly_count = anomaly_count.saturating_add(per_seed_anomalies);
        transaction_count = transaction_count.saturating_add(report.transaction_count);
        committed_count = committed_count.saturating_add(report.committed_count);
        aborted_count = aborted_count.saturating_add(report.aborted_count);
        multi_range_count = multi_range_count.saturating_add(report.multi_range_committed_count);
        point_read_count = point_read_count.saturating_add(report.point_read_count);
        range_read_count = range_read_count.saturating_add(report.range_read_count);
        if let Some(anomaly) = report.anomalies.first() {
            first_details.push(format!(
                "seed {seed}, class {:?}, transaction {:?}: {}",
                anomaly.class, anomaly.transaction, anomaly.detail
            ));
        }
        let encoded = serde_json::to_vec(&first_history).unwrap_or_default();
        artifact_refs.push(format!(
            "okv-serializability-history://{}/{seed}/{transactions}/{}",
            mode.id(),
            sha256(&encoded)
        ));
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(per_seed_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "okv-independent-occ-history-v1"),
                (
                    "anomaly.class",
                    if per_seed_anomalies == 0 {
                        "none"
                    } else {
                        "strict_serializability"
                    },
                ),
            ]),
        });
    }

    let expected_transactions = u64::try_from(transactions)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(seeds.len()).unwrap_or(u64::MAX));
    let product_shape_exercised = transaction_count == expected_transactions
        && committed_count > 0
        && aborted_count > 0
        && multi_range_count > 0
        && point_read_count > 0
        && range_read_count > 0;
    let passed = anomaly_count == 0 && exact_replay && product_shape_exercised;
    let error = (!passed).then(|| {
        let detail = first_details
            .first()
            .cloned()
            .unwrap_or_else(|| "no anomaly detail".to_owned());
        format!(
            "strict-serializability gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, product_shape={product_shape_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "serializability.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "serializability.product_shape_exercised".to_owned(),
                status: gate_status(product_shape_exercised),
                detail: Some(format!(
                    "transactions={transaction_count}, committed={committed_count}, aborted={aborted_count}, multi_range_committed={multi_range_count}, point_reads={point_read_count}, range_reads={range_read_count}"
                )),
            },
            HardGateResult {
                id: "serializability.oracle_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: first_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(transaction_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("serializability.transactions".to_owned(), bounded_count(transaction_count)),
            ("serializability.committed".to_owned(), bounded_count(committed_count)),
            ("serializability.aborted".to_owned(), bounded_count(aborted_count)),
            (
                "serializability.multi_range_committed".to_owned(),
                bounded_count(multi_range_count),
            ),
            ("serializability.point_reads".to_owned(), bounded_count(point_read_count)),
            ("serializability.range_reads".to_owned(), bounded_count(range_read_count)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_transaction_process_serializability(
    workload: &WorkloadConfig,
    seeds: &[u64],
    _backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "transaction process workload requires at least one seed".to_owned(),
        ));
    }
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => TransactionProcessMode::Correct,
        Some("accept_conflicts") => TransactionProcessMode::AcceptConflicts,
        Some("partial_apply") => TransactionProcessMode::PartialApply,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown transaction process negative control {other}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let machine_config = match transaction_machine_config(workload) {
        Ok(config) => config,
        Err(error) => return execution_from_result(Err(error)),
    };
    let machine_mode = machine_config.is_some();
    let machine_artifact_dir = match transaction_machine_artifact_dir(workload, machine_mode) {
        Ok(path) => path,
        Err(error) => return execution_from_result(Err(error)),
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut elections = 0_u64;
    let mut dropped_replies = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut committed_count = 0_u64;
    let mut aborted_count = 0_u64;
    let mut multi_range_count = 0_u64;
    let mut point_read_count = 0_u64;
    let mut range_read_count = 0_u64;
    let mut exact_replay = true;
    let mut machine_topology_exact = true;
    let mut machine_topology_count = 0_u64;
    let mut machine_artifact_count = 0_u64;
    let mut final_state_equal = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let TransactionContractRun {
            process: first,
            topology_sha256: first_topology,
            machine_report: first_machine_report,
        } = match run_transaction_contract_once(*seed, mode, &executable, machine_config.as_ref()) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let TransactionContractRun {
            process: second,
            topology_sha256: second_topology,
            machine_report: second_machine_report,
        } = match run_transaction_contract_once(*seed, mode, &executable, machine_config.as_ref()) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        machine_topology_exact &=
            first_topology == second_topology && first_machine_report == second_machine_report;
        machine_topology_count = machine_topology_count.saturating_add(u64::from(
            first_topology.is_some() && second_topology.is_some(),
        ));
        let oracle_history = match process_history_to_oracle(&first.history) {
            Ok(history) => history,
            Err(error) => return execution_from_result(Err(error)),
        };
        let oracle = check_history(&oracle_history);
        let per_seed_anomalies = first
            .anomaly_count
            .saturating_add(u64::try_from(oracle.anomalies.len()).unwrap_or(u64::MAX));
        anomaly_count = anomaly_count.saturating_add(per_seed_anomalies);
        event_count = event_count.saturating_add(first.executed_checks);
        process_starts = process_starts.saturating_add(first.process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        elections = elections.saturating_add(first.elections);
        dropped_replies = dropped_replies.saturating_add(first.dropped_replies);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        committed_count = committed_count.saturating_add(oracle.committed_count);
        aborted_count = aborted_count.saturating_add(oracle.aborted_count);
        multi_range_count = multi_range_count.saturating_add(oracle.multi_range_committed_count);
        point_read_count = point_read_count.saturating_add(oracle.point_read_count);
        range_read_count = range_read_count.saturating_add(oracle.range_read_count);
        final_state_equal &= first.final_state_equal;
        if let Some(detail) = first.first_mismatch.as_ref() {
            mismatch_details.push(format!("seed {seed}, process: {detail}"));
        } else if let Some(anomaly) = oracle.anomalies.first() {
            mismatch_details.push(format!(
                "seed {seed}, oracle {:?}, transaction {:?}: {}",
                anomaly.class, anomaly.transaction, anomaly.detail
            ));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(per_seed_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "okv-independent-occ-process-history-v1"),
                (
                    "anomaly.class",
                    if per_seed_anomalies == 0 {
                        "none"
                    } else {
                        "replicated_strict_serializability"
                    },
                ),
            ]),
        });
        artifact_refs.push(if let Some(topology) = first_topology {
            format!(
                "okv-transaction-machine://{topology}/{}/{seed}/{}",
                mode.id(),
                first.trace_sha256
            )
        } else {
            format!(
                "okv-transaction-process://{}/{seed}/{}",
                mode.id(),
                first.trace_sha256
            )
        });
        if let Some(bytes) = first_machine_report {
            let Some(directory) = machine_artifact_dir.as_ref() else {
                return execution_from_result(Err(
                    "transaction machine artifact directory is missing".to_owned(),
                ));
            };
            let digest = sha256(&bytes);
            let path = directory.join(format!("{}-{seed}-{digest}.json", workload.id));
            if let Err(error) = fs::write(&path, &bytes) {
                return execution_from_result(Err(format!(
                    "failed to write transaction machine receipt {}: {error}",
                    path.display()
                )));
            }
            machine_artifact_count = machine_artifact_count.saturating_add(1);
            artifact_refs.push(format!("{}#sha256={digest}", path.display()));
        }
    }

    let process_contract_exercised = process_starts >= u64::try_from(seeds.len()).unwrap_or(0) * 4
        && process_kills >= u64::try_from(seeds.len()).unwrap_or(0)
        && elections >= u64::try_from(seeds.len()).unwrap_or(0) * 2
        && dropped_replies == u64::try_from(seeds.len()).unwrap_or(0)
        && recovered_outcomes == u64::try_from(seeds.len()).unwrap_or(0)
        && final_state_equal;
    let product_shape_exercised = committed_count > 0
        && aborted_count > 0
        && multi_range_count > 0
        && point_read_count > 0
        && range_read_count > 0;
    let machine_topology_attested = !machine_mode
        || (machine_topology_exact
            && machine_topology_count == u64::try_from(seeds.len()).unwrap_or(u64::MAX)
            && machine_artifact_count == u64::try_from(seeds.len()).unwrap_or(u64::MAX));
    let passed = anomaly_count == 0
        && exact_replay
        && process_contract_exercised
        && product_shape_exercised
        && machine_topology_attested;
    let error = (!passed).then(|| {
        format!(
            "transaction process gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, process_contract={process_contract_exercised}, product_shape={product_shape_exercised}, machine_topology={machine_topology_attested}; {}",
            mode.id(),
            mismatch_details
                .first()
                .cloned()
                .unwrap_or_else(|| "no mismatch detail".to_owned())
        )
    });
    let mut hard_gates = vec![
            HardGateResult {
                id: "transaction_process.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "transaction_process.failover_recovery".to_owned(),
                status: gate_status(process_contract_exercised),
                detail: Some(format!(
                    "starts={process_starts}, kills={process_kills}, elections={elections}, dropped_replies={dropped_replies}, recovered_outcomes={recovered_outcomes}, final_state_equal={final_state_equal}"
                )),
            },
            HardGateResult {
                id: "transaction_process.product_shape_exercised".to_owned(),
                status: gate_status(product_shape_exercised),
                detail: Some(format!(
                    "committed={committed_count}, aborted={aborted_count}, multi_range_committed={multi_range_count}, point_reads={point_read_count}, range_reads={range_read_count}"
                )),
            },
            HardGateResult {
                id: "transaction_process.oracle_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
        ];
    if machine_mode {
        hard_gates.push(HardGateResult {
            id: "transaction_process.independent_machine_topology".to_owned(),
            status: gate_status(machine_topology_attested),
            detail: Some(format!(
                "topology_reports={machine_topology_count}, persisted_receipts={machine_artifact_count}, seeds={}, exact={machine_topology_exact}",
                seeds.len()
            )),
        });
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "transaction_process.process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "transaction_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "transaction_process.elections".to_owned(),
                bounded_count(elections),
            ),
            (
                "transaction_process.dropped_replies".to_owned(),
                bounded_count(dropped_replies),
            ),
            (
                "transaction_process.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
            (
                "transaction_process.committed".to_owned(),
                bounded_count(committed_count),
            ),
            (
                "transaction_process.aborted".to_owned(),
                bounded_count(aborted_count),
            ),
            (
                "transaction_process.multi_range_committed".to_owned(),
                bounded_count(multi_range_count),
            ),
        ]),
    }
}

fn transaction_machine_config(
    workload: &WorkloadConfig,
) -> Result<Option<TransactionMachineConfig>, String> {
    let Some(variable) = workload
        .parameters
        .get("machine_config_env")
        .and_then(toml::Value::as_str)
    else {
        return Ok(None);
    };
    let path = std::env::var(variable).map_err(|_| {
        format!("transaction machine config environment variable {variable} is unset")
    })?;
    let bytes = fs::read(&path)
        .map_err(|error| format!("failed to read transaction machine config {path}: {error}"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("invalid transaction machine config {path}: {error}"))
}

fn transaction_machine_artifact_dir(
    workload: &WorkloadConfig,
    machine_mode: bool,
) -> Result<Option<PathBuf>, String> {
    if !machine_mode {
        return Ok(None);
    }
    let variable = workload
        .parameters
        .get("machine_artifact_dir_env")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| "machine workload must declare machine_artifact_dir_env".to_owned())?;
    let path = PathBuf::from(std::env::var(variable).map_err(|_| {
        format!("transaction machine artifact environment variable {variable} is unset")
    })?);
    if !path.is_absolute() {
        return Err("transaction machine artifact directory must be absolute".to_owned());
    }
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "failed to create transaction machine artifact directory {}: {error}",
            path.display()
        )
    })?;
    Ok(Some(path))
}

fn run_transaction_contract_once(
    seed: u64,
    mode: TransactionProcessMode,
    executable: &Path,
    machine_config: Option<&TransactionMachineConfig>,
) -> Result<TransactionContractRun, String> {
    if let Some(config) = machine_config {
        let report = run_transaction_machine_contract(seed, mode, config.clone())?;
        let encoded = serde_json::to_vec(&report).map_err(|error| error.to_string())?;
        return Ok(TransactionContractRun {
            process: report.process,
            topology_sha256: Some(report.topology_sha256),
            machine_report: Some(encoded),
        });
    }
    run_transaction_process_contract(seed, mode, executable).map(|process| TransactionContractRun {
        process,
        topology_sha256: None,
        machine_report: None,
    })
}

fn process_history_to_oracle(
    history: &okv_consensus::ProcessTransactionHistory,
) -> Result<TransactionHistoryV1, String> {
    let writer_by_version: BTreeMap<u64, u64> = history
        .transactions
        .iter()
        .filter_map(|transaction| match transaction.result {
            ProcessTransactionResult::Committed { commit_version } => {
                Some((commit_version, transaction.id))
            }
            ProcessTransactionResult::Aborted { .. } => None,
        })
        .collect();
    let transactions = history
        .transactions
        .iter()
        .map(|transaction| {
            let reads = transaction
                .reads
                .iter()
                .map(|read| process_read_to_oracle(read, &writer_by_version))
                .collect::<Result<Vec<_>, _>>()?;
            let writes = transaction
                .mutations
                .iter()
                .map(transaction_mutation_to_oracle)
                .collect::<Result<Vec<_>, _>>()?;
            let applied_writes = transaction
                .applied_mutations
                .iter()
                .map(transaction_mutation_to_oracle)
                .collect::<Result<Vec<_>, _>>()?;
            let outcome = match &transaction.result {
                ProcessTransactionResult::Committed { commit_version } => {
                    OracleTransactionOutcome::Committed {
                        commit_version: *commit_version,
                    }
                }
                ProcessTransactionResult::Aborted { reason } => OracleTransactionOutcome::Aborted {
                    reason: reason.clone(),
                },
            };
            Ok(OracleTransactionRecord {
                id: transaction.id,
                begin_tick: transaction.begin_tick,
                complete_tick: transaction.complete_tick,
                read_version: transaction.read_version,
                reads,
                read_conflicts: transaction
                    .read_conflicts
                    .iter()
                    .map(transaction_range_to_oracle)
                    .collect(),
                writes,
                write_conflicts: transaction
                    .write_conflicts
                    .iter()
                    .map(transaction_range_to_oracle)
                    .collect(),
                outcome,
                applied_writes,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(TransactionHistoryV1 {
        schema_version: HISTORY_SCHEMA_VERSION,
        cell_id: history.cell_id.clone(),
        tenant_id: history.tenant_id.clone(),
        seed: history.seed,
        initial_state: Vec::new(),
        transactions,
    })
}

fn process_read_to_oracle(
    read: &ProcessReadOperation,
    writer_by_version: &BTreeMap<u64, u64>,
) -> Result<OracleReadOperation, String> {
    match read {
        ProcessReadOperation::Point { key, observed } => Ok(OracleReadOperation::Point {
            key: key.clone(),
            observed: observed
                .as_ref()
                .map(|value| process_observed_to_oracle(value, writer_by_version))
                .transpose()?,
        }),
        ProcessReadOperation::Range { range, observed } => Ok(OracleReadOperation::Range {
            range: transaction_range_to_oracle(range),
            observed: observed
                .iter()
                .map(|value| process_observed_to_oracle(value, writer_by_version))
                .collect::<Result<Vec<_>, _>>()?,
        }),
    }
}

fn process_observed_to_oracle(
    observed: &okv_consensus::ProcessObservedValue,
    writer_by_version: &BTreeMap<u64, u64>,
) -> Result<OracleObservedValue, String> {
    let writer = writer_by_version
        .get(&observed.writer_version)
        .copied()
        .ok_or_else(|| {
            format!(
                "observed unknown writer version {}",
                observed.writer_version
            )
        })?;
    Ok(OracleObservedValue {
        key: observed.key.clone(),
        value: observed.value.clone(),
        writer: Some(writer),
    })
}

fn transaction_range_to_oracle(range: &okv_consensus::TransactionKeyRange) -> OracleKeyRange {
    OracleKeyRange {
        start: range.start.clone(),
        end: range.end.clone(),
    }
}

fn transaction_mutation_to_oracle(
    mutation: &TransactionMutation,
) -> Result<OracleKeyValue, String> {
    match mutation {
        TransactionMutation::Set { key, value } => Ok(OracleKeyValue {
            key: key.clone(),
            value: value.clone(),
        }),
        TransactionMutation::Clear { .. } | TransactionMutation::ClearRange { .. } => {
            Err("transaction process v1 oracle bridge supports Set mutations only".to_owned())
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_model_differential(workload: &WorkloadConfig, seeds: &[u64]) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "model differential workload requires at least one seed".to_owned(),
        ));
    }
    let steps = workload
        .parameters
        .get("steps")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(1_000);
    let legacy_range_bug = workload
        .parameters
        .get("inject_ignore_range_clears")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match (control, legacy_range_bug) {
        (None | Some("none"), false) => DifferentialMode::Correct,
        (None | Some("ignore_range_clears"), true) | (Some("ignore_range_clears"), false) => {
            DifferentialMode::IgnoreRangeClears
        }
        (Some("mutation_order_affects_replay"), false) => {
            DifferentialMode::MutationOrderAffectsReplay
        }
        (Some("accept_conflicting_replay"), false) => DifferentialMode::AcceptConflictingReplay,
        (Some("future_read_falls_back"), false) => DifferentialMode::FutureReadFallsBack,
        (Some("reject_retention_boundary"), false) => DifferentialMode::RejectRetentionBoundary,
        (Some("serve_expired_read"), false) => DifferentialMode::ServeExpiredRead,
        (Some("accept_stale_generation"), false) => DifferentialMode::AcceptStaleGeneration,
        (Some(other), _) => {
            return execution_from_result(Err(format!("unknown model negative control {other}")));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut range_clear_count = 0_u64;
    let mut exact_replay_count = 0_u64;
    let mut conflicting_replay_count = 0_u64;
    let mut future_read_count = 0_u64;
    let mut retention_count = 0_u64;
    let mut too_old_read_count = 0_u64;
    let mut historical_read_count = 0_u64;
    let mut stale_generation_count = 0_u64;
    let mut read_count = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_differential_history(*seed, steps, mode);
        let second = run_differential_history(*seed, steps, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        range_clear_count = range_clear_count.saturating_add(first.range_clear_count);
        exact_replay_count = exact_replay_count.saturating_add(first.exact_replay_count);
        conflicting_replay_count =
            conflicting_replay_count.saturating_add(first.conflicting_replay_count);
        future_read_count = future_read_count.saturating_add(first.future_read_count);
        retention_count = retention_count.saturating_add(first.retention_count);
        too_old_read_count = too_old_read_count.saturating_add(first.too_old_read_count);
        historical_read_count = historical_read_count.saturating_add(first.historical_read_count);
        stale_generation_count =
            stale_generation_count.saturating_add(first.stale_generation_count);
        read_count = read_count.saturating_add(first.read_count);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "okv-model-independent-snapshot-v2"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        "mvcc_semantics"
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-model-history://{}/{seed}/{steps}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let range_clear_exercised = range_clear_count > 0;
    let semantic_operations_exercised = range_clear_exercised
        && exact_replay_count > 0
        && conflicting_replay_count > 0
        && future_read_count > 0
        && retention_count > 0
        && too_old_read_count > 0
        && historical_read_count > 0
        && stale_generation_count > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "MVCC differential gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "model.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "model.range_clear_exercised".to_owned(),
                status: if range_clear_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(range_clear_count.to_string()),
            },
            HardGateResult {
                id: "model.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "exact_replay={exact_replay_count}, conflicting_replay={conflicting_replay_count}, future_read={future_read_count}, retention={retention_count}, too_old={too_old_read_count}, historical={historical_read_count}, stale_generation={stale_generation_count}"
                )),
            },
            HardGateResult {
                id: "model.oracle_agreement".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "model.history.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "model.history.range_clears".to_owned(),
                bounded_count(range_clear_count),
            ),
            (
                "model.history.exact_replays".to_owned(),
                bounded_count(exact_replay_count),
            ),
            (
                "model.history.conflicting_replays".to_owned(),
                bounded_count(conflicting_replay_count),
            ),
            (
                "model.history.future_reads".to_owned(),
                bounded_count(future_read_count),
            ),
            (
                "model.history.retentions".to_owned(),
                bounded_count(retention_count),
            ),
            (
                "model.history.too_old_reads".to_owned(),
                bounded_count(too_old_read_count),
            ),
            (
                "model.history.historical_reads".to_owned(),
                bounded_count(historical_read_count),
            ),
            (
                "model.history.stale_generations".to_owned(),
                bounded_count(stale_generation_count),
            ),
            ("model.history.reads".to_owned(), bounded_count(read_count)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_store_conformance(workload: &WorkloadConfig, backend_id: &str) -> WorkloadExecution {
    let profile = match workload
        .parameters
        .get("contract_profile")
        .and_then(toml::Value::as_str)
    {
        Some("segment") => ConformanceProfile::Segment,
        Some("authority") => ConformanceProfile::Authority,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unsupported object-store contract profile {other}"
            )));
        }
        None => {
            return execution_from_result(Err(
                "object-store workload requires contract_profile".to_owned()
            ));
        }
    };

    let filesystem_root = std::env::temp_dir().join(format!("okv-object-eval-{}", Uuid::new_v4()));
    let backend = match backend_id {
        "memory" => Ok(memory_backend()),
        "filesystem" => filesystem_backend(&filesystem_root),
        "minio" => minio_backend_from_env(),
        "gcs" => gcs_backend_from_env(),
        other => {
            return execution_from_result(Err(format!(
                "object-store conformance has no backend adapter for {other}"
            )));
        }
    };
    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return execution_from_result(Err(format!(
                "failed to create object-store evaluation runtime: {error}"
            )));
        }
    };
    let report = runtime.block_on(run_conformance(
        backend,
        profile,
        &ConformanceOptions::default(),
    ));
    if let Err(error) = validate_conformance_report(&report) {
        return execution_from_result(Err(error.to_string()));
    }

    if backend_id == "filesystem" {
        let _ = fs::remove_dir_all(&filesystem_root);
    }

    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(report.failure_count),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "okv-object-store-v1"),
            (
                "anomaly.class",
                if report.passed() {
                    "none"
                } else {
                    "backend_contract"
                },
            ),
        ]),
    }];
    for case in &report.cases {
        measurements.push(Measurement {
            metric: "compatibility.cases",
            value: 1.0,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("compat.suite", "okv-object-store-v1"),
                ("feature.class", &case.id),
                (
                    "status",
                    match case.status {
                        CaseStatus::Pass => "pass",
                        CaseStatus::Fail => "fail",
                        CaseStatus::Unsupported => "unsupported",
                    },
                ),
            ]),
        });
    }
    let mut request_count = 0_u64;
    let mut request_bytes = 0_u64;
    let mut response_bytes = 0_u64;
    for stat in &report.stats.requests {
        request_count = request_count.saturating_add(stat.count);
        request_bytes = request_bytes.saturating_add(stat.request_bytes);
        response_bytes = response_bytes.saturating_add(stat.response_bytes);
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(stat.count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend_id),
                ("store", &report.backend.id),
                ("api", &stat.api),
                ("result", &stat.result),
            ]),
        });
        if stat.request_bytes > 0 {
            measurements.push(Measurement {
                metric: "object_store.bytes",
                value: bounded_count(stat.request_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend_id),
                    ("store", &report.backend.id),
                    ("direction", "request"),
                    ("api", &stat.api),
                ]),
            });
        }
        if stat.response_bytes > 0 {
            measurements.push(Measurement {
                metric: "object_store.bytes",
                value: bounded_count(stat.response_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend_id),
                    ("store", &report.backend.id),
                    ("direction", "response"),
                    ("api", &stat.api),
                ]),
            });
        }
    }

    let hard_gates = report
        .cases
        .iter()
        .map(|case| HardGateResult {
            id: format!("object_store.{}", case.id),
            status: match case.status {
                CaseStatus::Pass => GateStatus::Pass,
                CaseStatus::Fail => GateStatus::Fail,
                CaseStatus::Unsupported => GateStatus::Error,
            },
            detail: Some(case.detail.clone()),
        })
        .collect();
    WorkloadExecution {
        error: (!report.passed()).then(|| {
            let failed: Vec<&str> = report
                .cases
                .iter()
                .filter(|case| case.required && case.status != CaseStatus::Pass)
                .map(|case| case.id.as_str())
                .collect();
            format!("object-store contract failed: {}", failed.join(", "))
        }),
        measurements,
        hard_gates,
        budget_units: bounded_count(request_count),
        artifact_refs: vec![format!(
            "okv-object-conformance://{}/{}/{}/{}",
            report.backend.id,
            profile,
            report.backend.driver_version,
            report.backend.server_version
        )],
        secondary_metrics: BTreeMap::from([
            (
                "object_store.conformance_failures".to_owned(),
                bounded_count(report.failure_count),
            ),
            (
                "object_store.request_count".to_owned(),
                bounded_count(request_count),
            ),
            (
                "object_store.request_bytes".to_owned(),
                bounded_count(request_bytes),
            ),
            (
                "object_store.response_bytes".to_owned(),
                bounded_count(response_bytes),
            ),
        ]),
    }
}

fn bounded_count(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn gate_status(passed: bool) -> GateStatus {
    if passed {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    }
}

fn execution_from_result(result: Result<(), String>) -> WorkloadExecution {
    WorkloadExecution {
        error: result.err(),
        measurements: Vec::new(),
        hard_gates: Vec::new(),
        budget_units: 1.0,
        artifact_refs: Vec::new(),
        secondary_metrics: BTreeMap::new(),
    }
}

fn run_generation_recovery(
    workload: &WorkloadConfig,
    candidate_commit: &str,
    seeds: &[u64],
) -> WorkloadExecution {
    let Some(seed) = seeds.first().copied() else {
        return execution_from_result(Err(
            "deterministic generation recovery requires at least one dataset seed".to_owned(),
        ));
    };
    let first = run_generation_fencing(seed, candidate_commit, false);
    let second = run_generation_fencing(seed, candidate_commit, false);

    match (first, second) {
        (Ok(first), Ok(second)) => {
            let exact_replay = first == second;
            let Ok(anomaly_count) = u32::try_from(first.invariant_failures.len()) else {
                return execution_from_result(Err(
                    "simulation anomaly count exceeds the result contract".to_owned(),
                ));
            };
            let Ok(event_count) = u32::try_from(first.events.len()) else {
                return execution_from_result(Err(
                    "simulation event count exceeds the result contract".to_owned(),
                ));
            };
            let anomaly_count = f64::from(anomaly_count);
            let event_count = f64::from(event_count);
            let mut errors = Vec::new();
            if !exact_replay {
                errors.push("same-seed simulation traces diverged".to_owned());
            }
            errors.extend(first.invariant_failures.clone());
            WorkloadExecution {
                error: (!errors.is_empty()).then(|| errors.join("; ")),
                measurements: vec![Measurement {
                    metric: "correctness.anomalies",
                    value: anomaly_count + f64::from(!exact_replay),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "generation-fencing-v1"),
                        (
                            "anomaly.class",
                            if exact_replay && anomaly_count == 0.0 {
                                "none"
                            } else {
                                "simulation_invariant"
                            },
                        ),
                    ]),
                }],
                hard_gates: vec![
                    HardGateResult {
                        id: "exact_seed_replay".to_owned(),
                        status: if exact_replay {
                            GateStatus::Pass
                        } else {
                            GateStatus::Fail
                        },
                        detail: Some(first.trace_sha256.clone()),
                    },
                    HardGateResult {
                        id: "stale_generation_fenced".to_owned(),
                        status: if anomaly_count == 0.0 {
                            GateStatus::Pass
                        } else {
                            GateStatus::Fail
                        },
                        detail: None,
                    },
                ],
                budget_units: event_count,
                artifact_refs: vec![format!(
                    "okv-sim://generation-fencing-v1/{seed}/{}",
                    first.trace_sha256
                )],
                secondary_metrics: BTreeMap::from([("simulation.events".to_owned(), event_count)]),
            }
        }
        (Err(first), Err(second)) => execution_from_result(Err(format!(
            "both simulation replays failed: first={first}; second={second}"
        ))),
        (Err(error), _) | (_, Err(error)) => {
            execution_from_result(Err(format!("simulation replay failed: {error}")))
        }
    }
}

fn run_model_smoke() -> Result<(), String> {
    let mut model = Model::default();
    let batch = CommitBatch {
        version: Version::new(1),
        identity: CommitIdentity::for_test(1),
        mutations: vec![Mutation::Set {
            key: b"inventory/sku-1".to_vec(),
            value: b"10".to_vec(),
        }],
    };
    if model
        .apply(batch.clone())
        .map_err(|error| error.to_string())?
        != ApplyOutcome::Applied
    {
        return Err("initial commit was not applied".to_owned());
    }
    if model.apply(batch).map_err(|error| error.to_string())? != ApplyOutcome::AlreadyApplied {
        return Err("exact replay was not idempotent".to_owned());
    }
    if model
        .get(b"inventory/sku-1", Version::new(1))
        .map_err(|error| error.to_string())?
        != Some(&b"10"[..])
    {
        return Err("snapshot read returned the wrong value".to_owned());
    }
    Ok(())
}

fn run_provider_semantic_preflight(workload: &WorkloadConfig, backend: &str) -> WorkloadExecution {
    let Some(subject) = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str)
    else {
        return execution_from_result(Err(
            "provider semantic preflight requires subject".to_owned()
        ));
    };
    let report = match okv_eval::provider_selection::run(subject, backend) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    let failed = report
        .gates
        .iter()
        .filter(|gate| !gate.passed)
        .map(|gate| gate.id)
        .collect::<Vec<_>>();
    WorkloadExecution {
        error: (!failed.is_empty()).then(|| {
            format!(
                "provider semantic preflight failed gates: {}",
                failed.join(", ")
            )
        }),
        measurements: vec![Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(report.result.correctness_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "rfc-0041-provider-semantic-preflight-v1"),
                (
                    "anomaly.class",
                    if report.result.correctness_anomalies == 0 {
                        "none"
                    } else {
                        "provider_contract"
                    },
                ),
            ]),
        }],
        hard_gates: report
            .gates
            .into_iter()
            .map(|gate| HardGateResult {
                id: gate.id.to_owned(),
                status: gate_status(gate.passed),
                detail: Some(gate.detail),
            })
            .collect(),
        budget_units: bounded_count(report.result.required_capabilities),
        artifact_refs: vec![format!(
            "okv-plane://provider-preflight-v1/{}",
            report.result.provider
        )],
        secondary_metrics: BTreeMap::from([
            (
                "provider.required_capabilities".to_owned(),
                bounded_count(report.result.required_capabilities),
            ),
            (
                "provider.unsupported_capabilities".to_owned(),
                bounded_count(
                    u64::try_from(report.result.unsupported_capabilities.len()).unwrap_or(u64::MAX),
                ),
            ),
            (
                "provider.write_skew_commits".to_owned(),
                bounded_count(report.result.write_skew_commits),
            ),
            (
                "provider.eligible_for_live_spike".to_owned(),
                f64::from(report.result.eligible_for_live_spike),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_foundationdb_logical_lifecycle(
    workload: &WorkloadConfig,
    run_id: &str,
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "foundationdb-7.4.6+objectkv-lifecycle+gcs";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "FoundationDB logical lifecycle requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "FoundationDB logical lifecycle requires a dataset".to_owned()
        ));
    };
    let Some(subject) = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str)
    else {
        return execution_from_result(Err(
            "FoundationDB logical lifecycle requires a subject".to_owned()
        ));
    };
    if subject != "foundationdb-7.4.6" {
        return execution_from_result(Err(format!(
            "FoundationDB logical lifecycle requires subject foundationdb-7.4.6, got {subject}"
        )));
    }
    let env_name = |parameter: &str| -> Result<&str, String> {
        profile
            .parameters
            .get(parameter)
            .and_then(toml::Value::as_str)
            .ok_or_else(|| format!("FoundationDB lifecycle profile requires {parameter}"))
    };
    let env_value = |parameter: &str| -> Result<String, String> {
        let name = env_name(parameter)?;
        std::env::var(name).map_err(|_| format!("FoundationDB lifecycle requires env {name}"))
    };
    let python = match env_value("lifecycle_python_env") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let probe = match env_value("lifecycle_probe_env") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let bucket = match env_value("gcs_bucket_env") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let artifact_directory = match env_value("artifact_directory_env") {
        Ok(value) => PathBuf::from(value),
        Err(error) => return execution_from_result(Err(error)),
    };
    if let Err(error) = fs::create_dir_all(&artifact_directory) {
        return execution_from_result(Err(format!(
            "create FoundationDB lifecycle artifact directory: {error}"
        )));
    }
    let output_path = artifact_directory.join(format!(
        "foundationdb-logical-lifecycle-{run_id}-{}.json",
        workload.id
    ));
    let chunk_records = profile
        .parameters
        .get("restore_chunk_records")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(200);
    let object_prefix = profile
        .parameters
        .get("gcs_object_prefix")
        .and_then(toml::Value::as_str)
        .unwrap_or("results/provider-r0/lifecycle");
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);

    let mut command = Command::new(&python);
    command
        .arg(&probe)
        .arg("--run-id")
        .arg(run_id)
        .arg("--bucket")
        .arg(&bucket)
        .arg("--object-prefix")
        .arg(object_prefix)
        .arg("--record-count")
        .arg(dataset.key_count.to_string())
        .arg("--restore-chunk-records")
        .arg(chunk_records.to_string())
        .arg("--output")
        .arg(&output_path);
    if let Some(control) = negative_control {
        command.arg("--negative-control").arg(control);
    }
    if let Ok(cluster_file) = std::env::var("OKV_FOUNDATIONDB_CLUSTER_FILE") {
        command.arg("--cluster-file").arg(cluster_file);
    }
    let process = match command.output() {
        Ok(output) => output,
        Err(error) => {
            return execution_from_result(Err(format!(
                "execute FoundationDB lifecycle probe: {error}"
            )));
        }
    };
    let receipt_bytes = match fs::read(&output_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            let stderr = String::from_utf8_lossy(&process.stderr);
            return execution_from_result(Err(format!(
                "FoundationDB lifecycle probe produced no receipt: {error}; stderr={stderr}"
            )));
        }
    };
    let receipt = match okv_eval::provider_lifecycle::Receipt::from_json(&receipt_bytes) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    if receipt.run_id != run_id {
        return execution_from_result(Err(format!(
            "FoundationDB lifecycle receipt run {} != eval run {run_id}",
            receipt.run_id
        )));
    }
    let candidate_passed = receipt.candidate_passed() && process.status.success();
    let poison_detected = negative_control.is_some_and(|control| {
        receipt.negative_control_detected(control) && !process.status.success()
    });
    let result = if let Some(control) = negative_control {
        if poison_detected {
            Err(format!(
                "FoundationDB lifecycle negative control {control} was detected"
            ))
        } else {
            Err(format!(
                "FoundationDB lifecycle negative control {control} escaped detection"
            ))
        }
    } else if candidate_passed {
        Ok(())
    } else {
        Err("FoundationDB logical lifecycle failed one or more frozen gates".to_owned())
    };
    let total_object_bytes = receipt.closure_bytes.saturating_add(receipt.manifest_bytes);
    let outcome = if candidate_passed { "pass" } else { "fail" };
    let mut measurements = vec![
        Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(receipt.correctness_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "foundationdb-logical-lifecycle-r0-v1"),
                (
                    "anomaly.class",
                    negative_control.unwrap_or(if candidate_passed {
                        "none"
                    } else {
                        "candidate"
                    }),
                ),
            ]),
        },
        Measurement {
            metric: "object_store.bytes",
            value: bounded_count(total_object_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "gcs"),
                ("direction", "write"),
                ("api", "put"),
            ]),
        },
        Measurement {
            metric: "object_store.bytes",
            value: bounded_count(total_object_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "gcs"),
                ("direction", "read"),
                ("api", "get"),
            ]),
        },
        Measurement {
            metric: "recovery.hydration_bytes",
            value: bounded_count(receipt.closure_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("source", "gcs-named-object"),
                ("range.class", "logical-generation"),
            ]),
        },
    ];
    if let Some(seconds) = receipt.timing_seconds("objectify") {
        measurements.push(Measurement {
            metric: "objectification.lag",
            value: seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("result", outcome),
            ]),
        });
    }
    if let Some(seconds) = receipt.timing_seconds("restore_empty_generation") {
        measurements.push(Measurement {
            metric: "recovery.hydration_duration",
            value: seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("range.class", "logical-generation"),
                ("result", outcome),
            ]),
        });
    }
    let mut hard_gates = receipt
        .gates
        .iter()
        .map(|gate| HardGateResult {
            id: gate.id.clone(),
            status: gate_status(gate.passed),
            detail: Some(gate.detail.clone()),
        })
        .collect::<Vec<_>>();
    hard_gates.extend([
        HardGateResult {
            id: "negative_control_detected".to_owned(),
            status: gate_status(negative_control.is_none() || poison_detected),
            detail: negative_control.map(|control| format!("control={control}")),
        },
        HardGateResult {
            id: "logical_scope_does_not_claim_media_loss_or_ha".to_owned(),
            status: gate_status(!receipt.media_loss_verified && !receipt.ha_verified),
            detail: Some(receipt.scope.clone()),
        },
        HardGateResult {
            id: "provider_revision_pinned".to_owned(),
            status: GateStatus::Pass,
            detail: Some(receipt.provider.clone()),
        },
    ]);
    let error = result.err();
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(
            receipt
                .record_count_requested
                .saturating_add(receipt.restored_chunks)
                .saturating_add(receipt.replayed_chunks),
        ),
        artifact_refs: [
            output_path.display().to_string(),
            receipt.closure_uri.clone(),
            receipt.manifest_uri.clone(),
        ]
        .into_iter()
        .filter(|artifact| !artifact.is_empty())
        .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "foundationdb_lifecycle.duration_seconds".to_owned(),
                std::time::Duration::from_nanos(receipt.duration_ns).as_secs_f64(),
            ),
            (
                "foundationdb_lifecycle.closure_bytes".to_owned(),
                bounded_count(receipt.closure_bytes),
            ),
            (
                "foundationdb_lifecycle.restore_chunks".to_owned(),
                bounded_count(receipt.restored_chunks),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_foundationdb_media_loss_lifecycle(
    workload: &WorkloadConfig,
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "foundationdb-7.4.6+fresh-provider+objectkv-lifecycle+gcs";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "FoundationDB media-loss lifecycle requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "FoundationDB media-loss lifecycle requires a dataset".to_owned()
        ));
    };
    let subject = workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str);
    if subject != Some("foundationdb-7.4.6") {
        return execution_from_result(Err(format!(
            "FoundationDB media-loss lifecycle requires subject foundationdb-7.4.6, got {subject:?}"
        )));
    }
    let receipt_schema = profile
        .parameters
        .get("provider_receipt_schema")
        .and_then(toml::Value::as_str);
    if receipt_schema != Some("../schema/provider-media-loss-receipt-v1.schema.json") {
        return execution_from_result(Err(format!(
            "FoundationDB media-loss lifecycle requires the frozen provider receipt schema, got {receipt_schema:?}"
        )));
    }
    let Some(receipt_env) = workload
        .parameters
        .get("receipt_env")
        .and_then(toml::Value::as_str)
    else {
        return execution_from_result(Err(
            "FoundationDB media-loss workload requires receipt_env".to_owned()
        ));
    };
    let receipt_path = match std::env::var(receipt_env) {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            return execution_from_result(Err(format!(
                "FoundationDB media-loss lifecycle requires env {receipt_env}"
            )));
        }
    };
    let receipt_bytes = match fs::read(&receipt_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return execution_from_result(Err(format!(
                "read provider-media-loss receipt {}: {error}",
                receipt_path.display()
            )));
        }
    };
    let receipt = match okv_eval::provider_media_loss::Receipt::from_json(&receipt_bytes) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let candidate_passed = receipt.candidate_passed();
    let poison_detected =
        negative_control.is_some_and(|control| receipt.negative_control_detected(control));
    let result = if let Some(control) = negative_control {
        if poison_detected {
            Err(format!(
                "FoundationDB media-loss negative control {control} was detected"
            ))
        } else {
            Err(format!(
                "FoundationDB media-loss negative control {control} escaped detection"
            ))
        }
    } else if candidate_passed {
        Ok(())
    } else {
        Err("FoundationDB provider-media-loss receipt failed one or more frozen gates".to_owned())
    };
    let object_bytes = receipt
        .object_closure
        .closure_bytes
        .saturating_add(receipt.object_closure.manifest_bytes);
    let outcome = if candidate_passed { "pass" } else { "fail" };
    let mut measurements = vec![
        Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(receipt.correctness_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "provider-media-loss-r0-v1"),
                (
                    "anomaly.class",
                    negative_control.unwrap_or(if candidate_passed {
                        "none"
                    } else {
                        "candidate"
                    }),
                ),
            ]),
        },
        Measurement {
            metric: "object_store.bytes",
            value: bounded_count(object_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", "gcs"),
                ("direction", "read"),
                ("api", "get"),
            ]),
        },
        Measurement {
            metric: "recovery.hydration_bytes",
            value: bounded_count(receipt.object_closure.closure_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("source", "gcs-named-object"),
                ("range.class", "fresh-provider-generation"),
            ]),
        },
    ];
    if let Some(seconds) = receipt.timing_seconds("restore") {
        measurements.push(Measurement {
            metric: "recovery.hydration_duration",
            value: seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("range.class", "fresh-provider-generation"),
                ("result", outcome),
            ]),
        });
    }
    let mut hard_gates = receipt
        .gates
        .iter()
        .map(|gate| HardGateResult {
            id: gate.id.clone(),
            status: gate_status(gate.passed),
            detail: Some(gate.detail.clone()),
        })
        .collect::<Vec<_>>();
    hard_gates.extend([
        HardGateResult {
            id: "provider_media_loss_receipt_admitted".to_owned(),
            status: gate_status(negative_control.is_some() || candidate_passed),
            detail: Some(receipt.scope.clone()),
        },
        HardGateResult {
            id: "negative_control_detected".to_owned(),
            status: gate_status(negative_control.is_none() || poison_detected),
            detail: negative_control.map(|control| format!("control={control}")),
        },
        HardGateResult {
            id: "provider_revision_pinned".to_owned(),
            status: GateStatus::Pass,
            detail: Some(receipt.provider.clone()),
        },
    ]);
    let error = result.err();
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(
            dataset
                .key_count
                .saturating_add(receipt.restore.restored_chunks)
                .saturating_add(receipt.restore.replayed_chunks),
        ),
        artifact_refs: vec![
            receipt_path.display().to_string(),
            receipt.object_closure.manifest_uri.clone(),
            receipt.object_closure.closure_uri.clone(),
        ],
        secondary_metrics: BTreeMap::from([
            (
                "provider_media_loss.object_bytes".to_owned(),
                bounded_count(object_bytes),
            ),
            (
                "provider_media_loss.restore_chunks".to_owned(),
                bounded_count(receipt.restore.restored_chunks),
            ),
            (
                "provider_media_loss.restored_records".to_owned(),
                bounded_count(receipt.restore.restored_record_count),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_provider_incarnation_authority(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "external-cell-incarnation-authority";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "provider incarnation contract requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "provider incarnation contract requires at least one seed".to_owned(),
        ));
    }
    if workload
        .parameters
        .get("subject")
        .and_then(toml::Value::as_str)
        != Some("external-cell-incarnation-authority")
    {
        return execution_from_result(Err(
            "provider incarnation contract requires the external authority subject".to_owned(),
        ));
    }
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match negative_control {
        None => okv_eval::provider_incarnation::ProviderIncarnationMode::Correct,
        Some("accept_stale_source_incarnation") => {
            okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
        }
        Some(control) => {
            return execution_from_result(Err(format!(
                "unknown provider incarnation negative control {control}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports = Vec::new();
    let mut exact_replay = true;
    for seed in seeds {
        let first = match okv_eval::provider_incarnation::run_provider_incarnation_contract(
            *seed,
            mode,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match okv_eval::provider_incarnation::run_provider_incarnation_contract(
            *seed,
            mode,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }
    let poison_detected = mode
        == okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
        && reports.iter().all(|report| {
            !report.checks["newer_incarnation_fences_old_commit_authority"]
                && !report.checks["newer_incarnation_fences_old_routing"]
                && !report.checks["newer_incarnation_fences_old_object_publication"]
                && report.anomaly_count >= 3
        });
    let candidate_passed = mode == okv_eval::provider_incarnation::ProviderIncarnationMode::Correct
        && exact_replay
        && reports.iter().all(|report| report.anomaly_count == 0);
    let error = if mode
        == okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
    {
        Some(if poison_detected {
            "provider incarnation stale-source poison was detected".to_owned()
        } else {
            "provider incarnation stale-source poison escaped detection".to_owned()
        })
    } else if candidate_passed {
        None
    } else {
        Some("provider incarnation contract failed one or more frozen gates".to_owned())
    };
    let measurements = reports
        .iter()
        .map(|report| Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(report.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "provider-incarnation-process-v1"),
                (
                    "anomaly.class",
                    negative_control.unwrap_or(if report.anomaly_count == 0 {
                        "none"
                    } else {
                        "candidate"
                    }),
                ),
            ]),
        })
        .collect::<Vec<_>>();
    let mut hard_gates = reports
        .first()
        .map(|report| {
            report
                .checks
                .iter()
                .map(|(id, passed)| HardGateResult {
                    id: id.clone(),
                    status: gate_status(*passed),
                    detail: None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    hard_gates.extend([
        HardGateResult {
            id: "exact_fresh_process_replay".to_owned(),
            status: gate_status(exact_replay),
            detail: None,
        },
        HardGateResult {
            id: "stale_source_poison_is_detected".to_owned(),
            status: gate_status(negative_control.is_none() || poison_detected),
            detail: negative_control.map(|control| format!("control={control}")),
        },
    ]);
    let executed_checks = reports
        .iter()
        .map(|report| report.executed_checks)
        .sum::<u64>();
    let authority_process_starts = reports
        .iter()
        .map(|report| report.authority_process_starts)
        .sum::<u64>();
    let data_process_starts = reports
        .iter()
        .map(|report| report.data_process_starts)
        .sum::<u64>();
    let process_kills = reports
        .iter()
        .map(|report| report.process_kills)
        .sum::<u64>();
    let authority_failovers = reports
        .iter()
        .map(|report| report.authority_failovers)
        .sum::<u64>();
    let fenced_commit_rejections = reports
        .iter()
        .map(|report| report.fenced_commit_rejections)
        .sum::<u64>();
    let publication_writes = reports
        .iter()
        .map(|report| report.publication_writes)
        .sum::<u64>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates,
        budget_units: bounded_count(executed_checks),
        artifact_refs: reports
            .iter()
            .map(|report| {
                format!(
                    "okv-provider-incarnation://{}/{}/{}",
                    mode.id(),
                    report.seed,
                    report.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "provider_incarnation.authority_process_starts".to_owned(),
                bounded_count(authority_process_starts),
            ),
            (
                "provider_incarnation.data_process_starts".to_owned(),
                bounded_count(data_process_starts),
            ),
            (
                "provider_incarnation.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "provider_incarnation.authority_failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "provider_incarnation.fenced_commit_rejections".to_owned(),
                bounded_count(fenced_commit_rejections),
            ),
            (
                "provider_incarnation.publication_writes".to_owned(),
                bounded_count(publication_writes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_foundationdb_provider_incarnation(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "foundationdb-7.4.6+external-incarnation-authority+gcs";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "FoundationDB provider-incarnation lifecycle requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.len() != 1 {
        return execution_from_result(Err(
            "FoundationDB provider-incarnation lifecycle requires exactly one seed".to_owned(),
        ));
    }
    if profile
        .parameters
        .get("provider_receipt_schema")
        .and_then(toml::Value::as_str)
        != Some("../schema/provider-incarnation-receipt-v1.schema.json")
    {
        return execution_from_result(Err(
            "FoundationDB provider-incarnation profile does not name the frozen receipt schema"
                .to_owned(),
        ));
    }
    let receipt_env = match workload
        .parameters
        .get("receipt_env")
        .and_then(toml::Value::as_str)
    {
        Some(value) => value,
        None => {
            return execution_from_result(Err(
                "FoundationDB provider-incarnation workload requires receipt_env".to_owned(),
            ));
        }
    };
    let receipt_path = match std::env::var(receipt_env) {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            return execution_from_result(Err(format!(
                "FoundationDB provider-incarnation lifecycle requires env {receipt_env}"
            )));
        }
    };
    let receipt_bytes = match fs::read(&receipt_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return execution_from_result(Err(format!(
                "read provider-incarnation receipt {}: {error}",
                receipt_path.display()
            )));
        }
    };
    let receipt = match okv_eval::provider_incarnation_provider::Receipt::from_json(&receipt_bytes)
    {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match negative_control {
        None => okv_eval::provider_incarnation::ProviderIncarnationMode::Correct,
        Some("accept_stale_source_incarnation") => {
            okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
        }
        Some(control) => {
            return execution_from_result(Err(format!(
                "unknown provider-incarnation control {control}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let report = match okv_eval::provider_incarnation::run_provider_incarnation_contract(
        seeds[0],
        mode,
        &executable,
    ) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    let trace_reproduced = report.trace_sha256 == receipt.authority_trace_sha256;
    let poison_detected = mode
        == okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
        && receipt.negative_control_detected()
        && report.anomaly_count >= 3
        && trace_reproduced;
    let candidate_passed = mode == okv_eval::provider_incarnation::ProviderIncarnationMode::Correct
        && receipt.positive_passed()
        && report.anomaly_count == 0
        && trace_reproduced;
    let error = if mode
        == okv_eval::provider_incarnation::ProviderIncarnationMode::AcceptStaleSourceIncarnation
    {
        Some(if poison_detected {
            "real-provider stale-source poison was detected".to_owned()
        } else {
            "real-provider stale-source poison escaped detection".to_owned()
        })
    } else if candidate_passed {
        None
    } else {
        Some("real-provider incarnation receipt failed one or more frozen gates".to_owned())
    };
    let mut hard_gates = receipt
        .gates
        .iter()
        .map(|gate| HardGateResult {
            id: gate.id.clone(),
            status: gate_status(gate.passed),
            detail: Some(gate.detail.clone()),
        })
        .collect::<Vec<_>>();
    hard_gates.extend([
        HardGateResult {
            id: "external_authority_trace_reproduced".to_owned(),
            status: gate_status(trace_reproduced),
            detail: Some(format!("trace={}", report.trace_sha256)),
        },
        HardGateResult {
            id: "provider_incarnation_receipt_admitted".to_owned(),
            status: gate_status(candidate_passed || poison_detected),
            detail: Some(format!("receipt={}", receipt_path.display())),
        },
        HardGateResult {
            id: "stale_source_poison_is_detected".to_owned(),
            status: gate_status(negative_control.is_none() || poison_detected),
            detail: negative_control.map(|control| format!("control={control}")),
        },
    ]);
    WorkloadExecution {
        error,
        measurements: vec![Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(receipt.correctness_anomalies),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "provider-incarnation-gcp-r0-v1"),
                (
                    "anomaly.class",
                    negative_control.unwrap_or(if receipt.correctness_anomalies == 0 {
                        "none"
                    } else {
                        "candidate"
                    }),
                ),
            ]),
        }],
        hard_gates,
        budget_units: bounded_count(
            report
                .executed_checks
                .saturating_add(u64::try_from(receipt.gates.len()).unwrap_or(u64::MAX)),
        ),
        artifact_refs: vec![
            format!(
                "okv-provider-incarnation-gcp://{}/{}/{}",
                mode.id(),
                report.seed,
                report.trace_sha256
            ),
            receipt_path.display().to_string(),
        ],
        secondary_metrics: BTreeMap::from([
            (
                "provider_incarnation.authority_process_starts".to_owned(),
                bounded_count(report.authority_process_starts),
            ),
            (
                "provider_incarnation.data_process_starts".to_owned(),
                bounded_count(report.data_process_starts),
            ),
            (
                "provider_incarnation.process_kills".to_owned(),
                bounded_count(report.process_kills),
            ),
            (
                "provider_incarnation.authority_failovers".to_owned(),
                bounded_count(report.authority_failovers),
            ),
            (
                "provider_incarnation.fenced_commit_rejections".to_owned(),
                bounded_count(report.fenced_commit_rejections),
            ),
            (
                "provider_incarnation.publication_writes".to_owned(),
                bounded_count(report.publication_writes),
            ),
            (
                "provider_incarnation.provider_records".to_owned(),
                bounded_count(receipt.object_closure.record_count),
            ),
            (
                "provider_incarnation.provider_object_bytes".to_owned(),
                bounded_count(
                    receipt
                        .object_closure
                        .closure_bytes
                        .saturating_add(receipt.object_closure.manifest_bytes),
                ),
            ),
        ]),
    }
}

fn attributes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn dataset_config<'a>(loaded: &'a LoadedSuite, profile_id: &str) -> Option<&'a DatasetConfig> {
    loaded
        .suite
        .dataset
        .get(profile_id)
        .or_else(|| loaded.suite.dataset.values().next())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn git_is_dirty() -> Result<bool, Box<dyn Error>> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output()?;
    if !output.status.success() {
        return Err("git status failed while establishing candidate identity".into());
    }
    Ok(!output.stdout.is_empty())
}

fn git_revision(revision: &str) -> Option<String> {
    command_output("git", &["rev-parse", revision])
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod t27_controller_tests {
    use super::{
        acquire_t27_host_lease_at, canonical_t27_host_lease_path, t27_local_bytes_attributes,
    };

    #[test]
    fn machine_and_device_identity_derive_one_canonical_lease_path() {
        let first = canonical_t27_host_lease_path("instance-1", "filesystem-1", "259:0");
        let replay = canonical_t27_host_lease_path("instance-1", "filesystem-1", "259:0");
        let other_device = canonical_t27_host_lease_path("instance-1", "filesystem-2", "259:1");

        assert_eq!(first, replay);
        assert_ne!(first, other_device);
        assert_eq!(
            first.parent().and_then(std::path::Path::to_str),
            Some("/var/lib/objectkv/receipts")
        );
    }

    #[test]
    fn two_controllers_contend_on_the_same_host_global_lock() {
        let root = tempfile::TempDir::new().expect("create lease root");
        let path = root.path().join("t27-host.lock");
        let first = acquire_t27_host_lease_at(
            &path,
            u64::MAX,
            &"a".repeat(64),
            "40000000-0000-4000-8000-000000000001",
            "10000000-0000-4000-8000-000000000001",
        )
        .expect("first controller acquires host lock");

        let error = match acquire_t27_host_lease_at(
            &path,
            u64::MAX,
            &"a".repeat(64),
            "40000000-0000-4000-8000-000000000001",
            "10000000-0000-4000-8000-000000000002",
        ) {
            Ok(_) => panic!("second controller unexpectedly acquired host lock"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("busy"));

        first.release().expect("release first host lock");
    }

    #[test]
    fn local_byte_telemetry_carries_the_complete_metric_contract() {
        let values = t27_local_bytes_attributes(
            "cache-pressure",
            "t27-native-snapshot",
            "gcs+nvme+rocksdb",
            "0",
        );

        assert_eq!(values["cache.tier"], "nvme-rocksdb");
        assert_eq!(values["range.class"], "complete-resident");
        for required in ["lane", "workload", "backend", "cache.tier", "range.class"] {
            assert!(values.contains_key(required));
        }
    }
}
