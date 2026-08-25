use chrono::Utc;
use clap::{Parser, Subcommand};
use okv_consensus::{
    run_cell_concurrent_history, run_cell_process_prototype, run_cell_range_phantom_history,
    run_cell_read_version_proxy_history, run_commit_proxy_generation_recovery_contract,
    run_commit_proxy_recovery_role_process, run_generation_process_contract,
    run_incomplete_staged_head_abort_contract, run_multi_commit_proxy_ordering_contract,
    run_multi_commit_proxy_process, run_multi_proxy_resolver_process, run_multi_proxy_tlog_process,
    run_multi_record_staged_prefix_contract, run_online_resolver_split_contract,
    run_online_resolver_split_process, run_online_resolver_split_proxy_process,
    run_partitioned_resolver_contract, run_process_node, run_publication_process_contract,
    run_raft_cluster_contract, run_raft_process_contract, run_raft_storage_contract,
    run_read_version_proxy_process, run_recovery_curve_role_process,
    run_resolver_hotspot_curve_contract, run_resolver_hotspot_worker, run_resolver_process,
    run_routine_reconfiguration_process_contract, run_snapshot_lease_process_contract,
    run_staged_head_generation_contract, run_stateless_resolver_authenticated_tlog_contract,
    run_stateless_resolver_recovery_contract, run_transaction_system_recovery_curve_contract,
    CellConcurrentHistoryMode, CellProcessPrototypeMode, CellRangePhantomMode,
    CellRangePhantomReport, CellReadVersionProxyMode, CellReadVersionProxyReport,
    CommitProxyRecoveryMode, CommitProxyRecoveryReport, CommitProxyRecoveryRoleConfig,
    GenerationProcessMode, IncompleteStagedHeadAbortMode, MultiCommitProxyMode,
    MultiCommitProxyProcessConfig, MultiCommitProxyReport, MultiProxyResolverProcessConfig,
    MultiProxyTlogProcessConfig, MultiRecordStagedPrefixMode, OnlineResolverSplitMode,
    OnlineResolverSplitProcessConfig, OnlineResolverSplitProxyProcessConfig,
    OnlineResolverSplitReport, PartitionedResolverMode, PartitionedResolverReport,
    ProcessNodeConfig, PublicationProcessMode, RaftClusterMode, RaftProcessMode, RaftStorageMode,
    ReadVersionProxyProcessConfig, RecoveryCurveRoleConfig, ResolverHotspotCurveConfig,
    ResolverHotspotCurveMode, ResolverHotspotCurveReport, ResolverHotspotDistribution,
    ResolverHotspotWorkerConfig, ResolverProcessConfig, RoutineReconfigurationProcessMode,
    RoutineReconfigurationProcessReport, SnapshotLeaseProcessMode, StagedHeadGenerationMode,
    StatelessResolverAuthenticatedTlogMode, StatelessResolverAuthenticatedTlogReport,
    StatelessResolverRecoveryMode, StatelessResolverRecoveryReport,
    TransactionSystemRecoveryCurveConfig, TransactionSystemRecoveryCurveMode,
    TransactionSystemRecoveryCurveReport,
};
use okv_eval::config::{
    load_suite, BudgetKind, ConstraintOp, DatasetConfig, LaneConfig, LoadedSuite, ProfileConfig,
    WorkloadConfig,
};
use okv_eval::mvcc_gc_authority::{
    run_mvcc_gc_authority_collector_process, run_mvcc_gc_authority_composition,
    MvccGcAuthorityCollectorProcessConfig, MvccGcAuthorityCompositionMode,
};
use okv_eval::result::{
    median, median_absolute_deviation, statistic, validate_result, BudgetResult, EvalResult,
    GateStatus, HardGateResult, PrimaryMetricResult, ProfileIdentity, Verdict,
};
use okv_eval::telemetry::{MetricRecorder, RunResource, Telemetry};
use okv_htap::{
    run_physical_overlay_contract, run_streaming_overlay_contract, PhysicalOverlayMode,
    StreamingOverlayMode,
};
use okv_model::{
    run_differential_history, run_htap_contract, run_publication_gc_contract,
    run_routine_reconfiguration_contract, ApplyOutcome, CommitBatch, CommitIdentity,
    DifferentialMode, HtapContractMode, Model, Mutation, PublicationGcMode,
    RoutineReconfigurationMode, Version,
};
use okv_object::{
    cleanup_range_serving_curve_gcs_scratch, filesystem_backend, gcs_backend_from_env,
    memory_backend, minio_backend_from_env, run_cell_commit_proxy_process,
    run_cell_commit_visibility_contract, run_cell_commit_visibility_worker_process,
    run_cell_objectification_contract, run_cell_serving_authority_feed_contract,
    run_cell_serving_authority_worker_process, run_cell_serving_recovery_contract,
    run_cell_serving_tagged_tlog_contract, run_cell_serving_tagged_tlog_worker_process,
    run_cell_serving_worker_process, run_cell_tagged_log_chunked_repair_contract,
    run_cell_tagged_log_lag_ratekeeping_contract, run_cell_tagged_log_lag_worker_process,
    run_cell_tagged_log_learner_repair_contract, run_cell_tagged_log_policy_transition_contract,
    run_cell_tagged_log_repair_worker_process, run_conformance, run_publication_adapter_contract,
    run_publication_publisher_manifest_recovery_contract,
    run_publication_publisher_manifest_recovery_node, run_publication_publisher_process_contract,
    run_publication_publisher_process_node, run_publication_publisher_publish_recovery_contract,
    run_publication_publisher_publish_recovery_node,
    run_publication_publisher_put_recovery_contract, run_publication_publisher_put_recovery_node,
    run_publication_root_graph_contract, run_range_cache_eviction_worker,
    run_range_cache_fault_contract, run_range_cache_fault_worker_process,
    run_range_read_process_contract, run_range_read_process_worker,
    run_range_route_refresh_process_contract, run_range_serving_concurrency_contract,
    run_range_serving_concurrency_worker, run_range_serving_curve_worker,
    run_range_serving_handoff_contract, run_range_serving_handoff_worker_process,
    run_tagged_log_process, validate_conformance_report, CaseStatus, CellCommitProxyProcessConfig,
    CellCommitVisibilityMode, CellCommitVisibilityWorkerProcessConfig, CellObjectificationMode,
    CellServingAuthorityFeedMode, CellServingAuthorityWorkerProcessConfig, CellServingRecoveryMode,
    CellServingTaggedTlogMode, CellServingTaggedTlogWorkerProcessConfig,
    CellServingWorkerProcessConfig, CellTaggedLogChunkedRepairMode,
    CellTaggedLogLagRatekeepingMode, CellTaggedLogLagWorkerProcessConfig,
    CellTaggedLogLearnerRepairMode, CellTaggedLogPolicyTransitionMode,
    CellTaggedLogRepairWorkerProcessConfig, ConformanceOptions, ConformanceProfile, KvRuntime,
    KvRuntimeAction, KvRuntimeAdmission, KvRuntimeConfig, KvRuntimeDecision,
    ProviderCacheEconomicsConfig, ProviderCacheEconomicsMode, ProviderCacheTraceDistribution,
    PublicationAdapterMode, PublicationRootGraphMode, PublisherManifestRecoveryMode,
    PublisherManifestRecoveryProcessConfig, PublisherProcessConfig, PublisherProcessMode,
    PublisherPublishRecoveryMode, PublisherPublishRecoveryProcessConfig, PublisherPutRecoveryMode,
    PublisherPutRecoveryProcessConfig, RangeCacheEvictionBackend, RangeCacheEvictionConfig,
    RangeCacheEvictionMode, RangeCacheEvictionReceipt, RangeCacheFaultMode,
    RangeCacheFaultWorkerConfig, RangeEngineId, RangeEngineUsage, RangeReadProcessConfig,
    RangeReadProcessMode, RangeRouteRefreshMode, RangeServingCacheMode,
    RangeServingConcurrencyConfig, RangeServingConcurrencyMode, RangeServingCurveConfig,
    RangeServingCurveReceipt, RangeServingHandoffMode, RangeServingHandoffWorkerConfig,
    RangeServingObjectBackend, RangeServingProviderMode, TaggedLogProcessConfig,
};
use okv_postgres::{
    prepare_postgres_worker_readiness_fixture, run_postgres_object_delta_contract_with_config,
    run_postgres_page_commit_process_contract, run_postgres_page_read_process_contract,
    run_postgres_page_read_process_worker, run_postgres_page_write_gate_contract,
    run_postgres_worker_readiness_process, PostgresObjectDeltaContractConfig,
    PostgresObjectDeltaMode, PostgresPageCommitProcessMode, PostgresPageReadProcessConfig,
    PostgresPageReadProcessMode, PostgresPageWriteGateMode, PostgresWorkerReadinessConfig,
    PostgresWorkerReadinessMode, PostgresWorkerReadinessReceipt,
};
use okv_sim::{
    run_commit_contract, run_generation_fencing, run_persisted_wal_contract, CommitContractMode,
    PersistedWalMode,
};
use okv_slate::{
    run_kv_runtime_density_worker, run_mvcc_gc_curve_worker, run_phase0_compaction_contract,
    run_phase0_compaction_coordinator_process_node, run_phase0_compaction_reclaim_contract,
    run_phase0_compaction_worker_process_node, run_phase0_coordinator_fencing_contract,
    run_phase0_coordinator_recovery_contract, run_phase0_filesystem_contract,
    run_phase0_minio_compaction_contract, run_phase0_orphan_gc_contract,
    run_snapshot_read_curve_worker, KvRuntimeDensityMode, KvRuntimeDensityReceipt,
    KvRuntimeDensityTopology, KvRuntimeDensityWorkerConfig, MvccGcCurveConfig, MvccGcCurveMode,
    MvccGcCurveReceipt, Phase0CompactionConfig, Phase0CompactionCoordinatorProcessConfig,
    Phase0CompactionMode, Phase0CompactionReclaimConfig, Phase0CompactionReclaimMode,
    Phase0CompactionReclaimReport, Phase0CompactionReport, Phase0CompactionWorkerProcessConfig,
    Phase0Config, Phase0CoordinatorFencingConfig, Phase0CoordinatorFencingMode,
    Phase0CoordinatorFencingReport, Phase0CoordinatorRecoveryConfig, Phase0CoordinatorRecoveryMode,
    Phase0CoordinatorRecoveryReport, Phase0IoDelta, Phase0Mode, Phase0OrphanGcConfig,
    Phase0OrphanGcMode, Phase0OrphanGcReport, Phase0PhaseReport, Phase0PhysicalProfile,
    Phase0Report, SnapshotReadCurveConfig, SnapshotReadCurveMode, SnapshotReadCurveReceipt,
    SLATEDB_REVISION,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};
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

#[derive(Serialize)]
struct KvRuntimeDensityArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    topology: &'a str,
    target_range_engines: usize,
    mode: &'a str,
    receipts: &'a [KvRuntimeDensityReceipt],
    semantic_replay_receipt: &'a KvRuntimeDensityReceipt,
}

#[derive(Serialize)]
struct SnapshotReadCurveArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    version_depth: u64,
    mode: &'a str,
    receipts: &'a [SnapshotReadCurveReceipt],
    semantic_replay_receipt: &'a SnapshotReadCurveReceipt,
}

#[derive(Serialize)]
struct RangeServingCurveArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    receipts: &'a [RangeServingCurveReceipt],
    semantic_replay_receipt: &'a RangeServingCurveReceipt,
}

#[derive(Serialize)]
struct ProviderBoundRangeArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    cache_state: &'a str,
    provider_mode: &'a str,
    receipts: &'a [RangeServingCurveReceipt],
    semantic_replay_receipt: &'a RangeServingCurveReceipt,
}

#[derive(Serialize)]
struct ProviderCacheEconomicsArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    distribution: &'a str,
    cache_capacity_bytes: u64,
    provider_mode: &'a str,
    economics_mode: &'a str,
    receipts: &'a [RangeServingCurveReceipt],
    semantic_replay_receipt: &'a RangeServingCurveReceipt,
}

#[derive(Serialize)]
struct PostgresWorkerReadinessArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    mode: &'a str,
    receipts: &'a [PostgresWorkerReadinessReceipt],
    semantic_replay_receipt: &'a PostgresWorkerReadinessReceipt,
}

#[derive(Serialize)]
struct MvccGcCurveArtifact<'a> {
    contract_version: u32,
    executable_sha256: &'a str,
    workload: &'a str,
    history_depth: u64,
    retained_versions: u64,
    mode: &'a str,
    receipts: &'a [MvccGcCurveReceipt],
    semantic_replay_receipt: &'a MvccGcCurveReceipt,
}

impl WorkloadExecution {
    fn passed(&self) -> bool {
        self.error.is_none()
    }
}

#[derive(Debug, Parser)]
#[command(name = "okv-eval", about = "objectKV evaluation runner")]
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
    /// Run the throwaway vertical Cell v0 semantic transaction prototype.
    CellProcessPrototype {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Run a bounded concurrent Cell v0 transaction history through real processes.
    CellConcurrentHistory {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value_t = 1_000)]
        transactions: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Run a bounded range-read and phantom-dependency history through real processes.
    CellRangePhantomHistory {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value_t = 100)]
        rounds: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Run a bounded session handoff across two read-version proxy instances.
    CellReadVersionProxyHistory {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value_t = 100)]
        rounds: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Run cross-range transactions through three independent resolver processes.
    CellPartitionedResolverHistory {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value_t = 100)]
        rounds: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Replace one failed memory-only resolver through a transaction-system generation.
    CellStatelessResolverRecovery {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value_t = 600)]
        attempts: u64,
        #[arg(long, default_value_t = 8)]
        batch_size: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Objectify committed cell envelopes and reconstruct one fresh worker.
    CellObjectification {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Recover one target version from an object base plus retained WAL suffix.
    CellServingRecovery {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Recover one target version through a live committed-envelope authority feed.
    CellServingAuthorityFeed {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Recover one target version through a dedicated range-tagged tLog quorum.
    CellServingTaggedTlog {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Hand off one authority-pinned `SlateDB` base across signed txLog suffixes.
    RangeServingHandoffTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Interleave exact reads with repeated authenticated tail publication.
    RangeServingConcurrencyTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Route exact point and scan reads through an independent KV Runtime process.
    RangeReadProcessTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Refresh a stale route and fan out at one unchanged read version.
    RangeRouteRefreshTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Read encoded `PostgreSQL` pages through an independent routed KV Runtime.
    PostgresPageReadProcessTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Admit `PostgreSQL` pages only after WAL is durable through every page LSN.
    PostgresPageWriteGateTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Commit `PostgreSQL` pages plus extent through a real Cell and leader handoff.
    PostgresPageCommitProcessTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Exercise persistent-cache overwrite and torn-write faults in fresh processes.
    RangeCacheFaultTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Exercise many logical ranges through one physically bounded cache.
    RangeCacheEvictionTrace {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Prove that visibility waits for every required tagged-log quorum.
    CellCommitVisibility {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Bound tagged-log bytes through objectification lag, pop, and resume.
    CellTaggedLogLagRatekeeping {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Rebuild one failed tagged-log process as a certified non-voting learner.
    CellTaggedLogLearnerRepair {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Resume a chunked learner base and catch an ordered concurrent tail.
    CellTaggedLogChunkedRepair {
        #[arg(long, default_value_t = 1103)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Move one repaired tagged-log learner through a replicated policy epoch.
    CellTaggedLogPolicyTransition {
        #[arg(long, default_value_t = 1103)]
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
    /// Emit one certified staged-head generation takeover trace.
    StagedHeadGenerationTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one incomplete staged-head fence-and-abort trace.
    IncompleteStagedHeadAbortTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one bounded multi-record staged-prefix recovery trace.
    MultiRecordStagedPrefixTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one composed stateless-resolver and authenticated-tLog recovery trace.
    StatelessResolverAuthenticatedTlogTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one multi-commit-proxy ordering trace without suite orchestration.
    MultiCommitProxyOrderingTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one commit-proxy generation-recovery trace without suite orchestration.
    CommitProxyGenerationRecoveryTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one online resolver-map split trace without suite orchestration.
    OnlineResolverMapSplitTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one same-generation voter-replacement trace through real processes.
    RoutineReconfigurationProcessTrace {
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
    /// Emit the RFC-0060 lease and collection process history.
    SnapshotLeaseProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Compose one issued collection token with real physical MVCC collection.
    MvccGcAuthorityCompositionTrace {
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
    /// Internal entrypoint used by the real-process consensus controller.
    #[command(hide = true)]
    ConsensusNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the read-version proxy controller.
    #[command(hide = true)]
    ReadVersionProxyNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the partitioned-resolver controller.
    #[command(hide = true)]
    ResolverNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0051 commit-proxy process.
    #[command(hide = true)]
    MultiCommitProxyNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0051 resolver worker process.
    #[command(hide = true)]
    MultiProxyResolverNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0051 tLog worker process.
    #[command(hide = true)]
    MultiProxyTlogNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0053 transaction-system role process.
    #[command(hide = true)]
    CommitProxyRecoveryRoleNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0054 recovery-curve role process.
    #[command(hide = true)]
    RecoveryCurveRoleNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0052 commit-proxy process.
    #[command(hide = true)]
    OnlineResolverSplitProxyNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0052 source or shadow resolver process.
    #[command(hide = true)]
    OnlineResolverSplitNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by one RFC-0055 resolver throughput worker.
    #[command(hide = true)]
    ResolverHotspotWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the serving-recovery controller.
    #[command(hide = true)]
    CellServingWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the live-authority serving controller.
    #[command(hide = true)]
    CellServingAuthorityWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the tagged-tLog serving controller.
    #[command(hide = true)]
    CellServingTaggedTlogWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the range-root handoff controller.
    #[command(hide = true)]
    RangeServingHandoffWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the concurrent-publication controller.
    #[command(hide = true)]
    RangeServingConcurrencyNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint for one routed-read KV Runtime process.
    #[command(hide = true)]
    RangeReadServiceNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint for one `PostgreSQL` page-serving KV Runtime.
    #[command(hide = true)]
    PostgresPageReadNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the `PostgreSQL` replacement-worker curve.
    #[command(hide = true)]
    PostgresWorkerReadinessNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the persistent-cache fault controller.
    #[command(hide = true)]
    RangeCacheFaultWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the multi-range eviction controller.
    #[command(hide = true)]
    RangeCacheEvictionNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the staged commit controller.
    #[command(hide = true)]
    CellCommitProxyNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the staged commit recovery controller.
    #[command(hide = true)]
    CellCommitVisibilityWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the sustained-lag recovery controller.
    #[command(hide = true)]
    CellTaggedLogLagWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the tagged-log learner repair controller.
    #[command(hide = true)]
    CellTaggedLogRepairWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint for one dedicated tagged-log process.
    #[command(hide = true)]
    TaggedLogNode {
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
    /// Internal entrypoint used by the real-process compaction controller.
    #[command(hide = true)]
    SlateCompactionWorkerNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the real-process compaction coordinator.
    #[command(hide = true)]
    SlateCompactionCoordinatorNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the physical KV Runtime density controller.
    #[command(hide = true)]
    KvRuntimeDensityNode {
        #[arg(long)]
        config_json: String,
        #[arg(long)]
        mode: String,
    },
    /// Internal entrypoint used by the exact-version read controller.
    #[command(hide = true)]
    SnapshotReadCurveNode {
        #[arg(long)]
        config_json: String,
        #[arg(long)]
        mode: String,
    },
    /// Internal entrypoint used by the authority-bound range performance curve.
    #[command(hide = true)]
    RangeServingCurveNode {
        #[arg(long)]
        config_json: String,
    },
    /// Internal entrypoint used by the retained-history compaction controller.
    #[command(hide = true)]
    MvccGcCurveNode {
        #[arg(long)]
        config_json: String,
        #[arg(long)]
        mode: String,
    },
    /// Internal entrypoint used by the authority-bound physical collector.
    #[command(hide = true)]
    MvccGcAuthorityCollectorNode {
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
        Commands::ListMetrics { suite } => list_metrics(&load_suite(&suite)?),
        Commands::Plan { suite, profile } => print_plan(&load_suite(&suite)?, &profile)?,
        Commands::Run {
            suite,
            profile,
            workload,
            backend,
            output,
            allow_dirty,
        } => run_suite(
            &suite,
            &profile,
            &workload,
            &backend,
            output.as_deref(),
            allow_dirty,
        )?,
        Commands::RaftProcessTrace { seed, mode } => {
            let mode = parse_raft_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_raft_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellProcessPrototype { seed, mode } => {
            let mode = parse_cell_process_prototype_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_process_prototype(seed, mode, &executable)?;
            println!("objectKV semantic Raft prototype");
            println!("question: {}", report.question);
            for step in &report.steps {
                let rows = step
                    .rows
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                println!(
                    "phase={} node={} leader={:?} applied={:?} commit={} envelopes={} rows=[{}]",
                    step.phase,
                    step.node_id,
                    step.leader,
                    step.applied_log_index,
                    step.latest_commit_sequence,
                    step.committed_envelopes,
                    rows
                );
            }
            println!(
                "answer={} checks={} anomalies={}",
                report.answer, report.executed_checks, report.anomaly_count
            );
            println!("receipt={}", serde_json::to_string(&report)?);
        }
        Commands::CellConcurrentHistory {
            seed,
            transactions,
            mode,
        } => {
            let mode = parse_cell_concurrent_history_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_concurrent_history(seed, transactions, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellRangePhantomHistory { seed, rounds, mode } => {
            let mode = parse_cell_range_phantom_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_range_phantom_history(seed, rounds, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellReadVersionProxyHistory { seed, rounds, mode } => {
            let mode = parse_cell_read_version_proxy_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_read_version_proxy_history(seed, rounds, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellPartitionedResolverHistory { seed, rounds, mode } => {
            let mode = parse_partitioned_resolver_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_partitioned_resolver_contract(seed, rounds, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellStatelessResolverRecovery {
            seed,
            attempts,
            batch_size,
            mode,
        } => {
            let mode =
                parse_stateless_resolver_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_stateless_resolver_recovery_contract(
                seed,
                attempts,
                batch_size,
                mode,
                &executable,
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellObjectification { seed, mode } => {
            let mode = parse_cell_objectification_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_objectification_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellServingRecovery { seed, mode } => {
            let mode = parse_cell_serving_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_serving_recovery_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellServingAuthorityFeed { seed, mode } => {
            let mode =
                parse_cell_serving_authority_feed_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_serving_authority_feed_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellServingTaggedTlog { seed, mode } => {
            let mode = parse_cell_serving_tagged_tlog_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_serving_tagged_tlog_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::RangeServingHandoffTrace { seed, mode } => {
            let mode = parse_range_serving_handoff_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_range_serving_handoff_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::RangeServingConcurrencyTrace { seed, mode } => {
            let mode =
                parse_range_serving_concurrency_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let receipt = run_range_serving_concurrency_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeReadProcessTrace { seed, mode } => {
            let mode = parse_range_read_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let receipt = run_range_read_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeRouteRefreshTrace { seed, mode } => {
            let mode = parse_range_route_refresh_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let receipt = run_range_route_refresh_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::PostgresPageReadProcessTrace { seed, mode } => {
            let mode =
                parse_postgres_page_read_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let receipt = run_postgres_page_read_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::PostgresPageWriteGateTrace { seed, mode } => {
            let mode = parse_postgres_page_write_gate_mode(&mode).map_err(std::io::Error::other)?;
            let receipt = run_postgres_page_write_gate_contract(seed, mode)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::PostgresPageCommitProcessTrace { seed, mode } => {
            let mode =
                parse_postgres_page_commit_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let receipt = run_postgres_page_commit_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeCacheFaultTrace { seed, mode } => {
            let mode = parse_range_cache_fault_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_range_cache_fault_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::RangeCacheEvictionTrace { seed, mode } => {
            let mode = parse_range_cache_eviction_mode(&mode).map_err(std::io::Error::other)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_range_cache_eviction_worker(
                &default_range_cache_eviction_config(seed, mode),
            ))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::CellCommitVisibility { seed, mode } => {
            let mode = parse_cell_commit_visibility_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_commit_visibility_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellTaggedLogLagRatekeeping { seed, mode } => {
            let mode =
                parse_cell_tagged_log_lag_ratekeeping_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_tagged_log_lag_ratekeeping_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellTaggedLogLearnerRepair { seed, mode } => {
            let mode =
                parse_cell_tagged_log_learner_repair_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_tagged_log_learner_repair_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellTaggedLogChunkedRepair { seed, mode } => {
            let mode =
                parse_cell_tagged_log_chunked_repair_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_tagged_log_chunked_repair_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CellTaggedLogPolicyTransition { seed, mode } => {
            let mode = parse_cell_tagged_log_policy_transition_mode(&mode)
                .map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_cell_tagged_log_policy_transition_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::GenerationProcessTrace { seed, mode } => {
            let mode = parse_generation_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_generation_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::StagedHeadGenerationTrace { seed, mode } => {
            let mode = parse_staged_head_generation_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_staged_head_generation_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::IncompleteStagedHeadAbortTrace { seed, mode } => {
            let mode =
                parse_incomplete_staged_head_abort_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_incomplete_staged_head_abort_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::MultiRecordStagedPrefixTrace { seed, mode } => {
            let mode =
                parse_multi_record_staged_prefix_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_multi_record_staged_prefix_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::StatelessResolverAuthenticatedTlogTrace { seed, mode } => {
            let mode = parse_stateless_resolver_authenticated_tlog_mode(&mode)
                .map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report =
                run_stateless_resolver_authenticated_tlog_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::MultiCommitProxyOrderingTrace { seed, mode } => {
            let mode = parse_multi_commit_proxy_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_multi_commit_proxy_ordering_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::CommitProxyGenerationRecoveryTrace { seed, mode } => {
            let mode = parse_commit_proxy_recovery_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_commit_proxy_generation_recovery_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::OnlineResolverMapSplitTrace { seed, mode } => {
            let mode = parse_online_resolver_split_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_online_resolver_split_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::RoutineReconfigurationProcessTrace { seed, mode } => {
            let mode =
                parse_routine_reconfiguration_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_routine_reconfiguration_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::PublicationProcessTrace { seed, mode } => {
            let mode = parse_publication_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_publication_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::SnapshotLeaseProcessTrace { seed, mode } => {
            let mode = parse_snapshot_lease_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_snapshot_lease_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::MvccGcAuthorityCompositionTrace { seed, mode } => {
            let mode =
                parse_mvcc_gc_authority_composition_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let report =
                runtime.block_on(run_mvcc_gc_authority_composition(seed, mode, &executable))?;
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
        Commands::ConsensusNode { config_json } => {
            let config = serde_json::from_str::<ProcessNodeConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_process_node(config))?;
        }
        Commands::ReadVersionProxyNode { config_json } => {
            let config = serde_json::from_str::<ReadVersionProxyProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_read_version_proxy_process(config))?;
        }
        Commands::ResolverNode { config_json } => {
            let config = serde_json::from_str::<ResolverProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_resolver_process(config))?;
        }
        Commands::MultiCommitProxyNode { config_json } => {
            let config = serde_json::from_str::<MultiCommitProxyProcessConfig>(&config_json)?;
            let receipt = run_multi_commit_proxy_process(config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::MultiProxyResolverNode { config_json } => {
            let config = serde_json::from_str::<MultiProxyResolverProcessConfig>(&config_json)?;
            let receipt = run_multi_proxy_resolver_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::MultiProxyTlogNode { config_json } => {
            let config = serde_json::from_str::<MultiProxyTlogProcessConfig>(&config_json)?;
            let receipt = run_multi_proxy_tlog_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::CommitProxyRecoveryRoleNode { config_json } => {
            let config = serde_json::from_str::<CommitProxyRecoveryRoleConfig>(&config_json)?;
            let receipt = run_commit_proxy_recovery_role_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RecoveryCurveRoleNode { config_json } => {
            let config = serde_json::from_str::<RecoveryCurveRoleConfig>(&config_json)?;
            let receipt = run_recovery_curve_role_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::OnlineResolverSplitProxyNode { config_json } => {
            let config =
                serde_json::from_str::<OnlineResolverSplitProxyProcessConfig>(&config_json)?;
            let receipt = run_online_resolver_split_proxy_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::OnlineResolverSplitNode { config_json } => {
            let config = serde_json::from_str::<OnlineResolverSplitProcessConfig>(&config_json)?;
            let receipt = run_online_resolver_split_process(&config)?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::ResolverHotspotWorkerNode { config_json } => {
            let config = serde_json::from_str::<ResolverHotspotWorkerConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_resolver_hotspot_worker(config))?;
        }
        Commands::CellServingWorkerNode { config_json } => {
            let config = serde_json::from_str::<CellServingWorkerProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_cell_serving_worker_process(config))?;
        }
        Commands::CellServingAuthorityWorkerNode { config_json } => {
            let config =
                serde_json::from_str::<CellServingAuthorityWorkerProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_cell_serving_authority_worker_process(config))?;
        }
        Commands::CellServingTaggedTlogWorkerNode { config_json } => {
            let config =
                serde_json::from_str::<CellServingTaggedTlogWorkerProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_cell_serving_tagged_tlog_worker_process(config))?;
        }
        Commands::RangeServingHandoffWorkerNode { config_json } => {
            let config = serde_json::from_str::<RangeServingHandoffWorkerConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_range_serving_handoff_worker_process(config))?;
        }
        Commands::RangeServingConcurrencyNode { config_json } => {
            let config = serde_json::from_str::<RangeServingConcurrencyConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_range_serving_concurrency_worker(config))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeReadServiceNode { config_json } => {
            let config = serde_json::from_str::<RangeReadProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_range_read_process_worker(config))?;
        }
        Commands::PostgresPageReadNode { config_json } => {
            let config = serde_json::from_str::<PostgresPageReadProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_postgres_page_read_process_worker(config))?;
        }
        Commands::PostgresWorkerReadinessNode { config_json } => {
            let config = serde_json::from_str::<PostgresWorkerReadinessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_postgres_worker_readiness_process(&config))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeCacheFaultWorkerNode { config_json } => {
            let config = serde_json::from_str::<RangeCacheFaultWorkerConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_range_cache_fault_worker_process(config))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeCacheEvictionNode { config_json } => {
            let config = serde_json::from_str::<RangeCacheEvictionConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_range_cache_eviction_worker(&config))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::CellCommitProxyNode { config_json } => {
            let config = serde_json::from_str::<CellCommitProxyProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_cell_commit_proxy_process(config))?;
        }
        Commands::CellCommitVisibilityWorkerNode { config_json } => {
            let config =
                serde_json::from_str::<CellCommitVisibilityWorkerProcessConfig>(&config_json)?;
            run_cell_commit_visibility_worker_process(config)?;
        }
        Commands::CellTaggedLogLagWorkerNode { config_json } => {
            let config = serde_json::from_str::<CellTaggedLogLagWorkerProcessConfig>(&config_json)?;
            run_cell_tagged_log_lag_worker_process(config)?;
        }
        Commands::CellTaggedLogRepairWorkerNode { config_json } => {
            let config =
                serde_json::from_str::<CellTaggedLogRepairWorkerProcessConfig>(&config_json)?;
            run_cell_tagged_log_repair_worker_process(config)?;
        }
        Commands::TaggedLogNode { config_json } => {
            let config = serde_json::from_str::<TaggedLogProcessConfig>(&config_json)?;
            run_tagged_log_process(&config)?;
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
        Commands::SlateCompactionWorkerNode { config_json } => {
            let config = serde_json::from_str::<Phase0CompactionWorkerProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_phase0_compaction_worker_process_node(config))?;
        }
        Commands::SlateCompactionCoordinatorNode { config_json } => {
            let config =
                serde_json::from_str::<Phase0CompactionCoordinatorProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_phase0_compaction_coordinator_process_node(config))?;
        }
        Commands::KvRuntimeDensityNode { config_json, mode } => {
            let config = serde_json::from_str::<KvRuntimeDensityWorkerConfig>(&config_json)?;
            let mode = parse_kv_runtime_density_mode(&mode).map_err(std::io::Error::other)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_kv_runtime_density_worker(&config, mode))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::SnapshotReadCurveNode { config_json, mode } => {
            let config = serde_json::from_str::<SnapshotReadCurveConfig>(&config_json)?;
            let mode = parse_snapshot_read_curve_mode(&mode).map_err(std::io::Error::other)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_snapshot_read_curve_worker(&config, mode))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::RangeServingCurveNode { config_json } => {
            let config = serde_json::from_str::<RangeServingCurveConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_range_serving_curve_worker(&config))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::MvccGcCurveNode { config_json, mode } => {
            let config = serde_json::from_str::<MvccGcCurveConfig>(&config_json)?;
            let mode = parse_mvcc_gc_curve_mode(&mode).map_err(std::io::Error::other)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let receipt = runtime.block_on(run_mvcc_gc_curve_worker(&config, mode))?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
        Commands::MvccGcAuthorityCollectorNode { config_json } => {
            let config =
                serde_json::from_str::<MvccGcAuthorityCollectorProcessConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_mvcc_gc_authority_collector_process(config))?;
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

    let run_id = Uuid::new_v4().to_string();
    let mut candidate_commit = git_revision("HEAD")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    if source_dirty {
        candidate_commit.push_str("+dirty");
    }
    let parent_commit = git_revision("HEAD^")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    let suite_hash = contract_hash(&loaded)?;
    let profile_hash = sha256(toml::to_string(profile)?.as_bytes());
    let lockfile_hash = sha256(&fs::read("Cargo.lock")?);
    let resource = RunResource {
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        environment: profile_id.to_owned(),
        run_id: run_id.clone(),
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
    let failures = f64::from(!execution_passed);
    recorder.record(
        "operation.duration",
        elapsed,
        attributes(&[
            ("lane", &lane.id),
            ("workload", &workload.id),
            ("operation", &workload.operation),
            ("backend", backend),
            ("result", if failures == 0.0 { "pass" } else { "fail" }),
        ]),
    )?;
    recorder.record(
        "correctness.failures",
        failures,
        attributes(&[("lane", &lane.id), ("workload", &workload.id)]),
    )?;
    let (constraint_gates, constraints_passed, constraint_failures) =
        evaluate_lane_constraints(lane, &recorder);

    let samples = recorder.samples(&lane.primary_metric).to_vec();
    let samples = if samples.is_empty() && lane.primary_metric == "correctness.failures" {
        vec![failures]
    } else {
        samples
    };
    if samples.is_empty() {
        if let Some(error) = &workload_execution.error {
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
    let budget_observed = match profile.budget_kind {
        BudgetKind::Seconds => elapsed,
        BudgetKind::Events | BudgetKind::Operations => workload_execution.budget_units,
    };
    let budget_passed = budget_observed <= profile.budget_limit;
    let correctness_passed = execution_passed && constraints_passed;
    let verdict = if !correctness_passed || !budget_passed {
        Verdict::Discard
    } else if source_dirty {
        Verdict::Inconclusive
    } else {
        Verdict::Keep
    };
    let reason = workload_execution.error.unwrap_or_else(|| {
        if !constraints_passed {
            format!(
                "lane constraints failed: {}",
                constraint_failures.join("; ")
            )
        } else if source_dirty {
            "diagnostic dirty-tree run; hard gates passed but the result is not comparable"
                .to_owned()
        } else if budget_passed {
            "all configured hard gates passed".to_owned()
        } else {
            format!(
                "budget exceeded: observed {budget_observed}, limit {}",
                profile.budget_limit
            )
        }
    });
    let result = EvalResult {
        schema_version: 1,
        run_id,
        created_at: Utc::now().to_rfc3339(),
        lane: lane.id.clone(),
        suite: loaded.suite.id.clone(),
        suite_hash,
        profile: ProfileIdentity {
            id: profile_id.to_owned(),
            hash: profile_hash,
            machine: command_output("uname", &["-m"]).unwrap_or_else(|| "unknown".to_owned()),
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
            gates.extend(constraint_gates);
            gates.extend(workload_execution.hard_gates);
            gates
        },
        primary_metric: PrimaryMetricResult {
            name: primary_definition.otel_name.clone(),
            unit: primary_definition.unit.clone(),
            direction: lane.direction,
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
    telemetry.shutdown();
    Ok(())
}

fn evaluate_lane_constraints(
    lane: &LaneConfig,
    recorder: &MetricRecorder,
) -> (Vec<HardGateResult>, bool, Vec<String>) {
    let mut gates = Vec::with_capacity(lane.constraints.len());
    let mut failures = Vec::new();
    for (index, constraint) in lane.constraints.iter().enumerate() {
        let observed = statistic(recorder.samples(&constraint.metric), &constraint.statistic);
        let passed =
            observed.is_some_and(|value| constraint_holds(value, constraint.op, constraint.value));
        let detail = observed.map_or_else(
            || {
                format!(
                    "no samples for {} statistic {}",
                    constraint.metric, constraint.statistic
                )
            },
            |value| {
                format!(
                    "observed={value}, op={}, target={}",
                    constraint_op_id(constraint.op),
                    constraint.value
                )
            },
        );
        if !passed {
            failures.push(format!(
                "{} {} {} {} ({detail})",
                constraint.metric,
                constraint.statistic,
                constraint_op_id(constraint.op),
                constraint.value
            ));
        }
        gates.push(HardGateResult {
            id: format!(
                "lane.constraint.{index}.{}.{}",
                constraint.metric, constraint.statistic
            ),
            status: if passed {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: Some(detail),
        });
    }
    (gates, failures.is_empty(), failures)
}

fn constraint_holds(observed: f64, op: ConstraintOp, target: f64) -> bool {
    let tolerance = 1.0e-12 * observed.abs().max(target.abs()).max(1.0);
    match op {
        ConstraintOp::Eq => (observed - target).abs() <= tolerance,
        ConstraintOp::Ge => observed >= target,
        ConstraintOp::Gt => observed > target,
        ConstraintOp::Le => observed <= target,
        ConstraintOp::Lt => observed < target,
    }
}

const fn constraint_op_id(op: ConstraintOp) -> &'static str {
    match op {
        ConstraintOp::Eq => "eq",
        ConstraintOp::Ge => "ge",
        ConstraintOp::Gt => "gt",
        ConstraintOp::Le => "le",
        ConstraintOp::Lt => "lt",
    }
}

#[allow(clippy::too_many_lines)]
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
        "deterministic_generation_recovery" => {
            run_generation_recovery(workload, candidate_commit, seeds)
        }
        "commit_envelope_contract" => run_commit_envelope_contract(workload, seeds),
        "persisted_wal_contract" => run_persisted_wal(workload, seeds, backend),
        "raft_storage_contract" => run_raft_storage(workload, seeds, backend),
        "raft_cluster_contract" => run_raft_cluster(workload, seeds, backend),
        "raft_process_contract" => run_raft_process(workload, seeds, backend),
        "cell_process_transaction_contract" => {
            run_cell_process_transaction(workload, seeds, backend)
        }
        "cell_concurrent_history_contract" => {
            run_cell_concurrent_history_workload(workload, seeds, backend)
        }
        "cell_range_phantom_history_contract" => {
            run_cell_range_phantom_history_workload(workload, seeds, backend)
        }
        "cell_read_version_proxy_history_contract" => {
            run_cell_read_version_proxy_history_workload(workload, seeds, backend)
        }
        "cell_partitioned_resolver_agreement_contract" => {
            run_partitioned_resolver_agreement_workload(workload, seeds, backend)
        }
        "cell_stateless_resolver_generation_recovery_contract" => {
            run_stateless_resolver_recovery_workload(workload, seeds, backend)
        }
        "cell_stateless_resolver_authenticated_tlog_recovery_contract" => {
            run_stateless_resolver_authenticated_tlog_workload(workload, seeds, backend)
        }
        "cell_multi_commit_proxy_ordering_contract" => {
            run_multi_commit_proxy_ordering_workload(workload, seeds, backend)
        }
        "cell_commit_proxy_generation_recovery_contract" => {
            run_commit_proxy_generation_recovery_workload(workload, seeds, backend)
        }
        "cell_transaction_system_recovery_curve" => {
            run_transaction_system_recovery_curve_workload(workload, seeds, backend, profile)
        }
        "cell_online_resolver_map_split_contract" => {
            run_online_resolver_split_workload(workload, seeds, backend)
        }
        "cell_resolver_hotspot_throughput_curve" => {
            run_resolver_hotspot_curve_workload(workload, seeds, backend, profile)
        }
        "kv_runtime_resource_envelope_contract" => {
            run_kv_runtime_resource_envelope(workload, seeds, backend, profile)
        }
        "kv_runtime_physical_density_contract" => run_kv_runtime_physical_density(
            workload,
            run_id,
            candidate_commit,
            seeds,
            backend,
            profile,
        ),
        "kv_runtime_exact_version_read_curve" => {
            run_snapshot_read_curve(workload, run_id, candidate_commit, seeds, backend, profile)
        }
        "kv_runtime_mvcc_history_gc_curve" => {
            run_mvcc_gc_curve(workload, run_id, candidate_commit, seeds, backend, profile)
        }
        "cell_objectification_contract" => run_cell_objectification(workload, seeds, backend),
        "cell_serving_recovery_contract" => run_cell_serving_recovery(workload, seeds, backend),
        "cell_serving_authority_feed_contract" => {
            run_cell_serving_authority_feed(workload, seeds, backend)
        }
        "cell_serving_tagged_tlog_contract" => {
            run_cell_serving_tagged_tlog(workload, seeds, backend)
        }
        "range_serving_handoff_contract" => run_range_serving_handoff(workload, seeds, backend),
        "range_serving_concurrency_process_contract" => {
            run_range_serving_concurrency_process(workload, seeds, backend)
        }
        "range_read_process_contract" => run_range_read_process(workload, seeds, backend),
        "range_route_refresh_process_contract" => {
            run_range_route_refresh_process(workload, seeds, backend)
        }
        "postgres_page_read_process_contract" => {
            run_postgres_page_read_process(workload, seeds, backend)
        }
        "postgres_page_write_gate_contract" => {
            run_postgres_page_write_gate(workload, seeds, backend)
        }
        "postgres_page_commit_process_contract" => {
            run_postgres_page_commit_process(workload, seeds, backend)
        }
        "postgres_incremental_object_delta_contract" => {
            run_postgres_object_delta(workload, seeds, backend)
        }
        "postgres_replacement_worker_readiness" => run_postgres_worker_readiness(
            workload,
            run_id,
            candidate_commit,
            seeds,
            backend,
            profile,
        ),
        "range_cache_fault_process_contract" => {
            run_range_cache_fault_process(workload, seeds, backend)
        }
        "range_cache_eviction_process_contract" => {
            run_range_cache_eviction_process(workload, seeds, backend, profile)
        }
        "range_serving_performance_curve" => run_range_serving_performance_curve(
            workload,
            run_id,
            candidate_commit,
            seeds,
            backend,
            profile,
        ),
        "provider_bound_range_read" => run_provider_bound_range_read(
            workload,
            run_id,
            candidate_commit,
            seeds,
            backend,
            dataset,
            profile,
        ),
        "provider_bound_cache_economics" => run_provider_bound_cache_economics(
            workload,
            run_id,
            candidate_commit,
            seeds,
            backend,
            dataset,
            profile,
        ),
        "cell_commit_visibility_contract" => run_cell_commit_visibility(workload, seeds, backend),
        "cell_tagged_log_certificate_contract" => {
            run_cell_tagged_log_certificate(workload, seeds, backend)
        }
        "cell_tagged_log_lag_ratekeeper_contract" => {
            run_cell_tagged_log_lag_ratekeeper(workload, seeds, backend)
        }
        "cell_tagged_log_learner_repair_contract" => {
            run_cell_tagged_log_learner_repair(workload, seeds, backend)
        }
        "cell_tagged_log_chunked_live_repair_contract" => {
            run_cell_tagged_log_chunked_repair(workload, seeds, backend)
        }
        "cell_tagged_log_policy_transition_contract" => {
            run_cell_tagged_log_policy_transition(workload, seeds, backend)
        }
        "cell_staged_head_generation_takeover_contract" => {
            run_staged_head_generation_takeover(workload, seeds, backend)
        }
        "cell_incomplete_staged_head_abort_contract" => {
            run_incomplete_staged_head_abort(workload, seeds, backend)
        }
        "cell_multi_record_staged_prefix_recovery_contract" => {
            run_multi_record_staged_prefix_recovery(workload, seeds, backend)
        }
        "generation_process_contract" => run_generation_process(workload, seeds, backend),
        "routine_reconfiguration_process_contract" => {
            run_routine_reconfiguration_process(workload, seeds, backend)
        }
        "publication_authority_process_contract" => {
            run_publication_process(workload, seeds, backend)
        }
        "snapshot_lease_authority_process_contract" => {
            run_snapshot_lease_process(workload, seeds, backend)
        }
        "mvcc_gc_authority_composition_contract" => {
            run_mvcc_gc_authority_composition_workload(workload, seeds, backend)
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
        "object_publication_gc_contract" => {
            run_object_publication_gc_contract(workload, seeds, backend)
        }
        "routine_reconfiguration_contract" => {
            run_routine_reconfiguration_contract_workload(workload, seeds, backend)
        }
        "object_publication_adapter_contract" => {
            run_object_publication_adapter_contract(workload, seeds, backend)
        }
        "object_publication_root_graph_contract" => {
            run_object_publication_root_graph_contract(workload, seeds, backend)
        }
        "model_differential_history" => run_model_differential(workload, seeds),
        "object_store_conformance" => run_object_store_conformance(workload, backend),
        "slatedb_phase0_filesystem_contract" => run_slatedb_phase0_filesystem(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_external_compaction_contract" => run_slatedb_phase0_external_compaction(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_minio_compaction_contract" => run_slatedb_phase0_minio_compaction(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_compaction_reclaim_contract" => run_slatedb_phase0_compaction_reclaim(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_coordinator_recovery_contract" => run_slatedb_phase0_coordinator_recovery(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_coordinator_fencing_contract" => run_slatedb_phase0_coordinator_fencing(
            workload,
            run_id,
            candidate_commit,
            dataset,
            profile,
            backend,
        ),
        "slatedb_phase0_orphan_gc_contract" => run_slatedb_phase0_orphan_gc(
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
    let physical_profile = match profile
        .parameters
        .get("physical_profile")
        .and_then(toml::Value::as_str)
        .unwrap_or("slatedb-default-v1")
    {
        "slatedb-default-v1" => Phase0PhysicalProfile::SlateDbDefaultV1,
        "objectkv-serving-v1" => Phase0PhysicalProfile::ObjectKvServingV1,
        other => {
            return execution_from_result(Err(format!(
                "unknown SlateDB Phase 0 physical_profile {other}"
            )));
        }
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
            physical_profile,
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

fn run_slatedb_phase0_external_compaction(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB external compaction workload requires a dataset".to_owned(),
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| {
                format!("SlateDB external compaction profile requires integer {name}")
            })?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let config = match (
        parameter("flush_count"),
        parameter("compaction_timeout_millis"),
    ) {
        (Ok(flush_count), Ok(timeout_millis)) => Phase0CompactionConfig {
            logical_bytes: dataset.logical_bytes,
            key_count: dataset.key_count,
            flush_count,
            seeds: dataset.seeds.clone(),
            timeout_millis,
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0CompactionMode::Correct,
        Some("skip_external_worker") => Phase0CompactionMode::SkipExternalWorker,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB external compaction negative control {other}"
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
                "build SlateDB external compaction runtime: {error}"
            )));
        }
    };
    let report = match runtime.block_on(run_phase0_compaction_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_compaction_execution(workload, run_id, candidate_commit, backend, &report)
}

fn run_slatedb_phase0_minio_compaction(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB MinIO compaction workload requires a dataset".to_owned()
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("SlateDB MinIO compaction profile requires integer {name}"))?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let config = match (
        parameter("flush_count"),
        parameter("compaction_timeout_millis"),
    ) {
        (Ok(flush_count), Ok(timeout_millis)) => Phase0CompactionConfig {
            logical_bytes: dataset.logical_bytes,
            key_count: dataset.key_count,
            flush_count,
            seeds: dataset.seeds.clone(),
            timeout_millis,
        },
        (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0CompactionMode::Correct,
        Some("skip_external_worker") => Phase0CompactionMode::SkipExternalWorker,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB MinIO compaction negative control {other}"
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
                "build SlateDB MinIO compaction runtime: {error}"
            )));
        }
    };
    let namespace = format!("{run_id}-{}", workload.id);
    let report = match runtime.block_on(run_phase0_minio_compaction_contract(
        &config, mode, &namespace,
    )) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_compaction_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_compaction_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0CompactionReport,
) -> WorkloadExecution {
    const ORACLE: &str = "deterministic-slate-external-compaction-v1";
    let lane = if report.store == "minio-s3" {
        "slatedb-minio-compaction"
    } else {
        "slatedb-external-compaction"
    };
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut total_io = Phase0IoDelta::default();
    let dataset_class = format!("{}-{}-bytes", report.store, report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "recovery.first_correct_read_duration",
            value: seed.reopen_first_correct_read_seconds,
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", &dataset_class),
                ("result", if report.passed() { "pass" } else { "fail" }),
            ]),
        });
        measurements.push(Measurement {
            metric: "compaction.write_amplification",
            value: seed.maintenance_write_amplification,
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("compaction.kind", "standalone-size-tiered"),
            ]),
        });
        for phase in [
            &seed.initial_open,
            &seed.ingest_and_flush,
            &seed.close_before_maintenance,
            &seed.maintenance,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.full_verify,
            &seed.final_close,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, lane, phase);
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
    add_phase0_object_measurements(
        &mut measurements,
        workload,
        backend,
        lane,
        &report.store,
        &total_io,
    );
    let anomalies = report.anomaly_count();
    measurements.push(Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", lane),
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
    let artifact_path = phase0_compaction_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_json_artifact(&artifact_path, report, "external compaction");
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB external compaction failed gates: {}",
            failed_gates.join(", ")
        ))
    };
    let max_write_amplification = report
        .seeds
        .iter()
        .map(|seed| seed.maintenance_write_amplification)
        .fold(0.0_f64, f64::max);
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
                "phase0.compaction.object_store.requests.total".to_owned(),
                bounded_count(total_io.request_total()),
            ),
            (
                "phase0.compaction.object_store.read_bytes.total".to_owned(),
                bounded_count(total_io.read_byte_total()),
            ),
            (
                "phase0.compaction.object_store.written_bytes.total".to_owned(),
                bounded_count(total_io.written_byte_total()),
            ),
            (
                "phase0.compaction.write_amplification.max".to_owned(),
                max_write_amplification,
            ),
            (
                "phase0.compaction.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
        ]),
    }
}

fn run_slatedb_phase0_compaction_reclaim(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB compaction reclaim workload requires a dataset".to_owned(),
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("SlateDB compaction reclaim profile requires integer {name}"))?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let worker_binary = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            return execution_from_result(Err(format!(
                "resolve compaction worker executable: {error}"
            )));
        }
    };
    let config = match (
        parameter("overwrite_rounds"),
        parameter("claim_timeout_millis"),
        parameter("reclaim_timeout_millis"),
        parameter("completion_timeout_millis"),
    ) {
        (
            Ok(overwrite_rounds),
            Ok(claim_timeout_millis),
            Ok(reclaim_timeout_millis),
            Ok(completion_timeout_millis),
        ) => Phase0CompactionReclaimConfig {
            logical_bytes: dataset.logical_bytes,
            key_count: dataset.key_count,
            overwrite_rounds,
            seeds: dataset.seeds.clone(),
            claim_timeout_millis,
            reclaim_timeout_millis,
            completion_timeout_millis,
            worker_binary,
        },
        (Err(error), _, _, _)
        | (_, Err(error), _, _)
        | (_, _, Err(error), _)
        | (_, _, _, Err(error)) => return execution_from_result(Err(error)),
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0CompactionReclaimMode::Correct,
        Some("skip_replacement_worker") => Phase0CompactionReclaimMode::SkipReplacementWorker,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB compaction reclaim negative control {other}"
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
                "build SlateDB compaction reclaim runtime: {error}"
            )));
        }
    };
    let report = match runtime.block_on(run_phase0_compaction_reclaim_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_compaction_reclaim_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_compaction_reclaim_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0CompactionReclaimReport,
) -> WorkloadExecution {
    const LANE: &str = "slatedb-compaction-reclaim";
    const ORACLE: &str = "deterministic-slate-compaction-reclaim-v1";
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut controller_io = Phase0IoDelta::default();
    let dataset_class = format!("local-fs-{}-bytes", report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "compaction.reclaim_duration",
            value: seed.kill_to_completion_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                (
                    "result",
                    if seed.replacement_completed {
                        "pass"
                    } else {
                        "fail"
                    },
                ),
            ]),
        });
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
            &seed.ingest,
            &seed.reclaim,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.full_verify,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, LANE, phase);
            total_operations += phase.logical_operations;
        }
        merge_counts(
            &mut controller_io.successful_requests,
            &seed.total_io_observed_by_controller.successful_requests,
        );
        merge_counts(
            &mut controller_io.failed_requests,
            &seed.total_io_observed_by_controller.failed_requests,
        );
        merge_counts(
            &mut controller_io.read_bytes,
            &seed.total_io_observed_by_controller.read_bytes,
        );
        merge_counts(
            &mut controller_io.written_bytes,
            &seed.total_io_observed_by_controller.written_bytes,
        );
    }
    add_phase0_object_measurements(
        &mut measurements,
        workload,
        backend,
        LANE,
        &report.store,
        &controller_io,
    );
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
    let artifact_path = phase0_compaction_reclaim_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_json_artifact(&artifact_path, report, "compaction reclaim");
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB compaction reclaim failed gates: {}",
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
                "phase0.compaction_reclaim.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
            (
                "phase0.compaction_reclaim.process_kills".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.first_worker_killed)
                        .count() as u64,
                ),
            ),
            (
                "phase0.compaction_reclaim.completed".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.replacement_completed)
                        .count() as u64,
                ),
            ),
            (
                "phase0.compaction_reclaim.controller_requests.total".to_owned(),
                bounded_count(controller_io.request_total()),
            ),
        ]),
    }
}

fn run_slatedb_phase0_coordinator_recovery(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB coordinator recovery workload requires a dataset".to_owned(),
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| {
                format!("SlateDB coordinator recovery profile requires integer {name}")
            })?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let process_binary = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            return execution_from_result(Err(format!(
                "resolve coordinator recovery executable: {error}"
            )));
        }
    };
    let config = match (
        parameter("overwrite_rounds"),
        parameter("compacted_timeout_millis"),
        parameter("completion_timeout_millis"),
    ) {
        (Ok(overwrite_rounds), Ok(compacted_timeout_millis), Ok(completion_timeout_millis)) => {
            Phase0CoordinatorRecoveryConfig {
                logical_bytes: dataset.logical_bytes,
                key_count: dataset.key_count,
                overwrite_rounds,
                seeds: dataset.seeds.clone(),
                compacted_timeout_millis,
                completion_timeout_millis,
                process_binary,
            }
        }
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return execution_from_result(Err(error));
        }
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0CoordinatorRecoveryMode::Correct,
        Some("skip_coordinator_restart") => Phase0CoordinatorRecoveryMode::SkipCoordinatorRestart,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB coordinator recovery negative control {other}"
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
                "build SlateDB coordinator recovery runtime: {error}"
            )));
        }
    };
    let report = match runtime.block_on(run_phase0_coordinator_recovery_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_coordinator_recovery_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_coordinator_recovery_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0CoordinatorRecoveryReport,
) -> WorkloadExecution {
    const LANE: &str = "slatedb-coordinator-recovery";
    const ORACLE: &str = "deterministic-slate-coordinator-recovery-v1";
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut controller_io = Phase0IoDelta::default();
    let dataset_class = format!("local-fs-{}-bytes", report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "compaction.coordinator_recovery_duration",
            value: seed.kill_to_completion_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                (
                    "result",
                    if seed.replacement_committed_existing_output {
                        "pass"
                    } else {
                        "fail"
                    },
                ),
            ]),
        });
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
            &seed.ingest,
            &seed.coordinator_recovery,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.full_verify,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, LANE, phase);
            total_operations += phase.logical_operations;
        }
        merge_counts(
            &mut controller_io.successful_requests,
            &seed.total_io_observed_by_controller.successful_requests,
        );
        merge_counts(
            &mut controller_io.failed_requests,
            &seed.total_io_observed_by_controller.failed_requests,
        );
        merge_counts(
            &mut controller_io.read_bytes,
            &seed.total_io_observed_by_controller.read_bytes,
        );
        merge_counts(
            &mut controller_io.written_bytes,
            &seed.total_io_observed_by_controller.written_bytes,
        );
    }
    add_phase0_object_measurements(
        &mut measurements,
        workload,
        backend,
        LANE,
        &report.store,
        &controller_io,
    );
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
    let artifact_path = phase0_coordinator_recovery_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_json_artifact(&artifact_path, report, "coordinator recovery");
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB coordinator recovery failed gates: {}",
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
                "phase0.coordinator_recovery.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
            (
                "phase0.coordinator_recovery.process_kills".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.first_coordinator_killed)
                        .count() as u64,
                ),
            ),
            (
                "phase0.coordinator_recovery.completed".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.replacement_committed_existing_output)
                        .count() as u64,
                ),
            ),
            (
                "phase0.coordinator_recovery.controller_requests.total".to_owned(),
                bounded_count(controller_io.request_total()),
            ),
        ]),
    }
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
            add_phase0_phase_measurements(&mut measurements, workload, backend, LANE, phase);
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
    lane: &str,
    phase: &Phase0PhaseReport,
) {
    let throughput = if phase.elapsed_seconds > 0.0 {
        bounded_count(phase.logical_operations) / phase.elapsed_seconds
    } else {
        0.0
    };
    measurements.push(Measurement {
        metric: "operation.throughput",
        value: throughput,
        attributes: attributes(&[
            ("lane", lane),
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

fn add_phase0_object_measurements(
    measurements: &mut Vec<Measurement>,
    workload: &WorkloadConfig,
    backend: &str,
    lane: &str,
    store: &str,
    io: &Phase0IoDelta,
) {
    for (api, value) in &io.successful_requests {
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", store),
                ("api", api),
                ("result", "success"),
            ]),
        });
    }
    for (api, value) in &io.failed_requests {
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", store),
                ("api", api),
                ("result", "error"),
            ]),
        });
    }
    for (api, value) in &io.read_bytes {
        measurements.push(Measurement {
            metric: "object_store.bytes",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", store),
                ("direction", "read"),
                ("api", api),
            ]),
        });
    }
    for (api, value) in &io.written_bytes {
        measurements.push(Measurement {
            metric: "object_store.bytes",
            value: bounded_count(*value),
            attributes: attributes(&[
                ("lane", lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("store", store),
                ("direction", "write"),
                ("api", api),
            ]),
        });
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

fn phase0_compaction_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    report: &Phase0CompactionReport,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    let contract = if report.store == "minio-s3" {
        "phase0-slate-minio-compaction"
    } else {
        "phase0-slate-external-compaction"
    };
    PathBuf::from("target/okv-eval-artifacts")
        .join(format!("{contract}-{candidate}-{run}-{}.json", report.mode))
}

fn phase0_compaction_reclaim_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    report: &Phase0CompactionReclaimReport,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "phase0-slate-compaction-reclaim-{candidate}-{run}-{}.json",
        report.mode
    ))
}

fn phase0_coordinator_recovery_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    report: &Phase0CoordinatorRecoveryReport,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "phase0-slate-coordinator-recovery-{candidate}-{run}-{}.json",
        report.mode
    ))
}

fn phase0_coordinator_fencing_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    report: &Phase0CoordinatorFencingReport,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "phase0-slate-coordinator-fencing-{candidate}-{run}-{}.json",
        report.mode
    ))
}

fn phase0_orphan_gc_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    report: &Phase0OrphanGcReport,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "phase0-slate-orphan-gc-{candidate}-{run}-{}.json",
        report.mode
    ))
}

fn kv_runtime_density_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "kv-runtime-density-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn snapshot_read_curve_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "snapshot-read-curve-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn range_serving_curve_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "range-serving-curve-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn provider_bound_range_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    let root = std::env::var_os("OKV_EVAL_ARTIFACT_DIR")
        .map_or_else(|| PathBuf::from("target/okv-eval-artifacts"), PathBuf::from);
    root.join(format!(
        "provider-bound-range-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn provider_cache_economics_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    let root = std::env::var_os("OKV_EVAL_ARTIFACT_DIR")
        .map_or_else(|| PathBuf::from("target/okv-eval-artifacts"), PathBuf::from);
    root.join(format!(
        "provider-cache-economics-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn postgres_worker_readiness_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "postgres-worker-readiness-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn mvcc_gc_curve_artifact_path(
    run_id: &str,
    candidate_commit: &str,
    workload: &WorkloadConfig,
) -> PathBuf {
    let candidate = candidate_commit.replace(['+', '/'], "-");
    let run = run_id.replace(['+', '/'], "-");
    PathBuf::from("target/okv-eval-artifacts").join(format!(
        "mvcc-gc-curve-{candidate}-{run}-{}.json",
        workload.id
    ))
}

fn write_json_artifact<T: Serialize>(path: &Path, report: &T, label: &str) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("{label} artifact path has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create {label} artifact directory: {error}"))?;
    let rendered = serde_json::to_string_pretty(report)
        .map_err(|error| format!("serialize {label} report: {error}"))?;
    fs::write(path, format!("{rendered}\n"))
        .map_err(|error| format!("write {label} artifact: {error}"))
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

fn normalized_routine_process_report(
    mut report: RoutineReconfigurationProcessReport,
) -> RoutineReconfigurationProcessReport {
    for position in [
        report.snapshot_position.as_mut(),
        report.learner_applied_position.as_mut(),
        report.membership_position.as_mut(),
    ]
    .into_iter()
    .flatten()
    {
        position.term = 0;
    }
    report
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

fn parse_cell_process_prototype_mode(value: &str) -> Result<CellProcessPrototypeMode, String> {
    match value {
        "correct" => Ok(CellProcessPrototypeMode::Correct),
        "durable_snapshot_pop" => Ok(CellProcessPrototypeMode::DurableSnapshotPop),
        "fresh_learner_repair" => Ok(CellProcessPrototypeMode::FreshLearnerRepair),
        "disable_dedup" => Ok(CellProcessPrototypeMode::DisableDedup),
        "log_only_learner_as_repair" => Ok(CellProcessPrototypeMode::LogOnlyLearnerAsRepair),
        "purge_without_durable_snapshot" => {
            Ok(CellProcessPrototypeMode::PurgeWithoutDurableSnapshot)
        }
        other => Err(format!("unknown cell process prototype mode {other}")),
    }
}

fn parse_cell_concurrent_history_mode(value: &str) -> Result<CellConcurrentHistoryMode, String> {
    match value {
        "correct" | "none" => Ok(CellConcurrentHistoryMode::Correct),
        "omit_hot_read_conflicts" => Ok(CellConcurrentHistoryMode::OmitHotReadConflicts),
        other => Err(format!("unknown cell concurrent history mode {other}")),
    }
}

fn parse_cell_range_phantom_mode(value: &str) -> Result<CellRangePhantomMode, String> {
    match value {
        "correct" | "none" => Ok(CellRangePhantomMode::Correct),
        "omit_range_conflict" => Ok(CellRangePhantomMode::OmitRangeConflict),
        other => Err(format!("unknown cell range phantom mode {other}")),
    }
}

fn parse_cell_read_version_proxy_mode(value: &str) -> Result<CellReadVersionProxyMode, String> {
    match value {
        "correct" | "none" => Ok(CellReadVersionProxyMode::Correct),
        "ignore_session_minimum" => Ok(CellReadVersionProxyMode::IgnoreSessionMinimum),
        other => Err(format!("unknown cell read-version proxy mode {other}")),
    }
}

fn parse_partitioned_resolver_mode(value: &str) -> Result<PartitionedResolverMode, String> {
    match value {
        "correct" | "none" => Ok(PartitionedResolverMode::Correct),
        "start_key_only_routing" => Ok(PartitionedResolverMode::StartKeyOnlyRouting),
        "partial_acceptance" => Ok(PartitionedResolverMode::PartialAcceptance),
        "duplicate_resolver_identity" => Ok(PartitionedResolverMode::DuplicateResolverIdentity),
        "mixed_map_epoch" => Ok(PartitionedResolverMode::MixedMapEpoch),
        "volatile_partition_decision" => Ok(PartitionedResolverMode::VolatilePartitionDecision),
        "skip_prior_finalization" => Ok(PartitionedResolverMode::SkipPriorFinalization),
        "split_with_prepared_transaction" => {
            Ok(PartitionedResolverMode::SplitWithPreparedTransaction)
        }
        other => Err(format!("unknown partitioned resolver mode {other}")),
    }
}

fn parse_stateless_resolver_recovery_mode(
    value: &str,
) -> Result<StatelessResolverRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(StatelessResolverRecoveryMode::Correct),
        "continue_after_resolver_loss" => {
            Ok(StatelessResolverRecoveryMode::ContinueAfterResolverLoss)
        }
        "activate_before_old_fence" => Ok(StatelessResolverRecoveryMode::ActivateBeforeOldFence),
        "accept_old_generation_reply" => {
            Ok(StatelessResolverRecoveryMode::AcceptOldGenerationReply)
        }
        "read_below_recovery_floor" => Ok(StatelessResolverRecoveryMode::ReadBelowRecoveryFloor),
        "publish_unresolved_old_work" => {
            Ok(StatelessResolverRecoveryMode::PublishUnresolvedOldWork)
        }
        "omit_durable_head" => Ok(StatelessResolverRecoveryMode::OmitDurableHead),
        other => Err(format!("unknown stateless resolver recovery mode {other}")),
    }
}

fn parse_stateless_resolver_authenticated_tlog_mode(
    value: &str,
) -> Result<StatelessResolverAuthenticatedTlogMode, String> {
    match value {
        "correct" | "none" => Ok(StatelessResolverAuthenticatedTlogMode::Correct),
        "publish_before_tlog_quorum" => {
            Ok(StatelessResolverAuthenticatedTlogMode::PublishBeforeTlogQuorum)
        }
        "recover_from_authority_marker_only" => {
            Ok(StatelessResolverAuthenticatedTlogMode::RecoverFromAuthorityMarkerOnly)
        }
        "activate_before_tlog_prefix_fence" => {
            Ok(StatelessResolverAuthenticatedTlogMode::ActivateBeforeTlogPrefixFence)
        }
        "accept_old_generation_resolver_reply" => {
            Ok(StatelessResolverAuthenticatedTlogMode::AcceptOldGenerationResolverReply)
        }
        "read_below_authenticated_recovery_floor" => {
            Ok(StatelessResolverAuthenticatedTlogMode::ReadBelowAuthenticatedRecoveryFloor)
        }
        "recover_below_quorum_present_prefix" => {
            Ok(StatelessResolverAuthenticatedTlogMode::RecoverBelowQuorumPresentPrefix)
        }
        "recover_beyond_absent_boundary" => {
            Ok(StatelessResolverAuthenticatedTlogMode::RecoverBeyondAbsentBoundary)
        }
        other => Err(format!(
            "unknown stateless resolver authenticated tLog mode {other}"
        )),
    }
}

fn parse_multi_commit_proxy_mode(value: &str) -> Result<MultiCommitProxyMode, String> {
    match value {
        "correct" | "none" => Ok(MultiCommitProxyMode::Correct),
        "duplicate_commit_version" => Ok(MultiCommitProxyMode::DuplicateCommitVersion),
        "skip_previous_version" => Ok(MultiCommitProxyMode::SkipPreviousVersion),
        "resolver_arrival_order" => Ok(MultiCommitProxyMode::ResolverArrivalOrder),
        "tlog_arrival_order" => Ok(MultiCommitProxyMode::TlogArrivalOrder),
        "mutate_ticketed_batch" => Ok(MultiCommitProxyMode::MutateTicketedBatch),
        "acknowledge_before_all_tlog_sets" => {
            Ok(MultiCommitProxyMode::AcknowledgeBeforeAllTlogSets)
        }
        "accept_stale_proxy_incarnation" => Ok(MultiCommitProxyMode::AcceptStaleProxyIncarnation),
        "omit_progress_frame" => Ok(MultiCommitProxyMode::OmitProgressFrame),
        other => Err(format!("unknown multi commit-proxy mode {other}")),
    }
}

fn parse_commit_proxy_recovery_mode(value: &str) -> Result<CommitProxyRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(CommitProxyRecoveryMode::Correct),
        "continue_same_generation" => Ok(CommitProxyRecoveryMode::ContinueSameGeneration),
        "replace_missing_ticket_with_noop" => {
            Ok(CommitProxyRecoveryMode::ReplaceMissingTicketWithNoop)
        }
        "publish_partial_tlog_durability" => {
            Ok(CommitProxyRecoveryMode::PublishPartialTlogDurability)
        }
        "omit_fully_durable_unknown_result" => {
            Ok(CommitProxyRecoveryMode::OmitFullyDurableUnknownResult)
        }
        "execute_across_missing_predecessor" => {
            Ok(CommitProxyRecoveryMode::ExecuteAcrossMissingPredecessor)
        }
        "trust_incomplete_tlog_inventory" => {
            Ok(CommitProxyRecoveryMode::TrustIncompleteTlogInventory)
        }
        "reuse_old_issued_version" => Ok(CommitProxyRecoveryMode::ReuseOldIssuedVersion),
        "accept_fenced_generation_reply" => {
            Ok(CommitProxyRecoveryMode::AcceptFencedGenerationReply)
        }
        "duplicate_unknown_result_mutation" => {
            Ok(CommitProxyRecoveryMode::DuplicateUnknownResultMutation)
        }
        other => Err(format!(
            "unknown commit-proxy generation-recovery mode {other}"
        )),
    }
}

fn parse_transaction_system_recovery_curve_mode(
    value: &str,
) -> Result<TransactionSystemRecoveryCurveMode, String> {
    match value {
        "correct" | "none" => Ok(TransactionSystemRecoveryCurveMode::Correct),
        "scan_permanent_database" => Ok(TransactionSystemRecoveryCurveMode::ScanPermanentDatabase),
        "trust_one_tlog_set" => Ok(TransactionSystemRecoveryCurveMode::TrustOneTlogSet),
        "quadratic_inventory_scan" => {
            Ok(TransactionSystemRecoveryCurveMode::QuadraticInventoryScan)
        }
        "resume_before_role_recruitment" => {
            Ok(TransactionSystemRecoveryCurveMode::ResumeBeforeRoleRecruitment)
        }
        other => Err(format!(
            "unknown transaction-system recovery curve mode {other}"
        )),
    }
}

fn parse_online_resolver_split_mode(value: &str) -> Result<OnlineResolverSplitMode, String> {
    match value {
        "correct" | "none" => Ok(OnlineResolverSplitMode::Correct),
        "cutover_before_shadow_catchup" => Ok(OnlineResolverSplitMode::CutoverBeforeShadowCatchup),
        "omit_source_history_entry" => Ok(OnlineResolverSplitMode::OmitSourceHistoryEntry),
        "mix_map_epoch_replies" => Ok(OnlineResolverSplitMode::MixMapEpochReplies),
        "accept_retired_source_reply" => Ok(OnlineResolverSplitMode::AcceptRetiredSourceReply),
        "route_to_one_split_child" => Ok(OnlineResolverSplitMode::RouteToOneSplitChild),
        "stale_proxy_map" => Ok(OnlineResolverSplitMode::StaleProxyMap),
        "activate_before_cutover_tlog_quorum" => {
            Ok(OnlineResolverSplitMode::ActivateBeforeCutoverTlogQuorum)
        }
        "mutate_split_descriptor" => Ok(OnlineResolverSplitMode::MutateSplitDescriptor),
        other => Err(format!("unknown online resolver split mode {other}")),
    }
}

fn parse_resolver_hotspot_curve_mode(value: &str) -> Result<ResolverHotspotCurveMode, String> {
    match value {
        "correct" | "none" => Ok(ResolverHotspotCurveMode::Correct),
        "route_crossing_to_one_child" => Ok(ResolverHotspotCurveMode::RouteCrossingToOneChild),
        "mutate_split_workload" => Ok(ResolverHotspotCurveMode::MutateSplitWorkload),
        "skip_outcome_validation" => Ok(ResolverHotspotCurveMode::SkipOutcomeValidation),
        "include_worker_startup" => Ok(ResolverHotspotCurveMode::IncludeWorkerStartup),
        "serialize_split_children" => Ok(ResolverHotspotCurveMode::SerializeSplitChildren),
        other => Err(format!("unknown resolver hotspot curve mode {other}")),
    }
}

fn parse_resolver_hotspot_distribution(value: &str) -> Result<ResolverHotspotDistribution, String> {
    match value {
        "balanced-independent"
        | "control-workload-drift"
        | "control-unvalidated"
        | "control-startup-timed"
        | "control-serialized-children" => Ok(ResolverHotspotDistribution::BalancedIndependent),
        "missed-hot-key-boundary" => Ok(ResolverHotspotDistribution::MissedHotKeyBoundary),
        "crossing-25" | "control-one-child" => Ok(ResolverHotspotDistribution::Crossing25),
        "crossing-100" => Ok(ResolverHotspotDistribution::Crossing100),
        other => Err(format!("unknown resolver hotspot distribution {other}")),
    }
}

fn parse_cell_objectification_mode(value: &str) -> Result<CellObjectificationMode, String> {
    match value {
        "correct" => Ok(CellObjectificationMode::Correct),
        "publish_incomplete_closure" => Ok(CellObjectificationMode::PublishIncompleteClosure),
        "trust_object_frontier_for_safe_pop" => {
            Ok(CellObjectificationMode::TrustObjectFrontierForSafePop)
        }
        other => Err(format!("unknown cell objectification mode {other}")),
    }
}

fn parse_cell_serving_recovery_mode(value: &str) -> Result<CellServingRecoveryMode, String> {
    match value {
        "correct" | "none" => Ok(CellServingRecoveryMode::Correct),
        "ignore_retained_suffix" => Ok(CellServingRecoveryMode::IgnoreRetainedSuffix),
        other => Err(format!("unknown cell serving-recovery mode {other}")),
    }
}

fn parse_cell_serving_authority_feed_mode(
    value: &str,
) -> Result<CellServingAuthorityFeedMode, String> {
    match value {
        "correct" | "none" => Ok(CellServingAuthorityFeedMode::Correct),
        "drop_final_envelope" => Ok(CellServingAuthorityFeedMode::DropFinalEnvelope),
        other => Err(format!("unknown cell serving-authority mode {other}")),
    }
}

fn parse_cell_serving_tagged_tlog_mode(value: &str) -> Result<CellServingTaggedTlogMode, String> {
    match value {
        "correct" | "none" => Ok(CellServingTaggedTlogMode::Correct),
        "omit_required_range_tag" => Ok(CellServingTaggedTlogMode::OmitRequiredRangeTag),
        other => Err(format!("unknown cell serving tagged-tlog mode {other}")),
    }
}

fn parse_range_serving_handoff_mode(value: &str) -> Result<RangeServingHandoffMode, String> {
    match value {
        "correct" | "none" => Ok(RangeServingHandoffMode::Correct),
        "publish_replacement_before_old_worker" => {
            Ok(RangeServingHandoffMode::PublishReplacementBeforeOldWorker)
        }
        "omit_intermediate_tail" => Ok(RangeServingHandoffMode::OmitIntermediateTail),
        "tamper_certificate" => Ok(RangeServingHandoffMode::TamperCertificate),
        "stale_policy_epoch" => Ok(RangeServingHandoffMode::StalePolicyEpoch),
        "wrong_expected_prior_root" => Ok(RangeServingHandoffMode::WrongExpectedPriorRoot),
        "skip_authority_failover" => Ok(RangeServingHandoffMode::SkipAuthorityFailover),
        "ignore_pinned_old_root" => Ok(RangeServingHandoffMode::IgnorePinnedOldRoot),
        "reuse_stale_mark_epoch" => Ok(RangeServingHandoffMode::ReuseStaleMarkEpoch),
        "retire_permit_before_delete" => Ok(RangeServingHandoffMode::RetirePermitBeforeDelete),
        "reuse_stale_authority_snapshot" => {
            Ok(RangeServingHandoffMode::ReuseStaleAuthoritySnapshot)
        }
        "fallback_to_stale_authority_when_unavailable" => {
            Ok(RangeServingHandoffMode::FallbackToStaleAuthorityWhenUnavailable)
        }
        other => Err(format!("unknown range serving handoff mode {other}")),
    }
}

fn parse_range_serving_concurrency_mode(
    value: &str,
) -> Result<RangeServingConcurrencyMode, String> {
    match value {
        "correct" | "none" => Ok(RangeServingConcurrencyMode::Correct),
        "accept_stale_rollback" => Ok(RangeServingConcurrencyMode::AcceptStaleRollback),
        "skip_reader_overlap" => Ok(RangeServingConcurrencyMode::SkipReaderOverlap),
        "accept_mixed_result" => Ok(RangeServingConcurrencyMode::AcceptMixedResult),
        "skip_stale_probe" => Ok(RangeServingConcurrencyMode::SkipStaleProbe),
        other => Err(format!("unknown range serving concurrency mode {other}")),
    }
}

fn parse_range_read_process_mode(value: &str) -> Result<RangeReadProcessMode, String> {
    match value {
        "correct" | "none" => Ok(RangeReadProcessMode::Correct),
        "accept_stale_route" => Ok(RangeReadProcessMode::AcceptStaleRoute),
        "accept_crossing_scan" => Ok(RangeReadProcessMode::AcceptCrossingScan),
        "accept_wrong_value" => Ok(RangeReadProcessMode::AcceptWrongValue),
        "skip_worker_kill" => Ok(RangeReadProcessMode::SkipWorkerKill),
        "route_refresh_fixture" => Ok(RangeReadProcessMode::RouteRefreshFixture),
        other => Err(format!("unknown range-read process mode {other}")),
    }
}

fn parse_range_route_refresh_mode(value: &str) -> Result<RangeRouteRefreshMode, String> {
    match value {
        "correct" | "none" => Ok(RangeRouteRefreshMode::Correct),
        "keep_stale_map" => Ok(RangeRouteRefreshMode::KeepStaleMap),
        "change_snapshot_version" => Ok(RangeRouteRefreshMode::ChangeSnapshotVersion),
        "skip_second_range" => Ok(RangeRouteRefreshMode::SkipSecondRange),
        other => Err(format!("unknown range-route refresh mode {other}")),
    }
}

fn parse_postgres_page_read_process_mode(
    value: &str,
) -> Result<PostgresPageReadProcessMode, String> {
    match value {
        "correct" | "none" => Ok(PostgresPageReadProcessMode::Correct),
        "missing_page" => Ok(PostgresPageReadProcessMode::MissingPage),
        "corrupt_payload" => Ok(PostgresPageReadProcessMode::CorruptPayload),
        "change_objectkv_version" => Ok(PostgresPageReadProcessMode::ChangeObjectKvVersion),
        "page_lsn_ahead" => Ok(PostgresPageReadProcessMode::PageLsnAhead),
        other => Err(format!("unknown PostgreSQL page-read process mode {other}")),
    }
}

fn parse_postgres_page_write_gate_mode(value: &str) -> Result<PostgresPageWriteGateMode, String> {
    match value {
        "correct" | "none" => Ok(PostgresPageWriteGateMode::Correct),
        "wal_behind_page" => Ok(PostgresPageWriteGateMode::WalBehindPage),
        "zero_objectkv_version" => Ok(PostgresPageWriteGateMode::ZeroObjectKvVersion),
        "oversized_batch" => Ok(PostgresPageWriteGateMode::OversizedBatch),
        "accept_wrong_digest" => Ok(PostgresPageWriteGateMode::AcceptWrongDigest),
        other => Err(format!("unknown PostgreSQL page-write gate mode {other}")),
    }
}

fn parse_postgres_page_commit_process_mode(
    value: &str,
) -> Result<PostgresPageCommitProcessMode, String> {
    match value {
        "correct" | "none" => Ok(PostgresPageCommitProcessMode::Correct),
        "omit_extent_mutation" => Ok(PostgresPageCommitProcessMode::OmitExtentMutation),
        "change_retry_identity" => Ok(PostgresPageCommitProcessMode::ChangeRetryIdentity),
        "wrong_receipt_identity" => Ok(PostgresPageCommitProcessMode::WrongReceiptIdentity),
        "non_advancing_commit_version" => {
            Ok(PostgresPageCommitProcessMode::NonAdvancingCommitVersion)
        }
        other => Err(format!(
            "unknown PostgreSQL page-commit process mode {other}"
        )),
    }
}

fn parse_postgres_object_delta_mode(value: &str) -> Result<PostgresObjectDeltaMode, String> {
    match value {
        "correct" | "none" | "restart_from_full_base_plus_object_deltas_plus_hot_tail" => {
            Ok(PostgresObjectDeltaMode::Correct)
        }
        "remove_selected_delta_object" => Ok(PostgresObjectDeltaMode::MissingObject),
        "corrupt_selected_delta_object" => Ok(PostgresObjectDeltaMode::CorruptObject),
        "change_prior_log_chain_digest" => Ok(PostgresObjectDeltaMode::BrokenChain),
        "omit_delta_from_stable_publication_closure" => Ok(PostgresObjectDeltaMode::OmittedClosure),
        "pop_txlog_beyond_selected_object_frontier" => Ok(PostgresObjectDeltaMode::PopAhead),
        "write_replacement_full_base_sst" => Ok(PostgresObjectDeltaMode::FullBaseRewrite),
        other => Err(format!("unknown PostgreSQL object-delta mode {other}")),
    }
}

fn parse_postgres_worker_readiness_mode(
    value: &str,
) -> Result<PostgresWorkerReadinessMode, String> {
    match value {
        "correct" | "none" => Ok(PostgresWorkerReadinessMode::Correct),
        "changed_manifest" => Ok(PostgresWorkerReadinessMode::ChangedManifest),
        "changed_delta" => Ok(PostgresWorkerReadinessMode::ChangedDelta),
        "skip_closure_audit" => Ok(PostgresWorkerReadinessMode::SkipClosureAudit),
        other => Err(format!(
            "unknown PostgreSQL replacement-worker mode {other}"
        )),
    }
}

fn parse_range_cache_fault_mode(value: &str) -> Result<RangeCacheFaultMode, String> {
    match value {
        "correct" | "none" => Ok(RangeCacheFaultMode::Correct),
        "skip_overwrite_injection" => Ok(RangeCacheFaultMode::SkipOverwriteInjection),
        "skip_torn_write_injection" => Ok(RangeCacheFaultMode::SkipTornWriteInjection),
        "accept_wrong_value_after_overwrite" => {
            Ok(RangeCacheFaultMode::AcceptWrongValueAfterOverwrite)
        }
        "accept_wrong_value_after_torn_write" => {
            Ok(RangeCacheFaultMode::AcceptWrongValueAfterTornWrite)
        }
        other => Err(format!("unknown range cache-fault mode {other}")),
    }
}

fn parse_range_cache_eviction_mode(value: &str) -> Result<RangeCacheEvictionMode, String> {
    match value {
        "correct" | "none" => Ok(RangeCacheEvictionMode::Correct),
        "disable_physical_bound" => Ok(RangeCacheEvictionMode::DisablePhysicalBound),
        "skip_reread" => Ok(RangeCacheEvictionMode::SkipReread),
        "accept_wrong_value" => Ok(RangeCacheEvictionMode::AcceptWrongValue),
        other => Err(format!("unknown range cache-eviction mode {other}")),
    }
}

fn default_range_cache_eviction_config(
    seed: u64,
    mode: RangeCacheEvictionMode,
) -> RangeCacheEvictionConfig {
    RangeCacheEvictionConfig {
        backend: RangeCacheEvictionBackend::Local,
        range_count: 8,
        keys_per_range: 8,
        value_bytes: 32 * 1_024,
        cache_limit_bytes: 3 * 64 * 1_024,
        cache_part_bytes: 64 * 1_024,
        decoded_cache_bytes: 64 * 1_024,
        seed,
        mode,
    }
}

fn parse_cell_commit_visibility_mode(value: &str) -> Result<CellCommitVisibilityMode, String> {
    match value {
        "correct" | "none" => Ok(CellCommitVisibilityMode::Correct),
        "acknowledge_after_one_log_set" => Ok(CellCommitVisibilityMode::AcknowledgeAfterOneLogSet),
        "authenticated_correct" => Ok(CellCommitVisibilityMode::AuthenticatedCorrect),
        "unsigned_node_list" => Ok(CellCommitVisibilityMode::UnsignedNodeList),
        "duplicate_attestation" => Ok(CellCommitVisibilityMode::DuplicateAttestation),
        "wrong_log_set_attestation" => Ok(CellCommitVisibilityMode::WrongLogSetAttestation),
        "tampered_statement" => Ok(CellCommitVisibilityMode::TamperedStatement),
        "obsolete_policy_epoch" => Ok(CellCommitVisibilityMode::ObsoletePolicyEpoch),
        other => Err(format!("unknown cell commit visibility mode {other}")),
    }
}

fn parse_cell_tagged_log_lag_ratekeeping_mode(
    value: &str,
) -> Result<CellTaggedLogLagRatekeepingMode, String> {
    match value {
        "correct" | "none" => Ok(CellTaggedLogLagRatekeepingMode::Correct),
        "ratekeep_after_partial_append" => {
            Ok(CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend)
        }
        "best_node_capacity" => Ok(CellTaggedLogLagRatekeepingMode::BestNodeCapacity),
        "stale_capacity_sample" => Ok(CellTaggedLogLagRatekeepingMode::StaleCapacitySample),
        "pop_beyond_object_frontier" => {
            Ok(CellTaggedLogLagRatekeepingMode::PopBeyondObjectFrontier)
        }
        "resume_without_pop_quorum" => Ok(CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum),
        "allocate_before_ratekeeping" => {
            Ok(CellTaggedLogLagRatekeepingMode::AllocateBeforeRatekeeping)
        }
        other => Err(format!(
            "unknown cell tagged-log lag ratekeeping mode {other}"
        )),
    }
}

fn parse_cell_tagged_log_learner_repair_mode(
    value: &str,
) -> Result<CellTaggedLogLearnerRepairMode, String> {
    match value {
        "correct" | "none" => Ok(CellTaggedLogLearnerRepairMode::Correct),
        "single_source" => Ok(CellTaggedLogLearnerRepairMode::SingleSource),
        "tampered_snapshot" => Ok(CellTaggedLogLearnerRepairMode::TamperedSnapshot),
        "stale_ready" => Ok(CellTaggedLogLearnerRepairMode::StaleReady),
        "wrong_learner_incarnation" => Ok(CellTaggedLogLearnerRepairMode::WrongLearnerIncarnation),
        "count_unpromoted_learner" => Ok(CellTaggedLogLearnerRepairMode::CountUnpromotedLearner),
        "duplicate_live_identity" => Ok(CellTaggedLogLearnerRepairMode::DuplicateLiveIdentity),
        other => Err(format!(
            "unknown cell tagged-log learner repair mode {other}"
        )),
    }
}

fn parse_cell_tagged_log_chunked_repair_mode(
    value: &str,
) -> Result<CellTaggedLogChunkedRepairMode, String> {
    match value {
        "correct" | "none" => Ok(CellTaggedLogChunkedRepairMode::Correct),
        "volatile_chunk_resume" => Ok(CellTaggedLogChunkedRepairMode::VolatileChunkResume),
        "missing_chunk" => Ok(CellTaggedLogChunkedRepairMode::MissingChunk),
        "conflicting_chunk_retry" => Ok(CellTaggedLogChunkedRepairMode::ConflictingChunkRetry),
        "tail_gap" => Ok(CellTaggedLogChunkedRepairMode::TailGap),
        "stale_readiness" => Ok(CellTaggedLogChunkedRepairMode::StaleReadiness),
        "count_uncaught_up_learner" => Ok(CellTaggedLogChunkedRepairMode::CountUncaughtUpLearner),
        "full_recopy_tail" => Ok(CellTaggedLogChunkedRepairMode::FullRecopyTail),
        other => Err(format!(
            "unknown cell tagged-log chunked repair mode {other}"
        )),
    }
}

fn parse_cell_tagged_log_policy_transition_mode(
    value: &str,
) -> Result<CellTaggedLogPolicyTransitionMode, String> {
    match value {
        "correct" | "none" => Ok(CellTaggedLogPolicyTransitionMode::Correct),
        "missing_repair_readiness" => Ok(CellTaggedLogPolicyTransitionMode::MissingRepairReadiness),
        "unresolved_old_stage" => Ok(CellTaggedLogPolicyTransitionMode::UnresolvedOldStage),
        "invalid_next_policy" => Ok(CellTaggedLogPolicyTransitionMode::InvalidNextPolicy),
        "mixed_policy_quorum" => Ok(CellTaggedLogPolicyTransitionMode::MixedPolicyQuorum),
        "missing_authority_activation" => {
            Ok(CellTaggedLogPolicyTransitionMode::MissingAuthorityActivation)
        }
        "removed_node_rejoin" => Ok(CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin),
        "double_transition" => Ok(CellTaggedLogPolicyTransitionMode::DoubleTransition),
        other => Err(format!(
            "unknown cell tagged-log policy transition mode {other}"
        )),
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

fn parse_staged_head_generation_mode(value: &str) -> Result<StagedHeadGenerationMode, String> {
    match value {
        "correct" | "none" => Ok(StagedHeadGenerationMode::Correct),
        "takeover_during_recovery" => Ok(StagedHeadGenerationMode::TakeoverDuringRecovery),
        "missing_log_certificate" => Ok(StagedHeadGenerationMode::MissingLogCertificate),
        "tampered_envelope_expectation" => {
            Ok(StagedHeadGenerationMode::TamperedEnvelopeExpectation)
        }
        "skip_staged_head" => Ok(StagedHeadGenerationMode::SkipStagedHead),
        "rewrite_staged_head_generation" => {
            Ok(StagedHeadGenerationMode::RewriteStagedHeadGeneration)
        }
        other => Err(format!("unknown staged-head generation mode {other}")),
    }
}

fn parse_incomplete_staged_head_abort_mode(
    value: &str,
) -> Result<IncompleteStagedHeadAbortMode, String> {
    match value {
        "correct" | "none" => Ok(IncompleteStagedHeadAbortMode::Correct),
        "abort_during_recovery" => Ok(IncompleteStagedHeadAbortMode::AbortDuringRecovery),
        "single_absence_signer" => Ok(IncompleteStagedHeadAbortMode::SingleAbsenceSigner),
        "missing_log_set_fence" => Ok(IncompleteStagedHeadAbortMode::MissingLogSetFence),
        "forged_absence_over_present_record" => {
            Ok(IncompleteStagedHeadAbortMode::ForgedAbsenceOverPresentRecord)
        }
        "volatile_fence_after_restart" => {
            Ok(IncompleteStagedHeadAbortMode::VolatileFenceAfterRestart)
        }
        "reuse_aborted_sequence_or_chain" => {
            Ok(IncompleteStagedHeadAbortMode::ReuseAbortedSequenceOrChain)
        }
        other => Err(format!("unknown incomplete staged-head abort mode {other}")),
    }
}

fn parse_multi_record_staged_prefix_mode(
    value: &str,
) -> Result<MultiRecordStagedPrefixMode, String> {
    match value {
        "correct" | "none" => Ok(MultiRecordStagedPrefixMode::Correct),
        "publish_beyond_absent_boundary" => {
            Ok(MultiRecordStagedPrefixMode::PublishBeyondAbsentBoundary)
        }
        "abort_quorum_present_record" => Ok(MultiRecordStagedPrefixMode::AbortQuorumPresentRecord),
        "skip_recoverable_prefix_record" => {
            Ok(MultiRecordStagedPrefixMode::SkipRecoverablePrefixRecord)
        }
        "retain_dependent_suffix" => Ok(MultiRecordStagedPrefixMode::RetainDependentSuffix),
        "accept_over_limit_window" => Ok(MultiRecordStagedPrefixMode::AcceptOverLimitWindow),
        "missing_log_set_inventory" => Ok(MultiRecordStagedPrefixMode::MissingLogSetInventory),
        other => Err(format!("unknown multi-record staged-prefix mode {other}")),
    }
}

fn parse_routine_reconfiguration_process_mode(
    value: &str,
) -> Result<RoutineReconfigurationProcessMode, String> {
    match value {
        "correct" | "none" => Ok(RoutineReconfigurationProcessMode::Correct),
        other => Err(format!(
            "unknown routine reconfiguration process mode {other}"
        )),
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

fn parse_snapshot_lease_process_mode(value: &str) -> Result<SnapshotLeaseProcessMode, String> {
    match value {
        "correct" | "none" => Ok(SnapshotLeaseProcessMode::Correct),
        "disable_request_dedup" => Ok(SnapshotLeaseProcessMode::DisableRequestDedup),
        "accept_backdated_lease" => Ok(SnapshotLeaseProcessMode::AcceptBackdatedLease),
        "omit_lease_root_epoch" => Ok(SnapshotLeaseProcessMode::OmitLeaseRootEpoch),
        "ignore_collection_range_epoch" => Ok(SnapshotLeaseProcessMode::IgnoreCollectionRangeEpoch),
        "advance_collection_without_publication" => {
            Ok(SnapshotLeaseProcessMode::AdvanceCollectionWithoutPublication)
        }
        "ignore_collection_input_root" => Ok(SnapshotLeaseProcessMode::IgnoreCollectionInputRoot),
        other => Err(format!("unknown snapshot lease process mode {other}")),
    }
}

fn parse_mvcc_gc_authority_composition_mode(
    value: &str,
) -> Result<MvccGcAuthorityCompositionMode, String> {
    match value {
        "correct" | "none" => Ok(MvccGcAuthorityCompositionMode::Correct),
        "omit_output_sst" => Ok(MvccGcAuthorityCompositionMode::OmitOutputSst),
        "semantic_digest_as_manifest" => {
            Ok(MvccGcAuthorityCompositionMode::SemanticDigestAsManifest)
        }
        "skip_authority_failover" => Ok(MvccGcAuthorityCompositionMode::SkipAuthorityFailover),
        other => Err(format!(
            "unknown MVCC GC authority composition mode {other}"
        )),
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

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_lines)]
fn run_staged_head_generation_takeover(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "staged-head generation takeover requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "generation-authority+transaction-openraft+signed-certificate-state";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "staged-head generation takeover requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_staged_head_generation_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut exact_replay = true;
    let mut authority_starts = 0_u64;
    let mut data_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut fence_signers = 0_u64;
    let mut recovery_signers = 0_u64;
    let mut log_certificates = 0_u64;
    let mut takeover_attempts = 0_u64;
    let mut takeover_commits = 0_u64;
    let mut takeover_retries = 0_u64;
    let mut old_publish_rejections = 0_u64;
    let mut baseline_frontier = 0_u64;
    let mut staged_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut successor_version = 0_u64;
    let mut final_generation = 0_u64;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let started = Instant::now();
        let first = match run_staged_head_generation_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_staged_head_generation_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        data_starts = data_starts.saturating_add(first.data_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        fence_signers = fence_signers.saturating_add(first.fence_certificate_signers);
        recovery_signers = recovery_signers.saturating_add(first.recovery_certificate_signers);
        log_certificates = log_certificates.saturating_add(first.tagged_log_certificates);
        takeover_attempts = takeover_attempts.saturating_add(first.takeover_attempts);
        takeover_commits = takeover_commits.saturating_add(first.takeover_commits);
        takeover_retries = takeover_retries.saturating_add(first.takeover_retries);
        old_publish_rejections =
            old_publish_rejections.saturating_add(first.fenced_old_publish_rejections);
        baseline_frontier = baseline_frontier.saturating_add(first.baseline_frontier);
        staged_version = staged_version.saturating_add(first.staged_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        successor_version = successor_version.saturating_add(first.successor_version);
        final_generation = final_generation.saturating_add(first.final_generation);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-staged-head-generation-takeover-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "generation_takeover" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "certified-staged-head-generation-takeover"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: started.elapsed().as_secs_f64(),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "certified-staged-head-generation-takeover"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-staged-head-generation://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries_exercised = checks == seed_count.saturating_mul(35)
        && authority_starts == seed_count.saturating_mul(3)
        && data_starts == seed_count.saturating_mul(6)
        && process_kills == seed_count
        && authority_failovers == seed_count
        && learner_additions == seed_count.saturating_mul(3)
        && membership_changes == seed_count
        && fence_signers == seed_count.saturating_mul(3)
        && recovery_signers == seed_count.saturating_mul(3)
        && baseline_frontier == seed_count.saturating_mul(10)
        && staged_version == seed_count.saturating_mul(11)
        && if mode == StagedHeadGenerationMode::Correct {
            log_certificates == seed_count.saturating_mul(2)
                && takeover_attempts == seed_count.saturating_mul(2)
                && takeover_commits == seed_count
                && takeover_retries == seed_count
                && old_publish_rejections == seed_count.saturating_mul(2)
                && observed_frontier == seed_count.saturating_mul(12)
                && successor_version == seed_count.saturating_mul(12)
                && final_generation == seed_count.saturating_mul(2)
        } else {
            true
        };
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "staged-head generation takeover gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "staged_head_generation.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "staged_head_generation.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, authority_starts={authority_starts}, data_starts={data_starts}, kills={process_kills}, failovers={authority_failovers}, learners={learner_additions}, membership={membership_changes}, fence_signers={fence_signers}, recovery_signers={recovery_signers}, log_certificates={log_certificates}, takeover_attempts={takeover_attempts}, takeover_commits={takeover_commits}, takeover_retries={takeover_retries}, old_publish_rejections={old_publish_rejections}, baseline={baseline_frontier}, staged={staged_version}, observed={observed_frontier}, successor={successor_version}, generation={final_generation}"
                )),
            },
            HardGateResult {
                id: "staged_head_generation.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("staged_head_generation.checks".to_owned(), bounded_count(checks)),
            (
                "staged_head_generation.process_starts".to_owned(),
                bounded_count(authority_starts.saturating_add(data_starts)),
            ),
            (
                "staged_head_generation.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "staged_head_generation.log_certificates".to_owned(),
                bounded_count(log_certificates),
            ),
            (
                "staged_head_generation.takeover_commits".to_owned(),
                bounded_count(takeover_commits),
            ),
            (
                "staged_head_generation.observed_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_incomplete_staged_head_abort(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "incomplete staged-head abort requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "generation-authority+transaction-openraft+tagged-tlog-fence-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "incomplete staged-head abort requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_incomplete_staged_head_abort_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut exact_replay = true;
    let mut authority_starts = 0_u64;
    let mut data_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut tlog_appends = 0_u64;
    let mut fence_attestations = 0_u64;
    let mut absence_attestations = 0_u64;
    let mut tlog_restarts = 0_u64;
    let mut late_append_attempts = 0_u64;
    let mut late_append_rejections = 0_u64;
    let mut abort_attempts = 0_u64;
    let mut abort_commits = 0_u64;
    let mut abort_retries = 0_u64;
    let mut baseline_frontier = 0_u64;
    let mut aborted_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut successor_version = 0_u64;
    let mut final_generation = 0_u64;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let started = Instant::now();
        let first = match run_incomplete_staged_head_abort_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_incomplete_staged_head_abort_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        data_starts = data_starts.saturating_add(first.data_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        tlog_appends = tlog_appends.saturating_add(first.tagged_log_appends);
        fence_attestations = fence_attestations.saturating_add(first.tagged_log_fence_attestations);
        absence_attestations =
            absence_attestations.saturating_add(first.tagged_log_absence_attestations);
        tlog_restarts = tlog_restarts.saturating_add(first.tagged_log_restarts);
        late_append_attempts = late_append_attempts.saturating_add(first.late_append_attempts);
        late_append_rejections =
            late_append_rejections.saturating_add(first.late_append_rejections);
        abort_attempts = abort_attempts.saturating_add(first.abort_attempts);
        abort_commits = abort_commits.saturating_add(first.abort_commits);
        abort_retries = abort_retries.saturating_add(first.abort_retries);
        baseline_frontier = baseline_frontier.saturating_add(first.baseline_frontier);
        aborted_version = aborted_version.saturating_add(first.aborted_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        successor_version = successor_version.saturating_add(first.successor_version);
        final_generation = final_generation.saturating_add(first.final_generation);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-incomplete-staged-head-abort-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "unsafe_staged_abort" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "incomplete-staged-head-fence-and-abort"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: started.elapsed().as_secs_f64(),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "incomplete-staged-head-fence-and-abort"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-incomplete-staged-abort://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let basic_process_boundaries = checks == seed_count.saturating_mul(44)
        && authority_starts == seed_count.saturating_mul(3)
        && data_starts == seed_count.saturating_mul(6)
        && tlog_starts == seed_count.saturating_mul(6)
        && authority_failovers == seed_count
        && learner_additions == seed_count.saturating_mul(3)
        && membership_changes == seed_count
        && tlog_restarts == seed_count
        && late_append_attempts == seed_count.saturating_mul(2)
        && abort_attempts == seed_count.saturating_mul(2)
        && abort_retries == seed_count
        && baseline_frontier == seed_count.saturating_mul(10)
        && aborted_version == seed_count.saturating_mul(11);
    let correct_receipt = mode != IncompleteStagedHeadAbortMode::Correct
        || (process_kills == seed_count.saturating_mul(2)
            && tlog_appends == seed_count.saturating_mul(3)
            && fence_attestations == seed_count.saturating_mul(6)
            && absence_attestations == seed_count.saturating_mul(3)
            && late_append_rejections == seed_count.saturating_mul(2)
            && abort_commits == seed_count
            && observed_frontier == seed_count.saturating_mul(12)
            && successor_version == seed_count.saturating_mul(12)
            && final_generation == seed_count.saturating_mul(2));
    let process_boundaries_exercised = basic_process_boundaries && correct_receipt;
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "incomplete staged-head abort gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "incomplete_staged_abort.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "incomplete_staged_abort.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, authority_starts={authority_starts}, data_starts={data_starts}, tlog_starts={tlog_starts}, kills={process_kills}, failovers={authority_failovers}, learners={learner_additions}, membership={membership_changes}, appends={tlog_appends}, fence_attestations={fence_attestations}, absence_attestations={absence_attestations}, tlog_restarts={tlog_restarts}, late_attempts={late_append_attempts}, late_rejections={late_append_rejections}, abort_attempts={abort_attempts}, abort_commits={abort_commits}, abort_retries={abort_retries}, baseline={baseline_frontier}, aborted={aborted_version}, observed={observed_frontier}, successor={successor_version}, generation={final_generation}"
                )),
            },
            HardGateResult {
                id: "incomplete_staged_abort.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "incomplete_staged_abort.checks".to_owned(),
                bounded_count(checks),
            ),
            (
                "incomplete_staged_abort.process_starts".to_owned(),
                bounded_count(
                    authority_starts
                        .saturating_add(data_starts)
                        .saturating_add(tlog_starts),
                ),
            ),
            (
                "incomplete_staged_abort.tlog_appends".to_owned(),
                bounded_count(tlog_appends),
            ),
            (
                "incomplete_staged_abort.fence_attestations".to_owned(),
                bounded_count(fence_attestations),
            ),
            (
                "incomplete_staged_abort.absence_attestations".to_owned(),
                bounded_count(absence_attestations),
            ),
            (
                "incomplete_staged_abort.abort_commits".to_owned(),
                bounded_count(abort_commits),
            ),
            (
                "incomplete_staged_abort.successor_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_multi_record_staged_prefix_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "multi-record staged-prefix recovery requires at least one seed".to_owned(),
        ));
    }
    let expected_backend =
        "generation-authority+transaction-openraft+tagged-tlog-prefix-fence-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "multi-record staged-prefix recovery requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_multi_record_staged_prefix_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut exact_replay = true;
    let mut authority_starts = 0_u64;
    let mut data_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut staged_records = 0_u64;
    let mut staged_bytes = 0_u64;
    let mut tlog_appends = 0_u64;
    let mut fence_attestations = 0_u64;
    let mut inventory_observations = 0_u64;
    let mut tlog_restarts = 0_u64;
    let mut late_append_attempts = 0_u64;
    let mut late_append_rejections = 0_u64;
    let mut recovery_attempts = 0_u64;
    let mut recovery_commits = 0_u64;
    let mut recovery_retries = 0_u64;
    let mut recovered_records = 0_u64;
    let mut aborted_records = 0_u64;
    let mut baseline_frontier = 0_u64;
    let mut recovered_frontier = 0_u64;
    let mut successor_frontier = 0_u64;
    let mut final_generation = 0_u64;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let started = Instant::now();
        let first = match run_multi_record_staged_prefix_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_multi_record_staged_prefix_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        data_starts = data_starts.saturating_add(first.data_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        staged_records = staged_records.saturating_add(first.staged_records);
        staged_bytes = staged_bytes.saturating_add(first.staged_bytes);
        tlog_appends = tlog_appends.saturating_add(first.tagged_log_appends);
        fence_attestations = fence_attestations.saturating_add(first.prefix_fence_attestations);
        inventory_observations =
            inventory_observations.saturating_add(first.inventory_observations);
        tlog_restarts = tlog_restarts.saturating_add(first.tagged_log_restarts);
        late_append_attempts = late_append_attempts.saturating_add(first.late_append_attempts);
        late_append_rejections =
            late_append_rejections.saturating_add(first.late_append_rejections);
        recovery_attempts = recovery_attempts.saturating_add(first.recovery_attempts);
        recovery_commits = recovery_commits.saturating_add(first.recovery_commits);
        recovery_retries = recovery_retries.saturating_add(first.recovery_retries);
        recovered_records = recovered_records.saturating_add(first.recovered_records);
        aborted_records = aborted_records.saturating_add(first.aborted_records);
        baseline_frontier = baseline_frontier.saturating_add(first.baseline_frontier);
        recovered_frontier = recovered_frontier.saturating_add(first.recovered_frontier);
        successor_frontier = successor_frontier.saturating_add(first.successor_frontier);
        final_generation = final_generation.saturating_add(first.final_generation);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-multi-record-staged-prefix-recovery-v0"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "unsafe_prefix_recovery"
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
                    ("operation", "multi-record-staged-prefix-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: started.elapsed().as_secs_f64(),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "multi-record-staged-prefix-recovery"),
                    ("backend", backend),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-staged-prefix://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let basic_process_boundaries = checks == seed_count.saturating_mul(56)
        && authority_starts == seed_count.saturating_mul(3)
        && data_starts == seed_count.saturating_mul(6)
        && tlog_starts == seed_count.saturating_mul(6)
        && authority_failovers == seed_count
        && learner_additions == seed_count.saturating_mul(3)
        && membership_changes == seed_count
        && tlog_restarts == seed_count
        && late_append_attempts == seed_count.saturating_mul(2)
        && recovery_attempts == seed_count.saturating_mul(2)
        && recovery_retries == seed_count
        && baseline_frontier == seed_count.saturating_mul(10);
    let correct_receipt = mode != MultiRecordStagedPrefixMode::Correct
        || (process_kills == seed_count.saturating_mul(2)
            && staged_records == seed_count.saturating_mul(4)
            && staged_bytes > 0
            && tlog_appends == seed_count.saturating_mul(13)
            && fence_attestations == seed_count.saturating_mul(6)
            && inventory_observations == seed_count.saturating_mul(24)
            && late_append_rejections == seed_count.saturating_mul(2)
            && recovery_commits == seed_count
            && recovered_records == seed_count.saturating_mul(2)
            && aborted_records == seed_count.saturating_mul(2)
            && recovered_frontier == seed_count.saturating_mul(12)
            && successor_frontier == seed_count.saturating_mul(15)
            && final_generation == seed_count.saturating_mul(2));
    let process_boundaries_exercised = basic_process_boundaries && correct_receipt;
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "multi-record staged-prefix gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "multi_record_prefix.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "multi_record_prefix.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, authority_starts={authority_starts}, data_starts={data_starts}, tlog_starts={tlog_starts}, kills={process_kills}, failovers={authority_failovers}, learners={learner_additions}, membership={membership_changes}, staged_records={staged_records}, staged_bytes={staged_bytes}, appends={tlog_appends}, fence_attestations={fence_attestations}, inventory_observations={inventory_observations}, tlog_restarts={tlog_restarts}, late_attempts={late_append_attempts}, late_rejections={late_append_rejections}, recovery_attempts={recovery_attempts}, recovery_commits={recovery_commits}, recovery_retries={recovery_retries}, recovered_records={recovered_records}, aborted_records={aborted_records}, baseline={baseline_frontier}, recovered={recovered_frontier}, successor={successor_frontier}, generation={final_generation}"
                )),
            },
            HardGateResult {
                id: "multi_record_prefix.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("multi_record_prefix.checks".to_owned(), bounded_count(checks)),
            (
                "multi_record_prefix.process_starts".to_owned(),
                bounded_count(
                    authority_starts
                        .saturating_add(data_starts)
                        .saturating_add(tlog_starts),
                ),
            ),
            (
                "multi_record_prefix.staged_records".to_owned(),
                bounded_count(staged_records),
            ),
            (
                "multi_record_prefix.staged_bytes".to_owned(),
                bounded_count(staged_bytes),
            ),
            (
                "multi_record_prefix.inventory_observations".to_owned(),
                bounded_count(inventory_observations),
            ),
            (
                "multi_record_prefix.recovered_records".to_owned(),
                bounded_count(recovered_records),
            ),
            (
                "multi_record_prefix.aborted_records".to_owned(),
                bounded_count(aborted_records),
            ),
            (
                "multi_record_prefix.successor_frontier".to_owned(),
                bounded_count(successor_frontier),
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
fn run_routine_reconfiguration_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "routine reconfiguration process workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_routine_reconfiguration_process_mode(control) {
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
    let mut committed_data_writes = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut learner_ready_signers = 0_u64;
    let mut membership_committed_signers = 0_u64;
    let mut rejected_controls = 0_u64;
    let mut exact_replay = true;
    let mut final_state_exact = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_routine_reconfiguration_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_routine_reconfiguration_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= normalized_routine_process_report(first.clone())
            == normalized_routine_process_report(second);
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        data_process_starts = data_process_starts.saturating_add(first.data_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        committed_data_writes = committed_data_writes.saturating_add(first.committed_data_writes);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        learner_ready_signers = learner_ready_signers.saturating_add(first.learner_ready_signers);
        membership_committed_signers =
            membership_committed_signers.saturating_add(first.membership_committed_signers);
        rejected_controls = rejected_controls.saturating_add(first.rejected_controls);
        final_state_exact &= first.generation == 1
            && first.membership_epoch == 1
            && first.active_voters == [202, 203, 204]
            && first.snapshot_position.is_some()
            && first.learner_applied_position.is_some_and(|position| {
                first
                    .snapshot_position
                    .is_some_and(|snapshot| position.index > snapshot.index)
            })
            && first.membership_position.is_some_and(|position| {
                first
                    .learner_applied_position
                    .is_some_and(|ready| position.index > ready.index)
            });
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
                    ("oracle", "routine-reconfiguration-process-v1"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "routine_reconfiguration"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_data_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "same-generation-routine-reconfiguration"),
                    ("result", if exact { "committed" } else { "unsafe-control" }),
                ]),
            },
            Measurement {
                metric: "recovery.membership_epoch",
                value: bounded_count(first.membership_epoch),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "7"),
                    ("transaction_system", "tx-g1"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "routine-voter-reconfiguration"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-routine-reconfiguration-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(19)
        && authority_process_starts == seed_count.saturating_mul(3)
        && data_process_starts == seed_count.saturating_mul(8)
        && process_kills == seed_count.saturating_mul(5)
        && committed_data_writes == seed_count.saturating_mul(3)
        && learner_additions == seed_count
        && membership_changes == seed_count
        && learner_ready_signers == seed_count.saturating_mul(3)
        && membership_committed_signers == seed_count.saturating_mul(3)
        && rejected_controls == seed_count.saturating_mul(5);
    let passed =
        anomaly_count == 0 && exact_replay && semantic_operations_exercised && final_state_exact;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "routine reconfiguration process gate failed: anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, final_state_exact={final_state_exact}; {detail}"
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "routine_reconfiguration_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "routine_reconfiguration_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, authority_starts={authority_process_starts}, data_starts={data_process_starts}, kills={process_kills}, commits={committed_data_writes}, learners={learner_additions}, membership_changes={membership_changes}, ready_signers={learner_ready_signers}, membership_signers={membership_committed_signers}, rejected_controls={rejected_controls}"
                )),
            },
            HardGateResult {
                id: "routine_reconfiguration_process.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && final_state_exact {
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
            (
                "routine_reconfiguration_process.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "routine_reconfiguration_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "routine_reconfiguration_process.rejected_controls".to_owned(),
                bounded_count(rejected_controls),
            ),
            (
                "routine_reconfiguration_process.membership_changes".to_owned(),
                bounded_count(membership_changes),
            ),
            (
                "routine_reconfiguration_process.committed_data_writes".to_owned(),
                bounded_count(committed_data_writes),
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
fn run_snapshot_lease_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "snapshot lease process workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_snapshot_lease_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut failovers = 0_u64;
    let mut dropped_replies = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut exact_retries = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_snapshot_lease_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_snapshot_lease_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        process_starts = process_starts.saturating_add(first.authority_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        failovers = failovers.saturating_add(first.authority_failovers);
        dropped_replies = dropped_replies.saturating_add(first.dropped_replies);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        exact_retries = exact_retries.saturating_add(first.exact_retries);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(mismatch) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}: {mismatch}"));
        }
        let exact = first.anomaly_count == 0;
        let common = [
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("result", if exact { "accepted" } else { "anomaly" }),
        ];
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "snapshot-lease-authority-process-v1"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "snapshot_lease_authority"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "snapshot_lease.active",
                value: bounded_count(first.final_active_leases),
                attributes: attributes(&common),
            },
            Measurement {
                metric: "snapshot_lease.minimum_readable_version",
                value: bounded_count(first.final_minimum_readable_version),
                attributes: attributes(&common),
            },
            Measurement {
                metric: "snapshot_lease.clock_tick",
                value: bounded_count(first.final_clock_tick),
                attributes: attributes(&common),
            },
            Measurement {
                metric: "mvcc_gc.prepared_jobs",
                value: bounded_count(first.final_prepared_jobs),
                attributes: attributes(&common),
            },
            Measurement {
                metric: "mvcc_gc.collected_through",
                value: bounded_count(first.final_collected_through),
                attributes: attributes(&common),
            },
            Measurement {
                metric: "mvcc_gc.root_epoch",
                value: bounded_count(first.final_root_epoch),
                attributes: attributes(&common),
            },
        ]);
        artifact_refs.push(format!(
            "okv-snapshot-lease-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = mode == SnapshotLeaseProcessMode::Correct
        && check_count == seed_count.saturating_mul(14)
        && process_starts == seed_count.saturating_mul(7)
        && process_kills == seed_count.saturating_mul(4)
        && failovers == seed_count.saturating_mul(4)
        && dropped_replies == seed_count.saturating_mul(3)
        && recovered_outcomes == seed_count.saturating_mul(3)
        && exact_retries == seed_count.saturating_mul(3);
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        format!(
            "snapshot lease authority gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    let mut hard_gates = vec![
        HardGateResult {
            id: "snapshot_lease_process.exact_fresh_process_replay".to_owned(),
            status: if exact_replay {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: None,
        },
        HardGateResult {
            id: "snapshot_lease_process.semantic_operations_exercised".to_owned(),
            status: if semantic_operations_exercised {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            detail: Some(format!(
                "checks={check_count}, starts={process_starts}, kills={process_kills}, failovers={failovers}, dropped={dropped_replies}, recovered={recovered_outcomes}, exact_retries={exact_retries}"
            )),
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
                "snapshot_lease_process.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "snapshot_lease_process.failovers".to_owned(),
                bounded_count(failovers),
            ),
            (
                "snapshot_lease_process.dropped_replies".to_owned(),
                bounded_count(dropped_replies),
            ),
            (
                "snapshot_lease_process.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_mvcc_gc_authority_composition_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+slatedb-mvcc-gc+openraft-publication-authority";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "MVCC GC authority composition requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "MVCC GC authority composition requires at least three seeds".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_mvcc_gc_authority_composition_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_starts = 0_u64;
    let mut authority_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut collector_starts = 0_u64;
    let mut exact_semantic_replay = true;
    let mut collector_boundary_truthful = true;
    let mut serving_root_binding_exact = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first =
            match runtime.block_on(run_mvcc_gc_authority_composition(*seed, mode, &executable)) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        let replay =
            match runtime.block_on(run_mvcc_gc_authority_composition(*seed, mode, &executable)) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_semantic_replay &= first.trace_sha256 == replay.trace_sha256;
        collector_boundary_truthful &= first.collector_process_boundary;
        serving_root_binding_exact &= first.serving_root_binding;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        authority_kills = authority_kills.saturating_add(first.authority_process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        collector_starts = collector_starts.saturating_add(first.collector_process_starts);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(mismatch) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}: {mismatch}"));
        }
        let result = if first.anomaly_count == 0 {
            "accepted"
        } else {
            "anomaly"
        };
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "mvcc-gc-authority-physical-composition-v1"),
                    (
                        "anomaly.class",
                        if first.anomaly_count == 0 {
                            "none"
                        } else {
                            mode.id()
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "mvcc_gc.collected_through",
                value: bounded_count(first.final_collected_through),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("result", result),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-mvcc-gc-authority-composition://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let authority_topology_exact = authority_starts == seed_count.saturating_mul(3)
        && collector_starts == seed_count
        && if mode == MvccGcAuthorityCompositionMode::SkipAuthorityFailover {
            authority_kills == 0 && authority_failovers == 0
        } else {
            authority_kills == seed_count && authority_failovers == seed_count
        };
    let negative_control_detected =
        mode == MvccGcAuthorityCompositionMode::Correct || anomaly_count > 0;
    let correct_contract = mode == MvccGcAuthorityCompositionMode::Correct
        && anomaly_count == 0
        && check_count == seed_count.saturating_mul(10)
        && aggregate_checks.values().all(|passed| *passed);
    let passed = correct_contract
        && exact_semantic_replay
        && authority_topology_exact
        && collector_boundary_truthful
        && serving_root_binding_exact;
    let error = (!passed).then(|| {
        format!(
            "MVCC GC authority composition discarded mode={}: anomalies={anomaly_count}, replay={exact_semantic_replay}, topology={authority_topology_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    let mut hard_gates = vec![
        gate("mvcc_gc_authority.minimum_seeds", seeds.len() >= 3),
        gate(
            "mvcc_gc_authority.semantic_replay_exact",
            exact_semantic_replay,
        ),
        gate(
            "mvcc_gc_authority.authority_topology_exact",
            authority_topology_exact,
        ),
        gate(
            "mvcc_gc_authority.collector_boundary_truthful",
            collector_boundary_truthful,
        ),
        gate(
            "mvcc_gc_authority.serving_root_binding_exact",
            serving_root_binding_exact,
        ),
        gate(
            "mvcc_gc_authority.negative_control_detected",
            negative_control_detected,
        ),
    ];
    hard_gates.extend(aggregate_checks.iter().map(|(id, passed)| HardGateResult {
        id: format!("mvcc_gc_authority.{id}"),
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
                "mvcc_gc_authority.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "mvcc_gc_authority.failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "mvcc_gc_authority.collector_process_starts".to_owned(),
                bounded_count(collector_starts),
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

fn run_slatedb_phase0_coordinator_fencing(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB coordinator fencing workload requires a dataset".to_owned(),
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| {
                format!("SlateDB coordinator fencing profile requires integer {name}")
            })?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let process_binary = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            return execution_from_result(Err(format!(
                "resolve coordinator fencing executable: {error}"
            )));
        }
    };
    let config = match (
        parameter("overwrite_rounds"),
        parameter("fencing_timeout_millis"),
        parameter("completion_timeout_millis"),
    ) {
        (Ok(overwrite_rounds), Ok(fencing_timeout_millis), Ok(completion_timeout_millis)) => {
            Phase0CoordinatorFencingConfig {
                logical_bytes: dataset.logical_bytes,
                key_count: dataset.key_count,
                overwrite_rounds,
                seeds: dataset.seeds.clone(),
                fencing_timeout_millis,
                completion_timeout_millis,
                process_binary,
            }
        }
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return execution_from_result(Err(error));
        }
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0CoordinatorFencingMode::Correct,
        Some("kill_stale_coordinator") => Phase0CoordinatorFencingMode::KillStaleCoordinator,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB coordinator fencing negative control {other}"
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
                "build SlateDB coordinator fencing runtime: {error}"
            )));
        }
    };
    let report = match runtime.block_on(run_phase0_coordinator_fencing_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_coordinator_fencing_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_coordinator_fencing_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0CoordinatorFencingReport,
) -> WorkloadExecution {
    const LANE: &str = "slatedb-coordinator-fencing";
    const ORACLE: &str = "deterministic-slate-coordinator-fencing-v1";
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut controller_io = Phase0IoDelta::default();
    let dataset_class = format!("local-fs-{}-bytes", report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "compaction.coordinator_fence_duration",
            value: seed.second_epoch_to_first_exit_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                (
                    "result",
                    if seed.first_coordinator_self_fenced {
                        "pass"
                    } else {
                        "fail"
                    },
                ),
            ]),
        });
        measurements.push(Measurement {
            metric: "recovery.first_correct_read_duration",
            value: seed.reopen_open.elapsed_seconds + seed.first_correct_read.elapsed_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", &dataset_class),
                ("result", if report.passed() { "pass" } else { "fail" }),
            ]),
        });
        for phase in [
            &seed.ingest,
            &seed.coordinator_fencing,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.full_verify,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, LANE, phase);
            total_operations += phase.logical_operations;
        }
        merge_counts(
            &mut controller_io.successful_requests,
            &seed.total_io_observed_by_controller.successful_requests,
        );
        merge_counts(
            &mut controller_io.failed_requests,
            &seed.total_io_observed_by_controller.failed_requests,
        );
        merge_counts(
            &mut controller_io.read_bytes,
            &seed.total_io_observed_by_controller.read_bytes,
        );
        merge_counts(
            &mut controller_io.written_bytes,
            &seed.total_io_observed_by_controller.written_bytes,
        );
    }
    add_phase0_object_measurements(
        &mut measurements,
        workload,
        backend,
        LANE,
        &report.store,
        &controller_io,
    );
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
    let artifact_path = phase0_coordinator_fencing_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_json_artifact(&artifact_path, report, "coordinator fencing");
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB coordinator fencing failed gates: {}",
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
                "phase0.coordinator_fencing.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
            (
                "phase0.coordinator_fencing.self_fenced".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.first_coordinator_self_fenced)
                        .count() as u64,
                ),
            ),
            (
                "phase0.coordinator_fencing.completed".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.second_coordinator_completed_compaction)
                        .count() as u64,
                ),
            ),
            (
                "phase0.coordinator_fencing.controller_requests.total".to_owned(),
                bounded_count(controller_io.request_total()),
            ),
        ]),
    }
}

fn run_slatedb_phase0_orphan_gc(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
    backend: &str,
) -> WorkloadExecution {
    let Some(dataset) = dataset else {
        return execution_from_result(Err(
            "SlateDB orphan GC workload requires a dataset".to_owned()
        ));
    };
    let parameter = |name: &str| -> Result<u64, String> {
        let value = profile
            .parameters
            .get(name)
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| format!("SlateDB orphan GC profile requires integer {name}"))?;
        u64::try_from(value).map_err(|error| format!("invalid {name}: {error}"))
    };
    let process_binary = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            return execution_from_result(Err(format!("resolve orphan GC executable: {error}")));
        }
    };
    let config = match (
        parameter("overwrite_rounds"),
        parameter("compacted_timeout_millis"),
        parameter("completion_timeout_millis"),
    ) {
        (Ok(overwrite_rounds), Ok(compacted_timeout_millis), Ok(completion_timeout_millis)) => {
            Phase0OrphanGcConfig {
                logical_bytes: dataset.logical_bytes,
                key_count: dataset.key_count,
                overwrite_rounds,
                seeds: dataset.seeds.clone(),
                compacted_timeout_millis,
                completion_timeout_millis,
                process_binary,
            }
        }
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return execution_from_result(Err(error));
        }
    };
    let mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
    {
        None | Some("none") => Phase0OrphanGcMode::Correct,
        Some("dry_run_orphan_deletion") => Phase0OrphanGcMode::DryRunOrphanDeletion,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown SlateDB orphan GC negative control {other}"
            )));
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return execution_from_result(Err(format!("build SlateDB orphan GC runtime: {error}")));
        }
    };
    let report = match runtime.block_on(run_phase0_orphan_gc_contract(&config, mode)) {
        Ok(report) => report,
        Err(error) => return execution_from_result(Err(error)),
    };
    phase0_orphan_gc_execution(workload, run_id, candidate_commit, backend, &report)
}

#[allow(clippy::too_many_lines)]
fn phase0_orphan_gc_execution(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    backend: &str,
    report: &Phase0OrphanGcReport,
) -> WorkloadExecution {
    const LANE: &str = "slatedb-orphan-gc";
    const ORACLE: &str = "deterministic-slate-orphan-gc-v1";
    let mut measurements = Vec::new();
    let mut total_operations = 0_u64;
    let mut controller_io = Phase0IoDelta::default();
    let dataset_class = format!("local-fs-{}-bytes", report.logical_bytes);
    for seed in &report.seeds {
        measurements.push(Measurement {
            metric: "gc.orphan_collection_duration",
            value: seed.orphan_gc_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("result", if seed.orphan_deleted { "pass" } else { "fail" }),
            ]),
        });
        measurements.push(Measurement {
            metric: "recovery.first_correct_read_duration",
            value: seed.reopen_open.elapsed_seconds + seed.first_correct_read.elapsed_seconds,
            attributes: attributes(&[
                ("lane", LANE),
                ("workload", &workload.id),
                ("backend", backend),
                ("dataset.class", &dataset_class),
                ("result", if report.passed() { "pass" } else { "fail" }),
            ]),
        });
        for phase in [
            &seed.ingest,
            &seed.garbage_collection,
            &seed.reopen_open,
            &seed.first_correct_read,
            &seed.full_verify,
        ] {
            add_phase0_phase_measurements(&mut measurements, workload, backend, LANE, phase);
            total_operations += phase.logical_operations;
        }
        merge_counts(
            &mut controller_io.successful_requests,
            &seed.total_io_observed_by_controller.successful_requests,
        );
        merge_counts(
            &mut controller_io.failed_requests,
            &seed.total_io_observed_by_controller.failed_requests,
        );
        merge_counts(
            &mut controller_io.read_bytes,
            &seed.total_io_observed_by_controller.read_bytes,
        );
        merge_counts(
            &mut controller_io.written_bytes,
            &seed.total_io_observed_by_controller.written_bytes,
        );
    }
    add_phase0_object_measurements(
        &mut measurements,
        workload,
        backend,
        LANE,
        &report.store,
        &controller_io,
    );
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
    let artifact_path = phase0_orphan_gc_artifact_path(run_id, candidate_commit, report);
    let artifact_result = write_json_artifact(&artifact_path, report, "orphan GC");
    let artifact_error = artifact_result.as_ref().err().cloned();
    let error = if failed_gates.is_empty() {
        artifact_error
    } else {
        Some(format!(
            "SlateDB orphan GC failed gates: {}",
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
                "phase0.orphan_gc.correctness.anomalies".to_owned(),
                bounded_count(anomalies),
            ),
            (
                "phase0.orphan_gc.active_outputs_preserved".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.active_output_survived_gc)
                        .count() as u64,
                ),
            ),
            (
                "phase0.orphan_gc.aged_orphans_deleted".to_owned(),
                bounded_count(
                    report
                        .seeds
                        .iter()
                        .filter(|seed| seed.orphan_deleted)
                        .count() as u64,
                ),
            ),
            (
                "phase0.orphan_gc.controller_requests.total".to_owned(),
                bounded_count(controller_io.request_total()),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_read_version_proxy_history_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell read-version proxy history requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_read_version_proxy_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let rounds = match workload
        .parameters
        .get("rounds")
        .and_then(toml::Value::as_integer)
        .unwrap_or(100)
        .try_into()
    {
        Ok(value) if value > 0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "cell read-version proxy round count must be positive".to_owned(),
            ));
        }
        Err(error) => {
            return execution_from_result(Err(format!(
                "invalid cell read-version proxy round count: {error}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut reports: Vec<CellReadVersionProxyReport> = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let first = match run_cell_read_version_proxy_history(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_read_version_proxy_history(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let proxy_requests = reports
        .iter()
        .map(|report| report.proxy_requests)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let handoffs = reports
        .iter()
        .map(|report| report.causal_handoffs)
        .sum::<u64>();
    let read_observations = reports
        .iter()
        .map(|report| report.read_observations)
        .sum::<u64>();
    let minimum_violations = reports
        .iter()
        .map(|report| report.minimum_version_violations)
        .sum::<u64>();
    let stale_values = reports
        .iter()
        .map(|report| report.stale_value_observations)
        .sum::<u64>();
    let checks = reports
        .iter()
        .map(|report| report.executed_checks)
        .sum::<u64>();
    let proxy_process_starts = reports
        .iter()
        .map(|report| report.proxy_process_starts)
        .sum::<u64>();
    let starts = reports
        .iter()
        .map(|report| report.process_starts)
        .sum::<u64>();
    let kills = reports
        .iter()
        .map(|report| report.process_kills)
        .sum::<u64>();
    let generations_exact = reports.iter().all(|report| report.generations_exact);
    let minimum_versions_honored = reports.iter().all(|report| report.minimum_versions_honored);
    let read_your_writes_exact = reports.iter().all(|report| report.read_your_writes_exact);
    let real_time_order_exact = reports.iter().all(|report| report.real_time_order_exact);
    let convergence_exact = reports.iter().all(|report| {
        report.all_nodes_exact && report.envelope_chain_valid && report.restarted_node_converges
    });
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = proxy_requests
        == seed_count.saturating_mul(rounds).saturating_mul(3)
        && commits == seed_count.saturating_mul(rounds)
        && handoffs == seed_count.saturating_mul(rounds)
        && read_observations == seed_count.saturating_mul(rounds)
        && checks == seed_count.saturating_mul(13)
        && proxy_process_starts == seed_count.saturating_mul(2)
        && starts == seed_count.saturating_mul(4)
        && kills == seed_count;
    let passed = anomalies == 0
        && exact_replay
        && semantic_operations_exercised
        && generations_exact
        && minimum_versions_honored
        && read_your_writes_exact
        && real_time_order_exact
        && convergence_exact;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "cell read-version proxy gate failed: mode={}, anomalies={anomalies}, minimum_violations={minimum_violations}, stale_values={stale_values}, exact_replay={exact_replay}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for report in &reports {
        let exact = report.anomaly_count == 0;
        let causal_exact = report.minimum_versions_honored
            && report.read_your_writes_exact
            && report.real_time_order_exact;
        let report_converged = report.all_nodes_exact
            && report.envelope_chain_valid
            && report.restarted_node_converges;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-read-version-proxy-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "stale-read-version" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(report.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("result", if exact { "accepted" } else { "mismatch" }),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.causal_handoffs),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "multi-proxy-real-time"),
                    ("result", if causal_exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if report_converged { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "read-version-proxy-handoff"),
                    ("fault", "leader-kill-after-commit"),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-read-version-proxy://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_read_version_proxy.exact_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_read_version_proxy.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "proxy_processes={proxy_process_starts}, proxy_requests={proxy_requests}, commits={commits}, handoffs={handoffs}, reads={read_observations}"
                )),
            },
            HardGateResult {
                id: "cell_read_version_proxy.generations_exact".to_owned(),
                status: if generations_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_read_version_proxy.minimum_versions_honored".to_owned(),
                status: if minimum_versions_honored {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("minimum_violations={minimum_violations}")),
            },
            HardGateResult {
                id: "cell_read_version_proxy.read_your_writes_exact".to_owned(),
                status: if read_your_writes_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("stale_values={stale_values}")),
            },
            HardGateResult {
                id: "cell_read_version_proxy.real_time_order_exact".to_owned(),
                status: if real_time_order_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_read_version_proxy.convergence_exact".to_owned(),
                status: if convergence_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
        ],
        budget_units: bounded_count(proxy_requests.saturating_add(commits)),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_read_version_proxy.process_starts".to_owned(),
                bounded_count(proxy_process_starts),
            ),
            (
                "cell_read_version_proxy.proxy_requests".to_owned(),
                bounded_count(proxy_requests),
            ),
            (
                "cell_read_version_proxy.commits".to_owned(),
                bounded_count(commits),
            ),
            (
                "cell_read_version_proxy.causal_handoffs".to_owned(),
                bounded_count(handoffs),
            ),
            (
                "cell_read_version_proxy.minimum_violations".to_owned(),
                bounded_count(minimum_violations),
            ),
            (
                "cell_read_version_proxy.stale_values".to_owned(),
                bounded_count(stale_values),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_partitioned_resolver_agreement_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "partitioned resolver agreement requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_partitioned_resolver_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let rounds = match workload
        .parameters
        .get("rounds")
        .and_then(toml::Value::as_integer)
        .unwrap_or(100)
        .try_into()
    {
        Ok(value) if value > 0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "partitioned resolver round count must be positive".to_owned()
            ));
        }
        Err(error) => {
            return execution_from_result(Err(format!(
                "invalid partitioned resolver round count: {error}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut reports: Vec<PartitionedResolverReport> = Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first = match run_partitioned_resolver_contract(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        durations.push(started.elapsed().as_secs_f64());
        let second = match run_partitioned_resolver_contract(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let attempts = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let conflicts = reports
        .iter()
        .map(|report| report.conflict_rejections)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let finalizations = reports
        .iter()
        .map(|report| report.durable_finalizations)
        .sum::<u64>();
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let process_boundaries = reports.iter().all(|report| {
        report.process_starts >= 6 && report.process_starts <= 7 && report.process_restarts <= 1
    });
    let correct_contract = mode != PartitionedResolverMode::Correct
        || (attempts == seed_count.saturating_mul(rounds).saturating_mul(6)
            && anomalies == 0
            && reports.iter().all(|report| {
                report.centralized_and_partitioned_statuses_match
                    && report.centralized_and_partitioned_rows_match
                    && report.envelope_chain_exact
                    && report.crossing_ranges_route_to_every_overlap
                    && report.all_required_partitions_decide
                    && report.resolver_identities_distinct
                    && report.map_epoch_exact
                    && report.decisions_durable_before_ack
                    && report.prior_disposition_order_exact
                    && report.finalization_exact
                    && report.restarted_resolver_replays_exact_decision
            }));
    let negative_detected = mode == PartitionedResolverMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == PartitionedResolverMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "partitioned resolver gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "centralized-cell-v0-conflict-index"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "partitioned-resolver-agreement"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(report.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("result", "committed"),
                ]),
            },
            Measurement {
                metric: "transaction.conflicts",
                value: bounded_count(report.conflict_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("conflict.kind", "ordered-partition-read-write"),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "ordered-partition-agreement"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(report.latest_commit_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0048-bounded-cell"),
                    ("range", "resolver-map-epoch-1"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "partitioned-resolver-agreement"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-partitioned-resolver://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "partitioned_resolver.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "partitioned_resolver.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "attempts={attempts}, commits={commits}, conflicts={conflicts}, decisions={decisions}, finalizations={finalizations}"
                )),
            },
            HardGateResult {
                id: "partitioned_resolver.centralized_oracle_agreement".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "partitioned_resolver.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(attempts),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("partitioned_resolver.attempts".to_owned(), bounded_count(attempts)),
            ("partitioned_resolver.commits".to_owned(), bounded_count(commits)),
            ("partitioned_resolver.conflicts".to_owned(), bounded_count(conflicts)),
            ("partitioned_resolver.decisions".to_owned(), bounded_count(decisions)),
            ("partitioned_resolver.finalizations".to_owned(), bounded_count(finalizations)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_stateless_resolver_recovery_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "stateless resolver recovery requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_stateless_resolver_recovery_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let attempts = 600;
    let batch_size = 8;
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<StatelessResolverRecoveryReport> = Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first = match run_stateless_resolver_recovery_contract(
            *seed,
            attempts,
            batch_size,
            mode,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        durations.push(started.elapsed().as_secs_f64());
        let second = match run_stateless_resolver_recovery_contract(
            *seed,
            attempts,
            batch_size,
            mode,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let attempted = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let conflicts = reports
        .iter()
        .map(|report| report.conflict_rejections)
        .sum::<u64>();
    let safe_false_conflicts = reports
        .iter()
        .map(|report| report.safe_false_conflicts)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let batches = reports
        .iter()
        .map(|report| report.ordered_batches)
        .sum::<u64>();
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let process_boundaries = mode != StatelessResolverRecoveryMode::Correct
        || reports.iter().all(|report| report.process_starts == 9);
    let correct_contract = mode != StatelessResolverRecoveryMode::Correct
        || (attempted == seed_count.saturating_mul(attempts)
            && anomalies == 0
            && safe_false_conflicts >= seed_count
            && reports.iter().all(|report| {
                report.partitioned_commits_subset_of_centralized_oracle
                    && report.centralized_conflicts_rejected
                    && report.rows_match_authoritative_outcomes
                    && report.envelope_chain_exact
                    && report.complete_resolver_agreement
                    && report.resolver_failure_stopped_old_generation
                    && report.old_generation_fenced_before_successor
                    && report.recovery_floor_includes_durable_head
                    && report.successor_resolver_state_started_empty
                    && report.successor_reads_at_or_above_floor
                    && report.old_generation_requests_rejected
                    && report.old_generation_replies_rejected
                    && report.unresolved_old_work_not_visible
                    && report.abandoned_work_retried_with_new_identity
                    && report.generation_fences == 1
                    && report.abandoned_candidates == 1
                    && report.resolver_durable_syncs == 0
                    && report.resolver_finalization_rpcs == 0
            }));
    let negative_detected = mode == StatelessResolverRecoveryMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == StatelessResolverRecoveryMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "stateless resolver recovery gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "centralized-cell-v0-conflict-index"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "stateless-resolver-generation-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(report.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("result", "committed"),
                ]),
            },
            Measurement {
                metric: "transaction.conflicts",
                value: bounded_count(report.conflict_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("conflict.kind", "generation-scoped-resolver"),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "stateless-resolver-generation"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(report.latest_commit_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0049-bounded-cell"),
                    ("range", "resolver-map-epoch-1"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "stateless-resolver-generation-recovery"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-stateless-resolver://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "stateless_resolver.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "stateless_resolver.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "attempts={attempted}, commits={commits}, conflicts={conflicts}, safe_false_conflicts={safe_false_conflicts}, decisions={decisions}, batches={batches}"
                )),
            },
            HardGateResult {
                id: "stateless_resolver.generation_recovery".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "stateless_resolver.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(attempted),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("stateless_resolver.attempts".to_owned(), bounded_count(attempted)),
            ("stateless_resolver.commits".to_owned(), bounded_count(commits)),
            ("stateless_resolver.conflicts".to_owned(), bounded_count(conflicts)),
            (
                "stateless_resolver.safe_false_conflicts".to_owned(),
                bounded_count(safe_false_conflicts),
            ),
            ("stateless_resolver.decisions".to_owned(), bounded_count(decisions)),
            ("stateless_resolver.batches".to_owned(), bounded_count(batches)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_stateless_resolver_authenticated_tlog_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "authenticated tLog resolver recovery requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_stateless_resolver_authenticated_tlog_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<StatelessResolverAuthenticatedTlogReport> =
        Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first =
            match run_stateless_resolver_authenticated_tlog_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        durations.push(started.elapsed().as_secs_f64());
        let second =
            match run_stateless_resolver_authenticated_tlog_contract(*seed, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let appends = reports
        .iter()
        .map(|report| report.tagged_log_appends)
        .sum::<u64>();
    let inventory_observations = reports
        .iter()
        .map(|report| report.inventory_observations)
        .sum::<u64>();
    let recovered = reports
        .iter()
        .map(|report| report.recovered_records)
        .sum::<u64>();
    let aborted = reports
        .iter()
        .map(|report| report.aborted_records)
        .sum::<u64>();
    let process_boundaries = mode != StatelessResolverAuthenticatedTlogMode::Correct
        || reports.iter().all(|report| {
            report.resolver_process_starts == 6
                && report.resolver_process_kills == 6
                && report.resolver_durable_syncs == 0
                && report.resolver_finalization_rpcs == 0
        });
    let correct_contract = mode != StatelessResolverAuthenticatedTlogMode::Correct
        || (anomalies == 0
            && reports.iter().all(|report| {
                report.complete_resolver_evidence_before_stage
                    && report.resolver_acceptance_did_not_publish
                    && report.partial_resolver_candidate_not_staged
                    && report.partial_resolver_candidate_not_visible
                    && report.staged_envelope_bytes_match_tlog_bytes
                    && report.visibility_required_authenticated_quorum
                    && report.every_required_tlog_prefix_fenced
                    && report.authenticated_recovery_prefix_maximal
                    && report.quorum_present_uncertified_record_recovered
                    && report.quorum_absent_suffix_aborted
                    && report.successor_resolver_state_started_empty
                    && report.successor_resolver_floor_exact
                    && report.successor_read_at_or_above_floor
                    && report.old_generation_resolver_request_rejected
                    && report.old_generation_resolver_reply_rejected
                    && report.old_generation_tlog_append_rejected
                    && report.abandoned_work_retried_with_new_identity
                    && report.exact_rows_and_envelopes
                    && report.recovery_frontier == 12
                    && report.successor_frontier == 15
            }));
    let negative_detected = mode == StatelessResolverAuthenticatedTlogMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == StatelessResolverAuthenticatedTlogMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "authenticated tLog resolver recovery gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "authenticated-tlog-prefix"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    (
                        "operation",
                        "stateless-resolver-authenticated-tlog-recovery",
                    ),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "resolver-and-tlog-composition"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(report.successor_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0050-bounded-cell"),
                    ("range", "resolver-map-epoch-1"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    (
                        "operation",
                        "stateless-resolver-authenticated-tlog-recovery",
                    ),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-stateless-resolver-tlog://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }
    let budget_units = reports.iter().fold(0_u64, |total, report| {
        total
            .saturating_add(report.executed_checks)
            .saturating_add(report.resolver_decisions)
            .saturating_add(report.tagged_log_appends)
            .saturating_add(report.inventory_observations)
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "stateless_resolver_tlog.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "stateless_resolver_tlog.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "decisions={decisions}, appends={appends}, inventory_observations={inventory_observations}"
                )),
            },
            HardGateResult {
                id: "stateless_resolver_tlog.authenticated_recovery".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "stateless_resolver_tlog.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(budget_units),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("stateless_resolver_tlog.decisions".to_owned(), bounded_count(decisions)),
            ("stateless_resolver_tlog.appends".to_owned(), bounded_count(appends)),
            (
                "stateless_resolver_tlog.inventory_observations".to_owned(),
                bounded_count(inventory_observations),
            ),
            ("stateless_resolver_tlog.recovered".to_owned(), bounded_count(recovered)),
            ("stateless_resolver_tlog.aborted".to_owned(), bounded_count(aborted)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_commit_proxy_generation_recovery_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "commit-proxy generation recovery requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_commit_proxy_recovery_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<CommitProxyRecoveryReport> = Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first = match run_commit_proxy_generation_recovery_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        durations.push(started.elapsed().as_secs_f64());
        let second = match run_commit_proxy_generation_recovery_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let tickets = reports
        .iter()
        .map(|report| report.sequencer_tickets)
        .sum::<u64>();
    let attempts = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let tlog_syncs = reports
        .iter()
        .map(|report| report.tlog_durable_syncs)
        .sum::<u64>();
    let process_boundaries = reports.iter().all(|report| {
        report.sequencer_nodes == 3
            && report.sequencer_process_starts >= 3
            && report.proxy_process_starts == 12
            && report.proxy_process_deaths == 3
            && report.resolver_process_starts == 12
            && report.tlog_process_starts == 24
            && report.resolver_durable_syncs == 0
            && report.resolver_finalization_rpcs == 0
    });
    let correct_contract = mode != CommitProxyRecoveryMode::Correct
        || (anomalies == 0
            && reports.iter().all(|report| {
                report.transaction_system_generations == 4
                    && report.generation_fences == 3
                    && report.sequencer_tickets == 36
                    && report.attempted_transactions == 144
                    && report.proxy_loss_fenced_complete_transaction_system
                    && report.old_sequencer_fenced
                    && report.old_proxies_fenced
                    && report.old_resolvers_fenced
                    && report.old_tlogs_fenced
                    && report.every_required_tlog_inventory_authenticated
                    && report.recovered_boundary_maximal_contiguous_quorum_prefix
                    && report.pre_resolver_ticket_and_suffix_abandoned
                    && report.partial_tlog_ticket_and_suffix_abandoned
                    && report.fully_durable_unknown_result_preserved
                    && report.unknown_result_resolved_by_stable_request_identity
                    && report.missing_nonempty_ticket_never_replaced_with_noop
                    && report.successors_blocked_by_missing_predecessor
                    && report.successor_generations_exceed_old_issued_high_watermarks
                    && report.versions_unique_across_generations
                    && report.stale_generation_requests_rejected
                    && report.stale_generation_replies_rejected
                    && report.every_transaction_uses_one_generation
                    && report.every_transaction_uses_one_resolver_map_epoch
                    && report.dispositions_match_oracle
                    && report.exact_rows_and_envelopes
                    && report.envelope_chain_valid
                    && report.all_tlog_inventory_roots_exact
                    && report.exact_acknowledgement_set
                    && report.exact_retained_outcomes
            }));
    let negative_detected = mode == CommitProxyRecoveryMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == CommitProxyRecoveryMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "commit-proxy generation-recovery gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "authenticated-generation-prefix"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "commit-proxy-generation-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "proxy-loss-generation-fence"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(report.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0053-bounded-cell"),
                    ("range", "resolver-map-epoch-1"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "commit-proxy-generation-recovery"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-commit-proxy-recovery://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }
    let budget_units = reports.iter().fold(0_u64, |total, report| {
        total
            .saturating_add(report.executed_checks)
            .saturating_add(report.sequencer_tickets)
            .saturating_add(report.resolver_decisions)
            .saturating_add(report.tlog_durable_syncs)
            .saturating_add(report.authenticated_inventory_receipts)
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "commit_proxy_recovery.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "commit_proxy_recovery.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "tickets={tickets}, attempts={attempts}, decisions={decisions}, tlog_syncs={tlog_syncs}"
                )),
            },
            HardGateResult {
                id: "commit_proxy_recovery.authenticated_generation_prefix".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "commit_proxy_recovery.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(budget_units),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("commit_proxy_recovery.tickets".to_owned(), bounded_count(tickets)),
            ("commit_proxy_recovery.attempts".to_owned(), bounded_count(attempts)),
            ("commit_proxy_recovery.commits".to_owned(), bounded_count(commits)),
            ("commit_proxy_recovery.decisions".to_owned(), bounded_count(decisions)),
            ("commit_proxy_recovery.tlog_syncs".to_owned(), bounded_count(tlog_syncs)),
            (
                "commit_proxy_recovery.generation_fences".to_owned(),
                bounded_count(
                    reports
                        .iter()
                        .map(|report| report.generation_fences)
                        .sum(),
                ),
            ),
            (
                "commit_proxy_recovery.abandoned_tickets".to_owned(),
                bounded_count(
                    reports
                        .iter()
                        .map(|report| report.abandoned_tickets)
                        .sum(),
                ),
            ),
            (
                "commit_proxy_recovery.unknown_results".to_owned(),
                bounded_count(
                    reports
                        .iter()
                        .map(|report| report.commit_unknown_results)
                        .sum(),
                ),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_transaction_system_recovery_curve_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "transaction-system recovery curve requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "local-process+replicated-authority+authenticated-tlog-inventory";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "transaction-system recovery curve requires {expected_backend}, got {backend}"
        )));
    }
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_transaction_system_recovery_curve_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let scale_class = match recovery_curve_string(workload, "scale_class") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<TransactionSystemRecoveryCurveReport> = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let config = match recovery_curve_config(workload, *seed, &scale_class) {
            Ok(config) => config,
            Err(error) => return execution_from_result(Err(error)),
        };
        let report = match run_transaction_system_recovery_curve_contract(
            &config,
            mode,
            profile.repeats,
            &executable,
        ) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        reports.push(report);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_samples = usize::try_from(seed_count.saturating_mul(u64::from(profile.repeats)))
        .unwrap_or(usize::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let exact_untimed_receipts = reports.iter().all(|report| report.exact_untimed_receipts);
    let phase_receipts_complete = reports.iter().all(|report| report.phase_receipts_complete);
    let duration_samples = reports
        .iter()
        .flat_map(|report| report.samples.iter().map(|sample| sample.total_seconds))
        .collect::<Vec<_>>();
    let duration_distribution_recorded = duration_samples.len() == expected_samples;
    let semantic_contract = reports.iter().all(|report| {
        let receipt = &report.receipt;
        receipt.old_generation_fenced_before_inventory
            && receipt.every_required_tlog_set_authenticated
            && receipt.recovered_boundary_is_maximal_contiguous_quorum_prefix
            && receipt.all_declared_successor_roles_recruited_before_resume
            && receipt.successor_version_exceeds_old_issued_high_watermark
            && receipt.permanent_database_bytes_read == 0
            && receipt.inventory_scan_is_linear
            && receipt.pending_classification_is_linear
    });
    let negative_detected = mode == TransactionSystemRecoveryCurveMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == TransactionSystemRecoveryCurveMode::Correct
        && anomalies == 0
        && exact_untimed_receipts
        && phase_receipts_complete
        && duration_distribution_recorded
        && semantic_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.config.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "transaction-system recovery curve discarded mode={}: anomalies={anomalies}, exact_untimed_receipts={exact_untimed_receipts}, phase_receipts_complete={phase_receipts_complete}, duration_distribution_recorded={duration_distribution_recorded}, semantic_contract={semantic_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    let mut phase_samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut total_inventory_bytes = 0_u64;
    let mut total_database_bytes = 0_u64;
    let mut total_work_units = 0_u64;
    for report in &reports {
        let exact = report.anomaly_count == 0;
        let repeated_inventory_bytes = report
            .receipt
            .inventory_bytes_examined
            .saturating_mul(u64::from(report.repetitions));
        let repeated_database_bytes = report
            .receipt
            .permanent_database_bytes_read
            .saturating_mul(u64::from(report.repetitions));
        let repeated_work_units = report
            .receipt
            .inventory_work_units
            .saturating_add(report.receipt.pending_work_units)
            .saturating_mul(u64::from(report.repetitions));
        total_inventory_bytes = total_inventory_bytes.saturating_add(repeated_inventory_bytes);
        total_database_bytes = total_database_bytes.saturating_add(repeated_database_bytes);
        total_work_units = total_work_units.saturating_add(repeated_work_units);
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "rfc0054-linear-recovery"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "recovery.inventory_bytes",
                value: bounded_count(repeated_inventory_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("scale.class", &scale_class),
                ]),
            },
            Measurement {
                metric: "recovery.database_bytes_read",
                value: bounded_count(repeated_database_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("scale.class", &scale_class),
                    ("source", "permanent-database"),
                ]),
            },
            Measurement {
                metric: "recovery.work_units",
                value: bounded_count(repeated_work_units),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("scale.class", &scale_class),
                    ("work.kind", "inventory+pending"),
                ]),
            },
        ]);
        for sample in &report.samples {
            measurements.push(Measurement {
                metric: "recovery.transaction_system_duration",
                value: sample.total_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("scale.class", &scale_class),
                    ("result", if exact { "pass" } else { "discard" }),
                ]),
            });
            for (phase, duration) in &sample.phase_seconds {
                phase_samples
                    .entry(phase.clone())
                    .or_default()
                    .push(*duration);
                measurements.push(Measurement {
                    metric: "recovery.phase_duration",
                    value: *duration,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("scale.class", &scale_class),
                        ("phase", phase),
                        ("result", if exact { "pass" } else { "discard" }),
                    ]),
                });
            }
        }
        artifact_refs.push(format!(
            "okv-transaction-system-recovery-curve://{}/{}/{}",
            mode.id(),
            report.config.seed,
            report.trace_sha256
        ));
    }

    let mut secondary_metrics = BTreeMap::from([
        (
            "recovery_curve.samples".to_owned(),
            bounded_count(u64::try_from(duration_samples.len()).unwrap_or(u64::MAX)),
        ),
        (
            "recovery_curve.total_seconds.median".to_owned(),
            median(&duration_samples),
        ),
        (
            "recovery_curve.total_seconds.mad".to_owned(),
            median_absolute_deviation(&duration_samples, median(&duration_samples)),
        ),
        (
            "recovery_curve.total_seconds.min".to_owned(),
            duration_samples
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min),
        ),
        (
            "recovery_curve.total_seconds.max".to_owned(),
            duration_samples.iter().copied().fold(0.0, f64::max),
        ),
        (
            "recovery_curve.inventory_bytes".to_owned(),
            bounded_count(total_inventory_bytes),
        ),
        (
            "recovery_curve.database_bytes_read".to_owned(),
            bounded_count(total_database_bytes),
        ),
        (
            "recovery_curve.work_units".to_owned(),
            bounded_count(total_work_units),
        ),
    ]);
    for (phase, samples) in phase_samples {
        secondary_metrics.insert(
            format!("recovery_curve.phase.{phase}.median"),
            median(&samples),
        );
    }
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "recovery_curve.exact_untimed_receipts".to_owned(),
                status: gate_status(exact_untimed_receipts),
                detail: None,
            },
            HardGateResult {
                id: "recovery_curve.phase_receipts_complete".to_owned(),
                status: gate_status(phase_receipts_complete && duration_distribution_recorded),
                detail: Some(format!(
                    "samples={}, expected={expected_samples}",
                    duration_samples.len()
                )),
            },
            HardGateResult {
                id: "recovery_curve.linear_zero_database_read_contract".to_owned(),
                status: gate_status(semantic_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "recovery_curve.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(total_work_units),
        artifact_refs,
        secondary_metrics,
    }
}

fn recovery_curve_string(workload: &WorkloadConfig, key: &str) -> Result<String, String> {
    workload
        .parameters
        .get(key)
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("workload {} requires string parameter {key}", workload.id))
}

fn recovery_curve_u64(workload: &WorkloadConfig, key: &str) -> Result<u64, String> {
    let value = workload
        .parameters
        .get(key)
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| format!("workload {} requires integer parameter {key}", workload.id))?;
    u64::try_from(value).map_err(|_| {
        format!(
            "workload {} parameter {key} must be non-negative",
            workload.id
        )
    })
}

fn recovery_curve_config(
    workload: &WorkloadConfig,
    seed: u64,
    scale_class: &str,
) -> Result<TransactionSystemRecoveryCurveConfig, String> {
    Ok(TransactionSystemRecoveryCurveConfig {
        seed,
        scale_class: scale_class.to_owned(),
        pending_tickets: recovery_curve_u64(workload, "pending_tickets")?,
        retained_records_per_tlog: recovery_curve_u64(workload, "retained_records_per_tlog")?,
        commit_proxy_roles: recovery_curve_u64(workload, "commit_proxy_roles")?,
        resolver_roles: recovery_curve_u64(workload, "resolver_roles")?,
        tlog_sets: recovery_curve_u64(workload, "tlog_sets")?,
        tlog_nodes_per_set: recovery_curve_u64(workload, "tlog_nodes_per_set")?,
        logical_database_bytes: recovery_curve_u64(workload, "logical_database_bytes")?,
    })
}

#[allow(clippy::too_many_lines)]
fn run_multi_commit_proxy_ordering_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "multi commit-proxy ordering requires at least one seed".to_owned(),
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_multi_commit_proxy_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<MultiCommitProxyReport> = Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first = match run_multi_commit_proxy_ordering_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        durations.push(started.elapsed().as_secs_f64());
        let second = match run_multi_commit_proxy_ordering_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let tickets = reports
        .iter()
        .map(|report| report.sequencer_tickets)
        .sum::<u64>();
    let attempts = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let conflicts = reports
        .iter()
        .map(|report| report.conflict_rejections)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let progress_frames = reports
        .iter()
        .map(|report| report.progress_frames)
        .sum::<u64>();
    let acknowledgements = reports
        .iter()
        .map(|report| report.acknowledged_batches)
        .sum::<u64>();
    let process_boundaries = reports.iter().all(|report| {
        report.sequencer_nodes == 3
            && report.proxy_process_starts == 3
            && report.resolver_process_starts == 3
            && report.tlog_process_starts == 6
            && report.resolver_durable_syncs == 0
            && report.resolver_finalization_rpcs == 0
    });
    let correct_contract = mode != MultiCommitProxyMode::Correct
        || (anomalies == 0
            && reports.iter().all(|report| {
                report.sequencer_tickets == 24
                    && report.attempted_transactions == 96
                    && report.unique_gap_free_ticket_chain
                    && report.authority_marker_binding_exact
                    && report.proxy_signatures_valid
                    && report.proxy_identities_pinned
                    && report.pending_window_bounded
                    && report.all_resolvers_same_order
                    && report.transactions_ordered_inside_batches
                    && report.crossing_ranges_reached_every_overlap
                    && report.dispositions_match_oracle
                    && report.every_batch_has_progress_frame
                    && report.conflict_only_batches_advance
                    && report.all_tlogs_same_order
                    && report.tlog_frames_match_ticketed_batches
                    && report.acknowledgements_require_every_tlog_set
                    && report.later_batches_blocked_by_missing_predecessor
                    && report.stale_proxy_rejected
                    && report.exact_rows_and_envelopes
                    && report.envelope_chain_valid
                    && report.acknowledged_batches == 24
            }));
    let negative_detected = mode == MultiCommitProxyMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == MultiCommitProxyMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "multi commit-proxy ordering gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "global-proxy-batch-order"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "multi-commit-proxy-ordering"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "multi-proxy-global-order"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(report.sequencer_tickets.saturating_mul(4)),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0051-bounded-cell"),
                    ("range", "resolver-map-epoch-1"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "multi-commit-proxy-ordering"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-multi-commit-proxy://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }
    let budget_units = reports.iter().fold(0_u64, |total, report| {
        total
            .saturating_add(report.executed_checks)
            .saturating_add(report.sequencer_tickets)
            .saturating_add(report.resolver_decisions)
            .saturating_add(report.progress_frames)
            .saturating_add(report.tlog_durable_syncs)
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "multi_commit_proxy.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "multi_commit_proxy.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "tickets={tickets}, attempts={attempts}, decisions={decisions}, progress_frames={progress_frames}"
                )),
            },
            HardGateResult {
                id: "multi_commit_proxy.global_order".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "multi_commit_proxy.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(budget_units),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("multi_commit_proxy.tickets".to_owned(), bounded_count(tickets)),
            ("multi_commit_proxy.attempts".to_owned(), bounded_count(attempts)),
            ("multi_commit_proxy.commits".to_owned(), bounded_count(commits)),
            ("multi_commit_proxy.conflicts".to_owned(), bounded_count(conflicts)),
            ("multi_commit_proxy.decisions".to_owned(), bounded_count(decisions)),
            (
                "multi_commit_proxy.progress_frames".to_owned(),
                bounded_count(progress_frames),
            ),
            (
                "multi_commit_proxy.acknowledgements".to_owned(),
                bounded_count(acknowledgements),
            ),
        ]),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KvRuntimeEnvelopeMode {
    Correct,
    ReserveCachePerRange,
    RefuseBeforeCacheEviction,
    IgnoreHardDebtLimit,
    SkipRangeMove,
}

impl KvRuntimeEnvelopeMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "correct" => Ok(Self::Correct),
            "reserve_cache_per_range" => Ok(Self::ReserveCachePerRange),
            "refuse_before_cache_eviction" => Ok(Self::RefuseBeforeCacheEviction),
            "ignore_hard_debt_limit" => Ok(Self::IgnoreHardDebtLimit),
            "skip_range_move" => Ok(Self::SkipRangeMove),
            other => Err(format!("unknown KV Runtime envelope mode {other}")),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ReserveCachePerRange => "reserve_cache_per_range",
            Self::RefuseBeforeCacheEviction => "refuse_before_cache_eviction",
            Self::IgnoreHardDebtLimit => "ignore_hard_debt_limit",
            Self::SkipRangeMove => "skip_range_move",
        }
    }
}

fn kv_runtime_profile_u64(profile: &ProfileConfig, key: &str) -> Result<u64, String> {
    let value = profile
        .parameters
        .get(key)
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| format!("KV Runtime profile requires integer parameter {key}"))?;
    u64::try_from(value).map_err(|_| format!("KV Runtime profile {key} must be non-negative"))
}

fn kv_runtime_profile_usize(profile: &ProfileConfig, key: &str) -> Result<usize, String> {
    usize::try_from(kv_runtime_profile_u64(profile, key)?)
        .map_err(|error| format!("KV Runtime profile {key} is too large: {error}"))
}

fn kv_runtime_workload_usize(workload: &WorkloadConfig, key: &str) -> Result<usize, String> {
    let value = workload
        .parameters
        .get(key)
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| format!("workload {} requires integer parameter {key}", workload.id))?;
    usize::try_from(value).map_err(|_| {
        format!(
            "workload {} parameter {key} must be non-negative and fit usize",
            workload.id
        )
    })
}

fn kv_runtime_with_ranges(
    config: KvRuntimeConfig,
    range_count: usize,
    usage: RangeEngineUsage,
    ram_cache_demand_bytes: u64,
    nvme_cache_demand_bytes: u64,
) -> Result<KvRuntimeDecision, String> {
    let mut runtime = KvRuntime::new(config)?;
    for raw_id in 0..range_count {
        let id = u64::try_from(raw_id)
            .map_err(|error| format!("Range Engine id does not fit u64: {error}"))?;
        runtime.assign_range_engine(RangeEngineId(id), usage)?;
    }
    runtime.set_cache_demand(ram_cache_demand_bytes, nvme_cache_demand_bytes);
    Ok(runtime.pressure_decision())
}

fn kv_runtime_actions_ordered(decision: &KvRuntimeDecision) -> bool {
    decision.actions.windows(2).all(|pair| pair[0] < pair[1])
}

#[allow(clippy::too_many_lines)]
fn run_kv_runtime_resource_envelope(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    let expected_backend = "model+accounted-resource-envelope";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "KV Runtime envelope requires {expected_backend}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "KV Runtime envelope requires at least one seed".to_owned()
        ));
    }
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match KvRuntimeEnvelopeMode::parse(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let range_count = match kv_runtime_workload_usize(workload, "range_engine_count") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_u64(profile, "ram_limit_bytes")?,
            kv_runtime_profile_u64(profile, "nvme_limit_bytes")?,
            kv_runtime_profile_usize(profile, "max_range_engines")?,
            kv_runtime_profile_u64(profile, "metadata_bytes_per_range")?,
            kv_runtime_profile_u64(profile, "recent_mvcc_bytes_per_range")?,
            kv_runtime_profile_u64(profile, "process_ram_cache_request_bytes")?,
            kv_runtime_profile_u64(profile, "process_nvme_cache_request_bytes")?,
            kv_runtime_profile_u64(profile, "soft_objectification_debt_bytes")?,
            kv_runtime_profile_u64(profile, "hard_objectification_debt_bytes")?,
        ))
    })();
    let (
        ram_limit_bytes,
        nvme_limit_bytes,
        max_range_engines,
        metadata_bytes_per_range,
        recent_mvcc_bytes_per_range,
        process_ram_cache_request_bytes,
        process_nvme_cache_request_bytes,
        soft_objectification_debt_bytes,
        hard_objectification_debt_bytes,
    ) = match profile_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let config = KvRuntimeConfig {
        ram_limit_bytes,
        nvme_limit_bytes,
        max_range_engines,
        soft_objectification_debt_bytes,
        hard_objectification_debt_bytes,
    };
    let usage = RangeEngineUsage {
        metadata_bytes: metadata_bytes_per_range,
        recent_mvcc_bytes: recent_mvcc_bytes_per_range,
        objectification_debt_bytes: 0,
    };
    let cache_multiplier = if mode == KvRuntimeEnvelopeMode::ReserveCachePerRange {
        u64::try_from(range_count).unwrap_or(u64::MAX)
    } else {
        1
    };
    let ram_cache_demand_bytes = process_ram_cache_request_bytes.saturating_mul(cache_multiplier);
    let nvme_cache_demand_bytes = process_nvme_cache_request_bytes.saturating_mul(cache_multiplier);
    let density = match kv_runtime_with_ranges(
        config,
        range_count,
        usage,
        ram_cache_demand_bytes,
        nvme_cache_demand_bytes,
    ) {
        Ok(decision) => decision,
        Err(error) => return execution_from_result(Err(error)),
    };

    let cache_config = KvRuntimeConfig {
        ram_limit_bytes: 100,
        nvme_limit_bytes: 1_000,
        max_range_engines: 1,
        soft_objectification_debt_bytes: 100,
        hard_objectification_debt_bytes: 200,
    };
    let mut cache_pressure = match kv_runtime_with_ranges(
        cache_config,
        1,
        RangeEngineUsage {
            metadata_bytes: 20,
            recent_mvcc_bytes: 20,
            objectification_debt_bytes: 0,
        },
        100,
        800,
    ) {
        Ok(decision) => decision,
        Err(error) => return execution_from_result(Err(error)),
    };
    if mode == KvRuntimeEnvelopeMode::RefuseBeforeCacheEviction {
        cache_pressure.admission = KvRuntimeAdmission::Refuse;
        cache_pressure.actions = vec![KvRuntimeAction::RefuseCommit];
    }

    let soft_debt = match kv_runtime_with_ranges(
        config,
        1,
        RangeEngineUsage {
            metadata_bytes: 1,
            recent_mvcc_bytes: 1,
            objectification_debt_bytes: soft_objectification_debt_bytes.saturating_add(1),
        },
        0,
        0,
    ) {
        Ok(decision) => decision,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mut hard_debt = match kv_runtime_with_ranges(
        config,
        1,
        RangeEngineUsage {
            metadata_bytes: 1,
            recent_mvcc_bytes: 1,
            objectification_debt_bytes: hard_objectification_debt_bytes.saturating_add(1),
        },
        0,
        0,
    ) {
        Ok(decision) => decision,
        Err(error) => return execution_from_result(Err(error)),
    };
    if mode == KvRuntimeEnvelopeMode::IgnoreHardDebtLimit {
        hard_debt.admission = KvRuntimeAdmission::Admit;
        hard_debt
            .actions
            .retain(|action| *action != KvRuntimeAction::RefuseCommit);
    }

    let pressure_config = KvRuntimeConfig {
        ram_limit_bytes: 100,
        nvme_limit_bytes: 1_000,
        max_range_engines: 1,
        soft_objectification_debt_bytes: 100,
        hard_objectification_debt_bytes: 200,
    };
    let mut non_evictable_pressure = match kv_runtime_with_ranges(
        pressure_config,
        1,
        RangeEngineUsage {
            metadata_bytes: 60,
            recent_mvcc_bytes: 60,
            objectification_debt_bytes: 0,
        },
        0,
        0,
    ) {
        Ok(decision) => decision,
        Err(error) => return execution_from_result(Err(error)),
    };
    if mode == KvRuntimeEnvelopeMode::SkipRangeMove {
        non_evictable_pressure
            .actions
            .retain(|action| *action != KvRuntimeAction::RequestRangeMove);
    }

    let range_count_u64 = u64::try_from(range_count).unwrap_or(u64::MAX);
    let fixed_per_range = metadata_bytes_per_range.saturating_add(recent_mvcc_bytes_per_range);
    let expected_fixed = fixed_per_range.saturating_mul(range_count_u64);
    let expected_accounted_ram = expected_fixed.saturating_add(
        process_ram_cache_request_bytes.min(ram_limit_bytes.saturating_sub(expected_fixed)),
    );
    let expected_accounted_nvme = process_nvme_cache_request_bytes.min(nvme_limit_bytes);
    let range_engine_count_exact = density.snapshot.range_engine_count == range_count;
    let process_cache_is_shared = density.snapshot.accounted_ram_bytes == expected_accounted_ram
        && density.snapshot.accounted_nvme_bytes == expected_accounted_nvme;
    let fixed_range_accounting_is_linear = density.snapshot.fixed_range_ram_bytes == expected_fixed;
    let accounted_ram_within_limit = density.snapshot.accounted_ram_bytes <= ram_limit_bytes;
    let accounted_nvme_within_limit = density.snapshot.accounted_nvme_bytes <= nvme_limit_bytes;
    let cache_evicted_before_refusal = cache_pressure.admission == KvRuntimeAdmission::Admit
        && cache_pressure.actions == vec![KvRuntimeAction::EvictRamCache]
        && cache_pressure.snapshot.evicted_ram_cache_bytes == 40;
    let soft_debt_rate_limits = soft_debt.admission == KvRuntimeAdmission::RateLimit
        && soft_debt
            .actions
            .contains(&KvRuntimeAction::RequestObjectification)
        && soft_debt.actions.contains(&KvRuntimeAction::RateLimit);
    let hard_debt_refuses = hard_debt.admission == KvRuntimeAdmission::Refuse
        && hard_debt.actions.contains(&KvRuntimeAction::RefuseCommit);
    let non_evictable_requests_objectification = non_evictable_pressure
        .actions
        .contains(&KvRuntimeAction::RequestObjectification);
    let non_evictable_requests_range_move = non_evictable_pressure
        .actions
        .contains(&KvRuntimeAction::RequestRangeMove);
    let pressure_action_order_exact = [
        &density,
        &cache_pressure,
        &soft_debt,
        &hard_debt,
        &non_evictable_pressure,
    ]
    .into_iter()
    .all(kv_runtime_actions_ordered);
    let deterministic_replay = seeds.iter().all(|_| {
        kv_runtime_with_ranges(
            config,
            range_count,
            usage,
            ram_cache_demand_bytes,
            nvme_cache_demand_bytes,
        )
        .is_ok_and(|replay| replay == density)
    });
    let minimum_seeds = seeds.len() >= 3;
    let checks = [
        minimum_seeds,
        range_engine_count_exact,
        process_cache_is_shared,
        fixed_range_accounting_is_linear,
        accounted_ram_within_limit,
        accounted_nvme_within_limit,
        cache_evicted_before_refusal,
        soft_debt_rate_limits,
        hard_debt_refuses,
        non_evictable_requests_objectification,
        non_evictable_requests_range_move,
        pressure_action_order_exact,
        deterministic_replay,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let negative_detected = mode == KvRuntimeEnvelopeMode::Correct || anomalies > 0;
    let passed = mode == KvRuntimeEnvelopeMode::Correct && anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "KV Runtime envelope discarded mode={}: anomalies={anomalies}, negative_detected={negative_detected}",
            mode.id()
        )
    });
    let pressure_class = match density.admission {
        KvRuntimeAdmission::Admit => "normal",
        KvRuntimeAdmission::RateLimit => "rate-limit",
        KvRuntimeAdmission::Refuse => "refuse",
    };
    let range_count_label = range_count.to_string();
    let anomaly_class = if anomalies == 0 { "none" } else { mode.id() };
    let trace = format!(
        "mode={mode:?};density={density:?};cache={cache_pressure:?};soft={soft_debt:?};hard={hard_debt:?};non_evictable={non_evictable_pressure:?};checks={checks:?}"
    );
    let trace_sha256 = sha256(trace.as_bytes());

    WorkloadExecution {
        error,
        measurements: vec![
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(anomalies),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "rfc0056-kv-runtime-envelope"),
                    ("anomaly.class", anomaly_class),
                ]),
            },
            Measurement {
                metric: "kv_runtime.fixed_ram_bytes_per_range",
                value: bounded_count(fixed_per_range),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("range.count", &range_count_label),
                ]),
            },
            Measurement {
                metric: "kv_runtime.accounted_ram_bytes",
                value: bounded_count(density.snapshot.accounted_ram_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("range.count", &range_count_label),
                    ("pressure.class", pressure_class),
                ]),
            },
            Measurement {
                metric: "kv_runtime.accounted_nvme_bytes",
                value: bounded_count(density.snapshot.accounted_nvme_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("range.count", &range_count_label),
                ]),
            },
            Measurement {
                metric: "kv_runtime.cache_evicted_bytes",
                value: bounded_count(density.snapshot.evicted_ram_cache_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("cache.kind", "ram"),
                    ("reason", "process-bound"),
                ]),
            },
            Measurement {
                metric: "kv_runtime.cache_evicted_bytes",
                value: bounded_count(density.snapshot.evicted_nvme_cache_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("cache.kind", "nvme"),
                    ("reason", "process-bound"),
                ]),
            },
            Measurement {
                metric: "kv_runtime.objectification_debt_bytes",
                value: bounded_count(density.snapshot.objectification_debt_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("range.count", &range_count_label),
                    ("pressure.class", pressure_class),
                ]),
            },
        ],
        hard_gates: vec![
            HardGateResult {
                id: "kv_runtime.minimum_seeds".to_owned(),
                status: gate_status(minimum_seeds),
                detail: Some(format!("seeds={}", seeds.len())),
            },
            HardGateResult {
                id: "kv_runtime.range_engine_count_exact".to_owned(),
                status: gate_status(range_engine_count_exact),
                detail: Some(format!(
                    "observed={}, expected={range_count}",
                    density.snapshot.range_engine_count
                )),
            },
            HardGateResult {
                id: "kv_runtime.process_cache_is_shared".to_owned(),
                status: gate_status(process_cache_is_shared),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.fixed_range_accounting_is_linear".to_owned(),
                status: gate_status(fixed_range_accounting_is_linear),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.accounted_ram_within_limit".to_owned(),
                status: gate_status(accounted_ram_within_limit),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.accounted_nvme_within_limit".to_owned(),
                status: gate_status(accounted_nvme_within_limit),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.cache_evicted_before_refusal".to_owned(),
                status: gate_status(cache_evicted_before_refusal),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.soft_debt_rate_limits".to_owned(),
                status: gate_status(soft_debt_rate_limits),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.hard_debt_refuses".to_owned(),
                status: gate_status(hard_debt_refuses),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.non_evictable_requests_objectification".to_owned(),
                status: gate_status(non_evictable_requests_objectification),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.non_evictable_requests_range_move".to_owned(),
                status: gate_status(non_evictable_requests_range_move),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.pressure_action_order_exact".to_owned(),
                status: gate_status(pressure_action_order_exact),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.deterministic_replay".to_owned(),
                status: gate_status(deterministic_replay),
                detail: None,
            },
            HardGateResult {
                id: "kv_runtime.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(
            range_count_u64
                .saturating_mul(u64::try_from(seeds.len()).unwrap_or(u64::MAX))
                .saturating_add(4),
        ),
        artifact_refs: vec![format!(
            "okv-kv-runtime-envelope://{}/{range_count}/{trace_sha256}",
            mode.id()
        )],
        secondary_metrics: BTreeMap::from([
            (
                "kv_runtime.range_engine_count".to_owned(),
                bounded_count(range_count_u64),
            ),
            (
                "kv_runtime.fixed_range_ram_bytes".to_owned(),
                bounded_count(density.snapshot.fixed_range_ram_bytes),
            ),
            (
                "kv_runtime.accounted_ram_bytes".to_owned(),
                bounded_count(density.snapshot.accounted_ram_bytes),
            ),
            (
                "kv_runtime.accounted_nvme_bytes".to_owned(),
                bounded_count(density.snapshot.accounted_nvme_bytes),
            ),
            (
                "kv_runtime.objectification_debt_bytes".to_owned(),
                bounded_count(density.snapshot.objectification_debt_bytes),
            ),
        ]),
    }
}

fn parse_kv_runtime_density_mode(value: &str) -> Result<KvRuntimeDensityMode, String> {
    match value {
        "correct" | "none" => Ok(KvRuntimeDensityMode::Correct),
        "substitute_accounted_rss" => Ok(KvRuntimeDensityMode::SubstituteAccountedRss),
        "claim_private_caches_are_shared" => Ok(KvRuntimeDensityMode::ClaimPrivateCachesAreShared),
        "reuse_warm_handle" => Ok(KvRuntimeDensityMode::ReuseWarmHandle),
        "omit_safety_receipt" => Ok(KvRuntimeDensityMode::OmitSafetyReceipt),
        other => Err(format!("unknown KV Runtime density mode {other}")),
    }
}

fn parse_kv_runtime_density_topology(value: &str) -> Result<KvRuntimeDensityTopology, String> {
    match value {
        "one-db-logical-ranges" => Ok(KvRuntimeDensityTopology::OneDbLogicalRanges),
        "many-db-shared-cache" => Ok(KvRuntimeDensityTopology::ManyDbSharedCache),
        "many-db-private-cache" => Ok(KvRuntimeDensityTopology::ManyDbPrivateCache),
        other => Err(format!("unknown KV Runtime density topology {other}")),
    }
}

fn run_kv_runtime_density_child(
    executable: &Path,
    config: &KvRuntimeDensityWorkerConfig,
    mode: KvRuntimeDensityMode,
) -> Result<KvRuntimeDensityReceipt, String> {
    let config_json = serde_json::to_string(config)
        .map_err(|error| format!("serialize KV Runtime density config: {error}"))?;
    let mut child = Command::new(executable)
        .arg("kv-runtime-density-node")
        .arg("--config-json")
        .arg(config_json)
        .arg("--mode")
        .arg(mode.id())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn KV Runtime density child: {error}"))?;
    let deadline = Duration::from_millis(config.timeout_millis.saturating_add(5_000));
    let wait_started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("collect KV Runtime density child: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "KV Runtime density child failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                return serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse KV Runtime density receipt: {error}"));
            }
            Ok(None) if wait_started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "KV Runtime density child exceeded controller timeout of {} ms",
                    deadline.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("poll KV Runtime density child: {error}"));
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn kv_runtime_density_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &KvRuntimeDensityReceipt,
) -> Vec<Measurement> {
    let target = receipt.target_range_engines.to_string();
    let topology = receipt.topology.as_str();
    let completed = u64::try_from(receipt.completed_range_engines).unwrap_or(u64::MAX);
    let rss_per_range = if completed == 0 {
        0.0
    } else {
        bounded_count(receipt.incremental_peak_rss_bytes) / bounded_count(completed)
    };
    let completion_ratio = bounded_usize(receipt.completed_range_engines)
        / bounded_usize(receipt.target_range_engines.max(1));
    let successful_requests = receipt
        .object_io
        .successful_requests
        .values()
        .copied()
        .sum::<u64>();
    let failed_requests = receipt
        .object_io
        .failed_requests
        .values()
        .copied()
        .sum::<u64>();
    let common = |phase: &str| {
        attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("backend", backend),
            ("topology", topology),
            ("range.count", &target),
            ("phase", phase),
        ])
    };
    let mut measurements = vec![
        Measurement {
            metric: "kv_runtime.physical_rss_bytes_per_range",
            value: rss_per_range,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
            ]),
        },
        Measurement {
            metric: "kv_runtime.density_completion_ratio",
            value: completion_ratio,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("stop.reason", &receipt.stop_reason),
            ]),
        },
        Measurement {
            metric: "kv_runtime.database_instances",
            value: bounded_usize(receipt.database_instances),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
            ]),
        },
        Measurement {
            metric: "kv_runtime.cache_instances",
            value: bounded_usize(receipt.decoded_cache_instances),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
                ("cache.kind", "decoded-ram"),
            ]),
        },
        Measurement {
            metric: "kv_runtime.cache_instances",
            value: bounded_usize(receipt.nvme_cache_instances),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
                ("cache.kind", "nvme"),
            ]),
        },
    ];

    for (phase, sample) in [
        ("baseline", &receipt.baseline),
        ("resident", &receipt.resident),
        ("after-close", &receipt.after_close),
    ] {
        measurements.extend([
            Measurement {
                metric: "kv_runtime.physical_rss_bytes",
                value: bounded_count(sample.rss_bytes),
                attributes: common(phase),
            },
            Measurement {
                metric: "kv_runtime.runtime_tasks",
                value: bounded_usize(sample.runtime_tasks),
                attributes: common(phase),
            },
            Measurement {
                metric: "kv_runtime.os_threads",
                value: bounded_count(sample.os_threads),
                attributes: common(phase),
            },
            Measurement {
                metric: "kv_runtime.open_file_descriptors",
                value: bounded_count(sample.open_file_descriptors),
                attributes: common(phase),
            },
        ]);
    }

    for (tier, files, bytes) in [
        (
            "object-fixture",
            receipt.object_files,
            receipt.object_file_bytes,
        ),
        (
            "nvme-cache",
            receipt.nvme_cache_files,
            receipt.nvme_cache_file_bytes,
        ),
    ] {
        measurements.extend([
            Measurement {
                metric: "kv_runtime.local_files",
                value: bounded_count(files),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("topology", topology),
                    ("range.count", &target),
                    ("tier", tier),
                ]),
            },
            Measurement {
                metric: "kv_runtime.local_bytes",
                value: bounded_count(bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("topology", topology),
                    ("range.count", &target),
                    ("tier", tier),
                ]),
            },
        ]);
    }

    for (result, requests) in [
        ("success", successful_requests),
        ("failure", failed_requests),
    ] {
        measurements.push(Measurement {
            metric: "kv_runtime.object_requests",
            value: bounded_count(requests),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
                ("result", result),
            ]),
        });
    }
    for (direction, bytes) in [
        ("read", receipt.object_io.read_byte_total()),
        ("write", receipt.object_io.written_byte_total()),
    ] {
        measurements.push(Measurement {
            metric: "kv_runtime.object_bytes",
            value: bounded_count(bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
                ("direction", direction),
            ]),
        });
    }
    for (phase, seconds) in [
        ("initial-open", receipt.initial_open_seconds),
        ("write-flush", receipt.write_flush_seconds),
        ("empty-cache-rebuild", receipt.empty_cache_rebuild_seconds),
    ] {
        measurements.push(Measurement {
            metric: "kv_runtime.phase_duration",
            value: seconds,
            attributes: common(phase),
        });
    }
    for (statistic, seconds) in [
        ("p50", receipt.cold_point_p50_seconds),
        ("p99", receipt.cold_point_p99_seconds),
    ] {
        measurements.push(Measurement {
            metric: "kv_runtime.point_read_latency",
            value: seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", topology),
                ("range.count", &target),
                ("statistic", statistic),
            ]),
        });
    }
    measurements
}

#[allow(clippy::too_many_lines)]
fn run_kv_runtime_physical_density(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+slatedb-objectkv-serving-v1";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "KV Runtime physical density requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "KV Runtime physical density requires at least one seed".to_owned(),
        ));
    }
    let topology = match workload
        .parameters
        .get("topology")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| format!("workload {} requires topology", workload.id))
        .and_then(parse_kv_runtime_density_topology)
    {
        Ok(topology) => topology,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_kv_runtime_density_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let target_range_engines = match kv_runtime_workload_usize(workload, "range_engine_count") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_u64(profile, "max_rss_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
            kv_runtime_profile_u64(profile, "decoded_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_part_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_open_file_handles")?,
            kv_runtime_profile_usize(profile, "keys_per_range")?,
            kv_runtime_profile_usize(profile, "value_bytes")?,
        ))
    })();
    let (
        max_rss_bytes,
        timeout_millis,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
        keys_per_range,
        value_bytes,
    ) = match profile_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return execution_from_result(Err(format!(
                "resolve KV Runtime density executable: {error}"
            )))
        }
    };
    let config_for_seed = |seed| KvRuntimeDensityWorkerConfig {
        topology,
        target_range_engines,
        seed,
        max_rss_bytes,
        timeout_millis,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
        keys_per_range,
        value_bytes,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let config = config_for_seed(*seed);
        match run_kv_runtime_density_child(&executable, &config, mode) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay_config = config_for_seed(seeds[0]);
    let replay_receipt = match run_kv_runtime_density_child(&executable, &replay_config, mode) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };

    let minimum_seeds = receipts.len() >= 3;
    let slatedb_revision_exact = receipts
        .iter()
        .all(|receipt| receipt.slatedb_revision == SLATEDB_REVISION);
    let objectkv_serving_profile_exact = receipts.iter().all(|receipt| {
        receipt.physical_profile == "objectkv-serving-v1"
            && !receipt.object_wal_enabled
            && !receipt.automatic_flush_enabled
            && !receipt.embedded_compactor
            && !receipt.embedded_garbage_collector
            && receipt.sst_block_size_bytes == 65_536
            && receipt.min_filter_keys == 1
    });
    let physical_rss_probe_supported = receipts
        .iter()
        .all(|receipt| receipt.physical_rss_probe_supported && receipt.peak_rss_bytes > 0);
    let topology_receipt_exact = receipts
        .iter()
        .all(|receipt| receipt.topology == topology.id());
    let database_instance_count_exact = receipts.iter().all(|receipt| {
        let expected = match topology {
            KvRuntimeDensityTopology::OneDbLogicalRanges => 1,
            KvRuntimeDensityTopology::ManyDbSharedCache
            | KvRuntimeDensityTopology::ManyDbPrivateCache => receipt.completed_range_engines,
        };
        receipt.database_instances == expected
    });
    let decoded_cache_instance_count_exact = receipts.iter().all(|receipt| {
        let expected = match topology {
            KvRuntimeDensityTopology::OneDbLogicalRanges
            | KvRuntimeDensityTopology::ManyDbSharedCache => 1,
            KvRuntimeDensityTopology::ManyDbPrivateCache => receipt.completed_range_engines,
        };
        receipt.decoded_cache_instances == expected
    });
    let one_process_wide_nvme_cache = receipts
        .iter()
        .all(|receipt| receipt.nvme_cache_instances == 1);
    let completed_range_reads_exact = receipts
        .iter()
        .all(|receipt| receipt.completed_range_reads_exact);
    let object_io_accounted = receipts.iter().all(|receipt| {
        receipt.object_io.request_total() > 0
            && receipt
                .object_io
                .read_byte_total()
                .saturating_add(receipt.object_io.written_byte_total())
                > 0
    });
    let object_file_inventory_measured = receipts
        .iter()
        .all(|receipt| receipt.object_files > 0 && receipt.object_file_bytes > 0);
    let nvme_file_inventory_measured = receipts
        .iter()
        .all(|receipt| receipt.nvme_cache_files > 0 && receipt.nvme_cache_file_bytes > 0);
    let empty_ram_and_nvme_reopen_executed = receipts
        .iter()
        .all(|receipt| receipt.empty_ram_and_nvme_reopen_executed);
    let runtime_task_probe_supported = receipts
        .iter()
        .all(|receipt| receipt.runtime_task_probe_supported);
    let thread_probe_supported = receipts
        .iter()
        .all(|receipt| receipt.thread_probe_supported);
    let file_descriptor_probe_supported = receipts
        .iter()
        .all(|receipt| receipt.file_descriptor_probe_supported);
    let safety_bounds_checked = receipts.iter().all(|receipt| receipt.safety_bounds_checked);
    let stopped_subject_receipt_exact = receipts.iter().all(|receipt| {
        receipt.completed_range_engines <= receipt.target_range_engines
            && (receipt.completed_range_engines == receipt.target_range_engines
                || matches!(receipt.stop_reason.as_str(), "rss-limit" | "time-limit"))
    });
    let semantic_receipt_replays = receipts.first().is_some_and(|receipt| {
        receipt.semantic_receipt_sha256 == replay_receipt.semantic_receipt_sha256
    });
    let one_db_targets_complete = topology != KvRuntimeDensityTopology::OneDbLogicalRanges
        || receipts.iter().all(|receipt| {
            receipt.completed_range_engines == receipt.target_range_engines
                && receipt.stop_reason == "none"
        });
    let checks = [
        minimum_seeds,
        slatedb_revision_exact,
        objectkv_serving_profile_exact,
        physical_rss_probe_supported,
        topology_receipt_exact,
        database_instance_count_exact,
        decoded_cache_instance_count_exact,
        one_process_wide_nvme_cache,
        completed_range_reads_exact,
        object_io_accounted,
        object_file_inventory_measured,
        nvme_file_inventory_measured,
        empty_ram_and_nvme_reopen_executed,
        runtime_task_probe_supported,
        thread_probe_supported,
        file_descriptor_probe_supported,
        safety_bounds_checked,
        stopped_subject_receipt_exact,
        semantic_receipt_replays,
        one_db_targets_complete,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let negative_control_detected = mode == KvRuntimeDensityMode::Correct || anomalies > 0;
    let passed = mode == KvRuntimeDensityMode::Correct && anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "KV Runtime physical density discarded mode={}: anomalies={anomalies}, negative_control_detected={negative_control_detected}",
            mode.id()
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "rfc0057-kv-runtime-physical-density"),
            (
                "anomaly.class",
                if anomalies == 0 { "none" } else { mode.id() },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(kv_runtime_density_measurements(workload, backend, receipt));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => {
            return execution_from_result(Err(format!(
                "establish KV Runtime density executable identity: {error}"
            )))
        }
    };
    let artifact_path = kv_runtime_density_artifact_path(run_id, candidate_commit, workload);
    let artifact = KvRuntimeDensityArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        topology: topology.id(),
        target_range_engines,
        mode: mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay_receipt,
    };
    if let Err(error) =
        write_json_artifact(&artifact_path, &artifact, "KV Runtime physical density")
    {
        return execution_from_result(Err(error));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            gate("kv_runtime_density.minimum_seeds", minimum_seeds),
            gate(
                "kv_runtime_density.slatedb_revision_exact",
                slatedb_revision_exact,
            ),
            gate(
                "kv_runtime_density.objectkv_serving_profile_exact",
                objectkv_serving_profile_exact,
            ),
            gate(
                "kv_runtime_density.physical_rss_probe_supported",
                physical_rss_probe_supported,
            ),
            gate(
                "kv_runtime_density.topology_receipt_exact",
                topology_receipt_exact,
            ),
            gate(
                "kv_runtime_density.database_instance_count_exact",
                database_instance_count_exact,
            ),
            gate(
                "kv_runtime_density.decoded_cache_instance_count_exact",
                decoded_cache_instance_count_exact,
            ),
            gate(
                "kv_runtime_density.one_process_wide_nvme_cache",
                one_process_wide_nvme_cache,
            ),
            gate(
                "kv_runtime_density.completed_range_reads_exact",
                completed_range_reads_exact,
            ),
            gate(
                "kv_runtime_density.object_io_accounted",
                object_io_accounted,
            ),
            gate(
                "kv_runtime_density.object_file_inventory_measured",
                object_file_inventory_measured,
            ),
            gate(
                "kv_runtime_density.nvme_file_inventory_measured",
                nvme_file_inventory_measured,
            ),
            gate(
                "kv_runtime_density.empty_ram_and_nvme_reopen_executed",
                empty_ram_and_nvme_reopen_executed,
            ),
            gate(
                "kv_runtime_density.runtime_task_probe_supported",
                runtime_task_probe_supported,
            ),
            gate(
                "kv_runtime_density.thread_probe_supported",
                thread_probe_supported,
            ),
            gate(
                "kv_runtime_density.file_descriptor_probe_supported",
                file_descriptor_probe_supported,
            ),
            gate(
                "kv_runtime_density.safety_bounds_checked",
                safety_bounds_checked,
            ),
            gate(
                "kv_runtime_density.stopped_subject_receipt_exact",
                stopped_subject_receipt_exact,
            ),
            gate(
                "kv_runtime_density.semantic_receipt_replays",
                semantic_receipt_replays,
            ),
            gate(
                "kv_runtime_density.one_db_targets_complete",
                one_db_targets_complete,
            ),
            gate(
                "kv_runtime_density.negative_control_detected",
                negative_control_detected,
            ),
        ],
        budget_units: receipts
            .iter()
            .map(|receipt| bounded_usize(receipt.completed_range_engines))
            .sum(),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "kv_runtime.completed_range_engines".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_usize(receipt.completed_range_engines))
                    .sum(),
            ),
            (
                "kv_runtime.object_files".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.object_files))
                    .sum(),
            ),
        ]),
    }
}

fn parse_snapshot_read_curve_mode(value: &str) -> Result<SnapshotReadCurveMode, String> {
    match value {
        "correct" | "none" => Ok(SnapshotReadCurveMode::Correct),
        "latest_only" => Ok(SnapshotReadCurveMode::LatestOnly),
        "skip_point_tombstone" => Ok(SnapshotReadCurveMode::SkipPointTombstone),
        "overstate_applied_frontier" => Ok(SnapshotReadCurveMode::OverstateAppliedFrontier),
        "length_prefix_user_keys" => Ok(SnapshotReadCurveMode::LengthPrefixUserKeys),
        other => Err(format!("unknown snapshot-read curve mode {other}")),
    }
}

fn run_snapshot_read_curve_child(
    executable: &Path,
    config: &SnapshotReadCurveConfig,
    mode: SnapshotReadCurveMode,
) -> Result<SnapshotReadCurveReceipt, String> {
    let config_json = serde_json::to_string(config)
        .map_err(|error| format!("serialize snapshot-read config: {error}"))?;
    let mut child = Command::new(executable)
        .arg("snapshot-read-curve-node")
        .arg("--config-json")
        .arg(config_json)
        .arg("--mode")
        .arg(mode.id())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn snapshot-read child: {error}"))?;
    let deadline = Duration::from_millis(config.timeout_millis.saturating_add(5_000));
    let wait_started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("collect snapshot-read child: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "snapshot-read child failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                return serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse snapshot-read receipt: {error}"));
            }
            Ok(None) if wait_started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "snapshot-read child exceeded controller timeout of {} ms",
                    deadline.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("poll snapshot-read child: {error}"));
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn snapshot_read_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &SnapshotReadCurveReceipt,
) -> Vec<Measurement> {
    let depth = receipt.version_depth.to_string();
    let mut measurements = vec![
        Measurement {
            metric: "kv_runtime.snapshot_physical_amplification",
            value: receipt.physical_bytes_per_live_byte,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
            ]),
        },
        Measurement {
            metric: "kv_runtime.phase_duration",
            value: receipt.ingest_flush_seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("topology", "one-db-logical-ranges"),
                ("range.count", "1"),
                ("phase", "mvcc-ingest-flush"),
            ]),
        },
    ];
    for target in &receipt.targets {
        if target.class == "near-latest" {
            measurements.push(Measurement {
                metric: "kv_runtime.snapshot_near_latest_cold_point_p99",
                value: target.cold_point_p99_seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("history.depth", &depth),
                ]),
            });
        }
        for (cache_state, statistic, seconds) in [
            ("warm", "p50", target.warm_point_p50_seconds),
            ("warm", "p99", target.warm_point_p99_seconds),
            ("cold", "p50", target.cold_point_p50_seconds),
            ("cold", "p99", target.cold_point_p99_seconds),
        ] {
            measurements.push(Measurement {
                metric: "kv_runtime.snapshot_point_latency",
                value: seconds,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("history.depth", &depth),
                    ("read.class", &target.class),
                    ("cache.state", cache_state),
                    ("statistic", statistic),
                ]),
            });
        }
        for (cache_state, seconds, rows, io) in [
            (
                "warm",
                target.warm_scan_seconds,
                target.warm_scan_rows,
                &target.warm_scan_io,
            ),
            (
                "cold",
                target.cold_scan_seconds,
                target.cold_scan_rows,
                &target.cold_scan_io,
            ),
        ] {
            let row_count = bounded_usize(rows.max(1));
            measurements.push(Measurement {
                metric: "kv_runtime.snapshot_scan_throughput",
                value: bounded_usize(rows) / seconds.max(f64::EPSILON),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("history.depth", &depth),
                    ("read.class", &target.class),
                    ("cache.state", cache_state),
                ]),
            });
            measurements.push(Measurement {
                metric: "kv_runtime.snapshot_object_requests_per_row",
                value: bounded_count(io.request_total()) / row_count,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("history.depth", &depth),
                    ("read.class", &target.class),
                    ("cache.state", cache_state),
                ]),
            });
            measurements.push(Measurement {
                metric: "kv_runtime.snapshot_object_bytes_per_row",
                value: bounded_count(io.read_byte_total()) / row_count,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("history.depth", &depth),
                    ("read.class", &target.class),
                    ("cache.state", cache_state),
                ]),
            });
        }
    }
    measurements
}

#[allow(clippy::too_many_lines)]
fn run_snapshot_read_curve(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+slatedb-objectkv-mvcc-v1";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "snapshot-read curve requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err(
            "snapshot-read curve requires at least one seed".to_owned()
        ));
    }
    let version_depth =
        match kv_runtime_workload_usize(workload, "version_depth").and_then(|value| {
            u64::try_from(value).map_err(|error| format!("version depth is too large: {error}"))
        }) {
            Ok(value) => value,
            Err(error) => return execution_from_result(Err(error)),
        };
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_snapshot_read_curve_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "key_count")?,
            kv_runtime_profile_usize(profile, "value_bytes")?,
            kv_runtime_profile_u64(profile, "max_rss_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
            kv_runtime_profile_u64(profile, "decoded_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_part_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_open_file_handles")?,
        ))
    })();
    let (
        key_count,
        value_bytes,
        max_rss_bytes,
        timeout_millis,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
    ) = match profile_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return execution_from_result(Err(format!("resolve snapshot-read executable: {error}")))
        }
    };
    let config_for_seed = |seed| SnapshotReadCurveConfig {
        version_depth,
        key_count,
        value_bytes,
        seed,
        max_rss_bytes,
        timeout_millis,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_snapshot_read_curve_child(&executable, &config_for_seed(*seed), mode) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay_receipt =
        match run_snapshot_read_curve_child(&executable, &config_for_seed(seeds[0]), mode) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };

    let minimum_seeds = receipts.len() >= 3;
    let version_depth_exact = receipts
        .iter()
        .all(|receipt| receipt.version_depth == version_depth);
    let revision_and_profile_exact = receipts.iter().all(|receipt| {
        receipt.slatedb_revision == SLATEDB_REVISION
            && receipt.physical_profile == "objectkv-serving-v1"
    });
    let point_reads_exact = receipts.iter().all(|receipt| {
        receipt
            .targets
            .iter()
            .all(|target| target.point_reads_exact)
    });
    let ordered_scans_exact = receipts.iter().all(|receipt| {
        receipt
            .targets
            .iter()
            .all(|target| target.ordered_scans_exact)
    });
    let tombstone_exact = receipts.iter().all(|receipt| receipt.tombstone_exact);
    let frontier_exact = receipts.iter().all(|receipt| {
        receipt.claimed_applied_frontier == receipt.actual_applied_frontier
            && receipt.future_frontier_refused
    });
    let binary_order_exact = receipts
        .iter()
        .all(|receipt| receipt.binary_key_order_exact);
    let close_reopen_exact = receipts.iter().all(|receipt| receipt.close_reopen_exact);
    let safety_bounds_held = receipts
        .iter()
        .all(|receipt| receipt.safety_bounds_held && receipt.peak_rss_bytes > 0);
    let object_io_accounted = receipts.iter().all(|receipt| {
        receipt.total_io.request_total() > 0
            && receipt
                .total_io
                .read_byte_total()
                .saturating_add(receipt.total_io.written_byte_total())
                > 0
    });
    let object_inventory_measured = receipts
        .iter()
        .all(|receipt| receipt.object_files > 0 && receipt.object_file_bytes > 0);
    let semantic_replay_exact = receipts.first().is_some_and(|receipt| {
        receipt.semantic_receipt_sha256 == replay_receipt.semantic_receipt_sha256
    });
    let checks = [
        minimum_seeds,
        version_depth_exact,
        revision_and_profile_exact,
        point_reads_exact,
        ordered_scans_exact,
        tombstone_exact,
        frontier_exact,
        binary_order_exact,
        close_reopen_exact,
        safety_bounds_held,
        object_io_accounted,
        object_inventory_measured,
        semantic_replay_exact,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let negative_control_detected = mode == SnapshotReadCurveMode::Correct || anomalies > 0;
    let passed = mode == SnapshotReadCurveMode::Correct && anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "snapshot-read curve discarded mode={}: anomalies={anomalies}, negative_control_detected={negative_control_detected}",
            mode.id()
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "rfc0058-exact-version-read"),
            (
                "anomaly.class",
                if anomalies == 0 { "none" } else { mode.id() },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(snapshot_read_measurements(workload, backend, receipt));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return execution_from_result(Err(error)),
    };
    let artifact_path = snapshot_read_curve_artifact_path(run_id, candidate_commit, workload);
    let artifact = SnapshotReadCurveArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        version_depth,
        mode: mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay_receipt,
    };
    if let Err(error) = write_json_artifact(&artifact_path, &artifact, "snapshot-read curve") {
        return execution_from_result(Err(error));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            gate("snapshot_read.minimum_seeds", minimum_seeds),
            gate("snapshot_read.version_depth_exact", version_depth_exact),
            gate(
                "snapshot_read.revision_and_profile_exact",
                revision_and_profile_exact,
            ),
            gate("snapshot_read.point_reads_exact", point_reads_exact),
            gate("snapshot_read.ordered_scans_exact", ordered_scans_exact),
            gate("snapshot_read.tombstone_exact", tombstone_exact),
            gate("snapshot_read.frontier_exact", frontier_exact),
            gate("snapshot_read.binary_order_exact", binary_order_exact),
            gate("snapshot_read.close_reopen_exact", close_reopen_exact),
            gate("snapshot_read.safety_bounds_held", safety_bounds_held),
            gate("snapshot_read.object_io_accounted", object_io_accounted),
            gate(
                "snapshot_read.object_inventory_measured",
                object_inventory_measured,
            ),
            gate("snapshot_read.semantic_replay_exact", semantic_replay_exact),
            gate(
                "snapshot_read.negative_control_detected",
                negative_control_detected,
            ),
        ],
        budget_units: bounded_count(version_depth)
            * bounded_usize(key_count)
            * bounded_usize(seeds.len()),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "kv_runtime.snapshot_object_files".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.object_files))
                    .sum(),
            ),
            (
                "kv_runtime.snapshot_history_depth".to_owned(),
                bounded_count(version_depth),
            ),
        ]),
    }
}

fn parse_mvcc_gc_curve_mode(value: &str) -> Result<MvccGcCurveMode, String> {
    match value {
        "correct" | "none" => Ok(MvccGcCurveMode::Correct),
        "ignore_lease_floor" => Ok(MvccGcCurveMode::IgnoreLeaseFloor),
        "drop_floor_anchor" => Ok(MvccGcCurveMode::DropFloorAnchor),
        "drop_tombstone_anchor" => Ok(MvccGcCurveMode::DropTombstoneAnchor),
        "reload_floor_during_job" => Ok(MvccGcCurveMode::ReloadFloorDuringJob),
        "claim_collection_without_publication" => {
            Ok(MvccGcCurveMode::ClaimCollectionWithoutPublication)
        }
        other => Err(format!("unknown MVCC GC curve mode {other}")),
    }
}

fn run_mvcc_gc_curve_child(
    executable: &Path,
    config: &MvccGcCurveConfig,
    mode: MvccGcCurveMode,
) -> Result<MvccGcCurveReceipt, String> {
    let config_json = serde_json::to_string(config)
        .map_err(|error| format!("serialize MVCC GC config: {error}"))?;
    let mut child = Command::new(executable)
        .arg("mvcc-gc-curve-node")
        .arg("--config-json")
        .arg(config_json)
        .arg("--mode")
        .arg(mode.id())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn MVCC GC child: {error}"))?;
    let deadline = Duration::from_millis(
        config
            .timeout_millis
            .saturating_mul(3)
            .saturating_add(5_000),
    );
    let wait_started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("collect MVCC GC child: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "MVCC GC child failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                return serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse MVCC GC receipt: {error}"));
            }
            Ok(None) if wait_started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "MVCC GC child exceeded controller timeout of {} ms",
                    deadline.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("poll MVCC GC child: {error}"));
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn mvcc_gc_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &MvccGcCurveReceipt,
) -> Vec<Measurement> {
    let depth = receipt.history_depth.to_string();
    let window = receipt.retained_versions.to_string();
    let cold_scan_rows = bounded_usize(receipt.cold_scan_rows.max(1));
    let logical_ingest_bytes = receipt
        .history_depth
        .saturating_mul(u64::try_from(receipt.key_count).unwrap_or(u64::MAX))
        .saturating_mul(u64::try_from(receipt.value_bytes).unwrap_or(u64::MAX));
    vec![
        Measurement {
            metric: "kv_runtime.mvcc_gc_post_physical_amplification",
            value: receipt.post_compaction_bytes_per_retained_logical_byte,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_live_byte_reduction",
            value: receipt.live_byte_reduction_fraction,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_compaction_duration",
            value: receipt.compaction_seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_cold_point_p99",
            value: receipt.cold_point_p99_seconds,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_cold_scan_throughput",
            value: bounded_usize(receipt.cold_scan_rows)
                / receipt.cold_scan_seconds.max(f64::EPSILON),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_live_sst_bytes",
            value: bounded_count(receipt.pre_compaction_live_sst_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
                ("state", "pre-compaction"),
            ]),
        },
        Measurement {
            metric: "kv_runtime.mvcc_gc_live_sst_bytes",
            value: bounded_count(receipt.post_compaction_live_sst_bytes),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("retention.window", &window),
                ("state", "post-compaction"),
            ]),
        },
        Measurement {
            metric: "kv_runtime.snapshot_object_requests_per_row",
            value: bounded_count(receipt.cold_scan_io.request_total()) / cold_scan_rows,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("read.class", "floor-after-gc"),
                ("cache.state", "cold"),
            ]),
        },
        Measurement {
            metric: "kv_runtime.snapshot_object_bytes_per_row",
            value: bounded_count(receipt.cold_scan_io.read_byte_total()) / cold_scan_rows,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("history.depth", &depth),
                ("read.class", "floor-after-gc"),
                ("cache.state", "cold"),
            ]),
        },
        Measurement {
            metric: "compaction.write_amplification",
            value: bounded_count(receipt.compaction_io.written_byte_total())
                / bounded_count(logical_ingest_bytes.max(1)),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("compaction.kind", "retained-history-rewrite"),
            ]),
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn run_mvcc_gc_curve(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+slatedb-objectkv-mvcc-gc-v1";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "MVCC GC curve requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.is_empty() {
        return execution_from_result(Err("MVCC GC curve requires at least one seed".to_owned()));
    }
    let workload_values = (|| {
        Ok::<_, String>((
            u64::try_from(kv_runtime_workload_usize(workload, "history_depth")?)
                .map_err(|error| format!("history depth is too large: {error}"))?,
            u64::try_from(kv_runtime_workload_usize(workload, "retained_versions")?)
                .map_err(|error| format!("retained versions are too large: {error}"))?,
            u64::try_from(kv_runtime_workload_usize(workload, "flush_stride")?)
                .map_err(|error| format!("flush stride is too large: {error}"))?,
        ))
    })();
    let (history_depth, retained_versions, flush_stride) = match workload_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_mvcc_gc_curve_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "key_count")?,
            kv_runtime_profile_usize(profile, "value_bytes")?,
            kv_runtime_profile_u64(profile, "max_rss_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
        ))
    })();
    let (key_count, value_bytes, max_rss_bytes, timeout_millis) = match profile_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return execution_from_result(Err(format!("resolve MVCC GC executable: {error}")))
        }
    };
    let config_for_seed = |seed| MvccGcCurveConfig {
        history_depth,
        retained_versions,
        flush_stride,
        key_count,
        value_bytes,
        seed,
        timeout_millis,
        max_rss_bytes,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_mvcc_gc_curve_child(&executable, &config_for_seed(*seed), mode) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay_receipt =
        match run_mvcc_gc_curve_child(&executable, &config_for_seed(seeds[0]), mode) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };

    let floor_version = history_depth
        .saturating_sub(retained_versions)
        .saturating_add(1);
    let minimum_seeds = receipts.len() >= 3;
    let config_exact = receipts.iter().zip(seeds).all(|(receipt, seed)| {
        receipt.contract_version == 1
            && receipt.mode == mode.id()
            && receipt.seed == *seed
            && receipt.history_depth == history_depth
            && receipt.retained_versions == retained_versions
            && receipt.flush_stride == flush_stride
            && receipt.floor_version == floor_version
            && receipt.key_count == key_count
            && receipt.value_bytes == value_bytes
    });
    let revision_and_profile_exact = receipts.iter().all(|receipt| {
        receipt.slatedb_revision == SLATEDB_REVISION
            && receipt.physical_profile == "objectkv-serving-v1+mvcc-gc-v1"
    });
    let publication_exact = receipts.iter().all(|receipt| {
        receipt.publication_completed && receipt.claimed_collected_through == floor_version
    });
    let frozen_floor_exact = receipts.iter().all(|receipt| {
        receipt.filter_floor_version == floor_version && !receipt.floor_advanced_mid_job
    });
    let expected_l0_ssts = history_depth.div_ceil(flush_stride);
    let compaction_topology_exact = receipts.iter().all(|receipt| {
        receipt.initial_l0_ssts == expected_l0_ssts
            && receipt.final_l0_ssts == 0
            && receipt.final_sorted_runs > 0
    });
    let filter_accounted = receipts.iter().all(|receipt| {
        receipt.filter_stats.inspected_user_entries > 0
            && receipt.filter_stats.kept_floor_anchors > 0
            && receipt.filter_stats.dropped_older_entries > 0
            && receipt.filter_stats.malformed_entries == 0
    });
    let live_bytes_reduced = receipts.iter().all(|receipt| {
        receipt.post_compaction_live_sst_bytes < receipt.pre_compaction_live_sst_bytes
            && receipt.live_byte_reduction_fraction > 0.0
            && receipt.post_compaction_bytes_per_retained_logical_byte > 0.0
    });
    let exact_reads = receipts.iter().all(|receipt| {
        receipt.floor_point_exact
            && receipt.floor_scan_exact
            && receipt.latest_point_exact
            && receipt.latest_scan_exact
            && receipt.tombstone_anchor_exact
    });
    let read_bounds_exact = receipts
        .iter()
        .all(|receipt| receipt.expired_snapshot_refused && receipt.future_snapshot_refused);
    let close_reopen_exact = receipts.iter().all(|receipt| receipt.close_reopen_exact);
    let safety_bounds_held = receipts
        .iter()
        .all(|receipt| receipt.safety_bounds_held && receipt.peak_rss_bytes > 0);
    let object_io_accounted = receipts.iter().all(|receipt| {
        receipt.compaction_io.request_total() > 0
            && receipt.compaction_io.read_byte_total() > 0
            && receipt.compaction_io.written_byte_total() > 0
            && receipt.cold_point_io.request_total() > 0
            && receipt.cold_scan_io.request_total() > 0
            && receipt.cold_scan_io.read_byte_total() > 0
    });
    let semantic_replay_exact = receipts.first().is_some_and(|receipt| {
        receipt.semantic_receipt_sha256 == replay_receipt.semantic_receipt_sha256
    });
    let checks = [
        minimum_seeds,
        config_exact,
        revision_and_profile_exact,
        publication_exact,
        frozen_floor_exact,
        compaction_topology_exact,
        filter_accounted,
        live_bytes_reduced,
        exact_reads,
        read_bounds_exact,
        close_reopen_exact,
        safety_bounds_held,
        object_io_accounted,
        semantic_replay_exact,
    ];
    let controller_anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let receipt_anomalies = receipts
        .iter()
        .map(MvccGcCurveReceipt::anomaly_count)
        .sum::<u64>();
    let anomalies = controller_anomalies.saturating_add(receipt_anomalies);
    let negative_control_detected = mode == MvccGcCurveMode::Correct || anomalies > 0;
    let passed = mode == MvccGcCurveMode::Correct && anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "MVCC GC curve discarded mode={}: anomalies={anomalies}, negative_control_detected={negative_control_detected}",
            mode.id()
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "rfc0059-retained-window-mvcc-gc"),
            (
                "anomaly.class",
                if anomalies == 0 { "none" } else { mode.id() },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(mvcc_gc_measurements(workload, backend, receipt));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return execution_from_result(Err(error)),
    };
    let artifact_path = mvcc_gc_curve_artifact_path(run_id, candidate_commit, workload);
    let artifact = MvccGcCurveArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        history_depth,
        retained_versions,
        mode: mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay_receipt,
    };
    if let Err(error) = write_json_artifact(&artifact_path, &artifact, "MVCC GC curve") {
        return execution_from_result(Err(error));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            gate("mvcc_gc.minimum_seeds", minimum_seeds),
            gate("mvcc_gc.config_exact", config_exact),
            gate(
                "mvcc_gc.revision_and_profile_exact",
                revision_and_profile_exact,
            ),
            gate("mvcc_gc.publication_exact", publication_exact),
            gate("mvcc_gc.frozen_floor_exact", frozen_floor_exact),
            gate(
                "mvcc_gc.compaction_topology_exact",
                compaction_topology_exact,
            ),
            gate("mvcc_gc.filter_accounted", filter_accounted),
            gate("mvcc_gc.live_bytes_reduced", live_bytes_reduced),
            gate("mvcc_gc.exact_reads", exact_reads),
            gate("mvcc_gc.read_bounds_exact", read_bounds_exact),
            gate("mvcc_gc.close_reopen_exact", close_reopen_exact),
            gate("mvcc_gc.safety_bounds_held", safety_bounds_held),
            gate("mvcc_gc.object_io_accounted", object_io_accounted),
            gate("mvcc_gc.semantic_replay_exact", semantic_replay_exact),
            gate(
                "mvcc_gc.negative_control_detected",
                negative_control_detected,
            ),
        ],
        budget_units: bounded_count(history_depth)
            * bounded_usize(key_count)
            * bounded_usize(seeds.len()),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "kv_runtime.mvcc_gc_history_depth".to_owned(),
                bounded_count(history_depth),
            ),
            (
                "kv_runtime.mvcc_gc_retained_versions".to_owned(),
                bounded_count(retained_versions),
            ),
            (
                "kv_runtime.mvcc_gc_pre_compaction_live_sst_bytes".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.pre_compaction_live_sst_bytes))
                    .sum(),
            ),
            (
                "kv_runtime.mvcc_gc_post_compaction_live_sst_bytes".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.post_compaction_live_sst_bytes))
                    .sum(),
            ),
            (
                "kv_runtime.mvcc_gc_dropped_entries".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.filter_stats.dropped_older_entries))
                    .sum(),
            ),
        ]),
    }
}

fn hotspot_profile_u64(profile: &ProfileConfig, key: &str) -> Result<u64, String> {
    let value = profile
        .parameters
        .get(key)
        .and_then(toml::Value::as_integer)
        .ok_or_else(|| format!("resolver hotspot profile requires integer parameter {key}"))?;
    u64::try_from(value).map_err(|_| format!("resolver hotspot profile {key} must be non-negative"))
}

fn hotspot_profile_usize(profile: &ProfileConfig, key: &str) -> Result<usize, String> {
    usize::try_from(hotspot_profile_u64(profile, key)?)
        .map_err(|error| format!("resolver hotspot profile {key} is too large: {error}"))
}

fn hotspot_workload_string(workload: &WorkloadConfig, key: &str) -> Result<String, String> {
    workload
        .parameters
        .get(key)
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("workload {} requires string parameter {key}", workload.id))
}

fn hotspot_workload_fraction(workload: &WorkloadConfig, key: &str) -> Result<f64, String> {
    workload
        .parameters
        .get(key)
        .and_then(toml::Value::as_float)
        .ok_or_else(|| format!("workload {} requires float parameter {key}", workload.id))
}

fn hotspot_distribution_parameters_exact(
    workload: &WorkloadConfig,
    distribution: ResolverHotspotDistribution,
) -> Result<bool, String> {
    let observed = (
        hotspot_workload_fraction(workload, "left_fraction")?,
        hotspot_workload_fraction(workload, "right_fraction")?,
        hotspot_workload_fraction(workload, "crossing_fraction")?,
        hotspot_workload_fraction(workload, "left_history_fraction")?,
    );
    let expected = match distribution {
        ResolverHotspotDistribution::BalancedIndependent => (0.5, 0.5, 0.0, 0.5),
        ResolverHotspotDistribution::MissedHotKeyBoundary => (1.0, 0.0, 0.0, 1.0),
        ResolverHotspotDistribution::Crossing25 => (0.375, 0.375, 0.25, 0.5),
        ResolverHotspotDistribution::Crossing100 => (0.0, 0.0, 1.0, 0.5),
    };
    Ok([
        (observed.0, expected.0),
        (observed.1, expected.1),
        (observed.2, expected.2),
        (observed.3, expected.3),
    ]
    .into_iter()
    .all(|(left, right)| (left - right).abs() <= f64::EPSILON))
}

#[allow(clippy::too_many_lines)]
fn run_resolver_hotspot_curve_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "resolver hotspot curve requires at least one seed".to_owned()
        ));
    }
    let expected_backend = "local-process+memory-only-ordered-resolver-workers";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "resolver hotspot curve requires {expected_backend}, got {backend}"
        )));
    }
    let mode_value = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_resolver_hotspot_curve_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let distribution_label = match hotspot_workload_string(workload, "distribution") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    let distribution = match parse_resolver_hotspot_distribution(&distribution_label) {
        Ok(distribution) => distribution,
        Err(error) => return execution_from_result(Err(error)),
    };
    let distribution_parameters_exact =
        match hotspot_distribution_parameters_exact(workload, distribution) {
            Ok(exact) => exact,
            Err(error) => return execution_from_result(Err(error)),
        };
    if !distribution_parameters_exact {
        return execution_from_result(Err(format!(
            "workload {} distribution fractions changed from the frozen point",
            workload.id
        )));
    }
    let profile_repetitions = match hotspot_profile_u64(profile, "repetitions_per_seed") {
        Ok(value) => value,
        Err(error) => return execution_from_result(Err(error)),
    };
    if profile_repetitions != u64::from(profile.repeats) {
        return execution_from_result(Err(
            "resolver hotspot profile repetition fields disagree".to_owned()
        ));
    }
    let base_config = match (|| {
        Ok::<_, String>(ResolverHotspotCurveConfig {
            seed: 0,
            distribution,
            logical_transactions: hotspot_profile_u64(profile, "logical_transactions")?,
            batches: hotspot_profile_u64(profile, "batches")?,
            transactions_per_batch: hotspot_profile_u64(profile, "transactions_per_batch")?,
            warmup_transactions: hotspot_profile_u64(profile, "warmup_transactions")?,
            history_entries_total: hotspot_profile_u64(profile, "history_entries_total")?,
            repetitions: profile.repeats,
            minimum_available_parallelism: hotspot_profile_usize(
                profile,
                "minimum_available_parallelism",
            )?,
            controller_threads: hotspot_profile_usize(profile, "controller_threads")?,
        })
    })() {
        Ok(config) => config,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<ResolverHotspotCurveReport> = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let mut config = base_config.clone();
        config.seed = *seed;
        match run_resolver_hotspot_curve_contract(&config, mode, &executable) {
            Ok(report) => reports.push(report),
            Err(error) => return execution_from_result(Err(error)),
        }
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_samples = usize::try_from(seed_count.saturating_mul(u64::from(profile.repeats)))
        .unwrap_or(usize::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let semantic_contract = reports.iter().all(|report| {
        report.source_and_split_workload_digest_exact
            && report.source_outcomes_match_oracle
            && report.split_outcomes_match_oracle
            && report.source_and_split_outcomes_match
            && report.crossing_transactions_reach_every_child
            && report.one_map_epoch_per_transaction
            && report.source_history_is_exact_child_union
    });
    let benchmark_integrity = reports.iter().all(|report| {
        report.worker_startup_excluded_from_timing
            && report.history_preparation_excluded_from_timing
            && report.warmup_excluded_from_timing
            && report.every_outcome_validated
            && report.split_child_execution_overlaps
            && report.operation_count_fixed
            && report.batch_order_fixed
            && report.controller_concurrency_fixed
            && report.same_executable_and_machine
            && report.alternating_topology_order_complete
    });
    let sample_count = reports
        .iter()
        .map(|report| report.samples.len())
        .sum::<usize>();
    let sample_contract = sample_count == expected_samples
        && reports
            .iter()
            .all(|report| report.duration_distribution_recorded && report.exact_untimed_replay);
    let negative_detected = mode == ResolverHotspotCurveMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == ResolverHotspotCurveMode::Correct
        && anomalies == 0
        && semantic_contract
        && benchmark_integrity
        && sample_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.config.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "resolver hotspot curve discarded mode={}: anomalies={anomalies}, semantic_contract={semantic_contract}, benchmark_integrity={benchmark_integrity}, sample_contract={sample_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    let mut source_decisions = 0_u64;
    let mut split_decisions = 0_u64;
    let mut source_history_examined = 0_u64;
    let mut split_history_examined = 0_u64;
    for report in &reports {
        let exact = report.anomaly_count == 0;
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(report.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "rfc0055-paired-resolver-curve"),
                ("anomaly.class", if exact { "none" } else { mode.id() }),
            ]),
        });
        for sample in &report.samples {
            source_decisions = source_decisions.saturating_add(sample.source_resolver_decisions);
            split_decisions = split_decisions.saturating_add(sample.split_resolver_decisions);
            source_history_examined =
                source_history_examined.saturating_add(sample.source_history_entries_examined);
            split_history_examined =
                split_history_examined.saturating_add(sample.split_history_entries_examined);
            measurements.extend([
                Measurement {
                    metric: "resolver.throughput_ratio",
                    value: sample.throughput_ratio,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("backend", backend),
                        ("distribution", &distribution_label),
                    ]),
                },
                Measurement {
                    metric: "operation.throughput",
                    value: sample.source_throughput,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "resolver-source"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "operation.throughput",
                    value: sample.split_throughput,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "resolver-split"),
                        ("backend", backend),
                    ]),
                },
                Measurement {
                    metric: "operation.duration",
                    value: sample.source_seconds,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "resolver-source"),
                        ("backend", backend),
                        ("result", if exact { "exact" } else { "discard" }),
                    ]),
                },
                Measurement {
                    metric: "operation.duration",
                    value: sample.split_seconds,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("operation", "resolver-split"),
                        ("backend", backend),
                        ("result", if exact { "exact" } else { "discard" }),
                    ]),
                },
                Measurement {
                    metric: "range.hotspot_ratio",
                    value: sample.split_hotspot_ratio,
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("topology", "split-two-child"),
                        ("distribution", &distribution_label),
                    ]),
                },
                Measurement {
                    metric: "serializability.constraints_checked",
                    value: bounded_count(sample.split_resolver_decisions),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("constraint.kind", "resolver-conflict-decision"),
                        ("result", if exact { "pass" } else { "fail" }),
                    ]),
                },
            ]);
        }
        artifact_refs.push(format!(
            "okv-resolver-hotspot-curve://{}/{}/{}/{}",
            mode.id(),
            distribution_label,
            report.config.seed,
            report.trace_sha256
        ));
    }

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "resolver_hotspot.semantic_contract".to_owned(),
                status: gate_status(semantic_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "resolver_hotspot.benchmark_integrity".to_owned(),
                status: gate_status(benchmark_integrity),
                detail: None,
            },
            HardGateResult {
                id: "resolver_hotspot.sample_distribution".to_owned(),
                status: gate_status(sample_contract),
                detail: Some(format!(
                    "samples={sample_count}, expected={expected_samples}"
                )),
            },
            HardGateResult {
                id: "resolver_hotspot.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(split_decisions),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "resolver_hotspot.source_decisions".to_owned(),
                bounded_count(source_decisions),
            ),
            (
                "resolver_hotspot.split_decisions".to_owned(),
                bounded_count(split_decisions),
            ),
            (
                "resolver_hotspot.source_history_entries_examined".to_owned(),
                bounded_count(source_history_examined),
            ),
            (
                "resolver_hotspot.split_history_entries_examined".to_owned(),
                bounded_count(split_history_examined),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_online_resolver_split_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "online resolver split requires at least one seed".to_owned()
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_online_resolver_split_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut reports: Vec<OnlineResolverSplitReport> = Vec::with_capacity(seeds.len());
    let mut durations = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let started = Instant::now();
        let first = match run_online_resolver_split_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        durations.push(started.elapsed().as_secs_f64());
        let second = match run_online_resolver_split_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let tickets = reports
        .iter()
        .map(|report| report.sequencer_tickets)
        .sum::<u64>();
    let attempts = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let conflicts = reports
        .iter()
        .map(|report| report.conflict_rejections)
        .sum::<u64>();
    let abandoned = reports
        .iter()
        .map(|report| report.abandoned_old_map_transactions)
        .sum::<u64>();
    let decisions = reports
        .iter()
        .map(|report| report.resolver_decisions)
        .sum::<u64>();
    let progress_frames = reports
        .iter()
        .map(|report| report.tlog_progress_frames)
        .sum::<u64>();
    let process_boundaries = reports.iter().all(|report| {
        report.sequencer_nodes == 3
            && report.proxy_process_starts == 3
            && report.source_resolver_process_starts == 3
            && report.shadow_resolver_process_starts == 2
            && report.tlog_process_starts == 6
            && report.resolver_durable_syncs == 0
            && report.resolver_finalization_rpcs == 0
    });
    let correct_contract = mode != OnlineResolverSplitMode::Correct
        || (anomalies == 0
            && reports.iter().all(|report| {
                report.sequencer_tickets == 30
                    && report.attempted_transactions == 120
                    && report.split_descriptor_is_immutable
                    && report.split_descriptor_binds_both_map_digests
                    && report.source_history_snapshot_is_exact
                    && report.source_entries_partition_exactly_across_children
                    && report.shadow_children_start_empty
                    && report.shadow_children_do_not_decide_before_cutover
                    && report.touching_catchup_batches_reach_source_and_children
                    && report.source_and_children_share_cutover_frontier
                    && report.no_unresolved_old_map_transaction_crosses_cutover
                    && report.all_proxies_apply_cutover_in_global_order
                    && report.every_tlog_set_durably_records_cutover
                    && report.new_map_waits_for_cutover_tlog_quorum
                    && report.every_transaction_uses_one_map_epoch
                    && report.crossing_ranges_route_to_every_child_overlap
                    && report.retired_source_requests_rejected
                    && report.retired_source_replies_rejected
                    && report.abandoned_old_map_work_retries_with_new_identity
                    && report.centralized_dispositions_exact
                    && report.exact_rows
                    && report.exact_visible_envelope_bytes
                    && report.commit_envelope_chain_valid
                    && report.exact_acknowledgement_set
                    && report.all_proxy_map_views_exact
                    && report.all_resolver_conflict_roots_exact
                    && report.all_tlog_progress_roots_exact
                    && report.durable_database_bytes_copied == 0
                    && report.maximum_pending_batches <= 8
            }));
    let negative_detected = mode == OnlineResolverSplitMode::Correct
        || (anomalies >= seed_count
            && reports
                .iter()
                .all(|report| report.negative_control_detected));
    let passed = mode == OnlineResolverSplitMode::Correct
        && exact_replay
        && process_boundaries
        && correct_contract;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "online resolver split gate discarded mode={}: anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });

    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for (report, duration) in reports.iter().zip(durations) {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "online-resolver-map-cutover"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "online-resolver-map-split"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.resolver_decisions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "resolver-map-cutover"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: 120.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "rfc0052-bounded-cell"),
                    ("range", "resolver-map-epoch-2"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: duration,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "online-resolver-map-split"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "discard" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-online-resolver-split://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }
    let budget_units = reports.iter().fold(0_u64, |total, report| {
        total
            .saturating_add(report.executed_checks)
            .saturating_add(report.sequencer_tickets)
            .saturating_add(report.resolver_decisions)
            .saturating_add(report.tlog_progress_frames)
            .saturating_add(report.tlog_durable_syncs)
            .saturating_add(report.source_history_entries)
            .saturating_add(report.child_snapshot_entries)
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "online_resolver_split.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "online_resolver_split.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "tickets={tickets}, attempts={attempts}, decisions={decisions}, progress_frames={progress_frames}"
                )),
            },
            HardGateResult {
                id: "online_resolver_split.cutover_contract".to_owned(),
                status: gate_status(correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "online_resolver_split.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(budget_units),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("online_resolver_split.tickets".to_owned(), bounded_count(tickets)),
            ("online_resolver_split.attempts".to_owned(), bounded_count(attempts)),
            ("online_resolver_split.commits".to_owned(), bounded_count(commits)),
            ("online_resolver_split.conflicts".to_owned(), bounded_count(conflicts)),
            ("online_resolver_split.abandoned".to_owned(), bounded_count(abandoned)),
            ("online_resolver_split.decisions".to_owned(), bounded_count(decisions)),
            (
                "online_resolver_split.progress_frames".to_owned(),
                bounded_count(progress_frames),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_range_phantom_history_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell range phantom history requires at least one seed".to_owned()
        ));
    }
    let mode_value = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_range_phantom_mode(mode_value) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let rounds = match workload
        .parameters
        .get("rounds")
        .and_then(toml::Value::as_integer)
        .unwrap_or(100)
        .try_into()
    {
        Ok(value) if value > 0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "cell range phantom round count must be positive".to_owned()
            ));
        }
        Err(error) => {
            return execution_from_result(Err(format!(
                "invalid cell range phantom round count: {error}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut reports: Vec<CellRangePhantomReport> = Vec::with_capacity(seeds.len());
    let mut exact_replay = true;
    for seed in seeds {
        let first = match run_cell_range_phantom_history(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_range_phantom_history(*seed, rounds, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        reports.push(first);
    }

    let anomalies = reports
        .iter()
        .map(|report| report.anomaly_count)
        .sum::<u64>();
    let attempts = reports
        .iter()
        .map(|report| report.attempted_transactions)
        .sum::<u64>();
    let commits = reports
        .iter()
        .map(|report| report.committed_transactions)
        .sum::<u64>();
    let conflicts = reports
        .iter()
        .map(|report| report.conflict_rejections)
        .sum::<u64>();
    let range_observations = reports
        .iter()
        .map(|report| report.range_observations)
        .sum::<u64>();
    let point_observations = reports
        .iter()
        .map(|report| report.point_observations)
        .sum::<u64>();
    let dependency_edges = reports
        .iter()
        .map(|report| report.dependency_edges_checked)
        .sum::<u64>();
    let dependency_cycles = reports
        .iter()
        .map(|report| report.dependency_cycles)
        .sum::<u64>();
    let checks = reports
        .iter()
        .map(|report| report.executed_checks)
        .sum::<u64>();
    let starts = reports
        .iter()
        .map(|report| report.process_starts)
        .sum::<u64>();
    let kills = reports
        .iter()
        .map(|report| report.process_kills)
        .sum::<u64>();
    let range_reads_exact = reports.iter().all(|report| report.range_reads_exact);
    let point_reads_exact = reports.iter().all(|report| report.point_reads_exact);
    let phantom_conflicts_exact = reports.iter().all(|report| report.phantom_conflicts_exact);
    let dependency_graph_acyclic = reports.iter().all(|report| report.dependency_graph_acyclic);
    let convergence_exact = reports.iter().all(|report| {
        report.all_nodes_exact && report.envelope_chain_valid && report.restarted_node_converges
    });
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = attempts
        == seed_count.saturating_mul(rounds).saturating_mul(2)
        && range_observations == seed_count.saturating_mul(rounds)
        && point_observations == seed_count.saturating_mul(rounds)
        && dependency_edges == seed_count.saturating_mul(rounds).saturating_mul(2)
        && checks == seed_count.saturating_mul(11)
        && starts == seed_count.saturating_mul(4)
        && kills == seed_count;
    let passed = anomalies == 0
        && exact_replay
        && semantic_operations_exercised
        && dependency_graph_acyclic
        && convergence_exact;
    let mismatch_details = reports
        .iter()
        .filter_map(|report| {
            report
                .first_mismatch
                .as_ref()
                .map(|detail| format!("seed {}, check: {detail}", report.seed))
        })
        .collect::<Vec<_>>();
    let error = (!passed).then(|| {
        format!(
            "cell range phantom gate failed: mode={}, anomalies={anomalies}, cycles={dependency_cycles}, exact_replay={exact_replay}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for report in &reports {
        let exact = report.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(report.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-range-phantom-v1"),
                    ("anomaly.class", if exact { "none" } else { "phantom" }),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(report.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("result", if exact { "accepted" } else { "mismatch" }),
                ]),
            },
            Measurement {
                metric: "transaction.conflicts",
                value: bounded_count(report.conflict_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("conflict.kind", "range-phantom"),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(report.dependency_edges_checked),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "range-phantom-cycle"),
                    (
                        "result",
                        if report.dependency_graph_acyclic {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if convergence_exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "range-phantom-history"),
                    ("fault", "leader-kill-between-dependent-writes"),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-range-phantom://{}/{}/{}",
            mode.id(),
            report.seed,
            report.trace_sha256
        ));
    }

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_range_phantom.exact_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_range_phantom.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "attempts={attempts}, range_reads={range_observations}, point_reads={point_observations}, edges={dependency_edges}"
                )),
            },
            HardGateResult {
                id: "cell_range_phantom.reads_exact".to_owned(),
                status: if range_reads_exact && point_reads_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_range_phantom.conflicts_exact".to_owned(),
                status: if phantom_conflicts_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("commits={commits}, conflicts={conflicts}")),
            },
            HardGateResult {
                id: "cell_range_phantom.dependency_graph_acyclic".to_owned(),
                status: if dependency_graph_acyclic {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("dependency_cycles={dependency_cycles}")),
            },
            HardGateResult {
                id: "cell_range_phantom.convergence_exact".to_owned(),
                status: if convergence_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
        ],
        budget_units: bounded_count(attempts),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_range_phantom.attempts".to_owned(), bounded_count(attempts)),
            ("cell_range_phantom.commits".to_owned(), bounded_count(commits)),
            ("cell_range_phantom.conflicts".to_owned(), bounded_count(conflicts)),
            (
                "cell_range_phantom.dependency_edges".to_owned(),
                bounded_count(dependency_edges),
            ),
            (
                "cell_range_phantom.dependency_cycles".to_owned(),
                bounded_count(dependency_cycles),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_concurrent_history_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell concurrent history workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_concurrent_history_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let requested_transactions = match workload
        .parameters
        .get("transactions")
        .and_then(toml::Value::as_integer)
        .unwrap_or(1_000)
        .try_into()
    {
        Ok(value) if value > 0 => value,
        Ok(_) => {
            return execution_from_result(Err(
                "cell concurrent history transaction count must be positive".to_owned(),
            ));
        }
        Err(error) => {
            return execution_from_result(Err(format!(
                "invalid cell concurrent history transaction count: {error}"
            )));
        }
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut attempts = 0_u64;
    let mut commits = 0_u64;
    let mut conflicts = 0_u64;
    let mut rounds = 0_u64;
    let mut starts = 0_u64;
    let mut kills = 0_u64;
    let mut retries = 0_u64;
    let mut read_observations = 0_u64;
    let mut actual_read_dependencies = 0_u64;
    let mut real_time_edges = 0_u64;
    let mut read_values_exact = true;
    let mut actual_read_dependencies_exact = true;
    let mut real_time_order_exact = true;
    let mut serializability_witness_valid = true;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first =
            match run_cell_concurrent_history(*seed, requested_transactions, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        let second =
            match run_cell_concurrent_history(*seed, requested_transactions, mode, &executable) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        attempts = attempts.saturating_add(first.attempted_transactions);
        commits = commits.saturating_add(first.committed_transactions);
        conflicts = conflicts.saturating_add(first.conflict_rejections);
        rounds = rounds.saturating_add(first.concurrent_rounds);
        starts = starts.saturating_add(first.process_starts);
        kills = kills.saturating_add(first.process_kills);
        retries = retries.saturating_add(first.duplicate_retries);
        read_observations = read_observations.saturating_add(first.read_observations);
        actual_read_dependencies =
            actual_read_dependencies.saturating_add(first.actual_read_dependencies_checked);
        real_time_edges = real_time_edges.saturating_add(first.real_time_edges_checked);
        read_values_exact &= first.read_values_exact;
        actual_read_dependencies_exact &= first.actual_read_dependencies_exact;
        real_time_order_exact &= first.real_time_order_exact;
        serializability_witness_valid &= first.serializability_witness_valid;
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-concurrent-history-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "serializability" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    (
                        "result",
                        if exact {
                            "concurrent-history-accepted"
                        } else {
                            "contract-mismatch"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.conflicts",
                value: bounded_count(first.conflict_rejections),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    ("conflict.kind", "read-write-hot-key"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "concurrent-transaction-history"),
                    ("fault", "leader-kill-lost-reply"),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(first.read_observations),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "read-value"),
                    (
                        "result",
                        if first.read_values_exact {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(first.real_time_edges_checked),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "real-time-order"),
                    (
                        "result",
                        if first.real_time_order_exact {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "serializability.constraints_checked",
                value: bounded_count(first.actual_read_dependencies_checked),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("constraint.kind", "actual-read-dependency"),
                    (
                        "result",
                        if first.actual_read_dependencies_exact {
                            "pass"
                        } else {
                            "fail"
                        },
                    ),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-concurrent-history://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_rounds = requested_transactions / 10;
    let semantic_operations_exercised = attempts
        == seed_count.saturating_mul(requested_transactions)
        && rounds == seed_count.saturating_mul(expected_rounds)
        && checks == seed_count.saturating_mul(17)
        && starts == seed_count.saturating_mul(4)
        && kills == seed_count
        && retries == seed_count
        && read_observations == seed_count.saturating_mul(expected_rounds).saturating_mul(4)
        && actual_read_dependencies > 0
        && real_time_edges > 0;
    let passed = anomalies == 0
        && exact_replay
        && semantic_operations_exercised
        && serializability_witness_valid;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "cell concurrent history gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_concurrent.exact_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_concurrent.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "attempts={attempts}, rounds={rounds}, checks={checks}, starts={starts}, kills={kills}, retries={retries}"
                )),
            },
            HardGateResult {
                id: "cell_concurrent.contract_agreement".to_owned(),
                status: if anomalies == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "cell_concurrent.read_values_exact".to_owned(),
                status: if read_values_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("read_observations={read_observations}")),
            },
            HardGateResult {
                id: "cell_concurrent.actual_read_dependencies_exact".to_owned(),
                status: if actual_read_dependencies_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_concurrent.real_time_order_exact".to_owned(),
                status: if real_time_order_exact {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!("real_time_edges_checked={real_time_edges}")),
            },
        ],
        budget_units: bounded_count(attempts),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_concurrent.attempted_transactions".to_owned(),
                bounded_count(attempts),
            ),
            (
                "cell_concurrent.committed_transactions".to_owned(),
                bounded_count(commits),
            ),
            (
                "cell_concurrent.conflict_rejections".to_owned(),
                bounded_count(conflicts),
            ),
            (
                "cell_concurrent.concurrent_rounds".to_owned(),
                bounded_count(rounds),
            ),
            (
                "cell_concurrent.read_observations".to_owned(),
                bounded_count(read_observations),
            ),
            (
                "cell_concurrent.real_time_edges_checked".to_owned(),
                bounded_count(real_time_edges),
            ),
            (
                "cell_concurrent.actual_read_dependencies_checked".to_owned(),
                bounded_count(actual_read_dependencies),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_process_transaction(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell process transaction workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_process_prototype_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut committed_transactions = 0_u64;
    let mut durable_rejections = 0_u64;
    let mut duplicate_retries = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_process_prototype(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_process_prototype(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        process_starts = process_starts.saturating_add(first.process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        committed_transactions =
            committed_transactions.saturating_add(first.committed_transactions);
        durable_rejections = durable_rejections.saturating_add(first.durable_rejections);
        duplicate_retries = duplicate_retries.saturating_add(first.duplicate_retries);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-process-transaction-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "cell_transaction" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-v0"),
                    (
                        "result",
                        if exact {
                            "semantic-raft-applied"
                        } else {
                            "contract-mismatch"
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
                    ("operation", "semantic-transaction-failover-replay"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-process-transaction://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let (checks_per_seed, starts_per_seed, kills_per_seed, commits_per_seed, retries_per_seed) =
        match mode {
            CellProcessPrototypeMode::DurableSnapshotPop => (14, 7, 4, 4, 2),
            CellProcessPrototypeMode::FreshLearnerRepair => (20, 9, 5, 4, 2),
            CellProcessPrototypeMode::LogOnlyLearnerAsRepair => (17, 6, 2, 3, 1),
            CellProcessPrototypeMode::PurgeWithoutDurableSnapshot => (11, 7, 4, 3, 1),
            CellProcessPrototypeMode::Correct | CellProcessPrototypeMode::DisableDedup => {
                (11, 4, 1, 3, 1)
            }
        };
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(checks_per_seed)
        && process_starts == seed_count.saturating_mul(starts_per_seed)
        && process_kills == seed_count.saturating_mul(kills_per_seed)
        && committed_transactions == seed_count.saturating_mul(commits_per_seed)
        && duplicate_retries == seed_count.saturating_mul(retries_per_seed);
    let expected_success_path = !matches!(
        mode,
        CellProcessPrototypeMode::Correct
            | CellProcessPrototypeMode::DurableSnapshotPop
            | CellProcessPrototypeMode::FreshLearnerRepair
    ) || durable_rejections == seed_count.saturating_mul(2);
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
            "cell process transaction gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "cell_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, starts={process_starts}, kills={process_kills}, commits={committed_transactions}, rejections={durable_rejections}, retries={duplicate_retries}"
                )),
            },
            HardGateResult {
                id: "cell_process.contract_agreement".to_owned(),
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
            ("cell_process.checks".to_owned(), bounded_count(check_count)),
            (
                "cell_process.process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "cell_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "cell_process.committed_transactions".to_owned(),
                bounded_count(committed_transactions),
            ),
            (
                "cell_process.durable_rejections".to_owned(),
                bounded_count(durable_rejections),
            ),
            (
                "cell_process.duplicate_retries".to_owned(),
                bounded_count(duplicate_retries),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_objectification(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell objectification workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_objectification_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut transaction_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut object_puts = 0_u64;
    let mut object_reads = 0_u64;
    let mut commit_frontier = 0_u64;
    let mut object_frontier = 0_u64;
    let mut snapshot_frontier = 0_u64;
    let mut safe_pop_frontier = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_objectification_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_objectification_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        transaction_starts = transaction_starts.saturating_add(first.transaction_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        object_puts = object_puts.saturating_add(first.object_puts);
        object_reads = object_reads.saturating_add(first.object_reads);
        commit_frontier = commit_frontier.saturating_add(first.commit_frontier);
        object_frontier = object_frontier.saturating_add(first.object_frontier);
        snapshot_frontier = snapshot_frontier.saturating_add(first.authority_snapshot_frontier);
        safe_pop_frontier = safe_pop_frontier.saturating_add(first.safe_log_pop_frontier);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-objectification-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "object_frontier" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "commit-objectify-reconstruct"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(first.commit_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "frontier.object_durable_version",
                value: bounded_count(first.object_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "frontier.authority_snapshot_version",
                value: bounded_count(first.authority_snapshot_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "frontier.safe_log_pop_version",
                value: bounded_count(first.safe_log_pop_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-objectification://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_puts = if mode == CellObjectificationMode::PublishIncompleteClosure {
        1
    } else {
        2
    };
    let expected_reads = if mode == CellObjectificationMode::PublishIncompleteClosure {
        0
    } else {
        2
    };
    let operations_exercised = checks == seed_count.saturating_mul(16)
        && transaction_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && process_kills == seed_count.saturating_mul(4)
        && object_puts == seed_count.saturating_mul(expected_puts)
        && object_reads == seed_count.saturating_mul(expected_reads);
    let passed = anomalies == 0 && exact_replay && operations_exercised;
    let error = (!passed).then(|| {
        format!(
            "cell objectification gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, operations_exercised={operations_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_objectification.exact_replay".to_owned(),
                status: if exact_replay { GateStatus::Pass } else { GateStatus::Fail },
                detail: None,
            },
            HardGateResult {
                id: "cell_objectification.operations_exercised".to_owned(),
                status: if operations_exercised { GateStatus::Pass } else { GateStatus::Fail },
                detail: Some(format!(
                    "checks={checks}, transaction_starts={transaction_starts}, publication_starts={publication_starts}, kills={process_kills}, puts={object_puts}, reads={object_reads}"
                )),
            },
            HardGateResult {
                id: "cell_objectification.contract_agreement".to_owned(),
                status: if anomalies == 0 { GateStatus::Pass } else { GateStatus::Fail },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_objectification.checks".to_owned(), bounded_count(checks)),
            ("cell_objectification.commit_frontier".to_owned(), bounded_count(commit_frontier)),
            ("cell_objectification.object_frontier".to_owned(), bounded_count(object_frontier)),
            ("cell_objectification.snapshot_frontier".to_owned(), bounded_count(snapshot_frontier)),
            ("cell_objectification.safe_pop_frontier".to_owned(), bounded_count(safe_pop_frontier)),
            ("cell_objectification.object_puts".to_owned(), bounded_count(object_puts)),
            ("cell_objectification.object_reads".to_owned(), bounded_count(object_reads)),
            ("cell_objectification.process_starts".to_owned(), bounded_count(transaction_starts.saturating_add(publication_starts))),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_serving_recovery(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell serving-recovery workload requires at least one seed".to_owned(),
        ));
    }
    if backend != "process-object-store-local-fs+publication-openraft+wal-quorum-fs" {
        return execution_from_result(Err(format!(
            "cell serving recovery requires process-object-store-local-fs+publication-openraft+wal-quorum-fs, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_serving_recovery_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut commit_frontier = 0_u64;
    let mut object_frontier = 0_u64;
    let mut target_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut base_envelopes = 0_u64;
    let mut suffix_envelopes = 0_u64;
    let mut suffix_records_recovered = 0_u64;
    let mut transaction_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut object_puts = 0_u64;
    let mut object_reads = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_serving_recovery_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_serving_recovery_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        commit_frontier = commit_frontier.saturating_add(first.commit_frontier);
        object_frontier = object_frontier.saturating_add(first.object_frontier);
        target_version = target_version.saturating_add(first.target_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        base_envelopes = base_envelopes.saturating_add(first.base_envelopes);
        suffix_envelopes = suffix_envelopes.saturating_add(first.suffix_envelopes);
        suffix_records_recovered =
            suffix_records_recovered.saturating_add(first.suffix_records_recovered);
        transaction_starts = transaction_starts.saturating_add(first.transaction_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        object_puts = object_puts.saturating_add(first.object_puts);
        object_reads = object_reads.saturating_add(first.object_reads);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-serving-recovery-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "serving_recovery" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-base-plus-retained-wal"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(first.target_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "frontier.object_durable_version",
                value: bounded_count(first.object_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "object_store.requests",
                value: bounded_count(first.object_reads),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("api", "serving-recovery"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-serving-recovery://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries_exercised = checks == seed_count.saturating_mul(15)
        && transaction_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && worker_starts == seed_count
        && process_kills == seed_count.saturating_mul(4)
        && object_puts == seed_count.saturating_mul(2)
        && object_reads == seed_count.saturating_mul(2)
        && commit_frontier == seed_count.saturating_mul(10)
        && object_frontier == seed_count.saturating_mul(8)
        && target_version == seed_count.saturating_mul(10)
        && base_envelopes > 0
        && suffix_envelopes > 0;
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "cell serving-recovery gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_serving_recovery.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_serving_recovery.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, transaction_starts={transaction_starts}, publication_starts={publication_starts}, worker_starts={worker_starts}, kills={process_kills}, puts={object_puts}, reads={object_reads}, C={commit_frontier}, O={object_frontier}, T={target_version}, observed={observed_frontier}, base_envelopes={base_envelopes}, suffix_envelopes={suffix_envelopes}, recovered={suffix_records_recovered}"
                )),
            },
            HardGateResult {
                id: "cell_serving_recovery.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_serving_recovery.checks".to_owned(), bounded_count(checks)),
            (
                "cell_serving_recovery.commit_frontier".to_owned(),
                bounded_count(commit_frontier),
            ),
            (
                "cell_serving_recovery.object_frontier".to_owned(),
                bounded_count(object_frontier),
            ),
            (
                "cell_serving_recovery.target_version".to_owned(),
                bounded_count(target_version),
            ),
            (
                "cell_serving_recovery.observed_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
            (
                "cell_serving_recovery.base_envelopes".to_owned(),
                bounded_count(base_envelopes),
            ),
            (
                "cell_serving_recovery.suffix_envelopes".to_owned(),
                bounded_count(suffix_envelopes),
            ),
            (
                "cell_serving_recovery.suffix_records_recovered".to_owned(),
                bounded_count(suffix_records_recovered),
            ),
            (
                "cell_serving_recovery.process_starts".to_owned(),
                bounded_count(
                    transaction_starts
                        .saturating_add(publication_starts)
                        .saturating_add(worker_starts),
                ),
            ),
            (
                "cell_serving_recovery.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "cell_serving_recovery.object_puts".to_owned(),
                bounded_count(object_puts),
            ),
            (
                "cell_serving_recovery.object_reads".to_owned(),
                bounded_count(object_reads),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_lines)]
fn run_cell_serving_authority_feed(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell serving-authority workload requires at least one seed".to_owned(),
        ));
    }
    if backend != "process-object-store-local-fs+publication-openraft+transaction-openraft" {
        return execution_from_result(Err(format!(
            "cell serving authority requires process-object-store-local-fs+publication-openraft+transaction-openraft, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_serving_authority_feed_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut commit_frontier = 0_u64;
    let mut object_frontier = 0_u64;
    let mut target_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut authority_position = 0_u64;
    let mut feed_envelopes = 0_u64;
    let mut expected_suffix_envelopes = 0_u64;
    let mut transaction_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut leader_kills = 0_u64;
    let mut copied_wal_directories = 0_u64;
    let mut object_puts = 0_u64;
    let mut object_reads = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_serving_authority_feed_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_serving_authority_feed_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        commit_frontier = commit_frontier.saturating_add(first.commit_frontier);
        object_frontier = object_frontier.saturating_add(first.object_frontier);
        target_version = target_version.saturating_add(first.target_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        authority_position = authority_position.saturating_add(first.authority_position);
        feed_envelopes = feed_envelopes.saturating_add(first.authority_feed_envelopes);
        expected_suffix_envelopes =
            expected_suffix_envelopes.saturating_add(first.expected_suffix_envelopes);
        transaction_starts = transaction_starts.saturating_add(first.transaction_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        leader_kills = leader_kills.saturating_add(first.transaction_leader_kills);
        copied_wal_directories =
            copied_wal_directories.saturating_add(first.copied_wal_directories);
        object_puts = object_puts.saturating_add(first.object_puts);
        object_reads = object_reads.saturating_add(first.object_reads);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-serving-authority-feed-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "authority_feed" },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-base-plus-live-authority-feed"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(first.target_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "frontier.object_durable_version",
                value: bounded_count(first.object_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
            Measurement {
                metric: "object_store.requests",
                value: bounded_count(first.object_reads),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("api", "authority-serving-recovery"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-serving-authority-feed://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries_exercised = checks == seed_count.saturating_mul(16)
        && transaction_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && worker_starts == seed_count
        && leader_kills == seed_count
        && copied_wal_directories == 0
        && object_puts == seed_count.saturating_mul(2)
        && object_reads == seed_count.saturating_mul(2)
        && commit_frontier == seed_count.saturating_mul(10)
        && object_frontier == seed_count.saturating_mul(8)
        && target_version == seed_count.saturating_mul(10)
        && authority_position >= target_version
        && expected_suffix_envelopes == seed_count;
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "cell serving-authority gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_serving_authority.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_serving_authority.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, transaction_starts={transaction_starts}, publication_starts={publication_starts}, worker_starts={worker_starts}, leader_kills={leader_kills}, copied_wal_dirs={copied_wal_directories}, puts={object_puts}, reads={object_reads}, C={commit_frontier}, O={object_frontier}, T={target_version}, observed={observed_frontier}, authority_position={authority_position}, feed_envelopes={feed_envelopes}, expected_suffix={expected_suffix_envelopes}"
                )),
            },
            HardGateResult {
                id: "cell_serving_authority.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_serving_authority.checks".to_owned(), bounded_count(checks)),
            (
                "cell_serving_authority.commit_frontier".to_owned(),
                bounded_count(commit_frontier),
            ),
            (
                "cell_serving_authority.object_frontier".to_owned(),
                bounded_count(object_frontier),
            ),
            (
                "cell_serving_authority.target_version".to_owned(),
                bounded_count(target_version),
            ),
            (
                "cell_serving_authority.observed_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
            (
                "cell_serving_authority.authority_position".to_owned(),
                bounded_count(authority_position),
            ),
            (
                "cell_serving_authority.feed_envelopes".to_owned(),
                bounded_count(feed_envelopes),
            ),
            (
                "cell_serving_authority.expected_suffix_envelopes".to_owned(),
                bounded_count(expected_suffix_envelopes),
            ),
            (
                "cell_serving_authority.process_starts".to_owned(),
                bounded_count(
                    transaction_starts
                        .saturating_add(publication_starts)
                        .saturating_add(worker_starts),
                ),
            ),
            (
                "cell_serving_authority.leader_kills".to_owned(),
                bounded_count(leader_kills),
            ),
            (
                "cell_serving_authority.copied_wal_directories".to_owned(),
                bounded_count(copied_wal_directories),
            ),
            (
                "cell_serving_authority.object_puts".to_owned(),
                bounded_count(object_puts),
            ),
            (
                "cell_serving_authority.object_reads".to_owned(),
                bounded_count(object_reads),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_serving_tagged_tlog(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell serving tagged-tlog workload requires at least one seed".to_owned(),
        ));
    }
    let expected_backend =
        "process-object-store-local-fs+publication-openraft+transaction-openraft+tagged-tlog";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell serving tagged-tlog requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_serving_tagged_tlog_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut commit_frontier = 0_u64;
    let mut object_frontier = 0_u64;
    let mut target_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut expected_suffix_envelopes = 0_u64;
    let mut append_acks = 0_u64;
    let mut required_tags_present = true;
    let mut backpressure_rejections = 0_u64;
    let mut survivor_responses = 0_u64;
    let mut quorum_records = 0_u64;
    let mut transaction_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut tlog_kills = 0_u64;
    let mut object_puts = 0_u64;
    let mut object_reads = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_serving_tagged_tlog_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_serving_tagged_tlog_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        commit_frontier = commit_frontier.saturating_add(first.commit_frontier);
        object_frontier = object_frontier.saturating_add(first.object_frontier);
        target_version = target_version.saturating_add(first.target_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        expected_suffix_envelopes =
            expected_suffix_envelopes.saturating_add(first.expected_suffix_envelopes);
        append_acks = append_acks.saturating_add(first.tlog_append_acks);
        required_tags_present &= first.tlog_required_tags_present;
        backpressure_rejections =
            backpressure_rejections.saturating_add(first.tlog_backpressure_rejections);
        survivor_responses = survivor_responses.saturating_add(first.tlog_survivor_responses);
        quorum_records = quorum_records.saturating_add(first.tlog_quorum_records);
        transaction_starts = transaction_starts.saturating_add(first.transaction_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tlog_process_starts);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        tlog_kills = tlog_kills.saturating_add(first.tlog_process_kills);
        object_puts = object_puts.saturating_add(first.object_puts);
        object_reads = object_reads.saturating_add(first.object_reads);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-serving-tagged-tlog-v0"),
                    ("anomaly.class", if exact { "none" } else { "tagged_tlog" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "object-base-plus-range-tagged-tlog"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(first.target_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "tag-10"),
                ]),
            },
            Measurement {
                metric: "frontier.object_durable_version",
                value: bounded_count(first.object_frontier),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "tag-10"),
                ]),
            },
            Measurement {
                metric: "object_store.requests",
                value: bounded_count(first.object_reads),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("store", "apache-object-store-local-fs"),
                    ("api", "tagged-tlog-serving-recovery"),
                    ("result", if exact { "pass" } else { "fail" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-serving-tagged-tlog://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries_exercised = checks == seed_count.saturating_mul(23)
        && transaction_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && tlog_starts == seed_count.saturating_mul(3)
        && worker_starts == seed_count
        && tlog_kills == seed_count
        && append_acks == seed_count.saturating_mul(3)
        && backpressure_rejections == seed_count.saturating_mul(3)
        && survivor_responses == seed_count.saturating_mul(2)
        && object_puts == seed_count.saturating_mul(2)
        && object_reads == seed_count.saturating_mul(2)
        && commit_frontier == seed_count.saturating_mul(10)
        && object_frontier == seed_count.saturating_mul(8)
        && target_version == seed_count.saturating_mul(10)
        && expected_suffix_envelopes == seed_count;
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "cell serving tagged-tlog gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_serving_tagged_tlog.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_serving_tagged_tlog.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, transaction_starts={transaction_starts}, publication_starts={publication_starts}, tlog_starts={tlog_starts}, worker_starts={worker_starts}, tlog_kills={tlog_kills}, append_acks={append_acks}, required_tags={required_tags_present}, backpressure_rejections={backpressure_rejections}, survivor_responses={survivor_responses}, quorum_records={quorum_records}, puts={object_puts}, reads={object_reads}, C={commit_frontier}, O={object_frontier}, T={target_version}, observed={observed_frontier}, expected_suffix={expected_suffix_envelopes}"
                )),
            },
            HardGateResult {
                id: "cell_serving_tagged_tlog.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_serving_tagged_tlog.checks".to_owned(), bounded_count(checks)),
            (
                "cell_serving_tagged_tlog.commit_frontier".to_owned(),
                bounded_count(commit_frontier),
            ),
            (
                "cell_serving_tagged_tlog.object_frontier".to_owned(),
                bounded_count(object_frontier),
            ),
            (
                "cell_serving_tagged_tlog.target_version".to_owned(),
                bounded_count(target_version),
            ),
            (
                "cell_serving_tagged_tlog.observed_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
            (
                "cell_serving_tagged_tlog.append_acks".to_owned(),
                bounded_count(append_acks),
            ),
            (
                "cell_serving_tagged_tlog.backpressure_rejections".to_owned(),
                bounded_count(backpressure_rejections),
            ),
            (
                "cell_serving_tagged_tlog.survivor_responses".to_owned(),
                bounded_count(survivor_responses),
            ),
            (
                "cell_serving_tagged_tlog.quorum_records".to_owned(),
                bounded_count(quorum_records),
            ),
            (
                "cell_serving_tagged_tlog.process_starts".to_owned(),
                bounded_count(
                    transaction_starts
                        .saturating_add(publication_starts)
                        .saturating_add(tlog_starts)
                        .saturating_add(worker_starts),
                ),
            ),
            (
                "cell_serving_tagged_tlog.process_kills".to_owned(),
                bounded_count(tlog_kills),
            ),
            (
                "cell_serving_tagged_tlog.object_puts".to_owned(),
                bounded_count(object_puts),
            ),
            (
                "cell_serving_tagged_tlog.object_reads".to_owned(),
                bounded_count(object_reads),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_cache_eviction_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "range cache-eviction gate requires at least three seeds".to_owned(),
        ));
    }
    let object_backend = match backend {
        "local-process+authority-bound-slatedb+shared-bounded-cache" => {
            RangeCacheEvictionBackend::Local
        }
        "gcs-process+authority-bound-slatedb+shared-bounded-cache" => {
            RangeCacheEvictionBackend::Gcs
        }
        other => {
            return execution_from_result(Err(format!(
                "unsupported range cache-eviction backend {other}"
            )))
        }
    };
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_cache_eviction_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "range_count")?,
            kv_runtime_profile_usize(profile, "keys_per_range")?,
            kv_runtime_profile_usize(profile, "value_bytes")?,
            kv_runtime_profile_usize(profile, "cache_limit_bytes")?,
            kv_runtime_profile_usize(profile, "cache_part_bytes")?,
            kv_runtime_profile_u64(profile, "decoded_cache_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
        ))
    })();
    let (
        range_count,
        keys_per_range,
        value_bytes,
        cache_limit_bytes,
        cache_part_bytes,
        decoded_cache_bytes,
        timeout_millis,
    ) = match values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let config_for_seed = |seed| RangeCacheEvictionConfig {
        backend: object_backend,
        range_count,
        keys_per_range,
        value_bytes,
        cache_limit_bytes,
        cache_part_bytes,
        decoded_cache_bytes,
        seed,
        mode,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_range_cache_eviction_child(&executable, &config_for_seed(*seed), timeout_millis) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_range_cache_eviction_child(
        &executable,
        &config_for_seed(seeds[0]),
        timeout_millis,
    ) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    let dimensions_exact = receipts.iter().all(|receipt| {
        receipt.format_version == 1
            && receipt.mode == mode
            && receipt.backend == object_backend
            && receipt.range_count == range_count
            && receipt.first_pass_ranges == range_count
    });
    let shared_topology = receipts
        .iter()
        .all(|receipt| receipt.shared_cache_roots == 1);
    let first_pass_exact = receipts.iter().all(|receipt| receipt.first_pass.is_exact());
    let reread_exercised = receipts
        .iter()
        .all(|receipt| receipt.reread_ranges == range_count);
    let reread_exact = receipts.iter().all(|receipt| receipt.reread.is_exact());
    let physical_bound_held = receipts.iter().all(|receipt| {
        receipt.cache_bound.held() && receipt.settled_cache_bytes <= receipt.cache_limit_bytes
    });
    let eviction_refill_observed = receipts.iter().all(|receipt| {
        receipt.eviction_refill.observed()
            && receipt.first_pass_backend_get_ranges > 0
            && receipt.reread_backend_get_ranges > 0
            && receipt.reread_backend_bytes > 0
    });
    let semantic_replay_exact = receipts
        .first()
        .is_some_and(|receipt| receipt.trace_sha256 == replay.trace_sha256);
    let scratch_cleanup = object_backend == RangeCacheEvictionBackend::Local
        || receipts
            .iter()
            .all(|receipt| receipt.scratch_objects_deleted > 0);
    let contract_checks = [
        dimensions_exact,
        shared_topology,
        first_pass_exact,
        reread_exercised,
        reread_exact,
        physical_bound_held,
        eviction_refill_observed,
        semantic_replay_exact,
        scratch_cleanup,
    ];
    let anomalies = u64::try_from(contract_checks.iter().filter(|passed| !**passed).count())
        .unwrap_or(u64::MAX);
    let negative_control_detected = mode == RangeCacheEvictionMode::Correct || anomalies > 0;
    let passed = mode == RangeCacheEvictionMode::Correct && anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "range cache-eviction gate discarded mode={}: anomalies={anomalies}, dimensions={dimensions_exact}, shared={shared_topology}, first_exact={first_pass_exact}, reread={reread_exercised}, reread_exact={reread_exact}, bounded={physical_bound_held}, refill={eviction_refill_observed}, replay={semantic_replay_exact}, cleanup={scratch_cleanup}",
            mode.id()
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "shared-cache-eviction-process-v1"),
            (
                "anomaly.class",
                if anomalies == 0 { "none" } else { mode.id() },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.push(Measurement {
            metric: "availability.success_ratio",
            value: if receipt.first_pass.is_exact() && receipt.reread.is_exact() {
                1.0
            } else {
                0.0
            },
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("operation", "shared-cache-eviction"),
                ("fault", mode.id()),
                ("backend", backend),
            ]),
        });
    }
    let total_reread_get_ranges = receipts.iter().fold(0_u64, |total, receipt| {
        total.saturating_add(receipt.reread_backend_get_ranges)
    });
    let total_reread_bytes = receipts.iter().fold(0_u64, |total, receipt| {
        total.saturating_add(receipt.reread_backend_bytes)
    });
    let maximum_cache_bytes = receipts
        .iter()
        .map(|receipt| receipt.settled_cache_bytes)
        .max()
        .unwrap_or(0);
    let scratch_objects_deleted = receipts.iter().fold(0_u64, |total, receipt| {
        total.saturating_add(receipt.scratch_objects_deleted)
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_cache_eviction.semantic_replay_exact".to_owned(),
                status: gate_status(semantic_replay_exact),
                detail: None,
            },
            HardGateResult {
                id: "range_cache_eviction.shared_topology".to_owned(),
                status: gate_status(shared_topology),
                detail: Some(format!("ranges={range_count}, cache_roots=1")),
            },
            HardGateResult {
                id: "range_cache_eviction.exact_reads".to_owned(),
                status: gate_status(first_pass_exact && reread_exact),
                detail: None,
            },
            HardGateResult {
                id: "range_cache_eviction.reread_exercised".to_owned(),
                status: gate_status(reread_exercised),
                detail: None,
            },
            HardGateResult {
                id: "range_cache_eviction.physical_bound_held".to_owned(),
                status: gate_status(physical_bound_held),
                detail: Some(format!(
                    "limit={cache_limit_bytes}, maximum_settled={maximum_cache_bytes}"
                )),
            },
            HardGateResult {
                id: "range_cache_eviction.backend_refill_observed".to_owned(),
                status: gate_status(eviction_refill_observed),
                detail: Some(format!(
                    "reread_get_ranges={total_reread_get_ranges}, reread_bytes={total_reread_bytes}"
                )),
            },
            HardGateResult {
                id: "range_cache_eviction.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
            HardGateResult {
                id: "range_cache_eviction.scratch_cleanup".to_owned(),
                status: gate_status(scratch_cleanup),
                detail: Some(format!("objects_deleted={scratch_objects_deleted}")),
            },
        ],
        budget_units: bounded_usize(range_count.saturating_mul(2).saturating_mul(seeds.len())),
        artifact_refs: receipts
            .iter()
            .map(|receipt| {
                format!(
                    "okv-range-cache-eviction://{}/{}/{}",
                    mode.id(),
                    receipt.seed,
                    receipt.trace_sha256
                )
            })
            .collect(),
        secondary_metrics: BTreeMap::from([
            (
                "range_cache_eviction.ranges".to_owned(),
                bounded_usize(range_count),
            ),
            (
                "range_cache_eviction.cache_limit_bytes".to_owned(),
                bounded_usize(cache_limit_bytes),
            ),
            (
                "range_cache_eviction.maximum_settled_cache_bytes".to_owned(),
                bounded_count(maximum_cache_bytes),
            ),
            (
                "range_cache_eviction.reread_backend_get_ranges".to_owned(),
                bounded_count(total_reread_get_ranges),
            ),
            (
                "range_cache_eviction.reread_backend_bytes".to_owned(),
                bounded_count(total_reread_bytes),
            ),
            (
                "range_cache_eviction.scratch_objects_deleted".to_owned(),
                bounded_count(scratch_objects_deleted),
            ),
        ]),
    }
}

fn run_range_cache_eviction_child(
    executable: &Path,
    config: &RangeCacheEvictionConfig,
    timeout_millis: u64,
) -> Result<RangeCacheEvictionReceipt, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let mut child = Command::new(executable)
        .arg("range-cache-eviction-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let deadline = Duration::from_millis(timeout_millis.saturating_add(5_000));
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| error.to_string())?;
                if !output.status.success() {
                    return Err(format!(
                        "range cache-eviction worker failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                return serde_json::from_slice(&output.stdout).map_err(|error| error.to_string());
            }
            Ok(None) if started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "range cache-eviction worker exceeded {} ms",
                    deadline.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.to_string());
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_cache_fault_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+authority-bound-slatedb+persistent-cache-faults";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "range cache-fault process gate requires at least three seeds".to_owned(),
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "range cache-fault process gate requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_cache_fault_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut worker_starts = 0_u64;
    let mut overwritten_parts = 0_u64;
    let mut torn_parts = 0_u64;
    let mut overwrite_refusals = 0_u64;
    let mut torn_refusals = 0_u64;
    let mut overwrite_backend_bytes = 0_u64;
    let mut torn_backend_bytes = 0_u64;
    let mut overwrite_backend_get_ranges = 0_u64;
    let mut torn_backend_get_ranges = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_range_cache_fault_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_range_cache_fault_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == replay;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        overwritten_parts = overwritten_parts.saturating_add(first.overwrite.mutated_parts);
        torn_parts = torn_parts.saturating_add(first.torn_write.mutated_parts);
        overwrite_refusals =
            overwrite_refusals.saturating_add(u64::from(first.overwrite.refused()));
        torn_refusals = torn_refusals.saturating_add(u64::from(first.torn_write.refused()));
        overwrite_backend_bytes =
            overwrite_backend_bytes.saturating_add(first.overwrite.backend_read_bytes);
        torn_backend_bytes = torn_backend_bytes.saturating_add(first.torn_write.backend_read_bytes);
        overwrite_backend_get_ranges =
            overwrite_backend_get_ranges.saturating_add(first.overwrite.backend_get_ranges);
        torn_backend_get_ranges =
            torn_backend_get_ranges.saturating_add(first.torn_write.backend_get_ranges);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "persistent-cache-fault-process-v1"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "persistent-cache-fault-process"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-range-cache-fault://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_topology_exact =
        checks == seed_count.saturating_mul(8) && worker_starts == seed_count.saturating_mul(4);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == RangeCacheFaultMode::Correct || anomalies > 0;
    let passed = mode == RangeCacheFaultMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact;
    let error = (!passed).then(|| {
        format!(
            "range cache-fault gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_cache_fault.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "range_cache_fault.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "checks={checks}, workers={worker_starts}, overwritten_parts={overwritten_parts}, torn_parts={torn_parts}, overwrite_refusals={overwrite_refusals}, torn_refusals={torn_refusals}, overwrite_backend_bytes={overwrite_backend_bytes}, torn_backend_bytes={torn_backend_bytes}, overwrite_backend_get_ranges={overwrite_backend_get_ranges}, torn_backend_get_ranges={torn_backend_get_ranges}"
                )),
            },
            HardGateResult {
                id: "range_cache_fault.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "range_cache_fault.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("range_cache_fault.checks".to_owned(), bounded_count(checks)),
            (
                "range_cache_fault.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "range_cache_fault.overwritten_parts".to_owned(),
                bounded_count(overwritten_parts),
            ),
            (
                "range_cache_fault.torn_parts".to_owned(),
                bounded_count(torn_parts),
            ),
            (
                "range_cache_fault.backend_read_bytes".to_owned(),
                bounded_count(overwrite_backend_bytes.saturating_add(torn_backend_bytes)),
            ),
            (
                "range_cache_fault.backend_get_ranges".to_owned(),
                bounded_count(
                    overwrite_backend_get_ranges.saturating_add(torn_backend_get_ranges),
                ),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_read_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+kv-runtime-routed-read+authority-bound-view";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "routed Range Engine reads require at least three seeds".to_owned(),
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "routed Range Engine reads require {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_read_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut worker_starts = 0_u64;
    let mut worker_kills = 0_u64;
    let mut point_reads = 0_u64;
    let mut scan_reads = 0_u64;
    let mut stale_refusals = 0_u64;
    let mut crossing_refusals = 0_u64;
    let mut unavailable_refusals = 0_u64;
    let mut killed_refusals = 0_u64;
    let mut wrong_values = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_range_read_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_range_read_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        worker_kills = worker_kills.saturating_add(first.worker_process_kills);
        point_reads = point_reads.saturating_add(first.point_reads);
        scan_reads = scan_reads.saturating_add(first.scan_reads);
        stale_refusals = stale_refusals.saturating_add(first.stale_route_refusals);
        crossing_refusals = crossing_refusals.saturating_add(first.crossing_scan_refusals);
        unavailable_refusals =
            unavailable_refusals.saturating_add(first.unavailable_snapshot_refusals);
        killed_refusals = killed_refusals.saturating_add(first.killed_worker_refusals);
        wrong_values = wrong_values.saturating_add(first.wrong_values);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        for nanos in &first.point_latency_nanos {
            measurements.push(Measurement {
                metric: "range_read.point_duration",
                value: bounded_count(*nanos) / 1_000_000_000.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("transport", "tcp-json-v1"),
                    ("cache.state", "process-warm"),
                ]),
            });
        }
        for nanos in &first.scan_latency_nanos {
            measurements.push(Measurement {
                metric: "range_read.scan_duration",
                value: bounded_count(*nanos) / 1_000_000_000.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("transport", "tcp-json-v1"),
                    ("cache.state", "process-warm"),
                ]),
            });
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "routed-range-read-process-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-range-read-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_kills = if mode == RangeReadProcessMode::SkipWorkerKill {
        0
    } else {
        seed_count
    };
    let process_topology_exact = worker_starts == seed_count
        && worker_kills == expected_kills
        && checks == seed_count.saturating_mul(7)
        && point_reads == seed_count.saturating_mul(64)
        && scan_reads == seed_count.saturating_mul(16);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == RangeReadProcessMode::Correct || anomalies > 0;
    let passed = mode == RangeReadProcessMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact;
    let error = (!passed).then(|| {
        format!(
            "routed read process gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_read_process.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "range_read_process.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "starts={worker_starts}, kills={worker_kills}, checks={checks}, points={point_reads}, scans={scan_reads}, stale_refusals={stale_refusals}, crossing_refusals={crossing_refusals}, unavailable_refusals={unavailable_refusals}, killed_refusals={killed_refusals}, wrong_values={wrong_values}"
                )),
            },
            HardGateResult {
                id: "range_read_process.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "range_read_process.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(point_reads.saturating_add(scan_reads)),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "range_read_process.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "range_read_process.worker_process_kills".to_owned(),
                bounded_count(worker_kills),
            ),
            (
                "range_read_process.point_reads".to_owned(),
                bounded_count(point_reads),
            ),
            (
                "range_read_process.scan_reads".to_owned(),
                bounded_count(scan_reads),
            ),
            (
                "range_read_process.wrong_values".to_owned(),
                bounded_count(wrong_values),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_route_refresh_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+kv-runtime-route-refresh+authority-bound-view";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "range-route refresh requires at least three seeds".to_owned()
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "range-route refresh requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_route_refresh_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut worker_starts = 0_u64;
    let mut worker_kills = 0_u64;
    let mut route_refreshes = 0_u64;
    let mut expected_rows = 0_u64;
    let mut observed_rows = 0_u64;
    let mut fixed_version_histories = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_range_route_refresh_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_range_route_refresh_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        worker_kills = worker_kills.saturating_add(first.worker_process_kills);
        route_refreshes = route_refreshes.saturating_add(first.route_refreshes);
        expected_rows = expected_rows.saturating_add(first.expected_rows);
        observed_rows = observed_rows.saturating_add(first.observed_rows);
        fixed_version_histories = fixed_version_histories.saturating_add(u64::from(
            first.requested_read_version == first.observed_read_version,
        ));
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "range-route-refresh-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-range-route-refresh://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_topology_exact = worker_starts == seed_count
        && worker_kills == seed_count
        && checks == seed_count.saturating_mul(6);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == RangeRouteRefreshMode::Correct || anomalies > 0;
    let passed = mode == RangeRouteRefreshMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact
        && route_refreshes == seed_count
        && fixed_version_histories == seed_count
        && observed_rows == expected_rows;
    let error = (!passed).then(|| {
        format!(
            "range-route refresh gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, refreshes={route_refreshes}, fixed_versions={fixed_version_histories}, rows={observed_rows}/{expected_rows}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_route_refresh.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "range_route_refresh.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "starts={worker_starts}, kills={worker_kills}, checks={checks}"
                )),
            },
            HardGateResult {
                id: "range_route_refresh.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "range_route_refresh.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "range_route_refresh.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "range_route_refresh.worker_process_kills".to_owned(),
                bounded_count(worker_kills),
            ),
            (
                "range_route_refresh.route_refreshes".to_owned(),
                bounded_count(route_refreshes),
            ),
            (
                "range_route_refresh.fixed_version_histories".to_owned(),
                bounded_count(fixed_version_histories),
            ),
            (
                "range_route_refresh.observed_rows".to_owned(),
                bounded_count(observed_rows),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_postgres_page_read_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+postgres-page-reader+authority-bound-view";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "PostgreSQL page reads require at least three seeds".to_owned()
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "PostgreSQL page reads require {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_postgres_page_read_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut worker_starts = 0_u64;
    let mut worker_kills = 0_u64;
    let mut route_refreshes = 0_u64;
    let mut fixed_object_versions = 0_u64;
    let mut expected_pages = 0_u64;
    let mut observed_pages = 0_u64;
    let mut vector_refusals = 0_u64;
    let mut point_refusals = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_postgres_page_read_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_postgres_page_read_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        worker_kills = worker_kills.saturating_add(first.worker_process_kills);
        route_refreshes = route_refreshes.saturating_add(first.route_refreshes);
        fixed_object_versions = fixed_object_versions.saturating_add(u64::from(
            first.requested_objectkv_version == first.observed_objectkv_version,
        ));
        expected_pages = expected_pages.saturating_add(first.expected_pages);
        observed_pages = observed_pages.saturating_add(first.observed_pages);
        vector_refusals = vector_refusals.saturating_add(u64::from(first.vector_error.is_some()));
        point_refusals = point_refusals.saturating_add(u64::from(first.point_error.is_some()));
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        measurements.push(Measurement {
            metric: "postgres.page_vector_duration",
            value: bounded_count(first.vector_duration_nanos) / 1_000_000_000.0,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("transport", "tcp-json-v1"),
                ("cache.state", "process-warm"),
            ]),
        });
        measurements.push(Measurement {
            metric: "postgres.page_point_duration",
            value: bounded_count(first.point_duration_nanos) / 1_000_000_000.0,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("transport", "tcp-json-v1"),
                ("cache.state", "process-warm"),
            ]),
        });
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "postgres-page-read-process-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-postgres-page-read://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_topology_exact = worker_starts == seed_count
        && worker_kills == seed_count
        && checks == seed_count.saturating_mul(7);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == PostgresPageReadProcessMode::Correct || anomalies > 0;
    let passed = mode == PostgresPageReadProcessMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact
        && route_refreshes == seed_count
        && fixed_object_versions == seed_count
        && observed_pages == expected_pages;
    let error = (!passed).then(|| {
        format!(
            "PostgreSQL page-read gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, refreshes={route_refreshes}, fixed_versions={fixed_object_versions}, pages={observed_pages}/{expected_pages}, vector_refusals={vector_refusals}, point_refusals={point_refusals}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "postgres_page_read.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "postgres_page_read.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "starts={worker_starts}, kills={worker_kills}, checks={checks}"
                )),
            },
            HardGateResult {
                id: "postgres_page_read.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "postgres_page_read.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "postgres_page_read.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "postgres_page_read.worker_process_kills".to_owned(),
                bounded_count(worker_kills),
            ),
            (
                "postgres_page_read.route_refreshes".to_owned(),
                bounded_count(route_refreshes),
            ),
            (
                "postgres_page_read.fixed_object_versions".to_owned(),
                bounded_count(fixed_object_versions),
            ),
            (
                "postgres_page_read.observed_pages".to_owned(),
                bounded_count(observed_pages),
            ),
            (
                "postgres_page_read.vector_refusals".to_owned(),
                bounded_count(vector_refusals),
            ),
            (
                "postgres_page_read.point_refusals".to_owned(),
                bounded_count(point_refusals),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_postgres_page_write_gate(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+postgres-page-write-admission";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "PostgreSQL page writes require at least three seeds".to_owned()
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "PostgreSQL page writes require {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_postgres_page_write_gate_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut admitted_batches = 0_u64;
    let mut admitted_mutations = 0_u64;
    let mut refusals = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_postgres_page_write_gate_contract(*seed, mode) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_postgres_page_write_gate_contract(*seed, mode) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        admitted_batches = admitted_batches.saturating_add(first.admitted_batches);
        admitted_mutations = admitted_mutations.saturating_add(first.admitted_mutations);
        refusals = refusals.saturating_add(u64::from(first.refusal.is_some()));
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "postgres-page-write-gate-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-postgres-page-write://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_checks = seed_count.saturating_mul(5);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == PostgresPageWriteGateMode::Correct || anomalies > 0;
    let expected_admission_shape = admitted_batches == seed_count
        && admitted_mutations == seed_count.saturating_mul(2)
        && refusals == 0;
    let passed = mode == PostgresPageWriteGateMode::Correct
        && contract_agreement
        && exact_replay
        && checks == expected_checks
        && expected_admission_shape;
    let error = (!passed).then(|| {
        format!(
            "PostgreSQL page-write gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, checks={checks}/{expected_checks}, batches={admitted_batches}/{seed_count}, mutations={admitted_mutations}/{}, refusals={refusals}, control_detected={negative_control_detected}; {}",
            mode.id(),
            seed_count.saturating_mul(2),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "postgres_page_write.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "postgres_page_write.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "postgres_page_write.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "postgres_page_write.admitted_batches".to_owned(),
                bounded_count(admitted_batches),
            ),
            (
                "postgres_page_write.admitted_mutations".to_owned(),
                bounded_count(admitted_mutations),
            ),
            (
                "postgres_page_write.refusals".to_owned(),
                bounded_count(refusals),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_postgres_page_commit_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+cell-v0+postgres-page-extent";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "PostgreSQL page commits require at least three seeds".to_owned()
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "PostgreSQL page commits require {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_postgres_page_commit_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut process_starts = 0_u64;
    let mut leader_handoffs = 0_u64;
    let mut page_mutations = 0_u64;
    let mut extent_mutations = 0_u64;
    let mut retry_responses_exact = 0_u64;
    let mut pages_after_failover = 0_u64;
    let mut exact_replay = true;
    let mut extents_exact = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_postgres_page_commit_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_postgres_page_commit_process_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        process_starts = process_starts.saturating_add(first.cell_process_starts);
        leader_handoffs = leader_handoffs.saturating_add(first.leader_handoffs);
        page_mutations = page_mutations.saturating_add(first.committed_page_mutations);
        extent_mutations = extent_mutations.saturating_add(first.committed_extent_mutations);
        retry_responses_exact = retry_responses_exact.saturating_add(first.retry_responses_exact);
        pages_after_failover =
            pages_after_failover.saturating_add(first.pages_visible_after_failover);
        extents_exact &= first.extent_nblocks_after_failover == Some(2);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "postgres-page-commit-process-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-postgres-page-commit://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_topology_exact = process_starts == seed_count.saturating_mul(4)
        && leader_handoffs == seed_count
        && checks == seed_count.saturating_mul(8);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == PostgresPageCommitProcessMode::Correct || anomalies > 0;
    let state_shape_exact = page_mutations == seed_count.saturating_mul(2)
        && extent_mutations == seed_count
        && retry_responses_exact == seed_count
        && pages_after_failover == seed_count.saturating_mul(2)
        && extents_exact;
    let passed = mode == PostgresPageCommitProcessMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact
        && state_shape_exact;
    let error = (!passed).then(|| {
        format!(
            "PostgreSQL page-commit gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, pages={page_mutations}/{}, extents={extent_mutations}/{seed_count}, retry_exact={retry_responses_exact}/{seed_count}, failover_pages={pages_after_failover}/{}, extent_values_exact={extents_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            seed_count.saturating_mul(2),
            seed_count.saturating_mul(2),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "postgres_page_commit.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "postgres_page_commit.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "starts={process_starts}, handoffs={leader_handoffs}, checks={checks}"
                )),
            },
            HardGateResult {
                id: "postgres_page_commit.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "postgres_page_commit.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "postgres_page_commit.cell_process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "postgres_page_commit.leader_handoffs".to_owned(),
                bounded_count(leader_handoffs),
            ),
            (
                "postgres_page_commit.page_mutations".to_owned(),
                bounded_count(page_mutations),
            ),
            (
                "postgres_page_commit.extent_mutations".to_owned(),
                bounded_count(extent_mutations),
            ),
            (
                "postgres_page_commit.retry_responses_exact".to_owned(),
                bounded_count(retry_responses_exact),
            ),
            (
                "postgres_page_commit.pages_after_failover".to_owned(),
                bounded_count(pages_after_failover),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_postgres_object_delta(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "postgres-certified-object-delta-local-filesystem";
    if seeds.is_empty() {
        return execution_from_result(Err(
            "PostgreSQL object-delta baseline requires at least one seed".to_owned(),
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "PostgreSQL object-delta baseline requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .or_else(|| workload.parameters.get("durable_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_postgres_object_delta_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let relation_blocks = match workload.parameters.get("relation_blocks") {
        Some(value) => match value
            .as_integer()
            .and_then(|blocks| u32::try_from(blocks).ok())
        {
            Some(blocks) => blocks,
            None => {
                return execution_from_result(Err(
                    "PostgreSQL object-delta relation_blocks must be an unsigned 32-bit integer"
                        .to_owned(),
                ));
            }
        },
        None => 128,
    };
    let reference_full_base_rewrite = workload
        .parameters
        .get("reference_full_base_rewrite")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let contract_config = PostgresObjectDeltaContractConfig {
        relation_blocks,
        reference_full_base_rewrite,
    };
    let mut anomalies = 0_u64;
    let mut check_count = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut object_delta_segments = 0_u64;
    let mut object_delta_bytes = 0_u64;
    let mut objectification_input_bytes = 0_u64;
    let mut object_delta_layers = 0_u64;
    let mut object_compaction_debt_bytes = 0_u64;
    let mut full_base_rewrite_bytes = 0_u64;
    let mut ssts_before_checkpoint = 0_u64;
    let mut ssts_after_checkpoint = 0_u64;
    let mut rewrite_ratio_samples = Vec::new();
    let mut materialization_duration_samples = Vec::new();
    let mut activation_duration_samples = Vec::new();
    let mut restart_duration_samples = Vec::new();
    let mut end_to_end_duration_samples = Vec::new();
    let mut full_rewrite_duration_samples = Vec::new();
    let mut delta_to_full_rewrite_ratio_samples = Vec::new();
    let mut delta_to_full_rewrite_duration_ratio_samples = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first =
            match run_postgres_object_delta_contract_with_config(*seed, mode, contract_config) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        let replay =
            match run_postgres_object_delta_contract_with_config(*seed, mode, contract_config) {
                Ok(report) => report,
                Err(error) => return execution_from_result(Err(error)),
            };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        check_count =
            check_count.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        object_delta_segments = object_delta_segments.saturating_add(first.object_delta_segments);
        object_delta_bytes = object_delta_bytes.saturating_add(first.object_delta_bytes);
        objectification_input_bytes =
            objectification_input_bytes.saturating_add(first.objectification_input_bytes);
        object_delta_layers = object_delta_layers.saturating_add(first.object_delta_layers);
        object_compaction_debt_bytes =
            object_compaction_debt_bytes.saturating_add(first.object_compaction_debt_bytes);
        full_base_rewrite_bytes =
            full_base_rewrite_bytes.saturating_add(first.full_base_rewrite_bytes);
        ssts_before_checkpoint =
            ssts_before_checkpoint.saturating_add(first.ssts_before_checkpoint);
        ssts_after_checkpoint = ssts_after_checkpoint.saturating_add(first.ssts_after_checkpoint);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let result = if first.anomaly_count == 0 {
            "accepted"
        } else {
            "discarded"
        };
        let metric_attributes = attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("backend", backend),
            ("result", result),
        ]);
        let rewrite_ratio = if first.objectification_input_bytes == 0 {
            f64::INFINITY
        } else {
            bounded_count(first.object_delta_bytes)
                / bounded_count(first.objectification_input_bytes)
        };
        let materialization_duration =
            Duration::from_nanos(first.object_delta_materialization_duration_nanos).as_secs_f64();
        let activation_duration =
            Duration::from_nanos(first.object_delta_activation_duration_nanos).as_secs_f64();
        let restart_duration =
            Duration::from_nanos(first.object_delta_restart_duration_nanos).as_secs_f64();
        let end_to_end_duration = materialization_duration + activation_duration;
        rewrite_ratio_samples.push(rewrite_ratio);
        materialization_duration_samples.push(materialization_duration);
        activation_duration_samples.push(activation_duration);
        restart_duration_samples.push(restart_duration);
        end_to_end_duration_samples.push(end_to_end_duration);
        measurements.extend([
            Measurement {
                metric: "postgres.object_delta_segments",
                value: bounded_count(first.object_delta_segments),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_bytes",
                value: bounded_count(first.object_delta_bytes),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.objectification_input_bytes",
                value: bounded_count(first.objectification_input_bytes),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.objectification_rewrite_ratio",
                value: rewrite_ratio,
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_layers",
                value: bounded_count(first.object_delta_layers),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_compaction_debt_bytes",
                value: bounded_count(first.object_compaction_debt_bytes),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_materialization_duration",
                value: materialization_duration,
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_activation_duration",
                value: activation_duration,
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_restart_duration",
                value: restart_duration,
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.object_delta_end_to_end_duration",
                value: end_to_end_duration,
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.relation_pages",
                value: bounded_count(first.relation_pages),
                attributes: metric_attributes.clone(),
            },
            Measurement {
                metric: "postgres.relation_bytes",
                value: bounded_count(first.relation_bytes),
                attributes: metric_attributes,
            },
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "postgres-object-delta-v0"),
                    (
                        "anomaly.class",
                        if first.anomaly_count == 0 {
                            "none"
                        } else {
                            mode.id()
                        },
                    ),
                ]),
            },
        ]);
        if first.full_base_rewrite_bytes > 0 {
            let full_rewrite_duration =
                Duration::from_nanos(first.full_base_rewrite_duration_nanos).as_secs_f64();
            let delta_to_full_rewrite_ratio = bounded_count(first.object_delta_bytes)
                / bounded_count(first.full_base_rewrite_bytes);
            let delta_to_full_rewrite_duration_ratio = if full_rewrite_duration == 0.0 {
                f64::INFINITY
            } else {
                end_to_end_duration / full_rewrite_duration
            };
            full_rewrite_duration_samples.push(full_rewrite_duration);
            delta_to_full_rewrite_ratio_samples.push(delta_to_full_rewrite_ratio);
            delta_to_full_rewrite_duration_ratio_samples.push(delta_to_full_rewrite_duration_ratio);
            let reference_attributes = attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend),
                ("result", result),
            ]);
            measurements.extend([
                Measurement {
                    metric: "postgres.full_base_rewrite_duration",
                    value: full_rewrite_duration,
                    attributes: reference_attributes.clone(),
                },
                Measurement {
                    metric: "postgres.full_base_rewrite_bytes",
                    value: bounded_count(first.full_base_rewrite_bytes),
                    attributes: reference_attributes.clone(),
                },
                Measurement {
                    metric: "postgres.delta_to_full_rewrite_bytes_ratio",
                    value: delta_to_full_rewrite_ratio,
                    attributes: reference_attributes.clone(),
                },
                Measurement {
                    metric: "postgres.delta_to_full_rewrite_duration_ratio",
                    value: delta_to_full_rewrite_duration_ratio,
                    attributes: reference_attributes,
                },
            ]);
        }
        artifact_refs.push(format!(
            "okv-postgres-object-delta://{}/{seed}/{}?delta_sha256={}",
            mode.id(),
            first.trace_sha256,
            first.object_delta_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == PostgresObjectDeltaMode::Correct
        || aggregate_checks
            .get("negative_control_detected")
            .is_some_and(|detected| *detected);
    let storage_shape_exact = object_delta_segments == seed_count
        && object_delta_layers == seed_count
        && objectification_input_bytes
            == seed_count.saturating_mul(u64::try_from(8 * 1024).unwrap_or(u64::MAX));
    let no_replacement_sst = ssts_before_checkpoint == ssts_after_checkpoint;
    let passed = mode == PostgresObjectDeltaMode::Correct
        && contract_agreement
        && exact_replay
        && storage_shape_exact
        && no_replacement_sst;
    let error = (!passed).then(|| {
        format!(
            "PostgreSQL object-delta gate discarded mode={}: anomalies={anomalies}, replay={exact_replay}, shape={storage_shape_exact}, ssts={ssts_before_checkpoint}/{ssts_after_checkpoint}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "postgres_object_delta.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "postgres_object_delta.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "postgres_object_delta.storage_shape_exact".to_owned(),
                status: gate_status(storage_shape_exact),
                detail: Some(format!(
                    "segments={object_delta_segments}, layers={object_delta_layers}, input_bytes={objectification_input_bytes}"
                )),
            },
            HardGateResult {
                id: "postgres_object_delta.no_replacement_sst".to_owned(),
                status: gate_status(no_replacement_sst),
                detail: Some(format!(
                    "ssts_before={ssts_before_checkpoint}, ssts_after={ssts_after_checkpoint}"
                )),
            },
            HardGateResult {
                id: "postgres_object_delta.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "postgres_object_delta.segments".to_owned(),
                bounded_count(object_delta_segments),
            ),
            (
                "postgres_object_delta.bytes".to_owned(),
                bounded_count(object_delta_bytes),
            ),
            (
                "postgres_object_delta.input_bytes".to_owned(),
                bounded_count(objectification_input_bytes),
            ),
            (
                "postgres_object_delta.relation_pages".to_owned(),
                bounded_count(u64::from(relation_blocks)),
            ),
            (
                "postgres_object_delta.relation_bytes".to_owned(),
                bounded_count(u64::from(relation_blocks)) * 8_192.0,
            ),
            (
                "postgres_object_delta.compaction_debt_bytes".to_owned(),
                bounded_count(object_compaction_debt_bytes),
            ),
            (
                "postgres_full_base_rewrite.bytes".to_owned(),
                bounded_count(full_base_rewrite_bytes),
            ),
            (
                "postgres_object_delta.rewrite_ratio_median".to_owned(),
                median(&rewrite_ratio_samples),
            ),
            (
                "postgres_object_delta.materialization_duration_median_seconds".to_owned(),
                median(&materialization_duration_samples),
            ),
            (
                "postgres_object_delta.activation_duration_median_seconds".to_owned(),
                median(&activation_duration_samples),
            ),
            (
                "postgres_object_delta.restart_duration_median_seconds".to_owned(),
                median(&restart_duration_samples),
            ),
            (
                "postgres_object_delta.end_to_end_duration_median_seconds".to_owned(),
                median(&end_to_end_duration_samples),
            ),
            (
                "postgres_full_base_rewrite.duration_median_seconds".to_owned(),
                if full_rewrite_duration_samples.is_empty() {
                    0.0
                } else {
                    median(&full_rewrite_duration_samples)
                },
            ),
            (
                "postgres_object_delta.delta_to_full_rewrite_bytes_ratio_median".to_owned(),
                if delta_to_full_rewrite_ratio_samples.is_empty() {
                    0.0
                } else {
                    median(&delta_to_full_rewrite_ratio_samples)
                },
            ),
            (
                "postgres_object_delta.delta_to_full_rewrite_duration_ratio_median".to_owned(),
                if delta_to_full_rewrite_duration_ratio_samples.is_empty() {
                    0.0
                } else {
                    median(&delta_to_full_rewrite_duration_ratio_samples)
                },
            ),
        ]),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_postgres_worker_readiness_child(
    executable: &Path,
    seed: u64,
    relation_blocks: u32,
    range_pages: u32,
    oracle_chunk_pages: u32,
    max_rss_bytes: u64,
    mode: PostgresWorkerReadinessMode,
    timeout_millis: u64,
) -> Result<PostgresWorkerReadinessReceipt, String> {
    let temporary = tempfile::tempdir().map_err(|error| error.to_string())?;
    let config = prepare_postgres_worker_readiness_fixture(
        temporary.path().join("durable"),
        seed,
        relation_blocks,
        range_pages,
        oracle_chunk_pages,
        max_rss_bytes,
        mode,
    )?;
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let mut child = Command::new(executable)
        .arg("postgres-worker-readiness-node")
        .arg("--config-json")
        .arg(config_json)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn PostgreSQL replacement worker: {error}"))?;
    let deadline = Duration::from_millis(timeout_millis.saturating_add(5_000));
    let wait_started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("collect PostgreSQL replacement worker: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "PostgreSQL replacement worker failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                return serde_json::from_slice(&output.stdout).map_err(|error| {
                    format!("parse PostgreSQL replacement-worker receipt: {error}")
                });
            }
            Ok(None) if wait_started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "PostgreSQL replacement worker exceeded controller timeout of {} ms",
                    deadline.as_millis()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("poll PostgreSQL replacement worker: {error}"));
            }
        }
    }
}

fn postgres_worker_readiness_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &PostgresWorkerReadinessReceipt,
) -> Vec<Measurement> {
    let relation_pages = receipt.relation_pages.to_string();
    let result =
        if receipt.mode == PostgresWorkerReadinessMode::Correct && receipt.anomaly_count == 0 {
            "accepted"
        } else {
            "discarded"
        };
    let attrs = |phase: &str| {
        attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("result", result),
            ("relation.pages", relation_pages.as_str()),
            ("phase", phase),
        ])
    };
    let seconds = |nanos| Duration::from_nanos(nanos).as_secs_f64();
    vec![
        Measurement {
            metric: "postgres.worker_root_load_duration",
            value: seconds(receipt.root_load_duration_nanos),
            attributes: attrs("durable-root-load"),
        },
        Measurement {
            metric: "postgres.worker_delta_auth_duration",
            value: seconds(receipt.delta_auth_duration_nanos),
            attributes: attrs("delta-lineage-auth"),
        },
        Measurement {
            metric: "postgres.worker_view_ready_duration",
            value: seconds(receipt.view_ready_duration_nanos),
            attributes: attrs("manifest-bound-view-ready"),
        },
        Measurement {
            metric: "postgres.worker_first_delta_point_duration",
            value: seconds(receipt.first_delta_point_duration_nanos),
            attributes: attrs("first-delta-overlay-point"),
        },
        Measurement {
            metric: "postgres.worker_first_base_point_duration",
            value: seconds(receipt.first_base_point_duration_nanos),
            attributes: attrs("first-immutable-base-point"),
        },
        Measurement {
            metric: "postgres.worker_first_range_duration",
            value: seconds(receipt.first_range_duration_nanos),
            attributes: attrs("first-bounded-range"),
        },
        Measurement {
            metric: "postgres.worker_full_oracle_duration",
            value: seconds(receipt.full_oracle_duration_nanos),
            attributes: attrs("bounded-memory-full-oracle"),
        },
        Measurement {
            metric: "postgres.worker_closure_audit_duration",
            value: seconds(receipt.closure_audit_duration_nanos),
            attributes: attrs("complete-physical-closure-audit"),
        },
        Measurement {
            metric: "postgres.worker_peak_rss_bytes",
            value: bounded_count(receipt.peak_rss_bytes),
            attributes: attrs("worker-rss-after-oracle"),
        },
        Measurement {
            metric: "postgres.worker_closure_bytes",
            value: bounded_count(receipt.physical_closure_bytes),
            attributes: attrs("selected-physical-closure"),
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn run_postgres_worker_readiness(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "postgres-object-root-local-filesystem-process";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "PostgreSQL replacement-worker curve requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.len() < 5 {
        return execution_from_result(Err(
            "PostgreSQL replacement-worker curve requires at least five seeds".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .or_else(|| workload.parameters.get("mode"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_postgres_worker_readiness_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let values = (|| {
        let relation_blocks =
            u32::try_from(kv_runtime_workload_usize(workload, "relation_blocks")?)
                .map_err(|error| error.to_string())?;
        let range_pages = u32::try_from(kv_runtime_profile_usize(profile, "range_pages")?)
            .map_err(|error| error.to_string())?;
        let oracle_chunk_pages =
            u32::try_from(kv_runtime_profile_usize(profile, "oracle_chunk_pages")?)
                .map_err(|error| error.to_string())?;
        Ok::<_, String>((
            relation_blocks,
            range_pages,
            oracle_chunk_pages,
            kv_runtime_profile_u64(profile, "max_rss_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
        ))
    })();
    let (relation_blocks, range_pages, oracle_chunk_pages, max_rss_bytes, timeout_millis) =
        match values {
            Ok(values) => values,
            Err(error) => return execution_from_result(Err(error)),
        };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_postgres_worker_readiness_child(
            &executable,
            *seed,
            relation_blocks,
            range_pages,
            oracle_chunk_pages,
            max_rss_bytes,
            mode,
            timeout_millis,
        ) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_postgres_worker_readiness_child(
        &executable,
        seeds[0],
        relation_blocks,
        range_pages,
        oracle_chunk_pages,
        max_rss_bytes,
        mode,
        timeout_millis,
    ) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    let parent_process_id = std::process::id();
    let process_boundary = receipts
        .iter()
        .all(|receipt| receipt.worker_process_id != parent_process_id);
    let dimensions_exact = receipts.iter().all(|receipt| {
        receipt.contract_version == 1
            && receipt.mode == mode
            && receipt.relation_pages == u64::from(relation_blocks)
            && receipt.relation_bytes == u64::from(relation_blocks).saturating_mul(8 * 1024)
    });
    let source_free = receipts.iter().all(|receipt| receipt.source_heap_absent);
    let roots_exact = receipts.iter().all(|receipt| receipt.root_identity_exact);
    let deltas_exact = receipts.iter().all(|receipt| receipt.delta_lineage_exact);
    let reads_exact = receipts.iter().all(|receipt| {
        receipt.first_delta_point_exact
            && receipt.first_base_point_exact
            && receipt.first_range_exact
            && receipt.full_oracle_exact
            && receipt.full_oracle_bounded
    });
    let closure_audited = receipts
        .iter()
        .all(|receipt| receipt.closure_audit_executed && receipt.closure_audit_exact);
    let rss_bound = receipts
        .iter()
        .all(|receipt| receipt.rss_bound_held && receipt.peak_rss_bytes > 0);
    let negative_control_detected = receipts
        .iter()
        .all(|receipt| receipt.negative_control_detected)
        && match mode {
            PostgresWorkerReadinessMode::Correct => true,
            PostgresWorkerReadinessMode::ChangedManifest => receipts.iter().all(|receipt| {
                receipt
                    .refusal_phase
                    .as_deref()
                    .is_some_and(|phase| phase.starts_with("view_open:"))
            }),
            PostgresWorkerReadinessMode::ChangedDelta => receipts.iter().all(|receipt| {
                receipt
                    .refusal_phase
                    .as_deref()
                    .is_some_and(|phase| phase.starts_with("delta_auth:"))
            }),
            PostgresWorkerReadinessMode::SkipClosureAudit => receipts.iter().all(|receipt| {
                reads_exact && !receipt.closure_audit_executed && !receipt.closure_audit_exact
            }),
        };
    let semantic_replay_exact = receipts
        .first()
        .is_some_and(|receipt| receipt.semantic_receipt_sha256 == replay.semantic_receipt_sha256);
    let subject_anomalies = receipts.iter().fold(0_u64, |total, receipt| {
        total.saturating_add(receipt.anomaly_count)
    });
    let structural_anomalies = u64::try_from(
        [
            process_boundary,
            dimensions_exact,
            semantic_replay_exact,
            negative_control_detected,
        ]
        .iter()
        .filter(|passed| !**passed)
        .count(),
    )
    .unwrap_or(u64::MAX);
    let anomalies = subject_anomalies.saturating_add(structural_anomalies);
    let passed = mode == PostgresWorkerReadinessMode::Correct
        && anomalies == 0
        && source_free
        && roots_exact
        && deltas_exact
        && reads_exact
        && closure_audited
        && rss_bound;
    let error = (!passed).then(|| {
        format!(
            "PostgreSQL replacement-worker curve discarded mode={}: anomalies={anomalies}, process={process_boundary}, dimensions={dimensions_exact}, source_free={source_free}, roots={roots_exact}, deltas={deltas_exact}, reads={reads_exact}, audit={closure_audited}, rss={rss_bound}, replay={semantic_replay_exact}, control={negative_control_detected}",
            mode.id()
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "postgres-replacement-worker-v0"),
            (
                "anomaly.class",
                if anomalies == 0 { "none" } else { mode.id() },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(postgres_worker_readiness_measurements(
            workload, backend, receipt,
        ));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return execution_from_result(Err(error)),
    };
    let artifact_path = postgres_worker_readiness_artifact_path(run_id, candidate_commit, workload);
    let artifact = PostgresWorkerReadinessArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        mode: mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay,
    };
    if let Err(error) = write_json_artifact(
        &artifact_path,
        &artifact,
        "PostgreSQL replacement-worker curve",
    ) {
        return execution_from_result(Err(error));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    let view_ready_samples = receipts
        .iter()
        .map(|receipt| Duration::from_nanos(receipt.view_ready_duration_nanos).as_secs_f64())
        .collect::<Vec<_>>();
    let first_base_samples = receipts
        .iter()
        .map(|receipt| Duration::from_nanos(receipt.first_base_point_duration_nanos).as_secs_f64())
        .collect::<Vec<_>>();
    let closure_samples = receipts
        .iter()
        .map(|receipt| Duration::from_nanos(receipt.closure_audit_duration_nanos).as_secs_f64())
        .collect::<Vec<_>>();
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            gate("postgres_worker.process_boundary", process_boundary),
            gate("postgres_worker.dimensions_exact", dimensions_exact),
            gate("postgres_worker.source_heap_absent", source_free),
            gate("postgres_worker.root_identity_exact", roots_exact),
            gate("postgres_worker.delta_lineage_exact", deltas_exact),
            gate("postgres_worker.reads_exact", reads_exact),
            gate("postgres_worker.closure_audited", closure_audited),
            gate("postgres_worker.rss_bound", rss_bound),
            gate("postgres_worker.semantic_replay", semantic_replay_exact),
            gate(
                "postgres_worker.negative_control_detected",
                negative_control_detected,
            ),
        ],
        budget_units: bounded_count(
            u64::try_from(receipts.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(11),
        ),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "postgres_worker.view_ready_median_seconds".to_owned(),
                median(&view_ready_samples),
            ),
            (
                "postgres_worker.first_base_point_median_seconds".to_owned(),
                median(&first_base_samples),
            ),
            (
                "postgres_worker.closure_audit_median_seconds".to_owned(),
                median(&closure_samples),
            ),
            (
                "postgres_worker.physical_closure_bytes".to_owned(),
                receipts
                    .first()
                    .map_or(0.0, |receipt| bounded_count(receipt.physical_closure_bytes)),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_serving_concurrency_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "local-process+slatedb-object-base+authenticated-tail+atomic-view";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "concurrent Range Engine publication requires at least three seeds".to_owned(),
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "concurrent Range Engine publication requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_serving_concurrency_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut worker_starts = 0_u64;
    let mut publications = 0_u64;
    let mut retained_old_reads = 0_u64;
    let mut current_new_reads = 0_u64;
    let mut mixed_results = 0_u64;
    let mut stale_refusals = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_range_serving_concurrency_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_range_serving_concurrency_contract(*seed, mode, &executable) {
            Ok(receipt) => receipt,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first.trace_sha256 == replay.trace_sha256;
        worker_starts = worker_starts.saturating_add(1);
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(u64::try_from(first.checks.len()).unwrap_or(u64::MAX));
        publications = publications.saturating_add(first.publications);
        retained_old_reads = retained_old_reads.saturating_add(first.retained_old_reads);
        current_new_reads = current_new_reads.saturating_add(first.current_new_reads);
        mixed_results = mixed_results.saturating_add(first.mixed_results);
        stale_refusals = stale_refusals.saturating_add(u64::from(first.stale_probe_refused));
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        for nanos in &first.publication_nanos {
            measurements.push(Measurement {
                metric: "range_serving.publication_swap_duration",
                value: bounded_count(*nanos) / 1_000_000_000.0,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    (
                        "result",
                        if first.anomaly_count == 0 {
                            "exact"
                        } else {
                            "anomaly"
                        },
                    ),
                ]),
            });
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "concurrent-immutable-view-publication-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        mode.id()
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-range-serving-concurrency://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_topology_exact = worker_starts == seed_count
        && checks == seed_count.saturating_mul(7)
        && publications == seed_count.saturating_mul(6)
        && current_new_reads == seed_count.saturating_mul(48);
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let negative_control_detected = mode == RangeServingConcurrencyMode::Correct || anomalies > 0;
    let passed = mode == RangeServingConcurrencyMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact;
    let error = (!passed).then(|| {
        format!(
            "concurrent Range Engine publication discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_serving_concurrency.exact_process_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "range_serving_concurrency.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "workers={worker_starts}, checks={checks}, publications={publications}, retained_old_reads={retained_old_reads}, current_new_reads={current_new_reads}, mixed_results={mixed_results}, stale_refusals={stale_refusals}"
                )),
            },
            HardGateResult {
                id: "range_serving_concurrency.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "range_serving_concurrency.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(
            publications
                .saturating_add(retained_old_reads)
                .saturating_add(current_new_reads),
        ),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "range_serving_concurrency.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "range_serving_concurrency.publications".to_owned(),
                bounded_count(publications),
            ),
            (
                "range_serving_concurrency.retained_old_reads".to_owned(),
                bounded_count(retained_old_reads),
            ),
            (
                "range_serving_concurrency.current_new_reads".to_owned(),
                bounded_count(current_new_reads),
            ),
            (
                "range_serving_concurrency.mixed_results".to_owned(),
                bounded_count(mixed_results),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_range_serving_handoff(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str =
        "local-process+slatedb-object-base+openraft-publication+signed-txlog-quorums";
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "range serving handoff requires at least three seeds".to_owned()
        ));
    }
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "range serving handoff requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_range_serving_handoff_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut transaction_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut txlog_starts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut authority_kills = 0_u64;
    let mut txlog_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut base_m0_frontier = 0_u64;
    let mut base_m1_frontier = 0_u64;
    let mut target_version = 0_u64;
    let mut m0_tail_records = 0_u64;
    let mut m1_tail_records = 0_u64;
    let mut post_gc_tail_records = 0_u64;
    let mut txlog_certificates = 0_u64;
    let mut reclamation_candidates = 0_u64;
    let mut lease_protected_rejections = 0_u64;
    let mut delete_permits = 0_u64;
    let mut reclaimed_objects = 0_u64;
    let mut cache_resurrection_attempts = 0_u64;
    let mut cache_resurrection_opened = 0_u64;
    let mut authority_unavailable_attempts = 0_u64;
    let mut authority_unavailable_opened = 0_u64;
    let mut exact_replay = true;
    let mut aggregate_checks = BTreeMap::<String, bool>::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_range_serving_handoff_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let replay = match run_range_serving_handoff_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == replay;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        transaction_starts = transaction_starts.saturating_add(first.transaction_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        txlog_starts = txlog_starts.saturating_add(first.txlog_process_starts);
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        authority_kills = authority_kills.saturating_add(first.authority_process_kills);
        txlog_kills = txlog_kills.saturating_add(first.txlog_process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        base_m0_frontier = base_m0_frontier.saturating_add(first.base_m0_frontier);
        base_m1_frontier = base_m1_frontier.saturating_add(first.base_m1_frontier);
        target_version = target_version.saturating_add(first.target_version);
        m0_tail_records = m0_tail_records.saturating_add(first.m0_tail_records);
        m1_tail_records = m1_tail_records.saturating_add(first.m1_tail_records);
        post_gc_tail_records = post_gc_tail_records.saturating_add(first.post_gc_tail_records);
        txlog_certificates = txlog_certificates.saturating_add(first.txlog_certificates);
        reclamation_candidates =
            reclamation_candidates.saturating_add(first.reclamation_candidates);
        lease_protected_rejections =
            lease_protected_rejections.saturating_add(first.lease_protected_rejections);
        delete_permits = delete_permits.saturating_add(first.delete_permits);
        reclaimed_objects = reclaimed_objects.saturating_add(first.reclaimed_objects);
        cache_resurrection_attempts =
            cache_resurrection_attempts.saturating_add(first.cache_resurrection_attempts);
        cache_resurrection_opened =
            cache_resurrection_opened.saturating_add(first.cache_resurrection_opened);
        authority_unavailable_attempts =
            authority_unavailable_attempts.saturating_add(first.authority_unavailable_attempts);
        authority_unavailable_opened =
            authority_unavailable_opened.saturating_add(first.authority_unavailable_opened);
        for (check, passed) in &first.checks {
            aggregate_checks
                .entry(check.clone())
                .and_modify(|aggregate| *aggregate &= *passed)
                .or_insert(*passed);
        }
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "authority-base-certified-tail-handoff-v1"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "authority-base-certified-tail-handoff"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-range-serving-handoff://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let authority_failure_count = if mode == RangeServingHandoffMode::SkipAuthorityFailover {
        0
    } else {
        seed_count
    };
    let process_topology_exact = checks == seed_count.saturating_mul(21)
        && transaction_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && txlog_starts == seed_count.saturating_mul(6)
        && worker_starts == seed_count.saturating_mul(5)
        && authority_kills == authority_failure_count
        && txlog_kills == seed_count.saturating_mul(2)
        && authority_failovers == authority_failure_count
        && base_m0_frontier == seed_count.saturating_mul(3)
        && base_m1_frontier == seed_count.saturating_mul(5)
        && target_version == seed_count.saturating_mul(10)
        && reclamation_candidates >= seed_count
        && cache_resurrection_attempts == seed_count
        && authority_unavailable_attempts == seed_count;
    let negative_control_detected = mode == RangeServingHandoffMode::Correct || anomalies > 0;
    let contract_agreement = anomalies == 0 && aggregate_checks.values().all(|passed| *passed);
    let passed = mode == RangeServingHandoffMode::Correct
        && contract_agreement
        && exact_replay
        && process_topology_exact;
    let error = (!passed).then(|| {
        format!(
            "range serving handoff discarded mode={}: anomalies={anomalies}, replay={exact_replay}, topology={process_topology_exact}, control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "range_serving_handoff.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "range_serving_handoff.process_topology_exact".to_owned(),
                status: gate_status(process_topology_exact),
                detail: Some(format!(
                    "checks={checks}, transaction_starts={transaction_starts}, publication_starts={publication_starts}, txlog_starts={txlog_starts}, worker_starts={worker_starts}, authority_kills={authority_kills}, txlog_kills={txlog_kills}, authority_failovers={authority_failovers}, M0={base_m0_frontier}, M1={base_m1_frontier}, T={target_version}, M0_tail={m0_tail_records}, M1_tail={m1_tail_records}, post_gc_tail={post_gc_tail_records}, certificates={txlog_certificates}, candidates={reclamation_candidates}, protected={lease_protected_rejections}, permits={delete_permits}, reclaimed={reclaimed_objects}, cache_resurrection_attempts={cache_resurrection_attempts}, cache_resurrection_opened={cache_resurrection_opened}, authority_unavailable_attempts={authority_unavailable_attempts}, authority_unavailable_opened={authority_unavailable_opened}"
                )),
            },
            HardGateResult {
                id: "range_serving_handoff.contract_agreement".to_owned(),
                status: gate_status(contract_agreement),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "range_serving_handoff.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("range_serving_handoff.checks".to_owned(), bounded_count(checks)),
            (
                "range_serving_handoff.process_starts".to_owned(),
                bounded_count(
                    transaction_starts
                        .saturating_add(publication_starts)
                        .saturating_add(txlog_starts)
                        .saturating_add(worker_starts),
                ),
            ),
            (
                "range_serving_handoff.process_kills".to_owned(),
                bounded_count(authority_kills.saturating_add(txlog_kills)),
            ),
            (
                "range_serving_handoff.txlog_certificates".to_owned(),
                bounded_count(txlog_certificates),
            ),
            (
                "range_serving_handoff.target_version".to_owned(),
                bounded_count(target_version),
            ),
            (
                "range_serving_handoff.reclamation_candidates".to_owned(),
                bounded_count(reclamation_candidates),
            ),
            (
                "range_serving_handoff.reclaimed_objects".to_owned(),
                bounded_count(reclaimed_objects),
            ),
            (
                "range_serving_handoff.cache_resurrection_attempts".to_owned(),
                bounded_count(cache_resurrection_attempts),
            ),
            (
                "range_serving_handoff.cache_resurrection_opened".to_owned(),
                bounded_count(cache_resurrection_opened),
            ),
            (
                "range_serving_handoff.authority_unavailable_attempts".to_owned(),
                bounded_count(authority_unavailable_attempts),
            ),
            (
                "range_serving_handoff.authority_unavailable_opened".to_owned(),
                bounded_count(authority_unavailable_opened),
            ),
        ]),
    }
}

fn run_range_serving_curve_child(
    executable: &Path,
    config: &RangeServingCurveConfig,
    timeout_millis: u64,
) -> Result<RangeServingCurveReceipt, String> {
    let config_json = serde_json::to_string(config)
        .map_err(|error| format!("serialize range-serving curve config: {error}"))?;
    let mut child = Command::new(executable)
        .arg("range-serving-curve-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn range-serving curve child: {error}"))?;
    let deadline = Duration::from_millis(timeout_millis.saturating_add(5_000));
    let wait_started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|error| format!("collect range-serving curve child: {error}"))?;
                if !output.status.success() {
                    let error = format!(
                        "range-serving curve child failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                    return Err(with_range_serving_child_cleanup(config, error));
                }
                return serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse range-serving curve receipt: {error}"));
            }
            Ok(None) if wait_started.elapsed() <= deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let error = format!(
                    "range-serving curve child exceeded controller timeout of {} ms",
                    deadline.as_millis()
                );
                return Err(with_range_serving_child_cleanup(config, error));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(with_range_serving_child_cleanup(
                    config,
                    format!("poll range-serving curve child: {error}"),
                ));
            }
        }
    }
}

fn with_range_serving_child_cleanup(config: &RangeServingCurveConfig, error: String) -> String {
    if config.object_backend != RangeServingObjectBackend::Gcs {
        return error;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(cleanup_error) => {
            return format!("{error}; build GCS cleanup runtime: {cleanup_error}")
        }
    };
    match runtime.block_on(cleanup_range_serving_curve_gcs_scratch(config)) {
        Ok(_) => error,
        Err(cleanup_error) => format!("{error}; GCS scratch cleanup failed: {cleanup_error}"),
    }
}

fn range_serving_curve_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &RangeServingCurveReceipt,
) -> Vec<Measurement> {
    let base_keys = receipt.base_key_count.to_string();
    let tail_records = receipt.tail_records.to_string();
    let attrs = |phase: &str| {
        attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("base.keys", base_keys.as_str()),
            ("tail.records", tail_records.as_str()),
            ("cache.mode", receipt.cache_mode.as_str()),
            ("phase", phase),
        ])
    };
    let point_reads = f64::from(u32::try_from(receipt.point_samples).unwrap_or(u32::MAX)).max(1.0);
    vec![
        Measurement {
            metric: "range_serving.view_open_duration",
            value: receipt.view_open_seconds,
            attributes: attrs("view-open"),
        },
        Measurement {
            metric: "range_serving.tail_auth_duration",
            value: receipt.tail_auth_seconds,
            attributes: attrs("tail-auth-index"),
        },
        Measurement {
            metric: "range_serving.first_point_duration",
            value: receipt.first_point_seconds,
            attributes: attrs("first-point-process-cold-os-warm"),
        },
        Measurement {
            metric: "range_serving.warm_point_p99",
            value: receipt.warm_point_p99_seconds,
            attributes: attrs("warm-point"),
        },
        Measurement {
            metric: "range_serving.scan_throughput",
            value: receipt.scan_rows_per_second,
            attributes: attrs("ordered-scan"),
        },
        Measurement {
            metric: "range_serving.object_requests_per_read",
            value: f64::from(
                u32::try_from(receipt.warm_point_io.request_total()).unwrap_or(u32::MAX),
            ) / point_reads,
            attributes: attrs("ram-warm-point-reads"),
        },
        Measurement {
            metric: "range_serving.cache_prepare_backend_requests",
            value: bounded_count(receipt.cache_prepare_io.request_total()),
            attributes: attrs("cache-prepare"),
        },
        Measurement {
            metric: "range_serving.peak_rss_bytes",
            value: bounded_count(receipt.peak_rss_bytes),
            attributes: attrs("worker-peak-rss"),
        },
    ]
}

#[allow(clippy::too_many_lines)]
fn run_range_serving_performance_curve(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const EXPECTED_BACKEND: &str = "local-process+authority-slate-base+certified-txlog-tail";
    if backend != EXPECTED_BACKEND {
        return execution_from_result(Err(format!(
            "range-serving curve requires {EXPECTED_BACKEND}, got {backend}"
        )));
    }
    if seeds.len() < 3 {
        return execution_from_result(Err(
            "range-serving curve requires at least three seeds".to_owned()
        ));
    }
    let workload_values = (|| {
        Ok::<_, String>((
            kv_runtime_workload_usize(workload, "base_key_count")?,
            kv_runtime_workload_usize(workload, "tail_records")?,
        ))
    })();
    let (base_key_count, tail_records) = match workload_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let cache_mode = match workload
        .parameters
        .get("cache_mode")
        .and_then(toml::Value::as_str)
        .unwrap_or("raw")
    {
        "raw" => RangeServingCacheMode::Raw,
        "shared_ram_nvme" => RangeServingCacheMode::SharedRamNvme,
        "metadata_reopen" => RangeServingCacheMode::MetadataReopen,
        "nvme_reopen" => RangeServingCacheMode::NvmeReopen,
        other => {
            return execution_from_result(Err(format!("unknown range-serving cache mode {other}")))
        }
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "value_bytes")?,
            kv_runtime_profile_usize(profile, "point_samples")?,
            kv_runtime_profile_usize(profile, "scan_rows")?,
            kv_runtime_profile_u64(profile, "max_rss_bytes")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
            kv_runtime_profile_u64(profile, "decoded_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_part_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_open_file_handles")?,
        ))
    })();
    let (
        value_bytes,
        point_samples,
        configured_scan_rows,
        max_rss_bytes,
        timeout_millis,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
    ) = match profile_values {
        Ok(values) => values,
        Err(error) => return execution_from_result(Err(error)),
    };
    let scan_rows = configured_scan_rows.min(base_key_count);
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return execution_from_result(Err(format!(
                "resolve range-serving curve executable: {error}"
            )))
        }
    };
    let config_for_seed = |seed| RangeServingCurveConfig {
        base_key_count,
        value_bytes,
        tail_records,
        point_samples,
        scan_rows,
        max_rss_bytes,
        cache_mode,
        decoded_cache_bytes,
        nvme_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
        provider_mode: RangeServingProviderMode::default(),
        object_backend: RangeServingObjectBackend::Local,
        scratch_prefix: None,
        warmup_reads: 0,
        measured_reads: 0,
        economics: None,
        seed,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_range_serving_curve_child(&executable, &config_for_seed(*seed), timeout_millis) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => return execution_from_result(Err(error)),
        }
    }
    let replay = match run_range_serving_curve_child(
        &executable,
        &config_for_seed(seeds[0]),
        timeout_millis,
    ) {
        Ok(receipt) => receipt,
        Err(error) => return execution_from_result(Err(error)),
    };
    let revision_exact = receipts
        .iter()
        .all(|receipt| receipt.slatedb_revision == SLATEDB_REVISION);
    let dimensions_exact = receipts.iter().all(|receipt| {
        receipt.base_key_count == base_key_count
            && receipt.tail_records == tail_records
            && receipt.authenticated_tail_records == u64::try_from(tail_records).unwrap_or(u64::MAX)
            && receipt.cache_mode == cache_mode.id()
    });
    let reads_exact = receipts.iter().all(|receipt| {
        receipt.first_point_exact && receipt.warm_points_exact && receipt.ordered_scan_exact
    });
    let io_accounted = receipts.iter().all(|receipt| {
        let setup_observed = match cache_mode {
            RangeServingCacheMode::Raw | RangeServingCacheMode::SharedRamNvme => {
                receipt.open_io.request_total() > 0
            }
            RangeServingCacheMode::MetadataReopen | RangeServingCacheMode::NvmeReopen => {
                receipt.cache_prepare_io.request_total() > 0
            }
        };
        setup_observed
            && receipt.total_io.read_byte_total() > 0
            && receipt.total_io.request_total() > 0
    });
    let safety_bounds_held = receipts
        .iter()
        .all(|receipt| receipt.safety_bounds_held && receipt.peak_rss_bytes > 0);
    let cache_request_contract = receipts.iter().all(|receipt| match cache_mode {
        RangeServingCacheMode::Raw => receipt.warm_point_io.request_total() > 0,
        RangeServingCacheMode::SharedRamNvme => receipt.warm_point_io.request_total() == 0,
        RangeServingCacheMode::MetadataReopen => {
            receipt.cache_prepare_io.request_total() > 0
                && receipt.first_point_io.read_byte_total() > 0
                && receipt.warm_point_io.request_total() == 0
        }
        RangeServingCacheMode::NvmeReopen => {
            receipt.cache_prepare_io.request_total() > 0
                && receipt.open_io.request_total() > 0
                && receipt.first_point_io.read_byte_total() == 0
                && receipt
                    .first_point_io
                    .successful_requests
                    .get("get_range")
                    .copied()
                    .unwrap_or(0)
                    == 0
                && receipt.warm_point_io.request_total() == 0
        }
    });
    let scan_request_bound = receipts.iter().all(|receipt| match cache_mode {
        RangeServingCacheMode::Raw => receipt.scan_io.request_total() <= 96,
        RangeServingCacheMode::SharedRamNvme | RangeServingCacheMode::MetadataReopen => {
            receipt.scan_io.request_total() <= 4
        }
        RangeServingCacheMode::NvmeReopen => receipt.scan_io.request_total() == 0,
    });
    let semantic_replay_exact = receipts
        .first()
        .is_some_and(|receipt| receipt.semantic_receipt_sha256 == replay.semantic_receipt_sha256);
    let checks = [
        revision_exact,
        dimensions_exact,
        reads_exact,
        io_accounted,
        safety_bounds_held,
        cache_request_contract,
        scan_request_bound,
        semantic_replay_exact,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let passed = anomalies == 0;
    let error = (!passed).then(|| {
        format!(
            "range-serving performance curve discarded: anomalies={anomalies}, dimensions={dimensions_exact}, reads={reads_exact}, io={io_accounted}, bounds={safety_bounds_held}, cache={cache_request_contract}, scan_requests={scan_request_bound}, replay={semantic_replay_exact}"
        )
    });
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "authority-base-certified-tail-curve-v1"),
            (
                "anomaly.class",
                if passed { "none" } else { "curve-contract" },
            ),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(range_serving_curve_measurements(workload, backend, receipt));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return execution_from_result(Err(error)),
    };
    let artifact_path = range_serving_curve_artifact_path(run_id, candidate_commit, workload);
    let artifact = RangeServingCurveArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        receipts: &receipts,
        semantic_replay_receipt: &replay,
    };
    if let Err(error) = write_json_artifact(&artifact_path, &artifact, "range-serving curve") {
        return execution_from_result(Err(error));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            gate("range_serving_curve.minimum_seeds", seeds.len() >= 3),
            gate("range_serving_curve.revision_exact", revision_exact),
            gate("range_serving_curve.dimensions_exact", dimensions_exact),
            gate("range_serving_curve.reads_exact", reads_exact),
            gate("range_serving_curve.io_accounted", io_accounted),
            gate(
                "range_serving_curve.cache_request_contract",
                cache_request_contract,
            ),
            gate("range_serving_curve.scan_request_bound", scan_request_bound),
            gate("range_serving_curve.safety_bounds_held", safety_bounds_held),
            gate(
                "range_serving_curve.semantic_replay_exact",
                semantic_replay_exact,
            ),
        ],
        budget_units: bounded_usize(base_key_count.saturating_add(tail_records))
            * bounded_usize(seeds.len()),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "range_serving_curve.base_keys".to_owned(),
                bounded_usize(base_key_count),
            ),
            (
                "range_serving_curve.tail_records".to_owned(),
                bounded_usize(tail_records),
            ),
            (
                "range_serving_curve.peak_rss_bytes".to_owned(),
                receipts
                    .iter()
                    .map(|receipt| bounded_count(receipt.peak_rss_bytes))
                    .fold(0.0_f64, f64::max),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_provider_bound_range_read(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const LOCAL_BACKEND: &str = "deterministic-versioned-object-store-process";
    const GCS_BACKEND: &str = "gcs-generation-bound-process";
    const LOCAL_PRICE_SNAPSHOT: &str = "local-versioned-zero";
    const GCS_PRICE_SNAPSHOT: &str = "gcs-us-central1-standard-2026-08-24";
    // Standard regional flat-namespace Class B operations: $0.0004 per 1,000.
    const GCS_CLASS_B_GET_USD: f64 = 0.004 / 10_000.0;
    let object_backend = match backend {
        LOCAL_BACKEND => RangeServingObjectBackend::Local,
        GCS_BACKEND => RangeServingObjectBackend::Gcs,
        other => {
            return provider_bound_discard(
                workload,
                other,
                "unresolved",
                format!("unknown provider-bound backend {other}"),
            )
        }
    };
    let (price_snapshot, get_request_cost_usd) = match object_backend {
        RangeServingObjectBackend::Local => (LOCAL_PRICE_SNAPSHOT, 0.0),
        RangeServingObjectBackend::Gcs => {
            let Some(configured_snapshot) = profile
                .parameters
                .get("price_snapshot")
                .and_then(toml::Value::as_str)
            else {
                return provider_bound_discard(
                    workload,
                    backend,
                    "unresolved",
                    "GCS provider-bound profile requires price_snapshot".to_owned(),
                );
            };
            if configured_snapshot != GCS_PRICE_SNAPSHOT {
                return provider_bound_discard(
                    workload,
                    backend,
                    "unresolved",
                    format!(
                        "unsupported GCS price snapshot {configured_snapshot}; expected {GCS_PRICE_SNAPSHOT}"
                    ),
                );
            }
            for variable in ["OKV_GCP_PROJECT", "OKV_GCS_BUCKET"] {
                if std::env::var(variable)
                    .ok()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return provider_bound_discard(
                        workload,
                        backend,
                        "unresolved",
                        format!("GCS provider-bound profile requires {variable}"),
                    );
                }
            }
            (GCS_PRICE_SNAPSHOT, GCS_CLASS_B_GET_USD)
        }
    };
    let Some(dataset) = dataset else {
        return provider_bound_discard(
            workload,
            backend,
            "unresolved",
            "provider-bound range read requires a dataset".to_owned(),
        );
    };
    if seeds.len() < 5 {
        return provider_bound_discard(
            workload,
            backend,
            "unresolved",
            "provider-bound range read requires at least five seeds".to_owned(),
        );
    }
    let cache_state = workload
        .parameters
        .get("cache_state")
        .and_then(toml::Value::as_str)
        .unwrap_or("empty-cache");
    let cache_mode = match cache_state {
        "metadata-warm-data-cold" => RangeServingCacheMode::MetadataReopen,
        "persistent-nvme-warm-decoded-ram-cold" => RangeServingCacheMode::NvmeReopen,
        "empty-cache" => RangeServingCacheMode::SharedRamNvme,
        other => {
            return provider_bound_discard(
                workload,
                backend,
                other,
                format!("unknown provider-bound cache state {other}"),
            )
        }
    };
    let provider_mode = match workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("correct")
    {
        "correct" | "none" => RangeServingProviderMode::Correct,
        "changed_generation" => RangeServingProviderMode::ChangedGeneration,
        "same_bytes_new_generation" => RangeServingProviderMode::SameBytesNewGeneration,
        "missing_revision" => RangeServingProviderMode::MissingRevision,
        "changed_bytes" => RangeServingProviderMode::ChangedBytes,
        "changed_namespace" => RangeServingProviderMode::ChangedNamespace,
        "skip_revision_enforcement" => RangeServingProviderMode::SkipRevisionEnforcement,
        other => {
            return provider_bound_discard(
                workload,
                backend,
                cache_state,
                format!("unknown provider-bound control {other}"),
            )
        }
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "point_bytes")?,
            kv_runtime_profile_usize(profile, "range_points")?,
            kv_runtime_profile_usize(profile, "warmup_reads")?,
            kv_runtime_profile_usize(profile, "measured_reads")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
        ))
    })();
    let (value_bytes, range_points, warmup_reads, measured_reads, timeout_millis) =
        match profile_values {
            Ok(values) => values,
            Err(error) => return provider_bound_discard(workload, backend, cache_state, error),
        };
    let Ok(base_key_count) = usize::try_from(dataset.key_count) else {
        return provider_bound_discard(
            workload,
            backend,
            cache_state,
            "provider-bound key count does not fit usize".to_owned(),
        );
    };
    let expected_logical_bytes = dataset
        .key_count
        .saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX));
    if expected_logical_bytes != dataset.logical_bytes || range_points > base_key_count {
        return provider_bound_discard(
            workload,
            backend,
            cache_state,
            "provider-bound dataset dimensions disagree with profile".to_owned(),
        );
    }
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return provider_bound_discard(
                workload,
                backend,
                cache_state,
                format!("resolve provider-bound executable: {error}"),
            )
        }
    };
    let config_for_seed = |seed| RangeServingCurveConfig {
        base_key_count,
        value_bytes,
        tail_records: 0,
        point_samples: measured_reads,
        scan_rows: range_points,
        max_rss_bytes: 1_073_741_824,
        cache_mode,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 134_217_728,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode,
        object_backend,
        scratch_prefix: (object_backend == RangeServingObjectBackend::Gcs).then(|| {
            format!(
                "scratch/provider-bound-range/{}/{}/{}/{}",
                workload.id,
                provider_mode.id(),
                seed,
                Uuid::new_v4()
            )
        }),
        warmup_reads,
        measured_reads,
        economics: None,
        seed,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_range_serving_curve_child(&executable, &config_for_seed(*seed), timeout_millis) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => {
                return provider_bound_discard(
                    workload,
                    backend,
                    cache_state,
                    format!("provider-bound control detected: {error}"),
                )
            }
        }
    }
    let replay = match run_range_serving_curve_child(
        &executable,
        &config_for_seed(seeds[0]),
        timeout_millis,
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            return provider_bound_discard(
                workload,
                backend,
                cache_state,
                format!("provider-bound replay failed: {error}"),
            )
        }
    };
    let dimensions_exact = receipts.iter().all(|receipt| {
        receipt.base_key_count == base_key_count
            && receipt.value_bytes == value_bytes
            && receipt.point_samples == measured_reads
            && receipt.scan_rows == range_points
            && receipt.cache_mode == cache_mode.id()
            && receipt.provider_mode == provider_mode.id()
            && receipt.object_backend == object_backend.id()
    });
    let reads_exact = receipts.iter().all(|receipt| {
        receipt.first_point_exact && receipt.warm_points_exact && receipt.ordered_scan_exact
    });
    let provider_identity_exact = receipts.iter().all(|receipt| {
        receipt.provider_closure_sha256.len() == 64
            && receipt.provider_get_requests > 0
            && receipt.provider_get_requests == receipt.provider_revision_checks
            && receipt.provider_refused_requests == 0
            && receipt.unversioned_fallbacks == 0
    });
    let request_and_bytes_observed = receipts
        .iter()
        .all(|receipt| receipt.provider_read_bytes > 0 && receipt.total_io.request_total() > 0);
    let cache_state_exact = receipts.iter().all(|receipt| match cache_mode {
        RangeServingCacheMode::MetadataReopen => {
            receipt.cache_prepare_io.request_total() > 0
                && receipt.first_point_io.read_byte_total() > 0
                && receipt.warm_point_io.request_total() == 0
        }
        RangeServingCacheMode::NvmeReopen => {
            receipt.cache_prepare_io.request_total() > 0
                && receipt.first_point_io.read_byte_total() == 0
                && receipt.warm_point_io.request_total() == 0
        }
        RangeServingCacheMode::SharedRamNvme => {
            receipt.cache_prepare_io.request_total() == 0
                && receipt.first_point_io.read_byte_total() > 0
                && receipt.warm_point_io.request_total() == 0
        }
        RangeServingCacheMode::Raw => false,
    });
    let empty_cache_budget = cache_mode != RangeServingCacheMode::SharedRamNvme
        || receipts.iter().all(|receipt| {
            receipt.first_point_io.request_total() <= 8
                && receipt.first_point_io.read_byte_total() <= 524_288
                && receipt.first_point_seconds <= 0.1
        });
    let semantic_replay_exact = receipts
        .first()
        .is_some_and(|receipt| receipt.semantic_receipt_sha256 == replay.semantic_receipt_sha256);
    let safety_bounds_held = receipts
        .iter()
        .all(|receipt| receipt.safety_bounds_held && receipt.peak_rss_bytes > 0);
    let scratch_cleanup_complete = receipts.iter().all(|receipt| {
        receipt.scratch_cleanup_complete
            && (object_backend == RangeServingObjectBackend::Local
                || receipt.scratch_objects_deleted > 0)
    });
    let checks = [
        dimensions_exact,
        reads_exact,
        provider_identity_exact,
        request_and_bytes_observed,
        cache_state_exact,
        empty_cache_budget,
        semantic_replay_exact,
        safety_bounds_held,
        scratch_cleanup_complete,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let passed = anomalies == 0;
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("oracle", "provider-bound-range-v2"),
            ("anomaly.class", if passed { "none" } else { "contract" }),
        ]),
    }];
    for receipt in &receipts {
        measurements.extend(provider_bound_measurements(
            workload,
            backend,
            cache_state,
            receipt,
            price_snapshot,
            get_request_cost_usd,
        ));
    }
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return provider_bound_discard(workload, backend, cache_state, error),
    };
    let artifact_path = provider_bound_range_artifact_path(run_id, candidate_commit, workload);
    let artifact = ProviderBoundRangeArtifact {
        contract_version: 2,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        cache_state,
        provider_mode: provider_mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay,
    };
    if let Err(error) = write_json_artifact(&artifact_path, &artifact, "provider-bound range") {
        return provider_bound_discard(workload, backend, cache_state, error);
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error: (!passed).then(|| {
            format!(
                "provider-bound range discarded: anomalies={anomalies}, dimensions={dimensions_exact}, reads={reads_exact}, identity={provider_identity_exact}, io={request_and_bytes_observed}, cache={cache_state_exact}, budget={empty_cache_budget}, replay={semantic_replay_exact}, bounds={safety_bounds_held}, cleanup={scratch_cleanup_complete}"
            )
        }),
        measurements,
        hard_gates: vec![
            gate("provider_bound.minimum_seeds", seeds.len() >= 5),
            gate("provider_bound.dimensions_exact", dimensions_exact),
            gate("provider_bound.reads_exact", reads_exact),
            gate("provider_bound.identity_exact", provider_identity_exact),
            gate("provider_bound.request_and_bytes_observed", request_and_bytes_observed),
            gate("provider_bound.cache_state_exact", cache_state_exact),
            gate("provider_bound.empty_cache_budget", empty_cache_budget),
            gate("provider_bound.semantic_replay_exact", semantic_replay_exact),
            gate("provider_bound.safety_bounds_held", safety_bounds_held),
            gate(
                "provider_bound.scratch_cleanup_complete",
                scratch_cleanup_complete,
            ),
        ],
        budget_units: bounded_usize(measured_reads.saturating_mul(seeds.len())),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "provider_bound.get_requests".to_owned(),
                bounded_count(
                    receipts
                        .iter()
                        .map(|receipt| receipt.provider_get_requests)
                        .sum(),
                ),
            ),
            (
                "provider_bound.read_bytes".to_owned(),
                bounded_count(
                    receipts
                        .iter()
                        .map(|receipt| receipt.provider_read_bytes)
                        .sum(),
                ),
            ),
            (
                "provider_bound.scratch_objects_deleted".to_owned(),
                bounded_count(
                    receipts
                        .iter()
                        .map(|receipt| receipt.scratch_objects_deleted)
                        .sum(),
                ),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_provider_bound_cache_economics(
    workload: &WorkloadConfig,
    run_id: &str,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
    dataset: Option<&DatasetConfig>,
    profile: &ProfileConfig,
) -> WorkloadExecution {
    const LOCAL_BACKEND: &str = "deterministic-versioned-provider-cache-process";
    const GCS_BACKEND: &str = "gcs-generation-bound-provider-cache-process";
    const LOCAL_PRICE_SNAPSHOT: &str = "local-versioned-zero";
    const GCS_PRICE_SNAPSHOT: &str = "gcs-us-central1-standard-2026-08-24";
    const GCS_GET_COST_NANO_USD: u64 = 400;
    let object_backend = match backend {
        LOCAL_BACKEND => RangeServingObjectBackend::Local,
        GCS_BACKEND => RangeServingObjectBackend::Gcs,
        other => {
            return provider_bound_discard(
                workload,
                other,
                "bounded-nvme",
                format!("unknown provider cache economics backend {other}"),
            )
        }
    };
    let (price_snapshot, provider_get_cost_nano_usd) = match object_backend {
        RangeServingObjectBackend::Local => (LOCAL_PRICE_SNAPSHOT, 0),
        RangeServingObjectBackend::Gcs => {
            let Some(configured_snapshot) = profile
                .parameters
                .get("price_snapshot")
                .and_then(toml::Value::as_str)
            else {
                return provider_bound_discard(
                    workload,
                    backend,
                    "bounded-nvme",
                    "GCS provider cache economics profile requires price_snapshot".to_owned(),
                );
            };
            if configured_snapshot != GCS_PRICE_SNAPSHOT {
                return provider_bound_discard(
                    workload,
                    backend,
                    "bounded-nvme",
                    format!(
                        "unsupported GCS price snapshot {configured_snapshot}; expected {GCS_PRICE_SNAPSHOT}"
                    ),
                );
            }
            for variable in ["OKV_GCP_PROJECT", "OKV_GCS_BUCKET"] {
                if std::env::var(variable)
                    .ok()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return provider_bound_discard(
                        workload,
                        backend,
                        "bounded-nvme",
                        format!("GCS provider cache economics profile requires {variable}"),
                    );
                }
            }
            (GCS_PRICE_SNAPSHOT, GCS_GET_COST_NANO_USD)
        }
    };
    let Some(dataset) = dataset else {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache economics requires a dataset".to_owned(),
        );
    };
    if seeds.len() < 5 {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache economics requires at least five seeds".to_owned(),
        );
    }
    let distribution = match workload
        .parameters
        .get("distribution")
        .and_then(toml::Value::as_str)
    {
        Some("uniform") => ProviderCacheTraceDistribution::Uniform,
        Some("zipfian") => ProviderCacheTraceDistribution::Zipfian,
        Some("moving_hotset") => ProviderCacheTraceDistribution::MovingHotset,
        Some(other) => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                format!("unknown provider cache distribution {other}"),
            )
        }
        None => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                "provider cache economics requires distribution".to_owned(),
            )
        }
    };
    let negative_control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let (economics_mode, provider_mode) = match negative_control {
        "none" | "correct" => (
            ProviderCacheEconomicsMode::Correct,
            RangeServingProviderMode::Correct,
        ),
        "disable_cache_bound" => (
            ProviderCacheEconomicsMode::DisableCacheBound,
            RangeServingProviderMode::Correct,
        ),
        "skip_exact_oracle" => (
            ProviderCacheEconomicsMode::SkipExactOracle,
            RangeServingProviderMode::Correct,
        ),
        "skip_revision_enforcement" => (
            ProviderCacheEconomicsMode::Correct,
            RangeServingProviderMode::SkipRevisionEnforcement,
        ),
        "perturb_replay_trace" => (
            ProviderCacheEconomicsMode::PerturbReplayTrace,
            RangeServingProviderMode::Correct,
        ),
        other => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                format!("unknown provider cache economics control {other}"),
            )
        }
    };
    let profile_values = (|| {
        Ok::<_, String>((
            kv_runtime_profile_usize(profile, "point_bytes")?,
            kv_runtime_profile_usize(profile, "warmup_reads")?,
            kv_runtime_profile_usize(profile, "measured_reads")?,
            kv_runtime_profile_u64(profile, "decoded_cache_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_part_bytes")?,
            kv_runtime_profile_usize(profile, "nvme_open_file_handles")?,
            kv_runtime_profile_u64(profile, "worker_timeout_millis")?,
        ))
    })();
    let (
        value_bytes,
        warmup_reads,
        measured_reads,
        decoded_cache_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
        timeout_millis,
    ) = match profile_values {
        Ok(values) => values,
        Err(error) => return provider_bound_discard(workload, backend, "bounded-nvme", error),
    };
    let cache_fraction_ppm = match kv_runtime_workload_usize(workload, "cache_fraction_ppm") {
        Ok(value @ 1..=1_000_000) => value,
        Ok(other) => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                format!("cache_fraction_ppm must be in 1..=1000000, got {other}"),
            )
        }
        Err(error) => return provider_bound_discard(workload, backend, "bounded-nvme", error),
    };
    let optional_workload_usize = |key: &str| {
        workload
            .parameters
            .get(key)
            .and_then(toml::Value::as_integer)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0)
    };
    let zipf_theta_milli =
        u32::try_from(optional_workload_usize("zipf_theta_milli")).unwrap_or(u32::MAX);
    let hotset_fraction_ppm =
        u32::try_from(optional_workload_usize("hotset_fraction_ppm")).unwrap_or(u32::MAX);
    let hot_read_fraction_ppm =
        u32::try_from(optional_workload_usize("hot_read_fraction_ppm")).unwrap_or(u32::MAX);
    let hotset_shift_every = optional_workload_usize("hotset_shift_every");
    let view_reopen_every = optional_workload_usize("view_reopen_every");
    let Ok(base_key_count) = usize::try_from(dataset.key_count) else {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache key count does not fit usize".to_owned(),
        );
    };
    let expected_logical_bytes = dataset
        .key_count
        .saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX));
    if expected_logical_bytes != dataset.logical_bytes {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache dataset dimensions disagree with profile".to_owned(),
        );
    }
    let cache_capacity_u64 = dataset
        .logical_bytes
        .saturating_mul(u64::try_from(cache_fraction_ppm).unwrap_or(u64::MAX))
        / 1_000_000;
    let Ok(cache_capacity_bytes) = usize::try_from(cache_capacity_u64) else {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache capacity does not fit usize".to_owned(),
        );
    };
    if cache_capacity_bytes < nvme_part_bytes.saturating_mul(2) {
        return provider_bound_discard(
            workload,
            backend,
            "bounded-nvme",
            "provider cache capacity must hold at least two physical parts".to_owned(),
        );
    }
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                format!("resolve provider cache executable: {error}"),
            )
        }
    };
    let config_for_seed = |seed| RangeServingCurveConfig {
        base_key_count,
        value_bytes,
        tail_records: 0,
        point_samples: measured_reads,
        scan_rows: 1,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes,
        nvme_cache_bytes: cache_capacity_bytes,
        nvme_part_bytes,
        nvme_open_file_handles,
        provider_mode,
        object_backend,
        scratch_prefix: (object_backend == RangeServingObjectBackend::Gcs).then(|| {
            format!(
                "scratch/provider-bound-range/cache-economics/{}/{}/{}/{}",
                workload.id,
                economics_mode.id(),
                seed,
                Uuid::new_v4()
            )
        }),
        warmup_reads,
        measured_reads,
        economics: Some(ProviderCacheEconomicsConfig {
            distribution,
            zipf_theta_milli,
            hotset_fraction_ppm,
            hot_read_fraction_ppm,
            hotset_shift_every,
            view_reopen_every,
            provider_get_cost_nano_usd,
            mode: economics_mode,
        }),
        seed,
    };
    let mut receipts = Vec::with_capacity(seeds.len());
    for seed in seeds {
        match run_range_serving_curve_child(&executable, &config_for_seed(*seed), timeout_millis) {
            Ok(receipt) => receipts.push(receipt),
            Err(error) => {
                return provider_bound_discard(
                    workload,
                    backend,
                    "bounded-nvme",
                    format!("provider cache economics child failed: {error}"),
                )
            }
        }
    }
    let replay = match run_range_serving_curve_child(
        &executable,
        &config_for_seed(seeds[0]),
        timeout_millis,
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            return provider_bound_discard(
                workload,
                backend,
                "bounded-nvme",
                format!("provider cache economics replay failed: {error}"),
            )
        }
    };
    let economics_receipts = receipts
        .iter()
        .filter_map(|receipt| receipt.economics.as_ref())
        .collect::<Vec<_>>();
    let dimensions_exact = economics_receipts.len() == seeds.len()
        && receipts.iter().all(|receipt| {
            receipt.base_key_count == base_key_count
                && receipt.value_bytes == value_bytes
                && receipt.point_samples == measured_reads
                && receipt.cache_mode == RangeServingCacheMode::SharedRamNvme.id()
                && receipt.provider_mode == provider_mode.id()
                && receipt.object_backend == object_backend.id()
        })
        && economics_receipts.iter().all(|receipt| {
            receipt.distribution == distribution.id()
                && receipt.warmup_reads == warmup_reads
                && receipt.measured_reads == measured_reads
                && receipt.cache_capacity_bytes == cache_capacity_u64
        });
    let reads_exact = economics_receipts.iter().all(|receipt| {
        receipt.logical_reads == u64::try_from(measured_reads).unwrap_or(u64::MAX)
            && receipt.cache_hits.saturating_add(receipt.cache_misses) == receipt.logical_reads
            && receipt.oracle_checks == receipt.logical_reads
            && receipt.oracle_exact
            && receipt.oracle_sha256.len() == 64
            && receipt.oracle_sha256 == receipt.observed_sha256
            && (receipt.cache_hit_ratio + receipt.cache_miss_ratio - 1.0).abs() <= 1.0e-12
    });
    let provider_identity_exact = receipts.iter().all(|receipt| {
        receipt.provider_closure_sha256.len() == 64
            && receipt.provider_get_requests > 0
            && receipt.provider_get_requests == receipt.provider_revision_checks
            && receipt.provider_refused_requests == 0
            && receipt.unversioned_fallbacks == 0
    });
    let cache_bound_held = economics_receipts.iter().all(|receipt| {
        receipt.cache_bound_enabled
            && receipt.cache_bound_held
            && receipt.settled_cache_bytes <= receipt.cache_capacity_bytes
    });
    let replay_economics = replay.economics.as_ref();
    let trace_replay_exact = economics_receipts.first().is_some_and(|first| {
        replay_economics.is_some_and(|replayed| {
            first.trace_sha256.len() == 64
                && first.reuse_distance_sha256.len() == 64
                && first.trace_sha256 == replayed.trace_sha256
                && first.reuse_distance_sha256 == replayed.reuse_distance_sha256
                && first.reuse_distance_p50 == replayed.reuse_distance_p50
                && first.reuse_distance_p95 == replayed.reuse_distance_p95
                && first.reuse_distance_p99 == replayed.reuse_distance_p99
        })
    });
    let expected_reopens = if view_reopen_every == 0 {
        0
    } else {
        u64::try_from(measured_reads.saturating_sub(1) / view_reopen_every).unwrap_or(u64::MAX)
    };
    let view_reopens_exact = economics_receipts
        .iter()
        .all(|receipt| receipt.view_reopens == expected_reopens);
    let receipts_complete = economics_receipts.iter().all(|receipt| {
        receipt.all_point_p50_seconds > 0.0
            && receipt.all_point_p99_seconds > 0.0
            && receipt.reuse_distance_samples > 0
            && receipt.open_provider_get_requests > 0
            && receipt.open_provider_read_bytes > 0
            && (receipt.cache_misses == 0
                || (receipt.point_provider_get_requests > 0
                    && receipt.point_provider_read_bytes > 0
                    && receipt.provider_miss_p99_seconds > 0.0))
            && (receipt.cache_hits == 0 || receipt.cache_hit_p99_seconds > 0.0)
            && receipt
                .estimated_request_cost_per_million_reads_usd
                .is_finite()
    });
    let safety_bounds_held = receipts
        .iter()
        .all(|receipt| receipt.safety_bounds_held && receipt.peak_rss_bytes > 0);
    let scratch_cleanup_complete = receipts.iter().all(|receipt| {
        receipt.scratch_cleanup_complete
            && (object_backend == RangeServingObjectBackend::Local
                || receipt.scratch_objects_deleted > 0)
    });
    let checks = [
        dimensions_exact,
        reads_exact,
        provider_identity_exact,
        cache_bound_held,
        trace_replay_exact,
        view_reopens_exact,
        receipts_complete,
        safety_bounds_held,
        scratch_cleanup_complete,
    ];
    let anomalies =
        u64::try_from(checks.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let executable_sha256 = match file_sha256(&executable) {
        Ok(digest) => digest,
        Err(error) => return provider_bound_discard(workload, backend, "bounded-nvme", error),
    };
    let artifact_path = provider_cache_economics_artifact_path(run_id, candidate_commit, workload);
    let artifact = ProviderCacheEconomicsArtifact {
        contract_version: 1,
        executable_sha256: &executable_sha256,
        workload: &workload.id,
        distribution: distribution.id(),
        cache_capacity_bytes: cache_capacity_u64,
        provider_mode: provider_mode.id(),
        economics_mode: economics_mode.id(),
        receipts: &receipts,
        semantic_replay_receipt: &replay,
    };
    if let Err(error) = write_json_artifact(&artifact_path, &artifact, "provider cache economics") {
        return provider_bound_discard(workload, backend, "bounded-nvme", error);
    }
    if negative_control != "none" && negative_control != "correct" {
        let control_detected = anomalies > 0;
        let mut measurements = vec![Measurement {
            metric: "correctness.anomalies",
            value: 1.0,
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("oracle", "provider-cache-economics-v1"),
                (
                    "anomaly.class",
                    if control_detected {
                        "control-detected"
                    } else {
                        "control-escaped"
                    },
                ),
            ]),
        }];
        let capacity = cache_capacity_u64.to_string();
        for receipt in &economics_receipts {
            measurements.push(Measurement {
                metric: "provider_bound.cache_miss_ratio",
                value: receipt.cache_miss_ratio,
                attributes: attributes(&[
                    ("lane", workload.lane.as_str()),
                    ("workload", workload.id.as_str()),
                    ("backend", backend),
                    ("trace.distribution", receipt.distribution.as_str()),
                    ("cache.capacity", capacity.as_str()),
                    ("result", "discard"),
                ]),
            });
        }
        return WorkloadExecution {
            error: Some(if control_detected {
                format!("provider cache economics control detected: {negative_control}")
            } else {
                format!("unsafe provider cache economics control escaped: {negative_control}")
            }),
            measurements,
            hard_gates: vec![HardGateResult {
                id: "provider_bound.control_detected".to_owned(),
                status: if control_detected {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(if control_detected {
                    format!("unsafe subject failed {anomalies} clean-run gates")
                } else {
                    "unsafe subject satisfied every clean-run gate".to_owned()
                }),
            }],
            budget_units: bounded_usize(measured_reads.saturating_mul(seeds.len())),
            artifact_refs: vec![artifact_path.display().to_string()],
            secondary_metrics: BTreeMap::new(),
        };
    }

    let passed = anomalies == 0;
    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(anomalies),
        attributes: attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("oracle", "provider-cache-economics-v1"),
            ("anomaly.class", if passed { "none" } else { "contract" }),
        ]),
    }];
    for (receipt, economics) in receipts.iter().zip(&economics_receipts) {
        measurements.extend(provider_cache_economics_measurements(
            workload,
            backend,
            receipt,
            economics,
            price_snapshot,
            f64::from(u32::try_from(provider_get_cost_nano_usd).unwrap_or(u32::MAX))
                / 1_000_000_000.0,
        ));
    }
    let gate = |id: &str, value: bool| HardGateResult {
        id: id.to_owned(),
        status: gate_status(value),
        detail: None,
    };
    WorkloadExecution {
        error: (!passed).then(|| {
            format!(
                "provider cache economics discarded: anomalies={anomalies}, dimensions={dimensions_exact}, reads={reads_exact}, identity={provider_identity_exact}, bound={cache_bound_held}, replay={trace_replay_exact}, reopens={view_reopens_exact}, receipts={receipts_complete}, safety={safety_bounds_held}, cleanup={scratch_cleanup_complete}"
            )
        }),
        measurements,
        hard_gates: vec![
            gate("provider_bound.minimum_seeds", seeds.len() >= 5),
            gate("provider_bound.dimensions_exact", dimensions_exact),
            gate("provider_bound.reads_exact", reads_exact),
            gate("provider_bound.identity_exact", provider_identity_exact),
            gate("provider_bound.cache_bound_held", cache_bound_held),
            gate("provider_bound.trace_replay_exact", trace_replay_exact),
            gate("provider_bound.view_reopens_exact", view_reopens_exact),
            gate("provider_bound.receipts_complete", receipts_complete),
            gate("provider_bound.safety_bounds_held", safety_bounds_held),
            gate(
                "provider_bound.scratch_cleanup_complete",
                scratch_cleanup_complete,
            ),
        ],
        budget_units: bounded_usize(measured_reads.saturating_mul(seeds.len())),
        artifact_refs: vec![artifact_path.display().to_string()],
        secondary_metrics: BTreeMap::from([
            (
                "provider_bound.cache_capacity_bytes".to_owned(),
                bounded_count(cache_capacity_u64),
            ),
            (
                "provider_bound.hit_p99_seconds".to_owned(),
                economics_receipts
                    .iter()
                    .map(|receipt| receipt.cache_hit_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "provider_bound.miss_p99_seconds".to_owned(),
                economics_receipts
                    .iter()
                    .map(|receipt| receipt.provider_miss_p99_seconds)
                    .fold(0.0_f64, f64::max),
            ),
            (
                "provider_bound.cost_per_million_reads_usd".to_owned(),
                economics_receipts
                    .iter()
                    .map(|receipt| receipt.estimated_request_cost_per_million_reads_usd)
                    .fold(0.0_f64, f64::max),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn provider_cache_economics_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    receipt: &RangeServingCurveReceipt,
    economics: &okv_object::ProviderCacheEconomicsReceipt,
    price_snapshot: &str,
    get_request_cost_usd: f64,
) -> Vec<Measurement> {
    let capacity = economics.cache_capacity_bytes.to_string();
    let common = |result: &str| {
        attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("trace.distribution", economics.distribution.as_str()),
            ("cache.capacity", capacity.as_str()),
            ("result", result),
        ])
    };
    let tier = |cache_result: &str| {
        attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("trace.distribution", economics.distribution.as_str()),
            ("cache.capacity", capacity.as_str()),
            ("cache.result", cache_result),
            ("result", "pass"),
        ])
    };
    let mut measurements = vec![
        Measurement {
            metric: "provider_bound.cache_miss_ratio",
            value: economics.cache_miss_ratio,
            attributes: common("pass"),
        },
        Measurement {
            metric: "provider_bound.cache_hit_ratio",
            value: economics.cache_hit_ratio,
            attributes: common("pass"),
        },
        Measurement {
            metric: "provider_bound.logical_reads",
            value: bounded_count(economics.cache_hits),
            attributes: tier("hit"),
        },
        Measurement {
            metric: "provider_bound.logical_reads",
            value: bounded_count(economics.cache_misses),
            attributes: tier("miss"),
        },
        Measurement {
            metric: "provider_bound.cache_resident_bytes",
            value: bounded_count(economics.settled_cache_bytes),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.capacity", capacity.as_str()),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.view_reopens",
            value: bounded_count(economics.view_reopens),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("trace.distribution", economics.distribution.as_str()),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.reuse_distance",
            value: bounded_count(economics.reuse_distance_p50),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("trace.distribution", economics.distribution.as_str()),
            ]),
        },
        Measurement {
            metric: "provider_bound.object_requests",
            value: bounded_count(economics.point_provider_get_requests),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", "bounded-nvme"),
                ("api", "get"),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.object_bytes",
            value: bounded_count(economics.point_provider_read_bytes),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", "bounded-nvme"),
                ("source", "provider"),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.revision_checks",
            value: bounded_count(receipt.provider_revision_checks),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", "bounded-nvme"),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.estimated_cost",
            value: f64::from(
                u32::try_from(economics.point_provider_get_requests).unwrap_or(u32::MAX),
            ) * get_request_cost_usd,
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", "bounded-nvme"),
                ("price.snapshot", price_snapshot),
                ("cost.class", "request"),
            ]),
        },
    ];
    if economics.cache_hits > 0 {
        measurements.push(Measurement {
            metric: "provider_bound.point_duration",
            value: economics.cache_hit_p99_seconds,
            attributes: tier("hit"),
        });
    }
    if economics.cache_misses > 0 {
        measurements.push(Measurement {
            metric: "provider_bound.point_duration",
            value: economics.provider_miss_p99_seconds,
            attributes: tier("miss"),
        });
    }
    measurements
}

fn provider_bound_measurements(
    workload: &WorkloadConfig,
    backend: &str,
    cache_state: &str,
    receipt: &RangeServingCurveReceipt,
    price_snapshot: &str,
    get_request_cost_usd: f64,
) -> Vec<Measurement> {
    let phase_attrs = |phase: &str| {
        attributes(&[
            ("lane", workload.lane.as_str()),
            ("workload", workload.id.as_str()),
            ("backend", backend),
            ("cache.state", cache_state),
            ("result", "pass"),
            ("phase", phase),
        ])
    };
    vec![
        Measurement {
            metric: "provider_bound.view_ready_duration",
            value: receipt.view_open_seconds,
            attributes: phase_attrs("view-ready"),
        },
        Measurement {
            metric: "provider_bound.first_point_duration",
            value: receipt.first_point_seconds,
            attributes: phase_attrs("first-point"),
        },
        Measurement {
            metric: "provider_bound.first_range_duration",
            value: receipt.scan_seconds,
            attributes: phase_attrs("first-range"),
        },
        Measurement {
            metric: "provider_bound.object_requests",
            value: bounded_count(receipt.provider_get_requests),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", cache_state),
                ("api", "get"),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.object_bytes",
            value: bounded_count(receipt.provider_read_bytes),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", cache_state),
                ("source", "provider"),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.revision_checks",
            value: bounded_count(receipt.provider_revision_checks),
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", cache_state),
                ("result", "pass"),
            ]),
        },
        Measurement {
            metric: "provider_bound.estimated_cost",
            value: f64::from(u32::try_from(receipt.provider_get_requests).unwrap_or(u32::MAX))
                * get_request_cost_usd,
            attributes: attributes(&[
                ("lane", workload.lane.as_str()),
                ("workload", workload.id.as_str()),
                ("backend", backend),
                ("cache.state", cache_state),
                ("price.snapshot", price_snapshot),
                ("cost.class", "request"),
            ]),
        },
    ]
}

fn provider_bound_discard(
    workload: &WorkloadConfig,
    backend: &str,
    cache_state: &str,
    error: String,
) -> WorkloadExecution {
    WorkloadExecution {
        error: Some(error),
        measurements: vec![
            Measurement {
                metric: "correctness.anomalies",
                value: 1.0,
                attributes: attributes(&[
                    ("lane", workload.lane.as_str()),
                    ("workload", workload.id.as_str()),
                    ("oracle", "provider-bound-range-v2"),
                    ("anomaly.class", "provider-identity"),
                ]),
            },
            Measurement {
                metric: "provider_bound.first_point_duration",
                value: 0.0,
                attributes: attributes(&[
                    ("lane", workload.lane.as_str()),
                    ("workload", workload.id.as_str()),
                    ("backend", backend),
                    ("cache.state", cache_state),
                    ("result", "discard"),
                    ("phase", "control"),
                ]),
            },
            Measurement {
                metric: "provider_bound.object_requests",
                value: 0.0,
                attributes: attributes(&[
                    ("lane", workload.lane.as_str()),
                    ("workload", workload.id.as_str()),
                    ("backend", backend),
                    ("cache.state", cache_state),
                    ("api", "get"),
                    ("result", "discard"),
                ]),
            },
        ],
        hard_gates: vec![HardGateResult {
            id: "provider_bound.control_detected".to_owned(),
            status: GateStatus::Pass,
            detail: None,
        }],
        budget_units: 1.0,
        artifact_refs: Vec::new(),
        secondary_metrics: BTreeMap::new(),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_commit_visibility(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    run_cell_commit_durability_workload(workload, seeds, backend, false)
}

fn run_cell_tagged_log_certificate(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    run_cell_commit_durability_workload(workload, seeds, backend, true)
}

#[allow(clippy::too_many_lines)]
fn run_cell_commit_durability_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
    authenticated_suite: bool,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell commit visibility workload requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = if authenticated_suite {
        "transaction-openraft+signed-tagged-tlog-certificates+process-proxies"
    } else {
        "transaction-openraft+two-tagged-tlog-sets+process-proxies"
    };
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell commit visibility requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let control = if authenticated_suite && matches!(control, "correct" | "none") {
        "authenticated_correct"
    } else {
        control
    };
    let mode = match parse_cell_commit_visibility_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut baseline_frontier = 0_u64;
    let mut target_version = 0_u64;
    let mut observed_frontier = 0_u64;
    let mut authority_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut proxy_starts = 0_u64;
    let mut proxy_kills = 0_u64;
    let mut worker_starts = 0_u64;
    let mut tlog_appends = 0_u64;
    let mut log_set_policies = 0_u64;
    let mut tlog_attestations = 0_u64;
    let mut certificate_rejections = 0_u64;
    let mut every_log_set_durable = true;
    let mut client_acknowledged = true;
    let mut authority_visible = true;
    let mut retry_retained = true;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_cell_commit_visibility_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_cell_commit_visibility_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        baseline_frontier = baseline_frontier.saturating_add(first.baseline_frontier);
        target_version = target_version.saturating_add(first.target_version);
        observed_frontier = observed_frontier.saturating_add(first.observed_frontier);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        proxy_starts = proxy_starts.saturating_add(first.proxy_process_starts);
        proxy_kills = proxy_kills.saturating_add(first.proxy_process_kills);
        worker_starts = worker_starts.saturating_add(first.worker_process_starts);
        tlog_appends = tlog_appends.saturating_add(first.tagged_log_appends);
        log_set_policies = log_set_policies.saturating_add(first.log_set_policy_count);
        tlog_attestations = tlog_attestations.saturating_add(first.tagged_log_attestations);
        certificate_rejections =
            certificate_rejections.saturating_add(first.certificate_rejections);
        every_log_set_durable &= first.durable_log_sets == first.required_log_sets;
        client_acknowledged &= first.client_acknowledged;
        authority_visible &= first.authority_visible;
        retry_retained &= first.retry_status
            == Some(okv_consensus::CellStagedTransactionStatus::AlreadyCommitted);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    (
                        "oracle",
                        if authenticated_suite {
                            "cell-tagged-log-certificate-v0"
                        } else {
                            "cell-commit-visibility-v0"
                        },
                    ),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "premature_visibility"
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
                    (
                        "operation",
                        if authenticated_suite {
                            "staged-commit-signed-log-certificates"
                        } else {
                            "staged-commit-tagged-log-visibility"
                        },
                    ),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "frontier.commit_version",
                value: bounded_count(first.target_version),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("range", "all"),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-commit-{}://{}/{seed}/{}",
            if authenticated_suite {
                "certificate"
            } else {
                "visibility"
            },
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_checks = if authenticated_suite { 32 } else { 28 };
    let process_boundaries_exercised = checks == seed_count.saturating_mul(expected_checks)
        && authority_starts == seed_count.saturating_mul(7)
        && tlog_starts == seed_count.saturating_mul(6)
        && worker_starts == seed_count
        && baseline_frontier == seed_count.saturating_mul(10)
        && target_version == seed_count.saturating_mul(11)
        && match mode {
            CellCommitVisibilityMode::Correct => {
                proxy_starts == seed_count.saturating_mul(3)
                    && proxy_kills == seed_count.saturating_mul(2)
                    && tlog_appends == seed_count.saturating_mul(6)
                    && observed_frontier == seed_count.saturating_mul(11)
                    && every_log_set_durable
                    && client_acknowledged
                    && authority_visible
                    && retry_retained
            }
            CellCommitVisibilityMode::AcknowledgeAfterOneLogSet => {
                proxy_starts == seed_count
                    && proxy_kills == 0
                    && tlog_appends == seed_count.saturating_mul(3)
                    && observed_frontier == seed_count.saturating_mul(10)
                    && !every_log_set_durable
                    && client_acknowledged
                    && !authority_visible
                    && !retry_retained
            }
            CellCommitVisibilityMode::AuthenticatedCorrect => {
                proxy_starts == seed_count.saturating_mul(3)
                    && proxy_kills == seed_count.saturating_mul(2)
                    && tlog_appends == seed_count.saturating_mul(6)
                    && observed_frontier == seed_count.saturating_mul(11)
                    && every_log_set_durable
                    && client_acknowledged
                    && authority_visible
                    && retry_retained
                    && log_set_policies == seed_count.saturating_mul(2)
                    && tlog_attestations >= seed_count.saturating_mul(6)
                    && certificate_rejections == 0
            }
            CellCommitVisibilityMode::UnsignedNodeList
            | CellCommitVisibilityMode::DuplicateAttestation
            | CellCommitVisibilityMode::WrongLogSetAttestation
            | CellCommitVisibilityMode::TamperedStatement
            | CellCommitVisibilityMode::ObsoletePolicyEpoch => {
                proxy_starts == seed_count
                    && proxy_kills == 0
                    && tlog_appends == seed_count.saturating_mul(3)
                    && observed_frontier == seed_count.saturating_mul(10)
                    && !every_log_set_durable
                    && !client_acknowledged
                    && !authority_visible
                    && !retry_retained
                    && log_set_policies == seed_count.saturating_mul(2)
                    && tlog_attestations >= seed_count.saturating_mul(3)
                    && certificate_rejections == seed_count
            }
        };
    let passed = anomalies == 0 && exact_replay && process_boundaries_exercised;
    let error = (!passed).then(|| {
        format!(
            "cell commit visibility gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries_exercised={process_boundaries_exercised}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_commit_visibility.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_commit_visibility.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, authority_starts={authority_starts}, tlog_starts={tlog_starts}, proxy_starts={proxy_starts}, proxy_kills={proxy_kills}, worker_starts={worker_starts}, appends={tlog_appends}, policies={log_set_policies}, attestations={tlog_attestations}, certificate_rejections={certificate_rejections}, baseline={baseline_frontier}, target={target_version}, observed={observed_frontier}, all_logs={every_log_set_durable}, ack={client_acknowledged}, visible={authority_visible}, retry={retry_retained}"
                )),
            },
            HardGateResult {
                id: "cell_commit_visibility.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_commit_visibility.checks".to_owned(),
                bounded_count(checks),
            ),
            (
                "cell_commit_visibility.baseline_frontier".to_owned(),
                bounded_count(baseline_frontier),
            ),
            (
                "cell_commit_visibility.target_version".to_owned(),
                bounded_count(target_version),
            ),
            (
                "cell_commit_visibility.observed_frontier".to_owned(),
                bounded_count(observed_frontier),
            ),
            (
                "cell_commit_visibility.authority_process_starts".to_owned(),
                bounded_count(authority_starts),
            ),
            (
                "cell_commit_visibility.tagged_log_process_starts".to_owned(),
                bounded_count(tlog_starts),
            ),
            (
                "cell_commit_visibility.proxy_process_starts".to_owned(),
                bounded_count(proxy_starts),
            ),
            (
                "cell_commit_visibility.proxy_process_kills".to_owned(),
                bounded_count(proxy_kills),
            ),
            (
                "cell_commit_visibility.worker_process_starts".to_owned(),
                bounded_count(worker_starts),
            ),
            (
                "cell_commit_visibility.tagged_log_appends".to_owned(),
                bounded_count(tlog_appends),
            ),
            (
                "cell_commit_visibility.log_set_policies".to_owned(),
                bounded_count(log_set_policies),
            ),
            (
                "cell_commit_visibility.tagged_log_attestations".to_owned(),
                bounded_count(tlog_attestations),
            ),
            (
                "cell_commit_visibility.certificate_rejections".to_owned(),
                bounded_count(certificate_rejections),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_tagged_log_chunked_repair(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell tagged-log chunked repair requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "transaction-openraft+signed-tagged-tlog-chunked-repair-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell tagged-log chunked repair requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_tagged_log_chunked_repair_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut authority_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut failed_tlogs = 0_u64;
    let mut learner_starts = 0_u64;
    let mut learner_restarts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut repair_attestations = 0_u64;
    let mut readiness_attestations = 0_u64;
    let mut base_chunks = 0_u64;
    let mut tail_chunks = 0_u64;
    let mut exact_retries = 0_u64;
    let mut active_appends = 0_u64;
    let mut base_payload_bytes = 0_u64;
    let mut tail_payload_bytes = 0_u64;
    let mut installed_records = 0_u64;
    let mut learner_frontier = 0_u64;
    let mut worker_frontier = 0_u64;
    let mut exact_replay = true;
    let mut correct_shape = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let trace_started = Instant::now();
        let first_result = run_cell_tagged_log_chunked_repair_contract(*seed, mode, &executable);
        let elapsed = trace_started.elapsed().as_secs_f64();
        let second_result = run_cell_tagged_log_chunked_repair_contract(*seed, mode, &executable);
        let (first, second) = match (first_result, second_result) {
            (Ok(first), Ok(second)) => (first, second),
            (Err(first_error), Err(second_error))
                if mode != CellTaggedLogChunkedRepairMode::Correct =>
            {
                exact_replay &= first_error == second_error;
                anomalies = anomalies.saturating_add(1);
                checks = checks.saturating_add(1);
                mismatch_details.push(format!("seed {seed}, bounded rejection: {first_error}"));
                measurements.extend([
                    Measurement {
                        metric: "correctness.anomalies",
                        value: 1.0,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("oracle", "cell-tagged-log-chunked-live-repair-v0"),
                            ("anomaly.class", mode.id()),
                        ]),
                    },
                    Measurement {
                        metric: "availability.success_ratio",
                        value: 0.0,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("operation", "chunked-live-repair"),
                            ("fault", mode.id()),
                            ("backend", backend),
                        ]),
                    },
                    Measurement {
                        metric: "operation.duration",
                        value: elapsed,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("operation", "chunked-live-repair"),
                            ("backend", backend),
                            ("result", "bounded-rejection"),
                        ]),
                    },
                ]);
                artifact_refs.push(format!(
                    "okv-cell-tagged-log-chunked-repair://{}/{seed}/{:x}",
                    mode.id(),
                    Sha256::digest(first_error.as_bytes())
                ));
                continue;
            }
            (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts =
            authority_starts.saturating_add(first.transaction_authority_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        failed_tlogs = failed_tlogs.saturating_add(first.failed_tagged_log_processes);
        learner_starts = learner_starts.saturating_add(first.learner_process_starts);
        learner_restarts = learner_restarts.saturating_add(first.learner_process_restarts);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        repair_attestations = repair_attestations.saturating_add(first.repair_attestations);
        readiness_attestations =
            readiness_attestations.saturating_add(first.readiness_attestations);
        base_chunks = base_chunks.saturating_add(first.durable_base_chunks);
        tail_chunks = tail_chunks.saturating_add(first.durable_tail_chunks);
        exact_retries = exact_retries.saturating_add(first.exact_chunk_retries);
        active_appends = active_appends.saturating_add(first.active_tail_appends);
        base_payload_bytes = base_payload_bytes.saturating_add(first.base_payload_bytes);
        tail_payload_bytes = tail_payload_bytes.saturating_add(first.tail_payload_bytes);
        installed_records = installed_records.saturating_add(first.installed_records);
        learner_frontier = learner_frontier.saturating_add(first.learner_frontier);
        worker_frontier = worker_frontier.saturating_add(first.worker_frontier);
        correct_shape &= first.object_frontier == 10
            && first.base_frontier == 14
            && first.target_frontier == 16
            && first.active_policy_members_counted == vec![2, 3];
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-tagged-log-chunked-live-repair-v0"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "chunked-live-repair"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(
                    first
                        .base_payload_bytes
                        .saturating_add(first.tail_payload_bytes),
                ),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "chunked-base-plus-live-tail"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: elapsed,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "chunked-live-repair"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "unsafe" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-tagged-log-chunked-repair://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries = checks <= seed_count.saturating_mul(40)
        && authority_starts == seed_count.saturating_mul(3)
        && tlog_starts == seed_count.saturating_mul(6)
        && failed_tlogs == seed_count
        && learner_starts == seed_count
        && learner_restarts == seed_count.saturating_mul(2)
        && worker_starts == seed_count;
    let correct_contract = mode != CellTaggedLogChunkedRepairMode::Correct
        || (anomalies == 0
            && correct_shape
            && repair_attestations == seed_count.saturating_mul(2)
            && readiness_attestations == seed_count.saturating_mul(2)
            && base_chunks == seed_count.saturating_mul(3)
            && tail_chunks == seed_count.saturating_mul(2)
            && exact_retries == seed_count.saturating_mul(2)
            && active_appends == seed_count.saturating_mul(2)
            && installed_records == seed_count.saturating_mul(6)
            && learner_frontier == seed_count.saturating_mul(16)
            && worker_frontier == seed_count.saturating_mul(16)
            && tail_payload_bytes > 0
            && tail_payload_bytes < base_payload_bytes);
    let negative_detected =
        mode == CellTaggedLogChunkedRepairMode::Correct || anomalies >= seed_count;
    let passed = anomalies == 0
        && exact_replay
        && process_boundaries
        && correct_contract
        && negative_detected;
    let error = (!passed).then(|| {
        format!(
            "cell tagged-log chunked repair gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_tagged_log_chunked_repair.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_tagged_log_chunked_repair.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "checks={checks}, authority={authority_starts}, tlogs={tlog_starts}, failed={failed_tlogs}, learners={learner_starts}, learner_restarts={learner_restarts}, workers={worker_starts}, repair={repair_attestations}, readiness={readiness_attestations}, base_chunks={base_chunks}, tail_chunks={tail_chunks}, retries={exact_retries}, appends={active_appends}, base_bytes={base_payload_bytes}, tail_bytes={tail_payload_bytes}, installed={installed_records}, learner_frontier={learner_frontier}, worker_frontier={worker_frontier}"
                )),
            },
            HardGateResult {
                id: "cell_tagged_log_chunked_repair.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0 && correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "cell_tagged_log_chunked_repair.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_tagged_log_chunked_repair.checks".to_owned(),
                bounded_count(checks),
            ),
            (
                "cell_tagged_log_chunked_repair.base_chunks".to_owned(),
                bounded_count(base_chunks),
            ),
            (
                "cell_tagged_log_chunked_repair.tail_chunks".to_owned(),
                bounded_count(tail_chunks),
            ),
            (
                "cell_tagged_log_chunked_repair.base_bytes".to_owned(),
                bounded_count(base_payload_bytes),
            ),
            (
                "cell_tagged_log_chunked_repair.tail_bytes".to_owned(),
                bounded_count(tail_payload_bytes),
            ),
            (
                "cell_tagged_log_chunked_repair.worker_frontier".to_owned(),
                bounded_count(worker_frontier),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_tagged_log_policy_transition(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell tagged-log policy transition requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "transaction-openraft+signed-tagged-tlog-policy-transition-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell tagged-log policy transition requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_tagged_log_policy_transition_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut authority_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut tlog_restarts = 0_u64;
    let mut failed_tlogs = 0_u64;
    let mut learner_starts = 0_u64;
    let mut learner_restarts = 0_u64;
    let mut repair_attestations = 0_u64;
    let mut readiness_attestations = 0_u64;
    let mut stage_attestations = 0_u64;
    let mut activation_attestations = 0_u64;
    let mut policy_prepares = 0_u64;
    let mut policy_commits = 0_u64;
    let mut idempotent_retries = 0_u64;
    let mut old_epoch_rejections = 0_u64;
    let mut final_frontier = 0_u64;
    let mut worker_frontier = 0_u64;
    let mut exact_replay = true;
    let mut correct_shape = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let trace_started = Instant::now();
        let first_result = run_cell_tagged_log_policy_transition_contract(*seed, mode, &executable);
        let elapsed = trace_started.elapsed().as_secs_f64();
        let second_result =
            run_cell_tagged_log_policy_transition_contract(*seed, mode, &executable);
        let (first, second) = match (first_result, second_result) {
            (Ok(first), Ok(second)) => (first, second),
            (Err(first_error), Err(second_error))
                if mode != CellTaggedLogPolicyTransitionMode::Correct =>
            {
                let replay_matches = first_error == second_error;
                exact_replay &= replay_matches;
                anomalies = anomalies.saturating_add(1);
                checks = checks.saturating_add(1);
                mismatch_details.push(format!("seed {seed}, bounded rejection: {first_error}"));
                measurements.extend([
                    Measurement {
                        metric: "correctness.anomalies",
                        value: 1.0,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("oracle", "cell-tagged-log-policy-transition-v0"),
                            ("anomaly.class", mode.id()),
                        ]),
                    },
                    Measurement {
                        metric: "availability.success_ratio",
                        value: 0.0,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("operation", "moving-tagged-log-policy"),
                            ("fault", mode.id()),
                            ("backend", backend),
                        ]),
                    },
                    Measurement {
                        metric: "operation.duration",
                        value: elapsed,
                        attributes: attributes(&[
                            ("lane", &workload.lane),
                            ("workload", &workload.id),
                            ("operation", "policy-transition"),
                            ("backend", backend),
                            ("result", "bounded-rejection"),
                        ]),
                    },
                ]);
                artifact_refs.push(format!(
                    "okv-cell-tagged-log-policy-transition://{}/{seed}/{}",
                    mode.id(),
                    format_args!("{:x}", Sha256::digest(first_error.as_bytes()))
                ));
                continue;
            }
            (Err(error), _) | (_, Err(error)) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts =
            authority_starts.saturating_add(first.transaction_authority_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        tlog_restarts = tlog_restarts.saturating_add(first.tagged_log_process_restarts);
        failed_tlogs = failed_tlogs.saturating_add(first.failed_tagged_log_processes);
        learner_starts = learner_starts.saturating_add(first.learner_process_starts);
        learner_restarts = learner_restarts.saturating_add(first.learner_process_restarts);
        repair_attestations = repair_attestations.saturating_add(first.repair_attestations);
        readiness_attestations =
            readiness_attestations.saturating_add(first.readiness_attestations);
        stage_attestations = stage_attestations.saturating_add(first.successor_stage_attestations);
        activation_attestations =
            activation_attestations.saturating_add(first.authority_activation_attestations);
        policy_prepares = policy_prepares.saturating_add(first.policy_prepares);
        policy_commits = policy_commits.saturating_add(first.policy_commits);
        idempotent_retries = idempotent_retries.saturating_add(first.idempotent_retries);
        old_epoch_rejections = old_epoch_rejections.saturating_add(first.old_epoch_rejections);
        final_frontier = final_frontier.saturating_add(first.final_frontier);
        worker_frontier = worker_frontier.saturating_add(first.worker_frontier);
        correct_shape &= first.generation_before == first.generation_after
            && first.object_frontier == 10
            && first.pre_transition_frontier == 14
            && first.capacity_members_counted == vec![3, 4]
            && first.serving_members_counted == vec![3, 4];
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-tagged-log-policy-transition-v0"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "moving-tagged-log-policy"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "recovery.membership_epoch",
                value: bounded_count(first.next_policy_epoch),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "bounded-cell-v0"),
                    ("transaction_system", "replicated-policy-authority"),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: elapsed,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "policy-transition"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "unsafe" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-tagged-log-policy-transition://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries = checks <= seed_count.saturating_mul(50)
        && authority_starts == seed_count.saturating_mul(3)
        && tlog_starts == seed_count.saturating_mul(6)
        && tlog_restarts == seed_count.saturating_mul(4)
        && failed_tlogs == seed_count.saturating_mul(2)
        && learner_starts == seed_count
        && learner_restarts == seed_count.saturating_mul(3);
    let correct_contract = mode != CellTaggedLogPolicyTransitionMode::Correct
        || (anomalies == 0
            && correct_shape
            && repair_attestations == seed_count.saturating_mul(2)
            && readiness_attestations == seed_count.saturating_mul(2)
            && stage_attestations >= seed_count.saturating_mul(2)
            && stage_attestations <= seed_count.saturating_mul(3)
            && activation_attestations == seed_count.saturating_mul(2)
            && policy_prepares == seed_count
            && policy_commits == seed_count
            && idempotent_retries == seed_count
            && old_epoch_rejections == seed_count
            && final_frontier == seed_count.saturating_mul(17)
            && worker_frontier == seed_count.saturating_mul(17));
    let negative_detected =
        mode == CellTaggedLogPolicyTransitionMode::Correct || anomalies >= seed_count;
    let passed = anomalies == 0
        && exact_replay
        && process_boundaries
        && correct_contract
        && negative_detected;
    let error = (!passed).then(|| {
        format!(
            "cell tagged-log policy transition gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_tagged_log_policy_transition.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_tagged_log_policy_transition.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "checks={checks}, authority={authority_starts}, tlogs={tlog_starts}, tlog_restarts={tlog_restarts}, failed={failed_tlogs}, learners={learner_starts}, learner_restarts={learner_restarts}, repair={repair_attestations}, readiness={readiness_attestations}, stage={stage_attestations}, activation={activation_attestations}, prepares={policy_prepares}, commits={policy_commits}, retries={idempotent_retries}, old_epoch_rejections={old_epoch_rejections}, final_frontier={final_frontier}, worker_frontier={worker_frontier}"
                )),
            },
            HardGateResult {
                id: "cell_tagged_log_policy_transition.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0 && correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "cell_tagged_log_policy_transition.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_tagged_log_policy_transition.checks".to_owned(),
                bounded_count(checks),
            ),
            (
                "cell_tagged_log_policy_transition.stage_attestations".to_owned(),
                bounded_count(stage_attestations),
            ),
            (
                "cell_tagged_log_policy_transition.activation_attestations".to_owned(),
                bounded_count(activation_attestations),
            ),
            (
                "cell_tagged_log_policy_transition.policy_commits".to_owned(),
                bounded_count(policy_commits),
            ),
            (
                "cell_tagged_log_policy_transition.worker_frontier".to_owned(),
                bounded_count(worker_frontier),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_tagged_log_learner_repair(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell tagged-log learner repair requires at least one seed".to_owned(),
        ));
    }
    let expected_backend = "transaction-openraft+signed-tagged-tlog-repair-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell tagged-log learner repair requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_tagged_log_learner_repair_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut authority_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut failed_tlogs = 0_u64;
    let mut learner_starts = 0_u64;
    let mut learner_restarts = 0_u64;
    let mut repair_attestations = 0_u64;
    let mut readiness_attestations = 0_u64;
    let mut installed_records = 0_u64;
    let mut worker_starts = 0_u64;
    let mut worker_frontier = 0_u64;
    let mut snapshot_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let trace_started = Instant::now();
        let first = match run_cell_tagged_log_learner_repair_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let elapsed = trace_started.elapsed().as_secs_f64();
        let second = match run_cell_tagged_log_learner_repair_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts =
            authority_starts.saturating_add(first.transaction_authority_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        failed_tlogs = failed_tlogs.saturating_add(first.failed_tagged_log_processes);
        learner_starts = learner_starts.saturating_add(first.learner_process_starts);
        learner_restarts = learner_restarts.saturating_add(first.learner_process_restarts);
        repair_attestations = repair_attestations.saturating_add(first.repair_attestations);
        readiness_attestations =
            readiness_attestations.saturating_add(first.readiness_attestations);
        installed_records = installed_records.saturating_add(first.installed_records);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        worker_frontier = worker_frontier.saturating_add(first.worker_frontier);
        snapshot_bytes = snapshot_bytes.max(first.repair_snapshot_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-tagged-log-learner-repair-v0"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "failed-tlog-learner-repair"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.repair_snapshot_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "one-failed-tlog-one-nonvoting-learner"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "operation.duration",
                value: elapsed,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "learner-repair"),
                    ("backend", backend),
                    ("result", if exact { "exact" } else { "unsafe" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-tagged-log-learner-repair://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries = checks <= seed_count.saturating_mul(70)
        && authority_starts == seed_count.saturating_mul(3)
        && tlog_starts == seed_count.saturating_mul(6)
        && failed_tlogs == seed_count
        && learner_starts >= seed_count
        && learner_restarts == seed_count
        && worker_starts == seed_count
        && worker_frontier == seed_count.saturating_mul(14);
    let correct_contract = mode != CellTaggedLogLearnerRepairMode::Correct
        || (anomalies == 0
            && learner_starts == seed_count
            && repair_attestations == seed_count.saturating_mul(2)
            && readiness_attestations == seed_count.saturating_mul(2)
            && installed_records == seed_count.saturating_mul(4)
            && snapshot_bytes > 0);
    let negative_detected =
        mode == CellTaggedLogLearnerRepairMode::Correct || anomalies >= seed_count;
    let passed = anomalies == 0
        && exact_replay
        && process_boundaries
        && correct_contract
        && negative_detected;
    let error = (!passed).then(|| {
        format!(
            "cell tagged-log learner repair gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries}, correct_contract={correct_contract}, negative_detected={negative_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_tagged_log_repair.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_tagged_log_repair.process_boundaries".to_owned(),
                status: gate_status(process_boundaries),
                detail: Some(format!(
                    "checks={checks}, authority={authority_starts}, tlogs={tlog_starts}, failed={failed_tlogs}, learners={learner_starts}, learner_restarts={learner_restarts}, workers={worker_starts}, repair_attestations={repair_attestations}, ready_attestations={readiness_attestations}, installed={installed_records}, snapshot_bytes={snapshot_bytes}, worker_frontier={worker_frontier}"
                )),
            },
            HardGateResult {
                id: "cell_tagged_log_repair.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0 && correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "cell_tagged_log_repair.negative_control".to_owned(),
                status: gate_status(negative_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "cell_tagged_log_repair.checks".to_owned(),
                bounded_count(checks),
            ),
            (
                "cell_tagged_log_repair.repair_attestations".to_owned(),
                bounded_count(repair_attestations),
            ),
            (
                "cell_tagged_log_repair.readiness_attestations".to_owned(),
                bounded_count(readiness_attestations),
            ),
            (
                "cell_tagged_log_repair.snapshot_bytes".to_owned(),
                bounded_count(snapshot_bytes),
            ),
            (
                "cell_tagged_log_repair.worker_frontier".to_owned(),
                bounded_count(worker_frontier),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_cell_tagged_log_lag_ratekeeper(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "cell tagged-log lag workload requires at least one seed".to_owned(),
        ));
    }
    let expected_backend =
        "transaction-openraft+publication-openraft+signed-tagged-tlog-ratekeeper-processes";
    if backend != expected_backend {
        return execution_from_result(Err(format!(
            "cell tagged-log lag requires {expected_backend}, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("mode")
        .or_else(|| workload.parameters.get("negative_control"))
        .and_then(toml::Value::as_str)
        .unwrap_or("correct");
    let mode = match parse_cell_tagged_log_lag_ratekeeping_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let mut anomalies = 0_u64;
    let mut checks = 0_u64;
    let mut authority_starts = 0_u64;
    let mut publication_starts = 0_u64;
    let mut tlog_starts = 0_u64;
    let mut worker_starts = 0_u64;
    let mut tlog_restarts = 0_u64;
    let mut admitted_commits = 0_u64;
    let mut rate_limited_attempts = 0_u64;
    let mut sequence_allocations_while_limited = 0_u64;
    let mut staged_records_while_limited = 0_u64;
    let mut tlog_appends = 0_u64;
    let mut partial_appends = 0_u64;
    let mut capacity_attestations = 0_u64;
    let mut pop_attestations = 0_u64;
    let mut hard_limit_rejections = 0_u64;
    let mut retained_high_watermark = 0_u64;
    let mut retained_after_pop = 0_u64;
    let mut object_publications = 0_u64;
    let mut object_frontier = 0_u64;
    let mut stalled_frontier = 0_u64;
    let mut final_frontier = 0_u64;
    let mut worker_frontier = 0_u64;
    let mut suffix_records = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let trace_started = Instant::now();
        let first = match run_cell_tagged_log_lag_ratekeeping_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let trace_elapsed = trace_started.elapsed().as_secs_f64();
        let second = match run_cell_tagged_log_lag_ratekeeping_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomalies = anomalies.saturating_add(first.anomaly_count);
        checks = checks.saturating_add(first.executed_checks);
        authority_starts = authority_starts.saturating_add(first.authority_process_starts);
        publication_starts = publication_starts.saturating_add(first.publication_process_starts);
        tlog_starts = tlog_starts.saturating_add(first.tagged_log_process_starts);
        worker_starts = worker_starts.saturating_add(first.serving_worker_process_starts);
        tlog_restarts = tlog_restarts.saturating_add(first.tagged_log_process_restarts);
        admitted_commits = admitted_commits.saturating_add(first.admitted_commits);
        rate_limited_attempts = rate_limited_attempts.saturating_add(first.rate_limited_attempts);
        sequence_allocations_while_limited = sequence_allocations_while_limited
            .saturating_add(first.sequence_allocations_while_limited);
        staged_records_while_limited =
            staged_records_while_limited.saturating_add(first.staged_records_while_limited);
        tlog_appends = tlog_appends.saturating_add(first.tagged_log_appends);
        partial_appends = partial_appends.saturating_add(first.partial_appends_while_limited);
        capacity_attestations = capacity_attestations.saturating_add(first.capacity_attestations);
        pop_attestations = pop_attestations.saturating_add(first.pop_attestations);
        hard_limit_rejections = hard_limit_rejections.saturating_add(first.hard_limit_rejections);
        retained_high_watermark = retained_high_watermark.max(first.retained_bytes_high_watermark);
        retained_after_pop = retained_after_pop.max(first.retained_bytes_after_pop);
        object_publications = object_publications.saturating_add(first.object_publications);
        object_frontier = object_frontier.saturating_add(first.object_frontier);
        stalled_frontier = stalled_frontier.saturating_add(first.stalled_frontier);
        final_frontier = final_frontier.saturating_add(first.final_frontier);
        worker_frontier = worker_frontier.saturating_add(first.worker_observed_frontier);
        suffix_records = suffix_records.saturating_add(first.suffix_records_recovered);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!("seed {seed}, check: {detail}"));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "cell-tagged-log-lag-ratekeeping-v0"),
                    ("anomaly.class", if exact { "none" } else { mode.id() }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "ratekeep-pop-resume"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.retained_bytes_high_watermark),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "two-three-node-tagged-log-sets"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "objectification.lag",
                value: trace_elapsed,
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend),
                    ("result", if exact { "bounded" } else { "unsafe" }),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-cell-tagged-log-lag-ratekeeping://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }
    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let process_boundaries_exercised = checks == seed_count.saturating_mul(60)
        && authority_starts == seed_count.saturating_mul(7)
        && publication_starts == seed_count.saturating_mul(3)
        && tlog_starts == seed_count.saturating_mul(6)
        && worker_starts == seed_count
        && tlog_restarts == seed_count
        && final_frontier == seed_count.saturating_mul(16)
        && object_frontier == seed_count.saturating_mul(12)
        && stalled_frontier == seed_count.saturating_mul(14);
    let correct_contract = mode != CellTaggedLogLagRatekeepingMode::Correct
        || (anomalies == 0
            && admitted_commits == seed_count.saturating_mul(6)
            && rate_limited_attempts == seed_count.saturating_mul(3)
            && sequence_allocations_while_limited == 0
            && staged_records_while_limited == 0
            && tlog_appends == seed_count.saturating_mul(36)
            && partial_appends == 0
            && pop_attestations == seed_count.saturating_mul(6)
            && hard_limit_rejections == 0
            && retained_high_watermark <= 8_192
            && retained_after_pop < retained_high_watermark
            && worker_frontier == seed_count.saturating_mul(16)
            && suffix_records == seed_count.saturating_mul(4));
    let negative_control_detected =
        mode == CellTaggedLogLagRatekeepingMode::Correct || anomalies >= seed_count;
    let passed = anomalies == 0
        && exact_replay
        && process_boundaries_exercised
        && correct_contract
        && negative_control_detected;
    let error = (!passed).then(|| {
        format!(
            "cell tagged-log lag gate failed: mode={}, anomalies={anomalies}, exact_replay={exact_replay}, process_boundaries={process_boundaries_exercised}, correct_contract={correct_contract}, negative_control_detected={negative_control_detected}; {}",
            mode.id(),
            mismatch_details.join("; ")
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "cell_tagged_log_lag.exact_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "cell_tagged_log_lag.process_boundaries_exercised".to_owned(),
                status: gate_status(process_boundaries_exercised),
                detail: Some(format!(
                    "checks={checks}, authority={authority_starts}, publication={publication_starts}, tlogs={tlog_starts}, workers={worker_starts}, restarts={tlog_restarts}, commits={admitted_commits}, limited={rate_limited_attempts}, appends={tlog_appends}, capacity_attestations={capacity_attestations}, pop_attestations={pop_attestations}, retained_high={retained_high_watermark}, retained_after_pop={retained_after_pop}, publications={object_publications}, O={object_frontier}, stalled={stalled_frontier}, final={final_frontier}, worker={worker_frontier}, suffix={suffix_records}"
                )),
            },
            HardGateResult {
                id: "cell_tagged_log_lag.contract_agreement".to_owned(),
                status: gate_status(anomalies == 0 && correct_contract),
                detail: mismatch_details.first().cloned(),
            },
            HardGateResult {
                id: "cell_tagged_log_lag.negative_control_detected".to_owned(),
                status: gate_status(negative_control_detected),
                detail: None,
            },
        ],
        budget_units: bounded_count(checks),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("cell_tagged_log_lag.checks".to_owned(), bounded_count(checks)),
            (
                "cell_tagged_log_lag.admitted_commits".to_owned(),
                bounded_count(admitted_commits),
            ),
            (
                "cell_tagged_log_lag.rate_limited_attempts".to_owned(),
                bounded_count(rate_limited_attempts),
            ),
            (
                "cell_tagged_log_lag.sequence_allocations_while_limited".to_owned(),
                bounded_count(sequence_allocations_while_limited),
            ),
            (
                "cell_tagged_log_lag.staged_records_while_limited".to_owned(),
                bounded_count(staged_records_while_limited),
            ),
            (
                "cell_tagged_log_lag.tagged_log_appends".to_owned(),
                bounded_count(tlog_appends),
            ),
            (
                "cell_tagged_log_lag.capacity_attestations".to_owned(),
                bounded_count(capacity_attestations),
            ),
            (
                "cell_tagged_log_lag.pop_attestations".to_owned(),
                bounded_count(pop_attestations),
            ),
            (
                "cell_tagged_log_lag.hard_limit_rejections".to_owned(),
                bounded_count(hard_limit_rejections),
            ),
            (
                "cell_tagged_log_lag.retained_high_watermark".to_owned(),
                bounded_count(retained_high_watermark),
            ),
            (
                "cell_tagged_log_lag.retained_after_pop".to_owned(),
                bounded_count(retained_after_pop),
            ),
            (
                "cell_tagged_log_lag.worker_frontier".to_owned(),
                bounded_count(worker_frontier),
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
fn run_object_publication_root_graph_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "object publication root-graph workload requires at least one seed".to_owned(),
        ));
    }
    if backend != "object-store-local-fs+authority-quorum-fs" {
        return execution_from_result(Err(format!(
            "object publication root graph requires object-store-local-fs+authority-quorum-fs, got {backend}"
        )));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PublicationRootGraphMode::Correct,
        Some("omit_analytical_lease_root") => PublicationRootGraphMode::OmitAnalyticalLeaseRoot,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown object publication root-graph negative control {other}"
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
                "failed to create publication root-graph runtime: {error}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut root_types_expected = 0_u64;
    let mut root_types_registered = 0_u64;
    let mut authority_reopens = 0_u64;
    let mut complete_marks = 0_u64;
    let mut deferred_deletes = 0_u64;
    let mut reclaimed_objects = 0_u64;
    let mut object_requests = 0_u64;
    let mut object_bytes_written = 0_u64;
    let mut object_bytes_read = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first_root = std::env::temp_dir().join(format!(
            "okv-publication-root-graph-eval-first-{seed}-{}",
            Uuid::new_v4()
        ));
        let second_root = std::env::temp_dir().join(format!(
            "okv-publication-root-graph-eval-second-{seed}-{}",
            Uuid::new_v4()
        ));
        let first = runtime.block_on(run_publication_root_graph_contract(
            &first_root,
            *seed,
            mode,
        ));
        let second = runtime.block_on(run_publication_root_graph_contract(
            &second_root,
            *seed,
            mode,
        ));
        let _ = fs::remove_dir_all(&first_root);
        let _ = fs::remove_dir_all(&second_root);
        let (first, second) = match (first, second) {
            (Ok(first), Ok(second)) => (first, second),
            (Err(error), _) | (_, Err(error)) => {
                return execution_from_result(Err(format!(
                    "object publication root-graph execution failed for seed {seed}: {error}"
                )));
            }
        };

        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        root_types_expected = root_types_expected.saturating_add(first.root_types_expected);
        root_types_registered = root_types_registered.saturating_add(first.root_types_registered);
        authority_reopens = authority_reopens.saturating_add(first.authority_reopens);
        complete_marks = complete_marks.saturating_add(first.complete_marks);
        deferred_deletes = deferred_deletes.saturating_add(first.deferred_deletes);
        reclaimed_objects = reclaimed_objects.saturating_add(first.reclaimed_objects);
        object_requests = object_requests.saturating_add(first.object_requests);
        object_bytes_written = object_bytes_written.saturating_add(first.object_bytes_written);
        object_bytes_read = object_bytes_read.saturating_add(first.object_bytes_read);
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
                    ("oracle", "object-publication-root-graph-v1"),
                    (
                        "anomaly.class",
                        if exact {
                            "none"
                        } else {
                            "publication_root_graph"
                        },
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
                    ("api", "publication-root-graph"),
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
                    ("api", "publication-root-graph"),
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
                    ("api", "publication-root-graph"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "mark-sweep-root-graph"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-publication-root-graph://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_checks = seed_count.saturating_mul(9);
    let expected_root_types = seed_count.saturating_mul(5);
    let physical_boundaries_exercised = check_count == expected_checks
        && root_types_expected == expected_root_types
        && root_types_registered == expected_root_types
        && authority_reopens == seed_count
        && complete_marks == seed_count.saturating_mul(3)
        && deferred_deletes == seed_count
        && reclaimed_objects == seed_count.saturating_mul(2)
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
            "object publication root-graph gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, physical_boundaries_exercised={physical_boundaries_exercised}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "publication_root_graph.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "publication_root_graph.physical_boundaries_exercised".to_owned(),
                status: gate_status(physical_boundaries_exercised),
                detail: Some(format!(
                    "checks={check_count}, root_types={root_types_registered}/{root_types_expected}, authority_reopens={authority_reopens}, complete_marks={complete_marks}, deferred={deferred_deletes}, reclaimed={reclaimed_objects}, requests={object_requests}"
                )),
            },
            HardGateResult {
                id: "publication_root_graph.contract_agreement".to_owned(),
                status: gate_status(anomaly_count == 0),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("publication_root_graph.checks".to_owned(), bounded_count(check_count)),
            (
                "publication_root_graph.root_types_expected".to_owned(),
                bounded_count(root_types_expected),
            ),
            (
                "publication_root_graph.root_types_registered".to_owned(),
                bounded_count(root_types_registered),
            ),
            (
                "publication_root_graph.authority_reopens".to_owned(),
                bounded_count(authority_reopens),
            ),
            (
                "publication_root_graph.complete_marks".to_owned(),
                bounded_count(complete_marks),
            ),
            (
                "publication_root_graph.deferred_deletes".to_owned(),
                bounded_count(deferred_deletes),
            ),
            (
                "publication_root_graph.reclaimed_objects".to_owned(),
                bounded_count(reclaimed_objects),
            ),
            (
                "publication_root_graph.object_requests".to_owned(),
                bounded_count(object_requests),
            ),
            (
                "publication_root_graph.object_bytes_written".to_owned(),
                bounded_count(object_bytes_written),
            ),
            (
                "publication_root_graph.object_bytes_read".to_owned(),
                bounded_count(object_bytes_read),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_routine_reconfiguration_contract_workload(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "routine reconfiguration workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => RoutineReconfigurationMode::Correct,
        Some("reuse_node_identity") => RoutineReconfigurationMode::ReuseNodeIdentity,
        Some("admit_learner_without_authority") => {
            RoutineReconfigurationMode::AdmitLearnerWithoutAuthority
        }
        Some("promote_before_catchup") => RoutineReconfigurationMode::PromoteBeforeCatchup,
        Some("accept_stale_membership_epoch") => {
            RoutineReconfigurationMode::AcceptStaleMembershipEpoch
        }
        Some("accept_concurrent_reconfiguration") => {
            RoutineReconfigurationMode::AcceptConcurrentReconfiguration
        }
        Some("double_apply_finalize_retry") => RoutineReconfigurationMode::DoubleApplyFinalizeRetry,
        Some("accept_removed_voter_commit") => RoutineReconfigurationMode::AcceptRemovedVoterCommit,
        Some("repair_without_data_quorum") => RoutineReconfigurationMode::RepairWithoutDataQuorum,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown routine reconfiguration negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_preparations = 0_u64;
    let mut learner_admissions = 0_u64;
    let mut learner_ready_certificates = 0_u64;
    let mut membership_changes = 0_u64;
    let mut finalize_attempts = 0_u64;
    let mut committed_transactions = 0_u64;
    let mut rejected_controls = 0_u64;
    let mut exact_replay = true;
    let mut final_generation = 0_u64;
    let mut final_membership_epoch = 0_u64;
    let mut active_voters = Vec::new();
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = run_routine_reconfiguration_contract(*seed, mode);
        let second = run_routine_reconfiguration_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_preparations =
            authority_preparations.saturating_add(first.authority_preparations);
        learner_admissions = learner_admissions.saturating_add(first.learner_admissions);
        learner_ready_certificates =
            learner_ready_certificates.saturating_add(first.learner_ready_certificates);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        finalize_attempts = finalize_attempts.saturating_add(first.finalize_attempts);
        committed_transactions =
            committed_transactions.saturating_add(first.committed_transactions);
        rejected_controls = rejected_controls.saturating_add(first.rejected_controls);
        final_generation = first.generation;
        final_membership_epoch = first.membership_epoch;
        active_voters.clone_from(&first.active_voters);
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
                    ("oracle", "routine-reconfiguration-v0"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "membership_repair" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_transactions),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "same-generation-routine-repair-v0"),
                    (
                        "result",
                        if exact {
                            "committed"
                        } else {
                            "contract_mismatch"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "recovery.membership_epoch",
                value: bounded_count(first.membership_epoch),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("cell", "17"),
                    ("transaction_system", "cell-v0"),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "routine-voter-reconfiguration"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-routine-reconfiguration://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(19)
        && finalize_attempts == seed_count.saturating_mul(2);
    let expected_final_state = final_generation == 7
        && final_membership_epoch == 5
        && active_voters == [2, 3, 4]
        && authority_preparations == seed_count
        && learner_admissions == seed_count
        && learner_ready_certificates == seed_count
        && membership_changes == seed_count
        && committed_transactions == seed_count.saturating_mul(2)
        && rejected_controls == seed_count.saturating_mul(6);
    let passed =
        anomaly_count == 0 && exact_replay && semantic_operations_exercised && expected_final_state;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "routine reconfiguration gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_final_state={expected_final_state}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "routine_reconfiguration.exact_seed_replay".to_owned(),
                status: gate_status(exact_replay),
                detail: None,
            },
            HardGateResult {
                id: "routine_reconfiguration.semantic_operations_exercised".to_owned(),
                status: gate_status(semantic_operations_exercised),
                detail: Some(format!(
                    "checks={check_count}, preparations={authority_preparations}, learner_admissions={learner_admissions}, ready_certificates={learner_ready_certificates}, membership_changes={membership_changes}, finalize_attempts={finalize_attempts}, commits={committed_transactions}, rejected_controls={rejected_controls}"
                )),
            },
            HardGateResult {
                id: "routine_reconfiguration.contract_agreement".to_owned(),
                status: gate_status(anomaly_count == 0 && expected_final_state),
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "routine_reconfiguration.checks".to_owned(),
                bounded_count(check_count),
            ),
            (
                "routine_reconfiguration.preparations".to_owned(),
                bounded_count(authority_preparations),
            ),
            (
                "routine_reconfiguration.learner_admissions".to_owned(),
                bounded_count(learner_admissions),
            ),
            (
                "routine_reconfiguration.membership_changes".to_owned(),
                bounded_count(membership_changes),
            ),
            (
                "routine_reconfiguration.finalize_attempts".to_owned(),
                bounded_count(finalize_attempts),
            ),
            (
                "routine_reconfiguration.committed_transactions".to_owned(),
                bounded_count(committed_transactions),
            ),
            (
                "routine_reconfiguration.rejected_controls".to_owned(),
                bounded_count(rejected_controls),
            ),
            (
                "routine_reconfiguration.membership_epoch".to_owned(),
                bounded_count(final_membership_epoch),
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

fn bounded_usize(value: usize) -> f64 {
    bounded_count(u64::try_from(value).unwrap_or(u64::MAX))
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

fn file_sha256(path: &Path) -> Result<String, String> {
    let path = path
        .to_str()
        .ok_or_else(|| "executable path is not valid UTF-8".to_owned())?;
    for (program, arguments) in [
        ("shasum", vec!["-a", "256", path]),
        ("sha256sum", vec![path]),
    ] {
        if let Some(output) = command_output(program, &arguments) {
            if let Some(digest) = output.split_whitespace().next() {
                if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Ok(digest.to_owned());
                }
            }
        }
    }
    fs::read(path)
        .map(|bytes| sha256(&bytes))
        .map_err(|error| format!("hash executable {path}: {error}"))
}

fn contract_hash(loaded: &LoadedSuite) -> Result<String, Box<dyn Error>> {
    let registry_bytes = fs::read(&loaded.registry_path)?;
    let schema_bytes = fs::read(&loaded.result_schema_path)?;
    let mut hasher = Sha256::new();
    for bytes in [&loaded.suite_bytes, &registry_bytes, &schema_bytes] {
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    for path in &loaded.contract_paths {
        let bytes = fs::read(path)?;
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
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
mod lane_constraint_tests {
    use super::constraint_holds;
    use okv_eval::config::ConstraintOp;

    #[test]
    fn applies_every_constraint_operator() {
        assert!(constraint_holds(1.0, ConstraintOp::Eq, 1.0));
        assert!(constraint_holds(2.0, ConstraintOp::Ge, 1.0));
        assert!(constraint_holds(2.0, ConstraintOp::Gt, 1.0));
        assert!(constraint_holds(1.0, ConstraintOp::Le, 2.0));
        assert!(constraint_holds(1.0, ConstraintOp::Lt, 2.0));
        assert!(!constraint_holds(0.2671, ConstraintOp::Le, 0.025));
    }
}
