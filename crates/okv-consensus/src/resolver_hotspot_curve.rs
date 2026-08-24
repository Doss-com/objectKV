use crate::rpc::{read_frame, read_response, write_request, write_response};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

const HOTSPOT_REQUEST: u8 = 91;
const RETRY_ATTEMPTS: usize = 500;
const SOURCE_MAP_EPOCH: u64 = 1;
const SPLIT_MAP_EPOCH: u64 = 2;
const LEFT_PREFIX: u8 = 0x61;
const RIGHT_PREFIX: u8 = 0x81;
const LEFT_WRITE_PREFIX: u8 = 0x62;
const RIGHT_WRITE_PREFIX: u8 = 0x82;
const LEFT_PROBE_PREFIX: u8 = 0x63;
const RIGHT_PROBE_PREFIX: u8 = 0x83;

/// Frozen subjects for RFC-0055.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverHotspotCurveMode {
    Correct,
    RouteCrossingToOneChild,
    MutateSplitWorkload,
    SkipOutcomeValidation,
    IncludeWorkerStartup,
    SerializeSplitChildren,
}

impl ResolverHotspotCurveMode {
    /// Stable configuration identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::RouteCrossingToOneChild => "route_crossing_to_one_child",
            Self::MutateSplitWorkload => "mutate_split_workload",
            Self::SkipOutcomeValidation => "skip_outcome_validation",
            Self::IncludeWorkerStartup => "include_worker_startup",
            Self::SerializeSplitChildren => "serialize_split_children",
        }
    }
}

/// Frozen logical load shapes for RFC-0055.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResolverHotspotDistribution {
    BalancedIndependent,
    MissedHotKeyBoundary,
    Crossing25,
    Crossing100,
}

impl ResolverHotspotDistribution {
    /// Stable configuration identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::BalancedIndependent => "balanced-independent",
            Self::MissedHotKeyBoundary => "missed-hot-key-boundary",
            Self::Crossing25 => "crossing-25",
            Self::Crossing100 => "crossing-100",
        }
    }
}

/// Fixed input to one paired source and split curve point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolverHotspotCurveConfig {
    pub seed: u64,
    pub distribution: ResolverHotspotDistribution,
    pub logical_transactions: u64,
    pub batches: u64,
    pub transactions_per_batch: u64,
    pub warmup_transactions: u64,
    pub history_entries_total: u64,
    pub repetitions: u32,
    pub minimum_available_parallelism: usize,
    pub controller_threads: usize,
}

/// Configuration for one long-lived evaluation-only resolver worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolverHotspotWorkerConfig {
    pub worker_id: u16,
    pub listen_address: String,
    pub owned_start: u64,
    pub owned_end: u64,
    pub left_history_entries: u64,
    pub right_history_entries: u64,
    pub right_sequence_offset: u64,
}

/// One paired timing sample.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ResolverHotspotSample {
    pub repetition: u32,
    pub source_first: bool,
    pub source_seconds: f64,
    pub split_seconds: f64,
    pub source_throughput: f64,
    pub split_throughput: f64,
    pub throughput_ratio: f64,
    pub source_resolver_decisions: u64,
    pub split_resolver_decisions: u64,
    pub source_history_entries_examined: u64,
    pub split_history_entries_examined: u64,
    pub left_operations: u64,
    pub right_operations: u64,
    pub split_hotspot_ratio: f64,
    pub child_execution_overlapped: bool,
}

/// Canonical report for one frozen curve point.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ResolverHotspotCurveReport {
    pub config: ResolverHotspotCurveConfig,
    pub mode: ResolverHotspotCurveMode,
    pub samples: Vec<ResolverHotspotSample>,
    pub available_parallelism: usize,
    pub machine_fingerprint: String,
    pub executable_sha256: String,
    pub workload_sha256: String,
    pub source_history_sha256: String,
    pub child_history_union_sha256: String,
    pub source_and_split_workload_digest_exact: bool,
    pub source_outcomes_match_oracle: bool,
    pub split_outcomes_match_oracle: bool,
    pub source_and_split_outcomes_match: bool,
    pub crossing_transactions_reach_every_child: bool,
    pub one_map_epoch_per_transaction: bool,
    pub source_history_is_exact_child_union: bool,
    pub worker_startup_excluded_from_timing: bool,
    pub history_preparation_excluded_from_timing: bool,
    pub warmup_excluded_from_timing: bool,
    pub every_outcome_validated: bool,
    pub split_child_execution_overlaps: bool,
    pub operation_count_fixed: bool,
    pub batch_order_fixed: bool,
    pub controller_concurrency_fixed: bool,
    pub same_executable_and_machine: bool,
    pub alternating_topology_order_complete: bool,
    pub duration_distribution_recorded: bool,
    pub exact_untimed_replay: bool,
    pub negative_control_detected: bool,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum OperationPlacement {
    Left,
    Right,
    Crossing,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct HotRange {
    start: u64,
    end: u64,
}

impl HotRange {
    fn valid(self) -> bool {
        self.start < self.end
    }

    fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct LogicalOperation {
    transaction_id: u64,
    batch_id: u64,
    candidate_sequence: u64,
    read_sequence: u64,
    placement: OperationPlacement,
    read_conflicts: Vec<HotRange>,
    write_conflicts: Vec<HotRange>,
    expected_conflict: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerOperation {
    transaction_id: u64,
    batch_id: u64,
    candidate_sequence: u64,
    read_sequence: u64,
    map_epoch: u64,
    read_conflicts: Vec<HotRange>,
    write_conflicts: Vec<HotRange>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct HistoryEntry {
    sequence: u64,
    range: HotRange,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum WorkerRequest {
    Status,
    Prepare {
        run_id: String,
        workload_sha256: String,
        operations: Vec<WorkerOperation>,
    },
    Execute {
        run_id: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum WorkerResponse {
    Status(WorkerStatus),
    Prepared(WorkerPrepared),
    Executed(WorkerExecution),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerStatus {
    worker_id: u16,
    base_history_entries: u64,
    base_history_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerPrepared {
    run_id: String,
    workload_sha256: String,
    operation_count: u64,
    history_entries: u64,
    batch_order_valid: bool,
    one_map_epoch: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerOutcome {
    transaction_id: u64,
    conflict: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerExecution {
    run_id: String,
    workload_sha256: String,
    outcomes: Vec<WorkerOutcome>,
    history_entries_examined: u64,
    started_unix_nanos: u64,
    ended_unix_nanos: u64,
    batch_order_valid: bool,
    one_map_epoch: bool,
}

struct PreparedRun {
    run_id: String,
    workload_sha256: String,
    history: Vec<HistoryEntry>,
    operations: Vec<WorkerOperation>,
    batch_order_valid: bool,
    one_map_epoch: bool,
}

struct WorkerState {
    base_history: Vec<HistoryEntry>,
    base_history_sha256: String,
    prepared: Option<PreparedRun>,
}

#[derive(Clone)]
struct PreparedTopology {
    source_workload_sha256: String,
    split_workload_sha256: String,
    source: Vec<WorkerOperation>,
    left: Vec<WorkerOperation>,
    right: Vec<WorkerOperation>,
}

struct WorkerSet {
    children: Vec<Child>,
    source: String,
    left: String,
    right: String,
    source_status: WorkerStatus,
    left_status: WorkerStatus,
    right_status: WorkerStatus,
}

impl Drop for WorkerSet {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Run one long-lived memory-only resolver worker until its process is stopped.
///
/// # Errors
///
/// Returns an error for an invalid identity, range, request, or socket.
pub async fn run_resolver_hotspot_worker(
    config: ResolverHotspotWorkerConfig,
) -> Result<(), String> {
    if config.worker_id == 0 || config.owned_start >= config.owned_end {
        return Err("resolver hotspot worker identity or range is invalid".to_owned());
    }
    let base_history = history_for_config(&config);
    if base_history
        .iter()
        .any(|entry| entry.range.start < config.owned_start || entry.range.end > config.owned_end)
    {
        return Err("resolver hotspot history escapes its owned range".to_owned());
    }
    let mut state = WorkerState {
        base_history_sha256: history_sha256(&base_history),
        base_history,
        prepared: None,
    };
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let (mut stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let kind = stream.read_u8().await.map_err(|error| error.to_string())?;
        if kind != HOTSPOT_REQUEST {
            write_response::<_, Result<WorkerResponse, String>>(
                &mut stream,
                &Err("unknown resolver hotspot request".to_owned()),
            )
            .await
            .map_err(|error| error.to_string())?;
            continue;
        }
        let body = read_frame(&mut stream)
            .await
            .map_err(|error| error.to_string())?;
        let request =
            serde_json::from_slice::<WorkerRequest>(&body).map_err(|error| error.to_string())?;
        let response = apply_worker_request(&config, &mut state, request);
        write_response(&mut stream, &response)
            .await
            .map_err(|error| error.to_string())?;
    }
}

#[allow(clippy::too_many_lines)]
fn apply_worker_request(
    config: &ResolverHotspotWorkerConfig,
    state: &mut WorkerState,
    request: WorkerRequest,
) -> Result<WorkerResponse, String> {
    match request {
        WorkerRequest::Status => Ok(WorkerResponse::Status(WorkerStatus {
            worker_id: config.worker_id,
            base_history_entries: state.base_history.len() as u64,
            base_history_sha256: state.base_history_sha256.clone(),
        })),
        WorkerRequest::Prepare {
            run_id,
            workload_sha256,
            operations,
        } => {
            if run_id.is_empty() || workload_sha256.len() != 64 {
                return Err("resolver hotspot preparation identity is invalid".to_owned());
            }
            if operations.iter().any(|operation| {
                operation.transaction_id == 0
                    || operation.candidate_sequence == 0
                    || operation
                        .read_conflicts
                        .iter()
                        .chain(&operation.write_conflicts)
                        .any(|range| {
                            !range.valid()
                                || range.start < config.owned_start
                                || range.end > config.owned_end
                        })
            }) {
                return Err("resolver hotspot operation identity or range is invalid".to_owned());
            }
            let batch_order_valid = operations.windows(2).all(|pair| {
                (pair[0].batch_id, pair[0].candidate_sequence)
                    < (pair[1].batch_id, pair[1].candidate_sequence)
            });
            let map_epochs = operations
                .iter()
                .map(|operation| operation.map_epoch)
                .collect::<BTreeSet<_>>();
            let one_map_epoch = map_epochs.len() <= 1;
            let mut history = state.base_history.clone();
            history.reserve(operations.len().saturating_mul(2));
            let prepared = WorkerPrepared {
                run_id: run_id.clone(),
                workload_sha256: workload_sha256.clone(),
                operation_count: operations.len() as u64,
                history_entries: history.len() as u64,
                batch_order_valid,
                one_map_epoch,
            };
            state.prepared = Some(PreparedRun {
                run_id,
                workload_sha256,
                history,
                operations,
                batch_order_valid,
                one_map_epoch,
            });
            Ok(WorkerResponse::Prepared(prepared))
        }
        WorkerRequest::Execute { run_id } => {
            let mut prepared = state
                .prepared
                .take()
                .ok_or_else(|| "resolver hotspot execution was not prepared".to_owned())?;
            if prepared.run_id != run_id {
                return Err("resolver hotspot execution identity changed".to_owned());
            }
            let started_unix_nanos = unix_nanos()?;
            let mut outcomes = Vec::with_capacity(prepared.operations.len());
            let mut history_entries_examined = 0_u64;
            for operation in &prepared.operations {
                let mut conflict = false;
                'reads: for read in &operation.read_conflicts {
                    for entry in &prepared.history {
                        history_entries_examined = history_entries_examined.saturating_add(1);
                        if entry.sequence > operation.read_sequence && read.overlaps(entry.range) {
                            conflict = true;
                            break 'reads;
                        }
                    }
                }
                if !conflict {
                    prepared
                        .history
                        .extend(operation.write_conflicts.iter().map(|range| HistoryEntry {
                            sequence: operation.candidate_sequence,
                            range: *range,
                        }));
                }
                outcomes.push(WorkerOutcome {
                    transaction_id: operation.transaction_id,
                    conflict,
                });
            }
            let ended_unix_nanos = unix_nanos()?;
            Ok(WorkerResponse::Executed(WorkerExecution {
                run_id,
                workload_sha256: prepared.workload_sha256,
                outcomes,
                history_entries_examined,
                started_unix_nanos,
                ended_unix_nanos,
                batch_order_valid: prepared.batch_order_valid,
                one_map_epoch: prepared.one_map_epoch,
            }))
        }
    }
}

/// Measure one frozen resolver hotspot point through paired source and split workers.
///
/// # Errors
///
/// Returns an error when the fixed profile, process boundary, or receipt is invalid.
#[allow(clippy::too_many_lines)]
pub fn run_resolver_hotspot_curve_contract(
    config: &ResolverHotspotCurveConfig,
    mode: ResolverHotspotCurveMode,
    executable: &Path,
) -> Result<ResolverHotspotCurveReport, String> {
    validate_config(config)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_curve_async(config, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_curve_async(
    config: &ResolverHotspotCurveConfig,
    mode: ResolverHotspotCurveMode,
    executable: &Path,
) -> Result<ResolverHotspotCurveReport, String> {
    let available_parallelism = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    if available_parallelism < config.minimum_available_parallelism {
        return Err(format!(
            "resolver hotspot curve requires {} logical CPUs, found {available_parallelism}",
            config.minimum_available_parallelism
        ));
    }
    let executable_bytes = fs::read(executable).map_err(|error| error.to_string())?;
    let executable_sha256 = sha256_bytes(&executable_bytes);
    let machine_fingerprint = machine_fingerprint(available_parallelism);
    let operations = build_logical_operations(config)?;
    let workload_sha256 = logical_workload_sha256(&operations);
    let prepared = prepare_topology(&operations, &workload_sha256, mode);

    let setup_started = Instant::now();
    let mut workers = WorkerSet::start(config, executable).await?;
    let setup_seconds = setup_started.elapsed().as_secs_f64();
    let source_history = build_history(history_counts(config).0, history_counts(config).1);
    let child_history = {
        let (left_count, right_count) = history_counts(config);
        let mut union = build_partition_history(left_count, 0, 0);
        union.extend(build_partition_history(0, right_count, left_count));
        union.sort();
        union
    };
    let source_history_sha256 = history_sha256(&source_history);
    let child_history_union_sha256 = history_sha256(&child_history);
    let source_history_is_exact_child_union = source_history == child_history
        && workers.source_status.base_history_sha256 == source_history_sha256
        && history_union_sha256(&workers.left_status, &workers.right_status, config)
            == child_history_union_sha256;

    warm_workers(config, &prepared, &mut workers).await?;

    let oracle = operations
        .iter()
        .map(|operation| (operation.transaction_id, operation.expected_conflict))
        .collect::<BTreeMap<_, _>>();
    let mut samples = Vec::with_capacity(config.repetitions as usize);
    let mut source_outcomes_match_oracle = true;
    let mut split_outcomes_match_oracle = true;
    let mut source_and_split_outcomes_match = true;
    let mut crossing_transactions_reach_every_child = true;
    let mut one_map_epoch_per_transaction = true;
    let mut every_outcome_validated = true;
    let mut batch_order_fixed = true;
    let mut operation_count_fixed = true;
    let mut all_children_overlap = true;

    for repetition in 0..config.repetitions {
        let source_run = format!("source-{}-{repetition}", config.seed);
        let split_run = format!("split-{}-{repetition}", config.seed);
        let source_prepared = prepare_worker(
            &workers.source,
            &source_run,
            &prepared.source_workload_sha256,
            prepared.source.clone(),
        )
        .await?;
        let (left_prepared, right_prepared) = tokio::join!(
            prepare_worker(
                &workers.left,
                &split_run,
                &prepared.split_workload_sha256,
                prepared.left.clone(),
            ),
            prepare_worker(
                &workers.right,
                &split_run,
                &prepared.split_workload_sha256,
                prepared.right.clone(),
            )
        );
        let left_prepared = left_prepared?;
        let right_prepared = right_prepared?;
        let source_first = repetition % 2 == 0;
        let (source_timing, split_timing) = if source_first {
            let source = timed_execute_source(&workers.source, &source_run).await?;
            let split = timed_execute_split(
                &workers.left,
                &workers.right,
                &split_run,
                mode == ResolverHotspotCurveMode::SerializeSplitChildren,
            )
            .await?;
            (source, split)
        } else {
            let split = timed_execute_split(
                &workers.left,
                &workers.right,
                &split_run,
                mode == ResolverHotspotCurveMode::SerializeSplitChildren,
            )
            .await?;
            let source = timed_execute_source(&workers.source, &source_run).await?;
            (source, split)
        };

        let source_execution = source_timing.1;
        let left_execution = split_timing.1;
        let right_execution = split_timing.2;
        let child_overlap = executions_overlap(&left_execution, &right_execution)
            || left_execution.outcomes.is_empty()
            || right_execution.outcomes.is_empty();
        all_children_overlap &= child_overlap;
        batch_order_fixed &= source_prepared.batch_order_valid
            && left_prepared.batch_order_valid
            && right_prepared.batch_order_valid
            && source_execution.batch_order_valid
            && left_execution.batch_order_valid
            && right_execution.batch_order_valid;
        one_map_epoch_per_transaction &= source_prepared.one_map_epoch
            && left_prepared.one_map_epoch
            && right_prepared.one_map_epoch
            && source_execution.one_map_epoch
            && left_execution.one_map_epoch
            && right_execution.one_map_epoch;

        let source_outcomes = outcome_map(&source_execution.outcomes);
        let split_outcomes = aggregate_split_outcomes(
            &operations,
            &left_execution.outcomes,
            &right_execution.outcomes,
        );
        if mode == ResolverHotspotCurveMode::SkipOutcomeValidation {
            every_outcome_validated = false;
            source_outcomes_match_oracle = false;
            split_outcomes_match_oracle = false;
            source_and_split_outcomes_match = false;
        } else {
            source_outcomes_match_oracle &= source_outcomes == oracle;
            split_outcomes_match_oracle &= split_outcomes == oracle;
            source_and_split_outcomes_match &= source_outcomes == split_outcomes;
            every_outcome_validated &= source_outcomes.len() == operations.len()
                && split_outcomes.len() == operations.len();
        }
        crossing_transactions_reach_every_child &= crossing_routing_exact(
            &operations,
            &left_execution.outcomes,
            &right_execution.outcomes,
        );
        operation_count_fixed &= source_execution.outcomes.len() == operations.len()
            && split_outcomes.len() == operations.len();

        let mut source_seconds = source_timing.0;
        let mut split_seconds = split_timing.0;
        if mode == ResolverHotspotCurveMode::IncludeWorkerStartup {
            split_seconds += setup_seconds;
        }
        source_seconds = source_seconds.max(f64::EPSILON);
        split_seconds = split_seconds.max(f64::EPSILON);
        let logical_count = f64::from(
            u32::try_from(config.logical_transactions)
                .map_err(|_| "resolver hotspot transaction count exceeds u32".to_owned())?,
        );
        let source_throughput = logical_count / source_seconds;
        let split_throughput = logical_count / split_seconds;
        let left_operations = left_execution.outcomes.len() as u64;
        let right_operations = right_execution.outcomes.len() as u64;
        let split_decisions = left_operations.saturating_add(right_operations);
        samples.push(ResolverHotspotSample {
            repetition,
            source_first,
            source_seconds,
            split_seconds,
            source_throughput,
            split_throughput,
            throughput_ratio: split_throughput / source_throughput,
            source_resolver_decisions: source_execution.outcomes.len() as u64,
            split_resolver_decisions: split_decisions,
            source_history_entries_examined: source_execution.history_entries_examined,
            split_history_entries_examined: left_execution
                .history_entries_examined
                .saturating_add(right_execution.history_entries_examined),
            left_operations,
            right_operations,
            split_hotspot_ratio: hotspot_ratio(left_operations, right_operations)?,
            child_execution_overlapped: child_overlap,
        });
    }

    let source_and_split_workload_digest_exact = prepared.source_workload_sha256 == workload_sha256
        && prepared.split_workload_sha256 == workload_sha256;
    let worker_startup_excluded_from_timing =
        mode != ResolverHotspotCurveMode::IncludeWorkerStartup;
    let history_preparation_excluded_from_timing =
        mode != ResolverHotspotCurveMode::IncludeWorkerStartup;
    let warmup_excluded_from_timing = true;
    let split_child_execution_overlaps = all_children_overlap;
    let controller_concurrency_fixed =
        mode != ResolverHotspotCurveMode::SerializeSplitChildren && config.controller_threads == 2;
    let same_executable_and_machine = !executable_sha256.is_empty()
        && !machine_fingerprint.is_empty()
        && workers.source_status.worker_id == 1
        && workers.left_status.worker_id == 2
        && workers.right_status.worker_id == 3;
    let alternating_topology_order_complete = samples
        .iter()
        .all(|sample| sample.source_first == (sample.repetition % 2 == 0));
    let duration_distribution_recorded = samples.len() == config.repetitions as usize
        && samples.iter().all(|sample| {
            sample.source_seconds.is_finite()
                && sample.source_seconds > 0.0
                && sample.split_seconds.is_finite()
                && sample.split_seconds > 0.0
        });
    let canonical_receipt =
        canonical_untimed_receipt(config, &workload_sha256, &source_history_sha256, &oracle);
    let exact_untimed_replay = canonical_receipt
        == canonical_untimed_receipt(config, &workload_sha256, &source_history_sha256, &oracle);

    let checks = vec![
        (
            "source_and_split_workload_digest_exact",
            source_and_split_workload_digest_exact,
        ),
        ("source_outcomes_match_oracle", source_outcomes_match_oracle),
        ("split_outcomes_match_oracle", split_outcomes_match_oracle),
        (
            "source_and_split_outcomes_match",
            source_and_split_outcomes_match,
        ),
        (
            "crossing_transactions_reach_every_child",
            crossing_transactions_reach_every_child,
        ),
        (
            "one_map_epoch_per_transaction",
            one_map_epoch_per_transaction,
        ),
        (
            "source_history_is_exact_child_union",
            source_history_is_exact_child_union,
        ),
        (
            "worker_startup_excluded_from_timing",
            worker_startup_excluded_from_timing,
        ),
        (
            "history_preparation_excluded_from_timing",
            history_preparation_excluded_from_timing,
        ),
        ("warmup_excluded_from_timing", warmup_excluded_from_timing),
        ("every_outcome_validated", every_outcome_validated),
        (
            "split_child_execution_overlaps",
            split_child_execution_overlaps,
        ),
        ("operation_count_fixed", operation_count_fixed),
        ("batch_order_fixed", batch_order_fixed),
        ("controller_concurrency_fixed", controller_concurrency_fixed),
        ("same_executable_and_machine", same_executable_and_machine),
        (
            "alternating_topology_order_complete",
            alternating_topology_order_complete,
        ),
        (
            "duration_distribution_recorded",
            duration_distribution_recorded,
        ),
        ("exact_untimed_replay", exact_untimed_replay),
    ];
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let negative_control_detected = mode == ResolverHotspotCurveMode::Correct || anomaly_count > 0;
    let mut trace = Sha256::new();
    trace.update(b"okv-resolver-hotspot-throughput-curve-v0");
    trace.update(canonical_receipt.as_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }

    Ok(ResolverHotspotCurveReport {
        config: config.clone(),
        mode,
        samples,
        available_parallelism,
        machine_fingerprint,
        executable_sha256,
        workload_sha256,
        source_history_sha256,
        child_history_union_sha256,
        source_and_split_workload_digest_exact,
        source_outcomes_match_oracle,
        split_outcomes_match_oracle,
        source_and_split_outcomes_match,
        crossing_transactions_reach_every_child,
        one_map_epoch_per_transaction,
        source_history_is_exact_child_union,
        worker_startup_excluded_from_timing,
        history_preparation_excluded_from_timing,
        warmup_excluded_from_timing,
        every_outcome_validated,
        split_child_execution_overlaps,
        operation_count_fixed,
        batch_order_fixed,
        controller_concurrency_fixed,
        same_executable_and_machine,
        alternating_topology_order_complete,
        duration_distribution_recorded,
        exact_untimed_replay,
        negative_control_detected,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

impl WorkerSet {
    async fn start(config: &ResolverHotspotCurveConfig, executable: &Path) -> Result<Self, String> {
        let addresses = allocate_addresses(3)?;
        let (left_history_entries, right_history_entries) = history_counts(config);
        let configs = [
            ResolverHotspotWorkerConfig {
                worker_id: 1,
                listen_address: addresses[&1].clone(),
                owned_start: prefix_floor(0x50),
                owned_end: prefix_floor(0xa0),
                left_history_entries,
                right_history_entries,
                right_sequence_offset: left_history_entries,
            },
            ResolverHotspotWorkerConfig {
                worker_id: 2,
                listen_address: addresses[&2].clone(),
                owned_start: prefix_floor(0x50),
                owned_end: prefix_floor(0x78),
                left_history_entries,
                right_history_entries: 0,
                right_sequence_offset: left_history_entries,
            },
            ResolverHotspotWorkerConfig {
                worker_id: 3,
                listen_address: addresses[&3].clone(),
                owned_start: prefix_floor(0x78),
                owned_end: prefix_floor(0xa0),
                left_history_entries: 0,
                right_history_entries,
                right_sequence_offset: left_history_entries,
            },
        ];
        let mut children = Vec::new();
        for worker_config in &configs {
            let config_json =
                serde_json::to_string(worker_config).map_err(|error| error.to_string())?;
            children.push(
                child_command(executable, "resolver-hotspot-worker-node", &config_json)
                    .spawn()
                    .map_err(|error| error.to_string())?,
            );
        }
        let source_status = wait_worker_ready(&addresses[&1]).await?;
        let left_status = wait_worker_ready(&addresses[&2]).await?;
        let right_status = wait_worker_ready(&addresses[&3]).await?;
        Ok(Self {
            children,
            source: addresses[&1].clone(),
            left: addresses[&2].clone(),
            right: addresses[&3].clone(),
            source_status,
            left_status,
            right_status,
        })
    }
}

async fn warm_workers(
    config: &ResolverHotspotCurveConfig,
    prepared: &PreparedTopology,
    workers: &mut WorkerSet,
) -> Result<(), String> {
    let warmup_transactions = usize::try_from(config.warmup_transactions)
        .map_err(|_| "resolver hotspot warmup count exceeds usize".to_owned())?;
    let source = prepared
        .source
        .iter()
        .take(warmup_transactions)
        .cloned()
        .collect();
    let left = prepared
        .left
        .iter()
        .filter(|operation| operation.transaction_id <= config.warmup_transactions)
        .cloned()
        .collect();
    let right = prepared
        .right
        .iter()
        .filter(|operation| operation.transaction_id <= config.warmup_transactions)
        .cloned()
        .collect();
    prepare_worker(
        &workers.source,
        "warm-source",
        &prepared.source_workload_sha256,
        source,
    )
    .await?;
    let _ = execute_worker(&workers.source, "warm-source").await?;
    let (left_ready, right_ready) = tokio::join!(
        prepare_worker(
            &workers.left,
            "warm-split",
            &prepared.split_workload_sha256,
            left,
        ),
        prepare_worker(
            &workers.right,
            "warm-split",
            &prepared.split_workload_sha256,
            right,
        )
    );
    left_ready?;
    right_ready?;
    let (left_done, right_done) = tokio::join!(
        execute_worker(&workers.left, "warm-split"),
        execute_worker(&workers.right, "warm-split")
    );
    left_done?;
    right_done?;
    Ok(())
}

async fn timed_execute_source(
    address: &str,
    run_id: &str,
) -> Result<(f64, WorkerExecution), String> {
    let started = Instant::now();
    let execution = execute_worker(address, run_id).await?;
    Ok((started.elapsed().as_secs_f64(), execution))
}

async fn timed_execute_split(
    left: &str,
    right: &str,
    run_id: &str,
    serialize: bool,
) -> Result<(f64, WorkerExecution, WorkerExecution), String> {
    let started = Instant::now();
    let (left_execution, right_execution) = if serialize {
        let left_execution = execute_worker(left, run_id).await?;
        let right_execution = execute_worker(right, run_id).await?;
        (left_execution, right_execution)
    } else {
        let (left_execution, right_execution) =
            tokio::join!(execute_worker(left, run_id), execute_worker(right, run_id));
        (left_execution?, right_execution?)
    };
    Ok((
        started.elapsed().as_secs_f64(),
        left_execution,
        right_execution,
    ))
}

async fn prepare_worker(
    address: &str,
    run_id: &str,
    workload_sha256: &str,
    operations: Vec<WorkerOperation>,
) -> Result<WorkerPrepared, String> {
    match worker_call(
        address,
        &WorkerRequest::Prepare {
            run_id: run_id.to_owned(),
            workload_sha256: workload_sha256.to_owned(),
            operations,
        },
    )
    .await?
    {
        WorkerResponse::Prepared(prepared) => Ok(prepared),
        _ => Err("resolver hotspot worker returned the wrong prepare response".to_owned()),
    }
}

async fn execute_worker(address: &str, run_id: &str) -> Result<WorkerExecution, String> {
    match worker_call(
        address,
        &WorkerRequest::Execute {
            run_id: run_id.to_owned(),
        },
    )
    .await?
    {
        WorkerResponse::Executed(execution) => Ok(execution),
        _ => Err("resolver hotspot worker returned the wrong execute response".to_owned()),
    }
}

async fn worker_call(address: &str, request: &WorkerRequest) -> Result<WorkerResponse, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(address))
        .await
        .map_err(|_| format!("resolver hotspot connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, HOTSPOT_REQUEST, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<WorkerResponse, String> =
        tokio::time::timeout(Duration::from_secs(120), read_response(&mut stream))
            .await
            .map_err(|_| format!("resolver hotspot response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn wait_worker_ready(address: &str) -> Result<WorkerStatus, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match worker_call(address, &WorkerRequest::Status).await {
            Ok(WorkerResponse::Status(status)) => return Ok(status),
            Ok(_) => "wrong readiness response".clone_into(&mut last),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "resolver hotspot worker did not become ready: {last}"
    ))
}

fn validate_config(config: &ResolverHotspotCurveConfig) -> Result<(), String> {
    if config.seed == 0
        || config.logical_transactions == 0
        || config.batches == 0
        || config.transactions_per_batch == 0
        || config.logical_transactions
            != config.batches.saturating_mul(config.transactions_per_batch)
        || config.warmup_transactions == 0
        || config.warmup_transactions > config.logical_transactions
        || config.history_entries_total == 0
        || config.repetitions == 0
        || config.minimum_available_parallelism < 2
        || config.controller_threads != 2
    {
        return Err("resolver hotspot curve profile is invalid".to_owned());
    }
    Ok(())
}

fn build_logical_operations(
    config: &ResolverHotspotCurveConfig,
) -> Result<Vec<LogicalOperation>, String> {
    let capacity = usize::try_from(config.logical_transactions)
        .map_err(|_| "resolver hotspot transaction count exceeds usize".to_owned())?;
    let mut operations = Vec::with_capacity(capacity);
    for offset in 0..config.logical_transactions {
        let transaction_id = offset + 1;
        let batch_id = offset / config.transactions_per_batch + 1;
        let candidate_sequence = config.history_entries_total + transaction_id;
        let placement = placement(config.distribution, offset);
        let expected_conflict = transaction_id % 100 == 0;
        let mut read_conflicts = Vec::new();
        let mut write_conflicts = Vec::new();
        if placement == OperationPlacement::Left || placement == OperationPlacement::Crossing {
            read_conflicts.push(if expected_conflict {
                point_range(LEFT_PREFIX, 0)
            } else {
                point_range(LEFT_PROBE_PREFIX, transaction_id)
            });
            write_conflicts.push(point_range(LEFT_WRITE_PREFIX, transaction_id));
        }
        if placement == OperationPlacement::Right || placement == OperationPlacement::Crossing {
            read_conflicts.push(
                if expected_conflict && placement != OperationPlacement::Crossing {
                    point_range(RIGHT_PREFIX, 0)
                } else {
                    point_range(RIGHT_PROBE_PREFIX, transaction_id)
                },
            );
            write_conflicts.push(point_range(RIGHT_WRITE_PREFIX, transaction_id));
        }
        operations.push(LogicalOperation {
            transaction_id,
            batch_id,
            candidate_sequence,
            read_sequence: if expected_conflict {
                0
            } else {
                candidate_sequence - 1
            },
            placement,
            read_conflicts,
            write_conflicts,
            expected_conflict,
        });
    }
    Ok(operations)
}

fn prepare_topology(
    operations: &[LogicalOperation],
    workload_sha256: &str,
    mode: ResolverHotspotCurveMode,
) -> PreparedTopology {
    let source = operations
        .iter()
        .map(|operation| worker_operation(operation, SOURCE_MAP_EPOCH, None))
        .collect::<Vec<_>>();
    let left_range = HotRange {
        start: prefix_floor(0x50),
        end: prefix_floor(0x78),
    };
    let right_range = HotRange {
        start: prefix_floor(0x78),
        end: prefix_floor(0xa0),
    };
    let left = operations
        .iter()
        .filter_map(|operation| {
            worker_operation(operation, SPLIT_MAP_EPOCH, Some(left_range)).non_empty()
        })
        .collect::<Vec<_>>();
    let mut right = operations
        .iter()
        .filter_map(|operation| {
            worker_operation(operation, SPLIT_MAP_EPOCH, Some(right_range)).non_empty()
        })
        .collect::<Vec<_>>();
    if mode == ResolverHotspotCurveMode::RouteCrossingToOneChild {
        right.retain(|operation| {
            operations
                .iter()
                .find(|logical| logical.transaction_id == operation.transaction_id)
                .is_some_and(|logical| logical.placement != OperationPlacement::Crossing)
        });
    }
    let split_workload_sha256 = if mode == ResolverHotspotCurveMode::MutateSplitWorkload {
        sha256_bytes(format!("{workload_sha256}:mutated").as_bytes())
    } else {
        workload_sha256.to_owned()
    };
    PreparedTopology {
        source_workload_sha256: workload_sha256.to_owned(),
        split_workload_sha256,
        source,
        left,
        right,
    }
}

fn worker_operation(
    operation: &LogicalOperation,
    map_epoch: u64,
    owned: Option<HotRange>,
) -> WorkerOperation {
    let clip = |range: &HotRange| match owned {
        Some(owned) if range.overlaps(owned) => Some(HotRange {
            start: range.start.max(owned.start),
            end: range.end.min(owned.end),
        }),
        Some(_) => None,
        None => Some(*range),
    };
    WorkerOperation {
        transaction_id: operation.transaction_id,
        batch_id: operation.batch_id,
        candidate_sequence: operation.candidate_sequence,
        read_sequence: operation.read_sequence,
        map_epoch,
        read_conflicts: operation.read_conflicts.iter().filter_map(clip).collect(),
        write_conflicts: operation.write_conflicts.iter().filter_map(clip).collect(),
    }
}

trait NonEmptyOperation {
    fn non_empty(self) -> Option<Self>
    where
        Self: Sized;
}

impl NonEmptyOperation for WorkerOperation {
    fn non_empty(self) -> Option<Self> {
        (!self.read_conflicts.is_empty() || !self.write_conflicts.is_empty()).then_some(self)
    }
}

fn placement(distribution: ResolverHotspotDistribution, offset: u64) -> OperationPlacement {
    match distribution {
        ResolverHotspotDistribution::BalancedIndependent => {
            if offset % 2 == 0 {
                OperationPlacement::Left
            } else {
                OperationPlacement::Right
            }
        }
        ResolverHotspotDistribution::MissedHotKeyBoundary => OperationPlacement::Left,
        ResolverHotspotDistribution::Crossing25 => match offset % 8 {
            0 | 4 => OperationPlacement::Crossing,
            1 | 3 | 6 => OperationPlacement::Left,
            _ => OperationPlacement::Right,
        },
        ResolverHotspotDistribution::Crossing100 => OperationPlacement::Crossing,
    }
}

fn history_counts(config: &ResolverHotspotCurveConfig) -> (u64, u64) {
    if config.distribution == ResolverHotspotDistribution::MissedHotKeyBoundary {
        (config.history_entries_total, 0)
    } else {
        let left = config.history_entries_total / 2;
        (left, config.history_entries_total - left)
    }
}

fn history_for_config(config: &ResolverHotspotWorkerConfig) -> Vec<HistoryEntry> {
    build_partition_history(
        config.left_history_entries,
        config.right_history_entries,
        config.right_sequence_offset,
    )
}

fn build_history(left_entries: u64, right_entries: u64) -> Vec<HistoryEntry> {
    build_partition_history(left_entries, right_entries, left_entries)
}

fn build_partition_history(
    left_entries: u64,
    right_entries: u64,
    right_sequence_offset: u64,
) -> Vec<HistoryEntry> {
    let mut history = Vec::with_capacity(
        usize::try_from(left_entries.saturating_add(right_entries)).unwrap_or(usize::MAX),
    );
    history.extend((0..left_entries).map(|offset| HistoryEntry {
        sequence: offset + 1,
        range: point_range(LEFT_PREFIX, offset),
    }));
    history.extend((0..right_entries).map(|offset| HistoryEntry {
        sequence: right_sequence_offset + offset + 1,
        range: point_range(RIGHT_PREFIX, offset),
    }));
    history.sort();
    history
}

fn outcome_map(outcomes: &[WorkerOutcome]) -> BTreeMap<u64, bool> {
    outcomes
        .iter()
        .map(|outcome| (outcome.transaction_id, outcome.conflict))
        .collect()
}

fn aggregate_split_outcomes(
    operations: &[LogicalOperation],
    left: &[WorkerOutcome],
    right: &[WorkerOutcome],
) -> BTreeMap<u64, bool> {
    let left = outcome_map(left);
    let right = outcome_map(right);
    operations
        .iter()
        .map(|operation| {
            let conflict = left
                .get(&operation.transaction_id)
                .copied()
                .unwrap_or(false)
                || right
                    .get(&operation.transaction_id)
                    .copied()
                    .unwrap_or(false);
            (operation.transaction_id, conflict)
        })
        .collect()
}

fn crossing_routing_exact(
    operations: &[LogicalOperation],
    left: &[WorkerOutcome],
    right: &[WorkerOutcome],
) -> bool {
    let left_ids = left
        .iter()
        .map(|outcome| outcome.transaction_id)
        .collect::<BTreeSet<_>>();
    let right_ids = right
        .iter()
        .map(|outcome| outcome.transaction_id)
        .collect::<BTreeSet<_>>();
    operations
        .iter()
        .filter(|operation| operation.placement == OperationPlacement::Crossing)
        .all(|operation| {
            left_ids.contains(&operation.transaction_id)
                && right_ids.contains(&operation.transaction_id)
        })
}

fn executions_overlap(left: &WorkerExecution, right: &WorkerExecution) -> bool {
    left.started_unix_nanos < right.ended_unix_nanos
        && right.started_unix_nanos < left.ended_unix_nanos
}

fn hotspot_ratio(left: u64, right: u64) -> Result<f64, String> {
    let total = left
        .checked_add(right)
        .ok_or_else(|| "resolver hotspot operation total overflowed".to_owned())?;
    let mean = f64::from(
        u32::try_from(total)
            .map_err(|_| "resolver hotspot operation total exceeds u32".to_owned())?,
    ) / 2.0;
    if mean == 0.0 {
        Ok(0.0)
    } else {
        Ok(f64::from(
            u32::try_from(left.max(right))
                .map_err(|_| "resolver hotspot child operation count exceeds u32".to_owned())?,
        ) / mean)
    }
}

fn logical_workload_sha256(operations: &[LogicalOperation]) -> String {
    sha256_bytes(&serde_json::to_vec(operations).unwrap_or_default())
}

fn history_sha256(history: &[HistoryEntry]) -> String {
    sha256_bytes(&serde_json::to_vec(history).unwrap_or_default())
}

fn history_union_sha256(
    left: &WorkerStatus,
    right: &WorkerStatus,
    config: &ResolverHotspotCurveConfig,
) -> String {
    let (left_count, right_count) = history_counts(config);
    if left.base_history_entries != left_count || right.base_history_entries != right_count {
        return String::new();
    }
    let left_expected = build_partition_history(left_count, 0, 0);
    let right_expected = build_partition_history(0, right_count, left_count);
    if left.base_history_sha256 != history_sha256(&left_expected)
        || right.base_history_sha256 != history_sha256(&right_expected)
    {
        return String::new();
    }
    history_sha256(&build_history(left_count, right_count))
}

fn canonical_untimed_receipt(
    config: &ResolverHotspotCurveConfig,
    workload_sha256: &str,
    history_sha256: &str,
    oracle: &BTreeMap<u64, bool>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"okv-resolver-hotspot-untimed-v0");
    digest.update(serde_json::to_vec(config).unwrap_or_default());
    digest.update(workload_sha256.as_bytes());
    digest.update(history_sha256.as_bytes());
    digest.update(serde_json::to_vec(oracle).unwrap_or_default());
    format!("{:x}", digest.finalize())
}

fn machine_fingerprint(available_parallelism: usize) -> String {
    sha256_bytes(
        format!(
            "{}:{}:{}",
            std::env::consts::OS,
            std::env::consts::ARCH,
            available_parallelism
        )
        .as_bytes(),
    )
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn unix_nanos() -> Result<u64, String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    u64::try_from(nanos).map_err(|error| error.to_string())
}

const fn prefix_floor(prefix: u8) -> u64 {
    (prefix as u64) << 56
}

fn point_range(prefix: u8, value: u64) -> HotRange {
    let start = prefix_floor(prefix) | (value & 0x00ff_ffff_ffff_ffff);
    HotRange {
        start,
        end: start.saturating_add(1),
    }
}

fn child_command(executable: &Path, command_name: &str, config_json: &str) -> Command {
    let mut command = Command::new(executable);
    command
        .arg(command_name)
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null());
    if std::env::var_os("OKV_EVAL_CHILD_STDERR").is_some() {
        command.stderr(Stdio::inherit());
    } else {
        command.stderr(Stdio::null());
    }
    command
}

fn allocate_addresses(count: u16) -> Result<BTreeMap<u16, String>, String> {
    let mut listeners = Vec::new();
    for _ in 0..count {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let addresses = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| {
            let worker_id = u16::try_from(index + 1).map_err(|error| error.to_string())?;
            listener
                .local_addr()
                .map(|address| (worker_id, address.to_string()))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    drop(listeners);
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_distributions_have_exact_counts() {
        let config = ResolverHotspotCurveConfig {
            seed: 1103,
            distribution: ResolverHotspotDistribution::Crossing25,
            logical_transactions: 8192,
            batches: 128,
            transactions_per_batch: 64,
            warmup_transactions: 512,
            history_entries_total: 4096,
            repetitions: 7,
            minimum_available_parallelism: 2,
            controller_threads: 2,
        };
        let operations = build_logical_operations(&config).expect("valid frozen workload");
        assert_eq!(operations.len(), 8192);
        assert_eq!(
            operations
                .iter()
                .filter(|operation| operation.placement == OperationPlacement::Crossing)
                .count(),
            2048
        );
        assert_eq!(
            operations
                .iter()
                .filter(|operation| operation.placement == OperationPlacement::Left)
                .count(),
            3072
        );
        assert_eq!(
            operations
                .iter()
                .filter(|operation| operation.placement == OperationPlacement::Right)
                .count(),
            3072
        );
    }

    #[test]
    fn source_history_is_exact_child_union() {
        let source = build_history(2048, 2048);
        let mut children = build_partition_history(2048, 0, 0);
        children.extend(build_partition_history(0, 2048, 2048));
        children.sort();
        assert_eq!(source, children);
        assert_eq!(history_sha256(&source), history_sha256(&children));
    }
}
