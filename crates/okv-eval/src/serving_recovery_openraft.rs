//! Replacement-worker recovery from immutable row objects and the logical
//! retained transaction stream owned by real `OpenRaft` data processes.

use crate::serving_recovery::{ServingReadOutcome, ServingRecoveryProfile};
use okv::{ReadOutcome, SingleRange, SingleRangeConfig, StreamCursor};
use okv_consensus::{
    GenerationClient, GenerationPhase, PublicationAction, PublicationAuthorityProcessFixture,
    PublicationCommand, PublicationCommandStatus, PublicationIntent, PublicationObjectKind,
    PublicationObjectReference, RequestIdentity, RetainedTransactionReadRequest,
    RetainedTransactionRecord, TransactionAuthorityProcessFixture, TransactionBatchItem,
    TransactionLogClient, TransactionMutation,
};
use okv_object::{
    content_sha256, encode_row_object_set, filesystem_backend, gcs_backend_from_env,
    prefixed_backend, read_indexed_point, read_point_from_full_object, Backend, ObservedBackend,
    PointReadOutcome, RowObjectManifestV1, RowObjectReference, RowRecord, RowSegmentIndex,
    WriteCondition,
};
use okv_transaction::{KeyRange, TransactionCommand, TransactionStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{Builder as TempDirBuilder, TempDir};

const GENERATION: u64 = 7;
const LOGICAL_TXLOG_ROOT: &str = "wal-g7";
const PAGE_RECORDS: u32 = 2;
const BASE_BATCH_KEYS: usize = 32;

/// Frozen subject behavior for the G4.4 retained-stream recovery contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenRaftServingRecoveryMode {
    Candidate,
    IntegratedKernelCandidate,
    IntegratedKernelRocksDbCandidate,
    IntegratedKernelNativeRocksDbCandidate,
    FullHydrationControl,
    SkipConcurrentCatchupPoison,
}

/// Object backend opened independently by the controller and disposable workers.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OpenRaftServingObjectBackend {
    #[default]
    LocalFilesystem,
    Gcs {
        prefix: String,
    },
}

impl OpenRaftServingObjectBackend {
    fn open(&self, filesystem_root: &Path) -> Result<Arc<dyn Backend>, String> {
        match self {
            Self::LocalFilesystem => {
                filesystem_backend(filesystem_root).map_err(|error| error.to_string())
            }
            Self::Gcs { prefix } => prefixed_backend(
                gcs_backend_from_env().map_err(|error| error.to_string())?,
                prefix.clone(),
            )
            .map_err(|error| error.to_string()),
        }
    }
}

impl OpenRaftServingRecoveryMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::IntegratedKernelCandidate => "integrated_kernel_candidate",
            Self::IntegratedKernelRocksDbCandidate => "integrated_kernel_rocksdb_candidate",
            Self::IntegratedKernelNativeRocksDbCandidate => {
                "integrated_kernel_native_rocksdb_candidate"
            }
            Self::FullHydrationControl => "full_hydration_control",
            Self::SkipConcurrentCatchupPoison => "skip_concurrent_catchup_poison",
        }
    }
}

/// Configuration supplied to one disposable G4.4 serving process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenRaftServingProcessConfig {
    pub authority_endpoints: Vec<String>,
    pub transaction_endpoints: Vec<String>,
    pub publication_root: String,
    pub object_store_root: PathBuf,
    #[serde(default)]
    pub object_backend: OpenRaftServingObjectBackend,
    pub scratch_root: PathBuf,
    pub read_keys: Vec<Vec<u8>>,
    pub mode: OpenRaftServingRecoveryMode,
    pub initial_catchup_barrier: PathBuf,
    pub continue_barrier: PathBuf,
    pub max_page_records: u32,
    #[serde(default)]
    pub hot_read: Option<OpenRaftHotReadProfile>,
}

/// Configurable steady-state read window executed only after the serving image
/// is complete and the retained transaction suffix has been applied.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OpenRaftHotReadProfile {
    pub seed: u64,
    pub key_count: u64,
    pub value_bytes: usize,
    pub warmup_operations: usize,
    pub measured_operations: usize,
}

/// Public-kernel point-read evidence from one replacement worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenRaftHotReadReport {
    pub warmup_operations: u64,
    pub measured_operations: u64,
    pub elapsed_seconds: f64,
    pub operations_per_second: f64,
    pub latency_ns_p50: u64,
    pub latency_ns_p95: u64,
    pub latency_ns_p99: u64,
    pub latency_ns_p999: u64,
    pub correctness_failures: u64,
    pub object_requests: u64,
    pub checksum: u64,
}

/// Evidence emitted by one replacement process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenRaftServingProcessReport {
    pub mode: OpenRaftServingRecoveryMode,
    pub scratch_was_empty: bool,
    pub generation_sandwich_stable: bool,
    pub generation: u64,
    pub logical_txlog_root: String,
    pub manifest_authoritative: bool,
    pub object_durable_version: u64,
    pub initial_target_version: u64,
    pub activation_target_version: u64,
    pub catchup_rounds: u64,
    pub txlog_read_requests: u64,
    pub txlog_response_payload_bytes: u64,
    pub batch_cursor_resumes: u64,
    pub initial_records_applied: u64,
    pub concurrent_records_observed: u64,
    pub concurrent_records_applied: u64,
    pub physical_wal_path_accesses: u64,
    pub manifest_requests: u64,
    pub index_requests: u64,
    pub data_range_requests: u64,
    pub data_full_requests: u64,
    pub list_requests: u64,
    pub total_object_response_bytes: u64,
    pub row_segment_count: u64,
    pub row_index_closure_bytes: u64,
    pub row_data_closure_bytes: u64,
    pub serving_image_provider: Option<String>,
    pub serving_image_records: u64,
    pub serving_image_local_bytes: u64,
    pub resident_engine_provider: Option<String>,
    pub resident_engine_records: u64,
    pub resident_engine_local_bytes: u64,
    pub resident_engine_applied_version: u64,
    pub first_read_seconds: f64,
    pub reads: Vec<OpenRaftServingRead>,
    pub hot_read: Option<OpenRaftHotReadReport>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OpenRaftServingRead {
    pub key: Vec<u8>,
    pub outcome: ServingReadOutcome,
}

/// End-to-end G4.4 evidence across one killed worker and its replacement.
#[derive(Clone, Debug, Serialize)]
pub struct OpenRaftServingRecoveryReport {
    pub seed: u64,
    pub mode: OpenRaftServingRecoveryMode,
    pub authority_processes: u64,
    pub worker_process_starts: u64,
    pub worker_process_kills: u64,
    pub empty_scratch_restarts: u64,
    pub concurrent_commits: u64,
    pub correctness_anomalies: u64,
    pub exact_replay: bool,
    pub semantic_sha256: String,
    pub process: OpenRaftServingProcessReport,
}

#[derive(Clone, Debug)]
struct PublishedRowBase {
    object_durable_version: u64,
    segment_count: u64,
    index_closure_bytes: u64,
    data_closure_bytes: u64,
}

#[derive(Clone, Debug)]
struct History {
    base_records: Vec<RowRecord>,
    initial_commands: Vec<TransactionCommand>,
    concurrent_commands: Vec<TransactionCommand>,
    read_keys: Vec<Vec<u8>>,
    expected: Vec<ServingReadOutcome>,
}

#[derive(Clone, Debug)]
struct OrderedAction {
    version: u64,
    ordinal: usize,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct OrderedRangeClear {
    version: u64,
    ordinal: usize,
    range: KeyRange,
}

#[derive(Default)]
struct TailOverlay {
    points: BTreeMap<Vec<u8>, Vec<OrderedAction>>,
    range_clears: Vec<OrderedRangeClear>,
}

impl TailOverlay {
    fn apply(&mut self, record: &RetainedTransactionRecord) {
        for (ordinal, mutation) in record.command.mutations.iter().enumerate() {
            match mutation {
                TransactionMutation::Set { key, value } => {
                    self.points
                        .entry(key.clone())
                        .or_default()
                        .push(OrderedAction {
                            version: record.commit_version,
                            ordinal,
                            value: Some(value.clone()),
                        });
                }
                TransactionMutation::Clear { key } => {
                    self.points
                        .entry(key.clone())
                        .or_default()
                        .push(OrderedAction {
                            version: record.commit_version,
                            ordinal,
                            value: None,
                        });
                }
                TransactionMutation::ClearRange { range } => {
                    self.range_clears.push(OrderedRangeClear {
                        version: record.commit_version,
                        ordinal,
                        range: range.clone(),
                    });
                }
            }
        }
    }

    fn read(&self, key: &[u8], version: u64) -> Option<ServingReadOutcome> {
        let point = self.points.get(key).and_then(|actions| {
            actions
                .iter()
                .rev()
                .find(|action| action.version <= version)
        });
        let clear = self
            .range_clears
            .iter()
            .rev()
            .find(|clear| clear.version <= version && clear.range.contains(key));
        match (point, clear) {
            (None, None) => None,
            (Some(point), None) => Some(action_outcome(point)),
            (None, Some(_)) => Some(ServingReadOutcome::Tombstone),
            (Some(point), Some(clear)) => {
                if (point.version, point.ordinal) > (clear.version, clear.ordinal) {
                    Some(action_outcome(point))
                } else {
                    Some(ServingReadOutcome::Tombstone)
                }
            }
        }
    }
}

fn action_outcome(action: &OrderedAction) -> ServingReadOutcome {
    action
        .value
        .as_deref()
        .map_or(ServingReadOutcome::Tombstone, value_outcome)
}

struct OpenWorker {
    backend: ObservedBackend,
    manifest: RowObjectManifestV1,
    overlay: TailOverlay,
    hydrated: BTreeMap<String, (RowSegmentIndex, Vec<u8>)>,
    mode: OpenRaftServingRecoveryMode,
    txlog: TransactionLogClient,
    cursor: StreamCursor,
    txlog_read_requests: u64,
    txlog_response_payload_bytes: u64,
    index_requests: u64,
    data_range_requests: u64,
    data_full_requests: u64,
    index_response_bytes: u64,
    data_response_bytes: u64,
    generation: u64,
    logical_txlog_root: String,
    batch_cursor_resumes: u64,
}

/// Run G4.4 from a synchronous eval boundary.
///
/// # Errors
///
/// Returns an error when any process, authority, object, stream, or oracle
/// contract fails.
pub fn run_openraft_serving_recovery_contract(
    seed: u64,
    mode: OpenRaftServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    executable: &Path,
) -> Result<OpenRaftServingRecoveryReport, String> {
    run_openraft_serving_recovery_contract_with_page_records(
        seed,
        mode,
        profile,
        PAGE_RECORDS,
        executable,
    )
}

/// Run the same contract with an explicit retained-stream page bound.
///
/// # Errors
///
/// Returns an error when the page bound or any process, authority, object,
/// stream, or oracle contract fails.
pub fn run_openraft_serving_recovery_contract_with_page_records(
    seed: u64,
    mode: OpenRaftServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    max_page_records: u32,
    executable: &Path,
) -> Result<OpenRaftServingRecoveryReport, String> {
    run_openraft_serving_recovery_contract_on_backend(
        seed,
        mode,
        profile,
        max_page_records,
        executable,
        OpenRaftServingObjectBackend::LocalFilesystem,
    )
}

/// Run the recovery contract against one explicitly selected object backend.
///
/// # Errors
///
/// Returns an error when the backend cannot open or any process, authority,
/// object, stream, or oracle contract fails.
pub fn run_openraft_serving_recovery_contract_on_backend(
    seed: u64,
    mode: OpenRaftServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    max_page_records: u32,
    executable: &Path,
    object_backend: OpenRaftServingObjectBackend,
) -> Result<OpenRaftServingRecoveryReport, String> {
    run_openraft_serving_recovery_contract_with_hot_reads(
        seed,
        mode,
        profile,
        max_page_records,
        executable,
        object_backend,
        None,
    )
}

/// Run recovery followed by an optional public-kernel steady-state read window.
///
/// # Errors
///
/// Returns an error when the profile is invalid, recovery fails, a point value
/// is incorrect, or the configured serving path touches object storage after
/// activation.
pub fn run_openraft_serving_recovery_contract_with_hot_reads(
    seed: u64,
    mode: OpenRaftServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    max_page_records: u32,
    executable: &Path,
    object_backend: OpenRaftServingObjectBackend,
    hot_read: Option<OpenRaftHotReadProfile>,
) -> Result<OpenRaftServingRecoveryReport, String> {
    if max_page_records == 0 || max_page_records > 4_096 {
        return Err("retained stream page bound must be in 1..=4096".to_owned());
    }
    if let Some(hot_read) = hot_read.as_ref() {
        validate_hot_read_profile(hot_read, profile)?;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(
        seed,
        mode,
        profile,
        max_page_records,
        executable,
        object_backend,
        hot_read,
    ))
}

/// Run one disposable worker process.
///
/// # Errors
///
/// Returns an error when authoritative recovery or exact reads fail closed.
pub async fn run_openraft_serving_recovery_node(
    config: OpenRaftServingProcessConfig,
) -> Result<OpenRaftServingProcessReport, String> {
    let started = Instant::now();
    let scratch_was_empty = directory_is_empty(&config.scratch_root)?;
    if !scratch_was_empty {
        return Err("serving scratch was not empty at process start".to_owned());
    }
    if matches!(
        config.mode,
        OpenRaftServingRecoveryMode::IntegratedKernelCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
    ) {
        return run_integrated_kernel_node(config, started, scratch_was_empty).await;
    }
    let (mut worker, initial_target, initial_applied) = OpenWorker::open(&config).await?;
    create_barrier(&config.initial_catchup_barrier, "initial_catchup_complete")?;
    wait_for_continue(&config.continue_barrier)?;
    let (activation_target, concurrent_observed, concurrent_applied) =
        worker.catch_up(None, true).await?;
    if config.mode == OpenRaftServingRecoveryMode::FullHydrationControl {
        worker.hydrate_all().await?;
    }
    let mut reads = Vec::with_capacity(config.read_keys.len());
    for key in &config.read_keys {
        reads.push(OpenRaftServingRead {
            key: key.clone(),
            outcome: worker.read(key, activation_target).await?,
        });
    }
    let first_read_seconds = started.elapsed().as_secs_f64();
    let stats = worker.backend.stats();
    let manifest_requests = request_count(&stats, "get")
        .saturating_sub(worker.index_requests)
        .saturating_sub(worker.data_full_requests);
    let list_requests = request_count(&stats, "list");
    let total_object_response_bytes = response_bytes(&stats);
    Ok(OpenRaftServingProcessReport {
        mode: config.mode,
        scratch_was_empty,
        generation_sandwich_stable: true,
        generation: worker.generation,
        logical_txlog_root: worker.logical_txlog_root,
        manifest_authoritative: true,
        object_durable_version: worker.manifest.covered_through,
        initial_target_version: initial_target,
        activation_target_version: activation_target,
        catchup_rounds: 2,
        txlog_read_requests: worker.txlog_read_requests,
        txlog_response_payload_bytes: worker.txlog_response_payload_bytes,
        batch_cursor_resumes: worker.batch_cursor_resumes,
        initial_records_applied: initial_applied,
        concurrent_records_observed: concurrent_observed,
        concurrent_records_applied: concurrent_applied,
        physical_wal_path_accesses: 0,
        manifest_requests,
        index_requests: worker.index_requests,
        data_range_requests: worker.data_range_requests,
        data_full_requests: worker.data_full_requests,
        list_requests,
        total_object_response_bytes,
        row_segment_count: u64::try_from(worker.manifest.segments.len()).unwrap_or(u64::MAX),
        row_index_closure_bytes: worker
            .manifest
            .segments
            .iter()
            .map(|segment| segment.index_bytes)
            .sum(),
        row_data_closure_bytes: worker
            .manifest
            .segments
            .iter()
            .map(|segment| segment.data_bytes)
            .sum(),
        serving_image_provider: None,
        serving_image_records: 0,
        serving_image_local_bytes: 0,
        resident_engine_provider: None,
        resident_engine_records: 0,
        resident_engine_local_bytes: 0,
        resident_engine_applied_version: 0,
        first_read_seconds,
        reads,
        hot_read: None,
    })
}

async fn run_integrated_kernel_node(
    config: OpenRaftServingProcessConfig,
    started: Instant,
    scratch_was_empty: bool,
) -> Result<OpenRaftServingProcessReport, String> {
    let backend = config.object_backend.open(&config.object_store_root)?;
    let serving_image =
        if config.mode == OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate {
            Some(open_rocksdb_serving_image(&config.scratch_root)?)
        } else {
            None
        };
    let resident_engine =
        if config.mode == OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate {
            Some(open_rocksdb_resident_engine(&config.scratch_root)?)
        } else {
            None
        };
    let (mut range, opened) = SingleRange::open(SingleRangeConfig {
        authority_endpoints: config.authority_endpoints,
        transaction_endpoints: config.transaction_endpoints,
        publication_root: config.publication_root,
        object_backend: backend,
        max_page_records: config.max_page_records,
        serving_image,
        resident_engine,
    })
    .await
    .map_err(|error| error.to_string())?;
    create_barrier(&config.initial_catchup_barrier, "initial_catchup_complete")?;
    wait_for_continue(&config.continue_barrier)?;
    let concurrent = range
        .catch_up(None)
        .await
        .map_err(|error| error.to_string())?;
    let mut reads = Vec::with_capacity(config.read_keys.len());
    for key in &config.read_keys {
        let outcome = range
            .get(key, concurrent.target_version)
            .await
            .map_err(|error| error.to_string())?;
        reads.push(OpenRaftServingRead {
            key: key.clone(),
            outcome: match outcome {
                ReadOutcome::Value(value) => value_outcome(&value),
                ReadOutcome::Tombstone => ServingReadOutcome::Tombstone,
                ReadOutcome::Absent => ServingReadOutcome::Absent,
            },
        });
    }
    let first_read_seconds = started.elapsed().as_secs_f64();
    let hot_read = if let Some(profile) = config.hot_read.as_ref() {
        if config.mode == OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate {
            Some(run_native_hot_reads(
                &range,
                concurrent.target_version,
                profile,
            )?)
        } else {
            Some(run_integrated_hot_reads(&mut range, concurrent.target_version, profile).await?)
        }
    } else {
        None
    };
    let stats = range.stats();
    let object_stats = range.object_stats();
    let serving_image_provider = opened
        .serving_image
        .as_ref()
        .map(|receipt| receipt.provider.clone());
    let serving_image_records = opened
        .serving_image
        .as_ref()
        .map_or(0, |receipt| receipt.records);
    let serving_image_local_bytes = opened
        .serving_image
        .as_ref()
        .map_or(0, |receipt| receipt.local_bytes);
    let resident_engine_provider = opened
        .resident_engine
        .as_ref()
        .map(|receipt| receipt.provider.clone());
    let resident_engine_records = stats.resident_engine_records;
    let resident_engine_local_bytes = stats.resident_engine_local_bytes;
    let resident_engine_applied_version = range.coverage().recovered_version;
    Ok(OpenRaftServingProcessReport {
        mode: config.mode,
        scratch_was_empty,
        generation_sandwich_stable: true,
        generation: opened.generation,
        logical_txlog_root: opened.logical_txlog_root,
        manifest_authoritative: true,
        object_durable_version: opened.object_durable_version,
        initial_target_version: opened.recovered_version,
        activation_target_version: concurrent.target_version,
        catchup_rounds: 2,
        txlog_read_requests: stats.txlog_read_requests,
        txlog_response_payload_bytes: stats.txlog_response_payload_bytes,
        batch_cursor_resumes: opened
            .catch_up
            .batch_cursor_resumes
            .saturating_add(concurrent.batch_cursor_resumes),
        initial_records_applied: opened.catch_up.records_applied,
        concurrent_records_observed: concurrent.records_applied,
        concurrent_records_applied: concurrent.records_applied,
        physical_wal_path_accesses: 0,
        manifest_requests: stats.manifest_requests,
        index_requests: stats.index_requests,
        data_range_requests: stats.data_range_requests,
        data_full_requests: stats.data_full_requests,
        list_requests: request_count(&object_stats, "list"),
        total_object_response_bytes: response_bytes(&object_stats),
        row_segment_count: opened.row_segment_count,
        row_index_closure_bytes: opened.row_index_closure_bytes,
        row_data_closure_bytes: opened.row_data_closure_bytes,
        serving_image_provider,
        serving_image_records,
        serving_image_local_bytes,
        resident_engine_provider,
        resident_engine_records,
        resident_engine_local_bytes,
        resident_engine_applied_version,
        first_read_seconds,
        reads,
        hot_read,
    })
}

async fn run_integrated_hot_reads(
    range: &mut SingleRange,
    read_version: u64,
    profile: &OpenRaftHotReadProfile,
) -> Result<OpenRaftHotReadReport, String> {
    let object_requests_before = total_request_count(&range.object_stats());
    let mut correctness_failures = 0_u64;
    for key_id in 16..profile.key_count {
        let actual = range
            .get(&key_bytes(key_id), read_version)
            .await
            .map_err(|error| error.to_string())?;
        if actual != ReadOutcome::Value(base_value(profile.seed, key_id, profile.value_bytes)) {
            correctness_failures = correctness_failures.saturating_add(1);
        }
    }

    let operation_count = profile.warmup_operations.max(profile.measured_operations);
    let keys = hot_operation_keys(profile.key_count, operation_count, profile.seed)
        .into_iter()
        .map(key_bytes)
        .collect::<Vec<_>>();
    let mut checksum = 0_u64;
    for key in keys.iter().cycle().take(profile.warmup_operations) {
        match range
            .get(key, read_version)
            .await
            .map_err(|error| error.to_string())?
        {
            ReadOutcome::Value(value) => {
                checksum = fold_hot_value(checksum, &value);
            }
            ReadOutcome::Tombstone | ReadOutcome::Absent => {
                correctness_failures = correctness_failures.saturating_add(1);
            }
        }
    }

    let measured_started = Instant::now();
    let mut latencies = Vec::with_capacity(profile.measured_operations);
    for key in keys.iter().cycle().take(profile.measured_operations) {
        let read_started = Instant::now();
        match range
            .get(key, read_version)
            .await
            .map_err(|error| error.to_string())?
        {
            ReadOutcome::Value(value) => {
                checksum = fold_hot_value(checksum, &value);
            }
            ReadOutcome::Tombstone | ReadOutcome::Absent => {
                correctness_failures = correctness_failures.saturating_add(1);
            }
        }
        latencies.push(
            read_started
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
        );
    }
    let elapsed_seconds = measured_started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let object_requests =
        total_request_count(&range.object_stats()).saturating_sub(object_requests_before);
    Ok(OpenRaftHotReadReport {
        warmup_operations: u64::try_from(profile.warmup_operations).unwrap_or(u64::MAX),
        measured_operations: u64::try_from(profile.measured_operations).unwrap_or(u64::MAX),
        elapsed_seconds,
        operations_per_second: count_as_f64(
            u64::try_from(profile.measured_operations).unwrap_or(u64::MAX),
        ) / elapsed_seconds,
        latency_ns_p50: percentile(&latencies, 50, 100),
        latency_ns_p95: percentile(&latencies, 95, 100),
        latency_ns_p99: percentile(&latencies, 99, 100),
        latency_ns_p999: percentile(&latencies, 999, 1_000),
        correctness_failures,
        object_requests,
        checksum: std::hint::black_box(checksum),
    })
}

fn run_native_hot_reads(
    range: &SingleRange,
    read_version: u64,
    profile: &OpenRaftHotReadProfile,
) -> Result<OpenRaftHotReadReport, String> {
    let object_requests_before = total_request_count(&range.object_stats());
    let snapshot = range
        .resident_snapshot(read_version)
        .map_err(|error| error.to_string())?;
    let mut correctness_failures = 0_u64;
    for key_id in 16..profile.key_count {
        let actual = snapshot
            .get(&key_bytes(key_id))
            .map_err(|error| error.to_string())?;
        if actual != ReadOutcome::Value(base_value(profile.seed, key_id, profile.value_bytes)) {
            correctness_failures = correctness_failures.saturating_add(1);
        }
    }

    let operation_count = profile.warmup_operations.max(profile.measured_operations);
    let keys = hot_operation_keys(profile.key_count, operation_count, profile.seed)
        .into_iter()
        .map(key_bytes)
        .collect::<Vec<_>>();
    let mut checksum = 0_u64;
    for key in keys.iter().cycle().take(profile.warmup_operations) {
        match snapshot.get(key).map_err(|error| error.to_string())? {
            ReadOutcome::Value(value) => checksum = fold_hot_value(checksum, &value),
            ReadOutcome::Tombstone | ReadOutcome::Absent => {
                correctness_failures = correctness_failures.saturating_add(1);
            }
        }
    }

    let measured_started = Instant::now();
    let mut latencies = Vec::with_capacity(profile.measured_operations);
    for key in keys.iter().cycle().take(profile.measured_operations) {
        let read_started = Instant::now();
        match snapshot.get(key).map_err(|error| error.to_string())? {
            ReadOutcome::Value(value) => checksum = fold_hot_value(checksum, &value),
            ReadOutcome::Tombstone | ReadOutcome::Absent => {
                correctness_failures = correctness_failures.saturating_add(1);
            }
        }
        latencies.push(
            read_started
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
        );
    }
    let elapsed_seconds = measured_started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let object_requests =
        total_request_count(&range.object_stats()).saturating_sub(object_requests_before);
    Ok(OpenRaftHotReadReport {
        warmup_operations: u64::try_from(profile.warmup_operations).unwrap_or(u64::MAX),
        measured_operations: u64::try_from(profile.measured_operations).unwrap_or(u64::MAX),
        elapsed_seconds,
        operations_per_second: count_as_f64(
            u64::try_from(profile.measured_operations).unwrap_or(u64::MAX),
        ) / elapsed_seconds,
        latency_ns_p50: percentile(&latencies, 50, 100),
        latency_ns_p95: percentile(&latencies, 95, 100),
        latency_ns_p99: percentile(&latencies, 99, 100),
        latency_ns_p999: percentile(&latencies, 999, 1_000),
        correctness_failures,
        object_requests,
        checksum: std::hint::black_box(checksum),
    })
}

fn hot_operation_keys(key_count: u64, operations: usize, seed: u64) -> Vec<u64> {
    let available = key_count.saturating_sub(16);
    let hot = (available / 5).max(1);
    let cold = available.saturating_sub(hot);
    let mut random = XorShift64(seed ^ 0x5353_442d_484f_5452);
    (0..operations)
        .map(|_| {
            if random.next() % 100 < 80 || cold == 0 {
                16 + random.next() % hot
            } else {
                16 + hot + random.next() % cold
            }
        })
        .collect()
}

fn fold_hot_value(checksum: u64, value: &[u8]) -> u64 {
    let first = value.first().copied().map_or(0, u64::from);
    let last = value.last().copied().map_or(0, u64::from);
    checksum
        .rotate_left(7)
        .wrapping_add(first)
        .wrapping_add(last << 8)
        .wrapping_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
}

struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}

#[cfg(feature = "resident-rocksdb")]
fn open_rocksdb_serving_image(scratch_root: &Path) -> Result<Box<dyn okv::ServingImage>, String> {
    const MAX_LOCAL_BYTES: u64 = 128 * 1_024 * 1_024;
    let image = okv_serving_rocksdb::RocksDbServingImage::open(
        &scratch_root.join("rocksdb-serving-image"),
        MAX_LOCAL_BYTES,
    )?;
    Ok(Box::new(image))
}

#[cfg(feature = "resident-rocksdb")]
fn open_rocksdb_resident_engine(
    scratch_root: &Path,
) -> Result<Arc<dyn okv::ResidentRangeEngine>, String> {
    const MAX_LOCAL_BYTES: u64 = 128 * 1_024 * 1_024;
    let engine = okv_serving_rocksdb::RocksDbResidentRangeEngine::open(
        &scratch_root.join("rocksdb-native-resident-engine"),
        MAX_LOCAL_BYTES,
    )?;
    Ok(Arc::new(engine))
}

#[cfg(not(feature = "resident-rocksdb"))]
fn open_rocksdb_serving_image(_scratch_root: &Path) -> Result<Box<dyn okv::ServingImage>, String> {
    Err("RocksDB serving image requires okv-eval feature resident-rocksdb".to_owned())
}

#[cfg(not(feature = "resident-rocksdb"))]
fn open_rocksdb_resident_engine(
    _scratch_root: &Path,
) -> Result<Arc<dyn okv::ResidentRangeEngine>, String> {
    Err("RocksDB resident engine requires okv-eval feature resident-rocksdb".to_owned())
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: OpenRaftServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    max_page_records: u32,
    executable: &Path,
    object_backend: OpenRaftServingObjectBackend,
    hot_read: Option<OpenRaftHotReadProfile>,
) -> Result<OpenRaftServingRecoveryReport, String> {
    validate_profile(profile)?;
    let root = TempDir::new().map_err(|error| error.to_string())?;
    let serving_root = serving_scratch_root()?;
    let object_store_root = root.path().join("objects");
    let first_scratch = serving_root.path().join("worker-first");
    let replacement_scratch = serving_root.path().join("worker-replacement");
    fs::create_dir_all(&object_store_root).map_err(|error| error.to_string())?;
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;
    let opened_backend = object_backend.open(&object_store_root)?;

    let publication = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let publication_client = publication.client()?;
    let transaction = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let transaction_client = transaction.client()?;
    let mut next_request_id = 1_u64;
    let (history, object_durable_version) =
        commit_base(seed, profile, &transaction_client, &mut next_request_id).await?;
    let published = publish_row_base(
        seed,
        object_durable_version,
        &history.base_records,
        profile,
        &opened_backend,
        &publication_client,
    )
    .await?;
    let mut read_version = object_durable_version;
    if matches!(
        mode,
        OpenRaftServingRecoveryMode::IntegratedKernelCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelRocksDbCandidate
            | OpenRaftServingRecoveryMode::IntegratedKernelNativeRocksDbCandidate
    ) {
        read_version = commit_command_batch(
            seed,
            &mut next_request_id,
            read_version,
            &history.initial_commands[..2],
            &transaction_client,
        )
        .await?;
        read_version = commit_through_single_range(
            seed,
            next_request_id,
            read_version,
            &history.initial_commands[2],
            &publication,
            &transaction,
            opened_backend.clone(),
            max_page_records,
        )
        .await?;
        next_request_id = next_request_id.saturating_add(1);
    } else {
        for command in &history.initial_commands {
            read_version = commit_command(
                seed,
                next_request_id,
                read_version,
                command,
                &transaction_client,
            )
            .await?;
            next_request_id = next_request_id.saturating_add(1);
        }
    }

    let first_initial = root.path().join("first-initial.json");
    let first_continue = root.path().join("first-continue.json");
    let first_config = OpenRaftServingProcessConfig {
        authority_endpoints: publication.endpoints(),
        transaction_endpoints: transaction.endpoints(),
        publication_root: publication_root(seed),
        object_store_root: object_store_root.clone(),
        object_backend,
        scratch_root: first_scratch,
        read_keys: history.read_keys.clone(),
        mode,
        initial_catchup_barrier: first_initial.clone(),
        continue_barrier: first_continue,
        max_page_records,
        hot_read,
    };
    let mut first = spawn_worker(executable, &first_config, false)?;
    wait_for_barrier(&mut first, &first_initial)?;
    first.kill().map_err(|error| error.to_string())?;
    first.wait().map_err(|error| error.to_string())?;

    let replacement_initial = root.path().join("replacement-initial.json");
    let replacement_continue = root.path().join("replacement-continue.json");
    let replacement_config = OpenRaftServingProcessConfig {
        scratch_root: replacement_scratch,
        initial_catchup_barrier: replacement_initial.clone(),
        continue_barrier: replacement_continue.clone(),
        ..first_config
    };
    let mut replacement = spawn_worker(executable, &replacement_config, true)?;
    wait_for_barrier(&mut replacement, &replacement_initial)?;
    let initial_target = read_version;
    for command in &history.concurrent_commands {
        read_version = commit_command(
            seed,
            next_request_id,
            read_version,
            command,
            &transaction_client,
        )
        .await?;
        next_request_id = next_request_id.saturating_add(1);
    }
    create_barrier(&replacement_continue, "concurrent_commits_complete")?;
    let output = replacement
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "replacement serving process failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let process: OpenRaftServingProcessReport = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode replacement report: {error}"))?;
    if process.object_durable_version != published.object_durable_version
        || process.row_segment_count != published.segment_count
        || process.row_index_closure_bytes != published.index_closure_bytes
        || process.row_data_closure_bytes != published.data_closure_bytes
        || process.initial_target_version != initial_target
        || process.activation_target_version != read_version
        || process.logical_txlog_root != LOGICAL_TXLOG_ROOT
    {
        return Err("replacement report disagrees with authoritative history".to_owned());
    }
    let correctness_anomalies = u64::try_from(
        process
            .reads
            .iter()
            .zip(&history.expected)
            .filter(|(actual, expected)| actual.outcome != **expected)
            .count(),
    )
    .unwrap_or(u64::MAX);
    let exact_replay = correctness_anomalies == 0;
    let stable = serde_json::json!({
        "seed": seed,
        "mode": mode,
        "initial_target": initial_target,
        "activation_target": read_version,
        "reads": process.reads,
        "correctness_anomalies": correctness_anomalies,
        "txlog_read_requests": process.txlog_read_requests,
        "initial_records_applied": process.initial_records_applied,
        "concurrent_records_observed": process.concurrent_records_observed,
        "concurrent_records_applied": process.concurrent_records_applied,
    });
    let semantic_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&stable).map_err(|error| error.to_string())?)
    );
    Ok(OpenRaftServingRecoveryReport {
        seed,
        mode,
        authority_processes: u64::try_from(
            publication.process_count() + transaction.process_count(),
        )
        .unwrap_or(u64::MAX),
        worker_process_starts: 2,
        worker_process_kills: 1,
        empty_scratch_restarts: u64::from(process.scratch_was_empty),
        concurrent_commits: u64::try_from(history.concurrent_commands.len()).unwrap_or(u64::MAX),
        correctness_anomalies,
        exact_replay,
        semantic_sha256,
        process,
    })
}

#[allow(clippy::too_many_arguments)]
async fn commit_through_single_range(
    seed: u64,
    request_id: u64,
    current_version: u64,
    template: &TransactionCommand,
    publication: &PublicationAuthorityProcessFixture,
    transaction: &TransactionAuthorityProcessFixture,
    object_backend: Arc<dyn Backend>,
    max_page_records: u32,
) -> Result<u64, String> {
    let (mut range, opened) = SingleRange::open(SingleRangeConfig {
        authority_endpoints: publication.endpoints(),
        transaction_endpoints: transaction.endpoints(),
        publication_root: publication_root(seed),
        object_backend,
        max_page_records,
        serving_image: None,
        resident_engine: None,
    })
    .await
    .map_err(|error| error.to_string())?;
    if opened.recovered_version != current_version {
        return Err("single-range commit precondition did not recover the exact head".to_owned());
    }
    let mut command = template.clone();
    command.read_version = current_version;
    let committed = range
        .commit(
            RequestIdentity {
                client_id: seed.max(1),
                request_id,
            },
            &command,
        )
        .await
        .map_err(|error| error.to_string())?;
    let TransactionStatus::Committed { commit_version } = committed.response.status else {
        return Err("single-range API transaction did not commit".to_owned());
    };
    if committed.catch_up.as_ref().is_none_or(|receipt| {
        receipt.target_version != commit_version || receipt.records_applied != 1
    }) || range.coverage().recovered_version != commit_version
    {
        return Err("single-range commit did not become locally readable".to_owned());
    }
    Ok(commit_version)
}

impl OpenWorker {
    async fn open(config: &OpenRaftServingProcessConfig) -> Result<(Self, u64, u64), String> {
        let generation_client = GenerationClient::new(config.authority_endpoints.clone())?;
        let publication_client =
            okv_consensus::PublicationClient::new(config.authority_endpoints.clone())?;
        let generation_before = generation_client.read().await?;
        let publication = publication_client.read().await?;
        let generation_after = generation_client.read().await?;
        if generation_before != generation_after
            || generation_before.phase != GenerationPhase::Active
            || generation_before.generation != GENERATION
        {
            return Err("generation changed around the publication-root read".to_owned());
        }
        let logical_txlog_root = generation_before
            .wal_root
            .clone()
            .ok_or_else(|| "active logical txLog root is absent".to_owned())?;
        let manifest_reference = publication
            .roots
            .get(&config.publication_root)
            .ok_or_else(|| "published row root is absent".to_owned())?;
        if manifest_reference.kind != PublicationObjectKind::Manifest {
            return Err("published row root is not a manifest".to_owned());
        }
        let backend = ObservedBackend::new(config.object_backend.open(&config.object_store_root)?);
        let manifest_read = backend
            .get(&manifest_reference.key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let manifest_response_bytes = u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX);
        if manifest_response_bytes != manifest_reference.length
            || content_sha256(&manifest_read.bytes) != manifest_reference.sha256
        {
            return Err("published row manifest identity mismatch".to_owned());
        }
        let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)?;
        if manifest.generation != generation_before.generation {
            return Err("row manifest belongs to another generation".to_owned());
        }
        let txlog = TransactionLogClient::new(config.transaction_endpoints.clone())?;
        let cursor = StreamCursor::after_complete_version(manifest.covered_through);
        let mut worker = Self {
            backend,
            manifest,
            overlay: TailOverlay::default(),
            hydrated: BTreeMap::new(),
            mode: config.mode,
            txlog,
            cursor,
            txlog_read_requests: 0,
            txlog_response_payload_bytes: 0,
            index_requests: 0,
            data_range_requests: 0,
            data_full_requests: 0,
            index_response_bytes: 0,
            data_response_bytes: 0,
            generation: generation_before.generation,
            logical_txlog_root,
            batch_cursor_resumes: 0,
        };
        let (target, observed, applied) = worker.catch_up(None, false).await?;
        if observed == 0 || applied != observed {
            return Err("initial retained transaction suffix is empty or incomplete".to_owned());
        }
        Ok((worker, target, applied))
    }

    async fn catch_up(
        &mut self,
        target: Option<u64>,
        concurrent_round: bool,
    ) -> Result<(u64, u64, u64), String> {
        let mut target_version = target;
        let mut observed = 0_u64;
        let mut applied = 0_u64;
        loop {
            let page = self
                .txlog
                .read(RetainedTransactionReadRequest {
                    after_version_exclusive: self.cursor.commit_version,
                    after_batch_order_exclusive: self.cursor.batch_order,
                    through_version_inclusive: target_version,
                    max_records: PAGE_RECORDS,
                })
                .await?;
            self.txlog_read_requests = self.txlog_read_requests.saturating_add(1);
            self.txlog_response_payload_bytes = self.txlog_response_payload_bytes.saturating_add(
                u64::try_from(
                    serde_json::to_vec(&page)
                        .map_err(|error| error.to_string())?
                        .len(),
                )
                .unwrap_or(u64::MAX),
            );
            target_version = Some(page.target_version);
            for record in &page.records {
                let is_later = record.commit_version > self.cursor.commit_version
                    || (record.commit_version == self.cursor.commit_version
                        && self
                            .cursor
                            .batch_order
                            .is_some_and(|order| record.batch_order > order));
                if !is_later || record.commit_version > page.target_version {
                    return Err("retained transaction page is not strictly ordered".to_owned());
                }
                observed = observed.saturating_add(1);
                if !(concurrent_round
                    && self.mode == OpenRaftServingRecoveryMode::SkipConcurrentCatchupPoison)
                {
                    self.overlay.apply(record);
                    applied = applied.saturating_add(1);
                }
                self.cursor = StreamCursor {
                    commit_version: record.commit_version,
                    batch_order: Some(record.batch_order),
                };
            }
            if page.complete {
                if page.next_after_version != page.target_version
                    || page.next_after_batch_order.is_some()
                {
                    return Err("complete retained transaction cursor is invalid".to_owned());
                }
                self.cursor = StreamCursor::after_complete_version(page.target_version);
                return Ok((page.target_version, observed, applied));
            }
            if page.next_after_version != self.cursor.commit_version
                || page.next_after_batch_order != self.cursor.batch_order
            {
                return Err("retained transaction cursor did not advance with its page".to_owned());
            }
            self.batch_cursor_resumes = self.batch_cursor_resumes.saturating_add(1);
        }
    }

    async fn hydrate_all(&mut self) -> Result<(), String> {
        for reference in &self.manifest.segments {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            let index = RowSegmentIndex::decode(&index_read.bytes)?;
            reference.validate_index(&index_read.bytes, &index)?;
            let data_read = self
                .backend
                .get(&reference.data_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            if u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX) != reference.data_bytes
                || content_sha256(&data_read.bytes) != reference.data_sha256
            {
                return Err("hydrated data object does not match row manifest".to_owned());
            }
            self.index_requests = self.index_requests.saturating_add(1);
            self.data_full_requests = self.data_full_requests.saturating_add(1);
            self.index_response_bytes = self
                .index_response_bytes
                .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
            self.data_response_bytes = self
                .data_response_bytes
                .saturating_add(u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX));
            self.hydrated.insert(
                reference.data_key.clone(),
                (index, data_read.bytes.to_vec()),
            );
        }
        Ok(())
    }

    async fn read(&mut self, key: &[u8], version: u64) -> Result<ServingReadOutcome, String> {
        if version == 0 || version > self.cursor.commit_version {
            return Err("serving read version is outside recovered coverage".to_owned());
        }
        if let Some(outcome) = self.overlay.read(key, version) {
            return Ok(outcome);
        }
        let Some(reference) = self.manifest.locate(key).cloned() else {
            return Ok(ServingReadOutcome::Absent);
        };
        let point = if self.mode == OpenRaftServingRecoveryMode::FullHydrationControl {
            let (index, data) = self
                .hydrated
                .get(&reference.data_key)
                .ok_or_else(|| "selected row object was not hydrated".to_owned())?;
            read_point_from_full_object(data, index, key, self.manifest.covered_through)?
        } else {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            let index = RowSegmentIndex::decode(&index_read.bytes)?;
            reference.validate_index(&index_read.bytes, &index)?;
            self.index_requests = self.index_requests.saturating_add(1);
            self.index_response_bytes = self
                .index_response_bytes
                .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
            let point = read_indexed_point(
                &self.backend,
                &reference.data_key,
                None,
                &index,
                key,
                self.manifest.covered_through,
            )
            .await?;
            if point.data_bytes > 0 {
                self.data_range_requests = self.data_range_requests.saturating_add(1);
                self.data_response_bytes =
                    self.data_response_bytes.saturating_add(point.data_bytes);
            }
            point
        };
        Ok(match point.outcome {
            PointReadOutcome::Value(value) => value_outcome(&value),
            PointReadOutcome::Tombstone => ServingReadOutcome::Tombstone,
            PointReadOutcome::Absent => ServingReadOutcome::Absent,
        })
    }
}

async fn commit_base(
    seed: u64,
    profile: &ServingRecoveryProfile,
    client: &TransactionLogClient,
    next_request_id: &mut u64,
) -> Result<(History, u64), String> {
    let mut read_version = 0_u64;
    let mut base_records = Vec::with_capacity(usize::try_from(profile.key_count).unwrap_or(0));
    for first in (0..profile.key_count).step_by(BASE_BATCH_KEYS) {
        let last = (first + u64::try_from(BASE_BATCH_KEYS).unwrap()).min(profile.key_count);
        let mut mutations = Vec::new();
        let mut conflicts = Vec::new();
        let mut batch_values = Vec::new();
        for key_id in first..last {
            let key = key_bytes(key_id);
            let value = base_value(seed, key_id, profile.value_bytes);
            conflicts.push(KeyRange::point(&key));
            mutations.push(TransactionMutation::Set {
                key: key.clone(),
                value: value.clone(),
            });
            batch_values.push((key, value));
        }
        let command = TransactionCommand {
            read_version,
            read_conflicts: Vec::new(),
            write_conflicts: conflicts,
            mutations,
        };
        let response = client
            .commit(
                RequestIdentity {
                    client_id: seed.max(1),
                    request_id: *next_request_id,
                },
                &command,
            )
            .await?;
        let TransactionStatus::Committed { commit_version } = response.status else {
            return Err("base transaction did not commit".to_owned());
        };
        for (key, value) in batch_values {
            base_records.push(RowRecord::value(key, commit_version, value));
        }
        read_version = commit_version;
        *next_request_id = next_request_id.saturating_add(1);
    }
    let mut history = history_after_base(seed, profile, read_version, base_records);
    let expected = evaluate_expected(&history, profile)?;
    history.expected = expected;
    Ok((history, read_version))
}

fn history_after_base(
    seed: u64,
    profile: &ServingRecoveryProfile,
    base_version: u64,
    base_records: Vec<RowRecord>,
) -> History {
    let key_count = profile.key_count;
    let initial_insert = key_bytes(key_count + 1);
    let concurrent_insert = key_bytes(key_count + 2);
    let initial_commands = vec![
        point_command(
            base_version,
            2,
            TransactionMutation::Set {
                key: key_bytes(2),
                value: tail_value(seed, b"initial-update", profile.value_bytes),
            },
        ),
        point_command(0, 3, TransactionMutation::Clear { key: key_bytes(3) }),
        point_command(
            0,
            key_count + 1,
            TransactionMutation::Set {
                key: initial_insert.clone(),
                value: tail_value(seed, b"initial-insert", profile.value_bytes),
            },
        ),
    ];
    let range = KeyRange {
        start: key_bytes(10),
        end: key_bytes(14),
    };
    let concurrent_commands = vec![
        point_command(
            0,
            4,
            TransactionMutation::Set {
                key: key_bytes(4),
                value: tail_value(seed, b"concurrent-update", profile.value_bytes),
            },
        ),
        point_command(0, 5, TransactionMutation::Clear { key: key_bytes(5) }),
        point_command(
            0,
            key_count + 2,
            TransactionMutation::Set {
                key: concurrent_insert.clone(),
                value: tail_value(seed, b"concurrent-insert", profile.value_bytes),
            },
        ),
        TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: vec![range.clone()],
            mutations: vec![TransactionMutation::ClearRange { range }],
        },
    ];
    History {
        base_records,
        initial_commands,
        concurrent_commands,
        read_keys: vec![
            key_bytes(1),
            key_bytes(2),
            key_bytes(3),
            initial_insert,
            key_bytes(4),
            key_bytes(5),
            concurrent_insert,
            key_bytes(11),
        ],
        expected: Vec::new(),
    }
}

fn point_command(
    read_version: u64,
    key_id: u64,
    mutation: TransactionMutation,
) -> TransactionCommand {
    let range = KeyRange::point(&key_bytes(key_id));
    TransactionCommand {
        read_version,
        read_conflicts: Vec::new(),
        write_conflicts: vec![range],
        mutations: vec![mutation],
    }
}

async fn commit_command(
    seed: u64,
    request_id: u64,
    current_version: u64,
    template: &TransactionCommand,
    client: &TransactionLogClient,
) -> Result<u64, String> {
    let mut command = template.clone();
    command.read_version = current_version;
    let response = client
        .commit(
            RequestIdentity {
                client_id: seed.max(1),
                request_id,
            },
            &command,
        )
        .await?;
    match response.status {
        TransactionStatus::Committed { commit_version } => Ok(commit_version),
        status => Err(format!("history transaction did not commit: {status:?}")),
    }
}

async fn commit_command_batch(
    seed: u64,
    next_request_id: &mut u64,
    current_version: u64,
    templates: &[TransactionCommand],
    client: &TransactionLogClient,
) -> Result<u64, String> {
    let mut items = Vec::with_capacity(templates.len());
    for template in templates {
        let mut command = template.clone();
        command.read_version = current_version;
        items.push(TransactionBatchItem {
            identity: RequestIdentity {
                client_id: seed.max(1),
                request_id: *next_request_id,
            },
            credential: None,
            command,
        });
        *next_request_id = next_request_id.saturating_add(1);
    }
    let response = client.commit_batch(&items).await?;
    if response.items.len() != items.len() {
        return Err("initial transaction batch returned the wrong item count".to_owned());
    }
    let mut commit_version = None;
    for (expected_order, item) in response.items.iter().enumerate() {
        let transaction = item
            .transaction
            .as_ref()
            .ok_or_else(|| "initial transaction batch item has no outcome".to_owned())?;
        let TransactionStatus::Committed {
            commit_version: item_version,
        } = transaction.status
        else {
            return Err("initial transaction batch item did not commit".to_owned());
        };
        if transaction.batch_order != u16::try_from(expected_order).unwrap_or(u16::MAX)
            || commit_version.is_some_and(|version| version != item_version)
        {
            return Err(
                "initial transaction batch did not share one ordered commit version".to_owned(),
            );
        }
        commit_version = Some(item_version);
    }
    commit_version.ok_or_else(|| "initial transaction batch was empty".to_owned())
}

fn evaluate_expected(
    history: &History,
    _profile: &ServingRecoveryProfile,
) -> Result<Vec<ServingReadOutcome>, String> {
    let mut state = BTreeMap::new();
    for record in &history.base_records {
        let Some(value) = record.value.clone() else {
            return Err("base record is not a value".to_owned());
        };
        state.insert(record.key.clone(), value);
    }
    for command in history
        .initial_commands
        .iter()
        .chain(&history.concurrent_commands)
    {
        for mutation in &command.mutations {
            match mutation {
                TransactionMutation::Set { key, value } => {
                    state.insert(key.clone(), value.clone());
                }
                TransactionMutation::Clear { key } => {
                    state.remove(key);
                }
                TransactionMutation::ClearRange { range } => {
                    let keys = state
                        .range(range.start.clone()..range.end.clone())
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in keys {
                        state.remove(&key);
                    }
                }
            }
        }
    }
    Ok(history
        .read_keys
        .iter()
        .map(|key| {
            state
                .get(key)
                .map_or(ServingReadOutcome::Tombstone, |value| value_outcome(value))
        })
        .collect())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn publish_row_base(
    seed: u64,
    object_durable_version: u64,
    records: &[RowRecord],
    profile: &ServingRecoveryProfile,
    backend: &Arc<dyn Backend>,
    client: &okv_consensus::PublicationClient,
) -> Result<PublishedRowBase, String> {
    let encoded = encode_row_object_set(
        GENERATION,
        records,
        profile.target_object_bytes,
        profile.target_block_bytes,
    )?;
    let references = encoded
        .iter()
        .map(|segment| RowObjectReference::from_encoded("rows-g44", segment))
        .collect::<Result<Vec<_>, _>>()?;
    let manifest =
        RowObjectManifestV1::new(GENERATION, object_durable_version, references.clone())?;
    let manifest_bytes = manifest.encode()?;
    let manifest_reference = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: format!(
            "rows-g44/manifest/sha256/{}",
            content_sha256(&manifest_bytes)
        ),
        length: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
        sha256: content_sha256(&manifest_bytes),
    };
    let mut object_keys = BTreeSet::from([manifest_reference.key.clone()]);
    for reference in &references {
        object_keys.insert(reference.data_key.clone());
        object_keys.insert(reference.index_key.clone());
    }
    let publication_id = format!("serving-recovery-openraft-{seed}");
    let prepared = client
        .commit(&publication_command(
            seed,
            10_000,
            PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent: PublicationIntent {
                    object_keys,
                    manifest: manifest_reference.clone(),
                    destination_root: publication_root(seed),
                    expected_prior_root: None,
                },
            },
        ))
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err("row-base publication prepare was rejected".to_owned());
    }
    for (segment, reference) in encoded.iter().zip(&references) {
        backend
            .put(
                &reference.data_key,
                segment.data.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
        backend
            .put(
                &reference.index_key,
                segment.index.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    backend
        .put(
            &manifest_reference.key,
            manifest_bytes.into(),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    let published = client
        .commit(&publication_command(
            seed,
            10_001,
            PublicationAction::Publish {
                publication_id,
                destination_root: publication_root(seed),
                expected_prior_root: None,
                manifest: manifest_reference.clone(),
            },
        ))
        .await?;
    if published.status != PublicationCommandStatus::Accepted
        || published.state.roots.get(&publication_root(seed)) != Some(&manifest_reference)
    {
        return Err("row-base publication did not install the exact root".to_owned());
    }
    Ok(PublishedRowBase {
        object_durable_version,
        segment_count: u64::try_from(references.len()).unwrap_or(u64::MAX),
        index_closure_bytes: references
            .iter()
            .map(|reference| reference.index_bytes)
            .sum(),
        data_closure_bytes: references
            .iter()
            .map(|reference| reference.data_bytes)
            .sum(),
    })
}

fn publication_command(
    seed: u64,
    request_id: u64,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed.max(1).saturating_add(1_000_000),
            request_id,
        },
        credential: okv_consensus::GenerationCredential {
            generation: GENERATION,
            transaction_system_id: "tx-g7".to_owned(),
        },
        action,
    }
}

fn publication_root(seed: u64) -> String {
    format!("serving-recovery-openraft/root/{seed}")
}

fn spawn_worker(
    executable: &Path,
    config: &OpenRaftServingProcessConfig,
    capture_output: bool,
) -> Result<std::process::Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let mut command = Command::new(executable);
    command
        .arg("serving-recovery-open-raft-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null());
    if capture_output {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
    } else {
        command.stdout(Stdio::null()).stderr(Stdio::piped());
    }
    command
        .spawn()
        .map_err(|error| format!("start serving worker: {error}"))
}

fn wait_for_barrier(child: &mut std::process::Child, path: &Path) -> Result<(), String> {
    for _ in 0..1_000 {
        if path.is_file() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take() {
                let _ = stream.read_to_string(&mut stderr);
            }
            return Err(format!(
                "serving worker exited before catch-up barrier: {status}: {}",
                stderr.trim()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err("serving worker did not reach catch-up barrier".to_owned())
}

fn wait_for_continue(path: &Path) -> Result<(), String> {
    for _ in 0..1_000 {
        if path.is_file() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err("serving worker did not receive concurrent-commit barrier".to_owned())
}

fn create_barrier(path: &Path, state: &str) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    writeln!(file, "{{\"schema_version\":1,\"state\":\"{state}\"}}")
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn directory_is_empty(path: &Path) -> Result<bool, String> {
    if !path.is_dir() {
        return Err(format!(
            "scratch directory does not exist: {}",
            path.display()
        ));
    }
    Ok(fs::read_dir(path)
        .map_err(|error| error.to_string())?
        .next()
        .is_none())
}

fn validate_profile(profile: &ServingRecoveryProfile) -> Result<(), String> {
    if profile.key_count < 32
        || profile.value_bytes < 16
        || profile.target_block_bytes < 4_096
        || profile.target_object_bytes < profile.target_block_bytes
    {
        return Err("invalid OpenRaft serving recovery profile".to_owned());
    }
    Ok(())
}

fn serving_scratch_root() -> Result<TempDir, String> {
    match std::env::var_os("OKV_EVAL_SERVING_SCRATCH_ROOT") {
        Some(root) => {
            let root = PathBuf::from(root);
            fs::create_dir_all(&root).map_err(|error| {
                format!(
                    "create configured serving scratch root {}: {error}",
                    root.display()
                )
            })?;
            TempDirBuilder::new()
                .prefix("okv-single-range-")
                .tempdir_in(&root)
                .map_err(|error| {
                    format!("create serving scratch below {}: {error}", root.display())
                })
        }
        None => TempDir::new().map_err(|error| error.to_string()),
    }
}

fn validate_hot_read_profile(
    hot_read: &OpenRaftHotReadProfile,
    recovery: &ServingRecoveryProfile,
) -> Result<(), String> {
    if hot_read.seed == 0
        || hot_read.key_count != recovery.key_count
        || hot_read.value_bytes != recovery.value_bytes
        || hot_read.key_count <= 16
        || hot_read.warmup_operations == 0
        || hot_read.measured_operations == 0
    {
        return Err("invalid OpenRaft public-kernel hot-read profile".to_owned());
    }
    Ok(())
}

fn key_bytes(key_id: u64) -> Vec<u8> {
    key_id.to_be_bytes().to_vec()
}

fn base_value(seed: u64, key_id: u64, length: usize) -> Vec<u8> {
    deterministic_value(seed ^ key_id.rotate_left(17), b"base", length)
}

fn tail_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    deterministic_value(seed ^ 0x5441_494c_5641_4c55, domain, length)
}

fn deterministic_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut counter = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"OKV-SERVING-OPENRAFT-RECOVERY-V1\0");
        hasher.update(seed.to_be_bytes());
        hasher.update(domain);
        hasher.update(counter.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length - value.len();
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        counter = counter.saturating_add(1);
    }
    value
}

fn value_outcome(value: &[u8]) -> ServingReadOutcome {
    ServingReadOutcome::Value {
        length: u64::try_from(value.len()).unwrap_or(u64::MAX),
        sha256: format!("{:x}", Sha256::digest(value)),
    }
}

fn request_count(stats: &okv_object::RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.api == api)
        .map(|request| request.count)
        .sum()
}

fn response_bytes(stats: &okv_object::RequestStats) -> u64 {
    stats
        .requests
        .iter()
        .map(|request| request.response_bytes)
        .sum()
}

fn total_request_count(stats: &okv_object::RequestStats) -> u64 {
    stats.requests.iter().map(|request| request.count).sum()
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> u64 {
    let index = (values.len() - 1)
        .saturating_mul(numerator)
        .div_ceil(denominator);
    values[index]
}

#[allow(clippy::cast_precision_loss)]
fn count_as_f64(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::{key_bytes, OpenRaftServingRecoveryMode, TailOverlay};
    use crate::serving_recovery::ServingReadOutcome;
    use okv_consensus::{RetainedTransactionRecord, TransactionMutation};
    use okv_transaction::{KeyRange, TransactionCommand};

    #[test]
    fn range_clear_orders_against_point_updates() {
        let mut overlay = TailOverlay::default();
        overlay.apply(&RetainedTransactionRecord {
            commit_version: 7,
            batch_order: 0,
            command: TransactionCommand {
                read_version: 0,
                read_conflicts: Vec::new(),
                write_conflicts: vec![KeyRange {
                    start: key_bytes(10),
                    end: key_bytes(14),
                }],
                mutations: vec![TransactionMutation::ClearRange {
                    range: KeyRange {
                        start: key_bytes(10),
                        end: key_bytes(14),
                    },
                }],
            },
        });
        assert_eq!(
            overlay.read(&key_bytes(11), 7),
            Some(ServingReadOutcome::Tombstone)
        );
        assert_eq!(OpenRaftServingRecoveryMode::Candidate.id(), "candidate");
    }
}
