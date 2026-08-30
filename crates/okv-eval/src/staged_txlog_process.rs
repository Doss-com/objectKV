//! One-host, three-process mechanism gate for the staged transaction log.
//!
//! This module proves the frozen L1 process contract. It is not a performance
//! benchmark and does not model independent machine or failure domains.

use okv_model::{CellTraceAssertionV1, CellTraceConfigV1, CellTraceEventV1, CellTraceRefinementV1};
use okv_wal::{
    StagedAppendOutcome, StagedLogError, StagedLogIdentity, StagedLogNode, StagedLogRecord,
    StagedRequestIdentity,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

const NODE_COUNT: usize = 3;
const WRITE_QUORUM: usize = 2;
const INITIAL_WRITER_EPOCH: u64 = 7;
const REPLACEMENT_WRITER_EPOCH: u64 = 8;
const JOURNAL_FILE_NAME: &str = "txlog.journal";
const PHYSICAL_BOUND_BYTES: u64 = 65_536;
const MAX_WIRE_BYTES: usize = 64 * 1_024 * 1_024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const START_ATTEMPTS: usize = 200;
const START_RETRY_DELAY: Duration = Duration::from_millis(10);
const MACHINE_PREFLIGHT_MAX_RECORDS: u64 = 65_536;
const MACHINE_PREFLIGHT_MAX_BATCH_RECORDS: usize = 4_096;
const MACHINE_PREFLIGHT_MAX_PAYLOAD_BYTES: usize = 4_096;
const MACHINE_CURVE_MAX_RECORDS: u64 = 1_048_576;
const MACHINE_CURVE_MAX_CLIENT_TASKS: usize = 256;
const MACHINE_CURVE_MAX_STREAMS: usize = 4_096;
const MACHINE_CURVE_MAX_QUEUE_RECORDS: usize = 1_048_576;
const MACHINE_CURVE_MAX_DWELL_MICROS: u64 = 100_000;
const MACHINE_CURVE_MAX_OFFERED_RECORDS_PER_SECOND: f64 = 2_000_000.0;

/// Fault mode exercised by the unchanged L1 process oracle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedTxLogProcessMode {
    Correct,
    AckBeforeSync,
    AcceptStaleEpoch,
    NodeSpecificSegmentBytes,
}

impl StagedTxLogProcessMode {
    /// Stable receipt identifier for this process mode.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AckBeforeSync => "ack_before_sync",
            Self::AcceptStaleEpoch => "accept_stale_epoch",
            Self::NodeSpecificSegmentBytes => "node_specific_segment_bytes",
        }
    }
}

/// Configuration passed to one internal staged-log child process.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StagedTxLogNodeConfig {
    pub node_id: u8,
    pub listen_addr: String,
    pub root: PathBuf,
    pub log_identity: StagedLogIdentity,
    pub mode: StagedTxLogProcessMode,
}

/// One diagnostic quorum-append timing from the one-host L1 gate.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogAppendSample {
    pub position: u64,
    pub payload_bytes: u64,
    pub durable_acknowledgements: u64,
    pub acknowledged_nodes: Vec<u8>,
    pub stable_nodes_observed: Vec<u8>,
    pub quorum_duration_seconds: f64,
}

/// Complete evidence emitted by one L1 process-contract seed.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogProcessReport {
    pub schema_version: u32,
    pub seed: u64,
    pub mode: String,
    pub node_count: u64,
    pub write_quorum: u64,
    pub process_starts: u64,
    pub process_kills: u64,
    pub initial_process_ids: Vec<u32>,
    pub restart_process_ids: Vec<u32>,
    pub distinct_roots: u64,
    pub distinct_listeners: u64,
    pub append_samples: Vec<StagedTxLogAppendSample>,
    pub acknowledged_appends: u64,
    pub exact_retry_no_physical_effect: bool,
    pub physical_bytes_before_retry: Vec<u64>,
    pub physical_bytes_after_retry: Vec<u64>,
    pub recovered_record_counts: Vec<u64>,
    pub exact_prefix_nodes: u64,
    pub acknowledged_record_loss: u64,
    pub consecutive_recovery: bool,
    pub torn_tail_repairs: u64,
    pub stale_writer_rejections: u64,
    pub stale_writer_mutations: u64,
    pub segment_digests: Vec<String>,
    pub segment_bytes: Vec<u64>,
    pub segment_bytes_identical: bool,
    pub object_operations: u64,
    pub max_node_physical_bytes: u64,
    pub physical_bound_bytes: u64,
    pub bounded_physical_bytes: bool,
    pub network_append_requests: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub cell_trace_refinement: CellTraceRefinementV1,
    pub trace_sha256: String,
}

/// One independently addressed log node in the L2 machine preflight.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StagedTxLogMachineNodeConfig {
    pub node_id: u8,
    pub machine_id: String,
    pub endpoint: String,
}

/// Frozen, bounded input for the first independent-machine mechanism run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StagedTxLogMachinePreflightConfig {
    pub schema_version: u32,
    pub seed: u64,
    pub writer_epoch: u64,
    pub log_identity: StagedLogIdentity,
    pub nodes: Vec<StagedTxLogMachineNodeConfig>,
    pub record_bytes: usize,
    pub record_count: u64,
    pub batch_records: usize,
}

/// One node identity observed over the real machine network.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogMachineNodeObservation {
    pub node_id: u8,
    pub machine_id: String,
    pub endpoint: String,
    pub process_id: u32,
    pub root: String,
    pub listener: String,
    pub final_physical_bytes: u64,
    pub final_record_count: u64,
}

/// Diagnostic result for the bounded L2 independent-machine preflight.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogMachinePreflightReport {
    pub schema_version: u32,
    pub scope: String,
    pub seed: u64,
    pub writer_epoch: u64,
    pub log_identity: StagedLogIdentity,
    pub nodes: Vec<StagedTxLogMachineNodeObservation>,
    pub record_bytes: u64,
    pub requested_records: u64,
    pub acknowledged_records: u64,
    pub batch_records: u64,
    pub batch_count: u64,
    pub network_batch_requests: u64,
    pub measurement_seconds: f64,
    pub records_per_second: f64,
    pub batch_ack_seconds: Vec<f64>,
    pub batch_ack_p50_seconds: f64,
    pub batch_ack_p95_seconds: f64,
    pub batch_ack_p99_seconds: f64,
    pub exact_state_nodes: u64,
    pub object_operations: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub report_sha256: String,
}

/// Frozen input for one open-loop L2 staged-log curve point.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StagedTxLogMachineCurveConfig {
    pub schema_version: u32,
    pub seed: u64,
    pub writer_epoch: u64,
    pub log_identity: StagedLogIdentity,
    pub nodes: Vec<StagedTxLogMachineNodeConfig>,
    pub record_bytes: usize,
    pub record_count: u64,
    pub max_batch_records: usize,
    pub max_batch_dwell_micros: u64,
    pub offered_records_per_second: f64,
    pub client_tasks: usize,
    pub stream_count: usize,
    pub queue_capacity_records: usize,
    pub node_queue_capacity_batches: usize,
}

/// Final digest and physical state observed from one L2 log node.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogMachineCurveNodeObservation {
    pub node_id: u8,
    pub machine_id: String,
    pub endpoint: String,
    pub process_id: u32,
    pub root: String,
    pub listener: String,
    pub final_physical_bytes: u64,
    pub final_record_count: u64,
    pub final_records_sha256: String,
}

/// One physical batch in the open-loop L2 staged-log curve.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogMachineBatchSample {
    pub batch_id: u64,
    pub record_count: u64,
    pub first_position: u64,
    pub last_position: u64,
    pub queue_depth_before_dispatch: u64,
    pub oldest_queue_dwell_seconds: f64,
    pub quorum_duration_seconds: f64,
}

/// Result for one bounded open-loop L2 staged-log curve point.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StagedTxLogMachineCurveReport {
    pub schema_version: u32,
    pub scope: String,
    pub seed: u64,
    pub writer_epoch: u64,
    pub log_identity: StagedLogIdentity,
    pub nodes: Vec<StagedTxLogMachineCurveNodeObservation>,
    pub record_bytes: u64,
    pub requested_records: u64,
    pub enqueued_records: u64,
    pub refused_records: u64,
    pub acknowledged_records: u64,
    pub offered_records_per_second: f64,
    pub realized_offered_records_per_second: f64,
    pub acknowledged_records_per_second: f64,
    pub client_tasks: u64,
    pub stream_count: u64,
    pub queue_capacity_records: u64,
    pub max_queue_depth_records: u64,
    pub max_batch_records: u64,
    pub max_batch_dwell_micros: u64,
    pub batch_count: u64,
    pub mean_batch_records: f64,
    pub max_observed_batch_records: u64,
    pub network_batch_requests: u64,
    pub producer_seconds: f64,
    pub measurement_seconds: f64,
    pub record_ack_p50_seconds: f64,
    pub record_ack_p95_seconds: f64,
    pub record_ack_p99_seconds: f64,
    pub record_ack_p999_seconds: f64,
    pub queue_dwell_p50_seconds: f64,
    pub queue_dwell_p95_seconds: f64,
    pub queue_dwell_p99_seconds: f64,
    pub queue_dwell_p999_seconds: f64,
    pub quorum_p50_seconds: f64,
    pub quorum_p95_seconds: f64,
    pub quorum_p99_seconds: f64,
    pub quorum_p999_seconds: f64,
    pub batch_samples: Vec<StagedTxLogMachineBatchSample>,
    pub expected_records_sha256: String,
    pub exact_state_nodes: u64,
    pub object_operations: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub report_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WireRecord {
    writer_epoch: u64,
    position: u64,
    request_identity: StagedRequestIdentity,
    payload: Vec<u8>,
}

impl From<StagedLogRecord> for WireRecord {
    fn from(record: StagedLogRecord) -> Self {
        Self {
            writer_epoch: record.writer_epoch,
            position: record.position,
            request_identity: record.request_identity,
            payload: record.payload,
        }
    }
}

impl From<WireRecord> for StagedLogRecord {
    fn from(record: WireRecord) -> Self {
        Self {
            writer_epoch: record.writer_epoch,
            position: record.position,
            request_identity: record.request_identity,
            payload: record.payload,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum NodeRequest {
    Health,
    InstallEpoch {
        writer_epoch: u64,
    },
    Append {
        writer_epoch: u64,
        position: u64,
        request_identity: StagedRequestIdentity,
        payload: Vec<u8>,
    },
    AppendBatch {
        records: Vec<WireRecord>,
    },
    State,
    StateDigest,
    Segment {
        first_position: u64,
        last_position: u64,
        committed_through: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum NodeResponse {
    Health {
        node_id: u8,
        process_id: u32,
        root: String,
        listener: String,
        log_identity: StagedLogIdentity,
        writer_epoch: Option<u64>,
        next_position: u64,
        record_count: u64,
        physical_bytes: u64,
        recovered_torn_tail: bool,
    },
    Epoch {
        writer_epoch: u64,
        physical_bytes: u64,
        replayed: bool,
        synchronized: bool,
    },
    Append {
        position: u64,
        frame_bytes: u64,
        physical_bytes: u64,
        replayed: bool,
        synchronized: bool,
    },
    AppendBatch {
        first_position: u64,
        last_position: u64,
        record_count: u64,
        new_record_count: u64,
        replayed_record_count: u64,
        frame_bytes: u64,
        physical_bytes: u64,
        synchronized: bool,
    },
    State {
        writer_epoch: Option<u64>,
        next_position: u64,
        physical_bytes: u64,
        recovered_torn_tail: bool,
        records: Vec<WireRecord>,
    },
    StateDigest {
        writer_epoch: Option<u64>,
        next_position: u64,
        physical_bytes: u64,
        recovered_torn_tail: bool,
        record_count: u64,
        records_sha256: String,
    },
    Segment {
        bytes: Vec<u8>,
    },
    Error {
        code: String,
        message: String,
    },
}

struct NodeRuntime {
    config: StagedTxLogNodeConfig,
    listener: String,
    node: StagedLogNode,
    volatile_records: BTreeMap<u64, WireRecord>,
}

impl NodeRuntime {
    fn open(config: StagedTxLogNodeConfig, listener: String) -> Result<Self, String> {
        let node = StagedLogNode::open(&config.root, config.log_identity)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            config,
            listener,
            node,
            volatile_records: BTreeMap::new(),
        })
    }

    fn handle(&mut self, request: NodeRequest) -> NodeResponse {
        match request {
            NodeRequest::Health => NodeResponse::Health {
                node_id: self.config.node_id,
                process_id: std::process::id(),
                root: self.config.root.display().to_string(),
                listener: self.listener.clone(),
                log_identity: self.config.log_identity,
                writer_epoch: self.node.writer_epoch(),
                next_position: self.node.next_position(),
                record_count: bounded_u64(self.node.records().len()),
                physical_bytes: self.node.physical_bytes().unwrap_or(u64::MAX),
                recovered_torn_tail: self.node.recovered_torn_tail(),
            },
            NodeRequest::InstallEpoch { writer_epoch } => self
                .node
                .install_writer_epoch(writer_epoch)
                .map_or_else(error_response, |outcome| NodeResponse::Epoch {
                    writer_epoch: outcome.writer_epoch,
                    physical_bytes: outcome.physical_bytes,
                    replayed: outcome.replayed,
                    synchronized: outcome.synchronized,
                }),
            NodeRequest::Append {
                writer_epoch,
                position,
                request_identity,
                payload,
            } => self.append(writer_epoch, position, request_identity, &payload),
            NodeRequest::AppendBatch { records } => self.append_batch(records),
            NodeRequest::State => NodeResponse::State {
                writer_epoch: self.node.writer_epoch(),
                next_position: self.node.next_position(),
                physical_bytes: self.node.physical_bytes().unwrap_or(u64::MAX),
                recovered_torn_tail: self.node.recovered_torn_tail(),
                records: self
                    .node
                    .records()
                    .into_iter()
                    .map(WireRecord::from)
                    .collect(),
            },
            NodeRequest::StateDigest => {
                let records = self
                    .node
                    .records()
                    .into_iter()
                    .map(WireRecord::from)
                    .collect::<Vec<_>>();
                NodeResponse::StateDigest {
                    writer_epoch: self.node.writer_epoch(),
                    next_position: self.node.next_position(),
                    physical_bytes: self.node.physical_bytes().unwrap_or(u64::MAX),
                    recovered_torn_tail: self.node.recovered_torn_tail(),
                    record_count: bounded_u64(records.len()),
                    records_sha256: wire_records_digest(records.iter()),
                }
            }
            NodeRequest::Segment {
                first_position,
                last_position,
                committed_through,
            } => self
                .node
                .encode_segment(first_position, last_position, committed_through)
                .map_or_else(error_response, |mut bytes| {
                    if self.config.mode == StagedTxLogProcessMode::NodeSpecificSegmentBytes {
                        bytes.push(self.config.node_id);
                    }
                    NodeResponse::Segment { bytes }
                }),
        }
    }

    fn append(
        &mut self,
        writer_epoch: u64,
        position: u64,
        request_identity: StagedRequestIdentity,
        payload: &[u8],
    ) -> NodeResponse {
        if self.config.mode == StagedTxLogProcessMode::AckBeforeSync {
            return self.append_without_sync(writer_epoch, position, request_identity, payload);
        }

        let actual_epoch = if self.config.mode == StagedTxLogProcessMode::AcceptStaleEpoch
            && self
                .node
                .writer_epoch()
                .is_some_and(|current| writer_epoch < current)
        {
            self.node.writer_epoch().unwrap_or(writer_epoch)
        } else {
            writer_epoch
        };
        self.node
            .append(actual_epoch, position, request_identity, payload)
            .map_or_else(error_response, append_response)
    }

    fn append_without_sync(
        &mut self,
        writer_epoch: u64,
        position: u64,
        request_identity: StagedRequestIdentity,
        payload: &[u8],
    ) -> NodeResponse {
        let Some(current) = self.node.writer_epoch() else {
            return error_response(StagedLogError::WriterNotOpen);
        };
        if writer_epoch != current {
            return error_response(if writer_epoch < current {
                StagedLogError::StaleWriter {
                    current,
                    proposed: writer_epoch,
                }
            } else {
                StagedLogError::WriterEpochMismatch {
                    current,
                    proposed: writer_epoch,
                }
            });
        }
        if let Some(existing) = self.volatile_records.get(&position) {
            return if existing.request_identity == request_identity && existing.payload == payload {
                NodeResponse::Append {
                    position,
                    frame_bytes: 0,
                    physical_bytes: self.node.physical_bytes().unwrap_or(u64::MAX),
                    replayed: true,
                    synchronized: true,
                }
            } else {
                error_response(StagedLogError::ConflictingRetry(position))
            };
        }
        let expected = self.volatile_records.last_key_value().map_or_else(
            || self.node.next_position(),
            |(last, _)| last.saturating_add(1),
        );
        if position != expected {
            return error_response(StagedLogError::NonConsecutive {
                expected,
                actual: position,
            });
        }
        self.volatile_records.insert(
            position,
            WireRecord {
                writer_epoch,
                position,
                request_identity,
                payload: payload.to_vec(),
            },
        );
        NodeResponse::Append {
            position,
            frame_bytes: 0,
            physical_bytes: self.node.physical_bytes().unwrap_or(u64::MAX),
            replayed: false,
            synchronized: true,
        }
    }

    fn append_batch(&mut self, records: Vec<WireRecord>) -> NodeResponse {
        if records.is_empty() {
            return NodeResponse::Error {
                code: "empty_batch".to_owned(),
                message: "staged txLog append batch is empty".to_owned(),
            };
        }
        let records = records
            .into_iter()
            .map(StagedLogRecord::from)
            .collect::<Vec<_>>();
        let first_position = records.first().map_or(0, |record| record.position);
        let last_position = records.last().map_or(0, |record| record.position);
        self.node
            .append_batch(&records)
            .map_or_else(error_response, |outcomes| {
                let new_record_count =
                    bounded_u64(outcomes.iter().filter(|outcome| !outcome.replayed).count());
                let replayed_record_count =
                    bounded_u64(outcomes.iter().filter(|outcome| outcome.replayed).count());
                NodeResponse::AppendBatch {
                    first_position,
                    last_position,
                    record_count: bounded_u64(outcomes.len()),
                    new_record_count,
                    replayed_record_count,
                    frame_bytes: outcomes.iter().map(|outcome| outcome.frame_bytes).sum(),
                    physical_bytes: outcomes.last().map_or(0, |outcome| outcome.physical_bytes),
                    synchronized: outcomes.iter().all(|outcome| outcome.synchronized),
                }
            })
    }
}

fn append_response(outcome: StagedAppendOutcome) -> NodeResponse {
    NodeResponse::Append {
        position: outcome.position,
        frame_bytes: outcome.frame_bytes,
        physical_bytes: outcome.physical_bytes,
        replayed: outcome.replayed,
        synchronized: outcome.synchronized,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn error_response(error: StagedLogError) -> NodeResponse {
    let message = error.to_string();
    let code = match error {
        StagedLogError::StaleWriter { .. } => "stale_writer",
        StagedLogError::ConflictingRetry(_) => "conflicting_retry",
        StagedLogError::NonConsecutive { .. } => "non_consecutive",
        StagedLogError::WriterEpochMismatch { .. } => "writer_epoch_mismatch",
        StagedLogError::WriterNotOpen => "writer_not_open",
        _ => "storage_error",
    };
    NodeResponse::Error {
        code: code.to_owned(),
        message,
    }
}

/// Run one staged-log child process until the controller terminates it.
///
/// # Errors
///
/// Returns an error when the listener, node journal, or wire protocol fails.
pub fn run_staged_txlog_node(config: StagedTxLogNodeConfig) -> Result<(), String> {
    let listener = TcpListener::bind(&config.listen_addr).map_err(|error| error.to_string())?;
    let local_addr = listener.local_addr().map_err(|error| error.to_string())?;
    let mut runtime = NodeRuntime::open(config, local_addr.to_string())?;
    for incoming in listener.incoming() {
        let mut stream = incoming.map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|error| error.to_string())?;
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|error| error.to_string())?;
        stream
            .set_nodelay(true)
            .map_err(|error| error.to_string())?;
        while let Some(request) = read_wire_optional::<NodeRequest>(&mut stream)? {
            let response = runtime.handle(request);
            write_wire(&mut stream, &response)?;
        }
    }
    Ok(())
}

struct RunningNode {
    node_id: u8,
    endpoint: String,
    child: Child,
}

impl RunningNode {
    fn stop(&mut self) -> bool {
        match self.child.try_wait() {
            Ok(Some(_)) | Err(_) => false,
            Ok(None) => {
                let killed = self.child.kill().is_ok();
                let _ = self.child.wait();
                killed
            }
        }
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Clone, Debug)]
struct HealthObservation {
    node_id: u8,
    process_id: u32,
    root: String,
    listener: String,
    log_identity: StagedLogIdentity,
    recovered_torn_tail: bool,
}

#[derive(Clone, Debug)]
struct StateObservation {
    node_id: u8,
    physical_bytes: u64,
    recovered_torn_tail: bool,
    records: Vec<WireRecord>,
}

struct ParallelAppend {
    responses: Vec<(u8, Result<NodeResponse, String>)>,
    quorum_duration_seconds: f64,
    durable_acknowledgements: u64,
}

/// Run the complete one-host L1 process contract for one immutable seed.
///
/// # Errors
///
/// Returns an infrastructure error when the child topology cannot execute.
#[allow(clippy::too_many_lines)]
pub fn run_staged_txlog_process_contract(
    seed: u64,
    mode: StagedTxLogProcessMode,
    executable: &Path,
) -> Result<StagedTxLogProcessReport, String> {
    let temp = tempfile::Builder::new()
        .prefix("okv-staged-txlog-l1-")
        .tempdir()
        .map_err(|error| error.to_string())?;
    let log_identity = identity(&[b"okv-staged-txlog-l1", &seed.to_be_bytes()]);
    let mut configs = Vec::with_capacity(NODE_COUNT);
    for node_index in 0..NODE_COUNT {
        let node_id = u8::try_from(node_index).map_err(|error| error.to_string())?;
        configs.push(StagedTxLogNodeConfig {
            node_id,
            listen_addr: reserve_loopback_address()?,
            root: temp.path().join(format!("node-{node_id}")),
            log_identity,
            mode,
        });
    }

    let distinct_roots = bounded_u64(
        configs
            .iter()
            .map(|config| config.root.clone())
            .collect::<BTreeSet<_>>()
            .len(),
    );
    let distinct_listeners = bounded_u64(
        configs
            .iter()
            .map(|config| config.listen_addr.clone())
            .collect::<BTreeSet<_>>()
            .len(),
    );

    let mut nodes = start_nodes(executable, &configs)?;
    let initial_health = wait_for_health(&mut nodes)?;
    let initial_process_ids = initial_health
        .iter()
        .map(|health| health.process_id)
        .collect::<Vec<_>>();
    install_epoch(&nodes, INITIAL_WRITER_EPOCH)?;

    let mut expected_initial = Vec::new();
    let mut append_samples = Vec::new();
    let mut acknowledged_appends = 0_u64;
    let mut network_append_requests = 0_u64;
    let payload_sizes = [128_usize, 1_024, 4_096];
    for (index, payload_size) in payload_sizes.into_iter().enumerate() {
        let position = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let payload = deterministic_payload(seed, position, payload_size);
        let request_identity = request_identity(seed, position, &payload);
        let result = append_parallel(
            &nodes,
            INITIAL_WRITER_EPOCH,
            position,
            request_identity,
            &payload,
        );
        network_append_requests = network_append_requests.saturating_add(bounded_u64(NODE_COUNT));
        if result.durable_acknowledgements >= bounded_u64(WRITE_QUORUM) {
            acknowledged_appends = acknowledged_appends.saturating_add(1);
        }
        append_samples.push(StagedTxLogAppendSample {
            position,
            payload_bytes: bounded_u64(payload_size),
            durable_acknowledgements: result.durable_acknowledgements,
            acknowledged_nodes: result.acknowledged_nodes(),
            stable_nodes_observed: Vec::new(),
            quorum_duration_seconds: result.quorum_duration_seconds,
        });
        expected_initial.push(WireRecord {
            writer_epoch: INITIAL_WRITER_EPOCH,
            position,
            request_identity,
            payload,
        });
    }

    let physical_bytes_before_retry = read_states(&nodes)?
        .into_iter()
        .map(|state| state.physical_bytes)
        .collect::<Vec<_>>();
    let retry = expected_initial
        .get(1)
        .ok_or_else(|| "missing retry record".to_owned())?;
    let retry_result = append_parallel(
        &nodes,
        retry.writer_epoch,
        retry.position,
        retry.request_identity,
        &retry.payload,
    );
    network_append_requests = network_append_requests.saturating_add(bounded_u64(NODE_COUNT));
    let physical_bytes_after_retry = read_states(&nodes)?
        .into_iter()
        .map(|state| state.physical_bytes)
        .collect::<Vec<_>>();
    let exact_retry_no_physical_effect = retry_result.durable_acknowledgements
        >= bounded_u64(WRITE_QUORUM)
        && physical_bytes_before_retry == physical_bytes_after_retry;

    let mut process_kills = stop_nodes(&mut nodes);
    inject_torn_tail(&configs[0].root)?;

    let mut nodes = start_nodes(executable, &configs)?;
    let restart_health = wait_for_health(&mut nodes)?;
    let restart_process_ids = restart_health
        .iter()
        .map(|health| health.process_id)
        .collect::<Vec<_>>();
    let torn_tail_repairs = bounded_u64(
        restart_health
            .iter()
            .filter(|health| health.recovered_torn_tail)
            .count(),
    );
    install_epoch(&nodes, REPLACEMENT_WRITER_EPOCH)?;

    let recovered_states = read_states(&nodes)?;
    let recovered_record_counts = recovered_states
        .iter()
        .map(|state| bounded_u64(state.records.len()))
        .collect::<Vec<_>>();
    let exact_prefix_nodes = bounded_u64(
        recovered_states
            .iter()
            .filter(|state| state.records == expected_initial)
            .count(),
    );
    let consecutive_recovery = exact_prefix_nodes >= bounded_u64(WRITE_QUORUM);
    let acknowledged_record_loss = if consecutive_recovery {
        0
    } else {
        bounded_u64(expected_initial.len())
    };

    let final_position = 4;
    let final_payload = deterministic_payload(seed, final_position, 64);
    let final_request_identity = request_identity(seed, final_position, &final_payload);
    let stale_result = append_parallel(
        &nodes,
        INITIAL_WRITER_EPOCH,
        final_position,
        final_request_identity,
        &final_payload,
    );
    network_append_requests = network_append_requests.saturating_add(bounded_u64(NODE_COUNT));
    let stale_writer_rejections = bounded_u64(
        stale_result
            .responses
            .iter()
            .filter(|(_, response)| {
                matches!(
                    response,
                    Ok(NodeResponse::Error { code, .. }) if code == "stale_writer"
                )
            })
            .count(),
    );
    let stale_writer_mutations = stale_result.durable_acknowledgements;

    let mut expected_complete = expected_initial.clone();
    if consecutive_recovery {
        let final_result = append_parallel(
            &nodes,
            REPLACEMENT_WRITER_EPOCH,
            final_position,
            final_request_identity,
            &final_payload,
        );
        network_append_requests = network_append_requests.saturating_add(bounded_u64(NODE_COUNT));
        if final_result.durable_acknowledgements >= bounded_u64(WRITE_QUORUM) {
            acknowledged_appends = acknowledged_appends.saturating_add(1);
        }
        append_samples.push(StagedTxLogAppendSample {
            position: final_position,
            payload_bytes: bounded_u64(final_payload.len()),
            durable_acknowledgements: final_result.durable_acknowledgements,
            acknowledged_nodes: final_result.acknowledged_nodes(),
            stable_nodes_observed: Vec::new(),
            quorum_duration_seconds: final_result.quorum_duration_seconds,
        });
        expected_complete.push(WireRecord {
            writer_epoch: REPLACEMENT_WRITER_EPOCH,
            position: final_position,
            request_identity: final_request_identity,
            payload: final_payload,
        });
    }

    let final_states = read_states(&nodes)?;
    for sample in &mut append_samples {
        sample.stable_nodes_observed = final_states
            .iter()
            .filter(|state| {
                state
                    .records
                    .iter()
                    .any(|record| record.position == sample.position)
            })
            .map(|state| state.node_id)
            .collect();
    }
    let complete_node_count = final_states
        .iter()
        .filter(|state| state.records == expected_complete)
        .count();
    let segment_responses = request_all(
        &nodes,
        &NodeRequest::Segment {
            first_position: 1,
            last_position: final_position,
            committed_through: final_position,
        },
    );
    let segments = segment_responses
        .into_iter()
        .filter_map(|(_, response)| match response {
            Ok(NodeResponse::Segment { bytes }) => Some(bytes),
            _ => None,
        })
        .collect::<Vec<_>>();
    let segment_digests = segments
        .iter()
        .map(|bytes| hex_digest(bytes))
        .collect::<Vec<_>>();
    let segment_bytes = segments
        .iter()
        .map(|bytes| bounded_u64(bytes.len()))
        .collect::<Vec<_>>();
    let segment_bytes_identical = segments.len() == complete_node_count
        && complete_node_count == NODE_COUNT
        && segments
            .first()
            .is_some_and(|first| segments.iter().all(|candidate| candidate == first));

    let max_node_physical_bytes = final_states
        .iter()
        .map(|state| state.physical_bytes)
        .max()
        .unwrap_or(u64::MAX);
    let bounded_physical_bytes = max_node_physical_bytes <= PHYSICAL_BOUND_BYTES;
    let object_operations = 0_u64;
    process_kills = process_kills.saturating_add(stop_nodes(&mut nodes));

    let distinct_initial_processes = initial_process_ids.iter().collect::<BTreeSet<_>>().len();
    let distinct_restart_processes = restart_process_ids.iter().collect::<BTreeSet<_>>().len();
    let health_identity_matches = health_matches_configs(&initial_health, &configs)
        && health_matches_configs(&restart_health, &configs);
    let durable_quorum = append_samples
        .iter()
        .all(|sample| sample.durable_acknowledgements >= bounded_u64(WRITE_QUORUM));
    let epoch_installs_synchronized = true;
    let recovery_state_reported = recovered_states
        .iter()
        .all(|state| state.recovered_torn_tail || state.records == expected_initial);
    let checks = [
        (
            "three distinct process roots and TCP listeners were exercised",
            distinct_roots == bounded_u64(NODE_COUNT)
                && distinct_listeners == bounded_u64(NODE_COUNT)
                && distinct_initial_processes == NODE_COUNT
                && distinct_restart_processes == NODE_COUNT
                && health_identity_matches,
        ),
        (
            "writer epochs installed before synchronized append",
            epoch_installs_synchronized,
        ),
        (
            "every accepted append reached a two-node synchronized quorum",
            durable_quorum && acknowledged_appends >= 3,
        ),
        (
            "exact retry changed no node journal bytes",
            exact_retry_no_physical_effect,
        ),
        (
            "acknowledged records recovered as one exact consecutive quorum prefix",
            consecutive_recovery && acknowledged_record_loss == 0 && recovery_state_reported,
        ),
        (
            "exactly one incomplete final frame was repaired before append",
            torn_tail_repairs == 1,
        ),
        (
            "the prior writer epoch mutated no node after restart",
            stale_writer_rejections == bounded_u64(NODE_COUNT) && stale_writer_mutations == 0,
        ),
        (
            "all complete nodes constructed byte-identical segment previews",
            segment_bytes_identical,
        ),
        ("no object operation occurred in L1", object_operations == 0),
        (
            "every node journal remained below the frozen physical bound",
            bounded_physical_bytes,
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find_map(|(detail, passed)| (!passed).then(|| (*detail).to_owned()));
    let anomaly_count = u64::from(first_mismatch.is_some());

    let cell_trace_refinement = staged_txlog_cell_trace(&append_samples)?;
    let mut report = StagedTxLogProcessReport {
        schema_version: 2,
        seed,
        mode: mode.id().to_owned(),
        node_count: bounded_u64(NODE_COUNT),
        write_quorum: bounded_u64(WRITE_QUORUM),
        process_starts: bounded_u64(NODE_COUNT.saturating_mul(2)),
        process_kills,
        initial_process_ids,
        restart_process_ids,
        distinct_roots,
        distinct_listeners,
        append_samples,
        acknowledged_appends,
        exact_retry_no_physical_effect,
        physical_bytes_before_retry,
        physical_bytes_after_retry,
        recovered_record_counts,
        exact_prefix_nodes,
        acknowledged_record_loss,
        consecutive_recovery,
        torn_tail_repairs,
        stale_writer_rejections,
        stale_writer_mutations,
        segment_digests,
        segment_bytes,
        segment_bytes_identical,
        object_operations,
        max_node_physical_bytes,
        physical_bound_bytes: PHYSICAL_BOUND_BYTES,
        bounded_physical_bytes,
        network_append_requests,
        executed_checks: bounded_u64(checks.len()),
        anomaly_count,
        first_mismatch,
        cell_trace_refinement,
        trace_sha256: String::new(),
    };
    report.trace_sha256 = hex_digest(
        &serde_json::to_vec(&report).map_err(|error| format!("encode process report: {error}"))?,
    );
    Ok(report)
}

struct ConnectedNode {
    node_id: u8,
    stream: TcpStream,
}

struct ParallelBatchAppend {
    responses: Vec<(u8, Result<NodeResponse, String>)>,
    quorum_duration_seconds: f64,
    durable_acknowledgements: u64,
}

/// Run one bounded client-only preflight against three already-running log
/// nodes on independently named machines.
///
/// This is a mechanism and batch-geometry diagnostic. It is not the frozen L2
/// open-loop curve.
///
/// # Errors
///
/// Returns an error when the topology, connection, or epoch installation is
/// invalid. Workload mismatches are retained in the returned report.
#[allow(clippy::too_many_lines)]
pub fn run_staged_txlog_machine_preflight(
    config: &StagedTxLogMachinePreflightConfig,
) -> Result<StagedTxLogMachinePreflightReport, String> {
    validate_machine_preflight_config(config)?;
    let mut health = Vec::with_capacity(config.nodes.len());
    for node in &config.nodes {
        match request_node(&node.endpoint, &NodeRequest::Health)? {
            NodeResponse::Health {
                node_id,
                process_id,
                root,
                listener,
                log_identity,
                ..
            } if node_id == node.node_id && log_identity == config.log_identity => {
                health.push(HealthObservation {
                    node_id,
                    process_id,
                    root,
                    listener,
                    log_identity,
                    recovered_torn_tail: false,
                });
            }
            other => {
                return Err(format!(
                    "machine {} returned unexpected health response: {other:?}",
                    node.machine_id
                ));
            }
        }
    }

    let mut nodes = config
        .nodes
        .iter()
        .map(ConnectedNode::connect)
        .collect::<Result<Vec<_>, _>>()?;
    for (node_id, response) in request_all_connected(
        &mut nodes,
        &NodeRequest::InstallEpoch {
            writer_epoch: config.writer_epoch,
        },
    ) {
        match response? {
            NodeResponse::Epoch {
                writer_epoch,
                synchronized: true,
                ..
            } if writer_epoch == config.writer_epoch => {}
            other => {
                return Err(format!(
                    "node {node_id} did not install epoch {}: {other:?}",
                    config.writer_epoch
                ));
            }
        }
    }

    let mut expected = Vec::with_capacity(
        usize::try_from(config.record_count).map_err(|error| error.to_string())?,
    );
    let mut acknowledged_records = 0_u64;
    let mut network_batch_requests = 0_u64;
    let mut batch_ack_seconds = Vec::new();
    let measured = Instant::now();
    let mut next_position = 1_u64;
    while next_position <= config.record_count {
        let remaining = config
            .record_count
            .saturating_sub(next_position)
            .saturating_add(1);
        let batch_len = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(config.batch_records);
        let mut batch = Vec::with_capacity(batch_len);
        for offset in 0..batch_len {
            let position = next_position.saturating_add(bounded_u64(offset));
            let payload = deterministic_payload(config.seed, position, config.record_bytes);
            batch.push(WireRecord {
                writer_epoch: config.writer_epoch,
                position,
                request_identity: request_identity(config.seed, position, &payload),
                payload,
            });
        }
        let result = append_batch_parallel_connected(&mut nodes, &batch);
        network_batch_requests =
            network_batch_requests.saturating_add(bounded_u64(config.nodes.len()));
        batch_ack_seconds.push(result.quorum_duration_seconds);
        let first_position = batch.first().map_or(0, |record| record.position);
        let last_position = batch.last().map_or(0, |record| record.position);
        let response_exact = result.responses.iter().all(|(_, response)| {
            matches!(
                response,
                Ok(NodeResponse::AppendBatch {
                    first_position: observed_first,
                    last_position: observed_last,
                    record_count,
                    synchronized: true,
                    ..
                }) if *observed_first == first_position
                    && *observed_last == last_position
                    && *record_count == bounded_u64(batch.len())
            )
        });
        if result.durable_acknowledgements < bounded_u64(WRITE_QUORUM) || !response_exact {
            break;
        }
        acknowledged_records = acknowledged_records.saturating_add(bounded_u64(batch.len()));
        expected.extend(batch);
        next_position = next_position.saturating_add(bounded_u64(batch_len));
    }
    let measurement_seconds = measured.elapsed().as_secs_f64();

    let state_responses = request_all_connected(&mut nodes, &NodeRequest::State);
    let mut exact_state_nodes = 0_u64;
    let mut physical_by_node = BTreeMap::new();
    let mut records_by_node = BTreeMap::new();
    for (node_id, response) in state_responses {
        if let Ok(NodeResponse::State {
            physical_bytes,
            records,
            ..
        }) = response
        {
            if records == expected {
                exact_state_nodes = exact_state_nodes.saturating_add(1);
            }
            physical_by_node.insert(node_id, physical_bytes);
            records_by_node.insert(node_id, bounded_u64(records.len()));
        }
    }

    let node_observations = config
        .nodes
        .iter()
        .filter_map(|node| {
            health
                .iter()
                .find(|observation| observation.node_id == node.node_id)
                .map(|observation| StagedTxLogMachineNodeObservation {
                    node_id: node.node_id,
                    machine_id: node.machine_id.clone(),
                    endpoint: node.endpoint.clone(),
                    process_id: observation.process_id,
                    root: observation.root.clone(),
                    listener: observation.listener.clone(),
                    final_physical_bytes: physical_by_node
                        .get(&node.node_id)
                        .copied()
                        .unwrap_or(u64::MAX),
                    final_record_count: records_by_node.get(&node.node_id).copied().unwrap_or(0),
                })
        })
        .collect::<Vec<_>>();
    let checks = [
        (
            "every requested record reached a stable quorum",
            acknowledged_records == config.record_count,
        ),
        (
            "every node reconstructed the exact requested history",
            exact_state_nodes == bounded_u64(NODE_COUNT),
        ),
        (
            "every configured machine returned one exact node identity",
            node_observations.len() == NODE_COUNT,
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find_map(|(detail, passed)| (!passed).then(|| (*detail).to_owned()));
    let anomaly_count = u64::from(first_mismatch.is_some());
    let mut sorted_ack_seconds = batch_ack_seconds.clone();
    sorted_ack_seconds.sort_by(f64::total_cmp);
    let records_per_second = if measurement_seconds > 0.0 {
        f64::from(u32::try_from(acknowledged_records).unwrap_or(u32::MAX)) / measurement_seconds
    } else {
        0.0
    };
    let mut report = StagedTxLogMachinePreflightReport {
        schema_version: 1,
        scope: "staged-txlog-l2-batched-persistent-preflight".to_owned(),
        seed: config.seed,
        writer_epoch: config.writer_epoch,
        log_identity: config.log_identity,
        nodes: node_observations,
        record_bytes: bounded_u64(config.record_bytes),
        requested_records: config.record_count,
        acknowledged_records,
        batch_records: bounded_u64(config.batch_records),
        batch_count: bounded_u64(batch_ack_seconds.len()),
        network_batch_requests,
        measurement_seconds,
        records_per_second,
        batch_ack_p50_seconds: percentile(&sorted_ack_seconds, 50),
        batch_ack_p95_seconds: percentile(&sorted_ack_seconds, 95),
        batch_ack_p99_seconds: percentile(&sorted_ack_seconds, 99),
        batch_ack_seconds,
        exact_state_nodes,
        object_operations: 0,
        anomaly_count,
        first_mismatch,
        report_sha256: String::new(),
    };
    report.report_sha256 = hex_digest(
        &serde_json::to_vec(&report).map_err(|error| format!("encode report: {error}"))?,
    );
    Ok(report)
}

struct CurveArrival {
    ordinal: u64,
    enqueued_at: Instant,
    payload: Vec<u8>,
}

#[derive(Clone)]
struct NodeBatchWork {
    batch_id: u64,
    records: Arc<Vec<WireRecord>>,
}

struct NodeBatchReply {
    batch_id: u64,
    node_id: u8,
    response: Result<NodeResponse, String>,
}

struct BatchProgress {
    first_position: u64,
    last_position: u64,
    record_count: u64,
    response_count: u64,
    durable_acknowledgements: u64,
    responding_nodes: BTreeSet<u8>,
}

struct DeterministicPoisson {
    state: u64,
}

impl DeterministicPoisson {
    const fn new(seed: u64) -> Self {
        Self { state: seed | 1 }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.state = value;
        value.wrapping_mul(2_685_821_657_736_338_717)
    }

    fn exponential_delay(&mut self, rate_per_second: f64) -> Duration {
        let high = u32::try_from(self.next_u64() >> 32).unwrap_or(u32::MAX);
        let unit = (f64::from(high) + 1.0) / (f64::from(u32::MAX) + 2.0);
        Duration::from_secs_f64(-unit.ln() / rate_per_second)
    }
}

/// Run one bounded open-loop curve point against three already-running log
/// nodes on independently named machines.
///
/// Producer threads submit Poisson arrivals to one bounded active-writer queue.
/// The writer closes a batch at the record or dwell limit, sends it over one
/// persistent connection per node, and acknowledges after an exact stable
/// quorum. All node responses are drained before final state digests are read.
///
/// # Errors
///
/// Returns an error when the topology, connection, epoch, queue, or wire path
/// cannot complete the frozen point.
#[allow(clippy::too_many_lines)]
pub fn run_staged_txlog_machine_curve(
    config: &StagedTxLogMachineCurveConfig,
) -> Result<StagedTxLogMachineCurveReport, String> {
    validate_machine_curve_config(config)?;
    let health = machine_health(&config.nodes, config.log_identity)?;
    let mut nodes = config
        .nodes
        .iter()
        .map(ConnectedNode::connect)
        .collect::<Result<Vec<_>, _>>()?;
    install_machine_epoch(&mut nodes, config.writer_epoch)?;

    let (node_reply_sender, node_reply_receiver) = mpsc::channel::<NodeBatchReply>();
    let mut node_senders = Vec::with_capacity(nodes.len());
    let mut node_handles = Vec::with_capacity(nodes.len());
    for mut node in nodes {
        let (sender, receiver) =
            mpsc::sync_channel::<NodeBatchWork>(config.node_queue_capacity_batches);
        let reply_sender = node_reply_sender.clone();
        node_senders.push(sender);
        node_handles.push(thread::spawn(move || {
            for work in receiver {
                let response = node.request(&NodeRequest::AppendBatch {
                    records: work.records.as_ref().clone(),
                });
                let _ = reply_sender.send(NodeBatchReply {
                    batch_id: work.batch_id,
                    node_id: node.node_id,
                    response,
                });
            }
            node
        }));
    }
    drop(node_reply_sender);

    let (arrival_sender, arrival_receiver) =
        mpsc::sync_channel::<CurveArrival>(config.queue_capacity_records);
    let attempted_records = Arc::new(AtomicU64::new(0));
    let enqueued_records = Arc::new(AtomicU64::new(0));
    let refused_records = Arc::new(AtomicU64::new(0));
    let queued_records = Arc::new(AtomicU64::new(0));
    let max_queue_depth = Arc::new(AtomicU64::new(0));
    let producer_finished_nanos = Arc::new(AtomicU64::new(0));
    let start_at = Instant::now() + Duration::from_millis(250);
    let mut producer_handles = Vec::with_capacity(config.client_tasks);
    for task_index in 0..config.client_tasks {
        let sender = arrival_sender.clone();
        let attempted = Arc::clone(&attempted_records);
        let enqueued = Arc::clone(&enqueued_records);
        let refused = Arc::clone(&refused_records);
        let queued = Arc::clone(&queued_records);
        let max_depth = Arc::clone(&max_queue_depth);
        let producer_finished = Arc::clone(&producer_finished_nanos);
        let seed = config.seed;
        let record_count = config.record_count;
        let record_bytes = config.record_bytes;
        let task_count = config.client_tasks;
        let stream_count = config.stream_count;
        let queue_capacity_records = config.queue_capacity_records;
        let offered_per_task = config.offered_records_per_second
            / f64::from(u32::try_from(task_count).unwrap_or(u32::MAX));
        producer_handles.push(thread::spawn(move || {
            let mut rng = DeterministicPoisson::new(
                seed ^ bounded_u64(task_index).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            );
            let mut target = start_at;
            let mut ordinal = bounded_u64(task_index);
            while ordinal < record_count {
                target += rng.exponential_delay(offered_per_task);
                if let Some(delay) = target.checked_duration_since(Instant::now()) {
                    thread::sleep(delay);
                }
                attempted.fetch_add(1, Ordering::Relaxed);
                let stream_id = ordinal % bounded_u64(stream_count);
                let payload = deterministic_payload(seed ^ stream_id, ordinal + 1, record_bytes);
                let depth = queued.fetch_add(1, Ordering::AcqRel).saturating_add(1);
                match sender.try_send(CurveArrival {
                    ordinal,
                    enqueued_at: Instant::now(),
                    payload,
                }) {
                    Ok(()) => {
                        enqueued.fetch_add(1, Ordering::Relaxed);
                        update_atomic_max(
                            &max_depth,
                            depth.min(bounded_u64(queue_capacity_records)),
                        );
                    }
                    Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) => {
                        queued.fetch_sub(1, Ordering::AcqRel);
                        refused.fetch_add(1, Ordering::Relaxed);
                    }
                }
                ordinal = ordinal.saturating_add(bounded_u64(task_count));
            }
            update_atomic_max(&producer_finished, duration_nanos_u64(start_at.elapsed()));
        }));
    }
    drop(arrival_sender);

    let dwell_limit = Duration::from_micros(config.max_batch_dwell_micros);
    let mut next_position = 1_u64;
    let mut next_batch_id = 1_u64;
    let mut acknowledged_records = 0_u64;
    let mut network_batch_requests = 0_u64;
    let mut received_node_responses = 0_u64;
    let mut node_response_anomalies = 0_u64;
    let mut progress = BTreeMap::<u64, BatchProgress>::new();
    let mut record_ack_seconds = Vec::new();
    let mut queue_dwell_seconds = Vec::new();
    let mut quorum_seconds = Vec::new();
    let mut batch_samples = Vec::new();
    let mut expected_hasher = Sha256::new();

    while let Ok(first) = arrival_receiver.recv() {
        queued_records.fetch_sub(1, Ordering::AcqRel);
        let deadline = first.enqueued_at + dwell_limit;
        let mut arrivals = vec![first];
        while arrivals.len() < config.max_batch_records {
            match arrival_receiver.try_recv() {
                Ok(arrival) => {
                    queued_records.fetch_sub(1, Ordering::AcqRel);
                    arrivals.push(arrival);
                }
                Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        break;
                    };
                    match arrival_receiver.recv_timeout(remaining) {
                        Ok(arrival) => {
                            queued_records.fetch_sub(1, Ordering::AcqRel);
                            arrivals.push(arrival);
                        }
                        Err(
                            mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected,
                        ) => {
                            break;
                        }
                    }
                }
            }
        }

        let dispatch_at = Instant::now();
        let first_position = next_position;
        let mut records = Vec::with_capacity(arrivals.len());
        for (offset, arrival) in arrivals.iter().enumerate() {
            let position = next_position.saturating_add(bounded_u64(offset));
            let identity_seed = config.seed ^ arrival.ordinal.rotate_left(17);
            records.push(WireRecord {
                writer_epoch: config.writer_epoch,
                position,
                request_identity: request_identity(identity_seed, position, &arrival.payload),
                payload: arrival.payload.clone(),
            });
            queue_dwell_seconds.push(
                dispatch_at
                    .saturating_duration_since(arrival.enqueued_at)
                    .as_secs_f64(),
            );
        }
        let last_position = records
            .last()
            .map_or(first_position, |record| record.position);
        for record in &records {
            update_wire_record_digest(&mut expected_hasher, record);
        }
        let records = Arc::new(records);
        progress.insert(
            next_batch_id,
            BatchProgress {
                first_position,
                last_position,
                record_count: bounded_u64(records.len()),
                response_count: 0,
                durable_acknowledgements: 0,
                responding_nodes: BTreeSet::new(),
            },
        );
        for sender in &node_senders {
            sender
                .send(NodeBatchWork {
                    batch_id: next_batch_id,
                    records: Arc::clone(&records),
                })
                .map_err(|_| "staged txLog node worker queue disconnected".to_owned())?;
            network_batch_requests = network_batch_requests.saturating_add(1);
        }

        let quorum_started = Instant::now();
        loop {
            let reply = node_reply_receiver
                .recv_timeout(IO_TIMEOUT + IO_TIMEOUT)
                .map_err(|error| format!("wait for staged txLog quorum: {error}"))?;
            received_node_responses = received_node_responses.saturating_add(1);
            let batch_progress = progress
                .get_mut(&reply.batch_id)
                .ok_or_else(|| format!("response for unknown batch {}", reply.batch_id))?;
            let unique_node = batch_progress.responding_nodes.insert(reply.node_id);
            let exact = unique_node
                && matches!(
                    reply.response,
                    Ok(NodeResponse::AppendBatch {
                        first_position: observed_first,
                        last_position: observed_last,
                        record_count,
                        synchronized: true,
                        ..
                    }) if observed_first == batch_progress.first_position
                        && observed_last == batch_progress.last_position
                        && record_count == batch_progress.record_count
                );
            batch_progress.response_count = batch_progress.response_count.saturating_add(1);
            if exact {
                batch_progress.durable_acknowledgements =
                    batch_progress.durable_acknowledgements.saturating_add(1);
            } else {
                node_response_anomalies = node_response_anomalies.saturating_add(1);
            }
            if reply.batch_id == next_batch_id
                && batch_progress.durable_acknowledgements >= bounded_u64(WRITE_QUORUM)
            {
                break;
            }
            if reply.batch_id == next_batch_id
                && batch_progress.response_count == bounded_u64(NODE_COUNT)
            {
                return Err(format!(
                    "batch {next_batch_id} did not reach an exact stable quorum"
                ));
            }
        }
        let acknowledged_at = Instant::now();
        let quorum_duration_seconds = quorum_started.elapsed().as_secs_f64();
        quorum_seconds.push(quorum_duration_seconds);
        for arrival in &arrivals {
            record_ack_seconds.push(
                acknowledged_at
                    .saturating_duration_since(arrival.enqueued_at)
                    .as_secs_f64(),
            );
        }
        batch_samples.push(StagedTxLogMachineBatchSample {
            batch_id: next_batch_id,
            record_count: bounded_u64(arrivals.len()),
            first_position,
            last_position,
            queue_depth_before_dispatch: queued_records.load(Ordering::Acquire),
            oldest_queue_dwell_seconds: arrivals
                .iter()
                .map(|arrival| {
                    dispatch_at
                        .saturating_duration_since(arrival.enqueued_at)
                        .as_secs_f64()
                })
                .fold(0.0, f64::max),
            quorum_duration_seconds,
        });
        acknowledged_records = acknowledged_records.saturating_add(bounded_u64(arrivals.len()));
        next_position = next_position.saturating_add(bounded_u64(arrivals.len()));
        next_batch_id = next_batch_id.saturating_add(1);
    }

    for handle in producer_handles {
        handle
            .join()
            .map_err(|_| "staged txLog curve producer panicked".to_owned())?;
    }
    while received_node_responses < network_batch_requests {
        let reply = node_reply_receiver
            .recv_timeout(IO_TIMEOUT + IO_TIMEOUT)
            .map_err(|error| format!("drain staged txLog node responses: {error}"))?;
        received_node_responses = received_node_responses.saturating_add(1);
        let batch_progress = progress
            .get_mut(&reply.batch_id)
            .ok_or_else(|| format!("response for unknown batch {}", reply.batch_id))?;
        let unique_node = batch_progress.responding_nodes.insert(reply.node_id);
        batch_progress.response_count = batch_progress.response_count.saturating_add(1);
        if unique_node
            && matches!(
                reply.response,
                Ok(NodeResponse::AppendBatch {
                    first_position,
                    last_position,
                    record_count,
                    synchronized: true,
                    ..
                }) if first_position == batch_progress.first_position
                    && last_position == batch_progress.last_position
                    && record_count == batch_progress.record_count
            )
        {
            batch_progress.durable_acknowledgements =
                batch_progress.durable_acknowledgements.saturating_add(1);
        } else {
            node_response_anomalies = node_response_anomalies.saturating_add(1);
        }
    }
    drop(node_senders);
    let mut nodes = node_handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| "staged txLog node connection worker panicked".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    nodes.sort_by_key(|node| node.node_id);

    let expected_records_sha256 = format!("{:x}", expected_hasher.finalize());
    let state_responses = request_all_connected(&mut nodes, &NodeRequest::StateDigest);
    let mut exact_state_nodes = 0_u64;
    let mut node_observations = Vec::new();
    for node in &config.nodes {
        let Some(observation) = health
            .iter()
            .find(|observation| observation.node_id == node.node_id)
        else {
            continue;
        };
        let Some((_, response)) = state_responses
            .iter()
            .find(|(node_id, _)| *node_id == node.node_id)
        else {
            continue;
        };
        if let Ok(NodeResponse::StateDigest {
            writer_epoch,
            next_position: observed_next,
            physical_bytes,
            record_count,
            records_sha256,
            ..
        }) = response
        {
            if *writer_epoch == Some(config.writer_epoch)
                && *observed_next == next_position
                && *record_count == acknowledged_records
                && records_sha256 == &expected_records_sha256
            {
                exact_state_nodes = exact_state_nodes.saturating_add(1);
            }
            node_observations.push(StagedTxLogMachineCurveNodeObservation {
                node_id: node.node_id,
                machine_id: node.machine_id.clone(),
                endpoint: node.endpoint.clone(),
                process_id: observation.process_id,
                root: observation.root.clone(),
                listener: observation.listener.clone(),
                final_physical_bytes: *physical_bytes,
                final_record_count: *record_count,
                final_records_sha256: records_sha256.clone(),
            });
        }
    }

    let attempted = attempted_records.load(Ordering::Acquire);
    let enqueued = enqueued_records.load(Ordering::Acquire);
    let refused = refused_records.load(Ordering::Acquire);
    let producer_seconds =
        Duration::from_nanos(producer_finished_nanos.load(Ordering::Acquire)).as_secs_f64();
    let measurement_seconds = start_at.elapsed().as_secs_f64();
    record_ack_seconds.sort_by(f64::total_cmp);
    queue_dwell_seconds.sort_by(f64::total_cmp);
    quorum_seconds.sort_by(f64::total_cmp);
    let checks = [
        (
            "every requested arrival was attempted",
            attempted == config.record_count,
        ),
        (
            "every attempted arrival was enqueued or refused",
            attempted == enqueued.saturating_add(refused),
        ),
        (
            "every enqueued record reached a stable quorum",
            acknowledged_records == enqueued,
        ),
        (
            "every node retained the exact acknowledged history",
            exact_state_nodes == bounded_u64(NODE_COUNT),
        ),
        (
            "every node response was synchronized",
            node_response_anomalies == 0,
        ),
        (
            "the active-writer queue remained bounded",
            max_queue_depth.load(Ordering::Acquire) <= bounded_u64(config.queue_capacity_records),
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find_map(|(detail, passed)| (!passed).then(|| (*detail).to_owned()));
    let anomaly_count = u64::from(first_mismatch.is_some());
    let acknowledged_as_f64 = f64::from(u32::try_from(acknowledged_records).unwrap_or(u32::MAX));
    let attempted_as_f64 = f64::from(u32::try_from(attempted).unwrap_or(u32::MAX));
    let batch_count = bounded_u64(batch_samples.len());
    let mut report = StagedTxLogMachineCurveReport {
        schema_version: 1,
        scope: "staged-txlog-l2-open-loop-curve-point".to_owned(),
        seed: config.seed,
        writer_epoch: config.writer_epoch,
        log_identity: config.log_identity,
        nodes: node_observations,
        record_bytes: bounded_u64(config.record_bytes),
        requested_records: config.record_count,
        enqueued_records: enqueued,
        refused_records: refused,
        acknowledged_records,
        offered_records_per_second: config.offered_records_per_second,
        realized_offered_records_per_second: if producer_seconds > 0.0 {
            attempted_as_f64 / producer_seconds
        } else {
            0.0
        },
        acknowledged_records_per_second: if measurement_seconds > 0.0 {
            acknowledged_as_f64 / measurement_seconds
        } else {
            0.0
        },
        client_tasks: bounded_u64(config.client_tasks),
        stream_count: bounded_u64(config.stream_count),
        queue_capacity_records: bounded_u64(config.queue_capacity_records),
        max_queue_depth_records: max_queue_depth.load(Ordering::Acquire),
        max_batch_records: bounded_u64(config.max_batch_records),
        max_batch_dwell_micros: config.max_batch_dwell_micros,
        batch_count,
        mean_batch_records: if batch_count > 0 {
            acknowledged_as_f64 / f64::from(u32::try_from(batch_count).unwrap_or(u32::MAX))
        } else {
            0.0
        },
        max_observed_batch_records: batch_samples
            .iter()
            .map(|sample| sample.record_count)
            .max()
            .unwrap_or(0),
        network_batch_requests,
        producer_seconds,
        measurement_seconds,
        record_ack_p50_seconds: percentile_per_mille(&record_ack_seconds, 500),
        record_ack_p95_seconds: percentile_per_mille(&record_ack_seconds, 950),
        record_ack_p99_seconds: percentile_per_mille(&record_ack_seconds, 990),
        record_ack_p999_seconds: percentile_per_mille(&record_ack_seconds, 999),
        queue_dwell_p50_seconds: percentile_per_mille(&queue_dwell_seconds, 500),
        queue_dwell_p95_seconds: percentile_per_mille(&queue_dwell_seconds, 950),
        queue_dwell_p99_seconds: percentile_per_mille(&queue_dwell_seconds, 990),
        queue_dwell_p999_seconds: percentile_per_mille(&queue_dwell_seconds, 999),
        quorum_p50_seconds: percentile_per_mille(&quorum_seconds, 500),
        quorum_p95_seconds: percentile_per_mille(&quorum_seconds, 950),
        quorum_p99_seconds: percentile_per_mille(&quorum_seconds, 990),
        quorum_p999_seconds: percentile_per_mille(&quorum_seconds, 999),
        batch_samples,
        expected_records_sha256,
        exact_state_nodes,
        object_operations: 0,
        anomaly_count,
        first_mismatch,
        report_sha256: String::new(),
    };
    report.report_sha256 = hex_digest(
        &serde_json::to_vec(&report).map_err(|error| format!("encode curve report: {error}"))?,
    );
    Ok(report)
}

fn machine_health(
    nodes: &[StagedTxLogMachineNodeConfig],
    log_identity: StagedLogIdentity,
) -> Result<Vec<HealthObservation>, String> {
    let mut health = Vec::with_capacity(nodes.len());
    for node in nodes {
        match request_node(&node.endpoint, &NodeRequest::Health)? {
            NodeResponse::Health {
                node_id,
                process_id,
                root,
                listener,
                log_identity: observed_identity,
                ..
            } if node_id == node.node_id && observed_identity == log_identity => {
                health.push(HealthObservation {
                    node_id,
                    process_id,
                    root,
                    listener,
                    log_identity: observed_identity,
                    recovered_torn_tail: false,
                });
            }
            other => {
                return Err(format!(
                    "machine {} returned unexpected health response: {other:?}",
                    node.machine_id
                ));
            }
        }
    }
    Ok(health)
}

fn install_machine_epoch(nodes: &mut [ConnectedNode], writer_epoch: u64) -> Result<(), String> {
    for (node_id, response) in
        request_all_connected(nodes, &NodeRequest::InstallEpoch { writer_epoch })
    {
        match response? {
            NodeResponse::Epoch {
                writer_epoch: observed_epoch,
                synchronized: true,
                ..
            } if observed_epoch == writer_epoch => {}
            other => {
                return Err(format!(
                    "node {node_id} did not install epoch {writer_epoch}: {other:?}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_machine_preflight_config(
    config: &StagedTxLogMachinePreflightConfig,
) -> Result<(), String> {
    if config.schema_version != 1
        || config.writer_epoch == 0
        || config.record_count == 0
        || config.record_count > MACHINE_PREFLIGHT_MAX_RECORDS
        || config.record_bytes == 0
        || config.record_bytes > MACHINE_PREFLIGHT_MAX_PAYLOAD_BYTES
        || config.batch_records == 0
        || config.batch_records > MACHINE_PREFLIGHT_MAX_BATCH_RECORDS
        || config.nodes.len() != NODE_COUNT
    {
        return Err("invalid staged txLog machine preflight bounds".to_owned());
    }
    validate_machine_topology(&config.nodes)
}

fn validate_machine_curve_config(config: &StagedTxLogMachineCurveConfig) -> Result<(), String> {
    if config.schema_version != 1
        || config.writer_epoch == 0
        || config.record_count == 0
        || config.record_count > MACHINE_CURVE_MAX_RECORDS
        || config.record_bytes == 0
        || config.record_bytes > MACHINE_PREFLIGHT_MAX_PAYLOAD_BYTES
        || config.max_batch_records == 0
        || config.max_batch_records > MACHINE_PREFLIGHT_MAX_BATCH_RECORDS
        || config.max_batch_dwell_micros > MACHINE_CURVE_MAX_DWELL_MICROS
        || !config.offered_records_per_second.is_finite()
        || config.offered_records_per_second <= 0.0
        || config.offered_records_per_second > MACHINE_CURVE_MAX_OFFERED_RECORDS_PER_SECOND
        || config.client_tasks == 0
        || config.client_tasks > MACHINE_CURVE_MAX_CLIENT_TASKS
        || config.stream_count == 0
        || config.stream_count > MACHINE_CURVE_MAX_STREAMS
        || config.queue_capacity_records == 0
        || config.queue_capacity_records > MACHINE_CURVE_MAX_QUEUE_RECORDS
        || config.node_queue_capacity_batches == 0
        || config.node_queue_capacity_batches > MACHINE_PREFLIGHT_MAX_BATCH_RECORDS
    {
        return Err("invalid staged txLog machine curve bounds".to_owned());
    }
    validate_machine_topology(&config.nodes)
}

fn validate_machine_topology(nodes: &[StagedTxLogMachineNodeConfig]) -> Result<(), String> {
    if nodes.len() != NODE_COUNT {
        return Err("machine topology requires exactly three nodes".to_owned());
    }
    let node_ids = nodes
        .iter()
        .map(|node| node.node_id)
        .collect::<BTreeSet<_>>();
    let machine_ids = nodes
        .iter()
        .map(|node| node.machine_id.as_str())
        .collect::<BTreeSet<_>>();
    let endpoints = nodes
        .iter()
        .map(|node| node.endpoint.as_str())
        .collect::<BTreeSet<_>>();
    if node_ids != BTreeSet::from([0, 1, 2])
        || machine_ids.len() != NODE_COUNT
        || machine_ids.contains("")
        || endpoints.len() != NODE_COUNT
        || nodes
            .iter()
            .any(|node| node.endpoint.parse::<SocketAddr>().is_err())
    {
        return Err(
            "machine curve requires three exact node, machine, and endpoint identities".to_owned(),
        );
    }
    Ok(())
}

impl ConnectedNode {
    fn connect(config: &StagedTxLogMachineNodeConfig) -> Result<Self, String> {
        let address = config
            .endpoint
            .parse::<SocketAddr>()
            .map_err(|error| format!("invalid node address {}: {error}", config.endpoint))?;
        let stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
            .map_err(|error| format!("connect {}: {error}", config.endpoint))?;
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
            .and_then(|()| stream.set_nodelay(true))
            .map_err(|error| error.to_string())?;
        Ok(Self {
            node_id: config.node_id,
            stream,
        })
    }

    fn request(&mut self, request: &NodeRequest) -> Result<NodeResponse, String> {
        write_wire(&mut self.stream, request)?;
        read_wire(&mut self.stream)
    }
}

fn request_all_connected(
    nodes: &mut [ConnectedNode],
    request: &NodeRequest,
) -> Vec<(u8, Result<NodeResponse, String>)> {
    let (sender, receiver) = mpsc::channel();
    thread::scope(|scope| {
        for node in nodes {
            let sender = sender.clone();
            let request = request.clone();
            scope.spawn(move || {
                let response = node.request(&request);
                let _ = sender.send((node.node_id, response));
            });
        }
        drop(sender);
    });
    let mut responses = receiver.into_iter().collect::<Vec<_>>();
    responses.sort_by_key(|(node_id, _)| *node_id);
    responses
}

fn append_batch_parallel_connected(
    nodes: &mut [ConnectedNode],
    records: &[WireRecord],
) -> ParallelBatchAppend {
    let request = NodeRequest::AppendBatch {
        records: records.to_vec(),
    };
    let started = Instant::now();
    let (sender, receiver) = mpsc::channel();
    let mut durable_acknowledgements = 0_u64;
    let mut quorum_duration_seconds = None;
    let mut responses = Vec::with_capacity(nodes.len());
    thread::scope(|scope| {
        for node in nodes {
            let sender = sender.clone();
            let request = request.clone();
            scope.spawn(move || {
                let response = node.request(&request);
                let _ = sender.send((node.node_id, response, started.elapsed()));
            });
        }
        drop(sender);
        for (node_id, response, elapsed) in receiver {
            if matches!(
                response,
                Ok(NodeResponse::AppendBatch {
                    synchronized: true,
                    ..
                })
            ) {
                durable_acknowledgements = durable_acknowledgements.saturating_add(1);
                if durable_acknowledgements == bounded_u64(WRITE_QUORUM) {
                    quorum_duration_seconds = Some(elapsed.as_secs_f64());
                }
            }
            responses.push((node_id, response));
        }
    });
    responses.sort_by_key(|(node_id, _)| *node_id);
    ParallelBatchAppend {
        responses,
        quorum_duration_seconds: quorum_duration_seconds
            .unwrap_or_else(|| started.elapsed().as_secs_f64()),
        durable_acknowledgements,
    }
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let maximum_index = sorted.len().saturating_sub(1);
    let position = maximum_index.saturating_mul(percentile).saturating_add(50) / 100;
    sorted[position.min(maximum_index)]
}

fn percentile_per_mille(sorted: &[f64], per_mille: usize) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let maximum_index = sorted.len().saturating_sub(1);
    let position = maximum_index.saturating_mul(per_mille).saturating_add(500) / 1_000;
    sorted[position.min(maximum_index)]
}

fn update_atomic_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn start_nodes(
    executable: &Path,
    configs: &[StagedTxLogNodeConfig],
) -> Result<Vec<RunningNode>, String> {
    let mut nodes = Vec::with_capacity(configs.len());
    for config in configs {
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("staged-txlog-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("start staged txLog node {}: {error}", config.node_id))?;
        nodes.push(RunningNode {
            node_id: config.node_id,
            endpoint: config.listen_addr.clone(),
            child,
        });
    }
    Ok(nodes)
}

fn wait_for_health(nodes: &mut [RunningNode]) -> Result<Vec<HealthObservation>, String> {
    let mut observations = Vec::with_capacity(nodes.len());
    for node in nodes {
        let mut observation = None;
        for _ in 0..START_ATTEMPTS {
            if let Some(status) = node.child.try_wait().map_err(|error| error.to_string())? {
                return Err(format!(
                    "staged txLog node {} exited during startup with {status}",
                    node.node_id
                ));
            }
            if let Ok(NodeResponse::Health {
                node_id,
                process_id,
                root,
                listener,
                log_identity,
                recovered_torn_tail,
                ..
            }) = request_node(&node.endpoint, &NodeRequest::Health)
            {
                observation = Some(HealthObservation {
                    node_id,
                    process_id,
                    root,
                    listener,
                    log_identity,
                    recovered_torn_tail,
                });
                break;
            }
            thread::sleep(START_RETRY_DELAY);
        }
        observations.push(
            observation.ok_or_else(|| {
                format!("staged txLog node {} did not become ready", node.node_id)
            })?,
        );
    }
    Ok(observations)
}

fn health_matches_configs(
    observations: &[HealthObservation],
    configs: &[StagedTxLogNodeConfig],
) -> bool {
    observations.len() == configs.len()
        && observations.iter().all(|health| {
            configs.iter().any(|config| {
                config.node_id == health.node_id
                    && config.root.display().to_string() == health.root
                    && config.listen_addr == health.listener
                    && config.log_identity == health.log_identity
            })
        })
}

fn install_epoch(nodes: &[RunningNode], writer_epoch: u64) -> Result<(), String> {
    let responses = request_all(nodes, &NodeRequest::InstallEpoch { writer_epoch });
    for (node_id, response) in responses {
        match response? {
            NodeResponse::Epoch {
                writer_epoch: observed,
                synchronized: true,
                ..
            } if observed == writer_epoch => {}
            other => {
                return Err(format!(
                    "node {node_id} did not install writer epoch {writer_epoch}: {other:?}"
                ));
            }
        }
    }
    Ok(())
}

fn read_states(nodes: &[RunningNode]) -> Result<Vec<StateObservation>, String> {
    request_all(nodes, &NodeRequest::State)
        .into_iter()
        .map(|(node_id, response)| match response? {
            NodeResponse::State {
                physical_bytes,
                recovered_torn_tail,
                records,
                ..
            } => Ok(StateObservation {
                node_id,
                physical_bytes,
                recovered_torn_tail,
                records,
            }),
            other => Err(format!(
                "node {node_id} returned non-state response {other:?}"
            )),
        })
        .collect()
}

impl ParallelAppend {
    fn acknowledged_nodes(&self) -> Vec<u8> {
        self.responses
            .iter()
            .filter_map(|(node_id, response)| {
                matches!(
                    response,
                    Ok(NodeResponse::Append {
                        synchronized: true,
                        ..
                    })
                )
                .then_some(*node_id)
            })
            .collect()
    }
}

fn staged_txlog_cell_trace(
    samples: &[StagedTxLogAppendSample],
) -> Result<CellTraceRefinementV1, String> {
    let config = CellTraceConfigV1::new(
        (0..NODE_COUNT).map(|node| format!("n{node}")),
        WRITE_QUORUM,
        1,
    )
    .map_err(|error| error.detail)?;
    let mut events = Vec::new();
    let mut assertions = Vec::new();
    let mut generation_advanced = false;
    for sample in samples {
        if sample.position > 3 && !generation_advanced {
            events.push(CellTraceEventV1::AdvanceGeneration);
            for node in 0..NODE_COUNT {
                events.push(CellTraceEventV1::InstallGeneration {
                    node: format!("n{node}"),
                });
            }
            generation_advanced = true;
        }
        let transaction = format!("txlog-position-{}", sample.position);
        events.push(CellTraceEventV1::Begin {
            transaction: transaction.clone(),
        });
        events.push(CellTraceEventV1::SequenceTxn {
            transaction: transaction.clone(),
            version: sample.position,
        });
        let staged_nodes = sample
            .acknowledged_nodes
            .iter()
            .chain(&sample.stable_nodes_observed)
            .copied()
            .collect::<BTreeSet<_>>();
        for node in staged_nodes {
            events.push(CellTraceEventV1::StageInRam {
                transaction: transaction.clone(),
                node: format!("n{node}"),
            });
        }
        for node in &sample.stable_nodes_observed {
            events.push(CellTraceEventV1::PersistOnStableMedia {
                transaction: transaction.clone(),
                node: format!("n{node}"),
            });
        }
        if sample.position <= 3 {
            assertions.push(CellTraceAssertionV1::StableQuorumAtAcknowledgement {
                transaction,
                acknowledged_nodes: sample
                    .acknowledged_nodes
                    .iter()
                    .map(|node| format!("n{node}"))
                    .collect(),
            });
        }
    }
    let refinement = CellTraceRefinementV1::evaluate(
        "staged-txlog-l1-stable-media-prefix",
        config,
        events,
        assertions,
    );
    refinement
        .validate()
        .map_err(|error| format!("invalid cell trace refinement: {}", error.detail))?;
    Ok(refinement)
}

fn append_parallel(
    nodes: &[RunningNode],
    writer_epoch: u64,
    position: u64,
    request_identity: StagedRequestIdentity,
    payload: &[u8],
) -> ParallelAppend {
    let request = NodeRequest::Append {
        writer_epoch,
        position,
        request_identity,
        payload: payload.to_vec(),
    };
    let started = Instant::now();
    let (sender, receiver) = mpsc::channel();
    let mut timed_responses = Vec::with_capacity(nodes.len());
    let mut durable_acknowledgements = 0_u64;
    let mut quorum_duration_seconds = None;
    thread::scope(|scope| {
        for node in nodes {
            let sender = sender.clone();
            let request = request.clone();
            scope.spawn(move || {
                let response = request_node(&node.endpoint, &request);
                let _ = sender.send((node.node_id, response, started.elapsed()));
            });
        }
        drop(sender);
        for (node_id, response, elapsed) in receiver {
            if matches!(
                &response,
                Ok(NodeResponse::Append {
                    synchronized: true,
                    ..
                })
            ) {
                durable_acknowledgements = durable_acknowledgements.saturating_add(1);
                if durable_acknowledgements == bounded_u64(WRITE_QUORUM) {
                    quorum_duration_seconds = Some(elapsed.as_secs_f64());
                }
            }
            timed_responses.push((node_id, response));
        }
    });

    let mut responses = timed_responses;
    responses.sort_by_key(|(node_id, _)| *node_id);
    ParallelAppend {
        responses,
        quorum_duration_seconds: quorum_duration_seconds
            .unwrap_or_else(|| started.elapsed().as_secs_f64()),
        durable_acknowledgements,
    }
}

fn request_all(
    nodes: &[RunningNode],
    request: &NodeRequest,
) -> Vec<(u8, Result<NodeResponse, String>)> {
    let (sender, receiver) = mpsc::channel();
    thread::scope(|scope| {
        for node in nodes {
            let sender = sender.clone();
            let request = request.clone();
            scope.spawn(move || {
                let _ = sender.send((node.node_id, request_node(&node.endpoint, &request)));
            });
        }
        drop(sender);
    });
    let mut responses = receiver.into_iter().collect::<Vec<_>>();
    responses.sort_by_key(|(node_id, _)| *node_id);
    responses
}

fn request_node(endpoint: &str, request: &NodeRequest) -> Result<NodeResponse, String> {
    let address = endpoint
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid node address {endpoint}: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
        .map_err(|error| format!("connect {endpoint}: {error}"))?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|error| error.to_string())?;
    write_wire(&mut stream, request)?;
    read_wire(&mut stream)
}

fn write_wire<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if bytes.len() > MAX_WIRE_BYTES {
        return Err("wire message exceeds limit".to_owned());
    }
    let length = u32::try_from(bytes.len()).map_err(|error| error.to_string())?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(&bytes))
        .and_then(|()| stream.flush())
        .map_err(|error| error.to_string())
}

fn read_wire<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T, String> {
    read_wire_optional(stream)?.ok_or_else(|| "wire peer closed before a response".to_owned())
}

fn read_wire_optional<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<Option<T>, String> {
    let mut header = [0_u8; 4];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.to_string()),
    }
    let length = usize::try_from(u32::from_be_bytes(header)).map_err(|error| error.to_string())?;
    if length > MAX_WIRE_BYTES {
        return Err("wire message exceeds limit".to_owned());
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn reserve_loopback_address() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    listener
        .local_addr()
        .map(|address| address.to_string())
        .map_err(|error| error.to_string())
}

fn stop_nodes(nodes: &mut Vec<RunningNode>) -> u64 {
    let killed = nodes
        .iter_mut()
        .map(|node| usize::from(node.stop()))
        .sum::<usize>();
    nodes.clear();
    bounded_u64(killed)
}

fn inject_torn_tail(root: &Path) -> Result<(), String> {
    let path = root.join(JOURNAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .map_err(|error| format!("open torn-tail target {}: {error}", path.display()))?;
    file.write_all(b"OKVT\0")
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("inject torn tail {}: {error}", path.display()))
}

fn deterministic_payload(seed: u64, position: u64, length: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(length);
    let mut block = 0_u64;
    while payload.len() < length {
        let digest = identity(&[
            b"okv-staged-txlog-payload",
            &seed.to_be_bytes(),
            &position.to_be_bytes(),
            &block.to_be_bytes(),
        ]);
        let remaining = length.saturating_sub(payload.len());
        payload.extend_from_slice(&digest[..remaining.min(digest.len())]);
        block = block.saturating_add(1);
    }
    payload
}

fn request_identity(seed: u64, position: u64, payload: &[u8]) -> StagedRequestIdentity {
    identity(&[
        b"okv-staged-txlog-request",
        &seed.to_be_bytes(),
        &position.to_be_bytes(),
        payload,
    ])
}

fn identity(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn wire_records_digest<'a>(records: impl IntoIterator<Item = &'a WireRecord>) -> String {
    let mut hasher = Sha256::new();
    for record in records {
        update_wire_record_digest(&mut hasher, record);
    }
    format!("{:x}", hasher.finalize())
}

fn update_wire_record_digest(hasher: &mut Sha256, record: &WireRecord) {
    hasher.update(record.writer_epoch.to_be_bytes());
    hasher.update(record.position.to_be_bytes());
    hasher.update(record.request_identity);
    hasher.update(bounded_u64(record.payload.len()).to_be_bytes());
    hasher.update(&record.payload);
}

fn bounded_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_protocol_round_trips_append_requests() {
        let request = NodeRequest::Append {
            writer_epoch: 7,
            position: 3,
            request_identity: [0x22; 32],
            payload: vec![0x33; 128],
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded = serde_json::from_slice::<NodeRequest>(&encoded).unwrap();
        match decoded {
            NodeRequest::Append {
                writer_epoch,
                position,
                request_identity,
                payload,
            } => {
                assert_eq!(writer_epoch, 7);
                assert_eq!(position, 3);
                assert_eq!(request_identity, [0x22; 32]);
                assert_eq!(payload, vec![0x33; 128]);
            }
            _ => panic!("decoded the wrong request variant"),
        }
    }

    #[test]
    fn wire_protocol_round_trips_append_batches() {
        let request = NodeRequest::AppendBatch {
            records: vec![WireRecord {
                writer_epoch: 7,
                position: 3,
                request_identity: [0x22; 32],
                payload: vec![0x33; 128],
            }],
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded = serde_json::from_slice::<NodeRequest>(&encoded).unwrap();
        match decoded {
            NodeRequest::AppendBatch { records } => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].position, 3);
                assert_eq!(records[0].payload, vec![0x33; 128]);
            }
            _ => panic!("decoded the wrong request variant"),
        }
    }

    #[test]
    fn node_runtime_persists_one_batch_with_one_aggregate_response() {
        let temp = tempfile::tempdir().unwrap();
        let config = StagedTxLogNodeConfig {
            node_id: 0,
            listen_addr: "127.0.0.1:0".to_owned(),
            root: temp.path().join("node"),
            log_identity: [0x11; 32],
            mode: StagedTxLogProcessMode::Correct,
        };
        let mut runtime = NodeRuntime::open(config, "127.0.0.1:7000".to_owned()).unwrap();
        assert!(matches!(
            runtime.handle(NodeRequest::InstallEpoch { writer_epoch: 7 }),
            NodeResponse::Epoch {
                synchronized: true,
                ..
            }
        ));
        let records = (1_u64..=256)
            .map(|position| WireRecord {
                writer_epoch: 7,
                position,
                request_identity: [u8::try_from(position % 251).unwrap(); 32],
                payload: vec![0x33; 128],
            })
            .collect::<Vec<_>>();
        let expected_digest = wire_records_digest(records.iter());
        assert!(matches!(
            runtime.handle(NodeRequest::AppendBatch {
                records: records.clone()
            }),
            NodeResponse::AppendBatch {
                first_position: 1,
                last_position: 256,
                record_count: 256,
                new_record_count: 256,
                replayed_record_count: 0,
                synchronized: true,
                ..
            }
        ));
        assert_eq!(runtime.node.records().len(), 256);
        assert!(matches!(
            runtime.handle(NodeRequest::StateDigest),
            NodeResponse::StateDigest {
                writer_epoch: Some(7),
                next_position: 257,
                record_count: 256,
                records_sha256,
                ..
            } if records_sha256 == expected_digest
        ));
    }

    #[test]
    fn machine_preflight_requires_three_distinct_machine_identities() {
        let mut config = StagedTxLogMachinePreflightConfig {
            schema_version: 1,
            seed: 17,
            writer_epoch: 7,
            log_identity: [0x11; 32],
            nodes: (0_u8..3)
                .map(|node_id| StagedTxLogMachineNodeConfig {
                    node_id,
                    machine_id: format!("machine-{node_id}"),
                    endpoint: format!("127.0.0.1:{}", 7_000_u16 + u16::from(node_id)),
                })
                .collect(),
            record_bytes: 128,
            record_count: 8_192,
            batch_records: 256,
        };
        validate_machine_preflight_config(&config).unwrap();
        config.nodes[2].machine_id = config.nodes[1].machine_id.clone();
        assert!(validate_machine_preflight_config(&config).is_err());
    }

    #[test]
    fn machine_curve_freezes_open_loop_and_queue_bounds() {
        let mut config = StagedTxLogMachineCurveConfig {
            schema_version: 1,
            seed: 17,
            writer_epoch: 7,
            log_identity: [0x11; 32],
            nodes: (0_u8..3)
                .map(|node_id| StagedTxLogMachineNodeConfig {
                    node_id,
                    machine_id: format!("machine-{node_id}"),
                    endpoint: format!("127.0.0.1:{}", 7_000_u16 + u16::from(node_id)),
                })
                .collect(),
            record_bytes: 128,
            record_count: 65_536,
            max_batch_records: 256,
            max_batch_dwell_micros: 250,
            offered_records_per_second: 100_000.0,
            client_tasks: 64,
            stream_count: 256,
            queue_capacity_records: 65_536,
            node_queue_capacity_batches: 1_024,
        };
        validate_machine_curve_config(&config).unwrap();
        config.queue_capacity_records = 0;
        assert!(validate_machine_curve_config(&config).is_err());
    }

    #[test]
    fn deterministic_poisson_delay_is_positive_and_repeatable() {
        let mut first = DeterministicPoisson::new(17);
        let mut second = DeterministicPoisson::new(17);
        for _ in 0..32 {
            let left = first.exponential_delay(100_000.0);
            let right = second.exponential_delay(100_000.0);
            assert_eq!(left, right);
            assert!(!left.is_zero());
        }
    }

    #[test]
    fn deterministic_payload_is_seeded_and_exact_length() {
        let first = deterministic_payload(1103, 1, 4_096);
        let second = deterministic_payload(1103, 1, 4_096);
        assert_eq!(first, second);
        assert_eq!(first.len(), 4_096);
        assert_ne!(first, deterministic_payload(2207, 1, 4_096));
    }
}
