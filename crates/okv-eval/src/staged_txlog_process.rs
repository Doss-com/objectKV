//! One-host, three-process mechanism gate for the staged transaction log.
//!
//! This module proves the frozen L1 process contract. It is not a performance
//! benchmark and does not model independent machine or failure domains.

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
use std::sync::mpsc;
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
    pub trace_sha256: String,
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
    State,
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
    State {
        writer_epoch: Option<u64>,
        next_position: u64,
        physical_bytes: u64,
        recovered_torn_tail: bool,
        records: Vec<WireRecord>,
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
        let request = read_wire::<NodeRequest>(&mut stream)?;
        let response = runtime.handle(request);
        write_wire(&mut stream, &response)?;
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
    recovered_torn_tail: bool,
}

#[derive(Clone, Debug)]
struct StateObservation {
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

    let mut report = StagedTxLogProcessReport {
        schema_version: 1,
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
        trace_sha256: String::new(),
    };
    report.trace_sha256 = hex_digest(
        &serde_json::to_vec(&report).map_err(|error| format!("encode process report: {error}"))?,
    );
    Ok(report)
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
                recovered_torn_tail,
                ..
            }) = request_node(&node.endpoint, &NodeRequest::Health)
            {
                observation = Some(HealthObservation {
                    node_id,
                    process_id,
                    root,
                    listener,
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
    let mut header = [0_u8; 4];
    stream
        .read_exact(&mut header)
        .map_err(|error| error.to_string())?;
    let length = usize::try_from(u32::from_be_bytes(header)).map_err(|error| error.to_string())?;
    if length > MAX_WIRE_BYTES {
        return Err("wire message exceeds limit".to_owned());
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
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
    fn deterministic_payload_is_seeded_and_exact_length() {
        let first = deterministic_payload(1103, 1, 4_096);
        let second = deterministic_payload(1103, 1, 4_096);
        assert_eq!(first, second);
        assert_eq!(first.len(), 4_096);
        assert_ne!(first, deterministic_payload(2207, 1, 4_096));
    }
}
