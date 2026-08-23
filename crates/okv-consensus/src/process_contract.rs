use crate::rpc::{
    read_response, write_request, ControlWrite, NodeStatus, WriteAck, CLIENT_WRITE, ELECT,
    HEARTBEAT, INITIALIZE, OUTCOME, STATUS,
};
use crate::{ApplyResponse, ClientCommand, NodeId, ProcessNodeConfig, RequestIdentity};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PAYLOAD_A: &[u8] = b"A";
const PAYLOAD_X: &[u8] = b"X";
const PAYLOAD_B: &[u8] = b"B";
const RETRY_ATTEMPTS: usize = 500;

/// Deliberately incorrect real-process behaviors used to validate the gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftProcessMode {
    Correct,
    DisableDedup,
    AcknowledgeBeforeQuorum,
    SkipKilledNodeRestart,
}

impl RaftProcessMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DisableDedup => "disable_dedup",
            Self::AcknowledgeBeforeQuorum => "acknowledge_before_quorum",
            Self::SkipKilledNodeRestart => "skip_killed_node_restart",
        }
    }
}

/// Canonical semantic report for one real-process `OpenRaft` scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RaftProcessReport {
    pub seed: u64,
    pub mode: RaftProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub committed_writes: u64,
    pub elections: u64,
    pub process_starts: u64,
    pub process_kills: u64,
    pub dropped_replies: u64,
    pub duplicate_retries: u64,
    pub recovered_outcomes: u64,
    pub caught_up_nodes: u64,
    pub trace_sha256: String,
}

/// Run a three-node `OpenRaft` contract with normal TCP and actual OS processes.
///
/// State-machine outcomes are reconstructed by replaying the retained Raft log
/// into a fresh in-memory state machine when a process restarts.
///
/// # Errors
///
/// Returns an error when the controller cannot allocate local addresses, start
/// a node process, or execute the bounded protocol.
pub fn run_raft_process_contract(
    seed: u64,
    mode: RaftProcessMode,
    executable: &Path,
) -> Result<RaftProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(ProcessScenario::new(seed, mode, executable)?.run())
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default)]
struct Observations {
    initial_cluster_applied: bool,
    lost_reply_observed: bool,
    leader_process_killed: bool,
    successor_elected: bool,
    retry_matches_durable_outcome: bool,
    retry_applied_once: bool,
    killed_node_recovered_outcome: bool,
    all_nodes_continue_exactly: bool,
    committed_writes: u64,
    elections: u64,
    process_starts: u64,
    process_kills: u64,
    dropped_replies: u64,
    duplicate_retries: u64,
    recovered_outcomes: u64,
    caught_up_nodes: u64,
    final_payloads: BTreeMap<NodeId, Vec<Vec<u8>>>,
    final_outcomes: BTreeMap<NodeId, Option<ApplyResponse>>,
}

struct ProcessScenario<'a> {
    seed: u64,
    mode: RaftProcessMode,
    executable: &'a Path,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: Observations,
    identity: RequestIdentity,
}

impl<'a> ProcessScenario<'a> {
    fn new(seed: u64, mode: RaftProcessMode, executable: &'a Path) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "process contract executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            mode,
            executable,
            root: TempRoot::new(seed, mode)?,
            addresses: allocate_addresses()?,
            children: ChildGroup::default(),
            observations: Observations::default(),
            identity: RequestIdentity {
                client_id: seed ^ 0x4f4b_5650_524f_4331,
                request_id: 1,
            },
        })
    }

    async fn run(mut self) -> Result<RaftProcessReport, String> {
        self.start_initial_cluster().await?;
        if self.mode == RaftProcessMode::AcknowledgeBeforeQuorum {
            self.run_acknowledge_before_quorum().await?;
        } else {
            self.run_lost_reply_recovery().await?;
        }
        Ok(build_report(self.seed, self.mode, &self.observations))
    }

    async fn start_initial_cluster(&mut self) -> Result<(), String> {
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        self.observations.elections += u64::from(elect_until_leader(self.address(1)?, 1).await);
        let initial = retry_write(self.address(1)?, PAYLOAD_A.to_vec(), false).await?;
        self.observations.committed_writes += u64::from(initial.committed);
        self.observations.initial_cluster_applied =
            wait_for_payloads(&self.addresses, &[1, 2, 3], &[PAYLOAD_A.to_vec()]).await;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run_lost_reply_recovery(&mut self) -> Result<(), String> {
        let command = ClientCommand {
            identity: self.identity,
            payload: PAYLOAD_X.to_vec(),
        }
        .encode()
        .map_err(|error| error.to_string())?;

        let dropped = write(self.address(1)?, command.clone(), true).await;
        self.observations.lost_reply_observed = dropped.is_err();
        self.observations.dropped_replies = u64::from(dropped.is_err());
        self.kill_node(1)?;
        self.observations.leader_process_killed = true;

        self.observations.successor_elected = elect_until_leader(self.address(2)?, 2).await;
        self.observations.elections += u64::from(self.observations.successor_elected);
        let original = wait_for_outcome(self.address(2)?, self.identity).await;
        let retry = retry_write(self.address(2)?, command, false).await?;
        self.observations.duplicate_retries = 1;
        self.observations.committed_writes += u64::from(retry.committed);
        self.observations.retry_matches_durable_outcome = original
            .as_ref()
            .zip(retry.response.as_ref())
            .is_some_and(|(left, right)| left == right);
        self.observations.retry_applied_once = if self.mode == RaftProcessMode::DisableDedup {
            status(self.address(2)?)
                .await
                .is_ok_and(|node| node.payloads == [PAYLOAD_A.to_vec(), PAYLOAD_X.to_vec()])
        } else {
            wait_for_payloads(
                &self.addresses,
                &[2, 3],
                &[PAYLOAD_A.to_vec(), PAYLOAD_X.to_vec()],
            )
            .await
        };

        if self.mode != RaftProcessMode::SkipKilledNodeRestart {
            self.start_node(1)?;
            wait_ready(self.address(1)?).await?;
            retry_control(self.address(2)?, HEARTBEAT, &()).await?;
            let replay_complete = match retry.log_index {
                Some(index) => wait_for_applied_index(self.address(1)?, index).await,
                None => false,
            };
            self.observations.killed_node_recovered_outcome = replay_complete
                && wait_for_outcome(self.address(1)?, self.identity).await == original
                && wait_for_payloads(
                    &self.addresses,
                    &[1],
                    &[PAYLOAD_A.to_vec(), PAYLOAD_X.to_vec()],
                )
                .await;
        }

        let final_write = retry_write(self.address(2)?, PAYLOAD_B.to_vec(), false).await?;
        self.observations.committed_writes += u64::from(final_write.committed);
        let live_nodes = if self.mode == RaftProcessMode::SkipKilledNodeRestart {
            vec![2, 3]
        } else {
            vec![1, 2, 3]
        };
        let live_exact = if self.mode == RaftProcessMode::DisableDedup {
            let _ = wait_for_payloads(
                &self.addresses,
                &live_nodes,
                &[
                    PAYLOAD_A.to_vec(),
                    PAYLOAD_X.to_vec(),
                    PAYLOAD_X.to_vec(),
                    PAYLOAD_B.to_vec(),
                ],
            )
            .await;
            false
        } else {
            wait_for_payloads(
                &self.addresses,
                &live_nodes,
                &[PAYLOAD_A.to_vec(), PAYLOAD_X.to_vec(), PAYLOAD_B.to_vec()],
            )
            .await
        };
        self.capture_final(&live_nodes, Some(self.identity)).await;
        self.observations.all_nodes_continue_exactly = live_exact
            && live_nodes.len() == 3
            && self.observations.caught_up_nodes == 3
            && self
                .observations
                .final_outcomes
                .values()
                .all(|outcome| outcome == &original);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run_acknowledge_before_quorum(&mut self) -> Result<(), String> {
        self.kill_node(2)?;
        self.kill_node(3)?;
        let command = ClientCommand {
            identity: self.identity,
            payload: PAYLOAD_X.to_vec(),
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let unsafe_ack = retry_write(self.address(1)?, command, false).await?;
        self.observations.committed_writes += u64::from(unsafe_ack.committed);
        self.observations.lost_reply_observed = !unsafe_ack.committed;
        self.kill_node(1)?;
        self.observations.leader_process_killed = true;

        self.start_node(2)?;
        self.start_node(3)?;
        wait_ready(self.address(2)?).await?;
        wait_ready(self.address(3)?).await?;
        self.observations.successor_elected = elect_until_leader(self.address(2)?, 2).await;
        self.observations.elections += u64::from(self.observations.successor_elected);
        let recovered = wait_for_absent_outcome(self.address(2)?, self.identity).await;
        self.observations.retry_matches_durable_outcome = !unsafe_ack.committed || !recovered;
        self.observations.retry_applied_once = recovered;

        self.start_node(1)?;
        wait_ready(self.address(1)?).await?;
        retry_control(self.address(2)?, HEARTBEAT, &()).await?;
        let no_unsafe_apply =
            wait_for_payloads(&self.addresses, &[1, 2, 3], &[PAYLOAD_A.to_vec()]).await;
        self.observations.killed_node_recovered_outcome = !no_unsafe_apply;
        let final_write = retry_write(self.address(2)?, PAYLOAD_B.to_vec(), false).await?;
        self.observations.committed_writes += u64::from(final_write.committed);
        let continued = wait_for_payloads(
            &self.addresses,
            &[1, 2, 3],
            &[PAYLOAD_A.to_vec(), PAYLOAD_B.to_vec()],
        )
        .await;
        self.capture_final(&[1, 2, 3], Some(self.identity)).await;
        self.observations.all_nodes_continue_exactly = continued && recovered;
        Ok(())
    }

    fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let config = ProcessNodeConfig {
            node_id,
            root: self.root.node(node_id),
            nodes: self.addresses.clone(),
            deduplicate_requests: self.mode != RaftProcessMode::DisableDedup,
            acknowledge_before_quorum: self.mode == RaftProcessMode::AcknowledgeBeforeQuorum
                && node_id == 1,
        };
        self.children.start(self.executable, &config)?;
        self.observations.process_starts = self.observations.process_starts.saturating_add(1);
        Ok(())
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills = self.observations.process_kills.saturating_add(1);
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    async fn capture_final(&mut self, node_ids: &[NodeId], identity: Option<RequestIdentity>) {
        let expected = if self.mode == RaftProcessMode::AcknowledgeBeforeQuorum {
            vec![PAYLOAD_A.to_vec(), PAYLOAD_B.to_vec()]
        } else {
            vec![PAYLOAD_A.to_vec(), PAYLOAD_X.to_vec(), PAYLOAD_B.to_vec()]
        };
        for node_id in node_ids {
            if let Ok(node) = status(self.address(*node_id).unwrap_or_default()).await {
                self.observations.caught_up_nodes += u64::from(node.payloads == expected);
                self.observations
                    .final_payloads
                    .insert(*node_id, node.payloads);
            }
            if let Some(identity) = identity {
                let outcome = outcome(self.address(*node_id).unwrap_or_default(), identity)
                    .await
                    .ok()
                    .flatten();
                self.observations.recovered_outcomes = self
                    .observations
                    .recovered_outcomes
                    .saturating_add(u64::from(outcome.is_some()));
                self.observations.final_outcomes.insert(*node_id, outcome);
            }
        }
    }
}

fn build_report(
    seed: u64,
    mode: RaftProcessMode,
    observations: &Observations,
) -> RaftProcessReport {
    let checks = [
        (
            "initial_cluster_applied",
            observations.initial_cluster_applied,
        ),
        ("lost_reply_observed", observations.lost_reply_observed),
        ("leader_process_killed", observations.leader_process_killed),
        ("successor_elected", observations.successor_elected),
        (
            "retry_matches_durable_outcome",
            observations.retry_matches_durable_outcome,
        ),
        ("retry_applied_once", observations.retry_applied_once),
        (
            "killed_node_recovered_outcome",
            observations.killed_node_recovered_outcome,
        ),
        (
            "all_nodes_continue_exactly",
            observations.all_nodes_continue_exactly,
        ),
    ];
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch_step = first.map(|(index, _)| (index + 1) as u64);
    let first_mismatch = first.map(|(_, (name, _))| (*name).to_owned());

    let mut trace = Sha256::new();
    trace.update(b"okv-openraft-process-contract-v1");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    for (node_id, payloads) in &observations.final_payloads {
        trace.update(node_id.to_be_bytes());
        for payload in payloads {
            trace.update((payload.len() as u64).to_be_bytes());
            trace.update(payload);
        }
        if let Some(outcome) = observations.final_outcomes.get(node_id) {
            match outcome {
                Some(outcome) => {
                    trace.update([1]);
                    trace.update(outcome.applied_log_index.to_be_bytes());
                    if let Some(identity) = outcome.identity {
                        trace.update(identity.client_id.to_be_bytes());
                        trace.update(identity.request_id.to_be_bytes());
                    }
                }
                None => trace.update([0]),
            }
        }
    }

    RaftProcessReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        committed_writes: observations.committed_writes,
        elections: observations.elections,
        process_starts: observations.process_starts,
        process_kills: observations.process_kills,
        dropped_replies: observations.dropped_replies,
        duplicate_retries: observations.duplicate_retries,
        recovered_outcomes: observations.recovered_outcomes,
        caught_up_nodes: observations.caught_up_nodes,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

#[derive(Default)]
struct ChildGroup {
    children: BTreeMap<NodeId, Child>,
}

impl ChildGroup {
    fn start(&mut self, executable: &Path, config: &ProcessNodeConfig) -> Result<(), String> {
        if self.children.contains_key(&config.node_id) {
            return Err(format!("node {} is already running", config.node_id));
        }
        let node_id = config.node_id;
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn kill(&mut self, node_id: NodeId) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&node_id)
            .ok_or_else(|| format!("node {node_id} is not running"))?;
        child
            .kill()
            .map_err(|error| format!("failed to kill node {node_id}: {error}"))?;
        child
            .wait()
            .map_err(|error| format!("failed to reap node {node_id}: {error}"))?;
        Ok(())
    }
}

impl Drop for ChildGroup {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn retry_control<Req>(address: &str, kind: u8, request: &Req) -> Result<(), String>
where
    Req: Serialize,
{
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, kind, request).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("control operation failed at {address}: {last}"))
}

async fn retry_write(
    address: &str,
    app_data: Vec<u8>,
    drop_reply_after_commit: bool,
) -> Result<WriteAck, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match write(address, app_data.clone(), drop_reply_after_commit).await {
            Ok(ack) => return Ok(ack),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("write failed at {address}: {last}"))
}

async fn write(
    address: &str,
    app_data: Vec<u8>,
    drop_reply_after_commit: bool,
) -> Result<WriteAck, String> {
    control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit,
        },
    )
    .await
}

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
}

async fn outcome(
    address: &str,
    identity: RequestIdentity,
) -> Result<Option<ApplyResponse>, String> {
    control(address, OUTCOME, &identity).await
}

async fn control<Req, Resp>(address: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn wait_ready(address: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match status(address).await {
            Ok(_) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("node did not become ready at {address}: {last}"))
}

async fn elect_until_leader(address: &str, node_id: NodeId) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let _: Result<(), String> = control(address, ELECT, &()).await;
        if status(address)
            .await
            .is_ok_and(|node| node.state == "leader" && node.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_payloads(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    expected: &[Vec<u8>],
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            exact &= status(address)
                .await
                .is_ok_and(|node| node.payloads == expected);
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_outcome(address: &str, identity: RequestIdentity) -> Option<ApplyResponse> {
    for _ in 0..RETRY_ATTEMPTS {
        if let Ok(Some(response)) = outcome(address, identity).await {
            return Some(response);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

async fn wait_for_applied_index(address: &str, expected: u64) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        if status(address).await.is_ok_and(|node| {
            node.last_applied_index
                .is_some_and(|index| index >= expected)
        }) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_absent_outcome(address: &str, identity: RequestIdentity) -> bool {
    for _ in 0..50 {
        match outcome(address, identity).await {
            Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
            Ok(Some(_)) | Err(_) => return false,
        }
    }
    true
}

fn allocate_addresses() -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (index, listener) in listeners.iter().enumerate() {
        let node_id = (index + 1) as u64;
        addresses.insert(
            node_id,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: RaftProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-openraft-process-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn node(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("node-{node_id}"))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
