use crate::sim_network::{
    elect, heartbeat, initialize, run_node, status, write, NodeStatus, WriteAck,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PAYLOAD_A: &[u8] = b"A";
const PAYLOAD_B: &[u8] = b"B";
const PAYLOAD_C: &[u8] = b"C";

/// Deliberately incorrect cluster behaviors used to validate the gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftClusterMode {
    Correct,
    AcknowledgeBeforeQuorum,
    SkipSuccessorElection,
    SkipRestartCatchup,
}

impl RaftClusterMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcknowledgeBeforeQuorum => "acknowledge_before_quorum",
            Self::SkipSuccessorElection => "skip_successor_election",
            Self::SkipRestartCatchup => "skip_restart_catchup",
        }
    }
}

/// Deterministic semantic report for one three-node replication scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftClusterReport {
    pub seed: u64,
    pub mode: RaftClusterMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub committed_writes: u64,
    pub elections: u64,
    pub stale_write_attempts: u64,
    pub stale_write_acks: u64,
    pub partitions: u64,
    pub repairs: u64,
    pub simulated_crashes: u64,
    pub simulated_bounces: u64,
    pub caught_up_nodes: u64,
    pub trace_sha256: String,
}

/// Run a three-node `OpenRaft` cluster over a seeded Turmoil TCP network.
///
/// # Errors
///
/// Returns an error only when the simulator or transport cannot execute. Any
/// semantic disagreement is recorded as an anomaly in the report.
pub fn run_raft_cluster_contract(
    seed: u64,
    mode: RaftClusterMode,
) -> Result<RaftClusterReport, String> {
    Scenario::new(seed, mode)?.run()
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default)]
struct Observations {
    phase_a_leader: bool,
    phase_a_committed: bool,
    phase_a_all_applied: bool,
    successor_elected: bool,
    stale_write_acked: bool,
    phase_b_committed: bool,
    phase_b_quorum_applied: bool,
    repaired_all_applied: bool,
    stale_suffix_removed: bool,
    phase_c_leader: bool,
    phase_c_committed: bool,
    phase_c_quorum_applied: bool,
    restarted_all_applied: bool,
    final_statuses: Vec<NodeStatus>,
}

struct Scenario {
    seed: u64,
    mode: RaftClusterMode,
    root: TempRoot,
    observations: Arc<Mutex<Observations>>,
}

impl Scenario {
    fn new(seed: u64, mode: RaftClusterMode) -> Result<Self, String> {
        Ok(Self {
            seed,
            mode,
            root: TempRoot::new(seed, mode)?,
            observations: Arc::new(Mutex::new(Observations::default())),
        })
    }

    #[allow(clippy::too_many_lines)]
    fn run(self) -> Result<RaftClusterReport, String> {
        let mut builder = turmoil::Builder::new();
        builder
            .rng_seed(self.seed)
            .tick_duration(Duration::from_millis(1))
            .simulation_duration(Duration::from_secs(20))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(3))
            .enable_random_order();
        let mut sim = builder.build();

        for node_id in 1_u64..=3 {
            let root = self.root.node(node_id);
            let acknowledge_before_quorum =
                self.mode == RaftClusterMode::AcknowledgeBeforeQuorum && node_id == 1;
            sim.host(format!("node-{node_id}"), move || {
                run_node(node_id, root.clone(), acknowledge_before_quorum)
            });
        }

        let phase_a = self.observations.clone();
        sim.client("controller-a", async move {
            retry(|| initialize("node-1")).await?;
            let leader = elect_until_leader("node-1", 1).await;
            let ack = timed_write("node-1", PAYLOAD_A, 1_000).await;
            let all_applied =
                wait_for_payloads(&["node-1", "node-2", "node-3"], &[PAYLOAD_A]).await;
            let mut observations = phase_a.lock().expect("observation lock poisoned");
            observations.phase_a_leader = leader;
            observations.phase_a_committed = is_committed(&ack);
            observations.phase_a_all_applied = all_applied;
            Ok(())
        });
        sim.run().map_err(|error| error.to_string())?;

        sim.partition("node-1", "node-2");
        sim.partition("node-1", "node-3");
        let phase_b = self.observations.clone();
        let skip_successor = self.mode == RaftClusterMode::SkipSuccessorElection;
        sim.client("controller-b", async move {
            if !skip_successor {
                let _ = elect_until_leader("node-2", 2).await;
            }
            let successor = wait_for_leader("node-2", 2).await;
            let stale_ack = timed_write("node-1", b"STALE", 100).await;
            let write_b = timed_write("node-2", PAYLOAD_B, 1_000).await;
            let quorum_applied =
                wait_for_payloads(&["node-2", "node-3"], &[PAYLOAD_A, PAYLOAD_B]).await;
            let mut observations = phase_b.lock().expect("observation lock poisoned");
            observations.successor_elected = successor;
            observations.stale_write_acked = is_committed(&stale_ack);
            observations.phase_b_committed = is_committed(&write_b);
            observations.phase_b_quorum_applied = quorum_applied;
            Ok(())
        });
        sim.run().map_err(|error| error.to_string())?;

        sim.repair("node-1", "node-2");
        sim.repair("node-1", "node-3");
        let repair = self.observations.clone();
        sim.client("controller-repair", async move {
            if skip_successor {
                let _ = elect_until_leader("node-2", 2).await;
                let _ = timed_write("node-2", PAYLOAD_B, 1_000).await;
            }
            retry(|| heartbeat("node-2")).await?;
            let all_applied =
                wait_for_payloads(&["node-1", "node-2", "node-3"], &[PAYLOAD_A, PAYLOAD_B]).await;
            let node1 = retry_status("node-1").await?;
            let mut observations = repair.lock().expect("observation lock poisoned");
            observations.repaired_all_applied = all_applied;
            observations.stale_suffix_removed =
                node1.payloads == [PAYLOAD_A.to_vec(), PAYLOAD_B.to_vec()];
            Ok(())
        });
        sim.run().map_err(|error| error.to_string())?;

        sim.crash("node-2");
        let phase_c = self.observations.clone();
        sim.client("controller-c", async move {
            let leader = elect_until_leader("node-3", 3).await;
            let ack = timed_write("node-3", PAYLOAD_C, 1_000).await;
            let quorum_applied =
                wait_for_payloads(&["node-1", "node-3"], &[PAYLOAD_A, PAYLOAD_B, PAYLOAD_C]).await;
            let mut observations = phase_c.lock().expect("observation lock poisoned");
            observations.phase_c_leader = leader;
            observations.phase_c_committed = is_committed(&ack);
            observations.phase_c_quorum_applied = quorum_applied;
            Ok(())
        });
        sim.run().map_err(|error| error.to_string())?;

        sim.bounce("node-2");
        let skip_restart_catchup = self.mode == RaftClusterMode::SkipRestartCatchup;
        if skip_restart_catchup {
            sim.partition("node-2", "node-1");
            sim.partition("node-2", "node-3");
        }
        let bounce = self.observations.clone();
        sim.client("controller-bounce", async move {
            if !skip_restart_catchup {
                retry(|| heartbeat("node-3")).await?;
            }
            let expected = [PAYLOAD_A, PAYLOAD_B, PAYLOAD_C];
            let all_applied = wait_for_payloads(&["node-1", "node-2", "node-3"], &expected).await;
            let mut statuses = Vec::new();
            for host in ["node-1", "node-2", "node-3"] {
                if let Ok(node_status) = status(host).await {
                    statuses.push(node_status);
                }
            }
            let mut observations = bounce.lock().expect("observation lock poisoned");
            observations.restarted_all_applied = all_applied;
            observations.final_statuses = statuses;
            Ok(())
        });
        sim.run().map_err(|error| error.to_string())?;

        Ok(build_report(
            self.seed,
            self.mode,
            &self.observations.lock().expect("observation lock poisoned"),
        ))
    }
}

fn build_report(
    seed: u64,
    mode: RaftClusterMode,
    observations: &Observations,
) -> RaftClusterReport {
    let checks = [
        ("initial_leader_elected", observations.phase_a_leader),
        (
            "initial_write_committed_and_replicated",
            observations.phase_a_committed && observations.phase_a_all_applied,
        ),
        ("successor_elected", observations.successor_elected),
        (
            "isolated_leader_did_not_ack",
            !observations.stale_write_acked,
        ),
        (
            "successor_write_committed_on_quorum",
            observations.phase_b_committed && observations.phase_b_quorum_applied,
        ),
        (
            "repaired_node_caught_up_without_stale_suffix",
            observations.repaired_all_applied && observations.stale_suffix_removed,
        ),
        (
            "post_crash_successor_committed",
            observations.phase_c_leader
                && observations.phase_c_committed
                && observations.phase_c_quorum_applied,
        ),
        (
            "restarted_node_replayed_and_caught_up",
            observations.restarted_all_applied,
        ),
    ];
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !passed);
    let anomaly_count =
        u64::try_from(checks.iter().filter(|(_, passed)| !passed).count()).unwrap_or(u64::MAX);
    let first_mismatch_step = first.and_then(|(index, _)| u64::try_from(index + 1).ok());
    let first_mismatch = first.map(|(_, (name, _))| (*name).to_owned());
    let committed_writes = u64::from(observations.phase_a_committed)
        + u64::from(observations.phase_b_committed)
        + u64::from(observations.phase_c_committed);
    let elections = u64::from(observations.phase_a_leader)
        + u64::from(observations.successor_elected)
        + u64::from(observations.phase_c_leader);
    let caught_up_nodes = observations
        .final_statuses
        .iter()
        .filter(|node| {
            node.payloads == [PAYLOAD_A.to_vec(), PAYLOAD_B.to_vec(), PAYLOAD_C.to_vec()]
        })
        .count()
        .try_into()
        .unwrap_or(u64::MAX);

    let mut trace = Sha256::new();
    trace.update(b"okv-openraft-cluster-contract-v1");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    for node in &observations.final_statuses {
        trace.update(node.node_id.to_be_bytes());
        trace.update(node.term.to_be_bytes());
        trace.update(node.state.as_bytes());
        trace.update(node.leader.unwrap_or_default().to_be_bytes());
        trace.update(node.last_log_index.unwrap_or_default().to_be_bytes());
        trace.update(node.last_applied_index.unwrap_or_default().to_be_bytes());
        for payload in &node.payloads {
            trace.update(payload);
        }
    }

    RaftClusterReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        committed_writes,
        elections,
        stale_write_attempts: 1,
        stale_write_acks: u64::from(observations.stale_write_acked),
        partitions: 2,
        repairs: 2,
        simulated_crashes: 1,
        simulated_bounces: 1,
        caught_up_nodes,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn is_committed(result: &Result<WriteAck, String>) -> bool {
    result
        .as_ref()
        .is_ok_and(|ack| ack.committed && ack.log_index.is_some())
        || result
            .as_ref()
            .is_ok_and(|ack| ack.committed && ack.log_index.is_none())
}

async fn timed_write(host: &str, payload: &[u8], millis: u64) -> Result<WriteAck, String> {
    tokio::time::timeout(Duration::from_millis(millis), write(host, payload))
        .await
        .map_err(|_| "write timed out".to_owned())?
}

async fn retry<F, Fut>(mut operation: F) -> turmoil::Result
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut last = String::new();
    for _ in 0..200 {
        match operation().await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(std::io::Error::other(format!("control operation failed: {last}")).into())
}

async fn retry_status(host: &str) -> Result<NodeStatus, Box<dyn std::error::Error>> {
    let mut last = String::new();
    for _ in 0..200 {
        match status(host).await {
            Ok(node_status) => return Ok(node_status),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(std::io::Error::other(format!("status failed for {host}: {last}")).into())
}

async fn wait_for_leader(host: &str, node_id: u64) -> bool {
    for _ in 0..200 {
        if status(host)
            .await
            .is_ok_and(|node| node.state == "leader" && node.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

async fn elect_until_leader(host: &str, node_id: u64) -> bool {
    for _ in 0..200 {
        let _ = elect(host).await;
        if status(host)
            .await
            .is_ok_and(|node| node.state == "leader" && node.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_payloads(hosts: &[&str], expected: &[&[u8]]) -> bool {
    let expected = expected
        .iter()
        .map(|payload| payload.to_vec())
        .collect::<Vec<_>>();
    for _ in 0..200 {
        let mut exact = true;
        for host in hosts {
            exact &= status(host)
                .await
                .is_ok_and(|node| node.payloads == expected);
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: RaftClusterMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-openraft-cluster-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn node(&self, node_id: u64) -> PathBuf {
        self.0.join(format!("node-{node_id}"))
    }
}

impl AsRef<Path> for TempRoot {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_node_cluster_preserves_commits_through_failover_and_bounce() {
        let report = run_raft_cluster_contract(7, RaftClusterMode::Correct).unwrap();
        assert_eq!(0, report.anomaly_count, "{report:?}");
        assert_eq!(3, report.committed_writes);
        assert_eq!(3, report.caught_up_nodes);
    }

    #[test]
    fn same_seed_replays_exactly() {
        let first = run_raft_cluster_contract(19, RaftClusterMode::Correct).unwrap();
        let second = run_raft_cluster_contract(19, RaftClusterMode::Correct).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn acknowledgement_before_quorum_is_rejected() {
        let report =
            run_raft_cluster_contract(7, RaftClusterMode::AcknowledgeBeforeQuorum).unwrap();
        assert!(report.anomaly_count > 0, "{report:?}");
        assert_eq!(1, report.stale_write_acks);
    }
}
