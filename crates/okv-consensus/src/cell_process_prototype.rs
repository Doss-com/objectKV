use crate::read_version_proxy::{request_read_version_proxy, ReadVersionProxyProcessConfig};
use crate::rpc::{
    read_response, write_request, AddLearnerRequest, ControlWrite, NodeStatus, WriteAck,
    ADD_LEARNER, CLIENT_WRITE, ELECT, HEARTBEAT, INITIALIZE, LINEARIZABLE_STATUS, OUTCOME, STATUS,
    TRIGGER_SNAPSHOT,
};
use crate::{
    ApplyResponse, CellKeyRange, CellMutation, CellReadVersion, CellStateSnapshot,
    CellTransactionCommand, CellTransactionStatus, NodeId, OpenRaftLogStore, ProcessNodeConfig,
    ProcessNodePolicy, RequestIdentity,
};
use okv_sim::CommitEnvelope;
use openraft::storage::RaftLogStorage;
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
use tokio::task::JoinSet;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const RETRY_ATTEMPTS: usize = 500;
const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];

/// Bounded subject modes used to prove that the vertical gate detects a broken
/// durable-outcome implementation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellProcessPrototypeMode {
    Correct,
    DurableSnapshotPop,
    FreshLearnerRepair,
    DisableDedup,
    LogOnlyLearnerAsRepair,
    PurgeWithoutDurableSnapshot,
}

impl CellProcessPrototypeMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DurableSnapshotPop => "durable_snapshot_pop",
            Self::FreshLearnerRepair => "fresh_learner_repair",
            Self::DisableDedup => "disable_dedup",
            Self::LogOnlyLearnerAsRepair => "log_only_learner_as_repair",
            Self::PurgeWithoutDurableSnapshot => "purge_without_durable_snapshot",
        }
    }
}

/// One visible step from the throwaway semantic transaction prototype.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellProcessPrototypeStep {
    pub phase: String,
    pub node_id: NodeId,
    pub leader: Option<NodeId>,
    pub applied_log_index: Option<u64>,
    pub snapshot_log_index: Option<u64>,
    pub latest_commit_sequence: u64,
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub committed_envelopes: u64,
}

/// Result of the first vertical semantic transaction proof through real processes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellProcessPrototypeReport {
    pub seed: u64,
    pub mode: CellProcessPrototypeMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub process_starts: u64,
    pub process_kills: u64,
    pub committed_transactions: u64,
    pub durable_rejections: u64,
    pub duplicate_retries: u64,
    /// Exact converged transaction state retained by the final live quorum.
    pub final_cell: Option<CellStateSnapshot>,
    /// Highest applied transaction-log position covered by a durable authority snapshot.
    pub authority_snapshot_frontier: Option<u64>,
    pub steps: Vec<CellProcessPrototypeStep>,
    pub trace_sha256: String,
}

/// Bounded fault modes for the concurrent Cell v0 history checker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellConcurrentHistoryMode {
    Correct,
    OmitHotReadConflicts,
}

impl CellConcurrentHistoryMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitHotReadConflicts => "omit_hot_read_conflicts",
        }
    }
}

/// Canonical summary of one concurrent transaction history through real processes.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellConcurrentHistoryReport {
    pub seed: u64,
    pub mode: CellConcurrentHistoryMode,
    pub requested_transactions: u64,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub concurrent_rounds: u64,
    pub duplicate_retries: u64,
    pub process_starts: u64,
    pub process_kills: u64,
    pub read_observations: u64,
    pub actual_read_dependencies_checked: u64,
    pub real_time_edges_checked: u64,
    pub read_values_exact: bool,
    pub actual_read_dependencies_exact: bool,
    pub real_time_order_exact: bool,
    pub serializability_witness_valid: bool,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub answer: String,
    pub trace_sha256: String,
}

/// Fault modes for the bounded range-read and phantom history.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellRangePhantomMode {
    Correct,
    OmitRangeConflict,
}

impl CellRangePhantomMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitRangeConflict => "omit_range_conflict",
        }
    }
}

/// Canonical summary of one range-phantom history through real processes.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellRangePhantomReport {
    pub seed: u64,
    pub mode: CellRangePhantomMode,
    pub rounds: u64,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub range_observations: u64,
    pub point_observations: u64,
    pub dependency_edges_checked: u64,
    pub dependency_cycles: u64,
    pub range_reads_exact: bool,
    pub point_reads_exact: bool,
    pub phantom_conflicts_exact: bool,
    pub dependency_graph_acyclic: bool,
    pub all_nodes_exact: bool,
    pub envelope_chain_valid: bool,
    pub restarted_node_converges: bool,
    pub process_starts: u64,
    pub process_kills: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Fault modes for the bounded multi-proxy read-version history.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellReadVersionProxyMode {
    Correct,
    IgnoreSessionMinimum,
}

impl CellReadVersionProxyMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::IgnoreSessionMinimum => "ignore_session_minimum",
        }
    }
}

/// Canonical summary of one multi-proxy read-version history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellReadVersionProxyReport {
    pub seed: u64,
    pub mode: CellReadVersionProxyMode,
    pub rounds: u64,
    pub proxy_instances: u64,
    pub proxy_process_starts: u64,
    pub proxy_requests: u64,
    pub committed_transactions: u64,
    pub causal_handoffs: u64,
    pub read_observations: u64,
    pub minimum_version_violations: u64,
    pub stale_value_observations: u64,
    pub generations_exact: bool,
    pub minimum_versions_honored: bool,
    pub read_your_writes_exact: bool,
    pub real_time_order_exact: bool,
    pub all_nodes_exact: bool,
    pub envelope_chain_valid: bool,
    pub restarted_node_converges: bool,
    pub process_starts: u64,
    pub process_kills: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Exercise semantic OCC, multi-key atomicity, envelope production, failover,
/// retry, and retained-log replay through the existing three-process Raft path.
///
/// # Errors
///
/// Returns an error when the local process fixture cannot start or complete its
/// bounded protocol.
pub fn run_cell_process_prototype(
    seed: u64,
    mode: CellProcessPrototypeMode,
    executable: &Path,
) -> Result<CellProcessPrototypeReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let mut fixture = CellProcessFixture::start(seed, mode, executable)?;
    runtime.block_on(fixture.run_history())
}

/// One completed leader handoff while a bounded Cell v0 fixture remains live.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLeaderHandoff {
    pub killed_leader: NodeId,
    pub successor: NodeId,
}

/// Live Cell v0 process fixture used only by cross-role evaluation contracts.
///
/// Dropping the fixture terminates every child process and removes its local
/// state. Production code must not depend on this controller.
#[doc(hidden)]
pub struct CellProcessFixture<'a> {
    scenario: CellProcessScenario<'a>,
    history_ran: bool,
}

impl<'a> CellProcessFixture<'a> {
    /// Start an uninitialized bounded fixture controller.
    ///
    /// # Errors
    ///
    /// Returns an error when the executable or temporary process root is invalid.
    pub fn start(
        seed: u64,
        mode: CellProcessPrototypeMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        Ok(Self {
            scenario: CellProcessScenario::new(
                seed,
                mode,
                executable,
                ProcessNodePolicy::default(),
            )?,
            history_ran: false,
        })
    }

    /// Start a bounded fixture with explicit eval-only process policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the executable or temporary process root is invalid.
    #[doc(hidden)]
    pub fn start_with_policy(
        seed: u64,
        mode: CellProcessPrototypeMode,
        executable: &'a Path,
        policy: ProcessNodePolicy,
    ) -> Result<Self, String> {
        Ok(Self {
            scenario: CellProcessScenario::new(seed, mode, executable, policy)?,
            history_ran: false,
        })
    }

    /// Execute the frozen history while retaining the live authority processes.
    ///
    /// # Errors
    ///
    /// Returns an error when the history has already run or cannot complete.
    pub async fn run_history(&mut self) -> Result<CellProcessPrototypeReport, String> {
        if self.history_ran {
            return Err("Cell v0 fixture history may execute only once".to_owned());
        }
        self.history_ran = true;
        self.scenario.run().await
    }

    /// Current authority endpoints in stable node-id order.
    #[must_use]
    pub fn endpoints(&self) -> Vec<String> {
        self.scenario.addresses.values().cloned().collect()
    }

    /// Read the current visible cell state through a live linearizable authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the history is absent or no live authority endpoint
    /// serves a linearizable snapshot within the bounded retry budget.
    pub async fn linearizable_cell_snapshot(&self) -> Result<CellStateSnapshot, String> {
        if !self.history_ran {
            return Err("Cell v0 fixture history must run before snapshot reads".to_owned());
        }
        let endpoints = self.endpoints();
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            let endpoint = &endpoints[attempt % endpoints.len()];
            match linearizable_status(endpoint).await {
                Ok(status) => {
                    return Ok(status.cells.first().cloned().unwrap_or(CellStateSnapshot {
                        cell_id: CELL_ID,
                        tenant_id: TENANT_ID,
                        generation: 1,
                        ..CellStateSnapshot::default()
                    }));
                }
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "Cell v0 linearizable snapshot could not be read: {last}"
        ))
    }

    /// Replicate one evaluation-only sequencer marker through the live authority.
    ///
    /// The marker occupies a unique key in a separate tenant domain. Its returned
    /// commit sequence proves that the three-node authority durably ordered the
    /// exact marker bytes before a multi-proxy batch ticket is exposed.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture history is absent, the marker is empty,
    /// the authority rejects the write, or the live nodes do not converge.
    #[doc(hidden)]
    pub async fn replicate_sequencer_marker(
        &self,
        marker_id: u64,
        marker: &[u8],
    ) -> Result<u64, String> {
        if !self.history_ran {
            return Err("Cell v0 fixture history must run before sequencing markers".to_owned());
        }
        if marker.is_empty() {
            return Err("sequencer marker must not be empty".to_owned());
        }
        let mut key = b"okv-sequencer-ticket/".to_vec();
        key.extend(marker_id.to_be_bytes());
        let command = CellTransactionCommand {
            identity: RequestIdentity {
                client_id: self.scenario.seed ^ 0x5345_5155_454e_4345,
                request_id: 10_000_u64.saturating_add(marker_id),
            },
            credential: None,
            cell_id: CELL_ID,
            tenant_id: [0x33; 16],
            generation: 1,
            read_version: CellReadVersion::origin(),
            read_conflicts: Vec::new(),
            write_conflicts: vec![CellKeyRange::point(&key)],
            mutations: vec![CellMutation::Set {
                key,
                value: marker.to_vec(),
            }],
            partitioned_resolution: None,
            accepted_resolvers: vec![1, 2],
            durable_log_tags: vec![10, 20],
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let current = statuses(&self.scenario.addresses, &[1, 2, 3]).await?;
        let leader = current
            .iter()
            .find(|node| node.state == "leader")
            .map(|node| node.node_id)
            .or_else(|| current.iter().find_map(|node| node.leader))
            .ok_or_else(|| "sequencer authority has no visible leader".to_owned())?;
        let ack = retry_write(self.scenario.address(leader)?, command, false).await?;
        let sequence = committed_sequence(cell_outcome(&ack)?)?;
        if !wait_for_applied_convergence(&self.scenario.addresses, &[1, 2, 3]).await {
            return Err(
                "sequencer marker did not converge on every live authority node".to_owned(),
            );
        }
        Ok(sequence)
    }

    /// Kill the current leader after the frozen history and elect one successor.
    ///
    /// # Errors
    ///
    /// Returns an error when the history is absent, no leader is visible, or a
    /// surviving quorum cannot elect a successor.
    pub async fn kill_leader_and_elect_successor(&mut self) -> Result<CellLeaderHandoff, String> {
        if !self.history_ran {
            return Err("Cell v0 fixture history must run before leader handoff".to_owned());
        }
        let current = statuses(&self.scenario.addresses, &[1, 2, 3]).await?;
        let killed_leader = current
            .iter()
            .find(|node| node.state == "leader")
            .map(|node| node.node_id)
            .or_else(|| current.iter().find_map(|node| node.leader))
            .ok_or_else(|| "Cell v0 fixture has no visible leader".to_owned())?;
        self.scenario.kill_node(killed_leader)?;
        for successor in [1_u64, 2, 3]
            .into_iter()
            .filter(|node_id| *node_id != killed_leader)
        {
            if elect_until_leader(self.scenario.address(successor)?, successor).await {
                return Ok(CellLeaderHandoff {
                    killed_leader,
                    successor,
                });
            }
        }
        Err("surviving Cell v0 quorum did not elect a successor".to_owned())
    }

    /// Inspect whether any owned authority process exited unexpectedly.
    ///
    /// # Errors
    ///
    /// Returns an error when a child process status cannot be read.
    #[doc(hidden)]
    pub fn process_exit_statuses(&mut self) -> Result<Vec<(NodeId, Option<i32>)>, String> {
        self.scenario.children.exit_statuses()
    }
}

/// Run deterministic concurrent transaction shapes through one real three-process cell.
///
/// Each round submits four transactions that contend on one read/write conflict key,
/// four disjoint two-key transactions, and two blind writers to one key. One disjoint
/// transaction loses its reply at the midpoint and is recovered after killing the
/// leader. Ten transactions per round makes 100 rounds a 1,000-transaction history.
///
/// # Errors
///
/// Returns an error when the process fixture or bounded protocol cannot complete.
pub fn run_cell_concurrent_history(
    seed: u64,
    requested_transactions: u64,
    mode: CellConcurrentHistoryMode,
    executable: &Path,
) -> Result<CellConcurrentHistoryReport, String> {
    if requested_transactions == 0 || !requested_transactions.is_multiple_of(10) {
        return Err("concurrent history size must be a positive multiple of 10".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(
        CellConcurrentHistoryScenario::new(seed, requested_transactions, mode, executable)?.run(),
    )
}

/// Run a bounded range-read and phantom-dependency history through real processes.
///
/// # Errors
///
/// Returns an error when the process fixture or bounded protocol cannot complete.
pub fn run_cell_range_phantom_history(
    seed: u64,
    rounds: u64,
    mode: CellRangePhantomMode,
    executable: &Path,
) -> Result<CellRangePhantomReport, String> {
    if rounds == 0 {
        return Err("range phantom history requires at least one round".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(CellRangePhantomScenario::new(seed, rounds, mode, executable)?.run())
}

/// Run a bounded two-proxy read-version causality history through real processes.
///
/// # Errors
///
/// Returns an error when the process fixture or bounded protocol cannot complete.
pub fn run_cell_read_version_proxy_history(
    seed: u64,
    rounds: u64,
    mode: CellReadVersionProxyMode,
    executable: &Path,
) -> Result<CellReadVersionProxyReport, String> {
    if rounds == 0 {
        return Err("read-version proxy history requires at least one round".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(CellReadVersionProxyScenario::new(seed, rounds, mode, executable)?.run())
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct Observations {
    initial_multi_key_commit: bool,
    stale_read_conflict_rejected: bool,
    conflict_rejection_durable: bool,
    lost_reply_observed: bool,
    successor_elected: bool,
    retry_matches_durable_outcome: bool,
    conflicting_retry_rejected: bool,
    restarted_node_recovers: bool,
    all_nodes_exact: bool,
    envelope_chain_valid: bool,
    atomic_rows_exact: bool,
    durable_snapshot_persisted: bool,
    post_pop_retry_exact: bool,
    post_pop_commit_continues: bool,
    replacement_uses_fresh_node_identity: bool,
    learner_addition_committed: bool,
    authority_snapshot_installed_on_learner: bool,
    retained_suffix_replayed_after_snapshot: bool,
    learner_restart_exact: bool,
    retained_outcomes_exact_on_learner: bool,
    process_starts: u64,
    process_kills: u64,
    committed_transactions: u64,
    durable_rejections: u64,
    duplicate_retries: u64,
    final_cell: Option<CellStateSnapshot>,
    authority_snapshot_frontier: Option<u64>,
    steps: Vec<CellProcessPrototypeStep>,
}

struct CellProcessScenario<'a> {
    seed: u64,
    mode: CellProcessPrototypeMode,
    executable: &'a Path,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    policy: ProcessNodePolicy,
    observations: Observations,
}

impl<'a> CellProcessScenario<'a> {
    fn new(
        seed: u64,
        mode: CellProcessPrototypeMode,
        executable: &'a Path,
        policy: ProcessNodePolicy,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "prototype executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            executable,
            mode,
            root: TempRoot::new(seed, mode)?,
            addresses: allocate_addresses()?,
            children: ChildGroup::default(),
            policy,
            observations: Observations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self) -> Result<CellProcessPrototypeReport, String> {
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.address(1)?, 1).await {
            return Err("node 1 did not become the initial leader".to_owned());
        }

        let first = command(
            self.seed,
            1,
            CellReadVersion::origin(),
            &[],
            &[(b"a", b"100"), (b"z", b"200")],
        )?;
        let first_ack = retry_write(self.address(1)?, first, false).await?;
        let first_outcome = cell_outcome(&first_ack)?;
        let first_sequence = committed_sequence(first_outcome)?;
        self.observations.committed_transactions += 1;
        self.observations.initial_multi_key_commit = wait_for_exact_cell(
            &self.addresses,
            &[1, 2, 3],
            &[
                (b"a".to_vec(), b"100".to_vec()),
                (b"z".to_vec(), b"200".to_vec()),
            ],
            1,
        )
        .await;
        self.capture_step("initial_multi_key_commit", 1).await;

        let snapshot_one = CellReadVersion {
            generation: 1,
            sequence: first_sequence,
        };
        let winner = command(self.seed, 2, snapshot_one, &[b"a"], &[(b"a", b"110")])?;
        let winner_ack = retry_write(self.address(1)?, winner, false).await?;
        let winner_sequence = committed_sequence(cell_outcome(&winner_ack)?)?;
        self.observations.committed_transactions += 1;

        let stale = command(self.seed, 3, snapshot_one, &[b"a"], &[(b"a", b"120")])?;
        let stale_identity = request_identity(self.seed, 3);
        let stale_ack = retry_write(self.address(1)?, stale, false).await?;
        self.observations.stale_read_conflict_rejected =
            cell_outcome(&stale_ack)?.status == CellTransactionStatus::Conflict;
        self.observations.durable_rejections +=
            u64::from(self.observations.stale_read_conflict_rejected);
        self.observations.conflict_rejection_durable =
            wait_for_outcome(self.address(3)?, stale_identity)
                .await
                .and_then(|outcome| outcome.cell_transaction)
                .is_some_and(|outcome| outcome.status == CellTransactionStatus::Conflict);
        self.capture_step("stale_snapshot_conflict", 1).await;

        let snapshot_two = CellReadVersion {
            generation: 1,
            sequence: winner_sequence,
        };
        let lost = command(
            self.seed,
            4,
            snapshot_two,
            &[b"a", b"z"],
            &[(b"a", b"90"), (b"z", b"220")],
        )?;
        let lost_identity = request_identity(self.seed, 4);
        let dropped = write(self.address(1)?, lost.clone(), true).await;
        self.observations.lost_reply_observed = dropped.is_err();
        self.observations.committed_transactions +=
            u64::from(self.observations.lost_reply_observed);
        self.kill_node(1)?;

        self.observations.successor_elected = elect_until_leader(self.address(2)?, 2).await;
        let recovered = wait_for_outcome(self.address(2)?, lost_identity).await;
        let retry = retry_write(self.address(2)?, lost.clone(), false).await?;
        self.observations.duplicate_retries += 1;
        self.observations.retry_matches_durable_outcome = recovered
            .as_ref()
            .zip(retry.response.as_ref())
            .is_some_and(|(left, right)| left == right);

        let conflicting = command(
            self.seed,
            4,
            snapshot_two,
            &[b"a", b"z"],
            &[(b"a", b"999"), (b"z", b"999")],
        )?;
        self.observations.conflicting_retry_rejected =
            write(self.address(2)?, conflicting, false).await.is_err();
        self.observations.durable_rejections +=
            u64::from(self.observations.conflicting_retry_rejected);
        self.capture_step("successor_retry", 2).await;

        self.start_node(1)?;
        wait_ready(self.address(1)?).await?;
        retry_control(self.address(2)?, HEARTBEAT, &()).await?;
        let final_rows = [
            (b"a".to_vec(), b"90".to_vec()),
            (b"z".to_vec(), b"220".to_vec()),
        ];
        let cell_state_exact =
            wait_for_exact_cell(&self.addresses, &[1, 2, 3], &final_rows, 3).await;
        let applied_index_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3]).await;
        self.observations.all_nodes_exact = cell_state_exact && applied_index_exact;
        self.observations.restarted_node_recovers = self.observations.all_nodes_exact
            && wait_for_outcome(self.address(1)?, lost_identity).await == recovered;
        let statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let cells = statuses
            .iter()
            .filter_map(|status| status.cells.first())
            .cloned()
            .collect::<Vec<_>>();
        self.observations.atomic_rows_exact =
            cells.len() == 3 && cells.iter().all(|cell| cell.rows == final_rows);
        self.observations.envelope_chain_valid = cells.len() == 3
            && cells.windows(2).all(|pair| pair[0] == pair[1])
            && cells
                .first()
                .is_some_and(|cell| valid_envelope_chain(cell, 3));
        if matches!(
            self.mode,
            CellProcessPrototypeMode::Correct | CellProcessPrototypeMode::DisableDedup
        ) {
            self.observations.final_cell = cells.first().cloned();
        }
        self.capture_step("restarted_node_convergence", 1).await;

        match self.mode {
            CellProcessPrototypeMode::DurableSnapshotPop
            | CellProcessPrototypeMode::FreshLearnerRepair => {
                self.run_durable_snapshot_pop_probe(&lost, lost_identity, recovered.as_ref())
                    .await?;
            }
            CellProcessPrototypeMode::PurgeWithoutDurableSnapshot => {
                self.run_unsafe_pop_probe().await?;
            }
            CellProcessPrototypeMode::Correct
            | CellProcessPrototypeMode::DisableDedup
            | CellProcessPrototypeMode::LogOnlyLearnerAsRepair => {}
        }

        if matches!(
            self.mode,
            CellProcessPrototypeMode::FreshLearnerRepair
                | CellProcessPrototypeMode::LogOnlyLearnerAsRepair
        ) {
            self.run_fresh_learner_probe().await?;
        }

        Ok(build_report(self.seed, self.mode, &self.observations))
    }

    fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let nodes = if node_id == 4 {
            BTreeMap::from([(4, self.address(4)?.to_owned())])
        } else {
            (1..=3)
                .map(|member| {
                    self.address(member)
                        .map(|address| (member, address.to_owned()))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?
        };
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: self.mode != CellProcessPrototypeMode::DisableDedup,
                acknowledge_before_quorum: false,
                policy: self.policy.clone(),
            },
        )?;
        self.observations.process_starts += 1;
        Ok(())
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    async fn capture_step(&mut self, phase: &str, node_id: NodeId) {
        if let Ok(status) = status(self.address(node_id).unwrap_or_default()).await {
            let cell = status.cells.first().cloned().unwrap_or_default();
            self.observations.steps.push(CellProcessPrototypeStep {
                phase: phase.to_owned(),
                node_id,
                leader: status.leader,
                applied_log_index: status.last_applied_index,
                snapshot_log_index: status.snapshot_log_index,
                latest_commit_sequence: cell.latest_sequence,
                rows: cell.rows,
                committed_envelopes: cell.committed_envelopes.len() as u64,
            });
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_durable_snapshot_pop_probe(
        &mut self,
        lost: &[u8],
        lost_identity: RequestIdentity,
        recovered_before_pop: Option<&ApplyResponse>,
    ) -> Result<(), String> {
        let before = statuses(&self.addresses, &[1, 2, 3]).await?;
        let snapshot_position = before
            .first()
            .and_then(|node| node.last_applied_index)
            .ok_or_else(|| "missing applied position before durable snapshot".to_owned())?;
        if !before
            .iter()
            .all(|node| node.last_applied_index == Some(snapshot_position))
        {
            return Err("nodes did not converge before durable snapshot".to_owned());
        }
        for node_id in 1..=3 {
            retry_control(self.address(node_id)?, TRIGGER_SNAPSHOT, &()).await?;
        }
        let snapshot_status =
            wait_for_snapshot_convergence(&self.addresses, &[1, 2, 3], snapshot_position).await;
        let snapshot_files = (1..=3).all(|node_id| {
            fs::metadata(self.root.node(node_id).join("state-machine.snapshot"))
                .is_ok_and(|metadata| metadata.len() > 0)
        });
        self.observations.durable_snapshot_persisted = snapshot_status && snapshot_files;
        if self.observations.durable_snapshot_persisted {
            self.observations.authority_snapshot_frontier = Some(snapshot_position);
        }
        self.capture_step("durable_snapshot_persisted", 2).await;
        if !self.observations.durable_snapshot_persisted {
            let observed = statuses(&self.addresses, &[1, 2, 3]).await?;
            return Err(format!(
                "durable snapshots did not cover applied position {snapshot_position}: {observed:?}, files={snapshot_files}"
            ));
        }

        for node_id in 1..=3 {
            self.kill_node(node_id)?;
        }
        for node_id in 1..=3 {
            purge_retained_log(&self.root.node(node_id)).await?;
        }
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        if !elect_until_leader(self.address(2)?, 2).await {
            return Err("node 2 did not become leader after durable snapshot restore".to_owned());
        }

        let restored = statuses(&self.addresses, &[1, 2, 3]).await?;
        let restored_snapshot = restored
            .iter()
            .all(|node| node.snapshot_log_index == Some(snapshot_position));
        let retried = retry_write(self.address(2)?, lost.to_vec(), false).await?;
        self.observations.duplicate_retries += 1;
        self.observations.post_pop_retry_exact = restored_snapshot
            && recovered_before_pop
                .zip(retried.response.as_ref())
                .is_some_and(|(left, right)| left == right)
            && wait_for_outcome(self.address(1)?, lost_identity)
                .await
                .as_ref()
                == recovered_before_pop;
        self.capture_step("post_pop_retry_exact", 2).await;

        let latest = status(self.address(2)?)
            .await?
            .cells
            .first()
            .map(|cell| CellReadVersion {
                generation: cell.generation,
                sequence: cell.latest_sequence,
            })
            .ok_or_else(|| "missing restored cell state".to_owned())?;
        let after_pop = command(
            self.seed,
            5,
            latest,
            &[b"a", b"z"],
            &[(b"a", b"80"), (b"z", b"240")],
        )?;
        let after_pop_ack = retry_write(self.address(2)?, after_pop, false).await?;
        committed_sequence(cell_outcome(&after_pop_ack)?)?;
        self.observations.committed_transactions += 1;
        let final_rows = [
            (b"a".to_vec(), b"80".to_vec()),
            (b"z".to_vec(), b"240".to_vec()),
        ];
        let rows_exact = wait_for_exact_cell(&self.addresses, &[1, 2, 3], &final_rows, 4).await;
        let applied_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3]).await;
        let final_statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let final_cells = final_statuses
            .iter()
            .filter_map(|node| node.cells.first())
            .collect::<Vec<_>>();
        let final_chain_exact = final_cells.len() == 3
            && final_cells.windows(2).all(|pair| pair[0] == pair[1])
            && final_cells
                .first()
                .is_some_and(|cell| valid_envelope_chain(cell, 4));
        self.observations.post_pop_commit_continues =
            rows_exact && applied_exact && final_chain_exact;
        if self.observations.post_pop_commit_continues {
            self.observations.final_cell = final_cells.first().copied().cloned();
        }
        self.capture_step("post_pop_commit_continues", 2).await;
        Ok(())
    }

    async fn run_unsafe_pop_probe(&mut self) -> Result<(), String> {
        for node_id in 1..=3 {
            self.kill_node(node_id)?;
        }
        for node_id in 1..=3 {
            purge_retained_log(&self.root.node(node_id)).await?;
        }
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        let mut all_ready = true;
        for node_id in 1..=3 {
            if wait_ready(self.address(node_id)?).await.is_err() {
                all_ready = false;
                break;
            }
        }
        if !all_ready {
            self.observations.restarted_node_recovers = false;
            self.observations.all_nodes_exact = false;
            self.observations.envelope_chain_valid = false;
            self.observations.atomic_rows_exact = false;
            self.observations.final_cell = None;
            self.observations.steps.push(CellProcessPrototypeStep {
                phase: "unsafe_pop_restart_unavailable".to_owned(),
                node_id: 1,
                leader: None,
                applied_log_index: None,
                snapshot_log_index: None,
                latest_commit_sequence: 0,
                rows: Vec::new(),
                committed_envelopes: 0,
            });
            return Ok(());
        }
        let statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let state_was_lost = statuses
            .iter()
            .all(|status| status.cells.first().is_none_or(|cell| cell.rows.is_empty()));
        if state_was_lost {
            self.observations.restarted_node_recovers = false;
            self.observations.all_nodes_exact = false;
            self.observations.envelope_chain_valid = false;
            self.observations.atomic_rows_exact = false;
            self.observations.final_cell = None;
        }
        self.capture_step("unsafe_pop_without_snapshot", 1).await;
        Ok(())
    }

    async fn run_fresh_learner_probe(&mut self) -> Result<(), String> {
        let source_before = status(self.address(2)?).await?;
        let expected_cell = source_before
            .cells
            .first()
            .cloned()
            .ok_or_else(|| "repair source has no Cell v0 state".to_owned())?;
        let expected_snapshot = self.observations.authority_snapshot_frontier;
        let expected_envelopes = expected_cell.committed_envelopes.len();

        self.start_node(4)?;
        wait_ready(self.address(4)?).await?;
        self.observations.replacement_uses_fresh_node_identity =
            status(self.address(4)?).await?.node_id == 4;
        let learner = add_learner(
            self.address(2)?,
            AddLearnerRequest {
                node_id: 4,
                address: self.address(4)?.to_owned(),
            },
        )
        .await?;
        self.observations.learner_addition_committed = learner.committed;
        retry_control(self.address(2)?, HEARTBEAT, &()).await?;

        let expected_rows = expected_cell.rows.clone();
        let rows_exact =
            wait_for_exact_cell(&self.addresses, &[4], &expected_rows, expected_envelopes).await;
        let applied_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3, 4]).await;
        let caught_up = status(self.address(4)?).await?;
        let caught_up_cell = caught_up.cells.first();
        self.observations.authority_snapshot_installed_on_learner = expected_snapshot
            .is_some_and(|snapshot| caught_up.snapshot_log_index == Some(snapshot));
        self.observations.retained_suffix_replayed_after_snapshot = self
            .observations
            .authority_snapshot_installed_on_learner
            && rows_exact
            && applied_exact
            && caught_up_cell == Some(&expected_cell)
            && caught_up_cell.is_some_and(|cell| valid_envelope_chain(cell, expected_envelopes));

        self.kill_node(4)?;
        self.start_node(4)?;
        wait_ready(self.address(4)?).await?;
        retry_control(self.address(2)?, HEARTBEAT, &()).await?;
        let restarted_rows =
            wait_for_exact_cell(&self.addresses, &[4], &expected_rows, expected_envelopes).await;
        let restarted = status(self.address(4)?).await?;
        self.observations.learner_restart_exact = restarted_rows
            && restarted.cells.first() == Some(&expected_cell)
            && restarted.snapshot_log_index == expected_snapshot;

        let mut outcome_identities = vec![request_identity(self.seed, 4)];
        if expected_envelopes == 4 {
            outcome_identities.push(request_identity(self.seed, 5));
        }
        let mut outcomes_exact = true;
        for identity in outcome_identities {
            let source_outcome = wait_for_outcome(self.address(2)?, identity).await;
            outcomes_exact &= source_outcome.is_some()
                && wait_for_outcome(self.address(4)?, identity).await == source_outcome;
        }
        self.observations.retained_outcomes_exact_on_learner = outcomes_exact;
        self.capture_step("fresh_learner_snapshot_plus_suffix", 4)
            .await;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConcurrentShape {
    Hot,
    Disjoint,
    Blind,
}

#[derive(Clone, Debug)]
struct PlannedConcurrentTransaction {
    round: u64,
    shape: ConcurrentShape,
    identity: RequestIdentity,
    read_version: CellReadVersion,
    observed_reads: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    payload: Vec<u8>,
    sets: Vec<(Vec<u8>, Vec<u8>)>,
    drop_reply: bool,
}

#[derive(Clone, Debug)]
struct CompletedConcurrentTransaction {
    plan: PlannedConcurrentTransaction,
    status: CellTransactionStatus,
    commit_sequence: Option<u64>,
}

#[allow(clippy::struct_excessive_bools)]
struct ConcurrentHistoryObservations {
    attempted_transactions: u64,
    committed_transactions: u64,
    conflict_rejections: u64,
    concurrent_rounds: u64,
    duplicate_retries: u64,
    process_starts: u64,
    process_kills: u64,
    read_observations: u64,
    actual_read_dependencies_checked: u64,
    real_time_edges_checked: u64,
    read_values_exact: bool,
    actual_read_dependencies_exact: bool,
    real_time_order_exact: bool,
    serializability_witness_valid: bool,
    hot_conflict_batches_exact: bool,
    disjoint_atomic_batches_exact: bool,
    blind_write_batches_exact: bool,
    batch_state_exact: bool,
    commit_sequences_unique: bool,
    lost_reply_observed: bool,
    successor_elected: bool,
    retry_matches_durable_outcome: bool,
    restarted_node_recovers: bool,
    all_nodes_exact: bool,
    envelope_chain_valid: bool,
}

impl Default for ConcurrentHistoryObservations {
    fn default() -> Self {
        Self {
            attempted_transactions: 0,
            committed_transactions: 0,
            conflict_rejections: 0,
            concurrent_rounds: 0,
            duplicate_retries: 0,
            process_starts: 0,
            process_kills: 0,
            read_observations: 0,
            actual_read_dependencies_checked: 0,
            real_time_edges_checked: 0,
            read_values_exact: true,
            actual_read_dependencies_exact: true,
            real_time_order_exact: true,
            serializability_witness_valid: true,
            hot_conflict_batches_exact: true,
            disjoint_atomic_batches_exact: true,
            blind_write_batches_exact: true,
            batch_state_exact: true,
            commit_sequences_unique: true,
            lost_reply_observed: false,
            successor_elected: false,
            retry_matches_durable_outcome: false,
            restarted_node_recovers: false,
            all_nodes_exact: false,
            envelope_chain_valid: false,
        }
    }
}

struct CellConcurrentHistoryScenario<'a> {
    seed: u64,
    requested_transactions: u64,
    mode: CellConcurrentHistoryMode,
    executable: &'a Path,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: ConcurrentHistoryObservations,
}

impl<'a> CellConcurrentHistoryScenario<'a> {
    fn new(
        seed: u64,
        requested_transactions: u64,
        mode: CellConcurrentHistoryMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "concurrent history executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            requested_transactions,
            mode,
            executable,
            root: TempRoot::new_with_label(seed, &format!("concurrent-{}", mode.id()))?,
            addresses: allocate_addresses()?,
            children: ChildGroup::default(),
            observations: ConcurrentHistoryObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<CellConcurrentHistoryReport, String> {
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.address(1)?, 1).await {
            return Err("node 1 did not become the concurrent history leader".to_owned());
        }

        let rounds = self.requested_transactions / 10;
        let failover_round = rounds / 2;
        let mut leader = 1;
        let mut killed_leader = None;
        let mut expected_rows: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut committed_sequences = std::collections::BTreeSet::new();
        let mut recovered_lost_outcome = None;
        let mut lost_identity = None;
        let mut history =
            Vec::with_capacity(usize::try_from(self.requested_transactions).unwrap_or(usize::MAX));

        for round in 0..rounds {
            let read_snapshot = self.linearizable_snapshot(leader).await?;
            let plans = self.plans(round, &read_snapshot, round == failover_round)?;
            let mut tasks = JoinSet::new();
            for plan in plans {
                let address = self.address(leader)?.to_owned();
                tasks.spawn(async move {
                    let result = if plan.drop_reply {
                        write(&address, plan.payload.clone(), true).await
                    } else {
                        retry_write(&address, plan.payload.clone(), false).await
                    };
                    (plan, result)
                });
            }

            let mut completed = Vec::new();
            let mut lost_plan = None;
            while let Some(joined) = tasks.join_next().await {
                let (plan, result) = joined.map_err(|error| error.to_string())?;
                self.observations.attempted_transactions += 1;
                if plan.drop_reply {
                    self.observations.lost_reply_observed = result.is_err();
                    lost_identity = Some(plan.identity);
                    lost_plan = Some(plan);
                    continue;
                }
                let ack = result?;
                let outcome = cell_outcome(&ack)?;
                self.record_outcome(
                    outcome.status,
                    outcome.commit_sequence,
                    &mut committed_sequences,
                );
                completed.push(CompletedConcurrentTransaction {
                    plan,
                    status: outcome.status,
                    commit_sequence: outcome.commit_sequence,
                });
            }

            if let Some(plan) = lost_plan {
                self.kill_node(leader)?;
                killed_leader = Some(leader);
                leader = if leader == 1 { 2 } else { 1 };
                self.observations.successor_elected =
                    elect_until_leader(self.address(leader)?, leader).await;
                let recovered = wait_for_outcome(self.address(leader)?, plan.identity).await;
                let retry = retry_write(self.address(leader)?, plan.payload.clone(), false).await?;
                self.observations.duplicate_retries += 1;
                self.observations.retry_matches_durable_outcome = recovered
                    .as_ref()
                    .zip(retry.response.as_ref())
                    .is_some_and(|(left, right)| left == right);
                let recovered_cell = recovered
                    .as_ref()
                    .and_then(|response| response.cell_transaction.as_ref())
                    .ok_or_else(|| "successor omitted the lost transaction outcome".to_owned())?;
                self.record_outcome(
                    recovered_cell.status,
                    recovered_cell.commit_sequence,
                    &mut committed_sequences,
                );
                completed.push(CompletedConcurrentTransaction {
                    plan,
                    status: recovered_cell.status,
                    commit_sequence: recovered_cell.commit_sequence,
                });
                recovered_lost_outcome = recovered;
            }

            self.check_round(leader, &completed, &mut expected_rows)
                .await?;
            history.extend(completed);
            self.observations.concurrent_rounds += 1;
        }

        let witness = check_serializability_witness(&history);
        self.observations.read_observations = witness.read_observations;
        self.observations.actual_read_dependencies_checked =
            witness.actual_read_dependencies_checked;
        self.observations.real_time_edges_checked = witness.real_time_edges_checked;
        self.observations.read_values_exact = witness.read_values_exact;
        self.observations.actual_read_dependencies_exact = witness.actual_read_dependencies_exact;
        self.observations.real_time_order_exact = witness.real_time_order_exact;
        self.observations.serializability_witness_valid = witness.valid();

        self.observations.commit_sequences_unique = committed_sequences.len()
            == usize::try_from(self.observations.committed_transactions).unwrap_or(usize::MAX);
        let killed_leader =
            killed_leader.ok_or_else(|| "history omitted leader failure".to_owned())?;
        self.start_node(killed_leader)?;
        wait_ready(self.address(killed_leader)?).await?;
        retry_control(self.address(leader)?, HEARTBEAT, &()).await?;

        let expected_rows = expected_rows.into_iter().collect::<Vec<_>>();
        let expected_envelopes = usize::try_from(self.observations.committed_transactions)
            .map_err(|error| error.to_string())?;
        let cell_state_exact = wait_for_exact_cell(
            &self.addresses,
            &[1, 2, 3],
            &expected_rows,
            expected_envelopes,
        )
        .await;
        let applied_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3]).await;
        self.observations.all_nodes_exact = cell_state_exact && applied_exact;
        let statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let cells = statuses
            .iter()
            .filter_map(|node| node.cells.first())
            .collect::<Vec<_>>();
        self.observations.envelope_chain_valid = cells.len() == 3
            && cells.windows(2).all(|pair| pair[0] == pair[1])
            && cells
                .first()
                .is_some_and(|cell| valid_envelope_chain(cell, expected_envelopes));
        let restarted_outcome = if let Some(identity) = lost_identity {
            wait_for_outcome(self.address(killed_leader)?, identity).await
        } else {
            None
        };
        self.observations.restarted_node_recovers =
            recovered_lost_outcome.is_some() && restarted_outcome == recovered_lost_outcome;

        Ok(build_concurrent_history_report(
            self.seed,
            self.requested_transactions,
            self.mode,
            &self.observations,
        ))
    }

    fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let nodes = (1..=3)
            .map(|member| {
                self.address(member)
                    .map(|address| (member, address.to_owned()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy: ProcessNodePolicy::default(),
            },
        )?;
        self.observations.process_starts += 1;
        Ok(())
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    async fn linearizable_snapshot(&self, leader: NodeId) -> Result<CellStateSnapshot, String> {
        let leader_status = linearizable_status(self.address(leader)?).await?;
        Ok(leader_status
            .cells
            .first()
            .cloned()
            .unwrap_or(CellStateSnapshot {
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                generation: 1,
                ..CellStateSnapshot::default()
            }))
    }

    fn plans(
        &self,
        round: u64,
        read_snapshot: &CellStateSnapshot,
        inject_lost_reply: bool,
    ) -> Result<Vec<PlannedConcurrentTransaction>, String> {
        let read_version = CellReadVersion {
            generation: read_snapshot.generation,
            sequence: read_snapshot.latest_sequence,
        };
        let observed_rows = read_snapshot
            .rows
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>();
        let mut plans = Vec::with_capacity(10);
        for slot in 0..4_u64 {
            let key = format!("hot/{:02}", (self.seed ^ round) % 17).into_bytes();
            let value = format!("round-{round:04}-hot-{slot}").into_bytes();
            let read_keys = if self.mode == CellConcurrentHistoryMode::Correct {
                vec![key.clone()]
            } else {
                Vec::new()
            };
            plans.push(self.plan(
                round,
                slot,
                ConcurrentShape::Hot,
                read_version,
                &read_keys,
                vec![(key.clone(), observed_rows.get(&key).cloned())],
                vec![(key, value)],
                false,
            )?);
        }
        for disjoint in 0..4_u64 {
            let slot = 4 + disjoint;
            let left = format!("a/range-{round:04}-{disjoint}").into_bytes();
            let right = format!("z/range-{round:04}-{disjoint}").into_bytes();
            plans.push(self.plan(
                round,
                slot,
                ConcurrentShape::Disjoint,
                read_version,
                &[],
                Vec::new(),
                vec![
                    (left, format!("left-{round:04}-{disjoint}").into_bytes()),
                    (right, format!("right-{round:04}-{disjoint}").into_bytes()),
                ],
                inject_lost_reply && disjoint == 0,
            )?);
        }
        let blind_key = format!("m/blind-{round:04}").into_bytes();
        for blind in 0..2_u64 {
            let slot = 8 + blind;
            plans.push(self.plan(
                round,
                slot,
                ConcurrentShape::Blind,
                read_version,
                &[],
                Vec::new(),
                vec![(
                    blind_key.clone(),
                    format!("blind-{round:04}-{blind}").into_bytes(),
                )],
                false,
            )?);
        }
        Ok(plans)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan(
        &self,
        round: u64,
        slot: u64,
        shape: ConcurrentShape,
        read_version: CellReadVersion,
        read_keys: &[Vec<u8>],
        observed_reads: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        sets: Vec<(Vec<u8>, Vec<u8>)>,
        drop_reply: bool,
    ) -> Result<PlannedConcurrentTransaction, String> {
        let identity = request_identity(self.seed, round.saturating_mul(10) + slot + 1);
        let payload = owned_command(identity, read_version, read_keys, &sets)?;
        Ok(PlannedConcurrentTransaction {
            round,
            shape,
            identity,
            read_version,
            observed_reads,
            payload,
            sets,
            drop_reply,
        })
    }

    fn record_outcome(
        &mut self,
        status: CellTransactionStatus,
        sequence: Option<u64>,
        sequences: &mut std::collections::BTreeSet<u64>,
    ) {
        match status {
            CellTransactionStatus::Committed => {
                self.observations.committed_transactions += 1;
                if let Some(sequence) = sequence {
                    if !sequences.insert(sequence) {
                        self.observations.commit_sequences_unique = false;
                    }
                } else {
                    self.observations.commit_sequences_unique = false;
                }
            }
            CellTransactionStatus::Conflict => self.observations.conflict_rejections += 1,
            _ => {}
        }
    }

    async fn check_round(
        &mut self,
        leader: NodeId,
        completed: &[CompletedConcurrentTransaction],
        expected_rows: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), String> {
        let leader_status = status(self.address(leader)?).await?;
        let cell = leader_status
            .cells
            .first()
            .ok_or_else(|| "leader omitted concurrent cell state".to_owned())?;
        let actual_rows = cell.rows.iter().cloned().collect::<BTreeMap<_, _>>();

        let hot = completed
            .iter()
            .filter(|transaction| transaction.plan.shape == ConcurrentShape::Hot)
            .collect::<Vec<_>>();
        let hot_committed = hot
            .iter()
            .copied()
            .filter(|transaction| transaction.status == CellTransactionStatus::Committed)
            .collect::<Vec<_>>();
        let hot_conflicts = hot
            .iter()
            .filter(|transaction| transaction.status == CellTransactionStatus::Conflict)
            .count();
        let latest_hot = latest_committed(&hot_committed);
        let hot_value_exact = latest_hot.is_some_and(|transaction| {
            transaction.plan.sets.first().is_some_and(|(key, value)| {
                actual_rows.get(key).is_some_and(|actual| actual == value)
            })
        });
        self.observations.hot_conflict_batches_exact &=
            hot_committed.len() == 1 && hot_conflicts == 3 && hot_value_exact;
        if let Some(transaction) = latest_hot {
            apply_expected(&transaction.plan.sets, expected_rows);
        }

        let disjoint = completed
            .iter()
            .filter(|transaction| transaction.plan.shape == ConcurrentShape::Disjoint)
            .collect::<Vec<_>>();
        let disjoint_exact = disjoint.len() == 4
            && disjoint.iter().all(|transaction| {
                transaction.status == CellTransactionStatus::Committed
                    && transaction.plan.sets.iter().all(|(key, value)| {
                        actual_rows.get(key).is_some_and(|actual| actual == value)
                    })
            });
        self.observations.disjoint_atomic_batches_exact &= disjoint_exact;
        for transaction in disjoint {
            if transaction.status == CellTransactionStatus::Committed {
                apply_expected(&transaction.plan.sets, expected_rows);
            }
        }

        let blind = completed
            .iter()
            .filter(|transaction| transaction.plan.shape == ConcurrentShape::Blind)
            .collect::<Vec<_>>();
        let blind_committed = blind
            .iter()
            .copied()
            .filter(|transaction| transaction.status == CellTransactionStatus::Committed)
            .collect::<Vec<_>>();
        let latest_blind = latest_committed(&blind_committed);
        let blind_exact = blind_committed.len() == 2
            && latest_blind.is_some_and(|transaction| {
                transaction.plan.sets.first().is_some_and(|(key, value)| {
                    actual_rows.get(key).is_some_and(|actual| actual == value)
                })
            });
        self.observations.blind_write_batches_exact &= blind_exact;
        if let Some(transaction) = latest_blind {
            apply_expected(&transaction.plan.sets, expected_rows);
        }
        self.observations.batch_state_exact &= actual_rows == *expected_rows;
        Ok(())
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RangePhantomObservations {
    attempted_transactions: u64,
    committed_transactions: u64,
    conflict_rejections: u64,
    range_observations: u64,
    point_observations: u64,
    dependency_edges_checked: u64,
    dependency_cycles: u64,
    range_reads_exact: bool,
    point_reads_exact: bool,
    phantom_conflicts_exact: bool,
    dependency_graph_acyclic: bool,
    successor_elected: bool,
    all_nodes_exact: bool,
    envelope_chain_valid: bool,
    restarted_node_converges: bool,
    process_starts: u64,
    process_kills: u64,
}

impl Default for RangePhantomObservations {
    fn default() -> Self {
        Self {
            attempted_transactions: 0,
            committed_transactions: 0,
            conflict_rejections: 0,
            range_observations: 0,
            point_observations: 0,
            dependency_edges_checked: 0,
            dependency_cycles: 0,
            range_reads_exact: true,
            point_reads_exact: true,
            phantom_conflicts_exact: true,
            dependency_graph_acyclic: true,
            successor_elected: false,
            all_nodes_exact: false,
            envelope_chain_valid: false,
            restarted_node_converges: false,
            process_starts: 0,
            process_kills: 0,
        }
    }
}

struct CellRangePhantomScenario<'a> {
    seed: u64,
    rounds: u64,
    mode: CellRangePhantomMode,
    executable: &'a Path,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: RangePhantomObservations,
}

impl<'a> CellRangePhantomScenario<'a> {
    fn new(
        seed: u64,
        rounds: u64,
        mode: CellRangePhantomMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "range phantom executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            rounds,
            mode,
            executable,
            root: TempRoot::new_with_label(seed, &format!("range-phantom-{}", mode.id()))?,
            addresses: allocate_addresses()?,
            children: ChildGroup::default(),
            observations: RangePhantomObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<CellRangePhantomReport, String> {
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.address(1)?, 1).await {
            return Err("node 1 did not become the range phantom leader".to_owned());
        }

        let failover_round = self.rounds / 2;
        let mut leader = 1;
        let mut killed_leader = None;
        let mut expected_rows: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut expected_envelopes = 0_usize;

        for round in 0..self.rounds {
            let snapshot = self.linearizable_snapshot(leader).await?;
            let snapshot_rows = snapshot.rows.iter().cloned().collect::<BTreeMap<_, _>>();
            let read_version = CellReadVersion {
                generation: snapshot.generation,
                sequence: snapshot.latest_sequence,
            };
            let prefix = format!("phantom/{:016x}/{round:04}/", self.seed).into_bytes();
            let mut range_end = prefix.clone();
            range_end.push(0xff);
            let range = CellKeyRange {
                start: prefix.clone(),
                end: range_end,
            };
            let item_key = [prefix.as_slice(), b"inserted"].concat();
            let summary_key = format!("summary/{:016x}/{round:04}", self.seed).into_bytes();
            let observed_range = snapshot_rows
                .range(range.start.clone()..range.end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            let expected_range = expected_rows
                .range(range.start.clone()..range.end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            self.observations.range_observations += 1;
            self.observations.point_observations += 1;
            self.observations.range_reads_exact &= observed_range == expected_range;
            self.observations.point_reads_exact &=
                snapshot_rows.get(&summary_key) == expected_rows.get(&summary_key);

            let summary_value = format!("count={}", observed_range.len()).into_bytes();
            let range_conflicts = if self.mode == CellRangePhantomMode::Correct {
                vec![range]
            } else {
                observed_range
                    .iter()
                    .map(|(key, _)| CellKeyRange::point(key))
                    .collect()
            };
            let range_payload = owned_command_with_ranges(
                request_identity(self.seed, round.saturating_mul(2) + 2),
                read_version,
                range_conflicts,
                &[(summary_key.clone(), summary_value.clone())],
            )?;
            let insert_payload = owned_command_with_ranges(
                request_identity(self.seed, round.saturating_mul(2) + 1),
                read_version,
                vec![CellKeyRange::point(&summary_key)],
                &[(item_key.clone(), b"present".to_vec())],
            )?;

            let insert_ack = retry_write(self.address(leader)?, insert_payload, false).await?;
            self.observations.attempted_transactions += 1;
            let insert_outcome = cell_outcome(&insert_ack)?;
            let insert_committed = insert_outcome.status == CellTransactionStatus::Committed;
            if insert_committed {
                self.observations.committed_transactions += 1;
                expected_envelopes += 1;
                expected_rows.insert(item_key, b"present".to_vec());
            }

            if round == failover_round {
                self.kill_node(leader)?;
                killed_leader = Some(leader);
                leader = if leader == 1 { 2 } else { 1 };
                self.observations.successor_elected =
                    elect_until_leader(self.address(leader)?, leader).await;
            }

            let range_ack = retry_write(self.address(leader)?, range_payload, false).await?;
            self.observations.attempted_transactions += 1;
            let range_outcome = cell_outcome(&range_ack)?;
            let range_committed = range_outcome.status == CellTransactionStatus::Committed;
            if range_committed {
                self.observations.committed_transactions += 1;
                expected_envelopes += 1;
                expected_rows.insert(summary_key, summary_value);
            } else if range_outcome.status == CellTransactionStatus::Conflict {
                self.observations.conflict_rejections += 1;
            }

            self.observations.dependency_edges_checked += 2;
            let dependency_cycle = insert_committed && range_committed;
            self.observations.dependency_cycles += u64::from(dependency_cycle);
            self.observations.dependency_graph_acyclic &= !dependency_cycle;
            self.observations.phantom_conflicts_exact &=
                insert_committed && range_outcome.status == CellTransactionStatus::Conflict;
        }

        let killed_leader = killed_leader
            .ok_or_else(|| "range phantom history omitted leader failure".to_owned())?;
        self.start_node(killed_leader)?;
        wait_ready(self.address(killed_leader)?).await?;
        retry_control(self.address(leader)?, HEARTBEAT, &()).await?;

        let expected_rows = expected_rows.into_iter().collect::<Vec<_>>();
        let cell_state_exact = wait_for_exact_cell(
            &self.addresses,
            &[1, 2, 3],
            &expected_rows,
            expected_envelopes,
        )
        .await;
        let applied_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3]).await;
        self.observations.all_nodes_exact = cell_state_exact && applied_exact;
        self.observations.restarted_node_converges = self.observations.all_nodes_exact;
        let statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let cells = statuses
            .iter()
            .filter_map(|node| node.cells.first())
            .collect::<Vec<_>>();
        self.observations.envelope_chain_valid = cells.len() == 3
            && cells.windows(2).all(|pair| pair[0] == pair[1])
            && cells
                .first()
                .is_some_and(|cell| valid_envelope_chain(cell, expected_envelopes));

        Ok(build_range_phantom_report(
            self.seed,
            self.rounds,
            self.mode,
            &self.observations,
        ))
    }

    fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let nodes = (1..=3)
            .map(|member| {
                self.address(member)
                    .map(|address| (member, address.to_owned()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy: ProcessNodePolicy::default(),
            },
        )?;
        self.observations.process_starts += 1;
        Ok(())
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    async fn linearizable_snapshot(&self, leader: NodeId) -> Result<CellStateSnapshot, String> {
        let leader_status = linearizable_status(self.address(leader)?).await?;
        Ok(leader_status
            .cells
            .first()
            .cloned()
            .unwrap_or(CellStateSnapshot {
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                generation: 1,
                ..CellStateSnapshot::default()
            }))
    }
}

#[allow(clippy::too_many_lines)]
fn build_range_phantom_report(
    seed: u64,
    rounds: u64,
    mode: CellRangePhantomMode,
    observations: &RangePhantomObservations,
) -> CellRangePhantomReport {
    let checks = [
        (
            "requested_transactions_executed",
            observations.attempted_transactions == rounds.saturating_mul(2),
        ),
        (
            "expected_outcome_counts",
            observations.committed_transactions == rounds
                && observations.conflict_rejections == rounds,
        ),
        ("range_reads_exact", observations.range_reads_exact),
        ("point_reads_exact", observations.point_reads_exact),
        (
            "dependency_edges_checked",
            observations.dependency_edges_checked == rounds.saturating_mul(2),
        ),
        (
            "phantom_conflicts_exact",
            observations.phantom_conflicts_exact,
        ),
        (
            "dependency_graph_acyclic",
            observations.dependency_graph_acyclic,
        ),
        ("successor_elected", observations.successor_elected),
        ("all_nodes_exact", observations.all_nodes_exact),
        ("envelope_chain_valid", observations.envelope_chain_valid),
        (
            "restarted_node_converges",
            observations.restarted_node_converges,
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-range-phantom-v1");
    trace.update(seed.to_be_bytes());
    trace.update(rounds.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(observations.attempted_transactions.to_be_bytes());
    trace.update(observations.committed_transactions.to_be_bytes());
    trace.update(observations.conflict_rejections.to_be_bytes());
    trace.update(observations.dependency_edges_checked.to_be_bytes());
    trace.update(observations.dependency_cycles.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    CellRangePhantomReport {
        seed,
        mode,
        rounds,
        attempted_transactions: observations.attempted_transactions,
        committed_transactions: observations.committed_transactions,
        conflict_rejections: observations.conflict_rejections,
        range_observations: observations.range_observations,
        point_observations: observations.point_observations,
        dependency_edges_checked: observations.dependency_edges_checked,
        dependency_cycles: observations.dependency_cycles,
        range_reads_exact: observations.range_reads_exact,
        point_reads_exact: observations.point_reads_exact,
        phantom_conflicts_exact: observations.phantom_conflicts_exact,
        dependency_graph_acyclic: observations.dependency_graph_acyclic,
        all_nodes_exact: observations.all_nodes_exact,
        envelope_chain_valid: observations.envelope_chain_valid,
        restarted_node_converges: observations.restarted_node_converges,
        process_starts: observations.process_starts,
        process_kills: observations.process_kills,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

#[allow(clippy::struct_excessive_bools)]
struct ReadVersionProxyObservations {
    proxy_process_starts: u64,
    proxy_requests: u64,
    committed_transactions: u64,
    causal_handoffs: u64,
    read_observations: u64,
    minimum_version_violations: u64,
    stale_value_observations: u64,
    generations_exact: bool,
    precommit_versions_exact: bool,
    minimum_versions_honored: bool,
    read_your_writes_exact: bool,
    real_time_order_exact: bool,
    successor_elected: bool,
    all_nodes_exact: bool,
    envelope_chain_valid: bool,
    restarted_node_converges: bool,
    process_starts: u64,
    process_kills: u64,
}

impl Default for ReadVersionProxyObservations {
    fn default() -> Self {
        Self {
            proxy_process_starts: 0,
            proxy_requests: 0,
            committed_transactions: 0,
            causal_handoffs: 0,
            read_observations: 0,
            minimum_version_violations: 0,
            stale_value_observations: 0,
            generations_exact: true,
            precommit_versions_exact: true,
            minimum_versions_honored: true,
            read_your_writes_exact: true,
            real_time_order_exact: true,
            successor_elected: false,
            all_nodes_exact: false,
            envelope_chain_valid: false,
            restarted_node_converges: false,
            process_starts: 0,
            process_kills: 0,
        }
    }
}

struct CellReadVersionProxyScenario<'a> {
    seed: u64,
    rounds: u64,
    mode: CellReadVersionProxyMode,
    executable: &'a Path,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    proxy_addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    proxy_children: ProxyChildGroup,
    observations: ReadVersionProxyObservations,
}

impl<'a> CellReadVersionProxyScenario<'a> {
    fn new(
        seed: u64,
        rounds: u64,
        mode: CellReadVersionProxyMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "read-version proxy executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            rounds,
            mode,
            executable,
            root: TempRoot::new_with_label(seed, &format!("read-version-proxy-{}", mode.id()))?,
            addresses: allocate_addresses()?,
            proxy_addresses: allocate_proxy_addresses()?,
            children: ChildGroup::default(),
            proxy_children: ProxyChildGroup::default(),
            observations: ReadVersionProxyObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<CellReadVersionProxyReport, String> {
        for node_id in 1..=3 {
            self.start_node(node_id)?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.address(1)?, 1).await {
            return Err("node 1 did not become the read-version proxy leader".to_owned());
        }
        for proxy_id in 1..=2 {
            self.start_proxy(proxy_id)?;
        }
        for proxy_id in 1..=2 {
            self.wait_proxy_ready(proxy_id).await?;
        }

        let failover_round = self.rounds / 2;
        let mut leader = 1;
        let mut killed_leader = None;
        let mut expected_rows: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut session_minimum = CellReadVersion::origin();

        for round in 0..self.rounds {
            let source_proxy = (round % 2) + 1;
            let target_proxy = if source_proxy == 1 { 2 } else { 1 };
            let source_snapshot = self.proxy_snapshot(source_proxy, session_minimum).await?;
            self.observations.proxy_requests += 1;
            let target_before_commit = self.proxy_snapshot(target_proxy, session_minimum).await?;
            self.observations.proxy_requests += 1;
            self.observations.precommit_versions_exact &=
                read_version_of(&source_snapshot) == read_version_of(&target_before_commit);
            self.observations.generations_exact &=
                source_snapshot.generation == 1 && target_before_commit.generation == 1;

            let key = format!("proxy/{:016x}/{round:04}", self.seed).into_bytes();
            let value = format!("commit-{round:04}").into_bytes();
            let payload = owned_command(
                request_identity(self.seed, round + 1),
                read_version_of(&source_snapshot),
                &[],
                &[(key.clone(), value.clone())],
            )?;
            let write_ack = retry_write(self.address(leader)?, payload, false).await?;
            let write_outcome = cell_outcome(&write_ack)?;
            let commit_sequence = committed_sequence(write_outcome)?;
            self.observations.committed_transactions += 1;
            expected_rows.insert(key.clone(), value.clone());
            session_minimum = CellReadVersion {
                generation: source_snapshot.generation,
                sequence: commit_sequence,
            };

            if round == failover_round {
                self.kill_node(leader)?;
                killed_leader = Some(leader);
                leader = if leader == 1 { 2 } else { 1 };
                self.observations.successor_elected =
                    elect_until_leader(self.address(leader)?, leader).await;
            }

            let handoff_snapshot = self.proxy_snapshot(target_proxy, session_minimum).await?;
            self.observations.proxy_requests += 1;
            self.observations.causal_handoffs += 1;
            self.observations.read_observations += 1;
            let handoff_version = read_version_of(&handoff_snapshot);
            let minimum_honored = version_at_least(handoff_version, session_minimum);
            let value_exact = handoff_snapshot
                .rows
                .iter()
                .any(|(observed_key, observed_value)| {
                    observed_key == &key && observed_value == &value
                });
            self.observations.minimum_version_violations += u64::from(!minimum_honored);
            self.observations.stale_value_observations += u64::from(!value_exact);
            self.observations.generations_exact &= handoff_snapshot.generation == 1;
            self.observations.minimum_versions_honored &= minimum_honored;
            self.observations.read_your_writes_exact &= value_exact;
            self.observations.real_time_order_exact &= minimum_honored && value_exact;
        }

        let killed_leader = killed_leader
            .ok_or_else(|| "read-version proxy history omitted leader failure".to_owned())?;
        self.start_node(killed_leader)?;
        wait_ready(self.address(killed_leader)?).await?;
        retry_control(self.address(leader)?, HEARTBEAT, &()).await?;

        let expected_rows = expected_rows.into_iter().collect::<Vec<_>>();
        let expected_envelopes = usize::try_from(self.rounds).unwrap_or(usize::MAX);
        let cell_state_exact = wait_for_exact_cell(
            &self.addresses,
            &[1, 2, 3],
            &expected_rows,
            expected_envelopes,
        )
        .await;
        let applied_exact = wait_for_applied_convergence(&self.addresses, &[1, 2, 3]).await;
        self.observations.all_nodes_exact = cell_state_exact && applied_exact;
        self.observations.restarted_node_converges = self.observations.all_nodes_exact;
        let statuses = statuses(&self.addresses, &[1, 2, 3]).await?;
        let cells = statuses
            .iter()
            .filter_map(|node| node.cells.first())
            .collect::<Vec<_>>();
        self.observations.envelope_chain_valid = cells.len() == 3
            && cells.windows(2).all(|pair| pair[0] == pair[1])
            && cells
                .first()
                .is_some_and(|cell| valid_envelope_chain(cell, expected_envelopes));

        Ok(build_read_version_proxy_report(
            self.seed,
            self.rounds,
            self.mode,
            &self.observations,
        ))
    }

    fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let nodes = (1..=3)
            .map(|member| {
                self.address(member)
                    .map(|address| (member, address.to_owned()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy: ProcessNodePolicy::default(),
            },
        )?;
        self.observations.process_starts += 1;
        Ok(())
    }

    fn start_proxy(&mut self, proxy_id: NodeId) -> Result<(), String> {
        let listen_address = self.proxy_address(proxy_id)?.to_owned();
        let authority_addresses = self.addresses.values().cloned().collect::<Vec<_>>();
        self.proxy_children.start(
            self.executable,
            &ReadVersionProxyProcessConfig {
                proxy_id,
                listen_address,
                authority_addresses,
                ignore_session_minimum: self.mode == CellReadVersionProxyMode::IgnoreSessionMinimum,
            },
        )?;
        self.observations.proxy_process_starts += 1;
        Ok(())
    }

    async fn wait_proxy_ready(&self, proxy_id: NodeId) -> Result<(), String> {
        let mut last = String::new();
        for _ in 0..RETRY_ATTEMPTS {
            match request_read_version_proxy(
                self.proxy_address(proxy_id)?,
                CellReadVersion::origin(),
            )
            .await
            {
                Ok(reply) if reply.proxy_id == proxy_id => return Ok(()),
                Ok(reply) => {
                    last = format!(
                        "read-version proxy {} answered for proxy {}",
                        proxy_id, reply.proxy_id
                    );
                }
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "read-version proxy {proxy_id} did not become ready: {last}"
        ))
    }

    async fn proxy_snapshot(
        &self,
        proxy_id: NodeId,
        minimum: CellReadVersion,
    ) -> Result<CellStateSnapshot, String> {
        let reply = request_read_version_proxy(self.proxy_address(proxy_id)?, minimum).await?;
        if reply.proxy_id != proxy_id {
            return Err(format!(
                "read-version proxy {} answered for proxy {}",
                proxy_id, reply.proxy_id
            ));
        }
        Ok(reply.snapshot)
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    fn proxy_address(&self, proxy_id: NodeId) -> Result<&str, String> {
        self.proxy_addresses
            .get(&proxy_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for read-version proxy {proxy_id}"))
    }
}

fn read_version_of(snapshot: &CellStateSnapshot) -> CellReadVersion {
    CellReadVersion {
        generation: snapshot.generation,
        sequence: snapshot.latest_sequence,
    }
}

fn version_at_least(version: CellReadVersion, minimum: CellReadVersion) -> bool {
    version.generation > minimum.generation
        || (version.generation == minimum.generation && version.sequence >= minimum.sequence)
}

#[allow(clippy::too_many_lines)]
fn build_read_version_proxy_report(
    seed: u64,
    rounds: u64,
    mode: CellReadVersionProxyMode,
    observations: &ReadVersionProxyObservations,
) -> CellReadVersionProxyReport {
    let checks = [
        (
            "proxy_processes_started",
            observations.proxy_process_starts == 2,
        ),
        (
            "proxy_requests_executed",
            observations.proxy_requests == rounds.saturating_mul(3),
        ),
        (
            "commits_executed",
            observations.committed_transactions == rounds,
        ),
        (
            "causal_handoffs_executed",
            observations.causal_handoffs == rounds && observations.read_observations == rounds,
        ),
        ("generations_exact", observations.generations_exact),
        (
            "precommit_versions_exact",
            observations.precommit_versions_exact,
        ),
        (
            "minimum_versions_honored",
            observations.minimum_versions_honored,
        ),
        (
            "read_your_writes_exact",
            observations.read_your_writes_exact,
        ),
        ("real_time_order_exact", observations.real_time_order_exact),
        ("successor_elected", observations.successor_elected),
        ("all_nodes_exact", observations.all_nodes_exact),
        ("envelope_chain_valid", observations.envelope_chain_valid),
        (
            "restarted_node_converges",
            observations.restarted_node_converges,
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-read-version-proxy-v1");
    trace.update(seed.to_be_bytes());
    trace.update(rounds.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(observations.proxy_process_starts.to_be_bytes());
    trace.update(observations.proxy_requests.to_be_bytes());
    trace.update(observations.committed_transactions.to_be_bytes());
    trace.update(observations.minimum_version_violations.to_be_bytes());
    trace.update(observations.stale_value_observations.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    CellReadVersionProxyReport {
        seed,
        mode,
        rounds,
        proxy_instances: 2,
        proxy_process_starts: observations.proxy_process_starts,
        proxy_requests: observations.proxy_requests,
        committed_transactions: observations.committed_transactions,
        causal_handoffs: observations.causal_handoffs,
        read_observations: observations.read_observations,
        minimum_version_violations: observations.minimum_version_violations,
        stale_value_observations: observations.stale_value_observations,
        generations_exact: observations.generations_exact,
        minimum_versions_honored: observations.minimum_versions_honored,
        read_your_writes_exact: observations.read_your_writes_exact,
        real_time_order_exact: observations.real_time_order_exact,
        all_nodes_exact: observations.all_nodes_exact,
        envelope_chain_valid: observations.envelope_chain_valid,
        restarted_node_converges: observations.restarted_node_converges,
        process_starts: observations.process_starts,
        process_kills: observations.process_kills,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

#[derive(Clone, Copy, Debug)]
struct SerializabilityWitness {
    read_observations: u64,
    actual_read_dependencies_checked: u64,
    real_time_edges_checked: u64,
    read_values_exact: bool,
    actual_read_dependencies_exact: bool,
    real_time_order_exact: bool,
}

impl SerializabilityWitness {
    const fn valid(self) -> bool {
        self.read_values_exact && self.actual_read_dependencies_exact && self.real_time_order_exact
    }
}

fn check_serializability_witness(
    history: &[CompletedConcurrentTransaction],
) -> SerializabilityWitness {
    let mut committed = history
        .iter()
        .filter(|transaction| transaction.status == CellTransactionStatus::Committed)
        .collect::<Vec<_>>();
    committed.sort_by_key(|transaction| transaction.commit_sequence);

    let mut state = BTreeMap::new();
    let mut snapshots = BTreeMap::from([(0_u64, state.clone())]);
    for transaction in &committed {
        if let Some(sequence) = transaction.commit_sequence {
            apply_expected(&transaction.plan.sets, &mut state);
            snapshots.insert(sequence, state.clone());
        }
    }

    let read_observations = history
        .iter()
        .map(|transaction| transaction.plan.observed_reads.len() as u64)
        .sum();
    let actual_read_dependencies_checked = committed
        .iter()
        .map(|transaction| transaction.plan.observed_reads.len() as u64)
        .sum();
    let read_values_exact = history.iter().all(|transaction| {
        let state_at_read = snapshots
            .range(..=transaction.plan.read_version.sequence)
            .next_back()
            .map(|(_, rows)| rows);
        state_at_read.is_some_and(|rows| {
            transaction
                .plan
                .observed_reads
                .iter()
                .all(|(key, value)| rows.get(key).cloned() == *value)
        })
    });
    let actual_read_dependencies_exact = committed.iter().all(|transaction| {
        let Some(commit_sequence) = transaction.commit_sequence else {
            return false;
        };
        !committed.iter().any(|other| {
            let Some(other_sequence) = other.commit_sequence else {
                return false;
            };
            other_sequence > transaction.plan.read_version.sequence
                && other_sequence < commit_sequence
                && transaction.plan.observed_reads.iter().any(|(read_key, _)| {
                    other
                        .plan
                        .sets
                        .iter()
                        .any(|(written_key, _)| written_key == read_key)
                })
        })
    });

    let mut real_time_edges_checked = 0_u64;
    let mut real_time_order_exact = true;
    for earlier in &committed {
        for later in &committed {
            if earlier.plan.round < later.plan.round {
                real_time_edges_checked = real_time_edges_checked.saturating_add(1);
                real_time_order_exact &= earlier.commit_sequence < later.commit_sequence;
            }
        }
    }

    SerializabilityWitness {
        read_observations,
        actual_read_dependencies_checked,
        real_time_edges_checked,
        read_values_exact,
        actual_read_dependencies_exact,
        real_time_order_exact,
    }
}

fn latest_committed<'a>(
    transactions: &'a [&CompletedConcurrentTransaction],
) -> Option<&'a CompletedConcurrentTransaction> {
    transactions
        .iter()
        .copied()
        .max_by_key(|transaction| transaction.commit_sequence)
}

fn apply_expected(sets: &[(Vec<u8>, Vec<u8>)], rows: &mut BTreeMap<Vec<u8>, Vec<u8>>) {
    for (key, value) in sets {
        rows.insert(key.clone(), value.clone());
    }
}

fn owned_command(
    identity: RequestIdentity,
    read_version: CellReadVersion,
    read_keys: &[Vec<u8>],
    sets: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    CellTransactionCommand {
        identity,
        credential: None,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        read_version,
        read_conflicts: read_keys
            .iter()
            .map(|key| CellKeyRange::point(key))
            .collect(),
        write_conflicts: sets
            .iter()
            .map(|(key, _)| CellKeyRange::point(key))
            .collect(),
        mutations: sets
            .iter()
            .map(|(key, value)| CellMutation::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: vec![10, 20],
    }
    .encode()
    .map_err(|error| error.to_string())
}

fn owned_command_with_ranges(
    identity: RequestIdentity,
    read_version: CellReadVersion,
    read_conflicts: Vec<CellKeyRange>,
    sets: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    CellTransactionCommand {
        identity,
        credential: None,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        read_version,
        read_conflicts,
        write_conflicts: sets
            .iter()
            .map(|(key, _)| CellKeyRange::point(key))
            .collect(),
        mutations: sets
            .iter()
            .map(|(key, value)| CellMutation::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: vec![10, 20],
    }
    .encode()
    .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_lines)]
fn build_concurrent_history_report(
    seed: u64,
    requested_transactions: u64,
    mode: CellConcurrentHistoryMode,
    observations: &ConcurrentHistoryObservations,
) -> CellConcurrentHistoryReport {
    let expected_rounds = requested_transactions / 10;
    let checks = [
        (
            "requested_transactions_executed",
            observations.attempted_transactions == requested_transactions
                && observations.concurrent_rounds == expected_rounds,
        ),
        (
            "expected_outcome_counts",
            observations.committed_transactions == expected_rounds.saturating_mul(7)
                && observations.conflict_rejections == expected_rounds.saturating_mul(3),
        ),
        (
            "hot_conflict_batches_exact",
            observations.hot_conflict_batches_exact,
        ),
        (
            "disjoint_atomic_batches_exact",
            observations.disjoint_atomic_batches_exact,
        ),
        (
            "blind_write_batches_exact",
            observations.blind_write_batches_exact,
        ),
        ("batch_state_exact", observations.batch_state_exact),
        (
            "commit_sequences_unique",
            observations.commit_sequences_unique,
        ),
        ("read_values_exact", observations.read_values_exact),
        (
            "actual_read_dependencies_exact",
            observations.actual_read_dependencies_exact,
        ),
        ("real_time_order_exact", observations.real_time_order_exact),
        (
            "serializability_witness_valid",
            observations.serializability_witness_valid,
        ),
        ("lost_reply_observed", observations.lost_reply_observed),
        ("successor_elected", observations.successor_elected),
        (
            "retry_matches_durable_outcome",
            observations.retry_matches_durable_outcome,
        ),
        (
            "restarted_node_recovers",
            observations.restarted_node_recovers,
        ),
        ("all_nodes_exact", observations.all_nodes_exact),
        ("envelope_chain_valid", observations.envelope_chain_valid),
    ];
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-concurrent-history-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(requested_transactions.to_be_bytes());
    trace.update(observations.attempted_transactions.to_be_bytes());
    trace.update(observations.committed_transactions.to_be_bytes());
    trace.update(observations.conflict_rejections.to_be_bytes());
    trace.update(observations.read_observations.to_be_bytes());
    trace.update(observations.actual_read_dependencies_checked.to_be_bytes());
    trace.update(observations.real_time_edges_checked.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    CellConcurrentHistoryReport {
        seed,
        mode,
        requested_transactions,
        attempted_transactions: observations.attempted_transactions,
        committed_transactions: observations.committed_transactions,
        conflict_rejections: observations.conflict_rejections,
        concurrent_rounds: observations.concurrent_rounds,
        duplicate_retries: observations.duplicate_retries,
        process_starts: observations.process_starts,
        process_kills: observations.process_kills,
        read_observations: observations.read_observations,
        actual_read_dependencies_checked: observations.actual_read_dependencies_checked,
        real_time_edges_checked: observations.real_time_edges_checked,
        read_values_exact: observations.read_values_exact,
        actual_read_dependencies_exact: observations.actual_read_dependencies_exact,
        real_time_order_exact: observations.real_time_order_exact,
        serializability_witness_valid: observations.serializability_witness_valid,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_concurrent_history"
        } else {
            "not_yet"
        }
        .to_owned(),
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn command(
    seed: u64,
    request_id: u64,
    read_version: CellReadVersion,
    read_keys: &[&[u8]],
    sets: &[(&[u8], &[u8])],
) -> Result<Vec<u8>, String> {
    CellTransactionCommand {
        identity: request_identity(seed, request_id),
        credential: None,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        read_version,
        read_conflicts: read_keys
            .iter()
            .map(|key| CellKeyRange::point(key))
            .collect(),
        write_conflicts: sets
            .iter()
            .map(|(key, _)| CellKeyRange::point(key))
            .collect(),
        mutations: sets
            .iter()
            .map(|(key, value)| CellMutation::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .collect(),
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: vec![10, 20],
    }
    .encode()
    .map_err(|error| error.to_string())
}

const fn request_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x4f4b_5654_584e_5630,
        request_id,
    }
}

fn cell_outcome(ack: &WriteAck) -> Result<&crate::CellTransactionApplyResponse, String> {
    ack.response
        .as_ref()
        .and_then(|response| response.cell_transaction.as_ref())
        .ok_or_else(|| "write acknowledgment omitted cell transaction outcome".to_owned())
}

fn committed_sequence(outcome: &crate::CellTransactionApplyResponse) -> Result<u64, String> {
    if outcome.status != CellTransactionStatus::Committed {
        return Err(format!(
            "transaction was not committed: {:?}",
            outcome.status
        ));
    }
    outcome
        .commit_sequence
        .ok_or_else(|| "committed transaction omitted its sequence".to_owned())
}

fn valid_envelope_chain(cell: &CellStateSnapshot, expected_envelopes: usize) -> bool {
    if cell.committed_envelopes.len() != expected_envelopes {
        return false;
    }
    let mut previous = [0_u8; 32];
    for bytes in &cell.committed_envelopes {
        let Ok(envelope) = CommitEnvelope::decode(bytes) else {
            return false;
        };
        if envelope.previous_log_chain() != previous
            || envelope.log_index() != envelope.version().sequence()
            || envelope.generation() != envelope.version().generation()
        {
            return false;
        }
        previous = Sha256::digest(bytes).into();
    }
    true
}

fn build_report(
    seed: u64,
    mode: CellProcessPrototypeMode,
    observations: &Observations,
) -> CellProcessPrototypeReport {
    let mut checks = vec![
        (
            "initial_multi_key_commit",
            observations.initial_multi_key_commit,
        ),
        (
            "stale_read_conflict_rejected",
            observations.stale_read_conflict_rejected,
        ),
        (
            "conflict_rejection_durable",
            observations.conflict_rejection_durable,
        ),
        ("lost_reply_observed", observations.lost_reply_observed),
        ("successor_elected", observations.successor_elected),
        (
            "retry_matches_durable_outcome",
            observations.retry_matches_durable_outcome,
        ),
        (
            "conflicting_retry_rejected",
            observations.conflicting_retry_rejected,
        ),
        (
            "restarted_node_recovers",
            observations.restarted_node_recovers,
        ),
        ("all_nodes_exact", observations.all_nodes_exact),
        ("envelope_chain_valid", observations.envelope_chain_valid),
        ("atomic_rows_exact", observations.atomic_rows_exact),
    ];
    if matches!(
        mode,
        CellProcessPrototypeMode::DurableSnapshotPop | CellProcessPrototypeMode::FreshLearnerRepair
    ) {
        append_snapshot_checks(&mut checks, observations);
    }
    if matches!(
        mode,
        CellProcessPrototypeMode::FreshLearnerRepair
            | CellProcessPrototypeMode::LogOnlyLearnerAsRepair
    ) {
        append_repair_checks(&mut checks, observations);
    }
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-process-prototype-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    for step in &observations.steps {
        trace.update(step.phase.as_bytes());
        trace.update(step.node_id.to_be_bytes());
        trace.update(step.applied_log_index.unwrap_or_default().to_be_bytes());
        trace.update(step.snapshot_log_index.unwrap_or_default().to_be_bytes());
        trace.update(step.latest_commit_sequence.to_be_bytes());
        for (key, value) in &step.rows {
            trace.update((key.len() as u64).to_be_bytes());
            trace.update(key);
            trace.update((value.len() as u64).to_be_bytes());
            trace.update(value);
        }
    }
    if let Some(cell) = &observations.final_cell {
        trace.update(cell.latest_sequence.to_be_bytes());
        for envelope in &cell.committed_envelopes {
            trace.update((envelope.len() as u64).to_be_bytes());
            trace.update(envelope);
        }
    }
    CellProcessPrototypeReport {
        seed,
        mode,
        question: "Can one centralized Cell v0 state machine preserve serializable multi-key transactions, durable retry outcomes, and exact recovery when requests flow through the existing three-process Raft path?".to_owned(),
        answer: if anomaly_count == 0 { "yes_within_the_bounded_prototype" } else { "not_yet" }.to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        process_starts: observations.process_starts,
        process_kills: observations.process_kills,
        committed_transactions: observations.committed_transactions,
        durable_rejections: observations.durable_rejections,
        duplicate_retries: observations.duplicate_retries,
        final_cell: observations.final_cell.clone(),
        authority_snapshot_frontier: observations.authority_snapshot_frontier,
        steps: observations.steps.clone(),
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn append_snapshot_checks(checks: &mut Vec<(&'static str, bool)>, observations: &Observations) {
    checks.extend([
        (
            "durable_snapshot_persisted",
            observations.durable_snapshot_persisted,
        ),
        ("post_pop_retry_exact", observations.post_pop_retry_exact),
        (
            "post_pop_commit_continues",
            observations.post_pop_commit_continues,
        ),
    ]);
}

fn append_repair_checks(checks: &mut Vec<(&'static str, bool)>, observations: &Observations) {
    checks.extend([
        (
            "replacement_uses_fresh_node_identity",
            observations.replacement_uses_fresh_node_identity,
        ),
        (
            "learner_addition_committed",
            observations.learner_addition_committed,
        ),
        (
            "authority_snapshot_installed_on_learner",
            observations.authority_snapshot_installed_on_learner,
        ),
        (
            "retained_suffix_replayed_after_snapshot",
            observations.retained_suffix_replayed_after_snapshot,
        ),
        ("learner_restart_exact", observations.learner_restart_exact),
        (
            "retained_outcomes_exact_on_learner",
            observations.retained_outcomes_exact_on_learner,
        ),
    ]);
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
            credential: None,
        },
    )
    .await
}

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
}

async fn linearizable_status(address: &str) -> Result<NodeStatus, String> {
    control(address, LINEARIZABLE_STATUS, &()).await
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

async fn wait_for_outcome(address: &str, identity: RequestIdentity) -> Option<ApplyResponse> {
    for _ in 0..RETRY_ATTEMPTS {
        if let Ok(Some(response)) = outcome(address, identity).await {
            return Some(response);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

async fn wait_for_exact_cell(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    rows: &[(Vec<u8>, Vec<u8>)],
    envelopes: usize,
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            exact &= status(address).await.is_ok_and(|node| {
                node.cells.first().is_some_and(|cell| {
                    cell.rows == rows && cell.committed_envelopes.len() == envelopes
                })
            });
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_applied_convergence(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut applied = Vec::new();
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            let Ok(node) = status(address).await else {
                applied.clear();
                break;
            };
            applied.push(node.last_applied_index);
        }
        if applied.len() == node_ids.len()
            && applied.first().is_some_and(Option::is_some)
            && applied.windows(2).all(|pair| pair[0] == pair[1])
        {
            return true;
        }
        if let Some(leader) = node_ids.iter().find_map(|node_id| addresses.get(node_id)) {
            let _: Result<(), String> = control(leader, HEARTBEAT, &()).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_snapshot_convergence(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    expected_index: u64,
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            exact &= status(address).await.is_ok_and(|node| {
                node.last_applied_index == Some(expected_index)
                    && node.snapshot_log_index == Some(expected_index)
            });
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn statuses(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
) -> Result<Vec<NodeStatus>, String> {
    let mut result = Vec::new();
    for node_id in node_ids {
        result.push(
            status(
                addresses
                    .get(node_id)
                    .ok_or_else(|| format!("missing address for node {node_id}"))?,
            )
            .await?,
        );
    }
    Ok(result)
}

async fn purge_retained_log(root: &Path) -> Result<(), String> {
    let mut store = OpenRaftLogStore::open(root).map_err(|error| error.to_string())?;
    let state = store
        .get_log_state()
        .await
        .map_err(|error| error.to_string())?;
    let last_log_id = state
        .last_log_id
        .ok_or_else(|| format!("node journal at {} has no log to purge", root.display()))?;
    store
        .purge(last_log_id)
        .await
        .map_err(|error| error.to_string())
}

fn allocate_addresses() -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in 0..4 {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (index, listener) in listeners.iter().enumerate() {
        addresses.insert(
            (index + 1) as u64,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

fn allocate_proxy_addresses() -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in 0..2 {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (index, listener) in listeners.iter().enumerate() {
        addresses.insert(
            (index + 1) as u64,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

async fn add_learner(address: &str, request: AddLearnerRequest) -> Result<WriteAck, String> {
    control(address, ADD_LEARNER, &request).await
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
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let mut command = Command::new(executable);
        command
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        if std::env::var_os("OKV_EVAL_CHILD_STDERR").is_some() {
            command.stderr(Stdio::inherit());
        } else {
            command.stderr(Stdio::null());
        }
        let child = command
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

    fn exit_statuses(&mut self) -> Result<Vec<(NodeId, Option<i32>)>, String> {
        let mut exited = Vec::new();
        for (node_id, child) in &mut self.children {
            if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
                exited.push((*node_id, status.code()));
            }
        }
        Ok(exited)
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

#[derive(Default)]
struct ProxyChildGroup {
    children: BTreeMap<NodeId, Child>,
}

impl ProxyChildGroup {
    fn start(
        &mut self,
        executable: &Path,
        config: &ReadVersionProxyProcessConfig,
    ) -> Result<(), String> {
        if self.children.contains_key(&config.proxy_id) {
            return Err(format!(
                "read-version proxy {} is already running",
                config.proxy_id
            ));
        }
        let proxy_id = config.proxy_id;
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("read-version-proxy-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start read-version proxy {proxy_id}: {error}"))?;
        self.children.insert(proxy_id, child);
        Ok(())
    }
}

impl Drop for ProxyChildGroup {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CellProcessPrototypeMode) -> Result<Self, String> {
        Self::new_with_label(seed, mode.id())
    }

    fn new_with_label(seed: u64, label: &str) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-cell-process-prototype-{}-{seed}-{}-{sequence}",
            label,
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
