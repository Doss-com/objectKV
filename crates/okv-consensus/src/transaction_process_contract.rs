use crate::rpc::{
    read_response, write_request, ControlWrite, NodeStatus, WriteAck, CLIENT_WRITE, ELECT,
    HEARTBEAT, INITIALIZE, LINEARIZABLE_STATUS, OUTCOME, STATUS,
};
use crate::{
    ApplyResponse, ClientCommand, NodeId, ProcessNodeConfig, ProcessNodePolicy, RequestIdentity,
};
use okv_transaction::{
    KeyRange, Mutation, TransactionApplyResponse, TransactionAuthorityFaults,
    TransactionAuthorityView, TransactionCommand, TransactionStatus,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::Command as TokioCommand;

const RETRY_ATTEMPTS: usize = 500;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionProcessMode {
    Correct,
    AcceptConflicts,
    PartialApply,
}

impl TransactionProcessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcceptConflicts => "accept_conflicts",
            Self::PartialApply => "partial_apply",
        }
    }

    const fn faults(self) -> TransactionAuthorityFaults {
        TransactionAuthorityFaults {
            accept_conflicts: matches!(self, Self::AcceptConflicts),
            partial_apply: matches!(self, Self::PartialApply),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessObservedValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub writer_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessReadOperation {
    Point {
        key: Vec<u8>,
        observed: Option<ProcessObservedValue>,
    },
    Range {
        range: KeyRange,
        observed: Vec<ProcessObservedValue>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProcessTransactionResult {
    Committed { commit_version: u64 },
    Aborted { reason: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessTransactionRecord {
    pub id: u64,
    pub begin_tick: u64,
    pub complete_tick: u64,
    pub read_version: u64,
    pub reads: Vec<ProcessReadOperation>,
    pub read_conflicts: Vec<KeyRange>,
    pub write_conflicts: Vec<KeyRange>,
    pub mutations: Vec<Mutation>,
    pub result: ProcessTransactionResult,
    pub applied_mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessTransactionHistory {
    pub cell_id: String,
    pub tenant_id: String,
    pub seed: u64,
    pub transactions: Vec<ProcessTransactionRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionProcessReport {
    pub seed: u64,
    pub mode: TransactionProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub process_starts: u64,
    pub process_kills: u64,
    pub elections: u64,
    pub dropped_replies: u64,
    pub recovered_outcomes: u64,
    pub final_state_equal: bool,
    pub history: ProcessTransactionHistory,
    pub trace_sha256: String,
}

/// Externally managed topology for the independent-machine form of the same
/// transaction process contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionMachineConfig {
    pub schema_version: u32,
    pub addresses: BTreeMap<NodeId, String>,
    pub roots: BTreeMap<NodeId, PathBuf>,
    pub machine_ids: BTreeMap<NodeId, String>,
    pub failure_domains: BTreeMap<NodeId, String>,
    pub controller_machine_id: String,
    pub controller_failure_domain: String,
    pub lifecycle_hook: PathBuf,
    pub hook_timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionMachineReport {
    pub schema_version: u32,
    pub machine_ids: BTreeMap<NodeId, String>,
    pub failure_domains: BTreeMap<NodeId, String>,
    pub controller_machine_id: String,
    pub controller_failure_domain: String,
    pub lifecycle_hook_sha256: String,
    pub topology_sha256: String,
    pub process: TransactionProcessReport,
}

/// Run the Cell v0 transaction authority through three normal OS processes and
/// the actual `OpenRaft` storage, TCP, retry, and replay path.
///
/// # Errors
///
/// Returns an error when a process cannot start or the bounded protocol cannot
/// complete.
pub fn run_transaction_process_contract(
    seed: u64,
    mode: TransactionProcessMode,
    executable: &Path,
) -> Result<TransactionProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(TransactionProcessScenario::new(seed, mode, executable)?.run())
}

/// Run the same Cell v0 transaction history against three externally managed
/// machine endpoints. The lifecycle hook owns machine or service start, kill,
/// and cleanup; it cannot alter the transaction history or oracle.
///
/// # Errors
///
/// Returns an error when the topology is not three distinct non-loopback
/// endpoints and failure domains, the hook is invalid, or the bounded process
/// contract cannot complete.
pub fn run_transaction_machine_contract(
    seed: u64,
    mode: TransactionProcessMode,
    config: TransactionMachineConfig,
) -> Result<TransactionMachineReport, String> {
    let topology = MachineTopologyReceipt::from_config(&config)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let process = runtime
        .block_on(TransactionProcessScenario::new_machine(seed, mode, config.clone())?.run())?;
    Ok(TransactionMachineReport {
        schema_version: 1,
        machine_ids: config.machine_ids,
        failure_domains: config.failure_domains,
        controller_machine_id: config.controller_machine_id,
        controller_failure_domain: config.controller_failure_domain,
        lifecycle_hook_sha256: topology.lifecycle_hook_sha256,
        topology_sha256: topology.topology_sha256,
        process,
    })
}

struct MachineTopologyReceipt {
    lifecycle_hook_sha256: String,
    topology_sha256: String,
}

impl MachineTopologyReceipt {
    fn from_config(config: &TransactionMachineConfig) -> Result<Self, String> {
        config.validate()?;
        let hook = fs::read(&config.lifecycle_hook).map_err(|error| {
            format!(
                "failed to read lifecycle hook {}: {error}",
                config.lifecycle_hook.display()
            )
        })?;
        let lifecycle_hook_sha256 = sha256_bytes(&hook);
        let mut topology = Sha256::new();
        topology.update(b"okv-transaction-machine-topology-v1");
        topology.update(serde_json::to_vec(config).map_err(|error| error.to_string())?);
        topology.update(lifecycle_hook_sha256.as_bytes());
        Ok(Self {
            lifecycle_hook_sha256,
            topology_sha256: format!("{:x}", topology.finalize()),
        })
    }
}

impl TransactionMachineConfig {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported transaction machine schema version {}",
                self.schema_version
            ));
        }
        let expected = BTreeSet::from([1, 2, 3]);
        for (name, keys) in [
            (
                "addresses",
                self.addresses.keys().copied().collect::<BTreeSet<_>>(),
            ),
            ("roots", self.roots.keys().copied().collect::<BTreeSet<_>>()),
            (
                "machine_ids",
                self.machine_ids.keys().copied().collect::<BTreeSet<_>>(),
            ),
            (
                "failure_domains",
                self.failure_domains
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            ),
        ] {
            if keys != expected {
                return Err(format!("{name} must contain exactly node IDs 1, 2, and 3"));
            }
        }
        let endpoints = self
            .addresses
            .values()
            .map(|address| {
                address
                    .parse::<SocketAddr>()
                    .map_err(|error| format!("invalid machine endpoint {address}: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ips: BTreeSet<IpAddr> = endpoints.iter().map(SocketAddr::ip).collect();
        if ips.len() != 3
            || ips.iter().any(|ip| ip.is_loopback() || ip.is_unspecified())
            || endpoints.iter().any(|endpoint| endpoint.port() == 0)
        {
            return Err(
                "machine endpoints must use three distinct routable IP addresses and nonzero ports"
                    .to_owned(),
            );
        }
        let machine_ids: BTreeSet<&str> = self.machine_ids.values().map(String::as_str).collect();
        if machine_ids.len() != 3
            || machine_ids.iter().any(|identity| identity.is_empty())
            || self.controller_machine_id.is_empty()
            || machine_ids.contains(self.controller_machine_id.as_str())
        {
            return Err(
                "node machine IDs must be distinct and exclude the controller machine".to_owned(),
            );
        }
        let failure_domains: BTreeSet<&str> =
            self.failure_domains.values().map(String::as_str).collect();
        if failure_domains.len() != 3
            || failure_domains.iter().any(|domain| domain.is_empty())
            || self.controller_failure_domain.is_empty()
            || failure_domains.contains(self.controller_failure_domain.as_str())
        {
            return Err("node failure domains must be distinct".to_owned());
        }
        if self.roots.values().any(|root| !root.is_absolute()) {
            return Err("machine roots must be absolute paths".to_owned());
        }
        if !self.lifecycle_hook.is_absolute() || !self.lifecycle_hook.is_file() {
            return Err("lifecycle hook must be an absolute existing file".to_owned());
        }
        if !(1..=900).contains(&self.hook_timeout_seconds) {
            return Err("hook timeout must be between 1 and 900 seconds".to_owned());
        }
        Ok(())
    }
}

struct TransactionProcessScenario {
    seed: u64,
    mode: TransactionProcessMode,
    addresses: BTreeMap<NodeId, String>,
    nodes: NodeRuntime,
    history: Vec<ProcessTransactionRecord>,
    checks: Vec<(String, bool)>,
    process_starts: u64,
    process_kills: u64,
    elections: u64,
    dropped_replies: u64,
    recovered_outcomes: u64,
}

impl TransactionProcessScenario {
    fn new(seed: u64, mode: TransactionProcessMode, executable: &Path) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "transaction process executable does not exist: {}",
                executable.display()
            ));
        }
        let addresses = allocate_addresses()?;
        Ok(Self {
            seed,
            mode,
            addresses,
            nodes: NodeRuntime::local(executable.to_path_buf(), TempRoot::new(seed, mode)?),
            history: Vec::new(),
            checks: Vec::new(),
            process_starts: 0,
            process_kills: 0,
            elections: 0,
            dropped_replies: 0,
            recovered_outcomes: 0,
        })
    }

    fn new_machine(
        seed: u64,
        mode: TransactionProcessMode,
        config: TransactionMachineConfig,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            seed,
            mode,
            addresses: config.addresses.clone(),
            nodes: NodeRuntime::Hook(HookGroup::new(config)),
            history: Vec::new(),
            checks: Vec::new(),
            process_starts: 0,
            process_kills: 0,
            elections: 0,
            dropped_replies: 0,
            recovered_outcomes: 0,
        })
    }

    async fn run(mut self) -> Result<TransactionProcessReport, String> {
        let result = self.run_steps().await.inspect(|final_state_equal| {
            self.checks
                .push(("final_state_equal".to_owned(), *final_state_equal));
        });
        let cleanup = self.nodes.cleanup().await;
        match (result, cleanup) {
            (Ok(final_state_equal), Ok(())) => Ok(self.report(final_state_equal)),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup)) => Err(format!("machine cleanup: {cleanup}")),
            (Err(error), Err(cleanup)) => Err(format!("{error}; machine cleanup: {cleanup}")),
        }
    }

    async fn run_steps(&mut self) -> Result<bool, String> {
        self.start_cluster()
            .await
            .map_err(|error| format!("start cluster: {error}"))?;
        self.seed_state()
            .await
            .map_err(|error| format!("seed state: {error}"))?;
        self.point_conflict_pair()
            .await
            .map_err(|error| format!("point conflict pair: {error}"))?;
        self.range_phantom_pair()
            .await
            .map_err(|error| format!("range phantom pair: {error}"))?;
        self.lost_reply_and_failover()
            .await
            .map_err(|error| format!("lost reply and failover: {error}"))?;
        let final_state_equal = self
            .restart_and_compare()
            .await
            .map_err(|error| format!("restart and compare: {error}"))?;
        Ok(final_state_equal)
    }

    async fn start_cluster(&mut self) -> Result<(), String> {
        for node_id in 1..=3 {
            let config = self.node_config(node_id)?;
            self.nodes.prepare(&config).await?;
        }
        for node_id in 1..=3 {
            self.start_node(node_id).await?;
        }
        for node_id in 1..=3 {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(1)?, INITIALIZE, &()).await?;
        let elected = elect_until_leader(self.address(1)?, 1).await;
        self.elections += u64::from(elected);
        self.checks
            .push(("initial_leader_elected".to_owned(), elected));
        Ok(())
    }

    async fn seed_state(&mut self) -> Result<(), String> {
        let transaction = transaction_command(
            0,
            Vec::new(),
            vec![set(b"a/account", 10), set(b"z/account", 20)],
        );
        let response = self.submit(1, 0, 5, Vec::new(), transaction, false).await?;
        self.checks.push((
            "seed_multi_range_committed".to_owned(),
            matches!(response.status, TransactionStatus::Committed { .. }),
        ));
        Ok(())
    }

    async fn point_conflict_pair(&mut self) -> Result<(), String> {
        let view = linearizable_status(self.address(1)?).await?.transaction;
        let reads = vec![
            point_read(&view, b"a/account"),
            point_read(&view, b"z/account"),
        ];
        let first = transaction_command(
            view.current_version,
            vec![KeyRange::point(b"a/account"), KeyRange::point(b"z/account")],
            vec![set(b"a/account", 11), set(b"z/account", 21)],
        );
        let first_response = self.submit(2, 10, 20, reads.clone(), first, false).await?;
        let second = transaction_command(
            view.current_version,
            vec![KeyRange::point(b"a/account"), KeyRange::point(b"z/account")],
            vec![set(b"a/account", 12), set(b"z/account", 22)],
        );
        let second_response = self.submit(3, 10, 21, reads, second, false).await?;
        self.checks.push((
            "point_pair_first_committed".to_owned(),
            matches!(first_response.status, TransactionStatus::Committed { .. }),
        ));
        let expected_second = if self.mode == TransactionProcessMode::AcceptConflicts {
            matches!(second_response.status, TransactionStatus::Committed { .. })
        } else {
            matches!(second_response.status, TransactionStatus::Conflict { .. })
        };
        self.checks
            .push(("point_pair_mode_observed".to_owned(), expected_second));
        Ok(())
    }

    async fn range_phantom_pair(&mut self) -> Result<(), String> {
        let view = linearizable_status(self.address(1)?).await?.transaction;
        let range = KeyRange {
            start: b"u/unique/".to_vec(),
            end: b"u/unique0".to_vec(),
        };
        let reads = vec![range_read(&view, &range)];
        let first = TransactionCommand {
            read_version: view.current_version,
            read_conflicts: vec![range.clone()],
            write_conflicts: vec![KeyRange::point(b"u/unique/one")],
            mutations: vec![set(b"u/unique/one", 1)],
        };
        let first_response = self.submit(4, 30, 40, reads.clone(), first, false).await?;
        let second = TransactionCommand {
            read_version: view.current_version,
            read_conflicts: vec![range],
            write_conflicts: vec![KeyRange::point(b"u/unique/two")],
            mutations: vec![set(b"u/unique/two", 2)],
        };
        let second_response = self.submit(5, 30, 41, reads, second, false).await?;
        self.checks.push((
            "range_pair_first_committed".to_owned(),
            matches!(first_response.status, TransactionStatus::Committed { .. }),
        ));
        let expected_second = if self.mode == TransactionProcessMode::AcceptConflicts {
            matches!(second_response.status, TransactionStatus::Committed { .. })
        } else {
            matches!(second_response.status, TransactionStatus::Conflict { .. })
        };
        self.checks
            .push(("range_pair_mode_observed".to_owned(), expected_second));
        Ok(())
    }

    async fn lost_reply_and_failover(&mut self) -> Result<(), String> {
        let view = linearizable_status(self.address(1)?).await?.transaction;
        let reads = vec![
            point_read(&view, b"a/account"),
            point_read(&view, b"z/account"),
        ];
        let transaction = transaction_command(
            view.current_version,
            vec![KeyRange::point(b"a/account"), KeyRange::point(b"z/account")],
            vec![set(b"a/account", 13), set(b"z/account", 23)],
        );
        let identity = identity(self.seed, 6);
        let encoded = encode_client(identity, &transaction)?;
        let dropped = write(self.address(1)?, encoded.clone(), true).await;
        self.dropped_replies += u64::from(dropped.is_err());
        self.checks
            .push(("lost_reply_observed".to_owned(), dropped.is_err()));
        self.kill_node(1).await?;
        let elected = elect_until_leader(self.address(2)?, 2).await;
        self.elections += u64::from(elected);
        self.checks.push(("successor_elected".to_owned(), elected));
        let recovered = wait_for_outcome(self.address(2)?, identity).await;
        self.recovered_outcomes += u64::from(recovered.is_some());
        let retry = retry_write(self.address(2)?, encoded, false).await?;
        let retry_response = retry
            .response
            .and_then(|response| response.transaction)
            .ok_or_else(|| "transaction retry response missing".to_owned())?;
        let recovered_response = recovered
            .and_then(|response| response.transaction)
            .ok_or_else(|| "recovered transaction outcome missing".to_owned())?;
        self.checks.push((
            "retry_matches_recovered_outcome".to_owned(),
            retry_response == recovered_response,
        ));
        self.history.push(record_from_response(
            6,
            50,
            60,
            reads,
            transaction,
            &recovered_response,
        ));
        Ok(())
    }

    async fn restart_and_compare(&mut self) -> Result<bool, String> {
        self.start_node(1).await?;
        wait_ready(self.address(1)?).await?;
        retry_control(self.address(2)?, HEARTBEAT, &()).await?;
        let leader = linearizable_status(self.address(2)?).await?.transaction;
        for _ in 0..RETRY_ATTEMPTS {
            let mut equal = true;
            for node_id in 1..=3 {
                let status = status(self.address(node_id)?).await?;
                equal &= status.transaction == leader;
            }
            if equal {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(false)
    }

    async fn submit(
        &mut self,
        transaction_id: u64,
        begin_tick: u64,
        complete_tick: u64,
        reads: Vec<ProcessReadOperation>,
        transaction: TransactionCommand,
        drop_reply_after_commit: bool,
    ) -> Result<TransactionApplyResponse, String> {
        let encoded = encode_client(identity(self.seed, transaction_id), &transaction)?;
        let ack = retry_write(self.address(1)?, encoded, drop_reply_after_commit).await?;
        let response = ack
            .response
            .and_then(|response| response.transaction)
            .ok_or_else(|| "transaction apply response missing".to_owned())?;
        self.history.push(record_from_response(
            transaction_id,
            begin_tick,
            complete_tick,
            reads,
            transaction,
            &response,
        ));
        Ok(response)
    }

    fn node_config(&self, node_id: NodeId) -> Result<ProcessNodeConfig, String> {
        Ok(ProcessNodeConfig {
            node_id,
            root: self.nodes.root(node_id)?,
            nodes: self.addresses.clone(),
            deduplicate_requests: true,
            acknowledge_before_quorum: false,
            policy: ProcessNodePolicy {
                transaction_authority_faults: self.mode.faults(),
                ..ProcessNodePolicy::default()
            },
        })
    }

    async fn start_node(&mut self, node_id: NodeId) -> Result<(), String> {
        let config = self.node_config(node_id)?;
        self.nodes.start(&config).await?;
        self.process_starts = self.process_starts.saturating_add(1);
        Ok(())
    }

    async fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.nodes.kill(node_id).await?;
        self.process_kills = self.process_kills.saturating_add(1);
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    fn report(self, final_state_equal: bool) -> TransactionProcessReport {
        let first_mismatch = self
            .checks
            .iter()
            .find(|(_, passed)| !passed)
            .map(|(name, _)| name.clone());
        let anomaly_count = u64::try_from(self.checks.iter().filter(|(_, passed)| !passed).count())
            .unwrap_or(u64::MAX);
        let history = ProcessTransactionHistory {
            cell_id: "cell-v0-process".to_owned(),
            tenant_id: "tenant-process".to_owned(),
            seed: self.seed,
            transactions: self.history,
        };
        let mut trace = Sha256::new();
        trace.update(b"okv-transaction-process-contract-v1");
        trace.update(self.seed.to_be_bytes());
        trace.update(self.mode.id().as_bytes());
        trace.update(serde_json::to_vec(&history).unwrap_or_default());
        for (name, passed) in &self.checks {
            trace.update(name.as_bytes());
            trace.update([u8::from(*passed)]);
        }
        TransactionProcessReport {
            seed: self.seed,
            mode: self.mode,
            executed_checks: u64::try_from(self.checks.len()).unwrap_or(u64::MAX),
            anomaly_count,
            first_mismatch,
            process_starts: self.process_starts,
            process_kills: self.process_kills,
            elections: self.elections,
            dropped_replies: self.dropped_replies,
            recovered_outcomes: self.recovered_outcomes,
            final_state_equal,
            history,
            trace_sha256: format!("{:x}", trace.finalize()),
        }
    }
}

fn transaction_command(
    read_version: u64,
    read_conflicts: Vec<KeyRange>,
    mutations: Vec<Mutation>,
) -> TransactionCommand {
    let mut write_conflicts: Vec<KeyRange> = mutations.iter().map(mutation_range).collect();
    write_conflicts.sort();
    TransactionCommand {
        read_version,
        read_conflicts,
        write_conflicts,
        mutations,
    }
}

fn set(key: &[u8], value: u8) -> Mutation {
    Mutation::Set {
        key: key.to_vec(),
        value: vec![value],
    }
}

fn mutation_range(mutation: &Mutation) -> KeyRange {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Clear { key } => KeyRange::point(key),
        Mutation::ClearRange { range } => range.clone(),
    }
}

fn point_read(view: &TransactionAuthorityView, key: &[u8]) -> ProcessReadOperation {
    ProcessReadOperation::Point {
        key: key.to_vec(),
        observed: view.values.get(key).map(|value| ProcessObservedValue {
            key: key.to_vec(),
            value: value.value.clone(),
            writer_version: value.version,
        }),
    }
}

fn range_read(view: &TransactionAuthorityView, range: &KeyRange) -> ProcessReadOperation {
    ProcessReadOperation::Range {
        range: range.clone(),
        observed: view
            .values
            .range(range.start.clone()..range.end.clone())
            .map(|(key, value)| ProcessObservedValue {
                key: key.clone(),
                value: value.value.clone(),
                writer_version: value.version,
            })
            .collect(),
    }
}

fn record_from_response(
    id: u64,
    begin_tick: u64,
    complete_tick: u64,
    reads: Vec<ProcessReadOperation>,
    transaction: TransactionCommand,
    response: &TransactionApplyResponse,
) -> ProcessTransactionRecord {
    let result = match response.status {
        TransactionStatus::Committed { commit_version } => {
            ProcessTransactionResult::Committed { commit_version }
        }
        TransactionStatus::Conflict {
            conflicting_version,
        } => ProcessTransactionResult::Aborted {
            reason: format!("conflict_at_{conflicting_version}"),
        },
        TransactionStatus::Rejected { reason } => ProcessTransactionResult::Aborted {
            reason: format!("rejected_{reason:?}"),
        },
    };
    let applied_count = usize::try_from(response.applied_mutation_count).unwrap_or(usize::MAX);
    ProcessTransactionRecord {
        id,
        begin_tick,
        complete_tick,
        read_version: transaction.read_version,
        reads,
        read_conflicts: transaction.read_conflicts,
        write_conflicts: transaction.write_conflicts,
        applied_mutations: transaction
            .mutations
            .iter()
            .take(applied_count)
            .cloned()
            .collect(),
        mutations: transaction.mutations,
        result,
    }
}

fn identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x4f4b_5654_584e_5031,
        request_id,
    }
}

fn encode_client(
    identity: RequestIdentity,
    transaction: &TransactionCommand,
) -> Result<Vec<u8>, String> {
    ClientCommand {
        identity,
        credential: None,
        payload: transaction.encode().map_err(|error| error.to_string())?,
    }
    .encode()
    .map_err(|error| error.to_string())
}

enum NodeRuntime {
    Local {
        executable: PathBuf,
        root: TempRoot,
        children: ChildGroup,
    },
    Hook(HookGroup),
}

impl NodeRuntime {
    fn local(executable: PathBuf, root: TempRoot) -> Self {
        Self::Local {
            executable,
            root,
            children: ChildGroup::default(),
        }
    }

    fn root(&self, node_id: NodeId) -> Result<PathBuf, String> {
        match self {
            Self::Local { root, .. } => Ok(root.node(node_id)),
            Self::Hook(group) => group
                .config
                .roots
                .get(&node_id)
                .cloned()
                .ok_or_else(|| format!("missing machine root for node {node_id}")),
        }
    }

    async fn prepare(&mut self, config: &ProcessNodeConfig) -> Result<(), String> {
        match self {
            Self::Local { .. } => Ok(()),
            Self::Hook(group) => group.invoke("prepare", config.node_id, Some(config)).await,
        }
    }

    async fn start(&mut self, config: &ProcessNodeConfig) -> Result<(), String> {
        match self {
            Self::Local {
                executable,
                children,
                ..
            } => children.start(executable, config),
            Self::Hook(group) => {
                group.invoke("start", config.node_id, Some(config)).await?;
                group.active.insert(config.node_id);
                Ok(())
            }
        }
    }

    async fn kill(&mut self, node_id: NodeId) -> Result<(), String> {
        match self {
            Self::Local { children, .. } => children.kill(node_id),
            Self::Hook(group) => {
                group.invoke("kill", node_id, None).await?;
                group.active.remove(&node_id);
                Ok(())
            }
        }
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        match self {
            Self::Local { children, .. } => children.kill_all(),
            Self::Hook(group) => group.cleanup().await,
        }
    }
}

struct HookGroup {
    config: TransactionMachineConfig,
    active: BTreeSet<NodeId>,
}

impl HookGroup {
    fn new(config: TransactionMachineConfig) -> Self {
        Self {
            config,
            active: BTreeSet::new(),
        }
    }

    async fn invoke(
        &self,
        action: &str,
        node_id: NodeId,
        config: Option<&ProcessNodeConfig>,
    ) -> Result<(), String> {
        let mut command = TokioCommand::new(&self.config.lifecycle_hook);
        command
            .kill_on_drop(true)
            .arg(action)
            .arg(node_id.to_string());
        if let Some(config) = config {
            command.arg(serde_json::to_string(config).map_err(|error| error.to_string())?);
        }
        let output = tokio::time::timeout(
            Duration::from_secs(self.config.hook_timeout_seconds),
            command.output(),
        )
        .await
        .map_err(|_| format!("lifecycle hook timed out during {action} for node {node_id}"))?
        .map_err(|error| format!("lifecycle hook failed to start: {error}"))?;
        if output.status.success() {
            return Ok(());
        }
        let stderr: String = String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(1_024)
            .collect();
        Err(format!(
            "lifecycle hook {action} failed for node {node_id} with {}: {stderr}",
            output.status
        ))
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        let mut first_error = None;
        for node_id in 1..=3 {
            if let Err(error) = self.invoke("cleanup", node_id, None).await {
                first_error.get_or_insert(error);
            }
        }
        self.active.clear();
        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Default)]
struct ChildGroup {
    children: BTreeMap<NodeId, Child>,
}

impl ChildGroup {
    fn start(&mut self, executable: &Path, config: &ProcessNodeConfig) -> Result<(), String> {
        let node_id = config.node_id;
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let child = StdCommand::new(executable)
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start transaction node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn kill(&mut self, node_id: NodeId) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&node_id)
            .ok_or_else(|| format!("transaction node {node_id} is not running"))?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        Ok(())
    }

    fn kill_all(&mut self) -> Result<(), String> {
        let mut first_error = None;
        for (_, mut child) in std::mem::take(&mut self.children) {
            if let Err(error) = child.kill().and_then(|()| child.wait().map(|_| ())) {
                first_error.get_or_insert_with(|| error.to_string());
            }
        }
        first_error.map_or(Ok(()), Err)
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

async fn retry_write(
    address: &str,
    app_data: Vec<u8>,
    drop_reply: bool,
) -> Result<WriteAck, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match write(address, app_data.clone(), drop_reply).await {
            Ok(ack) => return Ok(ack),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("transaction write failed at {address}: {last}"))
}

async fn write(address: &str, app_data: Vec<u8>, drop_reply: bool) -> Result<WriteAck, String> {
    control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit: drop_reply,
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

async fn wait_for_outcome(address: &str, identity: RequestIdentity) -> Option<ApplyResponse> {
    for _ in 0..RETRY_ATTEMPTS {
        if let Ok(Some(response)) = control(address, OUTCOME, &identity).await {
            return Some(response);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
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
    Err(format!("transaction control failed at {address}: {last}"))
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
    Err(format!(
        "transaction node did not become ready at {address}: {last}"
    ))
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

async fn control<Req, Resp>(address: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("transaction connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("transaction response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

fn allocate_addresses() -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (index, listener) in listeners.iter().enumerate() {
        addresses.insert(
            u64::try_from(index + 1).unwrap_or(u64::MAX),
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: TransactionProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-transaction-process-{}-{seed}-{}-{sequence}",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn machine_config() -> (TempRoot, TransactionMachineConfig) {
        let root = TempRoot::new(9_001, TransactionProcessMode::Correct).expect("temp root");
        let hook = root.0.join("lifecycle-hook");
        fs::write(&hook, b"bounded hook fixture").expect("write hook");
        let config = TransactionMachineConfig {
            schema_version: 1,
            addresses: BTreeMap::from([
                (1, "192.0.2.11:32100".to_owned()),
                (2, "192.0.2.12:32100".to_owned()),
                (3, "192.0.2.13:32100".to_owned()),
            ]),
            roots: BTreeMap::from([
                (1, root.0.join("machine-1")),
                (2, root.0.join("machine-2")),
                (3, root.0.join("machine-3")),
            ]),
            machine_ids: BTreeMap::from([
                (1, "machine-1".to_owned()),
                (2, "machine-2".to_owned()),
                (3, "machine-3".to_owned()),
            ]),
            failure_domains: BTreeMap::from([
                (1, "zone-a".to_owned()),
                (2, "zone-b".to_owned()),
                (3, "zone-c".to_owned()),
            ]),
            controller_machine_id: "machine-controller".to_owned(),
            controller_failure_domain: "zone-controller".to_owned(),
            lifecycle_hook: hook,
            hook_timeout_seconds: 300,
        };
        (root, config)
    }

    #[test]
    fn independent_machine_topology_is_canonical_and_exact() {
        let (_root, config) = machine_config();
        let first = MachineTopologyReceipt::from_config(&config).expect("valid topology");
        let second = MachineTopologyReceipt::from_config(&config).expect("repeat topology");
        assert_eq!(first.topology_sha256, second.topology_sha256);
        assert_eq!(first.lifecycle_hook_sha256, second.lifecycle_hook_sha256);
    }

    #[test]
    fn independent_machine_topology_rejects_false_failure_domains() {
        let (_root, mut config) = machine_config();
        config.addresses.insert(3, "127.0.0.1:32100".to_owned());
        assert!(config
            .validate()
            .is_err_and(|error| error.contains("routable")));

        let (_root, mut config) = machine_config();
        config.failure_domains.insert(3, "zone-b".to_owned());
        assert!(config
            .validate()
            .is_err_and(|error| error.contains("failure domains")));

        let (_root, mut config) = machine_config();
        config.controller_machine_id = "machine-1".to_owned();
        assert!(config
            .validate()
            .is_err_and(|error| error.contains("exclude the controller")));

        let (_root, mut config) = machine_config();
        config.controller_failure_domain = "zone-a".to_owned();
        assert!(config
            .validate()
            .is_err_and(|error| error.contains("failure domains")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_hook_receives_the_frozen_action_boundary() {
        use std::os::unix::fs::PermissionsExt;

        let (_root, mut config) = machine_config();
        let log = config.lifecycle_hook.with_extension("log");
        fs::write(
            &config.lifecycle_hook,
            format!(
                "#!/bin/sh\nprintf '%s|%s|%s\\n' \"$1\" \"$2\" \"${{3:-}}\" >> '{}'\n",
                log.display()
            ),
        )
        .expect("write hook");
        fs::set_permissions(&config.lifecycle_hook, fs::Permissions::from_mode(0o700))
            .expect("make hook executable");
        config.hook_timeout_seconds = 2;
        let group = HookGroup::new(config.clone());
        let node = ProcessNodeConfig {
            node_id: 1,
            root: config.roots[&1].clone(),
            nodes: config.addresses.clone(),
            deduplicate_requests: true,
            acknowledge_before_quorum: false,
            policy: ProcessNodePolicy::default(),
        };
        group
            .invoke("prepare", 1, Some(&node))
            .await
            .expect("invoke hook");
        let recorded = fs::read_to_string(log).expect("read hook log");
        assert!(recorded.starts_with("prepare|1|"));
        assert!(recorded.contains("\"node_id\":1"));
    }
}
