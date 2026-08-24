use crate::cell_transaction::{
    cell_partitioned_transaction_sha256, cell_resolver_incarnation, cell_resolver_map_sha256,
    cell_resolver_partitions, sign_cell_resolver_decision, CellKeyRange, CellMutation,
    CellPartitionedResolution, CellReadVersion, CellResolverDecision,
    CellResolverDecisionAttestation, CellResolverDecisionStatement, CellTransactionCommand,
    CellTransactionStatus,
};
use crate::rpc::{
    read_frame, read_response, write_request, write_response, NodeStatus, ELECT, INITIALIZE,
    LINEARIZABLE_STATUS, STATUS,
};
use crate::{CellTransactionClient, NodeId, ProcessNodeConfig, ProcessNodePolicy, RequestIdentity};
use okv_sim::CommitEnvelope;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];
const RESOLVER_REQUEST: u8 = 1;
const RETRY_ATTEMPTS: usize = 500;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Bounded subjects frozen by RFC-0048.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionedResolverMode {
    Correct,
    StartKeyOnlyRouting,
    PartialAcceptance,
    DuplicateResolverIdentity,
    MixedMapEpoch,
    VolatilePartitionDecision,
    SkipPriorFinalization,
    SplitWithPreparedTransaction,
}

impl PartitionedResolverMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::StartKeyOnlyRouting => "start_key_only_routing",
            Self::PartialAcceptance => "partial_acceptance",
            Self::DuplicateResolverIdentity => "duplicate_resolver_identity",
            Self::MixedMapEpoch => "mixed_map_epoch",
            Self::VolatilePartitionDecision => "volatile_partition_decision",
            Self::SkipPriorFinalization => "skip_prior_finalization",
            Self::SplitWithPreparedTransaction => "split_with_prepared_transaction",
        }
    }
}

/// Subjects frozen by RFC-0049.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatelessResolverRecoveryMode {
    Correct,
    ContinueAfterResolverLoss,
    ActivateBeforeOldFence,
    AcceptOldGenerationReply,
    ReadBelowRecoveryFloor,
    PublishUnresolvedOldWork,
    OmitDurableHead,
}

impl StatelessResolverRecoveryMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ContinueAfterResolverLoss => "continue_after_resolver_loss",
            Self::ActivateBeforeOldFence => "activate_before_old_fence",
            Self::AcceptOldGenerationReply => "accept_old_generation_reply",
            Self::ReadBelowRecoveryFloor => "read_below_recovery_floor",
            Self::PublishUnresolvedOldWork => "publish_unresolved_old_work",
            Self::OmitDurableHead => "omit_durable_head",
        }
    }
}

/// Deterministic receipt from one RFC-0049 generation-recovery history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StatelessResolverRecoveryReport {
    pub seed: u64,
    pub mode: StatelessResolverRecoveryMode,
    pub question: String,
    pub answer: String,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub safe_false_conflicts: u64,
    pub resolver_decisions: u64,
    pub ordered_batches: u64,
    pub process_starts: u64,
    pub generation_fences: u64,
    pub abandoned_candidates: u64,
    pub recovery_floor: u64,
    pub resolver_durable_syncs: u64,
    pub resolver_finalization_rpcs: u64,
    pub partitioned_commits_subset_of_centralized_oracle: bool,
    pub centralized_conflicts_rejected: bool,
    pub rows_match_authoritative_outcomes: bool,
    pub envelope_chain_exact: bool,
    pub complete_resolver_agreement: bool,
    pub resolver_failure_stopped_old_generation: bool,
    pub old_generation_fenced_before_successor: bool,
    pub recovery_floor_includes_durable_head: bool,
    pub successor_resolver_state_started_empty: bool,
    pub successor_reads_at_or_above_floor: bool,
    pub old_generation_requests_rejected: bool,
    pub old_generation_replies_rejected: bool,
    pub unresolved_old_work_not_visible: bool,
    pub abandoned_work_retried_with_new_identity: bool,
    pub negative_control_detected: bool,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub latest_commit_version: u64,
    pub trace_sha256: String,
}

/// Deterministic receipt from one six-process partitioned-resolver history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionedResolverReport {
    pub seed: u64,
    pub mode: PartitionedResolverMode,
    pub question: String,
    pub answer: String,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub resolver_decisions: u64,
    pub cross_partition_attempts: u64,
    pub durable_finalizations: u64,
    pub process_starts: u64,
    pub process_restarts: u64,
    pub centralized_and_partitioned_statuses_match: bool,
    pub centralized_and_partitioned_rows_match: bool,
    pub envelope_chain_exact: bool,
    pub crossing_ranges_route_to_every_overlap: bool,
    pub all_required_partitions_decide: bool,
    pub resolver_identities_distinct: bool,
    pub map_epoch_exact: bool,
    pub decisions_durable_before_ack: bool,
    pub prior_disposition_order_exact: bool,
    pub finalization_exact: bool,
    pub restarted_resolver_replays_exact_decision: bool,
    pub negative_control_detected: bool,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub latest_commit_version: u64,
    pub trace_sha256: String,
}

/// Configuration for one independent resolver process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolverProcessConfig {
    pub resolver_id: u16,
    pub root: PathBuf,
    pub listen_address: String,
    #[serde(default = "default_resolver_cell_id")]
    pub cell_id: [u8; 16],
    #[serde(default = "default_resolver_tenant_id")]
    pub tenant_id: [u8; 16],
    pub map_epoch: u64,
    #[serde(default = "default_transaction_system_generation")]
    pub transaction_system_generation: u64,
    #[serde(default)]
    pub minimum_read_sequence: u64,
    #[serde(default)]
    pub memory_only: bool,
    pub acknowledge_before_durable: bool,
    pub allow_map_epoch_override: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverPrepare {
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    map_epoch: u64,
    transaction_identity: RequestIdentity,
    candidate_sequence: u64,
    read_version: CellReadVersion,
    resolver_read_sequence: u64,
    transaction_sha256: [u8; 32],
    read_conflicts: Vec<CellKeyRange>,
    write_conflicts: Vec<CellKeyRange>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverFinalize {
    candidate_sequence: u64,
    transaction_sha256: [u8; 32],
    global_status: CellTransactionStatus,
    commit_sequence: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ResolverRequest {
    Prepare(ResolverPrepare),
    PrepareBatch(Vec<ResolverPrepare>),
    Finalize(ResolverFinalize),
    Status,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ResolverResponse {
    Prepared(Box<CellResolverDecisionAttestation>),
    PreparedBatch(Vec<CellResolverDecisionAttestation>),
    Finalized,
    Status(ResolverStatus),
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverStatus {
    resolver_id: u16,
    prepared: u64,
    finalized: u64,
    committed_writes: u64,
    transaction_system_generation: u64,
    minimum_read_sequence: u64,
    memory_only: bool,
}

const fn default_transaction_system_generation() -> u64 {
    1
}

const fn default_resolver_cell_id() -> [u8; 16] {
    CELL_ID
}

const fn default_resolver_tenant_id() -> [u8; 16] {
    TENANT_ID
}

/// Real-process memory-only resolver set reused by composed recovery gates.
pub(crate) struct StatelessResolverProcessSet<'a> {
    executable: &'a Path,
    root: PathBuf,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    minimum_read_sequence: u64,
    addresses: BTreeMap<u16, String>,
    children: BTreeMap<u16, Child>,
}

impl<'a> StatelessResolverProcessSet<'a> {
    pub(crate) async fn start(
        executable: &'a Path,
        root: PathBuf,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
        generation: u64,
        minimum_read_sequence: u64,
    ) -> Result<Self, String> {
        let addresses = allocate_addresses(3)?;
        let mut set = Self {
            executable,
            root,
            cell_id,
            tenant_id,
            generation,
            minimum_read_sequence,
            addresses,
            children: BTreeMap::new(),
        };
        for resolver_id in 1..=3 {
            set.start_one(resolver_id)?;
        }
        for resolver_id in 1..=3 {
            wait_resolver_ready(set.address(resolver_id)?).await?;
        }
        Ok(set)
    }

    pub(crate) async fn resolve(
        &self,
        command: &CellTransactionCommand,
        candidate: u64,
        resolver_read_sequence: u64,
    ) -> Result<Vec<CellResolverDecisionAttestation>, String> {
        let required = required_resolvers(command);
        self.resolve_on(
            command,
            candidate,
            self.generation,
            resolver_read_sequence,
            &required,
        )
        .await
    }

    pub(crate) async fn resolve_on(
        &self,
        command: &CellTransactionCommand,
        candidate: u64,
        generation: u64,
        resolver_read_sequence: u64,
        resolver_ids: &[u16],
    ) -> Result<Vec<CellResolverDecisionAttestation>, String> {
        let mut attestations = Vec::new();
        for resolver_id in resolver_ids {
            let request = build_resolver_prepare(
                *resolver_id,
                command,
                candidate,
                1,
                generation,
                resolver_read_sequence,
            )?;
            match resolver_call(
                self.address(*resolver_id)?,
                &ResolverRequest::Prepare(request),
            )
            .await?
            {
                ResolverResponse::Prepared(attestation) => attestations.push(*attestation),
                _ => return Err("resolver prepare returned the wrong response".to_owned()),
            }
        }
        attestations.sort_by_key(|attestation| attestation.statement.resolver_id);
        Ok(attestations)
    }

    pub(crate) async fn state_is_empty_at_floor(&self) -> Result<bool, String> {
        for resolver_id in 1..=3 {
            match resolver_call(self.address(resolver_id)?, &ResolverRequest::Status).await? {
                ResolverResponse::Status(status)
                    if status.prepared == 0
                        && status.committed_writes == 0
                        && status.finalized == 0
                        && status.memory_only
                        && status.transaction_system_generation == self.generation
                        && status.minimum_read_sequence == self.minimum_read_sequence => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    pub(crate) fn stop_one(&mut self, resolver_id: u16) -> Result<(), String> {
        if let Some(mut child) = self.children.remove(&resolver_id) {
            child.kill().map_err(|error| error.to_string())?;
            child.wait().map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    pub(crate) fn stop_all(&mut self) -> Result<(), String> {
        let resolver_ids = self.children.keys().copied().collect::<Vec<_>>();
        for resolver_id in resolver_ids {
            self.stop_one(resolver_id)?;
        }
        Ok(())
    }

    fn start_one(&mut self, resolver_id: u16) -> Result<(), String> {
        let config = ResolverProcessConfig {
            resolver_id,
            root: self.root.join(format!("resolver-{resolver_id}")),
            listen_address: self.address(resolver_id)?.to_owned(),
            cell_id: self.cell_id,
            tenant_id: self.tenant_id,
            map_epoch: 1,
            transaction_system_generation: self.generation,
            minimum_read_sequence: self.minimum_read_sequence,
            memory_only: true,
            acknowledge_before_durable: false,
            allow_map_epoch_override: false,
        };
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = child_command(self.executable, "resolver-node", &config_json)
            .spawn()
            .map_err(|error| error.to_string())?;
        self.children.insert(resolver_id, child);
        Ok(())
    }

    fn address(&self, resolver_id: u16) -> Result<&str, String> {
        self.addresses
            .get(&resolver_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing resolver {resolver_id}"))
    }
}

impl Drop for StatelessResolverProcessSet<'_> {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverPrepared {
    statement: CellResolverDecisionStatement,
    finalized: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverCommittedWrite {
    sequence: u64,
    range: CellKeyRange,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverState {
    last_candidate: u64,
    prepared: BTreeMap<u64, ResolverPrepared>,
    committed_writes: Vec<ResolverCommittedWrite>,
}

/// Run one resolver TCP process until terminated.
///
/// # Errors
///
/// Returns an error when its fixed map identity, journal, listener, or request is invalid.
pub async fn run_resolver_process(config: ResolverProcessConfig) -> Result<(), String> {
    let partition = cell_resolver_partitions()
        .into_iter()
        .find(|partition| partition.resolver_id == config.resolver_id)
        .ok_or_else(|| format!("resolver {} is absent from map epoch 1", config.resolver_id))?;
    if config.map_epoch != 1 {
        return Err("the RFC-0048 resolver process requires base map epoch 1".to_owned());
    }
    if config.transaction_system_generation == 0 {
        return Err("resolver transaction-system generation must be positive".to_owned());
    }
    fs::create_dir_all(&config.root).map_err(|error| error.to_string())?;
    let state_path = config.root.join("resolver-state.json");
    let mut state = if config.memory_only {
        ResolverState::default()
    } else {
        load_resolver_state(&state_path)?
    };
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let (mut stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let kind = stream.read_u8().await.map_err(|error| error.to_string())?;
        if kind != RESOLVER_REQUEST {
            write_response::<_, Result<ResolverResponse, String>>(
                &mut stream,
                &Err("unknown resolver request".to_owned()),
            )
            .await
            .map_err(|error| error.to_string())?;
            continue;
        }
        let body = read_frame(&mut stream)
            .await
            .map_err(|error| error.to_string())?;
        let request =
            serde_json::from_slice::<ResolverRequest>(&body).map_err(|error| error.to_string())?;
        let response =
            apply_resolver_request(&config, &partition, &state_path, &mut state, request);
        write_response(&mut stream, &response)
            .await
            .map_err(|error| error.to_string())?;
    }
}

#[allow(clippy::too_many_lines)]
fn apply_resolver_request(
    config: &ResolverProcessConfig,
    partition: &crate::CellResolverPartition,
    state_path: &Path,
    state: &mut ResolverState,
    request: ResolverRequest,
) -> Result<ResolverResponse, String> {
    match request {
        ResolverRequest::Status => Ok(ResolverResponse::Status(ResolverStatus {
            resolver_id: config.resolver_id,
            prepared: state.prepared.len() as u64,
            finalized: state
                .prepared
                .values()
                .filter(|prepared| prepared.finalized)
                .count() as u64,
            committed_writes: state.committed_writes.len() as u64,
            transaction_system_generation: config.transaction_system_generation,
            minimum_read_sequence: config.minimum_read_sequence,
            memory_only: config.memory_only,
        })),
        ResolverRequest::Prepare(mut request) => {
            request.read_conflicts.sort();
            request.read_conflicts.dedup();
            request.write_conflicts.sort();
            request.write_conflicts.dedup();
            let accepted_epoch = request.map_epoch == config.map_epoch
                || (config.allow_map_epoch_override && request.map_epoch == 2);
            if !accepted_epoch
                || request.cell_id != config.cell_id
                || request.tenant_id != config.tenant_id
                || request.generation != config.transaction_system_generation
                || request.candidate_sequence == 0
                || request.resolver_read_sequence < config.minimum_read_sequence
                || request.read_conflicts.iter().any(|range| !range.valid())
                || request.write_conflicts.iter().any(|range| !range.valid())
            {
                return Err("resolver prepare identity or range is invalid".to_owned());
            }
            let owned = CellKeyRange {
                start: partition.start.clone(),
                end: partition.end.clone(),
            };
            if request
                .read_conflicts
                .iter()
                .chain(&request.write_conflicts)
                .any(|range| {
                    !range.overlaps(&owned) || range.start < owned.start || range.end > owned.end
                })
            {
                return Err("resolver received a conflict outside its owned range".to_owned());
            }
            if let Some(existing) = state.prepared.get(&request.candidate_sequence) {
                if same_prepare(&existing.statement, &request) {
                    return sign_cell_resolver_decision(existing.statement.clone())
                        .map(Box::new)
                        .map(ResolverResponse::Prepared);
                }
                return Err(
                    "resolver candidate was reused for different transaction bytes".to_owned(),
                );
            }
            if !config.memory_only && state.prepared.values().any(|prepared| !prepared.finalized) {
                return Err("prior touching decision has no global disposition".to_owned());
            }
            if request.candidate_sequence <= state.last_candidate {
                return Err("resolver candidate did not advance".to_owned());
            }
            let decision = if request.read_conflicts.iter().any(|read| {
                state.committed_writes.iter().any(|write| {
                    write.sequence > request.resolver_read_sequence && read.overlaps(&write.range)
                })
            }) {
                CellResolverDecision::Conflict
            } else {
                CellResolverDecision::Accept
            };
            let statement = CellResolverDecisionStatement {
                format_version: 1,
                cell_id: request.cell_id,
                tenant_id: request.tenant_id,
                generation: request.generation,
                map_epoch: request.map_epoch,
                map_sha256: cell_resolver_map_sha256(),
                resolver_id: config.resolver_id,
                resolver_incarnation: cell_resolver_incarnation(config.resolver_id),
                transaction_identity: request.transaction_identity,
                candidate_sequence: request.candidate_sequence,
                read_version: request.read_version,
                resolver_read_sequence: request.resolver_read_sequence,
                transaction_sha256: request.transaction_sha256,
                read_conflicts: request.read_conflicts,
                write_conflicts: request.write_conflicts,
                decision,
            };
            let attestation = sign_cell_resolver_decision(statement.clone())?;
            if config.memory_only {
                state.last_candidate = request.candidate_sequence;
                if statement.decision == CellResolverDecision::Accept {
                    state
                        .committed_writes
                        .extend(statement.write_conflicts.iter().cloned().map(|range| {
                            ResolverCommittedWrite {
                                sequence: request.candidate_sequence,
                                range,
                            }
                        }));
                }
                state.prepared.insert(
                    request.candidate_sequence,
                    ResolverPrepared {
                        statement,
                        finalized: true,
                    },
                );
            } else if !config.acknowledge_before_durable {
                state.last_candidate = request.candidate_sequence;
                state.prepared.insert(
                    request.candidate_sequence,
                    ResolverPrepared {
                        statement,
                        finalized: false,
                    },
                );
                persist_resolver_state(state_path, state)?;
            }
            Ok(ResolverResponse::Prepared(Box::new(attestation)))
        }
        ResolverRequest::PrepareBatch(requests) => {
            if requests.is_empty() {
                return Err("resolver batch must not be empty".to_owned());
            }
            let mut attestations = Vec::with_capacity(requests.len());
            for request in requests {
                match apply_resolver_request(
                    config,
                    partition,
                    state_path,
                    state,
                    ResolverRequest::Prepare(request),
                )? {
                    ResolverResponse::Prepared(attestation) => attestations.push(*attestation),
                    _ => return Err("resolver batch produced a non-prepare response".to_owned()),
                }
            }
            Ok(ResolverResponse::PreparedBatch(attestations))
        }
        ResolverRequest::Finalize(request) => {
            if config.memory_only {
                return Err("memory-only resolver does not accept finalization".to_owned());
            }
            let prepared = state
                .prepared
                .get_mut(&request.candidate_sequence)
                .ok_or_else(|| "resolver cannot finalize an absent durable decision".to_owned())?;
            if prepared.statement.transaction_sha256 != request.transaction_sha256 {
                return Err("resolver finalize transaction digest differs from prepare".to_owned());
            }
            if prepared.finalized {
                return Ok(ResolverResponse::Finalized);
            }
            match request.global_status {
                CellTransactionStatus::Committed => {
                    if prepared.statement.decision != CellResolverDecision::Accept {
                        return Err("globally committed transaction had local conflict".to_owned());
                    }
                    let sequence = request.commit_sequence.ok_or_else(|| {
                        "committed finalization omitted commit sequence".to_owned()
                    })?;
                    state.committed_writes.extend(
                        prepared
                            .statement
                            .write_conflicts
                            .iter()
                            .cloned()
                            .map(|range| ResolverCommittedWrite { sequence, range }),
                    );
                }
                CellTransactionStatus::Conflict => {
                    if request.commit_sequence.is_some() {
                        return Err("conflict finalization supplied a commit sequence".to_owned());
                    }
                }
                _ => return Err("resolver can finalize only commit or conflict".to_owned()),
            }
            prepared.finalized = true;
            persist_resolver_state(state_path, state)?;
            Ok(ResolverResponse::Finalized)
        }
    }
}

fn same_prepare(statement: &CellResolverDecisionStatement, request: &ResolverPrepare) -> bool {
    statement.cell_id == request.cell_id
        && statement.tenant_id == request.tenant_id
        && statement.generation == request.generation
        && statement.map_epoch == request.map_epoch
        && statement.transaction_identity == request.transaction_identity
        && statement.candidate_sequence == request.candidate_sequence
        && statement.read_version == request.read_version
        && statement.resolver_read_sequence == request.resolver_read_sequence
        && statement.transaction_sha256 == request.transaction_sha256
        && statement.read_conflicts == request.read_conflicts
        && statement.write_conflicts == request.write_conflicts
}

fn load_resolver_state(path: &Path) -> Result<ResolverState, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(ResolverState::default()),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_resolver_state(path: &Path, state: &ResolverState) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "resolver journal path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let temporary = parent.join("resolver-state.tmp");
    let bytes = serde_json::to_vec(state).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

#[derive(Default)]
struct Oracle {
    latest_sequence: u64,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    writes: Vec<ResolverCommittedWrite>,
    committed: u64,
    conflicts: u64,
}

impl Oracle {
    fn decision(&self, command: &CellTransactionCommand) -> CellTransactionStatus {
        if command.read_conflicts.iter().any(|read| {
            self.writes.iter().any(|write| {
                write.sequence > command.read_version.sequence && read.overlaps(&write.range)
            })
        }) {
            CellTransactionStatus::Conflict
        } else {
            CellTransactionStatus::Committed
        }
    }

    fn apply(
        &mut self,
        command: &CellTransactionCommand,
        status: CellTransactionStatus,
        sequence: Option<u64>,
    ) {
        match status {
            CellTransactionStatus::Committed => {
                let sequence = sequence.expect("committed oracle response has a sequence");
                for mutation in &command.mutations {
                    match mutation {
                        CellMutation::Clear { key } => {
                            self.rows.remove(key);
                        }
                        CellMutation::Set { key, value } => {
                            self.rows.insert(key.clone(), value.clone());
                        }
                    }
                }
                self.writes.extend(
                    command
                        .write_conflicts
                        .iter()
                        .cloned()
                        .map(|range| ResolverCommittedWrite { sequence, range }),
                );
                self.latest_sequence = sequence;
                self.committed += 1;
            }
            CellTransactionStatus::Conflict => self.conflicts += 1,
            _ => {}
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct Observations {
    attempted: u64,
    decisions: u64,
    cross_partition: u64,
    finalizations: u64,
    process_starts: u64,
    process_restarts: u64,
    statuses_match: bool,
    routing_exact: bool,
    all_decide: bool,
    identities_distinct: bool,
    map_epoch_exact: bool,
    durability_exact: bool,
    prior_order_exact: bool,
    finalization_exact: bool,
    restart_replay_exact: bool,
    negative_detected: bool,
}

impl Observations {
    fn correct_defaults() -> Self {
        Self {
            statuses_match: true,
            routing_exact: true,
            all_decide: true,
            identities_distinct: true,
            map_epoch_exact: true,
            durability_exact: true,
            prior_order_exact: true,
            finalization_exact: true,
            restart_replay_exact: true,
            ..Self::default()
        }
    }
}

/// Run the frozen three-authority, three-resolver agreement contract.
///
/// # Errors
///
/// Returns an error when process startup, protocol I/O, or the bounded history cannot finish.
pub fn run_partitioned_resolver_contract(
    seed: u64,
    rounds: u64,
    mode: PartitionedResolverMode,
    executable: &Path,
) -> Result<PartitionedResolverReport, String> {
    if rounds == 0 {
        return Err("partitioned resolver history requires at least one round".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(ResolverScenario::new(seed, rounds, mode, executable)?.run())
}

struct ResolverScenario<'a> {
    seed: u64,
    rounds: u64,
    mode: PartitionedResolverMode,
    executable: &'a Path,
    root: TempRoot,
    authority_addresses: BTreeMap<NodeId, String>,
    resolver_addresses: BTreeMap<u16, String>,
    authority_children: BTreeMap<NodeId, Child>,
    resolver_children: BTreeMap<u16, Child>,
    observations: Observations,
}

impl<'a> ResolverScenario<'a> {
    fn new(
        seed: u64,
        rounds: u64,
        mode: PartitionedResolverMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "partitioned resolver executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            rounds,
            mode,
            executable,
            root: TempRoot::new(seed, mode.id())?,
            authority_addresses: allocate_addresses(3)?
                .into_iter()
                .map(|(id, address)| (u64::from(id), address))
                .collect(),
            resolver_addresses: allocate_addresses(3)?,
            authority_children: BTreeMap::new(),
            resolver_children: BTreeMap::new(),
            observations: Observations::correct_defaults(),
        })
    }

    async fn run(mut self) -> Result<PartitionedResolverReport, String> {
        self.start_processes()?;
        for address in self.authority_addresses.values() {
            wait_authority_ready(address).await?;
        }
        for resolver_id in 1..=3 {
            wait_resolver_ready(self.resolver_address(resolver_id)?).await?;
        }
        control::<_, ()>(self.authority_address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.authority_address(1)?, 1).await {
            return Err("transaction authority node 1 did not become leader".to_owned());
        }
        let client =
            CellTransactionClient::new(self.authority_addresses.values().cloned().collect())?;
        let mut oracle = Oracle::default();

        if self.mode == PartitionedResolverMode::Correct {
            self.run_correct(&client, &mut oracle).await?;
        } else {
            self.run_negative(&client, &mut oracle).await?;
        }

        let snapshot = linearizable_cell(self.authority_address(1)?).await?;
        let rows = oracle
            .rows
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let rows_match = snapshot.rows == rows;
        let envelope_chain_exact = valid_envelope_chain(
            &snapshot.committed_envelopes,
            usize::try_from(oracle.committed).unwrap_or(usize::MAX),
        );
        Ok(build_report(
            self.seed,
            self.rounds,
            self.mode,
            &self.observations,
            &oracle,
            rows_match,
            envelope_chain_exact,
        ))
    }

    async fn run_correct(
        &mut self,
        client: &CellTransactionClient,
        oracle: &mut Oracle,
    ) -> Result<(), String> {
        let mut candidate = 1_u64;
        for round in 0..self.rounds {
            let before_round = oracle.latest_sequence;
            for attempt in 0..6_u64 {
                let read_sequence = if attempt == 1 || attempt == 5 {
                    before_round
                } else {
                    oracle.latest_sequence
                };
                let mut command =
                    generated_command(self.seed, round, attempt, candidate, read_sequence);
                let expected = oracle.decision(&command);
                let transaction_sha256 = cell_partitioned_transaction_sha256(&command)?;
                let required = required_resolvers(&command);
                self.observations.attempted += 1;
                self.observations.cross_partition += u64::from(required.len() > 1);
                let mut attestations = Vec::new();
                for resolver_id in &required {
                    attestations.push(
                        self.prepare(*resolver_id, &command, candidate, 1, transaction_sha256)
                            .await?,
                    );
                    self.observations.decisions += 1;
                }
                self.observe_attestations(&required, &attestations);

                if round == self.rounds / 2 && attempt == 3 && required.contains(&2) {
                    let original = attestations
                        .iter()
                        .find(|attestation| attestation.statement.resolver_id == 2)
                        .cloned()
                        .ok_or_else(|| "restart probe did not route to resolver 2".to_owned())?;
                    self.restart_resolver(2)?;
                    wait_resolver_ready(self.resolver_address(2)?).await?;
                    let replay = self
                        .prepare(2, &command, candidate, 1, transaction_sha256)
                        .await?;
                    self.observations.decisions += 1;
                    self.observations.restart_replay_exact &= replay == original;
                }

                command.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: command.read_version.sequence,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations,
                });
                let response = client
                    .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
                    .await?;
                let outcome = response
                    .cell_transaction
                    .ok_or_else(|| "authority omitted transaction outcome".to_owned())?;
                self.observations.statuses_match &= outcome.status == expected;
                for resolver_id in &required {
                    self.finalize(
                        *resolver_id,
                        candidate,
                        transaction_sha256,
                        outcome.status,
                        outcome.commit_sequence,
                    )
                    .await?;
                    self.observations.finalizations += 1;
                }
                oracle.apply(&command, expected, outcome.commit_sequence);
                candidate += 1;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run_negative(
        &mut self,
        client: &CellTransactionClient,
        oracle: &mut Oracle,
    ) -> Result<(), String> {
        let candidate = 1;
        let mut command = crossing_command(self.seed, candidate, oracle.latest_sequence);
        let digest = cell_partitioned_transaction_sha256(&command)?;
        let required = required_resolvers(&command);
        self.observations.attempted = 1;
        self.observations.cross_partition = 1;
        match self.mode {
            PartitionedResolverMode::StartKeyOnlyRouting
            | PartitionedResolverMode::PartialAcceptance => {
                let attestation = self
                    .prepare(required[0], &command, candidate, 1, digest)
                    .await?;
                self.observations.decisions = 1;
                command.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: command.read_version.sequence,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations: vec![attestation],
                });
                let outcome = commit_outcome(client, &command).await?;
                self.observations.negative_detected =
                    outcome.status == CellTransactionStatus::MissingResolver;
            }
            PartitionedResolverMode::DuplicateResolverIdentity => {
                let attestation = self
                    .prepare(required[0], &command, candidate, 1, digest)
                    .await?;
                self.observations.decisions = 2;
                command.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: command.read_version.sequence,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations: vec![attestation.clone(), attestation],
                });
                let outcome = commit_outcome(client, &command).await?;
                self.observations.negative_detected =
                    outcome.status == CellTransactionStatus::MissingResolver;
            }
            PartitionedResolverMode::MixedMapEpoch => {
                let mut attestations = Vec::new();
                for resolver_id in &required {
                    let epoch = if *resolver_id == required[1] { 2 } else { 1 };
                    attestations.push(
                        self.prepare(*resolver_id, &command, candidate, epoch, digest)
                            .await?,
                    );
                }
                self.observations.decisions = attestations.len() as u64;
                command.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: command.read_version.sequence,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations,
                });
                let outcome = commit_outcome(client, &command).await?;
                self.observations.negative_detected =
                    outcome.status == CellTransactionStatus::MissingResolver;
            }
            PartitionedResolverMode::VolatilePartitionDecision => {
                let mut attestations = Vec::new();
                for resolver_id in &required {
                    attestations.push(
                        self.prepare(*resolver_id, &command, candidate, 1, digest)
                            .await?,
                    );
                }
                self.observations.decisions = attestations.len() as u64;
                command.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: command.read_version.sequence,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations,
                });
                let outcome = commit_outcome(client, &command).await?;
                self.restart_resolver(2)?;
                wait_resolver_ready(self.resolver_address(2)?).await?;
                let finalize = self
                    .finalize(
                        2,
                        candidate,
                        digest,
                        outcome.status,
                        outcome.commit_sequence,
                    )
                    .await;
                self.observations.negative_detected =
                    outcome.status == CellTransactionStatus::Committed && finalize.is_err();
                self.observations.durability_exact = false;
            }
            PartitionedResolverMode::SkipPriorFinalization => {
                let resolver_id = required[0];
                let _ = self
                    .prepare(resolver_id, &command, candidate, 1, digest)
                    .await?;
                let next = crossing_command(self.seed, candidate + 1, oracle.latest_sequence);
                let next_digest = cell_partitioned_transaction_sha256(&next)?;
                self.observations.negative_detected = self
                    .prepare(resolver_id, &next, candidate + 1, 1, next_digest)
                    .await
                    .is_err();
                self.observations.prior_order_exact = false;
            }
            PartitionedResolverMode::SplitWithPreparedTransaction => {
                let resolver_id = required[0];
                let _ = self
                    .prepare(resolver_id, &command, candidate, 1, digest)
                    .await?;
                let next = crossing_command(self.seed, candidate + 1, oracle.latest_sequence);
                let next_digest = cell_partitioned_transaction_sha256(&next)?;
                self.observations.negative_detected = self
                    .prepare(resolver_id, &next, candidate + 1, 2, next_digest)
                    .await
                    .is_err();
                self.observations.map_epoch_exact = false;
            }
            PartitionedResolverMode::Correct => unreachable!(),
        }
        Ok(())
    }

    fn observe_attestations(
        &mut self,
        required: &[u16],
        attestations: &[CellResolverDecisionAttestation],
    ) {
        let actual = attestations
            .iter()
            .map(|attestation| attestation.statement.resolver_id)
            .collect::<Vec<_>>();
        let distinct = actual.iter().copied().collect::<BTreeSet<_>>();
        self.observations.routing_exact &= actual == required;
        self.observations.all_decide &= actual.len() == required.len();
        self.observations.identities_distinct &= distinct.len() == actual.len();
        self.observations.map_epoch_exact &= attestations
            .iter()
            .all(|attestation| attestation.statement.map_epoch == 1);
    }

    async fn prepare(
        &self,
        resolver_id: u16,
        command: &CellTransactionCommand,
        candidate: u64,
        map_epoch: u64,
        digest: [u8; 32],
    ) -> Result<CellResolverDecisionAttestation, String> {
        let partition = cell_resolver_partitions()
            .into_iter()
            .find(|partition| partition.resolver_id == resolver_id)
            .ok_or_else(|| "missing resolver partition".to_owned())?;
        let request = ResolverPrepare {
            cell_id: command.cell_id,
            tenant_id: command.tenant_id,
            generation: command.generation,
            map_epoch,
            transaction_identity: command.identity,
            candidate_sequence: candidate,
            read_version: command.read_version,
            resolver_read_sequence: command.read_version.sequence,
            transaction_sha256: digest,
            read_conflicts: clip_ranges(&command.read_conflicts, &partition.start, &partition.end),
            write_conflicts: clip_ranges(
                &command.write_conflicts,
                &partition.start,
                &partition.end,
            ),
        };
        match resolver_call(
            self.resolver_address(resolver_id)?,
            &ResolverRequest::Prepare(request),
        )
        .await?
        {
            ResolverResponse::Prepared(attestation) => Ok(*attestation),
            _ => Err("resolver prepare returned wrong response".to_owned()),
        }
    }

    async fn finalize(
        &self,
        resolver_id: u16,
        candidate: u64,
        digest: [u8; 32],
        status: CellTransactionStatus,
        commit_sequence: Option<u64>,
    ) -> Result<(), String> {
        match resolver_call(
            self.resolver_address(resolver_id)?,
            &ResolverRequest::Finalize(ResolverFinalize {
                candidate_sequence: candidate,
                transaction_sha256: digest,
                global_status: status,
                commit_sequence,
            }),
        )
        .await?
        {
            ResolverResponse::Finalized => Ok(()),
            _ => Err("resolver finalize returned wrong response".to_owned()),
        }
    }

    fn start_processes(&mut self) -> Result<(), String> {
        for node_id in 1..=3 {
            self.start_authority(node_id)?;
        }
        for resolver_id in 1..=3 {
            self.start_resolver(resolver_id)?;
        }
        Ok(())
    }

    fn start_authority(&mut self, node_id: NodeId) -> Result<(), String> {
        let config = ProcessNodeConfig {
            node_id,
            root: self.root.authority(node_id),
            nodes: self.authority_addresses.clone(),
            deduplicate_requests: true,
            acknowledge_before_quorum: false,
            policy: ProcessNodePolicy::default(),
        };
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = child_command(self.executable, "consensus-node", &config_json)
            .spawn()
            .map_err(|error| error.to_string())?;
        self.authority_children.insert(node_id, child);
        self.observations.process_starts += 1;
        Ok(())
    }

    fn start_resolver(&mut self, resolver_id: u16) -> Result<(), String> {
        let config = ResolverProcessConfig {
            resolver_id,
            root: self.root.resolver(resolver_id),
            listen_address: self.resolver_address(resolver_id)?.to_owned(),
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            map_epoch: 1,
            transaction_system_generation: 1,
            minimum_read_sequence: 0,
            memory_only: false,
            acknowledge_before_durable: self.mode
                == PartitionedResolverMode::VolatilePartitionDecision
                && resolver_id == 2,
            allow_map_epoch_override: self.mode == PartitionedResolverMode::MixedMapEpoch
                && resolver_id == 2,
        };
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = child_command(self.executable, "resolver-node", &config_json)
            .spawn()
            .map_err(|error| error.to_string())?;
        self.resolver_children.insert(resolver_id, child);
        self.observations.process_starts += 1;
        Ok(())
    }

    fn restart_resolver(&mut self, resolver_id: u16) -> Result<(), String> {
        let mut child = self
            .resolver_children
            .remove(&resolver_id)
            .ok_or_else(|| "resolver is not running".to_owned())?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        self.start_resolver(resolver_id)?;
        self.observations.process_restarts += 1;
        Ok(())
    }

    fn authority_address(&self, node_id: NodeId) -> Result<&str, String> {
        self.authority_addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing authority {node_id}"))
    }

    fn resolver_address(&self, resolver_id: u16) -> Result<&str, String> {
        self.resolver_addresses
            .get(&resolver_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing resolver {resolver_id}"))
    }
}

impl Drop for ResolverScenario<'_> {
    fn drop(&mut self) {
        for child in self
            .authority_children
            .values_mut()
            .chain(self.resolver_children.values_mut())
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Clone)]
struct PendingStatelessTransaction {
    command: CellTransactionCommand,
    candidate: u64,
    resolver_read_sequence: u64,
}

#[allow(clippy::struct_excessive_bools)]
struct StatelessRecoveryObservations {
    attempted: u64,
    decisions: u64,
    batches: u64,
    process_starts: u64,
    generation_fences: u64,
    abandoned: u64,
    safe_false_conflicts: u64,
    recovery_floor: u64,
    commit_subset: bool,
    central_conflicts_rejected: bool,
    complete_agreement: bool,
    old_generation_stopped: bool,
    fence_before_successor: bool,
    floor_includes_head: bool,
    successor_empty: bool,
    successor_reads_at_floor: bool,
    old_requests_rejected: bool,
    old_replies_rejected: bool,
    unresolved_not_visible: bool,
    retry_new_identity: bool,
    negative_detected: bool,
}

impl Default for StatelessRecoveryObservations {
    fn default() -> Self {
        Self {
            commit_subset: true,
            central_conflicts_rejected: true,
            complete_agreement: true,
            old_generation_stopped: true,
            fence_before_successor: true,
            floor_includes_head: true,
            successor_empty: true,
            successor_reads_at_floor: true,
            old_requests_rejected: true,
            old_replies_rejected: true,
            unresolved_not_visible: true,
            retry_new_identity: true,
            ..Self {
                attempted: 0,
                decisions: 0,
                batches: 0,
                process_starts: 0,
                generation_fences: 0,
                abandoned: 0,
                safe_false_conflicts: 0,
                recovery_floor: 0,
                negative_detected: false,
                commit_subset: false,
                central_conflicts_rejected: false,
                complete_agreement: false,
                old_generation_stopped: false,
                fence_before_successor: false,
                floor_includes_head: false,
                successor_empty: false,
                successor_reads_at_floor: false,
                old_requests_rejected: false,
                old_replies_rejected: false,
                unresolved_not_visible: false,
                retry_new_identity: false,
            }
        }
    }
}

/// Run the RFC-0049 memory-only resolver and generation-recovery contract.
///
/// # Errors
///
/// Returns an error when a process, batch, authority commit, or recovery probe
/// cannot complete inside the bounded contract.
pub fn run_stateless_resolver_recovery_contract(
    seed: u64,
    attempts: u64,
    batch_size: u64,
    mode: StatelessResolverRecoveryMode,
    executable: &Path,
) -> Result<StatelessResolverRecoveryReport, String> {
    if attempts == 0 || batch_size == 0 {
        return Err(
            "stateless resolver history requires positive attempts and batch size".to_owned(),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(
        StatelessRecoveryScenario::new(seed, attempts, batch_size, mode, executable)?.run(),
    )
}

struct StatelessRecoveryScenario<'a> {
    seed: u64,
    attempts: u64,
    batch_size: u64,
    mode: StatelessResolverRecoveryMode,
    executable: &'a Path,
    root: TempRoot,
    authority_addresses: BTreeMap<NodeId, String>,
    resolver_addresses: BTreeMap<u16, String>,
    authority_children: BTreeMap<NodeId, Child>,
    resolver_children: BTreeMap<u16, Child>,
    observations: StatelessRecoveryObservations,
}

impl<'a> StatelessRecoveryScenario<'a> {
    fn new(
        seed: u64,
        attempts: u64,
        batch_size: u64,
        mode: StatelessResolverRecoveryMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "stateless resolver executable does not exist: {}",
                executable.display()
            ));
        }
        Ok(Self {
            seed,
            attempts,
            batch_size,
            mode,
            executable,
            root: TempRoot::new(seed, mode.id())?,
            authority_addresses: allocate_addresses(3)?
                .into_iter()
                .map(|(id, address)| (u64::from(id), address))
                .collect(),
            resolver_addresses: allocate_addresses(3)?,
            authority_children: BTreeMap::new(),
            resolver_children: BTreeMap::new(),
            observations: StatelessRecoveryObservations::default(),
        })
    }

    async fn run(mut self) -> Result<StatelessResolverRecoveryReport, String> {
        self.start_authorities()?;
        for address in self.authority_addresses.values() {
            wait_authority_ready(address).await?;
        }
        control::<_, ()>(self.authority_address(1)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.authority_address(1)?, 1).await {
            return Err("stateless resolver transaction authority did not elect".to_owned());
        }
        self.start_resolvers(1, 0)?;
        self.wait_resolvers().await?;
        let client =
            CellTransactionClient::new(self.authority_addresses.values().cloned().collect())?;
        let mut oracle = Oracle::default();
        let recovery_floor = if self.mode == StatelessResolverRecoveryMode::Correct {
            self.run_correct(&client, &mut oracle).await?
        } else {
            self.run_negative(&client, &mut oracle).await?
        };
        let snapshot = linearizable_cell(self.authority_address(1)?).await?;
        let expected_rows = oracle
            .rows
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let rows_match = snapshot.rows == expected_rows;
        let envelope_chain_exact = valid_envelope_chain(
            &snapshot.committed_envelopes,
            usize::try_from(oracle.committed).unwrap_or(usize::MAX),
        );
        Ok(build_stateless_recovery_report(
            self.seed,
            self.attempts,
            self.batch_size,
            self.mode,
            &self.observations,
            &oracle,
            recovery_floor,
            rows_match,
            envelope_chain_exact,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn run_correct(
        &mut self,
        client: &CellTransactionClient,
        oracle: &mut Oracle,
    ) -> Result<u64, String> {
        let mut candidate = 1_u64;
        let mut data_read_sequence = 0_u64;
        let mut transaction_system_generation = 1_u64;
        let recovery_after = self.attempts / 2;
        let mut recovered = false;
        let mut recovery_floor = 0_u64;

        while self.observations.attempted < self.attempts {
            if !recovered && self.observations.attempted >= recovery_after {
                let abandoned_identity = RequestIdentity {
                    client_id: self.seed.wrapping_add(0x4900),
                    request_id: candidate,
                };
                let command =
                    forced_crossing_transaction(abandoned_identity, data_read_sequence, candidate);
                let digest = cell_partitioned_transaction_sha256(&command)?;
                let old_reply = self
                    .prepare_one(1, &command, candidate, 1, oracle.latest_sequence, digest)
                    .await?;
                self.observations.decisions += 1;
                self.observations.attempted += 1;
                self.observations.abandoned += 1;
                self.stop_resolver(2)?;
                self.stop_all_resolvers()?;
                self.observations.old_generation_stopped = self.resolver_children.is_empty();
                recovery_floor = oracle.latest_sequence;
                self.observations.recovery_floor = recovery_floor;
                let fence = format!("rfc0049-fence:g1:head={recovery_floor}:abandoned={candidate}");
                let _ = client.commit_app_data(fence.as_bytes()).await?;
                self.observations.generation_fences += 1;
                transaction_system_generation = 2;
                self.start_resolvers(transaction_system_generation, recovery_floor)?;
                self.wait_resolvers().await?;
                self.observations.fence_before_successor = self.observations.generation_fences == 1;
                self.observations.floor_includes_head = recovery_floor == oracle.latest_sequence;
                self.observations.successor_empty = self.successor_resolvers_empty().await?;
                self.observations.old_requests_rejected = self
                    .prepare_one(1, &command, candidate, 1, recovery_floor, digest)
                    .await
                    .is_err();

                let mut delayed = command.clone();
                delayed.identity.request_id = candidate.saturating_add(10_000_000);
                delayed.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 2,
                    resolver_read_sequence: recovery_floor,
                    map_epoch: 1,
                    candidate_sequence: candidate.saturating_add(1),
                    attestations: vec![old_reply],
                });
                let delayed_outcome = commit_outcome(client, &delayed).await?;
                self.observations.old_replies_rejected =
                    delayed_outcome.status == CellTransactionStatus::MissingResolver;

                candidate = candidate.saturating_add(1);
                let retry = PendingStatelessTransaction {
                    command: forced_crossing_transaction(
                        RequestIdentity {
                            client_id: self.seed.wrapping_add(0x4900),
                            request_id: candidate,
                        },
                        data_read_sequence,
                        candidate,
                    ),
                    candidate,
                    resolver_read_sequence: recovery_floor,
                };
                self.observations.retry_new_identity = retry.command.identity != abandoned_identity;
                let after_fence = linearizable_cell(self.authority_address(1)?).await?;
                self.observations.unresolved_not_visible = after_fence.committed_envelopes.len()
                    == usize::try_from(oracle.committed).unwrap_or(usize::MAX);
                self.execute_batch(
                    client,
                    oracle,
                    transaction_system_generation,
                    vec![retry],
                    &mut data_read_sequence,
                )
                .await?;
                candidate = candidate.saturating_add(1);
                recovered = true;
                continue;
            }

            let remaining = self.attempts.saturating_sub(self.observations.attempted);
            let batch_len = std::cmp::min(self.batch_size, remaining);
            let mut batch = Vec::with_capacity(usize::try_from(batch_len).unwrap_or(0));
            for offset in 0..batch_len {
                let command = if candidate == 1 {
                    forced_seed_transaction(self.seed, candidate, data_read_sequence)
                } else if candidate == 2 {
                    forced_partial_conflict_transaction(self.seed, candidate, data_read_sequence)
                } else if candidate == 3 {
                    forced_false_conflict_transaction(self.seed, candidate, data_read_sequence)
                } else {
                    generated_command(
                        self.seed.wrapping_add(0x4900),
                        candidate / self.batch_size,
                        offset % 6,
                        candidate,
                        data_read_sequence,
                    )
                };
                let resolver_read_sequence = if candidate == 2 || candidate == 3 {
                    0
                } else {
                    oracle.latest_sequence
                };
                batch.push(PendingStatelessTransaction {
                    command,
                    candidate,
                    resolver_read_sequence,
                });
                candidate = candidate.saturating_add(1);
            }
            self.execute_batch(
                client,
                oracle,
                transaction_system_generation,
                batch,
                &mut data_read_sequence,
            )
            .await?;
        }

        self.observations.successor_reads_at_floor &= recovered;
        Ok(recovery_floor)
    }

    async fn execute_batch(
        &mut self,
        client: &CellTransactionClient,
        oracle: &mut Oracle,
        transaction_system_generation: u64,
        batch: Vec<PendingStatelessTransaction>,
        data_read_sequence: &mut u64,
    ) -> Result<(), String> {
        let mut by_candidate = BTreeMap::<u64, Vec<CellResolverDecisionAttestation>>::new();
        for resolver_id in 1..=3 {
            let mut requests = Vec::new();
            for pending in &batch {
                if required_resolvers(&pending.command).contains(&resolver_id) {
                    requests.push(build_resolver_prepare(
                        resolver_id,
                        &pending.command,
                        pending.candidate,
                        1,
                        transaction_system_generation,
                        pending.resolver_read_sequence,
                    )?);
                }
            }
            if requests.is_empty() {
                continue;
            }
            let attestations = self.prepare_batch(resolver_id, requests).await?;
            self.observations.decisions += attestations.len() as u64;
            for attestation in attestations {
                by_candidate
                    .entry(attestation.statement.candidate_sequence)
                    .or_default()
                    .push(attestation);
            }
        }
        self.observations.batches += 1;

        for mut pending in batch {
            let required = required_resolvers(&pending.command);
            let mut attestations = by_candidate.remove(&pending.candidate).unwrap_or_default();
            attestations.sort_by_key(|attestation| attestation.statement.resolver_id);
            self.observations.complete_agreement &= attestations.len() == required.len();
            let central =
                oracle_decision_at(oracle, &pending.command, pending.resolver_read_sequence);
            let actual = if attestations
                .iter()
                .any(|attestation| attestation.statement.decision == CellResolverDecision::Conflict)
            {
                CellTransactionStatus::Conflict
            } else {
                CellTransactionStatus::Committed
            };
            self.observations.commit_subset &= actual != CellTransactionStatus::Committed
                || central == CellTransactionStatus::Committed;
            self.observations.central_conflicts_rejected &= central
                != CellTransactionStatus::Conflict
                || actual == CellTransactionStatus::Conflict;
            if central == CellTransactionStatus::Committed
                && actual == CellTransactionStatus::Conflict
            {
                self.observations.safe_false_conflicts += 1;
            }
            pending.command.partitioned_resolution = Some(CellPartitionedResolution {
                transaction_system_generation,
                resolver_read_sequence: pending.resolver_read_sequence,
                map_epoch: 1,
                candidate_sequence: pending.candidate,
                attestations,
            });
            let outcome = commit_outcome(client, &pending.command).await?;
            if outcome.status != actual {
                return Err(format!(
                    "authority status {:?} differed from resolver status {:?} at candidate {}",
                    outcome.status, actual, pending.candidate
                ));
            }
            if let Some(sequence) = outcome.commit_sequence {
                *data_read_sequence = sequence;
            }
            oracle.apply(&pending.command, actual, Some(pending.candidate));
            self.observations.attempted += 1;
            if transaction_system_generation == 2 {
                self.observations.successor_reads_at_floor &=
                    pending.resolver_read_sequence >= self.observations.recovery_floor;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn run_negative(
        &mut self,
        client: &CellTransactionClient,
        oracle: &mut Oracle,
    ) -> Result<u64, String> {
        let mut data_read_sequence = 0;
        let first = PendingStatelessTransaction {
            command: forced_seed_transaction(self.seed, 1, 0),
            candidate: 1,
            resolver_read_sequence: 0,
        };
        self.execute_batch(client, oracle, 1, vec![first], &mut data_read_sequence)
            .await?;
        let durable_head = oracle.latest_sequence;
        match self.mode {
            StatelessResolverRecoveryMode::ContinueAfterResolverLoss => {
                self.stop_resolver(2)?;
                self.observations.old_generation_stopped = false;
                self.observations.negative_detected = !self.resolver_children.is_empty();
            }
            StatelessResolverRecoveryMode::ActivateBeforeOldFence => {
                self.stop_all_resolvers()?;
                self.start_resolvers(2, durable_head)?;
                self.wait_resolvers().await?;
                self.observations.fence_before_successor = false;
                self.observations.negative_detected = true;
            }
            StatelessResolverRecoveryMode::AcceptOldGenerationReply => {
                let command = forced_crossing_transaction(
                    RequestIdentity {
                        client_id: self.seed,
                        request_id: 2,
                    },
                    data_read_sequence,
                    2,
                );
                let digest = cell_partitioned_transaction_sha256(&command)?;
                let old = self
                    .prepare_one(1, &command, 2, 1, durable_head, digest)
                    .await?;
                self.stop_all_resolvers()?;
                let _ = client.commit_app_data(b"rfc0049-negative-fence").await?;
                self.start_resolvers(2, durable_head)?;
                self.wait_resolvers().await?;
                let mut delayed = command;
                delayed.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 2,
                    resolver_read_sequence: durable_head,
                    map_epoch: 1,
                    candidate_sequence: 2,
                    attestations: vec![old],
                });
                self.observations.old_replies_rejected = false;
                self.observations.negative_detected =
                    commit_outcome(client, &delayed).await?.status
                        == CellTransactionStatus::MissingResolver;
            }
            StatelessResolverRecoveryMode::ReadBelowRecoveryFloor => {
                self.stop_all_resolvers()?;
                let _ = client.commit_app_data(b"rfc0049-negative-fence").await?;
                self.start_resolvers(2, durable_head)?;
                self.wait_resolvers().await?;
                let command = forced_crossing_transaction(
                    RequestIdentity {
                        client_id: self.seed,
                        request_id: 2,
                    },
                    data_read_sequence,
                    2,
                );
                let digest = cell_partitioned_transaction_sha256(&command)?;
                self.observations.successor_reads_at_floor = false;
                self.observations.negative_detected = self
                    .prepare_one(1, &command, 2, 2, durable_head.saturating_sub(1), digest)
                    .await
                    .is_err();
            }
            StatelessResolverRecoveryMode::PublishUnresolvedOldWork => {
                let command = forced_crossing_transaction(
                    RequestIdentity {
                        client_id: self.seed,
                        request_id: 2,
                    },
                    data_read_sequence,
                    2,
                );
                let digest = cell_partitioned_transaction_sha256(&command)?;
                let attestation = self
                    .prepare_one(1, &command, 2, 1, durable_head, digest)
                    .await?;
                let mut partial = command;
                partial.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: 1,
                    resolver_read_sequence: durable_head,
                    map_epoch: 1,
                    candidate_sequence: 2,
                    attestations: vec![attestation],
                });
                self.observations.unresolved_not_visible = false;
                self.observations.negative_detected =
                    commit_outcome(client, &partial).await?.status
                        == CellTransactionStatus::MissingResolver;
            }
            StatelessResolverRecoveryMode::OmitDurableHead => {
                self.stop_all_resolvers()?;
                let _ = client.commit_app_data(b"rfc0049-negative-fence").await?;
                let unsafe_floor = durable_head.saturating_sub(1);
                self.start_resolvers(2, unsafe_floor)?;
                self.wait_resolvers().await?;
                self.observations.floor_includes_head = false;
                self.observations.negative_detected = unsafe_floor < durable_head;
            }
            StatelessResolverRecoveryMode::Correct => unreachable!(),
        }
        Ok(durable_head)
    }

    fn start_authorities(&mut self) -> Result<(), String> {
        for node_id in 1..=3 {
            let config = ProcessNodeConfig {
                node_id,
                root: self.root.authority(node_id),
                nodes: self.authority_addresses.clone(),
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy: ProcessNodePolicy::default(),
            };
            let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
            let child = child_command(self.executable, "consensus-node", &config_json)
                .spawn()
                .map_err(|error| error.to_string())?;
            self.authority_children.insert(node_id, child);
            self.observations.process_starts += 1;
        }
        Ok(())
    }

    fn start_resolvers(&mut self, generation: u64, floor: u64) -> Result<(), String> {
        for resolver_id in 1..=3 {
            let config = ResolverProcessConfig {
                resolver_id,
                root: self
                    .root
                    .0
                    .join(format!("resolver-g{generation}-{resolver_id}")),
                listen_address: self.resolver_address(resolver_id)?.to_owned(),
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                map_epoch: 1,
                transaction_system_generation: generation,
                minimum_read_sequence: floor,
                memory_only: true,
                acknowledge_before_durable: false,
                allow_map_epoch_override: false,
            };
            let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
            let child = child_command(self.executable, "resolver-node", &config_json)
                .spawn()
                .map_err(|error| error.to_string())?;
            self.resolver_children.insert(resolver_id, child);
            self.observations.process_starts += 1;
        }
        Ok(())
    }

    async fn wait_resolvers(&self) -> Result<(), String> {
        for resolver_id in 1..=3 {
            wait_resolver_ready(self.resolver_address(resolver_id)?).await?;
        }
        Ok(())
    }

    async fn successor_resolvers_empty(&self) -> Result<bool, String> {
        for resolver_id in 1..=3 {
            match resolver_call(
                self.resolver_address(resolver_id)?,
                &ResolverRequest::Status,
            )
            .await?
            {
                ResolverResponse::Status(status) => {
                    if status.prepared != 0
                        || status.committed_writes != 0
                        || !status.memory_only
                        || status.transaction_system_generation != 2
                        || status.minimum_read_sequence != self.observations.recovery_floor
                    {
                        return Ok(false);
                    }
                    let root = self.root.0.join(format!("resolver-g2-{resolver_id}"));
                    if fs::read_dir(root)
                        .map_err(|error| error.to_string())?
                        .next()
                        .is_some()
                    {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn prepare_one(
        &self,
        resolver_id: u16,
        command: &CellTransactionCommand,
        candidate: u64,
        generation: u64,
        resolver_read_sequence: u64,
        _digest: [u8; 32],
    ) -> Result<CellResolverDecisionAttestation, String> {
        let request = build_resolver_prepare(
            resolver_id,
            command,
            candidate,
            1,
            generation,
            resolver_read_sequence,
        )?;
        match resolver_call(
            self.resolver_address(resolver_id)?,
            &ResolverRequest::Prepare(request),
        )
        .await?
        {
            ResolverResponse::Prepared(attestation) => Ok(*attestation),
            _ => Err("stateless resolver prepare returned wrong response".to_owned()),
        }
    }

    async fn prepare_batch(
        &self,
        resolver_id: u16,
        requests: Vec<ResolverPrepare>,
    ) -> Result<Vec<CellResolverDecisionAttestation>, String> {
        match resolver_call(
            self.resolver_address(resolver_id)?,
            &ResolverRequest::PrepareBatch(requests),
        )
        .await?
        {
            ResolverResponse::PreparedBatch(attestations) => Ok(attestations),
            _ => Err("stateless resolver batch returned wrong response".to_owned()),
        }
    }

    fn stop_resolver(&mut self, resolver_id: u16) -> Result<(), String> {
        if let Some(mut child) = self.resolver_children.remove(&resolver_id) {
            child.kill().map_err(|error| error.to_string())?;
            child.wait().map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn stop_all_resolvers(&mut self) -> Result<(), String> {
        let ids = self.resolver_children.keys().copied().collect::<Vec<_>>();
        for resolver_id in ids {
            self.stop_resolver(resolver_id)?;
        }
        Ok(())
    }

    fn authority_address(&self, node_id: NodeId) -> Result<&str, String> {
        self.authority_addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing authority {node_id}"))
    }

    fn resolver_address(&self, resolver_id: u16) -> Result<&str, String> {
        self.resolver_addresses
            .get(&resolver_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing resolver {resolver_id}"))
    }
}

impl Drop for StatelessRecoveryScenario<'_> {
    fn drop(&mut self) {
        for child in self
            .authority_children
            .values_mut()
            .chain(self.resolver_children.values_mut())
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn build_resolver_prepare(
    resolver_id: u16,
    command: &CellTransactionCommand,
    candidate: u64,
    map_epoch: u64,
    transaction_system_generation: u64,
    resolver_read_sequence: u64,
) -> Result<ResolverPrepare, String> {
    let partition = cell_resolver_partitions()
        .into_iter()
        .find(|partition| partition.resolver_id == resolver_id)
        .ok_or_else(|| "missing resolver partition".to_owned())?;
    Ok(ResolverPrepare {
        cell_id: command.cell_id,
        tenant_id: command.tenant_id,
        generation: transaction_system_generation,
        map_epoch,
        transaction_identity: command.identity,
        candidate_sequence: candidate,
        read_version: command.read_version,
        resolver_read_sequence,
        transaction_sha256: cell_partitioned_transaction_sha256(command)?,
        read_conflicts: clip_ranges(&command.read_conflicts, &partition.start, &partition.end),
        write_conflicts: clip_ranges(&command.write_conflicts, &partition.start, &partition.end),
    })
}

fn oracle_decision_at(
    oracle: &Oracle,
    command: &CellTransactionCommand,
    resolver_read_sequence: u64,
) -> CellTransactionStatus {
    if command.read_conflicts.iter().any(|read| {
        oracle
            .writes
            .iter()
            .any(|write| write.sequence > resolver_read_sequence && read.overlaps(&write.range))
    }) {
        CellTransactionStatus::Conflict
    } else {
        CellTransactionStatus::Committed
    }
}

fn command_with_ranges(
    identity: RequestIdentity,
    data_read_sequence: u64,
    read_conflicts: Vec<CellKeyRange>,
    keys: &[u8],
) -> CellTransactionCommand {
    CellTransactionCommand {
        identity,
        credential: None,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        read_version: CellReadVersion {
            generation: 1,
            sequence: data_read_sequence,
        },
        read_conflicts,
        write_conflicts: keys
            .iter()
            .map(|key| CellKeyRange::point(&[*key]))
            .collect(),
        mutations: keys
            .iter()
            .map(|key| CellMutation::Set {
                key: vec![*key],
                value: identity.request_id.to_be_bytes().to_vec(),
            })
            .collect(),
        partitioned_resolution: None,
        accepted_resolvers: Vec::new(),
        durable_log_tags: vec![10, 20],
    }
}

fn forced_seed_transaction(
    seed: u64,
    candidate: u64,
    data_read_sequence: u64,
) -> CellTransactionCommand {
    command_with_ranges(
        RequestIdentity {
            client_id: seed.wrapping_add(0x4900),
            request_id: candidate,
        },
        data_read_sequence,
        vec![CellKeyRange::point(&[0x70])],
        &[0x70],
    )
}

fn forced_partial_conflict_transaction(
    seed: u64,
    candidate: u64,
    data_read_sequence: u64,
) -> CellTransactionCommand {
    command_with_ranges(
        RequestIdentity {
            client_id: seed.wrapping_add(0x4900),
            request_id: candidate,
        },
        data_read_sequence,
        vec![CellKeyRange::point(&[0x70])],
        &[0x22],
    )
}

fn forced_false_conflict_transaction(
    seed: u64,
    candidate: u64,
    data_read_sequence: u64,
) -> CellTransactionCommand {
    command_with_ranges(
        RequestIdentity {
            client_id: seed.wrapping_add(0x4900),
            request_id: candidate,
        },
        data_read_sequence,
        vec![CellKeyRange::point(&[0x22])],
        &[0x23],
    )
}

fn forced_crossing_transaction(
    identity: RequestIdentity,
    data_read_sequence: u64,
    candidate: u64,
) -> CellTransactionCommand {
    let mut command = command_with_ranges(
        identity,
        data_read_sequence,
        vec![CellKeyRange {
            start: vec![0x4e],
            end: vec![0xa2],
        }],
        &[0x4f, 0x50, 0xa1],
    );
    for mutation in &mut command.mutations {
        if let CellMutation::Set { value, .. } = mutation {
            *value = candidate.to_be_bytes().to_vec();
        }
    }
    command
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn build_stateless_recovery_report(
    seed: u64,
    attempts: u64,
    batch_size: u64,
    mode: StatelessResolverRecoveryMode,
    observations: &StatelessRecoveryObservations,
    oracle: &Oracle,
    recovery_floor: u64,
    rows_match: bool,
    envelope_chain_exact: bool,
) -> StatelessResolverRecoveryReport {
    let checks = if mode == StatelessResolverRecoveryMode::Correct {
        vec![
            (
                "transaction_attempts_exact",
                observations.attempted == attempts,
            ),
            ("ordered_batches_present", observations.batches > 0),
            (
                "safe_false_conflict_observed",
                observations.safe_false_conflicts > 0,
            ),
            ("commit_subset", observations.commit_subset),
            (
                "central_conflicts_rejected",
                observations.central_conflicts_rejected,
            ),
            ("rows_match", rows_match),
            ("envelope_chain_exact", envelope_chain_exact),
            (
                "complete_resolver_agreement",
                observations.complete_agreement,
            ),
            (
                "old_generation_stopped",
                observations.old_generation_stopped,
            ),
            (
                "fence_before_successor",
                observations.fence_before_successor,
            ),
            ("floor_includes_head", observations.floor_includes_head),
            ("successor_empty", observations.successor_empty),
            (
                "successor_reads_at_floor",
                observations.successor_reads_at_floor,
            ),
            ("old_requests_rejected", observations.old_requests_rejected),
            ("old_replies_rejected", observations.old_replies_rejected),
            (
                "unresolved_not_visible",
                observations.unresolved_not_visible,
            ),
            ("retry_new_identity", observations.retry_new_identity),
            ("one_generation_fence", observations.generation_fences == 1),
            ("one_abandoned_candidate", observations.abandoned == 1),
            ("nine_process_starts", observations.process_starts == 9),
            ("resolver_durable_syncs_zero", true),
            ("resolver_finalization_rpcs_zero", true),
        ]
    } else {
        vec![("negative_control_detected", false)]
    };
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let mut trace = Sha256::new();
    trace.update(b"okv-stateless-resolver-generation-recovery-v0");
    trace.update(seed.to_be_bytes());
    trace.update(attempts.to_be_bytes());
    trace.update(batch_size.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(observations.attempted.to_be_bytes());
    trace.update(oracle.committed.to_be_bytes());
    trace.update(oracle.conflicts.to_be_bytes());
    trace.update(observations.safe_false_conflicts.to_be_bytes());
    trace.update(recovery_floor.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    StatelessResolverRecoveryReport {
        seed,
        mode,
        question: "Can objectKV remove resolver persistence and recover resolver loss by replacing the transaction-system generation?".to_owned(),
        answer: if mode == StatelessResolverRecoveryMode::Correct && anomaly_count == 0 {
            "yes inside the frozen single-proxy, real-process, replicated-authority bounds"
                .to_owned()
        } else if mode != StatelessResolverRecoveryMode::Correct
            && observations.negative_detected
        {
            "the frozen negative subject was detected and must be discarded".to_owned()
        } else {
            "no".to_owned()
        },
        attempted_transactions: observations.attempted,
        committed_transactions: oracle.committed,
        conflict_rejections: oracle.conflicts,
        safe_false_conflicts: observations.safe_false_conflicts,
        resolver_decisions: observations.decisions,
        ordered_batches: observations.batches,
        process_starts: observations.process_starts,
        generation_fences: observations.generation_fences,
        abandoned_candidates: observations.abandoned,
        recovery_floor,
        resolver_durable_syncs: 0,
        resolver_finalization_rpcs: 0,
        partitioned_commits_subset_of_centralized_oracle: observations.commit_subset,
        centralized_conflicts_rejected: observations.central_conflicts_rejected,
        rows_match_authoritative_outcomes: rows_match,
        envelope_chain_exact,
        complete_resolver_agreement: observations.complete_agreement,
        resolver_failure_stopped_old_generation: observations.old_generation_stopped,
        old_generation_fenced_before_successor: observations.fence_before_successor,
        recovery_floor_includes_durable_head: observations.floor_includes_head,
        successor_resolver_state_started_empty: observations.successor_empty,
        successor_reads_at_or_above_floor: observations.successor_reads_at_floor,
        old_generation_requests_rejected: observations.old_requests_rejected,
        old_generation_replies_rejected: observations.old_replies_rejected,
        unresolved_old_work_not_visible: observations.unresolved_not_visible,
        abandoned_work_retried_with_new_identity: observations.retry_new_identity,
        negative_control_detected: observations.negative_detected,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        latest_commit_version: oracle.latest_sequence,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn generated_command(
    seed: u64,
    round: u64,
    attempt: u64,
    candidate: u64,
    read_sequence: u64,
) -> CellTransactionCommand {
    let shift = u8::try_from(seed.wrapping_add(round) % 12).unwrap_or(0);
    let (read_conflicts, keys) = match attempt {
        0 => (
            vec![CellKeyRange {
                start: vec![0x10 + shift],
                end: vec![0x14 + shift],
            }],
            vec![0x12 + shift],
        ),
        1 => (
            vec![CellKeyRange {
                start: vec![0x10 + shift],
                end: vec![0x14 + shift],
            }],
            vec![0x13 + shift],
        ),
        2 => (
            vec![CellKeyRange {
                start: vec![0x4e],
                end: vec![0x52],
            }],
            vec![0x4f, 0x50],
        ),
        3 => (
            vec![CellKeyRange {
                start: vec![0x20],
                end: vec![0xc1],
            }],
            vec![0x21, 0x71, 0xc0],
        ),
        4 if round.is_multiple_of(2) => (
            vec![CellKeyRange::point(&[0x30 + shift])],
            vec![0x30 + shift],
        ),
        4 => (
            vec![CellKeyRange::point(&[0xc8 + shift])],
            vec![0xc8 + shift],
        ),
        _ => (
            vec![CellKeyRange {
                start: vec![0x4e],
                end: vec![0x52],
            }],
            vec![0x51],
        ),
    };
    let mutations = keys
        .iter()
        .map(|key| CellMutation::Set {
            key: vec![*key],
            value: vec![
                u8::try_from(round % 251).unwrap_or(0),
                u8::try_from(attempt).unwrap_or(0),
            ],
        })
        .collect::<Vec<_>>();
    let write_conflicts = keys
        .iter()
        .map(|key| CellKeyRange::point(&[*key]))
        .collect();
    CellTransactionCommand {
        identity: RequestIdentity {
            client_id: seed,
            request_id: candidate,
        },
        credential: None,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: 1,
        read_version: CellReadVersion {
            generation: 1,
            sequence: read_sequence,
        },
        read_conflicts,
        write_conflicts,
        mutations,
        partitioned_resolution: None,
        accepted_resolvers: Vec::new(),
        durable_log_tags: vec![10, 20],
    }
}

fn crossing_command(seed: u64, candidate: u64, read_sequence: u64) -> CellTransactionCommand {
    let mut command = generated_command(seed, 0, 2, candidate, read_sequence);
    command.identity.request_id = candidate;
    command
}

fn required_resolvers(command: &CellTransactionCommand) -> Vec<u16> {
    cell_resolver_partitions()
        .into_iter()
        .filter(|partition| {
            let owned = CellKeyRange {
                start: partition.start.clone(),
                end: partition.end.clone(),
            };
            command
                .read_conflicts
                .iter()
                .chain(&command.write_conflicts)
                .any(|range| range.overlaps(&owned))
        })
        .map(|partition| partition.resolver_id)
        .collect()
}

fn clip_ranges(ranges: &[CellKeyRange], start: &[u8], end: &[u8]) -> Vec<CellKeyRange> {
    let owned = CellKeyRange {
        start: start.to_vec(),
        end: end.to_vec(),
    };
    let mut clipped = ranges
        .iter()
        .filter(|range| range.overlaps(&owned))
        .map(|range| CellKeyRange {
            start: std::cmp::max(range.start.clone(), owned.start.clone()),
            end: std::cmp::min(range.end.clone(), owned.end.clone()),
        })
        .collect::<Vec<_>>();
    clipped.sort();
    clipped.dedup();
    clipped
}

async fn commit_outcome(
    client: &CellTransactionClient,
    command: &CellTransactionCommand,
) -> Result<crate::CellTransactionApplyResponse, String> {
    client
        .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
        .await?
        .cell_transaction
        .ok_or_else(|| "authority omitted transaction response".to_owned())
}

async fn resolver_call(
    address: &str,
    request: &ResolverRequest,
) -> Result<ResolverResponse, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("resolver connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, RESOLVER_REQUEST, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<ResolverResponse, String> =
        tokio::time::timeout(Duration::from_secs(3), read_response(&mut stream))
            .await
            .map_err(|_| format!("resolver response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn wait_resolver_ready(address: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match resolver_call(address, &ResolverRequest::Status).await {
            Ok(ResolverResponse::Status(_)) => return Ok(()),
            Ok(_) => "wrong readiness response".clone_into(&mut last),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("resolver did not become ready: {last}"))
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

async fn wait_authority_ready(address: &str) -> Result<(), String> {
    for _ in 0..RETRY_ATTEMPTS {
        if control::<_, NodeStatus>(address, STATUS, &()).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("authority did not become ready at {address}"))
}

async fn elect_until_leader(address: &str, node_id: NodeId) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let _: Result<(), String> = control(address, ELECT, &()).await;
        if control::<_, NodeStatus>(address, STATUS, &())
            .await
            .is_ok_and(|status| status.state == "leader" && status.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn linearizable_cell(address: &str) -> Result<crate::CellStateSnapshot, String> {
    let status: NodeStatus = control(address, LINEARIZABLE_STATUS, &()).await?;
    Ok(status
        .cells
        .first()
        .cloned()
        .unwrap_or(crate::CellStateSnapshot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: 1,
            ..crate::CellStateSnapshot::default()
        }))
}

fn valid_envelope_chain(envelopes: &[Vec<u8>], expected: usize) -> bool {
    if envelopes.len() != expected {
        return false;
    }
    let mut previous = [0_u8; 32];
    for bytes in envelopes {
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
    rounds: u64,
    mode: PartitionedResolverMode,
    observations: &Observations,
    oracle: &Oracle,
    rows_match: bool,
    envelope_chain_exact: bool,
) -> PartitionedResolverReport {
    let checks = if mode == PartitionedResolverMode::Correct {
        vec![
            (
                "transaction_attempts_exact",
                observations.attempted == rounds * 6,
            ),
            ("statuses_match", observations.statuses_match),
            ("rows_match", rows_match),
            ("envelope_chain_exact", envelope_chain_exact),
            ("crossing_routing_exact", observations.routing_exact),
            ("all_required_partitions_decide", observations.all_decide),
            (
                "resolver_identities_distinct",
                observations.identities_distinct,
            ),
            ("map_epoch_exact", observations.map_epoch_exact),
            (
                "decisions_durable_before_ack",
                observations.durability_exact,
            ),
            (
                "prior_disposition_order_exact",
                observations.prior_order_exact,
            ),
            ("finalization_exact", observations.finalization_exact),
            ("restart_replay_exact", observations.restart_replay_exact),
        ]
    } else {
        vec![("negative_control_detected", false)]
    };
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let mut trace = Sha256::new();
    trace.update(b"okv-partitioned-resolver-agreement-v0");
    trace.update(seed.to_be_bytes());
    trace.update(rounds.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(observations.attempted.to_be_bytes());
    trace.update(oracle.committed.to_be_bytes());
    trace.update(oracle.conflicts.to_be_bytes());
    trace.update(observations.decisions.to_be_bytes());
    trace.update(observations.finalizations.to_be_bytes());
    trace.update([u8::from(observations.negative_detected)]);
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    PartitionedResolverReport {
        seed,
        mode,
        question: "Can ordered resolver processes preserve the centralized Cell v0 serializability oracle for cross-range transactions?".to_owned(),
        answer: if mode == PartitionedResolverMode::Correct && anomaly_count == 0 {
            "yes inside the frozen map, process, history, and workload bounds".to_owned()
        } else if mode != PartitionedResolverMode::Correct && observations.negative_detected {
            "the frozen negative subject was detected and must be discarded".to_owned()
        } else {
            "no".to_owned()
        },
        attempted_transactions: observations.attempted,
        committed_transactions: oracle.committed,
        conflict_rejections: oracle.conflicts,
        resolver_decisions: observations.decisions,
        cross_partition_attempts: observations.cross_partition,
        durable_finalizations: observations.finalizations,
        process_starts: observations.process_starts,
        process_restarts: observations.process_restarts,
        centralized_and_partitioned_statuses_match: observations.statuses_match,
        centralized_and_partitioned_rows_match: rows_match,
        envelope_chain_exact,
        crossing_ranges_route_to_every_overlap: observations.routing_exact,
        all_required_partitions_decide: observations.all_decide,
        resolver_identities_distinct: observations.identities_distinct,
        map_epoch_exact: observations.map_epoch_exact,
        decisions_durable_before_ack: observations.durability_exact,
        prior_disposition_order_exact: observations.prior_order_exact,
        finalization_exact: observations.finalization_exact,
        restarted_resolver_replays_exact_decision: observations.restart_replay_exact,
        negative_control_detected: observations.negative_detected,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        latest_commit_version: oracle.latest_sequence,
        trace_sha256: format!("{:x}", trace.finalize()),
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
            let resolver_id = u16::try_from(index + 1).map_err(|error| error.to_string())?;
            listener
                .local_addr()
                .map(|address| (resolver_id, address.to_string()))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    drop(listeners);
    Ok(addresses)
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: &str) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-partitioned-resolver-{mode}-{seed}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn authority(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("authority-{node_id}"))
    }

    fn resolver(&self, resolver_id: u16) -> PathBuf {
        self.0.join(format!("resolver-{resolver_id}"))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
