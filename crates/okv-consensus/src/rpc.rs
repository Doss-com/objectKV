use crate::{
    cell_log_set_policy_authority_seed, recovery_public_key,
    sign_cell_log_set_policy_activation_statement, sign_publication_pop_capability,
    sign_recovery_statement, sign_routine_reconfiguration_statement, ApplyError, ApplyResponse,
    CellCommittedEnvelopeFeed, CellCommittedEnvelopeRequest, CellLogSetPolicyActivationAttestation,
    CellLogSetPolicyActivationStatement, CellStagedTransactionCommand, CellStateSnapshot,
    CellTransactionCommand, ClientCommand, ConsensusProcessRole, GenerationAction,
    GenerationApplyResponse, GenerationAuthorityState, GenerationCommand, GenerationCredential,
    GenerationFenceConfig, GenerationFenceFaults, GenerationPhase, NodeId,
    PublicationAuthorityState, PublicationCommand, PublicationPopCapabilityAttestation,
    PublicationPopCapabilityStatement, Raft, RecoveryAttestation, RecoveryCertificateKind,
    RecoveryCertificateStatement, RecoverySignerConfig, RequestIdentity,
    RoutineReconfigurationCertificateKind, RoutineReconfigurationCertificateStatement,
    StateMachineStore, TypeConfig,
};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, ServerState};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) const PORT: u16 = 24_091;
pub(crate) const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const APPEND: u8 = 1;
pub(crate) const VOTE: u8 = 2;
pub(crate) const INSTALL_SNAPSHOT: u8 = 3;
pub(crate) const INITIALIZE: u8 = 10;
pub(crate) const ELECT: u8 = 11;
pub(crate) const HEARTBEAT: u8 = 12;
pub(crate) const CLIENT_WRITE: u8 = 13;
pub(crate) const STATUS: u8 = 14;
pub(crate) const OUTCOME: u8 = 15;
pub(crate) const ADD_LEARNER: u8 = 16;
pub(crate) const CHANGE_MEMBERSHIP: u8 = 17;
pub(crate) const LINEARIZABLE_STATUS: u8 = 18;
pub(crate) const GENERATION_WRITE: u8 = 19;
pub(crate) const GENERATION_READ: u8 = 20;
pub(crate) const DATA_GENERATION_WRITE: u8 = 21;
pub(crate) const PREAUTHORIZED_CLIENT_WRITE: u8 = 22;
pub(crate) const RECOVERY_ATTEST: u8 = 23;
pub(crate) const PUBLICATION_WRITE: u8 = 24;
pub(crate) const PUBLICATION_READ: u8 = 25;
pub(crate) const PUBLICATION_OUTCOME: u8 = 26;
pub(crate) const TRIGGER_SNAPSHOT: u8 = 27;
pub(crate) const ROUTINE_ADD_LEARNER: u8 = 28;
pub(crate) const ROUTINE_CHANGE_MEMBERSHIP: u8 = 29;
pub(crate) const ROUTINE_ATTEST: u8 = 30;
pub(crate) const CELL_COMMITTED_ENVELOPE_READ: u8 = 31;
pub(crate) const PUBLICATION_POP_ATTEST: u8 = 32;
pub(crate) const CELL_LOG_SET_POLICY_ACTIVATION_ATTEST: u8 = 33;

pub(crate) type WireResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerFaults {
    pub acknowledge_before_quorum: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ServerPolicy {
    pub node_id: NodeId,
    pub faults: ServerFaults,
    pub role: ConsensusProcessRole,
    pub generation_fence: Option<GenerationFenceConfig>,
    pub generation_fence_faults: GenerationFenceFaults,
    pub publication_fence_faults: crate::PublicationFenceFaults,
    pub recovery_signer: Option<RecoverySignerConfig>,
    pub publication_signer: Option<RecoverySignerConfig>,
    pub storage_incarnation: Option<[u8; 16]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ControlWrite {
    pub app_data: Vec<u8>,
    pub drop_reply_after_commit: bool,
    pub credential: Option<GenerationCredential>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PublicationWriteRequest {
    pub command: PublicationCommand,
    pub drop_reply_after_commit: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddLearnerRequest {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChangeMembershipRequest {
    pub voters: BTreeSet<NodeId>,
    pub credential: GenerationCredential,
    pub recovery_id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RoutineAddLearnerRequest {
    pub generation: u64,
    pub membership_epoch: u64,
    pub reconfiguration_id: u64,
    pub node_id: NodeId,
    pub address: String,
    pub storage_incarnation: [u8; 16],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RoutineChangeMembershipRequest {
    pub voters: BTreeSet<NodeId>,
    pub credential: GenerationCredential,
    pub membership_epoch: u64,
    pub reconfiguration_id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NodeStatus {
    pub node_id: NodeId,
    pub term: u64,
    pub state: String,
    pub leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub last_applied_position: Option<crate::RecoveryLogPosition>,
    pub snapshot_log_index: Option<u64>,
    pub snapshot_log_position: Option<crate::RecoveryLogPosition>,
    pub membership_position: Option<crate::RecoveryLogPosition>,
    pub membership_voters: BTreeSet<NodeId>,
    pub membership_nodes: BTreeSet<NodeId>,
    pub payloads: Vec<Vec<u8>>,
    pub cells: Vec<CellStateSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WriteAck {
    pub committed: bool,
    pub log_index: Option<u64>,
    pub log_position: Option<crate::RecoveryLogPosition>,
    pub response: Option<ApplyResponse>,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_connection<S>(
    mut stream: S,
    raft: Raft,
    state_machine: Arc<StateMachineStore>,
    nodes: Arc<BTreeMap<NodeId, BasicNode>>,
    policy: ServerPolicy,
) -> WireResult
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let kind = stream.read_u8().await?;
    let body = read_frame(&mut stream).await?;
    match kind {
        APPEND => {
            let request = serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(&body)?;
            write_response(&mut stream, &raft.append_entries(request).await).await?;
        }
        VOTE => {
            let request = serde_json::from_slice::<VoteRequest<NodeId>>(&body)?;
            write_response(&mut stream, &raft.vote(request).await).await?;
        }
        INSTALL_SNAPSHOT => {
            let request = serde_json::from_slice::<InstallSnapshotRequest<TypeConfig>>(&body)?;
            write_response(&mut stream, &raft.install_snapshot(request).await).await?;
        }
        INITIALIZE => {
            let _: () = serde_json::from_slice(&body)?;
            write_string_result(&mut stream, raft.initialize((*nodes).clone()).await).await?;
        }
        ELECT => {
            let _: () = serde_json::from_slice(&body)?;
            write_string_result(&mut stream, raft.trigger().elect().await).await?;
        }
        HEARTBEAT => {
            let _: () = serde_json::from_slice(&body)?;
            write_string_result(&mut stream, raft.trigger().heartbeat().await).await?;
        }
        CLIENT_WRITE => {
            let request = serde_json::from_slice::<ControlWrite>(&body)?;
            if let Some(result) = client_write(&raft, &policy, request).await {
                write_response(&mut stream, &result).await?;
            }
        }
        PREAUTHORIZED_CLIENT_WRITE => {
            let request = serde_json::from_slice::<ControlWrite>(&body)?;
            let result = if policy
                .generation_fence_faults
                .allow_preauthorized_test_write
            {
                client_write_after_authorization(&raft, &policy, request).await
            } else {
                Some(Err(
                    "preauthorized write endpoint is disabled outside the takeover contract"
                        .to_owned(),
                ))
            };
            if let Some(result) = result {
                write_response(&mut stream, &result).await?;
            }
        }
        STATUS => {
            let _: () = serde_json::from_slice(&body)?;
            write_response(
                &mut stream,
                &Ok::<_, String>(node_status(&raft, &state_machine).await),
            )
            .await?;
        }
        OUTCOME => {
            let identity = serde_json::from_slice::<RequestIdentity>(&body)?;
            write_response(
                &mut stream,
                &Ok::<_, String>(state_machine.durable_outcome(identity).await),
            )
            .await?;
        }
        ADD_LEARNER => {
            let request = serde_json::from_slice::<AddLearnerRequest>(&body)?;
            write_response(&mut stream, &add_learner(&raft, &policy, request).await).await?;
        }
        CHANGE_MEMBERSHIP => {
            let request = serde_json::from_slice::<ChangeMembershipRequest>(&body)?;
            write_response(
                &mut stream,
                &change_membership(&raft, &policy, request).await,
            )
            .await?;
        }
        LINEARIZABLE_STATUS => {
            let _: () = serde_json::from_slice(&body)?;
            write_response(
                &mut stream,
                &linearizable_status(&raft, &state_machine).await,
            )
            .await?;
        }
        CELL_COMMITTED_ENVELOPE_READ => {
            let request = serde_json::from_slice::<CellCommittedEnvelopeRequest>(&body)?;
            write_response(
                &mut stream,
                &linearizable_committed_envelope_feed(&raft, &state_machine, &request).await,
            )
            .await?;
        }
        GENERATION_WRITE => {
            let command = serde_json::from_slice::<GenerationCommand>(&body)?;
            write_response(
                &mut stream,
                &generation_write(&raft, &policy, command).await,
            )
            .await?;
        }
        GENERATION_READ => {
            let _: () = serde_json::from_slice(&body)?;
            write_response(
                &mut stream,
                &generation_read(&raft, &state_machine, &policy).await,
            )
            .await?;
        }
        DATA_GENERATION_WRITE => {
            let command = serde_json::from_slice::<GenerationCommand>(&body)?;
            write_response(
                &mut stream,
                &data_generation_write(&raft, &policy, command).await,
            )
            .await?;
        }
        RECOVERY_ATTEST => {
            let statement = serde_json::from_slice::<RecoveryCertificateStatement>(&body)?;
            write_response(
                &mut stream,
                &recovery_attest(&state_machine, &policy, &statement).await,
            )
            .await?;
        }
        PUBLICATION_WRITE => {
            let request = serde_json::from_slice::<PublicationWriteRequest>(&body)?;
            if let Some(result) = publication_write(&raft, &policy, request).await {
                write_response(&mut stream, &result).await?;
            }
        }
        PUBLICATION_READ => {
            let _: () = serde_json::from_slice(&body)?;
            write_response(
                &mut stream,
                &publication_read(&raft, &state_machine, &policy).await,
            )
            .await?;
        }
        PUBLICATION_POP_ATTEST => {
            let statement = serde_json::from_slice::<PublicationPopCapabilityStatement>(&body)?;
            write_response(
                &mut stream,
                &publication_pop_attest(&state_machine, &policy, &statement).await,
            )
            .await?;
        }
        PUBLICATION_OUTCOME => {
            let identity = serde_json::from_slice::<RequestIdentity>(&body)?;
            write_response(
                &mut stream,
                &publication_outcome(&raft, &state_machine, &policy, identity).await,
            )
            .await?;
        }
        TRIGGER_SNAPSHOT => {
            let _: () = serde_json::from_slice(&body)?;
            write_string_result(&mut stream, raft.trigger().snapshot().await).await?;
        }
        ROUTINE_ADD_LEARNER => {
            let request = serde_json::from_slice::<RoutineAddLearnerRequest>(&body)?;
            write_response(
                &mut stream,
                &routine_add_learner(&raft, &policy, request).await,
            )
            .await?;
        }
        ROUTINE_CHANGE_MEMBERSHIP => {
            let request = serde_json::from_slice::<RoutineChangeMembershipRequest>(&body)?;
            write_response(
                &mut stream,
                &routine_change_membership(&raft, &policy, request).await,
            )
            .await?;
        }
        ROUTINE_ATTEST => {
            let statement =
                serde_json::from_slice::<RoutineReconfigurationCertificateStatement>(&body)?;
            write_response(
                &mut stream,
                &routine_reconfiguration_attest(&state_machine, &policy, &statement).await,
            )
            .await?;
        }
        CELL_LOG_SET_POLICY_ACTIVATION_ATTEST => {
            let statement = serde_json::from_slice::<CellLogSetPolicyActivationStatement>(&body)?;
            write_response(
                &mut stream,
                &cell_log_set_policy_activation_attest(&state_machine, &policy, &statement).await,
            )
            .await?;
        }
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown wire kind").into()),
    }
    Ok(())
}

async fn client_write(
    raft: &Raft,
    policy: &ServerPolicy,
    request: ControlWrite,
) -> Option<Result<WriteAck, String>> {
    if let Err(error) =
        authorize_commit(policy, request.credential.as_ref(), &request.app_data).await
    {
        return Some(Err(error));
    }
    client_write_after_authorization(raft, policy, request).await
}

async fn client_write_after_authorization(
    raft: &Raft,
    policy: &ServerPolicy,
    request: ControlWrite,
) -> Option<Result<WriteAck, String>> {
    if policy.faults.acknowledge_before_quorum {
        let raft = raft.clone();
        tokio::spawn(async move {
            let _: Result<_, openraft::error::RaftError<NodeId, _>> =
                raft.client_write(request.app_data).await;
        });
        return Some(Ok(WriteAck {
            committed: true,
            log_index: None,
            log_position: None,
            response: None,
        }));
    }
    let result = raft
        .client_write(request.app_data)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
        .and_then(reject_application_error);
    if request.drop_reply_after_commit && result.is_ok() {
        None
    } else {
        Some(result)
    }
}

async fn add_learner(
    raft: &Raft,
    policy: &ServerPolicy,
    request: AddLearnerRequest,
) -> Result<WriteAck, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("authority node cannot add data learner".to_owned());
    }
    if let Some(fence) = &policy.generation_fence {
        let authority = read_linearizable_authority(&fence.authority_nodes).await?;
        let full_recovery_authorized = authority.phase == GenerationPhase::Fencing
            && authority.transaction_system_id.as_deref()
                == Some(fence.credential.transaction_system_id.as_str())
            && authority
                .transaction_system_members
                .contains_key(&policy.node_id)
            && authority
                .pending_transaction_system_members
                .contains_key(&request.node_id);
        if !full_recovery_authorized {
            return Err(
                "generation-fenced learner admission requires an authorized recovery handoff"
                    .to_owned(),
            );
        }
    }
    raft.add_learner(request.node_id, BasicNode::new(request.address), true)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
}

async fn change_membership(
    raft: &Raft,
    policy: &ServerPolicy,
    request: ChangeMembershipRequest,
) -> Result<WriteAck, String> {
    authorize_recovery(policy, &request.credential, request.recovery_id).await?;
    raft.change_membership(request.voters, false)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
}

async fn routine_add_learner(
    raft: &Raft,
    policy: &ServerPolicy,
    request: RoutineAddLearnerRequest,
) -> Result<WriteAck, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot add a routine data learner".to_owned());
    }
    let fence = policy
        .generation_fence
        .as_ref()
        .ok_or_else(|| "routine learner admission requires generation authority".to_owned())?;
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if !authority.authorizes_node(
        fence.credential.generation,
        &fence.credential.transaction_system_id,
        policy.node_id,
    ) {
        return Err("this data node is not an active voter".to_owned());
    }
    if !authority.authorizes_routine_learner(
        request.generation,
        request.membership_epoch,
        request.reconfiguration_id,
        request.node_id,
        request.storage_incarnation,
    ) {
        return Err("coordinator did not authorize this learner admission".to_owned());
    }
    raft.add_learner(request.node_id, BasicNode::new(request.address), true)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
}

async fn routine_change_membership(
    raft: &Raft,
    policy: &ServerPolicy,
    request: RoutineChangeMembershipRequest,
) -> Result<WriteAck, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot change routine data membership".to_owned());
    }
    let fence = policy
        .generation_fence
        .as_ref()
        .ok_or_else(|| "routine membership change requires generation authority".to_owned())?;
    if request.credential != fence.credential {
        return Err(
            "routine membership credential does not match this transaction system".to_owned(),
        );
    }
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if !authority.authorizes_node(
        request.credential.generation,
        &request.credential.transaction_system_id,
        policy.node_id,
    ) {
        return Err("this data node is not an active voter".to_owned());
    }
    if !authority.authorizes_routine_membership(
        request.credential.generation,
        request.membership_epoch,
        request.reconfiguration_id,
        &request.voters,
    ) {
        return Err("coordinator did not authorize this routine membership transition".to_owned());
    }
    raft.change_membership(request.voters, false)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
}

async fn linearizable_status(
    raft: &Raft,
    state_machine: &StateMachineStore,
) -> Result<NodeStatus, String> {
    raft.ensure_linearizable()
        .await
        .map_err(|error| error.to_string())?;
    Ok(node_status(raft, state_machine).await)
}

async fn linearizable_committed_envelope_feed(
    raft: &Raft,
    state_machine: &StateMachineStore,
    request: &CellCommittedEnvelopeRequest,
) -> Result<CellCommittedEnvelopeFeed, String> {
    raft.ensure_linearizable()
        .await
        .map_err(|error| error.to_string())?;
    state_machine.committed_envelope_feed(request).await
}

async fn generation_write(
    raft: &Raft,
    policy: &ServerPolicy,
    command: GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Err("data node cannot mutate generation authority".to_owned());
    }
    let encoded = command.encode().map_err(|error| error.to_string())?;
    raft.client_write(encoded)
        .await
        .map_err(|error| error.to_string())?
        .data
        .generation
        .ok_or_else(|| "generation response missing".to_owned())
}

async fn publication_write(
    raft: &Raft,
    policy: &ServerPolicy,
    request: PublicationWriteRequest,
) -> Option<Result<WriteAck, String>> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Some(Err(
            "data node cannot mutate publication authority".to_owned()
        ));
    }
    let encoded = match request.command.encode() {
        Ok(encoded) => encoded,
        Err(error) => return Some(Err(error.to_string())),
    };
    if policy.faults.acknowledge_before_quorum {
        let raft = raft.clone();
        tokio::spawn(async move {
            let _: Result<_, openraft::error::RaftError<NodeId, _>> =
                raft.client_write(encoded).await;
        });
        return Some(Ok(WriteAck {
            committed: true,
            log_index: None,
            log_position: None,
            response: None,
        }));
    }
    let result = raft
        .client_write(encoded)
        .await
        .map(write_ack)
        .map_err(|error| error.to_string())
        .and_then(reject_application_error);
    if request.drop_reply_after_commit && result.is_ok() {
        None
    } else {
        Some(result)
    }
}

async fn publication_read(
    raft: &Raft,
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
) -> Result<PublicationAuthorityState, String> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Err("data node cannot read publication authority".to_owned());
    }
    raft.ensure_linearizable()
        .await
        .map_err(|error| error.to_string())?;
    Ok(state_machine.publication_authority().await)
}

async fn publication_pop_attest(
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
    statement: &PublicationPopCapabilityStatement,
) -> Result<PublicationPopCapabilityAttestation, String> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Err("data node cannot attest publication roots".to_owned());
    }
    let signer = policy
        .publication_signer
        .as_ref()
        .ok_or_else(|| "publication authority has no pop signing key".to_owned())?;
    let generation = state_machine.generation_authority().await;
    if generation.cell_id != statement.authority_cell_id
        || !generation.authorizes(statement.generation, &statement.transaction_system_id)
    {
        return Err("pop capability does not match active publication generation".to_owned());
    }
    let publication = state_machine.publication_authority().await;
    if publication.roots.get(&statement.destination_root) != Some(&statement.manifest) {
        return Err("pop capability does not match the replicated publication root".to_owned());
    }
    sign_publication_pop_capability(policy.node_id, &signer.private_key_seed, statement)
}

async fn publication_outcome(
    raft: &Raft,
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
    identity: RequestIdentity,
) -> Result<Option<ApplyResponse>, String> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Err("data node cannot read publication outcomes".to_owned());
    }
    if !policy.publication_fence_faults.local_stale_outcome_read {
        raft.ensure_linearizable()
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(state_machine.durable_outcome(identity).await)
}

async fn data_generation_write(
    raft: &Raft,
    policy: &ServerPolicy,
    command: GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("authority node cannot mutate the data generation mirror".to_owned());
    }
    let fence = policy
        .generation_fence
        .as_ref()
        .ok_or_else(|| "data generation transition requires external authority".to_owned())?;
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if !authority_matches_data_transition(&authority, &command.action) {
        return Err(
            "external authority does not authorize the data generation transition".to_owned(),
        );
    }
    let encoded = command.encode().map_err(|error| error.to_string())?;
    raft.client_write(encoded)
        .await
        .map_err(|error| error.to_string())?
        .data
        .generation
        .ok_or_else(|| "data generation response missing".to_owned())
}

fn authority_matches_data_transition(
    authority: &GenerationAuthorityState,
    action: &GenerationAction,
) -> bool {
    match action {
        GenerationAction::Bootstrap {
            cell_id,
            generation,
            transaction_system_id,
            transaction_system_members,
            transaction_system_incarnations,
            wal_root,
            control_root_version,
        } => {
            authority.cell_id == *cell_id
                && authority.authorizes(*generation, transaction_system_id)
                && authority.transaction_system_members == *transaction_system_members
                && authority.transaction_system_incarnations == *transaction_system_incarnations
                && authority.wal_root.as_deref() == Some(wal_root)
                && authority.control_root_version == *control_root_version
        }
        GenerationAction::Prepare {
            next_generation,
            recovery_id,
            next_transaction_system_id,
            next_transaction_system_members,
            next_transaction_system_incarnations,
            ..
        } => {
            authority.authorizes_fencing(*next_generation, *recovery_id, next_transaction_system_id)
                && authority.pending_transaction_system_members == *next_transaction_system_members
                && authority.pending_transaction_system_incarnations
                    == *next_transaction_system_incarnations
        }
        GenerationAction::Reserve {
            generation,
            recovery_id,
            transaction_system_id,
            certificate,
            ..
        } => {
            authority.authorizes_recovery(*generation, *recovery_id, transaction_system_id)
                && certificate.as_ref().is_some_and(|certificate| {
                    authority.fenced_log_position == Some(certificate.statement.log_position)
                })
        }
        GenerationAction::Activate {
            generation,
            transaction_system_id,
            certificate,
            ..
        } => {
            authority.authorizes(*generation, transaction_system_id)
                && certificate.as_ref().map_or_else(
                    || {
                        authority.recovered_log_position.is_none()
                            && authority.recovered_log_index == 0
                    },
                    |certificate| {
                        authority.recovered_log_position == Some(certificate.statement.log_position)
                    },
                )
        }
        GenerationAction::PrepareRoutineReconfiguration { .. }
        | GenerationAction::MarkRoutineLearnerReady { .. }
        | GenerationAction::FinalizeRoutineReconfiguration { .. } => false,
    }
}

async fn recovery_attest(
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
    statement: &RecoveryCertificateStatement,
) -> Result<RecoveryAttestation, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot attest a data-log position".to_owned());
    }
    let signer = policy
        .recovery_signer
        .as_ref()
        .ok_or_else(|| "data node has no recovery signing key".to_owned())?;
    let state = state_machine.generation_authority().await;
    let (expected_phase, expected_position, members) = match statement.kind {
        RecoveryCertificateKind::Fence => (
            GenerationPhase::Fencing,
            state_machine.generation_transition_position().await,
            &state.transaction_system_members,
        ),
        RecoveryCertificateKind::Recovered => (
            GenerationPhase::Recovering,
            state_machine.membership_position().await,
            &state.pending_transaction_system_members,
        ),
    };
    if state.phase != expected_phase {
        return Err(format!(
            "data node cannot attest {:?} while generation phase is {:?}",
            statement.kind, state.phase
        ));
    }
    let position = expected_position
        .ok_or_else(|| "data node has not applied the required log transition".to_owned())?;
    let expected = RecoveryCertificateStatement::new(statement.kind, &state, position, members);
    if statement != &expected {
        return Err("certificate statement does not match the exact local observation".to_owned());
    }
    let public_key = recovery_public_key(&signer.private_key_seed)?;
    if members.get(&policy.node_id) != Some(&public_key) {
        return Err("node signing key is not pinned in the certified voter set".to_owned());
    }
    sign_recovery_statement(policy.node_id, &signer.private_key_seed, statement)
}

async fn routine_reconfiguration_attest(
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
    statement: &RoutineReconfigurationCertificateStatement,
) -> Result<RecoveryAttestation, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot attest routine data membership".to_owned());
    }
    let signer = policy
        .recovery_signer
        .as_ref()
        .ok_or_else(|| "data node has no reconfiguration signing key".to_owned())?;
    let fence = policy
        .generation_fence
        .as_ref()
        .ok_or_else(|| "routine attestation requires generation authority".to_owned())?;
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    let expected = RoutineReconfigurationCertificateStatement::new(
        statement.kind,
        &authority,
        statement.snapshot_position,
        statement.applied_position,
    )
    .ok_or_else(|| "authority has no pending routine reconfiguration".to_owned())?;
    if statement != &expected {
        return Err("routine certificate statement does not match authority state".to_owned());
    }
    let pending = authority
        .pending_reconfiguration
        .as_ref()
        .expect("expected statement requires pending state");
    let local_generation = state_machine.generation_authority().await;
    if !local_generation.authorizes(statement.generation, &statement.transaction_system_id) {
        return Err("local data generation does not match the routine certificate".to_owned());
    }
    match statement.kind {
        RoutineReconfigurationCertificateKind::LearnerReady => {
            if policy.node_id == pending.replacement_node {
                if policy.storage_incarnation != Some(pending.replacement_incarnation)
                    || state_machine.snapshot_log_position().await
                        != Some(statement.snapshot_position)
                    || state_machine.last_applied_position().await
                        != Some(statement.applied_position)
                {
                    return Err(
                        "replacement did not durably reach the certified snapshot and suffix"
                            .to_owned(),
                    );
                }
            } else if !authority
                .transaction_system_members
                .contains_key(&policy.node_id)
                || !state_machine
                    .membership_nodes()
                    .await
                    .contains(&pending.replacement_node)
                || state_machine
                    .last_applied_position()
                    .await
                    .is_none_or(|position| position.index < statement.applied_position.index)
            {
                return Err(
                    "existing voter cannot observe the admitted learner position".to_owned(),
                );
            }
        }
        RoutineReconfigurationCertificateKind::MembershipCommitted => {
            let next_voters = pending
                .next_transaction_system_members
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
            if !next_voters.contains(&policy.node_id)
                || state_machine.membership_voters().await != next_voters
                || state_machine.membership_position().await != Some(statement.applied_position)
            {
                return Err("node has not applied the certified next voter set".to_owned());
            }
        }
    }
    let public_key = recovery_public_key(&signer.private_key_seed)?;
    let pinned_key = authority
        .transaction_system_members
        .get(&policy.node_id)
        .or_else(|| pending.next_transaction_system_members.get(&policy.node_id));
    if pinned_key != Some(&public_key) {
        return Err("node signing key is not pinned in the routine voter sets".to_owned());
    }
    sign_routine_reconfiguration_statement(policy.node_id, &signer.private_key_seed, statement)
}

async fn cell_log_set_policy_activation_attest(
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
    statement: &CellLogSetPolicyActivationStatement,
) -> Result<CellLogSetPolicyActivationAttestation, String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot attest a cell log-set policy".to_owned());
    }
    let signer = policy
        .recovery_signer
        .as_ref()
        .ok_or_else(|| "transaction authority has no policy signing material".to_owned())?;
    let snapshots = state_machine.cell_snapshots().await;
    let completed = snapshots
        .iter()
        .find(|snapshot| {
            snapshot.cell_id == statement.cell_id
                && snapshot.tenant_id == statement.tenant_id
                && snapshot.generation == statement.generation
        })
        .and_then(|snapshot| {
            snapshot
                .completed_log_set_policy_transitions
                .iter()
                .find(|completed| {
                    completed.transition.transition_id == statement.transition_id
                        && completed.transition.log_set_id == statement.log_set_id
                })
        })
        .ok_or_else(|| {
            "authority has not applied the named log-set policy transition".to_owned()
        })?;
    let expected = CellLogSetPolicyActivationStatement::new(completed);
    if statement != &expected {
        return Err("policy activation statement differs from applied authority state".to_owned());
    }
    let private_key_seed =
        cell_log_set_policy_authority_seed(&signer.private_key_seed, policy.node_id);
    sign_cell_log_set_policy_activation_statement(policy.node_id, &private_key_seed, statement)
}

async fn generation_read(
    raft: &Raft,
    state_machine: &StateMachineStore,
    policy: &ServerPolicy,
) -> Result<GenerationAuthorityState, String> {
    if policy.role != ConsensusProcessRole::GenerationAuthority {
        return Err("data node cannot read generation authority".to_owned());
    }
    raft.ensure_linearizable()
        .await
        .map_err(|error| error.to_string())?;
    Ok(state_machine.generation_authority().await)
}

fn write_ack(response: openraft::raft::ClientWriteResponse<TypeConfig>) -> WriteAck {
    WriteAck {
        committed: true,
        log_index: Some(response.log_id.index),
        log_position: Some(crate::RecoveryLogPosition::from_log_id(response.log_id)),
        response: Some(response.data),
    }
}

fn reject_application_error(ack: WriteAck) -> Result<WriteAck, String> {
    match ack.response.as_ref().and_then(|response| response.error) {
        Some(ApplyError::GenerationFenced) => Err("data generation is fenced".to_owned()),
        Some(ApplyError::ConflictingRequestIdentity) => {
            Err("request identity has conflicting application bytes".to_owned())
        }
        Some(ApplyError::UnknownCommandVersion) => {
            Err("unknown objectKV command version".to_owned())
        }
        None => Ok(ack),
    }
}

async fn authorize_commit(
    policy: &ServerPolicy,
    credential: Option<&GenerationCredential>,
    app_data: &[u8],
) -> Result<(), String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot accept data commits".to_owned());
    }
    let Some(fence) = &policy.generation_fence else {
        return Ok(());
    };
    let credential = credential.ok_or_else(|| "generation credential is required".to_owned())?;
    if credential != &fence.credential {
        return Err("generation credential does not match this transaction system".to_owned());
    }
    let bound_credential = bound_generation_credential(app_data)?
        .ok_or_else(|| "generation-fenced commit requires a supported command".to_owned())?;
    if &bound_credential != credential {
        return Err("replicated command does not bind the presented generation".to_owned());
    }
    if policy.generation_fence_faults.bypass_commit_fence {
        return Ok(());
    }
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if authority.authorizes_node(
        credential.generation,
        &credential.transaction_system_id,
        policy.node_id,
    ) {
        return Ok(());
    }
    if policy.generation_fence_faults.accept_recovering_commits
        && fence.recovery_id.is_some_and(|recovery_id| {
            authority.authorizes_recovery(
                credential.generation,
                recovery_id,
                &credential.transaction_system_id,
            )
        })
    {
        return Ok(());
    }
    Err(format!(
        "transaction system {} generation {} is fenced by {:?} generation {}",
        credential.transaction_system_id,
        credential.generation,
        authority.phase,
        authority.generation
    ))
}

fn bound_generation_credential(app_data: &[u8]) -> Result<Option<GenerationCredential>, String> {
    if let Some(command) = ClientCommand::decode(app_data).map_err(|error| error.to_string())? {
        return Ok(command.credential);
    }
    if let Some(command) =
        CellStagedTransactionCommand::decode(app_data).map_err(|error| error.to_string())?
    {
        return Ok(command.credential);
    }
    if let Some(command) =
        CellTransactionCommand::decode(app_data).map_err(|error| error.to_string())?
    {
        return Ok(command.credential);
    }
    Ok(None)
}

async fn authorize_recovery(
    policy: &ServerPolicy,
    credential: &GenerationCredential,
    recovery_id: u64,
) -> Result<(), String> {
    if policy.role != ConsensusProcessRole::Data {
        return Err("generation authority cannot change data membership".to_owned());
    }
    let fence = policy
        .generation_fence
        .as_ref()
        .ok_or_else(|| "membership change requires generation authority".to_owned())?;
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if authority.authorizes_recovery(
        credential.generation,
        recovery_id,
        &credential.transaction_system_id,
    ) {
        Ok(())
    } else {
        Err("coordinator did not authorize this recovery handoff".to_owned())
    }
}

async fn read_linearizable_authority(
    authority_nodes: &BTreeMap<NodeId, String>,
) -> Result<GenerationAuthorityState, String> {
    let mut errors = Vec::new();
    for address in authority_nodes.values() {
        match authority_read(address).await {
            Ok(state) => return Ok(state),
            Err(error) => errors.push(error),
        }
    }
    Err(format!(
        "no coordinator leader supplied a linearizable generation read: {}",
        errors.join("; ")
    ))
}

async fn authority_read(address: &str) -> Result<GenerationAuthorityState, String> {
    let mut stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address))
        .await
        .map_err(|_| format!("authority connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, GENERATION_READ, &())
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<GenerationAuthorityState, String> =
        tokio::time::timeout(Duration::from_secs(2), read_response(&mut stream))
            .await
            .map_err(|_| format!("authority read timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

async fn node_status(raft: &Raft, state_machine: &StateMachineStore) -> NodeStatus {
    let metrics = raft.metrics();
    let metrics = metrics.borrow().clone();
    let state = match metrics.state {
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Leader => "leader",
        ServerState::Shutdown => "shutdown",
    };
    NodeStatus {
        node_id: metrics.id,
        term: metrics.current_term,
        state: state.to_owned(),
        leader: metrics.current_leader,
        last_log_index: metrics.last_log_index,
        last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
        last_applied_position: state_machine.last_applied_position().await,
        snapshot_log_index: state_machine.snapshot_log_index().await,
        snapshot_log_position: state_machine.snapshot_log_position().await,
        membership_position: state_machine.membership_position().await,
        membership_voters: state_machine.membership_voters().await,
        membership_nodes: state_machine.membership_nodes().await,
        payloads: state_machine.applied_payloads().await,
        cells: state_machine.cell_snapshots().await,
    }
}

async fn write_string_result<S, T, E>(stream: &mut S, result: Result<T, E>) -> WireResult
where
    S: AsyncWrite + Unpin,
    T: Serialize,
    E: std::fmt::Display,
{
    write_response(stream, &result.map_err(|error| error.to_string())).await
}

pub(crate) async fn write_response<S, T>(stream: &mut S, response: &T) -> WireResult
where
    S: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(response)?;
    write_frame(stream, &bytes).await?;
    Ok(())
}

pub(crate) async fn write_request<S, T>(stream: &mut S, kind: u8, request: &T) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(request).map_err(invalid_data)?;
    stream.write_u8(kind).await?;
    write_frame(stream, &body).await
}

pub(crate) async fn read_response<S, T>(stream: &mut S) -> io::Result<T>
where
    S: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let response = read_frame(stream).await?;
    serde_json::from_slice(&response).map_err(invalid_data)
}

pub(crate) async fn write_frame<S>(stream: &mut S, body: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let length = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame exceeds u32"))?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds contract limit",
        ));
    }
    stream.write_u32(length).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

pub(crate) async fn read_frame<S>(stream: &mut S) -> io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let length = usize::try_from(stream.read_u32().await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid frame length"))?;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds contract limit",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellKeyRange, CellMutation, CellReadVersion};

    fn credential() -> GenerationCredential {
        GenerationCredential {
            generation: 7,
            transaction_system_id: "tx-g7".to_owned(),
        }
    }

    #[test]
    fn extracts_generation_binding_from_supported_command_families() {
        let expected = credential();
        let generic = ClientCommand {
            identity: RequestIdentity {
                client_id: 1,
                request_id: 2,
            },
            credential: Some(expected.clone()),
            payload: b"generic".to_vec(),
        }
        .encode()
        .expect("generic command encodes");
        let semantic = CellTransactionCommand {
            identity: RequestIdentity {
                client_id: 1,
                request_id: 3,
            },
            credential: Some(expected.clone()),
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 7,
            read_version: CellReadVersion {
                generation: 7,
                sequence: 1,
            },
            read_conflicts: vec![CellKeyRange::point(b"key")],
            write_conflicts: vec![CellKeyRange::point(b"key")],
            mutations: vec![CellMutation::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }],
            partitioned_resolution: None,
            accepted_resolvers: vec![1, 2],
            durable_log_tags: vec![10, 20],
        }
        .encode()
        .expect("semantic command encodes");

        assert_eq!(
            bound_generation_credential(&generic).expect("generic binding decodes"),
            Some(expected.clone())
        );
        assert_eq!(
            bound_generation_credential(&semantic).expect("semantic binding decodes"),
            Some(expected)
        );
        assert_eq!(
            bound_generation_credential(b"unversioned").expect("unknown bytes are classified"),
            None
        );
    }
}
