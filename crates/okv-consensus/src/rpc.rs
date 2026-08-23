use crate::{
    ApplyError, ApplyResponse, ClientCommand, ConsensusProcessRole, GenerationAction,
    GenerationApplyResponse, GenerationAuthorityState, GenerationCommand, GenerationCredential,
    GenerationFenceConfig, GenerationFenceFaults, NodeId, Raft, RequestIdentity, StateMachineStore,
    TypeConfig,
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

pub(crate) type WireResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerFaults {
    pub acknowledge_before_quorum: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ServerPolicy {
    pub faults: ServerFaults,
    pub role: ConsensusProcessRole,
    pub generation_fence: Option<GenerationFenceConfig>,
    pub generation_fence_faults: GenerationFenceFaults,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ControlWrite {
    pub app_data: Vec<u8>,
    pub drop_reply_after_commit: bool,
    pub credential: Option<GenerationCredential>,
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
pub(crate) struct NodeStatus {
    pub node_id: NodeId,
    pub term: u64,
    pub state: String,
    pub leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub payloads: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WriteAck {
    pub committed: bool,
    pub log_index: Option<u64>,
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
    if policy.role == ConsensusProcessRole::Data {
        raft.add_learner(request.node_id, BasicNode::new(request.address), true)
            .await
            .map(write_ack)
            .map_err(|error| error.to_string())
    } else {
        Err("authority node cannot add data learner".to_owned())
    }
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

async fn linearizable_status(
    raft: &Raft,
    state_machine: &StateMachineStore,
) -> Result<NodeStatus, String> {
    raft.ensure_linearizable()
        .await
        .map_err(|error| error.to_string())?;
    Ok(node_status(raft, state_machine).await)
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
            wal_root,
            control_root_version,
        } => {
            authority.cell_id == *cell_id
                && authority.authorizes(*generation, transaction_system_id)
                && authority.wal_root.as_deref() == Some(wal_root)
                && authority.control_root_version == *control_root_version
        }
        GenerationAction::Prepare {
            next_generation,
            recovery_id,
            next_transaction_system_id,
            ..
        } => {
            authority.authorizes_fencing(*next_generation, *recovery_id, next_transaction_system_id)
        }
        GenerationAction::Reserve {
            generation,
            recovery_id,
            transaction_system_id,
            fenced_log_index,
            ..
        } => {
            authority.authorizes_recovery(*generation, *recovery_id, transaction_system_id)
                && authority.fenced_log_index == *fenced_log_index
        }
        GenerationAction::Activate {
            generation,
            transaction_system_id,
            recovered_log_index,
            ..
        } => {
            authority.authorizes(*generation, transaction_system_id)
                && authority.recovered_log_index == *recovered_log_index
        }
    }
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
        response: Some(response.data),
    }
}

fn reject_application_error(ack: WriteAck) -> Result<WriteAck, String> {
    match ack.response.as_ref().and_then(|response| response.error) {
        Some(ApplyError::GenerationFenced) => Err("data generation is fenced".to_owned()),
        Some(ApplyError::ConflictingRequestIdentity) => {
            Err("request identity has conflicting application bytes".to_owned())
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
    let command = ClientCommand::decode(app_data)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "generation-fenced commit requires a versioned client command".to_owned())?;
    if command.credential.as_ref() != Some(credential) {
        return Err("replicated command does not bind the presented generation".to_owned());
    }
    if policy.generation_fence_faults.bypass_commit_fence {
        return Ok(());
    }
    let authority = read_linearizable_authority(&fence.authority_nodes).await?;
    if authority.authorizes(credential.generation, &credential.transaction_system_id) {
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
        payloads: state_machine.applied_payloads().await,
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
