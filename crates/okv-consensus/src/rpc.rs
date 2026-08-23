use crate::{ApplyResponse, NodeId, Raft, RequestIdentity, StateMachineStore, TypeConfig};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, ServerState};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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

pub(crate) type WireResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServerFaults {
    pub acknowledge_before_quorum: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ControlWrite {
    pub app_data: Vec<u8>,
    pub drop_reply_after_commit: bool,
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

pub(crate) async fn handle_connection<S>(
    mut stream: S,
    raft: Raft,
    state_machine: Arc<StateMachineStore>,
    nodes: Arc<BTreeMap<NodeId, BasicNode>>,
    faults: ServerFaults,
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
            if faults.acknowledge_before_quorum {
                let raft = raft.clone();
                tokio::spawn(async move {
                    let _: Result<_, openraft::error::RaftError<NodeId, _>> =
                        raft.client_write(request.app_data).await;
                });
                write_response(
                    &mut stream,
                    &Ok::<_, String>(WriteAck {
                        committed: true,
                        log_index: None,
                        response: None,
                    }),
                )
                .await?;
            } else {
                let result = raft
                    .client_write(request.app_data)
                    .await
                    .map(|response| WriteAck {
                        committed: true,
                        log_index: Some(response.log_id.index),
                        response: Some(response.data),
                    })
                    .map_err(|error| error.to_string());
                if request.drop_reply_after_commit && result.is_ok() {
                    return Ok(());
                }
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
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown wire kind").into()),
    }
    Ok(())
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
