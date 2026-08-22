use crate::{NodeId, OpenRaftLogStore, Raft, StateMachineStore, TypeConfig};
use openraft::error::{RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, ServerState, SnapshotPolicy};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

const PORT: u16 = 24_091;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const APPEND: u8 = 1;
const VOTE: u8 = 2;
const INSTALL_SNAPSHOT: u8 = 3;
const INITIALIZE: u8 = 10;
const ELECT: u8 = 11;
const HEARTBEAT: u8 = 12;
const CLIENT_WRITE: u8 = 13;
const STATUS: u8 = 14;

#[derive(Clone, Debug)]
pub(crate) struct SimNetworkFactory;

#[derive(Clone, Debug)]
pub(crate) struct SimConnection {
    target: NodeId,
    host: String,
}

impl RaftNetworkFactory<TypeConfig> for SimNetworkFactory {
    type Network = SimConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        SimConnection {
            target,
            host: node.addr.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for SimConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.send_raft(APPEND, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.send_raft(INSTALL_SNAPSHOT, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.send_raft(VOTE, &rpc).await
    }

    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(10)))
    }
}

impl SimConnection {
    async fn send_raft<Req, Resp, ApiError>(
        &self,
        kind: u8,
        request: &Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, RaftError<NodeId, ApiError>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        ApiError: std::error::Error + DeserializeOwned,
    {
        let response: Result<Resp, RaftError<NodeId, ApiError>> =
            send(&self.host, kind, request).await.map_err(|error| {
                RPCError::<NodeId, BasicNode, RaftError<NodeId, ApiError>>::from(Unreachable::new(
                    &error,
                ))
            })?;
        response.map_err(|error| RemoteError::new(self.target, error).into())
    }
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

impl NodeStatus {
    fn from_parts(raft: &Raft) -> Self {
        let metrics = raft.metrics();
        let metrics = metrics.borrow().clone();
        let state = match metrics.state {
            ServerState::Learner => "learner",
            ServerState::Follower => "follower",
            ServerState::Candidate => "candidate",
            ServerState::Leader => "leader",
            ServerState::Shutdown => "shutdown",
        };
        Self {
            node_id: metrics.id,
            term: metrics.current_term,
            state: state.to_owned(),
            leader: metrics.current_leader,
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            payloads: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WriteAck {
    pub committed: bool,
    pub log_index: Option<u64>,
}

pub(crate) async fn run_node(
    node_id: NodeId,
    root: PathBuf,
    acknowledge_before_quorum: bool,
) -> turmoil::Result {
    let log_store = OpenRaftLogStore::open(root)?;
    let state_machine = Arc::new(StateMachineStore::default());
    let raft = Raft::new(
        node_id,
        cluster_config()?,
        SimNetworkFactory,
        log_store,
        state_machine.clone(),
    )
    .await?;
    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let state_machine = state_machine.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, raft, state_machine, acknowledge_before_quorum).await;
        });
    }
}

fn cluster_config() -> Result<Arc<Config>, openraft::ConfigError> {
    let config = Config {
        cluster_name: "okv-cluster-contract-v1".to_owned(),
        enable_tick: false,
        enable_heartbeat: false,
        enable_elect: false,
        snapshot_policy: SnapshotPolicy::Never,
        ..Config::default()
    }
    .validate()?;
    Ok(Arc::new(config))
}

async fn handle_connection(
    mut stream: TcpStream,
    raft: Raft,
    state_machine: Arc<StateMachineStore>,
    acknowledge_before_quorum: bool,
) -> Result<(), Box<dyn std::error::Error>> {
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
            let nodes = BTreeMap::from([
                (1, BasicNode::new("node-1")),
                (2, BasicNode::new("node-2")),
                (3, BasicNode::new("node-3")),
            ]);
            write_string_result(&mut stream, raft.initialize(nodes).await).await?;
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
            let payload = serde_json::from_slice::<Vec<u8>>(&body)?;
            if acknowledge_before_quorum {
                let raft = raft.clone();
                tokio::spawn(async move {
                    let _: Result<_, openraft::error::RaftError<NodeId, _>> =
                        raft.client_write(payload).await;
                });
                write_response(
                    &mut stream,
                    &Ok::<_, String>(WriteAck {
                        committed: true,
                        log_index: None,
                    }),
                )
                .await?;
            } else {
                let result = raft
                    .client_write(payload)
                    .await
                    .map(|response| WriteAck {
                        committed: true,
                        log_index: Some(response.log_id.index),
                    })
                    .map_err(|error| error.to_string());
                write_response(&mut stream, &result).await?;
            }
        }
        STATUS => {
            let _: () = serde_json::from_slice(&body)?;
            let mut status = NodeStatus::from_parts(&raft);
            status.payloads = state_machine.applied_payloads().await;
            write_response(&mut stream, &Ok::<_, String>(status)).await?;
        }
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown wire kind").into()),
    }
    Ok(())
}

async fn write_string_result<T, E>(
    stream: &mut TcpStream,
    result: Result<T, E>,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: Serialize,
    E: std::fmt::Display,
{
    write_response(stream, &result.map_err(|error| error.to_string())).await
}

async fn write_response<T: Serialize>(
    stream: &mut TcpStream,
    response: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(response)?;
    write_frame(stream, &bytes).await?;
    Ok(())
}

pub(crate) async fn initialize(host: &str) -> Result<(), String> {
    control(host, INITIALIZE, &()).await
}

pub(crate) async fn elect(host: &str) -> Result<(), String> {
    control(host, ELECT, &()).await
}

pub(crate) async fn heartbeat(host: &str) -> Result<(), String> {
    control(host, HEARTBEAT, &()).await
}

pub(crate) async fn write(host: &str, payload: &[u8]) -> Result<WriteAck, String> {
    control(host, CLIENT_WRITE, &payload.to_vec()).await
}

pub(crate) async fn status(host: &str) -> Result<NodeStatus, String> {
    control(host, STATUS, &()).await
}

async fn control<Req, Resp>(host: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    send::<_, Result<Resp, String>>(host, kind, request)
        .await
        .map_err(|error| error.to_string())?
}

async fn send<Req, Resp>(host: &str, kind: u8, request: &Req) -> io::Result<Resp>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = TcpStream::connect((host, PORT)).await?;
    let body = serde_json::to_vec(request).map_err(invalid_data)?;
    stream.write_u8(kind).await?;
    write_frame(&mut stream, &body).await?;
    let response = read_frame(&mut stream).await?;
    serde_json::from_slice(&response).map_err(invalid_data)
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
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

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
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
