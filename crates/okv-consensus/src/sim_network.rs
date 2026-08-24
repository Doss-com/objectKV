use crate::rpc::{
    handle_connection, read_response, write_request, ControlWrite, NodeStatus, ServerFaults,
    ServerPolicy, WriteAck, APPEND, CLIENT_WRITE, ELECT, HEARTBEAT, INITIALIZE, INSTALL_SNAPSHOT,
    PORT, STATUS, VOTE,
};
use crate::{NodeId, OpenRaftLogStore, Raft, StateMachineStore, TypeConfig};
use openraft::error::{RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, SnapshotPolicy};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use turmoil::net::{TcpListener, TcpStream};

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
    let nodes = Arc::new(BTreeMap::from([
        (1, BasicNode::new("node-1")),
        (2, BasicNode::new("node-2")),
        (3, BasicNode::new("node-3")),
    ]));
    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let state_machine = state_machine.clone();
        let nodes = nodes.clone();
        tokio::spawn(async move {
            let _ = Box::pin(handle_connection(
                stream,
                raft,
                state_machine,
                nodes,
                ServerPolicy {
                    faults: ServerFaults {
                        acknowledge_before_quorum,
                    },
                    ..ServerPolicy::default()
                },
            ))
            .await;
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
    control(
        host,
        CLIENT_WRITE,
        &ControlWrite {
            app_data: payload.to_vec(),
            drop_reply_after_commit: false,
            credential: None,
        },
    )
    .await
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
    write_request(&mut stream, kind, request).await?;
    read_response(&mut stream).await
}
