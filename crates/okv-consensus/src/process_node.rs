use crate::rpc::{
    handle_connection, read_response, write_request, ServerFaults, ServerPolicy, APPEND,
    INSTALL_SNAPSHOT, VOTE,
};
use crate::{
    ConsensusProcessRole, GenerationAuthorityFaults, GenerationFenceConfig, GenerationFenceFaults,
    NodeId, OpenRaftLogStore, PublicationAuthorityFaults, PublicationFenceFaults, Raft,
    RecoverySignerConfig, StateMachineStore, TypeConfig,
};
use openraft::error::{RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, SnapshotPolicy};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// Configuration for one real OS process in the bounded consensus harness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessNodeConfig {
    pub node_id: NodeId,
    pub root: PathBuf,
    pub nodes: BTreeMap<NodeId, String>,
    pub deduplicate_requests: bool,
    pub acknowledge_before_quorum: bool,
    pub policy: ProcessNodePolicy,
}

/// Role and generation policy for one real-process node.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessNodePolicy {
    pub role: ConsensusProcessRole,
    pub generation_fence: Option<GenerationFenceConfig>,
    pub generation_authority_faults: GenerationAuthorityFaults,
    pub generation_fence_faults: GenerationFenceFaults,
    pub publication_authority_faults: PublicationAuthorityFaults,
    pub publication_fence_faults: PublicationFenceFaults,
    pub recovery_signer: Option<RecoverySignerConfig>,
}

#[derive(Clone, Debug)]
struct ProcessNetworkFactory;

#[derive(Clone, Debug)]
struct ProcessConnection {
    target: NodeId,
    address: String,
}

impl RaftNetworkFactory<TypeConfig> for ProcessNetworkFactory {
    type Network = ProcessConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        ProcessConnection {
            target,
            address: node.addr.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for ProcessConnection {
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
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(20)))
    }
}

impl ProcessConnection {
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
            send(&self.address, kind, request).await.map_err(|error| {
                RPCError::<NodeId, BasicNode, RaftError<NodeId, ApiError>>::from(Unreachable::new(
                    &error,
                ))
            })?;
        response.map_err(|error| RemoteError::new(self.target, error).into())
    }
}

/// Run one normal-TCP `OpenRaft` node until its process is terminated.
///
/// # Errors
///
/// Returns an error when configuration, stable storage, Raft startup, or the
/// TCP listener fails.
pub async fn run_process_node(config: ProcessNodeConfig) -> Result<(), String> {
    let address = config
        .nodes
        .get(&config.node_id)
        .ok_or_else(|| format!("missing address for node {}", config.node_id))?
        .clone();
    let nodes = Arc::new(
        config
            .nodes
            .iter()
            .map(|(node_id, address)| (*node_id, BasicNode::new(address)))
            .collect::<BTreeMap<_, _>>(),
    );
    let log_store = OpenRaftLogStore::open(&config.root).map_err(|error| error.to_string())?;
    let state_machine = Arc::new(StateMachineStore::new_with_authority_faults(
        config.deduplicate_requests,
        config.policy.generation_authority_faults,
        config.policy.generation_fence_faults,
        config.policy.publication_authority_faults,
        config.policy.publication_fence_faults,
    ));
    let raft = Raft::new(
        config.node_id,
        cluster_config().map_err(|error| error.to_string())?,
        ProcessNetworkFactory,
        log_store,
        state_machine.clone(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let listener = TcpListener::bind(&address)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let raft = raft.clone();
        let state_machine = state_machine.clone();
        let nodes = nodes.clone();
        let policy = ServerPolicy {
            node_id: config.node_id,
            faults: ServerFaults {
                acknowledge_before_quorum: config.acknowledge_before_quorum,
            },
            role: config.policy.role,
            generation_fence: config.policy.generation_fence.clone(),
            generation_fence_faults: config.policy.generation_fence_faults,
            publication_fence_faults: config.policy.publication_fence_faults,
            recovery_signer: config.policy.recovery_signer.clone(),
        };
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, raft, state_machine, nodes, policy).await
            {
                eprintln!("consensus control connection failed: {error}");
            }
        });
    }
}

fn cluster_config() -> Result<Arc<Config>, openraft::ConfigError> {
    Ok(Arc::new(
        Config {
            cluster_name: "okv-process-contract-v1".to_owned(),
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            snapshot_policy: SnapshotPolicy::Never,
            ..Config::default()
        }
        .validate()?,
    ))
}

async fn send<Req, Resp>(address: &str, kind: u8, request: &Req) -> std::io::Result<Resp>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = TcpStream::connect(address).await?;
    write_request(&mut stream, kind, request).await?;
    read_response(&mut stream).await
}
