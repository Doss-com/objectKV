use crate::rpc::{
    read_response, write_request, NodeStatus, ELECT, GENERATION_WRITE, INITIALIZE, STATUS,
};
use crate::{
    recovery_public_key, ConsensusProcessRole, GenerationAction, GenerationApplyResponse,
    GenerationAuthorityFaults, GenerationCommand, GenerationCommandStatus, GenerationFenceFaults,
    NodeId, ProcessNodeConfig, ProcessNodePolicy, PublicationAuthorityFaults, PublicationClient,
    RequestIdentity,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const AUTHORITY_NODES: [NodeId; 3] = [101, 102, 103];
const RETRY_ATTEMPTS: usize = 500;

/// Real-process publication-authority fixture used by physical worker gates.
#[doc(hidden)]
pub struct PublicationAuthorityProcessFixture {
    root: PathBuf,
    addresses: BTreeMap<NodeId, String>,
    children: BTreeMap<NodeId, Child>,
    deduplicate_requests: bool,
    publication_authority_faults: PublicationAuthorityFaults,
}

impl PublicationAuthorityProcessFixture {
    /// Start and bootstrap three generation-authority voters.
    ///
    /// # Errors
    ///
    /// Returns an error when processes, stable storage, election, or generation
    /// bootstrap cannot complete.
    pub async fn start(executable: &Path, seed: u64) -> Result<Self, String> {
        Self::start_with_faults(
            executable,
            seed,
            true,
            PublicationAuthorityFaults::default(),
        )
        .await
    }

    /// Start with bounded unsafe authority behavior for a frozen negative
    /// control.
    ///
    /// # Errors
    ///
    /// Returns an error when processes, stable storage, election, or generation
    /// bootstrap cannot complete.
    #[doc(hidden)]
    pub async fn start_with_faults(
        executable: &Path,
        seed: u64,
        deduplicate_requests: bool,
        publication_authority_faults: PublicationAuthorityFaults,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "publication authority executable does not exist: {}",
                executable.display()
            ));
        }
        let addresses = allocate_addresses(&AUTHORITY_NODES)?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "okv-publisher-authority-{seed}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        let mut fixture = Self {
            root,
            addresses,
            children: BTreeMap::new(),
            deduplicate_requests,
            publication_authority_faults,
        };
        for node_id in AUTHORITY_NODES {
            fixture.start_node(executable, node_id)?;
        }
        for node_id in AUTHORITY_NODES {
            wait_ready(fixture.address(node_id)?).await?;
        }
        retry_control(fixture.address(101)?, INITIALIZE, &()).await?;
        if !elect_until_leader(fixture.address(101)?, 101).await {
            return Err("publisher authority leader election failed".to_owned());
        }
        let bootstrap = retry_generation_write(
            fixture.address(101)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: seed.max(1),
                    request_id: 1,
                },
                action: GenerationAction::Bootstrap {
                    cell_id: 17,
                    generation: 7,
                    transaction_system_id: "tx-g7".to_owned(),
                    transaction_system_members: recovery_members(&[201, 202, 203])?,
                    wal_root: "wal-g7".to_owned(),
                    control_root_version: 1,
                },
            },
        )
        .await?;
        if bootstrap.status != GenerationCommandStatus::Accepted
            || !bootstrap.state.authorizes(7, "tx-g7")
        {
            return Err("publisher authority generation bootstrap failed".to_owned());
        }
        Ok(fixture)
    }

    /// Client endpoints for the bootstrapped authority.
    ///
    /// # Errors
    ///
    /// Returns an error only if the internally validated endpoint set is empty.
    pub fn client(&self) -> Result<PublicationClient, String> {
        PublicationClient::new(self.addresses.values().cloned().collect())
    }

    /// Stable endpoint set passed to disposable worker processes.
    #[must_use]
    pub fn endpoints(&self) -> Vec<String> {
        self.addresses.values().cloned().collect()
    }

    /// Number of real authority processes owned by this fixture.
    #[must_use]
    pub fn process_count(&self) -> usize {
        self.children.len()
    }

    /// Kill the initial leader and elect node 102 from the surviving quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if the leader is absent, cannot be reaped, or no
    /// successor can be elected.
    #[doc(hidden)]
    pub async fn kill_initial_leader_and_elect_successor(&mut self) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&101)
            .ok_or_else(|| "initial publication leader process is absent".to_owned())?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        if !elect_until_leader(self.address(102)?, 102).await {
            return Err("publication authority successor election failed".to_owned());
        }
        Ok(())
    }

    fn start_node(&mut self, executable: &Path, node_id: NodeId) -> Result<(), String> {
        let config = ProcessNodeConfig {
            node_id,
            root: self.root.join(format!("node-{node_id}")),
            nodes: self.addresses.clone(),
            deduplicate_requests: self.deduplicate_requests,
            acknowledge_before_quorum: false,
            policy: ProcessNodePolicy {
                role: ConsensusProcessRole::GenerationAuthority,
                generation_authority_faults: GenerationAuthorityFaults::default(),
                generation_fence_faults: GenerationFenceFaults {
                    allow_preauthorized_test_write: true,
                    ..GenerationFenceFaults::default()
                },
                publication_authority_faults: self.publication_authority_faults,
                ..ProcessNodePolicy::default()
            },
        };
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start authority node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing authority address for node {node_id}"))
    }
}

impl Drop for PublicationAuthorityProcessFixture {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if self.root.starts_with(std::env::temp_dir())
            && self.root.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-publisher-authority-")
            })
        {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

fn recovery_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-PUBLISHER-PROCESS-RECOVERY-SIGNER-V1\0");
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

fn recovery_members(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
    node_ids
        .iter()
        .map(|node_id| {
            recovery_public_key(&recovery_seed(*node_id)).map(|public_key| (*node_id, public_key))
        })
        .collect()
}

async fn retry_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("generation write failed at {address}: {last}"))
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

async fn wait_ready(address: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control::<_, NodeStatus>(address, STATUS, &()).await {
            Ok(_) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "authority node did not become ready at {address}: {last}"
    ))
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
        tokio::time::timeout(Duration::from_secs(8), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

fn allocate_addresses(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in node_ids {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (node_id, listener) in node_ids.iter().zip(&listeners) {
        addresses.insert(
            *node_id,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    Ok(addresses)
}
