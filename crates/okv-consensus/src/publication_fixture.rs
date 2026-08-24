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
        Self::start_custom(
            executable,
            seed,
            true,
            PublicationAuthorityFaults::default(),
            17,
            7,
            "tx-g7",
        )
        .await
    }

    /// Start an authority bound to the transaction generation under test.
    ///
    /// # Errors
    ///
    /// Returns an error when processes, stable storage, election, or generation
    /// bootstrap cannot complete.
    #[doc(hidden)]
    pub async fn start_for_generation(
        executable: &Path,
        seed: u64,
        cell_id: u64,
        generation: u64,
        transaction_system_id: &str,
    ) -> Result<Self, String> {
        Self::start_custom(
            executable,
            seed,
            true,
            PublicationAuthorityFaults::default(),
            cell_id,
            generation,
            transaction_system_id,
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
        Self::start_custom(
            executable,
            seed,
            deduplicate_requests,
            publication_authority_faults,
            17,
            7,
            "tx-g7",
        )
        .await
    }

    async fn start_custom(
        executable: &Path,
        seed: u64,
        deduplicate_requests: bool,
        publication_authority_faults: PublicationAuthorityFaults,
        cell_id: u64,
        generation: u64,
        transaction_system_id: &str,
    ) -> Result<Self, String> {
        if generation == 0 || transaction_system_id.is_empty() {
            return Err("publication authority generation identity is invalid".to_owned());
        }
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
                    cell_id,
                    generation,
                    transaction_system_id: transaction_system_id.to_owned(),
                    transaction_system_members: recovery_members(&[201, 202, 203])?,
                    transaction_system_incarnations: fixture_incarnations(&[201, 202, 203]),
                    wal_root: format!("wal-g{generation}"),
                    control_root_version: 1,
                },
            },
        )
        .await?;
        if bootstrap.status != GenerationCommandStatus::Accepted
            || !bootstrap
                .state
                .authorizes(generation, transaction_system_id)
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

    /// Client endpoints ordered with one expected leader first.
    ///
    /// # Errors
    ///
    /// Returns an error when the node is outside the fixed authority set.
    #[doc(hidden)]
    pub fn client_starting_with(&self, node_id: NodeId) -> Result<PublicationClient, String> {
        let first = self.address(node_id)?.to_owned();
        let endpoints = std::iter::once(first)
            .chain(
                self.addresses
                    .iter()
                    .filter(|(candidate, _)| **candidate != node_id)
                    .map(|(_, address)| address.clone()),
            )
            .collect();
        PublicationClient::new(endpoints)
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

    /// Pinned public keys for the three publication-authority process signers.
    ///
    /// # Errors
    ///
    /// Returns an error only if the deterministic evaluation key cannot be derived.
    #[doc(hidden)]
    pub fn pop_capability_members() -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
        AUTHORITY_NODES
            .iter()
            .map(|node_id| {
                recovery_public_key(&publication_pop_seed(*node_id))
                    .map(|public_key| (*node_id, public_key))
            })
            .collect()
    }

    /// Kill the initial leader and elect node 102 from the surviving quorum.
    ///
    /// # Errors
    ///
    /// Returns an error if the leader is absent, cannot be reaped, or no
    /// successor can be elected.
    #[doc(hidden)]
    pub async fn kill_initial_leader_and_elect_successor(&mut self) -> Result<(), String> {
        self.kill_leader_and_elect_successor(101, 102).await
    }

    /// Kill one current leader and elect a named successor from the survivors.
    ///
    /// # Errors
    ///
    /// Returns an error if the leader is absent, cannot be reaped, or the
    /// successor cannot win a quorum.
    #[doc(hidden)]
    pub async fn kill_leader_and_elect_successor(
        &mut self,
        leader: NodeId,
        successor: NodeId,
    ) -> Result<(), String> {
        if leader == successor {
            return Err("publication leader and successor must differ".to_owned());
        }
        let mut child = self
            .children
            .remove(&leader)
            .ok_or_else(|| format!("publication leader {leader} process is absent"))?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        if !elect_until_leader(self.address(successor)?, successor).await {
            return Err(format!(
                "publication authority successor {successor} election failed"
            ));
        }
        Ok(())
    }

    /// Restart one process on its existing stable root and wait for it to
    /// catch up with the current leader.
    ///
    /// # Errors
    ///
    /// Returns an error when the process is already running, cannot restart,
    /// or cannot recover the leader's applied position.
    #[doc(hidden)]
    pub async fn restart_node(
        &mut self,
        executable: &Path,
        node_id: NodeId,
        leader: NodeId,
    ) -> Result<(), String> {
        if self.children.contains_key(&node_id) {
            return Err(format!("publication node {node_id} is already running"));
        }
        self.start_node(executable, node_id)?;
        wait_ready(self.address(node_id)?).await?;
        let expected = status(self.address(leader)?)
            .await?
            .last_applied_index
            .unwrap_or_default();
        if !wait_for_applied_index(self.address(node_id)?, expected).await {
            return Err(format!(
                "publication node {node_id} did not recover applied index {expected}"
            ));
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
                publication_signer: Some(crate::RecoverySignerConfig {
                    private_key_seed: publication_pop_seed(node_id),
                }),
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

fn publication_pop_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-PUBLICATION-POP-SIGNER-V1\0");
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

fn fixture_incarnations(node_ids: &[NodeId]) -> BTreeMap<NodeId, [u8; 16]> {
    node_ids
        .iter()
        .map(|node_id| (*node_id, [u8::try_from(node_id % 251).unwrap_or(1); 16]))
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

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
}

async fn wait_for_applied_index(address: &str, expected: u64) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        if status(address).await.is_ok_and(|node| {
            node.last_applied_index
                .is_some_and(|index| index >= expected)
        }) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
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
