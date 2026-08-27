use crate::rpc::{
    read_response, write_request, NodeStatus, PurgeLogRequest, BUILD_SNAPSHOT, ELECT,
    GENERATION_WRITE, HEARTBEAT, INITIALIZE, LOG_IO_STATS, PURGE_LOG, STATUS,
};
use crate::{
    recovery_public_key, ConsensusProcessRole, GenerationAction, GenerationApplyResponse,
    GenerationAuthorityFaults, GenerationCommand, GenerationCommandStatus, GenerationFenceFaults,
    NodeId, OpenRaftIoStats, ProcessJournalCompactionObservation, ProcessNodeConfig,
    ProcessNodePolicy, PublicationAuthorityFaults, PublicationClient, RequestIdentity,
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
    executable: PathBuf,
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
            executable: executable.to_path_buf(),
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

    /// Stable authority node map used by generation-fenced data fixtures.
    #[must_use]
    pub fn authority_nodes(&self) -> BTreeMap<NodeId, String> {
        self.addresses.clone()
    }

    /// Number of real authority processes owned by this fixture.
    #[must_use]
    pub fn process_count(&self) -> usize {
        self.children.len()
    }

    /// Read cumulative stable-log observations from every live publication
    /// voter.
    ///
    /// # Errors
    ///
    /// Returns an error when any voter cannot return its local counters.
    #[doc(hidden)]
    pub async fn io_stats(&self) -> Result<BTreeMap<NodeId, OpenRaftIoStats>, String> {
        let mut stats = BTreeMap::new();
        for node_id in self.children.keys() {
            stats.insert(
                *node_id,
                control(self.address(*node_id)?, LOG_IO_STATS, &()).await?,
            );
        }
        Ok(stats)
    }

    /// Converge, snapshot, purge, and canonical-compact every publication
    /// voter through one common applied position.
    ///
    /// # Errors
    ///
    /// Returns an error when convergence, snapshot, purge, compaction, or
    /// observation fails.
    pub async fn snapshot_and_purge_applied_all(
        &self,
    ) -> Result<(u64, BTreeMap<NodeId, ProcessJournalCompactionObservation>), String> {
        let through_index = wait_cluster_applied(&self.addresses, self.address(101)?).await?;
        let before = self.io_stats().await?;
        for node_id in self.children.keys() {
            retry_control(self.address(*node_id)?, BUILD_SNAPSHOT, &()).await?;
        }
        for node_id in self.children.keys() {
            wait_for_snapshot(self.address(*node_id)?, through_index).await?;
        }
        for node_id in self.children.keys() {
            retry_control(
                self.address(*node_id)?,
                PURGE_LOG,
                &PurgeLogRequest { through_index },
            )
            .await?;
        }

        let mut observations = BTreeMap::new();
        for node_id in self.children.keys() {
            let status = wait_for_purge(self.address(*node_id)?, through_index).await?;
            let after: OpenRaftIoStats =
                control(self.address(*node_id)?, LOG_IO_STATS, &()).await?;
            let prior = before
                .get(node_id)
                .ok_or_else(|| format!("missing publication stats for voter {node_id}"))?;
            observations.insert(
                *node_id,
                ProcessJournalCompactionObservation {
                    node_id: *node_id,
                    snapshot_index: status.snapshot_index.unwrap_or(0),
                    purged_index: status.purged_index.unwrap_or(0),
                    journal_bytes_before: prior.physical_journal_bytes,
                    journal_bytes_after: after.physical_journal_bytes,
                    snapshot_bytes: after.state_machine_snapshot_bytes,
                    compaction_calls: after
                        .compaction_calls
                        .saturating_sub(prior.compaction_calls),
                    compaction_reclaimed_bytes: after
                        .compaction_reclaimed_bytes
                        .saturating_sub(prior.compaction_reclaimed_bytes),
                },
            );
        }
        Ok((through_index, observations))
    }

    /// Stop every publication voter, reopen snapshot plus retained journal,
    /// and elect the original voter.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown, restart, election, or catch-up fails.
    pub async fn restart_all_and_elect_initial(&mut self) -> Result<(), String> {
        for child in self.children.values_mut() {
            child.kill().map_err(|error| error.to_string())?;
            child.wait().map_err(|error| error.to_string())?;
        }
        self.children.clear();
        let executable = self.executable.clone();
        for node_id in AUTHORITY_NODES {
            self.start_node(&executable, node_id)?;
        }
        for node_id in AUTHORITY_NODES {
            wait_ready(self.address(node_id)?).await?;
        }
        if !elect_until_leader(self.address(101)?, 101).await {
            return Err("reopened publication quorum could not elect node 101".to_owned());
        }
        wait_cluster_applied(&self.addresses, self.address(101)?).await?;
        Ok(())
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

pub(crate) fn recovery_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-PUBLISHER-PROCESS-RECOVERY-SIGNER-V1\0");
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

pub(crate) fn recovery_members(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
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

async fn wait_for_snapshot(address: &str, through_index: u64) -> Result<NodeStatus, String> {
    let mut last = None;
    for _ in 0..RETRY_ATTEMPTS {
        if let Ok(status) = control::<_, NodeStatus>(address, STATUS, &()).await {
            last = status.snapshot_index;
            if last.is_some_and(|index| index >= through_index) {
                return Ok(status);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "publication voter at {address} snapshot reached {last:?}, expected {through_index}"
    ))
}

async fn wait_for_purge(address: &str, through_index: u64) -> Result<NodeStatus, String> {
    let mut last = None;
    for _ in 0..RETRY_ATTEMPTS {
        if let Ok(status) = control::<_, NodeStatus>(address, STATUS, &()).await {
            last = status.purged_index;
            if last.is_some_and(|index| index >= through_index) {
                return Ok(status);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "publication voter at {address} purge reached {last:?}, expected {through_index}"
    ))
}

async fn wait_cluster_applied(
    addresses: &BTreeMap<NodeId, String>,
    leader_address: &str,
) -> Result<u64, String> {
    let leader = control::<_, NodeStatus>(leader_address, STATUS, &()).await?;
    let expected = leader.last_log_index.unwrap_or(0);
    let mut observed = BTreeMap::new();
    for _ in 0..RETRY_ATTEMPTS {
        let _: Result<(), String> = control(leader_address, HEARTBEAT, &()).await;
        observed.clear();
        for (node_id, address) in addresses {
            if let Ok(status) = control::<_, NodeStatus>(address, STATUS, &()).await {
                observed.insert(*node_id, status.last_applied_index.unwrap_or(0));
            }
        }
        if observed.len() == addresses.len()
            && observed.values().all(|applied| *applied >= expected)
        {
            return Ok(expected);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "publication voters did not converge through log index {expected}: {observed:?}"
    ))
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
