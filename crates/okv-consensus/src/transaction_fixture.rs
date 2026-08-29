use crate::rpc::{
    read_response, write_request, NodeStatus, PurgeLogRequest, BUILD_SNAPSHOT,
    DATA_GENERATION_WRITE, ELECT, HEARTBEAT, INITIALIZE, LOG_IO_STATS, PURGE_LOG, STATUS,
};
use crate::{
    ConsensusProcessRole, GenerationAction, GenerationApplyResponse, GenerationCommand,
    GenerationCommandStatus, GenerationCredential, GenerationFenceConfig, NodeId, OpenRaftIoStats,
    ProcessNodeConfig, ProcessNodePolicy, RecoverySignerConfig, RequestIdentity,
    TransactionLogClient,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const DATA_NODES: [NodeId; 3] = [201, 202, 203];
const RETRY_ATTEMPTS: usize = 500;

/// One voter's durable snapshot and physical journal-compaction observation.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct ProcessJournalCompactionObservation {
    pub node_id: NodeId,
    pub snapshot_index: u64,
    pub purged_index: u64,
    pub journal_bytes_before: u64,
    pub journal_bytes_after: u64,
    pub snapshot_bytes: u64,
    pub compaction_calls: u64,
    pub compaction_reclaimed_bytes: u64,
}

/// Real-process transaction-authority fixture used by physical recovery gates.
#[doc(hidden)]
pub struct TransactionAuthorityProcessFixture {
    root: PathBuf,
    executable: PathBuf,
    addresses: BTreeMap<NodeId, String>,
    children: BTreeMap<NodeId, Child>,
    generation_fence: Option<GenerationFenceConfig>,
    acknowledge_before_quorum: bool,
}

impl TransactionAuthorityProcessFixture {
    /// Start, initialize, and elect three data-authority voters.
    ///
    /// # Errors
    ///
    /// Returns an error when process startup, stable storage, initialization,
    /// or election cannot complete.
    pub async fn start(executable: &Path, seed: u64) -> Result<Self, String> {
        Self::start_internal(executable, seed, None, false).await
    }

    /// Start the same three-voter topology with the early-ack poison enabled.
    ///
    /// # Errors
    ///
    /// Returns an error when process startup, initialization, or election
    /// cannot complete.
    #[doc(hidden)]
    pub async fn start_early_ack_poison(executable: &Path, seed: u64) -> Result<Self, String> {
        Self::start_internal(executable, seed, None, true).await
    }

    /// Start a data quorum bound to an already bootstrapped generation and
    /// publication authority.
    ///
    /// # Errors
    ///
    /// Returns an error when startup, authority authorization, or data-mirror
    /// bootstrap cannot complete.
    pub async fn start_fenced(
        executable: &Path,
        seed: u64,
        authority_nodes: BTreeMap<NodeId, String>,
    ) -> Result<Self, String> {
        if authority_nodes.is_empty() {
            return Err("fenced transaction fixture requires authority nodes".to_owned());
        }
        Self::start_internal(
            executable,
            seed,
            Some(GenerationFenceConfig {
                credential: GenerationCredential {
                    generation: 7,
                    transaction_system_id: "tx-g7".to_owned(),
                },
                recovery_id: None,
                authority_nodes,
            }),
            false,
        )
        .await
    }

    async fn start_internal(
        executable: &Path,
        seed: u64,
        generation_fence: Option<GenerationFenceConfig>,
        acknowledge_before_quorum: bool,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "transaction authority executable does not exist: {}",
                executable.display()
            ));
        }
        let addresses = allocate_addresses(&DATA_NODES)?;
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "okv-transaction-authority-{seed}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        let mut fixture = Self {
            root,
            executable: executable.to_path_buf(),
            addresses,
            children: BTreeMap::new(),
            generation_fence,
            acknowledge_before_quorum,
        };
        for node_id in DATA_NODES {
            fixture.start_node(executable, node_id)?;
        }
        for node_id in DATA_NODES {
            wait_ready(fixture.address(node_id)?).await?;
        }
        retry_control(fixture.address(201)?, INITIALIZE, &()).await?;
        if !elect_until_leader(fixture.address(201)?, 201).await {
            return Err("transaction authority leader election failed".to_owned());
        }
        wait_cluster_applied(&fixture.addresses, fixture.address(201)?).await?;
        if fixture.generation_fence.is_some() {
            let bootstrap = retry_data_generation_write(
                fixture.address(201)?,
                &GenerationCommand {
                    identity: RequestIdentity {
                        client_id: seed.max(1),
                        request_id: 1,
                    },
                    action: GenerationAction::Bootstrap {
                        cell_id: 17,
                        generation: 7,
                        transaction_system_id: "tx-g7".to_owned(),
                        transaction_system_members: crate::publication_fixture::recovery_members(
                            &DATA_NODES,
                        )?,
                        wal_root: "wal-g7".to_owned(),
                        control_root_version: 1,
                    },
                },
            )
            .await?;
            if bootstrap.status != GenerationCommandStatus::Accepted
                || !bootstrap.state.authorizes(7, "tx-g7")
            {
                return Err("transaction authority generation bootstrap failed".to_owned());
            }
        }
        Ok(fixture)
    }

    /// Client over all voter endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error only if the internally validated endpoint set is empty.
    pub fn client(&self) -> Result<TransactionLogClient, String> {
        TransactionLogClient::new(self.endpoints())
    }

    /// Stable endpoint set passed to replacement worker processes.
    #[must_use]
    pub fn endpoints(&self) -> Vec<String> {
        self.addresses.values().cloned().collect()
    }

    /// Number of real authority processes owned by this fixture.
    #[must_use]
    pub fn process_count(&self) -> usize {
        self.children.len()
    }

    /// Return the aggregate bytes in all three voter scratch directories.
    ///
    /// # Errors
    ///
    /// Returns an error when a fixture-owned path cannot be inspected.
    #[doc(hidden)]
    pub fn physical_storage_bytes(&self) -> Result<u64, String> {
        directory_bytes(&self.root)
    }

    /// Read cumulative stable-log observations from every live data voter.
    ///
    /// # Errors
    ///
    /// Returns an error when any live voter cannot return its local counters.
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

    /// Durably snapshot every live voter, purge only through that proven
    /// position, and wait for canonical node-journal compaction.
    ///
    /// # Errors
    ///
    /// Returns an error when a voter is behind, snapshot or purge fails, or the
    /// physical compaction observation cannot be read before the retry budget.
    pub async fn snapshot_and_purge_all(
        &self,
        through_index: u64,
    ) -> Result<BTreeMap<NodeId, ProcessJournalCompactionObservation>, String> {
        if through_index == 0 {
            return Err("snapshot and purge index must be positive".to_owned());
        }
        let before = self.io_stats().await?;
        for node_id in self.children.keys() {
            let status: NodeStatus = control(self.address(*node_id)?, STATUS, &()).await?;
            if status
                .last_applied_index
                .is_none_or(|index| index < through_index)
            {
                return Err(format!(
                    "transaction voter {node_id} is behind purge index {through_index}"
                ));
            }
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
                .ok_or_else(|| format!("missing pre-compaction stats for voter {node_id}"))?;
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
        Ok(observations)
    }

    /// Converge every voter, durably snapshot the complete applied position,
    /// then purge and compact through exactly that position.
    ///
    /// # Errors
    ///
    /// Returns an error when the quorum cannot converge or any snapshot,
    /// purge, compaction, or observation step fails.
    pub async fn snapshot_and_purge_applied_all(
        &self,
    ) -> Result<(u64, BTreeMap<NodeId, ProcessJournalCompactionObservation>), String> {
        let through_index = wait_cluster_applied(&self.addresses, self.address(201)?).await?;
        let observations = self.snapshot_and_purge_all(through_index).await?;
        Ok((through_index, observations))
    }

    /// Attempt one unsafe purge without first creating a durable snapshot.
    /// Reserved for the G4.11a fail-closed poison.
    ///
    /// # Errors
    ///
    /// Returns the expected guard rejection or an unexpected transport error.
    #[doc(hidden)]
    pub async fn purge_without_snapshot_once(
        &self,
        node_id: NodeId,
        through_index: u64,
    ) -> Result<(), String> {
        control(
            self.address(node_id)?,
            PURGE_LOG,
            &PurgeLogRequest { through_index },
        )
        .await
    }

    /// Kill both followers while retaining the initial leader process.
    ///
    /// # Errors
    ///
    /// Returns an error when either follower is absent or cannot be killed.
    #[doc(hidden)]
    pub fn kill_followers_for_poison(&mut self) -> Result<(), String> {
        for node_id in [202, 203] {
            let mut child = self
                .children
                .remove(&node_id)
                .ok_or_else(|| format!("transaction follower {node_id} is absent"))?;
            child.kill().map_err(|error| error.to_string())?;
            child.wait().map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Kill the isolated initial leader without electing a successor.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial leader is absent or cannot be killed.
    #[doc(hidden)]
    pub fn kill_isolated_initial_leader(&mut self) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&201)
            .ok_or_else(|| "isolated transaction leader is absent".to_owned())?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Restart the two unpoisoned followers and elect node 202.
    ///
    /// # Errors
    ///
    /// Returns an error when either process cannot restart or the surviving
    /// quorum cannot elect node 202.
    #[doc(hidden)]
    pub async fn restart_followers_and_elect_for_poison(&mut self) -> Result<(), String> {
        let executable = self.executable.clone();
        for node_id in [202, 203] {
            self.start_node(&executable, node_id)?;
            wait_ready(self.address(node_id)?).await?;
        }
        if !elect_until_leader(self.address(202)?, 202).await {
            return Err("poison recovery quorum could not elect node 202".to_owned());
        }
        Ok(())
    }

    /// Wait until one local voter has applied at least the requested version.
    ///
    /// # Errors
    ///
    /// Returns an error when the voter does not catch up within the bounded
    /// retry budget.
    #[doc(hidden)]
    pub async fn wait_for_voter_version(
        &self,
        node_id: NodeId,
        expected_version: u64,
    ) -> Result<(), String> {
        let mut observed = 0_u64;
        for _ in 0..RETRY_ATTEMPTS {
            if let Ok(status) = control::<_, NodeStatus>(self.address(node_id)?, STATUS, &()).await
            {
                observed = status.transaction.current_version;
                if observed >= expected_version {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "transaction voter {node_id} reached version {observed}, expected {expected_version}"
        ))
    }

    /// Read one voter's local transaction state after a caller-established
    /// catch-up barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when the voter is unavailable.
    #[doc(hidden)]
    pub async fn voter_transaction_view(
        &self,
        node_id: NodeId,
    ) -> Result<okv_transaction::TransactionAuthorityView, String> {
        control::<_, NodeStatus>(self.address(node_id)?, STATUS, &())
            .await
            .map(|status| status.transaction)
    }

    /// Kill the initial data leader and elect node 202 from the surviving
    /// quorum.
    ///
    /// # Errors
    ///
    /// Returns an error when the leader is absent or a successor cannot be
    /// elected.
    pub async fn kill_initial_leader_and_elect_successor(&mut self) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&201)
            .ok_or_else(|| "initial transaction leader process is absent".to_owned())?;
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        if !elect_until_leader(self.address(202)?, 202).await {
            return Err("transaction authority successor election failed".to_owned());
        }
        Ok(())
    }

    /// Restart the killed initial voter on its existing stable state.
    ///
    /// # Errors
    ///
    /// Returns an error when the process is already live or cannot become
    /// reachable.
    pub async fn restart_initial_voter(&mut self) -> Result<(), String> {
        if self.children.contains_key(&201) {
            return Err("initial transaction voter is already running".to_owned());
        }
        let executable = self.executable.clone();
        self.start_node(&executable, 201)?;
        wait_ready(self.address(201)?).await
    }

    /// Kill every voter, reopen all persistent snapshots and journals, and
    /// elect the original voter from the recovered quorum.
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
        for node_id in DATA_NODES {
            self.start_node(&executable, node_id)?;
        }
        for node_id in DATA_NODES {
            wait_ready(self.address(node_id)?).await?;
        }
        if !elect_until_leader(self.address(201)?, 201).await {
            return Err("reopened transaction quorum could not elect node 201".to_owned());
        }
        wait_cluster_applied(&self.addresses, self.address(201)?).await?;
        Ok(())
    }

    fn start_node(&mut self, executable: &Path, node_id: NodeId) -> Result<(), String> {
        let config = ProcessNodeConfig {
            node_id,
            root: self.root.join(format!("node-{node_id}")),
            nodes: self.addresses.clone(),
            deduplicate_requests: true,
            acknowledge_before_quorum: self.acknowledge_before_quorum,
            policy: ProcessNodePolicy {
                role: ConsensusProcessRole::Data,
                generation_fence: self.generation_fence.clone(),
                recovery_signer: self
                    .generation_fence
                    .as_ref()
                    .map(|_| RecoverySignerConfig {
                        private_key_seed: crate::publication_fixture::recovery_seed(node_id),
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
            .map_err(|error| format!("failed to start transaction node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing transaction address for node {node_id}"))
    }
}

async fn retry_data_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, DATA_GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "data generation bootstrap failed at {address}: {last}"
    ))
}

impl Drop for TransactionAuthorityProcessFixture {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if self.root.starts_with(std::env::temp_dir())
            && self.root.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-transaction-authority-")
            })
        {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
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
        "transaction node did not become ready at {address}: {last}"
    ))
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
        "transaction voter at {address} snapshot reached {last:?}, expected {through_index}"
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
        "transaction voter at {address} purge reached {last:?}, expected {through_index}"
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
        "transaction voters did not converge through initialization index {expected}: {observed:?}"
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

fn directory_bytes(root: &Path) -> Result<u64, String> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if file_type.is_dir() {
            bytes = bytes.saturating_add(directory_bytes(&entry.path())?);
        } else if file_type.is_file() {
            bytes =
                bytes.saturating_add(entry.metadata().map_err(|error| error.to_string())?.len());
        }
    }
    Ok(bytes)
}
