#![allow(clippy::result_large_err)]

//! Consensus adapter boundary for objectKV.
//!
//! The bootstrap implementation pins `OpenRaft` 0.9.25 behind objectKV-owned
//! request bytes and a per-node stable journal. Network transport and cluster
//! orchestration are deliberately separate from this storage admission gate.

mod cluster_contract;
mod contract;
mod generation;
mod generation_process_contract;
mod process_contract;
mod process_node;
mod rpc;
mod sim_network;
mod state_machine;

pub use cluster_contract::{run_raft_cluster_contract, RaftClusterMode, RaftClusterReport};
pub use contract::{run_raft_storage_contract, RaftStorageMode, RaftStorageReport};
pub use generation::{
    recovery_membership_digest, recovery_public_key, sign_recovery_statement, ConsensusProcessRole,
    GenerationAction, GenerationApplyResponse, GenerationAuthorityFaults, GenerationAuthorityState,
    GenerationCommand, GenerationCommandStatus, GenerationCredential, GenerationFenceConfig,
    GenerationFenceFaults, GenerationPhase, RecoveryAttestation, RecoveryCertificate,
    RecoveryCertificateKind, RecoveryCertificateStatement, RecoveryLogPosition,
    RecoverySignerConfig,
};
pub use generation_process_contract::{
    run_generation_process_contract, GenerationProcessMode, GenerationProcessReport,
};
pub use process_contract::{run_raft_process_contract, RaftProcessMode, RaftProcessReport};
pub use process_node::{run_process_node, ProcessNodeConfig, ProcessNodePolicy};
pub use state_machine::{
    ApplyError, ApplyResponse, ClientCommand, RequestIdentity, StateMachineStore,
};

use okv_wal::{JournalError, JournalMarker, NodeJournal};
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogId, RaftLogReader, StorageError, Vote,
};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// objectKV bootstrap Raft type boundary.
    pub TypeConfig:
        D = Vec<u8>,
        R = ApplyResponse,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type RaftEntry = Entry<TypeConfig>;

/// Durable `OpenRaft` log adapter backed by one objectKV node journal.
#[derive(Clone, Debug)]
pub struct OpenRaftLogStore {
    inner: Arc<Mutex<NodeJournal>>,
    root: Arc<PathBuf>,
}

impl OpenRaftLogStore {
    /// Create or recover one `OpenRaft` node's stable log.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be opened or recovered.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, JournalError> {
        let root = root.as_ref().to_path_buf();
        let journal = NodeJournal::open(&root)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(journal)),
            root: Arc::new(root),
        })
    }

    /// Root directory of this node's stable log.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Exact journal path for bounded crash and corruption probes.
    pub async fn journal_path(&self) -> PathBuf {
        self.inner.lock().await.path().to_path_buf()
    }

    /// Current physical stable-log bytes.
    ///
    /// # Errors
    ///
    /// Returns an `OpenRaft` storage error when file metadata cannot be read.
    pub async fn physical_bytes(&self) -> Result<u64, StorageError<NodeId>> {
        self.inner
            .lock()
            .await
            .physical_bytes()
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, &error))
    }

    /// Whether the most recent open repaired an incomplete final frame.
    pub async fn recovered_torn_tail(&self) -> bool {
        self.inner.lock().await.recovered_torn_tail()
    }

    async fn persist_entries<I>(&self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| {
                let index = entry.get_log_id().index;
                serde_json::to_vec(&entry)
                    .map(|bytes| (index, bytes))
                    .map_err(|error| {
                        storage_error(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, &error)
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.inner
            .lock()
            .await
            .append(&encoded)
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, &error))
    }
}

impl RaftLogReader<TypeConfig> for OpenRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        let encoded = self.inner.lock().await.state().entries(range);
        encoded
            .into_iter()
            .map(|(index, bytes)| decode_entry(index, &bytes))
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for OpenRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let journal = self.inner.lock().await;
        let last_purged_log_id = journal
            .state()
            .last_purged()
            .map(decode_marker)
            .transpose()?;
        let last_log_id = journal
            .state()
            .last_entry()
            .map(|(index, bytes)| decode_entry(index, bytes).map(|entry| entry.log_id))
            .transpose()?
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(vote)
            .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Write, &error))?;
        self.inner
            .lock()
            .await
            .save_vote(&bytes)
            .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Write, &error))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.inner
            .lock()
            .await
            .state()
            .vote()
            .map(|bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Read, &error))
            })
            .transpose()
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let encoded = committed
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, &error))?;
        self.inner
            .lock()
            .await
            .save_committed(encoded.as_deref())
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, &error))
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.inner
            .lock()
            .await
            .state()
            .committed()
            .map(|bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, &error))
            })
            .transpose()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        self.persist_entries(entries).await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner
            .lock()
            .await
            .truncate(log_id.index)
            .map_err(|error| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, &error))
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let payload = serde_json::to_vec(&log_id)
            .map_err(|error| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Write, &error))?;
        self.inner
            .lock()
            .await
            .purge(JournalMarker {
                index: log_id.index,
                payload,
            })
            .map_err(|error| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, &error))
    }
}

fn decode_entry(index: u64, bytes: &[u8]) -> Result<RaftEntry, StorageError<NodeId>> {
    let entry: RaftEntry = serde_json::from_slice(bytes)
        .map_err(|error| storage_error(ErrorSubject::LogIndex(index), ErrorVerb::Read, &error))?;
    if entry.log_id.index != index {
        return Err(storage_error(
            ErrorSubject::LogIndex(index),
            ErrorVerb::Read,
            &format!(
                "journal index {index} disagrees with encoded entry index {}",
                entry.log_id.index
            ),
        ));
    }
    Ok(entry)
}

fn decode_marker(marker: &JournalMarker) -> Result<LogId<NodeId>, StorageError<NodeId>> {
    let log_id: LogId<NodeId> = serde_json::from_slice(&marker.payload).map_err(|error| {
        storage_error(
            ErrorSubject::LogIndex(marker.index),
            ErrorVerb::Read,
            &error,
        )
    })?;
    if log_id.index != marker.index {
        return Err(storage_error(
            ErrorSubject::LogIndex(marker.index),
            ErrorVerb::Read,
            &"purge marker index disagrees with encoded log identifier",
        ));
    }
    Ok(log_id)
}

fn storage_error(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    error: &dyn std::fmt::Display,
) -> StorageError<NodeId> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::RaftEntry as _;
    use openraft::testing::{StoreBuilder, Suite};
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "okv-consensus-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct Builder;

    impl StoreBuilder<TypeConfig, OpenRaftLogStore, Arc<StateMachineStore>, TempDir> for Builder {
        async fn build(
            &self,
        ) -> Result<(TempDir, OpenRaftLogStore, Arc<StateMachineStore>), StorageError<NodeId>>
        {
            let root = TempDir::new("suite");
            let store = OpenRaftLogStore::open(&root.0)
                .map_err(|error| storage_error(ErrorSubject::Store, ErrorVerb::Write, &error))?;
            Ok((root, store, Arc::new(StateMachineStore::default())))
        }
    }

    #[test]
    fn passes_openraft_storage_conformance_suite() {
        Suite::<TypeConfig, OpenRaftLogStore, Arc<StateMachineStore>, Builder, TempDir>::test_all(
            Builder,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn openraft_vote_commit_and_conflict_replacement_survive_reopen() {
        let root = TempDir::new("reopen");
        let mut store = OpenRaftLogStore::open(&root.0).unwrap();
        let vote = Vote::new(3, 1);
        store.save_vote(&vote).await.unwrap();

        let old = [entry(3, 1, 0), entry(3, 1, 1), entry(3, 1, 2)];
        store.persist_entries(old).await.unwrap();
        store.save_committed(Some(log_id(3, 1, 1))).await.unwrap();
        store.truncate(log_id(4, 2, 1)).await.unwrap();
        let replacement = [entry(4, 2, 1), entry(4, 2, 2)];
        store.persist_entries(replacement).await.unwrap();
        drop(store);

        let mut reopened = OpenRaftLogStore::open(&root.0).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        assert_eq!(
            reopened.read_committed().await.unwrap(),
            Some(log_id(3, 1, 1))
        );
        let entries = reopened.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[1].log_id, log_id(4, 2, 1));
        assert_eq!(entries[2].log_id, log_id(4, 2, 2));
        assert!(matches!(entries[0].payload, EntryPayload::Blank));
    }

    fn entry(term: u64, node_id: NodeId, index: u64) -> RaftEntry {
        RaftEntry::new_blank(log_id(term, node_id, index))
    }

    fn log_id(term: u64, node_id: NodeId, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, node_id), index)
    }
}
