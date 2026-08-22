use crate::{NodeId, TypeConfig};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Result returned after one committed Raft entry is applied.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyResponse {
    pub applied_log_index: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    applied_payloads: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Bootstrap state machine used by the storage conformance and replication gates.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    data: RwLock<StateMachineData>,
    snapshot_sequence: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachineStore {
    /// Applied normal-entry payloads in log order.
    pub async fn applied_payloads(&self) -> Vec<Vec<u8>> {
        self.data.read().await.applied_payloads.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state = self.data.read().await.clone();
        let data = serde_json::to_vec(&state)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let snapshot_sequence = self.snapshot_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = state.last_applied_log.map_or_else(
            || format!("--{snapshot_sequence}"),
            |last| format!("{}-{}-{snapshot_sequence}", last.leader_id, last.index),
        );
        let meta = SnapshotMeta {
            last_log_id: state.last_applied_log,
            last_membership: state.last_membership,
            snapshot_id,
        };
        *self.current_snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state = self.data.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state = self.data.write().await;
        let mut responses = Vec::new();
        for entry in entries {
            state.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(payload) => state.applied_payloads.push(payload),
                EntryPayload::Membership(membership) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                }
            }
            responses.push(ApplyResponse {
                applied_log_index: entry.log_id.index,
            });
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<NodeId>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };
        let decoded: StateMachineData = serde_json::from_slice(&stored.data).map_err(|error| {
            StorageIOError::read_snapshot(Some(stored.meta.signature()), &error)
        })?;
        *self.data.write().await = decoded;
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .as_ref()
            .map(|stored| Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
    }
}
