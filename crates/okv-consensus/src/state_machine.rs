use crate::{
    GenerationApplyResponse, GenerationAuthorityFaults, GenerationAuthorityState,
    GenerationCommand, GenerationCredential, GenerationFenceFaults, GenerationPhase, NodeId,
    TypeConfig,
};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

const COMMAND_MAGIC: &[u8] = b"OKVQ1";

/// Stable identity for one retryable client command.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RequestIdentity {
    pub client_id: u64,
    pub request_id: u64,
}

/// Versioned application command carried as opaque Raft application bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientCommand {
    pub identity: RequestIdentity,
    pub credential: Option<GenerationCredential>,
    pub payload: Vec<u8>,
}

impl ClientCommand {
    /// Encode the versioned command into objectKV-owned application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// Result returned after one committed Raft entry is applied.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyResponse {
    pub applied_log_index: u64,
    pub identity: Option<RequestIdentity>,
    pub error: Option<ApplyError>,
    pub generation: Option<GenerationApplyResponse>,
}

/// Application-level rejection reconstructed identically after replay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyError {
    ConflictingRequestIdentity,
    GenerationFenced,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    applied_payloads: Vec<Vec<u8>>,
    durable_outcomes: BTreeMap<RequestIdentity, ApplyResponse>,
    request_fingerprints: BTreeMap<RequestIdentity, [u8; 32]>,
    generation_authority: GenerationAuthorityState,
}

#[derive(Clone, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Bootstrap state machine used by the storage conformance and replication gates.
#[derive(Debug)]
pub struct StateMachineStore {
    data: RwLock<StateMachineData>,
    snapshot_sequence: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    deduplicate_requests: bool,
    generation_faults: GenerationAuthorityFaults,
    generation_fence_faults: GenerationFenceFaults,
}

impl Default for StateMachineStore {
    fn default() -> Self {
        Self::new(true)
    }
}

impl StateMachineStore {
    /// Create a state machine, optionally disabling request deduplication for a
    /// bounded negative control.
    #[must_use]
    pub fn new(deduplicate_requests: bool) -> Self {
        Self::new_with_generation_faults(
            deduplicate_requests,
            GenerationAuthorityFaults::default(),
            GenerationFenceFaults::default(),
        )
    }

    /// Create a state machine with bounded authority faults for negative tests.
    #[must_use]
    pub fn new_with_generation_faults(
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
    ) -> Self {
        Self {
            data: RwLock::new(StateMachineData::default()),
            snapshot_sequence: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
        }
    }

    /// Applied normal-entry payloads in log order.
    pub async fn applied_payloads(&self) -> Vec<Vec<u8>> {
        self.data.read().await.applied_payloads.clone()
    }

    /// Recovered response for one request identity.
    pub async fn durable_outcome(&self, identity: RequestIdentity) -> Option<ApplyResponse> {
        self.data
            .read()
            .await
            .durable_outcomes
            .get(&identity)
            .cloned()
    }

    /// Current coordinator authority state at this node's applied position.
    pub async fn generation_authority(&self) -> GenerationAuthorityState {
        self.data.read().await.generation_authority.clone()
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

    #[allow(clippy::too_many_lines)]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state = self.data.write().await;
        let mut responses = Vec::new();
        for entry in entries {
            state.last_applied_log = Some(entry.log_id);
            let mut response = ApplyResponse {
                applied_log_index: entry.log_id.index,
                identity: None,
                error: None,
                generation: None,
            };
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(payload) => {
                    if let Some(command) = GenerationCommand::decode(&payload)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                    {
                        let fingerprint = request_fingerprint(&payload);
                        if let Some(recovered) = state.durable_outcomes.get(&command.identity) {
                            if state.request_fingerprints.get(&command.identity)
                                == Some(&fingerprint)
                            {
                                response = recovered.clone();
                            } else {
                                response.identity = Some(command.identity);
                                response.error = Some(ApplyError::ConflictingRequestIdentity);
                            }
                        } else {
                            let status = state
                                .generation_authority
                                .apply(&command.action, self.generation_faults);
                            response.identity = Some(command.identity);
                            response.generation = Some(GenerationApplyResponse {
                                status,
                                state: state.generation_authority.clone(),
                                applied_log_index: entry.log_id.index,
                            });
                            state
                                .request_fingerprints
                                .insert(command.identity, fingerprint);
                            state
                                .durable_outcomes
                                .insert(command.identity, response.clone());
                        }
                    } else if let Some(command) = ClientCommand::decode(&payload)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                    {
                        let fingerprint = request_fingerprint(&payload);
                        let generation_authorized =
                            command.credential.as_ref().is_some_and(|credential| {
                                state.generation_authority.authorizes(
                                    credential.generation,
                                    &credential.transaction_system_id,
                                ) || (self.generation_fence_faults.accept_apply_during_recovery
                                    && state.generation_authority.recovery_id.is_some_and(
                                        |recovery_id| {
                                            state.generation_authority.authorizes_recovery(
                                                credential.generation,
                                                recovery_id,
                                                &credential.transaction_system_id,
                                            )
                                        },
                                    ))
                            });
                        let generation_fenced = state.generation_authority.phase
                            != GenerationPhase::Uninitialized
                            && !self.generation_fence_faults.bypass_apply_fence
                            && !generation_authorized;
                        if self.deduplicate_requests {
                            if let Some(recovered) =
                                state.durable_outcomes.get(&command.identity).cloned()
                            {
                                if state.request_fingerprints.get(&command.identity)
                                    == Some(&fingerprint)
                                {
                                    response = recovered;
                                } else {
                                    response.identity = Some(command.identity);
                                    response.error = Some(ApplyError::ConflictingRequestIdentity);
                                }
                            } else {
                                response.identity = Some(command.identity);
                                if generation_fenced {
                                    response.error = Some(ApplyError::GenerationFenced);
                                } else {
                                    state.applied_payloads.push(command.payload);
                                }
                                state
                                    .request_fingerprints
                                    .insert(command.identity, fingerprint);
                                state
                                    .durable_outcomes
                                    .insert(command.identity, response.clone());
                            }
                        } else {
                            response.identity = Some(command.identity);
                            if generation_fenced {
                                response.error = Some(ApplyError::GenerationFenced);
                            } else {
                                state.applied_payloads.push(command.payload);
                            }
                            state
                                .request_fingerprints
                                .insert(command.identity, fingerprint);
                            state
                                .durable_outcomes
                                .insert(command.identity, response.clone());
                        }
                    } else {
                        state.applied_payloads.push(payload);
                    }
                }
                EntryPayload::Membership(membership) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                }
            }
            responses.push(response);
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

fn request_fingerprint(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}
