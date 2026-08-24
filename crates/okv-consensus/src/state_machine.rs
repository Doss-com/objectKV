use crate::{
    cell_transaction::CellTransactionState, CellCommittedEnvelopeFeed,
    CellCommittedEnvelopeRequest, CellStagedTransactionApplyResponse, CellStagedTransactionCommand,
    CellStateSnapshot, CellTransactionApplyResponse, CellTransactionCommand,
    GenerationApplyResponse, GenerationAuthorityFaults, GenerationAuthorityState,
    GenerationCommand, GenerationCredential, GenerationFenceFaults, GenerationPhase, NodeId,
    PublicationApplyResponse, PublicationAuthorityContext, PublicationAuthorityFaults,
    PublicationAuthorityPosition, PublicationAuthorityState, PublicationCommand,
    PublicationCommandStatus, PublicationFenceFaults, RecoveryLogPosition, TypeConfig,
};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

const COMMAND_MAGIC: &[u8] = b"OKVQ1";
const SNAPSHOT_MAGIC: &[u8; 4] = b"OKVS";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;
const SNAPSHOT_HEADER_BYTES: usize = 4 + 2 + 4 + 8 + 32;
const SNAPSHOT_FILE: &str = "state-machine.snapshot";

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
    pub publication: Option<PublicationApplyResponse>,
    pub cell_transaction: Option<CellTransactionApplyResponse>,
    #[serde(default)]
    pub cell_staged_transaction: Option<CellStagedTransactionApplyResponse>,
}

/// Application-level rejection reconstructed identically after replay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyError {
    ConflictingRequestIdentity,
    GenerationFenced,
    UnknownCommandVersion,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    applied_payloads: Vec<Vec<u8>>,
    #[serde(with = "request_map_serde")]
    durable_outcomes: BTreeMap<RequestIdentity, ApplyResponse>,
    #[serde(with = "request_map_serde")]
    request_fingerprints: BTreeMap<RequestIdentity, [u8; 32]>,
    generation_authority: GenerationAuthorityState,
    #[serde(default)]
    publication_authority: PublicationAuthorityState,
    #[serde(default)]
    cell_transactions: CellTransactionState,
    last_generation_transition_log: Option<LogId<NodeId>>,
}

#[derive(Clone, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

mod request_map_serde {
    use super::RequestIdentity;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S, V>(
        values: &BTreeMap<RequestIdentity, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        values.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D, V>(deserializer: D) -> Result<BTreeMap<RequestIdentity, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let entries = Vec::<(RequestIdentity, V)>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let values = entries.into_iter().collect::<BTreeMap<_, _>>();
        if values.len() != entry_count {
            return Err(D::Error::custom(
                "state-machine snapshot contains duplicate request identities",
            ));
        }
        Ok(values)
    }
}

#[derive(Debug)]
pub struct SnapshotFileError(String);

impl std::fmt::Display for SnapshotFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SnapshotFileError {}

/// Bootstrap state machine used by the storage conformance and replication gates.
#[derive(Debug)]
pub struct StateMachineStore {
    data: RwLock<StateMachineData>,
    snapshot_sequence: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_path: Option<PathBuf>,
    deduplicate_requests: bool,
    generation_faults: GenerationAuthorityFaults,
    generation_fence_faults: GenerationFenceFaults,
    publication_faults: PublicationAuthorityFaults,
    publication_fence_faults: PublicationFenceFaults,
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
        Self::new_with_authority_faults(
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            PublicationAuthorityFaults::default(),
            PublicationFenceFaults::default(),
        )
    }

    /// Create a state machine with bounded generation and publication faults.
    #[must_use]
    pub fn new_with_authority_faults(
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
        publication_faults: PublicationAuthorityFaults,
        publication_fence_faults: PublicationFenceFaults,
    ) -> Self {
        Self {
            data: RwLock::new(StateMachineData::default()),
            snapshot_sequence: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            snapshot_path: None,
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
        }
    }

    /// Open a process state machine and restore its checksummed durable
    /// snapshot when one exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot file cannot be read or does not bind
    /// exactly to the serialized applied state and membership.
    pub fn open_with_authority_faults(
        root: impl AsRef<Path>,
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
        publication_faults: PublicationAuthorityFaults,
        publication_fence_faults: PublicationFenceFaults,
    ) -> Result<Self, SnapshotFileError> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(|error| {
            SnapshotFileError(format!(
                "failed to create state-machine root {}: {error}",
                root.display()
            ))
        })?;
        let snapshot_path = root.join(SNAPSHOT_FILE);
        let restored = load_snapshot(&snapshot_path)?;
        let data = restored
            .as_ref()
            .map(decode_and_validate_snapshot)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            data: RwLock::new(data),
            snapshot_sequence: AtomicU64::new(0),
            current_snapshot: RwLock::new(restored),
            snapshot_path: Some(snapshot_path),
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
        })
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

    /// Current publication authority state at this node's applied position.
    pub async fn publication_authority(&self) -> PublicationAuthorityState {
        self.data.read().await.publication_authority.clone()
    }

    /// Exact materialized transaction state at this node's applied position.
    pub async fn cell_snapshots(&self) -> Vec<CellStateSnapshot> {
        self.data.read().await.cell_transactions.snapshots()
    }

    /// Return one exact committed-envelope suffix from the applied authority state.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown domain, invalid bounds, stale generation,
    /// or an incomplete target suffix.
    pub async fn committed_envelope_feed(
        &self,
        request: &CellCommittedEnvelopeRequest,
    ) -> Result<CellCommittedEnvelopeFeed, String> {
        let state = self.data.read().await;
        let authority_position = state
            .last_applied_log
            .map(RecoveryLogPosition::from_log_id)
            .ok_or_else(|| "transaction authority has no applied position".to_owned())?;
        state
            .cell_transactions
            .committed_envelope_feed(request, authority_position)
    }

    /// Exact applied position of the most recent generation transition.
    pub async fn generation_transition_position(&self) -> Option<RecoveryLogPosition> {
        self.data
            .read()
            .await
            .last_generation_transition_log
            .map(RecoveryLogPosition::from_log_id)
    }

    /// Exact applied position of the current voter-set transition.
    pub async fn membership_position(&self) -> Option<RecoveryLogPosition> {
        self.data
            .read()
            .await
            .last_membership
            .log_id()
            .map(RecoveryLogPosition::from_log_id)
    }

    /// Exact applied log position at this state-machine replica.
    pub async fn last_applied_position(&self) -> Option<RecoveryLogPosition> {
        self.data
            .read()
            .await
            .last_applied_log
            .map(RecoveryLogPosition::from_log_id)
    }

    /// Voter identities in the latest applied membership entry.
    pub async fn membership_voters(&self) -> BTreeSet<NodeId> {
        self.data.read().await.last_membership.voter_ids().collect()
    }

    /// All voter and learner identities in the latest applied membership.
    pub async fn membership_nodes(&self) -> BTreeSet<NodeId> {
        self.data
            .read()
            .await
            .last_membership
            .nodes()
            .map(|(node_id, _)| *node_id)
            .collect()
    }

    /// Applied log index bound into the currently durable or in-memory
    /// snapshot.
    pub async fn snapshot_log_index(&self) -> Option<u64> {
        self.current_snapshot
            .read()
            .await
            .as_ref()
            .and_then(|snapshot| snapshot.meta.last_log_id)
            .map(|log_id| log_id.index)
    }

    /// Exact applied position bound into the current durable or in-memory snapshot.
    pub async fn snapshot_log_position(&self) -> Option<RecoveryLogPosition> {
        self.current_snapshot
            .read()
            .await
            .as_ref()
            .and_then(|snapshot| snapshot.meta.last_log_id)
            .map(RecoveryLogPosition::from_log_id)
    }

    fn persist_snapshot(&self, snapshot: &StoredSnapshot) -> Result<(), SnapshotFileError> {
        let Some(path) = &self.snapshot_path else {
            return Ok(());
        };
        persist_snapshot(path, snapshot)
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
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        self.persist_snapshot(&stored)
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        *self.current_snapshot.write().await = Some(stored);
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
                publication: None,
                cell_transaction: None,
                cell_staged_transaction: None,
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
                            if status == crate::GenerationCommandStatus::Accepted {
                                state.last_generation_transition_log = Some(entry.log_id);
                            }
                            response.identity = Some(command.identity);
                            response.generation = Some(GenerationApplyResponse {
                                status,
                                state: state.generation_authority.clone(),
                                applied_log_index: entry.log_id.index,
                                applied_log_position: RecoveryLogPosition::from_log_id(
                                    entry.log_id,
                                ),
                            });
                            state
                                .request_fingerprints
                                .insert(command.identity, fingerprint);
                            state
                                .durable_outcomes
                                .insert(command.identity, response.clone());
                        }
                    } else if let Some(command) = PublicationCommand::decode(&payload)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                    {
                        let fingerprint = request_fingerprint(&payload);
                        let recovered = self
                            .deduplicate_requests
                            .then(|| state.durable_outcomes.get(&command.identity).cloned())
                            .flatten();
                        if let Some(recovered) = recovered {
                            if state.request_fingerprints.get(&command.identity)
                                == Some(&fingerprint)
                            {
                                response = recovered;
                            } else {
                                response.identity = Some(command.identity);
                                response.error = Some(ApplyError::ConflictingRequestIdentity);
                            }
                        } else {
                            let authorized = self.publication_fence_faults.bypass_generation_fence
                                || state.generation_authority.authorizes(
                                    command.credential.generation,
                                    &command.credential.transaction_system_id,
                                );
                            let log_position = RecoveryLogPosition::from_log_id(entry.log_id);
                            let (status, outcome) = if authorized {
                                let context_generation = if self
                                    .publication_fence_faults
                                    .prepare_as_previous_generation
                                    && matches!(
                                        &command.action,
                                        crate::PublicationAction::Prepare { .. }
                                    ) {
                                    command.credential.generation.saturating_sub(1)
                                } else {
                                    command.credential.generation
                                };
                                let transition = state.publication_authority.apply(
                                    &command.action,
                                    PublicationAuthorityContext {
                                        generation: context_generation,
                                        position: PublicationAuthorityPosition {
                                            term: log_position.term,
                                            index: log_position.index,
                                        },
                                    },
                                    self.publication_faults,
                                );
                                (transition.status, transition.outcome)
                            } else {
                                (PublicationCommandStatus::GenerationFenced, None)
                            };
                            response.identity = Some(command.identity);
                            response.publication = Some(PublicationApplyResponse {
                                status,
                                outcome,
                                state: state.publication_authority.clone(),
                                applied_log_position: log_position,
                            });
                            if self.deduplicate_requests {
                                state
                                    .request_fingerprints
                                    .insert(command.identity, fingerprint);
                                state
                                    .durable_outcomes
                                    .insert(command.identity, response.clone());
                            }
                        }
                    } else if let Some(command) = CellStagedTransactionCommand::decode(&payload)
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
                        let recovered = self
                            .deduplicate_requests
                            .then(|| state.durable_outcomes.get(&command.identity).cloned())
                            .flatten();
                        if let Some(recovered) = recovered {
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
                                let generation_authority = state.generation_authority.clone();
                                response.cell_staged_transaction =
                                    Some(state.cell_transactions.apply_staged(
                                        &command,
                                        entry.log_id.index,
                                        &generation_authority,
                                        self.generation_fence_faults,
                                    ));
                            }
                            state
                                .request_fingerprints
                                .insert(command.identity, fingerprint);
                            state
                                .durable_outcomes
                                .insert(command.identity, response.clone());
                        }
                    } else if let Some(command) = CellTransactionCommand::decode(&payload)
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
                        let recovered = self
                            .deduplicate_requests
                            .then(|| state.durable_outcomes.get(&command.identity).cloned())
                            .flatten();
                        if let Some(recovered) = recovered {
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
                                response.cell_transaction = Some(state.cell_transactions.apply(
                                    &command,
                                    entry.log_id.index,
                                    self.generation_fence_faults,
                                ));
                            }
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
                    } else if payload.starts_with(b"OKV") {
                        response.error = Some(ApplyError::UnknownCommandVersion);
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
        validate_snapshot_state(&stored, &decoded).map_err(|error| {
            StorageIOError::read_snapshot(Some(stored.meta.signature()), &error)
        })?;
        self.persist_snapshot(&stored)
            .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
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

fn persist_snapshot(path: &Path, snapshot: &StoredSnapshot) -> Result<(), SnapshotFileError> {
    let bytes = encode_durable_snapshot(snapshot)?;
    let parent = path.parent().ok_or_else(|| {
        SnapshotFileError(format!(
            "snapshot path {} has no parent directory",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        SnapshotFileError(format!(
            "failed to create snapshot directory {}: {error}",
            parent.display()
        ))
    })?;
    let sequence = std::process::id();
    let temporary = parent.join(format!("{SNAPSHOT_FILE}.tmp-{sequence}"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            SnapshotFileError(format!(
                "failed to create snapshot temporary file {}: {error}",
                temporary.display()
            ))
        })?;
    file.write_all(&bytes).map_err(|error| {
        SnapshotFileError(format!(
            "failed to write snapshot temporary file {}: {error}",
            temporary.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        SnapshotFileError(format!(
            "failed to synchronize snapshot temporary file {}: {error}",
            temporary.display()
        ))
    })?;
    fs::rename(&temporary, path).map_err(|error| {
        SnapshotFileError(format!(
            "failed to publish snapshot {}: {error}",
            path.display()
        ))
    })?;
    OpenOptions::new()
        .read(true)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            SnapshotFileError(format!(
                "failed to synchronize snapshot directory {}: {error}",
                parent.display()
            ))
        })
}

fn encode_durable_snapshot(snapshot: &StoredSnapshot) -> Result<Vec<u8>, SnapshotFileError> {
    let meta = serde_json::to_vec(&snapshot.meta).map_err(|error| {
        SnapshotFileError(format!("failed to encode snapshot metadata: {error}"))
    })?;
    let meta_length = u32::try_from(meta.len())
        .map_err(|_| SnapshotFileError("snapshot metadata exceeds u32 length".to_owned()))?;
    let data_length = u64::try_from(snapshot.data.len())
        .map_err(|_| SnapshotFileError("snapshot data exceeds u64 length".to_owned()))?;
    let mut checksum = Sha256::new();
    checksum.update(SNAPSHOT_MAGIC);
    checksum.update(SNAPSHOT_FORMAT_VERSION.to_be_bytes());
    checksum.update(meta_length.to_be_bytes());
    checksum.update(data_length.to_be_bytes());
    checksum.update(&meta);
    checksum.update(&snapshot.data);
    let checksum = checksum.finalize();
    let capacity = SNAPSHOT_HEADER_BYTES
        .checked_add(meta.len())
        .and_then(|length| length.checked_add(snapshot.data.len()))
        .ok_or_else(|| SnapshotFileError("snapshot frame length overflow".to_owned()))?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(SNAPSHOT_MAGIC);
    frame.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_be_bytes());
    frame.extend_from_slice(&meta_length.to_be_bytes());
    frame.extend_from_slice(&data_length.to_be_bytes());
    frame.extend_from_slice(&checksum);
    frame.extend_from_slice(&meta);
    frame.extend_from_slice(&snapshot.data);
    Ok(frame)
}

fn load_snapshot(path: &Path) -> Result<Option<StoredSnapshot>, SnapshotFileError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(SnapshotFileError(format!(
                "failed to read snapshot {}: {error}",
                path.display()
            )))
        }
    };
    let snapshot = decode_durable_snapshot(&bytes).map_err(|error| {
        SnapshotFileError(format!(
            "failed to decode durable snapshot {}: {error}",
            path.display()
        ))
    })?;
    decode_and_validate_snapshot(&snapshot)?;
    Ok(Some(snapshot))
}

fn decode_durable_snapshot(bytes: &[u8]) -> Result<StoredSnapshot, SnapshotFileError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES {
        return Err(SnapshotFileError("snapshot frame is truncated".to_owned()));
    }
    if &bytes[..4] != SNAPSHOT_MAGIC {
        return Err(SnapshotFileError("snapshot magic is invalid".to_owned()));
    }
    let format_version = u16::from_be_bytes(
        bytes[4..6]
            .try_into()
            .expect("snapshot version slice has exact length"),
    );
    if format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotFileError(format!(
            "unsupported snapshot format {format_version}"
        )));
    }
    let meta_length = u32::from_be_bytes(
        bytes[6..10]
            .try_into()
            .expect("snapshot metadata length slice has exact length"),
    ) as usize;
    let data_length = usize::try_from(u64::from_be_bytes(
        bytes[10..18]
            .try_into()
            .expect("snapshot data length slice has exact length"),
    ))
    .map_err(|_| SnapshotFileError("snapshot data length exceeds usize".to_owned()))?;
    let expected_length = SNAPSHOT_HEADER_BYTES
        .checked_add(meta_length)
        .and_then(|length| length.checked_add(data_length))
        .ok_or_else(|| SnapshotFileError("snapshot frame length overflow".to_owned()))?;
    if bytes.len() != expected_length {
        return Err(SnapshotFileError(format!(
            "snapshot frame length mismatch: expected {expected_length}, got {}",
            bytes.len()
        )));
    }
    let mut checksum = Sha256::new();
    checksum.update(&bytes[..18]);
    checksum.update(&bytes[SNAPSHOT_HEADER_BYTES..]);
    let actual_checksum = checksum.finalize();
    if actual_checksum.as_slice() != &bytes[18..SNAPSHOT_HEADER_BYTES] {
        return Err(SnapshotFileError(
            "snapshot frame checksum mismatch".to_owned(),
        ));
    }
    let meta_end = SNAPSHOT_HEADER_BYTES + meta_length;
    let meta =
        serde_json::from_slice(&bytes[SNAPSHOT_HEADER_BYTES..meta_end]).map_err(|error| {
            SnapshotFileError(format!("failed to decode snapshot metadata: {error}"))
        })?;
    Ok(StoredSnapshot {
        meta,
        data: bytes[meta_end..].to_vec(),
    })
}

fn decode_and_validate_snapshot(
    snapshot: &StoredSnapshot,
) -> Result<StateMachineData, SnapshotFileError> {
    let state = serde_json::from_slice::<StateMachineData>(&snapshot.data).map_err(|error| {
        SnapshotFileError(format!("failed to decode state-machine snapshot: {error}"))
    })?;
    validate_snapshot_state(snapshot, &state)?;
    Ok(state)
}

fn validate_snapshot_state(
    snapshot: &StoredSnapshot,
    state: &StateMachineData,
) -> Result<(), SnapshotFileError> {
    if snapshot.meta.last_log_id != state.last_applied_log {
        return Err(SnapshotFileError(
            "snapshot metadata does not match the applied log position".to_owned(),
        ));
    }
    if snapshot.meta.last_membership != state.last_membership {
        return Err(SnapshotFileError(
            "snapshot metadata does not match state-machine membership".to_owned(),
        ));
    }
    Ok(())
}

fn request_fingerprint(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GenerationAction, GenerationCommandStatus, PublicationAction, PublicationIntent,
        PublicationObjectKind, PublicationObjectReference, SnapshotClosure,
    };
    use openraft::CommittedLeaderId;
    use std::collections::{BTreeMap, BTreeSet};

    static SNAPSHOT_TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct SnapshotTestRoot(PathBuf);

    impl SnapshotTestRoot {
        fn new() -> Self {
            let sequence = SNAPSHOT_TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "okv-state-machine-snapshot-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for SnapshotTestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    async fn active_store(deduplicate_requests: bool) -> Arc<StateMachineStore> {
        let store = Arc::new(StateMachineStore::new_with_authority_faults(
            deduplicate_requests,
            GenerationAuthorityFaults::default(),
            GenerationFenceFaults::default(),
            PublicationAuthorityFaults::default(),
            PublicationFenceFaults::default(),
        ));
        let status = store.data.write().await.generation_authority.apply(
            &GenerationAction::Bootstrap {
                cell_id: 1,
                generation: 7,
                transaction_system_id: "txn-7".to_owned(),
                transaction_system_members: BTreeMap::from([(1, vec![1; 32])]),
                transaction_system_incarnations: BTreeMap::from([(1, [1; 16])]),
                wal_root: "wal-7".to_owned(),
                control_root_version: 1,
            },
            GenerationAuthorityFaults::default(),
        );
        assert_eq!(GenerationCommandStatus::Accepted, status);
        store
    }

    fn publication_command(identity: RequestIdentity) -> PublicationCommand {
        let manifest = PublicationObjectReference {
            kind: PublicationObjectKind::Manifest,
            key: "objects/manifest".to_owned(),
            length: 10,
            sha256: "a".repeat(64),
        };
        PublicationCommand {
            identity,
            credential: GenerationCredential {
                generation: 7,
                transaction_system_id: "txn-7".to_owned(),
            },
            action: PublicationAction::Prepare {
                publication_id: "publication-1".to_owned(),
                intent: PublicationIntent {
                    object_keys: BTreeSet::from([manifest.key.clone(), "objects/data".to_owned()]),
                    manifest,
                    destination_root: "range-1".to_owned(),
                    expected_prior_root: None,
                },
            },
        }
    }

    fn normal_entry(index: u64, payload: Vec<u8>) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), index),
            payload: EntryPayload::Normal(payload),
        }
    }

    #[tokio::test]
    async fn publication_apply_is_fenced_deduplicated_and_fail_closed() {
        let identity = RequestIdentity {
            client_id: 41,
            request_id: 1,
        };
        let command = publication_command(identity);
        let payload = command.encode().unwrap();
        let mut store = active_store(true).await;
        let first = store
            .apply([normal_entry(2, payload.clone())])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            Some(PublicationCommandStatus::Accepted),
            first.publication.as_ref().map(|response| response.status)
        );

        let retry = store
            .apply([normal_entry(3, payload)])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(first, retry);

        let mut conflicting = command.clone();
        conflicting.action = PublicationAction::Unpin {
            pin_id: "other".to_owned(),
            expected: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "objects/other".to_owned(),
                length: 10,
                sha256: "b".repeat(64),
            },
        };
        let conflict = store
            .apply([normal_entry(4, conflicting.encode().unwrap())])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(Some(ApplyError::ConflictingRequestIdentity), conflict.error);

        let mut stale = publication_command(RequestIdentity {
            client_id: 41,
            request_id: 2,
        });
        stale.credential.generation = 6;
        let fenced = store
            .apply([normal_entry(5, stale.encode().unwrap())])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            Some(PublicationCommandStatus::GenerationFenced),
            fenced.publication.map(|response| response.status)
        );

        let unknown = store
            .apply([normal_entry(6, b"OKVP9{}".to_vec())])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(Some(ApplyError::UnknownCommandVersion), unknown.error);
        assert_eq!(1, store.publication_authority().await.intents.len());
    }

    #[tokio::test]
    async fn disabled_dedup_reapplies_the_same_publication_request() {
        let identity = RequestIdentity {
            client_id: 43,
            request_id: 1,
        };
        let payload = publication_command(identity).encode().unwrap();
        let mut store = active_store(false).await;
        let first = store
            .apply([normal_entry(2, payload.clone())])
            .await
            .unwrap()
            .remove(0);
        let second = store
            .apply([normal_entry(3, payload)])
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            Some(PublicationCommandStatus::Accepted),
            first.publication.map(|response| response.status)
        );
        assert_eq!(
            Some(PublicationCommandStatus::PublicationExists),
            second.publication.map(|response| response.status)
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn durable_snapshot_restores_outcomes_and_rejects_corruption() {
        let root = SnapshotTestRoot::new();
        let identity = RequestIdentity {
            client_id: 47,
            request_id: 9,
        };
        let log_id = LogId::new(CommittedLeaderId::new(4, 2), 7);
        let outcome = ApplyResponse {
            applied_log_index: 7,
            identity: Some(identity),
            error: Some(ApplyError::UnknownCommandVersion),
            ..ApplyResponse::default()
        };
        let store = Arc::new(
            StateMachineStore::open_with_authority_faults(
                &root.0,
                true,
                GenerationAuthorityFaults::default(),
                GenerationFenceFaults::default(),
                PublicationAuthorityFaults::default(),
                PublicationFenceFaults::default(),
            )
            .unwrap(),
        );
        let expected_publication_authority = {
            let mut state = store.data.write().await;
            state.last_applied_log = Some(log_id);
            state.durable_outcomes.insert(identity, outcome.clone());
            state.request_fingerprints.insert(identity, [7; 32]);
            let manifest = PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "objects/snapshot-manifest".to_owned(),
                length: 10,
                sha256: "c".repeat(64),
            };
            let authority_context = PublicationAuthorityContext {
                generation: 7,
                position: PublicationAuthorityPosition { term: 4, index: 7 },
            };
            state.publication_authority.apply(
                &PublicationAction::SetRetentionWindow {
                    expected_policy_epoch: 0,
                    retention_window: 64,
                },
                authority_context,
                PublicationAuthorityFaults::default(),
            );
            state.publication_authority.apply(
                &PublicationAction::ObserveCommittedFrontier {
                    committed_frontier: 128,
                },
                authority_context,
                PublicationAuthorityFaults::default(),
            );
            state.publication_authority.apply(
                &PublicationAction::AcquireLease {
                    lease_id: "snapshot-lease".to_owned(),
                    tenant_id: "tenant-1".to_owned(),
                    snapshot_version: 96,
                    owner: "query-1".to_owned(),
                    purpose: "restore-test".to_owned(),
                    deadline_tick: 20,
                    closure: SnapshotClosure {
                        manifest: manifest.clone(),
                        object_keys: BTreeSet::from([
                            manifest.key,
                            "objects/snapshot-data".to_owned(),
                        ]),
                    },
                },
                authority_context,
                PublicationAuthorityFaults::default(),
            );
            state.publication_authority.clone()
        };
        let mut builder = store.clone();
        builder.build_snapshot().await.unwrap();
        assert_eq!(Some(7), store.snapshot_log_index().await);
        drop(builder);
        drop(store);

        let restored = StateMachineStore::open_with_authority_faults(
            &root.0,
            true,
            GenerationAuthorityFaults::default(),
            GenerationFenceFaults::default(),
            PublicationAuthorityFaults::default(),
            PublicationFenceFaults::default(),
        )
        .unwrap();
        assert_eq!(Some(7), restored.snapshot_log_index().await);
        assert_eq!(Some(outcome), restored.durable_outcome(identity).await);
        assert_eq!(
            expected_publication_authority,
            restored.publication_authority().await
        );
        drop(restored);

        let path = root.0.join(SNAPSHOT_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes.push(b'x');
        fs::write(&path, bytes).unwrap();
        assert!(StateMachineStore::open_with_authority_faults(
            &root.0,
            true,
            GenerationAuthorityFaults::default(),
            GenerationFenceFaults::default(),
            PublicationAuthorityFaults::default(),
            PublicationFenceFaults::default(),
        )
        .is_err());
    }

    #[test]
    fn state_machine_snapshot_v1_fixture_is_dual_readable() {
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "fixture-v1".to_owned(),
            },
            data: serde_json::to_vec(&StateMachineData::default()).unwrap(),
        };
        let fixture = decode_hex(include_str!("../fixtures/state-machine-snapshot-v1.hex"));
        assert_eq!(fixture, encode_durable_snapshot(&snapshot).unwrap());
        let decoded = decode_durable_snapshot(&fixture).unwrap();
        assert_eq!("fixture-v1", decoded.meta.snapshot_id);
        decode_and_validate_snapshot(&decoded).unwrap();
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let value = value.trim().as_bytes();
        assert_eq!(value.len() % 2, 0);
        value
            .chunks_exact(2)
            .map(|digits| (nibble(digits[0]) << 4) | nibble(digits[1]))
            .collect()
    }

    const fn nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid fixture hex"),
        }
    }
}
