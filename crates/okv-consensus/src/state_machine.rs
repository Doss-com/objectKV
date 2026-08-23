use crate::{
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
    pub publication: Option<PublicationApplyResponse>,
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
    durable_outcomes: BTreeMap<RequestIdentity, ApplyResponse>,
    request_fingerprints: BTreeMap<RequestIdentity, [u8; 32]>,
    generation_authority: GenerationAuthorityState,
    #[serde(default)]
    publication_authority: PublicationAuthorityState,
    last_generation_transition_log: Option<LogId<NodeId>>,
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
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
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

    /// Current publication authority state at this node's applied position.
    pub async fn publication_authority(&self) -> PublicationAuthorityState {
        self.data.read().await.publication_authority.clone()
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
                publication: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GenerationAction, GenerationCommandStatus, PublicationAction, PublicationIntent,
        PublicationObjectKind, PublicationObjectReference,
    };
    use openraft::CommittedLeaderId;
    use std::collections::{BTreeMap, BTreeSet};

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
}
