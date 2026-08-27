use crate::{
    verify_object_frontier_certificate, GenerationApplyResponse, GenerationAuthorityFaults,
    GenerationAuthorityState, GenerationCommand, GenerationCredential, GenerationFenceFaults,
    GenerationPhase, NodeId, ObjectFrontierAdvance, ObjectFrontierApplyResponse,
    ObjectFrontierLogPosition, ObjectFrontierRecord, PublicationApplyResponse,
    PublicationAuthorityContext, PublicationAuthorityFaults, PublicationAuthorityPosition,
    PublicationAuthorityState, PublicationAuthorization, PublicationCommand,
    PublicationCommandStatus, PublicationFenceFaults, RecoveryLogPosition,
    TransactionFrontierAdvance, TransactionFrontierApplyResponse, TransactionRetryFloor,
    TypeConfig,
};
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;
use okv_transaction::{
    RetainedTransactionRecord, TransactionApplyResponse, TransactionAuthority,
    TransactionAuthorityFaults, TransactionAuthorityView, TransactionCommand, TransactionStatus,
};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

const COMMAND_MAGIC_V1: &[u8] = b"OKVQ1";
const COMMAND_MAGIC_V2: &[u8] = b"OKVQ2";
const TRANSACTION_BATCH_MAGIC_V1: &[u8] = b"OKVB1";
const TRANSACTION_BATCH_MAGIC_V2: &[u8] = b"OKVB2";
const MAX_TRANSACTION_BATCH_ITEMS: usize = 32;
const SNAPSHOT_MAGIC: &[u8; 4] = b"OKVS";
const SNAPSHOT_FORMAT_VERSION: u16 = 1;
const SNAPSHOT_HEADER_BYTES: usize = 4 + 2 + 4 + 8 + 8;
const SNAPSHOT_CHECKSUM_BYTES: usize = 32;
const SNAPSHOT_FILE_NAME: &str = "state-machine.snapshot";
const SNAPSHOT_NEXT_FILE_NAME: &str = "state-machine.snapshot.next";
const MAX_SNAPSHOT_META_BYTES: usize = 16 * 1024 * 1024;
const MAX_SNAPSHOT_DATA_BYTES: usize = 4 * 1024 * 1024 * 1024;

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

#[derive(Deserialize, Serialize)]
struct WireClientCommandV2 {
    identity: RequestIdentity,
    credential: Option<GenerationCredential>,
    payload: String,
}

impl From<&ClientCommand> for WireClientCommandV2 {
    fn from(command: &ClientCommand) -> Self {
        Self {
            identity: command.identity,
            credential: command.credential.clone(),
            payload: STANDARD_NO_PAD.encode(&command.payload),
        }
    }
}

impl TryFrom<WireClientCommandV2> for ClientCommand {
    type Error = serde_json::Error;

    fn try_from(command: WireClientCommandV2) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: command.identity,
            credential: command.credential,
            payload: decode_wire_payload(&command.payload)?,
        })
    }
}

fn decode_wire_payload(encoded: &str) -> Result<Vec<u8>, serde_json::Error> {
    STANDARD_NO_PAD.decode(encoded).map_err(|error| {
        serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid base64 application payload: {error}"),
        ))
    })
}

impl ClientCommand {
    /// Encode the versioned command into objectKV-owned application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = COMMAND_MAGIC_V2.to_vec();
        encoded.extend(serde_json::to_vec(&WireClientCommandV2::from(self))?);
        Ok(encoded)
    }

    fn encode_v1_for_compatibility(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = COMMAND_MAGIC_V1.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        if let Some(encoded) = bytes.strip_prefix(COMMAND_MAGIC_V2) {
            return serde_json::from_slice::<WireClientCommandV2>(encoded)
                .and_then(TryInto::try_into)
                .map(Some);
        }
        bytes
            .strip_prefix(COMMAND_MAGIC_V1)
            .map(serde_json::from_slice)
            .transpose()
    }
}

#[derive(Clone, Copy)]
struct TransactionRequestFingerprints {
    canonical: [u8; 32],
    legacy_v1: [u8; 32],
}

impl TransactionRequestFingerprints {
    fn matches(self, stored: Option<&[u8; 32]>) -> bool {
        stored.is_some_and(|stored| *stored == self.canonical || *stored == self.legacy_v1)
    }
}

/// One bounded set of independently retryable transaction commands carried by
/// a single Raft application entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatchCommand {
    pub commands: Vec<ClientCommand>,
}

#[derive(Deserialize, Serialize)]
struct WireTransactionBatchCommandV2 {
    commands: Vec<WireClientCommandV2>,
}

impl TransactionBatchCommand {
    /// Encode the first objectKV transaction-batch wire format.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the batch cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = TRANSACTION_BATCH_MAGIC_V2.to_vec();
        encoded.extend(serde_json::to_vec(&WireTransactionBatchCommandV2 {
            commands: self.commands.iter().map(Into::into).collect(),
        })?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        if let Some(encoded) = bytes.strip_prefix(TRANSACTION_BATCH_MAGIC_V2) {
            let batch = serde_json::from_slice::<WireTransactionBatchCommandV2>(encoded)?;
            return batch
                .commands
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map(|commands| Some(Self { commands }));
        }
        bytes
            .strip_prefix(TRANSACTION_BATCH_MAGIC_V1)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// One independently retained result inside a committed transaction batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatchItemApplyResponse {
    pub identity: RequestIdentity,
    pub error: Option<ApplyError>,
    pub transaction: Option<TransactionApplyResponse>,
}

/// Ordered results returned for one committed transaction batch entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatchApplyResponse {
    pub applied_log_index: u64,
    pub items: Vec<TransactionBatchItemApplyResponse>,
}

/// Result returned after one committed Raft entry is applied.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyResponse {
    pub applied_log_index: u64,
    pub identity: Option<RequestIdentity>,
    pub error: Option<ApplyError>,
    pub generation: Option<GenerationApplyResponse>,
    pub publication: Option<PublicationApplyResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionApplyResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_batch: Option<TransactionBatchApplyResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_frontier: Option<TransactionFrontierApplyResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_frontier: Option<ObjectFrontierApplyResponse>,
}

/// Application-level rejection reconstructed identically after replay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyError {
    ConflictingRequestIdentity,
    GenerationFenced,
    InvalidTransactionFrontier,
    InvalidTransactionBatch,
    RetryIdentityExpired,
    TransactionFrontierExpired,
    TransactionFrontierSequenceGap,
    InvalidObjectFrontier,
    ObjectFrontierExpired,
    UnknownCommandVersion,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct TransactionRetryState {
    #[serde(
        default,
        serialize_with = "serialize_request_identity_map",
        deserialize_with = "deserialize_request_identity_map"
    )]
    outcomes: BTreeMap<RequestIdentity, ApplyResponse>,
    #[serde(
        default,
        serialize_with = "serialize_request_identity_map",
        deserialize_with = "deserialize_request_identity_map"
    )]
    fingerprints: BTreeMap<RequestIdentity, [u8; 32]>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    client_floors: BTreeMap<u64, u64>,
}

impl TransactionRetryState {
    fn outcome(&self, identity: RequestIdentity) -> Option<ApplyResponse> {
        self.outcomes.get(&identity).cloned()
    }

    fn is_expired(&self, identity: RequestIdentity) -> bool {
        self.client_floors
            .get(&identity.client_id)
            .is_some_and(|floor| identity.request_id <= *floor)
    }

    fn insert(
        &mut self,
        identity: RequestIdentity,
        fingerprint: [u8; 32],
        response: ApplyResponse,
    ) {
        self.fingerprints.insert(identity, fingerprint);
        self.outcomes.insert(identity, response);
    }

    fn validate_floors(&self, floors: &[TransactionRetryFloor]) -> bool {
        floors
            .windows(2)
            .all(|window| window[0].client_id < window[1].client_id)
            && floors.iter().all(|floor| {
                self.client_floors
                    .get(&floor.client_id)
                    .is_none_or(|current| floor.through_request_id >= *current)
            })
    }

    fn advance_floors(&mut self, floors: &[TransactionRetryFloor]) -> u64 {
        let before = self.outcomes.len();
        for floor in floors {
            self.client_floors
                .insert(floor.client_id, floor.through_request_id);
        }
        let client_floors = &self.client_floors;
        self.outcomes.retain(|identity, _| {
            client_floors
                .get(&identity.client_id)
                .is_none_or(|floor| identity.request_id > *floor)
        });
        self.fingerprints.retain(|identity, _| {
            client_floors
                .get(&identity.client_id)
                .is_none_or(|floor| identity.request_id > *floor)
        });
        u64::try_from(before.saturating_sub(self.outcomes.len())).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct TransactionFrontierState {
    sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_fingerprint: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_response: Option<ApplyResponse>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct AppliedObjectFrontierState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frontier: Option<ObjectFrontierRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied_log_position: Option<ObjectFrontierLogPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_fingerprint: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_response: Option<ApplyResponse>,
}

impl AppliedObjectFrontierState {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    applied_payloads: Vec<Vec<u8>>,
    #[serde(
        serialize_with = "serialize_request_identity_map",
        deserialize_with = "deserialize_request_identity_map"
    )]
    durable_outcomes: BTreeMap<RequestIdentity, ApplyResponse>,
    #[serde(
        serialize_with = "serialize_request_identity_map",
        deserialize_with = "deserialize_request_identity_map"
    )]
    request_fingerprints: BTreeMap<RequestIdentity, [u8; 32]>,
    generation_authority: GenerationAuthorityState,
    #[serde(default)]
    publication_authority: PublicationAuthorityState,
    #[serde(default)]
    transaction_authority: TransactionAuthority,
    #[serde(default)]
    transaction_retry_state: TransactionRetryState,
    #[serde(default)]
    transaction_frontier_state: TransactionFrontierState,
    #[serde(default, skip_serializing_if = "AppliedObjectFrontierState::is_empty")]
    applied_object_frontier: AppliedObjectFrontierState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retained_transactions: Vec<RetainedTransactionRecord>,
    #[serde(default, skip_serializing_if = "is_zero")]
    transaction_retention_floor: u64,
    last_generation_transition_log: Option<LogId<NodeId>>,
}

fn serialize_request_identity_map<S, V>(
    values: &BTreeMap<RequestIdentity, V>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    let mut map = serializer.serialize_map(Some(values.len()))?;
    for (identity, value) in values {
        map.serialize_entry(
            &format!("{}:{}", identity.client_id, identity.request_id),
            value,
        )?;
    }
    map.end()
}

fn deserialize_request_identity_map<'de, D, V>(
    deserializer: D,
) -> Result<BTreeMap<RequestIdentity, V>, D::Error>
where
    D: Deserializer<'de>,
    V: Deserialize<'de>,
{
    let encoded = BTreeMap::<String, V>::deserialize(deserializer)?;
    encoded
        .into_iter()
        .map(|(key, value)| {
            let (client_id, request_id) = key.split_once(':').ok_or_else(|| {
                serde::de::Error::custom(format!("invalid request identity key: {key}"))
            })?;
            let client_id = client_id.parse().map_err(|_| {
                serde::de::Error::custom(format!("invalid request identity client id: {key}"))
            })?;
            let request_id = request_id.parse().map_err(|_| {
                serde::de::Error::custom(format!("invalid request identity request id: {key}"))
            })?;
            Ok((
                RequestIdentity {
                    client_id,
                    request_id,
                },
                value,
            ))
        })
        .collect()
}

impl StateMachineData {
    fn apply_transaction(
        &mut self,
        applied_log_index: u64,
        batch_order: u16,
        command: &TransactionCommand,
        faults: TransactionAuthorityFaults,
    ) -> TransactionApplyResponse {
        let response = if batch_order == 0 {
            self.transaction_authority
                .apply(applied_log_index, command, faults)
        } else {
            self.transaction_authority.apply_in_batch(
                applied_log_index,
                batch_order,
                command,
                faults,
            )
        };
        if response.status
            == (TransactionStatus::Committed {
                commit_version: applied_log_index,
            })
        {
            debug_assert!(self.retained_transactions.last().is_none_or(|record| {
                (record.commit_version, record.batch_order) < (applied_log_index, batch_order)
            }));
            self.retained_transactions.push(RetainedTransactionRecord {
                commit_version: applied_log_index,
                batch_order,
                command: command.clone(),
            });
        }
        response
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transaction_request(
        &mut self,
        applied_log_index: u64,
        identity: RequestIdentity,
        fingerprints: TransactionRequestFingerprints,
        command: &TransactionCommand,
        batch_order: u16,
        deduplicate_requests: bool,
        generation_fenced: bool,
        faults: TransactionAuthorityFaults,
    ) -> ApplyResponse {
        if deduplicate_requests {
            if let Some(recovered) = self.transaction_retry_state.outcome(identity) {
                return if fingerprints
                    .matches(self.transaction_retry_state.fingerprints.get(&identity))
                {
                    recovered
                } else {
                    application_error(
                        applied_log_index,
                        identity,
                        ApplyError::ConflictingRequestIdentity,
                    )
                };
            }
            // Snapshots written before RFC-0029 retained transaction outcomes
            // in the generic control-plane maps. Keep those retries exact after
            // an upgrade without adding new transaction records there.
            if let Some(recovered) = self.durable_outcomes.get(&identity).cloned() {
                return if fingerprints.matches(self.request_fingerprints.get(&identity)) {
                    recovered
                } else {
                    application_error(
                        applied_log_index,
                        identity,
                        ApplyError::ConflictingRequestIdentity,
                    )
                };
            }
        }
        if self.transaction_retry_state.is_expired(identity) {
            return application_error(
                applied_log_index,
                identity,
                ApplyError::RetryIdentityExpired,
            );
        }

        let mut response = ApplyResponse {
            applied_log_index,
            identity: Some(identity),
            ..ApplyResponse::default()
        };
        if generation_fenced {
            response.error = Some(ApplyError::GenerationFenced);
        } else {
            response.transaction =
                Some(self.apply_transaction(applied_log_index, batch_order, command, faults));
        }
        self.transaction_retry_state
            .insert(identity, fingerprints.canonical, response.clone());
        response
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transaction_batch(
        &mut self,
        applied_log_index: u64,
        batch: &TransactionBatchCommand,
        generation_authority_initialized: bool,
        generation_authority: &GenerationAuthorityState,
        generation_fence_faults: GenerationFenceFaults,
        deduplicate_requests: bool,
        transaction_faults: TransactionAuthorityFaults,
    ) -> ApplyResponse {
        let identities = batch
            .commands
            .iter()
            .map(|command| command.identity)
            .collect::<std::collections::BTreeSet<_>>();
        let decoded = batch
            .commands
            .iter()
            .map(|command| TransactionCommand::decode(&command.payload))
            .collect::<Result<Vec<_>, _>>();
        let valid = !batch.commands.is_empty()
            && batch.commands.len() <= MAX_TRANSACTION_BATCH_ITEMS
            && identities.len() == batch.commands.len()
            && decoded
                .as_ref()
                .is_ok_and(|commands| commands.iter().all(Option::is_some));
        if !valid {
            return ApplyResponse {
                applied_log_index,
                error: Some(ApplyError::InvalidTransactionBatch),
                ..ApplyResponse::default()
            };
        }

        let transactions = decoded
            .expect("validated transaction batch decode")
            .into_iter()
            .map(|command| command.expect("validated transaction payload"));
        let mut items = Vec::with_capacity(batch.commands.len());
        for (batch_order, (command, transaction)) in
            batch.commands.iter().zip(transactions).enumerate()
        {
            let fingerprints = transaction_request_fingerprints(command, &transaction);
            let generation_authorized = command.credential.as_ref().is_some_and(|credential| {
                generation_authority
                    .authorizes(credential.generation, &credential.transaction_system_id)
                    || (generation_fence_faults.accept_apply_during_recovery
                        && generation_authority.recovery_id.is_some_and(|recovery_id| {
                            generation_authority.authorizes_recovery(
                                credential.generation,
                                recovery_id,
                                &credential.transaction_system_id,
                            )
                        }))
            });
            let generation_fenced = generation_authority_initialized
                && !generation_fence_faults.bypass_apply_fence
                && !generation_authorized;
            let batch_order = u16::try_from(batch_order)
                .expect("transaction batch bound fits in a 16-bit batch order");
            let item = self.apply_transaction_request(
                applied_log_index,
                command.identity,
                fingerprints,
                &transaction,
                batch_order,
                deduplicate_requests,
                generation_fenced,
                transaction_faults,
            );
            items.push(TransactionBatchItemApplyResponse {
                identity: command.identity,
                error: item.error,
                transaction: item.transaction,
            });
        }
        ApplyResponse {
            applied_log_index,
            transaction_batch: Some(TransactionBatchApplyResponse {
                applied_log_index,
                items,
            }),
            ..ApplyResponse::default()
        }
    }

    fn apply_transaction_frontier(
        &mut self,
        applied_log_index: u64,
        identity: RequestIdentity,
        fingerprint: [u8; 32],
        advance: &TransactionFrontierAdvance,
        generation_fenced: bool,
    ) -> ApplyResponse {
        let current_sequence = self.transaction_frontier_state.sequence;
        if advance.sequence == current_sequence {
            return if self.transaction_frontier_state.last_fingerprint == Some(fingerprint) {
                self.transaction_frontier_state
                    .last_response
                    .clone()
                    .unwrap_or_else(|| {
                        application_error(
                            applied_log_index,
                            identity,
                            ApplyError::InvalidTransactionFrontier,
                        )
                    })
            } else {
                application_error(
                    applied_log_index,
                    identity,
                    ApplyError::ConflictingRequestIdentity,
                )
            };
        }
        if advance.sequence < current_sequence {
            return application_error(
                applied_log_index,
                identity,
                ApplyError::TransactionFrontierExpired,
            );
        }
        if advance.sequence != current_sequence.saturating_add(1) {
            return application_error(
                applied_log_index,
                identity,
                ApplyError::TransactionFrontierSequenceGap,
            );
        }
        if generation_fenced {
            return application_error(applied_log_index, identity, ApplyError::GenerationFenced);
        }
        if self
            .transaction_authority
            .validate_conflict_retention_floor(advance.conflict_retention_floor)
            .is_err()
            || !self
                .transaction_retry_state
                .validate_floors(&advance.retry_floors)
        {
            return application_error(
                applied_log_index,
                identity,
                ApplyError::InvalidTransactionFrontier,
            );
        }

        let Ok(pruned_conflict_versions) = self
            .transaction_authority
            .advance_conflict_retention_floor(advance.conflict_retention_floor)
        else {
            return application_error(
                applied_log_index,
                identity,
                ApplyError::InvalidTransactionFrontier,
            );
        };
        let pruned_retry_outcomes = self
            .transaction_retry_state
            .advance_floors(&advance.retry_floors);
        let frontier = TransactionFrontierApplyResponse {
            applied_log_index,
            sequence: advance.sequence,
            conflict_retention_floor: advance.conflict_retention_floor,
            retry_floors: advance.retry_floors.clone(),
            pruned_conflict_versions,
            pruned_retry_outcomes,
        };
        let response = ApplyResponse {
            applied_log_index,
            identity: Some(identity),
            transaction_frontier: Some(frontier),
            ..ApplyResponse::default()
        };
        self.transaction_frontier_state = TransactionFrontierState {
            sequence: advance.sequence,
            last_fingerprint: Some(fingerprint),
            last_response: Some(response.clone()),
        };
        response
    }

    fn apply_object_frontier(
        &mut self,
        applied_log_position: ObjectFrontierLogPosition,
        identity: RequestIdentity,
        fingerprint: [u8; 32],
        advance: &ObjectFrontierAdvance,
        credential_generation: Option<u64>,
        generation_fenced: bool,
    ) -> ApplyResponse {
        if let Some(current) = self.applied_object_frontier.frontier.as_ref() {
            if current == &advance.frontier {
                let same_identity = self
                    .applied_object_frontier
                    .last_response
                    .as_ref()
                    .and_then(|response| response.identity)
                    == Some(identity);
                return if same_identity
                    && self.applied_object_frontier.last_fingerprint == Some(fingerprint)
                {
                    self.applied_object_frontier
                        .last_response
                        .clone()
                        .unwrap_or_else(|| {
                            application_error(
                                applied_log_position.index,
                                identity,
                                ApplyError::InvalidObjectFrontier,
                            )
                        })
                } else if same_identity {
                    application_error(
                        applied_log_position.index,
                        identity,
                        ApplyError::ConflictingRequestIdentity,
                    )
                } else {
                    application_error(
                        applied_log_position.index,
                        identity,
                        ApplyError::ObjectFrontierExpired,
                    )
                };
            }
            if advance.frontier.covered_through <= current.covered_through {
                return application_error(
                    applied_log_position.index,
                    identity,
                    ApplyError::ObjectFrontierExpired,
                );
            }
        }
        let high_watermark = self.transaction_authority.current_version();
        if generation_fenced
            || !advance.frontier.is_valid()
            || credential_generation != Some(advance.frontier.owner_generation)
            || advance.frontier.covered_through <= self.transaction_retention_floor
            || advance.frontier.covered_through > high_watermark
        {
            return application_error(
                applied_log_position.index,
                identity,
                if generation_fenced {
                    ApplyError::GenerationFenced
                } else {
                    ApplyError::InvalidObjectFrontier
                },
            );
        }

        let prior_retention_floor = self.transaction_retention_floor;
        let retained_before = self.retained_transactions.len();
        self.retained_transactions
            .retain(|record| record.commit_version > advance.frontier.covered_through);
        let popped_records =
            u64::try_from(retained_before.saturating_sub(self.retained_transactions.len()))
                .unwrap_or(u64::MAX);
        self.transaction_retention_floor = advance.frontier.covered_through;
        let object_frontier = ObjectFrontierApplyResponse {
            applied_log_position,
            frontier: advance.frontier.clone(),
            prior_retention_floor,
            retention_floor: advance.frontier.covered_through,
            popped_records,
        };
        let response = ApplyResponse {
            applied_log_index: applied_log_position.index,
            identity: Some(identity),
            object_frontier: Some(object_frontier),
            ..ApplyResponse::default()
        };
        self.applied_object_frontier = AppliedObjectFrontierState {
            frontier: Some(advance.frontier.clone()),
            applied_log_position: Some(applied_log_position),
            last_fingerprint: Some(fingerprint),
            last_response: Some(response.clone()),
        };
        response
    }
}

fn application_error(
    applied_log_index: u64,
    identity: RequestIdentity,
    error: ApplyError,
) -> ApplyResponse {
    ApplyResponse {
        applied_log_index,
        identity: Some(identity),
        error: Some(error),
        ..ApplyResponse::default()
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
    sequence: u64,
}

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
    transaction_faults: TransactionAuthorityFaults,
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
        Self::new_with_transaction_faults(
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
            TransactionAuthorityFaults::default(),
        )
    }

    /// Create a state machine with bounded transaction faults for process-gate
    /// negative controls.
    #[must_use]
    pub fn new_with_transaction_faults(
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
        publication_faults: PublicationAuthorityFaults,
        publication_fence_faults: PublicationFenceFaults,
        transaction_faults: TransactionAuthorityFaults,
    ) -> Self {
        Self::from_snapshot(
            None,
            None,
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
            transaction_faults,
        )
    }

    /// Open one process state machine from its crash-safe snapshot file.
    ///
    /// The `OpenRaft` log remains responsible for replaying entries newer than
    /// the recovered snapshot. A stale pre-rename snapshot candidate is ignored
    /// only after the authoritative snapshot has decoded and validated.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot directory, frame, checksum, metadata,
    /// or encoded state is invalid.
    pub fn open_persistent_with_transaction_faults(
        root: impl AsRef<Path>,
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
        publication_faults: PublicationAuthorityFaults,
        publication_fence_faults: PublicationFenceFaults,
        transaction_faults: TransactionAuthorityFaults,
    ) -> Result<Self, String> {
        let snapshot_path = root.as_ref().join(SNAPSHOT_FILE_NAME);
        let snapshot = load_snapshot(&snapshot_path).map_err(|error| error.to_string())?;
        Ok(Self::from_snapshot(
            Some(snapshot_path),
            snapshot,
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
            transaction_faults,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn from_snapshot(
        snapshot_path: Option<PathBuf>,
        snapshot: Option<StoredSnapshot>,
        deduplicate_requests: bool,
        generation_faults: GenerationAuthorityFaults,
        generation_fence_faults: GenerationFenceFaults,
        publication_faults: PublicationAuthorityFaults,
        publication_fence_faults: PublicationFenceFaults,
        transaction_faults: TransactionAuthorityFaults,
    ) -> Self {
        let sequence = snapshot.as_ref().map_or(0, |stored| stored.sequence);
        let data = snapshot
            .as_ref()
            .map(|stored| {
                serde_json::from_slice(&stored.data)
                    .expect("validated persistent snapshot must decode twice")
            })
            .unwrap_or_default();
        Self {
            data: RwLock::new(data),
            snapshot_sequence: AtomicU64::new(sequence),
            current_snapshot: RwLock::new(snapshot),
            snapshot_path,
            deduplicate_requests,
            generation_faults,
            generation_fence_faults,
            publication_faults,
            publication_fence_faults,
            transaction_faults,
        }
    }

    /// Applied normal-entry payloads in log order.
    pub async fn applied_payloads(&self) -> Vec<Vec<u8>> {
        self.data.read().await.applied_payloads.clone()
    }

    /// Current physical bytes in the crash-safe state-machine snapshot file.
    /// In-memory test stores report zero.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured snapshot path cannot be inspected.
    pub fn physical_snapshot_bytes(&self) -> Result<u64, String> {
        self.snapshot_path.as_ref().map_or(Ok(0), |path| {
            if path.exists() {
                fs::metadata(path)
                    .map(|metadata| metadata.len())
                    .map_err(|error| error.to_string())
            } else {
                Ok(0)
            }
        })
    }

    /// Recovered response for one request identity.
    pub async fn durable_outcome(&self, identity: RequestIdentity) -> Option<ApplyResponse> {
        let state = self.data.read().await;
        state
            .transaction_retry_state
            .outcome(identity)
            .or_else(|| state.durable_outcomes.get(&identity).cloned())
    }

    /// Current coordinator authority state at this node's applied position.
    pub async fn generation_authority(&self) -> GenerationAuthorityState {
        self.data.read().await.generation_authority.clone()
    }

    /// Current publication authority state at this node's applied position.
    pub async fn publication_authority(&self) -> PublicationAuthorityState {
        self.data.read().await.publication_authority.clone()
    }

    /// Current deterministic transaction state at this node's applied
    /// position.
    pub async fn transaction_authority(&self) -> TransactionAuthorityView {
        self.data.read().await.transaction_authority.view()
    }

    /// Exact immutable frontier and data-log position physically applied by
    /// this local state machine.
    pub async fn applied_object_frontier(
        &self,
    ) -> Option<(ObjectFrontierRecord, ObjectFrontierLogPosition)> {
        let state = self.data.read().await;
        state
            .applied_object_frontier
            .frontier
            .clone()
            .zip(state.applied_object_frontier.applied_log_position)
    }

    /// Read one frozen page from the committed transaction recovery stream.
    ///
    /// The caller is responsible for executing the Raft linearizability
    /// barrier before invoking this local state-machine read.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page size, a cursor below the retention
    /// floor, a target behind the cursor, or a target above the high watermark.
    pub async fn retained_transactions(
        &self,
        request: crate::RetainedTransactionReadRequest,
    ) -> Result<crate::RetainedTransactionReadResponse, String> {
        if request.max_records == 0
            || request.max_records > crate::transaction_log::MAX_RETAINED_TRANSACTION_PAGE_RECORDS
        {
            return Err(format!(
                "retained transaction page size must be between 1 and {}",
                crate::transaction_log::MAX_RETAINED_TRANSACTION_PAGE_RECORDS
            ));
        }
        let state = self.data.read().await;
        let high_watermark = state.transaction_authority.current_version();
        if request.after_version_exclusive < state.transaction_retention_floor {
            return Err(format!(
                "retained transaction cursor {} is below retention floor {}",
                request.after_version_exclusive, state.transaction_retention_floor
            ));
        }
        let target_version = request.through_version_inclusive.unwrap_or(high_watermark);
        if target_version < request.after_version_exclusive {
            return Err("retained transaction target precedes its cursor".to_owned());
        }
        if target_version > high_watermark {
            return Err(format!(
                "retained transaction target {target_version} exceeds high watermark {high_watermark}"
            ));
        }
        let limit = usize::try_from(request.max_records).unwrap_or(usize::MAX);
        let mut records = state
            .retained_transactions
            .iter()
            .filter(|record| {
                (record.commit_version > request.after_version_exclusive
                    || (record.commit_version == request.after_version_exclusive
                        && request
                            .after_batch_order_exclusive
                            .is_some_and(|batch_order| record.batch_order > batch_order)))
                    && record.commit_version <= target_version
            })
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let complete = records.len() <= limit;
        if !complete {
            records.truncate(limit);
        }
        let (next_after_version, next_after_batch_order) = if complete {
            (target_version, None)
        } else {
            records.last().map_or(
                (
                    request.after_version_exclusive,
                    request.after_batch_order_exclusive,
                ),
                |record| (record.commit_version, Some(record.batch_order)),
            )
        };
        Ok(crate::RetainedTransactionReadResponse {
            format_version: 1,
            retention_floor: state.transaction_retention_floor,
            high_watermark,
            target_version,
            next_after_version,
            next_after_batch_order,
            complete,
            records,
        })
    }

    /// Return exact serialized state accounting and a non-mutating retained-log
    /// pop projection for the bounded-state evaluation.
    ///
    /// # Errors
    ///
    /// Returns an error when the projected floor precedes the actual floor,
    /// exceeds the transaction high watermark, or state serialization fails.
    #[doc(hidden)]
    pub async fn transaction_log_storage_stats(
        &self,
        request: crate::TransactionLogStorageStatsRequest,
    ) -> Result<crate::TransactionLogStorageStats, String> {
        let state = self.data.read().await.clone();
        let high_watermark = state.transaction_authority.current_version();
        let projected_retention_floor = request
            .projected_retention_floor
            .unwrap_or(state.transaction_retention_floor);
        if projected_retention_floor < state.transaction_retention_floor {
            return Err(format!(
                "projected retention floor {projected_retention_floor} precedes actual floor {}",
                state.transaction_retention_floor
            ));
        }
        if projected_retention_floor > high_watermark {
            return Err(format!(
                "projected retention floor {projected_retention_floor} exceeds high watermark {high_watermark}"
            ));
        }

        let encoded_state = serde_json::to_value(&state).map_err(|error| error.to_string())?;
        let snapshot_bytes = encoded_len(&state)?;
        let transaction_authority_bytes =
            encoded_field_len(&encoded_state, "transaction_authority")?;
        let serving_state_bytes = encoded_len(state.transaction_authority.serving())?;
        let resolver_state_bytes = encoded_len(state.transaction_authority.resolver())?;
        let transaction_retry_state_bytes =
            encoded_field_len(&encoded_state, "transaction_retry_state")?;
        let transaction_frontier_state_bytes =
            encoded_field_len(&encoded_state, "transaction_frontier_state")?;
        let encoded_retry_state = serde_json::to_value(&state.transaction_retry_state)
            .map_err(|error| error.to_string())?;
        let retained_transactions_bytes =
            encoded_field_len(&encoded_state, "retained_transactions")?;
        let durable_outcomes_bytes = encoded_field_len(&encoded_state, "durable_outcomes")?
            .saturating_add(encoded_field_len(&encoded_retry_state, "outcomes")?);
        let request_fingerprints_bytes = encoded_field_len(&encoded_state, "request_fingerprints")?
            .saturating_add(encoded_field_len(&encoded_retry_state, "fingerprints")?);

        let mut projected = state.clone();
        projected
            .retained_transactions
            .retain(|record| record.commit_version > projected_retention_floor);
        projected.transaction_retention_floor = projected_retention_floor;
        let projected_state =
            serde_json::to_value(&projected).map_err(|error| error.to_string())?;
        let transaction = state.transaction_authority.view();
        Ok(crate::TransactionLogStorageStats {
            format_version: 2,
            high_watermark,
            retention_floor: state.transaction_retention_floor,
            projected_retention_floor,
            conflict_retention_floor: transaction.conflict_retention_floor,
            retry_clients: u64::try_from(state.transaction_retry_state.client_floors.len())
                .unwrap_or(u64::MAX),
            live_keys: u64::try_from(transaction.values.len()).unwrap_or(u64::MAX),
            retained_conflict_versions: transaction.retained_conflict_versions,
            durable_outcomes: u64::try_from(
                state
                    .durable_outcomes
                    .len()
                    .saturating_add(state.transaction_retry_state.outcomes.len()),
            )
            .unwrap_or(u64::MAX),
            request_fingerprints: u64::try_from(
                state
                    .request_fingerprints
                    .len()
                    .saturating_add(state.transaction_retry_state.fingerprints.len()),
            )
            .unwrap_or(u64::MAX),
            transaction_retry_outcomes: u64::try_from(state.transaction_retry_state.outcomes.len())
                .unwrap_or(u64::MAX),
            transaction_retry_fingerprints: u64::try_from(
                state.transaction_retry_state.fingerprints.len(),
            )
            .unwrap_or(u64::MAX),
            retained_records: u64::try_from(state.retained_transactions.len()).unwrap_or(u64::MAX),
            projected_retained_records: u64::try_from(projected.retained_transactions.len())
                .unwrap_or(u64::MAX),
            snapshot_bytes,
            projected_snapshot_bytes: encoded_len(&projected)?,
            transaction_authority_bytes,
            serving_state_bytes,
            resolver_state_bytes,
            transaction_retry_state_bytes,
            transaction_frontier_state_bytes,
            retained_transactions_bytes,
            projected_retained_transactions_bytes: encoded_field_len(
                &projected_state,
                "retained_transactions",
            )?,
            durable_outcomes_bytes,
            request_fingerprints_bytes,
        })
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

fn persist_snapshot(path: &Path, snapshot: &StoredSnapshot) -> io::Result<()> {
    let bytes = encode_snapshot(snapshot)?;
    let root = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "snapshot has no parent"))?;
    fs::create_dir_all(root)?;
    sync_directory(root)?;
    let next = root.join(SNAPSHOT_NEXT_FILE_NAME);
    if next.exists() {
        fs::remove_file(&next)?;
        sync_directory(root)?;
    }
    let mut replacement = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&next)?;
    replacement.write_all(&bytes)?;
    replacement.sync_all()?;
    drop(replacement);
    fs::rename(&next, path)?;
    sync_directory(root)
}

fn load_snapshot(path: &Path) -> io::Result<Option<StoredSnapshot>> {
    let root = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "snapshot has no parent"))?;
    fs::create_dir_all(root)?;
    sync_directory(root)?;
    let next = root.join(SNAPSHOT_NEXT_FILE_NAME);
    if !path.exists() {
        if next.exists() {
            fs::remove_file(&next)?;
            sync_directory(root)?;
        }
        return Ok(None);
    }

    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    let snapshot = decode_snapshot(&bytes)?;
    if next.exists() {
        fs::remove_file(&next)?;
        sync_directory(root)?;
    }
    Ok(Some(snapshot))
}

fn encode_snapshot(snapshot: &StoredSnapshot) -> io::Result<Vec<u8>> {
    let meta = serde_json::to_vec(&snapshot.meta).map_err(invalid_snapshot)?;
    if meta.len() > MAX_SNAPSHOT_META_BYTES
        || snapshot.data.len() > MAX_SNAPSHOT_DATA_BYTES
        || u32::try_from(meta.len()).is_err()
        || u64::try_from(snapshot.data.len()).is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state-machine snapshot exceeds frozen bounds",
        ));
    }
    let mut bytes = Vec::with_capacity(
        SNAPSHOT_HEADER_BYTES
            .saturating_add(meta.len())
            .saturating_add(snapshot.data.len())
            .saturating_add(SNAPSHOT_CHECKSUM_BYTES),
    );
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(
        &u32::try_from(meta.len())
            .map_err(invalid_snapshot)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(
        &u64::try_from(snapshot.data.len())
            .map_err(invalid_snapshot)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&snapshot.sequence.to_be_bytes());
    bytes.extend_from_slice(&meta);
    bytes.extend_from_slice(&snapshot.data);
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    Ok(bytes)
}

fn decode_snapshot(bytes: &[u8]) -> io::Result<StoredSnapshot> {
    let minimum = SNAPSHOT_HEADER_BYTES.saturating_add(SNAPSHOT_CHECKSUM_BYTES);
    if bytes.len() < minimum || &bytes[..4] != SNAPSHOT_MAGIC {
        return Err(invalid_snapshot("state-machine snapshot header is invalid"));
    }
    let version = u16::from_be_bytes(
        bytes[4..6]
            .try_into()
            .map_err(|_| invalid_snapshot("state-machine snapshot version is truncated"))?,
    );
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(invalid_snapshot(
            "state-machine snapshot format version is unsupported",
        ));
    }
    let meta_len =
        usize::try_from(u32::from_be_bytes(bytes[6..10].try_into().map_err(
            |_| invalid_snapshot("state-machine snapshot metadata is truncated"),
        )?))
        .map_err(invalid_snapshot)?;
    let data_len =
        usize::try_from(u64::from_be_bytes(bytes[10..18].try_into().map_err(
            |_| invalid_snapshot("state-machine snapshot data length is truncated"),
        )?))
        .map_err(invalid_snapshot)?;
    let sequence = u64::from_be_bytes(
        bytes[18..26]
            .try_into()
            .map_err(|_| invalid_snapshot("state-machine snapshot sequence is truncated"))?,
    );
    if meta_len > MAX_SNAPSHOT_META_BYTES || data_len > MAX_SNAPSHOT_DATA_BYTES {
        return Err(invalid_snapshot(
            "state-machine snapshot exceeds frozen bounds",
        ));
    }
    let checksum_offset = SNAPSHOT_HEADER_BYTES
        .checked_add(meta_len)
        .and_then(|value| value.checked_add(data_len))
        .ok_or_else(|| invalid_snapshot("state-machine snapshot length overflow"))?;
    if checksum_offset.saturating_add(SNAPSHOT_CHECKSUM_BYTES) != bytes.len()
        || Sha256::digest(&bytes[..checksum_offset]).as_slice() != &bytes[checksum_offset..]
    {
        return Err(invalid_snapshot(
            "state-machine snapshot length or checksum is invalid",
        ));
    }
    let meta: SnapshotMeta<NodeId, BasicNode> =
        serde_json::from_slice(&bytes[SNAPSHOT_HEADER_BYTES..SNAPSHOT_HEADER_BYTES + meta_len])
            .map_err(invalid_snapshot)?;
    let data = bytes[SNAPSHOT_HEADER_BYTES + meta_len..checksum_offset].to_vec();
    let decoded: StateMachineData = serde_json::from_slice(&data).map_err(invalid_snapshot)?;
    validate_snapshot_state(&meta, &decoded)?;
    Ok(StoredSnapshot {
        meta,
        data,
        sequence,
    })
}

fn validate_snapshot_state(
    meta: &SnapshotMeta<NodeId, BasicNode>,
    state: &StateMachineData,
) -> io::Result<()> {
    if meta.last_log_id != state.last_applied_log || meta.last_membership != state.last_membership {
        return Err(invalid_snapshot(
            "state-machine snapshot metadata disagrees with encoded state",
        ));
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn invalid_snapshot(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
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
            sequence: snapshot_sequence,
        };
        if let Some(path) = &self.snapshot_path {
            persist_snapshot(path, &stored)
                .map_err(|error| StorageIOError::write_snapshot(Some(meta.signature()), &error))?;
        }
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
                transaction: None,
                transaction_batch: None,
                transaction_frontier: None,
                object_frontier: None,
            };
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(payload) => {
                    if let Some(batch) = TransactionBatchCommand::decode(&payload)
                        .map_err(|error| StorageIOError::read_state_machine(&error))?
                    {
                        let generation_authority = state.generation_authority.clone();
                        response = state.apply_transaction_batch(
                            entry.log_id.index,
                            &batch,
                            generation_authority.phase != GenerationPhase::Uninitialized,
                            &generation_authority,
                            self.generation_fence_faults,
                            self.deduplicate_requests,
                            self.transaction_faults,
                        );
                    } else if let Some(command) = GenerationCommand::decode(&payload)
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
                                let publication_authorization = match &command.action {
                                    crate::PublicationAction::ActivateObjectFrontier {
                                        certificate,
                                        ..
                                    } => PublicationAuthorization {
                                        object_frontier_certificate_valid:
                                            verify_object_frontier_certificate(
                                                certificate,
                                                &state.generation_authority,
                                            ),
                                    },
                                    _ => PublicationAuthorization::default(),
                                };
                                let transition = state.publication_authority.apply_authenticated(
                                    &command.action,
                                    PublicationAuthorityContext {
                                        generation: context_generation,
                                        position: PublicationAuthorityPosition {
                                            term: log_position.term,
                                            index: log_position.index,
                                        },
                                    },
                                    self.publication_faults,
                                    publication_authorization,
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
                        if let Some(object_frontier) =
                            ObjectFrontierAdvance::decode(&command.payload)
                                .map_err(|error| StorageIOError::read_state_machine(&error))?
                        {
                            response = state.apply_object_frontier(
                                ObjectFrontierLogPosition {
                                    term: entry.log_id.leader_id.term,
                                    index: entry.log_id.index,
                                },
                                command.identity,
                                fingerprint,
                                &object_frontier,
                                command
                                    .credential
                                    .as_ref()
                                    .map(|credential| credential.generation),
                                generation_fenced,
                            );
                        } else if let Some(frontier) =
                            TransactionFrontierAdvance::decode(&command.payload)
                                .map_err(|error| StorageIOError::read_state_machine(&error))?
                        {
                            response = state.apply_transaction_frontier(
                                entry.log_id.index,
                                command.identity,
                                fingerprint,
                                &frontier,
                                generation_fenced,
                            );
                        } else if let Some(transaction) =
                            TransactionCommand::decode(&command.payload)
                                .map_err(|error| StorageIOError::read_state_machine(&error))?
                        {
                            let fingerprints =
                                transaction_request_fingerprints(&command, &transaction);
                            response = state.apply_transaction_request(
                                entry.log_id.index,
                                command.identity,
                                fingerprints,
                                &transaction,
                                0,
                                self.deduplicate_requests,
                                generation_fenced,
                                self.transaction_faults,
                            );
                        } else if self.deduplicate_requests {
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
        let sequence = self.snapshot_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
            sequence,
        };
        let decoded: StateMachineData = serde_json::from_slice(&stored.data).map_err(|error| {
            StorageIOError::read_snapshot(Some(stored.meta.signature()), &error)
        })?;
        validate_snapshot_state(&stored.meta, &decoded).map_err(|error| {
            StorageIOError::read_snapshot(Some(stored.meta.signature()), &error)
        })?;
        if let Some(path) = &self.snapshot_path {
            persist_snapshot(path, &stored).map_err(|error| {
                StorageIOError::write_snapshot(Some(stored.meta.signature()), &error)
            })?;
        }
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

fn transaction_request_fingerprints(
    command: &ClientCommand,
    transaction: &TransactionCommand,
) -> TransactionRequestFingerprints {
    let canonical = serde_json::to_vec(&(command.credential.as_ref(), transaction))
        .map(|encoded| request_fingerprint(&encoded))
        .expect("serializing a decoded transaction fingerprint cannot fail");
    let legacy_payload = transaction
        .encode_v1_for_compatibility()
        .expect("serializing a decoded v1 transaction cannot fail");
    let legacy_v1 = ClientCommand {
        identity: command.identity,
        credential: command.credential.clone(),
        payload: legacy_payload,
    }
    .encode_v1_for_compatibility()
    .map(|encoded| request_fingerprint(&encoded))
    .expect("serializing a decoded v1 client command cannot fail");
    TransactionRequestFingerprints {
        canonical,
        legacy_v1,
    }
}

fn encoded_len<T: Serialize>(value: &T) -> Result<u64, String> {
    serde_json::to_vec(value)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .map_err(|error| error.to_string())
}

fn encoded_field_len(value: &serde_json::Value, field: &str) -> Result<u64, String> {
    value.get(field).map_or(Ok(0), encoded_len)
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

    static SNAPSHOT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct SnapshotTempDir(PathBuf);

    impl SnapshotTempDir {
        fn new(label: &str) -> Self {
            let sequence = SNAPSHOT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "okv-state-snapshot-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for SnapshotTempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn persistent_store(root: &Path) -> Arc<StateMachineStore> {
        Arc::new(
            StateMachineStore::open_persistent_with_transaction_faults(
                root,
                true,
                GenerationAuthorityFaults::default(),
                GenerationFenceFaults::default(),
                PublicationAuthorityFaults::default(),
                PublicationFenceFaults::default(),
                TransactionAuthorityFaults::default(),
            )
            .expect("open persistent test state machine"),
        )
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
                transaction_system_members: BTreeMap::from([(
                    1,
                    crate::recovery_public_key(&[1; 32]).expect("derive test voter key"),
                )]),
                wal_root: "wal-7".to_owned(),
                control_root_version: 1,
            },
            GenerationAuthorityFaults::default(),
        );
        assert_eq!(GenerationCommandStatus::Accepted, status);
        store
    }

    #[test]
    fn v1_envelopes_remain_readable_and_v2_bounds_large_values() {
        let transaction = TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: vec![okv_transaction::KeyRange::point(b"large")],
            mutations: vec![okv_transaction::Mutation::Set {
                key: b"large".to_vec(),
                value: vec![7; 8_192],
            }],
        };
        let command = ClientCommand {
            identity: RequestIdentity {
                client_id: 9,
                request_id: 1,
            },
            credential: None,
            payload: transaction.encode().expect("encode v2 transaction"),
        };
        let v2_command = command.encode().expect("encode v2 client envelope");
        assert!(v2_command.starts_with(COMMAND_MAGIC_V2));
        assert_eq!(
            ClientCommand::decode(&v2_command).expect("decode v2 client envelope"),
            Some(command.clone())
        );
        let v1_command = command
            .encode_v1_for_compatibility()
            .expect("encode v1 client envelope");
        assert_eq!(
            ClientCommand::decode(&v1_command).expect("decode v1 client envelope"),
            Some(command.clone())
        );

        let batch = TransactionBatchCommand {
            commands: vec![command],
        };
        let v2_batch = batch.encode().expect("encode v2 batch envelope");
        assert!(v2_batch.starts_with(TRANSACTION_BATCH_MAGIC_V2));
        assert!(v2_batch.len() < 20 * 1_024);
        assert_eq!(
            TransactionBatchCommand::decode(&v2_batch).expect("decode v2 batch envelope"),
            Some(batch.clone())
        );
        let mut v1_batch = TRANSACTION_BATCH_MAGIC_V1.to_vec();
        v1_batch.extend(serde_json::to_vec(&batch).expect("encode v1 batch envelope"));
        assert_eq!(
            TransactionBatchCommand::decode(&v1_batch).expect("decode v1 batch envelope"),
            Some(batch)
        );
    }

    #[tokio::test]
    async fn v1_transaction_retry_recovers_through_v2_semantic_fingerprint() {
        let identity = RequestIdentity {
            client_id: 61,
            request_id: 1,
        };
        let transaction = TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: vec![okv_transaction::KeyRange::point(b"upgrade")],
            mutations: vec![okv_transaction::Mutation::Set {
                key: b"upgrade".to_vec(),
                value: vec![1, 2, 3],
            }],
        };
        let v1_payload = ClientCommand {
            identity,
            credential: None,
            payload: transaction
                .encode_v1_for_compatibility()
                .expect("encode v1 transaction"),
        }
        .encode_v1_for_compatibility()
        .expect("encode v1 client envelope");
        let v2_payload = ClientCommand {
            identity,
            credential: None,
            payload: transaction.encode().expect("encode v2 transaction"),
        }
        .encode()
        .expect("encode v2 client envelope");
        let mut store = Arc::new(StateMachineStore::new(true));
        let first = store
            .apply([normal_entry(7, v1_payload)])
            .await
            .expect("apply v1 transaction")
            .remove(0);
        let retry = store
            .apply([normal_entry(8, v2_payload)])
            .await
            .expect("retry through v2")
            .remove(0);
        assert_eq!(retry, first);
        assert_eq!(store.transaction_authority().await.current_version, 7);
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
    async fn persistent_snapshot_reopens_exact_state_and_metadata() {
        let root = SnapshotTempDir::new("reopen");
        let mut store = persistent_store(&root.0);
        store
            .apply([normal_entry(7, b"durable-state".to_vec())])
            .await
            .expect("apply state before snapshot");
        let mut builder = store.get_snapshot_builder().await;
        let built = builder
            .build_snapshot()
            .await
            .expect("build persistent snapshot");
        assert_eq!(built.meta.last_log_id.map(|log_id| log_id.index), Some(7));
        assert!(root.0.join(SNAPSHOT_FILE_NAME).is_file());
        drop(built);
        drop(builder);
        drop(store);

        let mut reopened = persistent_store(&root.0);
        assert_eq!(
            reopened.applied_payloads().await,
            vec![b"durable-state".to_vec()]
        );
        let (last_applied, membership) = reopened
            .applied_state()
            .await
            .expect("read reopened applied state");
        assert_eq!(last_applied.map(|log_id| log_id.index), Some(7));
        assert_eq!(membership, StoredMembership::default());
        let snapshot = reopened
            .get_current_snapshot()
            .await
            .expect("read reopened snapshot")
            .expect("persistent snapshot exists");
        assert_eq!(
            snapshot.meta.last_log_id.map(|log_id| log_id.index),
            Some(7)
        );
    }

    #[tokio::test]
    async fn stale_snapshot_candidate_is_ignored_after_authoritative_replay() {
        let root = SnapshotTempDir::new("stale");
        let mut store = persistent_store(&root.0);
        store
            .apply([normal_entry(8, b"authoritative".to_vec())])
            .await
            .expect("apply authoritative state");
        store
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build authoritative snapshot");
        drop(store);
        let next = root.0.join(SNAPSHOT_NEXT_FILE_NAME);
        fs::write(&next, b"uncommitted snapshot candidate").unwrap();

        let reopened = persistent_store(&root.0);

        assert_eq!(
            reopened.applied_payloads().await,
            vec![b"authoritative".to_vec()]
        );
        assert!(!next.exists());
    }

    #[tokio::test]
    async fn corrupt_persistent_snapshot_fails_closed() {
        let root = SnapshotTempDir::new("corrupt");
        let mut store = persistent_store(&root.0);
        store
            .apply([normal_entry(9, b"checksum".to_vec())])
            .await
            .expect("apply state before corrupt probe");
        store
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("build snapshot before corrupt probe");
        drop(store);
        let path = root.0.join(SNAPSHOT_FILE_NAME);
        let mut bytes = fs::read(&path).unwrap();
        bytes[SNAPSHOT_HEADER_BYTES] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let error = StateMachineStore::open_persistent_with_transaction_faults(
            &root.0,
            true,
            GenerationAuthorityFaults::default(),
            GenerationFenceFaults::default(),
            PublicationAuthorityFaults::default(),
            PublicationFenceFaults::default(),
            TransactionAuthorityFaults::default(),
        )
        .expect_err("corrupt snapshot must fail closed");

        assert!(error.contains("checksum"));
    }

    #[test]
    fn transaction_state_snapshot_is_backward_compatible_and_byte_frozen() {
        let old: StateMachineData = serde_json::from_slice(include_bytes!(
            "../fixtures/state-machine-pre-transaction-v1.json"
        ))
        .expect("decode pre-transaction snapshot");
        assert_eq!(old.transaction_authority, TransactionAuthority::default());

        let split: StateMachineData = serde_json::from_slice(include_bytes!(
            "../fixtures/state-machine-transaction-v1.json"
        ))
        .expect("decode pre-split transaction snapshot");
        assert_eq!(split.transaction_authority, TransactionAuthority::default());
        assert!(split.transaction_retry_state.outcomes.is_empty());
        assert_eq!(split.transaction_frontier_state.sequence, 0);

        let current = serde_json::to_vec(&StateMachineData::default()).expect("encode snapshot");
        assert_eq!(
            current,
            include_bytes!("../fixtures/state-machine-split-frontiers-v2.json")
                .strip_suffix(b"\n")
                .unwrap_or(include_bytes!(
                    "../fixtures/state-machine-split-frontiers-v2.json"
                ))
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn transaction_apply_is_conflict_checked_atomic_and_deduplicated() {
        let identity = RequestIdentity {
            client_id: 51,
            request_id: 1,
        };
        let conflicts = vec![
            okv_transaction::KeyRange::point(b"a/account"),
            okv_transaction::KeyRange::point(b"z/account"),
        ];
        let transaction = TransactionCommand {
            read_version: 0,
            read_conflicts: conflicts.clone(),
            write_conflicts: conflicts,
            mutations: vec![
                okv_transaction::Mutation::Set {
                    key: b"a/account".to_vec(),
                    value: vec![1],
                },
                okv_transaction::Mutation::Set {
                    key: b"z/account".to_vec(),
                    value: vec![2],
                },
            ],
        };
        let payload = ClientCommand {
            identity,
            credential: None,
            payload: transaction.encode().expect("encode transaction"),
        }
        .encode()
        .expect("encode client command");
        let mut store = Arc::new(StateMachineStore::new(true));
        let first = store
            .apply([normal_entry(7, payload.clone())])
            .await
            .expect("apply transaction")
            .remove(0);
        assert_eq!(
            first
                .transaction
                .as_ref()
                .map(|response| response.status.clone()),
            Some(okv_transaction::TransactionStatus::Committed { commit_version: 7 })
        );
        assert_eq!(store.transaction_authority().await.values.len(), 2);

        let retry = store
            .apply([normal_entry(8, payload)])
            .await
            .expect("apply retry")
            .remove(0);
        assert_eq!(retry, first);
        let first_page = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: None,
                max_records: 1,
            })
            .await
            .expect("read retained transaction");
        assert_eq!(first_page.high_watermark, 7);
        assert_eq!(first_page.target_version, 7);
        assert_eq!(first_page.next_after_version, 7);
        assert!(first_page.complete);
        assert_eq!(first_page.records.len(), 1);
        assert_eq!(first_page.records[0].commit_version, 7);

        let stats = store
            .transaction_log_storage_stats(crate::TransactionLogStorageStatsRequest {
                projected_retention_floor: Some(7),
            })
            .await
            .expect("account populated transaction state");
        assert_eq!(stats.high_watermark, 7);
        assert_eq!(stats.retention_floor, 0);
        assert_eq!(stats.projected_retention_floor, 7);
        assert_eq!(stats.live_keys, 2);
        assert_eq!(stats.retained_conflict_versions, 1);
        assert_eq!(stats.durable_outcomes, 1);
        assert_eq!(stats.request_fingerprints, 1);
        assert_eq!(stats.retained_records, 1);
        assert_eq!(stats.projected_retained_records, 0);
        assert!(stats.projected_snapshot_bytes < stats.snapshot_bytes);
        assert!(stats.transaction_authority_bytes > 0);
        assert!(stats.retained_transactions_bytes > 0);
        assert_eq!(stats.projected_retained_transactions_bytes, 0);
        assert!(stats.durable_outcomes_bytes > 0);
        assert!(stats.request_fingerprints_bytes > 0);
        assert!(store
            .transaction_log_storage_stats(crate::TransactionLogStorageStatsRequest {
                projected_retention_floor: Some(8),
            })
            .await
            .is_err());

        let mut snapshot_builder = store.get_snapshot_builder().await;
        let snapshot = snapshot_builder
            .build_snapshot()
            .await
            .expect("build populated snapshot");
        let mut restored_store = Arc::new(StateMachineStore::new(true));
        restored_store
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install populated snapshot");
        let restored_page = restored_store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 4_096,
            })
            .await
            .expect("read restored retained transaction");
        assert_eq!(restored_page.records, first_page.records);
        assert_eq!(
            restored_store.durable_outcome(identity).await,
            Some(first.clone())
        );

        let conflicting_payload = ClientCommand {
            identity: RequestIdentity {
                client_id: 51,
                request_id: 2,
            },
            credential: None,
            payload: transaction.encode().expect("encode transaction"),
        }
        .encode()
        .expect("encode client command");
        let conflict = store
            .apply([normal_entry(9, conflicting_payload)])
            .await
            .expect("apply conflict")
            .remove(0);
        assert_eq!(
            conflict.transaction.map(|response| response.status),
            Some(okv_transaction::TransactionStatus::Conflict {
                conflicting_version: 7
            })
        );
        assert_eq!(store.transaction_authority().await.current_version, 7);
        let after_conflict = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 4_096,
            })
            .await
            .expect("read retained transaction after conflict");
        assert_eq!(after_conflict.records, first_page.records);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn transaction_batch_orders_results_retries_and_recovery_pages() {
        let transaction =
            |read_conflicts: Vec<okv_transaction::KeyRange>, key: &[u8]| TransactionCommand {
                read_version: 0,
                read_conflicts,
                write_conflicts: vec![okv_transaction::KeyRange::point(key)],
                mutations: vec![okv_transaction::Mutation::Set {
                    key: key.to_vec(),
                    value: key.to_vec(),
                }],
            };
        let identities = [
            RequestIdentity {
                client_id: 81,
                request_id: 1,
            },
            RequestIdentity {
                client_id: 81,
                request_id: 2,
            },
            RequestIdentity {
                client_id: 81,
                request_id: 3,
            },
        ];
        let commands = vec![
            transaction(Vec::new(), b"a"),
            transaction(vec![okv_transaction::KeyRange::point(b"a")], b"b"),
            transaction(Vec::new(), b"c"),
        ];
        let client_commands = identities
            .iter()
            .zip(&commands)
            .map(|(identity, transaction)| ClientCommand {
                identity: *identity,
                credential: None,
                payload: transaction.encode().expect("encode transaction"),
            })
            .collect::<Vec<_>>();
        let payload = TransactionBatchCommand {
            commands: client_commands.clone(),
        }
        .encode()
        .expect("encode batch");
        let mut store = Arc::new(StateMachineStore::new(true));
        let response = store
            .apply([normal_entry(7, payload.clone())])
            .await
            .expect("apply transaction batch")
            .remove(0)
            .transaction_batch
            .expect("transaction batch response");
        assert_eq!(response.items.len(), 3);
        assert_eq!(
            response.items[0]
                .transaction
                .as_ref()
                .map(|item| (&item.status, item.batch_order)),
            Some((&TransactionStatus::Committed { commit_version: 7 }, 0))
        );
        assert_eq!(
            response.items[1]
                .transaction
                .as_ref()
                .map(|item| (&item.status, item.batch_order)),
            Some((
                &TransactionStatus::Conflict {
                    conflicting_version: 7
                },
                1
            ))
        );
        assert_eq!(
            response.items[2]
                .transaction
                .as_ref()
                .map(|item| (&item.status, item.batch_order)),
            Some((&TransactionStatus::Committed { commit_version: 7 }, 2))
        );

        let first = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 1,
            })
            .await
            .expect("read first batch page");
        assert!(!first.complete);
        assert_eq!(first.records[0].batch_order, 0);
        let second = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: first.next_after_version,
                after_batch_order_exclusive: first.next_after_batch_order,
                through_version_inclusive: Some(7),
                max_records: 1,
            })
            .await
            .expect("read second batch page");
        assert!(second.complete);
        assert_eq!(second.records[0].batch_order, 2);

        let individual_retry = store
            .apply([normal_entry(
                8,
                client_commands[2]
                    .encode()
                    .expect("encode individual retry"),
            )])
            .await
            .expect("apply individual retry")
            .remove(0);
        assert_eq!(
            individual_retry.transaction,
            response.items[2].transaction.clone()
        );
        let whole_retry = store
            .apply([normal_entry(9, payload)])
            .await
            .expect("apply whole batch retry")
            .remove(0)
            .transaction_batch
            .expect("whole batch retry response");
        assert_eq!(whole_retry.items, response.items);

        let duplicate = TransactionBatchCommand {
            commands: vec![client_commands[0].clone(), client_commands[0].clone()],
        }
        .encode()
        .expect("encode duplicate batch");
        let rejected = store
            .apply([normal_entry(10, duplicate)])
            .await
            .expect("reject duplicate batch")
            .remove(0);
        assert_eq!(rejected.error, Some(ApplyError::InvalidTransactionBatch));
        assert_eq!(store.transaction_authority().await.values.len(), 2);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn split_frontiers_reclaim_and_fail_closed() {
        let transaction_identity = RequestIdentity {
            client_id: 71,
            request_id: 1,
        };
        let range = okv_transaction::KeyRange::point(b"account/71");
        let transaction = TransactionCommand {
            read_version: 0,
            read_conflicts: vec![range.clone()],
            write_conflicts: vec![range],
            mutations: vec![okv_transaction::Mutation::Set {
                key: b"account/71".to_vec(),
                value: vec![9],
            }],
        };
        let transaction_payload = ClientCommand {
            identity: transaction_identity,
            credential: None,
            payload: transaction.encode().expect("encode transaction"),
        }
        .encode()
        .expect("encode client transaction");
        let mut store = Arc::new(StateMachineStore::new(true));
        let committed = store
            .apply([normal_entry(7, transaction_payload.clone())])
            .await
            .expect("apply transaction")
            .remove(0);
        assert!(matches!(
            committed
                .transaction
                .as_ref()
                .map(|response| &response.status),
            Some(okv_transaction::TransactionStatus::Committed { commit_version: 7 })
        ));

        let frontier_identity = RequestIdentity {
            client_id: 9001,
            request_id: 1,
        };
        let frontier = crate::TransactionFrontierAdvance {
            sequence: 1,
            conflict_retention_floor: 7,
            retry_floors: vec![crate::TransactionRetryFloor {
                client_id: transaction_identity.client_id,
                through_request_id: transaction_identity.request_id,
            }],
        };
        let frontier_payload = ClientCommand {
            identity: frontier_identity,
            credential: None,
            payload: frontier.encode().expect("encode frontier"),
        }
        .encode()
        .expect("encode frontier client command");
        let advanced = store
            .apply([normal_entry(8, frontier_payload.clone())])
            .await
            .expect("advance frontiers")
            .remove(0);
        let frontier_response = advanced
            .transaction_frontier
            .as_ref()
            .expect("frontier response");
        assert_eq!(frontier_response.sequence, 1);
        assert_eq!(frontier_response.pruned_conflict_versions, 1);
        assert_eq!(frontier_response.pruned_retry_outcomes, 1);
        assert_eq!(
            store.transaction_authority().await.conflict_retention_floor,
            7
        );
        assert_eq!(
            store
                .transaction_authority()
                .await
                .retained_conflict_versions,
            0
        );
        assert_eq!(store.durable_outcome(transaction_identity).await, None);

        let expired = store
            .apply([normal_entry(9, transaction_payload)])
            .await
            .expect("reject expired retry")
            .remove(0);
        assert_eq!(expired.error, Some(ApplyError::RetryIdentityExpired));
        assert_eq!(store.transaction_authority().await.current_version, 7);

        let stale_payload = ClientCommand {
            identity: RequestIdentity {
                client_id: 71,
                request_id: 2,
            },
            credential: None,
            payload: transaction.encode().expect("encode stale transaction"),
        }
        .encode()
        .expect("encode stale client command");
        let stale = store
            .apply([normal_entry(10, stale_payload)])
            .await
            .expect("reject stale read")
            .remove(0);
        assert_eq!(
            stale.transaction.map(|response| response.status),
            Some(okv_transaction::TransactionStatus::Rejected {
                reason: okv_transaction::TransactionRejectReason::ReadVersionExpired
            })
        );
        assert_eq!(store.transaction_authority().await.current_version, 7);

        let replay = store
            .apply([normal_entry(11, frontier_payload)])
            .await
            .expect("replay latest frontier")
            .remove(0);
        assert_eq!(replay, advanced);

        let gap = crate::TransactionFrontierAdvance {
            sequence: 3,
            conflict_retention_floor: 7,
            retry_floors: Vec::new(),
        };
        let gap_payload = ClientCommand {
            identity: RequestIdentity {
                client_id: 9001,
                request_id: 3,
            },
            credential: None,
            payload: gap.encode().expect("encode gap"),
        }
        .encode()
        .expect("encode gap client command");
        let rejected_gap = store
            .apply([normal_entry(12, gap_payload)])
            .await
            .expect("reject frontier gap")
            .remove(0);
        assert_eq!(
            rejected_gap.error,
            Some(ApplyError::TransactionFrontierSequenceGap)
        );
        assert_eq!(
            store.transaction_authority().await.conflict_retention_floor,
            7
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn object_frontier_physically_pops_and_survives_snapshot_replay() {
        let credential = GenerationCredential {
            generation: 7,
            transaction_system_id: "txn-7".to_owned(),
        };
        let transaction = TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: vec![okv_transaction::KeyRange::point(b"account/object")],
            mutations: vec![okv_transaction::Mutation::Set {
                key: b"account/object".to_vec(),
                value: vec![4, 7],
            }],
        };
        let transaction_payload = ClientCommand {
            identity: RequestIdentity {
                client_id: 81,
                request_id: 1,
            },
            credential: Some(credential.clone()),
            payload: transaction.encode().expect("encode transaction"),
        }
        .encode()
        .expect("encode transaction command");
        let mut store = active_store(true).await;
        let committed = store
            .apply([normal_entry(7, transaction_payload)])
            .await
            .expect("apply transaction")
            .remove(0);
        assert!(matches!(
            committed.transaction.map(|response| response.status),
            Some(TransactionStatus::Committed { commit_version: 7 })
        ));

        let frontier = ObjectFrontierRecord {
            owner_generation: 7,
            source_root: "range-main".to_owned(),
            manifest: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "objects/row-manifest".to_owned(),
                length: 113,
                sha256: "c".repeat(64),
            },
            covered_through: 7,
            prepared_at: PublicationAuthorityPosition { term: 2, index: 14 },
        };
        let identity = RequestIdentity {
            client_id: 82,
            request_id: 1,
        };
        let advance = ObjectFrontierAdvance {
            frontier: frontier.clone(),
        };
        let command = ClientCommand {
            identity,
            credential: Some(credential),
            payload: advance.encode().expect("encode object frontier"),
        }
        .encode()
        .expect("encode object-frontier command");
        let applied = store
            .apply([normal_entry(8, command.clone())])
            .await
            .expect("apply object frontier")
            .remove(0);
        let response = applied.object_frontier.as_ref().expect("frontier response");
        assert_eq!(response.frontier, frontier);
        assert_eq!(response.prior_retention_floor, 0);
        assert_eq!(response.retention_floor, 7);
        assert_eq!(response.popped_records, 1);
        assert!(store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 16,
            })
            .await
            .is_err());
        let suffix = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 7,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 16,
            })
            .await
            .expect("read suffix at retained floor");
        assert!(suffix.records.is_empty());

        let retry = store
            .apply([normal_entry(9, command)])
            .await
            .expect("retry object frontier")
            .remove(0);
        assert_eq!(retry, applied);

        let mut snapshot_builder = store.get_snapshot_builder().await;
        let snapshot = snapshot_builder
            .build_snapshot()
            .await
            .expect("build frontier snapshot");
        let mut restored = active_store(true).await;
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install frontier snapshot");
        assert_eq!(
            restored.applied_object_frontier().await,
            Some((frontier, ObjectFrontierLogPosition { term: 3, index: 8 }))
        );
        assert!(restored
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(7),
                max_records: 16,
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn publication_activation_verifies_the_data_voter_certificate() {
        let mut store = active_store(true).await;
        let frontier = ObjectFrontierRecord {
            owner_generation: 7,
            source_root: "range-main".to_owned(),
            manifest: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "objects/certified-manifest".to_owned(),
                length: 144,
                sha256: "e".repeat(64),
            },
            covered_through: 13,
            prepared_at: PublicationAuthorityPosition { term: 2, index: 11 },
        };
        store
            .data
            .write()
            .await
            .publication_authority
            .pending_object_frontier = Some(frontier.clone());
        let generation = store.generation_authority().await;
        let statement = crate::object_frontier_certificate_statement(
            &generation,
            frontier.clone(),
            ObjectFrontierLogPosition { term: 4, index: 18 },
        );
        let certificate = crate::ObjectFrontierCertificate {
            attestations: vec![
                crate::sign_object_frontier_statement(1, &[1; 32], &statement)
                    .expect("sign object frontier"),
            ],
            statement,
        };
        let command = PublicationCommand {
            identity: RequestIdentity {
                client_id: 83,
                request_id: 1,
            },
            credential: GenerationCredential {
                generation: 7,
                transaction_system_id: "txn-7".to_owned(),
            },
            action: PublicationAction::ActivateObjectFrontier {
                expected_pending: frontier.clone(),
                certificate,
            },
        }
        .encode()
        .expect("encode frontier activation");
        let activated = store
            .apply([normal_entry(19, command)])
            .await
            .expect("apply frontier activation")
            .remove(0);
        assert_eq!(
            activated.publication.map(|response| response.status),
            Some(PublicationCommandStatus::Accepted)
        );
        let publication = store.publication_authority().await;
        assert_eq!(publication.active_object_frontier, Some(frontier));
        assert!(publication.pending_object_frontier.is_none());
    }

    #[tokio::test]
    async fn retained_transaction_pages_freeze_targets_and_tolerate_version_gaps() {
        let mut store = Arc::new(StateMachineStore::new(true));
        for (index, identity, key) in [(4, 1, b"a".as_slice()), (9, 2, b"z".as_slice())] {
            let command = TransactionCommand {
                read_version: if index == 4 { 0 } else { 4 },
                read_conflicts: vec![okv_transaction::KeyRange::point(key)],
                write_conflicts: vec![okv_transaction::KeyRange::point(key)],
                mutations: vec![okv_transaction::Mutation::Set {
                    key: key.to_vec(),
                    value: vec![u8::try_from(index).unwrap()],
                }],
            };
            let payload = ClientCommand {
                identity: RequestIdentity {
                    client_id: 61,
                    request_id: identity,
                },
                credential: None,
                payload: command.encode().expect("encode transaction"),
            }
            .encode()
            .expect("encode client command");
            let response = store
                .apply([normal_entry(index, payload)])
                .await
                .expect("apply transaction")
                .remove(0);
            assert!(matches!(
                response.transaction.map(|transaction| transaction.status),
                Some(TransactionStatus::Committed { .. })
            ));
        }

        let first = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: None,
                max_records: 1,
            })
            .await
            .expect("first page");
        assert_eq!(first.target_version, 9);
        assert_eq!(first.next_after_version, 4);
        assert!(!first.complete);
        assert_eq!(first.records[0].commit_version, 4);

        let second = store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: first.next_after_version,
                after_batch_order_exclusive: first.next_after_batch_order,
                through_version_inclusive: Some(first.target_version),
                max_records: 1,
            })
            .await
            .expect("second page");
        assert_eq!(second.target_version, 9);
        assert_eq!(second.next_after_version, 9);
        assert!(second.complete);
        assert_eq!(second.records[0].commit_version, 9);

        assert!(store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 9,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(10),
                max_records: 1,
            })
            .await
            .is_err());
        assert!(store
            .retained_transactions(crate::RetainedTransactionReadRequest {
                after_version_exclusive: 0,
                after_batch_order_exclusive: None,
                through_version_inclusive: None,
                max_records: 0,
            })
            .await
            .is_err());
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
