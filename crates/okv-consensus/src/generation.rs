use crate::RequestIdentity;
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const GENERATION_COMMAND_MAGIC: &[u8] = b"OKVG1";
const RECOVERY_CERTIFICATE_MAGIC: &[u8] = b"OKV-RECOVERY-CERTIFICATE-V1\0";
const RECOVERY_CERTIFICATE_VERSION: u16 = 1;
const ROUTINE_CERTIFICATE_MAGIC: &[u8] = b"OKV-ROUTINE-RECONFIGURATION-CERTIFICATE-V1\0";
const ROUTINE_CERTIFICATE_VERSION: u16 = 1;

mod member_map_wire {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S>(members: &BTreeMap<u64, Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        members.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<u64, Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(u64, Vec<u8>)>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let members = entries.into_iter().collect::<BTreeMap<_, _>>();
        if members.len() != entry_count {
            return Err(D::Error::custom("duplicate recovery member identity"));
        }
        Ok(members)
    }
}

mod incarnation_map_wire {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S>(
        incarnations: &BTreeMap<u64, [u8; 16]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        incarnations
            .iter()
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<u64, [u8; 16]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(u64, [u8; 16])>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let incarnations = entries.into_iter().collect::<BTreeMap<_, _>>();
        if incarnations.len() != entry_count {
            return Err(D::Error::custom("duplicate storage-incarnation identity"));
        }
        Ok(incarnations)
    }
}

/// Exact identity of one committed entry in a data transaction-system log.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryLogPosition {
    pub term: u64,
    pub index: u64,
}

impl RecoveryLogPosition {
    /// Convert an `OpenRaft` log identifier into the certificate wire form.
    #[must_use]
    pub const fn from_log_id(log_id: openraft::LogId<u64>) -> Self {
        Self {
            term: log_id.leader_id.term,
            index: log_id.index,
        }
    }
}

/// Local observation a data quorum certifies during generation takeover.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCertificateKind {
    Fence,
    Recovered,
}

/// Canonical statement signed independently by data voters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryCertificateStatement {
    pub protocol_version: u16,
    pub kind: RecoveryCertificateKind,
    pub cell_id: u64,
    pub generation: u64,
    pub recovery_id: u64,
    pub active_transaction_system_id: String,
    pub pending_transaction_system_id: String,
    pub log_position: RecoveryLogPosition,
    pub membership_sha256: [u8; 32],
}

impl RecoveryCertificateStatement {
    /// Construct a versioned statement with the canonical membership digest.
    #[must_use]
    pub fn new(
        kind: RecoveryCertificateKind,
        state: &GenerationAuthorityState,
        log_position: RecoveryLogPosition,
        members: &BTreeMap<u64, Vec<u8>>,
    ) -> Self {
        Self {
            protocol_version: RECOVERY_CERTIFICATE_VERSION,
            kind,
            cell_id: state.cell_id,
            generation: state.generation,
            recovery_id: state.recovery_id.unwrap_or_default(),
            active_transaction_system_id: state.transaction_system_id.clone().unwrap_or_default(),
            pending_transaction_system_id: state
                .pending_transaction_system_id
                .clone()
                .unwrap_or_default(),
            log_position,
            membership_sha256: recovery_membership_digest(members),
        }
    }

    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = RECOVERY_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// One data-voter signature over a recovery certificate statement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryAttestation {
    pub signer_id: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof presented to the external generation authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryCertificate {
    pub statement: RecoveryCertificateStatement,
    pub attestations: Vec<RecoveryAttestation>,
}

/// Certificate purpose for one healthy-quorum voter replacement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineReconfigurationCertificateKind {
    LearnerReady,
    MembershipCommitted,
}

/// Canonical statement signed during routine voter replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutineReconfigurationCertificateStatement {
    pub protocol_version: u16,
    pub kind: RoutineReconfigurationCertificateKind,
    pub cell_id: u64,
    pub generation: u64,
    pub membership_epoch: u64,
    pub reconfiguration_id: u64,
    pub transaction_system_id: String,
    pub old_membership_sha256: [u8; 32],
    pub next_membership_sha256: [u8; 32],
    pub replacement_node: u64,
    pub replacement_incarnation: [u8; 16],
    pub snapshot_position: RecoveryLogPosition,
    pub applied_position: RecoveryLogPosition,
}

impl RoutineReconfigurationCertificateStatement {
    /// Construct the exact statement for the authority's pending replacement.
    #[must_use]
    pub fn new(
        kind: RoutineReconfigurationCertificateKind,
        state: &GenerationAuthorityState,
        snapshot_position: RecoveryLogPosition,
        applied_position: RecoveryLogPosition,
    ) -> Option<Self> {
        let pending = state.pending_reconfiguration.as_ref()?;
        Some(Self {
            protocol_version: ROUTINE_CERTIFICATE_VERSION,
            kind,
            cell_id: state.cell_id,
            generation: state.generation,
            membership_epoch: state.membership_epoch,
            reconfiguration_id: pending.reconfiguration_id,
            transaction_system_id: state.transaction_system_id.clone().unwrap_or_default(),
            old_membership_sha256: routine_membership_digest(
                &state.transaction_system_members,
                &state.transaction_system_incarnations,
            ),
            next_membership_sha256: routine_membership_digest(
                &pending.next_transaction_system_members,
                &pending.next_transaction_system_incarnations,
            ),
            replacement_node: pending.replacement_node,
            replacement_incarnation: pending.replacement_incarnation,
            snapshot_position,
            applied_position,
        })
    }

    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = ROUTINE_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// Quorum proof for learner readiness or the committed next voter set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutineReconfigurationCertificate {
    pub statement: RoutineReconfigurationCertificateStatement,
    pub attestations: Vec<RecoveryAttestation>,
}

/// Sign one canonical routine-reconfiguration statement.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_routine_reconfiguration_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &RoutineReconfigurationCertificateStatement,
) -> Result<RecoveryAttestation, String> {
    let key_pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "reconfiguration signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(RecoveryAttestation {
        signer_id,
        signature: key_pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Stable digest of voter identities, public keys, and storage incarnations.
#[must_use]
pub fn routine_membership_digest(
    members: &BTreeMap<u64, Vec<u8>>,
    incarnations: &BTreeMap<u64, [u8; 16]>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"OKV-ROUTINE-MEMBERSHIP-V1\0");
    digest.update((members.len() as u64).to_be_bytes());
    for (node_id, public_key) in members {
        digest.update(node_id.to_be_bytes());
        digest.update((public_key.len() as u64).to_be_bytes());
        digest.update(public_key);
        if let Some(incarnation) = incarnations.get(node_id) {
            digest.update(incarnation);
        }
    }
    digest.finalize().into()
}

/// Compute the stable digest of a voter identity and Ed25519 public-key map.
#[must_use]
pub fn recovery_membership_digest(members: &BTreeMap<u64, Vec<u8>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"OKV-RECOVERY-MEMBERSHIP-V1\0");
    digest.update((members.len() as u64).to_be_bytes());
    for (node_id, public_key) in members {
        digest.update(node_id.to_be_bytes());
        digest.update((public_key.len() as u64).to_be_bytes());
        digest.update(public_key);
    }
    digest.finalize().into()
}

/// Derive the Ed25519 public key for one 32-byte private seed.
///
/// # Errors
///
/// Returns an error when the seed is not a valid Ed25519 seed.
pub fn recovery_public_key(private_key_seed: &[u8]) -> Result<Vec<u8>, String> {
    Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map(|key_pair| key_pair.public_key().as_ref().to_vec())
        .map_err(|_| "recovery signing seed must contain exactly 32 bytes".to_owned())
}

/// Sign one canonical recovery statement for a configured data voter.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_recovery_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &RecoveryCertificateStatement,
) -> Result<RecoveryAttestation, String> {
    let key_pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "recovery signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(RecoveryAttestation {
        signer_id,
        signature: key_pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Lifecycle phase owned by the external cell coordinator quorum.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationPhase {
    #[default]
    Uninitialized,
    Active,
    Fencing,
    Recovering,
}

/// Authority phase for one same-generation voter replacement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineReconfigurationPhase {
    Prepared,
    LearnerReady,
}

/// Replicated pending state for one routine voter replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingRoutineReconfiguration {
    pub reconfiguration_id: u64,
    pub expected_membership_epoch: u64,
    pub phase: RoutineReconfigurationPhase,
    pub replacement_node: u64,
    pub replacement_incarnation: [u8; 16],
    #[serde(with = "member_map_wire")]
    pub next_transaction_system_members: BTreeMap<u64, Vec<u8>>,
    #[serde(with = "incarnation_map_wire")]
    pub next_transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
    pub learner_snapshot_position: Option<RecoveryLogPosition>,
    pub learner_applied_position: Option<RecoveryLogPosition>,
}

/// Durable idempotence receipt for a completed replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletedRoutineReconfiguration {
    pub reconfiguration_id: u64,
    pub membership_epoch: u64,
    pub membership_sha256: [u8; 32],
    pub membership_position: RecoveryLogPosition,
    pub certificate_sha256: [u8; 32],
}

/// Coordinator-owned transaction-system descriptor for one bounded cell.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationAuthorityState {
    pub cell_id: u64,
    pub generation: u64,
    pub phase: GenerationPhase,
    pub recovery_id: Option<u64>,
    pub transaction_system_id: Option<String>,
    pub pending_transaction_system_id: Option<String>,
    #[serde(with = "member_map_wire")]
    pub transaction_system_members: BTreeMap<u64, Vec<u8>>,
    #[serde(
        default,
        with = "incarnation_map_wire",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
    #[serde(with = "member_map_wire")]
    pub pending_transaction_system_members: BTreeMap<u64, Vec<u8>>,
    #[serde(
        default,
        with = "incarnation_map_wire",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub pending_transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
    pub wal_root: Option<String>,
    pub control_root_version: u64,
    pub fenced_log_index: u64,
    pub recovered_log_index: u64,
    pub fenced_log_position: Option<RecoveryLogPosition>,
    pub recovered_log_position: Option<RecoveryLogPosition>,
    pub last_completed_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completed_recovery_id: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub membership_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_reconfiguration: Option<PendingRoutineReconfiguration>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub completed_reconfigurations: BTreeMap<u64, CompletedRoutineReconfiguration>,
}

impl GenerationAuthorityState {
    /// Whether a linearizable coordinator read authorizes one client commit.
    #[must_use]
    pub fn authorizes(&self, generation: u64, transaction_system_id: &str) -> bool {
        self.phase == GenerationPhase::Active
            && self.generation == generation
            && self.transaction_system_id.as_deref() == Some(transaction_system_id)
    }

    /// Whether an active voter process may originate one client commit.
    #[must_use]
    pub fn authorizes_node(
        &self,
        generation: u64,
        transaction_system_id: &str,
        node_id: u64,
    ) -> bool {
        self.authorizes(generation, transaction_system_id)
            && self.transaction_system_members.contains_key(&node_id)
    }

    /// Whether the authority admits one exact fresh learner.
    #[must_use]
    pub fn authorizes_routine_learner(
        &self,
        generation: u64,
        membership_epoch: u64,
        reconfiguration_id: u64,
        node_id: u64,
        storage_incarnation: [u8; 16],
    ) -> bool {
        self.phase == GenerationPhase::Active
            && self.generation == generation
            && self.membership_epoch == membership_epoch
            && self
                .pending_reconfiguration
                .as_ref()
                .is_some_and(|pending| {
                    pending.reconfiguration_id == reconfiguration_id
                        && pending.replacement_node == node_id
                        && pending.replacement_incarnation == storage_incarnation
                        && matches!(
                            pending.phase,
                            RoutineReconfigurationPhase::Prepared
                                | RoutineReconfigurationPhase::LearnerReady
                        )
                })
    }

    /// Whether the authority permits the exact next voter-set transition.
    #[must_use]
    pub fn authorizes_routine_membership(
        &self,
        generation: u64,
        membership_epoch: u64,
        reconfiguration_id: u64,
        voters: &BTreeSet<u64>,
    ) -> bool {
        self.phase == GenerationPhase::Active
            && self.generation == generation
            && self.membership_epoch == membership_epoch
            && self
                .pending_reconfiguration
                .as_ref()
                .is_some_and(|pending| {
                    pending.reconfiguration_id == reconfiguration_id
                        && pending.phase == RoutineReconfigurationPhase::LearnerReady
                        && pending
                            .next_transaction_system_members
                            .keys()
                            .copied()
                            .collect::<BTreeSet<_>>()
                            == *voters
                })
    }

    /// Whether a linearizable coordinator read authorizes one quiesced handoff.
    #[must_use]
    pub fn authorizes_recovery(
        &self,
        generation: u64,
        recovery_id: u64,
        pending_transaction_system_id: &str,
    ) -> bool {
        self.phase == GenerationPhase::Recovering
            && self.generation == generation
            && self.recovery_id == Some(recovery_id)
            && self.pending_transaction_system_id.as_deref() == Some(pending_transaction_system_id)
    }

    /// Whether a linearizable coordinator read authorizes the data-log barrier.
    #[must_use]
    pub fn authorizes_fencing(
        &self,
        generation: u64,
        recovery_id: u64,
        pending_transaction_system_id: &str,
    ) -> bool {
        self.phase == GenerationPhase::Fencing
            && self.generation == generation
            && self.recovery_id == Some(recovery_id)
            && self.pending_transaction_system_id.as_deref() == Some(pending_transaction_system_id)
    }

    fn verify_recovery_certificate(
        &self,
        certificate: &RecoveryCertificate,
        expected_kind: RecoveryCertificateKind,
        members: &BTreeMap<u64, Vec<u8>>,
    ) -> bool {
        let statement = &certificate.statement;
        if statement.protocol_version != RECOVERY_CERTIFICATE_VERSION
            || statement.kind != expected_kind
            || statement.cell_id != self.cell_id
            || statement.generation != self.generation
            || Some(statement.recovery_id) != self.recovery_id
            || Some(statement.active_transaction_system_id.as_str())
                != self.transaction_system_id.as_deref()
            || Some(statement.pending_transaction_system_id.as_str())
                != self.pending_transaction_system_id.as_deref()
            || statement.log_position.index == 0
            || !valid_recovery_members(members)
            || statement.membership_sha256 != recovery_membership_digest(members)
        {
            return false;
        }
        if expected_kind == RecoveryCertificateKind::Recovered
            && statement.log_position.index <= self.fenced_log_index
        {
            return false;
        }
        let Ok(bytes) = statement.signing_bytes() else {
            return false;
        };
        let mut distinct_signers = BTreeSet::new();
        for attestation in &certificate.attestations {
            if !distinct_signers.insert(attestation.signer_id) {
                return false;
            }
            let Some(public_key) = members.get(&attestation.signer_id) else {
                return false;
            };
            if UnparsedPublicKey::new(&ED25519, public_key)
                .verify(&bytes, &attestation.signature)
                .is_err()
            {
                return false;
            }
        }
        distinct_signers.len() >= quorum_size(members.len())
    }

    fn verify_routine_certificate(
        &self,
        certificate: &RoutineReconfigurationCertificate,
        expected_kind: RoutineReconfigurationCertificateKind,
    ) -> bool {
        let Some(pending) = self.pending_reconfiguration.as_ref() else {
            return false;
        };
        let statement = &certificate.statement;
        let expected = RoutineReconfigurationCertificateStatement::new(
            expected_kind,
            self,
            statement.snapshot_position,
            statement.applied_position,
        );
        if expected.as_ref() != Some(statement)
            || statement.snapshot_position.index == 0
            || statement.applied_position.index < statement.snapshot_position.index
        {
            return false;
        }
        if expected_kind == RoutineReconfigurationCertificateKind::MembershipCommitted
            && pending
                .learner_applied_position
                .is_none_or(|position| statement.applied_position.index <= position.index)
        {
            return false;
        }
        let Ok(bytes) = statement.signing_bytes() else {
            return false;
        };
        let mut distinct_signers = BTreeSet::new();
        for attestation in &certificate.attestations {
            if !distinct_signers.insert(attestation.signer_id) {
                return false;
            }
            let public_key = self
                .transaction_system_members
                .get(&attestation.signer_id)
                .or_else(|| {
                    pending
                        .next_transaction_system_members
                        .get(&attestation.signer_id)
                });
            let Some(public_key) = public_key else {
                return false;
            };
            if UnparsedPublicKey::new(&ED25519, public_key)
                .verify(&bytes, &attestation.signature)
                .is_err()
            {
                return false;
            }
        }
        match expected_kind {
            RoutineReconfigurationCertificateKind::LearnerReady => {
                let old_signers = distinct_signers
                    .iter()
                    .filter(|node_id| self.transaction_system_members.contains_key(node_id))
                    .count();
                distinct_signers.contains(&pending.replacement_node)
                    && old_signers >= quorum_size(self.transaction_system_members.len())
            }
            RoutineReconfigurationCertificateKind::MembershipCommitted => {
                let next_signers = distinct_signers
                    .iter()
                    .filter(|node_id| {
                        pending
                            .next_transaction_system_members
                            .contains_key(node_id)
                    })
                    .count();
                next_signers >= quorum_size(pending.next_transaction_system_members.len())
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply(
        &mut self,
        action: &GenerationAction,
        faults: GenerationAuthorityFaults,
    ) -> GenerationCommandStatus {
        match action {
            GenerationAction::Bootstrap {
                cell_id,
                generation,
                transaction_system_id,
                transaction_system_members,
                transaction_system_incarnations,
                wal_root,
                control_root_version,
            } => {
                if self.phase != GenerationPhase::Uninitialized
                    || *cell_id == 0
                    || *generation == 0
                    || transaction_system_id.is_empty()
                    || !valid_members_with_optional_incarnations(
                        transaction_system_members,
                        transaction_system_incarnations,
                    )
                    || wal_root.is_empty()
                    || *control_root_version == 0
                {
                    return GenerationCommandStatus::InvalidPhase;
                }
                *self = Self {
                    cell_id: *cell_id,
                    generation: *generation,
                    phase: GenerationPhase::Active,
                    recovery_id: None,
                    transaction_system_id: Some(transaction_system_id.clone()),
                    pending_transaction_system_id: None,
                    transaction_system_members: transaction_system_members.clone(),
                    transaction_system_incarnations: transaction_system_incarnations.clone(),
                    pending_transaction_system_members: BTreeMap::new(),
                    pending_transaction_system_incarnations: BTreeMap::new(),
                    wal_root: Some(wal_root.clone()),
                    control_root_version: *control_root_version,
                    fenced_log_index: 0,
                    recovered_log_index: 0,
                    fenced_log_position: None,
                    recovered_log_position: None,
                    last_completed_generation: *generation,
                    last_completed_recovery_id: None,
                    membership_epoch: 0,
                    pending_reconfiguration: None,
                    completed_reconfigurations: BTreeMap::new(),
                };
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Prepare {
                expected_generation,
                next_generation,
                expected_control_root_version,
                recovery_id,
                next_transaction_system_id,
                next_transaction_system_members,
                next_transaction_system_incarnations,
            } => {
                let normal = self.phase == GenerationPhase::Active
                    && self.pending_reconfiguration.is_none()
                    && *expected_generation == self.generation
                    && *next_generation == self.generation.saturating_add(1)
                    && *expected_control_root_version == self.control_root_version;
                let competing = faults.accept_competing_recovery
                    && self.phase == GenerationPhase::Fencing
                    && *next_generation == self.generation
                    && expected_generation.saturating_add(1) == self.generation
                    && *expected_control_root_version == self.control_root_version;
                if !normal && !competing {
                    return if *expected_generation < self.generation {
                        GenerationCommandStatus::StaleGeneration
                    } else if matches!(
                        self.phase,
                        GenerationPhase::Fencing | GenerationPhase::Recovering
                    ) {
                        GenerationCommandStatus::RecoveryConflict
                    } else {
                        GenerationCommandStatus::CompareFailed
                    };
                }
                if *recovery_id == 0
                    || next_transaction_system_id.is_empty()
                    || !valid_members_with_optional_incarnations(
                        next_transaction_system_members,
                        next_transaction_system_incarnations,
                    )
                {
                    return GenerationCommandStatus::InvalidRequest;
                }
                self.generation = *next_generation;
                self.phase = GenerationPhase::Fencing;
                self.recovery_id = Some(*recovery_id);
                self.pending_transaction_system_id = Some(next_transaction_system_id.clone());
                self.pending_transaction_system_members = next_transaction_system_members.clone();
                self.pending_transaction_system_incarnations =
                    next_transaction_system_incarnations.clone();
                self.fenced_log_index = 0;
                self.recovered_log_index = 0;
                self.fenced_log_position = None;
                self.recovered_log_position = None;
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Reserve {
                generation,
                recovery_id,
                transaction_system_id,
                expected_control_root_version,
                certificate,
            } => {
                if self.phase != GenerationPhase::Fencing || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if self.recovery_id != Some(*recovery_id)
                    || self.pending_transaction_system_id.as_deref()
                        != Some(transaction_system_id.as_str())
                {
                    return GenerationCommandStatus::RecoveryConflict;
                }
                if *expected_control_root_version != self.control_root_version {
                    return GenerationCommandStatus::CompareFailed;
                }
                let Some(certificate) = certificate else {
                    return GenerationCommandStatus::MissingRecoveryProof;
                };
                if !faults.accept_invalid_recovery_certificate
                    && !self.verify_recovery_certificate(
                        certificate,
                        RecoveryCertificateKind::Fence,
                        &self.transaction_system_members,
                    )
                {
                    return GenerationCommandStatus::InvalidRecoveryProof;
                }
                self.phase = GenerationPhase::Recovering;
                self.fenced_log_index = certificate.statement.log_position.index;
                self.fenced_log_position = Some(certificate.statement.log_position);
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Activate {
                generation,
                recovery_id,
                transaction_system_id,
                wal_root,
                expected_control_root_version,
                next_control_root_version,
                certificate,
            } => {
                if self.phase != GenerationPhase::Recovering || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if self.recovery_id != Some(*recovery_id)
                    || self.pending_transaction_system_id.as_deref()
                        != Some(transaction_system_id.as_str())
                {
                    return GenerationCommandStatus::RecoveryConflict;
                }
                if *expected_control_root_version != self.control_root_version
                    || *next_control_root_version != self.control_root_version.saturating_add(1)
                {
                    return GenerationCommandStatus::CompareFailed;
                }
                if transaction_system_id.is_empty() || wal_root.is_empty() {
                    return GenerationCommandStatus::InvalidRequest;
                }
                let Some(certificate) = certificate else {
                    if faults.activate_without_recovery_proof {
                        self.phase = GenerationPhase::Active;
                        self.last_completed_recovery_id = self.recovery_id;
                        self.recovery_id = None;
                        self.transaction_system_id = Some(transaction_system_id.clone());
                        self.transaction_system_members =
                            self.pending_transaction_system_members.clone();
                        self.transaction_system_incarnations =
                            self.pending_transaction_system_incarnations.clone();
                        self.pending_transaction_system_id = None;
                        self.pending_transaction_system_members.clear();
                        self.pending_transaction_system_incarnations.clear();
                        self.wal_root = Some(wal_root.clone());
                        self.control_root_version = *next_control_root_version;
                        self.last_completed_generation = *generation;
                        return GenerationCommandStatus::Accepted;
                    }
                    return GenerationCommandStatus::MissingRecoveryProof;
                };
                if !faults.accept_invalid_recovery_certificate
                    && !self.verify_recovery_certificate(
                        certificate,
                        RecoveryCertificateKind::Recovered,
                        &self.pending_transaction_system_members,
                    )
                {
                    return GenerationCommandStatus::InvalidRecoveryProof;
                }
                self.phase = GenerationPhase::Active;
                self.last_completed_recovery_id = self.recovery_id;
                self.recovery_id = None;
                self.transaction_system_id = Some(transaction_system_id.clone());
                self.transaction_system_members = self.pending_transaction_system_members.clone();
                self.transaction_system_incarnations =
                    self.pending_transaction_system_incarnations.clone();
                self.pending_transaction_system_id = None;
                self.pending_transaction_system_members.clear();
                self.pending_transaction_system_incarnations.clear();
                self.wal_root = Some(wal_root.clone());
                self.control_root_version = *next_control_root_version;
                self.recovered_log_index = certificate.statement.log_position.index;
                self.recovered_log_position = Some(certificate.statement.log_position);
                self.last_completed_generation = *generation;
                GenerationCommandStatus::Accepted
            }
            GenerationAction::PrepareRoutineReconfiguration {
                expected_generation,
                expected_membership_epoch,
                expected_membership_sha256,
                reconfiguration_id,
                replacement_node,
                replacement_incarnation,
                next_transaction_system_members,
                next_transaction_system_incarnations,
            } => {
                let next_digest = routine_membership_digest(
                    next_transaction_system_members,
                    next_transaction_system_incarnations,
                );
                if let Some(completed) = self.completed_reconfigurations.get(reconfiguration_id) {
                    return if completed.membership_sha256 == next_digest
                        && completed.membership_epoch == expected_membership_epoch.saturating_add(1)
                    {
                        GenerationCommandStatus::Accepted
                    } else {
                        GenerationCommandStatus::ReconfigurationConflict
                    };
                }
                if self.phase != GenerationPhase::Active || *expected_generation != self.generation
                {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if *expected_membership_epoch != self.membership_epoch {
                    return GenerationCommandStatus::StaleMembershipEpoch;
                }
                let active_digest = routine_membership_digest(
                    &self.transaction_system_members,
                    &self.transaction_system_incarnations,
                );
                if *expected_membership_sha256 != active_digest {
                    return GenerationCommandStatus::CompareFailed;
                }
                if *reconfiguration_id == 0
                    || *replacement_node == 0
                    || *replacement_incarnation == [0; 16]
                    || !valid_routine_replacement(
                        &self.transaction_system_members,
                        &self.transaction_system_incarnations,
                        next_transaction_system_members,
                        next_transaction_system_incarnations,
                        *replacement_node,
                        *replacement_incarnation,
                    )
                {
                    return GenerationCommandStatus::InvalidRequest;
                }
                let candidate = PendingRoutineReconfiguration {
                    reconfiguration_id: *reconfiguration_id,
                    expected_membership_epoch: *expected_membership_epoch,
                    phase: RoutineReconfigurationPhase::Prepared,
                    replacement_node: *replacement_node,
                    replacement_incarnation: *replacement_incarnation,
                    next_transaction_system_members: next_transaction_system_members.clone(),
                    next_transaction_system_incarnations: next_transaction_system_incarnations
                        .clone(),
                    learner_snapshot_position: None,
                    learner_applied_position: None,
                };
                if let Some(pending) = &self.pending_reconfiguration {
                    return if pending == &candidate {
                        GenerationCommandStatus::Accepted
                    } else {
                        GenerationCommandStatus::ReconfigurationConflict
                    };
                }
                self.pending_reconfiguration = Some(candidate);
                GenerationCommandStatus::Accepted
            }
            GenerationAction::MarkRoutineLearnerReady {
                generation,
                membership_epoch,
                reconfiguration_id,
                certificate,
            } => {
                if self.phase != GenerationPhase::Active || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if *membership_epoch != self.membership_epoch {
                    return GenerationCommandStatus::StaleMembershipEpoch;
                }
                let Some(pending) = self.pending_reconfiguration.as_ref() else {
                    return GenerationCommandStatus::ReconfigurationConflict;
                };
                if pending.reconfiguration_id != *reconfiguration_id {
                    return GenerationCommandStatus::ReconfigurationConflict;
                }
                if pending.phase == RoutineReconfigurationPhase::LearnerReady {
                    return certificate.as_ref().map_or(
                        GenerationCommandStatus::MissingRoutineProof,
                        |certificate| {
                            if pending.learner_snapshot_position
                                == Some(certificate.statement.snapshot_position)
                                && pending.learner_applied_position
                                    == Some(certificate.statement.applied_position)
                            {
                                GenerationCommandStatus::Accepted
                            } else {
                                GenerationCommandStatus::ReconfigurationConflict
                            }
                        },
                    );
                }
                let Some(certificate) = certificate else {
                    return GenerationCommandStatus::MissingRoutineProof;
                };
                if !self.verify_routine_certificate(
                    certificate,
                    RoutineReconfigurationCertificateKind::LearnerReady,
                ) {
                    return GenerationCommandStatus::InvalidRoutineProof;
                }
                let pending = self
                    .pending_reconfiguration
                    .as_mut()
                    .expect("pending reconfiguration was verified above");
                pending.phase = RoutineReconfigurationPhase::LearnerReady;
                pending.learner_snapshot_position = Some(certificate.statement.snapshot_position);
                pending.learner_applied_position = Some(certificate.statement.applied_position);
                GenerationCommandStatus::Accepted
            }
            GenerationAction::FinalizeRoutineReconfiguration {
                generation,
                expected_membership_epoch,
                reconfiguration_id,
                certificate,
            } => {
                if let Some(completed) = self.completed_reconfigurations.get(reconfiguration_id) {
                    return certificate.as_ref().map_or(
                        GenerationCommandStatus::MissingRoutineProof,
                        |certificate| {
                            let certificate_sha256 = routine_certificate_digest(certificate);
                            if completed.membership_epoch
                                == expected_membership_epoch.saturating_add(1)
                                && completed.membership_position
                                    == certificate.statement.applied_position
                                && completed.membership_sha256
                                    == certificate.statement.next_membership_sha256
                                && certificate_sha256 == Some(completed.certificate_sha256)
                                && verify_routine_quorum(
                                    certificate,
                                    &self.transaction_system_members,
                                )
                            {
                                GenerationCommandStatus::Accepted
                            } else {
                                GenerationCommandStatus::ReconfigurationConflict
                            }
                        },
                    );
                }
                if self.phase != GenerationPhase::Active || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if *expected_membership_epoch != self.membership_epoch {
                    return GenerationCommandStatus::StaleMembershipEpoch;
                }
                let Some(pending) = self.pending_reconfiguration.as_ref() else {
                    return GenerationCommandStatus::ReconfigurationConflict;
                };
                if pending.reconfiguration_id != *reconfiguration_id
                    || pending.phase != RoutineReconfigurationPhase::LearnerReady
                {
                    return GenerationCommandStatus::ReconfigurationConflict;
                }
                let Some(certificate) = certificate else {
                    return GenerationCommandStatus::MissingRoutineProof;
                };
                if !self.verify_routine_certificate(
                    certificate,
                    RoutineReconfigurationCertificateKind::MembershipCommitted,
                ) {
                    return GenerationCommandStatus::InvalidRoutineProof;
                }
                let pending = self
                    .pending_reconfiguration
                    .take()
                    .expect("pending reconfiguration was verified above");
                self.transaction_system_members = pending.next_transaction_system_members;
                self.transaction_system_incarnations = pending.next_transaction_system_incarnations;
                self.membership_epoch = self.membership_epoch.saturating_add(1);
                let certificate_sha256 = routine_certificate_digest(certificate)
                    .expect("verified certificate has canonical bytes");
                self.completed_reconfigurations.insert(
                    *reconfiguration_id,
                    CompletedRoutineReconfiguration {
                        reconfiguration_id: *reconfiguration_id,
                        membership_epoch: self.membership_epoch,
                        membership_sha256: certificate.statement.next_membership_sha256,
                        membership_position: certificate.statement.applied_position,
                        certificate_sha256,
                    },
                );
                GenerationCommandStatus::Accepted
            }
        }
    }
}

fn valid_recovery_members(members: &BTreeMap<u64, Vec<u8>>) -> bool {
    !members.is_empty()
        && members
            .iter()
            .all(|(node_id, public_key)| *node_id != 0 && public_key.len() == 32)
}

fn valid_routine_members(
    members: &BTreeMap<u64, Vec<u8>>,
    incarnations: &BTreeMap<u64, [u8; 16]>,
) -> bool {
    valid_recovery_members(members)
        && members.keys().eq(incarnations.keys())
        && incarnations
            .values()
            .all(|incarnation| *incarnation != [0; 16])
}

fn valid_members_with_optional_incarnations(
    members: &BTreeMap<u64, Vec<u8>>,
    incarnations: &BTreeMap<u64, [u8; 16]>,
) -> bool {
    valid_recovery_members(members)
        && (incarnations.is_empty() || valid_routine_members(members, incarnations))
}

fn valid_routine_replacement(
    old_members: &BTreeMap<u64, Vec<u8>>,
    old_incarnations: &BTreeMap<u64, [u8; 16]>,
    next_members: &BTreeMap<u64, Vec<u8>>,
    next_incarnations: &BTreeMap<u64, [u8; 16]>,
    replacement_node: u64,
    replacement_incarnation: [u8; 16],
) -> bool {
    if !valid_routine_members(old_members, old_incarnations)
        || !valid_routine_members(next_members, next_incarnations)
        || old_members.len() != next_members.len()
        || old_members.contains_key(&replacement_node)
        || next_incarnations.get(&replacement_node) != Some(&replacement_incarnation)
        || old_incarnations
            .values()
            .any(|incarnation| incarnation == &replacement_incarnation)
    {
        return false;
    }
    let removed = old_members
        .keys()
        .filter(|node_id| !next_members.contains_key(node_id))
        .count();
    let added = next_members
        .keys()
        .filter(|node_id| !old_members.contains_key(node_id))
        .count();
    removed == 1 && added == 1 && next_members.contains_key(&replacement_node)
}

fn routine_certificate_digest(certificate: &RoutineReconfigurationCertificate) -> Option<[u8; 32]> {
    certificate
        .statement
        .signing_bytes()
        .ok()
        .map(|bytes| Sha256::digest(bytes).into())
}

fn verify_routine_quorum(
    certificate: &RoutineReconfigurationCertificate,
    members: &BTreeMap<u64, Vec<u8>>,
) -> bool {
    let Ok(bytes) = certificate.statement.signing_bytes() else {
        return false;
    };
    let mut distinct_signers = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct_signers.insert(attestation.signer_id) {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct_signers.len() >= quorum_size(members.len())
}

const fn quorum_size(member_count: usize) -> usize {
    member_count / 2 + 1
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One command replicated by the coordinator's `OpenRaft` log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationCommand {
    pub identity: RequestIdentity,
    pub action: GenerationAction,
}

impl GenerationCommand {
    /// Encode this command into objectKV-owned application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = GENERATION_COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(GENERATION_COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// State transition requested from the coordinator quorum.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GenerationAction {
    Bootstrap {
        cell_id: u64,
        generation: u64,
        transaction_system_id: String,
        #[serde(with = "member_map_wire")]
        transaction_system_members: BTreeMap<u64, Vec<u8>>,
        #[serde(default, with = "incarnation_map_wire")]
        transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
        wal_root: String,
        control_root_version: u64,
    },
    Prepare {
        expected_generation: u64,
        next_generation: u64,
        expected_control_root_version: u64,
        recovery_id: u64,
        next_transaction_system_id: String,
        #[serde(with = "member_map_wire")]
        next_transaction_system_members: BTreeMap<u64, Vec<u8>>,
        #[serde(default, with = "incarnation_map_wire")]
        next_transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
    },
    Reserve {
        generation: u64,
        recovery_id: u64,
        transaction_system_id: String,
        expected_control_root_version: u64,
        certificate: Option<RecoveryCertificate>,
    },
    Activate {
        generation: u64,
        recovery_id: u64,
        transaction_system_id: String,
        wal_root: String,
        expected_control_root_version: u64,
        next_control_root_version: u64,
        certificate: Option<RecoveryCertificate>,
    },
    PrepareRoutineReconfiguration {
        expected_generation: u64,
        expected_membership_epoch: u64,
        expected_membership_sha256: [u8; 32],
        reconfiguration_id: u64,
        replacement_node: u64,
        replacement_incarnation: [u8; 16],
        #[serde(with = "member_map_wire")]
        next_transaction_system_members: BTreeMap<u64, Vec<u8>>,
        #[serde(with = "incarnation_map_wire")]
        next_transaction_system_incarnations: BTreeMap<u64, [u8; 16]>,
    },
    MarkRoutineLearnerReady {
        generation: u64,
        membership_epoch: u64,
        reconfiguration_id: u64,
        certificate: Option<RoutineReconfigurationCertificate>,
    },
    FinalizeRoutineReconfiguration {
        generation: u64,
        expected_membership_epoch: u64,
        reconfiguration_id: u64,
        certificate: Option<RoutineReconfigurationCertificate>,
    },
}

/// Stable semantic result of a coordinator command.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationCommandStatus {
    Accepted,
    StaleGeneration,
    CompareFailed,
    RecoveryConflict,
    MissingRecoveryProof,
    InvalidRecoveryProof,
    StaleMembershipEpoch,
    ReconfigurationConflict,
    MissingRoutineProof,
    InvalidRoutineProof,
    InvalidPhase,
    InvalidRequest,
}

/// Response reconstructed by replaying the coordinator log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationApplyResponse {
    pub status: GenerationCommandStatus,
    pub state: GenerationAuthorityState,
    pub applied_log_index: u64,
    pub applied_log_position: RecoveryLogPosition,
}

/// Bounded unsafe authority behaviors used only by negative-control nodes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationAuthorityFaults {
    pub accept_competing_recovery: bool,
    pub activate_without_recovery_proof: bool,
    pub accept_invalid_recovery_certificate: bool,
}

/// Role hosted by one real-process consensus node.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusProcessRole {
    #[default]
    Data,
    GenerationAuthority,
}

/// Generation identity presented by a transaction-system process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationCredential {
    pub generation: u64,
    pub transaction_system_id: String,
}

/// Coordinator endpoints and local identity used to fence data commits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationFenceConfig {
    pub credential: GenerationCredential,
    pub recovery_id: Option<u64>,
    pub authority_nodes: BTreeMap<u64, String>,
}

/// Test-harness signing material installed in one data process.
///
/// Production deployments must load equivalent key material through a secret
/// provider rather than process arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoverySignerConfig {
    pub private_key_seed: Vec<u8>,
}

/// Bounded unsafe transaction-system behaviors used by negative controls.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationFenceFaults {
    pub bypass_commit_fence: bool,
    pub bypass_apply_fence: bool,
    pub accept_apply_during_recovery: bool,
    pub accept_recovering_commits: bool,
    pub allow_preauthorized_test_write: bool,
    pub accept_incomplete_staged_head: bool,
    pub ignore_staged_head_takeover_expectation: bool,
    pub allow_successor_to_skip_staged_head: bool,
    pub accept_invalid_staged_abort_proof: bool,
    pub reuse_aborted_sequence_or_chain: bool,
    pub publish_beyond_staged_absence: bool,
    pub abort_quorum_present_staged_record: bool,
    pub skip_recoverable_staged_prefix: bool,
    pub retain_aborted_staged_suffix: bool,
    pub accept_over_limit_staged_window: bool,
    pub accept_missing_staged_inventory: bool,
    pub ratekeeper_accept_best_node_capacity: bool,
    pub ratekeeper_accept_stale_sample: bool,
    pub ratekeeper_allow_stage_without_reservation: bool,
    pub policy_transition_accept_missing_readiness: bool,
    pub policy_transition_accept_unresolved_stage: bool,
    pub policy_transition_accept_invalid_next_policy: bool,
    pub policy_transition_accept_mixed_stage_quorum: bool,
    pub policy_transition_double_apply: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(action: GenerationAction) -> GenerationCommand {
        GenerationCommand {
            identity: RequestIdentity {
                client_id: 9,
                request_id: 1,
            },
            action,
        }
    }

    fn seed(node_id: u8) -> Vec<u8> {
        vec![node_id; 32]
    }

    fn members(node_ids: &[u8]) -> BTreeMap<u64, Vec<u8>> {
        node_ids
            .iter()
            .map(|node_id| {
                (
                    u64::from(*node_id),
                    recovery_public_key(&seed(*node_id)).unwrap(),
                )
            })
            .collect()
    }

    fn incarnations(node_ids: &[u8]) -> BTreeMap<u64, [u8; 16]> {
        node_ids
            .iter()
            .map(|node_id| (u64::from(*node_id), [*node_id; 16]))
            .collect()
    }

    fn certificate(
        kind: RecoveryCertificateKind,
        state: &GenerationAuthorityState,
        position: RecoveryLogPosition,
        voters: &BTreeMap<u64, Vec<u8>>,
        signers: &[u8],
    ) -> RecoveryCertificate {
        let statement = RecoveryCertificateStatement::new(kind, state, position, voters);
        RecoveryCertificate {
            attestations: signers
                .iter()
                .map(|node_id| {
                    sign_recovery_statement(u64::from(*node_id), &seed(*node_id), &statement)
                        .unwrap()
                })
                .collect(),
            statement,
        }
    }

    fn routine_certificate(
        kind: RoutineReconfigurationCertificateKind,
        state: &GenerationAuthorityState,
        snapshot_position: RecoveryLogPosition,
        applied_position: RecoveryLogPosition,
        signers: &[u8],
    ) -> RoutineReconfigurationCertificate {
        let statement = RoutineReconfigurationCertificateStatement::new(
            kind,
            state,
            snapshot_position,
            applied_position,
        )
        .unwrap();
        RoutineReconfigurationCertificate {
            attestations: signers
                .iter()
                .map(|node_id| {
                    sign_routine_reconfiguration_statement(
                        u64::from(*node_id),
                        &seed(*node_id),
                        &statement,
                    )
                    .unwrap()
                })
                .collect(),
            statement,
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn reserve_fences_active_generation_until_proven_activation() {
        let mut state = GenerationAuthorityState::default();
        let generation_one_members = members(&[1, 2, 3]);
        let generation_two_members = members(&[4, 5, 6]);
        let bootstrap = command(GenerationAction::Bootstrap {
            cell_id: 7,
            generation: 1,
            transaction_system_id: "tx-g1".to_owned(),
            transaction_system_members: generation_one_members.clone(),
            transaction_system_incarnations: incarnations(&[1, 2, 3]),
            wal_root: "wal-g1".to_owned(),
            control_root_version: 1,
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&bootstrap.action, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes(1, "tx-g1"));

        let prepare = command(GenerationAction::Prepare {
            expected_generation: 1,
            next_generation: 2,
            expected_control_root_version: 1,
            recovery_id: 22,
            next_transaction_system_id: "tx-g2".to_owned(),
            next_transaction_system_members: generation_two_members.clone(),
            next_transaction_system_incarnations: incarnations(&[4, 5, 6]),
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&prepare.action, GenerationAuthorityFaults::default())
        );
        assert!(!state.authorizes(1, "tx-g1"));
        assert!(!state.authorizes(2, "tx-g2"));
        assert!(state.authorizes_fencing(2, 22, "tx-g2"));

        let fence_certificate = certificate(
            RecoveryCertificateKind::Fence,
            &state,
            RecoveryLogPosition { term: 1, index: 17 },
            &generation_one_members,
            &[1, 2],
        );
        let mut single_signer = fence_certificate.clone();
        single_signer.attestations.truncate(1);
        let invalid_reserve = command(GenerationAction::Reserve {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            expected_control_root_version: 1,
            certificate: Some(single_signer),
        });
        assert_eq!(
            GenerationCommandStatus::InvalidRecoveryProof,
            state.apply(
                &invalid_reserve.action,
                GenerationAuthorityFaults::default()
            )
        );
        let reserve = command(GenerationAction::Reserve {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            expected_control_root_version: 1,
            certificate: Some(fence_certificate),
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&reserve.action, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes_recovery(2, 22, "tx-g2"));

        let without_proof = command(GenerationAction::Activate {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: None,
        });
        assert_eq!(
            GenerationCommandStatus::MissingRecoveryProof,
            state.apply(&without_proof.action, GenerationAuthorityFaults::default())
        );
        let recovery_certificate = certificate(
            RecoveryCertificateKind::Recovered,
            &state,
            RecoveryLogPosition { term: 1, index: 19 },
            &generation_two_members,
            &[4, 5],
        );
        let with_proof = GenerationAction::Activate {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: Some(recovery_certificate),
        };
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&with_proof, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes(2, "tx-g2"));
    }

    #[test]
    fn generation_command_round_trips_numeric_member_ids() {
        let command = command(GenerationAction::Bootstrap {
            cell_id: 7,
            generation: 1,
            transaction_system_id: "tx-g1".to_owned(),
            transaction_system_members: members(&[1, 2, 3]),
            transaction_system_incarnations: incarnations(&[1, 2, 3]),
            wal_root: "wal-g1".to_owned(),
            control_root_version: 1,
        });
        let encoded = command.encode().unwrap();
        assert_eq!(Some(command), GenerationCommand::decode(&encoded).unwrap());
    }

    #[test]
    fn legacy_generation_command_decodes_without_storage_incarnations() {
        let command = command(GenerationAction::Bootstrap {
            cell_id: 7,
            generation: 1,
            transaction_system_id: "tx-g1".to_owned(),
            transaction_system_members: members(&[1, 2, 3]),
            transaction_system_incarnations: incarnations(&[1, 2, 3]),
            wal_root: "wal-g1".to_owned(),
            control_root_version: 1,
        });
        let mut legacy = serde_json::to_value(command).unwrap();
        legacy
            .get_mut("action")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("transaction_system_incarnations");
        let legacy: GenerationCommand = serde_json::from_value(legacy).unwrap();
        let mut state = GenerationAuthorityState::default();
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&legacy.action, GenerationAuthorityFaults::default())
        );
        assert!(state.transaction_system_incarnations.is_empty());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn routine_reconfiguration_preserves_generation_and_finalizes_once() {
        let mut state = GenerationAuthorityState::default();
        let old_members = members(&[1, 2, 3]);
        let old_incarnations = incarnations(&[1, 2, 3]);
        let next_members = members(&[2, 3, 4]);
        let next_incarnations = incarnations(&[2, 3, 4]);
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(
                &GenerationAction::Bootstrap {
                    cell_id: 7,
                    generation: 9,
                    transaction_system_id: "tx-g9".to_owned(),
                    transaction_system_members: old_members.clone(),
                    transaction_system_incarnations: old_incarnations.clone(),
                    wal_root: "wal-g9".to_owned(),
                    control_root_version: 1,
                },
                GenerationAuthorityFaults::default(),
            )
        );
        let active_digest = routine_membership_digest(&old_members, &old_incarnations);
        let prepare = GenerationAction::PrepareRoutineReconfiguration {
            expected_generation: 9,
            expected_membership_epoch: 0,
            expected_membership_sha256: active_digest,
            reconfiguration_id: 77,
            replacement_node: 4,
            replacement_incarnation: [4; 16],
            next_transaction_system_members: next_members.clone(),
            next_transaction_system_incarnations: next_incarnations.clone(),
        };
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&prepare, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes_routine_learner(9, 0, 77, 4, [4; 16]));
        assert!(!state.authorizes_routine_membership(9, 0, 77, &BTreeSet::from([2, 3, 4]),));

        let conflicting = GenerationAction::PrepareRoutineReconfiguration {
            expected_generation: 9,
            expected_membership_epoch: 0,
            expected_membership_sha256: active_digest,
            reconfiguration_id: 78,
            replacement_node: 5,
            replacement_incarnation: [5; 16],
            next_transaction_system_members: members(&[1, 3, 5]),
            next_transaction_system_incarnations: incarnations(&[1, 3, 5]),
        };
        assert_eq!(
            GenerationCommandStatus::ReconfigurationConflict,
            state.apply(&conflicting, GenerationAuthorityFaults::default())
        );

        let ready_certificate = routine_certificate(
            RoutineReconfigurationCertificateKind::LearnerReady,
            &state,
            RecoveryLogPosition { term: 2, index: 8 },
            RecoveryLogPosition { term: 2, index: 11 },
            &[1, 2, 4],
        );
        let mut missing_replacement = ready_certificate.clone();
        missing_replacement
            .attestations
            .retain(|attestation| attestation.signer_id != 4);
        assert_eq!(
            GenerationCommandStatus::InvalidRoutineProof,
            state.apply(
                &GenerationAction::MarkRoutineLearnerReady {
                    generation: 9,
                    membership_epoch: 0,
                    reconfiguration_id: 77,
                    certificate: Some(missing_replacement),
                },
                GenerationAuthorityFaults::default(),
            )
        );
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(
                &GenerationAction::MarkRoutineLearnerReady {
                    generation: 9,
                    membership_epoch: 0,
                    reconfiguration_id: 77,
                    certificate: Some(ready_certificate),
                },
                GenerationAuthorityFaults::default(),
            )
        );
        assert!(state.authorizes_routine_membership(9, 0, 77, &BTreeSet::from([2, 3, 4]),));

        let committed_certificate = routine_certificate(
            RoutineReconfigurationCertificateKind::MembershipCommitted,
            &state,
            RecoveryLogPosition { term: 2, index: 8 },
            RecoveryLogPosition { term: 3, index: 13 },
            &[2, 3],
        );
        let finalize = GenerationAction::FinalizeRoutineReconfiguration {
            generation: 9,
            expected_membership_epoch: 0,
            reconfiguration_id: 77,
            certificate: Some(committed_certificate),
        };
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&finalize, GenerationAuthorityFaults::default())
        );
        assert_eq!(9, state.generation);
        assert_eq!(1, state.membership_epoch);
        assert!(state.authorizes_node(9, "tx-g9", 4));
        assert!(!state.authorizes_node(9, "tx-g9", 1));
        assert_eq!(next_members, state.transaction_system_members);
        assert_eq!(next_incarnations, state.transaction_system_incarnations);

        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&finalize, GenerationAuthorityFaults::default())
        );
        assert_eq!(1, state.membership_epoch);
    }
}
