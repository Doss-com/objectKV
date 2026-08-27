use crate::{
    ObjectFrontierAttestation, ObjectFrontierCertificate, ObjectFrontierCertificateStatement,
    ObjectFrontierLogPosition, ObjectFrontierRecord, RequestIdentity,
    OBJECT_FRONTIER_CERTIFICATE_VERSION,
};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const GENERATION_COMMAND_MAGIC: &[u8] = b"OKVG1";
const RECOVERY_CERTIFICATE_MAGIC: &[u8] = b"OKV-RECOVERY-CERTIFICATE-V1\0";
const RECOVERY_CERTIFICATE_VERSION: u16 = 1;
const OBJECT_FRONTIER_CERTIFICATE_MAGIC: &[u8] = b"OKV-OBJECT-FRONTIER-CERTIFICATE-V1\0";

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

/// Compute the stable digest of the voter set authorized to certify a physical
/// object-frontier apply.
#[must_use]
pub fn object_frontier_membership_digest(members: &BTreeMap<u64, Vec<u8>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"OKV-OBJECT-FRONTIER-MEMBERSHIP-V1\0");
    digest.update((members.len() as u64).to_be_bytes());
    for (node_id, public_key) in members {
        digest.update(node_id.to_be_bytes());
        digest.update((public_key.len() as u64).to_be_bytes());
        digest.update(public_key);
    }
    digest.finalize().into()
}

/// Construct the only object-frontier statement admissible for one active
/// generation and applied data-log position.
#[must_use]
pub fn object_frontier_certificate_statement(
    state: &GenerationAuthorityState,
    frontier: ObjectFrontierRecord,
    data_log_position: ObjectFrontierLogPosition,
) -> ObjectFrontierCertificateStatement {
    ObjectFrontierCertificateStatement {
        protocol_version: OBJECT_FRONTIER_CERTIFICATE_VERSION,
        cell_id: state.cell_id,
        generation: state.generation,
        transaction_system_id: state.transaction_system_id.clone().unwrap_or_default(),
        frontier,
        data_log_position,
        membership_sha256: object_frontier_membership_digest(&state.transaction_system_members),
    }
}

fn object_frontier_signing_bytes(
    statement: &ObjectFrontierCertificateStatement,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = OBJECT_FRONTIER_CERTIFICATE_MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(statement)?);
    Ok(bytes)
}

/// Sign one canonical object-frontier statement for a configured data voter.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_object_frontier_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &ObjectFrontierCertificateStatement,
) -> Result<ObjectFrontierAttestation, String> {
    let key_pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "object-frontier signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = object_frontier_signing_bytes(statement).map_err(|error| error.to_string())?;
    Ok(ObjectFrontierAttestation {
        signer_id,
        signature: key_pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Verify a data-quorum proof against the exact active generation membership.
#[must_use]
pub fn verify_object_frontier_certificate(
    certificate: &ObjectFrontierCertificate,
    state: &GenerationAuthorityState,
) -> bool {
    let statement = &certificate.statement;
    if statement.protocol_version != OBJECT_FRONTIER_CERTIFICATE_VERSION
        || state.phase != GenerationPhase::Active
        || statement.cell_id != state.cell_id
        || statement.generation != state.generation
        || statement.frontier.owner_generation != state.generation
        || Some(statement.transaction_system_id.as_str()) != state.transaction_system_id.as_deref()
        || !statement.data_log_position.is_valid()
        || !valid_recovery_members(&state.transaction_system_members)
        || statement.membership_sha256
            != object_frontier_membership_digest(&state.transaction_system_members)
    {
        return false;
    }
    let Ok(bytes) = object_frontier_signing_bytes(statement) else {
        return false;
    };
    let mut distinct_signers = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct_signers.insert(attestation.signer_id) {
            return false;
        }
        let Some(public_key) = state.transaction_system_members.get(&attestation.signer_id) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct_signers.len() >= quorum_size(state.transaction_system_members.len())
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
    #[serde(with = "member_map_wire")]
    pub pending_transaction_system_members: BTreeMap<u64, Vec<u8>>,
    pub wal_root: Option<String>,
    pub control_root_version: u64,
    pub fenced_log_index: u64,
    pub recovered_log_index: u64,
    pub fenced_log_position: Option<RecoveryLogPosition>,
    pub recovered_log_position: Option<RecoveryLogPosition>,
    pub last_completed_generation: u64,
}

impl GenerationAuthorityState {
    /// Whether a linearizable coordinator read authorizes one client commit.
    #[must_use]
    pub fn authorizes(&self, generation: u64, transaction_system_id: &str) -> bool {
        self.phase == GenerationPhase::Active
            && self.generation == generation
            && self.transaction_system_id.as_deref() == Some(transaction_system_id)
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
                wal_root,
                control_root_version,
            } => {
                if self.phase != GenerationPhase::Uninitialized
                    || *cell_id == 0
                    || *generation == 0
                    || transaction_system_id.is_empty()
                    || !valid_recovery_members(transaction_system_members)
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
                    pending_transaction_system_members: BTreeMap::new(),
                    wal_root: Some(wal_root.clone()),
                    control_root_version: *control_root_version,
                    fenced_log_index: 0,
                    recovered_log_index: 0,
                    fenced_log_position: None,
                    recovered_log_position: None,
                    last_completed_generation: *generation,
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
            } => {
                let normal = self.phase == GenerationPhase::Active
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
                    || !valid_recovery_members(next_transaction_system_members)
                {
                    return GenerationCommandStatus::InvalidRequest;
                }
                self.generation = *next_generation;
                self.phase = GenerationPhase::Fencing;
                self.recovery_id = Some(*recovery_id);
                self.pending_transaction_system_id = Some(next_transaction_system_id.clone());
                self.pending_transaction_system_members = next_transaction_system_members.clone();
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
                        self.recovery_id = None;
                        self.transaction_system_id = Some(transaction_system_id.clone());
                        self.transaction_system_members =
                            self.pending_transaction_system_members.clone();
                        self.pending_transaction_system_id = None;
                        self.pending_transaction_system_members.clear();
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
                self.recovery_id = None;
                self.transaction_system_id = Some(transaction_system_id.clone());
                self.transaction_system_members = self.pending_transaction_system_members.clone();
                self.pending_transaction_system_id = None;
                self.pending_transaction_system_members.clear();
                self.wal_root = Some(wal_root.clone());
                self.control_root_version = *next_control_root_version;
                self.recovered_log_index = certificate.statement.log_position.index;
                self.recovered_log_position = Some(certificate.statement.log_position);
                self.last_completed_generation = *generation;
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

const fn quorum_size(member_count: usize) -> usize {
    member_count / 2 + 1
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

    #[test]
    fn object_frontier_certificate_requires_exact_distinct_data_quorum() {
        let voter_members = members(&[1, 2, 3]);
        let mut state = GenerationAuthorityState::default();
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(
                &GenerationAction::Bootstrap {
                    cell_id: 19,
                    generation: 7,
                    transaction_system_id: "data-g7".to_owned(),
                    transaction_system_members: voter_members.clone(),
                    wal_root: "wal-g7".to_owned(),
                    control_root_version: 1,
                },
                GenerationAuthorityFaults::default(),
            )
        );
        let frontier = ObjectFrontierRecord {
            owner_generation: 7,
            source_root: "range-main".to_owned(),
            manifest: crate::PublicationObjectReference {
                kind: crate::PublicationObjectKind::Manifest,
                key: "objects/frontier-manifest".to_owned(),
                length: 128,
                sha256: "d".repeat(64),
            },
            covered_through: 91,
            prepared_at: crate::PublicationAuthorityPosition { term: 3, index: 44 },
        };
        let statement = object_frontier_certificate_statement(
            &state,
            frontier,
            ObjectFrontierLogPosition { term: 8, index: 55 },
        );
        let certificate = ObjectFrontierCertificate {
            attestations: [1_u8, 2]
                .iter()
                .map(|node_id| {
                    sign_object_frontier_statement(u64::from(*node_id), &seed(*node_id), &statement)
                        .unwrap()
                })
                .collect(),
            statement: statement.clone(),
        };
        assert!(verify_object_frontier_certificate(&certificate, &state));

        let mut subquorum = certificate.clone();
        subquorum.attestations.truncate(1);
        assert!(!verify_object_frontier_certificate(&subquorum, &state));

        let mut duplicate = certificate.clone();
        duplicate.attestations[1] = duplicate.attestations[0].clone();
        assert!(!verify_object_frontier_certificate(&duplicate, &state));

        let mut tampered = certificate;
        tampered.statement.frontier.covered_through = 92;
        assert!(!verify_object_frontier_certificate(&tampered, &state));
    }

    #[test]
    fn reserve_fences_active_generation_until_proven_activation() {
        let mut state = GenerationAuthorityState::default();
        let generation_one_members = members(&[1, 2, 3]);
        let generation_two_members = members(&[4, 5, 6]);
        let bootstrap = command(GenerationAction::Bootstrap {
            cell_id: 7,
            generation: 1,
            transaction_system_id: "tx-g1".to_owned(),
            transaction_system_members: generation_one_members.clone(),
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
            wal_root: "wal-g1".to_owned(),
            control_root_version: 1,
        });
        let encoded = command.encode().unwrap();
        assert_eq!(Some(command), GenerationCommand::decode(&encoded).unwrap());
    }
}
