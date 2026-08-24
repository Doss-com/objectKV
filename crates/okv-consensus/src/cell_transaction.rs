use crate::{
    GenerationAuthorityState, GenerationCredential, GenerationFenceFaults, RecoveryLogPosition,
    RequestIdentity,
};
use okv_model::Version;
use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const COMMAND_MAGIC: &[u8] = b"OKVT1";
const STAGED_COMMAND_MAGIC: &[u8] = b"OKVT2";
const TAGGED_LOG_CERTIFICATE_MAGIC: &[u8] = b"OKV-TAGGED-LOG-CERTIFICATE-V1\0";
const TAGGED_LOG_FENCE_CERTIFICATE_MAGIC: &[u8] = b"OKV-TAGGED-LOG-FENCE-CERTIFICATE-V1\0";
const TAGGED_LOG_PREFIX_FENCE_CERTIFICATE_MAGIC: &[u8] =
    b"OKV-TAGGED-LOG-PREFIX-FENCE-CERTIFICATE-V1\0";
const TAGGED_LOG_CAPACITY_CERTIFICATE_MAGIC: &[u8] = b"OKV-TAGGED-LOG-CAPACITY-CERTIFICATE-V1\0";
const TAGGED_LOG_POP_CERTIFICATE_MAGIC: &[u8] = b"OKV-TAGGED-LOG-POP-CERTIFICATE-V1\0";
const TAGGED_LOG_REPAIR_CERTIFICATE_MAGIC: &[u8] = b"OKV-TAGGED-LOG-REPAIR-CERTIFICATE-V1\0";
const TAGGED_LOG_POLICY_STAGE_CERTIFICATE_MAGIC: &[u8] =
    b"OKV-TAGGED-LOG-POLICY-STAGE-CERTIFICATE-V1\0";
const LOG_SET_POLICY_ACTIVATION_CERTIFICATE_MAGIC: &[u8] =
    b"OKV-LOG-SET-POLICY-ACTIVATION-CERTIFICATE-V1\0";
const STAGED_WINDOW_MAGIC: &[u8] = b"OKV-STAGED-WINDOW-V1\0";
const RESOLVER_DECISION_MAGIC: &[u8] = b"OKV-RESOLVER-DECISION-V1\0";
const MAX_STAGED_PREFIX_RECORDS: usize = 4;
const MAX_STAGED_PREFIX_BYTES: u64 = 16 * 1024;
const RESOLVER_SET_ID: [u8; 16] = [0x33; 16];
const REQUIRED_RESOLVERS: [u16; 2] = [1, 2];
const REQUIRED_LOG_TAGS: [u16; 2] = [10, 20];

mod row_map_serde {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S>(rows: &BTreeMap<Vec<u8>, Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        rows.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(Vec<u8>, Vec<u8>)>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let rows = entries.into_iter().collect::<BTreeMap<_, _>>();
        if rows.len() != entry_count {
            return Err(D::Error::custom(
                "state-machine snapshot contains duplicate row keys",
            ));
        }
        Ok(rows)
    }
}

/// A generation-aware snapshot version supplied by the read-version authority.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellReadVersion {
    pub generation: u64,
    pub sequence: u64,
}

impl CellReadVersion {
    #[must_use]
    pub const fn origin() -> Self {
        Self {
            generation: 0,
            sequence: 0,
        }
    }
}

/// A non-empty half-open conflict range.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CellKeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl CellKeyRange {
    #[must_use]
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            start: key.to_vec(),
            end,
        }
    }

    pub(crate) fn valid(&self) -> bool {
        self.start < self.end
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

/// Point mutations admitted by the centralized Cell v0 prototype.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellMutation {
    Clear { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
}

impl CellMutation {
    fn key(&self) -> &[u8] {
        match self {
            Self::Clear { key } | Self::Set { key, .. } => key,
        }
    }
}

/// One semantic transaction command carried directly by the Raft application log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTransactionCommand {
    pub identity: RequestIdentity,
    pub credential: Option<GenerationCredential>,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub read_version: CellReadVersion,
    pub read_conflicts: Vec<CellKeyRange>,
    pub write_conflicts: Vec<CellKeyRange>,
    pub mutations: Vec<CellMutation>,
    #[serde(default)]
    pub partitioned_resolution: Option<CellPartitionedResolution>,
    pub accepted_resolvers: Vec<u16>,
    pub durable_log_tags: Vec<u16>,
}

impl CellTransactionCommand {
    /// Encode a canonical transaction request into objectKV-owned Raft bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut canonical = self.clone();
        canonicalize_transaction(&mut canonical);
        let mut encoded = COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(&canonical)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

fn canonicalize_transaction(command: &mut CellTransactionCommand) {
    command.read_conflicts.sort();
    command.read_conflicts.dedup();
    command.write_conflicts.sort();
    command.write_conflicts.dedup();
    command.mutations.sort();
    command.mutations.dedup();
    if let Some(resolution) = &mut command.partitioned_resolution {
        resolution
            .attestations
            .sort_by_key(|attestation| attestation.statement.resolver_id);
    }
    command.accepted_resolvers.sort_unstable();
    command.accepted_resolvers.dedup();
    command.durable_log_tags.sort_unstable();
    command.durable_log_tags.dedup();
}

/// One ordered resolver partition in the bounded epoch-1 evaluation map.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellResolverPartition {
    pub resolver_id: u16,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// Durable local conflict decision made by one resolver partition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellResolverDecision {
    Accept,
    Conflict,
}

/// Exact transaction and map identity signed by one resolver process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellResolverDecisionStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub map_epoch: u64,
    pub map_sha256: [u8; 32],
    pub resolver_id: u16,
    pub resolver_incarnation: [u8; 16],
    pub transaction_identity: RequestIdentity,
    pub candidate_sequence: u64,
    pub read_version: CellReadVersion,
    /// Generation-local logical version used by the resolver conflict index.
    #[serde(default)]
    pub resolver_read_sequence: u64,
    pub transaction_sha256: [u8; 32],
    pub read_conflicts: Vec<CellKeyRange>,
    pub write_conflicts: Vec<CellKeyRange>,
    pub decision: CellResolverDecision,
}

impl CellResolverDecisionStatement {
    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = RESOLVER_DECISION_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// One signed and durably journaled resolver decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellResolverDecisionAttestation {
    pub statement: CellResolverDecisionStatement,
    pub signature: Vec<u8>,
}

/// Complete process-derived conflict result for one authority command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellPartitionedResolution {
    /// Transaction-system generation that produced the resolver evidence.
    ///
    /// Zero preserves the RFC-0048 encoding and resolves to the data generation.
    #[serde(default)]
    pub transaction_system_generation: u64,
    /// Generation-local logical version used for resolver conflict checks.
    ///
    /// Zero preserves RFC-0048 behavior and uses the command read sequence.
    #[serde(default)]
    pub resolver_read_sequence: u64,
    pub map_epoch: u64,
    pub candidate_sequence: u64,
    pub attestations: Vec<CellResolverDecisionAttestation>,
}

/// The fixed three-partition map used by RFC-0048's first bounded gate.
#[must_use]
pub fn cell_resolver_partitions() -> Vec<CellResolverPartition> {
    vec![
        CellResolverPartition {
            resolver_id: 1,
            start: vec![0x00],
            end: vec![0x50],
        },
        CellResolverPartition {
            resolver_id: 2,
            start: vec![0x50],
            end: vec![0xa0],
        },
        CellResolverPartition {
            resolver_id: 3,
            start: vec![0xa0],
            end: vec![0xf0],
        },
    ]
}

/// Canonical digest of the bounded resolver map.
///
/// # Panics
///
/// Panics only if the fixed, in-memory map cannot be serialized.
#[must_use]
pub fn cell_resolver_map_sha256() -> [u8; 32] {
    let bytes =
        serde_json::to_vec(&cell_resolver_partitions()).expect("fixed resolver map must serialize");
    Sha256::digest(bytes).into()
}

/// Deterministic evaluation-only resolver key seed for one process incarnation.
#[must_use]
pub fn cell_resolver_private_key_seed(
    resolver_id: u16,
    resolver_incarnation: [u8; 16],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"okv-eval-resolver-key-v1");
    digest.update(resolver_id.to_be_bytes());
    digest.update(resolver_incarnation);
    digest.finalize().into()
}

/// Fixed storage and process incarnation pinned by the first resolver-map epoch.
#[must_use]
pub fn cell_resolver_incarnation(resolver_id: u16) -> [u8; 16] {
    let mut incarnation = [0_u8; 16];
    incarnation[..2].copy_from_slice(&resolver_id.to_be_bytes());
    incarnation[2..].copy_from_slice(b"okv-resolver-v");
    incarnation
}

/// Derive the pinned public key for one evaluation resolver incarnation.
#[must_use]
pub fn cell_resolver_public_key(resolver_id: u16, resolver_incarnation: [u8; 16]) -> Vec<u8> {
    let seed = cell_resolver_private_key_seed(resolver_id, resolver_incarnation);
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .expect("fixed resolver seed has exact length")
        .public_key()
        .as_ref()
        .to_vec()
}

/// Sign one resolver decision only after its local journal is durable.
///
/// # Errors
///
/// Returns an error when the statement cannot be encoded.
pub fn sign_cell_resolver_decision(
    statement: CellResolverDecisionStatement,
) -> Result<CellResolverDecisionAttestation, String> {
    let seed =
        cell_resolver_private_key_seed(statement.resolver_id, statement.resolver_incarnation);
    let pair = Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| "resolver signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(CellResolverDecisionAttestation {
        statement,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Digest one transaction without its process-derived resolver evidence.
///
/// # Errors
///
/// Returns an error when the canonical command cannot be encoded.
pub fn cell_partitioned_transaction_sha256(
    command: &CellTransactionCommand,
) -> Result<[u8; 32], String> {
    let mut canonical = command.clone();
    canonical.partitioned_resolution = None;
    canonical.accepted_resolvers.clear();
    canonicalize_transaction(&mut canonical);
    let bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    Ok(Sha256::digest(bytes).into())
}

/// Stable semantic result of applying one committed transaction command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTransactionApplyResponse {
    pub status: CellTransactionStatus,
    pub generation: u64,
    pub commit_sequence: Option<u64>,
    pub envelope: Option<Vec<u8>>,
    pub row_count: u64,
}

/// Deterministic admission result. Rejections are durable Raft outcomes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTransactionStatus {
    Committed,
    Conflict,
    FutureReadVersion,
    InvalidReadVersion,
    InvalidRequest,
    MissingLogTag,
    MissingResolver,
}

/// Read-only state exposed by the process prototype for exact convergence checks.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellStateSnapshot {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub latest_sequence: u64,
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub committed_envelopes: Vec<Vec<u8>>,
    #[serde(default)]
    pub log_set_policies: Vec<CellLogSetPolicy>,
    #[serde(default)]
    pub pending_log_set_policy_transition: Option<CellLogSetPolicyTransition>,
    #[serde(default)]
    pub completed_log_set_policy_transitions: Vec<CompletedCellLogSetPolicyTransition>,
}

/// Bounded linearizable request for committed serving mutations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommittedEnvelopeRequest {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub after_version: u64,
    pub through_version: u64,
}

/// Exact committed-envelope suffix served by the transaction authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommittedEnvelopeFeed {
    pub authority_position: RecoveryLogPosition,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub after_version: u64,
    pub through_version: u64,
    pub latest_commit_version: u64,
    pub envelopes: Vec<Vec<u8>>,
}

/// One local-process quorum receipt for an exact envelope in one tagged log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogReceipt {
    pub format_version: u16,
    pub log_set_id: u16,
    pub generation: u64,
    pub envelope_sha256: [u8; 32],
    pub durable_position: u64,
    pub quorum_node_ids: Vec<u64>,
}

/// One authenticated member of a tagged transaction-log set.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CellLogSetMember {
    pub node_id: u64,
    pub public_key: Vec<u8>,
}

/// Replicated membership and quorum policy for one tagged-log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLogSetPolicy {
    pub format_version: u16,
    pub generation: u64,
    pub policy_epoch: u64,
    pub log_set_id: u16,
    pub quorum_size: u16,
    #[serde(default)]
    pub ratekeeper_soft_limit_bytes: u64,
    pub members: Vec<CellLogSetMember>,
}

/// Canonical durable fact signed by one tagged-log process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transaction_identity: RequestIdentity,
    pub commit_sequence: u64,
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub envelope_sha256: [u8; 32],
    pub durable_position: u64,
}

impl CellTaggedLogStatement {
    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// One process signature over a tagged-log durability statement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogAttestation {
    pub signer_id: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof that one exact staged envelope is durable in one log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogCertificate {
    pub statement: CellTaggedLogStatement,
    pub attestations: Vec<CellTaggedLogAttestation>,
}

/// Common statement for one durable tagged-log generation fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogFenceStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub recovery_id: u64,
    pub transaction_identity: RequestIdentity,
    pub commit_sequence: u64,
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub envelope_sha256: [u8; 32],
}

impl CellTaggedLogFenceStatement {
    fn signing_bytes(&self, record_present: bool) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_FENCE_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        bytes.push(u8::from(record_present));
        Ok(bytes)
    }
}

/// One process signature after durably fencing and observing the exact record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogFenceAttestation {
    pub signer_id: u64,
    pub record_present: bool,
    pub signature: Vec<u8>,
}

/// Quorum proof that one tagged-log set can no longer accept old-generation appends.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogFenceCertificate {
    pub statement: CellTaggedLogFenceStatement,
    pub attestations: Vec<CellTaggedLogFenceAttestation>,
}

/// One exact staged record named by a bounded generation-recovery window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellStagedWindowRecord {
    pub transaction_identity: RequestIdentity,
    pub commit_sequence: u64,
    pub envelope_sha256: [u8; 32],
}

/// Exact ordered unresolved transaction window presented during takeover.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellStagedWindow {
    pub format_version: u16,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub encoded_bytes: u64,
    pub records: Vec<CellStagedWindowRecord>,
    pub window_sha256: [u8; 32],
}

impl CellStagedWindow {
    /// Construct the canonical bounded-window identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, unordered, or zero-byte window.
    pub fn new(records: Vec<CellStagedWindowRecord>, encoded_bytes: u64) -> Result<Self, String> {
        let first_sequence = records
            .first()
            .map(|record| record.commit_sequence)
            .ok_or_else(|| "staged window must contain at least one record".to_owned())?;
        let last_sequence = records
            .last()
            .map(|record| record.commit_sequence)
            .unwrap_or_default();
        if encoded_bytes == 0
            || records.iter().any(|record| record.commit_sequence == 0)
            || records
                .windows(2)
                .any(|pair| pair[1].commit_sequence != pair[0].commit_sequence.saturating_add(1))
        {
            return Err("staged window identity is invalid".to_owned());
        }
        let mut window = Self {
            format_version: 1,
            first_sequence,
            last_sequence,
            encoded_bytes,
            records,
            window_sha256: [0; 32],
        };
        window.window_sha256 = window.canonical_sha256()?;
        Ok(window)
    }

    fn canonical_sha256(&self) -> Result<[u8; 32], String> {
        let mut bytes = STAGED_WINDOW_MAGIC.to_vec();
        bytes.extend(
            serde_json::to_vec(&(
                self.format_version,
                self.first_sequence,
                self.last_sequence,
                self.encoded_bytes,
                &self.records,
            ))
            .map_err(|error| error.to_string())?,
        );
        Ok(Sha256::digest(bytes).into())
    }

    fn valid_identity(&self) -> bool {
        self.format_version == 1
            && !self.records.is_empty()
            && self.first_sequence
                == self
                    .records
                    .first()
                    .map(|record| record.commit_sequence)
                    .unwrap_or_default()
            && self.last_sequence
                == self
                    .records
                    .last()
                    .map(|record| record.commit_sequence)
                    .unwrap_or_default()
            && self
                .records
                .windows(2)
                .all(|pair| pair[1].commit_sequence == pair[0].commit_sequence.saturating_add(1))
            && self
                .canonical_sha256()
                .is_ok_and(|digest| digest == self.window_sha256)
    }
}

/// Common statement for a durable fence plus one exact staged-window inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPrefixFenceStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub recovery_id: u64,
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub window: CellStagedWindow,
}

/// One local tLog observation for one exact record in the staged window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPrefixObservation {
    pub transaction_identity: RequestIdentity,
    pub commit_sequence: u64,
    pub envelope_sha256: [u8; 32],
    pub record_present: bool,
}

impl CellTaggedLogPrefixFenceStatement {
    fn signing_bytes(
        &self,
        observations: &[CellTaggedLogPrefixObservation],
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_PREFIX_FENCE_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&(self, observations))?);
        Ok(bytes)
    }
}

/// One process signature over its complete ordered staged-window inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPrefixFenceAttestation {
    pub signer_id: u64,
    pub observations: Vec<CellTaggedLogPrefixObservation>,
    pub signature: Vec<u8>,
}

/// Quorum proof that one tLog set is fenced with a complete staged inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPrefixFenceCertificate {
    pub statement: CellTaggedLogPrefixFenceStatement,
    pub attestations: Vec<CellTaggedLogPrefixFenceAttestation>,
}

/// Common pre-admission capacity sample requested from one tagged-log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogCapacityStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transaction_identity: RequestIdentity,
    pub transaction_sha256: [u8; 32],
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub projected_frame_bytes: u64,
    pub soft_limit_bytes: u64,
    pub reservation_epoch: u64,
}

impl CellTaggedLogCapacityStatement {
    fn signing_bytes(
        &self,
        attestation: &CellTaggedLogCapacityAttestation,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_CAPACITY_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&(
            self,
            attestation.signer_id,
            attestation.last_position,
            attestation.popped_through,
            attestation.retained_bytes,
            attestation.hard_limit_bytes,
            attestation.sample_epoch,
        ))?);
        Ok(bytes)
    }
}

/// One signed local retained-byte and position observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogCapacityAttestation {
    pub signer_id: u64,
    pub last_position: u64,
    pub popped_through: u64,
    pub retained_bytes: u64,
    pub hard_limit_bytes: u64,
    pub sample_epoch: u64,
    pub signature: Vec<u8>,
}

/// Authenticated capacity observations from one required tagged-log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogCapacityCertificate {
    pub statement: CellTaggedLogCapacityStatement,
    pub attestations: Vec<CellTaggedLogCapacityAttestation>,
}

/// Common authenticated pop identity for one tagged-log set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPopStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub publication_root_sha256: [u8; 32],
    pub object_frontier: u64,
    pub pop_epoch: u64,
}

impl CellTaggedLogPopStatement {
    fn signing_bytes(
        &self,
        attestation: &CellTaggedLogPopAttestation,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_POP_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&(
            self,
            attestation.signer_id,
            attestation.last_position,
            attestation.popped_through,
            attestation.retained_bytes,
            attestation.sample_epoch,
        ))?);
        Ok(bytes)
    }
}

/// One signed result after a process has durably popped an exact prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPopAttestation {
    pub signer_id: u64,
    pub last_position: u64,
    pub popped_through: u64,
    pub retained_bytes: u64,
    pub sample_epoch: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof that one tagged-log set durably popped through a watermark.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPopCertificate {
    pub statement: CellTaggedLogPopStatement,
    pub attestations: Vec<CellTaggedLogPopAttestation>,
}

/// Phase of a quorum-certified tagged-log learner repair.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTaggedLogRepairPhase {
    BaseSnapshot,
    LearnerReady,
}

/// Exact retained suffix and learner identity signed by an active tLog quorum.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogRepairStatement {
    pub format_version: u16,
    pub phase: CellTaggedLogRepairPhase,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub repair_id: u64,
    pub failed_node_id: u64,
    pub learner_node_id: u64,
    pub learner_incarnation: [u8; 16],
    pub learner_public_key: Vec<u8>,
    pub last_position: u64,
    pub popped_through: u64,
    pub snapshot_length: u64,
    pub snapshot_sha256: [u8; 32],
}

impl CellTaggedLogRepairStatement {
    fn signing_bytes(
        &self,
        attestation: &CellTaggedLogRepairAttestation,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_REPAIR_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&(
            self,
            attestation.signer_id,
            attestation.source_sample_epoch,
        ))?);
        Ok(bytes)
    }
}

/// One source tLog signature over an exact repair snapshot identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogRepairAttestation {
    pub signer_id: u64,
    pub source_sample_epoch: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof for either the repair base or synchronized learner readiness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogRepairCertificate {
    pub statement: CellTaggedLogRepairStatement,
    pub attestations: Vec<CellTaggedLogRepairAttestation>,
}

/// Exact one-member successor policy authorized after learner repair.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLogSetPolicyTransition {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transition_id: u64,
    pub log_set_id: u16,
    pub old_policy: CellLogSetPolicy,
    pub next_policy: CellLogSetPolicy,
    pub failed_node_id: u64,
    pub learner_node_id: u64,
    pub learner_incarnation: [u8; 16],
    pub learner_public_key: Vec<u8>,
    pub repair_readiness_sha256: [u8; 32],
    pub retained_root_sha256: [u8; 32],
    pub retained_last_position: u64,
}

/// Canonical successor-policy stage observed by one proposed member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPolicyStageStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transition_id: u64,
    pub log_set_id: u16,
    pub old_policy_epoch: u64,
    pub next_policy_epoch: u64,
    pub transition_sha256: [u8; 32],
    pub retained_root_sha256: [u8; 32],
    pub retained_last_position: u64,
}

impl CellTaggedLogPolicyStageStatement {
    fn signing_bytes(
        &self,
        attestation: &CellTaggedLogPolicyStageAttestation,
    ) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = TAGGED_LOG_POLICY_STAGE_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&(
            self,
            attestation.signer_id,
            attestation.source_sample_epoch,
        ))?);
        Ok(bytes)
    }
}

/// One successor-member signature over an exact staged policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPolicyStageAttestation {
    pub signer_id: u64,
    pub source_sample_epoch: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof that enough successor members durably staged one policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPolicyStageCertificate {
    pub statement: CellTaggedLogPolicyStageStatement,
    pub attestations: Vec<CellTaggedLogPolicyStageAttestation>,
}

/// Replicated completion retained for authority activation attestations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletedCellLogSetPolicyTransition {
    pub transition: CellLogSetPolicyTransition,
    pub successor_stage_sha256: [u8; 32],
    pub authority_commit_index: u64,
}

/// Canonical authority observation that one successor policy is committed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLogSetPolicyActivationStatement {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transition_id: u64,
    pub log_set_id: u16,
    pub next_policy_epoch: u64,
    pub next_policy_sha256: [u8; 32],
    pub successor_stage_sha256: [u8; 32],
    pub authority_commit_index: u64,
}

impl CellLogSetPolicyActivationStatement {
    /// Construct the exact activation statement for one replicated completion.
    #[must_use]
    pub fn new(completed: &CompletedCellLogSetPolicyTransition) -> Self {
        Self {
            format_version: 1,
            cell_id: completed.transition.cell_id,
            tenant_id: completed.transition.tenant_id,
            generation: completed.transition.generation,
            transition_id: completed.transition.transition_id,
            log_set_id: completed.transition.log_set_id,
            next_policy_epoch: completed.transition.next_policy.policy_epoch,
            next_policy_sha256: cell_log_set_policy_sha256(&completed.transition.next_policy),
            successor_stage_sha256: completed.successor_stage_sha256,
            authority_commit_index: completed.authority_commit_index,
        }
    }

    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = LOG_SET_POLICY_ACTIVATION_CERTIFICATE_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// One transaction-authority signature after applying the policy transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLogSetPolicyActivationAttestation {
    pub signer_id: u64,
    pub signature: Vec<u8>,
}

/// Authority quorum capability that permits successor tLogs to activate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellLogSetPolicyActivationCertificate {
    pub statement: CellLogSetPolicyActivationStatement,
    pub attestations: Vec<CellLogSetPolicyActivationAttestation>,
}

/// Derive the Ed25519 public key for one tagged-log signer seed.
///
/// # Errors
///
/// Returns an error when the seed is not exactly 32 bytes.
pub fn tagged_log_public_key(private_key_seed: &[u8]) -> Result<Vec<u8>, String> {
    Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map(|pair| pair.public_key().as_ref().to_vec())
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())
}

/// Sign one canonical statement after a tagged-log process has made it durable.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &CellTaggedLogStatement,
) -> Result<CellTaggedLogAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(CellTaggedLogAttestation {
        signer_id,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Verify that one exact committed envelope has a quorum durability
/// certificate under the supplied tagged-log policy.
///
/// This is the serving-side verification boundary. Transaction admission uses
/// the same certificate, but a disposable Range Engine must be able to
/// authenticate retained txLog bytes without reconstructing the private staged
/// transaction state held by the transaction authority.
#[must_use]
pub fn verify_tagged_log_envelope_certificate(
    certificate: &CellTaggedLogCertificate,
    policy: &CellLogSetPolicy,
    encoded_envelope: &[u8],
) -> bool {
    let Ok(envelope) = CommitEnvelope::decode(encoded_envelope) else {
        return false;
    };
    let (encoded_client_id, request_id) = envelope.client_identity();
    if encoded_client_id[..8] != [0; 8] {
        return false;
    }
    let mut client_id = [0_u8; 8];
    client_id.copy_from_slice(&encoded_client_id[8..]);
    let statement = &certificate.statement;
    let envelope_sha256: [u8; 32] = Sha256::digest(encoded_envelope).into();
    let unique_policy_members = policy
        .members
        .iter()
        .map(|member| member.node_id)
        .collect::<BTreeSet<_>>();
    if policy.format_version != 1
        || policy.generation != envelope.generation()
        || policy.policy_epoch == 0
        || policy.log_set_id == 0
        || policy.quorum_size == 0
        || usize::from(policy.quorum_size) > policy.members.len()
        || unique_policy_members.len() != policy.members.len()
        || statement.format_version != 1
        || statement.cell_id != envelope.cell_id()
        || statement.tenant_id != envelope.tenant_id()
        || statement.generation != envelope.generation()
        || statement.transaction_identity
            != (RequestIdentity {
                client_id: u64::from_be_bytes(client_id),
                request_id,
            })
        || statement.commit_sequence != envelope.version().sequence()
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.envelope_sha256 != envelope_sha256
        || statement.durable_position == 0
        || !envelope.required_log_tags().contains(&statement.log_set_id)
    {
        return false;
    }
    let Ok(bytes) = statement.signing_bytes() else {
        return false;
    };
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) {
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
    distinct.len() >= usize::from(policy.quorum_size)
}

/// Sign one presence observation after the tagged-log generation fence is durable.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_fence_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &CellTaggedLogFenceStatement,
    record_present: bool,
) -> Result<CellTaggedLogFenceAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes(record_present)
        .map_err(|error| error.to_string())?;
    Ok(CellTaggedLogFenceAttestation {
        signer_id,
        record_present,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Sign one complete inventory after its old-generation fence is durable.
///
/// # Errors
///
/// Returns an error when the seed or canonical inventory cannot be encoded.
pub fn sign_tagged_log_prefix_fence_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &CellTaggedLogPrefixFenceStatement,
    observations: Vec<CellTaggedLogPrefixObservation>,
) -> Result<CellTaggedLogPrefixFenceAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes(&observations)
        .map_err(|error| error.to_string())?;
    Ok(CellTaggedLogPrefixFenceAttestation {
        signer_id,
        observations,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Sign one local tagged-log capacity observation.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_capacity_statement(
    private_key_seed: &[u8],
    statement: &CellTaggedLogCapacityStatement,
    mut attestation: CellTaggedLogCapacityAttestation,
) -> Result<CellTaggedLogCapacityAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    attestation.signature.clear();
    let bytes = statement
        .signing_bytes(&attestation)
        .map_err(|error| error.to_string())?;
    attestation.signature = pair.sign(&bytes).as_ref().to_vec();
    Ok(attestation)
}

/// Sign one durable tagged-log pop result.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_pop_statement(
    private_key_seed: &[u8],
    statement: &CellTaggedLogPopStatement,
    mut attestation: CellTaggedLogPopAttestation,
) -> Result<CellTaggedLogPopAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    attestation.signature.clear();
    let bytes = statement
        .signing_bytes(&attestation)
        .map_err(|error| error.to_string())?;
    attestation.signature = pair.sign(&bytes).as_ref().to_vec();
    Ok(attestation)
}

/// Sign one exact retained-suffix identity for learner repair.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_repair_statement(
    private_key_seed: &[u8],
    statement: &CellTaggedLogRepairStatement,
    mut attestation: CellTaggedLogRepairAttestation,
) -> Result<CellTaggedLogRepairAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    attestation.signature.clear();
    let bytes = statement
        .signing_bytes(&attestation)
        .map_err(|error| error.to_string())?;
    attestation.signature = pair.sign(&bytes).as_ref().to_vec();
    Ok(attestation)
}

/// Sign one exact successor-policy stage observation.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_tagged_log_policy_stage_statement(
    private_key_seed: &[u8],
    statement: &CellTaggedLogPolicyStageStatement,
    mut attestation: CellTaggedLogPolicyStageAttestation,
) -> Result<CellTaggedLogPolicyStageAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "tagged-log signing seed must contain exactly 32 bytes".to_owned())?;
    attestation.signature.clear();
    let bytes = statement
        .signing_bytes(&attestation)
        .map_err(|error| error.to_string())?;
    attestation.signature = pair.sign(&bytes).as_ref().to_vec();
    Ok(attestation)
}

/// Derive one authority-node key for policy activation from eval-only base material.
///
/// Production deployments must provision independent node keys instead.
#[must_use]
pub fn cell_log_set_policy_authority_seed(base_seed: &[u8], node_id: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"okv-cell-log-set-policy-authority-seed-v1");
    digest.update(base_seed);
    digest.update(node_id.to_be_bytes());
    digest.finalize().into()
}

/// Sign a replicated log-set policy activation observation.
///
/// # Errors
///
/// Returns an error when the seed or statement cannot be encoded.
pub fn sign_cell_log_set_policy_activation_statement(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &CellLogSetPolicyActivationStatement,
) -> Result<CellLogSetPolicyActivationAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "policy authority seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(CellLogSetPolicyActivationAttestation {
        signer_id,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Verify one authority quorum over a committed policy activation.
#[must_use]
pub fn verify_cell_log_set_policy_activation_certificate(
    certificate: &CellLogSetPolicyActivationCertificate,
    authority_members: &BTreeMap<u64, Vec<u8>>,
    quorum_size: u16,
) -> bool {
    if certificate.statement.format_version != 1
        || certificate.statement.generation == 0
        || certificate.statement.transition_id == 0
        || certificate.statement.log_set_id == 0
        || certificate.statement.next_policy_epoch == 0
        || certificate.statement.next_policy_sha256 == [0; 32]
        || certificate.statement.successor_stage_sha256 == [0; 32]
        || certificate.statement.authority_commit_index == 0
        || quorum_size == 0
        || usize::from(quorum_size) > authority_members.len()
    {
        return false;
    }
    let Ok(bytes) = certificate.statement.signing_bytes() else {
        return false;
    };
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) {
            return false;
        }
        let Some(public_key) = authority_members.get(&attestation.signer_id) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(quorum_size)
}

/// Canonical digest for one log-set policy.
#[must_use]
pub fn cell_log_set_policy_sha256(policy: &CellLogSetPolicy) -> [u8; 32] {
    let mut canonical = policy.clone();
    canonical.members.sort();
    Sha256::digest(serde_json::to_vec(&canonical).unwrap_or_default()).into()
}

/// Canonical digest for one policy transition.
#[must_use]
pub fn cell_log_set_policy_transition_sha256(transition: &CellLogSetPolicyTransition) -> [u8; 32] {
    let mut canonical = transition.clone();
    canonical.old_policy.members.sort();
    canonical.next_policy.members.sort();
    Sha256::digest(serde_json::to_vec(&canonical).unwrap_or_default()).into()
}

/// Canonical digest for one repair certificate.
#[must_use]
pub fn cell_tagged_log_repair_certificate_sha256(
    certificate: &CellTaggedLogRepairCertificate,
) -> [u8; 32] {
    let mut canonical = certificate.clone();
    canonical
        .attestations
        .sort_by_key(|attestation| attestation.signer_id);
    Sha256::digest(serde_json::to_vec(&canonical).unwrap_or_default()).into()
}

/// Canonical digest for one successor-policy stage certificate.
#[must_use]
pub fn cell_tagged_log_policy_stage_certificate_sha256(
    certificate: &CellTaggedLogPolicyStageCertificate,
) -> [u8; 32] {
    let mut canonical = certificate.clone();
    canonical
        .attestations
        .sort_by_key(|attestation| attestation.signer_id);
    Sha256::digest(serde_json::to_vec(&canonical).unwrap_or_default()).into()
}

/// Verify a successor-policy stage quorum.
#[must_use]
pub fn verify_tagged_log_policy_stage_certificate(
    certificate: &CellTaggedLogPolicyStageCertificate,
    transition: &CellLogSetPolicyTransition,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.cell_id != transition.cell_id
        || statement.tenant_id != transition.tenant_id
        || statement.generation != transition.generation
        || statement.transition_id != transition.transition_id
        || statement.log_set_id != transition.log_set_id
        || statement.old_policy_epoch != transition.old_policy.policy_epoch
        || statement.next_policy_epoch != transition.next_policy.policy_epoch
        || statement.transition_sha256 != cell_log_set_policy_transition_sha256(transition)
        || statement.retained_root_sha256 != transition.retained_root_sha256
        || statement.retained_last_position != transition.retained_last_position
    {
        return false;
    }
    let members = transition
        .next_policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) || attestation.source_sample_epoch == 0 {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(attestation) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(transition.next_policy.quorum_size)
}

/// Verify one capacity quorum against replicated tagged-log policy.
#[must_use]
pub fn verify_tagged_log_capacity_certificate(
    certificate: &CellTaggedLogCapacityCertificate,
    policy: &CellLogSetPolicy,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.generation != policy.generation
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.projected_frame_bytes == 0
        || statement.soft_limit_bytes == 0
        || statement.reservation_epoch == 0
    {
        return false;
    }
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id)
            || attestation.sample_epoch == 0
            || attestation.hard_limit_bytes < statement.soft_limit_bytes
        {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(attestation) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(policy.quorum_size)
}

/// Hash one canonical transaction for a pre-admission capacity reservation.
///
/// # Errors
///
/// Returns an error only when the canonical transaction cannot be serialized.
pub fn ratekeeper_transaction_sha256(
    transaction: &CellTransactionCommand,
) -> Result<[u8; 32], String> {
    let mut canonical = transaction.clone();
    canonicalize_transaction(&mut canonical);
    let bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    let mut digest = Sha256::new();
    digest.update(b"okv-ratekeeper-transaction-v1");
    digest.update(bytes);
    Ok(digest.finalize().into())
}

/// Verify one durable pop quorum against replicated tagged-log policy.
#[must_use]
pub fn verify_tagged_log_pop_certificate(
    certificate: &CellTaggedLogPopCertificate,
    policy: &CellLogSetPolicy,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.generation != policy.generation
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.object_frontier == 0
        || statement.pop_epoch == 0
        || statement.publication_root_sha256 == [0; 32]
    {
        return false;
    }
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id)
            || attestation.sample_epoch == 0
            || attestation.popped_through != statement.object_frontier
        {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(attestation) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(policy.quorum_size)
}

/// Verify one repair quorum against the active tagged-log policy.
#[must_use]
pub fn verify_tagged_log_repair_certificate(
    certificate: &CellTaggedLogRepairCertificate,
    policy: &CellLogSetPolicy,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.generation != policy.generation
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.repair_id == 0
        || statement.failed_node_id == 0
        || statement.learner_node_id == 0
        || statement.failed_node_id == statement.learner_node_id
        || statement.learner_incarnation == [0; 16]
        || statement.learner_public_key.len() != 32
        || statement.last_position == 0
        || statement.popped_through == 0
        || statement.snapshot_length == 0
        || statement.snapshot_sha256 == [0; 32]
        || !policy
            .members
            .iter()
            .any(|member| member.node_id == statement.failed_node_id)
        || policy
            .members
            .iter()
            .any(|member| member.node_id == statement.learner_node_id)
    {
        return false;
    }
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) || attestation.source_sample_epoch == 0 {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(attestation) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(policy.quorum_size)
}

/// Two-stage transaction action carried by the replicated Cell v0 authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellStagedTransactionAction {
    InstallLogSetPolicies {
        policies: Vec<CellLogSetPolicy>,
    },
    PrepareLogSetPolicyTransition {
        transition: Box<CellLogSetPolicyTransition>,
        repair_readiness: Box<CellTaggedLogRepairCertificate>,
    },
    CommitLogSetPolicyTransition {
        transition_id: u64,
        successor_stage: CellTaggedLogPolicyStageCertificate,
    },
    ReserveCapacity {
        transaction: CellTransactionCommand,
        certificates: Vec<CellTaggedLogCapacityCertificate>,
    },
    Stage {
        transaction: CellTransactionCommand,
    },
    RecordLogReceipt {
        receipt: CellTaggedLogReceipt,
    },
    RecordLogCertificate {
        certificate: CellTaggedLogCertificate,
    },
    Publish,
    TakeoverPublish {
        previous_generation: u64,
        recovery_id: u64,
        expected_commit_sequence: u64,
        expected_envelope_sha256: [u8; 32],
    },
    TakeoverAbort {
        previous_generation: u64,
        recovery_id: u64,
        expected_commit_sequence: u64,
        expected_envelope_sha256: [u8; 32],
        log_set_fences: Vec<CellTaggedLogFenceCertificate>,
    },
    TakeoverRecoverPrefix {
        previous_generation: u64,
        recovery_id: u64,
        staged_window: CellStagedWindow,
        log_set_inventories: Vec<CellTaggedLogPrefixFenceCertificate>,
    },
}

/// Replicated command that separates ordering from visible commit publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellStagedTransactionCommand {
    pub identity: RequestIdentity,
    pub credential: Option<GenerationCredential>,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub transaction_identity: RequestIdentity,
    pub action: CellStagedTransactionAction,
}

impl CellStagedTransactionCommand {
    /// Encode a canonical staged-transaction transition into application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut canonical = self.clone();
        match &mut canonical.action {
            CellStagedTransactionAction::InstallLogSetPolicies { policies } => {
                for policy in policies.iter_mut() {
                    policy.members.sort();
                }
                policies.sort_by_key(|policy| policy.log_set_id);
            }
            CellStagedTransactionAction::PrepareLogSetPolicyTransition {
                transition,
                repair_readiness,
            } => {
                transition.old_policy.members.sort();
                transition.next_policy.members.sort();
                repair_readiness
                    .attestations
                    .sort_by_key(|attestation| attestation.signer_id);
            }
            CellStagedTransactionAction::CommitLogSetPolicyTransition {
                successor_stage, ..
            } => {
                successor_stage
                    .attestations
                    .sort_by_key(|attestation| attestation.signer_id);
            }
            CellStagedTransactionAction::ReserveCapacity {
                transaction,
                certificates,
            } => {
                canonicalize_transaction(transaction);
                for certificate in certificates.iter_mut() {
                    certificate
                        .attestations
                        .sort_by_key(|attestation| attestation.signer_id);
                }
                certificates.sort_by_key(|certificate| certificate.statement.log_set_id);
            }
            CellStagedTransactionAction::Stage { transaction } => {
                canonicalize_transaction(transaction);
            }
            CellStagedTransactionAction::RecordLogReceipt { receipt } => {
                receipt.quorum_node_ids.sort_unstable();
                receipt.quorum_node_ids.dedup();
            }
            CellStagedTransactionAction::RecordLogCertificate { certificate } => {
                certificate
                    .attestations
                    .sort_by_key(|attestation| attestation.signer_id);
            }
            CellStagedTransactionAction::Publish
            | CellStagedTransactionAction::TakeoverPublish { .. } => {}
            CellStagedTransactionAction::TakeoverAbort { log_set_fences, .. } => {
                for certificate in log_set_fences.iter_mut() {
                    certificate
                        .attestations
                        .sort_by_key(|attestation| attestation.signer_id);
                }
                log_set_fences.sort_by_key(|certificate| certificate.statement.log_set_id);
            }
            CellStagedTransactionAction::TakeoverRecoverPrefix {
                log_set_inventories,
                ..
            } => {
                for certificate in log_set_inventories.iter_mut() {
                    certificate
                        .attestations
                        .sort_by_key(|attestation| attestation.signer_id);
                }
                log_set_inventories.sort_by_key(|certificate| certificate.statement.log_set_id);
            }
        }
        let mut encoded = STAGED_COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(&canonical)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(STAGED_COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// Durable state transition returned for a staged transaction command.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellStagedTransactionStatus {
    LogSetPoliciesInstalled,
    PolicyTransitionPrepared,
    PolicyTransitionCommitted,
    AlreadyPolicyTransitionCommitted,
    InvalidPolicyTransition,
    CapacityReserved,
    RateLimited,
    InvalidCapacityCertificate,
    RatekeepingRequired,
    Staged,
    LogReceiptRecorded,
    LogCertificateRecorded,
    Committed,
    AlreadyCommitted,
    Conflict,
    InvalidRequest,
    InvalidLogReceipt,
    ConflictingLogReceipt,
    InvalidLogCertificate,
    ConflictingLogCertificate,
    InvalidLogSetPolicy,
    MissingLogReceipt,
    InvalidGenerationTakeover,
    Aborted,
    AlreadyAborted,
    PrefixRecovered,
    AlreadyPrefixRecovered,
    InvalidStagedPrefix,
}

/// Exact staged or visible outcome returned by the transaction authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellStagedTransactionApplyResponse {
    pub status: CellStagedTransactionStatus,
    pub transaction_identity: RequestIdentity,
    pub generation: u64,
    pub commit_sequence: Option<u64>,
    pub envelope: Option<Vec<u8>>,
    pub durable_log_sets: Vec<u16>,
    pub visible: bool,
    pub aborted: bool,
    pub row_count: u64,
    #[serde(default)]
    pub recovered_records: u64,
    #[serde(default)]
    pub aborted_records: u64,
    #[serde(default)]
    pub capacity_retry_token: Option<[u8; 32]>,
    #[serde(default)]
    pub capacity_reservation_epoch: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct CellTransactionState {
    domains: Vec<CellDomainState>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct CellDomainState {
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    latest_sequence: u64,
    #[serde(default)]
    last_resolver_candidate: u64,
    #[serde(default)]
    last_resolver_transaction_system_generation: u64,
    #[serde(with = "row_map_serde")]
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    committed_writes: Vec<CommittedWrite>,
    committed_envelopes: Vec<Vec<u8>>,
    previous_log_chain: [u8; 32],
    #[serde(default)]
    log_set_policies: BTreeMap<u16, CellLogSetPolicy>,
    #[serde(default)]
    pending_log_set_policy_transition: Option<CellLogSetPolicyTransition>,
    #[serde(default)]
    completed_log_set_policy_transitions: BTreeMap<u64, CompletedCellLogSetPolicyTransition>,
    #[serde(default)]
    staged_transactions: Vec<StagedTransaction>,
    #[serde(default)]
    capacity_reservations: BTreeMap<String, CapacityReservation>,
    #[serde(default)]
    capacity_attempt_epochs: BTreeMap<String, u64>,
    #[serde(default)]
    capacity_retry_tokens: BTreeMap<String, [u8; 32]>,
    #[serde(default)]
    capacity_sample_epochs: BTreeMap<u16, BTreeMap<u64, u64>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CapacityReservation {
    transaction: CellTransactionCommand,
    reservation_epoch: u64,
    sample_epochs: BTreeMap<u16, BTreeMap<u64, u64>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CommittedWrite {
    sequence: u64,
    range: CellKeyRange,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StagedTransaction {
    identity: RequestIdentity,
    transaction: CellTransactionCommand,
    commit_sequence: u64,
    envelope: Vec<u8>,
    receipts: BTreeMap<u16, CellTaggedLogReceipt>,
    #[serde(default)]
    certificates: BTreeMap<u16, CellTaggedLogCertificate>,
    visible: bool,
    #[serde(default)]
    aborted: bool,
}

impl CellTransactionState {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply(
        &mut self,
        command: &CellTransactionCommand,
        log_index: u64,
        generation_fence_faults: GenerationFenceFaults,
    ) -> CellTransactionApplyResponse {
        let domain_index = self
            .domains
            .iter()
            .position(|domain| {
                domain.cell_id == command.cell_id && domain.tenant_id == command.tenant_id
            })
            .unwrap_or_else(|| {
                self.domains.push(CellDomainState {
                    cell_id: command.cell_id,
                    tenant_id: command.tenant_id,
                    generation: command.generation,
                    ..CellDomainState::default()
                });
                self.domains.len() - 1
            });
        let domain = &mut self.domains[domain_index];
        let reject = |status, domain: &CellDomainState| CellTransactionApplyResponse {
            status,
            generation: command.generation,
            commit_sequence: None,
            envelope: None,
            row_count: domain.rows.len() as u64,
        };

        if generation_fence_faults.allow_successor_to_skip_staged_head
            && command.generation == domain.generation.saturating_add(1)
            && domain
                .staged_transactions
                .iter()
                .any(|staged| !staged.visible && !staged.aborted)
        {
            domain.generation = command.generation;
        }

        if command.generation == 0
            || domain.generation != command.generation
            || (command.read_version.sequence != 0
                && command.read_version.generation != command.generation)
        {
            return reject(CellTransactionStatus::InvalidReadVersion, domain);
        }
        if command.read_version.sequence > domain.latest_sequence {
            return reject(CellTransactionStatus::FutureReadVersion, domain);
        }
        if !contains_all(&command.durable_log_tags, &REQUIRED_LOG_TAGS) {
            return reject(CellTransactionStatus::MissingLogTag, domain);
        }
        if !valid_request(command) {
            return reject(CellTransactionStatus::InvalidRequest, domain);
        }
        let partitioned_decision = if let Some(resolution) = &command.partitioned_resolution {
            let transaction_system_generation = if resolution.transaction_system_generation == 0 {
                command.generation
            } else {
                resolution.transaction_system_generation
            };
            let candidate_order_valid = if transaction_system_generation
                == domain.last_resolver_transaction_system_generation
            {
                resolution.candidate_sequence == domain.last_resolver_candidate.saturating_add(1)
            } else {
                transaction_system_generation > domain.last_resolver_transaction_system_generation
                    && resolution.candidate_sequence > domain.last_resolver_candidate
            };
            if !candidate_order_valid {
                return reject(CellTransactionStatus::InvalidRequest, domain);
            }
            match verify_partitioned_resolution(command, resolution) {
                Ok(decision) => {
                    domain.last_resolver_candidate = resolution.candidate_sequence;
                    domain.last_resolver_transaction_system_generation =
                        transaction_system_generation;
                    Some(decision)
                }
                Err(()) => return reject(CellTransactionStatus::MissingResolver, domain),
            }
        } else {
            if !contains_all(&command.accepted_resolvers, &REQUIRED_RESOLVERS) {
                return reject(CellTransactionStatus::MissingResolver, domain);
            }
            None
        };
        if partitioned_decision == Some(CellResolverDecision::Conflict) {
            return reject(CellTransactionStatus::Conflict, domain);
        }
        if partitioned_decision.is_none()
            && command.read_conflicts.iter().any(|read| {
                domain.committed_writes.iter().any(|write| {
                    write.sequence > command.read_version.sequence && read.overlaps(&write.range)
                })
            })
        {
            return reject(CellTransactionStatus::Conflict, domain);
        }

        let read_conflicts = serde_json::to_vec(&command.read_conflicts)
            .expect("validated transaction conflict ranges serialize");
        let write_conflicts = serde_json::to_vec(&command.write_conflicts)
            .expect("validated transaction conflict ranges serialize");
        let canonical_mutations = serde_json::to_vec(&command.mutations)
            .expect("validated transaction mutations serialize");
        let mut client_id = [0_u8; 16];
        client_id[8..].copy_from_slice(&command.identity.client_id.to_be_bytes());
        let required_resolvers = command.partitioned_resolution.as_ref().map_or_else(
            || REQUIRED_RESOLVERS.to_vec(),
            |resolution| {
                resolution
                    .attestations
                    .iter()
                    .map(|attestation| attestation.statement.resolver_id)
                    .collect()
            },
        );
        let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
            cell_id: command.cell_id,
            tenant_id: command.tenant_id,
            generation: command.generation,
            version: Version::from_parts(command.generation, log_index),
            log_index,
            client_id,
            request_id: command.identity.request_id,
            resolver_set_id: RESOLVER_SET_ID,
            read_conflicts,
            write_conflicts,
            canonical_mutations,
            required_resolvers,
            required_log_tags: REQUIRED_LOG_TAGS.to_vec(),
            previous_log_chain: domain.previous_log_chain,
        });
        let encoded_envelope = envelope.encode();

        for mutation in &command.mutations {
            match mutation {
                CellMutation::Clear { key } => {
                    domain.rows.remove(key);
                }
                CellMutation::Set { key, value } => {
                    domain.rows.insert(key.clone(), value.clone());
                }
            }
        }
        for range in &command.write_conflicts {
            domain.committed_writes.push(CommittedWrite {
                sequence: log_index,
                range: range.clone(),
            });
        }
        domain.generation = command.generation;
        domain.latest_sequence = log_index;
        domain.previous_log_chain = Sha256::digest(&encoded_envelope).into();
        domain.committed_envelopes.push(encoded_envelope.clone());

        CellTransactionApplyResponse {
            status: CellTransactionStatus::Committed,
            generation: command.generation,
            commit_sequence: Some(log_index),
            envelope: Some(encoded_envelope),
            row_count: domain.rows.len() as u64,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply_staged(
        &mut self,
        command: &CellStagedTransactionCommand,
        log_index: u64,
        generation_authority: &GenerationAuthorityState,
        generation_fence_faults: GenerationFenceFaults,
    ) -> CellStagedTransactionApplyResponse {
        let domain_index = self
            .domains
            .iter()
            .position(|domain| {
                domain.cell_id == command.cell_id && domain.tenant_id == command.tenant_id
            })
            .unwrap_or_else(|| {
                self.domains.push(CellDomainState {
                    cell_id: command.cell_id,
                    tenant_id: command.tenant_id,
                    generation: command.generation,
                    ..CellDomainState::default()
                });
                self.domains.len() - 1
            });
        let domain = &mut self.domains[domain_index];
        let takeover = matches!(
            command.action,
            CellStagedTransactionAction::TakeoverPublish { .. }
                | CellStagedTransactionAction::TakeoverAbort { .. }
                | CellStagedTransactionAction::TakeoverRecoverPrefix { .. }
        );
        if command.generation == 0 || (!takeover && domain.generation != command.generation) {
            return staged_response(
                domain,
                command.transaction_identity,
                CellStagedTransactionStatus::InvalidRequest,
                None,
            );
        }
        match &command.action {
            CellStagedTransactionAction::InstallLogSetPolicies { policies } => {
                if policies.is_empty()
                    || domain
                        .staged_transactions
                        .iter()
                        .any(|staged| !staged.visible && !staged.aborted)
                    || policies.iter().any(|policy| {
                        !valid_log_set_policy(policy, command.generation)
                            || domain.log_set_policies.get(&policy.log_set_id).is_some_and(
                                |existing| {
                                    existing != policy && existing.generation == policy.generation
                                },
                            )
                    })
                    || policies
                        .iter()
                        .map(|policy| policy.log_set_id)
                        .collect::<BTreeSet<_>>()
                        .len()
                        != policies.len()
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidLogSetPolicy,
                        None,
                    );
                }
                for policy in policies {
                    domain
                        .log_set_policies
                        .insert(policy.log_set_id, policy.clone());
                }
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::LogSetPoliciesInstalled,
                    None,
                )
            }
            CellStagedTransactionAction::PrepareLogSetPolicyTransition {
                transition,
                repair_readiness,
            } => {
                if let Some(completed) = domain
                    .completed_log_set_policy_transitions
                    .get(&transition.transition_id)
                {
                    let status = if completed.transition == **transition {
                        CellStagedTransactionStatus::AlreadyPolicyTransitionCommitted
                    } else {
                        CellStagedTransactionStatus::InvalidPolicyTransition
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                if let Some(pending) = &domain.pending_log_set_policy_transition {
                    let status = if pending == transition.as_ref() {
                        CellStagedTransactionStatus::PolicyTransitionPrepared
                    } else {
                        CellStagedTransactionStatus::InvalidPolicyTransition
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                let unresolved = domain
                    .staged_transactions
                    .iter()
                    .any(|staged| !staged.visible && !staged.aborted);
                let valid = domain
                    .log_set_policies
                    .get(&transition.log_set_id)
                    .is_some_and(|current| {
                        valid_log_set_policy_transition(
                            transition,
                            repair_readiness,
                            current,
                            command.cell_id,
                            command.tenant_id,
                            command.generation,
                        )
                    });
                let readiness_missing = transition.repair_readiness_sha256 == [0; 32];
                let accepts_invalid = generation_fence_faults
                    .policy_transition_accept_invalid_next_policy
                    || (readiness_missing
                        && generation_fence_faults.policy_transition_accept_missing_readiness);
                if (unresolved
                    && !generation_fence_faults.policy_transition_accept_unresolved_stage)
                    || (!valid && !accepts_invalid)
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidPolicyTransition,
                        None,
                    );
                }
                domain.pending_log_set_policy_transition = Some(transition.as_ref().clone());
                domain.latest_sequence = domain.latest_sequence.saturating_add(1);
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::PolicyTransitionPrepared,
                    None,
                )
            }
            CellStagedTransactionAction::CommitLogSetPolicyTransition {
                transition_id,
                successor_stage,
            } => {
                if let Some(completed) = domain
                    .completed_log_set_policy_transitions
                    .get(transition_id)
                {
                    let status = if generation_fence_faults.policy_transition_double_apply {
                        CellStagedTransactionStatus::PolicyTransitionCommitted
                    } else if completed.successor_stage_sha256
                        == cell_tagged_log_policy_stage_certificate_sha256(successor_stage)
                    {
                        CellStagedTransactionStatus::AlreadyPolicyTransitionCommitted
                    } else {
                        CellStagedTransactionStatus::InvalidPolicyTransition
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                let Some(transition) = domain.pending_log_set_policy_transition.clone() else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidPolicyTransition,
                        None,
                    );
                };
                let unresolved = domain
                    .staged_transactions
                    .iter()
                    .any(|staged| !staged.visible && !staged.aborted);
                if transition.transition_id != *transition_id
                    || (unresolved
                        && !generation_fence_faults.policy_transition_accept_unresolved_stage)
                    || (!verify_tagged_log_policy_stage_certificate(successor_stage, &transition)
                        && !generation_fence_faults.policy_transition_accept_mixed_stage_quorum)
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidPolicyTransition,
                        None,
                    );
                }
                let successor_stage_sha256 =
                    cell_tagged_log_policy_stage_certificate_sha256(successor_stage);
                domain
                    .log_set_policies
                    .insert(transition.log_set_id, transition.next_policy.clone());
                domain.completed_log_set_policy_transitions.insert(
                    *transition_id,
                    CompletedCellLogSetPolicyTransition {
                        transition,
                        successor_stage_sha256,
                        authority_commit_index: log_index,
                    },
                );
                domain.pending_log_set_policy_transition = None;
                domain.latest_sequence = domain.latest_sequence.saturating_add(1);
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::PolicyTransitionCommitted,
                    None,
                )
            }
            CellStagedTransactionAction::ReserveCapacity {
                transaction,
                certificates,
            } => {
                let capacity_key = capacity_identity_key(command.transaction_identity);
                if transaction.identity != command.transaction_identity
                    || transaction.cell_id != command.cell_id
                    || transaction.tenant_id != command.tenant_id
                    || transaction.generation != command.generation
                    || transaction.credential != command.credential
                    || transaction.read_version.sequence > domain.latest_sequence
                    || !contains_all(&transaction.accepted_resolvers, &REQUIRED_RESOLVERS)
                    || !contains_all(&transaction.durable_log_tags, &REQUIRED_LOG_TAGS)
                    || !valid_request(transaction)
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                }
                if domain
                    .pending_log_set_policy_transition
                    .as_ref()
                    .is_some_and(|pending| {
                        transaction.durable_log_tags.contains(&pending.log_set_id)
                    })
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidPolicyTransition,
                        None,
                    );
                }
                if let Some(existing) = domain
                    .staged_transactions
                    .iter()
                    .find(|staged| staged.identity == command.transaction_identity)
                {
                    if generation_fence_faults.ratekeeper_allow_stage_without_reservation
                        && existing.transaction == *transaction
                        && !existing.visible
                        && !existing.aborted
                    {
                        // The eval-only fault deliberately lets the subsequent
                        // capacity decision observe an already allocated head.
                    } else {
                        let status = if existing.transaction != *transaction {
                            CellStagedTransactionStatus::InvalidRequest
                        } else if existing.visible {
                            CellStagedTransactionStatus::AlreadyCommitted
                        } else if existing.aborted {
                            CellStagedTransactionStatus::AlreadyAborted
                        } else {
                            CellStagedTransactionStatus::Staged
                        };
                        return staged_response(domain, command.transaction_identity, status, None);
                    }
                }
                let configured = transaction
                    .durable_log_tags
                    .iter()
                    .filter_map(|log_set| domain.log_set_policies.get(log_set))
                    .filter(|policy| policy.ratekeeper_soft_limit_bytes > 0)
                    .collect::<Vec<_>>();
                let distinct = certificates
                    .iter()
                    .map(|certificate| certificate.statement.log_set_id)
                    .collect::<BTreeSet<_>>();
                let reservation_epoch = certificates
                    .first()
                    .map(|certificate| certificate.statement.reservation_epoch)
                    .unwrap_or_default();
                let previous_epoch = domain
                    .capacity_attempt_epochs
                    .get(&capacity_key)
                    .copied()
                    .unwrap_or_default();
                let transaction_sha256 = ratekeeper_transaction_sha256(transaction).ok();
                let certificates_valid = !configured.is_empty()
                    && configured.len() == certificates.len()
                    && distinct.len() == certificates.len()
                    && reservation_epoch > previous_epoch
                    && certificates.iter().all(|certificate| {
                        let statement = &certificate.statement;
                        domain
                            .log_set_policies
                            .get(&statement.log_set_id)
                            .is_some_and(|policy| {
                                transaction.durable_log_tags.contains(&statement.log_set_id)
                                    && statement.cell_id == transaction.cell_id
                                    && statement.tenant_id == transaction.tenant_id
                                    && statement.generation == transaction.generation
                                    && statement.transaction_identity == transaction.identity
                                    && Some(statement.transaction_sha256) == transaction_sha256
                                    && statement.soft_limit_bytes
                                        == policy.ratekeeper_soft_limit_bytes
                                    && statement.reservation_epoch == reservation_epoch
                                    && (generation_fence_faults.ratekeeper_accept_stale_sample
                                        || certificate.attestations.iter().all(|attestation| {
                                            attestation.sample_epoch
                                                > domain
                                                    .capacity_sample_epochs
                                                    .get(&statement.log_set_id)
                                                    .and_then(|epochs| {
                                                        epochs.get(&attestation.signer_id)
                                                    })
                                                    .copied()
                                                    .unwrap_or_default()
                                        }))
                                    && verify_tagged_log_capacity_certificate(certificate, policy)
                            })
                    });
                if !certificates_valid {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidCapacityCertificate,
                        None,
                    );
                }
                domain
                    .capacity_attempt_epochs
                    .insert(capacity_key.clone(), reservation_epoch);
                domain
                    .capacity_retry_tokens
                    .entry(capacity_key.clone())
                    .or_insert_with(|| capacity_retry_token(command.transaction_identity));
                let has_capacity = configured.iter().all(|policy| {
                    certificates
                        .iter()
                        .find(|certificate| certificate.statement.log_set_id == policy.log_set_id)
                        .is_some_and(|certificate| {
                            let eligible = certificate
                                .attestations
                                .iter()
                                .filter(|attestation| {
                                    attestation
                                        .retained_bytes
                                        .saturating_add(certificate.statement.projected_frame_bytes)
                                        <= policy.ratekeeper_soft_limit_bytes
                                })
                                .count();
                            eligible
                                >= if generation_fence_faults.ratekeeper_accept_best_node_capacity {
                                    1
                                } else {
                                    usize::from(policy.quorum_size)
                                }
                        })
                });
                if !has_capacity {
                    domain.capacity_reservations.remove(&capacity_key);
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::RateLimited,
                        None,
                    );
                }
                domain.capacity_reservations.insert(
                    capacity_key,
                    CapacityReservation {
                        transaction: transaction.clone(),
                        reservation_epoch,
                        sample_epochs: certificates
                            .iter()
                            .map(|certificate| {
                                (
                                    certificate.statement.log_set_id,
                                    certificate
                                        .attestations
                                        .iter()
                                        .map(|attestation| {
                                            (attestation.signer_id, attestation.sample_epoch)
                                        })
                                        .collect(),
                                )
                            })
                            .collect(),
                    },
                );
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::CapacityReserved,
                    None,
                )
            }
            CellStagedTransactionAction::Stage { transaction } => {
                let capacity_key = capacity_identity_key(command.transaction_identity);
                let configured_policy_count = transaction
                    .durable_log_tags
                    .iter()
                    .filter(|log_set| domain.log_set_policies.contains_key(log_set))
                    .count();
                if transaction.identity != command.transaction_identity
                    || transaction.cell_id != command.cell_id
                    || transaction.tenant_id != command.tenant_id
                    || transaction.generation != command.generation
                    || transaction.credential != command.credential
                    || (transaction.read_version.sequence != 0
                        && transaction.read_version.generation != transaction.generation)
                    || transaction.read_version.sequence > domain.latest_sequence
                    || (transaction.partitioned_resolution.is_none()
                        && !contains_all(&transaction.accepted_resolvers, &REQUIRED_RESOLVERS))
                    || !contains_all(&transaction.durable_log_tags, &REQUIRED_LOG_TAGS)
                    || (configured_policy_count != 0
                        && configured_policy_count != transaction.durable_log_tags.len())
                    || !valid_request(transaction)
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                }
                if domain
                    .pending_log_set_policy_transition
                    .as_ref()
                    .is_some_and(|pending| {
                        transaction.durable_log_tags.contains(&pending.log_set_id)
                    })
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidPolicyTransition,
                        None,
                    );
                }
                if let Some(existing) = domain
                    .staged_transactions
                    .iter()
                    .find(|staged| staged.identity == command.transaction_identity)
                {
                    let status = if existing.transaction != *transaction {
                        CellStagedTransactionStatus::InvalidRequest
                    } else if existing.visible {
                        CellStagedTransactionStatus::AlreadyCommitted
                    } else if existing.aborted {
                        CellStagedTransactionStatus::AlreadyAborted
                    } else {
                        CellStagedTransactionStatus::Staged
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                let partitioned_decision =
                    if let Some(resolution) = &transaction.partitioned_resolution {
                        let transaction_system_generation =
                            if resolution.transaction_system_generation == 0 {
                                transaction.generation
                            } else {
                                resolution.transaction_system_generation
                            };
                        let candidate_order_valid = if transaction_system_generation
                            == domain.last_resolver_transaction_system_generation
                        {
                            resolution.candidate_sequence
                                == domain.last_resolver_candidate.saturating_add(1)
                        } else {
                            transaction_system_generation
                                > domain.last_resolver_transaction_system_generation
                                && resolution.candidate_sequence > domain.last_resolver_candidate
                        };
                        if !candidate_order_valid {
                            return staged_response(
                                domain,
                                command.transaction_identity,
                                CellStagedTransactionStatus::InvalidRequest,
                                None,
                            );
                        }
                        match verify_partitioned_resolution(transaction, resolution) {
                            Ok(decision) => {
                                domain.last_resolver_candidate = resolution.candidate_sequence;
                                domain.last_resolver_transaction_system_generation =
                                    transaction_system_generation;
                                Some(decision)
                            }
                            Err(()) => {
                                return staged_response(
                                    domain,
                                    command.transaction_identity,
                                    CellStagedTransactionStatus::InvalidRequest,
                                    None,
                                );
                            }
                        }
                    } else {
                        None
                    };
                if partitioned_decision == Some(CellResolverDecision::Conflict) {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::Conflict,
                        None,
                    );
                }
                let ratekeeping_required = transaction.durable_log_tags.iter().any(|log_set| {
                    domain
                        .log_set_policies
                        .get(log_set)
                        .is_some_and(|policy| policy.ratekeeper_soft_limit_bytes > 0)
                });
                if ratekeeping_required
                    && !generation_fence_faults.ratekeeper_allow_stage_without_reservation
                    && !domain
                        .capacity_reservations
                        .get(&capacity_key)
                        .is_some_and(|reservation| reservation.transaction == *transaction)
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::RatekeepingRequired,
                        None,
                    );
                }
                let conflict = partitioned_decision.is_none()
                    && transaction.read_conflicts.iter().any(|read| {
                        domain.committed_writes.iter().any(|write| {
                            write.sequence > transaction.read_version.sequence
                                && read.overlaps(&write.range)
                        }) || domain.staged_transactions.iter().any(|staged| {
                            !staged.aborted
                                && staged.commit_sequence > transaction.read_version.sequence
                                && staged
                                    .transaction
                                    .write_conflicts
                                    .iter()
                                    .any(|write| read.overlaps(write))
                        })
                    });
                if conflict {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::Conflict,
                        None,
                    );
                }
                let previous_log_chain = domain
                    .staged_transactions
                    .iter()
                    .rev()
                    .find(|staged| {
                        generation_fence_faults.reuse_aborted_sequence_or_chain || !staged.aborted
                    })
                    .map_or(domain.previous_log_chain, |staged| {
                        Sha256::digest(&staged.envelope).into()
                    });
                let commit_sequence = domain
                    .staged_transactions
                    .iter()
                    .rev()
                    .find(|staged| {
                        !generation_fence_faults.reuse_aborted_sequence_or_chain || !staged.aborted
                    })
                    .map_or(domain.latest_sequence, |staged| staged.commit_sequence)
                    .saturating_add(1);
                let read_conflicts = serde_json::to_vec(&transaction.read_conflicts)
                    .expect("validated staged read conflicts serialize");
                let write_conflicts = serde_json::to_vec(&transaction.write_conflicts)
                    .expect("validated staged write conflicts serialize");
                let canonical_mutations = serde_json::to_vec(&transaction.mutations)
                    .expect("validated staged mutations serialize");
                let required_resolvers = transaction.partitioned_resolution.as_ref().map_or_else(
                    || REQUIRED_RESOLVERS.to_vec(),
                    |resolution| {
                        resolution
                            .attestations
                            .iter()
                            .map(|attestation| attestation.statement.resolver_id)
                            .collect()
                    },
                );
                let mut client_id = [0_u8; 16];
                client_id[8..].copy_from_slice(&transaction.identity.client_id.to_be_bytes());
                let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
                    cell_id: transaction.cell_id,
                    tenant_id: transaction.tenant_id,
                    generation: transaction.generation,
                    version: Version::from_parts(transaction.generation, commit_sequence),
                    log_index,
                    client_id,
                    request_id: transaction.identity.request_id,
                    resolver_set_id: RESOLVER_SET_ID,
                    read_conflicts,
                    write_conflicts,
                    canonical_mutations,
                    required_resolvers,
                    required_log_tags: transaction.durable_log_tags.clone(),
                    previous_log_chain,
                });
                let encoded_envelope = envelope.encode();
                domain.staged_transactions.push(StagedTransaction {
                    identity: command.transaction_identity,
                    transaction: transaction.clone(),
                    commit_sequence,
                    envelope: encoded_envelope.clone(),
                    receipts: BTreeMap::new(),
                    certificates: BTreeMap::new(),
                    visible: false,
                    aborted: false,
                });
                if let Some(reservation) = domain.capacity_reservations.remove(&capacity_key) {
                    for (log_set_id, samples) in reservation.sample_epochs {
                        let retained = domain.capacity_sample_epochs.entry(log_set_id).or_default();
                        for (signer_id, sample_epoch) in samples {
                            retained
                                .entry(signer_id)
                                .and_modify(|current| *current = (*current).max(sample_epoch))
                                .or_insert(sample_epoch);
                        }
                    }
                }
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::Staged,
                    Some(encoded_envelope),
                )
            }
            CellStagedTransactionAction::RecordLogReceipt { receipt } => {
                let Some(staged_index) = domain
                    .staged_transactions
                    .iter()
                    .position(|staged| staged.identity == command.transaction_identity)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                };
                let staged = &domain.staged_transactions[staged_index];
                if staged.aborted {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyAborted,
                        None,
                    );
                }
                if domain.log_set_policies.contains_key(&receipt.log_set_id) {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidLogReceipt,
                        None,
                    );
                }
                let required = staged
                    .transaction
                    .durable_log_tags
                    .contains(&receipt.log_set_id);
                let digest: [u8; 32] = Sha256::digest(&staged.envelope).into();
                let quorum = receipt
                    .quorum_node_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                if receipt.format_version != 1
                    || receipt.generation != command.generation
                    || command.credential != staged.transaction.credential
                    || receipt.envelope_sha256 != digest
                    || receipt.durable_position == 0
                    || quorum.len() < 2
                    || quorum.len() != receipt.quorum_node_ids.len()
                    || !required
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidLogReceipt,
                        None,
                    );
                }
                if let Some(existing) = staged.receipts.get(&receipt.log_set_id) {
                    let status = if existing == receipt {
                        CellStagedTransactionStatus::LogReceiptRecorded
                    } else {
                        CellStagedTransactionStatus::ConflictingLogReceipt
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                domain.staged_transactions[staged_index]
                    .receipts
                    .insert(receipt.log_set_id, receipt.clone());
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::LogReceiptRecorded,
                    None,
                )
            }
            CellStagedTransactionAction::RecordLogCertificate { certificate } => {
                let Some(staged_index) = domain
                    .staged_transactions
                    .iter()
                    .position(|staged| staged.identity == command.transaction_identity)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                };
                let staged = &domain.staged_transactions[staged_index];
                if staged.aborted {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyAborted,
                        None,
                    );
                }
                let Some(policy) = domain
                    .log_set_policies
                    .get(&certificate.statement.log_set_id)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidLogCertificate,
                        None,
                    );
                };
                if command.credential != staged.transaction.credential
                    || !verify_tagged_log_certificate(
                        certificate,
                        policy,
                        staged,
                        command.cell_id,
                        command.tenant_id,
                        command.generation,
                    )
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidLogCertificate,
                        None,
                    );
                }
                let log_set_id = certificate.statement.log_set_id;
                if let Some(existing) = staged.certificates.get(&log_set_id) {
                    let status = if existing == certificate {
                        CellStagedTransactionStatus::LogCertificateRecorded
                    } else {
                        CellStagedTransactionStatus::ConflictingLogCertificate
                    };
                    return staged_response(domain, command.transaction_identity, status, None);
                }
                domain.staged_transactions[staged_index]
                    .certificates
                    .insert(log_set_id, certificate.clone());
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::LogCertificateRecorded,
                    None,
                )
            }
            CellStagedTransactionAction::Publish => {
                let Some(staged_index) = domain
                    .staged_transactions
                    .iter()
                    .position(|staged| staged.identity == command.transaction_identity)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                };
                if domain.staged_transactions[staged_index].visible {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyCommitted,
                        None,
                    );
                }
                if domain.staged_transactions[staged_index].aborted {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyAborted,
                        None,
                    );
                }
                if command.credential
                    != domain.staged_transactions[staged_index]
                        .transaction
                        .credential
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidRequest,
                        None,
                    );
                }
                let complete = domain.staged_transactions[staged_index]
                    .transaction
                    .durable_log_tags
                    .iter()
                    .all(|log_set| {
                        if domain.log_set_policies.contains_key(log_set) {
                            domain.staged_transactions[staged_index]
                                .certificates
                                .contains_key(log_set)
                        } else {
                            domain.staged_transactions[staged_index]
                                .receipts
                                .contains_key(log_set)
                        }
                    });
                let earlier_pending = domain.staged_transactions.iter().any(|staged| {
                    !staged.visible
                        && !staged.aborted
                        && staged.commit_sequence
                            < domain.staged_transactions[staged_index].commit_sequence
                });
                if !complete || earlier_pending {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::MissingLogReceipt,
                        None,
                    );
                }
                let envelope = publish_staged_transaction(domain, staged_index, None);
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::Committed,
                    Some(envelope),
                )
            }
            CellStagedTransactionAction::TakeoverPublish {
                previous_generation,
                recovery_id,
                expected_commit_sequence,
                expected_envelope_sha256,
            } => {
                let Some(staged_index) = domain
                    .staged_transactions
                    .iter()
                    .position(|staged| staged.identity == command.transaction_identity)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidGenerationTakeover,
                        None,
                    );
                };
                if domain.staged_transactions[staged_index].visible
                    && domain.generation == command.generation
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyCommitted,
                        None,
                    );
                }
                let staged = &domain.staged_transactions[staged_index];
                let envelope_digest: [u8; 32] = Sha256::digest(&staged.envelope).into();
                let envelope_exact =
                    CommitEnvelope::decode(&staged.envelope).is_ok_and(|envelope| {
                        envelope.generation() == *previous_generation
                            && envelope.version().sequence() == *expected_commit_sequence
                    });
                let certificates_complete = staged
                    .transaction
                    .durable_log_tags
                    .iter()
                    .all(|log_set| staged.certificates.contains_key(log_set));
                let unresolved_count = domain
                    .staged_transactions
                    .iter()
                    .filter(|candidate| !candidate.visible && !candidate.aborted)
                    .count();
                let successor_authorized = command.credential.as_ref().is_some_and(|credential| {
                    credential.generation == command.generation
                        && (generation_authority
                            .authorizes(credential.generation, &credential.transaction_system_id)
                            || (generation_fence_faults.accept_apply_during_recovery
                                && generation_authority.authorizes_recovery(
                                    credential.generation,
                                    *recovery_id,
                                    &credential.transaction_system_id,
                                )))
                });
                if !successor_authorized
                    || (!generation_fence_faults.accept_apply_during_recovery
                        && generation_authority.last_completed_recovery_id != Some(*recovery_id))
                    || command.generation != previous_generation.saturating_add(1)
                    || domain.generation != *previous_generation
                    || staged.transaction.generation != *previous_generation
                    || (!generation_fence_faults.ignore_staged_head_takeover_expectation
                        && (staged.commit_sequence != *expected_commit_sequence
                            || envelope_digest != *expected_envelope_sha256))
                    || !envelope_exact
                    || (!generation_fence_faults.accept_incomplete_staged_head
                        && !certificates_complete)
                    || unresolved_count != 1
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidGenerationTakeover,
                        None,
                    );
                }
                let envelope =
                    publish_staged_transaction(domain, staged_index, Some(command.generation));
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::Committed,
                    Some(envelope),
                )
            }
            CellStagedTransactionAction::TakeoverAbort {
                previous_generation,
                recovery_id,
                expected_commit_sequence,
                expected_envelope_sha256,
                log_set_fences,
            } => {
                let Some(staged_index) = domain
                    .staged_transactions
                    .iter()
                    .position(|staged| staged.identity == command.transaction_identity)
                else {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidGenerationTakeover,
                        None,
                    );
                };
                if domain.staged_transactions[staged_index].aborted
                    && domain.generation == command.generation
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::AlreadyAborted,
                        None,
                    );
                }
                let staged = &domain.staged_transactions[staged_index];
                let envelope_digest: [u8; 32] = Sha256::digest(&staged.envelope).into();
                let envelope_exact =
                    CommitEnvelope::decode(&staged.envelope).is_ok_and(|envelope| {
                        envelope.generation() == *previous_generation
                            && envelope.version().sequence() == *expected_commit_sequence
                    });
                let unresolved_count = domain
                    .staged_transactions
                    .iter()
                    .filter(|candidate| !candidate.visible && !candidate.aborted)
                    .count();
                let successor_authorized = command.credential.as_ref().is_some_and(|credential| {
                    credential.generation == command.generation
                        && (generation_authority
                            .authorizes(credential.generation, &credential.transaction_system_id)
                            || (generation_fence_faults.accept_apply_during_recovery
                                && generation_authority.authorizes_recovery(
                                    credential.generation,
                                    *recovery_id,
                                    &credential.transaction_system_id,
                                )))
                });
                let required_log_sets = staged
                    .transaction
                    .durable_log_tags
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                let fenced_log_sets = log_set_fences
                    .iter()
                    .map(|certificate| certificate.statement.log_set_id)
                    .collect::<BTreeSet<_>>();
                let every_fence_valid = fenced_log_sets == required_log_sets
                    && fenced_log_sets.len() == log_set_fences.len()
                    && log_set_fences.iter().all(|certificate| {
                        domain
                            .log_set_policies
                            .get(&certificate.statement.log_set_id)
                            .is_some_and(|policy| {
                                verify_tagged_log_fence_certificate(
                                    certificate,
                                    policy,
                                    staged,
                                    command.cell_id,
                                    command.tenant_id,
                                    *previous_generation,
                                    *recovery_id,
                                )
                            })
                    });
                let missing_durability = required_log_sets
                    .iter()
                    .filter(|log_set| !staged.certificates.contains_key(log_set))
                    .copied()
                    .collect::<BTreeSet<_>>();
                let missing_set_has_absence_quorum = log_set_fences.iter().any(|certificate| {
                    missing_durability.contains(&certificate.statement.log_set_id)
                        && domain
                            .log_set_policies
                            .get(&certificate.statement.log_set_id)
                            .is_some_and(|policy| {
                                certificate
                                    .attestations
                                    .iter()
                                    .filter(|attestation| !attestation.record_present)
                                    .count()
                                    >= usize::from(policy.quorum_size)
                            })
                });
                let proof_valid = !missing_durability.is_empty()
                    && every_fence_valid
                    && missing_set_has_absence_quorum;
                if !successor_authorized
                    || (!generation_fence_faults.accept_apply_during_recovery
                        && generation_authority.last_completed_recovery_id != Some(*recovery_id))
                    || command.generation != previous_generation.saturating_add(1)
                    || domain.generation != *previous_generation
                    || staged.transaction.generation != *previous_generation
                    || staged.visible
                    || staged.commit_sequence != *expected_commit_sequence
                    || envelope_digest != *expected_envelope_sha256
                    || !envelope_exact
                    || (!generation_fence_faults.accept_invalid_staged_abort_proof && !proof_valid)
                    || unresolved_count != 1
                {
                    return staged_response(
                        domain,
                        command.transaction_identity,
                        CellStagedTransactionStatus::InvalidGenerationTakeover,
                        None,
                    );
                }
                domain.staged_transactions[staged_index].aborted = true;
                domain.generation = command.generation;
                staged_response(
                    domain,
                    command.transaction_identity,
                    CellStagedTransactionStatus::Aborted,
                    None,
                )
            }
            CellStagedTransactionAction::TakeoverRecoverPrefix {
                previous_generation,
                recovery_id,
                staged_window,
                log_set_inventories,
            } => apply_takeover_recover_prefix(
                domain,
                command,
                generation_authority,
                generation_fence_faults,
                *previous_generation,
                *recovery_id,
                staged_window,
                log_set_inventories,
            ),
        }
    }

    pub(crate) fn snapshots(&self) -> Vec<CellStateSnapshot> {
        self.domains
            .iter()
            .map(|domain| CellStateSnapshot {
                cell_id: domain.cell_id,
                tenant_id: domain.tenant_id,
                generation: domain.generation,
                latest_sequence: domain.latest_sequence,
                rows: domain
                    .rows
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                committed_envelopes: domain.committed_envelopes.clone(),
                log_set_policies: domain.log_set_policies.values().cloned().collect(),
                pending_log_set_policy_transition: domain.pending_log_set_policy_transition.clone(),
                completed_log_set_policy_transitions: domain
                    .completed_log_set_policy_transitions
                    .values()
                    .cloned()
                    .collect(),
            })
            .collect()
    }

    pub(crate) fn committed_envelope_feed(
        &self,
        request: &CellCommittedEnvelopeRequest,
        authority_position: RecoveryLogPosition,
    ) -> Result<CellCommittedEnvelopeFeed, String> {
        if request.generation == 0
            || request.after_version >= request.through_version
            || request.through_version > authority_position.index
        {
            return Err("committed-envelope request has an invalid version bound".to_owned());
        }
        let domain = self
            .domains
            .iter()
            .find(|domain| {
                domain.cell_id == request.cell_id && domain.tenant_id == request.tenant_id
            })
            .ok_or_else(|| "committed-envelope request names an unknown domain".to_owned())?;
        if domain.generation != request.generation {
            return Err("committed-envelope request generation is stale".to_owned());
        }
        if request.through_version > domain.latest_sequence {
            return Err(format!(
                "committed-envelope request target {} exceeds latest commit {}",
                request.through_version, domain.latest_sequence
            ));
        }
        let mut envelopes = Vec::new();
        for encoded in &domain.committed_envelopes {
            let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
            let version = envelope.version().sequence();
            if version > request.after_version && version <= request.through_version {
                envelopes.push(encoded.clone());
            }
        }
        let last = envelopes
            .last()
            .ok_or_else(|| "committed-envelope request returned an empty suffix".to_owned())
            .and_then(|encoded| {
                CommitEnvelope::decode(encoded)
                    .map(|envelope| envelope.version().sequence())
                    .map_err(|error| error.to_string())
            })?;
        if last != request.through_version {
            return Err(format!(
                "committed-envelope suffix ends at {last}, expected {}",
                request.through_version
            ));
        }
        Ok(CellCommittedEnvelopeFeed {
            authority_position,
            cell_id: request.cell_id,
            tenant_id: request.tenant_id,
            generation: request.generation,
            after_version: request.after_version,
            through_version: request.through_version,
            latest_commit_version: domain.latest_sequence,
            envelopes,
        })
    }
}

fn publish_staged_transaction(
    domain: &mut CellDomainState,
    staged_index: usize,
    next_generation: Option<u64>,
) -> Vec<u8> {
    let transaction = domain.staged_transactions[staged_index].transaction.clone();
    let commit_sequence = domain.staged_transactions[staged_index].commit_sequence;
    let envelope = domain.staged_transactions[staged_index].envelope.clone();
    for mutation in &transaction.mutations {
        match mutation {
            CellMutation::Clear { key } => {
                domain.rows.remove(key);
            }
            CellMutation::Set { key, value } => {
                domain.rows.insert(key.clone(), value.clone());
            }
        }
    }
    for range in &transaction.write_conflicts {
        domain.committed_writes.push(CommittedWrite {
            sequence: commit_sequence,
            range: range.clone(),
        });
    }
    domain.generation = next_generation.unwrap_or(transaction.generation);
    domain.latest_sequence = commit_sequence;
    domain.previous_log_chain = Sha256::digest(&envelope).into();
    domain.committed_envelopes.push(envelope.clone());
    domain.staged_transactions[staged_index].visible = true;
    envelope
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn apply_takeover_recover_prefix(
    domain: &mut CellDomainState,
    command: &CellStagedTransactionCommand,
    generation_authority: &GenerationAuthorityState,
    faults: GenerationFenceFaults,
    previous_generation: u64,
    recovery_id: u64,
    staged_window: &CellStagedWindow,
    log_set_inventories: &[CellTaggedLogPrefixFenceCertificate],
) -> CellStagedTransactionApplyResponse {
    let invalid = |domain: &CellDomainState| {
        staged_response(
            domain,
            command.transaction_identity,
            CellStagedTransactionStatus::InvalidStagedPrefix,
            None,
        )
    };
    let successor_authorized = command.credential.as_ref().is_some_and(|credential| {
        credential.generation == command.generation
            && generation_authority
                .authorizes(credential.generation, &credential.transaction_system_id)
    });
    if !successor_authorized
        || generation_authority.last_completed_recovery_id != Some(recovery_id)
        || command.generation != previous_generation.saturating_add(1)
        || !staged_window.valid_identity()
    {
        return invalid(domain);
    }

    let mut staged_indexes = Vec::with_capacity(staged_window.records.len());
    for expected in &staged_window.records {
        let Some(index) = domain
            .staged_transactions
            .iter()
            .position(|staged| staged.identity == expected.transaction_identity)
        else {
            return invalid(domain);
        };
        let staged = &domain.staged_transactions[index];
        let digest: [u8; 32] = Sha256::digest(&staged.envelope).into();
        if staged.commit_sequence != expected.commit_sequence
            || digest != expected.envelope_sha256
            || staged.transaction.generation != previous_generation
        {
            return invalid(domain);
        }
        staged_indexes.push(index);
    }
    let encoded_bytes = staged_indexes.iter().fold(0_u64, |total, index| {
        total.saturating_add(
            u64::try_from(domain.staged_transactions[*index].envelope.len()).unwrap_or(u64::MAX),
        )
    });
    let within_limit = staged_window.records.len() <= MAX_STAGED_PREFIX_RECORDS
        && encoded_bytes <= MAX_STAGED_PREFIX_BYTES;
    if staged_window.encoded_bytes != encoded_bytes
        || (!faults.accept_over_limit_staged_window && !within_limit)
        || staged_indexes.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return invalid(domain);
    }

    let retry = domain.generation == command.generation
        && staged_indexes.iter().all(|index| {
            let staged = &domain.staged_transactions[*index];
            staged.visible || staged.aborted
        });
    if !retry {
        let unresolved = domain
            .staged_transactions
            .iter()
            .enumerate()
            .filter(|(_, staged)| !staged.visible && !staged.aborted)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if domain.generation != previous_generation || unresolved != staged_indexes {
            return invalid(domain);
        }
    }

    let chain_exact = staged_indexes.iter().enumerate().all(|(offset, index)| {
        let staged = &domain.staged_transactions[*index];
        CommitEnvelope::decode(&staged.envelope).is_ok_and(|envelope| {
            let expected_previous = if offset == 0 {
                if retry {
                    envelope.previous_log_chain()
                } else {
                    domain.previous_log_chain
                }
            } else {
                Sha256::digest(&domain.staged_transactions[staged_indexes[offset - 1]].envelope)
                    .into()
            };
            envelope.generation() == previous_generation
                && envelope.version().sequence() == staged.commit_sequence
                && envelope.previous_log_chain() == expected_previous
        })
    });
    if !chain_exact {
        return invalid(domain);
    }

    let required_log_sets = staged_indexes
        .first()
        .map(|index| {
            domain.staged_transactions[*index]
                .transaction
                .durable_log_tags
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if required_log_sets.is_empty()
        || staged_indexes.iter().any(|index| {
            domain.staged_transactions[*index]
                .transaction
                .durable_log_tags
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                != required_log_sets
        })
    {
        return invalid(domain);
    }
    let supplied_log_sets = log_set_inventories
        .iter()
        .map(|certificate| certificate.statement.log_set_id)
        .collect::<BTreeSet<_>>();
    let inventory_set_valid = if faults.accept_missing_staged_inventory {
        !supplied_log_sets.is_empty() && supplied_log_sets.is_subset(&required_log_sets)
    } else {
        supplied_log_sets == required_log_sets
    };
    if !inventory_set_valid
        || supplied_log_sets.len() != log_set_inventories.len()
        || log_set_inventories.iter().any(|certificate| {
            domain
                .log_set_policies
                .get(&certificate.statement.log_set_id)
                .is_none_or(|policy| {
                    !verify_tagged_log_prefix_fence_certificate(
                        certificate,
                        policy,
                        staged_window,
                        command.cell_id,
                        command.tenant_id,
                        previous_generation,
                        recovery_id,
                    )
                })
        })
    {
        return invalid(domain);
    }

    let mut recovered_count = 0_usize;
    let mut absent_boundary = None;
    for (offset, record) in staged_window.records.iter().enumerate() {
        let mut present_everywhere = true;
        let mut absent_somewhere = false;
        for certificate in log_set_inventories {
            let Some(policy) = domain
                .log_set_policies
                .get(&certificate.statement.log_set_id)
            else {
                return invalid(domain);
            };
            let quorum = usize::from(policy.quorum_size);
            let matching = |attestation: &CellTaggedLogPrefixFenceAttestation| {
                attestation
                    .observations
                    .iter()
                    .find(|observation| {
                        observation.transaction_identity == record.transaction_identity
                            && observation.commit_sequence == record.commit_sequence
                            && observation.envelope_sha256 == record.envelope_sha256
                    })
                    .map(|observation| observation.record_present)
            };
            let present = certificate
                .attestations
                .iter()
                .filter(|attestation| matching(attestation) == Some(true))
                .count();
            let absent = certificate
                .attestations
                .iter()
                .filter(|attestation| matching(attestation) == Some(false))
                .count();
            if present >= quorum && absent >= quorum {
                return invalid(domain);
            }
            present_everywhere &= present >= quorum;
            absent_somewhere |= absent >= quorum;
        }
        if present_everywhere {
            if absent_boundary.is_some() {
                return invalid(domain);
            }
            recovered_count = offset.saturating_add(1);
        } else if absent_somewhere {
            absent_boundary = Some(offset);
            break;
        } else {
            return invalid(domain);
        }
    }
    if absent_boundary.is_none() && recovered_count != staged_indexes.len() {
        return invalid(domain);
    }
    if faults.abort_quorum_present_staged_record && recovered_count > 0 {
        recovered_count = recovered_count.saturating_sub(1);
    }
    if faults.publish_beyond_staged_absence && recovered_count < staged_indexes.len() {
        recovered_count = recovered_count.saturating_add(1);
    }

    if retry {
        let recovered_records = staged_indexes
            .iter()
            .filter(|index| domain.staged_transactions[**index].visible)
            .count();
        let aborted_records = staged_indexes
            .iter()
            .filter(|index| domain.staged_transactions[**index].aborted)
            .count();
        return staged_prefix_response(
            domain,
            command.transaction_identity,
            CellStagedTransactionStatus::AlreadyPrefixRecovered,
            recovered_records,
            aborted_records,
        );
    }

    for (offset, index) in staged_indexes.iter().copied().enumerate() {
        if offset < recovered_count {
            if faults.skip_recoverable_staged_prefix && offset == 0 {
                domain.staged_transactions[index].aborted = true;
            } else {
                publish_staged_transaction(domain, index, Some(command.generation));
            }
        } else if faults.retain_aborted_staged_suffix
            && offset == staged_indexes.len().saturating_sub(1)
        {
            publish_staged_transaction(domain, index, Some(command.generation));
        } else {
            domain.staged_transactions[index].aborted = true;
        }
    }
    domain.generation = command.generation;
    let recovered_records = staged_indexes
        .iter()
        .filter(|index| domain.staged_transactions[**index].visible)
        .count();
    let aborted_records = staged_indexes
        .iter()
        .filter(|index| domain.staged_transactions[**index].aborted)
        .count();
    staged_prefix_response(
        domain,
        command.transaction_identity,
        CellStagedTransactionStatus::PrefixRecovered,
        recovered_records,
        aborted_records,
    )
}

fn staged_prefix_response(
    domain: &CellDomainState,
    transaction_identity: RequestIdentity,
    status: CellStagedTransactionStatus,
    recovered_records: usize,
    aborted_records: usize,
) -> CellStagedTransactionApplyResponse {
    let mut response = staged_response(domain, transaction_identity, status, None);
    response.recovered_records = u64::try_from(recovered_records).unwrap_or(u64::MAX);
    response.aborted_records = u64::try_from(aborted_records).unwrap_or(u64::MAX);
    response
}

fn valid_log_set_policy(policy: &CellLogSetPolicy, generation: u64) -> bool {
    let member_ids = policy
        .members
        .iter()
        .map(|member| member.node_id)
        .collect::<BTreeSet<_>>();
    policy.format_version == 1
        && policy.generation == generation
        && policy.policy_epoch > 0
        && policy.log_set_id > 0
        && policy.quorum_size > 0
        && (policy.ratekeeper_soft_limit_bytes == 0 || policy.ratekeeper_soft_limit_bytes >= 1024)
        && usize::from(policy.quorum_size) <= policy.members.len()
        && member_ids.len() == policy.members.len()
        && policy
            .members
            .iter()
            .all(|member| member.node_id > 0 && member.public_key.len() == 32)
}

fn valid_log_set_policy_transition(
    transition: &CellLogSetPolicyTransition,
    repair_readiness: &CellTaggedLogRepairCertificate,
    current: &CellLogSetPolicy,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> bool {
    let old_members = transition
        .old_policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let next_members = transition
        .next_policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let removed = old_members
        .keys()
        .filter(|node_id| !next_members.contains_key(node_id))
        .copied()
        .collect::<Vec<_>>();
    let added = next_members
        .keys()
        .filter(|node_id| !old_members.contains_key(node_id))
        .copied()
        .collect::<Vec<_>>();
    let unchanged_exact = old_members.iter().all(|(node_id, public_key)| {
        *node_id == transition.failed_node_id
            || next_members
                .get(node_id)
                .is_some_and(|next| next == public_key)
    });
    let repair = &repair_readiness.statement;
    transition.format_version == 1
        && transition.cell_id == cell_id
        && transition.tenant_id == tenant_id
        && transition.generation == generation
        && transition.transition_id > 0
        && transition.log_set_id == current.log_set_id
        && transition.old_policy == *current
        && valid_log_set_policy(&transition.next_policy, generation)
        && transition.next_policy.log_set_id == current.log_set_id
        && transition.next_policy.policy_epoch == current.policy_epoch.saturating_add(1)
        && transition.next_policy.quorum_size == current.quorum_size
        && transition.next_policy.ratekeeper_soft_limit_bytes == current.ratekeeper_soft_limit_bytes
        && transition.next_policy.members.len() == current.members.len()
        && removed == [transition.failed_node_id]
        && added == [transition.learner_node_id]
        && unchanged_exact
        && next_members.get(&transition.learner_node_id)
            == Some(&transition.learner_public_key.as_slice())
        && transition.learner_incarnation != [0; 16]
        && transition.learner_public_key.len() == 32
        && repair.phase == CellTaggedLogRepairPhase::LearnerReady
        && repair.cell_id == cell_id
        && repair.tenant_id == tenant_id
        && repair.generation == generation
        && repair.log_set_id == transition.log_set_id
        && repair.policy_epoch == current.policy_epoch
        && repair.failed_node_id == transition.failed_node_id
        && repair.learner_node_id == transition.learner_node_id
        && repair.learner_incarnation == transition.learner_incarnation
        && repair.learner_public_key == transition.learner_public_key
        && repair.snapshot_sha256 == transition.retained_root_sha256
        && repair.last_position == transition.retained_last_position
        && cell_tagged_log_repair_certificate_sha256(repair_readiness)
            == transition.repair_readiness_sha256
        && verify_tagged_log_repair_certificate(repair_readiness, current)
}

fn verify_tagged_log_certificate(
    certificate: &CellTaggedLogCertificate,
    policy: &CellLogSetPolicy,
    staged: &StagedTransaction,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> bool {
    let statement = &certificate.statement;
    if statement.cell_id != cell_id
        || statement.tenant_id != tenant_id
        || statement.generation != generation
        || statement.transaction_identity != staged.identity
        || statement.commit_sequence != staged.commit_sequence
        || !staged
            .transaction
            .durable_log_tags
            .contains(&statement.log_set_id)
    {
        return false;
    }
    verify_tagged_log_envelope_certificate(certificate, policy, &staged.envelope)
}

fn verify_tagged_log_fence_certificate(
    certificate: &CellTaggedLogFenceCertificate,
    policy: &CellLogSetPolicy,
    staged: &StagedTransaction,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    recovery_id: u64,
) -> bool {
    let statement = &certificate.statement;
    let envelope_sha256: [u8; 32] = Sha256::digest(&staged.envelope).into();
    if statement.format_version != 1
        || statement.cell_id != cell_id
        || statement.tenant_id != tenant_id
        || statement.generation != generation
        || statement.recovery_id != recovery_id
        || statement.transaction_identity != staged.identity
        || statement.commit_sequence != staged.commit_sequence
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.envelope_sha256 != envelope_sha256
        || !staged
            .transaction
            .durable_log_tags
            .contains(&statement.log_set_id)
    {
        return false;
    }
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(attestation.record_present) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(policy.quorum_size)
}

fn verify_tagged_log_prefix_fence_certificate(
    certificate: &CellTaggedLogPrefixFenceCertificate,
    policy: &CellLogSetPolicy,
    staged_window: &CellStagedWindow,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    recovery_id: u64,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.cell_id != cell_id
        || statement.tenant_id != tenant_id
        || statement.generation != generation
        || statement.recovery_id != recovery_id
        || statement.log_set_id != policy.log_set_id
        || statement.policy_epoch != policy.policy_epoch
        || statement.window != *staged_window
        || !statement.window.valid_identity()
    {
        return false;
    }
    let expected_observations = staged_window
        .records
        .iter()
        .map(|record| {
            (
                record.transaction_identity,
                record.commit_sequence,
                record.envelope_sha256,
            )
        })
        .collect::<Vec<_>>();
    let members = policy
        .members
        .iter()
        .map(|member| (member.node_id, member.public_key.as_slice()))
        .collect::<BTreeMap<_, _>>();
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id)
            || attestation.observations.len() != expected_observations.len()
            || attestation
                .observations
                .iter()
                .zip(&expected_observations)
                .any(|(observation, expected)| {
                    (
                        observation.transaction_identity,
                        observation.commit_sequence,
                        observation.envelope_sha256,
                    ) != *expected
                })
        {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        let Ok(bytes) = statement.signing_bytes(&attestation.observations) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(policy.quorum_size)
}

fn staged_response(
    domain: &CellDomainState,
    transaction_identity: RequestIdentity,
    status: CellStagedTransactionStatus,
    envelope: Option<Vec<u8>>,
) -> CellStagedTransactionApplyResponse {
    let capacity_key = capacity_identity_key(transaction_identity);
    let staged = domain
        .staged_transactions
        .iter()
        .find(|staged| staged.identity == transaction_identity);
    CellStagedTransactionApplyResponse {
        status,
        transaction_identity,
        generation: domain.generation,
        commit_sequence: staged.map(|staged| staged.commit_sequence),
        envelope: envelope.or_else(|| staged.map(|staged| staged.envelope.clone())),
        durable_log_sets: staged.map_or_else(Vec::new, |staged| {
            staged
                .receipts
                .keys()
                .chain(staged.certificates.keys())
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        }),
        visible: staged.is_some_and(|staged| staged.visible),
        aborted: staged.is_some_and(|staged| staged.aborted),
        row_count: domain.rows.len() as u64,
        recovered_records: 0,
        aborted_records: 0,
        capacity_retry_token: domain.capacity_retry_tokens.get(&capacity_key).copied(),
        capacity_reservation_epoch: domain
            .capacity_reservations
            .get(&capacity_key)
            .map(|reservation| reservation.reservation_epoch)
            .or_else(|| domain.capacity_attempt_epochs.get(&capacity_key).copied()),
    }
}

fn capacity_identity_key(identity: RequestIdentity) -> String {
    format!("{:016x}:{:016x}", identity.client_id, identity.request_id)
}

fn capacity_retry_token(identity: RequestIdentity) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"okv-ratekeeper-retry-token-v1");
    digest.update(identity.client_id.to_be_bytes());
    digest.update(identity.request_id.to_be_bytes());
    digest.finalize().into()
}

fn verify_partitioned_resolution(
    command: &CellTransactionCommand,
    resolution: &CellPartitionedResolution,
) -> Result<CellResolverDecision, ()> {
    if resolution.map_epoch != 1 || resolution.candidate_sequence == 0 {
        return Err(());
    }
    let transaction_system_generation = if resolution.transaction_system_generation == 0 {
        command.generation
    } else {
        resolution.transaction_system_generation
    };
    let resolver_read_sequence = if resolution.resolver_read_sequence == 0 {
        command.read_version.sequence
    } else {
        resolution.resolver_read_sequence
    };
    let transaction_sha256 = cell_partitioned_transaction_sha256(command).map_err(|_| ())?;
    let map_sha256 = cell_resolver_map_sha256();
    let partitions = cell_resolver_partitions();
    let mut required = BTreeMap::new();
    for partition in &partitions {
        let read_conflicts = clipped_conflicts(&command.read_conflicts, partition);
        let write_conflicts = clipped_conflicts(&command.write_conflicts, partition);
        if !read_conflicts.is_empty() || !write_conflicts.is_empty() {
            required.insert(partition.resolver_id, (read_conflicts, write_conflicts));
        }
    }
    if required.is_empty() || resolution.attestations.len() != required.len() {
        return Err(());
    }
    let mut distinct = BTreeSet::new();
    let mut combined = CellResolverDecision::Accept;
    for attestation in &resolution.attestations {
        let statement = &attestation.statement;
        let Some((read_conflicts, write_conflicts)) = required.get(&statement.resolver_id) else {
            return Err(());
        };
        if !distinct.insert(statement.resolver_id)
            || statement.format_version != 1
            || statement.cell_id != command.cell_id
            || statement.tenant_id != command.tenant_id
            || statement.generation != transaction_system_generation
            || statement.map_epoch != resolution.map_epoch
            || statement.map_sha256 != map_sha256
            || statement.resolver_incarnation != cell_resolver_incarnation(statement.resolver_id)
            || statement.transaction_identity != command.identity
            || statement.candidate_sequence != resolution.candidate_sequence
            || statement.read_version != command.read_version
            || statement.resolver_read_sequence != resolver_read_sequence
            || statement.transaction_sha256 != transaction_sha256
            || &statement.read_conflicts != read_conflicts
            || &statement.write_conflicts != write_conflicts
        {
            return Err(());
        }
        let bytes = statement.signing_bytes().map_err(|_| ())?;
        let public_key =
            cell_resolver_public_key(statement.resolver_id, statement.resolver_incarnation);
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return Err(());
        }
        if statement.decision == CellResolverDecision::Conflict {
            combined = CellResolverDecision::Conflict;
        }
    }
    if distinct.len() != required.len() {
        return Err(());
    }
    Ok(combined)
}

fn clipped_conflicts(
    conflicts: &[CellKeyRange],
    partition: &CellResolverPartition,
) -> Vec<CellKeyRange> {
    let owned = CellKeyRange {
        start: partition.start.clone(),
        end: partition.end.clone(),
    };
    let mut clipped = conflicts
        .iter()
        .filter(|range| range.overlaps(&owned))
        .map(|range| CellKeyRange {
            start: std::cmp::max(range.start.clone(), owned.start.clone()),
            end: std::cmp::min(range.end.clone(), owned.end.clone()),
        })
        .collect::<Vec<_>>();
    clipped.sort();
    clipped.dedup();
    clipped
}

fn valid_request(command: &CellTransactionCommand) -> bool {
    if command.mutations.is_empty()
        || command
            .read_conflicts
            .iter()
            .chain(&command.write_conflicts)
            .any(|range| !range.valid())
    {
        return false;
    }
    let mutation_keys: BTreeSet<&[u8]> = command.mutations.iter().map(CellMutation::key).collect();
    mutation_keys.len() == command.mutations.len()
        && command.mutations.iter().all(|mutation| {
            command
                .write_conflicts
                .iter()
                .any(|range| range.contains(mutation.key()))
        })
}

fn contains_all(actual: &[u16], required: &[u16]) -> bool {
    let actual: BTreeSet<u16> = actual.iter().copied().collect();
    required.iter().all(|item| actual.contains(item))
}
