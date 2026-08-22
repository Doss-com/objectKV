use okv_model::Version;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display};

const MAGIC: &[u8; 4] = b"OKVC";
const CODEC_VERSION: u16 = 1;
const QUORUM: usize = 2;
const REQUIRED_RESOLVERS: [u16; 2] = [1, 2];
const REQUIRED_LOG_TAGS: [u16; 2] = [10, 20];
const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];
const RESOLVER_SET_ID: [u8; 16] = [0x33; 16];

/// Deliberately incorrect subject behavior used to prove one commit invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitContractMode {
    /// The intended contract model.
    Correct,
    /// Rebuild no client outcomes after a process restart.
    RamOnlyDedup,
    /// Treat a different request under one retained identity as the old commit.
    AcceptConflictingRetry,
    /// Commit after only a subset of required resolvers accepts.
    AcceptPartialResolver,
    /// Commit an envelope missing one required durable-log tag.
    OmitRequiredLogTag,
    /// Commit a request from a fenced transaction-system generation.
    AcceptStaleGeneration,
    /// Acknowledge a commit after only one WAL replica fsyncs it.
    AckBeforeQuorum,
}

impl CommitContractMode {
    /// Stable configuration identifier used by eval suites and artifact refs.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::RamOnlyDedup => "ram_only_dedup",
            Self::AcceptConflictingRetry => "accept_conflicting_retry",
            Self::AcceptPartialResolver => "accept_partial_resolver",
            Self::OmitRequiredLogTag => "omit_required_log_tag",
            Self::AcceptStaleGeneration => "accept_stale_generation",
            Self::AckBeforeQuorum => "ack_before_quorum",
        }
    }
}

/// Compact result from the deterministic commit contract scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitContractReport {
    pub seed: u64,
    pub mode: CommitContractMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub acknowledged_commits: u64,
    pub recovered_commits: u64,
    pub retry_count: u64,
    pub leader_only_attempts: u64,
    pub trace_sha256: String,
}

/// Model codec for the proposed Cell v0 commit envelope.
///
/// This freezes the fields and failure checks exercised by the contract model.
/// It is not yet the stable production WAL format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitEnvelope {
    codec_version: u16,
    envelope_len: u32,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    version: Version,
    log_index: u64,
    client_id: [u8; 16],
    request_id: u64,
    resolver_set_id: [u8; 16],
    logical_fingerprint: [u8; 32],
    read_conflicts: Vec<u8>,
    write_conflicts: Vec<u8>,
    canonical_mutations: Vec<u8>,
    required_resolvers: Vec<u16>,
    required_log_tags: Vec<u16>,
    previous_log_chain: [u8; 32],
    checksum: [u8; 32],
}

impl CommitEnvelope {
    fn new(
        request: &CommitRequest,
        version: Version,
        log_index: u64,
        required_log_tags: Vec<u16>,
        previous_log_chain: [u8; 32],
    ) -> Self {
        let mut envelope = Self {
            codec_version: CODEC_VERSION,
            envelope_len: 0,
            cell_id: request.cell_id,
            tenant_id: request.tenant_id,
            generation: request.generation,
            version,
            log_index,
            client_id: request.client_id,
            request_id: request.request_id,
            resolver_set_id: RESOLVER_SET_ID,
            logical_fingerprint: request.fingerprint(),
            read_conflicts: request.read_conflicts.clone(),
            write_conflicts: request.write_conflicts.clone(),
            canonical_mutations: request.canonical_mutations.clone(),
            required_resolvers: REQUIRED_RESOLVERS.to_vec(),
            required_log_tags,
            previous_log_chain,
            checksum: [0; 32],
        };
        envelope.envelope_len = u32::try_from(envelope.encode_without_checksum().len() + 32)
            .expect("model envelope fits u32");
        envelope.checksum = digest(&envelope.encode_without_checksum());
        envelope
    }

    /// Encode the complete envelope with a checksum over every preceding byte.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = self.encode_without_checksum();
        bytes.extend_from_slice(&self.checksum);
        bytes
    }

    /// Decode and validate the model envelope.
    ///
    /// # Errors
    ///
    /// Returns a typed error for truncation, bad magic, unsupported codec,
    /// checksum mismatch, invalid generation/version binding, or trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CommitCodecError> {
        if bytes.len() < 32 {
            return Err(CommitCodecError::Truncated);
        }
        let payload_len = bytes.len() - 32;
        let (payload, stored_checksum) = bytes.split_at(payload_len);
        if digest(payload).as_slice() != stored_checksum {
            return Err(CommitCodecError::ChecksumMismatch);
        }

        let mut reader = Reader::new(payload);
        if reader.take::<4>()? != *MAGIC {
            return Err(CommitCodecError::BadMagic);
        }
        let codec_version = reader.u16()?;
        if codec_version != CODEC_VERSION {
            return Err(CommitCodecError::UnsupportedCodec(codec_version));
        }
        let envelope_len = reader.u32()?;
        if usize::try_from(envelope_len).ok() != Some(bytes.len()) {
            return Err(CommitCodecError::InvalidLength);
        }
        let cell_id = reader.take::<16>()?;
        let tenant_id = reader.take::<16>()?;
        let generation = reader.u64()?;
        let version = Version::from_be_bytes(reader.take::<16>()?);
        if version == Version::ZERO || version.generation() != generation {
            return Err(CommitCodecError::InvalidGenerationVersion);
        }
        let log_index = reader.u64()?;
        let client_id = reader.take::<16>()?;
        let request_id = reader.u64()?;
        let resolver_set_id = reader.take::<16>()?;
        let logical_fingerprint = reader.take::<32>()?;
        let read_conflicts = reader.byte_vec()?;
        let write_conflicts = reader.byte_vec()?;
        let canonical_mutations = reader.byte_vec()?;
        let required_resolvers = reader.u16_vec()?;
        let required_log_tags = reader.u16_vec()?;
        let previous_log_chain = reader.take::<32>()?;
        if !reader.is_empty() {
            return Err(CommitCodecError::TrailingBytes);
        }
        if required_resolvers.is_empty()
            || required_log_tags.is_empty()
            || !strictly_sorted(&required_resolvers)
            || !strictly_sorted(&required_log_tags)
        {
            return Err(CommitCodecError::InvalidSet);
        }
        let request = CommitRequest {
            cell_id,
            tenant_id,
            generation,
            client_id,
            request_id,
            read_conflicts: read_conflicts.clone(),
            write_conflicts: write_conflicts.clone(),
            canonical_mutations: canonical_mutations.clone(),
        };
        if request.fingerprint() != logical_fingerprint {
            return Err(CommitCodecError::FingerprintMismatch);
        }
        let mut checksum = [0; 32];
        checksum.copy_from_slice(stored_checksum);
        Ok(Self {
            codec_version,
            envelope_len,
            cell_id,
            tenant_id,
            generation,
            version,
            log_index,
            client_id,
            request_id,
            resolver_set_id,
            logical_fingerprint,
            read_conflicts,
            write_conflicts,
            canonical_mutations,
            required_resolvers,
            required_log_tags,
            previous_log_chain,
            checksum,
        })
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn version(&self) -> Version {
        self.version
    }

    pub(crate) const fn log_index(&self) -> u64 {
        self.log_index
    }

    pub(crate) const fn client_identity(&self) -> ([u8; 16], u64) {
        (self.client_id, self.request_id)
    }

    pub(crate) const fn logical_fingerprint(&self) -> [u8; 32] {
        self.logical_fingerprint
    }

    pub(crate) const fn previous_log_chain(&self) -> [u8; 32] {
        self.previous_log_chain
    }

    fn encode_without_checksum(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.codec_version.to_be_bytes());
        bytes.extend_from_slice(&self.envelope_len.to_be_bytes());
        bytes.extend_from_slice(&self.cell_id);
        bytes.extend_from_slice(&self.tenant_id);
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.version.to_be_bytes());
        bytes.extend_from_slice(&self.log_index.to_be_bytes());
        bytes.extend_from_slice(&self.client_id);
        bytes.extend_from_slice(&self.request_id.to_be_bytes());
        bytes.extend_from_slice(&self.resolver_set_id);
        bytes.extend_from_slice(&self.logical_fingerprint);
        push_byte_vec(&mut bytes, &self.read_conflicts);
        push_byte_vec(&mut bytes, &self.write_conflicts);
        push_byte_vec(&mut bytes, &self.canonical_mutations);
        push_u16_vec(&mut bytes, &self.required_resolvers);
        push_u16_vec(&mut bytes, &self.required_log_tags);
        bytes.extend_from_slice(&self.previous_log_chain);
        bytes
    }
}

pub(crate) fn fixture_envelope(
    seed: u64,
    ordinal: u64,
    generation: u64,
    sequence: u64,
    log_index: u64,
    previous_log_chain: [u8; 32],
) -> CommitEnvelope {
    CommitEnvelope::new(
        &request(seed, ordinal, generation),
        Version::from_parts(generation, sequence),
        log_index,
        REQUIRED_LOG_TAGS.to_vec(),
        previous_log_chain,
    )
}

/// Commit envelope decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitCodecError {
    Truncated,
    BadMagic,
    UnsupportedCodec(u16),
    InvalidLength,
    ChecksumMismatch,
    InvalidGenerationVersion,
    FingerprintMismatch,
    InvalidSet,
    TrailingBytes,
}

impl Display for CommitCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CommitCodecError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommitRequest {
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    client_id: [u8; 16],
    request_id: u64,
    read_conflicts: Vec<u8>,
    write_conflicts: Vec<u8>,
    canonical_mutations: Vec<u8>,
}

impl CommitRequest {
    fn fingerprint(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"okv-commit-request-v1");
        hasher.update(self.cell_id);
        hasher.update(self.tenant_id);
        hasher.update(self.generation.to_be_bytes());
        hasher.update(self.client_id);
        hasher.update(self.request_id.to_be_bytes());
        hash_byte_vec(&mut hasher, &self.read_conflicts);
        hash_byte_vec(&mut hasher, &self.write_conflicts);
        hash_byte_vec(&mut hasher, &self.canonical_mutations);
        hasher.finalize().into()
    }

    fn key(&self) -> ([u8; 16], u64) {
        (self.client_id, self.request_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RejectReason {
    ConflictingRetry,
    MissingResolver,
    MissingLogTag,
    StaleGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommitOutcome {
    Committed(Version),
    Unknown,
    Unavailable,
    Rejected(RejectReason),
}

#[derive(Clone, Debug)]
struct CommitCertificate {
    acknowledgers: Vec<u8>,
}

impl CommitCertificate {
    fn is_quorum(&self) -> bool {
        let unique: BTreeSet<u8> = self
            .acknowledgers
            .iter()
            .copied()
            .filter(|replica_id| usize::from(*replica_id) < 3)
            .collect();
        unique.len() >= QUORUM
    }
}

#[derive(Clone, Debug)]
struct StoredRecord {
    envelope: CommitEnvelope,
    certificate: CommitCertificate,
}

#[derive(Default)]
struct Replica {
    records: BTreeMap<u64, StoredRecord>,
}

struct CommitModel {
    mode: CommitContractMode,
    active_generation: u64,
    next_sequence: u64,
    next_log_index: u64,
    previous_log_chain: [u8; 32],
    replicas: [Replica; 3],
    outcomes: BTreeMap<([u8; 16], u64), ([u8; 32], Version)>,
}

impl CommitModel {
    fn new(mode: CommitContractMode) -> Self {
        Self {
            mode,
            active_generation: 3,
            next_sequence: 0,
            next_log_index: 0,
            previous_log_chain: [0; 32],
            replicas: std::array::from_fn(|_| Replica::default()),
            outcomes: BTreeMap::new(),
        }
    }

    fn submit(
        &mut self,
        request: &CommitRequest,
        accepted_resolvers: &[u16],
        routed_tags: &[u16],
        fsynced_replicas: &[u8],
        deliver_reply: bool,
    ) -> CommitOutcome {
        if let Some((fingerprint, version)) = self.outcomes.get(&request.key()) {
            return if fingerprint == &request.fingerprint()
                || self.mode == CommitContractMode::AcceptConflictingRetry
            {
                CommitOutcome::Committed(*version)
            } else {
                CommitOutcome::Rejected(RejectReason::ConflictingRetry)
            };
        }
        if request.generation != self.active_generation
            && self.mode != CommitContractMode::AcceptStaleGeneration
        {
            return CommitOutcome::Rejected(RejectReason::StaleGeneration);
        }
        if !contains_all(accepted_resolvers, &REQUIRED_RESOLVERS)
            && self.mode != CommitContractMode::AcceptPartialResolver
        {
            return CommitOutcome::Rejected(RejectReason::MissingResolver);
        }
        if !contains_all(routed_tags, &REQUIRED_LOG_TAGS)
            && self.mode != CommitContractMode::OmitRequiredLogTag
        {
            return CommitOutcome::Rejected(RejectReason::MissingLogTag);
        }

        self.next_sequence += 1;
        self.next_log_index += 1;
        let version = Version::from_parts(self.active_generation, self.next_sequence);
        let mut tags = routed_tags.to_vec();
        tags.sort_unstable();
        tags.dedup();
        let envelope = CommitEnvelope::new(
            request,
            version,
            self.next_log_index,
            tags,
            self.previous_log_chain,
        );
        self.previous_log_chain = digest(&envelope.encode());
        let certificate = CommitCertificate {
            acknowledgers: fsynced_replicas.to_vec(),
        };
        for replica_id in fsynced_replicas {
            if let Some(replica) = self.replicas.get_mut(usize::from(*replica_id)) {
                replica.records.insert(
                    self.next_log_index,
                    StoredRecord {
                        envelope: envelope.clone(),
                        certificate: certificate.clone(),
                    },
                );
            }
        }

        if certificate.is_quorum() {
            self.outcomes
                .insert(request.key(), (request.fingerprint(), version));
            if deliver_reply {
                CommitOutcome::Committed(version)
            } else {
                CommitOutcome::Unknown
            }
        } else if self.mode == CommitContractMode::AckBeforeQuorum {
            CommitOutcome::Committed(version)
        } else {
            CommitOutcome::Unavailable
        }
    }

    fn recover(&mut self) -> u64 {
        self.outcomes.clear();
        let mut recovered = BTreeMap::new();
        let mut maximum_sequence = 0;
        let mut maximum_index = 0;
        let mut recovered_log_chain = [0; 32];
        for replica in &self.replicas {
            for record in replica.records.values() {
                if !record.certificate.is_quorum() {
                    continue;
                }
                let encoded = record.envelope.encode();
                let Ok(envelope) = CommitEnvelope::decode(&encoded) else {
                    continue;
                };
                maximum_sequence = maximum_sequence.max(envelope.version.sequence());
                if envelope.log_index > maximum_index {
                    maximum_index = envelope.log_index;
                    recovered_log_chain = digest(&encoded);
                }
                recovered.insert(
                    (envelope.client_id, envelope.request_id),
                    (envelope.logical_fingerprint, envelope.version),
                );
            }
        }
        self.next_sequence = maximum_sequence;
        self.next_log_index = maximum_index;
        self.previous_log_chain = recovered_log_chain;
        if self.mode != CommitContractMode::RamOnlyDedup {
            self.outcomes = recovered;
        }
        u64::try_from(self.outcomes.len()).unwrap_or(u64::MAX)
    }
}

struct Scenario {
    seed: u64,
    mode: CommitContractMode,
    model: CommitModel,
    trace: Sha256,
    step: u64,
    first_mismatch: Option<String>,
    first_mismatch_step: Option<u64>,
    acknowledged_commits: u64,
    recovered_commits: u64,
    retry_count: u64,
    leader_only_attempts: u64,
}

impl Scenario {
    fn new(seed: u64, mode: CommitContractMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-commit-contract-v1");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Self {
            seed,
            mode,
            model: CommitModel::new(mode),
            trace,
            step: 0,
            first_mismatch: None,
            first_mismatch_step: None,
            acknowledged_commits: 0,
            recovered_commits: 0,
            retry_count: 0,
            leader_only_attempts: 0,
        }
    }

    fn run(&mut self) {
        self.codec_round_trip();
        self.happy_commit();
        self.lost_reply_retry();
        self.conflicting_retry();
        self.partial_resolver();
        self.missing_log_tag();
        self.stale_generation();
        self.leader_only_fsync();
    }

    fn codec_round_trip(&mut self) {
        let request = request(self.seed, 0, 3);
        let envelope = CommitEnvelope::new(
            &request,
            Version::from_parts(3, 1),
            1,
            REQUIRED_LOG_TAGS.to_vec(),
            [0; 32],
        );
        let encoded = envelope.encode();
        let decoded = CommitEnvelope::decode(&encoded);
        let mut corrupt = encoded;
        corrupt[12] ^= 0xff;
        let corruption_rejected = matches!(
            CommitEnvelope::decode(&corrupt),
            Err(CommitCodecError::ChecksumMismatch)
        );
        self.check(
            "codec_round_trip",
            decoded == Ok(envelope) && corruption_rejected,
            &format!("decoded={decoded:?}, corruption_rejected={corruption_rejected}"),
        );
    }

    fn happy_commit(&mut self) {
        let outcome = self.model.submit(
            &request(self.seed, 1, 3),
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[0, 1],
            true,
        );
        self.acknowledged_commits += u64::from(matches!(outcome, CommitOutcome::Committed(_)));
        self.check(
            "quorum_commit",
            outcome == CommitOutcome::Committed(Version::from_parts(3, 1)),
            &format!("outcome={outcome:?}"),
        );
    }

    fn lost_reply_retry(&mut self) {
        let request = request(self.seed, 2, 3);
        let first = self.model.submit(
            &request,
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[0, 2],
            false,
        );
        self.recovered_commits = self.model.recover();
        let retry = self.model.submit(
            &request,
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[1, 2],
            true,
        );
        self.retry_count += 1;
        self.acknowledged_commits += u64::from(matches!(retry, CommitOutcome::Committed(_)));
        self.check(
            "lost_reply_retry",
            first == CommitOutcome::Unknown
                && retry == CommitOutcome::Committed(Version::from_parts(3, 2)),
            &format!("first={first:?}, retry={retry:?}"),
        );
    }

    fn conflicting_retry(&mut self) {
        let mut conflicting = request(self.seed, 2, 3);
        conflicting.canonical_mutations = tagged_digest(self.seed, 2, b"conflicting").to_vec();
        let outcome = self.model.submit(
            &conflicting,
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[0, 1],
            true,
        );
        self.retry_count += 1;
        self.check(
            "conflicting_retry",
            outcome == CommitOutcome::Rejected(RejectReason::ConflictingRetry),
            &format!("outcome={outcome:?}"),
        );
    }

    fn partial_resolver(&mut self) {
        let outcome = self.model.submit(
            &request(self.seed, 3, 3),
            &[1],
            &REQUIRED_LOG_TAGS,
            &[0, 1],
            true,
        );
        self.check(
            "partial_resolver",
            outcome == CommitOutcome::Rejected(RejectReason::MissingResolver),
            &format!("outcome={outcome:?}"),
        );
    }

    fn missing_log_tag(&mut self) {
        let outcome = self.model.submit(
            &request(self.seed, 4, 3),
            &REQUIRED_RESOLVERS,
            &[10],
            &[0, 1],
            true,
        );
        self.check(
            "missing_log_tag",
            outcome == CommitOutcome::Rejected(RejectReason::MissingLogTag),
            &format!("outcome={outcome:?}"),
        );
    }

    fn stale_generation(&mut self) {
        let outcome = self.model.submit(
            &request(self.seed, 5, 2),
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[0, 1],
            true,
        );
        self.check(
            "stale_generation",
            outcome == CommitOutcome::Rejected(RejectReason::StaleGeneration),
            &format!("outcome={outcome:?}"),
        );
    }

    fn leader_only_fsync(&mut self) {
        let request = request(self.seed, 6, 3);
        let outcome = self.model.submit(
            &request,
            &REQUIRED_RESOLVERS,
            &REQUIRED_LOG_TAGS,
            &[0],
            true,
        );
        self.leader_only_attempts += 1;
        let acknowledged = matches!(outcome, CommitOutcome::Committed(_));
        self.acknowledged_commits += u64::from(acknowledged);
        self.recovered_commits = self.model.recover();
        let survived = self.model.outcomes.contains_key(&request.key());
        self.check(
            "leader_only_fsync",
            outcome == CommitOutcome::Unavailable && !survived,
            &format!("outcome={outcome:?}, survived={survived}"),
        );
    }

    fn check(&mut self, action: &str, passed: bool, detail: &str) {
        self.step += 1;
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(action.as_bytes());
        self.trace.update([u8::from(passed)]);
        self.trace.update(detail.as_bytes());
        if !passed && self.first_mismatch.is_none() {
            self.first_mismatch_step = Some(self.step);
            self.first_mismatch = Some(format!("{action}: {detail}"));
        }
    }

    fn report(&self) -> CommitContractReport {
        CommitContractReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: u64::from(self.first_mismatch.is_some()),
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch.clone(),
            acknowledged_commits: self.acknowledged_commits,
            recovered_commits: self.recovered_commits,
            retry_count: self.retry_count,
            leader_only_attempts: self.leader_only_attempts,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

/// Run the deterministic Cell v0 commit envelope and recovery contract model.
#[must_use]
pub fn run_commit_contract(seed: u64, mode: CommitContractMode) -> CommitContractReport {
    let mut scenario = Scenario::new(seed, mode);
    scenario.run();
    scenario.report()
}

fn request(seed: u64, ordinal: u64, generation: u64) -> CommitRequest {
    let mut client_id = [0; 16];
    client_id.copy_from_slice(&tagged_digest(seed, ordinal, b"client")[..16]);
    CommitRequest {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation,
        client_id,
        request_id: ordinal,
        read_conflicts: tagged_digest(seed, ordinal, b"read-conflict").to_vec(),
        write_conflicts: tagged_digest(seed, ordinal, b"write-conflict").to_vec(),
        canonical_mutations: tagged_digest(seed, ordinal, b"mutation").to_vec(),
    }
}

fn tagged_digest(seed: u64, ordinal: u64, label: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-commit-fixture-v1");
    hasher.update(seed.to_be_bytes());
    hasher.update(ordinal.to_be_bytes());
    hasher.update(label);
    hasher.finalize().into()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hash_byte_vec(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn contains_all(actual: &[u16], required: &[u16]) -> bool {
    let actual: BTreeSet<u16> = actual.iter().copied().collect();
    required.iter().all(|item| actual.contains(item))
}

fn strictly_sorted(values: &[u16]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

fn push_u16_vec(bytes: &mut Vec<u8>, values: &[u16]) {
    let count = u16::try_from(values.len()).expect("model vectors fit u16");
    bytes.extend_from_slice(&count.to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
}

fn push_byte_vec(bytes: &mut Vec<u8>, value: &[u8]) {
    let count = u32::try_from(value.len()).expect("model payloads fit u32");
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(value);
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CommitCodecError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(CommitCodecError::Truncated)?;
        let source = self
            .bytes
            .get(self.offset..end)
            .ok_or(CommitCodecError::Truncated)?;
        let mut value = [0; N];
        value.copy_from_slice(source);
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, CommitCodecError> {
        Ok(u16::from_be_bytes(self.take()?))
    }

    fn u64(&mut self) -> Result<u64, CommitCodecError> {
        Ok(u64::from_be_bytes(self.take()?))
    }

    fn u32(&mut self) -> Result<u32, CommitCodecError> {
        Ok(u32::from_be_bytes(self.take()?))
    }

    fn byte_vec(&mut self) -> Result<Vec<u8>, CommitCodecError> {
        let count = usize::try_from(self.u32()?).map_err(|_| CommitCodecError::Truncated)?;
        let end = self
            .offset
            .checked_add(count)
            .ok_or(CommitCodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CommitCodecError::Truncated)?
            .to_vec();
        self.offset = end;
        Ok(value)
    }

    fn u16_vec(&mut self) -> Result<Vec<u16>, CommitCodecError> {
        let count = usize::from(self.u16()?);
        (0..count).map(|_| self.u16()).collect()
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_rejects_corruption() {
        let envelope = CommitEnvelope::new(
            &request(1103, 1, 3),
            Version::from_parts(3, 9),
            7,
            REQUIRED_LOG_TAGS.to_vec(),
            [0x44; 32],
        );
        let mut encoded = envelope.encode();
        assert_eq!(CommitEnvelope::decode(&encoded), Ok(envelope.clone()));
        encoded[20] ^= 0x01;
        assert_eq!(
            CommitEnvelope::decode(&encoded),
            Err(CommitCodecError::ChecksumMismatch)
        );

        let mut inconsistent = envelope;
        inconsistent.canonical_mutations.push(0xff);
        inconsistent.envelope_len =
            u32::try_from(inconsistent.encode_without_checksum().len() + 32).unwrap();
        inconsistent.checksum = digest(&inconsistent.encode_without_checksum());
        assert_eq!(
            CommitEnvelope::decode(&inconsistent.encode()),
            Err(CommitCodecError::FingerprintMismatch)
        );
    }

    #[test]
    fn correct_contract_is_exactly_replayable() {
        let first = run_commit_contract(1103, CommitContractMode::Correct);
        let second = run_commit_contract(1103, CommitContractMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert_eq!(first.executed_steps, 8);
        assert!(first.acknowledged_commits > 0);
        assert!(first.recovered_commits > 0);
        assert!(first.retry_count > 0);
        assert_eq!(first.leader_only_attempts, 1);
    }

    #[test]
    fn every_commit_negative_control_has_a_bounded_failure() {
        let controls = [
            (CommitContractMode::RamOnlyDedup, 3),
            (CommitContractMode::AcceptConflictingRetry, 4),
            (CommitContractMode::AcceptPartialResolver, 5),
            (CommitContractMode::OmitRequiredLogTag, 6),
            (CommitContractMode::AcceptStaleGeneration, 7),
            (CommitContractMode::AckBeforeQuorum, 8),
        ];
        for (mode, expected_step) in controls {
            let report = run_commit_contract(1103, mode);
            assert_eq!(report.anomaly_count, 1, "{}", mode.id());
            assert_eq!(
                report.first_mismatch_step,
                Some(expected_step),
                "{}: {:?}",
                mode.id(),
                report.first_mismatch
            );
        }
    }

    #[test]
    fn seed_changes_commit_trace() {
        let first = run_commit_contract(1103, CommitContractMode::Correct);
        let second = run_commit_contract(1104, CommitContractMode::Correct);
        assert_ne!(first.trace_sha256, second.trace_sha256);
    }
}
