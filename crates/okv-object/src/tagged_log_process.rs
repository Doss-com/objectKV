use okv_consensus::{
    cell_log_set_policy_sha256, cell_log_set_policy_transition_sha256,
    cell_tagged_log_policy_stage_certificate_sha256, cell_tagged_log_repair_certificate_sha256,
    sign_tagged_log_capacity_statement, sign_tagged_log_fence_statement,
    sign_tagged_log_policy_stage_statement, sign_tagged_log_pop_statement,
    sign_tagged_log_prefix_fence_statement, sign_tagged_log_repair_statement,
    sign_tagged_log_statement, tagged_log_public_key,
    verify_cell_log_set_policy_activation_certificate, verify_publication_pop_capability,
    verify_tagged_log_policy_stage_certificate, verify_tagged_log_repair_certificate,
    CellLogSetPolicy, CellLogSetPolicyActivationCertificate, CellLogSetPolicyTransition,
    CellStateSnapshot, CellTaggedLogAttestation, CellTaggedLogCapacityAttestation,
    CellTaggedLogCapacityStatement, CellTaggedLogFenceAttestation, CellTaggedLogFenceStatement,
    CellTaggedLogPolicyStageAttestation, CellTaggedLogPolicyStageCertificate,
    CellTaggedLogPolicyStageStatement, CellTaggedLogPopAttestation, CellTaggedLogPopStatement,
    CellTaggedLogPrefixFenceAttestation, CellTaggedLogPrefixFenceStatement,
    CellTaggedLogPrefixObservation, CellTaggedLogRepairAttestation, CellTaggedLogRepairCertificate,
    CellTaggedLogRepairPhase, CellTaggedLogRepairStatement, CellTaggedLogStatement,
    PublicationPopCapabilityCertificate, RequestIdentity,
};
use okv_sim::CommitEnvelope;
use okv_wal::{LocalReplicatedWal, FRAME_HEADER_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const TAGGED_LOG_FORMAT_VERSION: u32 = 1;
const FRAME_CHECKSUM_BYTES: u64 = 32;

/// One exact committed envelope retained by a dedicated tagged-log process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaggedLogRecord {
    pub format_version: u32,
    pub position: u64,
    pub range_tags: Vec<u16>,
    #[serde(with = "compact_bytes")]
    pub envelope: Vec<u8>,
    #[serde(default, with = "compact_bytes")]
    pub padding: Vec<u8>,
}

mod compact_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Encoded {
        Base64(String),
        Legacy(Vec<u8>),
    }

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Encoded::deserialize(deserializer)? {
            Encoded::Base64(value) => STANDARD.decode(value).map_err(serde::de::Error::custom),
            Encoded::Legacy(value) => Ok(value),
        }
    }
}

impl TaggedLogRecord {
    /// Construct a normal retained record with no evaluation padding.
    #[must_use]
    pub fn committed(position: u64, range_tags: Vec<u16>, envelope: Vec<u8>) -> Self {
        Self {
            format_version: TAGGED_LOG_FORMAT_VERSION,
            position,
            range_tags,
            envelope,
            padding: Vec::new(),
        }
    }
}

/// Configuration for one independent tagged-log process.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaggedLogProcessConfig {
    pub log_set_id: u16,
    pub node_id: u64,
    pub listen_addr: String,
    pub root: PathBuf,
    pub retained_bytes_limit: u64,
    pub accept_missing_required_tags: bool,
    #[serde(default)]
    pub policy_epoch: u64,
    #[serde(default)]
    pub signing_seed: Option<Vec<u8>>,
    #[serde(default)]
    pub publication_pop_policy: Option<PublicationPopPolicy>,
    #[serde(default)]
    pub accept_unauthenticated_pop: bool,
    #[serde(default = "default_true")]
    pub persist_fences: bool,
    #[serde(default)]
    pub storage_incarnation: [u8; 16],
    #[serde(default)]
    pub learner: bool,
    #[serde(default)]
    pub repair_source_policy: Option<CellLogSetPolicy>,
    #[serde(default)]
    pub repair_faults: TaggedLogRepairFaults,
    #[serde(default)]
    pub policy_activation_authority: Option<PublicationPopPolicy>,
    #[serde(default)]
    pub policy_transition_faults: TaggedLogPolicyTransitionFaults,
}

/// Eval-only unsafe learner behaviors used to prove the repair oracle.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaggedLogRepairFaults {
    pub accept_invalid_certificate: bool,
    pub accept_target_mismatch: bool,
    pub accept_snapshot_identity_mismatch: bool,
    pub accept_local_snapshot_mismatch: bool,
}

/// Immutable identity for one resumable learner transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaggedLogRepairTransfer {
    pub format_version: u16,
    pub transfer_id: u64,
    pub certificate: CellTaggedLogRepairCertificate,
    pub base_snapshot_sha256: Option<[u8; 32]>,
    pub payload_sha256: [u8; 32],
    pub payload_length: u64,
    pub chunk_count: u16,
}

/// Eval-only unsafe policy-transition behaviors used by negative controls.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaggedLogPolicyTransitionFaults {
    pub accept_invalid_stage: bool,
    pub accept_missing_authority_activation: bool,
    pub accept_removed_member_activation: bool,
}

/// Pinned publication-authority signer set required before local log deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationPopPolicy {
    pub members: BTreeMap<u64, Vec<u8>>,
    pub quorum_size: u16,
}

const fn default_true() -> bool {
    true
}

/// Request accepted by one tagged-log process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaggedLogRequest {
    Append {
        record: TaggedLogRecord,
    },
    Read {
        range_tag: u16,
        after_version: u64,
        through_version: u64,
    },
    Attest {
        statement: CellTaggedLogStatement,
    },
    Fence {
        statement: CellTaggedLogFenceStatement,
    },
    PrefixFence {
        statement: CellTaggedLogPrefixFenceStatement,
    },
    Capacity {
        statement: CellTaggedLogCapacityStatement,
    },
    Pop {
        statement: CellTaggedLogPopStatement,
        capability: PublicationPopCapabilityCertificate,
        #[serde(with = "compact_bytes")]
        manifest_bytes: Vec<u8>,
    },
    RepairAttest {
        statement: CellTaggedLogRepairStatement,
        #[serde(with = "compact_bytes")]
        snapshot_bytes: Vec<u8>,
    },
    RepairInstall {
        certificate: CellTaggedLogRepairCertificate,
        #[serde(with = "compact_bytes")]
        snapshot_bytes: Vec<u8>,
    },
    RepairReady {
        certificate: CellTaggedLogRepairCertificate,
        #[serde(with = "compact_bytes")]
        snapshot_bytes: Vec<u8>,
    },
    RepairChunk {
        transfer: Box<TaggedLogRepairTransfer>,
        chunk_index: u16,
        #[serde(with = "compact_bytes")]
        chunk_bytes: Vec<u8>,
    },
    RepairFinalize {
        transfer: Box<TaggedLogRepairTransfer>,
    },
    PolicyStage {
        transition: CellLogSetPolicyTransition,
    },
    PolicyActivate {
        transition: Box<CellLogSetPolicyTransition>,
        successor_stage: Box<CellTaggedLogPolicyStageCertificate>,
        activation: CellLogSetPolicyActivationCertificate,
    },
    Status,
}

/// Exact response from one tagged-log process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaggedLogResponse {
    Appended {
        log_set_id: u16,
        node_id: u64,
        position: u64,
        record_sha256: [u8; 32],
        frame_bytes: u64,
        retained_bytes: u64,
    },
    Feed {
        log_set_id: u16,
        node_id: u64,
        records: Vec<TaggedLogRecord>,
        retained_bytes: u64,
    },
    Ready {
        log_set_id: u16,
        node_id: u64,
        last_position: u64,
        popped_through: u64,
        retained_bytes: u64,
        sample_epoch: u64,
    },
    Attested {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogStatement,
        attestation: CellTaggedLogAttestation,
    },
    Fenced {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogFenceStatement,
        attestation: CellTaggedLogFenceAttestation,
        durable: bool,
    },
    PrefixFenced {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogPrefixFenceStatement,
        attestation: CellTaggedLogPrefixFenceAttestation,
        durable: bool,
    },
    CapacityAttested {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogCapacityStatement,
        attestation: CellTaggedLogCapacityAttestation,
    },
    Popped {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogPopStatement,
        attestation: CellTaggedLogPopAttestation,
        durable: bool,
    },
    RepairAttested {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogRepairStatement,
        attestation: CellTaggedLogRepairAttestation,
    },
    RepairInstalled {
        log_set_id: u16,
        node_id: u64,
        repair_id: u64,
        last_position: u64,
        popped_through: u64,
        installed_records: u64,
        snapshot_bytes: u64,
        durable: bool,
    },
    RepairReady {
        log_set_id: u16,
        node_id: u64,
        repair_id: u64,
        last_position: u64,
        retained_root_sha256: [u8; 32],
        durable: bool,
    },
    RepairChunkStored {
        log_set_id: u16,
        node_id: u64,
        transfer_id: u64,
        chunk_index: u16,
        chunk_bytes: u64,
        durable: bool,
    },
    PolicyStaged {
        log_set_id: u16,
        node_id: u64,
        transition_id: u64,
        attestation: CellTaggedLogPolicyStageAttestation,
        durable: bool,
    },
    PolicyActivated {
        log_set_id: u16,
        node_id: u64,
        transition_id: u64,
        policy_epoch: u64,
        durable: bool,
    },
    RetainedBytesLimit {
        log_set_id: u16,
        node_id: u64,
        retained_bytes: u64,
        proposed_frame_bytes: u64,
        limit: u64,
    },
    Rejected {
        detail: String,
    },
}

/// Run one blocking dedicated tagged-log process.
///
/// # Errors
///
/// Returns an error when the listener or private stable storage cannot open.
pub fn run_tagged_log_process(config: &TaggedLogProcessConfig) -> Result<(), String> {
    if config.log_set_id == 0 || config.node_id == 0 {
        return Err("tagged-log process requires nonzero log-set and node identities".to_owned());
    }
    LocalReplicatedWal::open(&config.root, 1, 1).map_err(|error| error.to_string())?;
    let listener = TcpListener::bind(&config.listen_addr).map_err(|error| error.to_string())?;
    for incoming in listener.incoming() {
        let mut stream = incoming.map_err(|error| error.to_string())?;
        let response = match read_request(&stream) {
            Ok(request) => handle_request(config, request),
            Err(error) => TaggedLogResponse::Rejected { detail: error },
        };
        let mut bytes = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
        bytes.push(b'\n');
        stream
            .write_all(&bytes)
            .map_err(|error| error.to_string())?;
        stream.flush().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn read_request(stream: &TcpStream) -> Result<TaggedLogRequest, String> {
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    serde_json::from_str(&line).map_err(|error| error.to_string())
}

fn handle_request(config: &TaggedLogProcessConfig, request: TaggedLogRequest) -> TaggedLogResponse {
    match handle_request_inner(config, request) {
        Ok(response) => response,
        Err(detail) => TaggedLogResponse::Rejected { detail },
    }
}

#[allow(clippy::too_many_lines)]
fn handle_request_inner(
    config: &TaggedLogProcessConfig,
    request: TaggedLogRequest,
) -> Result<TaggedLogResponse, String> {
    let wal = LocalReplicatedWal::open(&config.root, 1, 1).map_err(|error| error.to_string())?;
    let recovery = wal.recover().map_err(|error| error.to_string())?;
    let mut retention = load_or_recover_retention(&config.root, &recovery.records)?;
    let retained_bytes = retained_frame_bytes(&recovery.records, retention.popped_through)?;
    let policy_activation = load_policy_activation(&config.root)?;
    let effective_policy_epoch = policy_activation
        .as_ref()
        .map_or(config.policy_epoch, |receipt| {
            receipt.transition.next_policy.policy_epoch
        });
    let effective_learner = config.learner && policy_activation.is_none();
    match request {
        TaggedLogRequest::Status => Ok(TaggedLogResponse::Ready {
            log_set_id: config.log_set_id,
            node_id: config.node_id,
            last_position: retention.last_position,
            popped_through: retention.popped_through,
            retained_bytes,
            sample_epoch: retention.sample_epoch,
        }),
        TaggedLogRequest::Append { record } => {
            if effective_learner {
                return Err("repair learner is excluded from active appends".to_owned());
            }
            let envelope =
                CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
            if load_fence(&config.root, envelope.generation())?.is_some()
                || load_prefix_fence(&config.root, envelope.generation())?.is_some()
            {
                return Err(format!(
                    "tagged-log generation {} is durably fenced",
                    envelope.generation()
                ));
            }
            let payload = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            let proposed_frame_bytes = u64::try_from(payload.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(FRAME_HEADER_BYTES).unwrap_or(u64::MAX))
                .saturating_add(FRAME_CHECKSUM_BYTES);
            if retained_bytes.saturating_add(proposed_frame_bytes) > config.retained_bytes_limit {
                return Ok(TaggedLogResponse::RetainedBytesLimit {
                    log_set_id: config.log_set_id,
                    node_id: config.node_id,
                    retained_bytes,
                    proposed_frame_bytes,
                    limit: config.retained_bytes_limit,
                });
            }
            validate_record(&record, config.accept_missing_required_tags)?;
            if !config.accept_missing_required_tags
                && !record.range_tags.contains(&config.log_set_id)
            {
                return Err("tagged-log record omits its configured log-set tag".to_owned());
            }
            let expected_position = retention.last_position.saturating_add(1);
            if record.position != expected_position {
                return Err(format!(
                    "non-consecutive tagged-log position: expected {expected_position}, received {}",
                    record.position
                ));
            }
            let outcome = wal
                .append(recovery.last_index().saturating_add(1), &payload, &[0])
                .map_err(|error| error.to_string())?;
            if !outcome.quorum_durable {
                return Err("single-node tagged-log append was not locally durable".to_owned());
            }
            retention.last_position = record.position;
            retention.sample_epoch = retention.sample_epoch.saturating_add(1);
            persist_retention(&config.root, &retention)?;
            Ok(TaggedLogResponse::Appended {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                position: record.position,
                record_sha256: Sha256::digest(&payload).into(),
                frame_bytes: outcome.frame_bytes,
                retained_bytes: retained_bytes.saturating_add(outcome.frame_bytes),
            })
        }
        TaggedLogRequest::Read {
            range_tag,
            after_version,
            through_version,
        } => {
            if after_version >= through_version {
                return Err("tagged-log read has invalid version bounds".to_owned());
            }
            let mut records = Vec::new();
            for recovered in recovery.records {
                let record: TaggedLogRecord = serde_json::from_slice(&recovered.payload)
                    .map_err(|error| error.to_string())?;
                validate_record(&record, config.accept_missing_required_tags)?;
                let envelope =
                    CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
                let version = envelope.version().sequence();
                if version > retention.popped_through
                    && record.range_tags.contains(&range_tag)
                    && version > after_version
                    && version <= through_version
                {
                    records.push(record);
                }
            }
            Ok(TaggedLogResponse::Feed {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                records,
                retained_bytes,
            })
        }
        TaggedLogRequest::Attest { statement } => {
            if effective_learner {
                return Err("repair learner is excluded from durability attestations".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            if effective_policy_epoch == 0
                || statement.policy_epoch != effective_policy_epoch
                || statement.log_set_id != config.log_set_id
                || statement.durable_position == 0
            {
                return Err("tagged-log statement does not match active signer policy".to_owned());
            }
            let recovered = recovery
                .records
                .iter()
                .find(|recovered| {
                    serde_json::from_slice::<TaggedLogRecord>(&recovered.payload)
                        .is_ok_and(|record| record.position == statement.durable_position)
                })
                .ok_or_else(|| {
                    "tagged-log statement names an absent durable position".to_owned()
                })?;
            let record: TaggedLogRecord =
                serde_json::from_slice(&recovered.payload).map_err(|error| error.to_string())?;
            validate_record(&record, config.accept_missing_required_tags)?;
            let envelope =
                CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
            let (client_id, request_id) = envelope.client_identity();
            if client_id[..8] != [0; 8] {
                return Err("tagged-log statement cannot map envelope client identity".to_owned());
            }
            let mut client_bytes = [0_u8; 8];
            client_bytes.copy_from_slice(&client_id[8..]);
            let transaction_identity = RequestIdentity {
                client_id: u64::from_be_bytes(client_bytes),
                request_id,
            };
            let envelope_sha256: [u8; 32] = Sha256::digest(&record.envelope).into();
            if statement.format_version != 1
                || statement.cell_id != envelope.cell_id()
                || statement.tenant_id != envelope.tenant_id()
                || statement.generation != envelope.generation()
                || statement.transaction_identity != transaction_identity
                || statement.commit_sequence != envelope.version().sequence()
                || statement.envelope_sha256 != envelope_sha256
                || record.position != statement.durable_position
                || !record.range_tags.contains(&config.log_set_id)
            {
                return Err("tagged-log statement differs from the durable record".to_owned());
            }
            let attestation = sign_tagged_log_statement(config.node_id, signing_seed, &statement)?;
            Ok(TaggedLogResponse::Attested {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
            })
        }
        TaggedLogRequest::Fence { statement } => {
            if effective_learner {
                return Err("repair learner is excluded from generation fences".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            if effective_policy_epoch == 0
                || statement.format_version != 1
                || statement.policy_epoch != effective_policy_epoch
                || statement.log_set_id != config.log_set_id
                || statement.generation == 0
                || statement.recovery_id == 0
                || statement.commit_sequence == 0
            {
                return Err("tagged-log fence does not match active signer policy".to_owned());
            }
            if let Some(existing) = load_fence(&config.root, statement.generation)? {
                if existing.statement != statement {
                    return Err(
                        "tagged-log generation is already fenced by another recovery".to_owned(),
                    );
                }
            }
            let mut record_present = false;
            for recovered in &recovery.records {
                let record: TaggedLogRecord = serde_json::from_slice(&recovered.payload)
                    .map_err(|error| error.to_string())?;
                let envelope =
                    CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
                let (client_id, request_id) = envelope.client_identity();
                if client_id[..8] != [0; 8] {
                    continue;
                }
                let mut client_bytes = [0_u8; 8];
                client_bytes.copy_from_slice(&client_id[8..]);
                let identity = RequestIdentity {
                    client_id: u64::from_be_bytes(client_bytes),
                    request_id,
                };
                let digest: [u8; 32] = Sha256::digest(&record.envelope).into();
                if envelope.cell_id() == statement.cell_id
                    && envelope.tenant_id() == statement.tenant_id
                    && envelope.generation() == statement.generation
                    && envelope.version().sequence() == statement.commit_sequence
                    && identity == statement.transaction_identity
                    && digest == statement.envelope_sha256
                    && record.range_tags.contains(&config.log_set_id)
                {
                    record_present = true;
                    break;
                }
            }
            if config.persist_fences {
                persist_fence(&config.root, &statement)?;
            }
            let attestation = sign_tagged_log_fence_statement(
                config.node_id,
                signing_seed,
                &statement,
                record_present,
            )?;
            Ok(TaggedLogResponse::Fenced {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
                durable: config.persist_fences,
            })
        }
        TaggedLogRequest::PrefixFence { statement } => {
            if effective_learner {
                return Err("repair learner is excluded from prefix fences".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            if effective_policy_epoch == 0
                || statement.format_version != 1
                || statement.policy_epoch != effective_policy_epoch
                || statement.log_set_id != config.log_set_id
                || statement.generation == 0
                || statement.recovery_id == 0
                || statement.window.format_version != 1
                || statement.window.records.is_empty()
            {
                return Err(
                    "tagged-log prefix fence does not match active signer policy".to_owned(),
                );
            }
            if let Some(existing) = load_prefix_fence(&config.root, statement.generation)? {
                if existing.statement != statement {
                    return Err(
                        "tagged-log generation is already prefix-fenced by another recovery"
                            .to_owned(),
                    );
                }
            }
            let mut observations = Vec::with_capacity(statement.window.records.len());
            for expected in &statement.window.records {
                let mut record_present = false;
                for recovered in &recovery.records {
                    let record: TaggedLogRecord = serde_json::from_slice(&recovered.payload)
                        .map_err(|error| error.to_string())?;
                    let envelope = CommitEnvelope::decode(&record.envelope)
                        .map_err(|error| error.to_string())?;
                    let (client_id, request_id) = envelope.client_identity();
                    if client_id[..8] != [0; 8] {
                        continue;
                    }
                    let mut client_bytes = [0_u8; 8];
                    client_bytes.copy_from_slice(&client_id[8..]);
                    let identity = RequestIdentity {
                        client_id: u64::from_be_bytes(client_bytes),
                        request_id,
                    };
                    let digest: [u8; 32] = Sha256::digest(&record.envelope).into();
                    if envelope.cell_id() == statement.cell_id
                        && envelope.tenant_id() == statement.tenant_id
                        && envelope.generation() == statement.generation
                        && envelope.version().sequence() == expected.commit_sequence
                        && identity == expected.transaction_identity
                        && digest == expected.envelope_sha256
                        && record.range_tags.contains(&config.log_set_id)
                    {
                        record_present = true;
                        break;
                    }
                }
                observations.push(CellTaggedLogPrefixObservation {
                    transaction_identity: expected.transaction_identity,
                    commit_sequence: expected.commit_sequence,
                    envelope_sha256: expected.envelope_sha256,
                    record_present,
                });
            }
            if config.persist_fences {
                persist_prefix_fence(&config.root, &statement)?;
            }
            let attestation = sign_tagged_log_prefix_fence_statement(
                config.node_id,
                signing_seed,
                &statement,
                observations,
            )?;
            Ok(TaggedLogResponse::PrefixFenced {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
                durable: config.persist_fences,
            })
        }
        TaggedLogRequest::Capacity { statement } => {
            if effective_learner {
                return Err("repair learner is excluded from capacity attestations".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            validate_capacity_statement(
                config,
                effective_policy_epoch,
                &statement,
                &recovery.records,
                &retention,
            )?;
            let attestation = sign_tagged_log_capacity_statement(
                signing_seed,
                &statement,
                CellTaggedLogCapacityAttestation {
                    signer_id: config.node_id,
                    last_position: retention.last_position,
                    popped_through: retention.popped_through,
                    retained_bytes,
                    hard_limit_bytes: config.retained_bytes_limit,
                    sample_epoch: retention.sample_epoch,
                    signature: Vec::new(),
                },
            )?;
            Ok(TaggedLogResponse::CapacityAttested {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
            })
        }
        TaggedLogRequest::Pop {
            statement,
            capability,
            manifest_bytes,
        } => {
            if effective_learner {
                return Err("repair learner is excluded from pop attestations".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            validate_pop_statement(
                config,
                effective_policy_epoch,
                &statement,
                &capability,
                &manifest_bytes,
                &recovery.records,
                &retention,
            )?;
            if statement.object_frontier > retention.popped_through {
                retention.popped_through = statement.object_frontier;
                retention.sample_epoch = retention.sample_epoch.saturating_add(1);
                persist_retention(&config.root, &retention)?;
                let mut rewritten = Vec::new();
                for recovered in &recovery.records {
                    let record: TaggedLogRecord = serde_json::from_slice(&recovered.payload)
                        .map_err(|error| error.to_string())?;
                    let envelope = CommitEnvelope::decode(&record.envelope)
                        .map_err(|error| error.to_string())?;
                    if envelope.version().sequence() > statement.object_frontier {
                        rewritten.push((
                            u64::try_from(rewritten.len())
                                .unwrap_or(u64::MAX)
                                .saturating_add(1),
                            recovered.payload.clone(),
                        ));
                    }
                }
                wal.rewrite(&rewritten).map_err(|error| error.to_string())?;
            }
            let compacted = wal.recover().map_err(|error| error.to_string())?;
            let compacted_bytes =
                retained_frame_bytes(&compacted.records, retention.popped_through)?;
            let attestation = sign_tagged_log_pop_statement(
                signing_seed,
                &statement,
                CellTaggedLogPopAttestation {
                    signer_id: config.node_id,
                    last_position: retention.last_position,
                    popped_through: retention.popped_through,
                    retained_bytes: compacted_bytes,
                    sample_epoch: retention.sample_epoch,
                    signature: Vec::new(),
                },
            )?;
            Ok(TaggedLogResponse::Popped {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
                durable: true,
            })
        }
        TaggedLogRequest::RepairAttest {
            statement,
            snapshot_bytes,
        } => {
            if effective_learner {
                return Err("a repair learner cannot attest as an active source".to_owned());
            }
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            validate_repair_snapshot(
                config,
                &statement,
                &snapshot_bytes,
                &recovery.records,
                &retention,
                true,
            )?;
            let attestation = sign_tagged_log_repair_statement(
                signing_seed,
                &statement,
                CellTaggedLogRepairAttestation {
                    signer_id: config.node_id,
                    source_sample_epoch: retention.sample_epoch,
                    signature: Vec::new(),
                },
            )?;
            Ok(TaggedLogResponse::RepairAttested {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                statement,
                attestation,
            })
        }
        TaggedLogRequest::RepairChunk {
            transfer,
            chunk_index,
            chunk_bytes,
        } => store_repair_chunk(config, &transfer, chunk_index, &chunk_bytes),
        TaggedLogRequest::RepairFinalize { transfer } => {
            finalize_repair_transfer(config, &wal, &recovery.records, &mut retention, &transfer)
        }
        TaggedLogRequest::RepairInstall {
            certificate,
            snapshot_bytes,
        } => {
            validate_repair_target(config, &certificate)?;
            if certificate.statement.phase != CellTaggedLogRepairPhase::BaseSnapshot {
                return Err("repair install requires a base-snapshot certificate".to_owned());
            }
            let install_path = repair_install_path(&config.root);
            let existing = load_repair_receipt(&install_path)?;
            if let Some(receipt) = existing {
                if receipt.certificate == certificate {
                    return Ok(TaggedLogResponse::RepairInstalled {
                        log_set_id: config.log_set_id,
                        node_id: config.node_id,
                        repair_id: certificate.statement.repair_id,
                        last_position: certificate.statement.last_position,
                        popped_through: certificate.statement.popped_through,
                        installed_records: receipt.installed_records,
                        snapshot_bytes: certificate.statement.snapshot_length,
                        durable: true,
                    });
                }
                return Err("learner root already contains a conflicting repair".to_owned());
            }
            if !recovery.records.is_empty()
                || retention.last_position != 0
                || retention.popped_through != 0
            {
                return Err("repair learner must start from an empty stable root".to_owned());
            }
            let records = validate_repair_snapshot(
                config,
                &certificate.statement,
                &snapshot_bytes,
                &[],
                &retention,
                false,
            )?;
            let rewritten = records
                .iter()
                .enumerate()
                .map(|(offset, record)| {
                    serde_json::to_vec(record)
                        .map(|payload| {
                            (
                                u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1),
                                payload,
                            )
                        })
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            wal.rewrite(&rewritten).map_err(|error| error.to_string())?;
            retention.last_position = certificate.statement.last_position;
            retention.popped_through = certificate.statement.popped_through;
            retention.sample_epoch = 1;
            persist_retention(&config.root, &retention)?;
            let receipt = DurableTaggedLogRepairReceipt {
                certificate: certificate.clone(),
                installed_records: u64::try_from(records.len()).unwrap_or(u64::MAX),
            };
            persist_repair_receipt(&config.root, &install_path, &receipt)?;
            Ok(TaggedLogResponse::RepairInstalled {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                repair_id: certificate.statement.repair_id,
                last_position: certificate.statement.last_position,
                popped_through: certificate.statement.popped_through,
                installed_records: receipt.installed_records,
                snapshot_bytes: certificate.statement.snapshot_length,
                durable: true,
            })
        }
        TaggedLogRequest::RepairReady {
            certificate,
            snapshot_bytes,
        } => {
            validate_repair_target(config, &certificate)?;
            if certificate.statement.phase != CellTaggedLogRepairPhase::LearnerReady {
                return Err("repair readiness requires a learner-ready certificate".to_owned());
            }
            let installed = load_repair_receipt(&repair_install_path(&config.root))?
                .ok_or_else(|| "repair learner has no durable base installation".to_owned())?;
            if installed.certificate.statement.repair_id != certificate.statement.repair_id
                || installed.certificate.statement.learner_node_id
                    != certificate.statement.learner_node_id
                || installed.certificate.statement.learner_incarnation
                    != certificate.statement.learner_incarnation
            {
                return Err("learner-ready certificate does not match installed repair".to_owned());
            }
            validate_repair_snapshot(
                config,
                &certificate.statement,
                &snapshot_bytes,
                &recovery.records,
                &retention,
                true,
            )?;
            let ready_path = repair_ready_path(&config.root);
            if let Some(existing) = load_repair_receipt(&ready_path)? {
                if existing.certificate != certificate {
                    return Err("learner root already contains conflicting readiness".to_owned());
                }
            } else {
                persist_repair_receipt(
                    &config.root,
                    &ready_path,
                    &DurableTaggedLogRepairReceipt {
                        certificate: certificate.clone(),
                        installed_records: u64::try_from(recovery.records.len())
                            .unwrap_or(u64::MAX),
                    },
                )?;
            }
            Ok(TaggedLogResponse::RepairReady {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                repair_id: certificate.statement.repair_id,
                last_position: certificate.statement.last_position,
                retained_root_sha256: certificate.statement.snapshot_sha256,
                durable: true,
            })
        }
        TaggedLogRequest::PolicyStage { transition } => {
            let signing_seed = config
                .signing_seed
                .as_deref()
                .ok_or_else(|| "tagged-log process has no signing key".to_owned())?;
            let local_public_key = tagged_log_public_key(signing_seed)?;
            let next_member = transition
                .next_policy
                .members
                .iter()
                .find(|member| member.node_id == config.node_id);
            let local_records = recovery
                .records
                .iter()
                .map(|recovered| {
                    serde_json::from_slice::<TaggedLogRecord>(&recovered.payload)
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let local_snapshot = encode_tagged_log_repair_snapshot(&local_records)?;
            let local_root: [u8; 32] = Sha256::digest(&local_snapshot).into();
            let readiness_matches = if config.learner {
                load_repair_receipt(&repair_ready_path(&config.root))?.is_some_and(|receipt| {
                    cell_tagged_log_repair_certificate_sha256(&receipt.certificate)
                        == transition.repair_readiness_sha256
                        && receipt.certificate.statement.learner_incarnation
                            == transition.learner_incarnation
                })
            } else {
                transition
                    .old_policy
                    .members
                    .iter()
                    .any(|member| member.node_id == config.node_id)
            };
            let stage_valid = transition.format_version == 1
                && transition.log_set_id == config.log_set_id
                && transition.old_policy.policy_epoch == config.policy_epoch
                && transition.next_policy.policy_epoch
                    == transition.old_policy.policy_epoch.saturating_add(1)
                && next_member.is_some_and(|member| member.public_key == local_public_key)
                && retention.last_position == transition.retained_last_position
                && local_root == transition.retained_root_sha256
                && readiness_matches;
            if !stage_valid && !config.policy_transition_faults.accept_invalid_stage {
                return Err("local tLog cannot stage the named successor policy".to_owned());
            }
            let statement = CellTaggedLogPolicyStageStatement {
                format_version: 1,
                cell_id: transition.cell_id,
                tenant_id: transition.tenant_id,
                generation: transition.generation,
                transition_id: transition.transition_id,
                log_set_id: transition.log_set_id,
                old_policy_epoch: transition.old_policy.policy_epoch,
                next_policy_epoch: transition.next_policy.policy_epoch,
                transition_sha256: cell_log_set_policy_transition_sha256(&transition),
                retained_root_sha256: transition.retained_root_sha256,
                retained_last_position: transition.retained_last_position,
            };
            let receipt = DurableTaggedLogPolicyStageReceipt {
                transition: transition.clone(),
                statement: statement.clone(),
            };
            persist_policy_stage(&config.root, &receipt)?;
            let attestation = sign_tagged_log_policy_stage_statement(
                signing_seed,
                &statement,
                CellTaggedLogPolicyStageAttestation {
                    signer_id: config.node_id,
                    source_sample_epoch: retention.sample_epoch,
                    signature: Vec::new(),
                },
            )?;
            Ok(TaggedLogResponse::PolicyStaged {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                transition_id: transition.transition_id,
                attestation,
                durable: true,
            })
        }
        TaggedLogRequest::PolicyActivate {
            transition,
            successor_stage,
            activation,
        } => {
            if let Some(existing) = policy_activation {
                if existing.transition == *transition
                    && existing.successor_stage_sha256
                        == cell_tagged_log_policy_stage_certificate_sha256(&successor_stage)
                    && existing.activation == activation
                {
                    return Ok(TaggedLogResponse::PolicyActivated {
                        log_set_id: config.log_set_id,
                        node_id: config.node_id,
                        transition_id: transition.transition_id,
                        policy_epoch: transition.next_policy.policy_epoch,
                        durable: true,
                    });
                }
                return Err("tLog root already activated a conflicting policy".to_owned());
            }
            let staged = load_policy_stage(&config.root)?
                .ok_or_else(|| "tLog has not durably staged the successor policy".to_owned())?;
            let local_member = transition
                .next_policy
                .members
                .iter()
                .any(|member| member.node_id == config.node_id);
            let stage_valid = staged.transition == *transition
                && staged.statement == successor_stage.statement
                && verify_tagged_log_policy_stage_certificate(&successor_stage, &transition);
            let activation_statement = &activation.statement;
            let activation_matches = activation_statement.format_version == 1
                && activation_statement.cell_id == transition.cell_id
                && activation_statement.tenant_id == transition.tenant_id
                && activation_statement.generation == transition.generation
                && activation_statement.transition_id == transition.transition_id
                && activation_statement.log_set_id == transition.log_set_id
                && activation_statement.next_policy_epoch == transition.next_policy.policy_epoch
                && activation_statement.next_policy_sha256
                    == cell_log_set_policy_sha256(&transition.next_policy)
                && activation_statement.successor_stage_sha256
                    == cell_tagged_log_policy_stage_certificate_sha256(&successor_stage);
            let authority_valid =
                config
                    .policy_activation_authority
                    .as_ref()
                    .is_some_and(|policy| {
                        verify_cell_log_set_policy_activation_certificate(
                            &activation,
                            &policy.members,
                            policy.quorum_size,
                        )
                    });
            if (!local_member
                && !config
                    .policy_transition_faults
                    .accept_removed_member_activation)
                || (!stage_valid && !config.policy_transition_faults.accept_invalid_stage)
                || !activation_matches
                || (!authority_valid
                    && !config
                        .policy_transition_faults
                        .accept_missing_authority_activation)
            {
                return Err("tLog policy activation lacks the committed authority proof".to_owned());
            }
            let receipt = DurableTaggedLogPolicyActivation {
                transition: transition.as_ref().clone(),
                successor_stage_sha256: cell_tagged_log_policy_stage_certificate_sha256(
                    &successor_stage,
                ),
                activation: activation.clone(),
            };
            persist_policy_activation(&config.root, &receipt)?;
            Ok(TaggedLogResponse::PolicyActivated {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                transition_id: transition.transition_id,
                policy_epoch: transition.next_policy.policy_epoch,
                durable: true,
            })
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct TaggedLogRetentionState {
    last_position: u64,
    popped_through: u64,
    sample_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableTaggedLogRepairReceipt {
    certificate: CellTaggedLogRepairCertificate,
    installed_records: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableTaggedLogPolicyStageReceipt {
    transition: CellLogSetPolicyTransition,
    statement: CellTaggedLogPolicyStageStatement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableTaggedLogPolicyActivation {
    transition: CellLogSetPolicyTransition,
    successor_stage_sha256: [u8; 32],
    activation: CellLogSetPolicyActivationCertificate,
}

fn retention_path(root: &Path) -> PathBuf {
    root.join("retention-state.json")
}

fn load_or_recover_retention(
    root: &Path,
    records: &[okv_wal::RecoveredRecord],
) -> Result<TaggedLogRetentionState, String> {
    let mut state = match fs::read(retention_path(root)) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            TaggedLogRetentionState::default()
        }
        Err(error) => return Err(error.to_string()),
    };
    let recovered_last = records.iter().try_fold(0_u64, |last, recovered| {
        let record: TaggedLogRecord =
            serde_json::from_slice(&recovered.payload).map_err(|error| error.to_string())?;
        Ok::<_, String>(last.max(record.position))
    })?;
    if recovered_last > state.last_position {
        state.last_position = recovered_last;
        state.sample_epoch = state.sample_epoch.saturating_add(1).max(1);
        persist_retention(root, &state)?;
    } else if state.sample_epoch == 0 {
        state.sample_epoch = 1;
        persist_retention(root, &state)?;
    }
    Ok(state)
}

fn persist_retention(root: &Path, state: &TaggedLogRetentionState) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let path = retention_path(root);
    let temporary = root.join(".retention-state.tmp");
    let bytes = serde_json::to_vec(state).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn repair_install_path(root: &Path) -> PathBuf {
    root.join("repair-install.json")
}

fn repair_ready_path(root: &Path) -> PathBuf {
    root.join("repair-ready.json")
}

fn repair_transfer_dir(root: &Path, transfer_id: u64) -> PathBuf {
    root.join(format!("repair-transfer-{transfer_id}"))
}

fn repair_transfer_descriptor_path(root: &Path, transfer_id: u64) -> PathBuf {
    repair_transfer_dir(root, transfer_id).join("descriptor.json")
}

fn repair_transfer_chunk_path(root: &Path, transfer_id: u64, chunk_index: u16) -> PathBuf {
    repair_transfer_dir(root, transfer_id).join(format!("chunk-{chunk_index:04}.bin"))
}

fn validate_repair_transfer(
    config: &TaggedLogProcessConfig,
    transfer: &TaggedLogRepairTransfer,
) -> Result<(), String> {
    validate_repair_target(config, &transfer.certificate)?;
    let phase = transfer.certificate.statement.phase;
    let phase_shape_valid = match phase {
        CellTaggedLogRepairPhase::BaseSnapshot => transfer.base_snapshot_sha256.is_none(),
        CellTaggedLogRepairPhase::LearnerReady => transfer.base_snapshot_sha256.is_some(),
    };
    if transfer.format_version != 1
        || transfer.transfer_id == 0
        || transfer.payload_length == 0
        || transfer.payload_length > 16 * 1024 * 1024
        || transfer.chunk_count == 0
        || transfer.chunk_count > 64
        || !phase_shape_valid
    {
        return Err("repair transfer descriptor is invalid".to_owned());
    }
    Ok(())
}

fn load_repair_transfer(
    root: &Path,
    transfer_id: u64,
) -> Result<Option<TaggedLogRepairTransfer>, String> {
    match fs::read(repair_transfer_descriptor_path(root, transfer_id)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_repair_transfer(root: &Path, transfer: &TaggedLogRepairTransfer) -> Result<(), String> {
    if let Some(existing) = load_repair_transfer(root, transfer.transfer_id)? {
        return if existing == *transfer {
            Ok(())
        } else {
            Err("repair transfer identity conflicts with durable state".to_owned())
        };
    }
    let directory = repair_transfer_dir(root, transfer.transfer_id);
    fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    persist_policy_receipt(
        &directory,
        &repair_transfer_descriptor_path(root, transfer.transfer_id),
        transfer,
    )
}

fn store_repair_chunk(
    config: &TaggedLogProcessConfig,
    transfer: &TaggedLogRepairTransfer,
    chunk_index: u16,
    chunk_bytes: &[u8],
) -> Result<TaggedLogResponse, String> {
    validate_repair_transfer(config, transfer)?;
    if chunk_index >= transfer.chunk_count
        || chunk_bytes.is_empty()
        || u64::try_from(chunk_bytes.len()).unwrap_or(u64::MAX) > transfer.payload_length
    {
        return Err("repair chunk is outside the transfer descriptor".to_owned());
    }
    persist_repair_transfer(&config.root, transfer)?;
    let directory = repair_transfer_dir(&config.root, transfer.transfer_id);
    let path = repair_transfer_chunk_path(&config.root, transfer.transfer_id, chunk_index);
    match fs::read(&path) {
        Ok(existing) => {
            if existing != chunk_bytes {
                return Err("repair chunk retry conflicts with durable bytes".to_owned());
            }
            return Ok(TaggedLogResponse::RepairChunkStored {
                log_set_id: config.log_set_id,
                node_id: config.node_id,
                transfer_id: transfer.transfer_id,
                chunk_index,
                chunk_bytes: u64::try_from(chunk_bytes.len()).unwrap_or(u64::MAX),
                durable: true,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    let temporary = directory.join(format!(".chunk-{chunk_index:04}.tmp"));
    if temporary.exists() {
        let recovered = fs::read(&temporary).map_err(|error| error.to_string())?;
        if recovered != chunk_bytes {
            return Err("repair chunk temporary state conflicts with retry".to_owned());
        }
        fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    } else {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        file.write_all(chunk_bytes)
            .map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    }
    File::open(&directory)
        .and_then(|value| value.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(TaggedLogResponse::RepairChunkStored {
        log_set_id: config.log_set_id,
        node_id: config.node_id,
        transfer_id: transfer.transfer_id,
        chunk_index,
        chunk_bytes: u64::try_from(chunk_bytes.len()).unwrap_or(u64::MAX),
        durable: true,
    })
}

fn finalize_repair_transfer(
    config: &TaggedLogProcessConfig,
    wal: &LocalReplicatedWal,
    local_records: &[okv_wal::RecoveredRecord],
    retention: &mut TaggedLogRetentionState,
    transfer: &TaggedLogRepairTransfer,
) -> Result<TaggedLogResponse, String> {
    validate_repair_transfer(config, transfer)?;
    let stored = load_repair_transfer(&config.root, transfer.transfer_id)?
        .ok_or_else(|| "repair transfer has no durable descriptor".to_owned())?;
    if stored != *transfer {
        return Err("repair finalize conflicts with durable transfer identity".to_owned());
    }
    let statement = &transfer.certificate.statement;
    let receipt_path = match statement.phase {
        CellTaggedLogRepairPhase::BaseSnapshot => repair_install_path(&config.root),
        CellTaggedLogRepairPhase::LearnerReady => repair_ready_path(&config.root),
    };
    if let Some(receipt) = load_repair_receipt(&receipt_path)? {
        if receipt.certificate != transfer.certificate {
            return Err("repair root already finalized a conflicting transfer".to_owned());
        }
        return Ok(repair_finalize_response(
            config,
            statement,
            receipt.installed_records,
        ));
    }
    let payload_records = load_repair_payload(&config.root, transfer)?;
    let mut combined =
        combine_repair_records(config, local_records, retention, transfer, payload_records)?;
    combined.sort_by_key(|record| record.position);
    let combined_bytes = encode_tagged_log_repair_snapshot(&combined)?;
    validate_repair_snapshot(config, statement, &combined_bytes, &[], retention, false)?;
    let rewritten = combined
        .iter()
        .enumerate()
        .map(|(offset, record)| {
            serde_json::to_vec(record)
                .map(|bytes| {
                    (
                        u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1),
                        bytes,
                    )
                })
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    wal.rewrite(&rewritten).map_err(|error| error.to_string())?;
    retention.last_position = statement.last_position;
    retention.popped_through = statement.popped_through;
    retention.sample_epoch = retention.sample_epoch.saturating_add(1).max(1);
    persist_retention(&config.root, retention)?;
    let installed_records = u64::try_from(combined.len()).unwrap_or(u64::MAX);
    let receipt = DurableTaggedLogRepairReceipt {
        certificate: transfer.certificate.clone(),
        installed_records,
    };
    persist_repair_receipt(&config.root, &receipt_path, &receipt)?;
    Ok(repair_finalize_response(
        config,
        statement,
        installed_records,
    ))
}

fn load_repair_payload(
    root: &Path,
    transfer: &TaggedLogRepairTransfer,
) -> Result<Vec<TaggedLogRecord>, String> {
    let mut payload = Vec::new();
    for chunk_index in 0..transfer.chunk_count {
        let chunk = fs::read(repair_transfer_chunk_path(
            root,
            transfer.transfer_id,
            chunk_index,
        ))
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("repair transfer is missing durable chunk {chunk_index}")
            } else {
                error.to_string()
            }
        })?;
        payload.extend_from_slice(&chunk);
    }
    let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) != transfer.payload_length
        || payload_digest != transfer.payload_sha256
    {
        return Err("repair transfer payload differs from its descriptor".to_owned());
    }
    let records: Vec<TaggedLogRecord> =
        serde_json::from_slice(&payload).map_err(|error| error.to_string())?;
    if records.is_empty()
        || records
            .windows(2)
            .any(|pair| pair[1].position != pair[0].position.saturating_add(1))
    {
        return Err("repair transfer payload is not one consecutive record run".to_owned());
    }
    Ok(records)
}

fn combine_repair_records(
    config: &TaggedLogProcessConfig,
    local_records: &[okv_wal::RecoveredRecord],
    retention: &TaggedLogRetentionState,
    transfer: &TaggedLogRepairTransfer,
    payload_records: Vec<TaggedLogRecord>,
) -> Result<Vec<TaggedLogRecord>, String> {
    let statement = &transfer.certificate.statement;
    match statement.phase {
        CellTaggedLogRepairPhase::BaseSnapshot => {
            if !local_records.is_empty()
                || retention.last_position != 0
                || retention.popped_through != 0
            {
                return Err("repair base transfer requires an empty learner root".to_owned());
            }
            Ok(payload_records)
        }
        CellTaggedLogRepairPhase::LearnerReady => {
            let installed = load_repair_receipt(&repair_install_path(&config.root))?
                .ok_or_else(|| "repair tail requires a durable base installation".to_owned())?;
            if installed.certificate.statement.repair_id != statement.repair_id
                || transfer.base_snapshot_sha256
                    != Some(installed.certificate.statement.snapshot_sha256)
            {
                return Err("repair tail does not extend the installed base".to_owned());
            }
            let mut base = local_records
                .iter()
                .map(|recovered| {
                    serde_json::from_slice::<TaggedLogRecord>(&recovered.payload)
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let base_bytes = encode_tagged_log_repair_snapshot(&base)?;
            let base_digest: [u8; 32] = Sha256::digest(base_bytes).into();
            if transfer.base_snapshot_sha256 != Some(base_digest)
                || payload_records.first().map(|record| record.position)
                    != Some(retention.last_position.saturating_add(1))
            {
                return Err("repair tail is not consecutive with the installed base".to_owned());
            }
            base.extend(payload_records);
            Ok(base)
        }
    }
}

fn repair_finalize_response(
    config: &TaggedLogProcessConfig,
    statement: &CellTaggedLogRepairStatement,
    installed_records: u64,
) -> TaggedLogResponse {
    match statement.phase {
        CellTaggedLogRepairPhase::BaseSnapshot => TaggedLogResponse::RepairInstalled {
            log_set_id: config.log_set_id,
            node_id: config.node_id,
            repair_id: statement.repair_id,
            last_position: statement.last_position,
            popped_through: statement.popped_through,
            installed_records,
            snapshot_bytes: statement.snapshot_length,
            durable: true,
        },
        CellTaggedLogRepairPhase::LearnerReady => TaggedLogResponse::RepairReady {
            log_set_id: config.log_set_id,
            node_id: config.node_id,
            repair_id: statement.repair_id,
            last_position: statement.last_position,
            retained_root_sha256: statement.snapshot_sha256,
            durable: true,
        },
    }
}

fn policy_stage_path(root: &Path) -> PathBuf {
    root.join("policy-stage.json")
}

fn policy_activation_path(root: &Path) -> PathBuf {
    root.join("policy-activation.json")
}

fn load_policy_stage(root: &Path) -> Result<Option<DurableTaggedLogPolicyStageReceipt>, String> {
    match fs::read(policy_stage_path(root)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn load_policy_activation(root: &Path) -> Result<Option<DurableTaggedLogPolicyActivation>, String> {
    match fs::read(policy_activation_path(root)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_policy_stage(
    root: &Path,
    receipt: &DurableTaggedLogPolicyStageReceipt,
) -> Result<(), String> {
    if let Some(existing) = load_policy_stage(root)? {
        return if existing == *receipt {
            Ok(())
        } else {
            Err("tLog root already staged a conflicting policy".to_owned())
        };
    }
    persist_policy_receipt(root, &policy_stage_path(root), receipt)
}

fn persist_policy_activation(
    root: &Path,
    receipt: &DurableTaggedLogPolicyActivation,
) -> Result<(), String> {
    if let Some(existing) = load_policy_activation(root)? {
        return if existing == *receipt {
            Ok(())
        } else {
            Err("tLog root already activated a conflicting policy".to_owned())
        };
    }
    persist_policy_receipt(root, &policy_activation_path(root), receipt)
}

fn persist_policy_receipt<T: Serialize>(
    root: &Path,
    path: &Path,
    receipt: &T,
) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let temporary = root.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("policy-receipt")
    ));
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn load_repair_receipt(path: &Path) -> Result<Option<DurableTaggedLogRepairReceipt>, String> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_repair_receipt(
    root: &Path,
    path: &Path,
    receipt: &DurableTaggedLogRepairReceipt,
) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let temporary = root.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("repair-receipt")
    ));
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn validate_repair_target(
    config: &TaggedLogProcessConfig,
    certificate: &CellTaggedLogRepairCertificate,
) -> Result<(), String> {
    let policy = config
        .repair_source_policy
        .as_ref()
        .ok_or_else(|| "tagged-log learner has no pinned repair-source policy".to_owned())?;
    let signing_seed = config
        .signing_seed
        .as_deref()
        .ok_or_else(|| "tagged-log learner has no private identity key".to_owned())?;
    let local_public_key = tagged_log_public_key(signing_seed)?;
    let statement = &certificate.statement;
    let target_matches = statement.log_set_id == config.log_set_id
        && statement.learner_node_id == config.node_id
        && statement.learner_incarnation == config.storage_incarnation
        && statement.learner_public_key == local_public_key;
    let certificate_valid = verify_tagged_log_repair_certificate(certificate, policy);
    if !config.learner
        || config.storage_incarnation == [0; 16]
        || (!target_matches && !config.repair_faults.accept_target_mismatch)
        || (!certificate_valid && !config.repair_faults.accept_invalid_certificate)
    {
        return Err(format!(
            "repair certificate does not authorize this learner identity: learner={}, incarnation={}, target_matches={target_matches}, certificate_valid={certificate_valid}",
            config.learner,
            config.storage_incarnation != [0; 16]
        ));
    }
    Ok(())
}

fn validate_repair_snapshot(
    config: &TaggedLogProcessConfig,
    statement: &CellTaggedLogRepairStatement,
    snapshot_bytes: &[u8],
    local_records: &[okv_wal::RecoveredRecord],
    retention: &TaggedLogRetentionState,
    require_local_match: bool,
) -> Result<Vec<TaggedLogRecord>, String> {
    let supplied_sha256: [u8; 32] = Sha256::digest(snapshot_bytes).into();
    let snapshot_identity_matches = statement.snapshot_length
        == u64::try_from(snapshot_bytes.len()).unwrap_or(u64::MAX)
        && statement.snapshot_sha256 == supplied_sha256;
    if statement.format_version != 1
        || statement.generation == 0
        || statement.log_set_id != config.log_set_id
        || (!snapshot_identity_matches && !config.repair_faults.accept_snapshot_identity_mismatch)
    {
        return Err("repair snapshot identity does not match supplied bytes".to_owned());
    }
    let records: Vec<TaggedLogRecord> =
        serde_json::from_slice(snapshot_bytes).map_err(|error| error.to_string())?;
    if records.is_empty()
        || (records.last().map(|record| record.position) != Some(statement.last_position)
            && !config.repair_faults.accept_local_snapshot_mismatch)
        || records
            .windows(2)
            .any(|pair| pair[1].position != pair[0].position.saturating_add(1))
    {
        return Err("repair snapshot does not contain one contiguous retained suffix".to_owned());
    }
    for record in &records {
        validate_record(record, config.accept_missing_required_tags)?;
        if !record.range_tags.contains(&config.log_set_id) {
            return Err("repair snapshot record omits its log-set tag".to_owned());
        }
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        if envelope.cell_id() != statement.cell_id
            || envelope.tenant_id() != statement.tenant_id
            || envelope.generation() != statement.generation
            || envelope.version().sequence() <= statement.popped_through
        {
            return Err("repair snapshot record differs from certified domain".to_owned());
        }
    }
    if require_local_match && !config.repair_faults.accept_local_snapshot_mismatch {
        if retention.last_position != statement.last_position
            || retention.popped_through != statement.popped_through
            || retention.sample_epoch == 0
        {
            return Err("repair snapshot differs from the local retained frontier".to_owned());
        }
        let local = local_records
            .iter()
            .map(|recovered| {
                serde_json::from_slice::<TaggedLogRecord>(&recovered.payload)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if local != records {
            return Err("repair snapshot differs from local retained bytes".to_owned());
        }
    }
    Ok(records)
}

/// Canonically encode one ordered retained suffix for a repair request.
///
/// # Errors
///
/// Returns a serialization error if a record cannot be encoded.
pub fn encode_tagged_log_repair_snapshot(records: &[TaggedLogRecord]) -> Result<Vec<u8>, String> {
    serde_json::to_vec(records).map_err(|error| error.to_string())
}

fn retained_frame_bytes(
    records: &[okv_wal::RecoveredRecord],
    popped_through: u64,
) -> Result<u64, String> {
    records.iter().try_fold(0_u64, |total, recovered| {
        let record: TaggedLogRecord =
            serde_json::from_slice(&recovered.payload).map_err(|error| error.to_string())?;
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        if envelope.version().sequence() <= popped_through {
            return Ok(total);
        }
        Ok(total
            .saturating_add(u64::try_from(recovered.payload.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(FRAME_HEADER_BYTES).unwrap_or(u64::MAX))
            .saturating_add(FRAME_CHECKSUM_BYTES))
    })
}

fn validate_statement_domain(
    records: &[okv_wal::RecoveredRecord],
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> Result<(), String> {
    if records.is_empty() {
        return Err("tagged-log statement requires at least one retained record".to_owned());
    }
    for recovered in records {
        let record: TaggedLogRecord =
            serde_json::from_slice(&recovered.payload).map_err(|error| error.to_string())?;
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        if envelope.cell_id() != cell_id
            || envelope.tenant_id() != tenant_id
            || envelope.generation() != generation
        {
            return Err("tagged-log statement differs from retained domain".to_owned());
        }
    }
    Ok(())
}

fn validate_capacity_statement(
    config: &TaggedLogProcessConfig,
    effective_policy_epoch: u64,
    statement: &CellTaggedLogCapacityStatement,
    records: &[okv_wal::RecoveredRecord],
    retention: &TaggedLogRetentionState,
) -> Result<(), String> {
    if statement.format_version != 1
        || statement.generation == 0
        || statement.log_set_id != config.log_set_id
        || statement.policy_epoch != effective_policy_epoch
        || statement.projected_frame_bytes == 0
        || statement.soft_limit_bytes == 0
        || statement.reservation_epoch == 0
        || statement.soft_limit_bytes > config.retained_bytes_limit
        || retention.sample_epoch == 0
    {
        return Err("tagged-log capacity statement does not match active policy".to_owned());
    }
    if records.is_empty() {
        Ok(())
    } else {
        validate_statement_domain(
            records,
            statement.cell_id,
            statement.tenant_id,
            statement.generation,
        )
    }
}

fn validate_pop_statement(
    config: &TaggedLogProcessConfig,
    effective_policy_epoch: u64,
    statement: &CellTaggedLogPopStatement,
    capability: &PublicationPopCapabilityCertificate,
    manifest_bytes: &[u8],
    records: &[okv_wal::RecoveredRecord],
    retention: &TaggedLogRetentionState,
) -> Result<(), String> {
    if statement.format_version != 1
        || statement.generation == 0
        || statement.log_set_id != config.log_set_id
        || statement.policy_epoch != effective_policy_epoch
        || statement.publication_root_sha256 == [0; 32]
        || statement.object_frontier < retention.popped_through
        || statement.pop_epoch == 0
    {
        return Err("tagged-log pop statement does not match active policy".to_owned());
    }
    if !config.accept_unauthenticated_pop {
        let publication_policy = config
            .publication_pop_policy
            .as_ref()
            .ok_or_else(|| "tagged-log process has no publication pop policy".to_owned())?;
        if !verify_publication_pop_capability(
            capability,
            &publication_policy.members,
            publication_policy.quorum_size,
        ) {
            return Err(
                "publication pop capability does not satisfy pinned quorum policy".to_owned(),
            );
        }
        let capability_statement = &capability.statement;
        let manifest_reference_bytes = serde_json::to_vec(&capability_statement.manifest)
            .map_err(|error| error.to_string())?;
        let manifest_reference_sha256: [u8; 32] = Sha256::digest(manifest_reference_bytes).into();
        let manifest_sha256 = hex_sha256(manifest_bytes);
        let snapshot: CellStateSnapshot =
            serde_json::from_slice(manifest_bytes).map_err(|error| error.to_string())?;
        if capability_statement.generation != statement.generation
            || capability_statement.object_frontier != statement.object_frontier
            || capability_statement.pop_epoch != statement.pop_epoch
            || manifest_reference_sha256 != statement.publication_root_sha256
            || capability_statement.manifest.length
                != u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX)
            || capability_statement.manifest.sha256 != manifest_sha256
            || snapshot.cell_id != statement.cell_id
            || snapshot.tenant_id != statement.tenant_id
            || snapshot.generation != statement.generation
            || snapshot.latest_sequence != statement.object_frontier
        {
            return Err(
                "publication pop capability does not bind the exact object frontier".to_owned(),
            );
        }
    }
    validate_statement_domain(
        records,
        statement.cell_id,
        statement.tenant_id,
        statement.generation,
    )?;
    let maximum_version = records.iter().try_fold(0_u64, |maximum, recovered| {
        let record: TaggedLogRecord =
            serde_json::from_slice(&recovered.payload).map_err(|error| error.to_string())?;
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        Ok::<_, String>(maximum.max(envelope.version().sequence()))
    })?;
    if statement.object_frontier > maximum_version {
        return Err("tagged-log pop exceeds its retained commit frontier".to_owned());
    }
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
    }
    value
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableTaggedLogFence {
    statement: CellTaggedLogFenceStatement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableTaggedLogPrefixFence {
    statement: CellTaggedLogPrefixFenceStatement,
}

fn fence_path(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("generation-{generation}.fence.json"))
}

fn prefix_fence_path(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("generation-{generation}.prefix-fence.json"))
}

fn load_fence(root: &Path, generation: u64) -> Result<Option<DurableTaggedLogFence>, String> {
    let path = fence_path(root, generation);
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_fence(root: &Path, statement: &CellTaggedLogFenceStatement) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let path = fence_path(root, statement.generation);
    if let Some(existing) = load_fence(root, statement.generation)? {
        return if existing.statement == *statement {
            Ok(())
        } else {
            Err("tagged-log generation is already fenced by another recovery".to_owned())
        };
    }
    let temporary = root.join(format!(".generation-{}.fence.tmp", statement.generation));
    if temporary.exists() {
        let recovered = fs::read(&temporary)
            .map_err(|error| error.to_string())
            .and_then(|bytes| {
                serde_json::from_slice::<DurableTaggedLogFence>(&bytes)
                    .map_err(|error| error.to_string())
            })?;
        if recovered.statement != *statement {
            return Err("tagged-log fence temporary file conflicts with retry".to_owned());
        }
        fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
        return File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string());
    }
    let bytes = serde_json::to_vec(&DurableTaggedLogFence {
        statement: statement.clone(),
    })
    .map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn load_prefix_fence(
    root: &Path,
    generation: u64,
) -> Result<Option<DurableTaggedLogPrefixFence>, String> {
    let path = prefix_fence_path(root, generation);
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn persist_prefix_fence(
    root: &Path,
    statement: &CellTaggedLogPrefixFenceStatement,
) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let path = prefix_fence_path(root, statement.generation);
    if let Some(existing) = load_prefix_fence(root, statement.generation)? {
        return if existing.statement == *statement {
            Ok(())
        } else {
            Err("tagged-log generation is already prefix-fenced by another recovery".to_owned())
        };
    }
    let temporary = root.join(format!(
        ".generation-{}.prefix-fence.tmp",
        statement.generation
    ));
    if temporary.exists() {
        let recovered = fs::read(&temporary)
            .map_err(|error| error.to_string())
            .and_then(|bytes| {
                serde_json::from_slice::<DurableTaggedLogPrefixFence>(&bytes)
                    .map_err(|error| error.to_string())
            })?;
        if recovered.statement != *statement {
            return Err("tagged-log prefix-fence temporary file conflicts with retry".to_owned());
        }
        fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
        return File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string());
    }
    let bytes = serde_json::to_vec(&DurableTaggedLogPrefixFence {
        statement: statement.clone(),
    })
    .map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn validate_record(
    record: &TaggedLogRecord,
    accept_missing_required_tags: bool,
) -> Result<(), String> {
    if record.format_version != TAGGED_LOG_FORMAT_VERSION || record.position == 0 {
        return Err("tagged-log record has invalid identity".to_owned());
    }
    let unique = record.range_tags.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != record.range_tags.len() || unique.is_empty() {
        return Err("tagged-log record tags must be non-empty and unique".to_owned());
    }
    let envelope = CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
    if !accept_missing_required_tags
        && envelope
            .required_log_tags()
            .iter()
            .any(|tag| !unique.contains(tag))
    {
        return Err("tagged-log record omits an envelope-required tag".to_owned());
    }
    Ok(())
}

/// Send one bounded request to a tagged-log endpoint.
///
/// # Errors
///
/// Returns an error for transport or response framing failure.
pub fn tagged_log_request(
    endpoint: &str,
    request: &TaggedLogRequest,
) -> Result<TaggedLogResponse, String> {
    let address = endpoint
        .parse::<SocketAddr>()
        .map_err(|error| error.to_string())?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    let mut bytes = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    stream
        .write_all(&bytes)
        .map_err(|error| error.to_string())?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    serde_json::from_str(&line).map_err(|error| error.to_string())
}

/// Local independent-process fixture for the bounded tagged-log contract.
pub struct TaggedLogProcessFixture {
    processes: Vec<Child>,
    endpoints: Vec<String>,
    roots: Vec<PathBuf>,
    configs: Vec<TaggedLogProcessConfig>,
    executable: PathBuf,
}

impl TaggedLogProcessFixture {
    /// Start the requested number of independent process and storage roots.
    ///
    /// # Errors
    ///
    /// Returns an error if a process cannot start or become responsive.
    pub fn start(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        count: usize,
        retained_bytes_limit: u64,
        accept_missing_required_tags: bool,
    ) -> Result<Self, String> {
        Self::start_inner(
            executable,
            root,
            log_set_id,
            count,
            retained_bytes_limit,
            accept_missing_required_tags,
            0,
            None,
            true,
            None,
            false,
        )
    }

    /// Start authenticated tagged-log processes with one private seed per node.
    ///
    /// # Errors
    ///
    /// Returns an error when the signer set is invalid or a process cannot start.
    #[allow(clippy::too_many_arguments)]
    pub fn start_signed(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        count: usize,
        retained_bytes_limit: u64,
        accept_missing_required_tags: bool,
        policy_epoch: u64,
        signing_seeds: &[Vec<u8>],
    ) -> Result<Self, String> {
        if policy_epoch == 0 || signing_seeds.len() != count {
            return Err("signed tagged-log fixture requires one key per node".to_owned());
        }
        Self::start_inner(
            executable,
            root,
            log_set_id,
            count,
            retained_bytes_limit,
            accept_missing_required_tags,
            policy_epoch,
            Some(signing_seeds),
            true,
            None,
            false,
        )
    }

    /// Start signed processes that require a pinned publication-authority quorum for pop.
    ///
    /// # Errors
    ///
    /// Returns an error when either signer policy is invalid or a process cannot start.
    #[allow(clippy::too_many_arguments)]
    pub fn start_signed_with_publication_pop_policy(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        count: usize,
        retained_bytes_limit: u64,
        accept_missing_required_tags: bool,
        policy_epoch: u64,
        signing_seeds: &[Vec<u8>],
        publication_pop_policy: &PublicationPopPolicy,
        accept_unauthenticated_pop: bool,
    ) -> Result<Self, String> {
        if policy_epoch == 0
            || signing_seeds.len() != count
            || publication_pop_policy.quorum_size == 0
            || usize::from(publication_pop_policy.quorum_size)
                > publication_pop_policy.members.len()
        {
            return Err("signed tagged-log fixture has an invalid pop policy".to_owned());
        }
        Self::start_inner(
            executable,
            root,
            log_set_id,
            count,
            retained_bytes_limit,
            accept_missing_required_tags,
            policy_epoch,
            Some(signing_seeds),
            true,
            Some(publication_pop_policy),
            accept_unauthenticated_pop,
        )
    }

    /// Start authenticated processes whose generation fences may be deliberately volatile.
    ///
    /// # Errors
    ///
    /// Returns an error when the signer set is invalid or a process cannot start.
    #[allow(clippy::too_many_arguments)]
    pub fn start_signed_with_fence_persistence(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        count: usize,
        retained_bytes_limit: u64,
        accept_missing_required_tags: bool,
        policy_epoch: u64,
        signing_seeds: &[Vec<u8>],
        persist_fences: bool,
    ) -> Result<Self, String> {
        if policy_epoch == 0 || signing_seeds.len() != count {
            return Err("signed tagged-log fixture requires one key per node".to_owned());
        }
        Self::start_inner(
            executable,
            root,
            log_set_id,
            count,
            retained_bytes_limit,
            accept_missing_required_tags,
            policy_epoch,
            Some(signing_seeds),
            persist_fences,
            None,
            false,
        )
    }

    /// Start one empty signed learner outside the active repair-source policy.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid learner identity or process startup failure.
    #[allow(clippy::too_many_arguments)]
    pub fn start_repair_learner(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        node_id: u64,
        retained_bytes_limit: u64,
        policy_epoch: u64,
        signing_seed: Vec<u8>,
        storage_incarnation: [u8; 16],
        repair_source_policy: CellLogSetPolicy,
        repair_faults: TaggedLogRepairFaults,
    ) -> Result<Self, String> {
        if node_id == 0
            || storage_incarnation == [0; 16]
            || repair_source_policy.log_set_id != log_set_id
            || repair_source_policy.policy_epoch != policy_epoch
            || repair_source_policy
                .members
                .iter()
                .any(|member| member.node_id == node_id)
        {
            return Err("repair learner identity conflicts with active policy".to_owned());
        }
        let endpoint = allocate_endpoint()?;
        let process_root = root.join(format!("learner-{node_id}"));
        let config = TaggedLogProcessConfig {
            log_set_id,
            node_id,
            listen_addr: endpoint.clone(),
            root: process_root.clone(),
            retained_bytes_limit,
            accept_missing_required_tags: false,
            policy_epoch,
            signing_seed: Some(signing_seed),
            publication_pop_policy: None,
            accept_unauthenticated_pop: false,
            persist_fences: true,
            storage_incarnation,
            learner: true,
            repair_source_policy: Some(repair_source_policy),
            repair_faults,
            policy_activation_authority: None,
            policy_transition_faults: TaggedLogPolicyTransitionFaults::default(),
        };
        let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("tagged-log-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start tagged-log repair learner: {error}"))?;
        let fixture = Self {
            processes: vec![child],
            endpoints: vec![endpoint],
            roots: vec![process_root],
            configs: vec![config],
            executable: executable.to_path_buf(),
        };
        fixture.wait_until_ready()?;
        Ok(fixture)
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner(
        executable: &Path,
        root: &Path,
        log_set_id: u16,
        count: usize,
        retained_bytes_limit: u64,
        accept_missing_required_tags: bool,
        policy_epoch: u64,
        signing_seeds: Option<&[Vec<u8>]>,
        persist_fences: bool,
        publication_pop_policy: Option<&PublicationPopPolicy>,
        accept_unauthenticated_pop: bool,
    ) -> Result<Self, String> {
        let mut processes = Vec::with_capacity(count);
        let mut endpoints = Vec::with_capacity(count);
        let mut roots = Vec::with_capacity(count);
        let mut configs = Vec::with_capacity(count);
        for index in 0..count {
            let endpoint = allocate_endpoint()?;
            let process_root = root.join(format!("node-{index}"));
            let config = TaggedLogProcessConfig {
                log_set_id,
                node_id: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                listen_addr: endpoint.clone(),
                root: process_root.clone(),
                retained_bytes_limit,
                accept_missing_required_tags,
                policy_epoch,
                signing_seed: signing_seeds
                    .as_ref()
                    .and_then(|seeds| seeds.get(index).cloned()),
                publication_pop_policy: publication_pop_policy.cloned(),
                accept_unauthenticated_pop,
                persist_fences,
                storage_incarnation: [0; 16],
                learner: false,
                repair_source_policy: None,
                repair_faults: TaggedLogRepairFaults::default(),
                policy_activation_authority: None,
                policy_transition_faults: TaggedLogPolicyTransitionFaults::default(),
            };
            let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
            let child = Command::new(executable)
                .arg("tagged-log-node")
                .arg("--config-json")
                .arg(config_json)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|error| format!("failed to start tagged-log process {index}: {error}"))?;
            processes.push(child);
            endpoints.push(endpoint);
            roots.push(process_root);
            configs.push(config);
        }
        let fixture = Self {
            processes,
            endpoints,
            roots,
            configs,
            executable: executable.to_path_buf(),
        };
        fixture.wait_until_ready()?;
        Ok(fixture)
    }

    /// All configured endpoints, including a subsequently killed process.
    #[must_use]
    pub fn endpoints(&self) -> Vec<String> {
        self.endpoints.clone()
    }

    /// Exact private storage roots for distinctness checks.
    #[must_use]
    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Kill one process after its durable append.
    ///
    /// # Errors
    ///
    /// Returns an error if the process cannot be killed and reaped.
    pub fn kill(&mut self, index: usize) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(index)
            .ok_or_else(|| "unknown tagged-log process index".to_owned())?;
        process.kill().map_err(|error| error.to_string())?;
        process.wait().map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Restart one process against its existing private stable root.
    ///
    /// # Errors
    ///
    /// Returns an error if the process cannot start and become responsive.
    pub fn restart(&mut self, index: usize) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(index)
            .ok_or_else(|| "unknown tagged-log process index".to_owned())?;
        if process
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_none()
        {
            return Err("tagged-log process must be stopped before restart".to_owned());
        }
        let config = self
            .configs
            .get(index)
            .ok_or_else(|| "unknown tagged-log process config".to_owned())?;
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        *process = Command::new(&self.executable)
            .arg("tagged-log-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to restart tagged-log process {index}: {error}"))?;
        self.wait_until_ready()
    }

    /// Restart one process with a pinned policy-activation authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the index is absent or restart fails.
    pub fn restart_with_policy_activation_authority(
        &mut self,
        index: usize,
        authority: PublicationPopPolicy,
        faults: TaggedLogPolicyTransitionFaults,
    ) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(index)
            .ok_or_else(|| "unknown tagged-log process index".to_owned())?;
        if process
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_none()
        {
            process.kill().map_err(|error| error.to_string())?;
            process.wait().map_err(|error| error.to_string())?;
        }
        let config = self
            .configs
            .get_mut(index)
            .ok_or_else(|| "unknown tagged-log process config".to_owned())?;
        config.policy_activation_authority = Some(authority);
        config.policy_transition_faults = faults;
        self.restart(index)
    }

    fn wait_until_ready(&self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        for endpoint in &self.endpoints {
            loop {
                if matches!(
                    tagged_log_request(endpoint, &TaggedLogRequest::Status),
                    Ok(TaggedLogResponse::Ready { .. })
                ) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "tagged-log process did not become ready: {endpoint}"
                    ));
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(())
    }
}

impl Drop for TaggedLogProcessFixture {
    fn drop(&mut self) {
        for process in &mut self.processes {
            if process.try_wait().ok().flatten().is_none() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

fn allocate_endpoint() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    drop(listener);
    Ok(address.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okv_consensus::{
        PublicationObjectKind, PublicationObjectReference, PublicationPopCapabilityStatement,
    };
    use okv_model::Version;
    use okv_sim::CommitEnvelopeParts;

    fn envelope() -> Vec<u8> {
        CommitEnvelope::from_parts(CommitEnvelopeParts {
            cell_id: [1; 16],
            tenant_id: [2; 16],
            generation: 1,
            version: Version::from_parts(1, 2),
            log_index: 2,
            client_id: [3; 16],
            request_id: 4,
            resolver_set_id: [5; 16],
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
            canonical_mutations: b"[]".to_vec(),
            required_resolvers: vec![1],
            required_log_tags: vec![10, 20],
            previous_log_chain: [0; 32],
        })
        .encode()
    }

    #[test]
    fn exact_required_tags_validate() {
        let record = TaggedLogRecord::committed(1, vec![10, 20], envelope());
        assert!(validate_record(&record, false).is_ok());
    }

    #[test]
    fn missing_required_tag_fails_closed() {
        let record = TaggedLogRecord::committed(1, vec![20], envelope());
        assert!(validate_record(&record, false).is_err());
        assert!(validate_record(&record, true).is_ok());
    }

    #[test]
    fn compact_binary_record_remains_legacy_json_readable() {
        let envelope = envelope();
        let record = TaggedLogRecord::committed(1, vec![10, 20], envelope.clone());
        let compact = serde_json::to_value(&record).unwrap();
        assert!(compact["envelope"].is_string());

        let legacy = serde_json::json!({
            "format_version": 1,
            "position": 1,
            "range_tags": [10, 20],
            "envelope": envelope,
            "padding": [0, 1, 2]
        });
        let decoded: TaggedLogRecord = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.envelope, record.envelope);
        assert_eq!(decoded.padding, vec![0, 1, 2]);
    }

    #[test]
    fn pop_rejects_a_capability_without_the_pinned_publication_quorum() {
        let config = TaggedLogProcessConfig {
            log_set_id: 10,
            node_id: 1,
            listen_addr: "127.0.0.1:1".to_owned(),
            root: PathBuf::from("unused"),
            retained_bytes_limit: 16_384,
            accept_missing_required_tags: false,
            policy_epoch: 1,
            signing_seed: Some(vec![1; 32]),
            publication_pop_policy: Some(PublicationPopPolicy {
                members: BTreeMap::from([(101, vec![1; 32]), (102, vec![2; 32])]),
                quorum_size: 2,
            }),
            accept_unauthenticated_pop: false,
            persist_fences: true,
            storage_incarnation: [0; 16],
            learner: false,
            repair_source_policy: None,
            repair_faults: TaggedLogRepairFaults::default(),
            policy_activation_authority: None,
            policy_transition_faults: TaggedLogPolicyTransitionFaults::default(),
        };
        let statement = CellTaggedLogPopStatement {
            format_version: 1,
            cell_id: [1; 16],
            tenant_id: [2; 16],
            generation: 1,
            log_set_id: 10,
            policy_epoch: 1,
            publication_root_sha256: [3; 32],
            object_frontier: 2,
            pop_epoch: 1,
        };
        let capability = PublicationPopCapabilityCertificate {
            statement: PublicationPopCapabilityStatement {
                format_version: 1,
                authority_cell_id: 17,
                generation: 1,
                transaction_system_id: "cell-process-g1".to_owned(),
                destination_root: "cell-17/ranges/all".to_owned(),
                manifest: PublicationObjectReference {
                    kind: PublicationObjectKind::Manifest,
                    key: "objects/base-2".to_owned(),
                    length: 1,
                    sha256: "a".repeat(64),
                },
                object_frontier: 2,
                pop_epoch: 1,
            },
            attestations: Vec::new(),
        };
        let payload =
            serde_json::to_vec(&TaggedLogRecord::committed(1, vec![10, 20], envelope())).unwrap();
        let records = vec![okv_wal::RecoveredRecord {
            log_index: 1,
            payload,
            replica_ids: vec![1, 2],
        }];
        let retention = TaggedLogRetentionState {
            last_position: 1,
            popped_through: 0,
            sample_epoch: 1,
        };
        let error = validate_pop_statement(
            &config,
            config.policy_epoch,
            &statement,
            &capability,
            b"invalid",
            &records,
            &retention,
        )
        .unwrap_err();
        assert!(error.contains("pinned quorum policy"));
    }
}
