use super::tagged_log_process::{
    tagged_log_request, TaggedLogProcessFixture, TaggedLogRecord, TaggedLogRequest,
    TaggedLogResponse,
};
use super::{filesystem_backend, sha256, ObjectClient};
use bytes::Bytes;
use okv_consensus::{
    run_cell_process_prototype, CellCommittedEnvelopeRequest, CellMutation, CellProcessFixture,
    CellProcessPrototypeMode, CellTransactionClient, GenerationCredential, PublicationAction,
    PublicationAuthorityProcessFixture, PublicationClient, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity,
};
use okv_sim::CommitEnvelope;
use okv_wal::LocalReplicatedWal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const PUBLICATION_CELL_ID: u64 = 17;
const DESTINATION_ROOT: &str = "cell-17/ranges/all/serving-recovery";
const TRANSACTION_SYSTEM_ID: &str = "cell-process-g1";
const EXPECTED_CHECKS: usize = 15;
const AUTHORITY_FEED_EXPECTED_CHECKS: usize = 16;
const TAGGED_TLOG_EXPECTED_CHECKS: usize = 23;
const TAGGED_TLOG_NODES: usize = 3;
const TAGGED_TLOG_QUORUM: usize = 2;
const ASSIGNED_RANGE_TAG: u16 = 10;
const TAGGED_TLOG_RETAINED_BYTES_LIMIT: u64 = 4096;

/// Deliberately unsafe serving-worker behavior used by the frozen eval suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellServingRecoveryMode {
    Correct,
    IgnoreRetainedSuffix,
}

impl CellServingRecoveryMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::IgnoreRetainedSuffix => "ignore_retained_suffix",
        }
    }
}

/// One named assertion in the serving-worker recovery contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingRecoveryCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable receipt for one object-base plus retained-WAL recovery history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingRecoveryReport {
    pub seed: u64,
    pub mode: CellServingRecoveryMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub commit_frontier: u64,
    pub object_frontier: u64,
    pub target_version: u64,
    pub observed_frontier: u64,
    pub base_envelopes: u64,
    pub suffix_envelopes: u64,
    pub suffix_records_recovered: u64,
    pub transaction_process_starts: u64,
    pub publication_process_starts: u64,
    pub serving_worker_process_starts: u64,
    pub process_kills: u64,
    pub object_puts: u64,
    pub object_reads: u64,
    pub reconstructed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub base_tail_chain_valid: bool,
    pub checks: Vec<CellServingRecoveryCheck>,
    pub trace_sha256: String,
}

/// Configuration passed to one disposable serving worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingWorkerProcessConfig {
    pub object_root: PathBuf,
    pub publication_endpoints: Vec<String>,
    pub destination_root: String,
    pub wal_root: PathBuf,
    pub wal_replicas: u8,
    pub wal_quorum: usize,
    pub target_version: u64,
    pub ignore_retained_suffix: bool,
    pub output_path: PathBuf,
}

/// Deliberately unsafe live-authority behavior used by RFC-0037's control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellServingAuthorityFeedMode {
    Correct,
    DropFinalEnvelope,
}

impl CellServingAuthorityFeedMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DropFinalEnvelope => "drop_final_envelope",
        }
    }
}

/// One named assertion in the live-authority serving contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingAuthorityFeedCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable receipt for object-base plus live transaction-authority recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingAuthorityFeedReport {
    pub seed: u64,
    pub mode: CellServingAuthorityFeedMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub commit_frontier: u64,
    pub object_frontier: u64,
    pub target_version: u64,
    pub observed_frontier: u64,
    pub authority_position: u64,
    pub authority_feed_envelopes: u64,
    pub expected_suffix_envelopes: u64,
    pub transaction_process_starts: u64,
    pub publication_process_starts: u64,
    pub serving_worker_process_starts: u64,
    pub transaction_leader_kills: u64,
    pub copied_wal_directories: u64,
    pub object_puts: u64,
    pub object_reads: u64,
    pub killed_transaction_leader: u64,
    pub successor_transaction_leader: u64,
    pub reconstructed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub base_tail_chain_valid: bool,
    pub checks: Vec<CellServingAuthorityFeedCheck>,
    pub trace_sha256: String,
}

/// Configuration passed to one live-authority serving worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingAuthorityWorkerProcessConfig {
    pub object_root: PathBuf,
    pub publication_endpoints: Vec<String>,
    pub transaction_endpoints: Vec<String>,
    pub destination_root: String,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub object_frontier: u64,
    pub target_version: u64,
    pub drop_final_envelope: bool,
    pub output_path: PathBuf,
}

/// Deliberately unsafe tagged-log behavior used by RFC-0038's control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellServingTaggedTlogMode {
    Correct,
    OmitRequiredRangeTag,
}

impl CellServingTaggedTlogMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitRequiredRangeTag => "omit_required_range_tag",
        }
    }
}

/// One named assertion in the range-tagged tLog serving contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingTaggedTlogCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable receipt for object-base plus dedicated tagged-log recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingTaggedTlogReport {
    pub seed: u64,
    pub mode: CellServingTaggedTlogMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub commit_frontier: u64,
    pub object_frontier: u64,
    pub target_version: u64,
    pub observed_frontier: u64,
    pub assigned_range_tag: u16,
    pub expected_suffix_envelopes: u64,
    pub tlog_append_acks: u64,
    pub tlog_required_tags_present: bool,
    pub tlog_backpressure_rejections: u64,
    pub tlog_survivor_responses: u64,
    pub tlog_quorum_records: u64,
    pub transaction_process_starts: u64,
    pub publication_process_starts: u64,
    pub tlog_process_starts: u64,
    pub serving_worker_process_starts: u64,
    pub tlog_process_kills: u64,
    pub killed_tlog_process: u64,
    pub object_puts: u64,
    pub object_reads: u64,
    pub reconstructed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub base_tail_chain_valid: bool,
    pub checks: Vec<CellServingTaggedTlogCheck>,
    pub trace_sha256: String,
}

/// Configuration passed to one tagged-log serving worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellServingTaggedTlogWorkerProcessConfig {
    pub object_root: PathBuf,
    pub publication_endpoints: Vec<String>,
    pub tlog_endpoints: Vec<String>,
    pub tlog_quorum: usize,
    pub destination_root: String,
    pub object_frontier: u64,
    pub target_version: u64,
    pub assigned_range_tag: u16,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellServingWorkerReceipt {
    resolved_published_root: bool,
    base_frontier: u64,
    target_version: u64,
    observed_frontier: u64,
    suffix_records_recovered: u64,
    object_reads: u64,
    base_tail_chain_valid: bool,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellServingAuthorityWorkerReceipt {
    resolved_published_root: bool,
    base_frontier: u64,
    target_version: u64,
    observed_frontier: u64,
    authority_position: u64,
    authority_feed_envelopes: u64,
    object_reads: u64,
    base_tail_chain_valid: bool,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellServingTaggedTlogWorkerReceipt {
    resolved_published_root: bool,
    base_frontier: u64,
    target_version: u64,
    observed_frontier: u64,
    survivor_responses: u64,
    quorum_records: u64,
    object_reads: u64,
    base_tail_chain_valid: bool,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct EnvelopeSegment {
    format_version: u32,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    from_version: u64,
    through_version: u64,
    envelopes: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellRangeManifest {
    format_version: u32,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    range_start: Vec<u8>,
    range_end: Option<Vec<u8>>,
    covered_through: u64,
    children: Vec<PublicationObjectReference>,
}

struct BaseReplay {
    manifest: CellRangeManifest,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
    last_chain: [u8; 32],
    object_reads: u64,
    chain_valid: bool,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CellServingRecoveryMode) -> Result<Self, String> {
        Self::new_with_label(seed, mode.id())
    }

    fn new_with_label(seed: u64, label: &str) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "okv-cell-serving-recovery-{}-{seed}-{}",
            label,
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        Ok(Self(root))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-cell-serving-recovery-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Execute one bounded recovery from an object base at `O` plus a retained WAL
/// suffix through `T` in a fresh serving process.
///
/// # Errors
///
/// Returns an error when the process or storage fixtures cannot execute. Any
/// semantic disagreement is retained in the returned report.
pub fn run_cell_serving_recovery_contract(
    seed: u64,
    mode: CellServingRecoveryMode,
    executable: &Path,
) -> Result<CellServingRecoveryReport, String> {
    let transaction = run_cell_process_prototype(
        seed,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_controller(seed, mode, executable, transaction))
}

/// Execute RFC-0037 against a live replicated transaction authority.
///
/// # Errors
///
/// Returns an error when a process, authority, or object fixture cannot execute.
/// Semantic disagreements remain in the returned report.
pub fn run_cell_serving_authority_feed_contract(
    seed: u64,
    mode: CellServingAuthorityFeedMode,
    executable: &Path,
) -> Result<CellServingAuthorityFeedReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let mut transaction = CellProcessFixture::start(
            seed,
            CellProcessPrototypeMode::DurableSnapshotPop,
            executable,
        )?;
        let report = transaction.run_history().await?;
        run_authority_feed_controller(seed, mode, executable, &mut transaction, report).await
    })
}

/// Execute RFC-0038 against three dedicated tagged-log processes.
///
/// # Errors
///
/// Returns an error when a process, authority, object, or tLog fixture cannot
/// execute. Semantic disagreements remain in the returned report.
pub fn run_cell_serving_tagged_tlog_contract(
    seed: u64,
    mode: CellServingTaggedTlogMode,
    executable: &Path,
) -> Result<CellServingTaggedTlogReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let mut transaction = CellProcessFixture::start(
            seed,
            CellProcessPrototypeMode::DurableSnapshotPop,
            executable,
        )?;
        let report = transaction.run_history().await?;
        run_tagged_tlog_controller(seed, mode, executable, &mut transaction, report).await
    })
}

#[allow(clippy::too_many_lines)]
async fn run_tagged_tlog_controller(
    seed: u64,
    mode: CellServingTaggedTlogMode,
    executable: &Path,
    transaction_fixture: &mut CellProcessFixture<'_>,
    transaction: okv_consensus::CellProcessPrototypeReport,
) -> Result<CellServingTaggedTlogReport, String> {
    let root = TempRoot::new_with_label(seed, mode.id())?;
    let object_root = root.0.join("object-store");
    let final_cell = transaction
        .final_cell
        .clone()
        .ok_or_else(|| "source transaction omitted final cell state".to_owned())?;
    let commit_frontier = final_cell.latest_sequence;
    let object_frontier = transaction
        .authority_snapshot_frontier
        .ok_or_else(|| "source transaction omitted authority snapshot frontier".to_owned())?;
    let target_version = commit_frontier;
    let mut base = Vec::new();
    for encoded in &final_cell.committed_envelopes {
        let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
        if envelope.version().sequence() <= object_frontier {
            base.push(encoded.clone());
        }
    }

    let request = CellCommittedEnvelopeRequest {
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        after_version: object_frontier,
        through_version: target_version,
    };
    let authority_feed = CellTransactionClient::new(transaction_fixture.endpoints())?
        .committed_envelopes(&request)
        .await?;
    let suffix = authority_feed.envelopes;

    let segment = EnvelopeSegment {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        from_version: first_envelope_version(&base)?,
        through_version: object_frontier,
        envelopes: base.clone(),
    };
    let segment_bytes = serde_json::to_vec(&segment).map_err(|error| error.to_string())?;
    let segment_ref = object_reference(PublicationObjectKind::Data, &segment_bytes);
    let manifest = CellRangeManifest {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        range_start: Vec::new(),
        range_end: None,
        covered_through: object_frontier,
        children: vec![segment_ref.clone()],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|error| error.to_string())?;
    let manifest_ref = object_reference(PublicationObjectKind::Manifest, &manifest_bytes);
    let writer =
        ObjectClient::new(filesystem_backend(&object_root).map_err(|error| error.to_string())?);
    writer
        .put_if_absent(&segment_ref.key, Bytes::from(segment_bytes))
        .await
        .map_err(|error| error.to_string())?;
    writer
        .put_if_absent(&manifest_ref.key, Bytes::from(manifest_bytes))
        .await
        .map_err(|error| error.to_string())?;
    let base_verified = replay_base(&writer, &manifest_ref).await.is_ok();

    let publication = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x544c_4f47_5241_4e47,
        PUBLICATION_CELL_ID,
        final_cell.generation,
        TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let publication_process_starts = publication.process_count() as u64;
    let publication_client = publication.client()?;
    let publication_id = format!("cell-serving-tagged-tlog-{seed}");
    let credential = GenerationCredential {
        generation: final_cell.generation,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let intent = PublicationIntent {
        object_keys: [segment_ref.key.clone(), manifest_ref.key.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        manifest: manifest_ref.clone(),
        destination_root: DESTINATION_ROOT.to_owned(),
        expected_prior_root: None,
    };
    let prepared = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 401),
            credential: credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent,
            },
        })
        .await?;
    let published = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 402),
            credential,
            action: PublicationAction::Publish {
                publication_id,
                destination_root: DESTINATION_ROOT.to_owned(),
                expected_prior_root: None,
                manifest: manifest_ref.clone(),
            },
        })
        .await?;
    let publication_state = publication_client.read().await?;
    let publication_root_exact =
        publication_state.roots.get(DESTINATION_ROOT) == Some(&manifest_ref);

    let tlog_root = root.0.join("tagged-tlog");
    let mut tlogs = TaggedLogProcessFixture::start(
        executable,
        &tlog_root,
        ASSIGNED_RANGE_TAG,
        TAGGED_TLOG_NODES,
        TAGGED_TLOG_RETAINED_BYTES_LIMIT,
        mode == CellServingTaggedTlogMode::OmitRequiredRangeTag,
    )?;
    let endpoints = tlogs.endpoints();
    let private_roots_distinct =
        tlogs.roots().iter().collect::<BTreeSet<_>>().len() == TAGGED_TLOG_NODES;
    let encoded = suffix
        .first()
        .cloned()
        .ok_or_else(|| "tagged-log source suffix is empty".to_owned())?;
    let envelope = CommitEnvelope::decode(&encoded).map_err(|error| error.to_string())?;
    let mut range_tags = envelope.required_log_tags().to_vec();
    if mode == CellServingTaggedTlogMode::OmitRequiredRangeTag {
        range_tags.retain(|tag| *tag != ASSIGNED_RANGE_TAG);
    }
    let required_tags_present = envelope
        .required_log_tags()
        .iter()
        .all(|tag| range_tags.contains(tag));
    let record = TaggedLogRecord::committed(1, range_tags, encoded.clone());
    let append_request = TaggedLogRequest::Append {
        record: record.clone(),
    };
    let append_acks = endpoints
        .iter()
        .filter(|endpoint| {
            matches!(
                tagged_log_request(endpoint, &append_request),
                Ok(TaggedLogResponse::Appended { position: 1, .. })
            )
        })
        .count();

    let status_before_probe = endpoints
        .iter()
        .map(|endpoint| tagged_log_request(endpoint, &TaggedLogRequest::Status))
        .collect::<Vec<_>>();
    let mut overflow_record = TaggedLogRecord::committed(2, vec![ASSIGNED_RANGE_TAG], encoded);
    overflow_record.padding = vec![
        0;
        usize::try_from(TAGGED_TLOG_RETAINED_BYTES_LIMIT).map_err(
            |_| "tagged-log retained-byte limit exceeds usize".to_owned()
        )?
    ];
    let overflow_request = TaggedLogRequest::Append {
        record: overflow_record,
    };
    let backpressure_rejections = endpoints
        .iter()
        .filter(|endpoint| {
            matches!(
                tagged_log_request(endpoint, &overflow_request),
                Ok(TaggedLogResponse::RetainedBytesLimit { .. })
            )
        })
        .count();
    let status_after_probe = endpoints
        .iter()
        .map(|endpoint| tagged_log_request(endpoint, &TaggedLogRequest::Status))
        .collect::<Vec<_>>();
    let retained_prefix_unchanged = status_before_probe == status_after_probe;

    let killed_tlog_process = usize::try_from(seed % TAGGED_TLOG_NODES as u64).unwrap_or(0);
    tlogs.kill(killed_tlog_process)?;
    let output_path = root.0.join("tagged-tlog-worker-output.json");
    let config = CellServingTaggedTlogWorkerProcessConfig {
        object_root,
        publication_endpoints: publication.endpoints(),
        tlog_endpoints: endpoints,
        tlog_quorum: TAGGED_TLOG_QUORUM,
        destination_root: DESTINATION_ROOT.to_owned(),
        object_frontier,
        target_version,
        assigned_range_tag: ASSIGNED_RANGE_TAG,
        output_path: output_path.clone(),
    };
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("cell-serving-tagged-tlog-worker-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start tagged-tlog serving worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "tagged-tlog serving worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellServingTaggedTlogWorkerReceipt =
        serde_json::from_slice(&fs::read(&output_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;

    let expected_suffix_envelopes = u64::try_from(suffix.len()).unwrap_or(u64::MAX);
    let checks: [CellServingTaggedTlogCheck; TAGGED_TLOG_EXPECTED_CHECKS] = [
        tagged_tlog_check("source_transaction_clean", transaction.anomaly_count == 0),
        tagged_tlog_check(
            "object_frontier_below_target",
            object_frontier > 0 && object_frontier < target_version,
        ),
        tagged_tlog_check("base_envelopes_present", !base.is_empty()),
        tagged_tlog_check("expected_suffix_present", expected_suffix_envelopes > 0),
        tagged_tlog_check(
            "authority_feed_matches_suffix",
            authority_feed.after_version == object_frontier
                && authority_feed.through_version == target_version,
        ),
        tagged_tlog_check("immutable_closure_verified", base_verified),
        tagged_tlog_check(
            "publication_prepared",
            prepared.status == PublicationCommandStatus::Accepted,
        ),
        tagged_tlog_check(
            "publication_committed",
            published.status == PublicationCommandStatus::Accepted,
        ),
        tagged_tlog_check("publication_root_exact", publication_root_exact),
        tagged_tlog_check("tlog_private_roots_distinct", private_roots_distinct),
        tagged_tlog_check("required_range_tags_present", required_tags_present),
        tagged_tlog_check(
            "tlog_append_quorum_durable",
            append_acks >= TAGGED_TLOG_QUORUM,
        ),
        tagged_tlog_check(
            "all_tlogs_retain_one_record",
            status_before_probe.iter().all(|status| {
                matches!(
                    status,
                    Ok(TaggedLogResponse::Ready {
                        last_position: 1,
                        ..
                    })
                )
            }),
        ),
        tagged_tlog_check(
            "retained_byte_limit_rejects",
            backpressure_rejections == TAGGED_TLOG_NODES,
        ),
        tagged_tlog_check("retained_prefix_unchanged", retained_prefix_unchanged),
        tagged_tlog_check(
            "one_tlog_process_killed",
            killed_tlog_process < TAGGED_TLOG_NODES,
        ),
        tagged_tlog_check("worker_process_started", output.status.success()),
        tagged_tlog_check(
            "worker_resolved_published_root",
            worker.resolved_published_root && worker.object_reads > 0,
        ),
        tagged_tlog_check(
            "worker_reads_two_survivors",
            worker.survivor_responses == TAGGED_TLOG_QUORUM as u64,
        ),
        tagged_tlog_check(
            "worker_reconstructs_quorum_suffix",
            worker.quorum_records == expected_suffix_envelopes,
        ),
        tagged_tlog_check("base_tail_chain_valid", worker.base_tail_chain_valid),
        tagged_tlog_check(
            "worker_reaches_target",
            worker.base_frontier == object_frontier
                && worker.target_version == target_version
                && worker.observed_frontier == target_version,
        ),
        tagged_tlog_check(
            "fresh_worker_reconstructs_exact_rows",
            worker.rows == final_cell.rows,
        ),
    ];
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-serving-tagged-tlog-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(commit_frontier.to_be_bytes());
    trace.update(object_frontier.to_be_bytes());
    trace.update(worker.observed_frontier.to_be_bytes());
    trace.update((append_acks as u64).to_be_bytes());
    trace.update((backpressure_rejections as u64).to_be_bytes());
    trace.update(worker.survivor_responses.to_be_bytes());
    trace.update(worker.quorum_records.to_be_bytes());
    trace.update((killed_tlog_process as u64).to_be_bytes());
    for (key, value) in &worker.rows {
        trace.update((key.len() as u64).to_be_bytes());
        trace.update(key);
        trace.update((value.len() as u64).to_be_bytes());
        trace.update(value);
    }
    for check in &checks {
        trace.update(check.id.as_bytes());
        trace.update([u8::from(check.passed)]);
    }
    Ok(CellServingTaggedTlogReport {
        seed,
        mode,
        question: "Can a fresh serving process reconstruct Database(T) from object state through O plus a quorum-matched range-tagged tLog suffix after one tLog process dies?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_process_fixture"
        } else {
            "no"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        commit_frontier,
        object_frontier,
        target_version,
        observed_frontier: worker.observed_frontier,
        assigned_range_tag: ASSIGNED_RANGE_TAG,
        expected_suffix_envelopes,
        tlog_append_acks: append_acks as u64,
        tlog_required_tags_present: required_tags_present,
        tlog_backpressure_rejections: backpressure_rejections as u64,
        tlog_survivor_responses: worker.survivor_responses,
        tlog_quorum_records: worker.quorum_records,
        transaction_process_starts: transaction.process_starts,
        publication_process_starts,
        tlog_process_starts: TAGGED_TLOG_NODES as u64,
        serving_worker_process_starts: 1,
        tlog_process_kills: 1,
        killed_tlog_process: killed_tlog_process as u64,
        object_puts: 2,
        object_reads: worker.object_reads,
        reconstructed_rows: worker.rows,
        base_tail_chain_valid: worker.base_tail_chain_valid,
        checks: checks.into_iter().collect(),
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

#[allow(clippy::too_many_lines)]
async fn run_controller(
    seed: u64,
    mode: CellServingRecoveryMode,
    executable: &Path,
    transaction: okv_consensus::CellProcessPrototypeReport,
) -> Result<CellServingRecoveryReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let object_root = root.0.join("object-store");
    let wal_root = root.0.join("retained-wal");
    let final_cell = transaction
        .final_cell
        .clone()
        .ok_or_else(|| "source transaction omitted final cell state".to_owned())?;
    let commit_frontier = final_cell.latest_sequence;
    let object_frontier = transaction
        .authority_snapshot_frontier
        .ok_or_else(|| "source transaction omitted authority snapshot frontier".to_owned())?;
    let target_version = commit_frontier;
    let mut base = Vec::new();
    let mut suffix = Vec::new();
    for encoded in &final_cell.committed_envelopes {
        let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
        if envelope.version().sequence() <= object_frontier {
            base.push(encoded.clone());
        } else if envelope.version().sequence() <= target_version {
            suffix.push(encoded.clone());
        }
    }

    let segment = EnvelopeSegment {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        from_version: first_envelope_version(&base)?,
        through_version: object_frontier,
        envelopes: base.clone(),
    };
    let segment_bytes = serde_json::to_vec(&segment).map_err(|error| error.to_string())?;
    let segment_ref = object_reference(PublicationObjectKind::Data, &segment_bytes);
    let manifest = CellRangeManifest {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        range_start: Vec::new(),
        range_end: None,
        covered_through: object_frontier,
        children: vec![segment_ref.clone()],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|error| error.to_string())?;
    let manifest_ref = object_reference(PublicationObjectKind::Manifest, &manifest_bytes);
    let writer =
        ObjectClient::new(filesystem_backend(&object_root).map_err(|error| error.to_string())?);
    writer
        .put_if_absent(&segment_ref.key, Bytes::from(segment_bytes))
        .await
        .map_err(|error| error.to_string())?;
    writer
        .put_if_absent(&manifest_ref.key, Bytes::from(manifest_bytes))
        .await
        .map_err(|error| error.to_string())?;
    let base_verified = replay_base(&writer, &manifest_ref).await.is_ok();

    let authority = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x5e12_71a6_5e12_71a6,
        PUBLICATION_CELL_ID,
        final_cell.generation,
        TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let publication_process_starts = authority.process_count() as u64;
    let publication_client = authority.client()?;
    let publication_id = format!("cell-serving-recovery-{seed}");
    let credential = GenerationCredential {
        generation: final_cell.generation,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let intent = PublicationIntent {
        object_keys: [segment_ref.key.clone(), manifest_ref.key.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        manifest: manifest_ref.clone(),
        destination_root: DESTINATION_ROOT.to_owned(),
        expected_prior_root: None,
    };
    let prepared = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 201),
            credential: credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent,
            },
        })
        .await?;
    let published = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 202),
            credential,
            action: PublicationAction::Publish {
                publication_id,
                destination_root: DESTINATION_ROOT.to_owned(),
                expected_prior_root: None,
                manifest: manifest_ref.clone(),
            },
        })
        .await?;
    let authority_state = publication_client.read().await?;
    let publication_root_exact = authority_state.roots.get(DESTINATION_ROOT) == Some(&manifest_ref);

    let wal = LocalReplicatedWal::open(&wal_root, 3, 2).map_err(|error| error.to_string())?;
    let mut suffix_quorum_durable = true;
    for (offset, encoded) in suffix.iter().enumerate() {
        let log_index = u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1);
        let outcome = wal
            .append(log_index, encoded, &[0, 1, 2])
            .map_err(|error| error.to_string())?;
        suffix_quorum_durable &= outcome.quorum_durable;
    }

    let output_path = root.0.join("worker-output.json");
    let config = CellServingWorkerProcessConfig {
        object_root,
        publication_endpoints: authority.endpoints(),
        destination_root: DESTINATION_ROOT.to_owned(),
        wal_root,
        wal_replicas: 3,
        wal_quorum: 2,
        target_version,
        ignore_retained_suffix: mode == CellServingRecoveryMode::IgnoreRetainedSuffix,
        output_path: output_path.clone(),
    };
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("cell-serving-worker-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start serving worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "serving worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellServingWorkerReceipt =
        serde_json::from_slice(&fs::read(&output_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;

    let base_envelopes = u64::try_from(base.len()).unwrap_or(u64::MAX);
    let suffix_envelopes = u64::try_from(suffix.len()).unwrap_or(u64::MAX);
    let checks: [CellServingRecoveryCheck; EXPECTED_CHECKS] = [
        check("source_transaction_clean", transaction.anomaly_count == 0),
        check(
            "object_frontier_below_target",
            object_frontier > 0 && object_frontier < target_version,
        ),
        check("base_envelopes_present", base_envelopes > 0),
        check("retained_suffix_present", suffix_envelopes > 0),
        check("immutable_closure_verified", base_verified),
        check("suffix_quorum_durable", suffix_quorum_durable),
        check(
            "publication_prepared",
            prepared.status == PublicationCommandStatus::Accepted,
        ),
        check(
            "publication_committed",
            published.status == PublicationCommandStatus::Accepted,
        ),
        check("publication_root_exact", publication_root_exact),
        check("worker_process_started", output.status.success()),
        check(
            "worker_resolved_published_root",
            worker.resolved_published_root && worker.object_reads > 0,
        ),
        check(
            "worker_recovered_expected_suffix",
            worker.suffix_records_recovered == suffix_envelopes,
        ),
        check("base_tail_chain_valid", worker.base_tail_chain_valid),
        check(
            "worker_reaches_target",
            worker.base_frontier == object_frontier
                && worker.target_version == target_version
                && worker.observed_frontier == target_version,
        ),
        check(
            "fresh_worker_reconstructs_exact_rows",
            worker.rows == final_cell.rows,
        ),
    ];
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-serving-recovery-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(commit_frontier.to_be_bytes());
    trace.update(object_frontier.to_be_bytes());
    trace.update(worker.observed_frontier.to_be_bytes());
    trace.update(base_envelopes.to_be_bytes());
    trace.update(suffix_envelopes.to_be_bytes());
    trace.update(worker.suffix_records_recovered.to_be_bytes());
    for (key, value) in &worker.rows {
        trace.update((key.len() as u64).to_be_bytes());
        trace.update(key);
        trace.update((value.len() as u64).to_be_bytes());
        trace.update(value);
    }
    for check in &checks {
        trace.update(check.id.as_bytes());
        trace.update([u8::from(check.passed)]);
    }
    Ok(CellServingRecoveryReport {
        seed,
        mode,
        question: "Can a fresh serving process reconstruct Database(T) from an object base through O plus a quorum-recovered retained suffix (O,T]?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_process_fixture"
        } else {
            "no"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        commit_frontier,
        object_frontier,
        target_version,
        observed_frontier: worker.observed_frontier,
        base_envelopes,
        suffix_envelopes,
        suffix_records_recovered: worker.suffix_records_recovered,
        transaction_process_starts: transaction.process_starts,
        publication_process_starts,
        serving_worker_process_starts: 1,
        process_kills: transaction.process_kills,
        object_puts: 2,
        object_reads: worker.object_reads,
        reconstructed_rows: worker.rows,
        base_tail_chain_valid: worker.base_tail_chain_valid,
        checks: checks.into_iter().collect(),
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

#[allow(clippy::too_many_lines)]
async fn run_authority_feed_controller(
    seed: u64,
    mode: CellServingAuthorityFeedMode,
    executable: &Path,
    transaction_fixture: &mut CellProcessFixture<'_>,
    transaction: okv_consensus::CellProcessPrototypeReport,
) -> Result<CellServingAuthorityFeedReport, String> {
    let root = TempRoot::new_with_label(seed, mode.id())?;
    let object_root = root.0.join("object-store");
    let final_cell = transaction
        .final_cell
        .clone()
        .ok_or_else(|| "source transaction omitted final cell state".to_owned())?;
    let commit_frontier = final_cell.latest_sequence;
    let object_frontier = transaction
        .authority_snapshot_frontier
        .ok_or_else(|| "source transaction omitted authority snapshot frontier".to_owned())?;
    let target_version = commit_frontier;
    let mut base = Vec::new();
    let mut suffix = Vec::new();
    for encoded in &final_cell.committed_envelopes {
        let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
        if envelope.version().sequence() <= object_frontier {
            base.push(encoded.clone());
        } else if envelope.version().sequence() <= target_version {
            suffix.push(encoded.clone());
        }
    }

    let segment = EnvelopeSegment {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        from_version: first_envelope_version(&base)?,
        through_version: object_frontier,
        envelopes: base.clone(),
    };
    let segment_bytes = serde_json::to_vec(&segment).map_err(|error| error.to_string())?;
    let segment_ref = object_reference(PublicationObjectKind::Data, &segment_bytes);
    let manifest = CellRangeManifest {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        range_start: Vec::new(),
        range_end: None,
        covered_through: object_frontier,
        children: vec![segment_ref.clone()],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|error| error.to_string())?;
    let manifest_ref = object_reference(PublicationObjectKind::Manifest, &manifest_bytes);
    let writer =
        ObjectClient::new(filesystem_backend(&object_root).map_err(|error| error.to_string())?);
    writer
        .put_if_absent(&segment_ref.key, Bytes::from(segment_bytes))
        .await
        .map_err(|error| error.to_string())?;
    writer
        .put_if_absent(&manifest_ref.key, Bytes::from(manifest_bytes))
        .await
        .map_err(|error| error.to_string())?;
    let base_verified = replay_base(&writer, &manifest_ref).await.is_ok();

    let authority = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x4155_5448_4645_4544,
        PUBLICATION_CELL_ID,
        final_cell.generation,
        TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let publication_process_starts = authority.process_count() as u64;
    let publication_client = authority.client()?;
    let publication_id = format!("cell-serving-authority-feed-{seed}");
    let credential = GenerationCredential {
        generation: final_cell.generation,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let intent = PublicationIntent {
        object_keys: [segment_ref.key.clone(), manifest_ref.key.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        manifest: manifest_ref.clone(),
        destination_root: DESTINATION_ROOT.to_owned(),
        expected_prior_root: None,
    };
    let prepared = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 301),
            credential: credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent,
            },
        })
        .await?;
    let published = publication_client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 302),
            credential,
            action: PublicationAction::Publish {
                publication_id,
                destination_root: DESTINATION_ROOT.to_owned(),
                expected_prior_root: None,
                manifest: manifest_ref.clone(),
            },
        })
        .await?;
    let authority_state = publication_client.read().await?;
    let publication_root_exact = authority_state.roots.get(DESTINATION_ROOT) == Some(&manifest_ref);

    let handoff = transaction_fixture
        .kill_leader_and_elect_successor()
        .await?;
    let output_path = root.0.join("authority-worker-output.json");
    let config = CellServingAuthorityWorkerProcessConfig {
        object_root,
        publication_endpoints: authority.endpoints(),
        transaction_endpoints: transaction_fixture.endpoints(),
        destination_root: DESTINATION_ROOT.to_owned(),
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        object_frontier,
        target_version,
        drop_final_envelope: mode == CellServingAuthorityFeedMode::DropFinalEnvelope,
        output_path: output_path.clone(),
    };
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("cell-serving-authority-worker-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start authority serving worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "authority serving worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellServingAuthorityWorkerReceipt =
        serde_json::from_slice(&fs::read(&output_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;

    let expected_suffix_envelopes = u64::try_from(suffix.len()).unwrap_or(u64::MAX);
    let copied_wal_directories = u64::from(root.0.join("retained-wal").exists());
    let checks: [CellServingAuthorityFeedCheck; AUTHORITY_FEED_EXPECTED_CHECKS] = [
        authority_check("source_transaction_clean", transaction.anomaly_count == 0),
        authority_check(
            "object_frontier_below_target",
            object_frontier > 0 && object_frontier < target_version,
        ),
        authority_check("base_envelopes_present", !base.is_empty()),
        authority_check("expected_suffix_present", expected_suffix_envelopes > 0),
        authority_check("no_copied_wal_directory", copied_wal_directories == 0),
        authority_check("immutable_closure_verified", base_verified),
        authority_check(
            "publication_prepared",
            prepared.status == PublicationCommandStatus::Accepted,
        ),
        authority_check(
            "publication_committed",
            published.status == PublicationCommandStatus::Accepted,
        ),
        authority_check("publication_root_exact", publication_root_exact),
        authority_check(
            "transaction_leader_handoff",
            handoff.killed_leader != handoff.successor,
        ),
        authority_check("worker_process_started", output.status.success()),
        authority_check(
            "worker_resolved_published_root",
            worker.resolved_published_root && worker.object_reads > 0,
        ),
        authority_check(
            "worker_read_linearizable_authority_feed",
            worker.authority_position >= target_version
                && worker.authority_feed_envelopes == expected_suffix_envelopes,
        ),
        authority_check("base_tail_chain_valid", worker.base_tail_chain_valid),
        authority_check(
            "worker_reaches_target",
            worker.base_frontier == object_frontier
                && worker.target_version == target_version
                && worker.observed_frontier == target_version,
        ),
        authority_check(
            "fresh_worker_reconstructs_exact_rows",
            worker.rows == final_cell.rows,
        ),
    ];
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-serving-authority-feed-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(commit_frontier.to_be_bytes());
    trace.update(object_frontier.to_be_bytes());
    trace.update(worker.observed_frontier.to_be_bytes());
    trace.update(worker.authority_position.to_be_bytes());
    trace.update(worker.authority_feed_envelopes.to_be_bytes());
    trace.update(handoff.killed_leader.to_be_bytes());
    trace.update(handoff.successor.to_be_bytes());
    for (key, value) in &worker.rows {
        trace.update((key.len() as u64).to_be_bytes());
        trace.update(key);
        trace.update((value.len() as u64).to_be_bytes());
        trace.update(value);
    }
    for check in &checks {
        trace.update(check.id.as_bytes());
        trace.update([u8::from(check.passed)]);
    }
    Ok(CellServingAuthorityFeedReport {
        seed,
        mode,
        question: "Can a fresh serving process reconstruct Database(T) from object state through O plus a committed-envelope feed read directly from the live replicated transaction authority?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_process_fixture"
        } else {
            "no"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        commit_frontier,
        object_frontier,
        target_version,
        observed_frontier: worker.observed_frontier,
        authority_position: worker.authority_position,
        authority_feed_envelopes: worker.authority_feed_envelopes,
        expected_suffix_envelopes,
        transaction_process_starts: transaction.process_starts,
        publication_process_starts,
        serving_worker_process_starts: 1,
        transaction_leader_kills: 1,
        copied_wal_directories,
        object_puts: 2,
        object_reads: worker.object_reads,
        killed_transaction_leader: handoff.killed_leader,
        successor_transaction_leader: handoff.successor,
        reconstructed_rows: worker.rows,
        base_tail_chain_valid: worker.base_tail_chain_valid,
        checks: checks.into_iter().collect(),
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

/// Execute one disposable serving-worker process and persist its exact receipt.
///
/// # Errors
///
/// Returns an error when authority, object, WAL, or receipt I/O is invalid.
pub async fn run_cell_serving_worker_process(
    config: CellServingWorkerProcessConfig,
) -> Result<(), String> {
    let publication = PublicationClient::new(config.publication_endpoints.clone())?;
    let authority = publication.read().await?;
    let root = authority
        .roots
        .get(&config.destination_root)
        .cloned()
        .ok_or_else(|| "serving worker could not resolve published root".to_owned())?;
    let client = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );
    let base = replay_base(&client, &root).await?;
    let mut rows = base.rows;
    let mut previous_chain = base.last_chain;
    let mut observed_frontier = base.manifest.covered_through;
    let mut chain_valid = base.chain_valid;
    let recovery =
        LocalReplicatedWal::open(&config.wal_root, config.wal_replicas, config.wal_quorum)
            .map_err(|error| error.to_string())?
            .recover()
            .map_err(|error| error.to_string())?;
    let records = if config.ignore_retained_suffix {
        &[][..]
    } else {
        recovery.records.as_slice()
    };
    for record in records {
        let envelope =
            CommitEnvelope::decode(&record.payload).map_err(|error| error.to_string())?;
        let version = envelope.version().sequence();
        if envelope.previous_log_chain() != previous_chain
            || version <= observed_frontier
            || version > config.target_version
        {
            chain_valid = false;
            break;
        }
        apply_envelope(&mut rows, &envelope)?;
        previous_chain = Sha256::digest(&record.payload).into();
        observed_frontier = version;
    }
    let receipt = CellServingWorkerReceipt {
        resolved_published_root: true,
        base_frontier: base.manifest.covered_through,
        target_version: config.target_version,
        observed_frontier,
        suffix_records_recovered: u64::try_from(records.len()).unwrap_or(u64::MAX),
        object_reads: base.object_reads,
        base_tail_chain_valid: chain_valid,
        rows: rows.into_iter().collect(),
    };
    persist_receipt(&config.output_path, &receipt)
}

/// Execute one disposable worker that reads its suffix from live authority.
///
/// # Errors
///
/// Returns an error when publication, object, authority, or receipt I/O fails.
pub async fn run_cell_serving_authority_worker_process(
    config: CellServingAuthorityWorkerProcessConfig,
) -> Result<(), String> {
    let publication = PublicationClient::new(config.publication_endpoints.clone())?;
    let publication_state = publication.read().await?;
    let root = publication_state
        .roots
        .get(&config.destination_root)
        .cloned()
        .ok_or_else(|| "authority worker could not resolve published root".to_owned())?;
    let object = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );
    let base = replay_base(&object, &root).await?;
    if base.manifest.covered_through != config.object_frontier {
        return Err("authority worker base frontier differs from request".to_owned());
    }
    let request = CellCommittedEnvelopeRequest {
        cell_id: config.cell_id,
        tenant_id: config.tenant_id,
        generation: config.generation,
        after_version: config.object_frontier,
        through_version: config.target_version,
    };
    let feed = CellTransactionClient::new(config.transaction_endpoints.clone())?
        .committed_envelopes(&request)
        .await?;
    let mut envelopes = feed.envelopes;
    if config.drop_final_envelope {
        envelopes.pop();
    }
    let mut rows = base.rows;
    let mut previous_chain = base.last_chain;
    let mut observed_frontier = base.manifest.covered_through;
    let mut chain_valid = base.chain_valid;
    for encoded in &envelopes {
        let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
        let version = envelope.version().sequence();
        if envelope.previous_log_chain() != previous_chain
            || version <= observed_frontier
            || version > config.target_version
        {
            chain_valid = false;
            break;
        }
        apply_envelope(&mut rows, &envelope)?;
        previous_chain = Sha256::digest(encoded).into();
        observed_frontier = version;
    }
    let receipt = CellServingAuthorityWorkerReceipt {
        resolved_published_root: true,
        base_frontier: base.manifest.covered_through,
        target_version: config.target_version,
        observed_frontier,
        authority_position: feed.authority_position.index,
        authority_feed_envelopes: u64::try_from(envelopes.len()).unwrap_or(u64::MAX),
        object_reads: base.object_reads,
        base_tail_chain_valid: chain_valid,
        rows: rows.into_iter().collect(),
    };
    persist_receipt(&config.output_path, &receipt)
}

/// Execute one disposable worker that reads its suffix from dedicated tLogs.
///
/// # Errors
///
/// Returns an error when publication, object, tLog, or receipt I/O fails.
pub async fn run_cell_serving_tagged_tlog_worker_process(
    config: CellServingTaggedTlogWorkerProcessConfig,
) -> Result<(), String> {
    let publication = PublicationClient::new(config.publication_endpoints.clone())?;
    let publication_state = publication.read().await?;
    let root = publication_state
        .roots
        .get(&config.destination_root)
        .cloned()
        .ok_or_else(|| "tagged-tlog worker could not resolve published root".to_owned())?;
    let object = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );
    let base = replay_base(&object, &root).await?;
    if base.manifest.covered_through != config.object_frontier {
        return Err("tagged-tlog worker base frontier differs from request".to_owned());
    }
    let request = TaggedLogRequest::Read {
        range_tag: config.assigned_range_tag,
        after_version: config.object_frontier,
        through_version: config.target_version,
    };
    let mut survivor_responses = 0_u64;
    let mut candidates = BTreeMap::<u64, BTreeMap<String, (TaggedLogRecord, usize)>>::new();
    for endpoint in &config.tlog_endpoints {
        let Ok(TaggedLogResponse::Feed { records, .. }) = tagged_log_request(endpoint, &request)
        else {
            continue;
        };
        survivor_responses = survivor_responses.saturating_add(1);
        for record in records {
            let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            let digest = sha256(&bytes);
            let candidate = candidates
                .entry(record.position)
                .or_default()
                .entry(digest)
                .or_insert_with(|| (record, 0));
            candidate.1 = candidate.1.saturating_add(1);
        }
    }
    let mut quorum_records = Vec::new();
    for by_digest in candidates.into_values() {
        let mut matching = by_digest
            .into_values()
            .filter(|(_, count)| *count >= config.tlog_quorum);
        if let Some((record, _)) = matching.next() {
            if matching.next().is_some() {
                return Err("tagged-tlog worker observed conflicting quorums".to_owned());
            }
            quorum_records.push(record);
        }
    }
    quorum_records.sort_by_key(|record| record.position);

    let mut rows = base.rows;
    let mut previous_chain = base.last_chain;
    let mut observed_frontier = base.manifest.covered_through;
    let mut chain_valid = base.chain_valid;
    for record in &quorum_records {
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        let version = envelope.version().sequence();
        if envelope.previous_log_chain() != previous_chain
            || version <= observed_frontier
            || version > config.target_version
        {
            chain_valid = false;
            break;
        }
        apply_envelope(&mut rows, &envelope)?;
        previous_chain = Sha256::digest(&record.envelope).into();
        observed_frontier = version;
    }
    let receipt = CellServingTaggedTlogWorkerReceipt {
        resolved_published_root: true,
        base_frontier: base.manifest.covered_through,
        target_version: config.target_version,
        observed_frontier,
        survivor_responses,
        quorum_records: u64::try_from(quorum_records.len()).unwrap_or(u64::MAX),
        object_reads: base.object_reads,
        base_tail_chain_valid: chain_valid,
        rows: rows.into_iter().collect(),
    };
    persist_receipt(&config.output_path, &receipt)
}

async fn replay_base(
    client: &ObjectClient,
    root: &PublicationObjectReference,
) -> Result<BaseReplay, String> {
    let (manifest_bytes, _) = client
        .read_full_verified(&root.key, None, root.length, &root.sha256)
        .await
        .map_err(|error| error.to_string())?;
    let manifest: CellRangeManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| error.to_string())?;
    if manifest.format_version != FORMAT_VERSION || manifest.children.is_empty() {
        return Err("serving manifest is invalid or empty".to_owned());
    }
    let mut rows = BTreeMap::new();
    let mut previous_chain = [0_u8; 32];
    let mut object_reads = 1_u64;
    let mut last_version = 0_u64;
    let mut chain_valid = true;
    for child in &manifest.children {
        let (segment_bytes, _) = client
            .read_full_verified(&child.key, None, child.length, &child.sha256)
            .await
            .map_err(|error| error.to_string())?;
        object_reads = object_reads.saturating_add(1);
        let segment: EnvelopeSegment =
            serde_json::from_slice(&segment_bytes).map_err(|error| error.to_string())?;
        if segment.format_version != FORMAT_VERSION
            || segment.cell_id != manifest.cell_id
            || segment.tenant_id != manifest.tenant_id
            || segment.generation != manifest.generation
            || segment.through_version != manifest.covered_through
        {
            return Err("serving segment identity differs from manifest".to_owned());
        }
        for encoded in &segment.envelopes {
            let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
            let version = envelope.version().sequence();
            if envelope.previous_log_chain() != previous_chain
                || version <= last_version
                || version > manifest.covered_through
            {
                chain_valid = false;
                break;
            }
            apply_envelope(&mut rows, &envelope)?;
            previous_chain = Sha256::digest(encoded).into();
            last_version = version;
        }
    }
    Ok(BaseReplay {
        manifest,
        rows,
        last_chain: previous_chain,
        object_reads,
        chain_valid,
    })
}

fn apply_envelope(
    rows: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    envelope: &CommitEnvelope,
) -> Result<(), String> {
    let mutations: Vec<CellMutation> = serde_json::from_slice(envelope.canonical_mutations())
        .map_err(|error| error.to_string())?;
    for mutation in mutations {
        match mutation {
            CellMutation::Clear { key } => {
                rows.remove(&key);
            }
            CellMutation::Set { key, value } => {
                rows.insert(key, value);
            }
        }
    }
    Ok(())
}

fn first_envelope_version(envelopes: &[Vec<u8>]) -> Result<u64, String> {
    envelopes
        .first()
        .ok_or_else(|| "object base has no committed envelope".to_owned())
        .and_then(|encoded| {
            CommitEnvelope::decode(encoded)
                .map(|envelope| envelope.version().sequence())
                .map_err(|error| error.to_string())
        })
}

fn object_reference(kind: PublicationObjectKind, bytes: &[u8]) -> PublicationObjectReference {
    let digest = sha256(bytes);
    PublicationObjectReference {
        kind,
        key: format!("objects/sha256/{digest}"),
        length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: digest,
    }
}

fn check(id: &str, passed: bool) -> CellServingRecoveryCheck {
    CellServingRecoveryCheck {
        id: id.to_owned(),
        passed,
    }
}

fn authority_check(id: &str, passed: bool) -> CellServingAuthorityFeedCheck {
    CellServingAuthorityFeedCheck {
        id: id.to_owned(),
        passed,
    }
}

fn tagged_tlog_check(id: &str, passed: bool) -> CellServingTaggedTlogCheck {
    CellServingTaggedTlogCheck {
        id: id.to_owned(),
        passed,
    }
}

const fn request_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: (seed ^ 0x7365_7276_696e_672d) | 1,
        request_id,
    }
}

fn persist_receipt<T: Serialize>(path: &Path, receipt: &T) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "serving worker output path has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    fs::write(path, bytes).map_err(|error| error.to_string())?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}
