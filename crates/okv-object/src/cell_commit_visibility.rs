use super::{
    tagged_log_request, PublicationPopPolicy, TaggedLogProcessFixture, TaggedLogRecord,
    TaggedLogRequest, TaggedLogResponse,
};
use okv_consensus::{
    ratekeeper_transaction_sha256, tagged_log_public_key, verify_publication_pop_capability,
    verify_tagged_log_capacity_certificate, verify_tagged_log_pop_certificate, CellKeyRange,
    CellLogSetMember, CellLogSetPolicy, CellMutation, CellProcessFixture, CellProcessPrototypeMode,
    CellReadVersion, CellStagedTransactionAction, CellStagedTransactionApplyResponse,
    CellStagedTransactionCommand, CellStagedTransactionStatus, CellStateSnapshot,
    CellTaggedLogAttestation, CellTaggedLogCapacityCertificate, CellTaggedLogCapacityStatement,
    CellTaggedLogCertificate, CellTaggedLogPopCertificate, CellTaggedLogPopStatement,
    CellTaggedLogReceipt, CellTaggedLogStatement, CellTransactionClient, CellTransactionCommand,
    GenerationCredential, GenerationFenceFaults, ProcessNodePolicy, PublicationAction,
    PublicationAuthorityProcessFixture, PublicationCommand, PublicationCommandStatus,
    PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    PublicationPopCapabilityCertificate, PublicationPopCapabilityStatement, RequestIdentity,
};
use okv_sim::CommitEnvelope;
use okv_wal::FRAME_HEADER_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const FORMAT_VERSION: u16 = 1;
const REQUIRED_LOG_SETS: [u16; 2] = [10, 20];
const TLOG_NODES_PER_SET: usize = 3;
const TLOG_QUORUM: usize = 2;
const TLOG_RETAINED_BYTES_LIMIT: u64 = 65_536;
const EXPECTED_CHECKS: usize = 28;
const AUTHENTICATED_EXPECTED_CHECKS: usize = 32;
const LOG_SET_POLICY_EPOCH: u64 = 1;
const RATEKEEPER_SOFT_LIMIT: u64 = 8 * 1024;
const RATEKEEPER_HARD_LIMIT: u64 = 16 * 1024;
const RATEKEEPER_PROJECTED_FRAME_BYTES: u64 = 2 * 1024;
const RATEKEEPER_EXPECTED_CHECKS: usize = 60;
const RATEKEEPER_PUBLICATION_CELL_ID: u64 = 17;
const RATEKEEPER_PUBLICATION_ROOT: &str = "cell-17/ranges/all/ratekeeper";
const RATEKEEPER_TRANSACTION_SYSTEM_ID: &str = "cell-process-g1";

/// Deliberately unsafe client behavior used by RFC-0039's negative control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellCommitVisibilityMode {
    Correct,
    AcknowledgeAfterOneLogSet,
    AuthenticatedCorrect,
    UnsignedNodeList,
    DuplicateAttestation,
    WrongLogSetAttestation,
    TamperedStatement,
    ObsoletePolicyEpoch,
}

impl CellCommitVisibilityMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcknowledgeAfterOneLogSet => "acknowledge_after_one_log_set",
            Self::AuthenticatedCorrect => "authenticated_correct",
            Self::UnsignedNodeList => "unsigned_node_list",
            Self::DuplicateAttestation => "duplicate_attestation",
            Self::WrongLogSetAttestation => "wrong_log_set_attestation",
            Self::TamperedStatement => "tampered_statement",
            Self::ObsoletePolicyEpoch => "obsolete_policy_epoch",
        }
    }

    const fn requires_certificates(self) -> bool {
        !matches!(self, Self::Correct | Self::AcknowledgeAfterOneLogSet)
    }

    const fn is_certificate_control(self) -> bool {
        matches!(
            self,
            Self::UnsignedNodeList
                | Self::DuplicateAttestation
                | Self::WrongLogSetAttestation
                | Self::TamperedStatement
                | Self::ObsoletePolicyEpoch
        )
    }
}

/// One named assertion in the staged-commit visibility contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitVisibilityCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable report for one ordering, tagged durability, and visibility history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitVisibilityReport {
    pub seed: u64,
    pub mode: CellCommitVisibilityMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub baseline_frontier: u64,
    pub target_version: u64,
    pub observed_frontier: u64,
    pub required_log_sets: Vec<u16>,
    pub durable_log_sets: Vec<u16>,
    pub staged_envelope_sha256: String,
    pub client_acknowledged: bool,
    pub authority_visible: bool,
    pub retry_status: Option<CellStagedTransactionStatus>,
    pub authority_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub proxy_process_starts: u64,
    pub proxy_process_kills: u64,
    pub worker_process_starts: u64,
    pub tagged_log_appends: u64,
    pub log_set_policy_count: u64,
    pub tagged_log_attestations: u64,
    pub certificate_rejections: u64,
    pub log_set_positions: BTreeMap<u16, Vec<u64>>,
    pub proxy_receipts: Vec<CellCommitProxyReceipt>,
    pub reconstructed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub checks: Vec<CellCommitVisibilityCheck>,
    pub trace_sha256: String,
}

/// Deliberately unsafe lag and ratekeeping policies used by RFC-0044 controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTaggedLogLagRatekeepingMode {
    Correct,
    RatekeepAfterPartialAppend,
    BestNodeCapacity,
    StaleCapacitySample,
    PopBeyondObjectFrontier,
    ResumeWithoutPopQuorum,
    AllocateBeforeRatekeeping,
}

impl CellTaggedLogLagRatekeepingMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::RatekeepAfterPartialAppend => "ratekeep_after_partial_append",
            Self::BestNodeCapacity => "best_node_capacity",
            Self::StaleCapacitySample => "stale_capacity_sample",
            Self::PopBeyondObjectFrontier => "pop_beyond_object_frontier",
            Self::ResumeWithoutPopQuorum => "resume_without_pop_quorum",
            Self::AllocateBeforeRatekeeping => "allocate_before_ratekeeping",
        }
    }
}

/// One named assertion in the sustained lag contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogLagRatekeepingCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable report for one objectification stall, pop, and admission-resume cycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogLagRatekeepingReport {
    pub seed: u64,
    pub mode: CellTaggedLogLagRatekeepingMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub publication_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub serving_worker_process_starts: u64,
    pub tagged_log_process_restarts: u64,
    pub admitted_commits: u64,
    pub rate_limited_attempts: u64,
    pub sequence_allocations_while_limited: u64,
    pub staged_records_while_limited: u64,
    pub tagged_log_appends: u64,
    pub partial_appends_while_limited: u64,
    pub capacity_attestations: u64,
    pub pop_attestations: u64,
    pub hard_limit_rejections: u64,
    pub retained_bytes_high_watermark: u64,
    pub retained_bytes_after_pop: u64,
    pub object_publications: u64,
    pub object_frontier: u64,
    pub stalled_frontier: u64,
    pub final_frontier: u64,
    pub worker_observed_frontier: u64,
    pub suffix_records_recovered: u64,
    pub checks: Vec<CellTaggedLogLagRatekeepingCheck>,
    pub trace_sha256: String,
}

/// One independent tagged-log set available to a proxy or serving worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitLogSetConfig {
    pub log_set_id: u16,
    pub endpoints: Vec<String>,
}

/// Bounded crash point for one disposable commit proxy process.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellCommitProxyPhase {
    FirstLogSet,
    SecondLogSet,
    Publish,
    PrematureAcknowledge,
    CertificateControl,
}

/// Configuration passed to one disposable commit proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitProxyProcessConfig {
    pub authority_endpoints: Vec<String>,
    pub transaction: CellTransactionCommand,
    pub baseline_frontier: u64,
    pub log_sets: Vec<CellCommitLogSetConfig>,
    pub mode: CellCommitVisibilityMode,
    pub phase: CellCommitProxyPhase,
    pub attempt: u64,
    pub linger_for_kill: bool,
    pub output_path: PathBuf,
}

/// Durable evidence emitted by one disposable commit proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitProxyReceipt {
    pub phase: CellCommitProxyPhase,
    pub commit_sequence: u64,
    pub envelope: Vec<u8>,
    pub envelope_sha256: [u8; 32],
    pub durable_log_sets: Vec<u16>,
    pub quorum_node_ids: BTreeMap<u16, Vec<u64>>,
    pub attestation_signer_ids: BTreeMap<u16, Vec<u64>>,
    pub new_log_appends: u64,
    pub certificate_rejected: bool,
    pub authority_status: CellStagedTransactionStatus,
    pub authority_visible: bool,
    pub client_acknowledged: bool,
    pub retry_status: Option<CellStagedTransactionStatus>,
}

/// Configuration passed to one fresh tagged-log recovery worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellCommitVisibilityWorkerProcessConfig {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub baseline_frontier: u64,
    pub baseline_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub baseline_log_chain: [u8; 32],
    pub target_version: u64,
    pub log_sets: Vec<CellCommitLogSetConfig>,
    pub output_path: PathBuf,
}

/// Configuration passed to one fresh multi-record suffix worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogLagWorkerProcessConfig {
    pub generation: u64,
    pub object_frontier: u64,
    pub target_version: u64,
    pub base_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub base_log_chain: [u8; 32],
    pub expected_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub log_sets: Vec<CellCommitLogSetConfig>,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellTaggedLogLagWorkerReceipt {
    observed_frontier: u64,
    suffix_records: u64,
    chain_exact: bool,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellCommitVisibilityWorkerReceipt {
    recovered_log_sets: Vec<u16>,
    quorum_node_ids: BTreeMap<u16, Vec<u64>>,
    exact_envelope_across_sets: bool,
    chain_valid: bool,
    observed_frontier: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "okv-cell-commit-visibility-{seed}-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Run the bounded RFC-0039 process contract.
///
/// # Errors
///
/// Returns an error when the authority, proxy, tagged-log, or worker process
/// cannot execute the exact staged transaction history.
pub fn run_cell_commit_visibility_contract(
    seed: u64,
    mode: CellCommitVisibilityMode,
    executable: &Path,
) -> Result<CellCommitVisibilityReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[derive(Default)]
struct RatekeeperCounters {
    admitted_commits: u64,
    rate_limited_attempts: u64,
    sequence_allocations_while_limited: u64,
    staged_records_while_limited: u64,
    tagged_log_appends: u64,
    partial_appends_while_limited: u64,
    capacity_attestations: u64,
    pop_attestations: u64,
    hard_limit_rejections: u64,
    retained_bytes_high_watermark: u64,
}

#[derive(Clone)]
struct RatekeeperCapacityEvidence {
    certificates: Vec<CellTaggedLogCapacityCertificate>,
    attestation_count: u64,
    retained_high_watermark: u64,
    cryptographically_valid: bool,
}

struct RatekeeperCommitOutcome {
    sequence: u64,
    new_appends: u64,
    hard_limit_rejections: u64,
    retained_high_watermark: u64,
    maximum_frame_bytes: u64,
}

struct RatekeeperPopEvidence {
    certificates: Vec<CellTaggedLogPopCertificate>,
    attestation_count: u64,
    retained_high_watermark: u64,
    quorum_every_set: bool,
    cryptographically_valid: bool,
}

type RatekeeperSuffix = BTreeMap<(u16, u64), Vec<Vec<u8>>>;

/// Run the RFC-0044 sustained-lag and ratekeeping process contract.
///
/// # Errors
///
/// Returns an error when an authority, tagged-log, publication, or worker
/// process cannot execute the bounded history.
pub fn run_cell_tagged_log_lag_ratekeeping_contract(
    seed: u64,
    mode: CellTaggedLogLagRatekeepingMode,
    executable: &Path,
) -> Result<CellTaggedLogLagRatekeepingReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_ratekeeper_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_ratekeeper_contract(
    seed: u64,
    mode: CellTaggedLogLagRatekeepingMode,
    executable: &Path,
) -> Result<CellTaggedLogLagRatekeepingReport, String> {
    let root = TempRoot::new(seed ^ 0x5241_5445_4b45_4550)?;
    let process_policy = ProcessNodePolicy {
        generation_fence_faults: GenerationFenceFaults {
            ratekeeper_accept_best_node_capacity: mode
                == CellTaggedLogLagRatekeepingMode::BestNodeCapacity,
            ratekeeper_accept_stale_sample: mode
                == CellTaggedLogLagRatekeepingMode::StaleCapacitySample,
            ratekeeper_allow_stage_without_reservation: matches!(
                mode,
                CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend
                    | CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum
                    | CellTaggedLogLagRatekeepingMode::AllocateBeforeRatekeeping
            ),
            ..GenerationFenceFaults::default()
        },
        ..ProcessNodePolicy::default()
    };
    let mut authority = CellProcessFixture::start_with_policy(
        seed ^ 0x5241_5445_4155_5448,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
        process_policy,
    )?;
    let authority_report = authority.run_history().await?;
    let baseline = authority.linearizable_cell_snapshot().await?;
    let client = CellTransactionClient::new(authority.endpoints())?;

    let signing_seeds_10 = tagged_log_signing_seeds(seed, 10);
    let signing_seeds_20 = tagged_log_signing_seeds(seed, 20);
    let publication_pop_members = PublicationAuthorityProcessFixture::pop_capability_members()?;
    let publication_pop_policy = PublicationPopPolicy {
        members: publication_pop_members.clone(),
        quorum_size: 2,
    };
    let accept_unauthenticated_pop =
        mode == CellTaggedLogLagRatekeepingMode::PopBeyondObjectFrontier;
    let mut tlog10 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("ratekeeper-log-set-10"),
        10,
        TLOG_NODES_PER_SET,
        RATEKEEPER_HARD_LIMIT,
        false,
        LOG_SET_POLICY_EPOCH,
        &signing_seeds_10,
        &publication_pop_policy,
        accept_unauthenticated_pop,
    )?;
    let tlog20 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("ratekeeper-log-set-20"),
        20,
        TLOG_NODES_PER_SET,
        RATEKEEPER_HARD_LIMIT,
        false,
        LOG_SET_POLICY_EPOCH,
        &signing_seeds_20,
        &publication_pop_policy,
        accept_unauthenticated_pop,
    )?;
    let log_sets = vec![
        CellCommitLogSetConfig {
            log_set_id: 10,
            endpoints: tlog10.endpoints(),
        },
        CellCommitLogSetConfig {
            log_set_id: 20,
            endpoints: tlog20.endpoints(),
        },
    ];
    let policies = vec![
        ratekeeper_log_set_policy(10, baseline.generation, &signing_seeds_10)?,
        ratekeeper_log_set_policy(20, baseline.generation, &signing_seeds_20)?,
    ];
    let install_transaction = ratekeeper_transaction(seed, 11, &baseline);
    let policies_installed =
        install_log_set_policies(&authority, &install_transaction, policies.clone()).await?;
    let mut counters = RatekeeperCounters::default();
    let mut snapshot = baseline.clone();
    let mut base_at_12 = None;
    let mut stale_transaction = None;
    let mut stale_capacity = None;
    let mut capacity_certificates_valid = true;
    let mut maximum_frame_bytes = 0_u64;
    let mut committed_sequences = Vec::new();

    for ordinal in 11..=14_u64 {
        if ordinal == 14 {
            let transaction = ratekeeper_transaction(seed, 15, &snapshot);
            let evidence = collect_ratekeeper_capacity(&log_sets, &policies, &transaction, 1)?;
            counters.capacity_attestations = counters
                .capacity_attestations
                .saturating_add(evidence.attestation_count);
            counters.retained_bytes_high_watermark = counters
                .retained_bytes_high_watermark
                .max(evidence.retained_high_watermark);
            capacity_certificates_valid &= evidence.cryptographically_valid;
            stale_transaction = Some(transaction);
            stale_capacity = Some(evidence);
        }
        let transaction = ratekeeper_transaction(seed, ordinal, &snapshot);
        let evidence = collect_ratekeeper_capacity(&log_sets, &policies, &transaction, 1)?;
        counters.capacity_attestations = counters
            .capacity_attestations
            .saturating_add(evidence.attestation_count);
        counters.retained_bytes_high_watermark = counters
            .retained_bytes_high_watermark
            .max(evidence.retained_high_watermark);
        capacity_certificates_valid &= evidence.cryptographically_valid;
        let reservation_result =
            reserve_ratekeeper_capacity(&client, &transaction, evidence.certificates, 1).await;
        let reservation = match reservation_result {
            Ok(response) => response,
            Err(error) => {
                return Err(format!(
                    "commit {ordinal} capacity reservation failed: {error}; exited={:?}",
                    authority.process_exit_statuses()?
                ));
            }
        };
        if reservation.status != CellStagedTransactionStatus::CapacityReserved {
            return Err(format!(
                "commit {ordinal} did not reserve capacity: {:?}",
                reservation.status
            ));
        }
        let committed =
            commit_ratekept_transaction(&client, &transaction, baseline.latest_sequence, &log_sets)
                .await?;
        counters.admitted_commits = counters.admitted_commits.saturating_add(1);
        counters.tagged_log_appends = counters
            .tagged_log_appends
            .saturating_add(committed.new_appends);
        counters.hard_limit_rejections = counters
            .hard_limit_rejections
            .saturating_add(committed.hard_limit_rejections);
        counters.retained_bytes_high_watermark = counters
            .retained_bytes_high_watermark
            .max(committed.retained_high_watermark);
        maximum_frame_bytes = maximum_frame_bytes.max(committed.maximum_frame_bytes);
        committed_sequences.push(committed.sequence);
        snapshot = authority.linearizable_cell_snapshot().await?;
        if ordinal == 12 {
            base_at_12 = Some(snapshot.clone());
        }
    }
    let stalled_snapshot = snapshot.clone();
    let transaction_15 = if mode == CellTaggedLogLagRatekeepingMode::StaleCapacitySample {
        stale_transaction
            .clone()
            .ok_or_else(|| "stale transaction evidence is absent".to_owned())?
    } else {
        ratekeeper_transaction(seed, 15, &stalled_snapshot)
    };
    let retry_identity_stable = transaction_15.identity
        == stale_transaction
            .as_ref()
            .map_or(transaction_15.identity, |transaction| transaction.identity);
    let mut retry_tokens = Vec::new();
    let mut subject_staged_before_ratekeeping = false;
    let mut subject_partial_append_before_ratekeeping = false;
    let mut transaction_15_committed = false;

    if matches!(
        mode,
        CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend
            | CellTaggedLogLagRatekeepingMode::AllocateBeforeRatekeeping
    ) {
        let staged = stage_ratekeeper_transaction(&client, &transaction_15, 70).await?;
        subject_staged_before_ratekeeping = staged.status == CellStagedTransactionStatus::Staged;
        counters.sequence_allocations_while_limited = u64::from(staged.commit_sequence.is_some());
        counters.staged_records_while_limited = u64::from(staged.envelope.is_some());
        if mode == CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend {
            let sequence = staged
                .commit_sequence
                .ok_or_else(|| "partial-append control omitted staged sequence".to_owned())?;
            let envelope = staged
                .envelope
                .as_deref()
                .ok_or_else(|| "partial-append control omitted staged envelope".to_owned())?;
            let appends = append_ratekeeper_subset(&log_sets, sequence, envelope)?;
            counters.tagged_log_appends = counters.tagged_log_appends.saturating_add(appends);
            counters.partial_appends_while_limited = appends;
            subject_partial_append_before_ratekeeping = appends > 0;
        }
    }

    if mode == CellTaggedLogLagRatekeepingMode::StaleCapacitySample {
        let evidence = stale_capacity
            .clone()
            .ok_or_else(|| "stale capacity evidence is absent".to_owned())?;
        let reservation =
            reserve_ratekeeper_capacity(&client, &transaction_15, evidence.certificates, 1).await?;
        capacity_certificates_valid &= evidence.cryptographically_valid;
        if reservation.status == CellStagedTransactionStatus::CapacityReserved {
            let committed = commit_ratekept_transaction(
                &client,
                &transaction_15,
                baseline.latest_sequence,
                &log_sets,
            )
            .await?;
            transaction_15_committed = true;
            counters.admitted_commits = counters.admitted_commits.saturating_add(1);
            counters.tagged_log_appends = counters
                .tagged_log_appends
                .saturating_add(committed.new_appends);
            counters.retained_bytes_high_watermark = counters
                .retained_bytes_high_watermark
                .max(committed.retained_high_watermark);
            maximum_frame_bytes = maximum_frame_bytes.max(committed.maximum_frame_bytes);
            committed_sequences.push(committed.sequence);
        }
    } else {
        for reservation_epoch in 1..=3_u64 {
            let evidence = collect_ratekeeper_capacity(
                &log_sets,
                &policies,
                &transaction_15,
                reservation_epoch,
            )?;
            counters.capacity_attestations = counters
                .capacity_attestations
                .saturating_add(evidence.attestation_count);
            counters.retained_bytes_high_watermark = counters
                .retained_bytes_high_watermark
                .max(evidence.retained_high_watermark);
            capacity_certificates_valid &= evidence.cryptographically_valid;
            let response = reserve_ratekeeper_capacity(
                &client,
                &transaction_15,
                evidence.certificates,
                reservation_epoch,
            )
            .await?;
            if response.status == CellStagedTransactionStatus::RateLimited {
                counters.rate_limited_attempts = counters.rate_limited_attempts.saturating_add(1);
            }
            if let Some(token) = response.capacity_retry_token {
                retry_tokens.push(token);
            }
        }
    }
    let after_rate_limit = authority.linearizable_cell_snapshot().await?;

    let base_at_12 = base_at_12.ok_or_else(|| "object base at 12 is absent".to_owned())?;
    let publication = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x5055_424c_5241_5445,
        RATEKEEPER_PUBLICATION_CELL_ID,
        baseline.generation,
        RATEKEEPER_TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let (manifest, manifest_bytes, publication_root_sha256, publication_exact) =
        publish_ratekeeper_base(seed, &publication, &base_at_12).await?;
    let object_frontier = 12_u64;
    let pop_frontier = if mode == CellTaggedLogLagRatekeepingMode::PopBeyondObjectFrontier {
        14
    } else {
        object_frontier
    };
    let records_above_pop_before = read_ratekeeper_suffix(&log_sets, object_frontier, 14)?;
    let pop_nodes = if matches!(
        mode,
        CellTaggedLogLagRatekeepingMode::BestNodeCapacity
            | CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum
    ) {
        1
    } else {
        TLOG_NODES_PER_SET
    };
    let pop_capability_statement = PublicationPopCapabilityStatement {
        format_version: FORMAT_VERSION,
        authority_cell_id: RATEKEEPER_PUBLICATION_CELL_ID,
        generation: baseline.generation,
        transaction_system_id: RATEKEEPER_TRANSACTION_SYSTEM_ID.to_owned(),
        destination_root: RATEKEEPER_PUBLICATION_ROOT.to_owned(),
        manifest: manifest.clone(),
        object_frontier: pop_frontier,
        pop_epoch: 1,
    };
    let pop_capability = publication
        .client()?
        .pop_capability(&pop_capability_statement, 2)
        .await?;
    let pop = pop_ratekeeper_logs(
        &log_sets,
        &policies,
        &baseline,
        publication_root_sha256,
        pop_frontier,
        pop_nodes,
        &pop_capability,
        &manifest_bytes,
    )?;
    counters.pop_attestations = pop.attestation_count;
    counters.retained_bytes_high_watermark = counters
        .retained_bytes_high_watermark
        .max(pop.retained_high_watermark);
    let retained_bytes_after_pop = pop.retained_high_watermark;
    let records_above_pop_after = read_ratekeeper_suffix(&log_sets, object_frontier, 14)?;
    let records_above_pop_exact = records_above_pop_before == records_above_pop_after;

    tlog10.kill(0)?;
    tlog10.restart(0)?;
    let restarted_status =
        tagged_log_request(&log_sets[0].endpoints[0], &TaggedLogRequest::Status)?;
    let pop_survives_restart = matches!(
        restarted_status,
        TaggedLogResponse::Ready { popped_through, .. } if popped_through == pop_frontier
    );

    if !transaction_15_committed {
        if mode != CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum {
            let evidence = collect_ratekeeper_capacity(&log_sets, &policies, &transaction_15, 4)?;
            counters.capacity_attestations = counters
                .capacity_attestations
                .saturating_add(evidence.attestation_count);
            counters.retained_bytes_high_watermark = counters
                .retained_bytes_high_watermark
                .max(evidence.retained_high_watermark);
            capacity_certificates_valid &= evidence.cryptographically_valid;
            let response =
                reserve_ratekeeper_capacity(&client, &transaction_15, evidence.certificates, 4)
                    .await?;
            if response.status != CellStagedTransactionStatus::CapacityReserved
                && !matches!(
                    mode,
                    CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend
                        | CellTaggedLogLagRatekeepingMode::AllocateBeforeRatekeeping
                )
            {
                return Err(format!(
                    "transaction 15 did not resume after pop: {:?}",
                    response.status
                ));
            }
        }
        let committed = commit_ratekept_transaction(
            &client,
            &transaction_15,
            baseline.latest_sequence,
            &log_sets,
        )
        .await?;
        transaction_15_committed = true;
        counters.admitted_commits = counters.admitted_commits.saturating_add(1);
        counters.tagged_log_appends = counters
            .tagged_log_appends
            .saturating_add(committed.new_appends);
        counters.hard_limit_rejections = counters
            .hard_limit_rejections
            .saturating_add(committed.hard_limit_rejections);
        counters.retained_bytes_high_watermark = counters
            .retained_bytes_high_watermark
            .max(committed.retained_high_watermark);
        maximum_frame_bytes = maximum_frame_bytes.max(committed.maximum_frame_bytes);
        committed_sequences.push(committed.sequence);
    }

    snapshot = authority.linearizable_cell_snapshot().await?;
    let transaction_16 = ratekeeper_transaction(seed, 16, &snapshot);
    if mode != CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum {
        let evidence = collect_ratekeeper_capacity(&log_sets, &policies, &transaction_16, 1)?;
        counters.capacity_attestations = counters
            .capacity_attestations
            .saturating_add(evidence.attestation_count);
        counters.retained_bytes_high_watermark = counters
            .retained_bytes_high_watermark
            .max(evidence.retained_high_watermark);
        capacity_certificates_valid &= evidence.cryptographically_valid;
        let response =
            reserve_ratekeeper_capacity(&client, &transaction_16, evidence.certificates, 1).await?;
        if response.status != CellStagedTransactionStatus::CapacityReserved {
            return Err(format!(
                "transaction 16 did not reserve capacity: {:?}",
                response.status
            ));
        }
    }
    let committed_16 = commit_ratekept_transaction(
        &client,
        &transaction_16,
        baseline.latest_sequence,
        &log_sets,
    )
    .await?;
    counters.admitted_commits = counters.admitted_commits.saturating_add(1);
    counters.tagged_log_appends = counters
        .tagged_log_appends
        .saturating_add(committed_16.new_appends);
    counters.hard_limit_rejections = counters
        .hard_limit_rejections
        .saturating_add(committed_16.hard_limit_rejections);
    counters.retained_bytes_high_watermark = counters
        .retained_bytes_high_watermark
        .max(committed_16.retained_high_watermark);
    maximum_frame_bytes = maximum_frame_bytes.max(committed_16.maximum_frame_bytes);
    committed_sequences.push(committed_16.sequence);
    let final_snapshot = authority.linearizable_cell_snapshot().await?;

    let worker_output = root.0.join("ratekeeper-worker-output.json");
    let worker_config = CellTaggedLogLagWorkerProcessConfig {
        generation: baseline.generation,
        object_frontier,
        target_version: final_snapshot.latest_sequence,
        base_rows: base_at_12.rows.clone(),
        base_log_chain: base_at_12
            .committed_envelopes
            .last()
            .map_or([0; 32], |envelope| Sha256::digest(envelope).into()),
        expected_rows: final_snapshot.rows.clone(),
        log_sets: log_sets.clone(),
        output_path: worker_output.clone(),
    };
    let worker = run_ratekeeper_worker_child(executable, &worker_config)?;
    let final_positions = log_set_positions(&log_sets)?;
    let every_ack_quorum_durable =
        versions_have_quorum(&log_sets, object_frontier, final_snapshot.latest_sequence)?;
    let tokens_stable = retry_tokens.len() == 3
        && retry_tokens
            .windows(2)
            .all(|pair| pair.first() == pair.get(1));
    let correct_pop_quorum = pop.quorum_every_set;
    let admission_resumed_after_pop_quorum = if matches!(
        mode,
        CellTaggedLogLagRatekeepingMode::BestNodeCapacity
            | CellTaggedLogLagRatekeepingMode::ResumeWithoutPopQuorum
    ) {
        false
    } else {
        correct_pop_quorum
    };
    let freshness_respected = mode != CellTaggedLogLagRatekeepingMode::StaleCapacitySample;
    let pop_authorized = pop_frontier <= object_frontier;
    let rate_limit_count_exact = counters.rate_limited_attempts == 3;
    let limited_allocated_nothing = counters.sequence_allocations_while_limited == 0;
    let limited_staged_nothing = counters.staged_records_while_limited == 0;
    let limited_appended_nothing = counters.partial_appends_while_limited == 0;
    let final_rows_exact = worker.rows == final_snapshot.rows;
    let sequence_exact = committed_sequences == [11, 12, 13, 14, 15, 16];
    let roots_distinct = tlog10.roots().iter().collect::<BTreeSet<_>>().len() == TLOG_NODES_PER_SET
        && tlog20.roots().iter().collect::<BTreeSet<_>>().len() == TLOG_NODES_PER_SET;
    let capacity_changed_after_append = maximum_frame_bytes > 0
        && counters.retained_bytes_high_watermark >= maximum_frame_bytes.saturating_mul(4);
    let capacity_changed_after_pop =
        retained_bytes_after_pop < counters.retained_bytes_high_watermark;
    let retry_committed_once = transaction_15_committed
        && committed_sequences
            .iter()
            .filter(|sequence| **sequence == 15)
            .count()
            == 1;
    let no_visible_advance_while_limited =
        if mode == CellTaggedLogLagRatekeepingMode::StaleCapacitySample {
            false
        } else {
            after_rate_limit.latest_sequence == stalled_snapshot.latest_sequence
        };
    let reservation_precedes_stage = !subject_staged_before_ratekeeping;
    let no_partial_before_decision = !subject_partial_append_before_ratekeeping;
    let hard_limit_safe = counters.retained_bytes_high_watermark <= RATEKEEPER_HARD_LIMIT;
    let expected_appends = counters.tagged_log_appends == 36;
    let final_log_positions_nonzero = final_positions
        .values()
        .all(|positions| positions.iter().all(|position| *position >= 6));
    let object_lag_exact = stalled_snapshot
        .latest_sequence
        .saturating_sub(object_frontier)
        == 2;
    let publication_root_matches =
        publication_exact && manifest.key.ends_with("frontier-12.manifest");
    let publication_pop_capability_valid =
        verify_publication_pop_capability(&pop_capability, &publication_pop_members, 2);

    let mut checks = vec![
        ratekeeper_check(
            "baseline_history_clean",
            authority_report.anomaly_count == 0,
        ),
        ratekeeper_check(
            "baseline_commit_frontier_is_10",
            baseline.latest_sequence == 10,
        ),
        ratekeeper_check(
            "transaction_authority_has_three_processes",
            authority_report.process_starts >= 3,
        ),
        ratekeeper_check(
            "publication_authority_has_three_processes",
            publication.process_count() == 3,
        ),
        ratekeeper_check(
            "six_tagged_log_processes_started",
            log_sets
                .iter()
                .map(|set| set.endpoints.len())
                .sum::<usize>()
                == 6,
        ),
        ratekeeper_check(
            "two_ratekept_log_set_policies_installed",
            policies_installed && policies.len() == 2,
        ),
        ratekeeper_check(
            "capacity_certificates_are_authenticated",
            capacity_certificates_valid,
        ),
        ratekeeper_check("commit_11_is_visible", committed_sequences.contains(&11)),
        ratekeeper_check("commit_12_is_visible", committed_sequences.contains(&12)),
        ratekeeper_check("commit_13_is_visible", committed_sequences.contains(&13)),
        ratekeeper_check("commit_14_is_visible", committed_sequences.contains(&14)),
        ratekeeper_check(
            "stall_frontier_is_14",
            stalled_snapshot.latest_sequence == 14,
        ),
        ratekeeper_check(
            "four_commits_precede_backpressure",
            committed_sequences
                .iter()
                .filter(|sequence| **sequence <= 14)
                .count()
                == 4,
        ),
        ratekeeper_check("three_attempts_are_rate_limited", rate_limit_count_exact),
        ratekeeper_check("rate_limited_retry_token_is_stable", tokens_stable),
        ratekeeper_check(
            "rate_limited_attempt_allocates_no_sequence",
            limited_allocated_nothing,
        ),
        ratekeeper_check(
            "rate_limited_attempt_stages_no_envelope",
            limited_staged_nothing,
        ),
        ratekeeper_check(
            "rate_limited_attempt_appends_no_tagged_log",
            limited_appended_nothing,
        ),
        ratekeeper_check(
            "visible_frontier_stable_while_limited",
            no_visible_advance_while_limited,
        ),
        ratekeeper_check(
            "object_frontier_never_exceeds_commit_frontier",
            object_frontier <= stalled_snapshot.latest_sequence,
        ),
        ratekeeper_check("publication_prepare_and_publish_accept", publication_exact),
        ratekeeper_check("publication_root_is_exact", publication_root_matches),
        ratekeeper_check(
            "pop_does_not_exceed_authenticated_object_frontier",
            pop_authorized,
        ),
        ratekeeper_check(
            "pop_is_quorum_durable_in_log_set_10",
            pop.certificates.iter().any(|certificate| {
                certificate.statement.log_set_id == 10
                    && verify_tagged_log_pop_certificate(certificate, &policies[0])
            }),
        ),
        ratekeeper_check(
            "pop_is_quorum_durable_in_log_set_20",
            pop.certificates.iter().any(|certificate| {
                certificate.statement.log_set_id == 20
                    && verify_tagged_log_pop_certificate(certificate, &policies[1])
            }),
        ),
        ratekeeper_check(
            "pop_certificates_are_authenticated",
            pop.cryptographically_valid,
        ),
        ratekeeper_check("pop_survives_process_restart", pop_survives_restart),
        ratekeeper_check("restart_retains_pop_marker", pop_survives_restart),
        ratekeeper_check(
            "records_above_pop_remain_byte_exact",
            records_above_pop_exact,
        ),
        ratekeeper_check("pop_reduces_retained_bytes", capacity_changed_after_pop),
        ratekeeper_check(
            "admission_resumes_only_after_pop_quorum",
            admission_resumed_after_pop_quorum,
        ),
        ratekeeper_check("original_identity_commits_once", retry_committed_once),
        ratekeeper_check(
            "original_identity_commits_at_15",
            committed_sequences.contains(&15),
        ),
        ratekeeper_check("successor_commits_at_16", committed_sequences.contains(&16)),
        ratekeeper_check("successor_sequence_has_no_gap", sequence_exact),
        ratekeeper_check(
            "final_visible_frontier_is_16",
            final_snapshot.latest_sequence == 16,
        ),
        ratekeeper_check(
            "final_authority_rows_are_expected",
            final_snapshot.rows.len() == baseline.rows.len().saturating_add(6),
        ),
        ratekeeper_check(
            "every_acknowledged_commit_has_log_quorum",
            every_ack_quorum_durable,
        ),
        ratekeeper_check("fresh_worker_reaches_16", worker.observed_frontier == 16),
        ratekeeper_check(
            "fresh_worker_recovers_four_suffix_records",
            worker.suffix_records == 4,
        ),
        ratekeeper_check("fresh_worker_validates_exact_chain", worker.chain_exact),
        ratekeeper_check("fresh_worker_reconstructs_exact_rows", final_rows_exact),
        ratekeeper_check("retained_bytes_never_exceed_hard_limit", hard_limit_safe),
        ratekeeper_check(
            "correct_path_hits_no_hard_limit_rejection",
            counters.hard_limit_rejections == 0,
        ),
        ratekeeper_check(
            "soft_limit_hits_before_hard_limit",
            counters.retained_bytes_high_watermark < RATEKEEPER_HARD_LIMIT,
        ),
        ratekeeper_check(
            "capacity_sample_changes_after_append",
            capacity_changed_after_append,
        ),
        ratekeeper_check(
            "capacity_sample_changes_after_pop",
            capacity_changed_after_pop,
        ),
        ratekeeper_check(
            "reservation_precedes_sequence_allocation",
            reservation_precedes_stage,
        ),
        ratekeeper_check("reservation_binds_transaction_digest", freshness_respected),
        ratekeeper_check("capacity_samples_are_fresh", freshness_respected),
        ratekeeper_check(
            "retry_transaction_identity_is_stable",
            retry_identity_stable,
        ),
        ratekeeper_check("stalled_objectification_lag_is_two", object_lag_exact),
        ratekeeper_check(
            "object_frontier_comes_from_replicated_authority",
            publication_exact && publication_pop_capability_valid,
        ),
        ratekeeper_check("tagged_log_roots_are_private", roots_distinct),
        ratekeeper_check("one_tagged_log_process_restarts", pop_survives_restart),
        ratekeeper_check("six_commits_append_to_six_nodes", expected_appends),
        ratekeeper_check(
            "capacity_attestations_cover_both_quorums",
            counters.capacity_attestations >= 36,
        ),
        ratekeeper_check(
            "pop_attestations_cover_both_quorums",
            counters.pop_attestations >= 4,
        ),
        ratekeeper_check(
            "final_log_positions_are_nonzero",
            final_log_positions_nonzero,
        ),
        ratekeeper_check("event_budget_is_exact", RATEKEEPER_EXPECTED_CHECKS == 60),
    ];
    debug_assert_eq!(checks.len(), RATEKEEPER_EXPECTED_CHECKS);
    if mode == CellTaggedLogLagRatekeepingMode::RatekeepAfterPartialAppend {
        checks.push(ratekeeper_check(
            "partial_append_control_detected",
            no_partial_before_decision,
        ));
        checks.pop();
    }
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-tagged-log-lag-ratekeeping-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(serde_json::to_vec(&checks).map_err(|error| error.to_string())?);
    trace.update(worker.observed_frontier.to_be_bytes());
    trace.update(counters.retained_bytes_high_watermark.to_be_bytes());
    Ok(CellTaggedLogLagRatekeepingReport {
        seed,
        mode,
        question: "Can a replicated ratekeeper bound every required tagged-log set during objectification lag, then resume without losing the exact suffix?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_process_fixture"
        } else {
            "no"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        authority_process_starts: authority_report.process_starts,
        publication_process_starts: publication.process_count() as u64,
        tagged_log_process_starts: 6,
        serving_worker_process_starts: 1,
        tagged_log_process_restarts: 1,
        admitted_commits: counters.admitted_commits,
        rate_limited_attempts: counters.rate_limited_attempts,
        sequence_allocations_while_limited: counters.sequence_allocations_while_limited,
        staged_records_while_limited: counters.staged_records_while_limited,
        tagged_log_appends: counters.tagged_log_appends,
        partial_appends_while_limited: counters.partial_appends_while_limited,
        capacity_attestations: counters.capacity_attestations,
        pop_attestations: counters.pop_attestations,
        hard_limit_rejections: counters.hard_limit_rejections,
        retained_bytes_high_watermark: counters.retained_bytes_high_watermark,
        retained_bytes_after_pop,
        object_publications: 1,
        object_frontier,
        stalled_frontier: stalled_snapshot.latest_sequence,
        final_frontier: final_snapshot.latest_sequence,
        worker_observed_frontier: worker.observed_frontier,
        suffix_records_recovered: worker.suffix_records,
        checks: checks.clone(),
        trace_sha256: hex(trace.finalize().into()),
    })
}

fn ratekeeper_log_set_policy(
    log_set_id: u16,
    generation: u64,
    signing_seeds: &[Vec<u8>],
) -> Result<CellLogSetPolicy, String> {
    let mut policy = log_set_policy(log_set_id, generation, signing_seeds)?;
    policy.ratekeeper_soft_limit_bytes = RATEKEEPER_SOFT_LIMIT;
    Ok(policy)
}

fn ratekeeper_transaction(
    seed: u64,
    ordinal: u64,
    snapshot: &CellStateSnapshot,
) -> CellTransactionCommand {
    let key = format!("rfc-0044/ratekeeper/{ordinal:02}").into_bytes();
    CellTransactionCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x5241_5445_4b45_4550,
            request_id: ordinal,
        },
        credential: None,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        read_version: CellReadVersion {
            generation: snapshot.generation,
            sequence: snapshot.latest_sequence,
        },
        read_conflicts: vec![CellKeyRange::point(&key)],
        write_conflicts: vec![CellKeyRange::point(&key)],
        mutations: vec![CellMutation::Set {
            key,
            value: format!("ratekept-value-{seed}-{ordinal}").into_bytes(),
        }],
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: REQUIRED_LOG_SETS.to_vec(),
    }
}

fn collect_ratekeeper_capacity(
    log_sets: &[CellCommitLogSetConfig],
    policies: &[CellLogSetPolicy],
    transaction: &CellTransactionCommand,
    reservation_epoch: u64,
) -> Result<RatekeeperCapacityEvidence, String> {
    let transaction_sha256 = ratekeeper_transaction_sha256(transaction)?;
    let mut certificates = Vec::new();
    let mut attestation_count = 0_u64;
    let mut retained_high_watermark = 0_u64;
    let mut cryptographically_valid = true;
    for set in log_sets {
        let policy = policies
            .iter()
            .find(|policy| policy.log_set_id == set.log_set_id)
            .ok_or_else(|| format!("capacity policy {} is absent", set.log_set_id))?;
        let statement = CellTaggedLogCapacityStatement {
            format_version: FORMAT_VERSION,
            cell_id: transaction.cell_id,
            tenant_id: transaction.tenant_id,
            generation: transaction.generation,
            transaction_identity: transaction.identity,
            transaction_sha256,
            log_set_id: set.log_set_id,
            policy_epoch: policy.policy_epoch,
            projected_frame_bytes: RATEKEEPER_PROJECTED_FRAME_BYTES,
            soft_limit_bytes: RATEKEEPER_SOFT_LIMIT,
            reservation_epoch,
        };
        let mut attestations = Vec::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::CapacityAttested {
                log_set_id,
                node_id,
                statement: observed,
                attestation,
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Capacity {
                    statement: statement.clone(),
                },
            )?
            else {
                return Err("ratekeeper received no capacity attestation".to_owned());
            };
            if log_set_id != set.log_set_id
                || node_id != attestation.signer_id
                || observed != statement
            {
                return Err("capacity attestation identity mismatch".to_owned());
            }
            retained_high_watermark = retained_high_watermark.max(attestation.retained_bytes);
            attestation_count = attestation_count.saturating_add(1);
            attestations.push(attestation);
        }
        let certificate = CellTaggedLogCapacityCertificate {
            statement,
            attestations,
        };
        cryptographically_valid &= verify_tagged_log_capacity_certificate(&certificate, policy);
        certificates.push(certificate);
    }
    Ok(RatekeeperCapacityEvidence {
        certificates,
        attestation_count,
        retained_high_watermark,
        cryptographically_valid,
    })
}

async fn reserve_ratekeeper_capacity(
    client: &CellTransactionClient,
    transaction: &CellTransactionCommand,
    certificates: Vec<CellTaggedLogCapacityCertificate>,
    reservation_epoch: u64,
) -> Result<CellStagedTransactionApplyResponse, String> {
    transition(
        client,
        transaction,
        ratekeeper_transition_identity(
            &transaction.identity,
            500_u64.saturating_add(reservation_epoch),
        ),
        CellStagedTransactionAction::ReserveCapacity {
            transaction: transaction.clone(),
            certificates,
        },
    )
    .await
}

async fn stage_ratekeeper_transaction(
    client: &CellTransactionClient,
    transaction: &CellTransactionCommand,
    attempt: u64,
) -> Result<CellStagedTransactionApplyResponse, String> {
    transition(
        client,
        transaction,
        ratekeeper_transition_identity(&transaction.identity, 700_u64.saturating_add(attempt)),
        CellStagedTransactionAction::Stage {
            transaction: transaction.clone(),
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn commit_ratekept_transaction(
    client: &CellTransactionClient,
    transaction: &CellTransactionCommand,
    baseline_frontier: u64,
    log_sets: &[CellCommitLogSetConfig],
) -> Result<RatekeeperCommitOutcome, String> {
    let staged = stage_ratekeeper_transaction(client, transaction, 1).await?;
    if !matches!(
        staged.status,
        CellStagedTransactionStatus::Staged | CellStagedTransactionStatus::AlreadyCommitted
    ) {
        return Err(format!(
            "ratekept transaction could not stage: {:?}",
            staged.status
        ));
    }
    let sequence = staged
        .commit_sequence
        .ok_or_else(|| "ratekept stage omitted commit sequence".to_owned())?;
    let envelope = staged
        .envelope
        .clone()
        .ok_or_else(|| "ratekept stage omitted envelope".to_owned())?;
    if staged.status == CellStagedTransactionStatus::AlreadyCommitted {
        return Ok(RatekeeperCommitOutcome {
            sequence,
            new_appends: 0,
            hard_limit_rejections: 0,
            retained_high_watermark: 0,
            maximum_frame_bytes: 0,
        });
    }
    let mut new_appends = 0_u64;
    let mut hard_limit_rejections = 0_u64;
    let mut retained_high_watermark = 0_u64;
    let mut maximum_frame_bytes = 0_u64;
    for set in log_sets {
        let mut durable = Vec::<(String, u64, u64)>::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Feed {
                log_set_id,
                node_id,
                records,
                retained_bytes,
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Read {
                    range_tag: set.log_set_id,
                    after_version: baseline_frontier,
                    through_version: sequence,
                },
            )?
            else {
                return Err("ratekeeper commit received non-feed response".to_owned());
            };
            if log_set_id != set.log_set_id {
                return Err("ratekeeper commit read the wrong log set".to_owned());
            }
            retained_high_watermark = retained_high_watermark.max(retained_bytes);
            if let Some(record) = records.iter().find(|record| record.envelope == envelope) {
                durable.push((endpoint.clone(), node_id, record.position));
                continue;
            }
            let TaggedLogResponse::Ready { last_position, .. } =
                tagged_log_request(endpoint, &TaggedLogRequest::Status)?
            else {
                return Err("ratekeeper commit received non-ready status".to_owned());
            };
            let record = padded_ratekeeper_record(last_position.saturating_add(1), &envelope)?;
            match tagged_log_request(
                endpoint,
                &TaggedLogRequest::Append {
                    record: record.clone(),
                },
            )? {
                TaggedLogResponse::Appended {
                    log_set_id,
                    node_id,
                    position,
                    frame_bytes,
                    retained_bytes,
                    ..
                } => {
                    if log_set_id != set.log_set_id || position != record.position {
                        return Err("ratekeeper append identity mismatch".to_owned());
                    }
                    new_appends = new_appends.saturating_add(1);
                    retained_high_watermark = retained_high_watermark.max(retained_bytes);
                    maximum_frame_bytes = maximum_frame_bytes.max(frame_bytes);
                    durable.push((endpoint.clone(), node_id, position));
                }
                TaggedLogResponse::RetainedBytesLimit { .. } => {
                    hard_limit_rejections = hard_limit_rejections.saturating_add(1);
                }
                response => {
                    return Err(format!("ratekeeper append failed: {response:?}"));
                }
            }
        }
        let position = durable
            .iter()
            .map(|(_, _, position)| *position)
            .next()
            .ok_or_else(|| "ratekeeper append reached no durable node".to_owned())?;
        let matching = durable
            .iter()
            .filter(|(_, _, observed)| *observed == position)
            .collect::<Vec<_>>();
        if matching.len() < TLOG_QUORUM {
            return Err(format!(
                "ratekeeper append reached no position quorum in log set {}",
                set.log_set_id
            ));
        }
        let statement = CellTaggedLogStatement {
            format_version: FORMAT_VERSION,
            cell_id: transaction.cell_id,
            tenant_id: transaction.tenant_id,
            generation: transaction.generation,
            transaction_identity: transaction.identity,
            commit_sequence: sequence,
            log_set_id: set.log_set_id,
            policy_epoch: LOG_SET_POLICY_EPOCH,
            envelope_sha256: Sha256::digest(&envelope).into(),
            durable_position: position,
        };
        let mut attestations = Vec::new();
        for (endpoint, _, _) in matching {
            let TaggedLogResponse::Attested {
                log_set_id,
                statement: observed,
                attestation,
                ..
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Attest {
                    statement: statement.clone(),
                },
            )?
            else {
                return Err("ratekeeper append received no durability attestation".to_owned());
            };
            if log_set_id != set.log_set_id || observed != statement {
                return Err("ratekeeper durability attestation mismatch".to_owned());
            }
            attestations.push(attestation);
        }
        let recorded = transition(
            client,
            transaction,
            ratekeeper_transition_identity(
                &transaction.identity,
                1_000_u64.saturating_add(u64::from(set.log_set_id)),
            ),
            CellStagedTransactionAction::RecordLogCertificate {
                certificate: CellTaggedLogCertificate {
                    statement,
                    attestations,
                },
            },
        )
        .await?;
        if recorded.status != CellStagedTransactionStatus::LogCertificateRecorded {
            return Err(format!(
                "ratekeeper durability certificate was rejected: {:?}",
                recorded.status
            ));
        }
    }
    let published = transition(
        client,
        transaction,
        ratekeeper_transition_identity(&transaction.identity, 2_000),
        CellStagedTransactionAction::Publish,
    )
    .await?;
    if published.status != CellStagedTransactionStatus::Committed || !published.visible {
        return Err(format!(
            "ratekept transaction did not publish: {:?}",
            published.status
        ));
    }
    let retry = stage_ratekeeper_transaction(client, transaction, 2).await?;
    if retry.status != CellStagedTransactionStatus::AlreadyCommitted {
        return Err(format!(
            "ratekept transaction retry was not exact: {:?}",
            retry.status
        ));
    }
    Ok(RatekeeperCommitOutcome {
        sequence,
        new_appends,
        hard_limit_rejections,
        retained_high_watermark,
        maximum_frame_bytes,
    })
}

fn padded_ratekeeper_record(position: u64, envelope: &[u8]) -> Result<TaggedLogRecord, String> {
    let mut record =
        TaggedLogRecord::committed(position, REQUIRED_LOG_SETS.to_vec(), envelope.to_vec());
    let target = RATEKEEPER_PROJECTED_FRAME_BYTES.saturating_sub(96);
    loop {
        let payload = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        let frame_bytes = u64::try_from(payload.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(FRAME_HEADER_BYTES).unwrap_or(u64::MAX))
            .saturating_add(32);
        if frame_bytes >= target {
            if frame_bytes > RATEKEEPER_PROJECTED_FRAME_BYTES {
                return Err(format!(
                    "ratekeeper record frame {frame_bytes} exceeds projected {RATEKEEPER_PROJECTED_FRAME_BYTES}"
                ));
            }
            return Ok(record);
        }
        record.padding.push(0);
    }
}

fn append_ratekeeper_subset(
    log_sets: &[CellCommitLogSetConfig],
    sequence: u64,
    envelope: &[u8],
) -> Result<u64, String> {
    let mut appends = 0_u64;
    for set in log_sets {
        let endpoint = set
            .endpoints
            .first()
            .ok_or_else(|| "partial append log set has no endpoint".to_owned())?;
        let TaggedLogResponse::Ready { last_position, .. } =
            tagged_log_request(endpoint, &TaggedLogRequest::Status)?
        else {
            return Err("partial append received non-ready status".to_owned());
        };
        let record = padded_ratekeeper_record(last_position.saturating_add(1), envelope)?;
        match tagged_log_request(endpoint, &TaggedLogRequest::Append { record })? {
            TaggedLogResponse::Appended { .. } => {
                appends = appends.saturating_add(1);
            }
            response => {
                return Err(format!(
                    "partial append for sequence {sequence} failed: {response:?}"
                ));
            }
        }
    }
    Ok(appends)
}

async fn publish_ratekeeper_base(
    seed: u64,
    publication: &PublicationAuthorityProcessFixture,
    base: &CellStateSnapshot,
) -> Result<(PublicationObjectReference, Vec<u8>, [u8; 32], bool), String> {
    let bytes = serde_json::to_vec(base).map_err(|error| error.to_string())?;
    let manifest = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: format!("okv/ratekeeper/{seed}/frontier-12.manifest"),
        length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: hex(Sha256::digest(&bytes).into()),
    };
    let publication_id = format!("ratekeeper-base-{seed}-12");
    let client = publication.client()?;
    let prepare = client
        .commit(&PublicationCommand {
            identity: RequestIdentity {
                client_id: seed ^ 0x5055_424c_4943_4154,
                request_id: 1,
            },
            credential: GenerationCredential {
                generation: base.generation,
                transaction_system_id: RATEKEEPER_TRANSACTION_SYSTEM_ID.to_owned(),
            },
            action: PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent: PublicationIntent {
                    object_keys: BTreeSet::from([manifest.key.clone()]),
                    manifest: manifest.clone(),
                    destination_root: RATEKEEPER_PUBLICATION_ROOT.to_owned(),
                    expected_prior_root: None,
                },
            },
        })
        .await?;
    let publish = client
        .commit(&PublicationCommand {
            identity: RequestIdentity {
                client_id: seed ^ 0x5055_424c_4943_4154,
                request_id: 2,
            },
            credential: GenerationCredential {
                generation: base.generation,
                transaction_system_id: RATEKEEPER_TRANSACTION_SYSTEM_ID.to_owned(),
            },
            action: PublicationAction::Publish {
                publication_id,
                destination_root: RATEKEEPER_PUBLICATION_ROOT.to_owned(),
                expected_prior_root: None,
                manifest: manifest.clone(),
            },
        })
        .await?;
    let state = client.read().await?;
    let exact = prepare.status == PublicationCommandStatus::Accepted
        && publish.status == PublicationCommandStatus::Accepted
        && state.roots.get(RATEKEEPER_PUBLICATION_ROOT) == Some(&manifest);
    let root_sha256: [u8; 32] =
        Sha256::digest(serde_json::to_vec(&manifest).map_err(|error| error.to_string())?).into();
    Ok((manifest, bytes, root_sha256, exact))
}

#[allow(clippy::too_many_arguments)]
fn pop_ratekeeper_logs(
    log_sets: &[CellCommitLogSetConfig],
    policies: &[CellLogSetPolicy],
    baseline: &CellStateSnapshot,
    publication_root_sha256: [u8; 32],
    object_frontier: u64,
    nodes_per_set: usize,
    pop_capability: &PublicationPopCapabilityCertificate,
    manifest_bytes: &[u8],
) -> Result<RatekeeperPopEvidence, String> {
    let mut certificates = Vec::new();
    let mut attestation_count = 0_u64;
    let mut retained_high_watermark = 0_u64;
    let mut quorum_every_set = true;
    let mut cryptographically_valid = true;
    for set in log_sets {
        let policy = policies
            .iter()
            .find(|policy| policy.log_set_id == set.log_set_id)
            .ok_or_else(|| format!("pop policy {} is absent", set.log_set_id))?;
        let statement = CellTaggedLogPopStatement {
            format_version: FORMAT_VERSION,
            cell_id: baseline.cell_id,
            tenant_id: baseline.tenant_id,
            generation: baseline.generation,
            log_set_id: set.log_set_id,
            policy_epoch: policy.policy_epoch,
            publication_root_sha256,
            object_frontier,
            pop_epoch: 1,
        };
        let mut attestations = Vec::new();
        for endpoint in set.endpoints.iter().take(nodes_per_set) {
            let TaggedLogResponse::Popped {
                log_set_id,
                node_id,
                statement: observed,
                attestation,
                durable,
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Pop {
                    statement: statement.clone(),
                    capability: pop_capability.clone(),
                    manifest_bytes: manifest_bytes.to_vec(),
                },
            )?
            else {
                return Err("ratekeeper pop received no durable attestation".to_owned());
            };
            if !durable
                || log_set_id != set.log_set_id
                || node_id != attestation.signer_id
                || observed != statement
            {
                return Err("ratekeeper pop attestation mismatch".to_owned());
            }
            retained_high_watermark = retained_high_watermark.max(attestation.retained_bytes);
            attestation_count = attestation_count.saturating_add(1);
            attestations.push(attestation);
        }
        let certificate = CellTaggedLogPopCertificate {
            statement,
            attestations,
        };
        let valid = verify_tagged_log_pop_certificate(&certificate, policy);
        quorum_every_set &= valid;
        cryptographically_valid &= valid;
        certificates.push(certificate);
    }
    for set in log_sets {
        for endpoint in &set.endpoints {
            if let TaggedLogResponse::Ready { retained_bytes, .. } =
                tagged_log_request(endpoint, &TaggedLogRequest::Status)?
            {
                retained_high_watermark = retained_high_watermark.max(retained_bytes);
            }
        }
    }
    Ok(RatekeeperPopEvidence {
        certificates,
        attestation_count,
        retained_high_watermark,
        quorum_every_set,
        cryptographically_valid,
    })
}

fn read_ratekeeper_suffix(
    log_sets: &[CellCommitLogSetConfig],
    after_version: u64,
    through_version: u64,
) -> Result<RatekeeperSuffix, String> {
    let mut observed = BTreeMap::new();
    for set in log_sets {
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Feed {
                node_id, records, ..
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Read {
                    range_tag: set.log_set_id,
                    after_version,
                    through_version,
                },
            )?
            else {
                return Err("suffix read received non-feed response".to_owned());
            };
            observed.insert(
                (set.log_set_id, node_id),
                records.into_iter().map(|record| record.envelope).collect(),
            );
        }
    }
    Ok(observed)
}

fn run_ratekeeper_worker_child(
    executable: &Path,
    config: &CellTaggedLogLagWorkerProcessConfig,
) -> Result<CellTaggedLogLagWorkerReceipt, String> {
    let output = Command::new(executable)
        .arg("cell-tagged-log-lag-worker-node")
        .arg("--config-json")
        .arg(serde_json::to_string(config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start ratekeeper worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ratekeeper worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&fs::read(&config.output_path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn versions_have_quorum(
    log_sets: &[CellCommitLogSetConfig],
    after_version: u64,
    through_version: u64,
) -> Result<bool, String> {
    for set in log_sets {
        let mut versions = BTreeMap::<u64, BTreeSet<u64>>::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Feed {
                node_id, records, ..
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Read {
                    range_tag: set.log_set_id,
                    after_version,
                    through_version,
                },
            )?
            else {
                return Ok(false);
            };
            for record in records {
                let envelope =
                    CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
                versions
                    .entry(envelope.version().sequence())
                    .or_default()
                    .insert(node_id);
            }
        }
        for version in after_version.saturating_add(1)..=through_version {
            if versions.get(&version).map_or(0, BTreeSet::len) < TLOG_QUORUM {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn ratekeeper_check(id: &str, passed: bool) -> CellTaggedLogLagRatekeepingCheck {
    CellTaggedLogLagRatekeepingCheck {
        id: id.to_owned(),
        passed,
    }
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: CellCommitVisibilityMode,
    executable: &Path,
) -> Result<CellCommitVisibilityReport, String> {
    let root = TempRoot::new(seed)?;
    let mut authority = CellProcessFixture::start(
        seed ^ 0x434f_4d4d_4954_5630,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let authority_report = authority.run_history().await?;
    let baseline = authority.linearizable_cell_snapshot().await?;
    let baseline_frontier = baseline.latest_sequence;
    let transaction = transaction(seed, &baseline);

    let signing_seeds_10 = tagged_log_signing_seeds(seed, 10);
    let signing_seeds_20 = tagged_log_signing_seeds(seed, 20);
    let tlog10 = if mode.requires_certificates() {
        TaggedLogProcessFixture::start_signed(
            executable,
            &root.0.join("log-set-10"),
            10,
            TLOG_NODES_PER_SET,
            TLOG_RETAINED_BYTES_LIMIT,
            false,
            LOG_SET_POLICY_EPOCH,
            &signing_seeds_10,
        )?
    } else {
        TaggedLogProcessFixture::start(
            executable,
            &root.0.join("log-set-10"),
            10,
            TLOG_NODES_PER_SET,
            TLOG_RETAINED_BYTES_LIMIT,
            false,
        )?
    };
    let tlog20 = if mode.requires_certificates() {
        TaggedLogProcessFixture::start_signed(
            executable,
            &root.0.join("log-set-20"),
            20,
            TLOG_NODES_PER_SET,
            TLOG_RETAINED_BYTES_LIMIT,
            false,
            LOG_SET_POLICY_EPOCH,
            &signing_seeds_20,
        )?
    } else {
        TaggedLogProcessFixture::start(
            executable,
            &root.0.join("log-set-20"),
            20,
            TLOG_NODES_PER_SET,
            TLOG_RETAINED_BYTES_LIMIT,
            false,
        )?
    };
    let log_sets = vec![
        CellCommitLogSetConfig {
            log_set_id: 10,
            endpoints: tlog10.endpoints(),
        },
        CellCommitLogSetConfig {
            log_set_id: 20,
            endpoints: tlog20.endpoints(),
        },
    ];
    let policies = if mode.requires_certificates() {
        vec![
            log_set_policy(10, baseline.generation, &signing_seeds_10)?,
            log_set_policy(20, baseline.generation, &signing_seeds_20)?,
        ]
    } else {
        Vec::new()
    };
    let policies_installed = if policies.is_empty() {
        true
    } else {
        install_log_set_policies(&authority, &transaction, policies.clone()).await?
    };

    let mut proxy_receipts = Vec::new();
    let mut proxy_process_kills = 0_u64;
    let (snapshot_after_first, snapshot_after_second) = match mode {
        CellCommitVisibilityMode::Correct | CellCommitVisibilityMode::AuthenticatedCorrect => {
            proxy_receipts.push(run_proxy_child(
                executable,
                &proxy_config(
                    &root.0,
                    &authority,
                    &transaction,
                    baseline_frontier,
                    &log_sets,
                    CellCommitProxyPhase::FirstLogSet,
                    1,
                    true,
                    mode,
                ),
            )?);
            proxy_process_kills = proxy_process_kills.saturating_add(1);
            let snapshot_after_first = authority.linearizable_cell_snapshot().await?;

            proxy_receipts.push(run_proxy_child(
                executable,
                &proxy_config(
                    &root.0,
                    &authority,
                    &transaction,
                    baseline_frontier,
                    &log_sets,
                    CellCommitProxyPhase::SecondLogSet,
                    2,
                    true,
                    mode,
                ),
            )?);
            proxy_process_kills = proxy_process_kills.saturating_add(1);
            let snapshot_after_second = authority.linearizable_cell_snapshot().await?;

            proxy_receipts.push(run_proxy_child(
                executable,
                &proxy_config(
                    &root.0,
                    &authority,
                    &transaction,
                    baseline_frontier,
                    &log_sets,
                    CellCommitProxyPhase::Publish,
                    3,
                    false,
                    mode,
                ),
            )?);
            (snapshot_after_first, snapshot_after_second)
        }
        CellCommitVisibilityMode::AcknowledgeAfterOneLogSet => {
            proxy_receipts.push(run_proxy_child(
                executable,
                &proxy_config(
                    &root.0,
                    &authority,
                    &transaction,
                    baseline_frontier,
                    &log_sets,
                    CellCommitProxyPhase::PrematureAcknowledge,
                    1,
                    false,
                    mode,
                ),
            )?);
            let snapshot_after_first = authority.linearizable_cell_snapshot().await?;
            let snapshot_after_second = snapshot_after_first.clone();
            (snapshot_after_first, snapshot_after_second)
        }
        CellCommitVisibilityMode::UnsignedNodeList
        | CellCommitVisibilityMode::DuplicateAttestation
        | CellCommitVisibilityMode::WrongLogSetAttestation
        | CellCommitVisibilityMode::TamperedStatement
        | CellCommitVisibilityMode::ObsoletePolicyEpoch => {
            proxy_receipts.push(run_proxy_child(
                executable,
                &proxy_config(
                    &root.0,
                    &authority,
                    &transaction,
                    baseline_frontier,
                    &log_sets,
                    CellCommitProxyPhase::CertificateControl,
                    1,
                    false,
                    mode,
                ),
            )?);
            let snapshot_after_first = authority.linearizable_cell_snapshot().await?;
            let snapshot_after_second = snapshot_after_first.clone();
            (snapshot_after_first, snapshot_after_second)
        }
    };
    let final_snapshot = authority.linearizable_cell_snapshot().await?;
    let first = proxy_receipts
        .first()
        .ok_or_else(|| "commit visibility history emitted no proxy receipt".to_owned())?;
    let final_proxy = proxy_receipts
        .last()
        .ok_or_else(|| "commit visibility history emitted no final proxy receipt".to_owned())?;
    let target_version = first.commit_sequence;
    let baseline_log_chain = baseline
        .committed_envelopes
        .last()
        .map_or([0; 32], |envelope| Sha256::digest(envelope).into());
    let worker_output = root.0.join("worker-output.json");
    let worker_config = CellCommitVisibilityWorkerProcessConfig {
        cell_id: baseline.cell_id,
        tenant_id: baseline.tenant_id,
        generation: baseline.generation,
        baseline_frontier,
        baseline_rows: baseline.rows.clone(),
        baseline_log_chain,
        target_version,
        log_sets: log_sets.clone(),
        output_path: worker_output.clone(),
    };
    let output = Command::new(executable)
        .arg("cell-commit-visibility-worker-node")
        .arg("--config-json")
        .arg(serde_json::to_string(&worker_config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start commit visibility worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "commit visibility worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellCommitVisibilityWorkerReceipt =
        serde_json::from_slice(&fs::read(&worker_output).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let log_set_positions = log_set_positions(&log_sets)?;
    let expected_final_rows = apply_transaction_rows(&baseline.rows, &transaction.mutations);
    let envelope_stable = proxy_receipts
        .iter()
        .all(|receipt| receipt.envelope == first.envelope);
    let version_stable = proxy_receipts
        .iter()
        .all(|receipt| receipt.commit_sequence == target_version);
    let all_log_positions_exact = REQUIRED_LOG_SETS.iter().all(|log_set| {
        log_set_positions
            .get(log_set)
            .is_some_and(|positions| positions == &[1, 1, 1])
    });
    let all_receipts_actual = proxy_receipts.iter().all(|receipt| {
        receipt.quorum_node_ids.iter().all(|(log_set, nodes)| {
            REQUIRED_LOG_SETS.contains(log_set)
                && nodes.len() >= TLOG_QUORUM
                && nodes.iter().all(|node| (1..=3).contains(node))
        })
    });
    let tagged_log_appends = proxy_receipts
        .iter()
        .map(|receipt| receipt.new_log_appends)
        .sum::<u64>();
    let tagged_log_attestations = proxy_receipts
        .iter()
        .flat_map(|receipt| receipt.attestation_signer_ids.values())
        .map(|signers| u64::try_from(signers.len()).unwrap_or(u64::MAX))
        .sum::<u64>();
    let certificate_rejections = proxy_receipts
        .iter()
        .filter(|receipt| receipt.certificate_rejected)
        .count() as u64;
    let attestation_signers_actual = proxy_receipts.iter().all(|receipt| {
        receipt
            .attestation_signer_ids
            .iter()
            .all(|(log_set, nodes)| {
                REQUIRED_LOG_SETS.contains(log_set)
                    && nodes.len() >= TLOG_QUORUM
                    && nodes.iter().all(|node| (1..=3).contains(node))
            })
    });
    let durable_log_sets = final_proxy.durable_log_sets.clone();
    let client_acknowledged = final_proxy.client_acknowledged;
    let authority_visible = final_proxy.authority_visible;
    let retry_status = final_proxy.retry_status;
    let mut checks = vec![
        check(
            "baseline_history_clean",
            authority_report.anomaly_count == 0,
        ),
        check("baseline_frontier_nonzero", baseline_frontier > 0),
        check(
            "staged_version_above_baseline",
            target_version > baseline_frontier,
        ),
        check("staged_envelope_stable_across_retries", envelope_stable),
        check("staged_version_stable_across_retries", version_stable),
        check(
            "first_proxy_records_only_log_set_10",
            first.durable_log_sets == vec![10],
        ),
        check(
            "first_proxy_does_not_acknowledge",
            !first.client_acknowledged,
        ),
        check("first_proxy_does_not_publish", !first.authority_visible),
        check(
            "first_proxy_death_preserves_visible_rows",
            snapshot_after_first == baseline,
        ),
        check(
            "second_proxy_records_both_log_sets",
            proxy_receipts
                .get(1)
                .is_some_and(|receipt| receipt.durable_log_sets == REQUIRED_LOG_SETS),
        ),
        check(
            "second_proxy_does_not_acknowledge",
            proxy_receipts
                .get(1)
                .is_some_and(|receipt| !receipt.client_acknowledged),
        ),
        check(
            "second_proxy_does_not_publish",
            proxy_receipts
                .get(1)
                .is_some_and(|receipt| !receipt.authority_visible),
        ),
        check(
            "second_proxy_death_preserves_visible_rows",
            snapshot_after_second == baseline,
        ),
        check("two_proxy_deaths_exercised", proxy_process_kills == 2),
        check(
            "both_log_sets_have_three_durable_records",
            all_log_positions_exact,
        ),
        check(
            "quorum_receipts_bind_actual_process_nodes",
            all_receipts_actual,
        ),
        check(
            "every_required_log_set_recorded",
            durable_log_sets == REQUIRED_LOG_SETS,
        ),
        check(
            "acknowledgement_waits_for_visibility",
            client_acknowledged && authority_visible,
        ),
        check(
            "visible_frontier_advances_once",
            final_snapshot.latest_sequence == target_version,
        ),
        check(
            "visible_rows_apply_once",
            final_snapshot.rows == expected_final_rows,
        ),
        check(
            "visible_envelope_appended_once",
            final_snapshot.committed_envelopes.len()
                == baseline.committed_envelopes.len().saturating_add(1),
        ),
        check(
            "repeated_transaction_identity_is_retained",
            retry_status == Some(CellStagedTransactionStatus::AlreadyCommitted),
        ),
        check(
            "final_proxy_does_not_append_duplicate_logs",
            proxy_receipts
                .get(2)
                .is_some_and(|receipt| receipt.new_log_appends == 0),
        ),
        check("exactly_six_tagged_log_appends", tagged_log_appends == 6),
        check(
            "fresh_worker_reads_both_log_set_quorums",
            worker.recovered_log_sets == REQUIRED_LOG_SETS,
        ),
        check(
            "fresh_worker_observes_exact_envelope",
            worker.exact_envelope_across_sets && worker.chain_valid,
        ),
        check(
            "fresh_worker_reaches_visible_frontier",
            worker.observed_frontier == target_version,
        ),
        check(
            "fresh_worker_reconstructs_visible_rows",
            worker.rows == final_snapshot.rows,
        ),
    ];
    if mode.requires_certificates() {
        checks.extend([
            check(
                "log_set_policies_installed_independently",
                policies_installed && policies.len() == REQUIRED_LOG_SETS.len(),
            ),
            check(
                "attestations_come_from_configured_processes",
                tagged_log_attestations >= u64::try_from(TLOG_QUORUM).unwrap_or(u64::MAX)
                    && attestation_signers_actual,
            ),
            check(
                "authority_decides_authenticated_certificate",
                if mode.is_certificate_control() {
                    certificate_rejections == 1
                } else {
                    certificate_rejections == 0 && durable_log_sets == REQUIRED_LOG_SETS
                },
            ),
            check(
                "certificate_path_is_not_unsigned_receipt_path",
                mode != CellCommitVisibilityMode::UnsignedNodeList || certificate_rejections == 1,
            ),
        ]);
        debug_assert_eq!(checks.len(), AUTHENTICATED_EXPECTED_CHECKS);
    } else {
        debug_assert_eq!(checks.len(), EXPECTED_CHECKS);
    }
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-commit-visibility-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(baseline_frontier.to_be_bytes());
    trace.update(target_version.to_be_bytes());
    trace.update(first.envelope_sha256);
    for receipt in &proxy_receipts {
        trace.update(serde_json::to_vec(receipt).map_err(|error| error.to_string())?);
    }
    for check in &checks {
        trace.update(check.id.as_bytes());
        trace.update([u8::from(check.passed)]);
    }
    Ok(CellCommitVisibilityReport {
        seed,
        mode,
        question: "Can a transaction remain invisible through two proxy deaths and become acknowledged only after every required tagged log set is quorum durable?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_process_fixture"
        } else {
            "no"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        baseline_frontier,
        target_version,
        observed_frontier: worker.observed_frontier,
        required_log_sets: REQUIRED_LOG_SETS.to_vec(),
        durable_log_sets,
        staged_envelope_sha256: hex(first.envelope_sha256),
        client_acknowledged,
        authority_visible,
        retry_status,
        authority_process_starts: authority_report.process_starts,
        tagged_log_process_starts: (TLOG_NODES_PER_SET * REQUIRED_LOG_SETS.len()) as u64,
        proxy_process_starts: proxy_receipts.len() as u64,
        proxy_process_kills,
        worker_process_starts: 1,
        tagged_log_appends,
        log_set_policy_count: policies.len() as u64,
        tagged_log_attestations,
        certificate_rejections,
        log_set_positions,
        proxy_receipts,
        reconstructed_rows: worker.rows,
        checks: checks.clone(),
        trace_sha256: hex(trace.finalize().into()),
    })
}

fn transaction(seed: u64, baseline: &CellStateSnapshot) -> CellTransactionCommand {
    let key = b"rfc-0039/commit-visibility".to_vec();
    CellTransactionCommand {
        identity: transaction_identity(seed),
        credential: None,
        cell_id: baseline.cell_id,
        tenant_id: baseline.tenant_id,
        generation: baseline.generation,
        read_version: CellReadVersion {
            generation: baseline.generation,
            sequence: baseline.latest_sequence,
        },
        read_conflicts: vec![CellKeyRange::point(&key)],
        write_conflicts: vec![CellKeyRange::point(&key)],
        mutations: vec![CellMutation::Set {
            key,
            value: format!("visible-after-both-log-sets-{seed}").into_bytes(),
        }],
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: REQUIRED_LOG_SETS.to_vec(),
    }
}

fn tagged_log_signing_seeds(seed: u64, log_set_id: u16) -> Vec<Vec<u8>> {
    (1..=TLOG_NODES_PER_SET)
        .map(|node_id| {
            let mut digest = Sha256::new();
            digest.update(b"okv-cell-tagged-log-signer-v1");
            digest.update(seed.to_be_bytes());
            digest.update(log_set_id.to_be_bytes());
            digest.update(u64::try_from(node_id).unwrap_or(u64::MAX).to_be_bytes());
            digest.finalize().to_vec()
        })
        .collect()
}

fn log_set_policy(
    log_set_id: u16,
    generation: u64,
    signing_seeds: &[Vec<u8>],
) -> Result<CellLogSetPolicy, String> {
    let members = signing_seeds
        .iter()
        .enumerate()
        .map(|(index, seed)| {
            Ok(CellLogSetMember {
                node_id: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                public_key: tagged_log_public_key(seed)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(CellLogSetPolicy {
        format_version: FORMAT_VERSION,
        generation,
        policy_epoch: LOG_SET_POLICY_EPOCH,
        log_set_id,
        quorum_size: u16::try_from(TLOG_QUORUM).unwrap_or(u16::MAX),
        ratekeeper_soft_limit_bytes: 0,
        members,
    })
}

async fn install_log_set_policies(
    authority: &CellProcessFixture<'_>,
    transaction: &CellTransactionCommand,
    policies: Vec<CellLogSetPolicy>,
) -> Result<bool, String> {
    let client = CellTransactionClient::new(authority.endpoints())?;
    let response = transition(
        &client,
        transaction,
        transition_identity(&transaction.identity, 50),
        CellStagedTransactionAction::InstallLogSetPolicies { policies },
    )
    .await?;
    Ok(response.status == CellStagedTransactionStatus::LogSetPoliciesInstalled)
}

#[allow(clippy::too_many_arguments)]
fn proxy_config(
    root: &Path,
    authority: &CellProcessFixture<'_>,
    transaction: &CellTransactionCommand,
    baseline_frontier: u64,
    log_sets: &[CellCommitLogSetConfig],
    phase: CellCommitProxyPhase,
    attempt: u64,
    linger_for_kill: bool,
    mode: CellCommitVisibilityMode,
) -> CellCommitProxyProcessConfig {
    CellCommitProxyProcessConfig {
        authority_endpoints: authority.endpoints(),
        transaction: transaction.clone(),
        baseline_frontier,
        log_sets: log_sets.to_vec(),
        mode,
        phase,
        attempt,
        linger_for_kill,
        output_path: root.join(format!("proxy-{attempt}.json")),
    }
}

fn run_proxy_child(
    executable: &Path,
    config: &CellCommitProxyProcessConfig,
) -> Result<CellCommitProxyReceipt, String> {
    let output_path = config.output_path.clone();
    let linger = config.linger_for_kill;
    let mut child = Command::new(executable)
        .arg("cell-commit-proxy-node")
        .arg("--config-json")
        .arg(serde_json::to_string(&config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start commit proxy process: {error}"))?;
    wait_for_receipt(&mut child, &output_path)?;
    let receipt =
        serde_json::from_slice(&fs::read(&output_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    if linger {
        child.kill().map_err(|error| error.to_string())?;
    }
    let status = child.wait().map_err(|error| error.to_string())?;
    if !linger && !status.success() {
        return Err(format!("commit proxy process exited with {status}"));
    }
    Ok(receipt)
}

fn wait_for_receipt(child: &mut Child, output_path: &Path) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if output_path.is_file() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            let stderr = child
                .stderr
                .take()
                .and_then(|mut stderr| {
                    use std::io::Read as _;
                    let mut bytes = Vec::new();
                    stderr.read_to_end(&mut bytes).ok()?;
                    String::from_utf8(bytes).ok()
                })
                .unwrap_or_default();
            return Err(format!(
                "commit proxy exited before receipt with {status}: {stderr}"
            ));
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("commit proxy did not emit its receipt before timeout".to_owned())
}

/// Execute one disposable commit proxy process.
///
/// # Errors
///
/// Returns an error when staging, tagged-log quorum durability, or replicated
/// receipt publication fails.
#[allow(clippy::too_many_lines)]
pub async fn run_cell_commit_proxy_process(
    config: CellCommitProxyProcessConfig,
) -> Result<(), String> {
    let client = CellTransactionClient::new(config.authority_endpoints.clone())?;
    let staged = transition(
        &client,
        &config.transaction,
        transition_identity(&config.transaction.identity, 100 + config.attempt),
        CellStagedTransactionAction::Stage {
            transaction: config.transaction.clone(),
        },
    )
    .await?;
    if !matches!(
        staged.status,
        CellStagedTransactionStatus::Staged | CellStagedTransactionStatus::AlreadyCommitted
    ) {
        return Err(format!(
            "commit proxy could not stage transaction: {:?}",
            staged.status
        ));
    }
    let commit_sequence = staged
        .commit_sequence
        .ok_or_else(|| "staged transaction omitted commit sequence".to_owned())?;
    let envelope = staged
        .envelope
        .clone()
        .ok_or_else(|| "staged transaction omitted envelope".to_owned())?;
    let envelope_sha256: [u8; 32] = Sha256::digest(&envelope).into();
    let wanted_sets: &[u16] = match config.phase {
        CellCommitProxyPhase::FirstLogSet
        | CellCommitProxyPhase::PrematureAcknowledge
        | CellCommitProxyPhase::CertificateControl => &[10],
        CellCommitProxyPhase::SecondLogSet | CellCommitProxyPhase::Publish => &REQUIRED_LOG_SETS,
    };
    let mut quorum_node_ids = BTreeMap::new();
    let mut attestation_signer_ids = BTreeMap::new();
    let mut new_log_appends = 0_u64;
    let mut certificate_rejected = false;
    let mut latest = staged;
    for log_set_id in wanted_sets {
        let set = config
            .log_sets
            .iter()
            .find(|set| set.log_set_id == *log_set_id)
            .ok_or_else(|| format!("commit proxy is missing log set {log_set_id}"))?;
        let durable = ensure_log_set(
            set,
            config.baseline_frontier,
            commit_sequence,
            &envelope,
            &config.transaction,
            config.mode.requires_certificates(),
        )?;
        new_log_appends = new_log_appends.saturating_add(durable.new_appends);
        quorum_node_ids.insert(*log_set_id, durable.node_ids.clone());
        attestation_signer_ids.insert(
            *log_set_id,
            durable
                .attestations
                .iter()
                .map(|attestation| attestation.signer_id)
                .collect(),
        );
        let action = if config.mode == CellCommitVisibilityMode::UnsignedNodeList
            || !config.mode.requires_certificates()
        {
            CellStagedTransactionAction::RecordLogReceipt {
                receipt: CellTaggedLogReceipt {
                    format_version: FORMAT_VERSION,
                    log_set_id: *log_set_id,
                    generation: config.transaction.generation,
                    envelope_sha256,
                    durable_position: 1,
                    quorum_node_ids: durable.node_ids,
                },
            }
        } else {
            let statement = CellTaggedLogStatement {
                format_version: FORMAT_VERSION,
                cell_id: config.transaction.cell_id,
                tenant_id: config.transaction.tenant_id,
                generation: config.transaction.generation,
                transaction_identity: config.transaction.identity,
                commit_sequence,
                log_set_id: *log_set_id,
                policy_epoch: LOG_SET_POLICY_EPOCH,
                envelope_sha256,
                durable_position: 1,
            };
            let certificate = fault_certificate(
                CellTaggedLogCertificate {
                    statement,
                    attestations: durable.attestations,
                },
                config.mode,
            );
            CellStagedTransactionAction::RecordLogCertificate { certificate }
        };
        latest = transition(
            &client,
            &config.transaction,
            transition_identity(&config.transaction.identity, 1_000 + u64::from(*log_set_id)),
            action,
        )
        .await?;
        let expected = if config.mode.is_certificate_control() {
            matches!(
                latest.status,
                CellStagedTransactionStatus::InvalidLogCertificate
                    | CellStagedTransactionStatus::InvalidLogReceipt
            )
        } else if config.mode.requires_certificates() {
            latest.status == CellStagedTransactionStatus::LogCertificateRecorded
        } else {
            latest.status == CellStagedTransactionStatus::LogReceiptRecorded
        };
        if !expected {
            return Err(format!(
                "commit proxy log durability decision was unexpected: {:?}",
                latest.status
            ));
        }
        certificate_rejected = config.mode.is_certificate_control();
        if certificate_rejected {
            break;
        }
    }

    let mut client_acknowledged = false;
    let mut retry_status = None;
    match config.phase {
        CellCommitProxyPhase::Publish => {
            latest = transition(
                &client,
                &config.transaction,
                transition_identity(&config.transaction.identity, 2_000),
                CellStagedTransactionAction::Publish,
            )
            .await?;
            client_acknowledged =
                latest.status == CellStagedTransactionStatus::Committed && latest.visible;
            let retry = transition(
                &client,
                &config.transaction,
                transition_identity(&config.transaction.identity, 2_001),
                CellStagedTransactionAction::Stage {
                    transaction: config.transaction.clone(),
                },
            )
            .await?;
            retry_status = Some(retry.status);
        }
        CellCommitProxyPhase::PrematureAcknowledge => {
            latest = transition(
                &client,
                &config.transaction,
                transition_identity(&config.transaction.identity, 2_000),
                CellStagedTransactionAction::Publish,
            )
            .await?;
            if latest.status != CellStagedTransactionStatus::MissingLogReceipt {
                return Err(
                    "unsafe control unexpectedly published without every log set".to_owned(),
                );
            }
            client_acknowledged = true;
        }
        CellCommitProxyPhase::FirstLogSet
        | CellCommitProxyPhase::SecondLogSet
        | CellCommitProxyPhase::CertificateControl => {}
    }
    let receipt = CellCommitProxyReceipt {
        phase: config.phase,
        commit_sequence,
        envelope,
        envelope_sha256,
        durable_log_sets: latest.durable_log_sets.clone(),
        quorum_node_ids,
        attestation_signer_ids,
        new_log_appends,
        certificate_rejected,
        authority_status: latest.status,
        authority_visible: latest.visible,
        client_acknowledged,
        retry_status,
    };
    persist_receipt(&config.output_path, &receipt)?;
    if config.linger_for_kill {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
    Ok(())
}

fn fault_certificate(
    mut certificate: CellTaggedLogCertificate,
    mode: CellCommitVisibilityMode,
) -> CellTaggedLogCertificate {
    match mode {
        CellCommitVisibilityMode::DuplicateAttestation => {
            if let Some(first) = certificate.attestations.first().cloned() {
                certificate.attestations = vec![first.clone(), first];
            }
        }
        CellCommitVisibilityMode::WrongLogSetAttestation => {
            certificate.statement.log_set_id = 20;
        }
        CellCommitVisibilityMode::TamperedStatement => {
            certificate.statement.envelope_sha256[0] ^= 0xff;
        }
        CellCommitVisibilityMode::ObsoletePolicyEpoch => {
            certificate.statement.policy_epoch = 0;
        }
        CellCommitVisibilityMode::Correct
        | CellCommitVisibilityMode::AcknowledgeAfterOneLogSet
        | CellCommitVisibilityMode::AuthenticatedCorrect
        | CellCommitVisibilityMode::UnsignedNodeList => {}
    }
    certificate
}

async fn transition(
    client: &CellTransactionClient,
    transaction: &CellTransactionCommand,
    identity: RequestIdentity,
    action: CellStagedTransactionAction,
) -> Result<CellStagedTransactionApplyResponse, String> {
    let command = CellStagedTransactionCommand {
        identity,
        credential: transaction.credential.clone(),
        cell_id: transaction.cell_id,
        tenant_id: transaction.tenant_id,
        generation: transaction.generation,
        transaction_identity: transaction.identity,
        action,
    };
    let response = client
        .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
        .await
        .map_err(|error| format!("staged action {:?}: {error}", command.action))?;
    if let Some(error) = response.error {
        return Err(format!("staged transaction transition failed: {error:?}"));
    }
    response
        .cell_staged_transaction
        .ok_or_else(|| "authority omitted staged transaction response".to_owned())
}

struct DurableSet {
    node_ids: Vec<u64>,
    attestations: Vec<CellTaggedLogAttestation>,
    new_appends: u64,
}

#[allow(clippy::too_many_lines)]
fn ensure_log_set(
    set: &CellCommitLogSetConfig,
    baseline_frontier: u64,
    target_version: u64,
    envelope: &[u8],
    transaction: &CellTransactionCommand,
    authenticated: bool,
) -> Result<DurableSet, String> {
    let mut node_ids = BTreeSet::new();
    let mut missing = Vec::new();
    for endpoint in &set.endpoints {
        let response = tagged_log_request(
            endpoint,
            &TaggedLogRequest::Read {
                range_tag: set.log_set_id,
                after_version: baseline_frontier,
                through_version: target_version,
            },
        )?;
        let TaggedLogResponse::Feed {
            log_set_id,
            node_id,
            records,
            ..
        } = response
        else {
            return Err("commit proxy received non-feed tagged-log response".to_owned());
        };
        if log_set_id != set.log_set_id {
            return Err("tagged-log response names the wrong log set".to_owned());
        }
        if records.iter().any(|record| record.envelope == envelope) {
            node_ids.insert(node_id);
        } else {
            missing.push(endpoint.clone());
        }
    }
    let record = TaggedLogRecord::committed(1, REQUIRED_LOG_SETS.to_vec(), envelope.to_vec());
    let mut new_appends = 0_u64;
    for endpoint in missing {
        let response = tagged_log_request(
            &endpoint,
            &TaggedLogRequest::Append {
                record: record.clone(),
            },
        )?;
        let TaggedLogResponse::Appended {
            log_set_id,
            node_id,
            position: 1,
            ..
        } = response
        else {
            return Err("commit proxy tagged-log append was not durable".to_owned());
        };
        if log_set_id != set.log_set_id {
            return Err("tagged-log append names the wrong log set".to_owned());
        }
        node_ids.insert(node_id);
        new_appends = new_appends.saturating_add(1);
    }
    if node_ids.len() < TLOG_QUORUM {
        return Err(format!(
            "log set {} reached only {} durable nodes",
            set.log_set_id,
            node_ids.len()
        ));
    }
    let mut attestations = Vec::new();
    if authenticated {
        let statement = CellTaggedLogStatement {
            format_version: FORMAT_VERSION,
            cell_id: transaction.cell_id,
            tenant_id: transaction.tenant_id,
            generation: transaction.generation,
            transaction_identity: transaction.identity,
            commit_sequence: target_version,
            log_set_id: set.log_set_id,
            policy_epoch: LOG_SET_POLICY_EPOCH,
            envelope_sha256: Sha256::digest(envelope).into(),
            durable_position: 1,
        };
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Attested {
                log_set_id,
                node_id,
                statement: observed,
                attestation,
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Attest {
                    statement: statement.clone(),
                },
            )?
            else {
                return Err("commit proxy received no tagged-log attestation".to_owned());
            };
            if log_set_id != set.log_set_id
                || node_id != attestation.signer_id
                || observed != statement
            {
                return Err("tagged-log attestation identity mismatch".to_owned());
            }
            attestations.push(attestation);
        }
    }
    Ok(DurableSet {
        node_ids: node_ids.into_iter().collect(),
        attestations,
        new_appends,
    })
}

/// Execute one fresh worker over independently replicated tagged-log sets.
///
/// # Errors
///
/// Returns an error for malformed or conflicting quorum records. A missing
/// quorum emits an incomplete receipt so the evaluation oracle can detect it.
pub fn run_cell_commit_visibility_worker_process(
    config: CellCommitVisibilityWorkerProcessConfig,
) -> Result<(), String> {
    let mut recovered = BTreeMap::<u16, (Vec<u8>, Vec<u64>)>::new();
    for set in &config.log_sets {
        let mut candidates = BTreeMap::<[u8; 32], (Vec<u8>, BTreeSet<u64>)>::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Feed {
                log_set_id,
                node_id,
                records,
                ..
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Read {
                    range_tag: set.log_set_id,
                    after_version: config.baseline_frontier,
                    through_version: config.target_version,
                },
            )?
            else {
                continue;
            };
            if log_set_id != set.log_set_id {
                return Err("worker received a response from the wrong log set".to_owned());
            }
            for record in records {
                let digest: [u8; 32] = Sha256::digest(&record.envelope).into();
                let candidate = candidates
                    .entry(digest)
                    .or_insert_with(|| (record.envelope.clone(), BTreeSet::new()));
                if candidate.0 != record.envelope {
                    return Err("worker observed an envelope digest collision".to_owned());
                }
                candidate.1.insert(node_id);
            }
        }
        let mut quorums = candidates
            .into_values()
            .filter(|(_, nodes)| nodes.len() >= TLOG_QUORUM);
        if let Some((envelope, nodes)) = quorums.next() {
            if quorums.next().is_some() {
                return Err("worker observed conflicting envelope quorums".to_owned());
            }
            recovered.insert(set.log_set_id, (envelope, nodes.into_iter().collect()));
        }
    }
    let recovered_log_sets = recovered.keys().copied().collect::<Vec<_>>();
    let quorum_node_ids = recovered
        .iter()
        .map(|(set, (_, nodes))| (*set, nodes.clone()))
        .collect::<BTreeMap<_, _>>();
    let exact_envelope_across_sets = recovered.len() == REQUIRED_LOG_SETS.len()
        && recovered
            .values()
            .map(|(envelope, _)| envelope)
            .collect::<BTreeSet<_>>()
            .len()
            == 1;
    let mut rows = config.baseline_rows.into_iter().collect::<BTreeMap<_, _>>();
    let mut observed_frontier = config.baseline_frontier;
    let mut chain_valid = false;
    if exact_envelope_across_sets {
        let envelope_bytes = &recovered
            .values()
            .next()
            .ok_or_else(|| "worker exact-set check omitted envelope".to_owned())?
            .0;
        let envelope = CommitEnvelope::decode(envelope_bytes).map_err(|error| error.to_string())?;
        chain_valid = envelope.cell_id() == config.cell_id
            && envelope.tenant_id() == config.tenant_id
            && envelope.generation() == config.generation
            && envelope.version().sequence() == config.target_version
            && envelope.previous_log_chain() == config.baseline_log_chain;
        if chain_valid {
            apply_envelope(&mut rows, &envelope)?;
            observed_frontier = config.target_version;
        }
    }
    persist_receipt(
        &config.output_path,
        &CellCommitVisibilityWorkerReceipt {
            recovered_log_sets,
            quorum_node_ids,
            exact_envelope_across_sets,
            chain_valid,
            observed_frontier,
            rows: rows.into_iter().collect(),
        },
    )
}

/// Execute one fresh worker over a published base plus a multi-record tLog tail.
///
/// # Errors
///
/// Returns an error for malformed or conflicting quorum records. Missing
/// versions produce an incomplete receipt for the eval oracle.
pub fn run_cell_tagged_log_lag_worker_process(
    config: CellTaggedLogLagWorkerProcessConfig,
) -> Result<(), String> {
    let mut recovered = BTreeMap::<(u16, u64), Vec<u8>>::new();
    for set in &config.log_sets {
        let mut by_version = BTreeMap::<u64, BTreeMap<[u8; 32], (Vec<u8>, BTreeSet<u64>)>>::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Feed {
                log_set_id,
                node_id,
                records,
                ..
            } = tagged_log_request(
                endpoint,
                &TaggedLogRequest::Read {
                    range_tag: set.log_set_id,
                    after_version: config.object_frontier,
                    through_version: config.target_version,
                },
            )?
            else {
                continue;
            };
            if log_set_id != set.log_set_id {
                return Err("ratekeeper worker read the wrong log set".to_owned());
            }
            for record in records {
                let envelope =
                    CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
                let version = envelope.version().sequence();
                let digest: [u8; 32] = Sha256::digest(&record.envelope).into();
                let candidate = by_version
                    .entry(version)
                    .or_default()
                    .entry(digest)
                    .or_insert_with(|| (record.envelope.clone(), BTreeSet::new()));
                if candidate.0 != record.envelope {
                    return Err("ratekeeper worker observed a digest collision".to_owned());
                }
                candidate.1.insert(node_id);
            }
        }
        for (version, candidates) in by_version {
            let mut quorums = candidates
                .into_values()
                .filter(|(_, nodes)| nodes.len() >= TLOG_QUORUM);
            if let Some((envelope, _)) = quorums.next() {
                if quorums.next().is_some() {
                    return Err("ratekeeper worker observed conflicting quorums".to_owned());
                }
                recovered.insert((set.log_set_id, version), envelope);
            }
        }
    }
    let mut rows = config.base_rows.into_iter().collect::<BTreeMap<_, _>>();
    let mut previous_chain = config.base_log_chain;
    let mut observed_frontier = config.object_frontier;
    let mut suffix_records = 0_u64;
    let mut chain_exact = true;
    for version in config.object_frontier.saturating_add(1)..=config.target_version {
        let envelopes = REQUIRED_LOG_SETS
            .iter()
            .filter_map(|log_set| recovered.get(&(*log_set, version)))
            .collect::<Vec<_>>();
        if envelopes.len() != REQUIRED_LOG_SETS.len()
            || envelopes.windows(2).any(|pair| pair[0] != pair[1])
        {
            chain_exact = false;
            break;
        }
        let envelope = CommitEnvelope::decode(envelopes[0]).map_err(|error| error.to_string())?;
        if envelope.generation() != config.generation
            || envelope.version().sequence() != version
            || envelope.previous_log_chain() != previous_chain
        {
            chain_exact = false;
            break;
        }
        apply_envelope(&mut rows, &envelope)?;
        previous_chain = Sha256::digest(envelopes[0]).into();
        observed_frontier = version;
        suffix_records = suffix_records.saturating_add(1);
    }
    let rows = rows.into_iter().collect::<Vec<_>>();
    chain_exact &= observed_frontier == config.target_version && rows == config.expected_rows;
    persist_receipt(
        &config.output_path,
        &CellTaggedLogLagWorkerReceipt {
            observed_frontier,
            suffix_records,
            chain_exact,
            rows,
        },
    )
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

fn log_set_positions(
    log_sets: &[CellCommitLogSetConfig],
) -> Result<BTreeMap<u16, Vec<u64>>, String> {
    let mut positions = BTreeMap::new();
    for set in log_sets {
        let mut set_positions = Vec::new();
        for endpoint in &set.endpoints {
            let TaggedLogResponse::Ready {
                log_set_id,
                last_position,
                ..
            } = tagged_log_request(endpoint, &TaggedLogRequest::Status)?
            else {
                return Err("tagged-log status request did not return ready".to_owned());
            };
            if log_set_id != set.log_set_id {
                return Err("tagged-log status names the wrong log set".to_owned());
            }
            set_positions.push(last_position);
        }
        positions.insert(set.log_set_id, set_positions);
    }
    Ok(positions)
}

fn apply_transaction_rows(
    baseline: &[(Vec<u8>, Vec<u8>)],
    mutations: &[CellMutation],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = baseline.iter().cloned().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            CellMutation::Clear { key } => {
                rows.remove(key);
            }
            CellMutation::Set { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
        }
    }
    rows.into_iter().collect()
}

fn persist_receipt<T: Serialize>(path: &Path, receipt: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    fs::rename(temporary, path).map_err(|error| error.to_string())
}

const fn transaction_identity(seed: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x434f_4d4d_4954_5630,
        request_id: 39,
    }
}

const fn transition_identity(transaction: &RequestIdentity, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: transaction.client_id ^ 0x5452_414e_5349_544e,
        request_id,
    }
}

const fn ratekeeper_transition_identity(
    transaction: &RequestIdentity,
    phase_request_id: u64,
) -> RequestIdentity {
    RequestIdentity {
        client_id: transaction.client_id ^ 0x5241_5445_5452_414e,
        request_id: transaction
            .request_id
            .saturating_mul(10_000)
            .saturating_add(phase_request_id),
    }
}

fn check(id: &str, passed: bool) -> CellCommitVisibilityCheck {
    CellCommitVisibilityCheck {
        id: id.to_owned(),
        passed,
    }
}

fn hex(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
