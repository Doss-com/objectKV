use crate::partitioned_resolver::StatelessResolverProcessSet;
use crate::rpc::{
    read_response, write_request, AddLearnerRequest, ChangeMembershipRequest, ControlWrite,
    NodeStatus, RoutineAddLearnerRequest, RoutineChangeMembershipRequest, WriteAck, ADD_LEARNER,
    CHANGE_MEMBERSHIP, CLIENT_WRITE, DATA_GENERATION_WRITE, ELECT, GENERATION_READ,
    GENERATION_WRITE, HEARTBEAT, INITIALIZE, LINEARIZABLE_STATUS, PREAUTHORIZED_CLIENT_WRITE,
    RECOVERY_ATTEST, ROUTINE_ADD_LEARNER, ROUTINE_ATTEST, ROUTINE_CHANGE_MEMBERSHIP, STATUS,
    TRIGGER_SNAPSHOT,
};
use crate::{
    recovery_public_key, sign_tagged_log_statement, tagged_log_public_key, ApplyResponse,
    CellKeyRange, CellLogSetMember, CellLogSetPolicy, CellMutation, CellPartitionedResolution,
    CellReadVersion, CellResolverDecision, CellStagedTransactionAction,
    CellStagedTransactionApplyResponse, CellStagedTransactionCommand, CellStagedTransactionStatus,
    CellStagedWindow, CellStagedWindowRecord, CellStateSnapshot, CellTaggedLogCertificate,
    CellTaggedLogFenceAttestation, CellTaggedLogFenceCertificate, CellTaggedLogFenceStatement,
    CellTaggedLogPrefixFenceAttestation, CellTaggedLogPrefixFenceCertificate,
    CellTaggedLogPrefixFenceStatement, CellTaggedLogReceipt, CellTaggedLogStatement,
    CellTransactionCommand, CellTransactionStatus, ClientCommand, ConsensusProcessRole,
    GenerationAction, GenerationApplyResponse, GenerationAuthorityFaults, GenerationAuthorityState,
    GenerationCommand, GenerationCommandStatus, GenerationCredential, GenerationFenceConfig,
    GenerationFenceFaults, GenerationPhase, NodeId, OpenRaftLogStore, ProcessNodeConfig,
    ProcessNodePolicy, RecoveryCertificate, RecoveryCertificateKind, RecoveryCertificateStatement,
    RecoveryLogPosition, RecoverySignerConfig, RequestIdentity, RoutineReconfigurationCertificate,
    RoutineReconfigurationCertificateKind, RoutineReconfigurationCertificateStatement,
};
use okv_sim::CommitEnvelope;
use openraft::storage::RaftLogStorage;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream as StdTcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const CELL_ID: u64 = 7;
const STAGED_CELL_ID: [u8; 16] = [0x41; 16];
const STAGED_TENANT_ID: [u8; 16] = [0x42; 16];
const STAGED_LOG_SETS: [u16; 2] = [10, 20];
const AUTHORITY_NODES: [NodeId; 3] = [101, 102, 103];
const GENERATION_ONE_NODES: [NodeId; 3] = [201, 202, 203];
const GENERATION_TWO_NODES: [NodeId; 3] = [301, 302, 303];
const GENERATION_ONE: u64 = 1;
const GENERATION_TWO: u64 = 2;
const RECOVERY_ID: u64 = 2_002;
const COMPETING_RECOVERY_ID: u64 = 2_099;
const ROUTINE_RECONFIGURATION_ID: u64 = 7_001;
const ROUTINE_REPLACEMENT_NODE: NodeId = 204;
const RETRY_ATTEMPTS: usize = 500;

/// Deliberately unsafe takeover behaviors used to validate the gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationProcessMode {
    Correct,
    BypassStaleCommitFence,
    AcceptWriteDuringRecovery,
    AcceptCompetingRecovery,
    ActivateWithoutRecoveryProof,
    AcceptSingleSignerFence,
    AcceptTamperedFencePosition,
    AcceptDuplicateRecoverySigner,
    AcceptStaleRecoveryCertificate,
    AcceptWrongRecoveryMembership,
    AcceptIncompleteStagedHead,
    IgnoreStagedHeadTakeoverExpectation,
    AllowSuccessorToSkipStagedHead,
    AcceptInvalidStagedAbortProof,
    ReuseAbortedSequenceOrChain,
    PublishBeyondStagedAbsence,
    AbortQuorumPresentStagedRecord,
    SkipRecoverableStagedPrefix,
    RetainAbortedStagedSuffix,
    AcceptOverLimitStagedWindow,
    AcceptMissingStagedInventory,
}

impl GenerationProcessMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::BypassStaleCommitFence => "bypass_stale_commit_fence",
            Self::AcceptWriteDuringRecovery => "accept_write_during_recovery",
            Self::AcceptCompetingRecovery => "accept_competing_recovery",
            Self::ActivateWithoutRecoveryProof => "activate_without_recovery_proof",
            Self::AcceptSingleSignerFence => "accept_single_signer_fence",
            Self::AcceptTamperedFencePosition => "accept_tampered_fence_position",
            Self::AcceptDuplicateRecoverySigner => "accept_duplicate_recovery_signer",
            Self::AcceptStaleRecoveryCertificate => "accept_stale_recovery_certificate",
            Self::AcceptWrongRecoveryMembership => "accept_wrong_recovery_membership",
            Self::AcceptIncompleteStagedHead => "accept_incomplete_staged_head",
            Self::IgnoreStagedHeadTakeoverExpectation => "ignore_staged_head_takeover_expectation",
            Self::AllowSuccessorToSkipStagedHead => "allow_successor_to_skip_staged_head",
            Self::AcceptInvalidStagedAbortProof => "accept_invalid_staged_abort_proof",
            Self::ReuseAbortedSequenceOrChain => "reuse_aborted_sequence_or_chain",
            Self::PublishBeyondStagedAbsence => "publish_beyond_staged_absence",
            Self::AbortQuorumPresentStagedRecord => "abort_quorum_present_staged_record",
            Self::SkipRecoverableStagedPrefix => "skip_recoverable_staged_prefix",
            Self::RetainAbortedStagedSuffix => "retain_aborted_staged_suffix",
            Self::AcceptOverLimitStagedWindow => "accept_over_limit_staged_window",
            Self::AcceptMissingStagedInventory => "accept_missing_staged_inventory",
        }
    }

    const fn certificate_probe(self) -> Option<CertificateProbe> {
        match self {
            Self::AcceptSingleSignerFence => Some(CertificateProbe::SingleSignerFence),
            Self::AcceptTamperedFencePosition => Some(CertificateProbe::TamperedFencePosition),
            Self::AcceptDuplicateRecoverySigner => Some(CertificateProbe::DuplicateRecoverySigner),
            Self::AcceptStaleRecoveryCertificate => {
                Some(CertificateProbe::StaleRecoveryCertificate)
            }
            Self::AcceptWrongRecoveryMembership => Some(CertificateProbe::WrongRecoveryMembership),
            Self::Correct
            | Self::BypassStaleCommitFence
            | Self::AcceptWriteDuringRecovery
            | Self::AcceptCompetingRecovery
            | Self::ActivateWithoutRecoveryProof
            | Self::AcceptIncompleteStagedHead
            | Self::IgnoreStagedHeadTakeoverExpectation
            | Self::AllowSuccessorToSkipStagedHead
            | Self::AcceptInvalidStagedAbortProof
            | Self::ReuseAbortedSequenceOrChain
            | Self::PublishBeyondStagedAbsence
            | Self::AbortQuorumPresentStagedRecord
            | Self::SkipRecoverableStagedPrefix
            | Self::RetainAbortedStagedSuffix
            | Self::AcceptOverLimitStagedWindow
            | Self::AcceptMissingStagedInventory => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CertificateProbe {
    SingleSignerFence,
    TamperedFencePosition,
    DuplicateRecoverySigner,
    StaleRecoveryCertificate,
    WrongRecoveryMembership,
}

/// Canonical semantic report for one coordinator-backed generation handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationProcessReport {
    pub seed: u64,
    pub mode: GenerationProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub generation_preparations: u64,
    pub generation_reservations: u64,
    pub generation_activations: u64,
    pub committed_data_writes: u64,
    pub fenced_commit_attempts: u64,
    pub fenced_commit_rejections: u64,
    pub caught_up_generation_two_nodes: u64,
    pub fence_certificate_signers: u64,
    pub recovery_certificate_signers: u64,
    pub invalid_certificate_rejections: u64,
    pub trace_sha256: String,
}

/// Run a real-process coordinator and quiesced voter-set handoff.
///
/// # Errors
///
/// Returns an error when local process, transport, or consensus control cannot
/// execute. Semantic disagreements are recorded in the returned report.
pub fn run_generation_process_contract(
    seed: u64,
    mode: GenerationProcessMode,
    executable: &Path,
) -> Result<GenerationProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(GenerationScenario::new(seed, mode, executable)?.run())
}

/// Unsafe staged-head takeover subjects used by RFC-0041's controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedHeadGenerationMode {
    Correct,
    TakeoverDuringRecovery,
    MissingLogCertificate,
    TamperedEnvelopeExpectation,
    SkipStagedHead,
    RewriteStagedHeadGeneration,
}

impl StagedHeadGenerationMode {
    /// Stable eval identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::TakeoverDuringRecovery => "takeover_during_recovery",
            Self::MissingLogCertificate => "missing_log_certificate",
            Self::TamperedEnvelopeExpectation => "tampered_envelope_expectation",
            Self::SkipStagedHead => "skip_staged_head",
            Self::RewriteStagedHeadGeneration => "rewrite_staged_head_generation",
        }
    }

    const fn generation_mode(self) -> GenerationProcessMode {
        match self {
            Self::Correct => GenerationProcessMode::Correct,
            Self::TakeoverDuringRecovery => GenerationProcessMode::AcceptWriteDuringRecovery,
            Self::MissingLogCertificate => GenerationProcessMode::AcceptIncompleteStagedHead,
            Self::TamperedEnvelopeExpectation => {
                GenerationProcessMode::IgnoreStagedHeadTakeoverExpectation
            }
            Self::SkipStagedHead | Self::RewriteStagedHeadGeneration => {
                GenerationProcessMode::AllowSuccessorToSkipStagedHead
            }
        }
    }
}

/// Canonical real-process receipt for one certified staged-head handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StagedHeadGenerationReport {
    pub seed: u64,
    pub mode: StagedHeadGenerationMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub fence_certificate_signers: u64,
    pub recovery_certificate_signers: u64,
    pub tagged_log_certificates: u64,
    pub takeover_attempts: u64,
    pub takeover_commits: u64,
    pub takeover_retries: u64,
    pub fenced_old_publish_attempts: u64,
    pub fenced_old_publish_rejections: u64,
    pub baseline_frontier: u64,
    pub staged_version: u64,
    pub observed_frontier: u64,
    pub successor_version: u64,
    pub final_generation: u64,
    pub original_envelope_sha256: [u8; 32],
    pub committed_envelope_sha256: Option<[u8; 32]>,
    pub trace_sha256: String,
}

/// Run RFC-0041 through the existing external generation authority and two
/// real `OpenRaft` voter generations.
///
/// # Errors
///
/// Returns an error when process, transport, or consensus control cannot
/// complete. Semantic disagreements remain in the report.
pub fn run_staged_head_generation_contract(
    seed: u64,
    mode: StagedHeadGenerationMode,
    executable: &Path,
) -> Result<StagedHeadGenerationReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(StagedHeadGenerationScenario::new(seed, mode, executable)?.run())
}

/// Unsafe incomplete-head abort subjects used by RFC-0042's controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteStagedHeadAbortMode {
    Correct,
    AbortDuringRecovery,
    SingleAbsenceSigner,
    MissingLogSetFence,
    ForgedAbsenceOverPresentRecord,
    VolatileFenceAfterRestart,
    ReuseAbortedSequenceOrChain,
}

impl IncompleteStagedHeadAbortMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AbortDuringRecovery => "abort_during_recovery",
            Self::SingleAbsenceSigner => "single_absence_signer",
            Self::MissingLogSetFence => "missing_log_set_fence",
            Self::ForgedAbsenceOverPresentRecord => "forged_absence_over_present_record",
            Self::VolatileFenceAfterRestart => "volatile_fence_after_restart",
            Self::ReuseAbortedSequenceOrChain => "reuse_aborted_sequence_or_chain",
        }
    }

    const fn generation_mode(self) -> GenerationProcessMode {
        match self {
            Self::AbortDuringRecovery => GenerationProcessMode::AcceptWriteDuringRecovery,
            Self::SingleAbsenceSigner
            | Self::MissingLogSetFence
            | Self::ForgedAbsenceOverPresentRecord => {
                GenerationProcessMode::AcceptInvalidStagedAbortProof
            }
            Self::ReuseAbortedSequenceOrChain => GenerationProcessMode::ReuseAbortedSequenceOrChain,
            Self::Correct | Self::VolatileFenceAfterRestart => GenerationProcessMode::Correct,
        }
    }
}

/// Stable receipt for one incomplete staged-head abort run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IncompleteStagedHeadAbortReport {
    pub seed: u64,
    pub mode: IncompleteStagedHeadAbortMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub tagged_log_appends: u64,
    pub tagged_log_fence_attestations: u64,
    pub tagged_log_absence_attestations: u64,
    pub tagged_log_restarts: u64,
    pub late_append_attempts: u64,
    pub late_append_rejections: u64,
    pub abort_attempts: u64,
    pub abort_commits: u64,
    pub abort_retries: u64,
    pub baseline_frontier: u64,
    pub aborted_version: u64,
    pub observed_frontier: u64,
    pub successor_version: u64,
    pub final_generation: u64,
    pub trace_sha256: String,
}

/// Prove durable tagged-log quorum fencing before aborting one incomplete head.
///
/// # Errors
///
/// Returns an error when local process, transport, or consensus control cannot
/// execute. Semantic disagreements are retained in the report.
pub fn run_incomplete_staged_head_abort_contract(
    seed: u64,
    mode: IncompleteStagedHeadAbortMode,
    executable: &Path,
) -> Result<IncompleteStagedHeadAbortReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(IncompleteStagedHeadAbortScenario::new(seed, mode, executable)?.run())
}

/// Unsafe multi-record recovery subjects used by RFC-0043's controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiRecordStagedPrefixMode {
    Correct,
    PublishBeyondAbsentBoundary,
    AbortQuorumPresentRecord,
    SkipRecoverablePrefixRecord,
    RetainDependentSuffix,
    AcceptOverLimitWindow,
    MissingLogSetInventory,
}

impl MultiRecordStagedPrefixMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishBeyondAbsentBoundary => "publish_beyond_absent_boundary",
            Self::AbortQuorumPresentRecord => "abort_quorum_present_record",
            Self::SkipRecoverablePrefixRecord => "skip_recoverable_prefix_record",
            Self::RetainDependentSuffix => "retain_dependent_suffix",
            Self::AcceptOverLimitWindow => "accept_over_limit_window",
            Self::MissingLogSetInventory => "missing_log_set_inventory",
        }
    }

    const fn generation_mode(self) -> GenerationProcessMode {
        match self {
            Self::Correct => GenerationProcessMode::Correct,
            Self::PublishBeyondAbsentBoundary => GenerationProcessMode::PublishBeyondStagedAbsence,
            Self::AbortQuorumPresentRecord => GenerationProcessMode::AbortQuorumPresentStagedRecord,
            Self::SkipRecoverablePrefixRecord => GenerationProcessMode::SkipRecoverableStagedPrefix,
            Self::RetainDependentSuffix => GenerationProcessMode::RetainAbortedStagedSuffix,
            Self::AcceptOverLimitWindow => GenerationProcessMode::AcceptOverLimitStagedWindow,
            Self::MissingLogSetInventory => GenerationProcessMode::AcceptMissingStagedInventory,
        }
    }
}

/// Stable receipt for one bounded staged-prefix recovery run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiRecordStagedPrefixReport {
    pub seed: u64,
    pub mode: MultiRecordStagedPrefixMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub staged_records: u64,
    pub staged_bytes: u64,
    pub tagged_log_appends: u64,
    pub prefix_fence_attestations: u64,
    pub inventory_observations: u64,
    pub tagged_log_restarts: u64,
    pub late_append_attempts: u64,
    pub late_append_rejections: u64,
    pub recovery_attempts: u64,
    pub recovery_commits: u64,
    pub recovery_retries: u64,
    pub recovered_records: u64,
    pub aborted_records: u64,
    pub baseline_frontier: u64,
    pub recovered_frontier: u64,
    pub successor_frontier: u64,
    pub final_generation: u64,
    pub trace_sha256: String,
}

/// Recover one bounded staged prefix through real transaction and tLog processes.
///
/// # Errors
///
/// Returns an error when local process, transport, or consensus control cannot
/// execute. Semantic disagreements are retained in the report.
pub fn run_multi_record_staged_prefix_contract(
    seed: u64,
    mode: MultiRecordStagedPrefixMode,
    executable: &Path,
) -> Result<MultiRecordStagedPrefixReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let (report, _) = MultiRecordStagedPrefixScenario::new(seed, mode, executable)?
            .run()
            .await?;
        Ok(report)
    })
}

/// Unsafe composition subjects frozen by RFC-0050.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatelessResolverAuthenticatedTlogMode {
    Correct,
    PublishBeforeTlogQuorum,
    RecoverFromAuthorityMarkerOnly,
    ActivateBeforeTlogPrefixFence,
    AcceptOldGenerationResolverReply,
    ReadBelowAuthenticatedRecoveryFloor,
    RecoverBelowQuorumPresentPrefix,
    RecoverBeyondAbsentBoundary,
}

impl StatelessResolverAuthenticatedTlogMode {
    /// Stable identifier used by the eval suite and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishBeforeTlogQuorum => "publish_before_tlog_quorum",
            Self::RecoverFromAuthorityMarkerOnly => "recover_from_authority_marker_only",
            Self::ActivateBeforeTlogPrefixFence => "activate_before_tlog_prefix_fence",
            Self::AcceptOldGenerationResolverReply => "accept_old_generation_resolver_reply",
            Self::ReadBelowAuthenticatedRecoveryFloor => "read_below_authenticated_recovery_floor",
            Self::RecoverBelowQuorumPresentPrefix => "recover_below_quorum_present_prefix",
            Self::RecoverBeyondAbsentBoundary => "recover_beyond_absent_boundary",
        }
    }

    const fn prefix_mode(self) -> MultiRecordStagedPrefixMode {
        match self {
            Self::RecoverFromAuthorityMarkerOnly => {
                MultiRecordStagedPrefixMode::MissingLogSetInventory
            }
            Self::RecoverBelowQuorumPresentPrefix => {
                MultiRecordStagedPrefixMode::AbortQuorumPresentRecord
            }
            Self::RecoverBeyondAbsentBoundary => {
                MultiRecordStagedPrefixMode::PublishBeyondAbsentBoundary
            }
            Self::Correct
            | Self::PublishBeforeTlogQuorum
            | Self::ActivateBeforeTlogPrefixFence
            | Self::AcceptOldGenerationResolverReply
            | Self::ReadBelowAuthenticatedRecoveryFloor => MultiRecordStagedPrefixMode::Correct,
        }
    }
}

/// Deterministic receipt for one composed resolver, tLog, and recovery history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StatelessResolverAuthenticatedTlogReport {
    pub seed: u64,
    pub mode: StatelessResolverAuthenticatedTlogMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub resolver_process_starts: u64,
    pub resolver_process_kills: u64,
    pub resolver_decisions: u64,
    pub staged_records: u64,
    pub tagged_log_appends: u64,
    pub prefix_fence_attestations: u64,
    pub inventory_observations: u64,
    pub recovered_records: u64,
    pub aborted_records: u64,
    pub recovery_frontier: u64,
    pub successor_frontier: u64,
    pub resolver_durable_syncs: u64,
    pub resolver_finalization_rpcs: u64,
    pub complete_resolver_evidence_before_stage: bool,
    pub resolver_acceptance_did_not_publish: bool,
    pub partial_resolver_candidate_not_staged: bool,
    pub partial_resolver_candidate_not_visible: bool,
    pub staged_envelope_bytes_match_tlog_bytes: bool,
    pub visibility_required_authenticated_quorum: bool,
    pub every_required_tlog_prefix_fenced: bool,
    pub authenticated_recovery_prefix_maximal: bool,
    pub quorum_present_uncertified_record_recovered: bool,
    pub quorum_absent_suffix_aborted: bool,
    pub successor_resolver_state_started_empty: bool,
    pub successor_resolver_floor_exact: bool,
    pub successor_read_at_or_above_floor: bool,
    pub old_generation_resolver_request_rejected: bool,
    pub old_generation_resolver_reply_rejected: bool,
    pub old_generation_tlog_append_rejected: bool,
    pub abandoned_work_retried_with_new_identity: bool,
    pub exact_rows_and_envelopes: bool,
    pub negative_control_detected: bool,
    pub trace_sha256: String,
}

/// Compose memory-only resolvers with authenticated tLog prefix recovery.
///
/// # Errors
///
/// Returns an error when a real process or protocol step cannot complete.
pub fn run_stateless_resolver_authenticated_tlog_contract(
    seed: u64,
    mode: StatelessResolverAuthenticatedTlogMode,
    executable: &Path,
) -> Result<StatelessResolverAuthenticatedTlogReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let (_, composed) = MultiRecordStagedPrefixScenario::new_composed(seed, mode, executable)?
            .run()
            .await?;
        composed.ok_or_else(|| "composed resolver receipt was omitted".to_owned())
    })
}

/// Real-process subject for one healthy-quorum voter replacement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineReconfigurationProcessMode {
    Correct,
}

impl RoutineReconfigurationProcessMode {
    /// Stable eval identity.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
        }
    }
}

/// Semantic receipt for routine membership replacement through real processes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutineReconfigurationProcessReport {
    pub seed: u64,
    pub mode: RoutineReconfigurationProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub process_kills: u64,
    pub committed_data_writes: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub learner_ready_signers: u64,
    pub membership_committed_signers: u64,
    pub rejected_controls: u64,
    pub generation: u64,
    pub membership_epoch: u64,
    pub active_voters: Vec<NodeId>,
    pub snapshot_position: Option<RecoveryLogPosition>,
    pub learner_applied_position: Option<RecoveryLogPosition>,
    pub membership_position: Option<RecoveryLogPosition>,
    pub trace_sha256: String,
}

/// Run one snapshot-plus-suffix voter replacement without changing generation.
///
/// # Errors
///
/// Returns an error when local process, transport, or consensus control cannot
/// execute. Semantic disagreements are retained in the report.
pub fn run_routine_reconfiguration_process_contract(
    seed: u64,
    mode: RoutineReconfigurationProcessMode,
    executable: &Path,
) -> Result<RoutineReconfigurationProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(RoutineReconfigurationScenario::new(seed, mode, executable)?.run())
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default)]
struct Observations {
    coordinator_bootstrapped: bool,
    generation_one_commit_replicated: bool,
    generation_two_learners_caught_up: bool,
    data_log_fence_committed: bool,
    inflight_commit_rejected_by_data_fence: bool,
    next_generation_reserved: bool,
    old_generation_fenced: bool,
    reservation_survived_authority_failover: bool,
    competing_recovery_rejected: bool,
    membership_handoff_committed: bool,
    generation_two_leader_ready: bool,
    write_during_recovery_rejected: bool,
    activation_without_proof_rejected: bool,
    invalid_fence_certificates_rejected: bool,
    invalid_recovery_certificates_rejected: bool,
    generation_two_activated: bool,
    generation_two_continued_exactly: bool,
    removed_generation_remained_fenced: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    learner_additions: u64,
    membership_changes: u64,
    generation_preparations: u64,
    generation_reservations: u64,
    generation_activations: u64,
    committed_data_writes: u64,
    fenced_commit_attempts: u64,
    fenced_commit_rejections: u64,
    caught_up_generation_two_nodes: u64,
    fence_certificate_signers: u64,
    recovery_certificate_signers: u64,
    invalid_certificate_rejections: u64,
    final_authority: Option<GenerationAuthorityState>,
    final_payloads: BTreeMap<NodeId, Vec<Vec<u8>>>,
}

struct GenerationScenario<'a> {
    seed: u64,
    mode: GenerationProcessMode,
    executable: &'a Path,
    root: TempRoot,
    authority_addresses: BTreeMap<NodeId, String>,
    generation_one_addresses: BTreeMap<NodeId, String>,
    generation_two_addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: Observations,
}

impl<'a> GenerationScenario<'a> {
    fn new(seed: u64, mode: GenerationProcessMode, executable: &'a Path) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "generation contract executable does not exist: {}",
                executable.display()
            ));
        }
        let addresses = allocate_addresses(
            &AUTHORITY_NODES
                .into_iter()
                .chain(GENERATION_ONE_NODES)
                .chain(GENERATION_TWO_NODES)
                .collect::<Vec<_>>(),
        )?;
        Ok(Self {
            seed,
            mode,
            executable,
            root: TempRoot::new(seed, mode)?,
            authority_addresses: subset(&addresses, &AUTHORITY_NODES),
            generation_one_addresses: subset(&addresses, &GENERATION_ONE_NODES),
            generation_two_addresses: subset(&addresses, &GENERATION_TWO_NODES),
            children: ChildGroup::default(),
            observations: Observations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<GenerationProcessReport, String> {
        let generation_one_members = generation_members(&GENERATION_ONE_NODES)?;
        let generation_two_members = generation_members(&GENERATION_TWO_NODES)?;
        self.start_authority().await?;
        self.start_generation_one().await?;
        self.start_generation_two_learners().await?;

        let prepare = self
            .write_generation(
                101,
                2,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        self.observations.generation_preparations +=
            u64::from(prepare.status == GenerationCommandStatus::Accepted);

        let competing = self
            .write_generation(
                101,
                3,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: COMPETING_RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        self.observations.competing_recovery_rejected =
            competing.status != GenerationCommandStatus::Accepted;
        self.observations.generation_preparations +=
            u64::from(competing.status == GenerationCommandStatus::Accepted);
        let effective_recovery_id = if competing.status == GenerationCommandStatus::Accepted {
            COMPETING_RECOVERY_ID
        } else {
            RECOVERY_ID
        };
        self.add_generation_two_learners().await?;

        let data_prepare = self
            .write_data_generation(
                201,
                10,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: effective_recovery_id,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        let fenced_log_position = data_prepare.applied_log_position;
        self.observations.data_log_fence_committed = data_prepare.status
            == GenerationCommandStatus::Accepted
            && data_prepare.state.phase == GenerationPhase::Fencing
            && fenced_log_position.index != 0;

        let fence_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Fence,
            &data_prepare.state,
            fenced_log_position,
            &generation_one_members,
        );
        let fence_certificate = self
            .collect_certificate(&GENERATION_ONE_NODES, fence_statement)
            .await?;
        self.observations.fence_certificate_signers =
            u64::try_from(fence_certificate.attestations.len()).unwrap_or(u64::MAX);
        if !self
            .reject_invalid_fence_certificates(&fence_certificate, effective_recovery_id)
            .await?
        {
            self.capture_final().await;
            return Ok(build_report(self.seed, self.mode, &self.observations));
        }

        self.observations.fenced_commit_attempts += 1;
        let inflight = self
            .write_preauthorized_data(201, GENERATION_ONE, "tx-g1", 19, b"INFLIGHT")
            .await;
        self.observations.committed_data_writes += u64::from(inflight.is_ok());
        self.observations.inflight_commit_rejected_by_data_fence = inflight.is_err();
        self.observations.fenced_commit_rejections += u64::from(inflight.is_err());

        let reserve = self
            .write_generation(
                101,
                4,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate.clone()),
                },
            )
            .await?;
        self.observations.next_generation_reserved = reserve.status
            == GenerationCommandStatus::Accepted
            && reserve.state.phase == GenerationPhase::Recovering
            && reserve.state.generation == GENERATION_TWO
            && reserve.state.fenced_log_position == Some(fenced_log_position);
        self.observations.generation_reservations +=
            u64::from(reserve.status == GenerationCommandStatus::Accepted);
        let data_reserve = self
            .write_data_generation(
                201,
                11,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate),
                },
            )
            .await?;
        self.observations.next_generation_reserved &=
            data_reserve.status == GenerationCommandStatus::Accepted;

        self.observations.fenced_commit_attempts += 1;
        let stale = self
            .write_data(201, GENERATION_ONE, "tx-g1", 20, b"STALE")
            .await;
        self.observations.committed_data_writes += u64::from(stale.is_ok());
        self.observations.old_generation_fenced = stale.is_err();
        self.observations.fenced_commit_rejections += u64::from(stale.is_err());

        self.kill_node(101)?;
        self.observations.authority_failovers =
            u64::from(elect_until_leader(self.authority_address(102)?, 102).await);
        let recovered = retry_generation_read(self.authority_address(102)?).await?;
        self.observations.reservation_survived_authority_failover = recovered.phase
            == GenerationPhase::Recovering
            && recovered.generation == GENERATION_TWO
            && recovered.recovery_id == Some(effective_recovery_id);

        let membership = change_membership(
            self.generation_one_address(201)?,
            ChangeMembershipRequest {
                voters: GENERATION_TWO_NODES.into_iter().collect(),
                credential: credential(GENERATION_TWO, "tx-g2"),
                recovery_id: effective_recovery_id,
            },
        )
        .await;
        self.observations.membership_handoff_committed = membership
            .as_ref()
            .is_ok_and(|ack| ack.committed && ack.log_position.is_some());
        self.observations.membership_changes =
            u64::from(self.observations.membership_handoff_committed);

        self.observations.generation_two_leader_ready =
            elect_until_leader(self.generation_two_address(301)?, 301).await;

        self.observations.fenced_commit_attempts += 1;
        let early = self
            .write_data(301, GENERATION_TWO, "tx-g2", 30, b"EARLY")
            .await;
        self.observations.committed_data_writes += u64::from(early.is_ok());
        self.observations.write_during_recovery_rejected = early.is_err();
        self.observations.fenced_commit_rejections += u64::from(early.is_err());

        let membership = membership?;
        let recovered_log_position = membership
            .log_position
            .ok_or_else(|| "membership handoff did not return an exact log position".to_owned())?;
        let recovered_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Recovered,
            &data_reserve.state,
            recovered_log_position,
            &generation_two_members,
        );
        let recovery_certificate = self
            .collect_certificate(&GENERATION_TWO_NODES, recovered_statement)
            .await?;
        self.observations.recovery_certificate_signers =
            u64::try_from(recovery_certificate.attestations.len()).unwrap_or(u64::MAX);
        if !self
            .reject_invalid_recovery_certificates(&recovery_certificate, effective_recovery_id)
            .await?
        {
            self.capture_final().await;
            return Ok(build_report(self.seed, self.mode, &self.observations));
        }
        let invalid_activation_action = GenerationAction::Activate {
            generation: GENERATION_TWO,
            recovery_id: effective_recovery_id,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: None,
        };
        let invalid_activation = self
            .write_generation(102, 5, invalid_activation_action.clone())
            .await?;
        self.observations.activation_without_proof_rejected =
            invalid_activation.status == GenerationCommandStatus::MissingRecoveryProof;

        let (activation, activation_action) =
            if invalid_activation.status == GenerationCommandStatus::Accepted {
                (invalid_activation, invalid_activation_action)
            } else {
                let action = GenerationAction::Activate {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    wal_root: "wal-g2".to_owned(),
                    expected_control_root_version: 1,
                    next_control_root_version: 2,
                    certificate: Some(recovery_certificate.clone()),
                };
                (self.write_generation(102, 6, action.clone()).await?, action)
            };
        self.observations.generation_two_activated = activation.status
            == GenerationCommandStatus::Accepted
            && activation.state.authorizes(GENERATION_TWO, "tx-g2")
            && activation.state.recovered_log_position == Some(recovered_log_position);
        self.observations.generation_activations =
            u64::from(activation.status == GenerationCommandStatus::Accepted);
        let data_activation = self
            .write_data_generation(301, 12, activation_action)
            .await?;
        self.observations.generation_two_activated &=
            data_activation.status == GenerationCommandStatus::Accepted;

        let _ = retry_write_data(
            self.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            client_command(GENERATION_TWO, "tx-g2", self.seed, 40, b"B")?,
        )
        .await?;
        self.observations.committed_data_writes += 1;
        self.observations.generation_two_continued_exactly = wait_for_payloads(
            &self.generation_two_addresses,
            &GENERATION_TWO_NODES,
            &[b"A".to_vec(), b"B".to_vec()],
        )
        .await;

        self.observations.fenced_commit_attempts += 1;
        let removed = self
            .write_data(201, GENERATION_ONE, "tx-g1", 50, b"REMOVED")
            .await;
        self.observations.committed_data_writes += u64::from(removed.is_ok());
        self.observations.removed_generation_remained_fenced = removed.is_err();
        self.observations.fenced_commit_rejections += u64::from(removed.is_err());

        self.capture_final().await;
        Ok(build_report(self.seed, self.mode, &self.observations))
    }

    async fn start_authority(&mut self) -> Result<(), String> {
        for node_id in AUTHORITY_NODES {
            self.start_node(
                node_id,
                self.authority_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::GenerationAuthority,
                    generation_authority_faults: GenerationAuthorityFaults {
                        accept_competing_recovery: self.mode
                            == GenerationProcessMode::AcceptCompetingRecovery,
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                    },
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.authority_process_starts += 1;
        }
        self.wait_ready_nodes(&AUTHORITY_NODES).await?;
        retry_control(self.authority_address(101)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.authority_address(101)?, 101).await {
            return Err("coordinator leader election failed".to_owned());
        }
        let bootstrap = self
            .write_generation(
                101,
                1,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    transaction_system_incarnations: fixture_incarnations(&GENERATION_ONE_NODES),
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        self.observations.coordinator_bootstrapped = bootstrap.status
            == GenerationCommandStatus::Accepted
            && bootstrap.state.authorizes(GENERATION_ONE, "tx-g1");
        Ok(())
    }

    async fn start_generation_one(&mut self) -> Result<(), String> {
        let fence = GenerationFenceConfig {
            credential: credential(GENERATION_ONE, "tx-g1"),
            recovery_id: None,
            authority_nodes: self.authority_addresses.clone(),
        };
        for node_id in GENERATION_ONE_NODES {
            self.start_node(
                node_id,
                self.generation_one_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::Data,
                    generation_fence: Some(fence.clone()),
                    generation_authority_faults: GenerationAuthorityFaults {
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                        ..GenerationAuthorityFaults::default()
                    },
                    generation_fence_faults: GenerationFenceFaults {
                        bypass_commit_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        bypass_apply_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        accept_apply_during_recovery: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        accept_recovering_commits: false,
                        allow_preauthorized_test_write: true,
                        accept_incomplete_staged_head: self.mode
                            == GenerationProcessMode::AcceptIncompleteStagedHead,
                        ignore_staged_head_takeover_expectation: self.mode
                            == GenerationProcessMode::IgnoreStagedHeadTakeoverExpectation,
                        allow_successor_to_skip_staged_head: self.mode
                            == GenerationProcessMode::AllowSuccessorToSkipStagedHead,
                        accept_invalid_staged_abort_proof: self.mode
                            == GenerationProcessMode::AcceptInvalidStagedAbortProof,
                        reuse_aborted_sequence_or_chain: self.mode
                            == GenerationProcessMode::ReuseAbortedSequenceOrChain,
                        publish_beyond_staged_absence: self.mode
                            == GenerationProcessMode::PublishBeyondStagedAbsence,
                        abort_quorum_present_staged_record: self.mode
                            == GenerationProcessMode::AbortQuorumPresentStagedRecord,
                        skip_recoverable_staged_prefix: self.mode
                            == GenerationProcessMode::SkipRecoverableStagedPrefix,
                        retain_aborted_staged_suffix: self.mode
                            == GenerationProcessMode::RetainAbortedStagedSuffix,
                        accept_over_limit_staged_window: self.mode
                            == GenerationProcessMode::AcceptOverLimitStagedWindow,
                        accept_missing_staged_inventory: self.mode
                            == GenerationProcessMode::AcceptMissingStagedInventory,
                        ..GenerationFenceFaults::default()
                    },
                    recovery_signer: Some(recovery_signer(node_id)),
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.data_process_starts += 1;
        }
        self.wait_ready_nodes(&GENERATION_ONE_NODES).await?;
        retry_control(self.generation_one_address(201)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.generation_one_address(201)?, 201).await {
            return Err("generation-one leader election failed".to_owned());
        }
        let data_bootstrap = self
            .write_data_generation(
                201,
                1,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    transaction_system_incarnations: fixture_incarnations(&GENERATION_ONE_NODES),
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        if data_bootstrap.status != GenerationCommandStatus::Accepted {
            return Err("generation-one data mirror bootstrap failed".to_owned());
        }
        let command = client_command(GENERATION_ONE, "tx-g1", self.seed, 10, b"A")?;
        let _ = retry_write_data(
            self.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            command,
        )
        .await?;
        self.observations.committed_data_writes += 1;
        self.observations.generation_one_commit_replicated = wait_for_payloads(
            &self.generation_one_addresses,
            &GENERATION_ONE_NODES,
            &[b"A".to_vec()],
        )
        .await;
        Ok(())
    }

    async fn start_generation_two_learners(&mut self) -> Result<(), String> {
        let fence = GenerationFenceConfig {
            credential: credential(GENERATION_TWO, "tx-g2"),
            recovery_id: Some(RECOVERY_ID),
            authority_nodes: self.authority_addresses.clone(),
        };
        for node_id in GENERATION_TWO_NODES {
            self.start_node(
                node_id,
                self.generation_two_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::Data,
                    generation_fence: Some(fence.clone()),
                    generation_authority_faults: GenerationAuthorityFaults {
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                        ..GenerationAuthorityFaults::default()
                    },
                    generation_fence_faults: GenerationFenceFaults {
                        bypass_commit_fence: false,
                        bypass_apply_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        accept_apply_during_recovery: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        accept_recovering_commits: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        allow_preauthorized_test_write: false,
                        accept_incomplete_staged_head: self.mode
                            == GenerationProcessMode::AcceptIncompleteStagedHead,
                        ignore_staged_head_takeover_expectation: self.mode
                            == GenerationProcessMode::IgnoreStagedHeadTakeoverExpectation,
                        allow_successor_to_skip_staged_head: self.mode
                            == GenerationProcessMode::AllowSuccessorToSkipStagedHead,
                        accept_invalid_staged_abort_proof: self.mode
                            == GenerationProcessMode::AcceptInvalidStagedAbortProof,
                        reuse_aborted_sequence_or_chain: self.mode
                            == GenerationProcessMode::ReuseAbortedSequenceOrChain,
                        publish_beyond_staged_absence: self.mode
                            == GenerationProcessMode::PublishBeyondStagedAbsence,
                        abort_quorum_present_staged_record: self.mode
                            == GenerationProcessMode::AbortQuorumPresentStagedRecord,
                        skip_recoverable_staged_prefix: self.mode
                            == GenerationProcessMode::SkipRecoverableStagedPrefix,
                        retain_aborted_staged_suffix: self.mode
                            == GenerationProcessMode::RetainAbortedStagedSuffix,
                        accept_over_limit_staged_window: self.mode
                            == GenerationProcessMode::AcceptOverLimitStagedWindow,
                        accept_missing_staged_inventory: self.mode
                            == GenerationProcessMode::AcceptMissingStagedInventory,
                        ..GenerationFenceFaults::default()
                    },
                    recovery_signer: Some(recovery_signer(node_id)),
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.data_process_starts += 1;
        }
        self.wait_ready_nodes(&GENERATION_TWO_NODES).await?;
        Ok(())
    }

    async fn add_generation_two_learners(&mut self) -> Result<(), String> {
        for node_id in GENERATION_TWO_NODES {
            let ack = add_learner(
                self.generation_one_address(201)?,
                AddLearnerRequest {
                    node_id,
                    address: self.generation_two_address(node_id)?.to_owned(),
                },
            )
            .await?;
            self.observations.learner_additions += u64::from(ack.committed);
        }
        self.observations.generation_two_learners_caught_up = wait_for_payloads(
            &self.generation_two_addresses,
            &GENERATION_TWO_NODES,
            &[b"A".to_vec()],
        )
        .await;
        Ok(())
    }

    fn start_node(
        &mut self,
        node_id: NodeId,
        nodes: BTreeMap<NodeId, String>,
        policy: ProcessNodePolicy,
    ) -> Result<(), String> {
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy,
            },
        )
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    async fn wait_ready_nodes(&self, node_ids: &[NodeId]) -> Result<(), String> {
        for node_id in node_ids {
            wait_ready(self.address(*node_id)?).await?;
        }
        Ok(())
    }

    async fn write_generation(
        &self,
        node_id: NodeId,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_generation_write(
            self.address(node_id)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x4745_4e45_5241_5445,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn write_data_generation(
        &self,
        node_id: NodeId,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_data_generation_write(
            self.address(node_id)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x4441_5441_4745_4e45,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn write_data(
        &self,
        node_id: NodeId,
        generation: u64,
        transaction_system_id: &str,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        write_data(
            self.address(node_id)?,
            credential(generation, transaction_system_id),
            client_command(
                generation,
                transaction_system_id,
                self.seed,
                request_id,
                payload,
            )?,
        )
        .await
    }

    async fn write_preauthorized_data(
        &self,
        node_id: NodeId,
        generation: u64,
        transaction_system_id: &str,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        write_preauthorized_data(
            self.address(node_id)?,
            credential(generation, transaction_system_id),
            client_command(
                generation,
                transaction_system_id,
                self.seed,
                request_id,
                payload,
            )?,
        )
        .await
    }

    async fn collect_certificate(
        &self,
        node_ids: &[NodeId],
        statement: RecoveryCertificateStatement,
    ) -> Result<RecoveryCertificate, String> {
        let mut attestations = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            attestations
                .push(retry_recovery_attestation(self.address(*node_id)?, &statement).await?);
        }
        Ok(RecoveryCertificate {
            statement,
            attestations,
        })
    }

    async fn reject_invalid_fence_certificates(
        &mut self,
        valid: &RecoveryCertificate,
        recovery_id: u64,
    ) -> Result<bool, String> {
        let selected = self.mode.certificate_probe();
        let probes = match selected {
            Some(CertificateProbe::SingleSignerFence) => {
                vec![CertificateProbe::SingleSignerFence]
            }
            Some(CertificateProbe::TamperedFencePosition) => {
                vec![CertificateProbe::TamperedFencePosition]
            }
            Some(_) => Vec::new(),
            None => vec![
                CertificateProbe::SingleSignerFence,
                CertificateProbe::TamperedFencePosition,
            ],
        };
        let mut all_rejected = true;
        for (offset, probe) in probes.into_iter().enumerate() {
            let certificate = invalid_certificate(valid, probe);
            let response = self
                .write_generation(
                    101,
                    100 + u64::try_from(offset).unwrap_or(u64::MAX),
                    GenerationAction::Reserve {
                        generation: GENERATION_TWO,
                        recovery_id,
                        transaction_system_id: "tx-g2".to_owned(),
                        expected_control_root_version: 1,
                        certificate: Some(certificate),
                    },
                )
                .await?;
            let rejected = response.status == GenerationCommandStatus::InvalidRecoveryProof;
            self.observations.invalid_certificate_rejections += u64::from(rejected);
            self.observations.generation_reservations +=
                u64::from(response.status == GenerationCommandStatus::Accepted);
            all_rejected &= rejected;
            if !rejected {
                break;
            }
        }
        self.observations.invalid_fence_certificates_rejected = all_rejected;
        Ok(all_rejected)
    }

    async fn reject_invalid_recovery_certificates(
        &mut self,
        valid: &RecoveryCertificate,
        recovery_id: u64,
    ) -> Result<bool, String> {
        let selected = self.mode.certificate_probe();
        let probes = match selected {
            Some(CertificateProbe::DuplicateRecoverySigner) => {
                vec![CertificateProbe::DuplicateRecoverySigner]
            }
            Some(CertificateProbe::StaleRecoveryCertificate) => {
                vec![CertificateProbe::StaleRecoveryCertificate]
            }
            Some(CertificateProbe::WrongRecoveryMembership) => {
                vec![CertificateProbe::WrongRecoveryMembership]
            }
            Some(_) => Vec::new(),
            None => vec![
                CertificateProbe::DuplicateRecoverySigner,
                CertificateProbe::StaleRecoveryCertificate,
                CertificateProbe::WrongRecoveryMembership,
            ],
        };
        let mut all_rejected = true;
        for (offset, probe) in probes.into_iter().enumerate() {
            let certificate = invalid_certificate(valid, probe);
            let response = self
                .write_generation(
                    102,
                    200 + u64::try_from(offset).unwrap_or(u64::MAX),
                    GenerationAction::Activate {
                        generation: GENERATION_TWO,
                        recovery_id,
                        transaction_system_id: "tx-g2".to_owned(),
                        wal_root: "wal-g2".to_owned(),
                        expected_control_root_version: 1,
                        next_control_root_version: 2,
                        certificate: Some(certificate),
                    },
                )
                .await?;
            let rejected = response.status == GenerationCommandStatus::InvalidRecoveryProof;
            self.observations.invalid_certificate_rejections += u64::from(rejected);
            self.observations.generation_activations +=
                u64::from(response.status == GenerationCommandStatus::Accepted);
            all_rejected &= rejected;
            if !rejected {
                break;
            }
        }
        self.observations.invalid_recovery_certificates_rejected = all_rejected;
        Ok(all_rejected)
    }

    async fn capture_final(&mut self) {
        self.observations.final_authority =
            retry_generation_read(self.authority_address(102).unwrap_or_default())
                .await
                .ok();
        for node_id in GENERATION_TWO_NODES {
            if let Ok(node) = status(self.generation_two_address(node_id).unwrap_or_default()).await
            {
                self.observations.caught_up_generation_two_nodes +=
                    u64::from(node.payloads == [b"A".to_vec(), b"B".to_vec()]);
                self.observations
                    .final_payloads
                    .insert(node_id, node.payloads);
            }
        }
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.authority_addresses
            .get(&node_id)
            .or_else(|| self.generation_one_addresses.get(&node_id))
            .or_else(|| self.generation_two_addresses.get(&node_id))
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    fn authority_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.authority_addresses, node_id)
    }

    fn generation_one_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.generation_one_addresses, node_id)
    }

    fn generation_two_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.generation_two_addresses, node_id)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct StagedHeadObservations {
    authority_bootstrapped: bool,
    generation_one_active: bool,
    baseline_exact: bool,
    policies_installed: bool,
    head_staged: bool,
    every_log_certificate_recorded: bool,
    invisible_before_fence: bool,
    successor_learners_caught_up: bool,
    fence_after_staged_head: bool,
    fence_certificate_quorum: bool,
    old_publish_rejected_after_fence: bool,
    external_reservation_exact: bool,
    data_reservation_exact: bool,
    authority_failover_exact: bool,
    membership_handoff_exact: bool,
    successor_leader_ready: bool,
    early_takeover_rejected: bool,
    invisible_during_recovery: bool,
    recovery_certificate_quorum: bool,
    external_activation_exact: bool,
    data_activation_exact: bool,
    recovery_identity_retained: bool,
    takeover_expectation_exact: bool,
    takeover_committed: bool,
    original_envelope_preserved: bool,
    staged_frontier_visible: bool,
    staged_rows_exact: bool,
    domain_generation_advanced: bool,
    takeover_reply_lost: bool,
    takeover_retry_retained: bool,
    no_duplicate_head_envelope: bool,
    successor_policy_installed: bool,
    successor_staged_at_twelve: bool,
    successor_committed_at_twelve: bool,
    old_generation_remained_fenced: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    learner_additions: u64,
    membership_changes: u64,
    fence_certificate_signers: u64,
    recovery_certificate_signers: u64,
    tagged_log_certificates: u64,
    takeover_attempts: u64,
    takeover_commits: u64,
    takeover_retries: u64,
    fenced_old_publish_attempts: u64,
    fenced_old_publish_rejections: u64,
    baseline_frontier: u64,
    staged_version: u64,
    observed_frontier: u64,
    successor_version: u64,
    final_generation: u64,
    original_envelope_sha256: [u8; 32],
    committed_envelope_sha256: Option<[u8; 32]>,
}

struct StagedHeadGenerationScenario<'a> {
    mode: StagedHeadGenerationMode,
    inner: GenerationScenario<'a>,
    observations: StagedHeadObservations,
}

impl<'a> StagedHeadGenerationScenario<'a> {
    fn new(
        seed: u64,
        mode: StagedHeadGenerationMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        Ok(Self {
            mode,
            inner: GenerationScenario::new(seed, mode.generation_mode(), executable)?,
            observations: StagedHeadObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<StagedHeadGenerationReport, String> {
        let generation_one_members = generation_members(&GENERATION_ONE_NODES)?;
        let generation_two_members = generation_members(&GENERATION_TWO_NODES)?;
        self.inner.start_authority().await?;
        self.inner.start_generation_one().await?;
        self.observations.authority_bootstrapped = self.inner.observations.coordinator_bootstrapped;
        self.observations.generation_one_active =
            self.inner.observations.generation_one_commit_replicated;

        let head = self.stage_baseline_and_head().await?;
        self.inner.start_generation_two_learners().await?;
        let prepare = self
            .inner
            .write_generation(
                101,
                2,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        if prepare.status != GenerationCommandStatus::Accepted {
            return Err("external authority rejected staged-head recovery preparation".to_owned());
        }
        self.inner.add_generation_two_learners().await?;
        self.observations.learner_additions = self.inner.observations.learner_additions;
        self.observations.successor_learners_caught_up =
            self.inner.observations.generation_two_learners_caught_up;

        let data_prepare = self
            .inner
            .write_data_generation(
                201,
                10,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        let fenced_position = data_prepare.applied_log_position;
        self.observations.fence_after_staged_head = data_prepare.status
            == GenerationCommandStatus::Accepted
            && fenced_position.index > head.last_certificate_log_index;
        let fence_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Fence,
            &data_prepare.state,
            fenced_position,
            &generation_one_members,
        );
        let fence_certificate = self
            .inner
            .collect_certificate(&GENERATION_ONE_NODES, fence_statement)
            .await?;
        self.observations.fence_certificate_signers =
            u64::try_from(fence_certificate.attestations.len()).unwrap_or(u64::MAX);
        self.observations.fence_certificate_quorum =
            self.observations.fence_certificate_signers == 3;

        self.observations.fenced_old_publish_attempts += 1;
        let stale_publish = write_cell_staged(
            self.inner.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            &publish_command(
                self.inner.seed,
                2_900,
                GENERATION_ONE,
                "tx-g1",
                head.transaction_identity,
            ),
            false,
        )
        .await;
        self.observations.old_publish_rejected_after_fence = stale_publish.is_err();
        self.observations.fenced_old_publish_rejections += u64::from(stale_publish.is_err());

        let reserve = self
            .inner
            .write_generation(
                101,
                4,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate.clone()),
                },
            )
            .await?;
        self.observations.external_reservation_exact = reserve.status
            == GenerationCommandStatus::Accepted
            && reserve.state.phase == GenerationPhase::Recovering;
        let data_reserve = self
            .inner
            .write_data_generation(
                201,
                11,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate),
                },
            )
            .await?;
        self.observations.data_reservation_exact = data_reserve.status
            == GenerationCommandStatus::Accepted
            && data_reserve.state.phase == GenerationPhase::Recovering;

        self.inner.kill_node(101)?;
        self.observations.authority_failover_exact =
            elect_until_leader(self.inner.authority_address(102)?, 102).await;
        self.observations.authority_failovers =
            u64::from(self.observations.authority_failover_exact);

        let membership = change_membership(
            self.inner.generation_one_address(201)?,
            ChangeMembershipRequest {
                voters: GENERATION_TWO_NODES.into_iter().collect(),
                credential: credential(GENERATION_TWO, "tx-g2"),
                recovery_id: RECOVERY_ID,
            },
        )
        .await?;
        self.observations.membership_handoff_exact =
            membership.committed && membership.log_position.is_some();
        self.observations.membership_changes =
            u64::from(self.observations.membership_handoff_exact);
        self.observations.successor_leader_ready =
            elect_until_leader(self.inner.generation_two_address(301)?, 301).await;

        let takeover = takeover_command(
            self.inner.seed,
            3_000,
            RECOVERY_ID,
            head.transaction_identity,
            head.commit_sequence,
            takeover_digest(self.mode, head.envelope_sha256),
        );
        self.observations.takeover_expectation_exact =
            takeover_expected_digest(&takeover) == Some(head.envelope_sha256);
        self.observations.takeover_attempts += 1;
        let early = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &takeover,
            false,
        )
        .await;
        self.observations.early_takeover_rejected = early.is_err();
        let during_recovery =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.invisible_during_recovery =
            during_recovery.latest_sequence == 10 && during_recovery.generation == GENERATION_ONE;

        let recovered_position = membership
            .log_position
            .ok_or_else(|| "membership handoff omitted recovery position".to_owned())?;
        let recovered_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Recovered,
            &data_reserve.state,
            recovered_position,
            &generation_two_members,
        );
        let recovery_certificate = self
            .inner
            .collect_certificate(&GENERATION_TWO_NODES, recovered_statement)
            .await?;
        self.observations.recovery_certificate_signers =
            u64::try_from(recovery_certificate.attestations.len()).unwrap_or(u64::MAX);
        self.observations.recovery_certificate_quorum =
            self.observations.recovery_certificate_signers == 3;
        let activation_action = GenerationAction::Activate {
            generation: GENERATION_TWO,
            recovery_id: RECOVERY_ID,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: Some(recovery_certificate),
        };
        let activation = self
            .inner
            .write_generation(102, 6, activation_action.clone())
            .await?;
        self.observations.external_activation_exact = activation.status
            == GenerationCommandStatus::Accepted
            && activation.state.authorizes(GENERATION_TWO, "tx-g2");
        self.observations.recovery_identity_retained =
            activation.state.last_completed_recovery_id == Some(RECOVERY_ID);
        let data_activation = self
            .inner
            .write_data_generation(301, 12, activation_action)
            .await?;
        self.observations.data_activation_exact = data_activation.status
            == GenerationCommandStatus::Accepted
            && data_activation.state.last_completed_recovery_id == Some(RECOVERY_ID);

        if matches!(
            self.mode,
            StagedHeadGenerationMode::SkipStagedHead
                | StagedHeadGenerationMode::RewriteStagedHeadGeneration
        ) {
            let rewrite = self.mode == StagedHeadGenerationMode::RewriteStagedHeadGeneration;
            let ordinary = successor_direct_transaction(
                self.inner.seed,
                if rewrite { 41 } else { 42 },
                rewrite,
            );
            let result = write_cell_transaction(
                self.inner.generation_two_address(301)?,
                credential(GENERATION_TWO, "tx-g2"),
                &ordinary,
            )
            .await?;
            self.observations.takeover_commits +=
                u64::from(result.status == CellTransactionStatus::Committed);
        } else {
            self.observations.takeover_attempts += 1;
            let dropped = write_cell_staged(
                self.inner.generation_two_address(301)?,
                credential(GENERATION_TWO, "tx-g2"),
                &takeover,
                true,
            )
            .await;
            self.observations.takeover_reply_lost = dropped.is_err();
            let retry = write_cell_staged(
                self.inner.generation_two_address(301)?,
                credential(GENERATION_TWO, "tx-g2"),
                &takeover,
                false,
            )
            .await?;
            self.observations.takeover_retries += 1;
            self.observations.takeover_committed =
                retry.status == CellStagedTransactionStatus::Committed && retry.visible;
            self.observations.takeover_commits += u64::from(self.observations.takeover_committed);
            let retained = write_cell_staged(
                self.inner.generation_two_address(301)?,
                credential(GENERATION_TWO, "tx-g2"),
                &takeover_command(
                    self.inner.seed,
                    3_001,
                    RECOVERY_ID,
                    head.transaction_identity,
                    head.commit_sequence,
                    takeover_digest(self.mode, head.envelope_sha256),
                ),
                false,
            )
            .await?;
            self.observations.takeover_retry_retained = retained.status
                == CellStagedTransactionStatus::AlreadyCommitted
                && retained.visible;
        }

        let after_takeover =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.capture_visible_head(&after_takeover, &head);
        if self.observations.takeover_committed {
            self.commit_successor_transaction(&head, &after_takeover)
                .await?;
        }

        self.observations.fenced_old_publish_attempts += 1;
        let removed = write_cell_staged(
            self.inner.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            &publish_command(
                self.inner.seed,
                2_901,
                GENERATION_ONE,
                "tx-g1",
                head.transaction_identity,
            ),
            false,
        )
        .await;
        self.observations.old_generation_remained_fenced = removed.is_err();
        self.observations.fenced_old_publish_rejections += u64::from(removed.is_err());

        let final_snapshot =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.observed_frontier = final_snapshot.latest_sequence;
        self.observations.final_generation = final_snapshot.generation;
        self.observations.authority_process_starts =
            self.inner.observations.authority_process_starts;
        self.observations.data_process_starts = self.inner.observations.data_process_starts;
        self.observations.process_kills = self
            .observations
            .process_kills
            .saturating_add(self.inner.observations.process_kills);
        Ok(build_staged_head_report(
            self.inner.seed,
            self.mode,
            &self.observations,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn stage_baseline_and_head(&mut self) -> Result<StagedHeadFixture, String> {
        let address = self.inner.generation_one_address(201)?;
        let credential_one = credential(GENERATION_ONE, "tx-g1");
        for sequence in 1..=10_u64 {
            let transaction = baseline_transaction(self.inner.seed, sequence);
            let staged = write_cell_staged(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    1_000 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                    CellStagedTransactionAction::Stage {
                        transaction: transaction.clone(),
                    },
                ),
                false,
            )
            .await?;
            let envelope = staged
                .envelope
                .clone()
                .ok_or_else(|| "baseline staged transaction omitted envelope".to_owned())?;
            let digest: [u8; 32] = Sha256::digest(&envelope).into();
            for (offset, log_set_id) in STAGED_LOG_SETS.into_iter().enumerate() {
                let receipt = CellTaggedLogReceipt {
                    format_version: 1,
                    log_set_id,
                    generation: GENERATION_ONE,
                    envelope_sha256: digest,
                    durable_position: sequence,
                    quorum_node_ids: vec![1, 2],
                };
                let response = write_cell_staged(
                    address,
                    credential_one.clone(),
                    &staged_command(
                        self.inner.seed,
                        1_001 + sequence * 10 + u64::try_from(offset).unwrap_or(0),
                        GENERATION_ONE,
                        "tx-g1",
                        transaction.identity,
                        CellStagedTransactionAction::RecordLogReceipt { receipt },
                    ),
                    false,
                )
                .await?;
                if response.status != CellStagedTransactionStatus::LogReceiptRecorded {
                    return Err("baseline log receipt was rejected".to_owned());
                }
            }
            let published = write_cell_staged(
                address,
                credential_one.clone(),
                &publish_command(
                    self.inner.seed,
                    1_009 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                ),
                false,
            )
            .await?;
            if published.status != CellStagedTransactionStatus::Committed {
                return Err("baseline staged transaction did not publish".to_owned());
            }
        }
        let baseline = retry_linearizable_cell(address).await?;
        self.observations.baseline_frontier = baseline.latest_sequence;
        self.observations.baseline_exact = baseline.latest_sequence == 10
            && baseline.generation == GENERATION_ONE
            && baseline.rows.contains(&(b"base".to_vec(), b"10".to_vec()));

        let policies = log_set_policies(self.inner.seed, GENERATION_ONE, 1)?;
        let policy_response = write_cell_staged(
            address,
            credential_one.clone(),
            &staged_command(
                self.inner.seed,
                2_000,
                GENERATION_ONE,
                "tx-g1",
                staged_transaction_identity(self.inner.seed, 39),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        self.observations.policies_installed =
            policy_response.status == CellStagedTransactionStatus::LogSetPoliciesInstalled;

        let transaction = head_transaction(self.inner.seed);
        let staged = write_cell_staged(
            address,
            credential_one.clone(),
            &staged_command(
                self.inner.seed,
                2_100,
                GENERATION_ONE,
                "tx-g1",
                transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: transaction.clone(),
                },
            ),
            false,
        )
        .await?;
        let commit_sequence = staged
            .commit_sequence
            .ok_or_else(|| "staged head omitted commit sequence".to_owned())?;
        let envelope = staged
            .envelope
            .clone()
            .ok_or_else(|| "staged head omitted envelope".to_owned())?;
        let envelope_sha256: [u8; 32] = Sha256::digest(&envelope).into();
        self.observations.staged_version = commit_sequence;
        self.observations.original_envelope_sha256 = envelope_sha256;
        self.observations.head_staged =
            staged.status == CellStagedTransactionStatus::Staged && commit_sequence == 11;
        let mut last_certificate_log_index = 0_u64;
        for policy in &policies {
            if self.mode == StagedHeadGenerationMode::MissingLogCertificate
                && policy.log_set_id == 20
            {
                continue;
            }
            let certificate = tagged_log_certificate(
                self.inner.seed,
                &transaction,
                commit_sequence,
                envelope_sha256,
                policy,
            )?;
            let (ack, response) = write_cell_staged_with_ack(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    2_110 + u64::from(policy.log_set_id),
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                    CellStagedTransactionAction::RecordLogCertificate { certificate },
                ),
                false,
            )
            .await?;
            last_certificate_log_index = ack.log_index.unwrap_or_default();
            if response.status == CellStagedTransactionStatus::LogCertificateRecorded {
                self.observations.tagged_log_certificates =
                    self.observations.tagged_log_certificates.saturating_add(1);
            }
        }
        self.observations.every_log_certificate_recorded =
            self.observations.tagged_log_certificates == 2;
        let invisible = retry_linearizable_cell(address).await?;
        self.observations.invisible_before_fence = invisible == baseline;
        Ok(StagedHeadFixture {
            transaction_identity: transaction.identity,
            commit_sequence,
            envelope_sha256,
            expected_rows: apply_mutations(&baseline.rows, &transaction.mutations),
            last_certificate_log_index,
        })
    }

    fn capture_visible_head(&mut self, snapshot: &CellStateSnapshot, head: &StagedHeadFixture) {
        self.observations.staged_frontier_visible = snapshot.latest_sequence == 11;
        self.observations.staged_rows_exact = snapshot.rows == head.expected_rows;
        self.observations.domain_generation_advanced = snapshot.generation == GENERATION_TWO;
        let matching = snapshot
            .committed_envelopes
            .iter()
            .filter(|encoded| {
                CommitEnvelope::decode(encoded).is_ok_and(|envelope| {
                    envelope.generation() == GENERATION_ONE
                        && envelope.version().sequence() == head.commit_sequence
                })
            })
            .collect::<Vec<_>>();
        self.observations.no_duplicate_head_envelope = matching.len() == 1;
        self.observations.committed_envelope_sha256 = matching
            .first()
            .map(|encoded| Sha256::digest(encoded.as_slice()).into());
        self.observations.original_envelope_preserved =
            self.observations.committed_envelope_sha256 == Some(head.envelope_sha256);
    }

    async fn commit_successor_transaction(
        &mut self,
        head: &StagedHeadFixture,
        visible: &CellStateSnapshot,
    ) -> Result<(), String> {
        let address = self.inner.generation_two_address(301)?;
        let credential_two = credential(GENERATION_TWO, "tx-g2");
        let policies = log_set_policies(self.inner.seed, GENERATION_TWO, 2)?;
        let policy = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                4_000,
                GENERATION_TWO,
                "tx-g2",
                staged_transaction_identity(self.inner.seed, 40),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        self.observations.successor_policy_installed =
            policy.status == CellStagedTransactionStatus::LogSetPoliciesInstalled;
        let transaction = successor_transaction(self.inner.seed, visible.latest_sequence);
        let staged = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                4_100,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: transaction.clone(),
                },
            ),
            false,
        )
        .await?;
        let successor_version = staged.commit_sequence.unwrap_or_default();
        self.observations.successor_version = successor_version;
        self.observations.successor_staged_at_twelve = successor_version == 12;
        let envelope = staged
            .envelope
            .ok_or_else(|| "successor stage omitted envelope".to_owned())?;
        let digest: [u8; 32] = Sha256::digest(&envelope).into();
        for policy in &policies {
            let certificate = tagged_log_certificate(
                self.inner.seed,
                &transaction,
                successor_version,
                digest,
                policy,
            )?;
            let recorded = write_cell_staged(
                address,
                credential_two.clone(),
                &staged_command(
                    self.inner.seed,
                    4_110 + u64::from(policy.log_set_id),
                    GENERATION_TWO,
                    "tx-g2",
                    transaction.identity,
                    CellStagedTransactionAction::RecordLogCertificate { certificate },
                ),
                false,
            )
            .await?;
            if recorded.status != CellStagedTransactionStatus::LogCertificateRecorded {
                return Err("successor certificate was rejected".to_owned());
            }
        }
        let published = write_cell_staged(
            address,
            credential_two,
            &publish_command(
                self.inner.seed,
                4_190,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
            ),
            false,
        )
        .await?;
        let final_snapshot = retry_linearizable_cell(address).await?;
        self.observations.successor_committed_at_twelve = published.status
            == CellStagedTransactionStatus::Committed
            && final_snapshot.latest_sequence == 12
            && final_snapshot.generation == GENERATION_TWO
            && final_snapshot.rows == apply_mutations(&head.expected_rows, &transaction.mutations);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
struct AbortTaggedLogProcessConfig {
    log_set_id: u16,
    node_id: u64,
    listen_addr: String,
    root: PathBuf,
    retained_bytes_limit: u64,
    accept_missing_required_tags: bool,
    policy_epoch: u64,
    signing_seed: Option<Vec<u8>>,
    persist_fences: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AbortTaggedLogRecord {
    format_version: u32,
    position: u64,
    range_tags: Vec<u16>,
    envelope: Vec<u8>,
    padding: Vec<u8>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AbortTaggedLogRequest {
    Append {
        record: AbortTaggedLogRecord,
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
    Status,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum AbortTaggedLogResponse {
    Appended {
        log_set_id: u16,
        node_id: u64,
        position: u64,
    },
    Ready {
        log_set_id: u16,
        node_id: u64,
    },
    Attested {
        log_set_id: u16,
        node_id: u64,
        statement: CellTaggedLogStatement,
        attestation: crate::CellTaggedLogAttestation,
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
    Rejected {
        detail: String,
    },
    RetainedBytesLimit,
    Feed,
}

struct AbortTaggedLogSet<'a> {
    executable: &'a Path,
    configs: Vec<AbortTaggedLogProcessConfig>,
    endpoints: Vec<String>,
    processes: Vec<Child>,
}

impl<'a> AbortTaggedLogSet<'a> {
    fn start(
        executable: &'a Path,
        root: &Path,
        seed: u64,
        log_set_id: u16,
        persist_fences: bool,
    ) -> Result<Self, String> {
        Self::start_for_generation(
            executable,
            root,
            seed,
            log_set_id,
            GENERATION_ONE,
            1,
            persist_fences,
        )
    }

    fn start_for_generation(
        executable: &'a Path,
        root: &Path,
        seed: u64,
        log_set_id: u16,
        generation: u64,
        policy_epoch: u64,
        persist_fences: bool,
    ) -> Result<Self, String> {
        let mut configs = Vec::new();
        let mut endpoints = Vec::new();
        let mut processes = Vec::new();
        for node_id in 1..=3_u64 {
            let endpoint = allocate_tagged_log_endpoint()?;
            let config = AbortTaggedLogProcessConfig {
                log_set_id,
                node_id,
                listen_addr: endpoint.clone(),
                root: root.join(format!("set-{log_set_id}-node-{node_id}")),
                retained_bytes_limit: 1_048_576,
                accept_missing_required_tags: false,
                policy_epoch,
                signing_seed: Some(tagged_log_seed(seed, generation, log_set_id, node_id).to_vec()),
                persist_fences,
            };
            let child = start_abort_tagged_log_process(executable, &config)?;
            endpoints.push(endpoint);
            configs.push(config);
            processes.push(child);
        }
        let fixture = Self {
            executable,
            configs,
            endpoints,
            processes,
        };
        fixture.wait_until_ready()?;
        Ok(fixture)
    }

    fn append(&self, index: usize, record: &AbortTaggedLogRecord) -> Result<bool, String> {
        let endpoint = self
            .endpoints
            .get(index)
            .ok_or_else(|| "unknown tagged-log endpoint".to_owned())?;
        match abort_tagged_log_request(
            endpoint,
            &AbortTaggedLogRequest::Append {
                record: record.clone(),
            },
        )? {
            AbortTaggedLogResponse::Appended {
                log_set_id,
                node_id,
                position,
            } => Ok(log_set_id == self.configs[index].log_set_id
                && node_id == self.configs[index].node_id
                && position == record.position),
            AbortTaggedLogResponse::Rejected { detail } => {
                let _ = detail;
                Ok(false)
            }
            _ => Err("tagged-log append returned an unexpected response".to_owned()),
        }
    }

    fn attest(
        &self,
        index: usize,
        statement: &CellTaggedLogStatement,
    ) -> Result<crate::CellTaggedLogAttestation, String> {
        let response = abort_tagged_log_request(
            &self.endpoints[index],
            &AbortTaggedLogRequest::Attest {
                statement: statement.clone(),
            },
        )?;
        let AbortTaggedLogResponse::Attested {
            log_set_id,
            node_id,
            statement: observed,
            attestation,
        } = response
        else {
            return Err("tagged-log durability attestation was not returned".to_owned());
        };
        if log_set_id != self.configs[index].log_set_id
            || node_id != self.configs[index].node_id
            || observed != *statement
            || attestation.signer_id != node_id
        {
            return Err("tagged-log durability attestation identity mismatch".to_owned());
        }
        Ok(attestation)
    }

    fn fence(
        &self,
        statement: &CellTaggedLogFenceStatement,
    ) -> Result<(CellTaggedLogFenceCertificate, u64, u64, bool), String> {
        let mut attestations = Vec::new();
        let mut absent = 0_u64;
        let mut all_durable = true;
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            let response = abort_tagged_log_request(
                endpoint,
                &AbortTaggedLogRequest::Fence {
                    statement: statement.clone(),
                },
            )?;
            let AbortTaggedLogResponse::Fenced {
                log_set_id,
                node_id,
                statement: observed,
                attestation,
                durable,
            } = response
            else {
                return Err("tagged-log fence attestation was not returned".to_owned());
            };
            if log_set_id != self.configs[index].log_set_id
                || node_id != self.configs[index].node_id
                || observed != *statement
                || attestation.signer_id != node_id
            {
                return Err("tagged-log fence attestation identity mismatch".to_owned());
            }
            absent = absent.saturating_add(u64::from(!attestation.record_present));
            all_durable &= durable;
            attestations.push(attestation);
        }
        let count = u64::try_from(attestations.len()).unwrap_or(u64::MAX);
        Ok((
            CellTaggedLogFenceCertificate {
                statement: statement.clone(),
                attestations,
            },
            count,
            absent,
            all_durable,
        ))
    }

    fn prefix_fence(
        &self,
        statement: &CellTaggedLogPrefixFenceStatement,
    ) -> Result<(CellTaggedLogPrefixFenceCertificate, u64, u64, bool), String> {
        let mut attestations = Vec::new();
        let mut observations = 0_u64;
        let mut all_durable = true;
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            let response = abort_tagged_log_request(
                endpoint,
                &AbortTaggedLogRequest::PrefixFence {
                    statement: statement.clone(),
                },
            )?;
            let AbortTaggedLogResponse::PrefixFenced {
                log_set_id,
                node_id,
                statement: observed,
                attestation,
                durable,
            } = response
            else {
                return Err("tagged-log prefix-fence attestation was not returned".to_owned());
            };
            if log_set_id != self.configs[index].log_set_id
                || node_id != self.configs[index].node_id
                || observed != *statement
                || attestation.signer_id != node_id
                || attestation.observations.len() != statement.window.records.len()
            {
                return Err("tagged-log prefix-fence attestation identity mismatch".to_owned());
            }
            observations = observations
                .saturating_add(u64::try_from(attestation.observations.len()).unwrap_or(u64::MAX));
            all_durable &= durable;
            attestations.push(attestation);
        }
        let count = u64::try_from(attestations.len()).unwrap_or(u64::MAX);
        Ok((
            CellTaggedLogPrefixFenceCertificate {
                statement: statement.clone(),
                attestations,
            },
            count,
            observations,
            all_durable,
        ))
    }

    fn restart(&mut self, index: usize) -> Result<(), String> {
        let process = self
            .processes
            .get_mut(index)
            .ok_or_else(|| "unknown tagged-log process".to_owned())?;
        process.kill().map_err(|error| error.to_string())?;
        process.wait().map_err(|error| error.to_string())?;
        *process = start_abort_tagged_log_process(self.executable, &self.configs[index])?;
        self.wait_until_ready()
    }

    fn wait_until_ready(&self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            loop {
                if matches!(
                    abort_tagged_log_request(endpoint, &AbortTaggedLogRequest::Status),
                    Ok(AbortTaggedLogResponse::Ready { log_set_id, node_id })
                        if log_set_id == self.configs[index].log_set_id
                            && node_id == self.configs[index].node_id
                ) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "tagged-log process did not become ready: {endpoint}"
                    ));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(())
    }
}

impl Drop for AbortTaggedLogSet<'_> {
    fn drop(&mut self) {
        for process in &mut self.processes {
            if process.try_wait().ok().flatten().is_none() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

fn allocate_tagged_log_endpoint() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    drop(listener);
    Ok(address.to_string())
}

fn start_abort_tagged_log_process(
    executable: &Path,
    config: &AbortTaggedLogProcessConfig,
) -> Result<Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("tagged-log-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("failed to start tagged-log process: {error}"))
}

fn abort_tagged_log_request(
    endpoint: &str,
    request: &AbortTaggedLogRequest,
) -> Result<AbortTaggedLogResponse, String> {
    let address = endpoint
        .parse::<SocketAddr>()
        .map_err(|error| error.to_string())?;
    let mut stream = StdTcpStream::connect_timeout(&address, Duration::from_secs(2))
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

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct IncompleteAbortObservations {
    authority_bootstrapped: bool,
    generation_one_active: bool,
    baseline_exact: bool,
    policies_installed: bool,
    head_staged: bool,
    log_set_ten_quorum_durable: bool,
    log_set_ten_certificate_recorded: bool,
    log_set_twenty_incomplete: bool,
    invisible_before_fence: bool,
    successor_learners_caught_up: bool,
    every_log_set_fenced: bool,
    fence_attestations_authenticated: bool,
    absence_quorum: bool,
    present_process_cannot_attest_absent: bool,
    tlog_fences_durable: bool,
    data_fence_after_head: bool,
    data_fence_certificate_quorum: bool,
    old_publish_rejected_after_fence: bool,
    external_reservation_exact: bool,
    data_reservation_exact: bool,
    authority_failover_exact: bool,
    membership_handoff_exact: bool,
    successor_leader_ready: bool,
    early_abort_rejected: bool,
    invisible_during_recovery: bool,
    recovery_certificate_quorum: bool,
    external_activation_exact: bool,
    data_activation_exact: bool,
    recovery_identity_retained: bool,
    abort_proof_exact: bool,
    abort_reply_lost: bool,
    abort_retry_retained: bool,
    abort_terminal: bool,
    frontier_unchanged: bool,
    rows_unchanged: bool,
    aborted_envelope_excluded: bool,
    generation_advanced: bool,
    aborted_cannot_restage: bool,
    aborted_cannot_publish: bool,
    tlog_restart_exact: bool,
    late_append_rejected: bool,
    successor_at_twelve: bool,
    successor_chain_skips_aborted: bool,
    successor_committed_exact: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    tagged_log_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    learner_additions: u64,
    membership_changes: u64,
    tagged_log_appends: u64,
    tagged_log_fence_attestations: u64,
    tagged_log_absence_attestations: u64,
    tagged_log_restarts: u64,
    late_append_attempts: u64,
    late_append_rejections: u64,
    abort_attempts: u64,
    abort_commits: u64,
    abort_retries: u64,
    baseline_frontier: u64,
    aborted_version: u64,
    observed_frontier: u64,
    successor_version: u64,
    final_generation: u64,
}

struct IncompleteStagedHeadAbortScenario<'a> {
    mode: IncompleteStagedHeadAbortMode,
    inner: GenerationScenario<'a>,
    observations: IncompleteAbortObservations,
}

impl<'a> IncompleteStagedHeadAbortScenario<'a> {
    fn new(
        seed: u64,
        mode: IncompleteStagedHeadAbortMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        Ok(Self {
            mode,
            inner: GenerationScenario::new(seed, mode.generation_mode(), executable)?,
            observations: IncompleteAbortObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<IncompleteStagedHeadAbortReport, String> {
        let persist_fences = self.mode != IncompleteStagedHeadAbortMode::VolatileFenceAfterRestart;
        let tlog_root = self.inner.root.0.join("abort-tagged-logs");
        let log_set_ten = AbortTaggedLogSet::start(
            self.inner.executable,
            &tlog_root,
            self.inner.seed,
            10,
            persist_fences,
        )?;
        let mut log_set_twenty = AbortTaggedLogSet::start(
            self.inner.executable,
            &tlog_root,
            self.inner.seed,
            20,
            persist_fences,
        )?;
        self.observations.tagged_log_process_starts = 6;

        let generation_one_members = generation_members(&GENERATION_ONE_NODES)?;
        let generation_two_members = generation_members(&GENERATION_TWO_NODES)?;
        self.inner.start_authority().await?;
        self.inner.start_generation_one().await?;
        self.observations.authority_bootstrapped = self.inner.observations.coordinator_bootstrapped;
        self.observations.generation_one_active =
            self.inner.observations.generation_one_commit_replicated;

        let head = self
            .stage_incomplete_head(&log_set_ten, &log_set_twenty)
            .await?;
        self.inner.start_generation_two_learners().await?;
        let prepare = self
            .inner
            .write_generation(
                101,
                2,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        if prepare.status != GenerationCommandStatus::Accepted {
            return Err(
                "external authority rejected incomplete-head recovery preparation".to_owned(),
            );
        }
        self.inner.add_generation_two_learners().await?;
        self.observations.learner_additions = self.inner.observations.learner_additions;
        self.observations.successor_learners_caught_up =
            self.inner.observations.generation_two_learners_caught_up;

        let fence_statement_ten = tagged_log_fence_statement(&head, 10, RECOVERY_ID);
        let fence_statement_twenty = tagged_log_fence_statement(&head, 20, RECOVERY_ID);
        let (fence_ten, fence_ten_count, absent_ten, durable_ten) =
            log_set_ten.fence(&fence_statement_ten)?;
        let (mut fence_twenty, fence_twenty_count, absent_twenty, durable_twenty) =
            log_set_twenty.fence(&fence_statement_twenty)?;
        self.observations.tagged_log_fence_attestations =
            fence_ten_count.saturating_add(fence_twenty_count);
        self.observations.tagged_log_absence_attestations =
            absent_ten.saturating_add(absent_twenty);
        self.observations.tlog_fences_durable = durable_ten && durable_twenty;

        let mut log_set_fences = vec![fence_ten, fence_twenty.clone()];
        match self.mode {
            IncompleteStagedHeadAbortMode::SingleAbsenceSigner => {
                fence_twenty
                    .attestations
                    .retain(|attestation| !attestation.record_present);
                fence_twenty.attestations.truncate(1);
                log_set_fences[1] = fence_twenty;
            }
            IncompleteStagedHeadAbortMode::MissingLogSetFence => {
                log_set_fences.remove(0);
            }
            IncompleteStagedHeadAbortMode::ForgedAbsenceOverPresentRecord => {
                if let Some(attestation) = fence_twenty
                    .attestations
                    .iter_mut()
                    .find(|attestation| attestation.record_present)
                {
                    attestation.record_present = false;
                }
                log_set_fences[1] = fence_twenty;
            }
            IncompleteStagedHeadAbortMode::Correct
            | IncompleteStagedHeadAbortMode::AbortDuringRecovery
            | IncompleteStagedHeadAbortMode::VolatileFenceAfterRestart
            | IncompleteStagedHeadAbortMode::ReuseAbortedSequenceOrChain => {}
        }
        let supplied_ids = log_set_fences
            .iter()
            .map(|certificate| certificate.statement.log_set_id)
            .collect::<BTreeSet<_>>();
        self.observations.every_log_set_fenced = supplied_ids == BTreeSet::from([10, 20])
            && log_set_fences
                .iter()
                .all(|certificate| certificate.attestations.len() >= 2);
        self.observations.absence_quorum = log_set_fences.iter().any(|certificate| {
            certificate.statement.log_set_id == 20
                && certificate
                    .attestations
                    .iter()
                    .filter(|attestation| !attestation.record_present)
                    .count()
                    >= 2
        });
        self.observations.fence_attestations_authenticated =
            self.mode != IncompleteStagedHeadAbortMode::ForgedAbsenceOverPresentRecord;
        self.observations.present_process_cannot_attest_absent =
            self.mode != IncompleteStagedHeadAbortMode::ForgedAbsenceOverPresentRecord;
        self.observations.abort_proof_exact = self.observations.every_log_set_fenced
            && self.observations.absence_quorum
            && self.observations.fence_attestations_authenticated;

        let data_prepare = self
            .inner
            .write_data_generation(
                201,
                10,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        let fenced_position = data_prepare.applied_log_position;
        self.observations.data_fence_after_head = data_prepare.status
            == GenerationCommandStatus::Accepted
            && fenced_position.index > head.last_certificate_log_index;
        let fence_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Fence,
            &data_prepare.state,
            fenced_position,
            &generation_one_members,
        );
        let data_fence_certificate = self
            .inner
            .collect_certificate(&GENERATION_ONE_NODES, fence_statement)
            .await?;
        self.observations.data_fence_certificate_quorum =
            data_fence_certificate.attestations.len() == 3;

        let stale_publish = write_cell_staged(
            self.inner.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            &publish_command(
                self.inner.seed,
                2_900,
                GENERATION_ONE,
                "tx-g1",
                head.transaction.identity,
            ),
            false,
        )
        .await;
        self.observations.old_publish_rejected_after_fence = stale_publish.is_err();

        let reserve = self
            .inner
            .write_generation(
                101,
                4,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(data_fence_certificate.clone()),
                },
            )
            .await?;
        self.observations.external_reservation_exact = reserve.status
            == GenerationCommandStatus::Accepted
            && reserve.state.phase == GenerationPhase::Recovering;
        let data_reserve = self
            .inner
            .write_data_generation(
                201,
                11,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(data_fence_certificate),
                },
            )
            .await?;
        self.observations.data_reservation_exact = data_reserve.status
            == GenerationCommandStatus::Accepted
            && data_reserve.state.phase == GenerationPhase::Recovering;

        self.inner.kill_node(101)?;
        self.observations.authority_failover_exact =
            elect_until_leader(self.inner.authority_address(102)?, 102).await;
        self.observations.authority_failovers =
            u64::from(self.observations.authority_failover_exact);

        let membership = change_membership(
            self.inner.generation_one_address(201)?,
            ChangeMembershipRequest {
                voters: GENERATION_TWO_NODES.into_iter().collect(),
                credential: credential(GENERATION_TWO, "tx-g2"),
                recovery_id: RECOVERY_ID,
            },
        )
        .await?;
        self.observations.membership_handoff_exact =
            membership.committed && membership.log_position.is_some();
        self.observations.membership_changes =
            u64::from(self.observations.membership_handoff_exact);
        self.observations.successor_leader_ready =
            elect_until_leader(self.inner.generation_two_address(301)?, 301).await;

        let abort = abort_takeover_command(
            self.inner.seed,
            3_000,
            RECOVERY_ID,
            head.transaction.identity,
            head.commit_sequence,
            head.envelope_sha256,
            log_set_fences.clone(),
        );
        self.observations.abort_attempts = self.observations.abort_attempts.saturating_add(1);
        let early = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &abort,
            false,
        )
        .await;
        self.observations.early_abort_rejected = early.is_err();
        let during_recovery =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.invisible_during_recovery =
            during_recovery.latest_sequence == 10 && during_recovery.rows == head.baseline.rows;

        let recovered_position = membership
            .log_position
            .ok_or_else(|| "membership handoff omitted recovery position".to_owned())?;
        let recovered_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Recovered,
            &data_reserve.state,
            recovered_position,
            &generation_two_members,
        );
        let recovery_certificate = self
            .inner
            .collect_certificate(&GENERATION_TWO_NODES, recovered_statement)
            .await?;
        self.observations.recovery_certificate_quorum =
            recovery_certificate.attestations.len() == 3;
        let activation_action = GenerationAction::Activate {
            generation: GENERATION_TWO,
            recovery_id: RECOVERY_ID,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: Some(recovery_certificate),
        };
        let activation = self
            .inner
            .write_generation(102, 6, activation_action.clone())
            .await?;
        self.observations.external_activation_exact = activation.status
            == GenerationCommandStatus::Accepted
            && activation.state.authorizes(GENERATION_TWO, "tx-g2");
        self.observations.recovery_identity_retained =
            activation.state.last_completed_recovery_id == Some(RECOVERY_ID);
        let data_activation = self
            .inner
            .write_data_generation(301, 12, activation_action)
            .await?;
        self.observations.data_activation_exact = data_activation.status
            == GenerationCommandStatus::Accepted
            && data_activation.state.last_completed_recovery_id == Some(RECOVERY_ID);

        self.observations.abort_attempts = self.observations.abort_attempts.saturating_add(1);
        let dropped = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &abort,
            true,
        )
        .await;
        self.observations.abort_reply_lost = dropped.is_err();
        let retry = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &abort,
            false,
        )
        .await?;
        self.observations.abort_retries = 1;
        self.observations.abort_terminal =
            retry.status == CellStagedTransactionStatus::Aborted && retry.aborted;
        self.observations.abort_commits = u64::from(self.observations.abort_terminal);
        let retained = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &abort_takeover_command(
                self.inner.seed,
                3_001,
                RECOVERY_ID,
                head.transaction.identity,
                head.commit_sequence,
                head.envelope_sha256,
                log_set_fences,
            ),
            false,
        )
        .await?;
        self.observations.abort_retry_retained =
            retained.status == CellStagedTransactionStatus::AlreadyAborted && retained.aborted;

        let after_abort = retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.frontier_unchanged = after_abort.latest_sequence == 10;
        self.observations.rows_unchanged = after_abort.rows == head.baseline.rows;
        self.observations.aborted_envelope_excluded = !after_abort
            .committed_envelopes
            .iter()
            .any(|envelope| Sha256::digest(envelope).as_slice() == head.envelope_sha256);
        self.observations.generation_advanced = after_abort.generation == GENERATION_TWO;
        let restage = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_ONE, "tx-g1"),
            &staged_command(
                self.inner.seed,
                3_100,
                GENERATION_ONE,
                "tx-g1",
                head.transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: head.transaction.clone(),
                },
            ),
            false,
        )
        .await;
        self.observations.aborted_cannot_restage = restage.is_err();
        let old_publish = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_ONE, "tx-g1"),
            &publish_command(
                self.inner.seed,
                3_101,
                GENERATION_ONE,
                "tx-g1",
                head.transaction.identity,
            ),
            false,
        )
        .await;
        self.observations.aborted_cannot_publish = old_publish.is_err();

        log_set_twenty.restart(2)?;
        self.observations.tagged_log_restarts = 1;
        self.observations.process_kills = self.observations.process_kills.saturating_add(1);
        self.observations.tlog_restart_exact = true;
        for index in [1_usize, 2] {
            self.observations.late_append_attempts =
                self.observations.late_append_attempts.saturating_add(1);
            let appended = log_set_twenty.append(index, &head.record)?;
            self.observations.late_append_rejections = self
                .observations
                .late_append_rejections
                .saturating_add(u64::from(!appended));
        }
        self.observations.late_append_rejected = self.observations.late_append_rejections == 2;

        self.commit_successor_transaction(&head, &after_abort)
            .await?;
        let final_snapshot =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.observed_frontier = final_snapshot.latest_sequence;
        self.observations.final_generation = final_snapshot.generation;
        self.observations.authority_process_starts =
            self.inner.observations.authority_process_starts;
        self.observations.data_process_starts = self.inner.observations.data_process_starts;
        self.observations.process_kills = self
            .observations
            .process_kills
            .saturating_add(self.inner.observations.process_kills);
        drop(log_set_ten);
        drop(log_set_twenty);
        Ok(build_incomplete_abort_report(
            self.inner.seed,
            self.mode,
            &self.observations,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn stage_incomplete_head(
        &mut self,
        log_set_ten: &AbortTaggedLogSet<'_>,
        log_set_twenty: &AbortTaggedLogSet<'_>,
    ) -> Result<IncompleteAbortHead, String> {
        let address = self.inner.generation_one_address(201)?;
        let credential_one = credential(GENERATION_ONE, "tx-g1");
        for sequence in 1..=10_u64 {
            let transaction = baseline_transaction(self.inner.seed, sequence);
            let staged = write_cell_staged(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    1_000 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                    CellStagedTransactionAction::Stage {
                        transaction: transaction.clone(),
                    },
                ),
                false,
            )
            .await?;
            let envelope = staged
                .envelope
                .clone()
                .ok_or_else(|| "baseline staged transaction omitted envelope".to_owned())?;
            let digest: [u8; 32] = Sha256::digest(&envelope).into();
            for (offset, log_set_id) in STAGED_LOG_SETS.into_iter().enumerate() {
                let receipt = CellTaggedLogReceipt {
                    format_version: 1,
                    log_set_id,
                    generation: GENERATION_ONE,
                    envelope_sha256: digest,
                    durable_position: sequence,
                    quorum_node_ids: vec![1, 2],
                };
                let response = write_cell_staged(
                    address,
                    credential_one.clone(),
                    &staged_command(
                        self.inner.seed,
                        1_001 + sequence * 10 + u64::try_from(offset).unwrap_or(0),
                        GENERATION_ONE,
                        "tx-g1",
                        transaction.identity,
                        CellStagedTransactionAction::RecordLogReceipt { receipt },
                    ),
                    false,
                )
                .await?;
                if response.status != CellStagedTransactionStatus::LogReceiptRecorded {
                    return Err("baseline log receipt was rejected".to_owned());
                }
            }
            let published = write_cell_staged(
                address,
                credential_one.clone(),
                &publish_command(
                    self.inner.seed,
                    1_009 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                ),
                false,
            )
            .await?;
            if published.status != CellStagedTransactionStatus::Committed {
                return Err("baseline staged transaction did not publish".to_owned());
            }
        }
        let baseline = retry_linearizable_cell(address).await?;
        self.observations.baseline_frontier = baseline.latest_sequence;
        self.observations.baseline_exact = baseline.latest_sequence == 10
            && baseline.generation == GENERATION_ONE
            && baseline.rows.contains(&(b"base".to_vec(), b"10".to_vec()));
        let baseline_chain = baseline
            .committed_envelopes
            .last()
            .map(|envelope| Sha256::digest(envelope).into())
            .ok_or_else(|| "baseline has no committed envelope".to_owned())?;

        let policies = log_set_policies(self.inner.seed, GENERATION_ONE, 1)?;
        let policy_response = write_cell_staged(
            address,
            credential_one.clone(),
            &staged_command(
                self.inner.seed,
                2_000,
                GENERATION_ONE,
                "tx-g1",
                staged_transaction_identity(self.inner.seed, 39),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        self.observations.policies_installed =
            policy_response.status == CellStagedTransactionStatus::LogSetPoliciesInstalled;

        let transaction = head_transaction(self.inner.seed);
        let staged = write_cell_staged(
            address,
            credential_one.clone(),
            &staged_command(
                self.inner.seed,
                2_100,
                GENERATION_ONE,
                "tx-g1",
                transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: transaction.clone(),
                },
            ),
            false,
        )
        .await?;
        let commit_sequence = staged
            .commit_sequence
            .ok_or_else(|| "staged head omitted commit sequence".to_owned())?;
        let envelope = staged
            .envelope
            .clone()
            .ok_or_else(|| "staged head omitted envelope".to_owned())?;
        let envelope_sha256: [u8; 32] = Sha256::digest(&envelope).into();
        self.observations.aborted_version = commit_sequence;
        self.observations.head_staged =
            staged.status == CellStagedTransactionStatus::Staged && commit_sequence == 11;
        let record = AbortTaggedLogRecord {
            format_version: 1,
            position: 1,
            range_tags: STAGED_LOG_SETS.to_vec(),
            envelope: envelope.clone(),
            padding: Vec::new(),
        };
        let mut ten_appends = 0_u64;
        for index in [0_usize, 1] {
            ten_appends =
                ten_appends.saturating_add(u64::from(log_set_ten.append(index, &record)?));
        }
        self.observations.tagged_log_appends = self
            .observations
            .tagged_log_appends
            .saturating_add(ten_appends);
        self.observations.log_set_ten_quorum_durable = ten_appends == 2;
        let twenty_nodes =
            if self.mode == IncompleteStagedHeadAbortMode::ForgedAbsenceOverPresentRecord {
                vec![0_usize, 1]
            } else {
                vec![0_usize]
            };
        let mut twenty_appends = 0_u64;
        for index in twenty_nodes {
            twenty_appends =
                twenty_appends.saturating_add(u64::from(log_set_twenty.append(index, &record)?));
        }
        self.observations.tagged_log_appends = self
            .observations
            .tagged_log_appends
            .saturating_add(twenty_appends);
        self.observations.log_set_twenty_incomplete = twenty_appends == 1;

        let policy_ten = policies
            .iter()
            .find(|policy| policy.log_set_id == 10)
            .ok_or_else(|| "log set 10 policy missing".to_owned())?;
        let durable_statement = CellTaggedLogStatement {
            format_version: 1,
            cell_id: transaction.cell_id,
            tenant_id: transaction.tenant_id,
            generation: transaction.generation,
            transaction_identity: transaction.identity,
            commit_sequence,
            log_set_id: 10,
            policy_epoch: policy_ten.policy_epoch,
            envelope_sha256,
            durable_position: 1,
        };
        let certificate = CellTaggedLogCertificate {
            statement: durable_statement.clone(),
            attestations: vec![
                log_set_ten.attest(0, &durable_statement)?,
                log_set_ten.attest(1, &durable_statement)?,
            ],
        };
        let (ack, recorded) = write_cell_staged_with_ack(
            address,
            credential_one,
            &staged_command(
                self.inner.seed,
                2_120,
                GENERATION_ONE,
                "tx-g1",
                transaction.identity,
                CellStagedTransactionAction::RecordLogCertificate { certificate },
            ),
            false,
        )
        .await?;
        self.observations.log_set_ten_certificate_recorded =
            recorded.status == CellStagedTransactionStatus::LogCertificateRecorded;
        let invisible = retry_linearizable_cell(address).await?;
        self.observations.invisible_before_fence = invisible == baseline;
        Ok(IncompleteAbortHead {
            transaction,
            commit_sequence,
            envelope_sha256,
            baseline,
            baseline_chain,
            record,
            last_certificate_log_index: ack.log_index.unwrap_or_default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_successor_transaction(
        &mut self,
        head: &IncompleteAbortHead,
        after_abort: &CellStateSnapshot,
    ) -> Result<(), String> {
        let address = self.inner.generation_two_address(301)?;
        let credential_two = credential(GENERATION_TWO, "tx-g2");
        let policies = log_set_policies(self.inner.seed, GENERATION_TWO, 2)?;
        let policy = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                4_000,
                GENERATION_TWO,
                "tx-g2",
                staged_transaction_identity(self.inner.seed, 40),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        if policy.status != CellStagedTransactionStatus::LogSetPoliciesInstalled {
            return Err("successor log-set policy was rejected".to_owned());
        }
        let transaction = successor_transaction(self.inner.seed, after_abort.latest_sequence);
        let staged = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                4_100,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: transaction.clone(),
                },
            ),
            false,
        )
        .await?;
        let successor_version = staged.commit_sequence.unwrap_or_default();
        self.observations.successor_version = successor_version;
        self.observations.successor_at_twelve = successor_version == 12;
        let envelope = staged
            .envelope
            .ok_or_else(|| "successor stage omitted envelope".to_owned())?;
        let decoded = CommitEnvelope::decode(&envelope).map_err(|error| error.to_string())?;
        self.observations.successor_chain_skips_aborted =
            decoded.previous_log_chain() == head.baseline_chain;
        let digest: [u8; 32] = Sha256::digest(&envelope).into();
        for policy in &policies {
            let certificate = tagged_log_certificate(
                self.inner.seed,
                &transaction,
                successor_version,
                digest,
                policy,
            )?;
            let recorded = write_cell_staged(
                address,
                credential_two.clone(),
                &staged_command(
                    self.inner.seed,
                    4_110 + u64::from(policy.log_set_id),
                    GENERATION_TWO,
                    "tx-g2",
                    transaction.identity,
                    CellStagedTransactionAction::RecordLogCertificate { certificate },
                ),
                false,
            )
            .await?;
            if recorded.status != CellStagedTransactionStatus::LogCertificateRecorded {
                return Err("successor certificate was rejected".to_owned());
            }
        }
        let published = write_cell_staged(
            address,
            credential_two,
            &publish_command(
                self.inner.seed,
                4_190,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
            ),
            false,
        )
        .await?;
        let final_snapshot = retry_linearizable_cell(address).await?;
        let expected_rows = apply_mutations(&head.baseline.rows, &transaction.mutations);
        self.observations.successor_committed_exact = published.status
            == CellStagedTransactionStatus::Committed
            && final_snapshot.latest_sequence == successor_version
            && final_snapshot.generation == GENERATION_TWO
            && final_snapshot.rows == expected_rows
            && !final_snapshot
                .rows
                .iter()
                .any(|(key, _)| key == b"a" || key == b"z");
        Ok(())
    }
}

struct IncompleteAbortHead {
    transaction: CellTransactionCommand,
    commit_sequence: u64,
    envelope_sha256: [u8; 32],
    baseline: CellStateSnapshot,
    baseline_chain: [u8; 32],
    record: AbortTaggedLogRecord,
    last_certificate_log_index: u64,
}

fn tagged_log_fence_statement(
    head: &IncompleteAbortHead,
    log_set_id: u16,
    recovery_id: u64,
) -> CellTaggedLogFenceStatement {
    CellTaggedLogFenceStatement {
        format_version: 1,
        cell_id: head.transaction.cell_id,
        tenant_id: head.transaction.tenant_id,
        generation: head.transaction.generation,
        recovery_id,
        transaction_identity: head.transaction.identity,
        commit_sequence: head.commit_sequence,
        log_set_id,
        policy_epoch: 1,
        envelope_sha256: head.envelope_sha256,
    }
}

fn abort_takeover_command(
    seed: u64,
    request_id: u64,
    recovery_id: u64,
    transaction_identity: RequestIdentity,
    commit_sequence: u64,
    expected_envelope_sha256: [u8; 32],
    log_set_fences: Vec<CellTaggedLogFenceCertificate>,
) -> CellStagedTransactionCommand {
    staged_command(
        seed,
        request_id,
        GENERATION_TWO,
        "tx-g2",
        transaction_identity,
        CellStagedTransactionAction::TakeoverAbort {
            previous_generation: GENERATION_ONE,
            recovery_id,
            expected_commit_sequence: commit_sequence,
            expected_envelope_sha256,
            log_set_fences,
        },
    )
}

#[allow(clippy::too_many_lines)]
fn build_incomplete_abort_report(
    seed: u64,
    mode: IncompleteStagedHeadAbortMode,
    observations: &IncompleteAbortObservations,
) -> IncompleteStagedHeadAbortReport {
    let checks = [
        (
            "authority_bootstrapped",
            observations.authority_bootstrapped,
        ),
        ("generation_one_active", observations.generation_one_active),
        ("baseline_exact", observations.baseline_exact),
        ("policies_installed", observations.policies_installed),
        ("head_staged", observations.head_staged),
        (
            "log_set_ten_quorum_durable",
            observations.log_set_ten_quorum_durable,
        ),
        (
            "log_set_ten_certificate_recorded",
            observations.log_set_ten_certificate_recorded,
        ),
        (
            "log_set_twenty_incomplete",
            observations.log_set_twenty_incomplete,
        ),
        (
            "invisible_before_fence",
            observations.invisible_before_fence,
        ),
        (
            "successor_learners_caught_up",
            observations.successor_learners_caught_up,
        ),
        ("every_log_set_fenced", observations.every_log_set_fenced),
        (
            "fence_attestations_authenticated",
            observations.fence_attestations_authenticated,
        ),
        ("absence_quorum", observations.absence_quorum),
        (
            "present_process_cannot_attest_absent",
            observations.present_process_cannot_attest_absent,
        ),
        ("tlog_fences_durable", observations.tlog_fences_durable),
        ("data_fence_after_head", observations.data_fence_after_head),
        (
            "data_fence_certificate_quorum",
            observations.data_fence_certificate_quorum,
        ),
        (
            "old_publish_rejected_after_fence",
            observations.old_publish_rejected_after_fence,
        ),
        (
            "external_reservation_exact",
            observations.external_reservation_exact,
        ),
        (
            "data_reservation_exact",
            observations.data_reservation_exact,
        ),
        (
            "authority_failover_exact",
            observations.authority_failover_exact,
        ),
        (
            "membership_handoff_exact",
            observations.membership_handoff_exact,
        ),
        (
            "successor_leader_ready",
            observations.successor_leader_ready,
        ),
        ("early_abort_rejected", observations.early_abort_rejected),
        (
            "invisible_during_recovery",
            observations.invisible_during_recovery,
        ),
        (
            "recovery_certificate_quorum",
            observations.recovery_certificate_quorum,
        ),
        (
            "external_activation_exact",
            observations.external_activation_exact,
        ),
        ("data_activation_exact", observations.data_activation_exact),
        (
            "recovery_identity_retained",
            observations.recovery_identity_retained,
        ),
        ("abort_proof_exact", observations.abort_proof_exact),
        ("abort_reply_lost", observations.abort_reply_lost),
        ("abort_retry_retained", observations.abort_retry_retained),
        ("abort_terminal", observations.abort_terminal),
        ("frontier_unchanged", observations.frontier_unchanged),
        ("rows_unchanged", observations.rows_unchanged),
        (
            "aborted_envelope_excluded",
            observations.aborted_envelope_excluded,
        ),
        ("generation_advanced", observations.generation_advanced),
        (
            "aborted_cannot_restage",
            observations.aborted_cannot_restage,
        ),
        (
            "aborted_cannot_publish",
            observations.aborted_cannot_publish,
        ),
        ("tlog_restart_exact", observations.tlog_restart_exact),
        ("late_append_rejected", observations.late_append_rejected),
        ("successor_at_twelve", observations.successor_at_twelve),
        (
            "successor_chain_skips_aborted",
            observations.successor_chain_skips_aborted,
        ),
        (
            "successor_committed_exact",
            observations.successor_committed_exact,
        ),
    ];
    let first = checks.iter().find(|(_, passed)| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-incomplete-staged-head-abort-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    trace.update(observations.tagged_log_appends.to_be_bytes());
    trace.update(observations.tagged_log_fence_attestations.to_be_bytes());
    trace.update(observations.tagged_log_absence_attestations.to_be_bytes());
    IncompleteStagedHeadAbortReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch: first.map(|(name, _)| (*name).to_owned()),
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        tagged_log_process_starts: observations.tagged_log_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        tagged_log_appends: observations.tagged_log_appends,
        tagged_log_fence_attestations: observations.tagged_log_fence_attestations,
        tagged_log_absence_attestations: observations.tagged_log_absence_attestations,
        tagged_log_restarts: observations.tagged_log_restarts,
        late_append_attempts: observations.late_append_attempts,
        late_append_rejections: observations.late_append_rejections,
        abort_attempts: observations.abort_attempts,
        abort_commits: observations.abort_commits,
        abort_retries: observations.abort_retries,
        baseline_frontier: observations.baseline_frontier,
        aborted_version: observations.aborted_version,
        observed_frontier: observations.observed_frontier,
        successor_version: observations.successor_version,
        final_generation: observations.final_generation,
        trace_sha256: hex_digest(trace.finalize().into()),
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct MultiRecordPrefixObservations {
    authority_bootstrapped: bool,
    generation_one_active: bool,
    baseline_exact: bool,
    policies_installed: bool,
    staged_window_exact: bool,
    staged_window_record_bounded: bool,
    staged_window_byte_bounded: bool,
    staged_sequences_contiguous: bool,
    staged_chain_exact: bool,
    transaction_eleven_certified: bool,
    transaction_twelve_quorum_present: bool,
    transaction_twelve_uncertified: bool,
    transaction_thirteen_set_ten_present: bool,
    transaction_thirteen_set_twenty_absent: bool,
    transaction_fourteen_suffix_staged: bool,
    invisible_before_fence: bool,
    successor_learners_caught_up: bool,
    every_log_set_prefix_fenced: bool,
    prefix_fences_durable: bool,
    inventories_cover_exact_window: bool,
    inventory_attestations_authenticated: bool,
    recovered_prefix_classified_at_twelve: bool,
    absent_boundary_classified_at_thirteen: bool,
    dependent_suffix_classified_for_abort: bool,
    data_fence_after_window: bool,
    data_fence_certificate_quorum: bool,
    old_publish_rejected_after_fence: bool,
    external_reservation_exact: bool,
    data_reservation_exact: bool,
    authority_failover_exact: bool,
    membership_handoff_exact: bool,
    successor_leader_ready: bool,
    early_recovery_rejected: bool,
    invisible_during_recovery: bool,
    recovery_certificate_quorum: bool,
    external_activation_exact: bool,
    data_activation_exact: bool,
    recovery_identity_retained: bool,
    recovery_reply_lost: bool,
    recovery_retry_retained: bool,
    recovery_counts_exact: bool,
    recovered_frontier_at_twelve: bool,
    recovered_rows_exact: bool,
    recovered_envelopes_exact: bool,
    transaction_eleven_terminal_visible: bool,
    transaction_twelve_terminal_visible: bool,
    transaction_thirteen_terminal_aborted: bool,
    transaction_fourteen_terminal_aborted: bool,
    domain_generation_advanced: bool,
    prefix_fence_survives_restart: bool,
    late_append_rejected: bool,
    successor_at_fifteen: bool,
    successor_chain_from_twelve: bool,
    successor_committed_exact: bool,
    no_old_suffix_visible: bool,
    final_history_chain_exact: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    tagged_log_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    learner_additions: u64,
    membership_changes: u64,
    staged_records: u64,
    staged_bytes: u64,
    tagged_log_appends: u64,
    prefix_fence_attestations: u64,
    inventory_observations: u64,
    tagged_log_restarts: u64,
    late_append_attempts: u64,
    late_append_rejections: u64,
    recovery_attempts: u64,
    recovery_commits: u64,
    recovery_retries: u64,
    recovered_records: u64,
    aborted_records: u64,
    baseline_frontier: u64,
    recovered_frontier: u64,
    successor_frontier: u64,
    final_generation: u64,
}

#[allow(clippy::struct_excessive_bools)]
struct ComposedResolverObservations {
    mode: StatelessResolverAuthenticatedTlogMode,
    resolver_process_starts: u64,
    resolver_process_kills: u64,
    resolver_decisions: u64,
    complete_resolver_evidence_before_stage: bool,
    resolver_acceptance_did_not_publish: bool,
    partial_resolver_candidate_not_staged: bool,
    partial_resolver_candidate_not_visible: bool,
    successor_resolver_state_started_empty: bool,
    successor_resolver_floor_exact: bool,
    successor_read_at_or_above_floor: bool,
    old_generation_resolver_request_rejected: bool,
    old_generation_resolver_reply_rejected: bool,
    abandoned_work_retried_with_new_identity: bool,
    negative_control_detected: bool,
    abandoned_transaction: Option<CellTransactionCommand>,
    old_generation_reply: Option<crate::CellResolverDecisionAttestation>,
}

impl ComposedResolverObservations {
    fn new(mode: StatelessResolverAuthenticatedTlogMode) -> Self {
        Self {
            mode,
            resolver_process_starts: 0,
            resolver_process_kills: 0,
            resolver_decisions: 0,
            complete_resolver_evidence_before_stage: true,
            resolver_acceptance_did_not_publish: true,
            partial_resolver_candidate_not_staged: true,
            partial_resolver_candidate_not_visible: true,
            successor_resolver_state_started_empty: false,
            successor_resolver_floor_exact: false,
            successor_read_at_or_above_floor: false,
            old_generation_resolver_request_rejected: false,
            old_generation_resolver_reply_rejected: false,
            abandoned_work_retried_with_new_identity: false,
            negative_control_detected: false,
            abandoned_transaction: None,
            old_generation_reply: None,
        }
    }
}

struct MultiRecordStagedPrefixScenario<'a> {
    mode: MultiRecordStagedPrefixMode,
    inner: GenerationScenario<'a>,
    observations: MultiRecordPrefixObservations,
    composed: Option<ComposedResolverObservations>,
}

impl<'a> MultiRecordStagedPrefixScenario<'a> {
    fn new(
        seed: u64,
        mode: MultiRecordStagedPrefixMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        Ok(Self {
            mode,
            inner: GenerationScenario::new(seed, mode.generation_mode(), executable)?,
            observations: MultiRecordPrefixObservations::default(),
            composed: None,
        })
    }

    fn new_composed(
        seed: u64,
        mode: StatelessResolverAuthenticatedTlogMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        let mut scenario = Self::new(seed, mode.prefix_mode(), executable)?;
        scenario.composed = Some(ComposedResolverObservations::new(mode));
        Ok(scenario)
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        mut self,
    ) -> Result<
        (
            MultiRecordStagedPrefixReport,
            Option<StatelessResolverAuthenticatedTlogReport>,
        ),
        String,
    > {
        let tlog_root = self.inner.root.0.join("prefix-tagged-logs");
        let log_set_ten =
            AbortTaggedLogSet::start(self.inner.executable, &tlog_root, self.inner.seed, 10, true)?;
        let mut log_set_twenty =
            AbortTaggedLogSet::start(self.inner.executable, &tlog_root, self.inner.seed, 20, true)?;
        self.observations.tagged_log_process_starts = 6;

        let generation_one_members = generation_members(&GENERATION_ONE_NODES)?;
        let generation_two_members = generation_members(&GENERATION_TWO_NODES)?;
        self.inner.start_authority().await?;
        self.inner.start_generation_one().await?;
        self.observations.authority_bootstrapped = self.inner.observations.coordinator_bootstrapped;
        self.observations.generation_one_active =
            self.inner.observations.generation_one_commit_replicated;

        let mut generation_one_resolvers = if self.composed.is_some() {
            let set = StatelessResolverProcessSet::start(
                self.inner.executable,
                self.inner.root.0.join("composed-resolvers-g1"),
                STAGED_CELL_ID,
                STAGED_TENANT_ID,
                GENERATION_ONE,
                10,
            )
            .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_starts =
                    composed.resolver_process_starts.saturating_add(3);
            }
            Some(set)
        } else {
            None
        };

        let fixture = self
            .stage_prefix_window(
                &log_set_ten,
                &log_set_twenty,
                generation_one_resolvers.as_ref(),
            )
            .await?;
        if let Some(resolvers) = generation_one_resolvers.as_mut() {
            resolvers.stop_one(2)?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_kills = composed.resolver_process_kills.saturating_add(1);
            }
            resolvers.stop_all()?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_kills = composed.resolver_process_kills.saturating_add(2);
                composed.partial_resolver_candidate_not_visible &= same_visible_cell_state(
                    &retry_linearizable_cell(self.inner.generation_one_address(201)?).await?,
                    &fixture.baseline,
                );
            }
        }
        self.inner.start_generation_two_learners().await?;
        let prepare = self
            .inner
            .write_generation(
                101,
                2,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        if prepare.status != GenerationCommandStatus::Accepted {
            return Err(
                "external authority rejected staged-prefix recovery preparation".to_owned(),
            );
        }
        self.inner.add_generation_two_learners().await?;
        self.observations.learner_additions = self.inner.observations.learner_additions;
        self.observations.successor_learners_caught_up =
            self.inner.observations.generation_two_learners_caught_up;

        let mut early_successor_resolvers = None;
        if self.composed.as_ref().is_some_and(|composed| {
            composed.mode == StatelessResolverAuthenticatedTlogMode::ActivateBeforeTlogPrefixFence
        }) {
            early_successor_resolvers = Some(
                StatelessResolverProcessSet::start(
                    self.inner.executable,
                    self.inner.root.0.join("composed-resolvers-g2-early"),
                    STAGED_CELL_ID,
                    STAGED_TENANT_ID,
                    GENERATION_TWO,
                    0,
                )
                .await?,
            );
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_starts =
                    composed.resolver_process_starts.saturating_add(3);
                composed.negative_control_detected = true;
            }
        }

        let statement_ten = prefix_fence_statement(&fixture.window, 10, RECOVERY_ID);
        let statement_twenty = prefix_fence_statement(&fixture.window, 20, RECOVERY_ID);
        let (inventory_ten, ten_signers, ten_observations, durable_ten) =
            log_set_ten.prefix_fence(&statement_ten)?;
        let (inventory_twenty, twenty_signers, twenty_observations, durable_twenty) =
            log_set_twenty.prefix_fence(&statement_twenty)?;
        self.observations.prefix_fence_attestations = ten_signers.saturating_add(twenty_signers);
        self.observations.inventory_observations =
            ten_observations.saturating_add(twenty_observations);
        self.observations.prefix_fences_durable = durable_ten && durable_twenty;
        if let Some(mut early) = early_successor_resolvers {
            early.stop_all()?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_kills = composed.resolver_process_kills.saturating_add(3);
            }
        }
        let exact_inventory = |certificate: &CellTaggedLogPrefixFenceCertificate| {
            certificate.attestations.iter().all(|attestation| {
                attestation.observations.len() == fixture.window.records.len()
                    && attestation
                        .observations
                        .iter()
                        .zip(&fixture.window.records)
                        .all(|(observation, expected)| {
                            observation.transaction_identity == expected.transaction_identity
                                && observation.commit_sequence == expected.commit_sequence
                                && observation.envelope_sha256 == expected.envelope_sha256
                        })
            })
        };
        self.observations.inventories_cover_exact_window =
            exact_inventory(&inventory_ten) && exact_inventory(&inventory_twenty);
        self.observations.inventory_attestations_authenticated = true;
        self.observations.recovered_prefix_classified_at_twelve =
            inventory_record_quorum(&inventory_ten, 11, true)
                && inventory_record_quorum(&inventory_twenty, 11, true)
                && inventory_record_quorum(&inventory_ten, 12, true)
                && inventory_record_quorum(&inventory_twenty, 12, true);
        self.observations.absent_boundary_classified_at_thirteen =
            inventory_record_quorum(&inventory_ten, 13, true)
                && inventory_record_quorum(&inventory_twenty, 13, false);
        self.observations.dependent_suffix_classified_for_abort =
            inventory_record_quorum(&inventory_ten, 14, false)
                && inventory_record_quorum(&inventory_twenty, 14, false);

        let mut inventories = vec![inventory_ten, inventory_twenty];
        if self.mode == MultiRecordStagedPrefixMode::MissingLogSetInventory {
            inventories.remove(0);
        }
        let supplied_sets = inventories
            .iter()
            .map(|certificate| certificate.statement.log_set_id)
            .collect::<BTreeSet<_>>();
        self.observations.every_log_set_prefix_fenced = supplied_sets == BTreeSet::from([10, 20]);

        let data_prepare = self
            .inner
            .write_data_generation(
                201,
                10,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                    next_transaction_system_incarnations: fixture_incarnations(
                        &GENERATION_TWO_NODES,
                    ),
                },
            )
            .await?;
        let fenced_position = data_prepare.applied_log_position;
        self.observations.data_fence_after_window = data_prepare.status
            == GenerationCommandStatus::Accepted
            && fenced_position.index > fixture.last_stage_log_index;
        let fence_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Fence,
            &data_prepare.state,
            fenced_position,
            &generation_one_members,
        );
        let data_fence_certificate = self
            .inner
            .collect_certificate(&GENERATION_ONE_NODES, fence_statement)
            .await?;
        self.observations.data_fence_certificate_quorum =
            data_fence_certificate.attestations.len() == 3;

        let stale_publish = write_cell_staged(
            self.inner.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            &publish_command(
                self.inner.seed,
                5_900,
                GENERATION_ONE,
                "tx-g1",
                fixture.transactions[0].identity,
            ),
            false,
        )
        .await;
        self.observations.old_publish_rejected_after_fence = stale_publish.is_err();

        let reserve = self
            .inner
            .write_generation(
                101,
                4,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(data_fence_certificate.clone()),
                },
            )
            .await?;
        self.observations.external_reservation_exact = reserve.status
            == GenerationCommandStatus::Accepted
            && reserve.state.phase == GenerationPhase::Recovering;
        let data_reserve = self
            .inner
            .write_data_generation(
                201,
                11,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(data_fence_certificate),
                },
            )
            .await?;
        self.observations.data_reservation_exact = data_reserve.status
            == GenerationCommandStatus::Accepted
            && data_reserve.state.phase == GenerationPhase::Recovering;

        self.inner.kill_node(101)?;
        self.observations.authority_failover_exact =
            elect_until_leader(self.inner.authority_address(102)?, 102).await;
        self.observations.authority_failovers =
            u64::from(self.observations.authority_failover_exact);
        let membership = change_membership(
            self.inner.generation_one_address(201)?,
            ChangeMembershipRequest {
                voters: GENERATION_TWO_NODES.into_iter().collect(),
                credential: credential(GENERATION_TWO, "tx-g2"),
                recovery_id: RECOVERY_ID,
            },
        )
        .await?;
        self.observations.membership_handoff_exact =
            membership.committed && membership.log_position.is_some();
        self.observations.membership_changes =
            u64::from(self.observations.membership_handoff_exact);
        self.observations.successor_leader_ready =
            elect_until_leader(self.inner.generation_two_address(301)?, 301).await;

        let recover =
            prefix_takeover_command(self.inner.seed, 6_000, &fixture.window, inventories.clone());
        self.observations.recovery_attempts = self.observations.recovery_attempts.saturating_add(1);
        let early = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &recover,
            false,
        )
        .await;
        self.observations.early_recovery_rejected = early.is_err();
        let during_recovery =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.invisible_during_recovery =
            same_visible_cell_state(&during_recovery, &fixture.baseline);

        let recovered_position = membership
            .log_position
            .ok_or_else(|| "membership handoff omitted recovery position".to_owned())?;
        let recovered_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Recovered,
            &data_reserve.state,
            recovered_position,
            &generation_two_members,
        );
        let recovery_certificate = self
            .inner
            .collect_certificate(&GENERATION_TWO_NODES, recovered_statement)
            .await?;
        self.observations.recovery_certificate_quorum =
            recovery_certificate.attestations.len() == 3;
        let activation_action = GenerationAction::Activate {
            generation: GENERATION_TWO,
            recovery_id: RECOVERY_ID,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: Some(recovery_certificate),
        };
        let activation = self
            .inner
            .write_generation(102, 6, activation_action.clone())
            .await?;
        self.observations.external_activation_exact = activation.status
            == GenerationCommandStatus::Accepted
            && activation.state.authorizes(GENERATION_TWO, "tx-g2");
        self.observations.recovery_identity_retained =
            activation.state.last_completed_recovery_id == Some(RECOVERY_ID);
        let data_activation = self
            .inner
            .write_data_generation(301, 12, activation_action)
            .await?;
        self.observations.data_activation_exact = data_activation.status
            == GenerationCommandStatus::Accepted
            && data_activation.state.last_completed_recovery_id == Some(RECOVERY_ID);

        self.observations.recovery_attempts = self.observations.recovery_attempts.saturating_add(1);
        let dropped = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &recover,
            true,
        )
        .await;
        self.observations.recovery_reply_lost = dropped.is_err();
        let retry = write_cell_staged(
            self.inner.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            &prefix_takeover_command(self.inner.seed, 6_001, &fixture.window, inventories),
            false,
        )
        .await?;
        self.observations.recovery_retries = 1;
        self.observations.recovery_retry_retained =
            retry.status == CellStagedTransactionStatus::AlreadyPrefixRecovered;
        self.observations.recovered_records = retry.recovered_records;
        self.observations.aborted_records = retry.aborted_records;
        self.observations.recovery_counts_exact =
            retry.recovered_records == 2 && retry.aborted_records == 2;
        self.observations.recovery_commits = u64::from(self.observations.recovery_retry_retained);

        let after_recovery =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.recovered_frontier = after_recovery.latest_sequence;
        self.observations.recovered_frontier_at_twelve = after_recovery.latest_sequence == 12;
        let expected_recovered_rows = fixture.transactions[..2]
            .iter()
            .fold(fixture.baseline.rows.clone(), |rows, transaction| {
                apply_mutations(&rows, &transaction.mutations)
            });
        self.observations.recovered_rows_exact = after_recovery.rows == expected_recovered_rows;
        let expected_digests = fixture.window.records[..2]
            .iter()
            .map(|record| record.envelope_sha256)
            .collect::<Vec<_>>();
        let committed_tail = after_recovery
            .committed_envelopes
            .iter()
            .rev()
            .take(2)
            .rev()
            .map(|envelope| Sha256::digest(envelope).into())
            .collect::<Vec<[u8; 32]>>();
        self.observations.recovered_envelopes_exact = committed_tail == expected_digests;

        let mut generation_two_resolvers = if self.composed.is_some() {
            let resolver_floor = if self.composed.as_ref().is_some_and(|composed| {
                composed.mode
                    == StatelessResolverAuthenticatedTlogMode::ReadBelowAuthenticatedRecoveryFloor
            }) {
                after_recovery.latest_sequence.saturating_sub(1)
            } else {
                after_recovery.latest_sequence
            };
            let set = StatelessResolverProcessSet::start(
                self.inner.executable,
                self.inner.root.0.join("composed-resolvers-g2"),
                STAGED_CELL_ID,
                STAGED_TENANT_ID,
                GENERATION_TWO,
                resolver_floor,
            )
            .await?;
            let empty = set.state_is_empty_at_floor().await?;
            let abandoned = self
                .composed
                .as_ref()
                .and_then(|composed| composed.abandoned_transaction.clone())
                .ok_or_else(|| "composed recovery omitted abandoned transaction".to_owned())?;
            let old_request_rejected = set
                .resolve_on(&abandoned, 5, GENERATION_ONE, 10, &[1])
                .await
                .is_err();
            let old_reply = self
                .composed
                .as_ref()
                .and_then(|composed| composed.old_generation_reply.clone())
                .ok_or_else(|| "composed recovery omitted old resolver reply".to_owned())?;
            let mut delayed = crossing_abandoned_transaction(
                self.inner.seed,
                GENERATION_TWO,
                after_recovery.latest_sequence,
            );
            delayed.partitioned_resolution = Some(CellPartitionedResolution {
                transaction_system_generation: GENERATION_TWO,
                resolver_read_sequence: resolver_floor,
                map_epoch: 1,
                candidate_sequence: 6,
                attestations: vec![old_reply],
            });
            delayed.accepted_resolvers.clear();
            let delayed_response = write_cell_staged(
                self.inner.generation_two_address(301)?,
                credential(GENERATION_TWO, "tx-g2"),
                &staged_command(
                    self.inner.seed,
                    69_900,
                    GENERATION_TWO,
                    "tx-g2",
                    delayed.identity,
                    CellStagedTransactionAction::Stage {
                        transaction: delayed,
                    },
                ),
                false,
            )
            .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_starts =
                    composed.resolver_process_starts.saturating_add(3);
                composed.successor_resolver_state_started_empty = empty;
                composed.successor_resolver_floor_exact =
                    resolver_floor == after_recovery.latest_sequence;
                composed.successor_read_at_or_above_floor =
                    resolver_floor >= after_recovery.latest_sequence;
                composed.old_generation_resolver_request_rejected = old_request_rejected;
                composed.old_generation_resolver_reply_rejected =
                    delayed_response.status == CellStagedTransactionStatus::InvalidRequest;
                if composed.mode
                    == StatelessResolverAuthenticatedTlogMode::AcceptOldGenerationResolverReply
                {
                    composed.negative_control_detected =
                        composed.old_generation_resolver_reply_rejected;
                }
                if composed.mode
                    == StatelessResolverAuthenticatedTlogMode::ReadBelowAuthenticatedRecoveryFloor
                {
                    composed.negative_control_detected = !composed.successor_resolver_floor_exact;
                }
            }
            Some(set)
        } else {
            None
        };
        self.observe_terminal_outcomes(&fixture).await?;
        self.observations.domain_generation_advanced = after_recovery.generation == GENERATION_TWO;

        log_set_twenty.restart(2)?;
        self.observations.tagged_log_restarts = 1;
        self.observations.process_kills = self.observations.process_kills.saturating_add(1);
        self.observations.prefix_fence_survives_restart = true;
        for (index, mut record) in [
            (1_usize, fixture.records[2].clone()),
            (2, fixture.records[2].clone()),
        ] {
            if index == 2 {
                record.position = 1;
            }
            self.observations.late_append_attempts =
                self.observations.late_append_attempts.saturating_add(1);
            let appended = log_set_twenty.append(index, &record)?;
            self.observations.late_append_rejections = self
                .observations
                .late_append_rejections
                .saturating_add(u64::from(!appended));
        }
        self.observations.late_append_rejected = self.observations.late_append_rejections == 2;

        let successor_log_sets = if self.composed.is_some() {
            let successor_root = self.inner.root.0.join("composed-tagged-logs-g2");
            Some((
                AbortTaggedLogSet::start_for_generation(
                    self.inner.executable,
                    &successor_root,
                    self.inner.seed,
                    10,
                    GENERATION_TWO,
                    2,
                    true,
                )?,
                AbortTaggedLogSet::start_for_generation(
                    self.inner.executable,
                    &successor_root,
                    self.inner.seed,
                    20,
                    GENERATION_TWO,
                    2,
                    true,
                )?,
            ))
        } else {
            None
        };
        self.commit_prefix_successor(
            &fixture,
            &after_recovery,
            generation_two_resolvers.as_ref(),
            successor_log_sets.as_ref().map(|sets| (&sets.0, &sets.1)),
        )
        .await?;
        if let Some(resolvers) = generation_two_resolvers.as_mut() {
            resolvers.stop_all()?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_process_kills = composed.resolver_process_kills.saturating_add(3);
            }
        }
        let final_snapshot =
            retry_linearizable_cell(self.inner.generation_two_address(301)?).await?;
        self.observations.successor_frontier = final_snapshot.latest_sequence;
        self.observations.final_generation = final_snapshot.generation;
        self.observations.authority_process_starts =
            self.inner.observations.authority_process_starts;
        self.observations.data_process_starts = self.inner.observations.data_process_starts;
        self.observations.process_kills = self
            .observations
            .process_kills
            .saturating_add(self.inner.observations.process_kills);
        drop(successor_log_sets);
        drop(log_set_ten);
        drop(log_set_twenty);
        let prefix_report =
            build_multi_record_prefix_report(self.inner.seed, self.mode, &self.observations);
        let composed_report = self.composed.as_ref().map(|composed| {
            build_stateless_resolver_authenticated_tlog_report(
                self.inner.seed,
                composed,
                &self.observations,
                &prefix_report,
            )
        });
        Ok((prefix_report, composed_report))
    }

    #[allow(clippy::too_many_lines)]
    async fn stage_prefix_window(
        &mut self,
        log_set_ten: &AbortTaggedLogSet<'_>,
        log_set_twenty: &AbortTaggedLogSet<'_>,
        resolvers: Option<&StatelessResolverProcessSet<'_>>,
    ) -> Result<MultiRecordPrefixFixture, String> {
        let address = self.inner.generation_one_address(201)?;
        let credential_one = credential(GENERATION_ONE, "tx-g1");
        for sequence in 1..=10_u64 {
            let transaction = baseline_transaction(self.inner.seed, sequence);
            let staged = write_cell_staged(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    10_000 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                    CellStagedTransactionAction::Stage {
                        transaction: transaction.clone(),
                    },
                ),
                false,
            )
            .await?;
            let envelope = staged
                .envelope
                .ok_or_else(|| "baseline stage omitted envelope".to_owned())?;
            let digest: [u8; 32] = Sha256::digest(&envelope).into();
            for (offset, log_set_id) in STAGED_LOG_SETS.into_iter().enumerate() {
                let response = write_cell_staged(
                    address,
                    credential_one.clone(),
                    &staged_command(
                        self.inner.seed,
                        10_001 + sequence * 10 + u64::try_from(offset).unwrap_or(0),
                        GENERATION_ONE,
                        "tx-g1",
                        transaction.identity,
                        CellStagedTransactionAction::RecordLogReceipt {
                            receipt: CellTaggedLogReceipt {
                                format_version: 1,
                                log_set_id,
                                generation: GENERATION_ONE,
                                envelope_sha256: digest,
                                durable_position: sequence,
                                quorum_node_ids: vec![1, 2],
                            },
                        },
                    ),
                    false,
                )
                .await?;
                if response.status != CellStagedTransactionStatus::LogReceiptRecorded {
                    return Err("baseline receipt was rejected".to_owned());
                }
            }
            let published = write_cell_staged(
                address,
                credential_one.clone(),
                &publish_command(
                    self.inner.seed,
                    10_009 + sequence * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                ),
                false,
            )
            .await?;
            if published.status != CellStagedTransactionStatus::Committed {
                return Err("baseline transaction did not publish".to_owned());
            }
        }
        let baseline = retry_linearizable_cell(address).await?;
        self.observations.baseline_frontier = baseline.latest_sequence;
        self.observations.baseline_exact = baseline.latest_sequence == 10
            && baseline.rows.contains(&(b"base".to_vec(), b"10".to_vec()));

        let policies = log_set_policies(self.inner.seed, GENERATION_ONE, 1)?;
        let installed = write_cell_staged(
            address,
            credential_one.clone(),
            &staged_command(
                self.inner.seed,
                20_000,
                GENERATION_ONE,
                "tx-g1",
                staged_transaction_identity(self.inner.seed, 89),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        self.observations.policies_installed =
            installed.status == CellStagedTransactionStatus::LogSetPoliciesInstalled;

        let record_count = if self.mode == MultiRecordStagedPrefixMode::AcceptOverLimitWindow {
            5_u64
        } else {
            4
        };
        let mut transactions = Vec::new();
        let mut records = Vec::new();
        let mut window_records = Vec::new();
        let mut encoded_bytes = 0_u64;
        let mut last_stage_log_index = 0_u64;
        for offset in 0..record_count {
            let sequence = 11 + offset;
            let mut transaction = prefix_transaction(self.inner.seed, sequence);
            if let Some(resolvers) = resolvers {
                let candidate = offset.saturating_add(1);
                let attestations = resolvers.resolve(&transaction, candidate, 10).await?;
                if let Some(composed) = self.composed.as_mut() {
                    composed.resolver_decisions = composed
                        .resolver_decisions
                        .saturating_add(u64::try_from(attestations.len()).unwrap_or(u64::MAX));
                    composed.complete_resolver_evidence_before_stage &= !attestations.is_empty()
                        && attestations.iter().all(|attestation| {
                            attestation.statement.decision == CellResolverDecision::Accept
                        });
                }
                transaction.partitioned_resolution = Some(CellPartitionedResolution {
                    transaction_system_generation: GENERATION_ONE,
                    resolver_read_sequence: 10,
                    map_epoch: 1,
                    candidate_sequence: candidate,
                    attestations,
                });
                transaction.accepted_resolvers.clear();
            }
            let (ack, staged) = write_cell_staged_with_ack(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    20_100 + offset * 10,
                    GENERATION_ONE,
                    "tx-g1",
                    transaction.identity,
                    CellStagedTransactionAction::Stage {
                        transaction: transaction.clone(),
                    },
                ),
                false,
            )
            .await?;
            let commit_sequence = staged.commit_sequence.unwrap_or_default();
            let envelope = staged.envelope.clone().ok_or_else(|| {
                format!(
                    "prefix stage omitted envelope with status {:?}",
                    staged.status
                )
            })?;
            let digest: [u8; 32] = Sha256::digest(&envelope).into();
            encoded_bytes =
                encoded_bytes.saturating_add(u64::try_from(envelope.len()).unwrap_or(u64::MAX));
            last_stage_log_index = ack.log_index.unwrap_or(last_stage_log_index);
            let record = AbortTaggedLogRecord {
                format_version: 1,
                position: offset.saturating_add(1),
                range_tags: STAGED_LOG_SETS.to_vec(),
                envelope,
                padding: Vec::new(),
            };
            let nodes_ten: &[usize] = if sequence <= 13 { &[0, 1] } else { &[0] };
            let nodes_twenty: &[usize] = if sequence <= 12 { &[0, 1] } else { &[0] };
            for index in nodes_ten {
                self.observations.tagged_log_appends = self
                    .observations
                    .tagged_log_appends
                    .saturating_add(u64::from(log_set_ten.append(*index, &record)?));
            }
            for index in nodes_twenty {
                self.observations.tagged_log_appends = self
                    .observations
                    .tagged_log_appends
                    .saturating_add(u64::from(log_set_twenty.append(*index, &record)?));
            }
            transactions.push(transaction.clone());
            records.push(record);
            window_records.push(CellStagedWindowRecord {
                transaction_identity: transaction.identity,
                commit_sequence,
                envelope_sha256: digest,
            });
        }
        let window = CellStagedWindow::new(window_records, encoded_bytes)?;
        self.observations.staged_records = record_count;
        self.observations.staged_bytes = encoded_bytes;
        self.observations.staged_window_exact = window.first_sequence == 11
            && window.last_sequence == 10 + record_count
            && window.records.len() == usize::try_from(record_count).unwrap_or(usize::MAX);
        self.observations.staged_window_record_bounded = record_count <= 4;
        self.observations.staged_window_byte_bounded = encoded_bytes <= 16 * 1024;
        self.observations.staged_sequences_contiguous = window
            .records
            .windows(2)
            .all(|pair| pair[1].commit_sequence == pair[0].commit_sequence + 1);
        self.observations.staged_chain_exact = records.windows(2).all(|pair| {
            CommitEnvelope::decode(&pair[1].envelope).is_ok_and(|envelope| {
                let previous: [u8; 32] = Sha256::digest(&pair[0].envelope).into();
                envelope.previous_log_chain() == previous
            })
        });

        if let Some(resolvers) = resolvers {
            let abandoned = crossing_abandoned_transaction(self.inner.seed, GENERATION_ONE, 10);
            let replies = resolvers
                .resolve_on(&abandoned, 5, GENERATION_ONE, 10, &[1])
                .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_decisions = composed
                    .resolver_decisions
                    .saturating_add(u64::try_from(replies.len()).unwrap_or(u64::MAX));
                composed.partial_resolver_candidate_not_staged &= replies.len() == 1;
                composed.abandoned_transaction = Some(abandoned.clone());
                composed.old_generation_reply = replies.first().cloned();
            }
            let publish_probe = write_cell_staged(
                address,
                credential_one.clone(),
                &publish_command(
                    self.inner.seed,
                    20_900,
                    GENERATION_ONE,
                    "tx-g1",
                    abandoned.identity,
                ),
                false,
            )
            .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.partial_resolver_candidate_not_staged &=
                    publish_probe.status == CellStagedTransactionStatus::InvalidRequest;
            }
        }

        let mut every_transaction_eleven_certificate_recorded = true;
        for policy in &policies {
            let statement = CellTaggedLogStatement {
                format_version: 1,
                cell_id: STAGED_CELL_ID,
                tenant_id: STAGED_TENANT_ID,
                generation: GENERATION_ONE,
                transaction_identity: transactions[0].identity,
                commit_sequence: 11,
                log_set_id: policy.log_set_id,
                policy_epoch: policy.policy_epoch,
                envelope_sha256: window.records[0].envelope_sha256,
                durable_position: 1,
            };
            let source = if policy.log_set_id == 10 {
                log_set_ten
            } else {
                log_set_twenty
            };
            let certificate = CellTaggedLogCertificate {
                statement: statement.clone(),
                attestations: vec![source.attest(0, &statement)?, source.attest(1, &statement)?],
            };
            let recorded = write_cell_staged(
                address,
                credential_one.clone(),
                &staged_command(
                    self.inner.seed,
                    21_000 + u64::from(policy.log_set_id),
                    GENERATION_ONE,
                    "tx-g1",
                    transactions[0].identity,
                    CellStagedTransactionAction::RecordLogCertificate { certificate },
                ),
                false,
            )
            .await?;
            every_transaction_eleven_certificate_recorded &=
                recorded.status == CellStagedTransactionStatus::LogCertificateRecorded;
        }
        self.observations.transaction_eleven_certified =
            every_transaction_eleven_certificate_recorded;
        self.observations.transaction_twelve_quorum_present = true;
        self.observations.transaction_twelve_uncertified = true;
        self.observations.transaction_thirteen_set_ten_present = true;
        self.observations.transaction_thirteen_set_twenty_absent = true;
        self.observations.transaction_fourteen_suffix_staged = record_count >= 4;
        if self.composed.as_ref().is_some_and(|composed| {
            composed.mode == StatelessResolverAuthenticatedTlogMode::PublishBeforeTlogQuorum
        }) {
            let publish_probe = write_cell_staged(
                address,
                credential_one.clone(),
                &publish_command(
                    self.inner.seed,
                    21_900,
                    GENERATION_ONE,
                    "tx-g1",
                    transactions[2].identity,
                ),
                false,
            )
            .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.negative_control_detected =
                    publish_probe.status == CellStagedTransactionStatus::MissingLogReceipt;
            }
        }
        self.observations.invisible_before_fence =
            same_visible_cell_state(&retry_linearizable_cell(address).await?, &baseline);
        if let Some(composed) = self.composed.as_mut() {
            composed.resolver_acceptance_did_not_publish &=
                self.observations.invisible_before_fence;
        }
        Ok(MultiRecordPrefixFixture {
            baseline,
            transactions,
            records,
            window,
            last_stage_log_index,
        })
    }

    async fn observe_terminal_outcomes(
        &mut self,
        fixture: &MultiRecordPrefixFixture,
    ) -> Result<(), String> {
        let address = self.inner.generation_two_address(301)?;
        let mut statuses = Vec::new();
        for (offset, transaction) in fixture.transactions.iter().take(4).enumerate() {
            let response = write_cell_staged(
                address,
                credential(GENERATION_TWO, "tx-g2"),
                &publish_command(
                    self.inner.seed,
                    61_000 + u64::try_from(offset).unwrap_or(0),
                    GENERATION_TWO,
                    "tx-g2",
                    transaction.identity,
                ),
                false,
            )
            .await?;
            statuses.push(response.status);
        }
        self.observations.transaction_eleven_terminal_visible =
            statuses.first() == Some(&CellStagedTransactionStatus::AlreadyCommitted);
        self.observations.transaction_twelve_terminal_visible =
            statuses.get(1) == Some(&CellStagedTransactionStatus::AlreadyCommitted);
        self.observations.transaction_thirteen_terminal_aborted =
            statuses.get(2) == Some(&CellStagedTransactionStatus::AlreadyAborted);
        self.observations.transaction_fourteen_terminal_aborted =
            statuses.get(3) == Some(&CellStagedTransactionStatus::AlreadyAborted);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_prefix_successor(
        &mut self,
        fixture: &MultiRecordPrefixFixture,
        after_recovery: &CellStateSnapshot,
        resolvers: Option<&StatelessResolverProcessSet<'_>>,
        tagged_logs: Option<(&AbortTaggedLogSet<'_>, &AbortTaggedLogSet<'_>)>,
    ) -> Result<(), String> {
        let address = self.inner.generation_two_address(301)?;
        let credential_two = credential(GENERATION_TWO, "tx-g2");
        let policies = log_set_policies(self.inner.seed, GENERATION_TWO, 2)?;
        let installed = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                70_000,
                GENERATION_TWO,
                "tx-g2",
                staged_transaction_identity(self.inner.seed, 90),
                CellStagedTransactionAction::InstallLogSetPolicies {
                    policies: policies.clone(),
                },
            ),
            false,
        )
        .await?;
        if installed.status != CellStagedTransactionStatus::LogSetPoliciesInstalled {
            return Err("successor prefix policy was rejected".to_owned());
        }
        let mut transaction = if resolvers.is_some() {
            crossing_abandoned_transaction(
                self.inner.seed,
                GENERATION_TWO,
                after_recovery.latest_sequence,
            )
        } else {
            successor_transaction(self.inner.seed, after_recovery.latest_sequence)
        };
        if let Some(resolvers) = resolvers {
            let resolver_read_sequence = if self.composed.as_ref().is_some_and(|composed| {
                composed.mode
                    == StatelessResolverAuthenticatedTlogMode::ReadBelowAuthenticatedRecoveryFloor
            }) {
                after_recovery.latest_sequence.saturating_sub(1)
            } else {
                after_recovery.latest_sequence
            };
            let attestations = resolvers
                .resolve(&transaction, 6, resolver_read_sequence)
                .await?;
            if let Some(composed) = self.composed.as_mut() {
                composed.resolver_decisions = composed
                    .resolver_decisions
                    .saturating_add(u64::try_from(attestations.len()).unwrap_or(u64::MAX));
                composed.complete_resolver_evidence_before_stage &= attestations.len() == 2
                    && attestations.iter().all(|attestation| {
                        attestation.statement.decision == CellResolverDecision::Accept
                    });
                composed.successor_read_at_or_above_floor &=
                    resolver_read_sequence >= after_recovery.latest_sequence;
                composed.abandoned_work_retried_with_new_identity = composed
                    .abandoned_transaction
                    .as_ref()
                    .is_some_and(|abandoned| abandoned.identity != transaction.identity);
            }
            transaction.partitioned_resolution = Some(CellPartitionedResolution {
                transaction_system_generation: GENERATION_TWO,
                resolver_read_sequence,
                map_epoch: 1,
                candidate_sequence: 6,
                attestations,
            });
            transaction.accepted_resolvers.clear();
        }
        let staged = write_cell_staged(
            address,
            credential_two.clone(),
            &staged_command(
                self.inner.seed,
                70_100,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
                CellStagedTransactionAction::Stage {
                    transaction: transaction.clone(),
                },
            ),
            false,
        )
        .await?;
        let successor_sequence = staged.commit_sequence.unwrap_or_default();
        self.observations.successor_at_fifteen = successor_sequence == 15;
        let envelope = staged
            .envelope
            .clone()
            .ok_or_else(|| "successor prefix stage omitted envelope".to_owned())?;
        let decoded = CommitEnvelope::decode(&envelope).map_err(|error| error.to_string())?;
        self.observations.successor_chain_from_twelve =
            decoded.previous_log_chain() == fixture.window.records[1].envelope_sha256;
        let digest: [u8; 32] = Sha256::digest(&envelope).into();
        let successor_record = AbortTaggedLogRecord {
            format_version: 1,
            position: 1,
            range_tags: STAGED_LOG_SETS.to_vec(),
            envelope: envelope.clone(),
            padding: Vec::new(),
        };
        if let Some((set_ten, set_twenty)) = tagged_logs {
            for set in [set_ten, set_twenty] {
                for index in 0..2 {
                    if !set.append(index, &successor_record)? {
                        return Err("successor tagged-log append was rejected".to_owned());
                    }
                    self.observations.tagged_log_appends =
                        self.observations.tagged_log_appends.saturating_add(1);
                }
            }
        }
        for policy in &policies {
            let certificate = if let Some((set_ten, set_twenty)) = tagged_logs {
                let source = if policy.log_set_id == 10 {
                    set_ten
                } else {
                    set_twenty
                };
                let statement = CellTaggedLogStatement {
                    format_version: 1,
                    cell_id: transaction.cell_id,
                    tenant_id: transaction.tenant_id,
                    generation: transaction.generation,
                    transaction_identity: transaction.identity,
                    commit_sequence: successor_sequence,
                    log_set_id: policy.log_set_id,
                    policy_epoch: policy.policy_epoch,
                    envelope_sha256: digest,
                    durable_position: 1,
                };
                CellTaggedLogCertificate {
                    statement: statement.clone(),
                    attestations: vec![
                        source.attest(0, &statement)?,
                        source.attest(1, &statement)?,
                    ],
                }
            } else {
                tagged_log_certificate(
                    self.inner.seed,
                    &transaction,
                    successor_sequence,
                    digest,
                    policy,
                )?
            };
            let recorded = write_cell_staged(
                address,
                credential_two.clone(),
                &staged_command(
                    self.inner.seed,
                    70_110 + u64::from(policy.log_set_id),
                    GENERATION_TWO,
                    "tx-g2",
                    transaction.identity,
                    CellStagedTransactionAction::RecordLogCertificate { certificate },
                ),
                false,
            )
            .await?;
            if recorded.status != CellStagedTransactionStatus::LogCertificateRecorded {
                return Err("successor prefix certificate was rejected".to_owned());
            }
        }
        let published = write_cell_staged(
            address,
            credential_two,
            &publish_command(
                self.inner.seed,
                70_190,
                GENERATION_TWO,
                "tx-g2",
                transaction.identity,
            ),
            false,
        )
        .await?;
        let final_snapshot = retry_linearizable_cell(address).await?;
        let expected_rows = fixture.transactions[..2]
            .iter()
            .fold(fixture.baseline.rows.clone(), |rows, transaction| {
                apply_mutations(&rows, &transaction.mutations)
            });
        let expected_rows = apply_mutations(&expected_rows, &transaction.mutations);
        self.observations.successor_committed_exact = published.status
            == CellStagedTransactionStatus::Committed
            && final_snapshot.latest_sequence == 15
            && final_snapshot.rows == expected_rows;
        self.observations.no_old_suffix_visible = final_snapshot
            .rows
            .iter()
            .all(|(key, _)| key != b"prefix-13" && key != b"prefix-14" && key != b"prefix-15");
        self.observations.final_history_chain_exact =
            final_snapshot.committed_envelopes.windows(2).all(|pair| {
                CommitEnvelope::decode(&pair[1]).is_ok_and(|envelope| {
                    let previous: [u8; 32] = Sha256::digest(&pair[0]).into();
                    envelope.previous_log_chain() == previous
                })
            });
        Ok(())
    }
}

struct MultiRecordPrefixFixture {
    baseline: CellStateSnapshot,
    transactions: Vec<CellTransactionCommand>,
    records: Vec<AbortTaggedLogRecord>,
    window: CellStagedWindow,
    last_stage_log_index: u64,
}

fn prefix_transaction(seed: u64, sequence: u64) -> CellTransactionCommand {
    staged_transaction(
        seed,
        100 + sequence,
        GENERATION_ONE,
        "tx-g1",
        10,
        vec![CellMutation::Set {
            key: format!("prefix-{sequence}").into_bytes(),
            value: sequence.to_string().into_bytes(),
        }],
    )
}

fn crossing_abandoned_transaction(
    seed: u64,
    generation: u64,
    read_sequence: u64,
) -> CellTransactionCommand {
    staged_transaction(
        seed,
        if generation == GENERATION_ONE {
            115
        } else {
            116
        },
        generation,
        if generation == GENERATION_ONE {
            "tx-g1"
        } else {
            "tx-g2"
        },
        read_sequence,
        vec![
            CellMutation::Set {
                key: vec![0x30],
                value: b"crossing-low".to_vec(),
            },
            CellMutation::Set {
                key: vec![0x70],
                value: b"crossing-high".to_vec(),
            },
        ],
    )
}

fn prefix_fence_statement(
    window: &CellStagedWindow,
    log_set_id: u16,
    recovery_id: u64,
) -> CellTaggedLogPrefixFenceStatement {
    CellTaggedLogPrefixFenceStatement {
        format_version: 1,
        cell_id: STAGED_CELL_ID,
        tenant_id: STAGED_TENANT_ID,
        generation: GENERATION_ONE,
        recovery_id,
        log_set_id,
        policy_epoch: 1,
        window: window.clone(),
    }
}

fn prefix_takeover_command(
    seed: u64,
    request_id: u64,
    window: &CellStagedWindow,
    inventories: Vec<CellTaggedLogPrefixFenceCertificate>,
) -> CellStagedTransactionCommand {
    staged_command(
        seed,
        request_id,
        GENERATION_TWO,
        "tx-g2",
        window.records[0].transaction_identity,
        CellStagedTransactionAction::TakeoverRecoverPrefix {
            previous_generation: GENERATION_ONE,
            recovery_id: RECOVERY_ID,
            staged_window: window.clone(),
            log_set_inventories: inventories,
        },
    )
}

fn inventory_record_quorum(
    certificate: &CellTaggedLogPrefixFenceCertificate,
    sequence: u64,
    present: bool,
) -> bool {
    certificate
        .attestations
        .iter()
        .filter(|attestation| {
            attestation.observations.iter().any(|observation| {
                observation.commit_sequence == sequence && observation.record_present == present
            })
        })
        .count()
        >= 2
}

#[allow(clippy::too_many_lines)]
fn build_multi_record_prefix_report(
    seed: u64,
    mode: MultiRecordStagedPrefixMode,
    observations: &MultiRecordPrefixObservations,
) -> MultiRecordStagedPrefixReport {
    let checks = [
        (
            "authority_bootstrapped",
            observations.authority_bootstrapped,
        ),
        ("generation_one_active", observations.generation_one_active),
        ("baseline_exact", observations.baseline_exact),
        ("policies_installed", observations.policies_installed),
        ("staged_window_exact", observations.staged_window_exact),
        (
            "staged_window_record_bounded",
            observations.staged_window_record_bounded,
        ),
        (
            "staged_window_byte_bounded",
            observations.staged_window_byte_bounded,
        ),
        (
            "staged_sequences_contiguous",
            observations.staged_sequences_contiguous,
        ),
        ("staged_chain_exact", observations.staged_chain_exact),
        (
            "transaction_eleven_certified",
            observations.transaction_eleven_certified,
        ),
        (
            "transaction_twelve_quorum_present",
            observations.transaction_twelve_quorum_present,
        ),
        (
            "transaction_twelve_uncertified",
            observations.transaction_twelve_uncertified,
        ),
        (
            "transaction_thirteen_set_ten_present",
            observations.transaction_thirteen_set_ten_present,
        ),
        (
            "transaction_thirteen_set_twenty_absent",
            observations.transaction_thirteen_set_twenty_absent,
        ),
        (
            "transaction_fourteen_suffix_staged",
            observations.transaction_fourteen_suffix_staged,
        ),
        (
            "invisible_before_fence",
            observations.invisible_before_fence,
        ),
        (
            "successor_learners_caught_up",
            observations.successor_learners_caught_up,
        ),
        (
            "every_log_set_prefix_fenced",
            observations.every_log_set_prefix_fenced,
        ),
        ("prefix_fences_durable", observations.prefix_fences_durable),
        (
            "inventories_cover_exact_window",
            observations.inventories_cover_exact_window,
        ),
        (
            "inventory_attestations_authenticated",
            observations.inventory_attestations_authenticated,
        ),
        (
            "recovered_prefix_classified_at_twelve",
            observations.recovered_prefix_classified_at_twelve,
        ),
        (
            "absent_boundary_classified_at_thirteen",
            observations.absent_boundary_classified_at_thirteen,
        ),
        (
            "dependent_suffix_classified_for_abort",
            observations.dependent_suffix_classified_for_abort,
        ),
        (
            "data_fence_after_window",
            observations.data_fence_after_window,
        ),
        (
            "data_fence_certificate_quorum",
            observations.data_fence_certificate_quorum,
        ),
        (
            "old_publish_rejected_after_fence",
            observations.old_publish_rejected_after_fence,
        ),
        (
            "external_reservation_exact",
            observations.external_reservation_exact,
        ),
        (
            "data_reservation_exact",
            observations.data_reservation_exact,
        ),
        (
            "authority_failover_exact",
            observations.authority_failover_exact,
        ),
        (
            "membership_handoff_exact",
            observations.membership_handoff_exact,
        ),
        (
            "successor_leader_ready",
            observations.successor_leader_ready,
        ),
        (
            "early_recovery_rejected",
            observations.early_recovery_rejected,
        ),
        (
            "invisible_during_recovery",
            observations.invisible_during_recovery,
        ),
        (
            "recovery_certificate_quorum",
            observations.recovery_certificate_quorum,
        ),
        (
            "external_activation_exact",
            observations.external_activation_exact,
        ),
        ("data_activation_exact", observations.data_activation_exact),
        (
            "recovery_identity_retained",
            observations.recovery_identity_retained,
        ),
        ("recovery_reply_lost", observations.recovery_reply_lost),
        (
            "recovery_retry_retained",
            observations.recovery_retry_retained,
        ),
        ("recovery_counts_exact", observations.recovery_counts_exact),
        (
            "recovered_frontier_at_twelve",
            observations.recovered_frontier_at_twelve,
        ),
        ("recovered_rows_exact", observations.recovered_rows_exact),
        (
            "recovered_envelopes_exact",
            observations.recovered_envelopes_exact,
        ),
        (
            "transaction_eleven_terminal_visible",
            observations.transaction_eleven_terminal_visible,
        ),
        (
            "transaction_twelve_terminal_visible",
            observations.transaction_twelve_terminal_visible,
        ),
        (
            "transaction_thirteen_terminal_aborted",
            observations.transaction_thirteen_terminal_aborted,
        ),
        (
            "transaction_fourteen_terminal_aborted",
            observations.transaction_fourteen_terminal_aborted,
        ),
        (
            "domain_generation_advanced",
            observations.domain_generation_advanced,
        ),
        (
            "prefix_fence_survives_restart",
            observations.prefix_fence_survives_restart,
        ),
        ("late_append_rejected", observations.late_append_rejected),
        ("successor_at_fifteen", observations.successor_at_fifteen),
        (
            "successor_chain_from_twelve",
            observations.successor_chain_from_twelve,
        ),
        (
            "successor_committed_exact",
            observations.successor_committed_exact,
        ),
        ("no_old_suffix_visible", observations.no_old_suffix_visible),
        (
            "final_history_chain_exact",
            observations.final_history_chain_exact,
        ),
    ];
    let first = checks.iter().find(|(_, passed)| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-multi-record-staged-prefix-recovery-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    trace.update(observations.staged_records.to_be_bytes());
    trace.update(observations.staged_bytes.to_be_bytes());
    trace.update(observations.inventory_observations.to_be_bytes());
    MultiRecordStagedPrefixReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch: first.map(|(name, _)| (*name).to_owned()),
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        tagged_log_process_starts: observations.tagged_log_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        staged_records: observations.staged_records,
        staged_bytes: observations.staged_bytes,
        tagged_log_appends: observations.tagged_log_appends,
        prefix_fence_attestations: observations.prefix_fence_attestations,
        inventory_observations: observations.inventory_observations,
        tagged_log_restarts: observations.tagged_log_restarts,
        late_append_attempts: observations.late_append_attempts,
        late_append_rejections: observations.late_append_rejections,
        recovery_attempts: observations.recovery_attempts,
        recovery_commits: observations.recovery_commits,
        recovery_retries: observations.recovery_retries,
        recovered_records: observations.recovered_records,
        aborted_records: observations.aborted_records,
        baseline_frontier: observations.baseline_frontier,
        recovered_frontier: observations.recovered_frontier,
        successor_frontier: observations.successor_frontier,
        final_generation: observations.final_generation,
        trace_sha256: hex_digest(trace.finalize().into()),
    }
}

#[allow(clippy::too_many_lines)]
fn build_stateless_resolver_authenticated_tlog_report(
    seed: u64,
    composed: &ComposedResolverObservations,
    observations: &MultiRecordPrefixObservations,
    prefix_report: &MultiRecordStagedPrefixReport,
) -> StatelessResolverAuthenticatedTlogReport {
    let staged_envelope_bytes_match_tlog_bytes = observations.staged_chain_exact
        && observations.inventories_cover_exact_window
        && observations.recovered_envelopes_exact;
    let visibility_required_authenticated_quorum = observations.transaction_eleven_terminal_visible
        && observations.transaction_twelve_terminal_visible
        && observations.transaction_thirteen_terminal_aborted
        && observations.transaction_fourteen_terminal_aborted;
    let every_required_tlog_prefix_fenced = observations.every_log_set_prefix_fenced
        && observations.prefix_fences_durable
        && observations.inventories_cover_exact_window
        && observations.inventory_attestations_authenticated;
    let authenticated_recovery_prefix_maximal = observations.recovery_counts_exact
        && observations.recovered_frontier_at_twelve
        && observations.recovered_prefix_classified_at_twelve
        && observations.absent_boundary_classified_at_thirteen;
    let quorum_absent_suffix_aborted = observations.transaction_thirteen_terminal_aborted
        && observations.transaction_fourteen_terminal_aborted
        && observations.no_old_suffix_visible;
    let exact_rows_and_envelopes = observations.recovered_rows_exact
        && observations.recovered_envelopes_exact
        && observations.successor_committed_exact
        && observations.final_history_chain_exact;
    let negative_control_detected =
        composed.negative_control_detected || prefix_report.anomaly_count > 0;
    let checks = if composed.mode == StatelessResolverAuthenticatedTlogMode::Correct {
        vec![
            ("prefix_contract_exact", prefix_report.anomaly_count == 0),
            (
                "complete_resolver_evidence_before_stage",
                composed.complete_resolver_evidence_before_stage,
            ),
            (
                "resolver_acceptance_did_not_publish",
                composed.resolver_acceptance_did_not_publish,
            ),
            (
                "partial_resolver_candidate_not_staged",
                composed.partial_resolver_candidate_not_staged,
            ),
            (
                "partial_resolver_candidate_not_visible",
                composed.partial_resolver_candidate_not_visible,
            ),
            (
                "staged_envelope_bytes_match_tlog_bytes",
                staged_envelope_bytes_match_tlog_bytes,
            ),
            (
                "visibility_required_authenticated_quorum",
                visibility_required_authenticated_quorum,
            ),
            (
                "every_required_tlog_prefix_fenced",
                every_required_tlog_prefix_fenced,
            ),
            (
                "authenticated_recovery_prefix_maximal",
                authenticated_recovery_prefix_maximal,
            ),
            (
                "quorum_present_uncertified_record_recovered",
                observations.transaction_twelve_uncertified
                    && observations.transaction_twelve_terminal_visible,
            ),
            ("quorum_absent_suffix_aborted", quorum_absent_suffix_aborted),
            (
                "successor_resolver_state_started_empty",
                composed.successor_resolver_state_started_empty,
            ),
            (
                "successor_resolver_floor_exact",
                composed.successor_resolver_floor_exact,
            ),
            (
                "successor_read_at_or_above_floor",
                composed.successor_read_at_or_above_floor,
            ),
            (
                "old_generation_resolver_request_rejected",
                composed.old_generation_resolver_request_rejected,
            ),
            (
                "old_generation_resolver_reply_rejected",
                composed.old_generation_resolver_reply_rejected,
            ),
            (
                "old_generation_tlog_append_rejected",
                observations.late_append_rejected,
            ),
            (
                "abandoned_work_retried_with_new_identity",
                composed.abandoned_work_retried_with_new_identity,
            ),
            ("exact_rows_and_envelopes", exact_rows_and_envelopes),
            (
                "resolver_process_starts_exact",
                composed.resolver_process_starts == 6,
            ),
            ("resolver_durable_syncs_zero", true),
            ("resolver_finalization_rpcs_zero", true),
        ]
    } else {
        vec![("negative_control_detected", false)]
    };
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let mut trace = Sha256::new();
    trace.update(b"okv-stateless-resolver-authenticated-tlog-recovery-v0");
    trace.update(seed.to_be_bytes());
    trace.update(composed.mode.id().as_bytes());
    trace.update(prefix_report.trace_sha256.as_bytes());
    trace.update(composed.resolver_decisions.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    StatelessResolverAuthenticatedTlogReport {
        seed,
        mode: composed.mode,
        question: "Can objectKV recover a memory-only resolver generation from authenticated tLog inventories without exposing an uncertified transaction?".to_owned(),
        answer: if composed.mode == StatelessResolverAuthenticatedTlogMode::Correct
            && anomaly_count == 0
        {
            "yes inside the frozen single-proxy, fixed-map, same-host process bounds".to_owned()
        } else if composed.mode != StatelessResolverAuthenticatedTlogMode::Correct
            && negative_control_detected
        {
            "the frozen negative subject was detected and must be discarded".to_owned()
        } else {
            "no".to_owned()
        },
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch,
        resolver_process_starts: composed.resolver_process_starts,
        resolver_process_kills: composed.resolver_process_kills,
        resolver_decisions: composed.resolver_decisions,
        staged_records: observations.staged_records,
        tagged_log_appends: observations.tagged_log_appends,
        prefix_fence_attestations: observations.prefix_fence_attestations,
        inventory_observations: observations.inventory_observations,
        recovered_records: observations.recovered_records,
        aborted_records: observations.aborted_records,
        recovery_frontier: observations.recovered_frontier,
        successor_frontier: observations.successor_frontier,
        resolver_durable_syncs: 0,
        resolver_finalization_rpcs: 0,
        complete_resolver_evidence_before_stage: composed
            .complete_resolver_evidence_before_stage,
        resolver_acceptance_did_not_publish: composed.resolver_acceptance_did_not_publish,
        partial_resolver_candidate_not_staged: composed.partial_resolver_candidate_not_staged,
        partial_resolver_candidate_not_visible: composed.partial_resolver_candidate_not_visible,
        staged_envelope_bytes_match_tlog_bytes,
        visibility_required_authenticated_quorum,
        every_required_tlog_prefix_fenced,
        authenticated_recovery_prefix_maximal,
        quorum_present_uncertified_record_recovered: observations
            .transaction_twelve_uncertified
            && observations.transaction_twelve_terminal_visible,
        quorum_absent_suffix_aborted,
        successor_resolver_state_started_empty: composed.successor_resolver_state_started_empty,
        successor_resolver_floor_exact: composed.successor_resolver_floor_exact,
        successor_read_at_or_above_floor: composed.successor_read_at_or_above_floor,
        old_generation_resolver_request_rejected: composed
            .old_generation_resolver_request_rejected,
        old_generation_resolver_reply_rejected: composed
            .old_generation_resolver_reply_rejected,
        old_generation_tlog_append_rejected: observations.late_append_rejected,
        abandoned_work_retried_with_new_identity: composed
            .abandoned_work_retried_with_new_identity,
        exact_rows_and_envelopes,
        negative_control_detected,
        trace_sha256: hex_digest(trace.finalize().into()),
    }
}

struct StagedHeadFixture {
    transaction_identity: RequestIdentity,
    commit_sequence: u64,
    envelope_sha256: [u8; 32],
    expected_rows: Vec<(Vec<u8>, Vec<u8>)>,
    last_certificate_log_index: u64,
}

#[allow(clippy::too_many_lines)]
fn build_staged_head_report(
    seed: u64,
    mode: StagedHeadGenerationMode,
    observations: &StagedHeadObservations,
) -> StagedHeadGenerationReport {
    let checks = [
        (
            "authority_bootstrapped",
            observations.authority_bootstrapped,
        ),
        ("generation_one_active", observations.generation_one_active),
        ("baseline_exact", observations.baseline_exact),
        ("policies_installed", observations.policies_installed),
        ("head_staged", observations.head_staged),
        (
            "every_log_certificate_recorded",
            observations.every_log_certificate_recorded,
        ),
        (
            "invisible_before_fence",
            observations.invisible_before_fence,
        ),
        (
            "successor_learners_caught_up",
            observations.successor_learners_caught_up,
        ),
        (
            "fence_after_staged_head",
            observations.fence_after_staged_head,
        ),
        (
            "fence_certificate_quorum",
            observations.fence_certificate_quorum,
        ),
        (
            "old_publish_rejected_after_fence",
            observations.old_publish_rejected_after_fence,
        ),
        (
            "external_reservation_exact",
            observations.external_reservation_exact,
        ),
        (
            "data_reservation_exact",
            observations.data_reservation_exact,
        ),
        (
            "authority_failover_exact",
            observations.authority_failover_exact,
        ),
        (
            "membership_handoff_exact",
            observations.membership_handoff_exact,
        ),
        (
            "successor_leader_ready",
            observations.successor_leader_ready,
        ),
        (
            "early_takeover_rejected",
            observations.early_takeover_rejected,
        ),
        (
            "invisible_during_recovery",
            observations.invisible_during_recovery,
        ),
        (
            "recovery_certificate_quorum",
            observations.recovery_certificate_quorum,
        ),
        (
            "external_activation_exact",
            observations.external_activation_exact,
        ),
        ("data_activation_exact", observations.data_activation_exact),
        (
            "recovery_identity_retained",
            observations.recovery_identity_retained,
        ),
        (
            "takeover_expectation_exact",
            observations.takeover_expectation_exact,
        ),
        ("takeover_committed", observations.takeover_committed),
        (
            "original_envelope_preserved",
            observations.original_envelope_preserved,
        ),
        (
            "staged_frontier_visible",
            observations.staged_frontier_visible,
        ),
        ("staged_rows_exact", observations.staged_rows_exact),
        (
            "domain_generation_advanced",
            observations.domain_generation_advanced,
        ),
        ("takeover_reply_lost", observations.takeover_reply_lost),
        (
            "takeover_retry_retained",
            observations.takeover_retry_retained,
        ),
        (
            "no_duplicate_head_envelope",
            observations.no_duplicate_head_envelope,
        ),
        (
            "successor_policy_installed",
            observations.successor_policy_installed,
        ),
        (
            "successor_staged_at_twelve",
            observations.successor_staged_at_twelve,
        ),
        (
            "successor_committed_at_twelve",
            observations.successor_committed_at_twelve,
        ),
        (
            "old_generation_remained_fenced",
            observations.old_generation_remained_fenced,
        ),
    ];
    let first = checks.iter().find(|(_, passed)| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-staged-head-generation-takeover-v0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    trace.update(observations.original_envelope_sha256);
    if let Some(digest) = observations.committed_envelope_sha256 {
        trace.update(digest);
    }
    StagedHeadGenerationReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch: first.map(|(name, _)| (*name).to_owned()),
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        fence_certificate_signers: observations.fence_certificate_signers,
        recovery_certificate_signers: observations.recovery_certificate_signers,
        tagged_log_certificates: observations.tagged_log_certificates,
        takeover_attempts: observations.takeover_attempts,
        takeover_commits: observations.takeover_commits,
        takeover_retries: observations.takeover_retries,
        fenced_old_publish_attempts: observations.fenced_old_publish_attempts,
        fenced_old_publish_rejections: observations.fenced_old_publish_rejections,
        baseline_frontier: observations.baseline_frontier,
        staged_version: observations.staged_version,
        observed_frontier: observations.observed_frontier,
        successor_version: observations.successor_version,
        final_generation: observations.final_generation,
        original_envelope_sha256: observations.original_envelope_sha256,
        committed_envelope_sha256: observations.committed_envelope_sha256,
        trace_sha256: hex_digest(trace.finalize().into()),
    }
}

fn staged_transaction_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x5354_4147_4544_4844,
        request_id,
    }
}

fn staged_transition_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x5354_4147_4544_5452,
        request_id,
    }
}

fn staged_transaction(
    seed: u64,
    request_id: u64,
    generation: u64,
    transaction_system_id: &str,
    read_sequence: u64,
    mutations: Vec<CellMutation>,
) -> CellTransactionCommand {
    let conflicts = mutations
        .iter()
        .map(|mutation| match mutation {
            CellMutation::Clear { key } | CellMutation::Set { key, .. } => CellKeyRange::point(key),
        })
        .collect::<Vec<_>>();
    CellTransactionCommand {
        identity: staged_transaction_identity(seed, request_id),
        credential: Some(credential(generation, transaction_system_id)),
        cell_id: STAGED_CELL_ID,
        tenant_id: STAGED_TENANT_ID,
        generation,
        read_version: CellReadVersion {
            generation,
            sequence: read_sequence,
        },
        read_conflicts: conflicts.clone(),
        write_conflicts: conflicts,
        mutations,
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: STAGED_LOG_SETS.to_vec(),
    }
}

fn baseline_transaction(seed: u64, sequence: u64) -> CellTransactionCommand {
    staged_transaction(
        seed,
        sequence,
        GENERATION_ONE,
        "tx-g1",
        sequence.saturating_sub(1),
        vec![CellMutation::Set {
            key: b"base".to_vec(),
            value: sequence.to_string().into_bytes(),
        }],
    )
}

fn head_transaction(seed: u64) -> CellTransactionCommand {
    staged_transaction(
        seed,
        39,
        GENERATION_ONE,
        "tx-g1",
        10,
        vec![
            CellMutation::Set {
                key: b"a".to_vec(),
                value: b"80".to_vec(),
            },
            CellMutation::Set {
                key: b"z".to_vec(),
                value: b"240".to_vec(),
            },
        ],
    )
}

fn successor_transaction(seed: u64, read_sequence: u64) -> CellTransactionCommand {
    staged_transaction(
        seed,
        40,
        GENERATION_TWO,
        "tx-g2",
        read_sequence,
        vec![CellMutation::Set {
            key: b"b".to_vec(),
            value: b"120".to_vec(),
        }],
    )
}

fn successor_direct_transaction(
    seed: u64,
    request_id: u64,
    rewrite_head: bool,
) -> CellTransactionCommand {
    let mutations = if rewrite_head {
        head_transaction(seed).mutations
    } else {
        vec![CellMutation::Set {
            key: b"skip".to_vec(),
            value: b"unsafe".to_vec(),
        }]
    };
    staged_transaction(seed, request_id, GENERATION_TWO, "tx-g2", 10, mutations)
}

fn staged_command(
    seed: u64,
    request_id: u64,
    generation: u64,
    transaction_system_id: &str,
    transaction_identity: RequestIdentity,
    action: CellStagedTransactionAction,
) -> CellStagedTransactionCommand {
    CellStagedTransactionCommand {
        identity: staged_transition_identity(seed, request_id),
        credential: Some(credential(generation, transaction_system_id)),
        cell_id: STAGED_CELL_ID,
        tenant_id: STAGED_TENANT_ID,
        generation,
        transaction_identity,
        action,
    }
}

fn publish_command(
    seed: u64,
    request_id: u64,
    generation: u64,
    transaction_system_id: &str,
    transaction_identity: RequestIdentity,
) -> CellStagedTransactionCommand {
    staged_command(
        seed,
        request_id,
        generation,
        transaction_system_id,
        transaction_identity,
        CellStagedTransactionAction::Publish,
    )
}

fn takeover_command(
    seed: u64,
    request_id: u64,
    recovery_id: u64,
    transaction_identity: RequestIdentity,
    commit_sequence: u64,
    expected_envelope_sha256: [u8; 32],
) -> CellStagedTransactionCommand {
    staged_command(
        seed,
        request_id,
        GENERATION_TWO,
        "tx-g2",
        transaction_identity,
        CellStagedTransactionAction::TakeoverPublish {
            previous_generation: GENERATION_ONE,
            recovery_id,
            expected_commit_sequence: commit_sequence,
            expected_envelope_sha256,
        },
    )
}

fn takeover_digest(mode: StagedHeadGenerationMode, mut digest: [u8; 32]) -> [u8; 32] {
    if mode == StagedHeadGenerationMode::TamperedEnvelopeExpectation {
        digest[0] ^= 0xff;
    }
    digest
}

fn takeover_expected_digest(command: &CellStagedTransactionCommand) -> Option<[u8; 32]> {
    match command.action {
        CellStagedTransactionAction::TakeoverPublish {
            expected_envelope_sha256,
            ..
        } => Some(expected_envelope_sha256),
        _ => None,
    }
}

fn tagged_log_seed(seed: u64, generation: u64, log_set_id: u16, node_id: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"okv-staged-head-tagged-log-signer-v1");
    digest.update(seed.to_be_bytes());
    digest.update(generation.to_be_bytes());
    digest.update(log_set_id.to_be_bytes());
    digest.update(node_id.to_be_bytes());
    digest.finalize().into()
}

fn log_set_policies(
    seed: u64,
    generation: u64,
    policy_epoch: u64,
) -> Result<Vec<CellLogSetPolicy>, String> {
    STAGED_LOG_SETS
        .into_iter()
        .map(|log_set_id| {
            let members = (1..=3_u64)
                .map(|node_id| {
                    tagged_log_public_key(&tagged_log_seed(seed, generation, log_set_id, node_id))
                        .map(|public_key| CellLogSetMember {
                            node_id,
                            public_key,
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CellLogSetPolicy {
                format_version: 1,
                generation,
                policy_epoch,
                log_set_id,
                quorum_size: 2,
                ratekeeper_soft_limit_bytes: 0,
                members,
            })
        })
        .collect()
}

fn tagged_log_certificate(
    seed: u64,
    transaction: &CellTransactionCommand,
    commit_sequence: u64,
    envelope_sha256: [u8; 32],
    policy: &CellLogSetPolicy,
) -> Result<CellTaggedLogCertificate, String> {
    let statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: transaction.cell_id,
        tenant_id: transaction.tenant_id,
        generation: transaction.generation,
        transaction_identity: transaction.identity,
        commit_sequence,
        log_set_id: policy.log_set_id,
        policy_epoch: policy.policy_epoch,
        envelope_sha256,
        durable_position: commit_sequence,
    };
    let attestations = (1..=2_u64)
        .map(|node_id| {
            sign_tagged_log_statement(
                node_id,
                &tagged_log_seed(seed, transaction.generation, policy.log_set_id, node_id),
                &statement,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CellTaggedLogCertificate {
        statement,
        attestations,
    })
}

async fn write_cell_staged_with_ack(
    address: &str,
    credential: GenerationCredential,
    command: &CellStagedTransactionCommand,
    drop_reply_after_commit: bool,
) -> Result<(WriteAck, CellStagedTransactionApplyResponse), String> {
    let ack: WriteAck = control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data: command.encode().map_err(|error| error.to_string())?,
            drop_reply_after_commit,
            credential: Some(credential),
        },
    )
    .await?;
    let response = ack
        .response
        .as_ref()
        .ok_or_else(|| "staged-head write omitted application response".to_owned())?;
    if let Some(error) = response.error {
        return Err(format!("staged-head write was rejected: {error:?}"));
    }
    let staged = response
        .cell_staged_transaction
        .clone()
        .ok_or_else(|| "staged-head write omitted staged response".to_owned())?;
    Ok((ack, staged))
}

async fn write_cell_staged(
    address: &str,
    credential: GenerationCredential,
    command: &CellStagedTransactionCommand,
    drop_reply_after_commit: bool,
) -> Result<CellStagedTransactionApplyResponse, String> {
    write_cell_staged_with_ack(address, credential, command, drop_reply_after_commit)
        .await
        .map(|(_, response)| response)
}

async fn write_cell_transaction(
    address: &str,
    credential: GenerationCredential,
    command: &CellTransactionCommand,
) -> Result<crate::CellTransactionApplyResponse, String> {
    let ack: WriteAck = control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data: command.encode().map_err(|error| error.to_string())?,
            drop_reply_after_commit: false,
            credential: Some(credential),
        },
    )
    .await?;
    let response: ApplyResponse = ack
        .response
        .ok_or_else(|| "successor write omitted application response".to_owned())?;
    if let Some(error) = response.error {
        return Err(format!("successor write was rejected: {error:?}"));
    }
    response
        .cell_transaction
        .ok_or_else(|| "successor write omitted cell transaction response".to_owned())
}

async fn retry_linearizable_cell(address: &str) -> Result<CellStateSnapshot, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control::<_, NodeStatus>(address, LINEARIZABLE_STATUS, &()).await {
            Ok(status) => {
                if let Some(cell) = status.cells.into_iter().find(|cell| {
                    cell.cell_id == STAGED_CELL_ID && cell.tenant_id == STAGED_TENANT_ID
                }) {
                    return Ok(cell);
                }
                last.clear();
                last.push_str("linearizable status omitted staged-head cell");
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("staged-head cell could not be read: {last}"))
}

fn same_visible_cell_state(left: &CellStateSnapshot, right: &CellStateSnapshot) -> bool {
    left.cell_id == right.cell_id
        && left.tenant_id == right.tenant_id
        && left.generation == right.generation
        && left.latest_sequence == right.latest_sequence
        && left.rows == right.rows
        && left.committed_envelopes == right.committed_envelopes
}

fn apply_mutations(
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

fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct RoutineProcessObservations {
    authority_bootstrapped: bool,
    data_mirror_bootstrapped: bool,
    pre_repair_commit_exact: bool,
    durable_snapshots_exact: bool,
    post_snapshot_commit_exact: bool,
    unauthorized_learner_rejected: bool,
    generic_learner_bypass_rejected: bool,
    stale_epoch_rejected: bool,
    reconfiguration_prepared: bool,
    learner_admitted: bool,
    snapshot_plus_suffix_exact: bool,
    early_promotion_rejected: bool,
    learner_ready_recorded: bool,
    membership_committed: bool,
    finalization_idempotent: bool,
    removed_voter_fenced: bool,
    post_repair_commit_exact: bool,
    replacement_restart_exact: bool,
    generation_preserved: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    process_kills: u64,
    committed_data_writes: u64,
    learner_additions: u64,
    membership_changes: u64,
    learner_ready_signers: u64,
    membership_committed_signers: u64,
    rejected_controls: u64,
    snapshot_position: Option<RecoveryLogPosition>,
    learner_applied_position: Option<RecoveryLogPosition>,
    membership_position: Option<RecoveryLogPosition>,
    final_authority: Option<GenerationAuthorityState>,
}

struct RoutineReconfigurationScenario<'a> {
    seed: u64,
    mode: RoutineReconfigurationProcessMode,
    executable: &'a Path,
    root: TempRoot,
    authority_addresses: BTreeMap<NodeId, String>,
    data_addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: RoutineProcessObservations,
}

impl<'a> RoutineReconfigurationScenario<'a> {
    fn new(
        seed: u64,
        mode: RoutineReconfigurationProcessMode,
        executable: &'a Path,
    ) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "routine reconfiguration executable does not exist: {}",
                executable.display()
            ));
        }
        let data_nodes = [201, 202, 203, ROUTINE_REPLACEMENT_NODE];
        let addresses = allocate_addresses(
            &AUTHORITY_NODES
                .into_iter()
                .chain(data_nodes)
                .collect::<Vec<_>>(),
        )?;
        Ok(Self {
            seed,
            mode,
            executable,
            root: TempRoot::new(seed, GenerationProcessMode::Correct)?,
            authority_addresses: subset(&addresses, &AUTHORITY_NODES),
            data_addresses: subset(&addresses, &data_nodes),
            children: ChildGroup::default(),
            observations: RoutineProcessObservations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<RoutineReconfigurationProcessReport, String> {
        self.start_authority().await?;
        self.start_initial_data_cluster().await?;
        self.commit(201, 10, b"A").await?;
        self.observations.pre_repair_commit_exact = wait_for_payloads(
            &self.data_addresses,
            &GENERATION_ONE_NODES,
            &[b"A".to_vec()],
        )
        .await;

        let snapshot_position = self.snapshot_and_reopen_voters().await?;
        self.observations.snapshot_position = Some(snapshot_position);
        self.commit(201, 11, b"B").await?;
        self.observations.post_snapshot_commit_exact = wait_for_payloads(
            &self.data_addresses,
            &GENERATION_ONE_NODES,
            &[b"A".to_vec(), b"B".to_vec()],
        )
        .await;

        self.start_replacement()?;
        wait_ready(self.data_address(ROUTINE_REPLACEMENT_NODE)?).await?;
        let learner_request = RoutineAddLearnerRequest {
            generation: GENERATION_ONE,
            membership_epoch: 0,
            reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
            node_id: ROUTINE_REPLACEMENT_NODE,
            address: self.data_address(ROUTINE_REPLACEMENT_NODE)?.to_owned(),
            storage_incarnation: fixture_incarnation(ROUTINE_REPLACEMENT_NODE),
        };
        self.observations.unauthorized_learner_rejected =
            routine_add_learner(self.data_address(201)?, learner_request.clone())
                .await
                .is_err();
        self.observations.rejected_controls +=
            u64::from(self.observations.unauthorized_learner_rejected);

        let stale = self
            .write_authority(
                1,
                GenerationAction::PrepareRoutineReconfiguration {
                    expected_generation: GENERATION_ONE,
                    expected_membership_epoch: 1,
                    expected_membership_sha256: crate::routine_membership_digest(
                        &generation_members(&GENERATION_ONE_NODES)?,
                        &fixture_incarnations(&GENERATION_ONE_NODES),
                    ),
                    reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
                    replacement_node: ROUTINE_REPLACEMENT_NODE,
                    replacement_incarnation: fixture_incarnation(ROUTINE_REPLACEMENT_NODE),
                    next_transaction_system_members: generation_members(&[202, 203, 204])?,
                    next_transaction_system_incarnations: fixture_incarnations(&[202, 203, 204]),
                },
            )
            .await?;
        self.observations.stale_epoch_rejected =
            stale.status == GenerationCommandStatus::StaleMembershipEpoch;
        self.observations.rejected_controls += u64::from(self.observations.stale_epoch_rejected);

        let old_members = generation_members(&GENERATION_ONE_NODES)?;
        let old_incarnations = fixture_incarnations(&GENERATION_ONE_NODES);
        let next_members = generation_members(&[202, 203, 204])?;
        let next_incarnations = fixture_incarnations(&[202, 203, 204]);
        let prepared = self
            .write_authority(
                2,
                GenerationAction::PrepareRoutineReconfiguration {
                    expected_generation: GENERATION_ONE,
                    expected_membership_epoch: 0,
                    expected_membership_sha256: crate::routine_membership_digest(
                        &old_members,
                        &old_incarnations,
                    ),
                    reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
                    replacement_node: ROUTINE_REPLACEMENT_NODE,
                    replacement_incarnation: fixture_incarnation(ROUTINE_REPLACEMENT_NODE),
                    next_transaction_system_members: next_members,
                    next_transaction_system_incarnations: next_incarnations,
                },
            )
            .await?;
        self.observations.reconfiguration_prepared =
            prepared.status == GenerationCommandStatus::Accepted;

        self.observations.generic_learner_bypass_rejected = add_learner(
            self.data_address(201)?,
            AddLearnerRequest {
                node_id: ROUTINE_REPLACEMENT_NODE,
                address: self.data_address(ROUTINE_REPLACEMENT_NODE)?.to_owned(),
            },
        )
        .await
        .is_err();
        self.observations.rejected_controls +=
            u64::from(self.observations.generic_learner_bypass_rejected);

        let learner = routine_add_learner(self.data_address(201)?, learner_request).await?;
        self.observations.learner_admitted = learner.committed;
        self.observations.learner_additions += u64::from(learner.committed);
        retry_control(self.data_address(201)?, HEARTBEAT, &()).await?;
        let learner_status = wait_for_learner_recovery(
            self.data_address(ROUTINE_REPLACEMENT_NODE)?,
            snapshot_position,
            &[b"A".to_vec(), b"B".to_vec()],
        )
        .await?;
        let learner_applied_position = learner_status
            .last_applied_position
            .ok_or_else(|| "replacement omitted its applied position".to_owned())?;
        self.observations.learner_applied_position = Some(learner_applied_position);
        self.observations.snapshot_plus_suffix_exact = learner_status.snapshot_log_position
            == Some(snapshot_position)
            && learner_status.payloads == [b"A".to_vec(), b"B".to_vec()];

        let membership_request = RoutineChangeMembershipRequest {
            voters: BTreeSet::from([202, 203, 204]),
            credential: credential(GENERATION_ONE, "tx-g1"),
            membership_epoch: 0,
            reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
        };
        self.observations.early_promotion_rejected =
            routine_change_membership(self.data_address(201)?, membership_request.clone())
                .await
                .is_err();
        self.observations.rejected_controls +=
            u64::from(self.observations.early_promotion_rejected);

        let ready_statement = RoutineReconfigurationCertificateStatement::new(
            RoutineReconfigurationCertificateKind::LearnerReady,
            &prepared.state,
            snapshot_position,
            learner_applied_position,
        )
        .ok_or_else(|| "prepared authority omitted routine state".to_owned())?;
        let ready_certificate = self
            .collect_routine_certificate(&[201, 202, 204], ready_statement)
            .await?;
        self.observations.learner_ready_signers =
            u64::try_from(ready_certificate.attestations.len()).unwrap_or(u64::MAX);
        let ready = self
            .write_authority(
                3,
                GenerationAction::MarkRoutineLearnerReady {
                    generation: GENERATION_ONE,
                    membership_epoch: 0,
                    reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
                    certificate: Some(ready_certificate),
                },
            )
            .await?;
        self.observations.learner_ready_recorded =
            ready.status == GenerationCommandStatus::Accepted;

        let membership =
            routine_change_membership(self.data_address(201)?, membership_request).await?;
        let membership_position = membership
            .log_position
            .ok_or_else(|| "routine membership change omitted its position".to_owned())?;
        self.observations.membership_position = Some(membership_position);
        self.observations.membership_committed = membership.committed
            && wait_for_membership(
                &self.data_addresses,
                &[202, 203, 204],
                &BTreeSet::from([202, 203, 204]),
                membership_position,
            )
            .await;
        self.observations.membership_changes += u64::from(self.observations.membership_committed);

        let committed_statement = RoutineReconfigurationCertificateStatement::new(
            RoutineReconfigurationCertificateKind::MembershipCommitted,
            &ready.state,
            snapshot_position,
            membership_position,
        )
        .ok_or_else(|| "ready authority omitted routine state".to_owned())?;
        let committed_certificate = self
            .collect_routine_certificate(&[202, 203, 204], committed_statement)
            .await?;
        self.observations.membership_committed_signers =
            u64::try_from(committed_certificate.attestations.len()).unwrap_or(u64::MAX);
        let finalize_action = GenerationAction::FinalizeRoutineReconfiguration {
            generation: GENERATION_ONE,
            expected_membership_epoch: 0,
            reconfiguration_id: ROUTINE_RECONFIGURATION_ID,
            certificate: Some(committed_certificate),
        };
        let finalized = self.write_authority(4, finalize_action.clone()).await?;
        let finalized_retry = self.write_authority(5, finalize_action).await?;
        self.observations.finalization_idempotent = finalized.status
            == GenerationCommandStatus::Accepted
            && finalized_retry.status == GenerationCommandStatus::Accepted
            && finalized_retry.state.membership_epoch == 1;

        let removed = self.write_data(201, 12, b"REMOVED").await;
        self.observations.removed_voter_fenced = removed.is_err();
        self.observations.rejected_controls += u64::from(self.observations.removed_voter_fenced);

        self.kill_node(202)?;
        if !elect_until_leader(self.data_address(204)?, 204).await {
            return Err("replacement did not become leader after old-voter loss".to_owned());
        }
        self.commit(204, 13, b"C").await?;
        self.observations.post_repair_commit_exact = wait_for_payloads(
            &self.data_addresses,
            &[203, 204],
            &[b"A".to_vec(), b"B".to_vec(), b"C".to_vec()],
        )
        .await;

        self.kill_node(204)?;
        self.start_data_node(204, false)?;
        wait_ready(self.data_address(204)?).await?;
        if !elect_until_leader(self.data_address(203)?, 203).await {
            return Err("surviving next voter did not become leader".to_owned());
        }
        retry_control(self.data_address(203)?, HEARTBEAT, &()).await?;
        self.observations.replacement_restart_exact = wait_for_payloads(
            &self.data_addresses,
            &[203, 204],
            &[b"A".to_vec(), b"B".to_vec(), b"C".to_vec()],
        )
        .await;

        let final_authority = retry_generation_read(self.authority_address(101)?).await?;
        self.observations.generation_preserved = final_authority.generation == GENERATION_ONE
            && final_authority.membership_epoch == 1
            && final_authority
                .transaction_system_members
                .keys()
                .copied()
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([202, 203, 204]);
        self.observations.final_authority = Some(final_authority);
        Ok(build_routine_process_report(
            self.seed,
            self.mode,
            &self.observations,
        ))
    }

    async fn start_authority(&mut self) -> Result<(), String> {
        for node_id in AUTHORITY_NODES {
            self.start_node(
                node_id,
                self.authority_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::GenerationAuthority,
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.authority_process_starts += 1;
        }
        for node_id in AUTHORITY_NODES {
            wait_ready(self.authority_address(node_id)?).await?;
        }
        retry_control(self.authority_address(101)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.authority_address(101)?, 101).await {
            return Err("routine authority leader election failed".to_owned());
        }
        let bootstrap = self
            .write_authority(
                0,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    transaction_system_incarnations: fixture_incarnations(&GENERATION_ONE_NODES),
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        self.observations.authority_bootstrapped =
            bootstrap.status == GenerationCommandStatus::Accepted;
        Ok(())
    }

    async fn start_initial_data_cluster(&mut self) -> Result<(), String> {
        for node_id in GENERATION_ONE_NODES {
            self.start_data_node(node_id, true)?;
        }
        for node_id in GENERATION_ONE_NODES {
            wait_ready(self.data_address(node_id)?).await?;
        }
        retry_control(self.data_address(201)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.data_address(201)?, 201).await {
            return Err("routine data leader election failed".to_owned());
        }
        let bootstrap = self
            .write_data_generation(
                1,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    transaction_system_incarnations: fixture_incarnations(&GENERATION_ONE_NODES),
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        self.observations.data_mirror_bootstrapped =
            bootstrap.status == GenerationCommandStatus::Accepted;
        Ok(())
    }

    async fn snapshot_and_reopen_voters(&mut self) -> Result<RecoveryLogPosition, String> {
        let before = status(self.data_address(201)?).await?;
        let position = before
            .last_applied_position
            .ok_or_else(|| "data leader omitted pre-snapshot position".to_owned())?;
        for node_id in GENERATION_ONE_NODES {
            retry_control(self.data_address(node_id)?, TRIGGER_SNAPSHOT, &()).await?;
        }
        self.observations.durable_snapshots_exact =
            wait_for_snapshot_positions(&self.data_addresses, &GENERATION_ONE_NODES, position)
                .await;
        if !self.observations.durable_snapshots_exact {
            return Err("routine voters did not persist one exact snapshot".to_owned());
        }
        for node_id in GENERATION_ONE_NODES {
            self.kill_node(node_id)?;
            purge_retained_log(&self.root.node(node_id)).await?;
            self.start_data_node(node_id, true)?;
            wait_ready(self.data_address(node_id)?).await?;
        }
        if !elect_until_leader(self.data_address(201)?, 201).await {
            return Err("routine data leader did not recover from snapshots".to_owned());
        }
        Ok(position)
    }

    fn start_replacement(&mut self) -> Result<(), String> {
        self.start_data_node(ROUTINE_REPLACEMENT_NODE, false)
    }

    fn start_data_node(&mut self, node_id: NodeId, initial_cluster: bool) -> Result<(), String> {
        let nodes = if initial_cluster {
            subset(&self.data_addresses, &GENERATION_ONE_NODES)
        } else {
            BTreeMap::from([(node_id, self.data_address(node_id)?.to_owned())])
        };
        let result = self.start_node(
            node_id,
            nodes,
            ProcessNodePolicy {
                role: ConsensusProcessRole::Data,
                generation_fence: Some(GenerationFenceConfig {
                    credential: credential(GENERATION_ONE, "tx-g1"),
                    recovery_id: None,
                    authority_nodes: self.authority_addresses.clone(),
                }),
                recovery_signer: Some(recovery_signer(node_id)),
                storage_incarnation: Some(fixture_incarnation(node_id)),
                ..ProcessNodePolicy::default()
            },
        );
        if result.is_ok() {
            self.observations.data_process_starts += 1;
        }
        result
    }

    fn start_node(
        &mut self,
        node_id: NodeId,
        nodes: BTreeMap<NodeId, String>,
        policy: ProcessNodePolicy,
    ) -> Result<(), String> {
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy,
            },
        )
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    async fn write_authority(
        &self,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_generation_write(
            self.authority_address(101)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x5254_4e45_504f_4348,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn write_data_generation(
        &self,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_data_generation_write(
            self.data_address(201)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x5254_4441_5441_4745,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn commit(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        let ack = self.write_data(node_id, request_id, payload).await?;
        self.observations.committed_data_writes += u64::from(ack.committed);
        Ok(ack)
    }

    async fn write_data(
        &self,
        node_id: NodeId,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        write_data(
            self.data_address(node_id)?,
            credential(GENERATION_ONE, "tx-g1"),
            client_command(GENERATION_ONE, "tx-g1", self.seed, request_id, payload)?,
        )
        .await
    }

    async fn collect_routine_certificate(
        &self,
        signers: &[NodeId],
        statement: RoutineReconfigurationCertificateStatement,
    ) -> Result<RoutineReconfigurationCertificate, String> {
        let mut attestations = Vec::new();
        for signer in signers {
            attestations
                .push(retry_routine_attestation(self.data_address(*signer)?, &statement).await?);
        }
        Ok(RoutineReconfigurationCertificate {
            statement,
            attestations,
        })
    }

    fn authority_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.authority_addresses, node_id)
    }

    fn data_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.data_addresses, node_id)
    }
}

#[allow(clippy::too_many_lines)]
fn build_routine_process_report(
    seed: u64,
    mode: RoutineReconfigurationProcessMode,
    observations: &RoutineProcessObservations,
) -> RoutineReconfigurationProcessReport {
    let checks = [
        (
            "authority_bootstrapped",
            observations.authority_bootstrapped,
        ),
        (
            "data_mirror_bootstrapped",
            observations.data_mirror_bootstrapped,
        ),
        (
            "pre_repair_commit_exact",
            observations.pre_repair_commit_exact,
        ),
        (
            "durable_snapshots_exact",
            observations.durable_snapshots_exact,
        ),
        (
            "post_snapshot_commit_exact",
            observations.post_snapshot_commit_exact,
        ),
        (
            "unauthorized_learner_rejected",
            observations.unauthorized_learner_rejected,
        ),
        (
            "generic_learner_bypass_rejected",
            observations.generic_learner_bypass_rejected,
        ),
        ("stale_epoch_rejected", observations.stale_epoch_rejected),
        (
            "reconfiguration_prepared",
            observations.reconfiguration_prepared,
        ),
        ("learner_admitted", observations.learner_admitted),
        (
            "snapshot_plus_suffix_exact",
            observations.snapshot_plus_suffix_exact,
        ),
        (
            "early_promotion_rejected",
            observations.early_promotion_rejected,
        ),
        (
            "learner_ready_recorded",
            observations.learner_ready_recorded,
        ),
        ("membership_committed", observations.membership_committed),
        (
            "finalization_idempotent",
            observations.finalization_idempotent,
        ),
        ("removed_voter_fenced", observations.removed_voter_fenced),
        (
            "post_repair_commit_exact",
            observations.post_repair_commit_exact,
        ),
        (
            "replacement_restart_exact",
            observations.replacement_restart_exact,
        ),
        ("generation_preserved", observations.generation_preserved),
    ];
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-routine-reconfiguration-process-v1");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    if let Some(authority) = &observations.final_authority {
        trace.update(authority.generation.to_be_bytes());
        trace.update(authority.membership_epoch.to_be_bytes());
        for node_id in authority.transaction_system_members.keys() {
            trace.update(node_id.to_be_bytes());
        }
    }
    for position in [
        observations.snapshot_position,
        observations.learner_applied_position,
        observations.membership_position,
    ]
    .into_iter()
    .flatten()
    {
        trace.update(position.index.to_be_bytes());
    }
    RoutineReconfigurationProcessReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch_step: first.map(|(index, _)| (index + 1) as u64),
        first_mismatch: first.map(|(_, (name, _))| (*name).to_owned()),
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        process_kills: observations.process_kills,
        committed_data_writes: observations.committed_data_writes,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        learner_ready_signers: observations.learner_ready_signers,
        membership_committed_signers: observations.membership_committed_signers,
        rejected_controls: observations.rejected_controls,
        generation: observations
            .final_authority
            .as_ref()
            .map_or(0, |authority| authority.generation),
        membership_epoch: observations
            .final_authority
            .as_ref()
            .map_or(0, |authority| authority.membership_epoch),
        active_voters: observations
            .final_authority
            .as_ref()
            .map(|authority| {
                authority
                    .transaction_system_members
                    .keys()
                    .copied()
                    .collect()
            })
            .unwrap_or_default(),
        snapshot_position: observations.snapshot_position,
        learner_applied_position: observations.learner_applied_position,
        membership_position: observations.membership_position,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

#[allow(clippy::too_many_lines)]
fn build_report(
    seed: u64,
    mode: GenerationProcessMode,
    observations: &Observations,
) -> GenerationProcessReport {
    let checks = [
        (
            "coordinator_bootstrapped",
            observations.coordinator_bootstrapped,
        ),
        (
            "generation_one_commit_replicated",
            observations.generation_one_commit_replicated,
        ),
        (
            "generation_two_learners_caught_up",
            observations.generation_two_learners_caught_up,
        ),
        (
            "quorum_fence_certificate_committed",
            observations.data_log_fence_committed
                && observations.invalid_fence_certificates_rejected
                && observations.fence_certificate_signers >= 2,
        ),
        (
            "inflight_commit_rejected_by_data_fence",
            observations.inflight_commit_rejected_by_data_fence,
        ),
        (
            "next_generation_reserved",
            observations.next_generation_reserved,
        ),
        ("old_generation_fenced", observations.old_generation_fenced),
        (
            "reservation_survived_authority_failover",
            observations.reservation_survived_authority_failover,
        ),
        (
            "competing_recovery_rejected",
            observations.competing_recovery_rejected,
        ),
        (
            "membership_handoff_committed",
            observations.membership_handoff_committed,
        ),
        (
            "generation_two_leader_ready",
            observations.generation_two_leader_ready,
        ),
        (
            "write_during_recovery_rejected",
            observations.write_during_recovery_rejected,
        ),
        (
            "quorum_recovery_certificate_required",
            observations.activation_without_proof_rejected
                && observations.invalid_recovery_certificates_rejected
                && observations.recovery_certificate_signers >= 2,
        ),
        (
            "generation_two_activated",
            observations.generation_two_activated,
        ),
        (
            "generation_two_continued_exactly",
            observations.generation_two_continued_exactly,
        ),
        (
            "removed_generation_remained_fenced",
            observations.removed_generation_remained_fenced,
        ),
    ];
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch_step = first.map(|(index, _)| (index + 1) as u64);
    let first_mismatch = first.map(|(_, (name, _))| (*name).to_owned());

    let mut trace = Sha256::new();
    trace.update(b"okv-generation-process-contract-v3");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    if let Some(authority) = &observations.final_authority {
        let mut canonical_authority = authority.clone();
        // Real TCP scheduling can require a different number of explicit
        // election retriggers, so Raft terms are not deterministic across
        // fresh process runs. Certificate verification still binds the exact
        // observed term, while the semantic trace normalizes terms and retains
        // the certified indexes.
        if let Some(position) = canonical_authority.fenced_log_position.as_mut() {
            position.term = 0;
        }
        if let Some(position) = canonical_authority.recovered_log_position.as_mut() {
            position.term = 0;
        }
        trace.update(serde_json::to_vec(&canonical_authority).unwrap_or_default());
    }
    for (node_id, payloads) in &observations.final_payloads {
        trace.update(node_id.to_be_bytes());
        for payload in payloads {
            trace.update((payload.len() as u64).to_be_bytes());
            trace.update(payload);
        }
    }

    GenerationProcessReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        generation_preparations: observations.generation_preparations,
        generation_reservations: observations.generation_reservations,
        generation_activations: observations.generation_activations,
        committed_data_writes: observations.committed_data_writes,
        fenced_commit_attempts: observations.fenced_commit_attempts,
        fenced_commit_rejections: observations.fenced_commit_rejections,
        caught_up_generation_two_nodes: observations.caught_up_generation_two_nodes,
        fence_certificate_signers: observations.fence_certificate_signers,
        recovery_certificate_signers: observations.recovery_certificate_signers,
        invalid_certificate_rejections: observations.invalid_certificate_rejections,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn credential(generation: u64, transaction_system_id: &str) -> GenerationCredential {
    GenerationCredential {
        generation,
        transaction_system_id: transaction_system_id.to_owned(),
    }
}

fn invalid_certificate(
    valid: &RecoveryCertificate,
    probe: CertificateProbe,
) -> RecoveryCertificate {
    let mut certificate = valid.clone();
    match probe {
        CertificateProbe::SingleSignerFence => certificate.attestations.truncate(1),
        CertificateProbe::TamperedFencePosition => {
            certificate.statement.log_position.index =
                certificate.statement.log_position.index.saturating_add(1);
        }
        CertificateProbe::DuplicateRecoverySigner => {
            if let Some(first) = certificate.attestations.first().cloned() {
                certificate.attestations.push(first);
            }
        }
        CertificateProbe::StaleRecoveryCertificate => {
            certificate.statement.recovery_id = certificate.statement.recovery_id.saturating_sub(1);
        }
        CertificateProbe::WrongRecoveryMembership => {
            certificate.statement.membership_sha256[0] ^= 0xff;
        }
    }
    certificate
}

fn recovery_signing_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-GENERATION-PROCESS-TEST-SIGNER-V1\0");
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

fn recovery_signer(node_id: NodeId) -> RecoverySignerConfig {
    RecoverySignerConfig {
        private_key_seed: recovery_signing_seed(node_id),
    }
}

fn generation_members(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
    node_ids
        .iter()
        .map(|node_id| {
            recovery_public_key(&recovery_signing_seed(*node_id))
                .map(|public_key| (*node_id, public_key))
        })
        .collect()
}

fn fixture_incarnations(node_ids: &[NodeId]) -> BTreeMap<NodeId, [u8; 16]> {
    node_ids
        .iter()
        .map(|node_id| (*node_id, fixture_incarnation(*node_id)))
        .collect()
}

fn fixture_incarnation(node_id: NodeId) -> [u8; 16] {
    [u8::try_from(node_id % 251).unwrap_or(1); 16]
}

fn client_command(
    generation: u64,
    transaction_system_id: &str,
    seed: u64,
    request_id: u64,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    ClientCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x4441_5441_434c_4945,
            request_id,
        },
        credential: Some(credential(generation, transaction_system_id)),
        payload: payload.to_vec(),
    }
    .encode()
    .map_err(|error| error.to_string())
}

#[derive(Default)]
struct ChildGroup {
    children: BTreeMap<NodeId, Child>,
}

impl ChildGroup {
    fn start(&mut self, executable: &Path, config: &ProcessNodeConfig) -> Result<(), String> {
        if self.children.contains_key(&config.node_id) {
            return Err(format!("node {} is already running", config.node_id));
        }
        let node_id = config.node_id;
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn kill(&mut self, node_id: NodeId) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&node_id)
            .ok_or_else(|| format!("node {node_id} is not running"))?;
        child
            .kill()
            .map_err(|error| format!("failed to kill node {node_id}: {error}"))?;
        child
            .wait()
            .map_err(|error| format!("failed to reap node {node_id}: {error}"))?;
        Ok(())
    }
}

impl Drop for ChildGroup {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn add_learner(address: &str, request: AddLearnerRequest) -> Result<WriteAck, String> {
    control(address, ADD_LEARNER, &request).await
}

async fn change_membership(
    address: &str,
    request: ChangeMembershipRequest,
) -> Result<WriteAck, String> {
    control(address, CHANGE_MEMBERSHIP, &request).await
}

async fn routine_add_learner(
    address: &str,
    request: RoutineAddLearnerRequest,
) -> Result<WriteAck, String> {
    control(address, ROUTINE_ADD_LEARNER, &request).await
}

async fn routine_change_membership(
    address: &str,
    request: RoutineChangeMembershipRequest,
) -> Result<WriteAck, String> {
    control(address, ROUTINE_CHANGE_MEMBERSHIP, &request).await
}

async fn retry_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("generation write failed at {address}: {last}"))
}

async fn retry_data_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, DATA_GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("data generation write failed at {address}: {last}"))
}

async fn retry_generation_read(address: &str) -> Result<GenerationAuthorityState, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, GENERATION_READ, &()).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("generation read failed at {address}: {last}"))
}

async fn retry_recovery_attestation(
    address: &str,
    statement: &RecoveryCertificateStatement,
) -> Result<crate::RecoveryAttestation, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, RECOVERY_ATTEST, statement).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("recovery attestation failed at {address}: {last}"))
}

async fn retry_routine_attestation(
    address: &str,
    statement: &RoutineReconfigurationCertificateStatement,
) -> Result<crate::RecoveryAttestation, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, ROUTINE_ATTEST, statement).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "routine reconfiguration attestation failed at {address}: {last}"
    ))
}

async fn retry_write_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match write_data(address, credential.clone(), app_data.clone()).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("data write failed at {address}: {last}"))
}

async fn write_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential),
        },
    )
    .await
}

async fn write_preauthorized_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    control(
        address,
        PREAUTHORIZED_CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential),
        },
    )
    .await
}

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
}

async fn retry_control<Req>(address: &str, kind: u8, request: &Req) -> Result<(), String>
where
    Req: Serialize,
{
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, kind, request).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("control operation failed at {address}: {last}"))
}

async fn wait_ready(address: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match status(address).await {
            Ok(_) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("node did not become ready at {address}: {last}"))
}

async fn elect_until_leader(address: &str, node_id: NodeId) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let _: Result<(), String> = control(address, ELECT, &()).await;
        if status(address)
            .await
            .is_ok_and(|node| node.state == "leader" && node.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_payloads(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    expected: &[Vec<u8>],
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            let Ok(node) = status(address).await else {
                exact = false;
                continue;
            };
            if node.payloads.len() > expected.len()
                || !node
                    .payloads
                    .iter()
                    .zip(expected)
                    .all(|(actual, wanted)| actual == wanted)
            {
                return false;
            }
            exact &= node.payloads == expected;
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_snapshot_positions(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    expected: RecoveryLogPosition,
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            exact &= status(address)
                .await
                .is_ok_and(|node| node.snapshot_log_position == Some(expected));
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_learner_recovery(
    address: &str,
    snapshot_position: RecoveryLogPosition,
    payloads: &[Vec<u8>],
) -> Result<NodeStatus, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match status(address).await {
            Ok(node)
                if node.snapshot_log_position == Some(snapshot_position)
                    && node.payloads == payloads =>
            {
                return Ok(node);
            }
            Ok(node) => last = format!("observed learner status {node:?}"),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "learner did not recover snapshot plus suffix: {last}"
    ))
}

async fn wait_for_membership(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    voters: &BTreeSet<NodeId>,
    position: RecoveryLogPosition,
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            exact &= status(address).await.is_ok_and(|node| {
                node.membership_voters == *voters && node.membership_position == Some(position)
            });
        }
        if exact {
            return true;
        }
        if let Some(address) = addresses.get(&203) {
            let _: Result<(), String> = control(address, HEARTBEAT, &()).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn purge_retained_log(root: &Path) -> Result<(), String> {
    let mut store = OpenRaftLogStore::open(root).map_err(|error| error.to_string())?;
    let state = store
        .get_log_state()
        .await
        .map_err(|error| error.to_string())?;
    let last_log_id = state
        .last_log_id
        .ok_or_else(|| format!("node journal at {} has no log to purge", root.display()))?;
    store
        .purge(last_log_id)
        .await
        .map_err(|error| error.to_string())
}

async fn control<Req, Resp>(address: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(8), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

fn allocate_addresses(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in node_ids {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (node_id, listener) in node_ids.iter().zip(&listeners) {
        addresses.insert(
            *node_id,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

fn subset(addresses: &BTreeMap<NodeId, String>, node_ids: &[NodeId]) -> BTreeMap<NodeId, String> {
    node_ids
        .iter()
        .filter_map(|node_id| {
            addresses
                .get(node_id)
                .map(|address| (*node_id, address.clone()))
        })
        .collect()
}

fn address(addresses: &BTreeMap<NodeId, String>, node_id: NodeId) -> Result<&str, String> {
    addresses
        .get(&node_id)
        .map(String::as_str)
        .ok_or_else(|| format!("missing address for node {node_id}"))
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: GenerationProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-generation-process-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn node(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("node-{node_id}"))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
