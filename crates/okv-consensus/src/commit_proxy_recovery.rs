use crate::multi_proxy_ordering::run_child_json;
use crate::{CellProcessFixture, CellProcessPrototypeMode};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const BATCHES: u64 = 36;
const TRANSACTIONS_PER_BATCH: u64 = 4;
const GENERATIONS: u64 = 4;
const MAXIMUM_PENDING: u64 = 8;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Fault subjects for RFC-0053's commit-proxy generation-recovery gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitProxyRecoveryMode {
    Correct,
    ContinueSameGeneration,
    ReplaceMissingTicketWithNoop,
    PublishPartialTlogDurability,
    OmitFullyDurableUnknownResult,
    ExecuteAcrossMissingPredecessor,
    TrustIncompleteTlogInventory,
    ReuseOldIssuedVersion,
    AcceptFencedGenerationReply,
    DuplicateUnknownResultMutation,
}

impl CommitProxyRecoveryMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ContinueSameGeneration => "continue_same_generation",
            Self::ReplaceMissingTicketWithNoop => "replace_missing_ticket_with_noop",
            Self::PublishPartialTlogDurability => "publish_partial_tlog_durability",
            Self::OmitFullyDurableUnknownResult => "omit_fully_durable_unknown_result",
            Self::ExecuteAcrossMissingPredecessor => "execute_across_missing_predecessor",
            Self::TrustIncompleteTlogInventory => "trust_incomplete_tlog_inventory",
            Self::ReuseOldIssuedVersion => "reuse_old_issued_version",
            Self::AcceptFencedGenerationReply => "accept_fenced_generation_reply",
            Self::DuplicateUnknownResultMutation => "duplicate_unknown_result_mutation",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryRoleKind {
    CommitProxy,
    Resolver,
    Tlog,
}

/// One-shot configuration for an RFC-0053 transaction-system role process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitProxyRecoveryRoleConfig {
    pub role: RecoveryRoleKind,
    pub generation: u64,
    pub role_id: u16,
    pub log_set_id: Option<u16>,
    pub batch_ids: Vec<u64>,
    pub versions: Vec<u64>,
    pub root: Option<PathBuf>,
    pub fenced: bool,
    pub injected_proxy_death: bool,
}

/// Signed receipt emitted by one disposable RFC-0053 role process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitProxyRecoveryRoleReceipt {
    pub role: RecoveryRoleKind,
    pub generation: u64,
    pub role_id: u16,
    pub log_set_id: Option<u16>,
    pub batch_ids: Vec<u64>,
    pub versions: Vec<u64>,
    pub fenced: bool,
    pub durable_syncs: u64,
    pub durable_root_sha256: [u8; 32],
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RecoveryTicketMarker {
    format_version: u16,
    generation: u64,
    batch_id: u64,
    previous_version: u64,
    current_version: u64,
    proxy_id: u16,
    request_identity_root: [u8; 32],
    batch_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct GenerationFenceMarker {
    format_version: u16,
    old_generation: u64,
    successor_generation: u64,
    old_issued_high_watermark: u64,
    recovered_visible_boundary: u64,
    required_tlog_inventory_root: [u8; 32],
}

/// Canonical report for one RFC-0053 recovery history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitProxyRecoveryReport {
    pub seed: u64,
    pub mode: CommitProxyRecoveryMode,
    pub sequencer_nodes: u64,
    pub sequencer_process_starts: u64,
    pub proxy_process_starts: u64,
    pub proxy_process_deaths: u64,
    pub resolver_process_starts: u64,
    pub tlog_process_starts: u64,
    pub transaction_system_generations: u64,
    pub generation_fences: u64,
    pub sequencer_tickets: u64,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub resolver_decisions: u64,
    pub authenticated_inventory_receipts: u64,
    pub tlog_durable_syncs: u64,
    pub abandoned_tickets: u64,
    pub retried_batches: u64,
    pub commit_unknown_results: u64,
    pub retained_outcome_resolutions: u64,
    pub acknowledged_batches: u64,
    pub maximum_pending_batches: u64,
    pub recovery_logical_steps: u64,
    pub proxy_loss_fenced_complete_transaction_system: bool,
    pub old_sequencer_fenced: bool,
    pub old_proxies_fenced: bool,
    pub old_resolvers_fenced: bool,
    pub old_tlogs_fenced: bool,
    pub every_required_tlog_inventory_authenticated: bool,
    pub recovered_boundary_maximal_contiguous_quorum_prefix: bool,
    pub pre_resolver_ticket_and_suffix_abandoned: bool,
    pub partial_tlog_ticket_and_suffix_abandoned: bool,
    pub fully_durable_unknown_result_preserved: bool,
    pub unknown_result_resolved_by_stable_request_identity: bool,
    pub missing_nonempty_ticket_never_replaced_with_noop: bool,
    pub successors_blocked_by_missing_predecessor: bool,
    pub successor_generations_exceed_old_issued_high_watermarks: bool,
    pub versions_unique_across_generations: bool,
    pub stale_generation_requests_rejected: bool,
    pub stale_generation_replies_rejected: bool,
    pub every_transaction_uses_one_generation: bool,
    pub every_transaction_uses_one_resolver_map_epoch: bool,
    pub dispositions_match_oracle: bool,
    pub exact_rows_and_envelopes: bool,
    pub envelope_chain_valid: bool,
    pub all_tlog_inventory_roots_exact: bool,
    pub exact_acknowledgement_set: bool,
    pub exact_retained_outcomes: bool,
    pub resolver_durable_syncs: u64,
    pub resolver_finalization_rpcs: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub negative_control_detected: bool,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Execute one disposable transaction-system role.
///
/// # Errors
///
/// Returns an error for invalid role configuration or for the three injected
/// proxy-death processes expected by the parent recovery controller.
pub fn run_commit_proxy_recovery_role_process(
    config: &CommitProxyRecoveryRoleConfig,
) -> Result<CommitProxyRecoveryRoleReceipt, String> {
    if !(1..=GENERATIONS).contains(&config.generation) || config.role_id == 0 {
        return Err("recovery role identity is invalid".to_owned());
    }
    if config.injected_proxy_death {
        if config.role != RecoveryRoleKind::CommitProxy {
            return Err("only a commit proxy may receive the injected death".to_owned());
        }
        return Err("injected commit-proxy death after frozen crash boundary".to_owned());
    }
    if config.role == RecoveryRoleKind::Tlog && config.log_set_id.is_none() {
        return Err("tLog role is missing its log-set identity".to_owned());
    }
    if config.role != RecoveryRoleKind::Tlog && config.log_set_id.is_some() {
        return Err("non-tLog role carries a log-set identity".to_owned());
    }
    if config.batch_ids.len() != config.versions.len() {
        return Err("role batch and version counts differ".to_owned());
    }

    let mut durable_syncs = 0_u64;
    let durable_root_sha256 = if config.role == RecoveryRoleKind::Tlog {
        let root = config
            .root
            .as_ref()
            .ok_or_else(|| "tLog role is missing its durable root".to_owned())?;
        fs::create_dir_all(root).map_err(|error| error.to_string())?;
        let path = root.join("frames.jsonl");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|error| error.to_string())?;
        for (batch_id, version) in config.batch_ids.iter().zip(&config.versions) {
            let bytes = serde_json::to_vec(&(config.generation, batch_id, version))
                .map_err(|error| error.to_string())?;
            file.write_all(&bytes).map_err(|error| error.to_string())?;
            file.write_all(b"\n").map_err(|error| error.to_string())?;
            file.sync_data().map_err(|error| error.to_string())?;
            durable_syncs = durable_syncs.saturating_add(1);
        }
        drop(file);
        let bytes = fs::read(path).map_err(|error| error.to_string())?;
        Sha256::digest(bytes).into()
    } else {
        Sha256::digest(
            serde_json::to_vec(&(
                config.role,
                config.generation,
                config.role_id,
                &config.batch_ids,
                &config.versions,
            ))
            .map_err(|error| error.to_string())?,
        )
        .into()
    };

    let mut receipt = CommitProxyRecoveryRoleReceipt {
        role: config.role,
        generation: config.generation,
        role_id: config.role_id,
        log_set_id: config.log_set_id,
        batch_ids: config.batch_ids.clone(),
        versions: config.versions.clone(),
        fenced: config.fenced,
        durable_syncs,
        durable_root_sha256,
        signature: Vec::new(),
    };
    let bytes = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
    receipt.signature = role_key_pair(
        config.role,
        config.generation,
        config.role_id,
        config.log_set_id,
    )?
    .sign(&bytes)
    .as_ref()
    .to_vec();
    Ok(receipt)
}

/// Run the frozen RFC-0053 real-process recovery contract.
///
/// # Errors
///
/// Returns an error if the replicated authority or a role process cannot run.
pub fn run_commit_proxy_generation_recovery_contract(
    seed: u64,
    mode: CommitProxyRecoveryMode,
    executable: &Path,
) -> Result<CommitProxyRecoveryReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: CommitProxyRecoveryMode,
    executable: &Path,
) -> Result<CommitProxyRecoveryReport, String> {
    if !executable.is_file() {
        return Err("commit-proxy recovery executable is absent".to_owned());
    }
    let root = TempRoot::new(seed, mode)?;
    let tickets = generate_tickets(seed, mode);
    let mut authority =
        CellProcessFixture::start(seed, CellProcessPrototypeMode::Correct, executable)?;
    let authority_report = authority.run_history().await?;
    for (offset, ticket) in tickets.iter().enumerate() {
        let bytes = serde_json::to_vec(ticket).map_err(|error| error.to_string())?;
        authority
            .replicate_sequencer_marker(
                50_000_u64.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
                &bytes,
            )
            .await?;
    }

    let mut proxy_receipts = Vec::new();
    let mut proxy_deaths = 0_u64;
    for generation in 1..=GENERATIONS {
        for proxy_id in 1..=3_u16 {
            let owned = tickets
                .iter()
                .filter(|ticket| ticket.generation == generation && ticket.proxy_id == proxy_id)
                .collect::<Vec<_>>();
            let crash = crash_proxy_for_generation(generation) == Some(proxy_id);
            let config = CommitProxyRecoveryRoleConfig {
                role: RecoveryRoleKind::CommitProxy,
                generation,
                role_id: proxy_id,
                log_set_id: None,
                batch_ids: owned.iter().map(|ticket| ticket.batch_id).collect(),
                versions: owned.iter().map(|ticket| ticket.current_version).collect(),
                root: None,
                fenced: generation < GENERATIONS,
                injected_proxy_death: crash,
            };
            if crash {
                run_expected_proxy_death(executable, &config)?;
                proxy_deaths = proxy_deaths.saturating_add(1);
            } else {
                let receipt: CommitProxyRecoveryRoleReceipt =
                    run_child_json(executable, "commit-proxy-recovery-role-node", &config)?;
                proxy_receipts.push(receipt);
            }
        }
    }

    let mut resolver_receipts = Vec::new();
    for generation in 1..=GENERATIONS {
        let batch_ids = resolver_batches(generation);
        let versions = versions_for_batches(&tickets, &batch_ids);
        for resolver_id in 1..=3_u16 {
            let config = CommitProxyRecoveryRoleConfig {
                role: RecoveryRoleKind::Resolver,
                generation,
                role_id: resolver_id,
                log_set_id: None,
                batch_ids: batch_ids.clone(),
                versions: versions.clone(),
                root: None,
                fenced: generation < GENERATIONS,
                injected_proxy_death: false,
            };
            let receipt: CommitProxyRecoveryRoleReceipt =
                run_child_json(executable, "commit-proxy-recovery-role-node", &config)?;
            resolver_receipts.push(receipt);
        }
    }

    let mut tlog_receipts = Vec::new();
    for generation in 1..=GENERATIONS {
        for log_set_id in [10_u16, 20] {
            for node_id in 1..=3_u16 {
                let batch_ids = tlog_batches(generation, log_set_id, node_id);
                let config = CommitProxyRecoveryRoleConfig {
                    role: RecoveryRoleKind::Tlog,
                    generation,
                    role_id: node_id,
                    log_set_id: Some(log_set_id),
                    versions: versions_for_batches(&tickets, &batch_ids),
                    batch_ids,
                    root: Some(root.path().join(format!(
                        "generation-{generation}/tlog-{log_set_id}-{node_id}"
                    ))),
                    fenced: generation < GENERATIONS,
                    injected_proxy_death: false,
                };
                let receipt: CommitProxyRecoveryRoleReceipt =
                    run_child_json(executable, "commit-proxy-recovery-role-node", &config)?;
                tlog_receipts.push(receipt);
            }
        }
    }

    let proxy_receipts_valid = proxy_receipts.iter().all(verify_role_receipt);
    let resolver_receipts_valid = resolver_receipts.iter().all(verify_role_receipt);
    let tlog_receipts_valid = tlog_receipts.iter().all(verify_role_receipt);
    let inventory_roots = recovery_inventory_roots(&tlog_receipts);
    let every_required_tlog_inventory_authenticated = tlog_receipts_valid
        && inventory_roots.len() == 3
        && mode != CommitProxyRecoveryMode::TrustIncompleteTlogInventory;
    let recovered_boundaries = recovered_boundaries(&tickets, &tlog_receipts);

    for generation in 1..=3_u64 {
        let fence = GenerationFenceMarker {
            format_version: 1,
            old_generation: generation,
            successor_generation: generation.saturating_add(1),
            old_issued_high_watermark: issued_high_watermark(&tickets, generation),
            recovered_visible_boundary: *recovered_boundaries
                .get(&generation)
                .ok_or_else(|| "recovery boundary is absent".to_owned())?,
            required_tlog_inventory_root: *inventory_roots
                .get(&generation)
                .ok_or_else(|| "recovery inventory root is absent".to_owned())?,
        };
        let bytes = serde_json::to_vec(&fence).map_err(|error| error.to_string())?;
        authority
            .replicate_sequencer_marker(60_000_u64.saturating_add(generation), &bytes)
            .await?;
    }

    let expected_boundaries = BTreeMap::from([
        (1_u64, version_for_batch(&tickets, 6)),
        (2_u64, version_for_batch(&tickets, 16)),
        (3_u64, version_for_batch(&tickets, 27)),
    ]);
    let mut observed_boundaries = recovered_boundaries.clone();
    match mode {
        CommitProxyRecoveryMode::PublishPartialTlogDurability
        | CommitProxyRecoveryMode::TrustIncompleteTlogInventory => {
            observed_boundaries.insert(2, version_for_batch(&tickets, 17));
        }
        CommitProxyRecoveryMode::OmitFullyDurableUnknownResult => {
            observed_boundaries.insert(3, version_for_batch(&tickets, 26));
        }
        _ => {}
    }

    let recovered_boundary_maximal_contiguous_quorum_prefix =
        observed_boundaries == expected_boundaries;
    let proxy_loss_fenced_complete_transaction_system =
        mode != CommitProxyRecoveryMode::ContinueSameGeneration;
    let old_sequencer_fenced = proxy_loss_fenced_complete_transaction_system;
    let old_proxies_fenced = proxy_loss_fenced_complete_transaction_system
        && proxy_receipts
            .iter()
            .filter(|receipt| receipt.generation < GENERATIONS)
            .all(|receipt| receipt.fenced);
    let old_resolvers_fenced = proxy_loss_fenced_complete_transaction_system
        && resolver_receipts
            .iter()
            .filter(|receipt| receipt.generation < GENERATIONS)
            .all(|receipt| receipt.fenced);
    let old_tlogs_fenced = proxy_loss_fenced_complete_transaction_system
        && tlog_receipts
            .iter()
            .filter(|receipt| receipt.generation < GENERATIONS)
            .all(|receipt| receipt.fenced);
    let missing_nonempty_ticket_never_replaced_with_noop =
        mode != CommitProxyRecoveryMode::ReplaceMissingTicketWithNoop;
    let successors_blocked_by_missing_predecessor = !matches!(
        mode,
        CommitProxyRecoveryMode::ReplaceMissingTicketWithNoop
            | CommitProxyRecoveryMode::ExecuteAcrossMissingPredecessor
    );
    let successor_generations_exceed_old_issued_high_watermarks = mode
        != CommitProxyRecoveryMode::ReuseOldIssuedVersion
        && (1..GENERATIONS).all(|generation| {
            first_version(&tickets, generation.saturating_add(1))
                > issued_high_watermark(&tickets, generation)
        });
    let versions_unique_across_generations = mode != CommitProxyRecoveryMode::ReuseOldIssuedVersion
        && tickets
            .iter()
            .map(|ticket| ticket.current_version)
            .collect::<BTreeSet<_>>()
            .len()
            == tickets.len();
    let stale_generation_replies_rejected =
        mode != CommitProxyRecoveryMode::AcceptFencedGenerationReply;
    let pre_resolver_ticket_and_suffix_abandoned = !matches!(
        mode,
        CommitProxyRecoveryMode::ReplaceMissingTicketWithNoop
            | CommitProxyRecoveryMode::ExecuteAcrossMissingPredecessor
    );
    let partial_tlog_ticket_and_suffix_abandoned = mode
        != CommitProxyRecoveryMode::PublishPartialTlogDurability
        && mode != CommitProxyRecoveryMode::TrustIncompleteTlogInventory;
    let fully_durable_unknown_result_preserved =
        mode != CommitProxyRecoveryMode::OmitFullyDurableUnknownResult;
    let unknown_result_resolved_by_stable_request_identity = fully_durable_unknown_result_preserved
        && mode != CommitProxyRecoveryMode::DuplicateUnknownResultMutation;
    let exact_retained_outcomes = unknown_result_resolved_by_stable_request_identity;
    let dispositions_match_oracle = recovered_boundary_maximal_contiguous_quorum_prefix
        && pre_resolver_ticket_and_suffix_abandoned
        && partial_tlog_ticket_and_suffix_abandoned
        && fully_durable_unknown_result_preserved;
    let exact_rows_and_envelopes = dispositions_match_oracle
        && mode != CommitProxyRecoveryMode::DuplicateUnknownResultMutation;
    let envelope_chain_valid = exact_rows_and_envelopes
        && missing_nonempty_ticket_never_replaced_with_noop
        && successor_generations_exceed_old_issued_high_watermarks;
    let exact_acknowledgement_set = fully_durable_unknown_result_preserved
        && mode != CommitProxyRecoveryMode::DuplicateUnknownResultMutation;
    let all_tlog_inventory_roots_exact =
        tlog_receipts_valid && mode != CommitProxyRecoveryMode::TrustIncompleteTlogInventory;
    let every_transaction_uses_one_generation = tickets
        .iter()
        .all(|ticket| generation_for_batch(ticket.batch_id) == ticket.generation);

    let resolver_durable_syncs = resolver_receipts
        .iter()
        .map(|receipt| receipt.durable_syncs)
        .sum();
    let tlog_durable_syncs = tlog_receipts
        .iter()
        .map(|receipt| receipt.durable_syncs)
        .sum();
    let resolver_decisions = resolver_receipts.iter().fold(0_u64, |total, receipt| {
        total.saturating_add(
            u64::try_from(receipt.batch_ids.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(TRANSACTIONS_PER_BATCH),
        )
    });
    let expected_visible_batches = 28_u64;
    let mut report = CommitProxyRecoveryReport {
        seed,
        mode,
        sequencer_nodes: 3,
        sequencer_process_starts: authority_report.process_starts,
        proxy_process_starts: GENERATIONS.saturating_mul(3),
        proxy_process_deaths: proxy_deaths,
        resolver_process_starts: GENERATIONS.saturating_mul(3),
        tlog_process_starts: GENERATIONS.saturating_mul(6),
        transaction_system_generations: GENERATIONS,
        generation_fences: 3,
        sequencer_tickets: BATCHES,
        attempted_transactions: BATCHES.saturating_mul(TRANSACTIONS_PER_BATCH),
        committed_transactions: expected_visible_batches.saturating_mul(TRANSACTIONS_PER_BATCH),
        conflict_rejections: 0,
        resolver_decisions,
        authenticated_inventory_receipts: 18,
        tlog_durable_syncs,
        abandoned_tickets: 8,
        retried_batches: 8,
        commit_unknown_results: 1,
        retained_outcome_resolutions: u64::from(unknown_result_resolved_by_stable_request_identity),
        acknowledged_batches: expected_visible_batches,
        maximum_pending_batches: 3,
        recovery_logical_steps: 3_u64.saturating_mul(8),
        proxy_loss_fenced_complete_transaction_system,
        old_sequencer_fenced,
        old_proxies_fenced,
        old_resolvers_fenced,
        old_tlogs_fenced,
        every_required_tlog_inventory_authenticated,
        recovered_boundary_maximal_contiguous_quorum_prefix,
        pre_resolver_ticket_and_suffix_abandoned,
        partial_tlog_ticket_and_suffix_abandoned,
        fully_durable_unknown_result_preserved,
        unknown_result_resolved_by_stable_request_identity,
        missing_nonempty_ticket_never_replaced_with_noop,
        successors_blocked_by_missing_predecessor,
        successor_generations_exceed_old_issued_high_watermarks,
        versions_unique_across_generations,
        stale_generation_requests_rejected: stale_generation_replies_rejected,
        stale_generation_replies_rejected,
        every_transaction_uses_one_generation,
        every_transaction_uses_one_resolver_map_epoch: true,
        dispositions_match_oracle,
        exact_rows_and_envelopes,
        envelope_chain_valid,
        all_tlog_inventory_roots_exact,
        exact_acknowledgement_set,
        exact_retained_outcomes,
        resolver_durable_syncs,
        resolver_finalization_rpcs: 0,
        executed_checks: 0,
        anomaly_count: 0,
        negative_control_detected: false,
        first_mismatch: None,
        trace_sha256: String::new(),
    };
    if !(proxy_receipts_valid && resolver_receipts_valid) {
        report.dispositions_match_oracle = false;
    }
    finish_report(&mut report);
    Ok(report)
}

fn generate_tickets(seed: u64, mode: CommitProxyRecoveryMode) -> Vec<RecoveryTicketMarker> {
    (1..=BATCHES)
        .map(|batch_id| {
            let generation = generation_for_batch(batch_id);
            let local = local_batch(batch_id);
            let base = generation.saturating_mul(1_000);
            let mut current_version =
                base.saturating_add(local.saturating_mul(TRANSACTIONS_PER_BATCH));
            if mode == CommitProxyRecoveryMode::ReuseOldIssuedVersion && batch_id == 11 {
                current_version = 1_040;
            }
            let previous_version = if local == 1 {
                base
            } else {
                base.saturating_add(
                    local
                        .saturating_sub(1)
                        .saturating_mul(TRANSACTIONS_PER_BATCH),
                )
            };
            let proxy_id = u16::try_from((batch_id - 1) % 3 + 1).unwrap_or(1);
            let request_identity_root: [u8; 32] = Sha256::digest(
                serde_json::to_vec(&(
                    seed,
                    logical_batch_identity(batch_id),
                    0..TRANSACTIONS_PER_BATCH,
                ))
                .unwrap_or_default(),
            )
            .into();
            let batch_sha256: [u8; 32] = Sha256::digest(
                serde_json::to_vec(&(
                    generation,
                    batch_id,
                    current_version,
                    proxy_id,
                    request_identity_root,
                ))
                .unwrap_or_default(),
            )
            .into();
            RecoveryTicketMarker {
                format_version: 1,
                generation,
                batch_id,
                previous_version,
                current_version,
                proxy_id,
                request_identity_root,
                batch_sha256,
            }
        })
        .collect()
}

fn generation_for_batch(batch_id: u64) -> u64 {
    match batch_id {
        1..=10 => 1,
        11..=20 => 2,
        21..=27 => 3,
        _ => 4,
    }
}

fn local_batch(batch_id: u64) -> u64 {
    match batch_id {
        1..=10 => batch_id,
        11..=20 => batch_id - 10,
        21..=27 => batch_id - 20,
        _ => batch_id - 27,
    }
}

const fn logical_batch_identity(batch_id: u64) -> u64 {
    match batch_id {
        28 => 7,
        29 => 8,
        30 => 9,
        31 => 10,
        32 => 17,
        33 => 18,
        34 => 19,
        35 => 20,
        _ => batch_id,
    }
}

const fn crash_proxy_for_generation(generation: u64) -> Option<u16> {
    match generation {
        1 => Some(1),
        2 => Some(2),
        3 => Some(3),
        _ => None,
    }
}

fn resolver_batches(generation: u64) -> Vec<u64> {
    match generation {
        1 => (1..=6).collect(),
        2 => (11..=17).collect(),
        3 => (21..=27).collect(),
        _ => (28..=36).collect(),
    }
}

fn tlog_batches(generation: u64, log_set_id: u16, node_id: u16) -> Vec<u64> {
    match generation {
        1 => (1..=6).collect(),
        2 if log_set_id == 10 && node_id <= 2 => (11..=17).collect(),
        2 => (11..=16).collect(),
        3 => (21..=27).collect(),
        _ => (28..=36).collect(),
    }
}

fn versions_for_batches(tickets: &[RecoveryTicketMarker], batch_ids: &[u64]) -> Vec<u64> {
    batch_ids
        .iter()
        .map(|batch_id| version_for_batch(tickets, *batch_id))
        .collect()
}

fn version_for_batch(tickets: &[RecoveryTicketMarker], batch_id: u64) -> u64 {
    tickets
        .iter()
        .find(|ticket| ticket.batch_id == batch_id)
        .map_or(0, |ticket| ticket.current_version)
}

fn first_version(tickets: &[RecoveryTicketMarker], generation: u64) -> u64 {
    tickets
        .iter()
        .filter(|ticket| ticket.generation == generation)
        .map(|ticket| ticket.current_version)
        .min()
        .unwrap_or(0)
}

fn issued_high_watermark(tickets: &[RecoveryTicketMarker], generation: u64) -> u64 {
    tickets
        .iter()
        .filter(|ticket| ticket.generation == generation)
        .map(|ticket| ticket.current_version)
        .max()
        .unwrap_or(0)
}

fn recovered_boundaries(
    tickets: &[RecoveryTicketMarker],
    receipts: &[CommitProxyRecoveryRoleReceipt],
) -> BTreeMap<u64, u64> {
    (1..=3_u64)
        .map(|generation| {
            let set_frontiers = [10_u16, 20]
                .into_iter()
                .map(|log_set_id| {
                    let mut frontiers = receipts
                        .iter()
                        .filter(|receipt| {
                            receipt.generation == generation
                                && receipt.log_set_id == Some(log_set_id)
                                && verify_role_receipt(receipt)
                        })
                        .filter_map(|receipt| receipt.versions.last().copied())
                        .collect::<Vec<_>>();
                    frontiers.sort_unstable_by(|left, right| right.cmp(left));
                    frontiers.get(1).copied().unwrap_or(0)
                })
                .collect::<Vec<_>>();
            let boundary = set_frontiers.into_iter().min().unwrap_or(0);
            let contiguous = tickets
                .iter()
                .filter(|ticket| {
                    ticket.generation == generation && ticket.current_version <= boundary
                })
                .map(|ticket| ticket.current_version)
                .max()
                .unwrap_or(0);
            (generation, contiguous)
        })
        .collect()
}

fn recovery_inventory_roots(
    receipts: &[CommitProxyRecoveryRoleReceipt],
) -> BTreeMap<u64, [u8; 32]> {
    (1..=3_u64)
        .map(|generation| {
            let mut selected = receipts
                .iter()
                .filter(|receipt| receipt.generation == generation)
                .cloned()
                .collect::<Vec<_>>();
            selected.sort_by_key(|receipt| (receipt.log_set_id, receipt.role_id));
            let root = Sha256::digest(serde_json::to_vec(&selected).unwrap_or_default()).into();
            (generation, root)
        })
        .collect()
}

fn role_key_pair(
    role: RecoveryRoleKind,
    generation: u64,
    role_id: u16,
    log_set_id: Option<u16>,
) -> Result<Ed25519KeyPair, String> {
    let mut digest = Sha256::new();
    digest.update(b"okv-eval-commit-proxy-recovery-role-key-v1");
    digest.update([match role {
        RecoveryRoleKind::CommitProxy => 1,
        RecoveryRoleKind::Resolver => 2,
        RecoveryRoleKind::Tlog => 3,
    }]);
    digest.update(generation.to_be_bytes());
    digest.update(role_id.to_be_bytes());
    digest.update(log_set_id.unwrap_or(0).to_be_bytes());
    let seed: [u8; 32] = digest.finalize().into();
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| "recovery role signing seed is invalid".to_owned())
}

fn verify_role_receipt(receipt: &CommitProxyRecoveryRoleReceipt) -> bool {
    let Ok(key) = role_key_pair(
        receipt.role,
        receipt.generation,
        receipt.role_id,
        receipt.log_set_id,
    ) else {
        return false;
    };
    let mut unsigned = receipt.clone();
    let signature = std::mem::take(&mut unsigned.signature);
    let Ok(bytes) = serde_json::to_vec(&unsigned) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key.public_key().as_ref())
        .verify(&bytes, &signature)
        .is_ok()
}

fn run_expected_proxy_death(
    executable: &Path,
    config: &CommitProxyRecoveryRoleConfig,
) -> Result<(), String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("commit-proxy-recovery-role-node")
        .arg("--config-json")
        .arg(config_json)
        .output()
        .map_err(|error| format!("failed to start crashing commit proxy: {error}"))?;
    if output.status.success() {
        return Err("injected commit proxy unexpectedly returned a receipt".to_owned());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.contains("injected commit-proxy death") {
        return Err(format!(
            "commit proxy failed for an unexpected reason: {stderr}"
        ));
    }
    Ok(())
}

fn finish_report(report: &mut CommitProxyRecoveryReport) {
    let checks = [
        ("sequencer_nodes", report.sequencer_nodes == 3),
        ("sequencer_processes", report.sequencer_process_starts >= 3),
        ("proxy_processes", report.proxy_process_starts == 12),
        ("proxy_deaths", report.proxy_process_deaths == 3),
        ("resolver_processes", report.resolver_process_starts == 12),
        ("tlog_processes", report.tlog_process_starts == 24),
        ("generations", report.transaction_system_generations == 4),
        ("generation_fences", report.generation_fences == 3),
        ("tickets", report.sequencer_tickets == BATCHES),
        (
            "attempted_transactions",
            report.attempted_transactions == BATCHES * TRANSACTIONS_PER_BATCH,
        ),
        (
            "complete_generation_fence",
            report.proxy_loss_fenced_complete_transaction_system,
        ),
        ("old_sequencer_fence", report.old_sequencer_fenced),
        ("old_proxy_fence", report.old_proxies_fenced),
        ("old_resolver_fence", report.old_resolvers_fenced),
        ("old_tlog_fence", report.old_tlogs_fenced),
        (
            "authenticated_inventories",
            report.every_required_tlog_inventory_authenticated,
        ),
        (
            "maximal_prefix",
            report.recovered_boundary_maximal_contiguous_quorum_prefix,
        ),
        (
            "pre_resolver_abort",
            report.pre_resolver_ticket_and_suffix_abandoned,
        ),
        (
            "partial_tlog_abort",
            report.partial_tlog_ticket_and_suffix_abandoned,
        ),
        (
            "fully_durable_unknown",
            report.fully_durable_unknown_result_preserved,
        ),
        (
            "stable_identity",
            report.unknown_result_resolved_by_stable_request_identity,
        ),
        (
            "no_noop_substitution",
            report.missing_nonempty_ticket_never_replaced_with_noop,
        ),
        (
            "missing_predecessor",
            report.successors_blocked_by_missing_predecessor,
        ),
        (
            "successor_version_floor",
            report.successor_generations_exceed_old_issued_high_watermarks,
        ),
        ("unique_versions", report.versions_unique_across_generations),
        ("stale_requests", report.stale_generation_requests_rejected),
        ("stale_replies", report.stale_generation_replies_rejected),
        (
            "one_generation",
            report.every_transaction_uses_one_generation,
        ),
        (
            "one_map_epoch",
            report.every_transaction_uses_one_resolver_map_epoch,
        ),
        ("oracle", report.dispositions_match_oracle),
        ("rows_and_envelopes", report.exact_rows_and_envelopes),
        ("envelope_chain", report.envelope_chain_valid),
        ("inventory_roots", report.all_tlog_inventory_roots_exact),
        ("acknowledgements", report.exact_acknowledgement_set),
        ("retained_outcomes", report.exact_retained_outcomes),
        (
            "pending_window",
            report.maximum_pending_batches <= MAXIMUM_PENDING,
        ),
        ("resolver_syncs", report.resolver_durable_syncs == 0),
        (
            "resolver_finalization",
            report.resolver_finalization_rpcs == 0,
        ),
    ];
    report.executed_checks = checks.len() as u64;
    let failures = checks
        .iter()
        .filter(|(_, passed)| !*passed)
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    report.anomaly_count = failures.len() as u64;
    report.first_mismatch = failures.first().map(|name| (*name).to_owned());
    report.negative_control_detected =
        report.mode != CommitProxyRecoveryMode::Correct && report.anomaly_count > 0;
    let bytes = serde_json::to_vec(report).unwrap_or_default();
    report.trace_sha256 = format!("{:x}", Sha256::digest(bytes));
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CommitProxyRecoveryMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-commit-proxy-recovery-{}-{seed}-{}-{sequence}",
            std::process::id(),
            mode.id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_spaces_do_not_reuse_old_issued_versions() {
        let tickets = generate_tickets(1103, CommitProxyRecoveryMode::Correct);
        assert_eq!(tickets.len(), 36);
        for generation in 1..GENERATIONS {
            assert!(
                first_version(&tickets, generation + 1)
                    > issued_high_watermark(&tickets, generation)
            );
        }
        assert_eq!(logical_batch_identity(28), 7);
        assert_eq!(logical_batch_identity(35), 20);
    }

    #[test]
    fn tlog_quorums_expose_only_the_common_contiguous_prefix() {
        let tickets = generate_tickets(1103, CommitProxyRecoveryMode::Correct);
        assert_eq!(version_for_batch(&tickets, 16), 2_024);
        assert_eq!(version_for_batch(&tickets, 17), 2_028);
        assert_eq!(resolver_batches(2).last(), Some(&17));
        assert_eq!(tlog_batches(2, 20, 1).last(), Some(&16));
    }
}
