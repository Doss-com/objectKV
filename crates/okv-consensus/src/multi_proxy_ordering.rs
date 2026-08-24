use crate::{
    cell_resolver_partitions, CellKeyRange, CellMutation, CellProcessFixture,
    CellProcessPrototypeMode,
};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];
const GENERATION: u64 = 1;
const BATCHES: u64 = 24;
const TRANSACTIONS_PER_BATCH: u64 = 4;
const MAXIMUM_PENDING: u64 = 8;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Fault subjects for RFC-0051's multi-proxy ordering gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiCommitProxyMode {
    Correct,
    DuplicateCommitVersion,
    SkipPreviousVersion,
    ResolverArrivalOrder,
    TlogArrivalOrder,
    MutateTicketedBatch,
    AcknowledgeBeforeAllTlogSets,
    AcceptStaleProxyIncarnation,
    OmitProgressFrame,
}

impl MultiCommitProxyMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DuplicateCommitVersion => "duplicate_commit_version",
            Self::SkipPreviousVersion => "skip_previous_version",
            Self::ResolverArrivalOrder => "resolver_arrival_order",
            Self::TlogArrivalOrder => "tlog_arrival_order",
            Self::MutateTicketedBatch => "mutate_ticketed_batch",
            Self::AcknowledgeBeforeAllTlogSets => "acknowledge_before_all_tlog_sets",
            Self::AcceptStaleProxyIncarnation => "accept_stale_proxy_incarnation",
            Self::OmitProgressFrame => "omit_progress_frame",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyTransaction {
    pub transaction_id: u64,
    pub read_sequence: u64,
    pub read_conflicts: Vec<CellKeyRange>,
    pub write_conflicts: Vec<CellKeyRange>,
    pub mutations: Vec<CellMutation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyBatch {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub proxy_id: u16,
    pub proxy_incarnation: [u8; 16],
    pub batch_id: u64,
    pub transactions: Vec<MultiProxyTransaction>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignedMultiProxyBatch {
    pub batch: MultiProxyBatch,
    pub signature: Vec<u8>,
}

/// One-shot configuration for an independent commit-proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiCommitProxyProcessConfig {
    pub proxy_id: u16,
    pub proxy_incarnation: [u8; 16],
    pub batches: Vec<MultiProxyBatch>,
}

/// Output emitted by one independent commit-proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiCommitProxyProcessReceipt {
    pub proxy_id: u16,
    pub proxy_incarnation: [u8; 16],
    pub signed_batches: Vec<SignedMultiProxyBatch>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SequencerMarker {
    format_version: u16,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    previous_version: u64,
    current_version: u64,
    proxy_id: u16,
    proxy_incarnation: [u8; 16],
    batch_id: u64,
    first_transaction_sequence: u64,
    last_transaction_sequence: u64,
    batch_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxySequencerTicket {
    pub authority_sequence: u64,
    pub marker_sha256: [u8; 32],
    marker: SequencerMarker,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TicketedMultiProxyBatch {
    pub ticket: MultiProxySequencerTicket,
    pub signed_batch: SignedMultiProxyBatch,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiProxyDecision {
    Accept,
    Conflict,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyResolverDecision {
    pub resolver_id: u16,
    pub batch_version: u64,
    pub candidate_sequence: u64,
    pub transaction_id: u64,
    pub decision: MultiProxyDecision,
}

/// One-shot configuration for one memory-only resolver worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyResolverProcessConfig {
    pub resolver_id: u16,
    pub arrival_batches: Vec<TicketedMultiProxyBatch>,
    pub process_arrival_order: bool,
}

/// Receipt emitted by one memory-only resolver worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyResolverProcessReceipt {
    pub resolver_id: u16,
    pub ticket_chain_valid: bool,
    pub batch_bytes_valid: bool,
    pub proxy_identities_pinned: bool,
    pub processed_batch_versions: Vec<u64>,
    pub decisions: Vec<MultiProxyResolverDecision>,
    pub maximum_pending_batches: u64,
    pub final_frontier: u64,
    pub durable_syncs: u64,
    pub finalization_rpcs: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyProgressFrame {
    pub format_version: u16,
    pub generation: u64,
    pub previous_version: u64,
    pub current_version: u64,
    pub batch_id: u64,
    pub batch_sha256: [u8; 32],
    pub outcome_sha256: [u8; 32],
    pub committed_transactions: u64,
    pub conflicted_transactions: u64,
}

/// One-shot configuration for an ordered durable tLog worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyTlogProcessConfig {
    pub log_set_id: u16,
    pub node_id: u16,
    pub root: PathBuf,
    pub arrival_frames: Vec<MultiProxyProgressFrame>,
    pub process_arrival_order: bool,
}

/// Signed durable receipt emitted by one tLog worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiProxyTlogProcessReceipt {
    pub log_set_id: u16,
    pub node_id: u16,
    pub chain_valid: bool,
    pub processed_versions: Vec<u64>,
    pub final_frontier: u64,
    pub maximum_pending_batches: u64,
    pub durable_root_sha256: [u8; 32],
    pub durable_syncs: u64,
    pub signature: Vec<u8>,
}

/// Canonical report for one multi-proxy ordering history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultiCommitProxyReport {
    pub seed: u64,
    pub mode: MultiCommitProxyMode,
    pub sequencer_nodes: u64,
    pub sequencer_process_starts: u64,
    pub proxy_process_starts: u64,
    pub resolver_process_starts: u64,
    pub tlog_process_starts: u64,
    pub sequencer_tickets: u64,
    pub attempted_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub resolver_decisions: u64,
    pub out_of_order_deliveries: u64,
    pub maximum_pending_batches: u64,
    pub progress_frames: u64,
    pub conflict_only_progress_frames: u64,
    pub tlog_durable_syncs: u64,
    pub acknowledged_batches: u64,
    pub unique_gap_free_ticket_chain: bool,
    pub authority_marker_binding_exact: bool,
    pub proxy_signatures_valid: bool,
    pub proxy_identities_pinned: bool,
    pub pending_window_bounded: bool,
    pub all_resolvers_same_order: bool,
    pub transactions_ordered_inside_batches: bool,
    pub crossing_ranges_reached_every_overlap: bool,
    pub dispositions_match_oracle: bool,
    pub every_batch_has_progress_frame: bool,
    pub conflict_only_batches_advance: bool,
    pub all_tlogs_same_order: bool,
    pub tlog_frames_match_ticketed_batches: bool,
    pub acknowledgements_require_every_tlog_set: bool,
    pub later_batches_blocked_by_missing_predecessor: bool,
    pub stale_proxy_rejected: bool,
    pub exact_rows_and_envelopes: bool,
    pub envelope_chain_valid: bool,
    pub resolver_durable_syncs: u64,
    pub resolver_finalization_rpcs: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub negative_control_detected: bool,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Execute one independent commit-proxy process payload.
///
/// # Errors
///
/// Returns an error when proxy identity or batch ownership is invalid.
pub fn run_multi_commit_proxy_process(
    config: MultiCommitProxyProcessConfig,
) -> Result<MultiCommitProxyProcessReceipt, String> {
    if config.proxy_id == 0 || config.batches.is_empty() {
        return Err("commit proxy process requires an identity and batches".to_owned());
    }
    let mut signed_batches = Vec::with_capacity(config.batches.len());
    for batch in config.batches {
        if batch.proxy_id != config.proxy_id
            || batch.proxy_incarnation != config.proxy_incarnation
            || batch.generation != GENERATION
            || batch.transactions.len() != usize::try_from(TRANSACTIONS_PER_BATCH).unwrap_or(4)
        {
            return Err("commit proxy batch ownership or shape is invalid".to_owned());
        }
        let bytes = serde_json::to_vec(&batch).map_err(|error| error.to_string())?;
        let key = proxy_key_pair(config.proxy_id, config.proxy_incarnation)?;
        signed_batches.push(SignedMultiProxyBatch {
            batch,
            signature: key.sign(&bytes).as_ref().to_vec(),
        });
    }
    Ok(MultiCommitProxyProcessReceipt {
        proxy_id: config.proxy_id,
        proxy_incarnation: config.proxy_incarnation,
        signed_batches,
    })
}

/// Execute one memory-only ordered resolver worker process.
///
/// # Errors
///
/// Returns an error when the resolver identity is absent from the frozen map.
pub fn run_multi_proxy_resolver_process(
    config: &MultiProxyResolverProcessConfig,
) -> Result<MultiProxyResolverProcessReceipt, String> {
    let partition = cell_resolver_partitions()
        .into_iter()
        .find(|partition| partition.resolver_id == config.resolver_id)
        .ok_or_else(|| "resolver is absent from the frozen map".to_owned())?;
    let validations = validate_ticketed_batches(&config.arrival_batches);
    let (ordered, maximum_pending, final_frontier) =
        order_ticketed_batches(&config.arrival_batches, config.process_arrival_order);
    let mut writes = baseline_writes(config.resolver_id);
    let mut decisions = Vec::new();
    for ticketed in &ordered {
        for (offset, transaction) in ticketed.signed_batch.batch.transactions.iter().enumerate() {
            let candidate_sequence = ticketed
                .ticket
                .marker
                .first_transaction_sequence
                .saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
            let read_conflicts = clipped(&transaction.read_conflicts, &partition);
            let write_conflicts = clipped(&transaction.write_conflicts, &partition);
            if read_conflicts.is_empty() && write_conflicts.is_empty() {
                continue;
            }
            let decision = if read_conflicts.iter().any(|read| {
                writes.iter().any(|(sequence, write)| {
                    *sequence > transaction.read_sequence && read.overlaps(write)
                })
            }) {
                MultiProxyDecision::Conflict
            } else {
                MultiProxyDecision::Accept
            };
            if decision == MultiProxyDecision::Accept {
                writes.extend(
                    write_conflicts
                        .iter()
                        .cloned()
                        .map(|range| (candidate_sequence, range)),
                );
            }
            decisions.push(MultiProxyResolverDecision {
                resolver_id: config.resolver_id,
                batch_version: ticketed.ticket.marker.current_version,
                candidate_sequence,
                transaction_id: transaction.transaction_id,
                decision,
            });
        }
    }
    Ok(MultiProxyResolverProcessReceipt {
        resolver_id: config.resolver_id,
        ticket_chain_valid: validations.ticket_chain_valid,
        batch_bytes_valid: validations.batch_bytes_valid && validations.proxy_signatures_valid,
        proxy_identities_pinned: validations.proxy_identities_pinned,
        processed_batch_versions: ordered
            .iter()
            .map(|batch| batch.ticket.marker.current_version)
            .collect(),
        decisions,
        maximum_pending_batches: maximum_pending,
        final_frontier,
        durable_syncs: 0,
        finalization_rpcs: 0,
    })
}

/// Execute one durable ordered tLog worker process.
///
/// # Errors
///
/// Returns an error when the worker root cannot be persisted and synchronized.
pub fn run_multi_proxy_tlog_process(
    config: &MultiProxyTlogProcessConfig,
) -> Result<MultiProxyTlogProcessReceipt, String> {
    if config.log_set_id == 0 || config.node_id == 0 {
        return Err("tLog process requires log-set and node identities".to_owned());
    }
    let (ordered, maximum_pending, final_frontier, chain_valid) =
        order_progress_frames(&config.arrival_frames, config.process_arrival_order);
    fs::create_dir_all(&config.root).map_err(|error| error.to_string())?;
    let path = config.root.join("ordered-progress-frames.json");
    let temporary = config.root.join("ordered-progress-frames.tmp");
    let bytes = serde_json::to_vec(&ordered).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    File::open(&config.root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    let mut receipt = MultiProxyTlogProcessReceipt {
        log_set_id: config.log_set_id,
        node_id: config.node_id,
        chain_valid,
        processed_versions: ordered.iter().map(|frame| frame.current_version).collect(),
        final_frontier,
        maximum_pending_batches: maximum_pending,
        durable_root_sha256: Sha256::digest(&bytes).into(),
        durable_syncs: 1,
        signature: Vec::new(),
    };
    let signing_bytes = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
    receipt.signature = tlog_key_pair(config.log_set_id, config.node_id)?
        .sign(&signing_bytes)
        .as_ref()
        .to_vec();
    Ok(receipt)
}

/// Run the frozen RFC-0051 real-process ordering contract.
///
/// # Errors
///
/// Returns an error when the replicated authority or any bounded child process
/// cannot execute its protocol.
pub fn run_multi_commit_proxy_ordering_contract(
    seed: u64,
    mode: MultiCommitProxyMode,
    executable: &Path,
) -> Result<MultiCommitProxyReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: MultiCommitProxyMode,
    executable: &Path,
) -> Result<MultiCommitProxyReport, String> {
    if !executable.is_file() {
        return Err("multi-proxy contract executable is absent".to_owned());
    }
    let root = TempRoot::new(seed, mode)?;
    let raw_batches = generate_batches(seed, mode);
    let mut signed_by_id = BTreeMap::new();
    for proxy_id in 1..=3_u16 {
        let batches = raw_batches
            .iter()
            .filter(|batch| batch.proxy_id == proxy_id)
            .cloned()
            .collect::<Vec<_>>();
        let config = MultiCommitProxyProcessConfig {
            proxy_id,
            proxy_incarnation: batches.first().map_or_else(
                || proxy_incarnation(proxy_id),
                |batch| batch.proxy_incarnation,
            ),
            batches,
        };
        let receipt: MultiCommitProxyProcessReceipt =
            run_child_json(executable, "multi-commit-proxy-node", &config)?;
        for signed in receipt.signed_batches {
            signed_by_id.insert(signed.batch.batch_id, signed);
        }
    }
    if signed_by_id.len() != usize::try_from(BATCHES).unwrap_or(24) {
        return Err("commit proxy processes omitted a batch".to_owned());
    }

    let mut authority =
        CellProcessFixture::start(seed, CellProcessPrototypeMode::Correct, executable)?;
    let authority_report = authority.run_history().await?;
    let sequence_order = proxy_interleaving(seed);
    let mut ticketed = Vec::with_capacity(sequence_order.len());
    let mut previous_version = 0_u64;
    for (ordinal, batch_id) in sequence_order.iter().enumerate() {
        let signed_batch = signed_by_id
            .get(batch_id)
            .cloned()
            .ok_or_else(|| "sequencer order referenced an absent batch".to_owned())?;
        let current_version = previous_version.saturating_add(TRANSACTIONS_PER_BATCH);
        let batch_sha256 = batch_sha256(&signed_batch.batch)?;
        let marker = SequencerMarker {
            format_version: 1,
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: GENERATION,
            previous_version,
            current_version,
            proxy_id: signed_batch.batch.proxy_id,
            proxy_incarnation: signed_batch.batch.proxy_incarnation,
            batch_id: *batch_id,
            first_transaction_sequence: previous_version.saturating_add(1),
            last_transaction_sequence: current_version,
            batch_sha256,
        };
        let marker_bytes = serde_json::to_vec(&marker).map_err(|error| error.to_string())?;
        let authority_sequence = authority
            .replicate_sequencer_marker(
                u64::try_from(ordinal).unwrap_or(u64::MAX).saturating_add(1),
                &marker_bytes,
            )
            .await?;
        ticketed.push(TicketedMultiProxyBatch {
            ticket: MultiProxySequencerTicket {
                authority_sequence,
                marker_sha256: Sha256::digest(&marker_bytes).into(),
                marker,
            },
            signed_batch,
        });
        previous_version = current_version;
    }
    inject_ticket_fault(&mut ticketed, mode);

    let mut resolver_receipts = Vec::new();
    for resolver_id in 1..=3_u16 {
        let arrival_batches = permute_ticketed(&ticketed, usize::from(resolver_id) + 1);
        let config = MultiProxyResolverProcessConfig {
            resolver_id,
            arrival_batches,
            process_arrival_order: mode == MultiCommitProxyMode::ResolverArrivalOrder
                && resolver_id == 2,
        };
        let receipt: MultiProxyResolverProcessReceipt =
            run_child_json(executable, "multi-proxy-resolver-node", &config)?;
        resolver_receipts.push(receipt);
    }

    let validations = validate_ticketed_batches(&ticketed);
    let oracle = resolve_oracle(&ticketed);
    let actual = combine_resolver_decisions(&ticketed, &resolver_receipts);
    let dispositions_match_oracle = oracle.statuses == actual.statuses;
    let (expected_rows, expected_envelopes) = materialize_visible(&ticketed, &oracle.statuses)?;
    let (actual_rows, actual_envelopes) = materialize_visible(&ticketed, &actual.statuses)?;
    let exact_rows_and_envelopes =
        expected_rows == actual_rows && expected_envelopes == actual_envelopes;
    let envelope_chain_valid = valid_visible_chain(&actual_envelopes);
    let frames = build_progress_frames(&ticketed, &actual.statuses, &actual_envelopes)?;
    let conflict_only_versions = frames
        .iter()
        .filter(|frame| frame.committed_transactions == 0)
        .map(|frame| frame.current_version)
        .collect::<BTreeSet<_>>();

    let mut tlog_receipts = Vec::new();
    for log_set_id in [10_u16, 20] {
        for node_id in 1..=3_u16 {
            let mut selected_frames = frames.clone();
            if mode == MultiCommitProxyMode::AcknowledgeBeforeAllTlogSets
                && log_set_id == 20
                && node_id <= 2
            {
                selected_frames.pop();
            }
            if mode == MultiCommitProxyMode::OmitProgressFrame && log_set_id == 20 {
                if let Some(version) = conflict_only_versions.iter().next().copied() {
                    selected_frames.retain(|frame| frame.current_version != version);
                }
            }
            let arrival_frames =
                permute_frames(&selected_frames, usize::from(log_set_id / 10 + node_id));
            let config = MultiProxyTlogProcessConfig {
                log_set_id,
                node_id,
                root: root.path().join(format!("tlog-{log_set_id}-{node_id}")),
                arrival_frames,
                process_arrival_order: mode == MultiCommitProxyMode::TlogArrivalOrder
                    && log_set_id == 10
                    && node_id == 1,
            };
            let receipt: MultiProxyTlogProcessReceipt =
                run_child_json(executable, "multi-proxy-tlog-node", &config)?;
            tlog_receipts.push(receipt);
        }
    }
    let all_tlog_signatures_valid = tlog_receipts.iter().all(verify_tlog_receipt);
    let expected_versions = frames
        .iter()
        .map(|frame| frame.current_version)
        .collect::<Vec<_>>();
    let all_tlogs_same_order = all_tlog_signatures_valid
        && tlog_receipts
            .iter()
            .all(|receipt| receipt.processed_versions == expected_versions);
    let tlog_frames_match_ticketed_batches = all_tlog_signatures_valid
        && tlog_receipts.iter().all(|receipt| {
            receipt.chain_valid
                && receipt.durable_root_sha256
                    == progress_root_for_versions(&frames, &receipt.processed_versions)
        });
    let quorum_frontiers = [10_u16, 20]
        .into_iter()
        .map(|log_set_id| {
            let mut frontiers = tlog_receipts
                .iter()
                .filter(|receipt| receipt.log_set_id == log_set_id && verify_tlog_receipt(receipt))
                .map(|receipt| receipt.final_frontier)
                .collect::<Vec<_>>();
            frontiers.sort_unstable_by(|left, right| right.cmp(left));
            (log_set_id, frontiers.get(1).copied().unwrap_or(0))
        })
        .collect::<BTreeMap<_, _>>();
    let correct_acknowledgements = ticketed
        .iter()
        .filter(|batch| {
            quorum_frontiers
                .values()
                .all(|frontier| *frontier >= batch.ticket.marker.current_version)
        })
        .map(|batch| batch.ticket.marker.current_version)
        .collect::<BTreeSet<_>>();
    let acknowledged = if mode == MultiCommitProxyMode::AcknowledgeBeforeAllTlogSets {
        ticketed
            .iter()
            .filter(|batch| {
                quorum_frontiers.get(&10).copied().unwrap_or(0)
                    >= batch.ticket.marker.current_version
            })
            .map(|batch| batch.ticket.marker.current_version)
            .collect::<BTreeSet<_>>()
    } else {
        correct_acknowledgements.clone()
    };
    let expected_acknowledgements = ticketed
        .iter()
        .map(|batch| batch.ticket.marker.current_version)
        .collect::<BTreeSet<_>>();

    let resolver_orders = resolver_receipts
        .iter()
        .map(|receipt| receipt.processed_batch_versions.clone())
        .collect::<Vec<_>>();
    let all_resolvers_same_order = resolver_orders
        .first()
        .is_some_and(|first| first == &expected_versions)
        && resolver_orders.windows(2).all(|pair| pair[0] == pair[1]);
    let maximum_pending_batches = resolver_receipts
        .iter()
        .map(|receipt| receipt.maximum_pending_batches)
        .chain(
            tlog_receipts
                .iter()
                .map(|receipt| receipt.maximum_pending_batches),
        )
        .max()
        .unwrap_or(0);
    let pending_window_bounded = maximum_pending_batches <= MAXIMUM_PENDING;
    let every_batch_has_progress_frame = frames.len() == ticketed.len();
    let conflict_only_batches_advance = conflict_only_versions.len() >= 4
        && conflict_only_versions
            .iter()
            .all(|version| frames.iter().any(|frame| frame.current_version == *version));
    let acknowledgements_require_every_tlog_set = acknowledged == correct_acknowledgements
        && correct_acknowledgements == expected_acknowledgements;
    let later_batches_blocked_by_missing_predecessor = if matches!(
        mode,
        MultiCommitProxyMode::OmitProgressFrame
            | MultiCommitProxyMode::AcknowledgeBeforeAllTlogSets
    ) {
        quorum_frontiers
            .values()
            .any(|frontier| *frontier < previous_version)
    } else {
        quorum_frontiers
            .values()
            .all(|frontier| *frontier == previous_version)
    };
    let stale_proxy_rejected = if mode == MultiCommitProxyMode::AcceptStaleProxyIncarnation {
        !validations.proxy_identities_pinned
    } else {
        validations.proxy_identities_pinned
    };
    let transactions_ordered_inside_batches = ticketed.iter().all(|batch| {
        batch
            .signed_batch
            .batch
            .transactions
            .windows(2)
            .all(|pair| pair[0].transaction_id < pair[1].transaction_id)
    });
    let crossing_ranges_reached_every_overlap = actual.crossing_required_decisions
        == actual.crossing_observed_decisions
        && actual.crossing_required_decisions > 0;
    let resolver_durable_syncs = resolver_receipts
        .iter()
        .map(|receipt| receipt.durable_syncs)
        .sum();
    let resolver_finalization_rpcs = resolver_receipts
        .iter()
        .map(|receipt| receipt.finalization_rpcs)
        .sum();

    let mut report = MultiCommitProxyReport {
        seed,
        mode,
        sequencer_nodes: 3,
        sequencer_process_starts: authority_report.process_starts,
        proxy_process_starts: 3,
        resolver_process_starts: 3,
        tlog_process_starts: 6,
        sequencer_tickets: ticketed.len() as u64,
        attempted_transactions: ticketed
            .iter()
            .map(|batch| batch.signed_batch.batch.transactions.len() as u64)
            .sum(),
        committed_transactions: actual
            .statuses
            .values()
            .filter(|status| **status == MultiProxyDecision::Accept)
            .count() as u64,
        conflict_rejections: actual
            .statuses
            .values()
            .filter(|status| **status == MultiProxyDecision::Conflict)
            .count() as u64,
        resolver_decisions: resolver_receipts
            .iter()
            .map(|receipt| receipt.decisions.len() as u64)
            .sum(),
        out_of_order_deliveries: count_out_of_order(&ticketed, &resolver_receipts),
        maximum_pending_batches,
        progress_frames: frames.len() as u64,
        conflict_only_progress_frames: conflict_only_versions.len() as u64,
        tlog_durable_syncs: tlog_receipts
            .iter()
            .map(|receipt| receipt.durable_syncs)
            .sum(),
        acknowledged_batches: acknowledged.len() as u64,
        unique_gap_free_ticket_chain: validations.ticket_chain_valid,
        authority_marker_binding_exact: validations.authority_marker_binding_exact,
        proxy_signatures_valid: validations.proxy_signatures_valid,
        proxy_identities_pinned: validations.proxy_identities_pinned,
        pending_window_bounded,
        all_resolvers_same_order,
        transactions_ordered_inside_batches,
        crossing_ranges_reached_every_overlap,
        dispositions_match_oracle,
        every_batch_has_progress_frame,
        conflict_only_batches_advance,
        all_tlogs_same_order,
        tlog_frames_match_ticketed_batches,
        acknowledgements_require_every_tlog_set,
        later_batches_blocked_by_missing_predecessor,
        stale_proxy_rejected,
        exact_rows_and_envelopes,
        envelope_chain_valid,
        resolver_durable_syncs,
        resolver_finalization_rpcs,
        executed_checks: 0,
        anomaly_count: 0,
        negative_control_detected: false,
        first_mismatch: None,
        trace_sha256: String::new(),
    };
    finish_report(&mut report);
    Ok(report)
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
struct TicketValidations {
    ticket_chain_valid: bool,
    authority_marker_binding_exact: bool,
    batch_bytes_valid: bool,
    proxy_signatures_valid: bool,
    proxy_identities_pinned: bool,
}

fn validate_ticketed_batches(batches: &[TicketedMultiProxyBatch]) -> TicketValidations {
    let mut sorted = batches.to_vec();
    sorted.sort_by_key(|batch| batch.ticket.marker.current_version);
    let mut prior = 0_u64;
    let mut versions = BTreeSet::new();
    let mut authorities = BTreeSet::new();
    let mut ticket_chain_valid = sorted.len() == usize::try_from(BATCHES).unwrap_or(24);
    let mut authority_marker_binding_exact = true;
    let mut batch_bytes_valid = true;
    let mut proxy_signatures_valid = true;
    let mut proxy_identities_pinned = true;
    for batch in &sorted {
        let marker = &batch.ticket.marker;
        ticket_chain_valid &= versions.insert(marker.current_version)
            && authorities.insert(batch.ticket.authority_sequence)
            && marker.previous_version == prior
            && marker.current_version == prior.saturating_add(TRANSACTIONS_PER_BATCH)
            && marker.first_transaction_sequence == prior.saturating_add(1)
            && marker.last_transaction_sequence == marker.current_version;
        let marker_bytes = serde_json::to_vec(marker).unwrap_or_default();
        let observed_marker_sha256: [u8; 32] = Sha256::digest(&marker_bytes).into();
        authority_marker_binding_exact &= batch.ticket.marker_sha256 == observed_marker_sha256;
        batch_bytes_valid &=
            marker.batch_sha256 == batch_sha256(&batch.signed_batch.batch).unwrap_or([0_u8; 32]);
        proxy_signatures_valid &= verify_proxy_batch(&batch.signed_batch);
        proxy_identities_pinned &= marker.proxy_id == batch.signed_batch.batch.proxy_id
            && marker.proxy_incarnation == batch.signed_batch.batch.proxy_incarnation
            && marker.proxy_incarnation == proxy_incarnation(marker.proxy_id)
            && marker.generation == GENERATION;
        prior = marker.current_version;
    }
    TicketValidations {
        ticket_chain_valid,
        authority_marker_binding_exact,
        batch_bytes_valid,
        proxy_signatures_valid,
        proxy_identities_pinned,
    }
}

fn order_ticketed_batches(
    arrival: &[TicketedMultiProxyBatch],
    process_arrival_order: bool,
) -> (Vec<TicketedMultiProxyBatch>, u64, u64) {
    if process_arrival_order {
        let frontier = arrival
            .last()
            .map_or(0, |batch| batch.ticket.marker.current_version);
        return (arrival.to_vec(), 0, frontier);
    }
    let mut frontier = 0_u64;
    let mut pending = BTreeMap::new();
    let mut ordered = Vec::new();
    let mut maximum_pending = 0_u64;
    for batch in arrival {
        pending.insert(batch.ticket.marker.current_version, batch.clone());
        loop {
            let next = pending
                .values()
                .find(|candidate| candidate.ticket.marker.previous_version == frontier)
                .cloned();
            let Some(next) = next else { break };
            pending.remove(&next.ticket.marker.current_version);
            frontier = next.ticket.marker.current_version;
            ordered.push(next);
        }
        maximum_pending = maximum_pending.max(pending.len() as u64);
    }
    (ordered, maximum_pending, frontier)
}

fn order_progress_frames(
    arrival: &[MultiProxyProgressFrame],
    process_arrival_order: bool,
) -> (Vec<MultiProxyProgressFrame>, u64, u64, bool) {
    if process_arrival_order {
        let chain_valid = arrival
            .windows(2)
            .all(|pair| pair[1].previous_version == pair[0].current_version)
            && arrival
                .first()
                .is_some_and(|frame| frame.previous_version == 0);
        let frontier = arrival.last().map_or(0, |frame| frame.current_version);
        return (arrival.to_vec(), 0, frontier, chain_valid);
    }
    let mut frontier = 0_u64;
    let mut pending = BTreeMap::new();
    let mut ordered = Vec::new();
    let mut maximum_pending = 0_u64;
    for frame in arrival {
        pending.insert(frame.current_version, frame.clone());
        loop {
            let next = pending
                .values()
                .find(|candidate| candidate.previous_version == frontier)
                .cloned();
            let Some(next) = next else { break };
            pending.remove(&next.current_version);
            frontier = next.current_version;
            ordered.push(next);
        }
        maximum_pending = maximum_pending.max(pending.len() as u64);
    }
    let chain_valid = ordered
        .windows(2)
        .all(|pair| pair[1].previous_version == pair[0].current_version)
        && ordered
            .first()
            .is_none_or(|frame| frame.previous_version == 0);
    (ordered, maximum_pending, frontier, chain_valid)
}

#[derive(Default)]
struct ResolvedHistory {
    statuses: BTreeMap<u64, MultiProxyDecision>,
    crossing_required_decisions: u64,
    crossing_observed_decisions: u64,
}

fn resolve_oracle(ticketed: &[TicketedMultiProxyBatch]) -> ResolvedHistory {
    let mut writes = baseline_writes(0);
    let mut history = ResolvedHistory::default();
    let mut sorted = ticketed.to_vec();
    sorted.sort_by_key(|batch| batch.ticket.marker.current_version);
    for batch in &sorted {
        for (offset, transaction) in batch.signed_batch.batch.transactions.iter().enumerate() {
            let candidate = batch
                .ticket
                .marker
                .first_transaction_sequence
                .saturating_add(offset as u64);
            let decision = if transaction.read_conflicts.iter().any(|read| {
                writes.iter().any(|(sequence, write)| {
                    *sequence > transaction.read_sequence && read.overlaps(write)
                })
            }) {
                MultiProxyDecision::Conflict
            } else {
                MultiProxyDecision::Accept
            };
            if decision == MultiProxyDecision::Accept {
                writes.extend(
                    transaction
                        .write_conflicts
                        .iter()
                        .cloned()
                        .map(|range| (candidate, range)),
                );
            }
            history
                .statuses
                .insert(transaction.transaction_id, decision);
        }
    }
    history
}

fn combine_resolver_decisions(
    ticketed: &[TicketedMultiProxyBatch],
    receipts: &[MultiProxyResolverProcessReceipt],
) -> ResolvedHistory {
    let by_transaction = receipts
        .iter()
        .flat_map(|receipt| receipt.decisions.iter())
        .fold(
            BTreeMap::<u64, Vec<&MultiProxyResolverDecision>>::new(),
            |mut map, decision| {
                map.entry(decision.transaction_id)
                    .or_default()
                    .push(decision);
                map
            },
        );
    let mut history = ResolvedHistory::default();
    for batch in ticketed {
        for transaction in &batch.signed_batch.batch.transactions {
            let required = cell_resolver_partitions()
                .into_iter()
                .filter(|partition| {
                    transaction
                        .read_conflicts
                        .iter()
                        .chain(&transaction.write_conflicts)
                        .any(|range| {
                            range.overlaps(&CellKeyRange {
                                start: partition.start.clone(),
                                end: partition.end.clone(),
                            })
                        })
                })
                .map(|partition| partition.resolver_id)
                .collect::<BTreeSet<_>>();
            let observed = by_transaction
                .get(&transaction.transaction_id)
                .cloned()
                .unwrap_or_default();
            if required.len() > 1 {
                history.crossing_required_decisions = history
                    .crossing_required_decisions
                    .saturating_add(required.len() as u64);
                history.crossing_observed_decisions =
                    history.crossing_observed_decisions.saturating_add(
                        observed
                            .iter()
                            .filter(|decision| required.contains(&decision.resolver_id))
                            .count() as u64,
                    );
            }
            let complete = required.len() == observed.len()
                && observed
                    .iter()
                    .all(|decision| required.contains(&decision.resolver_id));
            let decision = if complete
                && observed
                    .iter()
                    .all(|decision| decision.decision == MultiProxyDecision::Accept)
            {
                MultiProxyDecision::Accept
            } else {
                MultiProxyDecision::Conflict
            };
            history
                .statuses
                .insert(transaction.transaction_id, decision);
        }
    }
    history
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct VisibleEnvelope {
    candidate_sequence: u64,
    batch_version: u64,
    transaction_id: u64,
    mutations: Vec<CellMutation>,
    previous_chain: [u8; 32],
}

type MaterializedVisible = (BTreeMap<Vec<u8>, Vec<u8>>, Vec<Vec<u8>>);

fn materialize_visible(
    ticketed: &[TicketedMultiProxyBatch],
    statuses: &BTreeMap<u64, MultiProxyDecision>,
) -> Result<MaterializedVisible, String> {
    let mut rows = BTreeMap::new();
    let mut envelopes = Vec::new();
    let mut previous = [0_u8; 32];
    let mut sorted = ticketed.to_vec();
    sorted.sort_by_key(|batch| batch.ticket.marker.current_version);
    for batch in &sorted {
        for (offset, transaction) in batch.signed_batch.batch.transactions.iter().enumerate() {
            if statuses.get(&transaction.transaction_id) != Some(&MultiProxyDecision::Accept) {
                continue;
            }
            for mutation in &transaction.mutations {
                match mutation {
                    CellMutation::Clear { key } => {
                        rows.remove(key);
                    }
                    CellMutation::Set { key, value } => {
                        rows.insert(key.clone(), value.clone());
                    }
                }
            }
            let envelope = VisibleEnvelope {
                candidate_sequence: batch
                    .ticket
                    .marker
                    .first_transaction_sequence
                    .saturating_add(offset as u64),
                batch_version: batch.ticket.marker.current_version,
                transaction_id: transaction.transaction_id,
                mutations: transaction.mutations.clone(),
                previous_chain: previous,
            };
            let bytes = serde_json::to_vec(&envelope).map_err(|error| error.to_string())?;
            previous = Sha256::digest(&bytes).into();
            envelopes.push(bytes);
        }
    }
    Ok((rows, envelopes))
}

fn valid_visible_chain(envelopes: &[Vec<u8>]) -> bool {
    let mut previous = [0_u8; 32];
    for bytes in envelopes {
        let Ok(envelope) = serde_json::from_slice::<VisibleEnvelope>(bytes) else {
            return false;
        };
        if envelope.previous_chain != previous {
            return false;
        }
        previous = Sha256::digest(bytes).into();
    }
    true
}

fn build_progress_frames(
    ticketed: &[TicketedMultiProxyBatch],
    statuses: &BTreeMap<u64, MultiProxyDecision>,
    envelopes: &[Vec<u8>],
) -> Result<Vec<MultiProxyProgressFrame>, String> {
    let envelope_by_transaction = envelopes
        .iter()
        .filter_map(|bytes| {
            serde_json::from_slice::<VisibleEnvelope>(bytes)
                .ok()
                .map(|envelope| (envelope.transaction_id, Sha256::digest(bytes).to_vec()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut sorted = ticketed.to_vec();
    sorted.sort_by_key(|batch| batch.ticket.marker.current_version);
    sorted
        .iter()
        .map(|batch| {
            let outcomes = batch
                .signed_batch
                .batch
                .transactions
                .iter()
                .map(|transaction| {
                    (
                        transaction.transaction_id,
                        statuses
                            .get(&transaction.transaction_id)
                            .copied()
                            .unwrap_or(MultiProxyDecision::Conflict),
                        envelope_by_transaction
                            .get(&transaction.transaction_id)
                            .cloned(),
                    )
                })
                .collect::<Vec<_>>();
            let committed = outcomes
                .iter()
                .filter(|(_, status, _)| *status == MultiProxyDecision::Accept)
                .count() as u64;
            let bytes = serde_json::to_vec(&outcomes).map_err(|error| error.to_string())?;
            Ok(MultiProxyProgressFrame {
                format_version: 1,
                generation: GENERATION,
                previous_version: batch.ticket.marker.previous_version,
                current_version: batch.ticket.marker.current_version,
                batch_id: batch.ticket.marker.batch_id,
                batch_sha256: batch.ticket.marker.batch_sha256,
                outcome_sha256: Sha256::digest(bytes).into(),
                committed_transactions: committed,
                conflicted_transactions: TRANSACTIONS_PER_BATCH.saturating_sub(committed),
            })
        })
        .collect()
}

fn generate_batches(seed: u64, mode: MultiCommitProxyMode) -> Vec<MultiProxyBatch> {
    (0..BATCHES)
        .map(|batch_id| {
            let proxy_id = u16::try_from(batch_id % 3 + 1).unwrap_or(1);
            let incarnation =
                if mode == MultiCommitProxyMode::AcceptStaleProxyIncarnation && batch_id % 3 == 0 {
                    stale_proxy_incarnation(proxy_id)
                } else {
                    proxy_incarnation(proxy_id)
                };
            let conflict_only = batch_id % 6 == 5;
            let transactions = (0..TRANSACTIONS_PER_BATCH)
                .map(|offset| {
                    let transaction_id = batch_id
                        .saturating_mul(TRANSACTIONS_PER_BATCH)
                        .saturating_add(offset)
                        .saturating_add(1);
                    if conflict_only {
                        let partition_id = u8::try_from(offset % 3).unwrap_or(0);
                        let key = hot_key(partition_id);
                        return MultiProxyTransaction {
                            transaction_id,
                            read_sequence: 0,
                            read_conflicts: vec![CellKeyRange::point(&key)],
                            write_conflicts: vec![CellKeyRange::point(&key)],
                            mutations: vec![CellMutation::Set {
                                key,
                                value: transaction_id.to_be_bytes().to_vec(),
                            }],
                        };
                    }
                    match offset {
                        0 | 2 => {
                            let partition_id = u8::try_from((batch_id + offset) % 3).unwrap_or(0);
                            let key = unique_key(partition_id, transaction_id);
                            MultiProxyTransaction {
                                transaction_id,
                                read_sequence: 0,
                                read_conflicts: Vec::new(),
                                write_conflicts: vec![CellKeyRange::point(&key)],
                                mutations: vec![CellMutation::Set {
                                    key,
                                    value: seed.wrapping_add(transaction_id).to_be_bytes().to_vec(),
                                }],
                            }
                        }
                        1 => {
                            let partition_id = u8::try_from(batch_id % 3).unwrap_or(0);
                            let key = hot_key(partition_id);
                            MultiProxyTransaction {
                                transaction_id,
                                read_sequence: 0,
                                read_conflicts: vec![CellKeyRange::point(&key)],
                                write_conflicts: vec![CellKeyRange::point(&key)],
                                mutations: vec![CellMutation::Set {
                                    key,
                                    value: transaction_id.to_be_bytes().to_vec(),
                                }],
                            }
                        }
                        _ => {
                            let left = unique_key(0, transaction_id);
                            let right = unique_key(2, transaction_id);
                            MultiProxyTransaction {
                                transaction_id,
                                read_sequence: 0,
                                read_conflicts: Vec::new(),
                                write_conflicts: vec![
                                    CellKeyRange::point(&left),
                                    CellKeyRange::point(&right),
                                ],
                                mutations: vec![
                                    CellMutation::Set {
                                        key: left,
                                        value: transaction_id.to_be_bytes().to_vec(),
                                    },
                                    CellMutation::Set {
                                        key: right,
                                        value: transaction_id.to_be_bytes().to_vec(),
                                    },
                                ],
                            }
                        }
                    }
                })
                .collect();
            MultiProxyBatch {
                format_version: 1,
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                generation: GENERATION,
                proxy_id,
                proxy_incarnation: incarnation,
                batch_id,
                transactions,
            }
        })
        .collect()
}

fn proxy_interleaving(seed: u64) -> Vec<u64> {
    let mut order = Vec::with_capacity(usize::try_from(BATCHES).unwrap_or(24));
    for round in 0..8_u64 {
        let rotation = (seed.wrapping_add(round) % 3) as usize;
        let proxies = [0_u64, 1, 2];
        for offset in 0..3 {
            let proxy = proxies[(rotation + offset) % 3];
            order.push(round.saturating_mul(3).saturating_add(proxy));
        }
    }
    order
}

fn inject_ticket_fault(ticketed: &mut [TicketedMultiProxyBatch], mode: MultiCommitProxyMode) {
    match mode {
        MultiCommitProxyMode::DuplicateCommitVersion if ticketed.len() > 2 => {
            ticketed[2].ticket.marker.current_version = ticketed[1].ticket.marker.current_version;
        }
        MultiCommitProxyMode::SkipPreviousVersion if ticketed.len() > 3 => {
            ticketed[3].ticket.marker.previous_version = ticketed[1].ticket.marker.current_version;
        }
        MultiCommitProxyMode::MutateTicketedBatch if ticketed.len() > 4 => {
            if let Some(MultiProxyTransaction { mutations, .. }) =
                ticketed[4].signed_batch.batch.transactions.first_mut()
            {
                if let Some(CellMutation::Set { value, .. }) = mutations.first_mut() {
                    value.push(0xff);
                }
            }
        }
        _ => {}
    }
}

fn finish_report(report: &mut MultiCommitProxyReport) {
    let checks = [
        ("sequencer_nodes", report.sequencer_nodes == 3),
        ("proxy_processes", report.proxy_process_starts == 3),
        ("resolver_processes", report.resolver_process_starts == 3),
        ("tlog_processes", report.tlog_process_starts == 6),
        ("transaction_count", report.attempted_transactions == 96),
        ("ticket_chain", report.unique_gap_free_ticket_chain),
        ("authority_binding", report.authority_marker_binding_exact),
        ("proxy_signatures", report.proxy_signatures_valid),
        ("proxy_identities", report.proxy_identities_pinned),
        ("pending_window", report.pending_window_bounded),
        ("resolver_order", report.all_resolvers_same_order),
        (
            "inside_batch_order",
            report.transactions_ordered_inside_batches,
        ),
        (
            "crossing_ranges",
            report.crossing_ranges_reached_every_overlap,
        ),
        ("oracle", report.dispositions_match_oracle),
        ("progress_frames", report.every_batch_has_progress_frame),
        ("conflict_progress", report.conflict_only_batches_advance),
        ("tlog_order", report.all_tlogs_same_order),
        ("tlog_bytes", report.tlog_frames_match_ticketed_batches),
        (
            "ack_quorums",
            report.acknowledgements_require_every_tlog_set,
        ),
        (
            "missing_predecessor",
            report.later_batches_blocked_by_missing_predecessor,
        ),
        ("stale_proxy", report.stale_proxy_rejected),
        ("rows_and_envelopes", report.exact_rows_and_envelopes),
        ("envelope_chain", report.envelope_chain_valid),
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
        report.mode != MultiCommitProxyMode::Correct && report.anomaly_count > 0;
    let bytes = serde_json::to_vec(report).unwrap_or_default();
    report.trace_sha256 = format!("{:x}", Sha256::digest(bytes));
}

fn clipped(
    conflicts: &[CellKeyRange],
    partition: &crate::CellResolverPartition,
) -> Vec<CellKeyRange> {
    let owned = CellKeyRange {
        start: partition.start.clone(),
        end: partition.end.clone(),
    };
    conflicts
        .iter()
        .filter(|range| range.overlaps(&owned))
        .map(|range| CellKeyRange {
            start: std::cmp::max(range.start.clone(), owned.start.clone()),
            end: std::cmp::min(range.end.clone(), owned.end.clone()),
        })
        .collect()
}

fn baseline_writes(resolver_id: u16) -> Vec<(u64, CellKeyRange)> {
    (0..3_u8)
        .filter(|partition| resolver_id == 0 || u16::from(*partition) + 1 == resolver_id)
        .map(|partition| (1, CellKeyRange::point(&hot_key(partition))))
        .collect()
}

fn hot_key(partition: u8) -> Vec<u8> {
    vec![match partition {
        0 => 0x20,
        1 => 0x70,
        _ => 0xc0,
    }]
}

fn unique_key(partition: u8, value: u64) -> Vec<u8> {
    let base = match partition {
        0 => 0x21,
        1 => 0x71,
        _ => 0xc1,
    };
    vec![base, u8::try_from(value % 32).unwrap_or(0)]
}

fn proxy_incarnation(proxy_id: u16) -> [u8; 16] {
    let mut incarnation = [0_u8; 16];
    incarnation[..2].copy_from_slice(&proxy_id.to_be_bytes());
    incarnation[2..].copy_from_slice(b"okv-proxy-v001");
    incarnation
}

fn stale_proxy_incarnation(proxy_id: u16) -> [u8; 16] {
    let mut incarnation = proxy_incarnation(proxy_id);
    incarnation[15] ^= 0xff;
    incarnation
}

fn proxy_key_pair(proxy_id: u16, incarnation: [u8; 16]) -> Result<Ed25519KeyPair, String> {
    let mut digest = Sha256::new();
    digest.update(b"okv-eval-multi-commit-proxy-key-v1");
    digest.update(proxy_id.to_be_bytes());
    digest.update(incarnation);
    let seed: [u8; 32] = digest.finalize().into();
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| "proxy signing seed is invalid".to_owned())
}

fn tlog_key_pair(log_set_id: u16, node_id: u16) -> Result<Ed25519KeyPair, String> {
    let mut digest = Sha256::new();
    digest.update(b"okv-eval-multi-proxy-tlog-key-v1");
    digest.update(log_set_id.to_be_bytes());
    digest.update(node_id.to_be_bytes());
    let seed: [u8; 32] = digest.finalize().into();
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| "tLog signing seed is invalid".to_owned())
}

fn batch_sha256(batch: &MultiProxyBatch) -> Result<[u8; 32], String> {
    serde_json::to_vec(batch)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|error| error.to_string())
}

pub(crate) fn verify_proxy_batch(signed: &SignedMultiProxyBatch) -> bool {
    let Ok(key) = proxy_key_pair(signed.batch.proxy_id, signed.batch.proxy_incarnation) else {
        return false;
    };
    let Ok(bytes) = serde_json::to_vec(&signed.batch) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key.public_key().as_ref())
        .verify(&bytes, &signed.signature)
        .is_ok()
}

fn verify_tlog_receipt(receipt: &MultiProxyTlogProcessReceipt) -> bool {
    let Ok(key) = tlog_key_pair(receipt.log_set_id, receipt.node_id) else {
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

fn permute_ticketed(
    batches: &[TicketedMultiProxyBatch],
    chunk_size: usize,
) -> Vec<TicketedMultiProxyBatch> {
    batches
        .chunks(chunk_size.max(2))
        .flat_map(|chunk| chunk.iter().rev().cloned())
        .collect()
}

fn permute_frames(
    frames: &[MultiProxyProgressFrame],
    chunk_size: usize,
) -> Vec<MultiProxyProgressFrame> {
    frames
        .chunks(chunk_size.max(2))
        .flat_map(|chunk| chunk.iter().rev().cloned())
        .collect()
}

fn progress_root_for_versions(frames: &[MultiProxyProgressFrame], versions: &[u64]) -> [u8; 32] {
    let selected = versions
        .iter()
        .filter_map(|version| {
            frames
                .iter()
                .find(|frame| frame.current_version == *version)
        })
        .cloned()
        .collect::<Vec<_>>();
    Sha256::digest(serde_json::to_vec(&selected).unwrap_or_default()).into()
}

fn count_out_of_order(
    ticketed: &[TicketedMultiProxyBatch],
    receipts: &[MultiProxyResolverProcessReceipt],
) -> u64 {
    let expected = ticketed
        .iter()
        .map(|batch| batch.ticket.marker.current_version)
        .collect::<Vec<_>>();
    receipts
        .iter()
        .map(|receipt| {
            receipt
                .processed_batch_versions
                .iter()
                .zip(&expected)
                .filter(|(observed, expected)| observed != expected)
                .count() as u64
        })
        .sum()
}

pub(crate) fn run_child_json<C: Serialize, R: DeserializeOwned>(
    executable: &Path,
    command: &str,
    config: &C,
) -> Result<R, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg(command)
        .arg("--config-json")
        .arg(config_json)
        .output()
        .map_err(|error| format!("failed to start {command}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{command} exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!(
            "{command} emitted an invalid receipt: {error}: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: MultiCommitProxyMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-multi-proxy-{}-{seed}-{}-{sequence}",
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
    fn bounded_permutations_require_buffering_but_preserve_membership() {
        let batches = generate_batches(1103, MultiCommitProxyMode::Correct);
        assert_eq!(batches.len(), 24);
        let order = proxy_interleaving(1103);
        assert_eq!(order.len(), 24);
        assert_eq!(order.iter().copied().collect::<BTreeSet<_>>().len(), 24);
        assert_eq!(proxy_incarnation(1), proxy_incarnation(1));
        assert_ne!(proxy_incarnation(1), stale_proxy_incarnation(1));
    }
}
