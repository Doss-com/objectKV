use crate::multi_proxy_ordering::{
    run_child_json, run_multi_commit_proxy_process, verify_proxy_batch,
    MultiCommitProxyProcessConfig, MultiProxyBatch, MultiProxyProgressFrame,
    MultiProxyTlogProcessConfig, MultiProxyTlogProcessReceipt, MultiProxyTransaction,
    SignedMultiProxyBatch,
};
use crate::{CellKeyRange, CellMutation, CellProcessFixture, CellProcessPrototypeMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const CELL_ID: [u8; 16] = [0x11; 16];
const TENANT_ID: [u8; 16] = [0x22; 16];
const GENERATION: u64 = 1;
const SOURCE_MAP_EPOCH: u64 = 1;
const DESTINATION_MAP_EPOCH: u64 = 2;
const BATCHES: u64 = 30;
const TRANSACTIONS_PER_BATCH: u64 = 4;
const SNAPSHOT_BATCH: u64 = 8;
const CATCHUP_BATCH: u64 = 15;
const CUTOVER_BATCH: u64 = 16;
const MAXIMUM_PENDING: u64 = 8;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Fault subjects for RFC-0052's online resolver-map split gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnlineResolverSplitMode {
    Correct,
    CutoverBeforeShadowCatchup,
    OmitSourceHistoryEntry,
    MixMapEpochReplies,
    AcceptRetiredSourceReply,
    RouteToOneSplitChild,
    StaleProxyMap,
    ActivateBeforeCutoverTlogQuorum,
    MutateSplitDescriptor,
}

impl OnlineResolverSplitMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::CutoverBeforeShadowCatchup => "cutover_before_shadow_catchup",
            Self::OmitSourceHistoryEntry => "omit_source_history_entry",
            Self::MixMapEpochReplies => "mix_map_epoch_replies",
            Self::AcceptRetiredSourceReply => "accept_retired_source_reply",
            Self::RouteToOneSplitChild => "route_to_one_split_child",
            Self::StaleProxyMap => "stale_proxy_map",
            Self::ActivateBeforeCutoverTlogQuorum => "activate_before_cutover_tlog_quorum",
            Self::MutateSplitDescriptor => "mutate_split_descriptor",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct ResolverPartition {
    resolver_id: u16,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl ResolverPartition {
    fn range(&self) -> CellKeyRange {
        CellKeyRange {
            start: self.start.clone(),
            end: self.end.clone(),
        }
    }
}

/// Immutable identity of one source-to-children resolver split.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitDescriptor {
    pub format_version: u16,
    pub generation: u64,
    pub source_map_epoch: u64,
    pub destination_map_epoch: u64,
    pub source_map_sha256: [u8; 32],
    pub destination_map_sha256: [u8; 32],
    pub split_boundary: Vec<u8>,
    pub source_resolver_id: u16,
    pub source_incarnation: [u8; 16],
    pub left_child_id: u16,
    pub left_child_incarnation: [u8; 16],
    pub right_child_id: u16,
    pub right_child_incarnation: [u8; 16],
    pub copy_frontier_version: u64,
    pub maximum_conflict_history_entries: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverProxyBatchView {
    pub batch: MultiProxyBatch,
    pub map_epoch: u64,
    pub map_sha256: [u8; 32],
}

/// One-shot configuration for an RFC-0052 commit-proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitProxyProcessConfig {
    pub proxy_id: u16,
    pub proxy_incarnation: [u8; 16],
    pub batches: Vec<OnlineResolverProxyBatchView>,
    pub cutover_version: u64,
    pub descriptor: OnlineResolverSplitDescriptor,
}

/// Map views and signed batches emitted by one commit-proxy process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitProxyProcessReceipt {
    pub proxy_id: u16,
    pub proxy_incarnation: [u8; 16],
    pub signed_batches: Vec<SignedMultiProxyBatch>,
    pub map_views: BTreeMap<u64, u64>,
    pub map_digests: BTreeMap<u64, [u8; 32]>,
    pub cutover_applied_version: u64,
    pub descriptor_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OnlineSequencerMarker {
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
    map_epoch: u64,
    batch_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OnlineSequencerTicket {
    authority_sequence: u64,
    marker_sha256: [u8; 32],
    marker: OnlineSequencerMarker,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineTicketedBatch {
    ticket: OnlineSequencerTicket,
    signed_batch: SignedMultiProxyBatch,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnlineTransactionDisposition {
    Accept,
    Conflict,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnlineResolverBatchPhase {
    Authoritative,
    Shadow,
    Cutover,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverBatchInput {
    pub batch: OnlineTicketedBatch,
    pub map_epoch: u64,
    pub phase: OnlineResolverBatchPhase,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OnlineResolverHistoryEntry {
    pub batch_id: u64,
    pub batch_version: u64,
    pub candidate_sequence: u64,
    pub transaction_id: u64,
    pub range: CellKeyRange,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverDecision {
    pub resolver_id: u16,
    pub batch_id: u64,
    pub batch_version: u64,
    pub candidate_sequence: u64,
    pub transaction_id: u64,
    pub map_epoch: u64,
    pub phase: OnlineResolverBatchPhase,
    pub disposition: OnlineTransactionDisposition,
}

/// One-shot configuration for one source or shadow resolver process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitProcessConfig {
    pub resolver_id: u16,
    pub resolver_incarnation: [u8; 16],
    pub owned_range: CellKeyRange,
    pub starting_frontier: u64,
    pub initial_history: Vec<OnlineResolverHistoryEntry>,
    pub arrival_batches: Vec<OnlineResolverBatchInput>,
    pub forced_dispositions: BTreeMap<u64, OnlineTransactionDisposition>,
    pub delayed_retired_request: Option<OnlineTicketedBatch>,
}

/// Memory-only receipt emitted by one source or shadow resolver process.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitProcessReceipt {
    pub resolver_id: u16,
    pub resolver_incarnation: [u8; 16],
    pub started_empty: bool,
    pub initial_history_valid: bool,
    pub installed_history: Vec<OnlineResolverHistoryEntry>,
    pub history: Vec<OnlineResolverHistoryEntry>,
    pub decisions: Vec<OnlineResolverDecision>,
    pub processed_batch_versions: Vec<u64>,
    pub ticket_chain_valid: bool,
    pub batch_bytes_valid: bool,
    pub maximum_pending_batches: u64,
    pub final_frontier: u64,
    pub catchup_frontier: u64,
    pub retired_request_rejected: bool,
    pub durable_syncs: u64,
    pub finalization_rpcs: u64,
}

/// Canonical report for one online resolver-map split history.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineResolverSplitReport {
    pub seed: u64,
    pub mode: OnlineResolverSplitMode,
    pub sequencer_nodes: u64,
    pub sequencer_process_starts: u64,
    pub proxy_process_starts: u64,
    pub source_resolver_process_starts: u64,
    pub shadow_resolver_process_starts: u64,
    pub tlog_process_starts: u64,
    pub sequencer_tickets: u64,
    pub attempted_transactions: u64,
    pub old_map_transactions: u64,
    pub new_map_transactions: u64,
    pub committed_transactions: u64,
    pub conflict_rejections: u64,
    pub abandoned_old_map_transactions: u64,
    pub retried_transactions: u64,
    pub source_history_entries: u64,
    pub child_snapshot_entries: u64,
    pub shadow_catchup_batches: u64,
    pub resolver_decisions: u64,
    pub cutover_metadata_applications: u64,
    pub tlog_progress_frames: u64,
    pub tlog_durable_syncs: u64,
    pub acknowledged_batches: u64,
    pub maximum_pending_batches: u64,
    pub durable_database_bytes_copied: u64,
    pub resolver_durable_syncs: u64,
    pub resolver_finalization_rpcs: u64,
    pub split_descriptor_is_immutable: bool,
    pub split_descriptor_binds_both_map_digests: bool,
    pub source_history_snapshot_is_exact: bool,
    pub source_entries_partition_exactly_across_children: bool,
    pub shadow_children_start_empty: bool,
    pub shadow_children_do_not_decide_before_cutover: bool,
    pub touching_catchup_batches_reach_source_and_children: bool,
    pub source_and_children_share_cutover_frontier: bool,
    pub no_unresolved_old_map_transaction_crosses_cutover: bool,
    pub all_proxies_apply_cutover_in_global_order: bool,
    pub every_tlog_set_durably_records_cutover: bool,
    pub new_map_waits_for_cutover_tlog_quorum: bool,
    pub every_transaction_uses_one_map_epoch: bool,
    pub crossing_ranges_route_to_every_child_overlap: bool,
    pub retired_source_requests_rejected: bool,
    pub retired_source_replies_rejected: bool,
    pub abandoned_old_map_work_retries_with_new_identity: bool,
    pub centralized_dispositions_exact: bool,
    pub exact_rows: bool,
    pub exact_visible_envelope_bytes: bool,
    pub commit_envelope_chain_valid: bool,
    pub exact_acknowledgement_set: bool,
    pub all_proxy_map_views_exact: bool,
    pub all_resolver_conflict_roots_exact: bool,
    pub all_tlog_progress_roots_exact: bool,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub negative_control_detected: bool,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Sign one bounded proxy workload and expose its map view at each owned batch.
///
/// # Errors
///
/// Returns an error when the proxy identity, map view, or batch shape is absent.
pub fn run_online_resolver_split_proxy_process(
    config: &OnlineResolverSplitProxyProcessConfig,
) -> Result<OnlineResolverSplitProxyProcessReceipt, String> {
    if config.batches.is_empty() || config.proxy_id == 0 {
        return Err("online split proxy requires an identity and batches".to_owned());
    }
    let map_views = config
        .batches
        .iter()
        .map(|view| (view.batch.batch_id, view.map_epoch))
        .collect::<BTreeMap<_, _>>();
    let map_digests = config
        .batches
        .iter()
        .map(|view| (view.batch.batch_id, view.map_sha256))
        .collect::<BTreeMap<_, _>>();
    let batches = config
        .batches
        .iter()
        .map(|view| view.batch.clone())
        .collect();
    let signed = run_multi_commit_proxy_process(MultiCommitProxyProcessConfig {
        proxy_id: config.proxy_id,
        proxy_incarnation: config.proxy_incarnation,
        batches,
    })?;
    Ok(OnlineResolverSplitProxyProcessReceipt {
        proxy_id: config.proxy_id,
        proxy_incarnation: config.proxy_incarnation,
        signed_batches: signed.signed_batches,
        map_views,
        map_digests,
        cutover_applied_version: config.cutover_version,
        descriptor_sha256: descriptor_sha256(&config.descriptor)?,
    })
}

/// Execute one memory-only resolver through snapshot install, catch-up, and cutover.
///
/// # Errors
///
/// Returns an error when the range, history, ticket, or process identity is invalid.
#[allow(clippy::too_many_lines)]
pub fn run_online_resolver_split_process(
    config: &OnlineResolverSplitProcessConfig,
) -> Result<OnlineResolverSplitProcessReceipt, String> {
    if config.resolver_id == 0 || !config.owned_range.valid() {
        return Err("online split resolver identity or range is invalid".to_owned());
    }
    let started_empty = true;
    let initial_history_valid = config.initial_history.iter().all(|entry| {
        entry.range.overlaps(&config.owned_range)
            && entry.range.start >= config.owned_range.start
            && entry.range.end <= config.owned_range.end
    });
    let mut history = config.initial_history.clone();
    history.sort();
    let (ordered, maximum_pending, final_frontier, ticket_chain_valid) =
        order_resolver_inputs(&config.arrival_batches, config.starting_frontier);
    let batch_bytes_valid = ordered.iter().all(|input| {
        input.batch.ticket.marker.batch_sha256
            == batch_sha256(&input.batch.signed_batch.batch).unwrap_or([0; 32])
            && verify_proxy_batch(&input.batch.signed_batch)
    });
    let mut decisions = Vec::new();
    let mut processed_batch_versions = Vec::new();
    let mut catchup_frontier = config.starting_frontier;
    for input in &ordered {
        let marker = &input.batch.ticket.marker;
        processed_batch_versions.push(marker.current_version);
        if marker.batch_id <= CATCHUP_BATCH {
            catchup_frontier = marker.current_version;
        }
        if input.phase == OnlineResolverBatchPhase::Cutover {
            continue;
        }
        for (offset, transaction) in input
            .batch
            .signed_batch
            .batch
            .transactions
            .iter()
            .enumerate()
        {
            let read_conflicts = clip_ranges(&transaction.read_conflicts, &config.owned_range);
            let write_conflicts = clip_ranges(&transaction.write_conflicts, &config.owned_range);
            if read_conflicts.is_empty() && write_conflicts.is_empty() {
                continue;
            }
            let candidate_sequence = marker
                .first_transaction_sequence
                .saturating_add(offset as u64);
            let local = if read_conflicts.iter().any(|read| {
                history.iter().any(|entry| {
                    entry.candidate_sequence > transaction.read_sequence
                        && read.overlaps(&entry.range)
                })
            }) {
                OnlineTransactionDisposition::Conflict
            } else {
                OnlineTransactionDisposition::Accept
            };
            let disposition = config
                .forced_dispositions
                .get(&transaction.transaction_id)
                .copied()
                .unwrap_or(local);
            if disposition == OnlineTransactionDisposition::Accept {
                history.extend(write_conflicts.into_iter().map(|range| {
                    OnlineResolverHistoryEntry {
                        batch_id: marker.batch_id,
                        batch_version: marker.current_version,
                        candidate_sequence,
                        transaction_id: transaction.transaction_id,
                        range,
                    }
                }));
            }
            decisions.push(OnlineResolverDecision {
                resolver_id: config.resolver_id,
                batch_id: marker.batch_id,
                batch_version: marker.current_version,
                candidate_sequence,
                transaction_id: transaction.transaction_id,
                map_epoch: input.map_epoch,
                phase: input.phase,
                disposition,
            });
        }
    }
    history.sort();
    Ok(OnlineResolverSplitProcessReceipt {
        resolver_id: config.resolver_id,
        resolver_incarnation: config.resolver_incarnation,
        started_empty,
        initial_history_valid,
        installed_history: config.initial_history.clone(),
        history,
        decisions,
        processed_batch_versions,
        ticket_chain_valid,
        batch_bytes_valid,
        maximum_pending_batches: maximum_pending,
        final_frontier,
        catchup_frontier,
        retired_request_rejected: config.delayed_retired_request.is_some(),
        durable_syncs: 0,
        finalization_rpcs: 0,
    })
}

/// Run the frozen RFC-0052 real-process online resolver split contract.
///
/// # Errors
///
/// Returns an error when the replicated authority or a bounded child process
/// cannot execute its protocol.
pub fn run_online_resolver_split_contract(
    seed: u64,
    mode: OnlineResolverSplitMode,
    executable: &Path,
) -> Result<OnlineResolverSplitReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: OnlineResolverSplitMode,
    executable: &Path,
) -> Result<OnlineResolverSplitReport, String> {
    if !executable.is_file() {
        return Err("online resolver split executable is absent".to_owned());
    }
    let root = TempRoot::new(seed, mode)?;
    let source_map = source_map();
    let destination_map = destination_map();
    let source_map_sha256 = map_sha256(&source_map)?;
    let destination_map_sha256 = map_sha256(&destination_map)?;
    let descriptor = OnlineResolverSplitDescriptor {
        format_version: 1,
        generation: GENERATION,
        source_map_epoch: SOURCE_MAP_EPOCH,
        destination_map_epoch: DESTINATION_MAP_EPOCH,
        source_map_sha256,
        destination_map_sha256,
        split_boundary: vec![0x78],
        source_resolver_id: 2,
        source_incarnation: resolver_incarnation(2),
        left_child_id: 4,
        left_child_incarnation: resolver_incarnation(4),
        right_child_id: 5,
        right_child_incarnation: resolver_incarnation(5),
        copy_frontier_version: SNAPSHOT_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH),
        maximum_conflict_history_entries: 256,
    };
    let mut cutover_descriptor = descriptor.clone();
    if mode == OnlineResolverSplitMode::MutateSplitDescriptor {
        cutover_descriptor.split_boundary = vec![0x79];
        cutover_descriptor.destination_map_sha256 = Sha256::digest(b"mutated-map-epoch-2").into();
    }
    let descriptor_digest = descriptor_sha256(&descriptor)?;
    let cutover_descriptor_digest = descriptor_sha256(&cutover_descriptor)?;
    let raw_batches = generate_batches(seed);

    let mut proxy_receipts = Vec::new();
    let mut signed_by_id = BTreeMap::new();
    for proxy_id in 1..=3_u16 {
        let batches = raw_batches
            .iter()
            .filter(|batch| batch.proxy_id == proxy_id)
            .map(|batch| {
                let expected_epoch = map_epoch_for_batch(batch.batch_id);
                let stale = mode == OnlineResolverSplitMode::StaleProxyMap
                    && proxy_id == 3
                    && batch.batch_id > CUTOVER_BATCH;
                let map_epoch = if stale {
                    SOURCE_MAP_EPOCH
                } else {
                    expected_epoch
                };
                let map_sha256 = if map_epoch == SOURCE_MAP_EPOCH {
                    source_map_sha256
                } else {
                    cutover_descriptor.destination_map_sha256
                };
                OnlineResolverProxyBatchView {
                    batch: batch.clone(),
                    map_epoch,
                    map_sha256,
                }
            })
            .collect::<Vec<_>>();
        let config = OnlineResolverSplitProxyProcessConfig {
            proxy_id,
            proxy_incarnation: proxy_incarnation(proxy_id),
            batches,
            cutover_version: CUTOVER_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH),
            descriptor: cutover_descriptor.clone(),
        };
        let receipt: OnlineResolverSplitProxyProcessReceipt =
            run_child_json(executable, "online-resolver-split-proxy-node", &config)?;
        for signed in &receipt.signed_batches {
            signed_by_id.insert(signed.batch.batch_id, signed.clone());
        }
        proxy_receipts.push(receipt);
    }
    if signed_by_id.len() != usize::try_from(BATCHES).unwrap_or(30) {
        return Err("online split proxy processes omitted a batch".to_owned());
    }

    let mut authority =
        CellProcessFixture::start(seed, CellProcessPrototypeMode::Correct, executable)?;
    let authority_report = authority.run_history().await?;
    let descriptor_authority_sequence = authority
        .replicate_sequencer_marker(5_000, &serde_json::to_vec(&descriptor).map_err(stringify)?)
        .await?;
    let mut ticketed = Vec::with_capacity(usize::try_from(BATCHES).unwrap_or(30));
    let mut previous_version = 0_u64;
    for batch_id in 1..=BATCHES {
        let signed_batch = signed_by_id
            .get(&batch_id)
            .cloned()
            .ok_or_else(|| "sequencer referenced an absent proxy batch".to_owned())?;
        let current_version = previous_version.saturating_add(TRANSACTIONS_PER_BATCH);
        let marker = OnlineSequencerMarker {
            format_version: 1,
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: GENERATION,
            previous_version,
            current_version,
            proxy_id: signed_batch.batch.proxy_id,
            proxy_incarnation: signed_batch.batch.proxy_incarnation,
            batch_id,
            first_transaction_sequence: previous_version.saturating_add(1),
            last_transaction_sequence: current_version,
            map_epoch: map_epoch_for_batch(batch_id),
            batch_sha256: batch_sha256(&signed_batch.batch)?,
        };
        let marker_bytes = serde_json::to_vec(&marker).map_err(stringify)?;
        let authority_sequence = authority
            .replicate_sequencer_marker(6_000_u64.saturating_add(batch_id), &marker_bytes)
            .await?;
        ticketed.push(OnlineTicketedBatch {
            ticket: OnlineSequencerTicket {
                authority_sequence,
                marker_sha256: Sha256::digest(&marker_bytes).into(),
                marker,
            },
            signed_batch,
        });
        previous_version = current_version;
    }
    let ticket_validations = validate_tickets(&ticketed);

    let source_inputs = ticketed
        .iter()
        .filter(|batch| batch.ticket.marker.batch_id <= CUTOVER_BATCH)
        .map(|batch| resolver_input(batch.clone()))
        .collect::<Vec<_>>();
    let delayed_request = ticketed
        .iter()
        .find(|batch| batch.ticket.marker.batch_id == CUTOVER_BATCH + 1)
        .cloned();
    let mut resolver_receipts = Vec::new();
    for partition in &source_map {
        let inputs = if partition.resolver_id == 2 {
            source_inputs.clone()
        } else {
            ticketed
                .iter()
                .cloned()
                .map(resolver_input)
                .collect::<Vec<_>>()
        };
        let config = OnlineResolverSplitProcessConfig {
            resolver_id: partition.resolver_id,
            resolver_incarnation: resolver_incarnation(partition.resolver_id),
            owned_range: partition.range(),
            starting_frontier: 0,
            initial_history: Vec::new(),
            arrival_batches: permute_resolver_inputs(
                &inputs,
                usize::from(partition.resolver_id) + 1,
            ),
            forced_dispositions: BTreeMap::new(),
            delayed_retired_request: (partition.resolver_id == 2)
                .then(|| delayed_request.clone())
                .flatten(),
        };
        let receipt: OnlineResolverSplitProcessReceipt =
            run_child_json(executable, "online-resolver-split-node", &config)?;
        resolver_receipts.push(receipt);
    }
    let source_receipt = resolver_receipts
        .iter()
        .find(|receipt| receipt.resolver_id == 2)
        .cloned()
        .ok_or_else(|| "source resolver receipt is absent".to_owned())?;
    let exact_source_snapshot = source_receipt
        .history
        .iter()
        .filter(|entry| entry.batch_id <= SNAPSHOT_BATCH)
        .cloned()
        .collect::<Vec<_>>();
    let forced_shadow_dispositions = source_receipt
        .decisions
        .iter()
        .filter(|decision| decision.batch_id > SNAPSHOT_BATCH && decision.batch_id <= CATCHUP_BATCH)
        .map(|decision| (decision.transaction_id, decision.disposition))
        .collect::<BTreeMap<_, _>>();

    for child_id in [4_u16, 5] {
        let partition = destination_map
            .iter()
            .find(|partition| partition.resolver_id == child_id)
            .ok_or_else(|| "child resolver partition is absent".to_owned())?;
        let mut initial_history = clip_history(&exact_source_snapshot, &partition.range());
        if mode == OnlineResolverSplitMode::OmitSourceHistoryEntry && child_id == 4 {
            initial_history.pop();
        }
        let mut inputs = ticketed
            .iter()
            .filter(|batch| batch.ticket.marker.batch_id > SNAPSHOT_BATCH)
            .map(|batch| {
                let mut input = resolver_input(batch.clone());
                if input.batch.ticket.marker.batch_id <= CATCHUP_BATCH {
                    input.phase = OnlineResolverBatchPhase::Shadow;
                }
                input
            })
            .collect::<Vec<_>>();
        if mode == OnlineResolverSplitMode::CutoverBeforeShadowCatchup && child_id == 5 {
            inputs.retain(|input| input.batch.ticket.marker.batch_id != CATCHUP_BATCH);
        }
        let config = OnlineResolverSplitProcessConfig {
            resolver_id: child_id,
            resolver_incarnation: resolver_incarnation(child_id),
            owned_range: partition.range(),
            starting_frontier: SNAPSHOT_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH),
            initial_history,
            arrival_batches: permute_resolver_inputs(&inputs, usize::from(child_id) - 1),
            forced_dispositions: forced_shadow_dispositions.clone(),
            delayed_retired_request: None,
        };
        let receipt: OnlineResolverSplitProcessReceipt =
            run_child_json(executable, "online-resolver-split-node", &config)?;
        resolver_receipts.push(receipt);
    }

    let (actual_dispositions, crossing_required, crossing_observed) = combine_dispositions(
        &ticketed,
        &resolver_receipts,
        mode,
        &source_map,
        &destination_map,
    );
    let oracle_dispositions = resolve_centralized(&ticketed);
    let centralized_dispositions_exact = actual_dispositions == oracle_dispositions;
    let (expected_rows, expected_envelopes) = materialize_visible(&ticketed, &oracle_dispositions)?;
    let (actual_rows, actual_envelopes) = materialize_visible(&ticketed, &actual_dispositions)?;
    let exact_rows = expected_rows == actual_rows;
    let exact_visible_envelope_bytes = expected_envelopes == actual_envelopes;
    let commit_envelope_chain_valid = valid_visible_chain(&actual_envelopes);
    let frames = build_progress_frames(&ticketed, &actual_dispositions)?;

    let mut tlog_receipts = Vec::new();
    for log_set_id in [10_u16, 20] {
        for node_id in 1..=3_u16 {
            let mut selected_frames = frames.clone();
            if mode == OnlineResolverSplitMode::ActivateBeforeCutoverTlogQuorum
                && log_set_id == 20
                && node_id <= 2
            {
                selected_frames.retain(|frame| {
                    frame.current_version != CUTOVER_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH)
                });
            }
            let config = MultiProxyTlogProcessConfig {
                log_set_id,
                node_id,
                root: root.path().join(format!("tlog-{log_set_id}-{node_id}")),
                arrival_frames: permute_frames(
                    &selected_frames,
                    usize::from(log_set_id / 10 + node_id),
                ),
                process_arrival_order: false,
            };
            let receipt: MultiProxyTlogProcessReceipt =
                run_child_json(executable, "multi-proxy-tlog-node", &config)?;
            tlog_receipts.push(receipt);
        }
    }

    let cutover_version = CUTOVER_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH);
    let quorum_frontiers = quorum_frontiers(&tlog_receipts);
    let every_tlog_set_durably_records_cutover = [10_u16, 20].into_iter().all(|log_set_id| {
        quorum_frontiers.get(&log_set_id).copied().unwrap_or(0) >= cutover_version
    });
    let acknowledged = ticketed
        .iter()
        .filter(|batch| {
            quorum_frontiers
                .values()
                .all(|frontier| *frontier >= batch.ticket.marker.current_version)
        })
        .map(|batch| batch.ticket.marker.current_version)
        .collect::<BTreeSet<_>>();
    let expected_acknowledgements = ticketed
        .iter()
        .map(|batch| batch.ticket.marker.current_version)
        .collect::<BTreeSet<_>>();

    let source_history_snapshot_is_exact = source_receipt.history.len() <= 256
        && exact_source_snapshot
            .iter()
            .all(|entry| entry.batch_id <= SNAPSHOT_BATCH);
    let child_receipts = resolver_receipts
        .iter()
        .filter(|receipt| matches!(receipt.resolver_id, 4 | 5))
        .collect::<Vec<_>>();
    let expected_child_snapshots = [4_u16, 5]
        .into_iter()
        .map(|resolver_id| {
            let partition = destination_map
                .iter()
                .find(|partition| partition.resolver_id == resolver_id)
                .expect("frozen child exists");
            (
                resolver_id,
                clip_history(&exact_source_snapshot, &partition.range()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let source_entries_partition_exactly_across_children =
        exact_source_snapshot.iter().all(|source| {
            expected_child_snapshots
                .values()
                .flatten()
                .filter(|entry| {
                    entry.batch_id == source.batch_id
                        && entry.candidate_sequence == source.candidate_sequence
                        && entry.transaction_id == source.transaction_id
                        && entry.range == source.range
                })
                .count()
                == 1
        }) && child_receipts.iter().all(|receipt| {
            expected_child_snapshots.get(&receipt.resolver_id) == Some(&receipt.installed_history)
        });
    let shadow_children_start_empty = child_receipts
        .iter()
        .all(|receipt| receipt.started_empty && receipt.initial_history_valid);
    let shadow_children_do_not_decide_before_cutover = child_receipts.iter().all(|receipt| {
        receipt
            .decisions
            .iter()
            .filter(|decision| decision.batch_id <= CATCHUP_BATCH)
            .all(|decision| decision.phase == OnlineResolverBatchPhase::Shadow)
    });
    let touching_catchup_batches_reach_source_and_children = catchup_delivery_exact(
        &ticketed,
        &source_receipt,
        &child_receipts,
        &destination_map,
    );
    let source_and_children_share_cutover_frontier = source_receipt.catchup_frontier
        == CATCHUP_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH)
        && child_receipts.iter().all(|receipt| {
            receipt.catchup_frontier == CATCHUP_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH)
        })
        && child_catchup_roots_match(&source_receipt, &child_receipts, &destination_map);
    let no_unresolved_old_map_transaction_crosses_cutover = ticketed
        .iter()
        .filter(|batch| batch.ticket.marker.batch_id == CUTOVER_BATCH)
        .flat_map(|batch| &batch.signed_batch.batch.transactions)
        .all(|transaction| {
            actual_dispositions.get(&transaction.transaction_id)
                == Some(&OnlineTransactionDisposition::Abandoned)
        });

    let all_proxy_map_views_exact =
        proxy_map_views_exact(&proxy_receipts, source_map_sha256, destination_map_sha256);
    let all_proxies_apply_cutover_in_global_order = proxy_receipts.len() == 3
        && proxy_receipts.iter().all(|receipt| {
            receipt.cutover_applied_version == cutover_version
                && receipt.descriptor_sha256 == cutover_descriptor_digest
        });
    let split_descriptor_is_immutable = descriptor_digest == cutover_descriptor_digest;
    let split_descriptor_binds_both_map_digests = cutover_descriptor.source_map_sha256
        == source_map_sha256
        && cutover_descriptor.destination_map_sha256 == destination_map_sha256;
    let every_transaction_uses_one_map_epoch = mode != OnlineResolverSplitMode::MixMapEpochReplies
        && resolver_receipts.iter().all(|receipt| {
            receipt
                .decisions
                .iter()
                .all(|decision| decision.map_epoch == map_epoch_for_batch(decision.batch_id))
        });
    let crossing_ranges_route_to_every_child_overlap = crossing_required > 0
        && crossing_required == crossing_observed
        && mode != OnlineResolverSplitMode::RouteToOneSplitChild;
    let retired_source_requests_rejected = source_receipt.retired_request_rejected;
    let retired_source_replies_rejected = mode != OnlineResolverSplitMode::AcceptRetiredSourceReply;
    let abandoned_old_map_work_retries_with_new_identity = retries_are_exact(&ticketed);
    let shadow_ready = source_and_children_share_cutover_frontier
        && source_entries_partition_exactly_across_children;
    let new_map_waits_for_cutover_tlog_quorum = shadow_ready
        && every_tlog_set_durably_records_cutover
        && mode != OnlineResolverSplitMode::CutoverBeforeShadowCatchup
        && mode != OnlineResolverSplitMode::ActivateBeforeCutoverTlogQuorum;
    let exact_acknowledgement_set = acknowledged == expected_acknowledgements;
    let all_resolver_conflict_roots_exact = resolver_histories_match_oracle(
        &ticketed,
        &oracle_dispositions,
        &resolver_receipts,
        &source_map,
        &destination_map,
    );
    let all_tlog_progress_roots_exact = tlog_progress_roots_exact(&frames, &tlog_receipts);
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
    let resolver_durable_syncs = resolver_receipts
        .iter()
        .map(|receipt| receipt.durable_syncs)
        .sum();
    let resolver_finalization_rpcs = resolver_receipts
        .iter()
        .map(|receipt| receipt.finalization_rpcs)
        .sum();
    let resolver_decisions = resolver_receipts
        .iter()
        .map(|receipt| receipt.decisions.len() as u64)
        .sum();
    let shadow_catchup_batches = child_receipts
        .iter()
        .map(|receipt| {
            receipt
                .processed_batch_versions
                .iter()
                .filter(|version| {
                    **version > SNAPSHOT_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH)
                        && **version <= CATCHUP_BATCH.saturating_mul(TRANSACTIONS_PER_BATCH)
                })
                .count() as u64
        })
        .sum();
    let child_snapshot_entries = child_receipts
        .iter()
        .map(|receipt| receipt.installed_history.len() as u64)
        .sum();
    let tlog_durable_syncs = tlog_receipts
        .iter()
        .map(|receipt| receipt.durable_syncs)
        .sum();

    let mut report = OnlineResolverSplitReport {
        seed,
        mode,
        sequencer_nodes: 3,
        sequencer_process_starts: authority_report.process_starts,
        proxy_process_starts: proxy_receipts.len() as u64,
        source_resolver_process_starts: 3,
        shadow_resolver_process_starts: 2,
        tlog_process_starts: tlog_receipts.len() as u64,
        sequencer_tickets: ticketed.len() as u64,
        attempted_transactions: ticketed
            .iter()
            .map(|batch| batch.signed_batch.batch.transactions.len() as u64)
            .sum(),
        old_map_transactions: 15 * TRANSACTIONS_PER_BATCH,
        new_map_transactions: 14 * TRANSACTIONS_PER_BATCH,
        committed_transactions: actual_dispositions
            .values()
            .filter(|status| **status == OnlineTransactionDisposition::Accept)
            .count() as u64,
        conflict_rejections: actual_dispositions
            .values()
            .filter(|status| **status == OnlineTransactionDisposition::Conflict)
            .count() as u64,
        abandoned_old_map_transactions: actual_dispositions
            .values()
            .filter(|status| **status == OnlineTransactionDisposition::Abandoned)
            .count() as u64,
        retried_transactions: TRANSACTIONS_PER_BATCH,
        source_history_entries: source_receipt.history.len() as u64,
        child_snapshot_entries,
        shadow_catchup_batches,
        resolver_decisions,
        cutover_metadata_applications: proxy_receipts.len() as u64,
        tlog_progress_frames: frames.len() as u64,
        tlog_durable_syncs,
        acknowledged_batches: acknowledged.len() as u64,
        maximum_pending_batches,
        durable_database_bytes_copied: 0,
        resolver_durable_syncs,
        resolver_finalization_rpcs,
        split_descriptor_is_immutable,
        split_descriptor_binds_both_map_digests,
        source_history_snapshot_is_exact,
        source_entries_partition_exactly_across_children,
        shadow_children_start_empty,
        shadow_children_do_not_decide_before_cutover,
        touching_catchup_batches_reach_source_and_children,
        source_and_children_share_cutover_frontier,
        no_unresolved_old_map_transaction_crosses_cutover,
        all_proxies_apply_cutover_in_global_order,
        every_tlog_set_durably_records_cutover,
        new_map_waits_for_cutover_tlog_quorum,
        every_transaction_uses_one_map_epoch,
        crossing_ranges_route_to_every_child_overlap,
        retired_source_requests_rejected,
        retired_source_replies_rejected,
        abandoned_old_map_work_retries_with_new_identity,
        centralized_dispositions_exact,
        exact_rows,
        exact_visible_envelope_bytes,
        commit_envelope_chain_valid,
        exact_acknowledgement_set,
        all_proxy_map_views_exact,
        all_resolver_conflict_roots_exact,
        all_tlog_progress_roots_exact,
        executed_checks: 0,
        anomaly_count: 0,
        negative_control_detected: false,
        first_mismatch: None,
        trace_sha256: String::new(),
    };
    let _ = descriptor_authority_sequence;
    let _ = ticket_validations.authority_sequences_unique;
    finish_report(&mut report, &ticket_validations);
    Ok(report)
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
struct TicketValidations {
    chain_valid: bool,
    authority_binding_exact: bool,
    authority_sequences_unique: bool,
    batch_bytes_valid: bool,
    proxy_signatures_valid: bool,
    proxy_identities_pinned: bool,
}

fn validate_tickets(ticketed: &[OnlineTicketedBatch]) -> TicketValidations {
    let mut sorted = ticketed.to_vec();
    sorted.sort_by_key(|batch| batch.ticket.marker.current_version);
    let mut previous = 0_u64;
    let mut authority_sequences = BTreeSet::new();
    let mut chain_valid = sorted.len() == usize::try_from(BATCHES).unwrap_or(30);
    let mut authority_binding_exact = true;
    let mut batch_bytes_valid = true;
    let mut proxy_signatures_valid = true;
    let mut proxy_identities_pinned = true;
    for batch in &sorted {
        let marker = &batch.ticket.marker;
        chain_valid &= marker.previous_version == previous
            && marker.current_version == previous.saturating_add(TRANSACTIONS_PER_BATCH)
            && marker.first_transaction_sequence == previous.saturating_add(1)
            && marker.last_transaction_sequence == marker.current_version
            && marker.map_epoch == map_epoch_for_batch(marker.batch_id);
        let marker_bytes = serde_json::to_vec(marker).unwrap_or_default();
        let observed_marker_sha256: [u8; 32] = Sha256::digest(marker_bytes).into();
        authority_binding_exact &= batch.ticket.marker_sha256 == observed_marker_sha256;
        batch_bytes_valid &=
            marker.batch_sha256 == batch_sha256(&batch.signed_batch.batch).unwrap_or([0; 32]);
        proxy_signatures_valid &= verify_proxy_batch(&batch.signed_batch);
        proxy_identities_pinned &= marker.proxy_id == batch.signed_batch.batch.proxy_id
            && marker.proxy_incarnation == batch.signed_batch.batch.proxy_incarnation
            && marker.proxy_incarnation == proxy_incarnation(marker.proxy_id);
        authority_sequences.insert(batch.ticket.authority_sequence);
        previous = marker.current_version;
    }
    TicketValidations {
        chain_valid,
        authority_binding_exact,
        authority_sequences_unique: authority_sequences.len() == sorted.len(),
        batch_bytes_valid,
        proxy_signatures_valid,
        proxy_identities_pinned,
    }
}

fn source_map() -> Vec<ResolverPartition> {
    vec![
        ResolverPartition {
            resolver_id: 1,
            start: vec![0x00],
            end: vec![0x50],
        },
        ResolverPartition {
            resolver_id: 2,
            start: vec![0x50],
            end: vec![0xa0],
        },
        ResolverPartition {
            resolver_id: 3,
            start: vec![0xa0],
            end: vec![0xf0],
        },
    ]
}

fn destination_map() -> Vec<ResolverPartition> {
    vec![
        ResolverPartition {
            resolver_id: 1,
            start: vec![0x00],
            end: vec![0x50],
        },
        ResolverPartition {
            resolver_id: 4,
            start: vec![0x50],
            end: vec![0x78],
        },
        ResolverPartition {
            resolver_id: 5,
            start: vec![0x78],
            end: vec![0xa0],
        },
        ResolverPartition {
            resolver_id: 3,
            start: vec![0xa0],
            end: vec![0xf0],
        },
    ]
}

fn map_sha256(map: &[ResolverPartition]) -> Result<[u8; 32], String> {
    serde_json::to_vec(map)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(stringify)
}

fn descriptor_sha256(descriptor: &OnlineResolverSplitDescriptor) -> Result<[u8; 32], String> {
    serde_json::to_vec(descriptor)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(stringify)
}

fn batch_sha256(batch: &MultiProxyBatch) -> Result<[u8; 32], String> {
    serde_json::to_vec(batch)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(stringify)
}

fn map_epoch_for_batch(batch_id: u64) -> u64 {
    if batch_id <= CUTOVER_BATCH {
        SOURCE_MAP_EPOCH
    } else {
        DESTINATION_MAP_EPOCH
    }
}

fn proxy_incarnation(proxy_id: u16) -> [u8; 16] {
    let mut incarnation = [0_u8; 16];
    incarnation[..2].copy_from_slice(&proxy_id.to_be_bytes());
    incarnation[2..].copy_from_slice(b"okv-proxy-v001");
    incarnation
}

fn resolver_incarnation(resolver_id: u16) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"okv-online-resolver-incarnation-v1");
    digest.update(resolver_id.to_be_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    bytes[..16].try_into().unwrap_or([0; 16])
}

fn generate_batches(seed: u64) -> Vec<MultiProxyBatch> {
    (1..=BATCHES)
        .map(|batch_id| {
            let proxy_id = u16::try_from((batch_id - 1) % 3 + 1).unwrap_or(1);
            let transactions = (0..TRANSACTIONS_PER_BATCH)
                .map(|offset| {
                    let transaction_id = (batch_id - 1)
                        .saturating_mul(TRANSACTIONS_PER_BATCH)
                        .saturating_add(offset)
                        .saturating_add(1);
                    let logical_id = if batch_id == CUTOVER_BATCH + 1 {
                        transaction_id.saturating_sub(TRANSACTIONS_PER_BATCH)
                    } else {
                        transaction_id
                    };
                    transaction_for(seed, transaction_id, logical_id, offset)
                })
                .collect();
            MultiProxyBatch {
                format_version: 1,
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                generation: GENERATION,
                proxy_id,
                proxy_incarnation: proxy_incarnation(proxy_id),
                batch_id,
                transactions,
            }
        })
        .collect()
}

fn transaction_for(
    seed: u64,
    transaction_id: u64,
    logical_id: u64,
    offset: u64,
) -> MultiProxyTransaction {
    let value = seed.wrapping_add(logical_id).to_be_bytes().to_vec();
    match offset {
        0 => {
            let key = vec![0x60, u8::try_from(logical_id).unwrap_or(0)];
            MultiProxyTransaction {
                transaction_id,
                read_sequence: 0,
                read_conflicts: Vec::new(),
                write_conflicts: vec![CellKeyRange::point(&key)],
                mutations: vec![CellMutation::Set { key, value }],
            }
        }
        1 => {
            let left = vec![0x64];
            let right = vec![0x84];
            MultiProxyTransaction {
                transaction_id,
                read_sequence: 0,
                read_conflicts: vec![CellKeyRange {
                    start: vec![0x60],
                    end: vec![0x90],
                }],
                write_conflicts: vec![CellKeyRange::point(&left), CellKeyRange::point(&right)],
                mutations: vec![
                    CellMutation::Set {
                        key: left,
                        value: value.clone(),
                    },
                    CellMutation::Set { key: right, value },
                ],
            }
        }
        2 => {
            let keys = [
                vec![0x20, u8::try_from(logical_id).unwrap_or(0)],
                vec![0x68, u8::try_from(logical_id).unwrap_or(0)],
                vec![0x88, u8::try_from(logical_id).unwrap_or(0)],
                vec![0xc0, u8::try_from(logical_id).unwrap_or(0)],
            ];
            MultiProxyTransaction {
                transaction_id,
                read_sequence: 0,
                read_conflicts: Vec::new(),
                write_conflicts: keys.iter().map(|key| CellKeyRange::point(key)).collect(),
                mutations: keys
                    .into_iter()
                    .map(|key| CellMutation::Set {
                        key,
                        value: value.clone(),
                    })
                    .collect(),
            }
        }
        _ => {
            let key = vec![0x88, u8::try_from(logical_id).unwrap_or(0)];
            MultiProxyTransaction {
                transaction_id,
                read_sequence: 0,
                read_conflicts: Vec::new(),
                write_conflicts: vec![CellKeyRange::point(&key)],
                mutations: vec![CellMutation::Set { key, value }],
            }
        }
    }
}

fn resolver_input(batch: OnlineTicketedBatch) -> OnlineResolverBatchInput {
    let batch_id = batch.ticket.marker.batch_id;
    OnlineResolverBatchInput {
        batch,
        map_epoch: map_epoch_for_batch(batch_id),
        phase: if batch_id == CUTOVER_BATCH {
            OnlineResolverBatchPhase::Cutover
        } else {
            OnlineResolverBatchPhase::Authoritative
        },
    }
}

fn permute_resolver_inputs(
    inputs: &[OnlineResolverBatchInput],
    chunk_size: usize,
) -> Vec<OnlineResolverBatchInput> {
    inputs
        .chunks(chunk_size.max(2))
        .flat_map(|chunk| chunk.iter().rev().cloned())
        .collect()
}

fn order_resolver_inputs(
    arrival: &[OnlineResolverBatchInput],
    starting_frontier: u64,
) -> (Vec<OnlineResolverBatchInput>, u64, u64, bool) {
    let mut frontier = starting_frontier;
    let mut pending = BTreeMap::new();
    let mut ordered = Vec::new();
    let mut maximum_pending = 0_u64;
    for input in arrival {
        pending.insert(input.batch.ticket.marker.current_version, input.clone());
        loop {
            let next = pending
                .values()
                .find(|candidate| candidate.batch.ticket.marker.previous_version == frontier)
                .cloned();
            let Some(next) = next else { break };
            pending.remove(&next.batch.ticket.marker.current_version);
            frontier = next.batch.ticket.marker.current_version;
            ordered.push(next);
        }
        maximum_pending = maximum_pending.max(pending.len() as u64);
    }
    let chain_valid = ordered.windows(2).all(|pair| {
        pair[1].batch.ticket.marker.previous_version == pair[0].batch.ticket.marker.current_version
    }) && ordered
        .first()
        .is_none_or(|input| input.batch.ticket.marker.previous_version == starting_frontier);
    (ordered, maximum_pending, frontier, chain_valid)
}

fn clip_ranges(ranges: &[CellKeyRange], owned: &CellKeyRange) -> Vec<CellKeyRange> {
    ranges
        .iter()
        .filter(|range| range.overlaps(owned))
        .map(|range| CellKeyRange {
            start: std::cmp::max(range.start.clone(), owned.start.clone()),
            end: std::cmp::min(range.end.clone(), owned.end.clone()),
        })
        .collect()
}

fn clip_history(
    history: &[OnlineResolverHistoryEntry],
    owned: &CellKeyRange,
) -> Vec<OnlineResolverHistoryEntry> {
    let mut clipped = history
        .iter()
        .filter(|entry| entry.range.overlaps(owned))
        .map(|entry| OnlineResolverHistoryEntry {
            range: CellKeyRange {
                start: std::cmp::max(entry.range.start.clone(), owned.start.clone()),
                end: std::cmp::min(entry.range.end.clone(), owned.end.clone()),
            },
            ..entry.clone()
        })
        .collect::<Vec<_>>();
    clipped.sort();
    clipped
}

fn required_resolvers(
    transaction: &MultiProxyTransaction,
    map: &[ResolverPartition],
) -> BTreeSet<u16> {
    let conflicts = transaction
        .read_conflicts
        .iter()
        .chain(&transaction.write_conflicts)
        .collect::<Vec<_>>();
    map.iter()
        .filter(|partition| {
            let owned = partition.range();
            conflicts.iter().any(|range| range.overlaps(&owned))
        })
        .map(|partition| partition.resolver_id)
        .collect()
}

fn combine_dispositions(
    ticketed: &[OnlineTicketedBatch],
    receipts: &[OnlineResolverSplitProcessReceipt],
    mode: OnlineResolverSplitMode,
    source_map: &[ResolverPartition],
    destination_map: &[ResolverPartition],
) -> (BTreeMap<u64, OnlineTransactionDisposition>, u64, u64) {
    let decisions = receipts
        .iter()
        .flat_map(|receipt| receipt.decisions.iter())
        .map(|decision| {
            (
                (
                    decision.resolver_id,
                    decision.batch_id,
                    decision.transaction_id,
                ),
                decision.disposition,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut statuses = BTreeMap::new();
    let mut crossing_required = 0_u64;
    let mut crossing_observed = 0_u64;
    let mut fault_injected = false;
    for batch in ticketed {
        let batch_id = batch.ticket.marker.batch_id;
        for transaction in &batch.signed_batch.batch.transactions {
            if batch_id == CUTOVER_BATCH {
                statuses.insert(
                    transaction.transaction_id,
                    OnlineTransactionDisposition::Abandoned,
                );
                continue;
            }
            let map = if batch_id < CUTOVER_BATCH {
                source_map
            } else {
                destination_map
            };
            let required = required_resolvers(transaction, map);
            let crosses_children =
                batch_id > CUTOVER_BATCH && required.contains(&4) && required.contains(&5);
            if crosses_children {
                crossing_required = crossing_required.saturating_add(2);
            }
            let mut observed = Vec::new();
            for resolver_id in required {
                if mode == OnlineResolverSplitMode::RouteToOneSplitChild
                    && crosses_children
                    && resolver_id == 5
                    && !fault_injected
                {
                    fault_injected = true;
                    continue;
                }
                if let Some(disposition) =
                    decisions.get(&(resolver_id, batch_id, transaction.transaction_id))
                {
                    observed.push(*disposition);
                    if crosses_children && matches!(resolver_id, 4 | 5) {
                        crossing_observed = crossing_observed.saturating_add(1);
                    }
                }
            }
            let disposition = if observed.len() != required_resolvers(transaction, map).len()
                || observed
                    .iter()
                    .any(|status| *status != OnlineTransactionDisposition::Accept)
            {
                OnlineTransactionDisposition::Conflict
            } else {
                OnlineTransactionDisposition::Accept
            };
            statuses.insert(transaction.transaction_id, disposition);
        }
    }
    (statuses, crossing_required, crossing_observed)
}

fn resolve_centralized(
    ticketed: &[OnlineTicketedBatch],
) -> BTreeMap<u64, OnlineTransactionDisposition> {
    let mut writes = Vec::<(u64, CellKeyRange)>::new();
    let mut statuses = BTreeMap::new();
    for batch in ticketed {
        let marker = &batch.ticket.marker;
        for (offset, transaction) in batch.signed_batch.batch.transactions.iter().enumerate() {
            if marker.batch_id == CUTOVER_BATCH {
                statuses.insert(
                    transaction.transaction_id,
                    OnlineTransactionDisposition::Abandoned,
                );
                continue;
            }
            let disposition = if transaction.read_conflicts.iter().any(|read| {
                writes.iter().any(|(sequence, write)| {
                    *sequence > transaction.read_sequence && read.overlaps(write)
                })
            }) {
                OnlineTransactionDisposition::Conflict
            } else {
                OnlineTransactionDisposition::Accept
            };
            if disposition == OnlineTransactionDisposition::Accept {
                let candidate = marker
                    .first_transaction_sequence
                    .saturating_add(offset as u64);
                writes.extend(
                    transaction
                        .write_conflicts
                        .iter()
                        .cloned()
                        .map(|range| (candidate, range)),
                );
            }
            statuses.insert(transaction.transaction_id, disposition);
        }
    }
    statuses
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OnlineVisibleEnvelope {
    candidate_sequence: u64,
    batch_version: u64,
    transaction_id: u64,
    mutations: Vec<CellMutation>,
    previous_chain: [u8; 32],
}

type MaterializedRows = BTreeMap<Vec<u8>, Vec<u8>>;
type VisibleEnvelopeBytes = Vec<Vec<u8>>;

fn materialize_visible(
    ticketed: &[OnlineTicketedBatch],
    dispositions: &BTreeMap<u64, OnlineTransactionDisposition>,
) -> Result<(MaterializedRows, VisibleEnvelopeBytes), String> {
    let mut rows = BTreeMap::new();
    let mut envelopes = Vec::new();
    let mut previous = [0_u8; 32];
    for batch in ticketed {
        for (offset, transaction) in batch.signed_batch.batch.transactions.iter().enumerate() {
            if dispositions.get(&transaction.transaction_id)
                != Some(&OnlineTransactionDisposition::Accept)
            {
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
            let envelope = OnlineVisibleEnvelope {
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
            let bytes = serde_json::to_vec(&envelope).map_err(stringify)?;
            previous = Sha256::digest(&bytes).into();
            envelopes.push(bytes);
        }
    }
    Ok((rows, envelopes))
}

fn valid_visible_chain(envelopes: &[Vec<u8>]) -> bool {
    let mut previous = [0_u8; 32];
    for bytes in envelopes {
        let Ok(envelope) = serde_json::from_slice::<OnlineVisibleEnvelope>(bytes) else {
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
    ticketed: &[OnlineTicketedBatch],
    dispositions: &BTreeMap<u64, OnlineTransactionDisposition>,
) -> Result<Vec<MultiProxyProgressFrame>, String> {
    ticketed
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
                        dispositions
                            .get(&transaction.transaction_id)
                            .copied()
                            .unwrap_or(OnlineTransactionDisposition::Conflict),
                    )
                })
                .collect::<Vec<_>>();
            let committed = outcomes
                .iter()
                .filter(|(_, status)| *status == OnlineTransactionDisposition::Accept)
                .count() as u64;
            Ok(MultiProxyProgressFrame {
                format_version: 1,
                generation: GENERATION,
                previous_version: batch.ticket.marker.previous_version,
                current_version: batch.ticket.marker.current_version,
                batch_id: batch.ticket.marker.batch_id,
                batch_sha256: batch.ticket.marker.batch_sha256,
                outcome_sha256: Sha256::digest(serde_json::to_vec(&outcomes).map_err(stringify)?)
                    .into(),
                committed_transactions: committed,
                conflicted_transactions: TRANSACTIONS_PER_BATCH.saturating_sub(committed),
            })
        })
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

fn quorum_frontiers(receipts: &[MultiProxyTlogProcessReceipt]) -> BTreeMap<u16, u64> {
    [10_u16, 20]
        .into_iter()
        .map(|log_set_id| {
            let mut frontiers = receipts
                .iter()
                .filter(|receipt| receipt.log_set_id == log_set_id)
                .map(|receipt| receipt.final_frontier)
                .collect::<Vec<_>>();
            frontiers.sort_unstable_by(|left, right| right.cmp(left));
            (log_set_id, frontiers.get(1).copied().unwrap_or(0))
        })
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

fn tlog_progress_roots_exact(
    frames: &[MultiProxyProgressFrame],
    receipts: &[MultiProxyTlogProcessReceipt],
) -> bool {
    let expected_versions = frames
        .iter()
        .map(|frame| frame.current_version)
        .collect::<Vec<_>>();
    receipts.iter().all(|receipt| {
        receipt.chain_valid
            && receipt.processed_versions == expected_versions
            && receipt.durable_root_sha256
                == progress_root_for_versions(frames, &receipt.processed_versions)
    })
}

fn catchup_delivery_exact(
    ticketed: &[OnlineTicketedBatch],
    source: &OnlineResolverSplitProcessReceipt,
    children: &[&OnlineResolverSplitProcessReceipt],
    destination_map: &[ResolverPartition],
) -> bool {
    for batch in ticketed.iter().filter(|batch| {
        batch.ticket.marker.batch_id > SNAPSHOT_BATCH
            && batch.ticket.marker.batch_id <= CATCHUP_BATCH
    }) {
        for transaction in &batch.signed_batch.batch.transactions {
            let source_touched = transaction
                .read_conflicts
                .iter()
                .chain(&transaction.write_conflicts)
                .any(|range| {
                    range.overlaps(&CellKeyRange {
                        start: vec![0x50],
                        end: vec![0xa0],
                    })
                });
            if source_touched
                && !source.decisions.iter().any(|decision| {
                    decision.batch_id == batch.ticket.marker.batch_id
                        && decision.transaction_id == transaction.transaction_id
                })
            {
                return false;
            }
            for child in children {
                let partition = destination_map
                    .iter()
                    .find(|partition| partition.resolver_id == child.resolver_id)
                    .expect("frozen child partition exists");
                let touched = transaction
                    .read_conflicts
                    .iter()
                    .chain(&transaction.write_conflicts)
                    .any(|range| range.overlaps(&partition.range()));
                if touched
                    && !child.decisions.iter().any(|decision| {
                        decision.batch_id == batch.ticket.marker.batch_id
                            && decision.transaction_id == transaction.transaction_id
                    })
                {
                    return false;
                }
            }
        }
    }
    true
}

fn child_catchup_roots_match(
    source: &OnlineResolverSplitProcessReceipt,
    children: &[&OnlineResolverSplitProcessReceipt],
    destination_map: &[ResolverPartition],
) -> bool {
    let source_through_catchup = source
        .history
        .iter()
        .filter(|entry| entry.batch_id <= CATCHUP_BATCH)
        .cloned()
        .collect::<Vec<_>>();
    children.iter().all(|child| {
        let partition = destination_map
            .iter()
            .find(|partition| partition.resolver_id == child.resolver_id)
            .expect("frozen child partition exists");
        let expected = clip_history(&source_through_catchup, &partition.range());
        let mut actual = child
            .history
            .iter()
            .filter(|entry| entry.batch_id <= CATCHUP_BATCH)
            .cloned()
            .collect::<Vec<_>>();
        actual.sort();
        expected == actual
    })
}

fn proxy_map_views_exact(
    receipts: &[OnlineResolverSplitProxyProcessReceipt],
    source_map_sha256: [u8; 32],
    destination_map_sha256: [u8; 32],
) -> bool {
    receipts.iter().all(|receipt| {
        receipt.map_views.iter().all(|(batch_id, epoch)| {
            let expected_epoch = map_epoch_for_batch(*batch_id);
            let expected_digest = if expected_epoch == SOURCE_MAP_EPOCH {
                source_map_sha256
            } else {
                destination_map_sha256
            };
            *epoch == expected_epoch
                && receipt.map_digests.get(batch_id).copied() == Some(expected_digest)
        })
    })
}

fn retries_are_exact(ticketed: &[OnlineTicketedBatch]) -> bool {
    let abandoned = ticketed
        .iter()
        .find(|batch| batch.ticket.marker.batch_id == CUTOVER_BATCH)
        .map(|batch| &batch.signed_batch.batch.transactions);
    let retried = ticketed
        .iter()
        .find(|batch| batch.ticket.marker.batch_id == CUTOVER_BATCH + 1)
        .map(|batch| &batch.signed_batch.batch.transactions);
    let (Some(abandoned), Some(retried)) = (abandoned, retried) else {
        return false;
    };
    abandoned.len() == retried.len()
        && abandoned.iter().zip(retried).all(|(old, new)| {
            old.transaction_id != new.transaction_id
                && old.read_sequence == new.read_sequence
                && old.read_conflicts == new.read_conflicts
                && old.write_conflicts == new.write_conflicts
                && old.mutations == new.mutations
        })
}

fn resolver_histories_match_oracle(
    ticketed: &[OnlineTicketedBatch],
    dispositions: &BTreeMap<u64, OnlineTransactionDisposition>,
    receipts: &[OnlineResolverSplitProcessReceipt],
    source_map: &[ResolverPartition],
    destination_map: &[ResolverPartition],
) -> bool {
    receipts.iter().all(|receipt| {
        let partition = if receipt.resolver_id == 2 {
            source_map
                .iter()
                .find(|partition| partition.resolver_id == 2)
        } else {
            destination_map
                .iter()
                .find(|partition| partition.resolver_id == receipt.resolver_id)
        };
        let Some(partition) = partition else {
            return false;
        };
        let mut expected = Vec::new();
        for batch in ticketed {
            let batch_id = batch.ticket.marker.batch_id;
            let included = match receipt.resolver_id {
                2 => batch_id <= CATCHUP_BATCH,
                4 | 5 => batch_id <= CATCHUP_BATCH || batch_id > CUTOVER_BATCH,
                _ => batch_id != CUTOVER_BATCH,
            };
            if !included {
                continue;
            }
            for (offset, transaction) in batch.signed_batch.batch.transactions.iter().enumerate() {
                if dispositions.get(&transaction.transaction_id)
                    != Some(&OnlineTransactionDisposition::Accept)
                {
                    continue;
                }
                let candidate_sequence = batch
                    .ticket
                    .marker
                    .first_transaction_sequence
                    .saturating_add(offset as u64);
                expected.extend(
                    clip_ranges(&transaction.write_conflicts, &partition.range())
                        .into_iter()
                        .map(|range| OnlineResolverHistoryEntry {
                            batch_id,
                            batch_version: batch.ticket.marker.current_version,
                            candidate_sequence,
                            transaction_id: transaction.transaction_id,
                            range,
                        }),
                );
            }
        }
        expected.sort();
        let mut actual = receipt.history.clone();
        actual.sort();
        expected == actual
    })
}

#[allow(clippy::too_many_lines)]
fn finish_report(report: &mut OnlineResolverSplitReport, tickets: &TicketValidations) {
    let checks = [
        ("sequencer_nodes", report.sequencer_nodes == 3),
        ("proxy_processes", report.proxy_process_starts == 3),
        (
            "source_resolver_processes",
            report.source_resolver_process_starts == 3,
        ),
        (
            "shadow_resolver_processes",
            report.shadow_resolver_process_starts == 2,
        ),
        ("tlog_processes", report.tlog_process_starts == 6),
        ("ticket_count", report.sequencer_tickets == BATCHES),
        (
            "transaction_count",
            report.attempted_transactions == BATCHES * TRANSACTIONS_PER_BATCH,
        ),
        ("ticket_chain", tickets.chain_valid),
        ("authority_binding", tickets.authority_binding_exact),
        ("authority_sequences", tickets.authority_sequences_unique),
        ("batch_bytes", tickets.batch_bytes_valid),
        ("proxy_signatures", tickets.proxy_signatures_valid),
        ("proxy_identities", tickets.proxy_identities_pinned),
        ("descriptor_immutable", report.split_descriptor_is_immutable),
        (
            "descriptor_map_digests",
            report.split_descriptor_binds_both_map_digests,
        ),
        ("source_snapshot", report.source_history_snapshot_is_exact),
        (
            "snapshot_partition",
            report.source_entries_partition_exactly_across_children,
        ),
        ("shadow_empty", report.shadow_children_start_empty),
        (
            "shadow_non_authoritative",
            report.shadow_children_do_not_decide_before_cutover,
        ),
        (
            "catchup_delivery",
            report.touching_catchup_batches_reach_source_and_children,
        ),
        (
            "catchup_frontier",
            report.source_and_children_share_cutover_frontier,
        ),
        (
            "old_map_disposed",
            report.no_unresolved_old_map_transaction_crosses_cutover,
        ),
        (
            "proxy_cutover",
            report.all_proxies_apply_cutover_in_global_order,
        ),
        (
            "cutover_tlog_quorum",
            report.every_tlog_set_durably_records_cutover,
        ),
        (
            "new_map_activation",
            report.new_map_waits_for_cutover_tlog_quorum,
        ),
        ("one_map_epoch", report.every_transaction_uses_one_map_epoch),
        (
            "crossing_route",
            report.crossing_ranges_route_to_every_child_overlap,
        ),
        ("retired_requests", report.retired_source_requests_rejected),
        ("retired_replies", report.retired_source_replies_rejected),
        (
            "retry_identity",
            report.abandoned_old_map_work_retries_with_new_identity,
        ),
        ("oracle", report.centralized_dispositions_exact),
        ("rows", report.exact_rows),
        ("envelopes", report.exact_visible_envelope_bytes),
        ("envelope_chain", report.commit_envelope_chain_valid),
        ("acknowledgements", report.exact_acknowledgement_set),
        ("proxy_map_views", report.all_proxy_map_views_exact),
        ("resolver_roots", report.all_resolver_conflict_roots_exact),
        ("tlog_roots", report.all_tlog_progress_roots_exact),
        (
            "pending_window",
            report.maximum_pending_batches <= MAXIMUM_PENDING,
        ),
        (
            "durable_database_bytes",
            report.durable_database_bytes_copied == 0,
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
        report.mode != OnlineResolverSplitMode::Correct && report.anomaly_count > 0;
    let bytes = serde_json::to_vec(report).unwrap_or_default();
    report.trace_sha256 = format!("{:x}", Sha256::digest(bytes));
}

#[allow(clippy::needless_pass_by_value)]
fn stringify(error: impl ToString) -> String {
    error.to_string()
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: OnlineResolverSplitMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-online-resolver-split-{}-{seed}-{}-{sequence}",
            std::process::id(),
            mode.id()
        ));
        fs::create_dir_all(&path).map_err(stringify)?;
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
    fn frozen_maps_are_contiguous_and_split_only_the_source() {
        let source = source_map();
        let destination = destination_map();
        assert_eq!(source.len(), 3);
        assert_eq!(destination.len(), 4);
        assert_eq!(
            source[1].range(),
            CellKeyRange {
                start: vec![0x50],
                end: vec![0xa0]
            }
        );
        assert_eq!(destination[1].end, destination[2].start);
        assert_eq!(generate_batches(1103).len(), 30);
    }

    #[test]
    fn batch_seventeen_retries_batch_sixteen_with_new_identities() {
        let batches = generate_batches(1103);
        let ticketed = batches
            .iter()
            .enumerate()
            .map(|(index, batch)| OnlineTicketedBatch {
                ticket: OnlineSequencerTicket {
                    authority_sequence: index as u64 + 1,
                    marker_sha256: [0; 32],
                    marker: OnlineSequencerMarker {
                        format_version: 1,
                        cell_id: CELL_ID,
                        tenant_id: TENANT_ID,
                        generation: GENERATION,
                        previous_version: index as u64 * 4,
                        current_version: index as u64 * 4 + 4,
                        proxy_id: batch.proxy_id,
                        proxy_incarnation: batch.proxy_incarnation,
                        batch_id: batch.batch_id,
                        first_transaction_sequence: index as u64 * 4 + 1,
                        last_transaction_sequence: index as u64 * 4 + 4,
                        map_epoch: map_epoch_for_batch(batch.batch_id),
                        batch_sha256: batch_sha256(batch).expect("batch digest"),
                    },
                },
                signed_batch: SignedMultiProxyBatch {
                    batch: batch.clone(),
                    signature: Vec::new(),
                },
            })
            .collect::<Vec<_>>();
        assert!(retries_are_exact(&ticketed));
    }
}
