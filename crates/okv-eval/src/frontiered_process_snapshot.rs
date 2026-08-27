//! G4.11a.1 frontier-aligned bounded process-snapshot contract.

use okv_consensus::{
    object_frontier_certificate_statement, GenerationClient, GenerationCredential,
    ObjectFrontierCertificate, ObjectFrontierRecord, ProcessJournalCompactionObservation,
    PublicationAction, PublicationAuthorityProcessFixture, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity, RetainedTransactionReadRequest, TransactionAuthorityProcessFixture,
    TransactionBatchApplyResponse, TransactionBatchItem, TransactionFrontierAdvance,
    TransactionLogClient, TransactionLogStorageStatsRequest, TransactionRetryFloor,
};
use okv_object::{
    advance_validated_row_object_frontier, content_sha256, encode_row_object_set,
    filesystem_backend, read_point_from_full_object, validate_row_object_frontier, Backend,
    ObjectClient, PointReadOutcome, RowObjectManifestV1, RowObjectReference, RowRecord,
    RowSegmentIndex, WriteCondition,
};
use okv_transaction::{
    KeyRange, Mutation, RetainedTransactionRecord, TransactionApplyResponse, TransactionCommand,
    TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";
const TARGET_OBJECT_BYTES: usize = 64 * 1024;
const TARGET_BLOCK_BYTES: usize = 8 * 1024;

/// Frozen G4.11a.1 candidate, omitted-frontier control, or accounting poison.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontieredProcessSnapshotMode {
    #[serde(rename = "aligned_r_q_o_candidate")]
    AlignedRqoCandidate,
    NoRetryFrontierControl,
    AccountingPoison,
}

impl FrontieredProcessSnapshotMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::AlignedRqoCandidate => "aligned_r_q_o_candidate",
            Self::NoRetryFrontierControl => "no_retry_frontier_control",
            Self::AccountingPoison => "accounting_poison",
        }
    }
}

/// Frozen G4.11a.1 repeated-frontier bounds.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontieredProcessSnapshotProfile {
    pub frontier_cycles: u64,
    pub transactions_per_cycle: u64,
    pub transactions_per_batch: usize,
    pub live_keys: u64,
    pub value_bytes: usize,
    pub retry_window: u64,
    pub max_physical_amplification: f64,
    pub max_snapshot_growth_ratio: f64,
}

/// Physical and semantic observations from one complete frontier cycle.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontieredSnapshotCycleObservation {
    pub cycle: u64,
    pub object_version: u64,
    pub resolver_floor: u64,
    pub retry_floor: Option<u64>,
    pub retained_retry_outcomes: u64,
    pub retained_retry_fingerprints: u64,
    pub retained_recovery_records: u64,
    pub retained_conflict_versions: u64,
    pub data_snapshot_position: u64,
    pub publication_snapshot_position: u64,
    pub data_compaction: Vec<ProcessJournalCompactionObservation>,
    pub publication_compaction: Vec<ProcessJournalCompactionObservation>,
    pub journal_bytes_before: u64,
    pub journal_bytes_after: u64,
    pub snapshot_bytes: u64,
    pub actual_physical_bytes: u64,
    pub reported_physical_bytes: u64,
    pub physical_amplification: f64,
    pub batch_commit_p99_seconds: f64,
    pub maintenance_seconds: f64,
    pub snapshot_covers_purge: bool,
    pub journals_reclaimed: bool,
    pub expired_retry_rejected_without_mutation: bool,
    pub retained_retry_exact: bool,
    pub full_quorum_restart_exact: bool,
    pub publication_retry_after_restart_exact: bool,
    pub frontier_attestation_after_restart_exact: bool,
    pub object_state_after_restart_exact: bool,
    pub object_plus_suffix_after_restart_exact: bool,
}

/// Canonical G4.11a.1 report from separate three-process publication and data
/// quorums plus one immutable-object backend.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FrontieredProcessSnapshotReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: FrontieredProcessSnapshotMode,
    pub data_authority_processes: u64,
    pub publication_authority_processes: u64,
    pub release_build: bool,
    pub logical_bytes: u64,
    pub frontier_cycles: u64,
    pub complete_frontier_cycles: u64,
    pub transaction_count: u64,
    pub committed_count: u64,
    pub cycles: Vec<FrontieredSnapshotCycleObservation>,
    pub maximum_physical_amplification: f64,
    pub maximum_actual_physical_amplification: f64,
    pub snapshot_growth_ratio: f64,
    pub bounded_lifetime_media_curve: bool,
    pub no_retry_frontier_control_detected: bool,
    pub accounting_poison_detected: bool,
    pub suffix_commit_after_final_restart: bool,
    pub final_object_plus_suffix_exact: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

#[derive(Clone, Debug)]
struct PublishedClosure {
    root: String,
    reference: PublicationObjectReference,
    manifest: RowObjectManifestV1,
}

/// Execute one G4.11a.1 repeated-frontier subject.
///
/// # Errors
///
/// Returns an error when profile validation, process startup, transaction
/// application, publication, frontier advancement, snapshot maintenance, or
/// recovery cannot complete.
pub fn run_frontiered_process_snapshot_contract(
    seed: u64,
    mode: FrontieredProcessSnapshotMode,
    profile: &FrontieredProcessSnapshotProfile,
    executable: &Path,
) -> Result<FrontieredProcessSnapshotReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run(seed, mode, profile, executable))
}

#[allow(clippy::too_many_lines)]
async fn run(
    seed: u64,
    mode: FrontieredProcessSnapshotMode,
    profile: &FrontieredProcessSnapshotProfile,
    executable: &Path,
) -> Result<FrontieredProcessSnapshotReport, String> {
    let object_root = TempDir::new().map_err(|error| error.to_string())?;
    let mut publication_authority =
        PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let publication = publication_authority.client()?;
    let generation = GenerationClient::new(publication_authority.endpoints())?;
    let mut data_authority = TransactionAuthorityProcessFixture::start_fenced(
        executable,
        seed.saturating_add(10_000),
        publication_authority.authority_nodes(),
    )
    .await?;
    let transaction_log = data_authority.client()?;
    let credential = GenerationCredential {
        generation: GENERATION,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let backend: Arc<dyn Backend> =
        filesystem_backend(object_root.path()).map_err(|error| error.to_string())?;
    let object_client = ObjectClient::new(backend.clone());
    let data_authority_processes =
        u64::try_from(data_authority.process_count()).unwrap_or(u64::MAX);
    let publication_authority_processes =
        u64::try_from(publication_authority.process_count()).unwrap_or(u64::MAX);
    let logical_bytes = profile
        .live_keys
        .saturating_mul(u64::try_from(profile.value_bytes).unwrap_or(u64::MAX));
    let workload_client_id = seed.max(1).saturating_add(100_000);
    let mut prior_manifest = None;
    let mut prior_active = None;
    let mut prior_version = 0;
    let mut committed_count = 0_u64;
    let mut first_item = None;
    let mut first_outcome = None;
    let mut final_object_state = BTreeMap::new();
    let mut final_object_version = 0;
    let mut cycles = Vec::new();

    for cycle in 1..=profile.frontier_cycles {
        let cycle_start = (cycle - 1).saturating_mul(profile.transactions_per_cycle);
        let items = workload_items(
            seed,
            workload_client_id,
            cycle_start,
            prior_version,
            profile,
            &credential,
        );
        if first_item.is_none() {
            first_item = items.first().cloned();
        }
        let mut responses = Vec::new();
        let mut batch_latencies = Vec::new();
        for batch in items.chunks(profile.transactions_per_batch) {
            let started = Instant::now();
            let response = transaction_log.commit_batch(batch).await?;
            batch_latencies.push(started.elapsed().as_secs_f64());
            responses.push(response);
        }
        let committed_this_cycle = count_committed(&responses);
        if committed_this_cycle != profile.transactions_per_cycle {
            return Err(format!(
                "G4.11a.1 cycle {cycle} committed {committed_this_cycle} of {} transactions",
                profile.transactions_per_cycle
            ));
        }
        committed_count = committed_count.saturating_add(committed_this_cycle);
        if first_outcome.is_none() {
            first_outcome = response_for_identity(
                &responses,
                first_item
                    .as_ref()
                    .ok_or_else(|| "G4.11a.1 workload is empty".to_owned())?
                    .identity,
            );
        }
        let latest_item = items
            .last()
            .cloned()
            .ok_or_else(|| "G4.11a.1 workload is empty".to_owned())?;
        let latest_outcome = response_for_identity(&responses, latest_item.identity)
            .ok_or_else(|| "G4.11a.1 latest transaction outcome is missing".to_owned())?;
        let view = data_authority.voter_transaction_view(201).await?;
        let object_version = view.current_version;
        if object_version <= prior_version {
            return Err(format!(
                "G4.11a.1 cycle {cycle} did not advance the transaction version"
            ));
        }
        let object_state = authority_values(&view);
        let row_records = authority_rows(&view);
        let closure = publish_closure(
            seed,
            cycle,
            object_version,
            &row_records,
            &backend,
            &publication,
            prior_manifest.as_ref(),
        )
        .await?;
        let pending = prepare_frontier(
            seed,
            cycle,
            object_version,
            &closure,
            &publication,
            prior_active.as_ref(),
        )
        .await?;
        let validated = validate_row_object_frontier(&object_client, &pending).await?;
        let total_requests = cycle.saturating_mul(profile.transactions_per_cycle);
        let retry_floor = (mode != FrontieredProcessSnapshotMode::NoRetryFrontierControl)
            .then(|| total_requests.saturating_sub(profile.retry_window));
        let retry_floors = retry_floor.map_or_else(Vec::new, |through_request_id| {
            vec![TransactionRetryFloor {
                client_id: workload_client_id,
                through_request_id,
            }]
        });
        let frontier_response = transaction_log
            .advance_frontiers_fenced(
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(400_000),
                    request_id: cycle,
                },
                &credential,
                &TransactionFrontierAdvance {
                    sequence: cycle,
                    conflict_retention_floor: object_version,
                    retry_floors: retry_floors.clone(),
                },
            )
            .await?;
        if frontier_response.conflict_retention_floor != object_version
            || frontier_response.retry_floors != retry_floors
        {
            return Err(format!(
                "G4.11a.1 cycle {cycle} applied different transaction frontiers"
            ));
        }
        let popped = advance_validated_row_object_frontier(
            &transaction_log,
            RequestIdentity {
                client_id: seed.max(1).saturating_add(410_000),
                request_id: cycle,
            },
            &credential,
            &validated,
        )
        .await?;
        let generation_state = generation.read().await?;
        let statement = object_frontier_certificate_statement(
            &generation_state,
            pending.clone(),
            popped.applied_log_position,
        );
        let certificate = ObjectFrontierCertificate {
            statement: statement.clone(),
            attestations: transaction_log.attest_object_frontier(&statement).await?,
        };
        let activation_command = publication_command(
            seed,
            cycle.saturating_mul(10).saturating_add(4),
            PublicationAction::ActivateObjectFrontier {
                expected_pending: pending.clone(),
                certificate,
            },
        );
        let activation = publication.commit(&activation_command).await?;
        if activation.status != PublicationCommandStatus::Accepted
            || activation.state.active_object_frontier.as_ref() != Some(&pending)
        {
            return Err(format!(
                "G4.11a.1 cycle {cycle} did not activate the exact object frontier"
            ));
        }

        let view_before_retry_probe = data_authority.voter_transaction_view(201).await?;
        let first_item_ref = first_item
            .as_ref()
            .ok_or_else(|| "G4.11a.1 first transaction is missing".to_owned())?;
        let first_expected = first_outcome
            .as_ref()
            .ok_or_else(|| "G4.11a.1 first transaction outcome is missing".to_owned())?;
        let expired_retry_rejected_without_mutation = if retry_floor.is_some() {
            let rejection = transaction_log
                .commit_fenced_once(
                    first_item_ref.identity,
                    &credential,
                    &first_item_ref.command,
                )
                .await
                .expect_err("expired G4.11a.1 retry must fail");
            rejection.contains("below its retained floor")
                && data_authority.voter_transaction_view(201).await? == view_before_retry_probe
        } else {
            transaction_log
                .commit_fenced(
                    first_item_ref.identity,
                    &credential,
                    &first_item_ref.command,
                )
                .await?
                == *first_expected
                && data_authority.voter_transaction_view(201).await? == view_before_retry_probe
        };
        let retained_retry_exact = transaction_log
            .commit_fenced(latest_item.identity, &credential, &latest_item.command)
            .await?
            == latest_outcome;
        let storage_before = transaction_log
            .storage_stats(TransactionLogStorageStatsRequest::default())
            .await?;
        let publication_state_before = publication.read().await?;
        let data_view_before = data_authority.voter_transaction_view(201).await?;

        let maintenance_started = Instant::now();
        let (data_snapshot_position, data_compaction) =
            data_authority.snapshot_and_purge_applied_all().await?;
        let (publication_snapshot_position, publication_compaction) = publication_authority
            .snapshot_and_purge_applied_all()
            .await?;
        let maintenance_seconds = maintenance_started.elapsed().as_secs_f64();
        let data_compaction = data_compaction.into_values().collect::<Vec<_>>();
        let publication_compaction = publication_compaction.into_values().collect::<Vec<_>>();
        let all_compaction = data_compaction
            .iter()
            .chain(publication_compaction.iter())
            .collect::<Vec<_>>();
        let snapshot_covers_purge = all_compaction.iter().all(|observation| {
            observation.snapshot_index >= observation.purged_index
                && observation.purged_index != 0
                && observation.compaction_calls > 0
        });
        let journals_reclaimed = all_compaction.iter().all(|observation| {
            observation.journal_bytes_after < observation.journal_bytes_before
                && observation.compaction_reclaimed_bytes > 0
        });
        let journal_bytes_before: u64 = all_compaction
            .iter()
            .map(|observation| observation.journal_bytes_before)
            .sum();
        let journal_bytes_after: u64 = all_compaction
            .iter()
            .map(|observation| observation.journal_bytes_after)
            .sum();
        let snapshot_bytes: u64 = all_compaction
            .iter()
            .map(|observation| observation.snapshot_bytes)
            .sum();
        let actual_physical_bytes = snapshot_bytes.saturating_add(journal_bytes_after);
        let reported_physical_bytes = if mode == FrontieredProcessSnapshotMode::AccountingPoison {
            journal_bytes_after
        } else {
            actual_physical_bytes
        };

        publication_authority
            .restart_all_and_elect_initial()
            .await?;
        data_authority.restart_all_and_elect_initial().await?;
        let publication_state_after = publication.read().await?;
        let data_view_after = data_authority.voter_transaction_view(201).await?;
        let storage_after = transaction_log
            .storage_stats(TransactionLogStorageStatsRequest::default())
            .await?;
        let full_quorum_restart_exact = publication_state_after == publication_state_before
            && data_view_after == data_view_before
            && storage_after == storage_before;
        let publication_retry_after_restart_exact =
            publication.commit(&activation_command).await? == activation;
        let restart_attestations = transaction_log.attest_object_frontier(&statement).await?;
        let distinct_attestations = restart_attestations
            .iter()
            .map(|attestation| attestation.signer_id)
            .collect::<BTreeSet<_>>();
        let frontier_attestation_after_restart_exact = distinct_attestations.len() == 3;
        let loaded_object_state = load_object_state(
            &object_client,
            &closure.manifest,
            object_version,
            &object_state,
        )
        .await?;
        let object_state_after_restart_exact = loaded_object_state == object_state;
        let retained_suffix = read_all(&transaction_log, object_version, object_version).await?;
        let mut reconstructed = loaded_object_state.clone();
        replay_into(&mut reconstructed, &retained_suffix);
        let object_plus_suffix_after_restart_exact = reconstructed == object_state;
        let expected_retry_outcomes = retry_floor.map_or(total_requests, |_| profile.retry_window);
        let expected_state_shape = storage_after.conflict_retention_floor == object_version
            && storage_after.transaction_retry_outcomes == expected_retry_outcomes
            && storage_after.transaction_retry_fingerprints == expected_retry_outcomes
            && storage_after.retained_records == 0
            && storage_after.retained_conflict_versions == 0;
        if !expected_state_shape {
            return Err(format!(
                "G4.11a.1 cycle {cycle} retained unexpected state: {storage_after:?}"
            ));
        }
        let physical_amplification = ratio(reported_physical_bytes, logical_bytes);
        cycles.push(FrontieredSnapshotCycleObservation {
            cycle,
            object_version,
            resolver_floor: storage_after.conflict_retention_floor,
            retry_floor,
            retained_retry_outcomes: storage_after.transaction_retry_outcomes,
            retained_retry_fingerprints: storage_after.transaction_retry_fingerprints,
            retained_recovery_records: storage_after.retained_records,
            retained_conflict_versions: storage_after.retained_conflict_versions,
            data_snapshot_position,
            publication_snapshot_position,
            data_compaction,
            publication_compaction,
            journal_bytes_before,
            journal_bytes_after,
            snapshot_bytes,
            actual_physical_bytes,
            reported_physical_bytes,
            physical_amplification,
            batch_commit_p99_seconds: percentile_99(&batch_latencies),
            maintenance_seconds,
            snapshot_covers_purge,
            journals_reclaimed,
            expired_retry_rejected_without_mutation,
            retained_retry_exact,
            full_quorum_restart_exact,
            publication_retry_after_restart_exact,
            frontier_attestation_after_restart_exact,
            object_state_after_restart_exact,
            object_plus_suffix_after_restart_exact,
        });
        prior_manifest = Some(closure.reference);
        prior_active = Some(pending);
        prior_version = object_version;
        final_object_state = object_state;
        final_object_version = object_version;
    }

    let suffix_items = suffix_items(seed, prior_version, profile, &credential);
    let suffix_response = transaction_log.commit_batch(&suffix_items).await?;
    let suffix_commit_after_final_restart = suffix_response.items.iter().all(|item| {
        item.transaction.as_ref().is_some_and(|response| {
            matches!(
                response.status,
                TransactionStatus::Committed { commit_version }
                    if commit_version > final_object_version
            )
        })
    });
    let final_view = data_authority.voter_transaction_view(201).await?;
    let final_suffix = read_all(
        &transaction_log,
        final_object_version,
        final_view.current_version,
    )
    .await?;
    let mut reconstructed = final_object_state;
    replay_into(&mut reconstructed, &final_suffix);
    let final_object_plus_suffix_exact = reconstructed == authority_values(&final_view);
    let maximum_physical_amplification = cycles
        .iter()
        .map(|cycle| cycle.physical_amplification)
        .fold(0.0_f64, f64::max);
    let maximum_actual_physical_amplification = cycles
        .iter()
        .map(|cycle| ratio(cycle.actual_physical_bytes, logical_bytes))
        .fold(0.0_f64, f64::max);
    let snapshot_growth_ratio = cycles
        .first()
        .zip(cycles.last())
        .map_or(f64::INFINITY, |(first, last)| {
            ratio(last.snapshot_bytes, first.snapshot_bytes)
        });
    let bounded_lifetime_media_curve = maximum_actual_physical_amplification
        <= profile.max_physical_amplification
        && snapshot_growth_ratio <= profile.max_snapshot_growth_ratio;
    let no_retry_frontier_control_detected = mode
        != FrontieredProcessSnapshotMode::NoRetryFrontierControl
        || cycles
            .first()
            .zip(cycles.last())
            .is_some_and(|(first, last)| {
                last.retained_retry_outcomes > first.retained_retry_outcomes
                    && last.retained_retry_outcomes
                        == profile
                            .frontier_cycles
                            .saturating_mul(profile.transactions_per_cycle)
            });
    let accounting_poison_detected = mode != FrontieredProcessSnapshotMode::AccountingPoison
        || cycles
            .iter()
            .all(|cycle| cycle.reported_physical_bytes < cycle.actual_physical_bytes);
    let semantic_exact = cycles.iter().all(|cycle| {
        cycle.snapshot_covers_purge
            && cycle.journals_reclaimed
            && cycle.expired_retry_rejected_without_mutation
            && cycle.retained_retry_exact
            && cycle.full_quorum_restart_exact
            && cycle.publication_retry_after_restart_exact
            && cycle.frontier_attestation_after_restart_exact
            && cycle.object_state_after_restart_exact
            && cycle.object_plus_suffix_after_restart_exact
    });
    let correctness_anomalies = u64::from(data_authority_processes != 3)
        + u64::from(publication_authority_processes != 3)
        + u64::from(committed_count != profile.frontier_cycles * profile.transactions_per_cycle)
        + u64::from(cycles.len() != usize::try_from(profile.frontier_cycles).unwrap_or(usize::MAX))
        + u64::from(!semantic_exact)
        + u64::from(!no_retry_frontier_control_detected)
        + u64::from(!accounting_poison_detected)
        + u64::from(!suffix_commit_after_final_restart)
        + u64::from(!final_object_plus_suffix_exact);
    let semantic_sha256 = semantic_sha(&(
        seed,
        mode.id(),
        &cycles,
        committed_count,
        suffix_commit_after_final_restart,
        final_object_plus_suffix_exact,
        correctness_anomalies,
    ))?;
    Ok(FrontieredProcessSnapshotReport {
        format_version: 1,
        seed,
        mode,
        data_authority_processes,
        publication_authority_processes,
        release_build: !cfg!(debug_assertions),
        logical_bytes,
        frontier_cycles: profile.frontier_cycles,
        complete_frontier_cycles: u64::try_from(cycles.len()).unwrap_or(u64::MAX),
        transaction_count: profile.frontier_cycles * profile.transactions_per_cycle,
        committed_count,
        cycles,
        maximum_physical_amplification,
        maximum_actual_physical_amplification,
        snapshot_growth_ratio,
        bounded_lifetime_media_curve,
        no_retry_frontier_control_detected,
        accounting_poison_detected,
        suffix_commit_after_final_restart,
        final_object_plus_suffix_exact,
        correctness_anomalies,
        semantic_sha256,
    })
}

fn workload_items(
    seed: u64,
    client_id: u64,
    start: u64,
    read_version: u64,
    profile: &FrontieredProcessSnapshotProfile,
    credential: &GenerationCredential,
) -> Vec<TransactionBatchItem> {
    (0..profile.transactions_per_cycle)
        .map(|offset| {
            let request_index = start.saturating_add(offset);
            let key = workload_key(seed, request_index % profile.live_keys);
            TransactionBatchItem {
                identity: RequestIdentity {
                    client_id,
                    request_id: request_index.saturating_add(1),
                },
                credential: Some(credential.clone()),
                command: TransactionCommand {
                    read_version,
                    read_conflicts: Vec::new(),
                    write_conflicts: vec![KeyRange::point(&key)],
                    mutations: vec![Mutation::Set {
                        key,
                        value: deterministic_value(seed, request_index, profile.value_bytes),
                    }],
                },
            }
        })
        .collect()
}

fn suffix_items(
    seed: u64,
    read_version: u64,
    profile: &FrontieredProcessSnapshotProfile,
    credential: &GenerationCredential,
) -> Vec<TransactionBatchItem> {
    (0..profile.transactions_per_batch)
        .map(|offset| {
            let offset = u64::try_from(offset).unwrap_or(u64::MAX);
            let key = format!("frontiered-snapshot/{seed}/suffix/{offset:04}").into_bytes();
            TransactionBatchItem {
                identity: RequestIdentity {
                    client_id: seed.max(1).saturating_add(200_000),
                    request_id: offset.saturating_add(1),
                },
                credential: Some(credential.clone()),
                command: TransactionCommand {
                    read_version,
                    read_conflicts: Vec::new(),
                    write_conflicts: vec![KeyRange::point(&key)],
                    mutations: vec![Mutation::Set {
                        key,
                        value: deterministic_value(seed, offset, profile.value_bytes),
                    }],
                },
            }
        })
        .collect()
}

fn workload_key(seed: u64, index: u64) -> Vec<u8> {
    format!("frontiered-snapshot/{seed}/key/{index:04}").into_bytes()
}

async fn publish_closure(
    seed: u64,
    cycle: u64,
    covered_through: u64,
    records: &[RowRecord],
    backend: &Arc<dyn Backend>,
    publication: &okv_consensus::PublicationClient,
    prior_manifest: Option<&PublicationObjectReference>,
) -> Result<PublishedClosure, String> {
    let encoded =
        encode_row_object_set(GENERATION, records, TARGET_OBJECT_BYTES, TARGET_BLOCK_BYTES)?;
    let prefix = format!("rows-g411a1/{seed}/cycle-{cycle}");
    let references = encoded
        .iter()
        .map(|segment| RowObjectReference::from_encoded(&prefix, segment))
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = RowObjectManifestV1::new(GENERATION, covered_through, references.clone())?;
    let manifest_bytes = manifest.encode()?;
    let reference = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: format!(
            "{prefix}/manifest/sha256/{}",
            content_sha256(&manifest_bytes)
        ),
        length: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
        sha256: content_sha256(&manifest_bytes),
    };
    let root = format!("object-frontier/g411a1/{seed}");
    let publication_id = format!("object-frontier-publication/g411a1/{seed}/{cycle}");
    let mut object_keys = BTreeSet::from([reference.key.clone()]);
    for child in &references {
        object_keys.insert(child.data_key.clone());
        object_keys.insert(child.index_key.clone());
    }
    let prepared = publication
        .commit(&publication_command(
            seed,
            cycle.saturating_mul(10).saturating_add(1),
            PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent: PublicationIntent {
                    object_keys,
                    manifest: reference.clone(),
                    destination_root: root.clone(),
                    expected_prior_root: prior_manifest.cloned(),
                },
            },
        ))
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "G4.11a.1 cycle {cycle} publication prepare was rejected: {:?}",
            prepared.status
        ));
    }
    for (segment, child) in encoded.iter().zip(&references) {
        backend
            .put(
                &child.data_key,
                segment.data.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
        backend
            .put(
                &child.index_key,
                segment.index.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    backend
        .put(
            &reference.key,
            manifest_bytes.into(),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    let published = publication
        .commit(&publication_command(
            seed,
            cycle.saturating_mul(10).saturating_add(2),
            PublicationAction::Publish {
                publication_id,
                destination_root: root.clone(),
                expected_prior_root: prior_manifest.cloned(),
                manifest: reference.clone(),
            },
        ))
        .await?;
    if published.status != PublicationCommandStatus::Accepted
        || published.state.roots.get(&root) != Some(&reference)
    {
        return Err(format!(
            "G4.11a.1 cycle {cycle} publication root was not installed exactly"
        ));
    }
    Ok(PublishedClosure {
        root,
        reference,
        manifest,
    })
}

async fn prepare_frontier(
    seed: u64,
    cycle: u64,
    covered_through: u64,
    closure: &PublishedClosure,
    publication: &okv_consensus::PublicationClient,
    prior_active: Option<&ObjectFrontierRecord>,
) -> Result<ObjectFrontierRecord, String> {
    let response = publication
        .commit(&publication_command(
            seed,
            cycle.saturating_mul(10).saturating_add(3),
            PublicationAction::PrepareObjectFrontier {
                source_root: closure.root.clone(),
                manifest: closure.reference.clone(),
                covered_through,
                expected_active: prior_active.cloned(),
            },
        ))
        .await?;
    if response.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "G4.11a.1 cycle {cycle} frontier prepare was rejected: {:?}",
            response.status
        ));
    }
    response
        .state
        .pending_object_frontier
        .ok_or_else(|| format!("G4.11a.1 cycle {cycle} retained no pending frontier"))
}

fn publication_command(
    seed: u64,
    request_id: u64,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed.max(1).saturating_add(300_000),
            request_id,
        },
        credential: GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn authority_rows(view: &okv_transaction::TransactionAuthorityView) -> Vec<RowRecord> {
    view.values
        .iter()
        .map(|(key, value)| RowRecord::value(key, value.version, &value.value))
        .collect()
}

fn authority_values(
    view: &okv_transaction::TransactionAuthorityView,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    view.values
        .iter()
        .map(|(key, value)| (key.clone(), value.value.clone()))
        .collect()
}

async fn load_object_state(
    client: &ObjectClient,
    manifest: &RowObjectManifestV1,
    version: u64,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
    let mut cached = BTreeMap::new();
    for reference in &manifest.segments {
        let (index_bytes, _) = client
            .read_full_verified(
                &reference.index_key,
                None,
                reference.index_bytes,
                &reference.index_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_bytes)?;
        let (data_bytes, _) = client
            .read_full_verified(
                &reference.data_key,
                None,
                reference.data_bytes,
                &reference.data_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        cached.insert(reference.data_key.clone(), (index, data_bytes));
    }
    let mut values = BTreeMap::new();
    for (key, expected_value) in expected {
        let reference = manifest
            .locate(key)
            .ok_or_else(|| "G4.11a.1 object manifest omitted an expected key".to_owned())?;
        let (index, data) = cached
            .get(&reference.data_key)
            .ok_or_else(|| "G4.11a.1 object cache omitted a referenced segment".to_owned())?;
        match read_point_from_full_object(data, index, key, version)?.outcome {
            PointReadOutcome::Value(value) if value.as_ref() == expected_value.as_slice() => {
                values.insert(key.clone(), value.to_vec());
            }
            PointReadOutcome::Value(_) | PointReadOutcome::Tombstone | PointReadOutcome::Absent => {
                return Err("G4.11a.1 object state differed from authority state".to_owned());
            }
        }
    }
    Ok(values)
}

async fn read_all(
    client: &TransactionLogClient,
    after: u64,
    through: u64,
) -> Result<Vec<RetainedTransactionRecord>, String> {
    let mut after_version_exclusive = after;
    let mut after_batch_order_exclusive = None;
    let mut records = Vec::new();
    loop {
        let page = client
            .read(RetainedTransactionReadRequest {
                after_version_exclusive,
                after_batch_order_exclusive,
                through_version_inclusive: Some(through),
                max_records: 127,
            })
            .await?;
        records.extend(page.records);
        if page.complete {
            break;
        }
        after_version_exclusive = page.next_after_version;
        after_batch_order_exclusive = page.next_after_batch_order;
    }
    Ok(records)
}

fn replay_into(values: &mut BTreeMap<Vec<u8>, Vec<u8>>, records: &[RetainedTransactionRecord]) {
    for record in records {
        for mutation in &record.command.mutations {
            match mutation {
                Mutation::Set { key, value } => {
                    values.insert(key.clone(), value.clone());
                }
                Mutation::Clear { key } => {
                    values.remove(key);
                }
                Mutation::ClearRange { range } => {
                    let removed = values
                        .range(range.start.clone()..range.end.clone())
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in removed {
                        values.remove(&key);
                    }
                }
            }
        }
    }
}

fn response_for_identity(
    responses: &[TransactionBatchApplyResponse],
    identity: RequestIdentity,
) -> Option<TransactionApplyResponse> {
    responses
        .iter()
        .flat_map(|response| &response.items)
        .find(|item| item.identity == identity)
        .and_then(|item| item.transaction.clone())
}

fn count_committed(responses: &[TransactionBatchApplyResponse]) -> u64 {
    u64::try_from(
        responses
            .iter()
            .flat_map(|response| &response.items)
            .filter(|item| {
                item.transaction.as_ref().is_some_and(|response| {
                    matches!(response.status, TransactionStatus::Committed { .. })
                })
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn deterministic_value(seed: u64, index: u64, bytes: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(bytes);
    let mut state = seed ^ index.rotate_left(17);
    for _ in 0..bytes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        value.push((state >> 56) as u8);
    }
    value
}

fn percentile_99(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let rank = ordered.len().saturating_mul(99).div_ceil(100).max(1);
    ordered[rank - 1]
}

#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        f64::INFINITY
    } else {
        numerator as f64 / denominator as f64
    }
}

fn validate_profile(profile: &FrontieredProcessSnapshotProfile) -> Result<(), String> {
    if profile.frontier_cycles != 4
        || profile.transactions_per_cycle == 0
        || profile.transactions_per_batch == 0
        || profile.transactions_per_batch > 32
        || profile.transactions_per_cycle
            % u64::try_from(profile.transactions_per_batch).unwrap_or(u64::MAX)
            != 0
        || profile.live_keys == 0
        || profile.value_bytes == 0
        || profile.retry_window == 0
        || profile.retry_window >= profile.transactions_per_cycle
        || !profile.max_physical_amplification.is_finite()
        || profile.max_physical_amplification <= 0.0
        || !profile.max_snapshot_growth_ratio.is_finite()
        || profile.max_snapshot_growth_ratio < 1.0
    {
        return Err("invalid frontiered process-snapshot profile".to_owned());
    }
    Ok(())
}

fn semantic_sha<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_vec(value)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> FrontieredProcessSnapshotProfile {
        FrontieredProcessSnapshotProfile {
            frontier_cycles: 4,
            transactions_per_cycle: 256,
            transactions_per_batch: 32,
            live_keys: 256,
            value_bytes: 128,
            retry_window: 64,
            max_physical_amplification: 8.0,
            max_snapshot_growth_ratio: 1.25,
        }
    }

    #[test]
    fn frozen_workload_uses_one_stable_client_and_complete_live_keyset() {
        let profile = profile();
        let credential = GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        };
        let items = workload_items(5_601, 105_601, 0, 0, &profile, &credential);
        assert_eq!(items.len(), 256);
        assert_eq!(items.first().unwrap().identity.request_id, 1);
        assert_eq!(items.last().unwrap().identity.request_id, 256);
        assert!(items.iter().all(|item| item.identity.client_id == 105_601));
        assert_eq!(
            items
                .iter()
                .map(|item| item.command.write_conflicts[0].start.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            256
        );
    }

    #[test]
    fn profile_rejects_changed_cycle_and_retry_contracts() {
        let mut invalid = profile();
        invalid.frontier_cycles = 3;
        invalid.retry_window = invalid.transactions_per_cycle;
        assert!(validate_profile(&invalid).is_err());
    }
}
