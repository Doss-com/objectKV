//! G4.10b concurrent commit-proxy and authenticated object-frontier contract.

use okv_consensus::{
    object_frontier_certificate_statement, GenerationClient, GenerationCredential,
    ObjectFrontierAdvance, ObjectFrontierCertificate, ObjectFrontierRecord, PublicationAction,
    PublicationAuthorityPosition, PublicationAuthorityProcessFixture, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity, RetainedTransactionReadRequest, TransactionAuthorityProcessFixture,
    TransactionBatchItem, TransactionBatcher, TransactionBatcherConfig, TransactionBatcherStats,
    TransactionLogClient, TransactionLogStorageStatsRequest,
};
use okv_object::{
    advance_validated_row_object_frontier, content_sha256, encode_row_object_set,
    filesystem_backend, read_point_from_full_object, validate_row_object_frontier, Backend,
    ObjectClient, ObservedBackend, PointReadOutcome, RowObjectManifestV1, RowObjectReference,
    RowRecord, RowSegmentIndex, WriteCondition,
};
use okv_transaction::{
    KeyRange, Mutation, RetainedTransactionRecord, TransactionApplyResponse, TransactionCommand,
    TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{Barrier, Semaphore};
use tokio::task::JoinSet;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";
const TARGET_OBJECT_BYTES: usize = 64 * 1024;
const TARGET_BLOCK_BYTES: usize = 8 * 1024;

/// Frozen G4.10b subject or negative-control behavior.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitProxyObjectFrontierMode {
    QuarterConflictCandidate,
    NoConflictControl,
    HighConflictControl,
    OneEntrySameDurabilityControl,
    MovingFrontierPoison,
    PrematurePopPoison,
}

impl CommitProxyObjectFrontierMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::QuarterConflictCandidate => "quarter_conflict_candidate",
            Self::NoConflictControl => "no_conflict_control",
            Self::HighConflictControl => "high_conflict_control",
            Self::OneEntrySameDurabilityControl => "one_entry_same_durability_control",
            Self::MovingFrontierPoison => "moving_frontier_poison",
            Self::PrematurePopPoison => "premature_pop_poison",
        }
    }

    const fn conflict_numerator(self) -> u64 {
        match self {
            Self::NoConflictControl => 0,
            Self::HighConflictControl => 3,
            Self::QuarterConflictCandidate
            | Self::OneEntrySameDurabilityControl
            | Self::MovingFrontierPoison
            | Self::PrematurePopPoison => 1,
        }
    }

    const fn conflict_denominator(self) -> u64 {
        let _ = self;
        4
    }

    const fn is_poison(self) -> bool {
        matches!(self, Self::MovingFrontierPoison | Self::PrematurePopPoison)
    }
}

/// Frozen G4.10b request, batching, conflict, and publication bounds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitProxyObjectFrontierProfile {
    pub prefix_transaction_count: u64,
    pub suffix_transaction_count: u64,
    pub value_bytes: usize,
    pub concurrent_clients: usize,
    pub max_batch_items: usize,
    pub max_entry_bytes: usize,
    pub max_batch_delay_micros: u64,
    pub queue_capacity: usize,
    pub hot_key_count: u64,
}

/// Canonical report for one fresh six-process G4.10b execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CommitProxyObjectFrontierReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: CommitProxyObjectFrontierMode,
    pub publication_authority_processes: u64,
    pub data_authority_processes: u64,
    pub release_build: bool,
    pub prefix_committed_count: u64,
    pub object_version: u64,
    pub suffix_attempted_count: u64,
    pub suffix_admitted_count: u64,
    pub suffix_resolved_count: u64,
    pub suffix_committed_count: u64,
    pub suffix_conflict_count: u64,
    pub suffix_backpressure_rejections: u64,
    pub suffix_workload_seconds: f64,
    pub resolved_outcomes_per_second: f64,
    pub commit_p99_seconds: f64,
    pub batcher: TransactionBatcherStats,
    pub leader_logical_transactions_per_append: f64,
    pub final_version: u64,
    pub conflict_oracle_exact: bool,
    pub versionstamps_unique: bool,
    pub batch_orders_contiguous: bool,
    pub foreground_object_requests: u64,
    pub pending_frontier_protected: bool,
    pub closure_validated: bool,
    pub closure_objects: u64,
    pub closure_bytes: u64,
    pub physical_pop_applied: bool,
    pub persisted_retention_floor: u64,
    pub retained_suffix_records: u64,
    pub retained_suffix_strictly_newer: bool,
    pub frontier_activation_accepted: bool,
    pub active_frontier_exact: bool,
    pub frontier_protocol_seconds: f64,
    pub suffix_resolutions_before_activation: u64,
    pub object_plus_suffix_reconstruction_exact: bool,
    pub committed_retry_exact: bool,
    pub conflicted_retry_exact: bool,
    pub data_leader_failover_exact: bool,
    pub publication_leader_failover_exact: bool,
    pub restarted_data_voter_exact: bool,
    pub fresh_controller_reconstruction_exact: bool,
    pub moving_frontier_poison_detected: bool,
    pub premature_pop_poison_detected: bool,
    pub poison_prefix_retained: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

#[derive(Debug)]
struct RequestObservation {
    item: TransactionBatchItem,
    latency_seconds: f64,
    outcome: Result<TransactionApplyResponse, String>,
}

#[derive(Clone, Debug)]
struct PublishedClosure {
    root: String,
    reference: PublicationObjectReference,
    manifest: RowObjectManifestV1,
}

/// Run one G4.10b subject against separate three-process publication and data
/// quorums plus one local immutable-object backend.
///
/// # Errors
///
/// Returns an error when profile validation, process startup, publication,
/// batching, or recovery cannot complete.
pub fn run_commit_proxy_object_frontier_contract(
    seed: u64,
    mode: CommitProxyObjectFrontierMode,
    profile: &CommitProxyObjectFrontierProfile,
    executable: &Path,
) -> Result<CommitProxyObjectFrontierReport, String> {
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
    mode: CommitProxyObjectFrontierMode,
    profile: &CommitProxyObjectFrontierProfile,
    executable: &Path,
) -> Result<CommitProxyObjectFrontierReport, String> {
    let object_root = TempDir::new().map_err(|error| error.to_string())?;
    let mut publication_authority =
        PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let publication_endpoints = publication_authority.endpoints();
    let publication = publication_authority.client()?;
    let generation = GenerationClient::new(publication_endpoints.clone())?;
    let mut data_authority = TransactionAuthorityProcessFixture::start_fenced(
        executable,
        seed.saturating_add(10_000),
        publication_authority.authority_nodes(),
    )
    .await?;
    let publication_authority_processes =
        u64::try_from(publication_authority.process_count()).unwrap_or(u64::MAX);
    let data_authority_processes =
        u64::try_from(data_authority.process_count()).unwrap_or(u64::MAX);
    let data_endpoints = data_authority.endpoints();
    let txlog = data_authority.client()?;
    let credential = GenerationCredential {
        generation: GENERATION,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };

    let inner_backend =
        filesystem_backend(object_root.path()).map_err(|error| error.to_string())?;
    let observed_backend = Arc::new(ObservedBackend::new(inner_backend));
    let backend: Arc<dyn Backend> = observed_backend.clone();
    let object_client = ObjectClient::new(backend.clone());

    let effective_batch_items =
        if mode == CommitProxyObjectFrontierMode::OneEntrySameDurabilityControl {
            1
        } else {
            profile.max_batch_items
        };
    let batcher_config = TransactionBatcherConfig {
        max_items: effective_batch_items,
        max_entry_bytes: profile.max_entry_bytes,
        max_delay: Duration::from_micros(profile.max_batch_delay_micros),
        queue_capacity: profile.queue_capacity,
    };

    let prefix_batcher = TransactionBatcher::start(txlog.clone(), batcher_config)?;
    let prefix_items = prefix_items(seed, profile, &credential);
    let prefix = execute_requests(
        &prefix_batcher,
        prefix_items,
        profile.concurrent_clients,
        None,
    )
    .await?;
    drop(prefix_batcher);
    let prefix_committed_count = count_status(&prefix, true);
    if prefix_committed_count != profile.prefix_transaction_count {
        return Err(format!(
            "G4.10b prefix committed {prefix_committed_count} of {} transactions",
            profile.prefix_transaction_count
        ));
    }
    let object_version = prefix
        .iter()
        .filter_map(committed_version)
        .max()
        .ok_or_else(|| "G4.10b prefix produced no commit version".to_owned())?;
    let prefix_records = read_all(&txlog, 0, object_version).await?;
    let object_state = replay_values(&prefix_records);
    let row_records = row_records(&prefix_records);
    let closure =
        publish_closure(seed, object_version, &row_records, &backend, &publication).await?;

    let normal_frontier = if mode.is_poison() {
        ObjectFrontierRecord {
            owner_generation: GENERATION,
            source_root: closure.root.clone(),
            manifest: closure.reference.clone(),
            covered_through: object_version,
            prepared_at: PublicationAuthorityPosition { term: 1, index: 1 },
        }
    } else {
        prepare_frontier(seed, object_version, &closure, &publication).await?
    };
    let pending_frontier_protected = if mode.is_poison() {
        false
    } else {
        publication
            .read()
            .await?
            .object_frontier_manifests()
            .contains(&&normal_frontier.manifest)
    };

    observed_backend.clear_stats();
    let io_before = data_authority.io_stats().await?;
    let suffix_batcher = TransactionBatcher::start(txlog.clone(), batcher_config)?;
    let suffix_items = suffix_items(seed, mode, profile, object_version, &credential);
    let barrier = Arc::new(Barrier::new(2));
    let resolved_during_protocol = Arc::new(AtomicU64::new(0));
    let suffix_task = {
        let barrier = Arc::clone(&barrier);
        let resolved = Arc::clone(&resolved_during_protocol);
        let batcher = suffix_batcher.clone();
        let concurrency = profile.concurrent_clients;
        tokio::spawn(async move {
            barrier.wait().await;
            execute_requests(&batcher, suffix_items, concurrency, Some(resolved)).await
        })
    };

    barrier.wait().await;
    let suffix_started = Instant::now();
    let frontier_started = Instant::now();
    let mut closure_validated = false;
    let mut closure_objects = 0;
    let mut closure_bytes = 0;
    let mut physical_pop_applied = false;
    let mut frontier_activation_accepted = false;
    let mut active_frontier_exact = false;
    let mut moving_frontier_poison_detected = false;
    let mut premature_pop_poison_detected = false;
    let mut applied_frontier = None;

    match mode {
        CommitProxyObjectFrontierMode::MovingFrontierPoison => {
            wait_for_resolution(&resolved_during_protocol).await?;
            let moving_version = data_authority
                .voter_transaction_view(201)
                .await?
                .current_version;
            if moving_version <= object_version {
                return Err("moving-frontier poison did not observe a suffix commit".to_owned());
            }
            let moving = prepare_frontier(seed, moving_version, &closure, &publication).await?;
            let before = txlog
                .storage_stats(TransactionLogStorageStatsRequest::default())
                .await?;
            moving_frontier_poison_detected = validate_row_object_frontier(&object_client, &moving)
                .await
                .is_err();
            let after = txlog
                .storage_stats(TransactionLogStorageStatsRequest::default())
                .await?;
            moving_frontier_poison_detected &= before.retention_floor == after.retention_floor;
        }
        CommitProxyObjectFrontierMode::PrematurePopPoison => {
            let validated = validate_row_object_frontier(&object_client, &normal_frontier).await?;
            closure_validated = true;
            closure_objects = validated.closure_objects();
            closure_bytes = validated.closure_bytes();
            wait_for_resolution(&resolved_during_protocol).await?;
            let before = txlog
                .storage_stats(TransactionLogStorageStatsRequest::default())
                .await?;
            premature_pop_poison_detected = txlog
                .advance_object_frontier_once(
                    RequestIdentity {
                        client_id: seed.max(1).saturating_add(400_000),
                        request_id: 1,
                    },
                    &credential,
                    &ObjectFrontierAdvance {
                        frontier: normal_frontier.clone(),
                    },
                )
                .await
                .is_err();
            let after = txlog
                .storage_stats(TransactionLogStorageStatsRequest::default())
                .await?;
            premature_pop_poison_detected &= before.retention_floor == after.retention_floor;
        }
        CommitProxyObjectFrontierMode::QuarterConflictCandidate
        | CommitProxyObjectFrontierMode::NoConflictControl
        | CommitProxyObjectFrontierMode::HighConflictControl
        | CommitProxyObjectFrontierMode::OneEntrySameDurabilityControl => {
            let validated = validate_row_object_frontier(&object_client, &normal_frontier).await?;
            closure_validated = true;
            closure_objects = validated.closure_objects();
            closure_bytes = validated.closure_bytes();
            wait_for_resolution(&resolved_during_protocol).await?;
            let popped = advance_validated_row_object_frontier(
                &txlog,
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(400_000),
                    request_id: 1,
                },
                &credential,
                &validated,
            )
            .await?;
            physical_pop_applied = true;
            let generation_state = generation.read().await?;
            let statement = object_frontier_certificate_statement(
                &generation_state,
                normal_frontier.clone(),
                popped.applied_log_position,
            );
            let certificate = ObjectFrontierCertificate {
                statement: statement.clone(),
                attestations: txlog.attest_object_frontier(&statement).await?,
            };
            let activation = publication
                .commit(&publication_command(
                    seed,
                    300_003,
                    PublicationAction::ActivateObjectFrontier {
                        expected_pending: normal_frontier.clone(),
                        certificate,
                    },
                ))
                .await?;
            frontier_activation_accepted = activation.status == PublicationCommandStatus::Accepted;
            active_frontier_exact =
                activation.state.active_object_frontier.as_ref() == Some(&normal_frontier);
            applied_frontier = Some(normal_frontier.clone());
        }
    }
    let suffix_resolutions_before_activation = resolved_during_protocol.load(Ordering::Relaxed);
    let frontier_protocol_seconds = frontier_started.elapsed().as_secs_f64();
    let suffix = suffix_task.await.map_err(|error| error.to_string())??;
    let suffix_workload_seconds = suffix_started.elapsed().as_secs_f64();
    let batcher = suffix_batcher.stats();
    drop(suffix_batcher);
    let io_after = data_authority.io_stats().await?;

    let suffix_resolved_count = count_resolved(&suffix);
    let suffix_committed_count = count_status(&suffix, true);
    let suffix_conflict_count = count_status(&suffix, false);
    let suffix_backpressure_rejections = count_errors(&suffix, "queue is full");
    let conflict_oracle_exact = conflict_oracle_exact(&suffix, object_version);
    let (versionstamps_unique, batch_orders_contiguous) = ordered_outcomes_exact(&suffix);
    let latencies = suffix
        .iter()
        .filter(|observation| observation.outcome.is_ok())
        .map(|observation| observation.latency_seconds)
        .collect::<Vec<_>>();
    let resolved_outcomes_per_second =
        ratio_seconds(suffix_resolved_count, suffix_workload_seconds);
    let leader_append_calls = io_after
        .get(&201)
        .zip(io_before.get(&201))
        .map_or(0, |(after, before)| {
            after.append_calls.saturating_sub(before.append_calls)
        });
    let leader_logical_transactions_per_append = ratio(suffix_resolved_count, leader_append_calls);

    let final_view = data_authority.voter_transaction_view(201).await?;
    let final_version = final_view.current_version;
    let retained_suffix = read_all(&txlog, object_version, final_version).await?;
    let storage = txlog
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    let poison_prefix_retained = if mode.is_poison() {
        read_all(&txlog, 0, object_version).await? == prefix_records
    } else {
        true
    };
    let retained_suffix_strictly_newer = retained_suffix
        .iter()
        .all(|record| record.commit_version > object_version);
    let mut reconstructed = object_state.clone();
    replay_into(&mut reconstructed, &retained_suffix);
    let final_authority_values = authority_values(&final_view);
    let object_plus_suffix_reconstruction_exact = reconstructed == final_authority_values;
    let committed_retry_exact = retry_exact(&txlog, &credential, &suffix, true).await?;
    let conflicted_retry_exact = if suffix_conflict_count == 0 {
        mode == CommitProxyObjectFrontierMode::NoConflictControl
    } else {
        retry_exact(&txlog, &credential, &suffix, false).await?
    };

    let positive = !mode.is_poison();
    let mut data_leader_failover_exact = !positive;
    let mut publication_leader_failover_exact = !positive;
    let mut restarted_data_voter_exact = !positive;
    let mut fresh_controller_reconstruction_exact = !positive;
    if positive {
        data_authority
            .kill_initial_leader_and_elect_successor()
            .await?;
        data_leader_failover_exact =
            read_all(&txlog, object_version, final_version).await? == retained_suffix;
        data_authority.restart_initial_voter().await?;
        data_authority
            .wait_for_voter_version(201, final_version)
            .await?;
        restarted_data_voter_exact =
            authority_values(&data_authority.voter_transaction_view(201).await?)
                == final_authority_values;
        publication_authority
            .kill_initial_leader_and_elect_successor()
            .await?;
        publication_leader_failover_exact = publication
            .read()
            .await
            .is_ok_and(|state| state.active_object_frontier.as_ref() == applied_frontier.as_ref());

        let fresh_txlog = TransactionLogClient::new(data_endpoints)?;
        let fresh_suffix = read_all(&fresh_txlog, object_version, final_version).await?;
        let fresh_client = ObjectClient::new(backend);
        let fresh_base = load_object_state(
            &fresh_client,
            &closure.manifest,
            object_version,
            &object_state,
        )
        .await?;
        let mut fresh_reconstructed = fresh_base;
        replay_into(&mut fresh_reconstructed, &fresh_suffix);
        let fresh_publication = okv_consensus::PublicationClient::new(publication_endpoints)?;
        fresh_controller_reconstruction_exact = fresh_reconstructed == final_authority_values
            && fresh_publication.read().await.is_ok_and(|state| {
                state.active_object_frontier.as_ref() == applied_frontier.as_ref()
            });
    }

    let expected_conflicts = expected_conflict_count(mode, profile);
    let common_anomalies = u64::from(publication_authority_processes != 3)
        + u64::from(data_authority_processes != 3)
        + u64::from(prefix_committed_count != profile.prefix_transaction_count)
        + u64::from(suffix_resolved_count != profile.suffix_transaction_count)
        + u64::from(suffix_committed_count + suffix_conflict_count != suffix_resolved_count)
        + u64::from(suffix_conflict_count != expected_conflicts)
        + u64::from(suffix_backpressure_rejections != 0)
        + u64::from(!conflict_oracle_exact)
        + u64::from(!versionstamps_unique)
        + u64::from(!batch_orders_contiguous)
        + u64::from(!object_plus_suffix_reconstruction_exact)
        + u64::from(!committed_retry_exact)
        + u64::from(!conflicted_retry_exact)
        + u64::from(suffix_resolutions_before_activation == 0);
    let mode_anomalies = if positive {
        u64::from(!pending_frontier_protected)
            + u64::from(!closure_validated)
            + u64::from(!physical_pop_applied)
            + u64::from(storage.retention_floor != object_version)
            + u64::from(!retained_suffix_strictly_newer)
            + u64::from(!frontier_activation_accepted)
            + u64::from(!active_frontier_exact)
            + u64::from(!data_leader_failover_exact)
            + u64::from(!publication_leader_failover_exact)
            + u64::from(!restarted_data_voter_exact)
            + u64::from(!fresh_controller_reconstruction_exact)
    } else {
        u64::from(storage.retention_floor != 0)
            + u64::from(!poison_prefix_retained)
            + match mode {
                CommitProxyObjectFrontierMode::MovingFrontierPoison => {
                    u64::from(!moving_frontier_poison_detected)
                }
                CommitProxyObjectFrontierMode::PrematurePopPoison => {
                    u64::from(!premature_pop_poison_detected)
                }
                _ => 0,
            }
    };
    let correctness_anomalies = common_anomalies + mode_anomalies;
    let semantic_sha256 = semantic_sha(
        seed,
        mode,
        object_version,
        final_version,
        &suffix,
        &final_authority_values,
        correctness_anomalies,
    )?;

    Ok(CommitProxyObjectFrontierReport {
        format_version: 1,
        seed,
        mode,
        publication_authority_processes,
        data_authority_processes,
        release_build: !cfg!(debug_assertions),
        prefix_committed_count,
        object_version,
        suffix_attempted_count: profile.suffix_transaction_count,
        suffix_admitted_count: batcher.accepted_items,
        suffix_resolved_count,
        suffix_committed_count,
        suffix_conflict_count,
        suffix_backpressure_rejections,
        suffix_workload_seconds,
        resolved_outcomes_per_second,
        commit_p99_seconds: percentile(&latencies, 99),
        batcher,
        leader_logical_transactions_per_append,
        final_version,
        conflict_oracle_exact,
        versionstamps_unique,
        batch_orders_contiguous,
        foreground_object_requests: 0,
        pending_frontier_protected,
        closure_validated,
        closure_objects,
        closure_bytes,
        physical_pop_applied,
        persisted_retention_floor: storage.retention_floor,
        retained_suffix_records: storage.retained_records,
        retained_suffix_strictly_newer,
        frontier_activation_accepted,
        active_frontier_exact,
        frontier_protocol_seconds,
        suffix_resolutions_before_activation,
        object_plus_suffix_reconstruction_exact,
        committed_retry_exact,
        conflicted_retry_exact,
        data_leader_failover_exact,
        publication_leader_failover_exact,
        restarted_data_voter_exact,
        fresh_controller_reconstruction_exact,
        moving_frontier_poison_detected,
        premature_pop_poison_detected,
        poison_prefix_retained,
        correctness_anomalies,
        semantic_sha256,
    })
}

fn prefix_items(
    seed: u64,
    profile: &CommitProxyObjectFrontierProfile,
    credential: &GenerationCredential,
) -> Vec<TransactionBatchItem> {
    (1..=profile.prefix_transaction_count)
        .map(|ordinal| {
            let key = format!("g410b/base/{seed}/{ordinal:08}").into_bytes();
            transaction_item(
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(100_000),
                    request_id: ordinal,
                },
                credential,
                0,
                key,
                Vec::new(),
                deterministic_value(seed, ordinal, profile.value_bytes),
            )
        })
        .collect()
}

fn suffix_items(
    seed: u64,
    mode: CommitProxyObjectFrontierMode,
    profile: &CommitProxyObjectFrontierProfile,
    read_version: u64,
    credential: &GenerationCredential,
) -> Vec<TransactionBatchItem> {
    let conflict_count = profile
        .suffix_transaction_count
        .saturating_mul(mode.conflict_numerator())
        / mode.conflict_denominator();
    (1..=profile.suffix_transaction_count)
        .map(|ordinal| {
            let conflict = ordinal <= conflict_count;
            let key = if conflict {
                let hot = ordinal.saturating_sub(1) % profile.hot_key_count;
                format!("g410b/hot/{seed}/{hot:08}").into_bytes()
            } else {
                format!("g410b/unique/{seed}/{ordinal:08}").into_bytes()
            };
            let read_conflicts = conflict
                .then(|| KeyRange::point(&key))
                .into_iter()
                .collect();
            transaction_item(
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(200_000),
                    request_id: ordinal,
                },
                credential,
                read_version,
                key,
                read_conflicts,
                deterministic_value(seed.saturating_add(1), ordinal, profile.value_bytes),
            )
        })
        .collect()
}

fn transaction_item(
    identity: RequestIdentity,
    credential: &GenerationCredential,
    read_version: u64,
    key: Vec<u8>,
    read_conflicts: Vec<KeyRange>,
    value: Vec<u8>,
) -> TransactionBatchItem {
    TransactionBatchItem {
        identity,
        credential: Some(credential.clone()),
        command: TransactionCommand {
            read_version,
            read_conflicts,
            write_conflicts: vec![KeyRange::point(&key)],
            mutations: vec![Mutation::Set { key, value }],
        },
    }
}

async fn execute_requests(
    batcher: &TransactionBatcher,
    items: Vec<TransactionBatchItem>,
    concurrency: usize,
    resolved_counter: Option<Arc<AtomicU64>>,
) -> Result<Vec<RequestObservation>, String> {
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    for item in items {
        let permit = Arc::clone(&permits);
        let batcher = batcher.clone();
        let counter = resolved_counter.clone();
        tasks.spawn(async move {
            let _permit = permit
                .acquire_owned()
                .await
                .map_err(|_| "G4.10b concurrency gate closed".to_owned())?;
            let started = Instant::now();
            let outcome = batcher.commit(item.clone()).await;
            if outcome.is_ok() {
                if let Some(counter) = counter {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok::<_, String>(RequestObservation {
                item,
                latency_seconds: started.elapsed().as_secs_f64(),
                outcome,
            })
        });
    }
    let mut observations = Vec::with_capacity(tasks.len());
    while let Some(joined) = tasks.join_next().await {
        observations.push(joined.map_err(|error| error.to_string())??);
    }
    observations.sort_by_key(|observation| observation.item.identity);
    Ok(observations)
}

async fn wait_for_resolution(counter: &AtomicU64) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(1), async {
        while counter.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "G4.10b frontier protocol observed no concurrent suffix resolution".to_owned())
}

async fn publish_closure(
    seed: u64,
    covered_through: u64,
    records: &[RowRecord],
    backend: &Arc<dyn Backend>,
    publication: &okv_consensus::PublicationClient,
) -> Result<PublishedClosure, String> {
    let encoded =
        encode_row_object_set(GENERATION, records, TARGET_OBJECT_BYTES, TARGET_BLOCK_BYTES)?;
    let prefix = format!("rows-g410b/{seed}");
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
    let root = format!("object-frontier/g410b/{seed}");
    let publication_id = format!("object-frontier-publication/g410b/{seed}");
    let mut object_keys = BTreeSet::from([reference.key.clone()]);
    for child in &references {
        object_keys.insert(child.data_key.clone());
        object_keys.insert(child.index_key.clone());
    }
    let prepared = publication
        .commit(&publication_command(
            seed,
            300_000,
            PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent: PublicationIntent {
                    object_keys,
                    manifest: reference.clone(),
                    destination_root: root.clone(),
                    expected_prior_root: None,
                },
            },
        ))
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err("G4.10b publication prepare was rejected".to_owned());
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
            300_001,
            PublicationAction::Publish {
                publication_id,
                destination_root: root.clone(),
                expected_prior_root: None,
                manifest: reference.clone(),
            },
        ))
        .await?;
    if published.status != PublicationCommandStatus::Accepted
        || published.state.roots.get(&root) != Some(&reference)
    {
        return Err("G4.10b publication root was not installed exactly".to_owned());
    }
    Ok(PublishedClosure {
        root,
        reference,
        manifest,
    })
}

async fn prepare_frontier(
    seed: u64,
    covered_through: u64,
    closure: &PublishedClosure,
    publication: &okv_consensus::PublicationClient,
) -> Result<ObjectFrontierRecord, String> {
    let response = publication
        .commit(&publication_command(
            seed,
            300_002,
            PublicationAction::PrepareObjectFrontier {
                source_root: closure.root.clone(),
                manifest: closure.reference.clone(),
                covered_through,
                expected_active: None,
            },
        ))
        .await?;
    if response.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "G4.10b frontier prepare was rejected: {:?}",
            response.status
        ));
    }
    response
        .state
        .pending_object_frontier
        .ok_or_else(|| "G4.10b accepted frontier prepare retained no pending root".to_owned())
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

fn row_records(records: &[RetainedTransactionRecord]) -> Vec<RowRecord> {
    let mut rows = records
        .iter()
        .flat_map(|record| {
            record
                .command
                .mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    Mutation::Set { key, value } => {
                        Some(RowRecord::value(key, record.commit_version, value))
                    }
                    Mutation::Clear { key } => {
                        Some(RowRecord::tombstone(key, record.commit_version))
                    }
                    Mutation::ClearRange { .. } => None,
                })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| right.version.cmp(&left.version))
    });
    rows
}

fn replay_values(records: &[RetainedTransactionRecord]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut values = BTreeMap::new();
    replay_into(&mut values, records);
    values
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
            .ok_or_else(|| "G4.10b object manifest omitted an expected key".to_owned())?;
        let (index, data) = cached
            .get(&reference.data_key)
            .ok_or_else(|| "G4.10b object cache omitted a referenced segment".to_owned())?;
        match read_point_from_full_object(data, index, key, version)?.outcome {
            PointReadOutcome::Value(value) if value.as_ref() == expected_value.as_slice() => {
                values.insert(key.clone(), value.to_vec());
            }
            PointReadOutcome::Value(_) | PointReadOutcome::Tombstone | PointReadOutcome::Absent => {
                return Err("G4.10b object state differed from the frozen prefix".to_owned());
            }
        }
    }
    Ok(values)
}

fn conflict_oracle_exact(observations: &[RequestObservation], read_version: u64) -> bool {
    let mut ordered = observations
        .iter()
        .filter_map(|observation| {
            observation.outcome.as_ref().ok().map(|response| {
                (
                    response.applied_log_index,
                    response.batch_order,
                    observation,
                )
            })
        })
        .collect::<Vec<_>>();
    ordered.sort_by_key(|(version, batch_order, _)| (*version, *batch_order));
    let mut first_writes = BTreeMap::<Vec<u8>, u64>::new();
    ordered.into_iter().all(|(_, _, observation)| {
        let response = observation.outcome.as_ref().expect("filtered response");
        let Some(range) = observation.item.command.read_conflicts.first() else {
            return matches!(response.status, TransactionStatus::Committed { .. });
        };
        if observation.item.command.read_version != read_version {
            return false;
        }
        let key = range.start.clone();
        if let Some(conflicting_version) = first_writes.get(&key) {
            matches!(
                response.status,
                TransactionStatus::Conflict {
                    conflicting_version: actual
                } if actual == *conflicting_version
            )
        } else if let TransactionStatus::Committed { commit_version } = response.status {
            first_writes.insert(key, commit_version);
            true
        } else {
            false
        }
    })
}

fn ordered_outcomes_exact(observations: &[RequestObservation]) -> (bool, bool) {
    let mut versionstamps = observations
        .iter()
        .filter_map(|observation| {
            observation
                .outcome
                .as_ref()
                .ok()
                .map(|response| (response.applied_log_index, response.batch_order))
        })
        .collect::<Vec<_>>();
    versionstamps.sort_unstable();
    let unique = versionstamps.windows(2).all(|window| window[0] < window[1]);
    let mut by_version = BTreeMap::<u64, Vec<u16>>::new();
    for (version, order) in versionstamps {
        by_version.entry(version).or_default().push(order);
    }
    let contiguous = by_version.values_mut().all(|orders| {
        orders.sort_unstable();
        orders
            .iter()
            .enumerate()
            .all(|(index, order)| *order == u16::try_from(index).unwrap_or(u16::MAX))
    });
    (unique, contiguous)
}

async fn retry_exact(
    txlog: &TransactionLogClient,
    credential: &GenerationCredential,
    observations: &[RequestObservation],
    committed: bool,
) -> Result<bool, String> {
    let Some(observation) = observations.iter().find(|observation| {
        observation.outcome.as_ref().is_ok_and(|response| {
            matches!(response.status, TransactionStatus::Committed { .. }) == committed
                && matches!(
                    response.status,
                    TransactionStatus::Committed { .. } | TransactionStatus::Conflict { .. }
                )
        })
    }) else {
        return Ok(false);
    };
    let retried = txlog
        .commit_fenced(
            observation.item.identity,
            credential,
            &observation.item.command,
        )
        .await?;
    Ok(Some(&retried) == observation.outcome.as_ref().ok())
}

fn authority_values(
    view: &okv_transaction::TransactionAuthorityView,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    view.values
        .iter()
        .map(|(key, value)| (key.clone(), value.value.clone()))
        .collect()
}

fn expected_conflict_count(
    mode: CommitProxyObjectFrontierMode,
    profile: &CommitProxyObjectFrontierProfile,
) -> u64 {
    let attempts = profile
        .suffix_transaction_count
        .saturating_mul(mode.conflict_numerator())
        / mode.conflict_denominator();
    attempts.saturating_sub(attempts.min(profile.hot_key_count))
}

fn count_resolved(observations: &[RequestObservation]) -> u64 {
    u64::try_from(
        observations
            .iter()
            .filter(|observation| observation.outcome.is_ok())
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn count_status(observations: &[RequestObservation], committed: bool) -> u64 {
    u64::try_from(
        observations
            .iter()
            .filter(|observation| {
                observation.outcome.as_ref().is_ok_and(|response| {
                    matches!(response.status, TransactionStatus::Committed { .. }) == committed
                        && matches!(
                            response.status,
                            TransactionStatus::Committed { .. }
                                | TransactionStatus::Conflict { .. }
                        )
                })
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn count_errors(observations: &[RequestObservation], pattern: &str) -> u64 {
    u64::try_from(
        observations
            .iter()
            .filter(|observation| {
                observation
                    .outcome
                    .as_ref()
                    .is_err_and(|error| error.contains(pattern))
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn committed_version(observation: &RequestObservation) -> Option<u64> {
    observation
        .outcome
        .as_ref()
        .ok()
        .and_then(|response| match response.status {
            TransactionStatus::Committed { commit_version } => Some(commit_version),
            TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => None,
        })
}

fn deterministic_value(seed: u64, ordinal: u64, value_bytes: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(value_bytes);
    let mut block = 0_u64;
    while value.len() < value_bytes {
        let mut digest = Sha256::new();
        digest.update(b"OKV-G410B-VALUE-V1\0");
        digest.update(seed.to_be_bytes());
        digest.update(ordinal.to_be_bytes());
        digest.update(block.to_be_bytes());
        value.extend_from_slice(&digest.finalize());
        block = block.saturating_add(1);
    }
    value.truncate(value_bytes);
    value
}

fn semantic_sha(
    seed: u64,
    mode: CommitProxyObjectFrontierMode,
    object_version: u64,
    final_version: u64,
    suffix: &[RequestObservation],
    values: &BTreeMap<Vec<u8>, Vec<u8>>,
    anomalies: u64,
) -> Result<String, String> {
    let outcomes = suffix
        .iter()
        .filter_map(|observation| {
            observation.outcome.as_ref().ok().map(|response| {
                (
                    response.applied_log_index,
                    response.batch_order,
                    observation.item.identity,
                    response.status.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    let ordered_values = values.iter().collect::<Vec<_>>();
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                seed,
                mode,
                object_version,
                final_version,
                outcomes,
                ordered_values,
                anomalies,
            ))
            .map_err(|error| error.to_string())?
        )
    ))
}

fn validate_profile(profile: &CommitProxyObjectFrontierProfile) -> Result<(), String> {
    if profile.prefix_transaction_count == 0
        || profile.suffix_transaction_count == 0
        || profile.value_bytes == 0
        || profile.concurrent_clients == 0
        || profile.max_batch_items == 0
        || profile.max_batch_items > 32
        || profile.max_entry_bytes == 0
        || profile.max_batch_delay_micros == 0
        || profile.queue_capacity < profile.max_batch_items
        || profile.hot_key_count == 0
        || profile.hot_key_count > profile.suffix_transaction_count / 4
    {
        return Err("G4.10b profile requires positive bounded fields, at most 32 items per batch, one complete queued batch, and a bounded hot-key set".to_owned());
    }
    Ok(())
}

fn percentile(samples: &[f64], percentile: usize) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let index = (ordered.len().saturating_sub(1) * percentile) / 100;
    ordered[index]
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    count_as_f64(numerator) / count_as_f64(denominator)
}

fn ratio_seconds(numerator: u64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        return f64::MAX;
    }
    count_as_f64(numerator) / denominator
}

fn count_as_f64(value: u64) -> f64 {
    u32::try_from(value).map_or(f64::from(u32::MAX), f64::from)
}

#[cfg(test)]
mod tests {
    use super::{
        expected_conflict_count, validate_profile, CommitProxyObjectFrontierMode,
        CommitProxyObjectFrontierProfile,
    };

    fn profile() -> CommitProxyObjectFrontierProfile {
        CommitProxyObjectFrontierProfile {
            prefix_transaction_count: 512,
            suffix_transaction_count: 1_024,
            value_bytes: 128,
            concurrent_clients: 64,
            max_batch_items: 32,
            max_entry_bytes: 262_144,
            max_batch_delay_micros: 2_000,
            queue_capacity: 2_048,
            hot_key_count: 64,
        }
    }

    #[test]
    fn frozen_profile_is_bounded() {
        let profile = profile();
        assert!(validate_profile(&profile).is_ok());
        assert!(validate_profile(&CommitProxyObjectFrontierProfile {
            queue_capacity: 16,
            ..profile
        })
        .is_err());
    }

    #[test]
    fn conflict_controls_have_exact_expected_counts() {
        let profile = profile();
        assert_eq!(
            expected_conflict_count(
                CommitProxyObjectFrontierMode::QuarterConflictCandidate,
                &profile
            ),
            192
        );
        assert_eq!(
            expected_conflict_count(CommitProxyObjectFrontierMode::NoConflictControl, &profile),
            0
        );
        assert_eq!(
            expected_conflict_count(CommitProxyObjectFrontierMode::HighConflictControl, &profile),
            704
        );
    }
}
