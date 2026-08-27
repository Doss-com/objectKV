//! G4.10a independent-request commit-proxy batching contract.

use okv_consensus::{
    RequestIdentity, RetainedTransactionReadRequest, TransactionAuthorityProcessFixture,
    TransactionBatchItem, TransactionBatcher, TransactionBatcherConfig, TransactionBatcherStats,
    TransactionKeyRange, TransactionMutation, TransactionStatus,
};
use okv_transaction::{RetainedTransactionRecord, TransactionApplyResponse, TransactionCommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Subject selected for the G4.10a commit-proxy falsifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitProxyMode {
    SaturatedCandidate,
    AdmissionKneeControl,
    SparseArrivalControl,
    ByteBoundControl,
    OverloadControl,
    OversizedItemPoison,
}

impl CommitProxyMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::SaturatedCandidate => "saturated_candidate",
            Self::AdmissionKneeControl => "admission_knee_control",
            Self::SparseArrivalControl => "sparse_arrival_control",
            Self::ByteBoundControl => "byte_bound_control",
            Self::OverloadControl => "overload_control",
            Self::OversizedItemPoison => "oversized_item_poison",
        }
    }
}

/// Frozen G4.10a request and commit-proxy bounds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitProxyProfile {
    pub transaction_count: u64,
    pub value_bytes: usize,
    pub concurrent_clients: usize,
    pub admission_knee_clients: usize,
    pub max_batch_items: usize,
    pub max_entry_bytes: usize,
    pub max_batch_delay_micros: u64,
    pub queue_capacity: usize,
    pub sparse_transaction_count: u64,
    pub byte_control_transaction_count: u64,
    pub byte_control_value_bytes: usize,
    pub byte_control_max_entry_bytes: usize,
    pub overload_transaction_count: u64,
    pub overload_queue_capacity: usize,
}

/// Canonical report for one fresh three-process G4.10a execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CommitProxyReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: CommitProxyMode,
    pub authority_processes: u64,
    pub release_build: bool,
    pub attempted_count: u64,
    pub accepted_count: u64,
    pub resolved_count: u64,
    pub committed_count: u64,
    pub conflict_count: u64,
    pub backpressure_rejections: u64,
    pub oversized_rejections: u64,
    pub workload_seconds: f64,
    pub transactions_per_second: f64,
    pub commit_p99_seconds: f64,
    pub batcher: TransactionBatcherStats,
    pub mean_batch_items: f64,
    pub leader_logical_transactions_per_append: f64,
    pub versionstamps_unique: bool,
    pub batch_orders_contiguous: bool,
    pub retained_stream_complete: bool,
    pub replay_matches_authority: bool,
    pub exact_individual_retry: bool,
    pub leader_failover_exact: bool,
    pub restarted_voter_exact: bool,
    pub overload_was_explicit: bool,
    pub oversized_rejected_before_mutation: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

#[derive(Debug)]
struct RequestObservation {
    item: TransactionBatchItem,
    latency_seconds: f64,
    outcome: Result<TransactionApplyResponse, String>,
}

/// Run one independent-request commit-proxy subject against three real
/// `OpenRaft` processes and synchronized stable journals.
///
/// # Errors
///
/// Returns an error when the profile, process topology, batching path, or
/// recovery probes cannot complete.
pub fn run_commit_proxy_contract(
    seed: u64,
    mode: CommitProxyMode,
    profile: &CommitProxyProfile,
    executable: &Path,
) -> Result<CommitProxyReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run(seed, mode, profile, executable))
}

#[allow(clippy::too_many_lines)]
async fn run(
    seed: u64,
    mode: CommitProxyMode,
    profile: &CommitProxyProfile,
    executable: &Path,
) -> Result<CommitProxyReport, String> {
    let mut authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    let before = authority.voter_transaction_view(201).await?;
    let io_before = authority.io_stats().await?;
    let subject = subject_bounds(mode, profile);
    let batcher = TransactionBatcher::start(
        client.clone(),
        TransactionBatcherConfig {
            max_items: profile.max_batch_items,
            max_entry_bytes: subject.max_entry_bytes,
            max_delay: Duration::from_micros(profile.max_batch_delay_micros),
            queue_capacity: subject.queue_capacity,
        },
    )?;
    let items = workload_items(seed, subject.transaction_count, subject.value_bytes);
    let started = Instant::now();
    let observations = execute_requests(&batcher, items, subject.concurrent_requests).await?;
    let workload_seconds = started.elapsed().as_secs_f64();
    let batcher_stats = batcher.stats();
    drop(batcher);
    let io_after = authority.io_stats().await?;

    let accepted_count = batcher_stats.accepted_items;
    let resolved = observations
        .iter()
        .filter_map(|observation| observation.outcome.as_ref().ok())
        .collect::<Vec<_>>();
    let resolved_count = u64::try_from(resolved.len()).unwrap_or(u64::MAX);
    let committed_count = resolved
        .iter()
        .filter(|response| matches!(response.status, TransactionStatus::Committed { .. }))
        .count();
    let committed_count = u64::try_from(committed_count).unwrap_or(u64::MAX);
    let conflict_count = resolved
        .iter()
        .filter(|response| matches!(response.status, TransactionStatus::Conflict { .. }))
        .count();
    let conflict_count = u64::try_from(conflict_count).unwrap_or(u64::MAX);
    let backpressure_rejections = observations
        .iter()
        .filter(|observation| {
            observation
                .outcome
                .as_ref()
                .is_err_and(|error| error.contains("queue is full"))
        })
        .count();
    let backpressure_rejections = u64::try_from(backpressure_rejections).unwrap_or(u64::MAX);
    let oversized_rejections = observations
        .iter()
        .filter(|observation| {
            observation
                .outcome
                .as_ref()
                .is_err_and(|error| error.contains("bytes above"))
        })
        .count();
    let oversized_rejections = u64::try_from(oversized_rejections).unwrap_or(u64::MAX);
    let unexpected_errors = u64::try_from(
        observations
            .iter()
            .filter(|observation| {
                observation.outcome.as_ref().is_err_and(|error| {
                    !error.contains("queue is full") && !error.contains("bytes above")
                })
            })
            .count(),
    )
    .unwrap_or(u64::MAX);

    let mut versionstamps = resolved
        .iter()
        .filter_map(|response| match response.status {
            TransactionStatus::Committed { commit_version } => {
                Some((commit_version, response.batch_order))
            }
            TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => None,
        })
        .collect::<Vec<_>>();
    versionstamps.sort_unstable();
    let versionstamps_unique = versionstamps.windows(2).all(|window| window[0] < window[1]);
    let batch_orders_contiguous = contiguous_batch_orders(&versionstamps);
    let high_watermark = versionstamps
        .last()
        .map_or(0, |versionstamp| versionstamp.0);
    let retained = if high_watermark == 0 {
        Vec::new()
    } else {
        read_all(&client, high_watermark).await?
    };
    let retained_versionstamps = retained
        .iter()
        .map(|record| (record.commit_version, record.batch_order))
        .collect::<Vec<_>>();
    let retained_stream_complete = retained_versionstamps == versionstamps;
    let replay = replay_values(&retained);
    let view = authority.voter_transaction_view(201).await?;
    let authority_values = view
        .values
        .iter()
        .map(|(key, value)| (key.clone(), value.value.clone()))
        .collect::<BTreeMap<_, _>>();
    let replay_matches_authority = replay == authority_values;

    let exact_individual_retry = if let Some(observation) =
        observations.iter().find(|observation| {
            matches!(
                observation.outcome,
                Ok(TransactionApplyResponse {
                    status: TransactionStatus::Committed { .. },
                    ..
                })
            )
        }) {
        client
            .commit(observation.item.identity, &observation.item.command)
            .await?
            == *observation.outcome.as_ref().map_err(Clone::clone)?
    } else {
        mode == CommitProxyMode::OversizedItemPoison
    };

    let verify_recovery = matches!(
        mode,
        CommitProxyMode::SaturatedCandidate | CommitProxyMode::AdmissionKneeControl
    );
    let mut leader_failover_exact = !verify_recovery;
    let mut restarted_voter_exact = !verify_recovery;
    if verify_recovery {
        authority.kill_initial_leader_and_elect_successor().await?;
        leader_failover_exact = read_all(&client, high_watermark).await? == retained;
        authority.restart_initial_voter().await?;
        authority
            .wait_for_voter_version(201, high_watermark)
            .await?;
        let restarted = authority.voter_transaction_view(201).await?;
        restarted_voter_exact = restarted.current_version == high_watermark
            && restarted
                .values
                .iter()
                .map(|(key, value)| (key.clone(), value.value.clone()))
                .collect::<BTreeMap<_, _>>()
                == replay;
    }

    let after = authority.voter_transaction_view(201).await?;
    let overload_was_explicit = mode != CommitProxyMode::OverloadControl
        || (backpressure_rejections > 0
            && backpressure_rejections == batcher_stats.backpressure_rejections
            && accepted_count == resolved_count);
    let oversized_rejected_before_mutation = mode != CommitProxyMode::OversizedItemPoison
        || (oversized_rejections == 1
            && batcher_stats.oversized_rejections == 1
            && before == after);
    let expected_all_resolved = !matches!(
        mode,
        CommitProxyMode::OverloadControl | CommitProxyMode::OversizedItemPoison
    );
    let expected_resolved_count = if expected_all_resolved {
        subject.transaction_count
    } else {
        accepted_count
    };
    let entry_bytes_bounded = batcher_stats.max_observed_entry_bytes
        <= u64::try_from(subject.max_entry_bytes).unwrap_or(u64::MAX);
    let mode_contract = match mode {
        CommitProxyMode::SaturatedCandidate | CommitProxyMode::AdmissionKneeControl => {
            backpressure_rejections == 0
                && batcher_stats.max_observed_batch_items > 1
                && batcher_stats.batches > 0
        }
        CommitProxyMode::SparseArrivalControl => {
            batcher_stats.delay_bound_closures > 0 && batcher_stats.max_observed_batch_items == 1
        }
        CommitProxyMode::ByteBoundControl => batcher_stats.byte_bound_closures > 0,
        CommitProxyMode::OverloadControl => overload_was_explicit,
        CommitProxyMode::OversizedItemPoison => oversized_rejected_before_mutation,
    };
    let correctness_anomalies = unexpected_errors
        + u64::from(authority_processes != 3)
        + u64::from(resolved_count != expected_resolved_count)
        + u64::from(!versionstamps_unique)
        + u64::from(!batch_orders_contiguous)
        + u64::from(!retained_stream_complete)
        + u64::from(!replay_matches_authority)
        + u64::from(!exact_individual_retry)
        + u64::from(!leader_failover_exact)
        + u64::from(!restarted_voter_exact)
        + u64::from(!entry_bytes_bounded)
        + u64::from(!mode_contract);
    let latencies = observations
        .iter()
        .filter(|observation| observation.outcome.is_ok())
        .map(|observation| observation.latency_seconds)
        .collect::<Vec<_>>();
    let transactions_per_second = if workload_seconds == 0.0 {
        f64::MAX
    } else {
        count_as_f64(committed_count) / workload_seconds
    };
    let mean_batch_items = ratio(batcher_stats.resolved_items, batcher_stats.batches);
    let leader_append_calls = io_after
        .get(&201)
        .zip(io_before.get(&201))
        .map_or(0, |(after, before)| {
            after.append_calls.saturating_sub(before.append_calls)
        });
    let leader_logical_transactions_per_append = ratio(committed_count, leader_append_calls);
    let semantic_sha256 = semantic_sha(seed, mode, &observations, &replay, correctness_anomalies)?;
    Ok(CommitProxyReport {
        format_version: 1,
        seed,
        mode,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        attempted_count: subject.transaction_count,
        accepted_count,
        resolved_count,
        committed_count,
        conflict_count,
        backpressure_rejections,
        oversized_rejections,
        workload_seconds,
        transactions_per_second,
        commit_p99_seconds: percentile(&latencies, 99),
        batcher: batcher_stats,
        mean_batch_items,
        leader_logical_transactions_per_append,
        versionstamps_unique,
        batch_orders_contiguous,
        retained_stream_complete,
        replay_matches_authority,
        exact_individual_retry,
        leader_failover_exact,
        restarted_voter_exact,
        overload_was_explicit,
        oversized_rejected_before_mutation,
        correctness_anomalies,
        semantic_sha256,
    })
}

#[derive(Clone, Copy)]
struct SubjectBounds {
    transaction_count: u64,
    value_bytes: usize,
    concurrent_requests: usize,
    max_entry_bytes: usize,
    queue_capacity: usize,
}

fn subject_bounds(mode: CommitProxyMode, profile: &CommitProxyProfile) -> SubjectBounds {
    match mode {
        CommitProxyMode::SaturatedCandidate => SubjectBounds {
            transaction_count: profile.transaction_count,
            value_bytes: profile.value_bytes,
            concurrent_requests: profile.concurrent_clients,
            max_entry_bytes: profile.max_entry_bytes,
            queue_capacity: profile.queue_capacity,
        },
        CommitProxyMode::AdmissionKneeControl => SubjectBounds {
            transaction_count: profile.transaction_count,
            value_bytes: profile.value_bytes,
            concurrent_requests: profile.admission_knee_clients,
            max_entry_bytes: profile.max_entry_bytes,
            queue_capacity: profile.queue_capacity,
        },
        CommitProxyMode::SparseArrivalControl => SubjectBounds {
            transaction_count: profile.sparse_transaction_count,
            value_bytes: profile.value_bytes,
            concurrent_requests: 1,
            max_entry_bytes: profile.max_entry_bytes,
            queue_capacity: profile.queue_capacity,
        },
        CommitProxyMode::ByteBoundControl => SubjectBounds {
            transaction_count: profile.byte_control_transaction_count,
            value_bytes: profile.byte_control_value_bytes,
            concurrent_requests: profile.concurrent_clients,
            max_entry_bytes: profile.byte_control_max_entry_bytes,
            queue_capacity: profile.queue_capacity,
        },
        CommitProxyMode::OverloadControl => SubjectBounds {
            transaction_count: profile.overload_transaction_count,
            value_bytes: profile.value_bytes,
            concurrent_requests: usize::try_from(profile.overload_transaction_count)
                .unwrap_or(usize::MAX),
            max_entry_bytes: profile.max_entry_bytes,
            queue_capacity: profile.overload_queue_capacity,
        },
        CommitProxyMode::OversizedItemPoison => SubjectBounds {
            transaction_count: 1,
            value_bytes: profile.max_entry_bytes,
            concurrent_requests: 1,
            max_entry_bytes: profile.max_entry_bytes,
            queue_capacity: profile.queue_capacity,
        },
    }
}

async fn execute_requests(
    batcher: &TransactionBatcher,
    items: Vec<TransactionBatchItem>,
    concurrency: usize,
) -> Result<Vec<RequestObservation>, String> {
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();
    for item in items {
        let permit = Arc::clone(&permits);
        let batcher = batcher.clone();
        tasks.spawn(async move {
            let _permit = permit
                .acquire_owned()
                .await
                .map_err(|_| "commit-proxy concurrency gate closed".to_owned())?;
            let started = Instant::now();
            let outcome = batcher.commit(item.clone()).await;
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

fn workload_items(
    seed: u64,
    transaction_count: u64,
    value_bytes: usize,
) -> Vec<TransactionBatchItem> {
    (1..=transaction_count)
        .map(|request_id| {
            let key = format!("commit-proxy/{seed}/{request_id:08}").into_bytes();
            TransactionBatchItem {
                identity: RequestIdentity {
                    client_id: seed.max(1),
                    request_id,
                },
                credential: None,
                command: TransactionCommand {
                    read_version: 0,
                    read_conflicts: Vec::new(),
                    write_conflicts: vec![TransactionKeyRange::point(&key)],
                    mutations: vec![TransactionMutation::Set {
                        key,
                        value: deterministic_value(seed, request_id, value_bytes),
                    }],
                },
            }
        })
        .collect()
}

fn deterministic_value(seed: u64, request_id: u64, value_bytes: usize) -> Vec<u8> {
    let mut value = vec![0_u8; value_bytes];
    for (offset, byte) in value.iter_mut().enumerate() {
        let offset = u64::try_from(offset).unwrap_or(u64::MAX);
        *byte = seed
            .wrapping_mul(17)
            .wrapping_add(request_id.wrapping_mul(31))
            .wrapping_add(offset.wrapping_mul(13))
            .to_le_bytes()[0];
    }
    value
}

fn contiguous_batch_orders(versionstamps: &[(u64, u16)]) -> bool {
    let mut by_version = BTreeMap::<u64, Vec<u16>>::new();
    for (version, order) in versionstamps {
        by_version.entry(*version).or_default().push(*order);
    }
    by_version.values_mut().all(|orders| {
        orders.sort_unstable();
        orders
            .iter()
            .enumerate()
            .all(|(index, order)| *order == u16::try_from(index).unwrap_or(u16::MAX))
    })
}

async fn read_all(
    client: &okv_consensus::TransactionLogClient,
    high_watermark: u64,
) -> Result<Vec<RetainedTransactionRecord>, String> {
    let mut after_version_exclusive = 0;
    let mut after_batch_order_exclusive = None;
    let mut records = Vec::new();
    loop {
        let page = client
            .read(RetainedTransactionReadRequest {
                after_version_exclusive,
                after_batch_order_exclusive,
                through_version_inclusive: Some(high_watermark),
                max_records: 31,
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

fn replay_values(records: &[RetainedTransactionRecord]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut values = BTreeMap::new();
    for record in records {
        for mutation in &record.command.mutations {
            match mutation {
                TransactionMutation::Set { key, value } => {
                    values.insert(key.clone(), value.clone());
                }
                TransactionMutation::Clear { key } => {
                    values.remove(key);
                }
                TransactionMutation::ClearRange { range } => {
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
    values
}

fn semantic_sha(
    seed: u64,
    mode: CommitProxyMode,
    observations: &[RequestObservation],
    values: &BTreeMap<Vec<u8>, Vec<u8>>,
    anomalies: u64,
) -> Result<String, String> {
    let resolved_identities = observations
        .iter()
        .filter(|observation| observation.outcome.is_ok())
        .map(|observation| observation.item.identity)
        .collect::<BTreeSet<_>>();
    let ordered_values = values.iter().collect::<Vec<_>>();
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(seed, mode, resolved_identities, ordered_values, anomalies))
                .map_err(|error| error.to_string())?
        )
    ))
}

fn validate_profile(profile: &CommitProxyProfile) -> Result<(), String> {
    if profile.transaction_count == 0
        || profile.value_bytes == 0
        || profile.concurrent_clients == 0
        || profile.admission_knee_clients == 0
        || profile.max_batch_items == 0
        || profile.max_batch_items > 32
        || profile.max_entry_bytes == 0
        || profile.max_batch_delay_micros == 0
        || profile.queue_capacity < profile.max_batch_items
        || profile.sparse_transaction_count == 0
        || profile.byte_control_transaction_count == 0
        || profile.byte_control_value_bytes == 0
        || profile.byte_control_max_entry_bytes == 0
        || profile.overload_transaction_count == 0
        || profile.overload_queue_capacity < profile.max_batch_items
    {
        return Err("commit-proxy profile requires positive bounded fields, max batch items at most 32, and queues at least one complete batch".to_owned());
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

fn count_as_f64(value: u64) -> f64 {
    u32::try_from(value).map_or(f64::from(u32::MAX), f64::from)
}

#[cfg(test)]
mod tests {
    use super::{contiguous_batch_orders, validate_profile, CommitProxyProfile};

    fn profile() -> CommitProxyProfile {
        CommitProxyProfile {
            transaction_count: 1_024,
            value_bytes: 128,
            concurrent_clients: 64,
            admission_knee_clients: 32,
            max_batch_items: 16,
            max_entry_bytes: 262_144,
            max_batch_delay_micros: 2_000,
            queue_capacity: 2_048,
            sparse_transaction_count: 32,
            byte_control_transaction_count: 128,
            byte_control_value_bytes: 8_192,
            byte_control_max_entry_bytes: 131_072,
            overload_transaction_count: 512,
            overload_queue_capacity: 16,
        }
    }

    #[test]
    fn profile_is_bounded() {
        let profile = profile();
        assert!(validate_profile(&profile).is_ok());
        assert!(validate_profile(&CommitProxyProfile {
            queue_capacity: 8,
            ..profile
        })
        .is_err());
    }

    #[test]
    fn batch_orders_must_start_at_zero_without_gaps() {
        assert!(contiguous_batch_orders(&[(7, 0), (7, 1), (8, 0)]));
        assert!(!contiguous_batch_orders(&[(7, 0), (7, 2)]));
    }
}
