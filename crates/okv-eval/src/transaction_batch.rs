//! G4.9 explicit transaction-batch entry contract.

use okv_consensus::{
    OpenRaftIoStats, RequestIdentity, RetainedTransactionReadRequest, TransactionApplyResponse,
    TransactionAuthorityProcessFixture, TransactionBatchApplyResponse, TransactionBatchItem,
    TransactionCommand, TransactionKeyRange, TransactionMutation, TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

/// Subject selected for the G4.9 transaction-batch falsifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionBatchMode {
    Candidate,
    DuplicateIdentityControl,
    EarlyAckPoison,
}

impl TransactionBatchMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::DuplicateIdentityControl => "duplicate_identity_control",
            Self::EarlyAckPoison => "early_ack_poison",
        }
    }
}

/// Frozen G4.9 workload bounds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatchProfile {
    pub live_keys: u64,
    pub value_bytes: usize,
    pub transaction_count: u64,
    pub transactions_per_batch: usize,
}

/// Workload-local stable-log delta for one voter.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct TransactionBatchNodeIoReport {
    pub node_id: u64,
    pub append_calls: u64,
    pub appended_entries: u64,
    pub entries_per_append: f64,
    pub logical_transactions_per_append: f64,
    pub append_durable_seconds: f64,
    pub committed_calls: u64,
    pub committed_durable_seconds: f64,
    pub physical_journal_bytes: u64,
}

/// Canonical report for one fresh three-process G4.9 candidate execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TransactionBatchReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: TransactionBatchMode,
    pub authority_processes: u64,
    pub release_build: bool,
    pub transaction_count: u64,
    pub batch_count: u64,
    pub transactions_per_batch: u64,
    pub committed_count: u64,
    pub workload_seconds: f64,
    pub transactions_per_second: f64,
    pub commit_p99_seconds: f64,
    pub versionstamps_unique_and_increasing: bool,
    pub shared_versions_and_contiguous_orders: bool,
    pub retained_stream_complete: bool,
    pub exact_final_values: bool,
    pub exact_individual_retry: bool,
    pub exact_batch_retry: bool,
    pub in_batch_conflict_detected: bool,
    pub duplicate_identity_rejected_before_mutation: bool,
    pub leader_failover_exact: bool,
    pub restarted_voter_exact: bool,
    pub early_ack_observed: bool,
    pub early_ack_missing_after_quorum_recovery: bool,
    pub node_io: Vec<TransactionBatchNodeIoReport>,
    pub leader_logical_transactions_per_append: f64,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

/// Run the explicit transaction-batch candidate against three real `OpenRaft`
/// processes and stable journals.
///
/// # Errors
///
/// Returns an error when the profile, process topology, commit path, recovery
/// path, or semantic probes cannot complete.
pub fn run_transaction_batch_contract(
    seed: u64,
    mode: TransactionBatchMode,
    profile: &TransactionBatchProfile,
    executable: &Path,
) -> Result<TransactionBatchReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        match mode {
            TransactionBatchMode::Candidate => run_candidate(seed, profile, executable).await,
            TransactionBatchMode::DuplicateIdentityControl => {
                run_duplicate_identity_control(seed, profile, executable).await
            }
            TransactionBatchMode::EarlyAckPoison => {
                run_early_ack_poison(seed, profile, executable).await
            }
        }
    })
}

#[allow(clippy::too_many_lines)]
async fn run_candidate(
    seed: u64,
    profile: &TransactionBatchProfile,
    executable: &Path,
) -> Result<TransactionBatchReport, String> {
    let mut authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    let io_before = authority.io_stats().await?;
    let batches = workload_batches(seed, profile);
    let first_batch = batches
        .first()
        .cloned()
        .ok_or_else(|| "transaction-batch workload is empty".to_owned())?;
    let mut responses = Vec::with_capacity(batches.len());
    let mut latencies =
        Vec::with_capacity(usize::try_from(profile.transaction_count).unwrap_or(usize::MAX));
    let started = Instant::now();
    for batch in &batches {
        let batch_started = Instant::now();
        let response = client.commit_batch(batch).await?;
        let latency = batch_started.elapsed().as_secs_f64();
        latencies.extend(std::iter::repeat_n(latency, batch.len()));
        responses.push(response);
    }
    let workload_seconds = started.elapsed().as_secs_f64();
    let io_after = authority.io_stats().await?;
    let node_io = io_delta(&io_before, &io_after, profile.transaction_count);
    let leader_logical_transactions_per_append = node_io
        .iter()
        .find(|report| report.node_id == 201)
        .map_or(0.0, |report| report.logical_transactions_per_append);

    let shared_versions_and_contiguous_orders = responses.iter().all(batch_is_ordered);
    let committed = committed_items(&responses);
    let committed_count = u64::try_from(committed.len()).unwrap_or(u64::MAX);
    let versionstamps = committed
        .iter()
        .map(|(_, response)| {
            let commit_version = match response.status {
                TransactionStatus::Committed { commit_version } => commit_version,
                TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => 0,
            };
            (commit_version, response.batch_order)
        })
        .collect::<Vec<_>>();
    let versionstamps_unique_and_increasing =
        versionstamps.windows(2).all(|window| window[0] < window[1]);
    let high_watermark = versionstamps
        .last()
        .map_or(0, |versionstamp| versionstamp.0);
    let retained = read_all(&client, high_watermark).await?;
    let retained_versionstamps = retained
        .iter()
        .map(|record| (record.commit_version, record.batch_order))
        .collect::<Vec<_>>();
    let retained_stream_complete = u64::try_from(retained.len()).unwrap_or(u64::MAX)
        == profile.transaction_count
        && retained_versionstamps == versionstamps;
    let final_values = replay_values(&retained);
    let expected_values = expected_values(seed, profile);
    let exact_final_values = values_without_versions(&final_values) == expected_values;

    authority.kill_initial_leader_and_elect_successor().await?;
    let failover_retained = read_all(&client, high_watermark).await?;
    let leader_failover_exact = failover_retained == retained;
    authority.restart_initial_voter().await?;
    authority
        .wait_for_voter_version(201, high_watermark)
        .await?;
    let restarted_view = authority.voter_transaction_view(201).await?;
    let restarted_voter_exact = restarted_view.current_version == high_watermark
        && values_without_versions(&restarted_view.values) == expected_values
        && restarted_view.retained_conflict_versions == profile.transaction_count;

    let first_response = responses
        .first()
        .cloned()
        .ok_or_else(|| "transaction-batch response set is empty".to_owned())?;
    let exact_batch_retry = client.commit_batch(&first_batch).await?.items == first_response.items;
    let last_item = batches
        .last()
        .and_then(|batch| batch.last())
        .ok_or_else(|| "transaction-batch workload has no final item".to_owned())?;
    let last_response = committed
        .last()
        .map(|(_, response)| response)
        .ok_or_else(|| "transaction-batch workload committed no item".to_owned())?;
    let exact_individual_retry = client
        .commit(last_item.identity, &last_item.command)
        .await?
        == *last_response;

    let in_batch_conflict_detected = in_batch_conflict_probe(&client, seed, high_watermark).await?;
    let duplicate_identity_rejected_before_mutation =
        duplicate_identity_probe(&client, &authority, seed, high_watermark).await?;

    let commit_p99_seconds = percentile(&latencies, 99);
    let transactions_per_second = if workload_seconds == 0.0 {
        f64::MAX
    } else {
        count_as_f64(committed_count) / workload_seconds
    };
    let correctness_anomalies = u64::from(authority_processes != 3)
        + u64::from(committed_count != profile.transaction_count)
        + u64::from(!versionstamps_unique_and_increasing)
        + u64::from(!shared_versions_and_contiguous_orders)
        + u64::from(!retained_stream_complete)
        + u64::from(!exact_final_values)
        + u64::from(!exact_individual_retry)
        + u64::from(!exact_batch_retry)
        + u64::from(!in_batch_conflict_detected)
        + u64::from(!duplicate_identity_rejected_before_mutation)
        + u64::from(!leader_failover_exact)
        + u64::from(!restarted_voter_exact);
    let semantic_sha256 = semantic_sha(
        seed,
        committed_count,
        high_watermark,
        &final_values,
        correctness_anomalies,
    )?;
    Ok(TransactionBatchReport {
        format_version: 1,
        seed,
        mode: TransactionBatchMode::Candidate,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        transaction_count: profile.transaction_count,
        batch_count: u64::try_from(batches.len()).unwrap_or(u64::MAX),
        transactions_per_batch: u64::try_from(profile.transactions_per_batch).unwrap_or(u64::MAX),
        committed_count,
        workload_seconds,
        transactions_per_second,
        commit_p99_seconds,
        versionstamps_unique_and_increasing,
        shared_versions_and_contiguous_orders,
        retained_stream_complete,
        exact_final_values,
        exact_individual_retry,
        exact_batch_retry,
        in_batch_conflict_detected,
        duplicate_identity_rejected_before_mutation,
        leader_failover_exact,
        restarted_voter_exact,
        early_ack_observed: false,
        early_ack_missing_after_quorum_recovery: false,
        node_io,
        leader_logical_transactions_per_append,
        correctness_anomalies,
        semantic_sha256,
    })
}

async fn run_duplicate_identity_control(
    seed: u64,
    profile: &TransactionBatchProfile,
    executable: &Path,
) -> Result<TransactionBatchReport, String> {
    let authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let before = authority.voter_transaction_view(201).await?;
    let key = format!("duplicate-control/{seed}").into_bytes();
    let item = TransactionBatchItem {
        identity: RequestIdentity {
            client_id: seed.max(1),
            request_id: 1,
        },
        credential: None,
        command: TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: vec![TransactionKeyRange::point(&key)],
            mutations: vec![TransactionMutation::Set {
                key: key.clone(),
                value: vec![4; profile.value_bytes],
            }],
        },
    };
    let rejected = client
        .commit_batch_once(&[item.clone(), item])
        .await
        .is_err();
    let after = authority.voter_transaction_view(201).await?;
    let unchanged = before == after && !after.values.contains_key(&key);
    let detected = rejected && unchanged;
    let semantic_sha256 = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                seed,
                TransactionBatchMode::DuplicateIdentityControl,
                detected
            ))
            .map_err(|error| error.to_string())?
        )
    );
    Ok(empty_report(
        seed,
        TransactionBatchMode::DuplicateIdentityControl,
        u64::try_from(authority.process_count()).unwrap_or(u64::MAX),
        detected,
        false,
        false,
        semantic_sha256,
    ))
}

async fn run_early_ack_poison(
    seed: u64,
    profile: &TransactionBatchProfile,
    executable: &Path,
) -> Result<TransactionBatchReport, String> {
    let mut authority =
        TransactionAuthorityProcessFixture::start_early_ack_poison(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    authority.kill_followers_for_poison()?;
    let items = workload_batches(seed, profile)
        .into_iter()
        .next()
        .ok_or_else(|| "early-ack transaction batch is empty".to_owned())?;
    let started = Instant::now();
    let early_ack_observed = client
        .acknowledge_batch_without_outcome_once(&items)
        .await
        .is_ok();
    let workload_seconds = started.elapsed().as_secs_f64();
    authority.kill_isolated_initial_leader()?;
    authority.restart_followers_and_elect_for_poison().await?;
    let recovered = authority.voter_transaction_view(202).await?;
    let early_ack_missing_after_quorum_recovery = recovered.current_version == 0
        && items.iter().all(|item| {
            item.command
                .mutations
                .iter()
                .all(|mutation| match mutation {
                    TransactionMutation::Set { key, .. } | TransactionMutation::Clear { key } => {
                        !recovered.values.contains_key(key)
                    }
                    TransactionMutation::ClearRange { .. } => true,
                })
        });
    let detected = early_ack_observed && early_ack_missing_after_quorum_recovery;
    let semantic_sha256 = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                seed,
                TransactionBatchMode::EarlyAckPoison,
                early_ack_observed,
                early_ack_missing_after_quorum_recovery,
            ))
            .map_err(|error| error.to_string())?
        )
    );
    let mut report = empty_report(
        seed,
        TransactionBatchMode::EarlyAckPoison,
        authority_processes,
        false,
        early_ack_observed,
        early_ack_missing_after_quorum_recovery,
        semantic_sha256,
    );
    report.transaction_count = u64::try_from(items.len()).unwrap_or(u64::MAX);
    report.batch_count = 1;
    report.transactions_per_batch = u64::try_from(items.len()).unwrap_or(u64::MAX);
    report.workload_seconds = workload_seconds;
    report.transactions_per_second = if workload_seconds == 0.0 {
        f64::MAX
    } else {
        count_as_f64(report.transaction_count) / workload_seconds
    };
    report.commit_p99_seconds = workload_seconds;
    report.correctness_anomalies = u64::from(detected);
    Ok(report)
}

fn empty_report(
    seed: u64,
    mode: TransactionBatchMode,
    authority_processes: u64,
    duplicate_identity_rejected_before_mutation: bool,
    early_ack_observed: bool,
    early_ack_missing_after_quorum_recovery: bool,
    semantic_sha256: String,
) -> TransactionBatchReport {
    TransactionBatchReport {
        format_version: 1,
        seed,
        mode,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        transaction_count: 0,
        batch_count: 0,
        transactions_per_batch: 0,
        committed_count: 0,
        workload_seconds: 0.0,
        transactions_per_second: 0.0,
        commit_p99_seconds: 0.0,
        versionstamps_unique_and_increasing: true,
        shared_versions_and_contiguous_orders: true,
        retained_stream_complete: true,
        exact_final_values: true,
        exact_individual_retry: true,
        exact_batch_retry: true,
        in_batch_conflict_detected: true,
        duplicate_identity_rejected_before_mutation,
        leader_failover_exact: true,
        restarted_voter_exact: true,
        early_ack_observed,
        early_ack_missing_after_quorum_recovery,
        node_io: Vec::new(),
        leader_logical_transactions_per_append: 0.0,
        correctness_anomalies: 0,
        semantic_sha256,
    }
}

fn validate_profile(profile: &TransactionBatchProfile) -> Result<(), String> {
    if profile.live_keys == 0
        || profile.value_bytes == 0
        || profile.transaction_count == 0
        || profile.transaction_count < profile.live_keys
        || profile.transactions_per_batch == 0
        || profile.transactions_per_batch > 32
        || !profile
            .transaction_count
            .is_multiple_of(u64::try_from(profile.transactions_per_batch).unwrap_or(u64::MAX))
    {
        return Err(
            "transaction-batch profile requires non-zero fields, transaction count at least live keys, batch size at most 32, and complete batches"
                .to_owned(),
        );
    }
    Ok(())
}

fn workload_batches(
    seed: u64,
    profile: &TransactionBatchProfile,
) -> Vec<Vec<TransactionBatchItem>> {
    let items = (1..=profile.transaction_count)
        .map(|request_id| TransactionBatchItem {
            identity: RequestIdentity {
                client_id: seed.max(1),
                request_id,
            },
            credential: None,
            command: command(seed, request_id, profile),
        })
        .collect::<Vec<_>>();
    items
        .chunks(profile.transactions_per_batch)
        .map(<[TransactionBatchItem]>::to_vec)
        .collect()
}

fn command(seed: u64, request_id: u64, profile: &TransactionBatchProfile) -> TransactionCommand {
    let key_index = (request_id - 1) % profile.live_keys;
    let key = format!("range/0001/key/{key_index:08}").into_bytes();
    TransactionCommand {
        read_version: 0,
        read_conflicts: Vec::new(),
        write_conflicts: vec![TransactionKeyRange::point(&key)],
        mutations: vec![TransactionMutation::Set {
            key,
            value: vec![value_fill(seed, key_index); profile.value_bytes],
        }],
    }
}

fn committed_items(
    responses: &[TransactionBatchApplyResponse],
) -> Vec<(RequestIdentity, TransactionApplyResponse)> {
    responses
        .iter()
        .flat_map(|batch| &batch.items)
        .filter_map(|item| {
            item.transaction.as_ref().and_then(|response| {
                matches!(response.status, TransactionStatus::Committed { .. })
                    .then(|| (item.identity, response.clone()))
            })
        })
        .collect()
}

fn batch_is_ordered(response: &TransactionBatchApplyResponse) -> bool {
    let mut commit_version = None;
    response.items.iter().enumerate().all(|(order, item)| {
        let Some(transaction) = &item.transaction else {
            return false;
        };
        let TransactionStatus::Committed {
            commit_version: item_version,
        } = transaction.status
        else {
            return false;
        };
        if commit_version.is_none() {
            commit_version = Some(item_version);
        }
        commit_version == Some(item_version)
            && transaction.batch_order == u16::try_from(order).unwrap_or(u16::MAX)
    })
}

async fn read_all(
    client: &okv_consensus::TransactionLogClient,
    high_watermark: u64,
) -> Result<Vec<okv_consensus::RetainedTransactionRecord>, String> {
    let mut after_version_exclusive = 0;
    let mut after_batch_order_exclusive = None;
    let mut records = Vec::new();
    loop {
        let page = client
            .read(RetainedTransactionReadRequest {
                after_version_exclusive,
                after_batch_order_exclusive,
                through_version_inclusive: Some(high_watermark),
                max_records: 7,
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

async fn in_batch_conflict_probe(
    client: &okv_consensus::TransactionLogClient,
    seed: u64,
    read_version: u64,
) -> Result<bool, String> {
    let first_key = format!("probe/{seed}/a").into_bytes();
    let second_key = format!("probe/{seed}/b").into_bytes();
    let items = vec![
        TransactionBatchItem {
            identity: RequestIdentity {
                client_id: seed.max(1).saturating_add(1),
                request_id: 1,
            },
            credential: None,
            command: TransactionCommand {
                read_version,
                read_conflicts: Vec::new(),
                write_conflicts: vec![TransactionKeyRange::point(&first_key)],
                mutations: vec![TransactionMutation::Set {
                    key: first_key.clone(),
                    value: vec![1],
                }],
            },
        },
        TransactionBatchItem {
            identity: RequestIdentity {
                client_id: seed.max(1).saturating_add(1),
                request_id: 2,
            },
            credential: None,
            command: TransactionCommand {
                read_version,
                read_conflicts: vec![TransactionKeyRange::point(&first_key)],
                write_conflicts: vec![TransactionKeyRange::point(&second_key)],
                mutations: vec![TransactionMutation::Set {
                    key: second_key,
                    value: vec![2],
                }],
            },
        },
    ];
    let response = client.commit_batch(&items).await?;
    Ok(matches!(
        response.items[0]
            .transaction
            .as_ref()
            .map(|item| &item.status),
        Some(TransactionStatus::Committed { .. })
    ) && matches!(
        response.items[1]
            .transaction
            .as_ref()
            .map(|item| &item.status),
        Some(TransactionStatus::Conflict { .. })
    ))
}

async fn duplicate_identity_probe(
    client: &okv_consensus::TransactionLogClient,
    authority: &TransactionAuthorityProcessFixture,
    seed: u64,
    prior_version: u64,
) -> Result<bool, String> {
    let key = format!("probe/{seed}/duplicate").into_bytes();
    let item = TransactionBatchItem {
        identity: RequestIdentity {
            client_id: seed.max(1).saturating_add(2),
            request_id: 1,
        },
        credential: None,
        command: TransactionCommand {
            read_version: prior_version,
            read_conflicts: Vec::new(),
            write_conflicts: vec![TransactionKeyRange::point(&key)],
            mutations: vec![TransactionMutation::Set {
                key: key.clone(),
                value: vec![3],
            }],
        },
    };
    let rejected = client
        .commit_batch_once(&[item.clone(), item])
        .await
        .is_err();
    let view = authority.voter_transaction_view(202).await?;
    Ok(rejected && !view.values.contains_key(&key))
}

fn replay_values(
    records: &[okv_consensus::RetainedTransactionRecord],
) -> BTreeMap<Vec<u8>, okv_transaction::VersionedValue> {
    let mut values = BTreeMap::new();
    for record in records {
        for mutation in &record.command.mutations {
            match mutation {
                TransactionMutation::Set { key, value } => {
                    values.insert(
                        key.clone(),
                        okv_transaction::VersionedValue {
                            version: record.commit_version,
                            value: value.clone(),
                        },
                    );
                }
                TransactionMutation::Clear { key } => {
                    values.remove(key);
                }
                TransactionMutation::ClearRange { range } => {
                    let keys = values
                        .range(range.start.clone()..range.end.clone())
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in keys {
                        values.remove(&key);
                    }
                }
            }
        }
    }
    values
}

fn expected_values(seed: u64, profile: &TransactionBatchProfile) -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..profile.live_keys)
        .map(|key_index| {
            let key = format!("range/0001/key/{key_index:08}").into_bytes();
            (key, vec![value_fill(seed, key_index); profile.value_bytes])
        })
        .collect()
}

fn values_without_versions(
    values: &BTreeMap<Vec<u8>, okv_transaction::VersionedValue>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.value.clone()))
        .collect()
}

fn value_fill(seed: u64, key_index: u64) -> u8 {
    u8::try_from(65 + ((seed + key_index) % 26)).unwrap_or(b'Z')
}

fn io_delta(
    before: &BTreeMap<u64, OpenRaftIoStats>,
    after: &BTreeMap<u64, OpenRaftIoStats>,
    transaction_count: u64,
) -> Vec<TransactionBatchNodeIoReport> {
    after
        .iter()
        .map(|(node_id, current)| {
            let prior = before.get(node_id).copied().unwrap_or_default();
            let append_calls = current.append_calls.saturating_sub(prior.append_calls);
            let appended_entries = current
                .appended_entries
                .saturating_sub(prior.appended_entries);
            TransactionBatchNodeIoReport {
                node_id: *node_id,
                append_calls,
                appended_entries,
                entries_per_append: ratio(appended_entries, append_calls),
                logical_transactions_per_append: ratio(transaction_count, append_calls),
                append_durable_seconds: nanos_as_seconds(
                    current
                        .append_durable_nanos
                        .saturating_sub(prior.append_durable_nanos),
                ),
                committed_calls: current
                    .committed_calls
                    .saturating_sub(prior.committed_calls),
                committed_durable_seconds: nanos_as_seconds(
                    current
                        .committed_durable_nanos
                        .saturating_sub(prior.committed_durable_nanos),
                ),
                physical_journal_bytes: current.physical_journal_bytes,
            }
        })
        .collect()
}

fn semantic_sha(
    seed: u64,
    committed_count: u64,
    high_watermark: u64,
    values: &BTreeMap<Vec<u8>, okv_transaction::VersionedValue>,
    anomalies: u64,
) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(
        seed,
        committed_count,
        high_watermark,
        values_without_versions(values)
            .into_iter()
            .collect::<Vec<_>>(),
        anomalies,
    ))
    .map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn percentile(samples: &[f64], percentile: usize) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    ordered[(ordered.len().saturating_sub(1) * percentile) / 100]
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        count_as_f64(numerator) / count_as_f64(denominator)
    }
}

#[allow(clippy::cast_precision_loss)]
fn count_as_f64(value: u64) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn nanos_as_seconds(value: u64) -> f64 {
    value as f64 / 1_000_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_requires_complete_bounded_batches() {
        let valid = TransactionBatchProfile {
            live_keys: 256,
            value_bytes: 128,
            transaction_count: 512,
            transactions_per_batch: 16,
        };
        assert!(validate_profile(&valid).is_ok());
        let mut invalid = valid.clone();
        invalid.transactions_per_batch = 33;
        assert!(validate_profile(&invalid).is_err());
        invalid = valid;
        invalid.transaction_count = 513;
        assert!(validate_profile(&invalid).is_err());
    }
}
