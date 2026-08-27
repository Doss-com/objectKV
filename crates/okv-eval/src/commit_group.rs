//! G4.8 bounded concurrent commit and stable-journal decomposition.

use okv_consensus::{
    OpenRaftIoStats, RequestIdentity, RetainedTransactionReadRequest,
    TransactionAuthorityProcessFixture, TransactionCommand, TransactionKeyRange,
    TransactionMutation, TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;
use tokio::task::JoinSet;

/// Subject selected for the G4.8 group-commit falsifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitGroupMode {
    Candidate,
    SequentialControl,
    EarlyAckPoison,
}

impl CommitGroupMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::SequentialControl => "sequential_control",
            Self::EarlyAckPoison => "early_ack_poison",
        }
    }
}

/// Frozen G4.8 workload and admission limits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitGroupProfile {
    pub live_keys: u64,
    pub value_bytes: usize,
    pub transaction_count: u64,
    pub candidate_max_in_flight: usize,
    pub control_max_in_flight: usize,
    pub candidate_min_transactions_per_second: u64,
    pub candidate_min_entries_per_append: u64,
    pub candidate_max_commit_p99_micros: u64,
}

/// Workload-local stable-log delta for one voter.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct CommitNodeIoReport {
    pub node_id: u64,
    pub append_calls: u64,
    pub appended_entries: u64,
    pub entries_per_append: f64,
    pub append_durable_seconds: f64,
    pub committed_calls: u64,
    pub committed_durable_seconds: f64,
    pub physical_journal_bytes: u64,
}

/// Canonical report for one fresh three-process G4.8 execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CommitGroupReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: CommitGroupMode,
    pub authority_processes: u64,
    pub release_build: bool,
    pub max_in_flight: u64,
    pub transaction_count: u64,
    pub committed_count: u64,
    pub workload_seconds: f64,
    pub transactions_per_second: f64,
    pub commit_p99_seconds: f64,
    pub commit_versions_unique_and_increasing: bool,
    pub retained_stream_complete: bool,
    pub exact_final_values: bool,
    pub exact_retry: bool,
    pub leader_failover_exact: bool,
    pub restarted_voter_exact: bool,
    pub early_ack_observed: bool,
    pub early_ack_missing_after_quorum_recovery: bool,
    pub node_io: Vec<CommitNodeIoReport>,
    pub median_entries_per_append: f64,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

/// Run one fixed G4.8 process contract.
///
/// # Errors
///
/// Returns an error when the profile is invalid, process startup fails, or the
/// candidate/control cannot complete its execution path.
pub fn run_commit_group_contract(
    seed: u64,
    mode: CommitGroupMode,
    profile: &CommitGroupProfile,
    executable: &Path,
) -> Result<CommitGroupReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        match mode {
            CommitGroupMode::Candidate | CommitGroupMode::SequentialControl => {
                run_normal(seed, mode, profile, executable).await
            }
            CommitGroupMode::EarlyAckPoison => {
                run_early_ack_poison(seed, profile, executable).await
            }
        }
    })
}

#[allow(clippy::too_many_lines)]
async fn run_normal(
    seed: u64,
    mode: CommitGroupMode,
    profile: &CommitGroupProfile,
    executable: &Path,
) -> Result<CommitGroupReport, String> {
    let mut authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    let io_before = authority.io_stats().await?;
    let max_in_flight = match mode {
        CommitGroupMode::Candidate => profile.candidate_max_in_flight,
        CommitGroupMode::SequentialControl => profile.control_max_in_flight,
        CommitGroupMode::EarlyAckPoison => unreachable!(),
    };
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut next_request_id = 1_u64;
    let mut results =
        Vec::with_capacity(usize::try_from(profile.transaction_count).unwrap_or(usize::MAX));
    while next_request_id <= profile.transaction_count || !tasks.is_empty() {
        while next_request_id <= profile.transaction_count && tasks.len() < max_in_flight {
            let request_id = next_request_id;
            next_request_id = next_request_id.saturating_add(1);
            let client = client.clone();
            let command = command(seed, request_id, profile);
            tasks.spawn(async move {
                let request_started = Instant::now();
                let response = client
                    .commit(
                        RequestIdentity {
                            client_id: seed.max(1),
                            request_id,
                        },
                        &command,
                    )
                    .await;
                (
                    request_id,
                    command,
                    response,
                    request_started.elapsed().as_secs_f64(),
                )
            });
        }
        let joined = tasks
            .join_next()
            .await
            .ok_or_else(|| "commit-group task set ended before all requests completed".to_owned())?
            .map_err(|error| error.to_string())?;
        let (request_id, command, response, latency) = joined;
        results.push((request_id, command, response?, latency));
    }
    let workload_seconds = started.elapsed().as_secs_f64();
    let io_after = authority.io_stats().await?;
    let node_io = io_delta(&io_before, &io_after);
    let median_entries_per_append = median_f64(
        &node_io
            .iter()
            .map(|report| report.entries_per_append)
            .collect::<Vec<_>>(),
    );

    let mut committed = results
        .iter()
        .filter_map(
            |(request_id, command, response, latency)| match response.status {
                TransactionStatus::Committed { commit_version } => Some((
                    commit_version,
                    *request_id,
                    command.clone(),
                    response.clone(),
                    *latency,
                )),
                TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => None,
            },
        )
        .collect::<Vec<_>>();
    committed.sort_by_key(|entry| entry.0);
    let committed_count = u64::try_from(committed.len()).unwrap_or(u64::MAX);
    let commit_versions_unique_and_increasing =
        committed.windows(2).all(|window| window[0].0 < window[1].0);
    let high_watermark = committed.last().map_or(0, |entry| entry.0);
    let page = client
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(high_watermark),
            max_records: u32::try_from(profile.transaction_count)
                .map_err(|error| error.to_string())?,
        })
        .await?;
    let retained_stream_complete = page.complete
        && u64::try_from(page.records.len()).unwrap_or(u64::MAX) == profile.transaction_count
        && page
            .records
            .windows(2)
            .all(|window| window[0].commit_version < window[1].commit_version);
    let final_values = replay_values(&page.records);
    let expected_values = expected_values(seed, profile);
    let exact_final_values = values_without_versions(&final_values) == expected_values;
    let latencies = results.iter().map(|entry| entry.3).collect::<Vec<_>>();
    let commit_p99_seconds = percentile(&latencies, 99);
    let transactions_per_second = if workload_seconds == 0.0 {
        f64::MAX
    } else {
        count_as_f64(committed_count) / workload_seconds
    };

    authority.kill_initial_leader_and_elect_successor().await?;
    let failover_page = client
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(high_watermark),
            max_records: u32::try_from(profile.transaction_count)
                .map_err(|error| error.to_string())?,
        })
        .await?;
    let leader_failover_exact = failover_page.records == page.records;
    authority.restart_initial_voter().await?;

    results.sort_by_key(|entry| entry.0);
    let first = results
        .first()
        .ok_or_else(|| "commit-group result set is empty".to_owned())?;
    let replay = client
        .commit(
            RequestIdentity {
                client_id: seed.max(1),
                request_id: first.0,
            },
            &first.1,
        )
        .await?;
    let exact_retry = replay == first.2;
    authority
        .wait_for_voter_version(201, high_watermark)
        .await?;
    let restarted_view = authority.voter_transaction_view(201).await?;
    let restarted_voter_exact = restarted_view.current_version == high_watermark
        && values_without_versions(&restarted_view.values) == expected_values
        && restarted_view.retained_conflict_versions == profile.transaction_count;

    let correctness_anomalies = u64::from(authority_processes != 3)
        + u64::from(committed_count != profile.transaction_count)
        + u64::from(!commit_versions_unique_and_increasing)
        + u64::from(!retained_stream_complete)
        + u64::from(!exact_final_values)
        + u64::from(!exact_retry)
        + u64::from(!leader_failover_exact)
        + u64::from(!restarted_voter_exact);
    let semantic_sha256 = semantic_sha(
        seed,
        mode,
        committed_count,
        high_watermark,
        &final_values,
        correctness_anomalies,
    )?;
    Ok(CommitGroupReport {
        format_version: 1,
        seed,
        mode,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        max_in_flight: u64::try_from(max_in_flight).unwrap_or(u64::MAX),
        transaction_count: profile.transaction_count,
        committed_count,
        workload_seconds,
        transactions_per_second,
        commit_p99_seconds,
        commit_versions_unique_and_increasing,
        retained_stream_complete,
        exact_final_values,
        exact_retry,
        leader_failover_exact,
        restarted_voter_exact,
        early_ack_observed: false,
        early_ack_missing_after_quorum_recovery: false,
        node_io,
        median_entries_per_append,
        correctness_anomalies,
        semantic_sha256,
    })
}

async fn run_early_ack_poison(
    seed: u64,
    profile: &CommitGroupProfile,
    executable: &Path,
) -> Result<CommitGroupReport, String> {
    let mut authority =
        TransactionAuthorityProcessFixture::start_early_ack_poison(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    authority.kill_followers_for_poison()?;
    let command = command(seed, 1, profile);
    let started = Instant::now();
    let early_ack_observed = client
        .acknowledge_without_outcome_once(
            RequestIdentity {
                client_id: seed.max(1),
                request_id: 1,
            },
            &command,
        )
        .await
        .is_ok();
    let workload_seconds = started.elapsed().as_secs_f64();
    authority.kill_isolated_initial_leader()?;
    authority.restart_followers_and_elect_for_poison().await?;
    let recovered = authority
        .client()?
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: None,
            max_records: 1,
        })
        .await?;
    let early_ack_missing_after_quorum_recovery = recovered.records.is_empty()
        && recovered.high_watermark == 0
        && recovered.retention_floor == 0;
    let correctness_anomalies =
        u64::from(early_ack_observed && early_ack_missing_after_quorum_recovery);
    let semantic_sha256 = semantic_sha(
        seed,
        CommitGroupMode::EarlyAckPoison,
        0,
        0,
        &BTreeMap::new(),
        correctness_anomalies,
    )?;
    Ok(CommitGroupReport {
        format_version: 1,
        seed,
        mode: CommitGroupMode::EarlyAckPoison,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        max_in_flight: 1,
        transaction_count: 1,
        committed_count: 0,
        workload_seconds,
        transactions_per_second: if workload_seconds == 0.0 {
            f64::MAX
        } else {
            1.0 / workload_seconds
        },
        commit_p99_seconds: workload_seconds,
        commit_versions_unique_and_increasing: true,
        retained_stream_complete: recovered.records.is_empty(),
        exact_final_values: recovered.records.is_empty(),
        exact_retry: false,
        leader_failover_exact: early_ack_missing_after_quorum_recovery,
        restarted_voter_exact: true,
        early_ack_observed,
        early_ack_missing_after_quorum_recovery,
        node_io: Vec::new(),
        median_entries_per_append: 0.0,
        correctness_anomalies,
        semantic_sha256,
    })
}

fn validate_profile(profile: &CommitGroupProfile) -> Result<(), String> {
    if profile.live_keys == 0
        || profile.value_bytes == 0
        || profile.transaction_count < profile.live_keys
        || profile.transaction_count > 4_096
    {
        return Err("commit-group profile has invalid workload bounds".to_owned());
    }
    if profile.candidate_max_in_flight < 2
        || profile.control_max_in_flight != 1
        || profile.candidate_max_in_flight > 256
        || profile.candidate_min_transactions_per_second == 0
        || profile.candidate_min_entries_per_append == 0
        || profile.candidate_max_commit_p99_micros == 0
    {
        return Err("commit-group profile has invalid admission gates".to_owned());
    }
    Ok(())
}

fn command(seed: u64, request_id: u64, profile: &CommitGroupProfile) -> TransactionCommand {
    let key_index = (request_id - 1) % profile.live_keys;
    let key = format!("range/0001/key/{key_index:08}").into_bytes();
    let fill = value_fill(seed, key_index);
    TransactionCommand {
        read_version: 0,
        read_conflicts: Vec::new(),
        write_conflicts: vec![TransactionKeyRange::point(&key)],
        mutations: vec![TransactionMutation::Set {
            key,
            value: vec![fill; profile.value_bytes],
        }],
    }
}

fn expected_values(seed: u64, profile: &CommitGroupProfile) -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..profile.live_keys)
        .map(|key_index| {
            let key = format!("range/0001/key/{key_index:08}").into_bytes();
            (key, vec![value_fill(seed, key_index); profile.value_bytes])
        })
        .collect()
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
) -> Vec<CommitNodeIoReport> {
    after
        .iter()
        .map(|(node_id, current)| {
            let prior = before.get(node_id).copied().unwrap_or_default();
            let append_calls = current.append_calls.saturating_sub(prior.append_calls);
            let appended_entries = current
                .appended_entries
                .saturating_sub(prior.appended_entries);
            CommitNodeIoReport {
                node_id: *node_id,
                append_calls,
                appended_entries,
                entries_per_append: ratio(appended_entries, append_calls),
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
    mode: CommitGroupMode,
    committed_count: u64,
    high_watermark: u64,
    values: &BTreeMap<Vec<u8>, okv_transaction::VersionedValue>,
    anomalies: u64,
) -> Result<String, String> {
    let ordered_values = values_without_versions(values)
        .into_iter()
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&(
        seed,
        mode,
        committed_count,
        high_watermark,
        ordered_values,
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
    let index = (ordered.len().saturating_sub(1) * percentile) / 100;
    ordered[index]
}

fn median_f64(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_by(f64::total_cmp);
    let midpoint = ordered.len() / 2;
    if ordered.len() % 2 == 0 {
        f64::midpoint(ordered[midpoint - 1], ordered[midpoint])
    } else {
        ordered[midpoint]
    }
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
    fn profile_rejects_unbounded_or_nonsequential_controls() {
        let valid = CommitGroupProfile {
            live_keys: 256,
            value_bytes: 128,
            transaction_count: 512,
            candidate_max_in_flight: 32,
            control_max_in_flight: 1,
            candidate_min_transactions_per_second: 200,
            candidate_min_entries_per_append: 4,
            candidate_max_commit_p99_micros: 250_000,
        };
        assert!(validate_profile(&valid).is_ok());
        let mut invalid = valid.clone();
        invalid.control_max_in_flight = 2;
        assert!(validate_profile(&invalid).is_err());
        invalid = valid;
        invalid.candidate_max_in_flight = 257;
        assert!(validate_profile(&invalid).is_err());
    }

    #[test]
    fn final_value_oracle_ignores_concurrent_overwrite_order() {
        let profile = CommitGroupProfile {
            live_keys: 1,
            value_bytes: 2,
            transaction_count: 2,
            candidate_max_in_flight: 2,
            control_max_in_flight: 1,
            candidate_min_transactions_per_second: 1,
            candidate_min_entries_per_append: 1,
            candidate_max_commit_p99_micros: 1,
        };
        assert_eq!(command(9, 1, &profile), command(9, 2, &profile));
    }
}
