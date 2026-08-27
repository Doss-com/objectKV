//! G4.11a durable process snapshot and physical journal-compaction contract.

use okv_consensus::{
    ProcessJournalCompactionObservation, RequestIdentity, RetainedTransactionReadRequest,
    RetainedTransactionRecord, TransactionApplyResponse, TransactionAuthorityProcessFixture,
    TransactionBatchApplyResponse, TransactionBatchItem, TransactionCommand, TransactionKeyRange,
    TransactionMutation, TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Frozen G4.11a subject or fail-closed control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessSnapshotCompactionMode {
    Candidate,
    PurgeBeforeSnapshotPoison,
}

impl ProcessSnapshotCompactionMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::PurgeBeforeSnapshotPoison => "purge_before_snapshot_poison",
        }
    }
}

/// Frozen G4.11a transaction and batch bounds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessSnapshotCompactionProfile {
    pub transaction_count: u64,
    pub transactions_per_batch: usize,
    pub live_keys: u64,
    pub value_bytes: usize,
}

/// Canonical G4.11a report from one real three-process data quorum.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProcessSnapshotCompactionReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: ProcessSnapshotCompactionMode,
    pub authority_processes: u64,
    pub release_build: bool,
    pub transaction_count: u64,
    pub committed_count: u64,
    pub high_watermark: u64,
    pub retained_records_before: u64,
    pub compaction: Vec<ProcessJournalCompactionObservation>,
    pub all_snapshots_cover_purge: bool,
    pub all_journals_reclaimed_bytes: bool,
    pub journal_bytes_before: u64,
    pub journal_bytes_after: u64,
    pub journal_reclaimed_bytes: u64,
    pub snapshot_bytes: u64,
    pub durable_bytes_after: u64,
    pub full_quorum_restart_exact: bool,
    pub retained_stream_restart_exact: bool,
    pub exact_retry_after_restart: bool,
    pub suffix_commit_after_restart: bool,
    pub purge_before_snapshot_rejected: bool,
    pub poison_journal_unchanged: bool,
    pub poison_restart_exact: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

/// Execute one G4.11a snapshot and journal-reclamation subject.
///
/// # Errors
///
/// Returns an error when profile validation, process startup, transaction
/// application, snapshot, purge, restart, or recovery cannot complete.
pub fn run_process_snapshot_compaction_contract(
    seed: u64,
    mode: ProcessSnapshotCompactionMode,
    profile: &ProcessSnapshotCompactionProfile,
    executable: &Path,
) -> Result<ProcessSnapshotCompactionReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        match mode {
            ProcessSnapshotCompactionMode::Candidate => {
                run_candidate(seed, profile, executable).await
            }
            ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison => {
                run_purge_before_snapshot_poison(seed, profile, executable).await
            }
        }
    })
}

#[allow(clippy::too_many_lines)]
async fn run_candidate(
    seed: u64,
    profile: &ProcessSnapshotCompactionProfile,
    executable: &Path,
) -> Result<ProcessSnapshotCompactionReport, String> {
    let mut authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    let batches = workload_batches(seed, profile, "prefix");
    let first_batch = batches
        .first()
        .cloned()
        .ok_or_else(|| "snapshot-compaction workload is empty".to_owned())?;
    let mut responses = Vec::with_capacity(batches.len());
    for batch in &batches {
        responses.push(client.commit_batch(batch).await?);
    }
    let committed_count = count_committed(&responses);
    let high_watermark = maximum_commit_version(&responses)
        .ok_or_else(|| "snapshot-compaction workload committed no transactions".to_owned())?;
    let retained_before = read_all(&client, high_watermark).await?;
    let view_before = authority.voter_transaction_view(201).await?;

    let compaction = authority.snapshot_and_purge_all(high_watermark).await?;
    let compaction = compaction.into_values().collect::<Vec<_>>();
    let all_snapshots_cover_purge = compaction.iter().all(|observation| {
        observation.snapshot_index >= high_watermark
            && observation.purged_index >= high_watermark
            && observation.compaction_calls > 0
    });
    let all_journals_reclaimed_bytes = compaction.iter().all(|observation| {
        observation.compaction_reclaimed_bytes > 0
            && observation.journal_bytes_after < observation.journal_bytes_before
    });
    let journal_bytes_before: u64 = compaction
        .iter()
        .map(|observation| observation.journal_bytes_before)
        .sum();
    let journal_bytes_after: u64 = compaction
        .iter()
        .map(|observation| observation.journal_bytes_after)
        .sum();
    let journal_reclaimed_bytes: u64 = compaction
        .iter()
        .map(|observation| observation.compaction_reclaimed_bytes)
        .sum();
    let snapshot_bytes: u64 = compaction
        .iter()
        .map(|observation| observation.snapshot_bytes)
        .sum();
    let durable_bytes_after = journal_bytes_after.saturating_add(snapshot_bytes);

    authority.restart_all_and_elect_initial().await?;
    let reopened_client = authority.client()?;
    let view_after = authority.voter_transaction_view(201).await?;
    let retained_after = read_all(&reopened_client, high_watermark).await?;
    let full_quorum_restart_exact = view_after == view_before;
    let retained_stream_restart_exact = retained_after == retained_before;
    let exact_retry_after_restart =
        reopened_client.commit_batch(&first_batch).await?.items == responses[0].items;

    let suffix = suffix_batch(seed, profile, high_watermark);
    let suffix_response = reopened_client.commit_batch(&suffix).await?;
    let suffix_commit_after_restart = suffix_response.items.iter().all(|item| {
        item.transaction.as_ref().is_some_and(|response| {
            matches!(
                response.status,
                TransactionStatus::Committed { commit_version } if commit_version > high_watermark
            )
        })
    });
    let correctness_anomalies = u64::from(authority_processes != 3)
        + u64::from(committed_count != profile.transaction_count)
        + u64::from(
            u64::try_from(retained_before.len()).unwrap_or(u64::MAX) != profile.transaction_count,
        )
        + u64::from(!all_snapshots_cover_purge)
        + u64::from(!all_journals_reclaimed_bytes)
        + u64::from(!full_quorum_restart_exact)
        + u64::from(!retained_stream_restart_exact)
        + u64::from(!exact_retry_after_restart)
        + u64::from(!suffix_commit_after_restart);
    let semantic_sha256 = semantic_sha(&(
        seed,
        mode_id(ProcessSnapshotCompactionMode::Candidate),
        committed_count,
        high_watermark,
        &retained_before,
        &compaction,
        full_quorum_restart_exact,
        suffix_commit_after_restart,
        correctness_anomalies,
    ))?;
    Ok(ProcessSnapshotCompactionReport {
        format_version: 1,
        seed,
        mode: ProcessSnapshotCompactionMode::Candidate,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        transaction_count: profile.transaction_count,
        committed_count,
        high_watermark,
        retained_records_before: u64::try_from(retained_before.len()).unwrap_or(u64::MAX),
        compaction,
        all_snapshots_cover_purge,
        all_journals_reclaimed_bytes,
        journal_bytes_before,
        journal_bytes_after,
        journal_reclaimed_bytes,
        snapshot_bytes,
        durable_bytes_after,
        full_quorum_restart_exact,
        retained_stream_restart_exact,
        exact_retry_after_restart,
        suffix_commit_after_restart,
        purge_before_snapshot_rejected: false,
        poison_journal_unchanged: false,
        poison_restart_exact: false,
        correctness_anomalies,
        semantic_sha256,
    })
}

async fn run_purge_before_snapshot_poison(
    seed: u64,
    profile: &ProcessSnapshotCompactionProfile,
    executable: &Path,
) -> Result<ProcessSnapshotCompactionReport, String> {
    let mut authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let client = authority.client()?;
    let first = workload_batches(seed, profile, "poison")
        .into_iter()
        .next()
        .ok_or_else(|| "purge poison workload is empty".to_owned())?;
    let response = client.commit_batch(&first).await?;
    let high_watermark = maximum_commit_version(std::slice::from_ref(&response))
        .ok_or_else(|| "purge poison committed no transaction".to_owned())?;
    let before_io = authority.io_stats().await?;
    let before_view = authority.voter_transaction_view(201).await?;
    let rejection = authority
        .purge_without_snapshot_once(201, high_watermark)
        .await
        .expect_err("purge-before-snapshot poison must be rejected");
    let after_io = authority.io_stats().await?;
    let purge_before_snapshot_rejected = rejection.contains("does not cover purge index");
    let poison_journal_unchanged = before_io.iter().all(|(node_id, before)| {
        after_io.get(node_id).is_some_and(|after| {
            after.physical_journal_bytes == before.physical_journal_bytes
                && after.compaction_calls == before.compaction_calls
                && after.compaction_reclaimed_bytes == before.compaction_reclaimed_bytes
        })
    });
    authority.restart_all_and_elect_initial().await?;
    let poison_restart_exact = authority.voter_transaction_view(201).await? == before_view;
    let detected =
        purge_before_snapshot_rejected && poison_journal_unchanged && poison_restart_exact;
    let semantic_sha256 = semantic_sha(&(
        seed,
        mode_id(ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison),
        high_watermark,
        purge_before_snapshot_rejected,
        poison_journal_unchanged,
        poison_restart_exact,
    ))?;
    Ok(ProcessSnapshotCompactionReport {
        format_version: 1,
        seed,
        mode: ProcessSnapshotCompactionMode::PurgeBeforeSnapshotPoison,
        authority_processes,
        release_build: !cfg!(debug_assertions),
        transaction_count: u64::try_from(first.len()).unwrap_or(u64::MAX),
        committed_count: count_committed(std::slice::from_ref(&response)),
        high_watermark,
        retained_records_before: u64::try_from(first.len()).unwrap_or(u64::MAX),
        compaction: Vec::new(),
        all_snapshots_cover_purge: false,
        all_journals_reclaimed_bytes: false,
        journal_bytes_before: before_io
            .values()
            .map(|stats| stats.physical_journal_bytes)
            .sum(),
        journal_bytes_after: after_io
            .values()
            .map(|stats| stats.physical_journal_bytes)
            .sum(),
        journal_reclaimed_bytes: 0,
        snapshot_bytes: after_io
            .values()
            .map(|stats| stats.state_machine_snapshot_bytes)
            .sum(),
        durable_bytes_after: after_io
            .values()
            .map(|stats| {
                stats
                    .physical_journal_bytes
                    .saturating_add(stats.state_machine_snapshot_bytes)
            })
            .sum(),
        full_quorum_restart_exact: false,
        retained_stream_restart_exact: false,
        exact_retry_after_restart: false,
        suffix_commit_after_restart: false,
        purge_before_snapshot_rejected,
        poison_journal_unchanged,
        poison_restart_exact,
        correctness_anomalies: u64::from(!detected),
        semantic_sha256,
    })
}

fn workload_batches(
    seed: u64,
    profile: &ProcessSnapshotCompactionProfile,
    namespace: &str,
) -> Vec<Vec<TransactionBatchItem>> {
    let items = (0..profile.transaction_count)
        .map(|index| {
            let key =
                format!("snapshot/{namespace}/{seed}/{}", index % profile.live_keys).into_bytes();
            TransactionBatchItem {
                identity: RequestIdentity {
                    client_id: seed.max(1),
                    request_id: index.saturating_add(1),
                },
                credential: None,
                command: TransactionCommand {
                    read_version: 0,
                    read_conflicts: Vec::new(),
                    write_conflicts: vec![TransactionKeyRange::point(&key)],
                    mutations: vec![TransactionMutation::Set {
                        key,
                        value: deterministic_value(seed, index, profile.value_bytes),
                    }],
                },
            }
        })
        .collect::<Vec<_>>();
    items
        .chunks(profile.transactions_per_batch)
        .map(<[TransactionBatchItem]>::to_vec)
        .collect()
}

fn suffix_batch(
    seed: u64,
    profile: &ProcessSnapshotCompactionProfile,
    read_version: u64,
) -> Vec<TransactionBatchItem> {
    (0..profile.transactions_per_batch)
        .map(|index| {
            let index = u64::try_from(index).unwrap_or(u64::MAX);
            let key = format!("snapshot/suffix/{seed}/{index}").into_bytes();
            TransactionBatchItem {
                identity: RequestIdentity {
                    client_id: seed.max(1).saturating_add(1_000_000),
                    request_id: index.saturating_add(1),
                },
                credential: None,
                command: TransactionCommand {
                    read_version,
                    read_conflicts: Vec::new(),
                    write_conflicts: vec![TransactionKeyRange::point(&key)],
                    mutations: vec![TransactionMutation::Set {
                        key,
                        value: deterministic_value(seed, index, profile.value_bytes),
                    }],
                },
            }
        })
        .collect()
}

async fn read_all(
    client: &okv_consensus::TransactionLogClient,
    high_watermark: u64,
) -> Result<Vec<RetainedTransactionRecord>, String> {
    let mut records = Vec::new();
    let mut after_version = 0;
    let mut after_order = None;
    loop {
        let page = client
            .read(RetainedTransactionReadRequest {
                after_version_exclusive: after_version,
                after_batch_order_exclusive: after_order,
                through_version_inclusive: Some(high_watermark),
                max_records: 128,
            })
            .await?;
        records.extend(page.records);
        if page.complete {
            break;
        }
        after_version = page.next_after_version;
        after_order = page.next_after_batch_order;
    }
    Ok(records)
}

fn count_committed(responses: &[TransactionBatchApplyResponse]) -> u64 {
    responses
        .iter()
        .flat_map(|response| &response.items)
        .filter(|item| {
            item.transaction.as_ref().is_some_and(|response| {
                matches!(response.status, TransactionStatus::Committed { .. })
            })
        })
        .count()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn maximum_commit_version(responses: &[TransactionBatchApplyResponse]) -> Option<u64> {
    responses
        .iter()
        .flat_map(|response| &response.items)
        .filter_map(|item| item.transaction.as_ref().and_then(committed_version))
        .max()
}

const fn committed_version(response: &TransactionApplyResponse) -> Option<u64> {
    match response.status {
        TransactionStatus::Committed { commit_version } => Some(commit_version),
        TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => None,
    }
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

fn validate_profile(profile: &ProcessSnapshotCompactionProfile) -> Result<(), String> {
    if profile.transaction_count == 0
        || profile.transactions_per_batch == 0
        || profile.transactions_per_batch > 32
        || profile.live_keys == 0
        || profile.value_bytes == 0
        || profile.value_bytes > 64 * 1024
    {
        return Err("invalid process snapshot-compaction profile".to_owned());
    }
    Ok(())
}

const fn mode_id(mode: ProcessSnapshotCompactionMode) -> &'static str {
    mode.id()
}

fn semantic_sha<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_vec(value)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_workload_batches_are_complete_and_bounded() {
        let profile = ProcessSnapshotCompactionProfile {
            transaction_count: 1_024,
            transactions_per_batch: 32,
            live_keys: 256,
            value_bytes: 128,
        };
        let batches = workload_batches(5_501, &profile, "test");
        assert_eq!(batches.len(), 32);
        assert!(batches.iter().all(|batch| batch.len() == 32));
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 1_024);
    }

    #[test]
    fn invalid_profiles_fail_closed() {
        let invalid = ProcessSnapshotCompactionProfile {
            transaction_count: 0,
            transactions_per_batch: 33,
            live_keys: 0,
            value_bytes: 0,
        };
        assert!(validate_profile(&invalid).is_err());
    }
}
