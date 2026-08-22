use crate::{NodeId, OpenRaftLogStore, RaftEntry};
use okv_wal::JournalError;
use openraft::entry::RaftEntry as _;
use openraft::storage::RaftLogStorage;
use openraft::{CommittedLeaderId, LogId, RaftLogReader, StorageError, Vote};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Deliberately incorrect interpretations used to validate the storage gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftStorageMode {
    Correct,
    RamOnlyVote,
    RamOnlyCommitted,
    IgnoreConflictTruncate,
    IgnorePurge,
    AcceptLogGap,
    IgnoreCompleteCorruption,
}

impl RaftStorageMode {
    /// Stable configuration identifier used by the eval suite.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::RamOnlyVote => "ram_only_vote",
            Self::RamOnlyCommitted => "ram_only_committed",
            Self::IgnoreConflictTruncate => "ignore_conflict_truncate",
            Self::IgnorePurge => "ignore_purge",
            Self::AcceptLogGap => "accept_log_gap",
            Self::IgnoreCompleteCorruption => "ignore_complete_corruption",
        }
    }
}

/// Deterministic report over real stable-log writes and fresh opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftStorageReport {
    pub seed: u64,
    pub mode: RaftStorageMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub reopened_stores: u64,
    pub durable_votes: u64,
    pub durable_committed_positions: u64,
    pub appended_entries: u64,
    pub conflict_truncations: u64,
    pub purged_prefixes: u64,
    pub rejected_log_gaps: u64,
    pub torn_tail_repairs: u64,
    pub corruption_failures: u64,
    pub physical_bytes: u64,
    pub trace_sha256: String,
}

/// Execute the per-node `OpenRaft` stable-storage contract.
///
/// # Errors
///
/// Returns an error when the scenario itself cannot execute. Contract
/// mismatches are recorded as anomalies in the returned report.
pub fn run_raft_storage_contract(
    seed: u64,
    mode: RaftStorageMode,
) -> Result<RaftStorageReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(Scenario::new(seed, mode)?.run())
}

struct Scenario {
    seed: u64,
    mode: RaftStorageMode,
    root: TempRoot,
    trace: Sha256,
    step: u64,
    anomaly_count: u64,
    first_mismatch: Option<String>,
    first_mismatch_step: Option<u64>,
    reopened_stores: u64,
    durable_votes: u64,
    durable_committed_positions: u64,
    appended_entries: u64,
    conflict_truncations: u64,
    purged_prefixes: u64,
    rejected_log_gaps: u64,
    torn_tail_repairs: u64,
    corruption_failures: u64,
    physical_bytes: u64,
}

impl Scenario {
    fn new(seed: u64, mode: RaftStorageMode) -> Result<Self, String> {
        let root = TempRoot::new(seed, mode)?;
        let mut trace = Sha256::new();
        trace.update(b"okv-openraft-storage-contract-v1");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Ok(Self {
            seed,
            mode,
            root,
            trace,
            step: 0,
            anomaly_count: 0,
            first_mismatch: None,
            first_mismatch_step: None,
            reopened_stores: 0,
            durable_votes: 0,
            durable_committed_positions: 0,
            appended_entries: 0,
            conflict_truncations: 0,
            purged_prefixes: 0,
            rejected_log_gaps: 0,
            torn_tail_repairs: 0,
            corruption_failures: 0,
            physical_bytes: 0,
        })
    }

    async fn run(mut self) -> Result<RaftStorageReport, String> {
        self.hard_state_and_entries().await?;
        self.conflict_replacement().await?;
        self.purge_and_gap_rejection().await?;
        self.torn_tail_repair().await?;
        self.complete_corruption().await?;
        Ok(self.report())
    }

    async fn hard_state_and_entries(&mut self) -> Result<(), String> {
        let root = self.case_root("hard-state");
        let mut store = open_store(&root)?;
        let vote = Vote::new(3, 1);
        store.save_vote(&vote).await.map_err(stable_error)?;
        self.durable_votes += 1;
        store
            .persist_entries([entry(3, 1, 0), entry(3, 1, 1), entry(3, 1, 2)])
            .await
            .map_err(stable_error)?;
        self.appended_entries += 3;
        let committed = log_id(3, 1, 1);
        store
            .save_committed(Some(committed))
            .await
            .map_err(stable_error)?;
        self.durable_committed_positions += 1;
        self.physical_bytes = self
            .physical_bytes
            .saturating_add(store.physical_bytes().await.map_err(stable_error)?);
        drop(store);

        let mut reopened = self.reopen(&root)?;
        let recovered_vote = reopened.read_vote().await.map_err(stable_error)?;
        let vote_exact = recovered_vote == Some(vote) && self.mode != RaftStorageMode::RamOnlyVote;
        self.check(
            "durable_vote_reopen",
            vote_exact,
            &format!("recovered_vote={recovered_vote:?}"),
        );

        let recovered_committed = reopened.read_committed().await.map_err(stable_error)?;
        let committed_exact = recovered_committed == Some(committed)
            && self.mode != RaftStorageMode::RamOnlyCommitted;
        self.check(
            "durable_committed_reopen",
            committed_exact,
            &format!("recovered_committed={recovered_committed:?}"),
        );

        let recovered = reopened
            .try_get_log_entries(..)
            .await
            .map_err(stable_error)?;
        self.check(
            "consecutive_entries_reopen",
            recovered
                .iter()
                .map(|value| value.log_id.index)
                .collect::<Vec<_>>()
                == [0, 1, 2],
            &format!("entry_count={}", recovered.len()),
        );
        Ok(())
    }

    async fn conflict_replacement(&mut self) -> Result<(), String> {
        let root = self.case_root("hard-state");
        let mut store = self.reopen(&root)?;
        store
            .truncate(log_id(4, 2, 1))
            .await
            .map_err(stable_error)?;
        self.conflict_truncations += 1;
        store
            .persist_entries([entry(4, 2, 1), entry(4, 2, 2)])
            .await
            .map_err(stable_error)?;
        self.appended_entries += 2;
        drop(store);

        let mut reopened = self.reopen(&root)?;
        let recovered = reopened
            .try_get_log_entries(..)
            .await
            .map_err(stable_error)?;
        let exact = recovered.len() == 3
            && recovered[0].log_id == log_id(3, 1, 0)
            && recovered[1].log_id == log_id(4, 2, 1)
            && recovered[2].log_id == log_id(4, 2, 2)
            && self.mode != RaftStorageMode::IgnoreConflictTruncate;
        self.check(
            "conflict_suffix_replaced",
            exact,
            &format!(
                "log_ids={:?}",
                recovered
                    .iter()
                    .map(|value| value.log_id)
                    .collect::<Vec<_>>()
            ),
        );
        Ok(())
    }

    async fn purge_and_gap_rejection(&mut self) -> Result<(), String> {
        let root = self.case_root("hard-state");
        let mut store = self.reopen(&root)?;
        let purged = log_id(4, 2, 1);
        store.purge(purged).await.map_err(stable_error)?;
        self.purged_prefixes += 1;
        drop(store);

        let mut reopened = self.reopen(&root)?;
        let state = reopened.get_log_state().await.map_err(stable_error)?;
        let retained = reopened
            .try_get_log_entries(..)
            .await
            .map_err(stable_error)?;
        let exact = state.last_purged_log_id == Some(purged)
            && state.last_log_id == Some(log_id(4, 2, 2))
            && retained.len() == 1
            && retained[0].log_id.index == 2
            && self.mode != RaftStorageMode::IgnorePurge;
        self.check(
            "purged_prefix_reopen",
            exact,
            &format!(
                "purged={:?}, retained={}",
                state.last_purged_log_id,
                retained.len()
            ),
        );

        let gap_rejected = reopened.persist_entries([entry(4, 2, 4)]).await.is_err()
            && self.mode != RaftStorageMode::AcceptLogGap;
        self.rejected_log_gaps += u64::from(gap_rejected);
        self.check("log_gap_rejected", gap_rejected, "attempted_index=4,last=2");
        Ok(())
    }

    async fn torn_tail_repair(&mut self) -> Result<(), String> {
        let root = self.case_root("torn-tail");
        let mut store = open_store(&root)?;
        store
            .save_vote(&Vote::new(1, 1))
            .await
            .map_err(stable_error)?;
        let path = store.journal_path().await;
        drop(store);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|error| error.to_string())?
            .write_all(b"OKR")
            .map_err(|error| error.to_string())?;
        let mut repaired = self.reopen(&root)?;
        let repaired_tail = repaired.recovered_torn_tail().await;
        self.torn_tail_repairs += u64::from(repaired_tail);
        repaired
            .save_committed(Some(log_id(1, 1, 0)))
            .await
            .map_err(stable_error)?;
        drop(repaired);
        let mut reopened = self.reopen(&root)?;
        let readable = reopened.read_vote().await.map_err(stable_error)? == Some(Vote::new(1, 1))
            && reopened.read_committed().await.map_err(stable_error)? == Some(log_id(1, 1, 0));
        self.check(
            "torn_tail_repaired_before_append",
            repaired_tail && readable,
            &format!("repaired={repaired_tail}, readable={readable}"),
        );
        Ok(())
    }

    async fn complete_corruption(&mut self) -> Result<(), String> {
        let root = self.case_root("corruption");
        let mut store = open_store(&root)?;
        store
            .save_vote(&Vote::new(1, 1))
            .await
            .map_err(stable_error)?;
        let path = store.journal_path().await;
        self.physical_bytes = self
            .physical_bytes
            .saturating_add(store.physical_bytes().await.map_err(stable_error)?);
        drop(store);
        let mut bytes = fs::read(&path).map_err(|error| error.to_string())?;
        let last = bytes
            .last_mut()
            .ok_or_else(|| "corruption fixture journal was empty".to_owned())?;
        *last ^= 0xff;
        fs::write(&path, bytes).map_err(|error| error.to_string())?;
        let failed_closed = matches!(
            OpenRaftLogStore::open(&root),
            Err(JournalError::CorruptFrame { .. })
        ) && self.mode != RaftStorageMode::IgnoreCompleteCorruption;
        self.corruption_failures += u64::from(failed_closed);
        self.check(
            "complete_corruption_fails_closed",
            failed_closed,
            &format!("mode={}", self.mode.id()),
        );
        Ok(())
    }

    fn reopen(&mut self, root: &PathBuf) -> Result<OpenRaftLogStore, String> {
        self.reopened_stores = self.reopened_stores.saturating_add(1);
        open_store(root)
    }

    fn check(&mut self, name: &str, passed: bool, detail: &str) {
        self.step = self.step.saturating_add(1);
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(name.as_bytes());
        self.trace.update([u8::from(passed)]);
        self.trace.update(detail.as_bytes());
        if !passed && self.first_mismatch.is_none() {
            self.first_mismatch_step = Some(self.step);
            self.first_mismatch = Some(format!("{name}: {detail}"));
        }
        self.anomaly_count = self.anomaly_count.saturating_add(u64::from(!passed));
    }

    fn case_root(&self, name: &str) -> PathBuf {
        self.root.0.join(name)
    }

    fn report(self) -> RaftStorageReport {
        RaftStorageReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: self.anomaly_count,
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch,
            reopened_stores: self.reopened_stores,
            durable_votes: self.durable_votes,
            durable_committed_positions: self.durable_committed_positions,
            appended_entries: self.appended_entries,
            conflict_truncations: self.conflict_truncations,
            purged_prefixes: self.purged_prefixes,
            rejected_log_gaps: self.rejected_log_gaps,
            torn_tail_repairs: self.torn_tail_repairs,
            corruption_failures: self.corruption_failures,
            physical_bytes: self.physical_bytes,
            trace_sha256: format!("{:x}", self.trace.finalize()),
        }
    }
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: RaftStorageMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-raft-storage-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn open_store(root: &PathBuf) -> Result<OpenRaftLogStore, String> {
    OpenRaftLogStore::open(root).map_err(|error| error.to_string())
}

fn entry(term: u64, node_id: NodeId, index: u64) -> RaftEntry {
    RaftEntry::new_blank(log_id(term, node_id, index))
}

fn log_id(term: u64, node_id: NodeId, index: u64) -> LogId<NodeId> {
    LogId::new(CommittedLeaderId::new(term, node_id), index)
}

#[allow(clippy::needless_pass_by_value)]
fn stable_error(error: StorageError<NodeId>) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_storage_contract_has_no_anomalies_and_replays_exactly() {
        let first = run_raft_storage_contract(1103, RaftStorageMode::Correct).unwrap();
        let second = run_raft_storage_contract(1103, RaftStorageMode::Correct).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.executed_steps, 8);
        assert_eq!(first.anomaly_count, 0);
        assert!(first.reopened_stores >= 6);
        assert_eq!(first.torn_tail_repairs, 1);
        assert_eq!(first.corruption_failures, 1);
    }

    #[test]
    fn every_negative_mode_breaks_one_bounded_contract() {
        for mode in [
            RaftStorageMode::RamOnlyVote,
            RaftStorageMode::RamOnlyCommitted,
            RaftStorageMode::IgnoreConflictTruncate,
            RaftStorageMode::IgnorePurge,
            RaftStorageMode::AcceptLogGap,
            RaftStorageMode::IgnoreCompleteCorruption,
        ] {
            let report = run_raft_storage_contract(1103, mode).unwrap();
            assert_eq!(report.anomaly_count, 1, "mode={}", mode.id());
            assert!(report.first_mismatch_step.is_some(), "mode={}", mode.id());
        }
    }
}
