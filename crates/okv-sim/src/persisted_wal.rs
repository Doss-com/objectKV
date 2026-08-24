use crate::commit::{fixture_envelope, CommitEnvelope};
use okv_model::Version;
use okv_wal::{LocalReplicatedWal, Recovery, WalError, FRAME_HEADER_BYTES};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const GENERATION: u64 = 3;
const REPLICA_COUNT: u8 = 3;
const QUORUM: usize = 2;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
type ClientKey = ([u8; 16], u64);
type RecoveredOutcome = ([u8; 32], Version);
type OutcomeMap = BTreeMap<ClientKey, RecoveredOutcome>;

/// Deliberately incorrect recovery behavior used to prove one persisted-WAL invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistedWalMode {
    Correct,
    RamOnlyDedup,
    AckBeforeQuorum,
    TrustSingleReplica,
    AcceptTornAsCommit,
    SkipLogChainValidation,
    IgnoreCompleteCorruption,
}

impl PersistedWalMode {
    /// Stable configuration identifier used by the eval suite.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::RamOnlyDedup => "ram_only_dedup",
            Self::AckBeforeQuorum => "ack_before_quorum",
            Self::TrustSingleReplica => "trust_single_replica",
            Self::AcceptTornAsCommit => "accept_torn_as_commit",
            Self::SkipLogChainValidation => "skip_log_chain_validation",
            Self::IgnoreCompleteCorruption => "ignore_complete_corruption",
        }
    }
}

/// Deterministic report over real local file writes and fresh WAL opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedWalReport {
    pub seed: u64,
    pub mode: PersistedWalMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub quorum_appends: u64,
    pub recovered_records: u64,
    pub reopened_wals: u64,
    pub recovered_outcomes: u64,
    pub leader_only_attempts: u64,
    pub torn_tail_replicas: u64,
    pub corruption_failures: u64,
    pub physical_bytes: u64,
    pub trace_sha256: String,
}

struct Scenario {
    seed: u64,
    mode: PersistedWalMode,
    root: TempRoot,
    trace: Sha256,
    step: u64,
    first_mismatch: Option<String>,
    first_mismatch_step: Option<u64>,
    quorum_appends: u64,
    recovered_records: u64,
    reopened_wals: u64,
    recovered_outcomes: u64,
    leader_only_attempts: u64,
    torn_tail_replicas: u64,
    corruption_failures: u64,
    physical_bytes: u64,
}

impl Scenario {
    fn new(seed: u64, mode: PersistedWalMode) -> Result<Self, String> {
        let root = TempRoot::new(seed, mode)?;
        let mut trace = Sha256::new();
        trace.update(b"okv-persisted-wal-contract-v1");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Ok(Self {
            seed,
            mode,
            root,
            trace,
            step: 0,
            first_mismatch: None,
            first_mismatch_step: None,
            quorum_appends: 0,
            recovered_records: 0,
            reopened_wals: 0,
            recovered_outcomes: 0,
            leader_only_attempts: 0,
            torn_tail_replicas: 0,
            corruption_failures: 0,
            physical_bytes: 0,
        })
    }

    fn run(&mut self) -> Result<(), String> {
        self.quorum_append()?;
        self.reopen_recovery()?;
        self.durable_retry_outcome()?;
        self.leader_only_suffix()?;
        self.torn_suffix()?;
        self.log_chain_mismatch()?;
        self.complete_corruption()?;
        Ok(())
    }

    fn quorum_append(&mut self) -> Result<(), String> {
        let root = self.case_root("quorum");
        let wal = open_wal(&root)?;
        let envelope = fixture_envelope(self.seed, 1, GENERATION, 1, 1, [0; 32]);
        let outcome = wal
            .append(1, &envelope.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        self.quorum_appends += u64::from(outcome.quorum_durable);
        self.check(
            "quorum_fsync",
            outcome.quorum_durable && outcome.synced_replicas == [0, 1, 2],
            &format!(
                "quorum={}, replicas={:?}",
                outcome.quorum_durable, outcome.synced_replicas
            ),
        );
        Ok(())
    }

    fn reopen_recovery(&mut self) -> Result<(), String> {
        let root = self.case_root("quorum");
        let recovery = reopen(&root, &mut self.reopened_wals)?;
        self.record_recovery(&recovery);
        let decoded = validate_envelope_chain(&recovery);
        self.check(
            "fresh_open_recovery",
            matches!(&decoded, Ok(outcomes) if outcomes.len() == 1) && recovery.last_index() == 1,
            &format!(
                "last_index={}, outcomes={}",
                recovery.last_index(),
                decoded.as_ref().map_or(0, BTreeMap::len)
            ),
        );
        Ok(())
    }

    fn durable_retry_outcome(&mut self) -> Result<(), String> {
        let root = self.case_root("quorum");
        let wal = open_wal(&root)?;
        let first = fixture_envelope(self.seed, 1, GENERATION, 1, 1, [0; 32]);
        let second = fixture_envelope(self.seed, 2, GENERATION, 2, 2, digest(&first.encode()));
        let outcome = wal
            .append(2, &second.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        self.quorum_appends += u64::from(outcome.quorum_durable);
        clear_replica(&wal, 2)?;
        drop(wal);

        let recovery = reopen(&root, &mut self.reopened_wals)?;
        self.record_recovery(&recovery);
        let mut outcomes = validate_envelope_chain(&recovery).map_err(str::to_owned)?;
        if self.mode == PersistedWalMode::RamOnlyDedup {
            outcomes.clear();
        }
        let recovered = outcomes.get(&second.client_identity()).copied();
        self.recovered_outcomes = u64::try_from(outcomes.len()).unwrap_or(u64::MAX);
        self.check(
            "durable_retry_outcome",
            recovered == Some((second.logical_fingerprint(), second.version())),
            &format!(
                "outcomes={}, recovered_original={}",
                outcomes.len(),
                recovered.is_some()
            ),
        );
        Ok(())
    }

    fn leader_only_suffix(&mut self) -> Result<(), String> {
        let root = self.case_root("leader-only");
        let wal = open_wal(&root)?;
        let first = fixture_envelope(self.seed, 3, GENERATION, 1, 1, [0; 32]);
        let second = fixture_envelope(self.seed, 4, GENERATION, 2, 2, digest(&first.encode()));
        wal.append(1, &first.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        let append = wal
            .append(2, &second.encode(), &[0])
            .map_err(stable_error)?;
        self.leader_only_attempts += 1;
        drop(wal);
        let recovery = reopen(&root, &mut self.reopened_wals)?;
        self.record_recovery(&recovery);

        let acknowledged = append.quorum_durable || self.mode == PersistedWalMode::AckBeforeQuorum;
        let recovered =
            recovery.last_index() >= 2 || self.mode == PersistedWalMode::TrustSingleReplica;
        self.check(
            "leader_only_not_committed",
            !acknowledged && !recovered,
            &format!(
                "acknowledged={acknowledged}, recovered={recovered}, ignored={}",
                recovery.ignored_uncommitted_records
            ),
        );
        Ok(())
    }

    fn torn_suffix(&mut self) -> Result<(), String> {
        let root = self.case_root("torn-tail");
        let wal = open_wal(&root)?;
        let envelope = fixture_envelope(self.seed, 5, GENERATION, 1, 1, [0; 32]);
        wal.append(1, &envelope.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        append_torn_tail(&wal, 0)?;
        drop(wal);
        let recovery = reopen(&root, &mut self.reopened_wals)?;
        self.record_recovery(&recovery);
        let reports_phantom = self.mode == PersistedWalMode::AcceptTornAsCommit;
        self.check(
            "torn_suffix_not_committed",
            recovery.last_index() == 1 && recovery.torn_tail_replicas == [0] && !reports_phantom,
            &format!(
                "last_index={}, torn={:?}, phantom={reports_phantom}",
                recovery.last_index(),
                recovery.torn_tail_replicas
            ),
        );
        Ok(())
    }

    fn log_chain_mismatch(&mut self) -> Result<(), String> {
        let root = self.case_root("bad-chain");
        let wal = open_wal(&root)?;
        let first = fixture_envelope(self.seed, 6, GENERATION, 1, 1, [0; 32]);
        let second = fixture_envelope(self.seed, 7, GENERATION, 2, 2, [0; 32]);
        wal.append(1, &first.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        wal.append(2, &second.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        drop(wal);
        let recovery = reopen(&root, &mut self.reopened_wals)?;
        self.record_recovery(&recovery);
        let validation = validate_envelope_chain(&recovery);
        let rejected = validation.is_err() && self.mode != PersistedWalMode::SkipLogChainValidation;
        self.check(
            "log_chain_mismatch_rejected",
            rejected,
            &format!(
                "validator_error={}, skip_validation={}",
                validation.is_err(),
                self.mode == PersistedWalMode::SkipLogChainValidation
            ),
        );
        Ok(())
    }

    fn complete_corruption(&mut self) -> Result<(), String> {
        let root = self.case_root("corruption");
        let wal = open_wal(&root)?;
        let envelope = fixture_envelope(self.seed, 8, GENERATION, 1, 1, [0; 32]);
        wal.append(1, &envelope.encode(), &[0, 1, 2])
            .map_err(stable_error)?;
        corrupt_payload(&wal, 1)?;
        clear_replica(&wal, 2)?;
        drop(wal);
        self.reopened_wals += 1;
        let result = open_wal(&root)?.recover();
        let failed_closed = matches!(
            result,
            Err(WalError::CompleteFrameCorruption {
                replica_id: 1,
                log_index: 1
            })
        );
        self.corruption_failures += u64::from(failed_closed);
        let accepted = self.mode == PersistedWalMode::IgnoreCompleteCorruption;
        self.check(
            "complete_corruption_fails_closed",
            failed_closed && !accepted,
            &format!("failed_closed={failed_closed}, accepted={accepted}"),
        );
        Ok(())
    }

    fn case_root(&self, label: &str) -> PathBuf {
        self.root.path().join(label)
    }

    fn record_recovery(&mut self, recovery: &Recovery) {
        self.recovered_records = self
            .recovered_records
            .saturating_add(u64::try_from(recovery.records.len()).unwrap_or(u64::MAX));
        self.torn_tail_replicas = self
            .torn_tail_replicas
            .saturating_add(u64::try_from(recovery.torn_tail_replicas.len()).unwrap_or(u64::MAX));
        self.physical_bytes = self.physical_bytes.max(recovery.physical_bytes);
    }

    fn check(&mut self, action: &str, passed: bool, detail: &str) {
        self.step += 1;
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(action.as_bytes());
        self.trace.update([u8::from(passed)]);
        self.trace.update(detail.as_bytes());
        if !passed && self.first_mismatch.is_none() {
            self.first_mismatch_step = Some(self.step);
            self.first_mismatch = Some(format!("{action}: {detail}"));
        }
    }

    fn report(&self) -> PersistedWalReport {
        PersistedWalReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: u64::from(self.first_mismatch.is_some()),
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch.clone(),
            quorum_appends: self.quorum_appends,
            recovered_records: self.recovered_records,
            reopened_wals: self.reopened_wals,
            recovered_outcomes: self.recovered_outcomes,
            leader_only_attempts: self.leader_only_attempts,
            torn_tail_replicas: self.torn_tail_replicas,
            corruption_failures: self.corruption_failures,
            physical_bytes: self.physical_bytes,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

/// Exercise checksummed local WAL persistence and quorum recovery.
///
/// # Errors
///
/// Returns a stable error when the filesystem fixture itself cannot run.
pub fn run_persisted_wal_contract(
    seed: u64,
    mode: PersistedWalMode,
) -> Result<PersistedWalReport, String> {
    let mut scenario = Scenario::new(seed, mode)?;
    scenario.run()?;
    Ok(scenario.report())
}

fn open_wal(path: &Path) -> Result<LocalReplicatedWal, String> {
    LocalReplicatedWal::open(path, REPLICA_COUNT, QUORUM).map_err(stable_error)
}

fn reopen(path: &Path, counter: &mut u64) -> Result<Recovery, String> {
    *counter = counter.saturating_add(1);
    open_wal(path)?.recover().map_err(stable_error)
}

fn validate_envelope_chain(recovery: &Recovery) -> Result<OutcomeMap, &'static str> {
    let mut previous_chain = [0; 32];
    let mut outcomes = BTreeMap::new();
    for record in &recovery.records {
        let envelope = CommitEnvelope::decode(&record.payload).map_err(|_| "invalid envelope")?;
        if envelope.generation() != GENERATION
            || envelope.log_index() != record.log_index
            || envelope.previous_log_chain() != previous_chain
        {
            return Err("invalid envelope chain");
        }
        previous_chain = digest(&record.payload);
        let old = outcomes.insert(
            envelope.client_identity(),
            (envelope.logical_fingerprint(), envelope.version()),
        );
        if old.is_some() {
            return Err("duplicate client identity");
        }
    }
    Ok(outcomes)
}

fn append_torn_tail(wal: &LocalReplicatedWal, replica_id: u8) -> Result<(), String> {
    let path = wal
        .replica_path(replica_id)
        .ok_or_else(|| "unknown replica".to_owned())?;
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|_| "open fault fixture failed".to_owned())?;
    file.write_all(b"OKV")
        .map_err(|_| "write fault fixture failed".to_owned())?;
    file.sync_all()
        .map_err(|_| "sync fault fixture failed".to_owned())
}

fn corrupt_payload(wal: &LocalReplicatedWal, replica_id: u8) -> Result<(), String> {
    let path = wal
        .replica_path(replica_id)
        .ok_or_else(|| "unknown replica".to_owned())?;
    let mut bytes = fs::read(&path).map_err(|_| "read fault fixture failed".to_owned())?;
    let byte = bytes
        .get_mut(FRAME_HEADER_BYTES)
        .ok_or_else(|| "fault fixture frame missing".to_owned())?;
    *byte ^= 0xff;
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|_| "open fault fixture failed".to_owned())?;
    file.write_all(&bytes)
        .map_err(|_| "write fault fixture failed".to_owned())?;
    file.sync_all()
        .map_err(|_| "sync fault fixture failed".to_owned())
}

fn clear_replica(wal: &LocalReplicatedWal, replica_id: u8) -> Result<(), String> {
    let path = wal
        .replica_path(replica_id)
        .ok_or_else(|| "unknown replica".to_owned())?;
    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|_| "open fault fixture failed".to_owned())?;
    file.sync_all()
        .map_err(|_| "sync fault fixture failed".to_owned())
}

fn stable_error(error: WalError) -> String {
    let message = match &error {
        WalError::InvalidTopology => "invalid topology",
        WalError::InvalidLogIndex(_) => "invalid log index",
        WalError::PayloadTooLarge(_) => "payload too large",
        WalError::Io(_) => "filesystem operation failed",
        WalError::CompleteFrameCorruption { .. } => "complete frame corruption",
        WalError::MissingContiguousQuorum(_) => "missing contiguous quorum",
        WalError::ConflictingQuorum(_) => "conflicting quorum",
    };
    drop(error);
    message.to_owned()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: PersistedWalMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-persisted-wal-{}-{seed}-{}-{sequence}",
            std::process::id(),
            mode.id()
        ));
        fs::create_dir_all(&path).map_err(|_| "create temp root failed".to_owned())?;
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
    fn correct_persisted_wal_contract_is_exactly_replayable() {
        let first = run_persisted_wal_contract(1103, PersistedWalMode::Correct).unwrap();
        let second = run_persisted_wal_contract(1103, PersistedWalMode::Correct).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert_eq!(first.executed_steps, 7);
        assert!(first.quorum_appends > 0);
        assert!(first.recovered_records > 0);
        assert!(first.reopened_wals > 0);
        assert!(first.recovered_outcomes > 0);
        assert_eq!(first.leader_only_attempts, 1);
        assert!(first.torn_tail_replicas > 0);
        assert_eq!(first.corruption_failures, 1);
    }

    #[test]
    fn every_persisted_wal_negative_control_has_a_bounded_failure() {
        let controls = [
            (PersistedWalMode::RamOnlyDedup, 3),
            (PersistedWalMode::AckBeforeQuorum, 4),
            (PersistedWalMode::TrustSingleReplica, 4),
            (PersistedWalMode::AcceptTornAsCommit, 5),
            (PersistedWalMode::SkipLogChainValidation, 6),
            (PersistedWalMode::IgnoreCompleteCorruption, 7),
        ];
        for (mode, expected_step) in controls {
            let report = run_persisted_wal_contract(1103, mode).unwrap();
            assert_eq!(report.anomaly_count, 1, "{}", mode.id());
            assert_eq!(
                report.first_mismatch_step,
                Some(expected_step),
                "{}: {:?}",
                mode.id(),
                report.first_mismatch
            );
        }
    }
}
