use super::{
    ApplyOutcome, CommitBatch, CommitIdentity, KeyRange, Model, ModelError, Mutation, Row, Version,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Subject behavior used to prove that each differential gate detects a bug.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DifferentialMode {
    Correct,
    IgnoreRangeClears,
    MutationOrderAffectsReplay,
    AcceptConflictingReplay,
    FutureReadFallsBack,
    RejectRetentionBoundary,
    ServeExpiredRead,
    AcceptStaleGeneration,
}

impl DifferentialMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::IgnoreRangeClears => "ignore_range_clears",
            Self::MutationOrderAffectsReplay => "mutation_order_affects_replay",
            Self::AcceptConflictingReplay => "accept_conflicting_replay",
            Self::FutureReadFallsBack => "future_read_falls_back",
            Self::RejectRetentionBoundary => "reject_retention_boundary",
            Self::ServeExpiredRead => "serve_expired_read",
            Self::AcceptStaleGeneration => "accept_stale_generation",
        }
    }
}

/// Deterministic generated-history result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialReport {
    pub seed: u64,
    pub requested_steps: u64,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub range_clear_count: u64,
    pub exact_replay_count: u64,
    pub conflicting_replay_count: u64,
    pub future_read_count: u64,
    pub retention_count: u64,
    pub too_old_read_count: u64,
    pub historical_read_count: u64,
    pub stale_generation_count: u64,
    pub read_count: u64,
    pub trace_sha256: String,
}

/// Run one generated MVCC history against an independent full-snapshot oracle.
///
/// A deterministic prelude exercises every semantic operation before random
/// scheduling begins, so each negative control has a bounded failing prefix.
#[must_use]
pub fn run_differential_history(
    seed: u64,
    requested_steps: u64,
    mode: DifferentialMode,
) -> DifferentialReport {
    let mut runner = Runner::new(seed, requested_steps, mode);
    runner.run();
    runner.report()
}

struct Runner {
    requested_steps: u64,
    mode: DifferentialMode,
    rng: SplitMix64,
    subject: Model,
    oracle: NaiveOracle,
    trace: Sha256,
    executed_steps: u64,
    first_mismatch: Option<String>,
    first_mismatch_step: Option<u64>,
    range_clear_count: u64,
    exact_replay_count: u64,
    conflicting_replay_count: u64,
    future_read_count: u64,
    retention_count: u64,
    too_old_read_count: u64,
    historical_read_count: u64,
    stale_generation_count: u64,
    read_count: u64,
    generation: u64,
    sequence: u64,
    first_version: Option<Version>,
    last_batch: Option<CommitBatch>,
}

impl Runner {
    fn new(seed: u64, requested_steps: u64, mode: DifferentialMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-generated-history-v2");
        trace.update(seed.to_be_bytes());
        trace.update(requested_steps.to_be_bytes());
        Self {
            requested_steps,
            mode,
            rng: SplitMix64::new(seed),
            subject: Model::default(),
            oracle: NaiveOracle::default(),
            trace,
            executed_steps: 0,
            first_mismatch: None,
            first_mismatch_step: None,
            range_clear_count: 0,
            exact_replay_count: 0,
            conflicting_replay_count: 0,
            future_read_count: 0,
            retention_count: 0,
            too_old_read_count: 0,
            historical_read_count: 0,
            stale_generation_count: 0,
            read_count: 0,
            generation: 1,
            sequence: 0,
            first_version: None,
            last_batch: None,
        }
    }

    fn run(&mut self) {
        self.prelude();
        while self.can_continue() {
            let step = self.executed_steps;
            if step % 97 == 0 {
                self.conflicting_replay();
            } else if step % 89 == 0 {
                self.stale_generation_apply();
            } else if step % 83 == 0 {
                self.future_read();
            } else if step % 79 == 0 {
                self.retain_latest();
            } else if step % 73 == 0 {
                self.too_old_read();
            } else if step % 67 == 0 {
                self.exact_replay();
            } else if step % 61 == 0 {
                self.historical_read();
            } else {
                let mutations = self.generated_mutations();
                self.commit(mutations);
            }
        }
    }

    fn prelude(&mut self) {
        if self.can_continue() {
            self.commit(vec![
                Mutation::Set {
                    key: key(0),
                    value: b"prelude-a".to_vec(),
                },
                Mutation::Set {
                    key: key(1),
                    value: b"prelude-b".to_vec(),
                },
            ]);
        }
        if self.can_continue() {
            self.commit(vec![
                Mutation::ClearRange {
                    range: KeyRange::new(key(0), key(2)).expect("valid fixed range"),
                },
                Mutation::Set {
                    key: key(1),
                    value: b"point-wins".to_vec(),
                },
            ]);
        }
        if self.can_continue() {
            self.exact_replay();
        }
        if self.can_continue() {
            self.conflicting_replay();
        }
        if self.can_continue() {
            self.future_read();
        }
        if self.can_continue() {
            self.retain_latest();
        }
        if self.can_continue() {
            self.retention_boundary_read();
        }
        if self.can_continue() {
            self.too_old_read();
        }
        if self.can_continue() {
            self.stale_generation_apply();
        }
    }

    fn can_continue(&self) -> bool {
        self.executed_steps < self.requested_steps && self.first_mismatch.is_none()
    }

    fn next_version(&mut self) -> Version {
        self.sequence += if self.executed_steps > 0 && self.executed_steps % 17 == 0 {
            2
        } else {
            1
        };
        if self.sequence > 256 {
            self.generation += 1;
            self.sequence = 1;
        }
        Version::from_parts(self.generation, self.sequence)
    }

    fn generated_mutations(&mut self) -> Vec<Mutation> {
        let selected = self.rng.bounded(10);
        let index = u8::try_from(self.rng.bounded(16)).expect("bounded to u8");
        match selected {
            0..=4 => vec![Mutation::Set {
                key: key(index),
                value: format!("v-{}-{}", self.generation, self.sequence + 1).into_bytes(),
            }],
            5..=7 => vec![Mutation::Clear { key: key(index) }],
            _ => {
                let start = u8::try_from(self.rng.bounded(14)).expect("bounded to u8");
                let width = 1 + u8::try_from(self.rng.bounded(u64::from(16 - start - 1)))
                    .expect("bounded to u8");
                let range = KeyRange::new(key(start), key(start + width)).expect("generated range");
                if selected == 9 {
                    vec![
                        Mutation::ClearRange { range },
                        Mutation::Set {
                            key: key(start),
                            value: b"same-version-point".to_vec(),
                        },
                    ]
                } else {
                    vec![Mutation::ClearRange { range }]
                }
            }
        }
    }

    fn commit(&mut self, mutations: Vec<Mutation>) {
        let version = self.next_version();
        let batch = CommitBatch {
            version,
            identity: self.identity(self.executed_steps + 1),
            mutations,
        };
        self.range_clear_count += count_range_clears(&batch);
        self.trace_batch(b'C', &batch);
        let expected = self.oracle.apply(&batch);
        let actual = self.subject.apply(self.subject_batch(batch.clone()));
        self.executed_steps += 1;
        self.compare_apply("commit", &expected, &actual);
        if self.first_mismatch.is_none() {
            self.compare_snapshot(version);
        }
        self.first_version.get_or_insert(version);
        self.last_batch = Some(batch);
    }

    fn exact_replay(&mut self) {
        let Some(mut batch) = self.last_batch.clone() else {
            return;
        };
        batch.mutations.reverse();
        self.trace_batch(b'R', &batch);
        let expected = self.oracle.apply(&batch);
        let mut actual = self.subject.apply(self.subject_batch(batch.clone()));
        if self.mode == DifferentialMode::MutationOrderAffectsReplay && batch.mutations.len() > 1 {
            actual = Err(ModelError::ConflictingReplay {
                version: batch.version,
            });
        }
        self.exact_replay_count += 1;
        self.executed_steps += 1;
        self.compare_apply("exact replay", &expected, &actual);
    }

    fn conflicting_replay(&mut self) {
        let Some(mut batch) = self.last_batch.clone() else {
            return;
        };
        batch.identity.request_id = batch.identity.request_id.wrapping_add(1);
        self.trace_batch(b'X', &batch);
        let expected = self.oracle.apply(&batch);
        let mut actual = self.subject.apply(self.subject_batch(batch));
        if self.mode == DifferentialMode::AcceptConflictingReplay
            && matches!(actual, Err(ModelError::ConflictingReplay { .. }))
        {
            actual = Ok(ApplyOutcome::AlreadyApplied);
        }
        self.conflicting_replay_count += 1;
        self.executed_steps += 1;
        self.compare_apply("conflicting replay", &expected, &actual);
    }

    fn future_read(&mut self) {
        let requested = next_after(self.subject.latest_version());
        self.trace_read(b'F', requested, &key(0));
        let expected = self.oracle.get(&key(0), requested);
        let actual = self.subject_get(&key(0), requested);
        self.future_read_count += 1;
        self.read_count += 1;
        self.executed_steps += 1;
        self.compare_read("future read", requested, &expected, &actual);
    }

    fn retain_latest(&mut self) {
        let boundary = self.subject.latest_version();
        self.trace.update(b"T");
        self.trace.update(boundary.to_be_bytes());
        let expected = self.oracle.retain_from(boundary);
        let actual = self.subject.retain_from(boundary);
        self.retention_count += 1;
        self.executed_steps += 1;
        if expected != actual {
            self.mismatch(format!(
                "retention at {boundary}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }

    fn retention_boundary_read(&mut self) {
        let requested = self.subject.oldest_readable_version();
        self.historical_read_count += 1;
        self.executed_steps += 1;
        self.compare_snapshot(requested);
    }

    fn too_old_read(&mut self) {
        let Some(requested) = self.first_version else {
            return;
        };
        if requested >= self.subject.oldest_readable_version() {
            self.historical_read();
            return;
        }
        self.trace_read(b'O', requested, &key(1));
        let expected = self.oracle.get(&key(1), requested);
        let actual = self.subject_get(&key(1), requested);
        self.too_old_read_count += 1;
        self.read_count += 1;
        self.executed_steps += 1;
        self.compare_read("expired read", requested, &expected, &actual);
    }

    fn historical_read(&mut self) {
        let latest = self.subject.latest_version();
        let oldest = self.subject.oldest_readable_version();
        let previous =
            Version::from_parts(latest.generation(), latest.sequence().saturating_sub(1));
        let requested = if previous >= oldest { previous } else { oldest };
        self.historical_read_count += 1;
        self.executed_steps += 1;
        self.compare_snapshot(requested);
    }

    fn stale_generation_apply(&mut self) {
        let latest = self.subject.latest_version();
        let stale = Version::from_parts(latest.generation().saturating_sub(1), u64::MAX);
        let batch = CommitBatch {
            version: stale,
            identity: self.identity(self.executed_steps + 1),
            mutations: vec![Mutation::Set {
                key: key(15),
                value: b"stale-generation".to_vec(),
            }],
        };
        self.trace_batch(b'G', &batch);
        let expected = self.oracle.apply(&batch);
        let mut actual = self.subject.apply(batch);
        if self.mode == DifferentialMode::AcceptStaleGeneration
            && matches!(actual, Err(ModelError::NonMonotonicVersion { .. }))
        {
            actual = Ok(ApplyOutcome::Applied);
        }
        self.stale_generation_count += 1;
        self.executed_steps += 1;
        self.compare_apply("stale-generation apply", &expected, &actual);
    }

    fn compare_snapshot(&mut self, version: Version) {
        for index in 0..16_u8 {
            let candidate = key(index);
            let expected = self.oracle.get(&candidate, version);
            let actual = self.subject_get(&candidate, version);
            self.read_count += 1;
            self.trace_read(b'P', version, &candidate);
            if expected != actual {
                self.mismatch(format!(
                    "get {candidate:?} at {version}: expected {expected:?}, actual {actual:?}"
                ));
                return;
            }
        }
        let range = KeyRange::new(key(0), key(16)).expect("fixed range");
        let expected = self.oracle.scan(&range, version);
        let actual = self.subject_scan(&range, version);
        self.read_count += 1;
        self.trace.update(b"S");
        self.trace.update(version.to_be_bytes());
        if expected != actual {
            self.mismatch(format!(
                "scan at {version}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }

    fn subject_get(&self, key: &[u8], requested: Version) -> Result<Option<Vec<u8>>, ModelError> {
        let actual = self
            .subject
            .get(key, requested)
            .map(|value| value.map(<[u8]>::to_vec));
        match (&self.mode, &actual) {
            (DifferentialMode::FutureReadFallsBack, Err(ModelError::VersionNotApplied { .. })) => {
                self.subject
                    .get(key, self.subject.latest_version())
                    .map(|value| value.map(<[u8]>::to_vec))
            }
            (DifferentialMode::ServeExpiredRead, Err(ModelError::VersionTooOld { .. })) => Ok(None),
            (DifferentialMode::RejectRetentionBoundary, _)
                if requested == self.subject.oldest_readable_version() =>
            {
                Err(ModelError::VersionTooOld {
                    oldest: requested,
                    requested,
                })
            }
            _ => actual,
        }
    }

    fn subject_scan(&self, range: &KeyRange, requested: Version) -> Result<Vec<Row>, ModelError> {
        let actual = self.subject.scan(range, requested);
        match (&self.mode, &actual) {
            (DifferentialMode::FutureReadFallsBack, Err(ModelError::VersionNotApplied { .. })) => {
                self.subject.scan(range, self.subject.latest_version())
            }
            (DifferentialMode::ServeExpiredRead, Err(ModelError::VersionTooOld { .. })) => {
                Ok(Vec::new())
            }
            (DifferentialMode::RejectRetentionBoundary, _)
                if requested == self.subject.oldest_readable_version() =>
            {
                Err(ModelError::VersionTooOld {
                    oldest: requested,
                    requested,
                })
            }
            _ => actual,
        }
    }

    fn subject_batch(&self, mut batch: CommitBatch) -> CommitBatch {
        if self.mode == DifferentialMode::IgnoreRangeClears {
            batch
                .mutations
                .retain(|mutation| !matches!(mutation, Mutation::ClearRange { .. }));
        }
        batch
    }

    fn compare_apply(
        &mut self,
        operation: &str,
        expected: &Result<ApplyOutcome, ModelError>,
        actual: &Result<ApplyOutcome, ModelError>,
    ) {
        if expected != actual {
            self.mismatch(format!(
                "{operation}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }

    fn compare_read(
        &mut self,
        operation: &str,
        requested: Version,
        expected: &Result<Option<Vec<u8>>, ModelError>,
        actual: &Result<Option<Vec<u8>>, ModelError>,
    ) {
        if expected != actual {
            self.mismatch(format!(
                "{operation} at {requested}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }

    fn identity(&mut self, request_id: u64) -> CommitIdentity {
        CommitIdentity::new(
            self.rng
                .next()
                .to_be_bytes()
                .repeat(2)
                .try_into()
                .expect("16 bytes"),
            request_id,
        )
    }

    fn trace_batch(&mut self, operation: u8, batch: &CommitBatch) {
        self.trace.update([operation]);
        self.trace.update(batch.version.to_be_bytes());
        self.trace
            .update(batch.fingerprint().expect("generated batch is valid"));
    }

    fn trace_read(&mut self, operation: u8, version: Version, key: &[u8]) {
        self.trace.update([operation]);
        self.trace.update(version.to_be_bytes());
        self.trace.update(key);
    }

    fn mismatch(&mut self, detail: String) {
        if self.first_mismatch.is_none() {
            self.first_mismatch_step = Some(self.executed_steps);
            self.first_mismatch = Some(detail);
        }
    }

    fn report(&self) -> DifferentialReport {
        DifferentialReport {
            seed: self.rng.seed,
            requested_steps: self.requested_steps,
            executed_steps: self.executed_steps,
            anomaly_count: u64::from(self.first_mismatch.is_some()),
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch.clone(),
            range_clear_count: self.range_clear_count,
            exact_replay_count: self.exact_replay_count,
            conflicting_replay_count: self.conflicting_replay_count,
            future_read_count: self.future_read_count,
            retention_count: self.retention_count,
            too_old_read_count: self.too_old_read_count,
            historical_read_count: self.historical_read_count,
            stale_generation_count: self.stale_generation_count,
            read_count: self.read_count,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OracleMutation {
    ClearRange(Vec<u8>, Vec<u8>),
    Clear(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
}

#[derive(Default)]
struct NaiveOracle {
    snapshots: BTreeMap<Version, BTreeMap<Vec<u8>, Vec<u8>>>,
    commits: BTreeMap<Version, (CommitIdentity, Vec<OracleMutation>)>,
    latest: Version,
    oldest_readable: Version,
}

impl NaiveOracle {
    fn apply(&mut self, batch: &CommitBatch) -> Result<ApplyOutcome, ModelError> {
        let signature = oracle_signature(&batch.mutations);
        if let Some((identity, existing)) = self.commits.get(&batch.version) {
            return if identity == &batch.identity && existing == &signature {
                Ok(ApplyOutcome::AlreadyApplied)
            } else {
                Err(ModelError::ConflictingReplay {
                    version: batch.version,
                })
            };
        }
        if batch.version == Version::ZERO {
            return Err(ModelError::ZeroVersion);
        }
        if batch.version <= self.latest {
            return Err(ModelError::NonMonotonicVersion {
                latest: self.latest,
                attempted: batch.version,
            });
        }
        let mut snapshot = self.snapshot_at(self.latest).cloned().unwrap_or_default();
        for mutation in &batch.mutations {
            if let Mutation::ClearRange { range } = mutation {
                snapshot.retain(|key, _| !range.contains(key));
            }
        }
        for mutation in &batch.mutations {
            match mutation {
                Mutation::Set { key, value } => {
                    snapshot.insert(key.clone(), value.clone());
                }
                Mutation::Clear { key } => {
                    snapshot.remove(key);
                }
                Mutation::ClearRange { .. } => {}
            }
        }
        self.commits
            .insert(batch.version, (batch.identity, signature));
        self.snapshots.insert(batch.version, snapshot);
        self.latest = batch.version;
        Ok(ApplyOutcome::Applied)
    }

    fn retain_from(&mut self, version: Version) -> Result<(), ModelError> {
        self.check_read_version(version)?;
        self.oldest_readable = version;
        Ok(())
    }

    fn get(&self, key: &[u8], version: Version) -> Result<Option<Vec<u8>>, ModelError> {
        self.check_read_version(version)?;
        Ok(self
            .snapshot_at(version)
            .and_then(|snapshot| snapshot.get(key).cloned()))
    }

    fn scan(&self, range: &KeyRange, version: Version) -> Result<Vec<Row>, ModelError> {
        self.check_read_version(version)?;
        Ok(self
            .snapshot_at(version)
            .into_iter()
            .flat_map(|snapshot| snapshot.range(range.start.clone()..range.end.clone()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn snapshot_at(&self, version: Version) -> Option<&BTreeMap<Vec<u8>, Vec<u8>>> {
        self.snapshots
            .range(..=version)
            .next_back()
            .map(|(_, snapshot)| snapshot)
    }

    fn check_read_version(&self, requested: Version) -> Result<(), ModelError> {
        if requested > self.latest {
            return Err(ModelError::VersionNotApplied {
                latest: self.latest,
                requested,
            });
        }
        if requested < self.oldest_readable {
            return Err(ModelError::VersionTooOld {
                oldest: self.oldest_readable,
                requested,
            });
        }
        Ok(())
    }
}

fn oracle_signature(mutations: &[Mutation]) -> Vec<OracleMutation> {
    let mut signature: Vec<OracleMutation> = mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Set { key, value } => OracleMutation::Set(key.clone(), value.clone()),
            Mutation::Clear { key } => OracleMutation::Clear(key.clone()),
            Mutation::ClearRange { range } => {
                OracleMutation::ClearRange(range.start.clone(), range.end.clone())
            }
        })
        .collect();
    signature.sort();
    signature.dedup();
    signature
}

struct SplitMix64 {
    state: u64,
    seed: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed, seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn bounded(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn count_range_clears(batch: &CommitBatch) -> u64 {
    batch
        .mutations
        .iter()
        .filter(|mutation| matches!(mutation, Mutation::ClearRange { .. }))
        .count() as u64
}

fn next_after(version: Version) -> Version {
    if version.sequence() == u64::MAX {
        Version::from_parts(version.generation().saturating_add(1), 0)
    } else {
        Version::from_parts(version.generation(), version.sequence() + 1)
    }
}

fn key(index: u8) -> Vec<u8> {
    format!("k{index:02}").into_bytes()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_history_is_exactly_replayable() {
        let first = run_differential_history(1103, 1_000, DifferentialMode::Correct);
        let second = run_differential_history(1103, 1_000, DifferentialMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert!(first.range_clear_count > 0);
        assert!(first.exact_replay_count > 0);
        assert!(first.conflicting_replay_count > 0);
        assert!(first.future_read_count > 0);
        assert!(first.retention_count > 0);
        assert!(first.too_old_read_count > 0);
        assert!(first.historical_read_count > 0);
        assert!(first.stale_generation_count > 0);
    }

    #[test]
    fn every_negative_control_has_a_bounded_failing_prefix() {
        let controls = [
            (DifferentialMode::IgnoreRangeClears, 2),
            (DifferentialMode::MutationOrderAffectsReplay, 3),
            (DifferentialMode::AcceptConflictingReplay, 4),
            (DifferentialMode::FutureReadFallsBack, 5),
            (DifferentialMode::RejectRetentionBoundary, 7),
            (DifferentialMode::ServeExpiredRead, 8),
            (DifferentialMode::AcceptStaleGeneration, 9),
        ];
        for (mode, expected_step) in controls {
            let report = run_differential_history(1103, 1_000, mode);
            assert_eq!(report.anomaly_count, 1, "{}", mode.id());
            assert_eq!(
                report.first_mismatch_step,
                Some(expected_step),
                "{}",
                mode.id()
            );
            assert!(report.first_mismatch.is_some(), "{}", mode.id());
        }
    }
}
