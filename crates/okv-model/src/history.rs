use super::{
    ApplyOutcome, CommitBatch, CommitIdentity, KeyRange, Model, ModelError, Mutation, Row, Version,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Subject behavior used to prove that the differential gate detects a bug.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DifferentialMode {
    Correct,
    IgnoreRangeClears,
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
    pub replay_count: u64,
    pub read_count: u64,
    pub trace_sha256: String,
}

/// Run one generated MVCC history against an independent full-snapshot oracle.
///
/// A deterministic prelude guarantees that the range-clear negative control is
/// exercised before random generation begins.
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
    replay_count: u64,
    read_count: u64,
    generation: u64,
    sequence: u64,
    last_batch: Option<CommitBatch>,
}

impl Runner {
    fn new(seed: u64, requested_steps: u64, mode: DifferentialMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-generated-history-v1");
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
            replay_count: 0,
            read_count: 0,
            generation: 1,
            sequence: 0,
            last_batch: None,
        }
    }

    fn run(&mut self) {
        let prelude = [
            vec![
                Mutation::Set {
                    key: key(0),
                    value: b"prelude-a".to_vec(),
                },
                Mutation::Set {
                    key: key(1),
                    value: b"prelude-b".to_vec(),
                },
            ],
            vec![
                Mutation::ClearRange {
                    range: KeyRange::new(key(0), key(2)).expect("valid fixed range"),
                },
                Mutation::Set {
                    key: key(1),
                    value: b"point-wins".to_vec(),
                },
            ],
        ];
        for mutations in prelude {
            if self.executed_steps >= self.requested_steps || self.first_mismatch.is_some() {
                return;
            }
            self.commit(mutations);
        }

        while self.executed_steps < self.requested_steps && self.first_mismatch.is_none() {
            if self.executed_steps > 0 && self.executed_steps % 29 == 0 {
                self.replay_last();
            } else {
                let mutations = self.generated_mutations();
                self.commit(mutations);
            }
        }
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
            identity: CommitIdentity::new(
                self.rng
                    .next()
                    .to_be_bytes()
                    .repeat(2)
                    .try_into()
                    .expect("16 bytes"),
                self.executed_steps + 1,
            ),
            mutations,
        };
        self.range_clear_count += batch
            .mutations
            .iter()
            .filter(|mutation| matches!(mutation, Mutation::ClearRange { .. }))
            .count() as u64;
        self.trace_batch(b'C', &batch);
        let expected = self.oracle.apply(&batch);
        let actual = self.subject.apply(self.subject_batch(batch.clone()));
        self.executed_steps += 1;
        self.compare_apply(&expected, &actual);
        if self.first_mismatch.is_none() {
            self.compare_reads(version);
        }
        self.last_batch = Some(batch);
    }

    fn replay_last(&mut self) {
        let Some(mut batch) = self.last_batch.clone() else {
            return;
        };
        batch.mutations.reverse();
        self.trace_batch(b'R', &batch);
        let expected = self.oracle.apply(&batch);
        let actual = self.subject.apply(self.subject_batch(batch));
        self.replay_count += 1;
        self.executed_steps += 1;
        self.compare_apply(&expected, &actual);
        if self.first_mismatch.is_none() {
            self.compare_reads(self.subject.latest_version());
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
        expected: &Result<ApplyOutcome, ModelError>,
        actual: &Result<ApplyOutcome, ModelError>,
    ) {
        if expected != actual {
            self.mismatch(format!("apply: expected {expected:?}, actual {actual:?}"));
        }
    }

    fn compare_reads(&mut self, version: Version) {
        for index in 0..16_u8 {
            let candidate = key(index);
            let expected = self.oracle.get(&candidate, version);
            let actual = self
                .subject
                .get(&candidate, version)
                .map(|value| value.map(<[u8]>::to_vec));
            self.read_count += 1;
            self.trace.update(b"G");
            self.trace.update(version.to_be_bytes());
            self.trace.update(&candidate);
            if expected != actual {
                self.mismatch(format!(
                    "get {candidate:?} at {version}: expected {expected:?}, actual {actual:?}"
                ));
                return;
            }
        }
        let range = KeyRange::new(key(0), key(16)).expect("fixed range");
        let expected = self.oracle.scan(&range, version);
        let actual = self.subject.scan(&range, version);
        self.read_count += 1;
        self.trace.update(b"S");
        self.trace.update(version.to_be_bytes());
        if expected != actual {
            self.mismatch(format!(
                "scan at {version}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }

    fn trace_batch(&mut self, operation: u8, batch: &CommitBatch) {
        self.trace.update([operation]);
        self.trace.update(batch.version.to_be_bytes());
        self.trace
            .update(batch.fingerprint().expect("generated batch is valid"));
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
            replay_count: self.replay_count,
            read_count: self.read_count,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

#[derive(Default)]
struct NaiveOracle {
    snapshots: BTreeMap<Version, BTreeMap<Vec<u8>, Vec<u8>>>,
    commits: BTreeMap<Version, ([u8; 32], CommitIdentity)>,
    latest: Version,
}

impl NaiveOracle {
    fn apply(&mut self, batch: &CommitBatch) -> Result<ApplyOutcome, ModelError> {
        let fingerprint = batch.fingerprint()?;
        if let Some((existing, identity)) = self.commits.get(&batch.version) {
            return if existing == &fingerprint && identity == &batch.identity {
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
        let mut snapshot = self
            .snapshots
            .get(&self.latest)
            .cloned()
            .unwrap_or_default();
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
            .insert(batch.version, (fingerprint, batch.identity));
        self.snapshots.insert(batch.version, snapshot);
        self.latest = batch.version;
        Ok(ApplyOutcome::Applied)
    }

    fn get(&self, key: &[u8], version: Version) -> Result<Option<Vec<u8>>, ModelError> {
        if version > self.latest {
            return Err(ModelError::VersionNotApplied {
                latest: self.latest,
                requested: version,
            });
        }
        Ok(self
            .snapshots
            .get(&version)
            .and_then(|snapshot| snapshot.get(key).cloned()))
    }

    fn scan(&self, range: &KeyRange, version: Version) -> Result<Vec<Row>, ModelError> {
        if version > self.latest {
            return Err(ModelError::VersionNotApplied {
                latest: self.latest,
                requested: version,
            });
        }
        Ok(self
            .snapshots
            .get(&version)
            .into_iter()
            .flat_map(|snapshot| snapshot.range(range.start.clone()..range.end.clone()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
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
        assert!(first.replay_count > 0);
    }

    #[test]
    fn negative_control_catches_ignored_range_clear() {
        let report = run_differential_history(1103, 1_000, DifferentialMode::IgnoreRangeClears);
        assert_eq!(report.anomaly_count, 1);
        assert_eq!(report.first_mismatch_step, Some(2));
        assert!(report.first_mismatch.is_some());
    }
}
