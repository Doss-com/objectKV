use okv_history_oracle::{
    KeyRange, KeyValue, ObservedValue, ReadOperation, TransactionHistoryV1, TransactionOutcome,
    TransactionRecord, HISTORY_SCHEMA_VERSION,
};
use std::collections::BTreeMap;

const BATCH_WIDTH: usize = 8;
const ACCOUNT_RANGES: usize = 4;
const ACCOUNTS_PER_RANGE: usize = 16;

/// Deliberately incorrect Cell v0 resolver behavior used to validate the
/// independent strict-serializability oracle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SerializabilityMode {
    Correct,
    AcceptPointConflict,
    AcceptRangePhantom,
    PartialCommit,
    OmitReadConflict,
    OmitWriteConflict,
    StaleReadVersion,
}

impl SerializabilityMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcceptPointConflict => "accept_point_conflict",
            Self::AcceptRangePhantom => "accept_range_phantom",
            Self::PartialCommit => "partial_commit",
            Self::OmitReadConflict => "omit_read_conflict",
            Self::OmitWriteConflict => "omit_write_conflict",
            Self::StaleReadVersion => "stale_read_version",
        }
    }
}

/// Emit one deterministic product-shaped history from the Cell v0 resolver
/// subject. The subject does not call the oracle and owns no oracle logic.
#[must_use]
pub fn run_serializability_history(
    seed: u64,
    transaction_count: usize,
    mode: SerializabilityMode,
) -> TransactionHistoryV1 {
    let initial_state = initial_state();
    let mut state: BTreeMap<Vec<u8>, ObservedValue> = initial_state
        .iter()
        .map(|item| {
            (
                item.key.clone(),
                ObservedValue {
                    key: item.key.clone(),
                    value: item.value.clone(),
                    writer: None,
                },
            )
        })
        .collect();
    let mut committed_writes: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut transactions = Vec::with_capacity(transaction_count);
    let mut latest_version = 0_u64;
    let mut rng = SplitMix64::new(seed);
    let mut next_id = 1_u64;

    for batch in 0..transaction_count.div_ceil(BATCH_WIDTH) {
        let batch_len = (transaction_count - transactions.len()).min(BATCH_WIDTH);
        let read_version = latest_version;
        let begin_tick = usize_to_u64(batch).saturating_mul(100).saturating_add(1);
        let mut candidates = Vec::with_capacity(batch_len);
        for slot in 0..batch_len {
            let id = next_id;
            next_id = next_id.saturating_add(1);
            let mut transaction = if slot == 2 || slot == 3 {
                phantom_candidate(id, begin_tick, read_version, batch, slot, &state)
            } else {
                transfer_candidate(id, begin_tick, read_version, batch, slot, &state, &mut rng)
            };
            transaction.complete_tick =
                transaction.complete_tick.saturating_add(usize_to_u64(slot));
            candidates.push(transaction);
        }

        for (slot, mut transaction) in candidates.into_iter().enumerate() {
            let has_conflict = committed_writes.iter().any(|(version, key)| {
                *version > transaction.read_version
                    && transaction
                        .read_conflicts
                        .iter()
                        .any(|range| range.contains(key))
            });
            let ignore_conflict =
                (mode == SerializabilityMode::AcceptPointConflict && batch == 0 && slot == 1)
                    || (mode == SerializabilityMode::AcceptRangePhantom && batch == 0 && slot == 3);
            if has_conflict && !ignore_conflict {
                transaction.outcome = TransactionOutcome::Aborted {
                    reason: "not_committed_conflict".to_owned(),
                };
            } else {
                latest_version = latest_version.saturating_add(1);
                transaction.outcome = TransactionOutcome::Committed {
                    commit_version: latest_version,
                };
                transaction.applied_writes.clone_from(&transaction.writes);
                for write in &transaction.applied_writes {
                    state.insert(
                        write.key.clone(),
                        ObservedValue {
                            key: write.key.clone(),
                            value: write.value.clone(),
                            writer: Some(transaction.id),
                        },
                    );
                    committed_writes.push((latest_version, write.key.clone()));
                }
            }
            transactions.push(transaction);
        }
    }

    inject_post_history_poison(&mut transactions, mode);
    TransactionHistoryV1 {
        schema_version: HISTORY_SCHEMA_VERSION,
        cell_id: "cell-v0-semantic-subject".to_owned(),
        tenant_id: "tenant-product-shaped".to_owned(),
        seed,
        initial_state,
        transactions,
    }
}

fn transfer_candidate(
    id: u64,
    begin_tick: u64,
    read_version: u64,
    batch: usize,
    slot: usize,
    state: &BTreeMap<Vec<u8>, ObservedValue>,
    rng: &mut SplitMix64,
) -> TransactionRecord {
    let (left_range, right_range, left_account, right_account) = if slot <= 1 {
        (0, 3, batch % ACCOUNTS_PER_RANGE, batch % ACCOUNTS_PER_RANGE)
    } else {
        let left_range = rng.index(ACCOUNT_RANGES);
        let mut right_range = rng.index(ACCOUNT_RANGES - 1);
        if right_range >= left_range {
            right_range += 1;
        }
        (
            left_range,
            right_range,
            rng.index(ACCOUNTS_PER_RANGE),
            rng.index(ACCOUNTS_PER_RANGE),
        )
    };
    let left = account_key(left_range, left_account);
    let right = account_key(right_range, right_account);
    let reads = vec![point_read(&left, state), point_read(&right, state)];
    let writes = vec![
        KeyValue {
            key: left.clone(),
            value: mutation_value(id, 0),
        },
        KeyValue {
            key: right.clone(),
            value: mutation_value(id, 1),
        },
    ];
    candidate(id, begin_tick, read_version, reads, writes)
}

fn phantom_candidate(
    id: u64,
    begin_tick: u64,
    read_version: u64,
    batch: usize,
    slot: usize,
    state: &BTreeMap<Vec<u8>, ObservedValue>,
) -> TransactionRecord {
    let prefix = format!("u/slot/{batch:08}/").into_bytes();
    let mut end = prefix.clone();
    end.push(0xff);
    let range = KeyRange {
        start: prefix.clone(),
        end,
    };
    let observed = state
        .range(range.start.clone()..range.end.clone())
        .map(|(_, value)| value.clone())
        .collect();
    let mut key = prefix;
    key.extend_from_slice(format!("candidate-{slot}").as_bytes());
    let reads = vec![ReadOperation::Range {
        range: range.clone(),
        observed,
    }];
    let writes = vec![KeyValue {
        key,
        value: mutation_value(id, 0),
    }];
    let mut transaction = candidate(id, begin_tick, read_version, reads, writes);
    transaction.read_conflicts = vec![range];
    transaction
}

fn candidate(
    id: u64,
    begin_tick: u64,
    read_version: u64,
    reads: Vec<ReadOperation>,
    writes: Vec<KeyValue>,
) -> TransactionRecord {
    TransactionRecord {
        id,
        begin_tick,
        complete_tick: begin_tick.saturating_add(50),
        read_version,
        read_conflicts: reads
            .iter()
            .map(|read| match read {
                ReadOperation::Point { key, .. } => KeyRange::point(key),
                ReadOperation::Range { range, .. } => range.clone(),
            })
            .collect(),
        write_conflicts: writes
            .iter()
            .map(|write| KeyRange::point(&write.key))
            .collect(),
        reads,
        writes,
        outcome: TransactionOutcome::Aborted {
            reason: "candidate_not_evaluated".to_owned(),
        },
        applied_writes: Vec::new(),
    }
}

fn inject_post_history_poison(transactions: &mut [TransactionRecord], mode: SerializabilityMode) {
    match mode {
        SerializabilityMode::PartialCommit => {
            if let Some(transaction) = transactions.iter_mut().find(|transaction| {
                transaction.outcome.commit_version().is_some() && transaction.writes.len() > 1
            }) {
                transaction.applied_writes.pop();
            }
        }
        SerializabilityMode::OmitReadConflict => {
            if let Some(transaction) = transactions
                .iter_mut()
                .find(|transaction| transaction.outcome.commit_version().is_some())
            {
                transaction.read_conflicts.clear();
            }
        }
        SerializabilityMode::OmitWriteConflict => {
            if let Some(transaction) = transactions
                .iter_mut()
                .find(|transaction| transaction.outcome.commit_version().is_some())
            {
                transaction.write_conflicts.clear();
            }
        }
        SerializabilityMode::StaleReadVersion => {
            if let Some(transaction) = transactions.iter_mut().find(|transaction| {
                transaction.begin_tick >= 100 && transaction.outcome.commit_version().is_some()
            }) {
                transaction.read_version = 0;
            }
        }
        SerializabilityMode::Correct
        | SerializabilityMode::AcceptPointConflict
        | SerializabilityMode::AcceptRangePhantom => {}
    }
}

fn initial_state() -> Vec<KeyValue> {
    (0..ACCOUNT_RANGES)
        .flat_map(|range| {
            (0..ACCOUNTS_PER_RANGE).map(move |account| KeyValue {
                key: account_key(range, account),
                value: usize_to_u64(range * ACCOUNTS_PER_RANGE + account)
                    .saturating_add(1_000)
                    .to_be_bytes()
                    .to_vec(),
            })
        })
        .collect()
}

fn account_key(range: usize, account: usize) -> Vec<u8> {
    format!(
        "{}/account/{account:04}",
        char::from(b'a' + u8::try_from(range).unwrap_or(0))
    )
    .into_bytes()
}

fn point_read(key: &[u8], state: &BTreeMap<Vec<u8>, ObservedValue>) -> ReadOperation {
    ReadOperation::Point {
        key: key.to_vec(),
        observed: state.get(key).cloned(),
    }
}

fn mutation_value(id: u64, ordinal: u64) -> Vec<u8> {
    id.rotate_left(u32::try_from(ordinal.saturating_mul(7)).unwrap_or(0))
        .to_be_bytes()
        .to_vec()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        usize::try_from(self.next() % usize_to_u64(upper)).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okv_history_oracle::{check_history, AnomalyClass};

    #[test]
    fn correct_subject_is_strict_serializable_and_multi_range() {
        let history = run_serializability_history(1103, 1_000, SerializabilityMode::Correct);
        let report = check_history(&history);
        assert!(report.passed(), "{:?}", report.anomalies.first());
        assert_eq!(report.transaction_count, 1_000);
        assert!(report.multi_range_committed_count > 0);
        assert!(report.aborted_count > 0);
    }

    #[test]
    fn every_poison_is_independently_detected() {
        let cases = [
            (
                SerializabilityMode::AcceptPointConflict,
                AnomalyClass::ReadWriteConflict,
            ),
            (
                SerializabilityMode::AcceptRangePhantom,
                AnomalyClass::ReadWriteConflict,
            ),
            (SerializabilityMode::PartialCommit, AnomalyClass::Atomicity),
            (
                SerializabilityMode::OmitReadConflict,
                AnomalyClass::ConflictCoverage,
            ),
            (
                SerializabilityMode::OmitWriteConflict,
                AnomalyClass::ConflictCoverage,
            ),
            (
                SerializabilityMode::StaleReadVersion,
                AnomalyClass::RealTimeOrder,
            ),
        ];
        for (mode, class) in cases {
            let report = check_history(&run_serializability_history(1103, 128, mode));
            assert!(
                report
                    .anomalies
                    .iter()
                    .any(|anomaly| anomaly.class == class),
                "mode={} anomalies={:?}",
                mode.id(),
                report.anomalies
            );
        }
    }

    #[test]
    fn generated_histories_replay_exactly() {
        for mode in [
            SerializabilityMode::Correct,
            SerializabilityMode::AcceptPointConflict,
            SerializabilityMode::AcceptRangePhantom,
            SerializabilityMode::PartialCommit,
            SerializabilityMode::OmitReadConflict,
            SerializabilityMode::OmitWriteConflict,
            SerializabilityMode::StaleReadVersion,
        ] {
            assert_eq!(
                run_serializability_history(2207, 128, mode),
                run_serializability_history(2207, 128, mode)
            );
        }
    }

    #[test]
    fn generated_history_matches_frozen_json_schema() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/transaction-history-v1.schema.json"
        ))
        .expect("schema JSON");
        let validator = jsonschema::validator_for(&schema).expect("valid schema");
        let history = serde_json::to_value(run_serializability_history(
            1103,
            128,
            SerializabilityMode::Correct,
        ))
        .expect("history JSON");
        let errors: Vec<String> = validator
            .iter_errors(&history)
            .map(|error| error.to_string())
            .collect();
        assert!(errors.is_empty(), "{errors:?}");
    }
}
