//! Independent strict-serializability checker for objectKV transaction histories.
//!
//! The subject under test emits [`TransactionHistoryV1`]. This crate checks the
//! history without importing the resolver, txLog, MVCC model, or serving code.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const HISTORY_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionHistoryV1 {
    pub schema_version: u32,
    pub cell_id: String,
    pub tenant_id: String,
    pub seed: u64,
    pub initial_state: Vec<KeyValue>,
    pub transactions: Vec<TransactionRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl KeyRange {
    #[must_use]
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            start: key.to_vec(),
            end,
        }
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    #[must_use]
    pub fn contains_range(&self, other: &Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    fn valid(&self) -> bool {
        self.start < self.end
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObservedValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub writer: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadOperation {
    Point {
        observed: Option<ObservedValue>,
        key: Vec<u8>,
    },
    Range {
        range: KeyRange,
        observed: Vec<ObservedValue>,
    },
}

impl ReadOperation {
    fn conflict_range(&self) -> KeyRange {
        match self {
            Self::Point { key, .. } => KeyRange::point(key),
            Self::Range { range, .. } => range.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TransactionOutcome {
    Committed { commit_version: u64 },
    Aborted { reason: String },
}

impl TransactionOutcome {
    #[must_use]
    pub const fn commit_version(&self) -> Option<u64> {
        match self {
            Self::Committed { commit_version } => Some(*commit_version),
            Self::Aborted { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionRecord {
    pub id: u64,
    pub begin_tick: u64,
    pub complete_tick: u64,
    pub read_version: u64,
    pub reads: Vec<ReadOperation>,
    pub read_conflicts: Vec<KeyRange>,
    pub writes: Vec<KeyValue>,
    pub write_conflicts: Vec<KeyRange>,
    pub outcome: TransactionOutcome,
    pub applied_writes: Vec<KeyValue>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyClass {
    Atomicity,
    ConflictCoverage,
    DuplicateIdentity,
    InvalidRecord,
    RealTimeOrder,
    ReadWriteConflict,
    SchemaVersion,
    SnapshotVisibility,
    VersionOrder,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Anomaly {
    pub class: AnomalyClass,
    pub transaction: Option<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OracleReport {
    pub history_schema_version: u32,
    pub transaction_count: u64,
    pub committed_count: u64,
    pub aborted_count: u64,
    pub multi_range_committed_count: u64,
    pub point_read_count: u64,
    pub range_read_count: u64,
    pub anomalies: Vec<Anomaly>,
}

impl OracleReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.anomalies.is_empty()
    }
}

/// Check one history against the RFC-0008 Cell v0 OCC contract.
///
/// A passing history is strict serializable in commit-version order: every
/// read is an exact snapshot, real-time predecessors are included, complete
/// read and write conflict ranges are declared, intervening conflicting writes
/// are rejected, and committed effects are atomic.
#[must_use]
pub fn check_history(history: &TransactionHistoryV1) -> OracleReport {
    let mut anomalies = Vec::new();
    if history.schema_version != HISTORY_SCHEMA_VERSION {
        push(
            &mut anomalies,
            AnomalyClass::SchemaVersion,
            None,
            format!(
                "expected schema version {HISTORY_SCHEMA_VERSION}, observed {}",
                history.schema_version
            ),
        );
    }

    let initial = validate_initial_state(history, &mut anomalies);
    let by_id = validate_identities(history, &mut anomalies);
    let committed = validate_versions(history, &mut anomalies);

    for transaction in &history.transactions {
        validate_record_shape(transaction, &mut anomalies);
        validate_conflict_coverage(transaction, &mut anomalies);
        validate_atomic_effect(transaction, &mut anomalies);
        validate_snapshot(transaction, &initial, &committed, &mut anomalies);
        validate_intervening_conflicts(transaction, &committed, &mut anomalies);
    }
    validate_real_time(history, &by_id, &mut anomalies);

    let committed_count = history
        .transactions
        .iter()
        .filter(|transaction| transaction.outcome.commit_version().is_some())
        .count();
    let multi_range_committed_count = history
        .transactions
        .iter()
        .filter(|transaction| {
            transaction.outcome.commit_version().is_some()
                && transaction
                    .writes
                    .iter()
                    .map(|write| write.key.first().copied())
                    .collect::<BTreeSet<_>>()
                    .len()
                    > 1
        })
        .count();
    let point_read_count = history
        .transactions
        .iter()
        .flat_map(|transaction| &transaction.reads)
        .filter(|read| matches!(read, ReadOperation::Point { .. }))
        .count();
    let range_read_count = history
        .transactions
        .iter()
        .flat_map(|transaction| &transaction.reads)
        .filter(|read| matches!(read, ReadOperation::Range { .. }))
        .count();
    OracleReport {
        history_schema_version: history.schema_version,
        transaction_count: usize_to_u64(history.transactions.len()),
        committed_count: usize_to_u64(committed_count),
        aborted_count: usize_to_u64(history.transactions.len().saturating_sub(committed_count)),
        multi_range_committed_count: usize_to_u64(multi_range_committed_count),
        point_read_count: usize_to_u64(point_read_count),
        range_read_count: usize_to_u64(range_read_count),
        anomalies,
    }
}

fn validate_initial_state(
    history: &TransactionHistoryV1,
    anomalies: &mut Vec<Anomaly>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut initial = BTreeMap::new();
    for item in &history.initial_state {
        if initial
            .insert(item.key.clone(), item.value.clone())
            .is_some()
        {
            push(
                anomalies,
                AnomalyClass::DuplicateIdentity,
                None,
                format!("duplicate initial key {:?}", item.key),
            );
        }
    }
    initial
}

fn validate_identities<'a>(
    history: &'a TransactionHistoryV1,
    anomalies: &mut Vec<Anomaly>,
) -> BTreeMap<u64, &'a TransactionRecord> {
    let mut by_id = BTreeMap::new();
    for transaction in &history.transactions {
        if by_id.insert(transaction.id, transaction).is_some() {
            push(
                anomalies,
                AnomalyClass::DuplicateIdentity,
                Some(transaction.id),
                "transaction identity appears more than once".to_owned(),
            );
        }
    }
    by_id
}

fn validate_versions<'a>(
    history: &'a TransactionHistoryV1,
    anomalies: &mut Vec<Anomaly>,
) -> Vec<(u64, &'a TransactionRecord)> {
    let mut versions = BTreeMap::new();
    for transaction in &history.transactions {
        let Some(version) = transaction.outcome.commit_version() else {
            continue;
        };
        if version == 0 || version <= transaction.read_version {
            push(
                anomalies,
                AnomalyClass::VersionOrder,
                Some(transaction.id),
                format!(
                    "commit version {version} must be greater than read version {}",
                    transaction.read_version
                ),
            );
        }
        if let Some(prior) = versions.insert(version, transaction) {
            push(
                anomalies,
                AnomalyClass::DuplicateIdentity,
                Some(transaction.id),
                format!(
                    "commit version {version} also belongs to transaction {}",
                    prior.id
                ),
            );
        }
    }
    versions.into_iter().collect()
}

fn validate_record_shape(transaction: &TransactionRecord, anomalies: &mut Vec<Anomaly>) {
    if transaction.begin_tick >= transaction.complete_tick {
        push(
            anomalies,
            AnomalyClass::InvalidRecord,
            Some(transaction.id),
            "begin_tick must be less than complete_tick".to_owned(),
        );
    }
    for range in transaction
        .read_conflicts
        .iter()
        .chain(transaction.write_conflicts.iter())
    {
        if !range.valid() {
            push(
                anomalies,
                AnomalyClass::InvalidRecord,
                Some(transaction.id),
                format!("invalid conflict range {:?}..{:?}", range.start, range.end),
            );
        }
    }
    for read in &transaction.reads {
        if let ReadOperation::Range { range, observed } = read {
            if !range.valid() {
                push(
                    anomalies,
                    AnomalyClass::InvalidRecord,
                    Some(transaction.id),
                    "range read has an invalid interval".to_owned(),
                );
            }
            if observed
                .windows(2)
                .any(|window| window[0].key >= window[1].key)
            {
                push(
                    anomalies,
                    AnomalyClass::InvalidRecord,
                    Some(transaction.id),
                    "range read observations must be strictly key ordered".to_owned(),
                );
            }
        }
    }
    if duplicate_keys(&transaction.writes) || duplicate_keys(&transaction.applied_writes) {
        push(
            anomalies,
            AnomalyClass::InvalidRecord,
            Some(transaction.id),
            "write sets may contain each key at most once".to_owned(),
        );
    }
}

fn validate_conflict_coverage(transaction: &TransactionRecord, anomalies: &mut Vec<Anomaly>) {
    for read in &transaction.reads {
        let required = read.conflict_range();
        if !transaction
            .read_conflicts
            .iter()
            .any(|declared| declared.contains_range(&required))
        {
            push(
                anomalies,
                AnomalyClass::ConflictCoverage,
                Some(transaction.id),
                format!(
                    "read range {:?}..{:?} is not covered",
                    required.start, required.end
                ),
            );
        }
    }
    for write in &transaction.writes {
        if !transaction
            .write_conflicts
            .iter()
            .any(|range| range.contains(&write.key))
        {
            push(
                anomalies,
                AnomalyClass::ConflictCoverage,
                Some(transaction.id),
                format!("write key {:?} is not covered", write.key),
            );
        }
    }
}

fn validate_atomic_effect(transaction: &TransactionRecord, anomalies: &mut Vec<Anomaly>) {
    let mut expected = transaction.writes.clone();
    let mut actual = transaction.applied_writes.clone();
    expected.sort();
    actual.sort();
    let valid = match transaction.outcome {
        TransactionOutcome::Committed { .. } => actual == expected,
        TransactionOutcome::Aborted { .. } => actual.is_empty(),
    };
    if !valid {
        push(
            anomalies,
            AnomalyClass::Atomicity,
            Some(transaction.id),
            format!("declared writes {expected:?}, applied writes {actual:?}"),
        );
    }
}

fn validate_snapshot(
    transaction: &TransactionRecord,
    initial: &BTreeMap<Vec<u8>, Vec<u8>>,
    committed: &[(u64, &TransactionRecord)],
    anomalies: &mut Vec<Anomaly>,
) {
    let snapshot = snapshot_at(transaction.read_version, initial, committed);
    for read in &transaction.reads {
        match read {
            ReadOperation::Point { key, observed } => {
                let expected = snapshot.get(key).cloned();
                if observed.as_ref() != expected.as_ref() {
                    push(
                        anomalies,
                        AnomalyClass::SnapshotVisibility,
                        Some(transaction.id),
                        format!("point read {key:?}: expected {expected:?}, observed {observed:?}"),
                    );
                }
            }
            ReadOperation::Range { range, observed } => {
                let expected: Vec<ObservedValue> = snapshot
                    .range(range.start.clone()..range.end.clone())
                    .map(|(_, value)| value.clone())
                    .collect();
                if observed != &expected {
                    push(
                        anomalies,
                        AnomalyClass::SnapshotVisibility,
                        Some(transaction.id),
                        format!(
                            "range read {:?}..{:?}: expected {expected:?}, observed {observed:?}",
                            range.start, range.end
                        ),
                    );
                }
            }
        }
    }
}

fn snapshot_at(
    version: u64,
    initial: &BTreeMap<Vec<u8>, Vec<u8>>,
    committed: &[(u64, &TransactionRecord)],
) -> BTreeMap<Vec<u8>, ObservedValue> {
    let mut state: BTreeMap<Vec<u8>, ObservedValue> = initial
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                ObservedValue {
                    key: key.clone(),
                    value: value.clone(),
                    writer: None,
                },
            )
        })
        .collect();
    for (commit_version, transaction) in committed {
        if *commit_version > version {
            break;
        }
        for write in &transaction.applied_writes {
            state.insert(
                write.key.clone(),
                ObservedValue {
                    key: write.key.clone(),
                    value: write.value.clone(),
                    writer: Some(transaction.id),
                },
            );
        }
    }
    state
}

fn validate_intervening_conflicts(
    transaction: &TransactionRecord,
    committed: &[(u64, &TransactionRecord)],
    anomalies: &mut Vec<Anomaly>,
) {
    let Some(commit_version) = transaction.outcome.commit_version() else {
        return;
    };
    for (other_version, other) in committed {
        if *other_version <= transaction.read_version || *other_version >= commit_version {
            continue;
        }
        for write in &other.applied_writes {
            if transaction
                .read_conflicts
                .iter()
                .any(|range| range.contains(&write.key))
            {
                push(
                    anomalies,
                    AnomalyClass::ReadWriteConflict,
                    Some(transaction.id),
                    format!(
                        "transaction {} committed at {other_version} and wrote {:?} after read version {}",
                        other.id, write.key, transaction.read_version
                    ),
                );
            }
        }
    }
}

fn validate_real_time(
    history: &TransactionHistoryV1,
    _by_id: &BTreeMap<u64, &TransactionRecord>,
    anomalies: &mut Vec<Anomaly>,
) {
    let committed: Vec<&TransactionRecord> = history
        .transactions
        .iter()
        .filter(|transaction| transaction.outcome.commit_version().is_some())
        .collect();
    for prior in &committed {
        let prior_version = prior.outcome.commit_version().unwrap_or(0);
        for later in &committed {
            if prior.complete_tick >= later.begin_tick {
                continue;
            }
            let later_version = later.outcome.commit_version().unwrap_or(0);
            if prior_version >= later_version || later.read_version < prior_version {
                push(
                    anomalies,
                    AnomalyClass::RealTimeOrder,
                    Some(later.id),
                    format!(
                        "completed transaction {} at version {prior_version} precedes begin, read version {}, commit version {later_version}",
                        prior.id, later.read_version
                    ),
                );
            }
        }
    }
}

fn duplicate_keys(items: &[KeyValue]) -> bool {
    let mut keys = BTreeSet::new();
    items.iter().any(|item| !keys.insert(&item.key))
}

fn push(
    anomalies: &mut Vec<Anomaly>,
    class: AnomalyClass,
    transaction: Option<u64>,
    detail: String,
) {
    anomalies.push(Anomaly {
        class,
        transaction,
        detail,
    });
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> Vec<u8> {
        name.as_bytes().to_vec()
    }

    fn write(name: &str, value: u8) -> KeyValue {
        KeyValue {
            key: key(name),
            value: vec![value],
        }
    }

    fn point(name: &str, value: u8, writer: Option<u64>) -> ReadOperation {
        ReadOperation::Point {
            key: key(name),
            observed: Some(ObservedValue {
                key: key(name),
                value: vec![value],
                writer,
            }),
        }
    }

    fn committed(
        id: u64,
        begin_tick: u64,
        complete_tick: u64,
        read_version: u64,
        commit_version: u64,
        reads: Vec<ReadOperation>,
        writes: Vec<KeyValue>,
    ) -> TransactionRecord {
        TransactionRecord {
            id,
            begin_tick,
            complete_tick,
            read_version,
            read_conflicts: reads.iter().map(ReadOperation::conflict_range).collect(),
            write_conflicts: writes
                .iter()
                .map(|item| KeyRange::point(&item.key))
                .collect(),
            reads,
            applied_writes: writes.clone(),
            writes,
            outcome: TransactionOutcome::Committed { commit_version },
        }
    }

    fn valid_history() -> TransactionHistoryV1 {
        TransactionHistoryV1 {
            schema_version: HISTORY_SCHEMA_VERSION,
            cell_id: "cell-test".to_owned(),
            tenant_id: "tenant-test".to_owned(),
            seed: 1103,
            initial_state: vec![write("a/account", 10), write("z/account", 20)],
            transactions: vec![
                committed(
                    1,
                    0,
                    5,
                    0,
                    1,
                    vec![point("a/account", 10, None), point("z/account", 20, None)],
                    vec![write("a/account", 9), write("z/account", 21)],
                ),
                committed(
                    2,
                    10,
                    15,
                    1,
                    2,
                    vec![point("a/account", 9, Some(1))],
                    vec![write("a/account", 8)],
                ),
            ],
        }
    }

    #[test]
    fn accepts_exact_multi_range_history() {
        let report = check_history(&valid_history());
        assert!(report.passed(), "{:?}", report.anomalies);
        assert_eq!(report.multi_range_committed_count, 1);
    }

    #[test]
    fn detects_each_load_bearing_anomaly_class() {
        let mut conflict = valid_history();
        conflict.transactions[1].read_version = 0;
        conflict.transactions[1].reads = vec![point("a/account", 10, None)];
        let report = check_history(&conflict);
        assert!(report
            .anomalies
            .iter()
            .any(|item| item.class == AnomalyClass::ReadWriteConflict));
        assert!(report
            .anomalies
            .iter()
            .any(|item| item.class == AnomalyClass::RealTimeOrder));

        let mut partial = valid_history();
        partial.transactions[0].applied_writes.pop();
        assert!(check_history(&partial)
            .anomalies
            .iter()
            .any(|item| item.class == AnomalyClass::Atomicity));

        let mut uncovered = valid_history();
        uncovered.transactions[0].read_conflicts.clear();
        assert!(check_history(&uncovered)
            .anomalies
            .iter()
            .any(|item| item.class == AnomalyClass::ConflictCoverage));

        let mut stale = valid_history();
        if let ReadOperation::Point { observed, .. } = &mut stale.transactions[1].reads[0] {
            observed.as_mut().expect("present").value = vec![99];
        }
        assert!(check_history(&stale)
            .anomalies
            .iter()
            .any(|item| item.class == AnomalyClass::SnapshotVisibility));
    }

    #[test]
    fn history_schema_is_stable_json() {
        let encoded = serde_json::to_vec(&valid_history()).expect("serialize");
        let decoded: TransactionHistoryV1 = serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded, valid_history());
    }
}
