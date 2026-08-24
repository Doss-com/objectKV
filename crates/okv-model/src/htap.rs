use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Deliberately incorrect behavior used to prove one `ZebraDB` HTAP invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtapContractMode {
    /// The intended exact-snapshot contract.
    Correct,
    /// Filter the tail before it invalidates matching base rows.
    PushdownPoison,
    /// Reduce a moved row independently in each physical partition.
    SchemaPartitionMove,
    /// Reclaim the analytical tail when the recovery WAL is popped.
    WalPopConflation,
    /// Rebase an active query onto objects outside its snapshot lease.
    LeaseGcRace,
    /// Validate a dependency token outside the serializable write transaction.
    CertificateToctou,
}

impl HtapContractMode {
    /// Stable configuration identifier used by eval suites and artifact refs.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PushdownPoison => "pushdown_poison",
            Self::SchemaPartitionMove => "schema_partition_move",
            Self::WalPopConflation => "wal_pop_conflation",
            Self::LeaseGcRace => "lease_gc_race",
            Self::CertificateToctou => "certificate_toctou",
        }
    }
}

/// Result of the deterministic exact-snapshot contract scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HtapContractReport {
    pub seed: u64,
    pub mode: HtapContractMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub exact_checks: u64,
    pub tail_rows: u64,
    pub tail_bytes: u64,
    pub peak_memory_bytes: u64,
    pub spill_bytes: u64,
    pub trace_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Status {
    Open,
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WriterFields {
    V1 { total_cents: u64 },
    V2 { amount_cents: u64, priority: u8 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WriterRow {
    id: u64,
    status: Status,
    partition: &'static str,
    fields: WriterFields,
}

impl WriterRow {
    fn normalize(&self) -> NormalizedRow {
        let (amount_cents, priority) = match self.fields {
            WriterFields::V1 { total_cents } => (total_cents, 0),
            WriterFields::V2 {
                amount_cents,
                priority,
            } => (amount_cents, priority),
        };
        NormalizedRow {
            id: self.id,
            status: self.status,
            partition: self.partition,
            amount_cents,
            priority,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NormalizedRow {
    id: u64,
    status: Status,
    partition: &'static str,
    amount_cents: u64,
    priority: u8,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProjectedRow {
    id: Option<u64>,
    status: Status,
    partition: &'static str,
    amount_cents: u64,
    priority: u8,
}

#[derive(Clone, Copy)]
struct Query {
    status: Option<Status>,
    include_id: bool,
    order_amount_desc: bool,
    limit: Option<usize>,
}

impl Query {
    const fn pushdown_poison() -> Self {
        Self {
            status: Some(Status::Open),
            include_id: false,
            order_amount_desc: true,
            limit: Some(1),
        }
    }

    const fn all() -> Self {
        Self {
            status: None,
            include_id: true,
            order_amount_desc: false,
            limit: None,
        }
    }

    fn execute(self, mut rows: Vec<NormalizedRow>) -> Vec<ProjectedRow> {
        if let Some(status) = self.status {
            rows.retain(|row| row.status == status);
        }
        if self.order_amount_desc {
            rows.sort_by(|left, right| {
                right
                    .amount_cents
                    .cmp(&left.amount_cents)
                    .then_with(|| left.id.cmp(&right.id))
            });
        } else {
            rows.sort_by(|left, right| {
                left.id
                    .cmp(&right.id)
                    .then_with(|| left.partition.cmp(right.partition))
            });
        }
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        rows.into_iter()
            .map(|row| ProjectedRow {
                id: self.include_id.then_some(row.id),
                status: row.status,
                partition: row.partition,
                amount_cents: row.amount_cents,
                priority: row.priority,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct Change {
    version: u64,
    id: u64,
    previous_partition: Option<&'static str>,
    after: Option<WriterRow>,
}

#[derive(Clone)]
struct History {
    initial: Vec<WriterRow>,
    changes: Vec<Change>,
}

impl History {
    fn writer_rows_at(&self, version: u64) -> BTreeMap<u64, WriterRow> {
        let mut rows: BTreeMap<u64, WriterRow> = self
            .initial
            .iter()
            .cloned()
            .map(|row| (row.id, row))
            .collect();
        for change in self
            .changes
            .iter()
            .filter(|change| change.version <= version)
        {
            if let Some(after) = &change.after {
                rows.insert(change.id, after.clone());
            } else {
                rows.remove(&change.id);
            }
        }
        rows
    }

    fn oracle(&self, version: u64, query: Query) -> Vec<ProjectedRow> {
        query.execute(
            self.writer_rows_at(version)
                .values()
                .map(WriterRow::normalize)
                .collect(),
        )
    }

    fn base(&self, partition: &'static str, watermark: u64) -> BasePartition {
        BasePartition {
            partition,
            watermark,
            rows: self
                .writer_rows_at(watermark)
                .into_values()
                .filter(|row| row.partition == partition)
                .collect(),
        }
    }
}

#[derive(Clone)]
struct BasePartition {
    partition: &'static str,
    watermark: u64,
    rows: Vec<WriterRow>,
}

struct OverlayResult {
    rows: Vec<NormalizedRow>,
    tail_rows: u64,
    tail_bytes: u64,
    peak_rows: u64,
}

fn overlay(
    bases: &[BasePartition],
    changes: &[Change],
    target: u64,
    query: Query,
    mode: HtapContractMode,
) -> OverlayResult {
    let watermarks: BTreeMap<&str, u64> = bases
        .iter()
        .map(|base| (base.partition, base.watermark))
        .collect();
    let mut rows: BTreeMap<(&str, u64), WriterRow> = bases
        .iter()
        .flat_map(|base| {
            base.rows
                .iter()
                .cloned()
                .map(|row| ((base.partition, row.id), row))
        })
        .collect();
    let mut tail_rows = 0_u64;
    let mut tail_bytes = 0_u64;
    let mut peak_rows = count(rows.len());

    for change in changes.iter().filter(|change| change.version <= target) {
        let previous_needed = change.previous_partition.is_some_and(|partition| {
            watermarks
                .get(partition)
                .is_some_and(|watermark| change.version > *watermark)
        });
        let next_needed = change.after.as_ref().is_some_and(|after| {
            watermarks
                .get(after.partition)
                .is_some_and(|watermark| change.version > *watermark)
        });
        if !previous_needed && !next_needed {
            continue;
        }

        let poisoned = mode == HtapContractMode::PushdownPoison
            && query.status.is_some()
            && change.after.as_ref().is_none_or(|after| {
                query
                    .status
                    .is_some_and(|status| after.normalize().status != status)
            });
        if poisoned {
            continue;
        }

        tail_rows = tail_rows.saturating_add(1);
        tail_bytes = tail_bytes.saturating_add(change_size(change));
        if previous_needed {
            let skip_cross_partition_invalidation = mode == HtapContractMode::SchemaPartitionMove
                && change
                    .after
                    .as_ref()
                    .is_some_and(|after| change.previous_partition != Some(after.partition));
            if !skip_cross_partition_invalidation {
                rows.remove(&(change.previous_partition.expect("checked above"), change.id));
            }
        }
        if next_needed {
            let after = change.after.as_ref().expect("checked above").clone();
            rows.insert((after.partition, change.id), after);
        }
        peak_rows = peak_rows.max(count(rows.len()));
    }

    OverlayResult {
        rows: rows.values().map(WriterRow::normalize).collect(),
        tail_rows,
        tail_bytes,
        peak_rows,
    }
}

fn change_size(change: &Change) -> u64 {
    let base = 8_u64 + 8 + 1;
    let old = change
        .previous_partition
        .map_or(0, |partition| count(partition.len()));
    let after = change
        .after
        .as_ref()
        .map_or(0, |row| 8 + 1 + count(row.partition.len()) + 8 + 1);
    base.saturating_add(old).saturating_add(after)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApprovalDecision {
    Retry,
    Approved,
}

struct Scenario {
    seed: u64,
    mode: HtapContractMode,
    trace: Sha256,
    step: u64,
    anomaly_count: u64,
    first_mismatch_step: Option<u64>,
    first_mismatch: Option<String>,
    exact_checks: u64,
    tail_rows: u64,
    tail_bytes: u64,
    peak_rows: u64,
}

impl Scenario {
    fn new(seed: u64, mode: HtapContractMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-htap-contract-v1");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Self {
            seed,
            mode,
            trace,
            step: 0,
            anomaly_count: 0,
            first_mismatch_step: None,
            first_mismatch: None,
            exact_checks: 0,
            tail_rows: 0,
            tail_bytes: 0,
            peak_rows: 0,
        }
    }

    fn run(&mut self) {
        self.pushdown_poison();
        self.schema_partition_move();
        self.wal_pop_conflation();
        self.lease_gc_race();
        self.certificate_toctou();
        self.multi_table_snapshot();
    }

    fn pushdown_poison(&mut self) {
        let bump = self.seed % 17;
        let history = History {
            initial: vec![
                row_v1(1, Status::Open, "p0", 100 + bump),
                row_v1(2, Status::Open, "p0", 90 + bump),
            ],
            changes: vec![
                Change {
                    version: 11,
                    id: 1,
                    previous_partition: Some("p0"),
                    after: Some(row_v2(1, Status::Closed, "p0", 100 + bump, 1)),
                },
                Change {
                    version: 12,
                    id: 3,
                    previous_partition: None,
                    after: Some(row_v2(3, Status::Open, "p0", 80 + bump, 2)),
                },
            ],
        };
        let query = Query::pushdown_poison();
        let expected = history.oracle(12, query);
        let actual = self.run_overlay(&[history.base("p0", 10)], &history.changes, 12, query);
        self.check(
            "pushdown_poison",
            actual == expected,
            &format!("expected={expected:?}, actual={actual:?}"),
        );
    }

    fn schema_partition_move(&mut self) {
        let bump = self.seed % 19;
        let history = History {
            initial: vec![row_v1(7, Status::Open, "west", 500 + bump)],
            changes: vec![Change {
                version: 11,
                id: 7,
                previous_partition: Some("west"),
                after: Some(row_v2(7, Status::Open, "east", 505 + bump, 7)),
            }],
        };
        let query = Query::all();
        let expected = history.oracle(12, query);
        let actual = self.run_overlay(
            &[history.base("west", 10), history.base("east", 9)],
            &history.changes,
            12,
            query,
        );
        self.check(
            "schema_partition_move",
            actual == expected,
            &format!("expected={expected:?}, actual={actual:?}"),
        );
    }

    fn wal_pop_conflation(&mut self) {
        let bump = self.seed % 23;
        let history = History {
            initial: vec![row_v2(9, Status::Open, "p0", 40 + bump, 1)],
            changes: vec![Change {
                version: 21,
                id: 9,
                previous_partition: Some("p0"),
                after: Some(row_v2(9, Status::Open, "p0", 60 + bump, 2)),
            }],
        };
        let query = Query::all();
        let expected = history.oracle(25, query);
        let retained_changes = if self.mode == HtapContractMode::WalPopConflation {
            &[][..]
        } else {
            history.changes.as_slice()
        };
        let actual = self.run_overlay(&[history.base("p0", 20)], retained_changes, 25, query);
        let analytical_tail_retained = self.mode != HtapContractMode::WalPopConflation;
        self.check(
            "wal_pop_conflation",
            analytical_tail_retained && actual == expected,
            &format!(
                "recovery_watermark=25, base_watermark=20, tail_retained={analytical_tail_retained}, expected={expected:?}, actual={actual:?}"
            ),
        );
    }

    fn lease_gc_race(&mut self) {
        let bump = self.seed % 29;
        let history = History {
            initial: vec![row_v2(11, Status::Open, "p0", 100 + bump, 1)],
            changes: vec![
                Change {
                    version: 11,
                    id: 11,
                    previous_partition: Some("p0"),
                    after: Some(row_v2(11, Status::Open, "p0", 120 + bump, 2)),
                },
                Change {
                    version: 15,
                    id: 11,
                    previous_partition: Some("p0"),
                    after: Some(row_v2(11, Status::Open, "p0", 150 + bump, 3)),
                },
            ],
        };
        let query = Query::all();
        let expected = history.oracle(12, query);
        let (bases, changes, closure_matches_lease) = if self.mode == HtapContractMode::LeaseGcRace
        {
            (vec![history.base("p0", 15)], &[][..], false)
        } else {
            (
                vec![history.base("p0", 10)],
                history.changes.as_slice(),
                true,
            )
        };
        let actual = self.run_overlay(&bases, changes, 12, query);
        self.check(
            "lease_gc_race",
            closure_matches_lease && actual == expected,
            &format!(
                "lease_target=12, closure_matches_lease={closure_matches_lease}, expected={expected:?}, actual={actual:?}"
            ),
        );
    }

    fn certificate_toctou(&mut self) {
        let exposure_at_snapshot = 80 + self.seed % 3;
        let new_order = 15;
        let concurrent_order = 10;
        let limit = 100;
        let certificate_token = 30;
        let current_token = 31;
        let expected = ApprovalDecision::Retry;
        let actual = if self.mode == HtapContractMode::CertificateToctou {
            ApprovalDecision::Approved
        } else if certificate_token != current_token
            || exposure_at_snapshot + concurrent_order + new_order > limit
        {
            ApprovalDecision::Retry
        } else {
            ApprovalDecision::Approved
        };
        self.check(
            "certificate_toctou",
            actual == expected,
            &format!(
                "snapshot_exposure={exposure_at_snapshot}, concurrent={concurrent_order}, new={new_order}, limit={limit}, certificate_token={certificate_token}, current_token={current_token}, expected={expected:?}, actual={actual:?}"
            ),
        );
    }

    fn multi_table_snapshot(&mut self) {
        let bump = self.seed % 31;
        let orders = History {
            initial: vec![row_v2(21, Status::Open, "orders", 50 + bump, 1)],
            changes: vec![Change {
                version: 12,
                id: 21,
                previous_partition: Some("orders"),
                after: Some(row_v2(21, Status::Open, "orders", 70 + bump, 2)),
            }],
        };
        let customers = History {
            initial: vec![row_v2(42, Status::Open, "customers", 0, 1)],
            changes: vec![Change {
                version: 13,
                id: 42,
                previous_partition: Some("customers"),
                after: Some(row_v2(42, Status::Open, "customers", 0, 3)),
            }],
        };
        let query = Query::all();
        let expected = (orders.oracle(14, query), customers.oracle(14, query));
        let actual = (
            self.run_overlay(&[orders.base("orders", 10)], &orders.changes, 14, query),
            self.run_overlay(
                &[customers.base("customers", 11)],
                &customers.changes,
                14,
                query,
            ),
        );
        self.check(
            "multi_table_snapshot",
            actual == expected,
            &format!("target=14, expected={expected:?}, actual={actual:?}"),
        );
    }

    fn run_overlay(
        &mut self,
        bases: &[BasePartition],
        changes: &[Change],
        target: u64,
        query: Query,
    ) -> Vec<ProjectedRow> {
        let result = overlay(bases, changes, target, query, self.mode);
        self.tail_rows = self.tail_rows.saturating_add(result.tail_rows);
        self.tail_bytes = self.tail_bytes.saturating_add(result.tail_bytes);
        self.peak_rows = self.peak_rows.max(result.peak_rows);
        query.execute(result.rows)
    }

    fn check(&mut self, action: &str, passed: bool, detail: &str) {
        self.step += 1;
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(action.as_bytes());
        self.trace.update([u8::from(passed)]);
        self.trace.update(detail.as_bytes());
        if passed {
            self.exact_checks += 1;
        } else {
            self.anomaly_count += 1;
            if self.first_mismatch.is_none() {
                self.first_mismatch_step = Some(self.step);
                self.first_mismatch = Some(format!("{action}: {detail}"));
            }
        }
    }

    fn report(&self) -> HtapContractReport {
        HtapContractReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: self.anomaly_count,
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch.clone(),
            exact_checks: self.exact_checks,
            tail_rows: self.tail_rows,
            tail_bytes: self.tail_bytes,
            peak_memory_bytes: self.peak_rows.saturating_mul(128),
            spill_bytes: 0,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

/// Run the deterministic `ZebraDB` base-plus-tail exactness contract model.
#[must_use]
pub fn run_htap_contract(seed: u64, mode: HtapContractMode) -> HtapContractReport {
    let mut scenario = Scenario::new(seed, mode);
    scenario.run();
    scenario.report()
}

fn row_v1(id: u64, status: Status, partition: &'static str, total_cents: u64) -> WriterRow {
    WriterRow {
        id,
        status,
        partition,
        fields: WriterFields::V1 { total_cents },
    }
}

fn row_v2(
    id: u64,
    status: Status,
    partition: &'static str,
    amount_cents: u64,
    priority: u8,
) -> WriterRow {
    WriterRow {
        id,
        status,
        partition,
        fields: WriterFields::V2 {
            amount_cents,
            priority,
        },
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
    fn correct_contract_is_exactly_replayable() {
        let first = run_htap_contract(1103, HtapContractMode::Correct);
        let second = run_htap_contract(1103, HtapContractMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert_eq!(first.exact_checks, 6);
        assert!(first.tail_rows > 0);
    }

    #[test]
    fn every_negative_control_has_one_bounded_failure() {
        let cases = [
            (HtapContractMode::PushdownPoison, 1),
            (HtapContractMode::SchemaPartitionMove, 2),
            (HtapContractMode::WalPopConflation, 3),
            (HtapContractMode::LeaseGcRace, 4),
            (HtapContractMode::CertificateToctou, 5),
        ];
        for (mode, step) in cases {
            let report = run_htap_contract(1103, mode);
            assert_eq!(report.anomaly_count, 1, "{mode:?}");
            assert_eq!(report.first_mismatch_step, Some(step), "{mode:?}");
            assert_eq!(report.exact_checks, 5, "{mode:?}");
        }
    }

    #[test]
    fn seed_changes_the_trace_without_changing_the_contract() {
        let first = run_htap_contract(1103, HtapContractMode::Correct);
        let second = run_htap_contract(2207, HtapContractMode::Correct);
        assert_ne!(first.trace_sha256, second.trace_sha256);
        assert_eq!(first.anomaly_count, second.anomaly_count);
    }
}
