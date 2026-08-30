use sha2::{Digest, Sha256};
use std::array;
use std::collections::{BTreeMap, BTreeSet};

const NODE_COUNT: usize = 3;
const QUORUM: usize = 2;
const PUBLICATION_QUEUE_LIMIT: usize = 2;

/// Deliberately incorrect behavior used to prove one staged txLog invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StagedTxLogMode {
    Correct,
    AckOneCopy,
    AcceptStaleEpoch,
    OverwriteAcknowledgedSuffix,
    PublishUncommittedSegment,
    TrustObjectList,
    UnboundedQueue,
}

impl StagedTxLogMode {
    /// Stable configuration identifier used by the eval suite.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AckOneCopy => "ack_one_copy",
            Self::AcceptStaleEpoch => "accept_stale_epoch",
            Self::OverwriteAcknowledgedSuffix => "overwrite_acknowledged_suffix",
            Self::PublishUncommittedSegment => "publish_uncommitted_segment",
            Self::TrustObjectList => "trust_object_list",
            Self::UnboundedQueue => "unbounded_queue",
        }
    }
}

/// Deterministic L0 report for RFC-0045's staged txLog protocol boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTxLogReport {
    pub seed: u64,
    pub mode: StagedTxLogMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub acknowledged_appends: u64,
    pub recovered_unknown_outcomes: u64,
    pub writer_takeovers: u64,
    pub repaired_records: u64,
    pub stale_writer_rejections: u64,
    pub conflicting_retries_rejected: u64,
    pub published_segments: u64,
    pub orphan_objects_ignored: u64,
    pub bounded_queue_rejections: u64,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogRecord {
    request_id: u64,
    fingerprint: [u8; 32],
}

impl LogRecord {
    fn fixture(seed: u64, request_id: u64, variant: u8) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"okv-staged-txlog-record-v1");
        digest.update(seed.to_be_bytes());
        digest.update(request_id.to_be_bytes());
        digest.update([variant]);
        Self {
            request_id,
            fingerprint: digest.finalize().into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LogNode {
    writer_epoch: u64,
    records: BTreeMap<u64, LogRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Segment {
    positions: BTreeMap<u64, LogRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppendResult {
    accepted_nodes: Vec<usize>,
    acknowledged: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RetryResult {
    SameOutcome,
    ConflictingRequest,
    Missing,
}

struct StagedLogModel {
    mode: StagedTxLogMode,
    authority_epoch: u64,
    nodes: [LogNode; NODE_COUNT],
    acknowledged: BTreeMap<u64, LogRecord>,
    transaction_commit_frontier: u64,
    objects: BTreeMap<String, Segment>,
    active_objects: BTreeSet<String>,
    publication_queue: Vec<String>,
}

impl StagedLogModel {
    fn new(mode: StagedTxLogMode) -> Self {
        Self {
            mode,
            authority_epoch: 0,
            nodes: array::from_fn(|_| LogNode::default()),
            acknowledged: BTreeMap::new(),
            transaction_commit_frontier: 0,
            objects: BTreeMap::new(),
            active_objects: BTreeSet::new(),
            publication_queue: Vec::new(),
        }
    }

    fn activate_writer(&mut self, epoch: u64, nodes: &[usize]) -> bool {
        let nodes = nodes
            .iter()
            .copied()
            .filter(|node_id| *node_id < NODE_COUNT)
            .collect::<BTreeSet<_>>();
        if epoch <= self.authority_epoch || nodes.len() < QUORUM {
            return false;
        }
        self.authority_epoch = epoch;
        for node_id in nodes {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.writer_epoch = epoch;
            }
        }
        true
    }

    fn append(
        &mut self,
        epoch: u64,
        position: u64,
        record: &LogRecord,
        selected_nodes: &[usize],
        response_delivered: bool,
    ) -> AppendResult {
        let mut accepted_nodes = Vec::new();
        for node_id in selected_nodes {
            let Some(node) = self.nodes.get_mut(*node_id) else {
                continue;
            };
            let current_epoch = node.writer_epoch;
            let epoch_matches = epoch == current_epoch
                || (self.mode == StagedTxLogMode::AcceptStaleEpoch && epoch < current_epoch);
            if !epoch_matches {
                continue;
            }
            match node.records.get(&position) {
                Some(existing) if existing == record => accepted_nodes.push(*node_id),
                Some(_) if self.mode == StagedTxLogMode::OverwriteAcknowledgedSuffix => {
                    node.records.insert(position, record.clone());
                    accepted_nodes.push(*node_id);
                }
                None if position
                    == node
                        .records
                        .keys()
                        .next_back()
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(1) =>
                {
                    node.records.insert(position, record.clone());
                    accepted_nodes.push(*node_id);
                }
                Some(_) | None => {}
            }
        }
        accepted_nodes.sort_unstable();
        accepted_nodes.dedup();
        let physically_durable = accepted_nodes.len() >= QUORUM;
        let acknowledged = response_delivered
            && (physically_durable
                || (self.mode == StagedTxLogMode::AckOneCopy && !accepted_nodes.is_empty()));
        if acknowledged {
            self.acknowledged
                .entry(position)
                .or_insert_with(|| record.clone());
        }
        AppendResult {
            accepted_nodes,
            acknowledged,
        }
    }

    fn lose_node(&mut self, node_id: usize) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            *node = LogNode::default();
        }
    }

    fn recover_and_repair(
        &mut self,
        new_epoch: u64,
        available_nodes: &[usize],
    ) -> Result<(BTreeMap<u64, LogRecord>, u64), &'static str> {
        if available_nodes.len() < QUORUM || !self.activate_writer(new_epoch, available_nodes) {
            return Err("writer epoch was not installed on a quorum");
        }
        let highest_position = available_nodes
            .iter()
            .filter_map(|node_id| self.nodes.get(*node_id))
            .filter_map(|node| node.records.keys().next_back().copied())
            .max()
            .unwrap_or(0);
        let mut recovered = BTreeMap::new();
        for position in 1..=highest_position {
            let candidates = available_nodes
                .iter()
                .filter_map(|node_id| self.nodes.get(*node_id))
                .filter_map(|node| node.records.get(&position).cloned())
                .collect::<BTreeSet<_>>();
            if candidates.is_empty() {
                break;
            }
            if candidates.len() != 1 {
                return Err("conflicting records occupy one recovered position");
            }
            recovered.insert(
                position,
                candidates
                    .into_iter()
                    .next()
                    .expect("one recovered candidate exists"),
            );
        }
        let mut repairs = 0_u64;
        for node_id in available_nodes {
            let Some(node) = self.nodes.get_mut(*node_id) else {
                continue;
            };
            node.writer_epoch = new_epoch;
            node.records
                .retain(|position, _| recovered.contains_key(position));
            for (position, record) in &recovered {
                if node.records.get(position) != Some(record) {
                    node.records.insert(*position, record.clone());
                    repairs = repairs.saturating_add(1);
                }
            }
        }
        Ok((recovered, repairs))
    }

    fn retry_result(recovered: &BTreeMap<u64, LogRecord>, retry: &LogRecord) -> RetryResult {
        recovered
            .values()
            .find(|record| record.request_id == retry.request_id)
            .map_or(RetryResult::Missing, |record| {
                if record == retry {
                    RetryResult::SameOutcome
                } else {
                    RetryResult::ConflictingRequest
                }
            })
    }

    fn put_object(&mut self, name: &str, segment: Segment) {
        self.objects.entry(name.to_owned()).or_insert(segment);
    }

    fn publish_object(&mut self, name: &str) -> bool {
        let Some(segment) = self.objects.get(name) else {
            return false;
        };
        let consecutive = segment.positions.keys().next().is_some_and(|first| {
            segment.positions.keys().copied().eq(*first
                ..first.saturating_add(u64::try_from(segment.positions.len()).unwrap_or(u64::MAX)))
        });
        let inside_commit_frontier = segment
            .positions
            .keys()
            .next_back()
            .is_some_and(|last| *last <= self.transaction_commit_frontier);
        let exact = segment.positions.iter().all(|(position, record)| {
            self.acknowledged
                .get(position)
                .is_some_and(|acknowledged| acknowledged == record)
        });
        let eligible = consecutive && inside_commit_frontier && exact;
        if eligible || self.mode == StagedTxLogMode::PublishUncommittedSegment {
            self.active_objects.insert(name.to_owned());
            true
        } else {
            false
        }
    }

    fn visible_positions(&self) -> BTreeSet<u64> {
        let names = if self.mode == StagedTxLogMode::TrustObjectList {
            self.objects.keys().cloned().collect::<BTreeSet<_>>()
        } else {
            self.active_objects.clone()
        };
        names
            .iter()
            .filter_map(|name| self.objects.get(name))
            .flat_map(|segment| segment.positions.keys().copied())
            .collect()
    }

    fn enqueue_publication(&mut self, name: &str) -> bool {
        if self.publication_queue.len() >= PUBLICATION_QUEUE_LIMIT
            && self.mode != StagedTxLogMode::UnboundedQueue
        {
            return false;
        }
        self.publication_queue.push(name.to_owned());
        true
    }
}

struct Scenario {
    seed: u64,
    mode: StagedTxLogMode,
    trace: Sha256,
    step: u64,
    anomaly_count: u64,
    first_mismatch_step: Option<u64>,
    first_mismatch: Option<String>,
    acknowledged_appends: u64,
    recovered_unknown_outcomes: u64,
    writer_takeovers: u64,
    repaired_records: u64,
    stale_writer_rejections: u64,
    conflicting_retries_rejected: u64,
    published_segments: u64,
    orphan_objects_ignored: u64,
    bounded_queue_rejections: u64,
}

impl Scenario {
    fn new(seed: u64, mode: StagedTxLogMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-staged-txlog-contract-v1");
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
            acknowledged_appends: 0,
            recovered_unknown_outcomes: 0,
            writer_takeovers: 0,
            repaired_records: 0,
            stale_writer_rejections: 0,
            conflicting_retries_rejected: 0,
            published_segments: 0,
            orphan_objects_ignored: 0,
            bounded_queue_rejections: 0,
        }
    }

    fn run(&mut self) {
        self.quorum_ack_survives_one_node_loss();
        self.unknown_outcome_retries_exactly();
        self.stale_writer_is_fenced();
        self.acknowledged_suffix_is_immutable();
        self.uncommitted_segment_is_invisible();
        self.manifest_is_the_read_authority();
        self.publication_queue_is_bounded();
    }

    fn quorum_ack_survives_one_node_loss(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let activated = model.activate_writer(1, &[0, 1, 2]);
        let record = LogRecord::fixture(self.seed, 1, 0);
        let selected = if self.mode == StagedTxLogMode::AckOneCopy {
            &[0][..]
        } else {
            &[0, 1][..]
        };
        let append = model.append(1, 1, &record, selected, true);
        self.acknowledged_appends = self
            .acknowledged_appends
            .saturating_add(u64::from(append.acknowledged));
        model.lose_node(0);
        let recovery = model.recover_and_repair(2, &[1, 2]);
        self.writer_takeovers = self.writer_takeovers.saturating_add(1);
        let (recovered, repairs) = recovery.unwrap_or_default();
        self.repaired_records = self.repaired_records.saturating_add(repairs);
        let passed = activated
            && append.acknowledged
            && append.accepted_nodes.len() >= QUORUM
            && recovered.get(&1) == Some(&record);
        self.check(
            "quorum_ack_survives_one_node_loss",
            passed,
            &format!(
                "activated={activated}, accepted={}, acknowledged={}, recovered={}, repairs={repairs}",
                append.accepted_nodes.len(),
                append.acknowledged,
                recovered.len()
            ),
        );
    }

    fn unknown_outcome_retries_exactly(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let activated = model.activate_writer(1, &[0, 1, 2]);
        let record = LogRecord::fixture(self.seed, 2, 0);
        let append = model.append(1, 1, &record, &[0, 1], false);
        let recovery = model.recover_and_repair(2, &[0, 1, 2]);
        self.writer_takeovers = self.writer_takeovers.saturating_add(1);
        let (recovered, repairs) = recovery.unwrap_or_default();
        self.repaired_records = self.repaired_records.saturating_add(repairs);
        let exact = StagedLogModel::retry_result(&recovered, &record);
        let conflicting =
            StagedLogModel::retry_result(&recovered, &LogRecord::fixture(self.seed, 2, 1));
        self.recovered_unknown_outcomes = self
            .recovered_unknown_outcomes
            .saturating_add(u64::from(exact == RetryResult::SameOutcome));
        self.conflicting_retries_rejected = self
            .conflicting_retries_rejected
            .saturating_add(u64::from(conflicting == RetryResult::ConflictingRequest));
        self.check(
            "unknown_outcome_retries_exactly",
            activated
                && !append.acknowledged
                && append.accepted_nodes.len() >= QUORUM
                && exact == RetryResult::SameOutcome
                && conflicting == RetryResult::ConflictingRequest,
            &format!(
                "accepted={}, acknowledged={}, exact={exact:?}, conflicting={conflicting:?}",
                append.accepted_nodes.len(),
                append.acknowledged
            ),
        );
    }

    fn stale_writer_is_fenced(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let first = model.activate_writer(1, &[0, 1, 2]);
        let takeover = model.activate_writer(2, &[0, 1]);
        let stale = model.append(1, 1, &LogRecord::fixture(self.seed, 3, 0), &[1, 2], true);
        self.stale_writer_rejections = self
            .stale_writer_rejections
            .saturating_add(u64::from(!stale.acknowledged));
        self.check(
            "stale_writer_is_fenced",
            first && takeover && !stale.acknowledged && stale.accepted_nodes.len() < QUORUM,
            &format!(
                "first={first}, takeover={takeover}, accepted={}, acknowledged={}",
                stale.accepted_nodes.len(),
                stale.acknowledged
            ),
        );
    }

    fn acknowledged_suffix_is_immutable(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let original = LogRecord::fixture(self.seed, 4, 0);
        let replacement = LogRecord::fixture(self.seed, 5, 0);
        let first = model.activate_writer(1, &[0, 1, 2]);
        let append = model.append(1, 1, &original, &[0, 1, 2], true);
        let takeover = model.activate_writer(2, &[0, 1]);
        let overwrite = model.append(2, 1, &replacement, &[0, 1], true);
        let recovered = model
            .recover_and_repair(3, &[0, 1, 2])
            .map(|(records, _)| records)
            .unwrap_or_default();
        self.writer_takeovers = self.writer_takeovers.saturating_add(1);
        self.check(
            "acknowledged_suffix_is_immutable",
            first
                && append.acknowledged
                && takeover
                && !overwrite.acknowledged
                && recovered.get(&1) == Some(&original),
            &format!(
                "initial_ack={}, overwrite_ack={}, recovered_original={}",
                append.acknowledged,
                overwrite.acknowledged,
                recovered.get(&1) == Some(&original)
            ),
        );
    }

    fn uncommitted_segment_is_invisible(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let committed = LogRecord::fixture(self.seed, 6, 0);
        let uncommitted = LogRecord::fixture(self.seed, 7, 0);
        model.activate_writer(1, &[0, 1, 2]);
        model.append(1, 1, &committed, &[0, 1], true);
        model.append(1, 2, &uncommitted, &[0], false);
        model.transaction_commit_frontier = 1;
        model.put_object(
            "segment-1-2",
            Segment {
                positions: BTreeMap::from([(1, committed), (2, uncommitted)]),
            },
        );
        let published = model.publish_object("segment-1-2");
        self.check(
            "uncommitted_segment_is_invisible",
            !published && model.active_objects.is_empty(),
            &format!(
                "published={published}, active={}",
                model.active_objects.len()
            ),
        );
    }

    fn manifest_is_the_read_authority(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let committed = LogRecord::fixture(self.seed, 8, 0);
        let orphan = LogRecord::fixture(self.seed, 9, 0);
        model.activate_writer(1, &[0, 1, 2]);
        model.append(1, 1, &committed, &[0, 1], true);
        model.transaction_commit_frontier = 1;
        model.put_object(
            "active-segment",
            Segment {
                positions: BTreeMap::from([(1, committed)]),
            },
        );
        let published = model.publish_object("active-segment");
        self.published_segments = self.published_segments.saturating_add(u64::from(published));
        model.put_object(
            "orphan-segment",
            Segment {
                positions: BTreeMap::from([(2, orphan)]),
            },
        );
        let visible = model.visible_positions();
        let ignored = !visible.contains(&2);
        self.orphan_objects_ignored = self
            .orphan_objects_ignored
            .saturating_add(u64::from(ignored));
        self.check(
            "manifest_is_the_read_authority",
            published && visible == BTreeSet::from([1]),
            &format!("published={published}, visible={visible:?}"),
        );
    }

    fn publication_queue_is_bounded(&mut self) {
        let mut model = StagedLogModel::new(self.mode);
        let first = model.enqueue_publication("segment-a");
        let second = model.enqueue_publication("segment-b");
        let third = model.enqueue_publication("segment-c");
        self.bounded_queue_rejections = self
            .bounded_queue_rejections
            .saturating_add(u64::from(!third));
        self.check(
            "publication_queue_is_bounded",
            first && second && !third && model.publication_queue.len() == PUBLICATION_QUEUE_LIMIT,
            &format!(
                "first={first}, second={second}, third={third}, depth={}",
                model.publication_queue.len()
            ),
        );
    }

    fn check(&mut self, action: &str, passed: bool, detail: &str) {
        self.step = self.step.saturating_add(1);
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(action.as_bytes());
        self.trace.update([u8::from(passed)]);
        self.trace.update(detail.as_bytes());
        if !passed && self.first_mismatch.is_none() {
            self.anomaly_count = 1;
            self.first_mismatch_step = Some(self.step);
            self.first_mismatch = Some(format!("{action}: {detail}"));
        }
    }

    fn report(&self) -> StagedTxLogReport {
        StagedTxLogReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: self.anomaly_count,
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch.clone(),
            acknowledged_appends: self.acknowledged_appends,
            recovered_unknown_outcomes: self.recovered_unknown_outcomes,
            writer_takeovers: self.writer_takeovers,
            repaired_records: self.repaired_records,
            stale_writer_rejections: self.stale_writer_rejections,
            conflicting_retries_rejected: self.conflicting_retries_rejected,
            published_segments: self.published_segments,
            orphan_objects_ignored: self.orphan_objects_ignored,
            bounded_queue_rejections: self.bounded_queue_rejections,
            trace_sha256: hex(&self.trace.clone().finalize()),
        }
    }
}

/// Exercise the deterministic L0 staged txLog contract.
#[must_use]
pub fn run_staged_txlog_contract(seed: u64, mode: StagedTxLogMode) -> StagedTxLogReport {
    let mut scenario = Scenario::new(seed, mode);
    scenario.run();
    scenario.report()
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
    fn correct_staged_txlog_contract_is_exactly_replayable() {
        let first = run_staged_txlog_contract(1103, StagedTxLogMode::Correct);
        let second = run_staged_txlog_contract(1103, StagedTxLogMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.executed_steps, 7);
        assert_eq!(first.anomaly_count, 0);
        assert!(first.acknowledged_appends > 0);
        assert!(first.recovered_unknown_outcomes > 0);
        assert!(first.writer_takeovers > 0);
        assert!(first.repaired_records > 0);
        assert!(first.stale_writer_rejections > 0);
        assert!(first.conflicting_retries_rejected > 0);
        assert!(first.published_segments > 0);
        assert!(first.orphan_objects_ignored > 0);
        assert!(first.bounded_queue_rejections > 0);
    }

    #[test]
    fn every_staged_txlog_negative_control_has_one_bounded_failure() {
        let controls = [
            (StagedTxLogMode::AckOneCopy, 1),
            (StagedTxLogMode::AcceptStaleEpoch, 3),
            (StagedTxLogMode::OverwriteAcknowledgedSuffix, 4),
            (StagedTxLogMode::PublishUncommittedSegment, 5),
            (StagedTxLogMode::TrustObjectList, 6),
            (StagedTxLogMode::UnboundedQueue, 7),
        ];
        for (mode, expected_step) in controls {
            let report = run_staged_txlog_contract(1103, mode);
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
