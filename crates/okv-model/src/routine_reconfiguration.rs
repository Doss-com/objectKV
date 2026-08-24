use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

/// One deliberately unsafe routine-repair behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutineReconfigurationMode {
    Correct,
    ReuseNodeIdentity,
    AdmitLearnerWithoutAuthority,
    PromoteBeforeCatchup,
    AcceptStaleMembershipEpoch,
    AcceptConcurrentReconfiguration,
    DoubleApplyFinalizeRetry,
    AcceptRemovedVoterCommit,
    RepairWithoutDataQuorum,
}

impl RoutineReconfigurationMode {
    /// Stable configuration identity used by eval suites and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ReuseNodeIdentity => "reuse_node_identity",
            Self::AdmitLearnerWithoutAuthority => "admit_learner_without_authority",
            Self::PromoteBeforeCatchup => "promote_before_catchup",
            Self::AcceptStaleMembershipEpoch => "accept_stale_membership_epoch",
            Self::AcceptConcurrentReconfiguration => "accept_concurrent_reconfiguration",
            Self::DoubleApplyFinalizeRetry => "double_apply_finalize_retry",
            Self::AcceptRemovedVoterCommit => "accept_removed_voter_commit",
            Self::RepairWithoutDataQuorum => "repair_without_data_quorum",
        }
    }
}

/// Deterministic result of the routine voter-reconfiguration contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineReconfigurationReport {
    pub seed: u64,
    pub mode: RoutineReconfigurationMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_preparations: u64,
    pub learner_admissions: u64,
    pub learner_ready_certificates: u64,
    pub membership_changes: u64,
    pub finalize_attempts: u64,
    pub committed_transactions: u64,
    pub rejected_controls: u64,
    pub generation: u64,
    pub membership_epoch: u64,
    pub active_voters: Vec<u64>,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingReconfiguration {
    id: u64,
    expected_epoch: u64,
    old_voters: BTreeSet<u64>,
    next_voters: BTreeSet<u64>,
    replacement_node: u64,
    replacement_incarnation: [u8; 16],
    learner_ready_position: Option<u64>,
    membership_position: Option<u64>,
}

#[derive(Clone, Debug)]
struct AuthorityModel {
    generation: u64,
    membership_epoch: u64,
    active_voters: BTreeSet<u64>,
    active_incarnations: BTreeMap<u64, [u8; 16]>,
    pending: Option<PendingReconfiguration>,
    completed: BTreeMap<u64, (u64, BTreeSet<u64>)>,
    mode: RoutineReconfigurationMode,
}

impl AuthorityModel {
    fn new(mode: RoutineReconfigurationMode) -> Self {
        Self {
            generation: 7,
            membership_epoch: 4,
            active_voters: BTreeSet::from([1, 2, 3]),
            active_incarnations: BTreeMap::from([
                (1, [0x11; 16]),
                (2, [0x22; 16]),
                (3, [0x33; 16]),
            ]),
            pending: None,
            completed: BTreeMap::new(),
            mode,
        }
    }

    fn prepare(
        &mut self,
        expected_generation: u64,
        expected_epoch: u64,
        id: u64,
        next_voters: BTreeSet<u64>,
        replacement_node: u64,
        replacement_incarnation: [u8; 16],
    ) -> bool {
        let stale_epoch = expected_epoch != self.membership_epoch;
        if expected_generation != self.generation
            || (stale_epoch && self.mode != RoutineReconfigurationMode::AcceptStaleMembershipEpoch)
            || id == 0
            || next_voters.len() != self.active_voters.len()
            || !next_voters.contains(&replacement_node)
        {
            return false;
        }
        if self.active_voters.contains(&replacement_node)
            && self.mode != RoutineReconfigurationMode::ReuseNodeIdentity
        {
            return false;
        }
        if self
            .active_incarnations
            .values()
            .any(|existing| existing == &replacement_incarnation)
            && self.mode != RoutineReconfigurationMode::ReuseNodeIdentity
        {
            return false;
        }
        let candidate = PendingReconfiguration {
            id,
            expected_epoch,
            old_voters: self.active_voters.clone(),
            next_voters,
            replacement_node,
            replacement_incarnation,
            learner_ready_position: None,
            membership_position: None,
        };
        if let Some(pending) = &self.pending {
            if pending == &candidate {
                return true;
            }
            if self.mode != RoutineReconfigurationMode::AcceptConcurrentReconfiguration {
                return false;
            }
        }
        self.pending = Some(candidate);
        true
    }

    fn authorize_learner(&self, id: u64, node_id: u64, incarnation: [u8; 16]) -> bool {
        self.pending.as_ref().is_some_and(|pending| {
            pending.id == id
                && pending.replacement_node == node_id
                && pending.replacement_incarnation == incarnation
        }) || self.mode == RoutineReconfigurationMode::AdmitLearnerWithoutAuthority
    }

    fn mark_learner_ready(
        &mut self,
        id: u64,
        snapshot_position: u64,
        applied_position: u64,
        next_voters: &BTreeSet<u64>,
    ) -> bool {
        let Some(pending) = self.pending.as_mut() else {
            return false;
        };
        if pending.id != id
            || snapshot_position == 0
            || applied_position < snapshot_position
            || &pending.next_voters != next_voters
        {
            return false;
        }
        pending.learner_ready_position = Some(applied_position);
        true
    }

    fn commit_membership(
        &mut self,
        id: u64,
        next_voters: &BTreeSet<u64>,
        membership_position: u64,
    ) -> bool {
        let Some(pending) = self.pending.as_mut() else {
            return false;
        };
        let ready = pending
            .learner_ready_position
            .is_some_and(|ready_position| membership_position > ready_position);
        if pending.id != id
            || &pending.next_voters != next_voters
            || (!ready && self.mode != RoutineReconfigurationMode::PromoteBeforeCatchup)
        {
            return false;
        }
        pending.membership_position = Some(membership_position);
        true
    }

    fn finalize(&mut self, id: u64, certified_position: u64) -> bool {
        if let Some((_, voters)) = self.completed.get(&id).cloned() {
            if self.mode == RoutineReconfigurationMode::DoubleApplyFinalizeRetry {
                self.membership_epoch = self.membership_epoch.saturating_add(1);
                self.active_voters = voters;
            }
            return true;
        }
        let Some(pending) = self.pending.take() else {
            return false;
        };
        if pending.id != id || pending.membership_position != Some(certified_position) {
            self.pending = Some(pending);
            return false;
        }
        self.active_voters.clone_from(&pending.next_voters);
        self.active_incarnations
            .retain(|node_id, _| self.active_voters.contains(node_id));
        self.active_incarnations
            .insert(pending.replacement_node, pending.replacement_incarnation);
        self.membership_epoch = self.membership_epoch.saturating_add(1);
        self.completed
            .insert(id, (self.membership_epoch, self.active_voters.clone()));
        true
    }

    fn can_commit(&self, node_id: u64, generation: u64, quorum_available: bool) -> bool {
        if !quorum_available || generation != self.generation {
            return false;
        }
        self.active_voters.contains(&node_id)
            || (self.mode == RoutineReconfigurationMode::AcceptRemovedVoterCommit && node_id == 1)
    }

    fn can_complete_routine_repair(&self, quorum_available: bool) -> bool {
        quorum_available || self.mode == RoutineReconfigurationMode::RepairWithoutDataQuorum
    }
}

struct Scenario {
    seed: u64,
    mode: RoutineReconfigurationMode,
    authority: AuthorityModel,
    trace: Sha256,
    step: u64,
    anomaly_count: u64,
    first_mismatch_step: Option<u64>,
    first_mismatch: Option<String>,
    authority_preparations: u64,
    learner_admissions: u64,
    learner_ready_certificates: u64,
    membership_changes: u64,
    finalize_attempts: u64,
    committed_transactions: u64,
    rejected_controls: u64,
}

impl Scenario {
    fn new(seed: u64, mode: RoutineReconfigurationMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"okv-routine-reconfiguration-v0");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Self {
            seed,
            mode,
            authority: AuthorityModel::new(mode),
            trace,
            step: 0,
            anomaly_count: 0,
            first_mismatch_step: None,
            first_mismatch: None,
            authority_preparations: 0,
            learner_admissions: 0,
            learner_ready_certificates: 0,
            membership_changes: 0,
            finalize_attempts: 0,
            committed_transactions: 0,
            rejected_controls: 0,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn run(mut self) -> RoutineReconfigurationReport {
        let generation = self.authority.generation;
        let initial_epoch = self.authority.membership_epoch;
        let expected_next_voters = BTreeSet::from([2, 3, 4]);
        let next_voters = if self.mode == RoutineReconfigurationMode::ReuseNodeIdentity {
            BTreeSet::from([1, 2, 3])
        } else {
            expected_next_voters.clone()
        };
        let reconfiguration_id = self.seed.max(1);
        let replacement_node = if self.mode == RoutineReconfigurationMode::ReuseNodeIdentity {
            1
        } else {
            4
        };
        let replacement_incarnation = if self.mode == RoutineReconfigurationMode::ReuseNodeIdentity
        {
            [0x11; 16]
        } else {
            incarnation(self.seed)
        };

        let unauthorized = self.authority.authorize_learner(
            reconfiguration_id,
            replacement_node,
            replacement_incarnation,
        );
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(!unauthorized));
        self.check("learner_requires_authority", !unauthorized);

        let mut stale_probe = self.authority.clone();
        let stale_accepted = stale_probe.prepare(
            generation,
            initial_epoch.saturating_sub(1),
            reconfiguration_id,
            next_voters.clone(),
            replacement_node,
            replacement_incarnation,
        );
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(!stale_accepted));
        self.check("stale_membership_epoch_rejected", !stale_accepted);

        let prepared = self.authority.prepare(
            generation,
            initial_epoch,
            reconfiguration_id,
            next_voters.clone(),
            replacement_node,
            replacement_incarnation,
        );
        self.authority_preparations = self
            .authority_preparations
            .saturating_add(u64::from(prepared));
        self.check("reconfiguration_prepared", prepared);
        self.check(
            "replacement_identity_and_incarnation_are_fresh",
            replacement_node == 4 && replacement_incarnation != [0x11; 16],
        );
        self.check(
            "prepare_preserves_generation",
            self.authority.generation == generation,
        );

        let mut concurrent_probe = self.authority.clone();
        let concurrent = concurrent_probe.prepare(
            generation,
            initial_epoch,
            reconfiguration_id.saturating_add(1),
            BTreeSet::from([1, 3, 5]),
            5,
            [0x55; 16],
        );
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(!concurrent));
        self.check("concurrent_reconfiguration_rejected", !concurrent);

        let authorized = self.authority.authorize_learner(
            reconfiguration_id,
            replacement_node,
            replacement_incarnation,
        );
        self.learner_admissions = self
            .learner_admissions
            .saturating_add(u64::from(authorized));
        self.check("prepared_learner_is_authorized", authorized);

        let during_catchup = self.authority.can_commit(2, generation, true);
        self.committed_transactions = self
            .committed_transactions
            .saturating_add(u64::from(during_catchup));
        self.check("commits_continue_during_catchup", during_catchup);

        let mut early_probe = self.authority.clone();
        let early_promoted = early_probe.commit_membership(reconfiguration_id, &next_voters, 10);
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(!early_promoted));
        self.check("promotion_requires_snapshot_and_suffix", !early_promoted);

        let learner_ready =
            self.authority
                .mark_learner_ready(reconfiguration_id, 8, 11, &next_voters);
        self.learner_ready_certificates = self
            .learner_ready_certificates
            .saturating_add(u64::from(learner_ready));
        self.check("learner_ready_binds_exact_position", learner_ready);

        let membership_committed =
            self.authority
                .commit_membership(reconfiguration_id, &next_voters, 12);
        self.membership_changes = self
            .membership_changes
            .saturating_add(u64::from(membership_committed));
        self.check("membership_change_committed_once", membership_committed);

        let finalized = self.authority.finalize(reconfiguration_id, 12);
        self.finalize_attempts = self.finalize_attempts.saturating_add(1);
        self.check("authority_finalizes_exact_membership", finalized);
        let retry = self.authority.finalize(reconfiguration_id, 12);
        self.finalize_attempts = self.finalize_attempts.saturating_add(1);
        self.check("lost_finalize_reply_retries_exactly", retry);
        self.check(
            "membership_epoch_advances_once",
            self.authority.membership_epoch == initial_epoch.saturating_add(1),
        );
        self.check(
            "active_voter_set_is_exact",
            self.authority.active_voters == expected_next_voters,
        );
        self.check(
            "routine_repair_preserves_generation",
            self.authority.generation == generation,
        );

        let removed_rejected = !self.authority.can_commit(1, generation, true);
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(removed_rejected));
        self.check("removed_voter_cannot_commit", removed_rejected);
        let repaired_commit = self.authority.can_commit(4, generation, true);
        self.committed_transactions = self
            .committed_transactions
            .saturating_add(u64::from(repaired_commit));
        self.check("new_voter_set_can_commit", repaired_commit);

        let no_quorum_rejected = !self.authority.can_complete_routine_repair(false);
        self.rejected_controls = self
            .rejected_controls
            .saturating_add(u64::from(no_quorum_rejected));
        self.check(
            "lost_data_quorum_requires_generation_recovery",
            no_quorum_rejected,
        );

        RoutineReconfigurationReport {
            seed: self.seed,
            mode: self.mode,
            executed_checks: self.step,
            anomaly_count: self.anomaly_count,
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch,
            authority_preparations: self.authority_preparations,
            learner_admissions: self.learner_admissions,
            learner_ready_certificates: self.learner_ready_certificates,
            membership_changes: self.membership_changes,
            finalize_attempts: self.finalize_attempts,
            committed_transactions: self.committed_transactions,
            rejected_controls: self.rejected_controls,
            generation: self.authority.generation,
            membership_epoch: self.authority.membership_epoch,
            active_voters: self.authority.active_voters.iter().copied().collect(),
            trace_sha256: digest_hex(self.trace),
        }
    }

    fn check(&mut self, invariant: &str, passed: bool) {
        self.step = self.step.saturating_add(1);
        self.trace.update(self.step.to_be_bytes());
        self.trace.update(invariant.as_bytes());
        self.trace.update([u8::from(passed)]);
        if !passed {
            self.anomaly_count = self.anomaly_count.saturating_add(1);
            if self.first_mismatch.is_none() {
                self.first_mismatch_step = Some(self.step);
                self.first_mismatch = Some(format!("{invariant} failed"));
            }
        }
    }
}

fn incarnation(seed: u64) -> [u8; 16] {
    let digest = Sha256::digest(seed.to_be_bytes());
    let mut incarnation = [0_u8; 16];
    incarnation.copy_from_slice(&digest[..16]);
    incarnation
}

fn digest_hex(digest: Sha256) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

/// Execute the routine voter-reconfiguration contract.
#[must_use]
pub fn run_routine_reconfiguration_contract(
    seed: u64,
    mode: RoutineReconfigurationMode,
) -> RoutineReconfigurationReport {
    Scenario::new(seed, mode).run()
}

#[cfg(test)]
mod tests {
    use super::{run_routine_reconfiguration_contract, RoutineReconfigurationMode};

    #[test]
    fn correct_contract_is_exactly_replayable() {
        let first = run_routine_reconfiguration_contract(1103, RoutineReconfigurationMode::Correct);
        let second =
            run_routine_reconfiguration_contract(1103, RoutineReconfigurationMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert_eq!(first.generation, 7);
        assert_eq!(first.membership_epoch, 5);
        assert_eq!(first.active_voters, vec![2, 3, 4]);
    }

    #[test]
    fn every_negative_control_has_a_bounded_failure() {
        for mode in [
            RoutineReconfigurationMode::ReuseNodeIdentity,
            RoutineReconfigurationMode::AdmitLearnerWithoutAuthority,
            RoutineReconfigurationMode::PromoteBeforeCatchup,
            RoutineReconfigurationMode::AcceptStaleMembershipEpoch,
            RoutineReconfigurationMode::AcceptConcurrentReconfiguration,
            RoutineReconfigurationMode::DoubleApplyFinalizeRetry,
            RoutineReconfigurationMode::AcceptRemovedVoterCommit,
            RoutineReconfigurationMode::RepairWithoutDataQuorum,
        ] {
            let report = run_routine_reconfiguration_contract(1103, mode);
            assert!(report.anomaly_count > 0, "{} escaped", mode.id());
            assert!(report
                .first_mismatch_step
                .is_some_and(|step| step <= report.executed_checks));
        }
    }
}
