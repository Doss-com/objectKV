use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

/// Deliberately incorrect behavior used to prove one publication or GC rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationGcMode {
    /// The intended publication and reachability contract.
    Correct,
    /// Install a reader-visible root before its immutable closure exists.
    PublishPointerBeforeBlocks,
    /// Upload an object without first registering an in-flight root.
    OmitPublicationIntent,
    /// Let a drifted refcount authorize deletion.
    TrustAccountingCounter,
    /// Let stale object listing define the reachable set.
    TrustListForLiveness,
    /// Sweep the complement of a partial reachability walk.
    ContinueIncompleteMark,
    /// Delete a candidate without revalidating roots changed after mark.
    DeleteWithoutRevalidation,
}

impl PublicationGcMode {
    /// Stable configuration identifier used by eval suites and artifact refs.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishPointerBeforeBlocks => "publish_pointer_before_blocks",
            Self::OmitPublicationIntent => "omit_publication_intent",
            Self::TrustAccountingCounter => "trust_accounting_counter",
            Self::TrustListForLiveness => "trust_list_for_liveness",
            Self::ContinueIncompleteMark => "continue_incomplete_mark",
            Self::DeleteWithoutRevalidation => "delete_without_revalidation",
        }
    }
}

/// Result of the deterministic publication and reachability scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationGcReport {
    pub seed: u64,
    pub mode: PublicationGcMode,
    pub executed_steps: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub exact_checks: u64,
    pub publication_intents: u64,
    pub published_roots: u64,
    pub verified_unknown_outcomes: u64,
    pub complete_marks: u64,
    pub incomplete_marks: u64,
    pub drifted_counters: u64,
    pub stale_list_observations: u64,
    pub deferred_deletes: u64,
    pub reclaimed_objects: u64,
    pub object_requests: u64,
    pub object_bytes_written: u64,
    pub physical_bytes: u64,
    pub live_bytes: u64,
    pub trace_sha256: String,
}

#[derive(Clone, Debug)]
struct StoredObject {
    bytes: u64,
    verified: bool,
    children: BTreeSet<String>,
}

#[derive(Default)]
struct ObjectModel {
    objects: BTreeMap<String, StoredObject>,
    requests: u64,
    bytes_written: u64,
}

impl ObjectModel {
    fn put(&mut self, id: &str, bytes: u64, children: &[&str]) {
        self.requests = self.requests.saturating_add(1);
        self.bytes_written = self.bytes_written.saturating_add(bytes);
        self.objects.insert(
            id.to_owned(),
            StoredObject {
                bytes,
                verified: true,
                children: children.iter().map(|child| (*child).to_owned()).collect(),
            },
        );
    }

    fn verify_named(&mut self, id: &str) -> bool {
        self.requests = self.requests.saturating_add(1);
        self.objects.get(id).is_some_and(|object| object.verified)
    }

    fn delete(&mut self, id: &str) {
        self.requests = self.requests.saturating_add(1);
        self.objects.remove(id);
    }

    fn physical_bytes(&self) -> u64 {
        self.objects
            .values()
            .map(|object| object.bytes)
            .fold(0_u64, u64::saturating_add)
    }
}

#[derive(Default)]
struct AuthorityModel {
    roots: BTreeMap<String, String>,
    intents: BTreeMap<String, BTreeSet<String>>,
    epoch: u64,
}

impl AuthorityModel {
    fn prepare(&mut self, publication: &str, objects: &[&str]) {
        self.intents.insert(
            publication.to_owned(),
            objects.iter().map(|object| (*object).to_owned()).collect(),
        );
        self.epoch = self.epoch.saturating_add(1);
    }

    fn publish(&mut self, publication: &str, root: &str, manifest: &str) {
        self.roots.insert(root.to_owned(), manifest.to_owned());
        self.intents.remove(publication);
        self.epoch = self.epoch.saturating_add(1);
    }
}

struct Mark {
    authority_epoch: u64,
    reachable: BTreeSet<String>,
    complete: bool,
}

struct Scenario {
    seed: u64,
    mode: PublicationGcMode,
    authority: AuthorityModel,
    store: ObjectModel,
    trace: Sha256,
    step: u64,
    anomaly_count: u64,
    first_mismatch_step: Option<u64>,
    first_mismatch: Option<String>,
    exact_checks: u64,
    publication_intents: u64,
    published_roots: u64,
    verified_unknown_outcomes: u64,
    complete_marks: u64,
    incomplete_marks: u64,
    drifted_counters: u64,
    stale_list_observations: u64,
    deferred_deletes: u64,
    reclaimed_objects: u64,
}

impl Scenario {
    fn new(seed: u64, mode: PublicationGcMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"object-publication-gc-v1");
        trace.update(seed.to_be_bytes());
        trace.update(mode.id().as_bytes());
        Self {
            seed,
            mode,
            authority: AuthorityModel::default(),
            store: ObjectModel::default(),
            trace,
            step: 0,
            anomaly_count: 0,
            first_mismatch_step: None,
            first_mismatch: None,
            exact_checks: 0,
            publication_intents: 0,
            published_roots: 0,
            verified_unknown_outcomes: 0,
            complete_marks: 0,
            incomplete_marks: 0,
            drifted_counters: 0,
            stale_list_observations: 0,
            deferred_deletes: 0,
            reclaimed_objects: 0,
        }
    }

    fn run(mut self) -> PublicationGcReport {
        self.primary_publication();
        self.inflight_publication_race();
        self.counter_drift();
        self.stale_listing();
        self.incomplete_walk();
        self.delete_revalidation();
        self.final_complete_collection();

        let live_bytes = self.reachable_bytes().unwrap_or_default();
        PublicationGcReport {
            seed: self.seed,
            mode: self.mode,
            executed_steps: self.step,
            anomaly_count: self.anomaly_count,
            first_mismatch_step: self.first_mismatch_step,
            first_mismatch: self.first_mismatch,
            exact_checks: self.exact_checks,
            publication_intents: self.publication_intents,
            published_roots: self.published_roots,
            verified_unknown_outcomes: self.verified_unknown_outcomes,
            complete_marks: self.complete_marks,
            incomplete_marks: self.incomplete_marks,
            drifted_counters: self.drifted_counters,
            stale_list_observations: self.stale_list_observations,
            deferred_deletes: self.deferred_deletes,
            reclaimed_objects: self.reclaimed_objects,
            object_requests: self.store.requests,
            object_bytes_written: self.store.bytes_written,
            physical_bytes: self.store.physical_bytes(),
            live_bytes,
            trace_sha256: digest_hex(self.trace),
        }
    }

    fn primary_publication(&mut self) {
        self.prepare("p1", &["block-a", "block-b", "manifest-1"]);
        if self.mode == PublicationGcMode::PublishPointerBeforeBlocks {
            self.publish("p1", "orders", "manifest-1");
            self.check("blocks_precede_visibility", self.root_is_readable("orders"));
        }

        self.store.put("block-a", 1024, &[]);
        if self.store.verify_named("block-a") {
            self.verified_unknown_outcomes = self.verified_unknown_outcomes.saturating_add(1);
        }
        self.store.put("block-b", 1024, &[]);
        self.store.put("manifest-1", 256, &["block-a", "block-b"]);
        if self.mode != PublicationGcMode::PublishPointerBeforeBlocks {
            self.publish("p1", "orders", "manifest-1");
            self.check("blocks_precede_visibility", self.root_is_readable("orders"));
        }
    }

    fn inflight_publication_race(&mut self) {
        if self.mode != PublicationGcMode::OmitPublicationIntent {
            self.prepare("p2", &["block-c", "manifest-2"]);
        }
        self.store.put("block-c", 768, &[]);
        self.store.put("manifest-2", 192, &["block-c"]);
        self.store.put("orphan-o", 640, &[]);
        self.store.put("orphan-z", 512, &[]);

        let mark = self.mark(None);
        let candidates = self.candidates(&mark);
        if self.mode == PublicationGcMode::OmitPublicationIntent {
            for candidate in candidates {
                self.store.delete(&candidate);
            }
        }
        self.publish("p2", "inventory", "manifest-2");
        self.check(
            "inflight_publication_is_rooted",
            self.root_is_readable("inventory"),
        );
    }

    fn counter_drift(&mut self) {
        self.drifted_counters = self.drifted_counters.saturating_add(1);
        if self.mode == PublicationGcMode::TrustAccountingCounter {
            self.store.delete("block-a");
        }
        self.check(
            "accounting_counters_are_non_authoritative",
            self.root_is_readable("orders"),
        );
    }

    fn stale_listing(&mut self) {
        self.stale_list_observations = self.stale_list_observations.saturating_add(1);
        if self.mode == PublicationGcMode::TrustListForLiveness {
            self.store.delete("block-b");
        }
        self.check("list_is_non_authoritative", self.root_is_readable("orders"));
    }

    fn incomplete_walk(&mut self) {
        let mark = self.mark(Some("orders"));
        if self.mode == PublicationGcMode::ContinueIncompleteMark {
            for candidate in self.candidates(&mark) {
                self.store.delete(&candidate);
            }
        }
        let fail_closed = !mark.complete && self.all_roots_are_readable();
        self.check("gc_walk_is_complete_or_fails_closed", fail_closed);
    }

    fn delete_revalidation(&mut self) {
        if !self.store.objects.contains_key("orphan-o") {
            self.store.put("orphan-o", 640, &[]);
        }
        if !self.store.objects.contains_key("orphan-z") {
            self.store.put("orphan-z", 512, &[]);
        }
        let mark = self.mark(None);
        let candidates = self.candidates(&mark);

        self.prepare("snapshot-p", &["orphan-o", "snapshot-manifest"]);
        self.store.put("snapshot-manifest", 128, &["orphan-o"]);
        self.publish("snapshot-p", "snapshot", "snapshot-manifest");

        if self.mode == PublicationGcMode::DeleteWithoutRevalidation {
            for candidate in candidates {
                self.store.delete(&candidate);
            }
        } else if mark.authority_epoch != self.authority.epoch {
            self.deferred_deletes = self
                .deferred_deletes
                .saturating_add(count(candidates.len()));
        }
        self.check(
            "delete_plan_is_revalidated",
            self.root_is_readable("snapshot"),
        );
    }

    fn final_complete_collection(&mut self) {
        let mark = self.mark(None);
        if mark.complete {
            for candidate in self.candidates(&mark) {
                self.store.delete(&candidate);
                self.reclaimed_objects = self.reclaimed_objects.saturating_add(1);
            }
        }
        self.check(
            "complete_mark_reclaims_only_unreachable_objects",
            self.all_roots_are_readable() && self.reclaimed_objects > 0,
        );
    }

    fn prepare(&mut self, publication: &str, objects: &[&str]) {
        self.authority.prepare(publication, objects);
        self.publication_intents = self.publication_intents.saturating_add(1);
        self.trace.update(b"prepare");
        self.trace.update(publication.as_bytes());
    }

    fn publish(&mut self, publication: &str, root: &str, manifest: &str) {
        self.authority.publish(publication, root, manifest);
        self.published_roots = self.published_roots.saturating_add(1);
        self.trace.update(b"publish");
        self.trace.update(root.as_bytes());
        self.trace.update(manifest.as_bytes());
    }

    fn mark(&mut self, fail_root: Option<&str>) -> Mark {
        let mut reachable = BTreeSet::new();
        let mut complete = true;
        for (root, manifest) in &self.authority.roots {
            if fail_root == Some(root.as_str()) {
                complete = false;
                continue;
            }
            if collect_closure(&self.store, manifest, &mut reachable).is_err() {
                complete = false;
            }
        }
        for objects in self.authority.intents.values() {
            for object in objects {
                reachable.insert(object.clone());
                if self.store.objects.contains_key(object) {
                    let _ = collect_closure(&self.store, object, &mut reachable);
                }
            }
        }
        if complete {
            self.complete_marks = self.complete_marks.saturating_add(1);
        } else {
            self.incomplete_marks = self.incomplete_marks.saturating_add(1);
        }
        self.trace.update(if complete {
            b"complete-mark".as_slice()
        } else {
            b"incomplete-mark".as_slice()
        });
        Mark {
            authority_epoch: self.authority.epoch,
            reachable,
            complete,
        }
    }

    fn candidates(&self, mark: &Mark) -> Vec<String> {
        self.store
            .objects
            .keys()
            .filter(|object| !mark.reachable.contains(*object))
            .cloned()
            .collect()
    }

    fn root_is_readable(&self, root: &str) -> bool {
        let Some(manifest) = self.authority.roots.get(root) else {
            return false;
        };
        collect_closure(&self.store, manifest, &mut BTreeSet::new()).is_ok()
    }

    fn all_roots_are_readable(&self) -> bool {
        self.authority
            .roots
            .keys()
            .all(|root| self.root_is_readable(root))
    }

    fn reachable_bytes(&self) -> Option<u64> {
        let mut reachable = BTreeSet::new();
        for manifest in self.authority.roots.values() {
            collect_closure(&self.store, manifest, &mut reachable).ok()?;
        }
        Some(
            reachable
                .iter()
                .filter_map(|object| self.store.objects.get(object))
                .map(|object| object.bytes)
                .fold(0_u64, u64::saturating_add),
        )
    }

    fn check(&mut self, invariant: &str, passed: bool) {
        self.step = self.step.saturating_add(1);
        self.exact_checks = self.exact_checks.saturating_add(1);
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

fn collect_closure(
    store: &ObjectModel,
    object: &str,
    reachable: &mut BTreeSet<String>,
) -> Result<(), ()> {
    if !reachable.insert(object.to_owned()) {
        return Ok(());
    }
    let stored = store.objects.get(object).ok_or(())?;
    if !stored.verified {
        return Err(());
    }
    for child in &stored.children {
        collect_closure(store, child, reachable)?;
    }
    Ok(())
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn digest_hex(digest: Sha256) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

/// Execute the frozen publication and reachability contract.
#[must_use]
pub fn run_publication_gc_contract(seed: u64, mode: PublicationGcMode) -> PublicationGcReport {
    Scenario::new(seed, mode).run()
}

#[cfg(test)]
mod tests {
    use super::{run_publication_gc_contract, PublicationGcMode};

    #[test]
    fn correct_contract_is_exactly_replayable() {
        let first = run_publication_gc_contract(1103, PublicationGcMode::Correct);
        let second = run_publication_gc_contract(1103, PublicationGcMode::Correct);
        assert_eq!(first, second);
        assert_eq!(first.anomaly_count, 0);
        assert!(first.reclaimed_objects > 0);
    }

    #[test]
    fn every_negative_control_has_a_bounded_failure() {
        for mode in [
            PublicationGcMode::PublishPointerBeforeBlocks,
            PublicationGcMode::OmitPublicationIntent,
            PublicationGcMode::TrustAccountingCounter,
            PublicationGcMode::TrustListForLiveness,
            PublicationGcMode::ContinueIncompleteMark,
            PublicationGcMode::DeleteWithoutRevalidation,
        ] {
            let report = run_publication_gc_contract(1103, mode);
            assert!(report.anomaly_count > 0, "{} escaped", mode.id());
            assert!(report.first_mismatch_step.is_some_and(|step| step <= 7));
        }
    }
}
