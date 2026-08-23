//! Pure publication-authority state and capability contract for objectKV.
//!
//! This crate contains no transport or storage implementation. Replicated and
//! simulated authorities apply the same deterministic transitions.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Current unpublished publication state format.
pub const PUBLICATION_FORMAT_VERSION: u32 = 1;

/// Exact committed authority-log position that ordered one transition.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityPosition {
    pub term: u64,
    pub index: u64,
}

impl AuthorityPosition {
    /// Whether this is an admissible committed position.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.index != 0
    }
}

/// Active generation and committed position supplied by the owning authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityContext {
    pub generation: u64,
    pub position: AuthorityPosition,
}

impl AuthorityContext {
    /// Whether this context can issue an accepted transition.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.generation != 0 && self.position.is_valid()
    }
}

/// Backend revision token observed for one exact immutable object.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RevisionToken {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

/// Exact immutable identity used to resolve or guard deletion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectIdentity {
    pub revision: RevisionToken,
    pub length: u64,
    pub sha256: String,
}

impl ObjectIdentity {
    fn is_valid(&self) -> bool {
        valid_sha256(&self.sha256)
    }
}

/// Physical role of one object named by a publication manifest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Data,
    Manifest,
}

/// Exact named object reference stored in publication state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectReference {
    pub kind: ObjectKind,
    pub key: String,
    pub length: u64,
    pub sha256: String,
}

impl ObjectReference {
    fn is_valid(&self) -> bool {
        !self.key.is_empty() && valid_sha256(&self.sha256)
    }
}

/// Complete immutable closure prepared before any object upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationIntent {
    pub object_keys: BTreeSet<String>,
    pub manifest: ObjectReference,
    pub destination_root: String,
    pub expected_prior_root: Option<ObjectReference>,
}

impl PublicationIntent {
    fn is_valid(&self) -> bool {
        self.manifest.kind == ObjectKind::Manifest
            && self.manifest.is_valid()
            && !self.destination_root.is_empty()
            && self
                .expected_prior_root
                .as_ref()
                .is_none_or(ObjectReference::is_valid)
            && self.object_keys.contains(&self.manifest.key)
            && self.object_keys.iter().all(|key| !key.is_empty())
    }
}

/// Generation-bound intent retained until compare-and-publish succeeds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreparedPublication {
    pub owner_generation: u64,
    pub intent: PublicationIntent,
}

/// Opaque capability created only by an accepted replicated reservation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletePermit {
    key: String,
    identity: ObjectIdentity,
    plan_id: String,
    owner_generation: u64,
    authority_position: AuthorityPosition,
}

impl DeletePermit {
    fn new(
        key: String,
        identity: ObjectIdentity,
        plan_id: String,
        context: AuthorityContext,
    ) -> Self {
        Self {
            key,
            identity,
            plan_id,
            owner_generation: context.generation,
            authority_position: context.position,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub const fn identity(&self) -> &ObjectIdentity {
        &self.identity
    }

    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    #[must_use]
    pub const fn owner_generation(&self) -> u64 {
        self.owner_generation
    }

    #[must_use]
    pub const fn authority_position(&self) -> AuthorityPosition {
        self.authority_position
    }
}

/// Replicated root, intent, pin, and deletion-reservation domain.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationAuthorityState {
    pub format_version: u32,
    pub revision: AuthorityPosition,
    pub root_intent_epoch: u64,
    pub intents: BTreeMap<String, PreparedPublication>,
    pub roots: BTreeMap<String, ObjectReference>,
    pub pins: BTreeMap<String, ObjectReference>,
    pub deletion_reservations: BTreeMap<String, DeletePermit>,
}

impl Default for PublicationAuthorityState {
    fn default() -> Self {
        Self {
            format_version: PUBLICATION_FORMAT_VERSION,
            revision: AuthorityPosition::default(),
            root_intent_epoch: 0,
            intents: BTreeMap::new(),
            roots: BTreeMap::new(),
            pins: BTreeMap::new(),
            deletion_reservations: BTreeMap::new(),
        }
    }
}

/// One state transition ordered by the cell authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicationAction {
    Prepare {
        publication_id: String,
        intent: PublicationIntent,
    },
    Publish {
        publication_id: String,
        destination_root: String,
        expected_prior_root: Option<ObjectReference>,
        manifest: ObjectReference,
    },
    Pin {
        pin_id: String,
        expected: Option<ObjectReference>,
        manifest: ObjectReference,
    },
    Unpin {
        pin_id: String,
        expected: ObjectReference,
    },
    ReserveDelete {
        plan_id: String,
        mark_epoch: u64,
        key: String,
        identity: ObjectIdentity,
    },
    RetireDelete {
        permit: DeletePermit,
    },
}

/// Stable semantic status retained for an exact request identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationCommandStatus {
    Accepted,
    GenerationFenced,
    InvalidRequest,
    PublicationExists,
    PublicationIntentMissing,
    PublicationIntentMismatch,
    CrossGenerationIntent,
    RootCompareFailed,
    PinCompareFailed,
    RootIntentEpochChanged,
    ObjectDeletionReserved,
    ObjectNamedByIntent,
    DeleteReservationExists,
    DeleteReservationMissing,
    CrossGenerationDeletePermit,
    DeletePlanMismatch,
}

/// Optional capability returned by an accepted transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicationOutcome {
    Applied,
    DeleteReserved { permit: DeletePermit },
}

/// Deterministic result of one pure state transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationTransition {
    pub status: PublicationCommandStatus,
    pub outcome: Option<PublicationOutcome>,
}

impl PublicationTransition {
    fn accepted(outcome: PublicationOutcome) -> Self {
        Self {
            status: PublicationCommandStatus::Accepted,
            outcome: Some(outcome),
        }
    }

    fn rejected(status: PublicationCommandStatus) -> Self {
        Self {
            status,
            outcome: None,
        }
    }
}

/// Bounded unsafe transition behavior used only by negative-control authorities.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationAuthorityFaults {
    pub publish_without_intent: bool,
    pub ignore_root_epoch: bool,
    pub ignore_delete_reservation: bool,
    pub ignore_root_compare: bool,
    pub allow_cross_generation_intent: bool,
    pub retire_by_plan_key_only: bool,
}

impl PublicationAuthorityState {
    /// Apply one deterministic publication action at an authority-owned context.
    pub fn apply(
        &mut self,
        action: &PublicationAction,
        context: AuthorityContext,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if self.format_version != PUBLICATION_FORMAT_VERSION || !context.is_valid() {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }

        let transition = match action {
            PublicationAction::Prepare {
                publication_id,
                intent,
            } => self.prepare(publication_id, intent, context.generation, faults),
            PublicationAction::Publish {
                publication_id,
                destination_root,
                expected_prior_root,
                manifest,
            } => self.publish(
                publication_id,
                destination_root,
                expected_prior_root.as_ref(),
                manifest,
                context.generation,
                faults,
            ),
            PublicationAction::Pin {
                pin_id,
                expected,
                manifest,
            } => self.pin(pin_id, expected.as_ref(), manifest),
            PublicationAction::Unpin { pin_id, expected } => self.unpin(pin_id, expected),
            PublicationAction::ReserveDelete {
                plan_id,
                mark_epoch,
                key,
                identity,
            } => self.reserve_delete(plan_id, *mark_epoch, key, identity, context, faults),
            PublicationAction::RetireDelete { permit } => {
                self.retire_delete(permit, context.generation, faults)
            }
        };
        if transition.status == PublicationCommandStatus::Accepted {
            self.revision = context.position;
        }
        transition
    }

    fn prepare(
        &mut self,
        publication_id: &str,
        intent: &PublicationIntent,
        owner_generation: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if publication_id.is_empty() || !intent.is_valid() {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.intents.contains_key(publication_id) {
            return PublicationTransition::rejected(PublicationCommandStatus::PublicationExists);
        }
        if !faults.ignore_delete_reservation
            && intent
                .object_keys
                .iter()
                .any(|key| self.deletion_reservations.contains_key(key))
        {
            return PublicationTransition::rejected(
                PublicationCommandStatus::ObjectDeletionReserved,
            );
        }
        self.intents.insert(
            publication_id.to_owned(),
            PreparedPublication {
                owner_generation,
                intent: intent.clone(),
            },
        );
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn publish(
        &mut self,
        publication_id: &str,
        destination_root: &str,
        expected_prior_root: Option<&ObjectReference>,
        manifest: &ObjectReference,
        generation: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if publication_id.is_empty()
            || destination_root.is_empty()
            || manifest.kind != ObjectKind::Manifest
            || !manifest.is_valid()
        {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        match self.intents.get(publication_id) {
            Some(prepared)
                if prepared.intent.manifest == *manifest
                    && prepared.intent.destination_root == destination_root
                    && prepared.intent.expected_prior_root.as_ref() == expected_prior_root
                    && prepared.intent.object_keys.contains(&manifest.key) =>
            {
                if prepared.owner_generation != generation && !faults.allow_cross_generation_intent
                {
                    return PublicationTransition::rejected(
                        PublicationCommandStatus::CrossGenerationIntent,
                    );
                }
            }
            Some(_) => {
                return PublicationTransition::rejected(
                    PublicationCommandStatus::PublicationIntentMismatch,
                );
            }
            None if faults.publish_without_intent => {}
            None => {
                return PublicationTransition::rejected(
                    PublicationCommandStatus::PublicationIntentMissing,
                );
            }
        }
        if !faults.ignore_root_compare && self.roots.get(destination_root) != expected_prior_root {
            return PublicationTransition::rejected(PublicationCommandStatus::RootCompareFailed);
        }
        self.roots
            .insert(destination_root.to_owned(), manifest.clone());
        self.intents.remove(publication_id);
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn pin(
        &mut self,
        pin_id: &str,
        expected: Option<&ObjectReference>,
        manifest: &ObjectReference,
    ) -> PublicationTransition {
        if pin_id.is_empty() || manifest.kind != ObjectKind::Manifest || !manifest.is_valid() {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.pins.get(pin_id) != expected {
            return PublicationTransition::rejected(PublicationCommandStatus::PinCompareFailed);
        }
        self.pins.insert(pin_id.to_owned(), manifest.clone());
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn unpin(&mut self, pin_id: &str, expected: &ObjectReference) -> PublicationTransition {
        if pin_id.is_empty() || !expected.is_valid() {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.pins.get(pin_id) != Some(expected) {
            return PublicationTransition::rejected(PublicationCommandStatus::PinCompareFailed);
        }
        self.pins.remove(pin_id);
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_delete(
        &mut self,
        plan_id: &str,
        mark_epoch: u64,
        key: &str,
        identity: &ObjectIdentity,
        context: AuthorityContext,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if plan_id.is_empty() || key.is_empty() || !identity.is_valid() {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if !faults.ignore_root_epoch && mark_epoch != self.root_intent_epoch {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RootIntentEpochChanged,
            );
        }
        if self
            .intents
            .values()
            .any(|prepared| prepared.intent.object_keys.contains(key))
        {
            return PublicationTransition::rejected(PublicationCommandStatus::ObjectNamedByIntent);
        }
        if self.deletion_reservations.contains_key(key) {
            return PublicationTransition::rejected(
                PublicationCommandStatus::DeleteReservationExists,
            );
        }
        let permit = DeletePermit::new(
            key.to_owned(),
            identity.clone(),
            plan_id.to_owned(),
            context,
        );
        self.deletion_reservations
            .insert(key.to_owned(), permit.clone());
        PublicationTransition::accepted(PublicationOutcome::DeleteReserved { permit })
    }

    fn retire_delete(
        &mut self,
        permit: &DeletePermit,
        generation: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        let Some(reservation) = self.deletion_reservations.get(permit.key()) else {
            return PublicationTransition::rejected(
                PublicationCommandStatus::DeleteReservationMissing,
            );
        };
        if permit.owner_generation() != generation {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CrossGenerationDeletePermit,
            );
        }
        let exact = reservation == permit;
        let weak_match = faults.retire_by_plan_key_only
            && reservation.plan_id() == permit.plan_id()
            && reservation.key() == permit.key();
        if !exact && !weak_match {
            return PublicationTransition::rejected(PublicationCommandStatus::DeletePlanMismatch);
        }
        self.deletion_reservations.remove(permit.key());
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(index: u64) -> AuthorityContext {
        generation_context(7, index)
    }

    fn generation_context(generation: u64, index: u64) -> AuthorityContext {
        AuthorityContext {
            generation,
            position: AuthorityPosition { term: 3, index },
        }
    }

    fn reference(key: &str, kind: ObjectKind) -> ObjectReference {
        ObjectReference {
            kind,
            key: key.to_owned(),
            length: 10,
            sha256: "a".repeat(64),
        }
    }

    fn intent(
        manifest: &ObjectReference,
        child: &str,
        destination_root: &str,
        expected_prior_root: Option<ObjectReference>,
    ) -> PublicationIntent {
        PublicationIntent {
            object_keys: BTreeSet::from([manifest.key.clone(), child.to_owned()]),
            manifest: manifest.clone(),
            destination_root: destination_root.to_owned(),
            expected_prior_root,
        }
    }

    fn publish_action(intent: &PublicationIntent, publication_id: &str) -> PublicationAction {
        PublicationAction::Publish {
            publication_id: publication_id.to_owned(),
            destination_root: intent.destination_root.clone(),
            expected_prior_root: intent.expected_prior_root.clone(),
            manifest: intent.manifest.clone(),
        }
    }

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            revision: RevisionToken {
                e_tag: Some("etag-1".to_owned()),
                version: None,
            },
            length: 10,
            sha256: "b".repeat(64),
        }
    }

    #[test]
    fn publication_requires_intent_and_retires_it_atomically() {
        let manifest = reference("objects/manifest", ObjectKind::Manifest);
        let publication = intent(&manifest, "objects/data", "r1", None);
        let mut state = PublicationAuthorityState::default();
        let missing = state.apply(
            &publish_action(&publication, "p1"),
            context(1),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::PublicationIntentMissing,
            missing.status
        );
        assert_eq!(AuthorityPosition::default(), state.revision);

        let prepared = state.apply(
            &PublicationAction::Prepare {
                publication_id: "p1".to_owned(),
                intent: publication.clone(),
            },
            context(2),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::Accepted, prepared.status);

        let published = state.apply(
            &publish_action(&publication, "p1"),
            context(3),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::Accepted, published.status);
        assert_eq!(Some(&manifest), state.roots.get("r1"));
        assert!(!state.intents.contains_key("p1"));
        assert_eq!(2, state.root_intent_epoch);
        assert_eq!(context(3).position, state.revision);
    }

    #[test]
    fn reservation_fences_epoch_and_intersecting_prepare() {
        let manifest = reference("objects/race-manifest", ObjectKind::Manifest);
        let mut state = PublicationAuthorityState::default();
        let stale_epoch = state.root_intent_epoch;
        let pin = state.apply(
            &PublicationAction::Pin {
                pin_id: "pin-1".to_owned(),
                expected: None,
                manifest: manifest.clone(),
            },
            context(1),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::Accepted, pin.status);
        let stale_reservation = state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "plan-1".to_owned(),
                mark_epoch: stale_epoch,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(2),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::RootIntentEpochChanged,
            stale_reservation.status
        );

        let reserved = state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "plan-1".to_owned(),
                mark_epoch: state.root_intent_epoch,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(3),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::DeleteReserved { permit }) = reserved.outcome else {
            panic!("reservation did not return a permit");
        };
        assert_eq!(7, permit.owner_generation());
        assert_eq!(context(3).position, permit.authority_position());

        let blocked = state.apply(
            &PublicationAction::Prepare {
                publication_id: "p-race".to_owned(),
                intent: intent(&manifest, "objects/data", "r-race", None),
            },
            context(4),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::ObjectDeletionReserved,
            blocked.status
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn root_pin_generation_and_retirement_require_exact_compare_values() {
        let manifest = reference("objects/manifest", ObjectKind::Manifest);
        let replacement = reference("objects/replacement", ObjectKind::Manifest);
        let publication = intent(&manifest, "objects/data", "r1", None);
        let mut root_state = PublicationAuthorityState::default();
        assert_eq!(
            PublicationCommandStatus::Accepted,
            root_state
                .apply(
                    &PublicationAction::Prepare {
                        publication_id: "p1".to_owned(),
                        intent: publication.clone(),
                    },
                    context(1),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        root_state
            .roots
            .insert("r1".to_owned(), replacement.clone());
        assert_eq!(
            PublicationCommandStatus::RootCompareFailed,
            root_state
                .apply(
                    &publish_action(&publication, "p1"),
                    context(2),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );

        let mut generation_state = PublicationAuthorityState::default();
        assert_eq!(
            PublicationCommandStatus::Accepted,
            generation_state
                .apply(
                    &PublicationAction::Prepare {
                        publication_id: "p2".to_owned(),
                        intent: publication.clone(),
                    },
                    context(3),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(
            PublicationCommandStatus::CrossGenerationIntent,
            generation_state
                .apply(
                    &publish_action(&publication, "p2"),
                    generation_context(8, 4),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );

        let mut pin_state = PublicationAuthorityState::default();
        assert_eq!(
            PublicationCommandStatus::Accepted,
            pin_state
                .apply(
                    &PublicationAction::Pin {
                        pin_id: "pin".to_owned(),
                        expected: None,
                        manifest: manifest.clone(),
                    },
                    context(5),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(
            PublicationCommandStatus::PinCompareFailed,
            pin_state
                .apply(
                    &PublicationAction::Unpin {
                        pin_id: "pin".to_owned(),
                        expected: replacement,
                    },
                    context(6),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );

        let mut delete_state = PublicationAuthorityState::default();
        let reserve = delete_state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "plan".to_owned(),
                mark_epoch: 0,
                key: "objects/delete".to_owned(),
                identity: identity(),
            },
            context(7),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::DeleteReserved { permit }) = reserve.outcome else {
            panic!("reservation did not return permit");
        };
        let mut forged = permit.clone();
        forged.authority_position.index = forged.authority_position.index.saturating_sub(1);
        assert_eq!(
            PublicationCommandStatus::DeletePlanMismatch,
            delete_state
                .apply(
                    &PublicationAction::RetireDelete { permit: forged },
                    context(8),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(
            PublicationCommandStatus::CrossGenerationDeletePermit,
            delete_state
                .apply(
                    &PublicationAction::RetireDelete {
                        permit: permit.clone(),
                    },
                    generation_context(8, 9),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(
            PublicationCommandStatus::Accepted,
            delete_state
                .apply(
                    &PublicationAction::RetireDelete { permit },
                    context(10),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
    }

    #[test]
    fn negative_faults_expose_each_frozen_violation() {
        let manifest = reference("objects/manifest", ObjectKind::Manifest);
        let publication = intent(&manifest, "objects/data", "r1", None);
        let mut publish_state = PublicationAuthorityState::default();
        let published = publish_state.apply(
            &publish_action(&publication, "missing"),
            context(1),
            PublicationAuthorityFaults {
                publish_without_intent: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, published.status);

        let mut epoch_state = PublicationAuthorityState {
            root_intent_epoch: 4,
            ..PublicationAuthorityState::default()
        };
        let reserved = epoch_state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "p".to_owned(),
                mark_epoch: 3,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(2),
            PublicationAuthorityFaults {
                ignore_root_epoch: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, reserved.status);

        let mut reservation_state = PublicationAuthorityState::default();
        let reserve = reservation_state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "p".to_owned(),
                mark_epoch: 0,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(3),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::Accepted, reserve.status);
        let prepared = reservation_state.apply(
            &PublicationAction::Prepare {
                publication_id: "p2".to_owned(),
                intent: publication.clone(),
            },
            context(4),
            PublicationAuthorityFaults {
                ignore_delete_reservation: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, prepared.status);

        let mut generation_state = PublicationAuthorityState::default();
        let prepared = generation_state.apply(
            &PublicationAction::Prepare {
                publication_id: "p3".to_owned(),
                intent: publication.clone(),
            },
            context(5),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::Accepted, prepared.status);
        let cross_generation = generation_state.apply(
            &publish_action(&publication, "p3"),
            generation_context(8, 6),
            PublicationAuthorityFaults {
                allow_cross_generation_intent: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, cross_generation.status);
    }
}
