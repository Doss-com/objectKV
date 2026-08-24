//! Pure publication-authority state and capability contract for objectKV.
//!
//! This crate contains no transport or storage implementation. Replicated and
//! simulated authorities apply the same deterministic transitions.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

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

/// Immutable object closure pinned by one admitted historical read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotClosure {
    pub manifest: ObjectReference,
    pub object_keys: BTreeSet<String>,
}

impl SnapshotClosure {
    fn is_valid(&self) -> bool {
        self.manifest.kind == ObjectKind::Manifest
            && self.manifest.is_valid()
            && self.object_keys.contains(&self.manifest.key)
            && self.object_keys.iter().all(|key| !key.is_empty())
    }

    fn names(&self, key: &str) -> bool {
        self.object_keys.contains(key)
    }
}

/// Durable capability for one admitted snapshot version and pinned closure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotLeaseToken {
    pub lease_id: String,
    pub tenant_id: String,
    pub snapshot_version: u64,
    pub lease_epoch: u64,
    pub owner: String,
    pub purpose: String,
    pub deadline_tick: u64,
    pub closure: SnapshotClosure,
    pub owner_generation: u64,
    pub authority_position: AuthorityPosition,
}

/// Why a previously issued snapshot lease cannot authorize a new reader.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotLeaseValidationError {
    AuthorityFormatInvalid,
    LeaseMissing,
    TokenMismatch,
    LeaseExpired,
}

impl Display for SnapshotLeaseValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AuthorityFormatInvalid => "publication authority format is invalid",
            Self::LeaseMissing => "snapshot lease is not active",
            Self::TokenMismatch => "snapshot lease token differs from authority state",
            Self::LeaseExpired => "snapshot lease deadline has passed",
        })
    }
}

impl Error for SnapshotLeaseValidationError {}

/// Frozen authority capability consumed by one physical collection worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CollectionJobToken {
    pub job_id: String,
    pub owner_generation: u64,
    pub authority_position: AuthorityPosition,
    pub frozen_floor: u64,
    pub input_manifest: ObjectReference,
    pub destination_root: String,
    pub range_map_epoch: u64,
    pub expected_collected_through: u64,
    pub output_namespace: String,
}

/// Exact immutable output returned by a collection worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CollectionReceipt {
    pub token: CollectionJobToken,
    pub output_manifest: ObjectReference,
    pub object_keys: BTreeSet<String>,
}

impl CollectionReceipt {
    fn is_valid(&self) -> bool {
        self.output_manifest.kind == ObjectKind::Manifest
            && self.output_manifest.is_valid()
            && self.object_keys.contains(&self.output_manifest.key)
            && self
                .object_keys
                .iter()
                .all(|key| key.starts_with(&self.token.output_namespace))
    }
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
    #[serde(default, skip_serializing_if = "is_zero")]
    pub observed_commit_frontier: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_window: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retention_policy_epoch: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub policy_floor: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub minimum_readable_version: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub physically_collected_through: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lease_clock_tick: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub lease_epoch: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub leases: BTreeMap<String, SnapshotLeaseToken>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub range_map_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_root: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub collection_jobs: BTreeMap<String, CollectionJobToken>,
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
            observed_commit_frontier: 0,
            retention_window: None,
            retention_policy_epoch: 0,
            policy_floor: 0,
            minimum_readable_version: 0,
            physically_collected_through: 0,
            lease_clock_tick: 0,
            lease_epoch: 0,
            leases: BTreeMap::new(),
            range_map_epoch: 0,
            collection_root: None,
            collection_jobs: BTreeMap::new(),
        }
    }
}

/// One state transition ordered by the cell authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicationAction {
    ObserveCommittedFrontier {
        committed_frontier: u64,
    },
    SetRetentionWindow {
        expected_policy_epoch: u64,
        retention_window: u64,
    },
    ObserveRangeMapEpoch {
        range_map_epoch: u64,
    },
    ConfigureCollectionRoot {
        expected: Option<String>,
        destination_root: String,
    },
    AcquireLease {
        lease_id: String,
        tenant_id: String,
        snapshot_version: u64,
        owner: String,
        purpose: String,
        deadline_tick: u64,
        closure: SnapshotClosure,
    },
    RenewLease {
        lease_id: String,
        expected_lease_epoch: u64,
        new_deadline_tick: u64,
    },
    ReleaseLease {
        lease_id: String,
        expected_lease_epoch: u64,
    },
    AdvanceLeaseClock {
        expected_tick: u64,
        next_tick: u64,
    },
    PrepareCollection {
        job_id: String,
        frozen_floor: u64,
        input_manifest: ObjectReference,
        destination_root: String,
        range_map_epoch: u64,
        expected_collected_through: u64,
        output_namespace: String,
    },
    PublishCollection {
        receipt: CollectionReceipt,
    },
    AbandonCollection {
        token: CollectionJobToken,
    },
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
    CommittedFrontierRetreated,
    RetentionPolicyEpochMismatch,
    RetentionPolicyUnconfigured,
    SnapshotBelowFloor,
    SnapshotAheadOfFrontier,
    LeaseExists,
    LeaseMissing,
    LeaseEpochMismatch,
    LeaseDeadlineInvalid,
    LeaseClockMismatch,
    LeaseClockRetreated,
    RangeMapEpochRetreated,
    RangeMapEpochMismatch,
    CollectionJobExists,
    CollectionJobMissing,
    CollectionJobMismatch,
    CollectionConflict,
    CollectionFloorInvalid,
    CollectionFrontierMismatch,
    CollectionReceiptMismatch,
    CollectionRootCompareFailed,
}

/// Optional capability returned by an accepted transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicationOutcome {
    Applied,
    DeleteReserved { permit: DeletePermit },
    LeaseAcquired { token: SnapshotLeaseToken },
    LeaseRenewed { token: SnapshotLeaseToken },
    LeasesExpired { lease_ids: Vec<String> },
    CollectionPrepared { token: CollectionJobToken },
    CollectionPublished { receipt: CollectionReceipt },
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
    pub accept_backdated_lease: bool,
    pub omit_lease_root_epoch: bool,
    pub ignore_collection_range_epoch: bool,
    pub advance_collection_without_publication: bool,
    pub ignore_collection_input_root: bool,
}

impl PublicationAuthorityState {
    /// Validate that an issued lease is still the exact active capability in
    /// this authority snapshot.
    ///
    /// This is a read-side check. The caller must obtain a current authority
    /// snapshot before opening a new historical reader. A token alone is not a
    /// permanent bearer credential.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal for invalid authority state, a missing lease,
    /// token drift, or an expired deadline.
    pub fn validate_active_snapshot_lease(
        &self,
        token: &SnapshotLeaseToken,
    ) -> Result<(), SnapshotLeaseValidationError> {
        if self.format_version != PUBLICATION_FORMAT_VERSION {
            return Err(SnapshotLeaseValidationError::AuthorityFormatInvalid);
        }
        let Some(active) = self.leases.get(&token.lease_id) else {
            return Err(SnapshotLeaseValidationError::LeaseMissing);
        };
        if active != token {
            return Err(SnapshotLeaseValidationError::TokenMismatch);
        }
        if token.deadline_tick <= self.lease_clock_tick {
            return Err(SnapshotLeaseValidationError::LeaseExpired);
        }
        Ok(())
    }

    /// Apply one deterministic publication action at an authority-owned context.
    #[allow(clippy::too_many_lines)]
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
            PublicationAction::ObserveCommittedFrontier { committed_frontier } => {
                self.observe_committed_frontier(*committed_frontier)
            }
            PublicationAction::SetRetentionWindow {
                expected_policy_epoch,
                retention_window,
            } => self.set_retention_window(*expected_policy_epoch, *retention_window),
            PublicationAction::ObserveRangeMapEpoch { range_map_epoch } => {
                self.observe_range_map_epoch(*range_map_epoch)
            }
            PublicationAction::ConfigureCollectionRoot {
                expected,
                destination_root,
            } => self.configure_collection_root(expected.as_deref(), destination_root),
            PublicationAction::AcquireLease {
                lease_id,
                tenant_id,
                snapshot_version,
                owner,
                purpose,
                deadline_tick,
                closure,
            } => self.acquire_lease(
                lease_id,
                tenant_id,
                *snapshot_version,
                owner,
                purpose,
                *deadline_tick,
                closure,
                context,
                faults,
            ),
            PublicationAction::RenewLease {
                lease_id,
                expected_lease_epoch,
                new_deadline_tick,
            } => self.renew_lease(
                lease_id,
                *expected_lease_epoch,
                *new_deadline_tick,
                context,
                faults,
            ),
            PublicationAction::ReleaseLease {
                lease_id,
                expected_lease_epoch,
            } => self.release_lease(lease_id, *expected_lease_epoch, faults),
            PublicationAction::AdvanceLeaseClock {
                expected_tick,
                next_tick,
            } => self.advance_lease_clock(*expected_tick, *next_tick, faults),
            PublicationAction::PrepareCollection {
                job_id,
                frozen_floor,
                input_manifest,
                destination_root,
                range_map_epoch,
                expected_collected_through,
                output_namespace,
            } => self.prepare_collection(
                job_id,
                *frozen_floor,
                input_manifest,
                destination_root,
                *range_map_epoch,
                *expected_collected_through,
                output_namespace,
                context,
                faults,
            ),
            PublicationAction::PublishCollection { receipt } => {
                self.publish_collection(receipt, context.generation, faults)
            }
            PublicationAction::AbandonCollection { token } => {
                self.abandon_collection(token, context.generation)
            }
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

    fn observe_committed_frontier(&mut self, committed_frontier: u64) -> PublicationTransition {
        if committed_frontier == 0 {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if committed_frontier < self.observed_commit_frontier {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CommittedFrontierRetreated,
            );
        }
        self.observed_commit_frontier = committed_frontier;
        self.recompute_read_floor();
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn set_retention_window(
        &mut self,
        expected_policy_epoch: u64,
        retention_window: u64,
    ) -> PublicationTransition {
        if expected_policy_epoch != self.retention_policy_epoch {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RetentionPolicyEpochMismatch,
            );
        }
        self.retention_window = Some(retention_window);
        self.retention_policy_epoch = self.retention_policy_epoch.saturating_add(1);
        self.recompute_read_floor();
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn observe_range_map_epoch(&mut self, range_map_epoch: u64) -> PublicationTransition {
        if range_map_epoch == 0 {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if range_map_epoch < self.range_map_epoch {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RangeMapEpochRetreated,
            );
        }
        self.range_map_epoch = range_map_epoch;
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn configure_collection_root(
        &mut self,
        expected: Option<&str>,
        destination_root: &str,
    ) -> PublicationTransition {
        if destination_root.is_empty() || !self.roots.contains_key(destination_root) {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.collection_root.as_deref() != expected {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionRootCompareFailed,
            );
        }
        if !self.collection_jobs.is_empty() {
            return PublicationTransition::rejected(PublicationCommandStatus::CollectionConflict);
        }
        self.collection_root = Some(destination_root.to_owned());
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_lease(
        &mut self,
        lease_id: &str,
        tenant_id: &str,
        snapshot_version: u64,
        owner: &str,
        purpose: &str,
        deadline_tick: u64,
        closure: &SnapshotClosure,
        context: AuthorityContext,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if lease_id.is_empty()
            || tenant_id.is_empty()
            || snapshot_version == 0
            || owner.is_empty()
            || purpose.is_empty()
            || !closure.is_valid()
        {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.retention_window.is_none() {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RetentionPolicyUnconfigured,
            );
        }
        if self.leases.contains_key(lease_id) {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseExists);
        }
        if snapshot_version < self.minimum_readable_version && !faults.accept_backdated_lease {
            return PublicationTransition::rejected(PublicationCommandStatus::SnapshotBelowFloor);
        }
        if snapshot_version > self.observed_commit_frontier {
            return PublicationTransition::rejected(
                PublicationCommandStatus::SnapshotAheadOfFrontier,
            );
        }
        if deadline_tick <= self.lease_clock_tick {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseDeadlineInvalid);
        }
        self.lease_epoch = self.lease_epoch.saturating_add(1);
        let token = SnapshotLeaseToken {
            lease_id: lease_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            snapshot_version,
            lease_epoch: self.lease_epoch,
            owner: owner.to_owned(),
            purpose: purpose.to_owned(),
            deadline_tick,
            closure: closure.clone(),
            owner_generation: context.generation,
            authority_position: context.position,
        };
        self.leases.insert(lease_id.to_owned(), token.clone());
        if !faults.omit_lease_root_epoch {
            self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        }
        self.recompute_read_floor();
        PublicationTransition::accepted(PublicationOutcome::LeaseAcquired { token })
    }

    fn renew_lease(
        &mut self,
        lease_id: &str,
        expected_lease_epoch: u64,
        new_deadline_tick: u64,
        context: AuthorityContext,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        let Some(existing) = self.leases.get(lease_id) else {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseMissing);
        };
        if existing.lease_epoch != expected_lease_epoch {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseEpochMismatch);
        }
        if new_deadline_tick <= self.lease_clock_tick || new_deadline_tick <= existing.deadline_tick
        {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseDeadlineInvalid);
        }
        self.lease_epoch = self.lease_epoch.saturating_add(1);
        let mut token = existing.clone();
        token.lease_epoch = self.lease_epoch;
        token.deadline_tick = new_deadline_tick;
        token.owner_generation = context.generation;
        token.authority_position = context.position;
        self.leases.insert(lease_id.to_owned(), token.clone());
        if !faults.omit_lease_root_epoch {
            self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        }
        PublicationTransition::accepted(PublicationOutcome::LeaseRenewed { token })
    }

    fn release_lease(
        &mut self,
        lease_id: &str,
        expected_lease_epoch: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        let Some(existing) = self.leases.get(lease_id) else {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseMissing);
        };
        if existing.lease_epoch != expected_lease_epoch {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseEpochMismatch);
        }
        self.leases.remove(lease_id);
        self.lease_epoch = self.lease_epoch.saturating_add(1);
        if !faults.omit_lease_root_epoch {
            self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        }
        self.recompute_read_floor();
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn advance_lease_clock(
        &mut self,
        expected_tick: u64,
        next_tick: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if expected_tick != self.lease_clock_tick {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseClockMismatch);
        }
        if next_tick <= expected_tick {
            return PublicationTransition::rejected(PublicationCommandStatus::LeaseClockRetreated);
        }
        self.lease_clock_tick = next_tick;
        let expired = self
            .leases
            .iter()
            .filter(|(_, lease)| lease.deadline_tick <= next_tick)
            .map(|(lease_id, _)| lease_id.clone())
            .collect::<Vec<_>>();
        for lease_id in &expired {
            self.leases.remove(lease_id);
        }
        if !expired.is_empty() {
            self.lease_epoch = self.lease_epoch.saturating_add(1);
            if !faults.omit_lease_root_epoch {
                self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
            }
        }
        self.recompute_read_floor();
        PublicationTransition::accepted(PublicationOutcome::LeasesExpired { lease_ids: expired })
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_collection(
        &mut self,
        job_id: &str,
        frozen_floor: u64,
        input_manifest: &ObjectReference,
        destination_root: &str,
        range_map_epoch: u64,
        expected_collected_through: u64,
        output_namespace: &str,
        context: AuthorityContext,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if job_id.is_empty()
            || frozen_floor == 0
            || input_manifest.kind != ObjectKind::Manifest
            || !input_manifest.is_valid()
            || destination_root.is_empty()
            || range_map_epoch == 0
            || output_namespace.is_empty()
            || !output_namespace.ends_with('/')
        {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        if self.collection_jobs.contains_key(job_id) {
            return PublicationTransition::rejected(PublicationCommandStatus::CollectionJobExists);
        }
        if self.collection_jobs.values().any(|job| {
            job.destination_root == destination_root
                || job.output_namespace == output_namespace
                || job.output_namespace.starts_with(output_namespace)
                || output_namespace.starts_with(&job.output_namespace)
        }) {
            return PublicationTransition::rejected(PublicationCommandStatus::CollectionConflict);
        }
        if range_map_epoch != self.range_map_epoch && !faults.ignore_collection_range_epoch {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RangeMapEpochMismatch,
            );
        }
        if self.collection_root.as_deref() != Some(destination_root) {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionRootCompareFailed,
            );
        }
        if expected_collected_through != self.physically_collected_through {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionFrontierMismatch,
            );
        }
        if frozen_floor <= self.physically_collected_through
            || frozen_floor > self.minimum_readable_version
        {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionFloorInvalid,
            );
        }
        if self.roots.get(destination_root) != Some(input_manifest)
            && !faults.ignore_collection_input_root
        {
            return PublicationTransition::rejected(PublicationCommandStatus::RootCompareFailed);
        }
        let token = CollectionJobToken {
            job_id: job_id.to_owned(),
            owner_generation: context.generation,
            authority_position: context.position,
            frozen_floor,
            input_manifest: input_manifest.clone(),
            destination_root: destination_root.to_owned(),
            range_map_epoch,
            expected_collected_through,
            output_namespace: output_namespace.to_owned(),
        };
        self.collection_jobs
            .insert(job_id.to_owned(), token.clone());
        if faults.advance_collection_without_publication {
            self.physically_collected_through = frozen_floor;
        }
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::CollectionPrepared { token })
    }

    fn publish_collection(
        &mut self,
        receipt: &CollectionReceipt,
        generation: u64,
        faults: PublicationAuthorityFaults,
    ) -> PublicationTransition {
        if !receipt.is_valid() || receipt.output_manifest == receipt.token.input_manifest {
            return PublicationTransition::rejected(PublicationCommandStatus::InvalidRequest);
        }
        let Some(prepared) = self.collection_jobs.get(&receipt.token.job_id) else {
            return PublicationTransition::rejected(PublicationCommandStatus::CollectionJobMissing);
        };
        if prepared != &receipt.token {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionReceiptMismatch,
            );
        }
        if prepared.owner_generation != generation {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CrossGenerationIntent,
            );
        }
        if prepared.range_map_epoch != self.range_map_epoch && !faults.ignore_collection_range_epoch
        {
            return PublicationTransition::rejected(
                PublicationCommandStatus::RangeMapEpochMismatch,
            );
        }
        if self.collection_root.as_deref() != Some(prepared.destination_root.as_str()) {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionRootCompareFailed,
            );
        }
        if prepared.expected_collected_through != self.physically_collected_through {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionFrontierMismatch,
            );
        }
        if self.roots.get(&prepared.destination_root) != Some(&prepared.input_manifest)
            && !faults.ignore_collection_input_root
        {
            return PublicationTransition::rejected(PublicationCommandStatus::RootCompareFailed);
        }
        if receipt
            .object_keys
            .iter()
            .any(|key| self.deletion_reservations.contains_key(key))
        {
            return PublicationTransition::rejected(
                PublicationCommandStatus::ObjectDeletionReserved,
            );
        }
        self.roots.insert(
            prepared.destination_root.clone(),
            receipt.output_manifest.clone(),
        );
        self.physically_collected_through = prepared.frozen_floor;
        self.collection_jobs.remove(&prepared.job_id.clone());
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::CollectionPublished {
            receipt: receipt.clone(),
        })
    }

    fn abandon_collection(
        &mut self,
        token: &CollectionJobToken,
        generation: u64,
    ) -> PublicationTransition {
        let Some(prepared) = self.collection_jobs.get(&token.job_id) else {
            return PublicationTransition::rejected(PublicationCommandStatus::CollectionJobMissing);
        };
        if prepared != token {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CollectionJobMismatch,
            );
        }
        if token.owner_generation != generation {
            return PublicationTransition::rejected(
                PublicationCommandStatus::CrossGenerationIntent,
            );
        }
        self.collection_jobs.remove(&token.job_id);
        self.root_intent_epoch = self.root_intent_epoch.saturating_add(1);
        PublicationTransition::accepted(PublicationOutcome::Applied)
    }

    fn recompute_read_floor(&mut self) {
        let Some(retention_window) = self.retention_window else {
            return;
        };
        let proposed_policy_floor = self
            .observed_commit_frontier
            .saturating_sub(retention_window);
        self.policy_floor = self.policy_floor.max(proposed_policy_floor);
        let candidate = self
            .leases
            .values()
            .map(|lease| lease.snapshot_version)
            .min()
            .map_or(self.policy_floor, |oldest_lease| {
                self.policy_floor.min(oldest_lease)
            });
        self.minimum_readable_version = self.minimum_readable_version.max(candidate);
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
        if self.leases.values().any(|lease| lease.closure.names(key))
            || self
                .collection_jobs
                .values()
                .any(|job| job.input_manifest.key == key || key.starts_with(&job.output_namespace))
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

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
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

    fn closure(manifest: &ObjectReference, child: &str) -> SnapshotClosure {
        SnapshotClosure {
            manifest: manifest.clone(),
            object_keys: BTreeSet::from([manifest.key.clone(), child.to_owned()]),
        }
    }

    #[test]
    fn active_snapshot_lease_validation_rejects_drift_release_and_expiry() {
        let manifest = reference("objects/m0", ObjectKind::Manifest);
        let mut state = PublicationAuthorityState::default();
        for (action, index) in [
            (
                PublicationAction::ObserveCommittedFrontier {
                    committed_frontier: 10,
                },
                1,
            ),
            (
                PublicationAction::SetRetentionWindow {
                    expected_policy_epoch: 0,
                    retention_window: 100,
                },
                2,
            ),
        ] {
            assert_eq!(
                PublicationCommandStatus::Accepted,
                state
                    .apply(
                        &action,
                        context(index),
                        PublicationAuthorityFaults::default()
                    )
                    .status
            );
        }
        let acquired = state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "reader-m0".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                snapshot_version: 10,
                owner: "range-engine-1".to_owned(),
                purpose: "historical-read".to_owned(),
                deadline_tick: 20,
                closure: closure(&manifest, "objects/data"),
            },
            context(3),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::LeaseAcquired { token }) = acquired.outcome else {
            panic!("lease acquisition did not return a token");
        };
        assert_eq!(state.validate_active_snapshot_lease(&token), Ok(()));

        let mut drifted = token.clone();
        drifted.owner = "range-engine-2".to_owned();
        assert_eq!(
            state.validate_active_snapshot_lease(&drifted),
            Err(SnapshotLeaseValidationError::TokenMismatch)
        );

        let mut impossible_expired_snapshot = state.clone();
        impossible_expired_snapshot.lease_clock_tick = token.deadline_tick;
        assert_eq!(
            impossible_expired_snapshot.validate_active_snapshot_lease(&token),
            Err(SnapshotLeaseValidationError::LeaseExpired)
        );

        assert_eq!(
            state
                .apply(
                    &PublicationAction::ReleaseLease {
                        lease_id: token.lease_id.clone(),
                        expected_lease_epoch: token.lease_epoch,
                    },
                    context(4),
                    PublicationAuthorityFaults::default(),
                )
                .status,
            PublicationCommandStatus::Accepted
        );
        assert_eq!(
            state.validate_active_snapshot_lease(&token),
            Err(SnapshotLeaseValidationError::LeaseMissing)
        );
    }

    fn collection_state(input: &ObjectReference) -> PublicationAuthorityState {
        let publication = intent(input, "objects/data", "range-1", None);
        let mut state = PublicationAuthorityState::default();
        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &PublicationAction::Prepare {
                        publication_id: "m0".to_owned(),
                        intent: publication.clone(),
                    },
                    context(1),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &publish_action(&publication, "m0"),
                    context(2),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        for (action, index) in [
            (
                PublicationAction::ConfigureCollectionRoot {
                    expected: None,
                    destination_root: "range-1".to_owned(),
                },
                3,
            ),
            (
                PublicationAction::ObserveCommittedFrontier {
                    committed_frontier: 288,
                },
                4,
            ),
            (
                PublicationAction::SetRetentionWindow {
                    expected_policy_epoch: 0,
                    retention_window: 64,
                },
                5,
            ),
            (
                PublicationAction::ObserveRangeMapEpoch { range_map_epoch: 9 },
                6,
            ),
        ] {
            assert_eq!(
                PublicationCommandStatus::Accepted,
                state
                    .apply(
                        &action,
                        context(index),
                        PublicationAuthorityFaults::default(),
                    )
                    .status
            );
        }
        state
    }

    fn prepare_collection(
        state: &mut PublicationAuthorityState,
        input: &ObjectReference,
        faults: PublicationAuthorityFaults,
    ) -> CollectionJobToken {
        let prepared = state.apply(
            &PublicationAction::PrepareCollection {
                job_id: "j1".to_owned(),
                frozen_floor: 200,
                input_manifest: input.clone(),
                destination_root: "range-1".to_owned(),
                range_map_epoch: 9,
                expected_collected_through: 0,
                output_namespace: "collections/j1/".to_owned(),
            },
            context(7),
            faults,
        );
        let Some(PublicationOutcome::CollectionPrepared { token }) = prepared.outcome else {
            panic!("collection prepare did not return a token: {prepared:?}");
        };
        token
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
    #[allow(clippy::too_many_lines)]
    fn lease_floor_and_collection_history_preserve_exact_roots() {
        let input = reference("objects/m0", ObjectKind::Manifest);
        let output = reference("collections/j1/m1", ObjectKind::Manifest);
        let snapshot = closure(&input, "objects/data");
        let mut state = PublicationAuthorityState::default();
        state.roots.insert("range-1".to_owned(), input.clone());

        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &PublicationAction::ConfigureCollectionRoot {
                        expected: None,
                        destination_root: "range-1".to_owned(),
                    },
                    context(1),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );

        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &PublicationAction::ObserveCommittedFrontier {
                        committed_frontier: 256,
                    },
                    context(2),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(0, state.minimum_readable_version);
        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &PublicationAction::SetRetentionWindow {
                        expected_policy_epoch: 0,
                        retention_window: 64,
                    },
                    context(3),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        assert_eq!(192, state.policy_floor);
        assert_eq!(192, state.minimum_readable_version);

        let rejected = state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "too-old".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                snapshot_version: 191,
                owner: "query-1".to_owned(),
                purpose: "olap".to_owned(),
                deadline_tick: 10,
                closure: snapshot.clone(),
            },
            context(4),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::SnapshotBelowFloor,
            rejected.status
        );

        let acquire_a = state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "lease-a".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                snapshot_version: 200,
                owner: "query-a".to_owned(),
                purpose: "olap".to_owned(),
                deadline_tick: 10,
                closure: snapshot.clone(),
            },
            context(5),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::LeaseAcquired { token: lease_a }) = acquire_a.outcome else {
            panic!("lease A did not return a token");
        };
        assert_eq!(200, lease_a.snapshot_version);

        let acquire_b = state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "lease-b".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                snapshot_version: 224,
                owner: "query-b".to_owned(),
                purpose: "olap".to_owned(),
                deadline_tick: 20,
                closure: snapshot.clone(),
            },
            context(6),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::LeaseAcquired { token: lease_b }) = acquire_b.outcome else {
            panic!("lease B did not return a token");
        };
        assert_eq!(3, state.root_intent_epoch);

        state.apply(
            &PublicationAction::ObserveCommittedFrontier {
                committed_frontier: 288,
            },
            context(7),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(224, state.policy_floor);
        assert_eq!(200, state.minimum_readable_version);
        state.apply(
            &PublicationAction::ObserveRangeMapEpoch { range_map_epoch: 9 },
            context(8),
            PublicationAuthorityFaults::default(),
        );

        let prepared = state.apply(
            &PublicationAction::PrepareCollection {
                job_id: "j1".to_owned(),
                frozen_floor: 200,
                input_manifest: input.clone(),
                destination_root: "range-1".to_owned(),
                range_map_epoch: 9,
                expected_collected_through: 0,
                output_namespace: "collections/j1/".to_owned(),
            },
            context(9),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::CollectionPrepared { token }) = prepared.outcome else {
            panic!("collection did not return a token");
        };

        let reserved_output = state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "output-race".to_owned(),
                mark_epoch: state.root_intent_epoch,
                key: "collections/j1/data".to_owned(),
                identity: identity(),
            },
            context(10),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::ObjectNamedByIntent,
            reserved_output.status
        );

        let mut forged_token = token.clone();
        forged_token.range_map_epoch = 8;
        let forged_receipt = CollectionReceipt {
            token: forged_token,
            output_manifest: output.clone(),
            object_keys: BTreeSet::from([output.key.clone(), "collections/j1/data".to_owned()]),
        };
        let forged_publish = state.apply(
            &PublicationAction::PublishCollection {
                receipt: forged_receipt,
            },
            context(11),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::CollectionReceiptMismatch,
            forged_publish.status
        );

        let expired = state.apply(
            &PublicationAction::AdvanceLeaseClock {
                expected_tick: 0,
                next_tick: 10,
            },
            context(12),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            Some(PublicationOutcome::LeasesExpired {
                lease_ids: vec!["lease-a".to_owned()]
            }),
            expired.outcome
        );
        assert_eq!(224, state.minimum_readable_version);
        assert_eq!(200, state.collection_jobs["j1"].frozen_floor);

        let receipt = CollectionReceipt {
            token,
            output_manifest: output.clone(),
            object_keys: BTreeSet::from([output.key.clone(), "collections/j1/data".to_owned()]),
        };
        let published = state.apply(
            &PublicationAction::PublishCollection {
                receipt: receipt.clone(),
            },
            context(13),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            Some(PublicationOutcome::CollectionPublished { receipt }),
            published.outcome
        );
        assert_eq!(200, state.physically_collected_through);
        assert_eq!(Some(&output), state.roots.get("range-1"));
        assert!(state.collection_jobs.is_empty());

        let lease_root = state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "plan-before-release".to_owned(),
                mark_epoch: state.root_intent_epoch,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(14),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::ObjectNamedByIntent,
            lease_root.status
        );

        let stale_mark = state.root_intent_epoch;
        assert_eq!(
            PublicationCommandStatus::Accepted,
            state
                .apply(
                    &PublicationAction::ReleaseLease {
                        lease_id: "lease-b".to_owned(),
                        expected_lease_epoch: lease_b.lease_epoch,
                    },
                    context(15),
                    PublicationAuthorityFaults::default(),
                )
                .status
        );
        let stale_delete = state.apply(
            &PublicationAction::ReserveDelete {
                plan_id: "stale-plan".to_owned(),
                mark_epoch: stale_mark,
                key: "objects/data".to_owned(),
                identity: identity(),
            },
            context(16),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(
            PublicationCommandStatus::RootIntentEpochChanged,
            stale_delete.status
        );
    }

    #[test]
    fn expiry_cannot_be_reversed_by_renewal() {
        let manifest = reference("objects/m0", ObjectKind::Manifest);
        let mut state = PublicationAuthorityState::default();
        state.apply(
            &PublicationAction::SetRetentionWindow {
                expected_policy_epoch: 0,
                retention_window: 64,
            },
            context(1),
            PublicationAuthorityFaults::default(),
        );
        state.apply(
            &PublicationAction::ObserveCommittedFrontier {
                committed_frontier: 128,
            },
            context(2),
            PublicationAuthorityFaults::default(),
        );
        let acquired = state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "lease".to_owned(),
                tenant_id: "tenant".to_owned(),
                snapshot_version: 96,
                owner: "query".to_owned(),
                purpose: "read".to_owned(),
                deadline_tick: 5,
                closure: closure(&manifest, "objects/data"),
            },
            context(3),
            PublicationAuthorityFaults::default(),
        );
        let Some(PublicationOutcome::LeaseAcquired { token }) = acquired.outcome else {
            panic!("acquire did not return token");
        };
        state.apply(
            &PublicationAction::AdvanceLeaseClock {
                expected_tick: 0,
                next_tick: 5,
            },
            context(4),
            PublicationAuthorityFaults::default(),
        );
        let renewal = state.apply(
            &PublicationAction::RenewLease {
                lease_id: "lease".to_owned(),
                expected_lease_epoch: token.lease_epoch,
                new_deadline_tick: 10,
            },
            context(5),
            PublicationAuthorityFaults::default(),
        );
        assert_eq!(PublicationCommandStatus::LeaseMissing, renewal.status);
    }

    #[test]
    fn format_one_state_without_lease_fields_reads_with_safe_defaults() {
        let encoded = "{\"format_version\":1,\"revision\":{\"term\":0,\"index\":0},\"root_intent_epoch\":0,\"intents\":{},\"roots\":{},\"pins\":{},\"deletion_reservations\":{}}";
        let state: PublicationAuthorityState = serde_json::from_str(encoded).unwrap();
        assert_eq!(PublicationAuthorityState::default(), state);
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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn lease_and_collection_faults_expose_each_frozen_violation() {
        let input = reference("objects/m0", ObjectKind::Manifest);
        let output = reference("collections/j1/m1", ObjectKind::Manifest);

        let mut backdated_state = collection_state(&input);
        let backdated = backdated_state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "lease-old".to_owned(),
                tenant_id: "tenant".to_owned(),
                snapshot_version: 223,
                owner: "query".to_owned(),
                purpose: "read".to_owned(),
                deadline_tick: 10,
                closure: closure(&input, "objects/data"),
            },
            context(8),
            PublicationAuthorityFaults {
                accept_backdated_lease: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, backdated.status);

        let mut epoch_state = collection_state(&input);
        let epoch_before_lease = epoch_state.root_intent_epoch;
        let acquired = epoch_state.apply(
            &PublicationAction::AcquireLease {
                lease_id: "lease".to_owned(),
                tenant_id: "tenant".to_owned(),
                snapshot_version: 224,
                owner: "query".to_owned(),
                purpose: "read".to_owned(),
                deadline_tick: 10,
                closure: closure(&input, "objects/data"),
            },
            context(8),
            PublicationAuthorityFaults {
                omit_lease_root_epoch: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, acquired.status);
        assert_eq!(epoch_before_lease, epoch_state.root_intent_epoch);

        let mut premature_state = collection_state(&input);
        prepare_collection(
            &mut premature_state,
            &input,
            PublicationAuthorityFaults {
                advance_collection_without_publication: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(200, premature_state.physically_collected_through);
        assert_eq!(Some(&input), premature_state.roots.get("range-1"));

        let mut range_state = collection_state(&input);
        let range_token = prepare_collection(
            &mut range_state,
            &input,
            PublicationAuthorityFaults::default(),
        );
        range_state.apply(
            &PublicationAction::ObserveRangeMapEpoch {
                range_map_epoch: 10,
            },
            context(8),
            PublicationAuthorityFaults::default(),
        );
        let range_published = range_state.apply(
            &PublicationAction::PublishCollection {
                receipt: CollectionReceipt {
                    token: range_token,
                    output_manifest: output.clone(),
                    object_keys: BTreeSet::from([output.key.clone()]),
                },
            },
            context(9),
            PublicationAuthorityFaults {
                ignore_collection_range_epoch: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, range_published.status);

        let mut root_state = collection_state(&input);
        let root_token = prepare_collection(
            &mut root_state,
            &input,
            PublicationAuthorityFaults::default(),
        );
        let intervening = reference("objects/mx", ObjectKind::Manifest);
        let intervening_intent = intent(
            &intervening,
            "objects/mx-data",
            "range-1",
            Some(input.clone()),
        );
        root_state.apply(
            &PublicationAction::Prepare {
                publication_id: "mx".to_owned(),
                intent: intervening_intent.clone(),
            },
            context(8),
            PublicationAuthorityFaults::default(),
        );
        root_state.apply(
            &publish_action(&intervening_intent, "mx"),
            context(9),
            PublicationAuthorityFaults::default(),
        );
        let root_published = root_state.apply(
            &PublicationAction::PublishCollection {
                receipt: CollectionReceipt {
                    token: root_token,
                    output_manifest: output,
                    object_keys: BTreeSet::from(["collections/j1/m1".to_owned()]),
                },
            },
            context(10),
            PublicationAuthorityFaults {
                ignore_collection_input_root: true,
                ..PublicationAuthorityFaults::default()
            },
        );
        assert_eq!(PublicationCommandStatus::Accepted, root_published.status);
    }
}
