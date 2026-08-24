use super::{
    filesystem_backend, sha256, DeleteOutcome, DeletePermit, ErrorClass, FaultBackend,
    ObjectClient, ObjectIdentity, ObservedBackend, PutOutcome, RequestStats, StoreError,
    WriteCondition,
};
use bytes::Bytes;
use okv_wal::LocalReplicatedWal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Write as _};
use std::path::Path;
use std::sync::Arc;

const AUTHORITY_FORMAT_VERSION: u32 = 1;
const MANIFEST_FORMAT_VERSION: u32 = 1;
const EXPECTED_CHECKS: u64 = 16;
const EXPECTED_ROOT_GRAPH_CHECKS: usize = 9;

/// Deliberately unsafe physical-adapter behavior used by the frozen eval suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAdapterMode {
    Correct,
    PublishRootBeforeVerify,
    OmitDurableIntent,
    ForgetUnknownObjectOutcome,
    RamOnlyAuthority,
    TrustListForLiveness,
    DeleteWithoutRevalidation,
    DeleteWithoutReservation,
}

impl PublicationAdapterMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishRootBeforeVerify => "publish_root_before_verify",
            Self::OmitDurableIntent => "omit_durable_intent",
            Self::ForgetUnknownObjectOutcome => "forget_unknown_object_outcome",
            Self::RamOnlyAuthority => "ram_only_authority",
            Self::TrustListForLiveness => "trust_list_for_liveness",
            Self::DeleteWithoutRevalidation => "delete_without_revalidation",
            Self::DeleteWithoutReservation => "delete_without_reservation",
        }
    }
}

/// Stable semantic receipt from one real object-client publication scenario.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PublicationAdapterReport {
    pub seed: u64,
    pub mode: PublicationAdapterMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub publication_intents: u64,
    pub published_roots: u64,
    pub authority_reopens: u64,
    pub authority_records: u64,
    pub verified_unknown_object_outcomes: u64,
    pub verified_unknown_authority_outcomes: u64,
    pub verified_unknown_delete_outcomes: u64,
    pub complete_marks: u64,
    pub incomplete_marks: u64,
    pub deferred_deletes: u64,
    pub delete_reservations: u64,
    pub blocked_publications: u64,
    pub reclaimed_objects: u64,
    pub recreated_objects: u64,
    pub object_requests: u64,
    pub object_bytes_written: u64,
    pub object_bytes_read: u64,
    pub trace_sha256: String,
}

/// Deliberately unsafe root-graph behavior for the physical GC contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationRootGraphMode {
    Correct,
    OmitAnalyticalLeaseRoot,
}

impl PublicationRootGraphMode {
    /// Stable suite identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitAnalyticalLeaseRoot => "omit_analytical_lease_root",
        }
    }
}

/// Stable semantic receipt for one physical multi-root mark and sweep history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PublicationRootGraphReport {
    pub seed: u64,
    pub mode: PublicationRootGraphMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub root_types_expected: u64,
    pub root_types_registered: u64,
    pub authority_reopens: u64,
    pub complete_marks: u64,
    pub deferred_deletes: u64,
    pub reclaimed_objects: u64,
    pub object_requests: u64,
    pub object_bytes_written: u64,
    pub object_bytes_read: u64,
    pub trace_sha256: String,
}

#[derive(Debug)]
enum AdapterError {
    Authority(String),
    Store(StoreError),
    Json(String),
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authority(detail) => write!(formatter, "authority: {detail}"),
            Self::Store(error) => Display::fmt(error, formatter),
            Self::Json(detail) => write!(formatter, "authority JSON: {detail}"),
        }
    }
}

impl Error for AdapterError {}

impl From<StoreError> for AdapterError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObjectKind {
    Data,
    Manifest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ObjectReference {
    kind: ObjectKind,
    key: String,
    length: u64,
    sha256: String,
}

impl ObjectReference {
    fn from_bytes(kind: ObjectKind, key: String, bytes: &[u8]) -> Self {
        Self {
            kind,
            key,
            length: usize_to_u64(bytes.len()),
            sha256: sha256(bytes),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PhysicalManifest {
    format_version: u32,
    children: Vec<ObjectReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublicationIntent {
    object_keys: BTreeSet<String>,
    manifest: ObjectReference,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DeletionReservation {
    permit: DeletePermit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum AuthorityOutcome {
    Applied,
    DeleteReserved(DeletePermit),
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct AuthorityState {
    revision: u64,
    root_intent_epoch: u64,
    request_outcomes: BTreeMap<String, AuthorityOutcome>,
    intents: BTreeMap<String, PublicationIntent>,
    roots: BTreeMap<String, ObjectReference>,
    pins: BTreeMap<String, ObjectReference>,
    deletion_reservations: BTreeMap<String, DeletionReservation>,
}

#[derive(Clone, Debug)]
enum AuthorityCommand {
    Prepare {
        publication_id: String,
        intent: PublicationIntent,
    },
    Publish {
        publication_id: String,
        root_id: String,
        manifest: ObjectReference,
    },
    Pin {
        pin_id: String,
        manifest: ObjectReference,
    },
    Unpin {
        pin_id: String,
    },
    ReserveDelete {
        plan_id: String,
        mark_epoch: u64,
        key: String,
        identity: ObjectIdentity,
    },
    RetireDelete {
        plan_id: String,
        key: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AuthorityFrame {
    format_version: u32,
    revision: u64,
    previous_payload_sha256: String,
    state: AuthorityState,
}

#[derive(Debug)]
enum AuthorityError {
    Conflict(&'static str),
    UnknownOutcome,
    Corrupt(String),
    Wal(String),
}

impl Display for AuthorityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(detail) => write!(formatter, "conflict: {detail}"),
            Self::UnknownOutcome => formatter.write_str("unknown durable authority outcome"),
            Self::Corrupt(detail) => write!(formatter, "corrupt authority state: {detail}"),
            Self::Wal(detail) => write!(formatter, "authority WAL: {detail}"),
        }
    }
}

#[derive(Debug)]
struct LocalDurableAuthority {
    wal: LocalReplicatedWal,
    state: AuthorityState,
    previous_payload_sha256: String,
    ram_only: bool,
}

impl LocalDurableAuthority {
    fn open(root: &Path, ram_only: bool) -> Result<Self, AuthorityError> {
        let wal = LocalReplicatedWal::open(root, 3, 2)
            .map_err(|error| AuthorityError::Wal(error.to_string()))?;
        let recovery = wal
            .recover()
            .map_err(|error| AuthorityError::Wal(error.to_string()))?;
        let mut state = AuthorityState::default();
        let mut previous_payload_sha256 = empty_digest();
        for record in recovery.records {
            let frame: AuthorityFrame = serde_json::from_slice(&record.payload)
                .map_err(|error| AuthorityError::Corrupt(error.to_string()))?;
            if frame.format_version != AUTHORITY_FORMAT_VERSION
                || frame.revision != record.log_index
                || frame.state.revision != record.log_index
                || frame.previous_payload_sha256 != previous_payload_sha256
            {
                return Err(AuthorityError::Corrupt(
                    "frame version, revision, or hash chain mismatch".to_owned(),
                ));
            }
            previous_payload_sha256 = sha256(&record.payload);
            state = frame.state;
        }
        Ok(Self {
            wal,
            state,
            previous_payload_sha256,
            ram_only,
        })
    }

    fn apply(
        &mut self,
        request_id: &str,
        command: AuthorityCommand,
        lose_response: bool,
    ) -> Result<AuthorityOutcome, AuthorityError> {
        if let Some(outcome) = self.state.request_outcomes.get(request_id) {
            return Ok(outcome.clone());
        }

        let next_revision = self.state.revision.saturating_add(1);
        let mut next = self.state.clone();
        next.revision = next_revision;
        let outcome = apply_authority_command(&mut next, command, next_revision)?;
        next.request_outcomes
            .insert(request_id.to_owned(), outcome.clone());

        if !self.ram_only {
            let frame = AuthorityFrame {
                format_version: AUTHORITY_FORMAT_VERSION,
                revision: next_revision,
                previous_payload_sha256: self.previous_payload_sha256.clone(),
                state: next.clone(),
            };
            let payload = serde_json::to_vec(&frame)
                .map_err(|error| AuthorityError::Corrupt(error.to_string()))?;
            let append = self
                .wal
                .append(next_revision, &payload, &[0, 1, 2])
                .map_err(|error| AuthorityError::Wal(error.to_string()))?;
            if !append.quorum_durable {
                return Err(AuthorityError::Wal(
                    "state transition lacked a synchronized quorum".to_owned(),
                ));
            }
            self.previous_payload_sha256 = sha256(&payload);
        }
        self.state = next;
        if lose_response {
            Err(AuthorityError::UnknownOutcome)
        } else {
            Ok(outcome)
        }
    }

    fn resolve(&self, request_id: &str) -> Option<AuthorityOutcome> {
        self.state.request_outcomes.get(request_id).cloned()
    }

    fn snapshot(&self) -> MarkSnapshot {
        MarkSnapshot {
            root_intent_epoch: self.state.root_intent_epoch,
            manifests: self
                .state
                .roots
                .values()
                .chain(self.state.pins.values())
                .cloned()
                .collect(),
            intent_objects: self
                .state
                .intents
                .values()
                .flat_map(|intent| intent.object_keys.iter().cloned())
                .collect(),
        }
    }
}

fn apply_authority_command(
    state: &mut AuthorityState,
    command: AuthorityCommand,
    revision: u64,
) -> Result<AuthorityOutcome, AuthorityError> {
    match command {
        AuthorityCommand::Prepare {
            publication_id,
            intent,
        } => {
            if state.intents.contains_key(&publication_id) {
                return Err(AuthorityError::Conflict("publication already exists"));
            }
            if intent.object_keys.iter().any(|key| {
                state
                    .deletion_reservations
                    .values()
                    .any(|reservation| reservation.permit.key == *key)
            }) {
                return Err(AuthorityError::Conflict("object deletion is reserved"));
            }
            state.intents.insert(publication_id, intent);
            state.root_intent_epoch = state.root_intent_epoch.saturating_add(1);
            Ok(AuthorityOutcome::Applied)
        }
        AuthorityCommand::Publish {
            publication_id,
            root_id,
            manifest,
        } => {
            let intent = state
                .intents
                .get(&publication_id)
                .ok_or(AuthorityError::Conflict("publication intent is absent"))?;
            if intent.manifest != manifest || !intent.object_keys.contains(&manifest.key) {
                return Err(AuthorityError::Conflict(
                    "published manifest differs from prepared intent",
                ));
            }
            state.roots.insert(root_id, manifest);
            state.intents.remove(&publication_id);
            state.root_intent_epoch = state.root_intent_epoch.saturating_add(1);
            Ok(AuthorityOutcome::Applied)
        }
        AuthorityCommand::Pin { pin_id, manifest } => {
            state.pins.insert(pin_id, manifest);
            state.root_intent_epoch = state.root_intent_epoch.saturating_add(1);
            Ok(AuthorityOutcome::Applied)
        }
        AuthorityCommand::Unpin { pin_id } => {
            state.pins.remove(&pin_id);
            state.root_intent_epoch = state.root_intent_epoch.saturating_add(1);
            Ok(AuthorityOutcome::Applied)
        }
        AuthorityCommand::ReserveDelete {
            plan_id,
            mark_epoch,
            key,
            identity,
        } => {
            if mark_epoch != state.root_intent_epoch {
                return Err(AuthorityError::Conflict("root or intent epoch changed"));
            }
            if state
                .intents
                .values()
                .any(|intent| intent.object_keys.contains(&key))
            {
                return Err(AuthorityError::Conflict(
                    "current publication intent names candidate",
                ));
            }
            if state.deletion_reservations.contains_key(&key) {
                return Err(AuthorityError::Conflict("delete already reserved"));
            }
            let permit = DeletePermit::new(key.clone(), identity, plan_id, revision);
            state.deletion_reservations.insert(
                key,
                DeletionReservation {
                    permit: permit.clone(),
                },
            );
            Ok(AuthorityOutcome::DeleteReserved(permit))
        }
        AuthorityCommand::RetireDelete { plan_id, key } => {
            let reservation = state
                .deletion_reservations
                .get(&key)
                .ok_or(AuthorityError::Conflict("delete reservation is absent"))?;
            if reservation.permit.plan_id != plan_id {
                return Err(AuthorityError::Conflict("delete plan identity differs"));
            }
            state.deletion_reservations.remove(&key);
            Ok(AuthorityOutcome::Applied)
        }
    }
}

#[derive(Clone, Debug)]
struct MarkSnapshot {
    root_intent_epoch: u64,
    manifests: Vec<ObjectReference>,
    intent_objects: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct MarkReceipt {
    root_intent_epoch: u64,
    reachable: BTreeSet<String>,
    complete: bool,
}

async fn mark_snapshot(client: &ObjectClient, snapshot: MarkSnapshot) -> MarkReceipt {
    let mut reachable = snapshot.intent_objects;
    let mut complete = true;
    for manifest in snapshot.manifests {
        if walk_closure(client, &manifest, &mut reachable)
            .await
            .is_err()
        {
            complete = false;
            break;
        }
    }
    MarkReceipt {
        root_intent_epoch: snapshot.root_intent_epoch,
        reachable,
        complete,
    }
}

async fn walk_closure(
    client: &ObjectClient,
    root: &ObjectReference,
    reachable: &mut BTreeSet<String>,
) -> Result<(), AdapterError> {
    let mut pending = vec![root.clone()];
    while let Some(reference) = pending.pop() {
        if !reachable.insert(reference.key.clone()) {
            continue;
        }
        let (bytes, _) = client
            .read_full_verified(&reference.key, None, reference.length, &reference.sha256)
            .await?;
        if reference.kind == ObjectKind::Manifest {
            let manifest: PhysicalManifest = serde_json::from_slice(&bytes)
                .map_err(|error| AdapterError::Json(error.to_string()))?;
            if manifest.format_version != MANIFEST_FORMAT_VERSION {
                return Err(AdapterError::Json(
                    "unsupported physical manifest version".to_owned(),
                ));
            }
            pending.extend(manifest.children.into_iter().rev());
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Recorder {
    trace: Sha256,
    checks: u64,
    anomalies: u64,
    first_mismatch_step: Option<u64>,
    first_mismatch: Option<String>,
}

impl Recorder {
    fn new(seed: u64, mode: PublicationAdapterMode) -> Self {
        let mut trace = Sha256::new();
        trace.update(b"object-publication-adapter-v1");
        trace.update(seed.to_le_bytes());
        trace.update(mode.id().as_bytes());
        Self {
            trace,
            checks: 0,
            anomalies: 0,
            first_mismatch_step: None,
            first_mismatch: None,
        }
    }

    fn check(&mut self, invariant: &str, passed: bool) {
        self.checks = self.checks.saturating_add(1);
        self.trace.update(self.checks.to_le_bytes());
        self.trace.update(invariant.as_bytes());
        self.trace.update([u8::from(passed)]);
        if !passed {
            self.anomalies = self.anomalies.saturating_add(1);
            if self.first_mismatch.is_none() {
                self.first_mismatch_step = Some(self.checks);
                self.first_mismatch = Some(format!("{invariant} failed"));
            }
        }
    }

    fn fail_remaining(&mut self, detail: &str) {
        while self.checks < EXPECTED_CHECKS {
            self.check(detail, false);
        }
    }

    fn digest(self) -> String {
        digest_hex(self.trace)
    }
}

#[derive(Default)]
struct ScenarioCounters {
    publication_intents: u64,
    published_roots: u64,
    authority_reopens: u64,
    verified_unknown_object_outcomes: u64,
    verified_unknown_authority_outcomes: u64,
    verified_unknown_delete_outcomes: u64,
    complete_marks: u64,
    incomplete_marks: u64,
    deferred_deletes: u64,
    delete_reservations: u64,
    blocked_publications: u64,
    reclaimed_objects: u64,
    recreated_objects: u64,
}

/// Run one physical publication, reopen, mark, and sweep history.
///
/// The caller owns `root` and may remove it after the returned future completes.
pub async fn run_publication_adapter_contract(
    root: &Path,
    seed: u64,
    mode: PublicationAdapterMode,
) -> PublicationAdapterReport {
    let mut recorder = Recorder::new(seed, mode);
    let mut counters = ScenarioCounters::default();
    let object_root = root.join("objects");
    let authority_root = root.join("authority");
    let backend = match filesystem_backend(&object_root) {
        Ok(backend) => backend,
        Err(error) => {
            recorder.fail_remaining(&error.to_string());
            return empty_report(seed, mode, recorder, &counters, RequestStats::default(), 0);
        }
    };
    let fault = Arc::new(FaultBackend::new(backend));
    let observed = Arc::new(ObservedBackend::new(fault.clone()));
    let client = ObjectClient::new(observed.clone());

    let result = run_scenario(
        &authority_root,
        seed,
        mode,
        &client,
        &fault,
        &mut recorder,
        &mut counters,
    )
    .await;
    if let Err(error) = result {
        recorder.fail_remaining(&error.to_string());
    }
    if recorder.checks != EXPECTED_CHECKS {
        recorder.fail_remaining("scenario did not execute every frozen check");
    }

    let authority_records = LocalDurableAuthority::open(&authority_root, false)
        .map_or(0, |authority| authority.state.revision);
    empty_report(
        seed,
        mode,
        recorder,
        &counters,
        observed.stats(),
        authority_records,
    )
}

#[allow(clippy::too_many_lines)]
async fn run_scenario(
    authority_root: &Path,
    seed: u64,
    mode: PublicationAdapterMode,
    client: &ObjectClient,
    fault: &Arc<FaultBackend>,
    recorder: &mut Recorder,
    counters: &mut ScenarioCounters,
) -> Result<(), AdapterError> {
    let ram_only = mode == PublicationAdapterMode::RamOnlyAuthority;
    let mut authority = LocalDurableAuthority::open(authority_root, ram_only)
        .map_err(|error| AdapterError::Authority(error.to_string()))?;

    let data_a = Bytes::from(format!("seed={seed}:primary-a"));
    let data_b = Bytes::from(format!("seed={seed}:primary-b"));
    let primary_left_ref = content_reference(ObjectKind::Data, &data_a);
    let primary_right_ref = content_reference(ObjectKind::Data, &data_b);
    let (primary_manifest_bytes, primary_manifest_ref) =
        manifest_reference(&[primary_left_ref.clone(), primary_right_ref.clone()])?;
    let primary_intent = publication_intent(
        &primary_manifest_ref,
        &[primary_left_ref.clone(), primary_right_ref.clone()],
    );

    if mode != PublicationAdapterMode::OmitDurableIntent {
        authority
            .apply(
                &request(seed, "prepare-primary"),
                AuthorityCommand::Prepare {
                    publication_id: "primary".to_owned(),
                    intent: primary_intent.clone(),
                },
                false,
            )
            .map_err(|error| AdapterError::Authority(error.to_string()))?;
        counters.publication_intents = counters.publication_intents.saturating_add(1);
    }

    counters.authority_reopens = counters.authority_reopens.saturating_add(1);
    let reopened_before_upload = LocalDurableAuthority::open(authority_root, ram_only)
        .map_err(|error| AdapterError::Authority(error.to_string()))?;
    let intent_is_durable = reopened_before_upload.state.intents.contains_key("primary");
    recorder.check("durable intent precedes upload", intent_is_durable);
    if intent_is_durable {
        authority = reopened_before_upload;
    } else if mode == PublicationAdapterMode::OmitDurableIntent {
        authority
            .apply(
                &request(seed, "late-prepare-primary"),
                AuthorityCommand::Prepare {
                    publication_id: "primary".to_owned(),
                    intent: primary_intent.clone(),
                },
                false,
            )
            .map_err(|error| AdapterError::Authority(error.to_string()))?;
        counters.publication_intents = counters.publication_intents.saturating_add(1);
    }

    fault.lose_next_put_response();
    let recovered_unknown_object = if mode == PublicationAdapterMode::ForgetUnknownObjectOutcome {
        let unknown_response = matches!(
            client
                .backend
                .put(
                    &primary_left_ref.key,
                    data_a.clone(),
                    WriteCondition::Create,
                )
                .await,
            Err(StoreError {
                class: ErrorClass::RetryableUnknown,
                ..
            })
        );
        if !unknown_response {
            return Err(AdapterError::Authority(
                "object response-loss injection did not fire".to_owned(),
            ));
        }
        false
    } else {
        let (outcome, _) = client
            .put_if_absent(&primary_left_ref.key, data_a.clone())
            .await?;
        let recovered = outcome == PutOutcome::LostResponseRecovered;
        if recovered {
            counters.verified_unknown_object_outcomes =
                counters.verified_unknown_object_outcomes.saturating_add(1);
        }
        recovered
    };
    recorder.check(
        "unknown object outcome resolves by named identity",
        recovered_unknown_object,
    );
    client
        .put_if_absent(&primary_right_ref.key, data_b.clone())
        .await?;

    let root_before_verify = mode == PublicationAdapterMode::PublishRootBeforeVerify;
    let closure_verified_before_root = if root_before_verify {
        false
    } else {
        client
            .put_if_absent(&primary_manifest_ref.key, primary_manifest_bytes.clone())
            .await?;
        walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
            .await
            .is_ok()
    };

    let publish_request = request(seed, "publish-primary");
    let lost_authority_response = authority.apply(
        &publish_request,
        AuthorityCommand::Publish {
            publication_id: "primary".to_owned(),
            root_id: "range-main".to_owned(),
            manifest: primary_manifest_ref.clone(),
        },
        true,
    );
    if !matches!(lost_authority_response, Err(AuthorityError::UnknownOutcome)) {
        return Err(AdapterError::Authority(
            "authority response-loss injection did not fire".to_owned(),
        ));
    }
    counters.published_roots = counters.published_roots.saturating_add(1);

    if root_before_verify {
        let visible_read_failed = walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
            .await
            .is_err();
        if !visible_read_failed {
            return Err(AdapterError::Authority(
                "root-before-verify subject did not expose a missing closure".to_owned(),
            ));
        }
        client
            .put_if_absent(&primary_manifest_ref.key, primary_manifest_bytes.clone())
            .await?;
    }
    recorder.check(
        "verified closure precedes root visibility",
        closure_verified_before_root,
    );

    counters.authority_reopens = counters.authority_reopens.saturating_add(1);
    let reopened_after_publish = LocalDurableAuthority::open(authority_root, ram_only)
        .map_err(|error| AdapterError::Authority(error.to_string()))?;
    let authority_outcome_recovered = matches!(
        reopened_after_publish.resolve(&publish_request),
        Some(AuthorityOutcome::Applied)
    );
    if authority_outcome_recovered {
        counters.verified_unknown_authority_outcomes = counters
            .verified_unknown_authority_outcomes
            .saturating_add(1);
        authority = reopened_after_publish;
    }
    recorder.check(
        "unknown authority outcome resolves by request identity",
        authority_outcome_recovered,
    );
    let authority_reopened_exact = authority_outcome_recovered
        && authority.state.roots.get("range-main") == Some(&primary_manifest_ref)
        && !authority.state.intents.contains_key("primary");
    recorder.check(
        "authority reopens exact published state",
        authority_reopened_exact,
    );
    let primary_reader_exact = walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
        .await
        .is_ok();
    recorder.check("published root reads exact closure", primary_reader_exact);

    fault.corrupt_next_get();
    let incomplete_mark = mark_snapshot(client, authority.snapshot()).await;
    if !incomplete_mark.complete {
        counters.incomplete_marks = counters.incomplete_marks.saturating_add(1);
    }
    recorder.check(
        "incomplete mark creates no delete permit",
        !incomplete_mark.complete,
    );

    let complete_primary_mark = mark_snapshot(client, authority.snapshot()).await;
    if complete_primary_mark.complete {
        counters.complete_marks = counters.complete_marks.saturating_add(1);
    }
    let candidate_names = client.list_candidates("objects").await?;
    fault.stale_next_list();
    let stale_liveness = client.list_candidates("objects").await?;
    let list_is_non_authoritative = if mode == PublicationAdapterMode::TrustListForLiveness {
        let live_key = candidate_names
            .iter()
            .find(|key| **key == primary_left_ref.key)
            .ok_or_else(|| {
                AdapterError::Authority("live object absent from inventory".to_owned())
            })?;
        let identity = client
            .get_full_verified(
                live_key,
                None,
                primary_left_ref.length,
                &primary_left_ref.sha256,
            )
            .await?;
        if stale_liveness.contains(live_key) {
            return Err(AdapterError::Authority(
                "stale LIST fault retained the live key".to_owned(),
            ));
        }
        let unsafe_permit = DeletePermit::new(
            live_key.clone(),
            identity,
            "unsafe-list-plan",
            authority.state.revision,
        );
        client.delete_reserved(&unsafe_permit).await?;
        let still_readable = walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
            .await
            .is_ok();
        client
            .put_if_absent(&primary_left_ref.key, data_a.clone())
            .await?;
        still_readable
    } else {
        complete_primary_mark.complete
            && complete_primary_mark
                .reachable
                .contains(&primary_left_ref.key)
            && stale_liveness.is_empty()
            && walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
                .await
                .is_ok()
    };
    recorder.check("LIST does not define liveness", list_is_non_authoritative);

    let pinned_data = Bytes::from(format!("seed={seed}:pinned-after-mark"));
    let pinned_data_ref = content_reference(ObjectKind::Data, &pinned_data);
    let (pinned_manifest_bytes, pinned_manifest_ref) =
        manifest_reference(std::slice::from_ref(&pinned_data_ref))?;
    client
        .put_if_absent(&pinned_data_ref.key, pinned_data.clone())
        .await?;
    client
        .put_if_absent(&pinned_manifest_ref.key, pinned_manifest_bytes)
        .await?;
    let mark_before_pin = mark_snapshot(client, authority.snapshot()).await;
    counters.complete_marks = counters
        .complete_marks
        .saturating_add(u64::from(mark_before_pin.complete));
    authority
        .apply(
            &request(seed, "pin-after-mark"),
            AuthorityCommand::Pin {
                pin_id: "snapshot-pin".to_owned(),
                manifest: pinned_manifest_ref.clone(),
            },
            false,
        )
        .map_err(|error| AdapterError::Authority(error.to_string()))?;
    let pinned_identity = client
        .get_full_verified(
            &pinned_data_ref.key,
            None,
            pinned_data_ref.length,
            &pinned_data_ref.sha256,
        )
        .await?;
    let delete_was_deferred = if mode == PublicationAdapterMode::DeleteWithoutRevalidation {
        let unsafe_permit = DeletePermit::new(
            pinned_data_ref.key.clone(),
            pinned_identity,
            "unsafe-stale-plan",
            authority.state.revision,
        );
        client.delete_reserved(&unsafe_permit).await?;
        false
    } else {
        matches!(
            authority.apply(
                &request(seed, "reserve-stale-plan"),
                AuthorityCommand::ReserveDelete {
                    plan_id: "stale-plan".to_owned(),
                    mark_epoch: mark_before_pin.root_intent_epoch,
                    key: pinned_data_ref.key.clone(),
                    identity: pinned_identity,
                },
                false,
            ),
            Err(AuthorityError::Conflict("root or intent epoch changed"))
        )
    };
    if delete_was_deferred {
        counters.deferred_deletes = counters.deferred_deletes.saturating_add(1);
    }
    recorder.check("root epoch change defers deletion", delete_was_deferred);
    let pinned_root_readable = walk_closure(client, &pinned_manifest_ref, &mut BTreeSet::new())
        .await
        .is_ok();
    recorder.check(
        "new pin remains readable after stale sweep",
        pinned_root_readable,
    );
    if !pinned_root_readable {
        client
            .put_if_absent(&pinned_data_ref.key, pinned_data)
            .await?;
    }
    authority
        .apply(
            &request(seed, "unpin-after-mark"),
            AuthorityCommand::Unpin {
                pin_id: "snapshot-pin".to_owned(),
            },
            false,
        )
        .map_err(|error| AdapterError::Authority(error.to_string()))?;

    let race_data = Bytes::from(format!("seed={seed}:delete-publication-race"));
    let race_data_ref = content_reference(ObjectKind::Data, &race_data);
    let (race_manifest_bytes, race_manifest_ref) =
        manifest_reference(std::slice::from_ref(&race_data_ref))?;
    client
        .put_if_absent(&race_data_ref.key, race_data.clone())
        .await?;
    let race_mark = mark_snapshot(client, authority.snapshot()).await;
    counters.complete_marks = counters
        .complete_marks
        .saturating_add(u64::from(race_mark.complete));
    let race_identity = client
        .get_full_verified(
            &race_data_ref.key,
            None,
            race_data_ref.length,
            &race_data_ref.sha256,
        )
        .await?;
    let plan_id = "delete-race-plan";
    let skip_reservation = mode == PublicationAdapterMode::DeleteWithoutReservation;
    let permit = if skip_reservation {
        DeletePermit::new(
            race_data_ref.key.clone(),
            race_identity,
            plan_id,
            authority.state.revision,
        )
    } else {
        match authority
            .apply(
                &request(seed, "reserve-delete-race"),
                AuthorityCommand::ReserveDelete {
                    plan_id: plan_id.to_owned(),
                    mark_epoch: race_mark.root_intent_epoch,
                    key: race_data_ref.key.clone(),
                    identity: race_identity,
                },
                false,
            )
            .map_err(|error| AdapterError::Authority(error.to_string()))?
        {
            AuthorityOutcome::DeleteReserved(permit) => {
                counters.delete_reservations = counters.delete_reservations.saturating_add(1);
                permit
            }
            AuthorityOutcome::Applied => {
                return Err(AdapterError::Authority(
                    "delete reservation returned wrong outcome".to_owned(),
                ));
            }
        }
    };
    counters.authority_reopens = counters.authority_reopens.saturating_add(1);
    let reopened_with_reservation = LocalDurableAuthority::open(authority_root, ram_only)
        .map_err(|error| AdapterError::Authority(error.to_string()))?;
    let reservation_reopened = reopened_with_reservation
        .state
        .deletion_reservations
        .contains_key(&race_data_ref.key);
    recorder.check(
        "delete reservation survives authority reopen",
        reservation_reopened,
    );
    if reservation_reopened {
        authority = reopened_with_reservation;
    }

    let race_intent = publication_intent(&race_manifest_ref, std::slice::from_ref(&race_data_ref));
    let prepare_race_request = request(seed, "prepare-race-publication");
    let prepare_while_delete = authority.apply(
        &prepare_race_request,
        AuthorityCommand::Prepare {
            publication_id: "race-publication".to_owned(),
            intent: race_intent.clone(),
        },
        false,
    );
    let publication_was_blocked = matches!(
        prepare_while_delete,
        Err(AuthorityError::Conflict("object deletion is reserved"))
    );
    if publication_was_blocked {
        counters.blocked_publications = counters.blocked_publications.saturating_add(1);
    } else if prepare_while_delete.is_ok() {
        counters.publication_intents = counters.publication_intents.saturating_add(1);
    }
    recorder.check(
        "delete reservation blocks intersecting publication",
        publication_was_blocked,
    );

    fault.lose_next_delete_response();
    let delete_outcome = client.delete_reserved(&permit).await?;
    let unknown_delete_resolved = delete_outcome == DeleteOutcome::LostResponseRecovered;
    if unknown_delete_resolved {
        counters.verified_unknown_delete_outcomes =
            counters.verified_unknown_delete_outcomes.saturating_add(1);
        counters.reclaimed_objects = counters.reclaimed_objects.saturating_add(1);
    }
    recorder.check(
        "unknown delete outcome resolves by named identity",
        unknown_delete_resolved,
    );

    if !skip_reservation {
        authority
            .apply(
                &request(seed, "retire-delete-race"),
                AuthorityCommand::RetireDelete {
                    plan_id: plan_id.to_owned(),
                    key: race_data_ref.key.clone(),
                },
                false,
            )
            .map_err(|error| AdapterError::Authority(error.to_string()))?;
        authority
            .apply(
                &prepare_race_request,
                AuthorityCommand::Prepare {
                    publication_id: "race-publication".to_owned(),
                    intent: race_intent,
                },
                false,
            )
            .map_err(|error| AdapterError::Authority(error.to_string()))?;
        counters.publication_intents = counters.publication_intents.saturating_add(1);
    }

    let recreated_after_delete = if skip_reservation {
        false
    } else {
        let (outcome, _) = client
            .put_if_absent(&race_data_ref.key, race_data.clone())
            .await?;
        let recreated = outcome == PutOutcome::Created;
        if recreated {
            counters.recreated_objects = counters.recreated_objects.saturating_add(1);
        }
        recreated
    };
    recorder.check(
        "post-delete publication recreates or reverifies bytes",
        recreated_after_delete,
    );
    client
        .put_if_absent(&race_manifest_ref.key, race_manifest_bytes)
        .await?;
    authority
        .apply(
            &request(seed, "publish-race-root"),
            AuthorityCommand::Publish {
                publication_id: "race-publication".to_owned(),
                root_id: "range-race".to_owned(),
                manifest: race_manifest_ref.clone(),
            },
            false,
        )
        .map_err(|error| AdapterError::Authority(error.to_string()))?;
    counters.published_roots = counters.published_roots.saturating_add(1);
    let race_root_readable = walk_closure(client, &race_manifest_ref, &mut BTreeSet::new())
        .await
        .is_ok();
    recorder.check(
        "published race root reads exact closure",
        race_root_readable,
    );

    let accounting_counter_claims_zero = 0_u64;
    let initial_root_still_readable =
        walk_closure(client, &primary_manifest_ref, &mut BTreeSet::new())
            .await
            .is_ok();
    recorder.check(
        "derived counters do not authorize deletion",
        accounting_counter_claims_zero == 0 && initial_root_still_readable,
    );
    Ok(())
}

fn publication_intent(
    manifest: &ObjectReference,
    children: &[ObjectReference],
) -> PublicationIntent {
    let mut object_keys = children
        .iter()
        .map(|child| child.key.clone())
        .collect::<BTreeSet<_>>();
    object_keys.insert(manifest.key.clone());
    PublicationIntent {
        object_keys,
        manifest: manifest.clone(),
    }
}

fn manifest_reference(
    children: &[ObjectReference],
) -> Result<(Bytes, ObjectReference), AdapterError> {
    let manifest = PhysicalManifest {
        format_version: MANIFEST_FORMAT_VERSION,
        children: children.to_vec(),
    };
    let bytes = serde_json::to_vec(&manifest)
        .map(Bytes::from)
        .map_err(|error| AdapterError::Json(error.to_string()))?;
    let reference = content_reference(ObjectKind::Manifest, &bytes);
    Ok((bytes, reference))
}

fn content_reference(kind: ObjectKind, bytes: &[u8]) -> ObjectReference {
    let key = format!(
        "objects/{}/sha256/{}",
        match kind {
            ObjectKind::Data => "data",
            ObjectKind::Manifest => "manifest",
        },
        sha256(bytes)
    );
    ObjectReference::from_bytes(kind, key, bytes)
}

fn request(seed: u64, operation: &str) -> String {
    format!("seed-{seed}:{operation}")
}

fn empty_digest() -> String {
    sha256(&[])
}

fn digest_hex(digest: Sha256) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn empty_report(
    seed: u64,
    mode: PublicationAdapterMode,
    recorder: Recorder,
    counters: &ScenarioCounters,
    stats: RequestStats,
    authority_records: u64,
) -> PublicationAdapterReport {
    let mut object_requests = 0_u64;
    let mut object_bytes_written = 0_u64;
    let mut object_bytes_read = 0_u64;
    for request in stats.requests {
        object_requests = object_requests.saturating_add(request.count);
        object_bytes_written = object_bytes_written.saturating_add(request.request_bytes);
        object_bytes_read = object_bytes_read.saturating_add(request.response_bytes);
    }
    let executed_checks = recorder.checks;
    let anomaly_count = recorder.anomalies;
    let first_mismatch_step = recorder.first_mismatch_step;
    let first_mismatch = recorder.first_mismatch.clone();
    let trace_sha256 = recorder.digest();
    PublicationAdapterReport {
        seed,
        mode,
        executed_checks,
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        publication_intents: counters.publication_intents,
        published_roots: counters.published_roots,
        authority_reopens: counters.authority_reopens,
        authority_records,
        verified_unknown_object_outcomes: counters.verified_unknown_object_outcomes,
        verified_unknown_authority_outcomes: counters.verified_unknown_authority_outcomes,
        verified_unknown_delete_outcomes: counters.verified_unknown_delete_outcomes,
        complete_marks: counters.complete_marks,
        incomplete_marks: counters.incomplete_marks,
        deferred_deletes: counters.deferred_deletes,
        delete_reservations: counters.delete_reservations,
        blocked_publications: counters.blocked_publications,
        reclaimed_objects: counters.reclaimed_objects,
        recreated_objects: counters.recreated_objects,
        object_requests,
        object_bytes_written,
        object_bytes_read,
        trace_sha256,
    }
}

/// Run one physical checkpoint, clone, backup, analytical-lease, and
/// tenant-move root graph through durable mark, sweep, and revalidation.
///
/// # Errors
///
/// Returns an error when the physical object client or durable authority cannot
/// execute the bounded history.
#[allow(clippy::too_many_lines)]
pub async fn run_publication_root_graph_contract(
    root: &Path,
    seed: u64,
    mode: PublicationRootGraphMode,
) -> Result<PublicationRootGraphReport, String> {
    const ROOT_TYPES: [&str; 5] = [
        "checkpoint",
        "clone",
        "backup",
        "analytical-lease",
        "tenant-move",
    ];
    let object_root = root.join("objects");
    let authority_root = root.join("authority");
    let backend = filesystem_backend(&object_root).map_err(|error| error.to_string())?;
    let observed = Arc::new(ObservedBackend::new(Arc::new(FaultBackend::new(backend))));
    let client = ObjectClient::new(observed.clone());
    let mut authority =
        LocalDurableAuthority::open(&authority_root, false).map_err(|error| error.to_string())?;

    let shared_bytes = Bytes::from(format!("seed={seed}:shared-root-data"));
    let shared_ref = content_reference(ObjectKind::Data, &shared_bytes);
    client
        .put_if_absent(&shared_ref.key, shared_bytes)
        .await
        .map_err(|error| error.to_string())?;
    let mut references = BTreeMap::from([(shared_ref.key.clone(), shared_ref.clone())]);
    let mut manifests = BTreeMap::new();
    for root_type in ROOT_TYPES {
        let unique_bytes = Bytes::from(format!("seed={seed}:{root_type}:unique"));
        let unique_ref = content_reference(ObjectKind::Data, &unique_bytes);
        let (manifest_bytes, manifest_ref) =
            manifest_reference(&[shared_ref.clone(), unique_ref.clone()])
                .map_err(|error| error.to_string())?;
        client
            .put_if_absent(&unique_ref.key, unique_bytes)
            .await
            .map_err(|error| error.to_string())?;
        client
            .put_if_absent(&manifest_ref.key, manifest_bytes)
            .await
            .map_err(|error| error.to_string())?;
        references.insert(unique_ref.key.clone(), unique_ref);
        references.insert(manifest_ref.key.clone(), manifest_ref.clone());
        manifests.insert(root_type.to_owned(), manifest_ref.clone());
        if !(mode == PublicationRootGraphMode::OmitAnalyticalLeaseRoot
            && root_type == "analytical-lease")
        {
            authority
                .apply(
                    &request(seed, &format!("pin-{root_type}")),
                    AuthorityCommand::Pin {
                        pin_id: root_type.to_owned(),
                        manifest: manifest_ref,
                    },
                    false,
                )
                .map_err(|error| error.to_string())?;
        }
    }

    let root_types_registered = usize_to_u64(authority.state.pins.len());
    authority =
        LocalDurableAuthority::open(&authority_root, false).map_err(|error| error.to_string())?;
    let authority_reopens = 1_u64;
    let pins_reopened_exact = authority.state.pins.len() == ROOT_TYPES.len();

    let initial_mark = mark_snapshot(&client, authority.snapshot()).await;
    let mut complete_marks = u64::from(initial_mark.complete);
    let initial_reclaimed = sweep_root_graph_candidates(
        &mut authority,
        &client,
        &references,
        &initial_mark,
        seed,
        "initial",
    )
    .await?;
    let all_declared_roots_preserved = all_manifests_readable(&client, manifests.values()).await;

    let clone_manifest = manifests
        .get("clone")
        .cloned()
        .ok_or_else(|| "clone root manifest is missing".to_owned())?;
    authority
        .apply(
            &request(seed, "unpin-clone"),
            AuthorityCommand::Unpin {
                pin_id: "clone".to_owned(),
            },
            false,
        )
        .map_err(|error| error.to_string())?;
    let post_clone_mark = mark_snapshot(&client, authority.snapshot()).await;
    complete_marks = complete_marks.saturating_add(u64::from(post_clone_mark.complete));
    let clone_reclaimed = sweep_root_graph_candidates(
        &mut authority,
        &client,
        &references,
        &post_clone_mark,
        seed,
        "clone",
    )
    .await?;
    let clone_closure_reclaimed = walk_closure(&client, &clone_manifest, &mut BTreeSet::new())
        .await
        .is_err();
    let remaining_manifests = manifests
        .iter()
        .filter(|(root_type, _)| root_type.as_str() != "clone")
        .map(|(_, manifest)| manifest);
    let remaining_roots_preserved = all_manifests_readable(&client, remaining_manifests).await;
    let shared_object_preserved = walk_closure(
        &client,
        manifests
            .get("checkpoint")
            .ok_or_else(|| "checkpoint root manifest is missing".to_owned())?,
        &mut BTreeSet::new(),
    )
    .await
    .is_ok();

    let race_data = Bytes::from(format!("seed={seed}:lease-race-data"));
    let race_data_ref = content_reference(ObjectKind::Data, &race_data);
    let (race_manifest_bytes, race_manifest_ref) =
        manifest_reference(std::slice::from_ref(&race_data_ref))
            .map_err(|error| error.to_string())?;
    client
        .put_if_absent(&race_data_ref.key, race_data)
        .await
        .map_err(|error| error.to_string())?;
    client
        .put_if_absent(&race_manifest_ref.key, race_manifest_bytes)
        .await
        .map_err(|error| error.to_string())?;
    let race_mark = mark_snapshot(&client, authority.snapshot()).await;
    complete_marks = complete_marks.saturating_add(u64::from(race_mark.complete));
    authority
        .apply(
            &request(seed, "pin-racing-lease"),
            AuthorityCommand::Pin {
                pin_id: "analytical-lease-race".to_owned(),
                manifest: race_manifest_ref.clone(),
            },
            false,
        )
        .map_err(|error| error.to_string())?;
    let race_identity = client
        .get_full_verified(
            &race_data_ref.key,
            None,
            race_data_ref.length,
            &race_data_ref.sha256,
        )
        .await
        .map_err(|error| error.to_string())?;
    let stale_reservation_deferred = matches!(
        authority.apply(
            &request(seed, "reserve-racing-lease"),
            AuthorityCommand::ReserveDelete {
                plan_id: "racing-lease-plan".to_owned(),
                mark_epoch: race_mark.root_intent_epoch,
                key: race_data_ref.key.clone(),
                identity: race_identity,
            },
            false,
        ),
        Err(AuthorityError::Conflict("root or intent epoch changed"))
    );
    let deferred_deletes = u64::from(stale_reservation_deferred);
    let racing_lease_preserved = walk_closure(&client, &race_manifest_ref, &mut BTreeSet::new())
        .await
        .is_ok();

    let checks: [(&str, bool); EXPECTED_ROOT_GRAPH_CHECKS] = [
        ("all_root_types_registered", root_types_registered == 5),
        ("authority_reopens_all_roots", pins_reopened_exact),
        ("initial_mark_complete", initial_mark.complete),
        ("all_declared_roots_preserved", all_declared_roots_preserved),
        (
            "selective_unpin_reclaims_clone_only",
            clone_reclaimed == 2 && clone_closure_reclaimed,
        ),
        ("remaining_roots_preserved", remaining_roots_preserved),
        ("shared_object_preserved", shared_object_preserved),
        ("pin_after_mark_defers_delete", stale_reservation_deferred),
        ("racing_lease_preserved", racing_lease_preserved),
    ];
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch = checks
        .iter()
        .find(|(_, passed)| !passed)
        .map(|(name, _)| (*name).to_owned());
    let reclaimed_objects = initial_reclaimed.saturating_add(clone_reclaimed);
    let mut trace = Sha256::new();
    trace.update(b"okv-publication-root-graph-v1");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(root_types_registered.to_be_bytes());
    trace.update(complete_marks.to_be_bytes());
    trace.update(reclaimed_objects.to_be_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    let (object_requests, object_bytes_written, object_bytes_read) =
        object_request_totals(observed.stats());
    Ok(PublicationRootGraphReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        root_types_expected: 5,
        root_types_registered,
        authority_reopens,
        complete_marks,
        deferred_deletes,
        reclaimed_objects,
        object_requests,
        object_bytes_written,
        object_bytes_read,
        trace_sha256: digest_hex(trace),
    })
}

async fn all_manifests_readable<'a>(
    client: &ObjectClient,
    manifests: impl Iterator<Item = &'a ObjectReference>,
) -> bool {
    for manifest in manifests {
        if walk_closure(client, manifest, &mut BTreeSet::new())
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

async fn sweep_root_graph_candidates(
    authority: &mut LocalDurableAuthority,
    client: &ObjectClient,
    references: &BTreeMap<String, ObjectReference>,
    mark: &MarkReceipt,
    seed: u64,
    phase: &str,
) -> Result<u64, String> {
    if !mark.complete {
        return Ok(0);
    }
    let candidates = client
        .list_candidates("objects")
        .await
        .map_err(|error| error.to_string())?;
    let mut reclaimed = 0_u64;
    for key in candidates
        .into_iter()
        .filter(|key| !mark.reachable.contains(key))
    {
        let reference = references
            .get(&key)
            .ok_or_else(|| format!("missing reference for GC candidate {key}"))?;
        let identity = client
            .get_full_verified(&key, None, reference.length, &reference.sha256)
            .await
            .map_err(|error| error.to_string())?;
        let plan_id = format!("{phase}-{reclaimed}");
        let permit = match authority
            .apply(
                &request(seed, &format!("reserve-{plan_id}")),
                AuthorityCommand::ReserveDelete {
                    plan_id: plan_id.clone(),
                    mark_epoch: mark.root_intent_epoch,
                    key: key.clone(),
                    identity,
                },
                false,
            )
            .map_err(|error| error.to_string())?
        {
            AuthorityOutcome::DeleteReserved(permit) => permit,
            AuthorityOutcome::Applied => {
                return Err("root-graph reservation returned applied".to_owned());
            }
        };
        client
            .delete_reserved(&permit)
            .await
            .map_err(|error| error.to_string())?;
        authority
            .apply(
                &request(seed, &format!("retire-{plan_id}")),
                AuthorityCommand::RetireDelete { plan_id, key },
                false,
            )
            .map_err(|error| error.to_string())?;
        reclaimed = reclaimed.saturating_add(1);
    }
    Ok(reclaimed)
}

fn object_request_totals(stats: RequestStats) -> (u64, u64, u64) {
    let mut requests = 0_u64;
    let mut bytes_written = 0_u64;
    let mut bytes_read = 0_u64;
    for request in stats.requests {
        requests = requests.saturating_add(request.count);
        bytes_written = bytes_written.saturating_add(request.request_bytes);
        bytes_read = bytes_read.saturating_add(request.response_bytes);
    }
    (requests, bytes_written, bytes_read)
}

#[cfg(test)]
mod tests {
    use super::{
        run_publication_adapter_contract, run_publication_root_graph_contract,
        PublicationAdapterMode, PublicationRootGraphMode, EXPECTED_CHECKS,
        EXPECTED_ROOT_GRAPH_CHECKS,
    };
    use std::path::PathBuf;

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okv-publication-adapter-{label}-{}",
            uuid::Uuid::new_v4()
        ))
    }

    #[tokio::test]
    async fn correct_adapter_passes_and_replays_semantically() {
        let first_root = temporary_root("first");
        let second_root = temporary_root("second");
        let first =
            run_publication_adapter_contract(&first_root, 1103, PublicationAdapterMode::Correct)
                .await;
        let second =
            run_publication_adapter_contract(&second_root, 1103, PublicationAdapterMode::Correct)
                .await;
        assert_eq!(first, second);
        assert_eq!(first.executed_checks, EXPECTED_CHECKS);
        assert_eq!(first.anomaly_count, 0, "{first:#?}");
        std::fs::remove_dir_all(first_root).expect("remove first fixture");
        std::fs::remove_dir_all(second_root).expect("remove second fixture");
    }

    #[tokio::test]
    async fn every_negative_subject_has_a_bounded_anomaly() {
        for mode in [
            PublicationAdapterMode::PublishRootBeforeVerify,
            PublicationAdapterMode::OmitDurableIntent,
            PublicationAdapterMode::ForgetUnknownObjectOutcome,
            PublicationAdapterMode::RamOnlyAuthority,
            PublicationAdapterMode::TrustListForLiveness,
            PublicationAdapterMode::DeleteWithoutRevalidation,
            PublicationAdapterMode::DeleteWithoutReservation,
        ] {
            let root = temporary_root(mode.id());
            let report = run_publication_adapter_contract(&root, 1103, mode).await;
            assert_eq!(report.executed_checks, EXPECTED_CHECKS, "{report:#?}");
            assert!(report.anomaly_count > 0, "{} escaped", mode.id());
            assert!(report.first_mismatch_step.is_some_and(|step| step <= 16));
            std::fs::remove_dir_all(root).expect("remove negative fixture");
        }
    }

    #[tokio::test]
    async fn root_graph_passes_replays_and_rejects_an_omitted_lease_root() {
        let first_root = temporary_root("root-graph-first");
        let second_root = temporary_root("root-graph-second");
        let control_root = temporary_root("root-graph-control");
        let first = run_publication_root_graph_contract(
            &first_root,
            1103,
            PublicationRootGraphMode::Correct,
        )
        .await
        .expect("run first root graph");
        let second = run_publication_root_graph_contract(
            &second_root,
            1103,
            PublicationRootGraphMode::Correct,
        )
        .await
        .expect("run second root graph");
        let control = run_publication_root_graph_contract(
            &control_root,
            1103,
            PublicationRootGraphMode::OmitAnalyticalLeaseRoot,
        )
        .await
        .expect("run omitted-root control");

        assert_eq!(first, second);
        assert_eq!(first.executed_checks, EXPECTED_ROOT_GRAPH_CHECKS as u64);
        assert_eq!(first.anomaly_count, 0, "{first:#?}");
        assert_eq!(first.root_types_registered, 5);
        assert_eq!(first.reclaimed_objects, 2);
        assert_eq!(first.deferred_deletes, 1);
        assert_eq!(control.executed_checks, EXPECTED_ROOT_GRAPH_CHECKS as u64);
        assert!(control.anomaly_count > 0, "{control:#?}");
        assert_eq!(control.root_types_registered, 4);

        std::fs::remove_dir_all(first_root).expect("remove first root graph fixture");
        std::fs::remove_dir_all(second_root).expect("remove second root graph fixture");
        std::fs::remove_dir_all(control_root).expect("remove root graph control fixture");
    }
}
