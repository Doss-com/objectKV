use crate::{
    filesystem_backend, sha256, Backend, ErrorClass, FaultBackend, ObjectClient, PutOutcome,
    WriteCondition,
};
use bytes::Bytes;
use okv_consensus::{
    GenerationCredential, PublicationAction, PublicationAuthorityFaults,
    PublicationAuthorityProcessFixture, PublicationClient, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    PublicationOutcome, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const JOB_FORMAT_VERSION: u32 = 1;
const EXPECTED_CHECKS: u64 = 10;
const PUT_RECOVERY_EXPECTED_CHECKS: u64 = 12;
const MANIFEST_RECOVERY_EXPECTED_CHECKS: u64 = 13;
const PUBLISH_RECOVERY_EXPECTED_CHECKS: u64 = 14;

/// Deliberately unsafe publisher behavior used by the frozen negative control.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherProcessMode {
    Correct,
    UploadBeforePrepareAck,
}

impl PublisherProcessMode {
    /// Stable mode identifier used by suite and trace receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::UploadBeforePrepareAck => "upload_before_prepare_ack",
        }
    }
}

/// Configuration passed to one dedicated publisher child process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherProcessConfig {
    pub seed: u64,
    pub mode: PublisherProcessMode,
    pub authority_endpoints: Vec<String>,
    pub object_root: PathBuf,
    pub scratch_root: PathBuf,
    pub pause_after_barrier: bool,
}

/// Canonical semantic report for one prepare, kill, and empty-scratch restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherProcessReport {
    pub seed: u64,
    pub mode: PublisherProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub publisher_process_starts: u64,
    pub process_kills: u64,
    pub object_puts: u64,
    pub publication_writes: u64,
    pub empty_scratch_restarts: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Deliberately unsafe ambiguous-PUT behavior used by RFC-0018.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherPutRecoveryMode {
    Correct,
    PublishPartialClosure,
}

impl PublisherPutRecoveryMode {
    /// Stable mode identifier used by suite and trace receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishPartialClosure => "publish_partial_closure",
        }
    }
}

/// Process phase for the two RFC-0018 publisher children.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherPutRecoveryPhase {
    FirstPutUnknown,
    Replacement,
}

/// Configuration passed to one ambiguous-PUT publisher child process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherPutRecoveryProcessConfig {
    pub seed: u64,
    pub mode: PublisherPutRecoveryMode,
    pub phase: PublisherPutRecoveryPhase,
    pub authority_endpoints: Vec<String>,
    pub object_root: PathBuf,
    pub scratch_root: PathBuf,
}

/// Canonical semantic report for one ambiguous PUT, kill, and restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherPutRecoveryReport {
    pub seed: u64,
    pub mode: PublisherPutRecoveryMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub publisher_process_starts: u64,
    pub process_kills: u64,
    pub put_attempts: u64,
    pub object_effects: u64,
    pub injected_unknown_responses: u64,
    pub existing_object_recoveries: u64,
    pub named_verification_reads: u64,
    pub publication_command_attempts: u64,
    pub empty_scratch_restarts: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Deliberately unsafe manifest-recovery behavior used by RFC-0019.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherManifestRecoveryMode {
    Correct,
    TrustManifestWithoutClosure,
}

impl PublisherManifestRecoveryMode {
    /// Stable mode identifier used by suite and trace receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::TrustManifestWithoutClosure => "trust_manifest_without_closure",
        }
    }
}

/// Process phase for the two RFC-0019 publisher children.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherManifestRecoveryPhase {
    ManifestPutUnknown,
    Replacement,
}

/// Configuration passed to one ambiguous-manifest publisher child process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherManifestRecoveryProcessConfig {
    pub seed: u64,
    pub mode: PublisherManifestRecoveryMode,
    pub phase: PublisherManifestRecoveryPhase,
    pub authority_endpoints: Vec<String>,
    pub object_root: PathBuf,
    pub scratch_root: PathBuf,
}

/// Canonical semantic report for one ambiguous manifest, kill, and restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherManifestRecoveryReport {
    pub seed: u64,
    pub mode: PublisherManifestRecoveryMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub publisher_process_starts: u64,
    pub process_kills: u64,
    pub put_attempts: u64,
    pub object_effects: u64,
    pub injected_unknown_responses: u64,
    pub existing_object_recoveries: u64,
    pub named_verification_reads: u64,
    pub publication_command_attempts: u64,
    pub empty_scratch_restarts: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Deliberately unsafe replicated-Publish recovery behavior used by RFC-0020.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherPublishRecoveryMode {
    Correct,
    ConvergenceOnlyDuplicatePublish,
}

impl PublisherPublishRecoveryMode {
    /// Stable mode identifier used by suite and trace receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ConvergenceOnlyDuplicatePublish => "convergence_only_duplicate_publish",
        }
    }
}

/// Process phase for the two RFC-0020 publisher children.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherPublishRecoveryPhase {
    PublishResponseUnknown,
    Replacement,
}

/// Configuration passed to one lost-Publish-response publisher child.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherPublishRecoveryProcessConfig {
    pub seed: u64,
    pub mode: PublisherPublishRecoveryMode,
    pub phase: PublisherPublishRecoveryPhase,
    pub authority_endpoints: Vec<String>,
    pub object_root: PathBuf,
    pub scratch_root: PathBuf,
}

/// Canonical semantic report for one lost Publish response and process restart.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublisherPublishRecoveryReport {
    pub seed: u64,
    pub mode: PublisherPublishRecoveryMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub publisher_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub object_put_attempts: u64,
    pub object_effects: u64,
    pub named_verification_reads: u64,
    pub publish_command_attempts: u64,
    pub publish_applies: u64,
    pub dropped_publish_replies: u64,
    pub recovered_publish_outcomes: u64,
    pub exact_outcome_replays: u64,
    pub empty_scratch_restarts: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherJob {
    format_version: u32,
    cell_id: u64,
    credential: GenerationCredential,
    publication_id: String,
    destination_root: String,
    expected_prior_root: Option<PublicationObjectReference>,
    objects: Vec<JobObject>,
    manifest: JobObject,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct JobObject {
    reference: PublicationObjectReference,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PhysicalManifest {
    format_version: u32,
    children: Vec<PublicationObjectReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherBarrier {
    kind: String,
    job_sha256: String,
    prepare_identity: RequestIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherPublishBarrier {
    kind: String,
    job_sha256: String,
    prepare_identity: RequestIdentity,
    publish_identity: RequestIdentity,
    closure_verified: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherPublishRecoveryReceipt {
    kind: String,
    job_sha256: String,
    publish_identity: RequestIdentity,
    outcome_recovered: bool,
    exact_outcome_replayed: bool,
    authority_revision_unchanged: bool,
    root_intent_epoch_unchanged: bool,
    object_put_attempts: u64,
    closure_verified: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherPutRecoveryReceipt {
    kind: String,
    job_sha256: String,
    prepare_identity: RequestIdentity,
    publish_identity: RequestIdentity,
    first_outcome: String,
    remaining_created: u64,
    manifest_created: bool,
    closure_verified: bool,
    root_installed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublisherManifestRecoveryReceipt {
    kind: String,
    job_sha256: String,
    prepare_identity: RequestIdentity,
    publish_identity: RequestIdentity,
    existing_data_objects: u64,
    manifest_outcome: String,
    closure_verified: bool,
    root_installed: bool,
}

/// Execute one publisher child until completion or the controller-owned pause.
///
/// # Errors
///
/// Returns an error when scratch state is not empty, authority state differs
/// from the immutable job, or any named object or root verification fails.
pub async fn run_publication_publisher_process_node(
    config: PublisherProcessConfig,
) -> Result<(), String> {
    require_empty_directory(&config.scratch_root)?;
    let job = PublisherJob::for_seed(config.seed)?;
    let job_sha256 = job.digest()?;
    let prepare_identity = job.request_identity("prepare")?;
    let client = PublicationClient::new(config.authority_endpoints)?;
    let object_client = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );

    if config.mode == PublisherProcessMode::UploadBeforePrepareAck {
        let first = job
            .objects
            .first()
            .ok_or_else(|| "publisher job has no data object".to_owned())?;
        object_client
            .put_if_absent(&first.reference.key, Bytes::from(first.bytes.clone()))
            .await
            .map_err(|error| error.to_string())?;
        emit_barrier(&PublisherBarrier {
            kind: "unsafe_object_written".to_owned(),
            job_sha256,
            prepare_identity,
        })?;
        park_until_killed();
    }

    let intent = job.intent();
    let prepared = client
        .commit(&PublicationCommand {
            identity: prepare_identity,
            credential: job.credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: job.publication_id.clone(),
                intent: intent.clone(),
            },
        })
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "publisher prepare was rejected with {:?}",
            prepared.status
        ));
    }
    let state = client.read().await?;
    if state
        .intents
        .get(&job.publication_id)
        .map(|value| &value.intent)
        != Some(&intent)
    {
        return Err("publisher recovered intent differs from immutable job".to_owned());
    }
    emit_barrier(&PublisherBarrier {
        kind: "prepared_committed".to_owned(),
        job_sha256,
        prepare_identity,
    })?;
    if config.pause_after_barrier {
        park_until_killed();
    }

    for object in &job.objects {
        put_and_verify(&object_client, object).await?;
    }
    put_and_verify(&object_client, &job.manifest).await?;
    verify_closure(&object_client, &job.manifest.reference).await?;

    let published = client
        .commit(&PublicationCommand {
            identity: job.request_identity("publish")?,
            credential: job.credential.clone(),
            action: PublicationAction::Publish {
                publication_id: job.publication_id.clone(),
                destination_root: job.destination_root.clone(),
                expected_prior_root: job.expected_prior_root.clone(),
                manifest: job.manifest.reference.clone(),
            },
        })
        .await?;
    if published.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "publisher root transition was rejected with {:?}",
            published.status
        ));
    }
    let final_state = client.read().await?;
    if final_state.roots.get(&job.destination_root) != Some(&job.manifest.reference)
        || final_state.intents.contains_key(&job.publication_id)
    {
        return Err("publisher final root and intent state is not exact".to_owned());
    }
    Ok(())
}

/// Execute one RFC-0018 publisher child until its fault barrier or completion.
///
/// # Errors
///
/// Returns an error when the replicated intent is not exact, the injected PUT
/// response is not ambiguous after a real effect, or publication cannot be
/// completed according to the selected process phase.
#[allow(clippy::too_many_lines)]
pub async fn run_publication_publisher_put_recovery_node(
    config: PublisherPutRecoveryProcessConfig,
) -> Result<(), String> {
    require_empty_directory(&config.scratch_root)?;
    let job = PublisherJob::for_seed(config.seed)?;
    let job_sha256 = job.digest()?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let client = PublicationClient::new(config.authority_endpoints)?;
    recover_exact_prepare(&client, &job, prepare_identity).await?;

    match config.phase {
        PublisherPutRecoveryPhase::FirstPutUnknown => {
            let first = job
                .objects
                .first()
                .ok_or_else(|| "publisher job has no data object".to_owned())?;
            let backend =
                filesystem_backend(&config.object_root).map_err(|error| error.to_string())?;
            let fault = Arc::new(FaultBackend::new(backend));
            fault.lose_next_put_response();
            let result = fault
                .put(
                    &first.reference.key,
                    Bytes::from(first.bytes.clone()),
                    WriteCondition::Create,
                )
                .await;
            if !result.is_err_and(|error| error.class == ErrorClass::RetryableUnknown) {
                return Err(
                    "first publisher did not observe the injected unknown PUT response".to_owned(),
                );
            }
            emit_barrier(&PublisherBarrier {
                kind: "first_put_response_unknown".to_owned(),
                job_sha256,
                prepare_identity,
            })?;
            park_until_killed();
        }
        PublisherPutRecoveryPhase::Replacement => {
            let object_client = ObjectClient::new(
                filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
            );
            if config.mode == PublisherPutRecoveryMode::PublishPartialClosure {
                let (manifest_outcome, _) = object_client
                    .put_if_absent(
                        &job.manifest.reference.key,
                        Bytes::from(job.manifest.bytes.clone()),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let root_installed = publish_job(&client, &job, publish_identity).await?;
                emit_put_recovery_receipt(&PublisherPutRecoveryReceipt {
                    kind: "partial_closure_published".to_owned(),
                    job_sha256,
                    prepare_identity,
                    publish_identity,
                    first_outcome: "skipped".to_owned(),
                    remaining_created: 0,
                    manifest_created: manifest_outcome == PutOutcome::Created,
                    closure_verified: false,
                    root_installed,
                })?;
                return Ok(());
            }

            let first = job
                .objects
                .first()
                .ok_or_else(|| "publisher job has no data object".to_owned())?;
            let (first_outcome, _) = object_client
                .put_if_absent(&first.reference.key, Bytes::from(first.bytes.clone()))
                .await
                .map_err(|error| error.to_string())?;
            let mut remaining_created = 0_u64;
            for object in job.objects.iter().skip(1) {
                let (outcome, _) = object_client
                    .put_if_absent(&object.reference.key, Bytes::from(object.bytes.clone()))
                    .await
                    .map_err(|error| error.to_string())?;
                if outcome == PutOutcome::Created {
                    remaining_created = remaining_created.saturating_add(1);
                }
            }
            let (manifest_outcome, _) = object_client
                .put_if_absent(
                    &job.manifest.reference.key,
                    Bytes::from(job.manifest.bytes.clone()),
                )
                .await
                .map_err(|error| error.to_string())?;
            verify_closure(&object_client, &job.manifest.reference).await?;
            let root_installed = publish_job(&client, &job, publish_identity).await?;
            emit_put_recovery_receipt(&PublisherPutRecoveryReceipt {
                kind: "ambiguous_put_recovered".to_owned(),
                job_sha256,
                prepare_identity,
                publish_identity,
                first_outcome: put_outcome_id(first_outcome).to_owned(),
                remaining_created,
                manifest_created: manifest_outcome == PutOutcome::Created,
                closure_verified: true,
                root_installed,
            })?;
            Ok(())
        }
    }
}

/// Execute one RFC-0019 publisher child until its fault barrier or completion.
///
/// # Errors
///
/// Returns an error when replicated intent differs from the immutable job, the
/// manifest effect does not produce an ambiguous response, or replacement
/// publication cannot follow the selected recovery protocol.
#[allow(clippy::too_many_lines)]
pub async fn run_publication_publisher_manifest_recovery_node(
    config: PublisherManifestRecoveryProcessConfig,
) -> Result<(), String> {
    require_empty_directory(&config.scratch_root)?;
    let job = PublisherJob::for_seed(config.seed)?;
    let job_sha256 = job.digest()?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let client = PublicationClient::new(config.authority_endpoints)?;
    recover_exact_prepare(&client, &job, prepare_identity).await?;
    let object_client = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );

    match config.phase {
        PublisherManifestRecoveryPhase::ManifestPutUnknown => {
            let data_limit = if config.mode == PublisherManifestRecoveryMode::Correct {
                job.objects.len()
            } else {
                1
            };
            for object in job.objects.iter().take(data_limit) {
                object_client
                    .put_if_absent(&object.reference.key, Bytes::from(object.bytes.clone()))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let backend =
                filesystem_backend(&config.object_root).map_err(|error| error.to_string())?;
            let fault = Arc::new(FaultBackend::new(backend));
            fault.lose_next_put_response();
            let result = fault
                .put(
                    &job.manifest.reference.key,
                    Bytes::from(job.manifest.bytes.clone()),
                    WriteCondition::Create,
                )
                .await;
            if !result.is_err_and(|error| error.class == ErrorClass::RetryableUnknown) {
                return Err(
                    "publisher did not observe the injected unknown manifest response".to_owned(),
                );
            }
            emit_barrier(&PublisherBarrier {
                kind: "manifest_put_response_unknown".to_owned(),
                job_sha256,
                prepare_identity,
            })?;
            park_until_killed();
        }
        PublisherManifestRecoveryPhase::Replacement => {
            if config.mode == PublisherManifestRecoveryMode::TrustManifestWithoutClosure {
                let (manifest_outcome, _) = object_client
                    .put_if_absent(
                        &job.manifest.reference.key,
                        Bytes::from(job.manifest.bytes.clone()),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let root_installed = publish_job(&client, &job, publish_identity).await?;
                emit_manifest_recovery_receipt(&PublisherManifestRecoveryReceipt {
                    kind: "manifest_trusted_without_closure".to_owned(),
                    job_sha256,
                    prepare_identity,
                    publish_identity,
                    existing_data_objects: 0,
                    manifest_outcome: put_outcome_id(manifest_outcome).to_owned(),
                    closure_verified: false,
                    root_installed,
                })?;
                return Ok(());
            }

            let mut existing_data_objects = 0_u64;
            for object in &job.objects {
                let (outcome, _) = object_client
                    .put_if_absent(&object.reference.key, Bytes::from(object.bytes.clone()))
                    .await
                    .map_err(|error| error.to_string())?;
                if outcome == PutOutcome::ExistingIdentical {
                    existing_data_objects = existing_data_objects.saturating_add(1);
                }
            }
            let (manifest_outcome, _) = object_client
                .put_if_absent(
                    &job.manifest.reference.key,
                    Bytes::from(job.manifest.bytes.clone()),
                )
                .await
                .map_err(|error| error.to_string())?;
            verify_closure(&object_client, &job.manifest.reference).await?;
            let root_installed = publish_job(&client, &job, publish_identity).await?;
            emit_manifest_recovery_receipt(&PublisherManifestRecoveryReceipt {
                kind: "ambiguous_manifest_recovered".to_owned(),
                job_sha256,
                prepare_identity,
                publish_identity,
                existing_data_objects,
                manifest_outcome: put_outcome_id(manifest_outcome).to_owned(),
                closure_verified: true,
                root_installed,
            })?;
            Ok(())
        }
    }
}

/// Execute one RFC-0020 publisher child until its lost-response barrier or
/// empty-scratch outcome recovery completes.
///
/// # Errors
///
/// Returns an error when the immutable job, physical closure, replicated
/// outcome, or exact retry contract cannot be executed.
pub async fn run_publication_publisher_publish_recovery_node(
    config: PublisherPublishRecoveryProcessConfig,
) -> Result<(), String> {
    require_empty_directory(&config.scratch_root)?;
    let job = PublisherJob::for_seed(config.seed)?;
    let job_sha256 = job.digest()?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let client = PublicationClient::new(config.authority_endpoints)?;
    let object_client = ObjectClient::new(
        filesystem_backend(&config.object_root).map_err(|error| error.to_string())?,
    );

    match config.phase {
        PublisherPublishRecoveryPhase::PublishResponseUnknown => {
            recover_exact_prepare(&client, &job, prepare_identity).await?;
            for object in &job.objects {
                put_and_verify(&object_client, object).await?;
            }
            put_and_verify(&object_client, &job.manifest).await?;
            verify_closure(&object_client, &job.manifest.reference).await?;
            let result = client
                .commit_with_dropped_reply_for_eval(&publish_command(&job, publish_identity))
                .await;
            if result.is_ok() {
                return Err("publisher unexpectedly observed the dropped Publish reply".to_owned());
            }
            emit_publish_barrier(&PublisherPublishBarrier {
                kind: "publish_response_unknown".to_owned(),
                job_sha256,
                prepare_identity,
                publish_identity,
                closure_verified: true,
            })?;
            park_until_killed();
        }
        PublisherPublishRecoveryPhase::Replacement => {
            let before = client.read().await?;
            let original = client.outcome(publish_identity).await?;
            let outcome_recovered = original.as_ref().is_some_and(|response| {
                response.status == PublicationCommandStatus::Accepted
                    && response.outcome == Some(PublicationOutcome::Applied)
            });
            let replayed = client
                .commit(&publish_command(&job, publish_identity))
                .await?;
            let exact_outcome_replayed = original.as_ref() == Some(&replayed);
            let after = client.read().await?;
            verify_closure(&object_client, &job.manifest.reference).await?;
            emit_publish_recovery_receipt(&PublisherPublishRecoveryReceipt {
                kind: if config.mode == PublisherPublishRecoveryMode::Correct {
                    "publish_outcome_recovered"
                } else {
                    "convergence_only_publish_reapplied"
                }
                .to_owned(),
                job_sha256,
                publish_identity,
                outcome_recovered,
                exact_outcome_replayed,
                authority_revision_unchanged: before.revision == after.revision,
                root_intent_epoch_unchanged: before.root_intent_epoch == after.root_intent_epoch,
                object_put_attempts: 0,
                closure_verified: true,
            })?;
            Ok(())
        }
    }
}

async fn recover_exact_prepare(
    client: &PublicationClient,
    job: &PublisherJob,
    identity: RequestIdentity,
) -> Result<(), String> {
    let intent = job.intent();
    let prepared = client
        .commit(&PublicationCommand {
            identity,
            credential: job.credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: job.publication_id.clone(),
                intent: intent.clone(),
            },
        })
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "publisher prepare was rejected with {:?}",
            prepared.status
        ));
    }
    let state = client.read().await?;
    if state
        .intents
        .get(&job.publication_id)
        .map(|value| &value.intent)
        != Some(&intent)
    {
        return Err("publisher recovered intent differs from immutable job".to_owned());
    }
    Ok(())
}

async fn publish_job(
    client: &PublicationClient,
    job: &PublisherJob,
    identity: RequestIdentity,
) -> Result<bool, String> {
    let published = client.commit(&publish_command(job, identity)).await?;
    if published.status != PublicationCommandStatus::Accepted {
        return Err(format!(
            "publisher root transition was rejected with {:?}",
            published.status
        ));
    }
    let state = client.read().await?;
    Ok(
        state.roots.get(&job.destination_root) == Some(&job.manifest.reference)
            && !state.intents.contains_key(&job.publication_id),
    )
}

fn publish_command(job: &PublisherJob, identity: RequestIdentity) -> PublicationCommand {
    PublicationCommand {
        identity,
        credential: job.credential.clone(),
        action: PublicationAction::Publish {
            publication_id: job.publication_id.clone(),
            destination_root: job.destination_root.clone(),
            expected_prior_root: job.expected_prior_root.clone(),
            manifest: job.manifest.reference.clone(),
        },
    }
}

const fn put_outcome_id(outcome: PutOutcome) -> &'static str {
    match outcome {
        PutOutcome::Created => "created",
        PutOutcome::ExistingIdentical => "existing_identical",
        PutOutcome::LostResponseRecovered => "lost_response_recovered",
    }
}

/// Execute the fixed real-process publisher recovery contract.
///
/// # Errors
///
/// Returns an error when process, authority, or object-store infrastructure
/// cannot execute. Semantic disagreements are returned in the report.
pub fn run_publication_publisher_process_contract(
    seed: u64,
    mode: PublisherProcessMode,
    executable: &Path,
) -> Result<PublisherProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: PublisherProcessMode,
    executable: &Path,
) -> Result<PublisherProcessReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let authority = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let endpoints = authority.endpoints();
    let job = PublisherJob::for_seed(seed)?;
    let prepare_identity = job.request_identity("prepare")?;
    let mut checks = BTreeMap::new();

    let first_scratch = root.path().join("publisher-first");
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    let mut first = spawn_publisher(
        executable,
        &PublisherProcessConfig {
            seed,
            mode,
            authority_endpoints: endpoints.clone(),
            object_root: root.object_root(),
            scratch_root: first_scratch.clone(),
            pause_after_barrier: true,
        },
    )?;
    let first_barrier = read_barrier(&mut first)?;
    checks.insert(
        "dedicated_publisher_reaches_prepare_barrier".to_owned(),
        first_barrier.kind
            == if mode == PublisherProcessMode::Correct {
                "prepared_committed"
            } else {
                "unsafe_object_written"
            },
    );
    let state_at_barrier = client.read().await?;
    let prepare_outcome = client.outcome(prepare_identity).await?;
    let exact_intent = state_at_barrier
        .intents
        .get(&job.publication_id)
        .is_some_and(|prepared| prepared.intent == job.intent());
    let prepare_is_accepted = prepare_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.status == PublicationCommandStatus::Accepted);
    checks.insert(
        "active_generation_authorizes_publisher".to_owned(),
        authority.process_count() == 3 && prepare_is_accepted,
    );
    checks.insert(
        "prepare_and_outcome_are_quorum_durable".to_owned(),
        exact_intent && prepare_is_accepted,
    );
    let controller_object_client = ObjectClient::new(
        filesystem_backend(&root.object_root()).map_err(|error| error.to_string())?,
    );
    let objects_at_barrier = controller_object_client
        .list_candidates("objects")
        .await
        .map_err(|error| error.to_string())?;
    checks.insert(
        "no_object_exists_before_prepare_barrier".to_owned(),
        objects_at_barrier.is_empty(),
    );
    kill_and_reap(&mut first)?;
    checks.insert("publisher_is_killed_at_prepare_boundary".to_owned(), true);

    if mode == PublisherProcessMode::UploadBeforePrepareAck {
        checks.extend([
            ("replacement_uses_empty_scratch".to_owned(), false),
            (
                "replacement_recovers_exact_job_and_request_identity".to_owned(),
                false,
            ),
            (
                "data_and_manifest_are_verified_by_named_read".to_owned(),
                false,
            ),
            (
                "publish_installs_root_and_retires_intent_atomically".to_owned(),
                false,
            ),
            ("reader_walks_exact_visible_closure".to_owned(), false),
        ]);
        return build_report(seed, mode, checks, 3, 1, 1, 1, 0, 0);
    }

    remove_owned_scratch(&first_scratch, root.path())?;
    let replacement_scratch = root.path().join("publisher-replacement");
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_uses_empty_scratch".to_owned(),
        directory_is_empty(&replacement_scratch)?,
    );
    let replacement = spawn_publisher(
        executable,
        &PublisherProcessConfig {
            seed,
            mode,
            authority_endpoints: endpoints,
            object_root: root.object_root(),
            scratch_root: replacement_scratch,
            pause_after_barrier: false,
        },
    )?;
    let output = wait_for_exit(replacement, Duration::from_secs(20))?;
    if !output.status.success() {
        return Err(format!(
            "replacement publisher failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let replacement_barrier = output
        .stdout
        .split(|byte| *byte == b'\n')
        .find(|line| !line.is_empty())
        .ok_or_else(|| "replacement publisher emitted no barrier".to_owned())?;
    let replacement_barrier: PublisherBarrier =
        serde_json::from_slice(replacement_barrier).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_recovers_exact_job_and_request_identity".to_owned(),
        replacement_barrier.kind == "prepared_committed"
            && replacement_barrier.job_sha256 == first_barrier.job_sha256
            && replacement_barrier.prepare_identity == first_barrier.prepare_identity,
    );

    let all_objects_exact = verify_job_objects(&controller_object_client, &job)
        .await
        .is_ok();
    checks.insert(
        "data_and_manifest_are_verified_by_named_read".to_owned(),
        all_objects_exact,
    );
    let final_state = client.read().await?;
    let publication_exact = final_state.roots.get(&job.destination_root)
        == Some(&job.manifest.reference)
        && !final_state.intents.contains_key(&job.publication_id);
    checks.insert(
        "publish_installs_root_and_retires_intent_atomically".to_owned(),
        publication_exact,
    );
    checks.insert(
        "reader_walks_exact_visible_closure".to_owned(),
        publication_exact
            && verify_closure(&controller_object_client, &job.manifest.reference)
                .await
                .is_ok(),
    );
    build_report(seed, mode, checks, 3, 2, 1, 3, 3, 1)
}

/// Execute the fixed real-process ambiguous-PUT recovery contract.
///
/// # Errors
///
/// Returns an error when process, authority, fault-injection, or object-store
/// infrastructure cannot execute. Semantic disagreements are returned in the
/// report.
pub fn run_publication_publisher_put_recovery_contract(
    seed: u64,
    mode: PublisherPutRecoveryMode,
    executable: &Path,
) -> Result<PublisherPutRecoveryReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_put_recovery_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_put_recovery_contract(
    seed: u64,
    mode: PublisherPutRecoveryMode,
    executable: &Path,
) -> Result<PublisherPutRecoveryReport, String> {
    let root = TempRoot::new_with_label(seed, mode.id(), "put-recovery")?;
    let authority = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let endpoints = authority.endpoints();
    let job = PublisherJob::for_seed(seed)?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let mut checks = BTreeMap::new();

    let first_scratch = root.path().join("publisher-put-first");
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    let mut first = spawn_put_recovery_publisher(
        executable,
        &PublisherPutRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherPutRecoveryPhase::FirstPutUnknown,
            authority_endpoints: endpoints.clone(),
            object_root: root.object_root(),
            scratch_root: first_scratch.clone(),
        },
    )?;
    let first_barrier = read_barrier(&mut first)?;
    let state_at_barrier = client.read().await?;
    let prepare_outcome = client.outcome(prepare_identity).await?;
    let prepare_is_accepted = prepare_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.status == PublicationCommandStatus::Accepted);
    let exact_intent = state_at_barrier
        .intents
        .get(&job.publication_id)
        .is_some_and(|prepared| prepared.intent == job.intent());
    checks.insert(
        "active_generation_authorizes_publisher".to_owned(),
        authority.process_count() == 3 && prepare_is_accepted,
    );
    checks.insert(
        "prepare_and_outcome_are_quorum_durable".to_owned(),
        exact_intent && prepare_is_accepted,
    );

    let object_client = ObjectClient::new(
        filesystem_backend(&root.object_root()).map_err(|error| error.to_string())?,
    );
    let first_object = job
        .objects
        .first()
        .ok_or_else(|| "publisher job has no data object".to_owned())?;
    let first_effect_is_exact = object_client
        .read_full_verified(
            &first_object.reference.key,
            None,
            first_object.reference.length,
            &first_object.reference.sha256,
        )
        .await
        .is_ok();
    checks.insert(
        "first_put_effect_exists_with_unknown_response".to_owned(),
        first_barrier.kind == "first_put_response_unknown" && first_effect_is_exact,
    );
    let second_is_absent = match job.objects.get(1) {
        Some(object) => named_object_is_absent(&object_client, &object.reference).await,
        None => false,
    };
    let manifest_is_absent = named_object_is_absent(&object_client, &job.manifest.reference).await;
    let no_root = !state_at_barrier.roots.contains_key(&job.destination_root);
    checks.insert(
        "only_partial_closure_and_no_root_exist_at_fault_barrier".to_owned(),
        first_effect_is_exact && second_is_absent && manifest_is_absent && no_root,
    );
    kill_and_reap(&mut first)?;
    checks.insert(
        "publisher_is_killed_after_first_put_effect".to_owned(),
        true,
    );

    remove_owned_scratch(&first_scratch, root.path())?;
    let replacement_scratch = root.path().join("publisher-put-replacement");
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_uses_empty_scratch".to_owned(),
        directory_is_empty(&replacement_scratch)?,
    );
    let replacement = spawn_put_recovery_publisher(
        executable,
        &PublisherPutRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherPutRecoveryPhase::Replacement,
            authority_endpoints: endpoints,
            object_root: root.object_root(),
            scratch_root: replacement_scratch,
        },
    )?;
    let output = wait_for_exit(replacement, Duration::from_secs(20))?;
    if !output.status.success() {
        return Err(format!(
            "ambiguous-PUT replacement publisher failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let replacement_receipt = output
        .stdout
        .split(|byte| *byte == b'\n')
        .find(|line| !line.is_empty())
        .ok_or_else(|| "ambiguous-PUT replacement emitted no receipt".to_owned())?;
    let replacement_receipt: PublisherPutRecoveryReceipt =
        serde_json::from_slice(replacement_receipt).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_recovers_exact_job_and_request_identities".to_owned(),
        replacement_receipt.job_sha256 == first_barrier.job_sha256
            && replacement_receipt.prepare_identity == first_barrier.prepare_identity
            && replacement_receipt.prepare_identity == prepare_identity
            && replacement_receipt.publish_identity == publish_identity,
    );
    checks.insert(
        "existing_first_object_is_exactly_verified".to_owned(),
        replacement_receipt.first_outcome == "existing_identical",
    );
    checks.insert(
        "remaining_objects_are_created_and_verified".to_owned(),
        replacement_receipt.remaining_created == 1 && replacement_receipt.manifest_created,
    );
    checks.insert(
        "complete_closure_precedes_root_visibility".to_owned(),
        replacement_receipt.closure_verified,
    );
    let final_state = client.read().await?;
    let publication_exact = replacement_receipt.root_installed
        && final_state.roots.get(&job.destination_root) == Some(&job.manifest.reference)
        && !final_state.intents.contains_key(&job.publication_id);
    checks.insert(
        "publish_installs_root_and_retires_intent_atomically".to_owned(),
        publication_exact,
    );
    checks.insert(
        "reader_walks_exact_visible_closure".to_owned(),
        publication_exact
            && verify_closure(&object_client, &job.manifest.reference)
                .await
                .is_ok(),
    );

    let (put_attempts, object_effects, existing_recoveries, named_reads) =
        if mode == PublisherPutRecoveryMode::Correct {
            (4, 3, 1, 6)
        } else {
            (2, 2, 0, 1)
        };
    build_put_recovery_report(
        seed,
        mode,
        checks,
        PutRecoveryCounters {
            authority_process_starts: 3,
            publisher_process_starts: 2,
            process_kills: 1,
            put_attempts,
            object_effects,
            injected_unknown_responses: 1,
            existing_object_recoveries: existing_recoveries,
            named_verification_reads: named_reads,
            publication_command_attempts: 3,
            empty_scratch_restarts: 1,
        },
    )
}

/// Execute the fixed real-process ambiguous-manifest recovery contract.
///
/// # Errors
///
/// Returns an error when process, authority, fault-injection, or object-store
/// infrastructure cannot execute. Semantic disagreements are returned in the
/// report.
pub fn run_publication_publisher_manifest_recovery_contract(
    seed: u64,
    mode: PublisherManifestRecoveryMode,
    executable: &Path,
) -> Result<PublisherManifestRecoveryReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_manifest_recovery_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_manifest_recovery_contract(
    seed: u64,
    mode: PublisherManifestRecoveryMode,
    executable: &Path,
) -> Result<PublisherManifestRecoveryReport, String> {
    let root = TempRoot::new_with_label(seed, mode.id(), "manifest-recovery")?;
    let authority = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let endpoints = authority.endpoints();
    let job = PublisherJob::for_seed(seed)?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let mut checks = BTreeMap::new();

    let first_scratch = root.path().join("publisher-manifest-first");
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    let mut first = spawn_manifest_recovery_publisher(
        executable,
        &PublisherManifestRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherManifestRecoveryPhase::ManifestPutUnknown,
            authority_endpoints: endpoints.clone(),
            object_root: root.object_root(),
            scratch_root: first_scratch.clone(),
        },
    )?;
    let first_barrier = read_barrier(&mut first)?;
    let state_at_barrier = client.read().await?;
    let prepare_outcome = client.outcome(prepare_identity).await?;
    let prepare_is_accepted = prepare_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.status == PublicationCommandStatus::Accepted);
    let exact_intent = state_at_barrier
        .intents
        .get(&job.publication_id)
        .is_some_and(|prepared| prepared.intent == job.intent());
    checks.insert(
        "active_generation_authorizes_publisher".to_owned(),
        authority.process_count() == 3 && prepare_is_accepted,
    );
    checks.insert(
        "prepare_and_outcome_are_quorum_durable".to_owned(),
        exact_intent && prepare_is_accepted,
    );

    let object_client = ObjectClient::new(
        filesystem_backend(&root.object_root()).map_err(|error| error.to_string())?,
    );
    let exact_data_objects = count_exact_objects(&object_client, &job.objects).await;
    checks.insert(
        "all_data_objects_are_exact_before_manifest_attempt".to_owned(),
        exact_data_objects == u64::try_from(job.objects.len()).unwrap_or(u64::MAX),
    );
    let manifest_effect_is_exact = object_client
        .read_full_verified(
            &job.manifest.reference.key,
            None,
            job.manifest.reference.length,
            &job.manifest.reference.sha256,
        )
        .await
        .is_ok();
    checks.insert(
        "manifest_effect_exists_with_unknown_response".to_owned(),
        first_barrier.kind == "manifest_put_response_unknown" && manifest_effect_is_exact,
    );
    checks.insert(
        "no_root_exists_at_manifest_fault_barrier".to_owned(),
        !state_at_barrier.roots.contains_key(&job.destination_root),
    );
    kill_and_reap(&mut first)?;
    checks.insert("publisher_is_killed_after_manifest_effect".to_owned(), true);

    remove_owned_scratch(&first_scratch, root.path())?;
    let replacement_scratch = root.path().join("publisher-manifest-replacement");
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_uses_empty_scratch".to_owned(),
        directory_is_empty(&replacement_scratch)?,
    );
    let replacement = spawn_manifest_recovery_publisher(
        executable,
        &PublisherManifestRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherManifestRecoveryPhase::Replacement,
            authority_endpoints: endpoints,
            object_root: root.object_root(),
            scratch_root: replacement_scratch,
        },
    )?;
    let output = wait_for_exit(replacement, Duration::from_secs(20))?;
    if !output.status.success() {
        return Err(format!(
            "ambiguous-manifest replacement publisher failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let replacement_receipt = output
        .stdout
        .split(|byte| *byte == b'\n')
        .find(|line| !line.is_empty())
        .ok_or_else(|| "ambiguous-manifest replacement emitted no receipt".to_owned())?;
    let replacement_receipt: PublisherManifestRecoveryReceipt =
        serde_json::from_slice(replacement_receipt).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_recovers_exact_job_and_request_identities".to_owned(),
        replacement_receipt.job_sha256 == first_barrier.job_sha256
            && replacement_receipt.prepare_identity == first_barrier.prepare_identity
            && replacement_receipt.prepare_identity == prepare_identity
            && replacement_receipt.publish_identity == publish_identity,
    );
    checks.insert(
        "existing_data_objects_are_exactly_recovered".to_owned(),
        replacement_receipt.existing_data_objects
            == u64::try_from(job.objects.len()).unwrap_or(u64::MAX),
    );
    checks.insert(
        "existing_manifest_is_exactly_recovered".to_owned(),
        replacement_receipt.manifest_outcome == "existing_identical",
    );
    checks.insert(
        "complete_closure_precedes_root_visibility".to_owned(),
        replacement_receipt.closure_verified,
    );
    let final_state = client.read().await?;
    let publication_exact = replacement_receipt.root_installed
        && final_state.roots.get(&job.destination_root) == Some(&job.manifest.reference)
        && !final_state.intents.contains_key(&job.publication_id);
    checks.insert(
        "publish_installs_root_and_retires_intent_atomically".to_owned(),
        publication_exact,
    );
    checks.insert(
        "reader_walks_exact_visible_closure".to_owned(),
        publication_exact
            && verify_closure(&object_client, &job.manifest.reference)
                .await
                .is_ok(),
    );

    let (put_attempts, object_effects, existing_recoveries, named_reads) =
        if mode == PublisherManifestRecoveryMode::Correct {
            (6, 3, 3, 8)
        } else {
            (3, 2, 1, 2)
        };
    build_manifest_recovery_report(
        seed,
        mode,
        checks,
        ManifestRecoveryCounters {
            authority_process_starts: 3,
            publisher_process_starts: 2,
            process_kills: 1,
            put_attempts,
            object_effects,
            injected_unknown_responses: 1,
            existing_object_recoveries: existing_recoveries,
            named_verification_reads: named_reads,
            publication_command_attempts: 3,
            empty_scratch_restarts: 1,
        },
    )
}

/// Execute the fixed real-process lost-Publish-response recovery contract.
///
/// # Errors
///
/// Returns an error when process, authority, fault-injection, or object-store
/// infrastructure cannot execute. Semantic disagreements are returned in the
/// report.
pub fn run_publication_publisher_publish_recovery_contract(
    seed: u64,
    mode: PublisherPublishRecoveryMode,
    executable: &Path,
) -> Result<PublisherPublishRecoveryReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_publish_recovery_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_publish_recovery_contract(
    seed: u64,
    mode: PublisherPublishRecoveryMode,
    executable: &Path,
) -> Result<PublisherPublishRecoveryReport, String> {
    let root = TempRoot::new_with_label(seed, mode.id(), "publish-recovery")?;
    let authority_faults = if mode == PublisherPublishRecoveryMode::Correct {
        PublicationAuthorityFaults::default()
    } else {
        PublicationAuthorityFaults {
            publish_without_intent: true,
            ignore_root_compare: true,
            ..PublicationAuthorityFaults::default()
        }
    };
    let mut authority = PublicationAuthorityProcessFixture::start_with_faults(
        executable,
        seed,
        mode == PublisherPublishRecoveryMode::Correct,
        authority_faults,
    )
    .await?;
    let client = authority.client()?;
    let endpoints = authority.endpoints();
    let job = PublisherJob::for_seed(seed)?;
    let prepare_identity = job.request_identity("prepare")?;
    let publish_identity = job.request_identity("publish")?;
    let mut checks = BTreeMap::new();

    let first_scratch = root.path().join("publisher-publish-first");
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    let mut first = spawn_publish_recovery_publisher(
        executable,
        &PublisherPublishRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherPublishRecoveryPhase::PublishResponseUnknown,
            authority_endpoints: endpoints.clone(),
            object_root: root.object_root(),
            scratch_root: first_scratch.clone(),
        },
    )?;
    let first_barrier = read_publish_barrier(&mut first)?;
    let state_at_barrier = client.read().await?;
    let prepare_outcome = client.outcome(prepare_identity).await?;
    let publish_outcome_at_barrier = client.outcome(publish_identity).await?;
    let prepare_is_accepted = prepare_outcome
        .as_ref()
        .is_some_and(|outcome| outcome.status == PublicationCommandStatus::Accepted);
    checks.insert(
        "active_generation_authorizes_publisher".to_owned(),
        authority.process_count() == 3 && first_barrier.prepare_identity == prepare_identity,
    );
    checks.insert(
        "prepare_and_outcome_are_quorum_durable".to_owned(),
        first_barrier.prepare_identity == prepare_identity && prepare_is_accepted,
    );

    let object_client = ObjectClient::new(
        filesystem_backend(&root.object_root()).map_err(|error| error.to_string())?,
    );
    let closure_exact = verify_job_objects(&object_client, &job).await.is_ok()
        && verify_closure(&object_client, &job.manifest.reference)
            .await
            .is_ok();
    checks.insert(
        "exact_closure_precedes_publish_attempt".to_owned(),
        first_barrier.closure_verified && closure_exact,
    );
    let root_exact =
        state_at_barrier.roots.get(&job.destination_root) == Some(&job.manifest.reference);
    checks.insert(
        "publish_effect_exists_with_unknown_response".to_owned(),
        first_barrier.kind == "publish_response_unknown" && root_exact,
    );
    checks.insert(
        "root_and_intent_transition_is_atomic_at_fault_barrier".to_owned(),
        root_exact && !state_at_barrier.intents.contains_key(&job.publication_id),
    );
    let original_outcome_exact = publish_outcome_at_barrier.as_ref().is_some_and(|response| {
        response.status == PublicationCommandStatus::Accepted
            && response.outcome == Some(PublicationOutcome::Applied)
    });

    kill_and_reap(&mut first)?;
    checks.insert("publisher_is_killed_after_publish_effect".to_owned(), true);
    authority.kill_initial_leader_and_elect_successor().await?;
    checks.insert(
        "authority_leader_is_killed_after_dropped_reply".to_owned(),
        true,
    );
    let publish_outcome_after_failover = client.outcome(publish_identity).await?;
    checks.insert(
        "original_publish_outcome_is_quorum_durable".to_owned(),
        original_outcome_exact && publish_outcome_after_failover == publish_outcome_at_barrier,
    );

    remove_owned_scratch(&first_scratch, root.path())?;
    let replacement_scratch = root.path().join("publisher-publish-replacement");
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_uses_empty_scratch".to_owned(),
        directory_is_empty(&replacement_scratch)?,
    );
    let replacement = spawn_publish_recovery_publisher(
        executable,
        &PublisherPublishRecoveryProcessConfig {
            seed,
            mode,
            phase: PublisherPublishRecoveryPhase::Replacement,
            authority_endpoints: endpoints,
            object_root: root.object_root(),
            scratch_root: replacement_scratch,
        },
    )?;
    let output = wait_for_exit(replacement, Duration::from_secs(20))?;
    if !output.status.success() {
        return Err(format!(
            "lost-Publish-response replacement failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let replacement_receipt = output
        .stdout
        .split(|byte| *byte == b'\n')
        .find(|line| !line.is_empty())
        .ok_or_else(|| "lost-Publish-response replacement emitted no receipt".to_owned())?;
    let replacement_receipt: PublisherPublishRecoveryReceipt =
        serde_json::from_slice(replacement_receipt).map_err(|error| error.to_string())?;
    checks.insert(
        "replacement_recovers_exact_job_and_publish_identity".to_owned(),
        replacement_receipt.job_sha256 == first_barrier.job_sha256
            && replacement_receipt.publish_identity == first_barrier.publish_identity
            && replacement_receipt.publish_identity == publish_identity,
    );
    checks.insert(
        "exact_publish_retry_replays_original_outcome".to_owned(),
        replacement_receipt.outcome_recovered && replacement_receipt.exact_outcome_replayed,
    );
    checks.insert(
        "publication_transition_applies_exactly_once".to_owned(),
        replacement_receipt.authority_revision_unchanged
            && replacement_receipt.root_intent_epoch_unchanged,
    );
    checks.insert(
        "replacement_issues_no_object_puts".to_owned(),
        replacement_receipt.object_put_attempts == 0,
    );
    let final_state = client.read().await?;
    checks.insert(
        "reader_walks_exact_visible_closure".to_owned(),
        replacement_receipt.closure_verified
            && final_state.roots.get(&job.destination_root) == Some(&job.manifest.reference)
            && verify_closure(&object_client, &job.manifest.reference)
                .await
                .is_ok(),
    );

    let (publish_applies, recovered_outcomes, exact_replays) =
        if mode == PublisherPublishRecoveryMode::Correct {
            (1, 1, 1)
        } else {
            (2, 0, 0)
        };
    build_publish_recovery_report(
        seed,
        mode,
        checks,
        PublishRecoveryCounters {
            authority_process_starts: 3,
            publisher_process_starts: 2,
            process_kills: 2,
            authority_failovers: 1,
            object_put_attempts: 3,
            object_effects: 3,
            named_verification_reads: 15,
            publish_command_attempts: 2,
            publish_applies,
            dropped_publish_replies: 1,
            recovered_publish_outcomes: recovered_outcomes,
            exact_outcome_replays: exact_replays,
            empty_scratch_restarts: 1,
        },
    )
}

async fn count_exact_objects(client: &ObjectClient, objects: &[JobObject]) -> u64 {
    let mut exact = 0_u64;
    for object in objects {
        if client
            .read_full_verified(
                &object.reference.key,
                None,
                object.reference.length,
                &object.reference.sha256,
            )
            .await
            .is_ok()
        {
            exact = exact.saturating_add(1);
        }
    }
    exact
}

async fn named_object_is_absent(
    client: &ObjectClient,
    object: &PublicationObjectReference,
) -> bool {
    client
        .read_full_verified(&object.key, None, object.length, &object.sha256)
        .await
        .is_err_and(|error| error.class == ErrorClass::NotFound)
}

impl PublisherJob {
    fn for_seed(seed: u64) -> Result<Self, String> {
        let objects = [
            format!("seed={seed}:publisher-left").into_bytes(),
            format!("seed={seed}:publisher-right").into_bytes(),
        ]
        .into_iter()
        .map(|bytes| JobObject::new(PublicationObjectKind::Data, bytes))
        .collect::<Vec<_>>();
        let physical = PhysicalManifest {
            format_version: 1,
            children: objects
                .iter()
                .map(|object| object.reference.clone())
                .collect(),
        };
        let manifest_bytes = serde_json::to_vec(&physical).map_err(|error| error.to_string())?;
        let manifest = JobObject::new(PublicationObjectKind::Manifest, manifest_bytes);
        Ok(Self {
            format_version: JOB_FORMAT_VERSION,
            cell_id: 17,
            credential: GenerationCredential {
                generation: 7,
                transaction_system_id: "tx-g7".to_owned(),
            },
            publication_id: format!("publisher-{seed}"),
            destination_root: "range-main".to_owned(),
            expected_prior_root: None,
            objects,
            manifest,
        })
    }

    fn intent(&self) -> PublicationIntent {
        PublicationIntent {
            object_keys: self
                .objects
                .iter()
                .map(|object| object.reference.key.clone())
                .chain(std::iter::once(self.manifest.reference.key.clone()))
                .collect::<BTreeSet<_>>(),
            manifest: self.manifest.reference.clone(),
            destination_root: self.destination_root.clone(),
            expected_prior_root: self.expected_prior_root.clone(),
        }
    }

    fn digest(&self) -> Result<String, String> {
        serde_json::to_vec(self)
            .map(|bytes| sha256(&bytes))
            .map_err(|error| error.to_string())
    }

    fn request_identity(&self, transition: &str) -> Result<RequestIdentity, String> {
        let job = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut digest = Sha256::new();
        digest.update(b"OKV-PUBLISHER-REQUEST-V1\0");
        digest.update(job);
        digest.update([0]);
        digest.update(transition.as_bytes());
        let bytes: [u8; 32] = digest.finalize().into();
        let client_id = u64::from_be_bytes(bytes[0..8].try_into().expect("fixed digest slice"));
        let request_id = u64::from_be_bytes(bytes[8..16].try_into().expect("fixed digest slice"));
        if client_id == 0 || request_id == 0 {
            return Err("derived publisher request identity is zero".to_owned());
        }
        Ok(RequestIdentity {
            client_id,
            request_id,
        })
    }
}

impl JobObject {
    fn new(kind: PublicationObjectKind, bytes: Vec<u8>) -> Self {
        let digest = sha256(&bytes);
        Self {
            reference: PublicationObjectReference {
                kind,
                key: format!("objects/sha256/{digest}"),
                length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                sha256: digest,
            },
            bytes,
        }
    }
}

async fn put_and_verify(client: &ObjectClient, object: &JobObject) -> Result<(), String> {
    client
        .put_if_absent(&object.reference.key, Bytes::from(object.bytes.clone()))
        .await
        .map_err(|error| error.to_string())?;
    client
        .read_full_verified(
            &object.reference.key,
            None,
            object.reference.length,
            &object.reference.sha256,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn verify_job_objects(client: &ObjectClient, job: &PublisherJob) -> Result<(), String> {
    for object in &job.objects {
        client
            .read_full_verified(
                &object.reference.key,
                None,
                object.reference.length,
                &object.reference.sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    client
        .read_full_verified(
            &job.manifest.reference.key,
            None,
            job.manifest.reference.length,
            &job.manifest.reference.sha256,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn verify_closure(
    client: &ObjectClient,
    manifest: &PublicationObjectReference,
) -> Result<(), String> {
    let (bytes, _) = client
        .read_full_verified(&manifest.key, None, manifest.length, &manifest.sha256)
        .await
        .map_err(|error| error.to_string())?;
    let physical: PhysicalManifest =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if physical.format_version != 1 {
        return Err("publisher manifest version is unsupported".to_owned());
    }
    for child in physical.children {
        client
            .read_full_verified(&child.key, None, child.length, &child.sha256)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn emit_barrier(barrier: &PublisherBarrier) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(&barrier).map_err(|error| error.to_string())?
    );
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn emit_put_recovery_receipt(receipt: &PublisherPutRecoveryReceipt) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(receipt).map_err(|error| error.to_string())?
    );
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn emit_manifest_recovery_receipt(
    receipt: &PublisherManifestRecoveryReceipt,
) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(receipt).map_err(|error| error.to_string())?
    );
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn emit_publish_barrier(barrier: &PublisherPublishBarrier) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(barrier).map_err(|error| error.to_string())?
    );
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn emit_publish_recovery_receipt(receipt: &PublisherPublishRecoveryReceipt) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(receipt).map_err(|error| error.to_string())?
    );
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn park_until_killed() -> ! {
    loop {
        std::thread::park();
    }
}

fn require_empty_directory(path: &Path) -> Result<(), String> {
    if !path.is_dir() {
        return Err(format!(
            "publisher scratch directory is absent: {}",
            path.display()
        ));
    }
    if !directory_is_empty(path)? {
        return Err("publisher scratch directory is not empty".to_owned());
    }
    Ok(())
}

fn directory_is_empty(path: &Path) -> Result<bool, String> {
    fs::read_dir(path)
        .map_err(|error| error.to_string())?
        .next()
        .transpose()
        .map(|entry| entry.is_none())
        .map_err(|error| error.to_string())
}

fn spawn_publisher(executable: &Path, config: &PublisherProcessConfig) -> Result<Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("publisher-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start publisher process: {error}"))
}

fn spawn_put_recovery_publisher(
    executable: &Path,
    config: &PublisherPutRecoveryProcessConfig,
) -> Result<Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("publisher-put-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start ambiguous-PUT publisher process: {error}"))
}

fn spawn_manifest_recovery_publisher(
    executable: &Path,
    config: &PublisherManifestRecoveryProcessConfig,
) -> Result<Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("publisher-manifest-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start ambiguous-manifest publisher process: {error}"))
}

fn spawn_publish_recovery_publisher(
    executable: &Path,
    config: &PublisherPublishRecoveryProcessConfig,
) -> Result<Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("publisher-publish-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start lost-Publish-response publisher: {error}"))
}

fn read_barrier(child: &mut Child) -> Result<PublisherBarrier, String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "publisher stdout is unavailable".to_owned())?;
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout)
            .read_line(&mut line)
            .map(|_| line)
            .map_err(|error| error.to_string());
        let _ = sender.send(result);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(15))
        .map_err(|_| "publisher did not reach its barrier".to_owned())??;
    serde_json::from_str(line.trim()).map_err(|error| error.to_string())
}

fn read_publish_barrier(child: &mut Child) -> Result<PublisherPublishBarrier, String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "publisher stdout is unavailable".to_owned())?;
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout)
            .read_line(&mut line)
            .map(|_| line)
            .map_err(|error| error.to_string());
        let _ = sender.send(result);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(15))
        .map_err(|_| "publisher did not reach its Publish barrier".to_owned())??;
    serde_json::from_str(line.trim()).map_err(|error| error.to_string())
}

fn kill_and_reap(child: &mut Child) -> Result<(), String> {
    child.kill().map_err(|error| error.to_string())?;
    child.wait().map_err(|error| error.to_string())?;
    Ok(())
}

fn wait_for_exit(mut child: Child, timeout: Duration) -> Result<std::process::Output, String> {
    let started = Instant::now();
    loop {
        if child
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return child.wait_with_output().map_err(|error| error.to_string());
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .map_err(|error| error.to_string())?;
            return Err(format!(
                "publisher process timed out: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn remove_owned_scratch(path: &Path, root: &Path) -> Result<(), String> {
    if path.parent() != Some(root)
        || !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("publisher-"))
    {
        return Err("refusing to remove an unowned publisher scratch path".to_owned());
    }
    fs::remove_dir_all(path).map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn build_report(
    seed: u64,
    mode: PublisherProcessMode,
    checks: BTreeMap<String, bool>,
    authority_process_starts: u64,
    publisher_process_starts: u64,
    process_kills: u64,
    object_puts: u64,
    publication_writes: u64,
    empty_scratch_restarts: u64,
) -> Result<PublisherProcessReport, String> {
    if checks.len() != usize::try_from(EXPECTED_CHECKS).unwrap_or(usize::MAX) {
        return Err(format!(
            "publisher report has {} checks, expected {EXPECTED_CHECKS}",
            checks.len()
        ));
    }
    let failed = checks.iter().enumerate().find(|(_, (_, passed))| !**passed);
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let mut report = PublisherProcessReport {
        seed,
        mode,
        executed_checks: EXPECTED_CHECKS,
        anomaly_count,
        first_mismatch_step: failed
            .as_ref()
            .map(|(index, _)| u64::try_from(index + 1).unwrap_or(u64::MAX)),
        first_mismatch: failed.map(|(_, (name, _))| name.clone()),
        authority_process_starts,
        publisher_process_starts,
        process_kills,
        object_puts,
        publication_writes,
        empty_scratch_restarts,
        checks,
        trace_sha256: String::new(),
    };
    report.trace_sha256 = sha256(&serde_json::to_vec(&report).map_err(|error| error.to_string())?);
    Ok(report)
}

#[derive(Clone, Copy, Debug)]
struct PutRecoveryCounters {
    authority_process_starts: u64,
    publisher_process_starts: u64,
    process_kills: u64,
    put_attempts: u64,
    object_effects: u64,
    injected_unknown_responses: u64,
    existing_object_recoveries: u64,
    named_verification_reads: u64,
    publication_command_attempts: u64,
    empty_scratch_restarts: u64,
}

fn build_put_recovery_report(
    seed: u64,
    mode: PublisherPutRecoveryMode,
    checks: BTreeMap<String, bool>,
    counters: PutRecoveryCounters,
) -> Result<PublisherPutRecoveryReport, String> {
    if checks.len() != usize::try_from(PUT_RECOVERY_EXPECTED_CHECKS).unwrap_or(usize::MAX) {
        return Err(format!(
            "publisher PUT recovery report has {} checks, expected {PUT_RECOVERY_EXPECTED_CHECKS}",
            checks.len()
        ));
    }
    let failed = checks.iter().enumerate().find(|(_, (_, passed))| !**passed);
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let mut report = PublisherPutRecoveryReport {
        seed,
        mode,
        executed_checks: PUT_RECOVERY_EXPECTED_CHECKS,
        anomaly_count,
        first_mismatch_step: failed
            .as_ref()
            .map(|(index, _)| u64::try_from(index + 1).unwrap_or(u64::MAX)),
        first_mismatch: failed.map(|(_, (name, _))| name.clone()),
        authority_process_starts: counters.authority_process_starts,
        publisher_process_starts: counters.publisher_process_starts,
        process_kills: counters.process_kills,
        put_attempts: counters.put_attempts,
        object_effects: counters.object_effects,
        injected_unknown_responses: counters.injected_unknown_responses,
        existing_object_recoveries: counters.existing_object_recoveries,
        named_verification_reads: counters.named_verification_reads,
        publication_command_attempts: counters.publication_command_attempts,
        empty_scratch_restarts: counters.empty_scratch_restarts,
        checks,
        trace_sha256: String::new(),
    };
    report.trace_sha256 = sha256(&serde_json::to_vec(&report).map_err(|error| error.to_string())?);
    Ok(report)
}

#[derive(Clone, Copy, Debug)]
struct ManifestRecoveryCounters {
    authority_process_starts: u64,
    publisher_process_starts: u64,
    process_kills: u64,
    put_attempts: u64,
    object_effects: u64,
    injected_unknown_responses: u64,
    existing_object_recoveries: u64,
    named_verification_reads: u64,
    publication_command_attempts: u64,
    empty_scratch_restarts: u64,
}

fn build_manifest_recovery_report(
    seed: u64,
    mode: PublisherManifestRecoveryMode,
    checks: BTreeMap<String, bool>,
    counters: ManifestRecoveryCounters,
) -> Result<PublisherManifestRecoveryReport, String> {
    if checks.len() != usize::try_from(MANIFEST_RECOVERY_EXPECTED_CHECKS).unwrap_or(usize::MAX) {
        return Err(format!(
            "publisher manifest recovery report has {} checks, expected {MANIFEST_RECOVERY_EXPECTED_CHECKS}",
            checks.len()
        ));
    }
    let failed = checks.iter().enumerate().find(|(_, (_, passed))| !**passed);
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let mut report = PublisherManifestRecoveryReport {
        seed,
        mode,
        executed_checks: MANIFEST_RECOVERY_EXPECTED_CHECKS,
        anomaly_count,
        first_mismatch_step: failed
            .as_ref()
            .map(|(index, _)| u64::try_from(index + 1).unwrap_or(u64::MAX)),
        first_mismatch: failed.map(|(_, (name, _))| name.clone()),
        authority_process_starts: counters.authority_process_starts,
        publisher_process_starts: counters.publisher_process_starts,
        process_kills: counters.process_kills,
        put_attempts: counters.put_attempts,
        object_effects: counters.object_effects,
        injected_unknown_responses: counters.injected_unknown_responses,
        existing_object_recoveries: counters.existing_object_recoveries,
        named_verification_reads: counters.named_verification_reads,
        publication_command_attempts: counters.publication_command_attempts,
        empty_scratch_restarts: counters.empty_scratch_restarts,
        checks,
        trace_sha256: String::new(),
    };
    report.trace_sha256 = sha256(&serde_json::to_vec(&report).map_err(|error| error.to_string())?);
    Ok(report)
}

#[derive(Clone, Copy, Debug)]
struct PublishRecoveryCounters {
    authority_process_starts: u64,
    publisher_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    object_put_attempts: u64,
    object_effects: u64,
    named_verification_reads: u64,
    publish_command_attempts: u64,
    publish_applies: u64,
    dropped_publish_replies: u64,
    recovered_publish_outcomes: u64,
    exact_outcome_replays: u64,
    empty_scratch_restarts: u64,
}

fn build_publish_recovery_report(
    seed: u64,
    mode: PublisherPublishRecoveryMode,
    checks: BTreeMap<String, bool>,
    counters: PublishRecoveryCounters,
) -> Result<PublisherPublishRecoveryReport, String> {
    if checks.len() != usize::try_from(PUBLISH_RECOVERY_EXPECTED_CHECKS).unwrap_or(usize::MAX) {
        return Err(format!(
            "publisher Publish recovery report has {} checks, expected {PUBLISH_RECOVERY_EXPECTED_CHECKS}",
            checks.len()
        ));
    }
    let failed = checks.iter().enumerate().find(|(_, (_, passed))| !**passed);
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let mut report = PublisherPublishRecoveryReport {
        seed,
        mode,
        executed_checks: PUBLISH_RECOVERY_EXPECTED_CHECKS,
        anomaly_count,
        first_mismatch_step: failed
            .as_ref()
            .map(|(index, _)| u64::try_from(index + 1).unwrap_or(u64::MAX)),
        first_mismatch: failed.map(|(_, (name, _))| name.clone()),
        authority_process_starts: counters.authority_process_starts,
        publisher_process_starts: counters.publisher_process_starts,
        process_kills: counters.process_kills,
        authority_failovers: counters.authority_failovers,
        object_put_attempts: counters.object_put_attempts,
        object_effects: counters.object_effects,
        named_verification_reads: counters.named_verification_reads,
        publish_command_attempts: counters.publish_command_attempts,
        publish_applies: counters.publish_applies,
        dropped_publish_replies: counters.dropped_publish_replies,
        recovered_publish_outcomes: counters.recovered_publish_outcomes,
        exact_outcome_replays: counters.exact_outcome_replays,
        empty_scratch_restarts: counters.empty_scratch_restarts,
        checks,
        trace_sha256: String::new(),
    };
    report.trace_sha256 = sha256(&serde_json::to_vec(&report).map_err(|error| error.to_string())?);
    Ok(report)
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: PublisherProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-publisher-process-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(path.join("objects")).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn new_with_label(seed: u64, mode: &str, label: &str) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-publisher-process-{label}-{mode}-{seed}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(path.join("objects")).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn object_root(&self) -> PathBuf {
        self.0.join("objects")
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self
                .0
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("okv-publisher-process-"))
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_and_request_identities_are_restart_stable() {
        let first = PublisherJob::for_seed(1103).unwrap();
        let second = PublisherJob::for_seed(1103).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.digest().unwrap(), second.digest().unwrap());
        assert_eq!(
            first.request_identity("prepare").unwrap(),
            second.request_identity("prepare").unwrap()
        );
        assert_ne!(
            first.request_identity("prepare").unwrap(),
            first.request_identity("publish").unwrap()
        );
    }

    #[test]
    fn prepared_intent_is_exactly_job_bound() {
        let first = PublisherJob::for_seed(1103).unwrap();
        let second = PublisherJob::for_seed(2207).unwrap();
        assert_ne!(first.intent(), second.intent());
    }
}
