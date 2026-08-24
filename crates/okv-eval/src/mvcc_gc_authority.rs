//! Exact adapter from a physical `SlateDB` collection receipt to the replicated
//! objectKV publication-authority receipt.

use okv_consensus::{
    CollectionJobToken, CollectionReceipt, GenerationCredential, PublicationAction,
    PublicationApplyResponse, PublicationAuthorityProcessFixture, PublicationClient,
    PublicationCommand, PublicationCommandStatus, PublicationIntent, PublicationObjectKind,
    PublicationObjectReference, PublicationOutcome, RequestIdentity,
};
use okv_slate::{
    run_authorized_mvcc_gc_curve_worker_at_root, verify_physical_manifest_on_local_root,
    MvccGcAuthorizedCurveReceipt, MvccGcCollectionAuthorization, MvccGcCollectionRequest,
    MvccGcCurveConfig, MvccGcCurveMode, MvccGcCurveReceipt, MvccGcPhysicalObjectReceipt,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";

/// Control applied to the physical collector and authority composition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MvccGcAuthorityCompositionMode {
    Correct,
    OmitOutputSst,
    SemanticDigestAsManifest,
    SkipAuthorityFailover,
}

impl MvccGcAuthorityCompositionMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitOutputSst => "omit_output_sst",
            Self::SemanticDigestAsManifest => "semantic_digest_as_manifest",
            Self::SkipAuthorityFailover => "skip_authority_failover",
        }
    }
}

/// A mismatch between an issued collection token and physical collector output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvccGcAuthorityBindingError {
    UnsafePhysicalReceipt,
    FrozenFloorMismatch,
    InputManifestMismatch,
    OutputNamespaceMismatch,
}

/// Receipt for one collector process plus three-process authority composition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcAuthorityCompositionReport {
    pub seed: u64,
    pub mode: MvccGcAuthorityCompositionMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub authority_process_kills: u64,
    pub authority_failovers: u64,
    pub collector_process_starts: u64,
    pub collector_process_boundary: bool,
    pub serving_root_binding: bool,
    pub frozen_floor: u64,
    pub input_manifest_id: u64,
    pub output_manifest_id: u64,
    pub output_object_count: u64,
    pub final_collected_through: u64,
    pub final_root_sha256: String,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Input for one dedicated physical collector process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccGcAuthorityCollectorProcessConfig {
    pub physical: MvccGcCurveConfig,
    pub authority_endpoints: Vec<String>,
    pub object_root: PathBuf,
    pub seed: u64,
    pub output_path: PathBuf,
}

struct CollectorTempRoot(PathBuf);

impl CollectorTempRoot {
    fn new(seed: u64) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "okv-mvcc-authority-collector-{seed}-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for CollectorTempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-mvcc-authority-collector-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

impl Display for MvccGcAuthorityBindingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafePhysicalReceipt => {
                formatter.write_str("physical MVCC collector receipt failed its safety gates")
            }
            Self::FrozenFloorMismatch => {
                formatter.write_str("physical collector floor does not match the issued job")
            }
            Self::InputManifestMismatch => {
                formatter.write_str("physical collector input does not match the issued job")
            }
            Self::OutputNamespaceMismatch => {
                formatter.write_str("physical collector output escaped the reserved namespace")
            }
        }
    }
}

impl Error for MvccGcAuthorityBindingError {}

/// Convert exact physical output into an authority receipt only when every
/// issued job field owned by the collector matches.
///
/// This adapter does not prove that the job was issued before compaction began.
/// The composed process gate owns that ordering and the later authority
/// publication transition.
///
/// # Errors
///
/// Returns a bounded mismatch when the physical receipt is unsafe, its frozen
/// floor or input manifest differs from the token, or any output object escapes
/// the reserved namespace.
pub fn bind_mvcc_gc_collection_receipt(
    token: &CollectionJobToken,
    physical: &MvccGcCurveReceipt,
) -> Result<CollectionReceipt, MvccGcAuthorityBindingError> {
    if physical.anomaly_count() != 0
        || !physical.publication_completed
        || !physical.input_physical_manifest.is_valid()
        || !physical.output_physical_manifest.is_valid()
        || physical.input_physical_manifest.manifest == physical.output_physical_manifest.manifest
    {
        return Err(MvccGcAuthorityBindingError::UnsafePhysicalReceipt);
    }
    if token.frozen_floor != physical.floor_version
        || token.frozen_floor != physical.filter_floor_version
        || token.frozen_floor != physical.claimed_collected_through
    {
        return Err(MvccGcAuthorityBindingError::FrozenFloorMismatch);
    }
    if token.input_manifest != object_reference(&physical.input_physical_manifest.manifest) {
        return Err(MvccGcAuthorityBindingError::InputManifestMismatch);
    }
    let output_manifest = object_reference(&physical.output_physical_manifest.manifest);
    let object_keys = std::iter::once(output_manifest.key.clone())
        .chain(
            physical
                .output_physical_manifest
                .live_ssts
                .iter()
                .map(|object| object.key.clone()),
        )
        .collect::<BTreeSet<_>>();
    if object_keys
        .iter()
        .any(|key| !key.starts_with(&token.output_namespace))
    {
        return Err(MvccGcAuthorityBindingError::OutputNamespaceMismatch);
    }
    Ok(CollectionReceipt {
        token: token.clone(),
        output_manifest,
        object_keys,
    })
}

/// Translate one exact authority token into the storage-owned authorization
/// type consumed before compaction starts.
///
/// # Errors
///
/// Returns a mismatch when the issued token does not name the discovered input
/// manifest or frozen floor.
pub fn authorize_mvcc_gc_collection(
    token: &CollectionJobToken,
    request: &MvccGcCollectionRequest,
) -> Result<MvccGcCollectionAuthorization, MvccGcAuthorityBindingError> {
    if token.frozen_floor != request.frozen_floor {
        return Err(MvccGcAuthorityBindingError::FrozenFloorMismatch);
    }
    if token.input_manifest != object_reference(&request.input_manifest.manifest) {
        return Err(MvccGcAuthorityBindingError::InputManifestMismatch);
    }
    Ok(MvccGcCollectionAuthorization {
        job_id: token.job_id.clone(),
        owner_generation: token.owner_generation,
        authority_term: token.authority_position.term,
        authority_index: token.authority_position.index,
        frozen_floor: token.frozen_floor,
        input_manifest: request.input_manifest.manifest.clone(),
        destination_root: token.destination_root.clone(),
        range_map_epoch: token.range_map_epoch,
        expected_collected_through: token.expected_collected_through,
        output_namespace: token.output_namespace.clone(),
    })
}

/// Run one real `SlateDB` history collection after a three-process authority
/// issues its exact token, replace the authority leader, and publish the exact
/// physical output closure through the successor.
///
/// # Errors
///
/// Returns an error when the authority, physical collector, exact binder,
/// failover, or final linearizable state check cannot complete.
#[allow(clippy::too_many_lines)]
pub async fn run_mvcc_gc_authority_composition(
    seed: u64,
    mode: MvccGcAuthorityCompositionMode,
    executable: &Path,
) -> Result<MvccGcAuthorityCompositionReport, String> {
    let mut fixture = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let leader_101 = fixture.client_starting_with(101)?;
    let collector_root = CollectorTempRoot::new(seed)?;
    let object_root = collector_root.0.join("object-store");
    let output_path = collector_root.0.join("collector-receipt.json");
    let physical = run_collector_process(
        executable,
        &MvccGcAuthorityCollectorProcessConfig {
            physical: composition_config(seed),
            authority_endpoints: fixture.endpoints(),
            object_root: object_root.clone(),
            seed,
            output_path,
        },
    )?;
    verify_physical_manifest_on_local_root(
        &object_root,
        &physical.physical.input_physical_manifest,
    )
    .await?;
    verify_physical_manifest_on_local_root(
        &object_root,
        &physical.physical.output_physical_manifest,
    )
    .await?;
    let before_publish = leader_101.read().await?;
    let token = before_publish
        .collection_jobs
        .get("physical-j1")
        .cloned()
        .ok_or_else(|| "physical collector did not leave an authority job token".to_owned())?;
    let input_root_held_until_authority_publish = before_publish.roots.get("cell-root")
        == Some(&token.input_manifest)
        && before_publish.physically_collected_through == 0
        && before_publish.collection_jobs.get(&token.job_id) == Some(&token);
    let exact_receipt = bind_mvcc_gc_collection_receipt(&token, &physical.physical)
        .map_err(|error| error.to_string())?;
    let mut submitted_receipt = exact_receipt.clone();
    match mode {
        MvccGcAuthorityCompositionMode::Correct
        | MvccGcAuthorityCompositionMode::SkipAuthorityFailover => {}
        MvccGcAuthorityCompositionMode::OmitOutputSst => {
            let omitted = physical
                .physical
                .output_physical_manifest
                .live_ssts
                .first()
                .ok_or_else(|| "physical output has no live SST to omit".to_owned())?;
            submitted_receipt.object_keys.remove(&omitted.key);
        }
        MvccGcAuthorityCompositionMode::SemanticDigestAsManifest => {
            let semantic_manifest = PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "kv-runtime/manifest/semantic-only.manifest".to_owned(),
                length: 64,
                sha256: physical.physical.semantic_receipt_sha256.clone(),
            };
            submitted_receipt.output_manifest = semantic_manifest.clone();
            submitted_receipt.object_keys = BTreeSet::from([semantic_manifest.key]);
        }
    }
    let authority_failover_exercised =
        mode != MvccGcAuthorityCompositionMode::SkipAuthorityFailover;
    let publication_client = if authority_failover_exercised {
        fixture.kill_leader_and_elect_successor(101, 102).await?;
        fixture.client_starting_with(102)?
    } else {
        leader_101.clone()
    };
    let published = publication_client
        .commit(&command(
            seed,
            107,
            PublicationAction::PublishCollection {
                receipt: submitted_receipt.clone(),
            },
        ))
        .await?;
    let final_state = publication_client.read().await?;
    let checks = BTreeMap::from([
        ("collector_process_boundary_exercised".to_owned(), true),
        (
            "authority_issued_before_compaction".to_owned(),
            physical.is_valid(),
        ),
        (
            "physical_input_matches_token".to_owned(),
            token.input_manifest
                == object_reference(&physical.physical.input_physical_manifest.manifest),
        ),
        (
            "physical_output_closure_is_exact".to_owned(),
            physical.physical.output_physical_manifest.is_valid()
                && submitted_receipt == exact_receipt,
        ),
        (
            "authority_root_held_until_authority_publish".to_owned(),
            input_root_held_until_authority_publish,
        ),
        (
            "authority_bound_reads_are_exact".to_owned(),
            physical.physical.authority_bound_input_reads_exact
                && physical.physical.authority_bound_output_reads_exact,
        ),
        (
            "authority_failover_exercised".to_owned(),
            authority_failover_exercised,
        ),
        (
            "authority_published_submitted_receipt".to_owned(),
            published.status == PublicationCommandStatus::Accepted
                && published.outcome
                    == Some(PublicationOutcome::CollectionPublished {
                        receipt: submitted_receipt.clone(),
                    }),
        ),
        (
            "root_and_frontier_advance_together".to_owned(),
            final_state.roots.get("cell-root") == Some(&exact_receipt.output_manifest)
                && final_state.physically_collected_through == token.frozen_floor
                && final_state.collection_jobs.is_empty(),
        ),
        (
            "physical_floor_and_latest_reads_are_exact".to_owned(),
            physical.physical.floor_point_exact
                && physical.physical.floor_scan_exact
                && physical.physical.latest_point_exact
                && physical.physical.latest_scan_exact
                && physical.physical.close_reopen_exact,
        ),
    ]);
    build_composition_report(seed, mode, checks, &physical, &final_state)
}

/// Run one physical collector process against the replicated authority and
/// persist its exact receipt for the controller.
///
/// # Errors
///
/// Returns an error when the authority cannot be reached, authorization fails,
/// physical collection fails, the persisted closure cannot be re-read, or the
/// receipt cannot be written.
pub async fn run_mvcc_gc_authority_collector_process(
    config: MvccGcAuthorityCollectorProcessConfig,
) -> Result<(), String> {
    let client = PublicationClient::new(config.authority_endpoints.clone())?;
    let client_for_authorization = client.clone();
    let seed = config.seed;
    let physical = run_authorized_mvcc_gc_curve_worker_at_root(
        &config.physical,
        MvccGcCurveMode::Correct,
        &config.object_root,
        move |request| async move {
            prepare_collection_authorization(&client_for_authorization, seed, &request).await
        },
    )
    .await?;
    verify_physical_manifest_on_local_root(
        &config.object_root,
        &physical.physical.input_physical_manifest,
    )
    .await?;
    verify_physical_manifest_on_local_root(
        &config.object_root,
        &physical.physical.output_physical_manifest,
    )
    .await?;
    let encoded = serde_json::to_vec(&physical).map_err(|error| error.to_string())?;
    fs::write(&config.output_path, encoded).map_err(|error| error.to_string())
}

fn run_collector_process(
    executable: &Path,
    config: &MvccGcAuthorityCollectorProcessConfig,
) -> Result<MvccGcAuthorizedCurveReceipt, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("mvcc-gc-authority-collector-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("spawn physical collector process: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "physical collector process failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let encoded = fs::read(&config.output_path)
        .map_err(|error| format!("read physical collector receipt: {error}"))?;
    serde_json::from_slice(&encoded)
        .map_err(|error| format!("decode physical collector receipt: {error}"))
}

async fn prepare_collection_authorization(
    client: &PublicationClient,
    seed: u64,
    request: &MvccGcCollectionRequest,
) -> Result<MvccGcCollectionAuthorization, String> {
    let input_manifest = object_reference(&request.input_manifest.manifest);
    let input_keys = std::iter::once(input_manifest.key.clone())
        .chain(
            request
                .input_manifest
                .live_ssts
                .iter()
                .map(|object| object.key.clone()),
        )
        .collect::<BTreeSet<_>>();
    require_accepted(
        &client
            .commit(&command(
                seed,
                100,
                PublicationAction::Prepare {
                    publication_id: "physical-m0".to_owned(),
                    intent: PublicationIntent {
                        object_keys: input_keys,
                        manifest: input_manifest.clone(),
                        destination_root: "cell-root".to_owned(),
                        expected_prior_root: None,
                    },
                },
            ))
            .await?,
        "prepare physical input root",
    )?;
    require_accepted(
        &client
            .commit(&command(
                seed,
                101,
                PublicationAction::Publish {
                    publication_id: "physical-m0".to_owned(),
                    destination_root: "cell-root".to_owned(),
                    expected_prior_root: None,
                    manifest: input_manifest.clone(),
                },
            ))
            .await?,
        "publish physical input root",
    )?;
    for (request_id, action, step) in [
        (
            102,
            PublicationAction::ConfigureCollectionRoot {
                expected: None,
                destination_root: "cell-root".to_owned(),
            },
            "configure collection root",
        ),
        (
            103,
            PublicationAction::ObserveCommittedFrontier {
                committed_frontier: 16,
            },
            "observe physical commit frontier",
        ),
        (
            104,
            PublicationAction::SetRetentionWindow {
                expected_policy_epoch: 0,
                retention_window: 3,
            },
            "configure physical retention window",
        ),
        (
            105,
            PublicationAction::ObserveRangeMapEpoch { range_map_epoch: 9 },
            "observe physical range-map epoch",
        ),
    ] {
        require_accepted(
            &client.commit(&command(seed, request_id, action)).await?,
            step,
        )?;
    }
    let prepared = client
        .commit(&command(
            seed,
            106,
            PublicationAction::PrepareCollection {
                job_id: "physical-j1".to_owned(),
                frozen_floor: request.frozen_floor,
                input_manifest,
                destination_root: "cell-root".to_owned(),
                range_map_epoch: 9,
                expected_collected_through: 0,
                output_namespace: "kv-runtime/".to_owned(),
            },
        ))
        .await?;
    let token = collection_token(&prepared, "prepare physical collection")?;
    authorize_mvcc_gc_collection(&token, request).map_err(|error| error.to_string())
}

fn object_reference(object: &MvccGcPhysicalObjectReceipt) -> PublicationObjectReference {
    PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: object.key.clone(),
        length: object.length,
        sha256: object.sha256.clone(),
    }
}

fn command(seed: u64, request_id: u64, action: PublicationAction) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed.max(1),
            request_id,
        },
        credential: GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn require_accepted(response: &PublicationApplyResponse, step: &str) -> Result<(), String> {
    if response.status == PublicationCommandStatus::Accepted {
        Ok(())
    } else {
        Err(format!("{step} returned {:?}", response.status))
    }
}

fn collection_token(
    response: &PublicationApplyResponse,
    step: &str,
) -> Result<CollectionJobToken, String> {
    require_accepted(response, step)?;
    match &response.outcome {
        Some(PublicationOutcome::CollectionPrepared { token }) => Ok(token.clone()),
        other => Err(format!("{step} returned unexpected outcome {other:?}")),
    }
}

fn composition_config(seed: u64) -> MvccGcCurveConfig {
    MvccGcCurveConfig {
        history_depth: 16,
        retained_versions: 4,
        flush_stride: 2,
        key_count: 32,
        value_bytes: 64,
        seed,
        timeout_millis: 10_000,
        max_rss_bytes: 1_073_741_824,
    }
}

fn build_composition_report(
    seed: u64,
    mode: MvccGcAuthorityCompositionMode,
    checks: BTreeMap<String, bool>,
    physical: &MvccGcAuthorizedCurveReceipt,
    final_state: &okv_consensus::PublicationAuthorityState,
) -> Result<MvccGcAuthorityCompositionReport, String> {
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(check, _)| check.clone())
        .collect::<Vec<_>>();
    let final_root_sha256 = final_state
        .roots
        .get("cell-root")
        .map_or_else(String::new, |root| root.sha256.clone());
    let semantic = (
        seed,
        mode,
        &checks,
        physical.authorization.frozen_floor,
        physical.physical.input_physical_manifest.manifest_id,
        physical.physical.output_physical_manifest.manifest_id,
        physical.physical.output_physical_manifest.live_ssts.len(),
        &physical.physical.semantic_receipt_sha256,
        final_state.physically_collected_through,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(MvccGcAuthorityCompositionReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        authority_process_starts: 3,
        authority_process_kills: u64::from(
            mode != MvccGcAuthorityCompositionMode::SkipAuthorityFailover,
        ),
        authority_failovers: u64::from(
            mode != MvccGcAuthorityCompositionMode::SkipAuthorityFailover,
        ),
        collector_process_starts: 1,
        collector_process_boundary: true,
        serving_root_binding: physical.physical.authority_bound_input_reads_exact
            && physical.physical.authority_bound_output_reads_exact,
        frozen_floor: physical.authorization.frozen_floor,
        input_manifest_id: physical.physical.input_physical_manifest.manifest_id,
        output_manifest_id: physical.physical.output_physical_manifest.manifest_id,
        output_object_count: u64::try_from(
            physical.physical.output_physical_manifest.live_ssts.len() + 1,
        )
        .unwrap_or(u64::MAX),
        final_collected_through: final_state.physically_collected_through,
        final_root_sha256,
        checks,
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        authorize_mvcc_gc_collection, bind_mvcc_gc_collection_receipt, MvccGcAuthorityBindingError,
    };
    use okv_consensus::{
        CollectionJobToken, PublicationAuthorityPosition, PublicationObjectKind,
        PublicationObjectReference,
    };
    use okv_slate::{run_mvcc_gc_curve_worker, MvccGcCurveConfig, MvccGcCurveMode};

    #[tokio::test]
    async fn binds_only_the_exact_physical_job_receipt() {
        let physical = run_mvcc_gc_curve_worker(&config(), MvccGcCurveMode::Correct)
            .await
            .expect("run physical collector");
        let token = token_for(&physical);
        let request = okv_slate::MvccGcCollectionRequest {
            frozen_floor: physical.floor_version,
            input_manifest: physical.input_physical_manifest.clone(),
        };
        let authorization =
            authorize_mvcc_gc_collection(&token, &request).expect("authorize exact request");
        assert_eq!(
            authorization.authority_index,
            token.authority_position.index
        );
        let receipt = bind_mvcc_gc_collection_receipt(&token, &physical)
            .expect("bind exact physical receipt");
        assert_eq!(receipt.token, token);
        assert_eq!(
            receipt.output_manifest.key,
            physical.output_physical_manifest.manifest.key
        );
        assert_eq!(
            receipt.object_keys.len(),
            physical.output_physical_manifest.live_ssts.len() + 1
        );

        let mut wrong_floor = token.clone();
        wrong_floor.frozen_floor = wrong_floor.frozen_floor.saturating_add(1);
        assert_eq!(
            bind_mvcc_gc_collection_receipt(&wrong_floor, &physical),
            Err(MvccGcAuthorityBindingError::FrozenFloorMismatch)
        );

        let mut wrong_input = token.clone();
        wrong_input.input_manifest.sha256 = "0".repeat(64);
        assert_eq!(
            bind_mvcc_gc_collection_receipt(&wrong_input, &physical),
            Err(MvccGcAuthorityBindingError::InputManifestMismatch)
        );

        let mut wrong_namespace = token;
        wrong_namespace.output_namespace = "collections/other/".to_owned();
        assert_eq!(
            bind_mvcc_gc_collection_receipt(&wrong_namespace, &physical),
            Err(MvccGcAuthorityBindingError::OutputNamespaceMismatch)
        );
    }

    #[tokio::test]
    async fn rejects_a_collector_that_claims_collection_without_publication() {
        let physical = run_mvcc_gc_curve_worker(
            &config(),
            MvccGcCurveMode::ClaimCollectionWithoutPublication,
        )
        .await
        .expect("run unsafe physical collector");
        let token = token_for(&physical);
        assert_eq!(
            bind_mvcc_gc_collection_receipt(&token, &physical),
            Err(MvccGcAuthorityBindingError::UnsafePhysicalReceipt)
        );
    }

    fn token_for(physical: &okv_slate::MvccGcCurveReceipt) -> CollectionJobToken {
        let input = &physical.input_physical_manifest.manifest;
        CollectionJobToken {
            job_id: "j1".to_owned(),
            owner_generation: 7,
            authority_position: PublicationAuthorityPosition { term: 3, index: 9 },
            frozen_floor: physical.floor_version,
            input_manifest: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: input.key.clone(),
                length: input.length,
                sha256: input.sha256.clone(),
            },
            destination_root: "cell-root".to_owned(),
            range_map_epoch: 9,
            expected_collected_through: 0,
            output_namespace: "kv-runtime/".to_owned(),
        }
    }

    fn config() -> MvccGcCurveConfig {
        MvccGcCurveConfig {
            history_depth: 16,
            retained_versions: 4,
            flush_stride: 2,
            key_count: 32,
            value_bytes: 64,
            seed: 1103,
            timeout_millis: 10_000,
            max_rss_bytes: 1_073_741_824,
        }
    }
}
