use super::{filesystem_backend, sha256, ObjectClient};
use bytes::Bytes;
use okv_consensus::{
    run_cell_process_prototype, CellMutation, CellProcessPrototypeMode, GenerationCredential,
    PublicationAction, PublicationAuthorityProcessFixture, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity,
};
use okv_sim::CommitEnvelope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const PUBLICATION_CELL_ID: u64 = 17;
const DESTINATION_ROOT: &str = "cell-17/ranges/all";
const TRANSACTION_SYSTEM_ID: &str = "cell-process-g1";

/// Bounded unsafe behavior used to prove that closure and dual-frontier gates detect drift.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellObjectificationMode {
    Correct,
    PublishIncompleteClosure,
    TrustObjectFrontierForSafePop,
}

impl CellObjectificationMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishIncompleteClosure => "publish_incomplete_closure",
            Self::TrustObjectFrontierForSafePop => "trust_object_frontier_for_safe_pop",
        }
    }
}

/// One named semantic assertion in the objectification proof.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellObjectificationCheck {
    pub id: String,
    pub passed: bool,
}

/// Stable receipt for one transaction-to-object-to-empty-cache reconstruction run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellObjectificationReport {
    pub seed: u64,
    pub mode: CellObjectificationMode,
    pub question: String,
    pub answer: String,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub commit_frontier: u64,
    pub object_frontier: u64,
    pub authority_snapshot_frontier: u64,
    pub safe_log_pop_frontier: u64,
    pub published_root: Option<PublicationObjectReference>,
    pub closure_verified_before_publish: bool,
    pub reconstructed_rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub transaction_process_starts: u64,
    pub publication_process_starts: u64,
    pub process_kills: u64,
    pub object_puts: u64,
    pub object_reads: u64,
    pub checks: Vec<CellObjectificationCheck>,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct EnvelopeSegment {
    format_version: u32,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    from_version: u64,
    through_version: u64,
    envelopes: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CellRangeManifest {
    format_version: u32,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    range_start: Vec<u8>,
    range_end: Option<Vec<u8>>,
    covered_through: u64,
    children: Vec<PublicationObjectReference>,
}

struct TempObjectRoot(PathBuf);

impl TempObjectRoot {
    fn new(seed: u64) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "okv-cell-objectification-{seed}-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        Ok(Self(root))
    }
}

impl Drop for TempObjectRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-cell-objectification-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Execute the first end-to-end objectification proof across the transaction
/// quorum, immutable object store, publication quorum, and a fresh worker.
///
/// # Errors
///
/// Returns an error when process or storage infrastructure cannot execute the
/// bounded scenario. Semantic disagreements are retained in the report.
pub fn run_cell_objectification_contract(
    seed: u64,
    mode: CellObjectificationMode,
    executable: &Path,
) -> Result<CellObjectificationReport, String> {
    let transaction = run_cell_process_prototype(
        seed,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(objectify(seed, mode, executable, transaction))
}

#[allow(clippy::too_many_lines)]
async fn objectify(
    seed: u64,
    mode: CellObjectificationMode,
    executable: &Path,
    transaction: okv_consensus::CellProcessPrototypeReport,
) -> Result<CellObjectificationReport, String> {
    let object_root = TempObjectRoot::new(seed)?;
    let writer =
        ObjectClient::new(filesystem_backend(&object_root.0).map_err(|error| error.to_string())?);
    let final_cell = transaction.final_cell.clone().unwrap_or_default();
    let commit_frontier = final_cell.latest_sequence;
    let authority_snapshot_frontier = transaction.authority_snapshot_frontier.unwrap_or_default();
    let segment = EnvelopeSegment {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        from_version: final_cell
            .committed_envelopes
            .first()
            .and_then(|bytes| CommitEnvelope::decode(bytes).ok())
            .map_or(0, |envelope| envelope.version().sequence()),
        through_version: commit_frontier,
        envelopes: final_cell.committed_envelopes.clone(),
    };
    let segment_bytes = serde_json::to_vec(&segment).map_err(|error| error.to_string())?;
    let segment_ref = object_reference(PublicationObjectKind::Data, &segment_bytes);
    let manifest = CellRangeManifest {
        format_version: FORMAT_VERSION,
        cell_id: final_cell.cell_id,
        tenant_id: final_cell.tenant_id,
        generation: final_cell.generation,
        range_start: Vec::new(),
        range_end: None,
        covered_through: commit_frontier,
        children: vec![segment_ref.clone()],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|error| error.to_string())?;
    let manifest_ref = object_reference(PublicationObjectKind::Manifest, &manifest_bytes);

    let omit_segment = mode == CellObjectificationMode::PublishIncompleteClosure;
    let mut object_puts = 0_u64;
    if !omit_segment {
        writer
            .put_if_absent(&segment_ref.key, Bytes::from(segment_bytes))
            .await
            .map_err(|error| error.to_string())?;
        object_puts += 1;
    }
    writer
        .put_if_absent(&manifest_ref.key, Bytes::from(manifest_bytes))
        .await
        .map_err(|error| error.to_string())?;
    object_puts += 1;
    let closure_verified_before_publish = verify_closure(&writer, &manifest_ref).await.is_ok();

    let authority = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x0b1e_c71f_1ca7_10a0,
        PUBLICATION_CELL_ID,
        final_cell.generation,
        TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let publication_process_starts = authority.process_count() as u64;
    let client = authority.client()?;
    let publication_id = format!("cell-objectification-{seed}");
    let credential = GenerationCredential {
        generation: final_cell.generation,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let intent = PublicationIntent {
        object_keys: [segment_ref.key.clone(), manifest_ref.key.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        manifest: manifest_ref.clone(),
        destination_root: DESTINATION_ROOT.to_owned(),
        expected_prior_root: None,
    };
    let prepared = client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 101),
            credential: credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent,
            },
        })
        .await?;
    let published = client
        .commit(&PublicationCommand {
            identity: request_identity(seed, 102),
            credential,
            action: PublicationAction::Publish {
                publication_id,
                destination_root: DESTINATION_ROOT.to_owned(),
                expected_prior_root: None,
                manifest: manifest_ref.clone(),
            },
        })
        .await?;
    let authority_state = client.read().await?;
    let published_root = authority_state.roots.get(DESTINATION_ROOT).cloned();

    let object_frontier = if closure_verified_before_publish {
        commit_frontier
    } else {
        0
    };
    let safe_minimum = object_frontier.min(authority_snapshot_frontier);
    let safe_log_pop_frontier = if mode == CellObjectificationMode::TrustObjectFrontierForSafePop {
        object_frontier
    } else {
        safe_minimum
    };

    let fresh =
        ObjectClient::new(filesystem_backend(&object_root.0).map_err(|error| error.to_string())?);
    let fresh_root = client.read().await?.roots.get(DESTINATION_ROOT).cloned();
    let reconstruction = match &fresh_root {
        Some(root) => reconstruct_from_root(&fresh, root).await,
        None => Err("fresh worker could not resolve the published root".to_owned()),
    };
    let (reconstructed_rows, reconstructed_frontier, object_reads, chain_valid) =
        reconstruction.unwrap_or_else(|_| (Vec::new(), 0, 0, false));
    let mutation_payloads_replayable = final_cell.committed_envelopes.iter().all(|bytes| {
        CommitEnvelope::decode(bytes).is_ok_and(|envelope| {
            serde_json::from_slice::<Vec<CellMutation>>(envelope.canonical_mutations()).is_ok()
        })
    });

    let checks = vec![
        check("transaction_proof_clean", transaction.anomaly_count == 0),
        check("final_cell_available", transaction.final_cell.is_some()),
        check(
            "commit_chain_valid",
            validate_envelope_chain(&final_cell.committed_envelopes),
        ),
        check("mutation_payloads_replayable", mutation_payloads_replayable),
        check(
            "segment_is_content_addressed",
            segment_ref.key == format!("objects/sha256/{}", segment_ref.sha256),
        ),
        check(
            "closure_complete_before_publish",
            closure_verified_before_publish,
        ),
        check(
            "publication_prepared",
            prepared.status == PublicationCommandStatus::Accepted,
        ),
        check(
            "publication_committed",
            published.status == PublicationCommandStatus::Accepted,
        ),
        check(
            "published_root_exact",
            published_root == Some(manifest_ref.clone()),
        ),
        check(
            "root_does_not_imply_object_frontier",
            closure_verified_before_publish || object_frontier == 0,
        ),
        check(
            "object_frontier_reaches_commit",
            object_frontier == commit_frontier && commit_frontier != 0,
        ),
        check(
            "authority_snapshot_is_independent",
            authority_snapshot_frontier != 0 && authority_snapshot_frontier < commit_frontier,
        ),
        check(
            "safe_pop_is_dual_frontier_minimum",
            safe_log_pop_frontier == safe_minimum,
        ),
        check("fresh_worker_resolves_root", fresh_root == published_root),
        check(
            "fresh_worker_reconstructs_exact_snapshot",
            reconstructed_frontier == commit_frontier
                && reconstructed_rows == final_cell.rows
                && chain_valid,
        ),
        check(
            "separate_roles_share_generation_fence",
            publication_process_starts == 3 && final_cell.generation == 1,
        ),
    ];
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.id.clone());
    let anomaly_count = checks.iter().filter(|check| !check.passed).count() as u64;
    let mut trace = Sha256::new();
    trace.update(b"okv-cell-objectification-v0\0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(commit_frontier.to_be_bytes());
    trace.update(object_frontier.to_be_bytes());
    trace.update(authority_snapshot_frontier.to_be_bytes());
    trace.update(safe_log_pop_frontier.to_be_bytes());
    trace.update(manifest_ref.sha256.as_bytes());
    for item in &checks {
        trace.update(item.id.as_bytes());
        trace.update([u8::from(item.passed)]);
    }

    Ok(CellObjectificationReport {
        seed,
        mode,
        question: "Can a real replicated transaction result become a verified immutable object closure, advance O_cell independently of the authority snapshot, and reconstruct an exact empty-cache worker from the published root?".to_owned(),
        answer: if anomaly_count == 0 {
            "yes_within_the_bounded_cross_role_prototype"
        } else {
            "not_yet"
        }
        .to_owned(),
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch,
        commit_frontier,
        object_frontier,
        authority_snapshot_frontier,
        safe_log_pop_frontier,
        published_root,
        closure_verified_before_publish,
        reconstructed_rows,
        transaction_process_starts: transaction.process_starts,
        publication_process_starts,
        process_kills: transaction.process_kills,
        object_puts,
        object_reads,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

async fn verify_closure(
    client: &ObjectClient,
    root: &PublicationObjectReference,
) -> Result<(), String> {
    let (manifest_bytes, _) = client
        .read_full_verified(&root.key, None, root.length, &root.sha256)
        .await
        .map_err(|error| error.to_string())?;
    let manifest: CellRangeManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| error.to_string())?;
    if manifest.format_version != FORMAT_VERSION || manifest.children.is_empty() {
        return Err("cell manifest is invalid or empty".to_owned());
    }
    for child in manifest.children {
        client
            .read_full_verified(&child.key, None, child.length, &child.sha256)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn reconstruct_from_root(
    client: &ObjectClient,
    root: &PublicationObjectReference,
) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, u64, u64, bool), String> {
    let (manifest_bytes, _) = client
        .read_full_verified(&root.key, None, root.length, &root.sha256)
        .await
        .map_err(|error| error.to_string())?;
    let manifest: CellRangeManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| error.to_string())?;
    let mut rows = BTreeMap::new();
    let mut all_envelopes = Vec::new();
    let mut reads = 1_u64;
    for child in &manifest.children {
        let (segment_bytes, _) = client
            .read_full_verified(&child.key, None, child.length, &child.sha256)
            .await
            .map_err(|error| error.to_string())?;
        reads += 1;
        let segment: EnvelopeSegment =
            serde_json::from_slice(&segment_bytes).map_err(|error| error.to_string())?;
        if segment.format_version != FORMAT_VERSION
            || segment.cell_id != manifest.cell_id
            || segment.tenant_id != manifest.tenant_id
            || segment.generation != manifest.generation
            || segment.through_version != manifest.covered_through
        {
            return Err("segment identity or frontier differs from manifest".to_owned());
        }
        for encoded in &segment.envelopes {
            let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
            let mutations: Vec<CellMutation> =
                serde_json::from_slice(envelope.canonical_mutations())
                    .map_err(|error| error.to_string())?;
            for mutation in mutations {
                match mutation {
                    CellMutation::Clear { key } => {
                        rows.remove(&key);
                    }
                    CellMutation::Set { key, value } => {
                        rows.insert(key, value);
                    }
                }
            }
            all_envelopes.push(encoded.clone());
        }
    }
    let chain_valid = validate_envelope_chain(&all_envelopes);
    Ok((
        rows.into_iter().collect(),
        manifest.covered_through,
        reads,
        chain_valid,
    ))
}

fn validate_envelope_chain(envelopes: &[Vec<u8>]) -> bool {
    let mut previous = [0_u8; 32];
    for bytes in envelopes {
        let Ok(envelope) = CommitEnvelope::decode(bytes) else {
            return false;
        };
        if envelope.previous_log_chain() != previous
            || envelope.log_index() != envelope.version().sequence()
        {
            return false;
        }
        previous = Sha256::digest(bytes).into();
    }
    !envelopes.is_empty()
}

fn object_reference(kind: PublicationObjectKind, bytes: &[u8]) -> PublicationObjectReference {
    let digest = sha256(bytes);
    PublicationObjectReference {
        kind,
        key: format!("objects/sha256/{digest}"),
        length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: digest,
    }
}

fn check(id: &str, passed: bool) -> CellObjectificationCheck {
    CellObjectificationCheck {
        id: id.to_owned(),
        passed,
    }
}

const fn request_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: (seed ^ 0x6f62_6a65_6374_6966) | 1,
        request_id,
    }
}
