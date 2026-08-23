use crate::{filesystem_backend, sha256, ObjectClient};
use bytes::Bytes;
use okv_consensus::{
    GenerationCredential, PublicationAction, PublicationAuthorityProcessFixture, PublicationClient,
    PublicationCommand, PublicationCommandStatus, PublicationIntent, PublicationObjectKind,
    PublicationObjectReference, RequestIdentity,
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
use std::time::{Duration, Instant};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const JOB_FORMAT_VERSION: u32 = 1;
const EXPECTED_CHECKS: u64 = 10;

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
