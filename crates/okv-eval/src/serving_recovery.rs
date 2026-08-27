//! Real-process `ServingWorker` recovery from a replicated root, immutable row
//! objects, and a quorum-reconstructed transaction-log suffix.

use okv_consensus::{
    GenerationClient, GenerationCredential, GenerationPhase, PublicationAction,
    PublicationAuthorityProcessFixture, PublicationClient, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity,
};
use okv_object::{
    content_sha256, encode_row_object_set, filesystem_backend, read_indexed_point,
    read_point_from_full_object, Backend, ObservedBackend, PointReadOutcome, RowObjectManifestV1,
    RowObjectReference, RowRecord, RowSegmentIndex, WriteCondition,
};
use okv_transaction::{
    KeyRange, Mutation, TransactionAuthority, TransactionAuthorityFaults, TransactionCommand,
    TransactionStatus,
};
use okv_wal::LocalReplicatedWal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";
const LOGICAL_WAL_ROOT: &str = "wal-g7";
const OBJECT_DURABLE_VERSION: u64 = 1;
const TARGET_VERSION: u64 = 4;
const REPLICA_COUNT: u8 = 3;
const QUORUM: usize = 2;

/// Frozen subject behavior for the G4.3 process contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServingRecoveryMode {
    /// Recover the tail and lazily fetch only the selected base block.
    Candidate,
    /// Recover the same state but hydrate every base object before reading.
    FullHydrationControl,
    /// Recover the durable bytes but omit the required tail from reads.
    SkipTailPoison,
}

impl ServingRecoveryMode {
    /// Stable mode identifier used in receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::FullHydrationControl => "full_hydration_control",
            Self::SkipTailPoison => "skip_tail_poison",
        }
    }
}

/// Fixed physical data shape for one process-recovery run.
#[derive(Clone, Debug)]
pub struct ServingRecoveryProfile {
    pub key_count: u64,
    pub value_bytes: usize,
    pub target_object_bytes: usize,
    pub target_block_bytes: usize,
}

/// One value returned across the serving-process JSON boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ServingReadOutcome {
    Value { length: u64, sha256: String },
    Tombstone,
    Absent,
}

/// One point read returned by the replacement process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServingProcessRead {
    pub key: Vec<u8>,
    pub outcome: ServingReadOutcome,
}

/// Configuration supplied to a disposable serving process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServingRecoveryProcessConfig {
    pub authority_endpoints: Vec<String>,
    pub publication_root: String,
    pub object_store_root: PathBuf,
    pub durable_log_parent: PathBuf,
    pub scratch_root: PathBuf,
    pub target_version: u64,
    pub read_keys: Vec<Vec<u8>>,
    pub mode: ServingRecoveryMode,
    pub recovered_barrier: Option<PathBuf>,
    pub hold_before_reads: bool,
}

/// Physical and semantic evidence emitted by one replacement process.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServingRecoveryProcessReport {
    pub mode: ServingRecoveryMode,
    pub scratch_was_empty: bool,
    pub generation_sandwich_stable: bool,
    pub generation: u64,
    pub transaction_system_id: String,
    pub logical_wal_root: String,
    pub manifest_authoritative: bool,
    pub object_durable_version: u64,
    pub target_version: u64,
    pub txlog_records_recovered: u64,
    pub txlog_tail_records: u64,
    pub txlog_tail_records_applied: u64,
    pub txlog_physical_bytes: u64,
    pub manifest_requests: u64,
    pub index_requests: u64,
    pub data_range_requests: u64,
    pub data_full_requests: u64,
    pub list_requests: u64,
    pub manifest_response_bytes: u64,
    pub index_response_bytes: u64,
    pub data_response_bytes: u64,
    pub total_object_response_bytes: u64,
    pub row_segment_count: u64,
    pub row_index_closure_bytes: u64,
    pub row_data_closure_bytes: u64,
    pub first_read_seconds: f64,
    pub reads: Vec<ServingProcessRead>,
}

/// End-to-end evidence for one killed worker and its replacement.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct ServingRecoveryReport {
    pub seed: u64,
    pub mode: ServingRecoveryMode,
    pub authority_processes: u64,
    pub worker_process_starts: u64,
    pub worker_process_kills: u64,
    pub empty_scratch_restarts: u64,
    pub first_correct_read_seconds: f64,
    pub correctness_anomalies: u64,
    pub exact_base_read: bool,
    pub exact_tail_update: bool,
    pub exact_tail_delete: bool,
    pub exact_tail_insert: bool,
    pub semantic_sha256: String,
    pub process: ServingRecoveryProcessReport,
}

#[derive(Clone, Debug)]
struct ExpectedHistory {
    read_keys: Vec<Vec<u8>>,
    outcomes: Vec<ServingReadOutcome>,
    commands: Vec<TransactionCommand>,
}

struct PublishedRowBase {
    segment_count: u64,
    index_closure_bytes: u64,
    data_closure_bytes: u64,
}

#[derive(Clone, Debug)]
struct OverlayMutation {
    version: u64,
    value: Option<Vec<u8>>,
}

struct OpenServingWorker {
    backend: ObservedBackend,
    manifest: RowObjectManifestV1,
    overlay: BTreeMap<Vec<u8>, Vec<OverlayMutation>>,
    hydrated: BTreeMap<String, (RowSegmentIndex, Vec<u8>)>,
    mode: ServingRecoveryMode,
    txlog_records_recovered: u64,
    txlog_tail_records: u64,
    txlog_tail_records_applied: u64,
    txlog_physical_bytes: u64,
    manifest_response_bytes: u64,
    index_requests: u64,
    data_range_requests: u64,
    data_full_requests: u64,
    index_response_bytes: u64,
    data_response_bytes: u64,
    generation: u64,
    transaction_system_id: String,
    logical_wal_root: String,
}

/// Run one real-process replacement-worker contract.
///
/// # Errors
///
/// Returns an error for invalid profile data, authority or publication failure,
/// non-durable tail records, process failure, or an invalid worker report.
pub fn run_serving_recovery_contract(
    seed: u64,
    mode: ServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    executable: &Path,
) -> Result<ServingRecoveryReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, profile, executable))
}

/// Open and run the disposable serving process selected by a hidden CLI
/// command.
///
/// # Errors
///
/// Returns an error when authoritative state cannot be reconstructed or the
/// process configuration violates the frozen empty-worker contract.
pub async fn run_serving_recovery_node(
    config: ServingRecoveryProcessConfig,
) -> Result<ServingRecoveryProcessReport, String> {
    let started = Instant::now();
    let scratch_was_empty = directory_is_empty(&config.scratch_root)?;
    if !scratch_was_empty {
        return Err("serving replacement scratch is not empty".to_owned());
    }
    if config.target_version == 0 || config.read_keys.is_empty() {
        return Err("serving replacement requires a target version and read keys".to_owned());
    }

    let mut worker = OpenServingWorker::open(&config).await?;
    if let Some(barrier) = &config.recovered_barrier {
        create_recovered_barrier(barrier)?;
    }
    if config.hold_before_reads {
        std::future::pending::<()>().await;
    }

    let mut reads = Vec::with_capacity(config.read_keys.len());
    let mut first_read_seconds = 0.0;
    for (position, key) in config.read_keys.iter().enumerate() {
        let outcome = worker.read(key, config.target_version).await?;
        if position == 0 {
            first_read_seconds = started.elapsed().as_secs_f64();
        }
        reads.push(ServingProcessRead {
            key: key.clone(),
            outcome,
        });
    }

    let stats = worker.backend.stats();
    Ok(ServingRecoveryProcessReport {
        mode: config.mode,
        scratch_was_empty,
        generation_sandwich_stable: true,
        generation: worker.generation,
        transaction_system_id: worker.transaction_system_id,
        logical_wal_root: worker.logical_wal_root,
        manifest_authoritative: true,
        object_durable_version: worker.manifest.covered_through,
        target_version: config.target_version,
        txlog_records_recovered: worker.txlog_records_recovered,
        txlog_tail_records: worker.txlog_tail_records,
        txlog_tail_records_applied: worker.txlog_tail_records_applied,
        txlog_physical_bytes: worker.txlog_physical_bytes,
        manifest_requests: 1,
        index_requests: worker.index_requests,
        data_range_requests: worker.data_range_requests,
        data_full_requests: worker.data_full_requests,
        list_requests: request_count(&stats, "list"),
        manifest_response_bytes: worker.manifest_response_bytes,
        index_response_bytes: worker.index_response_bytes,
        data_response_bytes: worker.data_response_bytes,
        total_object_response_bytes: response_bytes(&stats),
        row_segment_count: u64::try_from(worker.manifest.segments.len()).unwrap_or(u64::MAX),
        row_index_closure_bytes: worker
            .manifest
            .segments
            .iter()
            .map(|reference| reference.index_bytes)
            .sum(),
        row_data_closure_bytes: worker
            .manifest
            .segments
            .iter()
            .map(|reference| reference.data_bytes)
            .sum(),
        first_read_seconds,
        reads,
    })
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: ServingRecoveryMode,
    profile: &ServingRecoveryProfile,
    executable: &Path,
) -> Result<ServingRecoveryReport, String> {
    if !executable.is_file() {
        return Err(format!(
            "serving recovery executable does not exist: {}",
            executable.display()
        ));
    }
    let root = TempDir::new().map_err(|error| format!("create recovery fixture: {error}"))?;
    let object_store_root = root.path().join("objects");
    let durable_log_parent = root.path().join("durable-log");
    let first_scratch = root.path().join("scratch-first");
    let replacement_scratch = root.path().join("scratch-replacement");
    let recovered_barrier = root.path().join("first-worker-recovered.json");
    fs::create_dir_all(&object_store_root).map_err(|error| error.to_string())?;
    fs::create_dir_all(&durable_log_parent).map_err(|error| error.to_string())?;
    fs::create_dir_all(&first_scratch).map_err(|error| error.to_string())?;
    fs::create_dir_all(&replacement_scratch).map_err(|error| error.to_string())?;

    let authority = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let history = expected_history(seed, profile);
    let published = publish_row_base(seed, profile, &object_store_root, &client).await?;
    append_durable_history(&durable_log_parent, &history.commands)?;

    let base_config = ServingRecoveryProcessConfig {
        authority_endpoints: authority.endpoints(),
        publication_root: publication_root(seed),
        object_store_root,
        durable_log_parent,
        scratch_root: first_scratch,
        target_version: TARGET_VERSION,
        read_keys: history.read_keys.clone(),
        mode,
        recovered_barrier: Some(recovered_barrier.clone()),
        hold_before_reads: true,
    };
    let mut first = spawn_worker(executable, &base_config, false)?;
    if let Err(error) = wait_for_barrier(&mut first, &recovered_barrier) {
        let _ = first.kill();
        let _ = first.wait();
        return Err(error);
    }
    first.kill().map_err(|error| error.to_string())?;
    first.wait().map_err(|error| error.to_string())?;

    let replacement_config = ServingRecoveryProcessConfig {
        scratch_root: replacement_scratch,
        recovered_barrier: None,
        hold_before_reads: false,
        ..base_config
    };
    if !directory_is_empty(&replacement_config.scratch_root)? {
        return Err("replacement scratch was not empty before process start".to_owned());
    }
    let output = spawn_worker(executable, &replacement_config, true)?
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "replacement serving process failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let process: ServingRecoveryProcessReport = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode replacement report: {error}"))?;
    let first_correct_read_seconds = process.first_read_seconds;
    if process.row_segment_count != published.segment_count
        || process.row_index_closure_bytes != published.index_closure_bytes
        || process.row_data_closure_bytes != published.data_closure_bytes
        || !process.manifest_authoritative
        || process.generation != GENERATION
        || process.logical_wal_root != LOGICAL_WAL_ROOT
    {
        return Err("replacement report disagrees with the published closure".to_owned());
    }
    let exact = process
        .reads
        .iter()
        .zip(&history.outcomes)
        .map(|(actual, expected)| actual.outcome == *expected)
        .collect::<Vec<_>>();
    let correctness_anomalies =
        u64::try_from(exact.iter().filter(|value| !**value).count()).unwrap_or(u64::MAX);
    let stable = StableReport {
        seed,
        mode,
        authority_processes: u64::try_from(authority.process_count()).unwrap_or(u64::MAX),
        worker_process_starts: 2,
        worker_process_kills: 1,
        empty_scratch_restarts: u64::from(process.scratch_was_empty),
        correctness_anomalies,
        reads: process.reads.clone(),
        generation: process.generation,
        logical_wal_root: process.logical_wal_root.clone(),
        object_durable_version: process.object_durable_version,
        target_version: process.target_version,
        txlog_records_recovered: process.txlog_records_recovered,
        txlog_tail_records: process.txlog_tail_records,
        txlog_tail_records_applied: process.txlog_tail_records_applied,
        manifest_requests: process.manifest_requests,
        index_requests: process.index_requests,
        data_range_requests: process.data_range_requests,
        data_full_requests: process.data_full_requests,
        list_requests: process.list_requests,
    };
    let semantic_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&stable).map_err(|error| error.to_string())?)
    );
    Ok(ServingRecoveryReport {
        seed,
        mode,
        authority_processes: stable.authority_processes,
        worker_process_starts: stable.worker_process_starts,
        worker_process_kills: stable.worker_process_kills,
        empty_scratch_restarts: stable.empty_scratch_restarts,
        first_correct_read_seconds,
        correctness_anomalies,
        exact_base_read: exact.first().copied().unwrap_or(false),
        exact_tail_update: exact.get(1).copied().unwrap_or(false),
        exact_tail_delete: exact.get(2).copied().unwrap_or(false),
        exact_tail_insert: exact.get(3).copied().unwrap_or(false),
        semantic_sha256,
        process,
    })
}

#[derive(Serialize)]
struct StableReport {
    seed: u64,
    mode: ServingRecoveryMode,
    authority_processes: u64,
    worker_process_starts: u64,
    worker_process_kills: u64,
    empty_scratch_restarts: u64,
    correctness_anomalies: u64,
    reads: Vec<ServingProcessRead>,
    generation: u64,
    logical_wal_root: String,
    object_durable_version: u64,
    target_version: u64,
    txlog_records_recovered: u64,
    txlog_tail_records: u64,
    txlog_tail_records_applied: u64,
    manifest_requests: u64,
    index_requests: u64,
    data_range_requests: u64,
    data_full_requests: u64,
    list_requests: u64,
}

impl OpenServingWorker {
    #[allow(clippy::too_many_lines)]
    async fn open(config: &ServingRecoveryProcessConfig) -> Result<Self, String> {
        let generation_client = GenerationClient::new(config.authority_endpoints.clone())?;
        let publication_client = PublicationClient::new(config.authority_endpoints.clone())?;
        let generation_before = generation_client.read().await?;
        let publication = publication_client.read().await?;
        let generation_after = generation_client.read().await?;
        if generation_before != generation_after
            || generation_before.phase != GenerationPhase::Active
            || generation_before.generation == 0
        {
            return Err("generation changed around the publication-root read".to_owned());
        }
        let transaction_system_id = generation_before
            .transaction_system_id
            .clone()
            .ok_or_else(|| "active transaction-system identity is absent".to_owned())?;
        let logical_wal_root = generation_before
            .wal_root
            .clone()
            .ok_or_else(|| "active logical txLog root is absent".to_owned())?;
        let manifest_reference = publication
            .roots
            .get(&config.publication_root)
            .ok_or_else(|| "published row root is absent".to_owned())?;
        if manifest_reference.kind != PublicationObjectKind::Manifest {
            return Err("published row root is not a manifest".to_owned());
        }

        let backend = ObservedBackend::new(
            filesystem_backend(&config.object_store_root).map_err(|error| error.to_string())?,
        );
        let manifest_read = backend
            .get(&manifest_reference.key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let manifest_response_bytes = u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX);
        if manifest_response_bytes != manifest_reference.length
            || content_sha256(&manifest_read.bytes) != manifest_reference.sha256
        {
            return Err("published row manifest identity mismatch".to_owned());
        }
        let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)?;
        if manifest.generation != generation_before.generation
            || manifest.covered_through >= config.target_version
        {
            return Err("published row manifest is inadmissible for target recovery".to_owned());
        }

        let wal_path = logical_child(&config.durable_log_parent, &logical_wal_root)?;
        let recovery = LocalReplicatedWal::open(wal_path, REPLICA_COUNT, QUORUM)
            .map_err(|error| error.to_string())?
            .recover()
            .map_err(|error| error.to_string())?;
        if recovery.last_index() < config.target_version {
            return Err("quorum txLog does not cover the target version".to_owned());
        }
        let txlog_records_recovered = u64::try_from(recovery.records.len()).unwrap_or(u64::MAX);
        let txlog_tail_records = u64::try_from(
            recovery
                .records
                .iter()
                .filter(|record| {
                    manifest.covered_through < record.log_index
                        && record.log_index <= config.target_version
                })
                .count(),
        )
        .unwrap_or(u64::MAX);
        if txlog_tail_records == 0 {
            return Err("recovered txLog suffix is empty".to_owned());
        }

        let mut authority = TransactionAuthority::default();
        let mut overlay: BTreeMap<Vec<u8>, Vec<OverlayMutation>> = BTreeMap::new();
        let mut txlog_tail_records_applied = 0_u64;
        for record in recovery
            .records
            .iter()
            .filter(|record| record.log_index <= config.target_version)
        {
            let command = TransactionCommand::decode(&record.payload)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "txLog record is not a transaction command".to_owned())?;
            let applied = authority.apply(
                record.log_index,
                &command,
                TransactionAuthorityFaults::default(),
            );
            if applied.status
                != (TransactionStatus::Committed {
                    commit_version: record.log_index,
                })
            {
                return Err("txLog transaction command failed replay validation".to_owned());
            }
            if record.log_index <= manifest.covered_through {
                continue;
            }
            if config.mode == ServingRecoveryMode::SkipTailPoison {
                continue;
            }
            apply_overlay(&mut overlay, record.log_index, &command)?;
            txlog_tail_records_applied = txlog_tail_records_applied.saturating_add(1);
        }

        let mut worker = Self {
            backend,
            manifest,
            overlay,
            hydrated: BTreeMap::new(),
            mode: config.mode,
            txlog_records_recovered,
            txlog_tail_records,
            txlog_tail_records_applied,
            txlog_physical_bytes: recovery.physical_bytes,
            manifest_response_bytes,
            index_requests: 0,
            data_range_requests: 0,
            data_full_requests: 0,
            index_response_bytes: 0,
            data_response_bytes: 0,
            generation: generation_before.generation,
            transaction_system_id,
            logical_wal_root,
        };
        if config.mode == ServingRecoveryMode::FullHydrationControl {
            worker.hydrate_all().await?;
        }
        Ok(worker)
    }

    async fn hydrate_all(&mut self) -> Result<(), String> {
        for reference in &self.manifest.segments {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            let index = RowSegmentIndex::decode(&index_read.bytes)?;
            reference.validate_index(&index_read.bytes, &index)?;
            let data_read = self
                .backend
                .get(&reference.data_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            if u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX) != reference.data_bytes
                || content_sha256(&data_read.bytes) != reference.data_sha256
            {
                return Err("hydrated data object does not match the row manifest".to_owned());
            }
            self.index_requests = self.index_requests.saturating_add(1);
            self.data_full_requests = self.data_full_requests.saturating_add(1);
            self.index_response_bytes = self
                .index_response_bytes
                .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
            self.data_response_bytes = self
                .data_response_bytes
                .saturating_add(u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX));
            self.hydrated.insert(
                reference.data_key.clone(),
                (index, data_read.bytes.to_vec()),
            );
        }
        Ok(())
    }

    async fn read(&mut self, key: &[u8], version: u64) -> Result<ServingReadOutcome, String> {
        if version == 0 || version > self.manifest.covered_through.max(TARGET_VERSION) {
            return Err("serving read version is outside recovered coverage".to_owned());
        }
        if let Some(mutation) = self.overlay.get(key).and_then(|mutations| {
            mutations
                .iter()
                .rev()
                .find(|entry| entry.version <= version)
        }) {
            return Ok(mutation
                .value
                .as_ref()
                .map_or(ServingReadOutcome::Tombstone, |value| value_outcome(value)));
        }
        let Some(reference) = self.manifest.locate(key).cloned() else {
            return Ok(ServingReadOutcome::Absent);
        };
        let read_version = version.min(self.manifest.covered_through);
        let point = if self.mode == ServingRecoveryMode::FullHydrationControl {
            let (index, data) = self
                .hydrated
                .get(&reference.data_key)
                .ok_or_else(|| "selected row object was not hydrated".to_owned())?;
            read_point_from_full_object(data, index, key, read_version)?
        } else {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            let index = RowSegmentIndex::decode(&index_read.bytes)?;
            reference.validate_index(&index_read.bytes, &index)?;
            self.index_requests = self.index_requests.saturating_add(1);
            self.index_response_bytes = self
                .index_response_bytes
                .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
            let point = read_indexed_point(
                &self.backend,
                &reference.data_key,
                None,
                &index,
                key,
                read_version,
            )
            .await?;
            if point.data_bytes > 0 {
                self.data_range_requests = self.data_range_requests.saturating_add(1);
                self.data_response_bytes =
                    self.data_response_bytes.saturating_add(point.data_bytes);
            }
            point
        };
        Ok(match point.outcome {
            PointReadOutcome::Value(bytes) => value_outcome(&bytes),
            PointReadOutcome::Tombstone => ServingReadOutcome::Tombstone,
            PointReadOutcome::Absent => ServingReadOutcome::Absent,
        })
    }
}

#[allow(clippy::too_many_lines)]
async fn publish_row_base(
    seed: u64,
    profile: &ServingRecoveryProfile,
    object_store_root: &Path,
    client: &PublicationClient,
) -> Result<PublishedRowBase, String> {
    let records = (0..profile.key_count)
        .map(|key_id| {
            RowRecord::value(
                key_bytes(key_id),
                OBJECT_DURABLE_VERSION,
                base_value(seed, key_id, profile.value_bytes),
            )
        })
        .collect::<Vec<_>>();
    let encoded = encode_row_object_set(
        GENERATION,
        &records,
        profile.target_object_bytes,
        profile.target_block_bytes,
    )?;
    let mut references = Vec::with_capacity(encoded.len());
    for segment in &encoded {
        references.push(RowObjectReference::from_encoded("rows", segment)?);
    }
    let manifest =
        RowObjectManifestV1::new(GENERATION, OBJECT_DURABLE_VERSION, references.clone())?;
    let manifest_bytes = manifest.encode()?;
    let manifest_reference = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: format!("rows/manifest/sha256/{}", content_sha256(&manifest_bytes)),
        length: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
        sha256: content_sha256(&manifest_bytes),
    };
    let mut object_keys = BTreeSet::new();
    object_keys.insert(manifest_reference.key.clone());
    for reference in &references {
        object_keys.insert(reference.data_key.clone());
        object_keys.insert(reference.index_key.clone());
    }
    let publication_id = format!("serving-recovery-{seed}");
    let intent = PublicationIntent {
        object_keys,
        manifest: manifest_reference.clone(),
        destination_root: publication_root(seed),
        expected_prior_root: None,
    };
    let prepared = client
        .commit(&publication_command(
            seed,
            10,
            PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent,
            },
        ))
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err("row-base publication prepare was rejected".to_owned());
    }

    let backend = filesystem_backend(object_store_root).map_err(|error| error.to_string())?;
    for (segment, reference) in encoded.iter().zip(&references) {
        backend
            .put(
                &reference.data_key,
                segment.data.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
        backend
            .put(
                &reference.index_key,
                segment.index.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    backend
        .put(
            &manifest_reference.key,
            manifest_bytes.into(),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    let published = client
        .commit(&publication_command(
            seed,
            11,
            PublicationAction::Publish {
                publication_id,
                destination_root: publication_root(seed),
                expected_prior_root: None,
                manifest: manifest_reference.clone(),
            },
        ))
        .await?;
    if published.status != PublicationCommandStatus::Accepted
        || published.state.roots.get(&publication_root(seed)) != Some(&manifest_reference)
    {
        return Err("row-base publication did not install the exact root".to_owned());
    }
    Ok(PublishedRowBase {
        segment_count: u64::try_from(references.len()).unwrap_or(u64::MAX),
        index_closure_bytes: references
            .iter()
            .map(|reference| reference.index_bytes)
            .sum(),
        data_closure_bytes: references
            .iter()
            .map(|reference| reference.data_bytes)
            .sum(),
    })
}

fn append_durable_history(
    durable_log_parent: &Path,
    commands: &[TransactionCommand],
) -> Result<(), String> {
    let wal = LocalReplicatedWal::open(
        logical_child(durable_log_parent, LOGICAL_WAL_ROOT)?,
        REPLICA_COUNT,
        QUORUM,
    )
    .map_err(|error| error.to_string())?;
    for (offset, command) in commands.iter().enumerate() {
        let log_index = u64::try_from(offset + 1).unwrap_or(u64::MAX);
        let encoded = command.encode().map_err(|error| error.to_string())?;
        let appended = wal
            .append(log_index, &encoded, &[0, 1])
            .map_err(|error| error.to_string())?;
        if !appended.quorum_durable || appended.synced_replicas != vec![0, 1] {
            return Err(format!("txLog record {log_index} was not quorum durable"));
        }
    }
    Ok(())
}

fn expected_history(seed: u64, profile: &ServingRecoveryProfile) -> ExpectedHistory {
    let quarter = (profile.key_count / 4).max(1);
    let updated_id = 1 + seed % quarter;
    let deleted_id = updated_id + quarter;
    let untouched_id = deleted_id + quarter;
    let inserted_id = profile.key_count + 1 + seed % 31;
    let updated_key = key_bytes(updated_id);
    let deleted_key = key_bytes(deleted_id);
    let inserted_key = key_bytes(inserted_id);
    let updated = tail_value(seed, b"updated", profile.value_bytes);
    let inserted = tail_value(seed, b"inserted", profile.value_bytes);
    let commands = vec![
        TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
            mutations: Vec::new(),
        },
        point_command(
            1,
            Mutation::Set {
                key: updated_key.clone(),
                value: updated.clone(),
            },
        ),
        point_command(
            2,
            Mutation::Clear {
                key: deleted_key.clone(),
            },
        ),
        point_command(
            3,
            Mutation::Set {
                key: inserted_key.clone(),
                value: inserted.clone(),
            },
        ),
    ];
    ExpectedHistory {
        read_keys: vec![
            key_bytes(untouched_id),
            updated_key,
            deleted_key,
            inserted_key,
        ],
        outcomes: vec![
            value_outcome(&base_value(seed, untouched_id, profile.value_bytes)),
            value_outcome(&updated),
            ServingReadOutcome::Tombstone,
            value_outcome(&inserted),
        ],
        commands,
    }
}

fn point_command(read_version: u64, mutation: Mutation) -> TransactionCommand {
    let key = match &mutation {
        Mutation::Set { key, .. } | Mutation::Clear { key } => key,
        Mutation::ClearRange { .. } => unreachable!("point command cannot contain a range clear"),
    };
    TransactionCommand {
        read_version,
        read_conflicts: Vec::new(),
        write_conflicts: vec![KeyRange::point(key)],
        mutations: vec![mutation],
    }
}

fn apply_overlay(
    overlay: &mut BTreeMap<Vec<u8>, Vec<OverlayMutation>>,
    version: u64,
    command: &TransactionCommand,
) -> Result<(), String> {
    for mutation in &command.mutations {
        let (key, value) = match mutation {
            Mutation::Set { key, value } => (key, Some(value.clone())),
            Mutation::Clear { key } => (key, None),
            Mutation::ClearRange { .. } => {
                return Err("range-clear replay is outside G4.3 point-tail scope".to_owned())
            }
        };
        let versions = overlay.entry(key.clone()).or_default();
        if versions
            .last()
            .is_some_and(|prior| prior.version >= version)
        {
            return Err("tail overlay versions are not strictly increasing".to_owned());
        }
        versions.push(OverlayMutation { version, value });
    }
    Ok(())
}

fn publication_command(
    seed: u64,
    request_id: u64,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x5345_5256_494e_4752,
            request_id,
        },
        credential: GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn publication_root(seed: u64) -> String {
    format!("range/{seed}/row-root")
}

fn spawn_worker(
    executable: &Path,
    config: &ServingRecoveryProcessConfig,
    capture: bool,
) -> Result<std::process::Child, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let mut command = Command::new(executable);
    command
        .arg("serving-recovery-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null());
    if capture {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
    } else {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }
    command
        .spawn()
        .map_err(|error| format!("start serving worker: {error}"))
}

fn wait_for_barrier(child: &mut std::process::Child, barrier: &Path) -> Result<(), String> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if barrier.is_file() {
            return Ok(());
        }
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            return Err(format!(
                "first serving worker exited before recovery barrier: {status}"
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err("first serving worker did not reach the recovery barrier".to_owned())
}

fn create_recovered_barrier(path: &Path) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(b"{\"schema_version\":1,\"state\":\"recovered_before_read\"}\n")
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn directory_is_empty(path: &Path) -> Result<bool, String> {
    if !path.is_dir() {
        return Err(format!(
            "serving scratch directory does not exist: {}",
            path.display()
        ));
    }
    Ok(fs::read_dir(path)
        .map_err(|error| error.to_string())?
        .next()
        .is_none())
}

fn logical_child(parent: &Path, logical_root: &str) -> Result<PathBuf, String> {
    let mut components = Path::new(logical_root).components();
    let component = components.next();
    if !matches!(component, Some(Component::Normal(_))) || components.next().is_some() {
        return Err("logical txLog root must be one safe path component".to_owned());
    }
    Ok(parent.join(logical_root))
}

fn validate_profile(profile: &ServingRecoveryProfile) -> Result<(), String> {
    if profile.key_count < 16
        || profile.value_bytes < 16
        || profile.target_block_bytes < 4_096
        || profile.target_object_bytes < profile.target_block_bytes
    {
        return Err("invalid serving recovery profile".to_owned());
    }
    Ok(())
}

fn key_bytes(key_id: u64) -> Vec<u8> {
    key_id.to_be_bytes().to_vec()
}

fn base_value(seed: u64, key_id: u64, length: usize) -> Vec<u8> {
    deterministic_value(seed ^ key_id.rotate_left(17), b"base", length)
}

fn tail_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    deterministic_value(seed ^ 0x5441_494c_5641_4c55, domain, length)
}

fn deterministic_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut counter = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"OKV-SERVING-RECOVERY-V1\0");
        hasher.update(seed.to_be_bytes());
        hasher.update(domain);
        hasher.update(counter.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length - value.len();
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        counter = counter.saturating_add(1);
    }
    value
}

fn value_outcome(value: &[u8]) -> ServingReadOutcome {
    ServingReadOutcome::Value {
        length: u64::try_from(value.len()).unwrap_or(u64::MAX),
        sha256: format!("{:x}", Sha256::digest(value)),
    }
}

fn request_count(stats: &okv_object::RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.api == api)
        .map(|request| request.count)
        .sum()
}

fn response_bytes(stats: &okv_object::RequestStats) -> u64 {
    stats
        .requests
        .iter()
        .map(|request| request.response_bytes)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{
        apply_overlay, expected_history, logical_child, ServingReadOutcome, ServingRecoveryProfile,
    };
    use std::collections::BTreeMap;
    use std::path::Path;

    fn profile() -> ServingRecoveryProfile {
        ServingRecoveryProfile {
            key_count: 64,
            value_bytes: 32,
            target_object_bytes: 16_384,
            target_block_bytes: 4_096,
        }
    }

    #[test]
    fn held_out_history_covers_base_update_delete_and_tail_insert() {
        let history = expected_history(1103, &profile());
        assert_eq!(history.read_keys.len(), 4);
        assert!(matches!(
            history.outcomes[0],
            ServingReadOutcome::Value { .. }
        ));
        assert!(matches!(
            history.outcomes[1],
            ServingReadOutcome::Value { .. }
        ));
        assert_eq!(history.outcomes[2], ServingReadOutcome::Tombstone);
        assert!(matches!(
            history.outcomes[3],
            ServingReadOutcome::Value { .. }
        ));
        let mut overlay = BTreeMap::new();
        for (offset, command) in history.commands.iter().enumerate().skip(1) {
            apply_overlay(&mut overlay, u64::try_from(offset + 1).unwrap(), command).unwrap();
        }
        assert_eq!(overlay.len(), 3);
    }

    #[test]
    fn logical_wal_root_rejects_path_traversal() {
        assert!(logical_child(Path::new("/tmp/okv"), "wal-g7").is_ok());
        assert!(logical_child(Path::new("/tmp/okv"), "../wal-g7").is_err());
        assert!(logical_child(Path::new("/tmp/okv"), "nested/wal-g7").is_err());
    }
}
