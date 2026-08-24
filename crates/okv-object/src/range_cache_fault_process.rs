//! Process-isolated persistent-cache corruption and torn-write controls.

use crate::range_serving_view::{AuthorityBoundRangeView, AuthorityRangeRoot};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_slate::{
    inspect_latest_physical_manifest, AuthorityManifestReference, CountingStore, IoCounters,
    SlateEngine,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::Settings;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::Db;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use uuid::Uuid;

const FORMAT_VERSION: u16 = 1;
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const CACHE_PART_BYTES: usize = 64 * 1024;
const VALUE_BYTES: usize = 256 * 1024;
const TARGET_VERSION: u64 = 1;

/// One unsafe subject for the process-isolated cache-fault gate.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheFaultMode {
    /// Inject both physical faults and require exact repair or refusal.
    #[default]
    Correct,
    /// Omit the overwrite injection so the exercise gate must fail.
    SkipOverwriteInjection,
    /// Omit the torn-write injection so the exercise gate must fail.
    SkipTornWriteInjection,
    /// Unsafe receipt path that accepts a wrong value after overwrite.
    AcceptWrongValueAfterOverwrite,
    /// Unsafe receipt path that accepts a wrong value after a torn write.
    AcceptWrongValueAfterTornWrite,
}

impl RangeCacheFaultMode {
    /// Stable mode identifier used by eval receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::SkipOverwriteInjection => "skip_overwrite_injection",
            Self::SkipTornWriteInjection => "skip_torn_write_injection",
            Self::AcceptWrongValueAfterOverwrite => "accept_wrong_value_after_overwrite",
            Self::AcceptWrongValueAfterTornWrite => "accept_wrong_value_after_torn_write",
        }
    }
}

/// One physical persistent-cache fault.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCachePhysicalFault {
    Overwrite,
    TornWrite,
}

impl RangeCachePhysicalFault {
    const fn id(self) -> &'static str {
        match self {
            Self::Overwrite => "overwrite",
            Self::TornWrite => "torn_write",
        }
    }
}

/// One child-process phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheFaultWorkerPhase {
    Prepare,
    Reopen,
}

/// Configuration for one cache-fault worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheFaultWorkerConfig {
    pub phase: RangeCacheFaultWorkerPhase,
    pub fault: RangeCachePhysicalFault,
    pub object_root: PathBuf,
    pub cache_root: PathBuf,
    pub database_path: String,
    pub authority_root: Option<AuthorityRangeRoot>,
    pub seed: u64,
    pub inject_wrong_value: bool,
}

/// Stable receipt emitted by one cache-fault worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheFaultWorkerReceipt {
    pub format_version: u16,
    pub phase: RangeCacheFaultWorkerPhase,
    pub fault: RangeCachePhysicalFault,
    pub authority_root: Option<AuthorityRangeRoot>,
    pub cache_parts: u64,
    pub view_opened: bool,
    pub refused: bool,
    pub exact_value: bool,
    pub backend_read_bytes: u64,
    pub backend_get_ranges: u64,
    pub error: Option<String>,
}

/// Stable report for overwrite and torn-write cache controls.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeCacheFaultOutcome {
    ExactRepair,
    Refused,
    WrongValue,
}

/// Stable receipt for one physical cache fault.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheFaultSubjectReport {
    pub mutated_parts: u64,
    pub outcome: RangeCacheFaultOutcome,
    pub backend_read_bytes: u64,
    pub backend_get_ranges: u64,
}

impl RangeCacheFaultSubjectReport {
    /// Whether reopen refused to serve the damaged local state.
    #[must_use]
    pub const fn refused(&self) -> bool {
        matches!(self.outcome, RangeCacheFaultOutcome::Refused)
    }
}

/// Stable report for overwrite and torn-write cache controls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeCacheFaultReport {
    pub seed: u64,
    pub mode: RangeCacheFaultMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub worker_process_starts: u64,
    pub overwrite: RangeCacheFaultSubjectReport,
    pub torn_write: RangeCacheFaultSubjectReport,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: RangeCacheFaultMode) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "okv-range-cache-fault-{}-{seed}-{}",
            mode.id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self
                .0
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("okv-range-cache-fault-"))
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Execute both persistent-cache physical fault controls through fresh worker
/// processes.
///
/// # Errors
///
/// Returns an error when fixture construction, process execution, or physical
/// fault injection cannot complete. Semantic disagreements remain in the
/// report as failed checks.
pub fn run_range_cache_fault_contract(
    seed: u64,
    mode: RangeCacheFaultMode,
    executable: &Path,
) -> Result<RangeCacheFaultReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let overwrite = run_fault_subject(
        seed,
        mode,
        executable,
        &root.0,
        RangeCachePhysicalFault::Overwrite,
    )?;
    let torn = run_fault_subject(
        seed,
        mode,
        executable,
        &root.0,
        RangeCachePhysicalFault::TornWrite,
    )?;
    let checks = BTreeMap::from([
        (
            "overwrite_preparation_exact".to_owned(),
            overwrite.prepared.view_opened
                && overwrite.prepared.exact_value
                && overwrite.prepared.cache_parts > 0,
        ),
        (
            "overwrite_fault_injected".to_owned(),
            overwrite.mutated_parts == overwrite.prepared.cache_parts
                && overwrite.mutated_parts > 0,
        ),
        (
            "overwrite_never_returns_wrong_value".to_owned(),
            overwrite.reopened.refused || overwrite.reopened.exact_value,
        ),
        (
            "overwrite_repairs_from_backend_or_refuses".to_owned(),
            overwrite.reopened.refused || overwrite.reopened.backend_get_ranges > 0,
        ),
        (
            "torn_write_preparation_exact".to_owned(),
            torn.prepared.view_opened && torn.prepared.exact_value && torn.prepared.cache_parts > 0,
        ),
        (
            "torn_write_fault_injected".to_owned(),
            torn.mutated_parts == torn.prepared.cache_parts && torn.mutated_parts > 0,
        ),
        (
            "torn_write_never_returns_wrong_value".to_owned(),
            torn.reopened.refused || torn.reopened.exact_value,
        ),
        (
            "torn_write_repairs_from_backend_or_refuses".to_owned(),
            torn.reopened.refused || torn.reopened.backend_get_ranges > 0,
        ),
    ]);
    build_report(seed, mode, checks, &overwrite, &torn)
}

struct FaultSubject {
    prepared: RangeCacheFaultWorkerReceipt,
    reopened: RangeCacheFaultWorkerReceipt,
    mutated_parts: u64,
}

fn run_fault_subject(
    seed: u64,
    mode: RangeCacheFaultMode,
    executable: &Path,
    root: &Path,
    fault: RangeCachePhysicalFault,
) -> Result<FaultSubject, String> {
    let subject_root = root.join(fault.id());
    let object_root = subject_root.join("objects");
    let cache_root = subject_root.join("nvme-cache");
    fs::create_dir_all(&object_root).map_err(|error| error.to_string())?;
    let database_path = format!("range-cache-fault-{}", fault.id());
    let prepared = run_worker(
        executable,
        &RangeCacheFaultWorkerConfig {
            phase: RangeCacheFaultWorkerPhase::Prepare,
            fault,
            object_root: object_root.clone(),
            cache_root: cache_root.clone(),
            database_path: database_path.clone(),
            authority_root: None,
            seed: seed ^ fault_seed(fault),
            inject_wrong_value: false,
        },
    )?;
    let skip = matches!(
        (mode, fault),
        (
            RangeCacheFaultMode::SkipOverwriteInjection,
            RangeCachePhysicalFault::Overwrite
        ) | (
            RangeCacheFaultMode::SkipTornWriteInjection,
            RangeCachePhysicalFault::TornWrite
        )
    );
    let mutated_parts = if skip {
        0
    } else {
        mutate_cache_parts(&cache_root, fault)?
    };
    let inject_wrong_value = matches!(
        (mode, fault),
        (
            RangeCacheFaultMode::AcceptWrongValueAfterOverwrite,
            RangeCachePhysicalFault::Overwrite
        ) | (
            RangeCacheFaultMode::AcceptWrongValueAfterTornWrite,
            RangeCachePhysicalFault::TornWrite
        )
    );
    let reopened = run_worker(
        executable,
        &RangeCacheFaultWorkerConfig {
            phase: RangeCacheFaultWorkerPhase::Reopen,
            fault,
            object_root,
            cache_root,
            database_path,
            authority_root: prepared.authority_root.clone(),
            seed: seed ^ fault_seed(fault),
            inject_wrong_value,
        },
    )?;
    Ok(FaultSubject {
        prepared,
        reopened,
        mutated_parts,
    })
}

/// Run one prepare or reopen phase inside a disposable process.
///
/// # Errors
///
/// Returns an error when the object root or cache cannot be opened, or the
/// preparation fixture cannot be written. Expected reopen refusal is encoded
/// in the returned receipt.
pub async fn run_range_cache_fault_worker_process(
    config: RangeCacheFaultWorkerConfig,
) -> Result<RangeCacheFaultWorkerReceipt, String> {
    match config.phase {
        RangeCacheFaultWorkerPhase::Prepare => prepare_cache(config).await,
        RangeCacheFaultWorkerPhase::Reopen => reopen_cache(config).await,
    }
}

async fn prepare_cache(
    config: RangeCacheFaultWorkerConfig,
) -> Result<RangeCacheFaultWorkerReceipt, String> {
    let local =
        LocalFileSystem::new_with_prefix(&config.object_root).map_err(|error| error.to_string())?;
    let counters = Arc::new(IoCounters::default());
    let backend: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    let db = Db::builder(config.database_path.as_str(), Arc::clone(&backend))
        .with_settings(settings)
        .with_seed(config.seed)
        .build()
        .await
        .map_err(|error| error.to_string())?;
    let engine = SlateEngine::new(db);
    engine
        .apply(CommitBatch {
            version: Version::new(TARGET_VERSION),
            identity: CommitIdentity::for_test(config.seed.max(1)),
            mutations: vec![Mutation::Set {
                key: target_key(),
                value: expected_value(config.seed),
            }],
        })
        .await
        .map_err(|error| error.to_string())?;
    engine.flush().await.map_err(|error| error.to_string())?;
    let physical = inspect_latest_physical_manifest(
        Arc::clone(&backend),
        &config.database_path,
        config.seed ^ 0x1a11,
    )
    .await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let authority_root = AuthorityRangeRoot {
        cell_id: [0x51; 16],
        tenant_id: [0x71; 16],
        generation: 1,
        manifest: AuthorityManifestReference {
            key: physical.manifest.key,
            length: physical.manifest.length,
            sha256: physical.manifest.sha256,
        },
        covered_through: TARGET_VERSION,
        minimum_readable_version: TARGET_VERSION,
        log_chain_sha256: [0; 32],
    };
    let view = AuthorityBoundRangeView::open_with_cache(
        &config.database_path,
        cached_store(&config.cache_root, Arc::clone(&backend)).await?,
        authority_root.clone(),
        TARGET_VERSION,
        Vec::new(),
        &BTreeMap::new(),
        config.seed ^ 0x1a12,
        decoded_cache(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let expected = expected_value(config.seed);
    let point = view
        .get_at(&target_key(), TARGET_VERSION)
        .await
        .map_err(|error| error.to_string())?;
    let scan = view
        .scan_at(b"k/", b"k0", TARGET_VERSION, 1)
        .await
        .map_err(|error| error.to_string())?;
    view.close().await.map_err(|error| error.to_string())?;
    let cache_parts = count_cache_parts(&config.cache_root)?;
    let io = counters.total();
    Ok(RangeCacheFaultWorkerReceipt {
        format_version: FORMAT_VERSION,
        phase: config.phase,
        fault: config.fault,
        authority_root: Some(authority_root),
        cache_parts,
        view_opened: true,
        refused: false,
        exact_value: point == Some(expected.clone()) && scan == vec![(target_key(), expected)],
        backend_read_bytes: io.read_byte_total(),
        backend_get_ranges: successful_get_ranges(&io),
        error: None,
    })
}

async fn reopen_cache(
    config: RangeCacheFaultWorkerConfig,
) -> Result<RangeCacheFaultWorkerReceipt, String> {
    let authority_root = config
        .authority_root
        .clone()
        .ok_or_else(|| "reopen worker omitted the authority root".to_owned())?;
    let local =
        LocalFileSystem::new_with_prefix(&config.object_root).map_err(|error| error.to_string())?;
    let counters = Arc::new(IoCounters::default());
    let backend: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));
    let cache = cached_store(&config.cache_root, backend).await?;
    let opened = AuthorityBoundRangeView::open_with_cache(
        &config.database_path,
        cache,
        authority_root.clone(),
        TARGET_VERSION,
        Vec::new(),
        &BTreeMap::new(),
        config.seed ^ 0x0101,
        decoded_cache(),
    )
    .await;
    let (view_opened, refused, mut exact_value, error) = match opened {
        Err(error) => (false, true, false, Some(error.to_string())),
        Ok(view) => {
            let result = view.get_at(&target_key(), TARGET_VERSION).await;
            let close_result = view.close().await;
            match result {
                Err(error) => (true, true, false, Some(error.to_string())),
                Ok(observed) => {
                    let exact = observed == Some(expected_value(config.seed));
                    let error = close_result.err().map(|error| error.to_string());
                    (true, error.is_some(), exact, error)
                }
            }
        }
    };
    if config.inject_wrong_value && !refused {
        exact_value = false;
    }
    let cache_parts = count_cache_parts(&config.cache_root)?;
    let io = counters.total();
    Ok(RangeCacheFaultWorkerReceipt {
        format_version: FORMAT_VERSION,
        phase: config.phase,
        fault: config.fault,
        authority_root: Some(authority_root),
        cache_parts,
        view_opened,
        refused,
        exact_value,
        backend_read_bytes: io.read_byte_total(),
        backend_get_ranges: successful_get_ranges(&io),
        error,
    })
}

async fn cached_store(
    root: &Path,
    backend: Arc<dyn ObjectStore>,
) -> Result<Arc<dyn ObjectStore>, String> {
    CachedObjectStore::builder(root, backend)
        .with_max_cache_size_bytes(Some(CACHE_BYTES))
        .with_part_size_bytes(CACHE_PART_BYTES)
        .with_cache_on_flush(false)
        .with_scan_interval(None)
        .with_max_open_file_handles(16)
        .build()
        .await
        .map(|store| store as Arc<dyn ObjectStore>)
        .map_err(|error| error.to_string())
}

fn decoded_cache() -> Arc<dyn DbCache> {
    Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: u64::try_from(CACHE_BYTES).unwrap_or(u64::MAX),
        time_to_live: None,
        time_to_idle: None,
    }))
}

fn expected_value(seed: u64) -> Vec<u8> {
    let digest = Sha256::digest(seed.to_be_bytes());
    (0..VALUE_BYTES)
        .map(|index| digest[index % digest.len()])
        .collect()
}

fn target_key() -> Vec<u8> {
    b"k/target".to_vec()
}

fn fault_seed(fault: RangeCachePhysicalFault) -> u64 {
    match fault {
        RangeCachePhysicalFault::Overwrite => 0x0a11_ce01,
        RangeCachePhysicalFault::TornWrite => 0x0a11_ce02,
    }
}

fn mutate_cache_parts(root: &Path, fault: RangeCachePhysicalFault) -> Result<u64, String> {
    let mut parts = Vec::new();
    collect_cache_parts(root, &mut parts).map_err(|error| error.to_string())?;
    for part in &parts {
        let length = fs::metadata(part).map_err(|error| error.to_string())?.len();
        match fault {
            RangeCachePhysicalFault::Overwrite => {
                let length = usize::try_from(length)
                    .map_err(|_| "cache part length does not fit usize".to_owned())?;
                fs::write(part, vec![0xa5; length]).map_err(|error| error.to_string())?;
            }
            RangeCachePhysicalFault::TornWrite => {
                let file = OpenOptions::new()
                    .write(true)
                    .open(part)
                    .map_err(|error| error.to_string())?;
                file.set_len(length / 2)
                    .map_err(|error| error.to_string())?;
                file.sync_all().map_err(|error| error.to_string())?;
            }
        }
    }
    if let Some(parent) = root.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(u64::try_from(parts.len()).unwrap_or(u64::MAX))
}

fn count_cache_parts(root: &Path) -> Result<u64, String> {
    let mut parts = Vec::new();
    collect_cache_parts(root, &mut parts).map_err(|error| error.to_string())?;
    Ok(u64::try_from(parts.len()).unwrap_or(u64::MAX))
}

fn collect_cache_parts(root: &Path, parts: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cache_parts(&path, parts)?;
        } else if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("_part"))
        {
            parts.push(path);
        }
    }
    parts.sort();
    Ok(())
}

fn successful_get_ranges(io: &okv_slate::Phase0IoDelta) -> u64 {
    io.successful_requests
        .get("get_range")
        .copied()
        .unwrap_or(0)
}

fn run_worker(
    executable: &Path,
    config: &RangeCacheFaultWorkerConfig,
) -> Result<RangeCacheFaultWorkerReceipt, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("range-cache-fault-worker-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "range cache-fault worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

fn build_report(
    seed: u64,
    mode: RangeCacheFaultMode,
    checks: BTreeMap<String, bool>,
    overwrite: &FaultSubject,
    torn: &FaultSubject,
) -> Result<RangeCacheFaultReport, String> {
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        seed,
        mode,
        &checks,
        overwrite.mutated_parts,
        torn.mutated_parts,
        overwrite.reopened.refused,
        torn.reopened.refused,
        overwrite.reopened.exact_value,
        torn.reopened.exact_value,
        overwrite.reopened.backend_read_bytes,
        torn.reopened.backend_read_bytes,
        overwrite.reopened.backend_get_ranges,
        torn.reopened.backend_get_ranges,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(RangeCacheFaultReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        worker_process_starts: 4,
        overwrite: subject_report(overwrite),
        torn_write: subject_report(torn),
        checks,
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn subject_report(subject: &FaultSubject) -> RangeCacheFaultSubjectReport {
    let outcome = if subject.reopened.refused {
        RangeCacheFaultOutcome::Refused
    } else if subject.reopened.exact_value {
        RangeCacheFaultOutcome::ExactRepair
    } else {
        RangeCacheFaultOutcome::WrongValue
    };
    RangeCacheFaultSubjectReport {
        mutated_parts: subject.mutated_parts,
        outcome,
        backend_read_bytes: subject.reopened.backend_read_bytes,
        backend_get_ranges: subject.reopened.backend_get_ranges,
    }
}
