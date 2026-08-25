//! Root-bound assigned-range placement into a derived provider-free local image.

use crate::range_image::{
    root_identity_digest, write_range_image, RangeImageIdentity, RangeImageReader,
};
use crate::{
    bind_provider_physical_manifest, promote_provider_bound_persistent_range_base,
    AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord,
    PersistentRangeBaseDescriptor, ProviderBoundObjectStore, ProviderKind,
};
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_consensus::{
    sign_tagged_log_statement, tagged_log_public_key, CellLogSetMember, CellLogSetPolicy,
    CellMutation, CellTaggedLogCertificate, CellTaggedLogStatement, RequestIdentity,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const FORMAT_VERSION: u16 = 1;
const DATABASE_PATH: &str = "assigned-range-placement";
const CELL_ID: [u8; 16] = [0x51; 16];
const TENANT_ID: [u8; 16] = [0x71; 16];
const GENERATION: u64 = 1;
const ASSIGNMENT_EPOCH: u64 = 1;
const LOG_SET_ID: u16 = 10;
const PROVIDER_NAMESPACE: &str = "local-versioned-assigned-range";
const CACHE_PART_BYTES: usize = 65_536;
const DECODED_CACHE_BYTES: u64 = 1_048_576;
const READY_RECEIPT_NAME: &str = "placed-ready.json";
const RANGE_IMAGE_NAME: &str = "range-image.okv";
const LOCAL_IMAGE_FORMAT: &str = "okv-derived-sorted-range-v2";
const DEFAULT_READER_MEMORY_BYTES: usize = 4_194_304;

type RangeRow = (Vec<u8>, Vec<u8>);
type RangeRows = Vec<RangeRow>;

/// Correct derived-image subject or one deliberately unsafe placement control.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedRangePlacementMode {
    #[default]
    Correct,
    PublishBeforeVerification,
    ReuseStaleReceipt,
    CorruptLocalPart,
    AcceptProviderFallback,
}

impl AssignedRangePlacementMode {
    /// Stable telemetry identifier.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishBeforeVerification => "publish_before_verification",
            Self::ReuseStaleReceipt => "reuse_stale_receipt",
            Self::CorruptLocalPart => "corrupt_local_part",
            Self::AcceptProviderFallback => "accept_provider_fallback",
        }
    }
}

/// Frozen inputs for one assigned-range placement worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AssignedRangePlacementConfig {
    pub key_count: usize,
    pub value_bytes: usize,
    pub logical_range_count: usize,
    pub assigned_range_index: usize,
    pub tail_records: usize,
    pub point_reads: usize,
    pub apply_unrelated_pressure: bool,
    pub reopen_retained_nvme: bool,
    pub root_advance: bool,
    pub mode: AssignedRangePlacementMode,
    pub seed: u64,
    #[serde(default)]
    pub process_probe_executable: Option<PathBuf>,
}

/// Inputs passed to a fresh process that may only inspect a retained local image.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AssignedRangeImageProbeConfig {
    pub placement: AssignedRangePlacementConfig,
    pub ready_root: PathBuf,
    pub target_version: u64,
    pub expected_receipt: PlacedRangeReceipt,
}

/// Result from a provider-free retained-image process probe.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AssignedRangeImageProbeReceipt {
    pub exact_points: bool,
    pub exact_scan: bool,
    pub outside_range_refused: bool,
    pub local_image_digest: String,
    pub point_p99_seconds: f64,
    pub scan_rows_per_second: f64,
    pub scan_rows: u64,
}

/// Exact local publication identity for one assigned range.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PlacedRangeReceipt {
    pub format_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub range_begin: Vec<u8>,
    pub range_end: Vec<u8>,
    pub assignment_epoch: u64,
    pub authority_generation: u64,
    pub authority_manifest_identity: String,
    pub provider_closure_digest: String,
    pub target_version: u64,
    pub final_log_chain_sha256: String,
    pub local_image_format: String,
    pub local_image_digest: String,
    pub logical_row_count: u64,
    pub logical_assigned_bytes: u64,
    pub placed_bytes: u64,
    pub placement_amplification: f64,
    pub hydration_provider_requests: u64,
    pub hydration_provider_bytes: u64,
    pub hydration_duration_seconds: f64,
    pub oracle_digest: String,
    pub published_at_unix_millis: u64,
}

/// Complete worker result for the assigned-range placement gate.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AssignedRangePlacementReceipt {
    pub format_version: u16,
    pub mode: AssignedRangePlacementMode,
    pub seed: u64,
    pub key_count: usize,
    pub value_bytes: usize,
    pub logical_range_count: usize,
    pub assigned_range_index: usize,
    pub tail_records: usize,
    pub point_reads: usize,
    pub process_reopen_requested: bool,
    pub process_reopen_executed: bool,
    pub root_advance_requested: bool,
    pub placed: PlacedRangeReceipt,
    pub hydration_throughput_bytes_per_second: f64,
    pub projected_one_copy_bytes: u64,
    pub projected_two_copy_bytes: u64,
    pub post_ready_provider_requests: u64,
    pub post_ready_provider_bytes: u64,
    pub post_ready_point_p99_seconds: f64,
    pub post_ready_scan_rows_per_second: f64,
    pub post_ready_scan_rows: u64,
    pub verification_complete: bool,
    pub exact_points: bool,
    pub exact_scan: bool,
    pub outside_range_refused: bool,
    pub ready_publication_atomic: bool,
    pub ready_receipt_exact: bool,
    pub local_image_digest_stable: bool,
    pub old_ready_refused_after_advance: bool,
    pub unsafe_provider_fallback_accepted: bool,
    pub corruption_detected: bool,
    pub scratch_cleanup_complete: bool,
    pub semantic_receipt_sha256: String,
}

struct GuardedScratchRoot(PathBuf);

impl GuardedScratchRoot {
    fn new(seed: u64) -> Result<Self, String> {
        tempfile::Builder::new()
            .prefix(&format!("okv-assigned-range-placement-{seed}-"))
            .tempdir()
            .map_err(|error| error.to_string())
            .map(|root| Self(root.keep()))
    }

    fn cleanup(&self) -> Result<bool, String> {
        if !self.0.starts_with(std::env::temp_dir())
            || !self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-assigned-range-placement-")
            })
        {
            return Err("refuse cleanup outside guarded assigned-range scratch".to_owned());
        }
        if self.0.exists() {
            fs::remove_dir_all(&self.0).map_err(|error| error.to_string())?;
        }
        Ok(!self.0.exists())
    }
}

impl Drop for GuardedScratchRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-assigned-range-placement-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Open and exhaustively verify a retained derived image without constructing
/// an object-store client.
///
/// # Errors
///
/// Returns an error when the ready receipt, image digest, deterministic
/// fixture, point reads, or complete scan disagree.
pub fn run_assigned_range_image_probe(
    config: &AssignedRangeImageProbeConfig,
) -> Result<AssignedRangeImageProbeReceipt, String> {
    validate_config(&config.placement)?;
    let (range_begin, range_end, assigned_first, assigned_end) =
        assignment_bounds(&config.placement);
    let expected = expected_rows_at_target(&config.placement, config.target_version)?;
    let (reader, receipt) = open_range_image(
        &config.ready_root,
        ASSIGNMENT_EPOCH,
        config.target_version,
        DEFAULT_READER_MEMORY_BYTES,
    )?;
    if receipt != config.expected_receipt
        || receipt.range_begin != range_begin
        || receipt.range_end != range_end
        || receipt.oracle_digest != rows_digest(&expected)
        || receipt.logical_row_count != u64::try_from(expected.len()).unwrap_or(u64::MAX)
    {
        return Err("retained range image does not match independent oracle identity".to_owned());
    }

    let mut exact_points = true;
    let mut point_seconds = Vec::with_capacity(config.placement.point_reads);
    for ordinal in point_order(&config.placement, assigned_first, assigned_end) {
        let key = key_for(ordinal);
        let started = Instant::now();
        let observed = reader.get(&key)?;
        point_seconds.push(started.elapsed().as_secs_f64());
        exact_points &= observed.as_ref() == expected_value(&expected, &key);
    }
    point_seconds.sort_by(f64::total_cmp);

    let scan_started = Instant::now();
    let observed = reader.scan(&range_begin, &range_end, expected.len())?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_rows = u64::try_from(observed.len()).unwrap_or(u64::MAX);
    let scan_rows_per_second = f64::from(u32::try_from(observed.len()).unwrap_or(u32::MAX))
        / scan_seconds.max(f64::EPSILON);
    let outside_range_refused = reader.get(&outside_key(&config.placement)).is_err();

    Ok(AssignedRangeImageProbeReceipt {
        exact_points,
        exact_scan: observed == expected,
        outside_range_refused,
        local_image_digest: receipt.local_image_digest,
        point_p99_seconds: percentile_or_zero(&point_seconds, 99),
        scan_rows_per_second,
        scan_rows,
    })
}

fn write_new_range_image(
    root: &Path,
    range_begin: &[u8],
    range_end: &[u8],
    target_version: u64,
    rows: &[RangeRow],
    root_identity: [u8; 32],
) -> Result<String, String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let path = root.join(RANGE_IMAGE_NAME);
    let receipt = write_range_image(
        &path,
        &RangeImageIdentity {
            target_version,
            range_begin,
            range_end,
            row_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
            root_identity_digest: root_identity,
            image_identity_sha256: None,
        },
        rows,
    )?;
    sync_parent(&path)?;
    Ok(receipt.image_identity_sha256)
}

fn replace_range_image(
    root: &Path,
    range_begin: &[u8],
    range_end: &[u8],
    target_version: u64,
    rows: &[RangeRow],
    root_identity: [u8; 32],
) -> Result<String, String> {
    let temporary = root.join("range-image.next");
    let path = root.join(RANGE_IMAGE_NAME);
    let receipt = write_range_image(
        &temporary,
        &RangeImageIdentity {
            target_version,
            range_begin,
            range_end,
            row_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
            root_identity_digest: root_identity,
            image_identity_sha256: None,
        },
        rows,
    )?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    sync_parent(&path)?;
    Ok(receipt.image_identity_sha256)
}

fn open_range_image(
    root: &Path,
    assignment_epoch: u64,
    target_version: u64,
    memory_budget_bytes: usize,
) -> Result<(RangeImageReader, PlacedRangeReceipt), String> {
    let receipt: PlacedRangeReceipt = serde_json::from_slice(
        &fs::read(root.join(READY_RECEIPT_NAME)).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    if receipt.format_version != FORMAT_VERSION
        || receipt.cell_id != CELL_ID
        || receipt.tenant_id != TENANT_ID
        || receipt.local_image_format != LOCAL_IMAGE_FORMAT
        || receipt.assignment_epoch != assignment_epoch
        || receipt.target_version != target_version
    {
        return Err("placed range receipt does not authorize requested assignment".to_owned());
    }
    let root_identity = placed_root_identity(&receipt);
    let (reader, _) = RangeImageReader::open(
        &root.join(RANGE_IMAGE_NAME),
        &RangeImageIdentity {
            target_version,
            range_begin: &receipt.range_begin,
            range_end: &receipt.range_end,
            row_count: receipt.logical_row_count,
            root_identity_digest: root_identity,
            image_identity_sha256: Some(&receipt.local_image_digest),
        },
        memory_budget_bytes,
    )?;
    Ok((reader, receipt))
}

fn expected_rows_at_target(
    config: &AssignedRangePlacementConfig,
    target_version: u64,
) -> Result<RangeRows, String> {
    let (_, mut oracle) = base_fixture(config);
    for tail_index in 0..config.tail_records {
        let sequence = u64::try_from(tail_index)
            .unwrap_or(u64::MAX)
            .saturating_add(2);
        let ordinal = tail_ordinal(config.seed, tail_index, config.key_count);
        oracle.insert(key_for(ordinal), value_for(config, ordinal, sequence));
    }
    let base_target = u64::try_from(config.tail_records)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    if target_version == base_target.saturating_add(1) && config.root_advance {
        let (_, _, assigned_first, assigned_end) = assignment_bounds(config);
        let ordinal = assigned_first.min(assigned_end.saturating_sub(1));
        oracle.insert(key_for(ordinal), value_for(config, ordinal, target_version));
    } else if target_version != base_target {
        return Err("range image probe target is not derivable from frozen fixture".to_owned());
    }
    let (range_begin, range_end, _, _) = assignment_bounds(config);
    Ok(expected_range(&oracle, &range_begin, &range_end))
}

fn expected_value<'a>(rows: &'a [RangeRow], key: &[u8]) -> Option<&'a Vec<u8>> {
    rows.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
        .ok()
        .map(|index| &rows[index].1)
}

fn outside_key(config: &AssignedRangePlacementConfig) -> Vec<u8> {
    let (_, _, assigned_first, assigned_end) = assignment_bounds(config);
    if config.logical_range_count == 1 {
        b"outside-assigned-range".to_vec()
    } else if config.assigned_range_index == 0 {
        key_for(assigned_end.min(config.key_count.saturating_sub(1)))
    } else {
        key_for(assigned_first.saturating_sub(1))
    }
}

fn run_image_probe_child(
    executable: &Path,
    config: &AssignedRangeImageProbeConfig,
) -> Result<AssignedRangeImageProbeReceipt, String> {
    let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("assigned-range-image-probe")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "assigned-range image probe failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

/// Execute the derived immutable-image candidate for one assigned logical range.
///
/// # Errors
///
/// Returns an error when fixture construction, authenticated hydration, local
/// publication, or guarded cleanup cannot complete. Contract misses remain
/// explicit fields in the receipt.
#[allow(clippy::too_many_lines)]
pub async fn run_assigned_range_placement_worker(
    config: &AssignedRangePlacementConfig,
) -> Result<AssignedRangePlacementReceipt, String> {
    validate_config(config)?;
    let scratch = GuardedScratchRoot::new(config.seed)?;
    let object_root = scratch.0.join("objects");
    let hydration_cache_root = scratch.0.join("hydration-cache");
    let pressure_cache_root = scratch.0.join("pressure-cache");
    let staging_root = scratch.0.join("placed.staging");
    let ready_root = scratch.0.join("placed.ready");
    fs::create_dir_all(&object_root).map_err(|error| error.to_string())?;

    let raw_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&object_root).map_err(|error| error.to_string())?,
    );
    let counters = Arc::new(IoCounters::default());
    let counted_store: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(
        Arc::clone(&raw_store),
        Arc::clone(&counters),
    ));
    let engine = build_engine(Arc::clone(&counted_store), config.seed).await?;
    let (base_mutations, mut oracle) = base_fixture(config);
    let base_cell_mutations = base_mutations
        .iter()
        .map(model_to_cell_mutation)
        .collect::<Vec<_>>();
    let base_envelope = envelope(1, [0; 32], &base_cell_mutations)?;
    let mut final_log_chain: [u8; 32] = Sha256::digest(base_envelope.encode()).into();
    engine
        .apply(CommitBatch {
            version: Version::new(1),
            identity: CommitIdentity::for_test(config.seed.max(1)),
            mutations: base_mutations,
        })
        .await
        .map_err(|error| error.to_string())?;
    engine.flush().await.map_err(|error| error.to_string())?;
    let physical = inspect_latest_physical_manifest(
        Arc::clone(&counted_store),
        DATABASE_PATH,
        config.seed ^ 0xa551,
    )
    .await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let range_root = AuthorityRangeRoot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        manifest: AuthorityManifestReference {
            key: physical.manifest.key.clone(),
            length: physical.manifest.length,
            sha256: physical.manifest.sha256.clone(),
        },
        covered_through: 1,
        minimum_readable_version: 1,
        log_chain_sha256: final_log_chain,
    };
    let base_descriptor = PersistentRangeBaseDescriptor {
        format_version: 1,
        database_path: DATABASE_PATH.to_owned(),
        root: range_root.clone(),
        physical: physical.clone(),
    };
    let provider = bind_provider_physical_manifest(
        Arc::clone(&counted_store),
        ProviderKind::VersionedTest,
        PROVIDER_NAMESPACE,
        &physical,
    )
    .await?;
    let descriptor = promote_provider_bound_persistent_range_base(&base_descriptor, provider)?;
    let manifest_identity = format!(
        "{}:{}:{}",
        range_root.manifest.key, range_root.manifest.length, range_root.manifest.sha256
    );
    let provider_store = Arc::new(ProviderBoundObjectStore::new(
        Arc::clone(&counted_store),
        ProviderKind::VersionedTest,
        PROVIDER_NAMESPACE,
        &descriptor.provider,
    )?);
    let provider_source: Arc<dyn ObjectStore> = provider_store.clone();
    let (policy, signing_seeds) = log_policy()?;
    let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
    let mut records = Vec::with_capacity(config.tail_records.saturating_add(1));
    for tail_index in 0..config.tail_records {
        let sequence = u64::try_from(tail_index)
            .unwrap_or(u64::MAX)
            .saturating_add(2);
        let ordinal = tail_ordinal(config.seed, tail_index, config.key_count);
        let key = key_for(ordinal);
        let value = value_for(config, ordinal, sequence);
        oracle.insert(key.clone(), value.clone());
        let commit = envelope(
            sequence,
            final_log_chain,
            &[CellMutation::Set { key, value }],
        )?;
        final_log_chain = Sha256::digest(commit.encode()).into();
        records.push(certified_record(&commit, &policy, &signing_seeds)?);
    }
    let mut target_version = u64::try_from(config.tail_records)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let (range_begin, range_end, assigned_first, assigned_end) = assignment_bounds(config);
    let mut expected = expected_range(&oracle, &range_begin, &range_end);
    let logical_assigned_bytes = logical_bytes(&expected);
    let logical_row_count = u64::try_from(expected.len()).unwrap_or(u64::MAX);
    let cache_limit = usize::try_from(logical_assigned_bytes.saturating_mul(3) / 2)
        .unwrap_or(usize::MAX)
        .max(CACHE_PART_BYTES.saturating_mul(2));
    let hydration_before_io = counters.total();
    let hydration_before_provider = provider_store.stats();
    let hydration_started = Instant::now();
    let staging_store = build_cached_store(
        &hydration_cache_root,
        Arc::clone(&provider_source),
        cache_limit,
    )
    .await?;
    let hydration_view = open_view(
        Arc::clone(&staging_store),
        range_root.clone(),
        target_version,
        records.clone(),
        &policies,
        config.seed ^ 0xa552,
    )
    .await?;
    let hydration_limit = if config.mode == AssignedRangePlacementMode::PublishBeforeVerification {
        expected.len().saturating_div(2).max(1)
    } else {
        expected.len()
    };
    let hydrated = hydration_view
        .scan_at(&range_begin, &range_end, target_version, hydration_limit)
        .await
        .map_err(|error| error.to_string())?;
    let verification_complete = hydrated == expected;
    hydration_view
        .close()
        .await
        .map_err(|error| error.to_string())?;
    drop(staging_store);
    let local_image_digest = write_new_range_image(
        &staging_root,
        &range_begin,
        &range_end,
        target_version,
        &hydrated,
        image_root_identity(
            &manifest_identity,
            &descriptor.provider.closure_sha256,
            target_version,
            &hex_digest(&final_log_chain),
        ),
    )?;
    if hydration_cache_root.exists() {
        fs::remove_dir_all(&hydration_cache_root).map_err(|error| error.to_string())?;
    }
    let hydration_duration_seconds = hydration_started.elapsed().as_secs_f64();
    let hydration_io = counters.total().difference_since(&hydration_before_io);
    let hydration_provider_requests = provider_store
        .stats()
        .get_requests
        .saturating_sub(hydration_before_provider.get_requests);
    let hydration_provider_bytes = hydration_io.read_byte_total();
    let oracle_digest = rows_digest(&expected);
    let mut placed = PlacedRangeReceipt {
        format_version: FORMAT_VERSION,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        range_begin: range_begin.clone(),
        range_end: range_end.clone(),
        assignment_epoch: ASSIGNMENT_EPOCH,
        authority_generation: range_root.generation,
        authority_manifest_identity: manifest_identity.clone(),
        provider_closure_digest: descriptor.provider.closure_sha256.clone(),
        target_version,
        final_log_chain_sha256: hex_digest(&final_log_chain),
        local_image_format: LOCAL_IMAGE_FORMAT.to_owned(),
        local_image_digest: local_image_digest.clone(),
        logical_row_count,
        logical_assigned_bytes,
        placed_bytes: 0,
        placement_amplification: 0.0,
        hydration_provider_requests,
        hydration_provider_bytes,
        hydration_duration_seconds,
        oracle_digest: oracle_digest.clone(),
        published_at_unix_millis: now_unix_millis(),
    };
    finalize_and_write_receipt(&staging_root, &mut placed)?;
    fs::rename(&staging_root, &ready_root).map_err(|error| error.to_string())?;
    sync_parent(&ready_root)?;
    let ready_publication_atomic = ready_root.is_dir() && !staging_root.exists();
    let persisted: PlacedRangeReceipt = serde_json::from_slice(
        &fs::read(ready_root.join(READY_RECEIPT_NAME)).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let mut ready_receipt_exact = persisted == placed;

    let digest_before_pressure = placed.local_image_digest.clone();
    if config.apply_unrelated_pressure && config.logical_range_count > 1 {
        let pressure_range = (config.assigned_range_index + 1) % config.logical_range_count;
        let (pressure_begin, pressure_end, _, _) = bounds_for_range(config, pressure_range);
        let pressure_store = build_cached_store(
            &pressure_cache_root,
            Arc::clone(&provider_source),
            cache_limit,
        )
        .await?;
        let pressure_view = open_view(
            pressure_store,
            range_root.clone(),
            target_version,
            records.clone(),
            &policies,
            config.seed ^ 0xa553,
        )
        .await?;
        let pressure_rows = config.key_count / config.logical_range_count;
        let _ = pressure_view
            .scan_at(
                &pressure_begin,
                &pressure_end,
                target_version,
                pressure_rows,
            )
            .await
            .map_err(|error| error.to_string())?;
        pressure_view
            .close()
            .await
            .map_err(|error| error.to_string())?;
        if pressure_cache_root.exists() {
            fs::remove_dir_all(&pressure_cache_root).map_err(|error| error.to_string())?;
        }
    }
    let local_image_digest_stable = open_range_image(
        &ready_root,
        ASSIGNMENT_EPOCH,
        target_version,
        DEFAULT_READER_MEMORY_BYTES,
    )
    .is_ok_and(|(reader, _)| reader.image_identity_sha256() == digest_before_pressure);

    let mut old_ready_refused_after_advance = true;
    if config.root_advance {
        let advance_sequence = target_version.saturating_add(1);
        let advance_ordinal = assigned_first.min(assigned_end.saturating_sub(1));
        let advance_key = key_for(advance_ordinal);
        let advance_value = value_for(config, advance_ordinal, advance_sequence);
        oracle.insert(advance_key.clone(), advance_value.clone());
        let advance = envelope(
            advance_sequence,
            final_log_chain,
            &[CellMutation::Set {
                key: advance_key,
                value: advance_value,
            }],
        )?;
        final_log_chain = Sha256::digest(advance.encode()).into();
        records.push(certified_record(&advance, &policy, &signing_seeds)?);
        target_version = advance_sequence;
        expected = expected_range(&oracle, &range_begin, &range_end);
        old_ready_refused_after_advance = placed.target_version != target_version;
        if config.mode == AssignedRangePlacementMode::ReuseStaleReceipt {
            old_ready_refused_after_advance = false;
        } else {
            placed.local_image_digest = replace_range_image(
                &ready_root,
                &range_begin,
                &range_end,
                target_version,
                &expected,
                image_root_identity(
                    &manifest_identity,
                    &descriptor.provider.closure_sha256,
                    target_version,
                    &hex_digest(&final_log_chain),
                ),
            )?;
            placed.target_version = target_version;
            placed.final_log_chain_sha256 = hex_digest(&final_log_chain);
            placed.oracle_digest = rows_digest(&expected);
            placed.logical_row_count = u64::try_from(expected.len()).unwrap_or(u64::MAX);
            placed.logical_assigned_bytes = logical_bytes(&expected);
            placed.published_at_unix_millis = now_unix_millis();
            atomic_replace_receipt(&ready_root, &mut placed)?;
            ready_receipt_exact &= serde_json::from_slice::<PlacedRangeReceipt>(
                &fs::read(ready_root.join(READY_RECEIPT_NAME))
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?
                == placed;
        }
    }

    if matches!(
        config.mode,
        AssignedRangePlacementMode::CorruptLocalPart
            | AssignedRangePlacementMode::AcceptProviderFallback
    ) {
        corrupt_range_image(&ready_root)?;
    }

    let post_before_io = counters.total();
    let post_before_provider = provider_store.stats();
    let mut probe_config = config.clone();
    probe_config.process_probe_executable = None;
    let image_probe = AssignedRangeImageProbeConfig {
        placement: probe_config,
        ready_root: ready_root.clone(),
        target_version,
        expected_receipt: placed.clone(),
    };
    let mut process_reopen_executed = false;
    let probe = if config.reopen_retained_nvme {
        if let Some(executable) = config.process_probe_executable.as_deref() {
            process_reopen_executed = true;
            run_image_probe_child(executable, &image_probe)
        } else {
            run_assigned_range_image_probe(&image_probe)
        }
    } else {
        run_assigned_range_image_probe(&image_probe)
    };
    let probe_failed = probe.is_err();
    let (
        exact_points,
        exact_scan,
        outside_range_refused,
        post_ready_point_p99_seconds,
        scan_rows_per_second,
        scan_rows,
    ) = match probe {
        Ok(receipt) => (
            receipt.exact_points,
            receipt.exact_scan,
            receipt.outside_range_refused,
            receipt.point_p99_seconds,
            receipt.scan_rows_per_second,
            receipt.scan_rows,
        ),
        Err(_) => (false, false, false, 0.0, 0.0, 0),
    };
    let mut corruption_detected = probe_failed;
    if config.mode == AssignedRangePlacementMode::AcceptProviderFallback {
        let manifest_path = object_store::path::Path::from(range_root.manifest.key.clone());
        let fallback = provider_source
            .get(&manifest_path)
            .await
            .map_err(|error| error.to_string())?;
        let _ = fallback.bytes().await.map_err(|error| error.to_string())?;
    }
    let post_io = counters.total().difference_since(&post_before_io);
    let post_ready_provider_requests = provider_store
        .stats()
        .get_requests
        .saturating_sub(post_before_provider.get_requests);
    let post_ready_provider_bytes = post_io.read_byte_total();
    if config.mode == AssignedRangePlacementMode::CorruptLocalPart && (!exact_points || !exact_scan)
    {
        corruption_detected = true;
    }
    let unsafe_provider_fallback_accepted =
        config.mode == AssignedRangePlacementMode::AcceptProviderFallback;
    let projected_one_copy_bytes = placed.placed_bytes;
    let projected_two_copy_bytes = placed.placed_bytes.saturating_mul(2);
    let hydration_throughput_bytes_per_second = f64::from(
        u32::try_from(logical_assigned_bytes.min(u64::from(u32::MAX))).unwrap_or(u32::MAX),
    ) / hydration_duration_seconds.max(f64::EPSILON);
    let semantic_receipt_sha256 = semantic_digest(
        config,
        &placed,
        [
            verification_complete,
            exact_points,
            exact_scan,
            outside_range_refused,
            old_ready_refused_after_advance,
        ],
    );
    let scratch_cleanup_complete = scratch.cleanup()?;
    Ok(AssignedRangePlacementReceipt {
        format_version: FORMAT_VERSION,
        mode: config.mode,
        seed: config.seed,
        key_count: config.key_count,
        value_bytes: config.value_bytes,
        logical_range_count: config.logical_range_count,
        assigned_range_index: config.assigned_range_index,
        tail_records: config.tail_records,
        point_reads: config.point_reads,
        process_reopen_requested: config.reopen_retained_nvme,
        process_reopen_executed,
        root_advance_requested: config.root_advance,
        placed,
        hydration_throughput_bytes_per_second,
        projected_one_copy_bytes,
        projected_two_copy_bytes,
        post_ready_provider_requests,
        post_ready_provider_bytes,
        post_ready_point_p99_seconds,
        post_ready_scan_rows_per_second: scan_rows_per_second,
        post_ready_scan_rows: scan_rows,
        verification_complete,
        exact_points,
        exact_scan,
        outside_range_refused,
        ready_publication_atomic,
        ready_receipt_exact,
        local_image_digest_stable,
        old_ready_refused_after_advance,
        unsafe_provider_fallback_accepted,
        corruption_detected,
        scratch_cleanup_complete,
        semantic_receipt_sha256,
    })
}

fn validate_config(config: &AssignedRangePlacementConfig) -> Result<(), String> {
    if config.key_count < 16
        || config.value_bytes < 1_024
        || config.logical_range_count == 0
        || config.logical_range_count > config.key_count
        || !config.key_count.is_multiple_of(config.logical_range_count)
        || config.assigned_range_index >= config.logical_range_count
        || config.point_reads < config.key_count / config.logical_range_count
    {
        return Err("assigned-range placement requires divisible nonzero ranges, at least 16 keys, 1 KiB values, and an exhaustive point workload".to_owned());
    }
    Ok(())
}

async fn build_engine(store: Arc<dyn ObjectStore>, seed: u64) -> Result<SlateEngine, String> {
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(DATABASE_PATH, store)
        .with_settings(settings)
        .with_seed(seed ^ 0xa550)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| error.to_string())
}

async fn build_cached_store(
    root: &Path,
    store: Arc<dyn ObjectStore>,
    maximum_bytes: usize,
) -> Result<Arc<dyn ObjectStore>, String> {
    CachedObjectStore::builder(root, store)
        .with_max_cache_size_bytes(Some(maximum_bytes))
        .with_part_size_bytes(CACHE_PART_BYTES)
        .with_cache_on_flush(false)
        .with_scan_interval(None)
        .with_max_open_file_handles(64)
        .build()
        .await
        .map(|store| store as Arc<dyn ObjectStore>)
        .map_err(|error| error.to_string())
}

async fn open_view(
    store: Arc<dyn ObjectStore>,
    root: AuthorityRangeRoot,
    target_version: u64,
    records: Vec<CertifiedTxLogRecord>,
    policies: &BTreeMap<u16, CellLogSetPolicy>,
    seed: u64,
) -> Result<AuthorityBoundRangeView, String> {
    let decoded: Arc<dyn DbCache> = Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: DECODED_CACHE_BYTES,
        time_to_live: None,
        time_to_idle: None,
    }));
    AuthorityBoundRangeView::open_with_cache(
        DATABASE_PATH,
        store,
        root,
        target_version,
        records,
        policies,
        seed,
        decoded,
    )
    .await
    .map_err(|error| error.to_string())
}

fn base_fixture(
    config: &AssignedRangePlacementConfig,
) -> (Vec<Mutation>, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut mutations = Vec::with_capacity(config.key_count);
    let mut oracle = BTreeMap::new();
    for ordinal in 0..config.key_count {
        let key = key_for(ordinal);
        let value = value_for(config, ordinal, 1);
        mutations.push(Mutation::Set {
            key: key.clone(),
            value: value.clone(),
        });
        oracle.insert(key, value);
    }
    (mutations, oracle)
}

fn model_to_cell_mutation(mutation: &Mutation) -> CellMutation {
    match mutation {
        Mutation::Set { key, value } => CellMutation::Set {
            key: key.clone(),
            value: value.clone(),
        },
        Mutation::Clear { key } => CellMutation::Clear { key: key.clone() },
        Mutation::ClearRange { .. } => {
            unreachable!("assigned-range base fixture contains only point mutations")
        }
    }
}

fn assignment_bounds(config: &AssignedRangePlacementConfig) -> (Vec<u8>, Vec<u8>, usize, usize) {
    bounds_for_range(config, config.assigned_range_index)
}

fn bounds_for_range(
    config: &AssignedRangePlacementConfig,
    range: usize,
) -> (Vec<u8>, Vec<u8>, usize, usize) {
    let keys_per_range = config.key_count / config.logical_range_count;
    let first = range.saturating_mul(keys_per_range);
    let end = first.saturating_add(keys_per_range);
    let begin_key = key_for(first);
    let end_key = if end == config.key_count {
        b"k0".to_vec()
    } else {
        key_for(end)
    };
    (begin_key, end_key, first, end)
}

fn expected_range(
    oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
    begin: &[u8],
    end: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    oracle
        .range(begin.to_vec()..end.to_vec())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn logical_bytes(rows: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    rows.iter().fold(0_u64, |total, (key, value)| {
        total.saturating_add(
            u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX),
        )
    })
}

fn point_order(config: &AssignedRangePlacementConfig, first: usize, end: usize) -> Vec<usize> {
    let mut base = (first..end).collect::<Vec<_>>();
    let mut state = config.seed ^ 0x9e37_79b9_7f4a_7c15;
    for index in (1..base.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let swap =
            usize::try_from(state % u64::try_from(index + 1).unwrap_or(u64::MAX)).unwrap_or(0);
        base.swap(index, swap);
    }
    (0..config.point_reads)
        .map(|index| base[index % base.len()])
        .collect()
}

fn key_for(ordinal: usize) -> Vec<u8> {
    format!("k/{ordinal:016x}").into_bytes()
}

fn value_for(config: &AssignedRangePlacementConfig, ordinal: usize, sequence: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(config.value_bytes);
    let mut block = 0_u64;
    while value.len() < config.value_bytes {
        let mut hasher = Sha256::new();
        hasher.update(config.seed.to_be_bytes());
        hasher.update(ordinal.to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        hasher.update(block.to_be_bytes());
        value.extend_from_slice(&hasher.finalize());
        block = block.saturating_add(1);
    }
    value.truncate(config.value_bytes);
    value
}

fn tail_ordinal(seed: u64, tail_index: usize, key_count: usize) -> usize {
    usize::try_from(seed)
        .unwrap_or(usize::MAX)
        .wrapping_add(tail_index.wrapping_mul(7_919))
        % key_count
}

fn envelope(
    sequence: u64,
    previous_log_chain: [u8; 32],
    mutations: &[CellMutation],
) -> Result<CommitEnvelope, String> {
    let mut client_id = [0_u8; 16];
    client_id[8..].copy_from_slice(&sequence.to_be_bytes());
    Ok(CommitEnvelope::from_parts(CommitEnvelopeParts {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        version: Version::from_parts(GENERATION, sequence),
        log_index: sequence,
        client_id,
        request_id: sequence,
        resolver_set_id: [0x33; 16],
        read_conflicts: Vec::new(),
        write_conflicts: Vec::new(),
        canonical_mutations: serde_json::to_vec(mutations).map_err(|error| error.to_string())?,
        required_resolvers: vec![1],
        required_log_tags: vec![LOG_SET_ID],
        previous_log_chain,
    }))
}

fn log_policy() -> Result<(CellLogSetPolicy, BTreeMap<u64, Vec<u8>>), String> {
    let seeds = BTreeMap::from([
        (101, vec![0x11; 32]),
        (102, vec![0x22; 32]),
        (103, vec![0x33; 32]),
    ]);
    let members = seeds
        .iter()
        .map(|(node_id, seed)| {
            tagged_log_public_key(seed)
                .map(|public_key| CellLogSetMember {
                    node_id: *node_id,
                    public_key,
                })
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        CellLogSetPolicy {
            format_version: 1,
            generation: GENERATION,
            policy_epoch: 1,
            log_set_id: LOG_SET_ID,
            quorum_size: 2,
            ratekeeper_soft_limit_bytes: u64::MAX,
            members,
        },
        seeds,
    ))
}

fn certified_record(
    envelope: &CommitEnvelope,
    policy: &CellLogSetPolicy,
    seeds: &BTreeMap<u64, Vec<u8>>,
) -> Result<CertifiedTxLogRecord, String> {
    let encoded = envelope.encode();
    let (encoded_client_id, request_id) = envelope.client_identity();
    let mut client_id = [0_u8; 8];
    client_id.copy_from_slice(&encoded_client_id[8..]);
    let statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        transaction_identity: RequestIdentity {
            client_id: u64::from_be_bytes(client_id),
            request_id,
        },
        commit_sequence: envelope.version().sequence(),
        log_set_id: LOG_SET_ID,
        policy_epoch: policy.policy_epoch,
        envelope_sha256: Sha256::digest(&encoded).into(),
        durable_position: envelope.version().sequence(),
    };
    let attestations = seeds
        .iter()
        .take(2)
        .map(|(node_id, seed)| {
            sign_tagged_log_statement(*node_id, seed, &statement).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CertifiedTxLogRecord {
        envelope: encoded,
        certificates: vec![CellTaggedLogCertificate {
            statement,
            attestations,
        }],
    })
}

fn finalize_and_write_receipt(
    directory: &Path,
    receipt: &mut PlacedRangeReceipt,
) -> Result<(), String> {
    fs::create_dir_all(directory).map_err(|error| error.to_string())?;
    let data_bytes = directory_bytes(directory, Some(READY_RECEIPT_NAME))?;
    let mut prior_length = 0_u64;
    for _ in 0..8 {
        let serialized = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
        let length = u64::try_from(serialized.len()).unwrap_or(u64::MAX);
        receipt.placed_bytes = data_bytes.saturating_add(length);
        receipt.placement_amplification =
            ratio(receipt.placed_bytes, receipt.logical_assigned_bytes);
        if length == prior_length {
            break;
        }
        prior_length = length;
    }
    let serialized = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    let path = directory.join(READY_RECEIPT_NAME);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|error| error.to_string())?;
    file.write_all(&serialized)
        .map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn atomic_replace_receipt(
    directory: &Path,
    receipt: &mut PlacedRangeReceipt,
) -> Result<(), String> {
    let data_bytes = directory_bytes(directory, Some(READY_RECEIPT_NAME))?;
    let mut prior_length = 0_u64;
    for _ in 0..8 {
        let serialized = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
        let length = u64::try_from(serialized.len()).unwrap_or(u64::MAX);
        receipt.placed_bytes = data_bytes.saturating_add(length);
        receipt.placement_amplification =
            ratio(receipt.placed_bytes, receipt.logical_assigned_bytes);
        if length == prior_length {
            break;
        }
        prior_length = length;
    }
    let temporary = directory.join("placed-ready.next");
    let path = directory.join(READY_RECEIPT_NAME);
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    sync_parent(&path)
}

fn sync_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "ready publication has no parent".to_owned())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

fn directory_bytes(root: &Path, excluded_name: Option<&str>) -> Result<u64, String> {
    collect_files(root)?
        .into_iter()
        .try_fold(0_u64, |total, path| {
            if excluded_name.is_some_and(|excluded| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == excluded)
            }) {
                return Ok(total);
            }
            fs::metadata(path)
                .map(|metadata| total.saturating_add(metadata.len()))
                .map_err(|error| error.to_string())
        })
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    collect_files_into(root, &mut files).map_err(|error| error.to_string())?;
    Ok(files)
}

fn collect_files_into(root: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_files_into(&entry.path(), files)?;
        } else if file_type.is_file() {
            files.push(entry.path());
        }
    }
    Ok(())
}

fn corrupt_range_image(root: &Path) -> Result<(), String> {
    let path = root.join(RANGE_IMAGE_NAME);
    let mut bytes = fs::read(&path).map_err(|error| error.to_string())?;
    let first = bytes
        .first_mut()
        .ok_or_else(|| "placed range image is empty".to_owned())?;
    *first ^= 0xff;
    fs::write(path, bytes).map_err(|error| error.to_string())
}

fn rows_digest(rows: &[(Vec<u8>, Vec<u8>)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-assigned-range-oracle-v1");
    for (key, value) in rows {
        hasher.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(key);
        hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(value);
    }
    format!("{:x}", hasher.finalize())
}

fn semantic_digest(
    config: &AssignedRangePlacementConfig,
    placed: &PlacedRangeReceipt,
    checks: [bool; 5],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-assigned-range-placement-result-v1");
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.mode.id().as_bytes());
    hasher.update(config.key_count.to_be_bytes());
    hasher.update(config.logical_range_count.to_be_bytes());
    hasher.update(config.assigned_range_index.to_be_bytes());
    hasher.update(placed.target_version.to_be_bytes());
    hasher.update(placed.local_image_digest.as_bytes());
    hasher.update(placed.oracle_digest.as_bytes());
    hasher.update(checks.map(u8::from));
    format!("{:x}", hasher.finalize())
}

fn percentile_or_zero(sorted: &[f64], percentile: usize) -> f64 {
    if sorted.is_empty() {
        0.0
    } else {
        let index = sorted.len().saturating_sub(1).saturating_mul(percentile) / 100;
        sorted[index]
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    let numerator = u32::try_from(numerator.min(u64::from(u32::MAX))).unwrap_or(u32::MAX);
    let denominator = u32::try_from(denominator.min(u64::from(u32::MAX)))
        .unwrap_or(u32::MAX)
        .max(1);
    f64::from(numerator) / f64::from(denominator)
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn image_root_identity(
    manifest_identity: &str,
    provider_closure_digest: &str,
    target_version: u64,
    final_log_chain_sha256: &str,
) -> [u8; 32] {
    let generation = GENERATION.to_be_bytes();
    let target = target_version.to_be_bytes();
    root_identity_digest(&[
        &CELL_ID,
        &TENANT_ID,
        &generation,
        manifest_identity.as_bytes(),
        provider_closure_digest.as_bytes(),
        &target,
        final_log_chain_sha256.as_bytes(),
    ])
}

fn placed_root_identity(receipt: &PlacedRangeReceipt) -> [u8; 32] {
    let generation = receipt.authority_generation.to_be_bytes();
    let target = receipt.target_version.to_be_bytes();
    root_identity_digest(&[
        &receipt.cell_id,
        &receipt.tenant_id,
        &generation,
        receipt.authority_manifest_identity.as_bytes(),
        receipt.provider_closure_digest.as_bytes(),
        &target,
        receipt.final_log_chain_sha256.as_bytes(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny(mode: AssignedRangePlacementMode) -> AssignedRangePlacementConfig {
        AssignedRangePlacementConfig {
            key_count: 32,
            value_bytes: 1_024,
            logical_range_count: 4,
            assigned_range_index: 1,
            tail_records: 2,
            point_reads: 16,
            apply_unrelated_pressure: true,
            reopen_retained_nvme: true,
            root_advance: true,
            mode,
            seed: 724_851,
            process_probe_executable: None,
        }
    }

    #[test]
    fn assigned_bounds_are_half_open_and_complete() {
        let config = tiny(AssignedRangePlacementMode::Correct);
        let (begin, end, first, last) = assignment_bounds(&config);
        assert_eq!((first, last), (8, 16));
        assert_eq!(begin, key_for(8));
        assert_eq!(end, key_for(16));
    }

    #[tokio::test]
    async fn derived_image_emits_exact_root_bound_receipt() {
        let receipt = Box::pin(run_assigned_range_placement_worker(&tiny(
            AssignedRangePlacementMode::Correct,
        )))
        .await
        .unwrap();
        assert!(receipt.verification_complete);
        assert!(receipt.exact_points);
        assert!(receipt.exact_scan);
        assert!(receipt.outside_range_refused);
        assert!(receipt.ready_publication_atomic);
        assert!(receipt.ready_receipt_exact);
        assert!(receipt.scratch_cleanup_complete);
        assert_eq!(
            receipt.placed.local_image_format,
            "okv-derived-sorted-range-v2"
        );
        assert_eq!(receipt.placed.oracle_digest.len(), 64);
    }

    #[tokio::test]
    async fn corrupt_image_is_detected_without_provider_fallback() {
        let receipt = Box::pin(run_assigned_range_placement_worker(&tiny(
            AssignedRangePlacementMode::CorruptLocalPart,
        )))
        .await
        .unwrap();
        assert!(receipt.corruption_detected);
        assert_eq!(receipt.post_ready_provider_requests, 0);
    }

    #[tokio::test]
    async fn unsafe_provider_fallback_control_issues_provider_work() {
        let receipt = Box::pin(run_assigned_range_placement_worker(&tiny(
            AssignedRangePlacementMode::AcceptProviderFallback,
        )))
        .await
        .unwrap();
        assert!(receipt.unsafe_provider_fallback_accepted);
        assert!(receipt.post_ready_provider_requests > 0);
        assert!(receipt.post_ready_provider_bytes > 0);
    }

    #[tokio::test]
    async fn full_range_uses_a_key_outside_the_logical_keyspace() {
        let mut config = tiny(AssignedRangePlacementMode::Correct);
        config.logical_range_count = 1;
        config.assigned_range_index = 0;
        config.apply_unrelated_pressure = false;
        config.point_reads = config.key_count;

        let receipt = Box::pin(run_assigned_range_placement_worker(&config))
            .await
            .unwrap();

        assert!(receipt.outside_range_refused);
    }

    #[tokio::test]
    async fn premature_publication_is_visible_in_receipt() {
        let receipt = Box::pin(run_assigned_range_placement_worker(&tiny(
            AssignedRangePlacementMode::PublishBeforeVerification,
        )))
        .await
        .unwrap();
        assert!(!receipt.verification_complete);
    }

    #[tokio::test]
    async fn stale_receipt_control_accepts_advanced_target() {
        let receipt = Box::pin(run_assigned_range_placement_worker(&tiny(
            AssignedRangePlacementMode::ReuseStaleReceipt,
        )))
        .await
        .unwrap();
        assert!(!receipt.old_ready_refused_after_advance);
    }
}
