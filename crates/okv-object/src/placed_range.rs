//! Root-bound assigned-range placement over the incumbent shared `SlateDB` cache.

use crate::{
    bind_provider_physical_manifest, promote_provider_bound_persistent_range_base,
    AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord,
    PersistentRangeBaseDescriptor, ProviderBoundObjectStore, ProviderKind,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
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
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
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

/// Correct incumbent subject or one deliberately unsafe placement control.
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

struct PlacedRangeReader {
    view: AuthorityBoundRangeView,
    range_begin: Vec<u8>,
    range_end: Vec<u8>,
    assignment_epoch: u64,
    target_version: u64,
}

impl PlacedRangeReader {
    fn validate_identity(&self, assignment_epoch: u64, target_version: u64) -> Result<(), String> {
        if self.assignment_epoch != assignment_epoch || self.target_version != target_version {
            return Err("placed range receipt does not authorize requested assignment".to_owned());
        }
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if key < self.range_begin.as_slice() || key >= self.range_end.as_slice() {
            return Err("point is outside placed range".to_owned());
        }
        self.view
            .get_at(key, self.target_version)
            .await
            .map_err(|error| error.to_string())
    }

    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if start < self.range_begin.as_slice() || end > self.range_end.as_slice() || start >= end {
            return Err("scan is outside placed range".to_owned());
        }
        self.view
            .scan_at(start, end, self.target_version, limit)
            .await
            .map_err(|error| error.to_string())
    }

    async fn close(self) -> Result<(), String> {
        self.view.close().await.map_err(|error| error.to_string())
    }
}

/// Execute the direct shared-cache incumbent for one assigned logical range.
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
    let staging_store =
        build_cached_store(&staging_root, Arc::clone(&provider_source), cache_limit).await?;
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
    let hydration_duration_seconds = hydration_started.elapsed().as_secs_f64();
    let hydration_io = counters.total().difference_since(&hydration_before_io);
    let hydration_provider_requests = provider_store
        .stats()
        .get_requests
        .saturating_sub(hydration_before_provider.get_requests);
    let hydration_provider_bytes = hydration_io.read_byte_total();
    let oracle_digest = rows_digest(&expected);
    let local_image_digest = directory_digest(&staging_root, Some(READY_RECEIPT_NAME))?;
    let manifest_identity = format!(
        "{}:{}:{}",
        range_root.manifest.key, range_root.manifest.length, range_root.manifest.sha256
    );
    let mut placed = PlacedRangeReceipt {
        format_version: FORMAT_VERSION,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        range_begin: range_begin.clone(),
        range_end: range_end.clone(),
        assignment_epoch: ASSIGNMENT_EPOCH,
        authority_generation: range_root.generation,
        authority_manifest_identity: manifest_identity,
        provider_closure_digest: descriptor.provider.closure_sha256.clone(),
        target_version,
        final_log_chain_sha256: hex_digest(&final_log_chain),
        local_image_format: "slatedb-cached-object-store-v0".to_owned(),
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

    let digest_before_pressure = directory_digest(&ready_root, Some(READY_RECEIPT_NAME))?;
    if config.apply_unrelated_pressure && config.logical_range_count > 1 {
        let pressure_range = (config.assigned_range_index + 1) % config.logical_range_count;
        let (pressure_begin, pressure_end, _, _) = bounds_for_range(config, pressure_range);
        let pressure_store =
            build_cached_store(&ready_root, Arc::clone(&provider_source), cache_limit).await?;
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
    }
    let local_image_digest_stable =
        directory_digest(&ready_root, Some(READY_RECEIPT_NAME))? == digest_before_pressure;

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
            placed.target_version = target_version;
            placed.final_log_chain_sha256 = hex_digest(&final_log_chain);
            placed.oracle_digest = rows_digest(&expected);
            placed.logical_row_count = u64::try_from(expected.len()).unwrap_or(u64::MAX);
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
        corrupt_first_cache_part(&ready_root)?;
    }

    let post_before_io = counters.total();
    let post_before_provider = provider_store.stats();
    let ready_store = build_cached_store(&ready_root, provider_source, cache_limit).await?;
    let ready_view = open_view(
        ready_store,
        range_root,
        target_version,
        records,
        &policies,
        config.seed ^ 0xa554,
    )
    .await;
    let mut point_seconds = Vec::with_capacity(config.point_reads);
    let mut exact_points = true;
    let exact_scan;
    let mut outside_range_refused = false;
    let mut scan_rows = 0_u64;
    let mut scan_rows_per_second = 0.0_f64;
    let mut corruption_detected = false;
    if let Ok(view) = ready_view {
        let reader = PlacedRangeReader {
            view,
            range_begin: range_begin.clone(),
            range_end: range_end.clone(),
            assignment_epoch: ASSIGNMENT_EPOCH,
            target_version,
        };
        let identity_checked = if config.mode == AssignedRangePlacementMode::ReuseStaleReceipt {
            true
        } else {
            reader
                .validate_identity(ASSIGNMENT_EPOCH, placed.target_version)
                .is_ok()
        };
        exact_points &= identity_checked;
        let order = point_order(config, assigned_first, assigned_end);
        for ordinal in order {
            let key = key_for(ordinal);
            let started = Instant::now();
            if let Ok(observed) = reader.get(&key).await {
                point_seconds.push(started.elapsed().as_secs_f64());
                exact_points &= observed.as_ref() == oracle.get(&key);
            } else {
                corruption_detected = true;
                exact_points = false;
                break;
            }
        }
        let scan_started = Instant::now();
        if let Ok(observed) = reader.scan(&range_begin, &range_end, expected.len()).await {
            let elapsed = scan_started.elapsed().as_secs_f64();
            scan_rows = u64::try_from(observed.len()).unwrap_or(u64::MAX);
            scan_rows_per_second = f64::from(u32::try_from(observed.len()).unwrap_or(u32::MAX))
                / elapsed.max(f64::EPSILON);
            exact_scan = observed == expected;
        } else {
            corruption_detected = true;
            exact_scan = false;
        }
        let outside_key = if config.logical_range_count == 1 {
            b"outside-assigned-range".to_vec()
        } else if config.assigned_range_index == 0 {
            key_for(assigned_end.min(config.key_count.saturating_sub(1)))
        } else {
            key_for(assigned_first.saturating_sub(1))
        };
        outside_range_refused = reader.get(&outside_key).await.is_err();
        reader.close().await?;
    } else {
        corruption_detected = true;
        exact_points = false;
        exact_scan = false;
    }
    point_seconds.sort_by(f64::total_cmp);
    let post_ready_point_p99_seconds = percentile_or_zero(&point_seconds, 99);
    let post_io = counters.total().difference_since(&post_before_io);
    let post_ready_provider_requests = provider_store
        .stats()
        .get_requests
        .saturating_sub(post_before_provider.get_requests);
    let post_ready_provider_bytes = post_io.read_byte_total();
    if config.mode == AssignedRangePlacementMode::CorruptLocalPart
        && (post_ready_provider_requests > 0 || !exact_points || !exact_scan)
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
        process_reopen_executed: false,
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

fn directory_digest(root: &Path, excluded_name: Option<&str>) -> Result<String, String> {
    let mut paths = collect_files(root)?;
    paths.retain(|path| {
        excluded_name.is_none_or(|excluded| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_none_or(|name| name != excluded)
        })
    });
    paths.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"okv-placed-range-local-image-v1");
    for path in paths {
        let relative = path.strip_prefix(root).map_err(|error| error.to_string())?;
        let name = relative.to_string_lossy();
        hasher.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(name.as_bytes());
        let mut file = File::open(&path).map_err(|error| error.to_string())?;
        let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
        loop {
            let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
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

fn corrupt_first_cache_part(root: &Path) -> Result<(), String> {
    let path = collect_files(root)?
        .into_iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("_part"))
        })
        .ok_or_else(|| "placed range has no cache part to corrupt".to_owned())?;
    let mut bytes = fs::read(&path).map_err(|error| error.to_string())?;
    let first = bytes
        .first_mut()
        .ok_or_else(|| "placed range cache part is empty".to_owned())?;
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
    async fn incumbent_emits_exact_root_bound_receipt() {
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
        assert_eq!(receipt.placed.oracle_digest.len(), 64);
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
