//! Same-history diagnostics for row, columnar, and hybrid immutable object runs.

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};
use okv_object::{
    content_sha256, decode_full_row_object, encode_row_object_set, filesystem_backend,
    prefixed_backend, read_indexed_point, Backend, ObservedBackend, PointReadOutcome, RequestStats,
    RevisionToken, RowObjectManifestV1, RowObjectReference, RowRecord, RowSegmentIndex,
    WriteCondition,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

mod columnar_aligned;
mod columnar_overlay;
mod t28_aligned;
mod t28_typed;

pub use t28_aligned::{
    publish_t28_aligned_layout, T28AlignedFixtureV1, T28AlignedLayoutPlacementInput,
    T28AlignedLayoutPublication, T28AlignedLayoutReader, T28AlignedPointGatherSnapshot,
    T28AlignedScan, T28AlignedScanSnapshot, T28OpenedAlignedLayout,
};
pub use t28_typed::{
    derive_t28_typed_point_trace, publish_t28_typed_layout, t28_typed_layout_profile,
    t28_typed_point_outcome_sha256, T28ColumnarLayoutReader, T28ColumnarScan,
    T28ColumnarScanSnapshot, T28OpenedTypedLayout, T28RowLayoutReader,
    T28TypedLayoutExecutionPlanV1, T28TypedLayoutPlacementInput, T28TypedLayoutPublication,
    T28TypedPointOperationV1, T28TypedPointTraceV1, T28TypedSeedOrderV1,
};

const GENERATION: u64 = 11;
const FORMAT_VERSION: u16 = 1;
const CHECKSUM_BLOCK_BYTES: usize = 64 * 1_024;
const PARQUET_DATA_KEY: &str = "layout/data.parquet";
const PARQUET_INDEX_KEY: &str = "layout/data.parquet.index";
const PARQUET_MANIFEST_KEY: &str = "layout/manifest.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageLayoutMode {
    IndexedRowObjectControl,
    IndexedParquetControl,
    CoalescedParquetCandidate,
    SplitProjectionSidecarCandidate,
    HybridColumnarCandidate,
    ColumnarRangeOverlayCandidate,
    ParquetFullFilePointPoison,
    HybridAccountingPoison,
    ColumnarInvalidationPoison,
}

impl StorageLayoutMode {
    #[must_use]
    pub const fn subject(self) -> &'static str {
        match self {
            Self::IndexedRowObjectControl => "indexed_row_object_control",
            Self::IndexedParquetControl => "indexed_parquet_control",
            Self::CoalescedParquetCandidate => "coalesced_parquet_candidate",
            Self::SplitProjectionSidecarCandidate => "split_projection_sidecar_candidate",
            Self::HybridColumnarCandidate => "hybrid_columnar_candidate",
            Self::ColumnarRangeOverlayCandidate => "columnar_range_overlay_candidate",
            Self::ParquetFullFilePointPoison => "scan_complete_parquet_object_for_point",
            Self::HybridAccountingPoison => "omit_row_capsule_bytes",
            Self::ColumnarInvalidationPoison => "apply_predicate_before_invalidation",
        }
    }

    const fn is_hybrid(self) -> bool {
        matches!(
            self,
            Self::HybridColumnarCandidate
                | Self::HybridAccountingPoison
                | Self::ColumnarInvalidationPoison
        )
    }

    const fn is_projection_sidecar(self) -> bool {
        matches!(self, Self::SplitProjectionSidecarCandidate)
    }

    const fn coalesces_parquet_ranges(self) -> bool {
        matches!(
            self,
            Self::CoalescedParquetCandidate | Self::SplitProjectionSidecarCandidate
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParquetColumns {
    Full,
    Hybrid,
    AnalyticalProjection,
}

impl StorageLayoutMode {
    const fn parquet_columns(self) -> ParquetColumns {
        if self.is_projection_sidecar() {
            ParquetColumns::AnalyticalProjection
        } else if self.is_hybrid() {
            ParquetColumns::Hybrid
        } else {
            ParquetColumns::Full
        }
    }
}

#[derive(Clone, Debug)]
pub struct StorageLayoutProfile {
    pub key_count: u64,
    pub canonical_live_row_bytes: usize,
    pub opaque_payload_bytes: usize,
    pub base_version: u64,
    pub delta_cycles: u64,
    pub update_fraction: f64,
    pub delete_fraction: f64,
    pub point_operations: usize,
    pub target_run_object_bytes: usize,
    pub row_block_bytes: usize,
    pub columnar_block_rows: usize,
    pub overlay_cache_bytes: usize,
    pub seeds: Vec<u64>,
    pub repeats: u32,
}

#[derive(Clone, Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct StorageLayoutSample {
    pub seed: u64,
    pub repeat: u32,
    pub subject: String,
    pub canonical_history_sha256: String,
    pub post_compaction_sha256: String,
    pub point_operations: u64,
    pub point_anomalies: u64,
    pub scan_anomalies: u64,
    pub accounting_anomalies: u64,
    pub invalidation_anomalies: u64,
    pub point_latency_ns_p50: u64,
    pub point_latency_ns_p95: u64,
    pub point_latency_ns_p99: u64,
    pub point_requests: u64,
    pub point_full_object_requests: u64,
    pub point_response_bytes: u64,
    pub point_backend_elapsed_micros: u64,
    pub overlay_fill_requests: u64,
    pub overlay_fill_response_bytes: u64,
    pub overlay_resident_bytes: u64,
    pub overlay_capacity_bytes: u64,
    pub warm_point_operations: u64,
    pub warm_point_anomalies: u64,
    pub warm_point_latency_ns_p99: u64,
    pub warm_point_requests: u64,
    pub warm_point_response_bytes: u64,
    pub scan_requests: u64,
    pub scan_response_bytes: u64,
    pub scan_opaque_payload_bytes: u64,
    pub scan_backend_elapsed_micros: u64,
    pub scan_rows: u64,
    pub scan_seconds: f64,
    pub scan_rows_per_second: f64,
    pub manifest_bytes: u64,
    pub index_bytes: u64,
    pub data_bytes: u64,
    pub stored_bytes: u64,
    pub live_logical_bytes: u64,
    pub storage_amplification: f64,
    pub resident_index_bytes: u64,
    pub build_seconds: f64,
    pub build_rows_per_second: f64,
    pub compaction_written_bytes: u64,
    pub logical_history_bytes: u64,
    pub compaction_write_amplification: f64,
    pub branch_incremental_bytes: u64,
    pub branch_shared_bytes: u64,
    pub active_manifest_complete: bool,
    pub list_requests: u64,
    pub checksum_covered_ranges: bool,
    pub restart_requests: u64,
    pub restart_response_bytes: u64,
    pub restart_anomalies: u64,
    pub branch_reused_immutable_runs: bool,
    pub poison_detected: bool,
}

impl StorageLayoutSample {
    #[must_use]
    pub fn correctness_anomalies(&self) -> u64 {
        self.point_anomalies
            .saturating_add(self.scan_anomalies)
            .saturating_add(self.accounting_anomalies)
            .saturating_add(self.invalidation_anomalies)
            .saturating_add(self.warm_point_anomalies)
            .saturating_add(self.restart_anomalies)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageLayoutReport {
    pub subject: String,
    pub samples: Vec<StorageLayoutSample>,
}

impl StorageLayoutReport {
    #[must_use]
    pub fn correctness_anomalies(&self) -> u64 {
        self.samples
            .iter()
            .map(StorageLayoutSample::correctness_anomalies)
            .sum()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarDataFusionMode {
    Correct,
    PayloadPrefetchPoison,
}

impl ColumnarDataFusionMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PayloadPrefetchPoison => "payload_prefetch_poison",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnarDataFusionSample {
    pub seed: u64,
    pub repeat: u32,
    pub mode: String,
    pub canonical_history_sha256: String,
    pub trace_sha256: String,
    pub query_anomalies: u64,
    pub expected_groups: u64,
    pub result_groups: u64,
    pub source_rows: u64,
    pub source_stripes: u64,
    pub source_batches: u64,
    pub scan_plans: u64,
    pub projection_pushdown_plans: u64,
    pub peak_batch_rows: u64,
    pub peak_batch_bytes: u64,
    pub scan_fetch_target_bytes: u64,
    pub peak_fetch_bytes: u64,
    pub maximum_projection_stripe_bytes: u64,
    pub projection_fetch_requests: u64,
    pub object_requests: u64,
    pub full_object_requests: u64,
    pub object_response_bytes: u64,
    pub opaque_payload_requests: u64,
    pub opaque_payload_response_bytes: u64,
    pub list_requests: u64,
    pub query_seconds: f64,
    pub source_rows_per_second: f64,
    pub projection_bytes: u64,
    pub payload_bytes: u64,
    pub checksum_covered_ranges: bool,
    pub poison_detected: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnarDataFusionReport {
    pub mode: String,
    pub samples: Vec<ColumnarDataFusionSample>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarCacheAdmissionMode {
    FullAdmit,
    NeverAdmitControl,
    GhostTwoChance,
}

impl ColumnarCacheAdmissionMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::FullAdmit => "full_admit",
            Self::NeverAdmitControl => "never_admit_control",
            Self::GhostTwoChance => "ghost_two_chance",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnarCacheAdmissionSample {
    pub seed: u64,
    pub repeat: u32,
    pub mode: String,
    pub cache_ratio_percent: u32,
    pub zipf_alpha: f64,
    pub trace_sha256: String,
    pub point_operations: u64,
    pub point_anomalies: u64,
    pub pre_scan_hit_ratio: f64,
    pub post_scan_hit_ratio: f64,
    pub pre_scan_object_requests: u64,
    pub post_scan_object_requests: u64,
    pub pollution_object_requests: u64,
    pub pre_scan_response_bytes: u64,
    pub post_scan_response_bytes: u64,
    pub pollution_response_bytes: u64,
    pub resident_bytes: u64,
    pub capacity_bytes: u64,
    pub ghost_entries: u64,
    pub evictions: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColumnarCacheAdmissionReport {
    pub mode: String,
    pub cache_ratio_percent: u32,
    pub zipf_alpha: f64,
    pub samples: Vec<ColumnarCacheAdmissionSample>,
}

/// Run SQL directly over the C5 range-fetched projection stripes.
///
/// # Errors
///
/// Returns an error for invalid configuration, malformed media, object I/O,
/// Arrow conversion, `DataFusion` planning, or query execution failure.
pub fn run_columnar_datafusion_contract(
    mode: ColumnarDataFusionMode,
    profile: &StorageLayoutProfile,
) -> Result<ColumnarDataFusionReport, String> {
    run_columnar_datafusion_contract_with_scan_fetch(mode, profile, 0)
}

/// Run SQL over C5 stripes with bounded adjacent-stripe range coalescing.
///
/// A zero target preserves the one-request-per-stripe control.
///
/// # Errors
///
/// Returns the same errors as [`run_columnar_datafusion_contract`].
pub fn run_columnar_datafusion_contract_with_scan_fetch(
    mode: ColumnarDataFusionMode,
    profile: &StorageLayoutProfile,
    scan_fetch_target_bytes: usize,
) -> Result<ColumnarDataFusionReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create columnar DataFusion runtime: {error}"))?;
    let samples = runtime.block_on(async {
        let capacity = profile
            .seeds
            .len()
            .saturating_mul(usize::try_from(profile.repeats).unwrap_or(usize::MAX));
        let mut samples = Vec::with_capacity(capacity);
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                let scratch = TempDir::new()
                    .map_err(|error| format!("create columnar DataFusion scratch: {error}"))?;
                let backend =
                    filesystem_backend(scratch.path()).map_err(|error| error.to_string())?;
                let observed = Arc::new(ObservedBackend::new(backend));
                let history = LogicalHistory::generate(profile, *seed)?;
                samples.push(
                    columnar_overlay::run_columnar_datafusion_seed(
                        mode,
                        profile,
                        *seed,
                        repeat,
                        &history,
                        observed,
                        scan_fetch_target_bytes,
                    )
                    .await?,
                );
            }
        }
        Ok::<_, String>(samples)
    })?;
    Ok(ColumnarDataFusionReport {
        mode: mode.id().to_owned(),
        samples,
    })
}

/// Run one deterministic C5 cache-admission ablation around a scan-pollution
/// phase.
///
/// # Errors
///
/// Returns an error for invalid ratios, invalid Zipf alpha, malformed media,
/// object I/O, or an incorrect point result.
pub fn run_columnar_cache_admission_contract(
    mode: ColumnarCacheAdmissionMode,
    profile: &StorageLayoutProfile,
    cache_ratio_percent: u32,
    zipf_alpha: f64,
) -> Result<ColumnarCacheAdmissionReport, String> {
    validate_profile(profile)?;
    if !(1..=100).contains(&cache_ratio_percent) {
        return Err("cache ratio must be between 1 and 100 percent".to_owned());
    }
    if !zipf_alpha.is_finite() || !(0.5..=3.0).contains(&zipf_alpha) {
        return Err("Zipf alpha must be finite and between 0.5 and 3.0".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create columnar cache-admission runtime: {error}"))?;
    let samples = runtime.block_on(async {
        let capacity = profile
            .seeds
            .len()
            .saturating_mul(usize::try_from(profile.repeats).unwrap_or(usize::MAX));
        let mut samples = Vec::with_capacity(capacity);
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                let scratch = TempDir::new()
                    .map_err(|error| format!("create cache-admission scratch: {error}"))?;
                let backend =
                    filesystem_backend(scratch.path()).map_err(|error| error.to_string())?;
                let observed = Arc::new(ObservedBackend::new(backend));
                let history = LogicalHistory::generate(profile, *seed)?;
                samples.push(
                    columnar_overlay::run_columnar_cache_admission_seed(
                        mode,
                        profile,
                        *seed,
                        repeat,
                        &history,
                        observed,
                        cache_ratio_percent,
                        zipf_alpha,
                    )
                    .await?,
                );
            }
        }
        Ok::<_, String>(samples)
    })?;
    Ok(ColumnarCacheAdmissionReport {
        mode: mode.id().to_owned(),
        cache_ratio_percent,
        zipf_alpha,
        samples,
    })
}

/// Run one deterministic C5 cache-admission ablation against an externally
/// supplied object backend.
///
/// Every sample receives a unique namespace below `root_prefix` so repeated
/// modes, seeds, and repeats cannot collide in one remote bucket.
///
/// # Errors
///
/// Returns the same configuration, media, or object I/O errors as
/// [`run_columnar_cache_admission_contract`], or an error when `root_prefix`
/// is invalid.
pub fn run_columnar_cache_admission_contract_on_backend(
    mode: ColumnarCacheAdmissionMode,
    profile: &StorageLayoutProfile,
    cache_ratio_percent: u32,
    zipf_alpha: f64,
    backend: &Arc<dyn Backend>,
    root_prefix: &str,
) -> Result<ColumnarCacheAdmissionReport, String> {
    validate_profile(profile)?;
    if !(1..=100).contains(&cache_ratio_percent) {
        return Err("cache ratio must be between 1 and 100 percent".to_owned());
    }
    if !zipf_alpha.is_finite() || !(0.5..=3.0).contains(&zipf_alpha) {
        return Err("Zipf alpha must be finite and between 0.5 and 3.0".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create columnar cache-admission runtime: {error}"))?;
    let samples = runtime.block_on(async {
        let capacity = profile
            .seeds
            .len()
            .saturating_mul(usize::try_from(profile.repeats).unwrap_or(usize::MAX));
        let mut samples = Vec::with_capacity(capacity);
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                let namespace = format!(
                    "{}/{}/seed-{seed}/repeat-{repeat}",
                    root_prefix.trim_matches('/'),
                    mode.id()
                );
                let scoped = prefixed_backend(Arc::clone(backend), namespace)
                    .map_err(|error| error.to_string())?;
                let observed = Arc::new(ObservedBackend::new(scoped));
                let history = LogicalHistory::generate(profile, *seed)?;
                samples.push(
                    columnar_overlay::run_columnar_cache_admission_seed(
                        mode,
                        profile,
                        *seed,
                        repeat,
                        &history,
                        observed,
                        cache_ratio_percent,
                        zipf_alpha,
                    )
                    .await?,
                );
            }
        }
        Ok::<_, String>(samples)
    })?;
    Ok(ColumnarCacheAdmissionReport {
        mode: mode.id().to_owned(),
        cache_ratio_percent,
        zipf_alpha,
        samples,
    })
}

/// Run the frozen same-history storage-layout diagnostic.
///
/// # Errors
///
/// Returns an error for invalid configuration, object I/O, malformed immutable
/// media, Arrow or Parquet failure, or a runtime construction failure.
pub fn run_storage_layout_contract(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
) -> Result<StorageLayoutReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create storage-layout runtime: {error}"))?;
    let samples = runtime.block_on(async {
        let mut samples = Vec::with_capacity(profile.seeds.len());
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                samples.push(run_seed(mode, profile, *seed, repeat).await?);
            }
        }
        Ok::<_, String>(samples)
    })?;
    Ok(StorageLayoutReport {
        subject: mode.subject().to_owned(),
        samples,
    })
}

/// Run a candidate and baseline in alternating order for every seed and repeat.
///
/// # Errors
///
/// Returns the same configuration, media, or runtime errors as
/// [`run_storage_layout_contract`].
pub fn run_storage_layout_pair_contract(
    candidate: StorageLayoutMode,
    baseline: StorageLayoutMode,
    profile: &StorageLayoutProfile,
) -> Result<(StorageLayoutReport, StorageLayoutReport), String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create storage-layout runtime: {error}"))?;
    let (candidate_samples, baseline_samples) = runtime.block_on(async {
        let capacity = profile
            .seeds
            .len()
            .saturating_mul(usize::try_from(profile.repeats).unwrap_or(usize::MAX));
        let mut candidate_samples = Vec::with_capacity(capacity);
        let mut baseline_samples = Vec::with_capacity(capacity);
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                let candidate_first = (u64::from(repeat).saturating_add(*seed)) % 2 == 0;
                if candidate_first {
                    candidate_samples.push(run_seed(candidate, profile, *seed, repeat).await?);
                    baseline_samples.push(run_seed(baseline, profile, *seed, repeat).await?);
                } else {
                    baseline_samples.push(run_seed(baseline, profile, *seed, repeat).await?);
                    candidate_samples.push(run_seed(candidate, profile, *seed, repeat).await?);
                }
            }
        }
        Ok::<_, String>((candidate_samples, baseline_samples))
    })?;
    Ok((
        StorageLayoutReport {
            subject: candidate.subject().to_owned(),
            samples: candidate_samples,
        },
        StorageLayoutReport {
            subject: baseline.subject().to_owned(),
            samples: baseline_samples,
        },
    ))
}

/// Run an alternating candidate and baseline against an externally supplied backend.
///
/// Every sample receives a unique object-key namespace below `root_prefix`, so
/// candidate and baseline media cannot collide even when they share one bucket.
///
/// # Errors
///
/// Returns the same configuration, media, or runtime errors as
/// [`run_storage_layout_contract`], or an error when `root_prefix` is invalid.
pub fn run_storage_layout_pair_contract_on_backend(
    candidate: StorageLayoutMode,
    baseline: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    backend: &Arc<dyn Backend>,
    root_prefix: &str,
) -> Result<(StorageLayoutReport, StorageLayoutReport), String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create storage-layout runtime: {error}"))?;
    let (candidate_samples, baseline_samples) = runtime.block_on(async {
        let capacity = profile
            .seeds
            .len()
            .saturating_mul(usize::try_from(profile.repeats).unwrap_or(usize::MAX));
        let mut candidate_samples = Vec::with_capacity(capacity);
        let mut baseline_samples = Vec::with_capacity(capacity);
        for repeat in 1..=profile.repeats {
            for seed in &profile.seeds {
                let candidate_first = (u64::from(repeat).saturating_add(*seed)) % 2 == 0;
                if candidate_first {
                    candidate_samples.push(
                        run_seed_in_prefix(
                            candidate,
                            profile,
                            *seed,
                            repeat,
                            Arc::clone(backend),
                            root_prefix,
                        )
                        .await?,
                    );
                    baseline_samples.push(
                        run_seed_in_prefix(
                            baseline,
                            profile,
                            *seed,
                            repeat,
                            Arc::clone(backend),
                            root_prefix,
                        )
                        .await?,
                    );
                } else {
                    baseline_samples.push(
                        run_seed_in_prefix(
                            baseline,
                            profile,
                            *seed,
                            repeat,
                            Arc::clone(backend),
                            root_prefix,
                        )
                        .await?,
                    );
                    candidate_samples.push(
                        run_seed_in_prefix(
                            candidate,
                            profile,
                            *seed,
                            repeat,
                            Arc::clone(backend),
                            root_prefix,
                        )
                        .await?,
                    );
                }
            }
        }
        Ok::<_, String>((candidate_samples, baseline_samples))
    })?;
    Ok((
        StorageLayoutReport {
            subject: candidate.subject().to_owned(),
            samples: candidate_samples,
        },
        StorageLayoutReport {
            subject: baseline.subject().to_owned(),
            samples: baseline_samples,
        },
    ))
}

async fn run_seed(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
) -> Result<StorageLayoutSample, String> {
    let scratch =
        TempDir::new().map_err(|error| format!("create storage-layout scratch: {error}"))?;
    let backend = filesystem_backend(scratch.path()).map_err(|error| error.to_string())?;
    run_seed_on_backend(mode, profile, seed, repeat, backend).await
}

async fn run_seed_in_prefix(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    backend: Arc<dyn Backend>,
    root_prefix: &str,
) -> Result<StorageLayoutSample, String> {
    let namespace = format!(
        "{}/{}/seed-{seed}/repeat-{repeat}",
        root_prefix.trim_matches('/'),
        mode.subject()
    );
    let backend = prefixed_backend(backend, namespace).map_err(|error| error.to_string())?;
    run_seed_on_backend(mode, profile, seed, repeat, backend).await
}

async fn run_seed_on_backend(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    backend: Arc<dyn Backend>,
) -> Result<StorageLayoutSample, String> {
    let history = LogicalHistory::generate(profile, seed)?;
    let observed = Arc::new(ObservedBackend::new(backend));
    match mode {
        StorageLayoutMode::IndexedRowObjectControl => {
            run_row_seed(profile, seed, repeat, &history, observed).await
        }
        StorageLayoutMode::ColumnarRangeOverlayCandidate => {
            columnar_overlay::run_columnar_overlay_seed(profile, seed, repeat, &history, observed)
                .await
        }
        StorageLayoutMode::IndexedParquetControl
        | StorageLayoutMode::CoalescedParquetCandidate
        | StorageLayoutMode::SplitProjectionSidecarCandidate
        | StorageLayoutMode::HybridColumnarCandidate
        | StorageLayoutMode::ParquetFullFilePointPoison
        | StorageLayoutMode::HybridAccountingPoison
        | StorageLayoutMode::ColumnarInvalidationPoison => {
            run_parquet_seed(mode, profile, seed, repeat, &history, observed).await
        }
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
struct LogicalHistory {
    records: Vec<RowRecord>,
    base_records: Vec<RowRecord>,
    delta_records: Vec<Vec<RowRecord>>,
    by_key: BTreeMap<u64, Vec<RowRecord>>,
    canonical_sha256: String,
    logical_history_bytes: u64,
}

impl LogicalHistory {
    fn generate(profile: &StorageLayoutProfile, seed: u64) -> Result<Self, String> {
        let mut by_key = BTreeMap::new();
        let mut base_records = Vec::with_capacity(as_usize(profile.key_count)?);
        let mut live = vec![true; as_usize(profile.key_count)?];
        for key in 0..profile.key_count {
            let value = canonical_value(profile, seed, key, profile.base_version)?;
            let record = RowRecord::value(key.to_be_bytes(), profile.base_version, value);
            base_records.push(record.clone());
            by_key.insert(key, vec![record]);
        }

        let mut delta_records = Vec::with_capacity(as_usize(profile.delta_cycles)?);
        for cycle in 1..=profile.delta_cycles {
            let version = profile
                .base_version
                .checked_add(cycle)
                .ok_or_else(|| "storage-layout version overflow".to_owned())?;
            let mut delta = Vec::new();
            for key in 0..profile.key_count {
                let index = as_usize(key)?;
                if !live[index] {
                    continue;
                }
                let delete_score = unit_interval(mix(seed, key, version, 0xd3));
                if delete_score < profile.delete_fraction {
                    let record = RowRecord::tombstone(key.to_be_bytes(), version);
                    by_key.entry(key).or_default().push(record.clone());
                    delta.push(record);
                    live[index] = false;
                    continue;
                }
                let update_score = unit_interval(mix(seed, key, version, 0xa7));
                if update_score < profile.update_fraction {
                    let value = canonical_value(profile, seed, key, version)?;
                    let record = RowRecord::value(key.to_be_bytes(), version, value);
                    by_key.entry(key).or_default().push(record.clone());
                    delta.push(record);
                }
            }
            delta_records.push(delta);
        }
        for versions in by_key.values_mut() {
            versions.sort_by(|left, right| right.version.cmp(&left.version));
        }
        let records = by_key.values().flatten().cloned().collect::<Vec<_>>();
        let canonical_sha256 = logical_digest(&records);
        let logical_history_bytes = records.iter().fold(0_u64, |total, record| {
            total.saturating_add(logical_record_bytes(record))
        });
        Ok(Self {
            records,
            base_records,
            delta_records,
            by_key,
            canonical_sha256,
            logical_history_bytes,
        })
    }

    fn visible(&self, key: u64, read_version: u64) -> Option<&RowRecord> {
        self.by_key
            .get(&key)?
            .iter()
            .find(|record| record.version <= read_version)
    }

    fn final_rows(&self, read_version: u64) -> Vec<ProjectedRow> {
        self.by_key
            .iter()
            .filter_map(|(key, versions)| {
                let record = versions
                    .iter()
                    .find(|record| record.version <= read_version)?;
                let value = record.value.as_ref()?;
                let fields = ValueFields::decode(value).ok()?;
                Some(ProjectedRow {
                    key: *key,
                    tenant: fields.tenant,
                    category: fields.category,
                    quantity: fields.quantity,
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectedRow {
    key: u64,
    tenant: u32,
    category: u16,
    quantity: i64,
}

#[derive(Clone, Debug)]
struct ValueFields {
    tenant: u32,
    category: u16,
    flags: u16,
    quantity: i64,
    updated_version: u64,
    checksum: u64,
    payload: Vec<u8>,
}

impl ValueFields {
    fn encode(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(32 + self.payload.len());
        value.extend_from_slice(&self.tenant.to_be_bytes());
        value.extend_from_slice(&self.category.to_be_bytes());
        value.extend_from_slice(&self.flags.to_be_bytes());
        value.extend_from_slice(&self.quantity.to_be_bytes());
        value.extend_from_slice(&self.updated_version.to_be_bytes());
        value.extend_from_slice(&self.checksum.to_be_bytes());
        value.extend_from_slice(&self.payload);
        value
    }

    fn decode(value: &[u8]) -> Result<Self, String> {
        if value.len() < 32 {
            return Err("canonical row value is truncated".to_owned());
        }
        Ok(Self {
            tenant: u32::from_be_bytes(array(&value[0..4])?),
            category: u16::from_be_bytes(array(&value[4..6])?),
            flags: u16::from_be_bytes(array(&value[6..8])?),
            quantity: i64::from_be_bytes(array(&value[8..16])?),
            updated_version: u64::from_be_bytes(array(&value[16..24])?),
            checksum: u64::from_be_bytes(array(&value[24..32])?),
            payload: value[32..].to_vec(),
        })
    }
}

fn canonical_value(
    profile: &StorageLayoutProfile,
    seed: u64,
    key: u64,
    version: u64,
) -> Result<Vec<u8>, String> {
    let tenant = u32::try_from(key % 32).map_err(|error| error.to_string())?;
    let category = u16::try_from(key % 64).map_err(|error| error.to_string())?;
    let flags = u16::try_from((key ^ version) & 0xffff).map_err(|error| error.to_string())?;
    let base_quantity = i64::try_from(key % 10_000).map_err(|error| error.to_string())?;
    let version_delta = i64::try_from(version).map_err(|error| error.to_string())?;
    let quantity = if mix(seed, key, version, 0x51) & 1 == 0 {
        base_quantity.saturating_add(version_delta)
    } else {
        base_quantity.saturating_sub(version_delta)
    };
    let checksum = mix(seed, key, version, 0xc5);
    let mut state = checksum;
    let mut payload = Vec::with_capacity(profile.opaque_payload_bytes);
    while payload.len() < profile.opaque_payload_bytes {
        state = splitmix64(state);
        let remaining = profile.opaque_payload_bytes - payload.len();
        payload.extend_from_slice(&state.to_be_bytes()[..remaining.min(8)]);
    }
    let value = ValueFields {
        tenant,
        category,
        flags,
        quantity,
        updated_version: version,
        checksum,
        payload,
    }
    .encode();
    if value.len() != profile.canonical_live_row_bytes {
        return Err(format!(
            "canonical row encoded {} bytes, expected {}",
            value.len(),
            profile.canonical_live_row_bytes
        ));
    }
    Ok(value)
}

#[allow(clippy::too_many_lines)]
async fn run_row_seed(
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    history: &LogicalHistory,
    observed: Arc<ObservedBackend>,
) -> Result<StorageLayoutSample, String> {
    let backend: Arc<dyn Backend> = observed.clone();
    let build_started = Instant::now();
    let prepared = prepare_row_layout(profile, history, backend.as_ref()).await?;
    let build_seconds = build_started.elapsed().as_secs_f64();

    observed.clear_stats();
    let point_keys = operation_keys(profile.key_count, profile.point_operations, seed);
    let mut point_latencies = Vec::with_capacity(point_keys.len());
    let mut point_anomalies = 0_u64;
    for (key, read_version) in &point_keys {
        let started = Instant::now();
        let expected = expected_outcome(history.visible(*key, *read_version));
        let actual = if let Some(reference) = prepared.manifest.locate(&key.to_be_bytes()) {
            let index = prepared
                .indexes
                .get(&reference.data_key)
                .ok_or_else(|| "row manifest selected an uncached index".to_owned())?;
            read_indexed_point(
                backend.as_ref(),
                &reference.data_key,
                None,
                index,
                &key.to_be_bytes(),
                *read_version,
            )
            .await?
            .outcome
        } else {
            PointReadOutcome::Absent
        };
        point_latencies.push(elapsed_ns(started));
        if actual != expected {
            point_anomalies = point_anomalies.saturating_add(1);
        }
    }
    let point_stats = observed.stats();

    observed.clear_stats();
    let scan_started = Instant::now();
    let mut scanned_records = Vec::new();
    for reference in &prepared.manifest.segments {
        let read = backend
            .get(&reference.data_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let index = prepared
            .indexes
            .get(&reference.data_key)
            .ok_or_else(|| "row scan could not locate a cached index".to_owned())?;
        scanned_records.extend(decode_full_row_object(&read.bytes, index)?);
    }
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_stats = observed.stats();
    let final_version = profile.base_version.saturating_add(profile.delta_cycles);
    let expected_projection = history.final_rows(final_version);
    let actual_projection = project_snapshot(&scanned_records, final_version)?;
    let scan_anomalies = u64::from(scanned_records != history.records)
        .saturating_add(u64::from(actual_projection != expected_projection));

    let compaction_written_bytes = row_compaction_written_bytes(profile, history)?;
    let stored_bytes = prepared
        .data_bytes
        .saturating_add(prepared.index_bytes)
        .saturating_add(prepared.manifest_bytes);
    let live_logical_bytes = live_logical_bytes(profile, expected_projection.len())?;
    let branch_manifest = branch_manifest_bytes(
        "indexed_row_object_control",
        &prepared.manifest_sha256,
        stored_bytes,
    )?;
    let operations = u64::try_from(point_keys.len()).unwrap_or(u64::MAX);
    let rows = u64::try_from(expected_projection.len()).unwrap_or(u64::MAX);
    let stats = latency_summary(&mut point_latencies);
    Ok(StorageLayoutSample {
        seed,
        repeat,
        subject: StorageLayoutMode::IndexedRowObjectControl
            .subject()
            .to_owned(),
        canonical_history_sha256: history.canonical_sha256.clone(),
        post_compaction_sha256: logical_digest(&scanned_records),
        point_operations: operations,
        point_anomalies,
        scan_anomalies,
        accounting_anomalies: 0,
        invalidation_anomalies: 0,
        point_latency_ns_p50: stats.0,
        point_latency_ns_p95: stats.1,
        point_latency_ns_p99: stats.2,
        point_requests: successful_requests(&point_stats, &["get.range", "get"]),
        point_full_object_requests: successful_requests(&point_stats, &["get"]),
        point_response_bytes: response_bytes(&point_stats),
        point_backend_elapsed_micros: elapsed_micros(&point_stats),
        overlay_fill_requests: 0,
        overlay_fill_response_bytes: 0,
        overlay_resident_bytes: 0,
        overlay_capacity_bytes: 0,
        warm_point_operations: 0,
        warm_point_anomalies: 0,
        warm_point_latency_ns_p99: 0,
        warm_point_requests: 0,
        warm_point_response_bytes: 0,
        scan_requests: successful_requests(&scan_stats, &["get.range", "get"]),
        scan_response_bytes: response_bytes(&scan_stats),
        scan_opaque_payload_bytes: response_bytes(&scan_stats),
        scan_backend_elapsed_micros: elapsed_micros(&scan_stats),
        scan_rows: rows,
        scan_seconds,
        scan_rows_per_second: rate(rows, scan_seconds),
        manifest_bytes: prepared.manifest_bytes,
        index_bytes: prepared.index_bytes,
        data_bytes: prepared.data_bytes,
        stored_bytes,
        live_logical_bytes,
        storage_amplification: ratio(stored_bytes, live_logical_bytes),
        resident_index_bytes: prepared.index_bytes.saturating_add(prepared.manifest_bytes),
        build_seconds,
        build_rows_per_second: rate(
            u64::try_from(history.records.len()).unwrap_or(u64::MAX),
            build_seconds,
        ),
        compaction_written_bytes,
        logical_history_bytes: history.logical_history_bytes,
        compaction_write_amplification: ratio(
            compaction_written_bytes,
            history.logical_history_bytes,
        ),
        branch_incremental_bytes: u64::try_from(branch_manifest.len()).unwrap_or(u64::MAX),
        branch_shared_bytes: stored_bytes,
        active_manifest_complete: prepared.active_manifest_complete,
        list_requests: successful_requests(&point_stats, &["list"])
            .saturating_add(successful_requests(&scan_stats, &["list"])),
        checksum_covered_ranges: true,
        restart_requests: 0,
        restart_response_bytes: 0,
        restart_anomalies: 0,
        branch_reused_immutable_runs: true,
        poison_detected: false,
    })
}

struct PreparedRowLayout {
    manifest: RowObjectManifestV1,
    manifest_sha256: String,
    indexes: BTreeMap<String, RowSegmentIndex>,
    manifest_bytes: u64,
    index_bytes: u64,
    data_bytes: u64,
    active_manifest_complete: bool,
}

async fn prepare_row_layout(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    backend: &dyn Backend,
) -> Result<PreparedRowLayout, String> {
    let encoded = encode_row_object_set(
        GENERATION,
        &history.records,
        profile.target_run_object_bytes,
        profile.row_block_bytes,
    )?;
    let references = encoded
        .iter()
        .map(|segment| RowObjectReference::from_encoded("layout/row", segment))
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = RowObjectManifestV1::new(
        GENERATION,
        profile.base_version.saturating_add(profile.delta_cycles),
        references,
    )?;
    let manifest_encoded = manifest.encode()?;
    let manifest_sha256 = content_sha256(&manifest_encoded);
    let mut indexes = BTreeMap::new();
    let mut index_bytes = 0_u64;
    let mut data_bytes = 0_u64;
    for (segment, reference) in encoded.into_iter().zip(&manifest.segments) {
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
        let index = RowSegmentIndex::decode(&segment.index)?;
        reference.validate_index(&segment.index, &index)?;
        data_bytes = data_bytes.saturating_add(reference.data_bytes);
        index_bytes = index_bytes.saturating_add(reference.index_bytes);
        indexes.insert(reference.data_key.clone(), index);
    }
    backend
        .put(
            "layout/row/active-manifest",
            Bytes::from(manifest_encoded.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(PreparedRowLayout {
        manifest,
        manifest_sha256,
        indexes,
        manifest_bytes: u64::try_from(manifest_encoded.len()).unwrap_or(u64::MAX),
        index_bytes,
        data_bytes,
        active_manifest_complete: true,
    })
}

fn row_compaction_written_bytes(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
) -> Result<u64, String> {
    let mut total = row_media_bytes(profile, &history.base_records)?;
    for delta in &history.delta_records {
        if !delta.is_empty() {
            total = total.saturating_add(row_media_bytes(profile, delta)?);
        }
    }
    total = total.saturating_add(row_media_bytes(profile, &history.records)?);
    Ok(total)
}

fn row_media_bytes(profile: &StorageLayoutProfile, records: &[RowRecord]) -> Result<u64, String> {
    let media = row_media_breakdown(profile, records)?;
    Ok(media
        .manifest_bytes
        .saturating_add(media.index_bytes)
        .saturating_add(media.data_bytes))
}

struct RowMediaBreakdown {
    manifest_bytes: u64,
    manifest_sha256: String,
    index_bytes: u64,
    data_bytes: u64,
}

fn row_media_breakdown(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
) -> Result<RowMediaBreakdown, String> {
    let encoded = encode_row_object_set(
        GENERATION,
        records,
        profile.target_run_object_bytes,
        profile.row_block_bytes,
    )?;
    let references = encoded
        .iter()
        .map(|segment| RowObjectReference::from_encoded("layout/measure", segment))
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = RowObjectManifestV1::new(
        GENERATION,
        records
            .iter()
            .map(|record| record.version)
            .max()
            .unwrap_or(1),
        references,
    )?
    .encode()?;
    Ok(RowMediaBreakdown {
        manifest_bytes: u64::try_from(manifest.len()).unwrap_or(u64::MAX),
        manifest_sha256: content_sha256(&manifest),
        index_bytes: encoded.iter().fold(0_u64, |total, segment| {
            total.saturating_add(u64::try_from(segment.index.len()).unwrap_or(u64::MAX))
        }),
        data_bytes: encoded.iter().fold(0_u64, |total, segment| {
            total.saturating_add(u64::try_from(segment.data.len()).unwrap_or(u64::MAX))
        }),
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ParquetFence {
    first_key: u64,
    last_key: u64,
    row_group: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ParquetAccessIndex {
    format_version: u16,
    object_length: u64,
    object_sha256: String,
    checksum_block_bytes: usize,
    block_sha256: Vec<String>,
    fences: Vec<ParquetFence>,
}

impl ParquetAccessIndex {
    fn new(bytes: &[u8], fences: Vec<ParquetFence>) -> Self {
        let block_sha256 = bytes
            .chunks(CHECKSUM_BLOCK_BYTES)
            .map(content_sha256)
            .collect();
        Self {
            format_version: FORMAT_VERSION,
            object_length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            object_sha256: content_sha256(bytes),
            checksum_block_bytes: CHECKSUM_BLOCK_BYTES,
            block_sha256,
            fences,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.format_version != FORMAT_VERSION
            || self.object_length == 0
            || self.checksum_block_bytes != CHECKSUM_BLOCK_BYTES
            || self.block_sha256.is_empty()
            || self.fences.is_empty()
        {
            return Err("invalid Parquet access-index header".to_owned());
        }
        for (position, fence) in self.fences.iter().enumerate() {
            if fence.first_key > fence.last_key || fence.row_group != position {
                return Err("invalid Parquet access-index fence".to_owned());
            }
            if let Some(previous) = position
                .checked_sub(1)
                .and_then(|index| self.fences.get(index))
            {
                if previous.last_key >= fence.first_key {
                    return Err("Parquet access-index fences overlap or regress".to_owned());
                }
            }
        }
        Ok(())
    }

    fn locate(&self, key: u64) -> Option<usize> {
        let mut lower = 0_usize;
        let mut upper = self.fences.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.fences[middle].first_key <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let fence = lower
            .checked_sub(1)
            .and_then(|index| self.fences.get(index))?;
        (key <= fence.last_key).then_some(fence.row_group)
    }

    fn expanded_range(&self, requested: Range<u64>) -> Result<Range<u64>, String> {
        if requested.start >= requested.end || requested.end > self.object_length {
            return Err("Parquet reader requested an invalid byte range".to_owned());
        }
        let block = u64::try_from(self.checksum_block_bytes).unwrap_or(u64::MAX);
        let start = requested.start / block * block;
        let end = requested.end.saturating_add(block.saturating_sub(1)) / block * block;
        Ok(start..end.min(self.object_length))
    }

    fn verify_expanded(&self, range: Range<u64>, bytes: &[u8]) -> Result<(), String> {
        let expected_length = range.end.saturating_sub(range.start);
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_length {
            return Err("Parquet checksum range length mismatch".to_owned());
        }
        let block = u64::try_from(self.checksum_block_bytes).unwrap_or(u64::MAX);
        let first_block = as_usize(range.start / block)?;
        for (offset, chunk) in bytes.chunks(self.checksum_block_bytes).enumerate() {
            let expected = self
                .block_sha256
                .get(first_block.saturating_add(offset))
                .ok_or_else(|| "Parquet checksum block is absent".to_owned())?;
            if content_sha256(chunk) != *expected {
                return Err("Parquet checksum block mismatch".to_owned());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LayoutManifest {
    format_version: u16,
    generation: u64,
    covered_through: u64,
    layout: String,
    data_key: String,
    data_bytes: u64,
    data_sha256: String,
    index_key: String,
    index_bytes: u64,
    index_sha256: String,
    sidecar_manifest_key: Option<String>,
    sidecar_manifest_bytes: u64,
    sidecar_manifest_sha256: Option<String>,
    sidecar_data_bytes: u64,
    sidecar_index_bytes: u64,
    capabilities: Vec<String>,
}

impl LayoutManifest {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let payload = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut encoded = b"OKVML1".to_vec();
        encoded.extend_from_slice(&payload);
        let checksum = Sha256::digest(&encoded);
        encoded.extend_from_slice(&checksum);
        Ok(encoded)
    }
}

struct PreparedParquetLayout {
    metadata: ArrowReaderMetadata,
    access_index: Arc<ParquetAccessIndex>,
    revision: RevisionToken,
    sidecar: Option<PreparedRowLayout>,
    manifest_sha256: String,
    manifest_bytes: u64,
    index_bytes: u64,
    data_bytes: u64,
    capsule_logical_bytes: u64,
    active_manifest_complete: bool,
    full_range_requests: Arc<AtomicU64>,
}

fn parquet_schema(columns: ParquetColumns) -> SchemaRef {
    let mut fields = vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("operation", DataType::UInt8, false),
        Field::new("tenant", DataType::UInt32, true),
        Field::new("category", DataType::UInt16, true),
        Field::new("quantity", DataType::Int64, true),
    ];
    if columns != ParquetColumns::AnalyticalProjection {
        fields.extend([
            Field::new("flags", DataType::UInt16, true),
            Field::new("updated_version", DataType::UInt64, true),
            Field::new("checksum", DataType::UInt64, true),
            Field::new("payload", DataType::Binary, true),
        ]);
    }
    if columns == ParquetColumns::Hybrid {
        fields.push(Field::new("row_capsule", DataType::Binary, true));
    }
    Arc::new(Schema::new(fields))
}

fn parquet_batch(records: &[RowRecord], columns: ParquetColumns) -> Result<RecordBatch, String> {
    let decoded = records
        .iter()
        .map(|record| {
            record
                .value
                .as_ref()
                .map(|value| ValueFields::decode(value))
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key_values = records
        .iter()
        .map(|record| key_u64(&record.key))
        .collect::<Result<Vec<_>, _>>()?;
    let keys = UInt64Array::from(key_values);
    let versions = UInt64Array::from_iter_values(records.iter().map(|record| record.version));
    let operations = UInt8Array::from_iter_values(
        records
            .iter()
            .map(|record| u8::from(record.value.is_some())),
    );
    let tenants = UInt32Array::from(
        decoded
            .iter()
            .map(|value| value.as_ref().map(|row| row.tenant))
            .collect::<Vec<_>>(),
    );
    let categories = UInt16Array::from(
        decoded
            .iter()
            .map(|value| value.as_ref().map(|row| row.category))
            .collect::<Vec<_>>(),
    );
    let quantities = Int64Array::from(
        decoded
            .iter()
            .map(|value| value.as_ref().map(|row| row.quantity))
            .collect::<Vec<_>>(),
    );
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(keys),
        Arc::new(versions),
        Arc::new(operations),
        Arc::new(tenants),
        Arc::new(categories),
        Arc::new(quantities),
    ];
    if columns != ParquetColumns::AnalyticalProjection {
        arrays.extend([
            Arc::new(UInt16Array::from(
                decoded
                    .iter()
                    .map(|value| value.as_ref().map(|row| row.flags))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                decoded
                    .iter()
                    .map(|value| value.as_ref().map(|row| row.updated_version))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                decoded
                    .iter()
                    .map(|value| value.as_ref().map(|row| row.checksum))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(BinaryArray::from(
                decoded
                    .iter()
                    .map(|value| value.as_ref().map(|row| row.payload.as_slice()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ]);
    }
    if columns == ParquetColumns::Hybrid {
        let capsule_values = records
            .iter()
            .map(|record| record.value.as_deref())
            .collect::<Vec<_>>();
        arrays.push(Arc::new(BinaryArray::from(capsule_values)));
    }
    RecordBatch::try_new(parquet_schema(columns), arrays).map_err(|error| error.to_string())
}

fn row_group_ranges(
    records: &[RowRecord],
    target_rows: usize,
) -> Result<Vec<Range<usize>>, String> {
    if records.is_empty() || target_rows == 0 {
        return Err("invalid Parquet row-group input".to_owned());
    }
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let group_start = cursor;
        let key = records[cursor].key.as_slice();
        while cursor < records.len() && records[cursor].key.as_slice() == key {
            cursor += 1;
        }
        if group_start > start && cursor.saturating_sub(start) > target_rows {
            ranges.push(start..group_start);
            start = group_start;
        }
    }
    ranges.push(start..records.len());
    Ok(ranges)
}

fn encode_parquet_layout(
    records: &[RowRecord],
    columns: ParquetColumns,
    target_rows: usize,
) -> Result<(Vec<u8>, ArrowReaderMetadata, ParquetAccessIndex, u64), String> {
    let ranges = row_group_ranges(records, target_rows)?;
    let mut bytes = Vec::new();
    let parquet_metadata = {
        let mut writer = ArrowWriter::try_new(&mut bytes, parquet_schema(columns), None)
            .map_err(|error| error.to_string())?;
        for range in &ranges {
            writer
                .write(&parquet_batch(&records[range.clone()], columns)?)
                .map_err(|error| error.to_string())?;
            writer.flush().map_err(|error| error.to_string())?;
        }
        writer.close().map_err(|error| error.to_string())?
    };
    let fences = ranges
        .iter()
        .enumerate()
        .map(|(row_group, range)| {
            Ok(ParquetFence {
                first_key: key_u64(&records[range.start].key)?,
                last_key: key_u64(&records[range.end - 1].key)?,
                row_group,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let access_index = ParquetAccessIndex::new(&bytes, fences);
    access_index.validate()?;
    if parquet_metadata.num_row_groups() != access_index.fences.len() {
        return Err("Parquet row groups differ from primary fences".to_owned());
    }
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), ArrowReaderOptions::default())
            .map_err(|error| error.to_string())?;
    let capsule_logical_bytes = if columns == ParquetColumns::Hybrid {
        records.iter().fold(0_u64, |total, record| {
            total.saturating_add(
                record
                    .value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            )
        })
    } else {
        0
    };
    Ok((bytes, metadata, access_index, capsule_logical_bytes))
}

async fn prepare_parquet_layout(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    backend: &dyn Backend,
) -> Result<PreparedParquetLayout, String> {
    let columns = mode.parquet_columns();
    let (bytes, metadata, access_index, capsule_logical_bytes) =
        encode_parquet_layout(&history.records, columns, profile.columnar_block_rows)?;
    let index_encoded = serde_json::to_vec(&access_index).map_err(|error| error.to_string())?;
    let sidecar = if mode.is_projection_sidecar() {
        Some(prepare_row_layout(profile, history, backend).await?)
    } else {
        None
    };
    let layout = match mode {
        StorageLayoutMode::CoalescedParquetCandidate => "coalesced_parquet",
        StorageLayoutMode::SplitProjectionSidecarCandidate => "split_projection_sidecar",
        _ if mode.is_hybrid() => "hybrid_columnar",
        _ => "indexed_parquet",
    };
    let manifest = LayoutManifest {
        format_version: FORMAT_VERSION,
        generation: GENERATION,
        covered_through: profile.base_version.saturating_add(profile.delta_cycles),
        layout: layout.to_owned(),
        data_key: PARQUET_DATA_KEY.to_owned(),
        data_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        data_sha256: content_sha256(&bytes),
        index_key: PARQUET_INDEX_KEY.to_owned(),
        index_bytes: u64::try_from(index_encoded.len()).unwrap_or(u64::MAX),
        index_sha256: content_sha256(&index_encoded),
        sidecar_manifest_key: sidecar
            .as_ref()
            .map(|_| "layout/row/active-manifest".to_owned()),
        sidecar_manifest_bytes: sidecar.as_ref().map_or(0, |row| row.manifest_bytes),
        sidecar_manifest_sha256: sidecar.as_ref().map(|row| row.manifest_sha256.clone()),
        sidecar_data_bytes: sidecar.as_ref().map_or(0, |row| row.data_bytes),
        sidecar_index_bytes: sidecar.as_ref().map_or(0, |row| row.index_bytes),
        capabilities: vec![
            "point_get".to_owned(),
            "ordered_scan".to_owned(),
            "typed_projection".to_owned(),
            "mixed_version_read".to_owned(),
        ],
    };
    let manifest_encoded = manifest.encode()?;
    let active_manifest_complete = sidecar
        .as_ref()
        .is_none_or(|row| row.active_manifest_complete);
    let revision = backend
        .put(PARQUET_DATA_KEY, Bytes::from(bytes), WriteCondition::Create)
        .await
        .map_err(|error| error.to_string())?;
    backend
        .put(
            PARQUET_INDEX_KEY,
            Bytes::from(index_encoded.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    backend
        .put(
            PARQUET_MANIFEST_KEY,
            Bytes::from(manifest_encoded.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(PreparedParquetLayout {
        metadata,
        access_index: Arc::new(access_index),
        revision,
        sidecar,
        manifest_sha256: content_sha256(&manifest_encoded),
        manifest_bytes: u64::try_from(manifest_encoded.len())
            .unwrap_or(u64::MAX)
            .saturating_add(manifest.sidecar_manifest_bytes),
        index_bytes: u64::try_from(index_encoded.len())
            .unwrap_or(u64::MAX)
            .saturating_add(manifest.sidecar_index_bytes),
        data_bytes: manifest
            .data_bytes
            .saturating_add(manifest.sidecar_data_bytes),
        capsule_logical_bytes,
        active_manifest_complete,
        full_range_requests: Arc::new(AtomicU64::new(0)),
    })
}

struct ChecksummedParquetReader {
    backend: Arc<dyn Backend>,
    data_key: String,
    revision: RevisionToken,
    metadata: ArrowReaderMetadata,
    access_index: Arc<ParquetAccessIndex>,
    coalesce_ranges: bool,
    full_range_requests: Arc<AtomicU64>,
}

impl ChecksummedParquetReader {
    fn new(
        backend: Arc<dyn Backend>,
        revision: RevisionToken,
        metadata: ArrowReaderMetadata,
        access_index: Arc<ParquetAccessIndex>,
        coalesce_ranges: bool,
        full_range_requests: Arc<AtomicU64>,
    ) -> Self {
        Self {
            backend,
            data_key: PARQUET_DATA_KEY.to_owned(),
            revision,
            metadata,
            access_index,
            coalesce_ranges,
            full_range_requests,
        }
    }
}

impl AsyncFileReader for ChecksummedParquetReader {
    fn get_bytes(&mut self, requested: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let expanded = self
                .access_index
                .expanded_range(requested.clone())
                .map_err(ParquetError::General)?;
            if expanded.start == 0 && expanded.end == self.access_index.object_length {
                self.full_range_requests.fetch_add(1, Ordering::Relaxed);
            }
            let read = self
                .backend
                .get(&self.data_key, Some(expanded.clone()), Some(&self.revision))
                .await
                .map_err(|error| ParquetError::General(error.to_string()))?;
            self.access_index
                .verify_expanded(expanded.clone(), &read.bytes)
                .map_err(ParquetError::General)?;
            let start = as_usize(requested.start.saturating_sub(expanded.start))
                .map_err(ParquetError::General)?;
            let end = start
                .checked_add(
                    as_usize(requested.end.saturating_sub(requested.start))
                        .map_err(ParquetError::General)?,
                )
                .ok_or_else(|| ParquetError::General("Parquet slice overflow".to_owned()))?;
            Ok(read.bytes.slice(start..end))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        requested: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            if !self.coalesce_ranges {
                let mut result = Vec::with_capacity(requested.len());
                for range in requested {
                    result.push(self.get_bytes(range).await?);
                }
                return Ok(result);
            }
            if requested.is_empty() {
                return Ok(Vec::new());
            }
            let expanded = requested
                .iter()
                .map(|range| self.access_index.expanded_range(range.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ParquetError::General)?;
            let start = expanded
                .iter()
                .map(|range| range.start)
                .min()
                .ok_or_else(|| ParquetError::General("missing coalesced range start".to_owned()))?;
            let end = expanded
                .iter()
                .map(|range| range.end)
                .max()
                .ok_or_else(|| ParquetError::General("missing coalesced range end".to_owned()))?;
            let union = start..end;
            if union.start == 0 && union.end == self.access_index.object_length {
                self.full_range_requests.fetch_add(1, Ordering::Relaxed);
            }
            let read = self
                .backend
                .get(&self.data_key, Some(union.clone()), Some(&self.revision))
                .await
                .map_err(|error| ParquetError::General(error.to_string()))?;
            self.access_index
                .verify_expanded(union.clone(), &read.bytes)
                .map_err(ParquetError::General)?;
            requested
                .into_iter()
                .map(|range| {
                    let slice_start = as_usize(range.start.saturating_sub(union.start))
                        .map_err(ParquetError::General)?;
                    let slice_end = slice_start
                        .checked_add(
                            as_usize(range.end.saturating_sub(range.start))
                                .map_err(ParquetError::General)?,
                        )
                        .ok_or_else(|| {
                            ParquetError::General("coalesced Parquet slice overflow".to_owned())
                        })?;
                    Ok(read.bytes.slice(slice_start..slice_end))
                })
                .collect()
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(self.metadata.metadata());
        async move { Ok(metadata) }.boxed()
    }
}

#[allow(clippy::too_many_lines)]
async fn run_parquet_seed(
    mode: StorageLayoutMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    history: &LogicalHistory,
    observed: Arc<ObservedBackend>,
) -> Result<StorageLayoutSample, String> {
    let backend: Arc<dyn Backend> = observed.clone();
    let build_started = Instant::now();
    let prepared = prepare_parquet_layout(mode, profile, history, backend.as_ref()).await?;
    let build_seconds = build_started.elapsed().as_secs_f64();

    observed.clear_stats();
    prepared.full_range_requests.store(0, Ordering::Relaxed);
    let point_count = if mode == StorageLayoutMode::ParquetFullFilePointPoison {
        1
    } else {
        profile.point_operations
    };
    let point_keys = operation_keys(profile.key_count, point_count, seed);
    let mut point_latencies = Vec::with_capacity(point_keys.len());
    let mut point_anomalies = 0_u64;
    for (key, read_version) in &point_keys {
        let started = Instant::now();
        let expected = expected_outcome(history.visible(*key, *read_version));
        let actual = if mode == StorageLayoutMode::ParquetFullFilePointPoison {
            parquet_full_file_point(
                backend.as_ref(),
                &prepared,
                *key,
                *read_version,
                mode.is_hybrid(),
            )
            .await?
        } else if mode.is_projection_sidecar() {
            sidecar_indexed_point(
                backend.as_ref(),
                prepared
                    .sidecar
                    .as_ref()
                    .ok_or_else(|| "projection sidecar is absent".to_owned())?,
                *key,
                *read_version,
            )
            .await?
        } else {
            parquet_indexed_point(
                Arc::clone(&backend),
                &prepared,
                *key,
                *read_version,
                mode.is_hybrid(),
                mode.coalesces_parquet_ranges(),
            )
            .await?
        };
        point_latencies.push(elapsed_ns(started));
        if actual != expected {
            point_anomalies = point_anomalies.saturating_add(1);
        }
    }
    let point_stats = observed.stats();
    let point_full_range_requests = prepared.full_range_requests.load(Ordering::Relaxed);

    observed.clear_stats();
    let scan_started = Instant::now();
    let projected = parquet_projected_scan(
        Arc::clone(&backend),
        &prepared,
        mode.coalesces_parquet_ranges(),
    )
    .await?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_stats = observed.stats();
    let final_version = profile.base_version.saturating_add(profile.delta_cycles);
    let expected_projection = history.final_rows(final_version);
    let scan_anomalies = u64::from(projected != expected_projection);
    let invalidation_anomalies = if mode == StorageLayoutMode::ColumnarInvalidationPoison {
        invalidation_poison_anomalies(history, profile)
    } else {
        0
    };

    let compaction_written_bytes = parquet_compaction_written_bytes(profile, history, mode)?;
    let actual_stored_bytes = prepared
        .data_bytes
        .saturating_add(prepared.index_bytes)
        .saturating_add(prepared.manifest_bytes);
    let reported_stored_bytes = if mode == StorageLayoutMode::HybridAccountingPoison {
        actual_stored_bytes.saturating_sub(prepared.capsule_logical_bytes)
    } else {
        actual_stored_bytes
    };
    let accounting_anomalies = u64::from(reported_stored_bytes != actual_stored_bytes);
    let live_logical_bytes = live_logical_bytes(profile, expected_projection.len())?;
    let branch_manifest = branch_manifest_bytes(
        mode.subject(),
        &prepared.manifest_sha256,
        actual_stored_bytes,
    )?;
    let point_full_object_requests =
        successful_requests(&point_stats, &["get"]).saturating_add(point_full_range_requests);
    let poison_detected = match mode {
        StorageLayoutMode::ParquetFullFilePointPoison => point_full_object_requests > 0,
        StorageLayoutMode::HybridAccountingPoison => accounting_anomalies > 0,
        StorageLayoutMode::ColumnarInvalidationPoison => invalidation_anomalies > 0,
        _ => false,
    };
    let stats = latency_summary(&mut point_latencies);
    let operations = u64::try_from(point_keys.len()).unwrap_or(u64::MAX);
    let rows = u64::try_from(expected_projection.len()).unwrap_or(u64::MAX);
    Ok(StorageLayoutSample {
        seed,
        repeat,
        subject: mode.subject().to_owned(),
        canonical_history_sha256: history.canonical_sha256.clone(),
        post_compaction_sha256: history.canonical_sha256.clone(),
        point_operations: operations,
        point_anomalies,
        scan_anomalies,
        accounting_anomalies,
        invalidation_anomalies,
        point_latency_ns_p50: stats.0,
        point_latency_ns_p95: stats.1,
        point_latency_ns_p99: stats.2,
        point_requests: successful_requests(&point_stats, &["get.range", "get"]),
        point_full_object_requests,
        point_response_bytes: response_bytes(&point_stats),
        point_backend_elapsed_micros: elapsed_micros(&point_stats),
        overlay_fill_requests: 0,
        overlay_fill_response_bytes: 0,
        overlay_resident_bytes: 0,
        overlay_capacity_bytes: 0,
        warm_point_operations: 0,
        warm_point_anomalies: 0,
        warm_point_latency_ns_p99: 0,
        warm_point_requests: 0,
        warm_point_response_bytes: 0,
        scan_requests: successful_requests(&scan_stats, &["get.range", "get"]),
        scan_response_bytes: response_bytes(&scan_stats),
        scan_opaque_payload_bytes: if mode.is_projection_sidecar() {
            0
        } else {
            response_bytes(&scan_stats)
        },
        scan_backend_elapsed_micros: elapsed_micros(&scan_stats),
        scan_rows: rows,
        scan_seconds,
        scan_rows_per_second: rate(rows, scan_seconds),
        manifest_bytes: prepared.manifest_bytes,
        index_bytes: prepared.index_bytes,
        data_bytes: prepared.data_bytes,
        stored_bytes: reported_stored_bytes,
        live_logical_bytes,
        storage_amplification: ratio(reported_stored_bytes, live_logical_bytes),
        resident_index_bytes: prepared.index_bytes.saturating_add(prepared.manifest_bytes),
        build_seconds,
        build_rows_per_second: rate(
            u64::try_from(history.records.len()).unwrap_or(u64::MAX),
            build_seconds,
        ),
        compaction_written_bytes,
        logical_history_bytes: history.logical_history_bytes,
        compaction_write_amplification: ratio(
            compaction_written_bytes,
            history.logical_history_bytes,
        ),
        branch_incremental_bytes: u64::try_from(branch_manifest.len()).unwrap_or(u64::MAX),
        branch_shared_bytes: actual_stored_bytes,
        active_manifest_complete: prepared.active_manifest_complete,
        list_requests: successful_requests(&point_stats, &["list"])
            .saturating_add(successful_requests(&scan_stats, &["list"])),
        checksum_covered_ranges: true,
        restart_requests: 0,
        restart_response_bytes: 0,
        restart_anomalies: 0,
        branch_reused_immutable_runs: true,
        poison_detected,
    })
}

async fn parquet_indexed_point(
    backend: Arc<dyn Backend>,
    prepared: &PreparedParquetLayout,
    key: u64,
    read_version: u64,
    hybrid: bool,
    coalesce_ranges: bool,
) -> Result<PointReadOutcome, String> {
    let Some(row_group) = prepared.access_index.locate(key) else {
        return Ok(PointReadOutcome::Absent);
    };
    let reader = ChecksummedParquetReader::new(
        backend,
        prepared.revision.clone(),
        prepared.metadata.clone(),
        Arc::clone(&prepared.access_index),
        coalesce_ranges,
        Arc::clone(&prepared.full_range_requests),
    );
    let columns: &[&str] = if hybrid {
        &["key", "version", "operation", "row_capsule"]
    } else {
        &[
            "key",
            "version",
            "operation",
            "tenant",
            "category",
            "flags",
            "quantity",
            "updated_version",
            "checksum",
            "payload",
        ]
    };
    let projection =
        ProjectionMask::columns(prepared.metadata.parquet_schema(), columns.iter().copied());
    let mut stream =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, prepared.metadata.clone())
            .with_row_groups(vec![row_group])
            .with_projection(projection)
            .with_batch_size(2_048)
            .build()
            .map_err(|error| error.to_string())?;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        if let Some(outcome) = point_from_batch(&batch, key, read_version, hybrid)? {
            return Ok(outcome);
        }
    }
    Ok(PointReadOutcome::Absent)
}

async fn sidecar_indexed_point(
    backend: &dyn Backend,
    sidecar: &PreparedRowLayout,
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    let key_bytes = key.to_be_bytes();
    let Some(reference) = sidecar.manifest.locate(&key_bytes) else {
        return Ok(PointReadOutcome::Absent);
    };
    let index = sidecar
        .indexes
        .get(&reference.data_key)
        .ok_or_else(|| "sidecar manifest selected an uncached index".to_owned())?;
    Ok(read_indexed_point(
        backend,
        &reference.data_key,
        None,
        index,
        &key_bytes,
        read_version,
    )
    .await?
    .outcome)
}

async fn parquet_full_file_point(
    backend: &dyn Backend,
    prepared: &PreparedParquetLayout,
    key: u64,
    read_version: u64,
    hybrid: bool,
) -> Result<PointReadOutcome, String> {
    let read = backend
        .get(PARQUET_DATA_KEY, None, Some(&prepared.revision))
        .await
        .map_err(|error| error.to_string())?;
    if content_sha256(&read.bytes) != prepared.access_index.object_sha256 {
        return Err("complete Parquet object digest mismatch".to_owned());
    }
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(read.bytes.clone())
            .map_err(|error| error.to_string())?;
    let mut reader = builder
        .with_batch_size(2_048)
        .build()
        .map_err(|error| error.to_string())?;
    for batch in &mut reader {
        let batch = batch.map_err(|error| error.to_string())?;
        if let Some(outcome) = point_from_batch(&batch, key, read_version, hybrid)? {
            return Ok(outcome);
        }
    }
    Ok(PointReadOutcome::Absent)
}

fn point_from_batch(
    batch: &RecordBatch,
    key: u64,
    read_version: u64,
    hybrid: bool,
) -> Result<Option<PointReadOutcome>, String> {
    let keys = column::<UInt64Array>(batch, "key")?;
    let versions = column::<UInt64Array>(batch, "version")?;
    let operations = column::<UInt8Array>(batch, "operation")?;
    for row in 0..batch.num_rows() {
        if keys.value(row) != key || versions.value(row) > read_version {
            continue;
        }
        if operations.value(row) == 0 {
            return Ok(Some(PointReadOutcome::Tombstone));
        }
        let value = if hybrid {
            let capsules = column::<BinaryArray>(batch, "row_capsule")?;
            capsules.value(row).to_vec()
        } else {
            reconstruct_parquet_value(batch, row)?
        };
        return Ok(Some(PointReadOutcome::Value(Bytes::from(value))));
    }
    Ok(None)
}

fn reconstruct_parquet_value(batch: &RecordBatch, row: usize) -> Result<Vec<u8>, String> {
    let fields = ValueFields {
        tenant: column::<UInt32Array>(batch, "tenant")?.value(row),
        category: column::<UInt16Array>(batch, "category")?.value(row),
        flags: column::<UInt16Array>(batch, "flags")?.value(row),
        quantity: column::<Int64Array>(batch, "quantity")?.value(row),
        updated_version: column::<UInt64Array>(batch, "updated_version")?.value(row),
        checksum: column::<UInt64Array>(batch, "checksum")?.value(row),
        payload: column::<BinaryArray>(batch, "payload")?.value(row).to_vec(),
    };
    Ok(fields.encode())
}

async fn parquet_projected_scan(
    backend: Arc<dyn Backend>,
    prepared: &PreparedParquetLayout,
    coalesce_ranges: bool,
) -> Result<Vec<ProjectedRow>, String> {
    let reader = ChecksummedParquetReader::new(
        backend,
        prepared.revision.clone(),
        prepared.metadata.clone(),
        Arc::clone(&prepared.access_index),
        coalesce_ranges,
        Arc::clone(&prepared.full_range_requests),
    );
    let projection = ProjectionMask::columns(
        prepared.metadata.parquet_schema(),
        [
            "key",
            "version",
            "operation",
            "tenant",
            "category",
            "quantity",
        ],
    );
    let mut stream =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, prepared.metadata.clone())
            .with_projection(projection)
            .with_batch_size(4_096)
            .build()
            .map_err(|error| error.to_string())?;
    let mut projected = Vec::new();
    let mut previous_key = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        let keys = column::<UInt64Array>(&batch, "key")?;
        let operations = column::<UInt8Array>(&batch, "operation")?;
        let tenants = column::<UInt32Array>(&batch, "tenant")?;
        let categories = column::<UInt16Array>(&batch, "category")?;
        let quantities = column::<Int64Array>(&batch, "quantity")?;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if previous_key == Some(key) {
                continue;
            }
            previous_key = Some(key);
            if operations.value(row) == 0 {
                continue;
            }
            projected.push(ProjectedRow {
                key,
                tenant: tenants.value(row),
                category: categories.value(row),
                quantity: quantities.value(row),
            });
        }
    }
    Ok(projected)
}

fn parquet_compaction_written_bytes(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    mode: StorageLayoutMode,
) -> Result<u64, String> {
    let mut total = parquet_media_bytes(profile, &history.base_records, mode)?;
    for delta in &history.delta_records {
        if !delta.is_empty() {
            total = total.saturating_add(parquet_media_bytes(profile, delta, mode)?);
        }
    }
    total = total.saturating_add(parquet_media_bytes(profile, &history.records, mode)?);
    Ok(total)
}

fn parquet_media_bytes(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
    mode: StorageLayoutMode,
) -> Result<u64, String> {
    let (bytes, _, index, _) =
        encode_parquet_layout(records, mode.parquet_columns(), profile.columnar_block_rows)?;
    let index_bytes = serde_json::to_vec(&index).map_err(|error| error.to_string())?;
    let sidecar = if mode.is_projection_sidecar() {
        Some(row_media_breakdown(profile, records)?)
    } else {
        None
    };
    let manifest = LayoutManifest {
        format_version: FORMAT_VERSION,
        generation: GENERATION,
        covered_through: records
            .iter()
            .map(|record| record.version)
            .max()
            .unwrap_or(1),
        layout: mode.subject().to_owned(),
        data_key: PARQUET_DATA_KEY.to_owned(),
        data_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        data_sha256: content_sha256(&bytes),
        index_key: PARQUET_INDEX_KEY.to_owned(),
        index_bytes: u64::try_from(index_bytes.len()).unwrap_or(u64::MAX),
        index_sha256: content_sha256(&index_bytes),
        sidecar_manifest_key: sidecar
            .as_ref()
            .map(|_| "layout/row/active-manifest".to_owned()),
        sidecar_manifest_bytes: sidecar.as_ref().map_or(0, |row| row.manifest_bytes),
        sidecar_manifest_sha256: sidecar.as_ref().map(|row| row.manifest_sha256.clone()),
        sidecar_data_bytes: sidecar.as_ref().map_or(0, |row| row.data_bytes),
        sidecar_index_bytes: sidecar.as_ref().map_or(0, |row| row.index_bytes),
        capabilities: Vec::new(),
    }
    .encode()?;
    Ok(u64::try_from(bytes.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(index_bytes.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(manifest.len()).unwrap_or(u64::MAX))
        .saturating_add(sidecar.as_ref().map_or(0, |row| {
            row.manifest_bytes
                .saturating_add(row.index_bytes)
                .saturating_add(row.data_bytes)
        })))
}

fn invalidation_poison_anomalies(history: &LogicalHistory, profile: &StorageLayoutProfile) -> u64 {
    let final_version = profile.base_version.saturating_add(profile.delta_cycles);
    let expected = history
        .final_rows(final_version)
        .into_iter()
        .filter(|row| row.quantity & 1 == 0)
        .collect::<Vec<_>>();
    let mut poisoned = history
        .final_rows(profile.base_version)
        .into_iter()
        .filter(|row| row.quantity & 1 == 0)
        .map(|row| (row.key, row))
        .collect::<BTreeMap<_, _>>();
    for delta in &history.delta_records {
        for record in delta {
            let Ok(key) = key_u64(&record.key) else {
                return 1;
            };
            let Some(value) = &record.value else {
                continue;
            };
            let Ok(fields) = ValueFields::decode(value) else {
                return 1;
            };
            if fields.quantity & 1 == 0 {
                poisoned.insert(
                    key,
                    ProjectedRow {
                        key,
                        tenant: fields.tenant,
                        category: fields.category,
                        quantity: fields.quantity,
                    },
                );
            }
        }
    }
    u64::from(poisoned.into_values().collect::<Vec<_>>() != expected)
}

fn project_snapshot(records: &[RowRecord], read_version: u64) -> Result<Vec<ProjectedRow>, String> {
    let mut projected = Vec::new();
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let key = key_u64(&records[cursor].key)?;
        let mut visible = None;
        while cursor < records.len() && key_u64(&records[cursor].key)? == key {
            if visible.is_none() && records[cursor].version <= read_version {
                visible = Some(&records[cursor]);
            }
            cursor += 1;
        }
        let Some(value) = visible.and_then(|record| record.value.as_ref()) else {
            continue;
        };
        let fields = ValueFields::decode(value)?;
        projected.push(ProjectedRow {
            key,
            tenant: fields.tenant,
            category: fields.category,
            quantity: fields.quantity,
        });
    }
    Ok(projected)
}

fn expected_outcome(record: Option<&RowRecord>) -> PointReadOutcome {
    record.map_or(PointReadOutcome::Absent, |record| {
        record
            .value
            .as_ref()
            .map_or(PointReadOutcome::Tombstone, |value| {
                PointReadOutcome::Value(Bytes::copy_from_slice(value))
            })
    })
}

fn operation_keys(key_count: u64, count: usize, seed: u64) -> Vec<(u64, u64)> {
    let mut state = seed;
    (0..count)
        .map(|operation| {
            state = splitmix64(state ^ u64::try_from(operation).unwrap_or(u64::MAX));
            let key = state % key_count;
            state = splitmix64(state);
            let read_version = 1 + state % 5;
            (key, read_version)
        })
        .collect()
}

fn logical_digest(records: &[RowRecord]) -> String {
    let mut digest = Sha256::new();
    for record in records {
        digest.update(
            u64::try_from(record.key.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(&record.key);
        digest.update(record.version.to_be_bytes());
        match &record.value {
            Some(value) => {
                digest.update([1]);
                digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
                digest.update(value);
            }
            None => digest.update([0]),
        }
    }
    format!("{:x}", digest.finalize())
}

fn logical_record_bytes(record: &RowRecord) -> u64 {
    u64::try_from(record.key.len())
        .unwrap_or(u64::MAX)
        .saturating_add(8 + 1)
        .saturating_add(
            record
                .value
                .as_ref()
                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
        )
}

fn live_logical_bytes(profile: &StorageLayoutProfile, live_rows: usize) -> Result<u64, String> {
    let bytes_per_row = 8_usize
        .checked_add(profile.canonical_live_row_bytes)
        .ok_or_else(|| "live logical bytes overflow".to_owned())?;
    u64::try_from(
        live_rows
            .checked_mul(bytes_per_row)
            .ok_or_else(|| "live logical bytes overflow".to_owned())?,
    )
    .map_err(|error| error.to_string())
}

fn branch_manifest_bytes(
    layout: &str,
    parent_manifest_sha256: &str,
    shared_bytes: u64,
) -> Result<Vec<u8>, String> {
    #[derive(Serialize)]
    struct BranchManifest<'a> {
        format_version: u16,
        branch: &'a str,
        parent_manifest_sha256: &'a str,
        shared_bytes: u64,
    }
    serde_json::to_vec(&BranchManifest {
        format_version: FORMAT_VERSION,
        branch: layout,
        parent_manifest_sha256,
        shared_bytes,
    })
    .map_err(|error| error.to_string())
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, String> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|error| error.to_string())?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("column {name} has the wrong Arrow type"))
}

fn successful_requests(stats: &RequestStats, apis: &[&str]) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.result == "ok" && apis.contains(&request.api.as_str()))
        .map(|request| request.count)
        .sum()
}

fn response_bytes(stats: &RequestStats) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.result == "ok")
        .map(|request| request.response_bytes)
        .sum()
}

fn elapsed_micros(stats: &RequestStats) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.result == "ok")
        .fold(0_u64, |total, request| {
            total.saturating_add(request.elapsed_micros)
        })
}

fn latency_summary(latencies: &mut [u64]) -> (u64, u64, u64) {
    latencies.sort_unstable();
    (
        percentile(latencies, 50),
        percentile(latencies, 95),
        percentile(latencies, 99),
    )
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = values.len().saturating_mul(percentile).saturating_add(99) / 100;
    values[index.saturating_sub(1).min(values.len() - 1)]
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}

#[allow(clippy::cast_precision_loss)]
fn rate(count: u64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        return 0.0;
    }
    count as f64 / seconds
}

fn key_u64(key: &[u8]) -> Result<u64, String> {
    Ok(u64::from_be_bytes(array(key)?))
}

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], String> {
    bytes
        .try_into()
        .map_err(|_| format!("expected {N} bytes, received {}", bytes.len()))
}

fn as_usize(value: u64) -> Result<usize, String> {
    usize::try_from(value).map_err(|error| error.to_string())
}

#[allow(clippy::cast_precision_loss)]
fn unit_interval(value: u64) -> f64 {
    value as f64 / u64::MAX as f64
}

fn mix(seed: u64, key: u64, version: u64, salt: u64) -> u64 {
    splitmix64(seed ^ key.rotate_left(17) ^ version.rotate_left(37) ^ salt)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_profile(profile: &StorageLayoutProfile) -> Result<(), String> {
    if profile.key_count == 0
        || profile.canonical_live_row_bytes != 32 + profile.opaque_payload_bytes
        || profile.base_version == 0
        || profile.delta_cycles != 4
        || !(0.0..1.0).contains(&profile.update_fraction)
        || !(0.0..1.0).contains(&profile.delete_fraction)
        || profile.point_operations == 0
        || profile.target_run_object_bytes < profile.row_block_bytes
        || profile.row_block_bytes < 4_096
        || profile.columnar_block_rows == 0
        || profile.overlay_cache_bytes == 0
        || profile.repeats == 0
        || profile.seeds.is_empty()
    {
        return Err("invalid storage-layout profile".to_owned());
    }
    if profile.overlay_cache_bytes < columnar_overlay::minimum_overlay_cache_bytes(profile)? {
        return Err("storage-layout overlay cache cannot hold one physical fetch unit".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        run_columnar_cache_admission_contract, run_columnar_cache_admission_contract_on_backend,
        run_columnar_datafusion_contract, run_columnar_datafusion_contract_with_scan_fetch,
        run_storage_layout_contract, run_storage_layout_pair_contract_on_backend,
        ColumnarCacheAdmissionMode, ColumnarDataFusionMode, StorageLayoutMode,
        StorageLayoutProfile, ValueFields,
    };
    use okv_object::memory_backend;

    fn profile() -> StorageLayoutProfile {
        StorageLayoutProfile {
            key_count: 256,
            canonical_live_row_bytes: 512,
            opaque_payload_bytes: 480,
            base_version: 1,
            delta_cycles: 4,
            update_fraction: 0.125,
            delete_fraction: 0.01,
            point_operations: 64,
            target_run_object_bytes: 65_536,
            row_block_bytes: 4_096,
            columnar_block_rows: 64,
            overlay_cache_bytes: 1_048_576,
            seeds: vec![5701],
            repeats: 1,
        }
    }

    #[test]
    fn canonical_value_round_trips() {
        let value = ValueFields {
            tenant: 7,
            category: 9,
            flags: 11,
            quantity: -13,
            updated_version: 17,
            checksum: 19,
            payload: vec![0x5a; 32],
        }
        .encode();
        assert_eq!(ValueFields::decode(&value).expect("decode").encode(), value);
    }

    #[test]
    fn row_parquet_and_hybrid_preserve_one_history() {
        let row =
            run_storage_layout_contract(StorageLayoutMode::IndexedRowObjectControl, &profile())
                .expect("row diagnostic");
        let parquet =
            run_storage_layout_contract(StorageLayoutMode::IndexedParquetControl, &profile())
                .expect("Parquet diagnostic");
        let coalesced =
            run_storage_layout_contract(StorageLayoutMode::CoalescedParquetCandidate, &profile())
                .expect("coalesced Parquet diagnostic");
        let sidecar = run_storage_layout_contract(
            StorageLayoutMode::SplitProjectionSidecarCandidate,
            &profile(),
        )
        .expect("split projection sidecar diagnostic");
        let hybrid =
            run_storage_layout_contract(StorageLayoutMode::HybridColumnarCandidate, &profile())
                .expect("hybrid diagnostic");
        let columnar_overlay = run_storage_layout_contract(
            StorageLayoutMode::ColumnarRangeOverlayCandidate,
            &profile(),
        )
        .expect("columnar range-overlay diagnostic");
        assert_eq!(row.correctness_anomalies(), 0);
        assert_eq!(parquet.correctness_anomalies(), 0);
        assert_eq!(coalesced.correctness_anomalies(), 0);
        assert_eq!(sidecar.correctness_anomalies(), 0);
        assert_eq!(hybrid.correctness_anomalies(), 0);
        assert_eq!(columnar_overlay.correctness_anomalies(), 0);
        assert_eq!(
            row.samples[0].canonical_history_sha256,
            parquet.samples[0].canonical_history_sha256
        );
        assert_eq!(
            row.samples[0].canonical_history_sha256,
            coalesced.samples[0].canonical_history_sha256
        );
        assert_eq!(
            row.samples[0].canonical_history_sha256,
            sidecar.samples[0].canonical_history_sha256
        );
        assert_eq!(
            row.samples[0].canonical_history_sha256,
            hybrid.samples[0].canonical_history_sha256
        );
        assert_eq!(
            row.samples[0].canonical_history_sha256,
            columnar_overlay.samples[0].canonical_history_sha256
        );
        assert_eq!(parquet.samples[0].point_full_object_requests, 0);
        assert_eq!(coalesced.samples[0].point_full_object_requests, 0);
        assert_eq!(sidecar.samples[0].point_full_object_requests, 0);
        assert_eq!(hybrid.samples[0].point_full_object_requests, 0);
        assert_eq!(columnar_overlay.samples[0].point_full_object_requests, 0);
        assert_eq!(columnar_overlay.samples[0].warm_point_requests, 0);
        assert_eq!(columnar_overlay.samples[0].scan_opaque_payload_bytes, 0);
        assert_eq!(columnar_overlay.samples[0].restart_anomalies, 0);
        assert!(
            coalesced.samples[0].point_requests < parquet.samples[0].point_requests,
            "coalescing should remove redundant object requests"
        );
        assert_eq!(
            sidecar.samples[0].point_requests,
            row.samples[0].point_requests
        );
    }

    #[test]
    fn negative_controls_are_detected() {
        for mode in [
            StorageLayoutMode::ParquetFullFilePointPoison,
            StorageLayoutMode::HybridAccountingPoison,
            StorageLayoutMode::ColumnarInvalidationPoison,
        ] {
            let report = run_storage_layout_contract(mode, &profile()).expect("negative control");
            assert!(report.samples[0].poison_detected, "{mode:?}");
        }
    }

    #[test]
    fn datafusion_reads_projection_stripes_without_payload_pages() {
        let report = run_columnar_datafusion_contract(ColumnarDataFusionMode::Correct, &profile())
            .expect("columnar DataFusion source");
        let sample = &report.samples[0];
        assert_eq!(sample.query_anomalies, 0);
        assert_eq!(sample.opaque_payload_requests, 0);
        assert_eq!(sample.full_object_requests, 0);
        assert_eq!(sample.object_requests, sample.source_stripes);
        assert_eq!(sample.source_batches, sample.source_stripes);
        assert!(sample.projection_pushdown_plans > 0);
        assert!(sample.peak_batch_rows <= 64);

        let coalesced = run_columnar_datafusion_contract_with_scan_fetch(
            ColumnarDataFusionMode::Correct,
            &profile(),
            32_768,
        )
        .expect("coalesced columnar DataFusion source");
        let coalesced = &coalesced.samples[0];
        assert_eq!(coalesced.query_anomalies, 0);
        assert!(coalesced.projection_fetch_requests < coalesced.source_stripes);
        assert_eq!(coalesced.object_response_bytes, coalesced.projection_bytes);
        assert!(coalesced.peak_fetch_bytes <= 32_768);

        let poison = run_columnar_datafusion_contract(
            ColumnarDataFusionMode::PayloadPrefetchPoison,
            &profile(),
        )
        .expect("payload prefetch poison");
        assert!(poison.samples[0].poison_detected);
        assert!(poison.samples[0].opaque_payload_requests > 0);
    }

    #[test]
    fn ghost_admission_preserves_hot_points_across_scan_pollution() {
        let full = run_columnar_cache_admission_contract(
            ColumnarCacheAdmissionMode::FullAdmit,
            &profile(),
            20,
            1.4,
        )
        .expect("full-admit cache subject");
        let discard = run_columnar_cache_admission_contract(
            ColumnarCacheAdmissionMode::NeverAdmitControl,
            &profile(),
            20,
            1.4,
        )
        .expect("never-admit control");
        let ghost = run_columnar_cache_admission_contract(
            ColumnarCacheAdmissionMode::GhostTwoChance,
            &profile(),
            20,
            1.4,
        )
        .expect("ghost two-chance subject");
        let full = &full.samples[0];
        let discard = &discard.samples[0];
        let ghost = &ghost.samples[0];
        assert_eq!(full.point_anomalies, 0);
        assert_eq!(discard.point_anomalies, 0);
        assert_eq!(ghost.point_anomalies, 0);
        assert!(ghost.post_scan_hit_ratio > discard.post_scan_hit_ratio);
        assert!(ghost.post_scan_object_requests < discard.post_scan_object_requests);
        assert!(ghost.resident_bytes <= ghost.capacity_bytes);
        assert!(ghost.ghost_entries > 0);
    }

    #[test]
    fn external_cache_admission_backend_uses_an_isolated_namespace() {
        let backend = memory_backend();
        let report = run_columnar_cache_admission_contract_on_backend(
            ColumnarCacheAdmissionMode::GhostTwoChance,
            &profile(),
            20,
            1.4,
            &backend,
            "objectkv/evals/columnar-cache/test-run",
        )
        .expect("external cache-admission backend");
        assert_eq!(report.samples.len(), 1);
        assert_eq!(report.samples[0].point_anomalies, 0);
        assert!(report.samples[0].resident_bytes <= report.samples[0].capacity_bytes);
    }

    #[test]
    fn external_backend_pair_uses_isolated_namespaces() {
        let (candidate, baseline) = run_storage_layout_pair_contract_on_backend(
            StorageLayoutMode::SplitProjectionSidecarCandidate,
            StorageLayoutMode::IndexedRowObjectControl,
            &profile(),
            &memory_backend(),
            "objectkv/evals/storage-layout/test-run",
        )
        .expect("external-backend pair");

        assert_eq!(candidate.correctness_anomalies(), 0);
        assert_eq!(baseline.correctness_anomalies(), 0);
        assert_eq!(
            candidate.samples[0].canonical_history_sha256,
            baseline.samples[0].canonical_history_sha256
        );
    }
}
