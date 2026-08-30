//! Fresh-process point positions for the RFC-0048 matched layout curve.

use crate::provider_attempt::{
    scope_logical_operation, ProviderAttemptBackend, ProviderAttemptEventV1, ProviderAttemptPhase,
};
use crate::storage_layout::{
    t28_typed_point_outcome_sha256, T28AlignedFixtureV1, T28AlignedLayoutReader, T28AlignedScan,
    T28ColumnarLayoutReader, T28ColumnarScan, T28OpenedAlignedLayout, T28OpenedTypedLayout,
    T28RowLayoutReader, T28TypedLayoutExecutionPlanV1,
};
use crate::t28_layout::{
    T28LayoutOracleV1, TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1,
    TypedLayoutPlacementLocatorV1, TypedLayoutSubjectV1,
};
use arrow::array::{Int64Array, UInt16Array, UInt32Array, UInt64Array};
use datafusion::prelude::SessionContext;
use futures_util::{stream, StreamExt};
use okv_object::{content_sha256, Backend, PointReadOutcome};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RECEIPT_SCHEMA_VERSION: u32 = 1;
const ALIGNED_POINT_RECEIPT_SCHEMA_VERSION: u32 = 2;

/// One subject in the matched point lane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28TypedPointSubjectV1 {
    C0IndexedRow,
    C5ColumnarMain,
    C5v2AlignedColumnar,
}

/// One subject in the matched projected-scan lane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28TypedScanSubjectV1 {
    C0IndexedRow,
    C5ColumnarMain,
    C5v2AlignedColumnar,
}

impl T28TypedScanSubjectV1 {
    fn id(self) -> &'static str {
        match self {
            Self::C0IndexedRow => "c0_indexed_row_scan",
            Self::C5ColumnarMain => "c5_columnar_main_scan",
            Self::C5v2AlignedColumnar => "c5v2_aligned_columnar_scan",
        }
    }
}

impl T28TypedPointSubjectV1 {
    fn id(self) -> &'static str {
        match self {
            Self::C0IndexedRow => "c0_indexed_row",
            Self::C5ColumnarMain => "c5_columnar_main",
            Self::C5v2AlignedColumnar => "c5v2_aligned_columnar",
        }
    }

    fn maximum_requests_per_point(self) -> u64 {
        match self {
            Self::C0IndexedRow => 1,
            Self::C5ColumnarMain | Self::C5v2AlignedColumnar => 2,
        }
    }
}

enum PointReader {
    C0(Arc<T28RowLayoutReader>),
    C5(Arc<T28ColumnarLayoutReader>),
    C5v2(Arc<T28AlignedLayoutReader>),
}

impl PointReader {
    async fn read(&self, key: u64, version: u64) -> Result<PointReadOutcome, String> {
        match self {
            Self::C0(reader) => reader.point(key, version).await.map(|read| read.outcome),
            Self::C5(reader) => reader.point(key, version).await,
            Self::C5v2(reader) => reader.point(key, version).await,
        }
    }

    fn point_gather_snapshot(&self) -> (u64, u64) {
        match self {
            Self::C5v2(reader) => {
                let snapshot = reader.point_gather_snapshot();
                (snapshot.point_pairs, snapshot.overlapping_point_pairs)
            }
            Self::C0(_) | Self::C5(_) => (0, 0),
        }
    }
}

struct MeasuredPoint {
    ordinal: u64,
    latency_nanos: u64,
    outcome_sha256: String,
    expected_outcome_sha256: String,
}

/// Immutable output from one fresh C0 or C5 point process.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28TypedPointPositionReceiptV1 {
    pub schema_version: u32,
    pub execution_plan_sha256: String,
    pub fixture_id: String,
    pub root_sha256: String,
    pub subject: T28TypedPointSubjectV1,
    pub trace_seed: u64,
    pub measured_operations: u64,
    pub concurrent_tasks: u64,
    pub warmup_canary_reads: u64,
    pub resident_metadata_bytes: u64,
    pub measured_provider_attempts: u64,
    pub measured_response_bytes: u64,
    pub maximum_point_bytes_upper_bound: u64,
    pub maximum_attempts_per_point: u64,
    pub full_object_requests: u64,
    pub list_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
    pub missing_expected_generation_requests: u64,
    pub returned_generation_mismatches: u64,
    pub provider_errors: u64,
    pub correctness_anomalies: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub point_pairs: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub overlapping_point_pairs: u64,
    pub latency_nanos: Vec<u64>,
    pub p50_latency_nanos: u64,
    pub p95_latency_nanos: u64,
    pub p99_latency_nanos: u64,
    pub p999_latency_nanos: u64,
    pub provider_latency_nanos: Vec<u64>,
    pub provider_p50_latency_nanos: u64,
    pub provider_p95_latency_nanos: u64,
    pub provider_p99_latency_nanos: u64,
    pub provider_p999_latency_nanos: u64,
    pub wall_elapsed_nanos: u64,
    pub process_id: u32,
    pub measured_started_unix_nanos: u64,
    pub measured_finished_unix_nanos: u64,
    pub receipt_sha256: String,
}

/// Per-logical-point latency decomposition from the provider attempt lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28AlignedObjectRoleV2 {
    IndexedRow,
    Payload,
    Projection,
}

/// One completed provider attempt bound to one logical point read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedProviderAttemptV2 {
    pub api: String,
    pub object_role: T28AlignedObjectRoleV2,
    pub object_key: String,
    pub requested_range: Range<u64>,
    pub returned_range: Range<u64>,
    pub expected_generation: String,
    pub returned_generation: String,
    pub response_payload_bytes: u64,
    pub started_monotonic_nanos: u64,
    pub elapsed_nanos: u64,
    pub result: String,
}

/// Per-logical-point latency decomposition from the provider attempt lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedPointOperationV2 {
    pub ordinal: u64,
    pub end_to_end_nanos: u64,
    pub provider_pair_max_nanos: u64,
    pub local_residual_nanos: u64,
    pub pair_start_skew_nanos: u64,
    pub pair_completion_nanos: u64,
    pub provider_attempts: u64,
    pub provider_pair_overlapped: bool,
    pub attempts: Vec<T28AlignedProviderAttemptV2>,
}

/// RFC-0049 point receipt with provider attempts correlated to logical reads.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedPointPositionReceiptV2 {
    pub schema_version: u32,
    pub base: T28TypedPointPositionReceiptV1,
    pub operation_latency_samples: Vec<T28AlignedPointOperationV2>,
    pub provider_pair_max_p50_nanos: u64,
    pub provider_pair_max_p95_nanos: u64,
    pub provider_pair_max_p99_nanos: u64,
    pub provider_pair_max_p999_nanos: u64,
    pub local_residual_p50_nanos: u64,
    pub local_residual_p95_nanos: u64,
    pub local_residual_p99_nanos: u64,
    pub local_residual_p999_nanos: u64,
    pub maximum_pair_start_skew_nanos: u64,
    pub maximum_pair_completion_nanos: u64,
    pub receipt_sha256: String,
}

/// Immutable output from one fresh C0 or C5 projected-scan process.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28TypedScanPositionReceiptV1 {
    pub schema_version: u32,
    pub execution_plan_sha256: String,
    pub fixture_id: String,
    pub root_sha256: String,
    pub subject: T28TypedScanSubjectV1,
    pub trace_seed: u64,
    pub query: String,
    pub configured_range_fetch_concurrency: u64,
    pub observed_peak_range_fetch_concurrency: u64,
    pub resident_metadata_bytes: u64,
    pub rows: u64,
    pub ordered_projection_sha256: String,
    pub quantity_sum: String,
    pub query_elapsed_nanos: u64,
    pub rows_per_second: f64,
    pub provider_attempts: u64,
    pub response_bytes: u64,
    pub full_object_requests: u64,
    pub list_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
    pub missing_expected_generation_requests: u64,
    pub returned_generation_mismatches: u64,
    pub provider_errors: u64,
    pub source_scan_plans: u64,
    pub source_projection_pushdown_plans: u64,
    pub source_stripes: u64,
    pub source_batches: u64,
    pub source_rows: u64,
    pub peak_arrow_batch_rows: u64,
    pub peak_arrow_batch_bytes: u64,
    pub projection_fetch_requests: u64,
    pub peak_fetch_bytes: u64,
    pub opaque_payload_requests: u64,
    pub opaque_payload_response_bytes: u64,
    pub correctness_anomalies: u64,
    pub process_id: u32,
    pub measured_started_unix_nanos: u64,
    pub measured_finished_unix_nanos: u64,
    pub receipt_sha256: String,
}

/// Authenticated media inventory derived from the published C0 and C5v2
/// child descriptors, never from plan constants.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedMediaObservationV1 {
    pub fixture_id: String,
    pub root_sha256: String,
    pub canonical_history_sha256: String,
    pub candidate_placement_envelope_sha256: String,
    pub source_root_sha256: String,
    pub source_placement_envelope_sha256: String,
    pub control_prefix: String,
    pub candidate_prefix: String,
    pub control_closure_sha256: String,
    pub candidate_closure_sha256: String,
    pub control_total_media_bytes: u64,
    pub candidate_total_media_bytes: u64,
    pub control_objects: Vec<crate::t28_layout::TypedLayoutObjectIdentityV1>,
    pub candidate_objects: Vec<crate::t28_layout::TypedLayoutObjectIdentityV1>,
    pub source_c0_reused_by_reference: bool,
    pub observation_sha256: String,
}

impl T28AlignedMediaObservationV1 {
    fn seal(
        opened: &T28OpenedAlignedLayout,
        aligned_locator: &TypedLayoutPlacementLocatorV1,
    ) -> Result<Self, String> {
        let fixture = opened.fixture();
        aligned_locator.validate()?;
        if aligned_locator.fixture_id != fixture.fixture_id
            || aligned_locator.root_sha256 != fixture.root_sha256
        {
            return Err("RFC-0049 aligned locator differs from opened media".to_owned());
        }
        let control_total_media_bytes = fixture
            .source_c0
            .objects
            .iter()
            .fold(0_u64, |total, object| total.saturating_add(object.length));
        let candidate_total_media_bytes = fixture
            .candidate
            .objects
            .iter()
            .fold(0_u64, |total, object| total.saturating_add(object.length));
        let mut observation = Self {
            fixture_id: fixture.fixture_id.clone(),
            root_sha256: fixture.root_sha256.clone(),
            canonical_history_sha256: fixture.canonical_history_sha256.clone(),
            candidate_placement_envelope_sha256: aligned_locator.envelope_sha256.clone(),
            source_root_sha256: fixture.source_root_sha256.clone(),
            source_placement_envelope_sha256: fixture.source_placement_envelope_sha256.clone(),
            control_prefix: fixture.source_c0_prefix.clone(),
            candidate_prefix: fixture.candidate.prefix.clone(),
            control_closure_sha256: fixture.source_c0.closure_sha256.clone(),
            candidate_closure_sha256: fixture.candidate.closure_sha256.clone(),
            control_total_media_bytes,
            candidate_total_media_bytes,
            control_objects: fixture.source_c0.objects.clone(),
            candidate_objects: fixture.candidate.objects.clone(),
            source_c0_reused_by_reference: fixture.source_c0_prefix != fixture.candidate.prefix
                && fixture.source_c0.canonical_history_sha256 == fixture.canonical_history_sha256,
            observation_sha256: String::new(),
        };
        observation.observation_sha256 = observation.calculated_sha256()?;
        observation.validate()?;
        Ok(observation)
    }

    /// Recompute the authenticated inventory digest and its component totals.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or duplicate media, arithmetic drift, or a
    /// changed observation digest.
    pub fn validate(&self) -> Result<(), String> {
        let control_total = self
            .control_objects
            .iter()
            .fold(0_u64, |total, object| total.saturating_add(object.length));
        let candidate_total = self
            .candidate_objects
            .iter()
            .fold(0_u64, |total, object| total.saturating_add(object.length));
        if !valid_sha256(&self.fixture_id)
            || !valid_sha256(&self.root_sha256)
            || !valid_sha256(&self.canonical_history_sha256)
            || !valid_sha256(&self.candidate_placement_envelope_sha256)
            || !valid_sha256(&self.source_root_sha256)
            || !valid_sha256(&self.source_placement_envelope_sha256)
            || self.control_prefix.trim_matches('/').is_empty()
            || self.candidate_prefix.trim_matches('/').is_empty()
            || self.control_prefix == self.candidate_prefix
            || !valid_sha256(&self.control_closure_sha256)
            || !valid_sha256(&self.candidate_closure_sha256)
            || self.control_objects.is_empty()
            || self.candidate_objects.is_empty()
            || control_total != self.control_total_media_bytes
            || candidate_total != self.candidate_total_media_bytes
            || !self.source_c0_reused_by_reference
            || self.observation_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 authenticated media observation".to_owned());
        }
        Ok(())
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.observation_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

/// Authenticate a persisted aligned-root locator against the media receipt
/// that is already bound into the performance run.
///
/// # Errors
///
/// Returns an error for a changed envelope, root generation, object identity,
/// fixture, placement, or child prefix.
pub fn validate_t28_aligned_candidate_locator(
    media: &T28AlignedMediaObservationV1,
    locator: &TypedLayoutPlacementLocatorV1,
) -> Result<(), String> {
    media.validate()?;
    locator.validate()?;
    if locator.envelope_sha256 != media.candidate_placement_envelope_sha256
        || locator.fixture_id != media.fixture_id
        || locator.root_sha256 != media.root_sha256
        || !media
            .candidate_prefix
            .starts_with(&format!("{}/", locator.prefix.trim_end_matches('/')))
    {
        return Err("RFC-0049 candidate locator differs from persisted media".to_owned());
    }
    Ok(())
}

impl T28TypedScanPositionReceiptV1 {
    /// Decode and authenticate one projected-scan receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or any identity, result, work,
    /// memory, concurrency, or digest drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Recompute and validate every derived projected-scan field.
    ///
    /// # Errors
    ///
    /// Returns an error unless the receipt proves one exact, bounded,
    /// generation-pinned scan.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || !valid_sha256(&self.execution_plan_sha256)
            || !valid_sha256(&self.fixture_id)
            || !valid_sha256(&self.root_sha256)
            || self.trace_seed == 0
            || self.query.is_empty()
            || self.configured_range_fetch_concurrency != 1
            || self.observed_peak_range_fetch_concurrency != 1
            || self.resident_metadata_bytes == 0
            || self.rows == 0
            || !valid_sha256(&self.ordered_projection_sha256)
            || self.quantity_sum.is_empty()
            || self.query_elapsed_nanos == 0
            || !self.rows_per_second.is_finite()
            || self.rows_per_second <= 0.0
            || self.provider_attempts == 0
            || self.response_bytes == 0
            || self.full_object_requests != 0
            || self.list_requests != 0
            || self.put_requests != 0
            || self.delete_requests != 0
            || self.missing_expected_generation_requests != 0
            || self.returned_generation_mismatches != 0
            || self.provider_errors != 0
            || self.source_scan_plans != 1
            || self.source_stripes == 0
            || self.source_batches != self.source_stripes
            || self.source_rows != self.rows
            || self.peak_arrow_batch_rows == 0
            || self.peak_arrow_batch_rows > 128
            || self.peak_arrow_batch_bytes == 0
            || self.peak_fetch_bytes == 0
            || self.peak_fetch_bytes > 256 * 1_024
            || self.opaque_payload_requests != 0
            || self.opaque_payload_response_bytes != 0
            || self.correctness_anomalies != 0
            || self.process_id == 0
            || self.measured_started_unix_nanos == 0
            || self.measured_finished_unix_nanos <= self.measured_started_unix_nanos
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0048 scan-position receipt".to_owned());
        }
        Ok(())
    }

    /// Calculate the receipt digest without trusting its stored digest field.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

impl T28TypedPointPositionReceiptV1 {
    /// Decode and authenticate one position receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or any identity, counter,
    /// percentile, generation, correctness, or digest drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Recompute and validate every derived receipt field.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt cannot prove one bounded exact point
    /// position.
    pub fn validate(&self) -> Result<(), String> {
        let operation_count =
            usize::try_from(self.measured_operations).map_err(|error| error.to_string())?;
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || !valid_sha256(&self.execution_plan_sha256)
            || !valid_sha256(&self.fixture_id)
            || !valid_sha256(&self.root_sha256)
            || self.trace_seed == 0
            || operation_count == 0
            || self.concurrent_tasks != 8
            || self.warmup_canary_reads != 128
            || self.resident_metadata_bytes == 0
            || self.measured_provider_attempts < self.measured_operations
            || self.measured_provider_attempts
                > self
                    .measured_operations
                    .saturating_mul(self.subject.maximum_requests_per_point())
            || self.measured_response_bytes == 0
            || self.maximum_point_bytes_upper_bound == 0
            || self.maximum_attempts_per_point != self.subject.maximum_requests_per_point()
            || self.full_object_requests != 0
            || self.list_requests != 0
            || self.put_requests != 0
            || self.delete_requests != 0
            || self.missing_expected_generation_requests != 0
            || self.returned_generation_mismatches != 0
            || self.provider_errors != 0
            || self.correctness_anomalies != 0
            || match self.subject {
                T28TypedPointSubjectV1::C5v2AlignedColumnar => {
                    self.point_pairs != self.measured_operations
                        || self.overlapping_point_pairs != self.point_pairs
                }
                T28TypedPointSubjectV1::C0IndexedRow | T28TypedPointSubjectV1::C5ColumnarMain => {
                    self.point_pairs != 0 || self.overlapping_point_pairs != 0
                }
            }
            || self.latency_nanos.len() != operation_count
            || self.provider_latency_nanos.len()
                != usize::try_from(self.measured_provider_attempts)
                    .map_err(|error| error.to_string())?
            || self.wall_elapsed_nanos == 0
            || self.process_id == 0
            || self.measured_started_unix_nanos == 0
            || self.measured_finished_unix_nanos <= self.measured_started_unix_nanos
            || self.p50_latency_nanos != nearest_rank(&self.latency_nanos, 50, 100)?
            || self.p95_latency_nanos != nearest_rank(&self.latency_nanos, 95, 100)?
            || self.p99_latency_nanos != nearest_rank(&self.latency_nanos, 99, 100)?
            || self.p999_latency_nanos != nearest_rank(&self.latency_nanos, 999, 1_000)?
            || self.provider_p50_latency_nanos
                != nearest_rank(&self.provider_latency_nanos, 50, 100)?
            || self.provider_p95_latency_nanos
                != nearest_rank(&self.provider_latency_nanos, 95, 100)?
            || self.provider_p99_latency_nanos
                != nearest_rank(&self.provider_latency_nanos, 99, 100)?
            || self.provider_p999_latency_nanos
                != nearest_rank(&self.provider_latency_nanos, 999, 1_000)?
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0048 point-position receipt".to_owned());
        }
        Ok(())
    }

    /// Calculate the content digest without trusting the stored digest field.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

impl T28AlignedPointPositionReceiptV2 {
    /// Decode and authenticate one RFC-0049 correlated point receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON, an invalid base receipt, an
    /// uncorrelated provider attempt, a sequential candidate pair, or any
    /// derived percentile or digest drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    fn seal(
        base: T28TypedPointPositionReceiptV1,
        operation_latency_samples: Vec<T28AlignedPointOperationV2>,
    ) -> Result<Self, String> {
        let mut provider = operation_latency_samples
            .iter()
            .map(|sample| sample.provider_pair_max_nanos)
            .collect::<Vec<_>>();
        let mut local = operation_latency_samples
            .iter()
            .map(|sample| sample.local_residual_nanos)
            .collect::<Vec<_>>();
        provider.sort_unstable();
        local.sort_unstable();
        let mut receipt = Self {
            schema_version: ALIGNED_POINT_RECEIPT_SCHEMA_VERSION,
            base,
            provider_pair_max_p50_nanos: nearest_rank(&provider, 50, 100)?,
            provider_pair_max_p95_nanos: nearest_rank(&provider, 95, 100)?,
            provider_pair_max_p99_nanos: nearest_rank(&provider, 99, 100)?,
            provider_pair_max_p999_nanos: nearest_rank(&provider, 999, 1_000)?,
            local_residual_p50_nanos: nearest_rank(&local, 50, 100)?,
            local_residual_p95_nanos: nearest_rank(&local, 95, 100)?,
            local_residual_p99_nanos: nearest_rank(&local, 99, 100)?,
            local_residual_p999_nanos: nearest_rank(&local, 999, 1_000)?,
            maximum_pair_start_skew_nanos: operation_latency_samples
                .iter()
                .map(|sample| sample.pair_start_skew_nanos)
                .max()
                .unwrap_or(0),
            maximum_pair_completion_nanos: operation_latency_samples
                .iter()
                .map(|sample| sample.pair_completion_nanos)
                .max()
                .unwrap_or(0),
            operation_latency_samples,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Recompute every correlation, percentile, and digest invariant.
    ///
    /// # Errors
    ///
    /// Returns an error unless the nested receipt and every logical point
    /// prove the exact RFC-0049 provider fanout and latency decomposition.
    pub fn validate(&self) -> Result<(), String> {
        self.base.validate()?;
        let operation_count =
            usize::try_from(self.base.measured_operations).map_err(|error| error.to_string())?;
        if self.schema_version != ALIGNED_POINT_RECEIPT_SCHEMA_VERSION
            || !matches!(
                self.base.subject,
                T28TypedPointSubjectV1::C0IndexedRow | T28TypedPointSubjectV1::C5v2AlignedColumnar
            )
            || self.operation_latency_samples.len() != operation_count
        {
            return Err("invalid RFC-0049 correlated point receipt boundary".to_owned());
        }

        let expected_attempts = self.base.subject.maximum_requests_per_point();
        let require_overlap = self.base.subject == T28TypedPointSubjectV1::C5v2AlignedColumnar;
        let mut previous_ordinal = None;
        let mut end_to_end = Vec::with_capacity(operation_count);
        let mut provider = Vec::with_capacity(operation_count);
        let mut local = Vec::with_capacity(operation_count);
        for sample in &self.operation_latency_samples {
            if previous_ordinal.is_some_and(|previous| previous >= sample.ordinal)
                || sample.end_to_end_nanos == 0
                || sample.provider_pair_max_nanos == 0
                || sample.local_residual_nanos
                    != sample
                        .end_to_end_nanos
                        .saturating_sub(sample.pair_completion_nanos)
                || sample.pair_completion_nanos < sample.provider_pair_max_nanos
                || sample.pair_completion_nanos > sample.end_to_end_nanos
                || sample.pair_start_skew_nanos > sample.pair_completion_nanos
                || sample.provider_attempts != expected_attempts
                || sample.provider_attempts
                    != u64::try_from(sample.attempts.len()).unwrap_or(u64::MAX)
                || sample.provider_pair_overlapped != require_overlap
                || !valid_aligned_attempts(sample, self.base.subject)
            {
                return Err("invalid RFC-0049 logical point correlation".to_owned());
            }
            previous_ordinal = Some(sample.ordinal);
            end_to_end.push(sample.end_to_end_nanos);
            provider.push(sample.provider_pair_max_nanos);
            local.push(sample.local_residual_nanos);
        }
        end_to_end.sort_unstable();
        provider.sort_unstable();
        local.sort_unstable();
        if end_to_end != self.base.latency_nanos
            || self.provider_pair_max_p50_nanos != nearest_rank(&provider, 50, 100)?
            || self.provider_pair_max_p95_nanos != nearest_rank(&provider, 95, 100)?
            || self.provider_pair_max_p99_nanos != nearest_rank(&provider, 99, 100)?
            || self.provider_pair_max_p999_nanos != nearest_rank(&provider, 999, 1_000)?
            || self.local_residual_p50_nanos != nearest_rank(&local, 50, 100)?
            || self.local_residual_p95_nanos != nearest_rank(&local, 95, 100)?
            || self.local_residual_p99_nanos != nearest_rank(&local, 99, 100)?
            || self.local_residual_p999_nanos != nearest_rank(&local, 999, 1_000)?
            || self.maximum_pair_start_skew_nanos
                != self
                    .operation_latency_samples
                    .iter()
                    .map(|sample| sample.pair_start_skew_nanos)
                    .max()
                    .unwrap_or(0)
            || self.maximum_pair_completion_nanos
                != self
                    .operation_latency_samples
                    .iter()
                    .map(|sample| sample.pair_completion_nanos)
                    .max()
                    .unwrap_or(0)
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 correlated point receipt derivation".to_owned());
        }
        Ok(())
    }

    /// Rebind every provider attempt to the persisted descriptor inventory.
    ///
    /// # Errors
    ///
    /// Returns an error when a key, generation, range, role, or byte count is
    /// not exactly authorized by the authenticated media observation.
    pub fn validate_against_media(
        &self,
        media: &T28AlignedMediaObservationV1,
    ) -> Result<(), String> {
        self.validate()?;
        media.validate()?;
        if self.base.fixture_id != media.fixture_id || self.base.root_sha256 != media.root_sha256 {
            return Err("RFC-0049 point receipt differs from persisted media".to_owned());
        }
        for sample in &self.operation_latency_samples {
            for attempt in &sample.attempts {
                let (objects, prefix, role) = match (self.base.subject, attempt.object_role) {
                    (T28TypedPointSubjectV1::C0IndexedRow, T28AlignedObjectRoleV2::IndexedRow) => (
                        &media.control_objects,
                        media.control_prefix.as_str(),
                        TypedLayoutObjectRoleV1::Data,
                    ),
                    (
                        T28TypedPointSubjectV1::C5v2AlignedColumnar,
                        T28AlignedObjectRoleV2::Projection,
                    ) => (
                        &media.candidate_objects,
                        media.candidate_prefix.as_str(),
                        TypedLayoutObjectRoleV1::Projection,
                    ),
                    (
                        T28TypedPointSubjectV1::C5v2AlignedColumnar,
                        T28AlignedObjectRoleV2::Payload,
                    ) => (
                        &media.candidate_objects,
                        media.candidate_prefix.as_str(),
                        TypedLayoutObjectRoleV1::Payload,
                    ),
                    _ => {
                        return Err(
                            "RFC-0049 persisted attempt role differs from its subject".to_owned()
                        );
                    }
                };
                let descriptor =
                    find_exact_fixture_object(objects, prefix, role, &attempt.object_key)
                        .ok_or_else(|| {
                            "RFC-0049 persisted media omits an attempt role".to_owned()
                        })?;
                let expected_key = fixture_object_key(prefix, descriptor);
                if attempt.object_key != expected_key
                    || attempt.expected_generation != descriptor.generation
                    || attempt.returned_generation != descriptor.generation
                    || attempt.requested_range != attempt.returned_range
                    || attempt.requested_range.end > descriptor.length
                    || attempt.response_payload_bytes
                        != attempt
                            .requested_range
                            .end
                            .saturating_sub(attempt.requested_range.start)
                {
                    return Err(
                        "RFC-0049 persisted attempt differs from its media descriptor".to_owned(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Calculate the receipt digest without trusting its stored digest field.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

fn valid_aligned_attempts(
    sample: &T28AlignedPointOperationV2,
    subject: T28TypedPointSubjectV1,
) -> bool {
    if sample.attempts.is_empty()
        || sample.attempts.iter().any(|attempt| {
            attempt.api != "get"
                || attempt.requested_range.start >= attempt.requested_range.end
                || attempt.requested_range != attempt.returned_range
                || attempt.expected_generation.is_empty()
                || attempt.expected_generation != attempt.returned_generation
                || attempt.response_payload_bytes == 0
                || attempt.elapsed_nanos == 0
                || attempt.result != "ok"
        })
    {
        return false;
    }
    let roles = sample
        .attempts
        .iter()
        .map(|attempt| attempt.object_role)
        .collect::<Vec<_>>();
    let object_roles_match = match subject {
        T28TypedPointSubjectV1::C0IndexedRow => roles == [T28AlignedObjectRoleV2::IndexedRow],
        T28TypedPointSubjectV1::C5v2AlignedColumnar => {
            roles
                == [
                    T28AlignedObjectRoleV2::Payload,
                    T28AlignedObjectRoleV2::Projection,
                ]
                && sample.attempts[0].object_key.ends_with("payload.okv2")
                && sample.attempts[1].object_key.ends_with("projection.okp2")
        }
        T28TypedPointSubjectV1::C5ColumnarMain => false,
    };
    if !object_roles_match {
        return false;
    }
    let earliest_start = sample
        .attempts
        .iter()
        .map(|attempt| attempt.started_monotonic_nanos)
        .min()
        .unwrap_or(0);
    let latest_start = sample
        .attempts
        .iter()
        .map(|attempt| attempt.started_monotonic_nanos)
        .max()
        .unwrap_or(0);
    let earliest_finish = sample
        .attempts
        .iter()
        .map(|attempt| {
            attempt
                .started_monotonic_nanos
                .saturating_add(attempt.elapsed_nanos)
        })
        .min()
        .unwrap_or(0);
    let latest_finish = sample
        .attempts
        .iter()
        .map(|attempt| {
            attempt
                .started_monotonic_nanos
                .saturating_add(attempt.elapsed_nanos)
        })
        .max()
        .unwrap_or(0);
    sample.provider_pair_max_nanos
        == sample
            .attempts
            .iter()
            .map(|attempt| attempt.elapsed_nanos)
            .max()
            .unwrap_or(0)
        && sample.pair_start_skew_nanos == latest_start.saturating_sub(earliest_start)
        && sample.pair_completion_nanos == latest_finish.saturating_sub(earliest_start)
        && sample.provider_pair_overlapped
            == (sample.attempts.len() > 1 && latest_start < earliest_finish)
}

/// Run one metadata-warm, data-cold point position in the current fresh
/// process.
///
/// # Errors
///
/// Returns an error for plan drift, failed generation-pinned open, a wrong
/// point outcome, extra or full-object provider work, or a malformed receipt.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_t28_typed_point_position(
    backend: Arc<dyn Backend>,
    locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
    subject: T28TypedPointSubjectV1,
    trace_seed: u64,
    measured_operations: usize,
) -> Result<T28TypedPointPositionReceiptV1, String> {
    let attempts = Arc::new(ProviderAttemptBackend::new(backend, subject.id())?);
    let observed_backend: Arc<dyn Backend> = attempts.clone();
    let opened = T28OpenedTypedLayout::open(Arc::clone(&observed_backend), locator).await?;
    let plan = T28TypedLayoutExecutionPlanV1::decode(
        plan_bytes,
        expected_plan_sha256,
        locator,
        opened.fixture(),
        oracle,
    )?;
    if measured_operations == 0
        || measured_operations > usize::try_from(plan.point_reads_per_position).unwrap_or(0)
        || plan.point_concurrent_tasks != 8
    {
        return Err("invalid RFC-0048 point-position size or concurrency".to_owned());
    }
    let trace = plan
        .trace(trace_seed)
        .ok_or_else(|| "RFC-0048 point position selected an unknown trace".to_owned())?;
    let (reader, resident_metadata_bytes) = match subject {
        T28TypedPointSubjectV1::C0IndexedRow => {
            let reader = Arc::new(opened.c0().await?);
            let bytes = reader.resident_metadata_bytes();
            (PointReader::C0(reader), bytes)
        }
        T28TypedPointSubjectV1::C5ColumnarMain => {
            let reader = Arc::new(opened.c5().await?);
            let bytes = reader.resident_metadata_bytes();
            (PointReader::C5(reader), bytes)
        }
        T28TypedPointSubjectV1::C5v2AlignedColumnar => {
            return Err("C5v2 requires an RFC-0049 aligned root".to_owned());
        }
    };
    let reader = Arc::new(reader);

    for _ in 0..plan.point_warmup_canary_reads {
        observed_backend
            .get(&locator.root_key, None, Some(&locator.root_revision()))
            .await
            .map_err(|error| error.to_string())?;
    }
    attempts.clear_events();

    let measured_started_unix_nanos = unix_nanos();
    let wall_started = Instant::now();
    let mut points = stream::iter(trace.operations.iter().take(measured_operations).cloned())
        .map(|operation| {
            let reader = Arc::clone(&reader);
            async move {
                let started = Instant::now();
                let outcome = reader.read(operation.key, operation.read_version).await?;
                Ok::<MeasuredPoint, String>(MeasuredPoint {
                    ordinal: operation.ordinal,
                    latency_nanos: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    outcome_sha256: t28_typed_point_outcome_sha256(&outcome),
                    expected_outcome_sha256: operation.expected_outcome_sha256,
                })
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let wall_elapsed_nanos = u64::try_from(wall_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let measured_finished_unix_nanos = unix_nanos();
    points.sort_by_key(|point| point.ordinal);
    let correctness_anomalies = points
        .iter()
        .filter(|point| point.outcome_sha256 != point.expected_outcome_sha256)
        .count();
    if correctness_anomalies != 0 {
        return Err("RFC-0048 point position returned an incorrect outcome".to_owned());
    }
    let mut latency_nanos = points
        .iter()
        .map(|point| point.latency_nanos)
        .collect::<Vec<_>>();
    latency_nanos.sort_unstable();
    let provider = evaluate_provider_events(&attempts.events(), subject)?;
    let mut receipt = T28TypedPointPositionReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        execution_plan_sha256: plan.execution_plan_sha256,
        fixture_id: opened.fixture().fixture_id.clone(),
        root_sha256: opened.fixture().root_sha256.clone(),
        subject,
        trace_seed,
        measured_operations: u64::try_from(measured_operations).unwrap_or(u64::MAX),
        concurrent_tasks: 8,
        warmup_canary_reads: plan.point_warmup_canary_reads,
        resident_metadata_bytes,
        measured_provider_attempts: provider.attempts,
        measured_response_bytes: provider.response_bytes,
        maximum_point_bytes_upper_bound: provider.maximum_point_bytes_upper_bound,
        maximum_attempts_per_point: subject.maximum_requests_per_point(),
        full_object_requests: provider.full_object_requests,
        list_requests: provider.list_requests,
        put_requests: provider.put_requests,
        delete_requests: provider.delete_requests,
        missing_expected_generation_requests: provider.missing_expected_generation_requests,
        returned_generation_mismatches: provider.returned_generation_mismatches,
        provider_errors: provider.errors,
        correctness_anomalies: u64::try_from(correctness_anomalies).unwrap_or(u64::MAX),
        point_pairs: 0,
        overlapping_point_pairs: 0,
        p50_latency_nanos: nearest_rank(&latency_nanos, 50, 100)?,
        p95_latency_nanos: nearest_rank(&latency_nanos, 95, 100)?,
        p99_latency_nanos: nearest_rank(&latency_nanos, 99, 100)?,
        p999_latency_nanos: nearest_rank(&latency_nanos, 999, 1_000)?,
        latency_nanos,
        provider_p50_latency_nanos: nearest_rank(&provider.latencies, 50, 100)?,
        provider_p95_latency_nanos: nearest_rank(&provider.latencies, 95, 100)?,
        provider_p99_latency_nanos: nearest_rank(&provider.latencies, 99, 100)?,
        provider_p999_latency_nanos: nearest_rank(&provider.latencies, 999, 1_000)?,
        provider_latency_nanos: provider.latencies,
        wall_elapsed_nanos,
        process_id: std::process::id(),
        measured_started_unix_nanos,
        measured_finished_unix_nanos,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

async fn open_aligned_with_source_plan(
    backend: Arc<dyn Backend>,
    aligned_locator: &TypedLayoutPlacementLocatorV1,
    source_locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
) -> Result<(T28OpenedAlignedLayout, T28TypedLayoutExecutionPlanV1), String> {
    let source = T28OpenedTypedLayout::open(Arc::clone(&backend), source_locator).await?;
    let plan = T28TypedLayoutExecutionPlanV1::decode(
        plan_bytes,
        expected_plan_sha256,
        source_locator,
        source.fixture(),
        oracle,
    )?;
    let aligned = T28OpenedAlignedLayout::open(backend, aligned_locator).await?;
    let source_c0 = source
        .fixture()
        .children
        .iter()
        .find(|child| child.subject == TypedLayoutSubjectV1::C0IndexedRow)
        .ok_or_else(|| "RFC-0049 source root omits C0".to_owned())?;
    let fixture = aligned.fixture();
    if fixture.source_root_sha256 != source.fixture().root_sha256
        || fixture.source_root_generation != source_locator.root_generation
        || fixture.source_placement_envelope_sha256 != source_locator.envelope_sha256
        || &fixture.source_c0 != source_c0
        || fixture.fixture_id != source.fixture().fixture_id
        || fixture.oracle_sha256 != source.fixture().oracle_sha256
        || fixture.workload_plan_sha256 != source.fixture().workload_plan_sha256
        || fixture.physical_plan_sha256
            != "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61"
    {
        return Err("RFC-0049 aligned root does not close over its source plan".to_owned());
    }
    Ok((aligned, plan))
}

/// Open the exact aligned root and derive stored-media totals and component
/// identities from its authenticated child descriptors.
///
/// # Errors
///
/// Returns an error for locator, root, source-plan, object-identity, or media
/// inventory drift.
pub async fn inspect_t28_aligned_media(
    backend: Arc<dyn Backend>,
    aligned_locator: &TypedLayoutPlacementLocatorV1,
    source_locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
) -> Result<T28AlignedMediaObservationV1, String> {
    let (opened, _) = open_aligned_with_source_plan(
        backend,
        aligned_locator,
        source_locator,
        oracle,
        plan_bytes,
        expected_plan_sha256,
    )
    .await?;
    T28AlignedMediaObservationV1::seal(&opened, aligned_locator)
}

/// Run one viewer-only RFC-0049 point position against the reused C0 or C5v2.
///
/// # Errors
///
/// Returns an error for source-plan drift, wrong outcomes, missing concurrent
/// pair overlap, extra provider work, or malformed receipt evidence.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_t28_aligned_point_position(
    backend: Arc<dyn Backend>,
    aligned_locator: &TypedLayoutPlacementLocatorV1,
    source_locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
    subject: T28TypedPointSubjectV1,
    trace_seed: u64,
    measured_operations: usize,
) -> Result<T28AlignedPointPositionReceiptV2, String> {
    if !matches!(
        subject,
        T28TypedPointSubjectV1::C0IndexedRow | T28TypedPointSubjectV1::C5v2AlignedColumnar
    ) {
        return Err("RFC-0049 point position selected an incompatible subject".to_owned());
    }
    let attempts = Arc::new(ProviderAttemptBackend::new(backend, subject.id())?);
    let observed_backend: Arc<dyn Backend> = attempts.clone();
    let (opened, plan) = open_aligned_with_source_plan(
        Arc::clone(&observed_backend),
        aligned_locator,
        source_locator,
        oracle,
        plan_bytes,
        expected_plan_sha256,
    )
    .await?;
    if measured_operations == 0
        || measured_operations > usize::try_from(plan.point_reads_per_position).unwrap_or(0)
        || plan.point_concurrent_tasks != 8
    {
        return Err("invalid RFC-0049 point-position size or concurrency".to_owned());
    }
    let trace = plan
        .trace(trace_seed)
        .ok_or_else(|| "RFC-0049 point position selected an unknown trace".to_owned())?;
    let (reader, resident_metadata_bytes) = match subject {
        T28TypedPointSubjectV1::C0IndexedRow => {
            let reader = Arc::new(opened.c0().await?);
            let bytes = reader.resident_metadata_bytes();
            (PointReader::C0(reader), bytes)
        }
        T28TypedPointSubjectV1::C5v2AlignedColumnar => {
            let reader = Arc::new(opened.c5v2().await?);
            let bytes = reader.resident_metadata_bytes();
            (PointReader::C5v2(reader), bytes)
        }
        T28TypedPointSubjectV1::C5ColumnarMain => unreachable!(),
    };
    let reader = Arc::new(reader);

    for _ in 0..plan.point_warmup_canary_reads {
        observed_backend
            .get(
                &aligned_locator.root_key,
                None,
                Some(&aligned_locator.root_revision()),
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    attempts.clear_events();

    let measured_started_unix_nanos = unix_nanos();
    let wall_started = Instant::now();
    let mut points = stream::iter(trace.operations.iter().take(measured_operations).cloned())
        .map(|operation| {
            let reader = Arc::clone(&reader);
            async move {
                let started = Instant::now();
                let outcome = scope_logical_operation(
                    operation.ordinal,
                    reader.read(operation.key, operation.read_version),
                )
                .await?;
                Ok::<MeasuredPoint, String>(MeasuredPoint {
                    ordinal: operation.ordinal,
                    latency_nanos: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    outcome_sha256: t28_typed_point_outcome_sha256(&outcome),
                    expected_outcome_sha256: operation.expected_outcome_sha256,
                })
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let wall_elapsed_nanos = u64::try_from(wall_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let measured_finished_unix_nanos = unix_nanos();
    points.sort_by_key(|point| point.ordinal);
    let correctness_anomalies = points
        .iter()
        .filter(|point| point.outcome_sha256 != point.expected_outcome_sha256)
        .count();
    if correctness_anomalies != 0 {
        return Err("RFC-0049 point position returned an incorrect outcome".to_owned());
    }
    let mut latency_nanos = points
        .iter()
        .map(|point| point.latency_nanos)
        .collect::<Vec<_>>();
    latency_nanos.sort_unstable();
    let provider_events = attempts.events();
    let correlated =
        correlate_provider_events(&provider_events, &points, subject, opened.fixture())?;
    let provider = evaluate_provider_events(&provider_events, subject)?;
    let expected_attempts = u64::try_from(measured_operations)
        .unwrap_or(u64::MAX)
        .saturating_mul(subject.maximum_requests_per_point());
    if provider.attempts != expected_attempts {
        return Err("RFC-0049 point position did not issue its exact provider fanout".to_owned());
    }
    let (point_pairs, overlapping_point_pairs) = reader.point_gather_snapshot();
    let mut receipt = T28TypedPointPositionReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        execution_plan_sha256: plan.execution_plan_sha256,
        fixture_id: opened.fixture().fixture_id.clone(),
        root_sha256: opened.fixture().root_sha256.clone(),
        subject,
        trace_seed,
        measured_operations: u64::try_from(measured_operations).unwrap_or(u64::MAX),
        concurrent_tasks: 8,
        warmup_canary_reads: plan.point_warmup_canary_reads,
        resident_metadata_bytes,
        measured_provider_attempts: provider.attempts,
        measured_response_bytes: provider.response_bytes,
        maximum_point_bytes_upper_bound: provider.maximum_point_bytes_upper_bound,
        maximum_attempts_per_point: subject.maximum_requests_per_point(),
        full_object_requests: provider.full_object_requests,
        list_requests: provider.list_requests,
        put_requests: provider.put_requests,
        delete_requests: provider.delete_requests,
        missing_expected_generation_requests: provider.missing_expected_generation_requests,
        returned_generation_mismatches: provider.returned_generation_mismatches,
        provider_errors: provider.errors,
        correctness_anomalies: u64::try_from(correctness_anomalies).unwrap_or(u64::MAX),
        point_pairs,
        overlapping_point_pairs,
        p50_latency_nanos: nearest_rank(&latency_nanos, 50, 100)?,
        p95_latency_nanos: nearest_rank(&latency_nanos, 95, 100)?,
        p99_latency_nanos: nearest_rank(&latency_nanos, 99, 100)?,
        p999_latency_nanos: nearest_rank(&latency_nanos, 999, 1_000)?,
        latency_nanos,
        provider_p50_latency_nanos: nearest_rank(&provider.latencies, 50, 100)?,
        provider_p95_latency_nanos: nearest_rank(&provider.latencies, 95, 100)?,
        provider_p99_latency_nanos: nearest_rank(&provider.latencies, 99, 100)?,
        provider_p999_latency_nanos: nearest_rank(&provider.latencies, 999, 1_000)?,
        provider_latency_nanos: provider.latencies,
        wall_elapsed_nanos,
        process_id: std::process::id(),
        measured_started_unix_nanos,
        measured_finished_unix_nanos,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    T28AlignedPointPositionReceiptV2::seal(receipt, correlated)
}

/// Run one complete projected scan in the current fresh process.
///
/// # Errors
///
/// Returns an error for plan drift, a wrong ordered projection or aggregate,
/// opaque-payload access, unbounded fetches or batches, concurrent provider
/// calls, generation drift, or a malformed receipt.
#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
pub async fn run_t28_typed_scan_position(
    backend: Arc<dyn Backend>,
    locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
    subject: T28TypedScanSubjectV1,
    trace_seed: u64,
) -> Result<T28TypedScanPositionReceiptV1, String> {
    let attempts = Arc::new(ProviderAttemptBackend::new(backend, subject.id())?);
    let observed_backend: Arc<dyn Backend> = attempts.clone();
    let opened = T28OpenedTypedLayout::open(Arc::clone(&observed_backend), locator).await?;
    let plan = T28TypedLayoutExecutionPlanV1::decode(
        plan_bytes,
        expected_plan_sha256,
        locator,
        opened.fixture(),
        oracle,
    )?;
    if plan.trace(trace_seed).is_none() || plan.scan_concurrent_fetches != 1 {
        return Err("invalid RFC-0048 scan-position seed or concurrency".to_owned());
    }

    let (provider, source_stats, columnar_scan, resident_metadata_bytes) = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => {
            let reader = opened.c0().await?;
            let resident = reader.resident_metadata_bytes();
            let provider = reader.table_provider();
            let stats = provider.stats();
            let provider: Arc<dyn datafusion::catalog::TableProvider> = provider;
            (provider, stats, None, resident)
        }
        T28TypedScanSubjectV1::C5ColumnarMain => {
            let reader = opened.c5().await?;
            let resident = reader.resident_metadata_bytes();
            let scan = reader.table_provider(
                usize::try_from(plan.scan_fetch_target_bytes).unwrap_or(usize::MAX),
            );
            let provider = scan.provider();
            let stats = provider.stats();
            let provider: Arc<dyn datafusion::catalog::TableProvider> = provider;
            (provider, stats, Some(scan), resident)
        }
        T28TypedScanSubjectV1::C5v2AlignedColumnar => {
            return Err("C5v2 requires an RFC-0049 aligned root".to_owned());
        }
    };

    for _ in 0..plan.point_warmup_canary_reads {
        observed_backend
            .get(&locator.root_key, None, Some(&locator.root_revision()))
            .await
            .map_err(|error| error.to_string())?;
    }
    attempts.clear_events();

    let context = SessionContext::new();
    context
        .register_table("okv_layout", provider)
        .map_err(|error| error.to_string())?;
    let measured_started_unix_nanos = unix_nanos();
    let started = Instant::now();
    let batches = context
        .sql(&plan.scan_query)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let rows = batches.iter().fold(0_usize, |total, batch| {
        total.saturating_add(batch.num_rows())
    });
    let expected_rows = usize::try_from(oracle.fixture.live_row_count).unwrap_or(usize::MAX);
    let expected_quantity_sum = oracle
        .fixture
        .aggregate
        .quantity_sum
        .parse::<i64>()
        .map_err(|error| error.to_string())?;
    let mut projection = Sha256::new();
    projection.update(b"okv-t28-ordered-projection-v1\0");
    projection.update(u64::try_from(rows).unwrap_or(u64::MAX).to_be_bytes());
    let mut anomalies = u64::from(rows != expected_rows);
    let mut previous_key = None;
    for batch in &batches {
        let keys = batch
            .column_by_name("key")
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| "RFC-0048 scan key column is absent".to_owned())?;
        let tenants = batch
            .column_by_name("tenant")
            .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| "RFC-0048 scan tenant column is absent".to_owned())?;
        let categories = batch
            .column_by_name("category")
            .and_then(|array| array.as_any().downcast_ref::<UInt16Array>())
            .ok_or_else(|| "RFC-0048 scan category column is absent".to_owned())?;
        let quantities = batch
            .column_by_name("quantity")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| "RFC-0048 scan quantity column is absent".to_owned())?;
        let counts = batch
            .column_by_name("row_count")
            .ok_or_else(|| "RFC-0048 scan row-count column is absent".to_owned())?;
        let sums = batch
            .column_by_name("quantity_sum")
            .ok_or_else(|| "RFC-0048 scan quantity-sum column is absent".to_owned())?;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            anomalies = anomalies.saturating_add(u64::from(
                previous_key.is_some_and(|previous| previous >= key),
            ));
            previous_key = Some(key);
            projection.update(key.to_be_bytes());
            projection.update(tenants.value(row).to_be_bytes());
            projection.update(categories.value(row).to_be_bytes());
            projection.update(quantities.value(row).to_be_bytes());
            anomalies = anomalies.saturating_add(u64::from(
                array_u64(counts.as_ref(), row)?
                    != u64::try_from(expected_rows).unwrap_or(u64::MAX),
            ));
            anomalies = anomalies.saturating_add(u64::from(
                array_i64(sums.as_ref(), row)? != expected_quantity_sum,
            ));
        }
    }
    let ordered_projection_sha256 = format!("{:x}", projection.finalize());
    anomalies = anomalies.saturating_add(u64::from(
        ordered_projection_sha256 != oracle.fixture.ordered_projection_sha256,
    ));
    let query_elapsed_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let measured_finished_unix_nanos = unix_nanos();
    let provider = evaluate_provider_events(
        &attempts.events(),
        match subject {
            T28TypedScanSubjectV1::C0IndexedRow => T28TypedPointSubjectV1::C0IndexedRow,
            T28TypedScanSubjectV1::C5ColumnarMain => T28TypedPointSubjectV1::C5ColumnarMain,
            T28TypedScanSubjectV1::C5v2AlignedColumnar => {
                T28TypedPointSubjectV1::C5v2AlignedColumnar
            }
        },
    )?;
    let source = source_stats.snapshot();
    let columnar = columnar_scan
        .as_ref()
        .map(T28ColumnarScan::source_snapshot)
        .unwrap_or_default();
    let projection_fetch_requests = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => provider.attempts,
        T28TypedScanSubjectV1::C5ColumnarMain | T28TypedScanSubjectV1::C5v2AlignedColumnar => {
            columnar.projection_fetch_requests
        }
    };
    let peak_fetch_bytes = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => provider.maximum_response_bytes,
        T28TypedScanSubjectV1::C5ColumnarMain | T28TypedScanSubjectV1::C5v2AlignedColumnar => {
            columnar.peak_fetch_bytes
        }
    };
    let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX);
    let mut receipt = T28TypedScanPositionReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        execution_plan_sha256: plan.execution_plan_sha256,
        fixture_id: opened.fixture().fixture_id.clone(),
        root_sha256: opened.fixture().root_sha256.clone(),
        subject,
        trace_seed,
        query: plan.scan_query,
        configured_range_fetch_concurrency: 1,
        observed_peak_range_fetch_concurrency: provider.peak_inflight,
        resident_metadata_bytes,
        rows: rows_u64,
        ordered_projection_sha256,
        quantity_sum: expected_quantity_sum.to_string(),
        query_elapsed_nanos,
        rows_per_second: rows_u64 as f64 / (query_elapsed_nanos as f64 / 1_000_000_000.0),
        provider_attempts: provider.attempts,
        response_bytes: provider.response_bytes,
        full_object_requests: provider.full_object_requests,
        list_requests: provider.list_requests,
        put_requests: provider.put_requests,
        delete_requests: provider.delete_requests,
        missing_expected_generation_requests: provider.missing_expected_generation_requests,
        returned_generation_mismatches: provider.returned_generation_mismatches,
        provider_errors: provider.errors,
        source_scan_plans: source.scan_plans,
        source_projection_pushdown_plans: source.projection_pushdown_plans,
        source_stripes: source.stripes_read,
        source_batches: source.batches_emitted,
        source_rows: source.rows_emitted,
        peak_arrow_batch_rows: source.peak_batch_rows,
        peak_arrow_batch_bytes: source.peak_batch_bytes,
        projection_fetch_requests,
        peak_fetch_bytes,
        opaque_payload_requests: columnar.payload_requests,
        opaque_payload_response_bytes: columnar.payload_response_bytes,
        correctness_anomalies: anomalies,
        process_id: std::process::id(),
        measured_started_unix_nanos,
        measured_finished_unix_nanos,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

/// Run one viewer-only RFC-0049 projected-scan position against reused C0 or
/// projection-only C5v2 media.
///
/// # Errors
///
/// Returns an error for source-plan drift, incorrect snapshot output, opaque
/// payload access, unbounded fetches, or malformed receipt evidence.
#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
pub async fn run_t28_aligned_scan_position(
    backend: Arc<dyn Backend>,
    aligned_locator: &TypedLayoutPlacementLocatorV1,
    source_locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    plan_bytes: &[u8],
    expected_plan_sha256: &str,
    subject: T28TypedScanSubjectV1,
    trace_seed: u64,
) -> Result<T28TypedScanPositionReceiptV1, String> {
    if !matches!(
        subject,
        T28TypedScanSubjectV1::C0IndexedRow | T28TypedScanSubjectV1::C5v2AlignedColumnar
    ) {
        return Err("RFC-0049 scan position selected an incompatible subject".to_owned());
    }
    let attempts = Arc::new(ProviderAttemptBackend::new(backend, subject.id())?);
    let observed_backend: Arc<dyn Backend> = attempts.clone();
    let (opened, plan) = open_aligned_with_source_plan(
        Arc::clone(&observed_backend),
        aligned_locator,
        source_locator,
        oracle,
        plan_bytes,
        expected_plan_sha256,
    )
    .await?;
    if plan.trace(trace_seed).is_none() || plan.scan_concurrent_fetches != 1 {
        return Err("invalid RFC-0049 scan-position seed or concurrency".to_owned());
    }

    let (provider, source_stats, aligned_scan, resident_metadata_bytes) = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => {
            let reader = opened.c0().await?;
            let resident = reader.resident_metadata_bytes();
            let provider = reader.table_provider();
            let stats = provider.stats();
            let provider: Arc<dyn datafusion::catalog::TableProvider> = provider;
            (provider, stats, None, resident)
        }
        T28TypedScanSubjectV1::C5v2AlignedColumnar => {
            let reader = opened.c5v2().await?;
            let resident = reader.resident_metadata_bytes();
            let scan = reader.table_provider(
                usize::try_from(plan.scan_fetch_target_bytes).unwrap_or(usize::MAX),
            );
            let provider = scan.provider();
            let stats = provider.stats();
            let provider: Arc<dyn datafusion::catalog::TableProvider> = provider;
            (provider, stats, Some(scan), resident)
        }
        T28TypedScanSubjectV1::C5ColumnarMain => unreachable!(),
    };

    for _ in 0..plan.point_warmup_canary_reads {
        observed_backend
            .get(
                &aligned_locator.root_key,
                None,
                Some(&aligned_locator.root_revision()),
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    attempts.clear_events();

    let context = SessionContext::new();
    context
        .register_table("okv_layout", provider)
        .map_err(|error| error.to_string())?;
    let measured_started_unix_nanos = unix_nanos();
    let started = Instant::now();
    let batches = context
        .sql(&plan.scan_query)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let rows = batches.iter().fold(0_usize, |total, batch| {
        total.saturating_add(batch.num_rows())
    });
    let expected_rows = usize::try_from(oracle.fixture.live_row_count).unwrap_or(usize::MAX);
    let expected_quantity_sum = oracle
        .fixture
        .aggregate
        .quantity_sum
        .parse::<i64>()
        .map_err(|error| error.to_string())?;
    let mut projection = Sha256::new();
    projection.update(b"okv-t28-ordered-projection-v1\0");
    projection.update(u64::try_from(rows).unwrap_or(u64::MAX).to_be_bytes());
    let mut anomalies = u64::from(rows != expected_rows);
    let mut previous_key = None;
    for batch in &batches {
        let keys = batch
            .column_by_name("key")
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| "RFC-0049 scan key column is absent".to_owned())?;
        let tenants = batch
            .column_by_name("tenant")
            .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| "RFC-0049 scan tenant column is absent".to_owned())?;
        let categories = batch
            .column_by_name("category")
            .and_then(|array| array.as_any().downcast_ref::<UInt16Array>())
            .ok_or_else(|| "RFC-0049 scan category column is absent".to_owned())?;
        let quantities = batch
            .column_by_name("quantity")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| "RFC-0049 scan quantity column is absent".to_owned())?;
        let counts = batch
            .column_by_name("row_count")
            .ok_or_else(|| "RFC-0049 scan row-count column is absent".to_owned())?;
        let sums = batch
            .column_by_name("quantity_sum")
            .ok_or_else(|| "RFC-0049 scan quantity-sum column is absent".to_owned())?;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            anomalies = anomalies.saturating_add(u64::from(
                previous_key.is_some_and(|previous| previous >= key),
            ));
            previous_key = Some(key);
            projection.update(key.to_be_bytes());
            projection.update(tenants.value(row).to_be_bytes());
            projection.update(categories.value(row).to_be_bytes());
            projection.update(quantities.value(row).to_be_bytes());
            anomalies = anomalies.saturating_add(u64::from(
                array_u64(counts.as_ref(), row)?
                    != u64::try_from(expected_rows).unwrap_or(u64::MAX),
            ));
            anomalies = anomalies.saturating_add(u64::from(
                array_i64(sums.as_ref(), row)? != expected_quantity_sum,
            ));
        }
    }
    let ordered_projection_sha256 = format!("{:x}", projection.finalize());
    anomalies = anomalies.saturating_add(u64::from(
        ordered_projection_sha256 != oracle.fixture.ordered_projection_sha256,
    ));
    let query_elapsed_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let measured_finished_unix_nanos = unix_nanos();
    let point_subject = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => T28TypedPointSubjectV1::C0IndexedRow,
        T28TypedScanSubjectV1::C5v2AlignedColumnar => T28TypedPointSubjectV1::C5v2AlignedColumnar,
        T28TypedScanSubjectV1::C5ColumnarMain => unreachable!(),
    };
    let provider = evaluate_provider_events(&attempts.events(), point_subject)?;
    let source = source_stats.snapshot();
    let columnar = aligned_scan
        .as_ref()
        .map(T28AlignedScan::source_snapshot)
        .unwrap_or_default();
    let projection_fetch_requests = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => provider.attempts,
        T28TypedScanSubjectV1::C5v2AlignedColumnar => columnar.projection_fetch_requests,
        T28TypedScanSubjectV1::C5ColumnarMain => unreachable!(),
    };
    let peak_fetch_bytes = match subject {
        T28TypedScanSubjectV1::C0IndexedRow => provider.maximum_response_bytes,
        T28TypedScanSubjectV1::C5v2AlignedColumnar => columnar.peak_fetch_bytes,
        T28TypedScanSubjectV1::C5ColumnarMain => unreachable!(),
    };
    if subject == T28TypedScanSubjectV1::C5v2AlignedColumnar
        && (projection_fetch_requests == 0 || projection_fetch_requests > 64)
    {
        return Err("RFC-0049 C5v2 scan exceeded its projection GET budget".to_owned());
    }
    let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX);
    let mut receipt = T28TypedScanPositionReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        execution_plan_sha256: plan.execution_plan_sha256,
        fixture_id: opened.fixture().fixture_id.clone(),
        root_sha256: opened.fixture().root_sha256.clone(),
        subject,
        trace_seed,
        query: plan.scan_query,
        configured_range_fetch_concurrency: 1,
        observed_peak_range_fetch_concurrency: provider.peak_inflight,
        resident_metadata_bytes,
        rows: rows_u64,
        ordered_projection_sha256,
        quantity_sum: expected_quantity_sum.to_string(),
        query_elapsed_nanos,
        rows_per_second: rows_u64 as f64 / (query_elapsed_nanos as f64 / 1_000_000_000.0),
        provider_attempts: provider.attempts,
        response_bytes: provider.response_bytes,
        full_object_requests: provider.full_object_requests,
        list_requests: provider.list_requests,
        put_requests: provider.put_requests,
        delete_requests: provider.delete_requests,
        missing_expected_generation_requests: provider.missing_expected_generation_requests,
        returned_generation_mismatches: provider.returned_generation_mismatches,
        provider_errors: provider.errors,
        source_scan_plans: source.scan_plans,
        source_projection_pushdown_plans: source.projection_pushdown_plans,
        source_stripes: source.stripes_read,
        source_batches: source.batches_emitted,
        source_rows: source.rows_emitted,
        peak_arrow_batch_rows: source.peak_batch_rows,
        peak_arrow_batch_bytes: source.peak_batch_bytes,
        projection_fetch_requests,
        peak_fetch_bytes,
        opaque_payload_requests: columnar.payload_requests,
        opaque_payload_response_bytes: columnar.payload_response_bytes,
        correctness_anomalies: anomalies,
        process_id: std::process::id(),
        measured_started_unix_nanos,
        measured_finished_unix_nanos,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

fn array_u64(array: &dyn arrow::array::Array, row: usize) -> Result<u64, String> {
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        Ok(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        u64::try_from(values.value(row)).map_err(|error| error.to_string())
    } else {
        Err("RFC-0048 scan count has the wrong type".to_owned())
    }
}

fn array_i64(array: &dyn arrow::array::Array, row: usize) -> Result<i64, String> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|values| values.value(row))
        .ok_or_else(|| "RFC-0048 scan sum has the wrong type".to_owned())
}

struct ProviderEvaluation {
    attempts: u64,
    response_bytes: u64,
    maximum_point_bytes_upper_bound: u64,
    full_object_requests: u64,
    list_requests: u64,
    put_requests: u64,
    delete_requests: u64,
    missing_expected_generation_requests: u64,
    returned_generation_mismatches: u64,
    errors: u64,
    latencies: Vec<u64>,
    maximum_response_bytes: u64,
    peak_inflight: u64,
}

#[allow(clippy::too_many_lines)]
fn evaluate_provider_events(
    events: &[ProviderAttemptEventV1],
    subject: T28TypedPointSubjectV1,
) -> Result<ProviderEvaluation, String> {
    let mut by_operation = BTreeMap::<u64, Vec<&ProviderAttemptEventV1>>::new();
    for event in events {
        by_operation
            .entry(event.operation_id)
            .or_default()
            .push(event);
    }
    let mut evaluation = ProviderEvaluation {
        attempts: 0,
        response_bytes: 0,
        maximum_point_bytes_upper_bound: 0,
        full_object_requests: 0,
        list_requests: 0,
        put_requests: 0,
        delete_requests: 0,
        missing_expected_generation_requests: 0,
        returned_generation_mismatches: 0,
        errors: 0,
        latencies: Vec::new(),
        maximum_response_bytes: 0,
        peak_inflight: 0,
    };
    let mut intervals = Vec::new();
    let mut maximum_projection = 0_u64;
    let mut maximum_payload = 0_u64;
    let mut maximum_other = 0_u64;
    for pair in by_operation.values() {
        if pair.len() != 2
            || pair[0].phase != ProviderAttemptPhase::Started
            || pair[1].phase != ProviderAttemptPhase::Completed
            || pair[0].attempt_ordinal != 1
            || pair[1].attempt_ordinal != 1
        {
            return Err(
                "RFC-0048 provider attempt lifecycle is not one start and completion".to_owned(),
            );
        }
        let started = pair[0];
        let completed = pair[1];
        evaluation.attempts = evaluation.attempts.saturating_add(1);
        evaluation.response_bytes = evaluation
            .response_bytes
            .saturating_add(completed.response_payload_bytes);
        evaluation.maximum_response_bytes = evaluation
            .maximum_response_bytes
            .max(completed.response_payload_bytes);
        evaluation.latencies.push(completed.elapsed_nanos);
        intervals.push((
            completed.started_monotonic_nanos,
            completed
                .started_monotonic_nanos
                .saturating_add(completed.elapsed_nanos),
        ));
        evaluation.full_object_requests = evaluation.full_object_requests.saturating_add(
            u64::from(started.api == "get" && started.requested_range.is_none()),
        );
        evaluation.list_requests = evaluation
            .list_requests
            .saturating_add(u64::from(started.api == "list"));
        evaluation.put_requests = evaluation
            .put_requests
            .saturating_add(u64::from(started.api == "put"));
        evaluation.delete_requests = evaluation
            .delete_requests
            .saturating_add(u64::from(started.api == "delete"));
        evaluation.missing_expected_generation_requests = evaluation
            .missing_expected_generation_requests
            .saturating_add(u64::from(
                started
                    .expected_revision
                    .as_ref()
                    .and_then(|revision| revision.version.as_deref())
                    .is_none(),
            ));
        evaluation.returned_generation_mismatches = evaluation
            .returned_generation_mismatches
            .saturating_add(u64::from(
                completed
                    .returned_revision
                    .as_ref()
                    .and_then(|revision| revision.version.as_deref())
                    != started
                        .expected_revision
                        .as_ref()
                        .and_then(|revision| revision.version.as_deref()),
            ));
        evaluation.errors = evaluation
            .errors
            .saturating_add(u64::from(completed.result.as_deref() != Some("ok")));
        if started.object_key.ends_with("projection.okcp")
            || started.object_key.ends_with("projection.okp2")
        {
            maximum_projection = maximum_projection.max(completed.response_payload_bytes);
        } else if started.object_key.ends_with("payload.okcv")
            || started.object_key.ends_with("payload.okv2")
        {
            maximum_payload = maximum_payload.max(completed.response_payload_bytes);
        } else {
            maximum_other = maximum_other.max(completed.response_payload_bytes);
        }
    }
    evaluation.latencies.sort_unstable();
    evaluation.maximum_point_bytes_upper_bound = match subject {
        T28TypedPointSubjectV1::C0IndexedRow => maximum_other,
        T28TypedPointSubjectV1::C5ColumnarMain | T28TypedPointSubjectV1::C5v2AlignedColumnar => {
            maximum_projection.saturating_add(maximum_payload)
        }
    };
    evaluation.peak_inflight = peak_inflight(&intervals);
    Ok(evaluation)
}

#[derive(Clone)]
struct CorrelatedProviderAttempt {
    observation: T28AlignedProviderAttemptV2,
    finished_monotonic_nanos: u64,
}

#[allow(clippy::too_many_lines)]
fn correlate_provider_events(
    events: &[ProviderAttemptEventV1],
    points: &[MeasuredPoint],
    subject: T28TypedPointSubjectV1,
    fixture: &T28AlignedFixtureV1,
) -> Result<Vec<T28AlignedPointOperationV2>, String> {
    let mut by_provider_operation = BTreeMap::<u64, Vec<&ProviderAttemptEventV1>>::new();
    for event in events {
        by_provider_operation
            .entry(event.operation_id)
            .or_default()
            .push(event);
    }
    let mut by_logical_operation = BTreeMap::<u64, Vec<CorrelatedProviderAttempt>>::new();
    for pair in by_provider_operation.values() {
        if pair.len() != 2
            || pair[0].phase != ProviderAttemptPhase::Started
            || pair[1].phase != ProviderAttemptPhase::Completed
            || pair[0].logical_operation_id.is_none()
            || pair[0].logical_operation_id != pair[1].logical_operation_id
            || pair[0].started_unix_nanos != pair[1].started_unix_nanos
            || pair[0].started_monotonic_nanos != pair[1].started_monotonic_nanos
            || pair[1].elapsed_nanos == 0
        {
            return Err("RFC-0049 provider attempt is not bound to one logical point".to_owned());
        }
        let logical_operation_id = pair[0]
            .logical_operation_id
            .ok_or_else(|| "RFC-0049 provider attempt omitted its logical point".to_owned())?;
        let started = pair[0];
        let completed = pair[1];
        let object_role = match subject {
            T28TypedPointSubjectV1::C0IndexedRow => T28AlignedObjectRoleV2::IndexedRow,
            T28TypedPointSubjectV1::C5v2AlignedColumnar
                if started.object_key.ends_with("projection.okp2") =>
            {
                T28AlignedObjectRoleV2::Projection
            }
            T28TypedPointSubjectV1::C5v2AlignedColumnar
                if started.object_key.ends_with("payload.okv2") =>
            {
                T28AlignedObjectRoleV2::Payload
            }
            _ => return Err("RFC-0049 provider attempt has an unexpected object role".to_owned()),
        };
        let requested_range = started
            .requested_range
            .clone()
            .ok_or_else(|| "RFC-0049 provider attempt is not a bounded range GET".to_owned())?;
        let returned_range = completed
            .returned_range
            .clone()
            .ok_or_else(|| "RFC-0049 provider attempt omitted its returned range".to_owned())?;
        let expected_generation = started
            .expected_revision
            .as_ref()
            .and_then(|revision| revision.version.clone())
            .ok_or_else(|| {
                "RFC-0049 provider attempt omitted its expected generation".to_owned()
            })?;
        let returned_generation = completed
            .returned_revision
            .as_ref()
            .and_then(|revision| revision.version.clone())
            .ok_or_else(|| {
                "RFC-0049 provider attempt omitted its returned generation".to_owned()
            })?;
        let (expected_prefix, expected_role) = match (subject, object_role) {
            (T28TypedPointSubjectV1::C0IndexedRow, T28AlignedObjectRoleV2::IndexedRow) => (
                fixture.source_c0_prefix.as_str(),
                TypedLayoutObjectRoleV1::Data,
            ),
            (T28TypedPointSubjectV1::C5v2AlignedColumnar, T28AlignedObjectRoleV2::Projection) => (
                fixture.candidate.prefix.as_str(),
                TypedLayoutObjectRoleV1::Projection,
            ),
            (T28TypedPointSubjectV1::C5v2AlignedColumnar, T28AlignedObjectRoleV2::Payload) => (
                fixture.candidate.prefix.as_str(),
                TypedLayoutObjectRoleV1::Payload,
            ),
            _ => return Err("RFC-0049 provider role differs from its subject".to_owned()),
        };
        let expected_object = match subject {
            T28TypedPointSubjectV1::C0IndexedRow => find_exact_fixture_object(
                &fixture.source_c0.objects,
                expected_prefix,
                expected_role,
                &started.object_key,
            ),
            T28TypedPointSubjectV1::C5v2AlignedColumnar => find_exact_fixture_object(
                &fixture.candidate.objects,
                expected_prefix,
                expected_role,
                &started.object_key,
            ),
            T28TypedPointSubjectV1::C5ColumnarMain => None,
        }
        .ok_or_else(|| "RFC-0049 fixture omits an expected point object".to_owned())?;
        let expected_key = fixture_object_key(expected_prefix, expected_object);
        if started.api != "get"
            || started.object_key != expected_key
            || completed.object_key != expected_key
            || expected_generation != expected_object.generation
            || returned_generation != expected_object.generation
            || requested_range != returned_range
            || requested_range.end > expected_object.length
            || completed.response_payload_bytes
                != requested_range.end.saturating_sub(requested_range.start)
        {
            return Err("RFC-0049 provider attempt differs from its fixture descriptor".to_owned());
        }
        let finished_monotonic_nanos = completed
            .started_monotonic_nanos
            .checked_add(completed.elapsed_nanos)
            .ok_or_else(|| "RFC-0049 provider completion overflow".to_owned())?;
        by_logical_operation
            .entry(logical_operation_id)
            .or_default()
            .push(CorrelatedProviderAttempt {
                observation: T28AlignedProviderAttemptV2 {
                    api: completed.api.clone(),
                    object_role,
                    object_key: completed.object_key.clone(),
                    requested_range,
                    returned_range,
                    expected_generation,
                    returned_generation,
                    response_payload_bytes: completed.response_payload_bytes,
                    started_monotonic_nanos: completed.started_monotonic_nanos,
                    elapsed_nanos: completed.elapsed_nanos,
                    result: completed.result.clone().unwrap_or_default(),
                },
                finished_monotonic_nanos,
            });
    }
    if by_logical_operation.len() != points.len() {
        return Err("RFC-0049 provider correlation does not cover every point".to_owned());
    }

    let expected_attempts =
        usize::try_from(subject.maximum_requests_per_point()).map_err(|error| error.to_string())?;
    let require_overlap = subject == T28TypedPointSubjectV1::C5v2AlignedColumnar;
    let mut correlated = Vec::with_capacity(points.len());
    for point in points {
        let calls = by_logical_operation
            .get(&point.ordinal)
            .ok_or_else(|| "RFC-0049 logical point has no provider attempts".to_owned())?;
        if calls.len() != expected_attempts {
            return Err("RFC-0049 logical point has the wrong provider fanout".to_owned());
        }
        let earliest_start = calls
            .iter()
            .map(|call| call.observation.started_monotonic_nanos)
            .min()
            .ok_or_else(|| "RFC-0049 logical point has no provider start".to_owned())?;
        let latest_start = calls
            .iter()
            .map(|call| call.observation.started_monotonic_nanos)
            .max()
            .ok_or_else(|| "RFC-0049 logical point has no provider start".to_owned())?;
        let earliest_finish = calls
            .iter()
            .map(|call| call.finished_monotonic_nanos)
            .min()
            .ok_or_else(|| "RFC-0049 logical point has no provider completion".to_owned())?;
        let latest_finish = calls
            .iter()
            .map(|call| call.finished_monotonic_nanos)
            .max()
            .ok_or_else(|| "RFC-0049 logical point has no provider completion".to_owned())?;
        let provider_pair_max_nanos = calls
            .iter()
            .map(|call| call.observation.elapsed_nanos)
            .max()
            .ok_or_else(|| "RFC-0049 logical point has no provider latency".to_owned())?;
        let provider_pair_overlapped = calls.len() > 1 && latest_start < earliest_finish;
        if provider_pair_overlapped != require_overlap {
            return Err("RFC-0049 logical point violated its provider overlap contract".to_owned());
        }
        let pair_completion_nanos = latest_finish.saturating_sub(earliest_start);
        let mut observations = calls
            .iter()
            .map(|call| call.observation.clone())
            .collect::<Vec<_>>();
        observations.sort_by_key(|attempt| attempt.object_role);
        correlated.push(T28AlignedPointOperationV2 {
            ordinal: point.ordinal,
            end_to_end_nanos: point.latency_nanos,
            provider_pair_max_nanos,
            local_residual_nanos: point.latency_nanos.saturating_sub(pair_completion_nanos),
            pair_start_skew_nanos: latest_start.saturating_sub(earliest_start),
            pair_completion_nanos,
            provider_attempts: u64::try_from(calls.len()).unwrap_or(u64::MAX),
            provider_pair_overlapped,
            attempts: observations,
        });
    }
    Ok(correlated)
}

fn find_exact_fixture_object<'a>(
    objects: &'a [TypedLayoutObjectIdentityV1],
    prefix: &str,
    role: TypedLayoutObjectRoleV1,
    observed_key: &str,
) -> Option<&'a TypedLayoutObjectIdentityV1> {
    objects
        .iter()
        .find(|object| object.role == role && fixture_object_key(prefix, object) == observed_key)
}

fn fixture_object_key(prefix: &str, object: &TypedLayoutObjectIdentityV1) -> String {
    format!(
        "{}/{}",
        prefix.trim_matches('/'),
        object.key.trim_start_matches('/')
    )
}

fn peak_inflight(intervals: &[(u64, u64)]) -> u64 {
    let mut events = intervals
        .iter()
        .flat_map(|(start, end)| [(*start, 1_i8), (*end, -1_i8)])
        .collect::<Vec<_>>();
    events.sort_unstable_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut active = 0_i64;
    let mut peak = 0_i64;
    for (_, delta) in events {
        active = active.saturating_add(i64::from(delta)).max(0);
        peak = peak.max(active);
    }
    u64::try_from(peak).unwrap_or(u64::MAX)
}

fn nearest_rank(values: &[u64], numerator: usize, denominator: usize) -> Result<u64, String> {
    if values.is_empty() || numerator == 0 || denominator == 0 || numerator > denominator {
        return Err("invalid RFC-0048 nearest-rank input".to_owned());
    }
    let rank = values
        .len()
        .saturating_mul(numerator)
        .saturating_add(denominator - 1)
        / denominator;
    values
        .get(rank.saturating_sub(1).min(values.len() - 1))
        .copied()
        .ok_or_else(|| "RFC-0048 nearest-rank sample is absent".to_owned())
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::{
        find_exact_fixture_object, nearest_rank, T28AlignedMediaObservationV1,
        T28AlignedObjectRoleV2, T28AlignedPointOperationV2, T28AlignedPointPositionReceiptV2,
        T28AlignedProviderAttemptV2, T28TypedPointPositionReceiptV1, T28TypedPointSubjectV1,
    };
    use crate::t28_layout::{TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1};

    #[test]
    fn nearest_rank_uses_frozen_tail_semantics() {
        let values = (1..=1_000).collect::<Vec<_>>();
        assert_eq!(nearest_rank(&values, 50, 100).expect("p50"), 500);
        assert_eq!(nearest_rank(&values, 99, 100).expect("p99"), 990);
        assert_eq!(nearest_rank(&values, 999, 1_000).expect("p999"), 999);
    }

    #[test]
    fn exact_fixture_match_selects_the_addressed_object_among_same_role_descriptors() {
        let objects = [
            object("layout/row/data/sha256/first", "101"),
            object("layout/row/data/sha256/second", "202"),
        ];
        let selected = find_exact_fixture_object(
            &objects,
            "runs/source",
            TypedLayoutObjectRoleV1::Data,
            "runs/source/layout/row/data/sha256/second",
        )
        .expect("second data object");

        assert_eq!(selected.generation, "202");
    }

    #[test]
    fn persisted_replay_accepts_the_addressed_object_among_same_role_descriptors() {
        let mut base = T28TypedPointPositionReceiptV1 {
            schema_version: 1,
            execution_plan_sha256: "a".repeat(64),
            fixture_id: "b".repeat(64),
            root_sha256: "c".repeat(64),
            subject: T28TypedPointSubjectV1::C0IndexedRow,
            trace_seed: 5_701,
            measured_operations: 1,
            concurrent_tasks: 8,
            warmup_canary_reads: 128,
            resident_metadata_bytes: 1,
            measured_provider_attempts: 1,
            measured_response_bytes: 10,
            maximum_point_bytes_upper_bound: 10,
            maximum_attempts_per_point: 1,
            full_object_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            missing_expected_generation_requests: 0,
            returned_generation_mismatches: 0,
            provider_errors: 0,
            correctness_anomalies: 0,
            point_pairs: 0,
            overlapping_point_pairs: 0,
            latency_nanos: vec![100],
            p50_latency_nanos: 100,
            p95_latency_nanos: 100,
            p99_latency_nanos: 100,
            p999_latency_nanos: 100,
            provider_latency_nanos: vec![80],
            provider_p50_latency_nanos: 80,
            provider_p95_latency_nanos: 80,
            provider_p99_latency_nanos: 80,
            provider_p999_latency_nanos: 80,
            wall_elapsed_nanos: 100,
            process_id: 1,
            measured_started_unix_nanos: 1,
            measured_finished_unix_nanos: 2,
            receipt_sha256: String::new(),
        };
        base.receipt_sha256 = base.calculated_sha256().expect("base digest");
        let key = "runs/source/layout/row/data/sha256/second";
        let sample = T28AlignedPointOperationV2 {
            ordinal: 0,
            end_to_end_nanos: 100,
            provider_pair_max_nanos: 80,
            local_residual_nanos: 20,
            pair_start_skew_nanos: 0,
            pair_completion_nanos: 80,
            provider_attempts: 1,
            provider_pair_overlapped: false,
            attempts: vec![T28AlignedProviderAttemptV2 {
                api: "get".to_owned(),
                object_role: T28AlignedObjectRoleV2::IndexedRow,
                object_key: key.to_owned(),
                requested_range: 0..10,
                returned_range: 0..10,
                expected_generation: "202".to_owned(),
                returned_generation: "202".to_owned(),
                response_payload_bytes: 10,
                started_monotonic_nanos: 10,
                elapsed_nanos: 80,
                result: "ok".to_owned(),
            }],
        };
        let receipt =
            T28AlignedPointPositionReceiptV2::seal(base, vec![sample]).expect("correlated receipt");
        let control_objects = vec![
            object("layout/row/data/sha256/first", "101"),
            object("layout/row/data/sha256/second", "202"),
        ];
        let candidate_objects = vec![object("layout/candidate/data", "303")];
        let mut media = T28AlignedMediaObservationV1 {
            fixture_id: "b".repeat(64),
            root_sha256: "c".repeat(64),
            canonical_history_sha256: "d".repeat(64),
            candidate_placement_envelope_sha256: "e".repeat(64),
            source_root_sha256: "f".repeat(64),
            source_placement_envelope_sha256: "1".repeat(64),
            control_prefix: "runs/source".to_owned(),
            candidate_prefix: "runs/candidate".to_owned(),
            control_closure_sha256: "2".repeat(64),
            candidate_closure_sha256: "3".repeat(64),
            control_total_media_bytes: 8_192,
            candidate_total_media_bytes: 4_096,
            control_objects,
            candidate_objects,
            source_c0_reused_by_reference: true,
            observation_sha256: String::new(),
        };
        media.observation_sha256 = media.calculated_sha256().expect("media digest");

        receipt
            .validate_against_media(&media)
            .expect("second same-role object remains valid during persisted replay");
    }

    fn object(key: &str, generation: &str) -> TypedLayoutObjectIdentityV1 {
        TypedLayoutObjectIdentityV1 {
            role: TypedLayoutObjectRoleV1::Data,
            key: key.to_owned(),
            generation: generation.to_owned(),
            length: 4_096,
            sha256: "a".repeat(64),
        }
    }
}
