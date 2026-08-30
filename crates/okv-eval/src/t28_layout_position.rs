//! Fresh-process point positions for the RFC-0048 matched layout curve.

use crate::provider_attempt::{
    ProviderAttemptBackend, ProviderAttemptEventV1, ProviderAttemptPhase,
};
use crate::storage_layout::{
    t28_typed_point_outcome_sha256, T28ColumnarLayoutReader, T28OpenedTypedLayout,
    T28RowLayoutReader, T28TypedLayoutExecutionPlanV1,
};
use crate::t28_layout::{T28LayoutOracleV1, TypedLayoutPlacementLocatorV1};
use futures_util::{stream, StreamExt};
use okv_object::{content_sha256, Backend, PointReadOutcome};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RECEIPT_SCHEMA_VERSION: u32 = 1;

/// One subject in the matched point lane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28TypedPointSubjectV1 {
    C0IndexedRow,
    C5ColumnarMain,
}

impl T28TypedPointSubjectV1 {
    fn id(self) -> &'static str {
        match self {
            Self::C0IndexedRow => "c0_indexed_row",
            Self::C5ColumnarMain => "c5_columnar_main",
        }
    }

    fn maximum_requests_per_point(self) -> u64 {
        match self {
            Self::C0IndexedRow => 1,
            Self::C5ColumnarMain => 2,
        }
    }
}

enum PointReader {
    C0(Arc<T28RowLayoutReader>),
    C5(Arc<T28ColumnarLayoutReader>),
}

impl PointReader {
    async fn read(&self, key: u64, version: u64) -> Result<PointReadOutcome, String> {
        match self {
            Self::C0(reader) => reader.point(key, version).await.map(|read| read.outcome),
            Self::C5(reader) => reader.point(key, version).await,
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
}

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
    };
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
        evaluation.latencies.push(completed.elapsed_nanos);
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
        if started.object_key.ends_with("projection.okcp") {
            maximum_projection = maximum_projection.max(completed.response_payload_bytes);
        } else if started.object_key.ends_with("payload.okcv") {
            maximum_payload = maximum_payload.max(completed.response_payload_bytes);
        } else {
            maximum_other = maximum_other.max(completed.response_payload_bytes);
        }
    }
    evaluation.latencies.sort_unstable();
    evaluation.maximum_point_bytes_upper_bound = match subject {
        T28TypedPointSubjectV1::C0IndexedRow => maximum_other,
        T28TypedPointSubjectV1::C5ColumnarMain => {
            maximum_projection.saturating_add(maximum_payload)
        }
    };
    Ok(evaluation)
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

#[cfg(test)]
mod tests {
    use super::nearest_rank;

    #[test]
    fn nearest_rank_uses_frozen_tail_semantics() {
        let values = (1..=1_000).collect::<Vec<_>>();
        assert_eq!(nearest_rank(&values, 50, 100).expect("p50"), 500);
        assert_eq!(nearest_rank(&values, 99, 100).expect("p99"), 990);
        assert_eq!(nearest_rank(&values, 999, 1_000).expect("p999"), 999);
    }
}
