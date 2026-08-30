//! Fresh-process concurrent point positions for the RFC-0046 GCS curve.

use crate::object_fixture::{open_generation_pinned_fixture_lazy, FixturePlacementLocatorV1};
use crate::provider_attempt::{
    ProviderAttemptBackend, ProviderAttemptEventV1, ProviderAttemptPhase,
};
use crate::t28_cold_point::{T28PointPlanV2, T28PointSubject};
use futures_util::{stream, StreamExt};
use okv_object::{content_sha256, read_planned_point, Backend, PointRead, PointReadOutcome};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const POSITION_SCHEMA_VERSION: u32 = 1;

/// One measured read completed inside a concurrent position.
struct MeasuredPoint {
    ordinal: u64,
    latency_nanos: u64,
    provider_latency_nanos: u64,
    local_residual_nanos: u64,
    read: PointRead,
    provider_events: Vec<ProviderAttemptEventV1>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AttemptIdentity {
    object_key: String,
    range_start: u64,
    range_end: u64,
    object_length: u64,
    response_bytes: u64,
}

/// Immutable output from one fresh T28 candidate or raw-control process.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28PointPositionReceiptV1 {
    pub schema_version: u32,
    pub plan_sha256: String,
    pub subject: T28PointSubject,
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub position_in_block: u64,
    pub concurrent_clients: u64,
    pub measured_operations: u64,
    pub warm_index_objects: u64,
    pub warm_provider_attempts: u64,
    pub warm_response_bytes: u64,
    pub measured_provider_attempts: u64,
    pub measured_response_bytes: u64,
    pub full_data_requests: u64,
    pub list_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
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
    pub local_residual_nanos: Vec<u64>,
    pub local_residual_p50_nanos: u64,
    pub local_residual_p95_nanos: u64,
    pub local_residual_p99_nanos: u64,
    pub local_residual_p999_nanos: u64,
    pub wall_elapsed_nanos: u64,
    pub machine_id: String,
    pub linux_boot_id: String,
    pub process_id: u32,
    pub linux_process_start_ticks: u64,
    pub measured_started_unix_nanos: u64,
    pub measured_finished_unix_nanos: u64,
    pub receipt_sha256: String,
}

impl T28PointPositionReceiptV1 {
    /// Return the canonical SHA-256 with the digest field excluded.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt cannot be serialized.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

/// Execute one metadata-warm, data-cold position without retaining data blocks.
///
/// # Errors
///
/// Returns an error for plan drift, invalid process parameters, metadata or
/// data corruption, a wrong value, an extra provider attempt, or any request
/// outside the sealed data ranges.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_t28_point_position(
    backend: Arc<dyn Backend>,
    placement: &FixturePlacementLocatorV1,
    expected_envelope_sha256: &str,
    plan: &T28PointPlanV2,
    subject: T28PointSubject,
    trace_seed: u64,
    block_ordinal: u64,
    position_in_block: u64,
    concurrent_clients: usize,
) -> Result<T28PointPositionReceiptV1, String> {
    plan.validate(placement)?;
    if trace_seed == 0
        || position_in_block > 3
        || concurrent_clients == 0
        || plan.cache_state != crate::t28_cold_point::T28CacheState::MetadataWarmDataCold
    {
        return Err("invalid T28 point position parameters".to_owned());
    }
    let observed = Arc::new(ProviderAttemptBackend::new(backend.clone(), subject.id())?);
    let reader_backend: Arc<dyn Backend> = observed.clone();
    let reader = Arc::new(
        open_generation_pinned_fixture_lazy(
            reader_backend,
            placement,
            expected_envelope_sha256,
            &placement.fixture.fixture_id,
            placement.base_version,
        )
        .await?,
    );
    let point_plans = plan
        .operations
        .iter()
        .map(|operation| operation.point.clone())
        .collect::<Vec<_>>();
    let prepared = Arc::new(reader.prepare_point_indexes(&point_plans).await?);
    let warm_events = observed.events();
    let (warm_provider_attempts, warm_response_bytes) = summarize_completed_gets(&warm_events)?;

    let machine_id = read_identity("/etc/machine-id")?;
    let linux_boot_id = read_identity("/proc/sys/kernel/random/boot_id")?;
    let linux_process_start_ticks = linux_process_start_ticks()?;
    let measured_started_unix_nanos = unix_nanos();
    let position_started = Instant::now();
    let operations = plan.operations.clone();
    let results = stream::iter(operations)
        .map(|operation| {
            let reader = reader.clone();
            let prepared = prepared.clone();
            let backend = backend.clone();
            async move {
                let observed = ProviderAttemptBackend::new(backend, subject.id())?;
                let started = Instant::now();
                let read = match subject {
                    T28PointSubject::Candidate => {
                        let derived = prepared.plan_point(&operation.point.key)?;
                        if derived != operation.point {
                            return Err(
                                "T28 candidate prepared index derived a different range".to_owned()
                            );
                        }
                        reader.validate_planned_point(&operation.point)?;
                        read_planned_point(
                            &observed,
                            &operation.point.data_key,
                            None,
                            &operation.point.block,
                            &operation.point.key,
                            operation.point.read_version,
                        )
                        .await?
                    }
                    T28PointSubject::RawRangeControl => {
                        read_planned_point(
                            &observed,
                            &operation.point.data_key,
                            None,
                            &operation.point.block,
                            &operation.point.key,
                            operation.point.read_version,
                        )
                        .await?
                    }
                };
                let latency_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                let provider_events = observed.events();
                let provider_latency_nanos = completed_provider_latency(&provider_events)?;
                Ok::<_, String>(MeasuredPoint {
                    ordinal: operation.ordinal,
                    latency_nanos,
                    provider_latency_nanos,
                    local_residual_nanos: latency_nanos.saturating_sub(provider_latency_nanos),
                    read,
                    provider_events,
                })
            }
        })
        .buffer_unordered(concurrent_clients)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let wall_elapsed_nanos =
        u64::try_from(position_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let measured_finished_unix_nanos = unix_nanos();
    validate_position(plan, subject, &results)?;

    let mut latency_nanos = results
        .iter()
        .map(|result| result.latency_nanos)
        .collect::<Vec<_>>();
    latency_nanos.sort_unstable();
    let mut provider_latency_nanos = results
        .iter()
        .map(|result| result.provider_latency_nanos)
        .collect::<Vec<_>>();
    provider_latency_nanos.sort_unstable();
    let mut local_residual_nanos = results
        .iter()
        .map(|result| result.local_residual_nanos)
        .collect::<Vec<_>>();
    local_residual_nanos.sort_unstable();
    let measured_response_bytes = results.iter().try_fold(0_u64, |total, result| {
        total
            .checked_add(result.read.data_bytes)
            .ok_or_else(|| "T28 position response-byte total overflow".to_owned())
    })?;
    let mut receipt = T28PointPositionReceiptV1 {
        schema_version: POSITION_SCHEMA_VERSION,
        plan_sha256: plan.plan_sha256.clone(),
        subject,
        trace_seed,
        block_ordinal,
        position_in_block,
        concurrent_clients: u64::try_from(concurrent_clients).unwrap_or(u64::MAX),
        measured_operations: u64::try_from(results.len()).unwrap_or(u64::MAX),
        warm_index_objects: u64::try_from(prepared.index_count()).unwrap_or(u64::MAX),
        warm_provider_attempts,
        warm_response_bytes,
        measured_provider_attempts: u64::try_from(results.len()).unwrap_or(u64::MAX),
        measured_response_bytes,
        full_data_requests: 0,
        list_requests: 0,
        put_requests: 0,
        delete_requests: 0,
        correctness_anomalies: 0,
        p50_latency_nanos: nearest_rank(&latency_nanos, 50, 100)?,
        p95_latency_nanos: nearest_rank(&latency_nanos, 95, 100)?,
        p99_latency_nanos: nearest_rank(&latency_nanos, 99, 100)?,
        p999_latency_nanos: nearest_rank(&latency_nanos, 999, 1_000)?,
        provider_p50_latency_nanos: nearest_rank(&provider_latency_nanos, 50, 100)?,
        provider_p95_latency_nanos: nearest_rank(&provider_latency_nanos, 95, 100)?,
        provider_p99_latency_nanos: nearest_rank(&provider_latency_nanos, 99, 100)?,
        provider_p999_latency_nanos: nearest_rank(&provider_latency_nanos, 999, 1_000)?,
        local_residual_p50_nanos: nearest_rank(&local_residual_nanos, 50, 100)?,
        local_residual_p95_nanos: nearest_rank(&local_residual_nanos, 95, 100)?,
        local_residual_p99_nanos: nearest_rank(&local_residual_nanos, 99, 100)?,
        local_residual_p999_nanos: nearest_rank(&local_residual_nanos, 999, 1_000)?,
        latency_nanos,
        provider_latency_nanos,
        local_residual_nanos,
        wall_elapsed_nanos,
        machine_id,
        linux_boot_id,
        process_id: std::process::id(),
        linux_process_start_ticks,
        measured_started_unix_nanos,
        measured_finished_unix_nanos,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    Ok(receipt)
}

fn validate_position(
    plan: &T28PointPlanV2,
    subject: T28PointSubject,
    results: &[MeasuredPoint],
) -> Result<(), String> {
    if results.len() != plan.operations.len() {
        return Err("T28 position operation or provider-attempt count mismatch".to_owned());
    }
    let mut ordered_results = results.iter().collect::<Vec<_>>();
    ordered_results.sort_by_key(|result| result.ordinal);
    for (operation, result) in plan.operations.iter().zip(ordered_results) {
        let PointReadOutcome::Value(value) = &result.read.outcome else {
            return Err("T28 position returned a non-value outcome".to_owned());
        };
        if result.ordinal != operation.ordinal
            || result.latency_nanos == 0
            || result.provider_latency_nanos == 0
            || result.provider_latency_nanos > result.latency_nanos
            || content_sha256(value) != operation.expected_value_sha256
            || result.read.data_bytes != operation.point.block.length
        {
            return Err("T28 position value, bytes, ordinal, or latency mismatch".to_owned());
        }
    }

    let mut expected = BTreeMap::<AttemptIdentity, u64>::new();
    for operation in &plan.operations {
        let range = operation.point.block.range()?;
        *expected
            .entry(AttemptIdentity {
                object_key: operation.point.data_key.clone(),
                range_start: range.start,
                range_end: range.end,
                object_length: operation.point.block.object_length,
                response_bytes: operation.point.block.length,
            })
            .or_default() += 1;
    }
    for result in results {
        let mut pair = result.provider_events.iter().collect::<Vec<_>>();
        pair.sort_by_key(|event| event.sequence);
        if pair.len() != 2 {
            return Err("T28 position provider operation is not one event pair".to_owned());
        }
        let started = pair[0];
        let completed = pair[1];
        let range = started
            .requested_range
            .as_ref()
            .ok_or_else(|| "T28 position provider range is absent".to_owned())?;
        let identity = AttemptIdentity {
            object_key: started.object_key.clone(),
            range_start: range.start,
            range_end: range.end,
            object_length: completed.object_length.unwrap_or(0),
            response_bytes: completed.response_payload_bytes,
        };
        if started.phase != ProviderAttemptPhase::Started
            || completed.phase != ProviderAttemptPhase::Completed
            || started.operation_id != completed.operation_id
            || started.sequence >= completed.sequence
            || started.attempt_ordinal != 1
            || completed.attempt_ordinal != 1
            || started.subject != subject.id()
            || completed.subject != subject.id()
            || started.provider != "gcs"
            || completed.provider != "gcs"
            || started.api != "get"
            || completed.api != "get"
            || started.object_key != completed.object_key
            || started.requested_range != completed.requested_range
            || completed.returned_range != completed.requested_range
            || completed.result.as_deref() != Some("ok")
            || started.request_payload_bytes != 0
            || completed.request_payload_bytes != 0
        {
            return Err("T28 position provider event pair differs from its contract".to_owned());
        }
        let count = expected
            .get_mut(&identity)
            .ok_or_else(|| "T28 position fetched an unplanned data range".to_owned())?;
        if *count == 0 {
            return Err("T28 position fetched one planned range too many times".to_owned());
        }
        *count -= 1;
    }
    if expected.values().any(|count| *count != 0) {
        return Err("T28 position omitted one or more planned data ranges".to_owned());
    }
    Ok(())
}

fn completed_provider_latency(events: &[ProviderAttemptEventV1]) -> Result<u64, String> {
    if events.len() != 2 {
        return Err("T28 measured provider attempt is not one event pair".to_owned());
    }
    events
        .iter()
        .find(|event| event.phase == ProviderAttemptPhase::Completed)
        .map(|event| event.elapsed_nanos)
        .filter(|elapsed| *elapsed > 0)
        .ok_or_else(|| "T28 measured provider completion latency is absent".to_owned())
}

fn summarize_completed_gets(events: &[ProviderAttemptEventV1]) -> Result<(u64, u64), String> {
    if events.iter().any(|event| {
        event.api != "get"
            || event.attempt_ordinal != 1
            || (event.phase == ProviderAttemptPhase::Completed
                && event.result.as_deref() != Some("ok"))
    }) {
        return Err("T28 metadata warmup emitted a non-GET or failed attempt".to_owned());
    }
    let completed = events
        .iter()
        .filter(|event| event.phase == ProviderAttemptPhase::Completed)
        .collect::<Vec<_>>();
    if events.len() != completed.len() * 2 {
        return Err("T28 metadata warmup provider events are incomplete".to_owned());
    }
    let bytes = completed.iter().try_fold(0_u64, |total, event| {
        total
            .checked_add(event.response_payload_bytes)
            .ok_or_else(|| "T28 metadata warmup byte total overflow".to_owned())
    })?;
    Ok((u64::try_from(completed.len()).unwrap_or(u64::MAX), bytes))
}

fn nearest_rank(sorted: &[u64], numerator: usize, denominator: usize) -> Result<u64, String> {
    if sorted.is_empty() || numerator == 0 || numerator > denominator {
        return Err("invalid T28 nearest-rank input".to_owned());
    }
    let rank = sorted
        .len()
        .checked_mul(numerator)
        .and_then(|value| value.checked_add(denominator - 1))
        .map(|value| value / denominator)
        .ok_or_else(|| "T28 nearest-rank overflow".to_owned())?;
    sorted
        .get(rank.saturating_sub(1))
        .copied()
        .ok_or_else(|| "T28 nearest-rank sample is absent".to_owned())
}

fn read_identity(path: &str) -> Result<String, String> {
    let value = fs::read_to_string(path).map_err(|error| error.to_string())?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(format!("T28 process identity is empty at {path}"));
    }
    Ok(value)
}

fn linux_process_start_ticks() -> Result<u64, String> {
    let stat = fs::read_to_string("/proc/self/stat").map_err(|error| error.to_string())?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| "T28 process stat comm field is malformed".to_owned())?;
    stat.get(close + 2..)
        .and_then(|fields| fields.split_whitespace().nth(19))
        .ok_or_else(|| "T28 process start ticks are absent".to_owned())?
        .parse::<u64>()
        .map_err(|error| error.to_string())
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{nearest_rank, T28PointPositionReceiptV1};
    use crate::t28_cold_point::T28PointSubject;

    #[test]
    fn nearest_rank_preserves_tail_samples() {
        let samples = (1_u64..=1_000).collect::<Vec<_>>();
        assert_eq!(nearest_rank(&samples, 50, 100).expect("p50"), 500);
        assert_eq!(nearest_rank(&samples, 95, 100).expect("p95"), 950);
        assert_eq!(nearest_rank(&samples, 99, 100).expect("p99"), 990);
        assert_eq!(nearest_rank(&samples, 999, 1_000).expect("p99.9"), 999);
    }

    #[test]
    fn point_position_receipt_matches_frozen_schema() {
        let mut receipt = T28PointPositionReceiptV1 {
            schema_version: 1,
            plan_sha256: "a".repeat(64),
            subject: T28PointSubject::Candidate,
            trace_seed: 1,
            block_ordinal: 0,
            position_in_block: 0,
            concurrent_clients: 8,
            measured_operations: 1,
            warm_index_objects: 1,
            warm_provider_attempts: 3,
            warm_response_bytes: 1,
            measured_provider_attempts: 1,
            measured_response_bytes: 1,
            full_data_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            correctness_anomalies: 0,
            latency_nanos: vec![2],
            p50_latency_nanos: 2,
            p95_latency_nanos: 2,
            p99_latency_nanos: 2,
            p999_latency_nanos: 2,
            provider_latency_nanos: vec![1],
            provider_p50_latency_nanos: 1,
            provider_p95_latency_nanos: 1,
            provider_p99_latency_nanos: 1,
            provider_p999_latency_nanos: 1,
            local_residual_nanos: vec![1],
            local_residual_p50_nanos: 1,
            local_residual_p95_nanos: 1,
            local_residual_p99_nanos: 1,
            local_residual_p999_nanos: 1,
            wall_elapsed_nanos: 2,
            machine_id: "machine".to_owned(),
            linux_boot_id: "boot".to_owned(),
            process_id: 1,
            linux_process_start_ticks: 1,
            measured_started_unix_nanos: 1,
            measured_finished_unix_nanos: 2,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256().expect("calculate receipt SHA");
        let value = serde_json::to_value(receipt).expect("encode position receipt value");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t28-point-position-receipt-v1.schema.json"
        ))
        .expect("decode position receipt schema");
        jsonschema::validator_for(&schema)
            .expect("compile position receipt schema")
            .validate(&value)
            .expect("validate position receipt schema");
    }
}
