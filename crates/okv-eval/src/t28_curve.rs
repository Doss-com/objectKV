//! Frozen RFC-0046 T28 curve plan and aggregate receipt.

use crate::t28_cold_point::T28PointSubject;
use crate::t28_position::T28PointPositionReceiptV2;
use crate::telemetry::TelemetryFlushReport;
use okv_object::content_sha256;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const PLAN_SCHEMA_VERSION: u32 = 1;
const RUN_SCHEMA_VERSION: u32 = 1;

/// One frozen T28 corrected-execution plan.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurvePlanV1 {
    pub schema_version: u32,
    pub plan_id: String,
    pub status: String,
    pub fixture_logical_bytes: u64,
    pub cache_state: String,
    pub measured_operations_per_position: u64,
    pub concurrent_clients: u64,
    pub blocks_per_seed: u64,
    pub positions_per_block: u64,
    pub object_store_max_retries: u64,
    pub data_cache_bytes: u64,
    pub max_data_range_bytes: u64,
    pub fixture: T28CurveFixtureV1,
    pub reader: T28CurveReaderV1,
    pub original_end_to_end_gate: T28CurveRatioGateV1,
    pub local_residual_addendum: T28CurveLocalGateV1,
    pub seeds: Vec<T28CurveSeedV1>,
    pub stopping_rule: T28CurveStoppingRuleV1,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveFixtureV1 {
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub placement_envelope_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveReaderV1 {
    pub principal_email: String,
    pub credential_source: String,
    pub iam_receipt_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveRatioGateV1 {
    pub percentile_numerator: u64,
    pub percentile_denominator: u64,
    pub max_candidate_to_control_ratio_millionths: u64,
    pub require_every_block: bool,
    pub result_authority: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveLocalGateV1 {
    pub definition: String,
    pub percentile_numerator: u64,
    pub percentile_denominator: u64,
    pub max_candidate_to_control_ratio_millionths: u64,
    pub max_candidate_minus_control_nanos: u64,
    pub require_every_block: bool,
    pub claim_if_passed: String,
    pub forbidden_claim: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveSeedV1 {
    pub trace_seed: u64,
    pub plan_sha256: String,
    pub orders: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveStoppingRuleV1 {
    pub execute_once: bool,
    pub publish_on_pass_or_fail: bool,
    pub rerun_requires_new_plan_id: bool,
    pub retain_prior_diagnostic: bool,
}

/// Decoded plan plus the SHA-256 of its exact TOML bytes.
pub struct LoadedT28CurvePlanV1 {
    pub plan: T28CurvePlanV1,
    pub raw_sha256: String,
}

/// One expected child process in immutable execution order.
#[derive(Clone, Debug)]
pub struct T28ExpectedPositionV1 {
    pub trace_seed: u64,
    pub point_plan_sha256: String,
    pub block_ordinal: u64,
    pub position_in_block: u64,
    pub subject: T28PointSubject,
}

impl T28CurvePlanV1 {
    /// Decode and validate one exact plan.
    ///
    /// # Errors
    ///
    /// Returns an error for a raw digest mismatch, malformed TOML, an invalid
    /// boundary, or an order matrix that does not rotate by block and seed.
    pub fn decode(bytes: &[u8], expected_sha256: &str) -> Result<LoadedT28CurvePlanV1, String> {
        let raw_sha256 = content_sha256(bytes);
        if raw_sha256 != expected_sha256 {
            return Err("T28 curve plan raw SHA-256 mismatch".to_owned());
        }
        let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let plan: Self = toml::from_str(text).map_err(|error| error.to_string())?;
        plan.validate()?;
        Ok(LoadedT28CurvePlanV1 { plan, raw_sha256 })
    }

    /// Return every expected child position in execution order.
    ///
    /// # Errors
    ///
    /// Returns an error when a subject order is malformed.
    pub fn expected_positions(&self) -> Result<Vec<T28ExpectedPositionV1>, String> {
        let mut positions = Vec::with_capacity(
            self.seeds
                .len()
                .saturating_mul(usize::try_from(self.blocks_per_seed).unwrap_or(0))
                .saturating_mul(usize::try_from(self.positions_per_block).unwrap_or(0)),
        );
        for seed in &self.seeds {
            for (block_ordinal, order) in seed.orders.iter().enumerate() {
                for (position_in_block, subject) in parse_order(order)?.into_iter().enumerate() {
                    positions.push(T28ExpectedPositionV1 {
                        trace_seed: seed.trace_seed,
                        point_plan_sha256: seed.plan_sha256.clone(),
                        block_ordinal: u64::try_from(block_ordinal).unwrap_or(u64::MAX),
                        position_in_block: u64::try_from(position_in_block).unwrap_or(u64::MAX),
                        subject,
                    });
                }
            }
        }
        Ok(positions)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != PLAN_SCHEMA_VERSION
            || self.plan_id.trim().is_empty()
            || self.status != "frozen_before_corrected_execution"
            || self.fixture_logical_bytes != 1_073_741_824
            || self.cache_state != "metadata_warm_data_cold"
            || self.measured_operations_per_position != 1_024
            || self.concurrent_clients != 8
            || self.blocks_per_seed != 5
            || self.positions_per_block != 4
            || self.object_store_max_retries != 0
            || self.data_cache_bytes != 0
            || self.max_data_range_bytes != 65_536
            || self.fixture.project.trim().is_empty()
            || self.fixture.bucket.trim().is_empty()
            || self.fixture.region.trim().is_empty()
            || !valid_sha256(&self.fixture.placement_envelope_sha256)
            || self.reader.principal_email.trim().is_empty()
            || self.reader.credential_source != "gce_metadata_server"
            || !valid_sha256(&self.reader.iam_receipt_sha256)
            || self.original_end_to_end_gate.percentile_numerator != 99
            || self.original_end_to_end_gate.percentile_denominator != 100
            || self
                .original_end_to_end_gate
                .max_candidate_to_control_ratio_millionths
                != 1_250_000
            || !self.original_end_to_end_gate.require_every_block
            || self.original_end_to_end_gate.result_authority != "unchanged_rfc0046_gate"
            || self.local_residual_addendum.definition
                != "operation_end_to_end_nanos_minus_same_operation_provider_nanos"
            || self.local_residual_addendum.percentile_numerator != 99
            || self.local_residual_addendum.percentile_denominator != 100
            || self
                .local_residual_addendum
                .max_candidate_to_control_ratio_millionths
                != 1_250_000
            || self
                .local_residual_addendum
                .max_candidate_minus_control_nanos
                != 250_000
            || !self.local_residual_addendum.require_every_block
            || self.local_residual_addendum.claim_if_passed
                != "candidate_local_cold_point_overhead_is_bounded"
            || self.local_residual_addendum.forbidden_claim
                != "end_to_end_gcs_point_latency_is_admitted"
            || !self.stopping_rule.execute_once
            || !self.stopping_rule.publish_on_pass_or_fail
            || !self.stopping_rule.rerun_requires_new_plan_id
            || !self.stopping_rule.retain_prior_diagnostic
            || self.seeds.len() != 3
        {
            return Err("invalid T28 curve plan boundary".to_owned());
        }
        let expected_seeds = [1_103_u64, 2_207, 3_301];
        let mut point_plans = BTreeSet::new();
        for (seed_index, (seed, expected_seed)) in self
            .seeds
            .iter()
            .zip(expected_seeds.into_iter())
            .enumerate()
        {
            if seed.trace_seed != expected_seed
                || !valid_sha256(&seed.plan_sha256)
                || !point_plans.insert(seed.plan_sha256.clone())
                || seed.orders.len() != 5
            {
                return Err("invalid T28 seed or point-plan identity".to_owned());
            }
            for (block_index, order) in seed.orders.iter().enumerate() {
                let subjects = parse_order(order)?;
                let candidate_first = (seed_index + block_index) % 2 == 0;
                let expected = if candidate_first {
                    [
                        T28PointSubject::Candidate,
                        T28PointSubject::RawRangeControl,
                        T28PointSubject::RawRangeControl,
                        T28PointSubject::Candidate,
                    ]
                } else {
                    [
                        T28PointSubject::RawRangeControl,
                        T28PointSubject::Candidate,
                        T28PointSubject::Candidate,
                        T28PointSubject::RawRangeControl,
                    ]
                };
                if subjects != expected {
                    return Err("T28 subject order does not rotate by seed and block".to_owned());
                }
            }
        }
        if self.expected_positions()?.len() != 60 {
            return Err("T28 plan does not contain exactly 60 positions".to_owned());
        }
        Ok(())
    }
}

/// One block's original and local-residual admission results.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveBlockReceiptV1 {
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub order: Vec<T28PointSubject>,
    pub candidate_samples: u64,
    pub raw_control_samples: u64,
    pub candidate_end_to_end_p99_nanos: u64,
    pub raw_control_end_to_end_p99_nanos: u64,
    pub end_to_end_ratio_millionths: u64,
    pub original_gate_passed: bool,
    pub candidate_provider_p99_nanos: u64,
    pub raw_control_provider_p99_nanos: u64,
    pub provider_ratio_millionths: u64,
    pub candidate_local_residual_p99_nanos: u64,
    pub raw_control_local_residual_p99_nanos: u64,
    pub local_residual_ratio_millionths: u64,
    pub local_residual_difference_nanos: i64,
    pub local_ratio_gate_passed: bool,
    pub local_additive_gate_passed: bool,
    pub local_addendum_passed: bool,
}

/// Run-wide latency distribution for one subject and stage.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurvePercentilesV1 {
    pub samples: u64,
    pub p50_nanos: u64,
    pub p95_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
}

/// Immutable controller receipt before independent collector confirmation.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28CurveRunReceiptV1 {
    pub schema_version: u32,
    pub plan_id: String,
    pub plan_sha256: String,
    pub controller_run_id: String,
    pub candidate_commit: String,
    pub executable_sha256: String,
    pub cargo_lock_sha256: String,
    pub telemetry_endpoint_sha256: String,
    pub position_receipt_sha256s: Vec<String>,
    pub machine_id: String,
    pub linux_boot_id: String,
    pub blocks: Vec<T28CurveBlockReceiptV1>,
    pub original_passed_blocks: u64,
    pub local_addendum_passed_blocks: u64,
    pub original_gate_passed: bool,
    pub local_addendum_passed: bool,
    pub candidate_end_to_end: T28CurvePercentilesV1,
    pub raw_control_end_to_end: T28CurvePercentilesV1,
    pub candidate_provider: T28CurvePercentilesV1,
    pub raw_control_provider: T28CurvePercentilesV1,
    pub candidate_local_residual: T28CurvePercentilesV1,
    pub raw_control_local_residual: T28CurvePercentilesV1,
    pub telemetry_metrics_flush_succeeded: bool,
    pub telemetry_traces_flush_succeeded: bool,
    pub telemetry_logs_flush_succeeded: bool,
    pub telemetry_metrics_shutdown_succeeded: bool,
    pub telemetry_traces_shutdown_succeeded: bool,
    pub telemetry_logs_shutdown_succeeded: bool,
    pub telemetry_flush_passed: bool,
    pub collector_confirmation_required: bool,
    pub eligible_pending_collector_confirmation: bool,
    pub receipt_sha256: String,
}

impl T28CurveRunReceiptV1 {
    /// Aggregate exactly one frozen plan and its ordered child receipts.
    ///
    /// # Errors
    ///
    /// Returns an error for position, subject, plan, machine, process, physical
    /// counter, sample-decomposition, or telemetry drift.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn new(
        loaded: &LoadedT28CurvePlanV1,
        controller_run_id: String,
        candidate_commit: String,
        executable_sha256: String,
        cargo_lock_sha256: String,
        telemetry_endpoint_sha256: String,
        telemetry_flush: TelemetryFlushReport,
        positions: &[T28PointPositionReceiptV2],
    ) -> Result<Self, String> {
        let expected = loaded.plan.expected_positions()?;
        if positions.len() != expected.len()
            || controller_run_id.trim().is_empty()
            || candidate_commit.trim().is_empty()
            || !valid_sha256(&executable_sha256)
            || !valid_sha256(&cargo_lock_sha256)
            || !valid_sha256(&telemetry_endpoint_sha256)
        {
            return Err("invalid T28 curve controller boundary".to_owned());
        }
        let machine_id = positions
            .first()
            .map(|receipt| receipt.machine_id.clone())
            .ok_or_else(|| "T28 curve has no position receipts".to_owned())?;
        let linux_boot_id = positions[0].linux_boot_id.clone();
        let mut process_identities = BTreeSet::new();
        for (receipt, expected) in positions.iter().zip(&expected) {
            receipt.validate()?;
            if receipt.trace_seed != expected.trace_seed
                || receipt.plan_sha256 != expected.point_plan_sha256
                || receipt.block_ordinal != expected.block_ordinal
                || receipt.position_in_block != expected.position_in_block
                || receipt.subject != expected.subject
                || receipt.concurrent_clients != loaded.plan.concurrent_clients
                || receipt.measured_operations != loaded.plan.measured_operations_per_position
                || receipt.machine_id != machine_id
                || receipt.linux_boot_id != linux_boot_id
                || !process_identities
                    .insert((receipt.process_id, receipt.linux_process_start_ticks))
            {
                return Err("T28 position differs from its frozen execution plan".to_owned());
            }
        }

        let mut blocks = Vec::with_capacity(15);
        let mut all = SubjectSamples::default();
        for chunk in positions.chunks_exact(4) {
            let block = aggregate_block(&loaded.plan, chunk)?;
            blocks.push(block);
            for receipt in chunk {
                all.push(receipt);
            }
        }
        if blocks.len() != 15 {
            return Err("T28 curve did not aggregate exactly 15 blocks".to_owned());
        }
        let original_passed_blocks = u64::try_from(
            blocks
                .iter()
                .filter(|block| block.original_gate_passed)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let local_addendum_passed_blocks = u64::try_from(
            blocks
                .iter()
                .filter(|block| block.local_addendum_passed)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let original_gate_passed = original_passed_blocks == 15;
        let local_addendum_passed = local_addendum_passed_blocks == 15;
        let telemetry_flush_passed = telemetry_flush.all_succeeded();
        let mut receipt = Self {
            schema_version: RUN_SCHEMA_VERSION,
            plan_id: loaded.plan.plan_id.clone(),
            plan_sha256: loaded.raw_sha256.clone(),
            controller_run_id,
            candidate_commit,
            executable_sha256,
            cargo_lock_sha256,
            telemetry_endpoint_sha256,
            position_receipt_sha256s: positions
                .iter()
                .map(|receipt| receipt.receipt_sha256.clone())
                .collect(),
            machine_id,
            linux_boot_id,
            blocks,
            original_passed_blocks,
            local_addendum_passed_blocks,
            original_gate_passed,
            local_addendum_passed,
            candidate_end_to_end: percentiles(&all.candidate.end_to_end)?,
            raw_control_end_to_end: percentiles(&all.raw.end_to_end)?,
            candidate_provider: percentiles(&all.candidate.provider)?,
            raw_control_provider: percentiles(&all.raw.provider)?,
            candidate_local_residual: percentiles(&all.candidate.local)?,
            raw_control_local_residual: percentiles(&all.raw.local)?,
            telemetry_metrics_flush_succeeded: telemetry_flush.metrics_flush_succeeded,
            telemetry_traces_flush_succeeded: telemetry_flush.traces_flush_succeeded,
            telemetry_logs_flush_succeeded: telemetry_flush.logs_flush_succeeded,
            telemetry_metrics_shutdown_succeeded: telemetry_flush.metrics_shutdown_succeeded,
            telemetry_traces_shutdown_succeeded: telemetry_flush.traces_shutdown_succeeded,
            telemetry_logs_shutdown_succeeded: telemetry_flush.logs_shutdown_succeeded,
            telemetry_flush_passed,
            collector_confirmation_required: true,
            eligible_pending_collector_confirmation: original_gate_passed
                && local_addendum_passed
                && telemetry_flush_passed,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        Ok(receipt)
    }

    /// Return the canonical digest with the digest field excluded.
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

#[derive(Default)]
struct StageSamples {
    end_to_end: Vec<u64>,
    provider: Vec<u64>,
    local: Vec<u64>,
}

#[derive(Default)]
struct SubjectSamples {
    candidate: StageSamples,
    raw: StageSamples,
}

impl SubjectSamples {
    fn push(&mut self, receipt: &T28PointPositionReceiptV2) {
        let target = match receipt.subject {
            T28PointSubject::Candidate => &mut self.candidate,
            T28PointSubject::RawRangeControl => &mut self.raw,
        };
        target.end_to_end.extend(&receipt.latency_nanos);
        target.provider.extend(&receipt.provider_latency_nanos);
        target.local.extend(&receipt.local_residual_nanos);
    }
}

fn aggregate_block(
    plan: &T28CurvePlanV1,
    positions: &[T28PointPositionReceiptV2],
) -> Result<T28CurveBlockReceiptV1, String> {
    if positions.len() != 4 {
        return Err("T28 block does not contain four positions".to_owned());
    }
    let trace_seed = positions[0].trace_seed;
    let block_ordinal = positions[0].block_ordinal;
    let mut samples = SubjectSamples::default();
    for receipt in positions {
        if receipt.trace_seed != trace_seed || receipt.block_ordinal != block_ordinal {
            return Err("T28 block crosses seed or block identity".to_owned());
        }
        samples.push(receipt);
    }
    if samples.candidate.end_to_end.len() != 2_048 || samples.raw.end_to_end.len() != 2_048 {
        return Err("T28 block subject sample count mismatch".to_owned());
    }
    let candidate_end = percentile(&samples.candidate.end_to_end, 99, 100)?;
    let raw_end = percentile(&samples.raw.end_to_end, 99, 100)?;
    let candidate_provider = percentile(&samples.candidate.provider, 99, 100)?;
    let raw_provider = percentile(&samples.raw.provider, 99, 100)?;
    let candidate_local = percentile(&samples.candidate.local, 99, 100)?;
    let raw_local = percentile(&samples.raw.local, 99, 100)?;
    let end_limit = plan
        .original_end_to_end_gate
        .max_candidate_to_control_ratio_millionths;
    let local_limit = plan
        .local_residual_addendum
        .max_candidate_to_control_ratio_millionths;
    let original_gate_passed = ratio_passes(candidate_end, raw_end, end_limit);
    let local_ratio_gate_passed = ratio_passes(candidate_local, raw_local, local_limit);
    let local_additive_gate_passed = candidate_local
        <= raw_local.saturating_add(
            plan.local_residual_addendum
                .max_candidate_minus_control_nanos,
        );
    Ok(T28CurveBlockReceiptV1 {
        trace_seed,
        block_ordinal,
        order: positions.iter().map(|receipt| receipt.subject).collect(),
        candidate_samples: u64::try_from(samples.candidate.end_to_end.len()).unwrap_or(u64::MAX),
        raw_control_samples: u64::try_from(samples.raw.end_to_end.len()).unwrap_or(u64::MAX),
        candidate_end_to_end_p99_nanos: candidate_end,
        raw_control_end_to_end_p99_nanos: raw_end,
        end_to_end_ratio_millionths: ratio_millionths_from_samples(candidate_end, raw_end)?,
        original_gate_passed,
        candidate_provider_p99_nanos: candidate_provider,
        raw_control_provider_p99_nanos: raw_provider,
        provider_ratio_millionths: ratio_millionths_from_samples(candidate_provider, raw_provider)?,
        candidate_local_residual_p99_nanos: candidate_local,
        raw_control_local_residual_p99_nanos: raw_local,
        local_residual_ratio_millionths: ratio_millionths_from_samples(candidate_local, raw_local)?,
        local_residual_difference_nanos: signed_difference(candidate_local, raw_local)?,
        local_ratio_gate_passed,
        local_additive_gate_passed,
        local_addendum_passed: local_ratio_gate_passed && local_additive_gate_passed,
    })
}

fn percentiles(samples: &[u64]) -> Result<T28CurvePercentilesV1, String> {
    Ok(T28CurvePercentilesV1 {
        samples: u64::try_from(samples.len()).unwrap_or(u64::MAX),
        p50_nanos: percentile(samples, 50, 100)?,
        p95_nanos: percentile(samples, 95, 100)?,
        p99_nanos: percentile(samples, 99, 100)?,
        p999_nanos: percentile(samples, 999, 1_000)?,
    })
}

fn percentile(samples: &[u64], numerator: usize, denominator: usize) -> Result<u64, String> {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    nearest_rank(&sorted, numerator, denominator)
}

fn nearest_rank(sorted: &[u64], numerator: usize, denominator: usize) -> Result<u64, String> {
    if sorted.is_empty() || numerator == 0 || numerator > denominator {
        return Err("invalid T28 curve nearest-rank input".to_owned());
    }
    let rank = sorted
        .len()
        .checked_mul(numerator)
        .and_then(|value| value.checked_add(denominator - 1))
        .map(|value| value / denominator)
        .ok_or_else(|| "T28 curve nearest-rank overflow".to_owned())?;
    sorted
        .get(rank.saturating_sub(1))
        .copied()
        .ok_or_else(|| "T28 curve nearest-rank sample is absent".to_owned())
}

fn parse_order(value: &str) -> Result<[T28PointSubject; 4], String> {
    let subjects = value
        .split(',')
        .map(|subject| match subject {
            "candidate" => Ok(T28PointSubject::Candidate),
            "raw" => Ok(T28PointSubject::RawRangeControl),
            _ => Err("unknown T28 curve subject".to_owned()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    subjects
        .try_into()
        .map_err(|_| "T28 curve order does not contain four subjects".to_owned())
}

fn ratio_millionths_from_samples(candidate: u64, control: u64) -> Result<u64, String> {
    if control == 0 {
        return Err("T28 ratio control is zero".to_owned());
    }
    let scaled = u128::from(candidate)
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(u128::from(control / 2)))
        .ok_or_else(|| "T28 ratio overflow".to_owned())?
        / u128::from(control);
    u64::try_from(scaled).map_err(|error| error.to_string())
}

fn ratio_passes(candidate: u64, control: u64, limit_millionths: u64) -> bool {
    u128::from(candidate).saturating_mul(1_000_000)
        <= u128::from(control).saturating_mul(u128::from(limit_millionths))
}

fn signed_difference(candidate: u64, control: u64) -> Result<i64, String> {
    let difference = i128::from(candidate) - i128::from(control);
    i64::try_from(difference).map_err(|error| error.to_string())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::{T28CurvePlanV1, T28CurveRunReceiptV1, T28ExpectedPositionV1};
    use crate::t28_cold_point::T28PointSubject;
    use crate::t28_position::{T28PointLatencySampleV2, T28PointPositionReceiptV2};
    use crate::telemetry::TelemetryFlushReport;
    use okv_object::content_sha256;

    #[test]
    fn frozen_addendum_plan_has_exact_rotated_order() {
        let bytes = include_bytes!("../../../evals/plans/t28-point-curve-addendum-v1.toml");
        let loaded = T28CurvePlanV1::decode(bytes, &content_sha256(bytes)).expect("decode plan");
        let positions = loaded.plan.expected_positions().expect("positions");
        assert_eq!(positions.len(), 60);
        assert_ne!(positions[0].subject, positions[20].subject);
        assert_eq!(positions[0].subject, positions[40].subject);
    }

    #[test]
    fn seed_order_substitution_is_rejected() {
        let bytes = include_bytes!("../../../evals/plans/t28-point-curve-addendum-v1.toml");
        let text = std::str::from_utf8(bytes).expect("plan text");
        let poisoned = text.replacen(
            "raw,candidate,candidate,raw",
            "candidate,raw,raw,candidate",
            1,
        );
        assert!(
            T28CurvePlanV1::decode(poisoned.as_bytes(), &content_sha256(poisoned.as_bytes()))
                .is_err()
        );
    }

    #[test]
    fn aggregate_enforces_original_and_local_gates() {
        let bytes = include_bytes!("../../../evals/plans/t28-point-curve-addendum-v1.toml");
        let loaded = T28CurvePlanV1::decode(bytes, &content_sha256(bytes)).expect("decode plan");
        let expected = loaded.plan.expected_positions().expect("positions");
        let positions = expected
            .iter()
            .enumerate()
            .map(|(ordinal, expected)| position(expected, ordinal, 110, 100))
            .collect::<Vec<_>>();
        let passing = T28CurveRunReceiptV1::new(
            &loaded,
            "run".to_owned(),
            "commit".to_owned(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            TelemetryFlushReport::succeeded(),
            &positions,
        )
        .expect("aggregate passing curve");
        assert!(passing.original_gate_passed);
        assert!(passing.local_addendum_passed);
        assert!(passing.eligible_pending_collector_confirmation);
        let value = serde_json::to_value(&passing).expect("run receipt value");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t28-curve-run-receipt-v1.schema.json"
        ))
        .expect("run receipt schema");
        jsonschema::validator_for(&schema)
            .expect("compile run receipt schema")
            .validate(&value)
            .expect("validate run receipt schema");

        let failing = expected
            .iter()
            .enumerate()
            .map(|(ordinal, expected)| {
                let candidate_local = if ordinal == 0 { 500 } else { 110 };
                position(expected, ordinal, candidate_local, 100)
            })
            .collect::<Vec<_>>();
        let rejected = T28CurveRunReceiptV1::new(
            &loaded,
            "run".to_owned(),
            "commit".to_owned(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            TelemetryFlushReport::succeeded(),
            &failing,
        )
        .expect("aggregate rejected curve");
        assert!(!rejected.original_gate_passed);
        assert!(!rejected.local_addendum_passed);
        assert!(!rejected.eligible_pending_collector_confirmation);
    }

    fn position(
        expected: &T28ExpectedPositionV1,
        ordinal: usize,
        candidate_local: u64,
        raw_local: u64,
    ) -> T28PointPositionReceiptV2 {
        let local = match expected.subject {
            T28PointSubject::Candidate => candidate_local,
            T28PointSubject::RawRangeControl => raw_local,
        };
        let provider = 1_000;
        let end_to_end = provider + local;
        let operation_latency_samples = (0_u64..1_024)
            .map(|sample_ordinal| T28PointLatencySampleV2 {
                ordinal: sample_ordinal,
                end_to_end_nanos: end_to_end,
                provider_nanos: provider,
                local_residual_nanos: local,
            })
            .collect::<Vec<_>>();
        let mut receipt = T28PointPositionReceiptV2 {
            schema_version: 2,
            plan_sha256: expected.point_plan_sha256.clone(),
            subject: expected.subject,
            trace_seed: expected.trace_seed,
            block_ordinal: expected.block_ordinal,
            position_in_block: expected.position_in_block,
            concurrent_clients: 8,
            measured_operations: 1_024,
            warm_index_objects: 1,
            warm_provider_attempts: 3,
            warm_response_bytes: 1,
            measured_provider_attempts: 1_024,
            measured_response_bytes: 1_024,
            full_data_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            correctness_anomalies: 0,
            operation_latency_samples,
            latency_nanos: vec![end_to_end; 1_024],
            p50_latency_nanos: end_to_end,
            p95_latency_nanos: end_to_end,
            p99_latency_nanos: end_to_end,
            p999_latency_nanos: end_to_end,
            provider_latency_nanos: vec![provider; 1_024],
            provider_p50_latency_nanos: provider,
            provider_p95_latency_nanos: provider,
            provider_p99_latency_nanos: provider,
            provider_p999_latency_nanos: provider,
            local_residual_nanos: vec![local; 1_024],
            local_residual_p50_nanos: local,
            local_residual_p95_nanos: local,
            local_residual_p99_nanos: local,
            local_residual_p999_nanos: local,
            wall_elapsed_nanos: end_to_end,
            machine_id: "machine".to_owned(),
            linux_boot_id: "boot".to_owned(),
            process_id: u32::try_from(ordinal + 1).expect("process ID"),
            linux_process_start_ticks: u64::try_from(ordinal + 1).expect("start ticks"),
            measured_started_unix_nanos: 1,
            measured_finished_unix_nanos: 2,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256().expect("receipt digest");
        receipt.validate().expect("position receipt");
        receipt
    }
}
