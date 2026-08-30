//! RFC-0049 aligned point and projected-scan performance aggregation.

use crate::t28_iam::T28ReaderIamReceiptV1;
use crate::t28_layout_position::{
    T28AlignedMediaObservationV1, T28AlignedPointPositionReceiptV2, T28TypedPointSubjectV1,
    T28TypedScanPositionReceiptV1, T28TypedScanSubjectV1,
};
use crate::telemetry::TelemetryFlushReport;
use chrono::Utc;
use okv_object::content_sha256;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const RUN_SCHEMA_VERSION: u32 = 1;

/// Frozen execution contract added after the first manual diagnostic curve.
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedAdmissionPlanV1 {
    pub schema_version: u32,
    pub plan_id: String,
    pub status: String,
    pub one_execution: bool,
    pub physical_plan: String,
    pub physical_plan_sha256: String,
    pub position_execution_plan_sha256: String,
    pub reader_iam_receipt_sha256: String,
    pub candidate_parent_commit: String,
    pub named_code_change: String,
    pub prior_diagnostic_archive_sha256: String,
    pub receipt_contract: T28AlignedReceiptContractV1,
    pub execution: T28AlignedExecutionContractV1,
    pub scope: T28AlignedAdmissionScopeV1,
    pub stopping_rule: T28AlignedStoppingRuleV1,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedReceiptContractV1 {
    pub point_position_schema_version: u32,
    pub scan_position_schema_version: u32,
    pub performance_run_schema_version: u32,
    pub require_controller_spawn_identity: bool,
    pub require_logical_point_provider_correlation: bool,
    pub require_provider_pair_max: bool,
    pub require_local_residual: bool,
    pub require_pair_start_skew: bool,
    pub require_pair_completion: bool,
    pub require_fresh_process_per_position: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedExecutionContractV1 {
    pub machine_receipt_required: bool,
    pub build_receipt_required: bool,
    pub runtime_cargo_lock_required: bool,
    pub object_viewer_only: bool,
    pub host_global_lock_required: bool,
    pub otel_logs_required: bool,
    pub otel_metrics_required: bool,
    pub otel_traces_required: bool,
    pub collector_confirmation_required: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedAdmissionScopeV1 {
    pub point_curve: bool,
    pub projected_scan_curve: bool,
    pub stored_media_ratio: bool,
    pub resident_metadata_ratio: bool,
    pub complete_child_closure_recovery: bool,
    pub compaction_write_amplification: bool,
    pub branch_reference_reuse: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedStoppingRuleV1 {
    pub execute_once: bool,
    pub publish_on_pass_or_fail: bool,
    pub rerun_requires_new_plan: bool,
    pub retain_prior_diagnostic: bool,
}

/// Admission plan bytes plus their authenticated digest.
pub struct LoadedT28AlignedAdmissionPlanV1 {
    pub plan: T28AlignedAdmissionPlanV1,
    pub raw_sha256: String,
}

impl T28AlignedAdmissionPlanV1 {
    /// Decode and validate one exact post-diagnostic admission plan.
    ///
    /// # Errors
    ///
    /// Returns an error for digest drift or any weakened execution boundary.
    pub fn decode(
        bytes: &[u8],
        expected_sha256: &str,
    ) -> Result<LoadedT28AlignedAdmissionPlanV1, String> {
        let raw_sha256 = content_sha256(bytes);
        if raw_sha256 != expected_sha256 {
            return Err("RFC-0049 admission plan raw SHA-256 mismatch".to_owned());
        }
        let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let plan: Self = toml::from_str(text).map_err(|error| error.to_string())?;
        plan.validate()?;
        Ok(LoadedT28AlignedAdmissionPlanV1 { plan, raw_sha256 })
    }

    fn validate(&self) -> Result<(), String> {
        let receipt = &self.receipt_contract;
        let execution = &self.execution;
        let scope = &self.scope;
        let stopping = &self.stopping_rule;
        let execution_identity_is_frozen = matches!(
            (
                self.plan_id.as_str(),
                self.candidate_parent_commit.as_str(),
                self.named_code_change.as_str(),
                self.prior_diagnostic_archive_sha256.as_str(),
            ),
            (
                "t28-aligned-columnar-v2-admission-r1",
                "8ff14a211b74f21928965f109f4aa730ed346bd7",
                "rust-controller-and-logical-point-provider-correlation",
                "a23dcd642c797e51a368b79d7bbcfde48cb089fefc1c250ae318f09deefa8530",
            ) | (
                "t28-aligned-columnar-v2-admission-r2",
                "dfc29f5d5058af936040f01455e526d42016bc85",
                "fixture-exact-same-role-object-selection",
                "90d2b6c29047edbe3d6b32dff071c69a8d7e1ca4f91ddb3e86fb0c71da49215d",
            )
        );
        if self.schema_version != 1
            || !execution_identity_is_frozen
            || self.status != "frozen_before_execution"
            || !self.one_execution
            || self.physical_plan != "t28-aligned-columnar-v2.toml"
            || self.physical_plan_sha256
                != "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61"
            || self.position_execution_plan_sha256
                != "2e04d69775f67cb7561b59374d27bf2082909ca2df23a72f40e209728131c797"
            || self.reader_iam_receipt_sha256
                != "f383977a0f13ddf791ebc6ac97381ffc903268f45416689fe7eb23db22f2c1e9"
            || receipt.point_position_schema_version != 2
            || receipt.scan_position_schema_version != 1
            || receipt.performance_run_schema_version != 1
            || !receipt.require_controller_spawn_identity
            || !receipt.require_logical_point_provider_correlation
            || !receipt.require_provider_pair_max
            || !receipt.require_local_residual
            || !receipt.require_pair_start_skew
            || !receipt.require_pair_completion
            || !receipt.require_fresh_process_per_position
            || !execution.machine_receipt_required
            || !execution.build_receipt_required
            || !execution.runtime_cargo_lock_required
            || !execution.object_viewer_only
            || !execution.host_global_lock_required
            || !execution.otel_logs_required
            || !execution.otel_metrics_required
            || !execution.otel_traces_required
            || !execution.collector_confirmation_required
            || !scope.point_curve
            || !scope.projected_scan_curve
            || !scope.stored_media_ratio
            || !scope.resident_metadata_ratio
            || scope.complete_child_closure_recovery
            || scope.compaction_write_amplification
            || scope.branch_reference_reuse
            || !stopping.execute_once
            || !stopping.publish_on_pass_or_fail
            || !stopping.rerun_requires_new_plan
            || !stopping.retain_prior_diagnostic
        {
            return Err("invalid RFC-0049 admission plan boundary".to_owned());
        }
        Ok(())
    }
}

/// Build-time identity that binds the admitted source edge to the exact
/// executable and dependency lockfile used by the controller.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedBuildReceiptV1 {
    pub schema_version: u32,
    pub candidate_parent_commit: String,
    pub candidate_commit: String,
    pub executable_sha256: String,
    pub cargo_lock_sha256: String,
    pub build_profile: String,
    pub receipt_sha256: String,
}

impl T28AlignedBuildReceiptV1 {
    /// Seal one build identity from independently supplied source and binary
    /// facts.
    ///
    /// # Errors
    ///
    /// Returns an error when a commit, artifact digest, or build profile is
    /// not admissible.
    pub fn seal(
        candidate_parent_commit: String,
        candidate_commit: String,
        executable_sha256: String,
        cargo_lock_sha256: String,
        build_profile: String,
    ) -> Result<Self, String> {
        let mut receipt = Self {
            schema_version: 1,
            candidate_parent_commit,
            candidate_commit,
            executable_sha256,
            cargo_lock_sha256,
            build_profile,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Decode and authenticate one build receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON, invalid identities, or digest
    /// drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Recompute the build identity and receipt digest.
    ///
    /// # Errors
    ///
    /// Returns an error for source, artifact, profile, or digest drift.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1
            || !valid_git_commit(&self.candidate_parent_commit)
            || !valid_git_commit(&self.candidate_commit)
            || self.candidate_parent_commit == self.candidate_commit
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.cargo_lock_sha256)
            || self.build_profile != "release"
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 build receipt".to_owned());
        }
        Ok(())
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

/// Machine facts proven by the schema-valid benchmark-machine receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct T28AlignedMachineIdentityV1 {
    pub instance_id: String,
    pub collector_instance_id: String,
    pub service_account: String,
    pub lease_expires_epoch: u64,
}

/// Decode a benchmark-machine receipt and bind it to the executing binary,
/// source revision, fixture placement, and independently captured IAM receipt.
///
/// # Errors
///
/// Returns an error for raw digest drift, schema or semantic drift, an expired
/// infrastructure lease, or a runtime identity mismatch.
#[allow(clippy::too_many_arguments)]
pub fn decode_t28_aligned_machine_identity(
    bytes: &[u8],
    expected_sha256: &str,
    candidate_commit: &str,
    executable_sha256: &str,
    project: &str,
    bucket: &str,
    region: &str,
    iam: &T28ReaderIamReceiptV1,
) -> Result<T28AlignedMachineIdentityV1, String> {
    if !valid_sha256(expected_sha256) || content_sha256(bytes) != expected_sha256 {
        return Err("RFC-0049 machine receipt raw identity mismatch".to_owned());
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../evals/schema/benchmark-machine-receipt-v1.schema.json"
    ))
    .map_err(|error| error.to_string())?;
    jsonschema::validator_for(&schema)
        .map_err(|error| error.to_string())?
        .validate(&value)
        .map_err(|error| error.to_string())?;
    let string_at = |pointer: &str| {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("RFC-0049 machine receipt omits {pointer}"))
    };
    let source_revision = string_at("/source_revision")?;
    let observed_project = string_at("/project")?;
    let observed_region = string_at("/region")?;
    let observed_bucket = string_at("/bucket/name")?;
    let instance_id = string_at("/runner/instance_id")?;
    let collector_instance_id = string_at("/collector/instance_id")?;
    let service_account = string_at("/runner/service_account")?;
    let declared_binary_sha256 = string_at("/runner/binary_sha256")?;
    let lease_expires_epoch = string_at("/runner/lease_expires_epoch")?
        .parse::<u64>()
        .map_err(|error| error.to_string())?;
    let now = u64::try_from(Utc::now().timestamp()).map_err(|error| error.to_string())?;
    if source_revision != candidate_commit
        || observed_project != project
        || observed_bucket != bucket
        || observed_region != region
        || declared_binary_sha256 != executable_sha256
        || lease_expires_epoch <= now
        || instance_id != iam.runner.instance_id
        || service_account != iam.principal.email
        || iam.project != project
        || iam.bucket != bucket
        || iam.region != region
    {
        return Err(
            "RFC-0049 machine, binary, source, IAM, or placement identity mismatch".to_owned(),
        );
    }
    Ok(T28AlignedMachineIdentityV1 {
        instance_id,
        collector_instance_id,
        service_account,
        lease_expires_epoch,
    })
}

/// Frozen RFC-0049 fields required to schedule and aggregate its performance curve.
#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedCurvePlanV1 {
    pub schema_version: u32,
    pub plan_id: String,
    pub one_execution: bool,
    pub object_store: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub logical_oracle: T28AlignedLogicalOracleV1,
    pub expected_candidate_media: T28AlignedExpectedMediaV1,
    pub point_lane: T28AlignedPointLaneV1,
    pub scan_lane: T28AlignedScanLaneV1,
    pub media: T28AlignedMediaGatesV1,
    pub telemetry: T28AlignedTelemetryGatesV1,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedLogicalOracleV1 {
    pub workload_plan_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedExpectedMediaV1 {
    pub total_media_bytes: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedPointLaneV1 {
    pub blocks_per_seed: u64,
    pub positions_per_block: u64,
    pub reads_per_position: u64,
    pub concurrent_tasks: u64,
    pub candidate_p99_ratio_millionths_max: u64,
    pub candidate_bytes_ratio_millionths_max: u64,
    pub candidate_sdk_attempts_per_indexed_point: u64,
    pub control_sdk_attempts_per_point: u64,
    pub require_pair_overlap_for_every_indexed_point: bool,
    pub seed_orders: Vec<T28AlignedSeedOrdersV1>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedScanLaneV1 {
    pub blocks_per_seed: u64,
    pub positions_per_block: u64,
    pub candidate_median_rows_per_second_ratio_millionths_min: u64,
    pub paired_ratio_count: u64,
    pub candidate_gets_per_complete_projection_max: u64,
    pub candidate_response_bytes_ratio_millionths_max: u64,
    pub candidate_opaque_payload_requests_max: u64,
    pub candidate_opaque_payload_bytes_max: u64,
    pub peak_fetch_bytes_max: u64,
    pub peak_arrow_batch_rows_max: u64,
    pub seed_orders: Vec<T28AlignedSeedOrdersV1>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedSeedOrdersV1 {
    pub seed: u64,
    pub orders: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedMediaGatesV1 {
    pub candidate_storage_amplification_ratio_millionths_max: u64,
    pub candidate_resident_metadata_ratio_millionths_max: u64,
    pub require_complete_child_closure_recovery: bool,
    pub require_canonical_history_digest_match: bool,
    pub require_branch_reference_reuse: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct T28AlignedTelemetryGatesV1 {
    pub require_logs: bool,
    pub require_metrics: bool,
    pub require_traces: bool,
    pub require_exporter_flush_and_shutdown: bool,
    pub require_independent_collector_confirmation: bool,
}

/// Exact plan bytes plus their authenticated digest.
pub struct LoadedT28AlignedCurvePlanV1 {
    pub plan: T28AlignedCurvePlanV1,
    pub raw_sha256: String,
}

/// One expected point child process in immutable execution order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct T28AlignedExpectedPointPositionV1 {
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub position_in_block: u64,
    pub subject: T28TypedPointSubjectV1,
}

/// One expected scan child process in immutable execution order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct T28AlignedExpectedScanPositionV1 {
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub position_in_block: u64,
    pub subject: T28TypedScanSubjectV1,
}

/// Lane identity for one fresh child process.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28AlignedChildLaneV1 {
    Point,
    Scan,
}

/// Controller-owned envelope binding a child receipt to its exact scheduled
/// run, plan, block, position, subject, process, and executable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedChildBindingV1 {
    pub schema_version: u32,
    pub controller_run_id: String,
    pub admission_plan_sha256: String,
    pub physical_plan_sha256: String,
    pub position_execution_plan_sha256: String,
    pub lane: T28AlignedChildLaneV1,
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub position_in_block: u64,
    pub subject: String,
    pub process_id: u32,
    pub executable_sha256: String,
    pub child_receipt_sha256: String,
    pub binding_sha256: String,
}

impl T28AlignedChildBindingV1 {
    /// Seal an exact controller-to-child schedule binding.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty identity or malformed digest.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        controller_run_id: String,
        admission_plan_sha256: String,
        physical_plan_sha256: String,
        position_execution_plan_sha256: String,
        lane: T28AlignedChildLaneV1,
        trace_seed: u64,
        block_ordinal: u64,
        position_in_block: u64,
        subject: String,
        process_id: u32,
        executable_sha256: String,
        child_receipt_sha256: String,
    ) -> Result<Self, String> {
        let mut binding = Self {
            schema_version: 1,
            controller_run_id,
            admission_plan_sha256,
            physical_plan_sha256,
            position_execution_plan_sha256,
            lane,
            trace_seed,
            block_ordinal,
            position_in_block,
            subject,
            process_id,
            executable_sha256,
            child_receipt_sha256,
            binding_sha256: String::new(),
        };
        binding.binding_sha256 = binding.calculated_sha256()?;
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1
            || self.controller_run_id.trim().is_empty()
            || !valid_sha256(&self.admission_plan_sha256)
            || !valid_sha256(&self.physical_plan_sha256)
            || !valid_sha256(&self.position_execution_plan_sha256)
            || self.trace_seed == 0
            || self.subject.trim().is_empty()
            || self.process_id == 0
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.child_receipt_sha256)
            || self.binding_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 controller child binding".to_owned());
        }
        Ok(())
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.binding_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

impl T28AlignedCurvePlanV1 {
    /// Decode and validate one exact RFC-0049 physical plan.
    ///
    /// # Errors
    ///
    /// Returns an error for digest drift, malformed TOML, changed thresholds,
    /// or a noncanonical seed and subject schedule.
    pub fn decode(
        bytes: &[u8],
        expected_sha256: &str,
    ) -> Result<LoadedT28AlignedCurvePlanV1, String> {
        let raw_sha256 = content_sha256(bytes);
        if raw_sha256 != expected_sha256 {
            return Err("RFC-0049 curve plan raw SHA-256 mismatch".to_owned());
        }
        let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let plan: Self = toml::from_str(text).map_err(|error| error.to_string())?;
        plan.validate()?;
        Ok(LoadedT28AlignedCurvePlanV1 { plan, raw_sha256 })
    }

    /// Return all 60 point positions in frozen order.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown subject marker.
    pub fn expected_point_positions(
        &self,
    ) -> Result<Vec<T28AlignedExpectedPointPositionV1>, String> {
        let mut positions = Vec::with_capacity(60);
        for seed in &self.point_lane.seed_orders {
            for (block, order) in seed.orders.iter().enumerate() {
                for (position, subject) in parse_point_order(order)?.into_iter().enumerate() {
                    positions.push(T28AlignedExpectedPointPositionV1 {
                        trace_seed: seed.seed,
                        block_ordinal: u64::try_from(block).unwrap_or(u64::MAX),
                        position_in_block: u64::try_from(position).unwrap_or(u64::MAX),
                        subject,
                    });
                }
            }
        }
        Ok(positions)
    }

    /// Return all 30 projected-scan positions in frozen order.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown subject marker.
    pub fn expected_scan_positions(&self) -> Result<Vec<T28AlignedExpectedScanPositionV1>, String> {
        let mut positions = Vec::with_capacity(30);
        for seed in &self.scan_lane.seed_orders {
            for (block, order) in seed.orders.iter().enumerate() {
                for (position, subject) in parse_scan_order(order)?.into_iter().enumerate() {
                    positions.push(T28AlignedExpectedScanPositionV1 {
                        trace_seed: seed.seed,
                        block_ordinal: u64::try_from(block).unwrap_or(u64::MAX),
                        position_in_block: u64::try_from(position).unwrap_or(u64::MAX),
                        subject,
                    });
                }
            }
        }
        Ok(positions)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1
            || self.plan_id != "t28-aligned-columnar-v2"
            || !self.one_execution
            || self.object_store != "gcs"
            || self.project != "doss-objectkv-dev"
            || self.bucket != "doss-objectkv-dev-okv-evals"
            || self.region != "us-central1"
            || !valid_sha256(&self.logical_oracle.workload_plan_sha256)
            || self.expected_candidate_media.total_media_bytes != 13_695_766
            || self.point_lane.blocks_per_seed != 5
            || self.point_lane.positions_per_block != 4
            || self.point_lane.reads_per_position != 1_024
            || self.point_lane.concurrent_tasks != 8
            || self.point_lane.candidate_p99_ratio_millionths_max != 2_000_000
            || self.point_lane.candidate_bytes_ratio_millionths_max != 500_000
            || self.point_lane.candidate_sdk_attempts_per_indexed_point != 2
            || self.point_lane.control_sdk_attempts_per_point != 1
            || !self.point_lane.require_pair_overlap_for_every_indexed_point
            || self.scan_lane.blocks_per_seed != 5
            || self.scan_lane.positions_per_block != 2
            || self
                .scan_lane
                .candidate_median_rows_per_second_ratio_millionths_min
                != 2_000_000
            || self.scan_lane.paired_ratio_count != 15
            || self.scan_lane.candidate_gets_per_complete_projection_max != 64
            || self.scan_lane.candidate_response_bytes_ratio_millionths_max != 500_000
            || self.scan_lane.candidate_opaque_payload_requests_max != 0
            || self.scan_lane.candidate_opaque_payload_bytes_max != 0
            || self.scan_lane.peak_fetch_bytes_max != 262_144
            || self.scan_lane.peak_arrow_batch_rows_max != 128
            || self
                .media
                .candidate_storage_amplification_ratio_millionths_max
                != 1_100_000
            || self.media.candidate_resident_metadata_ratio_millionths_max != 2_000_000
            || !self.media.require_complete_child_closure_recovery
            || !self.media.require_canonical_history_digest_match
            || !self.media.require_branch_reference_reuse
            || !self.telemetry.require_logs
            || !self.telemetry.require_metrics
            || !self.telemetry.require_traces
            || !self.telemetry.require_exporter_flush_and_shutdown
            || !self.telemetry.require_independent_collector_confirmation
        {
            return Err("invalid RFC-0049 performance plan boundary".to_owned());
        }
        validate_orders(&self.point_lane.seed_orders, 4)?;
        validate_orders(&self.scan_lane.seed_orders, 2)?;
        if self.expected_point_positions()?.len() != 60
            || self.expected_scan_positions()?.len() != 30
        {
            return Err("RFC-0049 plan has the wrong position count".to_owned());
        }
        Ok(())
    }
}

/// Run-wide nearest-rank distribution for one subject and stage.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedCurvePercentilesV1 {
    pub samples: u64,
    pub p50_nanos: u64,
    pub p95_nanos: u64,
    pub p99_nanos: u64,
    pub p999_nanos: u64,
}

/// One four-position point block.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedPointBlockReceiptV1 {
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub candidate_p99_nanos: u64,
    pub control_p99_nanos: u64,
    pub p99_ratio_millionths: u64,
    pub candidate_response_bytes: u64,
    pub control_response_bytes: u64,
    pub response_bytes_ratio_millionths: u64,
    pub candidate_maximum_point_bytes: u64,
    pub control_maximum_point_bytes: u64,
    pub maximum_point_bytes_ratio_millionths: u64,
    pub candidate_provider_attempts: u64,
    pub control_provider_attempts: u64,
    pub candidate_overlapping_pairs: u64,
    pub candidate_points: u64,
    pub passed: bool,
}

/// One two-position projected-scan block.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedScanBlockReceiptV1 {
    pub trace_seed: u64,
    pub block_ordinal: u64,
    pub candidate_query_elapsed_nanos: u64,
    pub control_query_elapsed_nanos: u64,
    pub throughput_ratio_millionths: u64,
    pub candidate_provider_attempts: u64,
    pub control_provider_attempts: u64,
    pub candidate_response_bytes: u64,
    pub control_response_bytes: u64,
    pub response_bytes_ratio_millionths: u64,
    pub passed: bool,
}

/// Rust-owned RFC-0049 performance receipt before collector confirmation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedPerformanceRunReceiptV1 {
    pub schema_version: u32,
    pub physical_plan_id: String,
    pub physical_plan_sha256: String,
    pub admission_plan_id: String,
    pub admission_plan_sha256: String,
    pub controller_run_id: String,
    pub candidate_commit: String,
    pub executable_sha256: String,
    pub cargo_lock_sha256: String,
    pub build_receipt_sha256: String,
    pub machine_receipt_sha256: String,
    pub machine_instance_id: String,
    pub collector_instance_id: String,
    pub reader_iam_receipt_sha256: String,
    pub runtime_principal: String,
    pub telemetry_endpoint_sha256: String,
    pub fixture_id: String,
    pub root_sha256: String,
    pub point_position_receipt_sha256s: Vec<String>,
    pub scan_position_receipt_sha256s: Vec<String>,
    pub point_child_binding_sha256s: Vec<String>,
    pub scan_child_binding_sha256s: Vec<String>,
    pub point_blocks: Vec<T28AlignedPointBlockReceiptV1>,
    pub point_passed_blocks: u64,
    pub point_gate_passed: bool,
    pub candidate_end_to_end: T28AlignedCurvePercentilesV1,
    pub control_end_to_end: T28AlignedCurvePercentilesV1,
    pub candidate_provider_pair_max: T28AlignedCurvePercentilesV1,
    pub control_provider_pair_max: T28AlignedCurvePercentilesV1,
    pub candidate_local_residual: T28AlignedCurvePercentilesV1,
    pub control_local_residual: T28AlignedCurvePercentilesV1,
    pub pooled_point_p99_ratio_millionths: u64,
    pub maximum_block_point_p99_ratio_millionths: u64,
    pub point_response_bytes_ratio_millionths: u64,
    pub maximum_point_bytes_ratio_millionths: u64,
    pub scan_blocks: Vec<T28AlignedScanBlockReceiptV1>,
    pub scan_passed_blocks: u64,
    pub scan_median_throughput_ratio_millionths: u64,
    pub scan_minimum_throughput_ratio_millionths: u64,
    pub scan_maximum_throughput_ratio_millionths: u64,
    pub scan_gate_passed: bool,
    pub media_observation_sha256: String,
    pub control_total_media_bytes: u64,
    pub candidate_total_media_bytes: u64,
    pub control_closure_sha256: String,
    pub candidate_closure_sha256: String,
    pub stored_media_ratio_millionths: u64,
    pub resident_metadata_ratio_millionths: u64,
    pub measured_media_gates_passed: bool,
    pub telemetry_metrics_flush_succeeded: bool,
    pub telemetry_traces_flush_succeeded: bool,
    pub telemetry_logs_flush_succeeded: bool,
    pub telemetry_metrics_shutdown_succeeded: bool,
    pub telemetry_traces_shutdown_succeeded: bool,
    pub telemetry_logs_shutdown_succeeded: bool,
    pub telemetry_flush_passed: bool,
    pub collector_confirmation_required: bool,
    pub remaining_shared_media_gates: Vec<String>,
    pub performance_eligible_pending_collector_confirmation: bool,
    pub receipt_sha256: String,
}

impl T28AlignedPerformanceRunReceiptV1 {
    /// Decode and validate a completed performance receipt without trusting
    /// any stored aggregate or digest field.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON, identity drift, missing positions,
    /// inconsistent aggregates, or receipt-digest drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Aggregate all 90 fresh-process RFC-0049 positions.
    ///
    /// # Errors
    ///
    /// Returns an error for identity, order, process, correctness, resource,
    /// percentile, media, or telemetry drift.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn new(
        loaded: &LoadedT28AlignedCurvePlanV1,
        admission: &LoadedT28AlignedAdmissionPlanV1,
        controller_run_id: String,
        candidate_commit: String,
        executable_sha256: String,
        cargo_lock_sha256: String,
        build_receipt: &T28AlignedBuildReceiptV1,
        machine_receipt_sha256: String,
        machine_identity: &T28AlignedMachineIdentityV1,
        reader_iam_receipt_sha256: String,
        telemetry_endpoint_sha256: String,
        telemetry_flush: TelemetryFlushReport,
        media: &T28AlignedMediaObservationV1,
        points: &[T28AlignedPointPositionReceiptV2],
        scans: &[T28TypedScanPositionReceiptV1],
        point_bindings: &[T28AlignedChildBindingV1],
        scan_bindings: &[T28AlignedChildBindingV1],
    ) -> Result<Self, String> {
        build_receipt.validate()?;
        if admission.plan.physical_plan_sha256 != loaded.raw_sha256
            || controller_run_id.trim().is_empty()
            || candidate_commit.trim().is_empty()
            || !valid_sha256(&executable_sha256)
            || !valid_sha256(&cargo_lock_sha256)
            || build_receipt.candidate_parent_commit != admission.plan.candidate_parent_commit
            || build_receipt.candidate_commit != candidate_commit
            || build_receipt.executable_sha256 != executable_sha256
            || build_receipt.cargo_lock_sha256 != cargo_lock_sha256
            || build_receipt.build_profile != "release"
            || !valid_sha256(&build_receipt.receipt_sha256)
            || !valid_sha256(&machine_receipt_sha256)
            || reader_iam_receipt_sha256 != admission.plan.reader_iam_receipt_sha256
            || machine_identity.instance_id.trim().is_empty()
            || machine_identity.collector_instance_id.trim().is_empty()
            || machine_identity.service_account.trim().is_empty()
            || !valid_sha256(&telemetry_endpoint_sha256)
        {
            return Err("invalid RFC-0049 controller identity".to_owned());
        }
        let expected_points = loaded.plan.expected_point_positions()?;
        let expected_scans = loaded.plan.expected_scan_positions()?;
        media.validate()?;
        if points.len() != expected_points.len()
            || scans.len() != expected_scans.len()
            || point_bindings.len() != expected_points.len()
            || scan_bindings.len() != expected_scans.len()
        {
            return Err("RFC-0049 controller did not receive every position".to_owned());
        }
        let fixture_id = points
            .first()
            .map(|receipt| receipt.base.fixture_id.clone())
            .ok_or_else(|| "RFC-0049 point receipt set is empty".to_owned())?;
        let root_sha256 = points[0].base.root_sha256.clone();
        if media.fixture_id != fixture_id
            || media.root_sha256 != root_sha256
            || media.candidate_total_media_bytes
                != loaded.plan.expected_candidate_media.total_media_bytes
        {
            return Err("RFC-0049 media observation differs from the measured root".to_owned());
        }
        let mut process_identities = BTreeSet::new();
        for ((receipt, binding), expected) in
            points.iter().zip(point_bindings).zip(&expected_points)
        {
            receipt.validate_against_media(media)?;
            binding.validate()?;
            let base = &receipt.base;
            if base.trace_seed != expected.trace_seed
                || base.subject != expected.subject
                || base.execution_plan_sha256 != admission.plan.position_execution_plan_sha256
                || base.measured_operations != loaded.plan.point_lane.reads_per_position
                || base.concurrent_tasks != loaded.plan.point_lane.concurrent_tasks
                || base.fixture_id != fixture_id
                || base.root_sha256 != root_sha256
                || binding.controller_run_id != controller_run_id
                || binding.admission_plan_sha256 != admission.raw_sha256
                || binding.physical_plan_sha256 != loaded.raw_sha256
                || binding.position_execution_plan_sha256
                    != admission.plan.position_execution_plan_sha256
                || binding.lane != T28AlignedChildLaneV1::Point
                || binding.trace_seed != expected.trace_seed
                || binding.block_ordinal != expected.block_ordinal
                || binding.position_in_block != expected.position_in_block
                || binding.subject != point_subject_id(expected.subject)
                || binding.process_id != base.process_id
                || binding.executable_sha256 != executable_sha256
                || binding.child_receipt_sha256 != receipt.receipt_sha256
                || !process_identities.insert((
                    base.process_id,
                    base.measured_started_unix_nanos,
                    base.measured_finished_unix_nanos,
                ))
            {
                return Err("RFC-0049 point position differs from its plan".to_owned());
            }
        }
        for ((receipt, binding), expected) in scans.iter().zip(scan_bindings).zip(&expected_scans) {
            receipt.validate()?;
            binding.validate()?;
            if receipt.trace_seed != expected.trace_seed
                || receipt.subject != expected.subject
                || receipt.execution_plan_sha256 != admission.plan.position_execution_plan_sha256
                || receipt.fixture_id != fixture_id
                || receipt.root_sha256 != root_sha256
                || binding.controller_run_id != controller_run_id
                || binding.admission_plan_sha256 != admission.raw_sha256
                || binding.physical_plan_sha256 != loaded.raw_sha256
                || binding.position_execution_plan_sha256
                    != admission.plan.position_execution_plan_sha256
                || binding.lane != T28AlignedChildLaneV1::Scan
                || binding.trace_seed != expected.trace_seed
                || binding.block_ordinal != expected.block_ordinal
                || binding.position_in_block != expected.position_in_block
                || binding.subject != scan_subject_id(expected.subject)
                || binding.process_id != receipt.process_id
                || binding.executable_sha256 != executable_sha256
                || binding.child_receipt_sha256 != receipt.receipt_sha256
                || !process_identities.insert((
                    receipt.process_id,
                    receipt.measured_started_unix_nanos,
                    receipt.measured_finished_unix_nanos,
                ))
            {
                return Err("RFC-0049 scan position differs from its plan".to_owned());
            }
        }

        let mut candidate_end_to_end = Vec::new();
        let mut control_end_to_end = Vec::new();
        let mut candidate_provider = Vec::new();
        let mut control_provider = Vec::new();
        let mut candidate_local = Vec::new();
        let mut control_local = Vec::new();
        let mut point_blocks = Vec::with_capacity(15);
        for (block_index, block) in points.chunks_exact(4).enumerate() {
            let expected = &expected_points[block_index * 4];
            point_blocks.push(aggregate_point_block(
                &loaded.plan,
                expected.trace_seed,
                expected.block_ordinal,
                block,
            )?);
            for receipt in block {
                let (end_to_end, provider, local) = match receipt.base.subject {
                    T28TypedPointSubjectV1::C5v2AlignedColumnar => (
                        &mut candidate_end_to_end,
                        &mut candidate_provider,
                        &mut candidate_local,
                    ),
                    T28TypedPointSubjectV1::C0IndexedRow => (
                        &mut control_end_to_end,
                        &mut control_provider,
                        &mut control_local,
                    ),
                    T28TypedPointSubjectV1::C5ColumnarMain => unreachable!(),
                };
                end_to_end.extend(
                    receipt
                        .operation_latency_samples
                        .iter()
                        .map(|sample| sample.end_to_end_nanos),
                );
                provider.extend(
                    receipt
                        .operation_latency_samples
                        .iter()
                        .map(|sample| sample.provider_pair_max_nanos),
                );
                local.extend(
                    receipt
                        .operation_latency_samples
                        .iter()
                        .map(|sample| sample.local_residual_nanos),
                );
            }
        }
        if point_blocks.len() != 15 {
            return Err("RFC-0049 did not aggregate exactly 15 point blocks".to_owned());
        }
        let candidate_end_to_end = percentiles(&candidate_end_to_end)?;
        let control_end_to_end = percentiles(&control_end_to_end)?;
        let candidate_provider_pair_max = percentiles(&candidate_provider)?;
        let control_provider_pair_max = percentiles(&control_provider)?;
        let candidate_local_residual = percentiles(&candidate_local)?;
        let control_local_residual = percentiles(&control_local)?;
        let pooled_point_p99_ratio_millionths =
            ratio_millionths(candidate_end_to_end.p99_nanos, control_end_to_end.p99_nanos)?;
        let maximum_block_point_p99_ratio_millionths = point_blocks
            .iter()
            .map(|block| block.p99_ratio_millionths)
            .max()
            .ok_or_else(|| "RFC-0049 point block set is empty".to_owned())?;
        let point_passed_blocks =
            u64::try_from(point_blocks.iter().filter(|block| block.passed).count())
                .unwrap_or(u64::MAX);
        let total_candidate_point_bytes = point_blocks
            .iter()
            .map(|block| block.candidate_response_bytes)
            .sum::<u64>();
        let total_control_point_bytes = point_blocks
            .iter()
            .map(|block| block.control_response_bytes)
            .sum::<u64>();
        let point_response_bytes_ratio_millionths =
            ratio_millionths(total_candidate_point_bytes, total_control_point_bytes)?;
        let maximum_point_bytes_ratio_millionths = point_blocks
            .iter()
            .map(|block| block.maximum_point_bytes_ratio_millionths)
            .max()
            .ok_or_else(|| "RFC-0049 point block set is empty".to_owned())?;
        let point_gate_passed = point_passed_blocks == 15;

        let mut scan_blocks = Vec::with_capacity(15);
        for (block_index, block) in scans.chunks_exact(2).enumerate() {
            let expected = &expected_scans[block_index * 2];
            scan_blocks.push(aggregate_scan_block(
                &loaded.plan,
                expected.trace_seed,
                expected.block_ordinal,
                block,
            )?);
        }
        if scan_blocks.len() != 15 {
            return Err("RFC-0049 did not aggregate exactly 15 scan blocks".to_owned());
        }
        let mut scan_ratios = scan_blocks
            .iter()
            .map(|block| block.throughput_ratio_millionths)
            .collect::<Vec<_>>();
        scan_ratios.sort_unstable();
        let scan_median_throughput_ratio_millionths = nearest_rank(&scan_ratios, 50, 100)?;
        let scan_minimum_throughput_ratio_millionths = *scan_ratios
            .first()
            .ok_or_else(|| "RFC-0049 scan ratio set is empty".to_owned())?;
        let scan_maximum_throughput_ratio_millionths = *scan_ratios
            .last()
            .ok_or_else(|| "RFC-0049 scan ratio set is empty".to_owned())?;
        let scan_passed_blocks =
            u64::try_from(scan_blocks.iter().filter(|block| block.passed).count())
                .unwrap_or(u64::MAX);
        let scan_gate_passed = scan_passed_blocks == 15
            && scan_median_throughput_ratio_millionths
                >= loaded
                    .plan
                    .scan_lane
                    .candidate_median_rows_per_second_ratio_millionths_min;

        let candidate_metadata =
            consistent_metadata(points, T28TypedPointSubjectV1::C5v2AlignedColumnar)?;
        let control_metadata = consistent_metadata(points, T28TypedPointSubjectV1::C0IndexedRow)?;
        let resident_metadata_ratio_millionths =
            ratio_millionths(candidate_metadata, control_metadata)?;
        let stored_media_ratio_millionths = ratio_millionths(
            media.candidate_total_media_bytes,
            media.control_total_media_bytes,
        )?;
        let measured_media_gates_passed = stored_media_ratio_millionths
            <= loaded
                .plan
                .media
                .candidate_storage_amplification_ratio_millionths_max
            && resident_metadata_ratio_millionths
                <= loaded
                    .plan
                    .media
                    .candidate_resident_metadata_ratio_millionths_max;
        let telemetry_flush_passed = telemetry_flush.all_succeeded();
        let performance_eligible_pending_collector_confirmation = point_gate_passed
            && scan_gate_passed
            && measured_media_gates_passed
            && telemetry_flush_passed;
        let mut receipt = Self {
            schema_version: RUN_SCHEMA_VERSION,
            physical_plan_id: loaded.plan.plan_id.clone(),
            physical_plan_sha256: loaded.raw_sha256.clone(),
            admission_plan_id: admission.plan.plan_id.clone(),
            admission_plan_sha256: admission.raw_sha256.clone(),
            controller_run_id,
            candidate_commit,
            executable_sha256,
            cargo_lock_sha256,
            build_receipt_sha256: build_receipt.receipt_sha256.clone(),
            machine_receipt_sha256,
            machine_instance_id: machine_identity.instance_id.clone(),
            collector_instance_id: machine_identity.collector_instance_id.clone(),
            reader_iam_receipt_sha256,
            runtime_principal: machine_identity.service_account.clone(),
            telemetry_endpoint_sha256,
            fixture_id,
            root_sha256,
            point_position_receipt_sha256s: points
                .iter()
                .map(|receipt| receipt.receipt_sha256.clone())
                .collect(),
            scan_position_receipt_sha256s: scans
                .iter()
                .map(|receipt| receipt.receipt_sha256.clone())
                .collect(),
            point_child_binding_sha256s: point_bindings
                .iter()
                .map(|binding| binding.binding_sha256.clone())
                .collect(),
            scan_child_binding_sha256s: scan_bindings
                .iter()
                .map(|binding| binding.binding_sha256.clone())
                .collect(),
            point_blocks,
            point_passed_blocks,
            point_gate_passed,
            candidate_end_to_end,
            control_end_to_end,
            candidate_provider_pair_max,
            control_provider_pair_max,
            candidate_local_residual,
            control_local_residual,
            pooled_point_p99_ratio_millionths,
            maximum_block_point_p99_ratio_millionths,
            point_response_bytes_ratio_millionths,
            maximum_point_bytes_ratio_millionths,
            scan_blocks,
            scan_passed_blocks,
            scan_median_throughput_ratio_millionths,
            scan_minimum_throughput_ratio_millionths,
            scan_maximum_throughput_ratio_millionths,
            scan_gate_passed,
            media_observation_sha256: media.observation_sha256.clone(),
            control_total_media_bytes: media.control_total_media_bytes,
            candidate_total_media_bytes: media.candidate_total_media_bytes,
            control_closure_sha256: media.control_closure_sha256.clone(),
            candidate_closure_sha256: media.candidate_closure_sha256.clone(),
            stored_media_ratio_millionths,
            resident_metadata_ratio_millionths,
            measured_media_gates_passed,
            telemetry_metrics_flush_succeeded: telemetry_flush.metrics_flush_succeeded,
            telemetry_traces_flush_succeeded: telemetry_flush.traces_flush_succeeded,
            telemetry_logs_flush_succeeded: telemetry_flush.logs_flush_succeeded,
            telemetry_metrics_shutdown_succeeded: telemetry_flush.metrics_shutdown_succeeded,
            telemetry_traces_shutdown_succeeded: telemetry_flush.traces_shutdown_succeeded,
            telemetry_logs_shutdown_succeeded: telemetry_flush.logs_shutdown_succeeded,
            telemetry_flush_passed,
            collector_confirmation_required: true,
            remaining_shared_media_gates: vec![
                "complete-child-closure-recovery".to_owned(),
                "compaction-write-amplification".to_owned(),
                "branch-reference-reuse".to_owned(),
            ],
            performance_eligible_pending_collector_confirmation,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Validate identities and recompute every aggregate that can be derived
    /// from the self-contained run receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete positions, inconsistent pass counts,
    /// changed percentile summaries, media drift, telemetry drift, or digest
    /// mismatch.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self) -> Result<(), String> {
        let point_hashes = self
            .point_position_receipt_sha256s
            .iter()
            .collect::<BTreeSet<_>>();
        let scan_hashes = self
            .scan_position_receipt_sha256s
            .iter()
            .collect::<BTreeSet<_>>();
        let point_binding_hashes = self
            .point_child_binding_sha256s
            .iter()
            .collect::<BTreeSet<_>>();
        let scan_binding_hashes = self
            .scan_child_binding_sha256s
            .iter()
            .collect::<BTreeSet<_>>();
        let point_passed_blocks = u64::try_from(
            self.point_blocks
                .iter()
                .filter(|block| block.passed)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let scan_passed_blocks =
            u64::try_from(self.scan_blocks.iter().filter(|block| block.passed).count())
                .unwrap_or(u64::MAX);
        let maximum_block_point_p99_ratio_millionths = self
            .point_blocks
            .iter()
            .map(|block| block.p99_ratio_millionths)
            .max()
            .unwrap_or(0);
        let maximum_point_bytes_ratio_millionths = self
            .point_blocks
            .iter()
            .map(|block| block.maximum_point_bytes_ratio_millionths)
            .max()
            .unwrap_or(0);
        let scan_ratios = self
            .scan_blocks
            .iter()
            .map(|block| block.throughput_ratio_millionths)
            .collect::<Vec<_>>();
        let telemetry_flush_passed = self.telemetry_metrics_flush_succeeded
            && self.telemetry_traces_flush_succeeded
            && self.telemetry_logs_flush_succeeded
            && self.telemetry_metrics_shutdown_succeeded
            && self.telemetry_traces_shutdown_succeeded
            && self.telemetry_logs_shutdown_succeeded;
        let measured_media_gates_passed = self.stored_media_ratio_millionths <= 1_100_000
            && self.resident_metadata_ratio_millionths <= 2_000_000;
        let admission_identity_is_frozen = matches!(
            (
                self.admission_plan_id.as_str(),
                self.admission_plan_sha256.as_str(),
            ),
            (
                "t28-aligned-columnar-v2-admission-r1",
                "1faec4b6eabd37ae99f2ac3309edec659915705ab31ab5e2c2f59cf7e784f01a",
            ) | (
                "t28-aligned-columnar-v2-admission-r2",
                "71ae74cde687872170459d9d0803875b077112a223ffe3cc6bb2e1100b0bb1d8",
            )
        );
        if self.schema_version != RUN_SCHEMA_VERSION
            || self.physical_plan_id != "t28-aligned-columnar-v2"
            || !admission_identity_is_frozen
            || !valid_sha256(&self.physical_plan_sha256)
            || self.controller_run_id.trim().is_empty()
            || self.candidate_commit.trim().is_empty()
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.cargo_lock_sha256)
            || !valid_sha256(&self.build_receipt_sha256)
            || !valid_sha256(&self.machine_receipt_sha256)
            || self.machine_instance_id.trim().is_empty()
            || self.collector_instance_id.trim().is_empty()
            || !valid_sha256(&self.reader_iam_receipt_sha256)
            || self.runtime_principal.trim().is_empty()
            || !valid_sha256(&self.telemetry_endpoint_sha256)
            || !valid_sha256(&self.fixture_id)
            || !valid_sha256(&self.root_sha256)
            || self.point_position_receipt_sha256s.len() != 60
            || point_hashes.len() != 60
            || self.scan_position_receipt_sha256s.len() != 30
            || scan_hashes.len() != 30
            || self.point_child_binding_sha256s.len() != 60
            || point_binding_hashes.len() != 60
            || self.scan_child_binding_sha256s.len() != 30
            || scan_binding_hashes.len() != 30
            || self.point_blocks.len() != 15
            || self.scan_blocks.len() != 15
            || self.point_passed_blocks != point_passed_blocks
            || self.point_gate_passed != (point_passed_blocks == 15)
            || self.maximum_block_point_p99_ratio_millionths
                != maximum_block_point_p99_ratio_millionths
            || self.maximum_point_bytes_ratio_millionths != maximum_point_bytes_ratio_millionths
            || self.scan_passed_blocks != scan_passed_blocks
            || self.scan_median_throughput_ratio_millionths != nearest_rank(&scan_ratios, 50, 100)?
            || self.scan_minimum_throughput_ratio_millionths
                != scan_ratios.iter().copied().min().unwrap_or(0)
            || self.scan_maximum_throughput_ratio_millionths
                != scan_ratios.iter().copied().max().unwrap_or(0)
            || self.scan_gate_passed
                != (scan_passed_blocks == 15
                    && self.scan_median_throughput_ratio_millionths >= 2_000_000)
            || !valid_sha256(&self.media_observation_sha256)
            || self.control_total_media_bytes == 0
            || self.candidate_total_media_bytes == 0
            || !valid_sha256(&self.control_closure_sha256)
            || !valid_sha256(&self.candidate_closure_sha256)
            || self.stored_media_ratio_millionths
                != ratio_millionths(
                    self.candidate_total_media_bytes,
                    self.control_total_media_bytes,
                )?
            || self.measured_media_gates_passed != measured_media_gates_passed
            || self.telemetry_flush_passed != telemetry_flush_passed
            || !self.collector_confirmation_required
            || self.performance_eligible_pending_collector_confirmation
                != (self.point_gate_passed
                    && self.scan_gate_passed
                    && self.measured_media_gates_passed
                    && self.telemetry_flush_passed)
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 performance-run receipt".to_owned());
        }
        Ok(())
    }

    /// Calculate the canonical receipt digest with the digest field cleared.
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

/// Sealed failure evidence emitted after telemetry is flushed and shut down.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedFailedRunReceiptV1 {
    pub schema_version: u32,
    pub physical_plan_sha256: String,
    pub admission_plan_sha256: String,
    pub controller_run_id: String,
    pub candidate_commit: String,
    pub error: String,
    pub completed_point_artifact_sha256s: Vec<String>,
    pub completed_scan_artifact_sha256s: Vec<String>,
    pub telemetry_metrics_flush_succeeded: bool,
    pub telemetry_traces_flush_succeeded: bool,
    pub telemetry_logs_flush_succeeded: bool,
    pub telemetry_metrics_shutdown_succeeded: bool,
    pub telemetry_traces_shutdown_succeeded: bool,
    pub telemetry_logs_shutdown_succeeded: bool,
    pub receipt_sha256: String,
}

impl T28AlignedFailedRunReceiptV1 {
    /// Seal a failed execution only after all telemetry exporters return.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        loaded: &LoadedT28AlignedCurvePlanV1,
        admission: &LoadedT28AlignedAdmissionPlanV1,
        controller_run_id: String,
        candidate_commit: String,
        error: String,
        completed_point_artifact_sha256s: Vec<String>,
        completed_scan_artifact_sha256s: Vec<String>,
        telemetry_flush: TelemetryFlushReport,
    ) -> Result<Self, String> {
        let mut receipt = Self {
            schema_version: 1,
            physical_plan_sha256: loaded.raw_sha256.clone(),
            admission_plan_sha256: admission.raw_sha256.clone(),
            controller_run_id,
            candidate_commit,
            error,
            completed_point_artifact_sha256s,
            completed_scan_artifact_sha256s,
            telemetry_metrics_flush_succeeded: telemetry_flush.metrics_flush_succeeded,
            telemetry_traces_flush_succeeded: telemetry_flush.traces_flush_succeeded,
            telemetry_logs_flush_succeeded: telemetry_flush.logs_flush_succeeded,
            telemetry_metrics_shutdown_succeeded: telemetry_flush.metrics_shutdown_succeeded,
            telemetry_traces_shutdown_succeeded: telemetry_flush.traces_shutdown_succeeded,
            telemetry_logs_shutdown_succeeded: telemetry_flush.logs_shutdown_succeeded,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        Ok(receipt)
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

/// Independent query result from the `OTel` collector for one controller run.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct T28AlignedCollectorQueryEvidenceV1 {
    schema_version: u32,
    checked_at: String,
    controller_run_id: String,
    collector: T28AlignedCollectorRuntimeV1,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct T28AlignedCollectorRuntimeV1 {
    instance_id: String,
    machine_id: String,
    boot_id: String,
    container_name: String,
    image: String,
    image_id: String,
    started_at: String,
    health: String,
}

fn matching_export_records(
    export: &[u8],
    resource_key: &str,
    performance: &T28AlignedPerformanceRunReceiptV1,
) -> Result<u64, String> {
    if export.is_empty() {
        return Err("RFC-0049 collector export is empty".to_owned());
    }
    let expected = BTreeMap::from([
        ("service.name", "okv-eval".to_owned()),
        ("service.version", env!("CARGO_PKG_VERSION").to_owned()),
        ("deployment.environment.name", "objectkv-dev-gcs".to_owned()),
        ("okv.eval.run.id", performance.controller_run_id.clone()),
        ("okv.eval.batch.id", performance.controller_run_id.clone()),
        ("okv.eval.suite.id", performance.admission_plan_id.clone()),
        (
            "okv.eval.suite.hash",
            performance.admission_plan_sha256.clone(),
        ),
        ("okv.eval.profile.id", "t28-rfc0049".to_owned()),
        (
            "okv.eval.profile.hash",
            performance.physical_plan_sha256.clone(),
        ),
        (
            "okv.eval.candidate.commit",
            performance.candidate_commit.clone(),
        ),
        ("okv.eval.backend", "gcs".to_owned()),
    ]);
    let mut matching = 0_u64;
    for line in export.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_slice(line).map_err(|error| error.to_string())?;
        let resources = value
            .get(resource_key)
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("RFC-0049 export omits {resource_key}"))?;
        for resource in resources {
            let attributes = resource
                .pointer("/resource/attributes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "RFC-0049 export resource omits attributes".to_owned())?;
            let mut observed = BTreeMap::new();
            for attribute in attributes {
                let key = attribute
                    .get("key")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "RFC-0049 export attribute omits key".to_owned())?;
                let string_value = attribute
                    .pointer("/value/stringValue")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "RFC-0049 export attribute is not a string".to_owned())?;
                if observed.insert(key, string_value).is_some() {
                    return Err("RFC-0049 export resource repeats an attribute".to_owned());
                }
            }
            if expected
                .iter()
                .all(|(key, value)| observed.get(key).copied() == Some(value.as_str()))
            {
                matching = matching.saturating_add(export_record_count(resource_key, resource)?);
            }
        }
    }
    Ok(matching)
}

fn export_record_count(resource_key: &str, resource: &serde_json::Value) -> Result<u64, String> {
    let count_array = |value: &serde_json::Value, field: &str| {
        value
            .get(field)
            .and_then(serde_json::Value::as_array)
            .map_or(0_u64, |items| {
                u64::try_from(items.len()).unwrap_or(u64::MAX)
            })
    };
    let count = match resource_key {
        "resourceLogs" => resource
            .get("scopeLogs")
            .and_then(serde_json::Value::as_array)
            .map_or(0, |scopes| {
                scopes.iter().fold(0_u64, |total, scope| {
                    total.saturating_add(count_array(scope, "logRecords"))
                })
            }),
        "resourceSpans" => resource
            .get("scopeSpans")
            .and_then(serde_json::Value::as_array)
            .map_or(0, |scopes| {
                scopes.iter().fold(0_u64, |total, scope| {
                    total.saturating_add(count_array(scope, "spans"))
                })
            }),
        "resourceMetrics" => resource
            .get("scopeMetrics")
            .and_then(serde_json::Value::as_array)
            .map_or(0, |scopes| {
                scopes.iter().fold(0_u64, |scope_total, scope| {
                    let metrics = scope.get("metrics").and_then(serde_json::Value::as_array);
                    scope_total.saturating_add(metrics.map_or(0, |metrics| {
                        metrics.iter().fold(0_u64, |metric_total, metric| {
                            let points = [
                                "gauge",
                                "sum",
                                "histogram",
                                "exponentialHistogram",
                                "summary",
                            ]
                            .iter()
                            .fold(0_u64, |total, kind| {
                                total.saturating_add(
                                    metric
                                        .get(kind)
                                        .map_or(0, |data| count_array(data, "dataPoints")),
                                )
                            });
                            metric_total.saturating_add(points)
                        })
                    }))
                })
            }),
        _ => return Err("RFC-0049 collector export has an unknown signal".to_owned()),
    };
    Ok(count)
}

/// Independent query result from the `OTel` collector for one controller run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedCollectorConfirmationV1 {
    pub schema_version: u32,
    pub controller_run_id: String,
    pub performance_run_sha256: String,
    pub collector_instance_id: String,
    pub collector_query_evidence_sha256: String,
    pub logs_export_bytes: u64,
    pub logs_export_sha256: String,
    pub metrics_export_bytes: u64,
    pub metrics_export_sha256: String,
    pub traces_export_bytes: u64,
    pub traces_export_sha256: String,
    pub matching_log_records: u64,
    pub matching_metric_points: u64,
    pub matching_trace_spans: u64,
    pub exact_run_resource_matched: bool,
    pub receipt_sha256: String,
}

impl T28AlignedCollectorConfirmationV1 {
    /// Parse one typed collector-side query result and derive confirmation
    /// counts and identity without caller-provided assertions.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed collector output, a machine or run
    /// mismatch, absent signals, or invalid exported-file identities.
    pub fn from_collector_exports(
        performance: &T28AlignedPerformanceRunReceiptV1,
        evidence_bytes: &[u8],
        logs_export: &[u8],
        metrics_export: &[u8],
        traces_export: &[u8],
    ) -> Result<Self, String> {
        performance.validate()?;
        let evidence: T28AlignedCollectorQueryEvidenceV1 =
            serde_json::from_slice(evidence_bytes).map_err(|error| error.to_string())?;
        if evidence.schema_version != 1
            || evidence.checked_at.trim().is_empty()
            || evidence.controller_run_id != performance.controller_run_id
            || evidence.collector.instance_id != performance.collector_instance_id
            || evidence.collector.machine_id.trim().is_empty()
            || evidence.collector.boot_id.trim().is_empty()
            || evidence.collector.container_name != "objectkv-otel"
            || evidence.collector.image.trim().is_empty()
            || !evidence.collector.image_id.starts_with("sha256:")
            || evidence.collector.image_id.len() != 71
            || evidence.collector.started_at.trim().is_empty()
            || evidence.collector.health != "Server available"
        {
            return Err("invalid RFC-0049 typed collector query evidence".to_owned());
        }
        let matching_log_records =
            matching_export_records(logs_export, "resourceLogs", performance)?;
        let matching_metric_points =
            matching_export_records(metrics_export, "resourceMetrics", performance)?;
        let matching_trace_spans =
            matching_export_records(traces_export, "resourceSpans", performance)?;
        if matching_log_records == 0 || matching_metric_points == 0 || matching_trace_spans == 0 {
            return Err("RFC-0049 collector exports omit a required exact-run signal".to_owned());
        }
        let mut receipt = Self {
            schema_version: 1,
            controller_run_id: performance.controller_run_id.clone(),
            performance_run_sha256: performance.receipt_sha256.clone(),
            collector_instance_id: evidence.collector.instance_id,
            collector_query_evidence_sha256: content_sha256(evidence_bytes),
            logs_export_bytes: u64::try_from(logs_export.len()).unwrap_or(u64::MAX),
            logs_export_sha256: content_sha256(logs_export),
            metrics_export_bytes: u64::try_from(metrics_export.len()).unwrap_or(u64::MAX),
            metrics_export_sha256: content_sha256(metrics_export),
            traces_export_bytes: u64::try_from(traces_export.len()).unwrap_or(u64::MAX),
            traces_export_sha256: content_sha256(traces_export),
            matching_log_records,
            matching_metric_points,
            matching_trace_spans,
            exact_run_resource_matched: true,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        receipt.validate()?;
        Ok(receipt)
    }

    /// Decode and authenticate an independent collector query receipt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or receipt-digest drift.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        receipt.validate()?;
        Ok(receipt)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1
            || self.controller_run_id.trim().is_empty()
            || !valid_sha256(&self.performance_run_sha256)
            || self.collector_instance_id.trim().is_empty()
            || !valid_sha256(&self.collector_query_evidence_sha256)
            || self.logs_export_bytes == 0
            || !valid_sha256(&self.logs_export_sha256)
            || self.metrics_export_bytes == 0
            || !valid_sha256(&self.metrics_export_sha256)
            || self.traces_export_bytes == 0
            || !valid_sha256(&self.traces_export_sha256)
            || self.matching_log_records == 0
            || self.matching_metric_points == 0
            || self.matching_trace_spans == 0
            || !self.exact_run_resource_matched
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 collector confirmation receipt".to_owned());
        }
        Ok(())
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

/// Final performance verdict after independent `OTel` collector confirmation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedVerifiedRunReceiptV1 {
    pub schema_version: u32,
    pub controller_run_id: String,
    pub performance_run_sha256: String,
    pub collector_confirmation_sha256: String,
    pub point_gate_passed: bool,
    pub scan_gate_passed: bool,
    pub measured_media_gates_passed: bool,
    pub telemetry_exporter_flush_passed: bool,
    pub independent_collector_confirmation_passed: bool,
    pub verified: bool,
    pub receipt_sha256: String,
}

impl T28AlignedVerifiedRunReceiptV1 {
    /// Join the self-validating performance run to an independent collector
    /// observation and seal the final verdict.
    ///
    /// # Errors
    ///
    /// Returns an error only when either input receipt is structurally invalid.
    /// A valid input pair that fails an admission gate produces a sealed
    /// `verified = false` receipt.
    pub fn finalize(
        performance: &T28AlignedPerformanceRunReceiptV1,
        collector: &T28AlignedCollectorConfirmationV1,
    ) -> Result<Self, String> {
        performance.validate()?;
        collector.validate()?;
        let performance_run_sha256 = performance.receipt_sha256.clone();
        let independent_collector_confirmation_passed = collector.controller_run_id
            == performance.controller_run_id
            && collector.performance_run_sha256 == performance_run_sha256
            && collector.exact_run_resource_matched
            && collector.matching_log_records > 0
            && collector.matching_metric_points > 0
            && collector.matching_trace_spans > 0;
        let verified = performance.performance_eligible_pending_collector_confirmation
            && independent_collector_confirmation_passed;
        let mut receipt = Self {
            schema_version: 1,
            controller_run_id: performance.controller_run_id.clone(),
            performance_run_sha256,
            collector_confirmation_sha256: collector.receipt_sha256.clone(),
            point_gate_passed: performance.point_gate_passed,
            scan_gate_passed: performance.scan_gate_passed,
            measured_media_gates_passed: performance.measured_media_gates_passed,
            telemetry_exporter_flush_passed: performance.telemetry_flush_passed,
            independent_collector_confirmation_passed,
            verified,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256()?;
        Ok(receipt)
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

#[allow(clippy::too_many_lines)]
fn aggregate_point_block(
    plan: &T28AlignedCurvePlanV1,
    trace_seed: u64,
    block_ordinal: u64,
    receipts: &[T28AlignedPointPositionReceiptV2],
) -> Result<T28AlignedPointBlockReceiptV1, String> {
    let mut candidate_latencies = Vec::new();
    let mut control_latencies = Vec::new();
    let mut candidate_response_bytes = 0_u64;
    let mut control_response_bytes = 0_u64;
    let mut candidate_provider_attempts = 0_u64;
    let mut control_provider_attempts = 0_u64;
    let mut candidate_maximum_point_bytes = 0_u64;
    let mut control_maximum_point_bytes = 0_u64;
    let mut candidate_overlapping_pairs = 0_u64;
    let mut candidate_points = 0_u64;
    for receipt in receipts {
        let base = &receipt.base;
        if base.trace_seed != trace_seed {
            return Err("RFC-0049 point block crosses trace seeds".to_owned());
        }
        match base.subject {
            T28TypedPointSubjectV1::C5v2AlignedColumnar => {
                candidate_latencies.extend(&base.latency_nanos);
                candidate_response_bytes =
                    candidate_response_bytes.saturating_add(base.measured_response_bytes);
                candidate_provider_attempts =
                    candidate_provider_attempts.saturating_add(base.measured_provider_attempts);
                candidate_overlapping_pairs =
                    candidate_overlapping_pairs.saturating_add(base.overlapping_point_pairs);
                candidate_points = candidate_points.saturating_add(base.measured_operations);
                candidate_maximum_point_bytes = candidate_maximum_point_bytes.max(
                    receipt
                        .operation_latency_samples
                        .iter()
                        .map(|sample| {
                            sample
                                .attempts
                                .iter()
                                .map(|attempt| attempt.response_payload_bytes)
                                .sum::<u64>()
                        })
                        .max()
                        .unwrap_or(0),
                );
            }
            T28TypedPointSubjectV1::C0IndexedRow => {
                control_latencies.extend(&base.latency_nanos);
                control_response_bytes =
                    control_response_bytes.saturating_add(base.measured_response_bytes);
                control_provider_attempts =
                    control_provider_attempts.saturating_add(base.measured_provider_attempts);
                control_maximum_point_bytes = control_maximum_point_bytes.max(
                    receipt
                        .operation_latency_samples
                        .iter()
                        .map(|sample| {
                            sample
                                .attempts
                                .iter()
                                .map(|attempt| attempt.response_payload_bytes)
                                .sum::<u64>()
                        })
                        .max()
                        .unwrap_or(0),
                );
            }
            T28TypedPointSubjectV1::C5ColumnarMain => {
                return Err("RFC-0049 point block contains C5v1".to_owned());
            }
        }
    }
    let expected_samples = plan.point_lane.reads_per_position.saturating_mul(2);
    if u64::try_from(candidate_latencies.len()).unwrap_or(u64::MAX) != expected_samples
        || u64::try_from(control_latencies.len()).unwrap_or(u64::MAX) != expected_samples
        || candidate_provider_attempts
            != candidate_points
                .saturating_mul(plan.point_lane.candidate_sdk_attempts_per_indexed_point)
        || control_provider_attempts
            != expected_samples.saturating_mul(plan.point_lane.control_sdk_attempts_per_point)
    {
        return Err("RFC-0049 point block has the wrong sample or attempt count".to_owned());
    }
    let candidate_p99_nanos = nearest_rank(&candidate_latencies, 99, 100)?;
    let control_p99_nanos = nearest_rank(&control_latencies, 99, 100)?;
    let p99_ratio_millionths = ratio_millionths(candidate_p99_nanos, control_p99_nanos)?;
    let response_bytes_ratio_millionths =
        ratio_millionths(candidate_response_bytes, control_response_bytes)?;
    let maximum_point_bytes_ratio_millionths =
        ratio_millionths(candidate_maximum_point_bytes, control_maximum_point_bytes)?;
    let passed = p99_ratio_millionths <= plan.point_lane.candidate_p99_ratio_millionths_max
        && maximum_point_bytes_ratio_millionths
            <= plan.point_lane.candidate_bytes_ratio_millionths_max
        && candidate_overlapping_pairs == candidate_points;
    Ok(T28AlignedPointBlockReceiptV1 {
        trace_seed,
        block_ordinal,
        candidate_p99_nanos,
        control_p99_nanos,
        p99_ratio_millionths,
        candidate_response_bytes,
        control_response_bytes,
        response_bytes_ratio_millionths,
        candidate_maximum_point_bytes,
        control_maximum_point_bytes,
        maximum_point_bytes_ratio_millionths,
        candidate_provider_attempts,
        control_provider_attempts,
        candidate_overlapping_pairs,
        candidate_points,
        passed,
    })
}

fn aggregate_scan_block(
    plan: &T28AlignedCurvePlanV1,
    trace_seed: u64,
    block_ordinal: u64,
    receipts: &[T28TypedScanPositionReceiptV1],
) -> Result<T28AlignedScanBlockReceiptV1, String> {
    if receipts.len() != 2 {
        return Err("RFC-0049 scan block does not contain two positions".to_owned());
    }
    let candidate = receipts
        .iter()
        .find(|receipt| receipt.subject == T28TypedScanSubjectV1::C5v2AlignedColumnar)
        .ok_or_else(|| "RFC-0049 scan block omitted its candidate".to_owned())?;
    let control = receipts
        .iter()
        .find(|receipt| receipt.subject == T28TypedScanSubjectV1::C0IndexedRow)
        .ok_or_else(|| "RFC-0049 scan block omitted its control".to_owned())?;
    if candidate.trace_seed != trace_seed
        || control.trace_seed != trace_seed
        || candidate.rows != control.rows
        || candidate.ordered_projection_sha256 != control.ordered_projection_sha256
        || candidate.quantity_sum != control.quantity_sum
    {
        return Err("RFC-0049 scan block result or seed mismatch".to_owned());
    }
    let throughput_ratio_millionths =
        ratio_millionths(control.query_elapsed_nanos, candidate.query_elapsed_nanos)?;
    let response_bytes_ratio_millionths =
        ratio_millionths(candidate.response_bytes, control.response_bytes)?;
    let passed = candidate.provider_attempts
        <= plan.scan_lane.candidate_gets_per_complete_projection_max
        && response_bytes_ratio_millionths
            <= plan.scan_lane.candidate_response_bytes_ratio_millionths_max
        && candidate.opaque_payload_requests
            <= plan.scan_lane.candidate_opaque_payload_requests_max
        && candidate.opaque_payload_response_bytes
            <= plan.scan_lane.candidate_opaque_payload_bytes_max
        && candidate.peak_fetch_bytes <= plan.scan_lane.peak_fetch_bytes_max
        && candidate.peak_arrow_batch_rows <= plan.scan_lane.peak_arrow_batch_rows_max;
    Ok(T28AlignedScanBlockReceiptV1 {
        trace_seed,
        block_ordinal,
        candidate_query_elapsed_nanos: candidate.query_elapsed_nanos,
        control_query_elapsed_nanos: control.query_elapsed_nanos,
        throughput_ratio_millionths,
        candidate_provider_attempts: candidate.provider_attempts,
        control_provider_attempts: control.provider_attempts,
        candidate_response_bytes: candidate.response_bytes,
        control_response_bytes: control.response_bytes,
        response_bytes_ratio_millionths,
        passed,
    })
}

fn consistent_metadata(
    receipts: &[T28AlignedPointPositionReceiptV2],
    subject: T28TypedPointSubjectV1,
) -> Result<u64, String> {
    let values = receipts
        .iter()
        .filter(|receipt| receipt.base.subject == subject)
        .map(|receipt| receipt.base.resident_metadata_bytes)
        .collect::<BTreeSet<_>>();
    if values.len() != 1 {
        return Err("RFC-0049 resident metadata changed across positions".to_owned());
    }
    values
        .first()
        .copied()
        .ok_or_else(|| "RFC-0049 resident metadata is absent".to_owned())
}

fn percentiles(values: &[u64]) -> Result<T28AlignedCurvePercentilesV1, String> {
    Ok(T28AlignedCurvePercentilesV1 {
        samples: u64::try_from(values.len()).unwrap_or(u64::MAX),
        p50_nanos: nearest_rank(values, 50, 100)?,
        p95_nanos: nearest_rank(values, 95, 100)?,
        p99_nanos: nearest_rank(values, 99, 100)?,
        p999_nanos: nearest_rank(values, 999, 1_000)?,
    })
}

fn nearest_rank(values: &[u64], numerator: usize, denominator: usize) -> Result<u64, String> {
    if values.is_empty() || numerator == 0 || denominator == 0 || numerator > denominator {
        return Err("invalid RFC-0049 nearest-rank input".to_owned());
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = sorted
        .len()
        .saturating_mul(numerator)
        .saturating_add(denominator - 1)
        / denominator;
    sorted
        .get(rank.saturating_sub(1).min(sorted.len() - 1))
        .copied()
        .ok_or_else(|| "RFC-0049 nearest-rank sample is absent".to_owned())
}

fn ratio_millionths(numerator: u64, denominator: u64) -> Result<u64, String> {
    if denominator == 0 {
        return Err("RFC-0049 ratio denominator is zero".to_owned());
    }
    let scaled = u128::from(numerator)
        .saturating_mul(1_000_000)
        .saturating_add(u128::from(denominator) / 2)
        / u128::from(denominator);
    u64::try_from(scaled).map_err(|error| error.to_string())
}

fn validate_orders(seed_orders: &[T28AlignedSeedOrdersV1], positions: usize) -> Result<(), String> {
    if seed_orders.len() != 3 {
        return Err("RFC-0049 plan must contain three seeds".to_owned());
    }
    for (seed_index, (seed, expected_seed)) in seed_orders
        .iter()
        .zip([5_701_u64, 5_702, 5_703])
        .enumerate()
    {
        if seed.seed != expected_seed || seed.orders.len() != 5 {
            return Err("RFC-0049 seed schedule is not canonical".to_owned());
        }
        for (block_index, order) in seed.orders.iter().enumerate() {
            let candidate_first = (seed_index + block_index) % 2 == 0;
            let expected = match (positions, candidate_first) {
                (4, true) => "ABBA",
                (4, false) => "BAAB",
                (2, true) => "AB",
                (2, false) => "BA",
                _ => return Err("RFC-0049 position count is unsupported".to_owned()),
            };
            if order != expected {
                return Err("RFC-0049 subject schedule does not rotate".to_owned());
            }
        }
    }
    Ok(())
}

fn parse_point_order(order: &str) -> Result<Vec<T28TypedPointSubjectV1>, String> {
    order
        .chars()
        .map(|marker| match marker {
            'A' => Ok(T28TypedPointSubjectV1::C5v2AlignedColumnar),
            'B' => Ok(T28TypedPointSubjectV1::C0IndexedRow),
            _ => Err("RFC-0049 point order contains an unknown subject".to_owned()),
        })
        .collect()
}

fn parse_scan_order(order: &str) -> Result<Vec<T28TypedScanSubjectV1>, String> {
    order
        .chars()
        .map(|marker| match marker {
            'A' => Ok(T28TypedScanSubjectV1::C5v2AlignedColumnar),
            'B' => Ok(T28TypedScanSubjectV1::C0IndexedRow),
            _ => Err("RFC-0049 scan order contains an unknown subject".to_owned()),
        })
        .collect()
}

const fn point_subject_id(subject: T28TypedPointSubjectV1) -> &'static str {
    match subject {
        T28TypedPointSubjectV1::C0IndexedRow => "c0_indexed_row",
        T28TypedPointSubjectV1::C5ColumnarMain => "c5_columnar_main",
        T28TypedPointSubjectV1::C5v2AlignedColumnar => "c5v2_aligned_columnar",
    }
}

const fn scan_subject_id(subject: T28TypedScanSubjectV1) -> &'static str {
    match subject {
        T28TypedScanSubjectV1::C0IndexedRow => "c0_indexed_row",
        T28TypedScanSubjectV1::C5ColumnarMain => "c5_columnar_main",
        T28TypedScanSubjectV1::C5v2AlignedColumnar => "c5v2_aligned_columnar",
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_git_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{
        T28AlignedAdmissionPlanV1, T28AlignedBuildReceiptV1, T28AlignedChildBindingV1,
        T28AlignedChildLaneV1, T28AlignedCollectorConfirmationV1, T28AlignedCurvePlanV1,
        T28AlignedMachineIdentityV1, T28AlignedPerformanceRunReceiptV1,
        T28AlignedVerifiedRunReceiptV1,
    };
    use crate::t28_layout::{
        TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1, TypedLayoutPlacementLocatorV1,
    };
    use crate::t28_layout_position::{
        validate_t28_aligned_candidate_locator, T28AlignedMediaObservationV1,
        T28AlignedObjectRoleV2, T28AlignedPointOperationV2, T28AlignedPointPositionReceiptV2,
        T28AlignedProviderAttemptV2, T28TypedPointPositionReceiptV1, T28TypedPointSubjectV1,
        T28TypedScanPositionReceiptV1, T28TypedScanSubjectV1,
    };
    use crate::telemetry::TelemetryFlushReport;
    use okv_object::content_sha256;

    #[test]
    fn persisted_scan_subject_ids_match_live_controller_ids() {
        assert_eq!(
            super::scan_subject_id(T28TypedScanSubjectV1::C0IndexedRow),
            "c0_indexed_row"
        );
        assert_eq!(
            super::scan_subject_id(T28TypedScanSubjectV1::C5ColumnarMain),
            "c5_columnar_main"
        );
        assert_eq!(
            super::scan_subject_id(T28TypedScanSubjectV1::C5v2AlignedColumnar),
            "c5v2_aligned_columnar"
        );
    }

    #[test]
    fn admission_plan_freezes_the_post_diagnostic_execution_contract() {
        let bytes =
            include_bytes!("../../../evals/plans/t28-aligned-columnar-v2-admission-r1.toml");
        let loaded = T28AlignedAdmissionPlanV1::decode(
            bytes,
            "1faec4b6eabd37ae99f2ac3309edec659915705ab31ab5e2c2f59cf7e784f01a",
        )
        .expect("decode admission plan");
        assert_eq!(
            loaded.plan.named_code_change,
            "rust-controller-and-logical-point-provider-correlation"
        );
        assert!(!loaded.plan.scope.complete_child_closure_recovery);
    }

    #[test]
    fn second_admission_plan_names_and_retains_the_first_admitted_failure() {
        let bytes =
            include_bytes!("../../../evals/plans/t28-aligned-columnar-v2-admission-r2.toml");
        let loaded = T28AlignedAdmissionPlanV1::decode(bytes, &content_sha256(bytes))
            .expect("decode second admission plan");
        assert_eq!(
            loaded.plan.named_code_change,
            "fixture-exact-same-role-object-selection"
        );
        assert_eq!(
            loaded.plan.prior_diagnostic_archive_sha256,
            "90d2b6c29047edbe3d6b32dff071c69a8d7e1ca4f91ddb3e86fb0c71da49215d"
        );
    }

    #[test]
    fn frozen_plan_has_exact_point_and_scan_schedules() {
        let bytes = include_bytes!("../../../evals/plans/t28-aligned-columnar-v2.toml");
        let loaded = T28AlignedCurvePlanV1::decode(
            bytes,
            "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61",
        )
        .expect("decode aligned plan");
        assert_eq!(loaded.plan.expected_point_positions().unwrap().len(), 60);
        assert_eq!(loaded.plan.expected_scan_positions().unwrap().len(), 30);
    }

    #[test]
    fn plan_digest_drift_fails_before_schedule_construction() {
        let bytes = include_bytes!("../../../evals/plans/t28-aligned-columnar-v2.toml");
        let error = T28AlignedCurvePlanV1::decode(bytes, &"0".repeat(64))
            .err()
            .expect("digest mismatch");
        assert!(error.contains("raw SHA-256 mismatch"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn aggregate_recomputes_all_ninety_positions_and_collector_finalizes() {
        let plan = T28AlignedCurvePlanV1::decode(
            include_bytes!("../../../evals/plans/t28-aligned-columnar-v2.toml"),
            "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61",
        )
        .expect("curve plan");
        let admission = T28AlignedAdmissionPlanV1::decode(
            include_bytes!("../../../evals/plans/t28-aligned-columnar-v2-admission-r1.toml"),
            "1faec4b6eabd37ae99f2ac3309edec659915705ab31ab5e2c2f59cf7e784f01a",
        )
        .expect("admission plan");
        let executable_sha256 = "e".repeat(64);
        let cargo_lock_sha256 = "c".repeat(64);
        let candidate_commit = "0123456789012345678901234567890123456789".to_owned();
        let build_receipt = T28AlignedBuildReceiptV1::seal(
            admission.plan.candidate_parent_commit.clone(),
            candidate_commit.clone(),
            executable_sha256.clone(),
            cargo_lock_sha256.clone(),
            "release".to_owned(),
        )
        .expect("build receipt");
        let controller_run_id = "controller-r0".to_owned();
        let mut points = Vec::new();
        let mut point_bindings = Vec::new();
        for (index, expected) in plan
            .plan
            .expected_point_positions()
            .expect("point schedule")
            .into_iter()
            .enumerate()
        {
            let process_id = u32::try_from(index + 1).expect("point pid");
            let receipt = point_receipt(expected.subject, expected.trace_seed, process_id);
            point_bindings.push(
                T28AlignedChildBindingV1::seal(
                    controller_run_id.clone(),
                    admission.raw_sha256.clone(),
                    plan.raw_sha256.clone(),
                    admission.plan.position_execution_plan_sha256.clone(),
                    T28AlignedChildLaneV1::Point,
                    expected.trace_seed,
                    expected.block_ordinal,
                    expected.position_in_block,
                    super::point_subject_id(expected.subject).to_owned(),
                    process_id,
                    executable_sha256.clone(),
                    receipt.receipt_sha256.clone(),
                )
                .expect("point binding"),
            );
            points.push(receipt);
        }
        let mut scans = Vec::new();
        let mut scan_bindings = Vec::new();
        for (index, expected) in plan
            .plan
            .expected_scan_positions()
            .expect("scan schedule")
            .into_iter()
            .enumerate()
        {
            let process_id = u32::try_from(index + 61).expect("scan pid");
            let receipt = scan_receipt(expected.subject, expected.trace_seed, process_id);
            scan_bindings.push(
                T28AlignedChildBindingV1::seal(
                    controller_run_id.clone(),
                    admission.raw_sha256.clone(),
                    plan.raw_sha256.clone(),
                    admission.plan.position_execution_plan_sha256.clone(),
                    T28AlignedChildLaneV1::Scan,
                    expected.trace_seed,
                    expected.block_ordinal,
                    expected.position_in_block,
                    super::scan_subject_id(expected.subject).to_owned(),
                    process_id,
                    executable_sha256.clone(),
                    receipt.receipt_sha256.clone(),
                )
                .expect("scan binding"),
            );
            scans.push(receipt);
        }
        let media = media_observation();
        validate_t28_aligned_candidate_locator(&media, &candidate_locator("10"))
            .expect("bound candidate locator");
        assert!(validate_t28_aligned_candidate_locator(&media, &candidate_locator("11")).is_err());
        let performance = T28AlignedPerformanceRunReceiptV1::new(
            &plan,
            &admission,
            controller_run_id.clone(),
            candidate_commit,
            executable_sha256,
            cargo_lock_sha256,
            &build_receipt,
            "d".repeat(64),
            &T28AlignedMachineIdentityV1 {
                instance_id: "runner-1".to_owned(),
                collector_instance_id: "collector-1".to_owned(),
                service_account: "reader@example.iam.gserviceaccount.com".to_owned(),
                lease_expires_epoch: u64::MAX,
            },
            admission.plan.reader_iam_receipt_sha256.clone(),
            "f".repeat(64),
            TelemetryFlushReport::succeeded(),
            &media,
            &points,
            &scans,
            &point_bindings,
            &scan_bindings,
        )
        .expect("aggregate performance receipt");
        assert!(performance.performance_eligible_pending_collector_confirmation);
        let encoded = serde_json::to_vec(&performance).expect("encode performance");
        T28AlignedPerformanceRunReceiptV1::decode(&encoded).expect("decode performance");
        let mut r2_performance = performance.clone();
        r2_performance.admission_plan_id = "t28-aligned-columnar-v2-admission-r2".to_owned();
        r2_performance.admission_plan_sha256 =
            "71ae74cde687872170459d9d0803875b077112a223ffe3cc6bb2e1100b0bb1d8".to_owned();
        r2_performance.receipt_sha256 = r2_performance
            .calculated_sha256()
            .expect("r2 performance digest");
        r2_performance
            .validate()
            .expect("r2 performance receipt identity");

        let collector_evidence = collector_query_evidence(&controller_run_id);
        let logs = collector_export("resourceLogs", &performance, true);
        let metrics = collector_export("resourceMetrics", &performance, true);
        let traces = collector_export("resourceSpans", &performance, true);
        let collector = T28AlignedCollectorConfirmationV1::from_collector_exports(
            &performance,
            &collector_evidence,
            &logs,
            &metrics,
            &traces,
        )
        .expect("collector receipt");
        let verified = T28AlignedVerifiedRunReceiptV1::finalize(&performance, &collector)
            .expect("verified receipt");
        assert!(verified.verified);

        let mut mismatched_collector = collector.clone();
        mismatched_collector.performance_run_sha256 = "9".repeat(64);
        mismatched_collector.receipt_sha256 = mismatched_collector
            .calculated_sha256()
            .expect("mismatched collector digest");
        let rejected =
            T28AlignedVerifiedRunReceiptV1::finalize(&performance, &mismatched_collector)
                .expect("sealed non-verified receipt");
        assert!(!rejected.verified);

        let missing_logs = collector_export("resourceLogs", &performance, false);
        assert!(T28AlignedCollectorConfirmationV1::from_collector_exports(
            &performance,
            &collector_evidence,
            &missing_logs,
            &metrics,
            &traces,
        )
        .is_err());
    }

    fn collector_query_evidence(controller_run_id: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "checked_at": "2026-08-30T00:00:00Z",
            "controller_run_id": controller_run_id,
            "collector": {
                "instance_id": "collector-1",
                "machine_id": "machine-1",
                "boot_id": "boot-1",
                "container_name": "objectkv-otel",
                "image": "otel/opentelemetry-collector-contrib:0.157.0",
                "image_id": format!("sha256:{}", "9".repeat(64)),
                "started_at": "2026-08-30T00:00:00Z",
                "health": "Server available"
            }
        }))
        .expect("collector query evidence")
    }

    fn collector_export(
        resource_key: &str,
        performance: &T28AlignedPerformanceRunReceiptV1,
        matching: bool,
    ) -> Vec<u8> {
        let run_id = if matching {
            performance.controller_run_id.clone()
        } else {
            "different-run".to_owned()
        };
        let attributes = [
            ("service.name", "okv-eval".to_owned()),
            ("service.version", env!("CARGO_PKG_VERSION").to_owned()),
            ("deployment.environment.name", "objectkv-dev-gcs".to_owned()),
            ("okv.eval.run.id", run_id.clone()),
            ("okv.eval.batch.id", run_id),
            ("okv.eval.suite.id", performance.admission_plan_id.clone()),
            (
                "okv.eval.suite.hash",
                performance.admission_plan_sha256.clone(),
            ),
            ("okv.eval.profile.id", "t28-rfc0049".to_owned()),
            (
                "okv.eval.profile.hash",
                performance.physical_plan_sha256.clone(),
            ),
            (
                "okv.eval.candidate.commit",
                performance.candidate_commit.clone(),
            ),
            ("okv.eval.backend", "gcs".to_owned()),
        ]
        .into_iter()
        .map(|(key, value)| {
            serde_json::json!({
                "key": key,
                "value": { "stringValue": value }
            })
        })
        .collect::<Vec<_>>();
        let signal_records = match resource_key {
            "resourceLogs" => serde_json::json!({
                "scopeLogs": [{ "logRecords": [{}] }]
            }),
            "resourceMetrics" => serde_json::json!({
                "scopeMetrics": [{
                    "metrics": [{ "gauge": { "dataPoints": [{}] } }]
                }]
            }),
            "resourceSpans" => serde_json::json!({
                "scopeSpans": [{ "spans": [{}] }]
            }),
            other => panic!("unknown collector test signal {other}"),
        };
        let mut resource = signal_records
            .as_object()
            .cloned()
            .expect("signal resource object");
        resource.insert(
            "resource".to_owned(),
            serde_json::json!({ "attributes": attributes }),
        );
        let mut root = serde_json::Map::new();
        root.insert(resource_key.to_owned(), serde_json::json!([resource]));
        let mut bytes =
            serde_json::to_vec(&serde_json::Value::Object(root)).expect("collector export record");
        bytes.push(b'\n');
        bytes
    }

    #[allow(clippy::too_many_lines)]
    fn point_receipt(
        subject: T28TypedPointSubjectV1,
        trace_seed: u64,
        process_id: u32,
    ) -> T28AlignedPointPositionReceiptV2 {
        let candidate = subject == T28TypedPointSubjectV1::C5v2AlignedColumnar;
        let end_to_end = if candidate { 120 } else { 100 };
        let provider_pair_max = if candidate { 60 } else { 80 };
        let pair_completion = if candidate { 60 } else { 80 };
        let local_residual = end_to_end - pair_completion;
        let operations = (0..1_024_u64)
            .map(|ordinal| {
                let attempts = if candidate {
                    vec![
                        provider_attempt(T28AlignedObjectRoleV2::Payload, ordinal, 1, 60, 40),
                        provider_attempt(T28AlignedObjectRoleV2::Projection, ordinal, 6, 50, 40),
                    ]
                } else {
                    vec![provider_attempt(
                        T28AlignedObjectRoleV2::IndexedRow,
                        ordinal,
                        1,
                        80,
                        200,
                    )]
                };
                T28AlignedPointOperationV2 {
                    ordinal,
                    end_to_end_nanos: end_to_end,
                    provider_pair_max_nanos: provider_pair_max,
                    local_residual_nanos: local_residual,
                    pair_start_skew_nanos: if candidate { 5 } else { 0 },
                    pair_completion_nanos: pair_completion,
                    provider_attempts: if candidate { 2 } else { 1 },
                    provider_pair_overlapped: candidate,
                    attempts,
                }
            })
            .collect::<Vec<_>>();
        let provider_latency_nanos = if candidate {
            [vec![50_u64; 1_024], vec![60_u64; 1_024]].concat()
        } else {
            vec![80; 1_024]
        };
        let measured_provider_attempts = if candidate { 2_048 } else { 1_024 };
        let measured_response_bytes = if candidate { 81_920 } else { 204_800 };
        let mut base = T28TypedPointPositionReceiptV1 {
            schema_version: 1,
            execution_plan_sha256:
                "2e04d69775f67cb7561b59374d27bf2082909ca2df23a72f40e209728131c797".to_owned(),
            fixture_id: "1".repeat(64),
            root_sha256: "2".repeat(64),
            subject,
            trace_seed,
            measured_operations: 1_024,
            concurrent_tasks: 8,
            warmup_canary_reads: 128,
            resident_metadata_bytes: 100,
            measured_provider_attempts,
            measured_response_bytes,
            maximum_point_bytes_upper_bound: if candidate { 80 } else { 200 },
            maximum_attempts_per_point: if candidate { 2 } else { 1 },
            full_object_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            missing_expected_generation_requests: 0,
            returned_generation_mismatches: 0,
            provider_errors: 0,
            correctness_anomalies: 0,
            point_pairs: if candidate { 1_024 } else { 0 },
            overlapping_point_pairs: if candidate { 1_024 } else { 0 },
            latency_nanos: vec![end_to_end; 1_024],
            p50_latency_nanos: end_to_end,
            p95_latency_nanos: end_to_end,
            p99_latency_nanos: end_to_end,
            p999_latency_nanos: end_to_end,
            provider_p50_latency_nanos: if candidate { 50 } else { 80 },
            provider_p95_latency_nanos: provider_pair_max,
            provider_p99_latency_nanos: provider_pair_max,
            provider_p999_latency_nanos: provider_pair_max,
            provider_latency_nanos,
            wall_elapsed_nanos: 1_000,
            process_id,
            measured_started_unix_nanos: u64::from(process_id) * 10,
            measured_finished_unix_nanos: u64::from(process_id) * 10 + 1,
            receipt_sha256: String::new(),
        };
        base.receipt_sha256 = base.calculated_sha256().expect("base digest");
        let mut receipt = T28AlignedPointPositionReceiptV2 {
            schema_version: 2,
            base,
            operation_latency_samples: operations,
            provider_pair_max_p50_nanos: provider_pair_max,
            provider_pair_max_p95_nanos: provider_pair_max,
            provider_pair_max_p99_nanos: provider_pair_max,
            provider_pair_max_p999_nanos: provider_pair_max,
            local_residual_p50_nanos: local_residual,
            local_residual_p95_nanos: local_residual,
            local_residual_p99_nanos: local_residual,
            local_residual_p999_nanos: local_residual,
            maximum_pair_start_skew_nanos: if candidate { 5 } else { 0 },
            maximum_pair_completion_nanos: pair_completion,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256().expect("point digest");
        receipt
            .validate()
            .unwrap_or_else(|error| panic!("{subject:?} point receipt: {error}"));
        receipt
    }

    fn provider_attempt(
        role: T28AlignedObjectRoleV2,
        _ordinal: u64,
        started_monotonic_nanos: u64,
        elapsed_nanos: u64,
        response_payload_bytes: u64,
    ) -> T28AlignedProviderAttemptV2 {
        let (prefix, suffix) = match role {
            T28AlignedObjectRoleV2::IndexedRow => ("control", "indexed.okv"),
            T28AlignedObjectRoleV2::Payload => ("root/candidate", "payload.okv2"),
            T28AlignedObjectRoleV2::Projection => ("root/candidate", "projection.okp2"),
        };
        T28AlignedProviderAttemptV2 {
            api: "get".to_owned(),
            object_role: role,
            object_key: format!("{prefix}/{suffix}"),
            requested_range: 0..response_payload_bytes,
            returned_range: 0..response_payload_bytes,
            expected_generation: "8".to_owned(),
            returned_generation: "8".to_owned(),
            response_payload_bytes,
            started_monotonic_nanos,
            elapsed_nanos,
            result: "ok".to_owned(),
        }
    }

    fn scan_receipt(
        subject: T28TypedScanSubjectV1,
        trace_seed: u64,
        process_id: u32,
    ) -> T28TypedScanPositionReceiptV1 {
        let candidate = subject == T28TypedScanSubjectV1::C5v2AlignedColumnar;
        let mut receipt = T28TypedScanPositionReceiptV1 {
            schema_version: 1,
            execution_plan_sha256:
                "2e04d69775f67cb7561b59374d27bf2082909ca2df23a72f40e209728131c797".to_owned(),
            fixture_id: "1".repeat(64),
            root_sha256: "2".repeat(64),
            subject,
            trace_seed,
            query: "select id, quantity from t".to_owned(),
            configured_range_fetch_concurrency: 1,
            observed_peak_range_fetch_concurrency: 1,
            resident_metadata_bytes: 100,
            rows: 1_000,
            ordered_projection_sha256: "3".repeat(64),
            quantity_sum: "1000".to_owned(),
            query_elapsed_nanos: if candidate { 10 } else { 100 },
            rows_per_second: if candidate { 100_000.0 } else { 10_000.0 },
            provider_attempts: if candidate { 7 } else { 203 },
            response_bytes: if candidate { 100 } else { 1_000 },
            full_object_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            missing_expected_generation_requests: 0,
            returned_generation_mismatches: 0,
            provider_errors: 0,
            source_scan_plans: 1,
            source_projection_pushdown_plans: u64::from(candidate),
            source_stripes: 1,
            source_batches: 1,
            source_rows: 1_000,
            peak_arrow_batch_rows: 128,
            peak_arrow_batch_bytes: 1_024,
            projection_fetch_requests: if candidate { 7 } else { 0 },
            peak_fetch_bytes: 1_024,
            opaque_payload_requests: 0,
            opaque_payload_response_bytes: 0,
            correctness_anomalies: 0,
            process_id,
            measured_started_unix_nanos: u64::from(process_id) * 10,
            measured_finished_unix_nanos: u64::from(process_id) * 10 + 1,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_sha256().expect("scan digest");
        receipt.validate().expect("scan receipt");
        receipt
    }

    fn media_observation() -> T28AlignedMediaObservationV1 {
        let candidate_locator = candidate_locator("10");
        let object = |role, length| TypedLayoutObjectIdentityV1 {
            role,
            key: format!("{role:?}.object"),
            generation: "8".to_owned(),
            length,
            sha256: "4".repeat(64),
        };
        let mut media = T28AlignedMediaObservationV1 {
            fixture_id: "1".repeat(64),
            root_sha256: "2".repeat(64),
            canonical_history_sha256: "5".repeat(64),
            candidate_placement_envelope_sha256: candidate_locator.envelope_sha256,
            source_root_sha256: "8".repeat(64),
            source_placement_envelope_sha256: "9".repeat(64),
            control_prefix: "control".to_owned(),
            candidate_prefix: "root/candidate".to_owned(),
            control_closure_sha256: "6".repeat(64),
            candidate_closure_sha256: "7".repeat(64),
            control_total_media_bytes: 13_125_073,
            candidate_total_media_bytes: 13_695_766,
            control_objects: vec![TypedLayoutObjectIdentityV1 {
                key: "indexed.okv".to_owned(),
                ..object(TypedLayoutObjectRoleV1::Data, 13_125_073)
            }],
            candidate_objects: vec![
                TypedLayoutObjectIdentityV1 {
                    key: "projection.okp2".to_owned(),
                    ..object(TypedLayoutObjectRoleV1::Projection, 1_701_414)
                },
                TypedLayoutObjectIdentityV1 {
                    key: "payload.okv2".to_owned(),
                    ..object(TypedLayoutObjectRoleV1::Payload, 11_994_352)
                },
            ],
            source_c0_reused_by_reference: true,
            observation_sha256: String::new(),
        };
        let mut unsigned = media.clone();
        unsigned.observation_sha256.clear();
        media.observation_sha256 = content_sha256(&serde_json::to_vec(&unsigned).expect("media"));
        media.validate().expect("media observation");
        media
    }

    fn candidate_locator(root_generation: &str) -> TypedLayoutPlacementLocatorV1 {
        TypedLayoutPlacementLocatorV1::seal(
            "1".repeat(64),
            "2".repeat(64),
            "doss-objectkv-dev".to_owned(),
            "doss-objectkv-dev-okv-evals".to_owned(),
            "us-central1".to_owned(),
            "root".to_owned(),
            "root/root.json".to_owned(),
            root_generation.to_owned(),
            100,
            "b".repeat(64),
        )
        .expect("candidate locator")
    }
}
