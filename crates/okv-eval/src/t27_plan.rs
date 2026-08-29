//! Immutable T27 fresh-process execution plans.

use crate::object_fixture::{
    derive_fixture_expected_identity, open_existing_fixture_at_revision, FixturePlacementLocatorV1,
    ObjectFixtureProfile,
};
use crate::serving_recovery_openraft::{canonical_hot_trace_sha256, OpenRaftHotReadAccessPattern};
use crate::telemetry::TelemetryFlushReport;
use chrono::DateTime;
use okv_object::{gcs_backend_from_env, prefixed_backend, RevisionToken};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use uuid::Uuid;

const PLAN_SCHEMA_VERSION: u32 = 1;
const RECEIPT_SCHEMA_VERSION: u32 = 1;
const PLAN_MAGIC: &[u8] = b"OKVT27P1";
const OPTIONS_MAGIC: &[u8] = b"OKVT27O1";
const RECEIPT_MAGIC: &[u8] = b"OKVT27R1";
const RUN_RECEIPT_MAGIC: &[u8] = b"OKVT27C1";
const STRATUM_RECEIPT_MAGIC: &[u8] = b"OKVT27S1";
const WORKLOAD_MAGIC: &[u8] = b"OKVT27W1";
const INCARNATION_RECEIPT_MAGIC: &[u8] = b"OKVT27I1";
const POISON_RECEIPT_MAGIC: &[u8] = b"OKVT27X1";
const POSITION_POISON_RECEIPT_MAGIC: &[u8] = b"OKVT27Y1";
const PLAN_POSITIONS_REJECTION: &str =
    "T27 execution plan positions differ from the frozen contract";
const HIDDEN_PROVIDER_REJECTION: &str = "T27 direct position opened a hidden runtime provider";
const FIXTURE_SEED: u64 = 4_244;
const FIXTURE_BASE_VERSION: u64 = 2;
const VALUE_BYTES: u64 = 1_024;
const TARGET_BLOCK_BYTES: u64 = 65_536;
const TARGET_OBJECT_BYTES: u64 = 8_388_608;
const PREVIEW_KEY_COUNT: u64 = 65_536;
const ADMISSION_KEY_COUNT: u64 = 1_048_576;
const ADMISSION_TRACE_SEEDS: [u64; 3] = [1_103, 2_207, 3_301];

/// Frozen execution size for the T27 plan contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27PlanProfileV1 {
    Preflight64Mib,
    Admission1Gib,
}

impl T27PlanProfileV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::Preflight64Mib => 1,
            Self::Admission1Gib => 2,
        }
    }
}

/// Subject owned by one fresh T27 process position.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27PlanSubjectV1 {
    NativeSnapshot,
    DirectOwnedRocksdb,
}

impl T27PlanSubjectV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::NativeSnapshot => 1,
            Self::DirectOwnedRocksdb => 2,
        }
    }
}

/// Frozen skew treatment for one T27 stratum.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27AccessPatternV1 {
    Zipf0_8,
    Zipf1_4,
    Zipf2_0,
}

impl T27AccessPatternV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::Zipf0_8 => 1,
            Self::Zipf1_4 => 2,
            Self::Zipf2_0 => 3,
        }
    }

    const fn id(self) -> &'static str {
        match self {
            Self::Zipf0_8 => "z08",
            Self::Zipf1_4 => "z14",
            Self::Zipf2_0 => "z20",
        }
    }
}

/// Runtime and machine identity frozen before any T27 position executes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27ExecutionEnvelopeV1 {
    pub runtime_source_sha256: String,
    pub runtime_executable_sha256: String,
    pub runtime_cargo_lock_sha256: String,
    pub machine_receipt_sha256: String,
    pub machine_instance_id: String,
    pub infrastructure_lease_expires_epoch: u64,
    pub linux_boot_id: String,
    pub scratch_root: String,
    pub scratch_mount_point: String,
    pub scratch_mount_source: String,
    pub scratch_filesystem_type: String,
    pub scratch_major_minor: String,
    pub scratch_device_number: u64,
    pub scratch_filesystem_uuid: String,
    pub scratch_block_device: String,
    pub host_lease_path: String,
}

impl T27ExecutionEnvelopeV1 {
    /// Return the digest of the exact runtime, machine, boot, device, and lease identity.
    #[must_use]
    pub fn calculated_execution_sha256(&self) -> String {
        let mut bytes = Vec::new();
        encode_execution_envelope(&mut bytes, self);
        sha256(&bytes)
    }

    /// Validate the exact executable, host, boot, and scratch-device identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any required identity is malformed or absent.
    pub fn validate(&self) -> Result<(), String> {
        for digest in [
            &self.runtime_source_sha256,
            &self.runtime_executable_sha256,
            &self.runtime_cargo_lock_sha256,
            &self.machine_receipt_sha256,
        ] {
            if !valid_sha256(digest) {
                return Err("T27 execution envelope contains an invalid digest".to_owned());
            }
        }
        Uuid::parse_str(&self.linux_boot_id)
            .map_err(|_| "T27 execution envelope boot ID is invalid".to_owned())?;
        if self.machine_instance_id.trim().is_empty()
            || self.infrastructure_lease_expires_epoch == 0
            || self.scratch_device_number == 0
            || self.scratch_filesystem_uuid.trim().is_empty()
            || self.scratch_mount_source.trim().is_empty()
            || self.scratch_filesystem_type.trim().is_empty()
            || self.scratch_major_minor.trim().is_empty()
            || !Path::new(&self.scratch_root).is_absolute()
            || !Path::new(&self.scratch_mount_point).is_absolute()
            || !Path::new(&self.scratch_block_device).is_absolute()
            || !Path::new(&self.host_lease_path).is_absolute()
        {
            return Err("T27 execution envelope machine or device identity is invalid".to_owned());
        }
        Ok(())
    }
}

/// Semantic oracle frozen into the plan before measured processes start.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27ExpectedIdentityV1 {
    pub tail_sha256: String,
    pub resident_logical_sha256: String,
    pub trace_sha256_by_stratum: BTreeMap<String, String>,
}

impl T27ExpectedIdentityV1 {
    fn validate(&self, positions: &[T27PlanPositionV1]) -> Result<(), String> {
        if !valid_sha256(&self.tail_sha256)
            || !valid_sha256(&self.resident_logical_sha256)
            || self
                .trace_sha256_by_stratum
                .values()
                .any(|digest| !valid_sha256(digest))
        {
            return Err("T27 semantic oracle contains an invalid digest".to_owned());
        }
        let expected_strata = positions
            .iter()
            .map(|position| position.stratum_id.clone())
            .collect::<BTreeSet<_>>();
        if self
            .trace_sha256_by_stratum
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_strata
        {
            return Err("T27 semantic oracle trace strata differ from the plan".to_owned());
        }
        Ok(())
    }
}

/// Measurements extracted from one fresh T27 process invocation.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct T27PositionObservationV1 {
    pub execution: T27ExecutionEnvelopeV1,
    pub measured_worker_process_id: u32,
    pub measured_worker_linux_boot_id: String,
    pub measured_worker_start_ticks: u64,
    pub fixture_id: String,
    pub image_provider: String,
    pub runtime_resident_provider: Option<String>,
    pub runtime_serving_image_provider: Option<String>,
    pub tail_sha256: String,
    pub resident_logical_sha256: String,
    pub report_semantic_sha256: String,
    pub trace_sha256: String,
    pub subject: T27PlanSubjectV1,
    pub effective_engine_options_sha256: String,
    pub engine_topology: String,
    pub database_count: u64,
    pub block_cache_count: u64,
    pub implicit_block_cache_count: u64,
    pub column_family_count: u64,
    pub metadata_cache_disabled: bool,
    pub direct_reads: bool,
    pub block_cache_capacity_bytes: u64,
    pub block_cache_usage_bytes: u64,
    pub block_cache_misses: u64,
    pub operations_per_second: f64,
    pub latency_ns_p99: u64,
    pub cpu_nanoseconds_per_read: f64,
    pub physical_read_bytes: u64,
    pub read_amplification_ratio: f64,
    pub flush_write_bytes: u64,
    pub compaction_read_bytes: u64,
    pub compaction_write_bytes: u64,
    pub correctness_failures: u64,
    pub object_requests: u64,
    pub scratch_was_empty: bool,
    pub process_cpu_supported: bool,
    pub linux_proc_supported: bool,
    pub raw_report_sha256: String,
}

/// One process position in an immutable T27 ABBA plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27PlanPositionV1 {
    pub ordinal: u64,
    pub stratum_id: String,
    pub block: u64,
    pub position_in_block: u8,
    pub subject: T27PlanSubjectV1,
    pub trace_seed: u64,
    pub access_pattern: T27AccessPatternV1,
    pub block_cache_bytes: u64,
    pub max_local_bytes: u64,
    pub warmup_operations: u64,
    pub measured_operations: u64,
    pub concurrent_clients: u64,
    pub direct_reads: bool,
    pub treatment_sha256: String,
    pub expected_engine_options_sha256: String,
}

/// Exact fixture and position schedule consumed by the T27 controller.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27ExecutionPlanV1 {
    pub schema_version: u32,
    pub profile: T27PlanProfileV1,
    pub fixture: FixturePlacementLocatorV1,
    pub execution: T27ExecutionEnvelopeV1,
    pub expected: T27ExpectedIdentityV1,
    pub positions: Vec<T27PlanPositionV1>,
    pub plan_sha256: String,
}

/// Authenticated proof that one frozen workload was rebound to replacement infrastructure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27ExecutionIncarnationReceiptV1 {
    pub schema_version: u32,
    pub source_plan_sha256: String,
    pub incarnated_plan_sha256: String,
    pub workload_sha256: String,
    pub source_execution_sha256: String,
    pub incarnated_execution_sha256: String,
    pub runtime_source_sha256: String,
    pub runtime_executable_sha256: String,
    pub runtime_cargo_lock_sha256: String,
    pub source_machine_instance_id: String,
    pub incarnated_machine_instance_id: String,
    pub source_linux_boot_id: String,
    pub incarnated_linux_boot_id: String,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T27ExecutionIncarnationReceiptV1 {
    /// Return the digest of the canonical execution-incarnation receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(INCARNATION_RECEIPT_MAGIC);
        bytes.extend_from_slice(&self.schema_version.to_be_bytes());
        push_string(&mut bytes, &self.source_plan_sha256);
        push_string(&mut bytes, &self.incarnated_plan_sha256);
        push_string(&mut bytes, &self.workload_sha256);
        push_string(&mut bytes, &self.source_execution_sha256);
        push_string(&mut bytes, &self.incarnated_execution_sha256);
        push_string(&mut bytes, &self.runtime_source_sha256);
        push_string(&mut bytes, &self.runtime_executable_sha256);
        push_string(&mut bytes, &self.runtime_cargo_lock_sha256);
        push_string(&mut bytes, &self.source_machine_instance_id);
        push_string(&mut bytes, &self.incarnated_machine_instance_id);
        push_string(&mut bytes, &self.source_linux_boot_id);
        push_string(&mut bytes, &self.incarnated_linux_boot_id);
        bytes.push(u8::from(self.passed));
        sha256(&bytes)
    }

    /// Validate that only the execution envelope changed and the runtime stayed exact.
    ///
    /// # Errors
    ///
    /// Returns an error for changed workload intent, runtime drift, a no-op machine
    /// replacement, or an altered receipt.
    pub fn validate(
        &self,
        source: &T27ExecutionPlanV1,
        incarnated: &T27ExecutionPlanV1,
    ) -> Result<(), String> {
        source.validate()?;
        incarnated.validate()?;
        let source_workload = source.calculated_workload_sha256();
        let incarnated_workload = incarnated.calculated_workload_sha256();
        let workload_equal = source.profile == incarnated.profile
            && source.fixture == incarnated.fixture
            && source.expected == incarnated.expected
            && source.positions == incarnated.positions
            && source_workload == incarnated_workload;
        let runtime_equal = source.execution.runtime_source_sha256
            == incarnated.execution.runtime_source_sha256
            && source.execution.runtime_executable_sha256
                == incarnated.execution.runtime_executable_sha256
            && source.execution.runtime_cargo_lock_sha256
                == incarnated.execution.runtime_cargo_lock_sha256;
        if !workload_equal {
            return Err("T27 execution incarnation changed frozen workload intent".to_owned());
        }
        if !runtime_equal {
            return Err("T27 execution incarnation changed runtime identity".to_owned());
        }
        if source.execution.machine_instance_id == incarnated.execution.machine_instance_id {
            return Err("T27 execution incarnation did not bind a replacement machine".to_owned());
        }
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || self.source_plan_sha256 != source.plan_sha256
            || self.incarnated_plan_sha256 != incarnated.plan_sha256
            || self.workload_sha256 != source_workload
            || self.source_execution_sha256 != source.execution.calculated_execution_sha256()
            || self.incarnated_execution_sha256
                != incarnated.execution.calculated_execution_sha256()
            || self.source_execution_sha256 == self.incarnated_execution_sha256
            || self.runtime_source_sha256 != source.execution.runtime_source_sha256
            || self.runtime_executable_sha256 != source.execution.runtime_executable_sha256
            || self.runtime_cargo_lock_sha256 != source.execution.runtime_cargo_lock_sha256
            || self.source_machine_instance_id != source.execution.machine_instance_id
            || self.incarnated_machine_instance_id != incarnated.execution.machine_instance_id
            || self.source_linux_boot_id != source.execution.linux_boot_id
            || self.incarnated_linux_boot_id != incarnated.execution.linux_boot_id
            || !self.passed
            || self.receipt_sha256 != self.calculated_receipt_sha256()
        {
            return Err("T27 execution incarnation receipt identity mismatch".to_owned());
        }
        Ok(())
    }
}

/// One controlled corruption of an otherwise authenticated T27 plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27PlanPoisonV1 {
    AabbSchedule,
    MissingPosition,
    OptionMismatch,
}

impl T27PlanPoisonV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::AabbSchedule => 1,
            Self::MissingPosition => 2,
            Self::OptionMismatch => 3,
        }
    }
}

impl std::str::FromStr for T27PlanPoisonV1 {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "aabb_schedule" => Ok(Self::AabbSchedule),
            "missing_position" => Ok(Self::MissingPosition),
            "option_mismatch" => Ok(Self::OptionMismatch),
            other => Err(format!("unknown T27 plan poison {other}")),
        }
    }
}

/// Sealed evidence that one exact poisoned plan failed closed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27PlanPoisonReceiptV1 {
    pub schema_version: u32,
    pub source_plan_sha256: String,
    pub poison: T27PlanPoisonV1,
    pub poisoned_plan_sha256: String,
    pub poisoned_plan_file_sha256: String,
    pub expected_rejection: String,
    pub observed_rejection: String,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T27PlanPoisonReceiptV1 {
    /// Return the digest of the canonical poison-receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(POISON_RECEIPT_MAGIC);
        bytes.extend_from_slice(&self.schema_version.to_be_bytes());
        push_string(&mut bytes, &self.source_plan_sha256);
        bytes.push(self.poison.tag());
        push_string(&mut bytes, &self.poisoned_plan_sha256);
        push_string(&mut bytes, &self.poisoned_plan_file_sha256);
        push_string(&mut bytes, &self.expected_rejection);
        push_string(&mut bytes, &self.observed_rejection);
        bytes.push(u8::from(self.passed));
        sha256(&bytes)
    }

    /// Reconstruct and validate the controlled corruption and its rejection.
    ///
    /// # Errors
    ///
    /// Returns an error if the source, poison bytes, rejection, or receipt was
    /// altered.
    pub fn validate(
        &self,
        source: &T27ExecutionPlanV1,
        poisoned_plan_bytes: &[u8],
    ) -> Result<(), String> {
        source.validate()?;
        let expected_poison = build_poisoned_t27_plan(source, self.poison)?;
        let expected_bytes =
            serde_json::to_vec_pretty(&expected_poison).map_err(|error| error.to_string())?;
        let observed_rejection =
            match decode_t27_execution_plan(poisoned_plan_bytes, &expected_poison.plan_sha256) {
                Ok(_) => return Err("T27 poisoned execution plan was accepted".to_owned()),
                Err(error) => error,
            };
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || self.source_plan_sha256 != source.plan_sha256
            || poisoned_plan_bytes != expected_bytes
            || self.poisoned_plan_sha256 != expected_poison.plan_sha256
            || self.poisoned_plan_file_sha256 != sha256(poisoned_plan_bytes)
            || self.expected_rejection != PLAN_POSITIONS_REJECTION
            || self.observed_rejection != observed_rejection
            || self.observed_rejection != self.expected_rejection
            || !self.passed
            || self.receipt_sha256 != self.calculated_receipt_sha256()
        {
            return Err("T27 plan poison receipt identity mismatch".to_owned());
        }
        Ok(())
    }
}

/// One controlled corruption of an authenticated T27 position receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27PositionPoisonV1 {
    HiddenNativeProvider,
}

impl T27PositionPoisonV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::HiddenNativeProvider => 1,
        }
    }
}

impl std::str::FromStr for T27PositionPoisonV1 {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "hidden_native_provider" => Ok(Self::HiddenNativeProvider),
            other => Err(format!("unknown T27 position poison {other}")),
        }
    }
}

/// Sealed evidence that one exact poisoned position receipt failed closed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27PositionPoisonReceiptV1 {
    pub schema_version: u32,
    pub source_plan_sha256: String,
    pub source_position_receipt_sha256: String,
    pub poison: T27PositionPoisonV1,
    pub poisoned_position_receipt_sha256: String,
    pub poisoned_position_receipt_file_sha256: String,
    pub expected_rejection: String,
    pub observed_rejection: String,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T27PositionPoisonReceiptV1 {
    /// Return the digest of the canonical position-poison receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(POSITION_POISON_RECEIPT_MAGIC);
        bytes.extend_from_slice(&self.schema_version.to_be_bytes());
        push_string(&mut bytes, &self.source_plan_sha256);
        push_string(&mut bytes, &self.source_position_receipt_sha256);
        bytes.push(self.poison.tag());
        push_string(&mut bytes, &self.poisoned_position_receipt_sha256);
        push_string(&mut bytes, &self.poisoned_position_receipt_file_sha256);
        push_string(&mut bytes, &self.expected_rejection);
        push_string(&mut bytes, &self.observed_rejection);
        bytes.push(u8::from(self.passed));
        sha256(&bytes)
    }

    /// Reconstruct and validate the controlled position-receipt corruption.
    ///
    /// # Errors
    ///
    /// Returns an error if the plan, source receipt, poison bytes, rejection,
    /// or receipt was altered.
    pub fn validate(
        &self,
        plan: &T27ExecutionPlanV1,
        source: &T27PositionReceiptV1,
        poisoned_receipt_bytes: &[u8],
    ) -> Result<(), String> {
        plan.validate()?;
        source.validate(plan)?;
        let expected_poison = build_poisoned_t27_position_receipt(plan, source, self.poison)?;
        let expected_bytes =
            serde_json::to_vec_pretty(&expected_poison).map_err(|error| error.to_string())?;
        let observed_rejection = match expected_poison.validate(plan) {
            Ok(()) => return Err("T27 poisoned position receipt was accepted".to_owned()),
            Err(error) => error,
        };
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || self.source_plan_sha256 != plan.plan_sha256
            || self.source_position_receipt_sha256 != source.receipt_sha256
            || poisoned_receipt_bytes != expected_bytes
            || self.poisoned_position_receipt_sha256 != expected_poison.receipt_sha256
            || self.poisoned_position_receipt_file_sha256 != sha256(poisoned_receipt_bytes)
            || self.expected_rejection != HIDDEN_PROVIDER_REJECTION
            || self.observed_rejection != observed_rejection
            || self.observed_rejection != self.expected_rejection
            || !self.passed
            || self.receipt_sha256 != self.calculated_receipt_sha256()
        {
            return Err("T27 position poison receipt identity mismatch".to_owned());
        }
        Ok(())
    }
}

/// Immutable evidence produced by one fresh T27 process position.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct T27PositionReceiptV1 {
    pub schema_version: u32,
    pub controller_id: String,
    pub lease_id: String,
    pub worker_id: String,
    pub wrapper_process_id: u32,
    pub measured_worker_process_id: u32,
    pub measured_worker_linux_boot_id: String,
    pub measured_worker_start_ticks: u64,
    pub plan_sha256: String,
    pub execution: T27ExecutionEnvelopeV1,
    pub position: T27PlanPositionV1,
    pub fixture_envelope_sha256: String,
    pub fixture_id: String,
    pub image_provider: String,
    pub runtime_resident_provider: Option<String>,
    pub runtime_serving_image_provider: Option<String>,
    pub started_at: String,
    pub finished_at: String,
    pub tail_sha256: String,
    pub resident_logical_sha256: String,
    pub report_semantic_sha256: String,
    pub trace_sha256: String,
    pub observed_subject: T27PlanSubjectV1,
    pub observed_treatment_sha256: String,
    pub effective_engine_options_sha256: String,
    pub engine_topology: String,
    pub database_count: u64,
    pub block_cache_count: u64,
    pub implicit_block_cache_count: u64,
    pub column_family_count: u64,
    pub metadata_cache_disabled: bool,
    pub direct_reads: bool,
    pub block_cache_capacity_bytes: u64,
    pub block_cache_usage_bytes: u64,
    pub block_cache_misses: u64,
    pub operations_per_second: f64,
    pub latency_ns_p99: u64,
    pub cpu_nanoseconds_per_read: f64,
    pub physical_read_bytes: u64,
    pub read_amplification_ratio: f64,
    pub flush_write_bytes: u64,
    pub compaction_read_bytes: u64,
    pub compaction_write_bytes: u64,
    pub correctness_failures: u64,
    pub object_requests: u64,
    pub scratch_was_empty: bool,
    pub process_cpu_supported: bool,
    pub linux_proc_supported: bool,
    pub raw_report_sha256: String,
    pub receipt_sha256: String,
}

impl T27PositionReceiptV1 {
    /// Build and seal one process-position receipt.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: &T27ExecutionPlanV1,
        position: &T27PlanPositionV1,
        controller_id: String,
        lease_id: String,
        worker_id: String,
        wrapper_process_id: u32,
        started_at: String,
        finished_at: String,
        observed: T27PositionObservationV1,
    ) -> Self {
        let observed_treatment_sha256 = treatment_sha256(
            position.access_pattern,
            observed.block_cache_capacity_bytes,
            position.max_local_bytes,
            position.warmup_operations,
            position.measured_operations,
            position.concurrent_clients,
            observed.direct_reads,
        );
        let mut receipt = Self {
            schema_version: RECEIPT_SCHEMA_VERSION,
            controller_id,
            lease_id,
            worker_id,
            wrapper_process_id,
            measured_worker_process_id: observed.measured_worker_process_id,
            measured_worker_linux_boot_id: observed.measured_worker_linux_boot_id,
            measured_worker_start_ticks: observed.measured_worker_start_ticks,
            plan_sha256: plan.plan_sha256.clone(),
            execution: observed.execution,
            position: position.clone(),
            fixture_envelope_sha256: plan.fixture.envelope_sha256.clone(),
            fixture_id: observed.fixture_id,
            image_provider: observed.image_provider,
            runtime_resident_provider: observed.runtime_resident_provider,
            runtime_serving_image_provider: observed.runtime_serving_image_provider,
            started_at,
            finished_at,
            tail_sha256: observed.tail_sha256,
            resident_logical_sha256: observed.resident_logical_sha256,
            report_semantic_sha256: observed.report_semantic_sha256,
            trace_sha256: observed.trace_sha256,
            observed_subject: observed.subject,
            observed_treatment_sha256,
            effective_engine_options_sha256: observed.effective_engine_options_sha256,
            engine_topology: observed.engine_topology,
            database_count: observed.database_count,
            block_cache_count: observed.block_cache_count,
            implicit_block_cache_count: observed.implicit_block_cache_count,
            column_family_count: observed.column_family_count,
            metadata_cache_disabled: observed.metadata_cache_disabled,
            direct_reads: observed.direct_reads,
            block_cache_capacity_bytes: observed.block_cache_capacity_bytes,
            block_cache_usage_bytes: observed.block_cache_usage_bytes,
            block_cache_misses: observed.block_cache_misses,
            operations_per_second: observed.operations_per_second,
            latency_ns_p99: observed.latency_ns_p99,
            cpu_nanoseconds_per_read: observed.cpu_nanoseconds_per_read,
            physical_read_bytes: observed.physical_read_bytes,
            read_amplification_ratio: observed.read_amplification_ratio,
            flush_write_bytes: observed.flush_write_bytes,
            compaction_read_bytes: observed.compaction_read_bytes,
            compaction_write_bytes: observed.compaction_write_bytes,
            correctness_failures: observed.correctness_failures,
            object_requests: observed.object_requests,
            scratch_was_empty: observed.scratch_was_empty,
            process_cpu_supported: observed.process_cpu_supported,
            linux_proc_supported: observed.linux_proc_supported,
            raw_report_sha256: observed.raw_report_sha256,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        receipt
    }

    /// Return the digest of the canonical receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        sha256(&encode_position_receipt_identity(self))
    }

    /// Validate this receipt against one exact immutable plan position.
    ///
    /// # Errors
    ///
    /// Returns an error for any identity, ordering, process, measurement, or
    /// hot-read gate mismatch.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self, plan: &T27ExecutionPlanV1) -> Result<(), String> {
        plan.validate()?;
        let ordinal = usize::try_from(self.position.ordinal)
            .map_err(|_| "T27 receipt ordinal exceeds usize".to_owned())?;
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || plan.positions.get(ordinal) != Some(&self.position)
            || self.plan_sha256 != plan.plan_sha256
            || self.execution != plan.execution
            || self.fixture_envelope_sha256 != plan.fixture.envelope_sha256
            || self.fixture_id != plan.fixture.fixture.fixture_id
        {
            return Err("T27 position receipt identity mismatch".to_owned());
        }
        Uuid::parse_str(&self.controller_id)
            .map_err(|_| "T27 controller ID is invalid".to_owned())?;
        Uuid::parse_str(&self.lease_id).map_err(|_| "T27 lease ID is invalid".to_owned())?;
        Uuid::parse_str(&self.worker_id).map_err(|_| "T27 worker ID is invalid".to_owned())?;
        if self.wrapper_process_id == 0
            || self.measured_worker_process_id == 0
            || self.wrapper_process_id == self.measured_worker_process_id
            || self.measured_worker_start_ticks == 0
            || self.measured_worker_linux_boot_id != plan.execution.linux_boot_id
        {
            return Err("T27 position process identity is invalid".to_owned());
        }
        let started = DateTime::parse_from_rfc3339(&self.started_at)
            .map_err(|_| "T27 position start timestamp is invalid".to_owned())?;
        let finished = DateTime::parse_from_rfc3339(&self.finished_at)
            .map_err(|_| "T27 position finish timestamp is invalid".to_owned())?;
        if finished < started {
            return Err("T27 position finished before it started".to_owned());
        }
        if self.observed_subject != self.position.subject
            || self.observed_treatment_sha256 != self.position.treatment_sha256
            || self.effective_engine_options_sha256 != self.position.expected_engine_options_sha256
            || self.database_count != 1
            || self.block_cache_count != 1
            || self.implicit_block_cache_count != 0
            || self.column_family_count == 0
            || self.engine_topology.trim().is_empty()
            || !self.direct_reads
            || self.block_cache_capacity_bytes != self.position.block_cache_bytes
        {
            return Err("T27 position effective options mismatch".to_owned());
        }
        match self.position.subject {
            T27PlanSubjectV1::NativeSnapshot => {
                if self.runtime_resident_provider.as_deref() != Some(&self.image_provider)
                    || self.runtime_serving_image_provider.is_some()
                    || self.column_family_count != 3
                    || !self.metadata_cache_disabled
                    || self.engine_topology != "native-resident:1db:1cache:3cf"
                {
                    return Err("T27 native position process inventory mismatch".to_owned());
                }
            }
            T27PlanSubjectV1::DirectOwnedRocksdb => {
                if self.runtime_resident_provider.is_some()
                    || self.runtime_serving_image_provider.is_some()
                    || !self.image_provider.contains("direct-owned")
                    || self.column_family_count != 1
                    || self.metadata_cache_disabled
                    || self.engine_topology != "direct-owned:1db:1cache:1cf"
                {
                    return Err("T27 direct position opened a hidden runtime provider".to_owned());
                }
            }
        }
        let maximum_cache_usage = self
            .position
            .block_cache_bytes
            .saturating_mul(105)
            .saturating_div(100);
        if self.block_cache_usage_bytes > maximum_cache_usage {
            return Err("T27 position exceeded its block-cache budget".to_owned());
        }
        if !self.operations_per_second.is_finite()
            || self.operations_per_second <= 0.0
            || self.latency_ns_p99 == 0
            || !self.cpu_nanoseconds_per_read.is_finite()
            || self.cpu_nanoseconds_per_read <= 0.0
            || !self.read_amplification_ratio.is_finite()
            || self.read_amplification_ratio < 0.0
            || self.flush_write_bytes != 0
            || self.compaction_read_bytes != 0
            || self.compaction_write_bytes != 0
            || self.correctness_failures != 0
            || self.object_requests != 0
            || !self.scratch_was_empty
            || !self.process_cpu_supported
            || !self.linux_proc_supported
        {
            return Err("T27 position measurement gates failed".to_owned());
        }
        for digest in [
            &self.tail_sha256,
            &self.resident_logical_sha256,
            &self.report_semantic_sha256,
            &self.trace_sha256,
            &self.effective_engine_options_sha256,
            &self.raw_report_sha256,
            &self.receipt_sha256,
        ] {
            if !valid_sha256(digest) {
                return Err("T27 position receipt contains an invalid digest".to_owned());
            }
        }
        if self.tail_sha256 != plan.expected.tail_sha256
            || self.resident_logical_sha256 != plan.expected.resident_logical_sha256
            || plan
                .expected
                .trace_sha256_by_stratum
                .get(&self.position.stratum_id)
                != Some(&self.trace_sha256)
        {
            return Err("T27 position differs from the frozen semantic oracle".to_owned());
        }
        if self.receipt_sha256 != self.calculated_receipt_sha256() {
            return Err("T27 position receipt digest mismatch".to_owned());
        }
        Ok(())
    }
}

/// Position order used for one drift-controlled native/control comparison.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T27ComparisonOrderV1 {
    Ab,
    Ba,
}

/// Performance gates for one stratum and one side of its ABBA blocks.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27StratumComparisonV1 {
    pub stratum_id: String,
    pub order: T27ComparisonOrderV1,
    pub sample_pairs: u64,
    pub native_throughput_ratio: f64,
    pub native_p99_ratio: f64,
    pub native_cpu_per_read_ratio: f64,
    pub native_block_cache_misses_per_read: f64,
    pub control_block_cache_misses_per_read: f64,
    pub native_physical_bytes_per_read: f64,
    pub control_physical_bytes_per_read: f64,
    pub native_physical_bytes_per_read_ratio: f64,
    pub native_read_amplification_ratio: f64,
    pub pressure_passed: bool,
    pub passed: bool,
}

/// Immutable completion evidence for one complete resumable T27 stratum.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27StratumRunReceiptV1 {
    pub schema_version: u32,
    pub controller_id: String,
    pub lease_id: String,
    pub lease_acquired_at: String,
    pub lease_released_at: String,
    pub plan_sha256: String,
    pub workload_sha256: String,
    pub execution_sha256: String,
    pub runtime_source_sha256: String,
    pub runtime_executable_sha256: String,
    pub runtime_cargo_lock_sha256: String,
    pub stratum_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub position_ordinals: Vec<u64>,
    pub position_receipt_sha256s: Vec<String>,
    pub comparisons: Vec<T27StratumComparisonV1>,
    pub telemetry_run_id: String,
    pub telemetry_endpoint_sha256: String,
    pub telemetry_required_signals: Vec<String>,
    pub telemetry_emitted_positions: u64,
    pub telemetry_metrics_flush_succeeded: bool,
    pub telemetry_traces_flush_succeeded: bool,
    pub telemetry_logs_flush_succeeded: bool,
    pub telemetry_metrics_shutdown_succeeded: bool,
    pub telemetry_traces_shutdown_succeeded: bool,
    pub telemetry_logs_shutdown_succeeded: bool,
    pub passed: bool,
    pub receipt_sha256: String,
}

/// Immutable completion evidence for one sequential fresh-process plan run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T27PlanRunReceiptV1 {
    pub schema_version: u32,
    pub controller_id: String,
    pub lease_id: String,
    pub lease_acquired_at: String,
    pub lease_released_at: String,
    pub plan_sha256: String,
    pub started_at: String,
    pub finished_at: String,
    pub position_receipt_sha256s: Vec<String>,
    pub comparisons: Vec<T27StratumComparisonV1>,
    pub telemetry_run_id: String,
    pub telemetry_endpoint_sha256: String,
    pub telemetry_required_signals: Vec<String>,
    pub telemetry_emitted_positions: u64,
    pub telemetry_metrics_flush_succeeded: bool,
    pub telemetry_traces_flush_succeeded: bool,
    pub telemetry_logs_flush_succeeded: bool,
    pub telemetry_metrics_shutdown_succeeded: bool,
    pub telemetry_traces_shutdown_succeeded: bool,
    pub telemetry_logs_shutdown_succeeded: bool,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T27StratumRunReceiptV1 {
    /// Build and validate one complete stratum receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when receipts do not cover one exact planned stratum,
    /// process identities overlap, comparisons are incomplete, or telemetry is
    /// not bound to the run.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: &T27ExecutionPlanV1,
        stratum_id: String,
        receipts: &[T27PositionReceiptV1],
        started_at: String,
        finished_at: String,
        lease_id: String,
        lease_acquired_at: String,
        lease_released_at: String,
        telemetry_endpoint_sha256: String,
        telemetry_flush: TelemetryFlushReport,
    ) -> Result<Self, String> {
        validate_t27_stratum_position_receipts(plan, &stratum_id, receipts)?;
        if !valid_sha256(&telemetry_endpoint_sha256) {
            return Err("T27 stratum telemetry endpoint digest is invalid".to_owned());
        }
        let comparisons = build_t27_comparisons(receipts)?;
        if comparisons.len() != 2 {
            return Err("T27 stratum did not produce both AB and BA comparisons".to_owned());
        }
        let controller_id = receipts
            .first()
            .map(|value| value.controller_id.clone())
            .ok_or_else(|| "T27 stratum run has no position receipts".to_owned())?;
        let passed = comparisons.iter().all(|comparison| comparison.passed)
            && telemetry_flush.all_succeeded();
        let mut receipt = Self {
            schema_version: RECEIPT_SCHEMA_VERSION,
            controller_id: controller_id.clone(),
            lease_id,
            lease_acquired_at,
            lease_released_at,
            plan_sha256: plan.plan_sha256.clone(),
            workload_sha256: plan.calculated_workload_sha256(),
            execution_sha256: plan.execution.calculated_execution_sha256(),
            runtime_source_sha256: plan.execution.runtime_source_sha256.clone(),
            runtime_executable_sha256: plan.execution.runtime_executable_sha256.clone(),
            runtime_cargo_lock_sha256: plan.execution.runtime_cargo_lock_sha256.clone(),
            stratum_id,
            started_at,
            finished_at,
            position_ordinals: receipts
                .iter()
                .map(|value| value.position.ordinal)
                .collect(),
            position_receipt_sha256s: receipts
                .iter()
                .map(|value| value.receipt_sha256.clone())
                .collect(),
            comparisons,
            telemetry_run_id: controller_id,
            telemetry_endpoint_sha256,
            telemetry_required_signals: vec![
                "logs".to_owned(),
                "metrics".to_owned(),
                "traces".to_owned(),
            ],
            telemetry_emitted_positions: u64::try_from(receipts.len()).unwrap_or(u64::MAX),
            telemetry_metrics_flush_succeeded: telemetry_flush.metrics_flush_succeeded,
            telemetry_traces_flush_succeeded: telemetry_flush.traces_flush_succeeded,
            telemetry_logs_flush_succeeded: telemetry_flush.logs_flush_succeeded,
            telemetry_metrics_shutdown_succeeded: telemetry_flush.metrics_shutdown_succeeded,
            telemetry_traces_shutdown_succeeded: telemetry_flush.traces_shutdown_succeeded,
            telemetry_logs_shutdown_succeeded: telemetry_flush.logs_shutdown_succeeded,
            passed,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        receipt.validate(plan, receipts)?;
        Ok(receipt)
    }

    /// Return the digest of the canonical stratum receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(STRATUM_RECEIPT_MAGIC);
        bytes.extend_from_slice(&self.schema_version.to_be_bytes());
        for value in [
            &self.controller_id,
            &self.lease_id,
            &self.lease_acquired_at,
            &self.lease_released_at,
            &self.plan_sha256,
            &self.workload_sha256,
            &self.execution_sha256,
            &self.runtime_source_sha256,
            &self.runtime_executable_sha256,
            &self.runtime_cargo_lock_sha256,
            &self.stratum_id,
            &self.started_at,
            &self.finished_at,
        ] {
            push_string(&mut bytes, value);
        }
        bytes.extend_from_slice(
            &u64::try_from(self.position_ordinals.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for ordinal in &self.position_ordinals {
            bytes.extend_from_slice(&ordinal.to_be_bytes());
        }
        bytes.extend_from_slice(
            &u64::try_from(self.position_receipt_sha256s.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for digest in &self.position_receipt_sha256s {
            push_string(&mut bytes, digest);
        }
        bytes.extend_from_slice(
            &u64::try_from(self.comparisons.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for comparison in &self.comparisons {
            encode_t27_comparison(&mut bytes, comparison);
        }
        push_string(&mut bytes, &self.telemetry_run_id);
        push_string(&mut bytes, &self.telemetry_endpoint_sha256);
        bytes.extend_from_slice(
            &u64::try_from(self.telemetry_required_signals.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for signal in &self.telemetry_required_signals {
            push_string(&mut bytes, signal);
        }
        bytes.extend_from_slice(&self.telemetry_emitted_positions.to_be_bytes());
        bytes.push(u8::from(self.telemetry_metrics_flush_succeeded));
        bytes.push(u8::from(self.telemetry_traces_flush_succeeded));
        bytes.push(u8::from(self.telemetry_logs_flush_succeeded));
        bytes.push(u8::from(self.telemetry_metrics_shutdown_succeeded));
        bytes.push(u8::from(self.telemetry_traces_shutdown_succeeded));
        bytes.push(u8::from(self.telemetry_logs_shutdown_succeeded));
        bytes.push(u8::from(self.passed));
        sha256(&bytes)
    }

    /// Validate one stratum completion receipt and all referenced positions.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete, reordered, overlapping, or tampered
    /// evidence.
    pub fn validate(
        &self,
        plan: &T27ExecutionPlanV1,
        receipts: &[T27PositionReceiptV1],
    ) -> Result<(), String> {
        validate_t27_stratum_position_receipts(plan, &self.stratum_id, receipts)?;
        let started = DateTime::parse_from_rfc3339(&self.started_at)
            .map_err(|_| "T27 stratum start timestamp is invalid".to_owned())?;
        let finished = DateTime::parse_from_rfc3339(&self.finished_at)
            .map_err(|_| "T27 stratum finish timestamp is invalid".to_owned())?;
        let lease_acquired = DateTime::parse_from_rfc3339(&self.lease_acquired_at)
            .map_err(|_| "T27 stratum lease acquisition timestamp is invalid".to_owned())?;
        let lease_released = DateTime::parse_from_rfc3339(&self.lease_released_at)
            .map_err(|_| "T27 stratum lease release timestamp is invalid".to_owned())?;
        Uuid::parse_str(&self.lease_id)
            .map_err(|_| "T27 stratum lease ID is invalid".to_owned())?;
        let first = receipts
            .first()
            .ok_or_else(|| "T27 stratum has no first position".to_owned())?;
        let last = receipts
            .last()
            .ok_or_else(|| "T27 stratum has no last position".to_owned())?;
        let first_started = DateTime::parse_from_rfc3339(&first.started_at)
            .map_err(|_| "T27 stratum first position timestamp is invalid".to_owned())?;
        let last_finished = DateTime::parse_from_rfc3339(&last.finished_at)
            .map_err(|_| "T27 stratum last position timestamp is invalid".to_owned())?;
        let comparisons = build_t27_comparisons(receipts)?;
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || self.controller_id != first.controller_id
            || self.lease_id != first.lease_id
            || self.plan_sha256 != plan.plan_sha256
            || self.workload_sha256 != plan.calculated_workload_sha256()
            || self.execution_sha256 != plan.execution.calculated_execution_sha256()
            || self.runtime_source_sha256 != plan.execution.runtime_source_sha256
            || self.runtime_executable_sha256 != plan.execution.runtime_executable_sha256
            || self.runtime_cargo_lock_sha256 != plan.execution.runtime_cargo_lock_sha256
            || self.position_ordinals
                != receipts
                    .iter()
                    .map(|value| value.position.ordinal)
                    .collect::<Vec<_>>()
            || self.position_receipt_sha256s
                != receipts
                    .iter()
                    .map(|value| value.receipt_sha256.clone())
                    .collect::<Vec<_>>()
            || self.comparisons != comparisons
            || self.comparisons.len() != 2
            || self.telemetry_run_id != self.controller_id
            || !valid_sha256(&self.telemetry_endpoint_sha256)
            || self.telemetry_required_signals != ["logs", "metrics", "traces"]
            || self.telemetry_emitted_positions != u64::try_from(receipts.len()).unwrap_or(u64::MAX)
            || self.passed
                != (self.comparisons.iter().all(|comparison| comparison.passed)
                    && self.telemetry_export_succeeded())
            || finished < started
            || lease_released < lease_acquired
            || lease_released < finished
            || lease_acquired > first_started
            || lease_released < last_finished
            || first_started < started
            || last_finished > finished
            || self.receipt_sha256 != self.calculated_receipt_sha256()
        {
            return Err("T27 stratum run receipt identity mismatch".to_owned());
        }
        Ok(())
    }

    #[must_use]
    pub const fn telemetry_export_succeeded(&self) -> bool {
        self.telemetry_metrics_flush_succeeded
            && self.telemetry_traces_flush_succeeded
            && self.telemetry_logs_flush_succeeded
            && self.telemetry_metrics_shutdown_succeeded
            && self.telemetry_traces_shutdown_succeeded
            && self.telemetry_logs_shutdown_succeeded
    }
}

impl T27PlanRunReceiptV1 {
    /// Build and validate one plan-completion receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied receipts do not cover the plan
    /// exactly once, sequentially, and in separate worker processes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: &T27ExecutionPlanV1,
        receipts: &[T27PositionReceiptV1],
        started_at: String,
        finished_at: String,
        lease_id: String,
        lease_acquired_at: String,
        lease_released_at: String,
        telemetry_endpoint_sha256: String,
        telemetry_flush: TelemetryFlushReport,
    ) -> Result<Self, String> {
        validate_t27_position_receipts(plan, receipts)?;
        if !valid_sha256(&telemetry_endpoint_sha256) {
            return Err("T27 telemetry endpoint digest is invalid".to_owned());
        }
        let comparisons = build_t27_comparisons(receipts)?;
        let passed = comparisons.iter().all(|comparison| comparison.passed)
            && telemetry_flush.all_succeeded();
        let controller_id = receipts
            .first()
            .map(|value| value.controller_id.clone())
            .ok_or_else(|| "T27 plan run has no position receipts".to_owned())?;
        let mut receipt = Self {
            schema_version: RECEIPT_SCHEMA_VERSION,
            controller_id: controller_id.clone(),
            lease_id,
            lease_acquired_at,
            lease_released_at,
            plan_sha256: plan.plan_sha256.clone(),
            started_at,
            finished_at,
            position_receipt_sha256s: receipts
                .iter()
                .map(|value| value.receipt_sha256.clone())
                .collect(),
            comparisons,
            telemetry_run_id: controller_id,
            telemetry_endpoint_sha256,
            telemetry_required_signals: vec![
                "logs".to_owned(),
                "metrics".to_owned(),
                "traces".to_owned(),
            ],
            telemetry_emitted_positions: u64::try_from(receipts.len()).unwrap_or(u64::MAX),
            telemetry_metrics_flush_succeeded: telemetry_flush.metrics_flush_succeeded,
            telemetry_traces_flush_succeeded: telemetry_flush.traces_flush_succeeded,
            telemetry_logs_flush_succeeded: telemetry_flush.logs_flush_succeeded,
            telemetry_metrics_shutdown_succeeded: telemetry_flush.metrics_shutdown_succeeded,
            telemetry_traces_shutdown_succeeded: telemetry_flush.traces_shutdown_succeeded,
            telemetry_logs_shutdown_succeeded: telemetry_flush.logs_shutdown_succeeded,
            passed,
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        receipt.validate(plan, receipts)?;
        Ok(receipt)
    }

    /// Return the digest of the canonical run-receipt fields.
    #[must_use]
    pub fn calculated_receipt_sha256(&self) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(RUN_RECEIPT_MAGIC);
        bytes.extend_from_slice(&self.schema_version.to_be_bytes());
        push_string(&mut bytes, &self.controller_id);
        push_string(&mut bytes, &self.lease_id);
        push_string(&mut bytes, &self.lease_acquired_at);
        push_string(&mut bytes, &self.lease_released_at);
        push_string(&mut bytes, &self.plan_sha256);
        push_string(&mut bytes, &self.started_at);
        push_string(&mut bytes, &self.finished_at);
        bytes.extend_from_slice(
            &u64::try_from(self.position_receipt_sha256s.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for digest in &self.position_receipt_sha256s {
            push_string(&mut bytes, digest);
        }
        bytes.extend_from_slice(
            &u64::try_from(self.comparisons.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for comparison in &self.comparisons {
            encode_t27_comparison(&mut bytes, comparison);
        }
        push_string(&mut bytes, &self.telemetry_run_id);
        push_string(&mut bytes, &self.telemetry_endpoint_sha256);
        bytes.extend_from_slice(
            &u64::try_from(self.telemetry_required_signals.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for signal in &self.telemetry_required_signals {
            push_string(&mut bytes, signal);
        }
        bytes.extend_from_slice(&self.telemetry_emitted_positions.to_be_bytes());
        bytes.push(u8::from(self.telemetry_metrics_flush_succeeded));
        bytes.push(u8::from(self.telemetry_traces_flush_succeeded));
        bytes.push(u8::from(self.telemetry_logs_flush_succeeded));
        bytes.push(u8::from(self.telemetry_metrics_shutdown_succeeded));
        bytes.push(u8::from(self.telemetry_traces_shutdown_succeeded));
        bytes.push(u8::from(self.telemetry_logs_shutdown_succeeded));
        bytes.push(u8::from(self.passed));
        sha256(&bytes)
    }

    /// Validate one completion receipt and all of its referenced positions.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete, reordered, overlapping, or tampered
    /// evidence.
    pub fn validate(
        &self,
        plan: &T27ExecutionPlanV1,
        receipts: &[T27PositionReceiptV1],
    ) -> Result<(), String> {
        validate_t27_position_receipts(plan, receipts)?;
        let started = DateTime::parse_from_rfc3339(&self.started_at)
            .map_err(|_| "T27 plan start timestamp is invalid".to_owned())?;
        let finished = DateTime::parse_from_rfc3339(&self.finished_at)
            .map_err(|_| "T27 plan finish timestamp is invalid".to_owned())?;
        let lease_acquired = DateTime::parse_from_rfc3339(&self.lease_acquired_at)
            .map_err(|_| "T27 lease acquisition timestamp is invalid".to_owned())?;
        let lease_released = DateTime::parse_from_rfc3339(&self.lease_released_at)
            .map_err(|_| "T27 lease release timestamp is invalid".to_owned())?;
        Uuid::parse_str(&self.lease_id).map_err(|_| "T27 lease ID is invalid".to_owned())?;
        let first_position_started = DateTime::parse_from_rfc3339(&receipts[0].started_at)
            .map_err(|_| "T27 first position start timestamp is invalid".to_owned())?;
        let last_position_finished = DateTime::parse_from_rfc3339(
            &receipts
                .last()
                .ok_or_else(|| "T27 plan run has no position receipts".to_owned())?
                .finished_at,
        )
        .map_err(|_| "T27 last position finish timestamp is invalid".to_owned())?;
        let comparisons = build_t27_comparisons(receipts)?;
        if self.schema_version != RECEIPT_SCHEMA_VERSION
            || self.plan_sha256 != plan.plan_sha256
            || self.controller_id != receipts[0].controller_id
            || self.lease_id != receipts[0].lease_id
            || self.position_receipt_sha256s
                != receipts
                    .iter()
                    .map(|value| value.receipt_sha256.clone())
                    .collect::<Vec<_>>()
            || self.comparisons != comparisons
            || self.telemetry_run_id != self.controller_id
            || !valid_sha256(&self.telemetry_endpoint_sha256)
            || self.telemetry_required_signals != ["logs", "metrics", "traces"]
            || self.telemetry_emitted_positions != u64::try_from(receipts.len()).unwrap_or(u64::MAX)
            || self.passed
                != (self.comparisons.iter().all(|comparison| comparison.passed)
                    && self.telemetry_export_succeeded())
            || finished < started
            || lease_released < lease_acquired
            || lease_acquired > first_position_started
            || lease_released < last_position_finished
            || first_position_started < started
            || last_position_finished > finished
            || self.receipt_sha256 != self.calculated_receipt_sha256()
        {
            return Err("T27 plan run receipt identity mismatch".to_owned());
        }
        Ok(())
    }

    #[must_use]
    pub const fn telemetry_export_succeeded(&self) -> bool {
        self.telemetry_metrics_flush_succeeded
            && self.telemetry_traces_flush_succeeded
            && self.telemetry_logs_flush_succeeded
            && self.telemetry_metrics_shutdown_succeeded
            && self.telemetry_traces_shutdown_succeeded
            && self.telemetry_logs_shutdown_succeeded
    }
}

/// Require one nonoverlapping fresh-process receipt for every plan position.
///
/// # Errors
///
/// Returns an error for missing, duplicated, reordered, overlapping, or
/// cross-controller evidence.
pub fn validate_t27_position_receipts(
    plan: &T27ExecutionPlanV1,
    receipts: &[T27PositionReceiptV1],
) -> Result<(), String> {
    plan.validate()?;
    if receipts.len() != plan.positions.len() || receipts.is_empty() {
        return Err("T27 plan receipt count mismatch".to_owned());
    }
    let controller_id = &receipts[0].controller_id;
    let lease_id = &receipts[0].lease_id;
    let mut worker_ids = BTreeSet::new();
    let mut wrapper_process_ids = BTreeSet::new();
    let mut measured_process_ids = BTreeSet::new();
    let mut receipt_digests = BTreeSet::new();
    for (expected, receipt) in plan.positions.iter().zip(receipts) {
        receipt.validate(plan)?;
        if receipt.position != *expected
            || receipt.controller_id != *controller_id
            || receipt.lease_id != *lease_id
        {
            return Err("T27 plan receipts are reordered or cross-controller".to_owned());
        }
        if !worker_ids.insert(receipt.worker_id.clone())
            || !wrapper_process_ids.insert(receipt.wrapper_process_id)
            || !measured_process_ids.insert((
                receipt.measured_worker_linux_boot_id.clone(),
                receipt.measured_worker_process_id,
                receipt.measured_worker_start_ticks,
            ))
            || !receipt_digests.insert(receipt.receipt_sha256.clone())
        {
            return Err("T27 plan reused a worker process or receipt".to_owned());
        }
    }
    for pair in receipts.windows(2) {
        let previous_finished = DateTime::parse_from_rfc3339(&pair[0].finished_at)
            .map_err(|_| "T27 position finish timestamp is invalid".to_owned())?;
        let next_started = DateTime::parse_from_rfc3339(&pair[1].started_at)
            .map_err(|_| "T27 position start timestamp is invalid".to_owned())?;
        if next_started < previous_finished {
            return Err("T27 plan positions overlap".to_owned());
        }
    }
    Ok(())
}

/// Require one nonoverlapping fresh-process receipt for every position in one
/// exact planned stratum.
///
/// # Errors
///
/// Returns an error for an unknown stratum, missing, duplicated, reordered,
/// overlapping, or cross-controller evidence.
pub fn validate_t27_stratum_position_receipts(
    plan: &T27ExecutionPlanV1,
    stratum_id: &str,
    receipts: &[T27PositionReceiptV1],
) -> Result<(), String> {
    plan.validate()?;
    let expected = plan
        .positions
        .iter()
        .filter(|position| position.stratum_id == stratum_id)
        .collect::<Vec<_>>();
    if expected.is_empty() || receipts.len() != expected.len() {
        return Err("T27 stratum receipt count mismatch".to_owned());
    }
    let controller_id = &receipts[0].controller_id;
    let lease_id = &receipts[0].lease_id;
    let mut worker_ids = BTreeSet::new();
    let mut wrapper_process_ids = BTreeSet::new();
    let mut measured_process_ids = BTreeSet::new();
    let mut receipt_digests = BTreeSet::new();
    for (expected, receipt) in expected.into_iter().zip(receipts) {
        receipt.validate(plan)?;
        if receipt.position != *expected
            || receipt.controller_id != *controller_id
            || receipt.lease_id != *lease_id
        {
            return Err("T27 stratum receipts are reordered or cross-controller".to_owned());
        }
        if !worker_ids.insert(receipt.worker_id.clone())
            || !wrapper_process_ids.insert(receipt.wrapper_process_id)
            || !measured_process_ids.insert((
                receipt.measured_worker_linux_boot_id.clone(),
                receipt.measured_worker_process_id,
                receipt.measured_worker_start_ticks,
            ))
            || !receipt_digests.insert(receipt.receipt_sha256.clone())
        {
            return Err("T27 stratum reused a worker process or receipt".to_owned());
        }
    }
    for pair in receipts.windows(2) {
        let previous_finished = DateTime::parse_from_rfc3339(&pair[0].finished_at)
            .map_err(|_| "T27 stratum position finish timestamp is invalid".to_owned())?;
        let next_started = DateTime::parse_from_rfc3339(&pair[1].started_at)
            .map_err(|_| "T27 stratum position start timestamp is invalid".to_owned())?;
        if next_started < previous_finished {
            return Err("T27 stratum positions overlap".to_owned());
        }
    }
    Ok(())
}

fn encode_t27_comparison(bytes: &mut Vec<u8>, comparison: &T27StratumComparisonV1) {
    push_string(bytes, &comparison.stratum_id);
    bytes.push(match comparison.order {
        T27ComparisonOrderV1::Ab => 1,
        T27ComparisonOrderV1::Ba => 2,
    });
    bytes.extend_from_slice(&comparison.sample_pairs.to_be_bytes());
    for value in [
        comparison.native_throughput_ratio,
        comparison.native_p99_ratio,
        comparison.native_cpu_per_read_ratio,
        comparison.native_block_cache_misses_per_read,
        comparison.control_block_cache_misses_per_read,
        comparison.native_physical_bytes_per_read,
        comparison.control_physical_bytes_per_read,
        comparison.native_physical_bytes_per_read_ratio,
        comparison.native_read_amplification_ratio,
    ] {
        bytes.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    bytes.push(u8::from(comparison.pressure_passed));
    bytes.push(u8::from(comparison.passed));
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn build_t27_comparisons(
    receipts: &[T27PositionReceiptV1],
) -> Result<Vec<T27StratumComparisonV1>, String> {
    let mut by_stratum = BTreeMap::<String, Vec<&T27PositionReceiptV1>>::new();
    for receipt in receipts {
        by_stratum
            .entry(receipt.position.stratum_id.clone())
            .or_default()
            .push(receipt);
    }
    let mut comparisons = Vec::with_capacity(by_stratum.len().saturating_mul(2));
    for (stratum_id, stratum) in by_stratum {
        if stratum.len() % 4 != 0 {
            return Err("T27 stratum does not contain complete ABBA blocks".to_owned());
        }
        for (order, native_position, control_position) in [
            (T27ComparisonOrderV1::Ab, 0_usize, 1_usize),
            (T27ComparisonOrderV1::Ba, 3_usize, 2_usize),
        ] {
            let pairs = stratum
                .chunks_exact(4)
                .map(|block| (block[native_position], block[control_position]))
                .collect::<Vec<_>>();
            let native_throughput = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| native.operations_per_second)
                    .collect::<Vec<_>>(),
            );
            let control_throughput = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| control.operations_per_second)
                    .collect::<Vec<_>>(),
            );
            let native_p99 = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| native.latency_ns_p99 as f64)
                    .collect::<Vec<_>>(),
            );
            let control_p99 = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| control.latency_ns_p99 as f64)
                    .collect::<Vec<_>>(),
            );
            let native_cpu = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| native.cpu_nanoseconds_per_read)
                    .collect::<Vec<_>>(),
            );
            let control_cpu = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| control.cpu_nanoseconds_per_read)
                    .collect::<Vec<_>>(),
            );
            let native_physical = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| physical_bytes_per_read(native))
                    .collect::<Vec<_>>(),
            );
            let control_physical = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| physical_bytes_per_read(control))
                    .collect::<Vec<_>>(),
            );
            let native_read_amp = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| native.read_amplification_ratio)
                    .collect::<Vec<_>>(),
            );
            let control_read_amp = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| control.read_amplification_ratio)
                    .collect::<Vec<_>>(),
            );
            let native_cache_misses_per_read = median_f64(
                &pairs
                    .iter()
                    .map(|(native, _)| block_cache_misses_per_read(native))
                    .collect::<Vec<_>>(),
            );
            let control_cache_misses_per_read = median_f64(
                &pairs
                    .iter()
                    .map(|(_, control)| block_cache_misses_per_read(control))
                    .collect::<Vec<_>>(),
            );
            let native_throughput_ratio = bounded_ratio(native_throughput, control_throughput);
            let native_p99_ratio = bounded_ratio(native_p99, control_p99);
            let native_cpu_per_read_ratio = bounded_ratio(native_cpu, control_cpu);
            let native_physical_bytes_per_read_ratio =
                bounded_ratio(native_physical, control_physical);
            let native_read_amplification_ratio = bounded_ratio(native_read_amp, control_read_amp);
            let pressure_passed = (native_cache_misses_per_read > 0.0 || native_physical > 0.0)
                && (control_cache_misses_per_read > 0.0 || control_physical > 0.0);
            let passed = pressure_passed
                && native_throughput_ratio >= 0.80
                && native_p99_ratio <= 1.20
                && native_cpu_per_read_ratio <= 1.25
                && native_physical_bytes_per_read_ratio <= 1.25
                && native_read_amplification_ratio <= 1.25;
            comparisons.push(T27StratumComparisonV1 {
                stratum_id: stratum_id.clone(),
                order,
                sample_pairs: u64::try_from(pairs.len()).unwrap_or(u64::MAX),
                native_throughput_ratio,
                native_p99_ratio,
                native_cpu_per_read_ratio,
                native_block_cache_misses_per_read: native_cache_misses_per_read,
                control_block_cache_misses_per_read: control_cache_misses_per_read,
                native_physical_bytes_per_read: native_physical,
                control_physical_bytes_per_read: control_physical,
                native_physical_bytes_per_read_ratio,
                native_read_amplification_ratio,
                pressure_passed,
                passed,
            });
        }
    }
    Ok(comparisons)
}

fn median_f64(values: &[f64]) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

#[allow(clippy::cast_precision_loss)]
fn physical_bytes_per_read(receipt: &T27PositionReceiptV1) -> f64 {
    receipt.physical_read_bytes as f64 / receipt.position.measured_operations as f64
}

#[allow(clippy::cast_precision_loss)]
fn block_cache_misses_per_read(receipt: &T27PositionReceiptV1) -> f64 {
    receipt.block_cache_misses as f64 / receipt.position.measured_operations as f64
}

fn bounded_ratio(candidate: f64, control: f64) -> f64 {
    if control == 0.0 {
        if candidate == 0.0 {
            1.0
        } else {
            f64::MAX
        }
    } else {
        candidate / control
    }
}

impl T27ExecutionPlanV1 {
    /// Return the digest of the fixed-field plan encoding.
    #[must_use]
    pub fn calculated_plan_sha256(&self) -> String {
        sha256(&encode_plan_identity(self))
    }

    /// Return the portable workload digest with the ephemeral execution envelope excluded.
    #[must_use]
    pub fn calculated_workload_sha256(&self) -> String {
        sha256(&encode_workload_identity(self))
    }

    /// Validate the complete fixture and regenerate every expected position.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixture, profile, plan digest, position order,
    /// subject, stratum, or option identity differs from the frozen contract.
    pub fn validate(&self) -> Result<(), String> {
        self.fixture.validate()?;
        self.execution.validate()?;
        self.expected.validate(&self.positions)?;
        if self.schema_version != PLAN_SCHEMA_VERSION
            || self.fixture.fixture_seed != FIXTURE_SEED
            || !valid_sha256(&self.plan_sha256)
            || self.plan_sha256 != self.calculated_plan_sha256()
        {
            return Err("invalid T27 execution plan identity".to_owned());
        }
        let expected = build_plan_unchecked(
            &self.fixture,
            self.profile,
            self.execution.clone(),
            self.expected.clone(),
        )?;
        if self.positions != expected.positions {
            return Err(PLAN_POSITIONS_REJECTION.to_owned());
        }
        Ok(())
    }
}

/// Build the exact preflight or admission plan from one verified placement.
///
/// # Errors
///
/// Returns an error when the locator does not match the selected T27 fixture
/// shape or seed.
pub fn build_t27_execution_plan(
    fixture: &FixturePlacementLocatorV1,
    profile: T27PlanProfileV1,
    execution: T27ExecutionEnvelopeV1,
    expected: T27ExpectedIdentityV1,
) -> Result<T27ExecutionPlanV1, String> {
    fixture.validate()?;
    if fixture.fixture_seed != FIXTURE_SEED {
        return Err("T27 fixture seed must be 4244".to_owned());
    }
    let plan = build_plan_unchecked(fixture, profile, execution, expected)?;
    plan.validate()?;
    Ok(plan)
}

/// Bind one authenticated frozen workload to a replacement execution environment.
///
/// # Errors
///
/// Returns an error when workload intent or runtime identity changes, when the
/// replacement machine is unchanged, or when either plan is invalid.
pub fn build_t27_execution_incarnation(
    source: &T27ExecutionPlanV1,
    execution: T27ExecutionEnvelopeV1,
) -> Result<(T27ExecutionPlanV1, T27ExecutionIncarnationReceiptV1), String> {
    source.validate()?;
    execution.validate()?;
    let incarnated = build_plan_unchecked(
        &source.fixture,
        source.profile,
        execution,
        source.expected.clone(),
    )?;
    incarnated.validate()?;
    let mut receipt = T27ExecutionIncarnationReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        source_plan_sha256: source.plan_sha256.clone(),
        incarnated_plan_sha256: incarnated.plan_sha256.clone(),
        workload_sha256: source.calculated_workload_sha256(),
        source_execution_sha256: source.execution.calculated_execution_sha256(),
        incarnated_execution_sha256: incarnated.execution.calculated_execution_sha256(),
        runtime_source_sha256: source.execution.runtime_source_sha256.clone(),
        runtime_executable_sha256: source.execution.runtime_executable_sha256.clone(),
        runtime_cargo_lock_sha256: source.execution.runtime_cargo_lock_sha256.clone(),
        source_machine_instance_id: source.execution.machine_instance_id.clone(),
        incarnated_machine_instance_id: incarnated.execution.machine_instance_id.clone(),
        source_linux_boot_id: source.execution.linux_boot_id.clone(),
        incarnated_linux_boot_id: incarnated.execution.linux_boot_id.clone(),
        passed: true,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
    receipt.validate(source, &incarnated)?;
    Ok((incarnated, receipt))
}

/// Exact-open the persisted fixture and freeze its semantic oracle before any
/// T27 measured process starts.
///
/// # Errors
///
/// Returns an error when the generation-pinned closure, canonical tail, or
/// trace schedule cannot be derived exactly.
pub async fn derive_t27_expected_identity(
    fixture: &FixturePlacementLocatorV1,
    profile: T27PlanProfileV1,
    executable: &Path,
) -> Result<T27ExpectedIdentityV1, String> {
    fixture.validate()?;
    let positions = build_positions(fixture, profile)?;
    let backend = prefixed_backend(
        gcs_backend_from_env().map_err(|error| error.to_string())?,
        fixture.prefix.clone(),
    )
    .map_err(|error| error.to_string())?;
    let revision = RevisionToken {
        e_tag: None,
        version: Some(fixture.descriptor_generation.clone()),
    };
    let (_, records, _) = open_existing_fixture_at_revision(
        &backend,
        &fixture.fixture,
        fixture.base_version,
        Some(&revision),
    )
    .await?;
    let object_profile = ObjectFixtureProfile {
        key_count: fixture.key_count,
        value_bytes: usize::try_from(fixture.value_bytes)
            .map_err(|_| "T27 value byte count exceeds usize".to_owned())?,
        target_object_bytes: usize::try_from(fixture.target_object_bytes)
            .map_err(|_| "T27 object byte target exceeds usize".to_owned())?,
        target_block_bytes: usize::try_from(fixture.target_block_bytes)
            .map_err(|_| "T27 block byte target exceeds usize".to_owned())?,
    };
    let (tail_sha256, resident_logical_sha256) = derive_fixture_expected_identity(
        fixture.fixture_seed,
        &object_profile,
        fixture.base_version,
        &records,
        executable,
    )
    .await?;
    let mut trace_sha256_by_stratum = BTreeMap::new();
    for position in &positions {
        let access_pattern = match position.access_pattern {
            T27AccessPatternV1::Zipf0_8 => OpenRaftHotReadAccessPattern::Zipf0_8,
            T27AccessPatternV1::Zipf1_4 => OpenRaftHotReadAccessPattern::Zipf1_4,
            T27AccessPatternV1::Zipf2_0 => OpenRaftHotReadAccessPattern::Zipf2_0,
        };
        let digest = canonical_hot_trace_sha256(
            fixture.key_count,
            usize::try_from(position.warmup_operations.max(position.measured_operations))
                .map_err(|_| "T27 trace operation count exceeds usize".to_owned())?,
            position.trace_seed,
            access_pattern,
        );
        match trace_sha256_by_stratum.insert(position.stratum_id.clone(), digest.clone()) {
            Some(previous) if previous != digest => {
                return Err("T27 stratum generated inconsistent oracle traces".to_owned());
            }
            _ => {}
        }
    }
    let expected = T27ExpectedIdentityV1 {
        tail_sha256,
        resident_logical_sha256,
        trace_sha256_by_stratum,
    };
    expected.validate(&positions)?;
    Ok(expected)
}

/// Decode one plan and require its independently supplied plan digest.
///
/// # Errors
///
/// Returns an error for malformed JSON, an invalid plan, or an unexpected
/// plan digest.
pub fn decode_t27_execution_plan(
    bytes: &[u8],
    expected_plan_sha256: &str,
) -> Result<T27ExecutionPlanV1, String> {
    if !valid_sha256(expected_plan_sha256) {
        return Err("expected T27 plan SHA-256 is invalid".to_owned());
    }
    let plan: T27ExecutionPlanV1 =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    plan.validate()?;
    if plan.plan_sha256 != expected_plan_sha256 {
        return Err("T27 execution plan identity mismatch".to_owned());
    }
    Ok(plan)
}

/// Build and independently reject one exact controlled plan corruption.
///
/// The returned bytes are the complete poisoned plan artifact. The receipt
/// binds the valid source plan, the exact artifact bytes, and the production
/// decoder's rejection.
///
/// # Errors
///
/// Returns an error when the source plan is invalid, the poison cannot be
/// constructed, or the decoder does not reject the intended contract breach.
pub fn verify_t27_plan_poison(
    source: &T27ExecutionPlanV1,
    poison: T27PlanPoisonV1,
) -> Result<(Vec<u8>, T27PlanPoisonReceiptV1), String> {
    source.validate()?;
    let poisoned = build_poisoned_t27_plan(source, poison)?;
    let poisoned_plan_bytes =
        serde_json::to_vec_pretty(&poisoned).map_err(|error| error.to_string())?;
    let observed_rejection = decode_t27_execution_plan(&poisoned_plan_bytes, &poisoned.plan_sha256)
        .map_or_else(
            |error| error,
            |_| "T27 poisoned execution plan was accepted".to_owned(),
        );
    let expected_rejection = PLAN_POSITIONS_REJECTION.to_owned();
    if observed_rejection != expected_rejection {
        return Err(format!(
            "T27 plan poison reached the wrong rejection: {observed_rejection}"
        ));
    }
    let mut receipt = T27PlanPoisonReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        source_plan_sha256: source.plan_sha256.clone(),
        poison,
        poisoned_plan_sha256: poisoned.plan_sha256,
        poisoned_plan_file_sha256: sha256(&poisoned_plan_bytes),
        expected_rejection,
        observed_rejection,
        passed: true,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
    receipt.validate(source, &poisoned_plan_bytes)?;
    Ok((poisoned_plan_bytes, receipt))
}

/// Build and independently reject one exact controlled position-receipt
/// corruption.
///
/// # Errors
///
/// Returns an error when the plan or source receipt is invalid, the poison
/// cannot be constructed, or the validator does not reject the intended
/// runtime-inventory breach.
pub fn verify_t27_position_poison(
    plan: &T27ExecutionPlanV1,
    source: &T27PositionReceiptV1,
    poison: T27PositionPoisonV1,
) -> Result<(Vec<u8>, T27PositionPoisonReceiptV1), String> {
    plan.validate()?;
    source.validate(plan)?;
    let poisoned = build_poisoned_t27_position_receipt(plan, source, poison)?;
    let poisoned_receipt_bytes =
        serde_json::to_vec_pretty(&poisoned).map_err(|error| error.to_string())?;
    let observed_rejection = poisoned.validate(plan).map_or_else(
        |error| error,
        |()| "T27 poisoned position receipt was accepted".to_owned(),
    );
    let expected_rejection = HIDDEN_PROVIDER_REJECTION.to_owned();
    if observed_rejection != expected_rejection {
        return Err(format!(
            "T27 position poison reached the wrong rejection: {observed_rejection}"
        ));
    }
    let mut receipt = T27PositionPoisonReceiptV1 {
        schema_version: RECEIPT_SCHEMA_VERSION,
        source_plan_sha256: plan.plan_sha256.clone(),
        source_position_receipt_sha256: source.receipt_sha256.clone(),
        poison,
        poisoned_position_receipt_sha256: poisoned.receipt_sha256,
        poisoned_position_receipt_file_sha256: sha256(&poisoned_receipt_bytes),
        expected_rejection,
        observed_rejection,
        passed: true,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
    receipt.validate(plan, source, &poisoned_receipt_bytes)?;
    Ok((poisoned_receipt_bytes, receipt))
}

fn build_poisoned_t27_plan(
    source: &T27ExecutionPlanV1,
    poison: T27PlanPoisonV1,
) -> Result<T27ExecutionPlanV1, String> {
    let mut plan = source.clone();
    match poison {
        T27PlanPoisonV1::AabbSchedule => {
            let block = plan
                .positions
                .get_mut(0..4)
                .ok_or_else(|| "T27 AABB poison requires one complete block".to_owned())?;
            block[1].subject = T27PlanSubjectV1::NativeSnapshot;
            block[1].expected_engine_options_sha256 =
                crate::serving_recovery_openraft::rocksdb_effective_options_sha256(
                    true,
                    block[1].block_cache_bytes,
                    block[1].direct_reads,
                );
            block[3].subject = T27PlanSubjectV1::DirectOwnedRocksdb;
            block[3].expected_engine_options_sha256 =
                crate::serving_recovery_openraft::rocksdb_effective_options_sha256(
                    false,
                    block[3].block_cache_bytes,
                    block[3].direct_reads,
                );
        }
        T27PlanPoisonV1::MissingPosition => {
            plan.positions
                .pop()
                .ok_or_else(|| "T27 missing-position poison requires a position".to_owned())?;
        }
        T27PlanPoisonV1::OptionMismatch => {
            let position = plan
                .positions
                .first_mut()
                .ok_or_else(|| "T27 option poison requires a position".to_owned())?;
            position.block_cache_bytes = position.block_cache_bytes.saturating_add(1);
        }
    }
    plan.plan_sha256 = plan.calculated_plan_sha256();
    Ok(plan)
}

fn build_poisoned_t27_position_receipt(
    plan: &T27ExecutionPlanV1,
    source: &T27PositionReceiptV1,
    poison: T27PositionPoisonV1,
) -> Result<T27PositionReceiptV1, String> {
    source.validate(plan)?;
    if source.position.subject != T27PlanSubjectV1::DirectOwnedRocksdb {
        return Err("T27 hidden-provider poison requires a direct position".to_owned());
    }
    let mut receipt = source.clone();
    match poison {
        T27PositionPoisonV1::HiddenNativeProvider => {
            receipt.runtime_resident_provider = Some("poison://hidden-native-provider".to_owned());
        }
    }
    receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
    Ok(receipt)
}

fn build_plan_unchecked(
    fixture: &FixturePlacementLocatorV1,
    profile: T27PlanProfileV1,
    execution: T27ExecutionEnvelopeV1,
    expected: T27ExpectedIdentityV1,
) -> Result<T27ExecutionPlanV1, String> {
    let positions = build_positions(fixture, profile)?;
    let mut plan = T27ExecutionPlanV1 {
        schema_version: PLAN_SCHEMA_VERSION,
        profile,
        fixture: fixture.clone(),
        execution,
        expected,
        positions,
        plan_sha256: String::new(),
    };
    plan.plan_sha256 = plan.calculated_plan_sha256();
    Ok(plan)
}

fn build_positions(
    fixture: &FixturePlacementLocatorV1,
    profile: T27PlanProfileV1,
) -> Result<Vec<T27PlanPositionV1>, String> {
    validate_fixture_shape(fixture, profile)?;
    let max_local_bytes = fixture.logical_bytes.saturating_mul(4);
    let mut positions = Vec::new();
    match profile {
        T27PlanProfileV1::Preflight64Mib => append_stratum(
            &mut positions,
            "c20-z14-s1103",
            1,
            1_103,
            T27AccessPatternV1::Zipf1_4,
            fixture.logical_bytes / 5,
            max_local_bytes,
            256,
            1_024,
            8,
        ),
        T27PlanProfileV1::Admission1Gib => {
            let cache_treatments = [
                ("c50", 536_870_912_u64),
                ("c20", 214_748_364_u64),
                ("c05", 53_687_091_u64),
            ];
            let skews = [
                T27AccessPatternV1::Zipf0_8,
                T27AccessPatternV1::Zipf1_4,
                T27AccessPatternV1::Zipf2_0,
            ];
            for (cache_id, block_cache_bytes) in cache_treatments {
                for access_pattern in skews {
                    for trace_seed in ADMISSION_TRACE_SEEDS {
                        append_stratum(
                            &mut positions,
                            &format!("{cache_id}-{}-s{trace_seed}", access_pattern.id()),
                            5,
                            trace_seed,
                            access_pattern,
                            block_cache_bytes,
                            max_local_bytes,
                            200_000,
                            1_000_000,
                            8,
                        );
                    }
                }
            }
        }
    }
    Ok(positions)
}

#[allow(clippy::too_many_arguments)]
fn append_stratum(
    positions: &mut Vec<T27PlanPositionV1>,
    stratum_id: &str,
    blocks: u64,
    trace_seed: u64,
    access_pattern: T27AccessPatternV1,
    block_cache_bytes: u64,
    max_local_bytes: u64,
    warmup_operations: u64,
    measured_operations: u64,
    concurrent_clients: u64,
) {
    const ORDER: [T27PlanSubjectV1; 4] = [
        T27PlanSubjectV1::NativeSnapshot,
        T27PlanSubjectV1::DirectOwnedRocksdb,
        T27PlanSubjectV1::DirectOwnedRocksdb,
        T27PlanSubjectV1::NativeSnapshot,
    ];
    for block in 0..blocks {
        for (position_in_block, subject) in ORDER.into_iter().enumerate() {
            let treatment_sha256 = treatment_sha256(
                access_pattern,
                block_cache_bytes,
                max_local_bytes,
                warmup_operations,
                measured_operations,
                concurrent_clients,
                true,
            );
            positions.push(T27PlanPositionV1 {
                ordinal: u64::try_from(positions.len()).unwrap_or(u64::MAX),
                stratum_id: stratum_id.to_owned(),
                block,
                position_in_block: u8::try_from(position_in_block).unwrap_or(u8::MAX),
                subject,
                trace_seed,
                access_pattern,
                block_cache_bytes,
                max_local_bytes,
                warmup_operations,
                measured_operations,
                concurrent_clients,
                direct_reads: true,
                treatment_sha256,
                expected_engine_options_sha256:
                    crate::serving_recovery_openraft::rocksdb_effective_options_sha256(
                        subject == T27PlanSubjectV1::NativeSnapshot,
                        block_cache_bytes,
                        true,
                    ),
            });
        }
    }
}

fn validate_fixture_shape(
    fixture: &FixturePlacementLocatorV1,
    profile: T27PlanProfileV1,
) -> Result<(), String> {
    let expected_keys = match profile {
        T27PlanProfileV1::Preflight64Mib => PREVIEW_KEY_COUNT,
        T27PlanProfileV1::Admission1Gib => ADMISSION_KEY_COUNT,
    };
    if fixture.base_version != FIXTURE_BASE_VERSION {
        return Err("fixture placement has the wrong T27 empty-anchor version".to_owned());
    }
    if fixture.key_count != expected_keys
        || fixture.value_bytes != VALUE_BYTES
        || fixture.logical_bytes != expected_keys.saturating_mul(VALUE_BYTES)
        || fixture.target_object_bytes != TARGET_OBJECT_BYTES
        || fixture.target_block_bytes != TARGET_BLOCK_BYTES
    {
        return Err("fixture placement differs from the selected T27 plan profile".to_owned());
    }
    Ok(())
}

fn treatment_sha256(
    access_pattern: T27AccessPatternV1,
    block_cache_bytes: u64,
    max_local_bytes: u64,
    warmup_operations: u64,
    measured_operations: u64,
    concurrent_clients: u64,
    direct_reads: bool,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(OPTIONS_MAGIC);
    bytes.push(access_pattern.tag());
    bytes.extend_from_slice(&block_cache_bytes.to_be_bytes());
    bytes.extend_from_slice(&max_local_bytes.to_be_bytes());
    bytes.extend_from_slice(&warmup_operations.to_be_bytes());
    bytes.extend_from_slice(&measured_operations.to_be_bytes());
    bytes.extend_from_slice(&concurrent_clients.to_be_bytes());
    bytes.push(u8::from(direct_reads));
    sha256(&bytes)
}

fn encode_plan_identity(plan: &T27ExecutionPlanV1) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PLAN_MAGIC);
    encode_plan_workload_prefix(&mut bytes, plan);
    encode_execution_envelope(&mut bytes, &plan.execution);
    encode_plan_workload_suffix(&mut bytes, plan);
    bytes
}

fn encode_workload_identity(plan: &T27ExecutionPlanV1) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(WORKLOAD_MAGIC);
    encode_plan_workload_prefix(&mut bytes, plan);
    encode_plan_workload_suffix(&mut bytes, plan);
    bytes
}

fn encode_plan_workload_prefix(bytes: &mut Vec<u8>, plan: &T27ExecutionPlanV1) {
    bytes.extend_from_slice(&plan.schema_version.to_be_bytes());
    bytes.push(plan.profile.tag());
    push_string(bytes, &plan.fixture.envelope_sha256);
    push_string(bytes, &plan.fixture.fixture.fixture_id);
    bytes.extend_from_slice(&plan.fixture.fixture_seed.to_be_bytes());
}

fn encode_plan_workload_suffix(bytes: &mut Vec<u8>, plan: &T27ExecutionPlanV1) {
    push_string(bytes, &plan.expected.tail_sha256);
    push_string(bytes, &plan.expected.resident_logical_sha256);
    bytes.extend_from_slice(
        &u64::try_from(plan.expected.trace_sha256_by_stratum.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for (stratum, digest) in &plan.expected.trace_sha256_by_stratum {
        push_string(bytes, stratum);
        push_string(bytes, digest);
    }
    bytes.extend_from_slice(
        &u64::try_from(plan.positions.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for position in &plan.positions {
        bytes.extend_from_slice(&position.ordinal.to_be_bytes());
        push_string(bytes, &position.stratum_id);
        bytes.extend_from_slice(&position.block.to_be_bytes());
        bytes.push(position.position_in_block);
        bytes.push(position.subject.tag());
        bytes.extend_from_slice(&position.trace_seed.to_be_bytes());
        bytes.push(position.access_pattern.tag());
        bytes.extend_from_slice(&position.block_cache_bytes.to_be_bytes());
        bytes.extend_from_slice(&position.max_local_bytes.to_be_bytes());
        bytes.extend_from_slice(&position.warmup_operations.to_be_bytes());
        bytes.extend_from_slice(&position.measured_operations.to_be_bytes());
        bytes.extend_from_slice(&position.concurrent_clients.to_be_bytes());
        bytes.push(u8::from(position.direct_reads));
        push_string(bytes, &position.treatment_sha256);
        push_string(bytes, &position.expected_engine_options_sha256);
    }
}

fn encode_position_receipt_identity(receipt: &T27PositionReceiptV1) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECEIPT_MAGIC);
    bytes.extend_from_slice(&receipt.schema_version.to_be_bytes());
    push_string(&mut bytes, &receipt.controller_id);
    push_string(&mut bytes, &receipt.lease_id);
    push_string(&mut bytes, &receipt.worker_id);
    bytes.extend_from_slice(&receipt.wrapper_process_id.to_be_bytes());
    bytes.extend_from_slice(&receipt.measured_worker_process_id.to_be_bytes());
    push_string(&mut bytes, &receipt.measured_worker_linux_boot_id);
    bytes.extend_from_slice(&receipt.measured_worker_start_ticks.to_be_bytes());
    push_string(&mut bytes, &receipt.plan_sha256);
    encode_execution_envelope(&mut bytes, &receipt.execution);
    bytes.extend_from_slice(&receipt.position.ordinal.to_be_bytes());
    push_string(&mut bytes, &receipt.position.stratum_id);
    bytes.extend_from_slice(&receipt.position.block.to_be_bytes());
    bytes.push(receipt.position.position_in_block);
    bytes.push(receipt.position.subject.tag());
    bytes.extend_from_slice(&receipt.position.trace_seed.to_be_bytes());
    bytes.push(receipt.position.access_pattern.tag());
    bytes.extend_from_slice(&receipt.position.block_cache_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.position.max_local_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.position.warmup_operations.to_be_bytes());
    bytes.extend_from_slice(&receipt.position.measured_operations.to_be_bytes());
    bytes.extend_from_slice(&receipt.position.concurrent_clients.to_be_bytes());
    bytes.push(u8::from(receipt.position.direct_reads));
    push_string(&mut bytes, &receipt.position.treatment_sha256);
    push_string(&mut bytes, &receipt.position.expected_engine_options_sha256);
    push_string(&mut bytes, &receipt.fixture_envelope_sha256);
    push_string(&mut bytes, &receipt.fixture_id);
    push_string(&mut bytes, &receipt.image_provider);
    push_option_string(&mut bytes, receipt.runtime_resident_provider.as_deref());
    push_option_string(
        &mut bytes,
        receipt.runtime_serving_image_provider.as_deref(),
    );
    push_string(&mut bytes, &receipt.started_at);
    push_string(&mut bytes, &receipt.finished_at);
    push_string(&mut bytes, &receipt.tail_sha256);
    push_string(&mut bytes, &receipt.resident_logical_sha256);
    push_string(&mut bytes, &receipt.report_semantic_sha256);
    push_string(&mut bytes, &receipt.trace_sha256);
    bytes.push(receipt.observed_subject.tag());
    push_string(&mut bytes, &receipt.observed_treatment_sha256);
    push_string(&mut bytes, &receipt.effective_engine_options_sha256);
    push_string(&mut bytes, &receipt.engine_topology);
    bytes.extend_from_slice(&receipt.database_count.to_be_bytes());
    bytes.extend_from_slice(&receipt.block_cache_count.to_be_bytes());
    bytes.extend_from_slice(&receipt.implicit_block_cache_count.to_be_bytes());
    bytes.extend_from_slice(&receipt.column_family_count.to_be_bytes());
    bytes.push(u8::from(receipt.metadata_cache_disabled));
    bytes.push(u8::from(receipt.direct_reads));
    bytes.extend_from_slice(&receipt.block_cache_capacity_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.block_cache_usage_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.block_cache_misses.to_be_bytes());
    bytes.extend_from_slice(&receipt.operations_per_second.to_bits().to_be_bytes());
    bytes.extend_from_slice(&receipt.latency_ns_p99.to_be_bytes());
    bytes.extend_from_slice(&receipt.cpu_nanoseconds_per_read.to_bits().to_be_bytes());
    bytes.extend_from_slice(&receipt.physical_read_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.read_amplification_ratio.to_bits().to_be_bytes());
    bytes.extend_from_slice(&receipt.flush_write_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.compaction_read_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.compaction_write_bytes.to_be_bytes());
    bytes.extend_from_slice(&receipt.correctness_failures.to_be_bytes());
    bytes.extend_from_slice(&receipt.object_requests.to_be_bytes());
    bytes.push(u8::from(receipt.scratch_was_empty));
    bytes.push(u8::from(receipt.process_cpu_supported));
    bytes.push(u8::from(receipt.linux_proc_supported));
    push_string(&mut bytes, &receipt.raw_report_sha256);
    bytes
}

fn encode_execution_envelope(bytes: &mut Vec<u8>, envelope: &T27ExecutionEnvelopeV1) {
    push_string(bytes, &envelope.runtime_source_sha256);
    push_string(bytes, &envelope.runtime_executable_sha256);
    push_string(bytes, &envelope.runtime_cargo_lock_sha256);
    push_string(bytes, &envelope.machine_receipt_sha256);
    push_string(bytes, &envelope.machine_instance_id);
    bytes.extend_from_slice(&envelope.infrastructure_lease_expires_epoch.to_be_bytes());
    push_string(bytes, &envelope.linux_boot_id);
    push_string(bytes, &envelope.scratch_root);
    push_string(bytes, &envelope.scratch_mount_point);
    push_string(bytes, &envelope.scratch_mount_source);
    push_string(bytes, &envelope.scratch_filesystem_type);
    push_string(bytes, &envelope.scratch_major_minor);
    bytes.extend_from_slice(&envelope.scratch_device_number.to_be_bytes());
    push_string(bytes, &envelope.scratch_filesystem_uuid);
    push_string(bytes, &envelope.scratch_block_device);
    push_string(bytes, &envelope.host_lease_path);
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn push_option_string(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            bytes.push(1);
            push_string(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{
        build_positions, build_t27_execution_incarnation, build_t27_execution_plan,
        decode_t27_execution_plan, validate_t27_position_receipts,
        validate_t27_stratum_position_receipts, verify_t27_plan_poison, verify_t27_position_poison,
        T27ExecutionEnvelopeV1, T27ExecutionPlanV1, T27ExpectedIdentityV1, T27PlanPoisonV1,
        T27PlanProfileV1, T27PlanRunReceiptV1, T27PlanSubjectV1, T27PositionObservationV1,
        T27PositionPoisonV1, T27PositionReceiptV1, T27StratumRunReceiptV1,
    };
    use crate::object_fixture::{FixturePlacementLocatorV1, ObjectFixtureLocatorV1};
    use crate::telemetry::TelemetryFlushReport;

    #[test]
    fn admission_plan_freezes_all_540_abba_positions() {
        let fixture = locator(1_048_576);
        let plan = test_plan(&fixture, T27PlanProfileV1::Admission1Gib);

        assert_eq!(plan.positions.len(), 540);
        assert_eq!(plan.positions.first().expect("first").ordinal, 0);
        assert_eq!(plan.positions.last().expect("last").ordinal, 539);
        for block in plan.positions.chunks_exact(4) {
            assert_eq!(
                block
                    .iter()
                    .map(|position| position.subject)
                    .collect::<Vec<_>>(),
                vec![
                    T27PlanSubjectV1::NativeSnapshot,
                    T27PlanSubjectV1::DirectOwnedRocksdb,
                    T27PlanSubjectV1::DirectOwnedRocksdb,
                    T27PlanSubjectV1::NativeSnapshot,
                ]
            );
            assert!(block.iter().all(|position| position.direct_reads));
            assert!(block
                .iter()
                .all(|position| position.treatment_sha256 == block[0].treatment_sha256));
            assert_ne!(
                block[0].expected_engine_options_sha256,
                block[1].expected_engine_options_sha256
            );
        }
        plan.validate().expect("validate admission plan");
    }

    #[test]
    fn preflight_plan_is_one_fresh_abba_block() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);

        assert_eq!(plan.positions.len(), 4);
        assert!(plan
            .positions
            .iter()
            .all(|position| position.trace_seed != fixture.fixture_seed));
        plan.validate().expect("validate preflight plan");
    }

    #[test]
    fn execution_incarnation_preserves_workload_and_runtime_on_replacement_machine() {
        let fixture = locator(65_536);
        let source = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (incarnated, receipt) =
            build_t27_execution_incarnation(&source, replacement_execution())
                .expect("bind frozen workload to replacement machine");

        assert_ne!(source.plan_sha256, incarnated.plan_sha256);
        assert_eq!(
            source.calculated_workload_sha256(),
            incarnated.calculated_workload_sha256()
        );
        assert_eq!(source.positions, incarnated.positions);
        assert_eq!(source.expected, incarnated.expected);
        receipt
            .validate(&source, &incarnated)
            .expect("validate execution incarnation receipt");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t27-execution-incarnation-receipt-v1.schema.json"
        ))
        .expect("decode execution incarnation schema");
        let validator =
            jsonschema::validator_for(&schema).expect("compile execution incarnation schema");
        let instance = serde_json::to_value(&receipt).expect("encode incarnation receipt");
        validator
            .validate(&instance)
            .expect("incarnation receipt must satisfy its schema");
    }

    #[test]
    fn execution_incarnation_rejects_runtime_drift() {
        let fixture = locator(65_536);
        let source = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut replacement = replacement_execution();
        replacement.runtime_executable_sha256 = "0".repeat(64);

        let error = build_t27_execution_incarnation(&source, replacement)
            .expect_err("runtime drift must fail");
        assert!(error.contains("runtime identity"));
    }

    #[test]
    fn execution_incarnation_rejects_same_machine_identity() {
        let fixture = locator(65_536);
        let source = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut replacement = replacement_execution();
        replacement.machine_instance_id = source.execution.machine_instance_id.clone();

        let error = build_t27_execution_incarnation(&source, replacement)
            .expect_err("same machine must fail");
        assert!(error.contains("replacement machine"));
    }

    #[test]
    fn execution_incarnation_receipt_rejects_workload_digest_tampering() {
        let fixture = locator(65_536);
        let source = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (incarnated, mut receipt) =
            build_t27_execution_incarnation(&source, replacement_execution())
                .expect("build incarnation receipt");
        receipt.workload_sha256 = "0".repeat(64);
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&source, &incarnated)
            .expect_err("workload digest tampering must fail");
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn fixture_with_wrong_anchor_version_fails_before_oracle_construction() {
        let mut fixture = locator(65_536);
        fixture.base_version = 1;
        fixture.envelope_sha256 = fixture.calculated_envelope_sha256();

        let error = build_positions(&fixture, T27PlanProfileV1::Preflight64Mib)
            .expect_err("T27 requires the canonical empty-anchor version");
        assert!(error.contains("anchor version"));
    }

    #[test]
    fn relabeled_aabb_plan_fails_even_with_a_recomputed_digest() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (bytes, receipt) = verify_t27_plan_poison(&plan, T27PlanPoisonV1::AabbSchedule)
            .expect("AABB poison must be rejected and sealed");
        let poisoned: T27ExecutionPlanV1 =
            serde_json::from_slice(&bytes).expect("decode poisoned plan artifact");

        assert_eq!(
            poisoned
                .positions
                .iter()
                .map(|position| position.subject)
                .collect::<Vec<_>>(),
            vec![
                T27PlanSubjectV1::NativeSnapshot,
                T27PlanSubjectV1::NativeSnapshot,
                T27PlanSubjectV1::DirectOwnedRocksdb,
                T27PlanSubjectV1::DirectOwnedRocksdb,
            ]
        );
        receipt
            .validate(&plan, &bytes)
            .expect("validate AABB poison receipt");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t27-plan-poison-receipt-v1.schema.json"
        ))
        .expect("decode poison receipt schema");
        let validator = jsonschema::validator_for(&schema).expect("compile poison receipt schema");
        let instance = serde_json::to_value(&receipt).expect("encode poison receipt");
        validator
            .validate(&instance)
            .expect("poison receipt must satisfy its schema");
    }

    #[test]
    fn missing_position_fails_even_with_a_recomputed_digest() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (bytes, receipt) = verify_t27_plan_poison(&plan, T27PlanPoisonV1::MissingPosition)
            .expect("missing-position poison must be rejected and sealed");

        receipt
            .validate(&plan, &bytes)
            .expect("validate missing-position poison receipt");
    }

    #[test]
    fn option_mismatch_fails_even_with_a_recomputed_digest() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (bytes, receipt) = verify_t27_plan_poison(&plan, T27PlanPoisonV1::OptionMismatch)
            .expect("option-mismatch poison must be rejected and sealed");

        receipt
            .validate(&plan, &bytes)
            .expect("validate option-mismatch poison receipt");
    }

    #[test]
    fn plan_poison_receipt_rejects_artifact_tampering() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let (mut bytes, receipt) = verify_t27_plan_poison(&plan, T27PlanPoisonV1::MissingPosition)
            .expect("build poison receipt");
        bytes.push(b' ');

        let error = receipt
            .validate(&plan, &bytes)
            .expect_err("tampered poison bytes must fail");
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn hidden_native_provider_position_poison_is_rejected_and_sealed() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let source = receipt(&plan, 1, T27PlanSubjectV1::DirectOwnedRocksdb);
        let (bytes, poison_receipt) =
            verify_t27_position_poison(&plan, &source, T27PositionPoisonV1::HiddenNativeProvider)
                .expect("hidden-provider poison must be rejected and sealed");
        let poisoned: T27PositionReceiptV1 =
            serde_json::from_slice(&bytes).expect("decode poisoned position receipt");

        assert_eq!(
            poisoned.runtime_resident_provider.as_deref(),
            Some("poison://hidden-native-provider")
        );
        assert_eq!(poisoned.database_count, source.database_count);
        assert_eq!(poisoned.block_cache_count, source.block_cache_count);
        poison_receipt
            .validate(&plan, &source, &bytes)
            .expect("validate hidden-provider poison receipt");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t27-position-poison-receipt-v1.schema.json"
        ))
        .expect("decode position poison receipt schema");
        let validator =
            jsonschema::validator_for(&schema).expect("compile position poison receipt schema");
        let instance =
            serde_json::to_value(&poison_receipt).expect("encode position poison receipt");
        validator
            .validate(&instance)
            .expect("position poison receipt must satisfy its schema");
    }

    #[test]
    fn decoder_requires_an_independent_plan_digest() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let bytes = serde_json::to_vec(&plan).expect("encode plan");
        decode_t27_execution_plan(&bytes, &plan.plan_sha256).expect("decode exact plan");

        let error = decode_t27_execution_plan(&bytes, &"f".repeat(64))
            .expect_err("wrong external digest must fail");
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn fresh_process_receipts_cover_one_complete_abba_plan() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();

        validate_t27_position_receipts(&plan, &receipts).expect("validate receipts");
        let run = T27PlanRunReceiptV1::new(
            &plan,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            TelemetryFlushReport::succeeded(),
        )
        .expect("build run receipt");
        run.validate(&plan, &receipts)
            .expect("validate run receipt");
    }

    #[test]
    fn one_complete_stratum_is_an_authenticated_resumable_unit() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        let stratum_id = plan.positions[0].stratum_id.clone();

        validate_t27_stratum_position_receipts(&plan, &stratum_id, &receipts)
            .expect("validate complete stratum");
        let run = T27StratumRunReceiptV1::new(
            &plan,
            stratum_id,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            TelemetryFlushReport::succeeded(),
        )
        .expect("build stratum run receipt");

        assert!(run.passed);
        assert_eq!(run.workload_sha256, plan.calculated_workload_sha256());
        assert_eq!(
            run.execution_sha256,
            plan.execution.calculated_execution_sha256()
        );
        assert_eq!(run.position_ordinals, vec![0, 1, 2, 3]);
        assert_eq!(run.comparisons.len(), 2);
        run.validate(&plan, &receipts)
            .expect("validate stratum run receipt");
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t27-stratum-run-receipt-v1.schema.json"
        ))
        .expect("decode stratum receipt schema");
        let validator = jsonschema::validator_for(&schema).expect("compile stratum schema");
        let instance = serde_json::to_value(&run).expect("encode stratum receipt");
        validator
            .validate(&instance)
            .expect("stratum receipt must satisfy its schema");

        let error = validate_t27_stratum_position_receipts(
            &plan,
            &run.stratum_id,
            &receipts[..receipts.len() - 1],
        )
        .expect_err("partial stratum must fail closed");
        assert!(error.contains("count mismatch"));
    }

    #[test]
    fn telemetry_binding_mismatch_fails_the_run_receipt() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        let mut run = T27PlanRunReceiptV1::new(
            &plan,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            TelemetryFlushReport::succeeded(),
        )
        .expect("build run receipt");
        run.telemetry_run_id = "10000000-0000-4000-8000-000000000002".to_owned();
        run.receipt_sha256 = run.calculated_receipt_sha256();

        let error = run
            .validate(&plan, &receipts)
            .expect_err("telemetry run identity mismatch must fail");
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn reused_worker_process_fails_the_plan() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        receipts[1].measured_worker_process_id = receipts[0].measured_worker_process_id;
        receipts[1].measured_worker_start_ticks = receipts[0].measured_worker_start_ticks;
        receipts[1].receipt_sha256 = receipts[1].calculated_receipt_sha256();

        let error =
            validate_t27_position_receipts(&plan, &receipts).expect_err("reused process must fail");
        assert!(error.contains("reused"));
    }

    #[test]
    fn overlapping_positions_fail_the_plan() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        receipts[1].started_at = "2026-08-29T01:00:00Z".to_owned();
        receipts[1].receipt_sha256 = receipts[1].calculated_receipt_sha256();

        let error =
            validate_t27_position_receipts(&plan, &receipts).expect_err("overlap must fail");
        assert!(error.contains("overlap"));
    }

    #[test]
    fn effective_option_mismatch_fails_one_position() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 0, T27PlanSubjectV1::NativeSnapshot);
        receipt.block_cache_capacity_bytes = receipt.block_cache_capacity_bytes.saturating_add(1);
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("effective option mismatch must fail");
        assert!(error.contains("options"));
    }

    #[test]
    fn execution_envelope_mismatch_fails_one_position() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 0, T27PlanSubjectV1::NativeSnapshot);
        receipt.execution.runtime_executable_sha256 = "0".repeat(64);
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("execution mismatch must fail");
        assert!(error.contains("identity"));
    }

    #[test]
    fn implicit_second_cache_fails_one_position() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 0, T27PlanSubjectV1::NativeSnapshot);
        receipt.implicit_block_cache_count = 1;
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("implicit cache must fail");
        assert!(error.contains("options"));
    }

    #[test]
    fn wrapper_substituted_for_measured_worker_fails() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 0, T27PlanSubjectV1::NativeSnapshot);
        receipt.measured_worker_process_id = receipt.wrapper_process_id;
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("wrapper PID substitution must fail");
        assert!(error.contains("process identity"));
    }

    #[test]
    fn malformed_raw_report_digest_fails_one_position() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 0, T27PlanSubjectV1::NativeSnapshot);
        receipt.raw_report_sha256 = "not-a-sha256".to_owned();
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("malformed raw-report digest must fail");
        assert!(error.contains("invalid digest"));
    }

    #[test]
    fn cross_lease_receipt_fails_the_plan() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        receipts[1].lease_id = "30000000-0000-4000-8000-000000000002".to_owned();
        receipts[1].receipt_sha256 = receipts[1].calculated_receipt_sha256();

        let error =
            validate_t27_position_receipts(&plan, &receipts).expect_err("cross lease must fail");
        assert!(error.contains("cross-controller"));
    }

    #[test]
    fn catastrophic_native_regression_is_preserved_in_failed_run_receipt() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        for receipt in &mut receipts {
            if receipt.observed_subject == T27PlanSubjectV1::NativeSnapshot {
                receipt.operations_per_second = 100_000.0;
                receipt.latency_ns_p99 = 20_000;
                receipt.cpu_nanoseconds_per_read = 9_000.0;
                receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
            }
        }

        let run = T27PlanRunReceiptV1::new(
            &plan,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            TelemetryFlushReport::succeeded(),
        )
        .expect("catastrophic regression still produces evidence");
        assert!(!run.passed);
        assert!(run.comparisons.iter().all(|comparison| !comparison.passed));
        run.validate(&plan, &receipts)
            .expect("failed run receipt remains valid evidence");
    }

    #[test]
    fn zero_cache_and_physical_pressure_is_preserved_as_failed_admission() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        for receipt in &mut receipts {
            receipt.block_cache_misses = 0;
            receipt.physical_read_bytes = 0;
            receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        }

        let run = T27PlanRunReceiptV1::new(
            &plan,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            TelemetryFlushReport::succeeded(),
        )
        .expect("zero-pressure run still produces evidence");
        assert!(!run.passed);
        assert!(run
            .comparisons
            .iter()
            .all(|comparison| !comparison.pressure_passed));
    }

    #[test]
    fn telemetry_flush_failure_is_preserved_as_failed_run_receipt() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        let mut telemetry_flush = TelemetryFlushReport::succeeded();
        telemetry_flush.logs_flush_succeeded = false;

        let run = T27PlanRunReceiptV1::new(
            &plan,
            &receipts,
            "2026-08-29T01:00:00Z".to_owned(),
            "2026-08-29T01:00:08Z".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            "2026-08-29T00:59:59Z".to_owned(),
            "2026-08-29T01:00:09Z".to_owned(),
            "d".repeat(64),
            telemetry_flush,
        )
        .expect("telemetry failure still produces evidence");
        assert!(!run.passed);
        assert!(!run.telemetry_export_succeeded());
        run.validate(&plan, &receipts)
            .expect("failed telemetry receipt remains valid evidence");
    }

    #[test]
    fn systematic_wrong_tail_fails_the_frozen_oracle() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        for receipt in &mut receipts {
            receipt.tail_sha256 = "5".repeat(64);
            receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        }

        let error =
            validate_t27_position_receipts(&plan, &receipts).expect_err("tail mismatch must fail");
        assert!(error.contains("oracle"));
    }

    #[test]
    fn systematic_wrong_trace_fails_the_frozen_oracle() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipts = plan
            .positions
            .iter()
            .enumerate()
            .map(|(index, position)| receipt(&plan, index, position.subject))
            .collect::<Vec<_>>();
        for receipt in &mut receipts {
            receipt.trace_sha256 = "6".repeat(64);
            receipt.receipt_sha256 = receipt.calculated_receipt_sha256();
        }

        let error =
            validate_t27_position_receipts(&plan, &receipts).expect_err("trace mismatch must fail");
        assert!(error.contains("oracle"));
    }

    #[test]
    fn direct_subject_with_hidden_native_provider_fails() {
        let fixture = locator(65_536);
        let plan = test_plan(&fixture, T27PlanProfileV1::Preflight64Mib);
        let mut receipt = receipt(&plan, 1, T27PlanSubjectV1::DirectOwnedRocksdb);
        receipt.runtime_resident_provider = Some("rocksdb-11.8.1-native-resident-v1".to_owned());
        receipt.receipt_sha256 = receipt.calculated_receipt_sha256();

        let error = receipt
            .validate(&plan)
            .expect_err("hidden native provider must fail");
        assert!(error.contains("hidden"));
    }

    fn receipt(
        plan: &T27ExecutionPlanV1,
        index: usize,
        subject: T27PlanSubjectV1,
    ) -> T27PositionReceiptV1 {
        let position = &plan.positions[index];
        let start_second = index.saturating_mul(2);
        let finish_second = start_second.saturating_add(1);
        T27PositionReceiptV1::new(
            plan,
            position,
            "10000000-0000-4000-8000-000000000001".to_owned(),
            "30000000-0000-4000-8000-000000000001".to_owned(),
            format!("20000000-0000-4000-8000-{:012}", index + 1),
            u32::try_from(index + 100).expect("process id"),
            format!("2026-08-29T01:00:{start_second:02}Z"),
            format!("2026-08-29T01:00:{finish_second:02}Z"),
            T27PositionObservationV1 {
                execution: plan.execution.clone(),
                measured_worker_process_id: u32::try_from(index + 1_000)
                    .expect("measured process id"),
                measured_worker_linux_boot_id: plan.execution.linux_boot_id.clone(),
                measured_worker_start_ticks: u64::try_from(index + 10_000)
                    .expect("process start ticks"),
                fixture_id: plan.fixture.fixture.fixture_id.clone(),
                image_provider: match subject {
                    T27PlanSubjectV1::NativeSnapshot => {
                        "rocksdb-11.8.1-native-resident-v1".to_owned()
                    }
                    T27PlanSubjectV1::DirectOwnedRocksdb => {
                        "rocksdb-11.8.1-direct-owned-v1".to_owned()
                    }
                },
                runtime_resident_provider: match subject {
                    T27PlanSubjectV1::NativeSnapshot => {
                        Some("rocksdb-11.8.1-native-resident-v1".to_owned())
                    }
                    T27PlanSubjectV1::DirectOwnedRocksdb => None,
                },
                runtime_serving_image_provider: None,
                tail_sha256: plan.expected.tail_sha256.clone(),
                resident_logical_sha256: plan.expected.resident_logical_sha256.clone(),
                report_semantic_sha256: "3".repeat(64),
                trace_sha256: plan.expected.trace_sha256_by_stratum[&position.stratum_id].clone(),
                subject,
                effective_engine_options_sha256: position.expected_engine_options_sha256.clone(),
                engine_topology: match subject {
                    T27PlanSubjectV1::NativeSnapshot => "native-resident:1db:1cache:3cf".to_owned(),
                    T27PlanSubjectV1::DirectOwnedRocksdb => {
                        "direct-owned:1db:1cache:1cf".to_owned()
                    }
                },
                database_count: 1,
                block_cache_count: 1,
                implicit_block_cache_count: 0,
                column_family_count: match subject {
                    T27PlanSubjectV1::NativeSnapshot => 3,
                    T27PlanSubjectV1::DirectOwnedRocksdb => 1,
                },
                metadata_cache_disabled: subject == T27PlanSubjectV1::NativeSnapshot,
                direct_reads: true,
                block_cache_capacity_bytes: position.block_cache_bytes,
                block_cache_usage_bytes: position.block_cache_bytes / 2,
                block_cache_misses: 100,
                operations_per_second: 1_000_000.0,
                latency_ns_p99: 2_000,
                cpu_nanoseconds_per_read: 900.0,
                physical_read_bytes: 4_096,
                read_amplification_ratio: 1.0,
                flush_write_bytes: 0,
                compaction_read_bytes: 0,
                compaction_write_bytes: 0,
                correctness_failures: 0,
                object_requests: 0,
                scratch_was_empty: true,
                process_cpu_supported: true,
                linux_proc_supported: true,
                raw_report_sha256: "7".repeat(64),
            },
        )
    }

    fn test_plan(
        fixture: &FixturePlacementLocatorV1,
        profile: T27PlanProfileV1,
    ) -> T27ExecutionPlanV1 {
        let positions = build_positions(fixture, profile).expect("build positions");
        let expected = T27ExpectedIdentityV1 {
            tail_sha256: "1".repeat(64),
            resident_logical_sha256: "2".repeat(64),
            trace_sha256_by_stratum: positions
                .iter()
                .map(|position| (position.stratum_id.clone(), "4".repeat(64)))
                .collect(),
        };
        build_t27_execution_plan(fixture, profile, execution(), expected).expect("build T27 plan")
    }

    fn execution() -> T27ExecutionEnvelopeV1 {
        T27ExecutionEnvelopeV1 {
            runtime_source_sha256: "8".repeat(64),
            runtime_executable_sha256: "9".repeat(64),
            runtime_cargo_lock_sha256: "a".repeat(64),
            machine_receipt_sha256: "b".repeat(64),
            machine_instance_id: "123456789".to_owned(),
            infrastructure_lease_expires_epoch: 2_000_000_000,
            linux_boot_id: "40000000-0000-4000-8000-000000000001".to_owned(),
            scratch_root: "/mnt/objectkv-hot/serving".to_owned(),
            scratch_mount_point: "/mnt/objectkv-hot".to_owned(),
            scratch_mount_source: "/dev/nvme0n1".to_owned(),
            scratch_filesystem_type: "ext4".to_owned(),
            scratch_major_minor: "259:0".to_owned(),
            scratch_device_number: 66304,
            scratch_filesystem_uuid: "50000000-0000-4000-8000-000000000001".to_owned(),
            scratch_block_device: "/dev/nvme0n1".to_owned(),
            host_lease_path: "/var/lib/objectkv/t27.lock".to_owned(),
        }
    }

    fn replacement_execution() -> T27ExecutionEnvelopeV1 {
        let mut replacement = execution();
        replacement.machine_receipt_sha256 = "c".repeat(64);
        replacement.machine_instance_id = "987654321".to_owned();
        replacement.infrastructure_lease_expires_epoch = 2_100_000_000;
        replacement.linux_boot_id = "40000000-0000-4000-8000-000000000002".to_owned();
        replacement.scratch_filesystem_uuid = "50000000-0000-4000-8000-000000000002".to_owned();
        replacement.host_lease_path = "/var/lib/objectkv/t27-replacement.lock".to_owned();
        replacement
    }

    fn locator(key_count: u64) -> FixturePlacementLocatorV1 {
        let fixture_id = "a".repeat(64);
        let mut locator = FixturePlacementLocatorV1 {
            schema_version: 1,
            fixture: ObjectFixtureLocatorV1 {
                fixture_id: fixture_id.clone(),
                descriptor_length: 651,
                descriptor_sha256: "b".repeat(64),
            },
            base_version: 2,
            provider: "gcs".to_owned(),
            bucket: "doss-objectkv-dev-okv-evals".to_owned(),
            prefix: "runs/t27-plan-test".to_owned(),
            descriptor_key: format!("fixtures/single-range/v1/descriptors/{fixture_id}.json"),
            descriptor_generation: "1787976513990982".to_owned(),
            fixture_seed: 4_244,
            key_count,
            value_bytes: 1_024,
            logical_bytes: key_count * 1_024,
            generator_version: 1,
            row_object_format_version: 1,
            target_object_bytes: 8_388_608,
            target_block_bytes: 65_536,
            source_sha256: "c".repeat(64),
            suite_sha256: "d".repeat(64),
            binary_sha256: "e".repeat(64),
            cargo_lock_sha256: "f".repeat(64),
            envelope_sha256: String::new(),
        };
        locator.envelope_sha256 = locator.calculated_envelope_sha256();
        locator
    }

    #[allow(dead_code)]
    fn assert_serializable(_: &T27ExecutionPlanV1) {}
}
