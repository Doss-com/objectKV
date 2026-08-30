//! Falsification boundary for RFC-0046 cold-point execution evidence.

use crate::object_fixture::FixturePlacementLocatorV1;
use crate::t28_cold_point::{T28CacheState, T28PointOperationV1, T28PointPlanV1, T28PointSubject};
use okv_object::content_sha256;
use serde::{Deserialize, Serialize};
use std::ops::Range;

const SCHEMA_VERSION: u32 = 1;
const VIEWER_ROLE: &str = "roles/storage.objectViewer";

/// One deliberate corruption of the T28 execution boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28BoundaryPoisonV1 {
    FullObjectRead,
    ListAuthority,
    StaleDescriptorGeneration,
    CrossFixtureIndex,
    HiddenLocalFixture,
    UnexpectedRetry,
    WriterAuthority,
    UnrestrictedReadVersion,
    OverlappingProcess,
    ReusedState,
}

impl T28BoundaryPoisonV1 {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::FullObjectRead => "full_object_read",
            Self::ListAuthority => "list_authority",
            Self::StaleDescriptorGeneration => "stale_descriptor_generation",
            Self::CrossFixtureIndex => "cross_fixture_index",
            Self::HiddenLocalFixture => "hidden_local_fixture",
            Self::UnexpectedRetry => "unexpected_retry",
            Self::WriterAuthority => "writer_authority",
            Self::UnrestrictedReadVersion => "unrestricted_read_version",
            Self::OverlappingProcess => "overlapping_process",
            Self::ReusedState => "reused_state",
        }
    }
}

/// Identity, authority, process, and request evidence surrounding one point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28ExecutionBoundaryV1 {
    pub schema_version: u32,
    pub plan_sha256: String,
    pub placement_envelope_sha256: String,
    pub fixture_id: String,
    pub descriptor_generation: String,
    pub read_version: u64,
    pub cache_state: T28CacheState,
    pub subject: T28PointSubject,
    pub operation_ordinal: u64,
    pub provider: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub principal_email: String,
    pub principal_unique_id: String,
    pub credential_source: String,
    pub iam_receipt_sha256: String,
    pub reader_role_bindings: Vec<String>,
    pub token_expires_unix_nanos: u64,
    pub denied_write_probe_error_class: String,
    pub denied_write_probe_object_absent: bool,
    pub index_key: String,
    pub index_sha256: String,
    pub data_key: String,
    pub requested_range: Range<u64>,
    pub logical_data_requests: u64,
    pub provider_attempts: u64,
    pub full_data_requests: u64,
    pub list_requests: u64,
    pub put_requests: u64,
    pub delete_requests: u64,
    pub machine_id: String,
    pub boot_id: String,
    pub process_id: u32,
    pub process_start_ticks: u64,
    pub process_started_unix_nanos: u64,
    pub process_finished_unix_nanos: u64,
    pub previous_process_id: u32,
    pub previous_process_start_ticks: u64,
    pub previous_process_finished_unix_nanos: u64,
    pub process_local_state_reused: bool,
    pub hidden_local_fixture_path: Option<String>,
    pub boundary_sha256: String,
}

impl T28ExecutionBoundaryV1 {
    /// Seal an execution boundary after its evidence fields are populated.
    ///
    /// # Errors
    ///
    /// Returns an error when the boundary cannot be serialized.
    pub fn seal(&mut self) -> Result<(), String> {
        self.boundary_sha256 = self.calculated_sha256()?;
        Ok(())
    }

    /// Return the canonical SHA-256 with the digest field excluded.
    ///
    /// # Errors
    ///
    /// Returns an error when the boundary cannot be serialized.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.boundary_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }

    /// Validate one positive boundary against independent placement and plan inputs.
    ///
    /// # Errors
    ///
    /// Returns an error for any fixture, authority, process, provider, range,
    /// retry, request-count, or boundary-digest mismatch.
    pub fn validate(
        &self,
        placement: &FixturePlacementLocatorV1,
        plan: &T28PointPlanV1,
    ) -> Result<(), String> {
        plan.validate(placement)?;
        let operation = plan
            .operations
            .get(usize::try_from(self.operation_ordinal).unwrap_or(usize::MAX))
            .ok_or_else(|| "T28 boundary operation is absent".to_owned())?;
        if self.schema_version != SCHEMA_VERSION
            || self.plan_sha256 != plan.plan_sha256
            || self.placement_envelope_sha256 != placement.envelope_sha256
            || self.fixture_id != placement.fixture.fixture_id
            || self.descriptor_generation != placement.descriptor_generation
            || self.read_version != placement.base_version
            || self.read_version != plan.read_version
            || self.cache_state != plan.cache_state
            || operation.ordinal != self.operation_ordinal
            || self.boundary_sha256 != self.calculated_sha256()?
        {
            return Err("T28 boundary plan, fixture, version, or digest mismatch".to_owned());
        }
        self.validate_authority(placement)?;
        self.validate_request(operation)?;
        self.validate_process()?;
        Ok(())
    }

    fn validate_authority(&self, placement: &FixturePlacementLocatorV1) -> Result<(), String> {
        let identity_invalid = self.provider != "gcs"
            || self.project.trim().is_empty()
            || self.bucket != placement.bucket
            || self.region.trim().is_empty()
            || self.principal_email.trim().is_empty()
            || self.principal_unique_id.trim().is_empty()
            || self.credential_source != "gce_metadata_server"
            || !valid_sha256(&self.iam_receipt_sha256)
            || self.reader_role_bindings != [VIEWER_ROLE]
            || self.denied_write_probe_error_class != "permission_denied"
            || !self.denied_write_probe_object_absent;
        let minimum_expiry = self
            .process_started_unix_nanos
            .saturating_add(900_000_000_000);
        if identity_invalid || self.token_expires_unix_nanos < minimum_expiry {
            return Err("T28 boundary read-only authority mismatch".to_owned());
        }
        Ok(())
    }

    fn validate_request(&self, operation: &T28PointOperationV1) -> Result<(), String> {
        let planned_range = operation.point.block.range()?;
        if self.index_key != operation.point.index_key
            || self.index_sha256 != operation.point.index_sha256
            || self.data_key != operation.point.data_key
            || self.requested_range != planned_range
            || self.logical_data_requests != 1
            || self.provider_attempts != 1
            || self.full_data_requests != 0
            || self.list_requests != 0
            || self.put_requests != 0
            || self.delete_requests != 0
        {
            return Err("T28 boundary provider request differs from the sealed point".to_owned());
        }
        Ok(())
    }

    fn validate_process(&self) -> Result<(), String> {
        if self.machine_id.trim().is_empty()
            || self.boot_id.trim().is_empty()
            || self.process_id == 0
            || self.process_start_ticks == 0
            || self.process_started_unix_nanos == 0
            || self.process_finished_unix_nanos < self.process_started_unix_nanos
            || self.previous_process_id == 0
            || self.previous_process_start_ticks == 0
            || (self.process_id == self.previous_process_id
                && self.process_start_ticks == self.previous_process_start_ticks)
            || self.previous_process_finished_unix_nanos > self.process_started_unix_nanos
            || self.process_local_state_reused
            || self.hidden_local_fixture_path.is_some()
        {
            return Err("T28 boundary process freshness or local-state mismatch".to_owned());
        }
        Ok(())
    }
}

/// Receipt proving that one sealed negative control was rejected.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28BoundaryPoisonReceiptV1 {
    pub schema_version: u32,
    pub plan_sha256: String,
    pub poison: T28BoundaryPoisonV1,
    pub poisoned_boundary_sha256: String,
    pub detected: bool,
    pub rejection: String,
    pub receipt_sha256: String,
}

impl T28BoundaryPoisonReceiptV1 {
    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.receipt_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }

    /// Validate poison receipt identity and detection outcome.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed digest, empty rejection, or an
    /// undetected poison.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != SCHEMA_VERSION
            || !valid_sha256(&self.plan_sha256)
            || !valid_sha256(&self.poisoned_boundary_sha256)
            || !self.detected
            || self.rejection.trim().is_empty()
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid T28 boundary poison receipt".to_owned());
        }
        Ok(())
    }
}

/// Apply one sealed corruption and require the unchanged boundary validator to reject it.
///
/// # Errors
///
/// Returns an error when the source boundary is invalid, the poison is not
/// detected, or the resulting receipt cannot be sealed.
pub fn verify_t28_boundary_poison(
    placement: &FixturePlacementLocatorV1,
    plan: &T28PointPlanV1,
    correct: &T28ExecutionBoundaryV1,
    poison: T28BoundaryPoisonV1,
) -> Result<(T28ExecutionBoundaryV1, T28BoundaryPoisonReceiptV1), String> {
    correct.validate(placement, plan)?;
    let mut poisoned = correct.clone();
    apply_poison(&mut poisoned, plan, poison)?;
    poisoned.seal()?;
    let Err(rejection) = poisoned.validate(placement, plan) else {
        return Err("T28 negative control escaped the unchanged validator".to_owned());
    };
    let mut receipt = T28BoundaryPoisonReceiptV1 {
        schema_version: SCHEMA_VERSION,
        plan_sha256: plan.plan_sha256.clone(),
        poison,
        poisoned_boundary_sha256: poisoned.boundary_sha256.clone(),
        detected: true,
        rejection,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok((poisoned, receipt))
}

fn apply_poison(
    boundary: &mut T28ExecutionBoundaryV1,
    plan: &T28PointPlanV1,
    poison: T28BoundaryPoisonV1,
) -> Result<(), String> {
    match poison {
        T28BoundaryPoisonV1::FullObjectRead => {
            let operation = plan
                .operations
                .get(
                    usize::try_from(boundary.operation_ordinal)
                        .map_err(|_| "T28 poison ordinal exceeds usize".to_owned())?,
                )
                .ok_or_else(|| "T28 poison operation is absent".to_owned())?;
            boundary.requested_range = 0..operation.point.block.object_length;
            boundary.full_data_requests = 1;
        }
        T28BoundaryPoisonV1::ListAuthority => boundary.list_requests = 1,
        T28BoundaryPoisonV1::StaleDescriptorGeneration => {
            boundary.descriptor_generation.push('1');
        }
        T28BoundaryPoisonV1::CrossFixtureIndex => {
            boundary.index_sha256 = "0".repeat(64);
        }
        T28BoundaryPoisonV1::HiddenLocalFixture => {
            "filesystem".clone_into(&mut boundary.provider);
            boundary.hidden_local_fixture_path = Some("/tmp/hidden-fixture".to_owned());
        }
        T28BoundaryPoisonV1::UnexpectedRetry => boundary.provider_attempts = 2,
        T28BoundaryPoisonV1::WriterAuthority => {
            boundary
                .reader_role_bindings
                .push("roles/storage.objectAdmin".to_owned());
            "ok".clone_into(&mut boundary.denied_write_probe_error_class);
        }
        T28BoundaryPoisonV1::UnrestrictedReadVersion => {
            boundary.read_version = boundary.read_version.saturating_add(1);
        }
        T28BoundaryPoisonV1::OverlappingProcess => {
            boundary.previous_process_finished_unix_nanos =
                boundary.process_started_unix_nanos.saturating_add(1);
        }
        T28BoundaryPoisonV1::ReusedState => {
            boundary.previous_process_id = boundary.process_id;
            boundary.previous_process_start_ticks = boundary.process_start_ticks;
            boundary.process_local_state_reused = true;
        }
    }
    Ok(())
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
        verify_t28_boundary_poison, T28BoundaryPoisonV1, T28ExecutionBoundaryV1, VIEWER_ROLE,
    };
    use crate::object_fixture::{
        FixturePlacementLocatorV1, FixturePointPlanV1, ObjectFixtureLocatorV1,
    };
    use crate::t28_cold_point::{T28CacheState, T28PointPlanV1, T28PointSubject};
    use okv_object::PointBlockPlanV1;
    use std::collections::BTreeSet;

    fn placement() -> FixturePlacementLocatorV1 {
        let mut placement = FixturePlacementLocatorV1 {
            schema_version: 1,
            fixture: ObjectFixtureLocatorV1 {
                fixture_id: "1".repeat(64),
                descriptor_length: 662,
                descriptor_sha256: "2".repeat(64),
            },
            base_version: 41,
            provider: "gcs".to_owned(),
            bucket: "doss-objectkv-dev-okv-evals".to_owned(),
            prefix: "runs/t28".to_owned(),
            descriptor_key: format!(
                "fixtures/single-range/v1/descriptors/{}.json",
                "1".repeat(64)
            ),
            descriptor_generation: "1788064498299168".to_owned(),
            fixture_seed: 7,
            key_count: 1_024,
            value_bytes: 256,
            logical_bytes: 262_144,
            generator_version: 1,
            row_object_format_version: 1,
            target_object_bytes: 1_048_576,
            target_block_bytes: 65_536,
            source_sha256: "3".repeat(64),
            suite_sha256: "4".repeat(64),
            binary_sha256: "5".repeat(64),
            cargo_lock_sha256: "6".repeat(64),
            envelope_sha256: String::new(),
        };
        placement.envelope_sha256 = placement.calculated_envelope_sha256();
        placement
    }

    fn plan(placement: &FixturePlacementLocatorV1) -> T28PointPlanV1 {
        T28PointPlanV1::seal(
            placement,
            T28CacheState::MetadataWarmDataCold,
            vec![(
                17,
                FixturePointPlanV1 {
                    fixture_id: placement.fixture.fixture_id.clone(),
                    read_version: placement.base_version,
                    key: 17_u64.to_be_bytes().to_vec(),
                    data_key: "fixtures/data/sha256/aaaaaaaa".to_owned(),
                    index_key: "fixtures/index/sha256/bbbbbbbb".to_owned(),
                    index_bytes: 4_096,
                    index_sha256: "7".repeat(64),
                    block: PointBlockPlanV1 {
                        object_length: 1_048_576,
                        data_sha256: "8".repeat(64),
                        offset: 65_536,
                        length: 65_000,
                        first_key: 17_u64.to_be_bytes().to_vec(),
                        last_key: 17_u64.to_be_bytes().to_vec(),
                        min_version: 41,
                        max_version: 41,
                        block_sha256: "9".repeat(64),
                    },
                },
            )],
        )
        .expect("seal plan")
    }

    fn correct_boundary(
        placement: &FixturePlacementLocatorV1,
        plan: &T28PointPlanV1,
    ) -> T28ExecutionBoundaryV1 {
        let operation = &plan.operations[0];
        let mut boundary = T28ExecutionBoundaryV1 {
            schema_version: 1,
            plan_sha256: plan.plan_sha256.clone(),
            placement_envelope_sha256: placement.envelope_sha256.clone(),
            fixture_id: placement.fixture.fixture_id.clone(),
            descriptor_generation: placement.descriptor_generation.clone(),
            read_version: placement.base_version,
            cache_state: plan.cache_state,
            subject: T28PointSubject::Candidate,
            operation_ordinal: 0,
            provider: "gcs".to_owned(),
            project: "doss-objectkv-dev".to_owned(),
            bucket: placement.bucket.clone(),
            region: "us-central1".to_owned(),
            principal_email: "objectkv-reader@example.invalid".to_owned(),
            principal_unique_id: "123456789".to_owned(),
            credential_source: "gce_metadata_server".to_owned(),
            iam_receipt_sha256: "a".repeat(64),
            reader_role_bindings: vec![VIEWER_ROLE.to_owned()],
            token_expires_unix_nanos: 2_000_000_000_000,
            denied_write_probe_error_class: "permission_denied".to_owned(),
            denied_write_probe_object_absent: true,
            index_key: operation.point.index_key.clone(),
            index_sha256: operation.point.index_sha256.clone(),
            data_key: operation.point.data_key.clone(),
            requested_range: operation.point.block.range().expect("point range"),
            logical_data_requests: 1,
            provider_attempts: 1,
            full_data_requests: 0,
            list_requests: 0,
            put_requests: 0,
            delete_requests: 0,
            machine_id: "runner-1".to_owned(),
            boot_id: "boot-1".to_owned(),
            process_id: 200,
            process_start_ticks: 2_000,
            process_started_unix_nanos: 1_000_000_000,
            process_finished_unix_nanos: 1_100_000_000,
            previous_process_id: 199,
            previous_process_start_ticks: 1_000,
            previous_process_finished_unix_nanos: 999_999_999,
            process_local_state_reused: false,
            hidden_local_fixture_path: None,
            boundary_sha256: String::new(),
        };
        boundary.seal().expect("seal boundary");
        boundary
    }

    #[test]
    fn unchanged_boundary_oracle_rejects_all_ten_poisons() {
        let placement = placement();
        let plan = plan(&placement);
        let correct = correct_boundary(&placement, &plan);
        correct
            .validate(&placement, &plan)
            .expect("positive boundary");
        let poisons = [
            T28BoundaryPoisonV1::FullObjectRead,
            T28BoundaryPoisonV1::ListAuthority,
            T28BoundaryPoisonV1::StaleDescriptorGeneration,
            T28BoundaryPoisonV1::CrossFixtureIndex,
            T28BoundaryPoisonV1::HiddenLocalFixture,
            T28BoundaryPoisonV1::UnexpectedRetry,
            T28BoundaryPoisonV1::WriterAuthority,
            T28BoundaryPoisonV1::UnrestrictedReadVersion,
            T28BoundaryPoisonV1::OverlappingProcess,
            T28BoundaryPoisonV1::ReusedState,
        ];
        let mut receipts = BTreeSet::new();
        for poison in poisons {
            let (_, receipt) = verify_t28_boundary_poison(&placement, &plan, &correct, poison)
                .expect("poison must be detected");
            assert!(receipt.detected, "{}", poison.id());
            assert!(receipts.insert(receipt.receipt_sha256));
        }
        assert_eq!(receipts.len(), 10);
    }

    #[test]
    fn unsealed_or_writer_capable_positive_boundary_is_rejected() {
        let placement = placement();
        let plan = plan(&placement);
        let mut boundary = correct_boundary(&placement, &plan);
        boundary.boundary_sha256.clear();
        assert!(boundary.validate(&placement, &plan).is_err());

        let mut writer = correct_boundary(&placement, &plan);
        writer
            .reader_role_bindings
            .push("roles/storage.objectCreator".to_owned());
        writer.seal().expect("seal writer boundary");
        assert!(writer.validate(&placement, &plan).is_err());
    }

    #[test]
    fn positive_boundary_and_all_poison_receipts_match_frozen_schemas() {
        let placement = placement();
        let plan = plan(&placement);
        let correct = correct_boundary(&placement, &plan);
        let boundary_schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t28-execution-boundary-v1.schema.json"
        ))
        .expect("decode boundary schema");
        jsonschema::validator_for(&boundary_schema)
            .expect("compile boundary schema")
            .validate(&serde_json::to_value(&correct).expect("encode boundary"))
            .expect("positive boundary matches schema");

        let receipt_schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t28-boundary-poison-receipt-v1.schema.json"
        ))
        .expect("decode poison schema");
        let validator = jsonschema::validator_for(&receipt_schema).expect("compile poison schema");
        for poison in [
            T28BoundaryPoisonV1::FullObjectRead,
            T28BoundaryPoisonV1::ListAuthority,
            T28BoundaryPoisonV1::StaleDescriptorGeneration,
            T28BoundaryPoisonV1::CrossFixtureIndex,
            T28BoundaryPoisonV1::HiddenLocalFixture,
            T28BoundaryPoisonV1::UnexpectedRetry,
            T28BoundaryPoisonV1::WriterAuthority,
            T28BoundaryPoisonV1::UnrestrictedReadVersion,
            T28BoundaryPoisonV1::OverlappingProcess,
            T28BoundaryPoisonV1::ReusedState,
        ] {
            let (_, receipt) = verify_t28_boundary_poison(&placement, &plan, &correct, poison)
                .expect("poison receipt");
            validator
                .validate(&serde_json::to_value(receipt).expect("encode poison receipt"))
                .expect("poison receipt matches schema");
        }
    }
}
