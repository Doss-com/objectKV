//! Frozen plan and unchanged oracle for RFC-0046 generation-pinned cold points.

use crate::object_fixture::{base_record_at, FixturePlacementLocatorV1, FixturePointPlanV1};
use crate::provider_attempt::{ProviderAttemptEventV1, ProviderAttemptPhase};
use okv_object::{content_sha256, PointRead, PointReadOutcome};
use serde::{Deserialize, Serialize};

const PLAN_SCHEMA_VERSION: u32 = 1;
const MAX_DATA_RANGE_BYTES: u64 = 64 * 1_024;

/// Process-local cache state frozen into one T28 position.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum T28CacheState {
    EmptyReader,
    MetadataWarmDataCold,
}

/// One point operation shared by the candidate and raw-range control.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28PointOperationV1 {
    pub ordinal: u64,
    pub key_id: u64,
    pub expected_value_sha256: String,
    pub point: FixturePointPlanV1,
}

/// Immutable T28 operation plan bound to one exact fixture generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28PointPlanV1 {
    pub schema_version: u32,
    pub placement_envelope_sha256: String,
    pub fixture_id: String,
    pub descriptor_generation: String,
    pub read_version: u64,
    pub cache_state: T28CacheState,
    pub object_store_max_retries: u32,
    pub max_data_range_bytes: u64,
    pub operations: Vec<T28PointOperationV1>,
    pub plan_sha256: String,
}

impl T28PointPlanV1 {
    /// Seal point plans derived from authenticated indexes into one immutable plan.
    ///
    /// # Errors
    ///
    /// Returns an error when placement identity or any point differs from the
    /// independently generated fixture value.
    pub fn seal(
        placement: &FixturePlacementLocatorV1,
        cache_state: T28CacheState,
        points: Vec<(u64, FixturePointPlanV1)>,
    ) -> Result<Self, String> {
        placement.validate()?;
        if points.is_empty() {
            return Err("T28 point plan must contain at least one operation".to_owned());
        }
        let value_bytes = usize::try_from(placement.value_bytes)
            .map_err(|_| "T28 fixture value bytes exceed usize".to_owned())?;
        let operations = points
            .into_iter()
            .enumerate()
            .map(|(ordinal, (key_id, point))| {
                let record = base_record_at(
                    placement.fixture_seed,
                    key_id,
                    value_bytes,
                    placement.base_version,
                );
                let value = record
                    .value
                    .ok_or_else(|| "T28 fixture generator returned a tombstone".to_owned())?;
                if point.key != record.key {
                    return Err("T28 indexed point differs from the frozen key ID".to_owned());
                }
                Ok(T28PointOperationV1 {
                    ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
                    key_id,
                    expected_value_sha256: content_sha256(&value),
                    point,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut plan = Self {
            schema_version: PLAN_SCHEMA_VERSION,
            placement_envelope_sha256: placement.envelope_sha256.clone(),
            fixture_id: placement.fixture.fixture_id.clone(),
            descriptor_generation: placement.descriptor_generation.clone(),
            read_version: placement.base_version,
            cache_state,
            object_store_max_retries: 0,
            max_data_range_bytes: MAX_DATA_RANGE_BYTES,
            operations,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = plan.calculated_sha256()?;
        plan.validate(placement)?;
        Ok(plan)
    }

    /// Validate this plan against an independently supplied placement locator.
    ///
    /// # Errors
    ///
    /// Returns an error for identity, version, retry, range, ordering, expected
    /// value, or plan-digest drift.
    pub fn validate(&self, placement: &FixturePlacementLocatorV1) -> Result<(), String> {
        placement.validate()?;
        if self.schema_version != PLAN_SCHEMA_VERSION
            || self.placement_envelope_sha256 != placement.envelope_sha256
            || self.fixture_id != placement.fixture.fixture_id
            || self.descriptor_generation != placement.descriptor_generation
            || self.read_version != placement.base_version
            || self.object_store_max_retries != 0
            || self.max_data_range_bytes != MAX_DATA_RANGE_BYTES
            || self.operations.is_empty()
            || self.plan_sha256 != self.calculated_sha256()?
        {
            return Err("T28 plan identity, retry, or digest mismatch".to_owned());
        }
        let value_bytes = usize::try_from(placement.value_bytes)
            .map_err(|_| "T28 fixture value bytes exceed usize".to_owned())?;
        for (ordinal, operation) in self.operations.iter().enumerate() {
            let record = base_record_at(
                placement.fixture_seed,
                operation.key_id,
                value_bytes,
                placement.base_version,
            );
            let expected_value = record
                .value
                .ok_or_else(|| "T28 fixture generator returned a tombstone".to_owned())?;
            if operation.ordinal != u64::try_from(ordinal).unwrap_or(u64::MAX)
                || operation.key_id >= placement.key_count
                || operation.point.fixture_id != self.fixture_id
                || operation.point.read_version != self.read_version
                || operation.point.key != record.key
                || operation.point.data_key.is_empty()
                || operation.point.index_key.is_empty()
                || operation.point.index_bytes == 0
                || !valid_sha256(&operation.point.index_sha256)
                || operation.expected_value_sha256 != content_sha256(&expected_value)
                || operation.point.block.length > self.max_data_range_bytes
                || operation.point.block.length == 0
                || operation.point.block.length == operation.point.block.object_length
                || operation.point.key < operation.point.block.first_key
                || operation.point.key > operation.point.block.last_key
            {
                return Err("T28 operation identity, value, or bounded range mismatch".to_owned());
            }
            operation.point.block.validate()?;
            let range = operation.point.block.range()?;
            if range.end > operation.point.block.object_length {
                return Err("T28 operation range exceeds its object".to_owned());
            }
        }
        Ok(())
    }

    /// Return the canonical SHA-256 with the digest field excluded.
    ///
    /// # Errors
    ///
    /// Returns an error when the plan cannot be serialized.
    pub fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.plan_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Evaluate one measured data range against its sealed operation and provider events.
///
/// # Errors
///
/// Returns an error when the value, provider, call count, event pairing, key,
/// range, returned identity, bytes, or retry ordinal differs from the plan.
pub fn evaluate_measured_point(
    operation: &T28PointOperationV1,
    read: &PointRead,
    expected_subject: &str,
    expected_provider: &str,
    events: &[ProviderAttemptEventV1],
) -> Result<(), String> {
    let value = match &read.outcome {
        PointReadOutcome::Value(value) => value,
        PointReadOutcome::Tombstone | PointReadOutcome::Absent => {
            return Err("T28 measured point did not return its expected value".to_owned());
        }
    };
    if content_sha256(value) != operation.expected_value_sha256
        || read.data_bytes != operation.point.block.length
    {
        return Err("T28 measured point value or byte count differs from plan".to_owned());
    }
    if events.len() != 2 {
        return Err("T28 measured point requires exactly one provider attempt pair".to_owned());
    }
    let started = &events[0];
    let completed = &events[1];
    let range = operation.point.block.range()?;
    if started.schema_version != 1
        || completed.schema_version != 1
        || started.phase != ProviderAttemptPhase::Started
        || completed.phase != ProviderAttemptPhase::Completed
        || started.sequence.checked_add(1) != Some(completed.sequence)
        || started.operation_id != completed.operation_id
        || started.attempt_ordinal != 1
        || completed.attempt_ordinal != 1
        || started.subject != expected_subject
        || completed.subject != expected_subject
        || started.provider != expected_provider
        || completed.provider != expected_provider
        || started.api != "get"
        || completed.api != "get"
        || started.object_key != operation.point.data_key
        || completed.object_key != operation.point.data_key
        || started.requested_range != Some(range.clone())
        || completed.requested_range != Some(range.clone())
        || completed.returned_range != Some(range)
        || completed.object_length != Some(operation.point.block.object_length)
        || completed.response_payload_bytes != operation.point.block.length
        || completed.result.as_deref() != Some("ok")
        || started.request_payload_bytes != 0
        || completed.request_payload_bytes != 0
    {
        return Err("T28 provider attempt differs from the sealed data range".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{evaluate_measured_point, T28CacheState, T28PointPlanV1};
    use crate::object_fixture::{
        FixturePlacementLocatorV1, FixturePointPlanV1, ObjectFixtureLocatorV1,
    };
    use crate::provider_attempt::{ProviderAttemptEventV1, ProviderAttemptPhase};
    use bytes::Bytes;
    use okv_object::{
        content_sha256, PointBlockPlanV1, PointRead, PointReadOutcome, RevisionToken,
    };

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

    fn point(key_id: u64) -> FixturePointPlanV1 {
        FixturePointPlanV1 {
            fixture_id: "1".repeat(64),
            read_version: 41,
            key: key_id.to_be_bytes().to_vec(),
            data_key: "fixtures/data/sha256/aaaaaaaa".to_owned(),
            index_key: "fixtures/index/sha256/bbbbbbbb".to_owned(),
            index_bytes: 4_096,
            index_sha256: "7".repeat(64),
            block: PointBlockPlanV1 {
                object_length: 1_048_576,
                data_sha256: "8".repeat(64),
                offset: 65_536,
                length: 65_000,
                first_key: key_id.to_be_bytes().to_vec(),
                last_key: key_id.to_be_bytes().to_vec(),
                min_version: 41,
                max_version: 41,
                block_sha256: "9".repeat(64),
            },
        }
    }

    fn successful_events(operation: &super::T28PointOperationV1) -> Vec<ProviderAttemptEventV1> {
        let range = operation.point.block.range().expect("range");
        vec![
            ProviderAttemptEventV1 {
                schema_version: 1,
                sequence: 1,
                operation_id: 1,
                attempt_ordinal: 1,
                subject: "candidate".to_owned(),
                provider: "gcs".to_owned(),
                phase: ProviderAttemptPhase::Started,
                api: "get".to_owned(),
                object_key: operation.point.data_key.clone(),
                requested_range: Some(range.clone()),
                expected_revision: None,
                started_unix_nanos: 1,
                result: None,
                returned_revision: None,
                object_length: None,
                returned_range: None,
                request_payload_bytes: 0,
                response_payload_bytes: 0,
                elapsed_nanos: 0,
            },
            ProviderAttemptEventV1 {
                schema_version: 1,
                sequence: 2,
                operation_id: 1,
                attempt_ordinal: 1,
                subject: "candidate".to_owned(),
                provider: "gcs".to_owned(),
                phase: ProviderAttemptPhase::Completed,
                api: "get".to_owned(),
                object_key: operation.point.data_key.clone(),
                requested_range: Some(range.clone()),
                expected_revision: None,
                started_unix_nanos: 1,
                result: Some("ok".to_owned()),
                returned_revision: Some(RevisionToken {
                    e_tag: None,
                    version: Some("1".to_owned()),
                }),
                object_length: Some(operation.point.block.object_length),
                returned_range: Some(range),
                request_payload_bytes: 0,
                response_payload_bytes: operation.point.block.length,
                elapsed_nanos: 10,
            },
        ]
    }

    #[test]
    fn sealed_plan_binds_generation_retry_value_and_bounded_range() {
        let placement = placement();
        let plan = T28PointPlanV1::seal(
            &placement,
            T28CacheState::MetadataWarmDataCold,
            vec![(17, point(17))],
        )
        .expect("seal point plan");
        plan.validate(&placement).expect("validate point plan");

        let mut stale = plan.clone();
        stale.descriptor_generation.push('1');
        stale.plan_sha256 = stale.calculated_sha256().expect("digest stale plan");
        assert!(stale.validate(&placement).is_err());

        let mut retrying = plan.clone();
        retrying.object_store_max_retries = 1;
        retrying.plan_sha256 = retrying.calculated_sha256().expect("digest retry plan");
        assert!(retrying.validate(&placement).is_err());

        let mut full_object = plan;
        full_object.operations[0].point.block.length =
            full_object.operations[0].point.block.object_length;
        full_object.operations[0].point.block.offset = 0;
        full_object.plan_sha256 = full_object.calculated_sha256().expect("digest full plan");
        assert!(full_object.validate(&placement).is_err());
    }

    #[test]
    fn measured_point_oracle_rejects_extra_attempt_and_full_object_poison() {
        let placement = placement();
        let plan = T28PointPlanV1::seal(
            &placement,
            T28CacheState::MetadataWarmDataCold,
            vec![(17, point(17))],
        )
        .expect("seal point plan");
        let operation = &plan.operations[0];
        let expected = super::base_record_at(7, 17, 256, 41)
            .value
            .expect("expected value");
        let read = PointRead {
            outcome: PointReadOutcome::Value(Bytes::from(expected)),
            data_bytes: operation.point.block.length,
        };
        let events = successful_events(operation);
        evaluate_measured_point(operation, &read, "candidate", "gcs", &events)
            .expect("evaluate measured point");

        let mut retry = events.clone();
        retry.extend(events.clone());
        assert!(evaluate_measured_point(operation, &read, "candidate", "gcs", &retry).is_err());

        let mut full_object = events;
        full_object[0].requested_range = Some(0..operation.point.block.object_length);
        assert!(
            evaluate_measured_point(operation, &read, "candidate", "gcs", &full_object).is_err()
        );
    }

    #[test]
    fn measured_point_oracle_rejects_value_and_provider_substitution() {
        let placement = placement();
        let plan = T28PointPlanV1::seal(
            &placement,
            T28CacheState::EmptyReader,
            vec![(17, point(17))],
        )
        .expect("seal point plan");
        let operation = &plan.operations[0];
        let wrong = PointRead {
            outcome: PointReadOutcome::Value(Bytes::from_static(b"wrong")),
            data_bytes: operation.point.block.length,
        };
        let events = successful_events(operation);
        assert!(evaluate_measured_point(operation, &wrong, "candidate", "gcs", &events).is_err());

        let expected = super::base_record_at(7, 17, 256, 41)
            .value
            .expect("expected value");
        let read = PointRead {
            outcome: PointReadOutcome::Value(Bytes::from(expected)),
            data_bytes: operation.point.block.length,
        };
        assert!(
            evaluate_measured_point(operation, &read, "candidate", "filesystem", &events).is_err()
        );
    }

    #[test]
    fn expected_digest_is_independent_of_candidate_bytes() {
        let placement = placement();
        let plan = T28PointPlanV1::seal(
            &placement,
            T28CacheState::EmptyReader,
            vec![(17, point(17))],
        )
        .expect("seal point plan");
        let expected = super::base_record_at(7, 17, 256, 41)
            .value
            .expect("expected value");
        assert_eq!(
            plan.operations[0].expected_value_sha256,
            content_sha256(&expected)
        );
    }
}
