//! RFC-0049 C5v2 compaction-amplification and branch-reuse gates.

use super::columnar_aligned::{
    aligned_compaction_written_bytes, prepare_t28_aligned_columnar_records,
};
use super::t28_aligned::{T28AlignedChildV1, T28OpenedAlignedLayout};
use super::t28_typed::{capture_identity, numeric_generation, validate_history_against_oracle};
use super::{row_compaction_written_bytes, t28_typed_layout_profile, LogicalHistory};
use crate::t28_layout::{
    T28LayoutOracleV1, TypedLayoutObjectIdentityV1, TypedLayoutPlacementLocatorV1,
};
use bytes::Bytes;
use okv_object::{
    content_sha256, prefixed_backend, Backend, ObservedBackend, RequestStats, WriteCondition,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const SCHEMA_VERSION: u32 = 1;
const PHYSICAL_PLAN_SHA256: &str =
    "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61";
const AMPLIFICATION_RATIO_MAX_MILLIONTHS: u64 = 1_100_000;

/// One real-provider observation for C5v2 compaction writes and metadata-only
/// branch creation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedMediaGateReceiptV1 {
    pub schema_version: u32,
    pub source_commit: String,
    pub executable_sha256: String,
    pub cargo_lock_sha256: String,
    pub provider_id: String,
    pub provider_driver: String,
    pub parent_locator_envelope_sha256: String,
    pub parent_root_sha256: String,
    pub parent_candidate_closure_sha256: String,
    pub branch_locator: TypedLayoutPlacementLocatorV1,
    pub branch_root_puts: u64,
    pub branch_incremental_bytes: u64,
    pub branch_shared_bytes: u64,
    pub branch_child_object_puts: u64,
    pub branch_reused_exact_objects: bool,
    pub compaction_prefix: String,
    pub compaction_run_count: u64,
    pub compaction_object_puts: u64,
    pub compaction_written_bytes: u64,
    pub expected_compaction_written_bytes: u64,
    pub control_compaction_written_bytes: u64,
    pub compaction_write_ratio_millionths: u64,
    pub compaction_list_requests: u64,
    pub compacted_child: T28AlignedChildV1,
    pub expected_canonical_history_sha256: String,
    pub compacted_canonical_history_sha256: String,
    pub compacted_record_count: u64,
    pub compacted_live_row_count: u64,
    pub branch_gate_passed: bool,
    pub compaction_gate_passed: bool,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T28AlignedMediaGateReceiptV1 {
    /// Recompute all gates and the self-contained receipt digest.
    ///
    /// # Errors
    ///
    /// Returns an error for identity drift, hidden branch copies, provider
    /// accounting drift, failed reconstruction, or threshold violation.
    pub fn validate(&self) -> Result<(), String> {
        self.branch_locator.validate()?;
        let branch_gate_passed = self.branch_root_puts == 1
            && self.branch_incremental_bytes == self.branch_locator.root_length
            && self.branch_child_object_puts == 0
            && self.branch_shared_bytes > self.branch_incremental_bytes
            && self.branch_reused_exact_objects
            && self.branch_locator.root_sha256 == self.parent_root_sha256;
        let ratio = ratio_millionths(
            self.compaction_written_bytes,
            self.control_compaction_written_bytes,
        );
        let compaction_gate_passed = self.compaction_run_count == 6
            && self.compaction_object_puts == 24
            && self.compaction_written_bytes == self.expected_compaction_written_bytes
            && ratio == self.compaction_write_ratio_millionths
            && ratio <= AMPLIFICATION_RATIO_MAX_MILLIONTHS
            && self.compaction_list_requests == 0
            && self.compacted_child.closure_sha256.len() == 64
            && self.compacted_child.objects.len() == 4
            && self.compacted_canonical_history_sha256 == self.expected_canonical_history_sha256
            && self.compacted_record_count == 25_014
            && self.compacted_live_row_count == 15_742;
        let passed = branch_gate_passed && compaction_gate_passed;
        if self.schema_version != SCHEMA_VERSION
            || self.source_commit.len() != 40
            || !self
                .source_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.cargo_lock_sha256)
            || self.provider_id.is_empty()
            || self.provider_driver.is_empty()
            || !valid_sha256(&self.parent_locator_envelope_sha256)
            || !valid_sha256(&self.parent_root_sha256)
            || !valid_sha256(&self.parent_candidate_closure_sha256)
            || self.branch_locator.envelope_sha256 == self.parent_locator_envelope_sha256
            || self.compaction_prefix.is_empty()
            || self.compacted_child.closure_sha256 == self.parent_candidate_closure_sha256
            || self.branch_gate_passed != branch_gate_passed
            || self.compaction_gate_passed != compaction_gate_passed
            || self.passed != passed
            || !passed
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 C5v2 media-gate receipt".to_owned());
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

/// Create one metadata-only branch and one real C5v2 base, delta, and compacted
/// write set against the supplied provider.
///
/// # Errors
///
/// Returns an error for parent drift, logical-oracle mismatch, hidden branch
/// copies, immutable-write failure, accounting drift, or reconstruction drift.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_t28_aligned_media_gates(
    backend: Arc<dyn Backend>,
    parent_locator: &TypedLayoutPlacementLocatorV1,
    oracle: &T28LayoutOracleV1,
    oracle_sha256: &str,
    branch_prefix: &str,
    compaction_prefix: &str,
    source_commit: String,
    executable_sha256: String,
    cargo_lock_sha256: String,
) -> Result<T28AlignedMediaGateReceiptV1, String> {
    parent_locator.validate()?;
    oracle.validate()?;
    if oracle_sha256
        != content_sha256(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../evals/oracles/t28-layout-geometry-v1-oracle.json"
        )))
        || parent_locator.prefix == branch_prefix
        || parent_locator.prefix == compaction_prefix
        || branch_prefix == compaction_prefix
    {
        return Err("invalid RFC-0049 media-gate boundary".to_owned());
    }

    let provider = backend.descriptor();
    let parent = T28OpenedAlignedLayout::open(Arc::clone(&backend), parent_locator).await?;
    let parent_fixture = parent.fixture().clone();
    if parent_fixture.oracle_sha256 != oracle_sha256
        || parent_fixture.physical_plan_sha256 != PHYSICAL_PLAN_SHA256
        || parent_fixture.canonical_history_sha256 != oracle.fixture.canonical_history_sha256
    {
        return Err("RFC-0049 media-gate parent differs from the oracle".to_owned());
    }

    let profile = t28_typed_layout_profile();
    let history = LogicalHistory::generate(&profile, oracle.fixture.seed)?;
    validate_history_against_oracle(&history, oracle)?;

    let observed = Arc::new(ObservedBackend::new(backend));
    let measured: Arc<dyn Backend> = observed.clone();

    let root = serde_json::to_vec(&parent_fixture).map_err(|error| error.to_string())?;
    let root_length = u64::try_from(root.len()).unwrap_or(u64::MAX);
    let root_object_sha256 = content_sha256(&root);
    let branch_root_key = format!("{branch_prefix}/roots/sha256/{root_object_sha256}.json");
    observed.clear_stats();
    let branch_revision = measured
        .put(&branch_root_key, Bytes::from(root), WriteCondition::Create)
        .await
        .map_err(|error| error.to_string())?;
    let branch_stats = observed.stats();
    let branch_root_puts = request_count(&branch_stats, "put.create");
    let branch_incremental_bytes = request_bytes(&branch_stats, "put.create");
    let branch_child_object_puts = branch_root_puts.saturating_sub(1);
    let branch_locator = TypedLayoutPlacementLocatorV1::seal(
        parent_fixture.fixture_id.clone(),
        parent_fixture.root_sha256.clone(),
        parent_fixture.project.clone(),
        parent_fixture.bucket.clone(),
        parent_fixture.region.clone(),
        branch_prefix.to_owned(),
        branch_root_key,
        numeric_generation(&branch_revision)?,
        root_length,
        root_object_sha256,
    )?;
    let branch = T28OpenedAlignedLayout::open(Arc::clone(&measured), &branch_locator).await?;
    let branch_reused_exact_objects = branch.fixture() == &parent_fixture;
    let branch_shared_bytes = parent_fixture
        .source_c0
        .objects
        .iter()
        .chain(&parent_fixture.candidate.objects)
        .fold(0_u64, |total, object| total.saturating_add(object.length));

    observed.clear_stats();
    let compaction_root = prefixed_backend(measured, compaction_prefix.to_owned())
        .map_err(|error| error.to_string())?;
    let base_backend = prefixed_backend(Arc::clone(&compaction_root), "base-v1".to_owned())
        .map_err(|error| error.to_string())?;
    prepare_t28_aligned_columnar_records(&profile, &history.base_records, base_backend.as_ref())
        .await?;
    let mut compaction_run_count = 1_u64;
    for (index, delta) in history.delta_records.iter().enumerate() {
        if delta.is_empty() {
            continue;
        }
        let version = profile
            .base_version
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX))
            .saturating_add(1);
        let delta_backend =
            prefixed_backend(Arc::clone(&compaction_root), format!("delta-v{version}"))
                .map_err(|error| error.to_string())?;
        prepare_t28_aligned_columnar_records(&profile, delta, delta_backend.as_ref()).await?;
        compaction_run_count = compaction_run_count.saturating_add(1);
    }
    let compacted_version = profile.base_version.saturating_add(profile.delta_cycles);
    let compacted_suffix = format!("compacted-v{compacted_version}");
    let compacted_backend =
        prefixed_backend(Arc::clone(&compaction_root), compacted_suffix.clone())
            .map_err(|error| error.to_string())?;
    let compacted_media = prepare_t28_aligned_columnar_records(
        &profile,
        &history.records,
        compacted_backend.as_ref(),
    )
    .await?;
    compaction_run_count = compaction_run_count.saturating_add(1);
    let compaction_stats = observed.stats();
    let compaction_object_puts = request_count(&compaction_stats, "put.create");
    let compaction_written_bytes = request_bytes(&compaction_stats, "put.create");
    let compaction_list_requests = request_count(&compaction_stats, "list");

    let mut objects: Vec<TypedLayoutObjectIdentityV1> = Vec::with_capacity(4);
    for (key, role) in compacted_media {
        objects.push(capture_identity(compacted_backend.as_ref(), &key, role).await?);
    }
    objects.sort_by(|left, right| {
        (left.key.as_str(), left.role).cmp(&(right.key.as_str(), right.role))
    });
    let compacted_child = T28AlignedChildV1::seal(
        format!("{compaction_prefix}/{compacted_suffix}"),
        parent_fixture.bucket.clone(),
        history.canonical_sha256.clone(),
        oracle.schema_sha256.clone(),
        oracle.fixture.covered_through_version,
        objects,
    )?;
    let compacted_reader = super::t28_aligned::T28AlignedLayoutReader::open(
        compacted_backend,
        &compacted_child,
        oracle.fixture.covered_through_version,
    )
    .await?;
    let compacted = compacted_reader.recover_complete_closure().await?;

    let expected_compaction_written_bytes = aligned_compaction_written_bytes(&profile, &history)?;
    let control_compaction_written_bytes = row_compaction_written_bytes(&profile, &history)?;
    let compaction_write_ratio_millionths =
        ratio_millionths(compaction_written_bytes, control_compaction_written_bytes);
    let branch_gate_passed = branch_root_puts == 1
        && branch_incremental_bytes == root_length
        && branch_child_object_puts == 0
        && branch_shared_bytes > branch_incremental_bytes
        && branch_reused_exact_objects;
    let compaction_gate_passed = compaction_run_count == 6
        && compaction_object_puts == 24
        && compaction_written_bytes == expected_compaction_written_bytes
        && compaction_write_ratio_millionths <= AMPLIFICATION_RATIO_MAX_MILLIONTHS
        && compaction_list_requests == 0
        && compacted.canonical_history_sha256 == history.canonical_sha256;
    let mut receipt = T28AlignedMediaGateReceiptV1 {
        schema_version: SCHEMA_VERSION,
        source_commit,
        executable_sha256,
        cargo_lock_sha256,
        provider_id: provider.id,
        provider_driver: provider.driver,
        parent_locator_envelope_sha256: parent_locator.envelope_sha256.clone(),
        parent_root_sha256: parent_fixture.root_sha256,
        parent_candidate_closure_sha256: parent_fixture.candidate.closure_sha256,
        branch_locator,
        branch_root_puts,
        branch_incremental_bytes,
        branch_shared_bytes,
        branch_child_object_puts,
        branch_reused_exact_objects,
        compaction_prefix: compaction_prefix.to_owned(),
        compaction_run_count,
        compaction_object_puts,
        compaction_written_bytes,
        expected_compaction_written_bytes,
        control_compaction_written_bytes,
        compaction_write_ratio_millionths,
        compaction_list_requests,
        compacted_child,
        expected_canonical_history_sha256: history.canonical_sha256,
        compacted_canonical_history_sha256: compacted.canonical_history_sha256,
        compacted_record_count: compacted.record_count,
        compacted_live_row_count: compacted.live_row_count,
        branch_gate_passed,
        compaction_gate_passed,
        passed: branch_gate_passed && compaction_gate_passed,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

fn request_count(stats: &RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.api == api && request.result == "ok")
        .fold(0_u64, |total, request| total.saturating_add(request.count))
}

fn request_bytes(stats: &RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.api == api && request.result == "ok")
        .fold(0_u64, |total, request| {
            total.saturating_add(request.request_bytes)
        })
}

fn ratio_millionths(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return u64::MAX;
    }
    numerator
        .saturating_mul(1_000_000)
        .checked_div(denominator)
        .unwrap_or(u64::MAX)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_uses_frozen_millionths_scale() {
        assert_eq!(ratio_millionths(11, 10), 1_100_000);
        assert_eq!(ratio_millionths(10, 0), u64::MAX);
    }
}
