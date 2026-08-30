//! RFC-0049 complete C5v2 child-closure recovery receipt.

use crate::provider_attempt::{
    ProviderAttemptBackend, ProviderAttemptEventV1, ProviderAttemptPhase,
};
use crate::storage_layout::T28OpenedAlignedLayout;
use crate::t28_layout::{TypedLayoutObjectIdentityV1, TypedLayoutPlacementLocatorV1};
use okv_object::{content_sha256, Backend, ObservedBackend, RequestStats};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

const SCHEMA_VERSION: u32 = 1;
const ORACLE_SHA256: &str = "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86";
const PHYSICAL_PLAN_SHA256: &str =
    "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61";
const PHYSICAL_ORACLE_SHA256: &str =
    "f2c2417eea48aa9c30e0c15554e5edb14aaff078e00cd2133066be3a21853b65";
const CANONICAL_HISTORY_SHA256: &str =
    "d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4";

/// One provider operation aggregate retained inside the recovery receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedRecoveryProviderRequestV1 {
    pub api: String,
    pub result: String,
    pub count: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub elapsed_micros: u64,
}

/// Exact empty-worker recovery result for one generation-pinned C5v2 child.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedClosureRecoveryReceiptV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub root_sha256: String,
    pub root_generation: String,
    pub candidate_closure_sha256: String,
    pub candidate_objects: Vec<TypedLayoutObjectIdentityV1>,
    pub oracle_sha256: String,
    pub physical_oracle_sha256: String,
    pub physical_plan_sha256: String,
    pub source_commit: String,
    pub executable_sha256: String,
    pub cargo_lock_sha256: String,
    pub process_id: u32,
    pub linux_boot_id: String,
    pub linux_process_start_ticks: u64,
    pub expected_canonical_history_sha256: String,
    pub recovered_canonical_history_sha256: String,
    pub expected_record_count: u64,
    pub recovered_record_count: u64,
    pub expected_live_row_count: u64,
    pub recovered_live_row_count: u64,
    pub group_count: u64,
    pub projection_proofs_verified: u64,
    pub payload_proofs_verified: u64,
    pub root_bytes: u64,
    pub manifest_bytes: u64,
    pub index_bytes: u64,
    pub projection_bytes: u64,
    pub payload_bytes: u64,
    pub object_get_requests: u64,
    pub object_response_bytes: u64,
    pub range_get_requests: u64,
    pub list_requests: u64,
    pub write_requests: u64,
    pub provider_requests: Vec<T28AlignedRecoveryProviderRequestV1>,
    pub provider_attempts: Vec<ProviderAttemptEventV1>,
    pub elapsed_micros: u64,
    pub exact_history: bool,
    pub complete_inventory_read: bool,
    pub list_free: bool,
    pub read_only: bool,
    pub passed: bool,
    pub receipt_sha256: String,
}

impl T28AlignedClosureRecoveryReceiptV1 {
    /// Recompute the self-contained receipt digest and all derived gates.
    ///
    /// # Errors
    ///
    /// Returns an error for identity drift, incomplete recovery, unexpected
    /// object operations, or a changed receipt digest.
    pub fn validate(&self) -> Result<(), String> {
        let expected_response_bytes = self
            .root_bytes
            .saturating_add(self.manifest_bytes)
            .saturating_add(self.index_bytes)
            .saturating_add(self.projection_bytes)
            .saturating_add(self.payload_bytes);
        let exact_history = self.recovered_record_count == self.expected_record_count
            && self.recovered_live_row_count == self.expected_live_row_count
            && self.recovered_canonical_history_sha256 == self.expected_canonical_history_sha256;
        let complete_inventory_read = self.object_get_requests == 5
            && self.object_response_bytes == expected_response_bytes
            && self.root_bytes > 0
            && self.manifest_bytes > 0
            && self.index_bytes > 0
            && self.projection_bytes > 0
            && self.payload_bytes > 0;
        let list_free = self.list_requests == 0;
        let read_only = self.range_get_requests == 0 && self.write_requests == 0;
        let passed = exact_history && complete_inventory_read && list_free && read_only;
        let provider_ledger_exact = self.provider_requests.len() == 1
            && self.provider_requests[0].api == "get"
            && self.provider_requests[0].result == "ok"
            && self.provider_requests[0].count == 5
            && self.provider_requests[0].request_bytes == 0
            && self.provider_requests[0].response_bytes == expected_response_bytes;
        let provider_attempts_exact = self.provider_attempts.len() == 10
            && self
                .provider_attempts
                .iter()
                .filter(|event| event.phase == ProviderAttemptPhase::Started)
                .count()
                == 5
            && self
                .provider_attempts
                .iter()
                .filter(|event| {
                    event.phase == ProviderAttemptPhase::Completed
                        && event.api == "get"
                        && event.requested_range.is_none()
                        && event.result.as_deref() == Some("ok")
                        && event.returned_range.as_ref().is_some_and(|range| {
                            range.start == 0
                                && Some(range.end) == event.object_length
                                && range.end == event.response_payload_bytes
                        })
                })
                .count()
                == 5;
        let object_inventory_exact = expected_object(
            &self.candidate_objects,
            "layout/columnar-v2/active-manifest",
            1_028,
            "82d8dfa5f7f1741b8488238f33dc7508c86cbaae916880e0276c5f18736ffa71",
        ) && expected_object(
            &self.candidate_objects,
            "layout/columnar-v2/index.oki2",
            19_148,
            "cf358d542ab3a790a713317f66ffc88d2c395d538d162c9852c1ab2dd2477faa",
        ) && expected_object(
            &self.candidate_objects,
            "layout/columnar-v2/projection.okp2",
            1_701_414,
            "dd67841b2c27a935273478d202d3bb00a506a7fecf522241df369669bb98e24c",
        ) && expected_object(
            &self.candidate_objects,
            "layout/columnar-v2/payload.okv2",
            11_974_176,
            "2b01f7a8544b5f886dc1a02d6aacd8d96ecdb4310498e9156981194d7e11673d",
        );
        if self.schema_version != SCHEMA_VERSION
            || self.fixture_id.len() != 64
            || self.root_sha256.len() != 64
            || self.root_generation.is_empty()
            || self.candidate_closure_sha256.len() != 64
            || self.candidate_objects.len() != 4
            || self
                .candidate_objects
                .iter()
                .any(|object| object.validate().is_err())
            || self.oracle_sha256 != ORACLE_SHA256
            || self.physical_oracle_sha256 != PHYSICAL_ORACLE_SHA256
            || self.physical_plan_sha256 != PHYSICAL_PLAN_SHA256
            || self.source_commit.len() != 40
            || !valid_lower_hex(&self.source_commit)
            || !valid_sha256(&self.executable_sha256)
            || !valid_sha256(&self.cargo_lock_sha256)
            || self.process_id == 0
            || self.linux_boot_id.trim().is_empty()
            || self.linux_process_start_ticks == 0
            || self.expected_canonical_history_sha256.len() != 64
            || self.recovered_canonical_history_sha256.len() != 64
            || self.expected_canonical_history_sha256 != CANONICAL_HISTORY_SHA256
            || self.expected_record_count != 25_014
            || self.recovered_record_count != 25_014
            || self.expected_live_row_count != 15_742
            || self.recovered_live_row_count != 15_742
            || self.group_count != 792
            || self.projection_proofs_verified != self.group_count
            || self.payload_proofs_verified != self.group_count
            || self.manifest_bytes != 1_028
            || self.index_bytes != 19_148
            || self.projection_bytes != 1_701_414
            || self.payload_bytes != 11_974_176
            || !provider_ledger_exact
            || !provider_attempts_exact
            || !object_inventory_exact
            || self.exact_history != exact_history
            || self.complete_inventory_read != complete_inventory_read
            || self.list_free != list_free
            || self.read_only != read_only
            || self.passed != passed
            || !passed
            || self.receipt_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 complete-closure recovery receipt".to_owned());
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

/// Open one root in an empty process, read every C5v2 child object without
/// LIST, and reconstruct the exact canonical MVCC history.
///
/// # Errors
///
/// Returns an error for provider, generation, closure, history, or receipt
/// drift.
pub async fn run_t28_aligned_closure_recovery(
    backend: Arc<dyn Backend>,
    locator: &TypedLayoutPlacementLocatorV1,
    source_commit: String,
    executable_sha256: String,
    cargo_lock_sha256: String,
) -> Result<T28AlignedClosureRecoveryReceiptV1, String> {
    let attempts = Arc::new(ProviderAttemptBackend::new(
        backend,
        "c5v2_complete_closure_recovery",
    )?);
    let attempted_backend: Arc<dyn Backend> = attempts.clone();
    let observed = Arc::new(ObservedBackend::new(attempted_backend));
    let measured_backend: Arc<dyn Backend> = observed.clone();
    let started = Instant::now();
    let opened = T28OpenedAlignedLayout::open(measured_backend, locator).await?;
    let fixture = opened.fixture().clone();
    let reader = opened.c5v2().await?;
    let recovered = reader.recover_complete_closure().await?;
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let stats = observed.stats();
    let provider_requests = stats
        .requests
        .iter()
        .map(|stat| T28AlignedRecoveryProviderRequestV1 {
            api: stat.api.clone(),
            result: stat.result.clone(),
            count: stat.count,
            request_bytes: stat.request_bytes,
            response_bytes: stat.response_bytes,
            elapsed_micros: stat.elapsed_micros,
        })
        .collect::<Vec<_>>();
    let provider_attempts = attempts.events();
    let object_get_requests = request_count(&stats, "get");
    let object_response_bytes = response_bytes(&stats, "get");
    let range_get_requests = request_count(&stats, "get.range");
    let list_requests = request_count(&stats, "list");
    let write_requests = stats
        .requests
        .iter()
        .filter(|stat| stat.api.starts_with("put.") || stat.api.starts_with("delete"))
        .fold(0_u64, |total, stat| total.saturating_add(stat.count));
    let manifest_bytes = fixture
        .candidate
        .objects
        .iter()
        .find(|object| object.key.ends_with("active-manifest"))
        .map_or(0, |object| object.length);
    let index_bytes = fixture
        .candidate
        .objects
        .iter()
        .find(|object| object.key.ends_with("index.oki2"))
        .map_or(0, |object| object.length);
    let exact_history = recovered.record_count == fixture.record_count
        && recovered.live_row_count == fixture.live_row_count
        && recovered.canonical_history_sha256 == fixture.canonical_history_sha256;
    let expected_response_bytes = locator
        .root_length
        .saturating_add(fixture.candidate.total_bytes());
    let complete_inventory_read = object_get_requests == 5
        && object_response_bytes == expected_response_bytes
        && manifest_bytes > 0
        && index_bytes > 0;
    let list_free = list_requests == 0;
    let read_only = range_get_requests == 0 && write_requests == 0;
    let linux_boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|error| error.to_string())?
        .trim()
        .to_owned();
    let linux_process_start_ticks = linux_process_start_ticks()?;
    let mut receipt = T28AlignedClosureRecoveryReceiptV1 {
        schema_version: SCHEMA_VERSION,
        fixture_id: fixture.fixture_id,
        root_sha256: fixture.root_sha256,
        root_generation: locator.root_generation.clone(),
        candidate_closure_sha256: fixture.candidate.closure_sha256,
        candidate_objects: fixture.candidate.objects,
        oracle_sha256: fixture.oracle_sha256,
        physical_oracle_sha256: content_sha256(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../evals/oracles/t28-aligned-columnar-v2-plan.json"
        ))),
        physical_plan_sha256: fixture.physical_plan_sha256,
        source_commit,
        executable_sha256,
        cargo_lock_sha256,
        process_id: std::process::id(),
        linux_boot_id,
        linux_process_start_ticks,
        expected_canonical_history_sha256: fixture.canonical_history_sha256,
        recovered_canonical_history_sha256: recovered.canonical_history_sha256,
        expected_record_count: fixture.record_count,
        recovered_record_count: recovered.record_count,
        expected_live_row_count: fixture.live_row_count,
        recovered_live_row_count: recovered.live_row_count,
        group_count: recovered.group_count,
        projection_proofs_verified: recovered.projection_proofs_verified,
        payload_proofs_verified: recovered.payload_proofs_verified,
        root_bytes: locator.root_length,
        manifest_bytes,
        index_bytes,
        projection_bytes: recovered.projection_bytes,
        payload_bytes: recovered.payload_bytes,
        object_get_requests,
        object_response_bytes,
        range_get_requests,
        list_requests,
        write_requests,
        provider_requests,
        provider_attempts,
        elapsed_micros,
        exact_history,
        complete_inventory_read,
        list_free,
        read_only,
        passed: exact_history && complete_inventory_read && list_free && read_only,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.calculated_sha256()?;
    receipt.validate()?;
    Ok(receipt)
}

fn linux_process_start_ticks() -> Result<u64, String> {
    let stat = std::fs::read_to_string("/proc/self/stat").map_err(|error| error.to_string())?;
    let (_, fields) = stat
        .rsplit_once(')')
        .ok_or_else(|| "Linux process stat has no command boundary".to_owned())?;
    fields
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| "Linux process stat omits start ticks".to_owned())?
        .parse::<u64>()
        .map_err(|error| error.to_string())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && valid_lower_hex(value)
}

fn valid_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn expected_object(
    objects: &[TypedLayoutObjectIdentityV1],
    key: &str,
    length: u64,
    sha256: &str,
) -> bool {
    objects
        .iter()
        .any(|object| object.key == key && object.length == length && object.sha256 == sha256)
}

fn request_count(stats: &RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|stat| stat.api == api)
        .fold(0_u64, |total, stat| total.saturating_add(stat.count))
}

fn response_bytes(stats: &RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|stat| stat.api == api)
        .fold(0_u64, |total, stat| {
            total.saturating_add(stat.response_bytes)
        })
}
