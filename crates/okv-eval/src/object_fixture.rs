//! RFC-0044 content-addressed object fixture, exact tail, and resident identity contract.

use crate::fixture_anchor::establish_fixture_anchor;
use bytes::Bytes;
use okv_consensus::{
    RequestIdentity, RetainedTransactionReadRequest, RetainedTransactionRecord,
    TransactionAuthorityProcessFixture, TransactionBatchItem, TransactionLogClient,
    TransactionLogStorageStatsRequest, TransactionMutation,
};
use okv_object::{
    content_sha256, decode_full_row_object, encode_row_object_set, filesystem_backend, Backend,
    FaultBackend, ObjectClient, ObservedBackend, PutOutcome, RowObjectManifestV1,
    RowObjectReference, RowRecord, RowSegmentIndex,
};
use okv_transaction::{KeyRange, TransactionCommand, TransactionStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const GENERATION: u64 = 7;
const FIXTURE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_GENERATOR_VERSION: u32 = 1;
const ROW_OBJECT_FORMAT_VERSION: u32 = 1;
const RESIDENT_IMAGE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_MAGIC: &[u8] = b"OKVF1";
const FIXTURE_PLACEMENT_MAGIC: &[u8] = b"OKVFP1";
const TAIL_MAGIC: &[u8] = b"OKVFT1";
const LOGICAL_IMAGE_MAGIC: &[u8] = b"OKVLI1";
const RESIDENT_IMAGE_MAGIC: &[u8] = b"OKVRI1";
const CONTENT_ROOT: &str = "fixtures/single-range/v1/blobs/sha256";
const DESCRIPTOR_ROOT: &str = "fixtures/single-range/v1/descriptors";
const EXPECTED_TAIL_RECORDS: usize = 7;

/// Subject or deliberate poison for the RFC-0044 local contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFixtureMode {
    Candidate,
    CorruptDescriptorPoison,
    MutatedAnchorPoison,
    TailMismatchPoison,
    SharedMutableImagePoison,
}

impl ObjectFixtureMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::CorruptDescriptorPoison => "corrupt_descriptor_poison",
            Self::MutatedAnchorPoison => "mutated_anchor_poison",
            Self::TailMismatchPoison => "tail_mismatch_poison",
            Self::SharedMutableImagePoison => "shared_mutable_image_poison",
        }
    }
}

/// Fixed local fixture shape for the RFC-0044 contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectFixtureProfile {
    pub key_count: u64,
    pub value_bytes: usize,
    pub target_object_bytes: usize,
    pub target_block_bytes: usize,
}

impl ObjectFixtureProfile {
    fn validate(&self) -> Result<(), String> {
        if self.key_count <= 16
            || self.value_bytes == 0
            || self.target_block_bytes < 4_096
            || self.target_object_bytes < self.target_block_bytes
        {
            return Err("invalid RFC-0044 object fixture profile".to_owned());
        }
        Ok(())
    }
}

/// Exact manifest object named by one fixture descriptor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureManifestIdentityV1 {
    pub key: String,
    pub length: u64,
    pub sha256: String,
}

/// Semantic identity of one immutable logical object fixture.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectFixtureDescriptorV1 {
    pub schema_version: u32,
    pub generator_version: u32,
    pub seed: u64,
    pub key_count: u64,
    pub value_bytes: u64,
    pub logical_bytes: u64,
    pub logical_key_value_sha256: String,
    pub base_version: u64,
    pub row_object_format_version: u32,
    pub target_object_bytes: u64,
    pub target_block_bytes: u64,
    pub manifest: FixtureManifestIdentityV1,
    pub closure_sha256: String,
    pub object_count: u64,
    pub object_bytes: u64,
}

impl ObjectFixtureDescriptorV1 {
    /// Return the lowercase SHA-256 of the frozen `OKVF1` field encoding.
    #[must_use]
    pub fn fixture_id(&self) -> String {
        content_sha256(&encode_fixture_identity(self))
    }
}

/// Semantic identity of one subject-local resident image.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentImageDescriptorV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub tail_sha256: String,
    pub subject: String,
    pub engine_provider: String,
    pub engine_format_version: u32,
    pub options_sha256: String,
    pub applied_through: u64,
    pub record_count: u64,
    pub resident_logical_sha256: String,
}

impl ResidentImageDescriptorV1 {
    #[must_use]
    pub fn resident_image_id(&self) -> String {
        content_sha256(&encode_resident_identity(self))
    }
}

/// RFC-0044 phase-1 local contract evidence.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectFixtureReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: ObjectFixtureMode,
    pub release_build: bool,
    pub fixture_id: String,
    pub fixture_descriptor_sha256: String,
    pub fixture_reused: bool,
    pub immutable_put_reuse_verified: bool,
    pub fixture_verification_seconds: f64,
    pub fixture_object_requests: u64,
    pub fixture_object_bytes: u64,
    pub base_anchor_version: u64,
    pub anchor_txlog_records: u64,
    pub anchor_txlog_mutations: u64,
    pub anchor_live_keys: u64,
    pub base_value_txlog_records: u64,
    pub base_value_txlog_mutation_bytes: u64,
    pub tail_records: u64,
    pub tail_sha256: String,
    pub native_resident_image_id: String,
    pub control_resident_image_id: String,
    pub resident_logical_sha256: String,
    pub resident_image_build_seconds: f64,
    pub resident_image_local_bytes: u64,
    pub resident_checkpoint_sha256: String,
    pub object_count: u64,
    pub object_bytes: u64,
    pub decoded_base_records: u64,
    pub all_base_records_at_anchor: bool,
    pub all_segment_versions_at_anchor: bool,
    pub subject_roots_distinct: bool,
    pub descriptor_deterministic: bool,
    pub tail_exact: bool,
    pub resident_images_distinct: bool,
    pub resident_logical_images_equal: bool,
    pub poison_detected: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ClosureObjectIdentity {
    key: String,
    length: u64,
    sha256: String,
}

pub(crate) struct BuiltFixture {
    pub descriptor: ObjectFixtureDescriptorV1,
    pub fixture_id: String,
    pub descriptor_sha256: String,
    pub descriptor_bytes: Vec<u8>,
    pub reused: bool,
}

/// Exact descriptor identity used to reopen one persisted fixture without
/// regenerating or rewriting its logical base.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectFixtureLocatorV1 {
    pub fixture_id: String,
    pub descriptor_length: u64,
    pub descriptor_sha256: String,
}

/// Cross-invocation identity and placement of one immutable object fixture.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixturePlacementLocatorV1 {
    pub schema_version: u32,
    pub fixture: ObjectFixtureLocatorV1,
    pub base_version: u64,
    pub provider: String,
    pub bucket: String,
    pub prefix: String,
    pub descriptor_key: String,
    pub descriptor_generation: String,
    pub fixture_seed: u64,
    pub key_count: u64,
    pub value_bytes: u64,
    pub logical_bytes: u64,
    pub generator_version: u32,
    pub row_object_format_version: u32,
    pub target_object_bytes: u64,
    pub target_block_bytes: u64,
    pub source_sha256: String,
    pub suite_sha256: String,
    pub binary_sha256: String,
    pub cargo_lock_sha256: String,
    pub envelope_sha256: String,
}

impl FixturePlacementLocatorV1 {
    /// Calculate the canonical locator-envelope digest without trusting its
    /// serialized `envelope_sha256` field.
    #[must_use]
    pub fn calculated_envelope_sha256(&self) -> String {
        content_sha256(&encode_fixture_placement_identity(self))
    }

    /// Validate semantic identity, GCS placement, frozen profile, and build
    /// envelope fields.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity field is absent, malformed, or
    /// internally inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != FIXTURE_SCHEMA_VERSION
            || self.base_version == 0
            || self.provider != "gcs"
            || !valid_bucket(&self.bucket)
            || !valid_prefix(&self.prefix)
            || self.fixture_seed == 0
            || self.key_count == 0
            || self.value_bytes == 0
            || self.logical_bytes != self.key_count.saturating_mul(self.value_bytes)
            || self.generator_version != FIXTURE_GENERATOR_VERSION
            || self.row_object_format_version != ROW_OBJECT_FORMAT_VERSION
            || self.target_block_bytes < 4_096
            || self.target_object_bytes < self.target_block_bytes
            || self.fixture.descriptor_length == 0
            || !valid_sha256(&self.fixture.fixture_id)
            || !valid_sha256(&self.fixture.descriptor_sha256)
            || self.descriptor_key != descriptor_key(&self.fixture.fixture_id)
            || self.descriptor_generation.is_empty()
            || !self
                .descriptor_generation
                .bytes()
                .all(|byte| byte.is_ascii_digit())
            || !valid_sha256(&self.source_sha256)
            || !valid_sha256(&self.suite_sha256)
            || !valid_sha256(&self.binary_sha256)
            || !valid_sha256(&self.cargo_lock_sha256)
            || !valid_sha256(&self.envelope_sha256)
            || self.envelope_sha256 != self.calculated_envelope_sha256()
        {
            return Err("invalid RFC-0044 fixture placement locator".to_owned());
        }
        Ok(())
    }
}

/// Decode one locator and require an independently supplied envelope digest.
///
/// # Errors
///
/// Returns an error for malformed JSON, an invalid locator, or a mismatch with
/// the expected envelope identity.
pub fn decode_fixture_placement_locator(
    bytes: &[u8],
    expected_envelope_sha256: &str,
) -> Result<FixturePlacementLocatorV1, String> {
    if !valid_sha256(expected_envelope_sha256) {
        return Err("expected fixture locator envelope SHA-256 is invalid".to_owned());
    }
    let locator: FixturePlacementLocatorV1 =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    locator.validate()?;
    if locator.envelope_sha256 != expected_envelope_sha256 {
        return Err("fixture locator envelope identity mismatch".to_owned());
    }
    Ok(locator)
}

impl BuiltFixture {
    #[must_use]
    pub(crate) fn locator(&self) -> ObjectFixtureLocatorV1 {
        ObjectFixtureLocatorV1 {
            fixture_id: self.fixture_id.clone(),
            descriptor_length: u64::try_from(self.descriptor_bytes.len()).unwrap_or(u64::MAX),
            descriptor_sha256: self.descriptor_sha256.clone(),
        }
    }
}

struct VerifiedFixture {
    descriptor: ObjectFixtureDescriptorV1,
    records: Vec<RowRecord>,
    segment_versions_at_anchor: bool,
    verification_seconds: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LogicalOutcome {
    Value(Vec<u8>),
    Tombstone,
    Absent,
}

/// Run the local descriptor, closure, tail, identity, and poison contract.
///
/// # Errors
///
/// Returns an error when profile validation, process topology, immutable
/// storage, transaction ordering, or a semantic invariant fails.
pub fn run_object_fixture_contract(
    seed: u64,
    mode: ObjectFixtureMode,
    profile: &ObjectFixtureProfile,
    executable: &Path,
) -> Result<ObjectFixtureReport, String> {
    profile.validate()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, profile, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: ObjectFixtureMode,
    profile: &ObjectFixtureProfile,
    executable: &Path,
) -> Result<ObjectFixtureReport, String> {
    let root = TempDir::new().map_err(|error| error.to_string())?;
    let object_root = root.path().join("objects");
    fs::create_dir_all(&object_root).map_err(|error| error.to_string())?;
    let stored = filesystem_backend(&object_root).map_err(|error| error.to_string())?;
    let fault = Arc::new(FaultBackend::new(stored));
    let observed = Arc::new(ObservedBackend::new(fault.clone()));
    let backend: Arc<dyn Backend> = observed.clone();
    let object_client = ObjectClient::new(backend.clone());

    let authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let transaction = authority.client()?;
    let anchor_identity = RequestIdentity {
        client_id: seed.max(1),
        request_id: 1,
    };
    let anchor = establish_fixture_anchor(&transaction, anchor_identity).await?;
    let base_records = base_records(seed, profile, anchor.version)?;
    let first = build_fixture(seed, profile, anchor.version, &base_records, &object_client).await?;
    let second =
        build_fixture(seed, profile, anchor.version, &base_records, &object_client).await?;
    let descriptor_deterministic = first.fixture_id == second.fixture_id
        && first.descriptor == second.descriptor
        && first.descriptor_bytes == second.descriptor_bytes;
    let immutable_put_reuse_verified = second.reused;

    let verified = verify_fixture(
        &backend,
        &first.fixture_id,
        first.descriptor_bytes.len(),
        &first.descriptor_sha256,
        anchor.version,
    )
    .await?;
    if verified.descriptor != first.descriptor || verified.records != base_records {
        return Err("verified object fixture differs from the generated fixture".to_owned());
    }

    let tail = commit_tail(seed, profile, anchor.version, &transaction).await?;
    let tail_digest = tail_sha256(&tail.records)?;
    validate_tail(&tail_digest, &tail.records)?;
    let all_records = transaction
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(tail.applied_through),
            max_records: 16,
        })
        .await?;
    if !all_records.complete || all_records.records.len() != EXPECTED_TAIL_RECORDS + 1 {
        return Err("fixture authority retained stream has the wrong record count".to_owned());
    }
    let anchor_records = all_records
        .records
        .iter()
        .filter(|record| record.commit_version == anchor.version)
        .collect::<Vec<_>>();
    let anchor_txlog_records = u64::try_from(anchor_records.len()).unwrap_or(u64::MAX);
    let anchor_txlog_mutations = anchor_records
        .iter()
        .map(|record| u64::try_from(record.command.mutations.len()).unwrap_or(u64::MAX))
        .sum::<u64>();
    let (base_value_txlog_records, base_value_txlog_mutation_bytes) =
        base_value_txlog_accounting(&all_records.records, &base_records);
    let authority_stats = transaction
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    if authority_stats.high_watermark != tail.applied_through
        || authority_stats.retained_records
            != u64::try_from(all_records.records.len()).unwrap_or(u64::MAX)
    {
        return Err("fixture authority storage stats disagree with the retained stream".to_owned());
    }

    let native_root = root.path().join("native-image");
    let control_root = root.path().join("control-image");
    fs::create_dir_all(&native_root).map_err(|error| error.to_string())?;
    fs::create_dir_all(&control_root).map_err(|error| error.to_string())?;
    let subject_roots_distinct = validate_subject_roots(&native_root, &control_root).is_ok();
    let image_started = Instant::now();
    let native_image = build_resident_image(
        "native_snapshot",
        "okv-rocksdb-resident-range-engine",
        1,
        "okv-native-resident-options-v1",
        &first.fixture_id,
        &tail_digest,
        tail.applied_through,
        profile,
        &verified.records,
        &tail.records,
    )?;
    let control_image = build_resident_image(
        "direct_owned_rocksdb",
        "rocksdb-owned-value-control",
        1,
        "okv-direct-owned-options-v1",
        &first.fixture_id,
        &tail_digest,
        tail.applied_through,
        profile,
        &verified.records,
        &tail.records,
    )?;
    let resident_image_build_seconds = image_started.elapsed().as_secs_f64();
    let resident_images_distinct =
        native_image.resident_image_id != control_image.resident_image_id;
    let resident_logical_images_equal = native_image.descriptor.resident_logical_sha256
        == control_image.descriptor.resident_logical_sha256;

    let poison_detected = match mode {
        ObjectFixtureMode::Candidate => true,
        ObjectFixtureMode::CorruptDescriptorPoison => {
            fault.corrupt_next_get();
            verify_fixture(
                &backend,
                &first.fixture_id,
                first.descriptor_bytes.len(),
                &first.descriptor_sha256,
                anchor.version,
            )
            .await
            .is_err()
        }
        ObjectFixtureMode::MutatedAnchorPoison => {
            let mut mutated = first.descriptor.clone();
            mutated.base_version = mutated.base_version.saturating_add(1);
            validate_descriptor(&mutated, anchor.version).is_err()
        }
        ObjectFixtureMode::TailMismatchPoison => {
            let mut mutated = tail.records.clone();
            if let Some(first_record) = mutated.first_mut() {
                first_record.commit_version = first_record.commit_version.saturating_add(1);
            }
            validate_tail(&tail_digest, &mutated).is_err()
        }
        ObjectFixtureMode::SharedMutableImagePoison => {
            validate_subject_roots(&native_root, &native_root).is_err()
        }
    };

    let all_base_records_at_anchor = verified
        .records
        .iter()
        .all(|record| record.version == anchor.version);
    let tail_exact = tail.records.len() == EXPECTED_TAIL_RECORDS
        && tail
            .records
            .iter()
            .all(|record| record.commit_version > anchor.version)
        && tail_digest == tail_sha256(&tail.records)?;
    let exact = authority.process_count() == 3
        && anchor_txlog_records == 1
        && anchor_txlog_mutations == 0
        && anchor.live_keys == 0
        && base_value_txlog_records == 0
        && base_value_txlog_mutation_bytes == 0
        && descriptor_deterministic
        && immutable_put_reuse_verified
        && verified.records.len() == usize::try_from(profile.key_count).unwrap_or(usize::MAX)
        && all_base_records_at_anchor
        && verified.segment_versions_at_anchor
        && subject_roots_distinct
        && tail_exact
        && resident_images_distinct
        && resident_logical_images_equal
        && poison_detected;
    let correctness_anomalies = u64::from(!exact);
    let object_stats = observed.stats();
    let fixture_object_requests = object_stats
        .requests
        .iter()
        .map(|request| request.count)
        .sum::<u64>();
    let stable = (
        seed,
        mode.id(),
        &first.fixture_id,
        &first.descriptor_sha256,
        anchor.version,
        anchor_txlog_records,
        anchor_txlog_mutations,
        anchor.live_keys,
        base_value_txlog_records,
        base_value_txlog_mutation_bytes,
        &tail_digest,
        &native_image.resident_image_id,
        &control_image.resident_image_id,
        &native_image.descriptor.resident_logical_sha256,
        exact,
        correctness_anomalies,
    );
    let semantic_sha256 =
        content_sha256(&serde_json::to_vec(&stable).map_err(|error| error.to_string())?);

    Ok(ObjectFixtureReport {
        format_version: 1,
        seed,
        mode,
        release_build: !cfg!(debug_assertions),
        fixture_id: first.fixture_id,
        fixture_descriptor_sha256: first.descriptor_sha256,
        // A temporary local filesystem run cannot claim persisted cross-subject reuse.
        fixture_reused: false,
        immutable_put_reuse_verified,
        fixture_verification_seconds: verified.verification_seconds,
        fixture_object_requests,
        fixture_object_bytes: first.descriptor.object_bytes,
        base_anchor_version: anchor.version,
        anchor_txlog_records,
        anchor_txlog_mutations,
        anchor_live_keys: anchor.live_keys,
        base_value_txlog_records,
        base_value_txlog_mutation_bytes,
        tail_records: u64::try_from(tail.records.len()).unwrap_or(u64::MAX),
        tail_sha256: tail_digest,
        native_resident_image_id: native_image.resident_image_id,
        control_resident_image_id: control_image.resident_image_id,
        resident_logical_sha256: native_image.descriptor.resident_logical_sha256,
        resident_image_build_seconds,
        resident_image_local_bytes: directory_bytes(&native_root)?
            .saturating_add(directory_bytes(&control_root)?),
        resident_checkpoint_sha256: empty_checkpoint_sha256(&native_root, &control_root)?,
        object_count: first.descriptor.object_count,
        object_bytes: first.descriptor.object_bytes,
        decoded_base_records: u64::try_from(verified.records.len()).unwrap_or(u64::MAX),
        all_base_records_at_anchor,
        all_segment_versions_at_anchor: verified.segment_versions_at_anchor,
        subject_roots_distinct,
        descriptor_deterministic,
        tail_exact,
        resident_images_distinct,
        resident_logical_images_equal,
        poison_detected,
        correctness_anomalies,
        semantic_sha256,
    })
}

struct TailFixture {
    records: Vec<RetainedTransactionRecord>,
    applied_through: u64,
}

struct BuiltResidentImage {
    descriptor: ResidentImageDescriptorV1,
    resident_image_id: String,
}

pub(crate) async fn build_fixture(
    seed: u64,
    profile: &ObjectFixtureProfile,
    base_version: u64,
    records: &[RowRecord],
    client: &ObjectClient,
) -> Result<BuiltFixture, String> {
    let encoded = encode_row_object_set(
        GENERATION,
        records,
        profile.target_object_bytes,
        profile.target_block_bytes,
    )?;
    let mut references = Vec::with_capacity(encoded.len());
    let mut objects = Vec::with_capacity(encoded.len().saturating_mul(2).saturating_add(1));
    for segment in &encoded {
        let mut reference = RowObjectReference::from_encoded("fixture-build", segment)?;
        reference.data_key = content_key(&reference.data_sha256);
        reference.index_key = content_key(&reference.index_sha256);
        objects.push((reference.data_key.clone(), segment.data.clone()));
        objects.push((reference.index_key.clone(), segment.index.clone()));
        references.push(reference);
    }
    let manifest = RowObjectManifestV1::new(GENERATION, base_version, references)?;
    let manifest_bytes = manifest.encode()?;
    let manifest_sha256 = content_sha256(&manifest_bytes);
    let manifest_key = content_key(&manifest_sha256);
    objects.push((manifest_key.clone(), Bytes::from(manifest_bytes.clone())));
    objects.sort_by(|left, right| left.0.cmp(&right.0));

    let closure = objects
        .iter()
        .map(|(key, bytes)| ClosureObjectIdentity {
            key: key.clone(),
            length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            sha256: content_sha256(bytes),
        })
        .collect::<Vec<_>>();
    let object_bytes = closure.iter().map(|object| object.length).sum::<u64>();
    let descriptor = ObjectFixtureDescriptorV1 {
        schema_version: FIXTURE_SCHEMA_VERSION,
        generator_version: FIXTURE_GENERATOR_VERSION,
        seed,
        key_count: profile.key_count,
        value_bytes: u64::try_from(profile.value_bytes).unwrap_or(u64::MAX),
        logical_bytes: records.iter().fold(0_u64, |bytes, record| {
            bytes.saturating_add(
                record
                    .value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            )
        }),
        logical_key_value_sha256: logical_key_value_sha256(records)?,
        base_version,
        row_object_format_version: ROW_OBJECT_FORMAT_VERSION,
        target_object_bytes: u64::try_from(profile.target_object_bytes).unwrap_or(u64::MAX),
        target_block_bytes: u64::try_from(profile.target_block_bytes).unwrap_or(u64::MAX),
        manifest: FixtureManifestIdentityV1 {
            key: manifest_key,
            length: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
            sha256: manifest_sha256,
        },
        closure_sha256: closure_sha256(&closure),
        object_count: u64::try_from(closure.len()).unwrap_or(u64::MAX),
        object_bytes,
    };
    validate_descriptor(&descriptor, base_version)?;
    let fixture_id = descriptor.fixture_id();
    let mut all_existing = true;
    for (key, bytes) in objects {
        let (outcome, _) = client
            .put_if_absent(&key, bytes)
            .await
            .map_err(|error| error.to_string())?;
        all_existing &= outcome == PutOutcome::ExistingIdentical;
    }
    verify_closure(client, &descriptor).await?;
    let descriptor_bytes = serde_json::to_vec(&descriptor).map_err(|error| error.to_string())?;
    let descriptor_sha256 = content_sha256(&descriptor_bytes);
    let descriptor_key = descriptor_key(&fixture_id);
    let (descriptor_outcome, _) = client
        .put_if_absent(&descriptor_key, Bytes::from(descriptor_bytes.clone()))
        .await
        .map_err(|error| error.to_string())?;
    Ok(BuiltFixture {
        descriptor,
        fixture_id,
        descriptor_sha256,
        descriptor_bytes,
        reused: all_existing && descriptor_outcome == PutOutcome::ExistingIdentical,
    })
}

async fn verify_fixture(
    backend: &Arc<dyn Backend>,
    fixture_id: &str,
    descriptor_length: usize,
    descriptor_sha256: &str,
    anchor_version: u64,
) -> Result<VerifiedFixture, String> {
    let started = Instant::now();
    let descriptor_read = backend
        .get(&descriptor_key(fixture_id), None, None)
        .await
        .map_err(|error| error.to_string())?;
    if descriptor_read.returned_range != (0..u64::try_from(descriptor_length).unwrap_or(u64::MAX))
        || descriptor_read.object_length != u64::try_from(descriptor_length).unwrap_or(u64::MAX)
        || content_sha256(&descriptor_read.bytes) != descriptor_sha256
    {
        return Err("fixture descriptor content identity mismatch".to_owned());
    }
    let descriptor: ObjectFixtureDescriptorV1 =
        serde_json::from_slice(&descriptor_read.bytes).map_err(|error| error.to_string())?;
    validate_descriptor(&descriptor, anchor_version)?;
    if descriptor.fixture_id() != fixture_id {
        return Err("fixture descriptor semantic identity mismatch".to_owned());
    }
    let client = ObjectClient::new(backend.clone());
    let (records, segment_versions_at_anchor) = verify_closure(&client, &descriptor).await?;
    Ok(VerifiedFixture {
        descriptor,
        records,
        segment_versions_at_anchor,
        verification_seconds: started.elapsed().as_secs_f64(),
    })
}

pub(crate) async fn verify_fixture_records(
    backend: &Arc<dyn Backend>,
    fixture_id: &str,
    descriptor_length: usize,
    descriptor_sha256: &str,
    anchor_version: u64,
) -> Result<Vec<RowRecord>, String> {
    Ok(verify_fixture(
        backend,
        fixture_id,
        descriptor_length,
        descriptor_sha256,
        anchor_version,
    )
    .await?
    .records)
}

pub(crate) async fn open_existing_fixture(
    backend: &Arc<dyn Backend>,
    locator: &ObjectFixtureLocatorV1,
    anchor_version: u64,
) -> Result<(BuiltFixture, Vec<RowRecord>, f64), String> {
    let descriptor_length = usize::try_from(locator.descriptor_length)
        .map_err(|_| "object fixture descriptor length exceeds usize".to_owned())?;
    let verified = verify_fixture(
        backend,
        &locator.fixture_id,
        descriptor_length,
        &locator.descriptor_sha256,
        anchor_version,
    )
    .await?;
    let descriptor_bytes =
        serde_json::to_vec(&verified.descriptor).map_err(|error| error.to_string())?;
    if descriptor_bytes.len() != descriptor_length
        || content_sha256(&descriptor_bytes) != locator.descriptor_sha256
    {
        return Err("reopened fixture descriptor identity changed after decoding".to_owned());
    }
    let fixture = BuiltFixture {
        descriptor: verified.descriptor,
        fixture_id: locator.fixture_id.clone(),
        descriptor_sha256: locator.descriptor_sha256.clone(),
        descriptor_bytes,
        reused: true,
    };
    Ok((fixture, verified.records, verified.verification_seconds))
}

async fn verify_closure(
    client: &ObjectClient,
    descriptor: &ObjectFixtureDescriptorV1,
) -> Result<(Vec<RowRecord>, bool), String> {
    let (manifest_bytes, _) = client
        .read_full_verified(
            &descriptor.manifest.key,
            None,
            descriptor.manifest.length,
            &descriptor.manifest.sha256,
        )
        .await
        .map_err(|error| error.to_string())?;
    let manifest = RowObjectManifestV1::decode(&manifest_bytes)?;
    if manifest.generation != GENERATION || manifest.covered_through != descriptor.base_version {
        return Err("fixture manifest generation or base version mismatch".to_owned());
    }
    let mut closure = vec![ClosureObjectIdentity {
        key: descriptor.manifest.key.clone(),
        length: descriptor.manifest.length,
        sha256: descriptor.manifest.sha256.clone(),
    }];
    let mut records = Vec::with_capacity(usize::try_from(descriptor.key_count).unwrap_or(0));
    let mut segment_versions_at_anchor = true;
    for reference in &manifest.segments {
        segment_versions_at_anchor &= reference.min_version == descriptor.base_version
            && reference.max_version == descriptor.base_version;
        let (index_bytes, _) = client
            .read_full_verified(
                &reference.index_key,
                None,
                reference.index_bytes,
                &reference.index_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_bytes)?;
        reference.validate_index(&index_bytes, &index)?;
        let (data_bytes, _) = client
            .read_full_verified(
                &reference.data_key,
                None,
                reference.data_bytes,
                &reference.data_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        records.extend(decode_full_row_object(&data_bytes, &index)?);
        closure.extend([
            ClosureObjectIdentity {
                key: reference.data_key.clone(),
                length: reference.data_bytes,
                sha256: reference.data_sha256.clone(),
            },
            ClosureObjectIdentity {
                key: reference.index_key.clone(),
                length: reference.index_bytes,
                sha256: reference.index_sha256.clone(),
            },
        ]);
    }
    closure.sort();
    if u64::try_from(closure.len()).unwrap_or(u64::MAX) != descriptor.object_count
        || closure.iter().map(|object| object.length).sum::<u64>() != descriptor.object_bytes
        || closure_sha256(&closure) != descriptor.closure_sha256
        || u64::try_from(records.len()).unwrap_or(u64::MAX) != descriptor.key_count
        || logical_key_value_sha256(&records)? != descriptor.logical_key_value_sha256
        || records
            .iter()
            .any(|record| record.version != descriptor.base_version)
    {
        return Err("fixture closure does not match its descriptor".to_owned());
    }
    Ok((records, segment_versions_at_anchor))
}

fn validate_descriptor(
    descriptor: &ObjectFixtureDescriptorV1,
    anchor_version: u64,
) -> Result<(), String> {
    if descriptor.schema_version != FIXTURE_SCHEMA_VERSION
        || descriptor.generator_version != FIXTURE_GENERATOR_VERSION
        || descriptor.seed == 0
        || descriptor.key_count == 0
        || descriptor.value_bytes == 0
        || descriptor.logical_bytes != descriptor.key_count.saturating_mul(descriptor.value_bytes)
        || descriptor.base_version != anchor_version
        || descriptor.row_object_format_version != ROW_OBJECT_FORMAT_VERSION
        || descriptor.target_block_bytes < 4_096
        || descriptor.target_object_bytes < descriptor.target_block_bytes
        || descriptor.manifest.key != content_key(&descriptor.manifest.sha256)
        || descriptor.manifest.length == 0
        || descriptor.object_count < 3
        || descriptor.object_bytes < descriptor.manifest.length
        || !valid_sha256(&descriptor.logical_key_value_sha256)
        || !valid_sha256(&descriptor.manifest.sha256)
        || !valid_sha256(&descriptor.closure_sha256)
    {
        return Err("invalid RFC-0044 fixture descriptor".to_owned());
    }
    Ok(())
}

async fn commit_tail(
    seed: u64,
    profile: &ObjectFixtureProfile,
    base_version: u64,
    client: &TransactionLogClient,
) -> Result<TailFixture, String> {
    let templates = tail_commands(seed, profile, base_version);
    let mut next_request_id = 2_u64;
    let mut items = Vec::with_capacity(2);
    for template in &templates[..2] {
        let mut command = template.clone();
        command.read_version = base_version;
        items.push(TransactionBatchItem {
            identity: RequestIdentity {
                client_id: seed.max(1),
                request_id: next_request_id,
            },
            credential: None,
            command,
        });
        next_request_id = next_request_id.saturating_add(1);
    }
    let batch = client.commit_batch(&items).await?;
    if batch.items.len() != items.len() {
        return Err("fixture tail batch returned the wrong item count".to_owned());
    }
    let mut applied_through = None;
    for (expected_order, item) in batch.items.iter().enumerate() {
        let response = item
            .transaction
            .as_ref()
            .ok_or_else(|| "fixture tail batch outcome is absent".to_owned())?;
        let TransactionStatus::Committed { commit_version } = response.status else {
            return Err("fixture tail batch item did not commit".to_owned());
        };
        if response.batch_order != u16::try_from(expected_order).unwrap_or(u16::MAX)
            || applied_through.is_some_and(|version| version != commit_version)
        {
            return Err("fixture tail batch version or order mismatch".to_owned());
        }
        applied_through = Some(commit_version);
    }
    let mut applied_through =
        applied_through.ok_or_else(|| "fixture tail batch was empty".to_owned())?;
    for template in &templates[2..] {
        let mut command = template.clone();
        command.read_version = applied_through;
        let response = client
            .commit(
                RequestIdentity {
                    client_id: seed.max(1),
                    request_id: next_request_id,
                },
                &command,
            )
            .await?;
        let TransactionStatus::Committed { commit_version } = response.status else {
            return Err("fixture tail transaction did not commit".to_owned());
        };
        applied_through = commit_version;
        next_request_id = next_request_id.saturating_add(1);
    }
    let retained = client
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: base_version,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(applied_through),
            max_records: 16,
        })
        .await?;
    if !retained.complete
        || retained.records.len() != EXPECTED_TAIL_RECORDS
        || retained
            .records
            .iter()
            .any(|record| record.commit_version <= base_version)
    {
        return Err(
            "fixture retained tail is incomplete or crosses the object frontier".to_owned(),
        );
    }
    Ok(TailFixture {
        records: retained.records,
        applied_through,
    })
}

fn tail_commands(
    seed: u64,
    profile: &ObjectFixtureProfile,
    base_version: u64,
) -> Vec<TransactionCommand> {
    let range = KeyRange {
        start: key_bytes(10),
        end: key_bytes(14),
    };
    vec![
        point_command(
            base_version,
            2,
            TransactionMutation::Set {
                key: key_bytes(2),
                value: tail_value(seed, b"initial-update", profile.value_bytes),
            },
        ),
        point_command(
            base_version,
            3,
            TransactionMutation::Clear { key: key_bytes(3) },
        ),
        point_command(
            base_version,
            profile.key_count.saturating_add(1),
            TransactionMutation::Set {
                key: key_bytes(profile.key_count.saturating_add(1)),
                value: tail_value(seed, b"initial-insert", profile.value_bytes),
            },
        ),
        point_command(
            base_version,
            4,
            TransactionMutation::Set {
                key: key_bytes(4),
                value: tail_value(seed, b"concurrent-update", profile.value_bytes),
            },
        ),
        point_command(
            base_version,
            5,
            TransactionMutation::Clear { key: key_bytes(5) },
        ),
        point_command(
            base_version,
            profile.key_count.saturating_add(2),
            TransactionMutation::Set {
                key: key_bytes(profile.key_count.saturating_add(2)),
                value: tail_value(seed, b"concurrent-insert", profile.value_bytes),
            },
        ),
        TransactionCommand {
            read_version: base_version,
            read_conflicts: Vec::new(),
            write_conflicts: vec![range.clone()],
            mutations: vec![TransactionMutation::ClearRange { range }],
        },
    ]
}

fn point_command(
    read_version: u64,
    key_id: u64,
    mutation: TransactionMutation,
) -> TransactionCommand {
    TransactionCommand {
        read_version,
        read_conflicts: Vec::new(),
        write_conflicts: vec![KeyRange::point(&key_bytes(key_id))],
        mutations: vec![mutation],
    }
}

pub(crate) fn base_records(
    seed: u64,
    profile: &ObjectFixtureProfile,
    version: u64,
) -> Result<Vec<RowRecord>, String> {
    usize::try_from(profile.key_count).map_err(|_| "fixture key count exceeds usize".to_owned())?;
    Ok((0..profile.key_count)
        .map(|key_id| {
            RowRecord::value(
                key_bytes(key_id),
                version,
                base_value(seed, key_id, profile.value_bytes),
            )
        })
        .collect())
}

pub(crate) fn base_value_txlog_accounting(
    retained: &[RetainedTransactionRecord],
    base: &[RowRecord],
) -> (u64, u64) {
    let base_values = base
        .iter()
        .filter_map(|record| record.value.as_ref().map(|value| (&record.key, value)))
        .collect::<BTreeMap<_, _>>();
    let mut records = 0_u64;
    let mut mutation_bytes = 0_u64;
    for record in retained {
        let mut record_contains_base = false;
        for mutation in &record.command.mutations {
            if let TransactionMutation::Set { key, value } = mutation {
                if base_values
                    .get(key)
                    .is_some_and(|base_value| base_value.as_slice() == value.as_slice())
                {
                    record_contains_base = true;
                    mutation_bytes = mutation_bytes
                        .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
                        .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
                }
            }
        }
        records = records.saturating_add(u64::from(record_contains_base));
    }
    (records, mutation_bytes)
}

#[allow(clippy::too_many_arguments)]
fn build_resident_image(
    subject: &str,
    provider: &str,
    engine_format_version: u32,
    options: &str,
    fixture_id: &str,
    tail_sha256: &str,
    applied_through: u64,
    profile: &ObjectFixtureProfile,
    base: &[RowRecord],
    tail: &[RetainedTransactionRecord],
) -> Result<BuiltResidentImage, String> {
    let outcomes = logical_image(profile, base, tail)?;
    let resident_logical_sha256 = logical_image_sha256(&outcomes);
    let descriptor = ResidentImageDescriptorV1 {
        schema_version: RESIDENT_IMAGE_SCHEMA_VERSION,
        fixture_id: fixture_id.to_owned(),
        tail_sha256: tail_sha256.to_owned(),
        subject: subject.to_owned(),
        engine_provider: provider.to_owned(),
        engine_format_version,
        options_sha256: content_sha256(options.as_bytes()),
        applied_through,
        record_count: u64::try_from(outcomes.len()).unwrap_or(u64::MAX),
        resident_logical_sha256,
    };
    let resident_image_id = descriptor.resident_image_id();
    Ok(BuiltResidentImage {
        descriptor,
        resident_image_id,
    })
}

fn logical_image(
    profile: &ObjectFixtureProfile,
    base: &[RowRecord],
    tail: &[RetainedTransactionRecord],
) -> Result<BTreeMap<Vec<u8>, LogicalOutcome>, String> {
    let mut image = BTreeMap::new();
    for record in base {
        let value = record
            .value
            .clone()
            .ok_or_else(|| "fixture base contains a tombstone".to_owned())?;
        image.insert(record.key.clone(), LogicalOutcome::Value(value));
    }
    image.insert(
        key_bytes(profile.key_count.saturating_add(1)),
        LogicalOutcome::Absent,
    );
    image.insert(
        key_bytes(profile.key_count.saturating_add(2)),
        LogicalOutcome::Absent,
    );
    image.insert(
        key_bytes(profile.key_count.saturating_add(3)),
        LogicalOutcome::Absent,
    );
    for record in tail {
        for mutation in &record.command.mutations {
            match mutation {
                TransactionMutation::Set { key, value } => {
                    image.insert(key.clone(), LogicalOutcome::Value(value.clone()));
                }
                TransactionMutation::Clear { key } => {
                    image.insert(key.clone(), LogicalOutcome::Tombstone);
                }
                TransactionMutation::ClearRange { range } => {
                    let keys = image
                        .range(range.start.clone()..range.end.clone())
                        .map(|(key, _)| key.clone())
                        .collect::<Vec<_>>();
                    for key in keys {
                        image.insert(key, LogicalOutcome::Tombstone);
                    }
                }
            }
        }
    }
    Ok(image)
}

pub(crate) fn logical_image_sha256(image: &BTreeMap<Vec<u8>, LogicalOutcome>) -> String {
    let mut bytes = LOGICAL_IMAGE_MAGIC.to_vec();
    push_u64(&mut bytes, u64::try_from(image.len()).unwrap_or(u64::MAX));
    for (key, outcome) in image {
        push_bytes(&mut bytes, key);
        match outcome {
            LogicalOutcome::Value(value) => {
                bytes.push(1);
                push_bytes(&mut bytes, value);
            }
            LogicalOutcome::Tombstone => bytes.push(2),
            LogicalOutcome::Absent => bytes.push(3),
        }
    }
    content_sha256(&bytes)
}

fn logical_key_value_sha256(records: &[RowRecord]) -> Result<String, String> {
    let mut bytes = b"OKVKV1".to_vec();
    push_u64(&mut bytes, u64::try_from(records.len()).unwrap_or(u64::MAX));
    for record in records {
        let value = record
            .value
            .as_ref()
            .ok_or_else(|| "fixture logical base contains a tombstone".to_owned())?;
        push_bytes(&mut bytes, &record.key);
        push_bytes(&mut bytes, value);
    }
    Ok(content_sha256(&bytes))
}

fn closure_sha256(objects: &[ClosureObjectIdentity]) -> String {
    let mut objects = objects.to_vec();
    objects.sort();
    let mut bytes = b"OKVFC1".to_vec();
    push_u64(&mut bytes, u64::try_from(objects.len()).unwrap_or(u64::MAX));
    for object in objects {
        push_string(&mut bytes, &object.key);
        push_u64(&mut bytes, object.length);
        push_string(&mut bytes, &object.sha256);
    }
    content_sha256(&bytes)
}

pub(crate) fn tail_sha256(records: &[RetainedTransactionRecord]) -> Result<String, String> {
    Ok(content_sha256(&encode_tail(records)?))
}

pub(crate) fn validate_tail(
    expected_sha256: &str,
    records: &[RetainedTransactionRecord],
) -> Result<(), String> {
    if records.len() != EXPECTED_TAIL_RECORDS
        || records.windows(2).any(|pair| {
            (pair[0].commit_version, pair[0].batch_order)
                >= (pair[1].commit_version, pair[1].batch_order)
        })
        || tail_sha256(records)? != expected_sha256
    {
        return Err("fixture retained tail identity mismatch".to_owned());
    }
    Ok(())
}

fn encode_tail(records: &[RetainedTransactionRecord]) -> Result<Vec<u8>, String> {
    let mut bytes = TAIL_MAGIC.to_vec();
    push_u64(&mut bytes, u64::try_from(records.len()).unwrap_or(u64::MAX));
    for record in records {
        push_u64(&mut bytes, record.commit_version);
        bytes.extend_from_slice(&record.batch_order.to_be_bytes());
        encode_command(&mut bytes, &record.command)?;
    }
    Ok(bytes)
}

fn encode_command(bytes: &mut Vec<u8>, command: &TransactionCommand) -> Result<(), String> {
    push_u64(bytes, command.read_version);
    encode_ranges(bytes, &command.read_conflicts)?;
    encode_ranges(bytes, &command.write_conflicts)?;
    push_u64(
        bytes,
        u64::try_from(command.mutations.len()).unwrap_or(u64::MAX),
    );
    for mutation in &command.mutations {
        match mutation {
            TransactionMutation::Set { key, value } => {
                bytes.push(1);
                push_bytes(bytes, key);
                push_bytes(bytes, value);
            }
            TransactionMutation::Clear { key } => {
                bytes.push(2);
                push_bytes(bytes, key);
            }
            TransactionMutation::ClearRange { range } => {
                bytes.push(3);
                encode_range(bytes, range)?;
            }
        }
    }
    Ok(())
}

fn encode_ranges(bytes: &mut Vec<u8>, ranges: &[KeyRange]) -> Result<(), String> {
    push_u64(bytes, u64::try_from(ranges.len()).unwrap_or(u64::MAX));
    for range in ranges {
        encode_range(bytes, range)?;
    }
    Ok(())
}

fn encode_range(bytes: &mut Vec<u8>, range: &KeyRange) -> Result<(), String> {
    if range.start >= range.end {
        return Err("fixture tail contains an invalid key range".to_owned());
    }
    push_bytes(bytes, &range.start);
    push_bytes(bytes, &range.end);
    Ok(())
}

fn encode_fixture_identity(descriptor: &ObjectFixtureDescriptorV1) -> Vec<u8> {
    let mut bytes = FIXTURE_MAGIC.to_vec();
    bytes.extend_from_slice(&descriptor.schema_version.to_be_bytes());
    bytes.extend_from_slice(&descriptor.generator_version.to_be_bytes());
    push_u64(&mut bytes, descriptor.seed);
    push_u64(&mut bytes, descriptor.key_count);
    push_u64(&mut bytes, descriptor.value_bytes);
    push_u64(&mut bytes, descriptor.logical_bytes);
    push_string(&mut bytes, &descriptor.logical_key_value_sha256);
    push_u64(&mut bytes, descriptor.base_version);
    bytes.extend_from_slice(&descriptor.row_object_format_version.to_be_bytes());
    push_u64(&mut bytes, descriptor.target_object_bytes);
    push_u64(&mut bytes, descriptor.target_block_bytes);
    push_string(&mut bytes, &descriptor.manifest.key);
    push_u64(&mut bytes, descriptor.manifest.length);
    push_string(&mut bytes, &descriptor.manifest.sha256);
    push_string(&mut bytes, &descriptor.closure_sha256);
    push_u64(&mut bytes, descriptor.object_count);
    push_u64(&mut bytes, descriptor.object_bytes);
    bytes
}

fn encode_fixture_placement_identity(locator: &FixturePlacementLocatorV1) -> Vec<u8> {
    let mut bytes = FIXTURE_PLACEMENT_MAGIC.to_vec();
    bytes.extend_from_slice(&locator.schema_version.to_be_bytes());
    push_string(&mut bytes, &locator.fixture.fixture_id);
    push_u64(&mut bytes, locator.fixture.descriptor_length);
    push_string(&mut bytes, &locator.fixture.descriptor_sha256);
    push_u64(&mut bytes, locator.base_version);
    push_string(&mut bytes, &locator.provider);
    push_string(&mut bytes, &locator.bucket);
    push_string(&mut bytes, &locator.prefix);
    push_string(&mut bytes, &locator.descriptor_key);
    push_string(&mut bytes, &locator.descriptor_generation);
    push_u64(&mut bytes, locator.fixture_seed);
    push_u64(&mut bytes, locator.key_count);
    push_u64(&mut bytes, locator.value_bytes);
    push_u64(&mut bytes, locator.logical_bytes);
    bytes.extend_from_slice(&locator.generator_version.to_be_bytes());
    bytes.extend_from_slice(&locator.row_object_format_version.to_be_bytes());
    push_u64(&mut bytes, locator.target_object_bytes);
    push_u64(&mut bytes, locator.target_block_bytes);
    push_string(&mut bytes, &locator.source_sha256);
    push_string(&mut bytes, &locator.suite_sha256);
    push_string(&mut bytes, &locator.binary_sha256);
    push_string(&mut bytes, &locator.cargo_lock_sha256);
    bytes
}

fn encode_resident_identity(descriptor: &ResidentImageDescriptorV1) -> Vec<u8> {
    let mut bytes = RESIDENT_IMAGE_MAGIC.to_vec();
    bytes.extend_from_slice(&descriptor.schema_version.to_be_bytes());
    push_string(&mut bytes, &descriptor.fixture_id);
    push_string(&mut bytes, &descriptor.tail_sha256);
    push_string(&mut bytes, &descriptor.subject);
    push_string(&mut bytes, &descriptor.engine_provider);
    bytes.extend_from_slice(&descriptor.engine_format_version.to_be_bytes());
    push_string(&mut bytes, &descriptor.options_sha256);
    push_u64(&mut bytes, descriptor.applied_through);
    push_u64(&mut bytes, descriptor.record_count);
    push_string(&mut bytes, &descriptor.resident_logical_sha256);
    bytes
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    push_u64(bytes, u64::try_from(value.len()).unwrap_or(u64::MAX));
    bytes.extend_from_slice(value);
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
    push_bytes(bytes, value.as_bytes());
}

fn content_key(sha256: &str) -> String {
    format!("{CONTENT_ROOT}/{sha256}")
}

fn descriptor_key(fixture_id: &str) -> String {
    format!("{DESCRIPTOR_ROOT}/{fixture_id}.json")
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_bucket(value: &str) -> bool {
    (3..=222).contains(&value.len())
        && !value.starts_with('.')
        && !value.ends_with('.')
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

fn valid_prefix(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("://")
        && value
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

fn validate_subject_roots(native: &Path, control: &Path) -> Result<(), String> {
    let native = native.canonicalize().map_err(|error| error.to_string())?;
    let control = control.canonicalize().map_err(|error| error.to_string())?;
    if native == control {
        return Err("native and control may not share one mutable resident image".to_owned());
    }
    Ok(())
}

fn directory_bytes(root: &Path) -> Result<u64, String> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_file()
        {
            bytes =
                bytes.saturating_add(entry.metadata().map_err(|error| error.to_string())?.len());
        }
    }
    Ok(bytes)
}

fn empty_checkpoint_sha256(native: &Path, control: &Path) -> Result<String, String> {
    let mut bytes = b"OKV-EMPTY-RESIDENT-CHECKPOINT-V1\0".to_vec();
    for root in [native, control] {
        let canonical = root.canonicalize().map_err(|error| error.to_string())?;
        push_string(
            &mut bytes,
            canonical
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(""),
        );
        push_u64(&mut bytes, directory_bytes(root)?);
    }
    Ok(content_sha256(&bytes))
}

fn key_bytes(key_id: u64) -> Vec<u8> {
    key_id.to_be_bytes().to_vec()
}

fn base_value(seed: u64, key_id: u64, length: usize) -> Vec<u8> {
    deterministic_value(seed ^ key_id.rotate_left(17), b"base", length)
}

fn tail_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    deterministic_value(seed ^ 0x5441_494c_5641_4c55, domain, length)
}

fn deterministic_value(seed: u64, domain: &[u8], length: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(length);
    let mut counter = 0_u64;
    while value.len() < length {
        let mut hasher = Sha256::new();
        hasher.update(b"OKV-SERVING-OPENRAFT-RECOVERY-V1\0");
        hasher.update(seed.to_be_bytes());
        hasher.update(domain);
        hasher.update(counter.to_be_bytes());
        let digest = hasher.finalize();
        let remaining = length.saturating_sub(value.len());
        value.extend_from_slice(&digest[..remaining.min(digest.len())]);
        counter = counter.saturating_add(1);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{
        base_records, build_fixture, content_sha256, decode_fixture_placement_locator,
        logical_image_sha256, open_existing_fixture, FixtureManifestIdentityV1,
        FixturePlacementLocatorV1, LogicalOutcome, ObjectFixtureDescriptorV1,
        ObjectFixtureLocatorV1, ObjectFixtureProfile,
    };
    use okv_object::{memory_backend, ObjectClient};
    use std::collections::BTreeMap;

    fn descriptor() -> ObjectFixtureDescriptorV1 {
        ObjectFixtureDescriptorV1 {
            schema_version: 1,
            generator_version: 1,
            seed: 7,
            key_count: 4,
            value_bytes: 8,
            logical_bytes: 32,
            logical_key_value_sha256: content_sha256(b"logical"),
            base_version: 2,
            row_object_format_version: 1,
            target_object_bytes: 65_536,
            target_block_bytes: 4_096,
            manifest: FixtureManifestIdentityV1 {
                key: format!(
                    "fixtures/single-range/v1/blobs/sha256/{}",
                    content_sha256(b"manifest")
                ),
                length: 42,
                sha256: content_sha256(b"manifest"),
            },
            closure_sha256: content_sha256(b"closure"),
            object_count: 3,
            object_bytes: 128,
        }
    }

    fn placement_locator() -> FixturePlacementLocatorV1 {
        let descriptor = descriptor();
        let mut locator = FixturePlacementLocatorV1 {
            schema_version: 1,
            fixture: ObjectFixtureLocatorV1 {
                fixture_id: descriptor.fixture_id(),
                descriptor_length: 777,
                descriptor_sha256: content_sha256(b"descriptor"),
            },
            base_version: 2,
            provider: "gcs".to_owned(),
            bucket: "doss-objectkv-dev-okv-evals".to_owned(),
            prefix: "runs/rfc0044-t27-fixture-v1".to_owned(),
            descriptor_key: String::new(),
            descriptor_generation: "1788000691000000".to_owned(),
            fixture_seed: 4244,
            key_count: 1_048_576,
            value_bytes: 1_024,
            logical_bytes: 1_073_741_824,
            generator_version: 1,
            row_object_format_version: 1,
            target_object_bytes: 8_388_608,
            target_block_bytes: 65_536,
            source_sha256: content_sha256(b"source"),
            suite_sha256: content_sha256(b"suite"),
            binary_sha256: content_sha256(b"binary"),
            cargo_lock_sha256: content_sha256(b"lockfile"),
            envelope_sha256: String::new(),
        };
        locator.descriptor_key = format!(
            "fixtures/single-range/v1/descriptors/{}.json",
            locator.fixture.fixture_id
        );
        locator.envelope_sha256 = locator.calculated_envelope_sha256();
        locator
    }

    #[test]
    fn fixture_identity_is_deterministic_and_field_sensitive() {
        let first = descriptor();
        let mut changed = first.clone();
        changed.target_block_bytes = changed.target_block_bytes.saturating_mul(2);
        assert_eq!(first.fixture_id(), first.fixture_id());
        assert_ne!(first.fixture_id(), changed.fixture_id());
    }

    #[test]
    fn placement_locator_round_trips_with_independent_envelope_identity() {
        let locator = placement_locator();
        let encoded = serde_json::to_vec(&locator).expect("encode locator");
        let decoded = decode_fixture_placement_locator(&encoded, &locator.envelope_sha256)
            .expect("decode exact locator");
        assert_eq!(decoded, locator);
    }

    #[test]
    fn placement_locator_rejects_corrupt_identity_and_profile_fields() {
        let locator = placement_locator();
        let expected = locator.envelope_sha256.clone();

        let mut changed_generation = locator.clone();
        changed_generation.descriptor_generation = "1788000691000001".to_owned();
        assert!(changed_generation.validate().is_err());

        let mut changed_profile = locator.clone();
        changed_profile.logical_bytes = changed_profile.logical_bytes.saturating_sub(1);
        changed_profile.envelope_sha256 = changed_profile.calculated_envelope_sha256();
        assert!(changed_profile.validate().is_err());

        let encoded = serde_json::to_vec(&locator).expect("encode locator");
        assert!(decode_fixture_placement_locator(&encoded, &content_sha256(b"wrong")).is_err());
        assert!(decode_fixture_placement_locator(&encoded, &expected).is_ok());
    }

    #[test]
    fn placement_locator_rejects_unsafe_or_unbound_placement() {
        let locator = placement_locator();
        let mutators: [fn(&mut FixturePlacementLocatorV1); 4] = [
            |value: &mut FixturePlacementLocatorV1| value.prefix = "../escape".to_owned(),
            |value: &mut FixturePlacementLocatorV1| value.bucket = "Bad Bucket".to_owned(),
            |value: &mut FixturePlacementLocatorV1| value.descriptor_generation.clear(),
            |value: &mut FixturePlacementLocatorV1| value.envelope_sha256.clear(),
        ];
        for mutate in mutators {
            let mut changed = locator.clone();
            mutate(&mut changed);
            assert!(changed.validate().is_err());
        }
    }

    #[test]
    fn logical_image_distinguishes_value_tombstone_and_absence() {
        let mut image = BTreeMap::from([
            (b"a".to_vec(), LogicalOutcome::Value(b"v".to_vec())),
            (b"b".to_vec(), LogicalOutcome::Tombstone),
            (b"c".to_vec(), LogicalOutcome::Absent),
        ]);
        let first = logical_image_sha256(&image);
        image.insert(b"b".to_vec(), LogicalOutcome::Absent);
        assert_ne!(first, logical_image_sha256(&image));
    }

    #[tokio::test]
    async fn persisted_fixture_reopens_by_exact_descriptor_identity() {
        let profile = ObjectFixtureProfile {
            key_count: 17,
            value_bytes: 8,
            target_object_bytes: 4_096,
            target_block_bytes: 4_096,
        };
        let backend = memory_backend();
        let records = base_records(7, &profile, 2).expect("base records");
        let built = build_fixture(
            7,
            &profile,
            2,
            &records,
            &ObjectClient::new(backend.clone()),
        )
        .await
        .expect("build fixture");
        let locator = built.locator();
        let (reopened, reopened_records, _) = open_existing_fixture(&backend, &locator, 2)
            .await
            .expect("open exact fixture");
        assert!(reopened.reused);
        assert_eq!(reopened.locator(), locator);
        assert_eq!(reopened_records, records);

        let mut wrong = locator;
        wrong.descriptor_sha256 = content_sha256(b"wrong descriptor");
        assert!(open_existing_fixture(&backend, &wrong, 2).await.is_err());
    }
}
