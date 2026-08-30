//! RFC-0049 root envelope and generation-pinned C5v2 reader.

use super::columnar_aligned::{
    prepare_t28_aligned_columnar_layout, T28AlignedColumnarCore, T28AlignedColumnarScanCore,
    T28AlignedPointPairSnapshot, INDEX_KEY, MANIFEST_KEY, PAYLOAD_KEY, PROJECTION_KEY,
};
use super::t28_typed::{
    capture_identity, child_total_bytes, numeric_generation, validate_history_against_oracle,
    T28_SCHEMA_ID,
};
use super::{t28_typed_layout_profile, LogicalHistory, T28RowLayoutReader};
use crate::t28_layout::{
    decode_typed_layout_fixture, T28LayoutOracleV1, TypedLayoutChildV1, TypedLayoutFixtureV1,
    TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1, TypedLayoutPlacementLocatorV1,
    TypedLayoutSubjectV1,
};
use bytes::Bytes;
use okv_object::{content_sha256, prefixed_backend, Backend, PointReadOutcome, WriteCondition};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const PHYSICAL_PLAN_SHA256: &str =
    "5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61";
const FORMAT_ID: &str = "okv.columnar-overlay.v2";
const FORMAT_VERSION: u32 = 2;
const SUBJECT: &str = "c5v2_aligned_columnar_main";
const CAPABILITIES: [&str; 5] = [
    "indexed_mvcc_point",
    "concurrent_aligned_gather",
    "projection_only_scan",
    "merkle_range_proof",
    "disposable_range_engine_cache",
];

/// Provider placement selected before RFC-0049 publication.
#[derive(Clone, Debug)]
pub struct T28AlignedLayoutPlacementInput {
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
}

/// C5v2 child closure under its own provider prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedChildV1 {
    pub subject: String,
    pub prefix: String,
    pub bucket: String,
    pub format_id: String,
    pub format_version: u32,
    pub canonical_history_sha256: String,
    pub schema_id: String,
    pub schema_sha256: String,
    pub covered_through_version: u64,
    pub manifest_key: String,
    pub capabilities: Vec<String>,
    pub objects: Vec<TypedLayoutObjectIdentityV1>,
    pub closure_sha256: String,
}

impl T28AlignedChildV1 {
    #[allow(clippy::too_many_arguments)]
    fn seal(
        prefix: String,
        bucket: String,
        canonical_history_sha256: String,
        schema_sha256: String,
        covered_through_version: u64,
        objects: Vec<TypedLayoutObjectIdentityV1>,
    ) -> Result<Self, String> {
        let mut child = Self {
            subject: SUBJECT.to_owned(),
            prefix,
            bucket,
            format_id: FORMAT_ID.to_owned(),
            format_version: FORMAT_VERSION,
            canonical_history_sha256,
            schema_id: T28_SCHEMA_ID.to_owned(),
            schema_sha256,
            covered_through_version,
            manifest_key: MANIFEST_KEY.to_owned(),
            capabilities: CAPABILITIES.map(str::to_owned).to_vec(),
            objects,
            closure_sha256: String::new(),
        };
        child.closure_sha256 = child.calculated_sha256()?;
        child.validate()?;
        Ok(child)
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.closure_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }

    fn validate(&self) -> Result<(), String> {
        if self.subject != SUBJECT
            || self.prefix.is_empty()
            || self.bucket.is_empty()
            || self.format_id != FORMAT_ID
            || self.format_version != FORMAT_VERSION
            || self.canonical_history_sha256.len() != 64
            || self.schema_id != T28_SCHEMA_ID
            || self.schema_sha256.len() != 64
            || self.covered_through_version == 0
            || self.manifest_key != MANIFEST_KEY
            || self.capabilities != CAPABILITIES.map(str::to_owned)
            || self.objects.len() != 4
            || self.closure_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 C5v2 child closure".to_owned());
        }
        let expected = [
            (MANIFEST_KEY, TypedLayoutObjectRoleV1::Manifest),
            (INDEX_KEY, TypedLayoutObjectRoleV1::Index),
            (PAYLOAD_KEY, TypedLayoutObjectRoleV1::Payload),
            (PROJECTION_KEY, TypedLayoutObjectRoleV1::Projection),
        ];
        for (object, (key, role)) in self.objects.iter().zip(expected) {
            object.validate()?;
            if object.key != key || object.role != role {
                return Err("RFC-0049 C5v2 object inventory differs from the format".to_owned());
            }
        }
        Ok(())
    }

    fn total_bytes(&self) -> u64 {
        self.objects
            .iter()
            .fold(0_u64, |total, object| total.saturating_add(object.length))
    }
}

/// New typed root that embeds the exact RFC-0048 C0 descriptor and one C5v2
/// child without copying C0 media.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28AlignedFixtureV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub fixture_seed: u64,
    pub key_count: u64,
    pub record_count: u64,
    pub live_row_count: u64,
    pub canonical_history_sha256: String,
    pub schema_id: String,
    pub schema_sha256: String,
    pub covered_through_version: u64,
    pub oracle_sha256: String,
    pub workload_plan_sha256: String,
    pub physical_plan_sha256: String,
    pub provider: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub source_root_sha256: String,
    pub source_root_generation: String,
    pub source_placement_envelope_sha256: String,
    pub source_c0_prefix: String,
    pub source_c0: TypedLayoutChildV1,
    pub candidate: T28AlignedChildV1,
    pub root_sha256: String,
}

impl T28AlignedFixtureV1 {
    #[allow(clippy::too_many_arguments)]
    fn seal(
        source: &TypedLayoutFixtureV1,
        source_locator: &TypedLayoutPlacementLocatorV1,
        candidate: T28AlignedChildV1,
        placement: &T28AlignedLayoutPlacementInput,
    ) -> Result<Self, String> {
        let source_c0 = source
            .child(TypedLayoutSubjectV1::C0IndexedRow)
            .ok_or_else(|| "RFC-0049 source root omits C0".to_owned())?
            .clone();
        let mut fixture = Self {
            schema_version: 1,
            fixture_id: source.fixture_id.clone(),
            fixture_seed: source.fixture_seed,
            key_count: source.key_count,
            record_count: source.record_count,
            live_row_count: source.live_row_count,
            canonical_history_sha256: source.canonical_history_sha256.clone(),
            schema_id: source.schema_id.clone(),
            schema_sha256: source.schema_sha256.clone(),
            covered_through_version: source.covered_through_version,
            oracle_sha256: source.oracle_sha256.clone(),
            workload_plan_sha256: source.workload_plan_sha256.clone(),
            physical_plan_sha256: PHYSICAL_PLAN_SHA256.to_owned(),
            provider: "gcs".to_owned(),
            project: placement.project.clone(),
            bucket: placement.bucket.clone(),
            region: placement.region.clone(),
            source_root_sha256: source.root_sha256.clone(),
            source_root_generation: source_locator.root_generation.clone(),
            source_placement_envelope_sha256: source_locator.envelope_sha256.clone(),
            source_c0_prefix: source_locator.prefix.clone(),
            source_c0,
            candidate,
            root_sha256: String::new(),
        };
        fixture.root_sha256 = fixture.calculated_sha256()?;
        fixture.validate()?;
        Ok(fixture)
    }

    fn calculated_sha256(&self) -> Result<String, String> {
        let mut unsigned = self.clone();
        unsigned.root_sha256.clear();
        serde_json::to_vec(&unsigned)
            .map(|bytes| content_sha256(&bytes))
            .map_err(|error| error.to_string())
    }

    fn validate(&self) -> Result<(), String> {
        self.source_c0.validate()?;
        self.candidate.validate()?;
        if self.schema_version != 1
            || self.fixture_id.len() != 64
            || self.fixture_seed == 0
            || self.key_count == 0
            || self.record_count < self.live_row_count
            || self.canonical_history_sha256.len() != 64
            || self.schema_id != T28_SCHEMA_ID
            || self.schema_sha256.len() != 64
            || self.covered_through_version == 0
            || self.oracle_sha256.len() != 64
            || self.workload_plan_sha256.len() != 64
            || self.physical_plan_sha256 != PHYSICAL_PLAN_SHA256
            || self.provider != "gcs"
            || self.project.is_empty()
            || self.bucket.is_empty()
            || self.region.is_empty()
            || self.source_root_sha256.len() != 64
            || self.source_root_generation.is_empty()
            || !self
                .source_root_generation
                .bytes()
                .all(|byte| byte.is_ascii_digit())
            || self.source_placement_envelope_sha256.len() != 64
            || self.source_c0_prefix.is_empty()
            || self.source_c0.subject != TypedLayoutSubjectV1::C0IndexedRow
            || self.source_c0.bucket != self.bucket
            || self.candidate.bucket != self.bucket
            || self.source_c0.canonical_history_sha256 != self.canonical_history_sha256
            || self.candidate.canonical_history_sha256 != self.canonical_history_sha256
            || self.source_c0.schema_id != self.schema_id
            || self.candidate.schema_id != self.schema_id
            || self.source_c0.schema_sha256 != self.schema_sha256
            || self.candidate.schema_sha256 != self.schema_sha256
            || self.source_c0.covered_through_version != self.covered_through_version
            || self.candidate.covered_through_version != self.covered_through_version
            || self.source_c0_prefix == self.candidate.prefix
            || self.root_sha256 != self.calculated_sha256()?
        {
            return Err("invalid RFC-0049 aligned typed root".to_owned());
        }
        Ok(())
    }
}

/// Immutable RFC-0049 root and placement returned by publication.
#[derive(Clone, Debug, Serialize)]
pub struct T28AlignedLayoutPublication {
    pub fixture: T28AlignedFixtureV1,
    pub locator: TypedLayoutPlacementLocatorV1,
    pub c0_reused_bytes: u64,
    pub c5v2_total_bytes: u64,
    pub root_bytes: u64,
}

/// Metadata-warm generation-pinned C5v2 point reader.
pub struct T28AlignedLayoutReader {
    inner: T28AlignedColumnarCore,
}

/// C5v2-specific object-fetch counters for one projected scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct T28AlignedScanSnapshot {
    pub projection_fetch_requests: u64,
    pub peak_fetch_bytes: u64,
    pub payload_requests: u64,
    pub payload_response_bytes: u64,
}

/// Runtime evidence that each point issued overlapping projection and payload
/// provider calls.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct T28AlignedPointGatherSnapshot {
    pub point_pairs: u64,
    pub overlapping_point_pairs: u64,
}

/// One C5v2 DataFusion provider and its source counters.
pub struct T28AlignedScan {
    inner: T28AlignedColumnarScanCore,
}

impl T28AlignedLayoutReader {
    async fn open(
        backend: Arc<dyn Backend>,
        child: &T28AlignedChildV1,
        read_version: u64,
    ) -> Result<Self, String> {
        child.validate()?;
        Ok(Self {
            inner: T28AlignedColumnarCore::open(
                backend,
                &child.objects,
                child.covered_through_version,
                read_version,
            )
            .await?,
        })
    }

    /// Execute one C5v2 point with an overlapping projection and payload GET.
    ///
    /// # Errors
    ///
    /// Returns an error for version, generation, frame, proof, or payload
    /// reconstruction failure.
    pub async fn point(&self, key: u64, read_version: u64) -> Result<PointReadOutcome, String> {
        self.inner.point(key, read_version).await
    }

    /// Create a projection-only C5v2 provider over bounded aligned frames.
    #[must_use]
    pub fn table_provider(&self, scan_fetch_target_bytes: usize) -> T28AlignedScan {
        T28AlignedScan {
            inner: self.inner.table_provider(scan_fetch_target_bytes),
        }
    }

    /// Bytes retained after manifest and index warmup.
    #[must_use]
    pub fn resident_metadata_bytes(&self) -> u64 {
        self.inner.resident_metadata_bytes()
    }

    /// Sample point-pair lifecycle counters.
    #[must_use]
    pub fn point_gather_snapshot(&self) -> T28AlignedPointGatherSnapshot {
        let snapshot: T28AlignedPointPairSnapshot = self.inner.point_pair_snapshot();
        T28AlignedPointGatherSnapshot {
            point_pairs: snapshot.point_pairs,
            overlapping_point_pairs: snapshot.overlapping_point_pairs,
        }
    }
}

impl T28AlignedScan {
    /// Return the provider registered in one fresh DataFusion context.
    #[must_use]
    pub fn provider(&self) -> Arc<okv_htap::RangeStripeTableProvider> {
        self.inner.provider()
    }

    /// Sample C5v2 source counters after the result stream has drained.
    #[must_use]
    pub fn source_snapshot(&self) -> T28AlignedScanSnapshot {
        let source = self.inner.source_snapshot();
        T28AlignedScanSnapshot {
            projection_fetch_requests: source.projection_fetch_requests,
            peak_fetch_bytes: source.peak_fetch_bytes,
            payload_requests: source.payload_requests,
            payload_response_bytes: source.payload_response_bytes,
        }
    }
}

/// Read-only RFC-0049 root opened at one exact generation.
pub struct T28OpenedAlignedLayout {
    fixture: T28AlignedFixtureV1,
    backend: Arc<dyn Backend>,
}

impl T28OpenedAlignedLayout {
    /// Open and authenticate one new typed root.
    ///
    /// # Errors
    ///
    /// Returns an error for root generation, content, placement, or closure
    /// drift.
    pub async fn open(
        backend: Arc<dyn Backend>,
        locator: &TypedLayoutPlacementLocatorV1,
    ) -> Result<Self, String> {
        locator.validate()?;
        let read = backend
            .get(&locator.root_key, None, Some(&locator.root_revision()))
            .await
            .map_err(|error| error.to_string())?;
        if read.object_length != locator.root_length
            || read.returned_range != (0..locator.root_length)
            || read.revision.version.as_deref() != Some(locator.root_generation.as_str())
            || content_sha256(&read.bytes) != locator.root_object_sha256
        {
            return Err("RFC-0049 typed root provider identity mismatch".to_owned());
        }
        let fixture: T28AlignedFixtureV1 =
            serde_json::from_slice(&read.bytes).map_err(|error| error.to_string())?;
        fixture.validate()?;
        if fixture.fixture_id != locator.fixture_id
            || fixture.root_sha256 != locator.root_sha256
            || fixture.project != locator.project
            || fixture.bucket != locator.bucket
            || fixture.region != locator.region
        {
            return Err("RFC-0049 typed root placement differs from its locator".to_owned());
        }
        Ok(Self { fixture, backend })
    }

    /// Return the authenticated root.
    #[must_use]
    pub const fn fixture(&self) -> &T28AlignedFixtureV1 {
        &self.fixture
    }

    /// Open the reused C0 child under its original provider prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact reused closure cannot be reopened.
    pub async fn c0(&self) -> Result<T28RowLayoutReader, String> {
        let backend = prefixed_backend(
            Arc::clone(&self.backend),
            self.fixture.source_c0_prefix.clone(),
        )
        .map_err(|error| error.to_string())?;
        T28RowLayoutReader::open(
            backend,
            &self.fixture.source_c0,
            self.fixture.covered_through_version,
        )
        .await
    }

    /// Open C5v2 under its new provider prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when its manifest, index, inventory, or generation
    /// differs from the root.
    pub async fn c5v2(&self) -> Result<T28AlignedLayoutReader, String> {
        let backend = prefixed_backend(
            Arc::clone(&self.backend),
            self.fixture.candidate.prefix.clone(),
        )
        .map_err(|error| error.to_string())?;
        T28AlignedLayoutReader::open(
            backend,
            &self.fixture.candidate,
            self.fixture.covered_through_version,
        )
        .await
    }
}

/// Publish only C5v2 and a new root while reusing the exact RFC-0048 C0 child.
///
/// # Errors
///
/// Returns an error for source-root drift, independent-oracle mismatch,
/// create-only publication failure, omitted numeric generation, incompatible
/// media, or failed pinned reopen.
#[allow(clippy::too_many_lines)]
pub async fn publish_t28_aligned_layout(
    backend: Arc<dyn Backend>,
    source_locator: &TypedLayoutPlacementLocatorV1,
    placement: &T28AlignedLayoutPlacementInput,
    oracle: &T28LayoutOracleV1,
    oracle_sha256: &str,
    physical_plan_sha256: &str,
) -> Result<T28AlignedLayoutPublication, String> {
    if physical_plan_sha256 != PHYSICAL_PLAN_SHA256 {
        return Err("RFC-0049 physical plan identity mismatch".to_owned());
    }
    oracle.validate()?;
    let source_read = backend
        .get(
            &source_locator.root_key,
            None,
            Some(&source_locator.root_revision()),
        )
        .await
        .map_err(|error| error.to_string())?;
    if source_read.revision.version.as_deref() != Some(source_locator.root_generation.as_str())
        || content_sha256(&source_read.bytes) != source_locator.root_object_sha256
    {
        return Err("RFC-0049 source root provider identity mismatch".to_owned());
    }
    let source = decode_typed_layout_fixture(&source_read.bytes, &source_locator.root_sha256)?;
    if source.fixture_id != source_locator.fixture_id
        || source.oracle_sha256 != oracle_sha256
        || source.oracle_sha256
            != content_sha256(include_bytes!(
                "../../../../evals/oracles/t28-layout-geometry-v1-oracle.json"
            ))
        || source.workload_plan_sha256 != oracle.workload_plan_sha256
    {
        return Err("RFC-0049 source root differs from the frozen oracle".to_owned());
    }

    let profile = t28_typed_layout_profile();
    let history = LogicalHistory::generate(&profile, oracle.fixture.seed)?;
    validate_history_against_oracle(&history, oracle)?;
    let candidate_prefix = format!("{}/c5v2", placement.prefix);
    if candidate_prefix == source_locator.prefix {
        return Err("RFC-0049 candidate prefix aliases the source".to_owned());
    }
    let candidate_backend = prefixed_backend(Arc::clone(&backend), candidate_prefix.clone())
        .map_err(|error| error.to_string())?;
    let media =
        prepare_t28_aligned_columnar_layout(&profile, &history, candidate_backend.as_ref()).await?;
    let mut objects = Vec::with_capacity(media.len());
    for (key, role) in media {
        objects.push(capture_identity(candidate_backend.as_ref(), &key, role).await?);
    }
    objects.sort_by(|left, right| {
        (left.key.as_str(), left.role).cmp(&(right.key.as_str(), right.role))
    });
    let candidate = T28AlignedChildV1::seal(
        candidate_prefix,
        placement.bucket.clone(),
        history.canonical_sha256,
        oracle.schema_sha256.clone(),
        oracle.fixture.covered_through_version,
        objects,
    )?;
    T28AlignedLayoutReader::open(
        Arc::clone(&candidate_backend),
        &candidate,
        oracle.fixture.covered_through_version,
    )
    .await?;

    let fixture = T28AlignedFixtureV1::seal(&source, source_locator, candidate, placement)?;
    let root = serde_json::to_vec(&fixture).map_err(|error| error.to_string())?;
    let root_bytes = u64::try_from(root.len()).unwrap_or(u64::MAX);
    let root_object_sha256 = content_sha256(&root);
    let root_key = format!(
        "{}/roots/sha256/{}.json",
        placement.prefix, root_object_sha256
    );
    let revision = backend
        .put(&root_key, Bytes::from(root.clone()), WriteCondition::Create)
        .await
        .map_err(|error| error.to_string())?;
    let root_generation = numeric_generation(&revision)?;
    let root_read = backend
        .get(&root_key, None, Some(&revision))
        .await
        .map_err(|error| error.to_string())?;
    if root_read.object_length != root_bytes
        || root_read.returned_range != (0..root_bytes)
        || root_read.bytes.as_ref() != root.as_slice()
        || root_read.revision.version.as_deref() != Some(root_generation.as_str())
    {
        return Err("RFC-0049 root publication identity mismatch".to_owned());
    }
    let locator = TypedLayoutPlacementLocatorV1::seal(
        fixture.fixture_id.clone(),
        fixture.root_sha256.clone(),
        placement.project.clone(),
        placement.bucket.clone(),
        placement.region.clone(),
        placement.prefix.clone(),
        root_key,
        root_generation,
        root_bytes,
        root_object_sha256,
    )?;
    let c0_reused_bytes = child_total_bytes(&fixture.source_c0);
    let c5v2_total_bytes = fixture.candidate.total_bytes();
    Ok(T28AlignedLayoutPublication {
        fixture,
        locator,
        c0_reused_bytes,
        c5v2_total_bytes,
        root_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_layout::{publish_t28_typed_layout, T28TypedLayoutPlacementInput};
    use crate::t28_layout::decode_t28_layout_oracle;
    use arrow::array::Int64Array;
    use async_trait::async_trait;
    use datafusion::prelude::SessionContext;
    use okv_object::{
        BackendDescriptor, BackendRead, ErrorClass, RevisionToken, StoreError, WriteCondition,
    };
    use std::collections::BTreeMap;
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct NumericGenerationBackend {
        next_generation: AtomicU64,
        objects: Mutex<BTreeMap<String, (Bytes, u64)>>,
    }

    impl NumericGenerationBackend {
        fn error(class: ErrorClass, detail: &str) -> StoreError {
            StoreError {
                class,
                detail: detail.to_owned(),
            }
        }
    }

    #[async_trait]
    impl Backend for NumericGenerationBackend {
        fn descriptor(&self) -> BackendDescriptor {
            BackendDescriptor {
                id: "numeric-generation-test".to_owned(),
                driver: "memory".to_owned(),
                driver_version: "1".to_owned(),
                server_version: "1".to_owned(),
                conditional_primitive: "generation-match".to_owned(),
                guarded_delete: true,
                delete_strategy: "generation-match".to_owned(),
            }
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            condition: WriteCondition,
        ) -> Result<RevisionToken, StoreError> {
            let mut objects = self.objects.lock().expect("objects lock");
            match condition {
                WriteCondition::Create if objects.contains_key(key) => {
                    return Err(Self::error(ErrorClass::AlreadyExists, "object exists"));
                }
                WriteCondition::Update(ref expected) => {
                    let current = objects
                        .get(key)
                        .ok_or_else(|| Self::error(ErrorClass::NotFound, "object missing"))?;
                    if expected
                        .version
                        .as_deref()
                        .and_then(|value| value.parse::<u64>().ok())
                        != Some(current.1)
                    {
                        return Err(Self::error(
                            ErrorClass::PreconditionFailed,
                            "generation mismatch",
                        ));
                    }
                }
                WriteCondition::Create | WriteCondition::Overwrite => {}
            }
            let generation = self
                .next_generation
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1);
            objects.insert(key.to_owned(), (bytes, generation));
            Ok(RevisionToken {
                e_tag: None,
                version: Some(generation.to_string()),
            })
        }

        async fn get(
            &self,
            key: &str,
            range: Option<Range<u64>>,
            expected: Option<&RevisionToken>,
        ) -> Result<BackendRead, StoreError> {
            let objects = self.objects.lock().expect("objects lock");
            let (bytes, generation) = objects
                .get(key)
                .ok_or_else(|| Self::error(ErrorClass::NotFound, "object missing"))?;
            let version = generation.to_string();
            if expected
                .and_then(|revision| revision.version.as_deref())
                .is_some_and(|value| value != version)
            {
                return Err(Self::error(
                    ErrorClass::PreconditionFailed,
                    "generation mismatch",
                ));
            }
            let object_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let returned_range = range.unwrap_or(0..object_length);
            let start = usize::try_from(returned_range.start)
                .map_err(|_| Self::error(ErrorClass::Other, "range overflow"))?;
            let end = usize::try_from(returned_range.end)
                .map_err(|_| Self::error(ErrorClass::Other, "range overflow"))?;
            let selected = bytes.get(start..end).ok_or_else(|| {
                Self::error(ErrorClass::PreconditionFailed, "range outside object")
            })?;
            Ok(BackendRead {
                bytes: Bytes::copy_from_slice(selected),
                revision: RevisionToken {
                    e_tag: None,
                    version: Some(version),
                },
                object_length,
                returned_range,
            })
        }

        async fn delete(
            &self,
            key: &str,
            expected: Option<&RevisionToken>,
        ) -> Result<(), StoreError> {
            let mut objects = self.objects.lock().expect("objects lock");
            let generation = objects
                .get(key)
                .ok_or_else(|| Self::error(ErrorClass::NotFound, "object missing"))?
                .1
                .to_string();
            if expected
                .and_then(|revision| revision.version.as_deref())
                .is_some_and(|value| value != generation)
            {
                return Err(Self::error(
                    ErrorClass::PreconditionFailed,
                    "generation mismatch",
                ));
            }
            objects.remove(key);
            Ok(())
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            Ok(self
                .objects
                .lock()
                .expect("objects lock")
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    fn oracle() -> T28LayoutOracleV1 {
        decode_t28_layout_oracle(
            include_bytes!("../../../../evals/oracles/t28-layout-geometry-v1-oracle.json"),
            "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86",
        )
        .expect("oracle")
    }

    #[tokio::test]
    async fn new_root_reuses_c0_and_serves_exact_c5v2_points() {
        let backend: Arc<dyn Backend> = Arc::new(NumericGenerationBackend::default());
        let oracle = oracle();
        let source = publish_t28_typed_layout(
            Arc::clone(&backend),
            &T28TypedLayoutPlacementInput {
                project: "doss-objectkv-dev".to_owned(),
                bucket: "doss-objectkv-dev-okv-evals".to_owned(),
                region: "us-central1".to_owned(),
                prefix: "tests/rfc0049/source".to_owned(),
            },
            &oracle,
            "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86",
        )
        .await
        .expect("source");
        let publication = publish_t28_aligned_layout(
            Arc::clone(&backend),
            &source.locator,
            &T28AlignedLayoutPlacementInput {
                project: "doss-objectkv-dev".to_owned(),
                bucket: "doss-objectkv-dev-okv-evals".to_owned(),
                region: "us-central1".to_owned(),
                prefix: "tests/rfc0049/candidate".to_owned(),
            },
            &oracle,
            "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86",
            PHYSICAL_PLAN_SHA256,
        )
        .await
        .expect("aligned publication");

        assert_eq!(publication.fixture.source_c0, source.fixture.children[0]);
        assert_eq!(publication.c0_reused_bytes, source.c0_total_bytes);
        assert_eq!(publication.c5v2_total_bytes, 13_695_766);
        let opened = T28OpenedAlignedLayout::open(backend, &publication.locator)
            .await
            .expect("open");
        let c0 = opened.c0().await.expect("C0");
        let c5v2 = opened.c5v2().await.expect("C5v2");
        assert_eq!(c5v2.resident_metadata_bytes(), 20_176);
        for (key, version) in [(0, 1), (31, 5), (8_191, 3), (16_383, 5)] {
            assert_eq!(
                c5v2.point(key, version).await.expect("C5v2 point"),
                c0.point(key, version).await.expect("C0 point").outcome
            );
        }

        let scan = c5v2.table_provider(256 * 1_024);
        let provider = scan.provider();
        let provider_stats = provider.stats();
        let context = SessionContext::new();
        context
            .register_table("c5v2", provider)
            .expect("register C5v2");
        let batches = context
            .sql("SELECT COUNT(*) AS row_count, SUM(quantity) AS quantity_sum FROM c5v2")
            .await
            .expect("plan C5v2 aggregate")
            .collect()
            .await
            .expect("execute C5v2 aggregate");
        assert_eq!(batches.len(), 1);
        let row_count = batches[0]
            .column_by_name("row_count")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .expect("row count");
        let quantity_sum = batches[0]
            .column_by_name("quantity_sum")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .expect("quantity sum");
        assert_eq!(row_count.value(0), 15_742);
        assert_eq!(quantity_sum.value(0), 67_524_278);
        let source = scan.source_snapshot();
        assert_eq!(source.projection_fetch_requests, 7);
        assert!(source.peak_fetch_bytes <= 256 * 1_024);
        assert_eq!(source.payload_requests, 0);
        assert_eq!(source.payload_response_bytes, 0);
        let provider = provider_stats.snapshot();
        assert_eq!(provider.stripes_read, 792);
        assert_eq!(provider.rows_emitted, 15_742);
        assert!(provider.peak_batch_rows <= 32);
    }

    #[test]
    fn root_rejects_candidate_closure_drift() {
        let fixture_bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/typed-layout-fixture-v1.json"
        ));
        let source: TypedLayoutFixtureV1 = serde_json::from_slice(fixture_bytes).expect("fixture");
        let source_c0 = source.children[0].clone();
        assert_eq!(source_c0.subject, TypedLayoutSubjectV1::C0IndexedRow);
        let mut candidate = T28AlignedChildV1 {
            subject: SUBJECT.to_owned(),
            prefix: "tests/candidate".to_owned(),
            bucket: source.bucket.clone(),
            format_id: FORMAT_ID.to_owned(),
            format_version: FORMAT_VERSION,
            canonical_history_sha256: source.canonical_history_sha256.clone(),
            schema_id: source.schema_id.clone(),
            schema_sha256: source.schema_sha256.clone(),
            covered_through_version: source.covered_through_version,
            manifest_key: MANIFEST_KEY.to_owned(),
            capabilities: CAPABILITIES.map(str::to_owned).to_vec(),
            objects: Vec::new(),
            closure_sha256: "0".repeat(64),
        };
        assert!(candidate.validate().is_err());
        candidate.format_version = 1;
        assert!(candidate.validate().is_err());
    }
}
