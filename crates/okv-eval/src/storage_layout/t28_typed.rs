//! RFC-0048 generation-pinned C0 row reader and matched `DataFusion` source.

use super::columnar_overlay::{
    prepare_t28_columnar_layout, T28ColumnarLayoutCore, T28ColumnarScanCore,
};
use super::{
    prepare_row_layout, project_snapshot, LogicalHistory, ProjectedRow, StorageLayoutProfile,
};
use crate::t28_layout::{
    decode_typed_layout_fixture, GenerationPinnedChildBackend, T28LayoutOracleV1,
    TypedLayoutCapabilityV1, TypedLayoutChildV1, TypedLayoutFixtureV1, TypedLayoutObjectIdentityV1,
    TypedLayoutObjectRoleV1, TypedLayoutPlacementLocatorV1, TypedLayoutSubjectV1,
};
use arrow::array::{ArrayRef, Int64Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use okv_htap::{RangeRowTableProvider, RangeStripeSource};
use okv_object::{
    content_sha256, prefixed_backend, read_indexed_point, read_planned_block, Backend,
    PointBlockPlanV1, PointRead, PointReadOutcome, RowObjectManifestV1, RowSegmentIndex,
    WriteCondition,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

const MAX_EMITTED_ROWS: usize = 128;
const C0_MANIFEST_KEY: &str = "layout/row/active-manifest";
const T28_SCHEMA_ID: &str = "objectkv.t28.typed-row.v1";

/// Provider placement selected before immutable RFC-0048 publication.
#[derive(Clone, Debug)]
pub struct T28TypedLayoutPlacementInput {
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
}

/// Immutable typed root and exact GCS generation returned by publication.
#[derive(Clone, Debug, Serialize)]
pub struct T28TypedLayoutPublication {
    pub fixture: TypedLayoutFixtureV1,
    pub locator: TypedLayoutPlacementLocatorV1,
    pub c0_total_bytes: u64,
    pub c5_total_bytes: u64,
    pub root_bytes: u64,
}

/// Read-only typed root reopened from one exact provider generation.
pub struct T28OpenedTypedLayout {
    fixture: TypedLayoutFixtureV1,
    backend: Arc<dyn Backend>,
}

/// Return the one dataset profile frozen by RFC-0048.
#[must_use]
pub fn t28_typed_layout_profile() -> StorageLayoutProfile {
    StorageLayoutProfile {
        key_count: 16_384,
        canonical_live_row_bytes: 512,
        opaque_payload_bytes: 480,
        base_version: 1,
        delta_cycles: 4,
        update_fraction: 0.125,
        delete_fraction: 0.01,
        point_operations: 1_024,
        target_run_object_bytes: 8 * 1_024 * 1_024,
        row_block_bytes: 64 * 1_024,
        columnar_block_rows: 128,
        overlay_cache_bytes: 8 * 1_024 * 1_024,
        seeds: vec![5_699],
        repeats: 1,
    }
}

/// Publish C0 and C5 from one independently verified logical history.
///
/// The backend is unscoped because the returned locator binds a fully
/// qualified root key. Child objects are written through the placement prefix
/// and recorded relative to it.
///
/// # Errors
///
/// Returns an error for oracle drift, invalid placement, publication failure,
/// an omitted numeric GCS generation, or any failed pinned reopen.
#[allow(clippy::too_many_lines)]
pub async fn publish_t28_typed_layout(
    backend: Arc<dyn Backend>,
    placement: &T28TypedLayoutPlacementInput,
    oracle: &T28LayoutOracleV1,
    oracle_sha256: &str,
) -> Result<T28TypedLayoutPublication, String> {
    oracle.validate()?;
    let profile = t28_typed_layout_profile();
    let history = LogicalHistory::generate(&profile, oracle.fixture.seed)?;
    validate_history_against_oracle(&history, oracle)?;
    let scoped = prefixed_backend(Arc::clone(&backend), placement.prefix.clone())
        .map_err(|error| error.to_string())?;

    let row = prepare_row_layout(&profile, &history, scoped.as_ref()).await?;
    let mut c0_objects = Vec::new();
    for reference in &row.manifest.segments {
        c0_objects.push(
            capture_identity(
                scoped.as_ref(),
                &reference.data_key,
                TypedLayoutObjectRoleV1::Data,
            )
            .await?,
        );
        c0_objects.push(
            capture_identity(
                scoped.as_ref(),
                &reference.index_key,
                TypedLayoutObjectRoleV1::Index,
            )
            .await?,
        );
    }
    c0_objects.push(
        capture_identity(
            scoped.as_ref(),
            C0_MANIFEST_KEY,
            TypedLayoutObjectRoleV1::Manifest,
        )
        .await?,
    );
    c0_objects.sort_by(|left, right| {
        (left.key.as_str(), left.role).cmp(&(right.key.as_str(), right.role))
    });
    let c0 = TypedLayoutChildV1::seal(
        TypedLayoutSubjectV1::C0IndexedRow,
        placement.bucket.clone(),
        history.canonical_sha256.clone(),
        T28_SCHEMA_ID.to_owned(),
        oracle.schema_sha256.clone(),
        oracle.fixture.covered_through_version,
        C0_MANIFEST_KEY.to_owned(),
        vec![
            TypedLayoutCapabilityV1::Point,
            TypedLayoutCapabilityV1::ProjectedScan,
        ],
        c0_objects,
    )?;

    let c5_media = prepare_t28_columnar_layout(&profile, &history, scoped.as_ref()).await?;
    let mut c5_objects = Vec::with_capacity(c5_media.len());
    for (key, role) in c5_media {
        c5_objects.push(capture_identity(scoped.as_ref(), &key, role).await?);
    }
    c5_objects.sort_by(|left, right| {
        (left.key.as_str(), left.role).cmp(&(right.key.as_str(), right.role))
    });
    let c5 = TypedLayoutChildV1::seal(
        TypedLayoutSubjectV1::C5ColumnarMain,
        placement.bucket.clone(),
        history.canonical_sha256.clone(),
        T28_SCHEMA_ID.to_owned(),
        oracle.schema_sha256.clone(),
        oracle.fixture.covered_through_version,
        "layout/columnar/active-manifest".to_owned(),
        vec![
            TypedLayoutCapabilityV1::Point,
            TypedLayoutCapabilityV1::ProjectedScan,
            TypedLayoutCapabilityV1::OpaquePayloadSplit,
        ],
        c5_objects,
    )?;

    T28RowLayoutReader::open(
        Arc::clone(&scoped),
        &c0,
        oracle.fixture.covered_through_version,
    )
    .await?;
    T28ColumnarLayoutReader::open(
        Arc::clone(&scoped),
        &c5,
        oracle.fixture.covered_through_version,
    )
    .await?;

    let c0_total_bytes = child_total_bytes(&c0);
    let c5_total_bytes = child_total_bytes(&c5);
    let fixture = TypedLayoutFixtureV1::seal(
        oracle.fixture.seed,
        oracle.fixture.key_count,
        oracle.fixture.record_count,
        oracle.fixture.live_row_count,
        history.canonical_sha256,
        T28_SCHEMA_ID.to_owned(),
        oracle.schema_sha256.clone(),
        oracle.fixture.covered_through_version,
        oracle_sha256.to_owned(),
        oracle.workload_plan_sha256.clone(),
        placement.project.clone(),
        placement.bucket.clone(),
        placement.region.clone(),
        vec![c0, c5],
    )?;
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
        return Err("RFC-0048 root publication identity mismatch".to_owned());
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
    Ok(T28TypedLayoutPublication {
        fixture,
        locator,
        c0_total_bytes,
        c5_total_bytes,
        root_bytes,
    })
}

impl T28OpenedTypedLayout {
    /// Reopen one typed root and bind every later child read to its generation.
    ///
    /// # Errors
    ///
    /// Returns an error for locator drift, root generation or content drift,
    /// cross-placement identity, or an invalid child closure.
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
            return Err("RFC-0048 typed root provider identity mismatch".to_owned());
        }
        let fixture = decode_typed_layout_fixture(&read.bytes, &locator.root_sha256)?;
        if fixture.fixture_id != locator.fixture_id
            || fixture.project != locator.project
            || fixture.bucket != locator.bucket
            || fixture.region != locator.region
        {
            return Err("RFC-0048 typed root placement identity mismatch".to_owned());
        }
        let scoped =
            prefixed_backend(backend, locator.prefix.clone()).map_err(|error| error.to_string())?;
        Ok(Self {
            fixture,
            backend: scoped,
        })
    }

    /// Return the authenticated shared root.
    #[must_use]
    pub const fn fixture(&self) -> &TypedLayoutFixtureV1 {
        &self.fixture
    }

    /// Open the C0 control at the root's exact version.
    ///
    /// # Errors
    ///
    /// Returns an error when the C0 closure is absent or cannot be reopened.
    pub async fn c0(&self) -> Result<T28RowLayoutReader, String> {
        let child = self
            .fixture
            .child(TypedLayoutSubjectV1::C0IndexedRow)
            .ok_or_else(|| "RFC-0048 typed root omits C0".to_owned())?;
        T28RowLayoutReader::open(
            Arc::clone(&self.backend),
            child,
            self.fixture.covered_through_version,
        )
        .await
    }

    /// Open the C5 candidate at the root's exact version.
    ///
    /// # Errors
    ///
    /// Returns an error when the C5 closure is absent or cannot be reopened.
    pub async fn c5(&self) -> Result<T28ColumnarLayoutReader, String> {
        let child = self
            .fixture
            .child(TypedLayoutSubjectV1::C5ColumnarMain)
            .ok_or_else(|| "RFC-0048 typed root omits C5".to_owned())?;
        T28ColumnarLayoutReader::open(
            Arc::clone(&self.backend),
            child,
            self.fixture.covered_through_version,
        )
        .await
    }
}

async fn capture_identity(
    backend: &dyn Backend,
    key: &str,
    role: TypedLayoutObjectRoleV1,
) -> Result<TypedLayoutObjectIdentityV1, String> {
    let read = backend
        .get(key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if read.returned_range != (0..read.object_length)
        || u64::try_from(read.bytes.len()).unwrap_or(u64::MAX) != read.object_length
    {
        return Err("RFC-0048 published child full-read framing mismatch".to_owned());
    }
    Ok(TypedLayoutObjectIdentityV1 {
        role,
        key: key.to_owned(),
        generation: numeric_generation(&read.revision)?,
        length: read.object_length,
        sha256: content_sha256(&read.bytes),
    })
}

fn numeric_generation(revision: &okv_object::RevisionToken) -> Result<String, String> {
    let generation = revision
        .version
        .as_ref()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| "RFC-0048 provider omitted numeric object generation".to_owned())?;
    Ok(generation.clone())
}

fn child_total_bytes(child: &TypedLayoutChildV1) -> u64 {
    child
        .objects
        .iter()
        .fold(0_u64, |total, object| total.saturating_add(object.length))
}

fn validate_history_against_oracle(
    history: &LogicalHistory,
    oracle: &T28LayoutOracleV1,
) -> Result<(), String> {
    let rows = history.final_rows(oracle.fixture.covered_through_version);
    let quantity_sum = rows.iter().fold(0_i128, |total, row| {
        total.saturating_add(i128::from(row.quantity))
    });
    if history.canonical_sha256 != oracle.fixture.canonical_history_sha256
        || u64::try_from(history.records.len()).unwrap_or(u64::MAX) != oracle.fixture.record_count
        || u64::try_from(rows.len()).unwrap_or(u64::MAX) != oracle.fixture.live_row_count
        || ordered_projection_sha256(&rows) != oracle.fixture.ordered_projection_sha256
        || quantity_sum.to_string() != oracle.fixture.aggregate.quantity_sum
    {
        return Err("RFC-0048 Rust history differs from the independent oracle".to_owned());
    }
    Ok(())
}

fn ordered_projection_sha256(rows: &[ProjectedRow]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"okv-t28-ordered-projection-v1\0");
    digest.update(u64::try_from(rows.len()).unwrap_or(u64::MAX).to_be_bytes());
    for row in rows {
        digest.update(row.key.to_be_bytes());
        digest.update(row.tenant.to_be_bytes());
        digest.update(row.category.to_be_bytes());
        digest.update(row.quantity.to_be_bytes());
    }
    format!("{:x}", digest.finalize())
}

/// C5-specific object-fetch counters for one matched projected scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct T28ColumnarScanSnapshot {
    pub projection_fetch_requests: u64,
    pub peak_fetch_bytes: u64,
    pub payload_requests: u64,
    pub payload_response_bytes: u64,
}

/// Generation-pinned C5 reader over projection and payload objects.
pub struct T28ColumnarLayoutReader {
    inner: T28ColumnarLayoutCore,
}

/// One C5 DataFusion provider and its independently sampled source counters.
pub struct T28ColumnarScan {
    inner: T28ColumnarScanCore,
}

impl T28ColumnarLayoutReader {
    /// Open C5 from one already fixture-scoped object backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor, manifest, index, object closure,
    /// or requested version disagree.
    pub async fn open(
        inner: Arc<dyn Backend>,
        child: &TypedLayoutChildV1,
        read_version: u64,
    ) -> Result<Self, String> {
        Ok(Self {
            inner: T28ColumnarLayoutCore::open(inner, child, read_version).await?,
        })
    }

    /// Execute one complete C5 point lookup through projection and payload media.
    ///
    /// # Errors
    ///
    /// Returns an error for a version beyond the opened snapshot or any failed
    /// generation, framing, or checksum check.
    pub async fn point(&self, key: u64, read_version: u64) -> Result<PointReadOutcome, String> {
        self.inner.point(key, read_version).await
    }

    /// Create the C5 semantic provider over the matched bounded scheduler.
    #[must_use]
    pub fn table_provider(&self, scan_fetch_target_bytes: usize) -> T28ColumnarScan {
        T28ColumnarScan {
            inner: self.inner.table_provider(scan_fetch_target_bytes),
        }
    }

    /// Bytes retained after manifest and index warmup.
    #[must_use]
    pub fn resident_metadata_bytes(&self) -> u64 {
        self.inner.resident_metadata_bytes()
    }
}

impl T28ColumnarScan {
    /// Return the provider registered in one fresh DataFusion context.
    #[must_use]
    pub fn provider(&self) -> Arc<okv_htap::RangeStripeTableProvider> {
        self.inner.provider()
    }

    /// Sample C5 counters after the final result batch has been drained.
    #[must_use]
    pub fn source_snapshot(&self) -> T28ColumnarScanSnapshot {
        let source = self.inner.source_snapshot();
        T28ColumnarScanSnapshot {
            projection_fetch_requests: source.projection_fetch_requests,
            peak_fetch_bytes: source.peak_fetch_bytes,
            payload_requests: source.payload_requests,
            payload_response_bytes: source.payload_response_bytes,
        }
    }
}

#[derive(Clone, Debug)]
struct RowBlockReadPlan {
    data_key: String,
    block: PointBlockPlanV1,
}

/// Authenticated C0 metadata plus a read-only generation-pinned data backend.
pub struct T28RowLayoutReader {
    backend: Arc<dyn Backend>,
    manifest: RowObjectManifestV1,
    indexes: BTreeMap<String, RowSegmentIndex>,
    blocks: Vec<RowBlockReadPlan>,
    read_version: u64,
    resident_metadata_bytes: u64,
}

impl Debug for T28RowLayoutReader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("T28RowLayoutReader")
            .field("segments", &self.manifest.segments.len())
            .field("blocks", &self.blocks.len())
            .field("read_version", &self.read_version)
            .field("resident_metadata_bytes", &self.resident_metadata_bytes)
            .finish_non_exhaustive()
    }
}

impl T28RowLayoutReader {
    /// Open C0 from one already fixture-scoped object backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor, manifest, warmed indexes, format
    /// generations, or complete named object closure disagree.
    pub async fn open(
        inner: Arc<dyn Backend>,
        child: &TypedLayoutChildV1,
        read_version: u64,
    ) -> Result<Self, String> {
        if child.subject != TypedLayoutSubjectV1::C0IndexedRow
            || read_version == 0
            || read_version > child.covered_through_version
        {
            return Err("invalid RFC-0048 C0 reader identity or version".to_owned());
        }
        let backend: Arc<dyn Backend> = Arc::new(GenerationPinnedChildBackend::new(inner, child)?);
        let manifest_read = backend
            .get(&child.manifest_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)?;
        if manifest.covered_through != child.covered_through_version {
            return Err("RFC-0048 C0 manifest coverage mismatch".to_owned());
        }

        let mut expected_keys = BTreeSet::from([child.manifest_key.as_str()]);
        let mut indexes = BTreeMap::new();
        let mut blocks = Vec::new();
        let mut resident_metadata_bytes =
            u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX);
        for reference in &manifest.segments {
            let data = child
                .object(&reference.data_key)
                .filter(|object| object.role == TypedLayoutObjectRoleV1::Data)
                .ok_or_else(|| "RFC-0048 C0 data object is absent".to_owned())?;
            let index_identity = child
                .object(&reference.index_key)
                .filter(|object| object.role == TypedLayoutObjectRoleV1::Index)
                .ok_or_else(|| "RFC-0048 C0 index object is absent".to_owned())?;
            if data.length != reference.data_bytes
                || data.sha256 != reference.data_sha256
                || index_identity.length != reference.index_bytes
                || index_identity.sha256 != reference.index_sha256
            {
                return Err("RFC-0048 C0 descriptor differs from its manifest".to_owned());
            }
            expected_keys.insert(&reference.data_key);
            expected_keys.insert(&reference.index_key);
            let index_read = backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| error.to_string())?;
            let index = RowSegmentIndex::decode(&index_read.bytes)?;
            reference.validate_index(&index_read.bytes, &index)?;
            resident_metadata_bytes = resident_metadata_bytes
                .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
            blocks.extend(
                index
                    .block_plans()
                    .into_iter()
                    .map(|block| RowBlockReadPlan {
                        data_key: reference.data_key.clone(),
                        block,
                    }),
            );
            indexes.insert(reference.data_key.clone(), index);
        }
        let actual_keys = child
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<BTreeSet<_>>();
        if expected_keys != actual_keys {
            return Err("RFC-0048 C0 descriptor has unreachable or missing media".to_owned());
        }
        Ok(Self {
            backend,
            manifest,
            indexes,
            blocks,
            read_version,
            resident_metadata_bytes,
        })
    }

    /// Execute one complete C0 point lookup with one range GET after metadata warmup.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent warmed index or a failed authenticated
    /// checksummed range read.
    pub async fn point(&self, key: u64, read_version: u64) -> Result<PointRead, String> {
        if read_version == 0 || read_version > self.read_version {
            return Err("RFC-0048 C0 point version exceeds the opened snapshot".to_owned());
        }
        let key = key.to_be_bytes();
        let Some(reference) = self.manifest.locate(&key) else {
            return Err("RFC-0048 C0 point key is outside the manifest".to_owned());
        };
        let index = self
            .indexes
            .get(&reference.data_key)
            .ok_or_else(|| "RFC-0048 C0 warmed index is absent".to_owned())?;
        read_indexed_point(
            self.backend.as_ref(),
            &reference.data_key,
            None,
            index,
            &key,
            read_version,
        )
        .await
    }

    /// Create the C0 semantic provider over the same bounded scheduler as C5.
    #[must_use]
    pub fn table_provider(&self) -> Arc<RangeRowTableProvider> {
        Arc::new(RangeRowTableProvider::new(Arc::new(RowProjectionSource {
            backend: Arc::clone(&self.backend),
            blocks: self.blocks.clone(),
            read_version: self.read_version,
        })))
    }

    /// Bytes retained after manifest and sparse-index warmup.
    #[must_use]
    pub const fn resident_metadata_bytes(&self) -> u64 {
        self.resident_metadata_bytes
    }
}

struct RowProjectionSource {
    backend: Arc<dyn Backend>,
    blocks: Vec<RowBlockReadPlan>,
    read_version: u64,
}

impl Debug for RowProjectionSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowProjectionSource")
            .field("blocks", &self.blocks.len())
            .field("read_version", &self.read_version)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RangeStripeSource for RowProjectionSource {
    fn schema(&self) -> SchemaRef {
        projection_schema()
    }

    fn stripe_count(&self) -> usize {
        self.blocks.len()
    }

    async fn read_stripe(
        &self,
        stripe_index: usize,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<RecordBatch> {
        let plan = self.blocks.get(stripe_index).ok_or_else(|| {
            DataFusionError::Execution("C0 row block index is outside the scan plan".to_owned())
        })?;
        let records = read_planned_block(self.backend.as_ref(), &plan.data_key, None, &plan.block)
            .await
            .map_err(DataFusionError::Execution)?;
        let rows =
            project_snapshot(&records, self.read_version).map_err(DataFusionError::Execution)?;
        if rows.len() > MAX_EMITTED_ROWS {
            return Err(DataFusionError::Execution(format!(
                "C0 row block emits {} rows, limit is {MAX_EMITTED_ROWS}",
                rows.len()
            )));
        }
        projection_batch(&rows, projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn projection_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("tenant", DataType::UInt32, false),
        Field::new("category", DataType::UInt16, false),
        Field::new("quantity", DataType::Int64, false),
    ]))
}

fn projection_batch(
    rows: &[ProjectedRow],
    projection: Option<&[usize]>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let full_schema = projection_schema();
    let indices = projection.map_or_else(
        || (0..full_schema.fields().len()).collect(),
        <[usize]>::to_vec,
    );
    let arrays = indices
        .iter()
        .map(|index| match index {
            0 => Ok(Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.key).collect::<Vec<_>>(),
            )) as ArrayRef),
            1 => Ok(Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.tenant).collect::<Vec<_>>(),
            )) as ArrayRef),
            2 => Ok(Arc::new(UInt16Array::from(
                rows.iter().map(|row| row.category).collect::<Vec<_>>(),
            )) as ArrayRef),
            3 => Ok(Arc::new(Int64Array::from(
                rows.iter().map(|row| row.quantity).collect::<Vec<_>>(),
            )) as ArrayRef),
            other => Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "projection column {other} is outside the C0 schema"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(full_schema.project(&indices)?);
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
}

#[cfg(test)]
mod tests {
    use super::{projection_batch, projection_schema, RowBlockReadPlan, RowProjectionSource};
    use crate::storage_layout::{prepare_row_layout, LogicalHistory, StorageLayoutProfile};
    use arrow::array::UInt64Array;
    use datafusion::prelude::SessionContext;
    use okv_htap::RangeRowTableProvider;
    use okv_object::memory_backend;
    use std::sync::Arc;

    fn profile() -> StorageLayoutProfile {
        StorageLayoutProfile {
            key_count: 1_024,
            canonical_live_row_bytes: 512,
            opaque_payload_bytes: 480,
            base_version: 1,
            delta_cycles: 4,
            update_fraction: 0.125,
            delete_fraction: 0.01,
            point_operations: 64,
            target_run_object_bytes: 512 * 1_024,
            row_block_bytes: 64 * 1_024,
            columnar_block_rows: 128,
            overlay_cache_bytes: 64 * 1_024,
            seeds: vec![5_699],
            repeats: 1,
        }
    }

    #[tokio::test]
    async fn c0_source_streams_exact_ordered_projection_through_datafusion() {
        let profile = profile();
        let history = LogicalHistory::generate(&profile, 5_699).expect("history");
        let backend = memory_backend();
        let prepared = prepare_row_layout(&profile, &history, backend.as_ref())
            .await
            .expect("row layout");
        let blocks = prepared
            .manifest
            .segments
            .iter()
            .flat_map(|reference| {
                prepared.indexes[&reference.data_key]
                    .block_plans()
                    .into_iter()
                    .map(|block| RowBlockReadPlan {
                        data_key: reference.data_key.clone(),
                        block,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let source = Arc::new(RowProjectionSource {
            backend: Arc::clone(&backend),
            blocks,
            read_version: 5,
        });
        let provider = Arc::new(RangeRowTableProvider::new(source));
        let context = SessionContext::new();
        context.register_table("c0", provider).expect("register C0");
        let batches = context
            .sql("SELECT key, tenant, category, quantity FROM c0 ORDER BY key")
            .await
            .expect("plan C0 query")
            .collect()
            .await
            .expect("execute C0 query");
        let keys = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("key")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .expect("key column")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            history
                .final_rows(5)
                .iter()
                .map(|row| row.key)
                .collect::<Vec<_>>()
        );
        assert_eq!(projection_schema().fields().len(), 4);
        assert!(projection_batch(&[], Some(&[0, 3])).is_ok());
    }
}
