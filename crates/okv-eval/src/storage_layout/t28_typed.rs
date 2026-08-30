//! RFC-0048 generation-pinned C0 row reader and matched `DataFusion` source.

use super::columnar_overlay::{T28ColumnarLayoutCore, T28ColumnarScanCore};
use super::{project_snapshot, ProjectedRow};
use crate::t28_layout::{
    GenerationPinnedChildBackend, TypedLayoutChildV1, TypedLayoutObjectRoleV1, TypedLayoutSubjectV1,
};
use arrow::array::{ArrayRef, Int64Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use okv_htap::{RangeRowTableProvider, RangeStripeSource};
use okv_object::{
    read_indexed_point, read_planned_block, Backend, PointBlockPlanV1, PointRead, PointReadOutcome,
    RowObjectManifestV1, RowSegmentIndex,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

const MAX_EMITTED_ROWS: usize = 128;

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
