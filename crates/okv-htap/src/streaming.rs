use arrow::array::{Array, ArrayRef, StringArray, UInt32Array, UInt64Array, UInt8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::equivalence::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures_util::{stream, StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Debug, Formatter, Write};
use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const STREAMING_OVERLAY_CONTRACT_VERSION: u32 = 1;

const TARGET_VERSION: u64 = 12;
const AFTER_TARGET_VERSION: u64 = 13;
const ANALYTICAL_COVERAGE: u64 = 13;
const WEST_WATERMARK: u64 = 5;
const EAST_WATERMARK: u64 = 8;
const OUTPUT_BATCH_ROWS: usize = 2;
const MAXIMUM_GROUP_ROWS: usize = 16;
const MAXIMUM_BUFFERED_ROWS: u64 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingOverlayMode {
    Correct,
    MaterializeInputs,
    ResetGroupAtBatchBoundary,
    StartTailAtMaximumWatermark,
    RebaseContinuationTarget,
    AcceptUnsortedInput,
}

impl StreamingOverlayMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::MaterializeInputs => "materialize_inputs",
            Self::ResetGroupAtBatchBoundary => "reset_group_at_batch_boundary",
            Self::StartTailAtMaximumWatermark => "start_tail_at_max_watermark",
            Self::RebaseContinuationTarget => "rebase_continuation_target",
            Self::AcceptUnsortedInput => "accept_unsorted_input",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "the report retains each frozen streaming hard gate independently"
)]
pub struct StreamingOverlayReport {
    pub schema_version: u32,
    pub seed: u64,
    pub mode: String,
    pub target_version: u64,
    pub minimum_base_watermark: u64,
    pub maximum_base_watermark: u64,
    pub analytical_coverage: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub base_rows: u64,
    pub tail_rows: u64,
    pub output_rows: u64,
    pub input_batches: u64,
    pub output_batches: u64,
    pub peak_buffered_rows: u64,
    pub peak_buffered_bytes: u64,
    pub maximum_group_rows_observed: u64,
    pub materialized_input_rows: u64,
    pub tail_bytes: u64,
    pub parquet_bytes: u64,
    pub spill_bytes: u64,
    pub parquet_round_trip: bool,
    pub arrow_tail_complete: bool,
    pub incremental_emission: bool,
    pub input_order_validated: bool,
    pub batch_boundary_groups_preserved: bool,
    pub independent_watermarks: bool,
    pub continuation_target_bound: bool,
    pub buffer_bound_holds: bool,
    pub output_order_declared: bool,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotRow {
    id: u64,
    status: String,
    partition: String,
    amount_cents: u64,
    priority: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BaseRow {
    row: SnapshotRow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TailEffect {
    id: u64,
    commit_version: u64,
    operation: String,
    after: Option<SnapshotRow>,
    source_batch: u64,
}

#[derive(Debug, Default)]
struct StreamExecutionStats {
    input_batches: AtomicU64,
    output_batches: AtomicU64,
    peak_buffered_rows: AtomicU64,
    peak_buffered_bytes: AtomicU64,
    maximum_group_rows: AtomicU64,
    materialized_input_rows: AtomicU64,
    output_order_declarations: AtomicU64,
}

impl StreamExecutionStats {
    fn record_input_batch(&self) {
        self.input_batches.fetch_add(1, Ordering::Relaxed);
    }

    fn record_output_batch(&self) {
        self.output_batches.fetch_add(1, Ordering::Relaxed);
    }

    fn record_buffered(&self, rows: u64, bytes: u64) {
        self.peak_buffered_rows.fetch_max(rows, Ordering::Relaxed);
        self.peak_buffered_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    fn record_group(&self, rows: usize) {
        self.maximum_group_rows
            .fetch_max(u64::try_from(rows).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

pub struct StreamingZebraSnapshotTableProvider {
    schema: SchemaRef,
    base: Arc<dyn TableProvider>,
    tail: Arc<dyn TableProvider>,
    target_version: u64,
    watermarks: BTreeMap<String, u64>,
    analytical_coverage: u64,
    mode: StreamingOverlayMode,
    stats: Arc<StreamExecutionStats>,
}

impl Debug for StreamingZebraSnapshotTableProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamingZebraSnapshotTableProvider")
            .field("target_version", &self.target_version)
            .field("watermarks", &self.watermarks)
            .field("analytical_coverage", &self.analytical_coverage)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl StreamingZebraSnapshotTableProvider {
    fn new(
        base: Arc<dyn TableProvider>,
        tail: Arc<dyn TableProvider>,
        target_version: u64,
        watermarks: BTreeMap<String, u64>,
        analytical_coverage: u64,
        mode: StreamingOverlayMode,
        stats: Arc<StreamExecutionStats>,
    ) -> Self {
        Self {
            schema: snapshot_schema(),
            base,
            tail,
            target_version,
            watermarks,
            analytical_coverage,
            mode,
            stats,
        }
    }
}

#[async_trait]
impl TableProvider for StreamingZebraSnapshotTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let minimum_watermark = self
            .watermarks
            .values()
            .copied()
            .min()
            .ok_or_else(|| DataFusionError::Execution("no base watermarks".to_owned()))?;
        if self.watermarks.values().any(|watermark| {
            *watermark > self.target_version || self.target_version > self.analytical_coverage
        }) {
            return Err(DataFusionError::Execution(format!(
                "snapshot unavailable: watermarks={:?}, target={}, coverage={}",
                self.watermarks, self.target_version, self.analytical_coverage
            )));
        }

        let base = self.base.scan(state, None, &[], None).await?;
        let tail = self.tail.scan(state, None, &[], None).await?;
        let overlay: Arc<dyn ExecutionPlan> = Arc::new(StreamingSnapshotOverlayExec::new(
            base,
            tail,
            self.target_version,
            minimum_watermark,
            self.watermarks.clone(),
            self.mode,
            Arc::clone(&self.stats),
        ));

        let Some(indices) = projection else {
            return Ok(overlay);
        };
        let expressions = indices
            .iter()
            .map(|index| {
                let field = self.schema.field(*index);
                (
                    Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(ProjectionExec::try_new(expressions, overlay)?))
    }
}

#[derive(Debug)]
struct StreamingSnapshotOverlayExec {
    base: Arc<dyn ExecutionPlan>,
    tail: Arc<dyn ExecutionPlan>,
    target_version: u64,
    minimum_watermark: u64,
    watermarks: BTreeMap<String, u64>,
    mode: StreamingOverlayMode,
    stats: Arc<StreamExecutionStats>,
    properties: Arc<PlanProperties>,
}

impl StreamingSnapshotOverlayExec {
    fn new(
        base: Arc<dyn ExecutionPlan>,
        tail: Arc<dyn ExecutionPlan>,
        target_version: u64,
        minimum_watermark: u64,
        watermarks: BTreeMap<String, u64>,
        mode: StreamingOverlayMode,
        stats: Arc<StreamExecutionStats>,
    ) -> Self {
        let schema = snapshot_schema();
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("id", 0)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("partition", 2)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ])
        .expect("streaming output ordering is non-empty");
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, [ordering]),
            Partitioning::UnknownPartitioning(1),
            if mode == StreamingOverlayMode::MaterializeInputs {
                EmissionType::Final
            } else {
                EmissionType::Incremental
            },
            Boundedness::Bounded,
        ));
        stats
            .output_order_declarations
            .fetch_add(1, Ordering::Relaxed);
        Self {
            base,
            tail,
            target_version,
            minimum_watermark,
            watermarks,
            mode,
            stats,
            properties,
        }
    }
}

impl DisplayAs for StreamingSnapshotOverlayExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "StreamingSnapshotOverlayExec: target={}, minimum_watermark={}, mode={}",
                self.target_version,
                self.minimum_watermark,
                self.mode.id()
            ),
            DisplayFormatType::TreeRender => write!(formatter, "StreamingSnapshotOverlayExec"),
        }
    }
}

impl ExecutionPlan for StreamingSnapshotOverlayExec {
    fn name(&self) -> &'static str {
        "StreamingSnapshotOverlayExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base, &self.tail]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "StreamingSnapshotOverlayExec requires two children, received {}",
                children.len()
            )));
        }
        let tail = children.pop().expect("length checked");
        let base = children.pop().expect("length checked");
        Ok(Arc::new(Self::new(
            base,
            tail,
            self.target_version,
            self.minimum_watermark,
            self.watermarks.clone(),
            self.mode,
            Arc::clone(&self.stats),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "StreamingSnapshotOverlayExec has one partition, received {partition}"
            )));
        }
        let base = self.base.execute(0, Arc::clone(&context))?;
        let tail = self.tail.execute(0, context)?;
        let schema = snapshot_schema();
        if self.mode == StreamingOverlayMode::MaterializeInputs {
            return Ok(materialized_stream(
                base,
                tail,
                self.target_version,
                self.minimum_watermark,
                self.watermarks.clone(),
                Arc::clone(&self.stats),
            ));
        }
        let state = StreamingMergeState::new(
            base,
            tail,
            self.target_version,
            if self.mode == StreamingOverlayMode::StartTailAtMaximumWatermark {
                self.watermarks
                    .values()
                    .copied()
                    .max()
                    .unwrap_or(self.minimum_watermark)
            } else {
                self.minimum_watermark
            },
            self.watermarks.clone(),
            self.mode,
            Arc::clone(&self.stats),
        );
        let batches = stream::try_unfold(state, |mut state| async move {
            let Some(batch) = state.next_output_batch().await? else {
                return Ok(None);
            };
            Ok::<_, DataFusionError>(Some((batch, state)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

struct BaseCursor {
    stream: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    row_index: usize,
    batch_number: u64,
    last_key: Option<(u64, String)>,
    watermarks: BTreeMap<String, u64>,
    validate_order: bool,
    stats: Arc<StreamExecutionStats>,
}

impl BaseCursor {
    fn new(
        stream: SendableRecordBatchStream,
        watermarks: BTreeMap<String, u64>,
        validate_order: bool,
        stats: Arc<StreamExecutionStats>,
    ) -> Self {
        Self {
            stream,
            batch: None,
            row_index: 0,
            batch_number: 0,
            last_key: None,
            watermarks,
            validate_order,
            stats,
        }
    }

    async fn next_row(&mut self) -> DataFusionResult<Option<BaseRow>> {
        loop {
            let Some(batch) = &self.batch else {
                if !self.load_batch().await? {
                    return Ok(None);
                }
                continue;
            };
            if self.row_index >= batch.num_rows() {
                self.batch = None;
                continue;
            }

            let index = self.row_index;
            self.row_index = self.row_index.saturating_add(1);
            let ids = required_array::<UInt64Array>(batch, "id")?;
            let statuses = required_array::<StringArray>(batch, "status")?;
            let partitions = required_array::<StringArray>(batch, "partition")?;
            let amounts = required_array::<UInt64Array>(batch, "total_cents")?;
            let writer_schemas = required_array::<UInt32Array>(batch, "writer_schema")?;
            let base_watermarks = required_array::<UInt64Array>(batch, "base_watermark")?;
            if ids.is_null(index) {
                return Err(DataFusionError::Execution(
                    "base primary key is null".to_owned(),
                ));
            }
            if writer_schemas.value(index) != 1 {
                return Err(DataFusionError::Execution(format!(
                    "unsupported base writer schema {}",
                    writer_schemas.value(index)
                )));
            }
            let partition = partitions.value(index).to_owned();
            let watermark = base_watermarks.value(index);
            let expected_watermark = self.watermarks.get(&partition).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "base row uses unknown physical partition {partition}"
                ))
            })?;
            if watermark != *expected_watermark {
                return Err(DataFusionError::Execution(format!(
                    "base row watermark {watermark} does not match partition {partition} watermark {expected_watermark}"
                )));
            }
            let key = (ids.value(index), partition.clone());
            if self.validate_order && self.last_key.as_ref().is_some_and(|last| last > &key) {
                return Err(DataFusionError::Execution(format!(
                    "base input is not ordered: {key:?} follows {:?}",
                    self.last_key
                )));
            }
            self.last_key = Some(key);
            return Ok(Some(BaseRow {
                row: SnapshotRow {
                    id: ids.value(index),
                    status: statuses.value(index).to_owned(),
                    partition,
                    amount_cents: amounts.value(index),
                    priority: 0,
                },
            }));
        }
    }

    async fn load_batch(&mut self) -> DataFusionResult<bool> {
        let Some(batch) = self.stream.next().await.transpose()? else {
            return Ok(false);
        };
        self.batch_number = self.batch_number.saturating_add(1);
        self.row_index = 0;
        self.stats.record_input_batch();
        self.batch = Some(batch);
        Ok(true)
    }

    fn remaining_rows(&self) -> u64 {
        self.batch.as_ref().map_or(0, |batch| {
            u64::try_from(batch.num_rows().saturating_sub(self.row_index)).unwrap_or(u64::MAX)
        })
    }

    fn buffered_bytes(&self) -> u64 {
        self.batch.as_ref().map_or(0, |batch| {
            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
        })
    }
}

struct TailCursor {
    stream: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    row_index: usize,
    batch_number: u64,
    last_key: Option<(u64, u64)>,
    lower_bound: u64,
    target_version: u64,
    validate_order: bool,
    stats: Arc<StreamExecutionStats>,
}

impl TailCursor {
    fn new(
        stream: SendableRecordBatchStream,
        lower_bound: u64,
        target_version: u64,
        validate_order: bool,
        stats: Arc<StreamExecutionStats>,
    ) -> Self {
        Self {
            stream,
            batch: None,
            row_index: 0,
            batch_number: 0,
            last_key: None,
            lower_bound,
            target_version,
            validate_order,
            stats,
        }
    }

    async fn next_row(&mut self) -> DataFusionResult<Option<TailEffect>> {
        loop {
            let Some(batch) = &self.batch else {
                if !self.load_batch().await? {
                    return Ok(None);
                }
                continue;
            };
            if self.row_index >= batch.num_rows() {
                self.batch = None;
                continue;
            }

            let index = self.row_index;
            self.row_index = self.row_index.saturating_add(1);
            let ids = required_array::<UInt64Array>(batch, "id")?;
            let versions = required_array::<UInt64Array>(batch, "commit_version")?;
            let operations = required_array::<StringArray>(batch, "operation")?;
            let _previous = required_array::<StringArray>(batch, "previous_partition")?;
            let statuses = required_array::<StringArray>(batch, "status")?;
            let partitions = required_array::<StringArray>(batch, "partition")?;
            let amounts = required_array::<UInt64Array>(batch, "amount_cents")?;
            let priorities = required_array::<UInt8Array>(batch, "priority")?;
            let writer_schemas = required_array::<UInt32Array>(batch, "writer_schema")?;
            if ids.is_null(index) {
                return Err(DataFusionError::Execution(
                    "tail primary key is null".to_owned(),
                ));
            }
            if writer_schemas.value(index) != 2 {
                return Err(DataFusionError::Execution(format!(
                    "unsupported tail writer schema {}",
                    writer_schemas.value(index)
                )));
            }
            let id = ids.value(index);
            let version = versions.value(index);
            let key = (id, version);
            if self.validate_order && self.last_key.is_some_and(|last| last > key) {
                return Err(DataFusionError::Execution(format!(
                    "tail input is not ordered: {key:?} follows {:?}",
                    self.last_key
                )));
            }
            self.last_key = Some(key);
            let operation = operations.value(index);
            if !matches!(operation, "INSERT" | "UPDATE" | "DELETE") {
                return Err(DataFusionError::Execution(format!(
                    "unknown tail operation {operation}"
                )));
            }
            let after = if operation == "DELETE" {
                None
            } else {
                if statuses.is_null(index)
                    || partitions.is_null(index)
                    || amounts.is_null(index)
                    || priorities.is_null(index)
                {
                    return Err(DataFusionError::Execution(format!(
                        "{operation} for id {id} has an incomplete after-image"
                    )));
                }
                Some(SnapshotRow {
                    id,
                    status: statuses.value(index).to_owned(),
                    partition: partitions.value(index).to_owned(),
                    amount_cents: amounts.value(index),
                    priority: priorities.value(index),
                })
            };
            if version <= self.lower_bound || version > self.target_version {
                continue;
            }
            return Ok(Some(TailEffect {
                id,
                commit_version: version,
                operation: operation.to_owned(),
                after,
                source_batch: self.batch_number,
            }));
        }
    }

    async fn load_batch(&mut self) -> DataFusionResult<bool> {
        let Some(batch) = self.stream.next().await.transpose()? else {
            return Ok(false);
        };
        self.batch_number = self.batch_number.saturating_add(1);
        self.row_index = 0;
        self.stats.record_input_batch();
        self.batch = Some(batch);
        Ok(true)
    }

    fn remaining_rows(&self) -> u64 {
        self.batch.as_ref().map_or(0, |batch| {
            u64::try_from(batch.num_rows().saturating_sub(self.row_index)).unwrap_or(u64::MAX)
        })
    }

    fn buffered_bytes(&self) -> u64 {
        self.batch.as_ref().map_or(0, |batch| {
            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
        })
    }
}

struct StreamingMergeState {
    base: BaseCursor,
    tail: TailCursor,
    next_base: Option<BaseRow>,
    next_tail: Option<TailEffect>,
    pending_output: VecDeque<SnapshotRow>,
    mode: StreamingOverlayMode,
    stats: Arc<StreamExecutionStats>,
}

impl StreamingMergeState {
    fn new(
        base: SendableRecordBatchStream,
        tail: SendableRecordBatchStream,
        target_version: u64,
        lower_bound: u64,
        watermarks: BTreeMap<String, u64>,
        mode: StreamingOverlayMode,
        stats: Arc<StreamExecutionStats>,
    ) -> Self {
        let validate_order = mode != StreamingOverlayMode::AcceptUnsortedInput;
        Self {
            base: BaseCursor::new(base, watermarks, validate_order, Arc::clone(&stats)),
            tail: TailCursor::new(
                tail,
                lower_bound,
                target_version,
                validate_order,
                Arc::clone(&stats),
            ),
            next_base: None,
            next_tail: None,
            pending_output: VecDeque::new(),
            mode,
            stats,
        }
    }

    async fn next_output_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        let mut output = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        while output.len() < OUTPUT_BATCH_ROWS {
            if let Some(row) = self.pending_output.pop_front() {
                output.push(row);
                self.record_buffered(0, output.len());
                continue;
            }
            let Some(group) = self.next_group().await? else {
                break;
            };
            self.pending_output.extend(group);
        }
        if output.is_empty() {
            return Ok(None);
        }
        self.stats.record_output_batch();
        Ok(Some(rows_to_batch(&output)?))
    }

    async fn next_group(&mut self) -> DataFusionResult<Option<Vec<SnapshotRow>>> {
        self.fill_lookahead().await?;
        let id = match (&self.next_base, &self.next_tail) {
            (Some(base), Some(tail)) => base.row.id.min(tail.id),
            (Some(base), None) => base.row.id,
            (None, Some(tail)) => tail.id,
            (None, None) => return Ok(None),
        };

        let mut base_rows = Vec::new();
        while self.next_base.as_ref().is_some_and(|row| row.row.id == id) {
            let row = self.next_base.take().expect("predicate checked");
            base_rows.push(row);
            self.next_base = self.base.next_row().await?;
        }

        let mut latest_tail = None;
        let mut tail_rows = 0_usize;
        let first_tail_batch = self
            .next_tail
            .as_ref()
            .filter(|effect| effect.id == id)
            .map(|effect| effect.source_batch);
        while self
            .next_tail
            .as_ref()
            .is_some_and(|effect| effect.id == id)
        {
            if self.mode == StreamingOverlayMode::ResetGroupAtBatchBoundary
                && tail_rows > 0
                && self
                    .next_tail
                    .as_ref()
                    .is_some_and(|effect| Some(effect.source_batch) != first_tail_batch)
            {
                break;
            }
            let effect = self.next_tail.take().expect("predicate checked");
            tail_rows = tail_rows.saturating_add(1);
            latest_tail = Some(effect);
            self.next_tail = self.tail.next_row().await?;
        }

        let group_rows = base_rows.len().saturating_add(tail_rows);
        self.stats.record_group(group_rows);
        if group_rows > MAXIMUM_GROUP_ROWS {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "logical id {id} group has {group_rows} rows; limit is {MAXIMUM_GROUP_ROWS}"
            )));
        }
        self.record_buffered(group_rows, 0);

        if let Some(effect) = latest_tail {
            if effect.operation == "DELETE" {
                return Ok(Some(Vec::new()));
            }
            let row = effect.after.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{} at {} has no after-image",
                    effect.operation, effect.commit_version
                ))
            })?;
            return Ok(Some(vec![row]));
        }

        let mut seen = BTreeSet::new();
        let mut rows = Vec::with_capacity(base_rows.len());
        for base in base_rows {
            let key = (base.row.id, base.row.partition.clone());
            if !seen.insert(key.clone()) {
                return Err(DataFusionError::Execution(format!(
                    "duplicate base representation {key:?}"
                )));
            }
            rows.push(base.row);
        }
        rows.sort_by(|left, right| left.partition.cmp(&right.partition));
        Ok(Some(rows))
    }

    async fn fill_lookahead(&mut self) -> DataFusionResult<()> {
        if self.next_base.is_none() {
            self.next_base = self.base.next_row().await?;
        }
        if self.next_tail.is_none() {
            self.next_tail = self.tail.next_row().await?;
        }
        self.record_buffered(0, 0);
        Ok(())
    }

    fn record_buffered(&self, group_rows: usize, output_rows: usize) {
        let variable_rows = group_rows.saturating_add(output_rows);
        let rows = self
            .base
            .remaining_rows()
            .saturating_add(self.tail.remaining_rows())
            .saturating_add(u64::try_from(variable_rows).unwrap_or(u64::MAX));
        let bytes = self
            .base
            .buffered_bytes()
            .saturating_add(self.tail.buffered_bytes())
            .saturating_add(
                u64::try_from(variable_rows)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(96),
            );
        self.stats.record_buffered(rows, bytes);
    }
}

fn materialized_stream(
    base: SendableRecordBatchStream,
    tail: SendableRecordBatchStream,
    target_version: u64,
    lower_bound: u64,
    watermarks: BTreeMap<String, u64>,
    stats: Arc<StreamExecutionStats>,
) -> SendableRecordBatchStream {
    let schema = snapshot_schema();
    let future = async move {
        let base_batches = base.try_collect::<Vec<_>>().await?;
        let tail_batches = tail.try_collect::<Vec<_>>().await?;
        let input_batches = base_batches.len().saturating_add(tail_batches.len());
        let input_rows = base_batches
            .iter()
            .chain(&tail_batches)
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        let input_bytes = base_batches
            .iter()
            .chain(&tail_batches)
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        stats.input_batches.fetch_add(
            u64::try_from(input_batches).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        stats.materialized_input_rows.fetch_add(
            u64::try_from(input_rows).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        stats.record_buffered(
            u64::try_from(input_rows).unwrap_or(u64::MAX),
            u64::try_from(input_bytes).unwrap_or(u64::MAX),
        );

        let inner_stats = Arc::new(StreamExecutionStats::default());
        let base_stream = batches_as_stream(base_input_schema(), base_batches);
        let tail_stream = batches_as_stream(tail_input_schema(), tail_batches);
        let mut merge = StreamingMergeState::new(
            base_stream,
            tail_stream,
            target_version,
            lower_bound,
            watermarks,
            StreamingOverlayMode::Correct,
            inner_stats,
        );
        let mut rows = Vec::new();
        while let Some(batch) = merge.next_output_batch().await? {
            rows.extend(decode_snapshot_batch(&batch)?);
        }
        stats.record_output_batch();
        rows_to_batch(&rows)
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::once(future)))
}

fn batches_as_stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(batches.into_iter().map(Ok)),
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotIdentity {
    cell: String,
    tenant: String,
    table: String,
    target_version: u64,
    schema_version: u32,
    partition_epoch: u64,
    plan_rule_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ContinuationToken {
    identity: SnapshotIdentity,
    last_id: u64,
    last_partition: String,
}

impl ContinuationToken {
    fn validate(&self, identity: &SnapshotIdentity) -> DataFusionResult<()> {
        if &self.identity != identity {
            return Err(DataFusionError::Execution(format!(
                "continuation snapshot mismatch: token={:?}, requested={identity:?}",
                self.identity
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct TailFixtureRow {
    id: u64,
    version: u64,
    operation: &'static str,
    previous_partition: Option<&'static str>,
    status: Option<&'static str>,
    partition: Option<&'static str>,
    amount_cents: Option<u64>,
    priority: Option<u8>,
}

/// Run the frozen Parquet plus Arrow streaming overlay contract.
///
/// # Errors
///
/// Returns an error when fixture construction, `DataFusion` planning, physical
/// execution, order validation, or result decoding fails.
pub fn run_streaming_overlay_contract(
    seed: u64,
    mode: StreamingOverlayMode,
) -> Result<StreamingOverlayReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode))
}

#[allow(
    clippy::too_many_lines,
    reason = "the frozen streaming fixture and independent hard gates stay reviewable together"
)]
async fn run_contract(
    seed: u64,
    mode: StreamingOverlayMode,
) -> Result<StreamingOverlayReport, String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let parquet_path = directory.path().join("orders-streaming-base-v1.parquet");
    let base_batch = base_fixture().map_err(|error| error.to_string())?;
    write_parquet(&parquet_path, &base_batch)?;
    let parquet_bytes = fs::metadata(&parquet_path)
        .map_err(|error| error.to_string())?
        .len();
    let tail_batches = tail_fixture_batches(false).map_err(|error| error.to_string())?;
    let tail_rows = tail_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let tail_bytes = tail_batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum::<usize>();
    let stats = Arc::new(StreamExecutionStats::default());
    let context = build_context(
        &parquet_path,
        tail_batches.clone(),
        TARGET_VERSION,
        mode,
        Arc::clone(&stats),
    )
    .await?;

    let checks = [
        (
            "pushdown_poison",
            "SELECT id, status, partition, amount_cents, priority FROM orders WHERE status = 'OPEN' AND partition = 'west' ORDER BY amount_cents DESC LIMIT 1",
            vec!["id=3|status=OPEN|partition=west|amount_cents=80|priority=0"],
        ),
        (
            "partition_move",
            "SELECT id, status, partition, amount_cents, priority FROM orders WHERE id = 7 ORDER BY partition",
            vec!["id=7|status=OPEN|partition=east|amount_cents=505|priority=7"],
        ),
        (
            "independent_watermark",
            "SELECT id, amount_cents FROM orders WHERE id = 8",
            vec!["id=8|amount_cents=45"],
        ),
        (
            "cross_batch_latest",
            "SELECT id, status, partition, amount_cents, priority FROM orders WHERE id = 1 ORDER BY id",
            vec!["id=1|status=CLOSED|partition=west|amount_cents=110|priority=1"],
        ),
        (
            "projection",
            "SELECT amount_cents FROM orders ORDER BY amount_cents",
            vec![
                "amount_cents=45",
                "amount_cents=60",
                "amount_cents=80",
                "amount_cents=110",
                "amount_cents=210",
                "amount_cents=300",
                "amount_cents=505",
            ],
        ),
    ];
    let mut anomaly_count = 0_u64;
    let mut first_mismatch = None;
    let mut trace = vec![format!("seed={seed}"), format!("mode={}", mode.id())];
    for (id, sql, expected) in checks {
        let actual = execute_sql(&context, sql).await?;
        trace.push(format!("{id}:{}", actual.join(",")));
        if actual != expected {
            record_mismatch(
                &mut anomaly_count,
                &mut first_mismatch,
                format!("{id}: expected {expected:?}, received {actual:?}"),
            );
        }
    }

    let output_batches_before = stats.output_batches.load(Ordering::Relaxed);
    let all_rows = execute_sql(
        &context,
        "SELECT id, status, partition, amount_cents, priority FROM orders ORDER BY id, partition",
    )
    .await?;
    let output_batches_for_full_scan = stats
        .output_batches
        .load(Ordering::Relaxed)
        .saturating_sub(output_batches_before);
    let expected_all_rows = vec![
        "id=1|status=CLOSED|partition=west|amount_cents=110|priority=1",
        "id=3|status=OPEN|partition=west|amount_cents=80|priority=0",
        "id=4|status=OPEN|partition=east|amount_cents=210|priority=2",
        "id=5|status=CLOSED|partition=east|amount_cents=300|priority=0",
        "id=6|status=OPEN|partition=east|amount_cents=60|priority=0",
        "id=7|status=OPEN|partition=east|amount_cents=505|priority=7",
        "id=8|status=OPEN|partition=west|amount_cents=45|priority=0",
    ];
    trace.push(format!("all_rows:{}", all_rows.join(",")));
    if all_rows != expected_all_rows {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            format!("all_rows: expected {expected_all_rows:?}, received {all_rows:?}"),
        );
    }

    let token = ContinuationToken {
        identity: snapshot_identity(TARGET_VERSION),
        last_id: 5,
        last_partition: "east".to_owned(),
    };
    let requested_target = if mode == StreamingOverlayMode::RebaseContinuationTarget {
        AFTER_TARGET_VERSION
    } else {
        TARGET_VERSION
    };
    let requested_identity = snapshot_identity(requested_target);
    let token_valid = token.validate(&requested_identity).is_ok();
    let continuation_context = if requested_target == TARGET_VERSION {
        None
    } else {
        Some(
            build_context(
                &parquet_path,
                tail_batches.clone(),
                requested_target,
                mode,
                Arc::new(StreamExecutionStats::default()),
            )
            .await?,
        )
    };
    let continuation_source = continuation_context.as_ref().unwrap_or(&context);
    let continuation_sql = format!(
        "SELECT id, status, partition, amount_cents, priority FROM orders WHERE id > {} OR (id = {} AND partition > '{}') ORDER BY id, partition LIMIT 4",
        token.last_id, token.last_id, token.last_partition
    );
    let continuation = execute_sql(continuation_source, &continuation_sql).await?;
    let expected_continuation = vec![
        "id=6|status=OPEN|partition=east|amount_cents=60|priority=0",
        "id=7|status=OPEN|partition=east|amount_cents=505|priority=7",
        "id=8|status=OPEN|partition=west|amount_cents=45|priority=0",
    ];
    trace.push(format!("continuation:{}", continuation.join(",")));
    let continuation_target_bound = token_valid && continuation == expected_continuation;
    if !continuation_target_bound {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            format!(
                "continuation target was not preserved: token_valid={token_valid}, expected {expected_continuation:?}, received {continuation:?}"
            ),
        );
    }

    let unsorted_context = build_context(
        &parquet_path,
        tail_fixture_batches(true).map_err(|error| error.to_string())?,
        TARGET_VERSION,
        mode,
        Arc::new(StreamExecutionStats::default()),
    )
    .await?;
    let unsorted_result = execute_sql(
        &unsorted_context,
        "SELECT id, status, partition, amount_cents, priority FROM orders ORDER BY id, partition",
    )
    .await;
    let input_order_validated = unsorted_result.is_err();
    trace.push(format!("unsorted_rejected={input_order_validated}"));
    if !input_order_validated {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            "unsorted tail input was accepted while ordered output was declared".to_owned(),
        );
    }

    let materialized_input_rows = stats.materialized_input_rows.load(Ordering::Relaxed);
    let peak_buffered_rows = stats.peak_buffered_rows.load(Ordering::Relaxed);
    let peak_buffered_bytes = stats.peak_buffered_bytes.load(Ordering::Relaxed);
    let maximum_group_rows_observed = stats.maximum_group_rows.load(Ordering::Relaxed);
    let incremental_emission = materialized_input_rows == 0 && output_batches_for_full_scan > 1;
    if !incremental_emission {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            format!(
                "operator did not emit incrementally: materialized_rows={materialized_input_rows}, full_scan_batches={output_batches_for_full_scan}"
            ),
        );
    }
    let buffer_bound_holds = materialized_input_rows == 0
        && peak_buffered_rows <= MAXIMUM_BUFFERED_ROWS
        && maximum_group_rows_observed <= u64::try_from(MAXIMUM_GROUP_ROWS).unwrap_or(u64::MAX);
    if !buffer_bound_holds {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            format!(
                "buffer bound failed: materialized_rows={materialized_input_rows}, peak_rows={peak_buffered_rows}, maximum_group={maximum_group_rows_observed}"
            ),
        );
    }
    let output_order_declared = stats.output_order_declarations.load(Ordering::Relaxed) > 0;
    if !output_order_declared {
        record_mismatch(
            &mut anomaly_count,
            &mut first_mismatch,
            "streaming plan did not declare output ordering".to_owned(),
        );
    }
    let batch_boundary_groups_preserved = all_rows
        .iter()
        .filter(|row| row.starts_with("id=1|"))
        .count()
        == 1
        && all_rows
            .iter()
            .any(|row| row == "id=1|status=CLOSED|partition=west|amount_cents=110|priority=1");
    let independent_watermarks = all_rows
        .iter()
        .any(|row| row == "id=8|status=OPEN|partition=west|amount_cents=45|priority=0");
    let trace_sha256 = hex_sha256(trace.join("\n").as_bytes());

    Ok(StreamingOverlayReport {
        schema_version: STREAMING_OVERLAY_CONTRACT_VERSION,
        seed,
        mode: mode.id().to_owned(),
        target_version: TARGET_VERSION,
        minimum_base_watermark: WEST_WATERMARK,
        maximum_base_watermark: EAST_WATERMARK,
        analytical_coverage: ANALYTICAL_COVERAGE,
        executed_checks: 8,
        anomaly_count,
        base_rows: u64::try_from(base_batch.num_rows()).unwrap_or(u64::MAX),
        tail_rows: u64::try_from(tail_rows).unwrap_or(u64::MAX),
        output_rows: u64::try_from(all_rows.len()).unwrap_or(u64::MAX),
        input_batches: stats.input_batches.load(Ordering::Relaxed),
        output_batches: stats.output_batches.load(Ordering::Relaxed),
        peak_buffered_rows,
        peak_buffered_bytes,
        maximum_group_rows_observed,
        materialized_input_rows,
        tail_bytes: u64::try_from(tail_bytes).unwrap_or(u64::MAX),
        parquet_bytes,
        spill_bytes: 0,
        parquet_round_trip: parquet_bytes > 0,
        arrow_tail_complete: tail_rows == 9,
        incremental_emission,
        input_order_validated,
        batch_boundary_groups_preserved,
        independent_watermarks,
        continuation_target_bound,
        buffer_bound_holds,
        output_order_declared,
        first_mismatch,
        trace_sha256,
    })
}

fn record_mismatch(anomaly_count: &mut u64, first_mismatch: &mut Option<String>, detail: String) {
    *anomaly_count = anomaly_count.saturating_add(1);
    first_mismatch.get_or_insert(detail);
}

fn snapshot_identity(target_version: u64) -> SnapshotIdentity {
    SnapshotIdentity {
        cell: "cell-17".to_owned(),
        tenant: "tenant-orders".to_owned(),
        table: "orders".to_owned(),
        target_version,
        schema_version: 2,
        partition_epoch: 4,
        plan_rule_version: 1,
    }
}

async fn build_context(
    parquet_path: &Path,
    tail_batches: Vec<RecordBatch>,
    target_version: u64,
    mode: StreamingOverlayMode,
    stats: Arc<StreamExecutionStats>,
) -> Result<SessionContext, String> {
    let mut session_config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(2);
    session_config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    let context = SessionContext::new_with_config(session_config);
    let parquet_uri = parquet_path
        .to_str()
        .ok_or_else(|| "fixture path is not UTF-8".to_owned())?;
    context
        .register_parquet(
            "_zebra_streaming_base",
            parquet_uri,
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let base_provider = context
        .table_provider("_zebra_streaming_base")
        .await
        .map_err(|error| error.to_string())?;
    let tail_provider = Arc::new(
        MemTable::try_new(tail_input_schema(), vec![tail_batches])
            .map_err(|error| error.to_string())?,
    );
    context
        .register_table(
            "orders",
            Arc::new(StreamingZebraSnapshotTableProvider::new(
                base_provider,
                tail_provider,
                target_version,
                partition_watermarks(),
                ANALYTICAL_COVERAGE,
                mode,
                stats,
            )),
        )
        .map_err(|error| error.to_string())?;
    Ok(context)
}

fn partition_watermarks() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("west".to_owned(), WEST_WATERMARK),
        ("east".to_owned(), EAST_WATERMARK),
    ])
}

async fn execute_sql(context: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
    let dataframe = context.sql(sql).await.map_err(|error| error.to_string())?;
    let batches = dataframe
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    canonical_rows(&batches)
}

fn canonical_rows(batches: &[RecordBatch]) -> Result<Vec<String>, String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for (column, field) in batch.columns().iter().zip(batch.schema().fields()) {
                let value = match field.data_type() {
                    DataType::UInt64 => column
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| format!("{} is not UInt64", field.name()))?
                        .value(row)
                        .to_string(),
                    DataType::UInt8 => column
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .ok_or_else(|| format!("{} is not UInt8", field.name()))?
                        .value(row)
                        .to_string(),
                    DataType::Utf8 => column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| format!("{} is not Utf8", field.name()))?
                        .value(row)
                        .to_owned(),
                    other => return Err(format!("unsupported output type {other:?}")),
                };
                values.push(format!("{}={value}", field.name()));
            }
            rows.push(values.join("|"));
        }
    }
    Ok(rows)
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<(), String> {
    let file = File::create(path).map_err(|error| error.to_string())?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(())
}

fn base_fixture() -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(
        base_input_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 4, 5, 6, 7, 8])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "OPEN", "OPEN", "OPEN", "CLOSED", "OPEN", "OPEN", "OPEN",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "west", "west", "east", "east", "east", "west", "west",
            ])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100, 90, 200, 300, 60, 500, 40])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![1, 1, 1, 1, 1, 1, 1])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![5, 5, 8, 8, 8, 5, 5])) as ArrayRef,
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[allow(
    clippy::too_many_lines,
    reason = "the frozen tail history stays explicit and reviewable"
)]
fn tail_fixture_batches(unsorted: bool) -> DataFusionResult<Vec<RecordBatch>> {
    let first = [TailFixtureRow {
        id: 1,
        version: 8,
        operation: "UPDATE",
        previous_partition: Some("west"),
        status: Some("CLOSED"),
        partition: Some("west"),
        amount_cents: Some(100),
        priority: Some(1),
    }];
    let second = [
        TailFixtureRow {
            id: 1,
            version: 12,
            operation: "UPDATE",
            previous_partition: Some("west"),
            status: Some("CLOSED"),
            partition: Some("west"),
            amount_cents: Some(110),
            priority: Some(1),
        },
        TailFixtureRow {
            id: 2,
            version: 9,
            operation: "DELETE",
            previous_partition: Some("west"),
            status: None,
            partition: None,
            amount_cents: None,
            priority: None,
        },
    ];
    let third = [
        TailFixtureRow {
            id: 3,
            version: 10,
            operation: "INSERT",
            previous_partition: None,
            status: Some("OPEN"),
            partition: Some("west"),
            amount_cents: Some(80),
            priority: Some(0),
        },
        TailFixtureRow {
            id: 4,
            version: 12,
            operation: "UPDATE",
            previous_partition: Some("east"),
            status: Some("OPEN"),
            partition: Some("east"),
            amount_cents: Some(210),
            priority: Some(2),
        },
        TailFixtureRow {
            id: 6,
            version: 7,
            operation: "INSERT",
            previous_partition: None,
            status: Some("OPEN"),
            partition: Some("east"),
            amount_cents: Some(60),
            priority: Some(0),
        },
    ];
    let ordered_fourth = [
        TailFixtureRow {
            id: 7,
            version: 11,
            operation: "UPDATE",
            previous_partition: Some("west"),
            status: Some("OPEN"),
            partition: Some("east"),
            amount_cents: Some(505),
            priority: Some(7),
        },
        TailFixtureRow {
            id: 8,
            version: 6,
            operation: "UPDATE",
            previous_partition: Some("west"),
            status: Some("OPEN"),
            partition: Some("west"),
            amount_cents: Some(45),
            priority: Some(0),
        },
        TailFixtureRow {
            id: 9,
            version: 13,
            operation: "INSERT",
            previous_partition: None,
            status: Some("OPEN"),
            partition: Some("west"),
            amount_cents: Some(70),
            priority: Some(0),
        },
    ];
    let unsorted_fourth = [ordered_fourth[1], ordered_fourth[0], ordered_fourth[2]];
    let fourth = if unsorted {
        &unsorted_fourth[..]
    } else {
        &ordered_fourth[..]
    };
    Ok(vec![
        tail_batch(&first)?,
        tail_batch(&second)?,
        tail_batch(&third)?,
        tail_batch(fourth)?,
    ])
}

fn tail_batch(rows: &[TailFixtureRow]) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(
        tail_input_schema(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.version).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.operation),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.previous_partition)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.status).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.partition).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.amount_cents).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt8Array::from(
                rows.iter().map(|row| row.priority).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vec![2; rows.len()])) as ArrayRef,
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn decode_snapshot_batch(batch: &RecordBatch) -> DataFusionResult<Vec<SnapshotRow>> {
    let ids = required_array::<UInt64Array>(batch, "id")?;
    let statuses = required_array::<StringArray>(batch, "status")?;
    let partitions = required_array::<StringArray>(batch, "partition")?;
    let amounts = required_array::<UInt64Array>(batch, "amount_cents")?;
    let priorities = required_array::<UInt8Array>(batch, "priority")?;
    Ok((0..batch.num_rows())
        .map(|index| SnapshotRow {
            id: ids.value(index),
            status: statuses.value(index).to_owned(),
            partition: partitions.value(index).to_owned(),
            amount_cents: amounts.value(index),
            priority: priorities.value(index),
        })
        .collect())
}

fn rows_to_batch(rows: &[SnapshotRow]) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(
        snapshot_schema(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.status.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.partition.as_str()),
            )) as ArrayRef,
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.amount_cents).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt8Array::from(
                rows.iter().map(|row| row.priority).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn required_array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> DataFusionResult<&'a T> {
    batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Execution(format!("missing column {name}")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| DataFusionError::Execution(format!("invalid type for column {name}")))
}

fn snapshot_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("partition", DataType::Utf8, false),
        Field::new("amount_cents", DataType::UInt64, false),
        Field::new("priority", DataType::UInt8, false),
    ]))
}

fn base_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("partition", DataType::Utf8, false),
        Field::new("total_cents", DataType::UInt64, false),
        Field::new("writer_schema", DataType::UInt32, false),
        Field::new("base_watermark", DataType::UInt64, false),
    ]))
}

fn tail_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("commit_version", DataType::UInt64, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("previous_partition", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("partition", DataType::Utf8, true),
        Field::new("amount_cents", DataType::UInt64, true),
        Field::new("priority", DataType::UInt8, true),
        Field::new("writer_schema", DataType::UInt32, false),
    ]))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{run_streaming_overlay_contract, StreamingOverlayMode};

    #[test]
    fn correct_streaming_overlay_is_exact_and_bounded() {
        let report = run_streaming_overlay_contract(1103, StreamingOverlayMode::Correct)
            .expect("streaming overlay runs");
        assert_eq!(report.anomaly_count, 0, "{:?}", report.first_mismatch);
        assert!(report.incremental_emission);
        assert!(report.buffer_bound_holds);
        assert!(report.input_order_validated);
        assert_eq!(report.output_rows, 7);
    }

    #[test]
    fn every_streaming_negative_control_is_detected() {
        for mode in [
            StreamingOverlayMode::MaterializeInputs,
            StreamingOverlayMode::ResetGroupAtBatchBoundary,
            StreamingOverlayMode::StartTailAtMaximumWatermark,
            StreamingOverlayMode::RebaseContinuationTarget,
            StreamingOverlayMode::AcceptUnsortedInput,
        ] {
            let report = run_streaming_overlay_contract(1103, mode)
                .expect("streaming negative control runs");
            assert!(report.anomaly_count > 0, "{} escaped", mode.id());
        }
    }
}
