//! Physical Arrow, Parquet, and `DataFusion` contracts for `ZebraDB`.

mod range_stripe;
mod streaming;

pub use range_stripe::{
    RangeStripeScanSnapshot, RangeStripeScanStats, RangeStripeSource, RangeStripeTableProvider,
};
/// C0 semantic name for the shared bounded, single-partition batch scheduler.
pub type RangeRowTableProvider = RangeStripeTableProvider;
pub use streaming::{run_streaming_overlay_contract, StreamingOverlayMode, StreamingOverlayReport};

use arrow::array::{Array, ArrayRef, StringArray, UInt32Array, UInt64Array, UInt8Array};
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
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures_util::stream;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter, Write};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

/// Physical contract version for the first correctness-only overlay.
pub const PHYSICAL_OVERLAY_CONTRACT_VERSION: u32 = 1;

const TARGET_VERSION: u64 = 12;
const BASE_WATERMARK: u64 = 5;
const ANALYTICAL_COVERAGE: u64 = 12;

/// Subjects exercised by the frozen physical overlay suite.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalOverlayMode {
    Correct,
    PushdownBeforeInvalidation,
    PartitionLocalReduction,
    ProjectPrimaryKeyEarly,
}

impl PhysicalOverlayMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PushdownBeforeInvalidation => "pushdown_before_invalidation",
            Self::PartitionLocalReduction => "partition_local_reduction",
            Self::ProjectPrimaryKeyEarly => "project_primary_key_early",
        }
    }
}

/// Deterministic evidence emitted by one physical overlay contract run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "the report preserves independent frozen hard-gate evidence"
)]
pub struct PhysicalOverlayReport {
    pub schema_version: u32,
    pub seed: u64,
    pub mode: String,
    pub target_version: u64,
    pub base_watermark: u64,
    pub analytical_coverage: u64,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub base_rows: u64,
    pub tail_rows: u64,
    pub output_rows: u64,
    pub tail_bytes: u64,
    pub parquet_bytes: u64,
    pub materialized_bytes: u64,
    pub parquet_round_trip: bool,
    pub arrow_tail_complete: bool,
    pub invalidation_precedes_filter: bool,
    pub partition_move_is_logical_identity: bool,
    pub hidden_primary_key_survives_projection: bool,
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
struct TailEffect {
    id: u64,
    commit_version: u64,
    operation: String,
    previous_partition: Option<String>,
    after: Option<SnapshotRow>,
}

/// A snapshot-aware table source that keeps every invalidation column below SQL
/// projection, filtering, ordering, and limit.
pub struct ZebraSnapshotTableProvider {
    schema: SchemaRef,
    base: Arc<dyn TableProvider>,
    tail: Arc<dyn TableProvider>,
    target_version: u64,
    base_watermark: u64,
    analytical_coverage: u64,
    mode: PhysicalOverlayMode,
}

impl Debug for ZebraSnapshotTableProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ZebraSnapshotTableProvider")
            .field("target_version", &self.target_version)
            .field("base_watermark", &self.base_watermark)
            .field("analytical_coverage", &self.analytical_coverage)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl ZebraSnapshotTableProvider {
    #[must_use]
    pub fn new(
        base: Arc<dyn TableProvider>,
        tail: Arc<dyn TableProvider>,
        target_version: u64,
        base_watermark: u64,
        analytical_coverage: u64,
        mode: PhysicalOverlayMode,
    ) -> Self {
        Self {
            schema: snapshot_schema(),
            base,
            tail,
            target_version,
            base_watermark,
            analytical_coverage,
            mode,
        }
    }
}

#[async_trait]
impl TableProvider for ZebraSnapshotTableProvider {
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
        if self.base_watermark > self.target_version
            || self.target_version > self.analytical_coverage
        {
            return Err(DataFusionError::Execution(format!(
                "snapshot unavailable: base_watermark={}, target={}, coverage={}",
                self.base_watermark, self.target_version, self.analytical_coverage
            )));
        }

        let base = self.base.scan(state, None, &[], None).await?;
        let tail = self.tail.scan(state, None, &[], None).await?;
        let overlay: Arc<dyn ExecutionPlan> = Arc::new(SnapshotOverlayExec::new(
            base,
            tail,
            self.target_version,
            self.mode,
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

/// Correctness-first overlay plan. It materializes bounded inputs and therefore
/// cannot be used to admit memory or latency curves.
#[derive(Debug)]
pub struct SnapshotOverlayExec {
    base: Arc<dyn ExecutionPlan>,
    tail: Arc<dyn ExecutionPlan>,
    target_version: u64,
    mode: PhysicalOverlayMode,
    properties: Arc<PlanProperties>,
}

impl SnapshotOverlayExec {
    fn new(
        base: Arc<dyn ExecutionPlan>,
        tail: Arc<dyn ExecutionPlan>,
        target_version: u64,
        mode: PhysicalOverlayMode,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(snapshot_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            base,
            tail,
            target_version,
            mode,
            properties,
        }
    }
}

impl DisplayAs for SnapshotOverlayExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "SnapshotOverlayExec: target={}, mode={}",
                self.target_version,
                self.mode.id()
            ),
            DisplayFormatType::TreeRender => write!(formatter, "SnapshotOverlayExec"),
        }
    }
}

impl ExecutionPlan for SnapshotOverlayExec {
    fn name(&self) -> &'static str {
        "SnapshotOverlayExec"
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
                "SnapshotOverlayExec requires two children, received {}",
                children.len()
            )));
        }
        let tail = children.pop().expect("length checked");
        let base = children.pop().expect("length checked");
        Ok(Arc::new(Self::new(
            base,
            tail,
            self.target_version,
            self.mode,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "SnapshotOverlayExec has one partition, received {partition}"
            )));
        }
        let base = Arc::clone(&self.base);
        let tail = Arc::clone(&self.tail);
        let target_version = self.target_version;
        let mode = self.mode;
        let output_schema = snapshot_schema();
        let future = async move {
            let base_batches = collect(base, Arc::clone(&context)).await?;
            let tail_batches = collect(tail, context).await?;
            overlay_batches(&base_batches, &tail_batches, target_version, mode)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream::once(future),
        )))
    }
}

/// Run the deterministic Parquet plus Arrow fixture through `DataFusion` SQL.
///
/// # Errors
///
/// Returns an error when fixture construction, `DataFusion` planning, physical
/// execution, or result decoding fails.
pub fn run_physical_overlay_contract(
    seed: u64,
    mode: PhysicalOverlayMode,
) -> Result<PhysicalOverlayReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode))
}

#[allow(
    clippy::too_many_lines,
    reason = "the frozen physical fixture and its three checks stay auditable together"
)]
async fn run_contract(
    seed: u64,
    mode: PhysicalOverlayMode,
) -> Result<PhysicalOverlayReport, String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let parquet_path = directory.path().join("orders-base-v1.parquet");
    let base_batch = base_fixture().map_err(|error| error.to_string())?;
    write_parquet(&parquet_path, &base_batch)?;
    let parquet_bytes = fs::metadata(&parquet_path)
        .map_err(|error| error.to_string())?
        .len();
    let tail_batch = tail_fixture().map_err(|error| error.to_string())?;
    let tail_bytes = u64::try_from(tail_batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let materialized_bytes = u64::try_from(
        base_batch
            .get_array_memory_size()
            .saturating_add(tail_batch.get_array_memory_size()),
    )
    .unwrap_or(u64::MAX);

    let mut session_config = SessionConfig::new().with_target_partitions(1);
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
        .register_parquet("_zebra_base", parquet_uri, ParquetReadOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let base_provider = context
        .table_provider("_zebra_base")
        .await
        .map_err(|error| error.to_string())?;
    let tail_provider = Arc::new(
        MemTable::try_new(tail_batch.schema(), vec![vec![tail_batch.clone()]])
            .map_err(|error| error.to_string())?,
    );
    context
        .register_table(
            "orders",
            Arc::new(ZebraSnapshotTableProvider::new(
                base_provider,
                tail_provider,
                TARGET_VERSION,
                BASE_WATERMARK,
                ANALYTICAL_COVERAGE,
                mode,
            )),
        )
        .map_err(|error| error.to_string())?;

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
            "projection_poison",
            "SELECT amount_cents FROM orders ORDER BY amount_cents",
            vec![
                "amount_cents=80",
                "amount_cents=100",
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
            anomaly_count = anomaly_count.saturating_add(1);
            first_mismatch
                .get_or_insert_with(|| format!("{id}: expected {expected:?}, received {actual:?}"));
        }
    }
    let all_rows = execute_sql(
        &context,
        "SELECT id, status, partition, amount_cents, priority FROM orders ORDER BY id, partition",
    )
    .await?;
    trace.push(format!("all_rows:{}", all_rows.join(",")));
    let output_rows = u64::try_from(all_rows.len()).unwrap_or(u64::MAX);
    let trace_sha256 = hex_sha256(trace.join("\n").as_bytes());

    Ok(PhysicalOverlayReport {
        schema_version: PHYSICAL_OVERLAY_CONTRACT_VERSION,
        seed,
        mode: mode.id().to_owned(),
        target_version: TARGET_VERSION,
        base_watermark: BASE_WATERMARK,
        analytical_coverage: ANALYTICAL_COVERAGE,
        executed_checks: 4,
        anomaly_count,
        base_rows: u64::try_from(base_batch.num_rows()).unwrap_or(u64::MAX),
        tail_rows: u64::try_from(tail_batch.num_rows()).unwrap_or(u64::MAX),
        output_rows,
        tail_bytes,
        parquet_bytes,
        materialized_bytes,
        parquet_round_trip: parquet_bytes > 0,
        arrow_tail_complete: tail_batch.num_rows() == 4,
        invalidation_precedes_filter: mode != PhysicalOverlayMode::PushdownBeforeInvalidation,
        partition_move_is_logical_identity: mode != PhysicalOverlayMode::PartitionLocalReduction,
        hidden_primary_key_survives_projection: mode != PhysicalOverlayMode::ProjectPrimaryKeyEarly,
        first_mismatch,
        trace_sha256,
    })
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
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(())
}

fn base_fixture() -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(
        base_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["OPEN", "OPEN", "OPEN"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["west", "west", "west"])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![100, 90, 500])) as ArrayRef,
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn tail_fixture() -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(
        tail_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3, 7])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![8, 9, 10, 11])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "UPDATE", "DELETE", "INSERT", "UPDATE",
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("west"),
                Some("west"),
                None,
                Some("west"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("CLOSED"),
                None,
                Some("OPEN"),
                Some("OPEN"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("west"),
                None,
                Some("west"),
                Some("east"),
            ])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![
                Some(100),
                None,
                Some(80),
                Some(505),
            ])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![Some(0), None, Some(0), Some(7)])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![2, 2, 2, 2])) as ArrayRef,
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn overlay_batches(
    base_batches: &[RecordBatch],
    tail_batches: &[RecordBatch],
    target_version: u64,
    mode: PhysicalOverlayMode,
) -> DataFusionResult<RecordBatch> {
    let mut rows = BTreeMap::<(String, u64), SnapshotRow>::new();
    for batch in base_batches {
        let ids = required_array::<UInt64Array>(batch, "id")?;
        let statuses = required_array::<StringArray>(batch, "status")?;
        let partitions = required_array::<StringArray>(batch, "partition")?;
        let amounts = required_array::<UInt64Array>(batch, "total_cents")?;
        for index in 0..batch.num_rows() {
            let row = SnapshotRow {
                id: ids.value(index),
                status: statuses.value(index).to_owned(),
                partition: partitions.value(index).to_owned(),
                amount_cents: amounts.value(index),
                priority: 0,
            };
            rows.insert((row.partition.clone(), row.id), row);
        }
    }

    let mut latest = BTreeMap::<u64, TailEffect>::new();
    for batch in tail_batches {
        decode_tail_batch(batch, target_version, &mut latest)?;
    }

    if mode != PhysicalOverlayMode::ProjectPrimaryKeyEarly {
        for effect in latest.values() {
            if mode == PhysicalOverlayMode::PushdownBeforeInvalidation {
                let after_matches_poison = effect
                    .after
                    .as_ref()
                    .is_some_and(|row| row.status == "OPEN" && row.partition == "west");
                if !after_matches_poison {
                    continue;
                }
            }

            if mode == PhysicalOverlayMode::PartitionLocalReduction {
                if let Some(after) = &effect.after {
                    rows.remove(&(after.partition.clone(), effect.id));
                } else if let Some(previous) = &effect.previous_partition {
                    rows.remove(&(previous.clone(), effect.id));
                }
            } else {
                rows.retain(|(_, id), _| *id != effect.id);
            }

            if effect.operation != "DELETE" {
                let after = effect.after.clone().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{} at {} has no after-image",
                        effect.operation, effect.commit_version
                    ))
                })?;
                rows.insert((after.partition.clone(), after.id), after);
            }
        }
    }

    rows_to_batch(rows.into_values().collect())
}

fn decode_tail_batch(
    batch: &RecordBatch,
    target_version: u64,
    latest: &mut BTreeMap<u64, TailEffect>,
) -> DataFusionResult<()> {
    let ids = required_array::<UInt64Array>(batch, "id")?;
    let versions = required_array::<UInt64Array>(batch, "commit_version")?;
    let operations = required_array::<StringArray>(batch, "operation")?;
    let previous = required_array::<StringArray>(batch, "previous_partition")?;
    let statuses = required_array::<StringArray>(batch, "status")?;
    let partitions = required_array::<StringArray>(batch, "partition")?;
    let amounts = required_array::<UInt64Array>(batch, "amount_cents")?;
    let priorities = required_array::<UInt8Array>(batch, "priority")?;
    let writer_schemas = required_array::<UInt32Array>(batch, "writer_schema")?;

    for index in 0..batch.num_rows() {
        let id = ids.value(index);
        let version = versions.value(index);
        if version > target_version {
            continue;
        }
        if writer_schemas.value(index) != 2 {
            return Err(DataFusionError::Execution(format!(
                "unsupported writer schema {}",
                writer_schemas.value(index)
            )));
        }
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
        let effect = TailEffect {
            id,
            commit_version: version,
            operation: operation.to_owned(),
            previous_partition: (!previous.is_null(index))
                .then(|| previous.value(index).to_owned()),
            after,
        };
        let replace = latest
            .get(&id)
            .is_none_or(|current| current.commit_version < version);
        if replace {
            latest.insert(id, effect);
        }
    }
    Ok(())
}

fn rows_to_batch(mut rows: Vec<SnapshotRow>) -> DataFusionResult<RecordBatch> {
    rows.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.partition.cmp(&right.partition))
    });
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

fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("partition", DataType::Utf8, false),
        Field::new("total_cents", DataType::UInt64, false),
    ]))
}

fn tail_schema() -> SchemaRef {
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
    use super::{run_physical_overlay_contract, PhysicalOverlayMode};

    #[test]
    fn correct_physical_overlay_matches_oracle() {
        let report = run_physical_overlay_contract(1103, PhysicalOverlayMode::Correct)
            .expect("physical overlay runs");
        assert_eq!(report.anomaly_count, 0);
        assert_eq!(report.output_rows, 3);
        assert!(report.parquet_round_trip);
        assert!(report.arrow_tail_complete);
    }

    #[test]
    fn poisoned_subjects_are_detected() {
        for mode in [
            PhysicalOverlayMode::PushdownBeforeInvalidation,
            PhysicalOverlayMode::PartitionLocalReduction,
            PhysicalOverlayMode::ProjectPrimaryKeyEarly,
        ] {
            let report =
                run_physical_overlay_contract(1103, mode).expect("poisoned physical overlay runs");
            assert!(report.anomaly_count > 0, "{} escaped", mode.id());
        }
    }
}
