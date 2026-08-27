//! Bounded `DataFusion` source for independently fetchable columnar stripes.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::equivalence::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::stream;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Storage-format adapter used by [`RangeStripeTableProvider`].
///
/// One call must return at most one independently fetchable physical stripe.
/// Projection indices refer to [`Self::schema`].
#[async_trait]
pub trait RangeStripeSource: Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn stripe_count(&self) -> usize;

    async fn read_stripe(
        &self,
        stripe_index: usize,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<RecordBatch>;
}

/// Stable snapshot of the source operator's bounded-work counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RangeStripeScanSnapshot {
    pub scan_plans: u64,
    pub projection_pushdown_plans: u64,
    pub stripes_read: u64,
    pub batches_emitted: u64,
    pub rows_emitted: u64,
    pub peak_batch_rows: u64,
    pub peak_batch_bytes: u64,
}

/// Shared counters for one provider instance.
#[derive(Debug, Default)]
pub struct RangeStripeScanStats {
    scan_plans: AtomicU64,
    projection_pushdown_plans: AtomicU64,
    stripes_read: AtomicU64,
    batches_emitted: AtomicU64,
    rows_emitted: AtomicU64,
    peak_batch_rows: AtomicU64,
    peak_batch_bytes: AtomicU64,
}

impl RangeStripeScanStats {
    fn record_plan(&self, projection_pushdown: bool) {
        self.scan_plans.fetch_add(1, Ordering::Relaxed);
        if projection_pushdown {
            self.projection_pushdown_plans
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_batch(&self, batch: &RecordBatch) {
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        self.stripes_read.fetch_add(1, Ordering::Relaxed);
        self.batches_emitted.fetch_add(1, Ordering::Relaxed);
        self.rows_emitted.fetch_add(rows, Ordering::Relaxed);
        self.peak_batch_rows.fetch_max(rows, Ordering::Relaxed);
        self.peak_batch_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> RangeStripeScanSnapshot {
        RangeStripeScanSnapshot {
            scan_plans: self.scan_plans.load(Ordering::Relaxed),
            projection_pushdown_plans: self.projection_pushdown_plans.load(Ordering::Relaxed),
            stripes_read: self.stripes_read.load(Ordering::Relaxed),
            batches_emitted: self.batches_emitted.load(Ordering::Relaxed),
            rows_emitted: self.rows_emitted.load(Ordering::Relaxed),
            peak_batch_rows: self.peak_batch_rows.load(Ordering::Relaxed),
            peak_batch_bytes: self.peak_batch_bytes.load(Ordering::Relaxed),
        }
    }
}

/// `DataFusion` table source that fetches and emits one physical stripe at a time.
pub struct RangeStripeTableProvider {
    source: Arc<dyn RangeStripeSource>,
    schema: SchemaRef,
    stats: Arc<RangeStripeScanStats>,
}

impl Debug for RangeStripeTableProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RangeStripeTableProvider")
            .field("stripe_count", &self.source.stripe_count())
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl RangeStripeTableProvider {
    #[must_use]
    pub fn new(source: Arc<dyn RangeStripeSource>) -> Self {
        Self {
            schema: source.schema(),
            source,
            stats: Arc::new(RangeStripeScanStats::default()),
        }
    }

    #[must_use]
    pub fn stats(&self) -> Arc<RangeStripeScanStats> {
        Arc::clone(&self.stats)
    }
}

#[async_trait]
impl TableProvider for RangeStripeTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        let projection_pushdown =
            projection.is_some_and(|indices| indices.len() < self.schema.fields().len());
        self.stats.record_plan(projection_pushdown);
        Ok(Arc::new(RangeStripeExec::new(
            Arc::clone(&self.source),
            projection.cloned(),
            projected_schema,
            Arc::clone(&self.stats),
        )))
    }
}

struct RangeStripeExec {
    source: Arc<dyn RangeStripeSource>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    stats: Arc<RangeStripeScanStats>,
    properties: Arc<PlanProperties>,
}

impl Debug for RangeStripeExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RangeStripeExec")
            .field("stripe_count", &self.source.stripe_count())
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

impl RangeStripeExec {
    fn new(
        source: Arc<dyn RangeStripeSource>,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
        stats: Arc<RangeStripeScanStats>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            projection,
            schema,
            stats,
            properties,
        }
    }
}

impl DisplayAs for RangeStripeExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "RangeStripeExec: stripes={}, projection={:?}",
                self.source.stripe_count(),
                self.projection
            ),
            DisplayFormatType::TreeRender => write!(formatter, "RangeStripeExec"),
        }
    }
}

impl ExecutionPlan for RangeStripeExec {
    fn name(&self) -> &'static str {
        "RangeStripeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "RangeStripeExec accepts no children, received {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "RangeStripeExec has one partition, received {partition}"
            )));
        }
        let source = Arc::clone(&self.source);
        let stripe_count = source.stripe_count();
        let projection = self.projection.clone();
        let stats = Arc::clone(&self.stats);
        let output = stream::unfold(0_usize, move |stripe_index| {
            let source = Arc::clone(&source);
            let projection = projection.clone();
            let stats = Arc::clone(&stats);
            async move {
                if stripe_index >= stripe_count {
                    return None;
                }
                let batch = source
                    .read_stripe(stripe_index, projection.as_deref())
                    .await;
                if let Ok(batch) = &batch {
                    stats.record_batch(batch);
                }
                Some((batch, stripe_index.saturating_add(1)))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            output,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{RangeStripeSource, RangeStripeTableProvider};
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion::common::Result as DataFusionResult;
    use datafusion::prelude::SessionContext;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    struct MemoryStripeSource {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Debug for MemoryStripeSource {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("MemoryStripeSource")
                .field("batches", &self.batches.len())
                .finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl RangeStripeSource for MemoryStripeSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn stripe_count(&self) -> usize {
            self.batches.len()
        }

        async fn read_stripe(
            &self,
            stripe_index: usize,
            projection: Option<&[usize]>,
        ) -> DataFusionResult<RecordBatch> {
            let batch = self.batches[stripe_index].clone();
            Ok(match projection {
                Some(indices) => batch.project(indices)?,
                None => batch,
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streams_projection_stripes_through_datafusion() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("tenant", DataType::UInt64, false),
            Field::new("amount", DataType::UInt64, false),
        ]));
        let batches = [(0_u64, 4_u64), (4, 8), (8, 12)]
            .into_iter()
            .map(|(start, end)| {
                let ids = (start..end).collect::<Vec<_>>();
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(UInt64Array::from(ids.clone())),
                    Arc::new(UInt64Array::from(
                        ids.iter().map(|id| id % 2).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt64Array::from(
                        ids.iter().map(|id| id * 10).collect::<Vec<_>>(),
                    )),
                ];
                RecordBatch::try_new(Arc::clone(&schema), columns).expect("record batch")
            })
            .collect();
        let source = Arc::new(MemoryStripeSource {
            schema: Arc::clone(&schema),
            batches,
        });
        let provider = Arc::new(RangeStripeTableProvider::new(source));
        let stats = provider.stats();
        let context = SessionContext::new();
        context
            .register_table("stripes", provider)
            .expect("register provider");
        let batches = context
            .sql("SELECT SUM(amount) AS total FROM stripes WHERE tenant = 1")
            .await
            .expect("plan SQL")
            .collect()
            .await
            .expect("execute SQL");
        assert_eq!(batches.len(), 1);
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sum output");
        assert_eq!(total.value(0), 360);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.stripes_read, 3);
        assert_eq!(snapshot.batches_emitted, 3);
        assert_eq!(snapshot.rows_emitted, 12);
        assert_eq!(snapshot.peak_batch_rows, 4);
        assert_eq!(snapshot.projection_pushdown_plans, 1);
    }
}
