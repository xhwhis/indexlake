use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{Statistics, project_schema};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::TryStreamExt;
use indexlake::table::{Table, TableScan};
use log::error;

#[derive(Debug)]
pub struct IndexLakeScanExec {
    table: Arc<Table>,
    scan: TableScan,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl IndexLakeScanExec {
    pub fn try_new(
        table: Arc<Table>,
        scan: TableScan,
        limit: Option<usize>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = project_schema(&table.schema, scan.projection.as_ref())?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            table,
            scan,
            limit,
            properties,
        })
    }
}

impl ExecutionPlan for IndexLakeScanExec {
    fn name(&self) -> &str {
        "IndexLakeScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "IndexLakeScanExec only supports one partition".to_string(),
            ));
        }
        let projected_schema = self.schema();
        let fut = get_batch_stream(
            self.table.clone(),
            projected_schema.clone(),
            self.scan.clone(),
            self.limit,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        if let Some(partition) = partition
            && partition != 0
        {
            return Err(DataFusionError::Execution(
                "IndexLakeScanExec only supports one partition".to_string(),
            ));
        }
        let row_count_result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.table.count().await })
        });
        match row_count_result {
            Ok(row_count) => Ok(Statistics {
                num_rows: Precision::Exact(row_count),
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            }),
            Err(e) => Err(DataFusionError::Plan(format!(
                "Error getting indexlake table {}.{} row count: {:?}",
                self.table.namespace_name, self.table.table_name, e
            ))),
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        match IndexLakeScanExec::try_new(self.table.clone(), self.scan.clone(), limit) {
            Ok(exec) => Some(Arc::new(exec)),
            Err(e) => {
                error!("Failed to create IndexLakeScanExec with fetch: {e}");
                None
            }
        }
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }
}

impl DisplayAs for IndexLakeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IndexLakeScanExec")
    }
}

async fn get_batch_stream(
    table: Arc<Table>,
    projected_schema: SchemaRef,
    mut scan: TableScan,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    // TODO support output partitioning
    if let Some(limit) = limit
        && limit < 1024
    {
        scan.batch_size = Some(limit);
    }
    let stream = table
        .scan(scan)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let stream = stream.map_err(|e| DataFusionError::Execution(e.to_string()));
    let stream = Box::pin(RecordBatchStreamAdapter::new(projected_schema, stream));
    let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let limit_stream = LimitStream::new(stream, 0, limit, metrics);
    Ok(Box::pin(limit_stream))
}
