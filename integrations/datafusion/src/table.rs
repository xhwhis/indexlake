use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{DFSchema, Statistics, stats::Precision},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use indexlake::table::{Table, TableScanPartition};
use log::warn;

use crate::{IndexLakeInsertExec, IndexLakeScanExec, datafusion_expr_to_indexlake_expr};

#[derive(Debug)]
pub struct IndexLakeTable {
    table: Arc<Table>,
    partition_count: usize,
}

impl IndexLakeTable {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            partition_count: num_cpus::get(),
        }
    }

    pub fn with_partition_count(mut self, partition_count: usize) -> Self {
        self.partition_count = partition_count;
        self
    }
}

#[async_trait::async_trait]
impl TableProvider for IndexLakeTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = IndexLakeScanExec::try_new(
            self.table.clone(),
            self.partition_count,
            projection.cloned(),
            filters.to_vec(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let df_schema = DFSchema::try_from(self.table.schema.clone())?;
        let mut supports = Vec::with_capacity(filters.len());
        for filter in filters {
            let Ok(il_expr) = datafusion_expr_to_indexlake_expr(filter, &df_schema) else {
                supports.push(TableProviderFilterPushDown::Unsupported);
                continue;
            };
            if self.table.supports_filter(&il_expr).is_ok() {
                supports.push(TableProviderFilterPushDown::Exact);
            } else {
                supports.push(TableProviderFilterPushDown::Unsupported);
            }
        }
        Ok(supports)
    }

    fn statistics(&self) -> Option<Statistics> {
        let row_count_result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.table
                    .count(TableScanPartition::single_partition())
                    .await
            })
        });
        match row_count_result {
            Ok(row_count) => Some(Statistics {
                num_rows: Precision::Exact(row_count),
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            }),
            Err(e) => {
                warn!(
                    "[indexlake] Error getting indexlake table {}.{} row count: {:?}",
                    self.table.namespace_name, self.table.table_name, e
                );
                None
            }
        }
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = IndexLakeInsertExec::try_new(self.table.clone(), input, insert_op)?;
        Ok(Arc::new(exec))
    }
}
