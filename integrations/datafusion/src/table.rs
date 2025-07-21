use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Statistics, stats::Precision},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use indexlake::table::Table;
use log::warn;

use crate::datafusion_expr_to_indexlake_expr;

#[derive(Debug)]
pub struct IndexLakeTable {
    table: Table,
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let mut supports = Vec::with_capacity(filters.len());
        for filter in filters {
            let Ok(il_expr) = datafusion_expr_to_indexlake_expr(filter) else {
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
            tokio::runtime::Handle::current().block_on(async { self.table.count().await })
        });
        match row_count_result {
            Ok(row_count) => Some(Statistics {
                num_rows: Precision::Exact(row_count),
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            }),
            Err(e) => {
                warn!(
                    "Error getting indexlake table {}.{} row count: {:?}",
                    self.table.namespace_name, self.table.table_name, e
                );
                None
            }
        }
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}
