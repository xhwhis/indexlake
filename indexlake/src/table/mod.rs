mod create;
mod delete;
mod insert;
mod scan;

pub use create::*;
pub use delete::*;
pub use insert::*;
pub use scan::*;

use crate::expr::Expr;
use crate::record::{Row, Scalar, SchemaRef};
use crate::utils::has_duplicated_items;
use crate::{Catalog, ILError, ILResult, Storage, TransactionHelper};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: i64,
    pub namespace_name: String,
    pub table_id: i64,
    pub table_name: String,
    pub schema: SchemaRef,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
}

impl Table {
    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        let transaction = self.catalog.transaction().await?;
        Ok(TransactionHelper::new(transaction))
    }

    pub async fn insert(&self, columns: &[String], values: Vec<Vec<Scalar>>) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        if has_duplicated_items(columns.iter()) {
            return Err(ILError::InvalidInput("Duplicated column names".to_string()));
        }
        let projected_schema = self.schema.project(columns);
        process_insert_values(&mut tx_helper, self.table_id, &projected_schema, values).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    // TODO stream rows
    pub async fn scan(&self) -> ILResult<Vec<Row>> {
        let mut tx_helper = self.transaction_helper().await?;
        let rows = process_table_scan(&mut tx_helper, self.table_id, &self.schema).await?;
        tx_helper.commit().await?;
        Ok(rows)
    }

    pub async fn update(
        &self,
        condition: Expr,
        columns: &[String],
        values: Vec<Scalar>,
    ) -> ILResult<()> {
        todo!()
    }

    pub async fn delete(&self, condition: &Expr) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_delete_rows(&mut tx_helper, self.table_id, &self.schema, condition).await?;
        tx_helper.commit().await?;
        Ok(())
    }
}
