mod config;
mod create;
mod delete;
mod drop;
mod dump;
mod insert;
mod scan;
mod truncate;
mod update;

pub use config::*;
pub(crate) use create::*;
pub(crate) use delete::*;
pub(crate) use drop::*;
pub(crate) use dump::*;
pub(crate) use insert::*;
pub(crate) use scan::*;
pub(crate) use truncate::*;
pub(crate) use update::*;

use crate::expr::Expr;
use crate::record::{Scalar, SchemaRef};
use crate::utils::has_duplicated_items;
use crate::{
    ILError, ILResult,
    catalog::{Catalog, RowStream, TransactionHelper},
    storage::Storage,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableCreation {
    pub namespace_name: String,
    pub table_name: String,
    pub schema: SchemaRef,
    pub config: TableConfig,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: i64,
    pub namespace_name: String,
    pub table_id: i64,
    pub table_name: String,
    pub schema: SchemaRef,
    pub config: TableConfig,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
}

impl Table {
    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        TransactionHelper::new(&self.catalog).await
    }

    pub async fn insert(&self, columns: &[String], values: Vec<Vec<Scalar>>) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        if has_duplicated_items(columns.iter()) {
            return Err(ILError::InvalidInput("Duplicated column names".to_string()));
        }
        let projected_schema = self.schema.project(columns);
        process_insert_values(&mut tx_helper, self.table_id, &projected_schema, values).await?;
        let inline_row_count = tx_helper.count_inline_rows(self.table_id).await?;
        tx_helper.commit().await?;

        if inline_row_count >= 3000 {
            spawn_dump_task(self).await?;
        }

        Ok(())
    }

    pub async fn scan(&self) -> ILResult<RowStream> {
        let mut tx_helper = self.transaction_helper().await?;
        let row_stream = process_table_scan(&mut tx_helper, self.table_id, &self.schema).await?;
        tx_helper.commit().await?;
        Ok(row_stream)
    }

    pub async fn update(&self, set_map: HashMap<String, Scalar>, condition: &Expr) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_update_rows(&mut tx_helper, self.table_id, set_map, condition).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    pub async fn delete(&self, condition: &Expr) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_delete_rows(&mut tx_helper, self.table_id, &self.schema, condition).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    // Delete all rows in the table
    pub async fn truncate(&self) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_table_truncate(&mut tx_helper, self.table_id).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    // Drop the table
    pub async fn drop(self) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_table_drop(&mut tx_helper, self.table_id).await?;
        tx_helper.commit().await?;
        Ok(())
    }
}
