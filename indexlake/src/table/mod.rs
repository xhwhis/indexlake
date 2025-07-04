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
pub use create::*;
pub(crate) use delete::*;
pub(crate) use drop::*;
pub(crate) use dump::*;
pub(crate) use insert::*;
pub use scan::*;
pub(crate) use truncate::*;
pub(crate) use update::*;

use crate::RecordBatchStream;
use crate::catalog::{CatalogHelper, CatalogSchemaRef, Scalar};
use crate::expr::Expr;
use crate::index::{Index, IndexDefination, IndexDefinationRef, SearchQuery};
use crate::utils::{has_duplicated_items, schema_with_row_id};
use crate::{
    ILError, ILResult,
    catalog::{Catalog, RowStream, TransactionHelper},
    storage::Storage,
};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: i64,
    pub namespace_name: String,
    pub table_id: i64,
    pub table_name: String,
    pub field_map: BTreeMap<i64, FieldRef>,
    pub schema: SchemaRef,
    pub indexes: HashMap<String, IndexDefinationRef>,
    pub config: Arc<TableConfig>,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
    pub index_kinds: HashMap<String, Arc<dyn Index>>,
}

impl Table {
    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        TransactionHelper::new(&self.catalog).await
    }

    pub async fn create_index(&mut self, index_creation: IndexCreation) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_create_index(&mut tx_helper, self, index_creation).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    pub async fn insert(&self, record: &RecordBatch) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        let schema = schema_with_row_id(&record.schema());
        if &schema != self.schema.as_ref() {
            return Err(ILError::InvalidInput(format!(
                "Schema mismatch: table schema {:?}, record batch schema {:?}",
                self.schema, schema
            )));
        }
        process_insert(&mut tx_helper, self.table_id, record).await?;
        tx_helper.commit().await?;

        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let inline_row_count = catalog_helper.count_inline_rows(self.table_id).await?;
        if inline_row_count as usize >= self.config.inline_row_count_limit {
            spawn_dump_task(self).await?;
        }

        Ok(())
    }

    pub async fn scan(&self, scan: TableScan) -> ILResult<RecordBatchStream> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let record_batch_stream = process_scan(
            &catalog_helper,
            self.table_id,
            &self.schema,
            scan,
            self.storage.clone(),
            self,
        )
        .await?;
        Ok(record_batch_stream)
    }

    pub async fn search(&self, query: Arc<dyn SearchQuery>) -> ILResult<RecordBatchStream> {
        todo!()
    }

    pub async fn update(&self, set_map: HashMap<String, Scalar>, condition: &Expr) -> ILResult<()> {
        check_condition_data_type(condition, &self.schema)?;
        let mut tx_helper = self.transaction_helper().await?;
        process_update(
            &mut tx_helper,
            self.storage.clone(),
            self.table_id,
            &self.schema,
            set_map,
            condition,
        )
        .await?;
        tx_helper.commit().await?;
        Ok(())
    }

    pub async fn delete(&self, condition: &Expr) -> ILResult<()> {
        check_condition_data_type(condition, &self.schema)?;
        let mut tx_helper = self.transaction_helper().await?;
        process_delete(
            &mut tx_helper,
            self.storage.clone(),
            self.table_id,
            &self.schema,
            condition,
        )
        .await?;
        tx_helper.commit().await?;
        Ok(())
    }

    // Delete all rows in the table
    pub async fn truncate(&self) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_truncate(&mut tx_helper, self.table_id).await?;
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

fn check_condition_data_type(condition: &Expr, schema: &SchemaRef) -> ILResult<()> {
    let data_type = condition.data_type(schema)?;
    if data_type != DataType::Boolean {
        return Err(ILError::InvalidInput(format!(
            "Condition must be a boolean expression, but got {data_type}"
        )));
    }
    Ok(())
}
