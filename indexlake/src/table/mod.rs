mod create;
mod delete;
mod dump;
mod insert;
mod scan;
mod update;

pub use create::*;
pub(crate) use delete::*;
pub(crate) use dump::*;
pub(crate) use insert::*;
pub use scan::*;
pub(crate) use update::*;

use crate::RecordBatchStream;
use crate::catalog::{CatalogHelper, CatalogSchemaRef, INTERNAL_ROW_ID_FIELD_NAME, Scalar};
use crate::expr::{Expr, visited_columns};
use crate::index::{IndexDefination, IndexDefinationRef, IndexKind, SearchQuery};
use crate::utils::{has_duplicated_items, schema_with_row_id};
use crate::{
    ILError, ILResult,
    catalog::{Catalog, RowStream, TransactionHelper},
    storage::Storage,
};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use serde::{Deserialize, Serialize};
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
    pub index_kinds: HashMap<String, Arc<dyn IndexKind>>,
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
        condition.check_data_type(&self.schema, &DataType::Boolean)?;
        if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
            todo!()
        } else {
            let catalog_helper = CatalogHelper::new(self.catalog.clone());
            let data_file_records = catalog_helper.get_data_files(self.table_id).await?;
            let matched_data_file_rows = parallel_find_matched_data_file_rows(
                self.storage.clone(),
                self.schema.clone(),
                condition.clone(),
                data_file_records,
            )
            .await?;

            let mut tx_helper = self.transaction_helper().await?;
            process_update_by_condition(
                &mut tx_helper,
                self.storage.clone(),
                self.table_id,
                &self.schema,
                set_map,
                condition,
                matched_data_file_rows,
            )
            .await?;
            tx_helper.commit().await?;
        }
        Ok(())
    }

    pub async fn delete(&self, condition: &Expr) -> ILResult<()> {
        condition.check_data_type(&self.schema, &DataType::Boolean)?;

        if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
            let mut tx_helper = self.transaction_helper().await?;
            process_delete_by_row_id_condition(&mut tx_helper, self.table_id, condition).await?;
            tx_helper.commit().await?;
        } else {
            let catalog_helper = CatalogHelper::new(self.catalog.clone());
            let data_file_records = catalog_helper.get_data_files(self.table_id).await?;
            let matched_data_file_row_ids = parallel_find_matched_data_file_row_ids(
                self.storage.clone(),
                self.schema.clone(),
                condition.clone(),
                data_file_records,
            )
            .await?;

            let mut tx_helper = self.transaction_helper().await?;
            process_delete_by_condition(
                &mut tx_helper,
                self.storage.clone(),
                self.table_id,
                &self.schema,
                condition,
                matched_data_file_row_ids,
            )
            .await?;
            tx_helper.commit().await?;
        }

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
        process_drop(&mut tx_helper, self.table_id).await?;
        tx_helper.commit().await?;
        Ok(())
    }
}

async fn process_truncate(tx_helper: &mut TransactionHelper, table_id: i64) -> ILResult<()> {
    tx_helper.truncate_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;
    tx_helper.delete_all_index_files(table_id).await?;

    Ok(())
}

async fn process_drop(tx_helper: &mut TransactionHelper, table_id: i64) -> ILResult<()> {
    tx_helper.drop_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;
    tx_helper.delete_all_index_files(table_id).await?;

    tx_helper.delete_indexes(table_id).await?;
    tx_helper.delete_fields(table_id).await?;
    tx_helper.delete_table(table_id).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub inline_row_count_limit: usize,
    pub parquet_row_group_size: usize,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            inline_row_count_limit: 10000,
            parquet_row_group_size: 1000,
        }
    }
}
