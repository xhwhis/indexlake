mod create;
mod delete;
mod dump;
mod insert;
mod scan;
mod search;
mod update;

pub use create::*;
pub(crate) use delete::*;
pub(crate) use dump::*;
pub(crate) use insert::*;
use log::warn;
pub use scan::*;
pub use search::*;
pub(crate) use update::*;
use uuid::Uuid;

use crate::RecordBatchStream;
use crate::catalog::IndexFileRecord;
use crate::catalog::{CatalogHelper, DataFileRecord, Scalar};
use crate::expr::Expr;
use crate::index::{IndexDefinationRef, IndexKind};
use crate::utils::schema_without_row_id;
use crate::{
    ILError, ILResult,
    catalog::{Catalog, TransactionHelper},
    storage::Storage,
};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: Uuid,
    pub namespace_name: String,
    pub table_id: Uuid,
    pub table_name: String,
    pub field_map: BTreeMap<Uuid, FieldRef>,
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

    pub async fn insert(&self, batches: &[RecordBatch]) -> ILResult<()> {
        let expected_schema = schema_without_row_id(&self.schema);
        let mut total_rows = 0;
        for batch in batches {
            total_rows += batch.num_rows();
            let batch_schema = batch.schema();
            if batch_schema.as_ref() != &expected_schema {
                return Err(ILError::InvalidInput(format!(
                    "Invalid record schema: {batch_schema:?}, expected schema: {expected_schema:?}",
                )));
            }
        }

        if total_rows >= self.config.inline_row_count_limit {
            let mut tx_helper = self.transaction_helper().await?;
            process_bypass_insert(&mut tx_helper, self, batches, total_rows).await?;
            tx_helper.commit().await?;
        } else {
            let mut tx_helper = self.transaction_helper().await?;
            process_insert_into_inline_rows(&mut tx_helper, &self.table_id, batches).await?;
            tx_helper.commit().await?;

            try_run_dump_task(self).await?;
        }

        Ok(())
    }

    pub async fn scan(&self, scan: TableScan) -> ILResult<RecordBatchStream> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let batch_stream = process_scan(
            &catalog_helper,
            self.table_id,
            &self.schema,
            scan,
            self.storage.clone(),
            self,
        )
        .await?;
        Ok(batch_stream)
    }

    pub async fn count(&self) -> ILResult<usize> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let inline_row_count = catalog_helper.count_inline_rows(&self.table_id).await? as usize;
        let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;
        let data_file_row_count: usize = data_file_records
            .iter()
            .map(|record| record.validity.valid_row_count())
            .sum();
        Ok(inline_row_count + data_file_row_count)
    }

    pub async fn search(&self, search: TableSearch) -> ILResult<RecordBatchStream> {
        let batch_stream = process_search(self, search).await?;
        Ok(batch_stream)
    }

    pub async fn update(&self, set_map: HashMap<String, Scalar>, condition: &Expr) -> ILResult<()> {
        condition.check_data_type(&self.schema, &DataType::Boolean)?;

        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;

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

        Ok(())
    }

    pub async fn delete(&self, condition: &Expr) -> ILResult<()> {
        condition.check_data_type(&self.schema, &DataType::Boolean)?;

        if condition.only_visit_row_id_column() {
            let mut tx_helper = self.transaction_helper().await?;
            process_delete_by_row_id_condition(&mut tx_helper, &self.table_id, condition).await?;
            tx_helper.commit().await?;
        } else {
            let catalog_helper = CatalogHelper::new(self.catalog.clone());
            let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;
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
                &self.table_id,
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
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;
        let index_file_records = catalog_helper.get_index_files(&self.table_id).await?;

        let mut tx_helper = self.transaction_helper().await?;
        process_catalog_truncate(&mut tx_helper, &self.table_id).await?;
        tx_helper.commit().await?;

        spawn_storage_clean_task(self.storage.clone(), data_file_records, index_file_records);
        Ok(())
    }

    // Drop the table
    pub async fn drop(self) -> ILResult<()> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;
        let index_file_records = catalog_helper.get_index_files(&self.table_id).await?;

        let mut tx_helper = self.transaction_helper().await?;
        process_catalog_drop(&mut tx_helper, &self.table_id).await?;
        tx_helper.commit().await?;

        spawn_storage_clean_task(self.storage.clone(), data_file_records, index_file_records);
        Ok(())
    }

    pub async fn optimize(&self) -> ILResult<()> {
        // TODO
        Ok(())
    }

    pub fn supports_filter(&self, filter: &Expr) -> ILResult<bool> {
        match filter {
            Expr::Function(_) => {
                for (_, index_def) in &self.indexes {
                    let index_kind =
                        self.index_kinds
                            .get(&index_def.kind)
                            .ok_or(ILError::InvalidInput(format!(
                                "Index kind {} not found",
                                index_def.kind
                            )))?;
                    if index_kind.supports_filter(index_def, filter)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(true),
        }
    }
}

async fn process_catalog_truncate(
    tx_helper: &mut TransactionHelper,
    table_id: &Uuid,
) -> ILResult<()> {
    tx_helper.truncate_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;
    tx_helper.delete_all_index_files(table_id).await?;

    Ok(())
}

async fn process_catalog_drop(tx_helper: &mut TransactionHelper, table_id: &Uuid) -> ILResult<()> {
    tx_helper.drop_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;
    tx_helper.delete_all_index_files(table_id).await?;

    tx_helper.delete_indexes(table_id).await?;
    tx_helper.delete_fields(table_id).await?;
    tx_helper.delete_table(table_id).await?;

    Ok(())
}

fn spawn_storage_clean_task(
    storage: Arc<Storage>,
    data_file_records: Vec<DataFileRecord>,
    index_file_records: Vec<IndexFileRecord>,
) {
    tokio::spawn(async move {
        for data_file_record in data_file_records {
            if let Err(e) = storage.delete(&data_file_record.relative_path).await {
                warn!(
                    "[indexlake] Failed to delete data file {}: {}",
                    data_file_record.relative_path, e
                );
            }
        }
        for index_file_record in index_file_records {
            if let Err(e) = storage.delete(&index_file_record.relative_path).await {
                warn!(
                    "[indexlake] Failed to delete index file {}: {}",
                    index_file_record.relative_path, e
                );
            }
        }
    });
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub inline_row_count_limit: usize,
    pub parquet_row_group_size: usize,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            inline_row_count_limit: 100000,
            parquet_row_group_size: 1000,
        }
    }
}
