mod create;
mod delete;
mod dump;
mod insert;
mod scan;
mod search;
mod update;

use arrow_schema::Schema;
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
            check_insert_batch_schema(batch.schema_ref(), &expected_schema)?;
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
            .map(|record| record.valid_row_count())
            .sum();
        Ok(inline_row_count + data_file_row_count)
    }

    pub async fn search(&self, search: TableSearch) -> ILResult<RecordBatchStream> {
        let batch_stream = process_search(self, search).await?;
        Ok(batch_stream)
    }

    pub async fn update(&self, set_map: HashMap<String, Expr>, condition: &Expr) -> ILResult<()> {
        // TODO update all rows
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

        if let Ok(scalar) = condition.constant_eval()
            && matches!(scalar, Scalar::Boolean(Some(true)))
        {
            return self.truncate().await;
        }

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
        let index_file_records = catalog_helper.get_table_index_files(&self.table_id).await?;

        let mut tx_helper = self.transaction_helper().await?;

        tx_helper.truncate_inline_row_table(&self.table_id).await?;

        tx_helper.delete_all_data_files(&self.table_id).await?;
        tx_helper.delete_table_index_files(&self.table_id).await?;

        tx_helper.commit().await?;

        spawn_storage_data_files_clean_task(self.storage.clone(), data_file_records);
        spawn_storage_index_files_clean_task(self.storage.clone(), index_file_records);
        Ok(())
    }

    // Drop the table
    pub async fn drop(self) -> ILResult<()> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let data_file_records = catalog_helper.get_data_files(&self.table_id).await?;
        let index_file_records = catalog_helper.get_table_index_files(&self.table_id).await?;

        let mut tx_helper = self.transaction_helper().await?;

        tx_helper.drop_inline_row_table(&self.table_id).await?;

        tx_helper.delete_all_data_files(&self.table_id).await?;
        tx_helper.delete_table_index_files(&self.table_id).await?;

        tx_helper.delete_table_indexes(&self.table_id).await?;
        tx_helper.delete_fields(&self.table_id).await?;
        tx_helper.delete_table(&self.table_id).await?;

        tx_helper.commit().await?;

        spawn_storage_data_files_clean_task(self.storage.clone(), data_file_records);
        spawn_storage_index_files_clean_task(self.storage.clone(), index_file_records);
        Ok(())
    }

    pub async fn drop_index(&mut self, index_name: &str, if_exists: bool) -> ILResult<()> {
        let Some(index_def) = self.indexes.get(index_name) else {
            if if_exists {
                return Ok(());
            }
            return Err(ILError::InvalidInput(format!(
                "Index {index_name} not found"
            )));
        };

        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let index_file_records = catalog_helper
            .get_index_files_by_index_id(&index_def.index_id)
            .await?;

        let mut tx_helper = self.transaction_helper().await?;

        tx_helper.delete_index(&index_def.index_id).await?;
        tx_helper.delete_index_files(&index_def.index_id).await?;

        tx_helper.commit().await?;

        spawn_storage_index_files_clean_task(self.storage.clone(), index_file_records);

        self.indexes.remove(index_name);
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

fn spawn_storage_data_files_clean_task(
    storage: Arc<Storage>,
    data_file_records: Vec<DataFileRecord>,
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
    });
}

fn spawn_storage_index_files_clean_task(
    storage: Arc<Storage>,
    index_file_records: Vec<IndexFileRecord>,
) {
    tokio::spawn(async move {
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

fn check_insert_batch_schema(batch_schema: &Schema, expected_schema: &Schema) -> ILResult<()> {
    if batch_schema.fields().len() != expected_schema.fields().len() {
        return Err(ILError::InvalidInput(format!(
            "Invalid record schema: {batch_schema:?}, expected schema: {expected_schema:?}",
        )));
    }

    for (table_field, batch_field) in expected_schema
        .fields()
        .iter()
        .zip(batch_schema.fields().iter())
    {
        if table_field.name() != batch_field.name()
            || table_field.data_type() != batch_field.data_type()
            || table_field.is_nullable() != batch_field.is_nullable()
        {
            return Err(ILError::InvalidInput(format!(
                "Invalid record schema: {batch_schema:?}, expected schema: {expected_schema:?}",
            )));
        }
    }

    Ok(())
}
