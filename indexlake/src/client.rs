use arrow::datatypes::Schema;

use crate::catalog::INTERNAL_ROW_ID_FIELD_REF;
use crate::catalog::TransactionHelper;
use crate::index::{FilterIndex, IndexKindManager, TopKIndex};
use crate::table::{Table, TableCreation, process_create_table};
use crate::{ILError, ILResult, catalog::Catalog, storage::Storage};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LakeClient {
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
    pub index_kind_manager: IndexKindManager,
}

impl LakeClient {
    pub fn new(catalog: Arc<dyn Catalog>, storage: Arc<Storage>) -> Self {
        Self {
            catalog,
            storage,
            index_kind_manager: IndexKindManager::new_empty(),
        }
    }

    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        TransactionHelper::new(&self.catalog).await
    }

    pub fn register_topk_index(&mut self, index: Arc<dyn TopKIndex>) -> ILResult<()> {
        self.index_kind_manager.register_topk_index(index)
    }

    pub fn register_filter_index(&mut self, index: Arc<dyn FilterIndex>) -> ILResult<()> {
        self.index_kind_manager.register_filter_index(index)
    }

    pub async fn create_namespace(&self, namespace_name: &str) -> ILResult<i64> {
        let mut tx_helper = self.transaction_helper().await?;

        if let Some(_) = tx_helper.get_namespace_id(namespace_name).await? {
            return Err(ILError::InvalidInput(format!(
                "Namespace {namespace_name} already exists"
            )));
        }

        let max_namespace_id = tx_helper.get_max_namespace_id().await?;
        let namespace_id = max_namespace_id + 1;

        tx_helper
            .insert_namespace(namespace_id, namespace_name)
            .await?;

        tx_helper.commit().await?;

        Ok(namespace_id)
    }

    pub async fn get_namespace_id(&self, namespace_name: &str) -> ILResult<Option<i64>> {
        let mut tx_helper = self.transaction_helper().await?;
        let namespace_id = tx_helper.get_namespace_id(namespace_name).await?;
        tx_helper.commit().await?;
        Ok(namespace_id)
    }

    pub async fn create_table(&self, table_creation: TableCreation) -> ILResult<i64> {
        let mut tx_helper = self.transaction_helper().await?;
        let table_id = process_create_table(&mut tx_helper, table_creation).await?;
        tx_helper.commit().await?;
        Ok(table_id)
    }

    pub async fn load_table(&self, namespace_name: &str, table_name: &str) -> ILResult<Table> {
        let mut tx_helper = self.transaction_helper().await?;

        let namespace_id = tx_helper
            .get_namespace_id(namespace_name)
            .await?
            .ok_or_else(|| {
                ILError::CatalogError(format!("Namespace {namespace_name} not found"))
            })?;

        let table_record = tx_helper
            .get_table(namespace_id, table_name)
            .await?
            .ok_or_else(|| {
                ILError::CatalogError(format!(
                    "Table {table_name} not found in namespace {namespace_name}"
                ))
            })?;

        let field_map = tx_helper.get_table_fields(table_record.table_id).await?;

        let mut fields = field_map.values().cloned().collect::<Vec<_>>();
        fields.insert(0, INTERNAL_ROW_ID_FIELD_REF.clone());
        // TODO support schema metadata
        let schema = Arc::new(Schema::new(fields));

        Ok(Table {
            namespace_id,
            namespace_name: namespace_name.to_string(),
            table_id: table_record.table_id,
            table_name: table_name.to_string(),
            field_map,
            schema,
            config: Arc::new(table_record.config),
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            index_kind_manager: self.index_kind_manager.clone(),
        })
    }
}
