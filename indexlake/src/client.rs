use crate::catalog::TransactionHelper;
use crate::table::{Table, TableCreation, process_create_table};
use crate::{Catalog, ILError, ILResult, Storage};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LakeClient {
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
}

impl LakeClient {
    pub fn new(catalog: Arc<dyn Catalog>, storage: Arc<Storage>) -> Self {
        Self { catalog, storage }
    }

    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        let transaction = self.catalog.transaction().await?;
        Ok(TransactionHelper::new(transaction))
    }

    pub async fn create_namespace(&self, namespace_name: &str) -> ILResult<i64> {
        let mut tx_helper = self.transaction_helper().await?;

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
        let table_id = tx_helper
            .get_table_id(namespace_id, table_name)
            .await?
            .ok_or_else(|| {
                ILError::CatalogError(format!(
                    "Table {table_name} not found in namespace {namespace_name}"
                ))
            })?;
        let schema = tx_helper.get_table_schema(table_id).await?;
        Ok(Table {
            namespace_id,
            namespace_name: namespace_name.to_string(),
            table_id,
            table_name: table_name.to_string(),
            schema,
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
        })
    }
}
