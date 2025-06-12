use crate::catalog::TransactionHelper;
use crate::table::{TableCreation, create_table};
use crate::{Catalog, ILResult, Storage};
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
        let namespace_id = tx_helper.create_namespace(namespace_name).await?;
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
        create_table(self, table_creation).await
    }
}
