use arrow::datatypes::Schema;
use uuid::Uuid;

use crate::catalog::CatalogHelper;
use crate::catalog::INTERNAL_ROW_ID_FIELD_REF;
use crate::catalog::TransactionHelper;
use crate::index::IndexDefination;
use crate::index::IndexKind;
use crate::table::{Table, TableCreation, process_create_table};
use crate::{ILError, ILResult, catalog::Catalog, storage::Storage};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LakeClient {
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
    pub index_kinds: HashMap<String, Arc<dyn IndexKind>>,
}

impl LakeClient {
    pub fn new(catalog: Arc<dyn Catalog>, storage: Arc<Storage>) -> Self {
        Self {
            catalog,
            storage,
            index_kinds: HashMap::new(),
        }
    }

    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        TransactionHelper::new(&self.catalog).await
    }

    pub fn register_index_kind(&mut self, index_kind: Arc<dyn IndexKind>) -> ILResult<()> {
        self.index_kinds
            .insert(index_kind.kind().to_string(), index_kind);
        Ok(())
    }

    pub async fn create_namespace(
        &self,
        namespace_name: &str,
        if_not_exists: bool,
    ) -> ILResult<Uuid> {
        let mut tx_helper = self.transaction_helper().await?;

        if let Some(namespace_id) = tx_helper.get_namespace_id(namespace_name).await? {
            if if_not_exists {
                return Ok(namespace_id);
            } else {
                return Err(ILError::InvalidInput(format!(
                    "Namespace {namespace_name} already exists"
                )));
            }
        }

        let namespace_id = Uuid::now_v7();
        tx_helper
            .insert_namespace(&namespace_id, namespace_name)
            .await?;
        tx_helper.commit().await?;

        Ok(namespace_id)
    }

    pub async fn get_namespace_id(&self, namespace_name: &str) -> ILResult<Option<Uuid>> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());
        let namespace_id = catalog_helper.get_namespace_id(namespace_name).await?;
        Ok(namespace_id)
    }

    pub async fn create_table(&self, table_creation: TableCreation) -> ILResult<Uuid> {
        let mut tx_helper = self.transaction_helper().await?;
        let table_id = process_create_table(&mut tx_helper, table_creation.clone()).await?;
        tx_helper.commit().await?;

        Ok(table_id)
    }

    pub async fn load_table(&self, namespace_name: &str, table_name: &str) -> ILResult<Table> {
        let catalog_helper = CatalogHelper::new(self.catalog.clone());

        let namespace_id = catalog_helper
            .get_namespace_id(namespace_name)
            .await?
            .ok_or_else(|| {
                ILError::InvalidInput(format!("Namespace {namespace_name} not found"))
            })?;

        let table_record = catalog_helper
            .get_table(&namespace_id, table_name)
            .await?
            .ok_or_else(|| {
                ILError::InvalidInput(format!(
                    "Table {table_name} not found in namespace {namespace_name}"
                ))
            })?;

        let field_map = catalog_helper
            .get_table_fields(&table_record.table_id)
            .await?;

        let mut fields = field_map.values().cloned().collect::<Vec<_>>();
        fields.insert(0, INTERNAL_ROW_ID_FIELD_REF.clone());
        // TODO support schema metadata
        let schema = Arc::new(Schema::new(fields));

        let index_records = catalog_helper
            .get_table_indexes(&table_record.table_id)
            .await?;
        let mut indexes = HashMap::new();
        for index_record in index_records {
            let index = IndexDefination::from_index_record(
                &index_record,
                &field_map,
                table_name,
                &schema,
                &self.index_kinds,
            )?;
            indexes.insert(index_record.index_name.clone(), Arc::new(index));
        }

        Ok(Table {
            namespace_id,
            namespace_name: namespace_name.to_string(),
            table_id: table_record.table_id,
            table_name: table_name.to_string(),
            field_map,
            schema,
            indexes,
            config: Arc::new(table_record.config),
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            index_kinds: self.index_kinds.clone(),
        })
    }
}
