use crate::{Catalog, ILError, ILResult, Storage, client::LakeClient, schema::SchemaRef};

pub struct TableCreation {
    pub namespace_name: String,
    pub table_name: String,
    pub schema: SchemaRef,
}

pub(crate) async fn create_table(client: &LakeClient, creation: TableCreation) -> ILResult<i64> {
    let mut tx_helper = client.transaction_helper().await?;

    let namespace_id = tx_helper
        .get_namespace_id(&creation.namespace_name)
        .await?
        .ok_or_else(|| {
            ILError::CatalogError(format!("Namespace {} not found", creation.namespace_name))
        })?;

    let table_id = tx_helper
        .create_table(namespace_id, &creation.table_name)
        .await?;

    tx_helper
        .create_table_fields(table_id, &creation.schema.fields)
        .await?;

    tx_helper.commit().await?;

    Ok(table_id)
}
