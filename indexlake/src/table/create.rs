use crate::{ILError, ILResult, TransactionHelper, schema::SchemaRef};

pub struct TableCreation {
    pub namespace_name: String,
    pub table_name: String,
    pub schema: SchemaRef,
}

pub(crate) async fn process_create_table(
    tx_helper: &mut TransactionHelper,
    creation: TableCreation,
) -> ILResult<i64> {
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

    tx_helper.create_row_table(table_id).await?;
    tx_helper
        .create_inline_table(table_id, &creation.schema)
        .await?;

    Ok(table_id)
}
