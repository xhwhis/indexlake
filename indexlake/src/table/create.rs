use crate::{
    ILError, ILResult,
    catalog::{TableRecord, TransactionHelper},
    table::TableCreation,
};

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

    let max_table_id = tx_helper.get_max_table_id().await?;
    let table_id = max_table_id + 1;
    tx_helper
        .insert_table(&TableRecord {
            table_id,
            table_name: creation.table_name,
            namespace_id,
            config: creation.config,
        })
        .await?;

    let max_field_id = tx_helper.get_max_field_id().await?;
    let field_ids = (max_field_id + 1..max_field_id + 1 + creation.schema.fields.len() as i64)
        .collect::<Vec<_>>();
    tx_helper
        .insert_fields(table_id, &field_ids, &creation.schema.fields)
        .await?;

    tx_helper.create_row_metadata_table(table_id).await?;
    tx_helper
        .create_inline_row_table(table_id, &creation.schema.fields)
        .await?;

    Ok(table_id)
}
