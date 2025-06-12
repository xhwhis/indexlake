use crate::{CatalogRow, ILResult, LakeClient, schema::SchemaRef};

pub async fn insert_rows(
    client: &LakeClient,
    table_id: i64,
    schema: &SchemaRef,
    rows: Vec<CatalogRow>,
) -> ILResult<()> {
    let mut tx_helper = client.transaction_helper().await?;

    tx_helper.insert_rows(table_id, schema, rows).await?;

    Ok(())
}
