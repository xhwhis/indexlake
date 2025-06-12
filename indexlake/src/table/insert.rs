use crate::{
    CatalogColumn, CatalogDataType, CatalogRow, CatalogScalar, CatalogSchema, ILResult, LakeClient,
    schema::SchemaRef,
};
use std::sync::Arc;

pub async fn insert_rows(
    client: &LakeClient,
    table_id: i64,
    schema: &SchemaRef,
    rows: Vec<CatalogRow>,
) -> ILResult<()> {
    let mut columns = vec![CatalogColumn::new("row_id", CatalogDataType::BigInt, false)];
    columns.extend(
        schema
            .fields
            .iter()
            .map(|f| f.to_catalog_column())
            .collect::<Vec<_>>(),
    );
    let catalog_schema = Arc::new(CatalogSchema::new(columns));

    let mut tx_helper = client.transaction_helper().await?;
    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    let mut full_rows = vec![];
    let mut row_id = max_row_id + 1;
    for row in rows {
        let mut values = vec![CatalogScalar::BigInt(Some(row_id))];
        values.extend(row.values);
        full_rows.push(CatalogRow::new(catalog_schema.clone(), values));

        row_id += 1;
    }

    tx_helper
        .insert_rows(table_id, &catalog_schema, full_rows)
        .await?;

    Ok(())
}
