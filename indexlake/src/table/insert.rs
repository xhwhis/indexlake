use crate::{
    ILResult, TransactionHelper,
    record::{DataType, Field, Row, Scalar, Schema, SchemaRef},
};
use std::sync::Arc;

pub(crate) async fn process_insert_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &SchemaRef,
    rows: Vec<Row>,
) -> ILResult<()> {
    let mut columns = vec![Field::new("row_id", DataType::BigInt, false, None)];
    columns.extend_from_slice(&schema.fields);
    let catalog_schema = Arc::new(Schema::new(columns));

    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    let mut full_rows = vec![];
    let mut row_id = max_row_id + 1;
    for row in rows {
        let mut values = vec![Scalar::BigInt(Some(row_id))];
        values.extend(row.values);
        full_rows.push(Row::new(catalog_schema.clone(), values));

        row_id += 1;
    }

    tx_helper
        .insert_rows(table_id, &catalog_schema, full_rows)
        .await?;

    Ok(())
}
