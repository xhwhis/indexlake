use crate::{
    ILError, ILResult, TransactionHelper,
    record::{INTERNAL_ROW_ID_FIELD, Scalar, Schema},
};

pub(crate) async fn process_insert_values(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &Schema,
    values: Vec<Vec<Scalar>>,
) -> ILResult<()> {
    let mut fields = vec![INTERNAL_ROW_ID_FIELD.clone()];
    fields.extend_from_slice(&schema.fields);

    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    // Generate row id for each row
    let mut new_values = vec![];
    let mut row_metadatas = vec![];
    let mut row_id = max_row_id + 1;
    for value in values {
        let mut new_value = vec![Scalar::BigInt(Some(row_id))];
        new_value.extend(value);

        if fields.len() != new_value.len() {
            return Err(ILError::InvalidInput(
                "column count does not match value count".to_string(),
            ));
        }

        new_values.push(new_value);

        row_metadatas.push((row_id, "inline".to_string()));

        row_id += 1;
    }

    tx_helper
        .insert_inline_rows(table_id, &fields, new_values)
        .await?;

    tx_helper
        .insert_row_metadatas(table_id, &row_metadatas)
        .await?;
    Ok(())
}
