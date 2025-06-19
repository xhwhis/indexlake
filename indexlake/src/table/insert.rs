use crate::{
    ILError, ILResult, TransactionHelper,
    catalog::INLINE_COLUMN_NAME_PREFIX,
    record::{INTERNAL_ROW_ID_FIELD, Scalar, Schema},
};

pub(crate) async fn process_insert_values(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &Schema,
    values: Vec<Vec<Scalar>>,
) -> ILResult<()> {
    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    // Generate row id for each row
    let mut new_values = vec![];
    let mut row_metadatas = vec![];
    let mut row_id = max_row_id + 1;
    for value in values {
        let mut new_value = vec![Scalar::BigInt(Some(row_id))];
        new_value.extend(value);

        new_values.push(new_value);

        row_metadatas.push((row_id, "inline".to_string()));

        row_id += 1;
    }

    let mut inline_field_names = vec![INTERNAL_ROW_ID_FIELD.name.clone()];
    for field in &schema.fields {
        let field_id = field
            .id
            .ok_or_else(|| ILError::InvalidInput("Field id is not set".to_string()))?;
        inline_field_names.push(format!("{INLINE_COLUMN_NAME_PREFIX}{field_id}"));
    }
    tx_helper
        .insert_inline_rows(table_id, &inline_field_names, new_values)
        .await?;

    tx_helper
        .insert_row_metadatas(table_id, &row_metadatas)
        .await?;
    Ok(())
}
