use crate::{
    ILResult, TransactionHelper,
    record::{DataType, Field, INTERNAL_ROW_ID_FIELD_NAME, Scalar, Schema},
};

pub(crate) async fn process_insert_values(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &Schema,
    values: Vec<Vec<Scalar>>,
) -> ILResult<()> {
    let mut fields = vec![Field::new(
        INTERNAL_ROW_ID_FIELD_NAME,
        DataType::BigInt,
        false,
        None,
    )];
    fields.extend_from_slice(&schema.fields);

    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    // Generate row id for each row
    let mut new_values = vec![];
    let mut row_id = max_row_id + 1;
    for value in values {
        let mut new_value = vec![Scalar::BigInt(Some(row_id))];
        new_value.extend(value);
        new_values.push(new_value);

        row_id += 1;
    }

    tx_helper
        .insert_values(table_id, &fields, new_values)
        .await?;

    Ok(())
}
