use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray};
use arrow::datatypes::SchemaRef;

use crate::catalog::{
    CatalogSchema, INTERNAL_ROW_ID_FIELD_NAME, TransactionHelper, rows_to_record_batch,
};
use crate::expr::{Expr, visited_columns};
use crate::{ILError, ILResult};

pub(crate) async fn process_delete_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
        return process_delete_rows_by_row_id_condition(tx_helper, table_id, condition).await;
    }

    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&table_schema)?);
    let rows = tx_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let record_batch = rows_to_record_batch(table_schema, &rows)?;

    let array = condition
        .eval(&record_batch)?
        .into_array(record_batch.num_rows())?;
    let bool_array = array.as_boolean_opt().ok_or_else(|| {
        ILError::InternalError(format!(
            "Expected BooleanArray, got {:?}",
            array.data_type()
        ))
    })?;

    let mut row_ids = Vec::new();
    for (i, v) in bool_array.iter().enumerate() {
        if let Some(v) = v
            && v
        {
            row_ids.push(
                rows[i]
                    .int64(0)?
                    .expect("Internal row ID should not be null"),
            );
        }
    }
    // Delete rows by their internal row IDs
    tx_helper
        .mark_rows_deleted_by_row_ids(table_id, &row_ids)
        .await?;

    // Directly delete inline rows
    tx_helper
        .delete_inline_rows_by_row_ids(table_id, &row_ids)
        .await?;
    Ok(())
}

pub(crate) async fn process_delete_rows_by_row_id_condition(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    row_id_condition: &Expr,
) -> ILResult<()> {
    tx_helper
        .mark_rows_deleted_by_condition(table_id, row_id_condition)
        .await?;
    tx_helper
        .delete_inline_rows_by_condition(table_id, row_id_condition)
        .await?;
    Ok(())
}
