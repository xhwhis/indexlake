use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray};
use arrow::datatypes::SchemaRef;

use crate::catalog::{CatalogSchema, TransactionHelper, rows_to_record_batch};
use crate::expr::Expr;
use crate::{ILError, ILResult};

pub(crate) async fn process_delete_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
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
        // TODO let chain
        if v.is_some() && v.unwrap() {
            row_ids.push(
                rows[i]
                    .int64(0)?
                    .expect("Internal row ID should not be null"),
            );
        }
    }
    // Delete rows by their internal row IDs
    tx_helper.mark_rows_deleted(table_id, &row_ids).await?;

    // Directly delete inline rows
    tx_helper.delete_inline_rows(table_id, &row_ids).await?;
    Ok(())
}
