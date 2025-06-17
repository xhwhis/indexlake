use std::sync::Arc;

use crate::ILResult;
use crate::catalog::TransactionHelper;
use crate::expr::Expr;
use crate::record::{INTERNAL_ROW_ID_FIELD, SchemaRef};

pub(crate) async fn process_delete_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    let mut schema = table_schema.as_ref().clone();
    schema.push_front(INTERNAL_ROW_ID_FIELD.clone());
    let schema = Arc::new(schema);

    let rows = tx_helper.scan_inline_rows(table_id, &schema).await?;

    let mut row_ids = Vec::new();
    for row in rows {
        let v = condition.eval(&row)?;
        if v.is_true() {
            row_ids.push(row.bigint(0).expect("Internal row ID should not be null"));
        }
    }
    // Delete rows by their internal row IDs
    tx_helper.mark_rows_deleted(table_id, &row_ids).await?;
    Ok(())
}
