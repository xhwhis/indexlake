use crate::ILResult;
use crate::catalog::TransactionHelper;
use crate::expr::Expr;
use crate::record::SchemaRef;

pub(crate) async fn process_delete_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    let rows = tx_helper.scan_inline_rows(table_id, &table_schema).await?;

    let mut row_ids = Vec::new();
    for row in rows {
        let v = condition.eval(&row)?;
        if v.is_true() {
            row_ids.push(row.int64(0)?.expect("Internal row ID should not be null"));
        }
    }
    // Delete rows by their internal row IDs
    tx_helper.mark_rows_deleted(table_id, &row_ids).await?;

    // Directly delete inline rows
    tx_helper.delete_inline_rows(table_id, &row_ids).await?;
    Ok(())
}
