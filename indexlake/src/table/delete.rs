use crate::ILResult;
use crate::catalog::TransactionHelper;

pub(crate) async fn process_delete_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    row_ids: Vec<i64>,
) -> ILResult<()> {
    // Delete rows by their internal row IDs
    tx_helper.mark_rows_deleted(table_id, &row_ids).await?;
    Ok(())
}
