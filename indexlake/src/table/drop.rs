use crate::ILResult;
use crate::catalog::TransactionHelper;

pub(crate) async fn process_table_drop(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
) -> ILResult<()> {
    tx_helper.drop_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;
    tx_helper.delete_fields(table_id).await?;
    tx_helper.delete_table(table_id).await?;

    // TODO clean up index data

    Ok(())
}
