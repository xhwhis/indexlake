use crate::ILResult;
use crate::catalog::TransactionHelper;

pub(crate) async fn process_truncate(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
) -> ILResult<()> {
    tx_helper.truncate_row_metadata_table(table_id).await?;
    tx_helper.truncate_inline_row_table(table_id).await?;

    tx_helper.delete_all_data_files(table_id).await?;

    // TODO clean up index data

    Ok(())
}
