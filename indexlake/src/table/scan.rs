use futures::StreamExt;

use crate::{
    ILResult, RowStream, TransactionHelper,
    record::{Row, SchemaRef},
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &SchemaRef,
) -> ILResult<RowStream> {
    // Inline rows are not deleted, so we can scan them directly
    let rows = tx_helper.scan_inline_rows(table_id, schema).await?;
    Ok(Box::pin(futures::stream::iter(rows).map(Ok)))
}
