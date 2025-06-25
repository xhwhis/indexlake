use std::sync::Arc;

use futures::StreamExt;

use crate::{
    ILResult, RowStream, TransactionHelper,
    record::{Row, SchemaRef},
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
) -> ILResult<RowStream<'static>> {
    let schema = Arc::new(table_schema.without_row_id());
    // Inline rows are not deleted, so we can scan them directly
    let rows = tx_helper.scan_inline_rows(table_id, &schema).await?;
    Ok(Box::pin(futures::stream::iter(rows).map(Ok)))
}
