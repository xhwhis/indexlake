use crate::{
    ILResult, TransactionHelper,
    record::{Row, SchemaRef},
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &SchemaRef,
) -> ILResult<Vec<Row>> {
    // Inline rows are not deleted, so we can scan them directly
    tx_helper.scan_inline_rows(table_id, schema).await
}
