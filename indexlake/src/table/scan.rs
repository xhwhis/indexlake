use crate::{
    ILResult, TransactionHelper,
    record::{Row, SchemaRef},
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &SchemaRef,
) -> ILResult<Vec<Row>> {
    tx_helper.scan_rows(table_id, schema).await
}
