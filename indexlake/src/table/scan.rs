use std::sync::Arc;

use crate::{CatalogRow, ILResult, TransactionHelper, schema::SchemaRef};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    schema: &SchemaRef,
) -> ILResult<Vec<CatalogRow>> {
    let catalog_schema = Arc::new(schema.to_catalog_schema());
    tx_helper.scan_rows(table_id, catalog_schema).await
}
