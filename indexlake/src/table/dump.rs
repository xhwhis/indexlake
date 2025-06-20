use std::sync::Arc;

use log::error;

use crate::{
    ILResult, TransactionHelper, catalog::Catalog, record::SchemaRef, storage::Storage,
    table::Table,
};

pub(crate) async fn spawn_dump_task(table: &Table) -> ILResult<()> {
    let dump_task = DumpTask {
        table_id: table.table_id,
        table_schema: table.schema.clone(),
        catalog: table.catalog.clone(),
        storage: table.storage.clone(),
    };
    tokio::spawn(async move {
        if let Err(e) = dump_task.run().await {
            error!("Failed to dump table: {:?}", e);
        }
    });
    Ok(())
}

pub(crate) struct DumpTask {
    table_id: i64,
    table_schema: SchemaRef,
    catalog: Arc<dyn Catalog>,
    storage: Arc<Storage>,
}

impl DumpTask {
    async fn run(&self) -> ILResult<()> {
        let transaction = self.catalog.transaction().await?;
        let mut tx_helper = TransactionHelper::new(transaction, self.catalog.database());

        let mut row_ids = tx_helper.scan_inline_row_ids(self.table_id).await?;
        row_ids.sort();

        Ok(())
    }
}
