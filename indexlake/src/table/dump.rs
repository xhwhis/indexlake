use std::{collections::HashMap, sync::Arc};

use log::{debug, error};

use crate::{
    ILError, ILResult, TransactionHelper, catalog::Catalog, record::SchemaRef, storage::Storage,
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
        let mut tx_helper = TransactionHelper::new(&self.catalog).await?;
        if tx_helper.insert_dump_task(self.table_id).await.is_err() {
            debug!("Table {} already has a dump task", self.table_id);
            return Ok(());
        }

        let dump_row_ids = self.get_dump_row_ids().await?;
        if dump_row_ids.len() != 3000 {
            debug!(
                "Table {} has less than 3000 inline rows, skip dump",
                self.table_id
            );
            return Ok(());
        }

        let row_id_to_location_map = self.write_dump_file(&dump_row_ids).await?;

        tx_helper
            .update_row_locations(self.table_id, &row_id_to_location_map)
            .await?;

        let deleted_count = tx_helper
            .delete_inline_rows(self.table_id, &dump_row_ids)
            .await?;
        if deleted_count != dump_row_ids.len() {
            return Err(ILError::InternalError(format!(
                "Dump row count mismatch: {} inline rows deleted, expected {}",
                deleted_count,
                dump_row_ids.len()
            )));
        }

        tx_helper.delete_dump_task(self.table_id).await?;

        tx_helper.commit().await?;

        Ok(())
    }

    async fn get_dump_row_ids(&self) -> ILResult<Vec<i64>> {
        let mut tx_helper = TransactionHelper::new(&self.catalog).await?;
        let dump_row_ids = tx_helper
            .scan_inline_row_ids_with_limit(self.table_id, 3000)
            .await?;
        tx_helper.commit().await?;
        Ok(dump_row_ids)
    }

    async fn write_dump_file(&self, row_ids: &[i64]) -> ILResult<HashMap<i64, String>> {
        let mut tx_helper = TransactionHelper::new(&self.catalog).await?;
        let row_stream = tx_helper
            .scan_inline_rows_by_row_ids(self.table_id, &self.table_schema, row_ids)
            .await?;
        tx_helper.commit().await?;
        // TODO
        Ok(HashMap::new())
    }
}
