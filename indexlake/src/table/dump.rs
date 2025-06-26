use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::{StreamExt, TryStreamExt};
use log::{debug, error};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::{
    ILError, ILResult,
    arrow::{rows_to_arrow_record, schema_to_arrow_schema},
    catalog::{Catalog, DataFileRecord, TransactionHelper},
    record::{Row, SchemaRef},
    storage::Storage,
    table::Table,
};

pub(crate) async fn spawn_dump_task(table: &Table) -> ILResult<()> {
    let mut tx_helper = TransactionHelper::new(&table.catalog).await?;
    let dump_row_ids = tx_helper
        .scan_inline_row_ids_with_limit(table.table_id, table.config.inline_row_count_limit)
        .await?;
    tx_helper.commit().await?;
    if dump_row_ids.len() < table.config.inline_row_count_limit {
        debug!(
            "Table {} has less than {} inline rows, skip dump",
            table.table_id, table.config.inline_row_count_limit
        );
        return Ok(());
    }

    let dump_task = DumpTask {
        namespace_id: table.namespace_id,
        table_id: table.table_id,
        table_schema: table.schema.clone(),
        catalog: table.catalog.clone(),
        storage: table.storage.clone(),
        dump_row_ids,
    };
    tokio::spawn(async move {
        let now = Instant::now();
        if let Err(e) = dump_task.run().await {
            error!("Failed to dump table: {:?}", e);
        }
        debug!(
            "Dump table {} inline rows in {} ms",
            dump_task.table_id,
            now.elapsed().as_millis()
        );
    });
    Ok(())
}

pub(crate) struct DumpTask {
    namespace_id: i64,
    table_id: i64,
    table_schema: SchemaRef,
    catalog: Arc<dyn Catalog>,
    storage: Arc<Storage>,
    dump_row_ids: Vec<i64>,
}

impl DumpTask {
    async fn run(&self) -> ILResult<()> {
        let mut tx_helper = TransactionHelper::new(&self.catalog).await?;
        if tx_helper.insert_dump_task(self.table_id).await.is_err() {
            debug!("Table {} already has a dump task", self.table_id);
            return Ok(());
        }

        let rows = tx_helper
            .scan_inline_rows_by_row_ids(self.table_id, &self.table_schema, &self.dump_row_ids)
            .await?;
        let record_count = rows.len() as i64;
        if record_count != self.dump_row_ids.len() as i64 {
            return Err(ILError::InternalError(format!(
                "Read row count mismatch: {} rows read, expected {}",
                record_count,
                self.dump_row_ids.len()
            )));
        }

        let relative_path = format!(
            "{}/{}/{}.parquet",
            self.namespace_id,
            self.table_id,
            uuid::Uuid::new_v4()
        );
        let (location_map, file_size_bytes) = self.write_dump_file(rows, &relative_path).await?;

        let data_file_id = tx_helper.get_max_data_file_id().await? + 1;
        tx_helper
            .insert_data_files(&[DataFileRecord {
                data_file_id,
                table_id: self.table_id,
                relative_path,
                file_size_bytes,
                record_count,
            }])
            .await?;

        tx_helper
            .update_row_locations(self.table_id, &location_map)
            .await?;

        let deleted_count = tx_helper
            .delete_inline_rows(self.table_id, &self.dump_row_ids)
            .await?;
        if deleted_count != self.dump_row_ids.len() {
            return Err(ILError::InternalError(format!(
                "Delete row count mismatch: {} inline rows deleted, expected {}",
                deleted_count,
                self.dump_row_ids.len()
            )));
        }

        tx_helper.delete_dump_task(self.table_id).await?;

        tx_helper.commit().await?;

        Ok(())
    }

    async fn write_dump_file(
        &self,
        rows: Vec<Row>,
        relative_path: &str,
    ) -> ILResult<(HashMap<i64, String>, i64)> {
        let mut location_map = HashMap::new();
        for row in rows.iter() {
            let row_id = row.get_row_id()?.expect("row_id is not null");
            location_map.insert(row_id, format!("parquet:{}", relative_path));
        }

        let arrow_schema = Arc::new(schema_to_arrow_schema(self.table_schema.as_ref())?);
        let record_batch = rows_to_arrow_record(self.table_schema.as_ref(), &rows)?;
        let output_file = self.storage.create_file(relative_path).await?;
        let mut arrow_writer = AsyncArrowWriter::try_new(output_file, arrow_schema, None)?;
        arrow_writer.write(&record_batch).await?;
        let file_size_bytes = arrow_writer.bytes_written() as i64;
        arrow_writer.close().await?;

        // TODO locations
        Ok((location_map, file_size_bytes))
    }
}
