use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use log::{debug, error};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::{
    ILError, ILResult, RowStream, TransactionHelper,
    arrow::{rows_to_arrow_record, schema_to_arrow_schema},
    catalog::Catalog,
    record::{Row, SchemaRef},
    storage::Storage,
    table::Table,
};

pub(crate) async fn spawn_dump_task(table: &Table) -> ILResult<()> {
    let mut tx_helper = TransactionHelper::new(&table.catalog).await?;
    let dump_row_ids = tx_helper
        .scan_inline_row_ids_with_limit(table.table_id, 3000)
        .await?;
    tx_helper.commit().await?;
    if dump_row_ids.len() < 3000 {
        debug!(
            "Table {} has less than 3000 inline rows, skip dump",
            table.table_id
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
        if let Err(e) = dump_task.run().await {
            error!("Failed to dump table: {:?}", e);
        }
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

        let max_data_file_id = tx_helper.get_max_data_file_id().await?;

        // TODO schema with row id
        let row_stream = tx_helper
            .scan_inline_rows_by_row_ids(self.table_id, &self.table_schema, &self.dump_row_ids)
            .await?;

        let mut row_id_to_location_map = HashMap::new();
        let mut data_file_id_to_file_path_map = HashMap::new();
        let mut data_file_id = max_data_file_id + 1;

        let mut chunks = row_stream.chunks(1000);
        while let Some(chunk) = chunks.next().await {
            let mut rows = Vec::new();
            for row in chunk {
                let row = row?;
                rows.push(row);
            }

            let file_path = format!(
                "{}/{}/{}.parquet",
                self.namespace_id,
                self.table_id,
                uuid::Uuid::new_v4()
            );
            let location_map = self.write_dump_file(rows, &file_path).await?;
            data_file_id_to_file_path_map.insert(data_file_id, file_path);
            row_id_to_location_map.extend(location_map);

            data_file_id += 1;
        }
        drop(chunks);

        if row_id_to_location_map.len() != self.dump_row_ids.len() {
            return Err(ILError::InternalError(format!(
                "Read row count mismatch: {} rows read, expected {}",
                row_id_to_location_map.len(),
                self.dump_row_ids.len()
            )));
        }

        for (data_file_id, file_path) in data_file_id_to_file_path_map {
            // TODO file size and record count
            // TODO batch insert
            tx_helper
                .insert_data_file(data_file_id, self.table_id, &file_path, 0, 0)
                .await?;
        }

        tx_helper
            .update_row_locations(self.table_id, &row_id_to_location_map)
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
        file_path: &str,
    ) -> ILResult<HashMap<i64, String>> {
        let arrow_schema = Arc::new(schema_to_arrow_schema(self.table_schema.as_ref())?);
        let record_batch = rows_to_arrow_record(self.table_schema.as_ref(), &rows)?;
        // TODO Storage supports relative path
        let absolute_file_path = format!("s3://{}", file_path);
        let storage_file = self.storage.new_storage_file(absolute_file_path).await?;
        let mut arrow_writer = AsyncArrowWriter::try_new(storage_file, arrow_schema, None)?;
        arrow_writer.write(&record_batch).await?;
        // TODO locations
        Ok(HashMap::new())
    }
}
