use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use backon::{ConstantBuilder, Retryable};
use futures::{StreamExt, TryStreamExt};
use log::{debug, error};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

use crate::{
    ILError, ILResult,
    catalog::{
        Catalog, CatalogHelper, CatalogSchema, DataFileRecord, IndexFileRecord, RowStream,
        RowsValidity, TransactionHelper, rows_to_record_batch,
    },
    index::{IndexBuilder, IndexDefinationRef, IndexKind},
    retry,
    storage::Storage,
    table::{Table, TableConfig},
};

pub(crate) async fn try_run_dump_task(table: &Table) -> ILResult<()> {
    let namespace_id = table.namespace_id;
    let table_id = table.table_id;
    let table_schema = table.schema.clone();
    let table_indexes = table.indexes.clone();
    let index_kinds = table.index_kinds.clone();
    let table_config = table.config.clone();
    let catalog = table.catalog.clone();
    let storage = table.storage.clone();
    tokio::spawn(async move {
        let result = async {
            let catalog_helper = CatalogHelper::new(catalog.clone());
            let inline_row_count = catalog_helper.count_inline_rows(table_id).await?;
            if inline_row_count < table_config.inline_row_count_limit as i64 {
                return Ok(());
            }
            if catalog_helper.dump_task_exists(table_id).await? {
                return Ok(());
            }

            let inline_row_count_limit = table_config.inline_row_count_limit;

            let dump_task = DumpTask {
                namespace_id,
                table_id,
                table_schema,
                table_indexes,
                index_kinds,
                table_config,
                catalog,
                storage,
            };

            let now = Instant::now();
            dump_task.run().await?;
            debug!(
                "Dump table {table_id} {inline_row_count_limit} inline rows in {} ms",
                now.elapsed().as_millis()
            );

            Ok::<(), ILError>(())
        }
        .await;
        if let Err(e) = result {
            error!("Failed to run dump task: {:?}", e);
        }
    });
    Ok(())
}

pub(crate) struct DumpTask {
    namespace_id: i64,
    table_id: i64,
    table_schema: SchemaRef,
    table_indexes: HashMap<String, IndexDefinationRef>,
    index_kinds: HashMap<String, Arc<dyn IndexKind>>,
    table_config: Arc<TableConfig>,
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

        let inline_row_count = tx_helper.count_inline_rows(self.table_id).await?;
        if inline_row_count < self.table_config.inline_row_count_limit as i64 {
            return Ok(());
        }

        let catalog_schema = Arc::new(CatalogSchema::from_arrow(&self.table_schema)?);
        let row_stream = tx_helper
            .scan_inline_rows(
                self.table_id,
                &catalog_schema,
                &[],
                Some(self.table_config.inline_row_count_limit),
            )
            .await?;

        let relative_path = DataFileRecord::build_relative_path(self.namespace_id, self.table_id);

        let mut index_builders = HashMap::new();
        for (index_name, index_def) in self.table_indexes.iter() {
            let index_kind = self.index_kinds.get(&index_def.kind).ok_or_else(|| {
                ILError::InternalError(format!("Index kind {} not found", index_def.kind))
            })?;
            let index_builder = index_kind.builder(index_def)?;
            index_builders.insert(index_name.clone(), index_builder);
        }

        let (row_ids, file_size_bytes) = self
            .write_dump_file(row_stream, &relative_path, &mut index_builders)
            .await?;

        if row_ids.len() != self.table_config.inline_row_count_limit {
            self.storage.delete(&relative_path).await?;
            return Err(ILError::InternalError(format!(
                "Read row count mismatch: {} rows read, expected {}",
                row_ids.len(),
                self.table_config.inline_row_count_limit
            )));
        }

        let mut insert_data_file_fn = async || {
            let data_file_id = tx_helper.get_max_data_file_id().await? + 1;
            tx_helper
                .insert_data_files(&[DataFileRecord {
                    data_file_id,
                    table_id: self.table_id,
                    relative_path: relative_path.clone(),
                    file_size_bytes: file_size_bytes as i64,
                    record_count: row_ids.len() as i64,
                    validity: RowsValidity {
                        validity: row_ids.iter().map(|id| (*id, true)).collect::<Vec<_>>(),
                    },
                }])
                .await?;
            Ok::<_, ILError>(data_file_id)
        };

        let data_file_id = retry!(insert_data_file_fn)?;

        let mut index_file_id = tx_helper.get_max_index_file_id().await? + 1;
        let mut index_file_records = Vec::new();
        for (index_name, index_builder) in index_builders.iter_mut() {
            let index_def = self
                .table_indexes
                .get(index_name)
                .ok_or_else(|| ILError::InternalError(format!("Index {index_name} not found")))?;
            let relative_path = IndexFileRecord::build_relative_path(
                self.namespace_id,
                self.table_id,
                data_file_id,
                index_def.index_id,
                index_file_id,
            );
            let output_file = self.storage.create_file(&relative_path).await?;
            index_builder.write_file(output_file).await?;
            index_file_records.push(IndexFileRecord {
                index_file_id,
                table_id: self.table_id,
                index_id: index_def.index_id,
                data_file_id,
                relative_path,
            });
            index_file_id += 1;
        }

        tx_helper.insert_index_files(&index_file_records).await?;

        let deleted_count = tx_helper
            .delete_inline_rows_by_row_ids(self.table_id, &row_ids)
            .await?;
        if deleted_count != row_ids.len() {
            return Err(ILError::InternalError(format!(
                "Delete row count mismatch: {} inline rows deleted, expected {}",
                deleted_count,
                row_ids.len()
            )));
        }

        tx_helper.delete_dump_task(self.table_id).await?;

        tx_helper.commit().await?;

        Ok(())
    }

    async fn write_dump_file(
        &self,
        row_stream: RowStream<'_>,
        relative_path: &str,
        index_builders: &mut HashMap<String, Box<dyn IndexBuilder>>,
    ) -> ILResult<(Vec<i64>, usize)> {
        let mut row_ids = Vec::new();

        let writer_properties = WriterProperties::builder()
            .set_max_row_group_size(self.table_config.parquet_row_group_size)
            .build();
        let output_file = self.storage.create_file(relative_path).await?;
        let mut arrow_writer = AsyncArrowWriter::try_new(
            output_file,
            self.table_schema.clone(),
            Some(writer_properties),
        )?;

        let mut chunk_stream = row_stream.chunks(self.table_config.parquet_row_group_size);

        while let Some(row_chunk) = chunk_stream.next().await {
            let mut rows = Vec::with_capacity(row_chunk.len());
            for row in row_chunk.into_iter() {
                let row = row?;
                let row_id = row.get_row_id()?.expect("row_id is not null");
                row_ids.push(row_id);
                rows.push(row);
            }
            let record_batch = rows_to_record_batch(&self.table_schema, &rows)?;

            for (_index_name, index_builder) in index_builders.iter_mut() {
                index_builder.append(&record_batch)?;
            }

            arrow_writer.write(&record_batch).await?;
        }

        let file_size_bytes = arrow_writer.bytes_written();
        arrow_writer.close().await?;

        Ok((row_ids, file_size_bytes))
    }
}
