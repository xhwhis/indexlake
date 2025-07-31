use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use log::{debug, error};
use parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};
use uuid::Uuid;

use crate::{
    ILError, ILResult,
    catalog::{
        Catalog, CatalogHelper, CatalogSchema, DataFileRecord, IndexFileRecord, RowStream,
        TransactionHelper, rows_to_record_batch,
    },
    index::{IndexBuilder, IndexDefinationRef, IndexKind},
    storage::{Storage, build_parquet_writer},
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
        let try_dump_fn = async || {
            let catalog_helper = CatalogHelper::new(catalog.clone());
            let inline_row_count = catalog_helper.count_inline_rows(&table_id).await?;
            if inline_row_count < table_config.inline_row_count_limit as i64 {
                return Ok(false);
            }
            if catalog_helper.dump_task_exists(&table_id).await? {
                return Ok(false);
            }

            let dump_task = DumpTask {
                namespace_id,
                table_id,
                table_schema: table_schema.clone(),
                table_indexes: table_indexes.clone(),
                index_kinds: index_kinds.clone(),
                table_config: table_config.clone(),
                catalog: catalog.clone(),
                storage: storage.clone(),
            };

            let continue_dump = dump_task.run().await?;

            Ok::<_, ILError>(continue_dump)
        };

        loop {
            match try_dump_fn().await {
                Ok(continue_dump) => {
                    if !continue_dump {
                        return;
                    }
                }
                Err(e) => {
                    error!("[indexlake] failed to run dump task: {e:?}");
                    return;
                }
            }
        }
    });
    Ok(())
}

pub(crate) struct DumpTask {
    namespace_id: Uuid,
    table_id: Uuid,
    table_schema: SchemaRef,
    table_indexes: HashMap<String, IndexDefinationRef>,
    index_kinds: HashMap<String, Arc<dyn IndexKind>>,
    table_config: Arc<TableConfig>,
    catalog: Arc<dyn Catalog>,
    storage: Arc<Storage>,
}

impl DumpTask {
    async fn run(&self) -> ILResult<bool> {
        let now = Instant::now();

        let mut tx_helper = TransactionHelper::new(&self.catalog).await?;
        if tx_helper.insert_dump_task(&self.table_id).await.is_err() {
            debug!("Table {} already has a dump task", self.table_id);
            return Ok(false);
        }

        let inline_row_count = tx_helper.count_inline_rows(&self.table_id).await?;
        if inline_row_count < self.table_config.inline_row_count_limit as i64 {
            return Ok(false);
        }

        let catalog_schema = Arc::new(CatalogSchema::from_arrow(&self.table_schema)?);
        let row_stream = tx_helper
            .scan_inline_rows(
                &self.table_id,
                &catalog_schema,
                &[],
                Some(self.table_config.inline_row_count_limit),
            )
            .await?;

        let data_file_id = uuid::Uuid::now_v7();
        let relative_path = DataFileRecord::build_relative_path(
            &self.namespace_id,
            &self.table_id,
            &data_file_id,
            self.table_config.preferred_data_file_format,
        );

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

        let mut index_file_records = Vec::new();
        for (index_name, index_builder) in index_builders.iter_mut() {
            let index_def = self
                .table_indexes
                .get(index_name)
                .ok_or_else(|| ILError::InternalError(format!("Index {index_name} not found")))?;

            let index_file_id = uuid::Uuid::now_v7();
            let relative_path = IndexFileRecord::build_relative_path(
                &self.namespace_id,
                &self.table_id,
                &index_file_id,
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
        }

        tx_helper
            .insert_data_files(&[DataFileRecord {
                data_file_id,
                table_id: self.table_id,
                format: self.table_config.preferred_data_file_format,
                relative_path: relative_path.clone(),
                file_size_bytes: file_size_bytes as i64,
                record_count: row_ids.len() as i64,
                row_ids: row_ids.clone(),
                validity: vec![true; row_ids.len()],
            }])
            .await?;

        tx_helper.insert_index_files(&index_file_records).await?;

        let deleted_count = tx_helper
            .delete_inline_rows_by_row_ids(&self.table_id, &row_ids)
            .await?;
        if deleted_count != row_ids.len() {
            return Err(ILError::InternalError(format!(
                "Delete row count mismatch: {} inline rows deleted, expected {}",
                deleted_count,
                row_ids.len()
            )));
        }

        tx_helper.delete_dump_task(&self.table_id).await?;

        tx_helper.commit().await?;

        debug!(
            "[indexlake] dump table {} {} inline rows in {} ms",
            self.table_id,
            self.table_config.inline_row_count_limit,
            now.elapsed().as_millis()
        );

        Ok(true)
    }

    async fn write_dump_file(
        &self,
        row_stream: RowStream<'_>,
        relative_path: &str,
        index_builders: &mut HashMap<String, Box<dyn IndexBuilder>>,
    ) -> ILResult<(Vec<i64>, usize)> {
        let mut row_ids = Vec::new();

        let output_file = self.storage.create_file(relative_path).await?;
        let mut arrow_writer = build_parquet_writer(
            output_file,
            self.table_schema.clone(),
            self.table_config.parquet_row_group_size,
            self.table_config.preferred_data_file_format,
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
