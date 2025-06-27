use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{CatalogSchema, DataFileRecord, TransactionHelper, rows_to_record_batch},
    storage::Storage,
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    storage: Arc<Storage>,
) -> ILResult<RecordBatchStream> {
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&table_schema)?);

    // Inline rows are not deleted, so we can scan them directly
    let rows = tx_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let batch = rows_to_record_batch(&table_schema, &rows)?;

    let mut batches = vec![batch];

    // TODO change to real stream
    let data_files = tx_helper.get_data_files(table_id).await?;
    for data_file in data_files {
        batches.extend(read_data_file(&data_file, &storage).await?);
    }

    Ok(Box::pin(futures::stream::iter(batches).map(Ok)))
}

pub(crate) async fn read_data_file(
    data_file: &DataFileRecord,
    storage: &Storage,
) -> ILResult<Vec<RecordBatch>> {
    let input_file = storage.open_file(&data_file.relative_path).await?;
    let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
    let stream = arrow_reader_builder.build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}
