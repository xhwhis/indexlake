use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::{
    ILError, ILResult,
    arrow::record_batch_to_rows,
    catalog::{DataFileRecord, RowStream, TransactionHelper},
    record::{Row, SchemaRef},
    storage::{self, Storage},
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    storage: Arc<Storage>,
) -> ILResult<RowStream<'static>> {
    let schema = Arc::new(table_schema.without_row_id());

    // Inline rows are not deleted, so we can scan them directly
    let mut rows = tx_helper.scan_inline_rows(table_id, &schema).await?;

    let data_files = tx_helper.get_data_files(table_id).await?;
    for data_file in data_files {
        rows.extend(read_data_file(&data_file, &schema, &storage).await?);
    }

    Ok(Box::pin(futures::stream::iter(rows).map(Ok)))
}

pub(crate) async fn read_data_file(
    data_file: &DataFileRecord,
    schema: &SchemaRef,
    storage: &Storage,
) -> ILResult<Vec<Row>> {
    let input_file = storage.open_file(&data_file.relative_path).await?;
    let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
    let stream = arrow_reader_builder.build().unwrap();
    let batches = stream.try_collect::<Vec<_>>().await?;
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(record_batch_to_rows(&batch, schema.clone())?);
    }
    Ok(rows)
}
