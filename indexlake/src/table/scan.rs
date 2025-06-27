use std::sync::Arc;

use arrow::array::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::{
    ILError, ILResult,
    arrow::{
        RecordBatchStream, record_batch_to_rows, record_batch_without_column, rows_to_arrow_record,
    },
    catalog::{DataFileRecord, RowStream, TransactionHelper},
    record::{INTERNAL_ROW_ID_FIELD_NAME, Row, SchemaRef},
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

    // TODO change to real stream
    let data_files = tx_helper.get_data_files(table_id).await?;
    for data_file in data_files {
        let batches = read_data_file(&data_file, &storage).await?;
        for batch in batches {
            rows.extend(record_batch_to_rows(&batch, schema.clone())?);
        }
    }

    Ok(Box::pin(futures::stream::iter(rows).map(Ok)))
}

pub(crate) async fn process_table_scan_arrow(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    storage: Arc<Storage>,
) -> ILResult<RecordBatchStream> {
    let schema = Arc::new(table_schema.without_row_id());

    // Inline rows are not deleted, so we can scan them directly
    let rows = tx_helper.scan_inline_rows(table_id, &schema).await?;
    let batch = rows_to_arrow_record(&schema, &rows)?;

    let mut batches = vec![batch];

    // TODO change to real stream
    let data_files = tx_helper.get_data_files(table_id).await?;
    for data_file in data_files {
        let file_batches = read_data_file(&data_file, &storage).await?;
        for batch in file_batches {
            batches.push(record_batch_without_column(
                &batch,
                INTERNAL_ROW_ID_FIELD_NAME,
            )?);
        }
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
