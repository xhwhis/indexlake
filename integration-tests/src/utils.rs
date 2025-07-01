use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions},
    util::pretty::pretty_format_batches,
};
use futures::TryStreamExt;
use indexlake::{ILError, ILResult, catalog::INTERNAL_ROW_ID_FIELD_NAME, table::Table};

pub fn sort_record_batches(batches: &[RecordBatch], sort_col: &str) -> ILResult<RecordBatch> {
    if batches.is_empty() {
        return Err(ILError::InvalidInput("record batches is empty".to_string()));
    }
    let record = arrow::compute::concat_batches(&batches[0].schema(), batches)?;

    let sort_col_idx = record.schema().index_of(sort_col)?;
    let sort_array = record.column(sort_col_idx);

    let indices = arrow::compute::sort_to_indices(sort_array, None, None)?;

    let sorted_arrays: Vec<ArrayRef> = record
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col, &indices, None))
        .collect::<arrow::error::Result<_>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(record.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        record.schema(),
        sorted_arrays,
        &options,
    )?)
}

pub async fn table_scan(table: &Table) -> ILResult<String> {
    let stream = table.scan().await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let sorted_batch = sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME)?;
    let table_str = pretty_format_batches(&[sorted_batch])?.to_string();
    Ok(table_str)
}
