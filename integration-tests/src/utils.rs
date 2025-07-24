use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions},
    util::pretty::pretty_format_batches_with_schema,
};
use futures::TryStreamExt;
use indexlake::{
    ILError, ILResult,
    catalog::INTERNAL_ROW_ID_FIELD_NAME,
    table::{Table, TableScan, TableSearch},
    utils::project_schema,
};

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

pub async fn full_table_scan(table: &Table) -> ILResult<String> {
    table_scan(table, TableScan::default()).await
}

pub async fn table_scan(table: &Table, scan: TableScan) -> ILResult<String> {
    let batch_schema = Arc::new(project_schema(&table.schema, scan.projection.as_ref())?);
    let stream = table.scan(scan).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let sorted_batches = if batches.is_empty() {
        vec![]
    } else {
        vec![sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME)?]
    };
    let table_str = pretty_format_batches_with_schema(batch_schema, &sorted_batches)?.to_string();
    Ok(table_str)
}

pub async fn table_search(table: &Table, search: TableSearch) -> ILResult<String> {
    let batch_schema = Arc::new(project_schema(&table.schema, search.projection.as_ref())?);
    let stream = table.search(search).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let table_str = pretty_format_batches_with_schema(batch_schema, &batches)?.to_string();
    Ok(table_str)
}
