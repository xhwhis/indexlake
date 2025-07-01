use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use futures::TryStreamExt;
use parquet::arrow::{
    ParquetRecordBatchStreamBuilder,
    arrow_reader::{RowFilter, RowSelection},
};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{CatalogHelper, CatalogSchema, RowLocation, rows_to_record_batch},
    expr::{Expr, ExprPredicate},
    storage::{Storage, read_parquet_files_by_locations},
};

pub(crate) async fn process_table_scan(
    catalog_helper: &CatalogHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    storage: Arc<Storage>,
) -> ILResult<RecordBatchStream> {
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&table_schema)?);

    // Inline rows are not deleted, so we can scan them directly
    let rows = catalog_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let batch = rows_to_record_batch(&table_schema, &rows)?;
    let batch_stream =
        Box::pin(futures::stream::once(futures::future::ready(Ok(batch)))) as RecordBatchStream;

    let row_metadatas = catalog_helper
        .scan_all_undeleted_row_metadata(table_id)
        .await?;
    let data_file_locations = row_metadatas
        .into_iter()
        .filter(|meta| matches!(meta.location, RowLocation::Parquet { .. }))
        .map(|meta| meta.location)
        .collect::<Vec<_>>();

    let stream =
        read_parquet_files_by_locations(storage, table_schema.clone(), data_file_locations, None)
            .await?;

    Ok(Box::pin(futures::stream::select_all(vec![
        batch_stream,
        stream,
    ])))
}
