use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{CatalogHelper, CatalogSchema, RowLocation, rows_to_record_batch},
    expr::Expr,
    storage::{Storage, read_parquet_files_by_locations},
};

#[derive(Debug, Clone, derive_with::With)]
pub struct TableScan {
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

impl TableScan {
    pub fn projected_schema(&self, table_schema: &SchemaRef) -> ILResult<SchemaRef> {
        if let Some(projection) = &self.projection {
            let projected_schema = table_schema.project(projection)?;
            Ok(Arc::new(projected_schema))
        } else {
            Ok(table_schema.clone())
        }
    }
}

impl Default for TableScan {
    fn default() -> Self {
        Self {
            projection: None,
            filters: vec![],
            limit: None,
        }
    }
}

pub(crate) async fn process_table_scan(
    catalog_helper: &CatalogHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    scan: TableScan,
    storage: Arc<Storage>,
) -> ILResult<RecordBatchStream> {
    let projected_schema = scan.projected_schema(table_schema)?;
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);

    // Inline rows are not deleted, so we can scan them directly
    let rows = catalog_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let batch = rows_to_record_batch(&projected_schema, &rows)?;
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

    let stream = read_parquet_files_by_locations(
        storage,
        table_schema.clone(),
        scan.projection.clone(),
        data_file_locations,
        None,
    )
    .await?;

    Ok(Box::pin(futures::stream::select_all(vec![
        batch_stream,
        stream,
    ])))
}
