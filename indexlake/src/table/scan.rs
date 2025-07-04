use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{CatalogHelper, CatalogSchema, RowLocation, rows_to_record_batch},
    expr::{Expr, merge_filters, split_conjunction_filters},
    index::{Index, IndexDefinationRef},
    storage::{Storage, read_parquet_files_by_locations},
    table::Table,
    utils::project_schema,
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

pub(crate) async fn process_scan(
    catalog_helper: &CatalogHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    scan: TableScan,
    storage: Arc<Storage>,
    table: &Table,
) -> ILResult<RecordBatchStream> {
    let filters = split_conjunction_filters(scan.filters.clone());

    let index_filter_assignment =
        assign_index_filters(&table.indexes, &table.index_kinds, &filters)?;

    if index_filter_assignment
        .values()
        .any(|filters| filters.len() > 0)
    {
        process_index_scan(
            catalog_helper,
            table,
            scan.projection,
            &filters,
            index_filter_assignment,
        )
        .await
    } else {
        process_table_scan(
            catalog_helper,
            &storage,
            table_id,
            table_schema,
            scan.projection,
            filters,
            scan.limit,
        )
        .await
    }
}

async fn process_table_scan(
    catalog_helper: &CatalogHelper,
    storage: &Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
) -> ILResult<RecordBatchStream> {
    // Scan inline rows
    let projected_schema = Arc::new(project_schema(table_schema, projection.as_ref())?);
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);
    let rows = catalog_helper
        .scan_inline_rows(table_id, &catalog_schema, &filters, limit)
        .await?;
    let inline_row_count = rows.len();
    let batch = rows_to_record_batch(&projected_schema, &rows)?;
    let batch_stream =
        Box::pin(futures::stream::once(futures::future::ready(Ok(batch)))) as RecordBatchStream;
    if let Some(limit) = limit
        && inline_row_count == limit
    {
        return Ok(batch_stream);
    }

    let left_limit = limit.map(|l| l - inline_row_count);

    // Scan data files
    let row_metadatas = catalog_helper
        .scan_undeleted_non_inline_row_metadata(table_id, left_limit)
        .await?;
    let data_file_locations = row_metadatas
        .into_iter()
        .filter(|meta| matches!(meta.location, RowLocation::Parquet { .. }))
        .map(|meta| meta.location)
        .collect::<Vec<_>>();
    let stream = read_parquet_files_by_locations(
        storage.clone(),
        table_schema.clone(),
        projection.clone(),
        data_file_locations,
        merge_filters(filters),
    )
    .await?;

    Ok(Box::pin(futures::stream::select_all(vec![
        batch_stream,
        stream,
    ])))
}

async fn process_index_scan(
    catalog_helper: &CatalogHelper,
    table: &Table,
    projection: Option<Vec<usize>>,
    filters: &[Expr],
    index_filter_assignment: HashMap<String, Vec<usize>>,
) -> ILResult<RecordBatchStream> {
    todo!()
}

fn assign_index_filters(
    indexes: &HashMap<String, IndexDefinationRef>,
    index_kinds: &HashMap<String, Arc<dyn Index>>,
    filters: &[Expr],
) -> ILResult<HashMap<String, Vec<usize>>> {
    let mut index_filter_assignment: HashMap<String, Vec<usize>> = HashMap::new();
    for (filter_idx, filter) in filters.iter().enumerate() {
        for (index_name, index_def) in indexes.iter() {
            let index_kind = &index_def.kind;
            let index = index_kinds.get(index_kind).ok_or_else(|| {
                ILError::InternalError(format!("Index kind {index_kind} not found"))
            })?;
            let supported = index.supports_filter(index_def, &filter)?;
            if supported {
                index_filter_assignment
                    .entry(index_name.clone())
                    .or_default()
                    .push(filter_idx);
            }
        }
    }
    Ok(index_filter_assignment)
}
