use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{Int32Array, Int64Array},
    datatypes::{Schema, SchemaRef},
};
use futures::StreamExt;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{
        CatalogHelper, CatalogSchema, DataFileRecord, IndexFileRecord, rows_to_record_batch,
    },
    expr::{Expr, merge_filters, split_conjunction_filters},
    index::{IndexDefinationRef, IndexKind},
    storage::{Storage, read_parquet_file_by_record},
    table::Table,
    utils::project_schema,
};

#[derive(Debug, Clone, derive_with::With)]
pub struct TableScan {
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
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
) -> ILResult<RecordBatchStream> {
    // Scan inline rows
    let projected_schema = Arc::new(project_schema(table_schema, projection.as_ref())?);
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);
    // TODO change this to stream
    let row_stream = catalog_helper
        .scan_inline_rows(table_id, &catalog_schema, &filters)
        .await?;
    let inline_stream = Box::pin(row_stream.chunks(1024).map(move |rows| {
        let rows = rows.into_iter().collect::<ILResult<Vec<_>>>()?;
        let batch = rows_to_record_batch(&projected_schema, &rows)?;
        Ok(batch)
    }));

    let merged_filter = merge_filters(filters);

    // Scan data files
    // TODO parallel scan data files
    let mut streams: Vec<RecordBatchStream> = vec![inline_stream];
    let data_file_records = catalog_helper.get_data_files(table_id).await?;
    for data_file_record in data_file_records {
        streams.push(
            read_parquet_file_by_record(
                storage,
                &table_schema,
                &data_file_record,
                projection.clone(),
                merged_filter.clone(),
                None,
            )
            .await?,
        );
    }

    Ok(Box::pin(futures::stream::select_all(streams)))
}

async fn process_index_scan(
    catalog_helper: &CatalogHelper,
    table: &Table,
    projection: Option<Vec<usize>>,
    filters: &[Expr],
    index_filter_assignment: HashMap<String, Vec<usize>>,
) -> ILResult<RecordBatchStream> {
    let non_index_filters = filters
        .iter()
        .enumerate()
        .filter(|(idx, _)| {
            !index_filter_assignment
                .values()
                .any(|indices| indices.contains(idx))
        })
        .map(|(_, filter)| filter.clone())
        .collect::<Vec<_>>();

    // Scan inline rows
    let inline_rows_stream = index_scan_inline_rows(
        catalog_helper,
        table,
        projection.clone(),
        filters,
        &index_filter_assignment,
        &non_index_filters,
    )
    .await?;

    let data_file_records = catalog_helper.get_data_files(table.table_id).await?;
    let mut streams = vec![inline_rows_stream];
    for data_file_record in data_file_records {
        let stream = index_scan_data_file(
            catalog_helper,
            table,
            projection.clone(),
            filters,
            &data_file_record,
            &index_filter_assignment,
        )
        .await?;
        streams.push(stream);
    }
    Ok(Box::pin(futures::stream::select_all(streams)))
}

// TODO save inline data index to catalog
async fn index_scan_inline_rows(
    catalog_helper: &CatalogHelper,
    table: &Table,
    projection: Option<Vec<usize>>,
    filters: &[Expr],
    index_filter_assignment: &HashMap<String, Vec<usize>>,
    non_index_filters: &[Expr],
) -> ILResult<RecordBatchStream> {
    let projected_schema = Arc::new(project_schema(&table.schema, projection.as_ref())?);
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);
    let row_stream = catalog_helper
        .scan_inline_rows(table.table_id, &catalog_schema, &non_index_filters)
        .await?;
    let mut inline_stream = row_stream.chunks(1024).map(move |rows| {
        let rows = rows.into_iter().collect::<ILResult<Vec<_>>>()?;
        let batch = rows_to_record_batch(&projected_schema, &rows)?;
        Ok::<_, ILError>(batch)
    });

    let mut index_builder_map = HashMap::new();
    for (index_name, _) in index_filter_assignment.iter() {
        let index_def = table
            .indexes
            .get(index_name)
            .ok_or_else(|| ILError::InternalError(format!("Index {index_name} not found")))?;
        let kind = &index_def.kind;
        let index_kind = table
            .index_kinds
            .get(kind)
            .ok_or_else(|| ILError::InternalError(format!("Index kind {kind} not found")))?;

        let index_builder = index_kind.builder(index_def)?;
        index_builder_map.insert(index_name, index_builder);
    }

    let mut inline_batches = Vec::new();
    while let Some(batch) = inline_stream.next().await {
        let batch = batch?;
        for (_, index_builder) in index_builder_map.iter_mut() {
            index_builder.append(&batch)?;
        }
        inline_batches.push(batch);
    }

    let mut filter_index_entries_list = Vec::new();
    for (index_name, filter_indices) in index_filter_assignment.iter() {
        let index_builder = index_builder_map.get_mut(index_name).ok_or_else(|| {
            ILError::InternalError(format!("Index builder not found for index {index_name}"))
        })?;

        let index = index_builder.build()?;

        let filters = filter_indices
            .iter()
            .map(|idx| filters[*idx].clone())
            .collect::<Vec<_>>();

        let filter_index_entries = index.filter(&filters).await?;
        filter_index_entries_list.push(filter_index_entries);
    }

    let mut intersected_row_ids = filter_index_entries_list[0]
        .row_ids
        .values()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for filter_index_entries in filter_index_entries_list.iter().skip(1) {
        let set = filter_index_entries
            .row_ids
            .values()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        intersected_row_ids = intersected_row_ids.intersection(&set).cloned().collect();
    }

    let mut selected_batches = Vec::new();
    for batch in inline_batches {
        let mut indicies = Vec::new();
        for i in 0..batch.num_rows() {
            let row_id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Row id array can not be downcasted to Int64Array"
                    ))
                })?;
            let row_id = row_id_array.value(i);
            if intersected_row_ids.contains(&row_id) {
                indicies.push(i as i32);
            }
        }
        let selected_batch =
            arrow::compute::take_record_batch(&batch, &Int32Array::from(indicies))?;
        selected_batches.push(selected_batch);
    }

    Ok(Box::pin(futures::stream::iter(
        selected_batches.into_iter().map(Ok),
    )))
}

async fn index_scan_data_file(
    catalog_helper: &CatalogHelper,
    table: &Table,
    projection: Option<Vec<usize>>,
    filters: &[Expr],
    data_file_record: &DataFileRecord,
    index_filter_assignment: &HashMap<String, Vec<usize>>,
) -> ILResult<RecordBatchStream> {
    let index_file_records = catalog_helper
        .get_index_files_by_data_file_id(data_file_record.data_file_id)
        .await?;
    let index_file_records_map = index_file_records
        .iter()
        .map(|record| (record.index_id, record))
        .collect::<HashMap<_, _>>();
    let row_ids = filter_index_files_row_ids(
        table,
        filters,
        &index_file_records_map,
        &index_filter_assignment,
    )
    .await?;

    let left_filters = filters
        .iter()
        .enumerate()
        .filter(|(idx, _)| {
            !index_filter_assignment
                .values()
                .any(|indices| indices.contains(idx))
        })
        .map(|(_, filter)| filter.clone())
        .collect::<Vec<_>>();
    let merged_filter = merge_filters(left_filters);

    read_parquet_file_by_record(
        &table.storage,
        &table.schema,
        &data_file_record,
        projection,
        merged_filter,
        Some(&row_ids),
    )
    .await
}

async fn filter_index_files_row_ids(
    table: &Table,
    filters: &[Expr],
    index_file_records: &HashMap<i64, &IndexFileRecord>,
    index_filter_assignment: &HashMap<String, Vec<usize>>,
) -> ILResult<HashSet<i64>> {
    let mut filter_index_entries_list = Vec::new();
    for (index_name, filter_indices) in index_filter_assignment.iter() {
        let index_def = table
            .indexes
            .get(index_name)
            .ok_or_else(|| ILError::InternalError(format!("Index {index_name} not found")))?;
        let kind = &index_def.kind;
        let index_kind = table
            .index_kinds
            .get(kind)
            .ok_or_else(|| ILError::InternalError(format!("Index kind {kind} not found")))?;

        let index_file_record = index_file_records.get(&index_def.index_id).ok_or_else(|| {
            ILError::InternalError(format!(
                "Index file record not found for index {index_name}"
            ))
        })?;

        let filters = filter_indices
            .iter()
            .map(|idx| filters[*idx].clone())
            .collect::<Vec<_>>();

        let input_file = table
            .storage
            .open_file(&index_file_record.relative_path)
            .await?;

        let mut index_builder = index_kind.builder(index_def)?;
        index_builder.read_file(input_file).await?;

        let index = index_builder.build()?;

        let filter_index_entries = index.filter(&filters).await?;
        filter_index_entries_list.push(filter_index_entries);
    }

    let mut intersected_row_ids = filter_index_entries_list[0]
        .row_ids
        .values()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for filter_index_entries in filter_index_entries_list.iter().skip(1) {
        let set = filter_index_entries
            .row_ids
            .values()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        intersected_row_ids = intersected_row_ids.intersection(&set).cloned().collect();
    }

    Ok(intersected_row_ids)
}

fn assign_index_filters(
    indexes: &HashMap<String, IndexDefinationRef>,
    index_kinds: &HashMap<String, Arc<dyn IndexKind>>,
    filters: &[Expr],
) -> ILResult<HashMap<String, Vec<usize>>> {
    let mut index_filter_assignment: HashMap<String, Vec<usize>> = HashMap::new();
    for (filter_idx, filter) in filters.iter().enumerate() {
        for (index_name, index_def) in indexes.iter() {
            let kind = &index_def.kind;
            let index_kind = index_kinds
                .get(kind)
                .ok_or_else(|| ILError::InternalError(format!("Index kind {kind} not found")))?;
            let supported = index_kind.supports_filter(index_def, &filter)?;
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
