use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{Float64Array, Int64Array, RecordBatch},
    compute::SortOptions,
};
use futures::{StreamExt, TryStreamExt};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{
        CatalogHelper, CatalogSchema, DataFileRecord, INTERNAL_ROW_ID_FIELD_NAME, IndexFileRecord,
        Row, rows_to_record_batch,
    },
    expr::col,
    index::{
        IndexDefination, IndexDefinationRef, IndexKind, RowIdScore, SearchIndexEntries, SearchQuery,
    },
    storage::read_parquet_file_by_record,
    table::Table,
    utils::project_schema,
};

pub struct TableSearch {
    pub query: Arc<dyn SearchQuery>,
    pub projection: Option<Vec<usize>>,
}

pub(crate) async fn process_search(
    table: &Table,
    search: TableSearch,
) -> ILResult<RecordBatchStream> {
    let index_kind = search.query.index_kind();
    let (_index_name, index_def) = table
        .indexes
        .iter()
        .find(|(_, index_def)| index_def.kind == index_kind)
        .ok_or(ILError::IndexError(format!(
            "Index kind {index_kind} not found"
        )))?;

    let index_kind = table
        .index_kinds
        .get(index_kind)
        .ok_or(ILError::IndexError(format!(
            "Index kind {index_kind} not found"
        )))?;

    let catalog_helper = CatalogHelper::new(table.catalog.clone());

    let inline_search_entries = search_inline_rows(
        &catalog_helper,
        table,
        index_kind.as_ref(),
        index_def,
        &search,
    )
    .await?;

    let data_file_records = catalog_helper.get_data_files(table.table_id).await?;

    let index_id = index_def.index_id;

    let mut index_file_search_entries = HashMap::new();
    for data_file_record in data_file_records.iter() {
        let data_file_id = data_file_record.data_file_id;
        let index_file_record = catalog_helper
            .get_index_file_by_index_id_and_data_file_id(index_id, data_file_id)
            .await?
            .ok_or(ILError::IndexError(format!(
                "Index file not found for index {index_id} and data file {data_file_id}"
            )))?;

        let search_entries = search_index_file(
            table,
            index_kind.as_ref(),
            index_def,
            &search,
            &index_file_record,
        )
        .await?;
        index_file_search_entries.insert(data_file_id, search_entries);
    }

    let score_higher_is_better = inline_search_entries.score_higher_is_better;

    let row_id_score_locations = merge_search_index_entries(
        inline_search_entries,
        index_file_search_entries,
        search.query.limit(),
    )?;

    let inline_batch = read_inline_rows(
        &catalog_helper,
        table,
        &row_id_score_locations,
        search.projection.clone(),
    )
    .await?;

    let data_file_batches = read_data_file_rows(
        table,
        &row_id_score_locations,
        search.projection.clone(),
        &data_file_records,
    )
    .await?;

    let sorted_batch = sort_batches(
        inline_batch,
        data_file_batches,
        &row_id_score_locations,
        score_higher_is_better,
    )?;

    Ok(Box::pin(futures::stream::iter(vec![Ok(sorted_batch)])))
}

async fn search_inline_rows(
    catalog_helper: &CatalogHelper,
    table: &Table,
    index_kind: &dyn IndexKind,
    index_def: &IndexDefinationRef,
    search: &TableSearch,
) -> ILResult<SearchIndexEntries> {
    let projected_schema = Arc::new(project_schema(&table.schema, search.projection.as_ref())?);
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);
    let row_stream = catalog_helper
        .scan_inline_rows(table.table_id, &catalog_schema, &[])
        .await?;
    let mut inline_stream = row_stream.chunks(1024).map(move |rows| {
        let rows = rows.into_iter().collect::<ILResult<Vec<_>>>()?;
        let batch = rows_to_record_batch(&projected_schema, &rows)?;
        Ok::<_, ILError>(batch)
    });

    let mut index_builder = index_kind.builder(index_def)?;

    while let Some(batch) = inline_stream.next().await {
        let batch = batch?;
        index_builder.append(&batch)?;
    }

    let index = index_builder.build()?;

    let search_index_entries = index.search(search.query.as_ref()).await?;

    Ok(search_index_entries)
}

async fn search_index_file(
    table: &Table,
    index_kind: &dyn IndexKind,
    index_def: &IndexDefinationRef,
    search: &TableSearch,
    index_file_record: &IndexFileRecord,
) -> ILResult<SearchIndexEntries> {
    let index_file = table
        .storage
        .open_file(&index_file_record.relative_path)
        .await?;

    let mut index_builder = index_kind.builder(index_def)?;
    index_builder.read_file(index_file).await?;

    let index = index_builder.build()?;

    let search_index_entries = index.search(search.query.as_ref()).await?;

    Ok(search_index_entries)
}

fn merge_search_index_entries(
    inline_search_entries: SearchIndexEntries,
    index_file_search_entries: HashMap<i64, SearchIndexEntries>,
    limit: Option<usize>,
) -> ILResult<Vec<(RowIdScore, RowLocation)>> {
    let mut row_id_score_locations = Vec::new();
    for row_id_score in inline_search_entries.row_id_scores.into_iter() {
        row_id_score_locations.push((row_id_score, RowLocation::Inline));
    }
    for (data_file_id, search_index_entries) in index_file_search_entries.into_iter() {
        for row_id_score in search_index_entries.row_id_scores.into_iter() {
            row_id_score_locations.push((row_id_score, RowLocation::DataFile(data_file_id)));
        }
    }

    if inline_search_entries.score_higher_is_better {
        row_id_score_locations.sort_by(|a, b| {
            b.0.score
                .partial_cmp(&a.0.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        row_id_score_locations.sort_by(|a, b| {
            a.0.score
                .partial_cmp(&b.0.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    if let Some(limit) = limit {
        row_id_score_locations.truncate(limit);
    }

    Ok(row_id_score_locations)
}

#[derive(Debug)]
enum RowLocation {
    Inline,
    DataFile(i64),
}

async fn read_inline_rows(
    catalog_helper: &CatalogHelper,
    table: &Table,
    row_id_score_locations: &[(RowIdScore, RowLocation)],
    projection: Option<Vec<usize>>,
) -> ILResult<RecordBatch> {
    let inline_row_ids = row_id_score_locations
        .iter()
        .filter(|(_, location)| matches!(location, RowLocation::Inline))
        .map(|(row_id_score, _)| row_id_score.row_id)
        .collect::<Vec<_>>();

    let projected_schema = Arc::new(project_schema(&table.schema, projection.as_ref())?);
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&projected_schema)?);

    let row_stream = catalog_helper
        .scan_inline_rows_by_row_ids(table.table_id, &catalog_schema, &inline_row_ids)
        .await?;
    let rows: Vec<Row> = row_stream.try_collect::<Vec<_>>().await?;
    let batch = rows_to_record_batch(&projected_schema, &rows)?;

    Ok(batch)
}

async fn read_data_file_rows(
    table: &Table,
    row_id_score_locations: &[(RowIdScore, RowLocation)],
    projection: Option<Vec<usize>>,
    data_file_records: &[DataFileRecord],
) -> ILResult<Vec<RecordBatch>> {
    let mut data_file_row_ids = HashMap::new();
    for (row_id_score, location) in row_id_score_locations {
        match location {
            RowLocation::Inline => {}
            RowLocation::DataFile(data_file_id) => {
                data_file_row_ids
                    .entry(*data_file_id)
                    .or_insert(HashSet::new())
                    .insert(row_id_score.row_id);
            }
        }
    }

    let mut all_batches = Vec::new();
    for (data_file_id, row_ids) in data_file_row_ids {
        let data_file_record = data_file_records
            .iter()
            .find(|record| record.data_file_id == data_file_id)
            .ok_or(ILError::IndexError(format!(
                "Data file record not found for data file id {data_file_id}"
            )))?;
        let stream = read_parquet_file_by_record(
            &table.storage,
            &table.schema,
            data_file_record,
            projection.clone(),
            None,
            Some(&row_ids),
        )
        .await?;
        let batches: Vec<RecordBatch> = stream.try_collect::<Vec<_>>().await?;
        all_batches.extend(batches);
    }

    Ok(all_batches)
}

fn sort_batches(
    inline_batch: RecordBatch,
    data_file_batches: Vec<RecordBatch>,
    row_id_score_locations: &[(RowIdScore, RowLocation)],
    score_higher_is_better: bool,
) -> ILResult<RecordBatch> {
    let mut batch = inline_batch;
    for data_file_batch in data_file_batches {
        batch = arrow::compute::concat_batches(&batch.schema(), [&batch, &data_file_batch])?;
    }

    let mut scores = Vec::new();
    let row_id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(ILError::IndexError(format!(
            "Row id column not found in batch"
        )))?;
    for row_id in row_id_array.iter() {
        let row_id = row_id.ok_or(ILError::IndexError(format!("Row id is null")))?;
        let row_id_score = row_id_score_locations
            .iter()
            .find(|(row_id_score, _)| row_id_score.row_id == row_id)
            .ok_or(ILError::IndexError(format!(
                "Row id score not found for row id {row_id}"
            )))?;
        scores.push(row_id_score.0.score);
    }

    let scores_array = Float64Array::from(scores);

    let sort_options = SortOptions::default().with_descending(score_higher_is_better);
    let indices = arrow::compute::sort_to_indices(&scores_array, Some(sort_options), None)?;

    let batch = arrow::compute::take_record_batch(&batch, &indices)?;

    Ok(batch)
}
