use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{CatalogHelper, CatalogSchema, IndexFileRecord, rows_to_record_batch},
    index::{IndexDefination, IndexDefinationRef, IndexKind, SearchIndexEntries, SearchQuery},
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
    for data_file_record in data_file_records {
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

    todo!()
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

fn top_search_index_entries(
    inline_search_entries: &SearchIndexEntries,
    index_file_search_entries: &HashMap<i64, SearchIndexEntries>,
) -> ILResult<Vec<i64>> {
    todo!()
}
