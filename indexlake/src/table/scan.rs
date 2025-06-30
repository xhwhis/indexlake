use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::{StreamExt, TryStreamExt};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::{RowSelection, RowSelector},
    },
    file::metadata::RowGroupMetaData,
};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{
        CatalogSchema, DataFileRecord, RowLocation, TransactionHelper, rows_to_record_batch,
    },
    storage::Storage,
};

pub(crate) async fn process_table_scan(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    storage: Arc<Storage>,
) -> ILResult<RecordBatchStream> {
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&table_schema)?);

    // Inline rows are not deleted, so we can scan them directly
    let rows = tx_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let batch = rows_to_record_batch(&table_schema, &rows)?;

    let mut batches = vec![batch];

    // TODO change to real stream
    let data_files = tx_helper.get_data_files(table_id).await?;
    for data_file in data_files {
        batches.extend(read_data_file(&data_file, &storage).await?);
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

// TODO support projection
pub(crate) async fn read_data_files_by_locations(
    storage: Arc<Storage>,
    data_file_locations: Vec<RowLocation>,
) -> ILResult<RecordBatchStream> {
    let mut file_locations_map: HashMap<String, Vec<RowLocation>> = HashMap::new();
    for location in data_file_locations {
        if let RowLocation::Parquet { relative_path, .. } = &location {
            file_locations_map
                .entry(relative_path.clone())
                .or_default()
                .push(location);
        }
    }

    let mut streams = Vec::new();
    for (relative_path, locations) in file_locations_map {
        let input_file = storage.open_file(&relative_path).await?;
        let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
        let parquet_metadata = arrow_reader_builder.metadata();
        let row_groups_metadata = parquet_metadata.row_groups();

        let mut row_group_offsets_map: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for location in locations {
            if let RowLocation::Parquet {
                row_group_index,
                row_group_offset,
                ..
            } = &location
            {
                row_group_offsets_map
                    .entry(*row_group_index)
                    .or_default()
                    .push(*row_group_offset);
            }
        }

        let row_group_num_rows = row_groups_metadata
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .collect::<Vec<_>>();
        let row_selection = build_row_selection(&row_group_num_rows, &row_group_offsets_map)?;

        let row_groups = row_group_offsets_map.keys().copied().collect::<Vec<_>>();
        let stream = arrow_reader_builder
            .with_row_groups(row_groups)
            .with_row_selection(row_selection)
            .build()?
            .map_err(ILError::from);

        streams.push(stream);
    }

    Ok(Box::pin(futures::stream::select_all(streams)))
}

fn build_row_selection(
    row_group_num_rows: &[usize],
    row_group_offsets_map: &BTreeMap<usize, Vec<usize>>,
) -> ILResult<RowSelection> {
    // merge these row group offsets into a single list of global offsets
    let mut total_rows = 0;
    let mut global_offsets = Vec::new();
    for (row_group_idx, offsets) in row_group_offsets_map {
        if *row_group_idx >= row_group_num_rows.len() {
            return Err(ILError::InternalError(format!(
                "Row group index out of bounds: {}",
                row_group_idx
            )));
        }
        let num_rows = row_group_num_rows[*row_group_idx];
        for offset in offsets {
            global_offsets.push(*offset + total_rows);
        }
        total_rows += num_rows;
    }

    let mut ranges = Vec::new();
    let mut offset_idx = 0;
    while offset_idx < global_offsets.len() {
        let current_offset = global_offsets[offset_idx];
        let mut next_offset_idx = offset_idx + 1;
        while next_offset_idx < global_offsets.len()
            && global_offsets[next_offset_idx] == current_offset + (next_offset_idx - offset_idx)
        {
            next_offset_idx += 1;
        }
        ranges.push(current_offset..global_offsets[next_offset_idx - 1] + 1);
        offset_idx = next_offset_idx;
    }
    Ok(RowSelection::from_consecutive_ranges(
        ranges.into_iter(),
        total_rows,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_row_selection() {
        let row_group_num_rows = vec![10, 20, 30, 40];
        let row_group_offsets_map =
            BTreeMap::from([(1, vec![8, 9, 14, 15, 18]), (3, vec![4, 17, 18, 19, 39])]);
        let row_selection =
            build_row_selection(&row_group_num_rows, &row_group_offsets_map).unwrap();
        assert_eq!(row_selection.row_count(), 10);
        assert_eq!(row_selection.skipped_row_count(), 50);
        assert_eq!(
            row_selection
                .iter()
                .map(|s| (s.row_count, s.skip))
                .collect::<Vec<_>>(),
            vec![
                (8, true),
                (2, false),
                (4, true),
                (2, false),
                (2, true),
                (1, false),
                (5, true),
                (1, false),
                (12, true),
                (3, false),
                (19, true),
                (1, false),
            ]
        );
    }
}
