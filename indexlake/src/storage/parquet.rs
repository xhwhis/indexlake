use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use futures::{TryStreamExt, future::BoxFuture};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::{ArrowReaderOptions, RowFilter, RowSelection},
        async_reader::AsyncFileReader,
        async_writer::AsyncFileWriter,
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::RowLocation,
    expr::{Expr, ExprPredicate},
    storage::{InputFile, OutputFile, Storage},
};

impl AsyncFileReader for InputFile {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        Box::pin(async move {
            let buffer = self
                .reader
                .read(range.start..range.end)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
            Ok(buffer.to_bytes())
        })
    }

    // TODO respect options
    fn get_metadata(
        &mut self,
        _options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async {
            let reader = ParquetMetaDataReader::new()
                .with_prefetch_hint(None)
                .with_column_indexes(false)
                .with_page_indexes(false)
                .with_offset_indexes(false);
            let size = self
                .file_size_bytes()
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
            let meta = reader.load_and_finish(self, size).await?;

            Ok(Arc::new(meta))
        })
    }
}

impl AsyncFileWriter for OutputFile {
    fn write(&mut self, bs: bytes::Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.writer
                .write(bs)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            let _ = self
                .writer
                .close()
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
            Ok(())
        })
    }
}

// TODO support projection
pub(crate) async fn read_parquet_files_by_locations(
    storage: Arc<Storage>,
    table_schema: SchemaRef,
    data_file_locations: Vec<RowLocation>,
    predicate: Option<Expr>,
) -> ILResult<RecordBatchStream> {
    let arrow_predicate_opt = match predicate {
        Some(expr) => Some(ExprPredicate::try_new(expr, &table_schema)?),
        None => None,
    };

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
        let mut arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
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

        let row_groups = row_group_offsets_map.keys().copied().collect::<Vec<_>>();

        let row_group_num_rows = row_groups_metadata
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .collect::<Vec<_>>();
        let row_selection = build_row_selection(&row_group_num_rows, row_group_offsets_map)?;

        if let Some(arrow_predicate) = &arrow_predicate_opt {
            arrow_reader_builder = arrow_reader_builder
                .with_row_filter(RowFilter::new(vec![Box::new(arrow_predicate.clone())]));
        }

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
    row_group_offsets_map: BTreeMap<usize, Vec<usize>>,
) -> ILResult<RowSelection> {
    // merge these row group offsets into a single list of global offsets
    let mut total_rows = 0;
    let mut global_offsets = Vec::new();
    for (row_group_idx, mut offsets) in row_group_offsets_map {
        if row_group_idx >= row_group_num_rows.len() {
            return Err(ILError::InternalError(format!(
                "Row group index out of bounds: {}",
                row_group_idx
            )));
        }
        let num_rows = row_group_num_rows[row_group_idx];
        offsets.sort();
        for offset in offsets {
            global_offsets.push(offset + total_rows);
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
            build_row_selection(&row_group_num_rows, row_group_offsets_map).unwrap();
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
