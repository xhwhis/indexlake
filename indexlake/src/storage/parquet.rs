use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Range,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, AsArray, Int64Array, RecordBatch, UInt64Array},
    datatypes::{Int64Type, Schema, SchemaRef},
};
use futures::{StreamExt, TryStreamExt, future::BoxFuture};
use parquet::{
    arrow::{
        ArrowSchemaConverter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask,
        arrow_reader::{ArrowReaderOptions, RowFilter, RowSelection},
        async_reader::AsyncFileReader,
        async_writer::AsyncFileWriter,
    },
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        properties::WriterProperties,
    },
};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{DataFileRecord, INTERNAL_ROW_ID_FIELD_REF},
    expr::{Expr, ExprPredicate},
    storage::{InputFile, OutputFile, Storage},
    utils::build_projection_from_condition,
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

pub(crate) async fn read_parquet_file_by_record(
    storage: &Storage,
    table_schema: &Schema,
    data_file_record: &DataFileRecord,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    row_ids: Option<&HashSet<i64>>,
) -> ILResult<RecordBatchStream> {
    let projection_mask = match projection {
        Some(projection) => {
            let parquet_schema = ArrowSchemaConverter::new().convert(table_schema)?;
            ProjectionMask::roots(&parquet_schema, projection)
        }
        None => ProjectionMask::all(),
    };

    let arrow_predicate_opt = if filters.is_empty() {
        None
    } else {
        Some(ExprPredicate::try_new(filters, projection_mask.clone())?)
    };

    let input_file = storage.open_file(&data_file_record.relative_path).await?;
    let mut arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;

    if let Some(arrow_predicate) = &arrow_predicate_opt {
        arrow_reader_builder = arrow_reader_builder
            .with_row_filter(RowFilter::new(vec![Box::new(arrow_predicate.clone())]));
    }

    let stream = arrow_reader_builder
        .with_row_selection(data_file_record.validity.row_selection(row_ids)?)
        .with_projection(projection_mask.clone())
        .build()?
        .map_err(ILError::from);
    Ok(Box::pin(stream))
}

pub(crate) async fn read_parquet_file_by_record_and_row_id_condition(
    storage: &Storage,
    table_schema: &Schema,
    data_file_record: &DataFileRecord,
    projection: Option<Vec<usize>>,
    row_id_condition: &Expr,
) -> ILResult<RecordBatchStream> {
    let valid_row_ids = data_file_record.validity.valid_row_ids();
    let valid_row_ids_array = Arc::new(Int64Array::from_iter_values(valid_row_ids)) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![INTERNAL_ROW_ID_FIELD_REF.clone()]));
    let batch = RecordBatch::try_new(schema, vec![valid_row_ids_array.clone()])?;

    let bool_array = row_id_condition.condition_eval(&batch)?;

    let mut indices = Vec::new();
    for (i, v) in bool_array.iter().enumerate() {
        if let Some(v) = v
            && v
        {
            indices.push(i as u64);
        }
    }
    if indices.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }
    let index_array = UInt64Array::from(indices);

    let take_array = arrow::compute::take(valid_row_ids_array.as_ref(), &index_array, None)?;
    let match_row_id_array = take_array.as_primitive_opt::<Int64Type>().ok_or_else(|| {
        ILError::InternalError(format!(
            "match row id array should be Int64Array, but got {:?}",
            take_array.data_type()
        ))
    })?;
    let match_row_ids = match_row_id_array
        .values()
        .iter()
        .map(|v| *v)
        .collect::<HashSet<_>>();

    let stream = read_parquet_file_by_record(
        &storage,
        &table_schema,
        &data_file_record,
        projection,
        vec![],
        Some(&match_row_ids),
    )
    .await?;
    Ok(stream)
}

pub(crate) async fn find_matched_row_ids_from_parquet_file(
    storage: &Storage,
    table_schema: &Schema,
    condition: &Expr,
    data_file_record: &DataFileRecord,
) -> ILResult<HashSet<i64>> {
    let mut projection = build_projection_from_condition(table_schema, &condition)?;
    // If the condition does not contain the row id column, add it to the projection
    if !projection.contains(&0) {
        projection.insert(0, 0);
    }

    let mut stream = read_parquet_file_by_record(
        storage,
        table_schema,
        data_file_record,
        Some(projection),
        vec![condition.clone()],
        None,
    )
    .await?;

    let mut matched_row_ids = HashSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let row_id_array = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| {
                ILError::InternalError(format!(
                    "row id array should be Int64Array, but got {:?}",
                    batch.column(0).data_type()
                ))
            })?;

        matched_row_ids.extend(
            row_id_array
                .iter()
                .map(|row_id| row_id.expect("Row id should not be null")),
        );
    }
    Ok(matched_row_ids)
}
