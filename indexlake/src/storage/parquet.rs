use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Range,
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
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
    catalog::DataFileRecord,
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
