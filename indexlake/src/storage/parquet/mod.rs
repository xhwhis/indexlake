use std::{ops::Range, sync::Arc};

use futures::future::BoxFuture;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader,
        async_writer::AsyncFileWriter,
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::storage::{InputFile, OutputFile};

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
