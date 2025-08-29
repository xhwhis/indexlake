use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use futures::StreamExt;
use indexlake::ILResult;
use indexlake::catalog::Scalar;
use indexlake::index::{Index, IndexBuilder, IndexDefinationRef};
use indexlake::storage::{InputFile, OutputFile};
use indexlake::utils::extract_row_id_array_from_record_batch;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::file::properties::WriterProperties;

use crate::{BTreeIndex, BTreeIndexParams};

#[derive(Debug)]
pub struct BTreeIndexBuilder {
    index_def: IndexDefinationRef,
    params: BTreeIndexParams,
    entries: Vec<(Scalar, i64)>,
    key_data_type: Option<DataType>,
}

impl BTreeIndexBuilder {
    pub fn try_new(index_def: IndexDefinationRef) -> ILResult<Self> {
        let params = index_def.downcast_params::<BTreeIndexParams>()?.clone();

        Ok(Self {
            index_def,
            params,
            entries: Vec::new(),
            key_data_type: None,
        })
    }

    fn process_batch_into_entries(&mut self, batch: &RecordBatch) -> ILResult<()> {
        let row_id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| indexlake::ILError::index("Invalid row_id column type"))?;

        let schema = batch.schema();
        let key_field = schema.field(1);
        let key_data_type = key_field.data_type();

        if self.key_data_type.is_none() {
            self.key_data_type = Some(key_data_type.clone());
        }

        match key_data_type {
            DataType::Int32 => {
                let key_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Int32 key array"))?;

                for i in 0..batch.num_rows() {
                    let row_id = row_id_array.value(i);
                    if !key_array.is_null(i) {
                        let key = Scalar::Int32(Some(key_array.value(i)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Int64 => {
                let key_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Int64 key array"))?;

                for i in 0..batch.num_rows() {
                    let row_id = row_id_array.value(i);
                    if !key_array.is_null(i) {
                        let key = Scalar::Int64(Some(key_array.value(i)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Float64 => {
                let key_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Float64 key array"))?;

                for i in 0..batch.num_rows() {
                    let row_id = row_id_array.value(i);
                    if !key_array.is_null(i) {
                        let key = Scalar::Float64(Some(key_array.value(i)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Utf8 => {
                let key_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Utf8 key array"))?;

                for i in 0..batch.num_rows() {
                    let row_id = row_id_array.value(i);
                    if !key_array.is_null(i) {
                        let key = Scalar::Utf8(Some(key_array.value(i).to_string()));
                        self.entries.push((key, row_id));
                    }
                }
            }
            _ => {
                return Err(indexlake::ILError::index("Unsupported key data type"));
            }
        }

        Ok(())
    }

    fn entries_to_record_batch(&self) -> ILResult<RecordBatch> {
        let row_ids: Vec<i64> = self.entries.iter().map(|(_, row_id)| *row_id).collect();
        let row_id_array = Arc::new(Int64Array::from(row_ids));

        let key_data_type = self.key_data_type.as_ref().unwrap_or(&DataType::Int64);

        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("key", key_data_type.clone(), true),
        ]));

        let key_array: ArrayRef = match key_data_type {
            DataType::Int32 => {
                let keys: Vec<Option<i32>> = self
                    .entries
                    .iter()
                    .map(|(key, _)| {
                        if let Scalar::Int32(val) = key {
                            *val
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(Int32Array::from(keys))
            }
            DataType::Int64 => {
                let keys: Vec<Option<i64>> = self
                    .entries
                    .iter()
                    .map(|(key, _)| {
                        if let Scalar::Int64(val) = key {
                            *val
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(Int64Array::from(keys))
            }
            DataType::Float64 => {
                let keys: Vec<Option<f64>> = self
                    .entries
                    .iter()
                    .map(|(key, _)| {
                        if let Scalar::Float64(val) = key {
                            *val
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(Float64Array::from(keys))
            }
            DataType::Utf8 => {
                let keys: Vec<Option<String>> = self
                    .entries
                    .iter()
                    .map(|(key, _)| {
                        if let Scalar::Utf8(val) = key {
                            val.clone()
                        } else {
                            None
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(keys))
            }
            _ => {
                return Err(indexlake::ILError::index("Unsupported key data type"));
            }
        };

        RecordBatch::try_new(schema, vec![row_id_array, key_array])
            .map_err(|e| indexlake::ILError::index(format!("Failed to create batch: {}", e)))
    }
}

#[async_trait::async_trait]
impl IndexBuilder for BTreeIndexBuilder {
    fn mergeable(&self) -> bool {
        true
    }

    fn index_def(&self) -> &IndexDefinationRef {
        &self.index_def
    }

    fn append(&mut self, batch: &RecordBatch) -> ILResult<()> {
        let row_id_array = extract_row_id_array_from_record_batch(batch)?;

        let key_column_name = &self.index_def.key_columns[0];
        let key_column_index = batch.schema_ref().index_of(key_column_name)?;
        let key_array = batch.column(key_column_index);

        let key_data_type = key_array.data_type();
        if self.key_data_type.is_none() {
            self.key_data_type = Some(key_data_type.clone());
        }

        match key_data_type {
            DataType::Int32 => {
                let key_typed_array = key_array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Int32 key array"))?;

                for row_idx in 0..batch.num_rows() {
                    let row_id = row_id_array.value(row_idx);
                    if !key_typed_array.is_null(row_idx) {
                        let key = Scalar::Int32(Some(key_typed_array.value(row_idx)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Int64 => {
                let key_typed_array = key_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Int64 key array"))?;

                for row_idx in 0..batch.num_rows() {
                    let row_id = row_id_array.value(row_idx);
                    if !key_typed_array.is_null(row_idx) {
                        let key = Scalar::Int64(Some(key_typed_array.value(row_idx)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Float64 => {
                let key_typed_array = key_array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Float64 key array"))?;

                for row_idx in 0..batch.num_rows() {
                    let row_id = row_id_array.value(row_idx);
                    if !key_typed_array.is_null(row_idx) {
                        let key = Scalar::Float64(Some(key_typed_array.value(row_idx)));
                        self.entries.push((key, row_id));
                    }
                }
            }
            DataType::Utf8 => {
                let key_typed_array = key_array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| indexlake::ILError::index("Invalid Utf8 key array"))?;

                for row_idx in 0..batch.num_rows() {
                    let row_id = row_id_array.value(row_idx);
                    if !key_typed_array.is_null(row_idx) {
                        let key = Scalar::Utf8(Some(key_typed_array.value(row_idx).to_string()));
                        self.entries.push((key, row_id));
                    }
                }
            }
            _ => {
                for row_idx in 0..batch.num_rows() {
                    let key = Scalar::try_from_array(key_array, row_idx)?;
                    if !key.is_null() {
                        let row_id = row_id_array.value(row_idx);
                        self.entries.push((key, row_id));
                    }
                }
            }
        }

        Ok(())
    }

    async fn read_file(&mut self, input_file: InputFile) -> ILResult<()> {
        let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
        let mut batch_stream = arrow_reader_builder.build()?;

        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            self.process_batch_into_entries(&batch)?;
        }

        Ok(())
    }

    async fn write_file(&mut self, output_file: OutputFile) -> ILResult<()> {
        let batch = self.entries_to_record_batch()?;
        let writer_properties = WriterProperties::builder()
            .set_max_row_group_size(4096)
            .build();
        let mut arrow_writer =
            AsyncArrowWriter::try_new(output_file, batch.schema(), Some(writer_properties))?;

        arrow_writer.write(&batch).await?;
        arrow_writer.close().await?;

        Ok(())
    }

    fn read_bytes(&mut self, buf: &[u8]) -> ILResult<()> {
        let stream_reader = StreamReader::try_new(buf, None)?;
        for batch in stream_reader {
            let batch = batch?;
            self.process_batch_into_entries(&batch)?;
        }
        Ok(())
    }

    fn write_bytes(&mut self, buf: &mut Vec<u8>) -> ILResult<()> {
        let batch = self.entries_to_record_batch()?;
        let mut stream_writer = StreamWriter::try_new(buf, batch.schema_ref())?;
        stream_writer.write(&batch)?;
        stream_writer.finish()?;
        Ok(())
    }

    fn build(&mut self) -> ILResult<Box<dyn Index>> {
        if self.entries.is_empty() {
            return Ok(Box::new(BTreeIndex::new(self.params.node_size)));
        }

        self.entries
            .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        self.entries.dedup_by(|a, b| a.0 == b.0);

        let mut index = BTreeIndex::new(self.params.node_size);
        for (key, row_id) in self.entries.drain(..) {
            index.insert(key, row_id)?;
        }

        Ok(Box::new(index))
    }
}
