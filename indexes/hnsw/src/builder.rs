use std::sync::Arc;

use arrow::array::{Float32Array, ListArray};
use arrow::record_batch::RecordBatch;
use indexlake::index::IndexDefinationRef;
use indexlake::index::{Index, IndexBuilder};
use indexlake::storage::{InputFile, OutputFile};
use indexlake::utils::extract_row_id_array_from_record_batch;
use indexlake::{ILError, ILResult};

use crate::{HnswIndex, HnswIndexParams};

pub struct HnswIndexBuilder {
    index_def: IndexDefinationRef,
    params: HnswIndexParams,
    index: Arc<usearch::Index>,
}

impl HnswIndexBuilder {
    pub fn try_new(index_def: IndexDefinationRef) -> ILResult<Self> {
        let params = index_def.downcast_params::<HnswIndexParams>()?.clone();
        let options = usearch::IndexOptions {
            dimensions: params.dimensions,
            metric: params.distance.to_usearch(),
            quantization: usearch::ScalarKind::F32,
            connectivity: params.connectivity,
            expansion_add: 0,
            expansion_search: 0,
            multi: false,
        };
        let index = usearch::Index::new(&options)
            .map_err(|e| ILError::IndexError(format!("Failed to create Hnsw index: {e}")))?;

        // TODO why we need this code?
        index.reserve(1).map_err(|e| {
            ILError::IndexError(format!("Failed to reserve capacity for Hnsw index: {e}"))
        })?;

        Ok(Self {
            index_def,
            params,
            index: Arc::new(index),
        })
    }
}

impl std::fmt::Debug for HnswIndexBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndexBuilder")
            .field("index_def", &self.index_def)
            .field("params", &self.params)
            .finish()
    }
}

#[async_trait::async_trait]
impl IndexBuilder for HnswIndexBuilder {
    fn append(&mut self, batch: &RecordBatch) -> ILResult<()> {
        let row_id_array = extract_row_id_array_from_record_batch(&batch)?;

        let key_column_name = &self.index_def.key_columns[0];
        let key_column_index = batch.schema_ref().index_of(&key_column_name)?;
        let key_column = batch.column(key_column_index);

        let key_column = key_column
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| ILError::IndexError(format!("Key column is not a list array")))?;

        // TODO fix
        self.index.reserve(row_id_array.len()).map_err(|e| {
            ILError::IndexError(format!("Failed to reserve capacity for Hnsw index: {e}"))
        })?;

        for (row_id, vector_arr) in row_id_array.iter().zip(key_column.iter()) {
            let row_id = row_id.expect("row id is null");
            if let Some(arr) = vector_arr {
                let vector = arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| ILError::IndexError(format!("Vector is not a float32 array")))?;
                self.index
                    .add(row_id as u64, vector.values())
                    .map_err(|e| {
                        ILError::IndexError(format!("Failed to add vector to Hnsw index: {e}"))
                    })?;
            }
        }
        Ok(())
    }

    async fn read_file(&mut self, input_file: InputFile) -> ILResult<()> {
        let buffer = input_file.read().await?;
        self.index.load_from_buffer(&buffer).map_err(|e| {
            ILError::IndexError(format!("Failed to load Hnsw index from file: {e}"))
        })?;
        Ok(())
    }

    async fn write_file(&mut self, mut output_file: OutputFile) -> ILResult<()> {
        let mut buffer = vec![0; self.index.memory_usage()];
        self.index.save_to_buffer(&mut buffer).map_err(|e| {
            ILError::IndexError(format!("Failed to save Hnsw index to buffer: {e}"))
        })?;

        let writer = output_file.writer();
        writer.write(buffer).await?;

        output_file.close().await?;
        Ok(())
    }

    fn serialize(&self) -> ILResult<Vec<u8>> {
        let mut buffer = vec![0; self.index.memory_usage()];
        self.index.save_to_buffer(&mut buffer).map_err(|e| {
            ILError::IndexError(format!("Failed to save Hnsw index to buffer: {e}"))
        })?;
        Ok(buffer)
    }

    fn build(&mut self) -> ILResult<Box<dyn Index>> {
        Ok(Box::new(HnswIndex::new(self.index.clone())))
    }
}
