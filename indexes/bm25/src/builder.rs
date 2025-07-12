use std::{
    ops::DerefMut,
    sync::{Arc, LazyLock},
};

use arrow::{
    array::{ArrayRef, AsArray, Int64Array, ListArray, PrimitiveArray},
    datatypes::{DataType, Field, Float32Type, Schema, SchemaRef, UInt32Type},
    record_batch::RecordBatch,
};
use bm25::{Embedder, EmbedderBuilder, Embedding, Scorer, TokenEmbedding};
use futures::StreamExt;
use indexlake::ILResult;
use indexlake::{
    ILError,
    index::{Index, IndexBuilder, IndexDefinationRef},
    storage::{InputFile, OutputFile},
    utils::extract_row_id_array_from_record_batch,
};
use parquet::{
    arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};

use crate::{BM25Index, BM25IndexParams};

static BM25_INDEX_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new(
            "embedding_indices",
            DataType::List(Arc::new(Field::new(
                "embedding_index",
                DataType::UInt32,
                false,
            ))),
            true,
        ),
        Field::new(
            "embedding_values",
            DataType::List(Arc::new(Field::new(
                "embedding_value",
                DataType::Float32,
                false,
            ))),
            true,
        ),
    ]))
});

#[derive(Debug)]
pub struct Bm25IndexBuilder {
    index_def: IndexDefinationRef,
    params: BM25IndexParams,
    embedder: Embedder,
    embeddings: Vec<(Int64Array, Vec<Option<Embedding>>)>,
}

impl Bm25IndexBuilder {
    pub fn try_new(index_def: IndexDefinationRef) -> ILResult<Self> {
        let params = index_def.downcast_params::<BM25IndexParams>()?.clone();
        let embedder = new_embedder(&params);
        Ok(Self {
            index_def,
            params,
            embedder,
            embeddings: Vec::new(),
        })
    }
}

#[async_trait::async_trait]
impl IndexBuilder for Bm25IndexBuilder {
    fn append(&mut self, batch: &RecordBatch) -> ILResult<()> {
        let row_id_array = extract_row_id_array_from_record_batch(&batch)?;

        let key_column_name = &self.index_def.key_columns[0];
        let key_column_index = batch.schema_ref().index_of(&key_column_name)?;
        let key_column = batch.column(key_column_index);

        let embeddings = compute_embeddings(&self.embedder, key_column)?;

        self.embeddings.push((row_id_array, embeddings));
        Ok(())
    }

    async fn read_file(&mut self, input_file: InputFile) -> ILResult<()> {
        let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(input_file).await?;
        let mut batch_stream = arrow_reader_builder.build()?;

        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            let (row_id_array, embeddings) = extract_record_batch(&batch)?;
            self.embeddings.push((row_id_array, embeddings));
        }

        Ok(())
    }

    async fn write_file(&mut self, output_file: OutputFile) -> ILResult<()> {
        let writer_properties = WriterProperties::builder()
            .set_max_row_group_size(4096)
            .build();
        let mut arrow_writer = AsyncArrowWriter::try_new(
            output_file,
            BM25_INDEX_SCHEMA.clone(),
            Some(writer_properties),
        )?;

        for (row_id_array, embeddings) in self.embeddings.iter() {
            let batch = build_index_record_batch(row_id_array.clone(), embeddings)?;
            arrow_writer.write(&batch).await?;
        }

        arrow_writer.close().await?;

        Ok(())
    }

    fn serialize(&self) -> ILResult<Vec<u8>> {
        todo!()
    }

    fn build(&mut self) -> ILResult<Box<dyn Index>> {
        let mut scorer = Scorer::<i64>::new();
        for (row_id_array, embeddings) in self.embeddings.iter_mut() {
            for (row_id, embedding) in row_id_array.iter().zip(embeddings.iter_mut()) {
                if let Some(embedding) = embedding {
                    scorer.upsert(
                        &row_id.expect("Row id should not be null"),
                        std::mem::replace(embedding, Embedding(vec![])),
                    );
                }
            }
        }
        let embedder = new_embedder(&self.params);
        let index = BM25Index {
            index_def: self.index_def.clone(),
            params: self.params.clone(),
            embedder,
            scorer,
        };
        Ok(Box::new(index))
    }
}

fn new_embedder(params: &BM25IndexParams) -> Embedder {
    let embedder = EmbedderBuilder::with_avgdl(256.)
        .language_mode(params.language.to_language_mode())
        .build();
    embedder
}

fn compute_embeddings(
    embedder: &Embedder,
    key_column: &ArrayRef,
) -> ILResult<Vec<Option<Embedding>>> {
    let data_type = key_column.data_type();
    let mut embeddings = Vec::with_capacity(key_column.len());
    match data_type {
        DataType::Utf8 => {
            let utf8_array = key_column.as_string::<i32>();
            for value in utf8_array.iter() {
                match value {
                    Some(value) => {
                        let embedding = embedder.embed(value);
                        embeddings.push(Some(embedding));
                    }
                    None => {
                        embeddings.push(None);
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            let large_utf8_array = key_column.as_string::<i64>();
            for value in large_utf8_array.iter() {
                match value {
                    Some(value) => {
                        let embedding = embedder.embed(value);
                        embeddings.push(Some(embedding));
                    }
                    None => {
                        embeddings.push(None);
                    }
                }
            }
        }
        DataType::Utf8View => {
            let utf8_view_array = key_column.as_string_view();
            for value in utf8_view_array.iter() {
                match value {
                    Some(value) => {
                        let embedding = embedder.embed(value);
                        embeddings.push(Some(embedding));
                    }
                    None => {
                        embeddings.push(None);
                    }
                }
            }
        }
        _ => {
            return Err(ILError::IndexError(format!(
                "Unsupported data type to compute embeddings: {data_type}"
            )));
        }
    }
    Ok(embeddings)
}

fn build_index_record_batch(
    row_id_array: Int64Array,
    embeddings: &[Option<Embedding>],
) -> ILResult<RecordBatch> {
    let mut embedding_indices: Vec<Option<Vec<Option<u32>>>> = Vec::with_capacity(embeddings.len());
    let mut embedding_values: Vec<Option<Vec<Option<f32>>>> = Vec::with_capacity(embeddings.len());
    for embedding_opt in embeddings.iter() {
        if let Some(embedding) = embedding_opt {
            let (indices, values) = embedding
                .iter()
                .map(|te| ((Some(te.index), Some(te.value))))
                .unzip();
            embedding_indices.push(Some(indices));
            embedding_values.push(Some(values));
        } else {
            embedding_indices.push(None);
            embedding_values.push(None);
        }
    }
    let indices_array = ListArray::from_iter_primitive::<UInt32Type, _, _>(embedding_indices);
    let values_array = ListArray::from_iter_primitive::<Float32Type, _, _>(embedding_values);

    Ok(RecordBatch::try_new(
        BM25_INDEX_SCHEMA.clone(),
        vec![
            Arc::new(row_id_array),
            Arc::new(indices_array),
            Arc::new(values_array),
        ],
    )?)
}

fn extract_record_batch(batch: &RecordBatch) -> ILResult<(Int64Array, Vec<Option<Embedding>>)> {
    let row_id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(ILError::IndexError(
            "Failed to downcast row id to Int64Array".to_string(),
        ))?;
    let indices_array =
        batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(ILError::IndexError(
                "Failed to downcast indices to ListArray".to_string(),
            ))?;
    let values_array =
        batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(ILError::IndexError(
                "Failed to downcast values to ListArray".to_string(),
            ))?;

    let mut embeddings = Vec::with_capacity(row_id_array.len());
    for (indices, values) in indices_array.iter().zip(values_array.iter()) {
        let Some(indices) = indices else {
            embeddings.push(None);
            continue;
        };
        let Some(values) = values else {
            embeddings.push(None);
            continue;
        };
        let indices = indices
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .ok_or(ILError::IndexError(
                "Failed to downcast inner indices to PrimitiveArray<UInt32Type>".to_string(),
            ))?;
        let values = values
            .as_any()
            .downcast_ref::<PrimitiveArray<Float32Type>>()
            .ok_or(ILError::IndexError(
                "Failed to downcast inner values to PrimitiveArray<Float32Type>".to_string(),
            ))?;

        let mut token_embeddings = Vec::with_capacity(indices.len());
        for (index, value) in indices.iter().zip(values.iter()) {
            token_embeddings.push(TokenEmbedding {
                index: index.expect("Index should not be null"),
                value: value.expect("Value should not be null"),
            });
        }
        let embedding = Embedding(token_embeddings);
        embeddings.push(Some(embedding));
    }
    Ok((row_id_array.clone(), embeddings))
}
