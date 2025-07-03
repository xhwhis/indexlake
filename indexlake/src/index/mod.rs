use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, RecordBatch},
    datatypes::{Field, FieldRef, SchemaRef},
};
use futures::Stream;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{IndexRecord, Scalar},
    expr::Expr,
    storage::{InputFile, OutputFile},
};

pub type BytesStream = Box<dyn Stream<Item = Vec<u8>> + Send>;

#[async_trait::async_trait]
pub trait Index: Debug + Send + Sync {
    // The kind of the index.
    fn kind(&self) -> &str;

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>>;

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()>;

    // Build the index from the given batches.
    async fn build(
        &self,
        index_def: &IndexDefination,
        batch_stream: RecordBatchStream,
        output_file: OutputFile,
    ) -> ILResult<()>;

    async fn search(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        query: &dyn SearchQuery,
    ) -> ILResult<SearchIndexEntries>;

    // TODO return a stream of entries
    async fn filter(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        filter: &Expr,
    ) -> ILResult<FilterIndexEntries>;
}

#[derive(Debug, Clone)]
pub struct SearchIndexEntries {
    pub row_ids: Int64Array,
    pub scores: Float64Array,
    pub score_higher_is_better: bool,
    pub include_columns: HashMap<usize, ArrayRef>,
}

#[derive(Debug, Clone)]
pub struct FilterIndexEntries {
    pub row_ids: Int64Array,
    pub include_columns: HashMap<String, ArrayRef>,
}

pub trait SearchQuery: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn limit(&self) -> Option<usize>;
}

pub trait IndexParams: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn encode(&self) -> ILResult<String>;
}

#[derive(Debug, Clone)]
pub struct IndexDefination {
    pub name: String,
    pub kind: String,
    pub table_id: i64,
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub key_columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub params: Arc<dyn IndexParams>,
}

impl IndexDefination {
    pub fn key_fields(&self) -> ILResult<Vec<&Field>> {
        let mut key_fields = Vec::new();
        for key_field_id in self.key_columns.iter() {
            let field = self.table_schema.field_with_name(key_field_id)?;
            key_fields.push(field);
        }
        Ok(key_fields)
    }

    pub fn include_fields(&self) -> ILResult<Vec<&Field>> {
        let mut include_fields = Vec::new();
        for include_field_id in self.include_columns.iter() {
            let field = self.table_schema.field_with_name(include_field_id)?;
            include_fields.push(field);
        }
        Ok(include_fields)
    }

    pub fn include_arrays(&self, record_batch: &RecordBatch) -> ILResult<Vec<ArrayRef>> {
        let mut include_arrays = Vec::new();
        for col_name in self.include_columns.iter() {
            let col_index = self.table_schema.index_of(col_name)?;
            let array = record_batch.column(col_index).clone();
            include_arrays.push(array);
        }
        Ok(include_arrays)
    }

    pub fn include_array_map(
        &self,
        record_batch: &RecordBatch,
    ) -> ILResult<HashMap<String, ArrayRef>> {
        let mut include_array_map = HashMap::new();
        for col_name in self.include_columns.iter() {
            let col_index = self.table_schema.index_of(col_name)?;
            let array = record_batch.column(col_index).clone();
            include_array_map.insert(col_name.clone(), array);
        }
        Ok(include_array_map)
    }

    pub fn downcast_params<T: 'static>(&self) -> ILResult<&T> {
        self.params.as_any().downcast_ref::<T>().ok_or_else(|| {
            ILError::InternalError(format!(
                "Index params is not {}",
                std::any::type_name::<T>()
            ))
        })
    }

    pub(crate) fn from_index_record(
        index_record: &IndexRecord,
        field_map: &BTreeMap<i64, FieldRef>,
        table_name: &str,
        table_schema: &SchemaRef,
        index_kinds: &HashMap<String, Arc<dyn Index>>,
    ) -> ILResult<Self> {
        let mut key_columns = Vec::new();
        for key_field_id in index_record.key_field_ids.iter() {
            let field = field_map.get(key_field_id).ok_or_else(|| {
                ILError::InternalError(format!(
                    "Key field id {key_field_id} not found in field map"
                ))
            })?;
            key_columns.push(field.name().to_string());
        }
        let mut include_columns = Vec::new();
        for include_field_id in index_record.include_field_ids.iter() {
            let field = field_map.get(include_field_id).ok_or_else(|| {
                ILError::InternalError(format!(
                    "Include field id {include_field_id} not found in field map"
                ))
            })?;
            include_columns.push(field.name().to_string());
        }

        let index_kind = index_kinds.get(&index_record.index_kind).ok_or_else(|| {
            ILError::InternalError(format!("Index kind {} not found", index_record.index_kind))
        })?;
        let params = index_kind.decode_params(&index_record.params)?;

        Ok(Self {
            name: index_record.index_name.clone(),
            kind: index_record.index_kind.clone(),
            table_id: index_record.table_id,
            table_name: table_name.to_string(),
            table_schema: table_schema.clone(),
            key_columns,
            include_columns,
            params,
        })
    }
}
