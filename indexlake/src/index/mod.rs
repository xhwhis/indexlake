use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, RecordBatch},
    datatypes::{FieldRef, SchemaRef},
};
use futures::Stream;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{IndexRecord, Scalar},
    expr::Expr,
    storage::InputFile,
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
    ) -> ILResult<BytesStream>;

    async fn search(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        query: &dyn SearchQuery,
    ) -> ILResult<SearchIndexEntries>;

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
    pub include_columns: HashMap<usize, ArrayRef>,
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
