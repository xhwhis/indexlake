use std::{collections::HashMap, fmt::Debug};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, RecordBatch},
    datatypes::SchemaRef,
};
use futures::Stream;

use crate::{ILResult, catalog::Scalar, expr::Expr};

pub type BytesStream = Box<dyn Stream<Item = Vec<u8>>>;

pub trait TopKIndex: Debug + Send + Sync {
    // The kind of the index.
    fn kind(&self) -> &str;

    // Build the index from the given batches.
    fn build(&self, index_def: &IndexDefination, batches: &[RecordBatch]) -> ILResult<BytesStream>;

    fn search(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        input: &Scalar,
        limit: usize,
    ) -> ILResult<TopKIndexEntries>;
}

#[derive(Debug, Clone)]
pub struct TopKIndexEntries {
    pub row_ids: Int64Array,
    pub scores: Float64Array,
    pub score_higher_is_better: bool,
    pub include_columns: HashMap<usize, ArrayRef>,
}

pub trait FilterIndex: Debug + Send + Sync {
    // The kind of the index.
    fn kind(&self) -> &str;

    // Build the index from the given batches.
    fn build(&self, index_def: &IndexDefination, batches: &[RecordBatch]) -> ILResult<BytesStream>;

    fn filter(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        filter: &Expr,
    ) -> ILResult<FilterIndexEntries>;
}

#[derive(Debug, Clone)]
pub struct FilterIndexEntries {
    pub row_ids: Int64Array,
    pub include_columns: HashMap<usize, ArrayRef>,
}

#[derive(Debug, Clone)]
pub struct IndexDefination {
    pub name: String,
    pub kind: String,
    pub table_id: i64,
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub key_columns: Vec<usize>,
    pub include_columns: Vec<usize>,
    pub config: HashMap<String, String>,
}
