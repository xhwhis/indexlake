mod defination;

pub use defination::*;

use crate::{
    ILError, ILResult, RecordBatchStream,
    expr::Expr,
    storage::{InputFile, OutputFile},
};
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

#[async_trait::async_trait]
pub trait Index: Debug + Send + Sync {
    // The kind of the index.
    fn kind(&self) -> &str;

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>>;

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()>;

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Arc<dyn IndexBuilder>>;

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

#[async_trait::async_trait]
pub trait IndexBuilder: Debug + Send + Sync {
    fn update(&mut self, batch: &RecordBatch) -> ILResult<()>;

    async fn write(&mut self, output_file: OutputFile) -> ILResult<()>;
}
