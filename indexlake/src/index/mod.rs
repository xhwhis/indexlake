mod defination;
mod manager;

pub use defination::*;
pub(crate) use manager::*;

use crate::{
    ILResult,
    expr::Expr,
    storage::{InputFile, OutputFile},
};
use arrow::array::{Int64Array, RecordBatch};
use std::{any::Any, fmt::Debug, sync::Arc};

pub trait IndexKind: Debug + Send + Sync {
    // The kind of the index.
    fn kind(&self) -> &str;

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>>;

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()>;

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>>;

    fn supports_search(
        &self,
        index_def: &IndexDefination,
        query: &dyn SearchQuery,
    ) -> ILResult<bool>;

    fn supports_filter(&self, index_def: &IndexDefination, filter: &Expr) -> ILResult<bool>;
}

#[async_trait::async_trait]
pub trait IndexBuilder: Debug + Send + Sync {
    fn mergeable(&self) -> bool;

    fn index_def(&self) -> &IndexDefinationRef;

    fn append(&mut self, batch: &RecordBatch) -> ILResult<()>;

    async fn read_file(&mut self, input_file: InputFile) -> ILResult<()>;

    async fn write_file(&mut self, output_file: OutputFile) -> ILResult<()>;

    fn read_bytes(&mut self, buf: &[u8]) -> ILResult<()>;

    fn write_bytes(&mut self, buf: &mut Vec<u8>) -> ILResult<()>;

    fn build(&mut self) -> ILResult<Box<dyn Index>>;
}

#[async_trait::async_trait]
pub trait Index: Debug + Send + Sync {
    async fn search(&self, query: &dyn SearchQuery) -> ILResult<SearchIndexEntries>;

    async fn filter(&self, filters: &[Expr]) -> ILResult<FilterIndexEntries>;
}

#[derive(Debug, Clone)]
pub struct SearchIndexEntries {
    pub row_id_scores: Vec<RowIdScore>,
    pub score_higher_is_better: bool,
}

#[derive(Debug, Clone)]
pub struct RowIdScore {
    pub row_id: i64,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct FilterIndexEntries {
    pub row_ids: Int64Array,
}

pub trait SearchQuery: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn index_kind(&self) -> &str;

    fn limit(&self) -> Option<usize>;
}

pub trait IndexParams: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn encode(&self) -> ILResult<String>;
}
