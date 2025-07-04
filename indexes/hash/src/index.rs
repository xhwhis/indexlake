use std::sync::Arc;

use indexlake::{
    ILError, ILResult, RecordBatchStream,
    expr::Expr,
    index::{
        FilterIndexEntries, Index, IndexBuilder, IndexDefination, IndexDefinationRef, IndexParams,
        SearchIndexEntries, SearchQuery,
    },
    storage::{InputFile, OutputFile},
};

#[derive(Debug)]
pub struct HashIndex;

#[async_trait::async_trait]
impl Index for HashIndex {
    fn kind(&self) -> &str {
        "hash"
    }

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>> {
        todo!()
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        Ok(())
    }

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        todo!()
    }

    async fn search(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        query: &dyn SearchQuery,
    ) -> ILResult<SearchIndexEntries> {
        Err(ILError::NotSupported(format!(
            "Hash index does not support search"
        )))
    }

    fn supports_filter(&self, index_def: &IndexDefination, filter: &Expr) -> ILResult<bool> {
        todo!()
    }

    async fn filter(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        filters: &[Expr],
    ) -> ILResult<FilterIndexEntries> {
        todo!()
    }
}

#[derive(Debug)]
pub struct HashIndexParams;

impl IndexParams for HashIndexParams {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn encode(&self) -> ILResult<String> {
        Ok("NONE".to_string())
    }
}
