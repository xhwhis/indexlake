use std::sync::Arc;

use indexlake::{
    expr::Expr, index::{
        BytesStream, FilterIndexEntries, Index, IndexDefination, IndexParams, SearchQuery,
        TopKIndexEntries,
    }, ILError, ILResult, RecordBatchStream
};

#[derive(Debug, Clone)]
pub struct RStarIndex;

#[async_trait::async_trait]
impl Index for RStarIndex {
    fn kind(&self) -> &str {
        "rstar"
    }

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>> {
        todo!()
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        Ok(())
    }

    async fn build(&self, index_def: &IndexDefination, batch_stream: RecordBatchStream) -> ILResult<BytesStream> {
        todo!()
    }

    async fn search(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        query: &dyn SearchQuery,
    ) -> ILResult<TopKIndexEntries> {
        Err(ILError::NotSupported(format!(
            "RStar index does not support search"
        )))
    }

    async fn filter(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        filter: &Expr,
    ) -> ILResult<FilterIndexEntries> {
        todo!()
    }
}
