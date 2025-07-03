use std::sync::Arc;

use indexlake::{
    expr::Expr, index::{
        BytesStream, FilterIndexEntries, Index, IndexDefination, IndexParams, SearchQuery,
        TopKIndexEntries,
    }, ILError, ILResult, RecordBatchStream
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
            "Hash index does not support search"
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
