use std::sync::Arc;

use arrow::array::RecordBatch;
use indexlake::{
    ILError, ILResult,
    expr::Expr,
    index::{
        BytesStream, FilterIndexEntries, Index, IndexDefination, IndexParams, SearchQuery,
        TopKIndexEntries,
    },
};

#[derive(Debug)]
pub struct HashIndex;

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

    fn build(&self, index_def: &IndexDefination, batches: &[RecordBatch]) -> ILResult<BytesStream> {
        todo!()
    }
    fn search(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        query: &dyn SearchQuery,
    ) -> ILResult<TopKIndexEntries> {
        Err(ILError::NotSupported(format!(
            "Hash index does not support search"
        )))
    }

    fn filter(
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
