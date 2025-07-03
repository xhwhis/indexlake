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

#[derive(Debug, Clone)]
pub struct RStarIndex;

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
            "RStar index does not support search"
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
