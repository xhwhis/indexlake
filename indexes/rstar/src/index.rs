use std::sync::Arc;

use arrow::array::RecordBatch;
use indexlake::{
    ILResult,
    expr::Expr,
    index::{BytesStream, FilterIndex, FilterIndexEntries, IndexDefination, IndexParams},
};

#[derive(Debug, Clone)]
pub struct RStarIndex;

impl FilterIndex for RStarIndex {
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

    fn filter(
        &self,
        index_def: &IndexDefination,
        index: BytesStream,
        filter: &Expr,
    ) -> ILResult<FilterIndexEntries> {
        todo!()
    }
}
