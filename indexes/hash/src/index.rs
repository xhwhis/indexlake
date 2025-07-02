use std::sync::Arc;

use arrow::array::RecordBatch;
use indexlake::{
    ILResult,
    expr::Expr,
    index::{BytesStream, FilterIndex, FilterIndexEntries, IndexDefination, IndexParams},
};

#[derive(Debug)]
pub struct HashIndex;

impl FilterIndex for HashIndex {
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
