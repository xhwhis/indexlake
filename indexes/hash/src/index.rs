use arrow::array::RecordBatch;
use indexlake::{
    ILResult,
    expr::Expr,
    index::{BytesStream, FilterIndex, FilterIndexEntries, IndexDefination},
};

#[derive(Debug)]
pub struct HashIndex;

impl FilterIndex for HashIndex {
    fn kind(&self) -> &str {
        "hash"
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
