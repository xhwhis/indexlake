use crate::schema::SchemaRef;
use crate::{CatalogRow, ILResult};
use arrow::array::RecordBatch;

pub fn record_to_rows(schema: &SchemaRef, record: RecordBatch) -> ILResult<Vec<CatalogRow>> {
    todo!()
}
