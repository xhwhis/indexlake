use crate::record::SchemaRef;
use crate::{ILResult, record::Row};
use arrow::array::RecordBatch;

pub fn record_to_rows(schema: &SchemaRef, record: RecordBatch) -> ILResult<Vec<Row>> {
    todo!()
}
