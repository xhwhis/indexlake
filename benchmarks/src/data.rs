use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

pub fn arrow_table_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("data", DataType::Binary, true),
    ]);
    Arc::new(schema)
}

pub fn new_record_batch(num_rows: usize) -> RecordBatch {
    let schema = arrow_table_schema();
    let id_array = Int32Array::from_iter_values(0..num_rows as i32);
    let content = "content".repeat(1000);
    let content_array = StringArray::from_iter_values(vec![content; num_rows]);
    let data = b"data".repeat(1000);
    let data_array = BinaryArray::from_iter_values(vec![data; num_rows]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(content_array) as ArrayRef,
            Arc::new(data_array) as ArrayRef,
        ],
    )
    .unwrap()
}
