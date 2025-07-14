use std::{collections::HashSet, hash::Hash, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions},
    datatypes::Schema,
};

use crate::{
    ILError, ILResult,
    catalog::{INTERNAL_ROW_ID_FIELD_NAME, INTERNAL_ROW_ID_FIELD_REF},
    expr::{Expr, visited_columns},
};

pub fn has_duplicated_items(container: impl Iterator<Item = impl Eq + Hash>) -> bool {
    let mut set = HashSet::new();
    for item in container {
        if set.contains(&item) {
            return true;
        }
        set.insert(item);
    }
    false
}

pub(crate) fn schema_with_row_id(schema: &Schema) -> Schema {
    let mut fields = vec![INTERNAL_ROW_ID_FIELD_REF.clone()];
    fields.extend(schema.fields.iter().map(|field| field.clone()));
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub(crate) fn record_batch_with_row_id(
    record: &RecordBatch,
    row_id_array: Int64Array,
) -> ILResult<RecordBatch> {
    let schema = schema_with_row_id(&record.schema());
    let mut arrays = vec![Arc::new(row_id_array) as ArrayRef];
    arrays.extend(record.columns().iter().map(|col| col.clone()));
    let options = RecordBatchOptions::new().with_row_count(Some(record.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(schema),
        arrays,
        &options,
    )?)
}

pub fn extract_row_id_array_from_record_batch(record_batch: &RecordBatch) -> ILResult<Int64Array> {
    let index = record_batch
        .schema_ref()
        .index_of(INTERNAL_ROW_ID_FIELD_NAME)?;
    let row_id_array = record_batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ILError::InternalError(format!("Row id column is not an Int64Array")))?;
    Ok(row_id_array.clone())
}

pub fn project_schema(schema: &Schema, projection: Option<&Vec<usize>>) -> ILResult<Schema> {
    if let Some(projection) = projection {
        let schema = schema.project(projection)?;
        Ok(schema)
    } else {
        Ok(schema.clone())
    }
}

pub fn build_projection_from_condition(schema: &Schema, condition: &Expr) -> ILResult<Vec<usize>> {
    let visited_columns = visited_columns(condition);
    if visited_columns.is_empty() {
        return Ok(Vec::new());
    }
    let mut projection = Vec::new();
    for col in visited_columns {
        let idx = schema.index_of(&col)?;
        projection.push(idx);
    }
    projection.sort();
    Ok(projection)
}

#[macro_export]
macro_rules! retry {
    ($fn:expr) => {{
        let mut retry_count = 0;
        loop {
            let result = $fn().await;
            if result.is_ok() {
                break result;
            }
            retry_count += 1;
            if retry_count > 3 {
                break result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }};
}

#[cfg(test)]
mod tests {
    use crate::utils::has_duplicated_items;

    #[test]
    fn test_has_duplicated_items() {
        let items = vec![1, 2, 3, 4, 5];
        assert!(!has_duplicated_items(items.iter()));

        let items_with_duplicates = vec![1, 2, 3, 4, 5, 3];
        assert!(has_duplicated_items(items_with_duplicates.iter()));
    }
}
