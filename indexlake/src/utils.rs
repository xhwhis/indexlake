use std::{collections::HashSet, hash::Hash, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions},
    datatypes::Schema,
};

use crate::{ILResult, catalog::INTERNAL_ROW_ID_FIELD};

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
    let mut fields = vec![Arc::new(INTERNAL_ROW_ID_FIELD.clone())];
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
