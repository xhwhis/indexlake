use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{AsArray, Int64Array, RecordBatch, RecordBatchOptions},
    datatypes::{Int64Type, SchemaRef},
};
use futures::StreamExt;

use crate::{
    ILError, ILResult,
    catalog::{INTERNAL_ROW_ID_FIELD_NAME, Scalar, TransactionHelper},
    expr::{Expr, visited_columns},
    storage::{Storage, read_parquet_file_by_record},
    table::process_insert_into_inline_rows,
};

pub(crate) async fn process_update(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
) -> ILResult<()> {
    let data_file_records = tx_helper.get_data_files(table_id).await?;

    for mut data_file_record in data_file_records {
        let mut stream = read_parquet_file_by_record(
            &storage,
            table_schema.clone(),
            &data_file_record,
            None,
            Some(condition.clone()),
        )
        .await?;

        let mut updated_row_ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let bool_array = condition.condition_eval(&batch)?;

            let row_id_array =
                batch
                    .column(0)
                    .as_primitive_opt::<Int64Type>()
                    .ok_or_else(|| {
                        ILError::InternalError(format!(
                            "row id array should be Int64Array, but got {:?}",
                            batch.column(0).data_type()
                        ))
                    })?;

            let mut selected_indices = Vec::new();
            for (i, v) in bool_array.iter().enumerate() {
                if let Some(v) = v
                    && v
                {
                    selected_indices.push(i as i64);
                    updated_row_ids.push(row_id_array.value(i));
                }
            }

            let indices = Arc::new(Int64Array::from(selected_indices));
            let selected_batch = arrow::compute::take_record_batch(&batch, indices.as_ref())?;
            if selected_batch.num_rows() == 0 {
                continue;
            }
            let updated_batch = update_record_batch(&selected_batch, &set_map)?;
            process_insert_into_inline_rows(tx_helper, table_id, &updated_batch).await?;
        }

        for (row_id, valid) in data_file_record.validity.validity.iter_mut() {
            if updated_row_ids.contains(&row_id) {
                *valid = false;
            }
        }

        tx_helper
            .update_data_file_validity(data_file_record.data_file_id, &data_file_record.validity)
            .await?;
    }

    tx_helper
        .update_inline_rows(table_id, &set_map, condition)
        .await?;

    Ok(())
}

fn update_record_batch(
    batch: &RecordBatch,
    set_map: &HashMap<String, Scalar>,
) -> ILResult<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for (name, value) in set_map {
        let idx = batch.schema().index_of(&name)?;
        let array = value.to_array_of_size(batch.num_rows())?;
        columns[idx] = Arc::new(array);
    }
    let options = RecordBatchOptions::default().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}
