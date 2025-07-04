use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{AsArray, Int64Array, RecordBatch, RecordBatchOptions},
    datatypes::{Int64Type, SchemaRef},
};
use futures::StreamExt;

use crate::{
    ILError, ILResult,
    catalog::{INTERNAL_ROW_ID_FIELD_NAME, RowLocation, Scalar, TransactionHelper},
    expr::{Expr, visited_columns},
    storage::{Storage, read_parquet_files_by_locations},
    table::process_insert_batch_with_row_id,
};

pub(crate) async fn process_update(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
) -> ILResult<()> {
    let mut row_metadata_condition =
        Expr::Column("deleted".to_string()).eq(Expr::Literal(Scalar::Boolean(Some(false))));
    if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
        row_metadata_condition = row_metadata_condition.and(condition.clone());
    }

    let row_metadatas = tx_helper
        .scan_row_metadata(table_id, &row_metadata_condition)
        .await?;
    let data_file_locations = row_metadatas
        .into_iter()
        .filter(|meta| matches!(meta.location, RowLocation::Parquet { .. }))
        .map(|meta| meta.location)
        .collect::<Vec<_>>();

    let mut updated_row_ids = Vec::new();
    let mut stream = read_parquet_files_by_locations(
        storage,
        table_schema.clone(),
        None,
        data_file_locations,
        Some(condition.clone()),
    )
    .await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let array = condition.eval(&batch)?.into_array(batch.num_rows())?;
        let bool_array = array.as_boolean_opt().ok_or_else(|| {
            ILError::InternalError(format!(
                "condition should return BooleanArray, but got {:?}",
                array.data_type()
            ))
        })?;

        let row_id_array = batch
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
        process_insert_batch_with_row_id(tx_helper, table_id, &updated_batch).await?;
    }

    tx_helper
        .update_row_location_as_inline(table_id, &updated_row_ids)
        .await?;

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
