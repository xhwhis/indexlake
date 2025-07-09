use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{Int64Type, Schema, SchemaRef};
use futures::StreamExt;

use crate::catalog::{INTERNAL_ROW_ID_FIELD_NAME, INTERNAL_ROW_ID_FIELD_REF, TransactionHelper};
use crate::expr::{Expr, visited_columns};
use crate::storage::{Storage, read_parquet_file_by_record};
use crate::{ILError, ILResult};

pub(crate) async fn process_delete(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
        return process_delete_by_row_id_condition(tx_helper, table_id, condition).await;
    }

    // Directly delete inline rows
    tx_helper
        .delete_inline_rows_by_condition(table_id, condition)
        .await?;

    delete_data_file_rows(tx_helper, storage, table_id, table_schema, condition).await?;
    Ok(())
}

pub(crate) async fn delete_data_file_rows(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    let data_file_records = tx_helper.get_data_files(table_id).await?;
    for data_file_record in data_file_records {
        let mut stream = read_parquet_file_by_record(
            &storage,
            table_schema.clone(),
            &data_file_record,
            None,
            Some(condition.clone()),
        )
        .await?;

        let mut deleted_row_ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let array = condition.eval(&batch)?.into_array(batch.num_rows())?;
            let bool_array = array.as_boolean_opt().ok_or_else(|| {
                ILError::InternalError(format!(
                    "condition should return BooleanArray, but got {:?}",
                    array.data_type()
                ))
            })?;

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

            for (i, v) in bool_array.iter().enumerate() {
                if let Some(v) = v
                    && v
                {
                    deleted_row_ids.push(row_id_array.value(i));
                }
            }
        }

        let mut row_id_metas = data_file_record.row_id_metas;
        for row_id_meta in row_id_metas.iter_mut() {
            if deleted_row_ids.contains(&row_id_meta.row_id) {
                row_id_meta.valid = false;
            }
        }

        tx_helper
            .update_data_file_row_id_metas(data_file_record.data_file_id, &row_id_metas)
            .await?;
    }
    Ok(())
}

pub(crate) async fn process_delete_by_row_id_condition(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    row_id_condition: &Expr,
) -> ILResult<()> {
    tx_helper
        .delete_inline_rows_by_condition(table_id, row_id_condition)
        .await?;

    let data_file_records = tx_helper.get_data_files(table_id).await?;
    for mut data_file_record in data_file_records {
        let row_ids = data_file_record
            .row_id_metas
            .iter()
            .map(|meta| meta.row_id)
            .collect::<Vec<_>>();
        let row_id_array = Int64Array::from(row_ids);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![INTERNAL_ROW_ID_FIELD_REF.clone()])),
            vec![Arc::new(row_id_array)],
        )?;
        let selected_array = row_id_condition
            .eval(&batch)?
            .into_array(batch.num_rows())?;
        let selected_bool_array = selected_array.as_boolean_opt().ok_or_else(|| {
            ILError::InternalError(format!(
                "condition should return BooleanArray, but got {:?}",
                selected_array.data_type()
            ))
        })?;

        for (i, v) in selected_bool_array.iter().enumerate() {
            if let Some(v) = v
                && v
            {
                data_file_record.row_id_metas[i].valid = false;
            }
        }

        tx_helper
            .update_data_file_row_id_metas(
                data_file_record.data_file_id,
                &data_file_record.row_id_metas,
            )
            .await?;
    }
    Ok(())
}
