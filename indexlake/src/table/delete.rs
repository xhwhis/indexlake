use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Int64Type, Schema, SchemaRef};
use futures::StreamExt;
use tokio::task::JoinHandle;

use crate::catalog::{
    CatalogHelper, DataFileRecord, INTERNAL_ROW_ID_FIELD_NAME, INTERNAL_ROW_ID_FIELD_REF,
    TransactionHelper,
};
use crate::expr::{Expr, visited_columns};
use crate::storage::{Storage, read_parquet_file_by_record};
use crate::utils::build_projection_from_columns;
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
    for mut data_file_record in data_file_records {
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

        for (row_id, valid) in data_file_record.validity.validity.iter_mut() {
            if deleted_row_ids.contains(&row_id) {
                *valid = false;
            }
        }

        tx_helper
            .update_data_file_validity(data_file_record.data_file_id, &data_file_record.validity)
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
        let row_ids = data_file_record.validity.row_ids();
        let row_id_array = Int64Array::from(row_ids);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![INTERNAL_ROW_ID_FIELD_REF.clone()])),
            vec![Arc::new(row_id_array)],
        )?;
        let bool_array = row_id_condition.condition_eval(&batch)?;

        for (i, v) in bool_array.iter().enumerate() {
            if let Some(v) = v
                && v
            {
                data_file_record.validity.validity[i].1 = false;
            }
        }

        tx_helper
            .update_data_file_validity(data_file_record.data_file_id, &data_file_record.validity)
            .await?;
    }
    Ok(())
}

pub(crate) async fn find_matched_data_file_row_ids(
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: SchemaRef,
    condition: Expr,
    data_file_records: Vec<DataFileRecord>,
) -> ILResult<HashMap<i64, Vec<i64>>> {
    condition.check_data_type(&table_schema, &DataType::Boolean)?;

    let visited_columns = visited_columns(&condition);
    if visited_columns.is_empty() {
        return Err(ILError::InternalError(
            "condition should not be a constant".to_string(),
        ));
    }

    let projection = build_projection_from_columns(&table_schema, &visited_columns)?;

    let mut handles = Vec::new();
    for data_file_record in data_file_records {
        let storage = storage.clone();
        let table_schema = table_schema.clone();
        let condition = condition.clone();
        let projection = projection.clone();

        let handle: JoinHandle<ILResult<(i64, Vec<i64>)>> = tokio::spawn(async move {
            let mut stream = read_parquet_file_by_record(
                &storage,
                table_schema.clone(),
                &data_file_record,
                Some(projection),
                Some(condition.clone()),
            )
            .await?;

            let mut matched_row_ids = Vec::new();
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

                for (i, v) in bool_array.iter().enumerate() {
                    if let Some(v) = v
                        && v
                    {
                        matched_row_ids.push(row_id_array.value(i));
                    }
                }
            }
            Ok((data_file_record.data_file_id, matched_row_ids))
        });
        handles.push(handle);
    }

    let mut matched_row_ids = HashMap::new();
    for handle in handles {
        let (data_file_id, row_ids) = handle
            .await
            .map_err(|e| ILError::InternalError(e.to_string()))??;
        matched_row_ids.insert(data_file_id, row_ids);
    }
    Ok(matched_row_ids)
}
