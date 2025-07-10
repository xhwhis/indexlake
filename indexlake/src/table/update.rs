use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{AsArray, Int64Array, RecordBatch, RecordBatchOptions, UInt64Array},
    datatypes::{Int64Type, SchemaRef},
};
use futures::StreamExt;
use tokio::task::JoinHandle;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::{DataFileRecord, INTERNAL_ROW_ID_FIELD_NAME, Scalar, TransactionHelper},
    expr::{Expr, visited_columns},
    storage::{Storage, read_parquet_file_by_record},
    table::process_insert_into_inline_rows,
};

pub(crate) async fn process_update_by_condition(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
    mut matched_data_file_rows: HashMap<i64, RecordBatchStream>,
) -> ILResult<()> {
    tx_helper
        .update_inline_rows(table_id, &set_map, condition)
        .await?;

    let data_file_records = tx_helper.get_data_files(table_id).await?;

    for data_file_record in data_file_records {
        if let Some(stream) = matched_data_file_rows.get_mut(&data_file_record.data_file_id) {
            update_data_file_rows_by_matched_rows(
                tx_helper,
                table_id,
                &set_map,
                stream,
                data_file_record,
            )
            .await?;
        } else {
            update_data_file_rows_by_condition(
                tx_helper,
                &storage,
                table_id,
                table_schema,
                &set_map,
                condition,
                data_file_record,
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn update_data_file_rows_by_matched_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    set_map: &HashMap<String, Scalar>,
    matched_data_file_rows: &mut RecordBatchStream,
    mut data_file_record: DataFileRecord,
) -> ILResult<()> {
    let mut updated_row_ids = Vec::new();
    while let Some(batch) = matched_data_file_rows.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let row_id_array = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| {
                ILError::InternalError(format!(
                    "row id array should be Int64Array, but got {:?}",
                    batch.column(0).data_type()
                ))
            })?;
        updated_row_ids.extend_from_slice(row_id_array.values());
        let updated_batch = update_record_batch(&batch, set_map)?;
        process_insert_into_inline_rows(tx_helper, table_id, &updated_batch).await?;
    }
    let update_row_id_map = updated_row_ids
        .into_iter()
        .map(|row_id| (row_id, ()))
        .collect::<HashMap<_, _>>();
    for (row_id, valid) in data_file_record.validity.iter_mut_valid_row_ids() {
        if update_row_id_map.contains_key(row_id) {
            *valid = false;
        }
    }
    tx_helper
        .update_data_file_validity(data_file_record.data_file_id, &data_file_record.validity)
        .await?;
    Ok(())
}

pub(crate) async fn update_data_file_rows_by_condition(
    tx_helper: &mut TransactionHelper,
    storage: &Storage,
    table_id: i64,
    table_schema: &SchemaRef,
    set_map: &HashMap<String, Scalar>,
    condition: &Expr,
    mut data_file_record: DataFileRecord,
) -> ILResult<()> {
    let mut stream = read_parquet_file_by_record(
        storage,
        &table_schema,
        &data_file_record,
        None,
        Some(condition.clone()),
        None,
    )
    .await?;

    let mut updated_row_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let bool_array = condition.condition_eval(&batch)?;

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
        let updated_batch = update_record_batch(&selected_batch, set_map)?;
        process_insert_into_inline_rows(tx_helper, table_id, &updated_batch).await?;
    }

    let update_row_id_map = updated_row_ids
        .into_iter()
        .map(|row_id| (row_id, ()))
        .collect::<HashMap<_, _>>();
    for (row_id, valid) in data_file_record.validity.iter_mut_valid_row_ids() {
        if update_row_id_map.contains_key(row_id) {
            *valid = false;
        }
    }

    tx_helper
        .update_data_file_validity(data_file_record.data_file_id, &data_file_record.validity)
        .await?;
    Ok(())
}

pub(crate) async fn parallel_find_matched_data_file_rows(
    storage: Arc<Storage>,
    table_schema: SchemaRef,
    condition: Expr,
    data_file_records: Vec<DataFileRecord>,
) -> ILResult<HashMap<i64, RecordBatchStream>> {
    let mut handles = Vec::new();
    for data_file_record in data_file_records {
        let storage = storage.clone();
        let table_schema = table_schema.clone();
        let condition = condition.clone();
        let handle: JoinHandle<ILResult<(i64, RecordBatchStream)>> = tokio::spawn(async move {
            let stream = read_parquet_file_by_record(
                &storage,
                &table_schema,
                &data_file_record,
                None,
                Some(condition.clone()),
                None,
            )
            .await?;
            let mut stream = Box::pin(stream.map(move |batch| {
                let batch = batch?;
                let bool_array = condition.condition_eval(&batch)?;
                let mut indices = Vec::new();
                for (i, v) in bool_array.iter().enumerate() {
                    if let Some(v) = v
                        && v
                    {
                        indices.push(i as u64);
                    }
                }
                let index_array = UInt64Array::from(indices);
                let selected_batch = arrow::compute::take_record_batch(&batch, &index_array)?;
                Ok(selected_batch)
            })) as RecordBatchStream;

            // prefetch record batch into memory
            let mut prefetch_row_count = 0;
            let mut stream_exhausted = true;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                prefetch_row_count += batch.num_rows();
                batches.push(batch);
                if prefetch_row_count > 1000 {
                    stream_exhausted = false;
                    break;
                }
            }

            let memory_stream =
                Box::pin(futures::stream::iter(batches).map(Ok)) as RecordBatchStream;

            let merged_stream = if stream_exhausted {
                memory_stream
            } else {
                Box::pin(futures::stream::select_all(vec![memory_stream, stream]))
            };
            Ok((data_file_record.data_file_id, merged_stream))
        });
        handles.push(handle);
    }
    let mut matched_rows = HashMap::new();
    for handle in handles {
        let (data_file_id, stream) = handle
            .await
            .map_err(|e| ILError::InternalError(e.to_string()))??;
        matched_rows.insert(data_file_id, stream);
    }
    Ok(matched_rows)
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
