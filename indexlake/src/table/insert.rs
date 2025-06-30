use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, RecordBatch, StringArray,
    },
    datatypes::{DataType, Field, Schema},
};

use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDatabase, CatalogSchema, INTERNAL_ROW_ID_FIELD, RowLocation, RowMetadataRecord,
        TransactionHelper,
    },
    utils::record_batch_with_row_id,
};

pub(crate) async fn process_insert_values(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    record: &RecordBatch,
) -> ILResult<()> {
    let max_row_id = tx_helper.get_max_row_id(table_id).await?;

    // Generate row id for each row
    let row_ids = (max_row_id + 1..max_row_id + 1 + record.num_rows() as i64).collect::<Vec<_>>();
    let row_metadatas = row_ids
        .iter()
        .map(|id| RowMetadataRecord::new(*id, RowLocation::Inline))
        .collect::<Vec<_>>();

    let row_id_array = Int64Array::from(row_ids);
    let record = record_batch_with_row_id(record, row_id_array)?;

    let sql_values = record_batch_to_sql_values(&record, tx_helper.database)?;

    let inline_field_names = record
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();

    tx_helper
        .insert_inline_rows(table_id, &inline_field_names, sql_values)
        .await?;

    tx_helper
        .insert_row_metadatas(table_id, &row_metadatas)
        .await?;
    Ok(())
}

pub(crate) fn record_batch_to_sql_values(
    record: &RecordBatch,
    database: CatalogDatabase,
) -> ILResult<Vec<String>> {
    let mut column_values_list = Vec::with_capacity(record.num_columns());
    for (i, field) in record.schema().fields().iter().enumerate() {
        let mut column_values = Vec::with_capacity(record.num_rows());
        let any_array = record.column(i).as_any();
        match field.data_type() {
            DataType::Boolean => {
                let array = any_array.downcast_ref::<BooleanArray>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to BooleanArray"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Int16 => {
                let array = any_array.downcast_ref::<Int16Array>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to Int16Array"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Int32 => {
                let array = any_array.downcast_ref::<Int32Array>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to Int32Array"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Int64 => {
                let array = any_array.downcast_ref::<Int64Array>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to Int64Array"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Float32 => {
                let array = any_array.downcast_ref::<Float32Array>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to Float32Array"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Float64 => {
                let array = any_array.downcast_ref::<Float64Array>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to Float64Array"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Utf8 => {
                let array = any_array.downcast_ref::<StringArray>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to StringArray"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => format!("'{}'", v),
                        None => "NULL".to_string(),
                    });
                }
            }
            DataType::Binary => {
                let array = any_array.downcast_ref::<BinaryArray>().ok_or_else(|| {
                    ILError::InternalError(format!(
                        "Failed to downcast field {field:?} to BinaryArray"
                    ))
                })?;
                for v in array.iter() {
                    column_values.push(match v {
                        Some(v) => database.sql_binary_value(v),
                        None => "NULL".to_string(),
                    });
                }
            }
            _ => {
                return Err(ILError::NotSupported(format!(
                    "Unsupported data type: {:?}",
                    field.data_type()
                )));
            }
        }
        column_values_list.push(column_values);
    }
    let mut sql_values = Vec::with_capacity(record.num_rows());
    for row_idx in 0..record.num_rows() {
        let mut row_values = Vec::with_capacity(record.num_columns());
        for column_values in column_values_list.iter() {
            row_values.push(column_values[row_idx].clone());
        }
        let row_values_str = row_values.join(", ");
        sql_values.push(format!("({})", row_values_str));
    }
    Ok(sql_values)
}
