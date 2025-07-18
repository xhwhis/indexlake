use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit, i256},
};

use crate::{
    ILError, ILResult,
    catalog::{CatalogDatabase, TransactionHelper},
    table::Table,
    utils::serialize_array,
};

pub(crate) async fn process_insert_into_inline_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    batches: &[RecordBatch],
) -> ILResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    for batch in batches {
        let sql_values = record_batch_to_sql_values(batch, tx_helper.database)?;

        let inline_field_names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();

        tx_helper
            .insert_inline_rows(table_id, &inline_field_names, sql_values)
            .await?;
    }
    Ok(())
}

pub(crate) async fn process_bypass_insert(table: &Table, record: &RecordBatch) -> ILResult<()> {
    todo!()
}

macro_rules! extract_sql_values {
    ($array:expr, $array_ty:ty, $convert:expr) => {{
        let mut sql_values = Vec::with_capacity($array.len());
        let array = $array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            ILError::InternalError(format!(
                "Failed to downcast array to {}",
                stringify!($array_ty),
            ))
        })?;
        for v in array.iter() {
            sql_values.push(match v {
                Some(v) => $convert(v),
                None => "NULL".to_string(),
            });
        }
        sql_values
    }};
}

pub(crate) fn record_batch_to_sql_values(
    record: &RecordBatch,
    database: CatalogDatabase,
) -> ILResult<Vec<String>> {
    let mut column_values_list = Vec::with_capacity(record.num_columns());
    for (i, field) in record.schema().fields().iter().enumerate() {
        let array = record.column(i);
        let column_values = match field.data_type() {
            DataType::Boolean => {
                extract_sql_values!(array, BooleanArray, |v: bool| v.to_string())
            }
            DataType::Int8 => {
                extract_sql_values!(array, Int8Array, |v: i8| v.to_string())
            }
            DataType::Int16 => {
                extract_sql_values!(array, Int16Array, |v: i16| v.to_string())
            }
            DataType::Int32 => {
                extract_sql_values!(array, Int32Array, |v: i32| v.to_string())
            }
            DataType::Int64 => {
                extract_sql_values!(array, Int64Array, |v: i64| v.to_string())
            }
            DataType::UInt8 => {
                extract_sql_values!(array, UInt8Array, |v: u8| v.to_string())
            }
            DataType::UInt16 => {
                extract_sql_values!(array, UInt16Array, |v: u16| v.to_string())
            }
            DataType::UInt32 => {
                extract_sql_values!(array, UInt32Array, |v: u32| v.to_string())
            }
            DataType::UInt64 => {
                extract_sql_values!(array, UInt64Array, |v: u64| v.to_string())
            }
            DataType::Float32 => {
                extract_sql_values!(array, Float32Array, |v: f32| v.to_string())
            }
            DataType::Float64 => {
                extract_sql_values!(array, Float64Array, |v: f64| v.to_string())
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                extract_sql_values!(array, TimestampSecondArray, |v: i64| v.to_string())
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                extract_sql_values!(array, TimestampMillisecondArray, |v: i64| v.to_string())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                extract_sql_values!(array, TimestampMicrosecondArray, |v: i64| v.to_string())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                extract_sql_values!(array, TimestampNanosecondArray, |v: i64| v.to_string())
            }
            DataType::Date32 => {
                extract_sql_values!(array, Date32Array, |v: i32| v.to_string())
            }
            DataType::Date64 => {
                extract_sql_values!(array, Date64Array, |v: i64| v.to_string())
            }
            DataType::Time32(TimeUnit::Second) => {
                extract_sql_values!(array, Time32SecondArray, |v: i32| v.to_string())
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                extract_sql_values!(array, Time32MillisecondArray, |v: i32| v.to_string())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                extract_sql_values!(array, Time64MicrosecondArray, |v: i64| v.to_string())
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                extract_sql_values!(array, Time64NanosecondArray, |v: i64| v.to_string())
            }
            DataType::Binary => {
                extract_sql_values!(array, BinaryArray, |v: &[u8]| database.sql_binary_value(v))
            }
            DataType::FixedSizeBinary(_) => {
                extract_sql_values!(array, FixedSizeBinaryArray, |v: &[u8]| database
                    .sql_binary_value(v))
            }
            DataType::LargeBinary => {
                extract_sql_values!(array, LargeBinaryArray, |v: &[u8]| database
                    .sql_binary_value(v))
            }
            DataType::BinaryView => {
                extract_sql_values!(array, BinaryViewArray, |v: &[u8]| database
                    .sql_binary_value(v))
            }
            DataType::Utf8 => {
                extract_sql_values!(array, StringArray, |v: &str| format!("'{}'", v))
            }
            DataType::LargeUtf8 => {
                extract_sql_values!(array, LargeStringArray, |v: &str| format!("'{}'", v))
            }
            DataType::Utf8View => {
                extract_sql_values!(array, StringViewArray, |v: &str| format!("'{}'", v))
            }
            DataType::List(inner_field) => {
                let mut sql_values = Vec::with_capacity(array.len());
                let array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    ILError::InternalError(format!("Failed to downcast array to ListArray"))
                })?;
                for v in array.iter() {
                    sql_values.push(match v {
                        Some(v) => {
                            database.sql_binary_value(&serialize_array(v, inner_field.clone())?)
                        }
                        None => "NULL".to_string(),
                    });
                }
                sql_values
            }
            DataType::FixedSizeList(inner_field, _len) => {
                let mut sql_values = Vec::with_capacity(array.len());
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        ILError::InternalError(format!(
                            "Failed to downcast array to FixedSizeListArray"
                        ))
                    })?;
                for v in array.iter() {
                    sql_values.push(match v {
                        Some(v) => {
                            database.sql_binary_value(&serialize_array(v, inner_field.clone())?)
                        }
                        None => "NULL".to_string(),
                    });
                }
                sql_values
            }
            DataType::LargeList(inner_field) => {
                let mut sql_values = Vec::with_capacity(array.len());
                let array = array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        ILError::InternalError(format!(
                            "Failed to downcast array to LargeListArray"
                        ))
                    })?;
                for v in array.iter() {
                    sql_values.push(match v {
                        Some(v) => {
                            database.sql_binary_value(&serialize_array(v, inner_field.clone())?)
                        }
                        None => "NULL".to_string(),
                    });
                }
                sql_values
            }
            DataType::Decimal128(_, _) => {
                extract_sql_values!(array, Decimal128Array, |v: i128| format!("'{}'", v))
            }
            DataType::Decimal256(_, _) => {
                extract_sql_values!(array, Decimal256Array, |v: i256| format!("'{}'", v))
            }
            _ => {
                return Err(ILError::NotSupported(format!(
                    "Unsupported data type: {}",
                    field.data_type()
                )));
            }
        };
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
