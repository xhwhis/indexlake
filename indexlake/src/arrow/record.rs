use std::sync::Arc;

use crate::arrow::{schema_to_catalog_schema, schema_without_column};
use crate::catalog::CatalogSchemaRef;
use crate::catalog::Row;
use crate::{ILError, ILResult, catalog::CatalogScalar};
use arrow::array::{
    Array, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, RecordBatch, RecordBatchOptions, StringArray, StringBuilder, make_builder,
};
use arrow::datatypes::{DataType, SchemaRef};

macro_rules! builder_append {
    ($builder:expr, $builder_ty:ty, $field:expr, $row:expr, $row_method:ident, $index:expr, $convert:expr) => {{
        let builder = $builder
            .as_any_mut()
            .downcast_mut::<$builder_ty>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast builder to {} for {:?}",
                    stringify!($builder_ty),
                    $field,
                )
            });
        let v = $row.$row_method($index).map_err(|e| {
            ILError::InternalError(format!(
                "Failed to get {} value for {:?}: {e:?}",
                stringify!($value_ty),
                $field,
            ))
        })?;

        match v {
            Some(v) => builder.append_value($convert(v)?),
            None => builder.append_null(),
        }
    }};
}

pub fn rows_to_record_batch(schema: &SchemaRef, rows: &[Row]) -> ILResult<RecordBatch> {
    let mut array_builders = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        array_builders.push(make_builder(field.data_type(), rows.len()));
    }

    for row in rows {
        for (i, field) in schema.fields.iter().enumerate() {
            match field.data_type() {
                DataType::Int16 => {
                    builder_append!(array_builders[i], Int16Builder, field, row, int16, i, |v| {
                        Ok::<_, ILError>(v as i16)
                    });
                }
                DataType::Int32 => {
                    builder_append!(array_builders[i], Int32Builder, field, row, int32, i, |v| {
                        Ok::<_, ILError>(v as i32)
                    });
                }
                DataType::Int64 => {
                    builder_append!(array_builders[i], Int64Builder, field, row, int64, i, |v| {
                        Ok::<_, ILError>(v as i64)
                    });
                }
                DataType::Float32 => {
                    builder_append!(
                        array_builders[i],
                        Float32Builder,
                        field,
                        row,
                        float32,
                        i,
                        |v| Ok::<_, ILError>(v as f32)
                    );
                }
                DataType::Float64 => {
                    builder_append!(
                        array_builders[i],
                        Float64Builder,
                        field,
                        row,
                        float64,
                        i,
                        |v| Ok::<_, ILError>(v as f64)
                    );
                }
                DataType::Boolean => {
                    builder_append!(
                        array_builders[i],
                        BooleanBuilder,
                        field,
                        row,
                        boolean,
                        i,
                        |v| Ok::<_, ILError>(v as bool)
                    );
                }
                DataType::Utf8 => {
                    let convert: for<'a> fn(&'a String) -> ILResult<&'a String> =
                        |v: &String| Ok(v);
                    builder_append!(
                        array_builders[i],
                        StringBuilder,
                        field,
                        row,
                        utf8,
                        i,
                        convert
                    );
                }
                DataType::Binary => {
                    let convert: for<'a> fn(&'a Vec<u8>) -> ILResult<&'a Vec<u8>> =
                        |v: &Vec<u8>| Ok(v);
                    builder_append!(
                        array_builders[i],
                        BinaryBuilder,
                        field,
                        row,
                        binary,
                        i,
                        convert
                    );
                }
                _ => todo!(),
            }
        }
    }

    let columns = array_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect();
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ILError::InternalError(format!("Failed to create record batch: {e:?}")))
}

pub fn record_batch_without_column(
    record_batch: &RecordBatch,
    column_name: &str,
) -> ILResult<RecordBatch> {
    let arrow_schema = record_batch.schema();
    let arrow_col_idx = arrow_schema.index_of(&column_name).map_err(|_e| {
        ILError::InternalError(format!(
            "Failed to find field {column_name} in arrow schema: {arrow_schema:?}"
        ))
    })?;

    let new_arrow_schema = schema_without_column(&arrow_schema, column_name)?;

    let mut arrays = Vec::with_capacity(record_batch.num_columns() - 1);
    for (i, array) in record_batch.columns().iter().enumerate() {
        if i != arrow_col_idx {
            arrays.push(array.clone());
        }
    }

    let options = RecordBatchOptions::new().with_row_count(Some(record_batch.num_rows()));
    let new_batch =
        RecordBatch::try_new_with_options(Arc::new(new_arrow_schema), arrays, &options)?;
    Ok(new_batch)
}
