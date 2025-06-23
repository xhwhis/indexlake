use std::sync::Arc;

use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use crate::record::DataType;
use crate::{
    ILError, ILResult,
    record::{Row, Schema},
};
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    RecordBatch, StringBuilder, make_builder,
};
use arrow::datatypes::DataType as ArrowDataType;

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

pub fn rows_to_arrow_record(schema: &Schema, rows: &[Row]) -> ILResult<RecordBatch> {
    let arrow_schema = schema_to_arrow_schema(schema)?;

    let mut array_builders = Vec::with_capacity(schema.fields.len());
    for arrow_field in arrow_schema.fields.iter() {
        array_builders.push(make_builder(arrow_field.data_type(), rows.len()));
    }

    for row in rows {
        for (i, field) in schema.fields.iter().enumerate() {
            match field.data_type {
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
            }
        }
    }

    let columns = array_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect();
    RecordBatch::try_new(Arc::new(arrow_schema), columns)
        .map_err(|e| ILError::InternalError(format!("Failed to create record batch: {e:?}")))
}
