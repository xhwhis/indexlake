use std::fmt::Display;

use crate::{
    ILError, ILResult,
    catalog::{CatalogSchemaRef, INTERNAL_ROW_ID_FIELD_NAME, Scalar},
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::{array::*, datatypes::i256};

#[derive(Debug)]
pub struct Row {
    pub schema: CatalogSchemaRef,
    pub values: Vec<Scalar>,
}

impl Row {
    pub fn new(schema: CatalogSchemaRef, values: Vec<Scalar>) -> Self {
        assert_eq!(schema.columns.len(), values.len());
        Self { schema, values }
    }

    pub fn get_row_id(&self) -> ILResult<Option<i64>> {
        let Some(idx) = self.schema.index_of(INTERNAL_ROW_ID_FIELD_NAME) else {
            return Ok(None);
        };
        self.int64(idx)
    }

    pub fn int8(&self, index: usize) -> ILResult<Option<i8>> {
        match self.values[index] {
            Scalar::Int8(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Int8 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn int16(&self, index: usize) -> ILResult<Option<i16>> {
        match self.values[index] {
            Scalar::Int16(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Int16 at index {index} for row {self:?}"
            ))),
        }
    }
    pub fn int32(&self, index: usize) -> ILResult<Option<i32>> {
        match self.values[index] {
            Scalar::Int32(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Int32 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn int64(&self, index: usize) -> ILResult<Option<i64>> {
        match self.values[index] {
            Scalar::Int64(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected BigInt at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn uint8(&self, index: usize) -> ILResult<Option<u8>> {
        match self.values[index] {
            Scalar::UInt8(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected UInt8 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn uint16(&self, index: usize) -> ILResult<Option<u16>> {
        match self.values[index] {
            Scalar::UInt16(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected UInt16 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn uint32(&self, index: usize) -> ILResult<Option<u32>> {
        match self.values[index] {
            Scalar::UInt32(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected UInt32 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn uint64(&self, index: usize) -> ILResult<Option<u64>> {
        match self.values[index] {
            Scalar::UInt64(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected UInt64 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn float32(&self, index: usize) -> ILResult<Option<f32>> {
        match self.values[index] {
            Scalar::Float32(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Float32 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn float64(&self, index: usize) -> ILResult<Option<f64>> {
        match self.values[index] {
            Scalar::Float64(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Float64 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn utf8(&self, index: usize) -> ILResult<Option<&String>> {
        match &self.values[index] {
            Scalar::Utf8(v) => Ok(v.as_ref()),
            _ => Err(ILError::InternalError(format!(
                "Expected Varchar at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn binary(&self, index: usize) -> ILResult<Option<&Vec<u8>>> {
        match &self.values[index] {
            Scalar::Binary(v) => Ok(v.as_ref()),
            _ => Err(ILError::InternalError(format!(
                "Expected Binary at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn boolean(&self, index: usize) -> ILResult<Option<bool>> {
        match self.values[index] {
            Scalar::Boolean(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Boolean at index {index} for row {self:?}"
            ))),
        }
    }
}

pub fn pretty_print_rows(schema_opt: Option<CatalogSchemaRef>, rows: &[Row]) -> impl Display {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");

    let schema_opt = schema_opt.or_else(|| {
        if rows.is_empty() {
            return None;
        } else {
            Some(rows[0].schema.clone())
        }
    });
    if let Some(schema) = schema_opt {
        let mut header = Vec::new();
        for field in schema.columns.iter() {
            header.push(field.name.clone());
        }
        table.set_header(header);
    }

    if rows.is_empty() {
        return table;
    }

    for row in rows {
        let mut cells = Vec::new();
        for value in row.values.iter() {
            cells.push(value.to_string());
        }
        table.add_row(cells);
    }
    table
}

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
        // TODO wait upstream to fix this
        let builder = match field.data_type() {
            DataType::BinaryView => {
                Box::new(BinaryViewBuilder::with_capacity(rows.len())) as Box<dyn ArrayBuilder>
            }
            DataType::Utf8View => {
                Box::new(StringViewBuilder::with_capacity(rows.len())) as Box<dyn ArrayBuilder>
            }
            _ => make_builder(field.data_type(), rows.len()),
        };
        array_builders.push(builder);
    }

    let binary_convert: for<'a> fn(&'a Vec<u8>) -> ILResult<&'a Vec<u8>> = |v: &Vec<u8>| Ok(v);
    let string_convert: for<'a> fn(&'a String) -> ILResult<&'a String> = |v: &String| Ok(v);
    for row in rows {
        for (i, field) in schema.fields.iter().enumerate() {
            match field.data_type() {
                DataType::Boolean => {
                    builder_append!(
                        array_builders[i],
                        BooleanBuilder,
                        field,
                        row,
                        boolean,
                        i,
                        |v| Ok::<_, ILError>(v)
                    );
                }
                DataType::Int8 => {
                    builder_append!(array_builders[i], Int8Builder, field, row, int8, i, |v| {
                        Ok::<_, ILError>(v)
                    });
                }
                DataType::Int16 => {
                    builder_append!(array_builders[i], Int16Builder, field, row, int16, i, |v| {
                        Ok::<_, ILError>(v)
                    });
                }
                DataType::Int32 => {
                    builder_append!(array_builders[i], Int32Builder, field, row, int32, i, |v| {
                        Ok::<_, ILError>(v)
                    });
                }
                DataType::Int64 => {
                    builder_append!(array_builders[i], Int64Builder, field, row, int64, i, |v| {
                        Ok::<_, ILError>(v)
                    });
                }
                DataType::UInt8 => {
                    builder_append!(array_builders[i], UInt8Builder, field, row, uint8, i, |v| {
                        Ok::<_, ILError>(v)
                    });
                }
                DataType::UInt16 => {
                    builder_append!(
                        array_builders[i],
                        UInt16Builder,
                        field,
                        row,
                        uint16,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::UInt32 => {
                    builder_append!(
                        array_builders[i],
                        UInt32Builder,
                        field,
                        row,
                        uint32,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::UInt64 => {
                    builder_append!(
                        array_builders[i],
                        UInt64Builder,
                        field,
                        row,
                        uint64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Float32 => {
                    builder_append!(
                        array_builders[i],
                        Float32Builder,
                        field,
                        row,
                        float32,
                        i,
                        |v| Ok::<_, ILError>(v)
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
                        |v| Ok::<_, ILError>(v)
                    );
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    builder_append!(
                        array_builders[i],
                        TimestampSecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    builder_append!(
                        array_builders[i],
                        TimestampMillisecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    builder_append!(
                        array_builders[i],
                        TimestampMicrosecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    builder_append!(
                        array_builders[i],
                        TimestampNanosecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Date32 => {
                    builder_append!(
                        array_builders[i],
                        Date32Builder,
                        field,
                        row,
                        int32,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Date64 => {
                    builder_append!(
                        array_builders[i],
                        Date64Builder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Time32(TimeUnit::Second) => {
                    builder_append!(
                        array_builders[i],
                        Time32SecondBuilder,
                        field,
                        row,
                        int32,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    builder_append!(
                        array_builders[i],
                        Time32MillisecondBuilder,
                        field,
                        row,
                        int32,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    builder_append!(
                        array_builders[i],
                        Time64MicrosecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    builder_append!(
                        array_builders[i],
                        Time64NanosecondBuilder,
                        field,
                        row,
                        int64,
                        i,
                        |v| { Ok::<_, ILError>(v) }
                    );
                }
                DataType::Binary => {
                    builder_append!(
                        array_builders[i],
                        BinaryBuilder,
                        field,
                        row,
                        binary,
                        i,
                        binary_convert
                    );
                }
                DataType::FixedSizeBinary(_) => {
                    let builder = array_builders[i]
                        .as_any_mut()
                        .downcast_mut::<FixedSizeBinaryBuilder>()
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to downcast builder to FixedSizeBinaryBuilder for {field:?}"
                            )
                        });
                    let v = row.binary(i).map_err(|e| {
                        ILError::InternalError(format!(
                            "Failed to get binary value for {field:?}: {e:?}"
                        ))
                    })?;

                    match v {
                        Some(v) => builder.append_value(v)?,
                        None => builder.append_null(),
                    }
                }
                DataType::LargeBinary => {
                    builder_append!(
                        array_builders[i],
                        LargeBinaryBuilder,
                        field,
                        row,
                        binary,
                        i,
                        binary_convert
                    );
                }
                DataType::BinaryView => {
                    builder_append!(
                        array_builders[i],
                        BinaryViewBuilder,
                        field,
                        row,
                        binary,
                        i,
                        binary_convert
                    );
                }
                DataType::Utf8 => {
                    builder_append!(
                        array_builders[i],
                        StringBuilder,
                        field,
                        row,
                        utf8,
                        i,
                        string_convert
                    );
                }
                DataType::LargeUtf8 => {
                    builder_append!(
                        array_builders[i],
                        LargeStringBuilder,
                        field,
                        row,
                        utf8,
                        i,
                        string_convert
                    );
                }
                DataType::Utf8View => {
                    builder_append!(
                        array_builders[i],
                        StringViewBuilder,
                        field,
                        row,
                        utf8,
                        i,
                        string_convert
                    );
                }
                DataType::Decimal128(_, _) => {
                    builder_append!(
                        array_builders[i],
                        Decimal128Builder,
                        field,
                        row,
                        utf8,
                        i,
                        |v: &String| {
                            let v = v.parse::<i128>().map_err(|e| {
                                ILError::InternalError(format!(
                                    "Failed to parse decimal128 value: {e:?}"
                                ))
                            })?;
                            Ok::<_, ILError>(v)
                        }
                    );
                }
                DataType::Decimal256(_, _) => {
                    builder_append!(
                        array_builders[i],
                        Decimal256Builder,
                        field,
                        row,
                        utf8,
                        i,
                        |v: &String| {
                            let v = v.parse::<i256>().map_err(|e| {
                                ILError::InternalError(format!(
                                    "Failed to parse decimal256 value: {e:?}"
                                ))
                            })?;
                            Ok::<_, ILError>(v)
                        }
                    );
                }
                _ => {
                    return Err(ILError::NotSupported(format!(
                        "Not supported data type: {}",
                        field.data_type()
                    )));
                }
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
