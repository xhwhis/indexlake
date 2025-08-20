use std::cmp::Ordering;
use std::fmt::Display;
use std::iter::repeat_n;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, GenericListArray, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{Field, Schema};
use derive_visitor::{Drive, DriveMut};

use crate::{ILError, ILResult, catalog::CatalogDatabase};

#[derive(Debug, Clone, Drive, DriveMut)]
pub enum Scalar {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Utf8View(Option<String>),
    LargeUtf8(Option<String>),
    Binary(Option<Vec<u8>>),
    BinaryView(Option<Vec<u8>>),
    FixedSizeBinary(i32, Option<Vec<u8>>),
    LargeBinary(Option<Vec<u8>>),
    #[drive(skip)]
    List(Arc<ListArray>),
}

impl Scalar {
    pub fn try_new_null(data_type: &DataType) -> ILResult<Self> {
        Ok(match data_type {
            DataType::Boolean => Scalar::Boolean(None),
            DataType::Int8 => Scalar::Int8(None),
            DataType::Int16 => Scalar::Int16(None),
            DataType::Int32 => Scalar::Int32(None),
            DataType::Int64 => Scalar::Int64(None),
            DataType::UInt8 => Scalar::UInt8(None),
            DataType::UInt16 => Scalar::UInt16(None),
            DataType::UInt32 => Scalar::UInt32(None),
            DataType::UInt64 => Scalar::UInt64(None),
            DataType::Float32 => Scalar::Float32(None),
            DataType::Float64 => Scalar::Float64(None),
            DataType::Utf8 => Scalar::Utf8(None),
            DataType::Utf8View => Scalar::Utf8View(None),
            DataType::LargeUtf8 => Scalar::LargeUtf8(None),
            DataType::Binary => Scalar::Binary(None),
            DataType::BinaryView => Scalar::BinaryView(None),
            DataType::FixedSizeBinary(size) => Scalar::FixedSizeBinary(*size, None),
            DataType::LargeBinary => Scalar::LargeBinary(None),
            DataType::List(field_ref) => {
                Scalar::List(Arc::new(GenericListArray::new_null(field_ref.clone(), 1)))
            }
            _ => {
                return Err(ILError::not_supported(format!(
                    "Cannot create null scalar for data type: {data_type}",
                )));
            }
        })
    }
    pub fn is_null(&self) -> bool {
        match self {
            Scalar::Boolean(v) => v.is_none(),
            Scalar::Int8(v) => v.is_none(),
            Scalar::Int16(v) => v.is_none(),
            Scalar::Int32(v) => v.is_none(),
            Scalar::Int64(v) => v.is_none(),
            Scalar::UInt8(v) => v.is_none(),
            Scalar::UInt16(v) => v.is_none(),
            Scalar::UInt32(v) => v.is_none(),
            Scalar::UInt64(v) => v.is_none(),
            Scalar::Float32(v) => v.is_none(),
            Scalar::Float64(v) => v.is_none(),
            Scalar::Utf8(v) | Scalar::Utf8View(v) | Scalar::LargeUtf8(v) => v.is_none(),
            Scalar::Binary(v)
            | Scalar::BinaryView(v)
            | Scalar::FixedSizeBinary(_, v)
            | Scalar::LargeBinary(v) => v.is_none(),
            Scalar::List(v) => v.len() == v.null_count(),
        }
    }

    pub fn as_bool(&self) -> ILResult<Option<bool>> {
        match self {
            Scalar::Boolean(v) => Ok(*v),
            _ => Err(ILError::invalid_input(format!(
                "Expected boolean, got {self:?}",
            ))),
        }
    }

    pub fn to_sql(&self, database: CatalogDatabase) -> ILResult<String> {
        match self {
            Scalar::Boolean(_)
            | Scalar::Int8(_)
            | Scalar::Int16(_)
            | Scalar::Int32(_)
            | Scalar::Int64(_)
            | Scalar::UInt8(_)
            | Scalar::UInt16(_)
            | Scalar::UInt32(_)
            | Scalar::UInt64(_)
            | Scalar::Float32(_)
            | Scalar::Float64(_) => Ok(self.to_string()),
            Scalar::Utf8(v) | Scalar::Utf8View(v) | Scalar::LargeUtf8(v) => match v {
                Some(value) => Ok(database.sql_string_value(value)),
                None => Ok("null".to_string()),
            },
            Scalar::Binary(v)
            | Scalar::BinaryView(v)
            | Scalar::FixedSizeBinary(_, v)
            | Scalar::LargeBinary(v) => match v {
                Some(value) => Ok(database.sql_binary_value(value)),
                None => Ok("null".to_string()),
            },
            Scalar::List(_) => {
                if self.is_null() {
                    Ok("null".to_string())
                } else {
                    Err(ILError::not_supported(
                        "Not supported to convert list scalar to sql",
                    ))
                }
            }
        }
    }

    pub fn to_arrow_scalar(&self) -> ILResult<arrow::array::Scalar<ArrayRef>> {
        Ok(arrow::array::Scalar::new(self.to_array_of_size(1)?))
    }

    pub fn to_array_of_size(&self, size: usize) -> ILResult<ArrayRef> {
        Ok(match self {
            Scalar::Boolean(e) => Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef,
            Scalar::Int8(e) => match e {
                Some(value) => Arc::new(Int8Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int8, size),
            },
            Scalar::Int16(e) => match e {
                Some(value) => Arc::new(Int16Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int16, size),
            },
            Scalar::Int32(e) => match e {
                Some(value) => Arc::new(Int32Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int32, size),
            },
            Scalar::Int64(e) => match e {
                Some(value) => Arc::new(Int64Array::from_value(*value, size)),
                None => new_null_array(&DataType::Int64, size),
            },
            Scalar::UInt8(e) => match e {
                Some(value) => Arc::new(UInt8Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt8, size),
            },
            Scalar::UInt16(e) => match e {
                Some(value) => Arc::new(UInt16Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt16, size),
            },
            Scalar::UInt32(e) => match e {
                Some(value) => Arc::new(UInt32Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt32, size),
            },
            Scalar::UInt64(e) => match e {
                Some(value) => Arc::new(UInt64Array::from_value(*value, size)),
                None => new_null_array(&DataType::UInt64, size),
            },
            Scalar::Float32(e) => match e {
                Some(value) => Arc::new(Float32Array::from_value(*value, size)),
                None => new_null_array(&DataType::Float32, size),
            },
            Scalar::Float64(e) => match e {
                Some(value) => Arc::new(Float64Array::from_value(*value, size)),
                None => new_null_array(&DataType::Float64, size),
            },
            Scalar::Utf8(e) => match e {
                Some(value) => Arc::new(StringArray::from_iter_values(repeat_n(value, size))),
                None => new_null_array(&DataType::Utf8, size),
            },
            Scalar::Utf8View(e) => match e {
                Some(value) => Arc::new(StringViewArray::from_iter_values(repeat_n(value, size))),
                None => new_null_array(&DataType::Utf8View, size),
            },
            Scalar::LargeUtf8(e) => match e {
                Some(value) => Arc::new(LargeStringArray::from_iter_values(repeat_n(value, size))),
                None => new_null_array(&DataType::LargeUtf8, size),
            },
            Scalar::Binary(e) => match e {
                Some(value) => {
                    Arc::new(repeat_n(Some(value.as_slice()), size).collect::<BinaryArray>())
                }
                None => Arc::new(repeat_n(None::<&str>, size).collect::<BinaryArray>()),
            },
            Scalar::BinaryView(e) => match e {
                Some(value) => Arc::new(BinaryViewArray::from_iter_values(repeat_n(value, size))),
                None => new_null_array(&DataType::BinaryView, size),
            },
            Scalar::FixedSizeBinary(s, e) => match e {
                Some(value) => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat_n(Some(value.as_slice()), size),
                        *s,
                    )
                    .unwrap(),
                ),
                None => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat_n(None::<&[u8]>, size),
                        *s,
                    )
                    .unwrap(),
                ),
            },
            Scalar::LargeBinary(e) => match e {
                Some(value) => Arc::new(LargeBinaryArray::from_iter_values(repeat_n(value, size))),
                None => new_null_array(&DataType::LargeBinary, size),
            },
            Scalar::List(arr) => {
                if size == 1 {
                    return Ok(Arc::clone(arr) as Arc<dyn Array>);
                }
                Self::list_to_array_of_size(arr.as_ref() as &dyn Array, size)?
            }
        })
    }

    pub fn list_to_array_of_size(arr: &dyn Array, size: usize) -> ILResult<ArrayRef> {
        let arrays = repeat_n(arr, size).collect::<Vec<_>>();
        let ret = match !arrays.is_empty() {
            true => arrow::compute::concat(arrays.as_slice())?,
            false => arr.slice(0, 0),
        };
        Ok(ret)
    }

    pub fn try_from_array(array: &dyn Array, index: usize) -> ILResult<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return Self::try_new_null(array.data_type());
        }
        Ok(match array.data_type() {
            DataType::Boolean => {
                let array = array.as_boolean();
                Scalar::Boolean(Some(array.value(index)))
            }
            DataType::Int8 => {
                let array = array.as_primitive::<Int8Type>();
                Scalar::Int8(Some(array.value(index)))
            }
            DataType::Int16 => {
                let array = array.as_primitive::<Int16Type>();
                Scalar::Int16(Some(array.value(index)))
            }
            DataType::Int32 => {
                let array = array.as_primitive::<Int32Type>();
                Scalar::Int32(Some(array.value(index)))
            }
            DataType::Int64 => {
                let array = array.as_primitive::<Int64Type>();
                Scalar::Int64(Some(array.value(index)))
            }
            DataType::UInt8 => {
                let array = array.as_primitive::<UInt8Type>();
                Scalar::UInt8(Some(array.value(index)))
            }
            DataType::UInt16 => {
                let array = array.as_primitive::<UInt16Type>();
                Scalar::UInt16(Some(array.value(index)))
            }
            DataType::UInt32 => {
                let array = array.as_primitive::<UInt32Type>();
                Scalar::UInt32(Some(array.value(index)))
            }
            DataType::UInt64 => {
                let array = array.as_primitive::<UInt64Type>();
                Scalar::UInt64(Some(array.value(index)))
            }
            DataType::Float32 => {
                let array = array.as_primitive::<Float32Type>();
                Scalar::Float32(Some(array.value(index)))
            }
            DataType::Float64 => {
                let array = array.as_primitive::<Float64Type>();
                Scalar::Float64(Some(array.value(index)))
            }
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                Scalar::Utf8(Some(array.value(index).to_string()))
            }
            DataType::Utf8View => {
                let array = array.as_string_view();
                Scalar::Utf8View(Some(array.value(index).to_string()))
            }
            DataType::LargeUtf8 => {
                let array = array.as_string::<i64>();
                Scalar::LargeUtf8(Some(array.value(index).to_string()))
            }
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                Scalar::Binary(Some(array.value(index).to_vec()))
            }
            DataType::BinaryView => {
                let array = array.as_binary_view();
                Scalar::BinaryView(Some(array.value(index).to_vec()))
            }
            DataType::FixedSizeBinary(size) => {
                let array = array.as_fixed_size_binary();
                Scalar::FixedSizeBinary(*size, Some(array.value(index).to_vec()))
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>();
                Scalar::LargeBinary(Some(array.value(index).to_vec()))
            }
            DataType::List(field_ref) => {
                let array = array.as_list::<i32>();
                let ele = array.value(index);
                let offsets = OffsetBuffer::from_lengths([ele.len()]);
                let list = ListArray::new(field_ref.clone(), offsets, ele, None);
                Scalar::List(Arc::new(list))
            }
            _ => {
                return Err(ILError::not_supported(format!(
                    "Unsupported array: {}",
                    array.data_type()
                )));
            }
        })
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Boolean(_) => DataType::Boolean,
            Scalar::Int8(_) => DataType::Int8,
            Scalar::Int16(_) => DataType::Int16,
            Scalar::Int32(_) => DataType::Int32,
            Scalar::Int64(_) => DataType::Int64,
            Scalar::UInt8(_) => DataType::UInt8,
            Scalar::UInt16(_) => DataType::UInt16,
            Scalar::UInt32(_) => DataType::UInt32,
            Scalar::UInt64(_) => DataType::UInt64,
            Scalar::Float32(_) => DataType::Float32,
            Scalar::Float64(_) => DataType::Float64,
            Scalar::Utf8(_) => DataType::Utf8,
            Scalar::Utf8View(_) => DataType::Utf8View,
            Scalar::LargeUtf8(_) => DataType::LargeUtf8,
            Scalar::Binary(_) => DataType::Binary,
            Scalar::BinaryView(_) => DataType::BinaryView,
            Scalar::FixedSizeBinary(size, _) => DataType::FixedSizeBinary(*size),
            Scalar::LargeBinary(_) => DataType::LargeBinary,
            Scalar::List(arr) => arr.data_type().clone(),
        }
    }

    pub fn cast_to(
        &self,
        target_type: &DataType,
        cast_options: &CastOptions<'static>,
    ) -> ILResult<Self> {
        let scalar_array = self.to_array_of_size(1)?;
        let cast_arr = arrow::compute::cast_with_options(&scalar_array, target_type, cast_options)?;
        Self::try_from_array(&cast_arr, 0)
    }

    pub fn arithmetic_negate(&self) -> ILResult<Self> {
        use arrow::array::ArrowNativeTypeOp;
        match self {
            Scalar::Int8(None)
            | Scalar::Int16(None)
            | Scalar::Int32(None)
            | Scalar::Int64(None)
            | Scalar::Float32(None)
            | Scalar::Float64(None) => Ok(self.clone()),
            Scalar::Float64(Some(v)) => Ok(Scalar::Float64(Some(-v))),
            Scalar::Float32(Some(v)) => Ok(Scalar::Float32(Some(-v))),
            Scalar::Int8(Some(v)) => Ok(Scalar::Int8(Some(v.neg_checked()?))),
            Scalar::Int16(Some(v)) => Ok(Scalar::Int16(Some(v.neg_checked()?))),
            Scalar::Int32(Some(v)) => Ok(Scalar::Int32(Some(v.neg_checked()?))),
            Scalar::Int64(Some(v)) => Ok(Scalar::Int64(Some(v.neg_checked()?))),
            _ => Err(ILError::invalid_input(format!(
                "Can not run arithmetic negative on scalar value {self:?}",
            ))),
        }
    }
}

pub fn serialize_scalar(scalar: &Scalar) -> ILResult<Vec<u8>> {
    let field = Field::new("scalar", scalar.data_type(), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let arr = scalar.to_array_of_size(1)?;
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])?;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(buf)
}

pub fn deserialize_scalar(buf: &[u8]) -> ILResult<Scalar> {
    let mut reader = StreamReader::try_new(buf, None)?;
    let Some(batch) = reader.next() else {
        return Err(ILError::invalid_input(
            "Failed to deserialize scalar: empty buffer",
        ));
    };
    let batch = batch?;
    if batch.num_columns() != 1 {
        return Err(ILError::invalid_input(
            "Failed to deserialize scalar: multiple columns",
        ));
    }
    let array = batch.column(0);
    if array.len() != 1 {
        return Err(ILError::invalid_input(
            "Failed to deserialize scalar: invalid array length",
        ));
    }
    let scalar = Scalar::try_from_array(array, 0)?;
    Ok(scalar)
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.eq(v2),
            (Scalar::Boolean(_), _) => false,
            (Scalar::Int8(v1), Scalar::Int8(v2)) => v1.eq(v2),
            (Scalar::Int8(_), _) => false,
            (Scalar::Int16(v1), Scalar::Int16(v2)) => v1.eq(v2),
            (Scalar::Int16(_), _) => false,
            (Scalar::Int32(v1), Scalar::Int32(v2)) => v1.eq(v2),
            (Scalar::Int32(_), _) => false,
            (Scalar::Int64(v1), Scalar::Int64(v2)) => v1.eq(v2),
            (Scalar::Int64(_), _) => false,
            (Scalar::UInt8(v1), Scalar::UInt8(v2)) => v1.eq(v2),
            (Scalar::UInt8(_), _) => false,
            (Scalar::UInt16(v1), Scalar::UInt16(v2)) => v1.eq(v2),
            (Scalar::UInt16(_), _) => false,
            (Scalar::UInt32(v1), Scalar::UInt32(v2)) => v1.eq(v2),
            (Scalar::UInt32(_), _) => false,
            (Scalar::UInt64(v1), Scalar::UInt64(v2)) => v1.eq(v2),
            (Scalar::UInt64(_), _) => false,
            (Scalar::Float32(v1), Scalar::Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Scalar::Float32(_), _) => false,
            (Scalar::Float64(v1), Scalar::Float64(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.to_bits() == d2.to_bits(),
                _ => v1.eq(v2),
            },
            (Scalar::Float64(_), _) => false,
            (Scalar::Utf8(v1), Scalar::Utf8(v2)) => v1.eq(v2),
            (Scalar::Utf8(_), _) => false,
            (Scalar::Utf8View(v1), Scalar::Utf8View(v2)) => v1.eq(v2),
            (Scalar::Utf8View(_), _) => false,
            (Scalar::LargeUtf8(v1), Scalar::LargeUtf8(v2)) => v1.eq(v2),
            (Scalar::LargeUtf8(_), _) => false,
            (Scalar::Binary(v1), Scalar::Binary(v2)) => v1.eq(v2),
            (Scalar::Binary(_), _) => false,
            (Scalar::BinaryView(v1), Scalar::BinaryView(v2)) => v1.eq(v2),
            (Scalar::BinaryView(_), _) => false,
            (Scalar::FixedSizeBinary(_, v1), Scalar::FixedSizeBinary(_, v2)) => v1.eq(v2),
            (Scalar::FixedSizeBinary(_, _), _) => false,
            (Scalar::LargeBinary(v1), Scalar::LargeBinary(v2)) => v1.eq(v2),
            (Scalar::LargeBinary(_), _) => false,
            (Scalar::List(v1), Scalar::List(v2)) => v1.eq(v2),
            (Scalar::List(_), _) => false,
        }
    }
}

impl Eq for Scalar {}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.partial_cmp(v2),
            (Scalar::Boolean(_), _) => None,
            (Scalar::Int8(v1), Scalar::Int8(v2)) => v1.partial_cmp(v2),
            (Scalar::Int8(_), _) => None,
            (Scalar::Int16(v1), Scalar::Int16(v2)) => v1.partial_cmp(v2),
            (Scalar::Int16(_), _) => None,
            (Scalar::Int32(v1), Scalar::Int32(v2)) => v1.partial_cmp(v2),
            (Scalar::Int32(_), _) => None,
            (Scalar::Int64(v1), Scalar::Int64(v2)) => v1.partial_cmp(v2),
            (Scalar::Int64(_), _) => None,
            (Scalar::UInt8(v1), Scalar::UInt8(v2)) => v1.partial_cmp(v2),
            (Scalar::UInt8(_), _) => None,
            (Scalar::UInt16(v1), Scalar::UInt16(v2)) => v1.partial_cmp(v2),
            (Scalar::UInt16(_), _) => None,
            (Scalar::UInt32(v1), Scalar::UInt32(v2)) => v1.partial_cmp(v2),
            (Scalar::UInt32(_), _) => None,
            (Scalar::UInt64(v1), Scalar::UInt64(v2)) => v1.partial_cmp(v2),
            (Scalar::UInt64(_), _) => None,
            (Scalar::Float32(v1), Scalar::Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.partial_cmp(f2),
                _ => v1.partial_cmp(v2),
            },
            (Scalar::Float32(_), _) => None,
            (Scalar::Float64(v1), Scalar::Float64(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.partial_cmp(d2),
                _ => v1.partial_cmp(v2),
            },
            (Scalar::Float64(_), _) => None,
            (Scalar::Utf8(v1), Scalar::Utf8(v2)) => v1.partial_cmp(v2),
            (Scalar::Utf8(_), _) => None,
            (Scalar::Utf8View(v1), Scalar::Utf8View(v2)) => v1.partial_cmp(v2),
            (Scalar::Utf8View(_), _) => None,
            (Scalar::LargeUtf8(v1), Scalar::LargeUtf8(v2)) => v1.partial_cmp(v2),
            (Scalar::LargeUtf8(_), _) => None,
            (Scalar::Binary(v1), Scalar::Binary(v2)) => v1.partial_cmp(v2),
            (Scalar::Binary(_), _) => None,
            (Scalar::BinaryView(v1), Scalar::BinaryView(v2)) => v1.partial_cmp(v2),
            (Scalar::BinaryView(_), _) => None,
            (Scalar::FixedSizeBinary(_, v1), Scalar::FixedSizeBinary(_, v2)) => v1.partial_cmp(v2),
            (Scalar::FixedSizeBinary(_, _), _) => None,
            (Scalar::LargeBinary(v1), Scalar::LargeBinary(v2)) => v1.partial_cmp(v2),
            (Scalar::LargeBinary(_), _) => None,
            (Scalar::List(arr1), Scalar::List(arr2)) => {
                partial_cmp_list(arr1.as_ref(), arr2.as_ref())
            }
            (Scalar::List(_), _) => None,
        }
    }
}

/// Compares two List/LargeList/FixedSizeList scalars
fn partial_cmp_list(arr1: &dyn Array, arr2: &dyn Array) -> Option<Ordering> {
    if arr1.data_type() != arr2.data_type() {
        return None;
    }
    let arr1 = first_array_for_list(arr1);
    let arr2 = first_array_for_list(arr2);

    let min_length = arr1.len().min(arr2.len());
    let arr1_trimmed = arr1.slice(0, min_length);
    let arr2_trimmed = arr2.slice(0, min_length);

    let lt_res = arrow::compute::kernels::cmp::lt(&arr1_trimmed, &arr2_trimmed).ok()?;
    let eq_res = arrow::compute::kernels::cmp::eq(&arr1_trimmed, &arr2_trimmed).ok()?;

    for j in 0..lt_res.len() {
        // In Postgres, NULL values in lists are always considered to be greater than non-NULL values:
        //
        // $ SELECT ARRAY[NULL]::integer[] > ARRAY[1]
        // true
        //
        // These next two if statements are introduced for replicating Postgres behavior, as
        // arrow::compute does not account for this.
        if arr1_trimmed.is_null(j) && !arr2_trimmed.is_null(j) {
            return Some(Ordering::Greater);
        }
        if !arr1_trimmed.is_null(j) && arr2_trimmed.is_null(j) {
            return Some(Ordering::Less);
        }

        if lt_res.is_valid(j) && lt_res.value(j) {
            return Some(Ordering::Less);
        }
        if eq_res.is_valid(j) && !eq_res.value(j) {
            return Some(Ordering::Greater);
        }
    }

    Some(arr1.len().cmp(&arr2.len()))
}

/// List/LargeList/FixedSizeList scalars always have a single element
/// array. This function returns that array
fn first_array_for_list(arr: &dyn Array) -> ArrayRef {
    assert_eq!(arr.len(), 1);
    if let Some(arr) = arr.as_list_opt::<i32>() {
        arr.value(0)
    } else if let Some(arr) = arr.as_list_opt::<i64>() {
        arr.value(0)
    } else if let Some(arr) = arr.as_fixed_size_list_opt() {
        arr.value(0)
    } else {
        unreachable!(
            "Since only List / LargeList / FixedSizeList are supported, this should never happen"
        )
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Boolean(Some(value)) => write!(f, "{value}"),
            Scalar::Boolean(None) => write!(f, "null"),
            Scalar::Int8(Some(value)) => write!(f, "{value}"),
            Scalar::Int8(None) => write!(f, "null"),
            Scalar::Int16(Some(value)) => write!(f, "{value}"),
            Scalar::Int16(None) => write!(f, "null"),
            Scalar::Int32(Some(value)) => write!(f, "{value}"),
            Scalar::Int32(None) => write!(f, "null"),
            Scalar::Int64(Some(value)) => write!(f, "{value}"),
            Scalar::Int64(None) => write!(f, "null"),
            Scalar::UInt8(Some(value)) => write!(f, "{value}"),
            Scalar::UInt8(None) => write!(f, "null"),
            Scalar::UInt16(Some(value)) => write!(f, "{value}"),
            Scalar::UInt16(None) => write!(f, "null"),
            Scalar::UInt32(Some(value)) => write!(f, "{value}"),
            Scalar::UInt32(None) => write!(f, "null"),
            Scalar::UInt64(Some(value)) => write!(f, "{value}"),
            Scalar::UInt64(None) => write!(f, "null"),
            Scalar::Float32(Some(value)) => write!(f, "{value}"),
            Scalar::Float32(None) => write!(f, "null"),
            Scalar::Float64(Some(value)) => write!(f, "{value}"),
            Scalar::Float64(None) => write!(f, "null"),
            Scalar::Utf8(v) | Scalar::Utf8View(v) | Scalar::LargeUtf8(v) => match v {
                Some(value) => write!(f, "{value}"),
                None => write!(f, "null"),
            },
            Scalar::Binary(Some(value)) => write!(f, "{}", hex::encode(value)),
            Scalar::Binary(None) => write!(f, "null"),
            Scalar::BinaryView(Some(value)) => write!(f, "{}", hex::encode(value)),
            Scalar::BinaryView(None) => write!(f, "null"),
            Scalar::FixedSizeBinary(_, Some(value)) => write!(f, "{}", hex::encode(value)),
            Scalar::FixedSizeBinary(_, None) => write!(f, "null"),
            Scalar::LargeBinary(Some(value)) => write!(f, "{}", hex::encode(value)),
            Scalar::LargeBinary(None) => write!(f, "null"),
            Scalar::List(arr) => fmt_list(arr.to_owned() as ArrayRef, f),
        }
    }
}

fn fmt_list(arr: ArrayRef, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    // ScalarValue List, LargeList, FixedSizeList should always have a single element
    assert_eq!(arr.len(), 1);
    let options = FormatOptions::default().with_display_error(true);
    let formatter = ArrayFormatter::try_new(arr.as_ref() as &dyn Array, &options).unwrap();
    let value_formatter = formatter.value(0);
    write!(f, "{value_formatter}")
}

macro_rules! impl_scalar_from {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for Scalar {
            fn from(value: $ty) -> Self {
                Scalar::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for Scalar {
            fn from(value: Option<$ty>) -> Self {
                Scalar::$scalar(value)
            }
        }
    };
}

impl_scalar_from!(bool, Boolean);
impl_scalar_from!(i8, Int8);
impl_scalar_from!(i16, Int16);
impl_scalar_from!(i32, Int32);
impl_scalar_from!(i64, Int64);
impl_scalar_from!(u8, UInt8);
impl_scalar_from!(u16, UInt16);
impl_scalar_from!(u32, UInt32);
impl_scalar_from!(u64, UInt64);
impl_scalar_from!(f32, Float32);
impl_scalar_from!(f64, Float64);
impl_scalar_from!(String, Utf8);
impl_scalar_from!(Vec<u8>, Binary);

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Scalar::Utf8(Some(value.to_string()))
    }
}
impl From<Option<&str>> for Scalar {
    fn from(value: Option<&str>) -> Self {
        Scalar::Utf8(value.map(|s| s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ListArray;

    #[test]
    fn test_scalar_serialization() {
        assert_scalar_serialization(Scalar::Boolean(Some(true)));
        assert_scalar_serialization(Scalar::Boolean(None));
        assert_scalar_serialization(Scalar::Int8(Some(1i8)));
        assert_scalar_serialization(Scalar::Int8(None));
        assert_scalar_serialization(Scalar::Int16(Some(1i16)));
        assert_scalar_serialization(Scalar::Int16(None));
        assert_scalar_serialization(Scalar::Int32(Some(1i32)));
        assert_scalar_serialization(Scalar::Int32(None));
        assert_scalar_serialization(Scalar::Int64(Some(1i64)));
        assert_scalar_serialization(Scalar::Int64(None));
        assert_scalar_serialization(Scalar::UInt8(Some(1u8)));
        assert_scalar_serialization(Scalar::UInt8(None));
        assert_scalar_serialization(Scalar::UInt16(Some(1u16)));
        assert_scalar_serialization(Scalar::UInt16(None));
        assert_scalar_serialization(Scalar::UInt32(Some(1u32)));
        assert_scalar_serialization(Scalar::UInt32(None));
        assert_scalar_serialization(Scalar::UInt64(Some(1u64)));
        assert_scalar_serialization(Scalar::UInt64(None));
        assert_scalar_serialization(Scalar::Float32(Some(1f32)));
        assert_scalar_serialization(Scalar::Float32(None));
        assert_scalar_serialization(Scalar::Float64(Some(1f64)));
        assert_scalar_serialization(Scalar::Float64(None));
        assert_scalar_serialization(Scalar::Utf8(Some("test".to_string())));
        assert_scalar_serialization(Scalar::Utf8(None));
        assert_scalar_serialization(Scalar::Utf8View(Some("test".to_string())));
        assert_scalar_serialization(Scalar::Utf8View(None));
        assert_scalar_serialization(Scalar::LargeUtf8(Some("test".to_string())));
        assert_scalar_serialization(Scalar::LargeUtf8(None));
        assert_scalar_serialization(Scalar::Binary(Some(vec![1u8, 2, 3])));
        assert_scalar_serialization(Scalar::Binary(None));
        assert_scalar_serialization(Scalar::BinaryView(Some(vec![1u8, 2, 3])));
        assert_scalar_serialization(Scalar::BinaryView(None));
        assert_scalar_serialization(Scalar::FixedSizeBinary(3, Some(vec![1u8, 2, 3])));
        assert_scalar_serialization(Scalar::FixedSizeBinary(3, None));
        assert_scalar_serialization(Scalar::LargeBinary(Some(vec![1u8, 2, 3])));
        assert_scalar_serialization(Scalar::LargeBinary(None));
        assert_scalar_serialization(Scalar::List(Arc::new(ListArray::from_iter_primitive::<
            Int32Type,
            _,
            _,
        >(vec![Some(vec![
            Some(0),
            Some(1),
            Some(2),
        ])]))));
        assert_scalar_serialization(
            Scalar::try_new_null(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                false,
            ))))
            .unwrap(),
        );
    }

    fn assert_scalar_serialization(scalar: Scalar) {
        let buf = serialize_scalar(&scalar).unwrap();
        let de_scalar = deserialize_scalar(&buf).unwrap();
        assert_eq!(scalar, de_scalar);
    }
}
