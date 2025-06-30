use std::fmt::Display;
use std::iter::repeat_n;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, new_null_array,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use derive_visitor::{Drive, DriveMut};

use crate::{ILError, ILResult, catalog::CatalogDatabase};

#[derive(Debug, Clone, Drive, DriveMut)]
pub enum Scalar {
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Binary(Option<Vec<u8>>),
    Boolean(Option<bool>),
}

impl Scalar {
    pub fn try_new_null(data_type: &DataType) -> ILResult<Self> {
        Ok(match data_type {
            DataType::Int16 => Scalar::Int16(None),
            DataType::Int32 => Scalar::Int32(None),
            DataType::Int64 => Scalar::Int64(None),
            DataType::Float32 => Scalar::Float32(None),
            DataType::Float64 => Scalar::Float64(None),
            DataType::Utf8 => Scalar::Utf8(None),
            DataType::Binary => Scalar::Binary(None),
            DataType::Boolean => Scalar::Boolean(None),
            _ => {
                return Err(ILError::NotSupported(format!(
                    "Cannot create null scalar for data type: {:?}",
                    data_type
                )));
            }
        })
    }
    pub fn is_null(&self) -> bool {
        match self {
            Scalar::Int16(v) => v.is_none(),
            Scalar::Int32(v) => v.is_none(),
            Scalar::Int64(v) => v.is_none(),
            Scalar::Float32(v) => v.is_none(),
            Scalar::Float64(v) => v.is_none(),
            Scalar::Utf8(v) => v.is_none(),
            Scalar::Binary(v) => v.is_none(),
            Scalar::Boolean(v) => v.is_none(),
        }
    }

    pub fn as_bool(&self) -> ILResult<Option<bool>> {
        match self {
            Scalar::Boolean(v) => Ok(*v),
            _ => Err(ILError::InvalidInput(format!(
                "Expected boolean, got {:?}",
                self
            ))),
        }
    }

    pub fn to_sql(&self, database: CatalogDatabase) -> String {
        match self {
            Scalar::Int16(Some(value)) => value.to_string(),
            Scalar::Int16(None) => "null".to_string(),
            Scalar::Int32(Some(value)) => value.to_string(),
            Scalar::Int32(None) => "null".to_string(),
            Scalar::Int64(Some(value)) => value.to_string(),
            Scalar::Int64(None) => "null".to_string(),
            Scalar::Float32(Some(value)) => value.to_string(),
            Scalar::Float32(None) => "null".to_string(),
            Scalar::Float64(Some(value)) => value.to_string(),
            Scalar::Float64(None) => "null".to_string(),
            Scalar::Utf8(Some(value)) => format!("'{}'", value),
            Scalar::Utf8(None) => "null".to_string(),
            Scalar::Binary(Some(value)) => match database {
                CatalogDatabase::Sqlite => format!("X'{}'", hex::encode(value)),
                CatalogDatabase::Postgres => format!("E'\\\\x{}'", hex::encode(value)),
            },
            Scalar::Binary(None) => "null".to_string(),
            Scalar::Boolean(Some(value)) => value.to_string(),
            Scalar::Boolean(None) => "null".to_string(),
        }
    }

    pub fn to_arrow_scalar(&self) -> ILResult<arrow::array::Scalar<ArrayRef>> {
        Ok(arrow::array::Scalar::new(self.to_array_of_size(1)?))
    }

    pub fn to_array_of_size(&self, size: usize) -> ILResult<ArrayRef> {
        Ok(match self {
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
            Scalar::Binary(e) => match e {
                Some(value) => {
                    Arc::new(repeat_n(Some(value.as_slice()), size).collect::<BinaryArray>())
                }
                None => Arc::new(repeat_n(None::<&str>, size).collect::<BinaryArray>()),
            },
            Scalar::Boolean(e) => Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef,
        })
    }

    pub fn try_from_array(array: &dyn Array, index: usize) -> ILResult<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return Self::try_new_null(array.data_type());
        }
        Ok(match array.data_type() {
            DataType::Int16 => {
                let array = array
                    .as_primitive_opt::<Int16Type>()
                    .expect("Failed to cast array");
                Scalar::Int16(Some(array.value(index)))
            }
            DataType::Int32 => {
                let array = array
                    .as_primitive_opt::<Int32Type>()
                    .expect("Failed to cast array");
                Scalar::Int32(Some(array.value(index)))
            }
            DataType::Int64 => {
                let array = array
                    .as_primitive_opt::<Int64Type>()
                    .expect("Failed to cast array");
                Scalar::Int64(Some(array.value(index)))
            }
            DataType::Float32 => {
                let array = array
                    .as_primitive_opt::<Float32Type>()
                    .expect("Failed to cast array");
                Scalar::Float32(Some(array.value(index)))
            }
            DataType::Float64 => {
                let array = array
                    .as_primitive_opt::<Float64Type>()
                    .expect("Failed to cast array");
                Scalar::Float64(Some(array.value(index)))
            }
            DataType::Utf8 => {
                let array = array.as_string_opt::<i32>().expect("Failed to cast array");
                Scalar::Utf8(Some(array.value(index).to_string()))
            }
            DataType::Binary => {
                let array = array.as_binary_opt::<i32>().expect("Failed to cast array");
                Scalar::Binary(Some(array.value(index).to_vec()))
            }
            DataType::Boolean => {
                let array = array.as_boolean_opt().expect("Failed to cast array");
                Scalar::Boolean(Some(array.value(index)))
            }
            _ => todo!(),
        })
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Scalar::Int16(v1), Scalar::Int16(v2)) => v1.eq(v2),
            (Scalar::Int16(_), _) => false,
            (Scalar::Int32(v1), Scalar::Int32(v2)) => v1.eq(v2),
            (Scalar::Int32(_), _) => false,
            (Scalar::Int64(v1), Scalar::Int64(v2)) => v1.eq(v2),
            (Scalar::Int64(_), _) => false,
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
            (Scalar::Binary(v1), Scalar::Binary(v2)) => v1.eq(v2),
            (Scalar::Binary(_), _) => false,
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.eq(v2),
            (Scalar::Boolean(_), _) => false,
        }
    }
}

impl Eq for Scalar {}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Scalar::Int16(v1), Scalar::Int16(v2)) => v1.partial_cmp(v2),
            (Scalar::Int16(_), _) => None,
            (Scalar::Int32(v1), Scalar::Int32(v2)) => v1.partial_cmp(v2),
            (Scalar::Int32(_), _) => None,
            (Scalar::Int64(v1), Scalar::Int64(v2)) => v1.partial_cmp(v2),
            (Scalar::Int64(_), _) => None,
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
            (Scalar::Binary(v1), Scalar::Binary(v2)) => v1.partial_cmp(v2),
            (Scalar::Binary(_), _) => None,
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.partial_cmp(v2),
            (Scalar::Boolean(_), _) => None,
        }
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Int16(Some(value)) => write!(f, "{}", value),
            Scalar::Int16(None) => write!(f, "null"),
            Scalar::Int32(Some(value)) => write!(f, "{}", value),
            Scalar::Int32(None) => write!(f, "null"),
            Scalar::Int64(Some(value)) => write!(f, "{}", value),
            Scalar::Int64(None) => write!(f, "null"),
            Scalar::Float32(Some(value)) => write!(f, "{}", value),
            Scalar::Float32(None) => write!(f, "null"),
            Scalar::Float64(Some(value)) => write!(f, "{}", value),
            Scalar::Float64(None) => write!(f, "null"),
            Scalar::Utf8(Some(value)) => write!(f, "{}", value),
            Scalar::Utf8(None) => write!(f, "null"),
            Scalar::Binary(Some(value)) => write!(f, "{}", hex::encode(value)),
            Scalar::Binary(None) => write!(f, "null"),
            Scalar::Boolean(Some(value)) => write!(f, "{}", value),
            Scalar::Boolean(None) => write!(f, "null"),
        }
    }
}
