use std::fmt::Display;

use derive_visitor::{Drive, DriveMut};

use crate::{ILError, ILResult, catalog::CatalogDatabase};

#[derive(Debug, Clone, Drive, DriveMut)]
pub enum CatalogScalar {
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Binary(Option<Vec<u8>>),
    Boolean(Option<bool>),
}

impl CatalogScalar {
    pub fn is_null(&self) -> bool {
        match self {
            CatalogScalar::Int16(None) => true,
            CatalogScalar::Int16(Some(_)) => false,
            CatalogScalar::Int32(None) => true,
            CatalogScalar::Int32(Some(_)) => false,
            CatalogScalar::Int64(None) => true,
            CatalogScalar::Int64(Some(_)) => false,
            CatalogScalar::Float32(None) => true,
            CatalogScalar::Float32(Some(_)) => false,
            CatalogScalar::Float64(None) => true,
            CatalogScalar::Float64(Some(_)) => false,
            CatalogScalar::Utf8(None) => true,
            CatalogScalar::Utf8(Some(_)) => false,
            CatalogScalar::Binary(None) => true,
            CatalogScalar::Binary(Some(_)) => false,
            CatalogScalar::Boolean(None) => true,
            CatalogScalar::Boolean(Some(_)) => false,
        }
    }

    pub fn is_true(&self) -> bool {
        match self {
            CatalogScalar::Boolean(Some(true)) => true,
            _ => false,
        }
    }

    pub fn to_sql(&self, database: CatalogDatabase) -> String {
        match self {
            CatalogScalar::Int16(Some(value)) => value.to_string(),
            CatalogScalar::Int16(None) => "null".to_string(),
            CatalogScalar::Int32(Some(value)) => value.to_string(),
            CatalogScalar::Int32(None) => "null".to_string(),
            CatalogScalar::Int64(Some(value)) => value.to_string(),
            CatalogScalar::Int64(None) => "null".to_string(),
            CatalogScalar::Float32(Some(value)) => value.to_string(),
            CatalogScalar::Float32(None) => "null".to_string(),
            CatalogScalar::Float64(Some(value)) => value.to_string(),
            CatalogScalar::Float64(None) => "null".to_string(),
            CatalogScalar::Utf8(Some(value)) => format!("'{}'", value),
            CatalogScalar::Utf8(None) => "null".to_string(),
            CatalogScalar::Binary(Some(value)) => match database {
                CatalogDatabase::Sqlite => format!("X'{}'", hex::encode(value)),
                CatalogDatabase::Postgres => format!("E'\\\\x{}'", hex::encode(value)),
            },
            CatalogScalar::Binary(None) => "null".to_string(),
            CatalogScalar::Boolean(Some(value)) => value.to_string(),
            CatalogScalar::Boolean(None) => "null".to_string(),
        }
    }

    pub fn add(&self, other: &CatalogScalar) -> ILResult<CatalogScalar> {
        match (self, other) {
            (CatalogScalar::Int16(Some(v1)), CatalogScalar::Int16(Some(v2))) => {
                Ok(CatalogScalar::Int16(Some(v1 + v2)))
            }
            (CatalogScalar::Int16(None), _) | (_, CatalogScalar::Int16(None)) => {
                Ok(CatalogScalar::Int16(None))
            }
            (CatalogScalar::Int32(Some(v1)), CatalogScalar::Int32(Some(v2))) => {
                Ok(CatalogScalar::Int32(Some(v1 + v2)))
            }
            (CatalogScalar::Int32(None), _) | (_, CatalogScalar::Int32(None)) => {
                Ok(CatalogScalar::Int32(None))
            }
            (CatalogScalar::Int64(Some(v1)), CatalogScalar::Int64(Some(v2))) => {
                Ok(CatalogScalar::Int64(Some(v1 + v2)))
            }
            (CatalogScalar::Int64(None), _) | (_, CatalogScalar::Int64(None)) => {
                Ok(CatalogScalar::Int64(None))
            }
            (CatalogScalar::Float32(Some(v1)), CatalogScalar::Float32(Some(v2))) => {
                Ok(CatalogScalar::Float32(Some(v1 + v2)))
            }
            (CatalogScalar::Float32(None), _) | (_, CatalogScalar::Float32(None)) => {
                Ok(CatalogScalar::Float32(None))
            }
            (CatalogScalar::Float64(Some(v1)), CatalogScalar::Float64(Some(v2))) => {
                Ok(CatalogScalar::Float64(Some(v1 + v2)))
            }
            (CatalogScalar::Float64(None), _) | (_, CatalogScalar::Float64(None)) => {
                Ok(CatalogScalar::Float64(None))
            }
            _ => Err(ILError::InvalidInput(format!(
                "Cannot add scalars: {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn sub(&self, other: &CatalogScalar) -> ILResult<CatalogScalar> {
        match (self, other) {
            (CatalogScalar::Int16(Some(v1)), CatalogScalar::Int16(Some(v2))) => {
                Ok(CatalogScalar::Int16(Some(v1 - v2)))
            }
            (CatalogScalar::Int16(None), _) | (_, CatalogScalar::Int16(None)) => {
                Ok(CatalogScalar::Int16(None))
            }
            (CatalogScalar::Int32(Some(v1)), CatalogScalar::Int32(Some(v2))) => {
                Ok(CatalogScalar::Int32(Some(v1 - v2)))
            }
            (CatalogScalar::Int32(None), _) | (_, CatalogScalar::Int32(None)) => {
                Ok(CatalogScalar::Int32(None))
            }
            (CatalogScalar::Int64(Some(v1)), CatalogScalar::Int64(Some(v2))) => {
                Ok(CatalogScalar::Int64(Some(v1 - v2)))
            }
            (CatalogScalar::Int64(None), _) | (_, CatalogScalar::Int64(None)) => {
                Ok(CatalogScalar::Int64(None))
            }
            (CatalogScalar::Float32(Some(v1)), CatalogScalar::Float32(Some(v2))) => {
                Ok(CatalogScalar::Float32(Some(v1 - v2)))
            }
            (CatalogScalar::Float32(None), _) | (_, CatalogScalar::Float32(None)) => {
                Ok(CatalogScalar::Float32(None))
            }
            (CatalogScalar::Float64(Some(v1)), CatalogScalar::Float64(Some(v2))) => {
                Ok(CatalogScalar::Float64(Some(v1 - v2)))
            }
            (CatalogScalar::Float64(None), _) | (_, CatalogScalar::Float64(None)) => {
                Ok(CatalogScalar::Float64(None))
            }
            _ => Err(ILError::InvalidInput(format!(
                "Cannot subtract scalars: {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn mul(&self, other: &CatalogScalar) -> ILResult<CatalogScalar> {
        match (self, other) {
            (CatalogScalar::Int16(Some(v1)), CatalogScalar::Int16(Some(v2))) => {
                Ok(CatalogScalar::Int16(Some(v1 * v2)))
            }
            (CatalogScalar::Int16(None), _) | (_, CatalogScalar::Int16(None)) => {
                Ok(CatalogScalar::Int16(None))
            }
            (CatalogScalar::Int32(Some(v1)), CatalogScalar::Int32(Some(v2))) => {
                Ok(CatalogScalar::Int32(Some(v1 * v2)))
            }
            (CatalogScalar::Int32(None), _) | (_, CatalogScalar::Int32(None)) => {
                Ok(CatalogScalar::Int32(None))
            }
            (CatalogScalar::Int64(Some(v1)), CatalogScalar::Int64(Some(v2))) => {
                Ok(CatalogScalar::Int64(Some(v1 * v2)))
            }
            (CatalogScalar::Int64(None), _) | (_, CatalogScalar::Int64(None)) => {
                Ok(CatalogScalar::Int64(None))
            }
            (CatalogScalar::Float32(Some(v1)), CatalogScalar::Float32(Some(v2))) => {
                Ok(CatalogScalar::Float32(Some(v1 * v2)))
            }
            (CatalogScalar::Float32(None), _) | (_, CatalogScalar::Float32(None)) => {
                Ok(CatalogScalar::Float32(None))
            }
            (CatalogScalar::Float64(Some(v1)), CatalogScalar::Float64(Some(v2))) => {
                Ok(CatalogScalar::Float64(Some(v1 * v2)))
            }
            (CatalogScalar::Float64(None), _) | (_, CatalogScalar::Float64(None)) => {
                Ok(CatalogScalar::Float64(None))
            }
            _ => Err(ILError::InvalidInput(format!(
                "Cannot multiply scalars: {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn div(&self, other: &CatalogScalar) -> ILResult<CatalogScalar> {
        match (self, other) {
            (CatalogScalar::Int16(Some(v1)), CatalogScalar::Int16(Some(v2))) => {
                Ok(CatalogScalar::Int16(Some(v1 / v2)))
            }
            (CatalogScalar::Int16(None), _) | (_, CatalogScalar::Int16(None)) => {
                Ok(CatalogScalar::Int16(None))
            }
            (CatalogScalar::Int32(Some(v1)), CatalogScalar::Int32(Some(v2))) => {
                Ok(CatalogScalar::Int32(Some(v1 / v2)))
            }
            (CatalogScalar::Int32(None), _) | (_, CatalogScalar::Int32(None)) => {
                Ok(CatalogScalar::Int32(None))
            }
            (CatalogScalar::Int64(Some(v1)), CatalogScalar::Int64(Some(v2))) => {
                Ok(CatalogScalar::Int64(Some(v1 / v2)))
            }
            (CatalogScalar::Int64(None), _) | (_, CatalogScalar::Int64(None)) => {
                Ok(CatalogScalar::Int64(None))
            }
            (CatalogScalar::Float32(Some(v1)), CatalogScalar::Float32(Some(v2))) => {
                Ok(CatalogScalar::Float32(Some(v1 / v2)))
            }
            (CatalogScalar::Float32(None), _) | (_, CatalogScalar::Float32(None)) => {
                Ok(CatalogScalar::Float32(None))
            }
            (CatalogScalar::Float64(Some(v1)), CatalogScalar::Float64(Some(v2))) => {
                Ok(CatalogScalar::Float64(Some(v1 / v2)))
            }
            (CatalogScalar::Float64(None), _) | (_, CatalogScalar::Float64(None)) => {
                Ok(CatalogScalar::Float64(None))
            }
            _ => Err(ILError::InvalidInput(format!(
                "Cannot divide scalars: {:?} and {:?}",
                self, other
            ))),
        }
    }

    pub fn rem(&self, other: &CatalogScalar) -> ILResult<CatalogScalar> {
        match (self, other) {
            (CatalogScalar::Int16(Some(v1)), CatalogScalar::Int16(Some(v2))) => {
                Ok(CatalogScalar::Int16(Some(v1 % v2)))
            }
            (CatalogScalar::Int16(None), _) | (_, CatalogScalar::Int16(None)) => {
                Ok(CatalogScalar::Int16(None))
            }
            (CatalogScalar::Int32(Some(v1)), CatalogScalar::Int32(Some(v2))) => {
                Ok(CatalogScalar::Int32(Some(v1 % v2)))
            }
            (CatalogScalar::Int32(None), _) | (_, CatalogScalar::Int32(None)) => {
                Ok(CatalogScalar::Int32(None))
            }
            (CatalogScalar::Int64(Some(v1)), CatalogScalar::Int64(Some(v2))) => {
                Ok(CatalogScalar::Int64(Some(v1 % v2)))
            }
            (CatalogScalar::Int64(None), _) | (_, CatalogScalar::Int64(None)) => {
                Ok(CatalogScalar::Int64(None))
            }
            (CatalogScalar::Float32(Some(v1)), CatalogScalar::Float32(Some(v2))) => {
                Ok(CatalogScalar::Float32(Some(v1 % v2)))
            }
            (CatalogScalar::Float32(None), _) | (_, CatalogScalar::Float32(None)) => {
                Ok(CatalogScalar::Float32(None))
            }
            (CatalogScalar::Float64(Some(v1)), CatalogScalar::Float64(Some(v2))) => {
                Ok(CatalogScalar::Float64(Some(v1 % v2)))
            }
            (CatalogScalar::Float64(None), _) | (_, CatalogScalar::Float64(None)) => {
                Ok(CatalogScalar::Float64(None))
            }
            _ => Err(ILError::InvalidInput(format!(
                "Cannot modulo scalars: {:?} and {:?}",
                self, other
            ))),
        }
    }
}

impl PartialEq for CatalogScalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CatalogScalar::Int16(v1), CatalogScalar::Int16(v2)) => v1.eq(v2),
            (CatalogScalar::Int16(_), _) => false,
            (CatalogScalar::Int32(v1), CatalogScalar::Int32(v2)) => v1.eq(v2),
            (CatalogScalar::Int32(_), _) => false,
            (CatalogScalar::Int64(v1), CatalogScalar::Int64(v2)) => v1.eq(v2),
            (CatalogScalar::Int64(_), _) => false,
            (CatalogScalar::Float32(v1), CatalogScalar::Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (CatalogScalar::Float32(_), _) => false,
            (CatalogScalar::Float64(v1), CatalogScalar::Float64(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.to_bits() == d2.to_bits(),
                _ => v1.eq(v2),
            },
            (CatalogScalar::Float64(_), _) => false,
            (CatalogScalar::Utf8(v1), CatalogScalar::Utf8(v2)) => v1.eq(v2),
            (CatalogScalar::Utf8(_), _) => false,
            (CatalogScalar::Binary(v1), CatalogScalar::Binary(v2)) => v1.eq(v2),
            (CatalogScalar::Binary(_), _) => false,
            (CatalogScalar::Boolean(v1), CatalogScalar::Boolean(v2)) => v1.eq(v2),
            (CatalogScalar::Boolean(_), _) => false,
        }
    }
}

impl Eq for CatalogScalar {}

impl PartialOrd for CatalogScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (CatalogScalar::Int16(v1), CatalogScalar::Int16(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Int16(_), _) => None,
            (CatalogScalar::Int32(v1), CatalogScalar::Int32(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Int32(_), _) => None,
            (CatalogScalar::Int64(v1), CatalogScalar::Int64(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Int64(_), _) => None,
            (CatalogScalar::Float32(v1), CatalogScalar::Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.partial_cmp(f2),
                _ => v1.partial_cmp(v2),
            },
            (CatalogScalar::Float32(_), _) => None,
            (CatalogScalar::Float64(v1), CatalogScalar::Float64(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.partial_cmp(d2),
                _ => v1.partial_cmp(v2),
            },
            (CatalogScalar::Float64(_), _) => None,
            (CatalogScalar::Utf8(v1), CatalogScalar::Utf8(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Utf8(_), _) => None,
            (CatalogScalar::Binary(v1), CatalogScalar::Binary(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Binary(_), _) => None,
            (CatalogScalar::Boolean(v1), CatalogScalar::Boolean(v2)) => v1.partial_cmp(v2),
            (CatalogScalar::Boolean(_), _) => None,
        }
    }
}

impl Display for CatalogScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogScalar::Int16(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Int16(None) => write!(f, "null"),
            CatalogScalar::Int32(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Int32(None) => write!(f, "null"),
            CatalogScalar::Int64(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Int64(None) => write!(f, "null"),
            CatalogScalar::Float32(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Float32(None) => write!(f, "null"),
            CatalogScalar::Float64(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Float64(None) => write!(f, "null"),
            CatalogScalar::Utf8(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Utf8(None) => write!(f, "null"),
            CatalogScalar::Binary(Some(value)) => write!(f, "{}", hex::encode(value)),
            CatalogScalar::Binary(None) => write!(f, "null"),
            CatalogScalar::Boolean(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Boolean(None) => write!(f, "null"),
        }
    }
}
