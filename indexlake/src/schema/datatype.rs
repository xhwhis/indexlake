use std::str::FromStr;

use crate::ILError;

#[derive(Debug, Clone)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Boolean,
}

impl FromStr for DataType {
    type Err = ILError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "INT32" => Ok(DataType::Int32),
            "INT64" => Ok(DataType::Int64),
            "FLOAT32" => Ok(DataType::Float32),
            "FLOAT64" => Ok(DataType::Float64),
            "UTF8" => Ok(DataType::Utf8),
            "BINARY" => Ok(DataType::Binary),
            "BOOLEAN" => Ok(DataType::Boolean),
            _ => Err(ILError::NotSupported(format!("Unsupported data type: {s}"))),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int32 => write!(f, "INT32"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Float32 => write!(f, "FLOAT32"),
            DataType::Float64 => write!(f, "FLOAT64"),
            DataType::Utf8 => write!(f, "UTF8"),
            DataType::Binary => write!(f, "BINARY"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}
