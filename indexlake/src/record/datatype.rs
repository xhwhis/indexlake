use std::str::FromStr;

use crate::ILError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Varchar,
    Varbinary,
    Boolean,
}

impl FromStr for DataType {
    type Err = ILError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "INTEGER" => Ok(DataType::Integer),
            "BIGINT" => Ok(DataType::BigInt),
            "FLOAT" => Ok(DataType::Float),
            "DOUBLE" => Ok(DataType::Double),
            "VARCHAR" => Ok(DataType::Varchar),
            "VARBINARY" => Ok(DataType::Varbinary),
            "BOOLEAN" => Ok(DataType::Boolean),
            _ => Err(ILError::NotSupported(format!("Unsupported data type: {s}"))),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Varchar => write!(f, "VARCHAR"),
            DataType::Varbinary => write!(f, "VARBINARY"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}
