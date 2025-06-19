use std::str::FromStr;

use crate::{CatalogDatabase, ILError, ILResult};

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

impl DataType {
    pub(crate) fn to_sql_type(&self, database: CatalogDatabase) -> String {
        match self {
            DataType::Integer => "INTEGER".to_string(),
            DataType::BigInt => "BIGINT".to_string(),
            DataType::Float => match database {
                CatalogDatabase::Sqlite => "FLOAT".to_string(),
                CatalogDatabase::Postgres => "FLOAT4".to_string(),
            },
            DataType::Double => match database {
                CatalogDatabase::Sqlite => "DOUBLE".to_string(),
                CatalogDatabase::Postgres => "FLOAT8".to_string(),
            },
            DataType::Varchar => "VARCHAR".to_string(),
            DataType::Varbinary => "VARBINARY".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
        }
    }

    pub(crate) fn parse_sql_type(s: &str, database: CatalogDatabase) -> ILResult<Self> {
        match (s, database) {
            ("INTEGER", CatalogDatabase::Sqlite) => Ok(DataType::Integer),
            ("INTEGER", CatalogDatabase::Postgres) => Ok(DataType::Integer),
            ("BIGINT", CatalogDatabase::Sqlite) => Ok(DataType::BigInt),
            ("BIGINT", CatalogDatabase::Postgres) => Ok(DataType::BigInt),
            ("FLOAT", CatalogDatabase::Sqlite) => Ok(DataType::Float),
            ("FLOAT4", CatalogDatabase::Postgres) => Ok(DataType::Float),
            ("DOUBLE", CatalogDatabase::Sqlite) => Ok(DataType::Double),
            ("FLOAT8", CatalogDatabase::Postgres) => Ok(DataType::Double),
            ("VARCHAR", CatalogDatabase::Sqlite) => Ok(DataType::Varchar),
            ("VARCHAR", CatalogDatabase::Postgres) => Ok(DataType::Varchar),
            ("VARBINARY", CatalogDatabase::Sqlite) => Ok(DataType::Varbinary),
            ("VARBINARY", CatalogDatabase::Postgres) => Ok(DataType::Varbinary),
            ("BOOLEAN", CatalogDatabase::Sqlite) => Ok(DataType::Boolean),
            ("BOOLEAN", CatalogDatabase::Postgres) => Ok(DataType::Boolean),
            _ => Err(ILError::NotSupported(format!(
                "Unsupported data type: {s} for database: {database}"
            ))),
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
