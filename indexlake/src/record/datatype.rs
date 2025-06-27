use crate::{ILError, ILResult, catalog::CatalogDatabase};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogDataType {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Boolean,
}

impl CatalogDataType {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        match self {
            CatalogDataType::Int16 => "SMALLINT".to_string(),
            CatalogDataType::Int32 => "INTEGER".to_string(),
            CatalogDataType::Int64 => "BIGINT".to_string(),
            CatalogDataType::Float32 => match database {
                CatalogDatabase::Sqlite => "FLOAT".to_string(),
                CatalogDatabase::Postgres => "FLOAT4".to_string(),
            },
            CatalogDataType::Float64 => match database {
                CatalogDatabase::Sqlite => "DOUBLE".to_string(),
                CatalogDatabase::Postgres => "FLOAT8".to_string(),
            },
            CatalogDataType::Utf8 => "VARCHAR".to_string(),
            CatalogDataType::Binary => match database {
                CatalogDatabase::Sqlite => "BLOB".to_string(),
                CatalogDatabase::Postgres => "BYTEA".to_string(),
            },
            CatalogDataType::Boolean => "BOOLEAN".to_string(),
        }
    }

    pub(crate) fn parse_sql_type(s: &str, database: CatalogDatabase) -> ILResult<Self> {
        match (s, database) {
            ("SMALLINT", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Int16),
            ("INTEGER", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Int32),
            ("BIGINT", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Int64),
            ("FLOAT", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Float32),
            ("DOUBLE", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Float64),
            ("VARCHAR", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Utf8),
            ("BLOB", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Binary),
            ("BOOLEAN", CatalogDatabase::Sqlite) => Ok(CatalogDataType::Boolean),
            ("SMALLINT", CatalogDatabase::Postgres) => Ok(CatalogDataType::Int16),
            ("INTEGER", CatalogDatabase::Postgres) => Ok(CatalogDataType::Int32),
            ("BIGINT", CatalogDatabase::Postgres) => Ok(CatalogDataType::Int64),
            ("FLOAT4", CatalogDatabase::Postgres) => Ok(CatalogDataType::Float32),
            ("FLOAT8", CatalogDatabase::Postgres) => Ok(CatalogDataType::Float64),
            ("VARCHAR", CatalogDatabase::Postgres) => Ok(CatalogDataType::Utf8),
            ("BYTEA", CatalogDatabase::Postgres) => Ok(CatalogDataType::Binary),
            ("BOOLEAN", CatalogDatabase::Postgres) => Ok(CatalogDataType::Boolean),
            _ => Err(ILError::NotSupported(format!(
                "Unsupported data type: {s} for database: {database}"
            ))),
        }
    }
}

impl std::fmt::Display for CatalogDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogDataType::Int16 => write!(f, "Int16"),
            CatalogDataType::Int32 => write!(f, "Int32"),
            CatalogDataType::Int64 => write!(f, "Int64"),
            CatalogDataType::Float32 => write!(f, "Float32"),
            CatalogDataType::Float64 => write!(f, "Float64"),
            CatalogDataType::Utf8 => write!(f, "Utf8"),
            CatalogDataType::Binary => write!(f, "Binary"),
            CatalogDataType::Boolean => write!(f, "Boolean"),
        }
    }
}
