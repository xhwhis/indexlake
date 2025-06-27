use std::sync::Arc;

use crate::catalog::CatalogDatabase;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: CatalogDataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

pub type CatalogSchemaRef = Arc<CatalogSchema>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogSchema {
    pub fields: Vec<Column>,
}

impl CatalogSchema {
    pub fn new(fields: Vec<Column>) -> Self {
        Self { fields }
    }

    pub fn index_of(&self, field_name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field_name)
    }

    pub fn get_field_by_name(&self, field_name: &str) -> Option<&Column> {
        self.fields.iter().find(|f| f.name == field_name)
    }
}
