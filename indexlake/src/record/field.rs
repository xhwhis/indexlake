use arrow::datatypes::{DataType, Field};

use crate::{catalog::CatalogDatabase, record::CatalogDataType};
use std::{collections::HashMap, sync::LazyLock};

pub static INTERNAL_ROW_ID_FIELD_NAME: &str = "_indexlake_row_id";
pub static INTERNAL_ROW_ID_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::new(INTERNAL_ROW_ID_FIELD_NAME, DataType::Int64, false));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: CatalogDataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

pub(crate) fn sql_identifier(ident: &str, database: CatalogDatabase) -> String {
    match database {
        CatalogDatabase::Sqlite => format!("`{}`", ident),
        CatalogDatabase::Postgres => format!("\"{}\"", ident),
    }
}
