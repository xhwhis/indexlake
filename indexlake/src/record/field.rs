use crate::{catalog::CatalogDatabase, record::DataType};
use std::{collections::HashMap, sync::LazyLock};

pub static INTERNAL_ROW_ID_FIELD_NAME: &str = "_indexlake_row_id";
pub static INTERNAL_ROW_ID_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::new(INTERNAL_ROW_ID_FIELD_NAME, DataType::Int64, false));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_default_value(mut self, default_value: Option<String>) -> Self {
        self.default_value = default_value;
        self
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
