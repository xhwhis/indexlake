use crate::record::DataType;
use std::collections::HashMap;

pub static INTERNAL_ROW_ID_FIELD_NAME: &str = "_indexlake_row_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl Field {
    pub fn new(
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            default_value,
            metadata: HashMap::new(),
        }
    }
}
