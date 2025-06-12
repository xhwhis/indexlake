use crate::CatalogColumn;
use crate::schema::DataType;
use std::collections::HashMap;

#[derive(Debug, Clone)]
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

    pub fn to_catalog_column(&self) -> CatalogColumn {
        CatalogColumn::new(
            self.name.clone(),
            self.data_type.to_catalog_data_type(),
            self.nullable,
        )
    }
}
