use crate::record::Column;
use std::sync::Arc;

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
