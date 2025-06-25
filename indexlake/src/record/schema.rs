use crate::record::{Field, INTERNAL_ROW_ID_FIELD_NAME};
use std::collections::HashMap;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub metadata: HashMap<String, String>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            fields,
            metadata: HashMap::new(),
        }
    }

    pub fn new_with_metadata(fields: Vec<Field>, metadata: HashMap<String, String>) -> Self {
        Self { fields, metadata }
    }

    pub fn without_row_id(&self) -> Self {
        let mut fields = self.fields.clone();
        for (i, field) in fields.iter().enumerate() {
            if field.name == INTERNAL_ROW_ID_FIELD_NAME {
                fields.remove(i);
                break;
            }
        }
        Self {
            fields,
            metadata: self.metadata.clone(),
        }
    }

    pub fn index_of(&self, field_name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field_name)
    }

    pub fn get_field_by_name(&self, field_name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == field_name)
    }

    pub fn project(&self, field_names: &[String]) -> Self {
        let mut projected_fields = Vec::new();
        for field in &self.fields {
            if field_names.contains(&field.name) {
                projected_fields.push(field.clone());
            }
        }
        Self {
            fields: projected_fields,
            metadata: self.metadata.clone(),
        }
    }
}
