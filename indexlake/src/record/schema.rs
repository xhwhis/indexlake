use crate::record::Field;
use std::collections::HashMap;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn push_front(&mut self, field: Field) {
        self.fields.insert(0, field);
    }

    pub fn push_back(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn index_of(&self, field_name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field_name)
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
