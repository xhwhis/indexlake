mod datatype;
mod field;

pub use datatype::*;
pub use field::*;

use std::collections::HashMap;
use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone)]
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
}
