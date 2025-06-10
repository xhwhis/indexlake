use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub enum DataType {
    BigInt,
    Varchar,
    Boolean,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

pub type SchemaRef = Arc<Schema>;

#[derive(Debug)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}

#[derive(Debug)]
pub enum Scalar {
    BigInt(Option<i64>),
    Varchar(Option<String>),
    Boolean(Option<bool>),
}

#[derive(Debug)]
pub struct Row {
    pub schema: SchemaRef,
    pub values: Vec<Scalar>,
}

impl Row {
    pub fn new(schema: SchemaRef, values: Vec<Scalar>) -> Self {
        assert_eq!(schema.columns.len(), values.len());
        Self { schema, values }
    }
}
