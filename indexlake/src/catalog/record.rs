use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub enum CatalogDataType {
    BigInt,
    Varchar,
    Boolean,
}

#[derive(Debug)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: CatalogDataType,
    pub nullable: bool,
}

impl CatalogColumn {
    pub fn new(name: impl Into<String>, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

pub type CatalogSchemaRef = Arc<CatalogSchema>;

#[derive(Debug)]
pub struct CatalogSchema {
    pub columns: Vec<CatalogColumn>,
}

impl CatalogSchema {
    pub fn new(columns: Vec<CatalogColumn>) -> Self {
        Self { columns }
    }
}

#[derive(Debug)]
pub enum CatalogScalar {
    BigInt(Option<i64>),
    Varchar(Option<String>),
    Boolean(Option<bool>),
}

#[derive(Debug)]
pub struct CatalogRow {
    pub schema: CatalogSchemaRef,
    pub values: Vec<CatalogScalar>,
}

impl CatalogRow {
    pub fn new(schema: CatalogSchemaRef, values: Vec<CatalogScalar>) -> Self {
        assert_eq!(schema.columns.len(), values.len());
        Self { schema, values }
    }
}
