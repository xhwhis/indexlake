use std::{fmt::Display, sync::Arc};

#[derive(Debug, Copy, Clone)]
pub enum CatalogDataType {
    Integer,
    BigInt,
    Float,
    Double,
    Varchar,
    Varbinary,
    Boolean,
}

impl std::fmt::Display for CatalogDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogDataType::Integer => write!(f, "INTEGER"),
            CatalogDataType::BigInt => write!(f, "BIGINT"),
            CatalogDataType::Float => write!(f, "FLOAT"),
            CatalogDataType::Double => write!(f, "DOUBLE"),
            CatalogDataType::Varchar => write!(f, "VARCHAR"),
            CatalogDataType::Varbinary => write!(f, "VARBINARY"),
            CatalogDataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
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
    Integer(Option<i32>),
    BigInt(Option<i64>),
    Float(Option<f32>),
    Double(Option<f64>),
    Varchar(Option<String>),
    Varbinary(Option<Vec<u8>>),
    Boolean(Option<bool>),
}

impl Display for CatalogScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogScalar::Integer(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Integer(None) => write!(f, "null"),
            CatalogScalar::BigInt(Some(value)) => write!(f, "{}", value),
            CatalogScalar::BigInt(None) => write!(f, "null"),
            CatalogScalar::Float(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Float(None) => write!(f, "null"),
            CatalogScalar::Double(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Double(None) => write!(f, "null"),
            CatalogScalar::Varchar(Some(value)) => write!(f, "'{}'", value),
            CatalogScalar::Varchar(None) => write!(f, "null"),
            CatalogScalar::Varbinary(Some(value)) => write!(f, "X'{}'", hex::encode(value)),
            CatalogScalar::Varbinary(None) => write!(f, "null"),
            CatalogScalar::Boolean(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Boolean(None) => write!(f, "null"),
        }
    }
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

    pub fn bigint(&self, index: usize) -> Option<i64> {
        match self.values[index] {
            CatalogScalar::BigInt(v) => v,
            _ => panic!("Expected BigInt at index {index} for row {self:?}"),
        }
    }
}

pub fn pretty_print_catalog_rows(
    schema_opt: Option<CatalogSchemaRef>,
    rows: &[CatalogRow],
) -> impl Display {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");

    let schema_opt = schema_opt.or_else(|| {
        if rows.is_empty() {
            return None;
        } else {
            Some(rows[0].schema.clone())
        }
    });
    if let Some(schema) = schema_opt {
        let mut header = Vec::new();
        for column in schema.columns.iter() {
            header.push(column.name.clone());
        }
        table.set_header(header);
    }

    if rows.is_empty() {
        return table;
    }

    for row in rows {
        let mut cells = Vec::new();
        for value in row.values.iter() {
            cells.push(value.to_string());
        }
        table.add_row(cells);
    }
    table
}
