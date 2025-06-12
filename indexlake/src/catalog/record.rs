use std::{fmt::Display, sync::Arc};

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

impl Display for CatalogScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogScalar::BigInt(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Varchar(Some(value)) => write!(f, "{}", value),
            CatalogScalar::Boolean(Some(value)) => write!(f, "{}", value),
            _ => write!(f, "null"),
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
