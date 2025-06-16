use std::fmt::Display;

use crate::record::{Scalar, SchemaRef};

#[derive(Debug)]
pub struct Row {
    pub schema: SchemaRef,
    pub values: Vec<Scalar>,
}

impl Row {
    pub fn new(schema: SchemaRef, values: Vec<Scalar>) -> Self {
        assert_eq!(schema.fields.len(), values.len());
        Self { schema, values }
    }

    pub fn bigint(&self, index: usize) -> Option<i64> {
        match self.values[index] {
            Scalar::BigInt(v) => v,
            _ => panic!("Expected BigInt at index {index} for row {self:?}"),
        }
    }

    pub fn varchar(&self, index: usize) -> Option<String> {
        match &self.values[index] {
            Scalar::Varchar(v) => v.clone(),
            _ => panic!("Expected Varchar at index {index} for row {self:?}"),
        }
    }

    pub fn boolean(&self, index: usize) -> Option<bool> {
        match self.values[index] {
            Scalar::Boolean(v) => v,
            _ => panic!("Expected Boolean at index {index} for row {self:?}"),
        }
    }
}

pub fn pretty_print_rows(schema_opt: Option<SchemaRef>, rows: &[Row]) -> impl Display {
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
        for field in schema.fields.iter() {
            header.push(field.name.clone());
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
