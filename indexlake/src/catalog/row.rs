use std::fmt::Display;

use crate::{
    ILError, ILResult,
    catalog::{CatalogScalar, CatalogSchemaRef, INTERNAL_ROW_ID_FIELD_NAME},
};

#[derive(Debug)]
pub struct Row {
    pub schema: CatalogSchemaRef,
    pub values: Vec<CatalogScalar>,
}

impl Row {
    pub fn new(schema: CatalogSchemaRef, values: Vec<CatalogScalar>) -> Self {
        assert_eq!(schema.fields.len(), values.len());
        Self { schema, values }
    }

    pub fn get_row_id(&self) -> ILResult<Option<i64>> {
        let Some(idx) = self.schema.index_of(INTERNAL_ROW_ID_FIELD_NAME) else {
            return Ok(None);
        };
        self.int64(idx)
    }

    pub fn int16(&self, index: usize) -> ILResult<Option<i16>> {
        match self.values[index] {
            CatalogScalar::Int16(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Int16 at index {index} for row {self:?}"
            ))),
        }
    }
    pub fn int32(&self, index: usize) -> ILResult<Option<i32>> {
        match self.values[index] {
            CatalogScalar::Int32(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Int32 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn int64(&self, index: usize) -> ILResult<Option<i64>> {
        match self.values[index] {
            CatalogScalar::Int64(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected BigInt at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn float32(&self, index: usize) -> ILResult<Option<f32>> {
        match self.values[index] {
            CatalogScalar::Float32(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Float32 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn float64(&self, index: usize) -> ILResult<Option<f64>> {
        match self.values[index] {
            CatalogScalar::Float64(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Float64 at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn utf8(&self, index: usize) -> ILResult<Option<&String>> {
        match &self.values[index] {
            CatalogScalar::Utf8(v) => Ok(v.as_ref()),
            _ => Err(ILError::InternalError(format!(
                "Expected Varchar at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn binary(&self, index: usize) -> ILResult<Option<&Vec<u8>>> {
        match &self.values[index] {
            CatalogScalar::Binary(v) => Ok(v.as_ref()),
            _ => Err(ILError::InternalError(format!(
                "Expected Binary at index {index} for row {self:?}"
            ))),
        }
    }

    pub fn boolean(&self, index: usize) -> ILResult<Option<bool>> {
        match self.values[index] {
            CatalogScalar::Boolean(v) => Ok(v),
            _ => Err(ILError::InternalError(format!(
                "Expected Boolean at index {index} for row {self:?}"
            ))),
        }
    }
}

pub fn pretty_print_rows(schema_opt: Option<CatalogSchemaRef>, rows: &[Row]) -> impl Display {
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
