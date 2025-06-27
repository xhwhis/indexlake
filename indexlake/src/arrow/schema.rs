use crate::{
    ILError, ILResult,
    record::{CatalogDataType, CatalogSchema, Column},
};
use arrow::datatypes::{DataType, Schema};

pub fn schema_to_catalog_schema(schema: &Schema) -> ILResult<CatalogSchema> {
    let mut fields = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let datatype = datatype_to_catalog_datatype(&field.data_type())?;
        fields.push(Column::new(
            field.name().clone(),
            datatype,
            field.is_nullable(),
        ));
    }
    Ok(CatalogSchema::new(fields))
}

pub fn schema_without_column(schema: &Schema, column_name: &str) -> ILResult<Schema> {
    let field_idx = schema.index_of(&column_name).map_err(|_e| {
        ILError::InternalError(format!(
            "Failed to find field {column_name} in schema: {schema:?}"
        ))
    })?;
    let mut fields = Vec::with_capacity(schema.fields.len() - 1);
    for (i, field) in schema.fields.iter().enumerate() {
        if i != field_idx {
            fields.push(field.clone());
        }
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata.clone()))
}

pub fn datatype_to_catalog_datatype(datatype: &DataType) -> ILResult<CatalogDataType> {
    match datatype {
        DataType::Int16 => Ok(CatalogDataType::Int16),
        DataType::Int32 => Ok(CatalogDataType::Int32),
        DataType::Int64 => Ok(CatalogDataType::Int64),
        DataType::Float32 => Ok(CatalogDataType::Float32),
        DataType::Float64 => Ok(CatalogDataType::Float64),
        DataType::Boolean => Ok(CatalogDataType::Boolean),
        DataType::Utf8 => Ok(CatalogDataType::Utf8),
        DataType::Binary => Ok(CatalogDataType::Binary),
        _ => Err(ILError::InternalError(format!(
            "Unsupported arrow datatype: {:?}",
            datatype
        ))),
    }
}
