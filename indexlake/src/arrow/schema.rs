use crate::{
    ILError, ILResult,
    record::{CatalogDataType, CatalogSchema, Column},
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

pub fn arrow_schema_to_schema(arrow_schema: &ArrowSchema) -> ILResult<CatalogSchema> {
    let mut fields = Vec::with_capacity(arrow_schema.fields.len());
    for arrow_field in arrow_schema.fields.iter() {
        let datatype = arrow_datatype_to_datatype(&arrow_field.data_type())?;
        fields.push(
            Column::new(
                arrow_field.name().clone(),
                datatype,
                arrow_field.is_nullable(),
            )
            .with_metadata(arrow_field.metadata().clone()),
        );
    }
    Ok(CatalogSchema::new_with_metadata(
        fields,
        arrow_schema.metadata.clone(),
    ))
}

pub fn schema_to_arrow_schema(schema: &CatalogSchema) -> ILResult<ArrowSchema> {
    let mut arrow_fields = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let arrow_datatype = datatype_to_arrow_datatype(&field.data_type);
        arrow_fields.push(
            ArrowField::new(field.name.clone(), arrow_datatype, field.nullable)
                .with_metadata(field.metadata.clone()),
        );
    }
    Ok(ArrowSchema::new_with_metadata(
        arrow_fields,
        schema.metadata.clone(),
    ))
}

pub fn arrow_schema_without_column(
    arrow_schema: &ArrowSchema,
    column_name: &str,
) -> ILResult<ArrowSchema> {
    let arrow_col_idx = arrow_schema.index_of(&column_name).map_err(|_e| {
        ILError::InternalError(format!(
            "Failed to find field {column_name} in arrow schema: {arrow_schema:?}"
        ))
    })?;
    let mut arrow_fields = Vec::with_capacity(arrow_schema.fields.len() - 1);
    for (i, field) in arrow_schema.fields.iter().enumerate() {
        if i != arrow_col_idx {
            arrow_fields.push(field.clone());
        }
    }
    Ok(ArrowSchema::new_with_metadata(
        arrow_fields,
        arrow_schema.metadata.clone(),
    ))
}

pub fn arrow_datatype_to_datatype(datatype: &ArrowDataType) -> ILResult<CatalogDataType> {
    match datatype {
        ArrowDataType::Int16 => Ok(CatalogDataType::Int16),
        ArrowDataType::Int32 => Ok(CatalogDataType::Int32),
        ArrowDataType::Int64 => Ok(CatalogDataType::Int64),
        ArrowDataType::Float32 => Ok(CatalogDataType::Float32),
        ArrowDataType::Float64 => Ok(CatalogDataType::Float64),
        ArrowDataType::Boolean => Ok(CatalogDataType::Boolean),
        ArrowDataType::Utf8 => Ok(CatalogDataType::Utf8),
        ArrowDataType::Binary => Ok(CatalogDataType::Binary),
        _ => Err(ILError::InternalError(format!(
            "Unsupported arrow datatype: {:?}",
            datatype
        ))),
    }
}

pub fn datatype_to_arrow_datatype(datatype: &CatalogDataType) -> ArrowDataType {
    match datatype {
        CatalogDataType::Int16 => ArrowDataType::Int16,
        CatalogDataType::Int32 => ArrowDataType::Int32,
        CatalogDataType::Int64 => ArrowDataType::Int64,
        CatalogDataType::Float32 => ArrowDataType::Float32,
        CatalogDataType::Float64 => ArrowDataType::Float64,
        CatalogDataType::Boolean => ArrowDataType::Boolean,
        CatalogDataType::Utf8 => ArrowDataType::Utf8,
        CatalogDataType::Binary => ArrowDataType::Binary,
    }
}
