use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use crate::{
    ILError, ILResult,
    catalog::{IndexRecord, TableRecord, TransactionHelper},
    index::IndexDefination,
    table::{Table, TableConfig},
};

#[derive(Debug, Clone)]
pub struct TableCreation {
    pub namespace_name: String,
    pub table_name: String,
    pub schema: SchemaRef,
    pub config: TableConfig,
}

pub(crate) async fn process_create_table(
    tx_helper: &mut TransactionHelper,
    creation: TableCreation,
) -> ILResult<i64> {
    let namespace_id = tx_helper
        .get_namespace_id(&creation.namespace_name)
        .await?
        .ok_or_else(|| {
            ILError::CatalogError(format!("Namespace {} not found", creation.namespace_name))
        })?;

    if tx_helper
        .table_name_exists(namespace_id, &creation.table_name)
        .await?
    {
        return Err(ILError::InvalidInput(format!(
            "Table {} already exists in namespace {}",
            creation.table_name, creation.namespace_name
        )));
    }

    let max_table_id = tx_helper.get_max_table_id().await?;
    let table_id = max_table_id + 1;
    tx_helper
        .insert_table(&TableRecord {
            table_id,
            table_name: creation.table_name,
            namespace_id,
            config: creation.config,
        })
        .await?;

    let max_field_id = tx_helper.get_max_field_id().await?;
    let field_ids = (max_field_id + 1..max_field_id + 1 + creation.schema.fields.len() as i64)
        .collect::<Vec<_>>();
    tx_helper
        .insert_fields(table_id, &field_ids, creation.schema.fields())
        .await?;

    tx_helper.create_row_metadata_table(table_id).await?;
    tx_helper
        .create_inline_row_table(table_id, creation.schema.fields())
        .await?;

    Ok(table_id)
}

#[derive(Debug, Clone)]
pub struct IndexCreation {
    pub name: String,
    pub kind: String,
    pub key_column_names: Vec<String>,
    pub include_column_names: Vec<String>,
    pub config: HashMap<String, String>,
}

pub(crate) async fn process_create_index(
    tx_helper: &mut TransactionHelper,
    table: &Table,
    creation: IndexCreation,
) -> ILResult<i64> {
    let mut key_column_indexes = Vec::new();
    for field_name in creation.key_column_names.iter() {
        key_column_indexes.push(table.schema.index_of(field_name)?);
    }

    let mut include_column_indexes = Vec::new();
    for field_name in creation.include_column_names.iter() {
        include_column_indexes.push(table.schema.index_of(field_name)?);
    }

    let index_def = IndexDefination {
        name: creation.name.clone(),
        kind: creation.kind.clone(),
        table_id: table.table_id,
        table_name: table.table_name.clone(),
        table_schema: table.schema.clone(),
        key_columns: key_column_indexes,
        include_columns: include_column_indexes,
        config: creation.config.clone(),
    };

    table.index_kind_manager.supports(&index_def)?;

    if tx_helper
        .index_name_exists(table.table_id, &creation.name)
        .await?
    {
        return Err(ILError::InvalidInput(format!(
            "Index name {} already exists",
            creation.name
        )));
    }

    let max_index_id = tx_helper.get_max_index_id().await?;
    let index_id = max_index_id + 1;

    let mut key_field_ids = Vec::new();
    for key_col_name in creation.key_column_names.iter() {
        let field_id_opt = table
            .field_map
            .iter()
            .find(|(_, field)| field.name() == key_col_name)
            .map(|(field_id, _)| *field_id);
        if let Some(field_id) = field_id_opt {
            key_field_ids.push(field_id);
        } else {
            return Err(ILError::InvalidInput(format!(
                "Key column name {key_col_name} not found in table schema"
            )));
        }
    }

    let mut include_field_ids = Vec::new();
    for include_col_name in creation.include_column_names.iter() {
        let field_id_opt = table
            .field_map
            .iter()
            .find(|(_, field)| field.name() == include_col_name)
            .map(|(field_id, _)| *field_id);
        if let Some(field_id) = field_id_opt {
            include_field_ids.push(field_id);
        } else {
            return Err(ILError::InvalidInput(format!(
                "Include column name {include_col_name} not found in table schema"
            )));
        }
    }

    tx_helper
        .insert_index(&IndexRecord {
            index_id,
            index_name: creation.name.clone(),
            index_kind: creation.kind.clone(),
            table_id: table.table_id,
            key_field_ids,
            include_field_ids,
            config: creation.config.clone(),
        })
        .await?;

    Ok(index_id)
}
