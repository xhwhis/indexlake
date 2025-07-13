use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow::datatypes::{FieldRef, SchemaRef};

use crate::{
    ILError, ILResult,
    catalog::{FieldRecord, IndexRecord, TableRecord, TransactionHelper},
    index::{IndexDefination, IndexParams},
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
            max_row_id: 0,
        })
        .await?;

    let max_field_id = tx_helper.get_max_field_id().await?;
    let field_ids = (max_field_id + 1..max_field_id + 1 + creation.schema.fields.len() as i64)
        .collect::<Vec<_>>();
    let mut field_records = Vec::new();
    for (field_id, field) in field_ids.iter().zip(creation.schema.fields()) {
        field_records.push(FieldRecord::new(*field_id, table_id, field));
    }
    tx_helper.insert_fields(&field_records).await?;

    tx_helper
        .create_inline_row_table(table_id, creation.schema.fields())
        .await?;

    Ok(table_id)
}

#[derive(Debug, Clone)]
pub struct IndexCreation {
    pub name: String,
    pub kind: String,
    pub key_columns: Vec<String>,
    pub params: Arc<dyn IndexParams>,
}

pub(crate) async fn process_create_index(
    tx_helper: &mut TransactionHelper,
    table: &mut Table,
    creation: IndexCreation,
) -> ILResult<i64> {
    let index_id = tx_helper.get_max_index_id().await? + 1;

    let index_def = IndexDefination {
        index_id,
        name: creation.name.clone(),
        kind: creation.kind.clone(),
        table_id: table.table_id,
        table_name: table.table_name.clone(),
        table_schema: table.schema.clone(),
        key_columns: creation.key_columns.clone(),
        params: creation.params.clone(),
    };

    let index = table
        .index_kinds
        .get(&creation.kind)
        .ok_or_else(|| ILError::InvalidInput(format!("Index kind {} not found", creation.kind)))?;
    index.supports(&index_def)?;

    if tx_helper
        .index_name_exists(table.table_id, &creation.name)
        .await?
    {
        return Err(ILError::InvalidInput(format!(
            "Index name {} already exists",
            creation.name
        )));
    }

    let key_field_ids = field_names_to_ids(&table.field_map, &creation.key_columns)?;

    tx_helper
        .insert_index(&IndexRecord {
            index_id,
            index_name: creation.name.clone(),
            index_kind: creation.kind.clone(),
            table_id: table.table_id,
            key_field_ids,
            params: creation.params.encode()?,
        })
        .await?;

    // TODO create index file
    table
        .indexes
        .insert(creation.name.clone(), Arc::new(index_def));

    Ok(index_id)
}

fn field_names_to_ids(field_map: &BTreeMap<i64, FieldRef>, names: &[String]) -> ILResult<Vec<i64>> {
    let mut field_ids = Vec::new();
    for name in names.iter() {
        let field_id_opt = field_map
            .iter()
            .find(|(_, field)| field.name() == name)
            .map(|(field_id, _)| *field_id);
        if let Some(field_id) = field_id_opt {
            field_ids.push(field_id);
        } else {
            return Err(ILError::InvalidInput(format!(
                "Field name {name} not found in table schema"
            )));
        }
    }
    Ok(field_ids)
}
