use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::{FieldRef, SchemaRef};
use futures::StreamExt;
use uuid::Uuid;

use crate::{
    ILError, ILResult,
    catalog::{FieldRecord, IndexFileRecord, IndexRecord, TableRecord, TransactionHelper},
    index::{IndexDefination, IndexParams},
    storage::read_data_file_by_record,
    table::{Table, TableConfig},
};

#[derive(Debug, Clone)]
pub struct TableCreation {
    pub namespace_name: String,
    pub table_name: String,
    pub schema: SchemaRef,
    pub config: TableConfig,
    pub if_not_exists: bool,
}

pub(crate) async fn process_create_table(
    tx_helper: &mut TransactionHelper,
    creation: TableCreation,
) -> ILResult<Uuid> {
    let namespace_id = tx_helper
        .get_namespace_id(&creation.namespace_name)
        .await?
        .ok_or_else(|| {
            ILError::InvalidInput(format!("Namespace {} not found", creation.namespace_name))
        })?;

    if let Some(table_id) = tx_helper
        .get_table_id(&namespace_id, &creation.table_name)
        .await?
    {
        return if creation.if_not_exists {
            Ok(table_id)
        } else {
            Err(ILError::InvalidInput(format!(
                "Table {} already exists in namespace {}",
                creation.table_name, creation.namespace_name
            )))
        };
    }

    let table_id = Uuid::now_v7();
    tx_helper
        .insert_table(&TableRecord {
            table_id,
            table_name: creation.table_name,
            namespace_id,
            config: creation.config,
        })
        .await?;

    let mut field_records = Vec::new();
    for field in creation.schema.fields() {
        field_records.push(FieldRecord::new(Uuid::now_v7(), table_id, field));
    }
    tx_helper.insert_fields(&field_records).await?;

    tx_helper
        .create_inline_row_table(&table_id, creation.schema.fields())
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
) -> ILResult<Uuid> {
    let index_id = Uuid::now_v7();
    let index_def = Arc::new(IndexDefination {
        index_id,
        name: creation.name.clone(),
        kind: creation.kind.clone(),
        table_id: table.table_id,
        table_name: table.table_name.clone(),
        table_schema: table.schema.clone(),
        key_columns: creation.key_columns.clone(),
        params: creation.params.clone(),
    });

    let index_kind = table
        .index_kinds
        .get(&creation.kind)
        .ok_or_else(|| ILError::InvalidInput(format!("Index kind {} not found", creation.kind)))?;
    index_kind.supports(&index_def)?;

    if tx_helper
        .index_name_exists(&table.table_id, &creation.name)
        .await?
    {
        return Err(ILError::InvalidInput(format!(
            "Index name {} already exists",
            creation.name
        )));
    }

    // create index file
    let mut projection = vec![0];
    for col in creation.key_columns.iter() {
        let idx = table.schema.index_of(col)?;
        projection.push(idx);
    }
    projection.sort();

    let data_file_records = tx_helper.get_data_files(&table.table_id).await?;

    let mut index_file_records = Vec::new();
    for data_file_record in data_file_records {
        let mut index_builder = index_kind.builder(&index_def)?;
        let mut stream = read_data_file_by_record(
            &table.storage,
            &table.schema,
            &data_file_record,
            Some(projection.clone()),
            vec![],
            None,
            1024,
        )
        .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            index_builder.append(&batch)?;
        }

        let index_file_id = Uuid::now_v7();
        let relative_path = IndexFileRecord::build_relative_path(
            &table.namespace_id,
            &table.table_id,
            &index_file_id,
        );
        let output_file = table.storage.create_file(&relative_path).await?;
        index_builder.write_file(output_file).await?;
        index_file_records.push(IndexFileRecord {
            index_file_id,
            table_id: table.table_id,
            index_id,
            data_file_id: data_file_record.data_file_id,
            relative_path,
        });
    }

    tx_helper.insert_index_files(&index_file_records).await?;

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

    table
        .indexes
        .insert(creation.name.clone(), index_def.clone());

    Ok(index_id)
}

fn field_names_to_ids(
    field_map: &BTreeMap<Uuid, FieldRef>,
    names: &[String],
) -> ILResult<Vec<Uuid>> {
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
