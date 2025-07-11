use crate::{
    ILError, ILResult,
    catalog::IndexRecord,
    index::{IndexKind, IndexParams},
};
use arrow::datatypes::{Field, FieldRef, SchemaRef};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

pub type IndexDefinationRef = Arc<IndexDefination>;

#[derive(Debug, Clone)]
pub struct IndexDefination {
    pub index_id: i64,
    pub name: String,
    pub kind: String,
    pub table_id: i64,
    pub table_name: String,
    pub table_schema: SchemaRef,
    pub key_columns: Vec<String>,
    pub params: Arc<dyn IndexParams>,
}

impl IndexDefination {
    pub fn key_fields(&self) -> ILResult<Vec<&Field>> {
        let mut key_fields = Vec::new();
        for key_field_id in self.key_columns.iter() {
            let field = self.table_schema.field_with_name(key_field_id)?;
            key_fields.push(field);
        }
        Ok(key_fields)
    }

    pub fn downcast_params<T: 'static>(&self) -> ILResult<&T> {
        self.params.as_any().downcast_ref::<T>().ok_or_else(|| {
            ILError::InternalError(format!(
                "Index params is not {}",
                std::any::type_name::<T>()
            ))
        })
    }

    pub(crate) fn from_index_record(
        index_record: &IndexRecord,
        field_map: &BTreeMap<i64, FieldRef>,
        table_name: &str,
        table_schema: &SchemaRef,
        index_kinds: &HashMap<String, Arc<dyn IndexKind>>,
    ) -> ILResult<Self> {
        let mut key_columns = Vec::new();
        for key_field_id in index_record.key_field_ids.iter() {
            let field = field_map.get(key_field_id).ok_or_else(|| {
                ILError::InternalError(format!(
                    "Key field id {key_field_id} not found in field map"
                ))
            })?;
            key_columns.push(field.name().to_string());
        }

        let index_kind = index_kinds.get(&index_record.index_kind).ok_or_else(|| {
            ILError::InternalError(format!("Index kind {} not found", index_record.index_kind))
        })?;
        let params = index_kind.decode_params(&index_record.params)?;

        Ok(Self {
            index_id: index_record.index_id,
            name: index_record.index_name.clone(),
            kind: index_record.index_kind.clone(),
            table_id: index_record.table_id,
            table_name: table_name.to_string(),
            table_schema: table_schema.clone(),
            key_columns,
            params,
        })
    }
}
