use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Field};
use parquet::arrow::arrow_reader::RowSelection;
use uuid::Uuid;

use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDataType, CatalogDatabase, CatalogSchema, Column, INTERNAL_ROW_ID_FIELD_NAME, Row,
    },
    table::TableConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct TableRecord {
    pub(crate) table_id: Uuid,
    pub(crate) table_name: String,
    pub(crate) namespace_id: Uuid,
    pub(crate) config: TableConfig,
}

impl TableRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> ILResult<String> {
        let config_str = serde_json::to_string(&self.config).map_err(|e| {
            ILError::InternalError(format!("Failed to serialize table config: {e:?}"))
        })?;
        Ok(format!(
            "({}, '{}', {}, '{}')",
            database.sql_uuid_value(&self.table_id),
            self.table_name,
            database.sql_uuid_value(&self.namespace_id),
            config_str
        ))
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("table_id", CatalogDataType::Uuid, false),
            Column::new("table_name", CatalogDataType::Utf8, false),
            Column::new("namespace_id", CatalogDataType::Uuid, false),
            Column::new("config", CatalogDataType::Utf8, false),
        ])
    }

    pub(crate) fn from_row(row: &Row) -> ILResult<Self> {
        let table_id = row.uuid(0)?.expect("table_id is not null");
        let table_name = row.utf8(1)?.expect("table_name is not null");
        let namespace_id = row.uuid(2)?.expect("namespace_id is not null");
        let config_str = row.utf8(3)?.expect("config is not null");
        let config: TableConfig = serde_json::from_str(&config_str).map_err(|e| {
            ILError::InternalError(format!("Failed to deserialize table config: {e:?}"))
        })?;
        Ok(TableRecord {
            table_id,
            table_name: table_name.clone(),
            namespace_id,
            config,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FieldRecord {
    pub(crate) field_id: Uuid,
    pub(crate) table_id: Uuid,
    pub(crate) field_name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
    pub(crate) metadata: HashMap<String, String>,
}

impl FieldRecord {
    pub(crate) fn new(field_id: Uuid, table_id: Uuid, field: &Field) -> Self {
        Self {
            field_id,
            table_id,
            field_name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            metadata: field.metadata().clone(),
        }
    }

    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> ILResult<String> {
        let data_type_str = serde_json::to_string(&self.data_type)
            .map_err(|e| ILError::InternalError(format!("Failed to serialize data type: {e:?}")))?;
        let metadata_str = serde_json::to_string(&self.metadata).map_err(|e| {
            ILError::InternalError(format!("Failed to serialize field metadata: {e:?}"))
        })?;
        Ok(format!(
            "({}, {}, '{}', '{}', {}, '{}')",
            database.sql_uuid_value(&self.field_id),
            database.sql_uuid_value(&self.table_id),
            self.field_name,
            data_type_str,
            self.nullable,
            metadata_str
        ))
    }

    pub(crate) fn into_field(self) -> Field {
        Field::new(self.field_name, self.data_type, self.nullable).with_metadata(self.metadata)
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("field_id", CatalogDataType::Uuid, false),
            Column::new("table_id", CatalogDataType::Uuid, false),
            Column::new("field_name", CatalogDataType::Utf8, false),
            Column::new("data_type", CatalogDataType::Utf8, false),
            Column::new("nullable", CatalogDataType::Boolean, false),
            Column::new("metadata", CatalogDataType::Utf8, false),
        ])
    }

    pub(crate) fn from_row(row: &Row) -> ILResult<Self> {
        let field_id = row.uuid(0)?.expect("field_id is not null");
        let table_id = row.uuid(1)?.expect("table_id is not null");
        let field_name = row.utf8(2)?.expect("field_name is not null");
        let data_type_str = row.utf8(3)?.expect("data_type is not null");
        let data_type: DataType = serde_json::from_str(&data_type_str).map_err(|e| {
            ILError::InternalError(format!("Failed to deserialize data type: {e:?}"))
        })?;
        let nullable = row.boolean(4)?.expect("nullable is not null");
        let metadata_str = row.utf8(5)?.expect("metadata is not null");
        let metadata: HashMap<String, String> =
            serde_json::from_str(&metadata_str).map_err(|e| {
                ILError::InternalError(format!("Failed to deserialize field metadata: {e:?}"))
            })?;
        Ok(FieldRecord {
            field_id,
            table_id,
            field_name: field_name.clone(),
            data_type,
            nullable,
            metadata,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DataFileRecord {
    pub(crate) data_file_id: Uuid,
    pub(crate) table_id: Uuid,
    pub(crate) relative_path: String,
    pub(crate) file_size_bytes: i64,
    pub(crate) record_count: i64,
    pub(crate) validity: RowsValidity,
}

impl DataFileRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        let validity_sql = database.sql_binary_value(&self.validity.to_bytes());
        format!(
            "({}, {}, '{}', {}, {}, {})",
            database.sql_uuid_value(&self.data_file_id),
            database.sql_uuid_value(&self.table_id),
            self.relative_path,
            self.file_size_bytes,
            self.record_count,
            validity_sql
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("data_file_id", CatalogDataType::Uuid, false),
            Column::new("table_id", CatalogDataType::Uuid, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
            Column::new("file_size_bytes", CatalogDataType::Int64, false),
            Column::new("record_count", CatalogDataType::Int64, false),
            Column::new("validity", CatalogDataType::Binary, false),
        ])
    }

    pub(crate) fn build_relative_path(
        namespace_id: &Uuid,
        table_id: &Uuid,
        data_file_id: &Uuid,
    ) -> String {
        format!(
            "{}/{}/{}.parquet",
            namespace_id.to_string(),
            table_id.to_string(),
            data_file_id.to_string()
        )
    }

    pub(crate) fn from_row(row: &Row) -> ILResult<Self> {
        let data_file_id = row.uuid(0)?.expect("data_file_id is not null");
        let table_id = row.uuid(1)?.expect("table_id is not null");
        let relative_path = row.utf8(2)?.expect("relative_path is not null").to_string();
        let file_size_bytes = row.int64(3)?.expect("file_size_bytes is not null");
        let record_count = row.int64(4)?.expect("record_count is not null");
        let validity_bytes = row.binary(5)?.expect("validity is not null");
        let validity = RowsValidity::from_bytes(&validity_bytes)?;
        Ok(DataFileRecord {
            data_file_id,
            table_id,
            relative_path,
            file_size_bytes,
            record_count,
            validity,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowsValidity {
    pub(crate) validity: Vec<(i64, bool)>,
}

impl RowsValidity {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.validity.len() * (8 + 1));
        for (row_id, valid) in &self.validity {
            bytes.extend_from_slice(&row_id.to_le_bytes());
            if *valid {
                bytes.extend_from_slice(&[1]);
            } else {
                bytes.extend_from_slice(&[0]);
            }
        }
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> ILResult<Self> {
        if bytes.len() % (8 + 1) != 0 {
            return Err(ILError::InternalError(format!(
                "Invalid row validity bytes length: {}",
                bytes.len()
            )));
        }
        let validity = bytes
            .chunks_exact(8 + 1)
            .map(|bytes| {
                let row_id = i64::from_le_bytes(bytes[..8].try_into().map_err(|e| {
                    ILError::InternalError(format!("Invalid row validity bytes: {e:?}"))
                })?);
                let valid = bytes[8] == 1;
                Ok((row_id, valid))
            })
            .collect::<ILResult<Vec<_>>>()?;
        Ok(Self { validity })
    }

    pub(crate) fn valid_row_count(&self) -> usize {
        self.validity.iter().filter(|(_, valid)| *valid).count()
    }

    pub(crate) fn row_selection(&self, row_ids: Option<&HashSet<i64>>) -> ILResult<RowSelection> {
        let offsets = self
            .validity
            .iter()
            .enumerate()
            .filter(|(_, (row_id, valid))| {
                *valid
                    && row_ids
                        .as_ref()
                        .map(|ids| ids.contains(row_id))
                        .unwrap_or(true)
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        let mut ranges = Vec::new();
        let mut offset_idx = 0;
        while offset_idx < offsets.len() {
            let current_offset = offsets[offset_idx];
            let mut next_offset_idx = offset_idx + 1;
            while next_offset_idx < offsets.len()
                && offsets[next_offset_idx] == current_offset + (next_offset_idx - offset_idx)
            {
                next_offset_idx += 1;
            }
            ranges.push(current_offset..offsets[next_offset_idx - 1] + 1);
            offset_idx = next_offset_idx;
        }
        Ok(RowSelection::from_consecutive_ranges(
            ranges.into_iter(),
            self.validity.len(),
        ))
    }

    pub(crate) fn row_ids(&self) -> Vec<i64> {
        self.validity
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>()
    }

    pub(crate) fn valid_row_ids(&self) -> impl Iterator<Item = i64> {
        self.validity
            .iter()
            .filter(|(_, valid)| *valid)
            .map(|(row_id, _)| *row_id)
    }

    pub(crate) fn iter_mut_valid_row_ids(&mut self) -> impl Iterator<Item = &mut (i64, bool)> {
        self.validity.iter_mut().filter(|(_, valid)| *valid)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexRecord {
    pub(crate) index_id: i64,
    pub(crate) index_name: String,
    pub(crate) index_kind: String,
    pub(crate) table_id: Uuid,
    pub(crate) key_field_ids: Vec<Uuid>,
    pub(crate) params: String,
}

impl IndexRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        let key_field_ids_str = self
            .key_field_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "({}, '{}', '{}', {}, '{}', '{}')",
            self.index_id,
            self.index_name,
            self.index_kind,
            database.sql_uuid_value(&self.table_id),
            key_field_ids_str,
            self.params
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("index_id", CatalogDataType::Int64, false),
            Column::new("index_name", CatalogDataType::Utf8, false),
            Column::new("index_kind", CatalogDataType::Utf8, false),
            Column::new("table_id", CatalogDataType::Uuid, false),
            Column::new("key_field_ids", CatalogDataType::Utf8, false),
            Column::new("params", CatalogDataType::Utf8, false),
        ])
    }

    pub(crate) fn from_row(row: &Row) -> ILResult<Self> {
        let index_id = row.int64(0)?.expect("index_id is not null");
        let index_name = row.utf8(1)?.expect("index_name is not null");
        let table_id = row.uuid(2)?.expect("table_id is not null");
        let kind = row.utf8(3)?.expect("kind is not null");
        let key_field_ids_str = row.utf8(4)?.expect("key_field_ids is not null");
        let key_field_ids = key_field_ids_str
            .split(",")
            .map(|id| Uuid::parse_str(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ILError::InternalError(format!("Failed to parse key field ids: {e:?}")))?;
        let params = row.utf8(6)?.expect("params is not null");
        Ok(IndexRecord {
            index_id,
            index_name: index_name.clone(),
            index_kind: kind.clone(),
            table_id,
            key_field_ids,
            params: params.clone(),
        })
    }
}

pub(crate) struct IndexFileRecord {
    pub(crate) index_file_id: i64,
    pub(crate) table_id: Uuid,
    pub(crate) index_id: i64,
    pub(crate) data_file_id: Uuid,
    pub(crate) relative_path: String,
}

impl IndexFileRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        format!(
            "({}, {}, {}, {}, '{}')",
            self.index_file_id,
            self.table_id,
            self.index_id,
            database.sql_uuid_value(&self.data_file_id),
            self.relative_path
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("index_file_id", CatalogDataType::Int64, false),
            Column::new("table_id", CatalogDataType::Uuid, false),
            Column::new("index_id", CatalogDataType::Int64, false),
            Column::new("data_file_id", CatalogDataType::Uuid, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
        ])
    }

    pub(crate) fn build_relative_path(
        namespace_id: &Uuid,
        table_id: &Uuid,
        data_file_id: &Uuid,
        index_id: i64,
        index_file_id: i64,
    ) -> String {
        format!(
            "{}/{}/{}-{}-{}.index",
            namespace_id.to_string(),
            table_id.to_string(),
            data_file_id.to_string(),
            index_id,
            index_file_id
        )
    }

    pub(crate) fn from_row(row: &Row) -> ILResult<Self> {
        let index_file_id = row.int64(0)?.expect("index_file_id is not null");
        let table_id = row.uuid(1)?.expect("table_id is not null");
        let index_id = row.int64(2)?.expect("index_id is not null");
        let data_file_id = row.uuid(3)?.expect("data_file_id is not null");
        let relative_path = row.utf8(4)?.expect("relative_path is not null").to_string();
        Ok(Self {
            index_file_id,
            table_id,
            index_id,
            data_file_id,
            relative_path,
        })
    }
}
