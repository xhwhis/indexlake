use std::collections::HashMap;

use arrow::datatypes::{DataType, Field};
use parquet::arrow::arrow_reader::RowSelection;

use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDataType, CatalogDatabase, CatalogSchema, Column, INTERNAL_ROW_ID_FIELD_NAME,
    },
    table::TableConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct TableRecord {
    pub(crate) table_id: i64,
    pub(crate) table_name: String,
    pub(crate) namespace_id: i64,
    pub(crate) config: TableConfig,
}

impl TableRecord {
    pub(crate) fn to_sql(&self) -> ILResult<String> {
        let config_str = serde_json::to_string(&self.config).map_err(|e| {
            ILError::InternalError(format!("Failed to serialize table config: {e:?}"))
        })?;
        Ok(format!(
            "({}, '{}', {}, '{}')",
            self.table_id, self.table_name, self.namespace_id, config_str
        ))
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("table_name", CatalogDataType::Utf8, false),
            Column::new("namespace_id", CatalogDataType::Int64, false),
            Column::new("config", CatalogDataType::Utf8, false),
        ])
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FieldRecord {
    pub(crate) field_id: i64,
    pub(crate) table_id: i64,
    pub(crate) field_name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
    pub(crate) metadata: HashMap<String, String>,
}

impl FieldRecord {
    pub(crate) fn new(field_id: i64, table_id: i64, field: &Field) -> Self {
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
        let metadata_str = serde_json::to_string(&self.metadata).map_err(|e| {
            ILError::InternalError(format!("Failed to serialize field metadata: {e:?}"))
        })?;
        Ok(format!(
            "({}, {}, '{}', '{}', {}, '{}')",
            self.field_id,
            self.table_id,
            self.field_name,
            self.data_type.to_string(),
            self.nullable,
            metadata_str
        ))
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("field_id", CatalogDataType::Int64, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("field_name", CatalogDataType::Utf8, false),
            Column::new("data_type", CatalogDataType::Utf8, false),
            Column::new("nullable", CatalogDataType::Boolean, false),
            Column::new("metadata", CatalogDataType::Utf8, false),
        ])
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DataFileRecord {
    pub(crate) data_file_id: i64,
    pub(crate) table_id: i64,
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
            self.data_file_id,
            self.table_id,
            self.relative_path,
            self.file_size_bytes,
            self.record_count,
            validity_sql
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("data_file_id", CatalogDataType::Int64, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
            Column::new("file_size_bytes", CatalogDataType::Int64, false),
            Column::new("record_count", CatalogDataType::Int64, false),
            Column::new("validity", CatalogDataType::Binary, false),
        ])
    }

    pub(crate) fn build_relative_path(
        namespace_id: i64,
        table_id: i64,
        data_file_id: i64,
    ) -> String {
        format!("{}/{}/{}.parquet", namespace_id, table_id, data_file_id)
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

    pub(crate) fn row_selection(&self) -> ILResult<RowSelection> {
        let offsets = self
            .validity
            .iter()
            .enumerate()
            .filter(|(_, (_, valid))| *valid)
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
}

#[derive(Debug, Clone)]
pub(crate) struct IndexRecord {
    pub(crate) index_id: i64,
    pub(crate) index_name: String,
    pub(crate) index_kind: String,
    pub(crate) table_id: i64,
    pub(crate) key_field_ids: Vec<i64>,
    pub(crate) include_field_ids: Vec<i64>,
    pub(crate) params: String,
}

impl IndexRecord {
    pub(crate) fn to_sql(&self) -> String {
        let key_field_ids_str = self
            .key_field_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let include_field_ids_str = self
            .include_field_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "({}, '{}', '{}', {}, '{}', '{}', '{}')",
            self.index_id,
            self.index_name,
            self.index_kind,
            self.table_id,
            key_field_ids_str,
            include_field_ids_str,
            self.params
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("index_id", CatalogDataType::Int64, false),
            Column::new("index_name", CatalogDataType::Utf8, false),
            Column::new("index_kind", CatalogDataType::Utf8, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("key_field_ids", CatalogDataType::Utf8, false),
            Column::new("include_field_ids", CatalogDataType::Utf8, false),
            Column::new("params", CatalogDataType::Utf8, false),
        ])
    }
}

pub(crate) struct IndexFileRecord {
    pub(crate) index_file_id: i64,
    pub(crate) index_id: i64,
    pub(crate) data_file_id: i64,
    pub(crate) relative_path: String,
}

impl IndexFileRecord {
    pub(crate) fn to_sql(&self) -> String {
        format!(
            "({}, {}, {}, '{}')",
            self.index_file_id, self.index_id, self.data_file_id, self.relative_path
        )
    }

    pub(crate) fn catalog_schema() -> CatalogSchema {
        CatalogSchema::new(vec![
            Column::new("index_file_id", CatalogDataType::Int64, false),
            Column::new("index_id", CatalogDataType::Int64, false),
            Column::new("data_file_id", CatalogDataType::Int64, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
        ])
    }

    pub(crate) fn build_relative_path(
        namespace_id: i64,
        table_id: i64,
        data_file_id: i64,
        index_id: i64,
        index_file_id: i64,
    ) -> String {
        format!(
            "{}/{}/{}-{}-{}.index",
            namespace_id, table_id, data_file_id, index_id, index_file_id
        )
    }
}
