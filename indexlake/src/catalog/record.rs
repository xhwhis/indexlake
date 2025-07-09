use std::{collections::HashMap, str::FromStr};

use parquet::arrow::arrow_reader::RowSelection;

use crate::{
    ILError, ILResult,
    catalog::{CatalogDatabase, INTERNAL_ROW_ID_FIELD_NAME},
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

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec!["table_id", "table_name", "namespace_id", "config"]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DataFileRecord {
    pub(crate) data_file_id: i64,
    pub(crate) table_id: i64,
    pub(crate) relative_path: String,
    pub(crate) file_size_bytes: i64,
    pub(crate) record_count: i64,
    pub(crate) row_id_metas: Vec<RowIdMeta>,
}

impl DataFileRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        let row_id_metas_bytes = self
            .row_id_metas
            .iter()
            .map(|meta| meta.to_bytes())
            .flatten()
            .collect::<Vec<_>>();
        let row_id_metas_sql = database.sql_binary_value(&row_id_metas_bytes);
        format!(
            "({}, {}, '{}', {}, {}, {})",
            self.data_file_id,
            self.table_id,
            self.relative_path,
            self.file_size_bytes,
            self.record_count,
            row_id_metas_sql
        )
    }

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec![
            "data_file_id",
            "table_id",
            "relative_path",
            "file_size_bytes",
            "record_count",
            "row_id_metas",
        ]
    }

    pub(crate) fn build_relative_path(
        namespace_id: i64,
        table_id: i64,
        data_file_id: i64,
    ) -> String {
        format!("{}/{}/{}.parquet", namespace_id, table_id, data_file_id)
    }

    pub(crate) fn valid_row_count(&self) -> usize {
        self.row_id_metas.iter().filter(|meta| meta.valid).count()
    }

    pub(crate) fn row_selection(&self) -> ILResult<RowSelection> {
        let offsets = self
            .row_id_metas
            .iter()
            .enumerate()
            .filter(|(_, meta)| meta.valid)
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
            self.row_id_metas.len(),
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowIdMeta {
    pub(crate) row_id: i64,
    pub(crate) valid: bool,
}

impl RowIdMeta {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.row_id.to_le_bytes());
        if self.valid {
            bytes.extend_from_slice(&[1]);
        } else {
            bytes.extend_from_slice(&[0]);
        }
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> ILResult<Self> {
        if bytes.len() != Self::byte_length() {
            return Err(ILError::InternalError(format!(
                "Invalid row id meta bytes length: {}",
                bytes.len()
            )));
        }
        let row_id =
            i64::from_le_bytes(bytes[..8].try_into().map_err(|e| {
                ILError::InternalError(format!("Invalid row id meta bytes: {e:?}"))
            })?);
        let valid = bytes[8] == 1;
        Ok(Self { row_id, valid })
    }

    pub(crate) fn byte_length() -> usize {
        8 + 1
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

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec![
            "index_id",
            "index_name",
            "index_kind",
            "table_id",
            "key_field_ids",
            "include_field_ids",
            "params",
        ]
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

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec!["index_file_id", "index_id", "data_file_id", "relative_path"]
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
