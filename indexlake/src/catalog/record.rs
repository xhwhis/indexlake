use std::{collections::HashMap, str::FromStr};

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
    pub(crate) row_ids: Vec<i64>,
}

impl DataFileRecord {
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        let row_ids_bytes = self
            .row_ids
            .iter()
            .map(|id| id.to_le_bytes())
            .flatten()
            .collect::<Vec<_>>();
        let row_ids_sql = database.sql_binary_value(&row_ids_bytes);
        format!(
            "({}, {}, '{}', {}, {}, {})",
            self.data_file_id,
            self.table_id,
            self.relative_path,
            self.file_size_bytes,
            self.record_count,
            row_ids_sql
        )
    }

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec![
            "data_file_id",
            "table_id",
            "relative_path",
            "file_size_bytes",
            "record_count",
            "row_ids",
        ]
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowMetadataRecord {
    pub(crate) row_id: i64,
    pub(crate) location: RowLocation,
    pub(crate) deleted: bool,
}

impl RowMetadataRecord {
    pub(crate) fn new(row_id: i64, location: RowLocation) -> Self {
        Self {
            row_id,
            location,
            deleted: false,
        }
    }

    pub(crate) fn to_sql(&self) -> String {
        format!(
            "({}, '{}', {})",
            self.row_id,
            match &self.location {
                RowLocation::Inline => "inline".to_string(),
                RowLocation::Parquet {
                    relative_path,
                    row_group_index,
                    row_group_offset,
                } => {
                    format!(
                        "parquet:{}:{}:{}",
                        relative_path, row_group_index, row_group_offset
                    )
                }
            },
            self.deleted
        )
    }

    pub(crate) fn select_items() -> Vec<&'static str> {
        vec![INTERNAL_ROW_ID_FIELD_NAME, "location", "deleted"]
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RowLocation {
    Inline,
    Parquet {
        relative_path: String,
        row_group_index: usize,
        row_group_offset: usize,
    },
}

impl FromStr for RowLocation {
    type Err = ILError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "inline" {
            Ok(RowLocation::Inline)
        } else if s.starts_with("parquet") {
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(ILError::InvalidInput(format!("Invalid row location: {s}")));
            }
            let relative_path = parts[1].to_string();
            let row_group_index = parts[2]
                .parse::<usize>()
                .map_err(|e| ILError::InvalidInput(format!("Invalid row group index: {}", e)))?;
            let row_group_offset = parts[3]
                .parse::<usize>()
                .map_err(|e| ILError::InvalidInput(format!("Invalid row group offset: {}", e)))?;
            Ok(RowLocation::Parquet {
                relative_path,
                row_group_index,
                row_group_offset,
            })
        } else {
            Err(ILError::InvalidInput(format!(
                "Invalid row location: {}",
                s
            )))
        }
    }
}

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
