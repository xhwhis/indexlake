use crate::{catalog::CatalogDatabase, table::TableConfig};

#[derive(Debug, Clone)]
pub(crate) struct TableRecord {
    pub(crate) table_id: i64,
    pub(crate) table_name: String,
    pub(crate) namespace_id: i64,
    pub(crate) config: TableConfig,
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
        let row_ids_sql = match database {
            CatalogDatabase::Sqlite => format!("X'{}'", hex::encode(&row_ids_bytes)),
            CatalogDatabase::Postgres => format!("E'\\\\x{}'", hex::encode(&row_ids_bytes)),
        };
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
    pub(crate) location: String,
    pub(crate) deleted: bool,
}

impl RowMetadataRecord {
    pub(crate) fn new(row_id: i64, location: impl Into<String>) -> Self {
        Self {
            row_id,
            location: location.into(),
            deleted: false,
        }
    }

    pub(crate) fn to_sql(&self) -> String {
        format!("({}, '{}', {})", self.row_id, self.location, self.deleted)
    }
}
