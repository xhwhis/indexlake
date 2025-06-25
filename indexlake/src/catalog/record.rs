#[derive(Debug, Clone)]
pub(crate) struct DataFileRecord {
    pub(crate) data_file_id: i64,
    pub(crate) table_id: i64,
    pub(crate) relative_path: String,
    pub(crate) file_size_bytes: usize,
    pub(crate) record_count: usize,
}

impl DataFileRecord {
    pub(crate) fn to_sql(&self) -> String {
        format!(
            "({}, {}, '{}', {}, {})",
            self.data_file_id,
            self.table_id,
            self.relative_path,
            self.file_size_bytes,
            self.record_count
        )
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
