use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field};

use crate::catalog::DataFileRecord;
use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDataType, CatalogSchema, CatalogSchemaRef, Column, INTERNAL_ROW_ID_FIELD,
        INTERNAL_ROW_ID_FIELD_NAME, Row, sql_identifier,
    },
    catalog::{RowStream, TableRecord, TransactionHelper},
    table::TableConfig,
};

impl TransactionHelper {
    pub(crate) async fn get_max_namespace_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_namespace_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows("SELECT MAX(namespace_id) FROM indexlake_namespace", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_namespace_id = rows[0].int64(0)?;
            Ok(max_namespace_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_namespace_id(&mut self, namespace_name: &str) -> ILResult<Option<i64>> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "namespace_id",
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT namespace_id FROM indexlake_namespace WHERE namespace_name = '{namespace_name}'"
                ),
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let namespace_id_opt = rows[0].int64(0)?;
            assert!(namespace_id_opt.is_some());
            Ok(namespace_id_opt)
        }
    }

    pub(crate) async fn get_max_table_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_table_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows("SELECT MAX(table_id) FROM indexlake_table", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_table_id = rows[0].int64(0)?;
            Ok(max_table_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_table(
        &mut self,
        namespace_id: i64,
        table_name: &str,
    ) -> ILResult<Option<TableRecord>> {
        let schema = Arc::new(CatalogSchema::new(vec![
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("table_name", CatalogDataType::Utf8, false),
            Column::new("namespace_id", CatalogDataType::Int64, false),
            Column::new("config", CatalogDataType::Utf8, false),
        ]));
        let rows = self
            .query_rows(
                &format!("SELECT {} FROM indexlake_table WHERE namespace_id = {namespace_id} AND table_name = '{table_name}'", TableRecord::select_items().join(", ")),
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let table_id = rows[0].int64(0)?.expect("table_id is not null");
            let table_name = rows[0].utf8(1)?.expect("table_name is not null");
            let namespace_id = rows[0].int64(2)?.expect("namespace_id is not null");
            let config_str = rows[0].utf8(3)?.expect("config is not null");
            let config: TableConfig = serde_json::from_str(&config_str).map_err(|e| {
                ILError::InternalError(format!("Failed to deserialize table config: {e:?}"))
            })?;
            Ok(Some(TableRecord {
                table_id,
                table_name: table_name.clone(),
                namespace_id,
                config,
            }))
        }
    }

    pub(crate) async fn get_max_field_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_field_id",
            CatalogDataType::Int64,
            true,
        )]));

        let rows = self
            .query_rows("SELECT MAX(field_id) FROM indexlake_field", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_field_id = rows[0].int64(0)?;
            Ok(max_field_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_table_fields(&mut self, table_id: i64) -> ILResult<Vec<Field>> {
        let catalog_schema = Arc::new(CatalogSchema::new(vec![
            Column::new("field_name", CatalogDataType::Utf8, false),
            Column::new("data_type", CatalogDataType::Utf8, false),
            Column::new("nullable", CatalogDataType::Boolean, false),
            Column::new("metadata", CatalogDataType::Utf8, false),
        ]));
        let rows = self
            .query_rows(
                &format!("SELECT field_name, data_type, nullable, metadata FROM indexlake_field WHERE table_id = {table_id} order by field_id asc"),
                catalog_schema,
            )
            .await?;
        let mut fields = Vec::new();
        for row in rows {
            let field_name = row.utf8(0)?.expect("field_name is not null");
            let data_type_str = row.utf8(1)?.expect("data_type is not null");
            let data_type = data_type_str.parse::<DataType>()?;
            let nullable = row.boolean(2)?.expect("nullable is not null");
            let metadata_str = row.utf8(3)?.expect("metadata is not null");
            let metadata: HashMap<String, String> =
                serde_json::from_str(&metadata_str).map_err(|e| {
                    ILError::InternalError(format!("Failed to deserialize field metadata: {e:?}"))
                })?;
            fields.push(Field::new(field_name, data_type, nullable).with_metadata(metadata));
        }
        Ok(fields)
    }

    pub(crate) async fn get_max_row_id(&mut self, table_id: i64) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_row_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows(
                &format!("SELECT MAX({INTERNAL_ROW_ID_FIELD_NAME}) FROM indexlake_row_metadata_{table_id}"),
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_row_id = rows[0].int64(0)?;
            Ok(max_row_id.unwrap_or(0))
        }
    }

    pub(crate) async fn scan_row_metadata(&mut self, table_id: i64) -> ILResult<Vec<Row>> {
        let schema = Arc::new(CatalogSchema::new(vec![
            Column::new("row_id", CatalogDataType::Int64, false),
            Column::new("location", CatalogDataType::Utf8, true),
        ]));
        self.query_rows(
                &format!("SELECT {INTERNAL_ROW_ID_FIELD_NAME}, location FROM indexlake_row_metadata_{table_id} WHERE deleted = FALSE"),
                schema,
            )
            .await
    }

    pub(crate) async fn scan_inline_rows(
        &mut self,
        table_id: i64,
        schema: &CatalogSchemaRef,
    ) -> ILResult<Vec<Row>> {
        let select_items = schema
            .fields
            .iter()
            .map(|f| sql_identifier(&f.name, self.database))
            .collect::<Vec<_>>();
        self.query_rows(
            &format!(
                "SELECT {}  FROM indexlake_inline_row_{table_id}",
                select_items.join(", ")
            ),
            Arc::clone(schema),
        )
        .await
    }

    pub(crate) async fn scan_inline_rows_by_row_ids(
        &mut self,
        table_id: i64,
        table_schema: &CatalogSchemaRef,
        row_ids: &[i64],
    ) -> ILResult<Vec<Row>> {
        let select_items = table_schema
            .fields
            .iter()
            .map(|f| sql_identifier(&f.name, self.database))
            .collect::<Vec<_>>();
        self.query_rows(
            &format!(
                "SELECT {} FROM indexlake_inline_row_{table_id} WHERE {} IN ({})",
                select_items.join(", "),
                INTERNAL_ROW_ID_FIELD_NAME,
                row_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Arc::clone(table_schema),
        )
        .await
    }

    pub(crate) async fn count_inline_rows(&mut self, table_id: i64) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "count",
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self
            .query_rows(
                &format!("SELECT COUNT(1) FROM indexlake_inline_row_{table_id}"),
                schema,
            )
            .await?;
        let count = rows[0].int64(0)?.expect("count is not null");
        Ok(count)
    }

    pub(crate) async fn scan_inline_row_ids(&mut self, table_id: i64) -> ILResult<Vec<i64>> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            INTERNAL_ROW_ID_FIELD_NAME.to_string(),
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT {} FROM indexlake_inline_row_{table_id}",
                    INTERNAL_ROW_ID_FIELD_NAME
                ),
                schema,
            )
            .await?;
        let mut row_ids = Vec::with_capacity(rows.len());
        for row in rows {
            row_ids.push(row.int64(0)?.expect("row_id is not null"));
        }
        Ok(row_ids)
    }

    pub(crate) async fn scan_inline_row_ids_with_limit(
        &mut self,
        table_id: i64,
        limit: usize,
    ) -> ILResult<Vec<i64>> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            INTERNAL_ROW_ID_FIELD_NAME.to_string(),
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT {} FROM indexlake_inline_row_{table_id} limit {limit}",
                    INTERNAL_ROW_ID_FIELD_NAME
                ),
                schema,
            )
            .await?;
        let mut row_ids = Vec::with_capacity(rows.len());
        for row in rows {
            row_ids.push(row.int64(0)?.expect("row_id is not null"));
        }
        Ok(row_ids)
    }

    pub(crate) async fn get_max_data_file_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_data_file_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows("SELECT MAX(data_file_id) FROM indexlake_data_file", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_table_id = rows[0].int64(0)?;
            Ok(max_table_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_data_files(&mut self, table_id: i64) -> ILResult<Vec<DataFileRecord>> {
        let schema = Arc::new(CatalogSchema::new(vec![
            Column::new("data_file_id", CatalogDataType::Int64, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
            Column::new("file_size_bytes", CatalogDataType::Int64, false),
            Column::new("record_count", CatalogDataType::Int64, false),
            Column::new("row_ids", CatalogDataType::Binary, false),
        ]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT {} FROM indexlake_data_file WHERE table_id = {table_id}",
                    DataFileRecord::select_items().join(", ")
                ),
                schema,
            )
            .await?;
        let mut data_files = Vec::with_capacity(rows.len());
        for row in rows {
            let data_file_id = row.int64(0)?.expect("data_file_id is not null");
            let table_id = row.int64(1)?.expect("table_id is not null");
            let relative_path = row.utf8(2)?.expect("relative_path is not null").to_string();
            let file_size_bytes = row.int64(3)?.expect("file_size_bytes is not null");
            let record_count = row.int64(4)?.expect("record_count is not null");
            let row_ids_bytes = row.binary(5)?.expect("row_ids is not null");
            if row_ids_bytes.len() % 8 != 0 {
                return Err(ILError::InternalError(format!(
                    "row_ids is not a multiple of 8: {}",
                    row_ids_bytes.len()
                )));
            }
            let row_ids = row_ids_bytes
                .chunks_exact(8)
                .map(|bytes| i64::from_le_bytes(bytes.try_into().unwrap()))
                .collect::<Vec<_>>();
            data_files.push(DataFileRecord {
                data_file_id,
                table_id,
                relative_path,
                file_size_bytes,
                record_count,
                row_ids,
            });
        }
        Ok(data_files)
    }
}
