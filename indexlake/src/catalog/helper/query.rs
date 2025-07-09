use std::collections::BTreeMap;
use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, FieldRef};

use crate::catalog::{CatalogHelper, DataFileRecord, IndexRecord, RowIdMeta, RowsValidity, Scalar};
use crate::expr::{Expr, col, lit};
use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDataType, CatalogSchema, CatalogSchemaRef, Column, INTERNAL_ROW_ID_FIELD_NAME, Row,
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

    pub(crate) async fn table_name_exists(
        &mut self,
        namespace_id: i64,
        table_name: &str,
    ) -> ILResult<bool> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "table_id",
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self.query_rows(&format!("SELECT table_id FROM indexlake_table WHERE namespace_id = {namespace_id} AND table_name = '{table_name}'"), schema).await?;
        Ok(rows.len() > 0)
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

    pub(crate) async fn get_max_row_id(&mut self, table_id: i64) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_row_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT MAX({INTERNAL_ROW_ID_FIELD_NAME}) FROM indexlake_inline_row_{table_id}"
                ),
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

    pub(crate) async fn scan_inline_rows(
        &mut self,
        table_id: i64,
        schema: &CatalogSchemaRef,
    ) -> ILResult<Vec<Row>> {
        self.query_rows(
            &format!(
                "SELECT {}  FROM indexlake_inline_row_{table_id}",
                schema.select_items(self.database).join(", ")
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
    ) -> ILResult<RowStream> {
        self.transaction
            .query(
                &format!(
                    "SELECT {} FROM indexlake_inline_row_{table_id} WHERE {} IN ({})",
                    table_schema.select_items(self.database).join(", "),
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
            Column::new("validity", CatalogDataType::Binary, false),
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
            let validity_bytes = row.binary(5)?.expect("validity is not null");
            let validity = RowsValidity::from_bytes(&validity_bytes)?;
            data_files.push(DataFileRecord {
                data_file_id,
                table_id,
                relative_path,
                file_size_bytes,
                record_count,
                validity,
            });
        }
        Ok(data_files)
    }

    pub(crate) async fn index_name_exists(
        &mut self,
        table_id: i64,
        index_name: &str,
    ) -> ILResult<bool> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "index_id",
            CatalogDataType::Int64,
            false,
        )]));
        let rows = self.query_rows(&format!("SELECT index_id FROM indexlake_index WHERE table_id = {table_id} AND index_name = '{index_name}'"), schema).await?;
        Ok(rows.len() > 0)
    }

    pub(crate) async fn get_max_index_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_index_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows("SELECT MAX(index_id) FROM indexlake_index", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_index_id = rows[0].int64(0)?;
            Ok(max_index_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_max_index_file_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![Column::new(
            "max_index_file_id",
            CatalogDataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows(
                "SELECT MAX(index_file_id) FROM indexlake_index_file",
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_index_file_id = rows[0].int64(0)?;
            Ok(max_index_file_id.unwrap_or(0))
        }
    }
}

impl CatalogHelper {
    pub(crate) async fn get_namespace_id(&self, namespace_name: &str) -> ILResult<Option<i64>> {
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

    pub(crate) async fn get_table(
        &self,
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

    pub(crate) async fn get_table_fields(
        &self,
        table_id: i64,
    ) -> ILResult<BTreeMap<i64, FieldRef>> {
        let catalog_schema = Arc::new(CatalogSchema::new(vec![
            Column::new("field_id", CatalogDataType::Int64, false),
            Column::new("field_name", CatalogDataType::Utf8, false),
            Column::new("data_type", CatalogDataType::Utf8, false),
            Column::new("nullable", CatalogDataType::Boolean, false),
            Column::new("metadata", CatalogDataType::Utf8, false),
        ]));
        let rows = self
            .query_rows(
                &format!("SELECT {} FROM indexlake_field WHERE table_id = {table_id} order by field_id asc", catalog_schema.select_items(self.catalog.database()).join(", ")),
                catalog_schema,
            )
            .await?;
        let mut field_map = BTreeMap::new();
        for row in rows {
            let field_id = row.int64(0)?.expect("field_id is not null");
            let field_name = row.utf8(1)?.expect("field_name is not null");
            let data_type_str = row.utf8(2)?.expect("data_type is not null");
            let data_type = data_type_str.parse::<DataType>()?;
            let nullable = row.boolean(3)?.expect("nullable is not null");
            let metadata_str = row.utf8(4)?.expect("metadata is not null");
            let metadata: HashMap<String, String> =
                serde_json::from_str(&metadata_str).map_err(|e| {
                    ILError::InternalError(format!("Failed to deserialize field metadata: {e:?}"))
                })?;
            field_map.insert(
                field_id,
                Arc::new(Field::new(field_name, data_type, nullable).with_metadata(metadata)),
            );
        }
        Ok(field_map)
    }

    pub(crate) async fn get_table_indexes(&self, table_id: i64) -> ILResult<Vec<IndexRecord>> {
        let catalog_schema = Arc::new(CatalogSchema::new(vec![
            Column::new("index_id", CatalogDataType::Int64, false),
            Column::new("index_name", CatalogDataType::Utf8, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("kind", CatalogDataType::Utf8, false),
            Column::new("key_field_ids", CatalogDataType::Utf8, false),
            Column::new("include_field_ids", CatalogDataType::Utf8, false),
            Column::new("params", CatalogDataType::Utf8, false),
        ]));
        let rows = self
            .query_rows(
                &format!(
                    "SELECT {} FROM indexlake_index WHERE table_id = {table_id}",
                    IndexRecord::select_items().join(", ")
                ),
                catalog_schema,
            )
            .await?;
        let mut indexes = Vec::with_capacity(rows.len());
        for row in rows {
            let index_id = row.int64(0)?.expect("index_id is not null");
            let index_name = row.utf8(1)?.expect("index_name is not null");
            let table_id = row.int64(2)?.expect("table_id is not null");
            let kind = row.utf8(3)?.expect("kind is not null");
            let key_field_ids_str = row.utf8(4)?.expect("key_field_ids is not null");
            let key_field_ids = key_field_ids_str
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .collect::<Vec<_>>();
            let include_field_ids_str = row.utf8(5)?.expect("include_field_ids is not null");
            let include_field_ids = include_field_ids_str
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .collect::<Vec<_>>();
            let params = row.utf8(6)?.expect("params is not null");
            indexes.push(IndexRecord {
                index_id,
                index_name: index_name.clone(),
                index_kind: kind.clone(),
                table_id,
                key_field_ids,
                include_field_ids,
                params: params.clone(),
            });
        }
        Ok(indexes)
    }

    pub(crate) async fn count_inline_rows(&self, table_id: i64) -> ILResult<i64> {
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

    pub(crate) async fn scan_inline_rows(
        &self,
        table_id: i64,
        schema: &CatalogSchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ILResult<Vec<Row>> {
        let where_clause = if filters.is_empty() {
            "".to_string()
        } else {
            let filters_str = filters
                .iter()
                .map(|f| f.to_sql(self.catalog.database()))
                .collect::<Result<Vec<_>, _>>()?
                .join(" AND ");
            format!(" WHERE {filters_str}")
        };
        let limit_clause = if let Some(limit) = limit {
            format!(" LIMIT {limit}")
        } else {
            "".to_string()
        };
        self.query_rows(
            &format!(
                "SELECT {}  FROM indexlake_inline_row_{table_id}{where_clause}{limit_clause}",
                schema.select_items(self.catalog.database()).join(", ")
            ),
            Arc::clone(schema),
        )
        .await
    }

    pub(crate) async fn get_data_files(&self, table_id: i64) -> ILResult<Vec<DataFileRecord>> {
        let schema = Arc::new(CatalogSchema::new(vec![
            Column::new("data_file_id", CatalogDataType::Int64, false),
            Column::new("table_id", CatalogDataType::Int64, false),
            Column::new("relative_path", CatalogDataType::Utf8, false),
            Column::new("file_size_bytes", CatalogDataType::Int64, false),
            Column::new("record_count", CatalogDataType::Int64, false),
            Column::new("validity", CatalogDataType::Binary, false),
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
            let validity_bytes = row.binary(5)?.expect("validity is not null");
            let validity = RowsValidity::from_bytes(&validity_bytes)?;
            data_files.push(DataFileRecord {
                data_file_id,
                table_id,
                relative_path,
                file_size_bytes,
                record_count,
                validity,
            });
        }
        Ok(data_files)
    }
}
