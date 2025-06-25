use std::{collections::HashMap, sync::Arc};

use crate::{
    ILError, ILResult,
    catalog::{RowStream, TransactionHelper},
    record::{
        DataType, Field, INTERNAL_ROW_ID_FIELD, INTERNAL_ROW_ID_FIELD_NAME, Row, Schema, SchemaRef,
        sql_identifier,
    },
};

impl TransactionHelper {
    pub(crate) async fn get_max_namespace_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_namespace_id",
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "namespace_id",
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_table_id",
            DataType::Int64,
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

    pub(crate) async fn get_table_id(
        &mut self,
        namespace_id: i64,
        table_name: &str,
    ) -> ILResult<Option<i64>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_id",
            DataType::Int64,
            false,
        )]));
        let rows = self
            .query_rows(
                &format!("SELECT table_id FROM indexlake_table WHERE namespace_id = {namespace_id} AND table_name = '{table_name}'"),
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let table_id_opt = rows[0].int64(0)?;
            assert!(table_id_opt.is_some());
            Ok(table_id_opt)
        }
    }

    pub(crate) async fn get_max_field_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_field_id",
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("default_value", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, false),
        ]));
        let rows = self
            .query_rows(
                &format!("SELECT field_name, data_type, nullable, default_value, metadata FROM indexlake_field WHERE table_id = {table_id} order by field_id asc"),
                schema,
            )
            .await?;
        let mut fields = Vec::new();
        for row in rows {
            let field_name = row.utf8(0)?.expect("field_name is not null");
            let data_type_str = row.utf8(1)?.expect("data_type is not null");
            let data_type = DataType::parse_sql_type(&data_type_str, self.database)?;
            let nullable = row.boolean(2)?.expect("nullable is not null");
            let default_value = row.utf8(3)?.map(|v| v.to_string());
            let metadata_str = row.utf8(4)?.expect("metadata is not null");
            let metadata: HashMap<String, String> =
                serde_json::from_str(&metadata_str).map_err(|e| {
                    ILError::InternalError(format!("Failed to deserialize field metadata: {e:?}"))
                })?;
            fields.push(
                Field::new(field_name, data_type, nullable)
                    .with_default_value(default_value)
                    .with_metadata(metadata),
            );
        }
        Ok(fields)
    }

    pub(crate) async fn get_max_row_id(&mut self, table_id: i64) -> ILResult<i64> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_row_id",
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("location", DataType::Utf8, true),
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
        schema: &SchemaRef,
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
        table_schema: &SchemaRef,
        row_ids: &[i64],
    ) -> ILResult<RowStream> {
        let select_items = table_schema
            .fields
            .iter()
            .map(|f| sql_identifier(&f.name, self.database))
            .collect::<Vec<_>>();
        self.transaction
            .query(
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            INTERNAL_ROW_ID_FIELD_NAME.to_string(),
            DataType::Int64,
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
        limit: i64,
    ) -> ILResult<Vec<i64>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            INTERNAL_ROW_ID_FIELD_NAME.to_string(),
            DataType::Int64,
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_data_file_id",
            DataType::Int64,
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
}
