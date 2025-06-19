use std::sync::Arc;

use crate::{
    ILError, ILResult, RowStream,
    catalog::{INLINE_COLUMN_NAME_PREFIX, TransactionHelper},
    record::{DataType, Field, INTERNAL_ROW_ID_FIELD_NAME, Row, Schema, SchemaRef},
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
            let max_namespace_id = rows[0].bigint(0);
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
            let namespace_id_opt = rows[0].bigint(0);
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
            let max_table_id = rows[0].bigint(0);
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
        let rows = self.query_rows(&format!("SELECT table_id FROM indexlake_table WHERE namespace_id = {namespace_id} AND table_name = '{table_name}'"), schema).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let table_id_opt = rows[0].bigint(0);
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
            let max_field_id = rows[0].bigint(0);
            Ok(max_field_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_table_schema(&mut self, table_id: i64) -> ILResult<SchemaRef> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("field_id", DataType::Int64, false),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("default_value", DataType::Utf8, true),
        ]));
        let rows = self.query_rows(&format!("SELECT field_id, field_name, data_type, nullable, default_value FROM indexlake_field WHERE table_id = {table_id} order by field_id asc"), schema).await?;
        let mut fields = Vec::new();
        for row in rows {
            fields.push(
                Field::new(
                    row.varchar(1).expect("field_name is not null"),
                    DataType::parse_sql_type(
                        &row.varchar(2).expect("data_type is not null"),
                        self.database,
                    )?,
                    row.boolean(3).expect("nullable is not null"),
                )
                .with_id(Some(row.bigint(0).expect("field_id is not null")))
                .with_default_value(row.varchar(4).map(|v| v.to_string())),
            );
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(crate) async fn get_max_row_id(&mut self, table_id: i64) -> ILResult<i64> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "max_row_id",
            DataType::Int64,
            true,
        )]));
        let rows = self
            .query_rows(
                &format!("SELECT MAX(row_id) FROM indexlake_row_metadata_{table_id}"),
                schema,
            )
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_row_id = rows[0].bigint(0);
            Ok(max_row_id.unwrap_or(0))
        }
    }

    pub(crate) async fn scan_row_metadata(&mut self, table_id: i64) -> ILResult<Vec<Row>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("location", DataType::Utf8, true),
        ]));
        self.query_rows(&format!("SELECT row_id, location FROM indexlake_row_metadata_{table_id} WHERE deleted = FALSE"), schema).await
    }

    pub(crate) async fn scan_inline_rows(
        &mut self,
        table_id: i64,
        schema: &SchemaRef,
    ) -> ILResult<RowStream> {
        let mut select_items = Vec::new();
        for field in &schema.fields {
            select_items.push(field.inline_field_name()?);
        }
        self.transaction
            .query(
                &format!(
                    "SELECT {}  FROM indexlake_inline_row_{table_id}",
                    select_items.join(", ")
                ),
                Arc::clone(schema),
            )
            .await
    }
}
