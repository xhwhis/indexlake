use std::sync::Arc;

use crate::{
    CatalogColumn, CatalogDataType, CatalogRow, ILResult,
    catalog::{CatalogSchema, Transaction},
    schema::{Field, SchemaRef},
};

pub(crate) struct TransactionHelper {
    transaction: Box<dyn Transaction>,
}

impl TransactionHelper {
    pub(crate) fn new(transaction: Box<dyn Transaction>) -> Self {
        Self { transaction }
    }

    pub(crate) async fn commit(&mut self) -> ILResult<()> {
        self.transaction.commit().await
    }

    pub(crate) async fn rollback(&mut self) -> ILResult<()> {
        self.transaction.rollback().await
    }

    pub(crate) async fn get_max_namespace_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![CatalogColumn::new(
            "max_namespace_id",
            CatalogDataType::BigInt,
            true,
        )]));
        let rows = self
            .transaction
            .query("SELECT MAX(namespace_id) FROM indexlake_namespace", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_namespace_id = rows[0].bigint(0);
            Ok(max_namespace_id.unwrap_or(0))
        }
    }

    pub(crate) async fn get_namespace_id(&mut self, namespace_name: &str) -> ILResult<Option<i64>> {
        let schema = Arc::new(CatalogSchema::new(vec![CatalogColumn::new(
            "namespace_id",
            CatalogDataType::BigInt,
            false,
        )]));
        let rows = self
            .transaction
            .query(
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

    pub(crate) async fn create_namespace(&mut self, namespace_name: &str) -> ILResult<i64> {
        let max_namespace_id = self.get_max_namespace_id().await?;
        let namespace_id = max_namespace_id + 1;
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_namespace (namespace_id, namespace_name) VALUES ({namespace_id}, '{namespace_name}')"
            ))
            .await?;
        Ok(namespace_id)
    }

    pub(crate) async fn get_max_table_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![CatalogColumn::new(
            "max_table_id",
            CatalogDataType::BigInt,
            true,
        )]));
        let rows = self
            .transaction
            .query("SELECT MAX(table_id) FROM indexlake_table", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_table_id = rows[0].bigint(0);
            Ok(max_table_id.unwrap_or(0))
        }
    }

    pub(crate) async fn create_table(
        &mut self,
        namespace_id: i64,
        table_name: &str,
    ) -> ILResult<i64> {
        let max_table_id = self.get_max_table_id().await?;
        let table_id = max_table_id + 1;
        self.transaction.execute(&format!("INSERT INTO indexlake_table (table_id, table_name, namespace_id) VALUES ({table_id}, '{table_name}', {namespace_id})")).await?;
        Ok(table_id)
    }

    pub(crate) async fn get_max_field_id(&mut self) -> ILResult<i64> {
        let schema = Arc::new(CatalogSchema::new(vec![CatalogColumn::new(
            "max_field_id",
            CatalogDataType::BigInt,
            true,
        )]));

        let rows = self
            .transaction
            .query("SELECT MAX(field_id) FROM indexlake_field", schema)
            .await?;
        if rows.is_empty() {
            Ok(0)
        } else {
            let max_field_id = rows[0].bigint(0);
            Ok(max_field_id.unwrap_or(0))
        }
    }

    pub(crate) async fn create_table_fields(
        &mut self,
        table_id: i64,
        fields: &[Field],
    ) -> ILResult<Vec<i64>> {
        let max_field_id = self.get_max_field_id().await?;
        let mut field_ids = Vec::new();
        let mut field_id = max_field_id + 1;
        let mut values = Vec::new();
        for field in fields {
            values.push(format!(
                "({field_id}, {table_id}, '{}', '{}', {}, '{}')",
                field.name,
                field.data_type.to_string(),
                field.nullable,
                field
                    .default_value
                    .as_ref()
                    .map(|v| format!("{v}"))
                    .unwrap_or("".to_string())
            ));
            field_ids.push(field_id);
            field_id += 1;
        }
        self.transaction.execute(&format!("INSERT INTO indexlake_field (field_id, table_id, field_name, data_type, nullable, default_value) VALUES {}", values.join(", "))).await?;
        Ok(field_ids)
    }

    pub(crate) async fn create_row_table(&mut self, table_id: i64) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "
            CREATE TABLE indexlake_row_{table_id} (
                row_id BIGINT PRIMARY KEY,
                location VARCHAR,
                deleted BOOLEAN
            )"
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn create_inline_table(
        &mut self,
        table_id: i64,
        schema: &SchemaRef,
    ) -> ILResult<()> {
        let mut columns = Vec::new();
        columns.push("row_id BIGINT PRIMARY KEY".to_string());
        for field in &schema.fields {
            columns.push(format!(
                "{} {} {} {}",
                field.name,
                field.data_type.to_catalog_data_type(),
                if field.nullable {
                    "NULL".to_string()
                } else {
                    "NOT NULL".to_string()
                },
                if let Some(default_value) = field.default_value.as_ref() {
                    format!("DEFAULT {}", default_value)
                } else {
                    "".to_string()
                }
            ));
        }
        self.transaction
            .execute(&format!(
                "CREATE TABLE indexlake_inline_{table_id} ({})",
                columns.join(", ")
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_rows(
        &mut self,
        table_id: i64,
        schema: &SchemaRef,
        rows: Vec<CatalogRow>,
    ) -> ILResult<()> {
        let mut values = Vec::new();
        for row in rows {
            values.push(format!(
                "({})",
                row.values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_inline_{table_id} ({}) VALUES {}",
                schema
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", ")
            ))
            .await?;
        Ok(())
    }
}
