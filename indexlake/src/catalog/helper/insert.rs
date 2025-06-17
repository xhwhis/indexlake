use crate::{
    ILResult, TransactionHelper,
    record::{Field, Scalar},
};

impl TransactionHelper {
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
                "({field_id}, {table_id}, '{}', '{}', {}, {})",
                field.name,
                field.data_type.to_string(),
                field.nullable,
                field
                    .default_value
                    .as_ref()
                    .map(|v| format!("'{v}'"))
                    .unwrap_or("NULL".to_string())
            ));
            field_ids.push(field_id);
            field_id += 1;
        }
        self.transaction.execute(&format!("INSERT INTO indexlake_field (field_id, table_id, field_name, data_type, nullable, default_value) VALUES {}", values.join(", "))).await?;
        Ok(field_ids)
    }

    pub(crate) async fn insert_inline_rows(
        &mut self,
        table_id: i64,
        fields: &[Field],
        values: Vec<Vec<Scalar>>,
    ) -> ILResult<()> {
        let mut value_strings = Vec::new();
        for value in values {
            value_strings.push(format!(
                "({})",
                value
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_inline_row_{table_id} ({}) VALUES {}",
                fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
                value_strings.join(", ")
            ))
            .await?;
        Ok(())
    }
}
