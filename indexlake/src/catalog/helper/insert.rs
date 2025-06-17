use crate::{
    ILResult, TransactionHelper,
    record::{Field, Scalar},
};

impl TransactionHelper {
    pub(crate) async fn insert_namespace(
        &mut self,
        namespace_id: i64,
        namespace_name: &str,
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_namespace (namespace_id, namespace_name) VALUES ({namespace_id}, '{namespace_name}')"
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_table(
        &mut self,
        namespace_id: i64,
        table_id: i64,
        table_name: &str,
    ) -> ILResult<()> {
        self.transaction.execute(&format!("INSERT INTO indexlake_table (table_id, table_name, namespace_id) VALUES ({table_id}, '{table_name}', {namespace_id})")).await?;
        Ok(())
    }

    pub(crate) async fn insert_fields(
        &mut self,
        table_id: i64,
        field_ids: &[i64],
        fields: &[Field],
    ) -> ILResult<()> {
        let mut values = Vec::new();
        for (field_id, field) in field_ids.iter().zip(fields.iter()) {
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
        }
        self.transaction.execute(&format!("INSERT INTO indexlake_field (field_id, table_id, field_name, data_type, nullable, default_value) VALUES {}", values.join(", "))).await?;
        Ok(())
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
