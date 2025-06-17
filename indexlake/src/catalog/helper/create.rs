use crate::record::INTERNAL_ROW_ID_FIELD_NAME;
use crate::{ILResult, TransactionHelper, record::SchemaRef};

impl TransactionHelper {
    pub(crate) async fn create_row_metadata_table(&mut self, table_id: i64) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "
            CREATE TABLE indexlake_row_metadata_{table_id} (
                row_id BIGINT PRIMARY KEY,
                location VARCHAR,
                deleted BOOLEAN
            )"
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn create_inline_row_table(
        &mut self,
        table_id: i64,
        schema: &SchemaRef,
    ) -> ILResult<()> {
        let mut columns = Vec::new();
        columns.push(format!("{} BIGINT PRIMARY KEY", INTERNAL_ROW_ID_FIELD_NAME));
        for field in &schema.fields {
            columns.push(format!(
                "{} {} {} {}",
                field.name,
                field.data_type,
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
                "CREATE TABLE indexlake_inline_row_{table_id} ({})",
                columns.join(", ")
            ))
            .await?;
        Ok(())
    }
}
