use crate::record::{Field, INTERNAL_ROW_ID_FIELD_NAME, sql_identifier};
use crate::{ILResult, TransactionHelper};

impl TransactionHelper {
    pub(crate) async fn create_row_metadata_table(&mut self, table_id: i64) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "
            CREATE TABLE indexlake_row_metadata_{table_id} (
                {INTERNAL_ROW_ID_FIELD_NAME} BIGINT PRIMARY KEY,
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
        fields: &[Field],
    ) -> ILResult<()> {
        let mut columns = Vec::new();
        columns.push(format!("{} BIGINT PRIMARY KEY", INTERNAL_ROW_ID_FIELD_NAME));
        for field in fields {
            columns.push(format!(
                "{} {} {} {}",
                sql_identifier(&field.name, self.database),
                field.data_type.to_sql(self.database),
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
