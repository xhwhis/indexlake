use arrow::datatypes::Fields;

use crate::{
    ILError, ILResult,
    catalog::{CatalogDataType, INTERNAL_ROW_ID_FIELD_NAME, TransactionHelper},
};

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
        fields: &Fields,
    ) -> ILResult<()> {
        let mut columns = Vec::new();
        columns.push(format!("{} BIGINT PRIMARY KEY", INTERNAL_ROW_ID_FIELD_NAME));
        for field in fields {
            columns.push(format!(
                "{} {} {}",
                self.database.sql_identifier(&field.name()),
                CatalogDataType::from_arrow(field.data_type())?.to_sql(self.database),
                if field.is_nullable() {
                    "NULL"
                } else {
                    "NOT NULL"
                },
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
