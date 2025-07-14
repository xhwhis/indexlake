use arrow::datatypes::Fields;

use crate::{
    ILError, ILResult,
    catalog::{CatalogDataType, CatalogDatabase, INTERNAL_ROW_ID_FIELD_NAME, TransactionHelper},
};

impl TransactionHelper {
    pub(crate) async fn create_inline_row_table(
        &mut self,
        table_id: i64,
        fields: &Fields,
    ) -> ILResult<()> {
        let mut columns = Vec::new();
        match self.database {
            CatalogDatabase::Postgres => {
                columns.push(format!(
                    "{} BIGSERIAL PRIMARY KEY",
                    INTERNAL_ROW_ID_FIELD_NAME
                ));
            }
            CatalogDatabase::Sqlite => {
                columns.push(format!(
                    "{} INTEGER PRIMARY KEY AUTOINCREMENT",
                    INTERNAL_ROW_ID_FIELD_NAME
                ));
            }
        }
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
