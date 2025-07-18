use arrow::datatypes::Fields;
use uuid::Uuid;

use crate::{
    ILError, ILResult,
    catalog::{
        CatalogDataType, CatalogDatabase, INTERNAL_FLAG_FIELD_NAME, INTERNAL_ROW_ID_FIELD_NAME,
        TransactionHelper, inline_row_table_name,
    },
};

impl TransactionHelper {
    pub(crate) async fn create_inline_row_table(
        &mut self,
        table_id: &Uuid,
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

        columns.push(format!("{} VARCHAR DEFAULT NULL", INTERNAL_FLAG_FIELD_NAME));

        self.transaction
            .execute(&format!(
                "CREATE TABLE {} ({})",
                inline_row_table_name(table_id),
                columns.join(", ")
            ))
            .await?;
        Ok(())
    }
}
