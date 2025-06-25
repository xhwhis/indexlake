use crate::catalog::TransactionHelper;
use crate::{ILResult, catalog::CatalogDatabase};

impl TransactionHelper {
    pub(crate) async fn truncate_inline_row_table(&mut self, table_id: i64) -> ILResult<()> {
        match self.database {
            CatalogDatabase::Sqlite => {
                self.transaction
                    .execute_batch(&[format!("DELETE FROM indexlake_inline_row_{table_id}")])
                    .await
            }
            CatalogDatabase::Postgres => {
                self.transaction
                    .execute_batch(&[format!("TRUNCATE TABLE indexlake_inline_row_{table_id}")])
                    .await
            }
        }
    }

    pub(crate) async fn truncate_row_metadata_table(&mut self, table_id: i64) -> ILResult<()> {
        match self.database {
            CatalogDatabase::Sqlite => {
                self.transaction
                    .execute_batch(&[format!("DELETE FROM indexlake_row_metadata_{table_id}")])
                    .await
            }
            CatalogDatabase::Postgres => {
                self.transaction
                    .execute_batch(&[format!("TRUNCATE TABLE indexlake_row_metadata_{table_id}")])
                    .await
            }
        }
    }
}
