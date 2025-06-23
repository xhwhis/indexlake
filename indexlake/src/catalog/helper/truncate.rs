use crate::ILResult;
use crate::catalog::TransactionHelper;

impl TransactionHelper {
    pub(crate) async fn truncate_inline_row_table(&mut self, table_id: i64) -> ILResult<()> {
        self.transaction
            .execute_batch(&[format!("TRUNCATE TABLE indexlake_inline_row_{table_id}")])
            .await
    }

    pub(crate) async fn truncate_row_metadata_table(&mut self, table_id: i64) -> ILResult<()> {
        self.transaction
            .execute_batch(&[format!("TRUNCATE TABLE indexlake_row_metadata_{table_id}")])
            .await
    }
}
