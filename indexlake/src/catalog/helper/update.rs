use crate::{ILResult, catalog::TransactionHelper};

impl TransactionHelper {
    pub(crate) async fn mark_rows_deleted(
        &mut self,
        table_id: i64,
        row_ids: &[i64],
    ) -> ILResult<()> {
        if row_ids.is_empty() {
            return Ok(());
        }
        let row_ids_str = row_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        self.transaction.execute(&format!("UPDATE indexlake_row_metadata_{table_id} SET deleted = TRUE WHERE row_id IN ({row_ids_str})")).await
    }
}
