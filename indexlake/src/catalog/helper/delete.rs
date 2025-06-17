use crate::{ILResult, catalog::TransactionHelper, record::INTERNAL_ROW_ID_FIELD_NAME};

impl TransactionHelper {
    pub(crate) async fn delete_inline_rows(
        &mut self,
        table_id: i64,
        row_ids: &[i64],
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_inline_row_{table_id} WHERE {} IN ({})",
                INTERNAL_ROW_ID_FIELD_NAME,
                row_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .await?;
        Ok(())
    }
}
