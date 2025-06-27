use crate::{ILResult, catalog::INTERNAL_ROW_ID_FIELD_NAME, catalog::TransactionHelper};

impl TransactionHelper {
    pub(crate) async fn delete_inline_rows(
        &mut self,
        table_id: i64,
        row_ids: &[i64],
    ) -> ILResult<usize> {
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
            .await
    }

    pub(crate) async fn delete_dump_task(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_dump_task WHERE table_id = {table_id}"
            ))
            .await
    }

    pub(crate) async fn delete_all_data_files(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_data_file WHERE table_id = {table_id}"
            ))
            .await
    }

    pub(crate) async fn delete_table(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_table WHERE table_id = {table_id}"
            ))
            .await
    }

    pub(crate) async fn delete_fields(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_field WHERE table_id = {table_id}"
            ))
            .await
    }
}
