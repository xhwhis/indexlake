use crate::{
    ILResult,
    catalog::{INTERNAL_FLAG_FIELD_NAME, INTERNAL_ROW_ID_FIELD_NAME, TransactionHelper},
    expr::Expr,
};

impl TransactionHelper {
    pub(crate) async fn delete_inline_rows_by_row_ids(
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

    pub(crate) async fn delete_inline_rows_by_condition(
        &mut self,
        table_id: i64,
        condition: &Expr,
    ) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_inline_row_{table_id} WHERE {} AND {INTERNAL_FLAG_FIELD_NAME} IS NULL",
                condition.to_sql(self.database)?
            ))
            .await
    }

    pub(crate) async fn delete_inline_rows_by_flag(
        &mut self,
        table_id: i64,
        flag: &str,
    ) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_inline_row_{table_id} WHERE {INTERNAL_FLAG_FIELD_NAME} = '{flag}'"
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

    pub(crate) async fn delete_all_index_files(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_index_file WHERE table_id = {table_id}"
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

    pub(crate) async fn delete_indexes(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "DELETE FROM indexlake_index WHERE table_id = {table_id}"
            ))
            .await
    }
}
