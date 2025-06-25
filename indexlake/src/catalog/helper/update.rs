use std::collections::HashMap;

use crate::{
    ILResult,
    catalog::TransactionHelper,
    expr::Expr,
    record::{INTERNAL_ROW_ID_FIELD_NAME, Scalar, sql_identifier},
};

impl TransactionHelper {
    pub(crate) async fn mark_rows_deleted(
        &mut self,
        table_id: i64,
        row_ids: &[i64],
    ) -> ILResult<usize> {
        if row_ids.is_empty() {
            return Ok(0);
        }
        let row_ids_str = row_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        self.transaction.execute(&format!("UPDATE indexlake_row_metadata_{table_id} SET deleted = TRUE WHERE {INTERNAL_ROW_ID_FIELD_NAME} IN ({row_ids_str})")).await
    }

    pub(crate) async fn update_inline_rows(
        &mut self,
        table_id: i64,
        set_map: &HashMap<String, Scalar>,
        condition: &Expr,
    ) -> ILResult<()> {
        let mut set_strs = Vec::new();
        for (field_name, new_value) in set_map {
            set_strs.push(format!(
                "{} = {}",
                sql_identifier(field_name, self.database),
                new_value.to_sql(self.database),
            ));
        }

        self.transaction
            .execute(&format!(
                "UPDATE indexlake_inline_row_{table_id} SET {} WHERE {}",
                set_strs.join(", "),
                condition.to_sql(self.database)
            ))
            .await?;

        Ok(())
    }

    pub(crate) async fn update_row_locations(
        &mut self,
        table_id: i64,
        row_id_to_location_map: &HashMap<i64, String>,
    ) -> ILResult<()> {
        let mut update_sqls = Vec::new();
        for (row_id, location) in row_id_to_location_map {
            update_sqls.push(format!("UPDATE indexlake_row_metadata_{table_id} SET location = '{location}' WHERE {INTERNAL_ROW_ID_FIELD_NAME} = {row_id}"));
        }
        self.transaction.execute_batch(&update_sqls).await
    }
}
