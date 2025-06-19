use std::collections::HashMap;

use crate::{
    ILResult,
    catalog::{INLINE_COLUMN_NAME_PREFIX, TransactionHelper},
    expr::Expr,
    record::{Scalar, SchemaRef},
};

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

    pub(crate) async fn update_inline_rows(
        &mut self,
        table_id: i64,
        field_id_to_value_map: &HashMap<i64, Scalar>,
        condition: &Expr,
    ) -> ILResult<()> {
        let mut set_strs = Vec::new();
        for (field_id, value) in field_id_to_value_map {
            set_strs.push(format!(
                "{} = {}",
                format!("{INLINE_COLUMN_NAME_PREFIX}{field_id}"),
                value
            ));
        }

        self.transaction
            .execute(&format!(
                "UPDATE indexlake_inline_row_{table_id} SET {} WHERE {}",
                set_strs.join(", "),
                condition
            ))
            .await?;

        Ok(())
    }
}
