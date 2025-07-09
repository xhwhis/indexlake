use std::collections::HashMap;

use crate::{
    ILResult,
    catalog::{RowIdMeta, Scalar, TransactionHelper},
    expr::Expr,
};

impl TransactionHelper {
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
                self.database.sql_identifier(field_name),
                new_value.to_sql(self.database),
            ));
        }

        self.transaction
            .execute(&format!(
                "UPDATE indexlake_inline_row_{table_id} SET {} WHERE {}",
                set_strs.join(", "),
                condition.to_sql(self.database)?
            ))
            .await?;

        Ok(())
    }

    pub(crate) async fn update_data_file_row_id_metas(
        &mut self,
        data_file_id: i64,
        row_id_metas: &[RowIdMeta],
    ) -> ILResult<usize> {
        let row_id_metas_bytes = row_id_metas
            .iter()
            .map(|meta| meta.to_bytes())
            .flatten()
            .collect::<Vec<_>>();
        let row_id_metas_sql = self.database.sql_binary_value(&row_id_metas_bytes);
        self.transaction.execute(&format!("UPDATE indexlake_data_file SET row_id_metas = {row_id_metas_sql} WHERE data_file_id = {data_file_id}")).await
    }
}
