use std::collections::HashMap;

use crate::{
    ILResult,
    catalog::{RowIdMeta, RowsValidity, Scalar, TransactionHelper},
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

    pub(crate) async fn update_data_file_validity(
        &mut self,
        data_file_id: i64,
        validity: &RowsValidity,
    ) -> ILResult<usize> {
        let validity_bytes = validity.to_bytes();
        let validity_sql = self.database.sql_binary_value(&validity_bytes);
        self.transaction.execute(&format!("UPDATE indexlake_data_file SET validity = {validity_sql} WHERE data_file_id = {data_file_id}")).await
    }
}
