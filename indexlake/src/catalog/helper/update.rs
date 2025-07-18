use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::{
    ILResult,
    catalog::{DataFileRecord, INTERNAL_FLAG_FIELD_NAME, RowsValidity, Scalar, TransactionHelper},
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
                "UPDATE indexlake_inline_row_{table_id} SET {} WHERE {} AND {INTERNAL_FLAG_FIELD_NAME} IS NULL",
                set_strs.join(", "),
                condition.to_sql(self.database)?
            ))
            .await?;

        Ok(())
    }

    pub(crate) async fn update_data_file_validity(
        &mut self,
        data_file_id: &Uuid,
        validity: &RowsValidity,
    ) -> ILResult<usize> {
        let validity_bytes = validity.to_bytes();
        let validity_sql = self.database.sql_binary_value(&validity_bytes);
        self.transaction
            .execute(&format!(
                "UPDATE indexlake_data_file SET validity = {validity_sql} WHERE data_file_id = {}",
                self.database.sql_uuid_value(data_file_id)
            ))
            .await
    }

    pub(crate) async fn update_data_file_rows_as_invalid(
        &mut self,
        mut data_file_record: DataFileRecord,
        invalid_row_ids: &HashSet<i64>,
    ) -> ILResult<usize> {
        if invalid_row_ids.is_empty() {
            return Ok(1);
        }

        for (row_id, valid) in data_file_record.validity.iter_mut_valid_row_ids() {
            if invalid_row_ids.contains(row_id) {
                *valid = false;
            }
        }
        self.update_data_file_validity(&data_file_record.data_file_id, &data_file_record.validity)
            .await
    }
}
