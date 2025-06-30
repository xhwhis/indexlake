use std::{collections::HashMap, sync::Arc};

use crate::{
    ILError, ILResult,
    catalog::{
        CatalogSchemaRef, INTERNAL_ROW_ID_FIELD, INTERNAL_ROW_ID_FIELD_NAME, Scalar,
        TransactionHelper,
    },
    expr::{Expr, visited_columns},
};

pub(crate) async fn process_update_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
) -> ILResult<()> {
    if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
        return process_update_rows_by_row_id_condition(tx_helper, table_id, set_map, condition)
            .await;
    }
    tx_helper
        .update_inline_rows(table_id, &set_map, condition)
        .await?;

    Ok(())
}

pub(crate) async fn process_update_rows_by_row_id_condition(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
) -> ILResult<()> {
    let condition_with_deleted = condition
        .clone()
        .and(Expr::Column("deleted".to_string()).eq(Expr::Literal(Scalar::Boolean(Some(false)))));
    let _row_ids = tx_helper
        .scan_row_metadata(table_id, &condition_with_deleted)
        .await?;
    // TODO insert new rows
    tx_helper
        .update_inline_rows(table_id, &set_map, condition)
        .await?;
    Ok(())
}
