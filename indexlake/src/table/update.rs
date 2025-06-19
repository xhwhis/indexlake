use std::{collections::HashMap, sync::Arc};

use crate::{
    ILError, ILResult, TransactionHelper,
    expr::Expr,
    record::{INTERNAL_ROW_ID_FIELD, Scalar, SchemaRef},
};

pub(crate) async fn process_update_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    set_map: HashMap<String, Scalar>,
    condition: &Expr,
) -> ILResult<()> {
    tx_helper
        .update_inline_rows(table_id, &set_map, condition)
        .await?;

    Ok(())
}
