use std::{collections::HashMap, sync::Arc};

use crate::{
    ILError, ILResult, TransactionHelper,
    expr::{Expr, rewrite_inline_column_names},
    record::{INTERNAL_ROW_ID_FIELD, Scalar, SchemaRef},
};

pub(crate) async fn process_update_rows(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    set: HashMap<String, Scalar>,
    mut condition: Expr,
) -> ILResult<()> {
    let mut field_id_to_value_map = HashMap::new();
    for (field_name, value) in set {
        let field = table_schema
            .get_field_by_name(&field_name)
            .ok_or_else(|| ILError::InvalidInput(format!("Field {} not found", field_name)))?;
        let field_id = field.id.ok_or_else(|| {
            ILError::InvalidInput(format!("Field id is not set for field {}", field_name))
        })?;
        field_id_to_value_map.insert(field_id, value);
    }

    rewrite_inline_column_names(&mut condition, table_schema.clone());
    tx_helper
        .update_inline_rows(table_id, &field_id_to_value_map, &condition)
        .await?;

    Ok(())
}
