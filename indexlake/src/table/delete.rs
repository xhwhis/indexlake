use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray, Int64Array};
use arrow::datatypes::{Int64Type, SchemaRef};
use futures::StreamExt;

use crate::catalog::{
    CatalogSchema, INTERNAL_ROW_ID_FIELD_NAME, RowLocation, Scalar, TransactionHelper,
    rows_to_record_batch,
};
use crate::expr::{Expr, visited_columns};
use crate::storage::Storage;
use crate::table::read_data_files_by_locations;
use crate::{ILError, ILResult};

pub(crate) async fn process_delete(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<()> {
    if visited_columns(condition) == vec![INTERNAL_ROW_ID_FIELD_NAME] {
        return process_delete_rows_by_row_id_condition(tx_helper, table_id, condition).await;
    }

    let inline_row_ids =
        find_matched_inline_row_ids(tx_helper, table_id, table_schema, condition).await?;
    let data_file_row_ids =
        find_matched_data_file_row_ids(tx_helper, storage, table_id, table_schema, condition)
            .await?;

    let row_ids = [inline_row_ids, data_file_row_ids].concat();

    // Delete rows by their internal row IDs
    tx_helper
        .mark_rows_deleted_by_row_ids(table_id, &row_ids)
        .await?;

    // Directly delete inline rows
    tx_helper
        .delete_inline_rows_by_row_ids(table_id, &row_ids)
        .await?;
    Ok(())
}

pub(crate) async fn find_matched_inline_row_ids(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<Vec<i64>> {
    let catalog_schema = Arc::new(CatalogSchema::from_arrow(&table_schema)?);
    let rows = tx_helper
        .scan_inline_rows(table_id, &catalog_schema)
        .await?;
    let record_batch = rows_to_record_batch(table_schema, &rows)?;

    let array = condition
        .eval(&record_batch)?
        .into_array(record_batch.num_rows())?;
    let bool_array = array.as_boolean_opt().ok_or_else(|| {
        ILError::InternalError(format!(
            "condition should return BooleanArray, but got {:?}",
            array.data_type()
        ))
    })?;

    let mut row_ids = Vec::new();
    for (i, v) in bool_array.iter().enumerate() {
        if let Some(v) = v
            && v
        {
            row_ids.push(
                rows[i]
                    .int64(0)?
                    .expect("Internal row ID should not be null"),
            );
        }
    }
    Ok(row_ids)
}

pub(crate) async fn find_matched_data_file_row_ids(
    tx_helper: &mut TransactionHelper,
    storage: Arc<Storage>,
    table_id: i64,
    table_schema: &SchemaRef,
    condition: &Expr,
) -> ILResult<Vec<i64>> {
    let row_metadata_condition =
        Expr::Column("deleted".to_string()).eq(Expr::Literal(Scalar::Boolean(Some(false))));
    let row_metadatas = tx_helper
        .scan_row_metadata(table_id, &row_metadata_condition)
        .await?;
    let data_file_locations = row_metadatas
        .into_iter()
        .filter(|meta| matches!(meta.location, RowLocation::Parquet { .. }))
        .map(|meta| meta.location)
        .collect::<Vec<_>>();

    let mut stream = read_data_files_by_locations(
        storage,
        table_schema.clone(),
        data_file_locations,
        Some(condition.clone()),
    )
    .await?;

    let mut row_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;

        let array = condition.eval(&batch)?.into_array(batch.num_rows())?;
        let bool_array = array.as_boolean_opt().ok_or_else(|| {
            ILError::InternalError(format!(
                "condition should return BooleanArray, but got {:?}",
                array.data_type()
            ))
        })?;

        let row_id_array = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| {
                ILError::InternalError(format!(
                    "row id array should be Int64Array, but got {:?}",
                    batch.column(0).data_type()
                ))
            })?;

        for (i, v) in bool_array.iter().enumerate() {
            if let Some(v) = v
                && v
            {
                row_ids.push(row_id_array.value(i));
            }
        }
    }
    Ok(row_ids)
}

pub(crate) async fn process_delete_rows_by_row_id_condition(
    tx_helper: &mut TransactionHelper,
    table_id: i64,
    row_id_condition: &Expr,
) -> ILResult<()> {
    tx_helper
        .mark_rows_deleted_by_condition(table_id, row_id_condition)
        .await?;
    tx_helper
        .delete_inline_rows_by_condition(table_id, row_id_condition)
        .await?;
    Ok(())
}
