mod create;
mod insert;
mod scan;

pub use create::*;
pub use insert::*;
pub use scan::*;

use crate::record::{Row, SchemaRef};
use crate::{Catalog, ILResult, Storage, TransactionHelper};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: i64,
    pub namespace_name: String,
    pub table_id: i64,
    pub table_name: String,
    pub schema: SchemaRef,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
}

impl Table {
    pub(crate) async fn transaction_helper(&self) -> ILResult<TransactionHelper> {
        let transaction = self.catalog.transaction().await?;
        Ok(TransactionHelper::new(transaction))
    }

    pub async fn insert_rows(&self, rows: Vec<Row>) -> ILResult<()> {
        let mut tx_helper = self.transaction_helper().await?;
        process_insert_rows(&mut tx_helper, self.table_id, &self.schema, rows).await?;
        tx_helper.commit().await?;
        Ok(())
    }

    pub async fn scan(&self) -> ILResult<Vec<Row>> {
        let mut tx_helper = self.transaction_helper().await?;
        let rows = process_table_scan(&mut tx_helper, self.table_id, &self.schema).await?;
        tx_helper.commit().await?;
        Ok(rows)
    }
}
