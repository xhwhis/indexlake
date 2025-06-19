mod create;
mod delete;
mod insert;
mod query;
mod update;

use futures::TryStreamExt;

use crate::{
    ILResult,
    catalog::Transaction,
    record::{Row, SchemaRef},
};

pub(crate) const INLINE_COLUMN_NAME_PREFIX: &str = "col_";

pub(crate) struct TransactionHelper {
    transaction: Box<dyn Transaction>,
}

impl TransactionHelper {
    pub(crate) fn new(transaction: Box<dyn Transaction>) -> Self {
        Self { transaction }
    }

    pub(crate) async fn query_rows(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>> {
        let stream = self.transaction.query(sql, schema).await?;
        stream.try_collect::<Vec<_>>().await
    }

    pub(crate) async fn commit(&mut self) -> ILResult<()> {
        self.transaction.commit().await
    }

    pub(crate) async fn rollback(&mut self) -> ILResult<()> {
        self.transaction.rollback().await
    }
}
