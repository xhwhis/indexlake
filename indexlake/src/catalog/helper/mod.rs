mod create;
mod delete;
mod insert;
mod query;
mod update;

use futures::TryStreamExt;

use crate::{
    CatalogDatabase, ILResult,
    catalog::Transaction,
    record::{Row, SchemaRef},
};

pub(crate) struct TransactionHelper {
    pub(crate) transaction: Box<dyn Transaction>,
    pub(crate) database: CatalogDatabase,
}

impl TransactionHelper {
    pub(crate) fn new(transaction: Box<dyn Transaction>, database: CatalogDatabase) -> Self {
        Self {
            transaction,
            database,
        }
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
