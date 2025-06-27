mod create;
mod delete;
mod drop;
mod insert;
mod query;
mod truncate;
mod update;

use std::sync::Arc;

use futures::TryStreamExt;

use crate::{
    ILResult,
    catalog::{Catalog, CatalogDatabase, Transaction},
    record::{CatalogSchemaRef, Row},
};

pub(crate) struct TransactionHelper {
    pub(crate) transaction: Box<dyn Transaction>,
    pub(crate) database: CatalogDatabase,
}

impl TransactionHelper {
    pub(crate) async fn new(catalog: &Arc<dyn Catalog>) -> ILResult<Self> {
        let transaction = catalog.transaction().await?;
        Ok(Self {
            transaction,
            database: catalog.database(),
        })
    }

    pub(crate) async fn query_rows(
        &mut self,
        sql: &str,
        schema: CatalogSchemaRef,
    ) -> ILResult<Vec<Row>> {
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
