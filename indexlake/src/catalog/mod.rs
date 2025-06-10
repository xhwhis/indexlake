mod helper;
mod record;

pub use record::*;

use crate::ILResult;
use std::fmt::Debug;

#[async_trait::async_trait]
pub trait Catalog: Debug + Send + Sync {
    /// Begin a new transaction.
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>>;
}

#[async_trait::async_trait(?Send)]
pub trait Transaction: Debug {
    /// Execute a query and return the result.
    async fn query(&mut self, sql: &str, schema: CatalogSchemaRef) -> ILResult<Vec<CatalogRow>>;
    /// Execute a SQL statement.
    async fn execute(&mut self, sql: &str) -> ILResult<()>;
    /// Commit the transaction.
    async fn commit(self) -> ILResult<()>;
    /// Rollback the transaction.
    async fn rollback(self) -> ILResult<()>;
}
