mod helper;

pub(crate) use helper::*;

use crate::{
    ILResult,
    record::{Row, SchemaRef},
};
use std::fmt::Debug;

#[async_trait::async_trait]
pub trait Catalog: Debug + Send + Sync {
    /// Begin a new transaction.
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>>;
}

// Transaction should be rolled back when dropped.
#[async_trait::async_trait(?Send)]
pub trait Transaction: Debug {
    /// Execute a query and return the result.
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>>;

    /// Execute a SQL statement.
    async fn execute(&mut self, sql: &str) -> ILResult<()>;

    /// Commit the transaction.
    async fn commit(&mut self) -> ILResult<()>;

    /// Rollback the transaction.
    async fn rollback(&mut self) -> ILResult<()>;
}
