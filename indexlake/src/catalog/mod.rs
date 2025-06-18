mod helper;

use futures::Stream;
pub(crate) use helper::*;

use crate::{
    ILResult,
    record::{Row, SchemaRef},
};
use std::{fmt::Debug, pin::Pin};

#[async_trait::async_trait]
pub trait Catalog: Debug + Send + Sync {
    /// Begin a new transaction.
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>>;
}

pub type RowStream = Pin<Box<dyn Stream<Item = ILResult<Row>> + Send>>;

// Transaction should be rolled back when dropped.
#[async_trait::async_trait(?Send)]
pub trait Transaction: Debug {
    /// Execute a query and return a stream of rows.
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<RowStream>;

    /// Execute a SQL statement.
    async fn execute(&mut self, sql: &str) -> ILResult<()>;

    /// Commit the transaction.
    async fn commit(&mut self) -> ILResult<()>;

    /// Rollback the transaction.
    async fn rollback(&mut self) -> ILResult<()>;
}
