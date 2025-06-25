mod helper;
mod record;

pub(crate) use helper::*;
pub(crate) use record::*;

use futures::Stream;

use crate::{
    ILResult,
    record::{Row, SchemaRef},
};
use std::{fmt::Debug, pin::Pin};

pub type RowStream<'a> = Pin<Box<dyn Stream<Item = ILResult<Row>> + Send + 'a>>;

#[async_trait::async_trait]
pub trait Catalog: Debug + Send + Sync {
    fn database(&self) -> CatalogDatabase;

    /// Begin a new transaction.
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>>;
}

#[derive(Debug, Clone, Copy)]
pub enum CatalogDatabase {
    Sqlite,
    Postgres,
}

impl std::fmt::Display for CatalogDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogDatabase::Sqlite => write!(f, "SQLite"),
            CatalogDatabase::Postgres => write!(f, "Postgres"),
        }
    }
}

// Transaction should be rolled back when dropped.
#[async_trait::async_trait]
pub trait Transaction: Debug + Send {
    /// Execute a query and return a stream of rows.
    async fn query<'a>(&'a mut self, sql: &str, schema: SchemaRef) -> ILResult<RowStream<'a>>;

    /// Execute a SQL statement.
    async fn execute(&mut self, sql: &str) -> ILResult<usize>;

    /// Execute a batch of SQL statements.
    async fn execute_batch(&mut self, sqls: &[String]) -> ILResult<()>;

    /// Commit the transaction.
    async fn commit(&mut self) -> ILResult<()>;

    /// Rollback the transaction.
    async fn rollback(&mut self) -> ILResult<()>;
}
