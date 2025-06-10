mod helper;
mod record;

pub use record::*;

use crate::ILResult;
use std::fmt::Debug;

#[async_trait::async_trait]
pub trait Catalog: Debug + Send + Sync {
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>>;
}

#[async_trait::async_trait(?Send)]
pub trait Transaction: Debug {
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>>;
    async fn execute(&mut self, sql: &str) -> ILResult<()>;
    async fn commit(self) -> ILResult<()>;
    async fn rollback(self) -> ILResult<()>;
}
