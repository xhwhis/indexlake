mod record;
mod schema;

pub use record::*;
pub use schema::*;

use crate::ILResult;
use arrow::array::RecordBatch;
use futures::Stream;
use std::pin::Pin;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = ILResult<RecordBatch>> + Send>>;
