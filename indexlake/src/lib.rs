pub mod catalog;
mod client;
mod error;
pub mod expr;
pub mod index;
pub mod storage;
pub mod table;
mod utils;

pub use client::*;
pub use error::*;

use arrow::array::RecordBatch;
use futures::Stream;
use std::pin::Pin;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = ILResult<RecordBatch>> + Send>>;
