mod create;
mod insert;
mod scan;

use crate::schema::SchemaRef;
use crate::{Catalog, Storage};
pub use create::*;
pub use insert::*;
pub use scan::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub namespace_id: i64,
    pub namespace_name: String,
    pub table_id: i64,
    pub table_name: String,
    pub schema: SchemaRef,
    pub catalog: Arc<dyn Catalog>,
    pub storage: Arc<Storage>,
}
