mod create;
mod insert;
mod scan;

use crate::schema::SchemaRef;
use crate::{Catalog, Storage};
pub use create::*;
pub use insert::*;
pub use scan::*;
use std::sync::Arc;

pub struct Table {
    namespace_id: i64,
    table_id: i64,
    table_name: String,
    schema: SchemaRef,
    catalog: Arc<dyn Catalog>,
    storage: Arc<Storage>,
}
