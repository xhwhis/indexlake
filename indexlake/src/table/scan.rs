use std::sync::Arc;

use crate::{Storage, schema::SchemaRef};

pub struct TableScan {
    table_id: i64,
    storage: Arc<Storage>,
    schema: SchemaRef,
}
