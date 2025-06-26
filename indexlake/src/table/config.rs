use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub inline_row_count_limit: usize,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            inline_row_count_limit: 10000,
        }
    }
}
