use std::{collections::HashMap, sync::Arc};

use crate::{
    ILError, ILResult,
    index::{FilterIndex, IndexDefination, TopKIndex},
};

#[derive(Debug, Clone)]
pub struct IndexKindManager {
    topk_index_kinds: HashMap<String, Arc<dyn TopKIndex>>,
    filter_index_kinds: HashMap<String, Arc<dyn FilterIndex>>,
}

impl IndexKindManager {
    pub fn new_empty() -> Self {
        Self {
            topk_index_kinds: HashMap::new(),
            filter_index_kinds: HashMap::new(),
        }
    }

    pub fn contains_kind(&self, kind: &str) -> bool {
        self.topk_index_kinds.contains_key(kind) || self.filter_index_kinds.contains_key(kind)
    }

    pub fn register_topk_index(&mut self, index: Arc<dyn TopKIndex>) -> ILResult<()> {
        let kind = index.kind();
        if self.contains_kind(kind) {
            return Err(ILError::InvalidInput(format!(
                "Index kind {kind} already registered"
            )));
        }
        self.topk_index_kinds.insert(kind.to_string(), index);
        Ok(())
    }

    pub fn register_filter_index(&mut self, index: Arc<dyn FilterIndex>) -> ILResult<()> {
        let kind = index.kind();
        if self.contains_kind(kind) {
            return Err(ILError::InvalidInput(format!(
                "Index kind {kind} already registered"
            )));
        }
        self.filter_index_kinds.insert(kind.to_string(), index);
        Ok(())
    }

    pub fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        if let Some(index) = self.topk_index_kinds.get(&index_def.kind) {
            index.supports(index_def)
        } else if let Some(index) = self.filter_index_kinds.get(&index_def.kind) {
            index.supports(index_def)
        } else {
            Err(ILError::InvalidInput(format!(
                "Index kind {} not found",
                index_def.kind
            )))
        }
    }
}
