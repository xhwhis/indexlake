use std::any::Any;
use std::sync::Arc;

use indexlake::expr::Expr;
use indexlake::index::{
    IndexBuilder, IndexDefination, IndexDefinationRef, IndexKind, IndexParams, SearchQuery,
};
use indexlake::{ILError, ILResult};

#[derive(Debug)]
pub struct BTreeIndexKind;

impl IndexKind for BTreeIndexKind {
    fn kind(&self) -> &str {
        "btree"
    }

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>> {
        todo!()
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        Ok(())
    }

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        todo!()
    }

    fn supports_search(
        &self,
        index_def: &IndexDefination,
        query: &dyn SearchQuery,
    ) -> ILResult<bool> {
        todo!()
    }

    fn supports_filter(&self, index_def: &IndexDefination, filter: &Expr) -> ILResult<bool> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct BTreeIndexParams;

impl IndexParams for BTreeIndexParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encode(&self) -> ILResult<String> {
        Ok("{}".to_string())
    }
}
