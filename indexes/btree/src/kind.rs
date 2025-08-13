use std::any::Any;
use std::sync::Arc;

use indexlake::ILResult;
use indexlake::expr::Expr;
use indexlake::index::{
    IndexBuilder, IndexDefination, IndexDefinationRef, IndexKind, IndexParams, SearchQuery,
};

#[derive(Debug)]
pub struct BTreeIndexKind;

impl IndexKind for BTreeIndexKind {
    fn kind(&self) -> &str {
        "btree"
    }

    fn decode_params(&self, _value: &str) -> ILResult<Arc<dyn IndexParams>> {
        Ok(Arc::new(BTreeIndexParams))
    }

    fn supports(&self, _index_def: &IndexDefination) -> ILResult<()> {
        Ok(())
    }

    fn builder(&self, _index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        todo!()
    }

    fn supports_search(
        &self,
        _index_def: &IndexDefination,
        _query: &dyn SearchQuery,
    ) -> ILResult<bool> {
        todo!()
    }

    fn supports_filter(&self, _index_def: &IndexDefination, _filter: &Expr) -> ILResult<bool> {
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
