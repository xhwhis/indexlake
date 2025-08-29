use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use indexlake::expr::Expr;
use indexlake::index::{
    FilterSupport, IndexBuilder, IndexDefination, IndexDefinationRef, IndexKind, IndexParams,
    SearchQuery,
};
use indexlake::{ILError, ILResult};
use serde::{Deserialize, Serialize};

use crate::{BTreeIndexBuilder, BTreeSearchQuery};

#[derive(Debug)]
pub struct BTreeIndexKind;

impl IndexKind for BTreeIndexKind {
    fn kind(&self) -> &str {
        "btree"
    }

    fn decode_params(&self, params: &str) -> ILResult<Arc<dyn IndexParams>> {
        let params: BTreeIndexParams = serde_json::from_str(params)
            .map_err(|e| ILError::index(format!("Failed to decode B-tree index params: {e}")))?;
        Ok(Arc::new(params))
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        if index_def.key_columns.len() != 1 {
            return Err(ILError::index(
                "B-tree index requires exactly one key column",
            ));
        }
        let key_column_name = &index_def.key_columns[0];
        let key_field = index_def.table_schema.field_with_name(key_column_name)?;

        match key_field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Timestamp(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => Ok(()),
            _ => {
                Err(ILError::index(
                    "B-tree index key column must be a comparable data type (integers, floats, strings, timestamps, dates)",
                ))
            }
        }
    }

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(BTreeIndexBuilder::try_new(index_def.clone())?))
    }

    fn supports_search(
        &self, _index_def: &IndexDefination, query: &dyn SearchQuery,
    ) -> ILResult<bool> {
        if query.as_any().downcast_ref::<BTreeSearchQuery>().is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn supports_filter(
        &self, _index_def: &IndexDefination, _filter: &Expr,
    ) -> ILResult<FilterSupport> {
        Ok(FilterSupport::Unsupported)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeIndexParams {
    pub node_size: usize,
}

impl IndexParams for BTreeIndexParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encode(&self) -> ILResult<String> {
        serde_json::to_string(self).map_err(|e| ILError::index(e.to_string()))
    }
}
