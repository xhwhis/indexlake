use std::{any::Any, sync::Arc};

use arrow::datatypes::DataType;
use indexlake::ILError;
use indexlake::index::{
    IndexBuilder, IndexDefination, IndexDefinationRef, IndexKind, IndexParams, SearchQuery,
};
use indexlake::{ILResult, expr::Expr};
use serde::{Deserialize, Serialize};

use crate::{HnswIndexBuilder, HnswSearchQuery};

#[derive(Debug)]
pub struct HnswIndexKind;

impl IndexKind for HnswIndexKind {
    fn kind(&self) -> &str {
        "hnsw"
    }

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>> {
        let params: HnswIndexParams = serde_json::from_str(value).map_err(|e| {
            ILError::InternalError(format!("Failed to decode HnswIndexParams: {}", e))
        })?;
        Ok(Arc::new(params))
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        if index_def.key_columns.len() != 1 {
            return Err(ILError::IndexError(format!(
                "Hnsw index requires exactly one key column"
            )));
        }
        let key_column_name = &index_def.key_columns[0];
        let key_field = index_def.table_schema.field_with_name(&key_column_name)?;
        match key_field.data_type() {
            DataType::List(inner) => {
                if !matches!(inner.data_type(), DataType::Float32) || inner.is_nullable() {
                    return Err(ILError::IndexError(format!(
                        "Hnsw index key column must be a list of non-nullable float32"
                    )));
                }
            }
            _ => {
                return Err(ILError::IndexError(format!(
                    "Hnsw index key column must be a list of non-nullable float32"
                )));
            }
        }
        Ok(())
    }

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(HnswIndexBuilder::try_new(index_def.clone())?))
    }

    fn supports_search(
        &self,
        index_def: &IndexDefination,
        query: &dyn SearchQuery,
    ) -> ILResult<bool> {
        let Some(query) = query.as_any().downcast_ref::<HnswSearchQuery>() else {
            return Ok(false);
        };
        let params = index_def.downcast_params::<HnswIndexParams>()?;
        if query.vector.len() != params.dimensions {
            return Err(ILError::IndexError(format!(
                "Hnsw index query dimensions mismatch: {} != {}",
                query.vector.len(),
                params.dimensions
            )));
        }
        Ok(true)
    }

    fn supports_filter(&self, _index_def: &IndexDefination, _filter: &Expr) -> ILResult<bool> {
        Err(ILError::NotSupported(
            "HnswIndex does not support filter".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswIndexParams {
    pub dimensions: usize,
    pub distance: DistanceKind,
    pub connectivity: usize,
}

impl IndexParams for HnswIndexParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn encode(&self) -> ILResult<String> {
        let json = serde_json::to_string(self).map_err(|e| {
            ILError::InternalError(format!("Failed to encode HnswIndexParams: {}", e))
        })?;
        Ok(json)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistanceKind {
    L2,
    Cosine,
    Hamming,
    Divergence,
}

impl DistanceKind {
    pub fn to_usearch(&self) -> usearch::MetricKind {
        match self {
            DistanceKind::L2 => usearch::MetricKind::L2sq,
            DistanceKind::Cosine => usearch::MetricKind::Cos,
            DistanceKind::Hamming => usearch::MetricKind::Hamming,
            DistanceKind::Divergence => usearch::MetricKind::Divergence,
        }
    }
}
