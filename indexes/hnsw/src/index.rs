use std::{any::Any, sync::Arc};

use indexlake::{
    ILError, ILResult,
    expr::Expr,
    index::{FilterIndexEntries, Index, RowIdScore, SearchIndexEntries, SearchQuery},
};

#[derive(Debug)]
pub struct HnswSearchQuery {
    pub vector: Vec<f32>,
    pub limit: usize,
}

impl SearchQuery for HnswSearchQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn index_kind(&self) -> &str {
        "hnsw"
    }

    fn limit(&self) -> Option<usize> {
        Some(self.limit)
    }
}

pub struct HnswIndex {
    index: Arc<usearch::Index>,
}

impl HnswIndex {
    pub fn new(index: Arc<usearch::Index>) -> Self {
        Self { index }
    }
}

impl std::fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIndex").finish()
    }
}

#[async_trait::async_trait]
impl Index for HnswIndex {
    async fn search(&self, query: &dyn SearchQuery) -> ILResult<SearchIndexEntries> {
        let query = query
            .as_any()
            .downcast_ref::<HnswSearchQuery>()
            .ok_or_else(|| {
                ILError::IndexError(format!(
                    "Hnsw index does not support search query: {query:?}"
                ))
            })?;
        let matches = self
            .index
            .search(&query.vector, query.limit)
            .map_err(|e| ILError::IndexError(format!("Failed to search Hnsw index: {e}")))?;
        let row_id_scores = matches
            .keys
            .iter()
            .zip(matches.distances.iter())
            .map(|(key, distance)| RowIdScore {
                row_id: *key as i64,
                score: *distance as f64,
            })
            .collect();
        Ok(SearchIndexEntries {
            row_id_scores,
            score_higher_is_better: false,
        })
    }

    async fn filter(&self, _filters: &[Expr]) -> ILResult<FilterIndexEntries> {
        Err(ILError::IndexError(format!(
            "Hnsw index does not support filter"
        )))
    }
}
