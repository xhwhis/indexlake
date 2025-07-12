use std::any::Any;

use arrow::array::{Float64Array, Int64Array};
use bm25::{Embedder, Scorer};
use indexlake::{
    ILError, ILResult,
    expr::Expr,
    index::{FilterIndexEntries, Index, IndexDefinationRef, SearchIndexEntries, SearchQuery},
};

use crate::BM25IndexParams;

#[derive(Debug)]
pub struct BM25SearchQuery {
    pub query: String,
    pub limit: Option<usize>,
}

impl SearchQuery for BM25SearchQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn limit(&self) -> Option<usize> {
        self.limit
    }
}

pub struct BM25Index {
    pub index_def: IndexDefinationRef,
    pub params: BM25IndexParams,
    pub embedder: Embedder,
    pub scorer: Scorer<i64>,
}

#[async_trait::async_trait]
impl Index for BM25Index {
    async fn search(&self, query: &dyn SearchQuery) -> ILResult<SearchIndexEntries> {
        let query = query
            .as_any()
            .downcast_ref::<BM25SearchQuery>()
            .ok_or(ILError::IndexError("Invalid query type".to_string()))?;
        let query_embedding = self.embedder.embed(&query.query);
        let mut matches = self.scorer.matches(&query_embedding);
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }

        let (row_ids, scores): (Vec<_>, Vec<_>) = matches
            .into_iter()
            .map(|doc| (doc.id, doc.score as f64))
            .unzip();
        let row_ids_array = Int64Array::from(row_ids);
        let scores_array = Float64Array::from(scores);

        Ok(SearchIndexEntries {
            row_ids: row_ids_array,
            scores: scores_array,
            score_higher_is_better: true,
        })
    }

    async fn filter(&self, _filters: &[Expr]) -> ILResult<FilterIndexEntries> {
        Err(ILError::IndexError(
            "BM25 index does not support filter".to_string(),
        ))
    }
}

impl std::fmt::Debug for BM25Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BM25Index")
            .field("index_def", &self.index_def)
            .field("params", &self.params)
            .field("embedder", &self.embedder)
            .finish()
    }
}
