use arrow::array::Int64Array;
use indexlake::{
    ILError, ILResult,
    catalog::Scalar,
    expr::Expr,
    index::{FilterIndexEntries, Index, SearchIndexEntries, SearchQuery},
};
use rstar::{AABB, RTree, RTreeObject};

use crate::{RStarIndexParams, compute_aabb};

#[derive(Debug)]
pub struct IndexTreeObject {
    pub aabb: AABB<[f64; 2]>,
    pub row_id: i64,
}

impl RTreeObject for IndexTreeObject {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}

#[derive(Debug)]
pub struct RStarIndex {
    pub rtree: RTree<IndexTreeObject>,
    pub params: RStarIndexParams,
}

#[async_trait::async_trait]
impl Index for RStarIndex {
    async fn search(&self, _query: &dyn SearchQuery) -> ILResult<SearchIndexEntries> {
        Err(ILError::NotSupported(format!(
            "RStar index does not support search"
        )))
    }

    async fn filter(&self, filters: &[Expr]) -> ILResult<FilterIndexEntries> {
        let aabb = match &filters[0] {
            Expr::Literal(Scalar::Binary(Some(wkb))) => {
                let aabb = compute_aabb(wkb, self.params.wkb_dialect)?;
                AABB::from_corners(
                    [aabb.lower().x, aabb.lower().y],
                    [aabb.upper().x, aabb.upper().y],
                )
            }
            _ => todo!(),
        };

        let selection = self.rtree.locate_in_envelope_intersecting(&aabb);
        let row_ids = selection
            .into_iter()
            .map(|object| object.row_id)
            .collect::<Vec<_>>();

        Ok(FilterIndexEntries {
            row_ids: Int64Array::from(row_ids),
        })
    }
}
