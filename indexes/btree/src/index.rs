use std::any::Any;

use indexlake::ILResult;
use indexlake::catalog::Scalar;
use indexlake::expr::Expr;
use indexlake::index::{FilterIndexEntries, Index, RowIdScore, SearchIndexEntries, SearchQuery};

use crate::BTreeNode;

#[derive(Debug)]
pub struct BTreeIndex {
    nodes: Vec<BTreeNode>,
    root: usize,
    node_size: usize,
}

#[derive(Debug)]
pub enum BTreeSearchQuery {
    Point {
        key: Scalar,
        limit: Option<usize>,
    },
    Range {
        start: Option<Scalar>,
        end: Option<Scalar>,
        limit: Option<usize>,
    },
}

impl SearchQuery for BTreeSearchQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn index_kind(&self) -> &str {
        "btree"
    }

    fn limit(&self) -> Option<usize> {
        match self {
            BTreeSearchQuery::Point { limit, .. } => *limit,
            BTreeSearchQuery::Range { limit, .. } => *limit,
        }
    }
}

impl BTreeIndex {
    pub fn new(node_size: usize) -> Self {
        let root_node = BTreeNode::new_leaf();
        Self {
            nodes: vec![root_node],
            root: 0,
            node_size,
        }
    }

    pub fn insert(&mut self, key: Scalar, row_id: i64) -> ILResult<()> {
        let leaf_idx = self.find_leaf(&key);
        self.insert_into_leaf(leaf_idx, key, row_id)?;
        Ok(())
    }

    fn find_leaf(&self, key: &Scalar) -> usize {
        let mut current = self.root;

        while !self.nodes[current].is_leaf {
            let child_idx = self.nodes[current].find_child_index(key);
            current = self.nodes[current].children[child_idx];
        }

        current
    }

    fn insert_into_leaf(&mut self, leaf_idx: usize, key: Scalar, row_id: i64) -> ILResult<()> {
        let leaf = &mut self.nodes[leaf_idx];

        let pos = match leaf.find_key_index(&key) {
            Ok(_) => return Ok(()),
            Err(pos) => pos,
        };

        leaf.keys.insert(pos, key);
        leaf.row_ids.insert(pos, row_id);

        if leaf.is_full(self.node_size) {
            self.split_leaf(leaf_idx)?;
        }

        Ok(())
    }

    fn split_leaf(&mut self, leaf_idx: usize) -> ILResult<()> {
        let mid = self.node_size / 2;

        let mut right_leaf = BTreeNode::new_leaf();

        let left_leaf = &mut self.nodes[leaf_idx];
        right_leaf.keys = left_leaf.keys.split_off(mid);
        right_leaf.row_ids = left_leaf.row_ids.split_off(mid);
        right_leaf.parent = left_leaf.parent;

        let right_idx = self.nodes.len();
        self.nodes.push(right_leaf);

        let promote_key = self.nodes[right_idx].keys[0].clone();

        if let Some(parent_idx) = self.nodes[leaf_idx].parent {
            self.insert_into_internal(parent_idx, promote_key, right_idx)?;
        } else {
            let mut new_root = BTreeNode::new_internal();
            new_root.keys.push(promote_key);
            new_root.children.push(leaf_idx);
            new_root.children.push(right_idx);

            let new_root_idx = self.nodes.len();
            self.nodes.push(new_root);

            self.nodes[leaf_idx].parent = Some(new_root_idx);
            self.nodes[right_idx].parent = Some(new_root_idx);

            self.root = new_root_idx;
        }

        Ok(())
    }

    fn insert_into_internal(
        &mut self, node_idx: usize, key: Scalar, child_idx: usize,
    ) -> ILResult<()> {
        let should_split = {
            let node = &mut self.nodes[node_idx];

            let pos = match node.find_key_index(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };

            node.keys.insert(pos, key);
            node.children.insert(pos + 1, child_idx);

            node.is_full(self.node_size)
        };

        self.nodes[child_idx].parent = Some(node_idx);

        if should_split {
            self.split_internal(node_idx)?;
        }

        Ok(())
    }

    fn split_internal(&mut self, node_idx: usize) -> ILResult<()> {
        let mid = self.node_size / 2;

        let mut right_node = BTreeNode::new_internal();

        let promote_key = {
            let left_node = &mut self.nodes[node_idx];
            let promote_key = left_node.keys.remove(mid);
            right_node.keys = left_node.keys.split_off(mid);
            right_node.children = left_node.children.split_off(mid + 1);
            right_node.parent = left_node.parent;
            promote_key
        };

        let right_idx = self.nodes.len();
        self.nodes.push(right_node);

        for &child_idx in &self.nodes[right_idx].children.clone() {
            self.nodes[child_idx].parent = Some(right_idx);
        }

        if let Some(parent_idx) = self.nodes[node_idx].parent {
            self.insert_into_internal(parent_idx, promote_key, right_idx)?;
        } else {
            let mut new_root = BTreeNode::new_internal();
            new_root.keys.push(promote_key);
            new_root.children.push(node_idx);
            new_root.children.push(right_idx);

            let new_root_idx = self.nodes.len();
            self.nodes.push(new_root);

            self.nodes[node_idx].parent = Some(new_root_idx);
            self.nodes[right_idx].parent = Some(new_root_idx);

            self.root = new_root_idx;
        }

        Ok(())
    }

    fn search_point(&self, key: &Scalar) -> Vec<i64> {
        let leaf_idx = self.find_leaf(key);
        let leaf = &self.nodes[leaf_idx];

        match leaf.find_key_index(key) {
            Ok(pos) => {
                vec![leaf.row_ids[pos]]
            }
            Err(_) => Vec::new(),
        }
    }

    fn search_range(&self, min: Option<&Scalar>, max: Option<&Scalar>) -> Vec<i64> {
        let mut results = Vec::with_capacity(512);
        self.collect_range_results(self.root, min, max, &mut results);
        results
    }

    fn collect_range_results(
        &self, node_idx: usize, min: Option<&Scalar>, max: Option<&Scalar>, results: &mut Vec<i64>,
    ) {
        let node = &self.nodes[node_idx];

        if node.is_leaf {
            let (start_pos, end_pos) = self.find_range_bounds(&node.keys, min, max);
            results.extend_from_slice(&node.row_ids[start_pos..end_pos]);
        } else {
            for (i, &child_idx) in node.children.iter().enumerate() {
                let should_check =
                    self.should_check_child(i, &node.keys, node.children.len(), min, max);
                if should_check {
                    self.collect_range_results(child_idx, min, max, results);
                }
            }
        }
    }

    fn should_check_child(
        &self, child_index: usize, keys: &[Scalar], children_len: usize, min: Option<&Scalar>,
        max: Option<&Scalar>,
    ) -> bool {
        match (min, max) {
            (Some(min_key), Some(max_key)) => {
                if child_index == 0 {
                    keys.is_empty() || &keys[0] >= min_key
                } else if child_index == children_len - 1 {
                    &keys[child_index - 1] <= max_key
                } else {
                    &keys[child_index - 1] <= max_key && &keys[child_index] >= min_key
                }
            }
            (Some(min_key), None) => {
                child_index == children_len - 1 || &keys[child_index] >= min_key
            }
            (None, Some(max_key)) => child_index == 0 || &keys[child_index - 1] <= max_key,
            (None, None) => true,
        }
    }

    fn find_range_bounds(
        &self, keys: &[Scalar], min: Option<&Scalar>, max: Option<&Scalar>,
    ) -> (usize, usize) {
        let start_pos = match min {
            Some(min_key) => {
                match keys.binary_search_by(|k| {
                    k.partial_cmp(min_key).unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    Ok(pos) => pos,
                    Err(pos) => pos,
                }
            }
            None => 0,
        };

        let end_pos = match max {
            Some(max_key) => {
                match keys.binary_search_by(|k| {
                    k.partial_cmp(max_key).unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                }
            }
            None => keys.len(),
        };

        (start_pos, end_pos.min(keys.len()))
    }
}

#[async_trait::async_trait]
impl Index for BTreeIndex {
    async fn search(&self, query: &dyn SearchQuery) -> ILResult<SearchIndexEntries> {
        let btree_query = query
            .as_any()
            .downcast_ref::<BTreeSearchQuery>()
            .ok_or_else(|| indexlake::ILError::index("Invalid query type for B-tree index"))?;

        let row_ids = match btree_query {
            BTreeSearchQuery::Point { key, .. } => self.search_point(key),
            BTreeSearchQuery::Range { start, end, .. } => {
                self.search_range(start.as_ref(), end.as_ref())
            }
        };

        let mut row_id_scores = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            row_id_scores.push(RowIdScore { row_id, score: 1.0 });
        }
        if let Some(limit) = btree_query.limit() {
            row_id_scores.truncate(limit);
        }

        Ok(SearchIndexEntries {
            row_id_scores,
            score_higher_is_better: false,
        })
    }

    async fn filter(&self, _filters: &[Expr]) -> ILResult<FilterIndexEntries> {
        Err(indexlake::ILError::not_supported(
            "B-tree filter operations are not yet implemented",
        ))
    }
}
