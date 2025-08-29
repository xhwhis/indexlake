use indexlake::catalog::Scalar;

#[derive(Debug)]
pub struct BTreeNode {
    pub keys: Vec<Scalar>,
    pub children: Vec<usize>,
    pub row_ids: Vec<i64>,
    pub is_leaf: bool,
    pub parent: Option<usize>,
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
            row_ids: Vec::new(),
            is_leaf: true,
            parent: None,
        }
    }

    pub fn new_internal() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
            row_ids: Vec::new(),
            is_leaf: false,
            parent: None,
        }
    }

    pub fn is_full(&self, node_size: usize) -> bool {
        self.keys.len() >= node_size
    }

    pub fn find_child_index(&self, key: &Scalar) -> usize {
        match self
            .keys
            .binary_search_by(|k| k.partial_cmp(key).unwrap_or(std::cmp::Ordering::Equal))
        {
            Ok(index) => index + 1,
            Err(index) => index,
        }
    }

    pub fn find_key_index(&self, key: &Scalar) -> Result<usize, usize> {
        self.keys
            .binary_search_by(|k| k.partial_cmp(key).unwrap_or(std::cmp::Ordering::Equal))
    }
}
