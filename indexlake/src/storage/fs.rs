use std::path::PathBuf;

use opendal::{Operator, services::FsConfig};

use crate::ILResult;

#[derive(Debug, Clone)]
pub(crate) struct FsStorage {
    root: PathBuf,
}

impl FsStorage {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) fn new_operator(&self) -> ILResult<Operator> {
        let mut cfg = FsConfig::default();
        cfg.root = Some(self.root.to_string_lossy().to_string());
        Ok(Operator::from_config(cfg)?.finish())
    }
}
