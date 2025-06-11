use opendal::{Operator, services::FsConfig};

use crate::ILResult;

/// Build new opendal operator from give path.
pub(crate) fn fs_config_build() -> ILResult<Operator> {
    let mut cfg = FsConfig::default();
    #[cfg(target_os = "windows")]
    {
        cfg.root = Some("C:/".to_string());
    }
    #[cfg(not(target_os = "windows"))]
    {
        cfg.root = Some("/".to_string());
    }

    Ok(Operator::from_config(cfg)?.finish())
}
