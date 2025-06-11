use opendal::{Configurator, Operator, services::S3Config};

use crate::{ILError, ILResult};

/// Build new opendal operator from give path.
pub(crate) fn s3_config_build(cfg: &S3Config, path: &str) -> ILResult<Operator> {
    let url = url::Url::parse(path).map_err(|e| ILError::StorageError(e.to_string()))?;
    let bucket = url
        .host_str()
        .ok_or_else(|| ILError::StorageError(format!("Invalid s3 url: {path}, missing bucket")))?;

    let builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket);

    Ok(Operator::new(builder)?.finish())
}
