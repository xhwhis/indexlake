use std::sync::Arc;

mod fs;
mod parquet;
mod s3;

use opendal::{Operator, services::S3Config};

use crate::{ILError, ILResult};

#[derive(Debug, Clone)]
pub enum Storage {
    LocalFs,
    S3 { config: Arc<S3Config> },
}

impl Storage {
    pub fn new_fs() -> Self {
        Storage::LocalFs
    }

    pub fn new_s3(config: S3Config) -> Self {
        Storage::S3 {
            config: Arc::new(config),
        }
    }

    pub async fn delete(&self, path: impl AsRef<str>) -> ILResult<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.delete(relative_path).await?)
    }

    pub async fn remove_dir_all(&self, path: impl AsRef<str>) -> ILResult<()> {
        let (op, relative_path) = self.create_operator(&path)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    pub async fn exists(&self, path: impl AsRef<str>) -> ILResult<bool> {
        let (op, relative_path) = self.create_operator(&path)?;
        Ok(op.exists(relative_path).await?)
    }

    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> ILResult<(Operator, &'a str)> {
        let path = path.as_ref();
        match self {
            Storage::LocalFs => {
                let op = fs::fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            Storage::S3 { config } => {
                let op = s3::s3_config_build(config, path)?;

                // Check prefix of s3 path.
                let prefix = format!("s3://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(ILError::StorageError(format!(
                        "Invalid s3 url: {path}, should start with {prefix}"
                    )))
                }
            }
        }
    }
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OutputFile {
    op: Operator,
    // Absolution path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: usize,
}
