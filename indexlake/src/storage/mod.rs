use std::{
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod fs;
mod parquet;
mod s3;

use opendal::{Operator, services::S3Config};

use crate::{
    ILError, ILResult,
    storage::{fs::FsStorage, s3::S3Storage},
};

#[derive(Debug, Clone)]
pub enum Storage {
    Fs(FsStorage),
    S3(S3Storage),
}

impl Storage {
    pub fn new_fs(root: impl Into<PathBuf>) -> Self {
        Storage::Fs(FsStorage::new(root.into()))
    }

    pub fn new_s3(config: S3Config, bucket: impl Into<String>) -> Self {
        Storage::S3(S3Storage::new(config, bucket.into()))
    }

    pub async fn delete(&self, relative_path: &str) -> ILResult<()> {
        let op = self.new_operator()?;
        Ok(op.delete(relative_path).await?)
    }

    pub async fn remove_dir_all(&self, relative_path: &str) -> ILResult<()> {
        let op = self.new_operator()?;
        let relative_path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&relative_path).await?)
    }

    pub async fn exists(&self, relative_path: &str) -> ILResult<bool> {
        let op = self.new_operator()?;
        Ok(op.exists(relative_path).await?)
    }

    pub(crate) fn new_operator(&self) -> ILResult<Operator> {
        match self {
            Storage::Fs(fs) => fs.new_operator(),
            Storage::S3(s3) => s3.new_operator(),
        }
    }

    pub async fn new_storage_file(&self, relative_path: &str) -> ILResult<StorageFile> {
        let op = self.new_operator()?;
        let reader = op.reader(relative_path).await?;
        Ok(StorageFile {
            op,
            relative_path: relative_path.to_string(),
            reader,
            writer: None,
        })
    }
}

/// Storage file is used for reading and writing to files.
pub struct StorageFile {
    op: Operator,
    relative_path: String,
    reader: opendal::Reader,
    writer: Option<opendal::Writer>,
}

impl StorageFile {
    pub async fn file_size_bytes(&self) -> ILResult<u64> {
        let meta = self.op.stat(&self.relative_path).await?;
        Ok(meta.content_length())
    }

    pub async fn exists(&self) -> ILResult<bool> {
        Ok(self.op.exists(&self.relative_path).await?)
    }

    pub async fn delete(&self) -> ILResult<()> {
        Ok(self.op.delete(&self.relative_path).await?)
    }

    pub async fn read(&self) -> ILResult<bytes::Bytes> {
        Ok(self.op.read(&self.relative_path).await?.to_bytes())
    }

    pub async fn write(&self, bytes: bytes::Bytes) -> ILResult<()> {
        let mut writer = self.op.writer(&self.relative_path).await?;
        writer.write(bytes).await?;
        writer.close().await?;
        Ok(())
    }
}
