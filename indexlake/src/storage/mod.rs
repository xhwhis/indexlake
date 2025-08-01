mod fs;
mod lance;
mod parquet;
mod s3;

use arrow_schema::Schema;
pub use fs::*;
pub use lance::*;
pub use opendal::services::S3Config;
pub use parquet::*;
pub use s3::*;

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::DataFileRecord,
    expr::Expr,
    storage::{fs::FsStorage, s3::S3Storage},
};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
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

    pub fn root_path(&self) -> ILResult<String> {
        match self {
            Storage::Fs(fs) => Ok(fs.root.to_string_lossy().to_string()),
            Storage::S3(s3) => Ok(format!("s3://{}/", s3.bucket)),
        }
    }

    pub fn storage_options(&self) -> HashMap<String, String> {
        match self {
            Storage::Fs(fs) => HashMap::new(),
            Storage::S3(s3) => {
                // TODO fix
                let mut options = HashMap::new();
                options.insert("aws_access_key_id".to_string(), "admin".to_string());
                options.insert("aws_secret_access_key".to_string(), "password".to_string());
                options.insert(
                    "aws_endpoint".to_string(),
                    "http://127.0.0.1:9000".to_string(),
                );
                options.insert("aws_allow_http".to_string(), "true".to_string());
                options.insert("aws_region".to_string(), "us-east-1".to_string());
                options.insert("aws_default_region".to_string(), "us-east-1".to_string());
                options.insert("AWS_EC2_METADATA_DISABLED".to_string(), "true".to_string());
                options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
                options
            }
        }
    }

    pub async fn create_file(&self, relative_path: &str) -> ILResult<OutputFile> {
        let op = self.new_operator()?;
        let writer = op.writer(relative_path).await?;
        Ok(OutputFile {
            op,
            relative_path: relative_path.to_string(),
            writer,
        })
    }

    pub async fn open_file(&self, relative_path: &str) -> ILResult<InputFile> {
        let op = self.new_operator()?;
        let reader = op.reader(relative_path).await?;
        Ok(InputFile {
            op,
            relative_path: relative_path.to_string(),
            reader,
        })
    }
}

/// Output file is used for writing to files.
pub struct OutputFile {
    op: Operator,
    relative_path: String,
    writer: opendal::Writer,
}

impl OutputFile {
    pub async fn file_size_bytes(&self) -> ILResult<u64> {
        let meta = self.op.stat(&self.relative_path).await?;
        Ok(meta.content_length())
    }

    pub async fn delete(&self) -> ILResult<()> {
        Ok(self.op.delete(&self.relative_path).await?)
    }

    pub fn writer(&mut self) -> &mut opendal::Writer {
        &mut self.writer
    }

    pub async fn close(&mut self) -> ILResult<()> {
        self.writer.close().await?;
        Ok(())
    }
}

pub struct InputFile {
    op: Operator,
    relative_path: String,
    reader: opendal::Reader,
}

impl InputFile {
    pub async fn file_size_bytes(&self) -> ILResult<u64> {
        let meta = self.op.stat(&self.relative_path).await?;
        Ok(meta.content_length())
    }

    pub async fn delete(&self) -> ILResult<()> {
        Ok(self.op.delete(&self.relative_path).await?)
    }

    pub async fn read(&self) -> ILResult<bytes::Bytes> {
        Ok(self.op.read(&self.relative_path).await?.to_bytes())
    }

    pub fn reader(&self) -> &opendal::Reader {
        &self.reader
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataFileFormat {
    ParquetV1,
    ParquetV2,
    LanceV2_1,
}

impl std::fmt::Display for DataFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::ParquetV1 => write!(f, "ParquetV1"),
            DataFileFormat::ParquetV2 => write!(f, "ParquetV2"),
            DataFileFormat::LanceV2_1 => write!(f, "LanceV2_1"),
        }
    }
}

impl std::str::FromStr for DataFileFormat {
    type Err = ILError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ParquetV1" => Ok(DataFileFormat::ParquetV1),
            "ParquetV2" => Ok(DataFileFormat::ParquetV2),
            "LanceV2_1" => Ok(DataFileFormat::LanceV2_1),
            _ => Err(ILError::InvalidInput(format!(
                "Invalid data file format: {s}"
            ))),
        }
    }
}

pub(crate) async fn read_data_file_by_record(
    storage: &Storage,
    table_schema: &Schema,
    data_file_record: &DataFileRecord,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    row_ids: Option<&HashSet<i64>>,
    batch_size: usize,
) -> ILResult<RecordBatchStream> {
    match data_file_record.format {
        DataFileFormat::ParquetV1 | DataFileFormat::ParquetV2 => {
            read_parquet_file_by_record(
                storage,
                table_schema,
                data_file_record,
                projection,
                filters,
                row_ids,
                batch_size,
            )
            .await
        }
        DataFileFormat::LanceV2_1 => {
            read_lance_file_by_record(
                storage,
                table_schema,
                data_file_record,
                projection,
                filters,
                row_ids,
                batch_size,
            )
            .await
        }
    }
}
