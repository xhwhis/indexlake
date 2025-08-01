pub type ILResult<T> = Result<T, ILError>;

#[derive(Debug)]
pub enum ILError {
    InternalError(String),
    NotSupported(String),
    CatalogError(String),
    StorageError(String),
    IndexError(String),
    InvalidInput(String),
}

impl std::fmt::Display for ILError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ILError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            ILError::NotSupported(msg) => write!(f, "Not supported: {}", msg),
            ILError::CatalogError(msg) => write!(f, "Catalog error: {}", msg),
            ILError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ILError::IndexError(msg) => write!(f, "Index error: {}", msg),
            ILError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

impl std::error::Error for ILError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl From<opendal::Error> for ILError {
    fn from(err: opendal::Error) -> Self {
        ILError::StorageError(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for ILError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        ILError::StorageError(err.to_string())
    }
}

impl From<lance_core::Error> for ILError {
    fn from(err: lance_core::Error) -> Self {
        ILError::StorageError(err.to_string())
    }
}

impl From<arrow::error::ArrowError> for ILError {
    fn from(err: arrow::error::ArrowError) -> Self {
        ILError::InternalError(err.to_string())
    }
}

impl From<tokio::task::JoinError> for ILError {
    fn from(err: tokio::task::JoinError) -> Self {
        ILError::InternalError(format!("Tokio join error: {err}"))
    }
}
