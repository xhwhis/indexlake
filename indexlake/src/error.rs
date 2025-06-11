pub type ILResult<T> = Result<T, ILError>;

#[derive(Debug)]
pub enum ILError {
    Internal(String),
    NotSupported(String),
    CatalogError(String),
    StorageError(String),
}

impl From<opendal::Error> for ILError {
    fn from(err: opendal::Error) -> Self {
        ILError::StorageError(err.to_string())
    }
}
