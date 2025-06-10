pub type ILResult<T> = Result<T, ILError>;

#[derive(Debug)]
pub enum ILError {
    Internal(String),
    NotSupported(String),
    CatalogError(String),
}
