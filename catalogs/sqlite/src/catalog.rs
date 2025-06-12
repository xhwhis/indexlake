use indexlake::{
    Catalog, CatalogDataType, CatalogRow, CatalogScalar, CatalogSchemaRef, Transaction,
};
use indexlake::{ILError, ILResult};
use std::path::PathBuf;

#[derive(Debug)]
pub struct SqliteCatalog {
    path: PathBuf,
}

impl SqliteCatalog {
    pub fn try_new(path: impl Into<String>) -> ILResult<Self> {
        let path = PathBuf::from(path.into());
        if !path.exists() {
            return Err(ILError::CatalogError(format!(
                "sqlite path {} does not exist",
                path.display()
            )));
        }
        Ok(SqliteCatalog { path })
    }
}

#[async_trait::async_trait]
impl Catalog for SqliteCatalog {
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>> {
        let conn = rusqlite::Connection::open(&self.path)
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        conn.execute_batch("BEGIN DEFERRED")
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        Ok(Box::new(SqliteTransaction { conn }))
    }
}

#[derive(Debug)]
pub struct SqliteTransaction {
    conn: rusqlite::Connection,
}

#[async_trait::async_trait(?Send)]
impl Transaction for SqliteTransaction {
    async fn query(&mut self, sql: &str, schema: CatalogSchemaRef) -> ILResult<Vec<CatalogRow>> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| ILError::CatalogError(e.to_string()))?;

        let mut catalog_rows: Vec<CatalogRow> = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| ILError::CatalogError(e.to_string()))?
        {
            let mut row_values = Vec::new();
            for (idx, col) in schema.columns.iter().enumerate() {
                match col.data_type {
                    CatalogDataType::BigInt => {
                        let v: Option<i64> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(CatalogScalar::BigInt(v));
                    }
                    CatalogDataType::Varchar => {
                        let v: Option<String> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(CatalogScalar::Varchar(v));
                    }
                    CatalogDataType::Boolean => {
                        let v: Option<bool> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(CatalogScalar::Boolean(v));
                    }
                }
            }
            catalog_rows.push(CatalogRow::new(schema.clone(), row_values));
        }
        Ok(catalog_rows)
    }

    async fn execute(&mut self, sql: &str) -> ILResult<()> {
        self.conn
            .execute_batch(sql)
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn commit(&mut self) -> ILResult<()> {
        self.conn
            .execute_batch("COMMIT")
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn rollback(&mut self) -> ILResult<()> {
        self.conn
            .execute_batch("ROLLBACK")
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }
}
