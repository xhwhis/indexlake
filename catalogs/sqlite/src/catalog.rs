use indexlake::{
    Catalog, Transaction,
    record::{DataType, Row, Scalar, SchemaRef},
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
        Ok(Box::new(SqliteTransaction { conn, done: false }))
    }
}

#[derive(Debug)]
pub struct SqliteTransaction {
    conn: rusqlite::Connection,
    done: bool,
}

#[async_trait::async_trait(?Send)]
impl Transaction for SqliteTransaction {
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| ILError::CatalogError(e.to_string()))?;

        let mut catalog_rows: Vec<Row> = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| ILError::CatalogError(e.to_string()))?
        {
            let mut row_values = Vec::new();
            for (idx, field) in schema.fields.iter().enumerate() {
                match field.data_type {
                    DataType::Integer => {
                        let v: Option<i32> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Integer(v));
                    }
                    DataType::BigInt => {
                        let v: Option<i64> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::BigInt(v));
                    }
                    DataType::Float => {
                        let v: Option<f32> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Float(v));
                    }
                    DataType::Double => {
                        let v: Option<f64> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Double(v));
                    }
                    DataType::Varchar => {
                        let v: Option<String> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Varchar(v));
                    }
                    DataType::Varbinary => {
                        let v: Option<Vec<u8>> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Varbinary(v));
                    }
                    DataType::Boolean => {
                        let v: Option<bool> = row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        row_values.push(Scalar::Boolean(v));
                    }
                }
            }
            catalog_rows.push(Row::new(schema.clone(), row_values));
        }
        Ok(catalog_rows)
    }

    async fn execute(&mut self, sql: &str) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        self.conn
            .execute_batch(sql)
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn commit(&mut self) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        self.conn
            .execute_batch("COMMIT")
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        self.done = true;
        Ok(())
    }

    async fn rollback(&mut self) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        self.conn
            .execute_batch("ROLLBACK")
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        self.done = true;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.conn.execute_batch("ROLLBACK").unwrap();
    }
}
