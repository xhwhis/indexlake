use futures::StreamExt;
use indexlake::{
    Catalog, CatalogDatabase, RowStream, Transaction,
    record::{DataType, Row, Scalar, SchemaRef},
};
use indexlake::{ILError, ILResult};
use log::debug;
use std::{path::PathBuf, sync::Mutex};

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
    fn database(&self) -> CatalogDatabase {
        CatalogDatabase::Sqlite
    }

    async fn transaction(&self) -> ILResult<Box<dyn Transaction>> {
        let conn = rusqlite::Connection::open(&self.path)
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        conn.execute_batch("BEGIN DEFERRED")
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        Ok(Box::new(SqliteTransaction {
            conn: Mutex::new(conn),
            done: false,
        }))
    }
}

#[derive(Debug)]
pub struct SqliteTransaction {
    // TODO use tokio rusqlite connection
    conn: Mutex<rusqlite::Connection>,
    done: bool,
}

#[async_trait::async_trait]
impl Transaction for SqliteTransaction {
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>> {
        debug!("sqlite transaction query: {sql}");
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        let mut sqlite_rows = stmt
            .query([])
            .map_err(|e| ILError::CatalogError(e.to_string()))?;

        let mut rows: Vec<Row> = Vec::new();
        while let Some(sqlite_row) = sqlite_rows
            .next()
            .map_err(|e| ILError::CatalogError(e.to_string()))?
        {
            let mut row_values = Vec::new();
            for (idx, field) in schema.fields.iter().enumerate() {
                let scalar = match field.data_type {
                    DataType::Int32 => {
                        let v: Option<i32> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Int32(v)
                    }
                    DataType::Int64 => {
                        let v: Option<i64> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Int64(v)
                    }
                    DataType::Float32 => {
                        let v: Option<f32> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Float32(v)
                    }
                    DataType::Float64 => {
                        let v: Option<f64> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Float64(v)
                    }
                    DataType::Utf8 => {
                        let v: Option<String> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Utf8(v)
                    }
                    DataType::Binary => {
                        let v: Option<Vec<u8>> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Binary(v)
                    }
                    DataType::Boolean => {
                        let v: Option<bool> = sqlite_row
                            .get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Boolean(v)
                    }
                };
                if !field.nullable && scalar.is_null() {
                    return Err(ILError::CatalogError(format!(
                        "column {} is not nullable but got null value",
                        field.name
                    )));
                }
                row_values.push(scalar);
            }
            rows.push(Row::new(schema.clone(), row_values));
        }
        Ok(rows)
    }

    async fn execute(&mut self, sql: &str) -> ILResult<usize> {
        debug!("sqlite transaction execute: {sql}");
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        let conn = self.conn.lock().unwrap();
        conn.execute(sql, [])
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn execute_batch(&mut self, sqls: &[String]) -> ILResult<()> {
        debug!("sqlite transaction execute batch: {:?}", sqls);
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(sqls.join(";").as_str())
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn commit(&mut self) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("COMMIT")
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
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("ROLLBACK")
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
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
    }
}
