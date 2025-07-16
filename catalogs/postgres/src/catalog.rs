use bb8::Pool;
use bb8_postgres::{PostgresConnectionManager, tokio_postgres::NoTls};
use futures::StreamExt;
use indexlake::{
    ILError, ILResult,
    catalog::{Catalog, CatalogDatabase, RowStream, Transaction},
    catalog::{CatalogDataType, CatalogSchemaRef, Row, Scalar},
};
use log::{debug, error};

#[derive(Debug, Clone)]
pub struct PostgresCatalog {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresCatalog {
    pub async fn try_new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        dbname: Option<&str>,
    ) -> ILResult<Self> {
        let mut config = bb8_postgres::tokio_postgres::config::Config::new();
        config.host(host).port(port).user(user).password(password);
        if let Some(dbname) = dbname {
            config.dbname(dbname);
        }
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to build postgres pool: {e}")))?;
        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl Catalog for PostgresCatalog {
    fn database(&self) -> CatalogDatabase {
        CatalogDatabase::Postgres
    }

    async fn query(&self, sql: &str, schema: CatalogSchemaRef) -> ILResult<RowStream<'static>> {
        debug!("postgres query: {sql}");
        let conn = self.pool.get_owned().await.map_err(|e| {
            ILError::CatalogError(format!("failed to get postgres connection: {e}"))
        })?;
        let pg_row_stream = conn
            .query_raw(sql, Vec::<String>::new())
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to query postgres: {e}")))?;

        let stream = pg_row_stream.map(move |row| {
            let pg_row =
                row.map_err(|e| ILError::CatalogError(format!("failed to get postgres row: {e}")))?;
            pg_row_to_row(&pg_row, &schema)
        });
        Ok(Box::pin(stream))
    }

    async fn transaction(&self) -> ILResult<Box<dyn Transaction>> {
        let conn = self.pool.get_owned().await.map_err(|e| {
            ILError::CatalogError(format!("failed to get postgres connection: {e}"))
        })?;
        conn.batch_execute("START TRANSACTION")
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to start postgres txn: {e}")))?;
        Ok(Box::new(PostgresTransaction { conn, done: false }))
    }
}

#[derive(Debug)]
pub struct PostgresTransaction {
    conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
    done: bool,
}

impl PostgresTransaction {
    fn check_done(&self) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transaction for PostgresTransaction {
    async fn query(&mut self, sql: &str, schema: CatalogSchemaRef) -> ILResult<RowStream> {
        debug!("postgres txn query: {sql}");
        self.check_done()?;

        let pg_row_stream = self
            .conn
            .query_raw(sql, Vec::<String>::new())
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to query postgres: {e}")))?;

        let stream = pg_row_stream.map(move |row| {
            let pg_row =
                row.map_err(|e| ILError::CatalogError(format!("failed to get postgres row: {e}")))?;
            pg_row_to_row(&pg_row, &schema)
        });
        Ok(Box::pin(stream))
    }

    async fn execute(&mut self, sql: &str) -> ILResult<usize> {
        debug!("postgres txn execute: {sql}");
        self.check_done()?;
        self.conn
            .execute(sql, &[])
            .await
            .map(|r| r as usize)
            .map_err(|e| ILError::CatalogError(format!("failed to execute postgres: {e}")))
    }

    async fn execute_batch(&mut self, sqls: &[String]) -> ILResult<()> {
        debug!("postgres txn execute batch: {:?}", sqls);
        self.check_done()?;
        self.conn
            .batch_execute(sqls.join(";").as_str())
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to execute batch postgres: {e}")))
    }

    async fn commit(&mut self) -> ILResult<()> {
        debug!("postgres txn commit");
        self.check_done()?;
        self.conn
            .batch_execute("COMMIT")
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to commit postgres txn: {e}")))?;
        self.done = true;
        Ok(())
    }

    async fn rollback(&mut self) -> ILResult<()> {
        debug!("postgres txn rollback");
        self.check_done()?;
        self.conn
            .batch_execute("ROLLBACK")
            .await
            .map_err(|e| ILError::CatalogError(format!("failed to rollback postgres txn: {e}")))?;
        self.done = true;
        Ok(())
    }
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Err(e) = self.conn.batch_execute("ROLLBACK").await {
                    error!("[indexlake] failed to rollback postgres txn: {e}");
                }
            });
        });
    }
}

fn pg_row_to_row(
    pg_row: &bb8_postgres::tokio_postgres::Row,
    schema: &CatalogSchemaRef,
) -> ILResult<Row> {
    let mut values = Vec::new();
    for (idx, field) in schema.columns.iter().enumerate() {
        let scalar = match field.data_type {
            CatalogDataType::Boolean => {
                let v: Option<bool> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Boolean(v)
            }
            CatalogDataType::Int8 => {
                let v: Option<i8> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Int8(v)
            }
            CatalogDataType::Int16 => {
                let v: Option<i16> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Int16(v)
            }
            CatalogDataType::Int32 => {
                let v: Option<i32> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Int32(v)
            }
            CatalogDataType::Int64 => {
                let v: Option<i64> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Int64(v)
            }
            CatalogDataType::Float32 => {
                let v: Option<f32> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Float32(v)
            }
            CatalogDataType::Float64 => {
                let v: Option<f64> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Float64(v)
            }
            CatalogDataType::Utf8 => {
                let v: Option<String> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Utf8(v)
            }
            CatalogDataType::Binary => {
                let v: Option<Vec<u8>> = pg_row
                    .try_get(idx)
                    .map_err(|e| ILError::CatalogError(e.to_string()))?;
                Scalar::Binary(v)
            }
        };
        if !field.nullable && scalar.is_null() {
            return Err(ILError::CatalogError(format!(
                "column {} is not nullable but got null value",
                field.name
            )));
        }
        values.push(scalar);
    }
    Ok(Row::new(schema.clone(), values))
}
