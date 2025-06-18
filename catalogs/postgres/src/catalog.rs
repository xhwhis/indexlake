use bb8::Pool;
use bb8_postgres::{PostgresConnectionManager, tokio_postgres::NoTls};
use indexlake::{
    Catalog, ILError, ILResult, Transaction,
    record::{DataType, Row, Scalar, SchemaRef},
};
use log::debug;

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
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl Catalog for PostgresCatalog {
    async fn transaction(&self) -> ILResult<Box<dyn Transaction>> {
        let conn = self
            .pool
            .get_owned()
            .await
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        conn.batch_execute("START TRANSACTION")
            .await
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
        Ok(Box::new(PostgresTransaction { conn, done: false }))
    }
}

#[derive(Debug)]
pub struct PostgresTransaction {
    conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
    done: bool,
}

#[async_trait::async_trait(?Send)]
impl Transaction for PostgresTransaction {
    async fn query(&mut self, sql: &str, schema: SchemaRef) -> ILResult<Vec<Row>> {
        debug!("postgres transaction query: {sql}");
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }

        let pg_rows = self
            .conn
            .query(sql, &[])
            .await
            .map_err(|e| ILError::CatalogError(e.to_string()))?;

        let mut result = Vec::new();
        for pg_row in pg_rows {
            let mut values = Vec::new();
            for (idx, field) in schema.fields.iter().enumerate() {
                let scalar = match field.data_type {
                    DataType::Integer => {
                        let v: Option<i32> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Integer(v)
                    }
                    DataType::BigInt => {
                        let v: Option<i64> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::BigInt(v)
                    }
                    DataType::Float => {
                        let v: Option<f32> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Float(v)
                    }
                    DataType::Double => {
                        let v: Option<f64> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Double(v)
                    }
                    DataType::Varchar => {
                        let v: Option<String> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Varchar(v)
                    }
                    DataType::Varbinary => {
                        let v: Option<Vec<u8>> = pg_row
                            .try_get(idx)
                            .map_err(|e| ILError::CatalogError(e.to_string()))?;
                        Scalar::Varbinary(v)
                    }
                    DataType::Boolean => {
                        let v: Option<bool> = pg_row
                            .try_get(idx)
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
                values.push(scalar);
            }
            result.push(Row::new(schema.clone(), values));
        }
        Ok(result)
    }

    async fn execute(&mut self, sql: &str) -> ILResult<()> {
        debug!("postgres transaction execute: {sql}");
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        self.conn
            .batch_execute(sql)
            .await
            .map_err(|e| ILError::CatalogError(e.to_string()))
    }

    async fn commit(&mut self) -> ILResult<()> {
        if self.done {
            return Err(ILError::CatalogError(
                "Transaction already committed or rolled back".to_string(),
            ));
        }
        self.conn
            .batch_execute("COMMIT")
            .await
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
            .batch_execute("ROLLBACK")
            .await
            .map_err(|e| ILError::CatalogError(e.to_string()))?;
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
                self.conn
                    .batch_execute("ROLLBACK")
                    .await
                    .expect("rollback failed");
            });
        });
    }
}
