use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum CatalogDatabase {
    Sqlite,
    Postgres,
}

impl CatalogDatabase {
    pub fn sql_identifier(&self, ident: &str) -> String {
        match self {
            CatalogDatabase::Sqlite => format!("`{ident}`"),
            CatalogDatabase::Postgres => format!("\"{ident}\""),
        }
    }

    pub fn sql_binary_literal(&self, value: &[u8]) -> String {
        match self {
            CatalogDatabase::Sqlite => format!("X'{}'", hex::encode(value)),
            CatalogDatabase::Postgres => format!("E'\\\\x{}'", hex::encode(value)),
        }
    }

    pub fn sql_uuid_literal(&self, value: &Uuid) -> String {
        match self {
            CatalogDatabase::Sqlite => self.sql_binary_literal(value.as_bytes()),
            CatalogDatabase::Postgres => format!("'{value}'"),
        }
    }

    pub fn sql_string_literal(&self, value: &str) -> String {
        let value = value.replace("'", "''");
        match self {
            CatalogDatabase::Sqlite => format!("'{value}'"),
            CatalogDatabase::Postgres => format!("'{value}'"),
        }
    }
}

impl std::fmt::Display for CatalogDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogDatabase::Sqlite => write!(f, "SQLite"),
            CatalogDatabase::Postgres => write!(f, "Postgres"),
        }
    }
}
