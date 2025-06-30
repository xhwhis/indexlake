#[derive(Debug, Clone, Copy)]
pub enum CatalogDatabase {
    Sqlite,
    Postgres,
}

impl CatalogDatabase {
    pub fn sql_identifier(&self, ident: &str) -> String {
        match self {
            CatalogDatabase::Sqlite => format!("`{}`", ident),
            CatalogDatabase::Postgres => format!("\"{}\"", ident),
        }
    }

    pub fn sql_binary_value(&self, value: &[u8]) -> String {
        match self {
            CatalogDatabase::Sqlite => format!("X'{}'", hex::encode(value)),
            CatalogDatabase::Postgres => format!("E'\\\\x{}'", hex::encode(value)),
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
