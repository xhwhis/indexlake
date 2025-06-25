mod docker;

use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use indexlake::{catalog::Catalog, storage::Storage};
use indexlake_catalog_postgres::PostgresCatalog;
use indexlake_catalog_sqlite::SqliteCatalog;
use opendal::services::S3Config;

use crate::docker::DockerCompose;

static ENV_LOGGER: OnceLock<()> = OnceLock::new();

pub fn init_env_logger() {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    ENV_LOGGER.get_or_init(|| {
        env_logger::init();
        ()
    });
}

pub fn setup_sqlite_db() -> PathBuf {
    let tmpdir = std::env::temp_dir();
    let db_path = tmpdir.join(uuid::Uuid::new_v4().to_string());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(include_str!("../../init_catalog.sql"))
        .unwrap();
    db_path
}

pub async fn setup_postgres_db() -> DockerCompose {
    let docker_compose = DockerCompose::new(
        "postgres",
        format!("{}/testdata/postgres", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.down();
    docker_compose.up();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    docker_compose
}

pub fn setup_minio() -> DockerCompose {
    let docker_compose = DockerCompose::new(
        "minio",
        format!("{}/testdata/minio", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.down();
    docker_compose.up();
    docker_compose
}

pub fn catalog_sqlite() -> Arc<dyn Catalog> {
    let db_path = setup_sqlite_db();
    Arc::new(SqliteCatalog::try_new(db_path.to_string_lossy().to_string()).unwrap())
}

pub async fn catalog_postgres() -> Arc<dyn Catalog> {
    let _ = setup_postgres_db().await;
    Arc::new(
        PostgresCatalog::try_new("localhost", 5432, "postgres", "password", Some("postgres"))
            .await
            .unwrap(),
    )
}

pub fn storage_fs() -> Arc<Storage> {
    let home = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "test_storage");
    Arc::new(Storage::new_fs(home))
}

pub fn storage_s3() -> Arc<Storage> {
    let _ = setup_minio();
    std::thread::sleep(std::time::Duration::from_secs(5));
    let mut config = S3Config::default();
    config.endpoint = Some("http://127.0.0.1:9000".to_string());
    config.access_key_id = Some("admin".to_string());
    config.secret_access_key = Some("password".to_string());
    config.region = Some("us-east-1".to_string());
    Arc::new(Storage::new_s3(config, "indexlake"))
}
