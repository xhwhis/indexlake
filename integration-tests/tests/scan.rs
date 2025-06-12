use indexlake::Storage;
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::setup_sqlite_db;

#[tokio::test]
async fn scan() {
    let db_path = setup_sqlite_db().display().to_string();
    let catalog = SqliteCatalog::try_new(db_path).unwrap();

    let storage = Storage::new_fs();

    let exists = storage.exists("file://test.txt").await.unwrap();
    assert!(!exists);
}
