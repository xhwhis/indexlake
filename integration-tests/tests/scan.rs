use indexlake::Storage;
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::{setup_sqlite_db, storage_fs};

#[tokio::test]
async fn scan() {
    let storage = storage_fs();

    let file = storage.new_storage_file("test.txt").await.unwrap();
    assert!(!file.exists().await.unwrap());

    let expected = bytes::Bytes::from("Hello, world!");
    file.write(expected.clone()).await.unwrap();
    let bytes = file.read().await.unwrap();
    assert_eq!(bytes, expected);

    file.delete().await.unwrap();
}
