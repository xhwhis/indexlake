use indexlake::{LakeClient, Storage};
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::setup_sqlite_db;
use std::sync::Arc;

#[tokio::test]
async fn create_namespace() {
    let db_path = setup_sqlite_db().display().to_string();
    let catalog = Arc::new(SqliteCatalog::try_new(db_path).unwrap());

    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let expected_namespace_id = client.create_namespace("test_namespace").await.unwrap();

    let namespace_id = client.get_namespace_id("test_namespace").await.unwrap();
    assert_eq!(namespace_id, Some(expected_namespace_id));

    let namespace_id = client.get_namespace_id("not_exists").await.unwrap();
    assert_eq!(namespace_id, None);
}
