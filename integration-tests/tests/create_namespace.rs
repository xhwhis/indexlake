use indexlake::{LakeClient, catalog::Catalog, storage::Storage};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite, storage_fs, storage_s3};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn create_namespace(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    let client = LakeClient::new(catalog, storage);

    let expected_namespace_id = client.create_namespace("test_namespace").await.unwrap();

    let namespace_id = client.get_namespace_id("test_namespace").await.unwrap();
    assert_eq!(namespace_id, Some(expected_namespace_id));

    let namespace_id = client.get_namespace_id("not_exists").await.unwrap();
    assert_eq!(namespace_id, None);
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn duplicated_namespace_name(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();
    let result = client.create_namespace(namespace_name).await;
    assert!(result.is_err());
}
