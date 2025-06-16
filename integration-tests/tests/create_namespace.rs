use indexlake::{Catalog, LakeClient, Storage};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() })]
#[case(async { catalog_postgres().await })]
#[tokio::test(flavor = "multi_thread")]
async fn create_namespace(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
) {
    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let expected_namespace_id = client.create_namespace("test_namespace").await.unwrap();

    let namespace_id = client.get_namespace_id("test_namespace").await.unwrap();
    assert_eq!(namespace_id, Some(expected_namespace_id));

    let namespace_id = client.get_namespace_id("not_exists").await.unwrap();
    assert_eq!(namespace_id, None);
}
