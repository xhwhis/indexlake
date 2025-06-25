use futures::TryStreamExt;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    record::{DataType, Field, Row, Scalar, Schema, pretty_print_rows},
    storage::Storage,
    table::TableCreation,
};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite, storage_fs, storage_s3};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[tokio::test(flavor = "multi_thread")]
async fn scan_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    let file = storage.new_storage_file("test.txt").await.unwrap();
    assert!(!file.exists().await.unwrap());

    let expected = bytes::Bytes::from("Hello, world!");
    file.write(expected.clone()).await.unwrap();
    let bytes = file.read().await.unwrap();
    assert_eq!(bytes, expected);

    file.delete().await.unwrap();
}
