use indexlake::storage::Storage;
use indexlake_integration_tests::{storage_fs, storage_s3};
use std::sync::Arc;

#[rstest::rstest]
#[case(storage_fs())]
#[case(storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn file_operations(#[case] storage: Arc<Storage>) {
    let file = storage.new_storage_file("test.txt").await.unwrap();
    assert!(!file.exists().await.unwrap());

    let expected = bytes::Bytes::from("Hello, world!");
    file.write(expected.clone()).await.unwrap();
    let bytes = file.read().await.unwrap();
    assert_eq!(bytes, expected);

    file.delete().await.unwrap();
    assert!(!file.exists().await.unwrap());
}
