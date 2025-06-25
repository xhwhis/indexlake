use indexlake::storage::Storage;
use indexlake_integration_tests::{storage_fs, storage_s3};
use std::sync::Arc;

#[rstest::rstest]
#[case(storage_fs())]
#[case(storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn file_operations(#[case] storage: Arc<Storage>) {
    let file_path = "test/test.txt";
    assert!(!storage.exists(file_path).await.unwrap());

    let file = storage.new_storage_file(file_path).await.unwrap();

    let expected = bytes::Bytes::from("Hello, world!");
    file.write(expected.clone()).await.unwrap();
    let bytes = file.read().await.unwrap();
    assert_eq!(bytes, expected);

    file.delete().await.unwrap();
    assert!(!storage.exists(file_path).await.unwrap());
}
