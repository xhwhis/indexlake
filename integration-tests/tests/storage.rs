use indexlake::storage::Storage;
use indexlake_integration_tests::{storage_fs, storage_s3};
use std::sync::Arc;

#[rstest::rstest]
#[case(storage_fs())]
#[case(storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn file_operations(#[case] storage: Arc<Storage>) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "test/test.txt";
    assert!(!storage.exists(file_path).await?);

    let output_file = storage.create_file(file_path).await?;
    let expected = bytes::Bytes::from("Hello, world!");
    output_file.write(expected.clone()).await?;

    let input_file = storage.open_file(file_path).await?;
    let bytes = input_file.read().await?;
    assert_eq!(bytes, expected);

    output_file.delete().await?;
    assert!(!storage.exists(file_path).await?);

    Ok(())
}
