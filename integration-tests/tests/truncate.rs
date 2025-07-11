use futures::TryStreamExt;
use indexlake::table::TableScan;
use indexlake::{LakeClient, catalog::Catalog, storage::Storage};
use indexlake_integration_tests::data::prepare_testing_table;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn truncate_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_testing_table(&client, "truncate_table").await?;

    table.truncate().await?;

    let scan = TableScan::default();
    let stream = table.scan(scan).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    assert_eq!(batches.len(), 0);

    Ok(())
}
