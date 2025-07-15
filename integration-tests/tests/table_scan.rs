use indexlake::expr::{col, lit};
use indexlake::table::TableScan;
use indexlake::{LakeClient, catalog::Catalog, storage::Storage};
use indexlake_integration_tests::data::prepare_simple_testing_table;
use indexlake_integration_tests::utils::table_scan;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn scan_with_projection(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let scan = TableScan::default().with_projection(Some(vec![0, 2]));
    let table_str = table_scan(&table, scan).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+-----+
| _indexlake_row_id | age |
+-------------------+-----+
| 1                 | 20  |
| 2                 | 21  |
| 3                 | 22  |
| 4                 | 23  |
+-------------------+-----+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn scan_with_filters(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let scan = TableScan::default()
        .with_filters(vec![col("age").gt(lit(21)), col("name").eq(lit("Charlie"))]);
    let table_str = table_scan(&table, scan).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 3                 | Charlie | 22  |
+-------------------+---------+-----+"#
    );

    Ok(())
}
