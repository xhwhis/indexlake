use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::expr::{col, lit};
use indexlake::{Client, catalog::Catalog, storage::Storage};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use indexlake_integration_tests::{data::prepare_simple_testing_table, utils::full_table_scan};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn delete_table_by_condition(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let condition = col("age").gt(lit(21i32));
    table.delete(&condition).await?;

    let table_str = full_table_scan(&table).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+-------+-----+
| _indexlake_row_id | name  | age |
+-------------------+-------+-----+
| 1                 | Alice | 20  |
| 2                 | Bob   | 21  |
+-------------------+-------+-----+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn delete_table_by_row_id(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let condition = col(INTERNAL_ROW_ID_FIELD_NAME).eq(lit(1i64));
    table.delete(&condition).await?;

    let table_str = full_table_scan(&table).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 2                 | Bob     | 21  |
| 3                 | Charlie | 22  |
| 4                 | David   | 23  |
+-------------------+---------+-----+"#,
    );

    Ok(())
}
