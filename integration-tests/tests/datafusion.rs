use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::storage::DataFileFormat;
use indexlake::{Client, catalog::Catalog, storage::Storage};
use indexlake_datafusion::IndexLakeTable;
use indexlake_integration_tests::data::prepare_simple_testing_table;
use indexlake_integration_tests::utils::sort_record_batches;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn datafusion_full_scan(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client, DataFileFormat::ParquetV2).await?;

    let df_table = IndexLakeTable::new(Arc::new(table));
    let session = SessionContext::new();
    session.register_table("indexlake_table", Arc::new(df_table))?;
    let df = session.sql("SELECT * FROM indexlake_table").await?;
    let batches = df.collect().await?;
    let sorted_batch = sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME)?;
    let table_str = pretty_format_batches(&vec![sorted_batch])?.to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 1                 | Alice   | 20  |
| 2                 | Bob     | 21  |
| 3                 | Charlie | 22  |
| 4                 | David   | 23  |
+-------------------+---------+-----+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn datafusion_scan_with_projection(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client, DataFileFormat::ParquetV2).await?;

    let df_table = IndexLakeTable::new(Arc::new(table));
    let session = SessionContext::new();
    session.register_table("indexlake_table", Arc::new(df_table))?;
    let df = session
        .sql("SELECT _indexlake_row_id, name FROM indexlake_table")
        .await?;
    let batches = df.collect().await?;
    let sorted_batch = sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME)?;
    let table_str = pretty_format_batches(&vec![sorted_batch])?.to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+
| _indexlake_row_id | name    |
+-------------------+---------+
| 1                 | Alice   |
| 2                 | Bob     |
| 3                 | Charlie |
| 4                 | David   |
+-------------------+---------+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn datafusion_scan_with_filters(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client, DataFileFormat::ParquetV2).await?;

    let df_table = IndexLakeTable::new(Arc::new(table));
    let session = SessionContext::new();
    session.register_table("indexlake_table", Arc::new(df_table))?;
    let df = session
        .sql("SELECT * FROM indexlake_table where age > 21")
        .await?;
    let batches = df.collect().await?;
    let sorted_batch = sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME)?;
    let table_str = pretty_format_batches(&vec![sorted_batch])?.to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 3                 | Charlie | 22  |
| 4                 | David   | 23  |
+-------------------+---------+-----+"#,
    );

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn datafusion_scan_with_limit(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);
    let table = prepare_simple_testing_table(&client, DataFileFormat::ParquetV2).await?;

    let df_table = IndexLakeTable::new(Arc::new(table));
    let session = SessionContext::new();
    session.register_table("indexlake_table", Arc::new(df_table))?;
    let df = session.sql("SELECT * FROM indexlake_table limit 2").await?;
    let batches = df.collect().await?;
    let num_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(num_rows, 2);

    Ok(())
}
