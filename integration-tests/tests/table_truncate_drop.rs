use arrow::array::RecordBatch;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use indexlake::table::TableScan;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::data::prepare_simple_testing_table;
use indexlake_integration_tests::utils::full_table_scan;
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
    let table = prepare_simple_testing_table(&client).await?;

    table.truncate().await?;

    let scan = TableScan::default();
    let stream = table.scan(scan).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    assert_eq!(batches.len(), 0);

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn drop_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: TableConfig::default(),
    };
    client.create_table(table_creation.clone()).await?;

    let table = client.load_table(namespace_name, &table_name).await?;

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;
    table.insert(&[record_batch.clone()]).await?;
    // avoid dump task failure due to inline row table dropped
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    table.drop().await?;
    assert!(
        client
            .load_table(namespace_name, &table_name)
            .await
            .is_err()
    );

    client.create_table(table_creation).await?;
    let table = client.load_table(namespace_name, &table_name).await?;
    table.insert(&[record_batch]).await?;
    let table_str = full_table_scan(&table).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+----+-------+
| _indexlake_row_id | id | name  |
+-------------------+----+-------+
| 1                 | 1  | Alice |
| 2                 | 2  | Bob   |
+-------------------+----+-------+"#,
    );

    Ok(())
}
