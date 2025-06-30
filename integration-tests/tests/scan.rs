use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::utils::sort_record_batches;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn scan_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let table_config = TableConfig {
        inline_row_count_limit: 2,
        parquet_row_group_size: 1,
    };
    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();
    table.insert(&record_batch).await.unwrap();
    // wait for dump task to finish
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let batch_stream = table.scan_arrow().await.unwrap();
    let batches = batch_stream.try_collect::<Vec<_>>().await.unwrap();
    let sorted_batch = sort_record_batches(&batches, INTERNAL_ROW_ID_FIELD_NAME).unwrap();
    let table_str = pretty_format_batches(&[sorted_batch]).unwrap().to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+----+---------+
| _indexlake_row_id | id | name    |
+-------------------+----+---------+
| 1                 | 1  | Alice   |
| 2                 | 2  | Bob     |
| 3                 | 3  | Charlie |
+-------------------+----+---------+"#,
    );
}
