use futures::TryStreamExt;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    record::{DataType, Field, Row, Scalar, Schema, pretty_print_rows},
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
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
        parquet_row_count_limit: 2,
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

    let columns = vec!["id".to_string(), "name".to_string()];
    let values = vec![
        vec![
            Scalar::Int64(Some(1)),
            Scalar::Utf8(Some("Alice".to_string())),
        ],
        vec![
            Scalar::Int64(Some(2)),
            Scalar::Utf8(Some("Bob".to_string())),
        ],
        vec![
            Scalar::Int64(Some(3)),
            Scalar::Utf8(Some("Charlie".to_string())),
        ],
    ];

    table.insert(&columns, values).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
}
