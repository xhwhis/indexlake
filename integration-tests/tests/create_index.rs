use arrow::datatypes::{DataType, Field, Schema};
use indexlake::{
    LakeClient,
    catalog::Catalog,
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

use std::collections::HashMap;

use indexlake::table::IndexCreation;
use indexlake_index_rstar::RStarIndex;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn create_index(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let mut client = LakeClient::new(catalog, storage);
    client.register_filter_index(Arc::new(RStarIndex)).unwrap();

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: expected_schema.clone(),
        config: TableConfig::default(),
    };

    client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();

    let index_name = "test_index";
    let index_creation = IndexCreation {
        name: index_name.to_string(),
        kind: "rstar".to_string(),
        key_column_names: vec!["id".to_string()],
        include_column_names: vec!["name".to_string()],
        config: HashMap::new(),
    };

    table.create_index(index_creation).await.unwrap();
}
