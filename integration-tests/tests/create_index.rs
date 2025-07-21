use arrow::datatypes::{DataType, Field, Schema};
use indexlake::{LakeClient, catalog::Catalog, index::IndexKind, storage::Storage};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, data::prepare_simple_testing_table, init_env_logger,
    storage_fs, storage_s3,
};
use std::sync::Arc;

use indexlake::table::{IndexCreation, TableConfig, TableCreation};
use indexlake_index_btree::{BTreeIndexKind, BTreeIndexParams};

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn duplicated_index_name(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let mut client = LakeClient::new(catalog, storage);
    client.register_index_kind(Arc::new(BTreeIndexKind))?;

    let namespace_name = uuid::Uuid::new_v4().to_string();
    client.create_namespace(&namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_creation = TableCreation {
        namespace_name: namespace_name.clone(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: TableConfig::default(),
    };
    client.create_table(table_creation).await?;

    let mut table = client.load_table(&namespace_name, &table_name).await?;

    let index_creation = IndexCreation {
        name: "test_index".to_string(),
        kind: BTreeIndexKind.kind().to_string(),
        key_columns: vec!["name".to_string()],
        params: Arc::new(BTreeIndexParams),
    };

    table.create_index(index_creation.clone()).await?;

    let result = table.create_index(index_creation).await;
    assert!(result.is_err());

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_index_kind(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let mut client = LakeClient::new(catalog, storage);
    client.register_index_kind(Arc::new(BTreeIndexKind))?;

    let mut table = prepare_simple_testing_table(&client).await?;

    let index_creation = IndexCreation {
        name: "test_index".to_string(),
        kind: "unsupported_index_kind".to_string(),
        key_columns: vec!["name".to_string()],
        params: Arc::new(BTreeIndexParams),
    };

    let result = table.create_index(index_creation).await;
    assert!(result.is_err());

    Ok(())
}
