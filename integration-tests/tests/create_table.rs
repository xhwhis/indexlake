use indexlake::{
    Catalog, LakeClient, Storage,
    record::{DataType, Field, Schema},
    table::TableCreation,
};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() })]
#[case(async { catalog_postgres().await })]
#[tokio::test(flavor = "multi_thread")]
async fn create_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
) {
    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    let expected_namespace_id = client.create_namespace(namespace_name).await.unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::BigInt, false, None),
        Field::new("name", DataType::Varchar, false, None),
    ]));

    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: expected_schema.clone(),
    };

    let expected_table_id = client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();
    println!("table: {:?}", table);
    assert_eq!(table.namespace_id, expected_namespace_id);
    assert_eq!(table.namespace_name, namespace_name);
    assert_eq!(table.table_id, expected_table_id);
    assert_eq!(table.table_name, table_name);
    assert_eq!(table.schema, expected_schema);
}
