use indexlake::{
    LakeClient, Storage,
    schema::{DataType, Field, Schema, SchemaRef},
    table::TableCreation,
};
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::setup_sqlite_db;
use std::sync::Arc;

#[tokio::test]
async fn create_table() {
    let db_path = setup_sqlite_db().display().to_string();
    let catalog = Arc::new(SqliteCatalog::try_new(db_path).unwrap());

    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    let expected_namespace_id = client.create_namespace(namespace_name).await.unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false, None),
        Field::new("name", DataType::Utf8, false, None),
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
