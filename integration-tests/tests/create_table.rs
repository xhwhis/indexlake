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
    client.create_namespace(namespace_name).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: "test_table".to_string(),
        schema,
    };

    let table_id = client.create_table(table_creation).await.unwrap();
    assert_eq!(table_id, 1);
}
