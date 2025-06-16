use indexlake::{
    LakeClient, Storage,
    record::{DataType, Field, Row, Scalar, Schema, pretty_print_rows},
    table::TableCreation,
};
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::setup_sqlite_db;
use std::sync::Arc;

#[tokio::test]
async fn insert_table() {
    let db_path = setup_sqlite_db().display().to_string();
    let catalog = Arc::new(SqliteCatalog::try_new(db_path).unwrap());
    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::BigInt, false, None),
        Field::new("name", DataType::Varchar, false, None),
    ]));
    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
    };
    client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();

    let rows = vec![
        Row::new(
            table_schema.clone(),
            vec![
                Scalar::BigInt(Some(1)),
                Scalar::Varchar(Some("Alice".to_string())),
            ],
        ),
        Row::new(
            table_schema.clone(),
            vec![
                Scalar::BigInt(Some(2)),
                Scalar::Varchar(Some("Bob".to_string())),
            ],
        ),
    ];

    table.insert_rows(rows).await.unwrap();

    let rows = table.scan().await.unwrap();
    let table_str = pretty_print_rows(Some(table_schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+---------+
| id | name    |
+----+---------+
| 1  | 'Alice' |
| 2  | 'Bob'   |
+----+---------+"#
    );
}
