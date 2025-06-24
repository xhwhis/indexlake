use futures::TryStreamExt;
use indexlake::{
    Catalog, LakeClient, Storage,
    record::{DataType, Field, Row, Scalar, Schema, pretty_print_rows},
    table::TableCreation,
};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() })]
#[case(async { catalog_postgres().await })]
#[tokio::test(flavor = "multi_thread")]
async fn insert_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
) {
    let storage = Arc::new(Storage::new_fs());

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
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
    ];

    table.insert(&columns, values).await.unwrap();

    let row_stream = table.scan().await.unwrap();
    let rows = row_stream.try_collect::<Vec<_>>().await.unwrap();
    let table_str = pretty_print_rows(Some(table_schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+"#
    );
}
