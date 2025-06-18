use futures::TryStreamExt;
use indexlake::expr::Expr;
use indexlake::{
    Catalog, LakeClient, Storage,
    record::{DataType, Field, Row, Scalar, Schema, pretty_print_rows},
    table::TableCreation,
};
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite, init_env_logger};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() })]
#[case(async { catalog_postgres().await })]
#[tokio::test(flavor = "multi_thread")]
async fn update_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
) {
    use std::collections::HashMap;

    init_env_logger();

    let storage = Arc::new(Storage::new_fs());
    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::BigInt, false),
        Field::new("name", DataType::Varchar, false),
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
            Scalar::BigInt(Some(1)),
            Scalar::Varchar(Some("Alice".to_string())),
        ],
        vec![
            Scalar::BigInt(Some(2)),
            Scalar::Varchar(Some("Bob".to_string())),
        ],
    ];
    table.insert(&columns, values).await.unwrap();

    let set = HashMap::from([(
        "name".to_string(),
        Scalar::Varchar(Some("Alice2".to_string())),
    )]);
    let condition = Expr::Column("id".to_string()).eq(Expr::Literal(Scalar::BigInt(Some(1))));
    table.update(set, &condition).await.unwrap();

    let row_stream = table.scan().await.unwrap();
    let mut rows = row_stream.try_collect::<Vec<_>>().await.unwrap();
    rows.sort_by_key(|row| row.bigint(0).unwrap());
    let table_str = pretty_print_rows(Some(table_schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+----------+
| id | name     |
+----+----------+
| 1  | 'Alice2' |
| 2  | 'Bob'    |
+----+----------+"#
    );
}
