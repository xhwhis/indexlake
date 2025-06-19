use futures::TryStreamExt;
use indexlake::{
    Catalog,
    record::{DataType, Field, Schema, pretty_print_rows},
};
use indexlake_integration_tests::init_env_logger;
use indexlake_integration_tests::{catalog_postgres, catalog_sqlite};
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() })]
#[case(async { catalog_postgres().await })]
#[tokio::test(flavor = "multi_thread")]
async fn catalog_crud(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
) {
    init_env_logger();

    let mut transaction = catalog.transaction().await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::BigInt, false),
        Field::new("name", DataType::Varchar, false),
    ]));

    transaction
        .execute("CREATE TABLE test (id BIGINT PRIMARY KEY, name VARCHAR)")
        .await
        .unwrap();

    transaction
        .execute("INSERT INTO test (id, name) VALUES (1, 'a')")
        .await
        .unwrap();

    let row_stream = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
    let rows = row_stream.try_collect::<Vec<_>>().await.unwrap();
    let table_str = pretty_print_rows(Some(schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+------+
| id | name |
+----+------+
| 1  | 'a'  |
+----+------+"#
    );

    transaction
        .execute("UPDATE test SET name = 'b' WHERE id = 1")
        .await
        .unwrap();
    let row_stream = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
    let rows = row_stream.try_collect::<Vec<_>>().await.unwrap();
    let table_str = pretty_print_rows(Some(schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+------+
| id | name |
+----+------+
| 1  | 'b'  |
+----+------+"#
    );

    transaction
        .execute("DELETE FROM test WHERE id = 1")
        .await
        .unwrap();
    let row_stream = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
    let rows = row_stream.try_collect::<Vec<_>>().await.unwrap();
    let table_str = pretty_print_rows(Some(schema.clone()), &rows).to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+----+------+
| id | name |
+----+------+
+----+------+"#
    );

    transaction.commit().await.unwrap();
}
