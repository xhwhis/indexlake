use indexlake::{
    Catalog,
    record::{DataType, Field, Schema, pretty_print_rows},
};
use indexlake_catalog_postgres::PostgresCatalog;
use indexlake_integration_tests::setup_postgres_db;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn catalog_postgres() {
    let docker_compose = setup_postgres_db().await;
    let catalog =
        PostgresCatalog::try_new("localhost", 5432, "postgres", "password", Some("postgres"))
            .await
            .unwrap();

    let mut transaction = catalog.transaction().await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::BigInt, false, None),
        Field::new("name", DataType::Varchar, false, None),
    ]));

    transaction
        .execute("CREATE TABLE test (id BIGINT PRIMARY KEY, name VARCHAR)")
        .await
        .unwrap();
    transaction
        .execute("INSERT INTO test (id, name) VALUES (1, 'a')")
        .await
        .unwrap();

    let rows = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
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
    let rows = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
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
    let rows = transaction
        .query("SELECT * FROM test", schema.clone())
        .await
        .unwrap();
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

    docker_compose.down();
}
