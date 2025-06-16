use indexlake::{
    Catalog,
    record::{DataType, Field, Row, Scalar, Schema, SchemaRef, pretty_print_rows},
};
use indexlake_catalog_sqlite::SqliteCatalog;
use indexlake_integration_tests::setup_sqlite_db;
use std::sync::Arc;

#[tokio::test]
async fn catalog_sqlite() {
    let db_path = setup_sqlite_db().display().to_string();
    let catalog = SqliteCatalog::try_new(db_path).unwrap();
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
}
