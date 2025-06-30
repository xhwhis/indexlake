use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::expr::Expr;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    catalog::{Scalar},
    storage::Storage,
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;
use indexlake_integration_tests::{data::prepare_testing_table, utils::table_scan};

// TODO fix tests
#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn delete_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_testing_table(&client, "delete_table").await.unwrap();

    let condition = Expr::Column("age".to_string()).gt(Expr::Literal(Scalar::Int32(Some(21))));
    table.delete(&condition).await.unwrap();

    let table_str = table_scan(&table).await.unwrap();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 1                 | Alice   | 20  |
| 2                 | Bob     | 21  |
| 3                 | Charlie | 22  |
+-------------------+---------+-----+"#,
    );
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn delete_table_by_row_id(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_testing_table(&client, "delete_table_by_row_id").await.unwrap();

    let condition = Expr::Column(INTERNAL_ROW_ID_FIELD_NAME.to_string())
        .eq(Expr::Literal(Scalar::Int64(Some(1))));
    table.delete(&condition).await.unwrap();

    let table_str = table_scan(&table).await.unwrap();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 1                 | Alice   | 20  |
| 2                 | Bob     | 21  |
| 3                 | Charlie | 22  |
| 4                 | David   | 23  |
+-------------------+---------+-----+"#,
    );
}
