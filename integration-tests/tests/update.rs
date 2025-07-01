use indexlake::expr::Expr;
use indexlake::{LakeClient, catalog::Catalog, catalog::Scalar, storage::Storage};
use indexlake_integration_tests::data::prepare_testing_table;
use indexlake_integration_tests::utils::table_scan;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::collections::HashMap;
use std::sync::Arc;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn update_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_testing_table(&client, "update_table")
        .await
        .unwrap();

    let set_map = HashMap::from([("age".to_string(), Scalar::Int32(Some(30)))]);
    let condition =
        Expr::Column("name".to_string()).eq(Expr::Literal(Scalar::Utf8(Some("Alice".to_string()))));
    table.update(set_map, &condition).await.unwrap();

    let table_str = table_scan(&table).await.unwrap();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+---------+-----+
| _indexlake_row_id | name    | age |
+-------------------+---------+-----+
| 1                 | Alice   | 30  |
| 2                 | Bob     | 21  |
| 3                 | Charlie | 22  |
| 4                 | David   | 23  |
+-------------------+---------+-----+"#,
    );
}
