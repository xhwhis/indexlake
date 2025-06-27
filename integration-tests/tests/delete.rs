use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use indexlake::expr::Expr;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    record::{CatalogDataType, CatalogScalar, CatalogSchema, Column, Row, pretty_print_rows},
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

// #[rstest::rstest]
// #[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
// #[tokio::test(flavor = "multi_thread")]
// async fn delete_table(
//     #[future(awt)]
//     #[case]
//     catalog: Arc<dyn Catalog>,
//     #[case] storage: Arc<Storage>,
// ) {
//     use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};

//     init_env_logger();

//     let client = LakeClient::new(catalog, storage);

//     let namespace_name = "test_namespace";
//     client.create_namespace(namespace_name).await.unwrap();

//     let table_schema = Arc::new(Schema::new(vec![
//         Field::new("id", DataType::Int64, false),
//         Field::new("name", DataType::Utf8, false),
//     ]));
//     let table_name = "test_table";
//     let table_creation = TableCreation {
//         namespace_name: namespace_name.to_string(),
//         table_name: table_name.to_string(),
//         schema: table_schema.clone(),
//         config: TableConfig::default(),
//     };
//     client.create_table(table_creation).await.unwrap();

//     let table = client.load_table(namespace_name, table_name).await.unwrap();

//     let columns = vec!["id".to_string(), "name".to_string()];
//     let values = vec![
//         vec![
//             CatalogScalar::Int64(Some(1)),
//             CatalogScalar::Utf8(Some("Alice".to_string())),
//         ],
//         vec![
//             CatalogScalar::Int64(Some(2)),
//             CatalogScalar::Utf8(Some("Bob".to_string())),
//         ],
//     ];
//     let record_batch = RecordBatch::try_new(
//         table_schema.clone(),
//         vec![
//             Arc::new(Int64Array::from(vec![1, 2])),
//             Arc::new(StringArray::from(vec!["Alice", "Bob"])),
//         ],
//     )
//     .unwrap();
//     table.insert(&record_batch).await.unwrap();

//     let condition = Expr::Column("id".to_string()).eq(Expr::Literal(CatalogScalar::Int64(Some(1))));
//     table.delete(&condition).await.unwrap();

//     let stream = table.scan_arrow().await.unwrap();
//     let batches = stream.try_collect::<Vec<_>>().await.unwrap();
//     let table_str = pretty_format_batches(&batches).unwrap().to_string();
//     println!("{}", table_str);
//     assert_eq!(
//         table_str,
//         r#"+----+------+
// | id | name |
// +----+------+
// | 2  | Bob  |
// +----+------+"#
//     );
// }
