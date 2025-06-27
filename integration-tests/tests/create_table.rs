use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use indexlake::arrow::schema_without_column;
use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::{
    LakeClient,
    catalog::Catalog,
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

use arrow::array::{
    BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn create_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    let expected_namespace_id = client.create_namespace(namespace_name).await.unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: expected_schema.clone(),
        config: TableConfig::default(),
    };

    let expected_table_id = client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();
    println!("table: {:?}", table);
    assert_eq!(table.namespace_id, expected_namespace_id);
    assert_eq!(table.namespace_name, namespace_name);
    assert_eq!(table.table_id, expected_table_id);
    assert_eq!(table.table_name, table_name);
    let schema = schema_without_column(&table.schema, INTERNAL_ROW_ID_FIELD_NAME).unwrap();
    assert_eq!(
        schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>(),
        expected_schema
            .fields
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|f| f.data_type())
            .collect::<Vec<_>>(),
        expected_schema
            .fields
            .iter()
            .map(|f| f.data_type())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|f| f.is_nullable())
            .collect::<Vec<_>>(),
        expected_schema
            .fields
            .iter()
            .map(|f| f.is_nullable())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|f| f.metadata().clone())
            .collect::<Vec<_>>(),
        expected_schema
            .fields
            .iter()
            .map(|f| f.metadata().clone())
            .collect::<Vec<_>>()
    );
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn table_data_types(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("int16_col", DataType::Int16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("int64_col", DataType::Int64, true),
        // Field::new("float32_col", DataType::Float32, true),
        // Field::new("float64_col", DataType::Float64, true),
        Field::new("utf8_col", DataType::Utf8, true),
        // Field::new("binary_col", DataType::Binary, true),
        // Field::new("boolean_col", DataType::Boolean, true),
    ]));

    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
        config: TableConfig::default(),
    };

    client.create_table(table_creation).await.unwrap();

    let table = client.load_table(namespace_name, table_name).await.unwrap();

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(Int16Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
            // Arc::new(Float32Array::from(vec![1.1])),
            // Arc::new(Float64Array::from(vec![1.1])),
            Arc::new(StringArray::from(vec!["utf8"])),
            // Arc::new(BinaryArray::from_vec(vec![b"0001"])),
            // Arc::new(BooleanArray::from(vec![true])),
        ],
    )
    .unwrap();
    table.insert(&record_batch).await.unwrap();

    let stream = table.scan_arrow().await.unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    let table_str = pretty_format_batches(&batches).unwrap().to_string();
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-----------+-----------+-----------+----------+
| int16_col | int32_col | int64_col | utf8_col |
+-----------+-----------+-----------+----------+
| 1         | 1         | 2         | utf8     |
+-----------+-----------+-----------+----------+"#,
    );
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
// #[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn duplicated_table_name(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name).await.unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_name = "test_table";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: expected_schema.clone(),
        config: TableConfig::default(),
    };

    client.create_table(table_creation.clone()).await.unwrap();
    let result = client.create_table(table_creation).await;
    assert!(result.is_err());
}
