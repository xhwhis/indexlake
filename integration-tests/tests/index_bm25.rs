use arrow::{
    array::{BinaryArray, StringArray},
    datatypes::{DataType, Field, Schema},
};
use indexlake::{
    LakeClient,
    catalog::Catalog,
    index::IndexKind,
    storage::Storage,
    table::{TableConfig, TableCreation, TableScan, TableSearch},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, data::prepare_simple_testing_table, init_env_logger,
    storage_fs, storage_s3,
};
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use indexlake::table::IndexCreation;
use indexlake_index_bm25::{BM25IndexKind, BM25IndexParams, BM25SearchQuery, Language};
use indexlake_integration_tests::utils::{table_scan, table_search};

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn create_bm25_index(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let mut client = LakeClient::new(catalog, storage);
    client.register_index_kind(Arc::new(BM25IndexKind))?;

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("title", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]));
    let table_config = TableConfig {
        inline_row_count_limit: 3,
        parquet_row_group_size: 2,
    };
    let table_name = "create_bm25_index";
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;
    let mut table = client.load_table(&namespace_name, table_name).await?;

    let index_creation = IndexCreation {
        name: "bm25_index".to_string(),
        kind: BM25IndexKind.kind().to_string(),
        key_columns: vec!["content".to_string()],
        params: Arc::new(BM25IndexParams {
            language: Language::English,
        }),
    };
    table.create_index(index_creation.clone()).await?;

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "title1", "title2", "title3", "title4",
            ])),
            Arc::new(StringArray::from(vec![
                "The sky blushed pink as the sun dipped below the horizon.",
                "She found a forgotten letter tucked inside an old book.",
                "Apples, oranges, pink grapefruits, and more pink grapefruits.",
                "A single drop of rain fell, followed by a thousand more.",
            ])),
        ],
    )?;
    table.insert(&record_batch).await?;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let search = TableSearch {
        query: Arc::new(BM25SearchQuery {
            query: "pink".to_string(),
            limit: Some(2),
        }),
        projection: None,
    };

    let table_str = table_search(&table, search).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+--------+---------------------------------------------------------------+
| _indexlake_row_id | title  | content                                                       |
+-------------------+--------+---------------------------------------------------------------+
| 3                 | title3 | Apples, oranges, pink grapefruits, and more pink grapefruits. |
| 1                 | title1 | The sky blushed pink as the sun dipped below the horizon.     |
+-------------------+--------+---------------------------------------------------------------+"#,
    );

    Ok(())
}
