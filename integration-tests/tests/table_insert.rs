use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use futures::TryStreamExt;
use indexlake::ILError;
use indexlake::table::TableScan;
use indexlake::{
    Client,
    catalog::Catalog,
    storage::Storage,
    table::{TableConfig, TableCreation},
};
use indexlake_integration_tests::utils::full_table_scan;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;
use std::time::Duration;

#[rstest::rstest]
// TODO fix sqlite test
// #[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);

    let namespace_name = uuid::Uuid::new_v4().to_string();
    client.create_namespace(&namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_config = TableConfig {
        inline_row_count_limit: 100,
        parquet_row_group_size: 10,
        ..Default::default()
    };
    let table_creation = TableCreation {
        namespace_name: namespace_name.clone(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;
    let table = client.load_table(&namespace_name, &table_name).await?;

    let data = (0..1000i64).collect::<Vec<_>>();
    let data_chunks = data.chunks(100);

    let mut handles = Vec::new();
    for (idx, data_chunk) in data_chunks.enumerate() {
        let table = table.clone();
        let table_schema = table_schema.clone();
        let data_chunk = data_chunk.to_vec();

        let handle = tokio::spawn(async move {
            let chunks = data_chunk.chunks(idx + 1);
            for chunk in chunks {
                let record_batch = RecordBatch::try_new(
                    table_schema.clone(),
                    vec![Arc::new(Int64Array::from(chunk.to_vec()))],
                )?;
                table.insert(&[record_batch]).await?;
            }
            Ok::<(), ILError>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let stream = table.scan(TableScan::default()).await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let mut read_data = Vec::new();
    for batch in batches {
        let batch_data = batch.column(1).as_primitive::<Int64Type>();
        read_data.extend(batch_data.iter().map(|v| v.unwrap()));
    }
    read_data.sort();
    assert_eq!(read_data, data);

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn bypass_insert_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = Client::new(catalog, storage);

    let namespace_name = uuid::Uuid::new_v4().to_string();
    client.create_namespace(&namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_config = TableConfig {
        inline_row_count_limit: 3,
        parquet_row_group_size: 2,
        ..Default::default()
    };
    let table_creation = TableCreation {
        namespace_name: namespace_name.clone(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;
    let table = client.load_table(&namespace_name, &table_name).await?;

    let batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..3))],
    )?;
    table.insert(&[batch]).await?;

    let table_str = full_table_scan(&table).await?;
    println!("{table_str}");
    assert_eq!(
        table_str,
        r#"+-------------------+----+
| _indexlake_row_id | id |
+-------------------+----+
| 1                 | 0  |
| 2                 | 1  |
| 3                 | 2  |
+-------------------+----+"#,
    );

    Ok(())
}
