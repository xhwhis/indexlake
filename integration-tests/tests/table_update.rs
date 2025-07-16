use arrow::{array::AsArray, datatypes::Int64Type};
use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;
use indexlake::ILError;
use indexlake::catalog::INTERNAL_ROW_ID_FIELD_NAME;
use indexlake::expr::{col, lit};
use indexlake::table::TableScan;
use indexlake::table::{TableConfig, TableCreation};
use indexlake::{LakeClient, catalog::Catalog, catalog::Scalar, storage::Storage};
use indexlake_integration_tests::data::prepare_simple_testing_table;
use indexlake_integration_tests::utils::full_table_scan;
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn update_table_by_condition(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let set_map = HashMap::from([("age".to_string(), Scalar::Int32(Some(30)))]);
    let condition = col("name").eq(lit("Alice"));
    table.update(set_map, &condition).await?;

    let table_str = full_table_scan(&table).await?;
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

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn update_table_by_row_id(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);
    let table = prepare_simple_testing_table(&client).await?;

    let set_map = HashMap::from([("age".to_string(), Scalar::Int32(Some(30)))]);
    let condition = col(INTERNAL_ROW_ID_FIELD_NAME).eq(lit(1i64));
    table.update(set_map, &condition).await?;

    let table_str = full_table_scan(&table).await?;
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

    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn parallel_update_different_rows_by_row_id(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "data",
        DataType::Int64,
        false,
    )]));
    let table_config = TableConfig {
        inline_row_count_limit: 100,
        parquet_row_group_size: 10,
    };
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;

    let table = client.load_table(&namespace_name, &table_name).await?;

    let data = (1..1001i64).collect::<Vec<_>>();
    let row_ids = (1..1001i64).collect::<Vec<_>>();
    let delta = 1000;
    let expected_data = (1001..2001i64).collect::<Vec<_>>();

    let record_batch =
        RecordBatch::try_new(table_schema.clone(), vec![Arc::new(Int64Array::from(data))])?;
    table.insert(&record_batch).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut handles = Vec::new();
    let row_ids_chunks = row_ids.chunks(100);
    for row_id_chunk in row_ids_chunks {
        let table = table.clone();
        let row_id_chunk = row_id_chunk.to_vec();

        let handle = tokio::spawn(async move {
            for row_id in row_id_chunk {
                let new_data = row_id + delta;
                let set_map = HashMap::from([("data".to_string(), Scalar::Int64(Some(new_data)))]);
                let condition = col(INTERNAL_ROW_ID_FIELD_NAME).eq(lit(row_id));
                table.update(set_map, &condition).await?;
            }
            Ok::<_, ILError>(())
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

    assert_eq!(read_data, expected_data);
    Ok(())
}

#[rstest::rstest]
#[case(async { catalog_sqlite() }, storage_fs())]
#[case(async { catalog_postgres().await }, storage_s3())]
#[tokio::test(flavor = "multi_thread")]
async fn parallel_update_different_rows_by_condition(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[case] storage: Arc<Storage>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let client = LakeClient::new(catalog, storage);

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "data",
        DataType::Int64,
        false,
    )]));
    let table_config = TableConfig {
        inline_row_count_limit: 100,
        parquet_row_group_size: 10,
    };
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;

    let table = client.load_table(&namespace_name, &table_name).await?;

    let data = (1..1001i64).collect::<Vec<_>>();
    let delta = 1000;
    let expected_data = (1001..2001i64).collect::<Vec<_>>();

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![Arc::new(Int64Array::from(data.clone()))],
    )?;
    table.insert(&record_batch).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut handles = Vec::new();
    let data_chunks = data.chunks(100);
    for data_chunk in data_chunks {
        let table = table.clone();
        let data_chunk = data_chunk.to_vec();

        let handle = tokio::spawn(async move {
            for data in data_chunk {
                let new_data = data + delta;
                let set_map = HashMap::from([("data".to_string(), Scalar::Int64(Some(new_data)))]);
                let condition = col("data").eq(lit(data));
                table.update(set_map, &condition).await?;
            }
            Ok::<_, ILError>(())
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

    assert_eq!(read_data, expected_data);
    Ok(())
}
