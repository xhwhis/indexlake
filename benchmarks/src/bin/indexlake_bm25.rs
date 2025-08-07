use std::sync::Arc;
use std::time::Instant;

use arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use indexlake::Client;
use indexlake::ILError;
use indexlake::index::IndexKind;
use indexlake::storage::DataFileFormat;
use indexlake::table::IndexCreation;
use indexlake::table::TableConfig;
use indexlake::table::TableCreation;
use indexlake::table::TableSearch;
use indexlake_benchmarks::data::{arrow_bm25_table_schema, new_bm25_record_batch};
use indexlake_index_bm25::BM25IndexKind;
use indexlake_index_bm25::BM25IndexParams;
use indexlake_index_bm25::BM25SearchQuery;
use indexlake_integration_tests::init_env_logger;
use indexlake_integration_tests::{catalog_postgres, storage_s3};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();
    let catalog = catalog_postgres().await;
    let storage = storage_s3();

    let mut client = Client::new(catalog, storage);
    client.register_index_kind(Arc::new(BM25IndexKind));

    let namespace_name = "test_namespace";
    client.create_namespace(namespace_name, true).await?;

    let table_name = uuid::Uuid::new_v4().to_string();
    let table_config = TableConfig {
        inline_row_count_limit: 10000,
        parquet_row_group_size: 100,
        preferred_data_file_format: DataFileFormat::ParquetV1,
    };
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.clone(),
        schema: arrow_bm25_table_schema(),
        config: table_config.clone(),
        if_not_exists: false,
    };
    client.create_table(table_creation).await?;

    let mut table = client.load_table(namespace_name, &table_name).await?;

    let index_name = "bm25_index";
    let index_creation = IndexCreation {
        name: index_name.to_string(),
        kind: BM25IndexKind.kind().to_string(),
        key_columns: vec!["content".to_string()],
        params: Arc::new(BM25IndexParams { avgdl: 256. }),
    };
    table.create_index(index_creation).await?;

    let total_rows = 1000000;
    let num_tasks = 10;
    let task_rows = total_rows / num_tasks;
    let insert_batch_size = 10000;

    let start_time = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..num_tasks {
        let table = table.clone();
        let handle = tokio::spawn(async move {
            let mut progress = 0;
            while progress < task_rows {
                let batch = new_bm25_record_batch(insert_batch_size);
                table.insert(&[batch]).await?;
                progress += insert_batch_size;
            }
            Ok::<_, ILError>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    let insert_cost_time = start_time.elapsed();
    println!(
        "IndexLake BM25: inserted {} rows, {} tasks, batch size: {}, format: {}, in {}ms",
        total_rows,
        num_tasks,
        insert_batch_size,
        table_config.preferred_data_file_format,
        insert_cost_time.as_millis()
    );

    let start_time = Instant::now();
    let limit = 10;
    let table_search = TableSearch {
        query: Arc::new(BM25SearchQuery {
            query: "杨绛女士".to_string(),
            limit: Some(limit),
        }),
        projection: None,
    };
    let mut stream = table.search(table_search).await?;
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        batches.push(batch);
    }

    let search_cost_time = start_time.elapsed();
    println!(
        "IndexLake BM25: searched {} rows in {}ms",
        limit,
        search_cost_time.as_millis()
    );

    println!("{}", pretty_format_batches(&batches).unwrap());

    Ok(())
}
