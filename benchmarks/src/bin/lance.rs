use std::{sync::Arc, time::Instant};

use arrow::{array::RecordBatchIterator, record_batch::RecordBatch};
use futures::StreamExt;
use indexlake_benchmarks::data::{arrow_table_schema, new_record_batch};
use indexlake_integration_tests::setup_minio;
use lance::Dataset;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // setup_minio();
    setup_s3_env();

    // create table
    let table_path = "s3://indexlake/lance/bench_table".to_string();
    let batch = RecordBatch::new_empty(arrow_table_schema());
    let batch_reader = RecordBatchIterator::new(vec![Ok(batch)], arrow_table_schema());
    let mut dataset = Dataset::write(batch_reader, &table_path, None).await?;

    // insert data
    let total_rows = 1000000;
    let insert_batch_size = 10000;

    let start_time = Instant::now();
    let mut progress = 0;
    while progress < total_rows {
        let batch = new_record_batch(insert_batch_size);
        let record_batch_reader =
            RecordBatchIterator::new(vec![batch].into_iter().map(Ok), arrow_table_schema());
        dataset.append(record_batch_reader, None).await?;
        progress += insert_batch_size;
    }

    println!(
        "Lance: inserted {} rows ({} per batch) in {}ms",
        total_rows,
        insert_batch_size,
        start_time.elapsed().as_millis()
    );

    let dataset = Dataset::open(&table_path).await?;
    let mut stream = dataset.scan().try_into_stream().await?;
    let mut count = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        count += batch.num_rows();
    }
    println!(
        "Lance: scanned {} rows in {}ms",
        count,
        start_time.elapsed().as_millis()
    );
    Ok(())
}

fn setup_s3_env() {
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "admin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "password");
        std::env::set_var("AWS_ENDPOINT", "http://127.0.0.1:9000");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_BUCKET", "indexlake");
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    }
}
