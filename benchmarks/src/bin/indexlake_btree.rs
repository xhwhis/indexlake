use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use indexlake::catalog::Scalar;
use indexlake::index::IndexKind;
use indexlake::storage::DataFileFormat;
use indexlake::table::{IndexCreation, TableConfig, TableCreation, TableSearch};
use indexlake::{Client, ILError};
use indexlake_benchmarks::data::{
    arrow_btree_integer_table_schema, arrow_btree_string_table_schema,
    new_btree_integer_record_batch, new_btree_string_record_batch,
};
use indexlake_index_btree::{BTreeIndexKind, BTreeIndexParams, BTreeSearchQuery};
use indexlake_integration_tests::{catalog_postgres, init_env_logger, storage_s3};

#[derive(Clone)]
enum DataType {
    Integer,
    String,
}

#[derive(Clone)]
struct BenchmarkConfig {
    data_type: DataType,
    total_rows: usize,
    batch_size: usize,
    node_size: usize,
    concurrent_tasks: usize,
    namespace_suffix: String,
}

impl BenchmarkConfig {
    fn new_integer(total_rows: usize) -> Self {
        Self {
            data_type: DataType::Integer,
            total_rows,
            batch_size: 1024,
            node_size: 1024,
            concurrent_tasks: 10,
            namespace_suffix: "integer".to_string(),
        }
    }

    fn new_string(total_rows: usize) -> Self {
        Self {
            data_type: DataType::String,
            total_rows,
            batch_size: 1024,
            node_size: 128,
            concurrent_tasks: 5,
            namespace_suffix: "string".to_string(),
        }
    }

    fn new_node_size(total_rows: usize, node_size: usize) -> Self {
        Self {
            data_type: DataType::Integer,
            total_rows,
            batch_size: 1024,
            node_size,
            concurrent_tasks: 1,
            namespace_suffix: format!("node_size_{}", node_size),
        }
    }

    fn namespace_name(&self) -> String {
        format!("btree_benchmark_{}", self.namespace_suffix)
    }

    fn schema(&self) -> SchemaRef {
        match self.data_type {
            DataType::Integer => arrow_btree_integer_table_schema(),
            DataType::String => arrow_btree_string_table_schema(),
        }
    }

    fn record_batch(&self, batch_size: usize) -> RecordBatch {
        match self.data_type {
            DataType::Integer => new_btree_integer_record_batch(batch_size),
            DataType::String => new_btree_string_record_batch(batch_size),
        }
    }

    fn key_column(&self) -> String {
        match self.data_type {
            DataType::Integer => "integer".to_owned(),
            DataType::String => "string".to_owned(),
        }
    }

    fn index_name(&self) -> String {
        format!("{}_index_{}", self.key_column(), self.node_size)
    }

    fn point_query(&self) -> BTreeSearchQuery {
        match self.data_type {
            DataType::Integer => {
                BTreeSearchQuery::Point {
                    key: Scalar::Int32(Some(100)),
                    limit: Some(100),
                }
            }
            DataType::String => {
                BTreeSearchQuery::Point {
                    key: Scalar::Utf8(Some("Alice000100".to_string())),
                    limit: Some(100),
                }
            }
        }
    }

    fn range_query(&self) -> BTreeSearchQuery {
        match self.data_type {
            DataType::Integer => {
                BTreeSearchQuery::Range {
                    start: Some(Scalar::Int32(Some(100))),
                    end: Some(Scalar::Int32(Some(200))),
                    limit: Some(100),
                }
            }
            DataType::String => {
                BTreeSearchQuery::Range {
                    start: Some(Scalar::Utf8(Some("Alice".to_string()))),
                    end: Some(Scalar::Utf8(Some("Bob".to_string()))),
                    limit: Some(100),
                }
            }
        }
    }
}

struct BenchmarkResult {
    config: BenchmarkConfig,
    index_build_time_ms: u128,
    point_query_time_ms: u128,
    range_query_time_ms: u128,
    point_results_count: usize,
    range_results_count: usize,
}

impl BenchmarkResult {
    fn index_type_display(&self) -> String {
        format!("B-tree (node_size={})", self.config.node_size)
    }

    fn print_summary(&self) {
        println!(
            "=== {} index performance ({}) | rows: {:>6} | node: {:>4} ===",
            self.index_type_display(),
            self.config.key_column(),
            self.config.total_rows,
            self.config.node_size
        );
        println!(
            "build: {:>6}ms | point: {:>4}ms ({:>3} results) | range: {:>4}ms ({:>4} results)",
            self.index_build_time_ms,
            self.point_query_time_ms,
            self.point_results_count,
            self.range_query_time_ms,
            self.range_results_count
        );
    }
}

struct BenchmarkContext {
    client: Client,
}

impl BenchmarkContext {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let catalog = catalog_postgres().await;
        let storage = storage_s3().await;
        let mut client = Client::new(catalog, storage);
        client.register_index_kind(Arc::new(BTreeIndexKind));

        Ok(Self { client })
    }

    async fn create_table_and_index(
        &self, config: &BenchmarkConfig,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let namespace_name = config.namespace_name();
        self.client.create_namespace(&namespace_name, true).await?;

        let table_name = uuid::Uuid::new_v4().to_string();
        let table_config = TableConfig {
            inline_row_count_limit: config.batch_size,
            parquet_row_group_size: 128,
            preferred_data_file_format: DataFileFormat::ParquetV1,
        };
        let table_creation = TableCreation {
            namespace_name: namespace_name.clone(),
            table_name: table_name.clone(),
            schema: config.schema(),
            default_values: HashMap::new(),
            config: table_config,
            if_not_exists: false,
        };
        self.client.create_table(table_creation).await?;

        let index_creation = IndexCreation {
            name: config.index_name(),
            kind: BTreeIndexKind.kind().to_string(),
            key_columns: vec![config.key_column().to_string()],
            params: Arc::new(BTreeIndexParams {
                node_size: config.node_size,
            }),
            if_not_exists: false,
        };
        let table = self.client.load_table(&namespace_name, &table_name).await?;
        table.create_index(index_creation).await?;

        Ok(table_name)
    }

    async fn insert_data_concurrent(
        &self, config: &BenchmarkConfig, table_name: &str,
    ) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
        let namespace_name = config.namespace_name();
        let table = self.client.load_table(&namespace_name, table_name).await?;

        let start_time = Instant::now();

        let task_rows = config.total_rows / config.concurrent_tasks;
        let mut handles = Vec::with_capacity(config.concurrent_tasks);

        for _ in 0..config.concurrent_tasks {
            let table = table.clone();
            let config = config.clone();
            let handle = tokio::spawn(async move {
                let mut progress = 0;
                while progress < task_rows {
                    let batch_size = (task_rows - progress).min(config.batch_size);
                    table.insert(&[config.record_batch(batch_size)]).await?;
                    progress += batch_size;
                }
                Ok::<_, ILError>(())
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await??;
        }

        Ok(start_time.elapsed())
    }

    async fn execute_query(
        &self, config: &BenchmarkConfig, table_name: &str, query: BTreeSearchQuery,
    ) -> Result<(std::time::Duration, usize), Box<dyn std::error::Error>> {
        let namespace_name = config.namespace_name();
        let table = self.client.load_table(&namespace_name, table_name).await?;

        let start_time = Instant::now();
        let table_search = TableSearch {
            query: Arc::new(query),
            projection: None,
        };

        let mut stream = table.search(table_search).await?;
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        let query_time = start_time.elapsed();
        let results_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok((query_time, results_count))
    }

    async fn benchmark_btree(
        &self, config: BenchmarkConfig,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let table_name = self.create_table_and_index(&config).await?;

        let insert_time = self.insert_data_concurrent(&config, &table_name).await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

        let point_query = config.point_query();

        let (point_query_time, point_results_count) = self
            .execute_query(&config, &table_name, point_query)
            .await?;

        let range_query = config.range_query();

        let (range_query_time, range_results_count) = self
            .execute_query(&config, &table_name, range_query)
            .await?;

        Ok(BenchmarkResult {
            config,
            index_build_time_ms: insert_time.as_millis(),
            point_query_time_ms: point_query_time.as_millis(),
            range_query_time_ms: range_query_time.as_millis(),
            point_results_count,
            range_results_count,
        })
    }

    async fn benchmark_btree_integer(
        &self, total_rows: usize,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let config = BenchmarkConfig::new_integer(total_rows);
        self.benchmark_btree(config).await
    }

    async fn benchmark_btree_string(
        &self, total_rows: usize,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let config = BenchmarkConfig::new_string(total_rows);
        self.benchmark_btree(config).await
    }

    async fn benchmark_btree_node_size(
        &self, node_size: usize, total_rows: usize,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let config = BenchmarkConfig::new_node_size(total_rows, node_size);
        self.benchmark_btree(config).await
    }
}

async fn run_node_size_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== B-tree node size comparison benchmark ===");

    let context = BenchmarkContext::init().await?;
    let total_rows = 100000;
    let node_sizes = vec![128, 256, 512, 1024];

    println!("dataset size: {:>8} rows", total_rows);
    println!("testing node sizes: {:?}", node_sizes);
    println!("{}", "=".repeat(60));

    let mut results = Vec::with_capacity(node_sizes.len());

    for node_size in node_sizes {
        println!("testing B-tree with node size: {:>4}", node_size);

        match context
            .benchmark_btree_node_size(node_size, total_rows)
            .await
        {
            Ok(result) => {
                result.print_summary();
                results.push(result);
            }
            Err(e) => {
                eprintln!("error benchmarking node size {:>4}: {}", node_size, e);
            }
        }
    }

    print_node_size_comparison_summary(&results);
    Ok(())
}

fn print_node_size_comparison_summary(results: &[BenchmarkResult]) {
    println!("=== node size comparison summary ===");
    println!(
        "{:<23} {:>10} {:>10} {:>10} {:>8} {:>8}",
        "type", "build(ms)", "point(ms)", "range(ms)", "pt_res", "rg_res"
    );
    println!("{}", "-".repeat(78));

    for result in results {
        println!(
            "{:<23} {:>10} {:>10} {:>10} {:>8} {:>8}",
            result.index_type_display(),
            result.index_build_time_ms,
            result.point_query_time_ms,
            result.range_query_time_ms,
            result.point_results_count,
            result.range_results_count
        );
    }

    let fastest = (
        results.iter().min_by_key(|r| r.index_build_time_ms),
        results.iter().min_by_key(|r| r.point_query_time_ms),
        results.iter().min_by_key(|r| r.range_query_time_ms),
    );

    if let (Some(build), Some(point), Some(range)) = fastest {
        println!(
            "best: build {}({:>4}ms) | point {}({:>3}ms) | range {}({:>3}ms)",
            build.index_type_display(),
            build.index_build_time_ms,
            point.index_type_display(),
            point.point_query_time_ms,
            range.index_type_display(),
            range.range_query_time_ms
        );
    }
}

async fn run_data_type_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== data type comparison benchmark ===");

    let context = BenchmarkContext::init().await?;

    println!("running integer benchmark (100,000 rows)...");
    let int_result = context.benchmark_btree_integer(100000).await?;
    int_result.print_summary();

    println!("running string benchmark (10,000 rows)...");
    let string_result = context.benchmark_btree_string(10000).await?;
    string_result.print_summary();

    print_data_type_comparison_summary(&int_result, &string_result);
    Ok(())
}

fn print_data_type_comparison_summary(
    int_result: &BenchmarkResult, string_result: &BenchmarkResult,
) {
    println!("=== performance comparison ===");
    println!(
        "{:>8} | {:>7} | {:>9} | {:>9} | {:>9}",
        "type", "rows", "build(ms)", "point(ms)", "range(ms)"
    );
    println!("{}", "-".repeat(54));

    for result in [int_result, string_result] {
        println!(
            "{:>8} | {:>7} | {:>9} | {:>9} | {:>9}",
            result.config.key_column(),
            result.config.total_rows,
            result.index_build_time_ms,
            result.point_query_time_ms,
            result.range_query_time_ms
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    println!("=== IndexLake B-tree benchmark suite: data types + node sizes ===");

    run_data_type_comparison().await?;
    println!("{}", "=".repeat(80));
    run_node_size_comparison().await?;

    println!("=== benchmark complete ===");

    Ok(())
}
