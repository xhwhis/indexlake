use std::sync::Arc;

use arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use indexlake::{
    Client, ILResult,
    storage::DataFileFormat,
    table::{Table, TableConfig, TableCreation},
};

pub async fn prepare_simple_testing_table(
    client: &Client,
    format: DataFileFormat,
) -> ILResult<Table> {
    let namespace_name = uuid::Uuid::new_v4().to_string();
    client.create_namespace(&namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let table_config = TableConfig {
        inline_row_count_limit: 3,
        parquet_row_group_size: 2,
        preferred_data_file_format: format,
    };
    let table_name = uuid::Uuid::new_v4().to_string();
    let table_creation = TableCreation {
        namespace_name: namespace_name.clone(),
        table_name: table_name.clone(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;

    let table = client.load_table(&namespace_name, &table_name).await?;

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![20, 21])),
        ],
    )?;
    table.insert(&[record_batch]).await?;

    log::info!("LWZTEST first insert success");

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Charlie", "David"])),
            Arc::new(Int32Array::from(vec![22, 23])),
        ],
    )?;
    table.insert(&[record_batch]).await?;

    log::info!("LWZTEST second insert success");

    // wait for dump task to finish
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    Ok(table)
}
