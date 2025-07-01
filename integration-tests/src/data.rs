use std::sync::Arc;

use arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use indexlake::{
    ILResult, LakeClient,
    table::{Table, TableConfig, TableCreation},
};

pub async fn prepare_testing_table(client: &LakeClient, table_name: &str) -> ILResult<Table> {
    let namespace_name = "test_namespace";
    create_namespace_if_not_exists(client, namespace_name).await?;
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let table_config = TableConfig {
        inline_row_count_limit: 3,
        parquet_row_group_size: 2,
    };
    let table_creation = TableCreation {
        namespace_name: namespace_name.to_string(),
        table_name: table_name.to_string(),
        schema: table_schema.clone(),
        config: table_config,
    };
    client.create_table(table_creation).await?;
    let table = client.load_table(namespace_name, table_name).await?;

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
            Arc::new(Int32Array::from(vec![20, 21, 22, 23])),
        ],
    )?;
    table.insert(&record_batch).await?;
    // wait for dump task to finish
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    Ok(table)
}

pub async fn create_namespace_if_not_exists(
    client: &LakeClient,
    namespace_name: &str,
) -> ILResult<i64> {
    if let Some(namespace_id) = client.get_namespace_id(namespace_name).await? {
        return Ok(namespace_id);
    }
    client.create_namespace(namespace_name).await
}
