use arrow::{
    array::BinaryArray,
    datatypes::{DataType, Field, Schema},
};
use indexlake::{
    Client,
    catalog::Catalog,
    index::IndexKind,
    storage::Storage,
    table::{TableConfig, TableCreation, TableScan},
};
use indexlake_integration_tests::{
    catalog_postgres, catalog_sqlite, init_env_logger, storage_fs, storage_s3,
};
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use geo::{Geometry, Point};
use geozero::{CoordDimensions, ToWkb};
use indexlake::expr::{Expr, col, func, lit};
use indexlake::storage::DataFileFormat;
use indexlake::table::IndexCreation;
use indexlake_index_rstar::{RStarIndexKind, RStarIndexParams, WkbDialect};
use indexlake_integration_tests::utils::table_scan;

#[rstest::rstest]
#[case(async { catalog_sqlite() }, async { storage_fs() }, DataFileFormat::ParquetV2)]
#[case(async { catalog_postgres().await }, async { storage_s3().await }, DataFileFormat::ParquetV1)]
#[case(async { catalog_postgres().await }, async { storage_s3().await }, DataFileFormat::ParquetV2)]
#[case(async { catalog_postgres().await }, async { storage_s3().await }, DataFileFormat::LanceV2_0)]
#[tokio::test(flavor = "multi_thread")]
async fn create_rstar_index_on_existing_table(
    #[future(awt)]
    #[case]
    catalog: Arc<dyn Catalog>,
    #[future(awt)]
    #[case]
    storage: Arc<Storage>,
    #[case] format: DataFileFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    init_env_logger();

    let mut client = Client::new(catalog, storage);
    client.register_index_kind(Arc::new(RStarIndexKind));

    let namespace_name = uuid::Uuid::new_v4().to_string();
    client.create_namespace(&namespace_name, true).await?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("geom", DataType::Binary, false),
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
        if_not_exists: false,
    };
    client.create_table(table_creation).await?;
    let mut table = client.load_table(&namespace_name, &table_name).await?;

    let record_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(BinaryArray::from(vec![
                Geometry::from(Point::new(10.0, 10.0))
                    .to_wkb(CoordDimensions::xy())?
                    .as_slice(),
                Geometry::from(Point::new(11.0, 11.0))
                    .to_wkb(CoordDimensions::xy())?
                    .as_slice(),
                Geometry::from(Point::new(12.0, 12.0))
                    .to_wkb(CoordDimensions::xy())?
                    .as_slice(),
                Geometry::from(Point::new(13.0, 13.0))
                    .to_wkb(CoordDimensions::xy())?
                    .as_slice(),
            ])),
        ],
    )?;
    table.insert(&[record_batch]).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let index_creation = IndexCreation {
        name: "rstar_index".to_string(),
        kind: RStarIndexKind.kind().to_string(),
        key_columns: vec!["geom".to_string()],
        params: Arc::new(RStarIndexParams {
            wkb_dialect: WkbDialect::Wkb,
        }),
        if_not_exists: false,
    };
    table.create_index(index_creation).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let scan = TableScan::default().with_filters(vec![func(
        "intersects",
        vec![
            col("geom"),
            lit(Geometry::from(Point::new(11.0, 11.0)).to_wkb(CoordDimensions::xy())?),
        ],
        DataType::Boolean,
    )]);

    let table_str = table_scan(&table, scan).await?;
    println!("{}", table_str);
    assert_eq!(
        table_str,
        r#"+-------------------+----+--------------------------------------------+
| _indexlake_row_id | id | geom                                       |
+-------------------+----+--------------------------------------------+
| 2                 | 2  | 010100000000000000000026400000000000002640 |
+-------------------+----+--------------------------------------------+"#,
    );

    Ok(())
}
