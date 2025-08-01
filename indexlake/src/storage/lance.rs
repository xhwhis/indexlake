use std::{collections::HashSet, sync::Arc};

use arrow::array::UInt64Array;
use arrow_schema::Schema;
use futures::{StreamExt, TryStreamExt};
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::{
    v2::{
        reader::{FileReader, FileReaderOptions, ReaderProjection},
        writer::{FileWriter, FileWriterOptions},
    },
    version::LanceFileVersion,
};
use lance_io::{
    object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry},
    scheduler::{ScanScheduler, SchedulerConfig},
    utils::CachedFileSize,
};

use crate::{
    ILError, ILResult, RecordBatchStream,
    catalog::DataFileRecord,
    expr::{Expr, merge_filters},
    storage::{DataFileFormat, Storage},
};

// TODO fix local fs
pub(crate) async fn build_lance_writer(
    storage: &Storage,
    relative_path: &str,
    schema: &Schema,
    data_file_format: DataFileFormat,
) -> ILResult<FileWriter> {
    let root_path = storage.root_path()?;

    let registry = Arc::new(ObjectStoreRegistry::default());

    let mut object_store_params = ObjectStoreParams::default();
    object_store_params.storage_options = Some(storage.storage_options());

    let (object_store, _) =
        ObjectStore::from_uri_and_params(registry, &root_path, &object_store_params).await?;
    let object_writer = object_store.create(&relative_path.into()).await?;

    let version = match data_file_format {
        DataFileFormat::LanceV2_1 => LanceFileVersion::V2_1,
        _ => {
            return Err(ILError::InvalidInput(format!(
                "Cannot build lance writer for file format: {data_file_format}",
            )));
        }
    };

    let writer = FileWriter::try_new(
        object_writer,
        schema.try_into()?,
        FileWriterOptions {
            format_version: Some(version),
            ..Default::default()
        },
    )?;

    Ok(writer)
}

pub(crate) async fn read_lance_file_by_record(
    storage: &Storage,
    table_schema: &Schema,
    data_file_record: &DataFileRecord,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    row_ids: Option<&HashSet<i64>>,
    batch_size: usize,
) -> ILResult<RecordBatchStream> {
    let root_path = storage.root_path()?;

    let registry = Arc::new(ObjectStoreRegistry::default());

    let mut object_store_params = ObjectStoreParams::default();
    object_store_params.storage_options = Some(storage.storage_options());

    let (object_store, _) =
        ObjectStore::from_uri_and_params(registry, &root_path, &object_store_params).await?;

    let store_scheduler = ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(&object_store),
    );

    let scheduler = store_scheduler
        .open_file(
            &data_file_record.relative_path.clone().into(),
            &CachedFileSize::unknown(),
        )
        .await?;

    let reader = FileReader::try_open(
        scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    )
    .await?;

    let version = match data_file_record.format {
        DataFileFormat::LanceV2_1 => LanceFileVersion::V2_1,
        _ => {
            return Err(ILError::InvalidInput(format!(
                "Cannot build lance reader for file format: {}",
                data_file_record.format
            )));
        }
    };

    // TODO fix projection
    let projection = match projection {
        Some(projection) => ReaderProjection {
            schema: Arc::new(table_schema.try_into()?),
            column_indices: projection.iter().map(|idx| *idx as u32).collect(),
        },
        None => ReaderProjection::from_whole_schema(&table_schema.try_into()?, version),
    };

    let ranges = data_file_record
        .row_ranges(row_ids)?
        .into_iter()
        .map(|r| r.start as u64..r.end as u64)
        .collect::<Vec<_>>();

    let mut stream = reader.read_tasks(
        lance_io::ReadBatchParams::Ranges(ranges.into()),
        batch_size as u32,
        None,
        FilterExpression::no_filter(),
    )?;

    let filter = merge_filters(filters);

    let mut batches = Vec::new();
    while let Some(task) = stream.next().await {
        let batch = task.task.await?;
        if let Some(filter) = &filter {
            let bool_array = filter.condition_eval(&batch)?;
            let indices = bool_array
                .iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if let Some(v) = v
                        && v
                    {
                        Some(i as u64)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let index_array = UInt64Array::from(indices);
            let filtered_batch = arrow::compute::take_record_batch(&batch, &index_array)?;
            batches.push(filtered_batch);
        } else {
            batches.push(batch);
        }
    }

    Ok(Box::pin(futures::stream::iter(batches).map(Ok)))
}
