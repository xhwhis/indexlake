use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef},
};
use futures::StreamExt;
use geo::BoundingRect;
use geozero::wkb::{FromWkb, WkbDialect as GeozeroWkbDialect};
use indexlake::{
    ILError, ILResult, RecordBatchStream,
    catalog::Scalar,
    expr::Expr,
    index::{
        FilterIndexEntries, Index, IndexDefination, IndexParams, SearchIndexEntries, SearchQuery,
    },
    storage::{InputFile, OutputFile},
    utils::extract_row_id_array_from_record_batch,
};
use parquet::{
    arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};
use rstar::{AABB, RTree, RTreeObject};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RStarIndex;

#[async_trait::async_trait]
impl Index for RStarIndex {
    fn kind(&self) -> &str {
        "rstar"
    }

    fn decode_params(&self, value: &str) -> ILResult<Arc<dyn IndexParams>> {
        let params = serde_json::from_str::<RStarIndexParams>(value)
            .map_err(|e| ILError::IndexError(format!("Failed to parse RStarIndexParams: {e}")))?;
        Ok(Arc::new(params))
    }

    fn supports(&self, index_def: &IndexDefination) -> ILResult<()> {
        if index_def.key_columns.len() != 1 {
            return Err(ILError::IndexError(format!(
                "RStar index requires exactly one key column"
            )));
        }
        let key_column_name = &index_def.key_columns[0];
        let key_field = index_def.table_schema.field_with_name(&key_column_name)?;
        if !matches!(
            key_field.data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ) {
            return Err(ILError::IndexError(format!(
                "RStar index key column must be a binary / large binary / binary view column"
            )));
        }
        Ok(())
    }

    async fn build(
        &self,
        index_def: &IndexDefination,
        mut batch_stream: RecordBatchStream,
        output_file: OutputFile,
    ) -> ILResult<()> {
        let params = index_def.downcast_params::<RStarIndexParams>()?;

        let include_fields = index_def.include_fields()?;
        let index_schema = index_schema(include_fields);

        let writer_properties = WriterProperties::builder()
            .set_max_row_group_size(4096)
            .build();
        let mut arrow_writer =
            AsyncArrowWriter::try_new(output_file, index_schema.clone(), Some(writer_properties))?;

        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;

            let row_id_array = extract_row_id_array_from_record_batch(&batch)?;

            let key_column_name = &index_def.key_columns[0];
            let key_column_index = index_def.table_schema.index_of(&key_column_name)?;
            let key_column = batch.column(key_column_index);
            let aabbs = compute_aabbs(key_column, params.wkb_dialect)?;

            let include_arrays = index_def.include_arrays(&batch)?;

            let index_batch = build_index_record_batch(
                index_schema.clone(),
                row_id_array,
                aabbs,
                include_arrays,
            )?;

            arrow_writer.write(&index_batch).await?;
        }

        arrow_writer.close().await?;

        Ok(())
    }

    async fn search(
        &self,
        _index_def: &IndexDefination,
        _index_file: InputFile,
        _query: &dyn SearchQuery,
    ) -> ILResult<SearchIndexEntries> {
        Err(ILError::NotSupported(format!(
            "RStar index does not support search"
        )))
    }

    async fn filter(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        filter: &Expr,
    ) -> ILResult<FilterIndexEntries> {
        let params = index_def.downcast_params::<RStarIndexParams>()?;

        let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(index_file).await?;
        let mut batch_stream = arrow_reader_builder.build()?;

        let aabb = match filter {
            Expr::Literal(Scalar::Binary(Some(wkb))) => {
                let aabb = compute_aabb(wkb, params.wkb_dialect)?;
                AABB::from_corners(
                    [aabb.lower().x, aabb.lower().y],
                    [aabb.upper().x, aabb.upper().y],
                )
            }
            _ => todo!(),
        };

        let mut selected_row_id_arrays = Vec::new();
        let mut selected_include_array_map: HashMap<String, Vec<ArrayRef>> = HashMap::new();

        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            let row_id_array = batch.column(0).as_primitive::<Int64Type>().clone();
            let xmin_array = batch.column(1).as_primitive::<Float64Type>().clone();
            let ymin_array = batch.column(2).as_primitive::<Float64Type>().clone();
            let xmax_array = batch.column(3).as_primitive::<Float64Type>().clone();
            let ymax_array = batch.column(4).as_primitive::<Float64Type>().clone();
            let include_array_map = index_def.include_array_map(&batch)?;

            let mut rtree_objects = Vec::with_capacity(batch.num_rows());
            for offset in 0..batch.num_rows() {
                if !xmin_array.is_null(offset) {
                    let aabb = AABB::from_corners(
                        [xmin_array.value(offset), ymin_array.value(offset)],
                        [xmax_array.value(offset), ymax_array.value(offset)],
                    );
                    let index_row = IndexTreeObject { aabb, offset };
                    rtree_objects.push(index_row);
                }
            }
            let rtree = RTree::bulk_load(rtree_objects);
            let selection = rtree.locate_in_envelope_intersecting(&aabb);
            let indecies = selection
                .into_iter()
                .map(|object| object.offset as u64)
                .collect::<Vec<_>>();
            let index_array = Arc::new(UInt64Array::from(indecies)) as ArrayRef;

            let selected_row_id_array = arrow::compute::take(&row_id_array, &index_array, None)?;
            selected_row_id_arrays.push(selected_row_id_array);

            for (col_name, array) in include_array_map.iter() {
                let selected_array = arrow::compute::take(array, &index_array, None)?;
                selected_include_array_map
                    .entry(col_name.clone())
                    .or_default()
                    .push(selected_array);
            }
        }

        let row_id_array = arrow::compute::concat(
            &selected_row_id_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )?;
        let row_id_array = row_id_array.as_primitive::<Int64Type>().clone();

        let mut include_array_map = HashMap::new();
        for (col_name, arrays) in selected_include_array_map.iter() {
            let concat_array = arrow::compute::concat(
                &arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?;
            include_array_map.insert(col_name.clone(), concat_array);
        }

        Ok(FilterIndexEntries {
            row_ids: row_id_array,
            include_columns: include_array_map,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RStarIndexParams {
    pub wkb_dialect: WkbDialect,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WkbDialect {
    Wkb,
    Ewkb,
    Geopackage,
    MySQL,
    SpatiaLite,
}

impl WkbDialect {
    fn to_geozero(&self) -> GeozeroWkbDialect {
        match self {
            WkbDialect::Wkb => GeozeroWkbDialect::Wkb,
            WkbDialect::Ewkb => GeozeroWkbDialect::Ewkb,
            WkbDialect::Geopackage => GeozeroWkbDialect::Geopackage,
            WkbDialect::MySQL => GeozeroWkbDialect::MySQL,
            WkbDialect::SpatiaLite => GeozeroWkbDialect::SpatiaLite,
        }
    }
}

impl IndexParams for RStarIndexParams {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn encode(&self) -> ILResult<String> {
        serde_json::to_string(self)
            .map_err(|e| ILError::IndexError(format!("Failed to serialize RStarIndexParams: {e}")))
    }
}

fn compute_aabbs(
    array: &ArrayRef,
    wkb_dialect: WkbDialect,
) -> ILResult<Vec<Option<AABB<geo::Coord<f64>>>>> {
    let data_type = array.data_type();
    let mut aabbs = Vec::new();
    match data_type {
        DataType::Binary => {
            let binary_array = array.as_binary::<i32>();
            for wkb_opt in binary_array.iter() {
                match wkb_opt {
                    Some(wkb) => {
                        let aabb = compute_aabb(wkb, wkb_dialect)?;
                        aabbs.push(Some(aabb));
                    }
                    None => {
                        aabbs.push(None);
                    }
                }
            }
        }
        DataType::LargeBinary => {
            let large_binary_array = array.as_binary::<i64>();
            for wkb_opt in large_binary_array.iter() {
                match wkb_opt {
                    Some(wkb) => {
                        let aabb = compute_aabb(wkb, wkb_dialect)?;
                        aabbs.push(Some(aabb));
                    }
                    None => {
                        aabbs.push(None);
                    }
                }
            }
        }
        DataType::BinaryView => {
            let binary_view_array = array.as_binary_view();
            for wkb_opt in binary_view_array.iter() {
                match wkb_opt {
                    Some(wkb) => {
                        let aabb = compute_aabb(wkb, wkb_dialect)?;
                        aabbs.push(Some(aabb));
                    }
                    None => {
                        aabbs.push(None);
                    }
                }
            }
        }
        _ => {
            return Err(ILError::IndexError(format!(
                "Unsupported data type to compute AABB: {data_type}"
            )));
        }
    }
    Ok(aabbs)
}

fn compute_aabb(wkb: &[u8], wkb_dialect: WkbDialect) -> ILResult<AABB<geo::Coord<f64>>> {
    let mut rdr = std::io::Cursor::new(wkb);
    let geom = geo::Geometry::from_wkb(&mut rdr, wkb_dialect.to_geozero())
        .map_err(|e| ILError::IndexError(format!("Failed to parse ewkb: {:?}", e)))?;
    if let Some(rect) = geom.bounding_rect() {
        Ok(AABB::from_corners(rect.min(), rect.max()))
    } else {
        Err(ILError::IndexError(format!(
            "Failed to compute AABB of geometry"
        )))
    }
}

fn index_schema(include_fields: Vec<&Field>) -> SchemaRef {
    let mut fields = vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("xmin", DataType::Float64, true),
        Field::new("ymin", DataType::Float64, true),
        Field::new("xmax", DataType::Float64, true),
        Field::new("ymax", DataType::Float64, true),
    ];
    fields.extend(include_fields.into_iter().map(|field| field.clone()));
    Arc::new(Schema::new(fields))
}

fn build_index_record_batch(
    index_schema: SchemaRef,
    row_id_array: Int64Array,
    aabbs: Vec<Option<AABB<geo::Coord<f64>>>>,
    include_arrays: Vec<ArrayRef>,
) -> ILResult<RecordBatch> {
    let xmin_array = Float64Array::from(
        aabbs
            .iter()
            .map(|aabb| aabb.map(|aabb| aabb.lower().x))
            .collect::<Vec<_>>(),
    );
    let ymin_array = Float64Array::from(
        aabbs
            .iter()
            .map(|aabb| aabb.map(|aabb| aabb.lower().y))
            .collect::<Vec<_>>(),
    );
    let xmax_array = Float64Array::from(
        aabbs
            .iter()
            .map(|aabb| aabb.map(|aabb| aabb.upper().x))
            .collect::<Vec<_>>(),
    );
    let ymax_array = Float64Array::from(
        aabbs
            .iter()
            .map(|aabb| aabb.map(|aabb| aabb.upper().y))
            .collect::<Vec<_>>(),
    );
    let mut arrays = vec![
        Arc::new(row_id_array) as ArrayRef,
        Arc::new(xmin_array) as ArrayRef,
        Arc::new(ymin_array) as ArrayRef,
        Arc::new(xmax_array) as ArrayRef,
        Arc::new(ymax_array) as ArrayRef,
    ];
    arrays.extend(include_arrays);

    let record_batch = RecordBatch::try_new(index_schema, arrays)?;

    Ok(record_batch)
}

struct IndexTreeObject {
    aabb: AABB<[f64; 2]>,
    offset: usize,
}

impl RTreeObject for IndexTreeObject {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.aabb
    }
}
