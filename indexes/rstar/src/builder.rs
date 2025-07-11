use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use geo::BoundingRect;
use geozero::wkb::FromWkb;
use indexlake::{
    ILError, ILResult,
    index::{IndexBuilder, IndexDefinationRef},
    storage::OutputFile,
    utils::extract_row_id_array_from_record_batch,
};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
use rstar::AABB;

use crate::{RStarIndexParams, WkbDialect};

#[derive(Debug, Clone)]
pub struct RStarIndexBuilder {
    index_def: IndexDefinationRef,
    index_schema: SchemaRef,
    index_batches: Vec<RecordBatch>,
}

impl RStarIndexBuilder {
    pub fn try_new(index_def: IndexDefinationRef) -> ILResult<Self> {
        let index_schema = index_schema();
        Ok(Self {
            index_def,
            index_schema,
            index_batches: Vec::new(),
        })
    }
}

#[async_trait::async_trait]
impl IndexBuilder for RStarIndexBuilder {
    fn append(&mut self, batch: &RecordBatch) -> ILResult<()> {
        let params = self.index_def.downcast_params::<RStarIndexParams>()?;

        let row_id_array = extract_row_id_array_from_record_batch(&batch)?;

        let key_column_name = &self.index_def.key_columns[0];
        let key_column_index = self.index_def.table_schema.index_of(&key_column_name)?;
        let key_column = batch.column(key_column_index);
        let aabbs = compute_aabbs(key_column, params.wkb_dialect)?;

        let index_batch = build_index_record_batch(self.index_schema.clone(), row_id_array, aabbs)?;

        self.index_batches.push(index_batch);

        Ok(())
    }

    async fn write(&mut self, output_file: OutputFile) -> ILResult<()> {
        let writer_properties = WriterProperties::builder()
            .set_max_row_group_size(4096)
            .build();
        let mut arrow_writer = AsyncArrowWriter::try_new(
            output_file,
            self.index_schema.clone(),
            Some(writer_properties),
        )?;

        for batch in self.index_batches.iter() {
            arrow_writer.write(batch).await?;
        }

        arrow_writer.close().await?;

        Ok(())
    }
}

fn index_schema() -> SchemaRef {
    let fields = vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("xmin", DataType::Float64, true),
        Field::new("ymin", DataType::Float64, true),
        Field::new("xmax", DataType::Float64, true),
        Field::new("ymax", DataType::Float64, true),
    ];
    Arc::new(Schema::new(fields))
}

fn build_index_record_batch(
    index_schema: SchemaRef,
    row_id_array: Int64Array,
    aabbs: Vec<Option<AABB<geo::Coord<f64>>>>,
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
    let arrays = vec![
        Arc::new(row_id_array) as ArrayRef,
        Arc::new(xmin_array) as ArrayRef,
        Arc::new(ymin_array) as ArrayRef,
        Arc::new(xmax_array) as ArrayRef,
        Arc::new(ymax_array) as ArrayRef,
    ];

    let record_batch = RecordBatch::try_new(index_schema, arrays)?;

    Ok(record_batch)
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

pub(crate) fn compute_aabb(wkb: &[u8], wkb_dialect: WkbDialect) -> ILResult<AABB<geo::Coord<f64>>> {
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
