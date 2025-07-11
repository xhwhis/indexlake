use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, UInt64Array},
    datatypes::{DataType, Float64Type, Int64Type},
};
use futures::StreamExt;
use geozero::wkb::WkbDialect as GeozeroWkbDialect;
use indexlake::{
    ILError, ILResult,
    catalog::Scalar,
    expr::Expr,
    index::{
        FilterIndexEntries, Index, IndexBuilder, IndexDefination, IndexDefinationRef, IndexParams,
        SearchIndexEntries, SearchQuery,
    },
    storage::InputFile,
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use rstar::{AABB, RTree, RTreeObject};
use serde::{Deserialize, Serialize};

use crate::{RStarIndexBuilder, compute_aabb};

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

    fn builder(&self, index_def: &IndexDefinationRef) -> ILResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(RStarIndexBuilder::try_new(index_def.clone())?))
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

    fn supports_filter(&self, index_def: &IndexDefination, filter: &Expr) -> ILResult<bool> {
        match filter {
            Expr::Function(function) => {
                if function.name == "intersects" {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    async fn filter(
        &self,
        index_def: &IndexDefination,
        index_file: InputFile,
        filters: &[Expr],
    ) -> ILResult<FilterIndexEntries> {
        let params = index_def.downcast_params::<RStarIndexParams>()?;

        let arrow_reader_builder = ParquetRecordBatchStreamBuilder::new(index_file).await?;
        let mut batch_stream = arrow_reader_builder.build()?;

        let aabb = match &filters[0] {
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

        while let Some(batch) = batch_stream.next().await {
            let batch = batch?;
            let row_id_array = batch.column(0).as_primitive::<Int64Type>().clone();
            let xmin_array = batch.column(1).as_primitive::<Float64Type>().clone();
            let ymin_array = batch.column(2).as_primitive::<Float64Type>().clone();
            let xmax_array = batch.column(3).as_primitive::<Float64Type>().clone();
            let ymax_array = batch.column(4).as_primitive::<Float64Type>().clone();

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
        }

        let row_id_array = arrow::compute::concat(
            &selected_row_id_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>(),
        )?;
        let row_id_array = row_id_array.as_primitive::<Int64Type>().clone();

        Ok(FilterIndexEntries {
            row_ids: row_id_array,
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
    pub fn to_geozero(&self) -> GeozeroWkbDialect {
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
