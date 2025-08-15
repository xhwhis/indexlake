use std::sync::Arc;

use datafusion::{
    error::DataFusionError, execution::FunctionRegistry, logical_expr::dml::InsertOp,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::{
    logical_plan::{
        DefaultLogicalExtensionCodec, from_proto::parse_exprs, to_proto::serialize_exprs,
    },
    physical_plan::PhysicalExtensionCodec,
};
use indexlake::{Client, table::Table};
use prost::Message;

use crate::{
    IndexLakeInsertExec, IndexLakeInsertExecNode, IndexLakeScanExec, IndexLakeScanExecNode,
};

#[derive(Debug)]
pub struct IndexLakePhysicalCodec {
    client: Arc<indexlake::Client>,
}

impl IndexLakePhysicalCodec {
    pub fn new(client: Arc<indexlake::Client>) -> Self {
        Self { client }
    }
}

impl PhysicalExtensionCodec for IndexLakePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if let Ok(node) = IndexLakeScanExecNode::decode(buf) {
            let table = load_table(&self.client, &node.namespace_name, &node.table_name)?;

            let projection = parse_projection(node.projection.as_ref());
            let filters = parse_exprs(&node.filters, registry, &DefaultLogicalExtensionCodec {})?;

            Ok(Arc::new(IndexLakeScanExec::try_new(
                Arc::new(table),
                node.partition_count as usize,
                projection,
                filters,
                node.limit.map(|l| l as usize),
            )?))
        } else if let Ok(node) = IndexLakeInsertExecNode::decode(buf) {
            if inputs.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "IndexLakeInsertExec requires exactly one input, got {}",
                    inputs.len()
                )));
            }
            let input = inputs[0].clone();

            let table = load_table(&self.client, &node.namespace_name, &node.table_name)?;

            let insert_op = parse_insert_op(node.insert_op)?;

            Ok(Arc::new(IndexLakeInsertExec::try_new(
                Arc::new(table),
                input,
                insert_op,
            )?))
        } else {
            Err(DataFusionError::Execution(format!(
                "IndexLakePhysicalCodec failed to decode physical plan"
            )))
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.as_any().downcast_ref::<IndexLakeScanExec>() {
            let projection = serialize_projection(exec.projection.as_ref());

            let filters = serialize_exprs(&exec.filters, &DefaultLogicalExtensionCodec {})?;

            let proto = IndexLakeScanExecNode {
                namespace_name: exec.table.namespace_name.clone(),
                table_name: exec.table.table_name.clone(),
                partition_count: exec.partition_count as u32,
                projection,
                filters,
                limit: exec.limit.map(|l| l as u32),
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to encode indexlake scan execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<IndexLakeInsertExec>() {
            let insert_op = serialize_insert_op(exec.insert_op);

            let proto = IndexLakeInsertExecNode {
                namespace_name: exec.table.namespace_name.clone(),
                table_name: exec.table.table_name.clone(),
                input: None,
                insert_op,
            };

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to encode indexlake insert execution plan: {e:?}"
                ))
            })?;

            Ok(())
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "IndexLakePhysicalCodec does not support encoding {}",
                node.name()
            )))
        }
    }
}

fn load_table(
    client: &Client,
    namespace_name: &str,
    table_name: &str,
) -> Result<Table, DataFusionError> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            client
                .load_table(namespace_name, table_name)
                .await
                .map_err(|e| DataFusionError::Internal(e.to_string()))
        })
    })
}

fn serialize_projection(projection: Option<&Vec<usize>>) -> Option<crate::protobuf::Projection> {
    projection.map(|p| crate::protobuf::Projection {
        projection: p.iter().map(|n| *n as u32).collect(),
    })
}

fn parse_projection(projection: Option<&crate::protobuf::Projection>) -> Option<Vec<usize>> {
    projection.map(|p| p.projection.iter().map(|n| *n as usize).collect())
}

fn serialize_insert_op(insert_op: InsertOp) -> i32 {
    let proto = match insert_op {
        InsertOp::Append => datafusion_proto::protobuf::InsertOp::Append,
        InsertOp::Overwrite => datafusion_proto::protobuf::InsertOp::Overwrite,
        InsertOp::Replace => datafusion_proto::protobuf::InsertOp::Replace,
    };
    proto.into()
}

fn parse_insert_op(insert_op: i32) -> Result<InsertOp, DataFusionError> {
    let proto = datafusion_proto::protobuf::InsertOp::try_from(insert_op)
        .map_err(|e| DataFusionError::Internal(format!("Failed to parse insert op: {e:?}")))?;
    match proto {
        datafusion_proto::protobuf::InsertOp::Append => Ok(InsertOp::Append),
        datafusion_proto::protobuf::InsertOp::Overwrite => Ok(InsertOp::Overwrite),
        datafusion_proto::protobuf::InsertOp::Replace => Ok(InsertOp::Replace),
    }
}
