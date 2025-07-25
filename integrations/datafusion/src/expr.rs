use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::{ExprSchemable, Operator};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::prelude::SessionContext;
use datafusion::{common::ScalarValue, error::DataFusionError, prelude::Expr};
use indexlake::catalog::Scalar as ILScalar;
use indexlake::expr::BinaryOp as ILOperator;
use indexlake::expr::Expr as ILExpr;

pub fn datafusion_expr_to_indexlake_expr(
    expr: &Expr,
    schema: &DFSchema,
) -> Result<ILExpr, DataFusionError> {
    match expr {
        Expr::Alias(alias) => datafusion_expr_to_indexlake_expr(&alias.expr, schema),
        Expr::Column(col) => Ok(ILExpr::Column(col.name.clone())),
        Expr::Literal(lit, _) => Ok(ILExpr::Literal(datafusion_scalar_to_indexlake_scalar(lit)?)),
        Expr::BinaryExpr(binary) => {
            let left = Box::new(datafusion_expr_to_indexlake_expr(&binary.left, schema)?);
            let right = Box::new(datafusion_expr_to_indexlake_expr(&binary.right, schema)?);
            let op = datafusion_operator_to_indexlake_operator(&binary.op)?;
            Ok(ILExpr::BinaryExpr(indexlake::expr::BinaryExpr {
                left,
                right,
                op,
            }))
        }
        Expr::Not(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr, schema)?);
            Ok(ILExpr::Not(expr))
        }
        Expr::IsNull(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr, schema)?);
            Ok(ILExpr::IsNull(expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr, schema)?);
            Ok(ILExpr::IsNotNull(expr))
        }
        Expr::InList(in_list) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&in_list.expr, schema)?);
            let list = in_list
                .list
                .iter()
                .map(|expr| datafusion_expr_to_indexlake_expr(expr, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ILExpr::InList(indexlake::expr::InList {
                expr,
                list,
                negated: in_list.negated,
            }))
        }
        Expr::Like(like) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&like.expr, schema)?);
            let pattern = Box::new(datafusion_expr_to_indexlake_expr(&like.pattern, schema)?);
            Ok(ILExpr::Like(indexlake::expr::Like {
                expr,
                pattern,
                negated: like.negated,
                case_insensitive: like.case_insensitive,
            }))
        }
        Expr::Cast(cast) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&cast.expr, schema)?);
            Ok(ILExpr::Cast(indexlake::expr::Cast {
                expr,
                cast_type: cast.data_type.clone(),
            }))
        }
        Expr::TryCast(try_cast) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&try_cast.expr, schema)?);
            Ok(ILExpr::TryCast(indexlake::expr::TryCast {
                expr,
                cast_type: try_cast.data_type.clone(),
            }))
        }
        Expr::Negative(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr, schema)?);
            Ok(ILExpr::Negative(expr))
        }
        Expr::ScalarFunction(func) => {
            let mut arg_types = Vec::with_capacity(func.args.len());
            for arg in func.args.iter() {
                let (data_type, _) = arg.data_type_and_nullable(schema)?;
                arg_types.push(data_type);
            }
            let args = func
                .args
                .iter()
                .map(|arg| datafusion_expr_to_indexlake_expr(arg, schema))
                .collect::<Result<Vec<_>, _>>()?;
            let name = func.name().to_string();
            let return_type = func.func.return_type(&arg_types)?;
            Ok(ILExpr::Function(indexlake::expr::Function {
                name,
                args,
                return_type,
            }))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expr: {expr}"
        ))),
    }
}

pub fn datafusion_scalar_to_indexlake_scalar(
    scalar: &ScalarValue,
) -> Result<ILScalar, DataFusionError> {
    match scalar {
        ScalarValue::Boolean(v) => Ok(ILScalar::Boolean(*v)),
        ScalarValue::Int8(v) => Ok(ILScalar::Int8(*v)),
        ScalarValue::Int16(v) => Ok(ILScalar::Int16(*v)),
        ScalarValue::Int32(v) => Ok(ILScalar::Int32(*v)),
        ScalarValue::Int64(v) => Ok(ILScalar::Int64(*v)),
        ScalarValue::UInt8(v) => Ok(ILScalar::UInt8(*v)),
        ScalarValue::UInt16(v) => Ok(ILScalar::UInt16(*v)),
        ScalarValue::UInt32(v) => Ok(ILScalar::UInt32(*v)),
        ScalarValue::UInt64(v) => Ok(ILScalar::UInt64(*v)),
        ScalarValue::Float32(v) => Ok(ILScalar::Float32(*v)),
        ScalarValue::Float64(v) => Ok(ILScalar::Float64(*v)),
        ScalarValue::Utf8(v) => Ok(ILScalar::Utf8(v.clone())),
        ScalarValue::Binary(v) => Ok(ILScalar::Binary(v.clone())),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported scalar: {scalar}"
        ))),
    }
}

pub fn datafusion_operator_to_indexlake_operator(
    operator: &Operator,
) -> Result<ILOperator, DataFusionError> {
    match operator {
        Operator::Eq => Ok(ILOperator::Eq),
        Operator::NotEq => Ok(ILOperator::NotEq),
        Operator::Lt => Ok(ILOperator::Lt),
        Operator::LtEq => Ok(ILOperator::LtEq),
        Operator::Gt => Ok(ILOperator::Gt),
        Operator::GtEq => Ok(ILOperator::GtEq),
        Operator::Plus => Ok(ILOperator::Plus),
        Operator::Minus => Ok(ILOperator::Minus),
        Operator::Multiply => Ok(ILOperator::Multiply),
        Operator::Divide => Ok(ILOperator::Divide),
        Operator::Modulo => Ok(ILOperator::Modulo),
        Operator::And => Ok(ILOperator::And),
        Operator::Or => Ok(ILOperator::Or),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {operator}",
        ))),
    }
}

pub fn parse_expr(expr: &str, schema: SchemaRef) -> Result<ILExpr, DataFusionError> {
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(schema)?;
    let expr = ctx.parse_sql_expr(expr, &df_schema)?;

    let mut rewriter = TypeCoercionRewriter::new(&df_schema);
    let coerced_expr = expr.rewrite(&mut rewriter)?;

    datafusion_expr_to_indexlake_expr(&coerced_expr.data, &df_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_parse_expr() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let expr = parse_expr("id + 1", schema).unwrap();
        assert_eq!(
            expr,
            ILExpr::BinaryExpr(indexlake::expr::BinaryExpr {
                left: Box::new(ILExpr::Cast(indexlake::expr::Cast {
                    expr: Box::new(indexlake::expr::col("id")),
                    cast_type: DataType::Int64,
                })),
                right: Box::new(indexlake::expr::lit(1i64)),
                op: ILOperator::Plus,
            })
        );
    }
}
