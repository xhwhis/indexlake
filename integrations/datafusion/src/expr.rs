use datafusion::logical_expr::Operator;
use datafusion::{common::ScalarValue, error::DataFusionError, prelude::Expr};
use indexlake::catalog::Scalar as ILScalar;
use indexlake::expr::BinaryOp as ILOperator;
use indexlake::expr::Expr as ILExpr;

pub fn datafusion_expr_to_indexlake_expr(expr: &Expr) -> Result<ILExpr, DataFusionError> {
    match expr {
        Expr::Column(col) => Ok(ILExpr::Column(col.name.clone())),
        Expr::Literal(lit) => Ok(ILExpr::Literal(datafusion_scalar_to_indexlake_scalar(lit)?)),
        Expr::BinaryExpr(binary) => {
            let left = Box::new(datafusion_expr_to_indexlake_expr(&binary.left)?);
            let right = Box::new(datafusion_expr_to_indexlake_expr(&binary.right)?);
            let op = datafusion_operator_to_indexlake_operator(&binary.op)?;
            Ok(ILExpr::BinaryExpr(indexlake::expr::BinaryExpr {
                left,
                right,
                op,
            }))
        }
        Expr::Not(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr)?);
            Ok(ILExpr::Not(expr))
        }
        Expr::IsNull(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr)?);
            Ok(ILExpr::IsNull(expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr)?);
            Ok(ILExpr::IsNotNull(expr))
        }
        Expr::InList(in_list) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&in_list.expr)?);
            let list = in_list
                .list
                .iter()
                .map(|expr| datafusion_expr_to_indexlake_expr(expr))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ILExpr::InList(indexlake::expr::InList {
                expr,
                list,
                negated: in_list.negated,
            }))
        }
        Expr::Like(like) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&like.expr)?);
            let pattern = Box::new(datafusion_expr_to_indexlake_expr(&like.pattern)?);
            Ok(ILExpr::Like(indexlake::expr::Like {
                expr,
                pattern,
                negated: like.negated,
                case_insensitive: like.case_insensitive,
            }))
        }
        Expr::Cast(cast) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(&cast.expr)?);
            Ok(ILExpr::Cast(indexlake::expr::Cast {
                expr,
                cast_type: cast.data_type.clone(),
                cast_options: None,
            }))
        }
        Expr::Negative(expr) => {
            let expr = Box::new(datafusion_expr_to_indexlake_expr(expr)?);
            Ok(ILExpr::Negative(expr))
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
            "Unsupported operator: {:?}",
            operator
        ))),
    }
}
