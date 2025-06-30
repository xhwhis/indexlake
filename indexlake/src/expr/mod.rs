mod binary;
mod builder;
mod visitor;

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch};
pub use binary::*;
pub use visitor::*;

use derive_visitor::{Drive, DriveMut};

use crate::{
    ILError, ILResult,
    catalog::CatalogDatabase,
    catalog::{Row, Scalar},
};

/// Represents logical expressions such as `A + 1`
#[derive(Debug, Clone, Drive, DriveMut)]
pub enum Expr {
    /// A named reference
    Column(String),
    /// A constant value
    Literal(Scalar),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// Negation of an expression. The expression's type must be a boolean to make sense
    Not(Box<Expr>),
    /// True if argument is NULL, false otherwise
    IsNull(Box<Expr>),
    /// True if argument is not NULL, false otherwise
    IsNotNull(Box<Expr>),
    /// True if argument is true, false otherwise
    IsTrue(Box<Expr>),
    /// True if argument is false, false otherwise
    IsFalse(Box<Expr>),
    /// True if argument is FALSE or NULL, false otherwise
    IsNotTrue(Box<Expr>),
    /// True if argument is TRUE or NULL, false otherwise
    IsNotFalse(Box<Expr>),
    /// Returns whether the list contains the expr value
    InList(InList),
}

impl Expr {
    pub fn eval(&self, batch: &RecordBatch) -> ILResult<ColumnarValue> {
        match self {
            Expr::Column(name) => {
                let index = batch.schema().index_of(name)?;
                Ok(ColumnarValue::Array(batch.column(index).clone()))
            }
            Expr::Literal(scalar) => Ok(ColumnarValue::Scalar(scalar.clone())),
            Expr::BinaryExpr(binary_expr) => binary_expr.eval(batch),
            Expr::Not(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => {
                    let array = array.as_boolean_opt().ok_or_else(|| {
                        ILError::InternalError(format!(
                            "Expected boolean array, got {}",
                            array.data_type()
                        ))
                    })?;
                    Ok(ColumnarValue::Array(Arc::new(
                        arrow::compute::kernels::boolean::not(array)?,
                    )))
                }
                ColumnarValue::Scalar(scalar) => {
                    let bool_value = scalar.as_bool()?;
                    let not_bool_value = bool_value.map(|b| !b);
                    Ok(ColumnarValue::Scalar(Scalar::Boolean(not_bool_value)))
                }
            },
            Expr::IsNull(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::is_null(&array)?,
                ))),
                ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(Scalar::Boolean(Some(
                    scalar.is_null(),
                )))),
            },
            Expr::IsNotNull(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::is_not_null(&array)?,
                ))),
                ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(Scalar::Boolean(Some(
                    !scalar.is_null(),
                )))),
            },
            _ => todo!(),
        }
    }

    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        match self {
            Expr::Column(name) => database.sql_identifier(name),
            Expr::Literal(scalar) => scalar.to_sql(database),
            Expr::BinaryExpr(binary_expr) => binary_expr.to_sql(database),
            Expr::Not(expr) => format!("NOT {}", expr.to_sql(database)),
            Expr::IsNull(expr) => format!("{} IS NULL", expr.to_sql(database)),
            Expr::IsNotNull(expr) => format!("{} IS NOT NULL", expr.to_sql(database)),
            Expr::IsTrue(expr) => format!("{} IS TRUE", expr.to_sql(database)),
            Expr::IsFalse(expr) => format!("{} IS FALSE", expr.to_sql(database)),
            Expr::IsNotTrue(expr) => format!("{} IS NOT TRUE", expr.to_sql(database)),
            Expr::IsNotFalse(expr) => format!("{} IS NOT FALSE", expr.to_sql(database)),
            Expr::InList(in_list) => {
                let list = in_list
                    .list
                    .iter()
                    .map(|expr| expr.to_sql(database))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", in_list.expr.to_sql(database), list)
            }
        }
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{}", name),
            Expr::Literal(scalar) => write!(f, "{}", scalar),
            Expr::BinaryExpr(binary_expr) => write!(f, "{}", binary_expr),
            Expr::Not(expr) => write!(f, "NOT {}", expr),
            Expr::IsNull(expr) => write!(f, "{} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", expr),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", expr),
            Expr::IsNotTrue(expr) => write!(f, "{} IS NOT TRUE", expr),
            Expr::IsNotFalse(expr) => write!(f, "{} IS NOT FALSE", expr),
            Expr::InList(in_list) => write!(
                f,
                "{} IN ({})",
                in_list.expr,
                in_list
                    .list
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// InList expression
#[derive(Debug, Clone, Drive, DriveMut)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(Scalar),
}

impl ColumnarValue {
    pub fn into_array(self, num_rows: usize) -> ILResult<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }
}
