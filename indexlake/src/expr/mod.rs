mod binary;
mod visitor;

use arrow::array::{ArrayRef, RecordBatch};
pub use binary::*;
pub use visitor::*;

use derive_visitor::{Drive, DriveMut};

use crate::record::sql_identifier;
use crate::{
    ILError, ILResult,
    catalog::CatalogDatabase,
    record::{CatalogScalar, Row},
};

/// Represents logical expressions such as `A + 1`
#[derive(Debug, Clone, Drive, DriveMut)]
pub enum Expr {
    /// A named reference
    Column(String),
    /// A constant value
    Literal(CatalogScalar),
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
    pub fn eval_arrow(&self, record: &RecordBatch) -> ILResult<ArrayRef> {
        todo!()
    }

    pub fn eval(&self, row: &Row) -> ILResult<CatalogScalar> {
        match self {
            Expr::Column(name) => {
                let index = row
                    .schema
                    .index_of(name)
                    .ok_or_else(|| ILError::InvalidInput(format!("Column {} not found", name)))?;
                Ok(row.values[index].clone())
            }
            Expr::Literal(scalar) => Ok(scalar.clone()),
            Expr::BinaryExpr(binary_expr) => binary_expr.eval(row),
            Expr::Not(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    CatalogScalar::Boolean(Some(value)) => Ok(CatalogScalar::Boolean(Some(!value))),
                    _ => Err(ILError::InvalidInput(
                        "Not expression must be a boolean".to_string(),
                    )),
                }
            }
            Expr::IsNull(expr) => {
                let scalar = expr.eval(row)?;
                Ok(CatalogScalar::Boolean(Some(scalar.is_null())))
            }
            Expr::IsNotNull(expr) => {
                let scalar = expr.eval(row)?;
                Ok(CatalogScalar::Boolean(Some(!scalar.is_null())))
            }
            Expr::IsTrue(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    CatalogScalar::Boolean(Some(value)) => Ok(CatalogScalar::Boolean(Some(value))),
                    _ => Ok(CatalogScalar::Boolean(Some(false))),
                }
            }
            Expr::IsFalse(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    CatalogScalar::Boolean(Some(value)) => Ok(CatalogScalar::Boolean(Some(!value))),
                    _ => Ok(CatalogScalar::Boolean(Some(false))),
                }
            }
            Expr::IsNotTrue(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    CatalogScalar::Boolean(Some(value)) => Ok(CatalogScalar::Boolean(Some(!value))),
                    _ => {
                        if scalar.is_null() {
                            Ok(CatalogScalar::Boolean(Some(true)))
                        } else {
                            Ok(CatalogScalar::Boolean(Some(false)))
                        }
                    }
                }
            }
            Expr::IsNotFalse(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    CatalogScalar::Boolean(Some(value)) => Ok(CatalogScalar::Boolean(Some(value))),
                    _ => {
                        if scalar.is_null() {
                            Ok(CatalogScalar::Boolean(Some(true)))
                        } else {
                            Ok(CatalogScalar::Boolean(Some(false)))
                        }
                    }
                }
            }
            Expr::InList(in_list) => {
                let scalar = in_list.expr.eval(row)?;
                let list = in_list
                    .list
                    .iter()
                    .map(|expr| expr.eval(row))
                    .collect::<ILResult<Vec<CatalogScalar>>>()?;
                Ok(CatalogScalar::Boolean(Some(list.contains(&scalar))))
            }
        }
    }

    pub fn eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        })
    }

    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        match self {
            Expr::Column(name) => sql_identifier(name, database),
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

    pub fn plus(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Plus,
            right: Box::new(other),
        })
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
