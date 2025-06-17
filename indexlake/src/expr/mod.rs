mod binary;

pub use binary::*;

use crate::{
    ILError, ILResult,
    record::{Row, Scalar},
};

/// Represents logical expressions such as `A + 1`
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
    pub fn eval(&self, row: &Row) -> ILResult<Scalar> {
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
                    Scalar::Boolean(Some(value)) => Ok(Scalar::Boolean(Some(!value))),
                    _ => Err(ILError::InvalidInput(
                        "Not expression must be a boolean".to_string(),
                    )),
                }
            }
            Expr::IsNull(expr) => {
                let scalar = expr.eval(row)?;
                Ok(Scalar::Boolean(Some(scalar.is_null())))
            }
            Expr::IsNotNull(expr) => {
                let scalar = expr.eval(row)?;
                Ok(Scalar::Boolean(Some(!scalar.is_null())))
            }
            Expr::IsTrue(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    Scalar::Boolean(Some(value)) => Ok(Scalar::Boolean(Some(value))),
                    _ => Ok(Scalar::Boolean(Some(false))),
                }
            }
            Expr::IsFalse(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    Scalar::Boolean(Some(value)) => Ok(Scalar::Boolean(Some(!value))),
                    _ => Ok(Scalar::Boolean(Some(false))),
                }
            }
            Expr::IsNotTrue(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    Scalar::Boolean(Some(value)) => Ok(Scalar::Boolean(Some(!value))),
                    _ => {
                        if scalar.is_null() {
                            Ok(Scalar::Boolean(Some(true)))
                        } else {
                            Ok(Scalar::Boolean(Some(false)))
                        }
                    }
                }
            }
            Expr::IsNotFalse(expr) => {
                let scalar = expr.eval(row)?;
                match scalar {
                    Scalar::Boolean(Some(value)) => Ok(Scalar::Boolean(Some(value))),
                    _ => {
                        if scalar.is_null() {
                            Ok(Scalar::Boolean(Some(true)))
                        } else {
                            Ok(Scalar::Boolean(Some(false)))
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
                    .collect::<ILResult<Vec<Scalar>>>()?;
                Ok(Scalar::Boolean(Some(list.contains(&scalar))))
            }
        }
    }
}

/// InList expression
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}
