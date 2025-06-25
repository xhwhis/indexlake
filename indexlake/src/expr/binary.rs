use derive_visitor::{Drive, DriveMut};

use crate::{
    ILError, ILResult,
    catalog::CatalogDatabase,
    expr::Expr,
    record::{Row, Scalar},
};

#[derive(Debug, Clone, Drive, DriveMut)]
pub enum BinaryOp {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::Plus => write!(f, "+"),
            BinaryOp::Minus => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulo => write!(f, "%"),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
        }
    }
}

/// Binary expression
#[derive(Debug, Clone, Drive, DriveMut)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: BinaryOp,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn eval(&self, row: &Row) -> ILResult<Scalar> {
        let left = self.left.eval(row)?;
        let right = self.right.eval(row)?;
        match self.op {
            BinaryOp::Eq => Ok(Scalar::Boolean(Some(left == right))),
            BinaryOp::NotEq => Ok(Scalar::Boolean(Some(left != right))),
            BinaryOp::Lt => Ok(Scalar::Boolean(Some(left < right))),
            BinaryOp::LtEq => Ok(Scalar::Boolean(Some(left <= right))),
            BinaryOp::Gt => Ok(Scalar::Boolean(Some(left > right))),
            BinaryOp::GtEq => Ok(Scalar::Boolean(Some(left >= right))),
            BinaryOp::Plus => left.add(&right),
            BinaryOp::Minus => left.sub(&right),
            BinaryOp::Multiply => left.mul(&right),
            BinaryOp::Divide => left.div(&right),
            BinaryOp::Modulo => left.rem(&right),
            BinaryOp::And => match (&left, &right) {
                (Scalar::Boolean(Some(v1)), Scalar::Boolean(Some(v2))) => {
                    Ok(Scalar::Boolean(Some(*v1 && *v2)))
                }
                (Scalar::Boolean(None), _) | (_, Scalar::Boolean(None)) => {
                    Ok(Scalar::Boolean(None))
                }
                _ => Err(ILError::InvalidInput(format!(
                    "Cannot AND scalars: {:?} and {:?}",
                    left, right
                ))),
            },
            BinaryOp::Or => match (&left, &right) {
                (Scalar::Boolean(Some(v1)), Scalar::Boolean(Some(v2))) => {
                    Ok(Scalar::Boolean(Some(*v1 || *v2)))
                }
                (Scalar::Boolean(None), _) | (_, Scalar::Boolean(None)) => {
                    Ok(Scalar::Boolean(None))
                }
                _ => Err(ILError::InvalidInput(format!(
                    "Cannot OR scalars: {:?} and {:?}",
                    left, right
                ))),
            },
        }
    }

    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> String {
        let left_sql = self.left.to_sql(database);
        let right_sql = self.right.to_sql(database);
        format!("({} {} {})", left_sql, self.op, right_sql)
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}
