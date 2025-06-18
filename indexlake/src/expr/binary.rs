use crate::{
    ILResult,
    expr::Expr,
    record::{Row, Scalar},
};

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
            _ => todo!(),
        }
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}
