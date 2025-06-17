use crate::{
    ILResult,
    expr::Expr,
    record::{Row, Scalar},
};

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

/// Binary expression
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
