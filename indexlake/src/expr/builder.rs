use crate::expr::{BinaryExpr, BinaryOp, Expr};

impl Expr {
    pub fn eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        })
    }

    pub fn plus(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Plus,
            right: Box::new(other),
        })
    }

    pub fn and(self, other: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        })
    }
}
