use crate::expr::{BinaryExpr, BinaryOp, Expr};

pub fn split_binary_expr(expr: Expr, operator: BinaryOp) -> Vec<Expr> {
    split_binary_expr_impl(expr, operator, Vec::new())
}

fn split_binary_expr_impl(expr: Expr, operator: BinaryOp, mut exprs: Vec<Expr>) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if op == operator => {
            let exprs = split_binary_expr_impl(*left, operator, exprs);
            split_binary_expr_impl(*right, operator, exprs)
        }
        other => {
            exprs.push(other);
            exprs
        }
    }
}

pub fn split_conjunction_filters(filters: Vec<Expr>) -> Vec<Expr> {
    let mut result = Vec::new();
    for filter in filters {
        result.extend(split_binary_expr(filter, BinaryOp::And));
    }
    result
}

pub fn merge_filters(mut filters: Vec<Expr>) -> Option<Expr> {
    if filters.is_empty() {
        return None;
    }

    let first_filter = filters.remove(0);
    let expr = filters.into_iter().fold(first_filter, |acc, expr| {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(acc),
            right: Box::new(expr),
            op: BinaryOp::And,
        })
    });
    Some(expr)
}

#[cfg(test)]
mod tests {
    use crate::expr::{col, lit};

    use super::*;

    #[test]
    fn test_split_binary_expr() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").is_null();
        let expr3 = col("c");
        let expr = expr1.clone().and(expr2.clone()).and(expr3.clone());
        let exprs = split_binary_expr(expr, BinaryOp::And);
        assert_eq!(exprs.len(), 3);
        assert_eq!(exprs[0], expr1);
        assert_eq!(exprs[1], expr2);
        assert_eq!(exprs[2], expr3);
    }
}
