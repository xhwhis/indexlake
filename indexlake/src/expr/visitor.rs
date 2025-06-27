use derive_visitor::{Drive, Visitor};

use crate::expr::Expr;

#[derive(Debug, Default, Visitor)]
#[visitor(Expr(enter))]
pub struct ColumnRecorder {
    columns: Vec<String>,
}

impl ColumnRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn enter_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(name) => self.columns.push(name.clone()),
            _ => {}
        }
    }
}

pub fn visited_columns(expr: &Expr) -> Vec<String> {
    let mut recorder = ColumnRecorder::new();
    expr.drive(&mut recorder);
    recorder.columns
}

#[cfg(test)]
mod tests {
    use crate::catalog::CatalogScalar;

    use super::*;

    #[test]
    fn test_visited_columns() {
        let col_a = Expr::Column("a".to_string());
        let col_b = Expr::Column("b".to_string());
        let scalar_1 = Expr::Literal(CatalogScalar::Int32(Some(1)));
        let expr = col_a.eq(col_b.plus(scalar_1));
        let columns = visited_columns(&expr);
        assert_eq!(columns, vec!["a", "b"]);
    }
}
