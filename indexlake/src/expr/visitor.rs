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
    use crate::expr::{col, lit};

    use super::*;

    #[test]
    fn test_visited_columns() {
        let expr = col("a").eq(col("b").plus(lit(1)));
        let columns = visited_columns(&expr);
        assert_eq!(columns, vec!["a", "b"]);
    }
}
