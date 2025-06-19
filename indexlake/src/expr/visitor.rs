use derive_visitor::{Drive, DriveMut, Visitor, VisitorMut};

use crate::{catalog::INLINE_COLUMN_NAME_PREFIX, expr::Expr, record::SchemaRef};

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

// TODO wait for derive-visitor to support falliable visitor
#[derive(Debug, Default, VisitorMut)]
#[visitor(Expr(enter))]
pub struct InlineColumnNameRewriter {
    table_schema: SchemaRef,
}

impl InlineColumnNameRewriter {
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }

    pub fn enter_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Column(name) => {
                let field = self.table_schema.get_field_by_name(name).unwrap();
                let inline_column_name = field.inline_field_name().unwrap();
                *name = inline_column_name;
            }
            _ => {}
        }
    }
}

pub fn rewrite_inline_column_names(expr: &mut Expr, table_schema: SchemaRef) {
    let mut rewriter = InlineColumnNameRewriter::new(table_schema);
    expr.drive_mut(&mut rewriter);
}

#[cfg(test)]
mod tests {
    use crate::record::Scalar;

    use super::*;

    #[test]
    fn test_visited_columns() {
        let col_a = Expr::Column("a".to_string());
        let col_b = Expr::Column("b".to_string());
        let scalar_1 = Expr::Literal(Scalar::Integer(Some(1)));
        let expr = col_a.eq(col_b.plus(scalar_1));
        let columns = visited_columns(&expr);
        assert_eq!(columns, vec!["a", "b"]);
    }
}
