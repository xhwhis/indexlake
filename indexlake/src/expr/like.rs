use arrow::{array::RecordBatch, datatypes::DataType};
use derive_visitor::{Drive, DriveMut};

use crate::{
    ILResult,
    catalog::CatalogDatabase,
    expr::{ColumnarValue, Expr, apply_cmp},
};

#[derive(Debug, Clone, Drive, DriveMut, PartialEq, Eq)]
pub struct Like {
    negated: bool,
    case_insensitive: bool,
    expr: Box<Expr>,
    pattern: Box<Expr>,
}

impl Like {
    pub fn new(negated: bool, expr: Box<Expr>, pattern: Box<Expr>, case_insensitive: bool) -> Self {
        Self {
            negated,
            case_insensitive,
            expr,
            pattern,
        }
    }
    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> ILResult<String> {
        let expr = self.expr.to_sql(database.clone())?;
        let pattern = self.pattern.to_sql(database)?;
        match database {
            CatalogDatabase::Postgres => match (self.negated, self.case_insensitive) {
                (true, true) => Ok(format!("{} NOT ILIKE {}", expr, pattern)),
                (true, false) => Ok(format!("{} NOT LIKE {}", expr, pattern)),
                (false, true) => Ok(format!("{} ILIKE {}", expr, pattern)),
                (false, false) => Ok(format!("{} LIKE {}", expr, pattern)),
            },
            CatalogDatabase::Sqlite => {
                // For case-sensitive LIKE, SQLite requires `PRAGMA case_sensitive_like = ON;`
                // to be set on the connection. This function only generates the SQL string
                // and does not set the PRAGMA.
                // For case-insensitive ILIKE, we use the `UPPER()` function on both
                // the expression and the pattern to ensure case-insensitivity.
                match (self.negated, self.case_insensitive) {
                    (false, false) => Ok(format!("{} LIKE {}", expr, pattern)),
                    (true, false) => Ok(format!("{} NOT LIKE {}", expr, pattern)),
                    (false, true) => Ok(format!("UPPER({}) LIKE UPPER({})", expr, pattern)),
                    (true, true) => Ok(format!("UPPER({}) NOT LIKE UPPER({})", expr, pattern)),
                }
            }
        }
    }

    pub(crate) fn eval(&self, batch: &RecordBatch) -> ILResult<ColumnarValue> {
        use arrow::compute::*;
        let lhs = self.expr.eval(batch)?;
        let rhs = self.pattern.eval(batch)?;
        match (self.negated, self.case_insensitive) {
            (false, false) => apply_cmp(&lhs, &rhs, like),
            (false, true) => apply_cmp(&lhs, &rhs, ilike),
            (true, false) => apply_cmp(&lhs, &rhs, nlike),
            (true, true) => apply_cmp(&lhs, &rhs, nilike),
        }
    }

    #[allow(unused)]
    pub fn data_type(&self) -> ILResult<DataType> {
        Ok(DataType::Boolean)
    }

    /// Operator name
    fn op_name(&self) -> &str {
        match (self.negated, self.case_insensitive) {
            (false, false) => "LIKE",
            (true, false) => "NOT LIKE",
            (false, true) => "ILIKE",
            (true, true) => "NOT ILIKE",
        }
    }
}

impl std::fmt::Display for Like {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.op_name(), self.pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{col, lit};
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8, false)]);
        let c1 = StringArray::from(vec!["hello", "world", "HELLO", "WORLD"]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1)]).unwrap()
    }

    #[test]
    fn test_like_eval() {
        let batch = create_batch();
        let num_rows = batch.num_rows();
        let expr = col("c1");

        // === LIKE (case-sensitive) ===
        // Test LIKE: c1 LIKE 'h%' -> ['hello'] -> [true, false, false, false]
        let pattern_h = lit("h%");
        let like_expr_h = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_h.clone()),
            false,
        );
        let result = like_expr_h
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![true, false, false, false])
        );

        // Test NOT LIKE: c1 NOT LIKE 'h%' -> ['world', 'HELLO', 'WORLD'] -> [false, true, true, true]
        let not_like_expr_h = Like::new(
            true,
            Box::new(expr.clone()),
            Box::new(pattern_h.clone()),
            false,
        );
        let result = not_like_expr_h
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, true, true])
        );

        // === ILIKE (case-insensitive) ===
        // Test ILIKE: c1 ILIKE 'h%' -> ['hello', 'HELLO'] -> [true, false, true, false]
        let ilike_expr_h = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_h.clone()),
            true,
        );
        let result = ilike_expr_h
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![true, false, true, false])
        );

        // Test NOT ILIKE: c1 NOT ILIKE 'h%' -> ['world', 'WORLD'] -> [false, true, false, true]
        let not_ilike_expr_h = Like::new(
            true,
            Box::new(expr.clone()),
            Box::new(pattern_h.clone()),
            true,
        );
        let result = not_ilike_expr_h
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, false, true])
        );

        // === More wildcards ===
        // Test with wildcard '%' at the start: c1 LIKE '%d' -> ['world'] -> [false, true, false, false]
        let pattern_d = lit("%d");
        let like_expr_d = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_d.clone()),
            false,
        );
        let result = like_expr_d
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, false, false])
        );

        // Test with wildcard '%' at the start (case-insensitive): c1 ILIKE '%d' -> ['world', 'WORLD'] -> [false, true, false, true]
        let ilike_expr_d = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_d.clone()),
            true,
        );
        let result = ilike_expr_d
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, false, true])
        );

        // Test with wildcard '_': c1 LIKE 'w_rld' -> ['world'] -> [false, true, false, false]
        let pattern_w = lit("w_rld");
        let like_expr_w = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_w.clone()),
            false,
        );
        let result = like_expr_w
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, false, false])
        );

        // Test with wildcard '_' (case-insensitive): c1 ILIKE 'W_RLD' -> ['world', 'WORLD'] -> [false, true, false, true]
        let pattern_w = lit("W_RLD");
        let ilike_expr_w = Like::new(
            false,
            Box::new(expr.clone()),
            Box::new(pattern_w.clone()),
            true,
        );
        let result = ilike_expr_w
            .eval(&batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result_bool,
            &BooleanArray::from(vec![false, true, false, true])
        );
    }
}
