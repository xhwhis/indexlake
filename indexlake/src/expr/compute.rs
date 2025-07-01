use arrow::{
    array::{BooleanArray, Datum, make_comparator},
    buffer::NullBuffer,
    compute::SortOptions,
};

use crate::{ILError, ILResult, expr::BinaryOp};

/// Compare with eq with either nested or non-nested
pub fn compare_with_eq(
    lhs: &dyn Datum,
    rhs: &dyn Datum,
    is_nested: bool,
) -> ILResult<BooleanArray> {
    if is_nested {
        compare_op_for_nested(BinaryOp::Eq, lhs, rhs)
    } else {
        Ok(arrow::compute::kernels::cmp::eq(lhs, rhs)?)
    }
}

/// Compare on nested type List, Struct, and so on
pub fn compare_op_for_nested(
    op: BinaryOp,
    lhs: &dyn Datum,
    rhs: &dyn Datum,
) -> ILResult<BooleanArray> {
    let (l, is_l_scalar) = lhs.get();
    let (r, is_r_scalar) = rhs.get();
    let l_len = l.len();
    let r_len = r.len();

    if l_len != r_len && !is_l_scalar && !is_r_scalar {
        return Err(ILError::InternalError("len mismatch".to_string()));
    }

    let len = match is_l_scalar {
        true => r_len,
        false => l_len,
    };

    // fast path, if compare with one null, then we can return null array directly
    if is_l_scalar && l.null_count() == 1 || is_r_scalar && r.null_count() == 1 {
        return Ok(BooleanArray::new_null(len));
    }

    // TODO: make SortOptions configurable
    // we choose the default behaviour from arrow-rs which has null-first that follow spark's behaviour
    let cmp = make_comparator(l, r, SortOptions::default())?;

    let cmp_with_op = |i, j| match op {
        BinaryOp::Eq => cmp(i, j).is_eq(),
        BinaryOp::Lt => cmp(i, j).is_lt(),
        BinaryOp::Gt => cmp(i, j).is_gt(),
        BinaryOp::LtEq => !cmp(i, j).is_gt(),
        BinaryOp::GtEq => !cmp(i, j).is_lt(),
        BinaryOp::NotEq => !cmp(i, j).is_eq(),
        _ => unreachable!("unexpected operator found"),
    };

    let values = match (is_l_scalar, is_r_scalar) {
        (false, false) => (0..len).map(|i| cmp_with_op(i, i)).collect(),
        (true, false) => (0..len).map(|i| cmp_with_op(0, i)).collect(),
        (false, true) => (0..len).map(|i| cmp_with_op(i, 0)).collect(),
        (true, true) => std::iter::once(cmp_with_op(0, 0)).collect(),
    };

    // If one of the side is NULL, we returns NULL
    // i.e. NULL eq NULL -> NULL
    let nulls = NullBuffer::union(l.nulls(), r.nulls());
    Ok(BooleanArray::new(values, nulls))
}
