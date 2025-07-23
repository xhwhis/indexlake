mod binary;
mod builder;
mod compute;
mod like;
mod utils;
mod visitor;

pub use binary::*;
pub use builder::*;
pub use compute::*;
pub use like::*;
pub use utils::*;
pub use visitor::*;

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, RecordBatch},
    buffer::BooleanBuffer,
    compute::CastOptions,
    datatypes::{DataType, Schema},
    error::ArrowError,
    util::display::{DurationFormat, FormatOptions},
};
use parquet::arrow::{ProjectionMask, arrow_reader::ArrowPredicate};

use derive_visitor::{Drive, DriveMut};

use crate::{
    ILError, ILResult,
    catalog::{CatalogDataType, CatalogDatabase, INTERNAL_ROW_ID_FIELD_NAME, Scalar},
};

pub const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

pub const DEFAULT_FORMAT_OPTIONS: FormatOptions<'static> =
    FormatOptions::new().with_duration_format(DurationFormat::Pretty);

/// Represents logical expressions such as `A + 1`
#[derive(Debug, Clone, Drive, DriveMut, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    /// A named reference
    Column(String),
    /// A constant value
    Literal(Scalar),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// Negation of an expression. The expression's type must be a boolean to make sense
    Not(Box<Expr>),
    /// True if argument is NULL, false otherwise
    IsNull(Box<Expr>),
    /// True if argument is not NULL, false otherwise
    IsNotNull(Box<Expr>),
    /// Returns whether the list contains the expr value
    InList(InList),
    Function(Function),
    Like(Like),
    Cast(Cast),
    Negative(Box<Expr>),
}

impl Expr {
    pub fn eval(&self, batch: &RecordBatch) -> ILResult<ColumnarValue> {
        match self {
            Expr::Column(name) => {
                let index = batch.schema().index_of(name)?;
                Ok(ColumnarValue::Array(batch.column(index).clone()))
            }
            Expr::Literal(scalar) => Ok(ColumnarValue::Scalar(scalar.clone())),
            Expr::BinaryExpr(binary_expr) => binary_expr.eval(batch),
            Expr::Not(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => {
                    let array = array.as_boolean_opt().ok_or_else(|| {
                        ILError::InternalError(format!(
                            "Expected boolean array, got {}",
                            array.data_type()
                        ))
                    })?;
                    Ok(ColumnarValue::Array(Arc::new(
                        arrow::compute::kernels::boolean::not(array)?,
                    )))
                }
                ColumnarValue::Scalar(scalar) => {
                    let bool_value = scalar.as_bool()?;
                    let not_bool_value = bool_value.map(|b| !b);
                    Ok(ColumnarValue::Scalar(Scalar::Boolean(not_bool_value)))
                }
            },
            Expr::IsNull(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::is_null(&array)?,
                ))),
                ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(Scalar::Boolean(Some(
                    scalar.is_null(),
                )))),
            },
            Expr::IsNotNull(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    arrow::compute::is_not_null(&array)?,
                ))),
                ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(Scalar::Boolean(Some(
                    !scalar.is_null(),
                )))),
            },
            Expr::InList(in_list) => {
                let num_rows = batch.num_rows();
                let value = in_list.expr.eval(batch)?.into_array(num_rows)?;
                let is_nested = value.data_type().is_nested();
                let found = in_list.list.iter().map(|expr| expr.eval(batch)).try_fold(
                    BooleanArray::new(BooleanBuffer::new_unset(num_rows), None),
                    |result, expr| -> ILResult<BooleanArray> {
                        let rhs = compare_with_eq(&value, &expr?.into_array(num_rows)?, is_nested)?;
                        Ok(arrow::compute::or_kleene(&result, &rhs)?)
                    },
                )?;

                let r = if in_list.negated {
                    arrow::compute::not(&found)?
                } else {
                    found
                };

                Ok(ColumnarValue::Array(Arc::new(r)))
            }
            Expr::Function(_) => Err(ILError::InvalidInput(
                "Function can only be used for index".to_string(),
            )),
            Expr::Like(like) => like.eval(batch),
            Expr::Cast(cast) => {
                let value = cast.expr.eval(batch)?;
                value.cast_to(&cast.cast_type, None)
            }
            Expr::Negative(expr) => match expr.eval(batch)? {
                ColumnarValue::Array(array) => {
                    let result = arrow::compute::kernels::numeric::neg_wrapping(array.as_ref())?;
                    Ok(ColumnarValue::Array(result))
                }
                ColumnarValue::Scalar(scalar) => {
                    Ok(ColumnarValue::Scalar(scalar.arithmetic_negate()?))
                }
            },
        }
    }

    pub fn check_data_type(&self, schema: &Schema, expected: &DataType) -> ILResult<()> {
        let return_type = self.data_type(schema)?;
        if &return_type != expected {
            return Err(ILError::InternalError(format!(
                "expr should return {expected} instead of {return_type}"
            )));
        }
        Ok(())
    }

    pub fn condition_eval(&self, batch: &RecordBatch) -> ILResult<BooleanArray> {
        self.check_data_type(&batch.schema(), &DataType::Boolean)?;

        let array = self.eval(batch)?.into_array(batch.num_rows())?;
        let bool_array = array.as_boolean_opt().ok_or_else(|| {
            ILError::InternalError(format!(
                "condition should return BooleanArray, but got {}",
                array.data_type()
            ))
        })?;
        Ok(bool_array.clone())
    }

    pub fn as_literal(self) -> ILResult<Scalar> {
        match self {
            Expr::Literal(scalar) => Ok(scalar),
            _ => Err(ILError::InternalError(format!("Expr is not a literal"))),
        }
    }

    pub fn data_type(&self, schema: &Schema) -> ILResult<DataType> {
        match self {
            Expr::Column(name) => {
                let index = schema.index_of(name)?;
                Ok(schema.field(index).data_type().clone())
            }
            Expr::Literal(scalar) => Ok(scalar.data_type()),
            Expr::BinaryExpr(binary_expr) => binary_expr.data_type(schema),
            Expr::Not(_) => Ok(DataType::Boolean),
            Expr::IsNull(_) => Ok(DataType::Boolean),
            Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::InList(_) => Ok(DataType::Boolean),
            Expr::Function(function) => Ok(function.return_type.clone()),
            Expr::Like(_) => Ok(DataType::Boolean),
            Expr::Cast(cast) => Ok(cast.cast_type.clone()),
            Expr::Negative(expr) => expr.data_type(schema),
        }
    }

    pub(crate) fn to_sql(&self, database: CatalogDatabase) -> ILResult<String> {
        match self {
            Expr::Column(name) => Ok(database.sql_identifier(name)),
            Expr::Literal(scalar) => Ok(scalar.to_sql(database)),
            Expr::BinaryExpr(binary_expr) => binary_expr.to_sql(database),
            Expr::Not(expr) => Ok(format!("NOT {}", expr.to_sql(database)?)),
            Expr::IsNull(expr) => Ok(format!("{} IS NULL", expr.to_sql(database)?)),
            Expr::IsNotNull(expr) => Ok(format!("{} IS NOT NULL", expr.to_sql(database)?)),
            Expr::InList(in_list) => {
                let list = in_list
                    .list
                    .iter()
                    .map(|expr| expr.to_sql(database))
                    .collect::<ILResult<Vec<_>>>()?
                    .join(", ");
                Ok(format!("{} IN ({})", in_list.expr.to_sql(database)?, list))
            }
            Expr::Function(_) => Err(ILError::InvalidInput(
                "Function can only be used for index".to_string(),
            )),
            Expr::Like(like) => like.to_sql(database),
            Expr::Cast(cast) => {
                let catalog_datatype = CatalogDataType::from_arrow(&cast.cast_type)?;
                let expr_sql = cast.expr.to_sql(database)?;
                Ok(format!(
                    "CAST({} AS {})",
                    expr_sql,
                    catalog_datatype.to_sql(database)
                ))
            }
            Expr::Negative(expr) => Ok(format!("-{}", expr.to_sql(database)?)),
        }
    }

    pub(crate) fn only_visit_row_id_column(&self) -> bool {
        visited_columns(self) == [INTERNAL_ROW_ID_FIELD_NAME]
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{}", name),
            Expr::Literal(scalar) => write!(f, "{}", scalar),
            Expr::BinaryExpr(binary_expr) => write!(f, "{}", binary_expr),
            Expr::Not(expr) => write!(f, "NOT {}", expr),
            Expr::IsNull(expr) => write!(f, "{} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            Expr::InList(in_list) => write!(
                f,
                "{} IN ({})",
                in_list.expr,
                in_list
                    .list
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expr::Function(function) => write!(
                f,
                "{}({})",
                function.name,
                function
                    .args
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expr::Like(like) => write!(f, "{}", like),
            Expr::Cast(cast) => write!(f, "CAST({} AS {})", cast.expr, cast.cast_type),
            Expr::Negative(expr) => write!(f, "-{}", expr),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExprPredicate {
    filter: Option<Expr>,
    projection: ProjectionMask,
}

impl ExprPredicate {
    pub(crate) fn try_new(filters: Vec<Expr>, projection: ProjectionMask) -> ILResult<Self> {
        let filter = merge_filters(filters);
        Ok(Self { filter, projection })
    }
}

impl ArrowPredicate for ExprPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        if let Some(filter) = &self.filter {
            let array = filter
                .eval(&batch)
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?
                .into_array(batch.num_rows())
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
            let bool_array = array.as_boolean_opt().ok_or_else(|| {
                ArrowError::ComputeError(format!(
                    "ExprPredicate evaluation expected boolean array, got {}",
                    array.data_type()
                ))
            })?;

            Ok(bool_array.clone())
        } else {
            let bool_array = BooleanArray::from(vec![true; batch.num_rows()]);
            Ok(bool_array)
        }
    }
}

/// InList expression
#[derive(Debug, Clone, Drive, DriveMut, PartialEq, Eq, Serialize, Deserialize)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

#[derive(Debug, Clone, Drive, DriveMut, PartialEq, Eq, Serialize, Deserialize)]
pub struct Function {
    pub name: String,
    pub args: Vec<Expr>,
    #[drive(skip)]
    pub return_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut, Serialize, Deserialize)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    #[drive(skip)]
    pub cast_type: DataType,
    // TODO wait https://github.com/apache/arrow-rs/pull/7981
    // #[drive(skip)]
    // pub cast_options: Option<CastOptions<'static>>,
}

#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(Scalar),
}

impl ColumnarValue {
    pub fn into_array(self, num_rows: usize) -> ILResult<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    pub fn cast_to(
        &self,
        cast_type: &DataType,
        cast_options: Option<&CastOptions<'static>>,
    ) -> ILResult<ColumnarValue> {
        let cast_options = cast_options.cloned().unwrap_or(DEFAULT_CAST_OPTIONS);
        match self {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                arrow::compute::kernels::cast::cast_with_options(array, cast_type, &cast_options)?,
            )),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                scalar.cast_to(cast_type, &cast_options)?,
            )),
        }
    }
}
