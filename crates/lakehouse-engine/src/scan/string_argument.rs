//! Exasol rounds a string function's length, count, start, or code to an integer and refuses one
//! outside ±999,999,999. DataFusion's string functions take only `Int64` there, and check it when
//! the SQL is planned, so the scan session replaces them with wrappers that convert the argument.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::{DataType, Decimal128Type, Field, Float64Type, Int64Type};
use arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

use super::exasol_numeric::numeric_overflow;
use super::raised_failure::RaisedFailure;

const LIMIT: i128 = 999_999_999;

/// A value the target type cannot hold fails the cast instead of becoming NULL.
const STRICT: CastOptions = CastOptions {
    safe: false,
    format_options: arrow::util::display::FormatOptions::new(),
};

/// What Exasol does with a negative argument, after rounding.
#[derive(Clone, Copy, Debug)]
enum Negative {
    /// The function returns NULL.
    Null,
    /// The query fails, naming the function.
    Refused(&'static str),
    /// The wrapped function's own handling. A negative `SUBSTR` start differs: Exasol counts it
    /// from the end, DataFusion from before the first character.
    Passed,
}

use Negative::{Null, Passed, Refused};

/// Adds the wrapped functions to `builder` after its defaults, so they replace DataFusion's own,
/// and plans `SUBSTR`, which the SQL parser reads as `SUBSTRING`, ahead of DataFusion's planner.
pub(crate) fn install(builder: &mut SessionStateBuilder, raised: &Arc<RaisedFailure>) {
    use datafusion::functions::{string, unicode};
    let wrap = |inner: Arc<ScalarUDF>, positions| {
        Arc::new(ScalarUDF::new_from_impl(ExasolStringFunction {
            signature: Signature::user_defined(inner.signature().volatility),
            inner,
            positions,
            raised: raised.clone(),
        }))
    };
    let substr = wrap(unicode::substr(), &[(1, Passed), (2, Null)]);
    builder
        .scalar_functions()
        .get_or_insert_with(Vec::new)
        .extend([
            wrap(unicode::lpad(), &[(1, Null)]),
            wrap(unicode::rpad(), &[(1, Null)]),
            wrap(string::repeat(), &[(1, Refused("REPEAT"))]),
            wrap(unicode::left(), &[(1, Null)]),
            wrap(unicode::right(), &[(1, Null)]),
            wrap(string::chr(), &[(0, Passed)]),
            substr.clone(),
        ]);
    builder
        .expr_planners()
        .get_or_insert_with(Vec::new)
        .insert(0, Arc::new(SubstringPlanner(substr)));
}

#[derive(Debug)]
struct SubstringPlanner(Arc<ScalarUDF>);

impl ExprPlanner for SubstringPlanner {
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(self.0.clone(), args),
        )))
    }
}

#[derive(Debug)]
struct ExasolStringFunction {
    inner: Arc<ScalarUDF>,
    positions: &'static [(usize, Negative)],
    signature: Signature,
    raised: Arc<RaisedFailure>,
}

impl ExasolStringFunction {
    /// `types` as the wrapped function sees them: every converted argument is `Int64`.
    fn inner_types(&self, types: &[DataType]) -> Vec<DataType> {
        let mut types = types.to_vec();
        for &(i, _) in self.positions {
            if let Some(t) = types.get_mut(i)
                && converts(t)
            {
                *t = DataType::Int64;
            }
        }
        types
    }

    fn convert(
        &self,
        value: &ColumnarValue,
        rows: usize,
        negative: Negative,
    ) -> Result<ColumnarValue> {
        let converted = to_int64(value.to_array(rows)?.as_ref()).map_err(|e| match e {
            ArrowError::ArithmeticOverflow(detail) => self.raise(numeric_overflow(detail)),
            other => other.into(),
        })?;
        let converted = match negative {
            Null => converted.iter().map(|v| v.filter(|v| *v >= 0)).collect(),
            Refused(function) if converted.iter().flatten().any(|v| v < 0) => {
                return Err(self.raise(format!(
                    "data exception - negative numbers not allowed in function {function}"
                )));
            }
            Refused(_) | Passed => converted,
        };
        Ok(ColumnarValue::Array(Arc::new(converted) as ArrayRef))
    }

    fn raise(&self, failure: String) -> DataFusionError {
        self.raised.record(failure.clone());
        DataFusionError::Execution(failure)
    }
}

/// A text argument is left to the wrapped function's coercion: the typed scan pass declines it.
fn converts(t: &DataType) -> bool {
    match t {
        DataType::Dictionary(_, value) => converts(value),
        t => t.is_numeric() || *t == DataType::Null,
    }
}

fn to_int64(array: &dyn Array) -> Result<Int64Array, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, value) => to_int64(cast(array, value)?.as_ref()),
        DataType::Null => Ok(Int64Array::new_null(array.len())),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            cast(array, &DataType::Float64)?
                .as_primitive::<Float64Type>()
                .try_unary::<_, Int64Type, _>(|v| in_range(v.round(), || v.to_string()))
        }
        t if t.is_integer() => cast_with_options(array, &DataType::Int64, &STRICT)
            .map_err(|_| out_of_range(&format!("of type {t}")))?
            .as_primitive::<Int64Type>()
            .try_unary::<_, Int64Type, _>(|v| in_range(v as f64, || v.to_string())),
        decimal @ (DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s)) => {
            let scale = (*s).max(0);
            let exact = cast_with_options(array, &DataType::Decimal128(38, scale), &STRICT)
                .map_err(|_| out_of_range(&format!("of type {decimal}")))?;
            let unit = 10i128.pow(scale as u32);
            exact
                .as_primitive::<Decimal128Type>()
                .try_unary::<_, Int64Type, _>(|v| {
                    let rounded = round_half_away(v, unit);
                    in_range(rounded as f64, || rounded.to_string())
                })
        }
        other => Err(ArrowError::InvalidArgumentError(format!(
            "a string function's numeric argument cannot be of type {other}"
        ))),
    }
}

fn round_half_away(v: i128, unit: i128) -> i128 {
    let (q, r) = (v / unit, v % unit);
    if 2 * r.abs() >= unit {
        q + v.signum()
    } else {
        q
    }
}

/// `value` is integral; `shown` renders it for the message only when it is out of range.
fn in_range(value: f64, shown: impl Fn() -> String) -> Result<i64, ArrowError> {
    if value.abs() <= LIMIT as f64 {
        Ok(value as i64)
    } else {
        Err(out_of_range(&shown()))
    }
}

fn out_of_range(value: &str) -> ArrowError {
    ArrowError::ArithmeticOverflow(format!("value {value} is not in [ -{LIMIT} .. {LIMIT} ]"))
}

impl PartialEq for ExasolStringFunction {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for ExasolStringFunction {}

impl Hash for ExasolStringFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name().hash(state);
    }
}

impl ScalarUDFImpl for ExasolStringFunction {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(&self.inner_types(arg_types))
    }

    /// The wrapped function coerces every argument but a converted one, which keeps its type.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let fields: Vec<_> = self
            .inner_types(arg_types)
            .into_iter()
            .map(|t| Arc::new(Field::new("arg", t, true)))
            .collect();
        let mut coerced: Vec<DataType> = fields_with_udf(&fields, self.inner.as_ref())?
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        for &(i, _) in self.positions {
            if let (Some(t), Some(slot)) = (arg_types.get(i), coerced.get_mut(i))
                && converts(t)
            {
                *slot = t.clone();
            }
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        for &(i, negative) in self.positions {
            if let Some(value) = args.args.get(i) {
                args.args[i] = self.convert(value, args.number_rows, negative)?;
                args.arg_fields[i] = Arc::new(
                    args.arg_fields[i]
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Int64),
                );
            }
        }
        self.inner.invoke_with_args(args)
    }
}

#[cfg(test)]
#[path = "string_argument_tests.rs"]
mod tests;
