//! The scalar function every DataFusion-dialect `FLOAT_DIV` renders as, so division by zero
//! fails the query instead of silently changing a pushed filter's row count (#370). Safe because
//! it sees only operands of a division the pushdown synthesised, unlike `arrow_value_at`'s
//! `is_nan()` guard, which cannot tell a computed non-finite from a stored one.

use arrow::array::{Array, AsArray, Float64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Float64Type};
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use vs_expression::CHECKED_FLOAT_DIV_FN;

/// Arrow keeps the values buffer full-length behind the null mask; nothing reads this slot.
const NULL_ROW_FILLER: f64 = 0.0;

/// Recognised BY TYPE via [`find_checked_float_div_error`], never by message text. `ZeroDivisor`
/// wins whenever the divisor is zero; `NonFiniteResult` covers any other non-finite quotient.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CheckedFloatDivError {
    /// Either sign: IEEE-754 `-0.0` equals `0.0`.
    ZeroDivisor { numerator: f64, divisor: f64 },
    /// An overflow from two finite operands, or a non-finite operand read from the table.
    NonFiniteResult {
        numerator: f64,
        divisor: f64,
        quotient: f64,
    },
}

impl fmt::Display for CheckedFloatDivError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDivisor { numerator, divisor } => write!(
                f,
                "data exception - division by zero: \
                 {CHECKED_FLOAT_DIV_FN}({numerator}, {divisor}) has no finite \
                 result, and Exasol's DOUBLE admits no infinite value"
            ),
            Self::NonFiniteResult {
                numerator,
                divisor,
                quotient,
            } => write!(
                f,
                "data exception - numeric value out of range: \
                 {CHECKED_FLOAT_DIV_FN}({numerator}, {divisor}) evaluates to \
                 {quotient}, and Exasol's DOUBLE admits no non-finite value"
            ),
        }
    }
}

impl Error for CheckedFloatDivError {}

/// DataFusion may wrap the error in `External`, `Context`, `ArrowError::ExternalError`, or an
/// `Arc`; walking `Error::source` recognises it through all of them.
pub(super) fn find_checked_float_div_error(
    error: &DataFusionError,
) -> Option<&CheckedFloatDivError> {
    let mut level: &(dyn Error + 'static) = error;
    loop {
        if let Some(found) = level.downcast_ref::<CheckedFloatDivError>() {
            return Some(found);
        }
        level = level.source()?;
    }
}

/// DataFusion's Parquet row filter flattens any predicate error into
/// `ArrowError::ComputeError(format!("Error evaluating filter predicate: {e:?}"))`
/// (`datafusion-datasource-parquet` 54.1, `row_filter.rs`), destroying the type on exactly the
/// #370 route, so the failure is also recorded here as a typed value. Scoped to one session,
/// i.e. one scan invocation.
#[derive(Debug, Default)]
struct RaisedFailure(OnceLock<CheckedFloatDivError>);

impl RaisedFailure {
    /// Partitions run concurrently but every failure names the same defect, so the first suffices.
    fn record(&self, failure: CheckedFloatDivError) {
        let _ = self.0.set(failure);
    }

    fn recorded(&self) -> Option<CheckedFloatDivError> {
        self.0.get().cloned()
    }
}

/// Reached by type through the session's function registry; see [`RaisedFailure`].
pub(super) fn session_checked_float_div_failure(
    session: &SessionContext,
) -> Option<CheckedFloatDivError> {
    let registered = session.state().udf(CHECKED_FLOAT_DIV_FN).ok()?;
    let implementation: &dyn Any = registered.inner().as_ref();
    implementation
        .downcast_ref::<CheckedFloatDivUdf>()?
        .raised
        .recorded()
}

/// `Signature::any` with a self-cast to `Float64`, because pushed operand types vary (an Iceberg
/// `long` arrives as `Decimal128(20, 0)` against an `Int64` literal). `Immutable` keeps it
/// eligible for the Parquet row filter, where a guard conjunct evaluated first stops a guarded
/// division from raising.
#[derive(Debug)]
struct CheckedFloatDivUdf {
    signature: Signature,
    raised: RaisedFailure,
}

impl CheckedFloatDivUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            raised: RaisedFailure::default(),
        }
    }
}

/// The recorded failure is session state, not identity, so it is excluded from plan comparison.
impl PartialEq for CheckedFloatDivUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for CheckedFloatDivUdf {}

impl Hash for CheckedFloatDivUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for CheckedFloatDivUdf {
    fn name(&self) -> &str {
        CHECKED_FLOAT_DIV_FN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{CHECKED_FLOAT_DIV_FN} takes exactly two arguments, got {}",
                args.args.len()
            )));
        };
        let left = as_float64(left, args.number_rows)?;
        let right = as_float64(right, args.number_rows)?;

        let nulls = NullBuffer::union(left.nulls(), right.nulls());
        let quotients =
            divide_checked(left.values(), right.values(), nulls.as_ref()).map_err(|failure| {
                self.raised.record(failure.clone());
                DataFusionError::External(Box::new(failure))
            })?;

        Ok(ColumnarValue::Array(Arc::new(Float64Array::new(
            quotients.into(),
            nulls,
        ))))
    }
}

/// Exasol's `FN_FLOAT_DIV` is always DOUBLE division; widening first avoids DataFusion's
/// truncating `Int64 / Int64` (#186).
fn as_float64(value: &ColumnarValue, rows: usize) -> Result<Float64Array, DataFusionError> {
    let widened = arrow::compute::cast(&value.to_array(rows)?, &DataType::Float64)?;
    Ok(widened.as_primitive::<Float64Type>().clone())
}

/// A NULL row is skipped without dividing, so NULL over zero yields NULL rather than raising.
fn divide_checked(
    left: &[f64],
    right: &[f64],
    nulls: Option<&NullBuffer>,
) -> Result<Vec<f64>, CheckedFloatDivError> {
    let mut quotients = Vec::with_capacity(left.len());
    for (row, (&numerator, &divisor)) in left.iter().zip(right).enumerate() {
        if nulls.is_some_and(|mask| mask.is_null(row)) {
            quotients.push(NULL_ROW_FILLER);
            continue;
        }
        quotients.push(checked_quotient(numerator, divisor)?);
    }
    Ok(quotients)
}

/// Exasol admits no non-finite DOUBLE (rejects `CAST('inf' AS DOUBLE)` at `22018`, `1E400` at
/// `22003`), so no non-finite quotient is ever correct. Checking `divisor == 0.0` after the
/// finiteness test also classifies `-0.0` as zero.
fn checked_quotient(numerator: f64, divisor: f64) -> Result<f64, CheckedFloatDivError> {
    let quotient = numerator / divisor;
    if quotient.is_finite() {
        return Ok(quotient);
    }
    if divisor == 0.0 {
        return Err(CheckedFloatDivError::ZeroDivisor { numerator, divisor });
    }
    Err(CheckedFloatDivError::NonFiniteResult {
        numerator,
        divisor,
        quotient,
    })
}

/// Registered unconditionally once per session: every scan path splices the same rendered
/// expressions, and a spec with no division is unaffected.
pub(super) fn register_checked_float_div_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CheckedFloatDivUdf::new()));
}

#[cfg(test)]
#[path = "checked_div_tests.rs"]
mod tests;
