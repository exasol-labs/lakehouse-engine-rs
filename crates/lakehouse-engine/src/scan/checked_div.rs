//! Checked float division: the scalar function every DataFusion-dialect
//! `FLOAT_DIV` node renders as, so a division by zero fails the query at the
//! point of division instead of silently changing a pushed filter's row count
//! (#370).
//!
//! `crates/vs-expression` names the function and this module implements it; the
//! only thing tying the two together is [`CHECKED_FLOAT_DIV_FN`], whose doc
//! comment states the contract below. The check is safe precisely because it
//! sees only the two operands of a division the pushdown itself synthesised —
//! unlike `convert::arrow_value_at`'s `is_nan()` guard, which sees a value read
//! from a column and cannot tell a computed non-finite from a stored one.

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

/// Value written into a NULL row's slot of the result's values buffer. Arrow
/// keeps that buffer at full length behind the null mask, and nothing reads a
/// slot the mask marks absent.
const NULL_ROW_FILLER: f64 = 0.0;

/// A checked division that refused to produce a value Exasol cannot represent.
///
/// Carried on the DataFusion error chain inside `DataFusionError::External`, so
/// [`crate::scan::emit::classify_scan_error`] recognises the failure BY TYPE
/// through [`find_checked_float_div_error`] rather than by matching text in a
/// message. The two variants stay distinct because a support case needs to tell
/// a zero divisor from an overflow. [`checked_quotient`] classifies on the
/// divisor, so `ZeroDivisor` is raised whenever the divisor is zero, including
/// when the numerator is a non-finite value read from the source table.
/// `NonFiniteResult` covers a non-finite result under a NON-ZERO divisor only:
/// an overflow between two finite operands, or a non-finite operand read from
/// the source table and divided by something other than zero.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CheckedFloatDivError {
    /// The divisor was zero, of either sign — IEEE-754 `-0.0` equals `0.0`.
    ZeroDivisor { numerator: f64, divisor: f64 },
    /// The quotient was `±Inf` or `NaN` for some other reason: an overflow from
    /// two finite operands, or a non-finite operand read from the source table.
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

/// Find a checked-division failure anywhere on `error`'s source chain.
///
/// The chain's shape is this module's secret, not the caller's: DataFusion
/// wraps a scalar function's error in `External`, and the layers above may wrap
/// that again in `Context`, in `ArrowError::ExternalError`, or in an `Arc`.
/// Walking `Error::source` and downcasting at every level recognises the failure
/// through all of them, so no caller ever has to match message text — the
/// coupling that would break on any wording change.
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

/// The first checked-division failure of one scan session.
///
/// DataFusion's Parquet row filter flattens ANY predicate error into
/// `ArrowError::ComputeError(format!("Error evaluating filter predicate:
/// {e:?}"))` (`datafusion-datasource-parquet` 54.1, `row_filter.rs`), which
/// destroys the error's type — and a filter predicate is issue #370's own
/// route. So for the very case the checked division exists to fix, the value
/// cannot reach the classifier along the error chain, and recovering it from the
/// flattened text would be exactly the message-matching coupling the spec
/// forbids. The failure is recorded here as a typed value instead.
///
/// Scoped to the session that registered the function, which is one scan
/// invocation, so no failure can survive into another. A recorded failure means
/// the division DID raise during this scan, so naming it is never a false
/// report even when another partition's error happened to surface first.
#[derive(Debug, Default)]
struct RaisedFailure(OnceLock<CheckedFloatDivError>);

impl RaisedFailure {
    /// Keep the FIRST failure. DataFusion evaluates partitions concurrently and
    /// only one of their errors reaches the caller, but every checked-division
    /// failure of one query names the same defect in the same query.
    fn record(&self, failure: CheckedFloatDivError) {
        let _ = self.0.set(failure);
    }

    fn recorded(&self) -> Option<CheckedFloatDivError> {
        self.0.get().cloned()
    }
}

/// The checked-division failure `session` recorded, if its checked division
/// raised during this scan.
///
/// The lookup goes through the session's own function registry, so the recorded
/// value is reached BY TYPE — never by matching text in a message — even on the
/// route that flattened the error. See [`RaisedFailure`] for why that route
/// exists.
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

/// [`ScalarUDFImpl`] for [`CHECKED_FLOAT_DIV_FN`]: divides two operands as
/// `DOUBLE` and raises rather than returning a value Exasol cannot represent.
///
/// Accepts any two argument types (`Signature::any`) and casts both to
/// `Float64` itself, because the pairing a pushed `FLOAT_DIV` really sees varies
/// with the source table's column types — an Iceberg `long` arrives as
/// `Decimal128(20, 0)` on one side and an `Int64` literal on the other.
/// Declared `Immutable` so DataFusion treats two evaluations over equal input as
/// equal, which keeps the expression eligible for the same plan-level handling
/// any other scalar expression receives — including the Parquet row filter,
/// where a guard conjunct evaluated ahead of the division is what keeps a
/// guarded division from raising.
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

/// Identity is the function's signature alone. Two registrations of this
/// function are the same function to DataFusion's plan comparison; the recorded
/// failure is one session's state, not part of what the function IS.
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

/// One operand widened to a `Float64` column of `rows` rows.
///
/// Reproduces Exasol's `FN_FLOAT_DIV`, which is always true float division
/// typed `DOUBLE`: widening before dividing is what stops an `Int64 / Int64`
/// pairing truncating the way DataFusion's own `/` operator does (#186). A
/// scalar argument expands to the batch's row count here, so the division below
/// sees two equal-length columns whichever side was a literal.
fn as_float64(value: &ColumnarValue, rows: usize) -> Result<Float64Array, DataFusionError> {
    let widened = arrow::compute::cast(&value.to_array(rows)?, &DataType::Float64)?;
    Ok(widened.as_primitive::<Float64Type>().clone())
}

/// Divide one batch, raising on the first row whose quotient is not finite.
///
/// A row `nulls` marks absent is skipped without dividing, so a NULL numerator
/// over a zero divisor yields NULL rather than raising: a NULL row carries no
/// value to divide. Both slices come from arrays of the batch's row count, the
/// contract DataFusion holds for every argument of a scalar function.
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

/// One row's quotient, or the reason it has no representable value.
///
/// Exasol admits no non-finite `DOUBLE` — it rejects `CAST('inf' AS DOUBLE)` at
/// `22018` and `1E400` at `22003` — so no non-finite quotient is ever a correct
/// answer, whether it came from a zero divisor or from an overflow between two
/// finite operands. `divisor == 0.0` classifies the cause AFTER the finiteness
/// test rather than before it, which is also what makes `-0.0` a division by
/// zero: IEEE-754 compares it equal to `0.0`.
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

/// Register [`CHECKED_FLOAT_DIV_FN`] on `ctx` so rendered SQL text can call it.
///
/// Called ONCE per session, from
/// [`build_session_context`](crate::scan::object_store::build_session_context),
/// unconditionally and without inspecting whether the spec's SQL contains a
/// division: the raw-row path, the broadcast-join path, and both
/// partial-aggregate paths splice the same rendered filter, projection,
/// `ORDER BY`, `GROUP BY`, and aggregate-argument strings, so one registration
/// there reaches every pushed expression the scan can evaluate. A spec with no
/// division is unaffected — the registration adds one entry to the session's
/// function registry and changes no generated SQL, plan shape, or result.
pub(super) fn register_checked_float_div_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(CheckedFloatDivUdf::new()));
}

#[cfg(test)]
#[path = "checked_div_tests.rs"]
mod tests;
