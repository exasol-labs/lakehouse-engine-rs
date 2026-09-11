use super::*;
use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
};
use arrow::datatypes::{DataType, Field, Float64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;

/// Evaluate the registered checked division over one batch of two operand
/// arrays, returning the `Float64` result column or the raised error.
///
/// Drives the same `invoke_with_args` entry point DataFusion calls per batch,
/// so a test asserts the function's observable output rather than any helper
/// it happens to be built from.
fn divide(left: ArrayRef, right: ArrayRef) -> Result<Float64Array, DataFusionError> {
    let rows = left.len();
    let udf = ScalarUDF::from(CheckedFloatDivUdf::new());
    let args = ScalarFunctionArgs {
        arg_fields: vec![
            Arc::new(Field::new("left", left.data_type().clone(), true)),
            Arc::new(Field::new("right", right.data_type().clone(), true)),
        ],
        args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
        number_rows: rows,
        return_field: Arc::new(Field::new("quotient", DataType::Float64, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let evaluated = udf.invoke_with_args(args)?;
    let array = evaluated.to_array(rows)?;
    Ok(array.as_primitive::<Float64Type>().clone())
}

/// `DECIMAL(20, 0)` — the Exasol type an Iceberg `long` column maps to, and the
/// decimal pairing a pushed `FLOAT_DIV` over two integral columns really sees.
fn decimal(values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(20, 0)
            .expect("DECIMAL(20, 0) is inside Arrow's Decimal128 domain"),
    )
}

fn int64(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn float64(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

/// The error the checked division raised, or a panic naming what came back
/// instead. Every raising test goes through here so a silently-returned value
/// fails as loudly as a wrong error would.
fn raised(left: ArrayRef, right: ArrayRef) -> DataFusionError {
    match divide(left, right) {
        Err(error) => error,
        Ok(values) => {
            panic!("the checked division must raise rather than return a value, got: {values:?}")
        }
    }
}

/// Evaluate the registered checked division over an arbitrary argument count.
///
/// Only a direct `invoke_with_args` call reaches a count other than two:
/// `Signature::any(2, ..)` makes DataFusion reject every other arity at plan
/// time, so the arity guard has no route through planned SQL.
fn invoke_with_argument_count(arguments: Vec<ArrayRef>) -> Result<ColumnarValue, DataFusionError> {
    let rows = arguments.first().map_or(0, |first| first.len());
    let udf = ScalarUDF::from(CheckedFloatDivUdf::new());
    let args = ScalarFunctionArgs {
        arg_fields: arguments
            .iter()
            .enumerate()
            .map(|(position, argument)| {
                Arc::new(Field::new(
                    format!("arg{position}"),
                    argument.data_type().clone(),
                    true,
                ))
            })
            .collect(),
        args: arguments.into_iter().map(ColumnarValue::Array).collect(),
        number_rows: rows,
        return_field: Arc::new(Field::new("quotient", DataType::Float64, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args)
}

/// Every operand pairing Iceberg or Delta can present divides as `DOUBLE` and
/// comes back `Float64`, so the caller needs no CAST of its own on either side.
/// `7 / 2` is `3.5` for all of them: an `Int64 / Int64` pairing must NOT
/// truncate to `3.0` the way DataFusion's own `/` operator does (#186).
#[test]
fn checked_float_div_divides_every_operand_pairing_as_double() {
    let pairings: Vec<(&str, ArrayRef, ArrayRef)> = vec![
        ("Int64/Int64", int64(vec![Some(7)]), int64(vec![Some(2)])),
        (
            "Int32/Int64",
            Arc::new(Int32Array::from(vec![Some(7)])),
            int64(vec![Some(2)]),
        ),
        (
            "Decimal128/Int64",
            decimal(vec![Some(7)]),
            int64(vec![Some(2)]),
        ),
        (
            "Int64/Decimal128",
            int64(vec![Some(7)]),
            decimal(vec![Some(2)]),
        ),
        (
            "Decimal128/Decimal128",
            decimal(vec![Some(7)]),
            decimal(vec![Some(2)]),
        ),
        (
            "Float32/Float64",
            Arc::new(Float32Array::from(vec![Some(7.0)])),
            float64(vec![Some(2.0)]),
        ),
        (
            "Float64/Float64",
            float64(vec![Some(7.0)]),
            float64(vec![Some(2.0)]),
        ),
    ];

    for (pairing, left, right) in pairings {
        let quotient = divide(left, right)
            .unwrap_or_else(|e| panic!("{pairing}: 7 / 2 must divide cleanly, got: {e}"));
        assert_eq!(
            quotient.data_type(),
            &DataType::Float64,
            "{pairing}: the result column must be Float64"
        );
        assert_eq!(
            quotient.value(0),
            3.5,
            "{pairing}: 7 / 2 must be true float division 3.5, not truncated"
        );
    }
}

/// A NULL in either operand yields NULL for that row with no error, including
/// the case a naive zero-divisor guard would raise on: a NULL numerator over a
/// zero divisor. A NULL row is absent, not a value to divide.
#[test]
fn checked_float_div_propagates_null_in_either_operand() {
    let left = float64(vec![None, Some(7.0), None, Some(7.0)]);
    let right = float64(vec![Some(2.0), None, Some(0.0), Some(2.0)]);

    let quotient = divide(left, right).expect("a NULL operand must not raise");

    assert!(quotient.is_null(0), "NULL numerator must yield NULL");
    assert!(quotient.is_null(1), "NULL divisor must yield NULL");
    assert!(
        quotient.is_null(2),
        "a NULL numerator over a zero divisor must yield NULL, not raise"
    );
    assert_eq!(
        quotient.value(3),
        3.5,
        "a fully populated row alongside NULL rows must still divide"
    );
}

/// A zero divisor under a non-zero numerator raises, naming a division by zero
/// in Exasol's own vocabulary rather than returning the `+Inf` that silently
/// changed a pushed filter's row count (#370).
#[test]
fn checked_float_div_raises_on_a_zero_divisor() {
    let message = raised(float64(vec![Some(7.0)]), float64(vec![Some(0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "a zero divisor must name a division by zero, got: {message}"
    );
    assert!(
        !message.contains("numeric value out of range"),
        "a zero divisor must NOT be reported as an out-of-range value, so the \
         two causes stay distinguishable in a support case, got: {message}"
    );
}

/// `0 / 0` raises with the same division-by-zero message a non-zero numerator
/// gets, rather than reaching the raw-scan NaN-at-emit gap that returned a
/// silent NULL (#246).
#[test]
fn checked_float_div_raises_on_zero_over_zero() {
    let message = raised(float64(vec![Some(0.0)]), float64(vec![Some(0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "0 / 0 must name a division by zero, not an out-of-range value, got: {message}"
    );
}

/// IEEE-754 `-0.0` equals `0.0`, so a negative-zero divisor is a division by
/// zero and not an overflow, whichever sign of infinity the quotient carries.
#[test]
fn checked_float_div_treats_negative_zero_as_zero() {
    let message = raised(float64(vec![Some(7.0)]), float64(vec![Some(-0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "a -0.0 divisor must name a division by zero, got: {message}"
    );
}

/// A finite numerator over a tiny finite divisor overflows to `+Inf` with no
/// zero in sight. It raises too, because Exasol can represent no non-finite
/// `DOUBLE` — but as an out-of-range value, not a division by zero.
#[test]
fn checked_float_div_raises_on_an_overflow_to_infinity() {
    let message = raised(float64(vec![Some(1e300)]), float64(vec![Some(1e-300)])).to_string();

    assert!(
        message.contains("numeric value out of range"),
        "an overflow to +Inf from two finite operands must name an \
         out-of-range value, got: {message}"
    );
    assert!(
        !message.contains("division by zero"),
        "an overflow with a non-zero divisor must NOT be reported as a \
         division by zero, got: {message}"
    );
}

/// A `NaN` stored in the source column raises as an out-of-range value. This is
/// the deliberate trade-off the spec records (#393): Exasol admits no non-finite
/// `DOUBLE`, so the value could not have been returned anyway.
#[test]
fn checked_float_div_raises_on_a_stored_non_finite_operand() {
    let message = raised(float64(vec![Some(f64::NAN)]), float64(vec![Some(2.0)])).to_string();

    assert!(
        message.contains("numeric value out of range"),
        "a stored NaN operand must name an out-of-range value, got: {message}"
    );
}

/// The transition case between the two variants: a stored non-finite numerator
/// over a ZERO divisor is a division by zero, not an out-of-range value.
///
/// [`checked_quotient`] tests finiteness first and classifies on
/// `divisor == 0.0` after it, which is what also makes a `-0.0` divisor a
/// division by zero. `NaN / 0.0` is the one input where the two variants
/// compete, and the pushdown spec makes a normative claim about which message
/// it gets, so the actual classification is pinned here.
#[test]
fn checked_float_div_reports_a_stored_non_finite_numerator_over_a_zero_divisor_as_a_zero_divisor() {
    let message = raised(float64(vec![Some(f64::NAN)]), float64(vec![Some(0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "a zero divisor must name a division by zero whatever the numerator \
         is, got: {message}"
    );
    assert!(
        !message.contains("numeric value out of range"),
        "a zero divisor must NOT be reported as an out-of-range value, even \
         when the numerator is a stored NaN, got: {message}"
    );
}

/// An empty batch is not an error: DataFusion evaluates a scalar function over
/// a zero-row batch whenever a filter or a file's row group leaves nothing.
#[test]
fn checked_float_div_returns_no_rows_for_an_empty_batch() {
    let quotient = divide(float64(vec![]), float64(vec![])).expect("an empty batch must not raise");

    assert_eq!(quotient.len(), 0, "an empty batch must yield no rows");
    assert_eq!(
        quotient.data_type(),
        &DataType::Float64,
        "an empty result column must still be Float64"
    );
}

/// Pins the route a division over two LITERAL operands takes out of the scan,
/// which the plan required establishing rather than assuming: an `Immutable`
/// scalar function is eligible for const-folding during optimization, and a
/// fold that raised would surface through `raw_scan`'s
/// `UdfError::User("DataFusion SQL error: {e}")` — bypassing
/// `classify_scan_error` and its framing entirely.
///
/// Measured on DataFusion 54.1: `ctx.sql` plans the statement successfully and
/// the raise arrives from the stream, the SAME route a column operand takes, so
/// the classifier sees it and no second framing site is needed. Should a later
/// DataFusion version fold this at plan time instead, `ctx.sql` starts returning
/// the error, this assertion fails, and the four planning sites
/// (`raw_scan` ×2, `partial_agg` ×2) then need the framing too.
#[tokio::test]
async fn checked_float_div_over_two_literals_surfaces_a_division_by_zero_message() {
    let ctx = SessionContext::new();
    register_checked_float_div_udf(&ctx);
    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(0, 0) AS quotient");

    let planned = ctx
        .sql(&sql)
        .await
        .expect("a two-literal division must still plan: the raise is expected from the stream");
    let error = planned
        .collect()
        .await
        .expect_err("a division over two zero literals must raise");

    let message = error.to_string();
    assert!(
        message.contains("division by zero"),
        "a two-literal division by zero must name a division by zero on the \
         route it leaves the scan, got: {message}"
    );
    assert!(
        find_checked_float_div_error(&error).is_some(),
        "the raised error must stay recognisable by type on the DataFusion \
         error chain, so the classifier never has to match message text, \
         got: {message}"
    );
}

/// The session keeps the failure as a TYPED value, which is what makes the
/// route DataFusion flattens recoverable.
///
/// `datafusion-datasource-parquet` 54.1's `row_filter.rs` turns ANY predicate
/// error into `ArrowError::ComputeError(format!("...{e:?}"))`, so a checked
/// division raised inside a pushed filter — issue #370's own route — reaches
/// the classifier with its type gone. Recovering it from that text would be the
/// message-matching coupling the spec forbids, so the lookup below must answer
/// from the session instead.
#[tokio::test]
async fn checked_float_div_records_its_failure_on_the_session() {
    let ctx = SessionContext::new();
    register_checked_float_div_udf(&ctx);

    assert!(
        session_checked_float_div_failure(&ctx).is_none(),
        "a session whose division has not raised must record no failure"
    );

    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(7, 0) AS quotient");
    ctx.sql(&sql)
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect_err("a zero divisor must raise");

    assert_eq!(
        session_checked_float_div_failure(&ctx),
        Some(CheckedFloatDivError::ZeroDivisor {
            numerator: 7.0,
            divisor: 0.0,
        }),
        "the session must hold the raised failure as a typed value, not as text"
    );
}

/// A session that never divided by zero records nothing, so the reframing is
/// inert for every scan whose SQL contains no division — the overwhelming
/// majority — and cannot turn an unrelated scan failure into a division error.
#[tokio::test]
async fn a_successful_checked_division_records_no_session_failure() {
    let ctx = SessionContext::new();
    register_checked_float_div_udf(&ctx);

    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(7, 2) AS quotient");
    ctx.sql(&sql)
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect("7 / 2 must divide cleanly");

    assert!(
        session_checked_float_div_failure(&ctx).is_none(),
        "a division that produced a finite value must record no failure"
    );
}

/// The recorded failure is scoped to ONE session, which is one scan invocation.
///
/// The pushdown spec requires that no failure survive into another scan. The
/// property holds because [`register_checked_float_div_udf`] builds a fresh
/// instance per call. Caching one instance in a `static`, a `LazyLock`, or a
/// shared `Arc` would leak one query's division failure into the next scan on a
/// pooled UDF VM, which reframes that scan's unrelated failure as a division by
/// zero. The scoping is pinned here rather than left to the registration's
/// shape.
#[tokio::test]
async fn a_second_session_records_no_failure_from_the_first() {
    let divided = SessionContext::new();
    let untouched = SessionContext::new();
    register_checked_float_div_udf(&divided);
    register_checked_float_div_udf(&untouched);

    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(7, 0) AS quotient");
    divided
        .sql(&sql)
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect_err("a zero divisor must raise");

    assert_eq!(
        session_checked_float_div_failure(&divided),
        Some(CheckedFloatDivError::ZeroDivisor {
            numerator: 7.0,
            divisor: 0.0,
        }),
        "the session whose division raised must hold the failure"
    );
    assert!(
        session_checked_float_div_failure(&untouched).is_none(),
        "a second session must record no failure from the first, or one query's \
         division leaks into the next scan on a pooled UDF VM"
    );
}

/// The arity guard in `invoke_with_args` refuses any count other than two.
///
/// Unreachable through planned SQL, because `Signature::any(2, ..)` makes
/// DataFusion reject a wrong arity first. It guards the direct-call path, which
/// is the one every test here uses and the one a future in-process caller would
/// use, so a silent wrong-arity read of `args.args` never happens.
#[test]
fn checked_float_div_refuses_an_argument_count_other_than_two() {
    let too_few = vec![float64(vec![Some(7.0)])];
    let too_many = vec![
        float64(vec![Some(7.0)]),
        float64(vec![Some(2.0)]),
        float64(vec![Some(1.0)]),
    ];

    for arguments in [too_few, too_many] {
        let count = arguments.len();
        let message = invoke_with_argument_count(arguments)
            .expect_err("an argument count other than two must be refused")
            .to_string();

        assert!(
            message.contains("takes exactly two arguments"),
            "a {count}-argument call must name the arity the function \
             requires, got: {message}"
        );
        assert!(
            message.contains(&format!("got {count}")),
            "a {count}-argument call must name the count it was given, so the \
             caller can see what it passed, got: {message}"
        );
    }
}
