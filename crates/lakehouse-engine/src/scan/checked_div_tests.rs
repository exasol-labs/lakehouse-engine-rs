use super::*;
use crate::scan::raised_failure::session_raised_failure;
use crate::scan::scan_session_context;
use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, Decimal128Type, Field, Float64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

/// Drives `invoke_with_args` as DataFusion does per batch, asserting observable output only.
fn divide(left: ArrayRef, right: ArrayRef) -> Result<Float64Array, DataFusionError> {
    let rows = left.len();
    let udf = ScalarUDF::from(CheckedFloatDivUdf::new(Arc::default()));
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

fn scan_session() -> SessionContext {
    scan_session_context(SessionConfig::new(), Arc::default())
}

/// The Exasol type an Iceberg `long` maps to, as a pushed `FLOAT_DIV` over integral columns sees it.
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

/// Panics on a returned value, so a silently-returned value fails as loudly as a wrong error.
fn raised(left: ArrayRef, right: ArrayRef) -> DataFusionError {
    match divide(left, right) {
        Err(error) => error,
        Ok(values) => {
            panic!("the checked division must raise rather than return a value, got: {values:?}")
        }
    }
}

/// `Signature::any(2, ..)` rejects other arities at plan time, so only a direct call reaches them.
fn invoke_with_argument_count(
    udf: ScalarUDF,
    arguments: Vec<ArrayRef>,
) -> Result<ColumnarValue, DataFusionError> {
    let rows = arguments.first().map_or(0, |first| first.len());
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

#[test]
fn checked_float_div_raises_on_zero_over_zero() {
    let message = raised(float64(vec![Some(0.0)]), float64(vec![Some(0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "0 / 0 must name a division by zero, not an out-of-range value, got: {message}"
    );
}

#[test]
fn checked_float_div_treats_negative_zero_as_zero() {
    let message = raised(float64(vec![Some(7.0)]), float64(vec![Some(-0.0)])).to_string();

    assert!(
        message.contains("division by zero"),
        "a -0.0 divisor must name a division by zero, got: {message}"
    );
}

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

#[test]
fn checked_float_div_raises_on_a_stored_non_finite_operand() {
    let message = raised(float64(vec![Some(f64::NAN)]), float64(vec![Some(2.0)])).to_string();

    assert!(
        message.contains("numeric value out of range"),
        "a stored NaN operand must name an out-of-range value, got: {message}"
    );
}

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

#[tokio::test]
async fn checked_float_div_over_two_literals_surfaces_a_division_by_zero_message() {
    let ctx = scan_session();
    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(0, 0) AS quotient");

    // If DataFusion ever const-folds this at plan time, the four planning sites
    // (`raw_scan` ×2, `partial_agg` ×2) bypass `classify_scan_error` and need framing too.
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
        find_checked_div_error(&error).is_some(),
        "the raised error must stay recognisable by type on the DataFusion \
         error chain, so the classifier never has to match message text, \
         got: {message}"
    );
}

#[tokio::test]
async fn checked_float_div_records_its_failure_on_the_session() {
    let ctx = scan_session();

    assert!(
        session_raised_failure(&ctx).is_none(),
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
        session_raised_failure(&ctx),
        Some(
            CheckedDivError::ZeroDivisor {
                numerator: 7.0,
                divisor: 0.0,
            }
            .to_string()
        ),
        "the session must hold the raised failure"
    );
}

#[tokio::test]
async fn a_successful_checked_division_records_no_session_failure() {
    let ctx = scan_session();

    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(7, 2) AS quotient");
    ctx.sql(&sql)
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect("7 / 2 must divide cleanly");

    assert!(
        session_raised_failure(&ctx).is_none(),
        "a division that produced a finite value must record no failure"
    );
}

#[tokio::test]
async fn a_second_session_records_no_failure_from_the_first() {
    let divided = scan_session();
    let untouched = scan_session();

    let sql = format!("SELECT {CHECKED_FLOAT_DIV_FN}(7, 0) AS quotient");
    divided
        .sql(&sql)
        .await
        .expect("the statement must plan")
        .collect()
        .await
        .expect_err("a zero divisor must raise");

    assert_eq!(
        session_raised_failure(&divided),
        Some(
            CheckedDivError::ZeroDivisor {
                numerator: 7.0,
                divisor: 0.0,
            }
            .to_string()
        ),
        "the session whose division raised must hold the failure"
    );
    assert!(
        session_raised_failure(&untouched).is_none(),
        "a second session must record no failure from the first, or one query's \
         division leaks into the next scan on a pooled UDF VM"
    );
}

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
        let message = invoke_with_argument_count(
            ScalarUDF::from(CheckedFloatDivUdf::new(Arc::default())),
            arguments,
        )
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

/// Drives `invoke_with_args` of the integer division as DataFusion does per batch.
fn int_divide(left: ArrayRef, right: ArrayRef) -> Result<Decimal128Array, DataFusionError> {
    let rows = left.len();
    let udf = ScalarUDF::from(CheckedIntDivUdf::new(Arc::default()));
    let declared = udf.return_type(&[left.data_type().clone(), right.data_type().clone()])?;
    let args = ScalarFunctionArgs {
        arg_fields: vec![
            Arc::new(Field::new("left", left.data_type().clone(), true)),
            Arc::new(Field::new("right", right.data_type().clone(), true)),
        ],
        args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
        number_rows: rows,
        return_field: Arc::new(Field::new("quotient", declared, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let array = udf.invoke_with_args(args)?.to_array(rows)?;
    Ok(array.as_primitive::<Decimal128Type>().clone())
}

fn int_quotients(left: Vec<Option<i64>>, right: Vec<Option<i64>>) -> Vec<Option<i128>> {
    int_divide(int64(left), int64(right))
        .expect("the division must succeed")
        .iter()
        .collect()
}

fn decimal_of(precision: u8, scale: i8, values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("the precision and scale must be inside Arrow's Decimal128 domain"),
    )
}

fn int_div_return_type(
    numerator: DataType,
    divisor: DataType,
) -> Result<DataType, DataFusionError> {
    CheckedIntDivUdf::new(Arc::default()).return_type(&[numerator, divisor])
}

#[test]
fn checked_int_div_truncates_toward_zero_for_every_sign_pairing() {
    let quotients = int_quotients(
        vec![Some(7), Some(-7), Some(7), Some(-7)],
        vec![Some(2), Some(2), Some(-2), Some(-2)],
    );

    assert_eq!(
        quotients,
        vec![Some(3), Some(-3), Some(-3), Some(3)],
        "an odd numerator must truncate toward zero, not round or floor"
    );
}

#[test]
fn checked_int_div_raises_on_a_zero_divisor_naming_the_numerator() {
    let error = int_divide(int64(vec![Some(7)]), int64(vec![Some(0)]))
        .expect_err("a zero divisor must raise");

    let found = find_checked_div_error(&error).expect("the raise must be recognisable by type");
    assert!(
        matches!(found, CheckedDivError::IntegerZeroDivisor { numerator: 7 }),
        "the raised error must be IntegerZeroDivisor over the numerator, got: {found}"
    );
    assert!(
        found
            .to_string()
            .starts_with("data exception - division by zero"),
        "the message must open with the division-by-zero state, got: {found}"
    );
}

#[test]
fn checked_int_div_refuses_an_argument_count_other_than_two() {
    let too_few = vec![int64(vec![Some(7)])];
    let too_many = vec![
        int64(vec![Some(7)]),
        int64(vec![Some(2)]),
        int64(vec![Some(1)]),
    ];

    for arguments in [too_few, too_many] {
        let count = arguments.len();
        let invoked = invoke_with_argument_count(
            ScalarUDF::from(CheckedIntDivUdf::new(Arc::default())),
            arguments,
        )
        .expect_err("an argument count other than two must be refused")
        .to_string();
        let planned = CheckedIntDivUdf::new(Arc::default())
            .return_type(&vec![DataType::Int64; count])
            .expect_err("a plan with an argument count other than two must be refused")
            .to_string();

        for message in [invoked, planned] {
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
}

#[test]
fn checked_int_div_raises_numeric_overflow_on_i128_min_over_minus_one() {
    let error = int_divide(
        decimal_of(38, 0, vec![Some(i128::MIN)]),
        decimal_of(38, 0, vec![Some(-1)]),
    )
    .expect_err("i128::MIN over -1 must raise rather than abort the VM");

    let found = find_checked_div_error(&error).expect("the raise must be recognisable by type");
    assert!(
        matches!(found, CheckedDivError::IntegerOverflow { .. }),
        "the raised error must be IntegerOverflow, got: {found}"
    );
    assert!(
        found.to_string().contains("numeric value out of range"),
        "the message must carry the numeric-overflow state, got: {found}"
    );
}

#[test]
fn checked_int_div_raises_on_zero_over_zero() {
    let error = int_divide(int64(vec![Some(0)]), int64(vec![Some(0)]))
        .expect_err("zero over zero must raise");

    assert!(matches!(
        find_checked_div_error(&error),
        Some(CheckedDivError::IntegerZeroDivisor { numerator: 0 })
    ));
}

#[test]
fn checked_int_div_yields_null_when_either_operand_is_null() {
    let quotients = int_quotients(
        vec![None, Some(7), None, Some(8)],
        vec![Some(0), None, None, Some(2)],
    );

    assert_eq!(
        quotients,
        vec![None, None, None, Some(4)],
        "a NULL row must be skipped before the zero check, so NULL over zero is NULL"
    );
}

#[test]
fn checked_int_div_divides_over_a_null_typed_divisor() {
    let divisor = new_null_array(&DataType::Null, 2);

    let quotients = int_divide(int64(vec![Some(7), Some(8)]), divisor)
        .expect("a Null-typed divisor must yield NULLs, not raise");

    assert_eq!(quotients.null_count(), 2, "every row must be NULL");
}

#[test]
fn checked_int_div_widens_the_most_negative_int64_over_minus_one() {
    let quotients = int_quotients(vec![Some(i64::MIN)], vec![Some(-1)]);

    assert_eq!(
        quotients,
        vec![Some(9_223_372_036_854_775_808)],
        "i64::MIN / -1 must widen instead of overflowing"
    );
}

#[test]
fn checked_int_div_keeps_the_largest_uint64_exact() {
    let quotient = int_divide(
        Arc::new(UInt64Array::from(vec![Some(u64::MAX)])),
        Arc::new(UInt64Array::from(vec![Some(1)])),
    )
    .expect("the division must succeed");

    assert_eq!(
        quotient.value(0),
        i128::from(u64::MAX),
        "UInt64::MAX / 1 must not pass through Int64"
    );
}

#[test]
fn checked_int_div_divides_a_36_digit_decimal() {
    let nines = 10_i128.pow(36) - 1;

    let quotient = int_divide(
        decimal_of(36, 0, vec![Some(nines)]),
        decimal_of(36, 0, vec![Some(7)]),
    )
    .expect("the division must succeed");

    assert_eq!(quotient.value(0), nines / 7);
    assert_eq!(quotient.data_type(), &DataType::Decimal128(36, 0));
}

#[test]
fn checked_int_div_result_type_is_the_numerator_precision_with_scale_zero() {
    let cases = [
        (DataType::Int8, 3),
        (DataType::Int16, 5),
        (DataType::Int32, 10),
        (DataType::Int64, 19),
        (DataType::UInt8, 3),
        (DataType::UInt16, 5),
        (DataType::UInt32, 10),
        (DataType::UInt64, 20),
        (DataType::Decimal128(12, 0), 12),
        (DataType::Null, 1),
    ];

    for (numerator, precision) in cases {
        let declared = int_div_return_type(numerator.clone(), DataType::Int8)
            .unwrap_or_else(|e| panic!("{numerator}: must plan, got: {e}"));
        assert_eq!(
            declared,
            DataType::Decimal128(precision, 0),
            "{numerator}: the result must carry the numerator's precision"
        );
    }
}

#[test]
fn checked_int_div_result_ignores_the_divisor_type() {
    let declared = int_div_return_type(DataType::Int16, DataType::UInt64)
        .expect("two integer types must plan");

    assert_eq!(declared, DataType::Decimal128(5, 0));
}

#[test]
fn checked_int_div_declared_type_matches_every_integer_array_type() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(9)])),
        Arc::new(Int16Array::from(vec![Some(9)])),
        Arc::new(Int32Array::from(vec![Some(9)])),
        Arc::new(UInt8Array::from(vec![Some(9)])),
        Arc::new(UInt16Array::from(vec![Some(9)])),
        Arc::new(UInt32Array::from(vec![Some(9)])),
    ];

    for numerator in arrays {
        let declared = int_div_return_type(numerator.data_type().clone(), DataType::Int8)
            .expect("an integer type must plan");
        let quotient = int_divide(numerator.clone(), int64(vec![Some(2)]))
            .unwrap_or_else(|e| panic!("{}: must divide, got: {e}", numerator.data_type()));
        assert_eq!(quotient.value(0), 4, "{}", numerator.data_type());
        assert_eq!(
            quotient.data_type(),
            &declared,
            "{}: the produced column must match the declared type",
            numerator.data_type()
        );
    }
}

#[test]
fn checked_int_div_refuses_a_non_integer_operand_at_planning() {
    let cases = [
        ("Float64 numerator", DataType::Float64, DataType::Int64),
        ("Float64 divisor", DataType::Int64, DataType::Float64),
        (
            "scaled decimal numerator",
            DataType::Decimal128(10, 2),
            DataType::Int64,
        ),
        (
            "scaled decimal divisor",
            DataType::Int64,
            DataType::Decimal128(10, 2),
        ),
    ];

    for (case, numerator, divisor) in cases {
        let message = int_div_return_type(numerator, divisor)
            .expect_err(&format!("{case}: must fail planning"))
            .to_string();
        assert!(
            message.contains(CHECKED_INT_DIV_FN) && message.contains("integers only"),
            "{case}: the failure must name the function and the constraint, got: {message}"
        );
    }
}

#[tokio::test]
async fn checked_int_div_divides_a_literal_numerator_of_twenty_digits() {
    let ctx = scan_session();
    let sql = format!("SELECT {CHECKED_INT_DIV_FN}(12345678901234567890, 3) AS quotient");

    let batches = ctx
        .sql(&sql)
        .await
        .expect("a 20-digit numerator must plan")
        .collect()
        .await
        .expect("the division must succeed");

    let quotient = batches[0].column(0).as_primitive::<Decimal128Type>();
    assert_eq!(quotient.value(0), 4_115_226_300_411_522_630);
    assert_eq!(quotient.data_type(), &DataType::Decimal128(20, 0));
}

#[tokio::test]
async fn checked_int_div_over_two_literals_surfaces_a_division_by_zero_message() {
    let ctx = scan_session();
    let sql = format!("SELECT {CHECKED_INT_DIV_FN}(7, 0) AS quotient");

    let error = ctx
        .sql(&sql)
        .await
        .expect("the statement must plan: the raise is expected from the stream")
        .collect()
        .await
        .expect_err("a zero divisor must raise");

    let message = error.to_string();
    assert!(
        message.contains("division by zero"),
        "the failure must name a division by zero, got: {message}"
    );
    assert!(
        find_checked_div_error(&error).is_some(),
        "the raised error must stay recognisable by type, got: {message}"
    );
    assert_eq!(
        session_raised_failure(&ctx),
        Some(CheckedDivError::IntegerZeroDivisor { numerator: 7 }.to_string()),
        "the session must hold the raised failure"
    );
}
