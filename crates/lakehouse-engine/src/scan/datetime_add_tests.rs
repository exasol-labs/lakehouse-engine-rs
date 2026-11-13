use super::*;
use crate::scan::raised_failure::session_raised_failure;
use crate::scan::scan_session_context;
use crate::scan::test_support::invoke_scalar_udf;
use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    NullArray, RecordBatch, TimestampMicrosecondArray, UInt64Array,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{Date32Type, Field};
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::common::config::ConfigOptions;
use datafusion::prelude::SessionConfig;

const TIMESTAMP_TEXT: &str = "%Y-%m-%d %H:%M:%S%.6f";

fn days(text: &str) -> i32 {
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .unwrap_or_else(|e| panic!("{text} must be a date: {e}"));
    Date32Type::from_naive_date(date)
}

fn micros(text: &str) -> i64 {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap_or_else(|e| panic!("{text} must be a timestamp: {e}"))
        .and_utc()
        .timestamp_micros()
}

fn date(text: &str) -> ArrayRef {
    Arc::new(Date32Array::from(vec![days(text)]))
}

fn timestamp(text: &str) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![micros(text)]))
}

fn double(value: f64) -> ArrayRef {
    Arc::new(Float64Array::from(vec![value]))
}

fn integer(value: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value]))
}

/// One DECIMAL(`precision`, `scale`) count whose unscaled value is `unscaled`.
fn decimal(unscaled: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![unscaled])
            .with_precision_and_scale(precision, scale)
            .expect("the test decimal must be a valid Decimal128"),
    )
}

/// A DECIMAL(30, 30) count of 0.4999…9 with 30 fractional digits, signed by `sign`.
fn thirty_digit_almost_half(sign: i128) -> ArrayRef {
    decimal(sign * (5 * 10_i128.pow(29) - 1), 30, 30)
}

fn add(unit: Unit, argument: ArrayRef, count: ArrayRef) -> Result<ArrayRef, DataFusionError> {
    add_in(CountArithmetic::Nanoseconds, unit, argument, count)
}

fn add_in(
    arithmetic: CountArithmetic,
    unit: Unit,
    argument: ArrayRef,
    count: ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let rows = argument.len();
    let args = vec![ColumnarValue::Array(argument), ColumnarValue::Array(count)];
    let udf = DatetimeAddUdf::new(unit, arithmetic, Arc::default());
    invoke_scalar_udf(&udf, args, rows)?.to_array(rows)
}

/// Each row as Exasol displays it, so a test reads as the SQL it stands for.
fn rendered(array: &ArrayRef) -> Vec<Option<String>> {
    match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .iter()
            .map(|value| value.map(|d| Date32Type::to_naive_date_opt(d).unwrap().to_string()))
            .collect(),
        _ => array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .map(|value| {
                value.map(|m| {
                    timestamp_us_to_datetime(m)
                        .unwrap()
                        .format(TIMESTAMP_TEXT)
                        .to_string()
                })
            })
            .collect(),
    }
}

/// The one result of a one-row call; panics on a failure, so a wrong failure reads as one.
fn added(unit: Unit, argument: ArrayRef, count: ArrayRef) -> String {
    let result = add(unit, argument, count)
        .unwrap_or_else(|e| panic!("{unit:?} must return a value, got: {e}"));
    rendered(&result)
        .pop()
        .flatten()
        .unwrap_or_else(|| panic!("{unit:?} must return a non-NULL value"))
}

/// The message of a failing call; panics on a returned value, so a value fails as loudly as a
/// wrong failure.
fn failure(unit: Unit, argument: ArrayRef, count: ArrayRef) -> String {
    failure_in(CountArithmetic::Nanoseconds, unit, argument, count)
}

fn failure_in(
    arithmetic: CountArithmetic,
    unit: Unit,
    argument: ArrayRef,
    count: ArrayRef,
) -> String {
    match add_in(arithmetic, unit, argument, count) {
        Err(DataFusionError::Execution(message)) => message,
        Err(other) => panic!("{unit:?} must fail with Exasol's message, got: {other}"),
        Ok(result) => panic!("{unit:?} must fail, got: {:?}", rendered(&result)),
    }
}

/// The error class the E2E harness compares with native Exasol's.
fn class(message: &str) -> &str {
    message.split([';', '(']).next().unwrap_or_default().trim()
}

/// Scenario: The scan rounds a fractional count as Exasol does
#[test]
fn counts_round_as_exasol_rounds() {
    let base = || date("2024-01-10");
    let cases: Vec<(&str, Unit, ArrayRef, &str)> = vec![
        ("DOUBLE 0.5", Unit::Days, double(0.5), "2024-01-11"),
        ("DOUBLE 1.5", Unit::Days, double(1.5), "2024-01-12"),
        ("DOUBLE 2.5", Unit::Days, double(2.5), "2024-01-13"),
        ("DOUBLE -0.5", Unit::Days, double(-0.5), "2024-01-09"),
        ("DOUBLE -1.5", Unit::Days, double(-1.5), "2024-01-08"),
        ("DOUBLE -2.5", Unit::Days, double(-2.5), "2024-01-07"),
        ("DOUBLE 2.49", Unit::Days, double(2.49), "2024-01-12"),
        (
            "DOUBLE 0.49999999999999994",
            Unit::Days,
            double(0.499_999_999_999_999_94),
            "2024-01-11",
        ),
        (
            "DOUBLE -0.49999999999999994",
            Unit::Days,
            double(-0.499_999_999_999_999_94),
            "2024-01-09",
        ),
        (
            "REAL 2.5",
            Unit::Days,
            Arc::new(Float32Array::from(vec![2.5_f32])),
            "2024-01-13",
        ),
        ("DECIMAL 2.50", Unit::Days, decimal(250, 3, 2), "2024-01-13"),
        (
            "DECIMAL -2.50",
            Unit::Days,
            decimal(-250, 3, 2),
            "2024-01-07",
        ),
        ("DECIMAL 2.49", Unit::Days, decimal(249, 3, 2), "2024-01-12"),
        (
            "DECIMAL -2.49",
            Unit::Days,
            decimal(-249, 3, 2),
            "2024-01-08",
        ),
        ("DECIMAL 0.50", Unit::Days, decimal(50, 3, 2), "2024-01-11"),
        (
            "DECIMAL 0.499…9 with 30 digits",
            Unit::Days,
            thirty_digit_almost_half(1),
            "2024-01-10",
        ),
        (
            "DECIMAL -0.499…9 with 30 digits",
            Unit::Days,
            thirty_digit_almost_half(-1),
            "2024-01-10",
        ),
        (
            "DECIMAL 0.5 at scale 30",
            Unit::Days,
            decimal(5 * 10_i128.pow(29), 30, 30),
            "2024-01-11",
        ),
        (
            "DECIMAL 3E2 at scale -2",
            Unit::Days,
            decimal(3, 3, -2),
            "2024-11-05",
        ),
        (
            "INTEGER 3",
            Unit::Days,
            Arc::new(Int32Array::from(vec![3])),
            "2024-01-13",
        ),
        ("BIGINT -3", Unit::Days, integer(-3), "2024-01-07"),
        (
            "UNSIGNED 3",
            Unit::Days,
            Arc::new(UInt64Array::from(vec![3_u64])),
            "2024-01-13",
        ),
        ("weeks DOUBLE 1.5", Unit::Weeks, double(1.5), "2024-01-24"),
        (
            "weeks DECIMAL 1.5",
            Unit::Weeks,
            decimal(15, 2, 1),
            "2024-01-24",
        ),
        (
            "weeks DECIMAL 0.49",
            Unit::Weeks,
            decimal(49, 2, 2),
            "2024-01-10",
        ),
        (
            "hours DOUBLE 1.5",
            Unit::Hours,
            double(1.5),
            "2024-01-10 02:00:00.000000",
        ),
        (
            "minutes DECIMAL -0.5",
            Unit::Minutes,
            decimal(-5, 1, 1),
            "2024-01-09 23:59:00.000000",
        ),
    ];

    for (count, unit, counts, expected) in cases {
        assert_eq!(added(unit, base(), counts), expected, "{unit:?} by {count}");
    }
}

/// Scenario: The scan rounds a fractional count as Exasol does
#[test]
fn non_finite_counts_fail() {
    let non_finite: Vec<(&str, ArrayRef)> = vec![
        ("NaN", double(f64::NAN)),
        ("+Infinity", double(f64::INFINITY)),
        ("-Infinity", double(f64::NEG_INFINITY)),
        ("REAL NaN", Arc::new(Float32Array::from(vec![f32::NAN]))),
    ];
    for unit in Unit::ALL {
        for (count, counts) in &non_finite {
            let message = failure(unit, date("2024-01-10"), counts.clone());
            assert!(
                message.starts_with("data exception - numeric value out of range"),
                "{unit:?} by {count} must fail as out of range, got: {message}"
            );
        }
    }
}

/// Scenario: A result outside Exasol's datetime range fails the query
#[test]
fn results_outside_the_range_fail() {
    let within: Vec<(Unit, ArrayRef, ArrayRef, &str)> = vec![
        (Unit::Days, date("9999-12-31"), integer(0), "9999-12-31"),
        (Unit::Days, date("0001-01-01"), integer(0), "0001-01-01"),
        (Unit::Days, date("9999-12-30"), integer(1), "9999-12-31"),
        (Unit::Days, date("0001-01-02"), integer(-1), "0001-01-01"),
        (Unit::Weeks, date("9999-12-24"), integer(1), "9999-12-31"),
        (Unit::Weeks, date("0001-01-08"), integer(-1), "0001-01-01"),
        (
            Unit::Hours,
            date("9999-12-31"),
            integer(23),
            "9999-12-31 23:00:00.000000",
        ),
        (
            Unit::Minutes,
            timestamp("9999-12-31 23:58:59.999999"),
            integer(1),
            "9999-12-31 23:59:59.999999",
        ),
        (
            Unit::Hours,
            timestamp("0001-01-01 01:00:00"),
            integer(-1),
            "0001-01-01 00:00:00.000000",
        ),
    ];
    for (unit, argument, count, expected) in within {
        assert_eq!(
            added(unit, argument, count),
            expected,
            "{unit:?} to {expected}"
        );
    }

    let overflow = "data exception - datetime field overflow";
    let underflow = "data exception - datetime field underflow";
    let outside: Vec<(&str, Unit, ArrayRef, ArrayRef, &str)> = vec![
        (
            "9999-12-31 + 1 day",
            Unit::Days,
            date("9999-12-31"),
            integer(1),
            overflow,
        ),
        (
            "0001-01-01 - 1 day",
            Unit::Days,
            date("0001-01-01"),
            integer(-1),
            underflow,
        ),
        (
            "9999-12-25 + 1 week",
            Unit::Weeks,
            date("9999-12-25"),
            integer(1),
            overflow,
        ),
        (
            "0001-01-07 - 1 week",
            Unit::Weeks,
            date("0001-01-07"),
            integer(-1),
            underflow,
        ),
        (
            "9999-12-31 + 24 hours",
            Unit::Hours,
            date("9999-12-31"),
            integer(24),
            overflow,
        ),
        (
            "0001-01-01 - 1 minute",
            Unit::Minutes,
            date("0001-01-01"),
            integer(-1),
            underflow,
        ),
        (
            "9999-12-31 23:59:00 + 1 minute",
            Unit::Minutes,
            timestamp("9999-12-31 23:59:00"),
            integer(1),
            overflow,
        ),
        (
            "0001-01-01 00:59:59.999999 - 1 hour",
            Unit::Hours,
            timestamp("0001-01-01 00:59:59.999999"),
            integer(-1),
            underflow,
        ),
    ];
    for (call, unit, argument, count, expected) in outside {
        let message = failure(unit, argument, count);
        assert_eq!(class(&message), expected, "{call}: {message}");
    }
}

/// Scenario: A result outside Exasol's datetime range fails the query
#[test]
fn day_and_week_counts_beyond_the_limit_fail() {
    let base = || date("2024-01-10");
    let numeric = "data exception - numeric value out of range";
    let overflow = "data exception - datetime field overflow";
    let underflow = "data exception - datetime field underflow";
    let cases: Vec<(&str, Unit, ArrayRef, &str)> = vec![
        (
            "DOUBLE 999999999.4 days",
            Unit::Days,
            double(999_999_999.4),
            numeric,
        ),
        (
            "DOUBLE -999999999.4 days",
            Unit::Days,
            double(-999_999_999.4),
            numeric,
        ),
        (
            "DOUBLE 999999999.4 weeks",
            Unit::Weeks,
            double(999_999_999.4),
            numeric,
        ),
        (
            "DOUBLE 999999999 days",
            Unit::Days,
            double(999_999_999.0),
            overflow,
        ),
        (
            "DECIMAL 999999999.4999999999999 days",
            Unit::Days,
            decimal(9_999_999_994_999_999_999_999, 22, 13),
            overflow,
        ),
        (
            "DECIMAL -999999999.4999999999999 days",
            Unit::Days,
            decimal(-9_999_999_994_999_999_999_999, 22, 13),
            underflow,
        ),
        (
            "DECIMAL 999999999.5 weeks",
            Unit::Weeks,
            decimal(9_999_999_995, 10, 1),
            numeric,
        ),
        (
            "BIGINT 1000000000 days",
            Unit::Days,
            integer(1_000_000_000),
            numeric,
        ),
        (
            "BIGINT -1000000000 days",
            Unit::Days,
            integer(-1_000_000_000),
            numeric,
        ),
        (
            "BIGINT 999999999 weeks",
            Unit::Weeks,
            integer(999_999_999),
            overflow,
        ),
        (
            "BIGINT 1000000000 hours",
            Unit::Hours,
            integer(1_000_000_000),
            overflow,
        ),
    ];
    for (call, unit, count, expected) in cases {
        let message = failure(unit, base(), count);
        assert!(
            class(&message).starts_with(expected),
            "{call} must fail with {expected}, got: {message}"
        );
    }

    assert_eq!(
        added(Unit::Minutes, base(), double(999_999_999.4)),
        "3925-05-08 10:39:00.000000",
        "ADD_MINUTES has no count limit"
    );
}

/// Scenario: A result outside Exasol's datetime range fails the query
#[test]
fn hour_and_minute_counts_beyond_the_nanosecond_range_fail_as_numeric_out_of_range() {
    let numeric = "data exception - numeric value out of range";
    let overflow = "data exception - datetime field overflow";
    let cases: Vec<(&str, Unit, ArrayRef, &str)> = vec![
        ("DOUBLE 1e30 hours", Unit::Hours, double(1e30), numeric),
        ("DOUBLE -1e30 hours", Unit::Hours, double(-1e30), numeric),
        ("DOUBLE 1e30 minutes", Unit::Minutes, double(1e30), numeric),
        ("DOUBLE 1e300 hours", Unit::Hours, double(1e300), numeric),
        ("DOUBLE -1e300 hours", Unit::Hours, double(-1e300), numeric),
        (
            "DOUBLE 1e300 minutes",
            Unit::Minutes,
            double(1e300),
            numeric,
        ),
        (
            "DECIMAL(31,0) 1e30 hours",
            Unit::Hours,
            decimal(10_i128.pow(30), 31, 0),
            numeric,
        ),
        ("DOUBLE 1e24 hours", Unit::Hours, double(1e24), overflow),
        ("DOUBLE 1e26 minutes", Unit::Minutes, double(1e26), overflow),
    ];
    for (call, unit, count, expected) in cases {
        let message = failure(unit, date("2024-01-10"), count);
        assert_eq!(class(&message), expected, "{call}: {message}");
    }
}

/// Exasol 8 refuses a rounded count whose milliseconds exceed 18 digits; the nanosecond
/// arithmetic of later versions reads the same counts as a datetime overflow.
#[test]
fn hour_and_minute_counts_beyond_exasol_8_milliseconds_fail_as_numeric_out_of_range() {
    use CountArithmetic::{Milliseconds, Nanoseconds};
    let numeric = "data exception - numeric value out of range";
    let overflow = "data exception - datetime field overflow";
    let underflow = "data exception - datetime field underflow";
    let cases: Vec<(&str, CountArithmetic, Unit, ArrayRef, &str)> = vec![
        (
            "hours at the limit",
            Milliseconds,
            Unit::Hours,
            integer(277_777_777_777),
            overflow,
        ),
        (
            "hours past it",
            Milliseconds,
            Unit::Hours,
            integer(277_777_777_778),
            numeric,
        ),
        (
            "negative hours past it",
            Milliseconds,
            Unit::Hours,
            integer(-277_777_777_778),
            numeric,
        ),
        (
            "negative hours at it",
            Milliseconds,
            Unit::Hours,
            integer(-277_777_777_777),
            underflow,
        ),
        (
            "DOUBLE hours rounding past it",
            Milliseconds,
            Unit::Hours,
            double(277_777_777_777.5),
            numeric,
        ),
        (
            "DOUBLE hours rounding to it",
            Milliseconds,
            Unit::Hours,
            double(277_777_777_777.4),
            overflow,
        ),
        (
            "minutes at the limit",
            Milliseconds,
            Unit::Minutes,
            integer(16_666_666_666_666),
            overflow,
        ),
        (
            "minutes past it",
            Milliseconds,
            Unit::Minutes,
            integer(16_666_666_666_667),
            numeric,
        ),
        (
            "DOUBLE 1e15 hours",
            Milliseconds,
            Unit::Hours,
            double(1e15),
            numeric,
        ),
        (
            "hours past it, nanoseconds",
            Nanoseconds,
            Unit::Hours,
            integer(277_777_777_778),
            overflow,
        ),
        (
            "DOUBLE 1e15 hours, nanoseconds",
            Nanoseconds,
            Unit::Hours,
            double(1e15),
            overflow,
        ),
    ];
    for (call, arithmetic, unit, count, expected) in cases {
        let message = failure_in(arithmetic, unit, date("2024-01-10"), count);
        assert_eq!(class(&message), expected, "{call}: {message}");
    }
}

#[test]
fn the_database_version_selects_the_count_arithmetic() {
    let cases = [
        ("8.29.13", CountArithmetic::Milliseconds),
        ("7.1.30", CountArithmetic::Milliseconds),
        ("2025.1.16", CountArithmetic::Nanoseconds),
        ("2026.1.0", CountArithmetic::Nanoseconds),
        ("", CountArithmetic::Nanoseconds),
        ("unknown", CountArithmetic::Nanoseconds),
    ];
    for (version, expected) in cases {
        assert_eq!(
            CountArithmetic::from_database_version(version),
            expected,
            "version={version:?}"
        );
    }
}

/// Scenario: A date before 1582-10-15 follows the proleptic Gregorian calendar in the scan
#[test]
fn dates_before_the_gregorian_switch_use_the_proleptic_calendar() {
    let cases: Vec<(Unit, ArrayRef, i64, &str)> = vec![
        (Unit::Days, date("1500-03-01"), -1, "1500-02-28"),
        (Unit::Days, date("1000-01-01"), 1, "1000-01-02"),
        (Unit::Days, date("1582-10-04"), 1, "1582-10-05"),
        (Unit::Days, date("1582-10-16"), -1, "1582-10-15"),
        (Unit::Weeks, date("1582-10-01"), 1, "1582-10-08"),
        (
            Unit::Hours,
            date("1000-01-01"),
            24,
            "1000-01-02 00:00:00.000000",
        ),
        (
            Unit::Minutes,
            timestamp("1500-03-01 00:00:00.25"),
            -1,
            "1500-02-28 23:59:00.250000",
        ),
    ];
    for (unit, argument, count, expected) in cases {
        assert_eq!(
            added(unit, argument, integer(count)),
            expected,
            "{unit:?} by {count}"
        );
    }
}

/// Scenario: The scan rounds a fractional count as Exasol does
#[test]
fn a_null_argument_or_count_returns_null() {
    // A NULL slot's value buffer holds 9999-12-31, which would overflow if it were read.
    let arguments: ArrayRef = Arc::new(Date32Array::new(
        ScalarBuffer::from(vec![
            days("9999-12-31"),
            days("2024-01-10"),
            days("2024-01-10"),
        ]),
        Some(NullBuffer::from(vec![false, true, true])),
    ));
    let counts: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(1.0)]));
    for unit in Unit::ALL {
        let result = add(unit, arguments.clone(), counts.clone())
            .unwrap_or_else(|e| panic!("{unit:?}: a NULL row must not fail, got: {e}"));
        let rows = rendered(&result);
        assert_eq!(
            rows[..2],
            [None::<String>, None],
            "{unit:?}: a NULL row must stay NULL"
        );
        assert!(
            rows[2].is_some(),
            "{unit:?}: a full row must return a value"
        );
    }

    let null_argument_with_bad_count = add(
        Unit::Days,
        Arc::new(Date32Array::from(vec![None])),
        double(f64::NAN),
    )
    .expect("a NULL argument must return NULL before its count is checked");
    assert_eq!(rendered(&null_argument_with_bad_count), [None::<String>]);

    let null_typed_count = add(Unit::Hours, date("2024-01-10"), Arc::new(NullArray::new(1)))
        .expect("a NULL literal count must return NULL");
    assert_eq!(rendered(&null_typed_count), [None::<String>]);
}

/// Scenario: The four functions over a TIMESTAMP keep its fractional seconds in the scan
#[test]
fn fractional_seconds_are_kept() {
    let base = || timestamp("2024-02-28 23:59:59.123456");
    let cases = [
        (Unit::Days, "2024-02-29 23:59:59.123456"),
        (Unit::Weeks, "2024-03-06 23:59:59.123456"),
        (Unit::Hours, "2024-02-29 00:59:59.123456"),
        (Unit::Minutes, "2024-02-29 00:00:59.123456"),
    ];
    for (unit, expected) in cases {
        assert_eq!(added(unit, base(), integer(1)), expected, "{unit:?}");
    }
    assert_eq!(
        added(
            Unit::Minutes,
            timestamp("2024-12-31 23:59:30.5"),
            integer(1)
        ),
        "2025-01-01 00:00:30.500000",
        "a year end"
    );
}

/// Scenario: ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan
/// Scenario: ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan
#[test]
fn result_types_follow_the_argument_and_the_unit() {
    let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, None);
    let cases = [
        (Unit::Days, DataType::Date32, DataType::Date32),
        (Unit::Weeks, DataType::Date32, DataType::Date32),
        (Unit::Hours, DataType::Date32, timestamp_type.clone()),
        (Unit::Minutes, DataType::Date32, timestamp_type.clone()),
    ];
    for (unit, argument, expected) in cases {
        let udf = DatetimeAddUdf::new(unit, CountArithmetic::default(), Arc::default());
        assert_eq!(
            udf.return_type(&[argument.clone(), DataType::Int64])
                .unwrap(),
            expected,
            "{unit:?} over {argument}"
        );
        assert_eq!(
            udf.return_type(&[timestamp_type.clone(), DataType::Float64])
                .unwrap(),
            timestamp_type,
            "{unit:?} over a timestamp"
        );
    }
}

#[test]
fn other_argument_and_count_types_fail_planning() {
    let date_type = DataType::Date32;
    let rejected = [
        vec![DataType::Utf8, DataType::Int64],
        vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Int64,
        ],
        vec![
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            DataType::Int64,
        ],
        vec![DataType::Null, DataType::Int64],
        vec![DataType::Date64, DataType::Int64],
        vec![date_type.clone(), DataType::Utf8],
        vec![date_type.clone(), DataType::Boolean],
        vec![date_type.clone(), DataType::Date32],
        vec![date_type.clone()],
        vec![date_type.clone(), DataType::Int64, DataType::Int64],
    ];
    for unit in Unit::ALL {
        let udf = DatetimeAddUdf::new(unit, CountArithmetic::default(), Arc::default());
        for arguments in &rejected {
            assert!(
                matches!(udf.return_type(arguments), Err(DataFusionError::Plan(_))),
                "{unit:?} must fail planning over {arguments:?}"
            );
        }
    }
}

#[test]
fn an_argument_count_other_than_two_fails() {
    let udf = DatetimeAddUdf::new(Unit::Days, CountArithmetic::default(), Arc::default());
    let args = ScalarFunctionArgs {
        arg_fields: vec![Arc::new(Field::new("arg0", DataType::Date32, true))],
        args: vec![ColumnarValue::Array(date("2024-01-10"))],
        number_rows: 1,
        return_field: Arc::new(Field::new(ADD_DAYS_FN, DataType::Date32, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let message = udf
        .invoke_with_args(args)
        .expect_err("one argument must fail")
        .to_string();
    assert!(message.contains("got 1"), "{message}");
}

fn scan_session() -> SessionContext {
    scan_session_context(SessionConfig::new(), Arc::default())
}

async fn collect(ctx: &SessionContext, sql: &str) -> Result<RecordBatch, DataFusionError> {
    let mut batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.remove(0))
}

/// Scenario: ADD_DAYS and ADD_WEEKS over a DATE return a DATE from the scan
/// Scenario: ADD_HOURS and ADD_MINUTES over a DATE return a timestamp from the scan
/// Scenario: The four functions over a TIMESTAMP keep its fractional seconds in the scan
#[tokio::test]
async fn the_scan_session_computes_each_rendered_call() {
    let sql = format!(
        "SELECT \
         {ADD_DAYS_FN}(CAST(DATE '2024-02-28' AS DATE), 1), \
         {ADD_WEEKS_FN}(CAST(DATE '2024-12-30' AS DATE), 1.5), \
         {ADD_HOURS_FN}(CAST(DATE '2024-01-01' AS DATE), 1.5), \
         {ADD_MINUTES_FN}(CAST(arrow_cast('2024-12-31 23:59:30.123456', \
         'Timestamp(Microsecond, None)') AS TIMESTAMP(6)), 1)"
    );
    let batch = collect(&scan_session(), &sql)
        .await
        .unwrap_or_else(|e| panic!("the rendered calls must run, got: {e}"));

    let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, None);
    let types: Vec<DataType> = batch
        .columns()
        .iter()
        .map(|c| c.data_type().clone())
        .collect();
    assert_eq!(
        types,
        [
            DataType::Date32,
            DataType::Date32,
            timestamp_type.clone(),
            timestamp_type
        ]
    );
    let values: Vec<Option<String>> = batch.columns().iter().flat_map(rendered).collect();
    assert_eq!(
        values,
        [
            Some("2024-02-29".to_string()),
            Some("2025-01-13".to_string()),
            Some("2024-01-01 02:00:00.000000".to_string()),
            Some("2025-01-01 00:00:30.123456".to_string()),
        ]
    );
}

/// Scenario: A result outside Exasol's datetime range fails the query
#[tokio::test]
async fn a_failure_is_recorded_on_the_session() {
    let ctx = scan_session();
    let sql = format!("SELECT {ADD_DAYS_FN}(CAST(DATE '9999-12-31' AS DATE), 1)");

    collect(&ctx, &sql)
        .await
        .expect_err("a result after 9999-12-31 must fail");

    let recorded = session_raised_failure(&ctx).expect("the session must hold the failure");
    assert_eq!(
        class(&recorded),
        "data exception - datetime field overflow",
        "{recorded}"
    );
}

#[tokio::test]
async fn the_session_extension_selects_the_count_arithmetic() {
    let sql = format!("SELECT {ADD_HOURS_FN}(CAST(DATE '2024-01-10' AS DATE), 277777777778)");
    let cases = [
        (None, "data exception - datetime field overflow"),
        (
            Some(CountArithmetic::Milliseconds),
            "data exception - numeric value out of range",
        ),
    ];
    for (arithmetic, expected) in cases {
        let config = arithmetic.map_or_else(SessionConfig::new, |arithmetic| {
            SessionConfig::new().with_extension(Arc::new(arithmetic))
        });
        let ctx = scan_session_context(config, Arc::default());
        collect(&ctx, &sql)
            .await
            .expect_err("a count beyond the range must fail");
        let recorded = session_raised_failure(&ctx).expect("the session must hold the failure");
        assert_eq!(class(&recorded), expected, "{arithmetic:?}: {recorded}");
    }
}
