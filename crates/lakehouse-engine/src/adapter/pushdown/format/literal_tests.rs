use super::*;
use crate::adapter::pushdown::test_support::{
    binary_lit, bool_lit, date_lit, fixed_binary_lit, lit, null_lit, num_lit, str_lit,
    timestamp_lit,
};
use serde_json::json;

const TS_2024_01_15_10_30_MICROS: i64 = 1_705_314_600_000_000;

fn double_lit(value: &str) -> Json {
    lit("literal_double", value)
}

fn utc(unit: TimeUnit) -> DataType {
    DataType::Timestamp(unit, Some("+00:00".into()))
}

/// Scenario: A numeric literal converts only when the column's type holds it exactly
#[test]
fn an_integer_column_takes_an_integral_numeral_only() {
    assert_eq!(
        exact_literal(&num_lit("47"), &DataType::Int32),
        Some(ScalarValue::Int32(Some(47)))
    );
    assert_eq!(
        exact_literal(&num_lit("-47.00"), &DataType::Int64),
        Some(ScalarValue::Int64(Some(-47))),
        "zero fraction digits change no value"
    );
    for (literal, column) in [
        (num_lit("47.5"), DataType::Int64),
        (num_lit("300"), DataType::Int8),
        (num_lit("1e2"), DataType::Int64),
        (double_lit("4.7e+01"), DataType::Int64),
        (str_lit("47"), DataType::Int64),
    ] {
        assert_eq!(
            exact_literal(&literal, &column),
            None,
            "{literal} on {column}"
        );
    }
}

/// Scenario: A numeric literal converts only when the column's type holds it exactly
#[test]
fn a_decimal_column_takes_a_numeral_that_fits_its_scale_and_precision() {
    let column = DataType::Decimal128(4, 1);
    let decimal = |unscaled| Some(ScalarValue::Decimal128(Some(unscaled), 4, 1));
    assert_eq!(exact_literal(&num_lit("2.5"), &column), decimal(25));
    assert_eq!(exact_literal(&num_lit("2.50"), &column), decimal(25));
    assert_eq!(exact_literal(&num_lit("-1.5"), &column), decimal(-15));
    assert_eq!(exact_literal(&num_lit("10"), &column), decimal(100));
    for numeral in ["2.55", "1000", "1e1", ".5"] {
        assert_eq!(exact_literal(&num_lit(numeral), &column), None, "{numeral}");
    }
}

/// Scenario: A numeric literal converts only when the column's type holds it exactly
#[test]
fn a_float_column_takes_only_a_value_it_holds_exactly() {
    assert_eq!(
        exact_literal(&num_lit("0.5"), &DataType::Float32),
        Some(ScalarValue::Float32(Some(0.5)))
    );
    assert_eq!(
        exact_literal(&double_lit("5.0000000000000000e-01"), &DataType::Float32),
        Some(ScalarValue::Float32(Some(0.5)))
    );
    assert_eq!(
        exact_literal(&num_lit("0.1"), &DataType::Float32),
        None,
        "0.1 rounds to 0.10000000149 in 32 bits"
    );
    assert_eq!(
        exact_literal(&double_lit("1.0000000000000001e-01"), &DataType::Float64),
        Some(ScalarValue::Float64(Some(0.1)))
    );
    assert_eq!(
        exact_literal(&num_lit("0.1"), &DataType::Float64),
        Some(ScalarValue::Float64(Some(0.1)))
    );
}

/// Scenario: A text or time literal converts only for a column of its own kind
#[test]
fn a_string_or_date_literal_must_not_be_empty() {
    assert_eq!(
        exact_literal(&str_lit("abc"), &DataType::Utf8),
        Some(ScalarValue::Utf8(Some("abc".into())))
    );
    assert_eq!(
        exact_literal(&date_lit("2024-01-15"), &DataType::Date32),
        Some(ScalarValue::Date32(Some(19_737)))
    );
    assert_eq!(exact_literal(&str_lit(""), &DataType::Utf8), None);
    assert_eq!(exact_literal(&date_lit(""), &DataType::Date32), None);
}

/// Scenario: A text or time literal converts only for a column of its own kind
#[test]
fn a_timestamp_literal_converts_only_at_or_above_the_column_precision() {
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    assert_eq!(
        exact_literal(&timestamp_lit("2024-01-15 10:30:00.123456000"), &micros),
        Some(ScalarValue::TimestampMicrosecond(
            Some(TS_2024_01_15_10_30_MICROS + 123_456),
            None
        ))
    );
    assert_eq!(
        exact_literal(&timestamp_lit("2024-01-15 10:30:00.123456789"), &micros),
        None
    );
    assert_eq!(
        exact_literal(&timestamp_lit("2024-01-15 10:30:00.123456789"), &nanos),
        Some(ScalarValue::TimestampNanosecond(
            Some(TS_2024_01_15_10_30_MICROS * 1_000 + 123_456_789),
            None
        ))
    );
}

/// Scenario: A text or time literal converts only for a column of its own kind
#[test]
fn a_utc_adjusted_column_reads_a_timestamp_literal_as_utc() {
    assert_eq!(
        exact_literal(
            &timestamp_lit("2024-01-15 10:30:00"),
            &utc(TimeUnit::Microsecond)
        ),
        Some(ScalarValue::TimestampMicrosecond(
            Some(TS_2024_01_15_10_30_MICROS),
            Some("+00:00".into())
        ))
    );
    assert_eq!(
        exact_literal(
            &timestamp_lit("2024-01-15 10:30:00.000000001"),
            &utc(TimeUnit::Nanosecond)
        ),
        Some(ScalarValue::TimestampNanosecond(
            Some(TS_2024_01_15_10_30_MICROS * 1_000 + 1),
            Some("+00:00".into())
        ))
    );
}

/// Scenario: A text or time literal converts only for a column of its own kind
#[test]
fn a_literal_of_another_kind_than_the_column_does_not_convert() {
    for (literal, column) in [
        (
            lit("literal_timestamputc", "2024-01-15 10:30:00"),
            utc(TimeUnit::Microsecond),
        ),
        (
            date_lit("2024-01-15"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (timestamp_lit("2024-01-15 00:00:00"), DataType::Date32),
        (num_lit(5), DataType::Utf8),
        (bool_lit(true), DataType::Int32),
        (null_lit(), DataType::Utf8),
        (lit("literal_interval", json!("1-2")), DataType::Utf8),
    ] {
        assert_eq!(
            exact_literal(&literal, &column),
            None,
            "{literal} on {column}"
        );
    }
}

#[test]
fn boolean_and_binary_literals_convert_by_value() {
    assert_eq!(
        exact_literal(&bool_lit(false), &DataType::Boolean),
        Some(ScalarValue::Boolean(Some(false)))
    );
    assert_eq!(
        exact_literal(&binary_lit("00abff"), &DataType::Binary),
        Some(ScalarValue::Binary(Some(vec![0x00, 0xab, 0xff])))
    );
    assert_eq!(
        exact_literal(&fixed_binary_lit("00ab", 2), &DataType::FixedSizeBinary(2)),
        Some(ScalarValue::FixedSizeBinary(2, Some(vec![0x00, 0xab])))
    );
    assert_eq!(
        exact_literal(&fixed_binary_lit("00ab", 2), &DataType::FixedSizeBinary(3)),
        None
    );
}
