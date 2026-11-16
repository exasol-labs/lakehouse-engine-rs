use super::*;
use arrow::array::Array;
use arrow::datatypes::{TimestampMicrosecondType, TimestampNanosecondType};

const SECONDS: &str = "%Y-%m-%d %H:%M:%S";
const FRACTION: &str = "%Y-%m-%d %H:%M:%S%.f";

fn parsed(text: &str, pattern: &str, digits: u8) -> std::result::Result<String, String> {
    let items = StrftimeItems::new(pattern)
        .parse()
        .expect("a valid pattern");
    parse_timestamp(text, &items, digits)
        .map(|value| value.format("%Y-%m-%d %H:%M:%S%.9f").to_string())
}

#[test]
fn reads_text_as_exasol_does() {
    let cases = [
        ("9999-12-31", "%Y-%m-%d", 0, "9999-12-31 00:00:00.000000000"),
        ("0001-01-01", "%Y-%m-%d", 0, "0001-01-01 00:00:00.000000000"),
        (
            "1600-01-01 00:00:00",
            SECONDS,
            0,
            "1600-01-01 00:00:00.000000000",
        ),
        ("2024-01-31", SECONDS, 0, "2024-01-31 00:00:00.000000000"),
        ("2024-01-31 10", SECONDS, 0, "2024-01-31 10:00:00.000000000"),
        (
            "2024-01-31 10:11:",
            SECONDS,
            0,
            "2024-01-31 10:11:00.000000000",
        ),
        ("2024-01-31 ", SECONDS, 0, "2024-01-31 00:00:00.000000000"),
        (
            "2024-01-31 10:11:12",
            FRACTION,
            6,
            "2024-01-31 10:11:12.000000000",
        ),
        (
            "2024-01-31 10:11:12.5",
            FRACTION,
            6,
            "2024-01-31 10:11:12.500000000",
        ),
        (
            "2024-01-31 10:11:12.1234567",
            FRACTION,
            6,
            "2024-01-31 10:11:12.123456000",
        ),
        (
            "2024-01-31 10:11:12.123456789",
            FRACTION,
            9,
            "2024-01-31 10:11:12.123456789",
        ),
        (
            "2024-01-31 10:11:12.123456",
            FRACTION,
            3,
            "2024-01-31 10:11:12.123000000",
        ),
        (
            "1960-01-01 00:00:00.999999",
            FRACTION,
            3,
            "1960-01-01 00:00:00.999000000",
        ),
    ];
    for (text, pattern, digits, expected) in cases {
        assert_eq!(
            parsed(text, pattern, digits).as_deref(),
            Ok(expected),
            "'{text}' over '{pattern}' keeping {digits} digits"
        );
    }
}

#[test]
fn fails_where_exasol_fails() {
    let cases = [
        ("2024-01", "%Y-%m-%d", INVALID_VALUE),
        ("2024-01-31 10:11:12", "%Y-%m-%d", INVALID_VALUE),
        ("event-01", "%Y-%m-%d", INVALID_VALUE),
        ("0000-01-01", "%Y-%m-%d", INVALID_DATE),
        ("10000-01-01", "%Y-%m-%d", INVALID_VALUE),
    ];
    for (text, pattern, class) in cases {
        let failure = parsed(text, pattern, 0).expect_err(text);
        assert!(failure.starts_with(class), "'{text}': {failure}");
        assert!(
            failure.contains(text),
            "the failure names the text: {failure}"
        );
    }
}

#[test]
fn keeps_up_to_six_digits_in_microseconds_and_more_in_nanoseconds() {
    let texts: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
        Some("9999-12-31 23:59:59.123456"),
        None,
    ]));
    let micros = ToTimestampUdf::convert(&texts, FRACTION, 6).expect("microseconds span 9999");
    let micros = micros.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(micros.value(0), 253_402_300_799_123_456);
    assert!(micros.is_null(1));

    let failure =
        ToTimestampUdf::convert(&texts, FRACTION, 9).expect_err("nanoseconds end in 2262");
    assert!(failure.starts_with(DATETIME_OVERFLOW), "{failure}");

    let near: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![
        "2024-01-31 10:11:12.123456789",
    ]));
    let nanos = ToTimestampUdf::convert(&near, FRACTION, 9).expect("inside the nanosecond range");
    assert_eq!(
        nanos.as_primitive::<TimestampNanosecondType>().value(0),
        1_706_695_872_123_456_789
    );
}
