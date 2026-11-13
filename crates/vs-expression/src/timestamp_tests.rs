use super::*;

#[test]
fn holding_picks_the_coarsest_unit_that_keeps_every_digit() {
    use TimestampUnit::*;
    let expected = [
        Second,
        Millisecond,
        Millisecond,
        Millisecond,
        Microsecond,
        Microsecond,
        Microsecond,
        Nanosecond,
        Nanosecond,
        Nanosecond,
    ];
    for (precision, unit) in (0u8..).zip(expected) {
        assert_eq!(
            TimestampUnit::holding(precision),
            unit,
            "precision {precision}"
        );
        assert!(unit.digits() >= precision, "precision {precision}");
    }
}

#[test]
fn a_literal_types_by_every_spelled_digit_and_fits_by_its_significant_ones() {
    let cases = [
        ("2024-01-01 00:00:00", 0, 0),
        ("2024-01-01 00:00:00.1200", 4, 2),
        ("2024-01-01 00:00:00.000000", 6, 0),
        ("2024-01-01 00:00:00.0000001", 7, 7),
    ];
    for (value, type_precision, significant) in cases {
        let literal = TimestampLiteral(value);
        assert_eq!(literal.type_precision(), type_precision, "{value}");
        assert_eq!(literal.significant_digits(), significant, "{value}");
    }
    let literal = TimestampLiteral("2024-01-01 00:00:00.1234000");
    assert!(literal.fits(TimestampUnit::Microsecond));
    assert!(!literal.fits(TimestampUnit::Millisecond));
}

#[test]
fn floor_cuts_only_a_literal_finer_than_the_precision() {
    let cases = [
        (
            "2024-01-01 00:00:00.1234567",
            3,
            Some("2024-01-01 00:00:00.123"),
        ),
        ("2024-01-01 00:00:00.9", 0, Some("2024-01-01 00:00:00")),
        ("2024-01-01 00:00:00.123000", 3, None),
        ("2024-01-01 00:00:00", 0, None),
    ];
    for (value, precision, expected) in cases {
        assert_eq!(
            TimestampLiteral(value).floor(precision).as_deref(),
            expected,
            "{value} at {precision}"
        );
    }
}
