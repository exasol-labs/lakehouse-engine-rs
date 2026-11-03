use super::*;

fn decimal(precision: u8, scale: i8) -> ExasolType {
    ExasolType::Decimal { precision, scale }
}

fn varchar(size: u32) -> ExasolType {
    ExasolType::Varchar { size, ascii: false }
}

#[test]
fn display_renders_every_spelling_the_producers_emit_byte_for_byte() {
    let spellings = [
        (ExasolType::Boolean, "BOOLEAN"),
        (decimal(3, 0), "DECIMAL(3,0)"),
        (decimal(10, 0), "DECIMAL(10,0)"),
        (decimal(20, 0), "DECIMAL(20,0)"),
        (decimal(18, 6), "DECIMAL(18,6)"),
        (decimal(36, 36), "DECIMAL(36,36)"),
        (decimal(5, -2), "DECIMAL(5,-2)"),
        (ExasolType::DoublePrecision, "DOUBLE PRECISION"),
        (ExasolType::Date, "DATE"),
        (ExasolType::Timestamp { precision: None }, "TIMESTAMP"),
        (ExasolType::Timestamp { precision: Some(6) }, "TIMESTAMP(6)"),
        (ExasolType::Timestamp { precision: Some(9) }, "TIMESTAMP(9)"),
        (
            ExasolType::TimestampWithLocalTimeZone,
            "TIMESTAMP WITH LOCAL TIME ZONE",
        ),
        (
            ExasolType::Char {
                size: 20,
                ascii: false,
            },
            "CHAR(20)",
        ),
        (
            ExasolType::Char {
                size: 2000,
                ascii: true,
            },
            "CHAR(2000) ASCII",
        ),
        (varchar(32), "VARCHAR(32)"),
        (ExasolType::WIDEST_VARCHAR, "VARCHAR(2000000)"),
        (
            ExasolType::Varchar {
                size: 10,
                ascii: true,
            },
            "VARCHAR(10) ASCII",
        ),
    ];
    for (exasol_type, rendered) in spellings {
        assert_eq!(exasol_type.to_string(), rendered);
    }
}

#[test]
fn bare_timestamp_and_timestamp_three_are_different_declarations() {
    assert_ne!(
        ExasolType::Timestamp { precision: None }.to_string(),
        ExasolType::Timestamp { precision: Some(3) }.to_string()
    );
}

#[test]
fn character_family_is_char_and_varchar_only() {
    assert!(varchar(5).is_character());
    assert!(ExasolType::WIDEST_VARCHAR.is_character());
    assert!(
        ExasolType::Char {
            size: 5,
            ascii: true
        }
        .is_character()
    );
    for other in [
        ExasolType::Boolean,
        decimal(10, 0),
        ExasolType::DoublePrecision,
        ExasolType::Date,
        ExasolType::Timestamp { precision: None },
        ExasolType::TimestampWithLocalTimeZone,
    ] {
        assert!(!other.is_character(), "{other}");
    }
}

#[test]
fn decimal_family_is_every_decimal_and_nothing_else() {
    assert!(decimal(10, 0).is_decimal());
    assert!(decimal(36, 36).is_decimal());
    assert!(decimal(5, -2).is_decimal());
    for other in [
        ExasolType::DoublePrecision,
        ExasolType::Date,
        varchar(5),
        ExasolType::Boolean,
    ] {
        assert!(!other.is_decimal(), "{other}");
    }
}
