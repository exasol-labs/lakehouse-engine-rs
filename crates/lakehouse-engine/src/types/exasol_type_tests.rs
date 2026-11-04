use super::*;

#[test]
fn display_renders_every_spelling_the_producers_emit_byte_for_byte() {
    let spellings = [
        (ExasolType::Boolean, "BOOLEAN"),
        (ExasolType::decimal(3, 0), "DECIMAL(3,0)"),
        (ExasolType::decimal(10, 0), "DECIMAL(10,0)"),
        (ExasolType::decimal(20, 0), "DECIMAL(20,0)"),
        (ExasolType::decimal(18, 6), "DECIMAL(18,6)"),
        (ExasolType::decimal(36, 36), "DECIMAL(36,36)"),
        (ExasolType::DoublePrecision, "DOUBLE PRECISION"),
        (ExasolType::Date, "DATE"),
        (ExasolType::Timestamp { precision: None }, "TIMESTAMP"),
        (ExasolType::timestamp(3), "TIMESTAMP(3)"),
        (ExasolType::timestamp(6), "TIMESTAMP(6)"),
        (ExasolType::timestamp(9), "TIMESTAMP(9)"),
        (
            ExasolType::TimestampWithLocalTimeZone,
            "TIMESTAMP WITH LOCAL TIME ZONE",
        ),
        (ExasolType::char(20), "CHAR(20)"),
        (
            ExasolType::Char {
                size: 2000,
                ascii: true,
            },
            "CHAR(2000) ASCII",
        ),
        (ExasolType::varchar(32), "VARCHAR(32)"),
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
