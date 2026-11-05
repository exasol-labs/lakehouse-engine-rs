use super::*;

/// Scenario: A VARCHAR dataType renders as VARCHAR(n) and its group key is not padded
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
        (ExasolType::timestamp(0), "TIMESTAMP(0)"),
        (ExasolType::timestamp(3), "TIMESTAMP"),
        (ExasolType::timestamp(6), "TIMESTAMP(6)"),
        (ExasolType::timestamp(9), "TIMESTAMP(9)"),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 3 },
            "TIMESTAMP WITH LOCAL TIME ZONE",
        ),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 6 },
            "TIMESTAMP(6) WITH LOCAL TIME ZONE",
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
        (
            ExasolType::IntervalYearToMonth { precision: 2 },
            "INTERVAL YEAR(2) TO MONTH",
        ),
        (
            ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 3,
            },
            "INTERVAL DAY(2) TO SECOND(3)",
        ),
        (ExasolType::Geometry { srid: 0 }, "GEOMETRY(0)"),
        (ExasolType::Hashtype { bytes: 16 }, "HASHTYPE(16 BYTE)"),
    ];
    for (exasol_type, rendered) in spellings {
        assert_eq!(exasol_type.to_string(), rendered);
    }
}

#[test]
fn decimal_new_rejects_what_exasol_rejects() {
    let cases = [
        (0, 0, false),
        (1, 0, true),
        (1, 1, true),
        (1, 2, false),
        (18, 6, true),
        (36, 0, true),
        (36, 36, true),
        (37, 0, false),
        (u8::MAX, 0, false),
        (10, u8::MAX, false),
    ];
    for (precision, scale, accepted) in cases {
        let decimal = Decimal::new(precision, scale);
        assert_eq!(decimal.is_some(), accepted, "DECIMAL({precision},{scale})");
        if let Some(decimal) = decimal {
            assert_eq!((decimal.precision(), decimal.scale()), (precision, scale));
        }
    }
}

/// Scenario: A pushdown request with a missing or invalid Exasol type fails the query
#[test]
fn exasol_type_from_json_refuses_an_incomplete_or_out_of_range_type() {
    let decimal =
        |precision: u8, scale: u8| ExasolType::Decimal(Decimal::new(precision, scale).unwrap());
    let cases = [
        (
            json!({"type": "decimal", "precision": 1, "scale": 0}),
            Some(decimal(1, 0)),
        ),
        (json!({"type": "decimal", "precision": 0, "scale": 0}), None),
        (
            json!({"type": "decimal", "precision": 36, "scale": 36}),
            Some(decimal(36, 36)),
        ),
        (
            json!({"type": "decimal", "precision": 37, "scale": 0}),
            None,
        ),
        (
            json!({"type": "decimal", "precision": 10, "scale": 11}),
            None,
        ),
        (
            json!({"type": "decimal", "precision": 10, "scale": -1}),
            None,
        ),
        (json!({"type": "decimal", "precision": 10}), None),
        (json!({"type": "decimal", "scale": 0}), None),
        (json!({"type": "decimal"}), None),
        (
            json!({"type": "decimal", "precision": 300, "scale": 2}),
            None,
        ),
        (
            json!({"type": "char", "size": 1}),
            Some(ExasolType::char(1)),
        ),
        (json!({"type": "char", "size": 0}), None),
        (
            json!({"type": "char", "size": 2000}),
            Some(ExasolType::char(2000)),
        ),
        (json!({"type": "char", "size": 2001}), None),
        (json!({"type": "char"}), None),
        (json!({"type": "char", "characterSet": "ASCII"}), None),
        (
            json!({"type": "varchar", "size": 1}),
            Some(ExasolType::varchar(1)),
        ),
        (json!({"type": "varchar", "size": 0}), None),
        (
            json!({"type": "varchar", "size": 2_000_000}),
            Some(ExasolType::WIDEST_VARCHAR),
        ),
        (json!({"type": "varchar", "size": 2_000_001}), None),
        (json!({"type": "varchar"}), None),
        (
            json!({"type": "varchar", "size": 4, "characterSet": "LATIN1"}),
            None,
        ),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": 0}),
            Some(ExasolType::timestamp(0)),
        ),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": 9}),
            Some(ExasolType::timestamp(9)),
        ),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": 10}),
            None,
        ),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": -1}),
            None,
        ),
        (
            json!({"type": "timestamp", "withLocalTimeZone": true, "fractionalSecondsPrecision": 10}),
            None,
        ),
        (
            json!({"type": "timestamp", "withLocalTimeZone": "yes"}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 1}),
            Some(ExasolType::IntervalYearToMonth { precision: 1 }),
        ),
        (
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 0}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 9}),
            Some(ExasolType::IntervalYearToMonth { precision: 9 }),
        ),
        (
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 10}),
            None,
        ),
        (json!({"type": "interval", "fromTo": "YEAR TO MONTH"}), None),
        (json!({"type": "interval", "precision": 2}), None),
        (
            json!({"type": "interval", "fromTo": "HOUR TO MINUTE", "precision": 2}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 0}),
            Some(ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 0,
            }),
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 9}),
            Some(ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 9,
            }),
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 10}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 0, "fraction": 3}),
            None,
        ),
        (
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 10, "fraction": 3}),
            None,
        ),
        (
            json!({"type": "hashtype", "bytesize": 1}),
            Some(ExasolType::Hashtype { bytes: 1 }),
        ),
        (json!({"type": "hashtype", "bytesize": 0}), None),
        (
            json!({"type": "hashtype", "bytesize": 1024}),
            Some(ExasolType::Hashtype { bytes: 1024 }),
        ),
        (json!({"type": "hashtype", "bytesize": 1025}), None),
        (json!({"type": "hashtype"}), None),
        (
            json!({"type": "geometry", "srid": 0}),
            Some(ExasolType::Geometry { srid: 0 }),
        ),
        (
            json!({"type": "geometry", "srid": u64::from(u32::MAX) + 1}),
            None,
        ),
        (json!({"type": "geometry"}), None),
        (json!({"type": "unsupported_target"}), None),
        (json!({"size": 4}), None),
        (json!("varchar"), None),
    ];
    for (data_type, expected) in cases {
        match (exasol_type_from_json(&data_type), expected) {
            (Ok(read), Some(expected)) => assert_eq!(read, expected, "{data_type}"),
            (Err(UdfError::User(message)), None) => assert!(
                message.contains(&data_type.to_string()),
                "the error must name the offending dataType {data_type}: {message}"
            ),
            (outcome, expected) => panic!("{data_type}: got {outcome:?}, expected {expected:?}"),
        }
    }
}

#[test]
fn exasol_type_from_json_states_exasols_decimal_range_for_an_oversized_field() {
    let cases = [
        (
            json!({"type": "decimal", "precision": 300, "scale": 2}),
            "`precision` must be an integer from 1 to 36",
        ),
        (
            json!({"type": "decimal", "precision": 10, "scale": 300}),
            "`scale` must be an integer from 0 to 36",
        ),
    ];
    for (data_type, constraint) in cases {
        match exasol_type_from_json(&data_type) {
            Err(UdfError::User(message)) => assert!(
                message.contains(constraint),
                "{data_type}: the error must state {constraint:?}: {message}"
            ),
            outcome => panic!("{data_type}: got {outcome:?}, expected a user error"),
        }
    }
}

/// A `dataType` the test knows to be valid.
fn valid(data_type: &Json) -> ExasolType {
    exasol_type_from_json(data_type).unwrap_or_else(|e| panic!("{data_type}: {e:?}"))
}

#[test]
fn exasol_type_from_json_reads_each_wire_form() {
    let tsltz = |precision| ExasolType::TimestampWithLocalTimeZone { precision };
    let cases = [
        // Exasol 8.x omits `fractionalSecondsPrecision` for its only precision, 3.
        (json!({"type": "timestamp"}), ExasolType::BARE_TIMESTAMP),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": 3}),
            ExasolType::BARE_TIMESTAMP,
        ),
        (
            json!({"type": "timestamp", "fractionalSecondsPrecision": 6}),
            ExasolType::timestamp(6),
        ),
        (
            json!({"type": "TIMESTAMP", "withLocalTimeZone": false, "fractionalSecondsPrecision": 6}),
            ExasolType::timestamp(6),
        ),
        (
            json!({"type": "timestamp", "withLocalTimeZone": true}),
            tsltz(3),
        ),
        (
            json!({"type": "timestamp", "withLocalTimeZone": true, "fractionalSecondsPrecision": 3}),
            tsltz(3),
        ),
        (
            json!({"type": "TIMESTAMP", "withLocalTimeZone": true, "fractionalSecondsPrecision": 7}),
            tsltz(7),
        ),
        (
            json!({"type": "INTERVAL", "fromTo": "YEAR TO MONTH", "precision": 2}),
            ExasolType::IntervalYearToMonth { precision: 2 },
        ),
        (
            json!({"type": "INTERVAL", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 3}),
            ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 3,
            },
        ),
        (
            json!({"type": "GEOMETRY", "srid": 4326}),
            ExasolType::Geometry { srid: 4326 },
        ),
        // `HASHTYPE(8 BIT)` arrives as one byte.
        (
            json!({"type": "HASHTYPE", "bytesize": 1}),
            ExasolType::Hashtype { bytes: 1 },
        ),
        // Exasol sends `characterSet` only for ASCII; UTF8 and an absent set are the default.
        (
            json!({"type": "VARCHAR", "size": 4, "characterSet": "ASCII"}),
            ExasolType::Varchar {
                size: 4,
                ascii: true,
            },
        ),
        (
            json!({"type": "VARCHAR", "size": 4}),
            ExasolType::varchar(4),
        ),
        (
            json!({"type": "CHAR", "size": 3, "characterSet": "ascii"}),
            ExasolType::Char {
                size: 3,
                ascii: true,
            },
        ),
        (
            json!({"type": "CHAR", "size": 20, "characterSet": "UTF8"}),
            ExasolType::char(20),
        ),
        (json!({"type": "CHAR", "size": 20}), ExasolType::char(20)),
    ];
    for (data_type, expected) in cases {
        assert_eq!(valid(&data_type), expected, "{data_type}");
    }
}

#[test]
fn only_the_types_a_scan_can_emit_are_udf_emittable() {
    let cases = [
        (ExasolType::Boolean, true),
        (ExasolType::decimal(18, 2), true),
        (ExasolType::DoublePrecision, true),
        (ExasolType::Date, true),
        (ExasolType::timestamp(6), true),
        (ExasolType::char(20), true),
        (
            ExasolType::Char {
                size: 3,
                ascii: true,
            },
            true,
        ),
        (ExasolType::varchar(10), true),
        (ExasolType::WIDEST_VARCHAR, true),
        (
            ExasolType::Varchar {
                size: 2_000_000,
                ascii: true,
            },
            true,
        ),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 3 },
            false,
        ),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 6 },
            false,
        ),
        (ExasolType::IntervalYearToMonth { precision: 2 }, false),
        (
            ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 3,
            },
            false,
        ),
        (ExasolType::Geometry { srid: 0 }, false),
        (ExasolType::Hashtype { bytes: 16 }, false),
    ];
    for (exasol_type, emittable) in cases {
        assert_eq!(exasol_type.is_udf_emittable(), emittable, "{exasol_type}");
    }
}

fn every_variant() -> Vec<ExasolType> {
    let mut variants = vec![
        ExasolType::Boolean,
        ExasolType::DoublePrecision,
        ExasolType::Date,
        ExasolType::WIDEST_VARCHAR,
        ExasolType::IntervalYearToMonth { precision: 2 },
        ExasolType::IntervalDayToSecond {
            precision: 9,
            fraction: 9,
        },
        ExasolType::Geometry { srid: 4326 },
        ExasolType::Hashtype { bytes: 16 },
    ];
    variants.extend([0, 3, 6, 9].map(ExasolType::timestamp));
    variants.extend([3, 6].map(|precision| ExasolType::TimestampWithLocalTimeZone { precision }));
    variants.extend(
        [(1, 0), (10, 0), (18, 6), (36, 36)]
            .map(|(precision, scale)| ExasolType::Decimal(Decimal::new(precision, scale).unwrap())),
    );
    for ascii in [false, true] {
        variants.push(ExasolType::Char { size: 1, ascii });
        variants.push(ExasolType::Char { size: 2000, ascii });
        variants.push(ExasolType::Varchar { size: 1, ascii });
        variants.push(ExasolType::Varchar {
            size: 2_000_000,
            ascii,
        });
    }
    variants
}

#[test]
fn exasol_type_to_json_round_trips_every_variant() {
    for exasol_type in every_variant() {
        assert_eq!(
            valid(&exasol_type_to_json(&exasol_type)),
            exasol_type,
            "{exasol_type} must read back as itself"
        );
    }
}

#[test]
fn exasol_type_to_json_renders_each_recorded_data_type_object() {
    let cases = [
        (ExasolType::Boolean, json!({"type": "boolean"})),
        (ExasolType::DoublePrecision, json!({"type": "double"})),
        (ExasolType::Date, json!({"type": "date"})),
        (
            ExasolType::decimal(18, 4),
            json!({"type": "decimal", "precision": 18, "scale": 4}),
        ),
        (ExasolType::timestamp(3), json!({"type": "timestamp"})),
        (
            ExasolType::timestamp(6),
            json!({"type": "timestamp", "fractionalSecondsPrecision": 6}),
        ),
        (
            ExasolType::timestamp(0),
            json!({"type": "timestamp", "fractionalSecondsPrecision": 0}),
        ),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 3 },
            json!({"type": "timestamp", "withLocalTimeZone": true}),
        ),
        (
            ExasolType::TimestampWithLocalTimeZone { precision: 6 },
            json!({"type": "timestamp", "withLocalTimeZone": true, "fractionalSecondsPrecision": 6}),
        ),
        (
            ExasolType::WIDEST_VARCHAR,
            json!({"type": "varchar", "size": 2_000_000}),
        ),
        (
            ExasolType::Varchar {
                size: 4,
                ascii: true,
            },
            json!({"type": "varchar", "size": 4, "characterSet": "ASCII"}),
        ),
        (ExasolType::char(3), json!({"type": "char", "size": 3})),
        (
            ExasolType::Char {
                size: 3,
                ascii: true,
            },
            json!({"type": "char", "size": 3, "characterSet": "ASCII"}),
        ),
        (
            ExasolType::IntervalYearToMonth { precision: 2 },
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 2}),
        ),
        (
            ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 3,
            },
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 3}),
        ),
        (
            ExasolType::Geometry { srid: 4326 },
            json!({"type": "geometry", "srid": 4326}),
        ),
        (
            ExasolType::Hashtype { bytes: 16 },
            json!({"type": "hashtype", "bytesize": 16}),
        ),
    ];
    for (exasol_type, expected) in cases {
        assert_eq!(exasol_type_to_json(&exasol_type), expected, "{exasol_type}");
    }
}
