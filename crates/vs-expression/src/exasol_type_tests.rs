use super::*;

/// One row per type spelling a producer emits or Exasol sends: the type, its SQL rendering, its
/// `dataType` JSON, and whether a scan UDF can emit it.
fn renderings() -> Vec<(ExasolType, &'static str, Json, bool)> {
    let char_ascii = |size| ExasolType::Char { size, ascii: true };
    let varchar_ascii = |size| ExasolType::Varchar { size, ascii: true };
    let tsltz = |precision| ExasolType::TimestampWithLocalTimeZone { precision };
    let decimal = |precision, scale| {
        (
            ExasolType::decimal(precision, scale),
            json!({"type": "decimal", "precision": precision, "scale": scale}),
        )
    };
    let mut rows = vec![
        (
            ExasolType::Boolean,
            "BOOLEAN",
            json!({"type": "boolean"}),
            true,
        ),
        (
            ExasolType::DoublePrecision,
            "DOUBLE PRECISION",
            json!({"type": "double"}),
            true,
        ),
        (ExasolType::Date, "DATE", json!({"type": "date"}), true),
        (
            ExasolType::timestamp(0),
            "TIMESTAMP(0)",
            json!({"type": "timestamp", "fractionalSecondsPrecision": 0}),
            true,
        ),
        (
            ExasolType::timestamp(3),
            "TIMESTAMP",
            json!({"type": "timestamp"}),
            true,
        ),
        (
            ExasolType::timestamp(6),
            "TIMESTAMP(6)",
            json!({"type": "timestamp", "fractionalSecondsPrecision": 6}),
            true,
        ),
        (
            ExasolType::timestamp(9),
            "TIMESTAMP(9)",
            json!({"type": "timestamp", "fractionalSecondsPrecision": 9}),
            true,
        ),
        (
            tsltz(3),
            "TIMESTAMP WITH LOCAL TIME ZONE",
            json!({"type": "timestamp", "withLocalTimeZone": true}),
            false,
        ),
        (
            tsltz(6),
            "TIMESTAMP(6) WITH LOCAL TIME ZONE",
            json!({"type": "timestamp", "withLocalTimeZone": true, "fractionalSecondsPrecision": 6}),
            false,
        ),
        (
            ExasolType::char(1),
            "CHAR(1)",
            json!({"type": "char", "size": 1}),
            true,
        ),
        (
            ExasolType::char(3),
            "CHAR(3)",
            json!({"type": "char", "size": 3}),
            true,
        ),
        (
            ExasolType::char(20),
            "CHAR(20)",
            json!({"type": "char", "size": 20}),
            true,
        ),
        (
            ExasolType::char(2000),
            "CHAR(2000)",
            json!({"type": "char", "size": 2000}),
            true,
        ),
        (
            char_ascii(1),
            "CHAR(1) ASCII",
            json!({"type": "char", "size": 1, "characterSet": "ASCII"}),
            true,
        ),
        (
            char_ascii(3),
            "CHAR(3) ASCII",
            json!({"type": "char", "size": 3, "characterSet": "ASCII"}),
            true,
        ),
        (
            char_ascii(2000),
            "CHAR(2000) ASCII",
            json!({"type": "char", "size": 2000, "characterSet": "ASCII"}),
            true,
        ),
        (
            ExasolType::varchar(1),
            "VARCHAR(1)",
            json!({"type": "varchar", "size": 1}),
            true,
        ),
        (
            ExasolType::varchar(32),
            "VARCHAR(32)",
            json!({"type": "varchar", "size": 32}),
            true,
        ),
        (
            ExasolType::WIDEST_VARCHAR,
            "VARCHAR(2000000)",
            json!({"type": "varchar", "size": 2_000_000}),
            true,
        ),
        (
            varchar_ascii(1),
            "VARCHAR(1) ASCII",
            json!({"type": "varchar", "size": 1, "characterSet": "ASCII"}),
            true,
        ),
        (
            varchar_ascii(4),
            "VARCHAR(4) ASCII",
            json!({"type": "varchar", "size": 4, "characterSet": "ASCII"}),
            true,
        ),
        (
            varchar_ascii(2_000_000),
            "VARCHAR(2000000) ASCII",
            json!({"type": "varchar", "size": 2_000_000, "characterSet": "ASCII"}),
            true,
        ),
        (
            ExasolType::IntervalYearToMonth { precision: 2 },
            "INTERVAL YEAR(2) TO MONTH",
            json!({"type": "interval", "fromTo": "YEAR TO MONTH", "precision": 2}),
            false,
        ),
        (
            ExasolType::IntervalDayToSecond {
                precision: 2,
                fraction: 3,
            },
            "INTERVAL DAY(2) TO SECOND(3)",
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 2, "fraction": 3}),
            false,
        ),
        (
            ExasolType::IntervalDayToSecond {
                precision: 9,
                fraction: 9,
            },
            "INTERVAL DAY(9) TO SECOND(9)",
            json!({"type": "interval", "fromTo": "DAY TO SECONDS", "precision": 9, "fraction": 9}),
            false,
        ),
        (
            ExasolType::Geometry { srid: 0 },
            "GEOMETRY(0)",
            json!({"type": "geometry", "srid": 0}),
            false,
        ),
        (
            ExasolType::Geometry { srid: 4326 },
            "GEOMETRY(4326)",
            json!({"type": "geometry", "srid": 4326}),
            false,
        ),
        (
            ExasolType::Hashtype { bytes: 16 },
            "HASHTYPE(16 BYTE)",
            json!({"type": "hashtype", "bytesize": 16}),
            false,
        ),
    ];
    let decimals = [
        ((1, 0), "DECIMAL(1,0)"),
        ((3, 0), "DECIMAL(3,0)"),
        ((10, 0), "DECIMAL(10,0)"),
        ((18, 4), "DECIMAL(18,4)"),
        ((18, 6), "DECIMAL(18,6)"),
        ((20, 0), "DECIMAL(20,0)"),
        ((36, 36), "DECIMAL(36,36)"),
    ];
    rows.extend(decimals.map(|((precision, scale), sql)| {
        let (exasol_type, data_type) = decimal(precision, scale);
        (exasol_type, sql, data_type, true)
    }));
    rows
}

/// Scenario: A VARCHAR dataType renders as VARCHAR(n) and its group key is not padded
#[test]
fn display_renders_every_spelling_the_producers_emit_byte_for_byte() {
    for (exasol_type, sql, _, _) in renderings() {
        assert_eq!(exasol_type.to_string(), sql);
    }
}

#[test]
fn exasol_type_to_json_renders_each_recorded_data_type_object() {
    for (exasol_type, _, data_type, _) in renderings() {
        assert_eq!(
            exasol_type_to_json(&exasol_type),
            data_type,
            "{exasol_type}"
        );
    }
}

#[test]
fn exasol_type_to_json_round_trips_every_variant() {
    for (exasol_type, _, _, _) in renderings() {
        assert_eq!(
            valid(&exasol_type_to_json(&exasol_type)),
            exasol_type,
            "{exasol_type} must read back as itself"
        );
    }
}

#[test]
fn only_the_types_a_scan_can_emit_are_udf_emittable() {
    for (exasol_type, _, _, emittable) in renderings() {
        assert_eq!(exasol_type.is_udf_emittable(), emittable, "{exasol_type}");
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
    let decimal = ExasolType::decimal;
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
