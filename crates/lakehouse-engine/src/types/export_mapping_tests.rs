use super::*;
use crate::types::exasol_type::{UdfColumnType, exasol_type_from_column_info};
use crate::types::mapping::arrow_to_exasol_type;
use vs_expression::udf_column_test_support::measured_column;

/// The Exasol type of a source column that Exasol describes as `type_name`.
fn exasol(type_name: &str) -> ExasolType {
    match exasol_type_from_column_info(&measured_column(type_name)) {
        UdfColumnType::Known(exasol_type) => exasol_type,
        UdfColumnType::Unrecognized { .. } => panic!("{type_name} is not a measured spelling"),
    }
}

#[test]
fn each_writable_type_maps_to_its_arrow_type() {
    let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
    let cases = [
        ("DECIMAL(9,0)", DataType::Decimal128(9, 0)),
        ("DECIMAL(18,0)", DataType::Decimal128(18, 0)),
        ("DECIMAL(36,6)", DataType::Decimal128(36, 6)),
        ("DOUBLE", DataType::Float64),
        ("BOOLEAN", DataType::Boolean),
        ("VARCHAR(20) UTF8", DataType::Utf8),
        ("VARCHAR(10) ASCII", DataType::Utf8),
        ("CHAR(5) UTF8", DataType::Utf8),
        ("DATE", DataType::Date32),
        ("TIMESTAMP(0)", micros.clone()),
        ("TIMESTAMP(3)", micros.clone()),
        ("TIMESTAMP(6)", micros),
    ];
    for (type_name, expected) in cases {
        assert_eq!(
            exasol_to_arrow(&exasol(type_name)),
            Some(expected),
            "{type_name}"
        );
    }
}

#[test]
fn read_mapping_round_trips_are_writable() {
    let candidates = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Decimal128(9, 2),
        DataType::Decimal128(36, 0),
        DataType::Decimal128(38, 10),
    ];
    let mut exact = 0;

    for column in candidates {
        let source = arrow_to_exasol_type(&column);
        if exasol_to_arrow(&source).as_ref() != Some(&column) {
            continue;
        }
        exact += 1;
        assert!(writable(&column, &source), "{column} from {source:?}");
    }
    assert_eq!(exact, 7, "the read side maps seven candidates exactly");
}
