use super::*;
use arrow::datatypes::TimeUnit;
use exasol_udf_sdk::value::ExaType;
use vs_expression::udf_column_test_support::measured_column;

fn schema_of(type_names: &[&str]) -> Result<Schema, UdfError> {
    let names: Vec<String> = (0..type_names.len()).map(|i| format!("C{i}")).collect();
    let columns: Vec<ColumnInfo> = type_names.iter().map(|t| measured_column(t)).collect();
    arrow_schema(&names, &columns)
}

fn refusal_of(type_names: &[&str]) -> String {
    match schema_of(type_names) {
        Err(error) => error.to_string(),
        Ok(schema) => panic!("{type_names:?} must be refused, got {schema:?}"),
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
        let schema = schema_of(&[type_name]).unwrap_or_else(|e| panic!("{type_name}: {e}"));
        assert_eq!(schema.field(0).data_type(), &expected, "{type_name}");
    }
}

#[test]
fn fields_keep_the_writer_spec_names_in_input_order_and_are_nullable() {
    let schema = schema_of(&["DATE", "DOUBLE", "BOOLEAN"]).expect("writable types");

    let fields: Vec<(&str, bool)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.is_nullable()))
        .collect();
    assert_eq!(fields, [("C0", true), ("C1", true), ("C2", true)]);
}

#[test]
fn a_timestamp_finer_than_microseconds_is_refused_with_a_cast_hint() {
    for type_name in ["TIMESTAMP(7)", "TIMESTAMP(9)"] {
        let message = refusal_of(&["DATE", type_name]);
        for fragment in [
            &format!("column C1 has type {type_name}, which the export cannot write"),
            "TIMESTAMP(0) to TIMESTAMP(6)",
            "cast a TIMESTAMP(7) to TIMESTAMP(9) column to TIMESTAMP(6)",
        ] {
            assert!(message.contains(fragment), "{message:?} lacks {fragment:?}");
        }
    }
}

#[test]
fn a_column_without_a_measured_spelling_is_refused_naming_it() {
    let cases = [
        ColumnInfo {
            typ: ExaType::Unsupported,
            type_name: "GEOMETRY".to_string(),
            ..measured_column("DOUBLE")
        },
        ColumnInfo {
            type_name: "TIMESTAMP(3) WITH LOCAL TIME ZONE".to_string(),
            ..measured_column("TIMESTAMP(3)")
        },
    ];
    for column in cases {
        let message = arrow_schema(&["G".to_string()], std::slice::from_ref(&column))
            .expect_err("an unmeasured spelling is refused")
            .to_string();
        assert!(
            message.contains(&format!("column G has type {}", column.type_name)),
            "{message}"
        );
    }
}
