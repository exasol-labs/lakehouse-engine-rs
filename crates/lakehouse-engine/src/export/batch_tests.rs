use super::*;
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{Decimal, ExaType, Value};
use std::sync::Arc;

const SPEC: &str = "the writer spec";

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// The batch of `rows`, each given without the writer spec of input column 0.
fn batch_of(fields: &[(&str, DataType)], rows: Vec<Vec<Value>>) -> Result<RecordBatch, UdfError> {
    let mut builder = BatchBuilder::bind(schema(fields))?;
    let rows = rows
        .into_iter()
        .map(|row| {
            std::iter::once(Value::String(SPEC.into()))
                .chain(row)
                .collect()
        })
        .collect();
    let mut ctx = TestContext::set(rows);
    while ctx.next()? {
        builder.append_row(&ctx)?;
    }
    builder.take()
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .expect("the column has the schema's type")
}

fn timestamp(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").expect("a timestamp")
}

fn numeric(unscaled: i128, scale: u8) -> Value {
    Value::Numeric(Decimal { unscaled, scale })
}

fn error_text(result: Result<RecordBatch, UdfError>) -> String {
    result.expect_err("expected a refusal").to_string()
}

#[test]
fn each_type_and_null_is_written_with_its_arrow_type() {
    let fields = [
        ("D", DataType::Decimal128(18, 2)),
        ("F", DataType::Float64),
        ("B", DataType::Boolean),
        ("S", DataType::Utf8),
        ("DT", DataType::Date32),
        ("TS", DataType::Timestamp(TimeUnit::Microsecond, None)),
    ];
    let values = vec![
        numeric(15, 1),
        Value::Double(-1.5),
        Value::Bool(true),
        Value::String("ab   ".into()),
        Value::Date(NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()),
        Value::Timestamp(timestamp("1970-01-01 00:00:01.000002")),
    ];
    let nulls = vec![Value::Null; fields.len()];

    let batch = batch_of(&fields, vec![values, nulls]).expect("a batch");

    assert_eq!(batch.schema(), schema(&fields));
    assert_eq!(column::<Decimal128Array>(&batch, 0).value(0), 150);
    assert_eq!(column::<Float64Array>(&batch, 1).value(0), -1.5);
    assert!(column::<BooleanArray>(&batch, 2).value(0));
    assert_eq!(column::<StringArray>(&batch, 3).value(0), "ab   ");
    assert_eq!(column::<Date32Array>(&batch, 4).value(0), 1);
    assert_eq!(
        column::<TimestampMicrosecondArray>(&batch, 5).value(0),
        1_000_002
    );
    for index in 0..fields.len() {
        assert!(batch.column(index).is_null(1), "column {index}");
    }
}

#[test]
fn every_decimal_variant_is_rescaled_to_the_table_scale() {
    let fields = [
        ("I32", DataType::Decimal128(9, 0)),
        ("I64", DataType::Decimal128(18, 0)),
        ("TRIMMED", DataType::Decimal128(18, 2)),
        ("WHOLE", DataType::Decimal128(18, 2)),
        ("INT", DataType::Decimal128(12, 2)),
    ];
    let row = vec![
        Value::Int32(-7),
        Value::Int64(9_000_000_000_000_000_000),
        numeric(15, 1),
        numeric(100, 0),
        Value::Int64(3),
    ];

    let batch = batch_of(&fields, vec![row]).expect("a batch");

    let unscaled: Vec<i128> = (0..fields.len())
        .map(|index| column::<Decimal128Array>(&batch, index).value(0))
        .collect();
    assert_eq!(unscaled, [-7, 9_000_000_000_000_000_000, 150, 10_000, 300]);
}

#[test]
fn a_decimal_finer_than_the_table_scale_is_refused_naming_the_column() {
    let message = error_text(batch_of(
        &[("PRICE", DataType::Decimal128(18, 2))],
        vec![vec![numeric(1_005, 3)]],
    ));

    assert!(message.contains("column PRICE"), "{message}");
    assert!(message.contains("scale 3"), "{message}");
}

#[test]
fn the_edge_values_are_written_unchanged() {
    let fields = [
        ("DT", DataType::Date32),
        ("TS", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("D", DataType::Decimal128(36, 6)),
        ("F", DataType::Float64),
        ("S", DataType::Utf8),
    ];
    let largest_decimal = 10_i128.pow(36) - 1;
    let low = vec![
        Value::Date(NaiveDate::from_ymd_opt(1, 1, 1).unwrap()),
        Value::Timestamp(timestamp("0001-01-01 00:00:00.000001")),
        numeric(-largest_decimal, 6),
        Value::Double(f64::MIN),
        Value::String("Grüße, 世界".into()),
    ];
    let high = vec![
        Value::Date(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
        Value::Timestamp(timestamp("9999-12-31 23:59:59.999999")),
        numeric(largest_decimal, 6),
        Value::Double(f64::MAX),
        Value::String(String::new()),
    ];

    let batch = batch_of(&fields, vec![low, high]).expect("a batch");

    let dates = column::<Date32Array>(&batch, 0);
    assert_eq!(dates.value_as_date(0), NaiveDate::from_ymd_opt(1, 1, 1));
    assert_eq!(
        dates.value_as_date(1),
        NaiveDate::from_ymd_opt(9999, 12, 31)
    );
    let timestamps = column::<TimestampMicrosecondArray>(&batch, 1);
    assert_eq!(
        timestamps.value_as_datetime(0),
        Some(timestamp("0001-01-01 00:00:00.000001"))
    );
    assert_eq!(
        timestamps.value_as_datetime(1),
        Some(timestamp("9999-12-31 23:59:59.999999"))
    );
    let decimals = column::<Decimal128Array>(&batch, 2);
    assert_eq!(
        (decimals.value(0), decimals.value(1)),
        (-largest_decimal, largest_decimal)
    );
    let doubles = column::<Float64Array>(&batch, 3);
    assert_eq!((doubles.value(0), doubles.value(1)), (f64::MIN, f64::MAX));
    assert_eq!(column::<StringArray>(&batch, 4).value(0), "Grüße, 世界");
}

#[test]
fn a_value_of_another_type_is_refused_naming_the_column_and_the_variant() {
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let cases = [
        (DataType::Boolean, Value::String("yes".into()), "String"),
        (DataType::Decimal128(18, 2), Value::Double(1.5), "Double"),
        (DataType::Utf8, Value::Int32(1), "Int32"),
        (DataType::Utf8, Value::Int64(1), "Int64"),
        (DataType::Utf8, numeric(1, 0), "Numeric"),
        (DataType::Utf8, Value::Bool(true), "Bool"),
        (DataType::Utf8, Value::Date(date), "Date"),
        (DataType::Utf8, Value::Timestamp(date.into()), "Timestamp"),
    ];

    for (data_type, value, variant) in cases {
        let message = error_text(batch_of(&[("C", data_type)], vec![vec![value]]));

        assert!(
            message.contains(&format!("column C received a {variant} value")),
            "{message}"
        );
        assert!(!message.contains("yes"), "{message}");
    }
}

#[test]
fn a_decimal_too_large_for_the_table_column_is_refused_naming_the_column() {
    let message = error_text(batch_of(
        &[("BIG", DataType::Decimal128(38, 37))],
        vec![vec![numeric(10_i128.pow(36), 0)]],
    ));

    assert!(
        message.contains("column BIG holds a value too large for the table column"),
        "{message}"
    );
}

#[test]
fn a_table_field_of_an_unwritable_arrow_type_is_refused_naming_it() {
    let refused = BatchBuilder::bind(schema(&[("N", DataType::Int64)]));

    let message = refused.err().expect("a refusal").to_string();
    assert!(
        message.contains("table column N has the Arrow type Int64"),
        "{message}"
    );
}

#[test]
fn a_batch_fills_at_its_row_limit_and_starts_empty_after_it_is_taken() {
    let fields = [("ID", DataType::Decimal128(9, 0))];
    let mut builder = BatchBuilder::bind(schema(&fields)).unwrap();
    let rows = (0..BATCH_ROWS as i32)
        .map(|id| vec![Value::String(SPEC.into()), Value::Int32(id)])
        .collect();
    let mut ctx = TestContext::set(rows);

    while ctx.next().unwrap() {
        assert!(!builder.is_full());
        builder.append_row(&ctx).unwrap();
    }

    assert!(builder.is_full());
    assert_eq!(builder.take().unwrap().num_rows(), BATCH_ROWS);
    assert!(builder.is_empty());
}

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

/// Column metadata as the Rust SLC reported an input column on `exasol/docker-db:2025.1.16`,
/// for the type spellings the export's tests use. Panics on any other spelling.
pub(in crate::export) fn measured_column(type_name: &str) -> ColumnInfo {
    let digits = |prefix: &str| -> Option<Vec<u32>> {
        let args = type_name.strip_prefix(prefix)?.split(')').next()?;
        Some(
            args.split(',')
                .map(|n| n.parse().expect("number"))
                .collect(),
        )
    };
    let (typ, size, precision, scale) = if let Some(args) = digits("DECIMAL(") {
        let (precision, scale) = (args[0], args[1]);
        let typ = match (precision, scale) {
            (0..=9, 0) => ExaType::Int32,
            (10..=18, 0) => ExaType::Int64,
            _ => ExaType::Numeric { precision, scale },
        };
        (typ, 0, precision, scale)
    } else if let Some(args) = digits("TIMESTAMP(") {
        (ExaType::Timestamp { precision: args[0] }, 0, args[0], 0)
    } else if let Some(args) = digits("VARCHAR(") {
        (ExaType::String { size: args[0] }, args[0], 0, 0)
    } else if let Some(args) = digits("CHAR(") {
        (ExaType::Char { size: args[0] }, args[0], 0, 0)
    } else {
        let typ = match type_name {
            "DOUBLE" => ExaType::Double,
            "BOOLEAN" => ExaType::Boolean,
            "DATE" => ExaType::Date,
            unmeasured => panic!("no measured column metadata for {unmeasured}"),
        };
        (typ, 0, 0, 0)
    };
    ColumnInfo {
        name: "0".to_string(),
        typ,
        type_name: type_name.to_string(),
        size: Some(size),
        precision: Some(precision),
        scale: Some(scale),
    }
}
