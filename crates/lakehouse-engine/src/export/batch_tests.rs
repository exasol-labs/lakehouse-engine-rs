use super::*;
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use exasol_udf_sdk::test_support::TestContext;
use exasol_udf_sdk::value::{Decimal, Value};
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

fn names(fields: &[(&str, DataType)]) -> Vec<String> {
    fields.iter().map(|(name, _)| name.to_string()).collect()
}

/// The batch of `rows`, each given without the writer spec of input column 0.
fn batch_of(fields: &[(&str, DataType)], rows: Vec<Vec<Value>>) -> Result<RecordBatch, UdfError> {
    let mut builder = BatchBuilder::bind(schema(fields), &names(fields))?;
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
    let refused = BatchBuilder::bind(schema(&[("N", DataType::Int64)]), &["N".into()]);

    let message = refused.err().expect("a refusal").to_string();
    assert!(
        message.contains("table column N has the Arrow type Int64"),
        "{message}"
    );
}

#[test]
fn source_columns_bind_to_table_fields_by_name() {
    let fields = [("A", DataType::Utf8), ("B", DataType::Float64)];
    let mut builder = BatchBuilder::bind(schema(&fields), &["B".into(), "A".into()])
        .expect("both fields have a source column");
    let mut ctx = TestContext::set(vec![vec![
        Value::String(SPEC.into()),
        Value::Double(2.0),
        Value::String("a".into()),
    ]]);
    ctx.next().unwrap();

    builder.append_row(&ctx).unwrap();
    let batch = builder.take().unwrap();

    assert_eq!(column::<StringArray>(&batch, 0).value(0), "a");
    assert_eq!(column::<Float64Array>(&batch, 1).value(0), 2.0);
}

#[test]
fn a_table_field_without_a_source_column_is_refused_naming_it() {
    let refused = BatchBuilder::bind(schema(&[("MISSING", DataType::Utf8)]), &["A".into()]);

    let message = refused.err().expect("a refusal").to_string();
    assert!(message.contains("MISSING"), "{message}");
}

#[test]
fn a_batch_fills_at_its_row_limit_and_starts_empty_after_it_is_taken() {
    let fields = [("ID", DataType::Decimal128(9, 0))];
    let mut builder = BatchBuilder::bind(schema(&fields), &names(&fields)).unwrap();
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
