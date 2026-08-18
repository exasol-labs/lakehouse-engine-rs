/// Convert a full RecordBatch to a Vec of rows (each row is a Vec<Value>).
///
/// Row order matches the batch order; columns match the batch schema order.
fn batch_to_rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            values.push(arrow_value_at(batch.column(col), row).unwrap());
        }
        rows.push(values);
    }
    rows
}

use super::*;
use arrow::array::{
    BinaryArray, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder, ListBuilder,
    RecordBatch, StringBuilder, StructArray, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

// Helper: one-column RecordBatch from an Arc<dyn Array>.
fn single_col_batch(name: &str, col: Arc<dyn Array>) -> RecordBatch {
    let dt = col.data_type().clone();
    let schema = Arc::new(Schema::new(vec![Field::new(name, dt, true)]));
    RecordBatch::try_new(schema, vec![col]).unwrap()
}

/// Scenario: Arrow columns map to the correct Value variants; null → Value::Null
#[test]
fn arrow_columns_map_to_value_variants() {
    // Bool
    let mut b = BooleanBuilder::new();
    b.append_value(true);
    b.append_null();
    let batch = single_col_batch("b", Arc::new(b.finish()));
    let rows = batch_to_rows(&batch);
    assert_eq!(rows[0][0], Value::Bool(true));
    assert_eq!(rows[1][0], Value::Null);

    // Int8 → Int32
    let mut i8b = Int8Builder::new();
    i8b.append_value(127);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(i8b.finish())));
    assert_eq!(rows[0][0], Value::Int32(127));

    // Int16 → Int32
    let mut i16b = Int16Builder::new();
    i16b.append_value(1000);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(i16b.finish())));
    assert_eq!(rows[0][0], Value::Int32(1000));

    // Int32 → Int32
    let mut i32b = Int32Builder::new();
    i32b.append_value(42);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(i32b.finish())));
    assert_eq!(rows[0][0], Value::Int32(42));

    // Int64 → Int64
    let mut i64b = Int64Builder::new();
    i64b.append_value(i64::MAX);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(i64b.finish())));
    assert_eq!(rows[0][0], Value::Int64(i64::MAX));

    // UInt8 → Int32
    let mut u8b = UInt8Builder::new();
    u8b.append_value(255);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u8b.finish())));
    assert_eq!(rows[0][0], Value::Int32(255));

    // UInt16 → Int32
    let mut u16b = UInt16Builder::new();
    u16b.append_value(65535);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u16b.finish())));
    assert_eq!(rows[0][0], Value::Int32(65535));

    // UInt32 → Int64
    let mut u32b = UInt32Builder::new();
    u32b.append_value(u32::MAX);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u32b.finish())));
    assert_eq!(rows[0][0], Value::Int64(u32::MAX as i64));

    // Float32 → Double
    let mut f32b = Float32Builder::new();
    f32b.append_value(1.5_f32);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(f32b.finish())));
    assert!(matches!(rows[0][0], Value::Double(_)));

    // Float64 → Double
    let mut f64b = Float64Builder::new();
    f64b.append_value(2.5); // exact representable; avoids approx-constant lint
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(f64b.finish())));
    assert_eq!(rows[0][0], Value::Double(2.5));

    // Utf8 → String
    let mut sb = StringBuilder::new();
    sb.append_value("hello");
    sb.append_null();
    let rows = batch_to_rows(&single_col_batch("s", Arc::new(sb.finish())));
    assert_eq!(rows[0][0], Value::String("hello".into()));
    assert_eq!(rows[1][0], Value::Null);

    // LargeUtf8 → String
    let mut lsb = LargeStringBuilder::new();
    lsb.append_value("world");
    let rows = batch_to_rows(&single_col_batch("s", Arc::new(lsb.finish())));
    assert_eq!(rows[0][0], Value::String("world".into()));

    // Date32 → Value::Date
    let mut db = Date32Builder::new();
    db.append_value(0); // epoch
    let rows = batch_to_rows(&single_col_batch("d", Arc::new(db.finish())));
    assert!(matches!(rows[0][0], Value::Date(_)));

    // Decimal128 in-range → Value::Numeric
    let mut dec_builder = Decimal128Builder::new()
        .with_precision_and_scale(18, 4)
        .unwrap();
    dec_builder.append_value(1_234_567_890_000_i128);
    let rows = batch_to_rows(&single_col_batch("d", Arc::new(dec_builder.finish())));
    match &rows[0][0] {
        Value::Numeric(d) => {
            assert_eq!(d.scale, 4);
            assert_eq!(d.unscaled, 1_234_567_890_000_i128);
        }
        other => panic!("expected Numeric, got {other:?}"),
    }
}

/// Scenario: the Arrow-to-Value converter dispatches on one flat arm per Arrow type.
/// `Int64` and in-range `UInt32`/`UInt64` all produce `Value::Int64`; `UInt64` above
/// `i64::MAX` produces `Value::Numeric` of scale 0.
#[test]
fn int64_uint32_uint64_convert_identically_through_flat_arms() {
    // Int64 → Value::Int64
    let mut i64b = Int64Builder::new();
    i64b.append_value(42);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(i64b.finish())));
    assert_eq!(rows[0][0], Value::Int64(42));

    // UInt32 → Value::Int64
    let mut u32b = UInt32Builder::new();
    u32b.append_value(u32::MAX);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u32b.finish())));
    assert_eq!(rows[0][0], Value::Int64(u32::MAX as i64));

    // UInt64 at i64::MAX → Value::Int64
    let mut u64b = UInt64Builder::new();
    u64b.append_value(i64::MAX as u64);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u64b.finish())));
    assert_eq!(rows[0][0], Value::Int64(i64::MAX));

    // UInt64 above i64::MAX → Value::Numeric of scale 0
    let mut u64b_over = UInt64Builder::new();
    let over = i64::MAX as u64 + 1;
    u64b_over.append_value(over);
    let rows = batch_to_rows(&single_col_batch("x", Arc::new(u64b_over.finish())));
    match &rows[0][0] {
        Value::Numeric(d) => {
            assert_eq!(d.scale, 0);
            assert_eq!(d.unscaled, over as i128);
        }
        other => panic!("expected Numeric, got {other:?}"),
    }
}

/// Scenario: out-of-domain math (e.g. `SQRT(-1)`) produces `NaN`, which must raise a
/// domain error at the emit boundary rather than silently becoming `Value::Null`
/// (issue #199). In-domain math (e.g. `SQRT(4)`) is unaffected.
#[test]
fn nan_double_and_float_are_domain_errors() {
    // Float64: SQRT(-1.0) → NaN → Err
    let mut f64b = Float64Builder::new();
    f64b.append_value((-1.0_f64).sqrt());
    let batch = single_col_batch("x", Arc::new(f64b.finish()));
    assert!(arrow_value_at(batch.column(0), 0).is_err());

    // Float32: ASIN(2.0) → NaN → Err
    let mut f32b = Float32Builder::new();
    f32b.append_value((2.0_f32).asin());
    let batch = single_col_batch("x", Arc::new(f32b.finish()));
    assert!(arrow_value_at(batch.column(0), 0).is_err());

    // In-domain math is unaffected: SQRT(4.0) = 2.0 → Ok(Value::Double(2.0))
    let mut ok_f64b = Float64Builder::new();
    ok_f64b.append_value((4.0_f64).sqrt());
    let batch = single_col_batch("x", Arc::new(ok_f64b.finish()));
    assert_eq!(
        arrow_value_at(batch.column(0), 0).unwrap(),
        Value::Double(2.0)
    );
}

/// Scenario: Incompatible columns (list/struct/map/binary) emit Value::String JSON;
/// never array/list/struct/map Value.
#[test]
fn incompatible_columns_emit_json_strings() {
    // Binary → Value::String
    let bin_arr = BinaryArray::from_vec(vec![b"abc".as_ref()]);
    let rows = batch_to_rows(&single_col_batch("b", Arc::new(bin_arr)));
    assert!(
        matches!(&rows[0][0], Value::String(_)),
        "Binary should emit Value::String, got {:?}",
        rows[0][0]
    );

    // List<Int32> → Value::String
    let item_field = Field::new("item", DataType::Int32, true);
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.values().append_value(1);
    list_builder.values().append_value(2);
    list_builder.append(true);
    let list_arr = list_builder.finish();
    let list_dt = DataType::List(Arc::new(item_field));
    let schema = Arc::new(Schema::new(vec![Field::new("l", list_dt, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list_arr)]).unwrap();
    let rows = batch_to_rows(&batch);
    assert!(
        matches!(&rows[0][0], Value::String(_)),
        "List should emit Value::String, got {:?}",
        rows[0][0]
    );

    // Struct → Value::String (a populated field, not a zero-field placeholder).
    let mut name_builder = StringBuilder::new();
    name_builder.append_value("Berlin");
    let struct_arr = StructArray::from(vec![(
        Arc::new(Field::new("city", DataType::Utf8, false)),
        Arc::new(name_builder.finish()) as arrow::array::ArrayRef,
    )]);
    let rows = batch_to_rows(&single_col_batch("st", Arc::new(struct_arr)));
    assert!(
        matches!(&rows[0][0], Value::String(_)),
        "Struct should emit Value::String"
    );

    // Verify it is never Bool/Int32/Int64/Double/Date/Numeric for the Binary case.
    let bin_arr2 = BinaryArray::from_vec(vec![b"xyz".as_ref()]);
    let rows2 = batch_to_rows(&single_col_batch("b", Arc::new(bin_arr2)));
    let v = &rows2[0][0];
    assert!(
        !matches!(
            v,
            Value::Bool(_) | Value::Int32(_) | Value::Int64(_) | Value::Double(_)
        ),
        "incompatible type must not produce numeric/bool Value, got {v:?}"
    );
}

/// Scenario: a timezone-aware `Timestamp(Microsecond, Some("UTC"))` column (the
/// internal Arrow representation of an Iceberg timestamptz) converts to a
/// `Value::Timestamp` at the correct UTC wall-clock instant — the value Exasol
/// receives as plain TIMESTAMP.
#[test]
fn tz_aware_timestamp_converts_to_utc_instant_value() {
    // 2024-01-01T00:00:00Z, a known UTC instant, in epoch microseconds.
    let epoch_micros: i64 = 1_704_067_200_000_000;
    let arr = TimestampMicrosecondArray::from(vec![Some(epoch_micros)]).with_timezone("UTC");
    let batch = single_col_batch("ts", Arc::new(arr));
    let rows = batch_to_rows(&batch);
    let expected = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    assert_eq!(rows[0][0], Value::Timestamp(expected));
}

#[test]
fn decimal128_in_range_produces_numeric() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(18, 4),
        true,
    )]));
    let mut builder = Decimal128Builder::new()
        .with_precision_and_scale(18, 4)
        .unwrap();
    builder.append_value(1_234_567_890_000_i128); // represents 1234567.8900 (scale 4)
    let arr = builder.finish();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
    let rows = batch_to_rows(&batch);
    match &rows[0][0] {
        Value::Numeric(d) => {
            assert_eq!(d.scale, 4);
            assert_eq!(d.unscaled, 1_234_567_890_000_i128);
        }
        other => panic!("expected Numeric, got {other:?}"),
    }
}
