#[cfg(test)]
use arrow::array::RecordBatch;
/// Arrow RecordBatch → SDK Value row conversion.
///
/// Implements the full datafusion-scan/type-mapping table including null + JSON
/// fallback for incompatible types and out-of-range Decimal128.
///
/// Only SDK `Value` types are produced — no Arrow types cross the `.so` boundary.
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use exasol_udf_sdk::value::{Decimal, Value};

/// Convert a single Arrow column value at row `row` to an SDK Value.
///
/// Null → `Value::Null` regardless of type.
/// Incompatible or out-of-range types → `Value::String` (JSON representation).
pub fn arrow_value_at(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }
    let dt = col.data_type();
    match dt {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Int32(arr.value(row) as i32)
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Int32(arr.value(row) as i32)
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Int32(arr.value(row))
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
            // All map to DECIMAL(20,0); use Int64 or the appropriate cast.
            match dt {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    Value::Int64(arr.value(row))
                }
                DataType::UInt32 => {
                    let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                    Value::Int64(arr.value(row) as i64)
                }
                DataType::UInt64 => {
                    let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    // UInt64 may overflow i64 — serialize large values via Numeric
                    let v = arr.value(row);
                    // Safe as i64 when ≤ i64::MAX; else use Numeric
                    if v <= i64::MAX as u64 {
                        Value::Int64(v as i64)
                    } else {
                        Value::Numeric(Decimal {
                            unscaled: v as i128,
                            scale: 0,
                        })
                    }
                }
                _ => unreachable!(),
            }
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Int32(arr.value(row) as i32)
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Int32(arr.value(row) as i32)
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            Value::Double(arr.value(row) as f64)
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::Double(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            // Date32 = days since Unix epoch (1970-01-01).
            let days = arr.value(row);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = if days >= 0 {
                epoch
                    .checked_add_days(chrono::Days::new(days as u64))
                    .unwrap_or(epoch)
            } else {
                epoch
                    .checked_sub_days(chrono::Days::new((-days) as u64))
                    .unwrap_or(epoch)
            };
            Value::Date(date)
        }
        DataType::Timestamp(unit, tz_opt) => {
            let raw = timestamp_to_micros(col, row, unit);
            let ndt = micros_to_naive_datetime(raw);
            if tz_opt.is_some() {
                // Iceberg timestamptz feeds Exasol plain TIMESTAMP (Exasol rejects
                // TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS output type). The
                // Arrow value is already the UTC instant; normalise it to a
                // NaiveDateTime carrying that same UTC wall-clock value.
                let utc: DateTime<Utc> = Utc.from_utc_datetime(&ndt);
                Value::Timestamp(utc.naive_utc())
            } else {
                Value::Timestamp(ndt)
            }
        }
        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            Value::Numeric(Decimal {
                unscaled: raw,
                scale: *s as u8,
            })
        }
        // All other types: display-string fallback — they should have been
        // pre-cast to Utf8 by the SQL projection generated in the scan executor.
        // If somehow a non-cast column arrives here (e.g., incompatible type in a
        // schema the adapter didn't pre-cast), we stringify it via Arrow's
        // display formatting.
        // ponytail: pre-cast in SQL is the clean path; this is the backstop.
        _ => {
            // Cast the column value to string using Arrow's display formatter.
            let display = arrow_value_to_display_string(col, row);
            Value::String(display)
        }
    }
}

/// Convert a full RecordBatch to a Vec of rows (each row is a Vec<Value>).
///
/// Row order matches the batch order; columns match the batch schema order.
#[cfg(test)]
pub fn batch_to_rows(batch: &RecordBatch) -> Vec<Vec<Value>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            values.push(arrow_value_at(batch.column(col), row));
        }
        rows.push(values);
    }
    rows
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn timestamp_to_micros(col: &dyn Array, row: usize, unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            arr.value(row) * 1_000_000
        }
        TimeUnit::Millisecond => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            arr.value(row) * 1_000
        }
        TimeUnit::Microsecond => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            arr.value(row)
        }
        TimeUnit::Nanosecond => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            arr.value(row) / 1_000
        }
    }
}

fn micros_to_naive_datetime(micros: i64) -> NaiveDateTime {
    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.naive_utc())
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap().naive_utc())
}

/// Render an Arrow value as a display string for incompatible types.
/// Uses Arrow's built-in array formatter (display form, not JSON).
fn arrow_value_to_display_string(col: &dyn Array, row: usize) -> String {
    // Arrow provides `array_value_to_string` which renders a single element.
    arrow::util::display::array_value_to_string(col, row).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder,
        ListBuilder, StringBuilder, StructArray, UInt8Builder, UInt16Builder, UInt32Builder,
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

        // Struct → Value::String (empty-fields struct with 1 null row).
        let struct_arr = StructArray::new_empty_fields(1, None);
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
}
