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
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, Value};

/// Error raised when a math kernel (SQRT/LN/LOG/ACOS/ASIN/...) produces `NaN`
/// for an out-of-domain input. Native Exasol rejects these inputs outright
/// (e.g. `data exception - squareroot of a negative number`); mirroring that,
/// a `NaN` at the emit boundary must raise an error rather than silently
/// coerce to `Value::Null`.
fn nan_domain_error() -> UdfError {
    UdfError::User(
        "numeric value out of range: NaN result from an out-of-domain math operation \
         (e.g. SQRT/LN/LOG of a negative number, or ACOS/ASIN outside [-1, 1])"
            .to_string(),
    )
}

/// Convert a single Arrow column value at row `row` to an SDK Value.
///
/// Null → `Value::Null` regardless of type.
/// Incompatible or out-of-range types → `Value::String` (JSON representation).
/// A `NaN` DOUBLE/REAL value → `Err` (domain error), matching native Exasol's
/// rejection of out-of-domain math results instead of silently emitting NULL.
pub fn arrow_value_at(col: &dyn Array, row: usize) -> Result<Value, UdfError> {
    if col.is_null(row) {
        return Ok(Value::Null);
    }
    let dt = col.data_type();
    Ok(match dt {
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
            // UInt64 may overflow i64 — serialize large values via Numeric.
            let v = arr.value(row);
            if v <= i64::MAX as u64 {
                Value::Int64(v as i64)
            } else {
                Value::Numeric(Decimal {
                    unscaled: v as i128,
                    scale: 0,
                })
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
            let v = arr.value(row);
            if v.is_nan() {
                return Err(nan_domain_error());
            }
            Value::Double(v as f64)
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row);
            if v.is_nan() {
                return Err(nan_domain_error());
            }
            Value::Double(v)
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
        DataType::Timestamp(unit, _tz_opt) => {
            // Iceberg timestamptz feeds Exasol plain TIMESTAMP (Exasol rejects
            // TIMESTAMP WITH LOCAL TIME ZONE as a UDF EMITS output type). The
            // Arrow value is already the UTC instant as a NaiveDateTime, so
            // tz-aware and tz-naive timestamps emit identically.
            let raw = timestamp_to_micros(col, row, unit);
            Value::Timestamp(micros_to_naive_datetime(raw))
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
    })
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
#[path = "convert_tests.rs"]
mod tests;
