use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, PrimitiveArray, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, Value};

/// Native Exasol rejects out-of-domain math inputs (e.g. SQRT of a negative), so a `NaN`
/// must raise rather than coerce to `Value::Null`.
fn nan_domain_error() -> UdfError {
    UdfError::User(
        "numeric value out of range: NaN result from an out-of-domain math operation \
         (e.g. SQRT/LN/LOG of a negative number, or ACOS/ASIN outside [-1, 1])"
            .to_string(),
    )
}

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
            // Values above i64::MAX go out as Numeric.
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
            // Iceberg timestamptz emits as plain TIMESTAMP: Exasol rejects TIMESTAMP WITH LOCAL
            // TIME ZONE as a UDF EMITS type. The Arrow value is already the UTC instant.
            Value::Timestamp(timestamp_to_naive_datetime(col, row, unit)?)
        }
        DataType::Decimal128(p, s) if *p <= 36 && *s <= 36 => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw: i128 = arr.value(row);
            Value::Numeric(Decimal {
                unscaled: raw,
                scale: *s as u8,
            })
        }
        // Backstop only: incompatible types are normally pre-cast to Utf8 in the generated SQL.
        _ => {
            let display = arrow_value_to_display_string(col, row);
            Value::String(display)
        }
    })
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Splits into seconds plus remainder: an `i64` of nanoseconds spans only 1677-2262, and one of
/// microseconds drops a nanosecond column's last three digits.
fn timestamp_to_naive_datetime(
    col: &dyn Array,
    row: usize,
    unit: &TimeUnit,
) -> Result<NaiveDateTime, UdfError> {
    fn cell<T: ArrowPrimitiveType<Native = i64>>(col: &dyn Array, row: usize) -> i64 {
        col.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .value(row)
    }

    let (raw, per_second) = match unit {
        TimeUnit::Second => (cell::<TimestampSecondType>(col, row), 1),
        TimeUnit::Millisecond => (cell::<TimestampMillisecondType>(col, row), 1_000),
        TimeUnit::Microsecond => (cell::<TimestampMicrosecondType>(col, row), 1_000_000),
        TimeUnit::Nanosecond => (cell::<TimestampNanosecondType>(col, row), NANOS_PER_SECOND),
    };
    let seconds = raw.div_euclid(per_second);
    let subsecond_nanos = (raw.rem_euclid(per_second) * (NANOS_PER_SECOND / per_second)) as u32;
    DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| {
            UdfError::User(format!(
                "timestamp out of range: a {unit:?} column value {raw} (derived seconds \
                 {seconds}, sub-second nanoseconds {subsecond_nanos}) falls outside the instant \
                 range chrono::NaiveDateTime represents"
            ))
        })
}

fn arrow_value_to_display_string(col: &dyn Array, row: usize) -> String {
    arrow::util::display::array_value_to_string(col, row).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
