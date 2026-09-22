//! Plan-time owner of `datafusion-scan/type-relaxation`'s supported cast-pair set — used instead of `arrow::compute::can_cast_types`, which also accepts narrowing casts no table format permits.
use arrow::datatypes::{DataType, TimeUnit};

/// `Int8`/`Int16`/`Int32` are all stored as `INT32`, so they share this decimal target base.
const INT32_SOURCE_DECIMAL_PRECISION: u8 = 10;

/// Row 11's target base for an `Int64` source, `INT64` being its physical form.
const INT64_SOURCE_DECIMAL_PRECISION: u8 = 20;

/// `None` means no row of the supported set covers either ordering — the caller reports a conflict instead of guessing.
pub fn widen(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if widens_to(left, right) {
        return Some(right.clone());
    }
    if widens_to(right, left) {
        return Some(left.clone());
    }
    None
}

fn widens_to(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Int8 | DataType::Int16, DataType::Int32 | DataType::Int64) => true,
        (DataType::Int32, DataType::Int64) => true,
        (DataType::Float32, DataType::Float64) => true,
        (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Float64) => true,
        (DataType::Decimal128(from_p, from_s), DataType::Decimal128(to_p, to_s)) => {
            widens_decimal((*from_p, *from_s), (*to_p, *to_s))
        }
        (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Decimal128(to_p, to_s)) => {
            widens_decimal((INT32_SOURCE_DECIMAL_PRECISION, 0), (*to_p, *to_s))
        }
        (DataType::Int64, DataType::Decimal128(to_p, to_s)) => {
            widens_decimal((INT64_SOURCE_DECIMAL_PRECISION, 0), (*to_p, *to_s))
        }
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, None)) => true,
        _ => false,
    }
}

/// Requires `k1 >= k2 >= 0` for `decimal(p, s)` → `decimal(p + k1, s + k2)`, forbidding the integral digit count from shrinking.
fn widens_decimal((from_p, from_s): (u8, i8), (to_p, to_s): (u8, i8)) -> bool {
    let precision_growth = i32::from(to_p) - i32::from(from_p);
    let scale_growth = i32::from(to_s) - i32::from(from_s);
    scale_growth >= 0 && precision_growth >= scale_growth
}

#[cfg(test)]
#[path = "widening_tests.rs"]
mod tests;
