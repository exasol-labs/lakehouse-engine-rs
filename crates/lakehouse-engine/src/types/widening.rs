//! The plan-time owner of `datafusion-scan/type-relaxation`'s supported pair set.
//!
//! The scan casts a file's narrow physical column up to the table's current logical type. WHICH
//! pairs it may cast is decided at plan time, pair by pair, by that feature's supported-set table
//! rather than by the cast. A plan-time caller that must DECIDE a column's current type — a
//! footer fold over a raw Parquet directory, which has no writer to record one — asks here rather
//! than carrying a second copy of that table.
//!
//! `arrow::compute::can_cast_types` is deliberately not consulted: it also accepts narrowing casts
//! no table format permits, which is the recorded reason the supported set is decided here.
use arrow::datatypes::{DataType, TimeUnit};

/// The decimal precision the supported set gives an `Int8`, `Int16`, or `Int32` source: all three
/// are stored as `INT32`, so row 10's target base is `decimal(10 + k1, k2)` for every one of them,
/// never a base derived from the narrower source's own range.
const INT32_SOURCE_DECIMAL_PRECISION: u8 = 10;

/// Row 11's target base for an `Int64` source: `decimal(20 + k1, k2)`, `INT64` being its physical
/// form.
const INT64_SOURCE_DECIMAL_PRECISION: u8 = 20;

/// The wider of `left` and `right` when one widens to the other under the supported set, the
/// shared type when the two are equal, and `None` when no row of that set covers either ordering.
///
/// `None` is an answer rather than a failure: the caller reports a conflict instead of guessing,
/// because a pair outside the set is one the scan could not cast a file up to.
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

/// Rows 3, 10, 11, and 12 share one condition: `decimal(p, s)` → `decimal(p + k1, s + k2)` with
/// `k1 >= k2 >= 0`. Requiring `k1 >= k2` forbids the INTEGRAL digit count shrinking, which is
/// strictly stronger than "precision and scale may both grow".
fn widens_decimal((from_p, from_s): (u8, i8), (to_p, to_s): (u8, i8)) -> bool {
    let precision_growth = i32::from(to_p) - i32::from(from_p);
    let scale_growth = i32::from(to_s) - i32::from(from_s);
    scale_growth >= 0 && precision_growth >= scale_growth
}

#[cfg(test)]
#[path = "widening_tests.rs"]
mod tests;
