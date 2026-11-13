use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use serde_json::Value as Json;

use crate::adapter::pushdown::literal_binary_bytes;
use crate::types::mapping::timestamp_unit;
use vs_expression::timestamp::TimestampLiteral;

use super::filter_json::non_empty_str;

/// The time zone of a UTC-adjusted column's Arrow type, on every pruning path.
pub(super) const UTC: &str = "+00:00";

/// A literal converts only when the column's type holds it exactly, because a rounded bound
/// could prune a file holding a matching row. Each pruning path maps the value to its own type.
pub(super) fn exact_literal(literal: &Json, column: &DataType) -> Option<ScalarValue> {
    let kind = literal.get("type")?.as_str()?;
    let value = literal.get("value")?;
    let text = match (kind, column) {
        ("literal_bool", DataType::Boolean) => value.as_bool()?.to_string(),
        ("literal_exactnumeric", integer) if integer.is_integer() => exact_numeral(value, 0)?,
        (
            "literal_exactnumeric",
            DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale),
        ) => exact_numeral(value, *scale)?,
        ("literal_exactnumeric" | "literal_double", DataType::Float64) => {
            return double(value).map(|wide| ScalarValue::Float64(Some(wide)));
        }
        ("literal_exactnumeric" | "literal_double", DataType::Float32) => {
            let wide = double(value)?;
            let narrow = wide as f32;
            return (f64::from(narrow) == wide).then_some(ScalarValue::Float32(Some(narrow)));
        }
        ("literal_string", DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        | ("literal_date", DataType::Date32 | DataType::Date64) => {
            non_empty_str(value)?.to_string()
        }
        ("literal_timestamp", DataType::Timestamp(unit, _)) => exact_timestamp(value, *unit)?,
        ("literal_binary", DataType::Binary) => {
            return literal_binary_bytes(literal).map(|bytes| ScalarValue::Binary(Some(bytes)));
        }
        ("literal_binary", DataType::FixedSizeBinary(width)) => {
            return literal_binary_bytes(literal)
                .filter(|bytes| usize::try_from(*width).ok() == Some(bytes.len()))
                .map(|bytes| ScalarValue::FixedSizeBinary(*width, Some(bytes)));
        }
        _ => return None,
    };
    ScalarValue::try_from_string(text, column)
        .ok()
        .filter(|scalar| !scalar.is_null())
}

fn double(value: &Json) -> Option<f64> {
    match value {
        Json::Number(number) => number.as_f64(),
        Json::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

/// A plain `[+-]digits[.digits]` numeral whose nonzero fraction fits `scale`, with its trailing
/// fraction zeros cut, since the string cast would round a finer one.
fn exact_numeral(value: &Json, scale: i8) -> Option<String> {
    let numeral = match value {
        Json::String(numeral) => numeral.clone(),
        Json::Number(number) => number.to_string(),
        _ => return None,
    };
    let unsigned = numeral.strip_prefix(['+', '-']).unwrap_or(&numeral);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let is_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !is_digits(whole) || !is_digits(fraction) {
        return None;
    }
    let significant = fraction.trim_end_matches('0');
    if significant.len() > usize::try_from(scale).ok()? {
        return None;
    }
    let sign_and_whole = &numeral[..numeral.len() - unsigned.len() + whole.len()];
    Some(if significant.is_empty() {
        sign_and_whole.to_string()
    } else {
        format!("{sign_and_whole}.{significant}")
    })
}

/// The string cast truncates a fraction finer than `unit`, so such a literal does not convert.
fn exact_timestamp(value: &Json, unit: TimeUnit) -> Option<String> {
    let text = non_empty_str(value)?;
    TimestampLiteral(text)
        .fits(timestamp_unit(unit))
        .then(|| text.to_string())
}

#[cfg(test)]
#[path = "literal_tests.rs"]
mod tests;
