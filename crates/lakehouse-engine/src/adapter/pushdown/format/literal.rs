use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use vs_expression::{Expr, LiteralKind, Scalar};

use crate::adapter::pushdown::literal_binary_bytes;
use crate::types::exasol_type::ExasolType;
use crate::types::mapping::{arrow_to_exasol_type, timestamp_unit};
use vs_expression::is_session_free_integer_text;
use vs_expression::timestamp::TimestampLiteral;

use super::filter_operands::non_empty_str;

/// The time zone of a UTC-adjusted column's Arrow type, on every pruning path.
pub(super) const UTC: &str = "+00:00";

/// A literal converts only when the column's type holds it exactly, because a rounded bound
/// could prune a file holding a matching row. Each pruning path maps the value to its own type.
pub(super) fn exact_literal(literal: &Expr, column: &DataType) -> Option<ScalarValue> {
    let Expr::Literal(vs_expression::Literal { kind, value }) = literal else {
        return None;
    };
    let text = match (kind, column) {
        (LiteralKind::Bool, DataType::Boolean) => match value {
            Scalar::Bool(value) => value.to_string(),
            _ => return None,
        },
        (LiteralKind::ExactNumeric, integer) if integer.is_integer() => exact_numeral(value, 0)?,
        (
            LiteralKind::ExactNumeric,
            DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale),
        ) => exact_numeral(value, *scale)?,
        (LiteralKind::ExactNumeric | LiteralKind::Double, DataType::Float64) => {
            return double(value).map(|wide| ScalarValue::Float64(Some(wide)));
        }
        (LiteralKind::ExactNumeric | LiteralKind::Double, DataType::Float32) => {
            let wide = double(value)?;
            let narrow = wide as f32;
            return (f64::from(narrow) == wide).then_some(ScalarValue::Float32(Some(narrow)));
        }
        (LiteralKind::String, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        | (LiteralKind::Date, DataType::Date32 | DataType::Date64) => {
            non_empty_str(value)?.to_string()
        }
        (LiteralKind::String, integer) if is_integer_text(literal, integer) => {
            non_empty_str(value)?.to_string()
        }
        (LiteralKind::Timestamp, DataType::Timestamp(unit, _)) => exact_timestamp(value, *unit)?,
        (LiteralKind::Binary { .. }, DataType::Binary) => {
            return literal_binary_bytes(literal).map(|bytes| ScalarValue::Binary(Some(bytes)));
        }
        (LiteralKind::Binary { .. }, DataType::FixedSizeBinary(width)) => {
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

/// Exasol compares a digits-only text literal with an integer column as that number, and the scan
/// converts it by `is_session_free_integer_text`, so pruning converts it by the same rule.
fn is_integer_text(literal: &Expr, column: &DataType) -> bool {
    match arrow_to_exasol_type(column) {
        ExasolType::Decimal(decimal) if decimal.scale() == 0 => {
            is_session_free_integer_text(literal, decimal.precision())
        }
        _ => false,
    }
}

fn double(value: &Scalar) -> Option<f64> {
    match value {
        Scalar::Number(number) => number.as_f64(),
        Scalar::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

/// A plain `[+-]digits[.digits]` numeral whose nonzero fraction fits `scale`, with its trailing
/// fraction zeros cut, since the string cast would round a finer one.
fn exact_numeral(value: &Scalar, scale: i8) -> Option<String> {
    let numeral = match value {
        Scalar::String(numeral) => numeral.clone(),
        Scalar::Number(number) => number.to_string(),
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
fn exact_timestamp(value: &Scalar, unit: TimeUnit) -> Option<String> {
    let text = non_empty_str(value)?;
    TimestampLiteral(text)
        .fits(timestamp_unit(unit))
        .then(|| text.to_string())
}

#[cfg(test)]
#[path = "literal_tests.rs"]
mod tests;
