use arrow::datatypes::{DataType as ArrowType, TimeUnit};
use datafusion::scalar::ScalarValue;
use delta_kernel::expressions::{DecimalData, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, StructField};
use delta_kernel::{Expression, Predicate};
use serde_json::Value as Json;

use super::filter_json::Comparison;
use super::literal::UTC;
use super::pruning_filter::{PruningFilter, StatisticsTarget};

/// Each declared column's name paired with the current snapshot field its binding key names.
type FilterColumns<'s> = [(&'s str, &'s StructField)];

/// `None` means no constraint (pass all files, never none): an untranslatable node must
/// widen the surviving file set, never narrow it. A filter column is found among `columns` by
/// its declared name and emitted under its field's current name; one missing from `columns`
/// constrains nothing.
pub(crate) fn to_delta_predicate(filter_json: &Json, columns: &FilterColumns) -> Option<Predicate> {
    let resolve = |column: &str| {
        let (name, primitive) = resolve_column(column, columns)?;
        Some((name.to_string(), arrow_type(primitive)))
    };
    PruningFilter::parse(filter_json, &resolve)
        .to_statistics(&DeltaTarget)
        .map(|translated| translated.predicate)
}

/// Builds kernel predicates over a field's current name, the column of each.
struct DeltaTarget;

impl StatisticsTarget for DeltaTarget {
    type Column = String;
    type Predicate = Predicate;

    fn compare(
        &self,
        name: &String,
        comparison: Comparison,
        literal: &ScalarValue,
    ) -> Option<Predicate> {
        Some(compare(name, comparison, scalar(literal)?))
    }

    /// Desugared into ORed equalities because the kernel prunes nothing for a native IN.
    fn in_list(&self, name: &String, literals: &[ScalarValue]) -> Option<Predicate> {
        let equalities = literals
            .iter()
            .map(|literal| Some(compare(name, Comparison::Equal, scalar(literal)?)))
            .collect::<Option<Vec<_>>>()?;
        Some(Predicate::or_from(equalities))
    }

    fn null_test(&self, name: &String, is_null: bool) -> Predicate {
        let column = Expression::column([name.as_str()]);
        if is_null {
            Predicate::is_null(column)
        } else {
            Predicate::is_not_null(column)
        }
    }

    /// The kernel has no prefix predicate, so a prefix becomes the range from it to its successor.
    /// A prefix of only the last code point has none, and every string above it starts with it.
    fn starts_with(&self, name: &String, prefix: String) -> Predicate {
        let upper = successor(&prefix);
        let lower = compare(name, Comparison::GreaterEqual, Scalar::String(prefix));
        match upper {
            Some(upper) => Predicate::and_from([
                lower,
                compare(name, Comparison::Less, Scalar::String(upper)),
            ]),
            None => lower,
        }
    }

    // The parse never builds an empty junction: `and_from([])` is `true`, `or_from([])` `false`.
    fn and(predicates: Vec<Predicate>) -> Predicate {
        Predicate::and_from(predicates)
    }

    fn or(predicates: Vec<Predicate>) -> Predicate {
        Predicate::or_from(predicates)
    }

    fn not(predicate: Predicate) -> Predicate {
        Predicate::not(predicate)
    }
}

fn compare(name: &str, comparison: Comparison, scalar: Scalar) -> Predicate {
    let compare: fn(Expression, Expression) -> Predicate = match comparison {
        Comparison::Equal => Predicate::eq,
        Comparison::NotEqual => Predicate::ne,
        Comparison::Less => Predicate::lt,
        Comparison::LessEqual => Predicate::le,
        Comparison::Greater => Predicate::gt,
        Comparison::GreaterEqual => Predicate::ge,
    };
    compare(Expression::column([name]), Expression::literal(scalar))
}

fn resolve_column<'s>(
    col_name: &str,
    columns: &FilterColumns<'s>,
) -> Option<(&'s str, &'s PrimitiveType)> {
    let column = col_name.to_uppercase();
    let (_, field) = columns
        .iter()
        .find(|(declared, _)| declared.to_uppercase() == column)?;
    match field.data_type() {
        DataType::Primitive(prim) => Some((field.name().as_str(), prim)),
        _ => None,
    }
}

fn arrow_type(primitive: &PrimitiveType) -> Option<ArrowType> {
    Some(match primitive {
        PrimitiveType::Boolean => ArrowType::Boolean,
        PrimitiveType::Byte => ArrowType::Int8,
        PrimitiveType::Short => ArrowType::Int16,
        PrimitiveType::Integer => ArrowType::Int32,
        PrimitiveType::Long => ArrowType::Int64,
        PrimitiveType::Float => ArrowType::Float32,
        PrimitiveType::Double => ArrowType::Float64,
        PrimitiveType::Decimal(decimal) => {
            ArrowType::Decimal128(decimal.precision(), i8::try_from(decimal.scale()).ok()?)
        }
        PrimitiveType::String => ArrowType::Utf8,
        PrimitiveType::Binary => ArrowType::Binary,
        PrimitiveType::Date => ArrowType::Date32,
        PrimitiveType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        PrimitiveType::TimestampNtz => ArrowType::Timestamp(TimeUnit::Microsecond, None),
        _ => return None,
    })
}

fn scalar(literal: &ScalarValue) -> Option<Scalar> {
    Some(match literal.clone() {
        ScalarValue::Boolean(Some(value)) => Scalar::Boolean(value),
        ScalarValue::Int8(Some(value)) => Scalar::Byte(value),
        ScalarValue::Int16(Some(value)) => Scalar::Short(value),
        ScalarValue::Int32(Some(value)) => Scalar::Integer(value),
        ScalarValue::Int64(Some(value)) => Scalar::Long(value),
        ScalarValue::Float32(Some(value)) => Scalar::Float(value),
        ScalarValue::Float64(Some(value)) => Scalar::Double(value),
        ScalarValue::Decimal128(Some(unscaled), precision, scale) => {
            let decimal = DecimalType::try_new(precision, u8::try_from(scale).ok()?).ok()?;
            Scalar::Decimal(DecimalData::try_new(unscaled, decimal).ok()?)
        }
        ScalarValue::Utf8(Some(value)) => Scalar::String(value),
        ScalarValue::Binary(Some(bytes)) => Scalar::Binary(bytes),
        ScalarValue::Date32(Some(days)) => Scalar::Date(days),
        ScalarValue::TimestampMicrosecond(Some(micros), Some(_)) => Scalar::Timestamp(micros),
        ScalarValue::TimestampMicrosecond(Some(micros), None) => Scalar::TimestampNtz(micros),
        _ => return None,
    })
}

/// The least string above every string starting with `prefix`, in code point order.
fn successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = (u32::from(last) + 1..=u32::from(char::MAX)).find_map(char::from_u32);
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

#[cfg(test)]
#[path = "delta_predicate_tests.rs"]
mod tests;
