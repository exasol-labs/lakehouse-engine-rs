use arrow::datatypes::{DataType as ArrowType, TimeUnit};
use datafusion::scalar::ScalarValue;
use delta_kernel::expressions::{DecimalData, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, StructField};
use delta_kernel::{Expression, Predicate};
use serde_json::Value as Json;

use super::filter_json::{
    Comparison, LikeBound, Translated, between_operands, comparison_operands, fold_and, fold_or,
    in_operands, like_operands, operands, subject_column,
};
use super::literal::exact_literal;

/// Each declared column's name paired with the current snapshot field its binding key names.
type FilterColumns<'s> = [(&'s str, &'s StructField)];

/// `None` means no constraint (pass all files, never none): an untranslatable node must
/// widen the surviving file set, never narrow it. A filter column is found among `columns` by
/// its declared name and emitted under its field's current name; one missing from `columns`
/// constrains nothing.
pub(crate) fn to_delta_predicate(filter_json: &Json, columns: &FilterColumns) -> Option<Predicate> {
    translate_node(filter_json, columns).map(|translated| translated.predicate)
}

fn translate_node(node: &Json, columns: &FilterColumns) -> Option<Translated<Predicate>> {
    let kind = node.get("type")?.as_str()?;
    if let Some(comparison) = Comparison::from_node_type(kind) {
        return translate_comparison(node, comparison, columns);
    }

    match kind {
        "predicate_in_constlist" => translate_in(node, columns),
        "predicate_is_null" => {
            let (field_name, _) = resolve_column(subject_column(node)?, columns)?;
            Some(Translated::exact(Predicate::is_null(Expression::column([
                field_name,
            ]))))
        }
        "predicate_is_not_null" => {
            let (field_name, _) = resolve_column(subject_column(node)?, columns)?;
            Some(Translated::exact(Predicate::is_not_null(
                Expression::column([field_name]),
            )))
        }
        // An empty junction is never built: `and_from([])` is `true` and `or_from([])` is `false`.
        "predicate_and" => fold_and(
            operands(node)?
                .iter()
                .map(|expr| translate_node(expr, columns)),
            Predicate::and_from,
        ),
        "predicate_or" => fold_or(
            operands(node)?
                .iter()
                .map(|expr| translate_node(expr, columns)),
            Predicate::or_from,
        ),
        "predicate_not" => {
            translate_node(node.get("expression")?, columns)?.negated(Predicate::not)
        }
        "predicate_between" => translate_between(node, columns),
        "predicate_like" => translate_like(node, columns),
        _ => None,
    }
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
        PrimitiveType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        PrimitiveType::TimestampNtz => ArrowType::Timestamp(TimeUnit::Microsecond, None),
        _ => return None,
    })
}

fn literal_to_scalar(literal: &Json, primitive: &PrimitiveType) -> Option<Scalar> {
    Some(match exact_literal(literal, &arrow_type(primitive)?)? {
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

fn translate_comparison(
    node: &Json,
    comparison: Comparison,
    columns: &FilterColumns,
) -> Option<Translated<Predicate>> {
    let (col_name, lit_node, comparison) = comparison_operands(node, comparison)?;
    let (field_name, prim) = resolve_column(col_name, columns)?;
    let scalar = literal_to_scalar(lit_node, prim)?;
    let column = Expression::column([field_name]);
    let literal = Expression::literal(scalar);

    let predicate = match comparison {
        Comparison::Equal => Predicate::eq(column, literal),
        Comparison::NotEqual => Predicate::ne(column, literal),
        Comparison::Less => Predicate::lt(column, literal),
        Comparison::LessEqual => Predicate::le(column, literal),
        Comparison::Greater => Predicate::gt(column, literal),
        Comparison::GreaterEqual => Predicate::ge(column, literal),
    };
    Some(Translated::exact(predicate))
}

/// Desugared into ORed equalities because the kernel prunes nothing for a native IN.
fn translate_in(node: &Json, columns: &FilterColumns) -> Option<Translated<Predicate>> {
    let (col_name, args) = in_operands(node)?;
    let (field_name, prim) = resolve_column(col_name, columns)?;
    fold_or(
        args.iter().map(|arg| {
            let scalar = literal_to_scalar(arg, prim)?;
            Some(Translated::exact(Predicate::eq(
                Expression::column([field_name]),
                Expression::literal(scalar),
            )))
        }),
        Predicate::or_from,
    )
}

/// Either bound alone is implied by BETWEEN, so an untranslatable bound is dropped.
fn translate_between(node: &Json, columns: &FilterColumns) -> Option<Translated<Predicate>> {
    let (col_name, low, high) = between_operands(node)?;
    let (field_name, prim) = resolve_column(col_name, columns)?;
    let bound = |literal: Option<&Json>, compare: fn(Expression, Expression) -> Predicate| {
        let scalar = literal_to_scalar(literal?, prim)?;
        Some(Translated::exact(compare(
            Expression::column([field_name]),
            Expression::literal(scalar),
        )))
    };
    fold_and(
        [bound(low, Predicate::ge), bound(high, Predicate::le)],
        Predicate::and_from,
    )
}

/// The kernel has no prefix predicate, so a prefix becomes the range from it to its successor.
fn translate_like(node: &Json, columns: &FilterColumns) -> Option<Translated<Predicate>> {
    let (col_name, pattern) = like_operands(node)?;
    let (field_name, prim) = resolve_column(col_name, columns)?;
    (*prim == PrimitiveType::String).then_some(())?;
    let compare = |compare: fn(Expression, Expression) -> Predicate, value: String| {
        compare(
            Expression::column([field_name]),
            Expression::literal(Scalar::String(value)),
        )
    };
    match pattern.bound()? {
        LikeBound::Equal(value) => Some(Translated::exact(compare(Predicate::eq, value))),
        LikeBound::Prefix { prefix, exact } => {
            let upper = successor(&prefix);
            let below = upper.map(|upper| Translated::exact(compare(Predicate::lt, upper)));
            let range = fold_and(
                [
                    Some(Translated::exact(compare(Predicate::ge, prefix))),
                    below,
                ],
                Predicate::and_from,
            )?;
            Some(Translated {
                exact: exact && range.exact,
                ..range
            })
        }
    }
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
