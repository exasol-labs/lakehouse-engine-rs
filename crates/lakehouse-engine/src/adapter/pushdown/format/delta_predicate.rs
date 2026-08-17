use delta_kernel::expressions::{DecimalData, Scalar};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType, StructType};
use delta_kernel::{Expression, Predicate};
use serde_json::Value as Json;

/// Translate an Exasol pushdown filter JSON node into a Delta pruning
/// predicate against the given schema.
///
/// `None` means "no constraint — pass all files, never no files": the caller
/// must treat an untranslatable node as widening the surviving file set, not
/// as narrowing it.
pub(crate) fn to_delta_predicate(filter_json: &Json, schema: &StructType) -> Option<Predicate> {
    translate_node(filter_json, schema).map(|translated| translated.predicate)
}

/// A translated predicate paired with whether its whole subtree translated
/// exactly. Only an exact translation may be negated: `NOT` turns a widened
/// child — one whose subtree dropped a node — into a narrowing one, pruning
/// files that still hold matching rows.
#[derive(Debug, PartialEq)]
struct Translated {
    predicate: Predicate,
    exact: bool,
}

impl Translated {
    fn exact(predicate: Predicate) -> Self {
        Self {
            predicate,
            exact: true,
        }
    }
}

#[derive(Clone, Copy)]
enum Comparison {
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Comparison {
    fn from_node_type(kind: &str) -> Option<Self> {
        Some(match kind {
            "predicate_equal" => Self::Equal,
            "predicate_less" => Self::Less,
            "predicate_lessequal" => Self::LessEqual,
            "predicate_greater" => Self::Greater,
            "predicate_greaterequal" => Self::GreaterEqual,
            _ => return None,
        })
    }

    fn flipped(self) -> Self {
        match self {
            Self::Equal => Self::Equal,
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
        }
    }
}

fn translate_node(filter_json: &Json, schema: &StructType) -> Option<Translated> {
    let kind = filter_json.get("type")?.as_str()?;
    if let Some(comparison) = Comparison::from_node_type(kind) {
        return translate_comparison(filter_json, comparison, schema);
    }

    match kind {
        "predicate_in_constlist" => translate_in(filter_json, schema),

        // predicate_notequal: not soundly prunable to a single range.
        "predicate_notequal" => None,

        "predicate_is_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (field_name, _prim) = resolve_column(col_name, schema)?;
            Some(Translated::exact(Predicate::is_null(Expression::column([
                field_name,
            ]))))
        }
        "predicate_is_not_null" => {
            let col_node = filter_json.get("expression")?;
            let col_name = extract_column(col_node)?;
            let (field_name, _prim) = resolve_column(col_name, schema)?;
            Some(Translated::exact(Predicate::is_not_null(
                Expression::column([field_name]),
            )))
        }
        "predicate_and" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_and(exprs.iter().map(|expr| translate_node(expr, schema)))
        }
        "predicate_or" => {
            let exprs = filter_json.get("expressions")?.as_array()?;
            fold_or(exprs.iter().map(|expr| translate_node(expr, schema)))
        }
        "predicate_not" => {
            let inner = filter_json.get("expression")?;
            let Translated { predicate, exact } = translate_node(inner, schema)?;
            exact.then(|| Translated::exact(Predicate::not(predicate)))
        }

        "predicate_between" => translate_between(filter_json, schema),

        _ => None,
    }
}

/// Combine the translated conjuncts of an AND, dropping the untranslatable
/// ones: fewer constraints only widen the surviving file set.
///
/// `None` when nothing survives — `Predicate::and_from([])` would normalize to
/// literal `true`.
fn fold_and(children: impl Iterator<Item = Option<Translated>>) -> Option<Translated> {
    let mut predicates = Vec::new();
    let mut exact = true;
    for child in children {
        match child {
            Some(translated) => {
                exact &= translated.exact;
                predicates.push(translated.predicate);
            }
            None => exact = false,
        }
    }
    if predicates.is_empty() {
        return None;
    }
    Some(Translated {
        predicate: Predicate::and_from(predicates),
        exact,
    })
}

/// Combine the translated disjuncts of an OR, forfeiting the whole disjunction
/// as soon as one is untranslatable: a dropped disjunct would narrow the
/// surviving file set below what the request implies.
///
/// The empty list is guarded on its own — `Predicate::or_from([])` would
/// normalize to literal `false` and prune every file.
fn fold_or(children: impl Iterator<Item = Option<Translated>>) -> Option<Translated> {
    let translated: Vec<Translated> = children.collect::<Option<_>>()?;
    if translated.is_empty() {
        return None;
    }
    let exact = translated.iter().all(|child| child.exact);
    Some(Translated {
        predicate: Predicate::or_from(translated.into_iter().map(|child| child.predicate)),
        exact,
    })
}

fn resolve_column<'s>(
    col_name: &str,
    schema: &'s StructType,
) -> Option<(&'s str, &'s PrimitiveType)> {
    let field = schema
        .fields()
        .find(|f| f.name().eq_ignore_ascii_case(col_name))?;
    match field.data_type() {
        DataType::Primitive(prim) => Some((field.name().as_str(), prim)),
        _ => None,
    }
}

/// Build a `Scalar` for a filter-JSON literal node, typed from the resolved
/// column's `PrimitiveType`.
///
/// `None` on any pair the column's type cannot represent, and on an empty
/// string, for which the kernel's parser answers a null scalar that constrains
/// nothing.
fn literal_to_scalar(lit: &Json, prim: &PrimitiveType) -> Option<Scalar> {
    let kind = lit.get("type")?.as_str()?;
    let value = lit.get("value")?;

    match (kind, prim) {
        ("literal_bool", PrimitiveType::Boolean) => parse_bool(value).map(Scalar::Boolean),

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Byte) => {
            i8::try_from(parse_i64(value)?).ok().map(Scalar::Byte)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Short) => {
            i16::try_from(parse_i64(value)?).ok().map(Scalar::Short)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Integer) => {
            i32::try_from(parse_i64(value)?).ok().map(Scalar::Integer)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Long) => {
            parse_i64(value).map(Scalar::Long)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Float) => {
            let wide = parse_f64(value)?;
            let narrowed = wide as f32;
            (f64::from(narrowed) == wide).then_some(Scalar::Float(narrowed))
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Double) => {
            parse_f64(value).map(Scalar::Double)
        }

        ("literal_exactnumeric" | "literal_double", PrimitiveType::Decimal(dtype)) => {
            parse_decimal(value, *dtype)
        }

        ("literal_string", PrimitiveType::String) => {
            non_empty_str(value).map(|s| Scalar::String(s.to_owned()))
        }

        ("literal_date", PrimitiveType::Date)
        | ("literal_timestamp", PrimitiveType::Timestamp | PrimitiveType::TimestampNtz)
        | ("literal_timestamp_utc", PrimitiveType::Timestamp) => {
            prim.parse_scalar(non_empty_str(value)?).ok()
        }

        _ => None,
    }
}

fn non_empty_str(value: &Json) -> Option<&str> {
    let s = value.as_str()?;
    (!s.is_empty()).then_some(s)
}

fn parse_bool(value: &Json) -> Option<bool> {
    match value {
        Json::Bool(b) => Some(*b),
        Json::String(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Json::String(s) if s.eq_ignore_ascii_case("false") => Some(false),
        Json::Number(n) => match n.as_i64()? {
            1 => Some(true),
            0 => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn parse_i64(value: &Json) -> Option<i64> {
    match value {
        Json::Number(n) => n.as_i64(),
        Json::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn parse_f64(value: &Json) -> Option<f64> {
    match value {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Build a decimal `Scalar` rescaled to the column's own scale.
///
/// The kernel's `parse_scalar` demands the literal already carry exactly the
/// column's scale, which a request literal does not, so the digits are rescaled
/// here; a literal finer than the column's scale yields `None` rather than a
/// rounded bound.
fn parse_decimal(value: &Json, dtype: DecimalType) -> Option<Scalar> {
    let raw = match value {
        Json::Number(n) => n.to_string(),
        Json::String(s) => s.trim().to_owned(),
        _ => return None,
    };
    let (unscaled, scale) = split_decimal(&raw)?;
    let aligned = rescale(unscaled, scale, i32::from(dtype.scale()))?;
    DecimalData::try_new(aligned, dtype)
        .ok()
        .map(Scalar::Decimal)
}

fn split_decimal(raw: &str) -> Option<(i128, i32)> {
    let (mantissa, exponent) = match raw.find(['e', 'E']) {
        None => (raw, 0),
        Some(pos) => (&raw[..pos], raw[pos + 1..].parse::<i32>().ok()?),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        None => (mantissa, ""),
        Some(pos) => (&mantissa[..pos], &mantissa[pos + 1..]),
    };
    let unscaled: i128 = format!("{int_part}{frac_part}").parse().ok()?;
    let scale = i32::try_from(frac_part.len()).ok()?.checked_sub(exponent)?;
    Some((unscaled, scale))
}

fn rescale(unscaled: i128, from: i32, to: i32) -> Option<i128> {
    let shift = to.checked_sub(from)?;
    let factor = 10i128.checked_pow(shift.unsigned_abs())?;
    if shift >= 0 {
        return unscaled.checked_mul(factor);
    }
    let quotient = unscaled.checked_div(factor)?;
    (quotient.checked_mul(factor)? == unscaled).then_some(quotient)
}

fn translate_comparison(
    node: &Json,
    comparison: Comparison,
    schema: &StructType,
) -> Option<Translated> {
    let left = node.get("left")?;
    let right = node.get("right")?;

    let (col_name, lit_node, col_is_left) = if let Some(name) = extract_column(left) {
        (name, right, true)
    } else if let Some(name) = extract_column(right) {
        (name, left, false)
    } else {
        return None;
    };

    let (field_name, prim) = resolve_column(col_name, schema)?;
    let scalar = literal_to_scalar(lit_node, prim)?;
    let column = Expression::column([field_name]);
    let literal = Expression::literal(scalar);

    let effective = if col_is_left {
        comparison
    } else {
        comparison.flipped()
    };

    let predicate = match effective {
        Comparison::Equal => Predicate::eq(column, literal),
        Comparison::Less => Predicate::lt(column, literal),
        Comparison::LessEqual => Predicate::le(column, literal),
        Comparison::Greater => Predicate::gt(column, literal),
        Comparison::GreaterEqual => Predicate::ge(column, literal),
    };
    Some(Translated::exact(predicate))
}

/// Desugar `IN (..)` into an OR-chain of equalities, since the kernel prunes
/// nothing for a native IN predicate here.
///
/// One untranslatable element forfeits the whole node, exactly as for a
/// hand-written OR: keeping the remaining equalities would prune files that
/// the dropped element could still match.
fn translate_in(node: &Json, schema: &StructType) -> Option<Translated> {
    let col_node = node.get("expression")?;
    let col_name = extract_column(col_node)?;
    let (field_name, prim) = resolve_column(col_name, schema)?;

    let args = node.get("arguments")?.as_array()?;
    fold_or(args.iter().map(|arg| {
        let scalar = literal_to_scalar(arg, prim)?;
        Some(Translated::exact(Predicate::eq(
            Expression::column([field_name]),
            Expression::literal(scalar),
        )))
    }))
}

/// BETWEEN: desugar to `col >= low AND col <= high`.
/// Either bound alone is still implied by BETWEEN, so a failing bound is
/// dropped under the implicit AND (sound: drops one conjunct, widens set).
fn translate_between(node: &Json, schema: &StructType) -> Option<Translated> {
    let col_node = node.get("expression")?;
    let col_name = extract_column(col_node)?;
    let (field_name, prim) = resolve_column(col_name, schema)?;

    let low_pred = node
        .get("left")
        .and_then(|n| literal_to_scalar(n, prim))
        .map(|scalar| {
            Translated::exact(Predicate::ge(
                Expression::column([field_name]),
                Expression::literal(scalar),
            ))
        });
    let high_pred = node
        .get("right")
        .and_then(|n| literal_to_scalar(n, prim))
        .map(|scalar| {
            Translated::exact(Predicate::le(
                Expression::column([field_name]),
                Expression::literal(scalar),
            ))
        });

    fold_and([low_pred, high_pred].into_iter())
}

fn extract_column(node: &Json) -> Option<&str> {
    if node.get("type")?.as_str()? == "column" {
        node.get("name")?.as_str()
    } else {
        None
    }
}

#[cfg(test)]
#[path = "delta_predicate_tests.rs"]
mod tests;
