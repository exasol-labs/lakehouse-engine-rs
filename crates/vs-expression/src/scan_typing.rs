//! The typed pass of every expression the scan computes. It declines a conversion to or from text
//! that Exasol formats by session settings the pushdown request does not carry (#587).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{ExasolType, exasol_type_from_json};
use crate::scalar_functions::{ResultRule, ScalarFunction, TextArgs, UNLISTED, scalar_function};

/// The Exasol type of a `column` node, keyed however the caller binds columns.
pub trait ColumnTypeLookup {
    fn column_type(&self, column: &Json) -> Option<ExasolType>;
}

/// The part of a node's Exasol type that decides whether the scan may convert it to text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeClass {
    /// CHAR or VARCHAR.
    Text,
    /// DECIMAL(p,0). Its text form is the same in every session.
    Integer { precision: Option<u8> },
    /// `TRUE` or `FALSE` in every session.
    Boolean,
    /// A NULL literal, NULL in every type.
    Null,
    /// DATE, TIMESTAMP, DOUBLE, DECIMAL(p,s>0), or a type this pass does not derive.
    Other,
}

impl TypeClass {
    fn of(ty: ExasolType) -> Self {
        match ty {
            ty if ty.is_character() => Self::Text,
            ExasolType::Decimal(d) if d.scale() == 0 => Self::Integer {
                precision: Some(d.precision()),
            },
            ExasolType::Boolean => Self::Boolean,
            _ => Self::Other,
        }
    }

    const INTEGER: Self = Self::Integer { precision: None };

    /// The class Exasol gives a CASE, GREATEST, or LEAST over these branches.
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, c) | (c, Self::Null) => c,
            (Self::Text, Self::Text) => Self::Text,
            (Self::Integer { .. }, Self::Integer { .. }) => Self::INTEGER,
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            _ => Self::Other,
        }
    }
}

/// Synthetic node: an integer operand converted to text, rendered only in the DataFusion dialect.
pub(crate) const INTEGER_TEXT_NODE: &str = "scan_integer_text";
/// Synthetic node: a boolean operand converted to Exasol's `TRUE`/`FALSE`.
pub(crate) const BOOLEAN_TEXT_NODE: &str = "scan_boolean_text";

/// The child fields that hold expressions. Curated, so a walk never hands a `dataType` object or
/// a `name` identifier to its visitor.
pub const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];
/// See [`EXPR_ARRAY_FIELDS`].
pub const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

fn decline(reason: String) -> UdfError {
    UdfError::User(format!("the scan cannot compute this expression: {reason}"))
}

fn kind(node: &Json) -> &str {
    node.get("type").and_then(Json::as_str).unwrap_or("")
}

fn wrap(node_type: &str, arg: Json) -> Json {
    serde_json::json!({"type": node_type, "arguments": [arg]})
}

/// `node` converted to text implicitly, as a LIKE operand, a `||` operand, or a string-function
/// argument.
fn to_text(node: Json, class: TypeClass) -> Result<Json, UdfError> {
    match class {
        TypeClass::Text | TypeClass::Null => Ok(node),
        TypeClass::Integer { .. } => Ok(wrap(INTEGER_TEXT_NODE, node)),
        TypeClass::Boolean => Ok(wrap(BOOLEAN_TEXT_NODE, node)),
        TypeClass::Other => Err(decline(format!(
            "converting {} to text depends on the session's NLS settings",
            short(&node)
        ))),
    }
}

fn short(node: &Json) -> String {
    let text = node.to_string();
    match text.char_indices().nth(120) {
        Some((byte, _)) => format!("{}…", &text[..byte]),
        None => text,
    }
}

impl ScalarFunction {
    /// Indices of the arguments Exasol converts to text, for this arity.
    fn string_positions(self, arg_count: usize) -> Vec<usize> {
        let positions = match self.text_args {
            TextArgs::Never => Vec::new(),
            TextArgs::All => (0..arg_count).collect(),
            TextArgs::First => vec![0],
            TextArgs::FirstTwo => vec![0, 1],
            TextArgs::Pad if arg_count > 2 => vec![0, 2],
            TextArgs::Pad => vec![0],
        };
        positions.into_iter().filter(|i| *i < arg_count).collect()
    }

    fn class(self, args: &[TypeClass]) -> TypeClass {
        match self.result {
            ResultRule::Text => TypeClass::Text,
            ResultRule::Integer => TypeClass::INTEGER,
            ResultRule::IntegerOverIntegers
                if args
                    .iter()
                    .all(|c| matches!(c, TypeClass::Integer { .. } | TypeClass::Null)) =>
            {
                TypeClass::INTEGER
            }
            ResultRule::FirstArgument => args.first().copied().unwrap_or(TypeClass::Other),
            ResultRule::Unified => args
                .iter()
                .copied()
                .reduce(TypeClass::union)
                .unwrap_or(TypeClass::Other),
            ResultRule::IntegerOverIntegers | ResultRule::Other => TypeClass::Other,
        }
    }
}

fn typed_function(fn_name: &str) -> ScalarFunction {
    scalar_function(fn_name).unwrap_or(UNLISTED)
}

/// The widest text an integer of `precision` digits converts to, sign included.
fn integer_text_width(precision: Option<u8>) -> u32 {
    u32::from(precision.unwrap_or(crate::exasol_type::MAX_DECIMAL_PRECISION)) + 1
}

/// A CAST to CHAR or VARCHAR. Text into a shorter VARCHAR or ASCII runs unchecked
/// (`docs/semantic-differences.md`); CHAR declines, because the scan does not blank-pad.
fn cast_to_text(
    source: Json,
    class: TypeClass,
    target: ExasolType,
    size: u32,
    is_char: bool,
) -> Result<Json, UdfError> {
    let refuse = |why: &str| {
        Err(decline(format!(
            "CAST of {} to {target} {why}",
            short(&source)
        )))
    };
    if is_char && class != TypeClass::Null {
        return refuse("blank-pads, which the scan does not reproduce");
    }
    match class {
        TypeClass::Null | TypeClass::Text => Ok(source),
        TypeClass::Integer { precision } => {
            if integer_text_width(precision) > size {
                return refuse("may not fit the integer's text");
            }
            Ok(wrap(INTEGER_TEXT_NODE, source))
        }
        TypeClass::Boolean => {
            if size < 5 {
                return refuse("may not fit FALSE");
            }
            Ok(wrap(BOOLEAN_TEXT_NODE, source))
        }
        TypeClass::Other => refuse("depends on the session's NLS settings"),
    }
}

/// A CAST from text to a type Exasol parses by session settings (`NLS_DATE_FORMAT`,
/// `NLS_TIMESTAMP_FORMAT`, `NLS_NUMERIC_CHARACTERS`).
fn cast_from_text(source: &Json, class: TypeClass, target: ExasolType) -> Result<(), UdfError> {
    let session_parsed = match target {
        ExasolType::Date
        | ExasolType::Timestamp { .. }
        | ExasolType::TimestampWithLocalTimeZone { .. }
        | ExasolType::DoublePrecision => true,
        ExasolType::Decimal(d) => d.scale() > 0,
        _ => false,
    };
    if session_parsed && matches!(class, TypeClass::Text) {
        return Err(decline(format!(
            "CAST of text {} to {target} parses by the session's NLS settings",
            short(source)
        )));
    }
    Ok(())
}

/// Rewrites `node` for the DataFusion dialect and derives its class, post-order. Text passes only
/// where Exasol takes text; elsewhere Exasol converts it by the session's NLS settings.
pub(crate) fn type_scan_tree(
    node: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Result<(Json, TypeClass), UdfError> {
    if !node.is_object() {
        return Ok((node.clone(), TypeClass::Other));
    }
    let node_kind = kind(node);
    if node_kind == "column" {
        let class = columns
            .column_type(node)
            .map_or(TypeClass::Other, TypeClass::of);
        return Ok((node.clone(), class));
    }

    let mut out = node.clone();
    let mut array_classes: Vec<(&str, Vec<TypeClass>)> = Vec::new();
    for field in EXPR_ARRAY_FIELDS {
        if let Some(Json::Array(children)) = node.get(field) {
            let mut rewritten = Vec::with_capacity(children.len());
            let mut classes = Vec::with_capacity(children.len());
            for child in children {
                let (c, class) = type_scan_tree(child, columns)?;
                rewritten.push(c);
                classes.push(class);
            }
            out[field] = Json::Array(rewritten);
            array_classes.push((field, classes));
        }
    }
    let mut single_classes: Vec<(&str, TypeClass)> = Vec::new();
    for field in EXPR_SINGLE_FIELDS {
        if let Some(child) = node.get(field)
            && child.is_object()
        {
            let (c, class) = type_scan_tree(child, columns)?;
            out[field] = c;
            single_classes.push((field, class));
        }
    }
    let classes_of = |field: &str| -> Vec<TypeClass> {
        array_classes
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| c.clone())
            .unwrap_or_default()
    };
    let class_of = |field: &str| -> Option<TypeClass> {
        single_classes
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| *c)
    };
    let all_children = || -> Vec<TypeClass> {
        array_classes
            .iter()
            .flat_map(|(_, c)| c.iter().copied())
            .chain(single_classes.iter().map(|(_, c)| *c))
            .collect()
    };

    let class = match node_kind {
        "literal_null" => TypeClass::Null,
        "literal_bool" => TypeClass::Boolean,
        "literal_string" => TypeClass::Text,
        "literal_exactnumeric" => {
            let text = match node.get("value") {
                Some(Json::Number(n)) if n.is_i64() || n.is_u64() => Some(n.to_string()),
                Some(Json::String(s)) => Some(s.clone()),
                _ => None,
            };
            text.as_deref()
                .and_then(integer_digits)
                .map_or(TypeClass::Other, |digits| TypeClass::Integer {
                    precision: Some(digits),
                })
        }
        "predicate_like" => {
            for field in ["expression", "pattern"] {
                if let Some(class) = class_of(field) {
                    out[field] = to_text(out[field].take(), class)?;
                }
            }
            TypeClass::Boolean
        }
        "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal"
        | "predicate_between"
        | "predicate_in_constlist" => {
            let mut operands: Vec<(&Json, TypeClass)> = ["expression", "left", "right"]
                .iter()
                .filter_map(|field| Some((node.get(*field)?, class_of(field)?)))
                .collect();
            if let Some(Json::Array(args)) = node.get("arguments") {
                operands.extend(args.iter().zip(classes_of("arguments")));
            }
            compare_without_session_parse(node, &mut out, &operands)?;
            TypeClass::Boolean
        }
        "predicate_is_null" | "predicate_is_not_null" => TypeClass::Boolean,
        k if k.starts_with("predicate_") => {
            refuse_implicit_text(node, &all_children())?;
            TypeClass::Boolean
        }
        "function_scalar_extract" => {
            refuse_implicit_text(node, &all_children())?;
            node.get("toExtract")
                .and_then(Json::as_str)
                .map_or(TypeClass::Other, |field| typed_function(field).class(&[]))
        }
        "function_scalar_case" => {
            match (node.get("basis"), class_of("basis")) {
                // A simple CASE compares its basis with each WHEN value.
                (Some(basis), Some(basis_class)) => {
                    let mut operands = vec![(basis, basis_class)];
                    if let Some(Json::Array(whens)) = node.get("arguments") {
                        operands.extend(whens.iter().zip(classes_of("arguments")));
                    }
                    compare_without_session_parse(node, &mut out, &operands)?;
                }
                _ => refuse_implicit_text(node, &classes_of("arguments"))?,
            }
            unify_branches(node, &classes_of("results"))?
        }
        "function_scalar_cast" => cast_class(&mut out, &classes_of("arguments"))?,
        "function_scalar" => {
            let fn_name = node
                .get("name")
                .and_then(Json::as_str)
                .unwrap_or("")
                .to_uppercase();
            if fn_name == "CAST" {
                cast_class(&mut out, &classes_of("arguments"))?
            } else {
                let args = classes_of("arguments");
                let function = typed_function(&fn_name);
                let text_positions = function.string_positions(args.len());
                for &i in &text_positions {
                    out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
                }
                if function.result == ResultRule::Unified {
                    unify_branches(node, &args)?
                } else {
                    let converted: Vec<TypeClass> = args
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| {
                            !text_positions.contains(i) && !function.parameters.contains(i)
                        })
                        .map(|(_, c)| *c)
                        .collect();
                    refuse_implicit_text(node, &converted)?;
                    function.class(&args)
                }
            }
        }
        "function_aggregate" => {
            let name = node
                .get("name")
                .and_then(Json::as_str)
                .unwrap_or("")
                .to_uppercase();
            let args = classes_of("arguments");
            match name.as_str() {
                "COUNT" => TypeClass::INTEGER,
                "MIN" | "MAX" => args.first().copied().unwrap_or(TypeClass::Other),
                _ => {
                    refuse_implicit_text(node, &args)?;
                    TypeClass::Other
                }
            }
        }
        _ => {
            refuse_implicit_text(node, &all_children())?;
            TypeClass::Other
        }
    };
    Ok((out, class))
}

/// The type the request states for `node`: a CAST's `dataType` or a column's type. `None`
/// otherwise, because Exasol's expression typing depends on values, session, and version (#476).
pub fn exact_expression_type(node: &Json, columns: &dyn ColumnTypeLookup) -> Option<ExasolType> {
    if let Some(data_type) = node.get("dataType") {
        return exasol_type_from_json(data_type).ok();
    }
    if kind(node) == "column" {
        columns.column_type(node)
    } else {
        None
    }
}

/// The digit count of `text` when it is digits with an optional leading minus sign.
fn integer_digits(text: &str) -> Option<u8> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    u8::try_from(digits.len()).ok()
}

/// A literal of at most `max_digits` digits reads as the same integer in every session, so
/// comparing it with an integer needs no session setting.
fn is_session_free_integer_text(node: &Json, max_digits: u8) -> bool {
    kind(node) == "literal_string"
        && node
            .get("value")
            .and_then(Json::as_str)
            .and_then(integer_digits)
            .is_some_and(|digits| digits <= max_digits)
}

/// Exasol parses text compared with a date or number by the session's NLS settings. A digits-only
/// literal against integers reads the same in every session, so it renders as that number.
fn compare_without_session_parse(
    node: &Json,
    out: &mut Json,
    operands: &[(&Json, TypeClass)],
) -> Result<(), UdfError> {
    let text: Vec<&Json> = operands
        .iter()
        .filter(|(_, c)| matches!(c, TypeClass::Text))
        .map(|(n, _)| *n)
        .collect();
    let non_text: Vec<TypeClass> = operands
        .iter()
        .map(|(_, c)| *c)
        .filter(|c| !matches!(c, TypeClass::Text | TypeClass::Null))
        .collect();
    if text.is_empty() || non_text.is_empty() {
        return Ok(());
    }
    // A multi-value IN converts the text to the integer's type, which fails natively on a literal
    // wider than its precision, so only a literal that fits reaches the scan.
    let max_digits = non_text.iter().try_fold(18, |max, c| match c {
        TypeClass::Integer { precision } => Some(precision.map_or(max, |p| max.min(p))),
        _ => None,
    });
    let integer_against_digits =
        max_digits.is_some_and(|max| text.iter().all(|n| is_session_free_integer_text(n, max)));
    if !integer_against_digits {
        return Err(decline(format!(
            "comparing text with another type parses the text by the session's NLS settings: {}",
            short(node)
        )));
    }
    let to_number = |operand: &mut Json| {
        if kind(operand) == "literal_string" {
            *operand = integer_literal(operand["value"].as_str().unwrap_or_default());
        }
    };
    for field in ["expression", "left", "right", "basis"] {
        if let Some(operand) = out.get_mut(field) {
            to_number(operand);
        }
    }
    if let Some(Json::Array(args)) = out.get_mut("arguments") {
        args.iter_mut().for_each(to_number);
    }
    Ok(())
}

fn integer_literal(digits: &str) -> Json {
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", digits),
    };
    let value = match digits.trim_start_matches('0') {
        "" => "0".to_string(),
        trimmed => format!("{sign}{trimmed}"),
    };
    serde_json::json!({"type": "literal_exactnumeric", "value": value})
}

/// Text in a position that expects a date, timestamp, number, or boolean: Exasol converts it by
/// the session's NLS settings, and DataFusion parses ISO dates and a `.` or fails to plan.
fn refuse_implicit_text(node: &Json, operands: &[TypeClass]) -> Result<(), UdfError> {
    if operands.iter().any(|c| matches!(c, TypeClass::Text)) {
        return Err(decline(format!(
            "converting a text operand to another type parses it by the session's NLS settings: {}",
            short(node)
        )));
    }
    Ok(())
}

/// The class of values Exasol brings to one type (CASE results, GREATEST, LEAST, NULLIF). Text
/// among other types converts by the session's NLS settings.
fn unify_branches(node: &Json, branches: &[TypeClass]) -> Result<TypeClass, UdfError> {
    let has_text = branches.iter().any(|c| matches!(c, TypeClass::Text));
    let has_other = branches
        .iter()
        .any(|c| !matches!(c, TypeClass::Text | TypeClass::Null));
    if has_text && has_other {
        return Err(decline(format!(
            "bringing text and another type to one type parses the text by the session's NLS settings: {}",
            short(node)
        )));
    }
    Ok(branches
        .iter()
        .copied()
        .reduce(TypeClass::union)
        .unwrap_or(TypeClass::Other))
}

fn cast_class(out: &mut Json, args: &[TypeClass]) -> Result<TypeClass, UdfError> {
    let target = out
        .get("dataType")
        .map(exasol_type_from_json)
        .transpose()?
        .ok_or_else(|| UdfError::User("CAST missing 'dataType'".into()))?;
    let source_class = args.first().copied().unwrap_or(TypeClass::Other);
    let source = out
        .get_mut("arguments")
        .and_then(|args| args.get_mut(0))
        .ok_or_else(|| UdfError::User("CAST missing its argument".into()))?;
    match target {
        ExasolType::Char { size, .. } => {
            *source = cast_to_text(source.take(), source_class, target, size, true)?;
        }
        ExasolType::Varchar { size, .. } => {
            *source = cast_to_text(source.take(), source_class, target, size, false)?;
        }
        _ => cast_from_text(source, source_class, target)?,
    }
    Ok(TypeClass::of(target))
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
