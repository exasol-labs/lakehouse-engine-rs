//! The typed pass every expression the scan computes goes through. It derives each node's type
//! class as Exasol types it, and declines a conversion to text whose result Exasol formats by
//! session settings the pushdown request does not carry (#587).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{ExasolType, MAX_VARCHAR_SIZE, exasol_type_from_json};

/// The Exasol type of a `column` node, keyed however the caller binds columns.
pub trait ColumnTypeLookup {
    fn column_type(&self, column: &Json) -> Option<ExasolType>;
}

/// The part of a node's Exasol type that decides whether the scan may convert it to text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeClass {
    /// CHAR or VARCHAR. `max_chars` and `ascii` only when the source states them.
    Text { max_chars: Option<u32>, ascii: bool },
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
            ExasolType::Char { size, ascii } | ExasolType::Varchar { size, ascii } => Self::Text {
                max_chars: Some(size),
                ascii,
            },
            ExasolType::Decimal(d) if d.scale() == 0 => Self::Integer {
                precision: Some(d.precision()),
            },
            ExasolType::Boolean => Self::Boolean,
            _ => Self::Other,
        }
    }

    const TEXT: Self = Self::Text {
        max_chars: None,
        ascii: false,
    };
    const INTEGER: Self = Self::Integer { precision: None };

    /// The class Exasol gives a CASE, GREATEST, or LEAST over these branches.
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, c) | (c, Self::Null) => c,
            (Self::Text { .. }, Self::Text { .. }) => Self::TEXT,
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

const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];
const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

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
        TypeClass::Text { .. } | TypeClass::Null => Ok(node),
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

/// Indices of the arguments Exasol converts to text, per function and arity (#210).
fn string_positions(fn_name: &str, arg_count: usize) -> Vec<usize> {
    let all = || (0..arg_count).collect();
    let first = || vec![0];
    match fn_name {
        "CONCAT" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "TRANSLATE" => all(),
        "LOWER" | "UPPER" | "ASCII" | "INITCAP" | "REVERSE" | "LENGTH" | "OCTET_LENGTH"
        | "UNICODE" | "SUBSTR" | "REPEAT" | "LEFT" | "RIGHT" | "TO_DATE" | "TO_TIMESTAMP" => {
            first()
        }
        "LPAD" | "RPAD" if arg_count > 2 => vec![0, 2],
        "LPAD" | "RPAD" => first(),
        "INSTR" | "LOCATE" => vec![0, 1],
        _ => Vec::new(),
    }
    .into_iter()
    .filter(|i| *i < arg_count)
    .collect()
}

fn function_class(fn_name: &str, args: &[TypeClass]) -> TypeClass {
    let all_integer = || {
        args.iter()
            .all(|c| matches!(c, TypeClass::Integer { .. } | TypeClass::Null))
    };
    match fn_name {
        "ADD" | "SUB" | "MULT" | "MOD" | "NEG" | "ABS" if all_integer() => TypeClass::INTEGER,
        "ZEROIFNULL" | "NULLIFZERO" | "NULLIF" => args.first().copied().unwrap_or(TypeClass::Other),
        "GREATEST" | "LEAST" => args
            .iter()
            .copied()
            .reduce(TypeClass::union)
            .unwrap_or(TypeClass::Other),
        "CONCAT" | "LOWER" | "UPPER" | "SUBSTR" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
        | "REPEAT" | "REVERSE" | "LPAD" | "RPAD" | "INITCAP" | "LEFT" | "RIGHT" | "TRANSLATE"
        | "CHR" | "UNICODECHR" => TypeClass::TEXT,
        "ASCII" | "UNICODE" | "LENGTH" | "OCTET_LENGTH" | "INSTR" | "LOCATE" | "YEAR" | "MONTH"
        | "DAY" | "HOUR" | "MINUTE" | "WEEK" => TypeClass::INTEGER,
        _ => TypeClass::Other,
    }
}

/// The widest text an integer of `precision` digits converts to, sign included.
fn integer_text_width(precision: Option<u8>) -> u32 {
    u32::from(precision.unwrap_or(crate::exasol_type::MAX_DECIMAL_PRECISION)) + 1
}

/// An explicit CAST to CHAR or VARCHAR: pushed only when the source type guarantees Exasol's
/// result, including its length check, blank padding, and ASCII check.
fn cast_to_text(source: Json, class: TypeClass, target: ExasolType) -> Result<Json, UdfError> {
    let (size, ascii, is_char) = match target {
        ExasolType::Char { size, ascii } => (size, ascii, true),
        ExasolType::Varchar { size, ascii } => (size, ascii, false),
        _ => unreachable!("cast_to_text called with a non-character target"),
    };
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
        TypeClass::Null => Ok(source),
        TypeClass::Text {
            max_chars,
            ascii: source_ascii,
        } => {
            let fits = size == MAX_VARCHAR_SIZE || max_chars.is_some_and(|m| m <= size);
            if !fits {
                return refuse("checks the length, which the scan does not reproduce");
            }
            if ascii && !source_ascii {
                return refuse("checks for ASCII, which the scan does not reproduce");
            }
            Ok(source)
        }
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
    if session_parsed && matches!(class, TypeClass::Text { .. }) {
        return Err(decline(format!(
            "CAST of text {} to {target} parses by the session's NLS settings",
            short(source)
        )));
    }
    Ok(())
}

/// Rewrites `node` for the DataFusion dialect and derives its class. Post-order, so a check sees
/// rewritten children.
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

    let class = match node_kind {
        "literal_null" => TypeClass::Null,
        "literal_bool" => TypeClass::Boolean,
        "literal_string" => {
            let text = node.get("value").and_then(Json::as_str).unwrap_or("");
            TypeClass::Text {
                max_chars: u32::try_from(text.chars().count()).ok(),
                ascii: text.is_ascii(),
            }
        }
        "literal_exactnumeric" => match node.get("value") {
            Some(Json::Number(n)) if n.is_i64() || n.is_u64() => TypeClass::Integer {
                precision: u8::try_from(n.to_string().trim_start_matches('-').len()).ok(),
            },
            Some(Json::String(s)) if s.bytes().all(|b| b.is_ascii_digit() || b == b'-') => {
                TypeClass::Integer {
                    precision: u8::try_from(s.trim_start_matches('-').len()).ok(),
                }
            }
            _ => TypeClass::Other,
        },
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
            let mut operands: Vec<TypeClass> = ["expression", "left", "right"]
                .iter()
                .filter_map(|field| class_of(field))
                .collect();
            operands.extend(classes_of("arguments"));
            compare_without_session_parse(node, &operands)?;
            TypeClass::Boolean
        }
        k if k.starts_with("predicate_") => TypeClass::Boolean,
        "function_scalar_extract" => {
            match node
                .get("toExtract")
                .and_then(Json::as_str)
                .map(str::to_uppercase)
            {
                Some(f) if matches!(f.as_str(), "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE") => {
                    TypeClass::INTEGER
                }
                _ => TypeClass::Other,
            }
        }
        "function_scalar_case" => classes_of("results")
            .into_iter()
            .reduce(TypeClass::union)
            .unwrap_or(TypeClass::Other),
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
                if matches!(fn_name.as_str(), "TO_DATE" | "TO_TIMESTAMP") && args.len() == 1 {
                    return Err(decline(format!(
                        "{fn_name} without a format parses by the session's NLS settings"
                    )));
                }
                for i in string_positions(&fn_name, args.len()) {
                    out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
                }
                function_class(&fn_name, &args)
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
                _ => TypeClass::Other,
            }
        }
        _ => TypeClass::Other,
    };
    Ok((out, class))
}

/// Exasol compares text with a date, timestamp, or number by converting the text with the
/// session's NLS settings; DataFusion always parses ISO dates and a `.` decimal point.
fn compare_without_session_parse(node: &Json, operands: &[TypeClass]) -> Result<(), UdfError> {
    let has_text = operands.iter().any(|c| matches!(c, TypeClass::Text { .. }));
    let has_non_text = operands
        .iter()
        .any(|c| !matches!(c, TypeClass::Text { .. } | TypeClass::Null));
    if has_text && has_non_text {
        return Err(decline(format!(
            "comparing text with another type parses the text by the session's NLS settings: {}",
            short(node)
        )));
    }
    Ok(())
}

fn cast_class(out: &mut Json, args: &[TypeClass]) -> Result<TypeClass, UdfError> {
    let target = out
        .get("dataType")
        .map(exasol_type_from_json)
        .transpose()?
        .ok_or_else(|| UdfError::User("CAST missing 'dataType'".into()))?;
    let source_class = args.first().copied().unwrap_or(TypeClass::Other);
    if target.is_character() {
        let source = out["arguments"][0].take();
        out["arguments"][0] = cast_to_text(source, source_class, target)?;
    } else {
        cast_from_text(&out["arguments"][0], source_class, target)?;
    }
    Ok(TypeClass::of(target))
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
