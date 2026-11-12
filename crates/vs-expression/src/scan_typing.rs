//! The typed pass of every expression the scan computes. It declines a conversion to or from text
//! that Exasol formats by session settings the pushdown request does not carry (#587).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{ExasolType, exasol_type_from_json};
use crate::json_value::{JsonValueCall, Returning};
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
    /// DOUBLE, DECIMAL(p,s>0), or a number of unproven scale. Its text form follows
    /// `NLS_NUMERIC_CHARACTERS`.
    Number,
    /// DATE, TIMESTAMP, or a type this pass does not derive.
    Other,
}

impl TypeClass {
    fn of(ty: ExasolType) -> Self {
        match ty {
            ty if ty.is_character() => Self::Text,
            ExasolType::Decimal(d) if d.scale() == 0 => Self::Integer {
                precision: Some(d.precision()),
            },
            ExasolType::Decimal(_) | ExasolType::DoublePrecision => Self::Number,
            ExasolType::Boolean => Self::Boolean,
            _ => Self::Other,
        }
    }

    const INTEGER: Self = Self::Integer { precision: None };

    fn is_numeric(self) -> bool {
        matches!(self, Self::Integer { .. } | Self::Number | Self::Null)
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Integer { .. } | Self::Null)
    }

    /// The class Exasol gives a CASE, GREATEST, or LEAST over these branches.
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, c) | (c, Self::Null) => c,
            (Self::Text, Self::Text) => Self::Text,
            (Self::Integer { .. }, Self::Integer { .. }) => Self::INTEGER,
            (Self::Integer { .. } | Self::Number, Self::Integer { .. } | Self::Number) => {
                Self::Number
            }
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            _ => Self::Other,
        }
    }
}

/// Synthetic node: an integer operand converted to text, rendered only in the DataFusion dialect.
pub(crate) const INTEGER_TEXT_NODE: &str = "scan_integer_text";
/// Synthetic node: a boolean operand converted to Exasol's `TRUE`/`FALSE`.
pub(crate) const BOOLEAN_TEXT_NODE: &str = "scan_boolean_text";
/// Synthetic node: a boolean operand converted to Exasol's 1 or 0, which DataFusion does not do
/// (apache/datafusion#16797).
pub(crate) const BOOLEAN_NUMBER_NODE: &str = "scan_boolean_number";

/// The child fields that hold expressions. Curated, so a walk never hands a `dataType` object or
/// a `name` identifier to its visitor.
pub const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];
/// See [`EXPR_ARRAY_FIELDS`].
pub const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

pub(crate) fn decline(reason: String) -> UdfError {
    UdfError::User(format!("the scan cannot compute this expression: {reason}"))
}

pub(crate) fn kind(node: &Json) -> &str {
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
        TypeClass::Number | TypeClass::Other => Err(decline(format!(
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
            ResultRule::Integer | ResultRule::IntegerOverIntegers => TypeClass::INTEGER,
            ResultRule::Number => TypeClass::Number,
            ResultRule::Arithmetic if args.iter().all(|c| c.is_integer()) => TypeClass::INTEGER,
            ResultRule::Arithmetic | ResultRule::NumberOverNumbers
                if args.iter().all(|c| c.is_numeric()) =>
            {
                TypeClass::Number
            }
            ResultRule::FirstArgument => args.first().copied().unwrap_or(TypeClass::Other),
            ResultRule::Unified => args
                .iter()
                .copied()
                .reduce(TypeClass::union)
                .unwrap_or(TypeClass::Other),
            ResultRule::Arithmetic | ResultRule::NumberOverNumbers | ResultRule::Other => {
                TypeClass::Other
            }
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
        TypeClass::Number | TypeClass::Other => refuse("depends on the session's NLS settings"),
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
    if kind(node) == "column" {
        let class = columns
            .column_type(node)
            .map_or(TypeClass::Other, TypeClass::of);
        return Ok((node.clone(), class));
    }
    let (mut out, children) = type_children(node, columns)?;
    let class = node_class(node, &mut out, &children)?;
    Ok((out, class))
}

/// The classes of a node's typed child expressions, by field.
#[derive(Default)]
struct ChildClasses {
    arrays: Vec<(&'static str, Vec<TypeClass>)>,
    singles: Vec<(&'static str, TypeClass)>,
}

impl ChildClasses {
    fn of(&self, field: &str) -> Vec<TypeClass> {
        self.arrays
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| c.clone())
            .unwrap_or_default()
    }

    fn one(&self, field: &str) -> Option<TypeClass> {
        self.singles
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| *c)
    }

    fn all(&self) -> Vec<TypeClass> {
        self.arrays
            .iter()
            .flat_map(|(_, c)| c.iter().copied())
            .chain(self.singles.iter().map(|(_, c)| *c))
            .collect()
    }
}

/// Types each child expression of `node`, returning `node` with its children rewritten.
fn type_children(
    node: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Result<(Json, ChildClasses), UdfError> {
    let mut out = node.clone();
    let mut children = ChildClasses::default();
    for field in EXPR_ARRAY_FIELDS {
        if let Some(Json::Array(items)) = node.get(field) {
            let (rewritten, classes): (Vec<Json>, Vec<TypeClass>) = items
                .iter()
                .map(|item| type_scan_tree(item, columns))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            out[field] = Json::Array(rewritten);
            children.arrays.push((field, classes));
        }
    }
    for field in EXPR_SINGLE_FIELDS {
        if let Some(child) = node.get(field)
            && child.is_object()
        {
            let (rewritten, class) = type_scan_tree(child, columns)?;
            out[field] = rewritten;
            children.singles.push((field, class));
        }
    }
    Ok((out, children))
}

/// The class of a non-column `node` whose children are typed, rewriting `out` where needed.
fn node_class(node: &Json, out: &mut Json, children: &ChildClasses) -> Result<TypeClass, UdfError> {
    let class = match kind(node) {
        "literal_null" => TypeClass::Null,
        "literal_bool" => TypeClass::Boolean,
        "literal_string" => TypeClass::Text,
        "literal_double" => TypeClass::Number,
        "literal_exactnumeric" => exact_numeric_class(node),
        "predicate_like" => {
            for field in ["expression", "pattern"] {
                if let Some(class) = children.one(field) {
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
            type_comparison(node, out, children)?;
            TypeClass::Boolean
        }
        "predicate_is_null" | "predicate_is_not_null" => TypeClass::Boolean,
        k if k.starts_with("predicate_") => {
            refuse_implicit_text(node, &children.all())?;
            TypeClass::Boolean
        }
        "function_scalar_extract" => {
            refuse_implicit_text(node, &children.all())?;
            node.get("toExtract")
                .and_then(Json::as_str)
                .map_or(TypeClass::Other, |field| typed_function(field).class(&[]))
        }
        "function_scalar_case" => case_class(node, out, children)?,
        "function_scalar_json_value" => json_value_class(node, &children.of("arguments"))?,
        "function_scalar_cast" => cast_class(out, &children.of("arguments"))?,
        "function_scalar" => scalar_function_class(node, out, &children.of("arguments"))?,
        "function_aggregate" => aggregate_class(node, &children.of("arguments"))?,
        _ => {
            refuse_implicit_text(node, &children.all())?;
            TypeClass::Other
        }
    };
    Ok(class)
}

fn upper_name(node: &Json) -> String {
    node.get("name")
        .and_then(Json::as_str)
        .unwrap_or("")
        .to_uppercase()
}

/// An exact numeric literal is an integer of its digit count, or a fractional number.
fn exact_numeric_class(node: &Json) -> TypeClass {
    let text = match node.get("value") {
        Some(Json::Number(n)) if n.is_i64() || n.is_u64() => Some(n.to_string()),
        Some(Json::String(s)) => Some(s.clone()),
        _ => None,
    };
    text.as_deref()
        .and_then(integer_digits)
        .map_or(TypeClass::Number, |digits| TypeClass::Integer {
            precision: Some(digits),
        })
}

fn case_class(node: &Json, out: &mut Json, children: &ChildClasses) -> Result<TypeClass, UdfError> {
    // A simple CASE compares its basis with each WHEN value.
    if children.one("basis").is_some() {
        type_comparison(node, out, children)?;
    } else {
        refuse_implicit_text(node, &children.of("arguments"))?;
    }
    unify_branches(node, &children.of("results"))
}

fn scalar_function_class(
    node: &Json,
    out: &mut Json,
    args: &[TypeClass],
) -> Result<TypeClass, UdfError> {
    let fn_name = upper_name(node);
    if fn_name == "CAST" {
        return cast_class(out, args);
    }
    let function = typed_function(&fn_name);
    let text_positions = function.string_positions(args.len());
    for &i in &text_positions {
        out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
    }
    if function.result == ResultRule::Unified {
        return unify_branches(node, args);
    }
    let converted: Vec<TypeClass> = args
        .iter()
        .enumerate()
        .filter(|(i, _)| !text_positions.contains(i) && !function.parameters.contains(i))
        .map(|(_, c)| *c)
        .collect();
    refuse_implicit_text(node, &converted)?;
    if function.result == ResultRule::IntegerOverIntegers && !args.iter().all(|c| c.is_integer()) {
        return Err(decline(format!(
            "{} computes over integer operands only: {}",
            function.name,
            short(node)
        )));
    }
    Ok(function.class(args))
}

fn aggregate_class(node: &Json, args: &[TypeClass]) -> Result<TypeClass, UdfError> {
    let name = upper_name(node);
    match name.as_str() {
        "COUNT" => Ok(TypeClass::INTEGER),
        "MIN" | "MAX" => Ok(args.first().copied().unwrap_or(TypeClass::Other)),
        "SUM" | "AVG" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
        | "VAR_SAMP" => numeric_aggregate(node, &name, args),
        _ => {
            refuse_implicit_text(node, args)?;
            Ok(TypeClass::Other)
        }
    }
}

fn json_value_class(node: &Json, args: &[TypeClass]) -> Result<TypeClass, UdfError> {
    if args.first() != Some(&TypeClass::Text) {
        return Err(decline(format!(
            "JSON_VALUE over a document that is not text: {}",
            short(node)
        )));
    }
    Ok(match JsonValueCall::parse(node)?.returning {
        Returning::Text { .. } => TypeClass::Text,
        Returning::Boolean => TypeClass::Boolean,
    })
}

/// The type the request states for `node`: a CAST's `dataType`, a `JSON_VALUE`'s
/// `returningDataType`, or a column's type. `None` otherwise, because Exasol's expression typing
/// depends on values, session, and version (#476).
pub fn exact_expression_type(node: &Json, columns: &dyn ColumnTypeLookup) -> Option<ExasolType> {
    if let Some(data_type) = node
        .get("dataType")
        .or_else(|| node.get("returningDataType"))
    {
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

/// The fields that hold a predicate's operands or a simple CASE's basis. `arguments` holds the
/// rest: an IN list or the WHEN values.
const COMPARED_FIELDS: [&str; 4] = ["expression", "left", "right", "basis"];

/// Types the operands that a predicate or a simple CASE compares with each other.
fn type_comparison(node: &Json, out: &mut Json, children: &ChildClasses) -> Result<(), UdfError> {
    let mut operands: Vec<(&Json, TypeClass)> = COMPARED_FIELDS
        .iter()
        .filter_map(|field| Some((node.get(*field)?, children.one(field)?)))
        .collect();
    if let Some(Json::Array(args)) = node.get("arguments") {
        operands.extend(args.iter().zip(children.of("arguments")));
    }
    compare_without_session_parse(node, out, &operands)?;
    compare_booleans_as_numbers(node, out, children, &operands)
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
    for field in COMPARED_FIELDS {
        if let Some(operand) = out.get_mut(field) {
            to_number(operand);
        }
    }
    if let Some(Json::Array(args)) = out.get_mut("arguments") {
        args.iter_mut().for_each(to_number);
    }
    Ok(())
}

/// Exasol compares a BOOLEAN with a number as 1 or 0, except a multi-value IN over a BOOLEAN,
/// which converts its numbers to BOOLEAN and fails on any but 0 and 1.
fn compare_booleans_as_numbers(
    node: &Json,
    out: &mut Json,
    children: &ChildClasses,
    operands: &[(&Json, TypeClass)],
) -> Result<(), UdfError> {
    let has_number = operands
        .iter()
        .any(|(_, c)| matches!(c, TypeClass::Integer { .. } | TypeClass::Number));
    let has_boolean = operands.iter().any(|(_, c)| *c == TypeClass::Boolean);
    if !has_number || !has_boolean {
        return Ok(());
    }
    if kind(node) == "predicate_in_constlist"
        && children.one("expression") == Some(TypeClass::Boolean)
    {
        return Err(decline(format!(
            "an IN list converts its numbers to the BOOLEAN it tests: {}",
            short(node)
        )));
    }
    for field in COMPARED_FIELDS {
        if children.one(field) == Some(TypeClass::Boolean) {
            out[field] = wrap(BOOLEAN_NUMBER_NODE, out[field].take());
        }
    }
    for (i, class) in children.of("arguments").into_iter().enumerate() {
        if class == TypeClass::Boolean {
            out["arguments"][i] = wrap(BOOLEAN_NUMBER_NODE, out["arguments"][i].take());
        }
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

/// DataFusion aggregates only numbers here. Exasol also converts text by the session's NLS
/// settings and BOOLEAN to 1 or 0, so those run in Exasol.
fn numeric_aggregate(node: &Json, name: &str, args: &[TypeClass]) -> Result<TypeClass, UdfError> {
    refuse_implicit_text(node, args)?;
    if !args.iter().all(|c| c.is_numeric()) {
        return Err(decline(format!(
            "{name} over a value that is not a number: {}",
            short(node)
        )));
    }
    // DataFusion cannot sum an untyped NULL, and Exasol folds a CAST of NULL to a bare NULL.
    if args.contains(&TypeClass::Null) {
        return Err(decline(format!("{name} over a bare NULL: {}", short(node))));
    }
    Ok(if name == "SUM" && args.iter().all(|c| c.is_integer()) {
        TypeClass::INTEGER
    } else {
        TypeClass::Number
    })
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
        ExasolType::Decimal(d) if source_class == TypeClass::Boolean => {
            if d.precision() == d.scale() {
                return Err(decline(format!(
                    "CAST of {} to {target} cannot hold 1",
                    short(source)
                )));
            }
            *source = wrap(BOOLEAN_NUMBER_NODE, source.take());
        }
        _ => cast_from_text(source, source_class, target)?,
    }
    Ok(TypeClass::of(target))
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
