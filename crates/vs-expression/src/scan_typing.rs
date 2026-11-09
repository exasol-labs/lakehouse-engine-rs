//! The typed pass every expression the scan computes goes through. It derives each node's type
//! class as Exasol types it, and declines a conversion to or from text that Exasol formats or
//! parses by session settings the pushdown request does not carry (#587).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{ExasolType, exasol_type_from_json};

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

/// Which arguments of a function Exasol converts to text (#210).
#[derive(Clone, Copy)]
enum TextArgs {
    Never,
    All,
    First,
    FirstTwo,
    /// The subject, and the fill of the three-argument form.
    Pad,
}

/// How a function's result class follows from its arguments' classes.
#[derive(Clone, Copy)]
enum ResultRule {
    Text,
    Integer,
    /// An integer when every argument is an integer or NULL.
    IntegerOverIntegers,
    FirstArgument,
    Other,
}

/// What the typed pass and the renderer know about one scalar function.
#[derive(Clone, Copy)]
struct ScalarFunction {
    name: &'static str,
    text_args: TextArgs,
    /// Text arguments that are parameters, not values: a format model or a unit.
    parameters: &'static [usize],
    result: ResultRule,
    /// The renderer's generic arm emits it as a same-arity DataFusion string call.
    string_call: bool,
}

impl ScalarFunction {
    const fn new(name: &'static str, text_args: TextArgs, result: ResultRule) -> Self {
        Self {
            name,
            text_args,
            parameters: &[],
            result,
            string_call: false,
        }
    }

    const fn string_call(self) -> Self {
        Self {
            string_call: true,
            ..self
        }
    }

    const fn parameters(self, parameters: &'static [usize]) -> Self {
        Self { parameters, ..self }
    }

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
            ResultRule::IntegerOverIntegers | ResultRule::Other => TypeClass::Other,
        }
    }
}

const UNLISTED: ScalarFunction = ScalarFunction::new("", TextArgs::Never, ResultRule::Other);

/// The one per-function table; a name absent here takes no text and types as `Other`.
const SCALAR_FUNCTIONS: &[ScalarFunction] = {
    use ResultRule as R;
    use ScalarFunction as F;
    use TextArgs as T;
    &[
        F::new("CONCAT", T::All, R::Text),
        F::new("TRIM", T::All, R::Text).string_call(),
        F::new("LTRIM", T::All, R::Text).string_call(),
        F::new("RTRIM", T::All, R::Text).string_call(),
        F::new("REPLACE", T::All, R::Text).string_call(),
        F::new("TRANSLATE", T::All, R::Text).string_call(),
        F::new("LOWER", T::First, R::Text).string_call(),
        F::new("UPPER", T::First, R::Text).string_call(),
        F::new("INITCAP", T::First, R::Text).string_call(),
        F::new("REVERSE", T::First, R::Text).string_call(),
        F::new("SUBSTR", T::First, R::Text).string_call(),
        F::new("REPEAT", T::First, R::Text).string_call(),
        F::new("LEFT", T::First, R::Text).string_call(),
        F::new("RIGHT", T::First, R::Text).string_call(),
        F::new("LPAD", T::Pad, R::Text).string_call(),
        F::new("RPAD", T::Pad, R::Text).string_call(),
        F::new("CHR", T::Never, R::Text).string_call(),
        F::new("UNICODECHR", T::Never, R::Text).string_call(),
        F::new("ASCII", T::First, R::Integer).string_call(),
        F::new("UNICODE", T::First, R::Integer).string_call(),
        F::new("LENGTH", T::First, R::Integer).string_call(),
        F::new("OCTET_LENGTH", T::First, R::Integer).string_call(),
        F::new("INSTR", T::FirstTwo, R::Integer),
        F::new("LOCATE", T::FirstTwo, R::Integer),
        F::new("TO_DATE", T::First, R::Other).parameters(&[1]),
        F::new("TO_TIMESTAMP", T::First, R::Other).parameters(&[1]),
        F::new("DATE_TRUNC", T::Never, R::Other).parameters(&[0]),
        F::new("YEAR", T::Never, R::Integer),
        F::new("MONTH", T::Never, R::Integer),
        F::new("DAY", T::Never, R::Integer),
        F::new("HOUR", T::Never, R::Integer),
        F::new("MINUTE", T::Never, R::Integer),
        F::new("WEEK", T::Never, R::Integer),
        F::new("ADD", T::Never, R::IntegerOverIntegers),
        F::new("SUB", T::Never, R::IntegerOverIntegers),
        F::new("MULT", T::Never, R::IntegerOverIntegers),
        F::new("MOD", T::Never, R::IntegerOverIntegers),
        F::new("NEG", T::Never, R::IntegerOverIntegers),
        F::new("ABS", T::Never, R::IntegerOverIntegers),
        F::new("ZEROIFNULL", T::Never, R::FirstArgument),
        F::new("NULLIFZERO", T::Never, R::FirstArgument),
        F::new("NULLIF", T::Never, R::FirstArgument),
    ]
};

fn scalar_function(fn_name: &str) -> ScalarFunction {
    SCALAR_FUNCTIONS
        .iter()
        .find(|f| f.name == fn_name)
        .copied()
        .unwrap_or(UNLISTED)
}

/// Whether the renderer emits upper-case `fn_name` as a same-arity DataFusion string call.
pub(crate) fn is_string_call(fn_name: &str) -> bool {
    scalar_function(fn_name).string_call
}

/// The widest text an integer of `precision` digits converts to, sign included.
fn integer_text_width(precision: Option<u8>) -> u32 {
    u32::from(precision.unwrap_or(crate::exasol_type::MAX_DECIMAL_PRECISION)) + 1
}

/// An explicit CAST to CHAR or VARCHAR. Text into a shorter VARCHAR or an ASCII target runs
/// unchecked, a documented difference (`docs/semantic-differences.md`); CHAR declines, because the
/// scan does not blank-pad.
fn cast_to_text(source: Json, class: TypeClass, target: ExasolType) -> Result<Json, UdfError> {
    let (size, is_char) = match target {
        ExasolType::Char { size, .. } => (size, true),
        ExasolType::Varchar { size, .. } => (size, false),
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

/// Rewrites `node` for the DataFusion dialect and derives its class. Post-order, so a check sees
/// rewritten children. Text passes only where Exasol takes text; anywhere else Exasol converts it
/// implicitly by the session's NLS settings, so the node declines.
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
                let function = scalar_function(&fn_name);
                let text_positions = function.string_positions(args.len());
                for &i in &text_positions {
                    out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
                }
                match fn_name.as_str() {
                    "GREATEST" | "LEAST" => unify_branches(node, &args)?,
                    "NULLIF" => {
                        unify_branches(node, &args)?;
                        function.class(&args)
                    }
                    _ => {
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

/// Exasol's exact type of `node` where the request states it: the node's own `dataType` (a CAST)
/// or a column's type. `None` otherwise: callers decline rather than copy Exasol's typing rules,
/// which depend on literal values, the session, and the version (#476).
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

/// A digits-only literal reads as the same integer in every session, so comparing it with an
/// integer needs no session setting.
fn is_session_free_integer_text(node: &Json) -> bool {
    kind(node) == "literal_string"
        && node.get("value").and_then(Json::as_str).is_some_and(|s| {
            let digits = s.strip_prefix('-').unwrap_or(s);
            (1..=18).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit())
        })
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
    let integer_against_digits = non_text
        .iter()
        .all(|c| matches!(c, TypeClass::Integer { .. }))
        && text.iter().all(|n| is_session_free_integer_text(n));
    if !integer_against_digits {
        return Err(decline(format!(
            "comparing text with another type parses the text by the session's NLS settings: {}",
            short(node)
        )));
    }
    let to_number = |operand: &mut Json| {
        if is_session_free_integer_text(operand) {
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
