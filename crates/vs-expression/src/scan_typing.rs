//! The typed pass of every expression the scan computes. It declines a conversion to or from text
//! that Exasol formats by session settings the pushdown request does not carry (#587).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{
    Decimal, ExasolType, MAX_FRACTIONAL_DIGITS, exasol_type_from_json, exasol_type_to_json,
};
use crate::json_value::{JsonValueCall, Returning};
use crate::scalar_functions::{
    DateResult, ResultRule, ScalarFunction, TextArgs, UNLISTED, scalar_function,
};
use crate::timestamp::{TimestampLiteral, TimestampUnit};

/// The Exasol type of a `column` node, keyed however the caller binds columns.
pub trait ColumnTypeLookup {
    fn column_type(&self, column: &Json) -> Option<ExasolType>;
}

/// What the typed pass knows of a node's Exasol type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Inferred {
    /// The type Exasol gives the node. Each rule that derives one is checked against a live
    /// Exasol.
    Exact(ExasolType),
    /// The kind is proven, its parameters are not.
    Partial(Partial),
    /// A NULL literal, NULL in every type.
    Null,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Partial {
    /// CHAR or VARCHAR.
    Text,
    /// DECIMAL(p,0).
    Integer,
    /// DOUBLE or DECIMAL(p,s): a number not proven to be an integer.
    Number,
    /// TIMESTAMP of a precision that differs by Exasol version.
    Timestamp,
}

const BOOLEAN: Inferred = Inferred::Exact(ExasolType::Boolean);
const DATE: Inferred = Inferred::Exact(ExasolType::Date);

impl Inferred {
    fn is_text(self) -> bool {
        match self {
            Self::Exact(ty) => ty.is_character(),
            other => other == Self::Partial(Partial::Text),
        }
    }

    /// NULL counts, as it does for [`Self::is_numeric`].
    fn is_integer(self) -> bool {
        match self {
            Self::Exact(ExasolType::Decimal(d)) => d.scale() == 0,
            other => matches!(other, Self::Partial(Partial::Integer) | Self::Null),
        }
    }

    fn is_numeric(self) -> bool {
        match self {
            Self::Exact(ty) => matches!(ty, ExasolType::Decimal(_) | ExasolType::DoublePrecision),
            other => other.is_integer() || other == Self::Partial(Partial::Number),
        }
    }

    fn is_timestamp(self) -> bool {
        self == Self::Partial(Partial::Timestamp) || self.timestamp_precision().is_some()
    }

    fn integer_precision(self) -> Option<u8> {
        match self {
            Self::Exact(ExasolType::Decimal(d)) if d.scale() == 0 => Some(d.precision()),
            _ => None,
        }
    }

    fn timestamp_precision(self) -> Option<u8> {
        match self {
            Self::Exact(ExasolType::Timestamp { precision }) => Some(precision),
            _ => None,
        }
    }

    /// The type of a CASE, `GREATEST`, or `LEAST` over these branches.
    fn union(self, other: Self) -> Self {
        let exact = match (self, other) {
            (Self::Null, ty) | (ty, Self::Null) => return ty,
            (Self::Exact(a), Self::Exact(b)) => unified_type(a, b),
            _ => None,
        };
        match exact {
            Some(ty) => Self::Exact(ty),
            None if self.is_text() && other.is_text() => Self::Partial(Partial::Text),
            None if self.is_integer() && other.is_integer() => Self::Partial(Partial::Integer),
            None if self.is_numeric() && other.is_numeric() => Self::Partial(Partial::Number),
            None if self.is_timestamp() && other.is_timestamp() => {
                Self::Partial(Partial::Timestamp)
            }
            None => Self::Unknown,
        }
    }
}

/// Exasol's type for two branches brought to one type, where a live check proved it. A DATE with
/// a TIMESTAMP has none: Exasol 2025 gives the TIMESTAMP's type, Exasol 8 the first branch's.
fn unified_type(a: ExasolType, b: ExasolType) -> Option<ExasolType> {
    match (a, b) {
        (ExasolType::Boolean, ExasolType::Boolean) | (ExasolType::Date, ExasolType::Date) => {
            Some(a)
        }
        (ExasolType::Decimal(x), ExasolType::Decimal(y)) if x.scale() == 0 && y.scale() == 0 => {
            Some(ExasolType::decimal(x.precision().max(y.precision()), 0))
        }
        (ExasolType::Timestamp { precision: p }, ExasolType::Timestamp { precision: q }) => {
            Some(ExasolType::timestamp(p.max(q)))
        }
        _ => None,
    }
}

/// Exasol rejects a constant `ADD_HOURS` or `ADD_MINUTES` count whose nanoseconds exceed 36 digits
/// before it reads a row, while the scan fails only per non-NULL row. No count above 1e10 yields a
/// result inside 0001-01-01 to 9999-12-31 in any unit (the range spans 5,258,964,960 minutes), so
/// Exasol computes such a call.
const MAX_SCAN_CONSTANT_COUNT: f64 = 1e10;

/// Synthetic node: an integer operand converted to text, rendered only in the DataFusion dialect.
pub(crate) const INTEGER_TEXT_NODE: &str = "scan_integer_text";
/// Synthetic node: a boolean operand converted to Exasol's `TRUE`/`FALSE`.
pub(crate) const BOOLEAN_TEXT_NODE: &str = "scan_boolean_text";
/// Synthetic node: a boolean operand converted to Exasol's 1 or 0, which DataFusion does not do
/// (apache/datafusion#16797).
pub(crate) const BOOLEAN_NUMBER_NODE: &str = "scan_boolean_number";
/// Synthetic node: the seconds of its argument with `fraction` fractional digits, which only this
/// pass knows.
pub(crate) const SECOND_NODE: &str = "scan_second";

/// `DECIMAL(2+p, p)`: Exasol's type of the seconds of a `TIMESTAMP(p)` on every supported version.
pub(crate) fn seconds_type(fraction: u8) -> ExasolType {
    ExasolType::decimal(2 + fraction, fraction)
}

/// What the scan computes for a one-argument `SECOND(x)` or an `EXTRACT(SECOND FROM x)`.
#[derive(Clone, Copy)]
enum Seconds {
    /// The seconds of a `TIMESTAMP(p)` with its first p fractional digits.
    OfTimestamp { fraction: u8 },
    /// 0, which Exasol types `DECIMAL(2,0)` on 2025.1 and `DECIMAL(5,3)` on 8.29.
    OfDate,
}

impl Seconds {
    fn exact_type(self) -> Option<ExasolType> {
        match self {
            Self::OfTimestamp { fraction } => Some(seconds_type(fraction)),
            Self::OfDate => None,
        }
    }

    /// A DATE's seconds are no integer for the text rule: their text form differs by version.
    fn inferred(self) -> Inferred {
        self.exact_type()
            .map_or(Inferred::Partial(Partial::Number), Inferred::Exact)
    }

    fn node(self, argument: Json) -> Json {
        let fraction = match self {
            Self::OfTimestamp { fraction } => fraction,
            Self::OfDate => 0,
        };
        serde_json::json!({"type": SECOND_NODE, SECOND_FRACTION: fraction, "arguments": [argument]})
    }
}

const SECOND_FRACTION: &str = "fraction";

/// The fractional digits a [`SECOND_NODE`] keeps.
pub(crate) fn second_fraction(node: &Json) -> Option<u8> {
    let fraction = node.get(SECOND_FRACTION)?.as_u64()?;
    u8::try_from(fraction).ok()
}

/// Classifies a one-argument `SECOND(x)` or an `EXTRACT(SECOND FROM x)` by the exact type of `x`.
/// `None` for any other node, `SECOND(x, n)` included. Exasol itself fails `EXTRACT(SECOND)` of a
/// DATE, so that declines.
fn classify_second(
    node: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Option<Result<Seconds, UdfError>> {
    let (name, extract) = match kind(node) {
        "function_scalar_extract" => (node.get("toExtract")?.as_str()?.to_uppercase(), true),
        "function_scalar" => (upper_name(node), false),
        _ => return None,
    };
    if name != "SECOND" {
        return None;
    }
    let [argument] = node.get("arguments")?.as_array()?.as_slice() else {
        return None;
    };
    Some(match exact_expression_type(argument, columns) {
        Some(ExasolType::Timestamp { precision }) if precision <= MAX_FRACTIONAL_DIGITS => {
            Ok(Seconds::OfTimestamp {
                fraction: precision,
            })
        }
        Some(ExasolType::Date) if !extract => Ok(Seconds::OfDate),
        _ => Err(decline(format!(
            "SECOND needs an argument declared TIMESTAMP(p), or DATE outside EXTRACT: {}",
            short(node)
        ))),
    })
}

/// The child fields that hold expressions. Curated, so a walk never hands a `dataType` object or
/// a `name` identifier to its visitor.
const EXPR_ARRAY_FIELDS: [&str; 3] = ["expressions", "arguments", "results"];
/// See [`EXPR_ARRAY_FIELDS`].
const EXPR_SINGLE_FIELDS: [&str; 5] = ["expression", "pattern", "left", "right", "basis"];

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
fn to_text(node: Json, ty: Inferred) -> Result<Json, UdfError> {
    if ty.is_text() || ty == Inferred::Null {
        Ok(node)
    } else if ty.is_integer() {
        Ok(wrap(INTEGER_TEXT_NODE, node))
    } else if ty == BOOLEAN {
        Ok(wrap(BOOLEAN_TEXT_NODE, node))
    } else {
        Err(decline(format!(
            "converting {} to text depends on the session's NLS settings",
            short(&node)
        )))
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

    fn result_type(self, args: &[Inferred]) -> Inferred {
        let partial = Inferred::Partial;
        match self.result {
            ResultRule::Text => partial(Partial::Text),
            ResultRule::Integer | ResultRule::IntegerOverIntegers => partial(Partial::Integer),
            ResultRule::Number => partial(Partial::Number),
            ResultRule::Arithmetic if args.iter().all(|ty| ty.is_integer()) => {
                partial(Partial::Integer)
            }
            ResultRule::Arithmetic | ResultRule::NumberOverNumbers
                if args.iter().all(|ty| ty.is_numeric()) =>
            {
                partial(Partial::Number)
            }
            ResultRule::FirstArgument => args.first().copied().unwrap_or(Inferred::Unknown),
            ResultRule::Unified => args
                .iter()
                .copied()
                .reduce(Inferred::union)
                .unwrap_or(Inferred::Unknown),
            ResultRule::DateAddition(date_result) => match args.first() {
                Some(&DATE) => match date_result {
                    DateResult::Date => DATE,
                    DateResult::Timestamp => Inferred::Exact(ExasolType::timestamp(0)),
                },
                Some(&first) if first.timestamp_precision().is_some() => first,
                _ => Inferred::Unknown,
            },
            ResultRule::Truncated => match args.get(1) {
                Some(&source) if source == DATE || source.is_timestamp() => source,
                _ => Inferred::Unknown,
            },
            ResultRule::Date => DATE,
            ResultRule::Timestamp => partial(Partial::Timestamp),
            ResultRule::Arithmetic | ResultRule::NumberOverNumbers | ResultRule::Other => {
                Inferred::Unknown
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
    ty: Inferred,
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
    if is_char && ty != Inferred::Null {
        return refuse("blank-pads, which the scan does not reproduce");
    }
    if ty == Inferred::Null || ty.is_text() {
        Ok(source)
    } else if ty.is_integer() {
        if integer_text_width(ty.integer_precision()) > size {
            return refuse("may not fit the integer's text");
        }
        Ok(wrap(INTEGER_TEXT_NODE, source))
    } else if ty == BOOLEAN {
        if size < 5 {
            return refuse("may not fit FALSE");
        }
        Ok(wrap(BOOLEAN_TEXT_NODE, source))
    } else {
        refuse("depends on the session's NLS settings")
    }
}

/// A CAST from text to a type Exasol parses by session settings (`NLS_DATE_FORMAT`,
/// `NLS_TIMESTAMP_FORMAT`, `NLS_NUMERIC_CHARACTERS`).
fn cast_from_text(source: &Json, ty: Inferred, target: ExasolType) -> Result<(), UdfError> {
    let session_parsed = match target {
        ExasolType::Date
        | ExasolType::Timestamp { .. }
        | ExasolType::TimestampWithLocalTimeZone { .. }
        | ExasolType::DoublePrecision => true,
        ExasolType::Decimal(d) => d.scale() > 0,
        _ => false,
    };
    if session_parsed && ty.is_text() {
        return Err(decline(format!(
            "CAST of text {} to {target} parses by the session's NLS settings",
            short(source)
        )));
    }
    Ok(())
}

/// Rewrites `node` for the DataFusion dialect and derives its type, post-order. Text passes only
/// where Exasol takes text; elsewhere Exasol converts it by the session's NLS settings.
pub(crate) fn type_scan_tree(
    node: &Json,
    columns: &dyn ColumnTypeLookup,
) -> Result<(Json, Inferred), UdfError> {
    if !node.is_object() {
        return Ok((node.clone(), Inferred::Unknown));
    }
    if kind(node) == "column" {
        let ty = columns
            .column_type(node)
            .map_or(Inferred::Unknown, Inferred::Exact);
        return Ok((node.clone(), ty));
    }
    let (mut out, children) = type_children(node, columns)?;
    if let Some(seconds) = classify_second(node, columns) {
        let seconds = seconds?;
        return Ok((seconds.node(out["arguments"][0].take()), seconds.inferred()));
    }
    let ty = node_type(node, &mut out, &children)?;
    Ok((out, ty))
}

/// The types of a node's typed child expressions, by field.
#[derive(Default)]
struct ChildTypes {
    arrays: Vec<(&'static str, Vec<Inferred>)>,
    singles: Vec<(&'static str, Inferred)>,
}

impl ChildTypes {
    fn of(&self, field: &str) -> Vec<Inferred> {
        self.arrays
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| c.clone())
            .unwrap_or_default()
    }

    fn one(&self, field: &str) -> Option<Inferred> {
        self.singles
            .iter()
            .find(|(f, _)| *f == field)
            .map(|(_, c)| *c)
    }

    fn all(&self) -> Vec<Inferred> {
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
) -> Result<(Json, ChildTypes), UdfError> {
    let mut out = node.clone();
    let mut children = ChildTypes::default();
    for field in EXPR_ARRAY_FIELDS {
        if let Some(Json::Array(items)) = node.get(field) {
            let (rewritten, types): (Vec<Json>, Vec<Inferred>) = items
                .iter()
                .map(|item| type_scan_tree(item, columns))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip();
            out[field] = Json::Array(rewritten);
            children.arrays.push((field, types));
        }
    }
    for field in EXPR_SINGLE_FIELDS {
        if let Some(child) = node.get(field)
            && child.is_object()
        {
            let (rewritten, ty) = type_scan_tree(child, columns)?;
            out[field] = rewritten;
            children.singles.push((field, ty));
        }
    }
    Ok((out, children))
}

/// The type of a non-column `node` whose children are typed, rewriting `out` where needed.
fn node_type(node: &Json, out: &mut Json, children: &ChildTypes) -> Result<Inferred, UdfError> {
    let ty = match kind(node) {
        "literal_null" => Inferred::Null,
        "literal_bool" => BOOLEAN,
        "literal_string" => Inferred::Partial(Partial::Text),
        "literal_double" => Inferred::Exact(ExasolType::DoublePrecision),
        "literal_exactnumeric" => exact_numeric_type(node),
        "literal_date" => DATE,
        "literal_timestamp" => timestamp_literal_type(node),
        "predicate_like" => {
            for field in ["expression", "pattern"] {
                if let Some(ty) = children.one(field) {
                    out[field] = to_text(out[field].take(), ty)?;
                }
            }
            BOOLEAN
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
            floor_timestamp_literals(node, out, children);
            BOOLEAN
        }
        "predicate_is_null" | "predicate_is_not_null" => BOOLEAN,
        k if k.starts_with("predicate_") => {
            refuse_implicit_text(node, &children.all())?;
            BOOLEAN
        }
        "function_scalar_extract" => {
            refuse_implicit_text(node, &children.all())?;
            node.get("toExtract")
                .and_then(Json::as_str)
                .map_or(Inferred::Unknown, |field| {
                    typed_function(field).result_type(&[])
                })
        }
        "function_scalar_case" => case_type(node, out, children)?,
        "function_scalar_json_value" => json_value_type(node, &children.of("arguments"))?,
        "function_scalar_cast" => cast_type(out, &children.of("arguments"))?,
        "function_scalar" => scalar_function_type(node, out, &children.of("arguments"))?,
        "function_aggregate" => aggregate_type(node, &children.of("arguments"))?,
        _ => {
            refuse_implicit_text(node, &children.all())?;
            Inferred::Unknown
        }
    };
    Ok(ty)
}

fn upper_name(node: &Json) -> String {
    node.get("name")
        .and_then(Json::as_str)
        .unwrap_or("")
        .to_uppercase()
}

/// An exact numeric literal is a DECIMAL(digits,0) or fractional. Exasol sends an integer beyond
/// 36 digits as `literal_double`.
fn exact_numeric_type(node: &Json) -> Inferred {
    let text = match node.get("value") {
        Some(Json::Number(n)) if n.is_i64() || n.is_u64() => Some(n.to_string()),
        Some(Json::String(s)) => Some(s.clone()),
        _ => None,
    };
    text.as_deref()
        .and_then(integer_digits)
        .and_then(|digits| Decimal::new(digits, 0))
        .map_or(Inferred::Partial(Partial::Number), |d| {
            Inferred::Exact(ExasolType::Decimal(d))
        })
}

fn timestamp_literal_type(node: &Json) -> Inferred {
    let precision = node
        .get("value")
        .and_then(Json::as_str)
        .and_then(|value| u8::try_from(TimestampLiteral(value).type_precision()).ok());
    match precision {
        Some(p) if p <= TimestampUnit::Nanosecond.digits() => {
            Inferred::Exact(ExasolType::timestamp(p))
        }
        _ => Inferred::Partial(Partial::Timestamp),
    }
}

fn case_type(node: &Json, out: &mut Json, children: &ChildTypes) -> Result<Inferred, UdfError> {
    // A simple CASE compares its basis with each WHEN value.
    if children.one("basis").is_some() {
        type_comparison(node, out, children)?;
    } else {
        refuse_implicit_text(node, &children.of("arguments"))?;
    }
    unify_branches(node, out, "results", &children.of("results"))
}

fn scalar_function_type(
    node: &Json,
    out: &mut Json,
    args: &[Inferred],
) -> Result<Inferred, UdfError> {
    let fn_name = upper_name(node);
    if fn_name == "CAST" {
        return cast_type(out, args);
    }
    let function = typed_function(&fn_name);
    let text_positions = function.string_positions(args.len());
    for &i in &text_positions {
        out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
    }
    if function.result == ResultRule::Unified {
        return unify_branches(node, out, "arguments", args);
    }
    if let ResultRule::DateAddition(_) = function.result {
        check_date_addition(node, out, args)?;
    } else {
        let converted: Vec<Inferred> = args
            .iter()
            .enumerate()
            .filter(|(i, _)| !text_positions.contains(i) && !function.parameters.contains(i))
            .map(|(_, ty)| *ty)
            .collect();
        refuse_implicit_text(node, &converted)?;
    }
    match function.result {
        ResultRule::IntegerOverIntegers if !args.iter().all(|ty| ty.is_integer()) => {
            return Err(decline(format!(
                "{} computes over integer operands only: {}",
                function.name,
                short(node)
            )));
        }
        // DataFusion truncates a DATE to a timestamp, so the scan casts the result back to DATE.
        ResultRule::Truncated if args.get(1) == Some(&DATE) => {
            *out = cast_node(ExasolType::Date, out.take());
        }
        _ => {}
    }
    Ok(function.result_type(args))
}

fn cast_node(target: ExasolType, argument: Json) -> Json {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": exasol_type_to_json(&target),
        "arguments": [argument],
    })
}

/// The scan computes `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over a DATE or a
/// TIMESTAMP, in `Date32` or a microsecond or nanosecond timestamp. The first argument gets a
/// CAST to that type, whatever Arrow type holds it.
fn check_date_addition(node: &Json, out: &mut Json, args: &[Inferred]) -> Result<(), UdfError> {
    let &[first, count] = args else {
        return Err(decline(format!(
            "a date addition takes 2 arguments, got {}: {}",
            args.len(),
            short(node)
        )));
    };
    refuse_implicit_text(node, args)?;
    let cast_target = if first == DATE {
        ExasolType::Date
    } else if let Some(precision) = first.timestamp_precision() {
        ExasolType::timestamp(
            TimestampUnit::holding(precision)
                .max(TimestampUnit::Microsecond)
                .digits(),
        )
    } else {
        return Err(decline(format!(
            "a date addition over a value that is not a DATE or TIMESTAMP: {}",
            short(node)
        )));
    };
    if count == BOOLEAN {
        out["arguments"][1] = wrap(BOOLEAN_NUMBER_NODE, out["arguments"][1].take());
    } else if !count.is_numeric() {
        return Err(decline(format!(
            "a date addition count that is not a number: {}",
            short(node)
        )));
    }
    refuse_constant_count_beyond_scan_limit(node)?;
    out["arguments"][0] = cast_node(cast_target, out["arguments"][0].take());
    Ok(())
}

fn refuse_constant_count_beyond_scan_limit(node: &Json) -> Result<(), UdfError> {
    let count = &node["arguments"][1];
    if !matches!(
        count["type"].as_str(),
        Some("literal_exactnumeric" | "literal_double")
    ) {
        return Ok(());
    }
    let magnitude = match &count["value"] {
        Json::String(text) => text.parse::<f64>().ok(),
        Json::Number(number) => number.as_f64(),
        _ => None,
    }
    .map(f64::abs);
    if magnitude.is_some_and(|magnitude| magnitude <= MAX_SCAN_CONSTANT_COUNT) {
        return Ok(());
    }
    Err(decline(format!(
        "a constant date addition count beyond {MAX_SCAN_CONSTANT_COUNT} runs in Exasol, which checks it before it reads a row: {}",
        short(node)
    )))
}

fn aggregate_type(node: &Json, args: &[Inferred]) -> Result<Inferred, UdfError> {
    let name = upper_name(node);
    match name.as_str() {
        "COUNT" => Ok(Inferred::Partial(Partial::Integer)),
        "MIN" | "MAX" => Ok(args.first().copied().unwrap_or(Inferred::Unknown)),
        "SUM" | "AVG" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
        | "VAR_SAMP" => numeric_aggregate(node, &name, args),
        _ => {
            refuse_implicit_text(node, args)?;
            Ok(Inferred::Unknown)
        }
    }
}

fn json_value_type(node: &Json, args: &[Inferred]) -> Result<Inferred, UdfError> {
    if !args.first().is_some_and(|ty| ty.is_text()) {
        return Err(decline(format!(
            "JSON_VALUE over a document that is not text: {}",
            short(node)
        )));
    }
    Ok(match JsonValueCall::parse(node)?.returning {
        Returning::Text { .. } => Inferred::Partial(Partial::Text),
        Returning::Boolean => BOOLEAN,
    })
}

/// The type the request states for `node`: a CAST's `dataType`, a `JSON_VALUE`'s
/// `returningDataType`, a column's type, or the type of `SECOND` over a `TIMESTAMP(p)`, which
/// depends on p alone. `None` otherwise, because Exasol's expression typing depends on values,
/// session, and version (#476).
pub fn exact_expression_type(node: &Json, columns: &dyn ColumnTypeLookup) -> Option<ExasolType> {
    if let Some(data_type) = node
        .get("dataType")
        .or_else(|| node.get("returningDataType"))
    {
        return exasol_type_from_json(data_type).ok();
    }
    if let Some(seconds) = classify_second(node, columns) {
        return seconds.ok()?.exact_type();
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
fn type_comparison(node: &Json, out: &mut Json, children: &ChildTypes) -> Result<(), UdfError> {
    let mut operands: Vec<(&Json, Inferred)> = COMPARED_FIELDS
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
    operands: &[(&Json, Inferred)],
) -> Result<(), UdfError> {
    let text: Vec<&Json> = operands
        .iter()
        .filter(|(_, ty)| ty.is_text())
        .map(|(n, _)| *n)
        .collect();
    let non_text: Vec<Inferred> = operands
        .iter()
        .map(|(_, ty)| *ty)
        .filter(|ty| !ty.is_text() && *ty != Inferred::Null)
        .collect();
    if text.is_empty() || non_text.is_empty() {
        return Ok(());
    }
    // A multi-value IN converts the text to the integer's type, which fails natively on a literal
    // wider than its precision, so only a literal that fits reaches the scan.
    let max_digits = non_text.iter().try_fold(18, |max, ty| {
        ty.is_integer()
            .then(|| ty.integer_precision().map_or(max, |p| max.min(p)))
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
    children: &ChildTypes,
    operands: &[(&Json, Inferred)],
) -> Result<(), UdfError> {
    let has_number = operands
        .iter()
        .any(|(_, ty)| ty.is_numeric() && *ty != Inferred::Null);
    let has_boolean = operands.iter().any(|(_, ty)| *ty == BOOLEAN);
    if !has_number || !has_boolean {
        return Ok(());
    }
    if kind(node) == "predicate_in_constlist" && children.one("expression") == Some(BOOLEAN) {
        return Err(decline(format!(
            "an IN list converts its numbers to the BOOLEAN it tests: {}",
            short(node)
        )));
    }
    for field in COMPARED_FIELDS {
        if children.one(field) == Some(BOOLEAN) {
            out[field] = wrap(BOOLEAN_NUMBER_NODE, out[field].take());
        }
    }
    for (i, ty) in children.of("arguments").into_iter().enumerate() {
        if ty == BOOLEAN {
            out["arguments"][i] = wrap(BOOLEAN_NUMBER_NODE, out["arguments"][i].take());
        }
    }
    Ok(())
}

/// DataFusion truncates a timestamp literal to its TIMESTAMP operand's unit, while Exasol compares
/// at full precision (#461, apache/datafusion#22142, fixed by apache/datafusion#22906). A DATE
/// needs no floor, because DataFusion compares it with a timestamp in nanoseconds.
fn floor_timestamp_literals(node: &Json, out: &mut Json, children: &ChildTypes) {
    let precision = |field: &str| children.one(field)?.timestamp_precision();
    let finer = |literal: &Json, p: u8| floored_literal(literal, p).is_some();
    match kind(node) {
        "predicate_between" => {
            let Some(p) = precision("expression") else {
                return;
            };
            if !finer(&out["left"], p) && !finer(&out["right"], p) {
                return;
            }
            let operand = &out["expression"];
            *out = serde_json::json!({"type": "predicate_and", "expressions": [
                floored_comparison("predicate_greaterequal", operand, &out["left"], p),
                floored_comparison("predicate_lessequal", operand, &out["right"], p),
            ]});
        }
        "predicate_in_constlist" => {
            let Some(p) = precision("expression") else {
                return;
            };
            let Some(Json::Array(values)) = out.get("arguments") else {
                return;
            };
            if !values.iter().any(|value| finer(value, p)) {
                return;
            }
            let operand = &out["expression"];
            let expressions: Vec<Json> = values
                .iter()
                .map(|value| floored_comparison("predicate_equal", operand, value, p))
                .collect();
            *out = serde_json::json!({"type": "predicate_or", "expressions": expressions});
        }
        op => {
            if let Some(p) = precision("left")
                && finer(&out["right"], p)
            {
                *out = floored_comparison(op, &out["left"], &out["right"], p);
            } else if let Some(p) = precision("right")
                && finer(&out["left"], p)
            {
                *out = floored_comparison(mirrored(op), &out["right"], &out["left"], p);
            }
        }
    }
}

fn mirrored(op: &str) -> &str {
    match op {
        "predicate_less" => "predicate_greater",
        "predicate_lessequal" => "predicate_greaterequal",
        "predicate_greater" => "predicate_less",
        "predicate_greaterequal" => "predicate_lessequal",
        op => op,
    }
}

fn floored_literal(node: &Json, precision: u8) -> Option<Json> {
    if kind(node) != "literal_timestamp" {
        return None;
    }
    let floor = TimestampLiteral(node.get("value")?.as_str()?).floor(precision)?;
    Some(serde_json::json!({"type": "literal_timestamp", "value": floor}))
}

/// `operand <op> literal`, with a literal finer than `precision` floored. `x <> x` and `x = x`
/// keep a NULL operand NULL, so an enclosing NOT stays exact.
fn floored_comparison(op: &str, operand: &Json, literal: &Json, precision: u8) -> Json {
    let Some(floor) = floored_literal(literal, precision) else {
        return serde_json::json!({"type": op, "left": operand, "right": literal});
    };
    let (op, right) = match op {
        "predicate_equal" => ("predicate_notequal", operand.clone()),
        "predicate_notequal" => ("predicate_equal", operand.clone()),
        "predicate_less" | "predicate_lessequal" => ("predicate_lessequal", floor),
        _ => ("predicate_greater", floor),
    };
    serde_json::json!({"type": op, "left": operand, "right": right})
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
fn refuse_implicit_text(node: &Json, operands: &[Inferred]) -> Result<(), UdfError> {
    if operands.iter().any(|ty| ty.is_text()) {
        return Err(decline(format!(
            "converting a text operand to another type parses it by the session's NLS settings: {}",
            short(node)
        )));
    }
    Ok(())
}

/// DataFusion aggregates only numbers here. Exasol also converts text by the session's NLS
/// settings and BOOLEAN to 1 or 0, so those run in Exasol.
fn numeric_aggregate(node: &Json, name: &str, args: &[Inferred]) -> Result<Inferred, UdfError> {
    refuse_implicit_text(node, args)?;
    if !args.iter().all(|ty| ty.is_numeric()) {
        return Err(decline(format!(
            "{name} over a value that is not a number: {}",
            short(node)
        )));
    }
    // DataFusion cannot sum an untyped NULL, and Exasol folds a CAST of NULL to a bare NULL.
    if args.contains(&Inferred::Null) {
        return Err(decline(format!("{name} over a bare NULL: {}", short(node))));
    }
    Ok(Inferred::Partial(
        if name == "SUM" && args.iter().all(|ty| ty.is_integer()) {
            Partial::Integer
        } else {
            Partial::Number
        },
    ))
}

/// Text among other types converts by the session's NLS settings.
fn refuse_text_among_others(node: &Json, types: &[Inferred]) -> Result<(), UdfError> {
    let has_text = types.iter().any(|ty| ty.is_text());
    let has_other = types
        .iter()
        .any(|ty| !ty.is_text() && *ty != Inferred::Null);
    if has_text && has_other {
        return Err(decline(format!(
            "bringing text and another type to one type parses the text by the session's NLS settings: {}",
            short(node)
        )));
    }
    Ok(())
}

/// The type Exasol brings the values in `field` to (CASE results, `GREATEST`, `LEAST`).
/// DataFusion brings timestamps to the finest unit among them, so each branch gets a CAST to the
/// unit that holds Exasol's precision.
fn unify_branches(
    node: &Json,
    out: &mut Json,
    field: &str,
    branches: &[Inferred],
) -> Result<Inferred, UdfError> {
    refuse_text_among_others(node, branches)?;
    let unified = branches
        .iter()
        .copied()
        .reduce(Inferred::union)
        .unwrap_or(Inferred::Unknown);
    if let Some(precision) = unified.timestamp_precision() {
        let target = ExasolType::timestamp(TimestampUnit::holding(precision).digits());
        for (i, branch) in branches.iter().enumerate() {
            if *branch != Inferred::Null {
                out[field][i] = cast_node(target, out[field][i].take());
            }
        }
    }
    Ok(unified)
}

fn cast_type(out: &mut Json, args: &[Inferred]) -> Result<Inferred, UdfError> {
    let target = out
        .get("dataType")
        .map(exasol_type_from_json)
        .transpose()?
        .ok_or_else(|| UdfError::User("CAST missing 'dataType'".into()))?;
    let source_type = args.first().copied().unwrap_or(Inferred::Unknown);
    let source = out
        .get_mut("arguments")
        .and_then(|args| args.get_mut(0))
        .ok_or_else(|| UdfError::User("CAST missing its argument".into()))?;
    match target {
        ExasolType::Char { size, .. } => {
            *source = cast_to_text(source.take(), source_type, target, size, true)?;
        }
        ExasolType::Varchar { size, .. } => {
            *source = cast_to_text(source.take(), source_type, target, size, false)?;
        }
        ExasolType::Decimal(d) if source_type == BOOLEAN => {
            if d.precision() == d.scale() {
                return Err(decline(format!(
                    "CAST of {} to {target} cannot hold 1",
                    short(source)
                )));
            }
            *source = wrap(BOOLEAN_NUMBER_NODE, source.take());
        }
        _ => cast_from_text(source, source_type, target)?,
    }
    Ok(Inferred::Exact(target))
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
