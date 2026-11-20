//! Exasol's type of a request expression, and the typed pass of every expression the scan
//! computes. The pass declines a conversion to or from text that Exasol formats by session
//! settings the pushdown request does not carry (#587).
use std::convert::Infallible;

use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::datetime_format::{format_literal, fraction_digits};
use crate::exasol_type::{
    Decimal, ExasolType, MAX_DECIMAL_PRECISION, MAX_FRACTIONAL_DIGITS, exasol_type_from_json,
    exasol_type_to_json,
};
use crate::json_value::{JsonValueCall, returning_type};
use crate::scalar_functions::{
    DateResult, IntegerArg, ResultRule, ScalarFunction, TextArgs, UNLISTED, scalar_function,
};
use crate::timestamp::{TimestampLiteral, TimestampUnit};
use crate::version::ExasolVersion;

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
    /// `TIMESTAMP(precision)` whose values hold at most `digits < precision` fractional digits,
    /// such as a `TO_TIMESTAMP` that Exasol 2025 types `TIMESTAMP(9)` over a format without
    /// `FF9`. The scan computes it at the unit holding `digits`, so no nanosecond range applies.
    CoarseTimestamp {
        precision: u8,
        digits: u8,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Partial {
    /// CHAR or VARCHAR.
    Text,
    /// DECIMAL(p,0).
    Integer,
    /// DOUBLE or DECIMAL(p,s): a number not proven to be an integer.
    Number,
    /// TIMESTAMP of a precision the pass does not prove.
    Timestamp,
}

const BOOLEAN: Inferred = Inferred::Exact(ExasolType::Boolean);
const DATE: Inferred = Inferred::Exact(ExasolType::Date);

impl Inferred {
    pub(crate) fn exact(self) -> Option<ExasolType> {
        match self {
            Self::Exact(ty) => Some(ty),
            Self::CoarseTimestamp { precision, .. } => Some(ExasolType::timestamp(precision)),
            _ => None,
        }
    }

    /// `self` with values of at most `digits` fractional digits.
    fn holding(self, digits: u8) -> Self {
        match self.timestamp_precision() {
            Some(precision) if digits < precision => Self::CoarseTimestamp { precision, digits },
            _ => self,
        }
    }

    /// The fractional digits a TIMESTAMP's values hold, which pick the unit the scan computes in.
    fn value_digits(self) -> Option<u8> {
        match self {
            Self::CoarseTimestamp { digits, .. } => Some(digits),
            other => other.timestamp_precision(),
        }
    }

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
            Self::Exact(ExasolType::Timestamp { precision })
            | Self::CoarseTimestamp { precision, .. } => Some(precision),
            _ => None,
        }
    }

    /// The type of a CASE, `GREATEST`, or `LEAST` over these branches, `self` first.
    fn union(self, other: Self, version: ExasolVersion) -> Self {
        let exact = match (self, other) {
            (Self::Null, ty) | (ty, Self::Null) => return ty,
            (DATE, timestamp) | (timestamp, DATE) if timestamp.is_timestamp() => {
                return match version {
                    ExasolVersion::Exasol8 => self,
                    ExasolVersion::Exasol2025 => timestamp,
                    ExasolVersion::Unknown => Self::Unknown,
                };
            }
            (Self::CoarseTimestamp { .. }, _) | (_, Self::CoarseTimestamp { .. }) => {
                let declared = |ty: Self| ty.exact().map_or(ty, Self::Exact);
                let union = declared(self).union(declared(other), version);
                return match self.value_digits().max(other.value_digits()) {
                    Some(digits) => union.holding(digits),
                    None => union,
                };
            }
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

/// Exasol's type for two branches brought to one type, where a live check proved it on every
/// version. [`Inferred::union`] types a DATE with a TIMESTAMP per version.
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
/// Synthetic node: a string function's numeric argument as the `Int64` DataFusion requires,
/// rounded as Exasol rounds it.
pub(crate) const INTEGER_ARGUMENT_NODE: &str = "scan_integer_argument";
const ROUND: &str = "scanRound";
const NEGATIVE_IS_NULL: &str = "scanNegativeIsNull";
/// Synthetic node: the seconds of its argument with `fraction` fractional digits, which only this
/// pass knows.
pub(crate) const SECOND_NODE: &str = "scan_second";

/// Synthetic node: its text argument with an empty string read as NULL, because Exasol has no
/// empty string and DataFusion keeps one as a value (#203).
pub(crate) const EMPTY_AS_NULL_NODE: &str = "scan_empty_as_null";

/// `DECIMAL(2+p, p)`: Exasol's type of the seconds of a `TIMESTAMP(p)` on every supported version.
pub(crate) fn seconds_type(fraction: u8) -> ExasolType {
    ExasolType::decimal(2 + fraction, fraction)
}

/// A TIMESTAMP that Exasol 2025 types with `precision` digits. Exasol 8 types every TIMESTAMP with
/// 3, so an unknown version proves only that one.
fn derived_timestamp(precision: u8, version: ExasolVersion) -> Inferred {
    let exasol_2025 = ExasolType::timestamp(precision);
    match version {
        ExasolVersion::Exasol8 => Inferred::Exact(ExasolType::BARE_TIMESTAMP),
        ExasolVersion::Exasol2025 => Inferred::Exact(exasol_2025),
        ExasolVersion::Unknown if exasol_2025 == ExasolType::BARE_TIMESTAMP => {
            Inferred::Exact(exasol_2025)
        }
        ExasolVersion::Unknown => Inferred::Partial(Partial::Timestamp),
    }
}

/// What the scan computes for a one-argument `SECOND(x)` or an `EXTRACT(SECOND FROM x)`.
#[derive(Clone, Copy)]
enum Seconds {
    /// The seconds of a `TIMESTAMP(p)` with its first p fractional digits.
    OfTimestamp { fraction: u8 },
    /// 0, which Exasol 8 types as a bare TIMESTAMP's seconds and Exasol 2025 as `DECIMAL(2,0)`.
    OfDate(ExasolVersion),
}

impl Seconds {
    /// `None` for a DATE on an unknown version.
    fn fraction(self) -> Option<u8> {
        match self {
            Self::OfTimestamp { fraction } => Some(fraction),
            Self::OfDate(ExasolVersion::Exasol8) => Some(TimestampUnit::Millisecond.digits()),
            Self::OfDate(ExasolVersion::Exasol2025) => Some(0),
            Self::OfDate(ExasolVersion::Unknown) => None,
        }
    }

    /// Without a fraction they are no integer for the text rule: their text differs by version.
    fn inferred(self) -> Inferred {
        self.fraction()
            .map_or(Inferred::Partial(Partial::Number), |fraction| {
                Inferred::Exact(seconds_type(fraction))
            })
    }

    fn node(self, argument: Json) -> Json {
        let fraction = self.fraction().unwrap_or(0);
        let argument = match self {
            Self::OfDate(_) if fraction > 0 => cast_node(ExasolType::timestamp(fraction), argument),
            _ => argument,
        };
        serde_json::json!({"type": SECOND_NODE, SECOND_FRACTION: fraction, "arguments": [argument]})
    }
}

const SECOND_FRACTION: &str = "fraction";

/// Set on a `TO_TIMESTAMP` node: the fractional digits it keeps, which only this pass knows.
const TIMESTAMP_DIGITS: &str = "scanDigits";

/// The fractional digits a `TO_TIMESTAMP` node keeps, when the typed pass derived them.
pub(crate) fn timestamp_digits(node: &Json) -> Option<u8> {
    u8::try_from(node.get(TIMESTAMP_DIGITS)?.as_u64()?).ok()
}

/// The fractional digits a [`SECOND_NODE`] keeps.
pub(crate) fn second_fraction(node: &Json) -> Option<u8> {
    let fraction = node.get(SECOND_FRACTION)?.as_u64()?;
    u8::try_from(fraction).ok()
}

/// How a node relates to the one-argument `SECOND(x)` and `EXTRACT(SECOND FROM x)`.
enum SecondCall {
    /// Any other node, `SECOND(x, n)` included.
    NotSecond,
    /// An argument the scan cannot take. Exasol itself fails `EXTRACT(SECOND)` of a DATE.
    Unsupported,
    Of(Seconds),
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

/// `node` at an [`IntegerArg`] position. A non-negative integer literal is already `Int64`.
fn to_integer_argument(
    node: Json,
    ty: Inferred,
    arg: IntegerArg,
    function: &str,
) -> Result<Json, UdfError> {
    let literal = kind(&node) == "literal_exactnumeric"
        && match &node["value"] {
            Json::Number(n) => n.is_u64(),
            Json::String(s) => s.parse::<i64>().is_ok_and(|v| v >= 0),
            _ => false,
        };
    if literal {
        return Ok(node);
    }
    let round = if ty.is_integer() {
        false
    } else if ty.is_numeric() {
        true
    } else {
        return Err(decline(format!(
            "{function} needs a number of known type for {}",
            short(&node)
        )));
    };
    let mut out = wrap(INTEGER_ARGUMENT_NODE, node);
    out[ROUND] = round.into();
    out[NEGATIVE_IS_NULL] = arg.negative_is_null.into();
    Ok(out)
}

/// The DataFusion SQL of an [`INTEGER_ARGUMENT_NODE`] over its rendered argument.
pub(crate) fn render_integer_argument(node: &Json, inner: &str) -> String {
    let flag = |field: &str| node.get(field).and_then(Json::as_bool).unwrap_or(false);
    let value = if flag(ROUND) {
        format!("CAST(round({inner}) AS BIGINT)")
    } else {
        format!("CAST({inner} AS BIGINT)")
    };
    if flag(NEGATIVE_IS_NULL) {
        format!("(CASE WHEN {value} >= 0 THEN {value} END)")
    } else {
        value
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

    fn result_type(self, args: &[Inferred], version: ExasolVersion) -> Inferred {
        let partial = Inferred::Partial;
        match self.result {
            ResultRule::Text => partial(Partial::Text),
            ResultRule::Integer | ResultRule::IntegerOverIntegers => partial(Partial::Integer),
            ResultRule::FixedInteger(precision) => {
                Inferred::Exact(ExasolType::decimal(precision, 0))
            }
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
            ResultRule::Unified => union_of(args, version),
            ResultRule::DateAddition(date_result) => match args.first() {
                Some(&DATE) => match date_result {
                    DateResult::Date => DATE,
                    DateResult::Timestamp => derived_timestamp(0, version),
                },
                Some(&first) if first.timestamp_precision().is_some() => first,
                _ => Inferred::Unknown,
            },
            ResultRule::Truncated => match args.get(1) {
                Some(&source) if source == DATE || source.is_timestamp() => source,
                _ => Inferred::Unknown,
            },
            ResultRule::Date => DATE,
            ResultRule::Timestamp => match args.first().copied() {
                Some(DATE) => derived_timestamp(0, version),
                Some(source) => match source.timestamp_precision() {
                    Some(precision) => derived_timestamp(precision, version),
                    None if source.is_text() => {
                        derived_timestamp(TimestampUnit::Nanosecond.digits(), version)
                    }
                    None => partial(Partial::Timestamp),
                },
                None => partial(Partial::Timestamp),
            },
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

/// What the typed pass reads besides the tree: the request's column types and the Exasol version.
/// Every entry to the pass goes through it, so no caller types with one and not the other.
#[derive(Clone, Copy)]
pub struct ScanTyping<'a> {
    columns: &'a dyn ColumnTypeLookup,
    version: ExasolVersion,
}

impl<'a> ScanTyping<'a> {
    pub fn new(columns: &'a dyn ColumnTypeLookup, version: ExasolVersion) -> Self {
        Self { columns, version }
    }

    /// The type Exasol gives `node`, where the typed pass proves it. `None` leaves the type to Exasol.
    pub fn exact_type(self, node: &Json) -> Option<ExasolType> {
        self.infer_type(node).exact()
    }

    /// An operand of a top-level `AND`, rendered alone only where it renders inside the `AND`,
    /// which refuses a text operand that Exasol rejects and DataFusion cannot combine.
    pub fn render_conjunct(self, conjunct: &Json) -> Result<String, UdfError> {
        refuse_implicit_text(conjunct, &[self.infer_type(conjunct)])?;
        self.render_filter_root(conjunct)
    }

    /// Exasol's type of `node`, whether or not the scan computes it. It reads the tree as Exasol sent
    /// it, because the scan's rewrites keep Exasol's semantics.
    pub(crate) fn infer_type(self, node: &Json) -> Inferred {
        if let Some(ty) = leaf_type(node, self.columns) {
            return ty;
        }
        let Ok(children) = ChildTypes::collect(node, |_, _, child| {
            Ok::<_, Infallible>(self.infer_type(child))
        });
        self.node_type(node, &children)
    }

    /// Rewrites `node` for the DataFusion dialect and derives its type, post-order. Text passes only
    /// where Exasol takes text; elsewhere Exasol converts it by the session's NLS settings.
    pub(crate) fn type_scan_tree(self, node: &Json) -> Result<(Json, Inferred), UdfError> {
        self.type_at(node, Position::Value)
    }

    /// [`Self::type_scan_tree`] for the root of a filter or of a join condition.
    pub(crate) fn type_scan_filter(self, node: &Json) -> Result<(Json, Inferred), UdfError> {
        self.type_at(node, Position::Filter)
    }

    fn type_at(self, node: &Json, position: Position) -> Result<(Json, Inferred), UdfError> {
        if let Some(ty) = leaf_type(node, self.columns) {
            return Ok((node.clone(), ty));
        }
        let mut out = node.clone();
        let bare = self.bare_form(node, position);
        let child_position = match kind(node) {
            "predicate_and" | "predicate_or" => position,
            _ => Position::Value,
        };
        let children = ChildTypes::collect(node, |field, index, child| {
            let (mut rewritten, ty) = self.type_at(child, child_position)?;
            if bare.is_none() {
                rewritten = empty_as_null(child, rewritten, ty);
            }
            match index {
                Some(i) => out[field][i] = rewritten,
                None => out[field] = rewritten,
            }
            Ok(ty)
        })?;
        let ty = self.node_type(node, &children);
        self.rewrite_for_scan(node, &mut out, &children, ty)?;
        if let Some(bare) = bare {
            bare.apply(&mut out);
        }
        Ok((out, ty))
    }

    /// The form of `node` that keeps its string columns bare, so statistics still prune it.
    fn bare_form(self, node: &Json, position: Position) -> Option<BareForm> {
        let text_column = |operand: &Json| {
            kind(operand) == "column"
                && leaf_type(operand, self.columns).is_some_and(Inferred::is_text)
        };
        let literal = |operand: &Json| matches!(kind(operand), "literal_string" | "literal_null");
        let subject = || {
            node.get("expression")
                .filter(|subject| text_column(subject))
        };
        let op = kind(node);
        match op {
            "predicate_is_null" => return subject().cloned().map(BareForm::OrEmpty),
            "predicate_is_not_null" => return subject().cloned().map(BareForm::Guarded),
            _ if position == Position::Value => return None,
            _ => {}
        }
        match op {
            "predicate_in_constlist" => {
                let values = node.get("arguments")?.as_array()?;
                (subject().is_some() && values.iter().all(literal)).then_some(BareForm::Unguarded)
            }
            "predicate_between" => {
                (subject().is_some() && literal(node.get("left")?) && literal(node.get("right")?))
                    .then_some(BareForm::Unguarded)
            }
            "predicate_like" => subject()
                .filter(|_| node.get("pattern").is_some_and(literal))
                .map(|column| BareForm::Guarded(column.clone())),
            "predicate_equal"
            | "predicate_notequal"
            | "predicate_less"
            | "predicate_lessequal"
            | "predicate_greater"
            | "predicate_greaterequal" => {
                let (left, right) = (node.get("left")?, node.get("right")?);
                let (column, op) = if text_column(left) && literal(right) {
                    (left, op)
                } else if literal(left) && text_column(right) {
                    (right, mirrored(op))
                } else if op == "predicate_equal" && text_column(left) && text_column(right) {
                    // Equal columns are both empty or neither, so one guard covers both.
                    return Some(BareForm::Guarded(left.clone()));
                } else {
                    return None;
                };
                Some(match op {
                    "predicate_notequal" | "predicate_less" | "predicate_lessequal" => {
                        BareForm::Guarded(column.clone())
                    }
                    _ => BareForm::Unguarded,
                })
            }
            _ => None,
        }
    }

    /// Exasol's type of a non-column `node` whose children are typed.
    fn node_type(self, node: &Json, children: &ChildTypes) -> Inferred {
        match self.second_call(node, children) {
            SecondCall::Of(seconds) => return seconds.inferred(),
            SecondCall::Unsupported => return Inferred::Unknown,
            SecondCall::NotSecond => {}
        }
        match kind(node) {
            "literal_null" => Inferred::Null,
            "literal_bool" => BOOLEAN,
            "literal_string" => Inferred::Partial(Partial::Text),
            "literal_double" => Inferred::Exact(ExasolType::DoublePrecision),
            "literal_exactnumeric" => exact_numeric_type(node),
            "literal_date" => DATE,
            "literal_timestamp" => timestamp_literal_type(node, self.version),
            k if k.starts_with("predicate_") => BOOLEAN,
            "function_scalar_extract" => node
                .get("toExtract")
                .and_then(Json::as_str)
                .map_or(Inferred::Unknown, |field| {
                    typed_function(field).result_type(&[], self.version)
                }),
            "function_scalar_case" => union_of(children.of("results"), self.version),
            "function_scalar_json_value" => json_value_type(node),
            _ if is_cast(node) => cast_target(node).map_or(Inferred::Unknown, Inferred::Exact),
            "function_scalar" => {
                let function = typed_function(&upper_name(node));
                let ty = function.result_type(children.of("arguments"), self.version);
                match format_digits(node, function) {
                    Some(digits) => ty.holding(digits),
                    None => ty,
                }
            }
            "function_aggregate" => aggregate_type(&upper_name(node), children.of("arguments")),
            _ => Inferred::Unknown,
        }
    }

    /// Declines what the scan cannot compute as Exasol does, and rewrites `out` for DataFusion.
    /// `ty` is the node's [`Self::node_type`].
    fn rewrite_for_scan(
        self,
        node: &Json,
        out: &mut Json,
        children: &ChildTypes,
        ty: Inferred,
    ) -> Result<(), UdfError> {
        match self.second_call(node, children) {
            SecondCall::Of(seconds) => {
                *out = seconds.node(out["arguments"][0].take());
                return Ok(());
            }
            SecondCall::Unsupported => {
                return Err(decline(format!(
                    "SECOND needs an argument of the exact type TIMESTAMP(p), or DATE outside EXTRACT: {}",
                    short(node)
                )));
            }
            SecondCall::NotSecond => {}
        }
        match kind(node) {
            "predicate_like" => {
                for field in ["expression", "pattern"] {
                    if let Some(ty) = children.one(field) {
                        out[field] = to_text(out[field].take(), ty)?;
                    }
                }
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
            }
            "predicate_is_null" | "predicate_is_not_null" => {}
            "function_scalar_case" => {
                // A simple CASE compares its basis with each WHEN value.
                if children.one("basis").is_some() {
                    type_comparison(node, out, children)?;
                } else {
                    refuse_implicit_text(node, children.of("arguments"))?;
                }
                unify_branches(node, out, "results", children.of("results"), ty)?;
            }
            "function_scalar_json_value" => check_json_value(node, children.of("arguments"))?,
            _ if is_cast(node) => rewrite_cast(out, children.of("arguments"))?,
            "function_scalar" => rewrite_scalar_function(node, out, children.of("arguments"), ty)?,
            "function_aggregate" => check_aggregate(node, children.of("arguments"))?,
            _ => refuse_implicit_text(node, &children.all())?,
        }
        Ok(())
    }

    fn second_call(self, node: &Json, children: &ChildTypes) -> SecondCall {
        let (name, extract) = match kind(node) {
            "function_scalar_extract" => (
                node.get("toExtract")
                    .and_then(Json::as_str)
                    .map(str::to_uppercase),
                true,
            ),
            "function_scalar" => (Some(upper_name(node)), false),
            _ => (None, false),
        };
        if name.as_deref() != Some("SECOND") {
            return SecondCall::NotSecond;
        }
        match children.of("arguments") {
            [Inferred::Exact(ExasolType::Date)] if !extract => {
                SecondCall::Of(Seconds::OfDate(self.version))
            }
            [argument] => match argument.timestamp_precision() {
                Some(fraction) if fraction <= MAX_FRACTIONAL_DIGITS => {
                    SecondCall::Of(Seconds::OfTimestamp { fraction })
                }
                _ => SecondCall::Unsupported,
            },
            _ => SecondCall::NotSecond,
        }
    }
}

/// Where a node sits. Under the root of a filter or of a join condition, reached through `AND` and
/// `OR` only, a FALSE result drops a row exactly as a NULL one does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Position {
    Filter,
    Value,
}

/// `typed`, the rewrite of the operand `node`, with a string column's empty strings read as NULL.
pub(crate) fn empty_as_null(node: &Json, typed: Json, ty: Inferred) -> Json {
    if kind(node) == "column" && ty.is_text() {
        wrap(EMPTY_AS_NULL_NODE, typed)
    } else {
        typed
    }
}

/// A predicate over bare string columns that reads an empty string as NULL without wrapping the
/// column, which statistics pruning could not read through.
enum BareForm {
    /// A predicate that is never TRUE for an empty string.
    Unguarded,
    /// A predicate that could be TRUE for an empty string, so it adds that the column is not one.
    Guarded(Json),
    /// `IS NULL`, which also holds for an empty string.
    OrEmpty(Json),
}

impl BareForm {
    fn apply(self, out: &mut Json) {
        let (junction, comparison, column) = match self {
            Self::Unguarded => return,
            Self::Guarded(column) => ("predicate_and", "predicate_notequal", column),
            Self::OrEmpty(column) => ("predicate_or", "predicate_equal", column),
        };
        *out = serde_json::json!({
            "type": junction,
            "expressions": [out.take(), {
                "type": comparison,
                "left": column,
                "right": {"type": "literal_string", "value": ""},
            }],
        });
    }
}

fn leaf_type(node: &Json, columns: &dyn ColumnTypeLookup) -> Option<Inferred> {
    if !node.is_object() {
        Some(Inferred::Unknown)
    } else if kind(node) == "column" {
        Some(
            columns
                .column_type(node)
                .map_or(Inferred::Unknown, Inferred::Exact),
        )
    } else {
        None
    }
}

/// The types of a node's typed child expressions, by field.
#[derive(Default)]
struct ChildTypes {
    arrays: Vec<(&'static str, Vec<Inferred>)>,
    singles: Vec<(&'static str, Inferred)>,
}

impl ChildTypes {
    /// Types each child expression of `node`, given its field and its index in an array field.
    fn collect<E>(
        node: &Json,
        mut type_child: impl FnMut(&'static str, Option<usize>, &Json) -> Result<Inferred, E>,
    ) -> Result<Self, E> {
        let mut children = Self::default();
        for field in EXPR_ARRAY_FIELDS {
            if let Some(Json::Array(items)) = node.get(field) {
                let types = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| type_child(field, Some(i), item))
                    .collect::<Result<_, _>>()?;
                children.arrays.push((field, types));
            }
        }
        for field in EXPR_SINGLE_FIELDS {
            if let Some(child) = node.get(field)
                && child.is_object()
            {
                children
                    .singles
                    .push((field, type_child(field, None, child)?));
            }
        }
        Ok(children)
    }

    fn of(&self, field: &str) -> &[Inferred] {
        self.arrays
            .iter()
            .find(|(f, _)| *f == field)
            .map_or(&[], |(_, c)| c.as_slice())
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

/// The `n` of a `TO_TIMESTAMP` format's `FFn`, or 0, when the format is a literal it translates.
fn format_digits(node: &Json, function: ScalarFunction) -> Option<u8> {
    if function.result != ResultRule::Timestamp {
        return None;
    }
    format_literal(node.get("arguments")?.get(1)?).and_then(fraction_digits)
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

fn timestamp_literal_type(node: &Json, version: ExasolVersion) -> Inferred {
    let precision = node
        .get("value")
        .and_then(Json::as_str)
        .and_then(|value| u8::try_from(TimestampLiteral(value).type_precision()).ok());
    match precision {
        Some(p) if p <= TimestampUnit::Nanosecond.digits() => derived_timestamp(p, version),
        _ => Inferred::Partial(Partial::Timestamp),
    }
}

fn rewrite_scalar_function(
    node: &Json,
    out: &mut Json,
    args: &[Inferred],
    ty: Inferred,
) -> Result<(), UdfError> {
    let function = typed_function(&upper_name(node));
    let text_positions = function.string_positions(args.len());
    for &i in &text_positions {
        out["arguments"][i] = to_text(out["arguments"][i].take(), args[i])?;
    }
    let integer_positions: Vec<usize> = function
        .integer_args()
        .iter()
        .filter(|arg| arg.position < args.len())
        .map(|&arg| {
            let i = arg.position;
            out["arguments"][i] =
                to_integer_argument(out["arguments"][i].take(), args[i], arg, function.name)?;
            Ok(i)
        })
        .collect::<Result<_, UdfError>>()?;
    if function.result == ResultRule::Unified {
        return unify_branches(node, out, "arguments", args, ty);
    }
    // An unknown version proves no digits, so the format's own `FFn` gives them.
    if function.result == ResultRule::Timestamp
        && let Some(digits) = ty.value_digits().or_else(|| format_digits(node, function))
    {
        out[TIMESTAMP_DIGITS] = digits.into();
    }
    if let ResultRule::DateAddition(_) = function.result {
        check_date_addition(node, out, args)?;
    } else {
        let converted: Vec<Inferred> = args
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                !text_positions.contains(i)
                    && !integer_positions.contains(i)
                    && !function.parameters.contains(i)
            })
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
        ResultRule::Text => *out = wrap(EMPTY_AS_NULL_NODE, out.take()),
        _ => {}
    }
    Ok(())
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
    } else if let Some(digits) = first.value_digits() {
        ExasolType::timestamp(
            TimestampUnit::holding(digits)
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

fn is_numeric_aggregate(name: &str) -> bool {
    matches!(
        name,
        "SUM"
            | "AVG"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
    )
}

/// Exasol's result type of an aggregate, each rule checked live (#476).
fn aggregate_type(name: &str, args: &[Inferred]) -> Inferred {
    match name {
        "COUNT" => Inferred::Exact(ExasolType::decimal(18, 0)),
        "MIN" | "MAX" => args.first().copied().unwrap_or(Inferred::Unknown),
        "SUM" => match args {
            [Inferred::Exact(ExasolType::Decimal(d))] => Inferred::Exact(ExasolType::decimal(
                (d.precision() + 11).min(MAX_DECIMAL_PRECISION),
                d.scale(),
            )),
            [Inferred::Exact(ExasolType::DoublePrecision)] => {
                Inferred::Exact(ExasolType::DoublePrecision)
            }
            _ if args.iter().all(|ty| ty.is_integer()) => Inferred::Partial(Partial::Integer),
            _ if args.iter().all(|ty| ty.is_numeric()) => Inferred::Partial(Partial::Number),
            _ => Inferred::Unknown,
        },
        _ if is_numeric_aggregate(name) && args.iter().all(|ty| ty.is_numeric()) => {
            Inferred::Exact(ExasolType::DoublePrecision)
        }
        _ => Inferred::Unknown,
    }
}

fn check_aggregate(node: &Json, args: &[Inferred]) -> Result<(), UdfError> {
    let name = upper_name(node);
    match name.as_str() {
        "COUNT" | "MIN" | "MAX" => Ok(()),
        n if is_numeric_aggregate(n) => check_numeric_aggregate(node, n, args),
        _ => refuse_implicit_text(node, args),
    }
}

/// The type the RETURNING clause states, or Exasol's default without one.
fn json_value_type(node: &Json) -> Inferred {
    returning_type(node).map_or(Inferred::Unknown, Inferred::Exact)
}

fn check_json_value(node: &Json, args: &[Inferred]) -> Result<(), UdfError> {
    if !args.first().is_some_and(|ty| ty.is_text()) {
        return Err(decline(format!(
            "JSON_VALUE over a document that is not text: {}",
            short(node)
        )));
    }
    JsonValueCall::parse(node).map(drop)
}

/// The most digits of a text literal the scan compares with an integer as a number.
const MAX_INTEGER_TEXT_DIGITS: u8 = 18;

/// The digit count of `text` when it is digits with an optional leading minus sign.
fn integer_digits(text: &str) -> Option<u8> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    u8::try_from(digits.len()).ok()
}

/// A literal of at most `precision` digits, and at most [`MAX_INTEGER_TEXT_DIGITS`], reads as the
/// same integer in every session, so comparing it with an integer of that precision needs no
/// session setting. Plan-time pruning reads the same literals as numbers.
pub fn is_session_free_integer_text(node: &Json, precision: u8) -> bool {
    kind(node) == "literal_string"
        && node
            .get("value")
            .and_then(Json::as_str)
            .and_then(integer_digits)
            .is_some_and(|digits| digits <= precision.min(MAX_INTEGER_TEXT_DIGITS))
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
        operands.extend(args.iter().zip(children.of("arguments").iter().copied()));
    }
    compare_without_session_parse(node, out, &operands)?;
    compare_dates_as_timestamps(out, children, &operands);
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
    let max_digits = non_text
        .iter()
        .try_fold(MAX_INTEGER_TEXT_DIGITS, |max, ty| {
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
    rewrite_operands(out, children, BOOLEAN, |operand| {
        wrap(BOOLEAN_NUMBER_NODE, operand)
    });
    Ok(())
}

/// A DATE compared with a TIMESTAMP gets a CAST to the timestamp's unit ([`common_timestamp`]).
fn compare_dates_as_timestamps(
    out: &mut Json,
    children: &ChildTypes,
    operands: &[(&Json, Inferred)],
) {
    let types: Vec<Inferred> = operands.iter().map(|(_, ty)| *ty).collect();
    if let Some(target) = common_timestamp(&types) {
        rewrite_operands(out, children, DATE, |operand| cast_node(target, operand));
    }
}

fn rewrite_operands(
    out: &mut Json,
    children: &ChildTypes,
    ty: Inferred,
    rewrite: impl Fn(Json) -> Json,
) {
    for field in COMPARED_FIELDS {
        if children.one(field) == Some(ty) {
            out[field] = rewrite(out[field].take());
        }
    }
    for (i, operand) in children.of("arguments").iter().enumerate() {
        if *operand == ty {
            out["arguments"][i] = rewrite(out["arguments"][i].take());
        }
    }
}

/// DataFusion truncates a timestamp literal to its TIMESTAMP operand's unit, while Exasol compares
/// at full precision (#461, apache/datafusion#22142, fixed by apache/datafusion#22906). A DATE
/// needs no floor, because it compares at the unit that holds the literal.
fn floor_timestamp_literals(node: &Json, out: &mut Json, children: &ChildTypes) {
    let precision = |field: &str| children.one(field)?.value_digits();
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
fn check_numeric_aggregate(node: &Json, name: &str, args: &[Inferred]) -> Result<(), UdfError> {
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
    Ok(())
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
/// Branches of DATEs and TIMESTAMPs each get a CAST to their [`common_timestamp`], or to DATE where
/// Exasol 8 brings a TIMESTAMP after a DATE to the DATE.
fn unify_branches(
    node: &Json,
    out: &mut Json,
    field: &str,
    branches: &[Inferred],
    unified: Inferred,
) -> Result<(), UdfError> {
    refuse_text_among_others(node, branches)?;
    if unified.exact() == Some(ExasolType::Date) {
        for (i, branch) in branches.iter().enumerate() {
            if branch.is_timestamp() {
                out[field][i] = cast_node(ExasolType::Date, out[field][i].take());
            }
        }
    } else if let Some(target) = common_timestamp(branches) {
        for (i, branch) in branches.iter().enumerate() {
            if *branch != Inferred::Null {
                out[field][i] = cast_node(target, out[field][i].take());
            }
        }
    }
    Ok(())
}

/// The TIMESTAMP at the unit holding the finest `TIMESTAMP(p)` among `types`, when all others
/// are DATE or NULL. DataFusion otherwise brings a DATE and a TIMESTAMP to nanoseconds, which
/// span only 1677 to 2262 (#642), and two TIMESTAMPs to the finer unit of their two.
fn common_timestamp(types: &[Inferred]) -> Option<ExasolType> {
    let finest = types
        .iter()
        .filter(|ty| !matches!(**ty, Inferred::Null | DATE))
        .try_fold(None, |finest: Option<u8>, ty| {
            Some(finest.max(Some(ty.value_digits()?)))
        })??;
    Some(ExasolType::timestamp(
        TimestampUnit::holding(finest).digits(),
    ))
}

fn union_of(branches: &[Inferred], version: ExasolVersion) -> Inferred {
    branches
        .iter()
        .copied()
        .reduce(|first, other| first.union(other, version))
        .unwrap_or(Inferred::Unknown)
}

fn is_cast(node: &Json) -> bool {
    match kind(node) {
        "function_scalar_cast" => true,
        "function_scalar" => upper_name(node) == "CAST",
        _ => false,
    }
}

fn cast_target(node: &Json) -> Result<ExasolType, UdfError> {
    node.get("dataType")
        .map(exasol_type_from_json)
        .transpose()?
        .ok_or_else(|| UdfError::User("CAST missing 'dataType'".into()))
}

fn rewrite_cast(out: &mut Json, args: &[Inferred]) -> Result<(), UdfError> {
    let target = cast_target(out)?;
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
    Ok(())
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
