//! Exasol's type of a request expression, and the typed pass of every expression the scan
//! computes. The pass declines a conversion to or from text that Exasol formats by session
//! settings the pushdown request does not carry (#587).
use std::convert::Infallible;

use exasol_udf_sdk::error::UdfError;

use crate::datetime_format::fraction_digits;
use crate::exasol_type::{Decimal, ExasolType, MAX_DECIMAL_PRECISION, MAX_FRACTIONAL_DIGITS};
use crate::expr::{
    Column, CompareOp, Expr, JsonValueCall, Literal, LiteralKind, Scalar, ScalarCall, ScanKind,
    take,
};
use crate::json_value::{ScanJsonValue, returning_type};
use crate::scalar_functions::{
    DateResult, IntegerArg, ResultRule, ScalarFunction, TextArgs, UNLISTED, scalar_function,
};
use crate::short;
use crate::timestamp::{TimestampLiteral, TimestampUnit};
use crate::version::ExasolVersion;

/// The Exasol type of a `column` node, keyed however the caller binds columns.
pub trait ColumnTypeLookup {
    fn column_type(&self, column: &Column) -> Option<ExasolType>;
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
const DOUBLE: Inferred = Inferred::Exact(ExasolType::DoublePrecision);

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
            // Exasol 8, 2025, and 2026 type a DOUBLE among numbers as DOUBLE, decimal literals too.
            (DOUBLE, number) | (number, DOUBLE) if number.is_numeric() => return DOUBLE,
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

    fn node(self, argument: Expr) -> Expr {
        let fraction = self.fraction().unwrap_or(0);
        let argument = match self {
            Self::OfDate(_) if fraction > 0 => cast_node(ExasolType::timestamp(fraction), argument),
            _ => argument,
        };
        Expr::scan(ScanKind::Second {
            argument: Box::new(argument),
            fraction,
        })
    }
}

/// How a node relates to the one-argument `SECOND(x)` and `EXTRACT(SECOND FROM x)`.
enum SecondCall {
    /// Any other node, `SECOND(x, n)` included.
    NotSecond,
    /// An argument the scan cannot take. Exasol itself fails `EXTRACT(SECOND)` of a DATE.
    Unsupported,
    Of(Seconds),
}

/// A field of a node that holds expressions the typed pass types. Curated: an `ESCAPE` character
/// and a JSON_VALUE `DEFAULT` are left as sent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Field {
    /// The operands of `AND` and `OR`.
    Expressions,
    /// A call's arguments, an IN list's values, or a CASE's WHEN values.
    Arguments,
    /// A CASE's THEN and ELSE results.
    Results,
    /// The subject of a unary predicate, IN, BETWEEN, or LIKE.
    Expression,
    Pattern,
    /// A comparison's left operand or BETWEEN's lower bound.
    Left,
    /// A comparison's right operand or BETWEEN's upper bound.
    Right,
    Basis,
}

/// The fields that hold a predicate's operands or a simple CASE's basis. [`Field::Arguments`]
/// holds the rest: an IN list or the WHEN values.
const COMPARED_FIELDS: [Field; 4] = [Field::Expression, Field::Left, Field::Right, Field::Basis];

/// Each typed child of `node` with its field and its index in an array field, the array fields
/// first.
fn typed_children(node: &Expr) -> Vec<(Field, Option<usize>, &Expr)> {
    fn array<'e>(field: Field, items: &[&'e Expr]) -> Vec<(Field, Option<usize>, &'e Expr)> {
        items
            .iter()
            .enumerate()
            .map(|(i, item)| (field, Some(i), *item))
            .collect()
    }
    fn refs(items: &[Expr]) -> Vec<&Expr> {
        items.iter().collect()
    }
    match node {
        Expr::Literal(_) | Expr::Column(_) => Vec::new(),
        Expr::And(items) | Expr::Or(items) => array(Field::Expressions, &refs(items)),
        Expr::Scalar(ScalarCall { arguments, .. }) => array(Field::Arguments, &refs(arguments)),
        Expr::Aggregate(call) => array(Field::Arguments, &refs(&call.arguments)),
        Expr::Extract { argument, .. } | Expr::Cast { argument, .. } => {
            array(Field::Arguments, &[argument])
        }
        Expr::Scan(scan) => array(Field::Arguments, &[scan.0.argument()]),
        Expr::JsonValue(call) => array(Field::Arguments, &[&call.document, &call.path]),
        Expr::Case(case) => {
            let mut children = array(Field::Arguments, &refs(&case.whens));
            children.extend(array(Field::Results, &refs(&case.results)));
            children.extend(
                case.basis
                    .as_deref()
                    .map(|basis| (Field::Basis, None, basis)),
            );
            children
        }
        Expr::InList { expression, values } => {
            let mut children = array(Field::Arguments, &refs(values));
            children.push((Field::Expression, None, expression));
            children
        }
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            vec![(Field::Expression, None, inner)]
        }
        Expr::Between {
            expression,
            low,
            high,
        } => vec![
            (Field::Expression, None, expression),
            (Field::Left, None, low),
            (Field::Right, None, high),
        ],
        Expr::Like {
            expression,
            pattern,
            ..
        } => vec![
            (Field::Expression, None, expression),
            (Field::Pattern, None, pattern),
        ],
        Expr::Compare { left, right, .. } => {
            vec![(Field::Left, None, left), (Field::Right, None, right)]
        }
    }
}

/// The typed child of `node` at `field` and `index`, as [`typed_children`] lists it.
fn slot(node: &mut Expr, field: Field, index: Option<usize>) -> Option<&mut Expr> {
    let i = index.unwrap_or(0);
    match field {
        Field::Expressions => match node {
            Expr::And(items) | Expr::Or(items) => items.get_mut(i),
            _ => None,
        },
        Field::Arguments => match node {
            Expr::Scalar(ScalarCall { arguments, .. }) => arguments.get_mut(i),
            Expr::Aggregate(call) => call.arguments.get_mut(i),
            Expr::Case(case) => case.whens.get_mut(i),
            Expr::InList { values, .. } => values.get_mut(i),
            Expr::Extract { argument, .. } | Expr::Cast { argument, .. } => {
                (i == 0).then_some(&mut **argument)
            }
            Expr::Scan(scan) => (i == 0).then(|| scan.0.argument_mut()),
            Expr::JsonValue(call) => match i {
                0 => Some(&mut *call.document),
                1 => Some(&mut *call.path),
                _ => None,
            },
            _ => None,
        },
        Field::Results => match node {
            Expr::Case(case) => case.results.get_mut(i),
            _ => None,
        },
        Field::Basis => match node {
            Expr::Case(case) => case.basis.as_deref_mut(),
            _ => None,
        },
        Field::Expression => match node {
            Expr::InList { expression, .. }
            | Expr::Between { expression, .. }
            | Expr::Like { expression, .. }
            | Expr::Not(expression)
            | Expr::IsNull(expression)
            | Expr::IsNotNull(expression) => Some(&mut **expression),
            _ => None,
        },
        Field::Pattern => match node {
            Expr::Like { pattern, .. } => Some(&mut **pattern),
            _ => None,
        },
        Field::Left => match node {
            Expr::Between { low: operand, .. } | Expr::Compare { left: operand, .. } => {
                Some(&mut **operand)
            }
            _ => None,
        },
        Field::Right => match node {
            Expr::Between { high: operand, .. } | Expr::Compare { right: operand, .. } => {
                Some(&mut **operand)
            }
            _ => None,
        },
    }
}

/// Replaces the typed child at `field` and `index` of `node` with `rewrite`'s result.
fn rewrite_slot<E>(
    node: &mut Expr,
    field: Field,
    index: Option<usize>,
    rewrite: impl FnOnce(Expr) -> Result<Expr, E>,
) -> Result<(), E> {
    let child = slot(node, field, index).expect("a typed child the node holds");
    *child = rewrite(take(child))?;
    Ok(())
}

pub(crate) fn decline(reason: String) -> UdfError {
    UdfError::User(format!("the scan cannot compute this expression: {reason}"))
}

fn wrap(kind: fn(Box<Expr>) -> ScanKind, argument: Expr) -> Expr {
    Expr::scan(kind(Box::new(argument)))
}

/// `node` converted to text implicitly, as a LIKE operand, a `||` operand, or a string-function
/// argument.
fn to_text(node: Expr, ty: Inferred) -> Result<Expr, UdfError> {
    if ty.is_text() || ty == Inferred::Null {
        Ok(node)
    } else if ty.is_integer() {
        Ok(wrap(ScanKind::IntegerText, node))
    } else if ty == BOOLEAN {
        Ok(wrap(ScanKind::BooleanText, node))
    } else {
        Err(decline(format!(
            "converting {} to text depends on the session's NLS settings",
            short(&node)
        )))
    }
}

/// `node` at an [`IntegerArg`] position. A non-negative integer literal is already `Int64`.
fn to_integer_argument(
    node: Expr,
    ty: Inferred,
    arg: IntegerArg,
    function: &str,
) -> Result<Expr, UdfError> {
    let literal = match node.as_literal() {
        Some(Literal {
            kind: LiteralKind::ExactNumeric,
            value,
        }) => match value {
            Scalar::Number(n) => n.is_u64(),
            Scalar::String(s) => s.parse::<i64>().is_ok_and(|v| v >= 0),
            _ => false,
        },
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
    Ok(Expr::scan(ScanKind::IntegerArgument {
        argument: Box::new(node),
        round,
        negative_is_null: arg.negative_is_null,
    }))
}

/// The DataFusion SQL of an integer-argument node over its rendered argument.
pub(crate) fn render_integer_argument(round: bool, negative_is_null: bool, inner: &str) -> String {
    let value = if round {
        format!("CAST(round({inner}) AS BIGINT)")
    } else {
        format!("CAST({inner} AS BIGINT)")
    };
    if negative_is_null {
        format!("(CASE WHEN {value} >= 0 THEN {value} END)")
    } else {
        value
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
    source: Expr,
    ty: Inferred,
    target: ExasolType,
    size: u32,
    is_char: bool,
) -> Result<Expr, UdfError> {
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
        Ok(wrap(ScanKind::IntegerText, source))
    } else if ty == BOOLEAN {
        if size < 5 {
            return refuse("may not fit FALSE");
        }
        Ok(wrap(ScanKind::BooleanText, source))
    } else {
        refuse("depends on the session's NLS settings")
    }
}

/// A CAST from text to a type Exasol parses by session settings (`NLS_DATE_FORMAT`,
/// `NLS_TIMESTAMP_FORMAT`, `NLS_NUMERIC_CHARACTERS`).
fn cast_from_text(source: &Expr, ty: Inferred, target: ExasolType) -> Result<(), UdfError> {
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
    pub fn exact_type(self, node: &Expr) -> Option<ExasolType> {
        self.infer_type(node).exact()
    }

    /// An operand of a top-level `AND`, rendered alone only where it renders inside the `AND`,
    /// which refuses a text operand that Exasol rejects and DataFusion cannot combine.
    pub fn render_conjunct(self, conjunct: &Expr) -> Result<String, UdfError> {
        refuse_implicit_text(conjunct, &[self.infer_type(conjunct)])?;
        self.render_filter_root(conjunct)
    }

    /// Exasol's type of `node`, whether or not the scan computes it. It reads the tree as Exasol sent
    /// it, because the scan's rewrites keep Exasol's semantics.
    pub(crate) fn infer_type(self, node: &Expr) -> Inferred {
        if let Some(ty) = self.leaf_type(node) {
            return ty;
        }
        let Ok(children) = ChildTypes::collect(node, |_, _, child| {
            Ok::<_, Infallible>(self.infer_type(child))
        });
        self.node_type(node, &children)
    }

    /// Rewrites `node` for the DataFusion dialect and derives its type, post-order. Text passes only
    /// where Exasol takes text; elsewhere Exasol converts it by the session's NLS settings.
    pub(crate) fn type_scan_tree(self, node: &Expr) -> Result<(Expr, Inferred), UdfError> {
        self.type_at(node, Position::Value)
    }

    /// [`Self::type_scan_tree`] for the root of a filter or of a join condition.
    pub(crate) fn type_scan_filter(self, node: &Expr) -> Result<(Expr, Inferred), UdfError> {
        self.type_at(node, Position::Filter)
    }

    fn type_at(self, node: &Expr, position: Position) -> Result<(Expr, Inferred), UdfError> {
        if let Some(ty) = self.leaf_type(node) {
            return Ok((node.clone(), ty));
        }
        let bare = self.bare_form(node, position);
        let child_position = match node {
            Expr::And(_) | Expr::Or(_) => position,
            _ => Position::Value,
        };
        let mut rewritten = Vec::new();
        let children = ChildTypes::collect(node, |field, index, child| {
            let (mut typed, ty) = self.type_at(child, child_position)?;
            if bare.is_none() {
                typed = empty_as_null(typed, ty);
            }
            rewritten.push((field, index, typed));
            Ok(ty)
        })?;
        let mut out = node.clone();
        for (field, index, typed) in rewritten {
            rewrite_slot(&mut out, field, index, |_| Ok::<_, Infallible>(typed))
                .unwrap_or_else(|never| match never {});
        }
        let ty = self.node_type(node, &children);
        self.rewrite_for_scan(node, &mut out, &children, ty)?;
        if let Some(bare) = bare {
            bare.apply(&mut out);
        }
        Ok((out, ty))
    }

    fn leaf_type(self, node: &Expr) -> Option<Inferred> {
        let Expr::Column(column) = node else {
            return None;
        };
        Some(
            self.columns
                .column_type(column)
                .map_or(Inferred::Unknown, Inferred::Exact),
        )
    }

    /// The form of `node` that keeps its string columns bare, so statistics still prune it.
    fn bare_form(self, node: &Expr, position: Position) -> Option<BareForm> {
        let text_column = |operand: &Expr| {
            matches!(operand, Expr::Column(_))
                && self.leaf_type(operand).is_some_and(Inferred::is_text)
        };
        let literal = |operand: &Expr| {
            matches!(
                operand.as_literal().map(|literal| literal.kind),
                Some(LiteralKind::String | LiteralKind::Null)
            )
        };
        match node {
            Expr::IsNull(subject) => {
                return text_column(subject).then(|| BareForm::OrEmpty((**subject).clone()));
            }
            Expr::IsNotNull(subject) => {
                return text_column(subject).then(|| BareForm::Guarded((**subject).clone()));
            }
            _ if position == Position::Value => return None,
            _ => {}
        }
        match node {
            Expr::InList { expression, values } => (text_column(expression)
                && values.iter().all(literal))
            .then_some(BareForm::Unguarded),
            Expr::Between {
                expression,
                low,
                high,
            } => (text_column(expression) && literal(low) && literal(high))
                .then_some(BareForm::Unguarded),
            Expr::Like {
                expression,
                pattern,
                ..
            } => (text_column(expression) && literal(pattern))
                .then(|| BareForm::Guarded((**expression).clone())),
            Expr::Compare { op, left, right } => {
                let (column, op) = if text_column(left) && literal(right) {
                    (left, *op)
                } else if literal(left) && text_column(right) {
                    (right, op.mirrored())
                } else if *op == CompareOp::Equal && text_column(left) && text_column(right) {
                    // Equal columns are both empty or neither, so one guard covers both.
                    return Some(BareForm::Guarded((**left).clone()));
                } else {
                    return None;
                };
                Some(match op {
                    CompareOp::NotEqual | CompareOp::Less | CompareOp::LessEqual => {
                        BareForm::Guarded((**column).clone())
                    }
                    _ => BareForm::Unguarded,
                })
            }
            _ => None,
        }
    }

    /// Exasol's type of a non-column `node` whose children are typed.
    fn node_type(self, node: &Expr, children: &ChildTypes) -> Inferred {
        match self.second_call(node, children) {
            SecondCall::Of(seconds) => return seconds.inferred(),
            SecondCall::Unsupported => return Inferred::Unknown,
            SecondCall::NotSecond => {}
        }
        match node {
            Expr::Literal(literal) => match literal.kind {
                LiteralKind::Null => Inferred::Null,
                LiteralKind::Bool => BOOLEAN,
                LiteralKind::String => Inferred::Partial(Partial::Text),
                LiteralKind::Double => Inferred::Exact(ExasolType::DoublePrecision),
                LiteralKind::ExactNumeric => exact_numeric_type(literal),
                LiteralKind::Date => DATE,
                LiteralKind::Timestamp => timestamp_literal_type(literal, self.version),
                LiteralKind::TimestampUtc
                | LiteralKind::LocalTimeZoneTimestamp
                | LiteralKind::Binary { .. } => Inferred::Unknown,
            },
            Expr::Compare { .. }
            | Expr::And(_)
            | Expr::Or(_)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. } => BOOLEAN,
            Expr::Extract { field, .. } => typed_function(field).result_type(&[], self.version),
            Expr::Case(_) => union_of(children.of(Field::Results), self.version),
            Expr::JsonValue(call) => Inferred::Exact(returning_type(call)),
            Expr::Cast { target, .. } => Inferred::Exact(*target),
            Expr::Scalar(call) => {
                let function = typed_function(&call.name);
                let ty = function.result_type(children.of(Field::Arguments), self.version);
                match format_digits(call, function) {
                    Some(digits) => ty.holding(digits),
                    None => ty,
                }
            }
            Expr::Aggregate(call) => aggregate_type(&call.name, children.of(Field::Arguments)),
            Expr::Column(_) | Expr::Scan(_) => Inferred::Unknown,
        }
    }

    /// Declines what the scan cannot compute as Exasol does, and rewrites `out` for DataFusion.
    /// `ty` is the node's [`Self::node_type`].
    fn rewrite_for_scan(
        self,
        node: &Expr,
        out: &mut Expr,
        children: &ChildTypes,
        ty: Inferred,
    ) -> Result<(), UdfError> {
        match self.second_call(node, children) {
            SecondCall::Of(seconds) => {
                let argument = slot(out, Field::Arguments, Some(0)).expect("SECOND's argument");
                *out = seconds.node(take(argument));
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
        match node {
            Expr::Like { .. } => {
                for field in [Field::Expression, Field::Pattern] {
                    if let Some(ty) = children.one(field) {
                        rewrite_slot(out, field, None, |operand| to_text(operand, ty))?;
                    }
                }
            }
            Expr::Compare { .. } | Expr::Between { .. } | Expr::InList { .. } => {
                type_comparison(node, out, children)?;
                floor_timestamp_literals(node, out, children);
            }
            Expr::IsNull(_) | Expr::IsNotNull(_) => {}
            Expr::Case(_) => {
                // A simple CASE compares its basis with each WHEN value.
                if children.one(Field::Basis).is_some() {
                    type_comparison(node, out, children)?;
                } else {
                    refuse_implicit_text(node, children.of(Field::Arguments))?;
                }
                unify_branches(node, out, Field::Results, children.of(Field::Results), ty)?;
            }
            Expr::JsonValue(call) => check_json_value(call, children.of(Field::Arguments))?,
            Expr::Cast { .. } => rewrite_cast(out, children.of(Field::Arguments))?,
            Expr::Scalar(call) => {
                rewrite_scalar_function(call, out, children.of(Field::Arguments), ty)?;
            }
            Expr::Aggregate(call) => {
                check_aggregate(node, &call.name, children.of(Field::Arguments))?
            }
            _ => refuse_implicit_text(node, &children.all())?,
        }
        Ok(())
    }

    fn second_call(self, node: &Expr, children: &ChildTypes) -> SecondCall {
        let (name, extract) = match node {
            Expr::Extract { field, .. } => (field.to_uppercase(), true),
            Expr::Scalar(call) => (call.name.clone(), false),
            _ => return SecondCall::NotSecond,
        };
        if name != "SECOND" {
            return SecondCall::NotSecond;
        }
        match children.of(Field::Arguments) {
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

/// `typed`, the rewrite of an operand, with a string column's empty strings read as NULL.
pub(crate) fn empty_as_null(typed: Expr, ty: Inferred) -> Expr {
    if matches!(typed, Expr::Column(_)) && ty.is_text() {
        wrap(ScanKind::EmptyAsNull, typed)
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
    Guarded(Expr),
    /// `IS NULL`, which also holds for an empty string.
    OrEmpty(Expr),
}

impl BareForm {
    fn apply(self, out: &mut Expr) {
        let empty = || Expr::Literal(Literal::string(""));
        *out = match self {
            Self::Unguarded => return,
            Self::Guarded(column) => Expr::And(vec![
                take(out),
                Expr::compare(CompareOp::NotEqual, column, empty()),
            ]),
            Self::OrEmpty(column) => Expr::Or(vec![
                take(out),
                Expr::compare(CompareOp::Equal, column, empty()),
            ]),
        };
    }
}

/// The types of a node's typed child expressions, by field.
#[derive(Default)]
struct ChildTypes {
    arrays: Vec<(Field, Vec<Inferred>)>,
    singles: Vec<(Field, Inferred)>,
}

impl ChildTypes {
    /// Types each child expression of `node`, given its field and its index in an array field.
    fn collect<E>(
        node: &Expr,
        mut type_child: impl FnMut(Field, Option<usize>, &Expr) -> Result<Inferred, E>,
    ) -> Result<Self, E> {
        let mut children = Self::default();
        for (field, index, child) in typed_children(node) {
            let ty = type_child(field, index, child)?;
            match index {
                Some(_) => match children.arrays.iter_mut().find(|(f, _)| *f == field) {
                    Some((_, types)) => types.push(ty),
                    None => children.arrays.push((field, vec![ty])),
                },
                None => children.singles.push((field, ty)),
            }
        }
        Ok(children)
    }

    fn of(&self, field: Field) -> &[Inferred] {
        self.arrays
            .iter()
            .find(|(f, _)| *f == field)
            .map_or(&[], |(_, c)| c.as_slice())
    }

    fn one(&self, field: Field) -> Option<Inferred> {
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
fn format_digits(call: &ScalarCall, function: ScalarFunction) -> Option<u8> {
    if function.result != ResultRule::Timestamp {
        return None;
    }
    call.arguments
        .get(1)?
        .as_string_literal()
        .and_then(fraction_digits)
}

/// An exact numeric literal is a DECIMAL(digits,0) or fractional. Exasol sends an integer beyond
/// 36 digits as `literal_double`.
fn exact_numeric_type(literal: &Literal) -> Inferred {
    let text = match &literal.value {
        Scalar::Number(n) if n.is_i64() || n.is_u64() => Some(n.to_string()),
        Scalar::String(s) => Some(s.clone()),
        _ => None,
    };
    text.as_deref()
        .and_then(integer_digits)
        .and_then(|digits| Decimal::new(digits, 0))
        .map_or(Inferred::Partial(Partial::Number), |d| {
            Inferred::Exact(ExasolType::Decimal(d))
        })
}

fn timestamp_literal_type(literal: &Literal, version: ExasolVersion) -> Inferred {
    let precision = literal
        .value
        .as_str()
        .and_then(|value| u8::try_from(TimestampLiteral(value).type_precision()).ok());
    match precision {
        Some(p) if p <= TimestampUnit::Nanosecond.digits() => derived_timestamp(p, version),
        _ => Inferred::Partial(Partial::Timestamp),
    }
}

fn rewrite_scalar_function(
    call: &ScalarCall,
    out: &mut Expr,
    args: &[Inferred],
    ty: Inferred,
) -> Result<(), UdfError> {
    let node = Expr::Scalar(call.clone());
    let function = typed_function(&call.name);
    let text_positions = function.string_positions(args.len());
    for &i in &text_positions {
        rewrite_slot(out, Field::Arguments, Some(i), |argument| {
            to_text(argument, args[i])
        })?;
    }
    let integer_positions: Vec<usize> = function
        .integer_args()
        .iter()
        .filter(|arg| arg.position < args.len())
        .map(|&arg| {
            let i = arg.position;
            rewrite_slot(out, Field::Arguments, Some(i), |argument| {
                to_integer_argument(argument, args[i], arg, function.name)
            })?;
            Ok(i)
        })
        .collect::<Result<_, UdfError>>()?;
    if function.result == ResultRule::Unified {
        return unify_branches(&node, out, Field::Arguments, args, ty);
    }
    // An unknown version proves no digits, so the format's own `FFn` gives them.
    if function.result == ResultRule::Timestamp
        && let Some(digits) = ty.value_digits().or_else(|| format_digits(call, function))
        && let Expr::Scalar(out) = out
    {
        out.timestamp_digits = Some(digits);
    }
    if let ResultRule::DateAddition(_) = function.result {
        check_date_addition(call, out, args)?;
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
        refuse_implicit_text(&node, &converted)?;
    }
    match function.result {
        ResultRule::IntegerOverIntegers if !args.iter().all(|ty| ty.is_integer()) => {
            return Err(decline(format!(
                "{} computes over integer operands only: {}",
                function.name,
                short(&node)
            )));
        }
        // DataFusion truncates a DATE to a timestamp, so the scan casts the result back to DATE.
        ResultRule::Truncated if args.get(1) == Some(&DATE) => {
            *out = cast_node(ExasolType::Date, take(out));
        }
        ResultRule::Text => *out = wrap(ScanKind::EmptyAsNull, take(out)),
        _ => {}
    }
    Ok(())
}

fn cast_node(target: ExasolType, argument: Expr) -> Expr {
    Expr::Cast {
        target,
        argument: Box::new(argument),
    }
}

/// The scan computes `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` over a DATE or a
/// TIMESTAMP, in `Date32` or a microsecond or nanosecond timestamp. The first argument gets a
/// CAST to that type, whatever Arrow type holds it.
fn check_date_addition(
    call: &ScalarCall,
    out: &mut Expr,
    args: &[Inferred],
) -> Result<(), UdfError> {
    let node = Expr::Scalar(call.clone());
    let &[first, count] = args else {
        return Err(decline(format!(
            "a date addition takes 2 arguments, got {}: {}",
            args.len(),
            short(&node)
        )));
    };
    refuse_implicit_text(&node, args)?;
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
            short(&node)
        )));
    };
    if count == BOOLEAN {
        rewrite_slot(out, Field::Arguments, Some(1), |count| {
            Ok::<_, Infallible>(wrap(ScanKind::BooleanNumber, count))
        })
        .unwrap_or_else(|never| match never {});
    } else if !count.is_numeric() {
        return Err(decline(format!(
            "a date addition count that is not a number: {}",
            short(&node)
        )));
    }
    refuse_constant_count_beyond_scan_limit(call)?;
    rewrite_slot(out, Field::Arguments, Some(0), |first| {
        Ok::<_, Infallible>(cast_node(cast_target, first))
    })
    .unwrap_or_else(|never| match never {});
    Ok(())
}

fn refuse_constant_count_beyond_scan_limit(call: &ScalarCall) -> Result<(), UdfError> {
    let Some(Expr::Literal(count)) = call.arguments.get(1) else {
        return Ok(());
    };
    if !matches!(count.kind, LiteralKind::ExactNumeric | LiteralKind::Double) {
        return Ok(());
    }
    let magnitude = match &count.value {
        Scalar::String(text) => text.parse::<f64>().ok(),
        Scalar::Number(number) => number.as_f64(),
        _ => None,
    }
    .map(f64::abs);
    if magnitude.is_some_and(|magnitude| magnitude <= MAX_SCAN_CONSTANT_COUNT) {
        return Ok(());
    }
    Err(decline(format!(
        "a constant date addition count beyond {MAX_SCAN_CONSTANT_COUNT} runs in Exasol, which checks it before it reads a row: {}",
        short(&Expr::Scalar(call.clone()))
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

fn check_aggregate(node: &Expr, name: &str, args: &[Inferred]) -> Result<(), UdfError> {
    match name {
        "COUNT" | "MIN" | "MAX" => Ok(()),
        n if is_numeric_aggregate(n) => check_numeric_aggregate(node, n, args),
        _ => refuse_implicit_text(node, args),
    }
}

fn check_json_value(call: &JsonValueCall, args: &[Inferred]) -> Result<(), UdfError> {
    if !args.first().is_some_and(|ty| ty.is_text()) {
        return Err(decline(format!(
            "JSON_VALUE over a document that is not text: {}",
            short(&Expr::JsonValue(call.clone()))
        )));
    }
    ScanJsonValue::of(call).map(drop)
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
pub fn is_session_free_integer_text(node: &Expr, precision: u8) -> bool {
    node.as_string_literal()
        .and_then(integer_digits)
        .is_some_and(|digits| digits <= precision.min(MAX_INTEGER_TEXT_DIGITS))
}

/// The child of `node` at a single `field`.
fn field_of(node: &Expr, field: Field) -> Option<&Expr> {
    typed_children(node)
        .into_iter()
        .find(|(f, index, _)| *f == field && index.is_none())
        .map(|(_, _, child)| child)
}

/// The operands of `node`'s [`Field::Arguments`].
fn arguments_of(node: &Expr) -> Vec<&Expr> {
    typed_children(node)
        .into_iter()
        .filter(|(f, _, _)| *f == Field::Arguments)
        .map(|(_, _, child)| child)
        .collect()
}

/// Types the operands that a predicate or a simple CASE compares with each other.
fn type_comparison(node: &Expr, out: &mut Expr, children: &ChildTypes) -> Result<(), UdfError> {
    let mut operands: Vec<(&Expr, Inferred)> = COMPARED_FIELDS
        .iter()
        .filter_map(|&field| Some((field_of(node, field)?, children.one(field)?)))
        .collect();
    operands.extend(
        arguments_of(node)
            .into_iter()
            .zip(children.of(Field::Arguments).iter().copied()),
    );
    compare_without_session_parse(node, out, children, &operands)?;
    compare_dates_as_timestamps(out, children, &operands);
    compare_booleans_as_numbers(node, out, children, &operands)
}

/// Exasol parses text compared with a date or number by the session's NLS settings. A digits-only
/// literal against integers reads the same in every session, so it renders as that number.
fn compare_without_session_parse(
    node: &Expr,
    out: &mut Expr,
    children: &ChildTypes,
    operands: &[(&Expr, Inferred)],
) -> Result<(), UdfError> {
    let text: Vec<&Expr> = operands
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
    let to_number = |operand: Expr| {
        Ok::<_, Infallible>(match operand.as_string_literal() {
            Some(digits) => integer_literal(digits),
            None => operand,
        })
    };
    for field in COMPARED_FIELDS {
        if children.one(field).is_some() {
            rewrite_slot(out, field, None, to_number).unwrap_or_else(|never| match never {});
        }
    }
    for i in 0..children.of(Field::Arguments).len() {
        rewrite_slot(out, Field::Arguments, Some(i), to_number)
            .unwrap_or_else(|never| match never {});
    }
    Ok(())
}

/// Exasol compares a BOOLEAN with a number as 1 or 0, except a multi-value IN over a BOOLEAN,
/// which converts its numbers to BOOLEAN and fails on any but 0 and 1.
fn compare_booleans_as_numbers(
    node: &Expr,
    out: &mut Expr,
    children: &ChildTypes,
    operands: &[(&Expr, Inferred)],
) -> Result<(), UdfError> {
    let has_number = operands
        .iter()
        .any(|(_, ty)| ty.is_numeric() && *ty != Inferred::Null);
    let has_boolean = operands.iter().any(|(_, ty)| *ty == BOOLEAN);
    if !has_number || !has_boolean {
        return Ok(());
    }
    if matches!(node, Expr::InList { .. }) && children.one(Field::Expression) == Some(BOOLEAN) {
        return Err(decline(format!(
            "an IN list converts its numbers to the BOOLEAN it tests: {}",
            short(node)
        )));
    }
    rewrite_operands(out, children, BOOLEAN, |operand| {
        wrap(ScanKind::BooleanNumber, operand)
    });
    Ok(())
}

/// A DATE compared with a TIMESTAMP gets a CAST to the timestamp's unit ([`common_timestamp`]).
fn compare_dates_as_timestamps(
    out: &mut Expr,
    children: &ChildTypes,
    operands: &[(&Expr, Inferred)],
) {
    let types: Vec<Inferred> = operands.iter().map(|(_, ty)| *ty).collect();
    if let Some(target) = common_timestamp(&types) {
        rewrite_operands(out, children, DATE, |operand| cast_node(target, operand));
    }
}

fn rewrite_operands(
    out: &mut Expr,
    children: &ChildTypes,
    ty: Inferred,
    rewrite: impl Fn(Expr) -> Expr,
) {
    let rewrite = |operand| Ok::<_, Infallible>(rewrite(operand));
    for field in COMPARED_FIELDS {
        if children.one(field) == Some(ty) {
            rewrite_slot(out, field, None, rewrite).unwrap_or_else(|never| match never {});
        }
    }
    for (i, operand) in children.of(Field::Arguments).iter().enumerate() {
        if *operand == ty {
            rewrite_slot(out, Field::Arguments, Some(i), rewrite)
                .unwrap_or_else(|never| match never {});
        }
    }
}

/// DataFusion compares a TIMESTAMP with a finer literal at nanoseconds, failing outside 1677-2262,
/// so the literal is floored to the operand's unit. A DATE compares at the literal's unit instead.
fn floor_timestamp_literals(node: &Expr, out: &mut Expr, children: &ChildTypes) {
    let precision = |field: Field| children.one(field)?.value_digits();
    let finer = |literal: &Expr, p: u8| floored_literal(literal, p).is_some();
    match (node, &*out) {
        (
            Expr::Between { .. },
            Expr::Between {
                expression,
                low,
                high,
            },
        ) => {
            let Some(p) = precision(Field::Expression) else {
                return;
            };
            if !finer(low, p) && !finer(high, p) {
                return;
            }
            *out = Expr::And(vec![
                floored_comparison(CompareOp::GreaterEqual, expression, low, p),
                floored_comparison(CompareOp::LessEqual, expression, high, p),
            ]);
        }
        (Expr::InList { .. }, Expr::InList { expression, values }) => {
            let Some(p) = precision(Field::Expression) else {
                return;
            };
            if !values.iter().any(|value| finer(value, p)) {
                return;
            }
            *out = Expr::Or(
                values
                    .iter()
                    .map(|value| floored_comparison(CompareOp::Equal, expression, value, p))
                    .collect(),
            );
        }
        (Expr::Compare { .. }, Expr::Compare { op, left, right }) => {
            if let Some(p) = precision(Field::Left)
                && finer(right, p)
            {
                *out = floored_comparison(*op, left, right, p);
            } else if let Some(p) = precision(Field::Right)
                && finer(left, p)
            {
                *out = floored_comparison(op.mirrored(), right, left, p);
            }
        }
        _ => {}
    }
}

fn floored_literal(node: &Expr, precision: u8) -> Option<Expr> {
    let literal = node.as_literal()?;
    if literal.kind != LiteralKind::Timestamp {
        return None;
    }
    let floor = TimestampLiteral(literal.value.as_str()?).floor(precision)?;
    Some(Expr::Literal(Literal::new(
        LiteralKind::Timestamp,
        Scalar::String(floor),
    )))
}

/// `operand <op> literal`, with a literal finer than `precision` floored. `x <> x` and `x = x`
/// keep a NULL operand NULL, so an enclosing NOT stays exact.
fn floored_comparison(op: CompareOp, operand: &Expr, literal: &Expr, precision: u8) -> Expr {
    let Some(floor) = floored_literal(literal, precision) else {
        return Expr::compare(op, operand.clone(), literal.clone());
    };
    let (op, right) = match op {
        CompareOp::Equal => (CompareOp::NotEqual, operand.clone()),
        CompareOp::NotEqual => (CompareOp::Equal, operand.clone()),
        CompareOp::Less | CompareOp::LessEqual => (CompareOp::LessEqual, floor),
        _ => (CompareOp::Greater, floor),
    };
    Expr::compare(op, operand.clone(), right)
}

fn integer_literal(digits: &str) -> Expr {
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", digits),
    };
    let value = match digits.trim_start_matches('0') {
        "" => "0".to_string(),
        trimmed => format!("{sign}{trimmed}"),
    };
    Expr::Literal(Literal::new(
        LiteralKind::ExactNumeric,
        Scalar::String(value),
    ))
}

/// Text in a position that expects a date, timestamp, number, or boolean: Exasol converts it by
/// the session's NLS settings, and DataFusion parses ISO dates and a `.` or fails to plan.
fn refuse_implicit_text(node: &Expr, operands: &[Inferred]) -> Result<(), UdfError> {
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
fn check_numeric_aggregate(node: &Expr, name: &str, args: &[Inferred]) -> Result<(), UdfError> {
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
fn refuse_text_among_others(node: &Expr, types: &[Inferred]) -> Result<(), UdfError> {
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
    node: &Expr,
    out: &mut Expr,
    field: Field,
    branches: &[Inferred],
    unified: Inferred,
) -> Result<(), UdfError> {
    refuse_text_among_others(node, branches)?;
    let cast_branches = |out: &mut Expr, target: ExasolType, keep: &dyn Fn(Inferred) -> bool| {
        for (i, branch) in branches.iter().enumerate() {
            if keep(*branch) {
                rewrite_slot(out, field, Some(i), |branch| {
                    Ok::<_, Infallible>(cast_node(target, branch))
                })
                .unwrap_or_else(|never| match never {});
            }
        }
    };
    if unified.exact() == Some(ExasolType::Date) {
        cast_branches(out, ExasolType::Date, &|branch| branch.is_timestamp());
    } else if let Some(target) = common_timestamp(branches) {
        cast_branches(out, target, &|branch| branch != Inferred::Null);
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

fn rewrite_cast(out: &mut Expr, args: &[Inferred]) -> Result<(), UdfError> {
    let Expr::Cast { target, argument } = out else {
        unreachable!("rewrite_cast runs on a CAST");
    };
    let target = *target;
    let source_type = args.first().copied().unwrap_or(Inferred::Unknown);
    match target {
        ExasolType::Char { size, .. } => {
            **argument = cast_to_text(take(argument), source_type, target, size, true)?;
        }
        ExasolType::Varchar { size, .. } => {
            **argument = cast_to_text(take(argument), source_type, target, size, false)?;
        }
        ExasolType::Decimal(d) if source_type == BOOLEAN => {
            if d.precision() == d.scale() {
                return Err(decline(format!(
                    "CAST of {} to {target} cannot hold 1",
                    short(argument)
                )));
            }
            **argument = wrap(ScanKind::BooleanNumber, take(argument));
        }
        _ => cast_from_text(argument, source_type, target)?,
    }
    Ok(())
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
