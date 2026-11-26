//! Exasol's type of a request expression, and the typed pass of every expression the scan
//! computes, which writes each node's DataFusion SQL as it types it. The pass declines a
//! conversion to or from text that Exasol formats by session settings the pushdown request does
//! not carry (#587).
use exasol_udf_sdk::error::UdfError;

use crate::datetime_format::fraction_digits;
use crate::exasol_type::{Decimal, ExasolType, MAX_DECIMAL_PRECISION, MAX_FRACTIONAL_DIGITS};
use crate::expr::{Column, CompareOp, Expr, JsonValueCall, Literal, ScalarCall};
use crate::json_value::{ScanJsonValue, returning_type};
use crate::scalar_functions::{
    DateResult, IntegerArg, ResultRule, ScalarFunction, TextArgs, UNLISTED, scalar_function,
};
use crate::scan_sql;
use crate::timestamp::{TimestampLiteral, TimestampUnit};
use crate::version::ExasolVersion;
use crate::{
    aggregate_sql, between_sql, case_sql, column_sql, compare_sql, in_list_sql, is_null_literal,
    junction_sql, short,
};

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

    fn sql(self, argument: &str) -> Result<String, UdfError> {
        let fraction = self.fraction().unwrap_or(0);
        match self {
            Self::OfDate(_) if fraction > 0 => {
                let timestamp = scan_sql::cast(argument, ExasolType::timestamp(fraction))?;
                scan_sql::seconds(&timestamp, fraction)
            }
            _ => scan_sql::seconds(argument, fraction),
        }
    }
}

/// The argument of a one-argument `SECOND(x)` or of an `EXTRACT(SECOND FROM x)`, and whether the
/// node is the EXTRACT. `None` for any other node, `SECOND(x, n)` included.
fn second_argument(node: &Expr) -> Option<(&Expr, bool)> {
    match node {
        Expr::Extract { field, argument } if field.to_uppercase() == "SECOND" => {
            Some((argument, true))
        }
        Expr::Scalar(call) if call.name == "SECOND" => match call.arguments.as_slice() {
            [argument] => Some((argument, false)),
            _ => None,
        },
        _ => None,
    }
}

/// A node's DataFusion SQL, with Exasol's type of the node as sent.
pub(crate) type Typed = (String, Inferred);

/// A typed operand of the node the pass is at: the operand as Exasol sent it, its DataFusion SQL,
/// and Exasol's type of it.
struct Operand<'e> {
    node: &'e Expr,
    sql: String,
    ty: Inferred,
}

impl Operand<'_> {
    fn with_sql(self, sql: String) -> Self {
        Self { sql, ..self }
    }
}

fn types(operands: &[Operand]) -> Vec<Inferred> {
    operands.iter().map(|operand| operand.ty).collect()
}

fn sqls(operands: Vec<Operand>) -> Vec<String> {
    operands.into_iter().map(|operand| operand.sql).collect()
}

pub(crate) fn decline(reason: String) -> UdfError {
    UdfError::User(format!("the scan cannot compute this expression: {reason}"))
}

/// `operand` converted to text implicitly, as a LIKE operand, a `||` operand, or a string-function
/// argument.
fn to_text(operand: Operand) -> Result<Operand, UdfError> {
    let ty = operand.ty;
    let sql = if ty.is_text() || ty == Inferred::Null {
        return Ok(operand);
    } else if ty.is_integer() {
        scan_sql::integer_text(&operand.sql)
    } else if ty == BOOLEAN {
        scan_sql::boolean_text(&operand.sql)
    } else {
        return Err(decline(format!(
            "converting {} to text depends on the session's NLS settings",
            short(operand.node)
        )));
    };
    Ok(operand.with_sql(sql))
}

/// `operand` at an [`IntegerArg`] position. A non-negative integer literal is already `Int64`.
fn to_integer_argument<'e>(
    operand: Operand<'e>,
    arg: IntegerArg,
    function: &str,
) -> Result<Operand<'e>, UdfError> {
    let literal = match operand.node.as_literal() {
        Some(Literal::ExactNumeric(value)) => value.parse::<i64>().is_ok_and(|v| v >= 0),
        _ => false,
    };
    if literal {
        return Ok(operand);
    }
    let round = if operand.ty.is_integer() {
        false
    } else if operand.ty.is_numeric() {
        true
    } else {
        return Err(decline(format!(
            "{function} needs a number of known type for {}",
            short(operand.node)
        )));
    };
    let sql = scan_sql::integer_argument(round, arg.negative_is_null, &operand.sql);
    Ok(operand.with_sql(sql))
}

impl ScalarFunction {
    /// Whether Exasol converts the argument at `position` to text.
    fn takes_text(self, position: usize) -> bool {
        match self.text_args {
            TextArgs::Never => false,
            TextArgs::All => true,
            TextArgs::First => position == 0,
            TextArgs::FirstTwo => position < 2,
            TextArgs::Pad => position == 0 || position == 2,
        }
    }

    fn integer_arg(self, position: usize) -> Option<IntegerArg> {
        self.integer_args()
            .iter()
            .find(|arg| arg.position == position)
            .copied()
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
    source: Operand,
    target: ExasolType,
    size: u32,
    is_char: bool,
) -> Result<String, UdfError> {
    let Operand { node, sql, ty } = source;
    let refuse = |why: &str| {
        Err(decline(format!(
            "CAST of {} to {target} {why}",
            short(node)
        )))
    };
    if is_char && ty != Inferred::Null {
        return refuse("blank-pads, which the scan does not reproduce");
    }
    if ty == Inferred::Null || ty.is_text() {
        Ok(sql)
    } else if ty.is_integer() {
        if integer_text_width(ty.integer_precision()) > size {
            return refuse("may not fit the integer's text");
        }
        Ok(scan_sql::integer_text(&sql))
    } else if ty == BOOLEAN {
        if size < 5 {
            return refuse("may not fit FALSE");
        }
        Ok(scan_sql::boolean_text(&sql))
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

    /// Exasol's type of `node`, whether or not the scan computes it.
    pub(crate) fn infer_type(self, node: &Expr) -> Inferred {
        if let Some((argument, extract)) = second_argument(node) {
            return self
                .seconds(self.infer_type(argument), extract)
                .map_or(Inferred::Unknown, Seconds::inferred);
        }
        let types = |nodes: &[Expr]| -> Vec<Inferred> {
            nodes.iter().map(|node| self.infer_type(node)).collect()
        };
        match node {
            Expr::Column(column) => self.column_type(column),
            Expr::Literal(literal) => self.literal_type(literal),
            Expr::Compare { .. }
            | Expr::And(_)
            | Expr::Or(_)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. } => BOOLEAN,
            Expr::Extract { field, .. } => self.extract_type(field),
            Expr::Case(case) => union_of(&types(&case.results), self.version),
            Expr::JsonValue(call) => Inferred::Exact(returning_type(call)),
            Expr::Cast { target, .. } => Inferred::Exact(*target),
            Expr::Scalar(call) => self.scalar_type(call, &types(&call.arguments)),
            Expr::Aggregate(call) => aggregate_type(&call.name, &types(&call.arguments)),
        }
    }

    /// The DataFusion SQL of `node` and its type, post-order. Text passes only where Exasol takes
    /// text; elsewhere Exasol converts it by the session's NLS settings.
    pub(crate) fn type_scan_tree(self, node: &Expr) -> Result<Typed, UdfError> {
        self.type_at(node, Position::Value)
    }

    /// [`Self::type_scan_tree`] for the root of a filter or of a join condition.
    pub(crate) fn type_scan_filter(self, node: &Expr) -> Result<Typed, UdfError> {
        self.type_at(node, Position::Filter)
    }

    /// Types the children of `node`, declines what the scan cannot compute as Exasol does, and
    /// writes the node for DataFusion.
    fn type_at<'e>(self, node: &'e Expr, position: Position) -> Result<Typed, UdfError> {
        let bare = self.bare_form(node, position);
        // A bare form reads the empty strings of its string columns as NULL itself.
        let wraps_columns = bare.is_none();
        let operand = |child: &'e Expr| -> Result<Operand<'e>, UdfError> {
            let (sql, ty) = self.type_at(child, Position::Value)?;
            let sql = if wraps_columns {
                empty_as_null(child, sql, ty)
            } else {
                sql
            };
            Ok(Operand {
                node: child,
                sql,
                ty,
            })
        };
        let operands = |children: &'e [Expr]| -> Result<Vec<Operand<'e>>, UdfError> {
            children.iter().map(&operand).collect()
        };
        if let Some((argument, extract)) = second_argument(node) {
            let argument = operand(argument)?;
            let Some(seconds) = self.seconds(argument.ty, extract) else {
                return Err(decline(format!(
                    "SECOND needs an argument of the exact type TIMESTAMP(p), or DATE outside EXTRACT: {}",
                    short(node)
                )));
            };
            return Ok((seconds.sql(&argument.sql)?, seconds.inferred()));
        }
        let (sql, ty) = match node {
            Expr::Column(column) => (column_sql(column), self.column_type(column)),
            Expr::Literal(literal) => (scan_sql::literal(literal)?, self.literal_type(literal)),
            Expr::And(items) => (
                self.junction(node, items, position, " AND ", "TRUE")?,
                BOOLEAN,
            ),
            Expr::Or(items) => (
                self.junction(node, items, position, " OR ", "FALSE")?,
                BOOLEAN,
            ),
            Expr::Not(inner) => {
                let inner = operand(inner)?;
                refuse_implicit_text(node, &[inner.ty])?;
                (format!("(NOT {})", inner.sql), BOOLEAN)
            }
            Expr::IsNull(inner) => (format!("({} IS NULL)", operand(inner)?.sql), BOOLEAN),
            Expr::IsNotNull(inner) => (format!("({} IS NOT NULL)", operand(inner)?.sql), BOOLEAN),
            Expr::Compare { op, left, right } => {
                let (left, right) = (operand(left)?, operand(right)?);
                let comparison = Comparison::of(node, &left, std::slice::from_ref(&right))?;
                let (left, right) = (comparison.coerce(left)?, comparison.coerce(right)?);
                (floored_compare(*op, &left, &right), BOOLEAN)
            }
            Expr::Between {
                expression,
                low,
                high,
            } => {
                let expression = operand(expression)?;
                let bounds = [operand(low)?, operand(high)?];
                let comparison = Comparison::of(node, &expression, &bounds)?;
                let [low, high] = bounds;
                let (low, high) = (comparison.coerce(low)?, comparison.coerce(high)?);
                let expression = comparison.coerce(expression)?;
                (floored_between(&expression, &low, &high), BOOLEAN)
            }
            Expr::InList { expression, values } => {
                let values = operands(values)?;
                let expression = operand(expression)?;
                let comparison = Comparison::of(node, &expression, &values)?;
                // A multi-value IN over a BOOLEAN converts its numbers to BOOLEAN instead, and
                // fails on any but 0 and 1.
                if comparison.booleans_as_numbers && expression.ty == BOOLEAN {
                    return Err(decline(format!(
                        "an IN list converts its numbers to the BOOLEAN it tests: {}",
                        short(node)
                    )));
                }
                let values = comparison.coerce_all(values)?;
                let expression = comparison.coerce(expression)?;
                (floored_in_list(&expression, &values), BOOLEAN)
            }
            Expr::Like {
                expression,
                pattern: pattern_node,
                escape,
            } => {
                let (expression, pattern) = (operand(expression)?, operand(pattern_node)?);
                let (expression, pattern) = (to_text(expression)?, to_text(pattern)?);
                let like = scan_sql::like(
                    &expression.sql,
                    &pattern.sql,
                    pattern_node,
                    escape.as_deref(),
                )?;
                (like, BOOLEAN)
            }
            Expr::Extract { field, argument } => {
                let argument = operand(argument)?;
                refuse_implicit_text(node, &[argument.ty])?;
                (
                    scan_sql::extract(field, &argument.sql),
                    self.extract_type(field),
                )
            }
            Expr::Case(case) => {
                let whens = operands(&case.whens)?;
                let results = operands(&case.results)?;
                let basis = case.basis.as_deref().map(&operand).transpose()?;
                let result_types = types(&results);
                let ty = union_of(&result_types, self.version);
                let (basis, whens) = match basis {
                    // A simple CASE compares its basis with each WHEN value.
                    Some(basis) => {
                        let comparison = Comparison::of(node, &basis, &whens)?;
                        let whens = sqls(comparison.coerce_all(whens)?);
                        (Some(comparison.coerce(basis)?.sql), whens)
                    }
                    None => {
                        refuse_implicit_text(node, &types(&whens))?;
                        (None, sqls(whens))
                    }
                };
                let results = unify_branches(node, sqls(results), &result_types, ty)?;
                (case_sql(basis.as_deref(), &whens, &results), ty)
            }
            Expr::JsonValue(call) => {
                let document = operand(&call.document)?;
                operand(&call.path)?;
                let scan_call = scan_json_value(node, call, document.ty)?;
                (
                    scan_sql::json_value(&scan_call, &document.sql),
                    Inferred::Exact(returning_type(call)),
                )
            }
            Expr::Cast { target, argument } => (
                scan_cast(*target, operand(argument)?)?,
                Inferred::Exact(*target),
            ),
            Expr::Scalar(call) => {
                let arguments = operands(&call.arguments)?;
                let ty = self.scalar_type(call, &types(&arguments));
                (scan_scalar(node, call, arguments, ty)?, ty)
            }
            Expr::Aggregate(call) => {
                let arguments = operands(&call.arguments)?;
                let types = types(&arguments);
                check_aggregate(node, &call.name, &types)?;
                (
                    aggregate_sql(call, &sqls(arguments)),
                    aggregate_type(&call.name, &types),
                )
            }
        };
        Ok(match bare {
            Some(bare) => (bare.apply(sql), ty),
            None => (sql, ty),
        })
    }

    /// The `AND` or `OR` of `items`, which sit at its position.
    fn junction(
        self,
        node: &Expr,
        items: &[Expr],
        position: Position,
        op: &str,
        empty: &str,
    ) -> Result<String, UdfError> {
        let typed: Vec<Typed> = items
            .iter()
            .map(|item| self.type_at(item, position))
            .collect::<Result<_, _>>()?;
        let (items, types): (Vec<String>, Vec<Inferred>) = typed.into_iter().unzip();
        refuse_implicit_text(node, &types)?;
        Ok(junction_sql(items, op, empty))
    }

    fn column_type(self, column: &Column) -> Inferred {
        self.columns
            .column_type(column)
            .map_or(Inferred::Unknown, Inferred::Exact)
    }

    fn literal_type(self, literal: &Literal) -> Inferred {
        match literal {
            Literal::Null => Inferred::Null,
            Literal::Bool(_) => BOOLEAN,
            Literal::String(_) => Inferred::Partial(Partial::Text),
            Literal::Double(_) => DOUBLE,
            Literal::ExactNumeric(value) => exact_numeric_type(value),
            Literal::Date(_) => DATE,
            Literal::Timestamp(value) => timestamp_literal_type(value, self.version),
            Literal::TimestampUtc(_)
            | Literal::LocalTimeZoneTimestamp(_)
            | Literal::Binary { .. } => Inferred::Unknown,
        }
    }

    /// Exasol's type of an `EXTRACT` of any field but SECOND.
    fn extract_type(self, field: &str) -> Inferred {
        typed_function(field).result_type(&[], self.version)
    }

    /// Exasol's type of a scalar call over arguments of these types, a one-argument `SECOND` aside.
    fn scalar_type(self, call: &ScalarCall, arguments: &[Inferred]) -> Inferred {
        let function = typed_function(&call.name);
        let ty = function.result_type(arguments, self.version);
        match format_digits(call, function) {
            Some(digits) => ty.holding(digits),
            None => ty,
        }
    }

    /// What the scan computes for the seconds of an argument of this type. `None` for an argument
    /// the scan cannot take. Exasol itself fails `EXTRACT(SECOND)` of a DATE.
    fn seconds(self, argument: Inferred, extract: bool) -> Option<Seconds> {
        if argument == DATE && !extract {
            return Some(Seconds::OfDate(self.version));
        }
        argument
            .timestamp_precision()
            .filter(|fraction| *fraction <= MAX_FRACTIONAL_DIGITS)
            .map(|fraction| Seconds::OfTimestamp { fraction })
    }

    /// The form of `node` that keeps its string columns bare, so statistics still prune it.
    fn bare_form(self, node: &Expr, position: Position) -> Option<BareForm> {
        let text_column = |operand: &Expr| {
            operand
                .as_column()
                .filter(|column| self.column_type(column).is_text())
                .map(column_sql)
        };
        let literal = |operand: &Expr| {
            matches!(
                operand.as_literal(),
                Some(Literal::String(_) | Literal::Null)
            )
        };
        match node {
            Expr::IsNull(subject) => return text_column(subject).map(BareForm::OrEmpty),
            Expr::IsNotNull(subject) => return text_column(subject).map(BareForm::Guarded),
            _ if position == Position::Value => return None,
            _ => {}
        }
        match node {
            Expr::InList { expression, values } => text_column(expression)
                .filter(|_| values.iter().all(literal))
                .map(|_| BareForm::Unguarded),
            Expr::Between {
                expression,
                low,
                high,
            } => text_column(expression)
                .filter(|_| literal(low) && literal(high))
                .map(|_| BareForm::Unguarded),
            Expr::Like {
                expression,
                pattern,
                ..
            } => text_column(expression)
                .filter(|_| literal(pattern))
                .map(BareForm::Guarded),
            Expr::Compare { op, left, right } => {
                let (column, op) = match (text_column(left), text_column(right)) {
                    (Some(column), _) if literal(right) => (column, *op),
                    (_, Some(column)) if literal(left) => (column, op.mirrored()),
                    // Equal columns are both empty or neither, so one guard covers both.
                    (Some(column), Some(_)) if *op == CompareOp::Equal => {
                        return Some(BareForm::Guarded(column));
                    }
                    _ => return None,
                };
                Some(match op {
                    CompareOp::NotEqual | CompareOp::Less | CompareOp::LessEqual => {
                        BareForm::Guarded(column)
                    }
                    _ => BareForm::Unguarded,
                })
            }
            _ => None,
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

/// The SQL of operand `node` with a string column's empty strings read as NULL.
pub(crate) fn empty_as_null(node: &Expr, sql: String, ty: Inferred) -> String {
    if matches!(node, Expr::Column(_)) && ty.is_text() {
        scan_sql::empty_as_null(&sql)
    } else {
        sql
    }
}

/// A predicate over bare string columns that reads an empty string as NULL without wrapping the
/// column, which statistics pruning could not read through. Each guard holds its column's SQL.
enum BareForm {
    /// A predicate that is never TRUE for an empty string.
    Unguarded,
    /// A predicate that could be TRUE for an empty string, so it adds that the column is not one.
    Guarded(String),
    /// `IS NULL`, which also holds for an empty string.
    OrEmpty(String),
}

impl BareForm {
    fn apply(self, sql: String) -> String {
        match self {
            Self::Unguarded => sql,
            Self::Guarded(column) => {
                let non_empty = compare_sql(CompareOp::NotEqual, &column, "''");
                junction_sql(vec![sql, non_empty], " AND ", "TRUE")
            }
            Self::OrEmpty(column) => {
                let empty = compare_sql(CompareOp::Equal, &column, "''");
                junction_sql(vec![sql, empty], " OR ", "FALSE")
            }
        }
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
fn exact_numeric_type(value: &str) -> Inferred {
    integer_digits(value)
        .and_then(|digits| Decimal::new(digits, 0))
        .map_or(Inferred::Partial(Partial::Number), |d| {
            Inferred::Exact(ExasolType::Decimal(d))
        })
}

fn timestamp_literal_type(value: &str, version: ExasolVersion) -> Inferred {
    let precision = u8::try_from(TimestampLiteral(value).type_precision()).ok();
    match precision {
        Some(p) if p <= TimestampUnit::Nanosecond.digits() => derived_timestamp(p, version),
        _ => Inferred::Partial(Partial::Timestamp),
    }
}

/// `arguments` with each one replaced by `convert`'s answer for its position.
fn convert_arguments<'e>(
    arguments: Vec<Operand<'e>>,
    convert: impl Fn(usize, Operand<'e>) -> Result<Operand<'e>, UdfError>,
) -> Result<Vec<Operand<'e>>, UdfError> {
    arguments
        .into_iter()
        .enumerate()
        .map(|(i, argument)| convert(i, argument))
        .collect()
}

/// The scalar call `node` over its typed `arguments`, each converted as Exasol converts it. `ty` is
/// the call's type.
fn scan_scalar(
    node: &Expr,
    call: &ScalarCall,
    arguments: Vec<Operand>,
    ty: Inferred,
) -> Result<String, UdfError> {
    let function = typed_function(&call.name);
    let types = types(&arguments);
    let arguments = convert_arguments(arguments, |i, argument| {
        if function.takes_text(i) {
            to_text(argument)
        } else {
            Ok(argument)
        }
    })?;
    let arguments = convert_arguments(arguments, |i, argument| match function.integer_arg(i) {
        Some(arg) => to_integer_argument(argument, arg, function.name),
        None => Ok(argument),
    })?;
    let arguments = sqls(arguments);
    if function.result == ResultRule::Unified {
        let arguments = unify_branches(node, arguments, &types, ty)?;
        return scan_sql::scalar(call, &arguments, None);
    }
    let arguments = if let ResultRule::DateAddition(_) = function.result {
        date_addition(node, call, arguments, &types)?
    } else {
        let converted: Vec<Inferred> = types
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                !function.takes_text(*i)
                    && function.integer_arg(*i).is_none()
                    && !function.parameters.contains(i)
            })
            .map(|(_, ty)| *ty)
            .collect();
        refuse_implicit_text(node, &converted)?;
        arguments
    };
    if function.result == ResultRule::IntegerOverIntegers && !types.iter().all(|ty| ty.is_integer())
    {
        return Err(decline(format!(
            "{} computes over integer operands only: {}",
            function.name,
            short(node)
        )));
    }
    // An unknown version proves no digits, so the format's own `FFn` gives them.
    let timestamp_digits = match function.result {
        ResultRule::Timestamp => ty.value_digits().or_else(|| format_digits(call, function)),
        _ => None,
    };
    let scalar = scan_sql::scalar(call, &arguments, timestamp_digits)?;
    match function.result {
        // DataFusion truncates a DATE to a timestamp, so the scan casts the result back to DATE.
        ResultRule::Truncated if types.get(1) == Some(&DATE) => {
            scan_sql::cast(&scalar, ExasolType::Date)
        }
        ResultRule::Text => Ok(scan_sql::empty_as_null(&scalar)),
        _ => Ok(scalar),
    }
}

/// The arguments of an `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, or `ADD_MINUTES`, which the scan
/// computes over a DATE or a TIMESTAMP, in `Date32` or a microsecond or nanosecond timestamp. The
/// first argument gets a CAST to that type, whatever Arrow type holds it.
fn date_addition(
    node: &Expr,
    call: &ScalarCall,
    arguments: Vec<String>,
    types: &[Inferred],
) -> Result<Vec<String>, UdfError> {
    let (Ok([first, count]), &[first_type, count_type]) =
        (<[String; 2]>::try_from(arguments), types)
    else {
        return Err(decline(format!(
            "a date addition takes 2 arguments, got {}: {}",
            types.len(),
            short(node)
        )));
    };
    refuse_implicit_text(node, types)?;
    let cast_target = if first_type == DATE {
        ExasolType::Date
    } else if let Some(digits) = first_type.value_digits() {
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
    let count = if count_type == BOOLEAN {
        scan_sql::boolean_number(&count)
    } else if count_type.is_numeric() {
        count
    } else {
        return Err(decline(format!(
            "a date addition count that is not a number: {}",
            short(node)
        )));
    };
    refuse_constant_count_beyond_scan_limit(node, call)?;
    Ok(vec![scan_sql::cast(&first, cast_target)?, count])
}

fn refuse_constant_count_beyond_scan_limit(node: &Expr, call: &ScalarCall) -> Result<(), UdfError> {
    let Some(Expr::Literal(Literal::ExactNumeric(count) | Literal::Double(count))) =
        call.arguments.get(1)
    else {
        return Ok(());
    };
    let magnitude = count.parse::<f64>().ok().map(f64::abs);
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

fn check_aggregate(node: &Expr, name: &str, args: &[Inferred]) -> Result<(), UdfError> {
    match name {
        "COUNT" | "MIN" | "MAX" => Ok(()),
        n if is_numeric_aggregate(n) => check_numeric_aggregate(node, n, args),
        _ => refuse_implicit_text(node, args),
    }
}

/// The `JSON_VALUE` call the scan computes over a document of this type.
fn scan_json_value<'e>(
    node: &Expr,
    call: &'e JsonValueCall,
    document: Inferred,
) -> Result<ScanJsonValue<'e>, UdfError> {
    if !document.is_text() {
        return Err(decline(format!(
            "JSON_VALUE over a document that is not text: {}",
            short(node)
        )));
    }
    ScanJsonValue::of(call)
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

/// How the scan brings the operands that a predicate or a simple CASE compares with each other to
/// the types Exasol compares them in.
#[derive(Clone, Copy)]
struct Comparison {
    /// A digits-only text literal compared with integers renders as that number.
    digits_as_numbers: bool,
    /// The TIMESTAMP a DATE compared with a TIMESTAMP gets a CAST to ([`common_timestamp`]).
    dates_as: Option<ExasolType>,
    /// Exasol compares a BOOLEAN with a number as 1 or 0.
    booleans_as_numbers: bool,
}

impl Comparison {
    fn of(node: &Expr, subject: &Operand, others: &[Operand]) -> Result<Self, UdfError> {
        let operands: Vec<&Operand> = std::iter::once(subject).chain(others).collect();
        let types: Vec<Inferred> = operands.iter().map(|operand| operand.ty).collect();
        let has_number = types
            .iter()
            .any(|ty| ty.is_numeric() && *ty != Inferred::Null);
        Ok(Self {
            digits_as_numbers: compares_digits_as_numbers(node, &operands)?,
            dates_as: common_timestamp(&types),
            booleans_as_numbers: has_number && types.contains(&BOOLEAN),
        })
    }

    fn coerce<'e>(self, operand: Operand<'e>) -> Result<Operand<'e>, UdfError> {
        let Operand { node, sql, ty } = operand;
        let sql = match node.as_string_literal() {
            Some(digits) if self.digits_as_numbers => integer_literal(digits),
            _ => sql,
        };
        let sql = match self.dates_as {
            Some(target) if ty == DATE => scan_sql::cast(&sql, target)?,
            _ if self.booleans_as_numbers && ty == BOOLEAN => scan_sql::boolean_number(&sql),
            _ => sql,
        };
        Ok(Operand { node, sql, ty })
    }

    fn coerce_all<'e>(self, operands: Vec<Operand<'e>>) -> Result<Vec<Operand<'e>>, UdfError> {
        operands
            .into_iter()
            .map(|operand| self.coerce(operand))
            .collect()
    }
}

/// Exasol parses text compared with a date or number by the session's NLS settings. A digits-only
/// literal against integers reads the same in every session, so it renders as that number.
/// `true` when the comparison holds such literals.
fn compares_digits_as_numbers(node: &Expr, operands: &[&Operand]) -> Result<bool, UdfError> {
    let text: Vec<&Expr> = operands
        .iter()
        .filter(|operand| operand.ty.is_text())
        .map(|operand| operand.node)
        .collect();
    let non_text: Vec<Inferred> = operands
        .iter()
        .map(|operand| operand.ty)
        .filter(|ty| !ty.is_text() && *ty != Inferred::Null)
        .collect();
    if text.is_empty() || non_text.is_empty() {
        return Ok(false);
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
    Ok(true)
}

/// DataFusion compares a TIMESTAMP with a finer literal at nanoseconds, failing outside 1677-2262,
/// so the literal is floored to the operand's unit. A DATE compares at the literal's unit instead.
fn floored_compare(op: CompareOp, left: &Operand, right: &Operand) -> String {
    if let Some(p) = left.ty.value_digits()
        && floored_literal(right.node, p).is_some()
    {
        floored_comparison(op, &left.sql, right, p)
    } else if let Some(p) = right.ty.value_digits()
        && floored_literal(left.node, p).is_some()
    {
        floored_comparison(op.mirrored(), &right.sql, left, p)
    } else {
        compare_sql(op, &left.sql, &right.sql)
    }
}

/// [`floored_compare`] for a BETWEEN.
fn floored_between(expression: &Operand, low: &Operand, high: &Operand) -> String {
    let finer = |p: u8| {
        [low, high]
            .iter()
            .any(|bound| floored_literal(bound.node, p).is_some())
    };
    match expression.ty.value_digits() {
        Some(p) if finer(p) => {
            let low = floored_comparison(CompareOp::GreaterEqual, &expression.sql, low, p);
            let high = floored_comparison(CompareOp::LessEqual, &expression.sql, high, p);
            junction_sql(vec![low, high], " AND ", "TRUE")
        }
        _ => between_sql(&expression.sql, &low.sql, &high.sql),
    }
}

/// [`floored_compare`] for an IN list.
fn floored_in_list(expression: &Operand, values: &[Operand]) -> String {
    let finer = |p: u8| {
        values
            .iter()
            .any(|value| floored_literal(value.node, p).is_some())
    };
    match expression.ty.value_digits() {
        Some(p) if finer(p) => {
            let equalities = values
                .iter()
                .map(|value| floored_comparison(CompareOp::Equal, &expression.sql, value, p))
                .collect();
            junction_sql(equalities, " OR ", "FALSE")
        }
        _ => {
            let values: Vec<String> = values
                .iter()
                .filter(|value| !is_null_literal(value.node))
                .map(|value| value.sql.clone())
                .collect();
            in_list_sql(&expression.sql, &values)
        }
    }
}

/// The SQL of timestamp literal `node` cut to `precision` fractional digits. `None` for any other
/// node, and for a literal no finer.
fn floored_literal(node: &Expr, precision: u8) -> Option<String> {
    let Some(Literal::Timestamp(value)) = node.as_literal() else {
        return None;
    };
    let floor = TimestampLiteral(value).floor(precision)?;
    Some(scan_sql::timestamp_literal(&floor))
}

/// `operand <op> literal`, with a literal finer than `precision` floored. `x <> x` and `x = x`
/// keep a NULL operand NULL, so an enclosing NOT stays exact.
fn floored_comparison(op: CompareOp, operand: &str, literal: &Operand, precision: u8) -> String {
    let Some(floor) = floored_literal(literal.node, precision) else {
        return compare_sql(op, operand, &literal.sql);
    };
    let (op, right) = match op {
        CompareOp::Equal => (CompareOp::NotEqual, operand),
        CompareOp::NotEqual => (CompareOp::Equal, operand),
        CompareOp::Less | CompareOp::LessEqual => (CompareOp::LessEqual, floor.as_str()),
        _ => (CompareOp::Greater, floor.as_str()),
    };
    compare_sql(op, operand, right)
}

/// The number a digits-only text literal reads as.
fn integer_literal(digits: &str) -> String {
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", digits),
    };
    match digits.trim_start_matches('0') {
        "" => "0".to_string(),
        trimmed => format!("{sign}{trimmed}"),
    }
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

/// `branches` brought to the type Exasol brings them to (CASE results, `GREATEST`, `LEAST`).
/// Branches of DATEs and TIMESTAMPs each get a CAST to their [`common_timestamp`], or to DATE where
/// Exasol 8 brings a TIMESTAMP after a DATE to the DATE.
fn unify_branches(
    node: &Expr,
    branches: Vec<String>,
    types: &[Inferred],
    unified: Inferred,
) -> Result<Vec<String>, UdfError> {
    refuse_text_among_others(node, types)?;
    let (target, casts): (ExasolType, fn(Inferred) -> bool) =
        if unified.exact() == Some(ExasolType::Date) {
            (ExasolType::Date, Inferred::is_timestamp)
        } else if let Some(target) = common_timestamp(types) {
            (target, |ty| ty != Inferred::Null)
        } else {
            return Ok(branches);
        };
    branches
        .into_iter()
        .zip(types)
        .map(|(branch, ty)| {
            if casts(*ty) {
                scan_sql::cast(&branch, target)
            } else {
                Ok(branch)
            }
        })
        .collect()
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

/// The CAST of `argument` to `target`.
fn scan_cast(target: ExasolType, argument: Operand) -> Result<String, UdfError> {
    let source = argument.ty;
    let inner = match target {
        ExasolType::Char { size, .. } => cast_to_text(argument, target, size, true)?,
        ExasolType::Varchar { size, .. } => cast_to_text(argument, target, size, false)?,
        ExasolType::Decimal(d) if source == BOOLEAN => {
            if d.precision() == d.scale() {
                return Err(decline(format!(
                    "CAST of {} to {target} cannot hold 1",
                    short(argument.node)
                )));
            }
            scan_sql::boolean_number(&argument.sql)
        }
        _ => {
            cast_from_text(argument.node, source, target)?;
            argument.sql
        }
    };
    scan_sql::cast(&inner, target)
}

#[cfg(test)]
#[path = "scan_typing_tests.rs"]
mod tests;
