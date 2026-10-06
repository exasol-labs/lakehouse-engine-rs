use arrow::datatypes::{DataType, TimeUnit};
use datafusion::scalar::ScalarValue;
use serde_json::Value as Json;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::scan::{non_null_partition_value, partition_scalar};

use super::filter_json::{
    Comparison, between_operands, comparison_operands, in_operands, non_empty_str, operands,
    subject_column,
};

/// A file or row group is kept iff TRUE is reachable at the root; an untranslatable node reaches
/// every truth value, so it can only widen the kept set. It evaluates partition values and
/// footer statistics alike, both as value ranges a [`ColumnFacts`] source describes, so the two
/// inputs share one meaning for every node.
pub(super) struct PartitionPredicate {
    root: Node,
}

impl PartitionPredicate {
    /// `declared` types the partition columns (uppercase fold); an undeclared one compares as
    /// `utf8`, the type direct storage declares. `None` keeps every file.
    pub(super) fn from_filter(filter_json: Option<&Json>, declared: &[(String, DataType)]) -> Self {
        Self::translated(
            filter_json,
            Translator {
                columns: declared,
                scope: LiteralScope::PartitionValues,
            },
        )
    }

    /// `columns` types the columns whose statistics the scan compares in that same type; any
    /// other column is untranslatable. A literal converts only to the value the scan itself
    /// compares against, so a footer bound never drops a row the scan would return.
    pub(super) fn for_footer_statistics(
        filter_json: Option<&Json>,
        columns: &[(String, DataType)],
    ) -> Self {
        Self::translated(
            filter_json,
            Translator {
                columns,
                scope: LiteralScope::FooterStatistics,
            },
        )
    }

    fn translated(filter_json: Option<&Json>, translator: Translator<'_>) -> Self {
        Self {
            root: filter_json.map_or(Node::Opaque, |node| translator.translate(node)),
        }
    }

    /// Column names match the facts' columns case-insensitively (uppercase fold).
    pub(super) fn keeps(&self, facts: &(impl ColumnFacts + ?Sized)) -> bool {
        self.root.reachable(facts, Polarity::Plain).can_be_true
    }
}

/// The predicate's question to a value source. `None` from either method means nothing is
/// known about the column, so every truth value stays reachable.
pub(super) trait ColumnFacts {
    fn orderings(&self, column: &str, literal: &ScalarValue) -> Option<Orderings>;
    fn nulls(&self, column: &str) -> Option<Nulls>;
}

/// Which orderings the column's non-null values reach against a literal, and whether a NaN row,
/// which no ordering describes, may exist.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Orderings {
    less: bool,
    equal: bool,
    greater: bool,
    nan_possible: bool,
}

impl Orderings {
    /// No non-null value: every comparison is NULL.
    pub(super) const NONE: Self = Self {
        less: false,
        equal: false,
        greater: false,
        nan_possible: false,
    };

    fn exactly(ordering: Ordering) -> Self {
        Self {
            less: ordering == Ordering::Less,
            equal: ordering == Ordering::Equal,
            greater: ordering == Ordering::Greater,
            nan_possible: false,
        }
    }

    /// Reads `[min, max]` as a value range of no NaN row; `None` when a bound does not compare
    /// with the literal or `min > max`, since such a range proves nothing.
    pub(super) fn within(
        min: &ScalarValue,
        max: &ScalarValue,
        literal: &ScalarValue,
    ) -> Option<Self> {
        if min.partial_cmp(max)? == Ordering::Greater {
            return None;
        }
        let from_min = min.partial_cmp(literal)?;
        let from_max = max.partial_cmp(literal)?;
        Some(Self {
            less: from_min == Ordering::Less,
            equal: from_min != Ordering::Greater && from_max != Ordering::Less,
            greater: from_max == Ordering::Greater,
            nan_possible: false,
        })
    }

    /// The same orderings, plus a NaN row the bounds exclude.
    pub(super) fn with_nan_row(self) -> Self {
        Self {
            nan_possible: true,
            ..self
        }
    }

    /// The union, across the rows, of the truth value each reachable ordering gives.
    fn reachable(self, comparison: Comparison, polarity: Polarity) -> Reachable {
        let nan_row = if self.nan_possible {
            nan_row_reachable(comparison, polarity)
        } else {
            Reachable::NULL_ONLY
        };
        [
            (self.less, Ordering::Less),
            (self.equal, Ordering::Equal),
            (self.greater, Ordering::Greater),
        ]
        .into_iter()
        .filter(|(reached, _)| *reached)
        .map(|(_, ordering)| Reachable::exactly(Some(comparison.holds(ordering))))
        .fold(nan_row, Reachable::union)
    }
}

/// The one home of the float NaN rule. Outside an odd number of `NOT`s a NaN row adds no truth
/// value to `<`, `<=`, `>`, `>=`, or `=`, the bound rule the scan's row-group pruning, Delta
/// planning, and Iceberg planning share (#393); otherwise it can be TRUE or FALSE.
fn nan_row_reachable(comparison: Comparison, polarity: Polarity) -> Reachable {
    if matches!(polarity, Polarity::Negated) || matches!(comparison, Comparison::NotEqual) {
        Reachable::ANY
    } else {
        Reachable::NULL_ONLY
    }
}

/// Whether a node sits under an even (`Plain`) or odd (`Negated`) number of enclosing `NOT`s.
#[derive(Clone, Copy)]
enum Polarity {
    Plain,
    Negated,
}

impl Polarity {
    fn flipped(self) -> Self {
        match self {
            Self::Plain => Self::Negated,
            Self::Negated => Self::Plain,
        }
    }
}

/// Whether some row is NULL and whether some row is not.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Nulls {
    any_null: bool,
    any_non_null: bool,
}

impl Nulls {
    pub(super) fn counted(null_count: u64, rows: u64) -> Self {
        Self {
            any_null: null_count > 0,
            any_non_null: null_count < rows,
        }
    }

    fn is_null(self) -> Reachable {
        Reachable {
            can_be_true: self.any_null,
            can_be_false: self.any_non_null,
        }
    }
}

/// One exact value per partition column; a NULL value holds only NULL.
impl ColumnFacts for BTreeMap<String, Option<String>> {
    fn orderings(&self, column: &str, literal: &ScalarValue) -> Option<Orderings> {
        match partition_value(self, column)? {
            None => Some(Orderings::NONE),
            Some(value) => typed_ordering(value, literal).map(Orderings::exactly),
        }
    }

    fn nulls(&self, column: &str) -> Option<Nulls> {
        partition_value(self, column).map(|value| Nulls {
            any_null: value.is_none(),
            any_non_null: value.is_some(),
        })
    }
}

enum Node {
    Compare {
        column: String,
        comparison: Comparison,
        literal: ScalarValue,
    },
    IsNull {
        column: String,
    },
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Opaque,
}

/// The SQL truth values a node can reach across a file's rows; neither flag set means only NULL.
#[derive(Clone, Copy)]
struct Reachable {
    can_be_true: bool,
    can_be_false: bool,
}

impl Reachable {
    const ANY: Self = Self {
        can_be_true: true,
        can_be_false: true,
    };

    const NULL_ONLY: Self = Self {
        can_be_true: false,
        can_be_false: false,
    };

    /// The values two sets of rows reach together, unlike [`Self::or`], which combines two
    /// operands of one row.
    fn union(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true || other.can_be_true,
            can_be_false: self.can_be_false || other.can_be_false,
        }
    }

    fn exactly(truth: Option<bool>) -> Self {
        Self {
            can_be_true: truth == Some(true),
            can_be_false: truth == Some(false),
        }
    }

    fn and(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true && other.can_be_true,
            can_be_false: self.can_be_false || other.can_be_false,
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            can_be_true: self.can_be_true || other.can_be_true,
            can_be_false: self.can_be_false && other.can_be_false,
        }
    }

    fn negated(self) -> Self {
        Self {
            can_be_true: self.can_be_false,
            can_be_false: self.can_be_true,
        }
    }
}

impl Node {
    fn reachable(&self, facts: &(impl ColumnFacts + ?Sized), polarity: Polarity) -> Reachable {
        match self {
            Self::Compare {
                column,
                comparison,
                literal,
            } => facts
                .orderings(column, literal)
                .map_or(Reachable::ANY, |orderings| {
                    orderings.reachable(*comparison, polarity)
                }),
            Self::IsNull { column } => facts.nulls(column).map_or(Reachable::ANY, Nulls::is_null),
            Self::And(operands) => operands
                .iter()
                .map(|operand| operand.reachable(facts, polarity))
                .fold(Reachable::exactly(Some(true)), Reachable::and),
            Self::Or(operands) => operands
                .iter()
                .map(|operand| operand.reachable(facts, polarity))
                .fold(Reachable::exactly(Some(false)), Reachable::or),
            Self::Not(operand) => operand.reachable(facts, polarity.flipped()).negated(),
            Self::Opaque => Reachable::ANY,
        }
    }
}

/// `None` when `column` names no partition key; `Some(None)` when the file's value is NULL, which
/// an empty value is too, as the scan reads it.
fn partition_value<'a>(
    values: &'a BTreeMap<String, Option<String>>,
    column: &str,
) -> Option<Option<&'a str>> {
    values
        .iter()
        .find(|(key, _)| folds_to(key, column))
        .map(|(_, value)| non_null_partition_value(value.as_deref()))
}

/// A source must answer such a column from the partition value alone, never from the statistics
/// of a Parquet column that folds to the same key.
pub(super) fn is_partition_key(values: &BTreeMap<String, Option<String>>, column: &str) -> bool {
    values.keys().any(|key| folds_to(key, column))
}

/// `column` is a filter column name, already uppercased.
pub(super) fn folds_to(name: &str, column: &str) -> bool {
    name.chars().flat_map(char::to_uppercase).eq(column.chars())
}

/// `None` for a value that does not convert, so its file is kept for the scan's own conversion to
/// fail the query.
fn typed_ordering(value: &str, literal: &ScalarValue) -> Option<Ordering> {
    // A string value converts to itself, so it compares as-is without the conversion's copy.
    if let ScalarValue::Utf8(Some(literal))
    | ScalarValue::LargeUtf8(Some(literal))
    | ScalarValue::Utf8View(Some(literal)) = literal
    {
        return Some(value.cmp(literal.as_str()));
    }
    let value = partition_scalar(value, &literal.data_type()).ok()?;
    if value.is_null() {
        return None;
    }
    value.partial_cmp(literal)
}

/// Which literal conversions a translation admits: the footer scope compares against the
/// scan's own literal value, while partition pruning keeps its exact declared-type conversions.
#[derive(Clone, Copy)]
enum LiteralScope {
    PartitionValues,
    FooterStatistics,
}

struct Translator<'a> {
    columns: &'a [(String, DataType)],
    scope: LiteralScope,
}

impl Translator<'_> {
    fn translate(&self, node: &Json) -> Node {
        let Some(kind) = node.get("type").and_then(Json::as_str) else {
            return Node::Opaque;
        };
        match kind {
            "predicate_and" => self
                .translate_operands(node)
                .map_or(Node::Opaque, Node::And),
            "predicate_or" => self.translate_operands(node).map_or(Node::Opaque, Node::Or),
            "predicate_not" => node.get("expression").map_or(Node::Opaque, |operand| {
                Node::Not(Box::new(self.translate(operand)))
            }),
            "predicate_is_null" => translate_is_null(node).unwrap_or(Node::Opaque),
            "predicate_is_not_null" => {
                translate_is_null(node).map_or(Node::Opaque, |is_null| Node::Not(Box::new(is_null)))
            }
            "predicate_in_constlist" => self.translate_in(node).unwrap_or(Node::Opaque),
            "predicate_between" => self.translate_between(node).unwrap_or(Node::Opaque),
            other => Comparison::from_node_type(other)
                .and_then(|comparison| self.translate_comparison(node, comparison))
                .unwrap_or(Node::Opaque),
        }
    }

    fn translate_operands(&self, node: &Json) -> Option<Vec<Node>> {
        Some(
            operands(node)?
                .iter()
                .map(|operand| self.translate(operand))
                .collect(),
        )
    }

    fn translate_comparison(&self, node: &Json, comparison: Comparison) -> Option<Node> {
        let (column, literal, comparison) = comparison_operands(node, comparison)?;
        let column = column.to_uppercase();
        Some(Node::Compare {
            literal: self.typed_literal(&column, literal)?,
            column,
            comparison,
        })
    }

    fn translate_in(&self, node: &Json) -> Option<Node> {
        let (column, arguments) = in_operands(node)?;
        let column = column.to_uppercase();
        let equalities = arguments
            .iter()
            .map(|argument| {
                Some(Node::Compare {
                    column: column.clone(),
                    comparison: Comparison::Equal,
                    literal: self.typed_literal(&column, argument)?,
                })
            })
            .collect::<Option<Vec<Node>>>()?;
        Some(Node::Or(equalities))
    }

    fn translate_between(&self, node: &Json) -> Option<Node> {
        let (column, low, high) = between_operands(node)?;
        let column = column.to_uppercase();
        let low = self.typed_literal(&column, low?)?;
        let high = self.typed_literal(&column, high?)?;
        Some(Node::And(vec![
            Node::Compare {
                column: column.clone(),
                comparison: Comparison::GreaterEqual,
                literal: low,
            },
            Node::Compare {
                column,
                comparison: Comparison::LessEqual,
                literal: high,
            },
        ]))
    }

    fn typed_literal(&self, column: &str, literal: &Json) -> Option<ScalarValue> {
        let column_type = self
            .columns
            .iter()
            .find(|(name, _)| folds_to(name, column))
            .map(|(_, data_type)| data_type);
        match self.scope {
            LiteralScope::PartitionValues => {
                literal_under(literal, column_type.unwrap_or(&DataType::Utf8))
            }
            LiteralScope::FooterStatistics => scan_literal_under(literal, column_type?),
        }
    }
}

fn translate_is_null(node: &Json) -> Option<Node> {
    Some(Node::IsNull {
        column: subject_column(node)?.to_uppercase(),
    })
}

/// Converts only exactly, since a rounded literal could prune a file holding a matching row, and
/// never an empty string literal, which Exasol reads as NULL.
fn literal_under(literal: &Json, declared: &DataType) -> Option<ScalarValue> {
    let kind = literal.get("type")?.as_str()?;
    let value = literal.get("value")?;
    let text = match (kind, declared) {
        ("literal_string", DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            non_empty_str(value)?.to_string()
        }
        ("literal_exactnumeric", integer) if integer.is_integer() => exact_numeral(value, 0)?,
        (
            "literal_exactnumeric",
            DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale),
        ) => exact_numeral(value, *scale)?,
        ("literal_date", DataType::Date32 | DataType::Date64) => non_empty_str(value)?.to_string(),
        ("literal_timestamp", DataType::Timestamp(unit, _)) => exact_timestamp(value, *unit)?,
        ("literal_bool", DataType::Boolean) => value.as_bool()?.to_string(),
        _ => return None,
    };
    ScalarValue::try_from_string(text, declared)
        .ok()
        .filter(|scalar| !scalar.is_null())
}

/// A plain `[+-]digits[.digits]` numeral with at most `scale` fraction digits; the string cast
/// would round a finer one.
fn exact_numeral(value: &Json, scale: i8) -> Option<String> {
    let numeral = numeral_text(value)?;
    let unsigned = numeral.strip_prefix(['+', '-']).unwrap_or(&numeral);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    let is_digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let fraction_digits = match fraction {
        None => 0,
        Some(fraction) if is_digits(fraction) => fraction.len(),
        Some(_) => return None,
    };
    (is_digits(whole) && fraction_digits <= usize::try_from(scale).ok()?).then_some(numeral)
}

/// The string cast truncates a fraction finer than `unit`, so such a literal does not convert.
fn exact_timestamp(value: &Json, unit: TimeUnit) -> Option<String> {
    let text = non_empty_str(value)?;
    let fraction = text.rsplit_once('.').map_or("", |(_, fraction)| fraction);
    let unit_digits = match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    };
    (fraction.trim_end_matches('0').len() <= unit_digits).then(|| text.to_string())
}

/// The text the scan receives: `vs-expression` passes a JSON string unchanged and renders a
/// JSON number with `to_string()`.
fn numeral_text(value: &Json) -> Option<String> {
    match value {
        Json::String(numeral) => Some(numeral.clone()),
        Json::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// Converts only to the value the scan itself compares a `column_type` column against.
/// DataFusion parses a numeral that is no 64-bit integer as `f64`, and `vs-expression` cuts a
/// timestamp literal to microseconds, so neither converts exactly.
fn scan_literal_under(literal: &Json, column_type: &DataType) -> Option<ScalarValue> {
    let kind = literal.get("type")?.as_str()?;
    let value = literal.get("value")?;
    match (kind, column_type) {
        ("literal_exactnumeric", DataType::Decimal128(..) | DataType::Decimal256(..))
            if numeral_text(value).is_some_and(|text| is_integer_numeral(&text)) =>
        {
            literal_under(literal, column_type)
        }
        (_, DataType::Decimal128(..) | DataType::Decimal256(..)) => None,
        ("literal_timestamp", DataType::Timestamp(..))
            if exact_timestamp(value, TimeUnit::Microsecond).is_some() =>
        {
            literal_under(literal, column_type)
        }
        ("literal_timestamp", DataType::Timestamp(..)) => None,
        ("literal_exactnumeric" | "literal_double", DataType::Float64) => {
            double_literal(&numeral_text(value)?)
        }
        ("literal_exactnumeric" | "literal_double", DataType::Float32) => {
            float_column_literal(&numeral_text(value)?)
        }
        _ => literal_under(literal, column_type),
    }
}

/// DataFusion's `parse_sql_number` keeps such a numeral an integer.
fn is_integer_numeral(text: &str) -> bool {
    text.parse::<i64>().is_ok() || text.parse::<u64>().is_ok()
}

/// `str::parse::<f64>` rounds to nearest, as DataFusion's own parse of the same text does. A
/// negative zero does not convert: DataFusion reads the numeral `-0` as `Int64(0)` and compares
/// against `+0.0`, which total order ranks above a stored `-0.0`.
fn double_literal(text: &str) -> Option<ScalarValue> {
    text.parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && !(*value == 0.0 && value.is_sign_negative()))
        .map(|value| ScalarValue::Float64(Some(value)))
}

/// DataFusion compares a FLOAT column against a 64-bit integer literal in FLOAT after rounding
/// the literal, so such a literal converts only when FLOAT holds it exactly. Any other literal
/// compares in DOUBLE, against the bounds the footer view widens to DOUBLE.
fn float_column_literal(text: &str) -> Option<ScalarValue> {
    let integer = text
        .parse::<i64>()
        .map(i128::from)
        .or_else(|_| text.parse::<u64>().map(i128::from));
    match integer {
        Ok(integer) if integer as f32 as i128 != integer => None,
        _ => double_literal(text),
    }
}

#[cfg(test)]
#[path = "partition_predicate_tests.rs"]
mod tests;
