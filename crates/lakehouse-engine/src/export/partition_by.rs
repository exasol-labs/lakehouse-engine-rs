//! The `PARTITION_BY` export option: its grammar, the naming of its fields, and the source types
//! each transform takes.

use super::batch::SourceColumns;
use super::refusal;
use crate::types::exasol_type::{ExasolType, UdfColumnType, exasol_type_from_column_info};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::{PartitionSpec, Schema, Transform, UnboundPartitionSpec};
use serde::{Deserialize, Serialize};
use std::iter::Peekable;
use std::str::CharIndices;

pub(super) const PARTITION_BY: &str = "PARTITION_BY";

pub(super) const FORM: &str = "PARTITION_BY = '<field>, ...' takes the fields <column>, \
     bucket(<n>, <column>), truncate(<w>, <column>), year(<column>), month(<column>), \
     day(<column>), and hour(<column>), with <n> and <w> integers of at least 1";

/// One partition field of a table the export creates, its column resolved to a source column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PartitionField {
    pub(super) transform: Transform,
    pub(super) column: String,
    pub(super) name: String,
}

/// The fields of `value`, in order, each resolved against the source column `columns`. Refuses
/// every rule that iceberg-rust's spec builder would refuse only once bound to a schema, except
/// the source type, which only a writer knows.
pub(super) fn parse(value: &str, columns: &[String]) -> Result<Vec<PartitionField>, UdfError> {
    let tokens = tokenize(value);
    if tokens.is_empty() {
        return Err(refusal(format_args!(
            "{PARTITION_BY} = '{value}' lists no partition field; {FORM}"
        )));
    }
    let mut fields: Vec<PartitionField> = Vec::new();
    for raw in split_fields(value, &tokens) {
        let field = raw.resolve(columns)?;
        raw.check_against(&field, &fields, columns)?;
        fields.push(field);
    }
    Ok(fields)
}

/// Refuses a field whose transform does not take its column's Exasol type.
pub(super) fn check_types(
    fields: &[PartitionField],
    source: &SourceColumns,
) -> Result<(), UdfError> {
    for field in fields {
        let Some(index) = source.names.iter().position(|name| *name == field.column) else {
            return Err(refusal(format_args!(
                "{PARTITION_BY} field {} names column {}, which the export source lacks",
                field.name, field.column
            )));
        };
        let column = &source.columns[index];
        let exasol_type = match exasol_type_from_column_info(column) {
            UdfColumnType::Known(exasol_type) => Some(exasol_type),
            UdfColumnType::Unrecognized { .. } => None,
        };
        let (takes, taken) = takes(field.transform, exasol_type.as_ref());
        if !takes {
            return Err(refusal(format_args!(
                "{PARTITION_BY} field {} applies {} to column {} of type {}, which {} does not \
                 take; {} takes {taken}",
                field.name,
                transform_name(field.transform),
                field.column,
                column.type_name,
                transform_name(field.transform),
                transform_name(field.transform),
            )));
        }
    }
    Ok(())
}

/// The partition spec of `fields` over `schema`, the Iceberg schema of the export's columns.
pub(super) fn unbound_spec(
    fields: &[PartitionField],
    schema: &Schema,
) -> Result<UnboundPartitionSpec, UdfError> {
    let unbuildable = |error: iceberg::Error| {
        refusal(format_args!(
            "could not build the partition spec of {PARTITION_BY}: {error}"
        ))
    };
    fields
        .iter()
        .try_fold(PartitionSpec::builder(schema.clone()), |builder, field| {
            builder.add_partition_field(&field.column, &field.name, field.transform)
        })
        .and_then(|builder| builder.build())
        .map(PartitionSpec::into_unbound)
        .map_err(unbuildable)
}

/// Whether `transform` takes a column of `exasol_type`, and the types it takes, for an error.
fn takes(transform: Transform, exasol_type: Option<&ExasolType>) -> (bool, &'static str) {
    let is = |accepted: fn(&ExasolType) -> bool| exasol_type.is_some_and(accepted);
    match transform {
        Transform::Bucket(_) => (
            is(|t| is_number_or_text(t) || is_date_or_timestamp(t)),
            "DECIMAL, VARCHAR, CHAR, DATE, and TIMESTAMP",
        ),
        Transform::Truncate(_) => (is(is_number_or_text), "DECIMAL, VARCHAR, and CHAR"),
        Transform::Year | Transform::Month | Transform::Day => {
            (is(is_date_or_timestamp), "DATE and TIMESTAMP")
        }
        Transform::Hour => (
            is(|t| matches!(t, ExasolType::Timestamp { .. })),
            "TIMESTAMP",
        ),
        _ => (true, "every type the export writes"),
    }
}

fn is_number_or_text(exasol_type: &ExasolType) -> bool {
    matches!(
        exasol_type,
        ExasolType::Decimal(_) | ExasolType::Varchar { .. } | ExasolType::Char { .. }
    )
}

fn is_date_or_timestamp(exasol_type: &ExasolType) -> bool {
    matches!(exasol_type, ExasolType::Date | ExasolType::Timestamp { .. })
}

fn transform_name(transform: Transform) -> &'static str {
    match transform {
        Transform::Bucket(_) => "bucket",
        Transform::Truncate(_) => "truncate",
        Transform::Year => "year",
        Transform::Month => "month",
        Transform::Day => "day",
        Transform::Hour => "hour",
        _ => "identity",
    }
}

fn name_suffix(transform: Transform) -> Option<&'static str> {
    match transform {
        Transform::Bucket(_) => Some("bucket"),
        Transform::Truncate(_) => Some("trunc"),
        Transform::Year => Some("year"),
        Transform::Month => Some("month"),
        Transform::Day => Some("day"),
        Transform::Hour => Some("hour"),
        _ => None,
    }
}

fn is_time(transform: Transform) -> bool {
    matches!(
        transform,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// A name without blanks, `,`, `(`, `)`, or `"`, or a number.
    Word(String),
    /// A double-quoted name, `""` read as `"`. `None` when the closing quote is missing.
    Quoted(Option<String>),
    Open,
    Close,
    Comma,
}

/// `token` and the byte range of its text in the option value.
type Spanned = (Token, usize, usize);

fn tokenize(value: &str) -> Vec<Spanned> {
    let mut tokens = Vec::new();
    let mut chars = value.char_indices().peekable();
    while let Some(&(start, c)) = chars.peek() {
        let token = match c {
            c if c.is_whitespace() => {
                chars.next();
                continue;
            }
            '(' | ')' | ',' => {
                chars.next();
                match c {
                    '(' => Token::Open,
                    ')' => Token::Close,
                    _ => Token::Comma,
                }
            }
            '"' => {
                chars.next();
                Token::Quoted(quoted(&mut chars))
            }
            _ => {
                let mut word = String::new();
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_whitespace() || matches!(c, '(' | ')' | ',' | '"') {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                Token::Word(word)
            }
        };
        let end = chars.peek().map_or(value.len(), |&(index, _)| index);
        tokens.push((token, start, end));
    }
    tokens
}

fn quoted(chars: &mut Peekable<CharIndices<'_>>) -> Option<String> {
    let mut name = String::new();
    while let Some((_, c)) = chars.next() {
        match c {
            '"' if chars.next_if(|&(_, c)| c == '"').is_some() => name.push('"'),
            '"' => return Some(name),
            c => name.push(c),
        }
    }
    None
}

/// The tokens of one field and its text, which every error about it names.
struct RawField<'a> {
    text: &'a str,
    tokens: Vec<Token>,
}

/// The column name `token` spells, if it is one.
fn name(token: &Token) -> Option<&str> {
    match token {
        Token::Word(word) => Some(word.as_str()),
        Token::Quoted(Some(name)) => Some(name.as_str()),
        _ => None,
    }
}

/// Splits the token list at each comma outside parentheses.
fn split_fields<'a>(value: &'a str, tokens: &[Spanned]) -> Vec<RawField<'a>> {
    let mut fields = Vec::new();
    let mut current: Vec<&Spanned> = Vec::new();
    let mut depth = 0usize;
    let finish = |current: &mut Vec<&Spanned>, fields: &mut Vec<RawField<'a>>| {
        let text = match (current.first(), current.last()) {
            (Some(first), Some(last)) => &value[first.1..last.2],
            _ => "",
        };
        fields.push(RawField {
            text,
            tokens: current
                .drain(..)
                .map(|(token, _, _)| token.clone())
                .collect(),
        });
    };
    for spanned in tokens {
        match spanned.0 {
            Token::Comma if depth == 0 => {
                finish(&mut current, &mut fields);
                continue;
            }
            Token::Open => depth += 1,
            Token::Close => depth = depth.saturating_sub(1),
            _ => {}
        }
        current.push(spanned);
    }
    finish(&mut current, &mut fields);
    fields
}

impl RawField<'_> {
    fn refused(&self, problem: impl std::fmt::Display) -> UdfError {
        refusal(format_args!(
            "{PARTITION_BY} field '{}' {problem}; {FORM}",
            self.text
        ))
    }

    fn malformed(&self) -> UdfError {
        self.refused("is not a partition field")
    }

    fn resolve(&self, columns: &[String]) -> Result<PartitionField, UdfError> {
        let (transform, column) = self.shape()?;
        let column = self.resolve_column(column, columns)?;
        let name = match name_suffix(transform) {
            Some(suffix) => format!("{column}_{suffix}"),
            None => column.clone(),
        };
        Ok(PartitionField {
            transform,
            column,
            name,
        })
    }

    /// The transform and the column as written.
    fn shape(&self) -> Result<(Transform, &str), UdfError> {
        match self.tokens.as_slice() {
            [column] => name(column)
                .map(|column| (Transform::Identity, column))
                .ok_or_else(|| self.malformed()),
            [Token::Word(transform), Token::Open, rest @ .., Token::Close] => {
                let transform_lower = transform.to_lowercase();
                let time = match transform_lower.as_str() {
                    "year" => Some(Transform::Year),
                    "month" => Some(Transform::Month),
                    "day" => Some(Transform::Day),
                    "hour" => Some(Transform::Hour),
                    _ => None,
                };
                if let Some(time) = time {
                    return match rest {
                        [column] => name(column)
                            .map(|c| (time, c))
                            .ok_or_else(|| self.malformed()),
                        _ => Err(self.malformed()),
                    };
                }
                let sized: fn(u32) -> Transform = match transform_lower.as_str() {
                    "bucket" => Transform::Bucket,
                    "truncate" => Transform::Truncate,
                    _ => {
                        return Err(
                            self.refused(format_args!("names the unknown transform {transform}"))
                        );
                    }
                };
                let [Token::Word(width), Token::Comma, column] = rest else {
                    return Err(self.malformed());
                };
                let width: u32 = width.parse().map_err(|_| self.malformed())?;
                if width < 1 {
                    return Err(self.refused(format_args!("has width {width}, which is below 1")));
                }
                let column = name(column).ok_or_else(|| self.malformed())?;
                Ok((sized(width), column))
            }
            _ => Err(self.malformed()),
        }
    }

    /// The source column `column` names: the same name, or else the one that differs from it
    /// only in letter case.
    fn resolve_column(&self, column: &str, columns: &[String]) -> Result<String, UdfError> {
        if columns.iter().any(|name| name == column) {
            return Ok(column.to_string());
        }
        let folded = column.to_lowercase();
        let matches: Vec<&String> = columns
            .iter()
            .filter(|name| name.to_lowercase() == folded)
            .collect();
        match matches.as_slice() {
            [one] => Ok(one.to_string()),
            [] => Err(self.refused(format_args!(
                "names column {column}, which the export source lacks; the source columns are {}",
                columns.join(", ")
            ))),
            several => Err(self.refused(format_args!(
                "names column {column}, which matches the source columns {} only by letter case; \
                 quote the column's exact name",
                several
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(" and ")
            ))),
        }
    }

    /// Refuses `field` when it shares its name with an earlier field or another source column, or
    /// puts a second time transform on a column.
    fn check_against(
        &self,
        field: &PartitionField,
        earlier: &[PartitionField],
        columns: &[String],
    ) -> Result<(), UdfError> {
        if earlier.iter().any(|other| other.name == field.name) {
            return Err(self.refused(format_args!(
                "has the name {} of an earlier field",
                field.name
            )));
        }
        if field.name != field.column && columns.contains(&field.name) {
            return Err(self.refused(format_args!(
                "has the name {} of another source column",
                field.name
            )));
        }
        if is_time(field.transform)
            && earlier
                .iter()
                .any(|other| other.column == field.column && is_time(other.transform))
        {
            return Err(self.refused(format_args!(
                "puts a second one of year, month, day, and hour on column {}",
                field.column
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "partition_by_tests.rs"]
mod tests;
