//! The `JSON_VALUE` calls the scan computes: a path of member and array-index steps, a `RETURNING`
//! type that needs no session setting, and constant `ON EMPTY` and `ON ERROR` results (#574).
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

use crate::exasol_type::{ExasolType, MAX_VARCHAR_SIZE, exasol_type_from_json};
use crate::scan_typing::decline;

/// Parses a document once into the tape the value functions read, so DataFusion's common
/// subexpression elimination shares one parse among the calls on one document.
pub const JSON_PARSE_FN: &str = "lakehouse_json_parse";
/// `(tape, path, max_chars, on_empty, on_error)`, returning text.
pub const JSON_VALUE_TEXT_FN: &str = "lakehouse_json_value_text";
/// `(tape, path, on_empty, on_error)`, returning BOOLEAN.
pub const JSON_VALUE_BOOLEAN_FN: &str = "lakehouse_json_value_boolean";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathStep {
    Member(String),
    Index(u32),
}

/// Exasol fails the statement on a larger index.
const MAX_ARRAY_INDEX: u32 = i32::MAX as u32;

/// `None` for a path outside the subset the scan evaluates: `$` and then `.name`, `."name"`, and
/// `[n]` steps, without whitespace, mode keyword, wildcard, list, range, or item method.
pub fn parse_json_path(path: &str) -> Option<Vec<JsonPathStep>> {
    let mut rest = path.strip_prefix('$')?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        let (step, tail) = if let Some(quoted) = rest.strip_prefix(".\"") {
            let end = quoted.find('"')?;
            let key = &quoted[..end];
            if key.contains('\\') {
                return None;
            }
            (JsonPathStep::Member(key.to_string()), &quoted[end + 1..])
        } else if let Some(member) = rest.strip_prefix('.') {
            let end = member
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(member.len());
            let key = &member[..end];
            if !key.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                return None;
            }
            (JsonPathStep::Member(key.to_string()), &member[end..])
        } else {
            let index = rest.strip_prefix('[')?;
            let end = index.find(']')?;
            let digits = &index[..end];
            let canonical = !digits.is_empty()
                && digits.bytes().all(|b| b.is_ascii_digit())
                && (digits == "0" || !digits.starts_with('0'));
            let n = digits
                .parse::<u32>()
                .ok()
                .filter(|n| canonical && *n <= MAX_ARRAY_INDEX)?;
            (JsonPathStep::Index(n), &index[end + 1..])
        };
        steps.push(step);
        rest = tail;
    }
    Some(steps)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Returning {
    Text { max_chars: u32 },
    Boolean,
}

/// The constant an `ON EMPTY` or `ON ERROR` clause yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fallback {
    Null,
    Text(String),
    Boolean(bool),
}

pub(crate) struct JsonValueCall<'a> {
    pub(crate) document: &'a Json,
    pub(crate) path: &'a str,
    pub(crate) returning: Returning,
    pub(crate) on_empty: Fallback,
    pub(crate) on_error: Fallback,
}

impl<'a> JsonValueCall<'a> {
    /// `Err` declines the call to Exasol.
    pub(crate) fn parse(node: &'a Json) -> Result<Self, UdfError> {
        let refuse = |why: &str| Err(decline(format!("JSON_VALUE {why}")));
        let [document, path] = node
            .get("arguments")
            .and_then(Json::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        else {
            return refuse("takes a document and a path");
        };
        let Some(path) = path
            .get("type")
            .filter(|t| *t == "literal_string")
            .and_then(|_| path.get("value"))
            .and_then(Json::as_str)
            .filter(|p| parse_json_path(p).is_some())
        else {
            return refuse(&format!("path {path} is outside the evaluated subset"));
        };
        let returning = match node
            .get("returningDataType")
            .map(exasol_type_from_json)
            .transpose()?
        {
            None => Returning::Text {
                max_chars: MAX_VARCHAR_SIZE,
            },
            Some(ExasolType::Varchar { size, ascii: false }) => Returning::Text { max_chars: size },
            Some(ExasolType::Boolean) => Returning::Boolean,
            Some(other) => {
                return refuse(&format!(
                    "RETURNING {other} converts by the session's NLS settings or pads"
                ));
            }
        };
        Ok(Self {
            document,
            path,
            returning,
            on_empty: fallback(node.get("emptyBehavior"), returning)?,
            on_error: fallback(node.get("errorBehavior"), returning)?,
        })
    }
}

/// Exasol casts a `DEFAULT` to the `RETURNING` type, failing the statement when it does not fit, so
/// only a literal that is already of that type runs in the scan.
fn fallback(behavior: Option<&Json>, returning: Returning) -> Result<Fallback, UdfError> {
    let Some(behavior) = behavior else {
        return Ok(Fallback::Null);
    };
    let expression = behavior.get("expression").unwrap_or(&Json::Null);
    let value = expression.get("value");
    let fallback = match (behavior.get("type").and_then(Json::as_str), returning) {
        (Some("NULL"), _) => Some(Fallback::Null),
        (Some("DEFAULT"), _) if expression.get("type") == Some(&"literal_null".into()) => {
            Some(Fallback::Null)
        }
        (Some("DEFAULT"), Returning::Text { max_chars }) => value
            .and_then(Json::as_str)
            .filter(|_| expression.get("type") == Some(&"literal_string".into()))
            .filter(|text| text.chars().count() <= max_chars as usize)
            .map(|text| match text {
                "" => Fallback::Null,
                text => Fallback::Text(text.to_string()),
            }),
        (Some("DEFAULT"), Returning::Boolean) => value
            .and_then(Json::as_bool)
            .filter(|_| expression.get("type") == Some(&"literal_bool".into()))
            .map(Fallback::Boolean),
        _ => None,
    };
    fallback.ok_or_else(|| decline(format!("JSON_VALUE behavior {behavior} runs in Exasol")))
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
