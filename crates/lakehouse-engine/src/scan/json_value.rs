//! `JSON_VALUE` in the scan (#574): each document parses into a `serde_json::Value`, paths run in
//! lax mode over item sequences, and the result converts to the `RETURNING` type.
//! docs/semantic-differences.md lists what this parse changes.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use vs_expression::{JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path};

/// What one path selects in one document.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Item<'a> {
    /// No item: `ON EMPTY` applies.
    Empty,
    /// Not JSON, several items, or a non-scalar item: `ON ERROR` applies.
    Error,
    Null,
    /// A string's text, or the text of a number, `true`, or `false`.
    Text(Cow<'a, str>),
}

/// `None` for a document that is not strict JSON.
fn parse(document: &str) -> Option<Value> {
    serde_json::from_str(document).ok()
}

/// Lax mode: a member step looks into the objects of an array, an index step treats a non-array
/// as a one-item array, and a structural mismatch selects nothing.
fn select<'a>(root: Option<&'a Value>, path: &[JsonPathStep]) -> Item<'a> {
    let Some(root) = root else {
        return Item::Error;
    };
    let mut items = vec![root];
    for step in path {
        items = items
            .into_iter()
            .flat_map(|item| match (step, item) {
                (JsonPathStep::Member(name), Value::Object(members)) => {
                    members.get(name).into_iter().collect()
                }
                (JsonPathStep::Member(name), Value::Array(elements)) => elements
                    .iter()
                    .filter_map(|element| element.as_object()?.get(name))
                    .collect(),
                (JsonPathStep::Index(index), Value::Array(elements)) => {
                    elements.get(*index as usize).into_iter().collect()
                }
                (JsonPathStep::Index(0), item) => vec![item],
                _ => Vec::new(),
            })
            .collect();
    }
    match items.as_slice() {
        [] => Item::Empty,
        [Value::Null] => Item::Null,
        [Value::String(text)] => Item::Text(Cow::Borrowed(text)),
        [Value::Bool(value)] => Item::Text(Cow::Borrowed(if *value { "true" } else { "false" })),
        [Value::Number(number)] => Item::Text(Cow::Owned(number.to_string())),
        _ => Item::Error,
    }
}

/// Exasol's text to BOOLEAN cast: spaces trimmed, case ignored, and digits read as a number.
fn boolean_from_text(text: &str) -> Option<bool> {
    let text = text.trim_matches(' ');
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return match text.trim_start_matches('0') {
            "" => Some(false),
            "1" => Some(true),
            _ => None,
        };
    }
    match text.to_ascii_uppercase().as_str() {
        "TRUE" | "T" | "YES" | "Y" => Some(true),
        "FALSE" | "F" | "NO" | "N" => Some(false),
        _ => None,
    }
}

/// `Err` means `ON ERROR` applies. Exasol reads the empty string as NULL.
fn text_value(text: &str, max_chars: usize) -> std::result::Result<Option<&str>, ()> {
    match text {
        "" => Ok(None),
        text if text.chars().count() > max_chars => Err(()),
        text => Ok(Some(text)),
    }
}

/// A NULL document or item gives NULL, and a failed conversion gives `on_error`.
fn resolve<'i, T: Copy>(
    item: Option<&'i Item<'_>>,
    convert: impl Fn(&'i str) -> std::result::Result<Option<T>, ()>,
    on_empty: Option<T>,
    on_error: Option<T>,
) -> Option<T> {
    match item? {
        Item::Null => None,
        Item::Empty => on_empty,
        Item::Error => on_error,
        Item::Text(text) => convert(text).unwrap_or(on_error),
    }
}

fn scalar_argument(args: &ScalarFunctionArgs, index: usize) -> Result<&ScalarValue> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(value)) => Ok(value),
        _ => Err(DataFusionError::Execution(format!(
            "JSON_VALUE scan argument {index} must be a constant"
        ))),
    }
}

/// `(document, path, max_chars, on_empty, on_error)`.
fn text_values(args: &ScalarFunctionArgs, items: &[Option<Item>]) -> Result<ArrayRef> {
    let ScalarValue::UInt64(Some(max_chars)) =
        scalar_argument(args, 2)?.cast_to(&DataType::UInt64)?
    else {
        return Err(DataFusionError::Execution(format!(
            "{JSON_VALUE_TEXT_FN} size"
        )));
    };
    let fallback = |index| match scalar_argument(args, index)? {
        ScalarValue::Null => Ok(None),
        other => other
            .try_as_str()
            .ok_or_else(|| DataFusionError::Execution(format!("fallback {other}"))),
    };
    let (on_empty, on_error) = (fallback(3)?, fallback(4)?);
    let text = |text| text_value(text, max_chars as usize);
    let values: StringArray = items
        .iter()
        .map(|item| resolve(item.as_ref(), text, on_empty, on_error))
        .collect();
    Ok(Arc::new(values))
}

/// `(document, path, on_empty, on_error)`.
fn boolean_values(args: &ScalarFunctionArgs, items: &[Option<Item>]) -> Result<ArrayRef> {
    let fallback = |index| match scalar_argument(args, index)? {
        ScalarValue::Boolean(value) => Ok(*value),
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Execution(format!("fallback {other}"))),
    };
    let (on_empty, on_error) = (fallback(2)?, fallback(3)?);
    let boolean = |text| boolean_from_text(text).map(Some).ok_or(());
    let values: BooleanArray = items
        .iter()
        .map(|item| resolve(item.as_ref(), boolean, on_empty, on_error))
        .collect();
    Ok(Arc::new(values))
}

/// The parsed documents of one batch. String arrays are read in place, without a copy.
fn parse_documents(documents: &ArrayRef) -> Result<Vec<Option<Option<Value>>>> {
    // Exasol reads the empty string as NULL.
    let parse = |document: Option<&str>| document.filter(|d| !d.is_empty()).map(parse);
    Ok(match documents.data_type() {
        DataType::Utf8 => documents.as_string::<i32>().iter().map(parse).collect(),
        DataType::LargeUtf8 => documents.as_string::<i64>().iter().map(parse).collect(),
        DataType::Utf8View => documents.as_string_view().iter().map(parse).collect(),
        _ => cast(documents, &DataType::Utf8)?
            .as_string::<i32>()
            .iter()
            .map(parse)
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Returning {
    Text,
    Boolean,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonUdf {
    name: &'static str,
    returning: Returning,
    signature: Signature,
}

impl ScalarUDFImpl for JsonUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.returning {
            Returning::Text => DataType::Utf8,
            Returning::Boolean => DataType::Boolean,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let path = scalar_argument(&args, 1)?
            .try_as_str()
            .flatten()
            .and_then(parse_json_path)
            .ok_or_else(|| DataFusionError::Execution(format!("{} path", self.name)))?;
        let documents = parse_documents(&args.args[0].to_array(args.number_rows)?)?;
        let items: Vec<Option<Item>> = documents
            .iter()
            .map(|root| root.as_ref().map(|root| select(root.as_ref(), &path)))
            .collect();
        let values = match self.returning {
            Returning::Text => text_values(&args, &items)?,
            Returning::Boolean => boolean_values(&args, &items)?,
        };
        Ok(ColumnarValue::Array(values))
    }
}

pub(super) fn register_json_value_udfs(ctx: &SessionContext) {
    for (name, returning, arity) in [
        (JSON_VALUE_TEXT_FN, Returning::Text, 5),
        (JSON_VALUE_BOOLEAN_FN, Returning::Boolean, 4),
    ] {
        ctx.register_udf(ScalarUDF::from(JsonUdf {
            name,
            returning,
            signature: Signature::any(arity, Volatility::Immutable),
        }));
    }
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
