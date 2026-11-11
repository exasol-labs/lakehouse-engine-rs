//! `JSON_VALUE` in the scan with Exasol's semantics (#574): the whole document must be JSON, paths
//! run in lax mode over item sequences, and the result converts to the `RETURNING` type.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde::Deserializer as _;
use serde::de::{MapAccess, Visitor};
use serde_json::value::RawValue;
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
    Boolean(bool),
    /// A string's text or a number's source text.
    Text(Cow<'a, str>),
}

/// Every member of one name, duplicates included, which a JSON object type would collapse.
struct Members<'p>(&'p str);

impl<'de> Visitor<'de> for Members<'_> {
    type Value = Vec<&'de RawValue>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut found = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            let value: &RawValue = map.next_value()?;
            if key == self.0 {
                found.push(value);
            }
        }
        Ok(found)
    }
}

fn members<'a>(object: &'a RawValue, name: &str) -> Vec<&'a RawValue> {
    serde_json::Deserializer::from_str(object.get())
        .deserialize_map(Members(name))
        .unwrap_or_default()
}

fn elements(array: &RawValue) -> Vec<&RawValue> {
    serde_json::from_str(array.get()).unwrap_or_default()
}

fn kind(value: &RawValue) -> u8 {
    value.get().as_bytes()[0]
}

/// Lax mode: a member step looks into the objects of an array, an index step treats a non-array
/// as a one-item array, and a structural mismatch selects nothing. Raw values keep each number's
/// source text, and serde_json skips them without recursion, so no nesting depth overflows.
fn select<'a>(doc: &'a str, path: &[JsonPathStep]) -> Item<'a> {
    let Ok(root) = serde_json::from_str::<&RawValue>(doc) else {
        return Item::Error;
    };
    let mut items = vec![root];
    for step in path {
        items = items
            .into_iter()
            .flat_map(|item| match (step, kind(item)) {
                (JsonPathStep::Member(name), b'{') => members(item, name),
                (JsonPathStep::Member(name), b'[') => elements(item)
                    .into_iter()
                    .filter(|element| kind(element) == b'{')
                    .flat_map(|object| members(object, name))
                    .collect(),
                (JsonPathStep::Index(index), b'[') => elements(item)
                    .into_iter()
                    .skip(*index as usize)
                    .take(1)
                    .collect(),
                (JsonPathStep::Index(0), _) => vec![item],
                _ => Vec::new(),
            })
            .collect();
    }
    match items.as_slice() {
        [] => Item::Empty,
        [item] => match kind(item) {
            b'n' => Item::Null,
            b't' => Item::Boolean(true),
            b'f' => Item::Boolean(false),
            b'"' => serde_json::from_str::<String>(item.get())
                .map_or(Item::Error, |text| Item::Text(text.into())),
            b'{' | b'[' => Item::Error,
            _ => Item::Text(item.get().into()),
        },
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

/// `None` is the result NULL; `Err(())` means `ON ERROR` applies.
fn text_value<'i>(
    item: &'i Item<'_>,
    max_chars: usize,
) -> Option<std::result::Result<&'i str, ()>> {
    let text = match item {
        Item::Null => return None,
        Item::Boolean(true) => "true",
        Item::Boolean(false) => "false",
        Item::Text(text) => text,
        Item::Empty | Item::Error => return Some(Err(())),
    };
    match text {
        "" => None,
        text if text.chars().count() > max_chars => Some(Err(())),
        text => Some(Ok(text)),
    }
}

fn boolean_value(item: &Item<'_>) -> Option<std::result::Result<bool, ()>> {
    match item {
        Item::Null => None,
        Item::Boolean(value) => Some(Ok(*value)),
        Item::Text(text) => Some(boolean_from_text(text).ok_or(())),
        Item::Empty | Item::Error => Some(Err(())),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonUdf {
    name: &'static str,
    signature: Signature,
}

fn texts(array: &ArrayRef) -> Result<Vec<Option<&str>>> {
    Ok(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => array.as_string::<i64>().iter().collect(),
        DataType::Utf8View => array.as_string_view().iter().collect(),
        DataType::Null => vec![None; array.len()],
        other => {
            return Err(DataFusionError::Execution(format!(
                "JSON_VALUE reads a text document, not {other}"
            )));
        }
    })
}

fn scalar_argument(args: &ScalarFunctionArgs, index: usize) -> Result<&ScalarValue> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(value)) => Ok(value),
        _ => Err(DataFusionError::Execution(format!(
            "JSON_VALUE scan argument {index} must be a constant"
        ))),
    }
}

impl JsonUdf {
    fn values(&self, args: &ScalarFunctionArgs) -> Result<ArrayRef> {
        let documents = args.args[0].to_array(args.number_rows)?;
        let path = scalar_argument(args, 1)?
            .try_as_str()
            .flatten()
            .and_then(parse_json_path)
            .ok_or_else(|| DataFusionError::Execution(format!("{} path", self.name)))?;
        // Exasol reads the empty string as NULL.
        let items: Vec<Option<Item>> = texts(&documents)?
            .into_iter()
            .map(|doc| doc.filter(|d| !d.is_empty()).map(|d| select(d, &path)))
            .collect();
        if self.name == JSON_VALUE_BOOLEAN_FN {
            let fallback = |index| match scalar_argument(args, index)? {
                ScalarValue::Boolean(value) => Ok(*value),
                ScalarValue::Null => Ok(None),
                other => Err(DataFusionError::Execution(format!("fallback {other}"))),
            };
            let (on_empty, on_error) = (fallback(2)?, fallback(3)?);
            let values: BooleanArray = items
                .iter()
                .map(|item| match boolean_value(item.as_ref()?)? {
                    Ok(value) => Some(value),
                    Err(()) if item == &Some(Item::Empty) => on_empty,
                    Err(()) => on_error,
                })
                .collect();
            return Ok(Arc::new(values));
        }
        let max_chars = scalar_argument(args, 2)?
            .cast_to(&DataType::UInt64)
            .ok()
            .and_then(|value| match value {
                ScalarValue::UInt64(Some(max)) => Some(max as usize),
                _ => None,
            })
            .ok_or_else(|| DataFusionError::Execution(format!("{} size", self.name)))?;
        let fallback = |index| match scalar_argument(args, index)? {
            ScalarValue::Null => Ok(None),
            other => other
                .try_as_str()
                .map(|text| text.map(str::to_string))
                .ok_or_else(|| DataFusionError::Execution(format!("fallback {other}"))),
        };
        let (on_empty, on_error) = (fallback(3)?, fallback(4)?);
        let values: StringArray = items
            .iter()
            .map(|item| match text_value(item.as_ref()?, max_chars)? {
                Ok(value) => Some(value),
                Err(()) if item == &Some(Item::Empty) => on_empty.as_deref(),
                Err(()) => on_error.as_deref(),
            })
            .collect();
        Ok(Arc::new(values))
    }
}

impl ScalarUDFImpl for JsonUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.name {
            JSON_VALUE_BOOLEAN_FN => DataType::Boolean,
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Array(self.values(&args)?))
    }
}

pub(super) fn register_json_value_udfs(ctx: &SessionContext) {
    for (name, arity) in [(JSON_VALUE_TEXT_FN, 5), (JSON_VALUE_BOOLEAN_FN, 4)] {
        ctx.register_udf(ScalarUDF::from(JsonUdf {
            name,
            signature: Signature::any(arity, Volatility::Immutable),
        }));
    }
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
