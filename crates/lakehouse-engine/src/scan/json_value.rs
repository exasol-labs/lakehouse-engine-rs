//! `JSON_VALUE` in the scan with Exasol's semantics (#574): the whole document must be JSON, paths
//! run in lax mode over item sequences, and the result converts to the `RETURNING` type.

use arrow::array::{ArrayRef, AsArray, BooleanArray, StringArray};
use arrow::compute::cast;
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
    /// A string's text, or the source text of a number, `true`, or `false`.
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

/// The first step's members of an object document, found in the same pass that validates the
/// document. `None` when the path starts otherwise or that pass fails, and `select` then validates
/// and walks in two passes, which gives the same result: this pass only ever accepts less.
fn first_members<'a>(doc: &'a str, path: &[JsonPathStep]) -> Option<Vec<&'a RawValue>> {
    let Some(JsonPathStep::Member(name)) = path.first() else {
        return None;
    };
    let mut document = serde_json::Deserializer::from_str(doc);
    let found = document.deserialize_map(Members(name)).ok()?;
    document.end().ok()?;
    Some(found)
}

/// Lax mode: a member step looks into the objects of an array, an index step treats a non-array
/// as a one-item array, and a structural mismatch selects nothing. Raw values keep each number's
/// source text, and serde_json skips them without recursion, so no nesting depth overflows.
fn select<'a>(doc: &'a str, path: &[JsonPathStep]) -> Item<'a> {
    let (mut items, rest) = match first_members(doc, path) {
        Some(found) => (found, &path[1..]),
        None => {
            let Ok(root) = serde_json::from_str::<&RawValue>(doc) else {
                return Item::Error;
            };
            (vec![root], path)
        }
    };
    for step in rest {
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

fn select_each<'a>(
    documents: impl Iterator<Item = Option<&'a str>>,
    path: &[JsonPathStep],
) -> Vec<Option<Item<'a>>> {
    // Exasol reads the empty string as NULL.
    documents
        .map(|doc| doc.filter(|d| !d.is_empty()).map(|d| select(d, path)))
        .collect()
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
        let documents = args.args[0].to_array(args.number_rows)?;
        let other;
        // String arrays are read in place: a cast would copy every document of the batch.
        let items = match documents.data_type() {
            DataType::Utf8 => select_each(documents.as_string::<i32>().iter(), &path),
            DataType::LargeUtf8 => select_each(documents.as_string::<i64>().iter(), &path),
            DataType::Utf8View => select_each(documents.as_string_view().iter(), &path),
            _ => {
                other = cast(&documents, &DataType::Utf8)?;
                select_each(other.as_string::<i32>().iter(), &path)
            }
        };
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
