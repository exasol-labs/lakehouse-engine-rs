//! `JSON_VALUE` in the scan with Exasol's semantics (#574): the whole document must be strict JSON,
//! paths run in lax mode over item sequences, and the result converts to the `RETURNING` type.
//! [`JSON_PARSE_FN`] validates each document once into a tape that every value call reads.

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use vs_expression::{
    JSON_PARSE_FN, JSON_VALUE_BOOLEAN_FN, JSON_VALUE_TEXT_FN, JsonPathStep, parse_json_path,
};

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const NUMBER: u8 = 3;
const STRING: u8 = 4;
const ARRAY: u8 = 5;
const OBJECT: u8 = 6;

/// Pre-order nodes: a tag byte, then for a number or string a `u32` length and its bytes (numbers
/// keep their source text, strings are unescaped), and for an array or object the `u32` byte
/// length of its content. An object's content alternates a key (length and bytes) and a node.
/// An empty tape marks a document that is not JSON.
type Tape = Vec<u8>;

fn write_len(tape: &mut Tape, at: usize) {
    let len = (tape.len() - at - 4) as u32;
    tape[at..at + 4].copy_from_slice(&len.to_le_bytes());
}

fn read_len(tape: &[u8], at: usize) -> usize {
    u32::from_le_bytes(tape[at..at + 4].try_into().expect("4-byte length")) as usize
}

/// Iterative, so no nesting depth overflows the stack: Exasol sets no depth limit.
fn parse(doc: &str) -> Option<Tape> {
    let bytes = doc.as_bytes();
    let mut tape = Tape::with_capacity(bytes.len() + 16);
    let mut pos = 0;
    // The header offset of each open container, and whether it is an object.
    let mut open: Vec<(usize, bool)> = Vec::new();
    let skip_ws = |pos: &mut usize| {
        while matches!(bytes.get(*pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            *pos += 1;
        }
    };
    let key = |pos: &mut usize, tape: &mut Tape| -> Option<()> {
        string(bytes, pos, tape)?;
        skip_ws(pos);
        (bytes.get(*pos) == Some(&b':')).then(|| *pos += 1)?;
        skip_ws(pos);
        Some(())
    };
    skip_ws(&mut pos);
    loop {
        match *bytes.get(pos)? {
            container @ (b'{' | b'[') => {
                let object = container == b'{';
                tape.push(if object { OBJECT } else { ARRAY });
                open.push((tape.len(), object));
                tape.extend([0; 4]);
                pos += 1;
                skip_ws(&mut pos);
                let close = if object { b'}' } else { b']' };
                if bytes.get(pos) != Some(&close) {
                    if object {
                        key(&mut pos, &mut tape)?;
                    }
                    continue;
                }
            }
            b'"' => {
                tape.push(STRING);
                string(bytes, &mut pos, &mut tape)?;
            }
            b't' => literal(bytes, &mut pos, b"true", TRUE, &mut tape)?,
            b'f' => literal(bytes, &mut pos, b"false", FALSE, &mut tape)?,
            b'n' => literal(bytes, &mut pos, b"null", NULL, &mut tape)?,
            _ => number(bytes, &mut pos, &mut tape)?,
        }
        loop {
            skip_ws(&mut pos);
            let Some(&(at, object)) = open.last() else {
                return (pos == bytes.len()).then_some(tape);
            };
            match *bytes.get(pos)? {
                b',' => {
                    pos += 1;
                    skip_ws(&mut pos);
                    if object {
                        key(&mut pos, &mut tape)?;
                    }
                    break;
                }
                b'}' if object => {}
                b']' if !object => {}
                _ => return None,
            }
            pos += 1;
            open.pop();
            write_len(&mut tape, at);
        }
    }
}

fn literal(bytes: &[u8], pos: &mut usize, word: &[u8], tag: u8, tape: &mut Tape) -> Option<()> {
    bytes[*pos..].starts_with(word).then(|| {
        *pos += word.len();
        tape.push(tag);
    })
}

/// RFC 8259 number grammar, kept as source text.
fn number(bytes: &[u8], pos: &mut usize, tape: &mut Tape) -> Option<()> {
    let start = *pos;
    let digits = |pos: &mut usize| {
        let from = *pos;
        while bytes.get(*pos).is_some_and(u8::is_ascii_digit) {
            *pos += 1;
        }
        *pos > from
    };
    if bytes.get(*pos) == Some(&b'-') {
        *pos += 1;
    }
    if bytes.get(*pos) == Some(&b'0') {
        *pos += 1;
    } else if !digits(pos) {
        return None;
    }
    if bytes.get(*pos) == Some(&b'.') {
        *pos += 1;
        digits(pos).then_some(())?;
    }
    if matches!(bytes.get(*pos), Some(b'e' | b'E')) {
        *pos += 1;
        if matches!(bytes.get(*pos), Some(b'+' | b'-')) {
            *pos += 1;
        }
        digits(pos).then_some(())?;
    }
    tape.push(NUMBER);
    tape.extend_from_slice(&((*pos - start) as u32).to_le_bytes());
    tape.extend_from_slice(&bytes[start..*pos]);
    Some(())
}

/// Writes the unescaped string at `pos` as a length and its bytes. A control character, an unknown
/// escape, or a lone surrogate is not JSON.
fn string(bytes: &[u8], pos: &mut usize, tape: &mut Tape) -> Option<()> {
    (bytes.get(*pos) == Some(&b'"')).then_some(())?;
    *pos += 1;
    let at = tape.len();
    tape.extend([0; 4]);
    loop {
        let run = bytes[*pos..]
            .iter()
            .position(|b| matches!(b, b'"' | b'\\') || *b < 0x20)?;
        tape.extend_from_slice(&bytes[*pos..*pos + run]);
        *pos += run;
        match bytes[*pos] {
            b'"' => break,
            b'\\' => {
                let unescaped = match *bytes.get(*pos + 1)? {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'u' => {
                        let high = hex4(bytes, *pos + 2)?;
                        *pos += 4;
                        let code = match high {
                            0xD800..=0xDBFF => {
                                bytes[*pos + 2..].starts_with(b"\\u").then_some(())?;
                                let low = hex4(bytes, *pos + 4)?;
                                (0xDC00..=0xDFFF).contains(&low).then_some(())?;
                                *pos += 6;
                                0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
                            }
                            0xDC00..=0xDFFF => return None,
                            code => code,
                        };
                        char::from_u32(code)?
                    }
                    _ => return None,
                };
                let mut utf8 = [0; 4];
                tape.extend_from_slice(unescaped.encode_utf8(&mut utf8).as_bytes());
                *pos += 2;
            }
            _ => return None,
        }
    }
    *pos += 1;
    write_len(tape, at);
    Some(())
}

fn hex4(bytes: &[u8], at: usize) -> Option<u32> {
    let digits = std::str::from_utf8(bytes.get(at..at + 4)?).ok()?;
    u32::from_str_radix(digits, 16)
        .ok()
        .filter(|_| digits.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn node_end(tape: &[u8], at: usize) -> usize {
    match tape[at] {
        NULL | FALSE | TRUE => at + 1,
        _ => at + 5 + read_len(tape, at + 1),
    }
}

/// The child node offsets of the container at `at`, with each object member's key.
fn children(tape: &[u8], at: usize) -> impl Iterator<Item = (Option<&[u8]>, usize)> {
    let object = tape[at] == OBJECT;
    let end = node_end(tape, at);
    let mut pos = at + 5;
    std::iter::from_fn(move || {
        if pos >= end {
            return None;
        }
        let key = object.then(|| {
            let len = read_len(tape, pos);
            let key = &tape[pos + 4..pos + 4 + len];
            pos += 4 + len;
            key
        });
        let node = pos;
        pos = node_end(tape, node);
        Some((key, node))
    })
}

/// What one path selects in one document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Item<'a> {
    /// No item: `ON EMPTY` applies.
    Empty,
    /// Not JSON, several items, or a non-scalar item: `ON ERROR` applies.
    Error,
    Null,
    Boolean(bool),
    /// A string's text or a number's source text.
    Text(&'a str),
}

/// Lax mode: a member step looks into the objects of an array, an index step treats a non-array
/// as a one-item array, and a structural mismatch selects nothing.
fn select<'a>(tape: &'a [u8], path: &[JsonPathStep]) -> Item<'a> {
    if tape.is_empty() {
        return Item::Error;
    }
    let mut items = vec![0];
    for step in path {
        let mut next = Vec::new();
        for &item in &items {
            match step {
                JsonPathStep::Member(name) => {
                    let objects: Vec<usize> = match tape[item] {
                        OBJECT => vec![item],
                        ARRAY => children(tape, item)
                            .map(|(_, node)| node)
                            .filter(|node| tape[*node] == OBJECT)
                            .collect(),
                        _ => Vec::new(),
                    };
                    for object in objects {
                        next.extend(
                            children(tape, object)
                                .filter(|(key, _)| *key == Some(name.as_bytes()))
                                .map(|(_, node)| node),
                        );
                    }
                }
                JsonPathStep::Index(index) if tape[item] == ARRAY => {
                    next.extend(children(tape, item).nth(*index as usize).map(|(_, n)| n));
                }
                JsonPathStep::Index(0) => next.push(item),
                JsonPathStep::Index(_) => {}
            }
        }
        items = next;
    }
    match items.as_slice() {
        [] => Item::Empty,
        [item] => match tape[*item] {
            NULL => Item::Null,
            FALSE => Item::Boolean(false),
            TRUE => Item::Boolean(true),
            NUMBER | STRING => {
                let len = read_len(tape, item + 1);
                std::str::from_utf8(&tape[item + 5..item + 5 + len]).map_or(Item::Error, Item::Text)
            }
            _ => Item::Error,
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
fn text_value<'a>(item: &Item<'a>, max_chars: usize) -> Option<std::result::Result<&'a str, ()>> {
    let text = match *item {
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
                "{JSON_PARSE_FN} reads text, not {other}"
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
    fn parse_documents(&self, args: &ScalarFunctionArgs) -> Result<ArrayRef> {
        let documents = args.args[0].to_array(args.number_rows)?;
        // Exasol reads the empty string as NULL.
        let tapes: BinaryArray = texts(&documents)?
            .into_iter()
            .map(|doc| {
                doc.filter(|d| !d.is_empty())
                    .map(|d| parse(d).unwrap_or_default())
            })
            .collect();
        Ok(Arc::new(tapes))
    }

    fn values(&self, args: &ScalarFunctionArgs) -> Result<ArrayRef> {
        let tapes = args.args[0].to_array(args.number_rows)?;
        let tapes = tapes.as_binary_opt::<i32>().ok_or_else(|| {
            DataFusionError::Execution(format!("{} reads a {JSON_PARSE_FN} tape", self.name))
        })?;
        let path = scalar_argument(args, 1)?
            .try_as_str()
            .flatten()
            .and_then(parse_json_path)
            .ok_or_else(|| DataFusionError::Execution(format!("{} path", self.name)))?;
        let items = tapes
            .iter()
            .map(|tape| tape.map(|tape| select(tape, &path)));
        if self.name == JSON_VALUE_BOOLEAN_FN {
            let fallback = |index| match scalar_argument(args, index)? {
                ScalarValue::Boolean(value) => Ok(*value),
                ScalarValue::Null => Ok(None),
                other => Err(DataFusionError::Execution(format!("fallback {other}"))),
            };
            let (on_empty, on_error) = (fallback(2)?, fallback(3)?);
            let values: BooleanArray = items
                .map(|item| match boolean_value(&item?)? {
                    Ok(value) => Some(value),
                    Err(()) if item == Some(Item::Empty) => on_empty,
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
            .map(|item| match text_value(item.as_ref()?, max_chars)? {
                Ok(value) => Some(value),
                Err(()) if item == Some(Item::Empty) => on_empty.as_deref(),
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
            JSON_PARSE_FN => DataType::Binary,
            JSON_VALUE_BOOLEAN_FN => DataType::Boolean,
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let values = if self.name == JSON_PARSE_FN {
            self.parse_documents(&args)?
        } else {
            self.values(&args)?
        };
        Ok(ColumnarValue::Array(values))
    }
}

pub(super) fn register_json_value_udfs(ctx: &SessionContext) {
    for (name, arity) in [
        (JSON_PARSE_FN, 1),
        (JSON_VALUE_TEXT_FN, 5),
        (JSON_VALUE_BOOLEAN_FN, 4),
    ] {
        ctx.register_udf(ScalarUDF::from(JsonUdf {
            name,
            signature: Signature::any(arity, Volatility::Immutable),
        }));
    }
}

#[cfg(test)]
#[path = "json_value_tests.rs"]
mod tests;
