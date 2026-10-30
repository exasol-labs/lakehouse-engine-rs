//! A binary column reaches Exasol as lowercase hex, the default `HASHTYPE` text format, because
//! Exasol has no binary type and rejects `HASHTYPE` as a UDF output type.

use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

use crate::types::mapping::MAX_BINARY_BYTES;

pub(super) const BINARY_HEX_UDF_NAME: &str = "lakehouse_binary_hex";

/// The scan exposes a binary column's raw bytes under this alias, so a filter the adapter
/// translates to a binary literal keeps Parquet statistics pruning.
pub(crate) fn raw_bytes_alias(column: &str) -> String {
    format!("_LH_BYTES_{column}")
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// An empty value renders NULL, because Exasol stores an empty `VARCHAR` as NULL and a filter
/// over the rendered text must agree with it.
pub(super) fn render_hex(array: &ArrayRef, column: &str) -> Result<ArrayRef> {
    let mut rendered = StringBuilder::with_capacity(array.len(), 0);
    let mut text = String::new();
    for value in binary_values(array)? {
        match value {
            None | Some([]) => rendered.append_null(),
            Some(bytes) if bytes.len() > MAX_BINARY_BYTES => {
                return Err(DataFusionError::Execution(format!(
                    "column '{column}' holds a binary value of {} bytes, over the \
                     {MAX_BINARY_BYTES} bytes whose hex rendering fits VARCHAR(2000000)",
                    bytes.len()
                )));
            }
            Some(bytes) => {
                text.clear();
                push_hex(&mut text, bytes);
                rendered.append_value(&text);
            }
        }
    }
    Ok(Arc::new(rendered.finish()))
}

/// For a JSON object name, where an empty key stays `""` as an empty nested member does.
pub(super) fn hex_map_keys(keys: &ArrayRef) -> Result<ArrayRef> {
    let mut rendered = StringBuilder::with_capacity(keys.len(), 0);
    let mut text = String::new();
    for key in binary_values(keys)? {
        match key {
            None => rendered.append_null(),
            Some(bytes) => {
                text.clear();
                push_hex(&mut text, bytes);
                rendered.append_value(&text);
            }
        }
    }
    Ok(Arc::new(rendered.finish()))
}

fn binary_values(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>> {
    Ok(match array.data_type() {
        DataType::Binary => Box::new(array.as_binary::<i32>().iter()),
        DataType::LargeBinary => Box::new(array.as_binary::<i64>().iter()),
        DataType::BinaryView => Box::new(array.as_binary_view().iter()),
        DataType::FixedSizeBinary(_) => Box::new(array.as_fixed_size_binary().iter()),
        other => {
            return Err(DataFusionError::Internal(format!(
                "{BINARY_HEX_UDF_NAME} renders binary values, not {other}"
            )));
        }
    })
}

fn push_hex(text: &mut String, bytes: &[u8]) {
    for byte in bytes {
        text.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
}

/// The form every source carries a binary partition value in, so the scan decodes one form.
pub(crate) fn lowercase_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    push_hex(&mut text, bytes);
    text
}

pub(crate) fn decode_lowercase_hex(text: &str) -> Option<Vec<u8>> {
    let digit = |c: u8| HEX_DIGITS.iter().position(|&d| d == c).map(|v| v as u8);
    let (pairs, rest) = text.as_bytes().as_chunks::<2>();
    if !rest.is_empty() {
        return None;
    }
    pairs
        .iter()
        .map(|[high, low]| Some(digit(*high)? << 4 | digit(*low)?))
        .collect()
}

/// Called as `lakehouse_binary_hex(column, 'NAME')`; the name only labels the oversize error.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryHexUdf {
    signature: Signature,
}

impl ScalarUDFImpl for BinaryHexUdf {
    fn name(&self) -> &str {
        BINARY_HEX_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let column = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(column)))) => column.clone(),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{BINARY_HEX_UDF_NAME} takes the column name as its second argument"
                )));
            }
        };
        let array = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(render_hex(&array, &column)?))
    }
}

pub(super) fn register_binary_hex_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BinaryHexUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}

#[cfg(test)]
#[path = "binary_hex_tests.rs"]
mod tests;
