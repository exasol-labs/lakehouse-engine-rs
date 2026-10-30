//! A binary column reaches Exasol as lowercase hex, the default `HASHTYPE` text format, because
//! Exasol has no binary type and rejects `HASHTYPE` as a UDF output type.

use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef, OffsetSizeTrait, StringBuilder};
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

/// An empty value renders NULL, because Exasol stores an empty `VARCHAR` as NULL and a filter
/// over the rendered text must agree with it.
pub(super) fn render_hex(array: &ArrayRef, column: &str) -> Result<ArrayRef> {
    hex_strings(array, |bytes| {
        if bytes.len() > MAX_BINARY_BYTES {
            return Err(DataFusionError::Execution(format!(
                "column '{column}' holds a binary value of {} bytes, over the \
                 {MAX_BINARY_BYTES} bytes whose hex rendering fits VARCHAR(2000000)",
                bytes.len()
            )));
        }
        Ok(!bytes.is_empty())
    })
}

/// For a JSON object name, where an empty key stays `""` as an empty nested member does.
pub(super) fn hex_map_keys(keys: &ArrayRef) -> Result<ArrayRef> {
    hex_strings(keys, |_| Ok(true))
}

/// `rendered` decides per value whether it renders as hex or as NULL.
fn hex_strings(
    array: &ArrayRef,
    mut rendered: impl FnMut(&[u8]) -> Result<bool>,
) -> Result<ArrayRef> {
    let mut hex = StringBuilder::with_capacity(array.len(), 2 * value_bytes(array));
    let mut digits = Vec::new();
    for value in binary_values(array)? {
        match value {
            Some(bytes) if rendered(bytes)? => hex.append_value(hex_digits(bytes, &mut digits)?),
            _ => hex.append_null(),
        }
    }
    Ok(Arc::new(hex.finish()))
}

fn value_bytes(array: &ArrayRef) -> usize {
    fn span<O: OffsetSizeTrait>(offsets: &[O]) -> usize {
        (offsets[offsets.len() - 1] - offsets[0]).as_usize()
    }
    match array.data_type() {
        DataType::Binary => span(array.as_binary::<i32>().value_offsets()),
        DataType::LargeBinary => span(array.as_binary::<i64>().value_offsets()),
        DataType::BinaryView => array.as_binary_view().total_bytes_len(),
        DataType::FixedSizeBinary(width) => array.len() * usize::try_from(*width).unwrap_or(0),
        _ => 0,
    }
}

pub(super) fn binary_values(
    array: &ArrayRef,
) -> Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>> {
    Ok(match array.data_type() {
        DataType::Binary => Box::new(array.as_binary::<i32>().iter()),
        DataType::LargeBinary => Box::new(array.as_binary::<i64>().iter()),
        DataType::BinaryView => Box::new(array.as_binary_view().iter()),
        DataType::FixedSizeBinary(_) => Box::new(array.as_fixed_size_binary().iter()),
        other => {
            return Err(DataFusionError::Internal(format!(
                "a binary value was expected, not {other}"
            )));
        }
    })
}

/// Reuses `digits` across values, so a batch allocates no `String` per value.
fn hex_digits<'a>(bytes: &[u8], digits: &'a mut Vec<u8>) -> Result<&'a str> {
    digits.resize(2 * bytes.len(), 0);
    hex::encode_to_slice(bytes, digits).map_err(|e| DataFusionError::Internal(e.to_string()))?;
    std::str::from_utf8(digits).map_err(|e| DataFusionError::Internal(e.to_string()))
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
        let column = column_label(&args, BINARY_HEX_UDF_NAME)?;
        let array = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(render_hex(&array, column)?))
    }
}

/// The column name a binary UDF takes as its second argument, to label its errors.
pub(super) fn column_label<'a>(args: &'a ScalarFunctionArgs, udf_name: &str) -> Result<&'a str> {
    match args.args.get(1) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(column)))) => Ok(column),
        _ => Err(DataFusionError::Internal(format!(
            "{udf_name} takes the column name as its second argument"
        ))),
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
