//! Reads stored bytes as UTF-8 text, failing on the first invalid value. DataFusion's cast from a
//! binary column to text does not validate, and arrow-rs validates only an annotated `STRING`
//! column on read, so every bytes-to-text read of the scan goes through here.

use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) const BINARY_TEXT_UDF_NAME: &str = "lakehouse_binary_text";

/// Reuses the binary buffers without copying; `column` only labels the error.
pub(crate) fn bytes_as_text(array: &ArrayRef, column: &str) -> Result<ArrayRef> {
    let text: std::result::Result<ArrayRef, _> = match array.data_type() {
        DataType::Binary => StringArray::try_from_binary(array.as_binary::<i32>().clone())
            .map(|text| Arc::new(text) as ArrayRef),
        DataType::LargeBinary => {
            LargeStringArray::try_from_binary(array.as_binary::<i64>().clone())
                .map(|text| Arc::new(text) as ArrayRef)
        }
        DataType::BinaryView => array
            .as_binary_view()
            .clone()
            .to_string_view()
            .map(|text| Arc::new(text) as ArrayRef),
        other => {
            return Err(DataFusionError::Internal(format!(
                "{BINARY_TEXT_UDF_NAME} reads variable-length binary values, not {other}"
            )));
        }
    };
    text.map_err(|_| invalid_utf8(array, column))
}

fn invalid_utf8(array: &ArrayRef, column: &str) -> DataFusionError {
    let values: Box<dyn Iterator<Item = Option<&[u8]>>> = match array.data_type() {
        DataType::LargeBinary => Box::new(array.as_binary::<i64>().iter()),
        DataType::BinaryView => Box::new(array.as_binary_view().iter()),
        _ => Box::new(array.as_binary::<i32>().iter()),
    };
    let detail = values
        .flatten()
        .find_map(|bytes| std::str::from_utf8(bytes).err())
        .map(|error| format!(": {error}"))
        .unwrap_or_default();
    DataFusionError::Execution(format!(
        "column '{column}' holds a value that is not valid UTF-8, which BINARY_AS_STRING cannot \
         read as text{detail}"
    ))
}

/// Called as `lakehouse_binary_text(column, 'NAME')`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BinaryTextUdf {
    signature: Signature,
}

impl ScalarUDFImpl for BinaryTextUdf {
    fn name(&self) -> &str {
        BINARY_TEXT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types.first() {
            Some(DataType::LargeBinary) => DataType::LargeUtf8,
            Some(DataType::BinaryView) => DataType::Utf8View,
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let column = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(column)))) => column.clone(),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{BINARY_TEXT_UDF_NAME} takes the column name as its second argument"
                )));
            }
        };
        let array = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(bytes_as_text(&array, &column)?))
    }
}

pub(super) fn register_binary_text_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BinaryTextUdf {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}

#[cfg(test)]
#[path = "binary_text_tests.rs"]
mod tests;
