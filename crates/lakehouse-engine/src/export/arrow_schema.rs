//! Maps the export's source columns to Arrow, the type model every target format derives its
//! own schema from. It owns the export's type rules and refusals (decisions [3] and [8]).

use super::refusal;
use crate::types::exasol_type::{
    ExasolType, UdfColumnType, exasol_type_from_column_info, is_reported_spelling,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::ColumnInfo;

/// Exasol truncates finer timestamps to microseconds before the writer reads them.
const MAX_TIMESTAMP_PRECISION: u8 = 6;

/// The Arrow schema of the source columns, named by `names`, in input order and every field
/// nullable, so every writer of one export derives the same schema.
pub(super) fn arrow_schema(names: &[String], columns: &[ColumnInfo]) -> Result<Schema, UdfError> {
    names
        .iter()
        .zip(columns)
        .map(|(name, column)| Ok(Field::new(name, arrow_type(name, column)?, true)))
        .collect::<Result<Vec<_>, UdfError>>()
        .map(Schema::new)
}

fn arrow_type(name: &str, column: &ColumnInfo) -> Result<DataType, UdfError> {
    let exasol_type = match exasol_type_from_column_info(column) {
        UdfColumnType::Known(exasol_type)
            if is_reported_spelling(exasol_type, &column.type_name) =>
        {
            exasol_type
        }
        _ => return Err(unwritable(name, &column.type_name)),
    };
    Ok(match exasol_type {
        ExasolType::Decimal(decimal) => {
            DataType::Decimal128(decimal.precision(), decimal.scale() as i8)
        }
        ExasolType::DoublePrecision => DataType::Float64,
        ExasolType::Boolean => DataType::Boolean,
        ExasolType::Varchar { .. } | ExasolType::Char { .. } => DataType::Utf8,
        ExasolType::Date => DataType::Date32,
        ExasolType::Timestamp { precision } if precision <= MAX_TIMESTAMP_PRECISION => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ => return Err(unwritable(name, &column.type_name)),
    })
}

fn unwritable(name: &str, type_name: &str) -> UdfError {
    refusal(format_args!(
        "column {name} has type {type_name}, which the export cannot write; it writes DECIMAL, \
         DOUBLE, BOOLEAN, VARCHAR, CHAR, DATE, and TIMESTAMP(0) to TIMESTAMP(6) columns; cast a \
         TIMESTAMP(7) to TIMESTAMP(9) column to TIMESTAMP(6)"
    ))
}

#[cfg(test)]
#[path = "arrow_schema_tests.rs"]
mod tests;
