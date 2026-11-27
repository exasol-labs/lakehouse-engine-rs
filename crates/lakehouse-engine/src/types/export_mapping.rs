//! The export's side of the type mapping: the Arrow type an Exasol source column writes as, and
//! which table column types each source type fills.

use crate::types::exasol_type::ExasolType;
use arrow::datatypes::{DataType, TimeUnit};
use vs_expression::timestamp::TimestampUnit;

/// The Arrow type of an Exasol source column, `None` for a type the export cannot write.
pub(crate) fn exasol_to_arrow(exasol: &ExasolType) -> Option<DataType> {
    Some(match exasol {
        ExasolType::Decimal(decimal) => {
            DataType::Decimal128(decimal.precision(), decimal.scale() as i8)
        }
        ExasolType::DoublePrecision => DataType::Float64,
        ExasolType::Boolean => DataType::Boolean,
        ExasolType::Varchar { .. } | ExasolType::Char { .. } => DataType::Utf8,
        ExasolType::Date => DataType::Date32,
        // Exasol truncates finer timestamps to microseconds before the writer reads them.
        ExasolType::Timestamp { precision }
            if *precision <= TimestampUnit::Microsecond.digits() =>
        {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ => return None,
    })
}

/// Whether a source column of type `source` fills a table column of Arrow type `column`. Integer
/// and `timestamptz` columns accept by value, so rows read through a virtual schema fill them
/// again.
pub(crate) fn writable(column: &DataType, source: &ExasolType) -> bool {
    let Some(source) = exasol_to_arrow(source) else {
        return false;
    };
    match (column, source) {
        (DataType::Decimal128(precision, scale), DataType::Decimal128(p, s)) => {
            s == *scale && p <= *precision
        }
        (DataType::Int32 | DataType::Int64, DataType::Decimal128(_, 0)) => true,
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ) => true,
        (column, source) => {
            matches!(
                column,
                DataType::Float64 | DataType::Boolean | DataType::Utf8 | DataType::Date32
            ) && *column == source
        }
    }
}

#[cfg(test)]
#[path = "export_mapping_tests.rs"]
mod tests;
