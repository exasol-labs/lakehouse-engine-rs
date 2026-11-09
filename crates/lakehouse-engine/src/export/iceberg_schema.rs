//! The Iceberg schema of an export table, derived from the export's Arrow schema with field IDs
//! 1 to n in input order. The type rules live in `arrow_schema.rs`, not here (decision [8]).

use arrow::datatypes::Schema;
use exasol_udf_sdk::error::UdfError;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;

pub(super) fn iceberg_schema(arrow: &Schema) -> Result<iceberg::spec::Schema, UdfError> {
    arrow_schema_to_schema_auto_assign_ids(arrow).map_err(|error| {
        UdfError::User(format!(
            "could not derive the Iceberg schema of the export columns: {error}"
        ))
    })
}

#[cfg(test)]
#[path = "iceberg_schema_tests.rs"]
mod tests;
