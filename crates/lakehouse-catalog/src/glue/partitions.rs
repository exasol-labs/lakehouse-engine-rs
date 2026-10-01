use crate::CatalogPartition;

use super::routing::{PARQUET_INPUT_FORMAT, format_of_input_format};
use super::source::GluePartition;
use super::trim_location;

/// Hive's literal for a NULL partition value.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Glue `Values` are positional against the table's partition keys. A partition declaring
/// no input format inherits the table's, which routing admitted as the Parquet one.
pub(super) fn neutral_partition(
    table: &str,
    partition_keys: &[String],
    partition: &GluePartition,
) -> Result<CatalogPartition, String> {
    let values = &partition.values;
    let location = partition
        .location
        .as_deref()
        .filter(|location| !location.is_empty())
        .ok_or_else(|| {
            format!("Glue partition {values:?} of table '{table}' declares no storage location")
        })?;
    if values.len() != partition_keys.len() {
        return Err(format!(
            "Glue partition at '{location}' of table '{table}' has {} values, but the table \
             declares {} partition keys",
            values.len(),
            partition_keys.len()
        ));
    }
    let input_format = partition
        .input_format
        .as_deref()
        .filter(|input_format| !input_format.is_empty())
        .unwrap_or(PARQUET_INPUT_FORMAT);

    Ok(CatalogPartition {
        values: partition_keys
            .iter()
            .cloned()
            .zip(values.iter().map(|value| partition_value(value)))
            .collect(),
        location: trim_location(location).to_string(),
        format: format_of_input_format(input_format),
        input_format: input_format.to_string(),
    })
}

fn partition_value(raw: &str) -> Option<String> {
    (raw != HIVE_DEFAULT_PARTITION).then(|| raw.to_string())
}

#[cfg(test)]
#[path = "partitions_tests.rs"]
mod tests;
