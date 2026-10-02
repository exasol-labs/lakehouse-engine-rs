use crate::{CatalogPartition, HIVE_DEFAULT_PARTITION, PartitionFormat};

use super::routing::PARQUET_INPUT_FORMAT;
use super::source::GluePartition;
use super::trim_location;

/// Glue `Values` are positional against the table's partition keys. A partition declaring
/// no input format inherits the table's, which routing admitted as the Parquet one.
pub(super) fn neutral_partition(
    table: &str,
    partition_keys: &[String],
    partition: GluePartition,
) -> Result<CatalogPartition, String> {
    let GluePartition {
        values,
        location,
        input_format,
    } = partition;
    let Some(location) = location.filter(|location| !location.is_empty()) else {
        return Err(format!(
            "Glue partition {values:?} of table '{table}' declares no storage location"
        ));
    };
    if values.len() != partition_keys.len() {
        return Err(format!(
            "Glue partition at '{location}' of table '{table}' has {} values, but the table \
             declares {} partition keys",
            values.len(),
            partition_keys.len()
        ));
    }
    let format = match input_format.filter(|input_format| !input_format.is_empty()) {
        Some(input_format) if input_format != PARQUET_INPUT_FORMAT => {
            PartitionFormat::Unsupported { input_format }
        }
        _ => PartitionFormat::Parquet,
    };

    Ok(CatalogPartition {
        values: partition_keys
            .iter()
            .cloned()
            .zip(values.into_iter().map(partition_value))
            .collect(),
        location: trim_location(&location).to_string(),
        format,
    })
}

fn partition_value(raw: String) -> Option<String> {
    (raw != HIVE_DEFAULT_PARTITION).then_some(raw)
}

#[cfg(test)]
#[path = "partitions_tests.rs"]
mod tests;
