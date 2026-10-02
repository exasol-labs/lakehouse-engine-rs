use aws_sdk_glue::types::Partition;

use crate::{CatalogPartition, HIVE_DEFAULT_PARTITION, PartitionFormat};

use super::routing::PARQUET_INPUT_FORMAT;
use super::trim_location;

/// Glue `Values` are positional against the table's partition keys. A partition declaring
/// no input format inherits the table's, which routing admitted as the Parquet one.
pub(super) fn neutral_partition(
    table: &str,
    partition_keys: &[String],
    partition: &Partition,
) -> Result<CatalogPartition, String> {
    let values = partition.values();
    let descriptor = partition.storage_descriptor();
    let Some(location) = descriptor
        .and_then(|descriptor| descriptor.location())
        .filter(|location| !location.is_empty())
    else {
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
    let format = match descriptor.and_then(|descriptor| descriptor.input_format()) {
        Some(input_format) if !input_format.is_empty() && input_format != PARQUET_INPUT_FORMAT => {
            PartitionFormat::Unsupported {
                input_format: input_format.to_string(),
            }
        }
        _ => PartitionFormat::Parquet,
    };

    Ok(CatalogPartition {
        values: partition_keys
            .iter()
            .cloned()
            .zip(
                values
                    .iter()
                    .map(|raw| (raw != HIVE_DEFAULT_PARTITION).then(|| raw.clone())),
            )
            .collect(),
        location: trim_location(location).to_string(),
        format,
    })
}
