//! Delta reader-protocol gate: refuses a table whose reader protocol version or
//! reader-feature set this engine does not implement, before any log replay.

use delta_kernel::table_features::{MAX_VALID_READER_VERSION, MIN_VALID_RW_VERSION, TableFeature};
use exasol_udf_sdk::error::UdfError;

/// Refuses a Delta table whose reader protocol this engine does not implement,
/// so no log replay, checkpoint read, or object-store listing happens beyond
/// opening the snapshot itself.
pub(crate) fn ensure_readable(
    min_reader_version: i32,
    reader_features: Option<&[TableFeature]>,
) -> Result<(), UdfError> {
    if !(MIN_VALID_RW_VERSION..=MAX_VALID_READER_VERSION).contains(&min_reader_version) {
        return Err(UdfError::User(format!(
            "Delta table declares min_reader_version {min_reader_version}, outside the range this engine reads ({MIN_VALID_RW_VERSION}..={MAX_VALID_READER_VERSION})"
        )));
    }
    let Some(features) = reader_features else {
        return Ok(());
    };
    let mut refused: Vec<String> = features
        .iter()
        .filter(|f| !is_allow_listed(f))
        .map(describe_refused_feature)
        .collect();
    if refused.is_empty() {
        return Ok(());
    }
    refused.sort();
    Err(UdfError::User(format!(
        "Delta table declares reader feature(s) this engine does not implement: {}",
        refused.join(", ")
    )))
}

fn describe_refused_feature(feature: &TableFeature) -> String {
    match feature {
        TableFeature::TypeWidening | TableFeature::TypeWideningPreview => {
            format!("{feature} (tracked as issue #349)")
        }
        other => other.to_string(),
    }
}

fn is_allow_listed(feature: &TableFeature) -> bool {
    matches!(
        feature,
        TableFeature::ColumnMapping
            | TableFeature::DeletionVectors
            | TableFeature::TimestampWithoutTimezone
            | TableFeature::V2Checkpoint
            | TableFeature::VacuumProtocolCheck
    )
}

#[cfg(test)]
#[path = "delta_protocol_tests.rs"]
mod tests;
