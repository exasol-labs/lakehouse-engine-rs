//! Parses and validates the `DIRECT_STORAGE` virtual-schema properties: `NAMESPACE`,
//! `MERGE_SCHEMA`, `HIVE_PARTITIONING`. Read only under `CatalogKind::DirectStorage`.
use crate::adapter::parquet_directory::{DirectoryOptions, MergeMode};
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

const PROP_MERGE_SCHEMA: &str = "MERGE_SCHEMA";
const PROP_HIVE_PARTITIONING: &str = "HIVE_PARTITIONING";

/// The resolved direct-storage properties for one virtual schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectStorageProperties {
    /// CONNECTION address joined with `NAMESPACE` (exactly one `/`), or the CONNECTION address
    /// alone when `NAMESPACE` is absent.
    pub base_path: String,
    /// Fold every data file's footer (`true`, default) vs. sample one file's footer (`false`).
    pub merge_schema: bool,
    /// `TRUE` (default) declares `key=value` directory segments as partition columns; `FALSE`
    /// reads them as plain directories, per `vs-adapter/direct-storage-hive-partitioning`.
    pub hive_partitioning: bool,
}

impl DirectStorageProperties {
    /// The ONE derivation site for the seam's two layout switches, so `createVirtualSchema` and
    /// pushdown resolve the identical [`DirectoryOptions`] and can never disagree.
    pub fn directory_options(&self) -> DirectoryOptions {
        DirectoryOptions {
            merge_mode: MergeMode::for_merge_schema(self.merge_schema),
            hive_partitioning: self.hive_partitioning,
        }
    }
}

/// An unparseable `MERGE_SCHEMA`/`HIVE_PARTITIONING` value is rejected rather than defaulted, and
/// a `NAMESPACE` with a URI scheme or leading `/` is rejected rather than repaired.
pub fn resolve_direct_storage_properties(
    props: &Json,
    connection_address: &str,
) -> Result<DirectStorageProperties, UdfError> {
    let namespace = parse_namespace(props)?;
    let base_path = join_storage_path(connection_address, namespace.as_deref());
    let merge_schema = parse_bool_property(props, PROP_MERGE_SCHEMA, true)?;
    let hive_partitioning = parse_bool_property(props, PROP_HIVE_PARTITIONING, true)?;
    Ok(DirectStorageProperties {
        base_path,
        merge_schema,
        hive_partitioning,
    })
}

/// The ONE owner of the base-path join for this kind, so `NAMESPACE` scoping and table-root
/// composition can never disagree on the separator and list two different prefixes for one table.
pub fn join_storage_path(base: &str, segment: Option<&str>) -> String {
    match segment {
        None => base.to_string(),
        Some(segment) => format!("{}/{segment}", base.trim_end_matches('/')),
    }
}

fn parse_namespace(props: &Json) -> Result<Option<String>, UdfError> {
    let Some(value) = super::nonempty_str(props, super::PROP_NAMESPACE) else {
        return Ok(None);
    };
    if value.contains("://") {
        return Err(UdfError::User(format!(
            "'{}' value '{value}' carries a URI scheme; it must be a path relative to the \
             CONNECTION address",
            super::PROP_NAMESPACE
        )));
    }
    if value.starts_with('/') {
        return Err(UdfError::User(format!(
            "'{}' value '{value}' begins with '/'; it must be a path relative to the \
             CONNECTION address",
            super::PROP_NAMESPACE
        )));
    }
    Ok(Some(value.to_string()))
}

fn parse_bool_property(props: &Json, key: &str, default: bool) -> Result<bool, UdfError> {
    match super::nonempty_str(props, key) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => Err(UdfError::User(format!(
            "'{key}' value '{value}' is neither a true nor a false spelling; accepted \
             spellings are 'TRUE' and 'FALSE' (case-insensitive)"
        ))),
    }
}

#[cfg(test)]
#[path = "direct_storage_properties_tests.rs"]
mod tests;
