use crate::adapter::parquet_directory::{DirectoryOptions, MergeMode};
use crate::adapter::properties::{
    PROP_HIVE_PARTITIONING, PROP_MERGE_SCHEMA, PROP_NAMESPACE, VsProperties, parse_boolean,
};
use exasol_udf_sdk::error::UdfError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectStorageProperties {
    /// CONNECTION address joined with `NAMESPACE` (exactly one `/`), or the CONNECTION address
    /// alone when `NAMESPACE` is absent.
    pub base_path: String,
    /// Fold every data file's footer (`true`, default) vs. sample one file's footer (`false`).
    pub merge_schema: bool,
    /// Whether `key=value` directory segments declare partition columns (default `TRUE`).
    pub hive_partitioning: bool,
}

impl DirectStorageProperties {
    /// The only place [`DirectoryOptions`] is derived, so enumeration and pushdown can't disagree.
    pub fn directory_options(&self) -> DirectoryOptions {
        DirectoryOptions {
            merge_mode: if self.merge_schema {
                MergeMode::FoldEveryFile
            } else {
                MergeMode::SampleOneFile
            },
            hive_partitioning: self.hive_partitioning,
        }
    }
}

/// An unparseable `MERGE_SCHEMA`/`HIVE_PARTITIONING` value is rejected rather than defaulted, and
/// a `NAMESPACE` with a URI scheme or leading `/` is rejected rather than repaired.
pub(super) fn resolve_direct_storage_properties(
    props: &VsProperties,
    connection_address: &str,
) -> Result<DirectStorageProperties, UdfError> {
    let namespace = parse_namespace(props.namespace.as_deref())?;
    let base_path = join_storage_path(connection_address, namespace);
    let merge_schema = parse_bool_property(props.merge_schema.as_deref(), PROP_MERGE_SCHEMA)?;
    let hive_partitioning =
        parse_bool_property(props.hive_partitioning.as_deref(), PROP_HIVE_PARTITIONING)?;
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

fn parse_namespace(value: Option<&str>) -> Result<Option<&str>, UdfError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.contains("://") {
        return Err(UdfError::User(format!(
            "'{}' value '{value}' carries a URI scheme; it must be a path relative to the \
             CONNECTION address",
            PROP_NAMESPACE
        )));
    }
    if value.starts_with('/') {
        return Err(UdfError::User(format!(
            "'{}' value '{value}' begins with '/'; it must be a path relative to the \
             CONNECTION address",
            PROP_NAMESPACE
        )));
    }
    Ok(Some(value))
}

/// An absent value is `TRUE`.
fn parse_bool_property(value: Option<&str>, key: &str) -> Result<bool, UdfError> {
    let Some(value) = value else {
        return Ok(true);
    };
    parse_boolean(value).ok_or_else(|| {
        UdfError::User(format!(
            "'{key}' value '{value}' is neither a true nor a false spelling; accepted \
             spellings are 'TRUE' and 'FALSE' (case-insensitive)"
        ))
    })
}

#[cfg(test)]
#[path = "direct_storage_properties_tests.rs"]
mod tests;
