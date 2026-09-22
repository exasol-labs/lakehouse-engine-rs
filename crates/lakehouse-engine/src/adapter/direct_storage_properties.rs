/// Parses and validates the three virtual-schema properties a `DIRECT_STORAGE`
/// virtual schema reads, so an operator scopes and tunes a catalog-free
/// virtual schema from `CREATE VIRTUAL SCHEMA` alone: `NAMESPACE` scopes which
/// subtree under the CONNECTION address holds the tables, `MERGE_SCHEMA`
/// selects whether a table's declared schema folds every data file's footer
/// or samples one, and `HIVE_PARTITIONING` is parsed and validated ahead of
/// its implementation (issue #408).
///
/// Read only under `CatalogKind::DirectStorage`; the other two kinds ignore
/// all three properties rather than rejecting them.
use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

const PROP_MERGE_SCHEMA: &str = "MERGE_SCHEMA";
const PROP_HIVE_PARTITIONING: &str = "HIVE_PARTITIONING";

/// The resolved direct-storage properties for one virtual schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectStorageProperties {
    /// The CONNECTION address joined with `NAMESPACE` (when supplied), by
    /// exactly one `/`; the CONNECTION address alone when `NAMESPACE` is
    /// absent.
    pub base_path: String,
    /// Whether a table's declared schema folds every data file's footer
    /// (`true`, the default) or samples exactly one file's footer (`false`).
    pub merge_schema: bool,
    /// Parsed and validated but not yet acted on (issue #408): this plan
    /// declares no partition column and carries no per-file partition value
    /// derived from it, regardless of this value.
    pub hive_partitioning: bool,
}

/// Resolve the three direct-storage VS properties against the given
/// CONNECTION address.
///
/// An unparseable `MERGE_SCHEMA` or `HIVE_PARTITIONING` value is rejected
/// rather than defaulted: a typo that silently selected the opposite mode
/// would return a narrower schema instead of an error. A `NAMESPACE` value
/// carrying a URI scheme or a leading `/` is rejected rather than repaired,
/// because both spellings express an intent the property cannot carry.
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

/// Join a direct-storage base path and an optional path segment with exactly
/// one `/` — the ONE owner of that join for this kind.
///
/// Both the `NAMESPACE` scoping at property-resolution time and the table-root
/// composition at pushdown time ask the same question, so both ask it here: two
/// formulas over one base path could answer `…/direct//events` and
/// `…/direct/events` for the same table and list its files from two different
/// prefixes. Every trailing `/` on `base` is stripped before the separator is
/// appended, so a CONNECTION address ending in any number of separators joins
/// to exactly one. An absent segment returns `base` unchanged.
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
