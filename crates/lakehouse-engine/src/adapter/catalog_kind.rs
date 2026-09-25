use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

// Read from plain VS properties, never the CONNECTION password JSON. Absent means Iceberg REST.
const PROP_CATALOG_KIND: &str = "CATALOG_KIND";

const CATALOG_KIND_UNITY_CATALOG: &str = "UNITY_CATALOG";

const CATALOG_KIND_DIRECT_STORAGE: &str = "DIRECT_STORAGE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    IcebergRest,
    UnityCatalogNative,
    DirectStorage,
}

/// Absent means Iceberg REST; an unrecognized value is an error, never defaulted.
pub fn resolve_catalog_kind(props: &Json) -> Result<CatalogKind, UdfError> {
    match super::nonempty_str(props, PROP_CATALOG_KIND) {
        None => Ok(CatalogKind::IcebergRest),
        Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_UNITY_CATALOG) => {
            Ok(CatalogKind::UnityCatalogNative)
        }
        Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_DIRECT_STORAGE) => {
            Ok(CatalogKind::DirectStorage)
        }
        Some(value) => Err(UdfError::User(format!(
            "unrecognized '{PROP_CATALOG_KIND}' value '{value}'; leave it absent for Iceberg REST (the default), or set it to '{CATALOG_KIND_UNITY_CATALOG}' or '{CATALOG_KIND_DIRECT_STORAGE}'"
        ))),
    }
}

#[cfg(test)]
#[path = "catalog_kind_tests.rs"]
mod tests;
