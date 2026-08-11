use exasol_udf_sdk::error::UdfError;
use serde_json::Value as Json;

// VS property selecting which catalog backend a virtual schema resolves
// against. Read from the request's plain VS properties, never from the
// CONNECTION password JSON. Absent defaults to Iceberg REST, so every
// pre-existing virtual schema keeps its current behavior unchanged.
const PROP_CATALOG_KIND: &str = "CATALOG_KIND";

const CATALOG_KIND_UNITY_CATALOG: &str = "UNITY_CATALOG";

/// Which catalog backend a virtual schema resolves against.
///
/// The variant IS the catalog kind: `resolve_catalog_kind` is the only site
/// that derives it from a VS property, and a single downstream construction
/// site matches it exhaustively to build the matching `CatalogClient`. Every
/// listing operation after that runs one shared pipeline and never re-matches
/// the kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    IcebergRest,
    UnityCatalogNative,
}

/// Resolve the `CATALOG_KIND` VS property.
///
/// Absent resolves `IcebergRest`. A value naming the Unity Catalog kind
/// resolves `UnityCatalogNative`, compared case-insensitively. Any other
/// value is rejected rather than silently defaulted — defaulting an
/// unrecognized kind would resolve a misconfigured virtual schema against
/// the wrong catalog.
pub fn resolve_catalog_kind(props: &Json) -> Result<CatalogKind, UdfError> {
    match super::nonempty_str(props, PROP_CATALOG_KIND) {
        None => Ok(CatalogKind::IcebergRest),
        Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_UNITY_CATALOG) => {
            Ok(CatalogKind::UnityCatalogNative)
        }
        Some(value) => Err(UdfError::User(format!(
            "unrecognized '{PROP_CATALOG_KIND}' value '{value}'; leave it absent for Iceberg REST (the default) or set it to '{CATALOG_KIND_UNITY_CATALOG}'"
        ))),
    }
}

#[cfg(test)]
#[path = "catalog_kind_tests.rs"]
mod tests;
