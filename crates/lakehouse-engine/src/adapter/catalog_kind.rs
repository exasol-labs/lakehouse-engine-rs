use exasol_udf_sdk::error::UdfError;

use super::properties::PROP_CATALOG_KIND;

const CATALOG_KIND_UNITY_CATALOG: &str = "UNITY_CATALOG";

const CATALOG_KIND_DIRECT_STORAGE: &str = "DIRECT_STORAGE";

const CATALOG_KIND_GLUE: &str = "GLUE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    IcebergRest,
    UnityCatalogNative,
    DirectStorage,
    Glue,
}

impl CatalogKind {
    /// Absent means Iceberg REST; an unrecognized value is an error, never defaulted.
    pub(super) fn parse(value: Option<&str>) -> Result<Self, UdfError> {
        match value {
            None => Ok(Self::IcebergRest),
            Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_UNITY_CATALOG) => {
                Ok(Self::UnityCatalogNative)
            }
            Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_DIRECT_STORAGE) => {
                Ok(Self::DirectStorage)
            }
            Some(value) if value.eq_ignore_ascii_case(CATALOG_KIND_GLUE) => Ok(Self::Glue),
            Some(value) => Err(UdfError::User(format!(
                "unrecognized '{PROP_CATALOG_KIND}' value '{value}'; leave it absent for Iceberg REST (the default), or set it to '{CATALOG_KIND_UNITY_CATALOG}', '{CATALOG_KIND_DIRECT_STORAGE}', or '{CATALOG_KIND_GLUE}'"
            ))),
        }
    }
}

#[cfg(test)]
#[path = "catalog_kind_tests.rs"]
mod tests;
