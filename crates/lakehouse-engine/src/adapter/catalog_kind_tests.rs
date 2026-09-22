use super::*;
use serde_json::json;

/// Scenario: Absent CATALOG_KIND resolves the Iceberg REST catalog kind.
#[test]
fn absent_catalog_kind_resolves_iceberg_rest() {
    let props = json!({});

    let kind = resolve_catalog_kind(&props).expect("absent CATALOG_KIND must resolve, not error");

    assert_eq!(kind, CatalogKind::IcebergRest);
}

/// Scenario: CATALOG_KIND naming Unity Catalog resolves the native Unity
/// Catalog kind, compared case-insensitively.
#[test]
fn unity_catalog_value_resolves_native_kind() {
    for value in ["UNITY_CATALOG", "unity_catalog", "Unity_Catalog"] {
        let props = json!({ "CATALOG_KIND": value });

        let kind = resolve_catalog_kind(&props)
            .unwrap_or_else(|err| panic!("'{value}' must resolve, got error: {err}"));

        assert_eq!(
            kind,
            CatalogKind::UnityCatalogNative,
            "'{value}' must resolve to UnityCatalogNative"
        );
    }
}

/// Scenario: CATALOG_KIND naming direct storage resolves the direct-storage
/// kind, compared case-insensitively.
#[test]
fn direct_storage_value_resolves_direct_storage_kind() {
    for value in ["DIRECT_STORAGE", "direct_storage", "Direct_Storage"] {
        let props = json!({ "CATALOG_KIND": value });

        let kind = resolve_catalog_kind(&props)
            .unwrap_or_else(|err| panic!("'{value}' must resolve, got error: {err}"));

        assert_eq!(
            kind,
            CatalogKind::DirectStorage,
            "'{value}' must resolve to DirectStorage"
        );
    }
}

/// Scenario: An unrecognized CATALOG_KIND value is rejected with a clear error
/// naming the offending value and all three accepted spellings, and never
/// silently falls back to a default.
#[test]
fn unrecognized_catalog_kind_is_rejected() {
    let props = json!({ "CATALOG_KIND": "SNOWFLAKE" });

    let err = resolve_catalog_kind(&props)
        .expect_err("an unrecognized CATALOG_KIND value must be rejected, not defaulted");

    let message = err.to_string();
    assert!(
        message.contains("SNOWFLAKE"),
        "expected the error to name the offending value, got: {message}"
    );
    assert!(
        message.contains(CATALOG_KIND_UNITY_CATALOG),
        "expected the error to name the accepted Unity Catalog value, got: {message}"
    );
    assert!(
        message.contains(CATALOG_KIND_DIRECT_STORAGE),
        "expected the error to name the accepted direct-storage value, got: {message}"
    );
    assert!(
        message.to_lowercase().contains("absent") && message.to_lowercase().contains("iceberg"),
        "expected the error to state that an absent CATALOG_KIND selects Iceberg REST, got: {message}"
    );
}
