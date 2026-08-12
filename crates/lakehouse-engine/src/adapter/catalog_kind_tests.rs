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

/// Scenario: An unrecognized CATALOG_KIND value is rejected with a clear
/// error naming the offending value and the accepted set, and never
/// silently falls back to a default.
#[test]
fn unrecognized_catalog_kind_is_rejected() {
    let props = json!({ "CATALOG_KIND": "SNOWFLAKE" });

    let err = resolve_catalog_kind(&props)
        .expect_err("an unrecognized CATALOG_KIND value must be rejected, not defaulted");

    assert!(
        err.to_string().contains("SNOWFLAKE"),
        "expected the error to name the offending value, got: {err}"
    );
    assert!(
        err.to_string().contains(CATALOG_KIND_UNITY_CATALOG),
        "expected the error to name the accepted Unity Catalog value, got: {err}"
    );
}
