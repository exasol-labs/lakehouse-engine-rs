use super::super::test_support::{
    UNREACHABLE_CATALOG, catalog_table, sample_storage, sigv4_creds, user_message,
};
use super::*;
use lakehouse_catalog::CatalogTableIdent;

const UNITY_TABLE_NAME: &str = "cat.sch.orders";

fn unity_table(format: TableFormat) -> CatalogTable {
    CatalogTable {
        ident: CatalogTableIdent {
            namespace: vec!["cat".into(), "sch".into()],
            name: "orders".into(),
        },
        vended_credential_key: Some("table-id-1".into()),
        ..catalog_table(format, "s3://bucket/cat/sch/orders")
    }
}

#[tokio::test]
async fn a_unity_catalog_load_refuses_an_iceberg_table() {
    let creds = sigv4_creds();
    let session = UnityCatalogSession::new(UNREACHABLE_CATALOG, creds.clone());
    let table = unity_table(TableFormat::Iceberg);
    let storage = sample_storage();

    let err = open_unity_table(
        &session,
        &table,
        &ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
    )
    .await
    .err()
    .expect("a Unity Catalog table reporting a non-Delta format must be refused");

    let message = user_message(err);
    assert!(
        message.contains(UNITY_TABLE_NAME),
        "the refusal must name the table it refused: {message}"
    );
    assert!(
        message.contains("Iceberg"),
        "the refusal must name the format the catalog reported: {message}"
    );
}

#[test]
fn a_direct_storage_load_composes_its_root_from_the_identifier() {
    for (root, directory, expected) in [
        (
            "s3://warehouse/direct",
            "events",
            "s3://warehouse/direct/events",
        ),
        (
            "s3://warehouse/lake/direct",
            "event_labels",
            "s3://warehouse/lake/direct/event_labels",
        ),
    ] {
        let root = ObjectLocation::from_raw(root).expect("a CONNECTION address names a root");

        let table_root = direct_storage_table_root(&root, directory)
            .expect("a first-level directory composes a table root");

        assert_eq!(table_root.to_string(), expected, "{root} + {directory}");
    }
}

#[test]
fn a_recorded_identifier_recovers_its_namespace_segments_and_table_name() {
    let three_level = unity_table_ident("cat.sch.orders").expect("a three-level identifier");
    assert_eq!(
        three_level.namespace,
        vec!["cat".to_string(), "sch".to_string()]
    );
    assert_eq!(three_level.name, "orders");

    let bare = unity_table_ident("orders")
        .expect_err("a separator-free identifier addresses no Unity Catalog table");
    assert!(
        bare.to_string().contains("'orders'"),
        "the refusal must name the identifier it rejected: {bare}"
    );
    assert!(
        bare.to_string().contains("catalog.schema.table"),
        "the refusal must state the Unity Catalog address form: {bare}"
    );
}

#[test]
fn a_recorded_identifier_without_a_table_name_is_refused() {
    for unresolvable in ["cat.sch.", "", "   "] {
        let err = unity_table_ident(unresolvable)
            .expect_err("an identifier naming no table must be refused");
        assert!(
            err.to_string().contains(&format!("'{unresolvable}'")),
            "the refusal must name the identifier it could not resolve: {err}"
        );
    }
}

#[test]
fn a_direct_storage_identifier_naming_no_first_level_directory_is_refused() {
    for unresolvable in ["", "   ", "sub/dir", "sub\\dir", "..", "."] {
        let err = direct_storage_directory(unresolvable)
            .expect_err("an identifier naming no first-level directory must be refused");
        assert!(
            err.to_string().contains(&format!("'{unresolvable}'")),
            "the refusal must name the identifier it rejected: {err}"
        );
        assert!(
            err.to_string().contains("recreate the virtual schema"),
            "the refusal must tell the operator how to repair it: {err}"
        );
    }
}
