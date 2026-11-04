use super::super::test_support::{
    RecordingCatalog, UNREACHABLE_CATALOG, catalog_table, endpoint_storage, glue_catalog_table,
    locationless_delta_table_body, resolved_connection, sample_storage, sigv4_creds,
    unauthenticated_creds, user_message,
};
use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::construct_catalog_client;
use lakehouse_catalog::CatalogTableIdent;
use serde_json::Value as Json;

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

fn glue_table(format: TableFormat) -> CatalogTable {
    CatalogTable {
        metadata_location: Some("s3://bucket/sales/orders/metadata/v1.json".into()),
        ..glue_catalog_table(
            "sales",
            "orders",
            format,
            "s3://bucket/sales/orders",
            &[("id", "int")],
        )
    }
}

fn offline_glue_session() -> GlueCatalogSession {
    GlueCatalogSession::new(UNREACHABLE_CATALOG, sample_storage(), sigv4_creds())
        .expect("the CONNECTION region signs the session without a request")
}

/// Scenario: A Unity Catalog or Glue table is planned by the format its catalog reports
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

/// Scenario: A Unity Catalog or Glue table is planned by the format its catalog reports
#[tokio::test]
async fn a_glue_load_refuses_a_delta_table() {
    let session = offline_glue_session();
    let table = glue_table(TableFormat::Delta);

    let err = open_glue_table(&session, &table, &sample_storage())
        .await
        .err()
        .expect("no Glue reader plans a Delta table");

    let message = user_message(err);
    assert!(message.contains("sales.orders"), "{message}");
    assert!(message.contains("Delta"), "{message}");
}

/// Scenario: A direct-storage pushdown loads its table from the directory its identifier note names
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

#[tokio::test]
async fn a_recorded_identifier_without_a_table_name_is_refused_before_any_catalog_request() {
    let catalog = RecordingCatalog::spawn(|_| (200, locationless_delta_table_body())).await;
    let session = UnityCatalogSession::new(&catalog.uri, unauthenticated_creds());

    for unresolvable in ["cat.sch.", "", "   "] {
        let err = session
            .check_identifier(unresolvable)
            .expect_err("an identifier naming no table must be refused");
        assert!(
            err.to_string().contains(&format!("'{unresolvable}'")),
            "the refusal must name the identifier it could not resolve: {err}"
        );
    }

    assert!(
        catalog.targets().is_empty(),
        "an identifier naming no table must cost no catalog request: {:?}",
        catalog.targets()
    );
}

#[tokio::test]
async fn a_direct_storage_identifier_naming_no_first_level_directory_is_refused() {
    let endpoint = RecordingCatalog::spawn(|_| (200, String::new())).await;
    let connection = resolved_connection(
        CatalogKind::DirectStorage,
        "s3://warehouse",
        endpoint_storage(&endpoint.uri),
        unauthenticated_creds(),
    );
    let client = construct_catalog_client(&connection, &Json::Null)
        .expect("a direct-storage client is built from the CONNECTION alone");

    for unresolvable in ["", "   ", "sub/dir", "sub\\dir", "..", "."] {
        let err = client
            .check_identifier(unresolvable)
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

    assert!(
        endpoint.targets().is_empty(),
        "a refused identifier must cost no object-store request: {:?}",
        endpoint.targets()
    );
}
