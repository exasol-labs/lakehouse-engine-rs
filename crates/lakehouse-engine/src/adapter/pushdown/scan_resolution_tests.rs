use super::super::binary_filter::BinaryLayout;
use super::super::test_support::{
    GlueEndpoint, ICEBERG_CONFIG_TARGET, ICEBERG_LOAD_TABLE_TARGET, RecordingCatalog,
    UNITY_TABLE_TARGET, UNREACHABLE_CATALOG, closed_port_storage, delta_notes, iceberg_catalog,
    involved_notes, locationless_delta_table_body, sample_storage, unauthenticated_creds,
};
use super::*;
use crate::adapter::connection::ConnectionCreds;
use crate::adapter::direct_storage_properties::join_storage_path;
use crate::adapter::permission::PermissionCheck;
use crate::scan::spec::{StorageBackend, StorageProps};
use crate::types::exasol_type::ExasolType;

/// Scenario: A Glue table resolves from its recorded identifier and fails loud when it is no longer plannable
#[tokio::test]
async fn glue_table_identity_round_trips_through_the_recorded_identifier() {
    let glue = GlueEndpoint::spawn(|_| {
        (
            400,
            serde_json::json!({"__type": "EntityNotFoundException", "Message": "gone"}),
        )
    })
    .await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();
    let resolver = TableScanResolver::for_request(
        CatalogKind::Glue,
        &glue.address,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["sales.orders"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a Glue session is built without contacting the catalog");
    assert!(glue.operations().is_empty());

    let err = resolver
        .resolve("sales.orders", None, &InvolvedColumns::default())
        .await
        .expect_err("a dropped Glue table cannot be planned");

    assert_eq!(glue.operations(), ["GetTable"], "exactly one GetTable call");
    let request = &glue.bodies_of("GetTable")[0];
    assert_eq!(request["DatabaseName"], "sales");
    assert_eq!(request["Name"], "orders");
    assert!(
        err.to_string().contains("sales.orders"),
        "the error must name the table the recorded identifier recovered: {err}"
    );
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

/// The refusal `for_request` returns for `identifiers` under `kind` over an unreachable catalog, so
/// a check that ran after a catalog request would surface as that request's transport error.
async fn refusal_before_any_catalog_request(
    kind: CatalogKind,
    creds: &ConnectionCreds,
    identifiers: &[&str],
) -> String {
    let storage = sample_storage();
    TableScanResolver::for_request(
        kind,
        UNREACHABLE_CATALOG,
        ConnectionStorage {
            storage: &storage,
            creds,
            allow_http: true,
        },
        identifiers,
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .err()
    .expect("the identifier must be refused")
    .to_string()
}

#[tokio::test]
async fn a_recorded_identifier_without_a_table_name_is_refused_before_any_catalog_request() {
    for unresolvable in ["cat.sch.", "", "   "] {
        let message = refusal_before_any_catalog_request(
            CatalogKind::UnityCatalogNative,
            &unauthenticated_creds(),
            &[unresolvable],
        )
        .await;
        assert!(
            message.contains(&format!("'{unresolvable}'")),
            "the refusal must name the identifier it could not resolve: {message}"
        );
    }
}

#[tokio::test]
async fn a_malformed_unity_identifier_is_refused_in_unity_terms() {
    let message = refusal_before_any_catalog_request(
        CatalogKind::UnityCatalogNative,
        &unauthenticated_creds(),
        &["orders"],
    )
    .await;

    assert!(
        message.contains("catalog.schema.table"),
        "the refusal must state the Unity Catalog address form: {message}"
    );
    assert!(
        !message.contains("namespace.table"),
        "Iceberg's identifier rule must not judge a Unity Catalog identifier: {message}"
    );
}

#[tokio::test]
async fn a_malformed_identifier_anywhere_in_the_request_is_refused_before_any_catalog_request() {
    // OAuth2 credentials, so a session built before the parse fails on its token request instead.
    let creds = ConnectionCreds {
        client_id: Some("oauth-client-id-sentinel".into()),
        client_secret: Some("oauth-client-secret-sentinel".into()),
        ..unauthenticated_creds()
    };

    let message = refusal_before_any_catalog_request(
        CatalogKind::IcebergRest,
        &creds,
        &["db.t", "malformed"],
    )
    .await;

    assert!(
        message.contains("'malformed'") && message.contains("namespace.table"),
        "the refusal must be the parse error naming the identifier it could not parse: {message}"
    );
    assert!(
        !message.contains("OAuth2"),
        "the identifier must be validated before the session's token request: {message}"
    );
}

/// Scenario: One catalog session per request serves every table the request resolves
/// Scenario: Single-table pushdown builds one catalog session and reuses it
/// Scenario: N-table join reuses one session across all legs
#[tokio::test]
async fn one_catalog_session_serves_every_table_the_resolver_resolves() {
    let catalog = iceberg_catalog().await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();
    let resolver = TableScanResolver::for_request(
        CatalogKind::IcebergRest,
        &catalog.uri,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["db.t", "db.u"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    resolver
        .resolve("db.t", None, &InvolvedColumns::default())
        .await
        .expect("first table");
    resolver
        .resolve("db.u", None, &InvolvedColumns::default())
        .await
        .expect("second table");

    assert_eq!(
        catalog.targets(),
        vec![
            ICEBERG_CONFIG_TARGET.to_string(),
            ICEBERG_LOAD_TABLE_TARGET.to_string(),
            "/v1/namespaces/db/tables/u?snapshots=refs".to_string()
        ],
        "the second table must be loaded on the session the first one used"
    );
}

/// Scenario: One catalog session per request serves every table the request resolves
#[tokio::test]
async fn one_unity_catalog_session_serves_every_table_the_resolver_resolves() {
    const SECOND_TABLE_TARGET: &str = "/api/2.1/unity-catalog/tables/cat.sch.customers";
    let catalog = RecordingCatalog::spawn(|target| {
        if target == UNITY_TABLE_TARGET || target == SECOND_TABLE_TARGET {
            (200, locationless_delta_table_body())
        } else {
            (404, r#"{"message":"no such table"}"#.to_string())
        }
    })
    .await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();
    let resolver = TableScanResolver::for_request(
        CatalogKind::UnityCatalogNative,
        &catalog.uri,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["cat.sch.orders", "cat.sch.customers"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err1 = resolver
        .resolve("cat.sch.orders", None, &InvolvedColumns::default())
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");
    let err2 = resolver
        .resolve("cat.sch.customers", None, &InvolvedColumns::default())
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");
    assert!(err1.to_string().contains("cat.sch.orders"));
    assert!(err2.to_string().contains("cat.sch.customers"));

    assert_eq!(
        catalog.targets(),
        vec![
            UNITY_TABLE_TARGET.to_string(),
            SECOND_TABLE_TARGET.to_string()
        ],
        "the second table must be loaded on the SAME session the first one used, \
         with no repeated auth target"
    );
}

const EMPTY_LIST_BUCKET_RESULT: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    "<Name>warehouse</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>",
    "<IsTruncated>false</IsTruncated></ListBucketResult>",
);

const DIRECT_STORAGE_ADDRESS: &str = "s3://warehouse";

async fn empty_s3_endpoint() -> RecordingCatalog {
    RecordingCatalog::spawn(|_| (200, EMPTY_LIST_BUCKET_RESULT.to_string())).await
}

fn direct_storage_backend(endpoint: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.to_string(),
        region: "us-east-1".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        allow_http: true,
        path_style: true,
        ..Default::default()
    })
}

#[tokio::test]
async fn one_session_or_store_per_request_serves_every_leg() {
    let endpoint = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = direct_storage_backend(&endpoint.uri);
    let props = serde_json::json!({"NAMESPACE": "direct"});
    let resolver = TableScanResolver::for_request(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["events", "event_labels"],
        &props,
        &PermissionCheck::Off,
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");

    assert!(
        endpoint.targets().is_empty(),
        "building the request's one store must cost no object-store request: {:?}",
        endpoint.targets()
    );

    resolver
        .resolve("events", None, &InvolvedColumns::default())
        .await
        .expect("a directory with no data file resolves an empty scan");
    resolver
        .resolve("event_labels", None, &InvolvedColumns::default())
        .await
        .expect("a directory with no data file resolves an empty scan");

    let targets = endpoint.targets();
    assert_eq!(
        targets.len(),
        2,
        "each leg costs exactly one listing and no session setup of its own: {targets:?}"
    );
    assert!(
        targets[0].contains("direct%2Fevents%2F") || targets[0].contains("direct/events/"),
        "the first leg lists its own table root under the composed base path: {targets:?}"
    );
    assert!(
        targets[1].contains("direct%2Fevent_labels%2F")
            || targets[1].contains("direct/event_labels/"),
        "the second leg lists its own table root, not the first leg's: {targets:?}"
    );
}

#[tokio::test]
async fn a_direct_storage_identifier_naming_no_first_level_directory_is_refused() {
    let creds = unauthenticated_creds();
    // An object-store request before the refusal would fail on the closed port instead.
    let storage = closed_port_storage();

    for unresolvable in ["", "   ", "sub/dir", "sub\\dir", "..", "."] {
        let err = TableScanResolver::for_request(
            CatalogKind::DirectStorage,
            DIRECT_STORAGE_ADDRESS,
            ConnectionStorage {
                storage: &storage,
                creds: &creds,
                allow_http: true,
            },
            &["events", unresolvable],
            &Json::Null,
            &PermissionCheck::Off,
        )
        .await
        .err()
        .expect("an identifier naming no first-level directory must be refused");
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

#[tokio::test]
async fn the_pushdown_table_root_equals_the_discovery_composed_storage_location() {
    let endpoint = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = direct_storage_backend(&endpoint.uri);
    let address = "s3://warehouse/direct//";

    let resolver = TableScanResolver::for_request(
        CatalogKind::DirectStorage,
        address,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["events"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");

    let (scan, _) = resolver
        .resolve("events", None, &InvolvedColumns::default())
        .await
        .expect("a directory with no data file resolves an empty scan");

    assert_eq!(
        scan.table_root,
        join_storage_path(address, Some("events")),
        "the pushdown table root must be the join the enumeration path composes \
         `storage_location` with"
    );
    assert_eq!(
        scan.table_root, "s3://warehouse/direct/events",
        "repeated trailing separators on the CONNECTION address collapse to exactly one"
    );
}

#[tokio::test]
async fn request_session_has_one_variant_per_kind() {
    let iceberg = iceberg_catalog().await;
    let endpoint = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();
    let direct_storage = direct_storage_backend(&endpoint.uri);

    for (kind, uri, backend, identifier) in [
        (
            CatalogKind::IcebergRest,
            iceberg.uri.as_str(),
            &storage,
            "db.t",
        ),
        (
            CatalogKind::UnityCatalogNative,
            iceberg.uri.as_str(),
            &storage,
            "cat.sch.orders",
        ),
        (
            CatalogKind::Glue,
            iceberg.uri.as_str(),
            &storage,
            "sales.orders",
        ),
        (
            CatalogKind::DirectStorage,
            DIRECT_STORAGE_ADDRESS,
            &direct_storage,
            "events",
        ),
    ] {
        TableScanResolver::for_request(
            kind,
            uri,
            ConnectionStorage {
                storage: backend,
                creds: &creds,
                allow_http: true,
            },
            &[identifier],
            &Json::Null,
            &PermissionCheck::Off,
        )
        .await
        .unwrap_or_else(|e| panic!("{kind:?} must resolve a session of its own: {e}"));
    }
}

#[test]
fn an_enforced_resolver_admits_only_the_tables_the_check_covered() {
    let admission = TableAdmission::Authorized(HashSet::from(["db.t".to_string()]));

    assert!(admission.admit("db.t").is_ok());
    assert!(admission.admit("db.other").is_err());
    assert!(TableAdmission::Unchecked.admit("db.other").is_ok());
}

#[tokio::test]
async fn the_resolver_returns_the_map_the_request_types_and_the_column_notes_build() {
    let endpoint = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = direct_storage_backend(&endpoint.uri);
    let resolver = TableScanResolver::for_request(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["events"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");
    let notes = delta_notes(&[("payload", "binary"), ("id", "long")], &[]);
    let number = ExasolType::decimal(20, 0);
    let involved = InvolvedColumns {
        types: vec![
            ("PAYLOAD".to_string(), ExasolType::WIDEST_VARCHAR),
            ("ID".to_string(), number),
        ],
        notes: involved_notes("EVENTS", &notes),
    };

    let (_, column_types) = resolver
        .resolve("events", None, &involved)
        .await
        .expect("a directory with no data file resolves an empty scan");

    let layouts: Vec<_> = column_types
        .iter()
        .map(|(column, column_type)| {
            (
                column.name.as_str(),
                column_type.exa_type,
                column_type.binary,
            )
        })
        .collect();
    assert_eq!(
        layouts,
        vec![
            (
                "PAYLOAD",
                ExasolType::WIDEST_VARCHAR,
                Some(BinaryLayout::Variable)
            ),
            ("ID", number, None),
        ],
        "the request's Exasol types, with the byte layout only the column notes carry"
    );
}
