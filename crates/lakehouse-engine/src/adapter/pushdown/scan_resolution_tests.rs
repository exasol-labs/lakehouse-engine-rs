use super::super::test_support::filter_json::equal;
use super::super::test_support::{
    GlueEndpoint, ICEBERG_CONFIG_TARGET, ICEBERG_LOAD_TABLE_TARGET, RecordingCatalog,
    UNITY_TABLE_TARGET, iceberg_catalog, locationless_delta_table_body, nullable_field,
    object_endpoint, sample_storage, unauthenticated_creds, unity_delta_catalog, user_message,
};
use super::super::test_support::{
    delta_commit_zero_key, delta_notes, delta_object_endpoint, fileless_delta_commit,
    involved_notes,
};
use super::*;
use crate::adapter::column_notes::ColumnDeclaration;
use crate::adapter::permission::PermissionCheck;
use crate::scan::spec::{LogicalField, StorageBackend, StorageProps};

/// Scenario: a Unity Catalog table's identity survives the round trip from the involved table
#[tokio::test]
async fn unity_table_identity_round_trips_through_the_recorded_identifier() {
    let catalog = RecordingCatalog::spawn(|target| {
        if target == UNITY_TABLE_TARGET {
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
        &["cat.sch.orders"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err = resolver
        .resolve("cat.sch.orders", None, &TableNotes::default())
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");

    assert_eq!(
        catalog.targets(),
        vec![UNITY_TABLE_TARGET.to_string()],
        "the loader must address exactly the recorded dotted identifier, once"
    );
    assert!(
        err.to_string().contains("cat.sch.orders"),
        "the reader must name the table the recorded identifier recovered: {err}"
    );
}

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
        .resolve("sales.orders", None, &TableNotes::default())
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

#[tokio::test]
async fn a_recorded_identifier_without_a_table_name_is_refused_before_any_catalog_request() {
    let catalog = RecordingCatalog::spawn(|_| (200, locationless_delta_table_body())).await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();

    for unresolvable in ["cat.sch.", "", "   "] {
        let err = TableScanResolver::for_request(
            CatalogKind::UnityCatalogNative,
            &catalog.uri,
            ConnectionStorage {
                storage: &storage,
                creds: &creds,
                allow_http: true,
            },
            &[unresolvable],
            &Json::Null,
            &PermissionCheck::Off,
        )
        .await
        .err()
        .expect("an identifier naming no table must be refused");
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
async fn a_malformed_identifier_anywhere_in_the_request_is_refused_before_any_catalog_request() {
    let catalog = iceberg_catalog().await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();

    let err = TableScanResolver::for_request(
        CatalogKind::IcebergRest,
        &catalog.uri,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["db.t", "malformed"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .err()
    .expect("a malformed identifier on any leg must be refused");

    assert!(
        err.to_string().contains("'malformed'"),
        "the refusal must name the identifier it could not parse: {err}"
    );
    assert!(
        catalog.targets().is_empty(),
        "a malformed identifier must cost no catalog request: {:?}",
        catalog.targets()
    );
}

#[tokio::test]
async fn an_iceberg_identifier_resolves_through_the_iceberg_reader_with_no_partition_columns() {
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
        &["db.t"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    let resolved = resolver
        .resolve("db.t", None, &TableNotes::default())
        .await
        .expect("a snapshotless Iceberg table resolves an empty scan");

    assert_eq!(
        catalog.targets(),
        vec![
            ICEBERG_CONFIG_TARGET.to_string(),
            ICEBERG_LOAD_TABLE_TARGET.to_string()
        ],
        "the resolver must load exactly the table the recorded identifier names"
    );
    assert!(
        resolved.partition_columns.is_empty(),
        "an Iceberg scan carries no partition columns"
    );
}

/// Scenario: one catalog session per request serves every table the request resolves
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
        .resolve("db.t", None, &TableNotes::default())
        .await
        .expect("first table");
    resolver
        .resolve("db.u", None, &TableNotes::default())
        .await
        .expect("second table");

    assert_eq!(
        catalog.targets(),
        vec![
            ICEBERG_CONFIG_TARGET.to_string(),
            ICEBERG_LOAD_TABLE_TARGET.to_string(),
            "/v1/namespaces/db/tables/u".to_string()
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
        .resolve("cat.sch.orders", None, &TableNotes::default())
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");
    let err2 = resolver
        .resolve("cat.sch.customers", None, &TableNotes::default())
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
        .resolve("events", None, &TableNotes::default())
        .await
        .expect("a directory with no data file resolves an empty scan");
    resolver
        .resolve("event_labels", None, &TableNotes::default())
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
    let endpoint = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = direct_storage_backend(&endpoint.uri);

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

    assert!(
        endpoint.targets().is_empty(),
        "a refused identifier must cost no object-store request: {:?}",
        endpoint.targets()
    );
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

    let scan = resolver
        .resolve("events", None, &TableNotes::default())
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

const REFUSED_TYPE: &str = "its type is refused";

fn note(field: LogicalField, partition: Option<usize>, refused: Option<&str>) -> String {
    ColumnDeclaration {
        source_type: field,
        partition,
        refused: refused.map(str::to_string),
    }
    .encode()
}

fn column_note(
    field: LogicalField,
    partition: Option<usize>,
    refused: Option<&str>,
) -> (String, String) {
    (field.name.to_uppercase(), note(field, partition, refused))
}

/// Scenario: Pushdown takes the declared schema from the column notes for every kind
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resolver_assembles_the_declared_schema_from_each_tables_notes() {
    let storage = object_endpoint(
        "warehouse",
        vec![
            (
                "direct/events/year=2026/p.parquet".to_string(),
                "not a parquet file".to_string(),
            ),
            (
                "direct/event_labels/q.parquet".to_string(),
                "not a parquet file".to_string(),
            ),
        ],
    )
    .await;
    let creds = unauthenticated_creds();
    let resolver = TableScanResolver::for_request(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["events", "event_labels"],
        &serde_json::json!({"NAMESPACE": "direct"}),
        &PermissionCheck::Off,
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");
    let events_notes = [
        column_note(nullable_field("id", "int64"), None, None),
        column_note(nullable_field("year", "utf8"), Some(0), None),
    ];
    let labels_notes = [column_note(nullable_field("label", "utf8"), None, None)];
    let year_2099 = equal("YEAR", "2099");

    let events = resolver
        .resolve(
            "events",
            Some(&year_2099),
            &involved_notes("EVENTS", &events_notes),
        )
        .await
        .expect("no footer is read, so unreadable files still plan");
    let labels = resolver
        .resolve(
            "event_labels",
            None,
            &involved_notes("EVENT_LABELS", &labels_notes),
        )
        .await
        .expect("no footer is read, so unreadable files still plan");

    assert_eq!(
        events
            .logical_schema
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "year"]
    );
    assert_eq!(events.partition_columns, vec!["year".to_string()]);
    assert!(
        events.files.is_empty(),
        "YEAR = '2099' prunes the year=2026 file from the recorded partition column: {:?}",
        events.files
    );
    assert_eq!(
        labels
            .logical_schema
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["label"],
        "each table plans from its own notes, never another leg's"
    );
    assert!(labels.partition_columns.is_empty());
    assert_eq!(labels.files.len(), 1);

    let iceberg = iceberg_catalog().await;
    let catalog_storage = sample_storage();
    let resolver = TableScanResolver::for_request(
        CatalogKind::IcebergRest,
        &iceberg.uri,
        ConnectionStorage {
            storage: &catalog_storage,
            creds: &creds,
            allow_http: true,
        },
        &["db.t"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");
    let score = LogicalField {
        field_id: Some(7),
        ..nullable_field("score", "int64")
    };
    let iceberg_notes = [
        column_note(score.clone(), None, None),
        column_note(nullable_field("blob", "utf8"), None, Some(REFUSED_TYPE)),
    ];

    let scores = resolver
        .resolve("db.t", None, &involved_notes("T", &iceberg_notes))
        .await
        .expect("a snapshotless Iceberg table plans");

    assert_eq!(
        scores.logical_schema,
        vec![score],
        "the declared field, which the current Iceberg schema does not carry"
    );
    assert_eq!(scores.refused_columns[0].column_name, "blob");
    assert!(scores.partition_columns.is_empty());

    let unity = unity_delta_catalog().await;
    let logged = [("id", "long"), ("blob", "binary")];
    let delta_storage = delta_object_endpoint(vec![(
        delta_commit_zero_key("orders"),
        fileless_delta_commit("orders", &logged),
    )])
    .await;
    let resolver = TableScanResolver::for_request(
        CatalogKind::UnityCatalogNative,
        &unity.uri,
        ConnectionStorage {
            storage: &delta_storage,
            creds: &creds,
            allow_http: true,
        },
        &["cat.sch.orders"],
        &Json::Null,
        &PermissionCheck::Off,
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");
    let delta_notes = delta_notes(&logged, &[]);

    let orders = resolver
        .resolve(
            "cat.sch.orders",
            None,
            &involved_notes("ORDERS", &delta_notes),
        )
        .await
        .expect("a fileless Delta table plans");

    let names: Vec<&str> = orders
        .logical_schema
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, vec!["id"]);
    assert_eq!(orders.refused_columns[0].column_name, "blob");
    assert_eq!(orders.table_root, "s3://bucket/orders");
    assert!(orders.files.is_empty() && orders.partition_columns.is_empty());
}

/// Scenario: Every pushdown request shape resolves through the one format-reader seam
#[tokio::test]
async fn a_refusing_declaration_fails_before_any_reader_loads_its_table_under_every_kind() {
    let iceberg = iceberg_catalog().await;
    let unity = unity_delta_catalog().await;
    let glue = GlueEndpoint::spawn(|_| (500, serde_json::json!({}))).await;
    let objects = empty_s3_endpoint().await;
    let creds = unauthenticated_creds();
    let storage = sample_storage();
    let direct_storage = direct_storage_backend(&objects.uri);
    let unreadable = TableNotes {
        table: "T",
        columns: vec![("ID", Some(r#"{"sourceType":"#)), ("YEAR", None)],
    };
    let unreadable_message = "the declaration note of column 'ID' in virtual table 'T' is missing \
                              or unreadable; ALTER VIRTUAL SCHEMA ... REFRESH rebuilds it (EOF \
                              while parsing a value at line 1 column 14)";
    let every_column_refused = [column_note(
        nullable_field("id", "int64"),
        None,
        Some(REFUSED_TYPE),
    )];
    let every_column_refused = involved_notes("T", &every_column_refused);
    let whole_table_message = "virtual table 'T' has no mappable column; every column is \
                               refused: 'id': its type is refused";
    let refused_partition = [
        column_note(nullable_field("id", "int64"), None, None),
        column_note(nullable_field("year", "utf8"), Some(0), Some(REFUSED_TYPE)),
    ];
    let refused_partition = involved_notes("T", &refused_partition);
    let partition_message = "virtual table 'T' declares 'year' as a partition column, but that \
                             column is refused (its type is refused); the scan cannot materialize \
                             a partition column absent from the logical schema";

    for (kind, uri, backend, identifier) in [
        (CatalogKind::IcebergRest, &iceberg.uri, &storage, "db.t"),
        (
            CatalogKind::UnityCatalogNative,
            &unity.uri,
            &storage,
            "cat.sch.orders",
        ),
        (CatalogKind::Glue, &glue.address, &storage, "sales.orders"),
        (
            CatalogKind::DirectStorage,
            &objects.uri,
            &direct_storage,
            "events",
        ),
    ] {
        let address = match kind {
            CatalogKind::DirectStorage => DIRECT_STORAGE_ADDRESS,
            _ => uri.as_str(),
        };
        let resolver = TableScanResolver::for_request(
            kind,
            address,
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
        .unwrap_or_else(|e| panic!("{kind:?} resolves a session: {e}"));
        for (notes, expected) in [
            (&unreadable, unreadable_message),
            (&every_column_refused, whole_table_message),
            (&refused_partition, partition_message),
        ] {
            let error = resolver
                .resolve(identifier, None, notes)
                .await
                .expect_err("the declaration refuses the scan");
            assert_eq!(user_message(error), expected, "{kind:?}");
        }
    }

    assert_eq!(
        iceberg.targets(),
        vec![ICEBERG_CONFIG_TARGET.to_string()],
        "the Iceberg reader loads no table for a declaration it cannot plan"
    );
    assert!(unity.targets().is_empty(), "{:?}", unity.targets());
    assert!(glue.operations().is_empty(), "{:?}", glue.operations());
    assert!(objects.targets().is_empty(), "{:?}", objects.targets());
}

/// Scenario: One batch-check per query decides every table before any table is read
#[test]
fn an_enforced_resolver_admits_only_the_tables_the_check_covered() {
    let admission = TableAdmission::Authorized(HashSet::from(["db.t".to_string()]));

    assert!(admission.admit("db.t").is_ok());
    assert!(admission.admit("db.other").is_err());
    assert!(TableAdmission::Unchecked.admit("db.other").is_ok());
}

/// Scenario: One batch-check per query decides every table before any table is read
#[tokio::test]
async fn an_enforced_check_admits_an_allowed_table_and_refuses_a_denied_one() {
    use crate::adapter::permission::{PERMISSION_CHECK, PermissionSettings, USER_MAPPING};

    let check = PermissionSettings::parse(&serde_json::json!({
        PERMISSION_CHECK: "LAKEKEEPER",
        USER_MAPPING: "oidc~{{ user }}",
    }))
    .unwrap()
    .check_for(|| Some("ALICE".into()))
    .unwrap();
    let (storage, creds) = (sample_storage(), unauthenticated_creds());
    for allowed in [true, false] {
        let catalog = RecordingCatalog::spawn(move |target| {
            if target.starts_with("/catalog/v1/config") {
                (200, "{}".to_string())
            } else {
                (
                    200,
                    format!(r#"{{"results":[{{"id":"read-data","allowed":{allowed}}}]}}"#),
                )
            }
        })
        .await;

        let resolver = TableScanResolver::for_request(
            CatalogKind::IcebergRest,
            &format!("{}/catalog", catalog.uri),
            ConnectionStorage {
                storage: &storage,
                creds: &creds,
                allow_http: true,
            },
            &["db.t"],
            &Json::Null,
            &check,
        )
        .await;

        assert_eq!(resolver.is_ok(), allowed);
    }
}
