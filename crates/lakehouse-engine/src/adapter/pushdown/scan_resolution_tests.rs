use super::super::test_support::{
    ICEBERG_CONFIG_TARGET, ICEBERG_LOAD_TABLE_TARGET, RecordingCatalog, UNITY_TABLE_TARGET,
    iceberg_catalog, locationless_delta_table_body, sample_storage, unauthenticated_creds,
};
use super::*;
use crate::scan::spec::{StorageBackend, StorageProps};

/// Scenario: A Unity Catalog table's identity survives the round trip from the
/// involved table.
///
/// The recorded dotted identifier is split into namespace segments and a table
/// name, and the loader re-joins them into the SAME dotted full name the Unity
/// Catalog addresses a table by — so exactly the recorded table is loaded, and
/// the reader that plans it names that same table back.
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
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err = resolver
        .resolve("cat.sch.orders", None, &[])
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

/// Scenario: A Unity Catalog table's identity survives the round trip from the
/// involved table.
///
/// The split is the exact inverse of the join that recorded the identifier: the
/// leading segments become the namespace and the last one the table name. An
/// identifier carrying no separator at all recovers an EMPTY namespace, which
/// addresses no Unity Catalog table, so it is refused here rather than sent to
/// the catalog.
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

/// Scenario: A Unity Catalog table's identity survives the round trip from the
/// involved table.
///
/// A recorded identifier that names no table — an empty last segment, or no
/// segment separator at all — is refused by name when the request's resolver is
/// built, BEFORE any catalog request, because trimming it back to the segment
/// before would address a different table.
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

/// Scenario: One catalog session per request serves every table the request
/// resolves.
///
/// EVERY identifier the request will resolve is checked, not just the first: a
/// join whose second leg is malformed is refused before the session is built, so
/// it costs no catalog round-trip at all.
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

/// Scenario: Every pushdown request shape resolves through the one format-reader
/// seam.
///
/// An `IcebergRest` kind reaches the Iceberg reader, which resolves the
/// requested identifier and comes back with EMPTY partition columns — what the
/// resolver itself owns. The reader's own resolved shape (table root, files,
/// effective storage, logical schema, name mapping) is covered by
/// `format/iceberg_tests.rs`.
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
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    let resolved = resolver
        .resolve("db.t", None, &[])
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

/// Scenario: One catalog session per request serves every table the request
/// resolves.
///
/// The session is resolved into the resolver ONCE and reused for every table,
/// so a two-table request performs no more catalog authentication round-trips
/// than a single-table one.
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
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    resolver
        .resolve("db.t", None, &[])
        .await
        .expect("first table");
    resolver
        .resolve("db.u", None, &[])
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

/// Scenario: One catalog session per request serves every table the request
/// resolves — the `UnityCatalogNative` twin of
/// `one_catalog_session_serves_every_table_the_resolver_resolves`, since that
/// test covers only the Iceberg arm of the resolver doc's session-reuse claim.
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
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err1 = resolver
        .resolve("cat.sch.orders", None, &[])
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");
    let err2 = resolver
        .resolve("cat.sch.customers", None, &[])
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

/// An empty, non-truncated S3 listing: a directory with no data file.
const EMPTY_LIST_BUCKET_RESULT: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    "<Name>warehouse</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>",
    "<IsTruncated>false</IsTruncated></ListBucketResult>",
);

/// The CONNECTION address a direct-storage virtual schema is created over.
const DIRECT_STORAGE_ADDRESS: &str = "s3://warehouse";

/// An S3-compatible endpoint answering every listing empty while recording each request line.
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

// Direct-storage case: the object store is built once (no request cost) and each leg lists only its own table root — one listing per leg, no per-leg session setup.
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
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");

    assert!(
        endpoint.targets().is_empty(),
        "building the request's one store must cost no object-store request: {:?}",
        endpoint.targets()
    );

    resolver
        .resolve("events", None, &[])
        .await
        .expect("a directory with no data file resolves an empty scan");
    resolver
        .resolve("event_labels", None, &[])
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

// Identifiers naming no first-level directory are refused before any listing, since they'd compose a table root outside the storage base path.
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

// The pushdown path must compose the same table_root as enumeration's storage_location, regardless of trailing separators in the CONNECTION address.
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
    )
    .await
    .expect("a direct-storage store is built from the CONNECTION alone");

    let scan = resolver
        .resolve("events", None, &[])
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

// RequestSession has one variant per catalog kind, built once here, so no later step needs a second kind match.
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
        )
        .await
        .unwrap_or_else(|e| panic!("{kind:?} must resolve a session of its own: {e}"));
    }
}

/// A non-truncated S3 listing holding one Hive-partitioned object whose bytes are no Parquet file.
const ONE_UNREADABLE_HIVE_FILE_LIST_BUCKET_RESULT: &str = concat!(
    r#"<?xml version="1.0" encoding="UTF-8"?>"#,
    r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    "<Name>warehouse</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>",
    "<IsTruncated>false</IsTruncated>",
    "<Contents><Key>events/year=2026/p.parquet</Key>",
    "<LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>18</Size></Contents>",
    "</ListBucketResult>",
);

async fn resolve_events_under_hive_partitioning(
    endpoint_uri: &str,
    hive_partitioning: &str,
    filter: &Json,
) -> Result<ResolvedScan, UdfError> {
    let creds = unauthenticated_creds();
    let storage = direct_storage_backend(endpoint_uri);
    let resolver = TableScanResolver::for_request(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        ConnectionStorage {
            storage: &storage,
            creds: &creds,
            allow_http: true,
        },
        &["events"],
        &serde_json::json!({ "HIVE_PARTITIONING": hive_partitioning }),
    )
    .await?;
    resolver.resolve("events", Some(filter), &[]).await
}

#[tokio::test]
async fn hive_partitioning_reaches_the_seam_on_pushdown() {
    let endpoint = RecordingCatalog::spawn(|target| {
        if target.contains("list-type=2") {
            (200, ONE_UNREADABLE_HIVE_FILE_LIST_BUCKET_RESULT.to_string())
        } else {
            (200, "not a parquet file".to_string())
        }
    })
    .await;
    let year_2099 = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "YEAR"},
        "right": {"type": "literal_string", "value": "2099"}
    });

    let pruned = resolve_events_under_hive_partitioning(&endpoint.uri, "TRUE", &year_2099)
        .await
        .expect("HIVE_PARTITIONING=TRUE must prune the file before its footer is read");
    assert_eq!(pruned.partition_columns, vec!["year".to_string()]);
    assert!(
        pruned.files.is_empty(),
        "year=2026 file must be pruned by YEAR = '2099': {:?}",
        pruned.files
    );

    let error = resolve_events_under_hive_partitioning(&endpoint.uri, "FALSE", &year_2099)
        .await
        .expect_err("HIVE_PARTITIONING=FALSE must not prune, so the bad footer is read")
        .to_string();
    assert!(
        error.contains("failed to read the Parquet footer"),
        "expected a footer-read error: {error}"
    );
}
