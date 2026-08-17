use super::super::test_support::{
    ICEBERG_CONFIG_TARGET, ICEBERG_LOAD_TABLE_TARGET, RecordingCatalog, UNITY_TABLE_TARGET,
    iceberg_catalog, locationless_delta_table_body, sample_storage, unauthenticated_creds,
};
use super::*;

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
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err = resolver
        .resolve("cat.sch.orders", None)
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
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    let resolved = resolver
        .resolve("db.t", None)
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
    )
    .await
    .expect("an Iceberg session resolves against a reachable catalog");

    resolver.resolve("db.t", None).await.expect("first table");
    resolver.resolve("db.u", None).await.expect("second table");

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
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err1 = resolver
        .resolve("cat.sch.orders", None)
        .await
        .expect_err("a Delta table carrying no storage location cannot be planned");
    let err2 = resolver
        .resolve("cat.sch.customers", None)
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
