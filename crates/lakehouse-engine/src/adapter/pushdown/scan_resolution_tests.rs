use super::super::binary_filter::BinaryLayout;
use super::super::test_support::{
    GlueEndpoint, ICEBERG_CONFIG_TARGET, ICEBERG_LOAD_TABLE_TARGET, RecordingCatalog,
    UNITY_TABLE_TARGET, UNREACHABLE_CATALOG, delta_notes, endpoint_storage, iceberg_catalog,
    involved_notes, locationless_delta_table_body, sample_storage, statement_over,
};
use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::direct_storage_properties::join_storage_path;
use crate::adapter::permission::PermissionSettings;
use crate::adapter::properties::{PERMISSION_CHECK, USER_MAPPING};
use crate::types::exasol_type::ExasolType;

fn lakekeeper_check() -> PermissionCheck {
    PermissionSettings::parse(&serde_json::json!({
        PERMISSION_CHECK: "LAKEKEEPER",
        USER_MAPPING: "oidc~{{ user }}",
    }))
    .unwrap()
    .check_for(|| Some("ALICE".into()))
    .unwrap()
}

/// Scenario: A Unity Catalog table's identity survives the round trip from the involved table
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
    let statement = statement_over(
        CatalogKind::UnityCatalogNative,
        &catalog.uri,
        sample_storage(),
    );
    let resolver = TableScanResolver::for_request(
        &statement,
        &PermissionCheck::Off,
        &["cat.sch.orders"],
        &Json::Null,
    )
    .await
    .expect("a Unity Catalog session is built without contacting the catalog");

    let err = resolver
        .resolve("cat.sch.orders", None, &InvolvedColumns::default())
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
    let statement = statement_over(CatalogKind::Glue, &glue.address, sample_storage());
    let resolver = TableScanResolver::for_request(
        &statement,
        &PermissionCheck::Off,
        &["sales.orders"],
        &Json::Null,
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

#[tokio::test]
async fn a_malformed_identifier_anywhere_in_the_request_is_refused_before_any_catalog_request() {
    let catalog = iceberg_catalog().await;
    let statement = statement_over(CatalogKind::IcebergRest, &catalog.uri, sample_storage());

    let err = TableScanResolver::for_request(
        &statement,
        &PermissionCheck::Off,
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

#[tokio::test]
async fn an_iceberg_identifier_resolves_through_the_iceberg_reader_with_no_partition_columns() {
    let catalog = iceberg_catalog().await;
    let statement = statement_over(CatalogKind::IcebergRest, &catalog.uri, sample_storage());
    let resolver =
        TableScanResolver::for_request(&statement, &PermissionCheck::Off, &["db.t"], &Json::Null)
            .await
            .expect("an Iceberg session resolves against a reachable catalog");

    let (resolved, _) = resolver
        .resolve("db.t", None, &InvolvedColumns::default())
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

#[tokio::test]
async fn the_pushdown_table_root_equals_the_discovery_composed_storage_location() {
    let endpoint = empty_s3_endpoint().await;
    let address = "s3://warehouse/direct//";
    let statement = statement_over(
        CatalogKind::DirectStorage,
        address,
        endpoint_storage(&endpoint.uri),
    );

    let resolver =
        TableScanResolver::for_request(&statement, &PermissionCheck::Off, &["events"], &Json::Null)
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
async fn an_enforced_check_reads_no_table_that_no_check_admitted() {
    let statement = statement_over(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        endpoint_storage(UNREACHABLE_CATALOG),
    );
    let check = lakekeeper_check();
    let resolver = TableScanResolver::for_request(&statement, &check, &["events"], &Json::Null)
        .await
        .expect("a kind that runs no check admits nothing and fails nothing");

    let err = resolver
        .resolve("events", None, &InvolvedColumns::default())
        .await
        .expect_err("a table no check admitted must not be read");

    assert!(
        err.to_string().contains("did not cover table 'events'"),
        "{err}"
    );
}

#[tokio::test]
async fn an_enforced_check_admits_an_allowed_table_and_refuses_a_denied_one() {
    let check = lakekeeper_check();
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
        let statement = statement_over(
            CatalogKind::IcebergRest,
            &format!("{}/catalog", catalog.uri),
            sample_storage(),
        );

        let resolver =
            TableScanResolver::for_request(&statement, &check, &["db.t"], &Json::Null).await;

        assert_eq!(resolver.is_ok(), allowed);
    }
}

#[tokio::test]
async fn the_resolver_returns_the_map_the_request_types_and_the_column_notes_build() {
    let endpoint = empty_s3_endpoint().await;
    let statement = statement_over(
        CatalogKind::DirectStorage,
        DIRECT_STORAGE_ADDRESS,
        endpoint_storage(&endpoint.uri),
    );
    let resolver =
        TableScanResolver::for_request(&statement, &PermissionCheck::Off, &["events"], &Json::Null)
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
