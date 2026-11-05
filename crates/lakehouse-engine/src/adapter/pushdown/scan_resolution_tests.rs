use super::super::test_support::{
    UNREACHABLE_CATALOG, endpoint_storage, sample_storage, statement_over,
};
use super::*;
use crate::adapter::catalog_kind::CatalogKind;
use crate::adapter::permission::PermissionSettings;
use crate::adapter::properties::{PERMISSION_CHECK, USER_MAPPING};

fn lakekeeper_check() -> PermissionCheck {
    PermissionSettings::parse(&serde_json::json!({
        PERMISSION_CHECK: "LAKEKEEPER",
        USER_MAPPING: "oidc~{{ user }}",
    }))
    .unwrap()
    .check_for(|| Some("ALICE".into()))
    .unwrap()
}

#[tokio::test]
async fn a_malformed_identifier_anywhere_in_the_request_is_refused_before_any_catalog_request() {
    // An enforced check sends a batch-check to the unreachable catalog, which fails with a
    // transport error, so a refusal naming the identifier was decided before any request.
    let statement = statement_over(
        CatalogKind::IcebergRest,
        UNREACHABLE_CATALOG,
        sample_storage(),
    );

    let err = TableScanResolver::for_request(
        &statement,
        &lakekeeper_check(),
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
}

#[tokio::test]
async fn an_enforced_check_reads_no_table_that_no_check_admitted() {
    let statement = statement_over(
        CatalogKind::DirectStorage,
        "s3://warehouse",
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
