use super::super::test_support::{UNREACHABLE_CATALOG, endpoint_storage, statement_over};
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
async fn a_malformed_identifier_anywhere_in_the_request_is_refused_by_its_kinds_rule_first() {
    // Under Iceberg REST, an enforced check sends a batch-check to the unreachable catalog, which
    // fails with a transport error, so a refusal naming the identifier was decided before it.
    let rules = [
        "namespace.table",
        "catalog.schema.table",
        "first-level directory",
    ];
    for (kind, address, identifiers, rule) in [
        (
            CatalogKind::IcebergRest,
            UNREACHABLE_CATALOG,
            ["db.t", "malformed"],
            rules[0],
        ),
        (
            CatalogKind::UnityCatalogNative,
            UNREACHABLE_CATALOG,
            ["cat.sch.t", "orders"],
            rules[1],
        ),
        (
            CatalogKind::DirectStorage,
            "s3://warehouse",
            ["events", "sub/dir"],
            rules[2],
        ),
    ] {
        let statement = statement_over(kind, address, endpoint_storage(UNREACHABLE_CATALOG));

        let err = TableScanResolver::for_request(
            &statement,
            &lakekeeper_check(),
            &identifiers,
            &Json::Null,
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{kind:?}: a malformed identifier on any leg must be refused"));

        let message = err.to_string();
        assert!(
            message.contains(&format!("'{}'", identifiers[1])),
            "{kind:?}: the refusal must name the identifier it could not resolve: {message}"
        );
        for other in rules {
            assert_eq!(
                message.contains(other),
                other == rule,
                "{kind:?}: only the kind's own identifier rule judges it: {message}"
            );
        }
    }
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
