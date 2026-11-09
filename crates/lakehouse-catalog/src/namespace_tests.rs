use super::*;
use crate::test_support::*;
use std::time::Duration;

#[test]
fn parse_table_ident_splits_namespace_table() {
    let (ns, tbl) = parse_table_ident("mydb.mytable").unwrap();
    let levels: &[String] = &ns;
    assert_eq!(levels, &["mydb".to_string()]);
    assert_eq!(tbl, "mytable");
}

#[test]
fn parse_table_ident_errors_on_no_dot() {
    let err = parse_table_ident("notable").unwrap_err();
    assert!(err.to_string().contains("namespace.table"));
}

/// Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent
#[test]
fn parse_table_ident_handles_multilevel_namespace() {
    let (ns, tbl) = parse_table_ident("prod.finance.orders").unwrap();
    let levels: &[String] = &ns;
    assert_eq!(
        levels,
        &["prod".to_string(), "finance".to_string()],
        "namespace must have two levels"
    );
    assert_eq!(tbl, "orders", "table name is the trailing segment");

    let (ns3, tbl3) = parse_table_ident("prod.finance.eu.orders").unwrap();
    let levels3: &[String] = &ns3;
    assert_eq!(
        levels3,
        &["prod".to_string(), "finance".to_string(), "eu".to_string()],
        "namespace must have three levels"
    );
    assert_eq!(tbl3, "orders");
}

#[tokio::test]
async fn list_namespace_tables_rejects_empty_namespace() {
    let creds = sigv4_creds();
    let session = CatalogSession::resolve("http://unused.invalid", &creds.warehouse, None, &creds)
        .await
        .expect("a SigV4 session resolves without any network access");

    let err = list_namespace_tables(&session, &[], &creds)
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("invalid namespace ''"),
        "error must name the empty configured namespace: {err}"
    );
}

/// Glue namespaces are single-level, so the signed listing sends no `parent` request.
#[tokio::test]
async fn signed_listing_lists_only_the_configured_namespace_under_the_catalogs_prefix() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let creds = sigv4_creds();
    let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, None, &creds)
        .await
        .expect("a SigV4 session resolves without any network access");

    list_namespace_tables(&session, &["db".to_string()], &creds)
        .await
        .expect("the signed listing must succeed against the stub");

    let heads = heads.lock().unwrap();
    let [head] = heads.as_slice() else {
        panic!("exactly one listing request, got {}", heads.len());
    };
    let request_line = head.lines().next().unwrap_or_default();
    assert!(
        request_line.starts_with("GET /v1/catalogs/123456789012/namespaces/db/tables "),
        "the signed listing must address the catalogs/{{account-id}} prefix: {request_line}"
    );
    let authorization = authorization_header(head).expect("the request must be signed");
    assert!(
        authorization.contains("/eu-west-1/glue/aws4_request"),
        "the listing must be signed for the resolved region: {authorization}"
    );
}

const EMPTY_LISTING: &str = r#"{"identifiers":[],"namespaces":[]}"#;

#[tokio::test]
async fn a_refused_catalog_connection_names_its_cause_signed_or_not() {
    let mut creds = base_creds();
    let Err(UdfError::User(msg)) =
        CatalogSession::resolve(CLOSED_PORT_URI, &creds.warehouse, None, &creds).await
    else {
        panic!("an unsigned session reads /v1/config, so a closed port must fail it");
    };
    assert!(
        msg.starts_with("failed to read the catalog config for warehouse ")
            && msg.contains(CLOSED_PORT_CAUSE),
        "{msg}"
    );

    creds.use_sigv4 = true;
    creds.region = "us-east-1".into();
    let session = CatalogSession::resolve(CLOSED_PORT_URI, &creds.warehouse, None, &creds)
        .await
        .expect("a SigV4 session resolves without the catalog");
    let Err(UdfError::User(msg)) =
        list_namespace_tables(&session, &["db".to_string()], &creds).await
    else {
        panic!("a closed catalog port must fail the listing as a user error");
    };
    assert!(
        msg.starts_with("failed to list tables in namespace 'db': ")
            && msg.contains(CLOSED_PORT_CAUSE),
        "{msg}"
    );
}

#[test]
fn direct_children_keeps_only_namespaces_one_level_below_the_parent() {
    let ns = |parts: &[&str]| NamespaceIdent::from_strs(parts).unwrap();
    let listed = [
        &["ds"][..],
        &["ds", "a"],
        &["ds", "a", "b"],
        &["ds", "ds"],
        &["dsx", "a"],
        &["other", "ds"],
    ];

    let children = direct_children(&ns(&["ds"]), listed.iter().map(|p| ns(p)).collect());

    assert_eq!(children, [ns(&["ds", "a"]), ns(&["ds", "ds"])]);
}

/// Scenario: Enumeration descends only into direct children of the listed namespace
#[tokio::test]
async fn listing_terminates_when_the_catalog_ignores_parent() {
    let (catalog_uri, _) = spawn_recording_catalog(
        r#"{"identifiers":[{"namespace":["ds"],"name":"t"}],"namespaces":[["ds"]]}"#,
    )
    .await;
    let creds = base_creds();
    let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, None, &creds)
        .await
        .expect("the session resolves against the stub");
    let root = ["ds".to_string()];

    let tables = tokio::time::timeout(
        Duration::from_secs(1),
        list_namespace_tables(&session, &root, &creds),
    )
    .await
    .expect("the listing must terminate")
    .expect("the listing must succeed");

    assert_eq!(tables, [TableIdent::from_strs(["ds", "t"]).unwrap()]);
}
