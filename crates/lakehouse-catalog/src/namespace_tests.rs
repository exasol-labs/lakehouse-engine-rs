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

fn sigv4_creds() -> ConnectionCreds {
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.region = "eu-west-1".into();
    creds.warehouse = "123456789012".into();
    creds
}

#[tokio::test]
async fn list_namespace_tables_rejects_empty_namespace() {
    let creds = sigv4_creds();
    let session = CatalogSession::resolve("http://unused.invalid", &creds.warehouse, &creds)
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
    let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds)
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
        CatalogSession::resolve(CLOSED_PORT_URI, &creds.warehouse, &creds).await
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
    let session = CatalogSession::resolve(CLOSED_PORT_URI, &creds.warehouse, &creds)
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

/// Answers `parent` listings with `namespaces` whatever the parent, and one table `t` per namespace.
async fn spawn_parent_ignoring_catalog(namespaces: &'static str) -> String {
    let (catalog_uri, _) = spawn_server("application/json", move |request| {
        let path = request.split(' ').nth(1).unwrap_or_default();
        if path.ends_with("/v1/config") {
            (200, r#"{"defaults":{},"overrides":{}}"#.into())
        } else if let Some(ns) = path
            .strip_prefix("/v1/namespaces/")
            .and_then(|rest| rest.strip_suffix("/tables"))
        {
            let parts: Vec<&str> = ns.split("%1F").collect();
            let identifiers = serde_json::json!([{ "namespace": parts, "name": "t" }]);
            (
                200,
                serde_json::json!({ "identifiers": identifiers }).to_string(),
            )
        } else {
            (200, format!(r#"{{"namespaces":{namespaces}}}"#))
        }
    })
    .await;
    catalog_uri
}

/// Scenario: Enumeration descends only into direct children of the listed namespace
#[tokio::test]
async fn listing_descends_only_into_direct_children_when_the_catalog_ignores_parent() {
    for (case, namespaces, expected) in [
        ("top level only", r#"[["ds"]]"#, &["ds.t"][..]),
        (
            "every level and lookalikes",
            r#"[["ds"],["ds","a"],["ds","a","b"],["ds","a","ds"],["dsx"],["other","ds"]]"#,
            &["ds.a.b.t", "ds.a.ds.t", "ds.a.t", "ds.t"][..],
        ),
    ] {
        let catalog_uri = spawn_parent_ignoring_catalog(namespaces).await;
        let creds = base_creds();
        let session = CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds)
            .await
            .expect("the session resolves against the stub");

        let root = ["ds".to_string()];
        let listing = list_namespace_tables(&session, &root, &creds);
        let tables = tokio::time::timeout(Duration::from_secs(5), listing)
            .await
            .unwrap_or_else(|_| panic!("{case}: the listing must terminate"))
            .unwrap_or_else(|e| panic!("{case}: the listing must succeed: {e}"));

        let mut names: Vec<String> = tables
            .iter()
            .map(|t| format!("{}.{}", t.namespace().join("."), t.name()))
            .collect();
        names.sort();
        assert_eq!(names, expected, "{case}");
    }
}
