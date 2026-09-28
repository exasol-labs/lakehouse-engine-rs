use super::*;
use crate::ConnectionCreds;
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

/// Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent.
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

fn sigv4_creds(region: &str) -> ConnectionCreds {
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.region = region.into();
    creds.warehouse = "123456789012".into();
    creds
}

async fn sigv4_session(catalog_uri: &str, creds: &ConnectionCreds) -> CatalogSession {
    CatalogSession::resolve(catalog_uri, &creds.warehouse, creds)
        .await
        .expect("a SigV4 session resolves without any request")
}

#[tokio::test]
async fn list_namespace_tables_rejects_empty_namespace() {
    let creds = sigv4_creds("us-east-1");
    let session = sigv4_session("http://unused.invalid", &creds).await;

    let err = list_namespace_tables(&session, &[], &creds)
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("invalid namespace ''"),
        "error must name the empty configured namespace: {err}"
    );
}

#[tokio::test]
async fn signed_enumeration_carries_the_catalogs_prefix_and_lists_no_children() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let creds = sigv4_creds("us-east-1");
    let session = sigv4_session(&catalog_uri, &creds).await;

    list_namespace_tables(&session, &["db".to_string()], &creds)
        .await
        .expect("the enumeration succeeds against the stub");

    let heads = heads.lock().unwrap();
    let request_lines: Vec<&str> = heads
        .iter()
        .map(|head| head.lines().next().unwrap_or_default())
        .collect();
    assert_eq!(
        request_lines.len(),
        1,
        "a flat catalog is asked for tables only, never for child namespaces: {request_lines:?}"
    );
    assert!(
        request_lines[0].contains("/v1/catalogs/123456789012/namespaces/db/tables"),
        "signed list_tables URL must carry the derived catalogs/{{account-id}} prefix: {request_lines:?}"
    );
}

const EMPTY_LISTING: &str = r#"{"identifiers":[],"namespaces":[]}"#;

#[tokio::test]
async fn signed_enumeration_is_signed_for_the_resolved_region() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let creds = sigv4_creds("eu-west-1");
    let session = sigv4_session(&catalog_uri, &creds).await;

    list_namespace_tables(&session, &["db".to_string()], &creds)
        .await
        .expect("the signed enumeration must succeed against the stub");

    let heads = heads.lock().unwrap();
    assert!(!heads.is_empty());
    for head in heads.iter() {
        let authorization = authorization_header(head).expect("every request must be signed");
        assert!(
            authorization.contains("/eu-west-1/glue/aws4_request"),
            "every enumeration request must be signed for the resolved region: {authorization}"
        );
    }
}

#[tokio::test]
async fn signed_enumeration_refuses_without_signing_region() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let creds = sigv4_creds("");

    let Err(UdfError::User(msg)) =
        CatalogSession::resolve(&catalog_uri, &creds.warehouse, &creds).await
    else {
        panic!("a SigV4 enumeration with no signing region must be refused as a user error");
    };

    assert_eq!(msg, crate::sigv4::MISSING_SIGNING_REGION);
    assert!(
        heads.lock().unwrap().is_empty(),
        "no catalog request may be sent without a signing region"
    );
}

#[tokio::test]
async fn descendant_namespaces_are_enumerated_depth_first_with_siblings_in_flight_together() {
    let catalog = spawn_routing_catalog(
        vec![
            ("/v1/config?warehouse=warehouse", vec![(200, "{}".into())]),
            (
                "/v1/namespaces/db/tables",
                vec![(200, list_tables_page(&["db"], &["t1"], None))],
            ),
            (
                "/v1/namespaces?parent=db",
                vec![(
                    200,
                    list_namespaces_page(&[&["db", "a"], &["db", "b"]], None),
                )],
            ),
            (
                "/v1/namespaces/db%1Fa/tables",
                vec![(200, list_tables_page(&["db", "a"], &["t2"], None))],
            ),
            (
                "/v1/namespaces?parent=db%1Fa",
                vec![(200, list_namespaces_page(&[], None))],
            ),
            (
                "/v1/namespaces/db%1Fb/tables",
                vec![(200, list_tables_page(&["db", "b"], &["t3"], None))],
            ),
            (
                "/v1/namespaces?parent=db%1Fb",
                vec![(200, list_namespaces_page(&[], None))],
            ),
        ],
        Duration::from_millis(40),
    )
    .await;
    let creds = creds_no_auth();
    let session = CatalogSession::resolve(&catalog.base_uri, &creds.warehouse, &creds)
        .await
        .expect("session resolves");

    let idents = list_namespace_tables(&session, &["db".to_string()], &creds)
        .await
        .expect("the nested enumeration succeeds");

    assert_eq!(
        idents.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
        ["db.t1", "db.a.t2", "db.b.t3"]
    );
    assert!(
        catalog
            .max_in_flight
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2,
        "a namespace's table and child listings, and sibling namespaces, must overlap"
    );
}

#[tokio::test]
async fn a_failed_child_listing_is_an_error_not_an_empty_subtree() {
    let catalog = spawn_routing_catalog(
        vec![
            ("/v1/config?warehouse=warehouse", vec![(200, "{}".into())]),
            (
                "/v1/namespaces/db/tables",
                vec![(200, list_tables_page(&["db"], &["t1"], None))],
            ),
            ("/v1/namespaces?parent=db", vec![(500, String::new())]),
        ],
        Duration::ZERO,
    )
    .await;
    let creds = creds_no_auth();
    let session = CatalogSession::resolve(&catalog.base_uri, &creds.warehouse, &creds)
        .await
        .expect("session resolves");

    let err = list_namespace_tables(&session, &["db".to_string()], &creds)
        .await
        .expect_err("a failed child listing must not silently drop the subtree");

    assert!(
        err.to_string()
            .contains("failed to list namespaces under 'db'"),
        "{err}"
    );
}
