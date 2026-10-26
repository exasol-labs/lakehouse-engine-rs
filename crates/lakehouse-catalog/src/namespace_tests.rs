use super::*;
use crate::test_support::*;

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
    for use_sigv4 in [false, true] {
        let mut creds = base_creds();
        creds.use_sigv4 = use_sigv4;
        creds.region = "us-east-1".into();
        let session = CatalogSession::resolve(CLOSED_PORT_URI, &creds.warehouse, &creds)
            .await
            .expect("neither mode needs the catalog to resolve a session");

        let Err(UdfError::User(msg)) =
            list_namespace_tables(&session, &["db".to_string()], &creds).await
        else {
            panic!("a closed catalog port must fail the listing as a user error");
        };

        assert!(
            msg.starts_with("failed to list tables in namespace 'db': ")
                && msg.contains(CLOSED_PORT_CAUSE),
            "use_sigv4={use_sigv4}: {msg}"
        );
    }
}
