use super::*;
use crate::test_support::*;

/// Scenario: single-level namespace — returns (NamespaceIdent::new("mydb"), "mytable").
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
/// "prod.finance.orders" → NamespaceIdent(["prod","finance"]), "orders".
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

    // Three-level namespace + table.
    let (ns3, tbl3) = parse_table_ident("prod.finance.eu.orders").unwrap();
    let levels3: &[String] = &ns3;
    assert_eq!(
        levels3,
        &["prod".to_string(), "finance".to_string(), "eu".to_string()],
        "namespace must have three levels"
    );
    assert_eq!(tbl3, "orders");
}

/// Scenario: an empty configured namespace is rejected by `NamespaceIdent::from_vec`
/// before any catalog request is issued — the error names the (empty) configured
/// namespace, not the underlying iceberg wording alone.
#[tokio::test]
async fn list_namespace_tables_rejects_empty_namespace() {
    let storage = static_backend();
    let creds = base_creds();

    let err = list_namespace_tables("http://unused.invalid", &[], &storage, &creds)
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("invalid namespace ''"),
        "error must name the empty configured namespace: {err}"
    );
}

/// Scenario: end-to-end — `list_namespace_tables`'s SigV4 enumeration path
/// (`list_in_namespace_signed` / `build_list_tables_url`) signs its
/// `list_tables` request against the derived `catalogs/{account-id}` prefix,
/// not the bare warehouse. This path bypasses `resolve_load_table_prefix`
/// entirely (it is `create-virtual-schema`'s namespace-enumeration path), so
/// it needs its own proof that `glue_catalog_prefix` reached it too.
///
/// `list_in_namespace_signed` issues the `list_tables` GET first; the
/// follow-up `list_namespaces?parent=` (child-namespace recursion) then sees
/// `EMPTY_LISTING`'s empty `namespaces` and stops, matching AWS Glue's flat
/// catalog. The first captured request head is therefore the `list_tables` one.
#[tokio::test]
async fn list_tables_signed_url_carries_catalogs_prefix() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let storage = static_backend();
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.warehouse = "123456789012".into();

    let result = list_namespace_tables(&catalog_uri, &["db".to_string()], &storage, &creds).await;

    assert!(
        result.is_ok(),
        "expected the enumeration to succeed: {:?}",
        result.err()
    );

    let head = heads
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("the list_tables request must have been captured");
    let request_line = head.lines().next().unwrap_or_default();
    assert!(
        request_line.contains("/v1/catalogs/123456789012/namespaces/db/tables"),
        "signed list_tables URL must carry the derived catalogs/{{account-id}} prefix: {request_line}"
    );
    assert!(
        !request_line.contains("/v1/123456789012/namespaces"),
        "signed list_tables URL must NOT use the bare warehouse as the prefix: {request_line}"
    );
}

/// A reply both enumeration requests parse: an empty `list_tables` page and an
/// empty `list_namespaces` page, so the enumeration stops after one level.
const EMPTY_LISTING: &str = r#"{"identifiers":[],"namespaces":[]}"#;

/// Scenario: A standard AWS Glue endpoint signs the catalog request even when
/// the CONNECTION states a different region — every namespace-enumeration
/// request is signed for the resolved region handed to the enumeration, never
/// for the stated `creds.region`.
#[tokio::test]
async fn signed_enumeration_is_signed_for_the_resolved_region() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.region = "us-east-1".into();
    let ns = NamespaceIdent::new("db".into());

    let enumeration = SignedEnumeration {
        catalog_uri: &catalog_uri,
        prefix: "catalogs/123456789012",
        creds: &creds,
        region: "eu-west-1",
    };
    enumeration
        .list_in_namespace_signed(&ns)
        .await
        .expect("the signed enumeration must succeed against the stub");

    let heads = heads.lock().unwrap();
    assert!(
        !heads.is_empty(),
        "the enumeration must have sent at least its list_tables request"
    );
    for head in heads.iter() {
        let authorization = authorization_header(head).expect("every request must be signed");
        assert!(
            authorization.contains("/eu-west-1/glue/aws4_request"),
            "every enumeration request must be signed for the resolved region: {authorization}"
        );
    }
}

/// Scenario: the signed enumeration refuses, before sending any request, when
/// neither the stated region nor the catalog URI supplies a signing region —
/// with an error naming `region`.
#[tokio::test]
async fn signed_enumeration_refuses_without_signing_region() {
    let (catalog_uri, heads) = spawn_recording_catalog(EMPTY_LISTING).await;
    let storage = static_backend();
    let mut creds = base_creds();
    creds.use_sigv4 = true;
    creds.region = String::new();

    let Err(UdfError::User(msg)) =
        list_namespace_tables(&catalog_uri, &["db".to_string()], &storage, &creds).await
    else {
        panic!("a SigV4 enumeration with no signing region must be refused as a user error");
    };

    assert_eq!(msg, crate::sigv4::MISSING_SIGNING_REGION);
    assert!(
        heads.lock().unwrap().is_empty(),
        "no catalog request may be sent without a signing region"
    );
}
