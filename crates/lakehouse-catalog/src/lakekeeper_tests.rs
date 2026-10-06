use super::*;
use crate::test_support::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

const WAREHOUSE_ID: &str = "5c25f9c4-be40-11f1-a87d-17102559a460";
const FIXTURE_PRINCIPAL: &str = "oidc~lakehouse-reader-a@corp.net";
const CLIENT_ID: &str = "CLIENT_ID_SENTINEL_VALUE";
const SCOPE: &str = "SCOPE_SENTINEL_VALUE";
const CONFIG_REQUEST: &str = "GET /catalog/v1/config";
const TOKEN_REQUEST: &str = "POST /catalog/v1/oauth/tokens";
const BATCH_CHECK_REQUEST: &str = "POST /management/v1/action/batch-check";
const CLOSED_PORT_CATALOG_URI: &str = "http://127.0.0.1:1/catalog";

const FIXTURES: [(&str, &str); 4] = [
    (
        "allowed",
        include_str!("../tests/fixtures/lakekeeper/batch-check/allowed.json"),
    ),
    (
        "denied",
        include_str!("../tests/fixtures/lakekeeper/batch-check/denied.json"),
    ),
    (
        "missing",
        include_str!("../tests/fixtures/lakekeeper/batch-check/missing.json"),
    ),
    (
        "cannot-inspect",
        include_str!("../tests/fixtures/lakekeeper/batch-check/cannot-inspect.json"),
    ),
];

struct Stub {
    base_uri: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Stub {
    fn catalog_uri(&self) -> String {
        format!("{}/catalog", self.base_uri)
    }

    fn batch_check_url(&self) -> String {
        format!("{}/management/v1/action/batch-check", self.base_uri)
    }

    fn batch_checks(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with(BATCH_CHECK_REQUEST))
            .cloned()
            .collect()
    }

    fn only_batch_check(&self) -> String {
        let mut checks = self.batch_checks();
        assert_eq!(checks.len(), 1, "exactly one batch-check must be sent");
        checks.remove(0)
    }
}

/// Serves the OAuth2 grant and `/v1/config` of a Lakekeeper catalog, and answers every
/// batch-check with `status` and `body`.
async fn lakekeeper_stub(content_type: &'static str, status: u16, body: String) -> Stub {
    let (base_uri, requests) = spawn_server(content_type, move |request| {
        if request.starts_with(TOKEN_REQUEST) {
            (200, format!(r#"{{"access_token":"{OAUTH_ACCESS_TOKEN}"}}"#))
        } else if request.starts_with(CONFIG_REQUEST) {
            (
                200,
                format!(r#"{{"defaults":{{"prefix":"{WAREHOUSE_ID}"}}}}"#),
            )
        } else if request.starts_with(BATCH_CHECK_REQUEST) {
            (status, body.clone())
        } else {
            (404, "unexpected request".into())
        }
    })
    .await;
    Stub { base_uri, requests }
}

async fn json_stub(status: u16, body: impl ToString) -> Stub {
    lakekeeper_stub("application/json", status, body.to_string()).await
}

fn oauth_creds(stub: &Stub) -> ConnectionCreds {
    ConnectionCreds {
        client_id: Some(CLIENT_ID.into()),
        client_secret: Some(CLIENT_SECRET.into()),
        scope: Some(SCOPE.into()),
        oauth2_server_uri: Some(format!("{}/catalog/v1/oauth/tokens", stub.base_uri)),
        ..base_creds()
    }
}

async fn session_of(stub: &Stub, creds: &ConnectionCreds) -> CatalogSession {
    CatalogSession::resolve(&stub.catalog_uri(), "lakehouse_authz", creds)
        .await
        .expect("the stub grants a token")
}

async fn check(
    stub: &Stub,
    principal: &str,
    tables: &[&str],
) -> Result<Vec<TableReadDecision>, UdfError> {
    let creds = oauth_creds(stub);
    let session = session_of(stub, &creds).await;
    lakekeeper_batch_check(&session, principal, tables, &creds).await
}

fn message(error: UdfError) -> String {
    match error {
        UdfError::User(message) => message,
        other => panic!("expected a user error, got {other:?}"),
    }
}

fn fixture(case: &str) -> Value {
    let text = FIXTURES
        .iter()
        .find_map(|(name, text)| (*name == case).then_some(*text))
        .unwrap_or_else(|| panic!("no fixture named {case}"))
        .replace("<warehouse-id>", WAREHOUSE_ID)
        .replace("<principal:lakehouse-reader-a>", FIXTURE_PRINCIPAL);
    serde_json::from_str(&text).expect("a fixture is one JSON object")
}

/// The `namespace.table` identifier of a fixture request's one check.
fn fixture_table(fixture: &Value) -> String {
    let table = &fixture["request"]["checks"][0]["operation"]["table"];
    let namespace: Vec<&str> = table["namespace"]
        .as_array()
        .expect("a namespace array")
        .iter()
        .map(|part| part.as_str().expect("a namespace part"))
        .collect();
    format!(
        "{}.{}",
        namespace.join("."),
        table["table"].as_str().unwrap()
    )
}

/// A fixture body that is not JSON is a JSON string.
fn fixture_body(fixture: &Value) -> String {
    match &fixture["response"]["body"] {
        Value::String(text) => text.clone(),
        json => json.to_string(),
    }
}

async fn replay(case: &str) -> (Stub, Value, Result<Vec<TableReadDecision>, UdfError>) {
    let fixture = fixture(case);
    let status = fixture["response"]["status"].as_u64().unwrap() as u16;
    let stub = json_stub(status, fixture_body(&fixture)).await;
    let table = fixture_table(&fixture);
    let result = check(&stub, FIXTURE_PRINCIPAL, &[&table]).await;
    (stub, fixture, result)
}

fn sent_body(stub: &Stub) -> Value {
    let request = stub.only_batch_check();
    serde_json::from_str(request_body(&request)).expect("the batch-check body is JSON")
}

/// Scenario: One batch-check per query decides every table before any table is read
#[tokio::test]
async fn batch_check_request_equals_each_fixture_request() {
    for (case, _) in FIXTURES {
        let (stub, fixture, _) = replay(case).await;

        assert_eq!(
            sent_body(&stub),
            fixture["request"],
            "the request for fixture {case} must equal the recorded request"
        );
    }
}

/// Scenario: A denied table refuses the whole query
#[tokio::test]
async fn each_fixture_response_yields_its_decision() {
    for (case, allowed) in [("allowed", true), ("denied", false), ("missing", false)] {
        let (_, fixture, result) = replay(case).await;

        assert_eq!(
            result.expect("a 200 answer yields decisions"),
            vec![TableReadDecision {
                table: fixture_table(&fixture),
                allowed
            }],
            "fixture {case}"
        );
    }
}

/// Scenario: One batch-check per query decides every table before any table is read
#[tokio::test]
async fn batch_check_sends_one_check_per_distinct_table() {
    let answer = json!({"results": [
        {"id": "read-data", "allowed": true},
        {"id": "read-data-1", "allowed": false},
        {"id": "read-data-2", "allowed": true},
    ]});
    let stub = json_stub(200, answer).await;

    let decisions = check(
        &stub,
        FIXTURE_PRINCIPAL,
        &[
            "sales.orders",
            "sales.items",
            "sales.orders",
            "lake.eu.events",
        ],
    )
    .await
    .expect("a well-formed answer yields decisions");

    let body = sent_body(&stub);
    let checks = body["checks"].as_array().unwrap();
    let sent: Vec<(&str, &str, Value)> = checks
        .iter()
        .map(|check| {
            let table = &check["operation"]["table"];
            (
                check["id"].as_str().unwrap(),
                table["table"].as_str().unwrap(),
                table["namespace"].clone(),
            )
        })
        .collect();
    assert_eq!(
        sent,
        vec![
            ("read-data", "orders", json!(["sales"])),
            ("read-data-1", "items", json!(["sales"])),
            ("read-data-2", "events", json!(["lake", "eu"])),
        ],
        "one check per distinct table, in first-seen order, with the namespace split"
    );
    assert_eq!(
        decisions,
        vec![
            TableReadDecision {
                table: "sales.orders".into(),
                allowed: true
            },
            TableReadDecision {
                table: "sales.items".into(),
                allowed: false
            },
            TableReadDecision {
                table: "lake.eu.events".into(),
                allowed: true
            },
        ]
    );
}

#[tokio::test]
async fn batch_check_sends_the_session_prefix_as_the_warehouse_id_and_the_session_bearer() {
    let stub = json_stub(200, r#"{"results":[{"id":"read-data","allowed":true}]}"#).await;

    check(&stub, FIXTURE_PRINCIPAL, &["authz.authz_alpha"])
        .await
        .expect("allowed");

    let request = stub.only_batch_check();
    assert_eq!(
        sent_body(&stub)["checks"][0]["operation"]["table"]["warehouse-id"],
        WAREHOUSE_ID
    );
    assert_eq!(
        authorization_header(&request),
        Some(format!("Bearer {OAUTH_ACCESS_TOKEN}").as_str()),
        "the batch-check carries the session's own bearer token"
    );
    assert_eq!(
        header_value(&request, "content-type"),
        Some("application/json")
    );
}

#[tokio::test]
async fn an_empty_session_prefix_is_sent_unchanged() {
    let (base_uri, requests) = spawn_server("application/json", |request| {
        if request.starts_with(BATCH_CHECK_REQUEST) {
            (
                422,
                "Failed to deserialize the JSON body: TabularIdentOrUuid".into(),
            )
        } else {
            (404, "not found".into())
        }
    })
    .await;
    let stub = Stub { base_uri, requests };
    let creds = ConnectionCreds {
        token: Some(BEARER_TOK.into()),
        ..base_creds()
    };
    let session = session_of(&stub, &creds).await;

    let result = lakekeeper_batch_check(&session, FIXTURE_PRINCIPAL, &["authz.t"], &creds).await;

    assert_eq!(
        sent_body(&stub)["checks"][0]["operation"]["table"]["warehouse-id"],
        ""
    );
    assert!(result.is_err(), "the 422 refuses the check");
}

#[tokio::test]
async fn no_tables_send_no_batch_check() {
    let stub = json_stub(200, "{}").await;

    let decisions = check(&stub, FIXTURE_PRINCIPAL, &[])
        .await
        .expect("nothing to ask");

    assert!(decisions.is_empty());
    assert!(stub.batch_checks().is_empty());
}

#[tokio::test]
async fn a_table_without_a_namespace_is_an_error_and_sends_nothing() {
    let stub = json_stub(200, "{}").await;

    let error = check(&stub, FIXTURE_PRINCIPAL, &["orders"])
        .await
        .expect_err("a bare table name has no namespace");

    assert!(message(error).contains("namespace.table"));
    assert!(stub.batch_checks().is_empty());
}

/// Scenario: The adapter trusts the template author and evaluates no user name as template text
#[tokio::test]
async fn batch_check_identity_decodes_to_the_exact_principal() {
    for principal in [
        r#"oidc~A"B\C@corp.net"#,
        "oidc~{{7*7}}@corp.net",
        "oidc~a\u{e9}\u{4e2d}@corp.net",
    ] {
        let stub = json_stub(200, r#"{"results":[{"id":"read-data","allowed":true}]}"#).await;

        check(&stub, principal, &["authz.authz_alpha"])
            .await
            .expect("allowed");

        assert_eq!(
            sent_body(&stub)["checks"][0]["identity"]["user"],
            principal,
            "the identity must decode to the principal exactly"
        );
    }
}

struct FailureClass {
    name: &'static str,
    content_type: &'static str,
    status: u16,
    body: String,
    marker: &'static str,
}

fn echoed_secrets() -> String {
    format!("{OAUTH_ACCESS_TOKEN} {CLIENT_SECRET} {CLIENT_ID} {SCOPE}")
}

fn failure_classes() -> Vec<FailureClass> {
    let echo = echoed_secrets();
    let class = |name, content_type, status, marker, body: String| FailureClass {
        name,
        content_type,
        status,
        body,
        marker,
    };
    vec![
        class(
            "400 BadRequestException from a server that is not Lakekeeper",
            "application/json",
            400,
            "BadRequestException",
            format!(
                r#"{{"error":{{"message":"No route for request {echo}","type":"BadRequestException","code":400}}}}"#
            ),
        ),
        class(
            "422 text/plain from an empty or non-UUID warehouse id",
            "text/plain",
            422,
            "TabularIdentOrUuid",
            format!("Failed to deserialize the JSON body into TabularIdentOrUuid {echo}"),
        ),
        class(
            "401",
            "application/json",
            401,
            "UnauthorizedException",
            format!(r#"{{"error":{{"type":"UnauthorizedException","message":"{echo}"}}}}"#),
        ),
        class(
            "403 that is not CannotInspectPermissions",
            "application/json",
            403,
            "ForbiddenException",
            format!(r#"{{"error":{{"type":"ForbiddenException","message":"{echo}"}}}}"#),
        ),
        class(
            "404",
            "text/html",
            404,
            "page-not-found",
            format!("<html>page-not-found {echo}</html>"),
        ),
        class(
            "500",
            "text/plain",
            500,
            "internal-failure",
            format!("internal-failure {echo}"),
        ),
        class(
            "200 whose body is not JSON",
            "text/html",
            200,
            "login-page",
            format!("<html>login-page {echo}</html>"),
        ),
        class(
            "200 whose body is JSON without results",
            "application/json",
            200,
            "no-results-here",
            format!(r#"{{"message":"no-results-here {echo}"}}"#),
        ),
        class(
            "200 whose results hold a non-boolean allowed",
            "application/json",
            200,
            "non-boolean",
            format!(
                r#"{{"results":[{{"id":"read-data","allowed":"yes"}}],"echo":"non-boolean {echo}"}}"#
            ),
        ),
        class(
            "200 whose results miss the check id",
            "application/json",
            200,
            "missing-id",
            format!(
                r#"{{"results":[{{"id":"other","allowed":true}}],"echo":"missing-id {echo}"}}"#
            ),
        ),
    ]
}

fn assert_no_credential(context: &str, message: &str) {
    for secret in [
        OAUTH_ACCESS_TOKEN,
        CLIENT_SECRET,
        CLIENT_ID,
        SCOPE,
        BEARER_TOK,
    ] {
        assert!(
            !message.contains(secret),
            "{context}: must not carry `{secret}`: {message}"
        );
    }
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[tokio::test]
async fn every_batch_check_failure_names_the_url_and_status_and_no_credential() {
    for class in failure_classes() {
        let stub = lakekeeper_stub(class.content_type, class.status, class.body).await;

        let message = message(
            check(&stub, FIXTURE_PRINCIPAL, &["authz.authz_alpha"])
                .await
                .expect_err(class.name),
        );

        assert!(
            message.contains(&stub.batch_check_url()),
            "{}: must name the batch-check URL: {message}",
            class.name
        );
        assert!(
            message.contains(&format!("HTTP {}", class.status)),
            "{}: must name the status: {message}",
            class.name
        );
        assert!(
            message.contains(class.marker),
            "{}: must carry the answer body: {message}",
            class.name
        );
        assert!(
            message.contains("The catalog must be a Lakekeeper server."),
            "{}: must state that the catalog must be Lakekeeper: {message}",
            class.name
        );
        assert_no_credential(class.name, &message);
    }
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[tokio::test]
async fn a_batch_check_that_cannot_be_sent_names_the_url_and_no_credential() {
    let catalog_uri = CLOSED_PORT_CATALOG_URI.to_string();
    let creds = ConnectionCreds {
        token: Some(BEARER_TOK.into()),
        ..base_creds()
    };
    let session = CatalogSession::resolve(&catalog_uri, "lakehouse_authz", &creds)
        .await
        .expect("a static token needs no grant, and a failed config lookup is not an error");

    let message = message(
        lakekeeper_batch_check(&session, FIXTURE_PRINCIPAL, &["authz.t"], &creds)
            .await
            .expect_err("nothing listens"),
    );

    let url = catalog_uri.replace("/catalog", "/management/v1/action/batch-check");
    assert!(message.contains(&url), "{message}");
    assert!(message.contains("could not be completed"), "{message}");
    assert!(
        message.contains("The catalog must be a Lakekeeper server."),
        "{message}"
    );
    assert_no_credential("transport failure", &message);
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[tokio::test]
async fn a_batch_check_answer_whose_body_breaks_off_names_the_url_and_the_status() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_uri = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            read_request(&mut stream).await;
            let truncated = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                             Content-Length: 100\r\nConnection: close\r\n\r\n{\"results\"";
            let _ = stream.write_all(truncated.as_bytes()).await;
        }
    });
    let creds = ConnectionCreds {
        token: Some(BEARER_TOK.into()),
        ..base_creds()
    };
    let session =
        CatalogSession::resolve(&format!("{base_uri}/catalog"), "lakehouse_authz", &creds)
            .await
            .expect("a static token needs no grant, and a failed config lookup is not an error");

    let message = message(
        lakekeeper_batch_check(&session, FIXTURE_PRINCIPAL, &["authz.t"], &creds)
            .await
            .expect_err("the body breaks off"),
    );

    assert!(
        message.contains(&format!("{base_uri}/management/v1/action/batch-check")),
        "{message}"
    );
    assert!(message.contains("HTTP 200"), "{message}");
    assert_no_credential("broken body", &message);
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[tokio::test]
async fn a_cannot_inspect_answer_names_the_missing_privilege() {
    let (stub, _, result) = replay("cannot-inspect").await;

    let message = message(result.expect_err("a 403 refuses the check"));

    assert!(message.contains(&stub.batch_check_url()), "{message}");
    assert!(message.contains("HTTP 403"), "{message}");
    assert!(message.contains("CannotInspectPermissions"), "{message}");
    assert!(message.contains("can_read_assignments"), "{message}");
    assert!(message.contains("manage_grants"), "{message}");
    assert!(
        !message.contains("must be a Lakekeeper server"),
        "a Lakekeeper that refuses the inspection is not a non-Lakekeeper catalog: {message}"
    );
}

/// Scenario: A failed batch-check refuses the query with an error that names the cause
#[tokio::test]
async fn a_cannot_inspect_answer_redacts_credentials_echoed_in_the_body() {
    let body = json!({"error": {
        "type": "CannotInspectPermissions",
        "message": echoed_secrets(),
    }});
    let stub = json_stub(403, body).await;

    let message = message(
        check(&stub, FIXTURE_PRINCIPAL, &["authz.authz_alpha"])
            .await
            .expect_err("a 403 refuses the check"),
    );

    assert!(message.contains("CannotInspectPermissions"), "{message}");
    assert_no_credential("403 CannotInspectPermissions", &message);
}

fn answer(results: Value) -> String {
    json!({ "results": results }).to_string()
}

#[test]
fn a_body_is_a_batch_check_answer_only_with_one_boolean_per_check_id() {
    let tables = ["a.x", "a.y"];
    let accepted = answer(json!([
        {"id": "read-data-1", "allowed": false},
        {"id": "read-data", "allowed": true},
    ]));
    assert_eq!(
        read_decisions(&tables, &accepted),
        Some(vec![
            TableReadDecision {
                table: "a.x".into(),
                allowed: true
            },
            TableReadDecision {
                table: "a.y".into(),
                allowed: false
            },
        ]),
        "answer order does not matter, check ids do"
    );

    for (case, body) in [
        (
            "a missing check id",
            answer(json!([{"id": "read-data", "allowed": true}])),
        ),
        (
            "a repeated check id",
            answer(json!([
                {"id": "read-data", "allowed": true},
                {"id": "read-data", "allowed": true},
            ])),
        ),
        (
            "an unknown check id",
            answer(json!([
                {"id": "read-data", "allowed": true},
                {"id": "read-data-7", "allowed": true},
            ])),
        ),
        (
            "an extra result",
            answer(json!([
                {"id": "read-data", "allowed": true},
                {"id": "read-data-1", "allowed": true},
                {"id": "read-data-2", "allowed": true},
            ])),
        ),
        (
            "a non-boolean allowed",
            answer(json!([
                {"id": "read-data", "allowed": "true"},
                {"id": "read-data-1", "allowed": true},
            ])),
        ),
        (
            "a null allowed",
            answer(json!([
                {"id": "read-data", "allowed": null},
                {"id": "read-data-1", "allowed": true},
            ])),
        ),
        (
            "a result without allowed",
            answer(json!([{"id": "read-data"}, {"id": "read-data-1", "allowed": true}])),
        ),
        ("no results", answer(json!([]))),
        ("a body without results", "{}".to_string()),
        ("a body that is not JSON", "<html></html>".to_string()),
    ] {
        assert_eq!(read_decisions(&tables, &body), None, "{case}");
    }
}

/// Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog
#[test]
fn management_url_replaces_the_trailing_catalog_segment() {
    for (catalog_uri, expected) in [
        (
            "http://lakekeeper:8181/catalog",
            "http://lakekeeper:8181/management",
        ),
        (
            "http://lakekeeper:8181/catalog/",
            "http://lakekeeper:8181/management",
        ),
        (
            "https://lk.example.com/lk/catalog",
            "https://lk.example.com/lk/management",
        ),
        (
            "https://lk.example.com/catalog/catalog",
            "https://lk.example.com/catalog/management",
        ),
    ] {
        assert_eq!(
            lakekeeper_management_url(catalog_uri).expect(catalog_uri),
            expected,
            "{catalog_uri}"
        );
    }
}

/// Scenario: The check is accepted only for an Iceberg REST catalog URI that ends in /catalog
#[test]
fn management_url_rejects_a_catalog_uri_that_does_not_end_in_catalog() {
    for catalog_uri in [
        "http://lakekeeper:8181",
        "http://lakekeeper:8181/",
        "http://lakekeeper:8181/api/v1",
        "http://lakekeeper:8181/catalog/v1",
        "http://lakekeeper:8181/catalogue",
        "http://lakekeeper:8181/catalog//",
        "http://lakekeeper:8181/catalog?warehouse=w",
        "http://catalog",
        "http://catalog/",
        "catalog",
        "/catalog",
        "",
    ] {
        let message = message(lakekeeper_management_url(catalog_uri).expect_err(catalog_uri));

        assert!(
            message.contains(&format!("'{catalog_uri}'")) && message.contains("'/catalog'"),
            "{catalog_uri}: must name the URI and the required ending: {message}"
        );
    }
}
