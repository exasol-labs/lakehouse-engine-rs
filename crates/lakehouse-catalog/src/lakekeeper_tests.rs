use super::*;
use serde_json::{Value, json};

const WAREHOUSE_ID: &str = "5c25f9c4-be40-11f1-a87d-17102559a460";
const PRINCIPAL: &str = "oidc~lakehouse-reader-a@corp.net";

fn recorded(text: &str) -> Value {
    let text = text
        .replace("<warehouse-id>", WAREHOUSE_ID)
        .replace("<principal:lakehouse-reader-a>", PRINCIPAL);
    serde_json::from_str(&text).expect("a fixture is one JSON object")
}

/// Scenario: One batch-check per query decides every table before any table is read
#[test]
fn the_request_equals_the_one_recorded_from_lakekeeper() {
    let fixture = recorded(include_str!(
        "../tests/fixtures/lakekeeper/batch-check/allowed.json"
    ));

    let request = build_batch_check(PRINCIPAL, &["authz.authz_alpha"], WAREHOUSE_ID).unwrap();

    assert_eq!(serde_json::to_value(request).unwrap(), fixture["request"]);
}

#[test]
fn each_table_gets_its_own_check_id_and_a_nested_namespace_is_split() {
    let request = build_batch_check(PRINCIPAL, &["a.b.t", "c.u"], WAREHOUSE_ID).unwrap();

    let json = serde_json::to_value(request).unwrap();
    let ids: Vec<&str> = json["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| check["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["read-data", "read-data-1"]);
    assert_eq!(
        json["checks"][0]["operation"]["table"]["namespace"],
        json!(["a", "b"])
    );
}

#[test]
fn a_table_without_a_namespace_is_refused() {
    assert!(build_batch_check(PRINCIPAL, &["bare"], WAREHOUSE_ID).is_err());
}

#[test]
fn repeated_tables_are_checked_once_in_first_seen_order() {
    assert_eq!(
        distinct_in_first_seen_order(&["a.t", "b.u", "a.t"]),
        ["a.t", "b.u"]
    );
}

/// Scenario: A denied table refuses the whole query
#[test]
fn recorded_answers_read_as_allowed_or_denied() {
    for (text, allowed) in [
        (
            include_str!("../tests/fixtures/lakekeeper/batch-check/allowed.json"),
            true,
        ),
        (
            include_str!("../tests/fixtures/lakekeeper/batch-check/denied.json"),
            false,
        ),
    ] {
        let body = recorded(text)["response"]["body"].to_string();

        let decisions = read_decisions(&["authz.authz_alpha"], &body).unwrap();

        assert_eq!(decisions[0].allowed, allowed);
    }
}

#[test]
fn an_answer_is_read_only_when_it_holds_one_boolean_per_check() {
    let tables = ["a.t", "b.u"];
    let good = json!({"results": [
        {"id": "read-data", "allowed": true},
        {"id": "read-data-1", "allowed": false}
    ]});
    let decisions = read_decisions(&tables, &good.to_string()).unwrap();
    assert_eq!(
        decisions.iter().map(|d| d.allowed).collect::<Vec<_>>(),
        [true, false]
    );

    let missing = json!({"results": [{"id": "read-data", "allowed": true}]});
    let duplicate = json!({"results": [
        {"id": "read-data", "allowed": true},
        {"id": "read-data", "allowed": true}
    ]});
    let not_boolean = json!({"results": [
        {"id": "read-data", "allowed": "yes"},
        {"id": "read-data-1", "allowed": true}
    ]});
    for body in [missing, duplicate, not_boolean, json!({"error": {}})] {
        assert!(
            read_decisions(&tables, &body.to_string()).is_none(),
            "{body}"
        );
    }
}

#[test]
fn a_cannot_inspect_refusal_names_the_missing_privilege() {
    let fixture = recorded(include_str!(
        "../tests/fixtures/lakekeeper/batch-check/cannot-inspect.json"
    ));
    let body = fixture["response"]["body"].to_string();

    let UdfError::User(message) = refusal("http://lk/management", 403, &body) else {
        panic!("expected a user error");
    };

    assert!(message.contains("can_read_assignments"), "{message}");
}

#[test]
fn the_management_url_replaces_the_trailing_catalog_segment() {
    assert_eq!(
        lakekeeper_management_url("https://lk.example:8181/catalog/").unwrap(),
        "https://lk.example:8181/management"
    );
}

#[test]
fn a_catalog_uri_without_a_trailing_catalog_segment_is_refused() {
    for uri in [
        "https://lk.example/api/v1",
        "https://lk.example/catalog?x=1",
        "https://lk.example/catalog#/catalog",
    ] {
        assert!(lakekeeper_management_url(uri).is_err(), "{uri}");
    }
}
