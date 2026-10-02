use super::*;

fn substitutions() -> Substitutions {
    Substitutions::new(vec![
        (
            "<warehouse-id>".to_string(),
            "11111111-1111-1111-1111-111111111111".to_string(),
        ),
        (
            "<principal:lakehouse-reader-a>".to_string(),
            "oidc~22222222-2222-2222-2222-222222222222".to_string(),
        ),
        (
            "<table-id:authz_alpha>".to_string(),
            "33333333-3333-3333-3333-333333333333".to_string(),
        ),
    ])
}

fn denied_response(message: &str) -> Value {
    json!({
        "status": 403,
        "body": {
            "error": {
                "message": message,
                "type": "CannotInspectPermissions",
                "code": 403,
                "stack": ["Error ID: <error-id>"]
            }
        }
    })
}

fn allowed_response(allowed: bool) -> Value {
    json!({"status": 200, "body": {"results": [{"id": "read-data", "allowed": allowed}]}})
}

#[test]
fn normalization_turns_live_ids_back_into_placeholders() {
    let live = json!({"message": "Table 33333333-3333-3333-3333-333333333333 in \
        11111111-1111-1111-1111-111111111111 for oidc~22222222-2222-2222-2222-222222222222"});

    let normalized = substitutions().normalize(&live);

    assert_eq!(
        normalized["message"],
        "Table <table-id:authz_alpha> in <warehouse-id> for <principal:lakehouse-reader-a>"
    );
}

#[test]
fn normalization_replaces_the_error_id_value() {
    let live = json!({"stack": ["Error ID: 01a0fbc9-4e6a-7252-8c73-c2511c37fa00", "other"]});

    let normalized = substitutions().normalize(&live);

    assert_eq!(normalized["stack"][0], "Error ID: <error-id>");
    assert_eq!(normalized["stack"][1], "other");
}

#[test]
fn normalization_keeps_an_error_id_prefix_not_followed_by_a_uuid() {
    let normalized = substitutions().normalize(&json!({"m": "Error ID: short"}));

    assert_eq!(normalized["m"], "Error ID: short");
}

#[test]
fn normalization_replaces_every_error_id_in_one_string() {
    let live = json!({"m": "Error ID: 01a0fbc9-4e6a-7252-8c73-c2511c37fa00 then Error ID: 02b0fbc9-4e6a-7252-8c73-c2511c37fa00"});

    let normalized = substitutions().normalize(&live);

    assert_eq!(
        normalized["m"],
        "Error ID: <error-id> then Error ID: <error-id>"
    );
}

#[test]
fn jwt_claims_decodes_the_payload_segment() {
    let token = format!("h.{}.s", URL_SAFE_NO_PAD.encode(r#"{"sub":"abc"}"#));

    assert_eq!(jwt_claims(&token)["sub"], "abc");
}

#[test]
#[should_panic(expected = "three-part JWT")]
fn jwt_claims_rejects_a_token_without_a_payload() {
    jwt_claims("abc");
}

#[test]
#[should_panic(expected = "not base64url")]
fn jwt_claims_rejects_a_payload_that_is_not_base64url() {
    jwt_claims("h.!!!.s");
}

#[test]
#[should_panic(expected = "not JSON")]
fn jwt_claims_rejects_a_payload_that_is_not_json() {
    jwt_claims(&format!("h.{}.s", URL_SAFE_NO_PAD.encode("not json")));
}

#[test]
#[should_panic(expected = "has no result for check")]
fn allowed_panics_when_the_check_has_no_result() {
    Exchange {
        status: 403,
        body: json!({"error": {}}),
    }
    .allowed("read-data");
}

#[test]
fn shape_matches_when_only_a_message_differs() {
    let live = denied_response("a different message");
    let fixture = denied_response("the recorded message");

    assert_eq!(shape_mismatch(&live, &fixture), None);
}

#[test]
fn shape_differs_when_allowed_changes() {
    assert!(shape_mismatch(&allowed_response(true), &allowed_response(false)).is_some());
}

#[test]
fn shape_differs_when_error_type_changes() {
    let mut live = denied_response("m");
    live["body"]["error"]["type"] = json!("UnauthorizedException");

    assert!(shape_mismatch(&live, &denied_response("m")).is_some());
}

#[test]
fn shape_differs_when_error_code_changes() {
    let mut live = denied_response("m");
    live["body"]["error"]["code"] = json!(401);

    assert!(shape_mismatch(&live, &denied_response("m")).is_some());
}

#[test]
fn shape_differs_when_status_changes() {
    let mut live = allowed_response(true);
    live["status"] = json!(404);

    assert!(shape_mismatch(&live, &allowed_response(true)).is_some());
}

#[test]
fn shape_differs_when_a_key_is_added() {
    let mut live = allowed_response(true);
    live["body"]["extra"] = json!(1);

    assert!(shape_mismatch(&live, &allowed_response(true)).is_some());
}

#[test]
fn shape_differs_when_a_key_is_missing() {
    let mut live = allowed_response(true);
    live["body"]["results"][0]
        .as_object_mut()
        .expect("result object")
        .remove("id");

    assert!(shape_mismatch(&live, &allowed_response(true)).is_some());
}

#[test]
fn shape_differs_when_a_value_type_changes() {
    let mut live = denied_response("m");
    live["body"]["error"]["message"] = json!(7);

    assert!(shape_mismatch(&live, &denied_response("m")).is_some());
}

#[test]
fn shape_differs_when_an_array_length_changes() {
    let mut live = allowed_response(true);
    live["body"]["results"]
        .as_array_mut()
        .expect("results array")
        .push(json!({"id": "second", "allowed": true}));

    assert!(shape_mismatch(&live, &allowed_response(true)).is_some());
}

#[test]
fn leak_scan_flags_secrets_tokens_and_live_ids() {
    let subs = substitutions();
    let text = "secret lakehouse-reader-a-secret, token eyJhbGciOi, wh \
                11111111-1111-1111-1111-111111111111";

    let leaks = fixture_leaks(text, &subs);

    assert_eq!(leaks.len(), 3, "{leaks:?}");
}

#[test]
fn leak_scan_passes_a_normalized_fixture() {
    let text = r#"{"user": "<principal:lakehouse-reader-a>", "wh": "<warehouse-id>"}"#;

    assert!(fixture_leaks(text, &substitutions()).is_empty());
}
