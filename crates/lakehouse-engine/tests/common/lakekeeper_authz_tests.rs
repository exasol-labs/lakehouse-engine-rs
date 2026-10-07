use super::*;

const WAREHOUSE: &str = "11111111-1111-1111-1111-111111111111";
const READER_A_ID: &str = "oidc~22222222-2222-2222-2222-222222222222";
const ALPHA_ID: &str = "33333333-3333-3333-3333-333333333333";
const AUTHZ_NAMESPACE_ID: &str = "44444444-4444-4444-4444-444444444444";
const E2E_NAMESPACE_ID: &str = "55555555-5555-5555-5555-555555555555";

fn fixture() -> AuthzFixture {
    AuthzFixture {
        warehouse_id: WAREHOUSE.to_string(),
        namespace_ids: BTreeMap::from([
            (AUTHZ_NAMESPACE, AUTHZ_NAMESPACE_ID.to_string()),
            (E2E_NAMESPACE, E2E_NAMESPACE_ID.to_string()),
        ]),
        table_ids: BTreeMap::from([(TABLE_ALPHA, ALPHA_ID.to_string())]),
        principal_ids: BTreeMap::from([(READER_A.client_id, READER_A_ID.to_string())]),
    }
}

/// A grant inherited from the namespace of any fixture table would outlive a reconciliation
/// that skips that namespace.
#[test]
fn reconciliation_covers_the_namespace_of_every_fixture_table() {
    let fixture = fixture();
    let reconciled: Vec<String> = ALL_SCOPES
        .iter()
        .filter(|scope| matches!(scope, Scope::Namespace(_)))
        .map(|scope| fixture.assignments_url(*scope))
        .collect();

    for namespace_id in [AUTHZ_NAMESPACE_ID, E2E_NAMESPACE_ID] {
        let url = format!(
            "{}/permissions/namespace/{namespace_id}/assignments",
            management_base()
        );
        assert!(reconciled.contains(&url), "{url} in {reconciled:?}");
    }
}

#[test]
fn normalize_turns_live_ids_back_into_placeholders() {
    let live = json!({"request": {"identity": READER_A_ID, "warehouse-id": WAREHOUSE},
        "response": {"body": {"error": {"message": format!("table {ALPHA_ID}")}}}});

    let normalized = fixture().normalize(&live);

    assert_eq!(
        normalized,
        json!({"request": {"identity": "<principal:lakehouse-reader-a>",
            "warehouse-id": "<warehouse-id>"},
            "response": {"body": {"error": {"message": "table <table-id:authz_alpha>"}}}})
    );
}

#[test]
fn normalize_replaces_the_error_id_value_only() {
    let live = json!({"response": {"body": {"error": {"stack": [
        "Error ID: 01a0fbc9-4e6a-7252-8c73-c2511c37fa00", "other"]}}}});

    let normalized = fixture().normalize(&live);

    assert_eq!(
        normalized["response"]["body"]["error"]["stack"],
        json!(["Error ID: <error-id>", "other"])
    );
}
