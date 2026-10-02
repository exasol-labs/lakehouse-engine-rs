//! Permission fixture for the Lakekeeper `lakekeeper-e2e` suite: OpenFGA-backed grants for
//! three test principals, `batch-check` calls, and the fixture codec.
//! Helpers panic, never skip, and never put a secret or token in a panic message.
#![cfg(feature = "lakekeeper-e2e")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

use super::lakekeeper::{
    self, WAREHOUSE_AUTHZ, WarehouseProfile, http_client, keycloak_client_credentials_token,
    keycloak_client_credentials_token_for, management_base,
};

pub const AUTHZ_NAMESPACE: &str = "authz";
pub const TABLE_ALPHA: &str = "authz_alpha";
pub const TABLE_BETA: &str = "authz_beta";
pub const TABLE_MISSING: &str = "authz_missing";

/// The id every batch check carries, so a denied and a missing table answer identically.
pub const CHECK_ID: &str = "read-data";

const RECREATE_HINT: &str = "Lakekeeper does not rebuild grants when its authz backend changes \
    after bootstrap; recreate the stack with `docker compose ... down -v`";

pub struct Principal {
    pub client_id: &'static str,
    pub client_secret: &'static str,
}

impl Principal {
    pub fn token(&self) -> String {
        keycloak_client_credentials_token_for(self.client_id, self.client_secret)
    }
}

pub const OPERATOR: Principal = Principal {
    client_id: lakekeeper::OAUTH_CLIENT_ID,
    client_secret: lakekeeper::OAUTH_CLIENT_SECRET,
};
pub const READER_A: Principal = Principal {
    client_id: "lakehouse-reader-a",
    client_secret: "lakehouse-reader-a-secret",
};
pub const READER_B: Principal = Principal {
    client_id: "lakehouse-reader-b",
    client_secret: "lakehouse-reader-b-secret",
};
pub const CHECKER: Principal = Principal {
    client_id: "lakehouse-checker",
    client_secret: "lakehouse-checker-secret",
};
const PRINCIPALS: [&Principal; 4] = [&OPERATOR, &READER_A, &READER_B, &CHECKER];

/// A status and a body; a body that is not JSON (a 422 is plain text) is a JSON string.
pub struct Exchange {
    pub status: u16,
    pub body: Value,
}

impl Exchange {
    pub fn to_value(&self) -> Value {
        json!({"status": self.status, "body": self.body})
    }

    pub fn allowed(&self, check_id: &str) -> bool {
        self.body["results"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|r| r["id"].as_str() == Some(check_id))
            .and_then(|r| r["allowed"].as_bool())
            .unwrap_or_else(|| {
                panic!(
                    "batch-check answer (status {}) has no result for check '{check_id}': {}",
                    self.status, self.body
                )
            })
    }

    pub fn error_type(&self) -> Option<&str> {
        self.body["error"]["type"].as_str()
    }
}

fn get(url: &str, token: &str) -> Exchange {
    exchange(
        http_client().get(url).bearer_auth(token),
        &format!("GET {url}"),
    )
}

fn post(url: &str, token: &str, body: &Value) -> Exchange {
    exchange(
        http_client().post(url).bearer_auth(token).json(body),
        &format!("POST {url}"),
    )
}

fn exchange(request: reqwest::blocking::RequestBuilder, label: &str) -> Exchange {
    let resp = request
        .send()
        .unwrap_or_else(|e| panic!("Lakekeeper {label} failed to send: {e}"));
    let status = resp.status().as_u16();
    let text = resp.text().unwrap_or_else(|e| {
        panic!("Lakekeeper {label} answered {status} with an unreadable body: {e}")
    });
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    Exchange { status, body }
}

fn expect_status(exchange: &Exchange, what: &str, accepted: &[u16]) {
    if accepted.contains(&exchange.status) {
        return;
    }
    if exchange.status == 403 {
        panic!("{what} returned 403: {}; {RECREATE_HINT}", exchange.body);
    }
    panic!(
        "{what} returned {} (expected one of {accepted:?}): {}",
        exchange.status, exchange.body
    );
}

pub fn lakekeeper_server_info() -> Value {
    let url = format!("{}/info", management_base());
    let exchange = get(&url, &keycloak_client_credentials_token());
    expect_status(&exchange, "Lakekeeper GET /info", &[200]);
    exchange.body
}

pub fn jwt_claims(token: &str) -> Value {
    let payload = token
        .split('.')
        .nth(1)
        .unwrap_or_else(|| panic!("access token is not a three-part JWT"));
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .unwrap_or_else(|e| panic!("JWT payload is not base64url: {e}"));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("JWT payload is not JSON: {e}"))
}

pub fn whoami_id(principal: &Principal) -> String {
    let url = format!("{}/whoami", management_base());
    let exchange = get(&url, &principal.token());
    expect_status(&exchange, "Lakekeeper GET /whoami", &[200]);
    exchange.body["id"]
        .as_str()
        .unwrap_or_else(|| panic!("Lakekeeper whoami answered no id: {}", exchange.body))
        .to_string()
}

/// A principal exists in Lakekeeper only after it registers itself.
fn register(principal: &Principal) {
    let url = format!("{}/user", management_base());
    let exchange = post(&url, &principal.token(), &json!({}));
    expect_status(
        &exchange,
        &format!("Lakekeeper register of '{}'", principal.client_id),
        &[201, 409],
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Server,
    Project,
    Warehouse,
    Namespace,
    Table(&'static str),
}

#[derive(Clone, Copy)]
pub struct Grant {
    pub scope: Scope,
    pub relation: &'static str,
}

const ALL_SCOPES: [Scope; 6] = [
    Scope::Server,
    Scope::Project,
    Scope::Warehouse,
    Scope::Namespace,
    Scope::Table(TABLE_ALPHA),
    Scope::Table(TABLE_BETA),
];

pub struct AuthzFixture {
    pub warehouse_id: String,
    pub namespace_id: String,
    table_ids: BTreeMap<&'static str, String>,
    principal_ids: BTreeMap<&'static str, String>,
}

impl AuthzFixture {
    pub fn principal_id(&self, principal: &Principal) -> &str {
        &self.principal_ids[principal.client_id]
    }

    pub fn table_id(&self, table: &str) -> &str {
        self.table_ids
            .get(table)
            .unwrap_or_else(|| panic!("'{table}' is not a fixture table"))
    }

    pub fn substitutions(&self) -> Substitutions {
        let mut pairs = vec![
            ("<warehouse-id>".to_string(), self.warehouse_id.clone()),
            ("<namespace-id>".to_string(), self.namespace_id.clone()),
        ];
        for (table, id) in &self.table_ids {
            pairs.push((format!("<table-id:{table}>"), id.clone()));
        }
        for (client_id, id) in &self.principal_ids {
            pairs.push((format!("<principal:{client_id}>"), id.clone()));
        }
        Substitutions::new(pairs)
    }

    pub fn read_check(&self, principal: &Principal, table: &str) -> Value {
        self.read_check_for_user(CHECK_ID, self.principal_id(principal), table)
    }

    pub fn read_check_for_user(&self, check_id: &str, user_id: &str, table: &str) -> Value {
        json!({
            "id": check_id,
            "identity": {"user": user_id},
            "operation": {"table": {
                "warehouse-id": self.warehouse_id,
                "namespace": [AUTHZ_NAMESPACE],
                "table": table,
                "action": {"action": "read_data"},
            }},
        })
    }

    fn assignments_url(&self, scope: Scope) -> String {
        let base = management_base();
        let wh = &self.warehouse_id;
        match scope {
            Scope::Server => format!("{base}/permissions/server/assignments"),
            Scope::Project => format!("{base}/permissions/project/assignments"),
            Scope::Warehouse => format!("{base}/permissions/warehouse/{wh}/assignments"),
            Scope::Namespace => {
                format!(
                    "{base}/permissions/namespace/{}/assignments",
                    self.namespace_id
                )
            }
            Scope::Table(table) => format!(
                "{base}/permissions/warehouse/{wh}/table/{}/assignments",
                self.table_id(table)
            ),
        }
    }

    fn relations_of(&self, scope: Scope, user_id: &str) -> Vec<String> {
        let url = self.assignments_url(scope);
        let exchange = get(&url, &keycloak_client_credentials_token());
        expect_status(
            &exchange,
            &format!("Lakekeeper GET {url} for user '{user_id}'"),
            &[200],
        );
        exchange.body["assignments"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|a| a["user"].as_str() == Some(user_id))
            .filter_map(|a| a["type"].as_str().map(str::to_string))
            .collect()
    }

    fn update_assignments(&self, scope: Scope, user_id: &str, diff: &AssignmentDiff) {
        if diff.writes.is_empty() && diff.deletes.is_empty() {
            return;
        }
        let assignment = |relation: &&str| json!({"type": relation, "user": user_id});
        let body = json!({
            "writes": diff.writes.iter().map(assignment).collect::<Vec<_>>(),
            "deletes": diff.deletes.iter().map(assignment).collect::<Vec<_>>(),
        });
        let url = self.assignments_url(scope);
        let exchange = post(&url, &keycloak_client_credentials_token(), &body);
        expect_status(
            &exchange,
            &format!("Lakekeeper POST {url} for user '{user_id}'"),
            &[200, 204],
        );
    }

    /// Reads first and writes only the absent grant, so a repeated run changes nothing.
    pub fn ensure_grant(&self, user_id: &str, grant: Grant) {
        let held = self.relations_of(grant.scope, user_id);
        if !held.iter().any(|r| r == grant.relation) {
            let diff = AssignmentDiff {
                writes: vec![grant.relation],
                deletes: vec![],
            };
            self.update_assignments(grant.scope, user_id, &diff);
        }
    }

    /// Replaces the checker's grants with exactly `grants`; a no-op when it already holds them.
    pub fn set_checker_assignments(&self, grants: &[Grant]) {
        let user_id = self.principal_id(&CHECKER);
        for scope in ALL_SCOPES {
            let wanted: Vec<&str> = grants
                .iter()
                .filter(|g| g.scope == scope)
                .map(|g| g.relation)
                .collect();
            let held = self.relations_of(scope, user_id);
            let deletes: Vec<&str> = held
                .iter()
                .map(String::as_str)
                .filter(|r| !wanted.contains(r))
                .collect();
            let writes: Vec<&str> = wanted
                .iter()
                .copied()
                .filter(|r| !held.iter().any(|h| h == r))
                .collect();
            self.update_assignments(scope, user_id, &AssignmentDiff { writes, deletes });
        }
    }
}

struct AssignmentDiff<'a> {
    writes: Vec<&'a str>,
    deletes: Vec<&'a str>,
}

fn catalog_url(warehouse_id: &str) -> String {
    format!("{}/v1/{warehouse_id}", lakekeeper::catalog_uri_host())
}

fn warehouse_id_by_name(name: &str) -> String {
    let url = format!("{}/warehouse", management_base());
    let exchange = get(&url, &keycloak_client_credentials_token());
    expect_status(&exchange, "Lakekeeper GET /warehouse", &[200]);
    exchange.body["warehouses"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|w| w["name"].as_str() == Some(name))
        .and_then(|w| w["id"].as_str())
        .unwrap_or_else(|| panic!("Lakekeeper lists no warehouse named '{name}'"))
        .to_string()
}

fn ensure_namespace(warehouse_id: &str) -> String {
    let token = keycloak_client_credentials_token();
    let base = catalog_url(warehouse_id);
    let create = post(
        &format!("{base}/namespaces"),
        &token,
        &json!({"namespace": [AUTHZ_NAMESPACE]}),
    );
    expect_status(&create, "Lakekeeper create namespace", &[200, 409]);
    let fetched = get(&format!("{base}/namespaces/{AUTHZ_NAMESPACE}"), &token);
    expect_status(&fetched, "Lakekeeper GET namespace", &[200]);
    fetched.body["properties"]["namespace_id"]
        .as_str()
        .unwrap_or_else(|| panic!("Lakekeeper namespace answered no namespace_id"))
        .to_string()
}

/// Metadata-only: no data file is written, since only the table id matters to a check.
fn ensure_table(warehouse_id: &str, table: &str) -> String {
    let token = keycloak_client_credentials_token();
    let base = catalog_url(warehouse_id);
    let create = post(
        &format!("{base}/namespaces/{AUTHZ_NAMESPACE}/tables"),
        &token,
        &json!({
            "name": table,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}],
            },
        }),
    );
    expect_status(
        &create,
        &format!("Lakekeeper create table '{table}'"),
        &[200, 409],
    );
    let fetched = get(
        &format!("{base}/namespaces/{AUTHZ_NAMESPACE}/tables/{table}"),
        &token,
    );
    expect_status(&fetched, &format!("Lakekeeper GET table '{table}'"), &[200]);
    fetched.body["metadata"]["table-uuid"]
        .as_str()
        .unwrap_or_else(|| panic!("Lakekeeper table '{table}' answered no table-uuid"))
        .to_string()
}

fn provision_authz_fixture() -> AuthzFixture {
    lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::authz());
    let warehouse_id = warehouse_id_by_name(WAREHOUSE_AUTHZ);

    let mut principal_ids = BTreeMap::new();
    for principal in PRINCIPALS {
        register(principal);
        principal_ids.insert(principal.client_id, whoami_id(principal));
    }

    let namespace_id = ensure_namespace(&warehouse_id);
    let table_ids = [TABLE_ALPHA, TABLE_BETA]
        .into_iter()
        .map(|table| (table, ensure_table(&warehouse_id, table)))
        .collect();

    let fixture = AuthzFixture {
        warehouse_id,
        namespace_id,
        table_ids,
        principal_ids,
    };
    let select = |table| Grant {
        scope: Scope::Table(table),
        relation: "select",
    };
    fixture.ensure_grant(fixture.principal_id(&READER_A), select(TABLE_ALPHA));
    fixture.ensure_grant(fixture.principal_id(&READER_B), select(TABLE_BETA));
    fixture
}

/// Idempotent: warehouse `lakehouse_authz`, namespace `authz`, tables `authz_alpha` and
/// `authz_beta`, and `select` on the first for reader A and on the second for reader B.
/// The server must already be bootstrapped.
pub fn ensure_authz_fixture() -> &'static AuthzFixture {
    static FIXTURE: OnceLock<AuthzFixture> = OnceLock::new();
    FIXTURE.get_or_init(provision_authz_fixture)
}

pub fn batch_check_request(checks: &[Value]) -> Value {
    json!({"checks": checks, "error-on-not-found": false})
}

pub fn post_batch_check(caller_token: &str, request: &Value) -> Exchange {
    let url = format!("{}/action/batch-check", management_base());
    post(&url, caller_token, request)
}

pub fn batch_check(caller_token: &str, checks: &[Value]) -> Exchange {
    post_batch_check(caller_token, &batch_check_request(checks))
}

/// Maps between live ids and the placeholders a committed fixture carries.
pub struct Substitutions {
    pairs: Vec<(String, String)>,
}

const ERROR_ID_PREFIX: &str = "Error ID: ";
const UUID_LEN: usize = 36;

impl Substitutions {
    /// Each pair is `(placeholder, live value)`.
    pub fn new(pairs: Vec<(String, String)>) -> Self {
        Substitutions { pairs }
    }

    pub fn normalize(&self, live: &Value) -> Value {
        let mut text = live.to_string();
        for (placeholder, live_value) in &self.pairs {
            text = text.replace(live_value, placeholder);
        }
        reparse(&replace_error_ids(&text))
    }

    fn live_values(&self) -> impl Iterator<Item = &str> {
        self.pairs.iter().map(|(_, live)| live.as_str())
    }
}

fn reparse(text: &str) -> Value {
    serde_json::from_str(text).expect("a substituted JSON document stays valid JSON")
}

/// Lakekeeper stamps a fresh `Error ID: <uuid>` on every error.
fn replace_error_ids(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(ERROR_ID_PREFIX) {
        let after = at + ERROR_ID_PREFIX.len();
        out.push_str(&rest[..after]);
        let id = rest.get(after..after + UUID_LEN);
        if id.is_some_and(|id| id.chars().all(|c| c.is_ascii_hexdigit() || c == '-')) {
            out.push_str("<error-id>");
            rest = &rest[after + UUID_LEN..];
        } else {
            rest = &rest[after..];
        }
    }
    out.push_str(rest);
    out
}

/// `None` when `live` and `fixture` agree on keys and JSON value types at every depth, and
/// on `status`, every `allowed`, and `error.type` and `error.code`. Message text may differ.
/// `live` and `fixture` are `Exchange::to_value` documents (`{status, body}`); `status` is
/// compared exactly only at the root.
pub fn shape_mismatch(live: &Value, fixture: &Value) -> Option<String> {
    shape_diff(live, fixture, "$")
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn is_exact_path(path: &str) -> bool {
    path == "$.status"
        || path.ends_with(".allowed")
        || path.ends_with(".error.type")
        || path.ends_with(".error.code")
}

fn shape_diff(live: &Value, fixture: &Value, path: &str) -> Option<String> {
    match (live, fixture) {
        (Value::Object(l), Value::Object(f)) => {
            let live_keys: Vec<&String> = l.keys().collect();
            let fixture_keys: Vec<&String> = f.keys().collect();
            if live_keys != fixture_keys {
                return Some(format!(
                    "{path}: keys differ, live {live_keys:?} vs fixture {fixture_keys:?}"
                ));
            }
            f.iter()
                .find_map(|(key, fv)| shape_diff(&l[key], fv, &format!("{path}.{key}")))
        }
        (Value::Array(l), Value::Array(f)) => {
            if l.len() != f.len() {
                return Some(format!(
                    "{path}: array length {} vs fixture {}",
                    l.len(),
                    f.len()
                ));
            }
            l.iter()
                .zip(f)
                .find_map(|(lv, fv)| shape_diff(lv, fv, &format!("{path}[]")))
        }
        _ => {
            if value_type(live) != value_type(fixture) {
                return Some(format!(
                    "{path}: type {} vs fixture {}",
                    value_type(live),
                    value_type(fixture)
                ));
            }
            (is_exact_path(path) && live != fixture)
                .then(|| format!("{path}: value {live} vs fixture {fixture}"))
        }
    }
}

/// Everything a committed fixture must not carry: client secrets, a token prefix, live ids.
pub fn fixture_leaks(text: &str, substitutions: &Substitutions) -> Vec<String> {
    let mut leaks = Vec::new();
    for principal in PRINCIPALS {
        if text.contains(principal.client_secret) {
            leaks.push(format!("the secret of '{}'", principal.client_id));
        }
    }
    if text.contains("eyJ") {
        leaks.push("a token prefix".to_string());
    }
    for live in substitutions.live_values() {
        if text.contains(live) {
            leaks.push(format!("the live value '{live}'"));
        }
    }
    leaks
}

pub const CAPTURE_VARIABLE: &str = "LH_LAKEKEEPER_FIXTURE_CAPTURE";

pub fn capture_requested() -> bool {
    std::env::var(CAPTURE_VARIABLE).is_ok_and(|v| v == "1")
}

fn fixture_path(case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../lakehouse-catalog/tests/fixtures/lakekeeper/batch-check")
        .join(format!("{case}.json"))
}

pub fn write_fixture(case: &str, fixture: &Value) {
    let path = fixture_path(case);
    std::fs::create_dir_all(path.parent().expect("fixture directory"))
        .unwrap_or_else(|e| panic!("create fixture directory for '{case}': {e}"));
    let text = serde_json::to_string_pretty(fixture).expect("fixture serializes") + "\n";
    std::fs::write(&path, text).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
}

pub fn read_fixture_text(case: &str) -> String {
    let path = fixture_path(case);
    std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "fixture {} is missing ({e}); capture it with \
             `{CAPTURE_VARIABLE}=1 make test-e2e-lakekeeper`",
            path.display()
        )
    })
}

#[cfg(test)]
#[path = "lakekeeper_authz_tests.rs"]
mod tests;
