//! Permission fixture for the Lakekeeper `lakekeeper-e2e` suite: OpenFGA-backed grants for
//! the test principals and for the ids the permission check's `USER_MAPPING` derives,
//! `batch-check` calls, and the fixture normalizer.
//! Helpers panic, never skip, and never put a secret or token in a panic message.
#![cfg(feature = "lakekeeper-e2e")]

use std::collections::BTreeMap;
use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

use super::lakekeeper::{
    self, WAREHOUSE_AUTHZ, WarehouseProfile, http_client, keycloak_client_credentials_token,
    keycloak_client_credentials_token_for, management_base,
};
use super::seed::{E2E_NAMESPACE, E2E_TABLE, SeedCatalogAuth, seed_events_table_with_auth};

pub const AUTHZ_NAMESPACE: &str = "authz";
pub const TABLE_ALPHA: &str = "authz_alpha";
pub const TABLE_BETA: &str = "authz_beta";
pub const TABLE_MISSING: &str = "authz_missing";

/// The ids the permission-check test's `USER_MAPPING` derives from its three Exasol users.
pub const MAPPED_ALLOWED: &str = "oidc~lk.allowed@lakehouse.test";
pub const MAPPED_DENIED: &str = "oidc~lk.denied@lakehouse.test";
pub const MAPPED_UNKNOWN: &str = "oidc~lk.unknown@lakehouse.test";

const ERROR_ID_PREFIX: &str = "Error ID: ";

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

const ALL_SCOPES: [Scope; 7] = [
    Scope::Server,
    Scope::Project,
    Scope::Warehouse,
    Scope::Namespace,
    Scope::Table(TABLE_ALPHA),
    Scope::Table(TABLE_BETA),
    Scope::Table(E2E_TABLE),
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

    fn placeholders(&self) -> Vec<(String, &str)> {
        let mut pairs = vec![("<warehouse-id>".to_string(), self.warehouse_id.as_str())];
        for (table, id) in &self.table_ids {
            pairs.push((format!("<table-id:{table}>"), id));
        }
        for (client_id, id) in &self.principal_ids {
            pairs.push((format!("<principal:{client_id}>"), id));
        }
        pairs
    }

    /// Turns a fixture document (`{case, caller, request, response}`) into its committed form:
    /// the live warehouse, table and principal ids and the `Error ID:` value become placeholders.
    pub fn normalize(&self, live: &Value) -> Value {
        let mut text = live.to_string();
        for (placeholder, id) in self.placeholders() {
            text = text.replace(id, &placeholder);
        }
        let mut document: Value =
            serde_json::from_str(&text).expect("a substituted JSON document stays valid JSON");
        let stack = document.pointer_mut("/response/body/error/stack");
        for line in stack.and_then(Value::as_array_mut).into_iter().flatten() {
            if line
                .as_str()
                .is_some_and(|l| l.starts_with(ERROR_ID_PREFIX))
            {
                *line = json!(format!("{ERROR_ID_PREFIX}<error-id>"));
            }
        }
        document
    }

    pub fn carries_secret_or_live_id(&self, text: &str) -> bool {
        let secrets = PRINCIPALS.iter().map(|p| p.client_secret);
        let live_ids = self.placeholders().into_iter().map(|(_, id)| id);
        secrets
            .chain(live_ids)
            .chain(["eyJ"])
            .any(|forbidden| text.contains(forbidden))
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

    fn relations_of(&self, token: &str, scope: Scope, user_id: &str) -> Vec<String> {
        let url = self.assignments_url(scope);
        let exchange = get(&url, token);
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

    fn update_assignments(&self, token: &str, scope: Scope, user_id: &str, diff: &AssignmentDiff) {
        if diff.writes.is_empty() && diff.deletes.is_empty() {
            return;
        }
        let assignment = |relation: &&str| json!({"type": relation, "user": user_id});
        let body = json!({
            "writes": diff.writes.iter().map(assignment).collect::<Vec<_>>(),
            "deletes": diff.deletes.iter().map(assignment).collect::<Vec<_>>(),
        });
        let url = self.assignments_url(scope);
        let exchange = post(&url, token, &body);
        expect_status(
            &exchange,
            &format!("Lakekeeper POST {url} for user '{user_id}'"),
            &[200, 204],
        );
    }

    /// Reads first and writes only the absent grant, so a repeated run changes nothing.
    pub fn ensure_grant(&self, user_id: &str, grant: Grant) {
        let token = keycloak_client_credentials_token();
        let held = self.relations_of(&token, grant.scope, user_id);
        if !held.iter().any(|r| r == grant.relation) {
            let diff = AssignmentDiff {
                writes: vec![grant.relation],
                deletes: vec![],
            };
            self.update_assignments(&token, grant.scope, user_id, &diff);
        }
    }

    /// Replaces the principal's grants with exactly `grants`; a no-op when it already holds them.
    pub fn set_assignments(&self, principal: &Principal, grants: &[Grant]) {
        self.set_user_assignments(self.principal_id(principal), grants);
    }

    /// [`Self::set_assignments`] for a Lakekeeper id that need never have logged in.
    pub fn set_user_assignments(&self, user_id: &str, grants: &[Grant]) {
        let token = keycloak_client_credentials_token();
        for scope in ALL_SCOPES {
            let wanted: Vec<&str> = grants
                .iter()
                .filter(|g| g.scope == scope)
                .map(|g| g.relation)
                .collect();
            let held = self.relations_of(&token, scope, user_id);
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
            self.update_assignments(&token, scope, user_id, &AssignmentDiff { writes, deletes });
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
    let create = post(
        &format!(
            "{}/namespaces/{AUTHZ_NAMESPACE}/tables",
            catalog_url(warehouse_id)
        ),
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
    table_uuid(warehouse_id, AUTHZ_NAMESPACE, table)
}

fn table_uuid(warehouse_id: &str, namespace: &str, table: &str) -> String {
    let url = format!(
        "{}/namespaces/{namespace}/tables/{table}",
        catalog_url(warehouse_id)
    );
    let fetched = get(&url, &keycloak_client_credentials_token());
    expect_status(
        &fetched,
        &format!("Lakekeeper GET table '{namespace}.{table}'"),
        &[200],
    );
    fetched.body["metadata"]["table-uuid"]
        .as_str()
        .unwrap_or_else(|| panic!("Lakekeeper table '{namespace}.{table}' answered no table-uuid"))
        .to_string()
}

/// The permission-check test scans this table, so unlike the fixture tables it holds data.
/// Seeding is idempotent, so the table and its id survive across runs.
fn ensure_seeded_events_table(warehouse_id: &str) -> String {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for seeding the authz warehouse");
    let auth = SeedCatalogAuth {
        token: Some(keycloak_client_credentials_token()),
        ..Default::default()
    };
    runtime
        .block_on(seed_events_table_with_auth(
            &lakekeeper::catalog_uri_host(),
            WAREHOUSE_AUTHZ,
            auth,
        ))
        .unwrap_or_else(|e| {
            panic!("seed events into Lakekeeper warehouse '{WAREHOUSE_AUTHZ}': {e:#}")
        });
    table_uuid(warehouse_id, E2E_NAMESPACE, E2E_TABLE)
}

/// Not cached: every call re-reconciles the grants. Prefer `ensure_authz_fixture`.
pub fn provision_authz_fixture() -> AuthzFixture {
    lakekeeper::lakekeeper_create_warehouse(&WarehouseProfile::authz());
    let warehouse_id = warehouse_id_by_name(WAREHOUSE_AUTHZ);

    let mut principal_ids = BTreeMap::new();
    for principal in PRINCIPALS {
        register(principal);
        principal_ids.insert(principal.client_id, whoami_id(principal));
    }

    let namespace_id = ensure_namespace(&warehouse_id);
    let mut table_ids: BTreeMap<&'static str, String> = [TABLE_ALPHA, TABLE_BETA]
        .into_iter()
        .map(|table| (table, ensure_table(&warehouse_id, table)))
        .collect();
    table_ids.insert(E2E_TABLE, ensure_seeded_events_table(&warehouse_id));

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
    fixture.set_assignments(&READER_A, &[select(TABLE_ALPHA)]);
    fixture.set_assignments(&READER_B, &[select(TABLE_BETA)]);
    fixture.set_user_assignments(MAPPED_ALLOWED, &[select(E2E_TABLE)]);
    fixture.set_user_assignments(MAPPED_DENIED, &[select(TABLE_ALPHA)]);
    fixture.set_user_assignments(MAPPED_UNKNOWN, &[]);
    fixture
}

/// Idempotent; leaves each reader with exactly one table select in the fixture's scopes.
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

#[cfg(test)]
#[path = "lakekeeper_authz_tests.rs"]
mod tests;
