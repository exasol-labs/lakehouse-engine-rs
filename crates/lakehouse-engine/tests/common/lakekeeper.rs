//! Lakekeeper + Keycloak provisioning helpers for the `lakekeeper-e2e` suite.
//!
//! An OpenID-secured (Keycloak) multi-warehouse Lakekeeper Iceberg REST catalog
//! backed by the base stack's MinIO. These helpers run host-side: they wait for
//! the two new services, obtain a Keycloak client-credentials bearer token for
//! Lakekeeper's management API, bootstrap the server, create the static- and
//! vended-credential warehouses, and build the CONNECTION password the UDF uses
//! at query time.
//!
//! Every value here is the source-of-truth documented in the header comment of
//! `docker-compose.lakekeeper.yml`; keep the two in sync.
//!
//! Fail-loud, never-skip: readiness waits and management calls panic (never
//! return `Err`) when the stack is unavailable — per project rules.
//!
//! Credential safety: neither the client secret, the obtained access token, nor
//! any S3 secret is ever embedded in a panic message.
#![cfg(feature = "lakekeeper-e2e")]

use std::time::Duration;

use super::stack::{self, CatalogConnectionPassword, wait_for_url};

// ---------------------------------------------------------------------------
// Constants — mirror `docker-compose.lakekeeper.yml`'s header comment exactly.
// ---------------------------------------------------------------------------

/// Keycloak realm holding the confidential client.
const KEYCLOAK_REALM: &str = "iceberg";
/// OAuth2 confidential client id used for the client-credentials grant.
const OAUTH_CLIENT_ID: &str = "lakehouse";
/// OAuth2 confidential client secret.
const OAUTH_CLIENT_SECRET: &str = "lakehouse-engine-secret";
/// S3 bucket both warehouses are rooted in.
const WAREHOUSE_BUCKET: &str = "warehouse";
/// S3 region reported to Lakekeeper (MinIO ignores it, but the profile requires one).
const S3_REGION: &str = "us-east-1";
/// Static-warehouse S3 access key (full MinIO admin; `sts-enabled:false`).
const STATIC_ACCESS_KEY: &str = "minioadmin";
/// Static-warehouse S3 secret key.
const STATIC_SECRET_KEY: &str = "minioadmin";
/// Vended-warehouse S3 access key (scoped MinIO user; `sts-enabled:true`).
const VENDED_ACCESS_KEY: &str = "lakekeeper";
/// Vended-warehouse S3 secret key.
const VENDED_SECRET_KEY: &str = "lakekeeper-secret-key";

/// Name of the static-credential (delegation-off) warehouse.
pub const WAREHOUSE_STATIC: &str = "lakehouse_static";
/// Name of the vended-credential (STS) warehouse.
pub const WAREHOUSE_VENDED: &str = "lakehouse_vended";

/// Keycloak realm import + boot can take a while on a cold stack, so allow a
/// generous ceiling; the wait still fails loudly at the deadline rather than
/// hanging forever.
const READINESS_TIMEOUT: Duration = Duration::from_secs(120);
/// Per-request timeout for the host-side HTTP calls.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Host-side ports and URLs (the harness reaches the stack via mapped ports).
// ---------------------------------------------------------------------------

fn port_from_env(env_var: &str, default: u16) -> u16 {
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

/// Keycloak host port. `LH_KEYCLOAK_PORT`, default 28080.
pub fn keycloak_port() -> u16 {
    port_from_env("LH_KEYCLOAK_PORT", 28080)
}

/// Lakekeeper host port. `LH_LAKEKEEPER_PORT`, default 28181.
pub fn lakekeeper_port() -> u16 {
    port_from_env("LH_LAKEKEEPER_PORT", 28181)
}

/// Keycloak OIDC token endpoint as reached from the host (mapped port).
fn keycloak_token_endpoint_host() -> String {
    format!(
        "http://localhost:{}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token",
        keycloak_port()
    )
}

/// Keycloak OIDC token endpoint as reached from inside the Exasol UDF container
/// (Docker-network name + internal port). This is what the CONNECTION password
/// carries — the UDF resolves `keycloak` via the overlay's `extra_hosts` loop.
fn keycloak_token_endpoint_internal() -> String {
    format!("http://keycloak:8080/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token")
}

/// Lakekeeper management API base (host-side).
fn management_base() -> String {
    format!("http://localhost:{}/management/v1", lakekeeper_port())
}

fn http_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("build Lakekeeper HTTP client")
}

// ---------------------------------------------------------------------------
// Readiness waits.
// ---------------------------------------------------------------------------

/// Block until Keycloak has imported the `iceberg` realm, or fail loudly.
///
/// Polls the realm's OIDC discovery document — a 2xx there proves realm import
/// finished, not merely that Keycloak's port is open.
pub fn wait_for_keycloak() {
    let url = format!(
        "http://localhost:{}/realms/{KEYCLOAK_REALM}/.well-known/openid-configuration",
        keycloak_port()
    );
    wait_for_url(&url, READINESS_TIMEOUT);
}

/// Block until Lakekeeper's HTTP health endpoint reports ready, or fail loudly.
pub fn wait_for_lakekeeper() {
    let url = format!("http://localhost:{}/health", lakekeeper_port());
    wait_for_url(&url, READINESS_TIMEOUT);
}

// ---------------------------------------------------------------------------
// Keycloak OAuth2 client-credentials grant (host-side management token).
// ---------------------------------------------------------------------------

/// Perform the OAuth2 client-credentials grant against Keycloak and return the
/// bearer access token, for host-side Lakekeeper management-API calls.
///
/// This is a test-only helper for provisioning; it is NOT the UDF's own OAuth2
/// path (the adapter issues its own grant at query time from the CONNECTION
/// fields built by [`lakekeeper_connection_password`]).
///
/// Panics (never returns `Err`) on any failure. Neither the client secret nor
/// the returned token is placed in a panic message.
pub fn keycloak_client_credentials_token() -> String {
    let endpoint = keycloak_token_endpoint_host();
    let resp = http_client()
        .post(&endpoint)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", OAUTH_CLIENT_ID),
            ("client_secret", OAUTH_CLIENT_SECRET),
        ])
        .send()
        .unwrap_or_else(|e| panic!("Keycloak token request to {endpoint} failed to send: {e}"));

    // Do not surface the response body: on success it carries the access token.
    let status = resp.status();
    assert!(
        status.is_success(),
        "Keycloak token request to {endpoint} returned {status} (expected 2xx)"
    );

    let body: serde_json::Value = resp
        .json()
        .unwrap_or_else(|e| panic!("Keycloak token response was not valid JSON: {e}"));
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| panic!("Keycloak token response contained no access_token field"))
}

// ---------------------------------------------------------------------------
// Lakekeeper management API — bootstrap.
// ---------------------------------------------------------------------------

/// Bootstrap the Lakekeeper server so it accepts warehouse-management calls.
///
/// Lakekeeper can only be bootstrapped once server-wide; the Docker stack can
/// persist across local re-runs, so this helper is idempotent — it first checks
/// the server-info endpoint and returns early when already bootstrapped, and
/// treats a `409 Conflict` from the bootstrap POST as an already-bootstrapped
/// success rather than a failure.
///
/// `is-operator` is requested so the machine client keeps full management access
/// under Lakekeeper's default `allowall` authz backend.
pub fn lakekeeper_bootstrap() {
    let token = keycloak_client_credentials_token();
    let base = management_base();

    if server_already_bootstrapped(&base, &token) {
        return;
    }

    let body = serde_json::json!({
        "accept-terms-of-use": true,
        "is-operator": true,
    });
    let url = format!("{base}/bootstrap");
    let resp = http_client()
        .post(&url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .unwrap_or_else(|e| panic!("Lakekeeper bootstrap POST to {url} failed to send: {e}"));

    let status = resp.status();
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return;
    }
    // The bootstrap request body carries no credentials, so the response body is
    // safe to surface for diagnostics.
    let detail = resp.text().unwrap_or_default();
    panic!("Lakekeeper bootstrap POST to {url} returned {status}: {detail}");
}

/// Query the server-info endpoint; return `true` only when it explicitly reports
/// the server as already bootstrapped. Any ambiguity (unreachable, unparseable,
/// field absent) returns `false` so the caller proceeds to POST bootstrap.
fn server_already_bootstrapped(base: &str, token: &str) -> bool {
    let url = format!("{base}/info");
    let Ok(resp) = http_client().get(&url).bearer_auth(token).send() else {
        return false;
    };
    if !resp.status().is_success() {
        return false;
    }
    resp.json::<serde_json::Value>()
        .ok()
        .and_then(|v| v.get("bootstrapped").and_then(|b| b.as_bool()))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Lakekeeper management API — warehouse creation.
// ---------------------------------------------------------------------------

/// A storage profile for a Lakekeeper warehouse over the base stack's MinIO.
///
/// Two variants are exposed via constructors: [`WarehouseProfile::static_creds`]
/// (full-admin static credentials, delegation off) and
/// [`WarehouseProfile::vended`] (scoped MinIO user, STS credential vending on).
pub struct WarehouseProfile {
    name: &'static str,
    vended: bool,
    access_key: &'static str,
    secret_key: &'static str,
}

impl WarehouseProfile {
    /// Static-credential warehouse: `sts-enabled:false`, full MinIO admin creds.
    pub fn static_creds() -> Self {
        WarehouseProfile {
            name: WAREHOUSE_STATIC,
            vended: false,
            access_key: STATIC_ACCESS_KEY,
            secret_key: STATIC_SECRET_KEY,
        }
    }

    /// Vended-credential warehouse: `sts-enabled:true`, scoped MinIO user. MinIO
    /// serves STS AssumeRole at its S3 endpoint and scopes the vended session by
    /// the policy attached to this user, so `sts-role-arn` is intentionally
    /// omitted (MinIO ignores it).
    pub fn vended() -> Self {
        WarehouseProfile {
            name: WAREHOUSE_VENDED,
            vended: true,
            access_key: VENDED_ACCESS_KEY,
            secret_key: VENDED_SECRET_KEY,
        }
    }

    /// The warehouse name Lakekeeper registers this profile under.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Create a warehouse for `profile` via Lakekeeper's management API.
///
/// Idempotent: an already-provisioned warehouse is treated as success so the
/// helper is safe against a persisted stack. Lakekeeper 0.13.1 signals this as an
/// HTTP 400 `CreateWarehouseStorageProfileOverlap` (its storage profile overlaps
/// the existing warehouse's), not a 409 Conflict, so both are accepted.
///
/// The request carries S3 credentials, so its response body is NEVER surfaced in
/// a panic message — only the endpoint and status code are.
pub fn lakekeeper_create_warehouse(profile: &WarehouseProfile) {
    let token = keycloak_client_credentials_token();
    let base = management_base();

    // MinIO is reached by Lakekeeper (and embedded into vended creds / table
    // metadata) via its Docker-network name. A per-warehouse key-prefix keeps
    // the two warehouses' data disjoint within the shared bucket.
    let storage_profile = serde_json::json!({
        "type": "s3",
        "bucket": WAREHOUSE_BUCKET,
        "endpoint": stack::minio_url_internal(),
        "region": S3_REGION,
        "path-style-access": true,
        "flavor": "s3-compat",
        "sts-enabled": profile.vended,
        "key-prefix": profile.name,
    });
    let body = serde_json::json!({
        "warehouse-name": profile.name,
        "storage-profile": storage_profile,
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": profile.access_key,
            "aws-secret-access-key": profile.secret_key,
        },
        "delete-profile": { "type": "hard" },
    });

    let url = format!("{base}/warehouse");
    let resp = http_client()
        .post(&url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .unwrap_or_else(|e| {
            panic!(
                "Lakekeeper create-warehouse POST to {url} for '{}' failed to send: {e}",
                profile.name
            )
        });

    let status = resp.status();
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return;
    }
    // Idempotency against a persisted stack: Lakekeeper 0.13.1 reports an
    // already-provisioned warehouse as HTTP 400 `CreateWarehouseStorageProfileOverlap`
    // (its storage profile overlaps the existing warehouse's), NOT 409 Conflict.
    // Treat that specific "already exists" 400 as success so a re-run against a
    // persisted stack stays idempotent. Each harness warehouse has a unique
    // per-name key-prefix, so an overlap can only mean this same warehouse already
    // exists. The overlap error body names only the warehouse/storage profile and
    // carries no credential; the panic message below still surfaces only the
    // endpoint and status, never the response body, per the credential-safety
    // contract.
    if status == reqwest::StatusCode::BAD_REQUEST {
        let already_exists = resp
            .text()
            .map(|b| {
                b.contains("StorageProfileOverlap")
                    || b.contains("overlaps with existing warehouse")
            })
            .unwrap_or(false);
        if already_exists {
            return;
        }
    }
    panic!(
        "Lakekeeper create-warehouse POST to {url} for '{}' returned {status} \
         (expected 2xx, 409, or an already-exists 400)",
        profile.name
    );
}

// ---------------------------------------------------------------------------
// CONNECTION password builder (consumed by the UDF at query time).
// ---------------------------------------------------------------------------

/// Build the `CatalogConnectionPassword` for a Lakekeeper CONNECTION.
///
/// Populated for the OAuth2 client-credentials flow the adapter runs at query
/// time: `client_id`/`client_secret` and the UDF-side Keycloak token endpoint.
/// The `warehouse` field is the Lakekeeper warehouse NAME (the value passed to
/// `GET /v1/config?warehouse=`), not an `s3://` path.
///
/// When `vended` is true the UDF requests short-lived vended S3 credentials via
/// `load_table`, so no static S3 fields are set. When `vended` is false the UDF
/// reads MinIO directly, so the static S3 fields (endpoint, region, keys,
/// path-style) are populated with the static warehouse's credentials.
pub fn lakekeeper_connection_password(
    warehouse_name: &str,
    vended: bool,
) -> CatalogConnectionPassword {
    let base = CatalogConnectionPassword {
        warehouse: warehouse_name.to_string(),
        use_vended_credentials: vended,
        client_id: Some(OAUTH_CLIENT_ID.to_string()),
        client_secret: Some(OAUTH_CLIENT_SECRET.to_string()),
        oauth2_server_uri: Some(keycloak_token_endpoint_internal()),
        ..Default::default()
    };

    if vended {
        return base;
    }

    CatalogConnectionPassword {
        endpoint: stack::minio_url_internal(),
        region: S3_REGION.to_string(),
        access_key: STATIC_ACCESS_KEY.to_string(),
        secret_key: STATIC_SECRET_KEY.to_string(),
        path_style: true,
        ..base
    }
}

// ---------------------------------------------------------------------------
// Unit tests — the pure CONNECTION-password builder (no live stack required).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lakekeeper_connection_password_static_populates_oauth_and_static_s3() {
        let pw = lakekeeper_connection_password(WAREHOUSE_STATIC, false);

        assert_eq!(pw.warehouse, WAREHOUSE_STATIC);
        assert!(!pw.use_vended_credentials);
        assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
        assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
        assert_eq!(
            pw.oauth2_server_uri.as_deref(),
            Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
        );
        // Static S3 fields are populated (UDF reads MinIO directly).
        assert_eq!(pw.endpoint, "http://minio:9000");
        assert_eq!(pw.region, S3_REGION);
        assert_eq!(pw.access_key, STATIC_ACCESS_KEY);
        assert_eq!(pw.secret_key, STATIC_SECRET_KEY);
        assert!(pw.path_style);
        // No STS session token on the static path.
        assert_eq!(pw.session_token, None);
        // Never SigV4 on the OAuth path (would be rejected as mutually exclusive).
        assert!(!pw.use_sigv4);
    }

    #[test]
    fn lakekeeper_connection_password_vended_omits_static_s3() {
        let pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);

        assert_eq!(pw.warehouse, WAREHOUSE_VENDED);
        assert!(pw.use_vended_credentials);
        // OAuth2 client-credentials fields are still present.
        assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
        assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
        assert_eq!(
            pw.oauth2_server_uri.as_deref(),
            Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
        );
        // Static S3 fields are NOT set — creds come from load_table vending.
        assert_eq!(pw.endpoint, "");
        assert_eq!(pw.region, "");
        assert_eq!(pw.access_key, "");
        assert_eq!(pw.secret_key, "");
        assert!(!pw.use_sigv4);
    }

    #[test]
    fn lakekeeper_connection_password_serializes_expected_json() {
        // The serialized JSON must be a valid catalog password: OAuth2 fields
        // present, and for vended, the static S3 keys must be absent/empty.
        let pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);
        let json_str = pw.to_sql_password_json();
        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("password serializes to valid JSON");

        assert_eq!(parsed["warehouse"], WAREHOUSE_VENDED);
        assert_eq!(parsed["use_vended_credentials"], true);
        assert_eq!(parsed["client_id"], OAUTH_CLIENT_ID);
        assert_eq!(parsed["client_secret"], OAUTH_CLIENT_SECRET);
        assert_eq!(
            parsed["oauth2_server_uri"],
            "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
        );
    }

    #[test]
    fn warehouse_profiles_carry_documented_names() {
        assert_eq!(WarehouseProfile::static_creds().name(), WAREHOUSE_STATIC);
        assert_eq!(WarehouseProfile::vended().name(), WAREHOUSE_VENDED);
    }
}
