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
#![cfg(any(feature = "lakekeeper-e2e", feature = "azure-e2e"))]

use std::time::Duration;

use lakehouse_catalog::ConnectionCreds;

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

/// A storage profile for a Lakekeeper warehouse over a real ADLS Gen2 container.
/// Per-run (not constant): the container is created/deleted by the owning run,
/// and the account name and key come from the environment.
///
/// Two variants: [`AdlsWarehouseProfile::static_creds`] (`sas-enabled: false`,
/// Lakekeeper reads the account key directly) and [`AdlsWarehouseProfile::vended`]
/// (`sas-enabled: true`, Lakekeeper mints a short-lived SAS per request from that
/// same account key). Both share one [`AdlsWarehouseProfile::storage_credential`].
pub struct AdlsWarehouseProfile {
    name: String,
    account_name: String,
    filesystem: String,
    account_key: String,
    sas_enabled: bool,
}

impl AdlsWarehouseProfile {
    /// Static-credential ADLS profile for the run owning `container_name`:
    /// `sas-enabled: false`, so Lakekeeper reads `account_key` directly. The
    /// warehouse name derives from the container (per-run suffix, `-static` tail
    /// to keep its `key-prefix` disjoint from a vended sibling).
    pub fn static_creds(container_name: &str, account_name: &str, account_key: &str) -> Self {
        AdlsWarehouseProfile {
            name: format!("{container_name}-static"),
            account_name: account_name.to_string(),
            filesystem: container_name.to_string(),
            account_key: account_key.to_string(),
            sas_enabled: false,
        }
    }

    /// Vended-credential ADLS profile: `sas-enabled: true` (Lakekeeper's own
    /// default), so it mints a short-lived SAS per request instead of handing out
    /// `account_key` directly. Warehouse name as in [`Self::static_creds`], with a
    /// `-vended` tail.
    pub fn vended(container_name: &str, account_name: &str, account_key: &str) -> Self {
        AdlsWarehouseProfile {
            name: format!("{container_name}-vended"),
            account_name: account_name.to_string(),
            filesystem: container_name.to_string(),
            account_key: account_key.to_string(),
            sas_enabled: true,
        }
    }

    /// The warehouse name Lakekeeper registers this profile under, which is also
    /// its `key-prefix` within the container.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn storage_profile(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "adls",
            "account-name": self.account_name,
            "filesystem": self.filesystem,
            "key-prefix": self.name,
            "sas-enabled": self.sas_enabled,
        })
    }

    fn storage_credential(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "az",
            "credential-type": "shared-access-key",
            "key": self.account_key,
        })
    }
}

/// Create the MinIO-backed warehouse for `profile` via Lakekeeper's management
/// API. Builds the `s3` request body; [`post_warehouse`] owns the endpoint,
/// idempotency, and panic-safety contracts.
pub fn lakekeeper_create_warehouse(profile: &WarehouseProfile) {
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
    let storage_credential = serde_json::json!({
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": profile.access_key,
        "aws-secret-access-key": profile.secret_key,
    });

    post_warehouse(profile.name, storage_profile, storage_credential);
}

/// Create the per-run ADLS warehouse for `profile` via Lakekeeper's management
/// API. Builds the `adls` request body; [`post_warehouse`] covers idempotency
/// and panic-safety for the account key it carries.
///
/// The container must already exist: Lakekeeper validates access by writing and
/// deleting a probe object, so a missing container or wrong key fails here
/// rather than surfacing later as a scan error.
pub fn lakekeeper_create_adls_warehouse(profile: &AdlsWarehouseProfile) {
    post_warehouse(
        &profile.name,
        profile.storage_profile(),
        profile.storage_credential(),
    );
}

/// POST one warehouse to Lakekeeper's management API and fail loudly on any
/// status other than 2xx, 409, or an already-exists 400. Single owner of the
/// create-warehouse endpoint for every storage backend.
///
/// Idempotent: Lakekeeper 0.13.1 reports an already-provisioned warehouse as
/// HTTP 400 `CreateWarehouseStorageProfileOverlap`, NOT 409 — both are treated
/// as success. For warehouses sharing a bucket/filesystem this is an unverified
/// inference, so callers needing certainty should read the warehouse back via
/// `lakekeeper_warehouse_storage_profile` (see `create_warehouse_and_confirm`).
///
/// Credential-safe: `storage_credential` carries an S3 secret or Azure account
/// key, so the response body never reaches a panic message — only the
/// endpoint, warehouse name, and status code do.
fn post_warehouse(
    warehouse_name: &str,
    storage_profile: serde_json::Value,
    storage_credential: serde_json::Value,
) {
    let token = keycloak_client_credentials_token();
    let body = serde_json::json!({
        "warehouse-name": warehouse_name,
        "storage-profile": storage_profile,
        "storage-credential": storage_credential,
        "delete-profile": { "type": "hard" },
    });

    let url = format!("{}/warehouse", management_base());
    let resp = http_client()
        .post(&url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .unwrap_or_else(|e| {
            panic!(
                "Lakekeeper create-warehouse POST to {url} for '{warehouse_name}' \
                 failed to send: {e}"
            )
        });

    let status = resp.status();
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return;
    }
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
        "Lakekeeper create-warehouse POST to {url} for '{warehouse_name}' returned {status} \
         (expected 2xx, 409, or an already-exists 400)"
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
/// `GET /v1/config?warehouse=`), not an `s3://` or `abfss://` path.
///
/// Shared by both the MinIO/STS arm and the ADLS/SAS arm: when `vended` is true,
/// no static storage field is populated for either backend (the UDF requests
/// short-lived credentials at scan time instead). When `vended` is false, the
/// static S3 fields (endpoint, region, keys, path-style) carry the static
/// warehouse's credentials.
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

/// The `ConnectionCreds` a HOST-side test parses out of a Lakekeeper CONNECTION,
/// projected from [`lakekeeper_connection_password`] so the two can never describe
/// different CONNECTIONs.
///
/// Exactly one field is deliberately not the UDF's: `oauth2_server_uri` is the
/// host-mapped Keycloak token endpoint, because the UDF-internal Docker-network URL
/// the CONNECTION carries is unreachable from the test process. `sas_token` is
/// absent — `CatalogConnectionPassword` carries no inline-SAS field to project from.
pub fn lakekeeper_host_connection_creds(warehouse_name: &str, vended: bool) -> ConnectionCreds {
    let password = lakekeeper_connection_password(warehouse_name, vended);
    ConnectionCreds {
        warehouse: password.warehouse,
        endpoint: password.endpoint,
        region: password.region,
        access_key: password.access_key,
        secret_key: password.secret_key,
        session_token: password.session_token,
        path_style: password.path_style,
        use_sigv4: password.use_sigv4,
        use_vended_credentials: password.use_vended_credentials,
        token: password.token,
        client_id: password.client_id,
        client_secret: password.client_secret,
        oauth2_server_uri: Some(keycloak_token_endpoint_host()),
        scope: password.scope,
        account_name: password.account_name,
        account_key: password.account_key,
        sas_token: None,
    }
}

/// Build the `CatalogConnectionPassword` for an Azure (ADLS) Lakekeeper
/// CONNECTION.
///
/// Carries the OAuth2 client-credentials fields plus the account name/key under
/// test (the `AdlsCred::AccountKey` path) — never the container-lifecycle
/// service principal, which would let the suite pass without exercising the
/// account-key path.
///
/// Every static S3 field is left empty: the adapter reads an empty string as
/// absent and rejects a CONNECTION naming both Azure and S3 storage fields as
/// ambiguous.
///
/// `warehouse_name` is the warehouse NAME (not an `abfss://` path); it cannot be
/// empty, since an empty `warehouse` is rejected before Azure validation runs.
pub fn lakekeeper_adls_connection_password(
    warehouse_name: &str,
    account_name: &str,
    account_key: &str,
) -> CatalogConnectionPassword {
    CatalogConnectionPassword {
        warehouse: warehouse_name.to_string(),
        use_vended_credentials: false,
        client_id: Some(OAUTH_CLIENT_ID.to_string()),
        client_secret: Some(OAUTH_CLIENT_SECRET.to_string()),
        oauth2_server_uri: Some(keycloak_token_endpoint_internal()),
        account_name: Some(account_name.to_string()),
        account_key: Some(account_key.to_string()),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Lakekeeper management API — read back a warehouse's storage profile.
// ---------------------------------------------------------------------------

/// Fetch `warehouse_name`'s storage profile exactly as Lakekeeper's management
/// API reports it.
///
/// Lists warehouses rather than fetching by id: every caller here only has the
/// warehouse NAME, and the create path never learns the server-assigned id.
/// Credential-safe: on failure the panic names only the endpoint and status; on
/// success the full body is safe to return since Lakekeeper's warehouse
/// representation never echoes a storage credential.
pub fn lakekeeper_warehouse_storage_profile(warehouse_name: &str) -> serde_json::Value {
    let token = keycloak_client_credentials_token();
    let url = format!("{}/warehouse", management_base());
    let resp = http_client()
        .get(&url)
        .bearer_auth(&token)
        .send()
        .unwrap_or_else(|e| panic!("Lakekeeper list-warehouse GET to {url} failed to send: {e}"));

    let status = resp.status();
    assert!(
        status.is_success(),
        "Lakekeeper list-warehouse GET to {url} returned {status} (expected 2xx)"
    );

    let body: serde_json::Value = resp
        .json()
        .unwrap_or_else(|e| panic!("Lakekeeper list-warehouse response was not valid JSON: {e}"));

    body["warehouses"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|w| w["name"].as_str() == Some(warehouse_name))
        .and_then(|w| w.get("storage-profile"))
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "Lakekeeper list-warehouse GET to {url} reported no warehouse named \
                 '{warehouse_name}' with a storage profile"
            )
        })
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
        // The vended branch is backend-neutral: no storage field is set.
        assert_eq!(pw.account_name, None);
        assert_eq!(pw.account_key, None);
        assert_eq!(pw.session_token, None);
        assert!(!pw.path_style);

        let json_str = pw.to_sql_password_json();
        let parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("password serializes to valid JSON");
        assert!(
            parsed.get("account_name").is_none() && parsed.get("account_key").is_none(),
            "the vended branch must not surface either backend's storage credential"
        );
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

    #[test]
    fn adls_warehouse_matches_lakekeeper_profile_shape() {
        let static_profile = AdlsWarehouseProfile::static_creds("lhrs-e2e-user-42", "acct", "a2V5");
        let vended_profile = AdlsWarehouseProfile::vended("lhrs-e2e-user-42", "acct", "a2V5");

        assert_eq!(
            static_profile.name(),
            "lhrs-e2e-user-42-static",
            "the warehouse carries the container's per-run suffix"
        );
        assert_eq!(
            vended_profile.name(),
            "lhrs-e2e-user-42-vended",
            "the warehouse carries the container's per-run suffix"
        );
        assert!(
            !vended_profile.name().starts_with(static_profile.name())
                && !static_profile.name().starts_with(vended_profile.name()),
            "neither warehouse name may be a prefix of the other — Lakekeeper's \
             key-prefix isolation between the two sibling warehouses depends on it"
        );

        let static_storage = static_profile.storage_profile();
        assert_eq!(static_storage["type"], "adls");
        assert_eq!(static_storage["account-name"], "acct");
        assert_eq!(static_storage["filesystem"], "lhrs-e2e-user-42");
        assert_eq!(static_storage["key-prefix"], static_profile.name());
        assert_eq!(
            static_storage["sas-enabled"], false,
            "the static-credential warehouse reads the account key directly, so \
             Lakekeeper's own true default must be overridden off"
        );

        let vended_storage = vended_profile.storage_profile();
        assert_eq!(vended_storage["type"], "adls");
        assert_eq!(vended_storage["account-name"], "acct");
        assert_eq!(vended_storage["filesystem"], "lhrs-e2e-user-42");
        assert_eq!(vended_storage["key-prefix"], vended_profile.name());
        assert_eq!(
            vended_storage["sas-enabled"], true,
            "the vended-credential warehouse matches Lakekeeper v0.13.1's own \
             sas-enabled default, so Lakekeeper mints a SAS token per request"
        );

        let static_credential = static_profile.storage_credential();
        let vended_credential = vended_profile.storage_credential();
        assert_eq!(static_credential["type"], "az");
        assert_eq!(static_credential["credential-type"], "shared-access-key");
        assert_eq!(static_credential["key"], "a2V5");
        assert_eq!(
            static_credential, vended_credential,
            "both modes register the same account key; Lakekeeper mints the \
             vended SAS from it rather than needing a separate credential shape"
        );
    }

    #[test]
    fn adls_connection_password_is_unambiguously_azure() {
        let pw = lakekeeper_adls_connection_password("lhrs-e2e-user-42-static", "acct", "a2V5");

        assert_eq!(pw.warehouse, "lhrs-e2e-user-42-static");
        assert_eq!(pw.account_name.as_deref(), Some("acct"));
        assert_eq!(pw.account_key.as_deref(), Some("a2V5"));
        // Same OAuth2 client-credentials catalog auth as the MinIO arm.
        assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
        assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
        assert_eq!(
            pw.oauth2_server_uri.as_deref(),
            Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
        );
        // Static S3 fields stay empty — ambiguous otherwise (see doc comment above).
        assert_eq!(pw.endpoint, "");
        assert_eq!(pw.region, "");
        assert_eq!(pw.access_key, "");
        assert_eq!(pw.secret_key, "");
        assert_eq!(pw.session_token, None);
        assert!(!pw.use_sigv4);
        assert!(!pw.use_vended_credentials);

        let parsed: serde_json::Value = serde_json::from_str(&pw.to_sql_password_json())
            .expect("password serializes to valid JSON");
        assert_eq!(parsed["account_name"], "acct");
        assert_eq!(parsed["account_key"], "a2V5");
        assert_eq!(parsed["endpoint"], "");
        assert_eq!(parsed["region"], "");
    }
}
