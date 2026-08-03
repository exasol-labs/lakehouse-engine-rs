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
///
/// Unlike the two MinIO profiles this is per-run, not constant: the container is
/// created and deleted by the run that owns it, and the account name and key
/// arrive from the environment.
///
/// Delegation is off. `sas-enabled` is stated explicitly because Lakekeeper
/// v0.13.1 defaults it to `true`: a warehouse left vending SAS tokens would let a
/// scan succeed without ever using the account key, which is the credential this
/// suite exists to verify.
pub struct AdlsWarehouseProfile {
    name: String,
    account_name: String,
    filesystem: String,
    account_key: String,
}

impl AdlsWarehouseProfile {
    /// Build the static-credential ADLS profile for the run that owns
    /// `container_name`.
    ///
    /// The warehouse name is derived from the container rather than supplied, so
    /// two properties hold by construction. It carries the container's per-run
    /// suffix, so a repeated local run never binds to a surviving warehouse whose
    /// container has already been deleted. And its `-static` tail keeps this
    /// warehouse's `key-prefix` disjoint from that of any sibling warehouse
    /// sharing the run's container — Lakekeeper rejects a second warehouse whose
    /// key-prefix overlaps an existing one's.
    pub fn new(container_name: &str, account_name: &str, account_key: &str) -> Self {
        AdlsWarehouseProfile {
            name: format!("{container_name}-static"),
            account_name: account_name.to_string(),
            filesystem: container_name.to_string(),
            account_key: account_key.to_string(),
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
            "sas-enabled": false,
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
/// API.
///
/// Builds only the `s3` request body and delegates to [`post_warehouse`], which
/// owns the endpoint, the already-exists idempotency rule, and the contract that
/// no response body reaches a panic message.
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
/// API.
///
/// Builds only the `adls` request body and delegates to the same
/// [`post_warehouse`] the MinIO arm uses, so that helper's idempotency handling
/// and its never-echo-the-response-body contract cover the account key this
/// request body carries.
///
/// The container must already exist: Lakekeeper creates no filesystem and
/// validates physical access here by writing and deleting a probe object, so a
/// missing container or a wrong account key fails this call rather than surfacing
/// later as a scan error.
pub fn lakekeeper_create_adls_warehouse(profile: &AdlsWarehouseProfile) {
    post_warehouse(
        &profile.name,
        profile.storage_profile(),
        profile.storage_credential(),
    );
}

/// POST one warehouse to Lakekeeper's management API and fail loudly unless it
/// exists afterwards.
///
/// The single owner of the create-warehouse endpoint for every storage backend,
/// so the two contracts below hold identically for each of them.
///
/// Idempotent: an already-provisioned warehouse is treated as success so the
/// helper is safe against a persisted stack. Lakekeeper 0.13.1 reports this as an
/// HTTP 400 `CreateWarehouseStorageProfileOverlap` (the storage profile overlaps
/// the existing warehouse's), NOT a 409 Conflict, so both are accepted. Each
/// harness warehouse has a unique key-prefix, so an overlap can only mean this
/// same warehouse already exists.
///
/// Credential-safe: `storage_credential` carries an S3 secret key or an Azure
/// account key, so the response body is NEVER surfaced in a panic message — only
/// the endpoint, the warehouse name, and the status code are.
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

/// Build the `CatalogConnectionPassword` for an Azure (ADLS) Lakekeeper
/// CONNECTION.
///
/// Populated for the OAuth2 client-credentials flow the adapter runs against
/// Keycloak at query time, plus the storage credential under test: the account
/// name and account key, which is the `AdlsCred::AccountKey` path. The
/// container-lifecycle service principal deliberately has no representation here
/// — a CONNECTION reaching Azure through it would let the suite pass while
/// exercising nothing the production read path ships.
///
/// Every static S3 storage field is left empty. The adapter reads an empty string
/// as absent and rejects a CONNECTION naming both an Azure and an S3 storage
/// field as an ambiguous credential set, so leaving them empty — rather than
/// inheriting the MinIO builder's endpoint and region — is what makes this an
/// Azure CONNECTION.
///
/// `warehouse_name` is the per-run ADLS warehouse NAME (the value passed to
/// `GET /v1/config?warehouse=`), not an `abfss://` path. It is the one field that
/// cannot be empty: an empty `warehouse` is rejected before any Azure validation
/// runs, and its value carries the run's own suffix.
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
/// Lists warehouses rather than fetching by id, because every caller here only
/// ever has the warehouse NAME — the create path never learns the server-assigned
/// id, and adding a name-to-id lookup step would buy nothing this single list
/// call doesn't already give. Credential-safe like every other call in this
/// file: on failure the panic names only the endpoint and status, never the
/// response body — though on success the body is safe to return in full, since
/// Lakekeeper's warehouse representation never echoes a storage credential.
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
    fn azure_offline_adls_warehouse_matches_lakekeeper_profile_shape() {
        let profile = AdlsWarehouseProfile::new("lhrs-e2e-user-42", "acct", "a2V5");

        assert_eq!(
            profile.name(),
            "lhrs-e2e-user-42-static",
            "the warehouse carries the container's per-run suffix"
        );

        let storage = profile.storage_profile();
        assert_eq!(storage["type"], "adls");
        assert_eq!(storage["account-name"], "acct");
        assert_eq!(storage["filesystem"], "lhrs-e2e-user-42");
        assert_eq!(storage["key-prefix"], profile.name());
        assert_eq!(
            storage["sas-enabled"], false,
            "Lakekeeper defaults sas-enabled to true, and a vending warehouse would let \
             the scan pass without ever using the account key under test"
        );

        let credential = profile.storage_credential();
        assert_eq!(credential["type"], "az");
        assert_eq!(credential["credential-type"], "shared-access-key");
        assert_eq!(credential["key"], "a2V5");
    }

    #[test]
    fn azure_offline_adls_connection_password_is_unambiguously_azure() {
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
        // Every static S3 storage field stays empty, which the adapter reads as
        // absent: a CONNECTION naming both an Azure and an S3 storage field is
        // rejected as an ambiguous credential set.
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
