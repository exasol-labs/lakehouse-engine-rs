//! Lakekeeper + Keycloak provisioning for the `lakekeeper-e2e` suite. Values mirror
//! the header comment of `docker-compose.lakekeeper.yml`; keep the two in sync.
//! Helpers panic, never skip, and never put a secret or token in a panic message.
#![cfg(any(feature = "lakekeeper-e2e", feature = "azure-e2e"))]

use std::time::Duration;

use lakehouse_catalog::ConnectionCreds;

use super::stack::{self, CatalogConnectionPassword, wait_for_url};

const KEYCLOAK_REALM: &str = "iceberg";
const OAUTH_CLIENT_ID: &str = "lakehouse";
const OAUTH_CLIENT_SECRET: &str = "lakehouse-engine-secret";
const WAREHOUSE_BUCKET: &str = "warehouse";
/// MinIO ignores the region, but the Lakekeeper storage profile requires one.
const S3_REGION: &str = "us-east-1";
const STATIC_ACCESS_KEY: &str = "minioadmin";
const STATIC_SECRET_KEY: &str = "minioadmin";
/// Scoped MinIO user, used with `sts-enabled:true`.
const VENDED_ACCESS_KEY: &str = "lakekeeper";
const VENDED_SECRET_KEY: &str = "lakekeeper-secret-key";

pub const WAREHOUSE_STATIC: &str = "lakehouse_static";
pub const WAREHOUSE_VENDED: &str = "lakehouse_vended";

/// Keycloak realm import can be slow on a cold stack.
const READINESS_TIMEOUT: Duration = Duration::from_secs(120);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn port_from_env(env_var: &str, default: u16) -> u16 {
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

pub fn keycloak_port() -> u16 {
    port_from_env("LH_KEYCLOAK_PORT", 28080)
}

pub fn lakekeeper_port() -> u16 {
    port_from_env("LH_LAKEKEEPER_PORT", 28181)
}

fn keycloak_token_endpoint_host() -> String {
    format!(
        "http://localhost:{}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token",
        keycloak_port()
    )
}

/// Reached from inside the Exasol UDF container via the overlay's `extra_hosts`.
fn keycloak_token_endpoint_internal() -> String {
    format!("http://keycloak:8080/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token")
}

fn management_base() -> String {
    format!("http://localhost:{}/management/v1", lakekeeper_port())
}

fn http_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("build Lakekeeper HTTP client")
}

/// A 2xx on the realm's discovery document proves realm import finished, not just that
/// the port is open.
pub fn wait_for_keycloak() {
    let url = format!(
        "http://localhost:{}/realms/{KEYCLOAK_REALM}/.well-known/openid-configuration",
        keycloak_port()
    );
    wait_for_url(&url, READINESS_TIMEOUT);
}

pub fn wait_for_lakekeeper() {
    let url = format!("http://localhost:{}/health", lakekeeper_port());
    wait_for_url(&url, READINESS_TIMEOUT);
}

/// Host-side provisioning token only; the adapter runs its own grant at query time.
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

    // On success the response body carries the access token.
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

/// Bootstrap is once-per-server and the stack persists across runs, so an
/// already-bootstrapped server and a `409` both count as success. `is-operator` keeps
/// full management access under Lakekeeper's default `allowall` authz backend.
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
    // The bootstrap request carries no credentials, so the response body is safe to show.
    let detail = resp.text().unwrap_or_default();
    panic!("Lakekeeper bootstrap POST to {url} returned {status}: {detail}");
}

/// Any ambiguity returns `false`, so the caller proceeds to POST bootstrap.
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

pub struct WarehouseProfile {
    name: &'static str,
    vended: bool,
    access_key: &'static str,
    secret_key: &'static str,
}

impl WarehouseProfile {
    pub fn static_creds() -> Self {
        WarehouseProfile {
            name: WAREHOUSE_STATIC,
            vended: false,
            access_key: STATIC_ACCESS_KEY,
            secret_key: STATIC_SECRET_KEY,
        }
    }

    /// `sts-role-arn` is omitted: MinIO ignores it and scopes the vended session by this
    /// user's policy.
    pub fn vended() -> Self {
        WarehouseProfile {
            name: WAREHOUSE_VENDED,
            vended: true,
            access_key: VENDED_ACCESS_KEY,
            secret_key: VENDED_SECRET_KEY,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Per-run: the container is created and deleted by the owning run.
pub struct AdlsWarehouseProfile {
    name: String,
    account_name: String,
    filesystem: String,
    account_key: String,
    sas_enabled: bool,
}

impl AdlsWarehouseProfile {
    pub fn static_creds(container_name: &str, account_name: &str, account_key: &str) -> Self {
        AdlsWarehouseProfile {
            name: format!("{container_name}-static"),
            account_name: account_name.to_string(),
            filesystem: container_name.to_string(),
            account_key: account_key.to_string(),
            sas_enabled: false,
        }
    }

    pub fn vended(container_name: &str, account_name: &str, account_key: &str) -> Self {
        AdlsWarehouseProfile {
            name: format!("{container_name}-vended"),
            account_name: account_name.to_string(),
            filesystem: container_name.to_string(),
            account_key: account_key.to_string(),
            sas_enabled: true,
        }
    }

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

pub fn lakekeeper_create_warehouse(profile: &WarehouseProfile) {
    // A per-warehouse key-prefix keeps the two warehouses' data disjoint in the shared bucket.
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

/// Lakekeeper validates access by writing a probe object, so a missing container or
/// wrong key fails here rather than later as a scan error.
pub fn lakekeeper_create_adls_warehouse(profile: &AdlsWarehouseProfile) {
    post_warehouse(
        &profile.name,
        profile.storage_profile(),
        profile.storage_credential(),
    );
}

/// Lakekeeper 0.13.1 reports an already-provisioned warehouse as HTTP 400
/// `CreateWarehouseStorageProfileOverlap`, not 409; both count as success. The response
/// body never reaches a panic message, since `storage_credential` carries a secret.
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

/// `warehouse` is the Lakekeeper warehouse name (`GET /v1/config?warehouse=`), not a path.
pub fn lakekeeper_connection_password(
    warehouse_name: &str,
    vended: bool,
) -> CatalogConnectionPassword {
    let base = CatalogConnectionPassword {
        warehouse: warehouse_name.to_string(),
        use_vended_credentials: vended,
        // Stated explicitly because the CONNECTION's value wins over the vended response, which
        // can state (or default to) virtual-hosted-style that MinIO cannot serve.
        path_style: true,
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

/// `oauth2_server_uri` is the host-mapped Keycloak endpoint, since the UDF-internal
/// Docker-network URL the CONNECTION carries is unreachable from the test process.
pub fn lakekeeper_host_connection_creds(warehouse_name: &str, vended: bool) -> ConnectionCreds {
    let password = lakekeeper_connection_password(warehouse_name, vended);
    ConnectionCreds {
        warehouse: password.warehouse,
        endpoint: password.endpoint,
        region: password.region,
        access_key: password.access_key,
        secret_key: password.secret_key,
        session_token: password.session_token,
        path_style: Some(password.path_style),
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

/// Never the container-lifecycle service principal, which would let the suite pass
/// without exercising the account-key path. Static S3 fields stay empty: the adapter
/// rejects a CONNECTION naming both Azure and S3 storage fields as ambiguous.
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

/// Lists warehouses rather than fetching by id: callers only know the name. The body is
/// safe to return since Lakekeeper never echoes a storage credential.
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
        assert_eq!(pw.endpoint, "http://minio:9000");
        assert_eq!(pw.region, S3_REGION);
        assert_eq!(pw.access_key, STATIC_ACCESS_KEY);
        assert_eq!(pw.secret_key, STATIC_SECRET_KEY);
        assert!(pw.path_style);
        assert_eq!(pw.session_token, None);
        // SigV4 and OAuth are mutually exclusive.
        assert!(!pw.use_sigv4);
    }

    #[test]
    fn lakekeeper_connection_password_vended_omits_static_s3() {
        let pw = lakekeeper_connection_password(WAREHOUSE_VENDED, true);

        assert_eq!(pw.warehouse, WAREHOUSE_VENDED);
        assert!(pw.use_vended_credentials);
        assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
        assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
        assert_eq!(
            pw.oauth2_server_uri.as_deref(),
            Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
        );
        assert_eq!(pw.endpoint, "");
        assert_eq!(pw.region, "");
        assert_eq!(pw.access_key, "");
        assert_eq!(pw.secret_key, "");
        assert!(!pw.use_sigv4);
        assert_eq!(pw.account_name, None);
        assert_eq!(pw.account_key, None);
        assert_eq!(pw.session_token, None);
        assert!(pw.path_style);

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
        assert_eq!(pw.client_id.as_deref(), Some(OAUTH_CLIENT_ID));
        assert_eq!(pw.client_secret.as_deref(), Some(OAUTH_CLIENT_SECRET));
        assert_eq!(
            pw.oauth2_server_uri.as_deref(),
            Some("http://keycloak:8080/realms/iceberg/protocol/openid-connect/token")
        );
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
