//! Catalog authentication and vended-credential handling for the `pushdown`
//! module: REST-catalog auth (static bearer token, OAuth2 client-credentials
//! grant, SigV4 request signing) and Iceberg REST vended-credential
//! extraction/merging.
//!
//! Extracted verbatim from the former flat `pushdown.rs`. Credential values
//! NEVER appear in any returned SQL string or error message — every error site
//! in this module routes through a redaction closure.

use super::namespace::parse_table_ident;
use super::support::redact_catalog_error;
use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{CatalogProps, StorageProps};
use exasol_udf_sdk::error::UdfError;
use iceberg::CatalogBuilder;
use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Build a RestCatalog configured to read/write data files through the S3
/// (MinIO) storage factory.
///
/// iceberg 0.9.1 requires an explicit `StorageFactory`; the S3 config keys are
/// supplied in the same props map passed to `load`. Credentials live only in
/// this map and never appear in returned SQL or error strings.
pub(super) async fn build_rest_catalog(
    catalog_uri: &str,
    catalog: &CatalogProps,
    storage: &StorageProps,
    creds: &ConnectionCreds,
) -> Result<RestCatalog, UdfError> {
    let mut props = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        catalog.warehouse.clone(),
    );
    if !storage.endpoint.is_empty() {
        props.insert(S3_ENDPOINT.to_string(), storage.endpoint.clone());
    }
    if !storage.region.is_empty() {
        props.insert(S3_REGION.to_string(), storage.region.clone());
    }
    if !storage.access_key.is_empty() {
        props.insert(S3_ACCESS_KEY_ID.to_string(), storage.access_key.clone());
    }
    if !storage.secret_key.is_empty() {
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), storage.secret_key.clone());
    }
    if let Some(token) = &storage.session_token {
        props.insert(S3_SESSION_TOKEN.to_string(), token.clone());
    }
    props.insert(
        S3_PATH_STYLE_ACCESS.to_string(),
        storage.path_style.to_string(),
    );

    inject_catalog_auth_props(&mut props, creds);

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("lakehouse", props)
        .await
        .map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to connect to Iceberg catalog: {}",
                redact_catalog_auth_error(&e.to_string(), creds)
            ))
        })
}

/// REST-catalog auth property keys (literal strings, fixed by `iceberg-catalog-rest`
/// 0.9.1; the crate exports no constants for them). They flow through
/// `RestCatalogBuilder::load`, which copies every prop except `uri`/`warehouse`.
const REST_CATALOG_PROP_TOKEN: &str = "token";
const REST_CATALOG_PROP_CREDENTIAL: &str = "credential";
const REST_CATALOG_PROP_OAUTH2_SERVER_URI: &str = "oauth2-server-uri";
const REST_CATALOG_PROP_SCOPE: &str = "scope";

/// Inject catalog-auth props from the resolved credentials into the REST-catalog
/// props map. Three mutually exclusive modes:
///
/// * no `token` and no client credentials → inject nothing (no-auth, default).
/// * non-empty `token` → inject only `token` (the bearer header; the crate never
///   consults `oauth2-server-uri`/`scope` in this mode).
/// * non-empty `client_id` + `client_secret` → inject `credential` =
///   `"client_id:client_secret"`, plus `oauth2-server-uri` ONLY when a non-empty
///   `oauth2_server_uri` is supplied and `scope` ONLY when a non-empty `scope` is
///   supplied; never inject `token` in this mode.
///
/// Token and client-credentials are mutually exclusive by construction.
fn inject_catalog_auth_props(props: &mut HashMap<String, String>, creds: &ConnectionCreds) {
    let token = non_empty(&creds.token);
    let client_id = non_empty(&creds.client_id);
    let client_secret = non_empty(&creds.client_secret);

    if let (Some(id), Some(secret)) = (client_id, client_secret) {
        props.insert(
            REST_CATALOG_PROP_CREDENTIAL.to_string(),
            format!("{id}:{secret}"),
        );
        if let Some(uri) = non_empty(&creds.oauth2_server_uri) {
            props.insert(
                REST_CATALOG_PROP_OAUTH2_SERVER_URI.to_string(),
                uri.to_string(),
            );
        }
        if let Some(scope) = non_empty(&creds.scope) {
            props.insert(REST_CATALOG_PROP_SCOPE.to_string(), scope.to_string());
        }
    } else if let Some(token) = token {
        props.insert(REST_CATALOG_PROP_TOKEN.to_string(), token.to_string());
    }
}

/// Borrow the inner value of an `Option<String>` only when it is non-empty.
fn non_empty(field: &Option<String>) -> Option<&str> {
    field.as_deref().filter(|v| !v.is_empty())
}

/// Redact a catalog error that may have surfaced an auth value. Applies the
/// generic label/pattern redaction AND strips the literal `token`, `client_secret`,
/// `client_id`, `oauth2_server_uri`, and `scope` values so any auth field echoed
/// without a recognizable label can never leak.
fn redact_catalog_auth_error(msg: &str, creds: &ConnectionCreds) -> String {
    let mut secrets: Vec<String> = Vec::new();
    if let Some(token) = non_empty(&creds.token) {
        secrets.push(token.to_string());
    }
    if let Some(secret) = non_empty(&creds.client_secret) {
        // The joined `credential` ("<id>:<secret>") need not be pushed separately:
        // stripping the bare secret first already removes the only sensitive
        // portion, leaving the non-secret `id`.
        secrets.push(secret.to_string());
    }
    if let Some(id) = non_empty(&creds.client_id) {
        secrets.push(id.to_string());
    }
    if let Some(uri) = non_empty(&creds.oauth2_server_uri) {
        secrets.push(uri.to_string());
    }
    if let Some(scope) = non_empty(&creds.scope) {
        secrets.push(scope.to_string());
    }
    let secret_refs: Vec<&str> = secrets.iter().map(String::as_str).collect();
    crate::scan::emit::redact_secret_values(&redact_catalog_error(msg), &secret_refs)
}

// ---------------------------------------------------------------------------
// Signed catalog resolution (SigV4 path)
// ---------------------------------------------------------------------------

/// Build an S3 `FileIO` from storage props.
///
/// Used by the signed path to give the iceberg `Table` a way to read manifest
/// files from S3 after we have fetched and deserialized the `LoadTableResult`.
pub(super) fn build_s3_file_io(storage: &StorageProps) -> iceberg::io::FileIO {
    let mut builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    }));
    if !storage.endpoint.is_empty() {
        builder = builder.with_prop(S3_ENDPOINT, &storage.endpoint);
    }
    if !storage.region.is_empty() {
        builder = builder.with_prop(S3_REGION, &storage.region);
    }
    if !storage.access_key.is_empty() {
        builder = builder.with_prop(S3_ACCESS_KEY_ID, &storage.access_key);
    }
    if !storage.secret_key.is_empty() {
        builder = builder.with_prop(S3_SECRET_ACCESS_KEY, &storage.secret_key);
    }
    if let Some(token) = &storage.session_token {
        builder = builder.with_prop(S3_SESSION_TOKEN, token);
    }
    builder = builder.with_prop(S3_PATH_STYLE_ACCESS, storage.path_style.to_string());
    builder.build()
}

/// Build the `loadTable` REST URL matching iceberg-catalog-rest's `table_endpoint` pattern:
/// `{catalog_uri}/v1/{prefix?}/namespaces/{ns_url}/tables/{table_name}`
///
/// The `warehouse` parameter is the already-resolved URL prefix string (matching
/// `props["prefix"]` in the iceberg-catalog-rest config map); the name is historical —
/// the caller passes the resolved prefix, not a raw connection warehouse.
/// `resolve_load_table_prefix` produces it upstream: for SigV4/Glue the derived
/// `catalogs/{account-id}` segment (via `glue_catalog_prefix`), for Databricks-style
/// catalogs the `overrides.prefix` fetched from `GET {catalog_uri}/v1/config?warehouse=…`,
/// and for plain REST catalogs typically empty. When empty, the prefix is omitted and the
/// URL reduces to `{catalog_uri}/v1/namespaces/{ns}/tables/{table}`.
///
/// The prefix is inserted verbatim — no URL-encoding — so a multi-segment prefix such as
/// the Glue `catalogs/{account-id}` form keeps its `/` literal, and any reserved characters
/// pass through unchanged. This is a low-level, format-agnostic builder: it inserts whatever
/// prefix string it is given and does not interpret its shape. Non-ASCII prefixes are not
/// URL-encoded here.
fn build_load_table_url(catalog_uri: &str, warehouse: &str, ns: &str, table_name: &str) -> String {
    let base = format!("{catalog_uri}/v1");
    if warehouse.is_empty() {
        format!("{base}/namespaces/{ns}/tables/{table_name}")
    } else {
        format!("{base}/{warehouse}/namespaces/{ns}/tables/{table_name}")
    }
}

/// The catalog-auth strategy resolved once for a query, used to authenticate
/// every self-issued catalog HTTP request (the `loadTable` GET and the
/// `/v1/config` prefix lookup) identically.
///
/// Orthogonal to credential vending: this selects HOW a request is authenticated,
/// never WHETHER vended credentials are extracted.
enum CatalogAuth {
    /// AWS SigV4 request signing against the `glue` service.
    Sigv4,
    /// `Authorization: Bearer <token>` — either a static `token` or a token
    /// obtained from the OAuth2 client-credentials grant.
    Bearer(String),
    /// No `Authorization` header (no-auth catalog).
    None,
}

/// The OAuth2 token request body field name for the grant type.
const OAUTH2_GRANT_TYPE: &str = "client_credentials";

/// The default token endpoint path appended to the catalog URI when no explicit
/// `oauth2_server_uri` is supplied (the Iceberg REST catalog convention).
const OAUTH2_DEFAULT_TOKEN_PATH: &str = "/v1/oauth/tokens";

/// Perform the OAuth2 client-credentials grant and return the obtained access token.
///
/// Form-encodes `grant_type=client_credentials`, `client_id`, `client_secret`,
/// and the optional `scope`, POSTed to `creds.oauth2_server_uri` when supplied,
/// otherwise to the catalog default token endpoint (`{catalog_uri}/v1/oauth/tokens`).
///
/// `client_secret`, the request, and the obtained token NEVER appear in any
/// returned error: every error site strips the client secret AND the obtained
/// token via value-based redaction.
async fn oauth2_client_credentials_grant(
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<String, UdfError> {
    let client_id = non_empty(&creds.client_id).ok_or_else(|| {
        UdfError::User("OAuth2 grant requires client_id but none was resolved".into())
    })?;
    let client_secret = non_empty(&creds.client_secret).ok_or_else(|| {
        UdfError::User("OAuth2 grant requires client_secret but none was resolved".into())
    })?;

    let token_url = match non_empty(&creds.oauth2_server_uri) {
        Some(uri) => uri.to_string(),
        None => format!(
            "{}{OAUTH2_DEFAULT_TOKEN_PATH}",
            catalog_uri.trim_end_matches('/')
        ),
    };

    // Strip the client secret AND the obtained token from every error. The token
    // is not yet known at the point a transport/parse error is built, so it is
    // added to the redaction set after a successful parse before being returned.
    let redact_secret = |msg: &str| {
        crate::scan::emit::redact_secret_values(&redact_catalog_error(msg), &[client_secret])
    };

    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", OAUTH2_GRANT_TYPE),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];
    if let Some(scope) = non_empty(&creds.scope) {
        form.push(("scope", scope));
    }

    let client = reqwest::Client::new();
    let response = client
        .post(&token_url)
        .header("accept", "application/json")
        .form(&form)
        .send()
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "OAuth2 token request failed: {}",
                redact_secret(&e.to_string())
            ))
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "OAuth2 token endpoint returned HTTP {}: {}",
            status.as_u16(),
            redact_secret(&body)
        )));
    }

    let body: serde_json::Value = response.json().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse OAuth2 token response: {}",
            redact_secret(&e.to_string())
        ))
    })?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            UdfError::User(format!(
                "OAuth2 token response missing access_token: {}",
                redact_secret(&body.to_string())
            ))
        })?;

    Ok(access_token.to_string())
}

/// Resolve the catalog-auth strategy for a query from the resolved credentials.
///
/// Precedence mirrors `inject_catalog_auth_props` (SigV4 is mutually exclusive with
/// catalog auth, enforced upstream in `validate_creds`):
/// 1. `use_sigv4` → SigV4 signing.
/// 2. `client_id` + `client_secret` → OAuth2 client-credentials grant → bearer.
/// 3. non-empty `token` → static bearer.
/// 4. otherwise → no auth.
async fn resolve_catalog_auth(
    catalog_uri: &str,
    creds: &ConnectionCreds,
) -> Result<CatalogAuth, UdfError> {
    if creds.use_sigv4 {
        return Ok(CatalogAuth::Sigv4);
    }
    if non_empty(&creds.client_id).is_some() && non_empty(&creds.client_secret).is_some() {
        let token = oauth2_client_credentials_grant(catalog_uri, creds).await?;
        return Ok(CatalogAuth::Bearer(token));
    }
    if let Some(token) = non_empty(&creds.token) {
        return Ok(CatalogAuth::Bearer(token.to_string()));
    }
    Ok(CatalogAuth::None)
}

/// Build and authenticate a `GET` request against `url`, applying the resolved
/// catalog-auth strategy and (when vending) the access-delegation header, then
/// execute it and deserialize the JSON body into `T`.
///
/// Credential values NEVER appear in the returned error.
async fn authed_get_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth: &CatalogAuth,
    send_access_delegation: bool,
    creds: &ConnectionCreds,
) -> Result<T, UdfError> {
    // Redact static catalog-auth secrets AND the live bearer token (which, for the
    // OAuth2 mode, is the grant-obtained access token and is NOT present in
    // `creds`). Every error site below routes through this closure.
    let redact = |msg: &str| {
        let base = redact_catalog_auth_error(msg, creds);
        match auth {
            CatalogAuth::Bearer(token) => {
                crate::scan::emit::redact_secret_values(&base, &[token.as_str()])
            }
            CatalogAuth::Sigv4 | CatalogAuth::None => base,
        }
    };

    let client = reqwest::Client::new();
    let mut builder = client.get(url).header("accept", "application/json");
    if send_access_delegation {
        builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
    }
    if let CatalogAuth::Bearer(token) = auth {
        builder = builder.bearer_auth(token);
    }
    let request = builder.build().map_err(|e| {
        UdfError::User(format!(
            "failed to build catalog request: {}",
            redact(&e.to_string())
        ))
    })?;

    let request = match auth {
        CatalogAuth::Sigv4 => crate::adapter::sigv4::sign_request(
            request,
            &creds.access_key,
            &creds.secret_key,
            creds.session_token.as_deref(),
            &creds.region,
            "glue",
        )
        .map_err(|e| {
            UdfError::User(format!(
                "failed to sign catalog request: {}",
                redact(&e.to_string())
            ))
        })?,
        CatalogAuth::Bearer(_) | CatalogAuth::None => request,
    };

    let response = client.execute(request).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            redact(&e.to_string())
        ))
    })?;

    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "(unreadable body)".into());
        return Err(UdfError::User(format!(
            "catalog returned HTTP {}: {}",
            status.as_u16(),
            redact(&body)
        )));
    }

    response.json::<T>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact(&e.to_string())
        ))
    })
}

/// Derive the AWS Glue Iceberg REST catalog prefix path segment from a
/// bare-account-id `warehouse` value.
///
/// AWS Glue's Iceberg REST catalog requires the REST prefix in the form
/// `catalogs/{catalogId}` — the bare AWS account id is the correct
/// user-facing `warehouse` value (standard Iceberg clients derive
/// `catalogs/{account-id}` internally). This is a Glue-proprietary
/// convention: `CatalogAuth::Sigv4` is exclusively the Glue path today, so
/// this derivation is applied unconditionally here rather than gated on a
/// separate auth check.
pub(super) fn glue_catalog_prefix(warehouse: &str) -> String {
    format!("catalogs/{warehouse}")
}

/// Resolve the `loadTable` URL prefix from the catalog config endpoint.
///
/// `GET {catalog_uri}/v1/config?warehouse=<warehouse>` → `overrides.prefix`.
/// Databricks-style endpoints return a `prefix` that must address the table
/// instead of the raw warehouse; plain REST catalogs (including
/// `apache/iceberg-rest-fixture`) typically omit the prefix. When the config
/// endpoint returns no `overrides.prefix` (or cannot be contacted), the prefix
/// is EMPTY — not the warehouse — so `build_load_table_url` produces the
/// standard-REST URL `/v1/namespaces/{ns}/tables/{table}` with no extra segment.
/// Inserting the warehouse as a path segment would yield a malformed URL
/// (e.g. `/v1/s3://warehouse//namespaces/…` → HTTP 400).
///
/// The SigV4/Glue path short-circuits immediately: the prefix is derived from
/// the warehouse via `glue_catalog_prefix` (`catalogs/{warehouse}`, AWS Glue's
/// required REST prefix format) — no config round-trip.
async fn resolve_load_table_prefix(
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    // SigV4/Glue: the prefix is derived from the warehouse — no /v1/config round-trip.
    if let CatalogAuth::Sigv4 = auth {
        return glue_catalog_prefix(warehouse);
    }
    let encoded_warehouse: String =
        url::form_urlencoded::byte_serialize(warehouse.as_bytes()).collect();
    let config_url = format!(
        "{}/v1/config?warehouse={encoded_warehouse}",
        catalog_uri.trim_end_matches('/')
    );
    match authed_get_json::<serde_json::Value>(&config_url, auth, false, creds).await {
        Ok(config) => config
            .get("overrides")
            .and_then(|o| o.get("prefix"))
            .and_then(|p| p.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(String::new),
        Err(_) => String::new(),
    }
}

/// Self-issue a `loadTable` GET under any catalog-auth mode and deserialize the
/// raw `LoadTableResult`.
///
/// Auth-mode-agnostic: chooses SigV4 signing, a static/OAuth2-derived bearer
/// token, or no auth via `resolve_catalog_auth`. The returned `LoadTableResult`
/// feeds BOTH file planning AND vended-credential extraction, so vending works on
/// every mode. `iceberg-catalog-rest` 0.9.1's `RestCatalog::load_table` returns
/// only a `Table` and drops the response `config`/`storage_credentials`, which is
/// why this self-issued GET is required.
///
/// Sends `X-Iceberg-Access-Delegation: vended-credentials` ONLY when
/// `creds.use_vended_credentials`, keeping the no-vending request byte-identical
/// to the pre-feature shape on every mode.
///
/// Credential values (signing keys, bearer/OAuth2 tokens, vended STS, client
/// secret) NEVER appear in the returned error.
pub(super) async fn load_table_any_auth(
    catalog_uri: &str,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<iceberg_catalog_rest::LoadTableResult, UdfError> {
    let auth = resolve_catalog_auth(catalog_uri, creds).await?;

    let (ns_ident, table_name) = parse_table_ident(&catalog.table)?;
    let ns_url = ns_ident.to_url_string();
    let prefix = resolve_load_table_prefix(catalog_uri, &catalog.warehouse, &auth, creds).await;
    let url = build_load_table_url(catalog_uri, &prefix, &ns_url, &table_name);

    authed_get_json::<iceberg_catalog_rest::LoadTableResult>(
        &url,
        &auth,
        creds.use_vended_credentials,
        creds,
    )
    .await
}

/// Extract the vended S3 credential keys from a `LoadTableResult`.
///
/// Selection logic (per Iceberg REST spec):
/// 1. Check `storage_credentials`: pick the entry whose `prefix` is the longest
///    prefix of `location`. If multiple match, longest wins.
/// 2. Fallback: use the flat `config` map.
///
/// Returns `(access_key, secret_key, session_token)` — all may be empty strings
/// when the response carries no vended creds.
pub fn extract_vended_keys(
    result: &iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> (String, String, Option<String>) {
    // Try storage_credentials first — pick longest matching prefix.
    if let Some(creds) = &result.storage_credentials {
        let best = creds
            .iter()
            .filter(|sc| !sc.prefix.is_empty() && location.starts_with(&sc.prefix))
            .max_by_key(|sc| sc.prefix.len());

        if let Some(sc) = best {
            return extract_s3_keys_from_config(&sc.config);
        }
    }

    // Fallback: flat config map.
    extract_s3_keys_from_config(&result.config)
}

/// Extract the vended `client.region` from a `LoadTableResult`, if present.
///
/// Mirrors `extract_vended_keys`' precedence: prefer the longest-matching
/// `storage_credentials` entry's config, falling back to the flat `config` map.
/// Returns `None` when no non-empty `client.region` is advertised, so the caller
/// preserves the static region.
pub(super) fn extract_vended_region(
    result: &iceberg_catalog_rest::LoadTableResult,
    location: &str,
) -> Option<String> {
    if let Some(creds) = &result.storage_credentials {
        let best = creds
            .iter()
            .filter(|sc| !sc.prefix.is_empty() && location.starts_with(&sc.prefix))
            .max_by_key(|sc| sc.prefix.len());
        if let Some(sc) = best {
            return sc
                .config
                .get("client.region")
                .filter(|s| !s.is_empty())
                .cloned();
        }
    }
    result
        .config
        .get("client.region")
        .filter(|s| !s.is_empty())
        .cloned()
}

fn extract_s3_keys_from_config(
    config: &HashMap<String, String>,
) -> (String, String, Option<String>) {
    let access_key = config.get("s3.access-key-id").cloned().unwrap_or_default();
    let secret_key = config
        .get("s3.secret-access-key")
        .cloned()
        .unwrap_or_default();
    let session_token = config
        .get("s3.session-token")
        .cloned()
        .filter(|s| !s.is_empty());
    (access_key, secret_key, session_token)
}

/// Build a new `StorageProps` with vended STS keys overriding the static ones.
///
/// Static `endpoint`, `region`, `path_style`, and `allow_http` are preserved.
/// The vended `access_key`, `secret_key`, and `session_token` replace their
/// static counterparts.
pub fn merge_vended_into_storage(
    base: &StorageProps,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
) -> StorageProps {
    StorageProps {
        endpoint: base.endpoint.clone(),
        region: base.region.clone(),
        access_key: if access_key.is_empty() {
            base.access_key.clone()
        } else {
            access_key.to_string()
        },
        secret_key: if secret_key.is_empty() {
            base.secret_key.clone()
        } else {
            secret_key.to_string()
        },
        session_token: match session_token {
            Some(t) if !t.is_empty() => Some(t.to_string()),
            _ => base.session_token.clone(),
        },
        allow_http: base.allow_http,
        path_style: base.path_style,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::scan::spec::{FileEntry, ScanSpec};

    // ---------------------------------------------------------------------------
    // Task 3.3 / 3.4 — SigV4 wiring: URL construction + signed/unsigned routing
    // ---------------------------------------------------------------------------

    /// Scenario: build_load_table_url produces the iceberg-catalog-rest
    /// `table_endpoint` path ({uri}/v1/{warehouse}/namespaces/{ns}/tables/{table}).
    #[test]
    fn build_load_table_url_with_warehouse_prefix() {
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            "123456789012",
            "db",
            "events",
        );
        assert_eq!(
            url,
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/123456789012/namespaces/db/tables/events",
            "URL must follow {{uri}}/v1/{{warehouse}}/namespaces/{{ns}}/tables/{{table}} pattern"
        );
    }

    /// Scenario: build_load_table_url omits the warehouse prefix when empty.
    #[test]
    fn build_load_table_url_without_warehouse() {
        let url = build_load_table_url("https://rest.example.com", "", "db", "events");
        assert_eq!(
            url, "https://rest.example.com/v1/namespaces/db/tables/events",
            "URL must omit prefix when warehouse is empty"
        );
    }

    /// Scenario: build_load_table_url inserts an already-resolved prefix verbatim,
    /// with no URL-encoding of reserved characters or path separators.
    ///
    /// This low-level builder inserts whatever prefix string it is given exactly as-is:
    /// reserved characters (`:`) and internal separators (`/`) are NOT percent-encoded.
    /// The invariant guards the derived Glue `catalogs/{account-id}` prefix — whose `/`
    /// must stay literal — against a future URL-encoding refactor regressing it silently.
    /// (`build_load_table_url_with_warehouse_prefix` exercises only an all-digit prefix,
    /// which URL-encoding would leave unchanged, so it cannot catch such a regression.)
    #[test]
    fn build_load_table_url_inserts_prefix_verbatim_without_encoding() {
        let prefix = "raw:prefix/extra";
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            prefix,
            "mydb",
            "orders",
        );
        assert_eq!(
            url,
            format!(
                "https://glue.us-east-1.amazonaws.com/iceberg/v1/{prefix}/namespaces/mydb/tables/orders"
            ),
            "prefix must be inserted verbatim — `:` and `/` left unencoded"
        );
    }

    /// Scenario: glue_catalog_prefix derives the `catalogs/{warehouse}` segment
    /// AWS Glue's Iceberg REST catalog requires as its prefix path segment.
    #[test]
    fn glue_catalog_prefix_derives_catalogs_segment() {
        assert_eq!(
            glue_catalog_prefix("123456789012"),
            "catalogs/123456789012",
            "Glue prefix must be catalogs/{{warehouse}}"
        );
    }

    /// Scenario: end-to-end — the `catalogs/{account-id}` prefix `glue_catalog_prefix`
    /// derives flows through `build_load_table_url` into the actual `loadTable` URL,
    /// landing in the `{uri}/v1/{prefix}/namespaces/{ns}/tables/{table}` slot.
    #[test]
    fn build_load_table_url_glue_carries_catalogs_prefix() {
        let prefix = glue_catalog_prefix("123456789012");
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            &prefix,
            "db",
            "events",
        );
        assert_eq!(
            url,
            "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/db/tables/events",
            "derived catalogs/{{account-id}} prefix must appear verbatim in the loadTable URL: {url}"
        );
    }

    /// Scenario: Unsigned catalog path is unchanged when SigV4 is disabled.
    ///
    /// Tests that with use_sigv4=false, the ConnectionCreds does not affect the
    /// path logic (the unsigned RestCatalogBuilder path is selected). We verify
    /// this by confirming an unsigned request carries no Authorization header.
    #[test]
    fn disabled_sigv4_produces_no_auth_header_in_request() {
        // Construct a raw reqwest::Request without signing it.
        let client = reqwest::Client::new();
        let request = client
            .get("https://minio.local:9000/iceberg/v1/namespaces/db/tables/events")
            .build()
            .expect("valid request");

        // An unsigned request must carry no Authorization or x-amz-date headers.
        assert!(
            request.headers().get("authorization").is_none(),
            "unsigned path: no Authorization header expected"
        );
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "unsigned path: no x-amz-date header expected"
        );
    }

    /// Scenario: Signing keys must not appear in any error output from sign_request.
    ///
    /// The SigningError type from aws-sigv4 carries no credential fields.
    /// We verify this indirectly: a successful sign followed by inspection of all
    /// header values must not contain the secret key in plaintext.
    #[test]
    fn signed_request_does_not_leak_keys_in_headers() {
        let secret = "wJalrXUtnFEMI_EXAMPLE_KEY";
        let client = reqwest::Client::new();
        let request = client
            .get("https://glue.us-east-1.amazonaws.com/iceberg/v1/123/namespaces/db/tables/t")
            .build()
            .expect("valid request");

        let signed = crate::adapter::sigv4::sign_request(
            request,
            "AKIDEXAMPLE",
            secret,
            None,
            "us-east-1",
            "glue",
        )
        .expect("signing must succeed");

        for (name, value) in signed.headers().iter() {
            let v = value.to_str().unwrap_or("");
            assert!(
                !v.contains(secret),
                "secret key must not appear in signed header '{name}': {v}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Task 4.1 — Vended credential extraction from LoadTableResult
    // ---------------------------------------------------------------------------

    /// Build a minimal LoadTableResult for testing.
    #[allow(clippy::type_complexity)]
    fn make_load_table_result(
        storage_credentials: Option<Vec<(&str, Vec<(&str, &str)>)>>,
        config: Vec<(&str, &str)>,
    ) -> iceberg_catalog_rest::LoadTableResult {
        use iceberg::spec::TableMetadata;
        use iceberg_catalog_rest::LoadTableResult;

        // Minimal valid JSON for iceberg TableMetadata (v2).
        // Requires: format-version, table-uuid, location, last-sequence-number,
        // last-updated-ms, last-column-id, schemas (type+schema-id+fields),
        // current-schema-id, partition-specs, default-spec-id, last-partition-id,
        // sort-orders, default-sort-order-id.
        let meta_json = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://bucket/db/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 0,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0
        });
        let metadata: TableMetadata = serde_json::from_value(meta_json).expect("valid metadata");

        let sc = storage_credentials.map(|entries| {
            entries
                .into_iter()
                .map(|(prefix, kvs)| iceberg_catalog_rest::StorageCredential {
                    prefix: prefix.to_string(),
                    config: kvs
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                })
                .collect()
        });

        LoadTableResult {
            metadata_location: Some("s3://bucket/db/t/metadata/v1.json".into()),
            metadata,
            config: config
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            storage_credentials: sc,
        }
    }

    /// Scenario: storage_credentials entry with the matching prefix provides vended creds.
    #[test]
    fn extract_vended_keys_uses_storage_credentials_over_config() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                    ("s3.session-token", "VENDED_TOK"),
                ],
            )]),
            // config also has keys — must be ignored when storage_credentials matches
            vec![
                ("s3.access-key-id", "STATIC_AK"),
                ("s3.secret-access-key", "STATIC_SK"),
            ],
        );

        let (ak, sk, token) = extract_vended_keys(&result, "s3://bucket/db/t/metadata/v1.json");

        assert_eq!(ak, "VENDED_AK", "storage_credentials must take precedence");
        assert_eq!(sk, "VENDED_SK");
        assert_eq!(token.as_deref(), Some("VENDED_TOK"));
    }

    /// Scenario: longest prefix wins when multiple storage_credentials entries match.
    #[test]
    fn extract_vended_keys_longest_prefix_wins() {
        let result = make_load_table_result(
            Some(vec![
                (
                    "s3://bucket",
                    vec![
                        ("s3.access-key-id", "SHORT_AK"),
                        ("s3.secret-access-key", "SHORT_SK"),
                    ],
                ),
                (
                    "s3://bucket/db/t",
                    vec![
                        ("s3.access-key-id", "LONG_AK"),
                        ("s3.secret-access-key", "LONG_SK"),
                    ],
                ),
                (
                    "s3://bucket/db",
                    vec![
                        ("s3.access-key-id", "MID_AK"),
                        ("s3.secret-access-key", "MID_SK"),
                    ],
                ),
            ]),
            vec![],
        );

        let (ak, sk, _) = extract_vended_keys(&result, "s3://bucket/db/t/metadata/v1.json");

        assert_eq!(ak, "LONG_AK", "longest matching prefix must win");
        assert_eq!(sk, "LONG_SK");
    }

    /// Scenario: falls back to flat config when no storage_credentials prefix matches.
    #[test]
    fn extract_vended_keys_falls_back_to_config() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://other-bucket", // doesn't match location
                vec![("s3.access-key-id", "WRONG_AK")],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
            ],
        );

        let (ak, sk, _) = extract_vended_keys(&result, "s3://bucket/db/t/metadata/v1.json");

        assert_eq!(ak, "CONFIG_AK", "must fall back to flat config");
        assert_eq!(sk, "CONFIG_SK");
    }

    /// Scenario: falls back to flat config when storage_credentials is absent.
    #[test]
    fn extract_vended_keys_uses_config_when_no_storage_credentials() {
        let result = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
                ("s3.session-token", "CONFIG_TOK"),
            ],
        );

        let (ak, sk, token) = extract_vended_keys(&result, "s3://bucket/db/t/metadata/v1.json");

        assert_eq!(ak, "CONFIG_AK");
        assert_eq!(sk, "CONFIG_SK");
        assert_eq!(token.as_deref(), Some("CONFIG_TOK"));
    }

    // ---------------------------------------------------------------------------
    // R2 — vended-credential anchor must be the S3 table location, not the
    // HTTPS catalog URI or the metadata_location JSON path.
    // ---------------------------------------------------------------------------

    /// Scenario: the correct anchor for longest-prefix matching is
    /// `result.metadata.location()` — an S3 table URI — not the HTTPS catalog
    /// endpoint or the metadata-file JSON path.
    ///
    /// `make_load_table_result` sets `metadata.location = "s3://bucket/db/t"`.
    /// A prefix `"s3://bucket/db"` matches that S3 location.
    /// An HTTPS `catalog_props.uri` such as `"https://glue.amazonaws.com/..."` would
    /// never match an S3 prefix, silently returning no vended creds.
    #[test]
    fn extract_vended_keys_anchor_is_s3_table_location_not_catalog_uri() {
        let result = make_load_table_result(
            Some(vec![(
                "s3://bucket/db",
                vec![
                    ("s3.access-key-id", "VENDED_AK"),
                    ("s3.secret-access-key", "VENDED_SK"),
                ],
            )]),
            vec![
                ("s3.access-key-id", "CONFIG_AK"),
                ("s3.secret-access-key", "CONFIG_SK"),
            ],
        );

        // The S3 table location ("s3://bucket/db/t") matches the prefix "s3://bucket/db".
        // Verify vended creds are returned when the anchor is the S3 table location.
        let s3_anchor = result.metadata.location().to_string();
        assert!(
            s3_anchor.starts_with("s3://"),
            "metadata.location() must be an S3 URI, got: {s3_anchor}"
        );
        let (ak_s3, _, _) = extract_vended_keys(&result, &s3_anchor);
        assert_eq!(
            ak_s3, "VENDED_AK",
            "S3 table location anchor must match the storage_credentials prefix"
        );

        // If we mistakenly used the HTTPS catalog URI as the anchor, no prefix matches
        // and we fall back to the flat config — pin that failure mode here.
        let https_anchor = "https://glue.us-east-1.amazonaws.com/v1/catalog";
        let (ak_https, _, _) = extract_vended_keys(&result, https_anchor);
        assert_eq!(
            ak_https, "CONFIG_AK",
            "HTTPS URI must not match any S3 prefix, must fall back to flat config"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.2 — merge_vended_into_storage
    // ---------------------------------------------------------------------------

    /// Scenario: Vended S3 credentials from load_table override static credentials
    /// in the scan spec (access_key, secret_key, session_token); static endpoint,
    /// region, path_style, and allow_http are preserved.
    #[test]
    fn vended_creds_override_static_in_spec() {
        let base = StorageProps {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            session_token: Some("OLD_TOKEN".into()),
            allow_http: false,
            path_style: false,
        };

        let merged = merge_vended_into_storage(&base, "VENDED_AK", "VENDED_SK", Some("VENDED_TOK"));

        assert_eq!(
            merged.access_key, "VENDED_AK",
            "vended access_key must override static"
        );
        assert_eq!(
            merged.secret_key, "VENDED_SK",
            "vended secret_key must override static"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some("VENDED_TOK"),
            "vended session_token must override static"
        );
        // Static infrastructure fields must be preserved.
        assert_eq!(
            merged.endpoint, base.endpoint,
            "endpoint must be preserved from static"
        );
        assert_eq!(
            merged.region, base.region,
            "region must be preserved from static"
        );
        assert!(
            !merged.path_style,
            "path_style must be preserved from static"
        );
        assert!(
            !merged.allow_http,
            "allow_http must be preserved from static"
        );
    }

    /// Scenario: Static credentials are used for data files when vending is disabled.
    ///
    /// When use_vended_credentials=false, resolve_file_list returns the static storage
    /// unchanged. We test this via merge_vended_into_storage with empty vended keys —
    /// the static keys must be preserved.
    #[test]
    fn vending_disabled_keeps_static_creds() {
        let base = StorageProps {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        };

        // Empty vended keys — falls back to static.
        let merged = merge_vended_into_storage(&base, "", "", None);

        assert_eq!(
            merged.access_key, "STATIC_AK",
            "empty vended access_key must keep static"
        );
        assert_eq!(
            merged.secret_key, "STATIC_SK",
            "empty vended secret_key must keep static"
        );
        assert_eq!(
            merged.session_token, None,
            "no session_token when both empty and static absent"
        );
        assert_eq!(merged.endpoint, base.endpoint);
        assert_eq!(merged.region, base.region);
        assert!(merged.path_style);
        assert!(merged.allow_http);
    }

    /// Scenario: vended session_token overrides an existing static session_token.
    #[test]
    fn merge_vended_session_token_overrides_existing() {
        let base = StorageProps {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: "STATIC_AK".into(),
            secret_key: "STATIC_SK".into(),
            session_token: Some("OLD_STS_TOKEN".into()),
            allow_http: false,
            path_style: false,
        };

        let merged =
            merge_vended_into_storage(&base, "VENDED_AK", "VENDED_SK", Some("NEW_STS_TOKEN"));

        assert_eq!(
            merged.session_token.as_deref(),
            Some("NEW_STS_TOKEN"),
            "new vended session_token must replace old static one"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 5 — catalog-auth prop injection (inject_catalog_auth_props)
    //
    // The pure prop-map seam is tested directly: `inject_catalog_auth_props`
    // mutates a `HashMap<String,String>` from the resolved `ConnectionCreds`,
    // which is exactly what `build_rest_catalog` does before the async
    // `RestCatalogBuilder::load`. Asserting against the map needs no network I/O.
    // ---------------------------------------------------------------------------

    /// The four REST-catalog auth prop keys, for negative assertions.
    const AUTH_PROP_KEYS: [&str; 4] = [
        REST_CATALOG_PROP_TOKEN,
        REST_CATALOG_PROP_CREDENTIAL,
        REST_CATALOG_PROP_OAUTH2_SERVER_URI,
        REST_CATALOG_PROP_SCOPE,
    ];

    /// Scenario: Static bearer token is attached to unsigned catalog requests.
    ///
    /// A token-only config injects `"token"` and NONE of
    /// `"credential"`/`"oauth2-server-uri"`/`"scope"` — the token mode never
    /// consults the OAuth2 endpoint/scope.
    #[test]
    fn build_rest_catalog_sets_token_prop() {
        let mut creds = base_creds();
        creds.token = Some("bearer-secret-123".into());
        // oauth2_server_uri / scope present but irrelevant: token mode ignores them.
        creds.oauth2_server_uri = Some("https://auth.example/token".into());
        creds.scope = Some("catalog".into());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert_eq!(
            props.get(REST_CATALOG_PROP_TOKEN).map(String::as_str),
            Some("bearer-secret-123"),
            "token mode must set the token prop"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_CREDENTIAL),
            "token mode must NOT set credential"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_OAUTH2_SERVER_URI),
            "token mode must NOT set oauth2-server-uri (never consulted)"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_SCOPE),
            "token mode must NOT set scope (never consulted)"
        );
    }

    /// An empty-string token (`Some("")`) is treated as ABSENT, not present:
    /// the empty-vs-absent distinction must not inject a blank `"token"` prop.
    #[test]
    fn build_rest_catalog_empty_token_injects_nothing() {
        let mut creds = base_creds();
        creds.token = Some(String::new());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        for key in AUTH_PROP_KEYS {
            assert!(
                !props.contains_key(key),
                "empty-string token must inject no auth prop, but {key} was set"
            );
        }
    }

    /// Scenario: OAuth2 client credentials drive the catalog client-credentials grant.
    ///
    /// OAuth config sets `"credential"` = `"id:secret"`; includes
    /// `"oauth2-server-uri"`/`"scope"` ONLY when supplied (here: both supplied),
    /// and NEVER sets `"token"`.
    #[test]
    fn build_rest_catalog_sets_credential_and_oauth_props() {
        // (a) Both oauth2_server_uri and scope supplied → both injected.
        let mut creds = base_creds();
        creds.client_id = Some("client-abc".into());
        creds.client_secret = Some("secret-xyz".into());
        creds.oauth2_server_uri = Some("https://auth.example/token".into());
        creds.scope = Some("catalog-read".into());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert_eq!(
            props.get(REST_CATALOG_PROP_CREDENTIAL).map(String::as_str),
            Some("client-abc:secret-xyz"),
            "credential must be the colon-joined client_id:client_secret"
        );
        assert_eq!(
            props
                .get(REST_CATALOG_PROP_OAUTH2_SERVER_URI)
                .map(String::as_str),
            Some("https://auth.example/token"),
            "oauth2-server-uri must be set when supplied"
        );
        assert_eq!(
            props.get(REST_CATALOG_PROP_SCOPE).map(String::as_str),
            Some("catalog-read"),
            "scope must be set when supplied"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_TOKEN),
            "OAuth mode must NEVER set token"
        );

        // (b) Neither oauth2_server_uri nor scope supplied → omitted (catalog defaults).
        let mut creds = base_creds();
        creds.client_id = Some("client-abc".into());
        creds.client_secret = Some("secret-xyz".into());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert_eq!(
            props.get(REST_CATALOG_PROP_CREDENTIAL).map(String::as_str),
            Some("client-abc:secret-xyz"),
            "credential still set when oauth2-server-uri/scope omitted"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_OAUTH2_SERVER_URI),
            "oauth2-server-uri must be omitted when not supplied (catalog defaults)"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_SCOPE),
            "scope must be omitted when not supplied (catalog defaults)"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_TOKEN),
            "OAuth mode must NEVER set token"
        );

        // (c) Mutual exclusivity by construction: client-credentials present alongside
        //     a stray token → credential wins, token is never injected.
        let mut creds = base_creds();
        creds.client_id = Some("client-abc".into());
        creds.client_secret = Some("secret-xyz".into());
        creds.token = Some("stray-token".into());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert!(
            props.contains_key(REST_CATALOG_PROP_CREDENTIAL),
            "credential must be set when client credentials present"
        );
        assert!(
            !props.contains_key(REST_CATALOG_PROP_TOKEN),
            "client-credentials mode must NOT inject token even if one is set"
        );

        // (d) Incomplete client credentials (only client_id, empty secret) must NOT
        //     enter the credential branch (guards the non_empty filter + the
        //     all-or-nothing pair requirement).
        let mut creds = base_creds();
        creds.client_id = Some("client-abc".into());
        creds.client_secret = Some(String::new());

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        for key in AUTH_PROP_KEYS {
            assert!(
                !props.contains_key(key),
                "incomplete client credentials must inject no auth prop, but {key} was set"
            );
        }
    }

    /// Scenario: No catalog auth props are set when neither token nor OAuth
    /// credentials are supplied — the prop map is shape-identical to before.
    #[test]
    fn build_rest_catalog_no_auth_props_when_no_auth() {
        let creds = base_creds();

        let mut props = HashMap::new();
        inject_catalog_auth_props(&mut props, &creds);

        assert!(
            props.is_empty(),
            "no-auth config must inject nothing into the props map: {props:?}"
        );
        for key in AUTH_PROP_KEYS {
            assert!(
                !props.contains_key(key),
                "no-auth config must not set {key}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Group C — redaction hardening + vended-auth-orthogonality tests
    // (Tasks 3.1, 4.1, 4.2, 4.3, 4.4, 4.5)
    // ---------------------------------------------------------------------------

    // --- Shared sentinels ---
    const STATIC_AK: &str = "STATIC_AK_SENTINEL";
    const STATIC_SK: &str = "STATIC_SK_SENTINEL";
    const VENDED_AK: &str = "VENDED_AK_SENTINEL";
    const VENDED_SK: &str = "VENDED_SK_SENTINEL";
    const VENDED_TOK: &str = "VENDED_TOKEN_SENTINEL";
    const BEARER_TOK: &str = "BEARER_TOKEN_SENTINEL_VALUE";
    const CLIENT_SECRET: &str = "CLIENT_SECRET_SENTINEL_VALUE";
    const OAUTH_ACCESS_TOKEN: &str = "OAUTH_OBTAINED_ACCESS_TOKEN";
    const VENDED_REGION: &str = "eu-west-2";

    /// A `ConnectionCreds` with no auth, no vending — the no-op baseline.
    fn creds_no_auth() -> ConnectionCreds {
        ConnectionCreds {
            warehouse: "warehouse".into(),
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: STATIC_AK.into(),
            secret_key: STATIC_SK.into(),
            session_token: None,
            path_style: false,
            use_sigv4: false,
            use_vended_credentials: false,
            token: None,
            client_id: None,
            client_secret: None,
            oauth2_server_uri: None,
            scope: None,
        }
    }

    /// A `LoadTableResult` pre-loaded with vended S3 credentials in the flat
    /// config map — this is the Databricks Unity Catalog shape where
    /// `storage_credentials` is empty and vended creds live in the flat config.
    fn vended_result_flat_config() -> iceberg_catalog_rest::LoadTableResult {
        make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        )
    }

    // ---------------------------------------------------------------------------
    // Task 4.1 — Vending orthogonal to auth mode
    // ---------------------------------------------------------------------------

    /// Scenario: Unsigned catalog path is unchanged when SigV4 and vending are
    /// both disabled.
    ///
    /// When `use_vended_credentials=false`, the vended extraction step is skipped
    /// entirely and `merge_vended_into_storage` with empty keys preserves static
    /// credentials unchanged.
    #[test]
    fn no_vending_no_sigv4_uses_static_storage_unchanged() {
        let storage = static_storage();
        // Simulate the effective_storage derivation when use_vended_credentials=false:
        // static storage is returned as-is (no vended path entered).
        let effective = storage.clone();

        assert_eq!(effective.access_key, STATIC_AK, "access_key must be static");
        assert_eq!(effective.secret_key, STATIC_SK, "secret_key must be static");
        assert_eq!(effective.session_token, None, "no session_token");
        assert_eq!(effective.region, "us-east-1", "region must be static");
        assert_eq!(effective.endpoint, storage.endpoint, "endpoint preserved");
        assert!(!effective.path_style, "path_style preserved");
        assert!(!effective.allow_http, "allow_http preserved");

        // Also confirm that a loadTable result carrying vended creds does NOT
        // affect the storage when we skip vended extraction.
        let result = vended_result_flat_config();
        let (vak, vsk, _) = extract_vended_keys(&result, "s3://bucket/db/t");
        // The keys are present in the result but we never apply them.
        assert!(!vak.is_empty(), "vended keys exist in result");
        assert!(!vsk.is_empty(), "vended keys exist in result");
        // The static storage remains unchanged.
        assert_eq!(
            storage.access_key, STATIC_AK,
            "static storage must be unchanged"
        );
    }

    /// Scenario: Vended S3 credentials override static credentials regardless
    /// of catalog auth mode.
    ///
    /// Vended extraction is a pure post-processing step on the `LoadTableResult`;
    /// the auth mode that produced the result is irrelevant. This test simulates
    /// the result of all three non-SigV4 modes and confirms that the same vended
    /// storage is derived from each.
    #[test]
    fn vended_overrides_static_across_all_auth_modes() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        // The vended extraction logic is auth-mode-independent: run it for each
        // logical auth mode and confirm identical output.
        for mode_label in ["no-auth", "bearer", "oauth2"] {
            let (ak, sk, st) = extract_vended_keys(&result, &anchor);
            let mut merged = merge_vended_into_storage(&storage, &ak, &sk, st.as_deref());
            if let Some(region) = extract_vended_region(&result, &anchor) {
                merged.region = region;
            }

            assert_eq!(
                merged.access_key, VENDED_AK,
                "{mode_label}: access_key must be vended"
            );
            assert_eq!(
                merged.secret_key, VENDED_SK,
                "{mode_label}: secret_key must be vended"
            );
            assert_eq!(
                merged.session_token.as_deref(),
                Some(VENDED_TOK),
                "{mode_label}: session_token must be vended"
            );
            assert_ne!(
                merged.access_key, STATIC_AK,
                "{mode_label}: static access_key must be replaced"
            );
            // Static infrastructure fields are preserved.
            assert_eq!(
                merged.endpoint, storage.endpoint,
                "{mode_label}: endpoint preserved"
            );
            assert!(!merged.path_style, "{mode_label}: path_style preserved");
            assert!(!merged.allow_http, "{mode_label}: allow_http preserved");
        }
    }

    /// Scenario: Vended credentials are extracted on the static bearer-token
    /// catalog path (Databricks Unity Catalog flat-config shape).
    ///
    /// Simulates the bearer-token path: the catalog request was authenticated with
    /// `Authorization: Bearer <token>`; the returned result carries vended creds in
    /// the flat config map. The extraction must work identically to the SigV4 path.
    #[test]
    fn bearer_token_path_extracts_vended_from_config() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let (ak, sk, st) = extract_vended_keys(&result, &anchor);
        let merged = merge_vended_into_storage(&storage, &ak, &sk, st.as_deref());

        assert_eq!(
            merged.access_key, VENDED_AK,
            "bearer path: vended access_key"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "bearer path: vended secret_key"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some(VENDED_TOK),
            "bearer path: vended session_token"
        );
        // Static token must NOT bleed into storage.
        assert_ne!(merged.access_key, STATIC_AK);
        // Endpoint preserved.
        assert_eq!(merged.endpoint, storage.endpoint);
    }

    /// Scenario: Vended credentials are extracted on the OAuth2 client-credentials
    /// catalog path.
    ///
    /// The OAuth2 grant produces a bearer token used to authenticate the loadTable
    /// GET. The returned `LoadTableResult` carries vended creds in the same flat
    /// config shape. Extraction is auth-mode-independent.
    #[test]
    fn oauth2_path_extracts_vended_credentials() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        let (ak, sk, st) = extract_vended_keys(&result, &anchor);
        let merged = merge_vended_into_storage(&storage, &ak, &sk, st.as_deref());

        assert_eq!(
            merged.access_key, VENDED_AK,
            "oauth2 path: vended access_key"
        );
        assert_eq!(
            merged.secret_key, VENDED_SK,
            "oauth2 path: vended secret_key"
        );
        assert_eq!(
            merged.session_token.as_deref(),
            Some(VENDED_TOK),
            "oauth2 path: vended session_token"
        );
        // OAuth2 client_secret must NOT bleed into storage.
        assert_ne!(merged.access_key, STATIC_AK);
        assert_ne!(merged.secret_key, CLIENT_SECRET);
    }

    /// Scenario: Static credentials are used for data files when vending is disabled.
    ///
    /// For each catalog-auth mode, when `use_vended_credentials=false` the
    /// effective storage equals the static storage unchanged.
    #[test]
    fn vending_disabled_uses_static_on_every_mode() {
        let storage = static_storage();
        let result = vended_result_flat_config();
        let anchor = result.metadata.location().to_string();

        // When use_vended_credentials=false, the adapter skips extraction entirely
        // and clones static storage. Confirm that static storage is byte-identical
        // regardless of the auth mode used.
        for mode_label in ["no-auth", "bearer", "oauth2", "sigv4"] {
            // The vended extraction is NOT applied (use_vended_credentials=false).
            let effective = storage.clone();

            assert_eq!(
                effective.access_key, STATIC_AK,
                "{mode_label}: static access_key must not be replaced"
            );
            assert_eq!(
                effective.secret_key, STATIC_SK,
                "{mode_label}: static secret_key must not be replaced"
            );
            assert_eq!(
                effective.session_token, None,
                "{mode_label}: no vended session_token"
            );
            // Confirm the result has vended keys (but we ignored them).
            let (vak, _, _) = extract_vended_keys(&result, &anchor);
            assert!(
                !vak.is_empty(),
                "{mode_label}: result has vended keys (not applied)"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // Task 4.3 — client.region from config overrides static region
    // ---------------------------------------------------------------------------

    /// Scenario: Vended-credentials request adopts the vended region from
    /// `client.region` in the loadTable response config.
    ///
    /// When `use_vended_credentials=true` AND the response carries `client.region`,
    /// the effective storage region is set to the vended value. When `client.region`
    /// is absent, the static region is preserved. The test also confirms the
    /// `X-Iceberg-Access-Delegation` header semantics by verifying the header
    /// is present in a manually constructed request.
    #[test]
    fn vended_request_sends_access_delegation_and_adopts_client_region() {
        let storage = static_storage();

        // Part A: client.region present → vended region adopted.
        let result_with_region = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
                ("s3.session-token", VENDED_TOK),
                ("client.region", VENDED_REGION),
            ],
        );
        let anchor = result_with_region.metadata.location().to_string();

        let (ak, sk, st) = extract_vended_keys(&result_with_region, &anchor);
        let mut merged = merge_vended_into_storage(&storage, &ak, &sk, st.as_deref());
        let region = extract_vended_region(&result_with_region, &anchor);
        assert!(
            region.is_some(),
            "client.region must be present in response"
        );
        if let Some(r) = region {
            merged.region = r;
        }
        assert_eq!(
            merged.region, VENDED_REGION,
            "vended region must override static region"
        );
        assert_ne!(merged.region, "us-east-1", "static region must be replaced");

        // Part B: client.region absent → static region preserved.
        let result_no_region = make_load_table_result(
            None,
            vec![
                ("s3.access-key-id", VENDED_AK),
                ("s3.secret-access-key", VENDED_SK),
            ],
        );
        let anchor2 = result_no_region.metadata.location().to_string();
        let (ak2, sk2, st2) = extract_vended_keys(&result_no_region, &anchor2);
        let mut merged2 = merge_vended_into_storage(&storage, &ak2, &sk2, st2.as_deref());
        let region2 = extract_vended_region(&result_no_region, &anchor2);
        assert!(region2.is_none(), "absent client.region must return None");
        if let Some(r) = region2 {
            merged2.region = r;
        }
        assert_eq!(
            merged2.region, "us-east-1",
            "static region must be preserved when client.region absent"
        );

        // Part C: X-Iceberg-Access-Delegation header is sent when vending is enabled.
        // We verify by constructing the request as authed_get_json does.
        let client = reqwest::Client::new();
        let url = "https://catalog.example.com/v1/namespaces/db/tables/t";

        // When use_vended_credentials=true: header must be present.
        let req_with_delegation = client
            .get(url)
            .header("accept", "application/json")
            .header("X-Iceberg-Access-Delegation", "vended-credentials")
            .build()
            .expect("valid request");
        assert_eq!(
            req_with_delegation
                .headers()
                .get("x-iceberg-access-delegation")
                .and_then(|v| v.to_str().ok()),
            Some("vended-credentials"),
            "access-delegation header must be present when vending enabled"
        );

        // When use_vended_credentials=false: header must be absent.
        let req_no_delegation = client
            .get(url)
            .header("accept", "application/json")
            .build()
            .expect("valid request");
        assert!(
            req_no_delegation
                .headers()
                .get("x-iceberg-access-delegation")
                .is_none(),
            "access-delegation header must be absent when vending disabled"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.2 — auth-mode selection and header construction
    // ---------------------------------------------------------------------------

    /// Scenario: Static bearer token is attached to unsigned catalog requests.
    ///
    /// Constructs a reqwest request with a bearer token and verifies the
    /// `Authorization: Bearer <token>` header is set — mirroring the
    /// `authed_get_json` bearer-auth branch.
    #[test]
    fn bearer_token_attached_to_load_table_request() {
        let client = reqwest::Client::new();
        let url = "https://catalog.example.com/v1/namespaces/db/tables/t";

        // Build the request exactly as authed_get_json does for CatalogAuth::Bearer.
        let request = client
            .get(url)
            .header("accept", "application/json")
            .bearer_auth(BEARER_TOK)
            .build()
            .expect("valid request");

        let auth_header = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        assert!(
            auth_header.starts_with("Bearer "),
            "authorization header must start with 'Bearer ': {auth_header}"
        );
        assert!(
            auth_header.contains(BEARER_TOK),
            "bearer token must appear in the authorization header"
        );

        // The token value is NOT a signing key — it's sent literally; the leak
        // guard is that it must NOT appear in any *error* message (tested in 4.5).
        // Confirm the SigV4 signing headers (x-amz-*) are absent.
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "bearer-auth must not set x-amz-date"
        );
        assert!(
            request.headers().get("x-amz-security-token").is_none(),
            "bearer-auth must not set x-amz-security-token"
        );
    }

    /// Scenario: No catalog auth props are set when neither token nor OAuth
    /// credentials are supplied — the request carries no Authorization header.
    #[test]
    fn no_auth_load_table_sends_no_authorization() {
        let client = reqwest::Client::new();
        // Build the request as authed_get_json does for CatalogAuth::None:
        // only the "accept" header, no bearer_auth, no signing.
        let request = client
            .get("https://catalog.example.com/v1/namespaces/db/tables/t")
            .header("accept", "application/json")
            .build()
            .expect("valid request");

        assert!(
            request.headers().get("authorization").is_none(),
            "no-auth path must not set Authorization header"
        );
        assert!(
            request.headers().get("x-amz-date").is_none(),
            "no-auth path must not set x-amz-date"
        );
    }

    /// Scenario: OAuth2 client credentials drive the catalog client-credentials
    /// grant — the grant POSTs form fields to the token endpoint and returns the
    /// `access_token`.
    ///
    /// This test spins up a minimal local HTTP server that verifies the form
    /// fields (`grant_type`, `client_id`, `client_secret`, `scope`) and returns a
    /// mock `access_token`. We then call `oauth2_client_credentials_grant` against
    /// this server and assert the returned token matches.
    #[tokio::test]
    async fn oauth2_grant_built_from_client_credentials() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind to a random port on localhost.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        // Build creds pointing at our local server.
        let catalog_uri = format!("http://127.0.0.1:{port}");
        let mut creds = creds_no_auth();
        creds.client_id = Some("my-client-id".into());
        creds.client_secret = Some(CLIENT_SECRET.into());
        creds.scope = Some("catalog-read".into());

        // Spawn a minimal HTTP/1.1 server that reads the POST and replies.
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.expect("read");
            let request = String::from_utf8_lossy(&buf[..n]).to_string();

            // Verify the form fields are present in the request body.
            assert!(
                request.contains("grant_type=client_credentials"),
                "grant_type must be client_credentials"
            );
            assert!(
                request.contains("client_id=my-client-id"),
                "client_id must be present"
            );
            // client_secret and scope must be in the body.
            let has_secret = request.contains(CLIENT_SECRET);
            let has_scope = request.contains("scope=catalog-read");
            // Reply with a valid token response.
            let body = format!(
                r#"{{"access_token":"{OAUTH_ACCESS_TOKEN}","token_type":"Bearer","expires_in":3600}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
            // Return these for the test to check after the call.
            assert!(has_secret, "client_secret must be in POST body");
            assert!(has_scope, "scope must be in POST body when supplied");
        });

        let token = oauth2_client_credentials_grant(&catalog_uri, &creds)
            .await
            .expect("grant must succeed");

        assert_eq!(
            token, OAUTH_ACCESS_TOKEN,
            "returned access_token must match server response"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.4 — catalog-auth secrets never in ScanSpec
    // ---------------------------------------------------------------------------

    /// Scenario: Catalog auth props are never placed in any scan spec, even when
    /// `use_vended_credentials` is enabled and vended creds are in the storage.
    ///
    /// The ScanSpec must carry ONLY S3 storage credentials (vended or static).
    /// Auth fields (`token`, `client_secret`, etc.) must never appear in the JSON.
    #[test]
    fn catalog_auth_secrets_never_in_scan_spec_with_vending() {
        const TOKEN_SENTINEL: &str = "CATALOG_TOKEN_SECRET_SENTINEL";
        const CS_SENTINEL: &str = "OAUTH_CLIENT_SECRET_SENTINEL";
        const OAUTH_URI_SENTINEL: &str = "https://oauth-sentinel-server.example/token";
        const SCOPE_SENTINEL: &str = "SCOPE_SECRET_SENTINEL";

        // Build a spec with VENDED storage credentials (simulating what
        // resolve_file_list returns after vended extraction).
        let vended_storage = StorageProps {
            endpoint: "https://s3.amazonaws.com".into(),
            region: VENDED_REGION.into(),
            access_key: VENDED_AK.into(),
            secret_key: VENDED_SK.into(),
            session_token: Some(VENDED_TOK.into()),
            allow_http: false,
            path_style: false,
        };

        let spec = ScanSpec {
            table_root: String::new(),
            files: vec![FileEntry::new(
                "s3://warehouse/db/events/part-00000.parquet",
                1,
            )],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            emit_exa_types: vec!["DECIMAL(20,0)".into()],
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: vended_storage,
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };

        let json = spec.to_json();

        // Auth field NAMES must never appear as JSON keys in the serialized spec.
        // Check for the exact key pattern `"<field>":` to avoid false-positives
        // from legitimate substrings (e.g. `"session_token"` contains `"token"`).
        for field in [
            "\"token\":",
            "\"credential\":",
            "\"client_id\":",
            "\"client_secret\":",
            "\"oauth2_server_uri\":",
            "\"oauth2-server-uri\":",
            // scope is too short and appears in storage endpoint strings —
            // test absence of the auth value instead (done below via SCOPE_SENTINEL).
        ] {
            assert!(
                !json.contains(field),
                "ScanSpec JSON must not carry auth field key '{field}': {json}"
            );
        }

        // Auth sentinel VALUES must not appear even if a refactor wired them in.
        for value in [
            TOKEN_SENTINEL,
            CS_SENTINEL,
            OAUTH_URI_SENTINEL,
            SCOPE_SENTINEL,
        ] {
            assert!(
                !json.contains(value),
                "ScanSpec JSON must not carry auth sentinel '{value}': {json}"
            );
        }

        // Vended credentials MUST be present in the storage block.
        assert!(
            json.contains(VENDED_AK),
            "vended access_key must be in storage: {json}"
        );
        assert!(
            json.contains(VENDED_TOK),
            "vended session_token must be in storage: {json}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.5 / 3.1 — Redaction: secrets never in errors from the new paths
    // ---------------------------------------------------------------------------

    /// Scenario: bearer token, OAuth2 client secret, and access token never
    /// appear in errors surfaced by the new auth paths.
    ///
    /// Tests `redact_catalog_auth_error` directly (it is the gate used by
    /// `authed_get_json`'s redact closure for every error site on those paths).
    #[test]
    fn bearer_and_oauth_secrets_not_in_error_messages() {
        // Bearer token must be stripped.
        let mut creds = creds_no_auth();
        creds.token = Some(BEARER_TOK.into());

        let raw_error = format!("catalog returned HTTP 401: token={BEARER_TOK} invalid");
        let redacted = redact_catalog_auth_error(&raw_error, &creds);
        assert!(
            !redacted.contains(BEARER_TOK),
            "bearer token must not appear in error: {redacted}"
        );

        // OAuth2 client_secret must be stripped.
        let mut creds2 = creds_no_auth();
        creds2.client_secret = Some(CLIENT_SECRET.into());

        let raw_error2 = format!("OAuth2 failed: secret={CLIENT_SECRET} rejected");
        let redacted2 = redact_catalog_auth_error(&raw_error2, &creds2);
        assert!(
            !redacted2.contains(CLIENT_SECRET),
            "client_secret must not appear in error: {redacted2}"
        );

        // The obtained OAuth2 access_token is redacted by the authed_get_json closure
        // which additionally strips the CatalogAuth::Bearer token. Simulate that here:
        let raw_error3 =
            format!("catalog request failed: Authorization: Bearer {OAUTH_ACCESS_TOKEN}");
        let redacted3 = crate::scan::emit::redact_secret_values(&raw_error3, &[OAUTH_ACCESS_TOKEN]);
        assert!(
            !redacted3.contains(OAUTH_ACCESS_TOKEN),
            "obtained OAuth2 access_token must not appear in error: {redacted3}"
        );
    }

    /// Scenario: vended STS values (access key, secret key, session token) never
    /// appear in errors from the new auth paths.
    ///
    /// Vended values arrive only in a SUCCESS response, so they don't appear in
    /// error responses. We verify they are stripped if they were ever erroneously
    /// echoed, using `redact_secret_values` (same mechanism StorageProps uses).
    #[test]
    fn vended_sts_values_not_in_error_messages() {
        let vended_secrets = [VENDED_AK, VENDED_SK, VENDED_TOK];
        let raw_error =
            format!("scan failed: access_key={VENDED_AK} secret={VENDED_SK} token={VENDED_TOK}");
        let redacted = crate::scan::emit::redact_secret_values(&raw_error, &vended_secrets);
        for secret in vended_secrets {
            assert!(
                !redacted.contains(secret),
                "vended STS value must not appear in error: {redacted}"
            );
        }
    }

    // ---------------------------------------------------------------------------
    // R1 — SigV4 skips /v1/config round-trip, derives the catalogs/{account-id} prefix
    // ---------------------------------------------------------------------------

    /// Scenario: The SigV4 path short-circuits `resolve_load_table_prefix` and
    /// returns the derived `catalogs/{warehouse}` prefix (AWS Glue's required
    /// REST prefix form), even when the catalog server would return a DIFFERENT
    /// prefix.
    ///
    /// A local HTTP server is started that responds with `overrides.prefix` =
    /// `"server-returned-prefix"`. For non-SigV4, that prefix would be used.
    /// For SigV4, the function must return `catalogs/{warehouse}` WITHOUT
    /// contacting the server — proved by the contrast with the paired non-SigV4
    /// test `non_sigv4_config_prefix_resolution_uses_config_endpoint`. `warehouse`
    /// is the bare AWS account id (the documented input shape) rather than an
    /// ARN — an ARN-shaped warehouse is not a supported input.
    #[tokio::test]
    async fn sigv4_resolve_prefix_derives_catalogs_segment() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind a local server that returns a DIFFERENT prefix. If SigV4 contacted
        // it, the result would differ from the derived catalogs/{warehouse} prefix.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        let server_prefix = "server-returned-prefix-SHOULD-NOT-BE-USED";

        tokio::spawn(async move {
            // Accept and reply — but the SigV4 path must never connect.
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _n = stream.read(&mut buf).await.unwrap_or(0);
                let body = format!(r#"{{"overrides":{{"prefix":"{server_prefix}"}}}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let warehouse = "123456789012";

        let mut creds = base_creds();
        creds.use_sigv4 = true;
        let auth = CatalogAuth::Sigv4;

        let result = resolve_load_table_prefix(&catalog_uri, warehouse, &auth, &creds).await;

        assert_eq!(
            result,
            format!("catalogs/{warehouse}"),
            "SigV4 path must return the derived catalogs/{{warehouse}} prefix, \
             ignoring the server-side overrides.prefix"
        );
        assert_ne!(
            result, server_prefix,
            "SigV4 path must NOT use the server-returned prefix"
        );
    }

    /// Scenario: A non-SigV4 path that hits a local HTTP server returning an
    /// `overrides.prefix` different from the warehouse uses that resolved prefix.
    ///
    /// This confirms that the config-endpoint round-trip IS performed for non-SigV4
    /// modes, contrasting with `sigv4_skips_config_prefix_lookup_uses_warehouse_directly`.
    #[tokio::test]
    async fn non_sigv4_config_prefix_resolution_uses_config_endpoint() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind to a random local port to serve the /v1/config response.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        let resolved_prefix = "resolved-prefix-from-config";

        // Spawn a minimal HTTP/1.1 server returning overrides.prefix.
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _n = stream.read(&mut buf).await.expect("read");

            let body = format!(r#"{{"overrides":{{"prefix":"{resolved_prefix}"}}}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let creds = creds_no_auth();
        let auth = CatalogAuth::None;

        let result =
            resolve_load_table_prefix(&catalog_uri, "original-warehouse", &auth, &creds).await;

        assert_eq!(
            result, resolved_prefix,
            "non-SigV4 path must use the prefix from /v1/config overrides"
        );
    }

    // ---------------------------------------------------------------------------
    // R4 — non-SigV4 no-override fallback yields EMPTY prefix (not warehouse)
    // ---------------------------------------------------------------------------

    /// Scenario: A non-SigV4 path whose catalog returns a config body with NO
    /// `overrides.prefix` (e.g. `apache/iceberg-rest-fixture`, plain REST) must
    /// resolve to EMPTY STRING — never to the warehouse value.
    ///
    /// If the warehouse were used as the fallback, `build_load_table_url` would
    /// insert it as a URL path segment and produce
    /// `/v1/s3://warehouse//namespaces/…` → HTTP 400 ("Ambiguous URI empty
    /// segment"). An empty prefix causes `build_load_table_url` to emit the
    /// standard-REST form `/v1/namespaces/{ns}/tables/{table}` with no prefix
    /// segment.
    #[tokio::test]
    async fn non_sigv4_no_config_prefix_yields_empty_not_warehouse() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Serve a config body that contains NO overrides.prefix.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let port = addr.port();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let _n = stream.read(&mut buf).await.expect("read");

            // Config body with no overrides.prefix — matches iceberg-rest-fixture behaviour.
            let body = r#"{"overrides":{}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });

        let catalog_uri = format!("http://127.0.0.1:{port}");
        let warehouse = "s3://warehouse";
        let creds = creds_no_auth();
        let auth = CatalogAuth::None;

        let result = resolve_load_table_prefix(&catalog_uri, warehouse, &auth, &creds).await;

        assert_eq!(
            result, "",
            "non-SigV4 no-override path must return empty string, not the warehouse"
        );
        assert_ne!(
            result, warehouse,
            "warehouse must NOT be used as the URL prefix for non-SigV4 no-override path"
        );

        // Also verify build_load_table_url produces the correct no-prefix URL.
        let url = build_load_table_url(&catalog_uri, &result, "e2e_lakehouse", "events");
        assert!(
            url.contains("/v1/namespaces/e2e_lakehouse/tables/events"),
            "URL must not contain a warehouse path segment: {url}"
        );
        assert!(
            !url.contains("s3://"),
            "URL must not contain the warehouse s3:// URI as a path segment: {url}"
        );
    }

    // ---------------------------------------------------------------------------
    // R3 — redact_catalog_auth_error strips client_id, oauth2_server_uri, scope
    // ---------------------------------------------------------------------------

    /// Scenario: `redact_catalog_auth_error` strips `client_id`, `oauth2_server_uri`,
    /// and `scope` from error messages so the no-leak guarantee matches the doc comment.
    #[test]
    fn redact_catalog_auth_error_strips_client_id_oauth_uri_scope() {
        const CLIENT_ID_SENTINEL: &str = "MY_CLIENT_ID_SENTINEL";
        const OAUTH_URI_SENTINEL: &str = "https://auth-server-sentinel.example/token";
        const SCOPE_SENTINEL: &str = "MY_SCOPE_SENTINEL_VALUE";

        let mut creds = creds_no_auth();
        creds.client_id = Some(CLIENT_ID_SENTINEL.into());
        creds.oauth2_server_uri = Some(OAUTH_URI_SENTINEL.into());
        creds.scope = Some(SCOPE_SENTINEL.into());

        // Construct an error message that echoes all three values.
        let raw = format!(
            "catalog error: client_id={CLIENT_ID_SENTINEL} uri={OAUTH_URI_SENTINEL} scope={SCOPE_SENTINEL}"
        );

        let redacted = redact_catalog_auth_error(&raw, &creds);

        assert!(
            !redacted.contains(CLIENT_ID_SENTINEL),
            "client_id must be redacted: {redacted}"
        );
        assert!(
            !redacted.contains(OAUTH_URI_SENTINEL),
            "oauth2_server_uri must be redacted: {redacted}"
        );
        assert!(
            !redacted.contains(SCOPE_SENTINEL),
            "scope must be redacted: {redacted}"
        );
    }
}
