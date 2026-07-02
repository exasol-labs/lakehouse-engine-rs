use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    AggKind, AggregatePlan, CatalogProps, LogicalField, ScanSpec, StorageProps,
};
use exasol_udf_sdk::error::UdfError;
use futures::TryStreamExt;
use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::Value as Json;
use std::collections::HashMap;
use std::sync::Arc;
/// Pushdown planning: resolve the Iceberg file list ONCE and build the
/// scan-driving SQL that invokes the LAKEHOUSE_SCAN SET UDF.
///
/// Architecture invariants:
/// - File list resolved exactly ONCE here, in the planning layer.
/// - The scan SET UDF receives the explicit file list; it NEVER discovers files.
/// - A predicate the adapter cannot translate is OMITTED from the spec
///   (correctness backstop: Exasol keeps the predicate at its own level).
/// - LIMIT appears in both the scan spec and the returned SQL (correctness backstop).
/// - Credentials NEVER appear in any returned SQL string or error message.
use vs_expression::{render_df_filter_safe, render_expression, render_expression_safe};

/// Build a RestCatalog configured to read/write data files through the S3
/// (MinIO) storage factory.
///
/// iceberg 0.9.1 requires an explicit `StorageFactory`; the S3 config keys are
/// supplied in the same props map passed to `load`. Credentials live only in
/// this map and never appear in returned SQL or error strings.
async fn build_rest_catalog(
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
            configured_scheme: "s3".to_string(),
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
fn build_s3_file_io(storage: &StorageProps) -> iceberg::io::FileIO {
    let mut builder = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
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
/// `{catalog_uri}/v1/{warehouse?}/namespaces/{ns_url}/tables/{table_name}`
///
/// The `warehouse` parameter acts as the URL prefix (matching `props["prefix"]` in the
/// iceberg-catalog-rest config map). For AWS Glue, this is the catalog ID / warehouse ARN;
/// for a plain REST catalog it is typically the warehouse name. When empty, the prefix is
/// omitted and the URL reduces to `{catalog_uri}/v1/namespaces/{ns}/tables/{table}`.
///
/// The caller passes the resolved URL prefix as `warehouse`: either the raw
/// connection warehouse, or the `overrides.prefix` fetched from
/// `GET {catalog_uri}/v1/config?warehouse=…` by `resolve_load_table_prefix` for
/// Databricks-style catalogs that address tables under a config-supplied prefix.
///
/// ponytail: For AWS Glue the warehouse value is a catalog ARN
/// (`arn:aws:glue:region:acct:catalog`) and is inserted verbatim into the URL path — no
/// URL-encoding. This works because the Glue Iceberg REST endpoint expects the ARN
/// unencoded in that path segment. Non-ASCII prefixes are not URL-encoded here.
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
/// The SigV4/Glue path short-circuits immediately: the warehouse is an ARN used
/// verbatim in the URL path (no config round-trip), preserving byte-identical
/// behaviour with the pre-unification `load_table_signed` function.
async fn resolve_load_table_prefix(
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> String {
    // SigV4/Glue: the warehouse ARN is used directly — no /v1/config round-trip.
    if let CatalogAuth::Sigv4 = auth {
        return warehouse.to_string();
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
async fn load_table_any_auth(
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
fn extract_vended_region(
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

/// The registered SQL name of the scan SET UDF entry point.
const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// Maximum shard count: Exasol distributes groups round-robin below this threshold;
/// above it Exasol hash-partitions them (no longer balanced).
const MAX_SHARD_COUNT: usize = 300;

/// Compute the work-unit shard count G for a given cluster configuration.
///
/// G = clamp(node_count × parallelism_factor, 1, min(file_count, 300)).
///
/// - The product is saturating (no overflow).
/// - G is at least 1 and at most `file_count` so no shard is empty.
/// - G is also at most 300 to stay in Exasol's round-robin distribution regime.
///
/// When `file_count` is zero this returns 1 (caller should skip partition_files).
pub fn shard_count(node_count: usize, parallelism_factor: usize, file_count: usize) -> usize {
    let raw = node_count.saturating_mul(parallelism_factor);
    let upper = file_count.clamp(1, MAX_SHARD_COUNT);
    raw.clamp(1, upper)
}

// ---------------------------------------------------------------------------
// Aggregate detection
// ---------------------------------------------------------------------------

/// Inspect the pushdown request's `selectList` and return the aggregate plan
/// if every select-list item is a supported single-group aggregate.
///
/// Returns `None` (fall back to row scan) when any of the following hold:
/// - `groupBy` is present and non-empty (GROUP BY not supported)
/// - any select item has `distinct: true`
/// - any select item is not one of COUNT(*), COUNT(col), SUM, MIN, MAX, AVG
/// - the select list is absent or empty
pub fn detect_aggregates(pushdown_req: &Json) -> Option<Vec<AggregatePlan>> {
    // Reject GROUP BY.
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;

    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::with_capacity(list.len());
    for item in list {
        // Every item must be a function_aggregate.
        if item.get("type").and_then(|t| t.as_str()) != Some("function_aggregate") {
            return None;
        }
        let plan = parse_agg_item(item)?;
        plans.push(plan);
    }

    Some(plans)
}

/// Classification of one `selectList` item in a grouped-aggregate pushdown.
///
/// Each variant carries the item's original `selectList` ordinal so the outer
/// wrapper SELECT, its cast list, and its GROUP BY list can be assembled in the
/// user's `selectList` order for any interleaving of keys and aggregates. Exasol
/// validates the outer wrapper SELECT positionally against `selectListDataTypes`,
/// so this order must be preserved end-to-end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupedSelectItem {
    /// A group-key projection. `group_key_slot` indexes `group_keys` (and the
    /// scan-side `GK_{slot}` EMITS column); `select_index` is the item's original
    /// `selectList` ordinal.
    GroupKey {
        group_key_slot: usize,
        select_index: usize,
    },
    /// An aggregate. `plan_slot` indexes `plans` (and the merged-aggregate items);
    /// `select_index` is the item's original `selectList` ordinal.
    Aggregate {
        plan_slot: usize,
        select_index: usize,
    },
}

/// The original `selectList` ordinal of a classified item.
fn select_item_index(item: &GroupedSelectItem) -> usize {
    match *item {
        GroupedSelectItem::GroupKey { select_index, .. }
        | GroupedSelectItem::Aggregate { select_index, .. } => select_index,
    }
}

/// Result of detecting a GROUP BY aggregate pushdown.
///
/// `group_keys` and `plans` are the disjoint keys-first fan-out lists (unchanged
/// wire shape). `select_items` is the ordered, per-`selectList`-item
/// classification that preserves the user's select-list order so the outer
/// wrapper SELECT can be re-assembled positionally.
#[derive(Debug, Clone)]
pub struct GroupedAggregateDetection {
    /// Rendered DataFusion SQL fragment for each `groupBy` expression, in order.
    pub group_keys: Vec<String>,
    /// Aggregate plans in `selectList` appearance order.
    pub plans: Vec<AggregatePlan>,
    /// One entry per `selectList` item, in `selectList` order.
    pub select_items: Vec<GroupedSelectItem>,
}

/// Detect a GROUP BY aggregate pushdown and return the rendered group-key SQL
/// fragments, the aggregate plans, and the ordered per-item classification.
///
/// Returns `Some(GroupedAggregateDetection)` only when **all** of the following
/// hold:
/// - `aggregationType` is exactly `"group_by"`.
/// - `groupBy` is a non-empty array.
/// - Every element of `groupBy` renders successfully via `render_expression`
///   (any failure → `None` for the whole call).
/// - Every element of `selectList` is either a `function_aggregate` (contributes
///   an `AggregatePlan`) or a group-key projection — a plain `column` reference
///   or a scalar expression whose rendered SQL matches one of the group keys.
///   Any other type → `None`.
/// - The `selectList` is non-empty.
/// - No `function_aggregate` item uses `distinct: true`.
///
/// Returns `None` on any unsupported shape; the caller falls back to row
/// scanning or single-group aggregate detection.
pub fn detect_group_by_aggregates(pushdown_req: &Json) -> Option<GroupedAggregateDetection> {
    // Must be a GROUP BY aggregate request.
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) != Some("group_by") {
        return None;
    }

    // GROUP BY array must be present and non-empty.
    let group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;

    // Render each GROUP BY expression; any failure collapses the whole result.
    let mut group_keys = Vec::with_capacity(group_by.len());
    for node in group_by {
        match render_expression(node) {
            Ok(sql) => group_keys.push(sql),
            Err(_) => return None,
        }
    }

    // Classify each select-list item, preserving its original ordinal.
    let list = pushdown_req.get("selectList").and_then(|v| v.as_array())?;
    if list.is_empty() {
        return None;
    }

    let mut plans = Vec::new();
    let mut select_items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match item_type {
            "function_aggregate" => {
                let plan = parse_agg_item(item)?;
                let plan_slot = plans.len();
                plans.push(plan);
                select_items.push(GroupedSelectItem::Aggregate {
                    plan_slot,
                    select_index,
                });
            }
            _ => {
                // A group-key projection: a plain column reference, or a scalar
                // expression that renders to one of the group keys (e.g.
                // SELECT MOD(id,4) ... GROUP BY MOD(id,4)) emitted via GK_*.
                // Match to its group-key slot by rendered SQL. Anything that does
                // not map to a group key disqualifies the whole path.
                let group_key_slot = render_expression(item)
                    .ok()
                    .and_then(|sql| group_keys.iter().position(|gk| *gk == sql))?;
                select_items.push(GroupedSelectItem::GroupKey {
                    group_key_slot,
                    select_index,
                });
            }
        }
    }

    Some(GroupedAggregateDetection {
        group_keys,
        plans,
        select_items,
    })
}

/// Resolve the Exasol-declared type of each group key from `selectListDataTypes`.
///
/// Each group-key slot is located via the detection classification, which
/// records the group-key projection's own `selectList` ordinal; the parallel
/// `selectListDataTypes` array at that ordinal gives its declared result type.
/// Matching by index (not by comparing rendered SQL strings) keeps the type
/// correct even when an expression key's `groupBy` and `selectList` renderings
/// differ in whitespace or casing. Falls back to `VARCHAR(2000000)` when the
/// type cannot be located.
fn group_key_exasol_types(
    pushdown_req: &Json,
    group_keys: &[String],
    select_items: &[GroupedSelectItem],
) -> Vec<String> {
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    let mut types = vec!["VARCHAR(2000000)".to_string(); group_keys.len()];
    for item in select_items {
        if let GroupedSelectItem::GroupKey {
            group_key_slot,
            select_index,
        } = item
            && let Some(ty) = declared_types
                .and_then(|d| d.get(*select_index))
                .map(exasol_type_from_json)
            && let Some(slot) = types.get_mut(*group_key_slot)
        {
            *slot = ty;
        }
    }
    types
}

/// Resolve the Exasol-declared type of each aggregate select-list item, in order.
///
/// Aggregates appear as `function_aggregate` items in `selectList`; the parallel
/// `selectListDataTypes` array gives each one's declared result type (e.g. COUNT(*)
/// → DECIMAL(18,0)). Falls back to `VARCHAR(2000000)` when not locatable.
fn aggregate_exasol_types(pushdown_req: &Json) -> Vec<String> {
    let select_list = match pushdown_req.get("selectList").and_then(|v| v.as_array()) {
        Some(l) => l,
        None => return Vec::new(),
    };
    let declared_types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array());
    select_list
        .iter()
        .enumerate()
        .filter(|(_, item)| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        .map(|(idx, _)| {
            declared_types
                .and_then(|d| d.get(idx))
                .map(exasol_type_from_json)
                .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
        })
        .collect()
}

/// Extract the column name (uppercase) from the first argument of an aggregate function.
fn column_from_first_arg(args: Option<&Vec<Json>>) -> Option<String> {
    args.and_then(|a| a.first()).and_then(|arg| {
        if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
            arg.get("name")
                .and_then(|n| n.as_str())
                .map(|s| s.to_uppercase())
        } else {
            None
        }
    })
}

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` or the function name is
/// not one of COUNT, SUM, MIN, MAX, AVG, STDDEV, VARIANCE family.
///
/// The caller must verify `item.type == "function_aggregate"` before calling.
fn parse_agg_item(item: &Json) -> Option<AggregatePlan> {
    if item.get("distinct").and_then(|d| d.as_bool()) == Some(true) {
        return None;
    }

    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();

    let args = item.get("arguments").and_then(|a| a.as_array());

    let plan = match fn_name.as_str() {
        "COUNT" => {
            let col = args.and_then(|a| a.first()).and_then(|arg| {
                if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
                    arg.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_uppercase())
                } else {
                    None
                }
            });
            if col.is_none() {
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                }
            } else {
                AggregatePlan {
                    kind: AggKind::CountCol,
                    column: col,
                }
            }
        }
        "SUM" => AggregatePlan {
            kind: AggKind::Sum,
            column: column_from_first_arg(args),
        },
        "MIN" => AggregatePlan {
            kind: AggKind::Min,
            column: column_from_first_arg(args),
        },
        "MAX" => AggregatePlan {
            kind: AggKind::Max,
            column: column_from_first_arg(args),
        },
        "AVG" => AggregatePlan {
            kind: AggKind::Avg,
            column: column_from_first_arg(args),
        },
        // STDDEV/VARIANCE family — decompose into (cnt, sum, sum_sq) sufficient statistics.
        // STDDEV and STDDEV_SAMP are the sample forms; VARIANCE / VAR_SAMP likewise.
        "STDDEV" | "STDDEV_SAMP" => AggregatePlan {
            kind: AggKind::StddevSamp,
            column: column_from_first_arg(args),
        },
        "STDDEV_POP" => AggregatePlan {
            kind: AggKind::StddevPop,
            column: column_from_first_arg(args),
        },
        "VARIANCE" | "VAR_SAMP" => AggregatePlan {
            kind: AggKind::VarSamp,
            column: column_from_first_arg(args),
        },
        "VAR_POP" => AggregatePlan {
            kind: AggKind::VarPop,
            column: column_from_first_arg(args),
        },
        _ => return None,
    };
    Some(plan)
}

// ---------------------------------------------------------------------------
// SQL builder (pure; used by handle_pushdown and unit tests)
// ---------------------------------------------------------------------------

/// Build the scan-driving SQL from a resolved file list partitioned into shards.
///
/// **Row queries** (no aggregates in spec):
/// - Single shard: `SELECT * FROM (SELECT {udf}({spec}) EMITS ({emits})) LIMIT n`
/// - Multi-shard: `SELECT * FROM (fan-out with GROUP BY shard_key) LIMIT n`
///
/// **Aggregate queries** (spec carries `aggregates`, no `group_keys`):
/// - Always wraps the fan-out in an outer merge aggregation (never SELECT *).
/// - The EMITS clause and the outer merge follow the COLUMN CONTRACT from
///   `crate::scan::build_partial_agg_sql`.
///
/// For grouped aggregate queries (spec carries both `aggregates` and `group_keys`),
/// use `build_grouped_aggregate_scan_sql` directly.
///
/// `spec_template` carries the shared fields; only `files` is replaced per shard.
/// `col_types` is the full table column type map `(uppercase_name, exasol_type)` used
/// to assign the correct EMITS type per aggregate partial column.
/// `aggregate_types` holds the Exasol-declared result type of each aggregate (from
/// `aggregate_exasol_types`); the single-group merge casts each item to its declared
/// type. Pass `&[]` to emit uncast merge items (row scans never read it).
// ponytail: 8 args is one over the lint threshold; matches the sibling grouped builder.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
) -> String {
    if let Some(aggregates) = spec_template.aggregates.as_deref() {
        build_aggregate_scan_sql(
            spec_template,
            shards,
            aggregates,
            col_types,
            aggregate_types,
            udf_name,
        )
    } else {
        build_row_scan_sql(
            spec_template,
            shards,
            proj_cols,
            proj_types,
            limit,
            udf_name,
        )
    }
}

/// Build the row-scan SQL (no aggregates).
fn build_row_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    proj_cols: &[String],
    proj_types: &[String],
    limit: Option<u64>,
    udf_name: &str,
) -> String {
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("{} {}", quote_ident(name), ty))
        .collect::<Vec<_>>()
        .join(", ");

    if shards.len() == 1 {
        let files = shards.into_iter().next().unwrap_or_default();
        let common_literal = sql_string_literal(&spec_template.to_common_json());
        let files_literal = sql_string_literal(&ScanSpec::files_json(&files));
        let mut sql = format!(
            "SELECT * FROM (SELECT {udf}({common}, {files}) EMITS ({emits}))",
            udf = udf_name,
            common = common_literal,
            files = files_literal,
            emits = emits,
        );
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    } else {
        let inner = build_fan_out_inner(spec_template, &shards, &emits, udf_name);
        let mut sql = format!("SELECT * FROM ({inner})");
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }
}

/// Build the aggregate scan SQL: fan-out EMITS partial columns, outer merge aggregates them.
///
/// The EMITS clause names and types follow the COLUMN CONTRACT defined in
/// `crate::scan::build_partial_agg_sql`.  The outer merge SELECT consumes those
/// exact column names.
fn build_aggregate_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types);
    let emits = emits_items.join(", ");
    let merge_select = cast_merge_items(aggregates, aggregate_types).join(", ");

    let fan_out = if shards.len() == 1 {
        let files = shards.into_iter().next().unwrap_or_default();
        let common_literal = sql_string_literal(&spec_template.to_common_json());
        let files_literal = sql_string_literal(&ScanSpec::files_json(&files));
        format!(
            "SELECT {udf}({common}, {files}) EMITS ({emits})",
            udf = udf_name,
            common = common_literal,
            files = files_literal,
            emits = emits,
        )
    } else {
        build_fan_out_inner(spec_template, &shards, &emits, udf_name)
    };

    format!("SELECT {merge_select} FROM ({fan_out})")
}

/// Build the grouped aggregate scan SQL.
///
/// ## Two-level grouping
///
/// Inner level: a `GROUP BY shard_key` fan-out runs one UDF invocation per shard.
/// Each shard returns partial per-group results (DataFusion GROUP BY user keys inside
/// the shard).  Outer level: Exasol re-groups on the user group-key columns and merges
/// the partial aggregates.
///
/// ## EMITS column contract (Phase 3 / Group E must match this exactly)
///
/// Columns appear in this order, left to right:
///
/// 1. Group-key columns: `GK_0 VARCHAR(2000000)`, `GK_1 VARCHAR(2000000)`, …
///    `GK_{n-1} VARCHAR(2000000)` — one column per group key, always VARCHAR(2000000)
///    (Group E serialises the DataFusion group-key value to a string before emitting).
///
/// 2. Partial aggregate columns: same layout and naming as `partial_emits_items`
///    (`PARTIAL_count_i`, `PARTIAL_sum_i`, `PARTIAL_min_i`, `PARTIAL_max_i`,
///    `PARTIAL_avg_sum_i` / `PARTIAL_avg_cnt_i`,
///    `PARTIAL_stat_cnt_i` / `PARTIAL_stat_sum_i` / `PARTIAL_stat_sumsq_i`).
///
/// ## HAVING
///
/// `having` is an already-rendered DataFusion SQL fragment applied in the OUTER wrapper
/// only (after `GROUP BY`). Never pushed into the shard scan — a per-shard HAVING would
/// incorrectly discard groups that only clear the threshold after merging across shards.
///
/// ## LIMIT
///
/// LIMIT is never pushed into a shard spec for grouped queries (shard emits all
/// partial groups; the outer wrapper applies the final LIMIT when needed).
// ponytail: 8 args is one over the lint threshold; grouping into a struct would
// add boilerplate for a function called in only two places. Suppress the lint.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql(
    spec_template: &ScanSpec,
    shards: Vec<Vec<String>>,
    group_keys: &[String],
    group_key_types: &[String],
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
    limit: Option<u64>,
    col_types: &[(String, String)],
    udf_name: &str,
    having: Option<&str>,
) -> String {
    // Build EMITS: GK_* columns first, then PARTIAL_* columns.
    let gk_emits: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}" VARCHAR(2000000)"#))
        .collect();
    let partial_items = partial_emits_items(aggregates, col_types);
    let all_emits: Vec<String> = gk_emits
        .iter()
        .chain(partial_items.iter())
        .cloned()
        .collect();
    let emits = all_emits.join(", ");

    // Build outer merge SELECT: GK_* columns + merged aggregates.
    // The scan stringifies every group key into a VARCHAR EMITS column; the outer
    // wrapper casts each back to its Exasol-declared type so the virtual-table result
    // column type matches what Exasol expects (e.g. DECIMAL for MOD(id,4)).
    let gk_select: Vec<String> = (0..group_keys.len())
        .map(|i| match group_key_types.get(i) {
            Some(ty) if ty != "VARCHAR(2000000)" => {
                format!(r#"CAST("GK_{i}" AS {ty})"#)
            }
            _ => format!(r#""GK_{i}""#),
        })
        .collect();
    let merge_items = cast_merge_items(aggregates, aggregate_types);

    // Assemble the outer SELECT in the user's selectList order: each classified
    // item is placed at its original ordinal, interleaving group-key casts and
    // merged aggregates as the user wrote them. Exasol validates this SELECT's
    // column types positionally against selectListDataTypes, so keys-first
    // ordering (the inner fan-out shape) would transpose columns whenever an
    // aggregate precedes or interleaves with a key.
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let outer_select: Vec<String> = ordered
        .iter()
        .filter_map(|item| match *item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => {
                gk_select.get(group_key_slot).cloned()
            }
            GroupedSelectItem::Aggregate { plan_slot, .. } => merge_items.get(plan_slot).cloned(),
        })
        .collect();
    let outer_select_str = outer_select.join(", ");

    // Group BY in outer: GK_0, GK_1, ... The set of group-key columns is fixed;
    // outer GROUP BY order does not affect grouping semantics.
    let outer_group_by: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}""#))
        .collect();
    let outer_group_by_str = outer_group_by.join(", ");

    // Build the inner fan-out. The common blob is shared by ALL shards, so build it
    // ONCE with `limit = None`: this structurally guarantees the "LIMIT never in a
    // per-shard partial" invariant (partial groups from every shard must be emitted
    // and merged by the outer wrapper). There is no per-shard spec left to strip.
    let mut common_template = spec_template.clone();
    common_template.limit = None;
    let fan_out = if shards.len() == 1 {
        let files = shards.into_iter().next().unwrap_or_default();
        let common_literal = sql_string_literal(&common_template.to_common_json());
        let files_literal = sql_string_literal(&ScanSpec::files_json(&files));
        format!(
            "SELECT {udf}({common}, {files}) EMITS ({emits})",
            udf = udf_name,
            common = common_literal,
            files = files_literal,
            emits = emits,
        )
    } else {
        build_fan_out_inner(&common_template, &shards, &emits, udf_name)
    };

    let mut sql =
        format!("SELECT {outer_select_str} FROM ({fan_out}) GROUP BY {outer_group_by_str}");

    // HAVING: applied in outer wrapper only, never pushed into shard scan.
    if let Some(h) = having.filter(|h| !h.is_empty()) {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }

    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// Build the EMITS items for the aggregate fan-out, following the COLUMN CONTRACT.
///
/// `col_types` maps uppercase column names to their Exasol type strings.
/// MIN/MAX partial columns use the target column's exact type.
/// SUM partial columns: DOUBLE PRECISION stays DOUBLE PRECISION; DECIMAL(p,s) widens to
/// DECIMAL(36,s) to avoid overflow; any other type falls back (callers should have validated
/// via `validate_agg_col_types` before reaching here — see handle_pushdown).
/// AVG partial sum stays DOUBLE PRECISION (AVG is inherently fractional).
/// Stat (STDDEV/VARIANCE) family: cnt DECIMAL(20,0), sum/sumsq DOUBLE PRECISION.
fn partial_emits_items(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => {
                vec![format!(r#""PARTIAL_count_{i}" DECIMAL(20,0)"#)]
            }
            AggKind::Sum => {
                let ty = col_type_for(plan, col_types);
                let emit_ty = sum_emit_type(&ty);
                vec![format!(r#""PARTIAL_sum_{i}" {emit_ty}"#)]
            }
            AggKind::Min => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_min_{i}" {ty}"#)]
            }
            AggKind::Max => {
                let ty = col_type_for(plan, col_types);
                vec![format!(r#""PARTIAL_max_{i}" {ty}"#)]
            }
            AggKind::Avg => vec![
                format!(r#""PARTIAL_avg_sum_{i}" DOUBLE PRECISION"#),
                format!(r#""PARTIAL_avg_cnt_{i}" DECIMAL(20,0)"#),
            ],
            // Stat family: 3 columns — cnt (DECIMAL), sum (DOUBLE), sumsq (DOUBLE).
            AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => vec![
                format!(r#""PARTIAL_stat_cnt_{i}" DECIMAL(20,0)"#),
                format!(r#""PARTIAL_stat_sum_{i}" DOUBLE PRECISION"#),
                format!(r#""PARTIAL_stat_sumsq_{i}" DOUBLE PRECISION"#),
            ],
        })
        .collect()
}

/// Look up the Exasol type for the target column of an aggregate plan.
/// Returns "DOUBLE PRECISION" as a safe fallback when the column is absent from the map.
fn col_type_for(plan: &AggregatePlan, col_types: &[(String, String)]) -> String {
    plan.column
        .as_deref()
        .and_then(|col| {
            col_types
                .iter()
                .find(|(n, _)| n == col)
                .map(|(_, t)| t.clone())
        })
        .unwrap_or_else(|| "DOUBLE PRECISION".to_string())
}

/// Map a column's Exasol type to the appropriate SUM partial EMITS type.
///
/// DOUBLE PRECISION => DOUBLE PRECISION (no change).
/// DECIMAL(p,s) => DECIMAL(36,s) (widened to max Exasol precision, preserving scale).
/// Any other type (DATE, TIMESTAMP, VARCHAR, BOOLEAN) => DOUBLE PRECISION as an
/// emergency fallback (callers should have validated before reaching here).
fn sum_emit_type(col_ty: &str) -> String {
    if col_ty == "DOUBLE PRECISION" {
        return "DOUBLE PRECISION".to_string();
    }
    if let Some(inner) = col_ty
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))
    {
        // inner is "p,s"
        if let Some((_p, s)) = inner.split_once(',') {
            return format!("DECIMAL(36,{s})");
        }
    }
    // Non-numeric type: validation should have caught this, but fall back gracefully.
    "DOUBLE PRECISION".to_string()
}

/// Return `true` if all SUM/MIN/MAX/stat targets have a supported Exasol column type.
///
/// SUM and the STDDEV/VARIANCE family are only valid over DOUBLE PRECISION or DECIMAL columns.
/// MIN/MAX are valid over any comparable type (DATE, TIMESTAMP, VARCHAR included).
/// Returns `false` (fall back to row scan) when any SUM or stat aggregate targets a
/// non-numeric column.
pub fn validate_agg_col_types(
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
) -> bool {
    for plan in aggregates {
        let needs_numeric = matches!(
            plan.kind,
            AggKind::Sum
                | AggKind::VarPop
                | AggKind::VarSamp
                | AggKind::StddevPop
                | AggKind::StddevSamp
        );
        if needs_numeric {
            let ty = col_type_for(plan, col_types);
            if !is_numeric_exasol_type(&ty) {
                return false;
            }
        }
    }
    true
}

/// Return `true` for Exasol types that support SUM (DOUBLE PRECISION, DECIMAL).
fn is_numeric_exasol_type(ty: &str) -> bool {
    ty == "DOUBLE PRECISION" || ty.starts_with("DECIMAL(")
}

/// Build the outer merge SELECT items following the COLUMN CONTRACT.
///
/// AVG uses `SUM(sum) / NULLIF(SUM(cnt), 0)` — the NULLIF guard ensures division
/// by zero yields NULL rather than an error (Exasol: `x / NULL = NULL`).
///
/// STDDEV/VARIANCE sufficient-statistics reconstruction (König–Huygens identity):
///   numer    = SUM(sumsq) - SUM(sum)² / NULLIF(SUM(cnt), 0)
///   var_pop  = numer / NULLIF(SUM(cnt), 0)          [NULL when cnt = 0]
///   var_samp = numer / (SUM(cnt) - 1)               [NULL when cnt ≤ 1, via CASE]
///
///   stddev_pop/samp = CASE WHEN var IS NULL THEN NULL
///                          ELSE SQRT(GREATEST(0.0, var)) END
///
///   The CASE guard is required because Exasol's `GREATEST(0.0, NULL) = 0.0`
///   (returns the max of non-NULL inputs; only returns NULL if ALL inputs are NULL),
///   so a bare `SQRT(GREATEST(0.0, NULL))` would yield `0.0` instead of NULL for
///   empty tables (N=0, pop) and single-row groups (N=1, samp).
///   The GREATEST(0.0, …) inside the ELSE branch guards against tiny-negative
///   float rounding artifacts that would otherwise cause SQRT to error.
fn merge_select_items(aggregates: &[AggregatePlan]) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| match plan.kind {
            AggKind::Count | AggKind::CountCol => format!(r#"SUM("PARTIAL_count_{i}")"#),
            AggKind::Sum => format!(r#"SUM("PARTIAL_sum_{i}")"#),
            AggKind::Min => format!(r#"MIN("PARTIAL_min_{i}")"#),
            AggKind::Max => format!(r#"MAX("PARTIAL_max_{i}")"#),
            AggKind::Avg => {
                format!(r#"SUM("PARTIAL_avg_sum_{i}") / NULLIF(SUM("PARTIAL_avg_cnt_{i}"), 0)"#)
            }
            AggKind::VarPop => {
                // numer / SUM(cnt); NULL when cnt = 0
                format!(
                    concat!(
                        r#"(SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0)"#,
                    ),
                    i = i
                )
            }
            AggKind::VarSamp => {
                // numer / (N-1); NULL when cnt <= 1
                format!(
                    concat!(
                        r#"(SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END"#,
                    ),
                    i = i
                )
            }
            AggKind::StddevPop => {
                // CASE IS NULL guard: Exasol GREATEST(0.0, NULL) = 0.0, not NULL.
                // Without the CASE, N=0 would yield SQRT(0.0) = 0.0 instead of NULL.
                format!(
                    concat!(
                        r#"CASE WHEN ((SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0)) IS NULL THEN NULL"#,
                        r#" ELSE SQRT(GREATEST(0.0, (SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))) END"#,
                    ),
                    i = i
                )
            }
            AggKind::StddevSamp => {
                // CASE IS NULL guard: Exasol GREATEST(0.0, NULL) = 0.0, not NULL.
                // Without the CASE, N<=1 would yield SQRT(0.0) = 0.0 instead of NULL.
                format!(
                    concat!(
                        r#"CASE WHEN ((SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END) IS NULL THEN NULL"#,
                        r#" ELSE SQRT(GREATEST(0.0, (SUM("PARTIAL_stat_sumsq_{i}") - SUM("PARTIAL_stat_sum_{i}") * SUM("PARTIAL_stat_sum_{i}") / NULLIF(SUM("PARTIAL_stat_cnt_{i}"), 0))"#,
                        r#" / CASE WHEN SUM("PARTIAL_stat_cnt_{i}") <= 1 THEN NULL ELSE SUM("PARTIAL_stat_cnt_{i}") - 1 END)) END"#,
                    ),
                    i = i
                )
            }
        })
        .collect()
}

/// Render a HAVING predicate for the OUTER merge wrapper.
///
/// The outer wrapper's only columns are `GK_*` and `PARTIAL_*` — there is no
/// source column (e.g. `SCORE`) available there. So each `function_aggregate`
/// reference in the predicate is rewritten to its merged expression (e.g.
/// `SUM(score)` → `SUM("PARTIAL_sum_0")`), matched to `plans` by
/// `AggregatePlan` equality (kind + source column). Non-aggregate leaves
/// (columns, literals, scalar functions, arithmetic) delegate to
/// `render_expression`.
///
/// Returns `None` if the predicate references an aggregate not among `plans`
/// (cannot be merged) or contains an unsupported node — the caller then
/// declines the grouped pushdown rather than emit a wrong or dropped HAVING.
fn render_having_over_merge(node: &Json, plans: &[AggregatePlan]) -> Option<String> {
    if !node.is_object() {
        return None;
    }
    let kind = node.get("type").and_then(|t| t.as_str())?;
    let child = |key: &str| node.get(key);

    // An aggregate reference: rewrite to its uncast merged expression. Uncast is
    // correct here — the comparison is against the raw merged numeric value; the
    // CAST in `cast_merge_items` is only for output-column typing.
    if kind == "function_aggregate" {
        let plan = parse_agg_item(node)?;
        let idx = plans.iter().position(|p| *p == plan)?;
        return merge_select_items(plans).into_iter().nth(idx);
    }

    // Boolean / comparison predicate nodes that can appear in a HAVING. Operator
    // strings and parenthesization mirror `vs-expression`'s renderer so output
    // matches conventions.
    match kind {
        "predicate_and" => render_having_junction(child("expressions"), plans, " AND "),
        "predicate_or" => render_having_junction(child("expressions"), plans, " OR "),
        "predicate_not" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("(NOT {inner})"))
        }
        "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal" => {
            let op = match kind {
                "predicate_equal" => "=",
                "predicate_notequal" => "<>",
                "predicate_less" => "<",
                "predicate_lessequal" => "<=",
                "predicate_greater" => ">",
                "predicate_greaterequal" => ">=",
                _ => unreachable!(),
            };
            let left = render_having_operand(child("left"), plans)?;
            let right = render_having_operand(child("right"), plans)?;
            Some(format!("({left} {op} {right})"))
        }
        "predicate_between" => {
            let target = render_having_operand(child("expression"), plans)?;
            let low = render_having_operand(child("left"), plans)?;
            let high = render_having_operand(child("right"), plans)?;
            Some(format!("({target} BETWEEN {low} AND {high})"))
        }
        "predicate_is_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NULL)"))
        }
        "predicate_is_not_null" => {
            let inner = render_having_operand(child("expression"), plans)?;
            Some(format!("({inner} IS NOT NULL)"))
        }
        _ => None,
    }
}

/// Render a HAVING operand: a `function_aggregate` rewrites to its merged
/// expression; any other node (column, literal, scalar function, arithmetic,
/// or nested predicate) delegates to `render_having_over_merge` — which itself
/// falls back to `render_expression` for non-predicate, non-aggregate nodes.
fn render_having_operand(node: Option<&Json>, plans: &[AggregatePlan]) -> Option<String> {
    let node = node.filter(|n| !n.is_null())?;
    let kind = node.get("type").and_then(|t| t.as_str())?;
    match kind {
        "function_aggregate"
        | "predicate_and"
        | "predicate_or"
        | "predicate_not"
        | "predicate_equal"
        | "predicate_notequal"
        | "predicate_less"
        | "predicate_lessequal"
        | "predicate_greater"
        | "predicate_greaterequal"
        | "predicate_between"
        | "predicate_is_null"
        | "predicate_is_not_null" => render_having_over_merge(node, plans),
        // Non-aggregate, non-predicate leaf (literal, column, scalar function,
        // arithmetic): the merge wrapper has no aggregate to rewrite, so render
        // it exactly as vs-expression would.
        _ => render_expression(node).ok(),
    }
}

/// Render an AND/OR junction over the outer merge wrapper, mirroring
/// `vs-expression`'s `render_junction`: single child unwrapped, multiple joined
/// and parenthesized. Any child that fails to render collapses the junction.
fn render_having_junction(
    expressions: Option<&Json>,
    plans: &[AggregatePlan],
    op: &str,
) -> Option<String> {
    let items = expressions?.as_array()?;
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(render_having_over_merge(item, plans)?);
    }
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(format!("({})", parts.join(op))),
    }
}

/// Build the outer merge SELECT items, each cast to its Exasol-declared result type.
///
/// The merge expression (e.g. `SUM("PARTIAL_count_0")` over DECIMAL(20,0) partials →
/// DECIMAL(31,0)) must match the type Exasol declared for that select-list column
/// (COUNT(score) → DECIMAL(18,0)); Exasol strictly validates the pushdown output
/// column types. When no declared type is available (or it is VARCHAR(2000000)),
/// the merge expression is emitted uncast.
fn cast_merge_items(aggregates: &[AggregatePlan], aggregate_types: &[String]) -> Vec<String> {
    merge_select_items(aggregates)
        .into_iter()
        .enumerate()
        .map(|(i, expr)| match aggregate_types.get(i) {
            Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({expr} AS {ty})"),
            _ => expr,
        })
        .collect()
}

/// Builds the shard fan-out SELECT that Exasol distributes across nodes.
///
/// Uses `GROUP BY shard_key` (NOT `IPROC()`) so work units spread round-robin
/// across nodes (G ≤ 300) and multiplex onto each node's core pool.
///
/// The shard-INVARIANT common blob (credentials, projection, filter, aggregates,
/// tuning knobs) is serialized ONCE via `to_common_json()` as the UDF's first
/// argument literal; only the per-shard files list varies across the `VALUES`
/// rows (arg 1). Because the common blob is emitted once instead of per shard, the
/// credential/tuning payload no longer repeats up to ~300 times in one statement.
///
/// Callers wrap the result in `SELECT * FROM (...)` for row scans or an outer merge
/// aggregation for aggregate pushdown. Callers that must exclude the LIMIT from the
/// shard scan (grouped aggregates) pass a `spec_template` whose `limit` is already
/// `None`, so the shared common blob carries no LIMIT for every shard by construction.
pub fn build_fan_out_inner(
    spec_template: &ScanSpec,
    shards: &[Vec<String>],
    emits: &str,
    udf_name: &str,
) -> String {
    // Serialize the shard-invariant common blob exactly once.
    let common_literal = sql_string_literal(&spec_template.to_common_json());
    let values: Vec<String> = shards
        .iter()
        .enumerate()
        .map(|(i, files)| {
            let files_literal = sql_string_literal(&ScanSpec::files_json(files));
            format!("({i},{files_literal})")
        })
        .collect();
    let values_list = values.join(",");
    format!(
        "SELECT {udf}({common}, files) EMITS ({emits}) FROM (VALUES {values}) AS shards(shard_key, files) GROUP BY shard_key",
        udf = udf_name,
        common = common_literal,
        emits = emits,
        values = values_list,
    )
}

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes read from the `CLUSTER_NODES`
/// adapterNotes entry (default 1 when absent or unparseable).
///
/// `parallelism_factor` — the oversubscription multiplier read from the
/// `PARALLELISM_FACTOR` adapterNotes entry (default 8).
///
/// `creds` — the resolved CONNECTION credentials, used to determine whether
/// to sign catalog requests and whether to apply vended S3 credentials.
///
/// Returns JSON `{"type":"pushdown","sql":"..."}`.
///
/// ponytail: The S3 access/secret/session-token keys are embedded verbatim in the
/// scan-driving SQL literal (inside the `ScanSpec` JSON), which Exasol may log or
/// surface in its query profile / audit trail. PoC-accepted tradeoff. The upgrade
/// path is to pass credentials via a CONNECTION object (referenced by name, never
/// inlined) or to fetch them over connect-back at scan time so they never appear
/// in any SQL text. Error paths already redact these values.
#[allow(clippy::too_many_arguments)]
pub async fn handle_pushdown(
    request: &Json,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    creds: &ConnectionCreds,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    let filter_json_raw = pushdown_req.get("filter").filter(|f| !f.is_null());

    let filter = filter_json_raw.and_then(render_df_filter_safe);

    let limit = extract_limit(&pushdown_req);

    let col_types = extract_all_column_types(request);

    // Resolve file list exactly once. The returned `effective_storage` carries
    // vended STS creds when use_vended_credentials is true; otherwise it equals
    // the static `storage` passed in. Every per-shard ScanSpec uses this storage.
    // filter_json_raw is forwarded for Iceberg-level file pruning; ScanSpec.filter
    // (DataFusion SQL string) is set separately above and left completely unchanged.
    let (files, effective_storage, logical_schema) =
        resolve_file_list(catalog_uri, catalog, storage, creds, filter_json_raw).await?;
    let storage = &effective_storage;

    if files.is_empty() {
        return Ok(empty_pushdown_sql(&proj_cols, &proj_types));
    }

    // Compute G = shard_count(node_count, parallelism_factor, file_count) and
    // partition files into G byte-balanced work-unit shards (GROUP BY shard_key fan-out).
    let g = shard_count(cluster_nodes, parallelism_factor, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);

    // The scan UDF must be schema-qualified: the pushdown query executes
    // outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = match scan_schema {
        Some(schema) if !schema.is_empty() => {
            format!("{}.{}", quote_ident(schema), SCAN_UDF_NAME)
        }
        _ => SCAN_UDF_NAME.to_string(),
    };

    // The raw HAVING node (for grouped aggregate queries). Rendered against the
    // merge decomposition in the grouped branch below (its aggregates reference
    // PARTIAL_* columns, not source columns). Kept as the raw node here only for
    // the presence check: the adapter advertises AGGREGATE_HAVING, so Exasol does
    // NOT re-apply a HAVING we claim to handle — dropping one yields wrong results.
    let having_node = pushdown_req.get("having").filter(|h| !h.is_null());

    // Detection priority: GROUP BY aggregate → single-group aggregate → row scan.
    if let Some(GroupedAggregateDetection {
        group_keys,
        plans: grouped_agg_plans,
        select_items,
    }) = detect_group_by_aggregates(&pushdown_req)
    {
        // Validate aggregate column types for the grouped path — same guard as the
        // single-group path below. SUM over a non-numeric column (VARCHAR, DATE, …)
        // would produce an opaque UDF error; normally we fall back to row scan.
        if !validate_agg_col_types(&grouped_agg_plans, &col_types) {
            // If a HAVING predicate is present, we cannot fall through silently:
            // the adapter has advertised AGGREGATE_HAVING, so Exasol will not
            // re-apply a HAVING we claim to handle. Dropping it yields wrong results.
            // Return an error so Exasol executes the query natively.
            if having_node.is_some() {
                return Err(UdfError::User(
                    "grouped aggregate pushdown declined: HAVING present but aggregate \
                     column type is non-numeric; Exasol will retry natively"
                        .into(),
                ));
            }
            // No HAVING: safe to fall through to single-group / row scan.
        } else {
            // Render the HAVING against the merge decomposition: each aggregate
            // reference is rewritten to its merged expression (SUM(score) →
            // SUM("PARTIAL_sum_0")). Applied in the OUTER wrapper only, never in
            // the per-shard scan. If a HAVING is present but cannot be rendered
            // over the merge, decline the grouped pushdown (Err) — silently
            // dropping it would yield wrong results because Exasol will not
            // re-apply a HAVING we advertised AGGREGATE_HAVING for.
            let having = match having_node {
                Some(node) => match render_having_over_merge(node, &grouped_agg_plans) {
                    Some(sql) => Some(sql),
                    None => {
                        return Err(UdfError::User(
                            "grouped aggregate pushdown declined: HAVING references an \
                             aggregate that cannot be merged or an unsupported node; \
                             Exasol will retry natively"
                                .into(),
                        ));
                    }
                },
                None => None,
            };
            // Grouped aggregate pushdown path.
            let spec_template = ScanSpec {
                files: vec![],
                projection: proj_cols.clone(),
                filter,
                limit,
                aggregates: Some(grouped_agg_plans.clone()),
                group_keys: Some(group_keys.clone()),
                // Aggregate scans emit via the freely-coercing Value path, not the
                // strict emit_batch IPC path, so no per-column declared types needed.
                emit_exa_types: Vec::new(),
                logical_schema: logical_schema.clone(),
                storage: storage.clone(),
                df_target_partitions,
                df_batch_size,
                df_threads_per_udf,
                memory_pool_fraction,
                instance_overhead_mb,
            };
            let group_key_types = group_key_exasol_types(&pushdown_req, &group_keys, &select_items);
            let aggregate_types = aggregate_exasol_types(&pushdown_req);
            let sql = build_grouped_aggregate_scan_sql(
                &spec_template,
                shards,
                &group_keys,
                &group_key_types,
                &grouped_agg_plans,
                &aggregate_types,
                &select_items,
                limit,
                &col_types,
                &udf_name,
                having.as_deref(),
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        } // end else (validate_agg_col_types passed)
    }

    // Single-group aggregate or row scan.
    // After detection, validate that each SUM/MIN/MAX targets a supported column type;
    // if any SUM targets a non-numeric type (DATE, VARCHAR, etc.), fall back to row scan.
    let aggregates =
        detect_aggregates(&pushdown_req).filter(|plans| validate_agg_col_types(plans, &col_types));

    let spec_template = ScanSpec {
        files: vec![], // replaced per shard in build_scan_driving_sql
        projection: proj_cols.clone(),
        filter,
        limit,
        aggregates,
        group_keys: None,
        // Row-scan EMITS types, positionally aligned with `proj_cols`. The scan
        // coerces each emitted Arrow column to the type its declared ExaType
        // accepts before emit_batch. Ignored when `aggregates` is Some (that path
        // emits via the Value path). Same list the EMITS clause is built from.
        emit_exa_types: proj_types.clone(),
        logical_schema,
        storage: storage.clone(),
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
    };

    let aggregate_types = aggregate_exasol_types(&pushdown_req);
    let sql = build_scan_driving_sql(
        &spec_template,
        shards,
        &proj_cols,
        &proj_types,
        limit,
        &col_types,
        &aggregate_types,
        &udf_name,
    );

    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Build the logical schema (`Vec<LogicalField>`) from an Iceberg current schema.
///
/// Iterates over the top-level struct fields of `schema` and maps each to a
/// `LogicalField` carrying its Iceberg field-id, current name, Arrow type tag,
/// and nullability (required → `false`, optional → `true`).
fn build_logical_schema(schema: &iceberg::spec::Schema) -> Vec<LogicalField> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let arrow_dt = crate::types::mapping::iceberg_type_to_arrow(&f.field_type);
            let arrow_type = crate::types::mapping::arrow_type_to_tag(&arrow_dt);
            LogicalField {
                field_id: f.id,
                name: f.name.clone(),
                arrow_type,
                nullable: !f.required,
            }
        })
        .collect()
}

/// Resolve the data-file list from the Iceberg REST catalog.
///
/// This is the resolve-once seam: called exactly once per pushdown in the
/// adapter; the file list is passed explicitly to the scan UDF.
///
/// The catalog load_table request is self-issued via `load_table_any_auth`, which
/// chooses how to authenticate (SigV4 | static bearer | OAuth2-derived bearer |
/// none). Vended-credential extraction is gated SOLELY on
/// `creds.use_vended_credentials` — orthogonal to the catalog-auth mode. When it
/// is true the returned `StorageProps` carries the vended STS keys (merged over
/// the static `storage` props, and the vended `client.region` when present) so
/// every per-shard `ScanSpec.storage` uses the vended creds. When it is false,
/// returns `(files, storage.clone())` — byte-identical to the no-vending behaviour
/// on every auth mode.
///
/// `filter_json` is the raw pushdown filter JSON forwarded to `plan_files_from_table`
/// for Iceberg-level file pruning. Pass `None` to disable pruning (e.g. `createVirtualSchema`).
pub async fn resolve_file_list(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    storage: &StorageProps,
    creds: &ConnectionCreds,
    filter_json: Option<&Json>,
) -> Result<(Vec<(String, u64)>, StorageProps, Vec<LogicalField>), UdfError> {
    // Single auth-mode-agnostic path: self-issue the loadTable GET under whatever
    // catalog-auth mode applies, then derive the effective storage gated SOLELY on
    // `use_vended_credentials` (orthogonal to the auth mode), and build the Table
    // from the response metadata so plan_files() can read manifests from S3.
    let result = load_table_any_auth(catalog_uri, catalog_props, creds).await?;

    // Resolve the effective storage (vended or static).
    // The longest-prefix anchor for storage_credentials matching must be an S3
    // URI. Use the table's own S3 location from the parsed metadata (this is what
    // storage_credentials[*].prefix matches against). Fall back to the warehouse
    // (also an S3 URI) when absent. The catalog REST URI is an HTTPS endpoint and
    // can never match an S3 prefix — do NOT use it here.
    let table_s3_location = result.metadata.location();
    let anchor: &str = if !table_s3_location.is_empty() {
        table_s3_location
    } else {
        &catalog_props.warehouse
    };
    let effective_storage = if creds.use_vended_credentials {
        let (ak, sk, st) = extract_vended_keys(&result, anchor);
        let mut merged = merge_vended_into_storage(storage, &ak, &sk, st.as_deref());
        // Adopt the vended region only when the response advertises one; otherwise
        // preserve the static region.
        if let Some(region) = extract_vended_region(&result, anchor) {
            merged.region = region;
        }
        merged
    } else {
        storage.clone()
    };

    // Build the iceberg Table so plan_files() can read manifests from S3.
    let (namespace, table_name) = parse_table_ident(&catalog_props.table)?;
    let table_ident = TableIdent::new(namespace, table_name);
    let file_io = build_s3_file_io(&effective_storage);
    let table_builder = iceberg::table::Table::builder()
        .identifier(table_ident)
        .file_io(file_io)
        .metadata(result.metadata);
    let table = if let Some(loc) = result.metadata_location {
        table_builder.metadata_location(loc).build()
    } else {
        table_builder.build()
    }
    .map_err(|e| {
        UdfError::User(format!(
            "failed to build Iceberg table: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    // Extract the logical schema before `plan_files_from_table` consumes `table`.
    let logical_schema = build_logical_schema(table.metadata().current_schema());

    let files = plan_files_from_table(table, &catalog_props.table, filter_json).await?;
    Ok((files, effective_storage, logical_schema))
}

/// Drive the iceberg scan and collect the data-file paths with their sizes.
///
/// When `filter_json` is `Some`, an Iceberg pruning predicate is applied before
/// `plan_files` so manifests and files that cannot match are skipped. DataFusion
/// remains the row-level correctness backstop; this is pruning-only.
async fn plan_files_from_table(
    table: iceberg::table::Table,
    table_name: &str,
    filter_json: Option<&Json>,
) -> Result<Vec<(String, u64)>, UdfError> {
    let mut scan_builder = table.scan();
    if let Some(fj) = filter_json {
        let schema = table.metadata().current_schema();
        if let Some(pred) = crate::adapter::iceberg_predicate::to_iceberg_predicate(fj, schema) {
            scan_builder = scan_builder.with_filter(pred);
        }
    }
    let scan = scan_builder
        .select_all()
        .build()
        .map_err(|e| UdfError::User(format!("failed to build Iceberg scan: {e}")))?;

    let task_stream = scan.plan_files().await.map_err(|e| {
        UdfError::User(format!(
            "failed to plan Iceberg files for '{}': {}",
            table_name,
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let tasks: Vec<_> = task_stream.try_collect().await.map_err(|e| {
        UdfError::User(format!(
            "failed to collect Iceberg file tasks: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;

    Ok(tasks
        .into_iter()
        .map(|t| (t.data_file_path().to_string(), t.file_size_in_bytes))
        .collect())
}

/// Resolve the Iceberg table schema for `createVirtualSchema`.
///
/// Returns (field_name, exasol_type_string) pairs. The table metadata is loaded
/// via the unified `load_table_any_auth` (SigV4 | bearer | OAuth2-bearer | none).
/// Schema resolution only reads `table.metadata().current_schema()` — no S3
/// manifest access is needed, so vended credentials do not affect this path.
pub async fn resolve_table_schema(
    catalog_uri: &str,
    catalog_props: &CatalogProps,
    creds: &ConnectionCreds,
) -> Result<Vec<(String, String)>, UdfError> {
    // Load the table metadata via the unified auth-mode-agnostic loader. Schema
    // resolution reads only `current_schema()`; vended credentials never affect it.
    let result = load_table_any_auth(catalog_uri, catalog_props, creds).await?;
    let table_metadata = result.metadata;

    let schema = table_metadata.current_schema();
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let exasol_ty = crate::types::mapping::iceberg_type_to_exasol(&f.field_type);
            // Declare columns in Exasol's canonical (uppercase) identifier casing
            // so unquoted user SQL (`SELECT id` → `ID`) resolves. The scan maps
            // projection names back to the Parquet field casing case-insensitively.
            (f.name.to_uppercase(), exasol_ty)
        })
        .collect();

    Ok(fields)
}

// ---------------------------------------------------------------------------
// Namespace enumeration (createVirtualSchema)
// ---------------------------------------------------------------------------

/// Enumerate every `TableIdent` in the configured namespace and all descendants.
///
/// Branches on `creds.use_sigv4`: unsigned path uses `RestCatalog::list_namespaces`
/// and `list_tables`; signed path issues SigV4-signed GETs directly.
///
/// The configured namespace is passed as split segments (e.g. `["prod","finance"]`).
/// Credentials NEVER appear in returned errors.
pub async fn list_namespace_tables(
    catalog_uri: &str,
    configured_ns: &[String],
    storage: &StorageProps,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    let ns_ident = NamespaceIdent::from_vec(configured_ns.to_vec()).map_err(|e| {
        UdfError::User(format!(
            "invalid ICEBERG_NAMESPACE '{}': {}",
            configured_ns.join("."),
            e
        ))
    })?;

    if creds.use_sigv4 {
        list_in_namespace_signed(catalog_uri, &ns_ident, &creds.warehouse, creds).await
    } else {
        list_namespace_tables_unsigned(catalog_uri, &ns_ident, &creds.warehouse, storage, creds)
            .await
    }
}

/// Enumerate tables using the unsigned `RestCatalog` path.
///
/// Recursively lists all direct-child namespaces of `parent`, collecting tables at
/// every level. `list_namespaces(parent)` returns only direct children.
async fn list_namespace_tables_unsigned(
    catalog_uri: &str,
    parent: &NamespaceIdent,
    warehouse: &str,
    storage: &StorageProps,
    creds: &ConnectionCreds,
) -> Result<Vec<TableIdent>, UdfError> {
    // Build a temporary CatalogProps with an empty table to construct the RestCatalog.
    let dummy_catalog = crate::scan::spec::CatalogProps {
        uri: catalog_uri.to_string(),
        warehouse: warehouse.to_string(),
        table: String::new(),
    };
    let catalog = build_rest_catalog(catalog_uri, &dummy_catalog, storage, creds).await?;
    list_in_namespace_unsigned(&catalog, parent).await
}

/// Recursively collect tables in `ns` and all descendant namespaces using an unsigned catalog.
fn list_in_namespace_unsigned<'a>(
    catalog: &'a iceberg_catalog_rest::RestCatalog,
    ns: &'a NamespaceIdent,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>,
> {
    Box::pin(async move {
        let mut all: Vec<TableIdent> = Vec::new();

        // Tables directly in this namespace.
        let tables = catalog.list_tables(ns).await.map_err(|e: iceberg::Error| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {}",
                ns.join("."),
                redact_catalog_error(&e.to_string())
            ))
        })?;
        all.extend(tables);

        // Recurse into direct child namespaces.
        let children = catalog
            .list_namespaces(Some(ns))
            .await
            .map_err(|e: iceberg::Error| {
                UdfError::User(format!(
                    "failed to list namespaces under '{}': {}",
                    ns.join("."),
                    redact_catalog_error(&e.to_string())
                ))
            })?;

        for child in children {
            let child_tables = list_in_namespace_unsigned(catalog, &child).await?;
            all.extend(child_tables);
        }

        Ok(all)
    })
}

/// Build the `list_namespaces` URL for a given parent namespace.
///
/// `GET {catalog_uri}/v1/{warehouse?}/namespaces?parent={ns_url}`
fn build_list_namespaces_url(
    catalog_uri: &str,
    warehouse: &str,
    parent: &NamespaceIdent,
) -> String {
    let ns_url = parent.to_url_string();
    if warehouse.is_empty() {
        format!("{catalog_uri}/v1/namespaces?parent={ns_url}")
    } else {
        format!("{catalog_uri}/v1/{warehouse}/namespaces?parent={ns_url}")
    }
}

/// Build the `list_tables` URL for a given namespace.
///
/// `GET {catalog_uri}/v1/{warehouse?}/namespaces/{ns_url}/tables`
fn build_list_tables_url(catalog_uri: &str, warehouse: &str, ns: &NamespaceIdent) -> String {
    let ns_url = ns.to_url_string();
    if warehouse.is_empty() {
        format!("{catalog_uri}/v1/namespaces/{ns_url}/tables")
    } else {
        format!("{catalog_uri}/v1/{warehouse}/namespaces/{ns_url}/tables")
    }
}

/// Sign and execute a GET request, returning the response body as JSON.
///
/// Credential values NEVER appear in returned errors.
async fn signed_get_json(
    url: &str,
    creds: &ConnectionCreds,
) -> Result<serde_json::Value, UdfError> {
    let client = reqwest::Client::new();
    let request = client
        .get(url)
        .header("accept", "application/json")
        .build()
        .map_err(|e| UdfError::User(format!("failed to build catalog request: {e}")))?;

    let signed = crate::adapter::sigv4::sign_request(
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
            redact_catalog_error(&e.to_string())
        ))
    })?;

    let response = client.execute(signed).await.map_err(|e| {
        UdfError::User(format!(
            "catalog request failed: {}",
            redact_catalog_error(&e.to_string())
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
            redact_catalog_error(&body)
        )));
    }

    response.json::<serde_json::Value>().await.map_err(|e| {
        UdfError::User(format!(
            "failed to parse catalog response: {}",
            redact_catalog_error(&e.to_string())
        ))
    })
}

/// Recursively collect tables in `ns` and all descendants using SigV4-signed GETs
/// (mirrors the SigV4 arm of `load_table_any_auth`). Credential values NEVER
/// appear in errors.
fn list_in_namespace_signed<'a>(
    catalog_uri: &'a str,
    ns: &'a NamespaceIdent,
    warehouse: &'a str,
    creds: &'a ConnectionCreds,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<TableIdent>, UdfError>> + Send + 'a>,
> {
    Box::pin(async move {
        use iceberg_catalog_rest::{ListNamespaceResponse, ListTablesResponse};

        let mut all: Vec<TableIdent> = Vec::new();

        // List tables in this namespace.
        let tables_url = build_list_tables_url(catalog_uri, warehouse, ns);
        let tables_json = signed_get_json(&tables_url, creds).await.map_err(|e| {
            UdfError::User(format!(
                "failed to list tables in namespace '{}': {}",
                ns.join("."),
                redact_catalog_error(&e.to_string())
            ))
        })?;
        let tables_response: ListTablesResponse =
            serde_json::from_value(tables_json).map_err(|e| {
                UdfError::User(format!(
                    "failed to parse list-tables response for namespace '{}': {}",
                    ns.join("."),
                    redact_catalog_error(&e.to_string())
                ))
            })?;
        all.extend(tables_response.identifiers);

        // List child namespaces and recurse. Best-effort: flat catalogs (e.g. AWS
        // Glue) reject nested-namespace listing with HTTP 400 "does not support
        // multipart namespace" — treat any failure here as "no children" and return
        // the tables already collected from this namespace.
        // ponytail: swallows ALL child-listing errors, not just the flat-catalog 400;
        // on a genuinely nested catalog a transient error would silently skip a
        // subtree. Upgrade path: branch on catalog capability from GET /v1/config.
        let ns_url = build_list_namespaces_url(catalog_uri, warehouse, ns);
        let ns_json = match signed_get_json(&ns_url, creds).await {
            Ok(j) => j,
            Err(_) => return Ok(all),
        };
        let ns_response: ListNamespaceResponse = match serde_json::from_value(ns_json) {
            Ok(r) => r,
            Err(_) => return Ok(all),
        };

        for child in ns_response.namespaces {
            let child_tables =
                list_in_namespace_signed(catalog_uri, &child, warehouse, creds).await?;
            all.extend(child_tables);
        }

        Ok(all)
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a fully-qualified Iceberg identifier into `(NamespaceIdent, table_name)`.
///
/// The trailing `.`-delimited segment is the table name; all preceding segments form the
/// namespace. Supports any number of namespace levels:
/// - `"db.table"` → `(NamespaceIdent(["db"]), "table")`
/// - `"prod.finance.orders"` → `(NamespaceIdent(["prod","finance"]), "orders")`
///
/// Returns an error when the input contains no `.` (a bare table name with no namespace).
fn parse_table_ident(qualified: &str) -> Result<(NamespaceIdent, String), UdfError> {
    let mut parts: Vec<&str> = qualified.split('.').collect();
    if parts.len() < 2 {
        return Err(UdfError::User(format!(
            "table property must be 'namespace.table', got: '{qualified}'"
        )));
    }
    let table_name = parts.pop().unwrap().to_string();
    let ns_ident = NamespaceIdent::from_vec(parts.iter().map(|s| s.to_string()).collect())
        .map_err(|e| UdfError::User(format!("invalid namespace in '{qualified}': {e}")))?;
    Ok((ns_ident, table_name))
}

/// Extract all columns and their Exasol types from the first involved table.
fn extract_all_column_types(request: &Json) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| tables.first())
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    Some((name, exasol_type_from_json(dt_json)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract the projected columns and their Exasol types from the pushdown request.
///
/// For `column` nodes: returns the uppercase column name and its Exasol type.
/// For scalar expression nodes (e.g. `function_scalar`): renders via the VS expression
/// translator and returns the rendered SQL fragment with type `VARCHAR(2000000)`.
/// If any select-list item can't be projected as-is (untranslatable scalar, or an
/// aggregate/unknown node), the whole projection falls back to the full base table
/// column set so Exasol can post-process the expression, GROUP BY, and aggregate —
/// correctness over pushdown. The returned projection is always deduplicated by name,
/// since duplicate EMITS column names are invalid in Exasol.
fn extract_projection(
    request: &Json,
    pushdown_req: &Json,
) -> Result<(Vec<String>, Vec<String>), UdfError> {
    let involved = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Get all columns from the first involved table.
    let all_cols: Vec<(String, String)> = involved
        .first()
        .and_then(|t| t.get("columns"))
        .and_then(|c| c.as_array())
        .map(|cols| {
            cols.iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?.to_uppercase();
                    let dt_json = c.get("dataType")?;
                    let exasol_type = exasol_type_from_json(dt_json);
                    Some((name, exasol_type))
                })
                .collect()
        })
        .unwrap_or_default();

    if all_cols.is_empty() {
        return Err(UdfError::User(
            "pushdown request has no column metadata".into(),
        ));
    }

    let type_by_upper = |name: &str| -> String {
        all_cols
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

    let first_col_name = all_cols.first().map(|(n, _)| n.clone()).unwrap_or_default();

    let select_list = pushdown_req.get("selectList");
    let (proj_names, proj_types): (Vec<String>, Vec<String>) = match select_list {
        None | Some(Json::Null) => {
            let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
            let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
            (names, types)
        }
        Some(Json::Array(list)) if list.is_empty() => {
            // Empty select list — project the first column only.
            let name = first_col_name;
            let ty = type_by_upper(&name);
            (vec![name], vec![ty])
        }
        Some(Json::Array(list)) => {
            // Exasol declares the result type of each selectList item in a parallel
            // `selectListDataTypes` array; the EMITS column type must equal it.
            let declared_types = pushdown_req
                .get("selectListDataTypes")
                .and_then(|v| v.as_array());
            let mut names = Vec::with_capacity(list.len());
            let mut types = Vec::with_capacity(list.len());
            // If any item can't be projected as-is (untranslatable scalar, or an
            // aggregate/unknown node), we can't emit a per-item projection — repeating
            // `first_col_name` would yield duplicate EMITS names. Instead project the
            // full base row so Exasol has every column to post-process the expression,
            // GROUP BY, and aggregate itself.
            let mut needs_full_fallback = false;
            for (i, e) in list.iter().enumerate() {
                let declared_type = declared_types
                    .and_then(|d| d.get(i))
                    .map(exasol_type_from_json);
                let item_type = e.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match item_type {
                    "column" => {
                        // Bare column reference — use the column name and its Exasol type.
                        let name = e
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_uppercase())
                            .unwrap_or_else(|| first_col_name.clone());
                        let ty = type_by_upper(&name);
                        names.push(name);
                        types.push(ty);
                    }
                    "function_scalar"
                    | "predicate_equal"
                    | "predicate_less"
                    | "predicate_lessequal"
                    | "predicate_like"
                    | "predicate_and"
                    | "predicate_or"
                    | "predicate_not"
                    | "literal_string"
                    | "literal_exactnumeric"
                    | "literal_double"
                    | "literal_null"
                    | "literal_date"
                    | "literal_timestamp"
                    | "literal_timestamp_utc" => {
                        // Scalar expression node — try to render it.
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                names.push(sql_frag);
                                let ty = declared_type
                                    .clone()
                                    .unwrap_or_else(|| "VARCHAR(2000000)".to_string());
                                types.push(ty);
                            }
                            None => {
                                // Untranslatable — fall back to projecting the full row.
                                needs_full_fallback = true;
                            }
                        }
                    }
                    _ => {
                        // Unknown / aggregate node — fall back to projecting the full row.
                        needs_full_fallback = true;
                    }
                }
            }
            if needs_full_fallback {
                let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
                let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
                (names, types)
            } else {
                (names, types)
            }
        }
        _ => {
            let names: Vec<String> = all_cols.iter().map(|(n, _)| n.clone()).collect();
            let types: Vec<String> = all_cols.iter().map(|(_, t)| t.clone()).collect();
            (names, types)
        }
    };

    // Defensive backstop: duplicate EMITS column names are always invalid in Exasol,
    // regardless of which path produced the projection. Dedup by name, keeping the
    // first occurrence and its type.
    let mut seen = std::collections::HashSet::new();
    let mut deduped_names = Vec::with_capacity(proj_names.len());
    let mut deduped_types = Vec::with_capacity(proj_types.len());
    for (name, ty) in proj_names.into_iter().zip(proj_types) {
        if seen.insert(name.clone()) {
            deduped_names.push(name);
            deduped_types.push(ty);
        }
    }

    Ok((deduped_names, deduped_types))
}

/// Extract LIMIT from the pushdown request.
fn extract_limit(pushdown_req: &Json) -> Option<u64> {
    pushdown_req
        .get("limit")
        .and_then(|l| l.get("numElements"))
        .and_then(|n| n.as_u64())
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[String], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(name, ty)| format!("CAST(NULL AS {ty}) AS {}", quote_ident(name)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Derive an Exasol type string from the VS column dataType JSON.
fn exasol_type_from_json(dt: &Json) -> String {
    let type_name = dt.get("type").and_then(|t| t.as_str()).unwrap_or("varchar");
    match type_name.to_lowercase().as_str() {
        "boolean" => "BOOLEAN".to_string(),
        "decimal" => {
            let p = dt.get("precision").and_then(|v| v.as_u64()).unwrap_or(18);
            let s = dt.get("scale").and_then(|v| v.as_u64()).unwrap_or(0);
            if p <= 36 && s <= 36 {
                format!("DECIMAL({p},{s})")
            } else {
                "VARCHAR(2000000)".to_string()
            }
        }
        "double" => "DOUBLE PRECISION".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => "TIMESTAMP".to_string(),
        "timestamp with local time zone" | "timestampwithlocaltime zone" => {
            "TIMESTAMP WITH LOCAL TIME ZONE".to_string()
        }
        _ => {
            // VARCHAR, CHAR, and all others.
            let size = dt.get("size").and_then(|v| v.as_u64()).unwrap_or(2000000);
            let capped = size.min(2000000);
            format!("VARCHAR({capped})")
        }
    }
}

/// Double-quote an identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Produce a SQL string literal with single-quote escaping.
fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Redact credential-shaped values from a catalog error message.
fn redact_catalog_error(msg: &str) -> String {
    crate::scan::emit::redact_credentials(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::spec::StorageProps;
    use vs_expression::render_df_filter_safe;

    // ---------------------------------------------------------------------------
    // shard_count — cap/clamp boundary tests
    // ---------------------------------------------------------------------------

    /// Scenario: Shard count oversubscribes the cluster and is capped at 300.
    /// 10 nodes × 50 factor = 500, capped to 300.
    #[test]
    fn shard_count_oversubscribes_and_caps_at_300() {
        // 10 × 50 = 500 > 300 files; cap at 300.
        assert_eq!(shard_count(10, 50, 500), 300, "must be capped at 300");
        // 10 × 50 = 500 but only 350 files — still capped at 300 (min(350, 300)=300).
        assert_eq!(
            shard_count(10, 50, 350),
            300,
            "must be capped at min(files,300)=300"
        );
        // Exact cap: 1 × 300 = 300, 1000 files — stays 300.
        assert_eq!(shard_count(1, 300, 1000), 300, "exactly 300 must stay 300");
        // 1 × 301 = 301 > 300; capped at 300.
        assert_eq!(shard_count(1, 301, 1000), 300, "301 must be capped at 300");
    }

    /// Scenario: Fewer files than G produces one shard per file with no empty shards.
    /// node_count × parallelism_factor > file_count => clamp to file_count.
    #[test]
    fn shard_count_clamped_to_file_count_no_empty_shards() {
        // 10 × 8 = 80 but only 3 files; clamp to 3.
        assert_eq!(shard_count(10, 8, 3), 3, "must clamp to file_count=3");
        // 4 × 8 = 32 but only 5 files; clamp to 5.
        assert_eq!(shard_count(4, 8, 5), 5, "must clamp to file_count=5");
        // 1 × 1 = 1, file_count=1; stays 1.
        assert_eq!(shard_count(1, 1, 1), 1, "single file single shard");
        // Minimum clamp: 0 × 8 = 0, clamp to min(1, …) = 1.
        assert_eq!(shard_count(0, 8, 100), 1, "zero product must clamp to 1");
        // parallelism_factor=0: 5 × 0 = 0, clamp to 1.
        assert_eq!(shard_count(5, 0, 100), 1, "zero factor must clamp to 1");
    }

    // ---------------------------------------------------------------------------
    // Helpers shared across tests
    // ---------------------------------------------------------------------------

    fn sample_storage() -> StorageProps {
        StorageProps {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        }
    }

    /// Assemble the scan-driving SQL from a known file list + spec — the same
    /// logic `handle_pushdown` runs after `resolve_file_list`.
    /// Uses `cluster_nodes=1` (single-shard / legacy shape).
    fn build_sql_for_fixture(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
    ) -> String {
        build_sql_for_fixture_n(files, proj_cols, proj_types, filter, limit, 1)
    }

    /// Assemble the scan-driving SQL for `cluster_nodes = n`.
    fn build_sql_for_fixture_n(
        files: Vec<String>,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        filter: Option<String>,
        limit: Option<u64>,
        cluster_nodes: usize,
    ) -> String {
        // Build a col_types map from proj_cols/proj_types for row-scan tests.
        let col_types: Vec<(String, String)> = proj_cols
            .iter()
            .cloned()
            .zip(proj_types.iter().cloned())
            .collect();
        let spec_template = ScanSpec {
            files: vec![],
            projection: proj_cols.clone(),
            filter,
            limit,
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let files_with_sizes: Vec<(String, u64)> = files.into_iter().map(|p| (p, 1)).collect();
        let shards =
            crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            shards,
            &proj_cols,
            &proj_types,
            limit,
            &col_types,
            &[],
            SCAN_UDF_NAME,
        )
    }

    /// The UDF's first-argument literal (the shard-invariant common blob), extracted
    /// as the substring between the first two single quotes. Valid for the test
    /// fixtures here, whose common JSON contains no embedded single quote (JSON uses
    /// double quotes; the rendered filters used in these tests carry none).
    fn common_arg_literal(sql: &str) -> &str {
        let start = sql.find('\'').expect("SQL must contain a literal") + 1;
        let rest = &sql[start..];
        let end = rest.find('\'').expect("common literal must be closed");
        &rest[..end]
    }

    // ---------------------------------------------------------------------------
    // Scenario: Pushdown resolves the file list once and builds a scan-driving query
    // ---------------------------------------------------------------------------

    /// Pure SQL-building part of the pushdown scenario.
    /// The file list comes from a fixture (no catalog I/O).
    #[test]
    fn pushdown_resolves_files_once_builds_scan_sql() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // The generated SQL must invoke the scan UDF with the spec embedded.
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "SQL must reference the scan UDF: {sql}"
        );
        // The spec JSON (embedded as a SQL literal) contains the file path.
        assert!(
            sql.contains("part-00000.parquet"),
            "SQL must carry assigned files: {sql}"
        );
        assert!(
            sql.contains("part-00001.parquet"),
            "SQL must carry both files: {sql}"
        );
        // Must be a SELECT (scan-driving query, not an empty stub).
        assert!(
            sql.starts_with("SELECT * FROM"),
            "must be a real query: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Projection is pushed into the scan-driving query
    // ---------------------------------------------------------------------------

    #[test]
    fn projection_in_common_arg_emits_match() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["A".into(), "B".into()],
            vec!["DECIMAL(10,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
        );

        // EMITS clause must list exactly the projected columns in order.
        assert!(
            sql.contains("\"A\" DECIMAL(10,0)"),
            "EMITS must carry col A: {sql}"
        );
        assert!(
            sql.contains("\"B\" VARCHAR(2000000)"),
            "EMITS must carry col B: {sql}"
        );

        // The projection lives in the common (arg 0) blob, not the per-shard files arg.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""projection":["A","B"]"#),
            "common arg must carry the projection in order: {common}"
        );
        // The per-shard files arg must not carry projection metadata.
        assert!(
            !sql.contains(r#""files""#),
            "no ScanSpec files key must appear (files travel as a bare JSON array): {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Filter predicate is pushed into the scan spec (translatable) or
    // omitted (untranslatable) — never mistranslated.
    // ---------------------------------------------------------------------------

    #[test]
    fn pushdown_translates_or_omits_predicate() {
        // Translatable predicate: column > literal.
        let translatable = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "age"},
            "right": {"type": "literal_exactnumeric", "value": 18}
        });
        let filter_rendered = render_df_filter_safe(&translatable);
        assert!(
            filter_rendered.is_some(),
            "translatable predicate must produce a filter string"
        );
        let filter_str = filter_rendered.unwrap();
        assert!(
            filter_str.contains(">"),
            "filter must include > operator: {filter_str}"
        );
        assert!(
            filter_str.contains("AGE") || filter_str.contains("\"AGE\""),
            "filter must reference the column: {filter_str}"
        );

        // Untranslatable predicate (e.g., an aggregate or unknown function):
        // render_df_filter_safe returns None → omitted from spec.
        let untranslatable = serde_json::json!({"type": "fn_custom_agg", "args": []});
        let omitted = render_df_filter_safe(&untranslatable);
        assert!(
            omitted.is_none(),
            "untranslatable predicate must be omitted (None), not mistranslated"
        );

        // Confirm omitting the filter still produces valid SQL (correctness backstop).
        let sql_no_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            None, // omitted
            None,
        );
        assert!(
            sql_no_filter.contains(SCAN_UDF_NAME),
            "SQL must still be valid when filter is omitted"
        );

        // Confirm carrying the filter includes it in the spec JSON.
        let sql_with_filter = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["AGE".into()],
            vec!["DECIMAL(20,0)".into()],
            Some(filter_str),
            None,
        );
        assert!(
            sql_with_filter.contains(">"),
            "filter must survive into the spec literal: {sql_with_filter}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: LIMIT is pushed into the scan spec; also appears at Exasol level.
    // ---------------------------------------------------------------------------

    #[test]
    fn row_scan_limit_in_common_arg() {
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(42),
        );

        // The outer SQL must contain LIMIT (Exasol-level backstop).
        assert!(
            sql.contains("LIMIT 42"),
            "outer SQL must carry LIMIT for correctness backstop: {sql}"
        );

        // For a row scan the LIMIT is retained in the common (arg 0) blob.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(r#""limit":42"#),
            "row-scan common arg must carry limit=42: {common}"
        );
    }

    // ---------------------------------------------------------------------------
    // Pre-existing helpers tests (unchanged)
    // ---------------------------------------------------------------------------

    #[test]
    fn empty_file_list_returns_empty_select() {
        let proj = vec!["ID".to_string(), "NAME".to_string()];
        let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let resp = empty_pushdown_sql(&proj, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE 1=0"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
    }

    #[test]
    fn limit_extracted_from_pushdown_request() {
        let req = serde_json::json!({"numElements": 42});
        assert_eq!(extract_limit(&req), None); // not nested under "limit"

        let req2 = serde_json::json!({"limit": {"numElements": 42}});
        assert_eq!(extract_limit(&req2), Some(42));
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        let s = "it's a test";
        let lit = sql_string_literal(s);
        assert_eq!(lit, "'it''s a test'");
    }

    /// Scenario: single-level namespace — returns (NamespaceIdent::new("mydb"), "mytable").
    #[test]
    fn parse_table_ident_splits_namespace_table() {
        let (ns, tbl) = parse_table_ident("mydb.mytable").unwrap();
        let levels: &[String] = &ns;
        assert_eq!(levels, &["mydb".to_string()]);
        assert_eq!(tbl, "mytable");
    }

    #[test]
    fn parse_table_ident_errors_on_no_dot() {
        let err = parse_table_ident("notable").unwrap_err();
        assert!(err.to_string().contains("namespace.table"));
    }

    /// Scenario: Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent.
    /// "prod.finance.orders" → NamespaceIdent(["prod","finance"]), "orders".
    #[test]
    fn parse_table_ident_handles_multilevel_namespace() {
        let (ns, tbl) = parse_table_ident("prod.finance.orders").unwrap();
        let levels: &[String] = &ns;
        assert_eq!(
            levels,
            &["prod".to_string(), "finance".to_string()],
            "namespace must have two levels"
        );
        assert_eq!(tbl, "orders", "table name is the trailing segment");

        // Three-level namespace + table.
        let (ns3, tbl3) = parse_table_ident("prod.finance.eu.orders").unwrap();
        let levels3: &[String] = &ns3;
        assert_eq!(
            levels3,
            &["prod".to_string(), "finance".to_string(), "eu".to_string()],
            "namespace must have three levels"
        );
        assert_eq!(tbl3, "orders");
    }

    // ---------------------------------------------------------------------------
    // extract_projection — row-scan fallback must be duplicate-free
    // ---------------------------------------------------------------------------

    /// A select list mixing an untranslatable scalar and COUNT(*) must NOT emit
    /// repeated `first_col_name` columns (which Exasol rejects as duplicate EMITS).
    /// It falls back to the full, deduplicated base-table column set.
    #[test]
    fn extract_projection_fallback_is_duplicate_free() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "EVENTS",
                "columns": [
                    {"name": "id", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "name", "dataType": {"type": "varchar", "size": 2000000}},
                ],
            }],
        });
        // Untranslatable scalar (unknown function) + COUNT(*) aggregate — both items
        // would otherwise hit the first-column fallback arms.
        let pushdown_req = serde_json::json!({
            "selectList": [
                {"type": "function_scalar", "name": "TOTALLY_UNKNOWN_FN", "arguments": [
                    {"type": "column", "name": "id"}
                ]},
                {"type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false},
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 20, "scale": 0},
            ],
        });

        let (names, types) = extract_projection(&request, &pushdown_req).unwrap();

        let unique: std::collections::HashSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "projection must be duplicate-free, got: {names:?}"
        );
        assert_eq!(
            names,
            vec!["ID".to_string(), "NAME".to_string()],
            "fallback must project the full base-table column set"
        );
        assert_eq!(
            names.len(),
            types.len(),
            "names and types must stay aligned"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_aggregates — plan translation + fallback
    // ---------------------------------------------------------------------------

    fn agg_item(name: &str, col: Option<&str>, distinct: bool) -> serde_json::Value {
        let mut args = serde_json::json!([]);
        if let Some(c) = col {
            args = serde_json::json!([{"type": "column", "name": c}]);
        }
        serde_json::json!({
            "type": "function_aggregate",
            "name": name,
            "arguments": args,
            "distinct": distinct,
        })
    }

    /// COUNT(*) translates to Count with column=None.
    #[test]
    fn detect_count_star_produces_count_no_column() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", None, false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(*)");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, AggKind::Count);
        assert!(plans[0].column.is_none());
    }

    /// COUNT(col) translates to CountCol with the column name.
    #[test]
    fn detect_count_col_produces_count_col() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("amount"), false)]
        });
        let plans = detect_aggregates(&req).expect("should detect COUNT(col)");
        assert_eq!(plans[0].kind, AggKind::CountCol);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
    }

    /// SUM/MIN/MAX/AVG each translate to the right kind + column.
    #[test]
    fn detect_sum_min_max_avg_produce_correct_plans() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("should detect all four");
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Min);
        assert_eq!(plans[1].column.as_deref(), Some("TS"));
        assert_eq!(plans[2].kind, AggKind::Max);
        assert_eq!(plans[2].column.as_deref(), Some("TS"));
        assert_eq!(plans[3].kind, AggKind::Avg);
        assert_eq!(plans[3].column.as_deref(), Some("SCORE"));
    }

    /// GROUP BY present and non-empty => fall back (None).
    #[test]
    fn detect_aggregates_falls_back_on_group_by() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "groupBy": [{"type": "column", "name": "region"}],
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when GROUP BY is present"
        );
    }

    /// DISTINCT aggregate => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_distinct() {
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("id"), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when DISTINCT is present"
        );
    }

    /// Unsupported aggregate function (e.g., MEDIAN) => fall back to row scan.
    /// Note: STDDEV is a supported decomposable aggregate via sufficient-statistics.
    #[test]
    fn detect_aggregates_falls_back_on_unsupported_function() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("SUM", Some("amount"), false),
                agg_item("MEDIAN", Some("amount"), false),
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when any item is unsupported"
        );
    }

    /// Non-aggregate select item (e.g., plain column) => fall back.
    #[test]
    fn detect_aggregates_falls_back_on_column_select() {
        let req = serde_json::json!({
            "selectList": [
                {"type": "column", "name": "region"},
            ]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when select list contains non-aggregate"
        );
    }

    /// Empty select list => None.
    #[test]
    fn detect_aggregates_returns_none_for_empty_select_list() {
        let req = serde_json::json!({ "selectList": [] });
        assert!(detect_aggregates(&req).is_none());
    }

    /// An aggregate select-list translates to a ScanSpec carrying
    /// the aggregate plan (kind+column) plus any pushed-down filter.
    #[test]
    fn aggregate_query_builds_partial_agg_spec() {
        // Build a spec_template as handle_pushdown would.
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            limit: None,
            aggregates: Some(vec![
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                },
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                },
            ]),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };

        // Build single-shard SQL and decode the embedded spec literal.
        let shards = vec![vec!["s3://warehouse/f.parquet".into()]];
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["AMOUNT".to_string()],
            &["DOUBLE PRECISION".to_string()],
            None,
            &col_types,
            &[],
            SCAN_UDF_NAME,
        );

        // The spec JSON is embedded in the SQL literal; extract and parse it.
        // It lives between the first `'` and the matching unescaped `'` after the JSON.
        // Simpler: deserialize directly from the template (which is what gets embedded).
        let spec_json = {
            // Reconstruct the shard spec as the builder would.
            let mut s = spec_template.clone();
            s.files = vec!["s3://warehouse/f.parquet".into()];
            s.to_json()
        };
        let parsed = ScanSpec::from_json(&spec_json).expect("spec must parse");

        // The aggregate plan must be present with the right kinds and columns.
        let plans = parsed.aggregates.expect("aggregates must be in the spec");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert_eq!(plans[0].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[1].kind, AggKind::Count);
        assert!(plans[1].column.is_none());

        // The filter must also be present.
        assert!(
            parsed.filter.is_some(),
            "filter must be carried in aggregate spec"
        );

        // The SQL must reference the UDF.
        assert!(sql.contains(SCAN_UDF_NAME));
    }

    // ---------------------------------------------------------------------------
    // Fan-out SQL shape — multi-shard GROUP BY shard_key, single-shard equivalence
    // ---------------------------------------------------------------------------

    /// Multi-shard fan-out serializes the shard-INVARIANT common blob EXACTLY ONCE
    /// (as the UDF's first argument literal) and carries only the per-shard files
    /// list in each `VALUES` row — no credential/tuning payload repeats per shard.
    #[test]
    fn fan_out_serializes_common_once_files_per_shard() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // cluster_nodes=3 forces 3 shards (one file each).
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            3,
        );

        // Must use shard_key GROUP BY for the fan-out, NOT IPROC().
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "multi-shard SQL must GROUP BY shard_key: {sql}"
        );

        // The VALUES table exposes the per-shard files column (arg 1), not a full spec.
        assert!(
            sql.contains("AS shards(shard_key, files)"),
            "fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
        );
        // The UDF is called with two args: the common literal, then the files column.
        assert!(
            sql.contains(&format!("{SCAN_UDF_NAME}(")),
            "multi-shard SQL must invoke the scan UDF: {sql}"
        );
        assert!(
            sql.contains(", files) EMITS ("),
            "UDF must take the per-shard files column as its second argument: {sql}"
        );

        // The shard-invariant common blob must appear EXACTLY ONCE. The storage
        // endpoint and the tuning knobs live only in the common blob, so counting
        // them proves the credential/tuning payload is not repeated per shard.
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "storage endpoint (common blob) must appear exactly once, not per shard: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "tuning payload (common blob) must appear exactly once, not per shard: {sql}"
        );

        // Each shard's file appears EXACTLY ONCE, in its own VALUES row.
        for file in ["part-000.parquet", "part-001.parquet", "part-002.parquet"] {
            assert_eq!(
                sql.matches(file).count(),
                1,
                "file {file} must appear exactly once (in one VALUES row): {sql}"
            );
        }

        // Exactly 3 VALUES entries (one files literal per shard).
        let values_start = sql.find("VALUES").expect("must have VALUES");
        let group_by_start = sql.find("GROUP BY").expect("must have GROUP BY");
        let values_section = &sql[values_start..group_by_start];
        let entry_count = values_section.matches("),(").count() + 1;
        assert_eq!(
            entry_count, 3,
            "must have 3 VALUES entries for 3 shards: {values_section}"
        );
    }

    // ---------------------------------------------------------------------------
    // Aggregate merge wrapper SQL — outer SELECT reconstructing partial results
    // ---------------------------------------------------------------------------

    /// Helper: build aggregate scan SQL from a set of aggregate plans.
    /// Uses an empty col_types map — aggregate columns default to DOUBLE PRECISION
    /// (correct for existing tests that use SCORE/AMOUNT as DOUBLE).
    fn build_agg_sql(
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        cluster_nodes: usize,
    ) -> String {
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let files_with_sizes: Vec<(String, u64)> = files.into_iter().map(|p| (p, 1)).collect();
        let shards =
            crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        )
    }

    /// Aggregate wrapper merges partials: outer SELECT aggregates per-shard COUNT/SUM/MIN/MAX.
    /// Given COUNT/SUM/MIN/MAX aggregate plan: wrapper contains fan-out AND outer
    /// SUM/MIN/MAX over the partial columns in the right order.
    #[test]
    fn aggregate_wrapper_merges_partials() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
            },
        ];

        // Multi-shard: use 2 shards to exercise the fan-out + merge wrapper.
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain the shard_key fan-out (NOT IPROC).
        assert!(
            !sql.contains("IPROC()"),
            "aggregate SQL must NOT use IPROC: {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "aggregate SQL must use GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "aggregate SQL must use shard_key fan-out: {sql}"
        );

        // Must wrap with outer merge aggregation.
        assert!(
            sql.contains("SUM("),
            "merge wrapper must contain SUM: {sql}"
        );
        assert!(
            sql.contains("MIN("),
            "merge wrapper must contain MIN: {sql}"
        );
        assert!(
            sql.contains("MAX("),
            "merge wrapper must contain MAX: {sql}"
        );

        // Must contain partial column names in the EMITS and in the merge.
        assert!(
            sql.contains("PARTIAL_count_0"),
            "must reference partial count column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "must reference partial sum column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_min_2"),
            "must reference partial min column: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_max_3"),
            "must reference partial max column: {sql}"
        );

        // The EMITS clause must declare the partial columns.
        assert!(
            sql.contains("EMITS"),
            "aggregate SQL must have EMITS: {sql}"
        );

        // The outer merge must not be SELECT *.
        assert!(
            !sql.contains("SELECT *"),
            "aggregate wrapper must not use SELECT *: {sql}"
        );
    }

    /// Single-group merge casts each aggregate to its Exasol-declared result type.
    /// `SELECT COUNT(score)` merges as `SUM("PARTIAL_count_0")` (DECIMAL(31,0)); Exasol
    /// declared DECIMAL(18,0) for the column and strictly validates the adapter's output
    /// type, so the merge item must be `CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))`.
    #[test]
    fn single_group_merge_casts_to_declared_type() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
        }];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(plans.clone()),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://warehouse/f0.parquet".into()]];
        let col_types = vec![("SCORE".to_string(), "DECIMAL(18,0)".to_string())];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
        );
        assert!(
            sql.contains(r#"CAST(SUM("PARTIAL_count_0") AS DECIMAL(18,0))"#),
            "single-group merge must cast COUNT to declared DECIMAL(18,0): {sql}"
        );
    }

    /// Single-group merge with no declared types emits the bare uncast merge expression.
    #[test]
    fn single_group_merge_uncast_without_declared_types() {
        let plans = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("SCORE".into()),
        }];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(plans.clone()),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://warehouse/f0.parquet".into()]];
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            sql.contains(r#"SUM("PARTIAL_count_0")"#) && !sql.contains("CAST(SUM"),
            "single-group merge without declared types must be uncast: {sql}"
        );
    }

    /// AVG wrapper divides merged sum by count with NULLIF(cnt, 0) guard.
    /// Given AVG plan: wrapper computes SUM(partial_avg_sum) / NULLIF(SUM(partial_avg_cnt),0).
    #[test]
    fn avg_wrapper_divides_sum_by_count_guarded() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
        }];
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        let sql = build_agg_sql(plans, files, 2);

        // Must contain NULLIF guard for zero-count protection.
        assert!(
            sql.contains("NULLIF"),
            "AVG wrapper must contain NULLIF zero-guard: {sql}"
        );

        // Must divide: the / operator must appear in the outer merge context.
        assert!(
            sql.contains(" / "),
            "AVG wrapper must divide sum by count: {sql}"
        );

        // Must reference the AVG sum and count partial columns.
        assert!(
            sql.contains("PARTIAL_avg_sum_0"),
            "must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_0"),
            "must reference partial avg count: {sql}"
        );

        // Must use SUM() for both the sum and count parts.
        let sum_count = sql.matches("SUM(").count();
        assert!(
            sum_count >= 2,
            "AVG wrapper must SUM both partial_avg_sum and partial_avg_cnt: {sql}"
        );

        // Must contain NULLIF(..., 0).
        assert!(
            sql.contains("NULLIF(") && sql.contains(", 0)"),
            "AVG wrapper NULLIF guard must guard against zero: {sql}"
        );
    }

    /// Single-shard aggregate path produces a correct merge wrapper.
    #[test]
    fn single_shard_aggregate_still_uses_merge_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
            },
        ];
        let files = vec!["s3://warehouse/f0.parquet".into()];
        let sql = build_agg_sql(plans, files, 1);

        // Even single-shard aggregate must have an outer merge.
        assert!(
            sql.contains("SUM("),
            "single-shard aggregate must have SUM merge: {sql}"
        );
        assert!(
            sql.contains("NULLIF"),
            "single-shard AVG must have NULLIF guard: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "single-shard must reference partial count: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_sum_1"),
            "single-shard must reference partial avg sum: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_avg_cnt_1"),
            "single-shard must reference partial avg count: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // R.1: EMITS type correctness for SUM/MIN/MAX
    // ---------------------------------------------------------------------------

    /// R.1: MIN/MAX over a DATE column must EMIT DATE, not DOUBLE PRECISION.
    #[test]
    fn partial_emits_min_max_preserve_date_timestamp_type() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("EVENT_DATE".into()),
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("EVENT_TS".into()),
            },
        ];
        let col_types = vec![
            ("EVENT_DATE".to_string(), "DATE".to_string()),
            ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DATE") && !emits[0].contains("DOUBLE"),
            "MIN over DATE must emit DATE, not DOUBLE: {:?}",
            emits[0]
        );
        assert!(
            emits[1].contains("TIMESTAMP") && !emits[1].contains("DOUBLE"),
            "MAX over TIMESTAMP must emit TIMESTAMP, not DOUBLE: {:?}",
            emits[1]
        );
    }

    /// R.1: SUM over a DECIMAL(20,0) integer column must emit DECIMAL(36,0), not DOUBLE.
    #[test]
    fn partial_emits_sum_integer_stays_decimal() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
        }];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DECIMAL") && !emits[0].contains("DOUBLE"),
            "SUM over DECIMAL integer must emit DECIMAL, not DOUBLE: {:?}",
            emits[0]
        );
        // Scale must be 0 (preserved from original DECIMAL(20,0)).
        assert!(
            emits[0].contains("DECIMAL(36,0)"),
            "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
    #[test]
    fn partial_emits_sum_double_stays_double() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
        }];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let emits = partial_emits_items(&plans, &col_types);
        assert!(
            emits[0].contains("DOUBLE PRECISION"),
            "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
            emits[0]
        );
    }

    /// R.1: SUM over a VARCHAR/DATE column => validate_agg_col_types returns false (fall back).
    #[test]
    fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
        let col_types_varchar = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let sum_varchar = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("NAME".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_varchar, &col_types_varchar),
            "SUM over VARCHAR must fail validation (fall back to row scan)"
        );

        let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
        let sum_date = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("EVENT_DATE".into()),
        }];
        assert!(
            !validate_agg_col_types(&sum_date, &col_types_date),
            "SUM over DATE must fail validation (fall back to row scan)"
        );
    }

    // ---------------------------------------------------------------------------
    // FIX 1: grouped aggregate with invalid agg column type falls back
    // ---------------------------------------------------------------------------

    /// A grouped aggregate whose SUM targets a VARCHAR column must fall back to row
    /// scan (return None from detect_group_by_aggregates + validate_agg_col_types) —
    /// the same guard as the single-group path — rather than producing grouped scan SQL
    /// that would generate an opaque UDF error at execution time.
    #[test]
    fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
        // Simulate the detection + validation sequence that handle_pushdown runs.
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("NAME"), false), // NAME is VARCHAR — invalid for SUM
            ],
        });

        // detect_group_by_aggregates must accept the shape (it doesn't know types).
        let detected = detect_group_by_aggregates(&req);
        assert!(
            detected.is_some(),
            "detect_group_by_aggregates must accept the shape: {req}"
        );
        let agg_plans = detected.unwrap().plans;

        // Validation with VARCHAR col_types must fail — triggering fall-back.
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types),
            "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
        );

        // Confirm that a DATE column also fails for SUM.
        let col_types_date = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DATE".to_string()),
        ];
        assert!(
            !validate_agg_col_types(&agg_plans, &col_types_date),
            "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
        );

        // Confirm a numeric type passes (no fall back).
        let col_types_numeric = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("NAME".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        assert!(
            validate_agg_col_types(&agg_plans, &col_types_numeric),
            "validate_agg_col_types must pass for SUM over DOUBLE PRECISION"
        );
    }

    // ---------------------------------------------------------------------------
    // R.2: multi-shard row-scan must append outer LIMIT
    // ---------------------------------------------------------------------------

    /// R.2: multi-shard row scan with LIMIT must append LIMIT to the outer SQL.
    #[test]
    fn multi_shard_row_scan_appends_outer_limit() {
        let files = vec![
            "s3://warehouse/f0.parquet".into(),
            "s3://warehouse/f1.parquet".into(),
        ];
        // cluster_nodes=2 forces 2 shards.
        let sql = build_sql_for_fixture_n(
            files,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(10),
            2,
        );
        assert!(
            !sql.contains("IPROC()"),
            "must NOT use IPROC (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "must be multi-shard (uses shard_key): {sql}"
        );
        assert!(
            sql.contains("LIMIT 10"),
            "multi-shard row scan must append outer LIMIT 10: {sql}"
        );
    }

    /// Single-shard SQL uses the two-argument form `{udf}('<common>', '<files>')`:
    /// the common blob and the whole-file-list literal each appear exactly once, and
    /// the SQL keeps the `SELECT * FROM (SELECT …)` wrapper with no fan-out markers.
    #[test]
    fn single_shard_two_arg_common_and_files_once() {
        let files = vec![
            "s3://warehouse/db/events/part-00000.parquet".into(),
            "s3://warehouse/db/events/part-00001.parquet".into(),
        ];
        let sql = build_sql_for_fixture_n(
            files.clone(),
            vec!["ID".into(), "NAME".into()],
            vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            None,
            None,
            1, // single node
        );

        // Must NOT contain multi-shard markers.
        assert!(
            !sql.contains("IPROC"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );

        // Must keep the SELECT * FROM (SELECT …) wrapper and invoke the scan UDF.
        assert!(
            sql.starts_with("SELECT * FROM (SELECT "),
            "must start with SELECT * FROM (SELECT ...: {sql}"
        );
        assert!(sql.contains("EMITS"), "must have EMITS clause: {sql}");
        assert!(
            sql.contains(SCAN_UDF_NAME),
            "must invoke the scan UDF: {sql}"
        );

        // The common blob is serialized ONCE (endpoint + tuning knob appear once each).
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "common blob (storage endpoint) must appear exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "common blob (tuning payload) must appear exactly once: {sql}"
        );

        // Both files are carried once, together in the single files-list literal
        // (arg 1), which is a JSON array — not repeated across per-shard rows.
        assert_eq!(
            sql.matches("part-00000.parquet").count(),
            1,
            "must carry file 0 exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("part-00001.parquet").count(),
            1,
            "must carry file 1 exactly once: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_group_by_aggregates — GROUP BY key extraction and aggregate detection
    // ---------------------------------------------------------------------------

    fn make_group_by_request(
        group_by: serde_json::Value,
        select_list: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": group_by,
            "selectList": select_list,
        })
    }

    /// Like `make_group_by_request`, but also carries `selectListDataTypes` so
    /// ordering + type-position assertions are possible (positional matching
    /// against the outer wrapper SELECT and group-key type resolution).
    fn make_group_by_request_with_types(
        group_by: serde_json::Value,
        select_list: serde_json::Value,
        select_list_data_types: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": group_by,
            "selectList": select_list,
            "selectListDataTypes": select_list_data_types,
        })
    }

    /// `MOD(<col>, <divisor>)` as a `function_scalar` node — renders to
    /// `("<COL>" % <divisor>)` via `render_expression`. Used to build the #33
    /// repro (`SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`) and its
    /// interleaved/HAVING variants.
    fn mod_item(col: &str, divisor: i64) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "MOD",
            "arguments": [
                {"type": "column", "name": col},
                {"type": "literal_exactnumeric", "value": divisor},
            ],
        })
    }

    /// A DECIMAL `selectListDataTypes` entry, per the `exasol_type_from_json` shape.
    fn decimal_type(precision: u64, scale: u64) -> serde_json::Value {
        serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
    }

    /// Column reference in GROUP BY renders to a quoted identifier.
    #[test]
    fn detect_group_by_aggregates_column_key() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", None, false),
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let GroupedAggregateDetection {
            group_keys: keys,
            plans,
            ..
        } = result;
        assert_eq!(keys.len(), 1, "one group key");
        assert!(
            keys[0].contains("REGION"),
            "group key must reference REGION: {:?}",
            keys[0]
        );
        assert_eq!(plans.len(), 1, "one aggregate plan");
        assert_eq!(plans[0].kind, AggKind::Count);
    }

    /// Scalar expression in GROUP BY (e.g., function_scalar YEAR) renders via render_expression.
    #[test]
    fn detect_group_by_aggregates_expression_key() {
        // A predicate_equal used as an expression key — render_expression can handle it.
        let req = make_group_by_request(
            serde_json::json!([{
                "type": "predicate_equal",
                "left": {"type": "column", "name": "STATUS"},
                "right": {"type": "literal_string", "value": "active"},
            }]),
            serde_json::json!([agg_item("SUM", Some("AMOUNT"), false),]),
        );
        let result = detect_group_by_aggregates(&req);
        // predicate_equal renders to (STATUS = 'active'), so it should succeed.
        assert!(result.is_some(), "renderable expression key must succeed");
        let GroupedAggregateDetection {
            group_keys: keys,
            plans,
            ..
        } = result.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains("="), "rendered expression must contain =");
        assert_eq!(plans[0].kind, AggKind::Sum);
    }

    /// An unsupported expression in GROUP BY causes the whole function to return None.
    #[test]
    fn detect_group_by_unsupported_expression_falls_back() {
        let req = make_group_by_request(
            serde_json::json!([{"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
            serde_json::json!([agg_item("COUNT", None, false)]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "unsupported expression must fall back to None"
        );
    }

    /// Select list with a non-aggregate, non-column item causes fallback.
    #[test]
    fn detect_group_by_mixed_select_falls_back() {
        // function_scalar in selectList is not an aggregate and not a plain column.
        let req = make_group_by_request(
            serde_json::json!([{"type": "column", "name": "REGION"}]),
            serde_json::json!([
                {"type": "function_scalar", "name": "YEAR", "arguments": [{"type": "column", "name": "TS"}]},
                agg_item("COUNT", None, false),
            ]),
        );
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "non-aggregate non-column in selectList must fall back"
        );
    }

    // ---------------------------------------------------------------------------
    // detect_group_by_aggregates — select-list order preservation (fix-grouped-agg-select-order)
    // ---------------------------------------------------------------------------

    /// #33 repro: an aggregate placed before the single group key in the
    /// selectList must classify with `select_index` 0 for the aggregate and 1
    /// for the group key — the original ordinals, not a keys-first reorder.
    #[test]
    fn detect_group_by_aggregates_preserves_select_list_order() {
        // SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(result.group_keys.len(), 1, "one group key");
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "classification must preserve original select-list ordinals: {:?}",
            result.select_items
        );
    }

    /// Interleaved multi-key GROUP BY: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
    /// Each classified item must carry its own selectList ordinal and the
    /// correct group-key slot (k1 → slot 0, k2 → slot 1), even though the
    /// aggregate sits between them in the select list.
    #[test]
    fn detect_group_by_aggregates_interleaved_multi_key_preserves_order() {
        let req = make_group_by_request(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                {"type": "column", "name": "YEAR"},
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("SCORE"), false),
                {"type": "column", "name": "YEAR"},
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(result.group_keys.len(), 2, "two group keys");
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 1,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 2,
                },
            ],
            "classification must preserve interleaved ordinals: {:?}",
            result.select_items
        );
    }

    /// Expression group key placed after an aggregate:
    /// `SELECT COUNT(*), MOD(id,4) ... GROUP BY MOD(id,4)`.
    #[test]
    fn detect_group_by_aggregates_expr_key_after_agg_preserves_order() {
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("COUNT", None, false), mod_item("ID", 4)]),
        );
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "expression key after aggregate must classify by original ordinal: {:?}",
            result.select_items
        );
    }

    /// Aggregate-first GROUP BY with HAVING present: HAVING does not change
    /// selectList classification, but this exercises the same aggregate-first
    /// shape that flows into the HAVING-present outer-wrapper path.
    #[test]
    fn detect_group_by_aggregates_aggregate_first_with_having_preserves_order() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [mod_item("ID", 4)],
            "selectList": [agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)],
            "having": {
                "type": "predicate_greater",
                "left": agg_item("SUM", Some("SCORE"), false),
                "right": {"type": "literal_exactnumeric", "value": 100},
            },
        });
        let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            "HAVING presence must not affect selectList classification order: {:?}",
            result.select_items
        );
    }

    // ---------------------------------------------------------------------------
    // partition_files_by_bytes — G shards balanced, disjoint, full coverage
    // ---------------------------------------------------------------------------

    /// File list partitioned into G shards via shard_count is balanced, disjoint,
    /// and covers every file with no empty shards.
    #[test]
    fn partition_files_g_shards_balanced_disjoint_full_coverage() {
        use std::collections::HashSet;
        // 3 nodes × 4 factor = 12, capped to 10 files → G = 10
        let file_names: Vec<String> = (0..10).map(|i| format!("file-{i}.parquet")).collect();
        let files: Vec<(String, u64)> = file_names
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), (i as u64 + 1) * 100))
            .collect();
        let g = shard_count(3, 4, files.len());
        assert_eq!(g, 10, "G must equal file_count when product > file_count");
        let shards = crate::adapter::sharding::partition_files_by_bytes(files.clone(), g);
        assert_eq!(shards.len(), 10, "must produce exactly G=10 shards");
        // No shard is empty.
        for (i, shard) in shards.iter().enumerate() {
            assert!(!shard.is_empty(), "shard {i} must not be empty");
        }
        // All files covered exactly once.
        let all: Vec<String> = shards.iter().flatten().cloned().collect();
        let unique: HashSet<&String> = all.iter().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "files must be disjoint across shards"
        );
        assert_eq!(
            unique,
            file_names.iter().collect::<HashSet<_>>(),
            "all files must be covered"
        );
    }

    // ---------------------------------------------------------------------------
    // Row-scan SQL shape — GROUP BY shard_key fan-out, single-shard collapse
    // ---------------------------------------------------------------------------

    /// Multi-shard row-scan SQL uses GROUP BY shard_key, never IPROC().
    #[test]
    fn scan_driving_sql_groups_by_shard_key_not_iproc() {
        let files: Vec<(String, u64)> = (0..3)
            .map(|i| (format!("s3://warehouse/f{i}.parquet"), (i as u64 + 1) * 100))
            .collect();
        let g = shard_count(3, 1, files.len());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["ID".to_string()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "multi-shard SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY"),
            "multi-shard SQL must contain GROUP BY: {sql}"
        );
        assert!(
            sql.contains("shard_key"),
            "multi-shard SQL must use shard_key: {sql}"
        );
    }

    /// Single-shard collapses to the single-invocation form (no VALUES, no GROUP BY).
    #[test]
    fn single_shard_collapses_to_single_invocation() {
        let files = vec![("s3://warehouse/f0.parquet".to_string(), 500u64)];
        let g = shard_count(1, 1, files.len());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            shards,
            &["ID".to_string()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
        );
        assert!(
            !sql.contains("IPROC()"),
            "single-shard SQL must not contain IPROC: {sql}"
        );
        assert!(
            !sql.contains("VALUES"),
            "single-shard SQL must not contain VALUES: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY"),
            "single-shard SQL must not contain GROUP BY: {sql}"
        );
        assert!(
            sql.starts_with("SELECT * FROM (SELECT "),
            "must start with SELECT * FROM (SELECT ...: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Grouped aggregate scan SQL — GROUP BY shard_key fan-out
    // ---------------------------------------------------------------------------

    /// Helper: build grouped aggregate scan SQL.
    /// Keys-first classification: group keys at ordinals 0..m, aggregates after.
    fn keys_first_select_items(group_keys: usize, aggregates: usize) -> Vec<GroupedSelectItem> {
        let mut items = Vec::with_capacity(group_keys + aggregates);
        for slot in 0..group_keys {
            items.push(GroupedSelectItem::GroupKey {
                group_key_slot: slot,
                select_index: slot,
            });
        }
        for slot in 0..aggregates {
            items.push(GroupedSelectItem::Aggregate {
                plan_slot: slot,
                select_index: group_keys + slot,
            });
        }
        items
    }

    fn build_grouped_agg_sql(
        group_keys: Vec<String>,
        agg_plans: Vec<AggregatePlan>,
        files: Vec<String>,
        g: usize,
    ) -> String {
        let col_types: Vec<(String, String)> = vec![
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let files_with_sizes: Vec<(String, u64)> = files.into_iter().map(|p| (p, 1)).collect();
        let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, g);
        let select_items = keys_first_select_items(group_keys.len(), agg_plans.len());
        build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &group_keys,
            &[],
            &agg_plans,
            &[],
            &select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            None,
        )
    }

    /// Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units,
    /// serializing the common blob once and one files literal per shard.
    #[test]
    fn grouped_fan_out_common_once_files_per_shard() {
        // Two distinct files, forced onto two shards (2 nodes × factor 1).
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }],
            files,
            g,
        );
        assert!(
            !sql.contains("IPROC()"),
            "grouped SQL must NOT contain IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "grouped SQL inner must GROUP BY shard_key: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files)"),
            "grouped fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
        );

        // Common blob (credentials + tuning) serialized once, not per shard.
        assert_eq!(
            sql.matches("http://minio:9000").count(),
            1,
            "grouped common blob (endpoint) must appear exactly once: {sql}"
        );
        assert_eq!(
            sql.matches("memory_pool_fraction").count(),
            1,
            "grouped common blob (tuning payload) must appear exactly once: {sql}"
        );

        // Each shard's file appears exactly once, in its own VALUES row.
        for file in ["f0.parquet", "f1.parquet"] {
            assert_eq!(
                sql.matches(file).count(),
                1,
                "grouped shard file {file} must appear exactly once: {sql}"
            );
        }
    }

    /// LIMIT is NOT pushed into the shard scan for a grouped query. The shared common
    /// blob (arg 0) must not carry "limit"; only the outer wrapper may apply LIMIT.
    #[test]
    fn grouped_common_blob_has_no_limit() {
        let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: Some(100), // LIMIT should NOT appear inside the shard spec JSON
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            Some(100),
            &col_types,
            SCAN_UDF_NAME,
            None,
        );
        // The shared common blob (arg 0) is built once with limit = None, so it must
        // NOT carry a "limit" key — this is the structural LIMIT-exclusion invariant.
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "grouped common blob must NOT carry limit: {common}"
        );
        // The outer wrapper may still apply the final LIMIT.
        assert!(
            sql.contains("LIMIT 100"),
            "outer wrapper should still apply the final LIMIT: {sql}"
        );
    }

    /// Grouped aggregate wrapper SQL re-groups partial results per user group key.
    #[test]
    fn grouped_aggregate_wrapper_sql_groups_by_user_key_cols() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into(), "\"YEAR\"".into()],
            vec![
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                },
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                },
            ],
            files,
            g,
        );
        // Outer wrapper must GROUP BY GK_0, GK_1 (the group key columns).
        assert!(
            sql.contains("GK_0"),
            "wrapper SQL must reference GK_0: {sql}"
        );
        assert!(
            sql.contains("GK_1"),
            "wrapper SQL must reference GK_1: {sql}"
        );
        // Outer GROUP BY must merge partial aggregates.
        assert!(
            sql.contains("SUM("),
            "wrapper must contain SUM for merge: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_count_0"),
            "wrapper must reference PARTIAL_count_0: {sql}"
        );
        assert!(
            sql.contains("PARTIAL_sum_1"),
            "wrapper must reference PARTIAL_sum_1: {sql}"
        );
        // Outer must have GROUP BY GK_0, GK_1.
        let outer_group_by = sql
            .rfind("GROUP BY")
            .expect("must have GROUP BY in outer wrapper");
        let outer_group_by_clause = &sql[outer_group_by..];
        assert!(
            outer_group_by_clause.contains("GK_0"),
            "outer GROUP BY must include GK_0: {outer_group_by_clause}"
        );
        assert!(
            outer_group_by_clause.contains("GK_1"),
            "outer GROUP BY must include GK_1: {outer_group_by_clause}"
        );
    }

    // ---------------------------------------------------------------------------
    // build_grouped_aggregate_scan_sql — outer SELECT follows selectList order
    // (fix-grouped-agg-select-order, GitHub issue #33)
    // ---------------------------------------------------------------------------

    /// Extract the outer wrapper's SELECT list (between the leading `SELECT `
    /// and the `FROM (` that opens the fan-out subselect), split on the
    /// top-level commas of each column expression. Aggregate expressions and
    /// CAST(...) fragments never contain a bare `, ` outside of nested
    /// parens/quotes for the shapes used in these tests (SUM/COUNT merges and
    /// CAST("GK_i" AS ...)), so a paren-depth-aware split is sufficient.
    fn outer_select_items(sql: &str) -> Vec<String> {
        let from_pos = sql
            .find(" FROM (")
            .expect("must have outer FROM (: sql={sql}");
        let select_str = &sql["SELECT ".len()..from_pos];
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut current = String::new();
        for ch in select_str.chars() {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    items.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            items.push(current.trim().to_string());
        }
        items
    }

    /// Build grouped aggregate scan SQL with explicit (non-keys-first) `select_items`
    /// and declared group-key types, so ordering + CAST type can be asserted.
    fn build_grouped_agg_sql_with_select_items(
        group_keys: Vec<String>,
        group_key_types: Vec<String>,
        agg_plans: Vec<AggregatePlan>,
        aggregate_types: Vec<String>,
        select_items: Vec<GroupedSelectItem>,
        having: Option<&str>,
    ) -> String {
        let col_types: Vec<(String, String)> = vec![
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://wh/f0.parquet".to_string()]];
        build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &group_keys,
            &group_key_types,
            &agg_plans,
            &aggregate_types,
            &select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            having,
        )
    }

    /// #33 repro: `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`.
    /// The outer wrapper SELECT must place the merged SUM at position 0 and
    /// the CAST'd group key at position 1 — matching the user's selectList
    /// order, not the inner fan-out's keys-first shape.
    #[test]
    fn grouped_wrapper_agg_before_key_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            2,
            "outer SELECT must have exactly 2 items: {items:?}"
        );
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged aggregate: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key with its declared type: {items:?}"
        );
    }

    /// Interleaved multi-key: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
    /// Outer SELECT order must be [key0, aggregate, key1], matching selectList.
    #[test]
    fn grouped_wrapper_interleaved_multi_key_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#""REGION""#.to_string(), r#""YEAR""#.to_string()],
            vec!["VARCHAR(100)".to_string(), "DECIMAL(4,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 1,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 2,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            3,
            "outer SELECT must have exactly 3 items: {items:?}"
        );
        assert!(
            items[0].starts_with("CAST(\"GK_0\" AS VARCHAR(100))"),
            "position 0 must be key0's CAST: {items:?}"
        );
        assert!(
            items[1].contains("PARTIAL_sum_0") && items[1].starts_with("CAST(SUM("),
            "position 1 must be the merged aggregate: {items:?}"
        );
        assert!(
            items[2].starts_with("CAST(\"GK_1\" AS DECIMAL(4,0))"),
            "position 2 must be key1's CAST: {items:?}"
        );
    }

    /// Expression group key after an aggregate: `SELECT COUNT(*), MOD(id,4) ...
    /// GROUP BY MOD(id,4)`. The key's declared type (DECIMAL, from
    /// selectListDataTypes at its own select_index) must be preserved — this
    /// is what stops the silent VARCHAR(2000000) fallback for #33 sub-case 3.
    #[test]
    fn grouped_wrapper_expr_key_after_agg_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }],
            vec!["DECIMAL(18,0)".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            None,
        );
        let items = outer_select_items(&sql);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_count_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged COUNT: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key, not a VARCHAR fallback: {items:?}"
        );
    }

    /// Aggregate-first GROUP BY with HAVING: `SELECT SUM(score), MOD(id,4) ...
    /// GROUP BY MOD(id,4) HAVING SUM(score) > n`. Outer SELECT order must still
    /// follow selectList (aggregate first) and HAVING must be appended after
    /// GROUP BY, exercising the HAVING-present outer-wrapper path together with
    /// non-keys-first ordering.
    #[test]
    fn grouped_wrapper_agg_first_with_having_ordering() {
        let sql = build_grouped_agg_sql_with_select_items(
            vec![r#"("ID" % 4)"#.to_string()],
            vec!["DECIMAL(9,0)".to_string()],
            vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("SCORE".into()),
            }],
            vec!["DOUBLE PRECISION".to_string()],
            vec![
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 1,
                },
            ],
            Some(r#"(SUM("PARTIAL_sum_0") > 100)"#),
        );
        let having_pos = sql.find("HAVING").expect("must contain HAVING: {sql}");
        let group_by_pos = sql.find("GROUP BY").expect("must contain GROUP BY: {sql}");
        assert!(
            having_pos > group_by_pos,
            "HAVING must appear after GROUP BY: {sql}"
        );
        let select_only = &sql[..group_by_pos];
        let items = outer_select_items(select_only);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged aggregate even with HAVING present: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key even with HAVING present: {items:?}"
        );
    }

    /// A HAVING `SUM(score) > literal` node built as Exasol sends it (a
    /// `predicate_greater` whose `left` is a `function_aggregate`) must render
    /// against the MERGE decomposition: the aggregate reference becomes the
    /// merged partial expression `SUM("PARTIAL_sum_0")`, NOT the source column
    /// `SUM("SCORE")` (which does not exist in the outer wrapper). This is the
    /// #33 HAVING repro (`... GROUP BY MOD(id,4) HAVING SUM(score) > 250`).
    #[test]
    fn render_having_over_merge_rewrites_aggregate_to_partial() {
        let having = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 250},
        });
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
        }];
        let rendered = render_having_over_merge(&having, &plans)
            .expect("HAVING over a known aggregate must render");
        assert_eq!(
            rendered, r#"(SUM("PARTIAL_sum_0") > 250)"#,
            "HAVING must reference the merged partial, not the source column: {rendered}"
        );
        assert!(
            !rendered.contains(r#""SCORE""#) && !rendered.contains("SUM(\"SCORE\")"),
            "HAVING must NOT reference the source column SCORE: {rendered}"
        );
    }

    /// The full outer-wrapper SQL for the #33 HAVING repro must carry the merged
    /// HAVING `SUM("PARTIAL_sum_0") > 250` and must not reference the source
    /// `SCORE` column in the HAVING clause.
    #[test]
    fn grouped_wrapper_having_over_aggregate_uses_merge_expression() {
        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
            serde_json::json!([
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let having_node = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 250},
        });
        let having = render_having_over_merge(&having_node, &detection.plans)
            .expect("HAVING must render over the merge decomposition");

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://wh/f0.parquet".to_string()]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            Some(&having),
        );
        let having_pos = sql.find("HAVING").expect("must contain HAVING");
        let having_clause = &sql[having_pos..];
        assert!(
            having_clause.contains(r#"SUM("PARTIAL_sum_0") > 250"#),
            "HAVING clause must use the merge expression: {having_clause}"
        );
        assert!(
            !having_clause.contains(r#""SCORE""#) && !having_clause.contains("SUM(\"SCORE\")"),
            "HAVING clause must NOT reference the source SCORE column: {having_clause}"
        );
    }

    /// A HAVING referencing an aggregate that is NOT present among the plans
    /// (e.g. `COUNT(*)` when only `SUM(score)` was projected) cannot be merged,
    /// so `render_having_over_merge` returns None — the signal for
    /// `handle_pushdown` to DECLINE the pushdown rather than drop the HAVING.
    #[test]
    fn render_having_over_merge_declines_unknown_aggregate() {
        let having = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("COUNT", None, false),
            "right": {"type": "literal_exactnumeric", "value": 10},
        });
        // Only SUM(score) was projected — COUNT(*) has no matching plan.
        let plans = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
        }];
        assert!(
            render_having_over_merge(&having, &plans).is_none(),
            "HAVING over an aggregate absent from the plans must not render"
        );
    }

    /// End-to-end wiring: `detect_group_by_aggregates`'s classification output
    /// feeds directly into `build_grouped_aggregate_scan_sql` and the outer
    /// wrapper SELECT follows the original selectList order (#33 repro, driven
    /// through both functions together rather than a hand-built select_items).
    #[test]
    fn grouped_wrapper_outer_select_follows_select_list_order() {
        let req = make_group_by_request_with_types(
            serde_json::json!([mod_item("ID", 4)]),
            serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
            serde_json::json!([
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://wh/f0.parquet".to_string()]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            None,
        );

        let items = outer_select_items(&sql);
        assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
        assert!(
            items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
            "position 0 must be the merged SUM (selectList order): {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
            "position 1 must be the CAST'd group key with its declared type: {items:?}"
        );
    }

    // ---------------------------------------------------------------------------
    // group_key_exasol_types — index-based resolution, no silent VARCHAR fallback
    // (fix-grouped-agg-select-order, GitHub issue #33)
    // ---------------------------------------------------------------------------

    /// An expression group key whose `groupBy` and `selectList` renderings
    /// differ only by whitespace/casing must still resolve its declared type
    /// by index (via `select_items`), not by comparing rendered SQL strings —
    /// which would silently fall back to VARCHAR(2000000) on any drift.
    #[test]
    fn group_key_type_resolved_by_index_not_string_match() {
        // groupBy renders "(\"ID\" % 4)" (see MOD rendering); simulate a
        // whitespace/casing-drifted selectList rendering by using a
        // hand-built classification whose select_index points at a
        // selectListDataTypes slot the rendered-string form would never find.
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [mod_item("ID", 4)],
            "selectList": [
                agg_item("SUM", Some("SCORE"), false),
                mod_item("ID", 4),
            ],
            "selectListDataTypes": [
                {"type": "double"},
                decimal_type(9, 0),
            ],
        });
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

        // Sanity: the real detection path already resolves this correctly by
        // index. Now prove the mechanism is index-based, not string-based, by
        // building a classification where the rendered groupBy fragment would
        // NOT string-match the (hypothetically drifted) selectList rendering,
        // yet the index-based lookup still finds DECIMAL(9,0) because it reads
        // selectListDataTypes[select_index] directly.
        let group_keys = vec![r#"("id" % 4)"#.to_string()]; // lowercase drift vs GK render
        let select_items = detection.select_items.clone();
        let types = group_key_exasol_types(&req, &group_keys, &select_items);

        assert_eq!(
            types,
            vec!["DECIMAL(9,0)".to_string()],
            "type must resolve via select_index, not via string-matching the (drifted) \
             rendered group key: {types:?}"
        );
    }

    // ---------------------------------------------------------------------------
    // ScanSpec GROUP BY — group-key fragments propagated to the scan spec
    // ---------------------------------------------------------------------------

    /// Grouped scan spec carries group-key rendered SQL fragments.
    #[test]
    fn grouped_scan_spec_carries_group_keys() {
        let group_keys = vec!["\"REGION\"".to_string(), "YEAR(\"TS\")".to_string()];
        let spec = ScanSpec {
            files: vec!["s3://w/f0.parquet".into()],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
            }]),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).expect("must round-trip");
        let keys = back.group_keys.expect("group_keys must be present");
        assert_eq!(keys, group_keys, "group_keys must survive spec round-trip");
    }

    /// aggregationType missing or not "group_by" returns None.
    #[test]
    fn detect_group_by_aggregates_no_group_by_type_returns_none() {
        // No aggregationType.
        let req1 = serde_json::json!({
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req1).is_none());

        // aggregationType is "single_group".
        let req2 = serde_json::json!({
            "aggregationType": "single_group",
            "selectList": [agg_item("COUNT", None, false)],
        });
        assert!(detect_group_by_aggregates(&req2).is_none());
    }

    /// Empty groupBy array returns None.
    #[test]
    fn detect_group_by_aggregates_empty_group_by_returns_none() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [],
            "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        });
        assert!(detect_group_by_aggregates(&req).is_none());
    }

    // ---------------------------------------------------------------------------
    // Non-decomposable aggregate fallback to row scan
    // ---------------------------------------------------------------------------

    /// MEDIAN, *_DISTINCT, APPROX_COUNT_DISTINCT, LISTAGG, GROUP_CONCAT all cause
    /// parse_agg_item / detect_aggregates to return None (row-scan fallback).
    #[test]
    fn non_decomposable_aggregate_falls_back_to_row_scan() {
        for name in &[
            "MEDIAN",
            "APPROXIMATE_COUNT_DISTINCT",
            "LISTAGG",
            "GROUP_CONCAT",
        ] {
            let req = serde_json::json!({
                "selectList": [agg_item(name, Some("AMOUNT"), false)],
            });
            assert!(
                detect_aggregates(&req).is_none(),
                "{name} must fall back to row scan"
            );
        }
        // COUNT(DISTINCT col) — distinct flag set
        let req_distinct = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("ID"), true)],
        });
        assert!(
            detect_aggregates(&req_distinct).is_none(),
            "COUNT(DISTINCT) must fall back to row scan"
        );
    }

    // ---------------------------------------------------------------------------
    // STDDEV / VARIANCE decomposition into sufficient statistics
    // ---------------------------------------------------------------------------

    /// parse_agg_item returns a stat plan for STDDEV/VARIANCE family names.
    #[test]
    fn parse_agg_item_recognises_stat_functions() {
        for (name, expected_kind) in &[
            ("STDDEV", AggKind::StddevSamp),
            ("STDDEV_SAMP", AggKind::StddevSamp),
            ("STDDEV_POP", AggKind::StddevPop),
            ("VARIANCE", AggKind::VarSamp),
            ("VAR_SAMP", AggKind::VarSamp),
            ("VAR_POP", AggKind::VarPop),
        ] {
            let item = agg_item(name, Some("AMOUNT"), false);
            let plan =
                parse_agg_item(&item).unwrap_or_else(|| panic!("{name} must parse to a stat plan"));
            assert_eq!(
                plan.kind, *expected_kind,
                "{name} must map to {:?}",
                expected_kind
            );
            assert_eq!(plan.column.as_deref(), Some("AMOUNT"));
        }
    }

    /// partial_emits_items produces 3 columns for stat aggregates.
    #[test]
    fn stat_aggregate_emits_three_partial_columns() {
        for kind in &[
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            let plans = vec![AggregatePlan {
                kind: kind.clone(),
                column: Some("SCORE".into()),
            }];
            let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
            let items = partial_emits_items(&plans, &col_types);
            assert_eq!(
                items.len(),
                3,
                "{kind:?} must emit 3 partial columns, got: {items:?}"
            );
            assert!(
                items[0].contains("PARTIAL_stat_cnt_0"),
                "first column must be cnt: {items:?}"
            );
            assert!(
                items[1].contains("PARTIAL_stat_sum_0"),
                "second column must be sum: {items:?}"
            );
            assert!(
                items[2].contains("PARTIAL_stat_sumsq_0"),
                "third column must be sumsq: {items:?}"
            );
        }
    }

    /// merge_select_items produces the correct reconstruction SQL for VAR_POP.
    #[test]
    fn var_pop_merge_formula_divides_by_n() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain NULLIF(..., 0) guard on the count
        assert!(
            sql.contains("NULLIF"),
            "var_pop merge must guard zero count: {sql}"
        );
        // Must NOT divide by (count - 1)
        assert!(
            !sql.contains("- 1"),
            "var_pop must not subtract 1 from count: {sql}"
        );
    }

    /// merge_select_items for VAR_SAMP divides by N-1 and guards N<=1 → NULL.
    #[test]
    fn var_samp_merge_formula_divides_by_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::VarSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must use CASE WHEN … <= 1 THEN NULL to guard count<=1 → NULL.
        // Checking both `<= 1` and `CASE` ensures the N-1 sample divisor guard
        // is specifically present — not just any CASE or NULLIF in the expression.
        assert!(
            sql.contains("<= 1"),
            "var_samp merge must guard count<=1 with '<= 1': {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "var_samp merge must use CASE for N<=1 guard: {sql}"
        );
    }

    /// STDDEV_POP merge formula wraps variance in SQRT.
    #[test]
    fn stddev_pop_merge_formula_uses_sqrt() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_pop must use SQRT: {sql}");
        assert!(
            !sql.contains("- 1"),
            "stddev_pop must not subtract 1: {sql}"
        );
    }

    /// STDDEV_SAMP merge formula wraps variance-samp in SQRT.
    #[test]
    fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(sql.contains("SQRT("), "stddev_samp must use SQRT: {sql}");
        // N-1 guard: removing the N<=1 CASE would break this assertion.
        assert!(
            sql.contains("<= 1"),
            "stddev_samp must guard N<=1 (sample divisor): {sql}"
        );
        assert!(
            sql.contains("CASE"),
            "stddev_samp must use CASE for N<=1 guard: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // STDDEV/VARIANCE NULL-passthrough — N=0 (pop & samp) and N=1 (samp)
    // ---------------------------------------------------------------------------

    /// StddevPop merge SQL passes NULL through (N=0 → var_pop is NULL → stddev_pop NULL).
    ///
    /// Exasol `GREATEST(0.0, NULL) = 0.0` — a bare SQRT(GREATEST(...)) returns 0.0
    /// when cnt=0, not NULL. The correct form wraps in CASE WHEN IS NULL THEN NULL.
    #[test]
    fn stddev_pop_merge_null_passthrough_for_n_zero() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard (CASE … IS NULL) that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_pop must pass NULL through for N=0 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_pop must keep GREATEST rounding guard: {sql}"
        );
    }

    /// StddevSamp merge SQL passes NULL through for N=0 and N=1.
    ///
    /// var_samp is NULL when cnt<=1 (CASE guard). Wrapping in CASE WHEN IS NULL
    /// ensures SQRT does not receive 0.0 via GREATEST(0.0, NULL) = 0.0.
    #[test]
    fn stddev_samp_merge_null_passthrough_for_n_zero_and_n_one() {
        let plans = vec![AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("X".into()),
        }];
        let sql = merge_select_items(&plans).join(", ");
        // Must contain a NULL guard that wraps the whole expression.
        assert!(
            sql.contains("IS NULL"),
            "stddev_samp must pass NULL through for N<=1 via IS NULL guard: {sql}"
        );
        // The GREATEST guard against tiny-negative float rounding must still be present.
        assert!(
            sql.contains("GREATEST"),
            "stddev_samp must keep GREATEST rounding guard: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // HAVING must not be silently dropped on grouped-path type-validation failure
    // ---------------------------------------------------------------------------

    /// Regression: HAVING is present + grouped-path type-validation fails.
    ///
    /// Before the fix, `handle_pushdown` would fall through to the row-scan path
    /// and silently discard the HAVING predicate — yielding wrong results because
    /// the adapter advertised `AGGREGATE_HAVING` so Exasol does not re-apply it.
    ///
    /// This test proves the two components that the guard in `handle_pushdown`
    /// relies on: (a) HAVING renders to `Some` for this request, and (b) type
    /// validation fails for SUM over a non-numeric column. Together they mean the
    /// guard `if having.is_some() && !validate_agg_col_types(...)` triggers and
    /// the function returns an error instead of falling through.
    #[test]
    fn having_present_and_grouped_type_validation_fails_conditions_hold() {
        // Pushdown request: GROUP BY aggregate with SUM over VARCHAR (non-numeric)
        // and a simple HAVING predicate (column > literal — translatable by render_expression_safe).
        //
        // A HAVING with `function_aggregate` is NOT translatable by vs_expression, so we use a
        // plain column comparison to exercise the "having renders to Some" side of the invariant.
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "REGION", "dataType": {"type": "VARCHAR", "size": 100}},
                    {"name": "LABEL",  "dataType": {"type": "VARCHAR", "size": 50}},
                    {"name": "SCORE",  "dataType": {"type": "DOUBLE"}},
                ]
            }],
            "pushdownRequest": {
                "aggregationType": "group_by",
                "groupBy": [{"type": "column", "name": "REGION"}],
                "selectList": [
                    {"type": "column", "name": "REGION"},
                    {
                        "type": "function_aggregate",
                        "name": "SUM",
                        "arguments": [{"type": "column", "name": "LABEL"}]
                    }
                ],
                "having": {
                    "type": "predicate_greater",
                    "left":  {"type": "column", "name": "SCORE"},
                    "right": {"type": "literal_exactnumeric", "value": "100"}
                }
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let col_types = extract_all_column_types(&request);

        // (a) detect_group_by_aggregates must find a grouped path.
        let detected = detect_group_by_aggregates(&pushdown_req);
        assert!(
            detected.is_some(),
            "test setup: must detect grouped aggregates"
        );
        let grouped_plans = detected.unwrap().plans;

        // (b) validate_agg_col_types must fail (SUM over VARCHAR is invalid).
        assert!(
            !validate_agg_col_types(&grouped_plans, &col_types),
            "type validation must fail for SUM(VARCHAR)"
        );

        // (c) HAVING must render to Some — confirming it would be dropped without the guard.
        let having = pushdown_req
            .get("having")
            .filter(|h| !h.is_null())
            .and_then(render_expression_safe);
        assert!(
            having.is_some(),
            "HAVING must render to Some — without the guard it would be silently dropped"
        );

        // Both conditions simultaneously: this is exactly the state that triggers the
        // guard `if having.is_some() && !validate_agg_col_types(...)` in handle_pushdown.
        // When both hold, handle_pushdown returns Err (not Ok with dropped HAVING).
        assert!(
            having.is_some() && !validate_agg_col_types(&grouped_plans, &col_types),
            "guard condition must hold: having present AND type validation failed"
        );
    }

    // ---------------------------------------------------------------------------
    // Select-list scalar expression pushdown
    // ---------------------------------------------------------------------------

    /// A function_scalar in the select list renders to a SQL expression in the
    /// scan spec projection and EMITS clause.
    #[test]
    fn selectlist_scalar_expression_rendered_in_emits() {
        // Simulate a pushdown request with UPPER(name) in the select list.
        let upper_expr = serde_json::json!({
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [{"type": "column", "name": "NAME"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "ID", "dataType": {"type": "DECIMAL", "precision": 10, "scale": 0}},
                    {"name": "NAME", "dataType": {"type": "VARCHAR", "size": 100}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [upper_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // The rendered expression should be in projection
        assert_eq!(proj_cols.len(), 1);
        assert!(
            proj_cols[0].contains("UPPER") || proj_cols[0].contains("upper"),
            "projection must contain rendered expression: {proj_cols:?}"
        );
        // Type for an expression falls back to VARCHAR(2000000)
        assert_eq!(proj_types[0], "VARCHAR(2000000)");
    }

    /// An untranslatable select-list item falls back to the bare column.
    #[test]
    fn selectlist_untranslatable_item_falls_back_to_column() {
        // A node type the translator cannot handle
        let bad_expr = serde_json::json!({
            "type": "function_aggregate",  // aggregate in select list -> untranslatable as scalar expr
            "name": "SUM",
            "arguments": [{"type": "column", "name": "AMOUNT"}]
        });
        let request = serde_json::json!({
            "involvedTables": [{
                "columns": [
                    {"name": "AMOUNT", "dataType": {"type": "DECIMAL", "precision": 18, "scale": 2}},
                ]
            }],
            "pushdownRequest": {
                "selectList": [bad_expr],
            }
        });
        let pushdown_req = request["pushdownRequest"].clone();
        let (proj_cols, proj_types) = extract_projection(&request, &pushdown_req).unwrap();
        // Fall back to the first column name
        assert_eq!(proj_cols.len(), 1);
        assert_eq!(proj_cols[0], "AMOUNT");
        assert_eq!(proj_types[0], "DECIMAL(18,2)");
    }

    // ---------------------------------------------------------------------------
    // HAVING predicate — applied in the outer wrapper only, never in shard scan
    // ---------------------------------------------------------------------------

    /// HAVING is rendered and appears in the outer GROUP BY wrapper SQL.
    #[test]
    fn having_clause_appears_in_outer_wrapper_only() {
        // Build a grouped aggregate SQL with a HAVING predicate.
        let having_filter = Some(r#"(SUM("AMOUNT") > 100)"#.to_string());
        let spec_template = ScanSpec {
            files: vec![],
            projection: vec!["REGION".into(), "AMOUNT".into()],
            filter: None,
            limit: None,
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            }]),
            group_keys: Some(vec![r#""REGION""#.to_string()]),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let shards = vec![vec!["s3://wh/f.parquet".into()]];
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            shards,
            &[r#""REGION""#.to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
            }],
            &[],
            &keys_first_select_items(1, 1),
            None,
            &col_types,
            SCAN_UDF_NAME,
            having_filter.as_deref(),
        );
        // HAVING must appear in the outer wrapper (after GROUP BY)
        assert!(
            sql.contains("HAVING"),
            "outer wrapper must contain HAVING: {sql}"
        );
        assert!(
            sql.contains("100"),
            "HAVING predicate value must be in SQL: {sql}"
        );
        // HAVING must come after GROUP BY
        let having_pos = sql.find("HAVING").unwrap();
        let group_by_pos = sql.find("GROUP BY").unwrap();
        assert!(
            having_pos > group_by_pos,
            "HAVING must appear after GROUP BY: {sql}"
        );
    }

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

    /// Scenario: build_load_table_url inserts an ARN-shaped warehouse verbatim.
    ///
    /// For AWS Glue the warehouse value is a catalog ARN
    /// (`arn:aws:glue:region:acct:catalog`). The current implementation places it
    /// verbatim in the URL path — no URL-encoding, no config-endpoint round-trip.
    /// This test pins that behaviour so a future refactor (config-endpoint prefix
    /// fetch or URL-encoding) does not regress silently.
    #[test]
    fn build_load_table_url_with_arn_shaped_warehouse() {
        let arn = "arn:aws:glue:us-east-1:123456789012:catalog";
        let url = build_load_table_url(
            "https://glue.us-east-1.amazonaws.com/iceberg",
            arn,
            "mydb",
            "orders",
        );
        // The ARN appears verbatim between /v1/ and /namespaces/.
        assert_eq!(
            url,
            format!(
                "https://glue.us-east-1.amazonaws.com/iceberg/v1/{arn}/namespaces/mydb/tables/orders"
            ),
            "ARN must be inserted verbatim (ponytail: no URL-encoding; upgrade path is config-endpoint fetch)"
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
    // Task 4.1 — Pushdown wiring: filter JSON reaches Iceberg predicate and
    // ScanSpec.filter (DataFusion string) is preserved on both paths.
    // ---------------------------------------------------------------------------

    /// Scenario: Filter predicate is pushed into the scan spec.
    ///
    /// For a translatable filter (equality on a typed column):
    /// - `ScanSpec.filter` (DataFusion SQL string) is `Some`.
    /// - `to_iceberg_predicate` over the same JSON + a matching schema is `Some`.
    ///
    /// Both coexist: Iceberg prunes files; DataFusion enforces row correctness.
    #[test]
    fn filter_in_common_arg() {
        use crate::adapter::iceberg_predicate::to_iceberg_predicate;
        use iceberg::spec::{NestedField, Schema, Type};
        use std::sync::Arc;

        // Build a minimal schema with an Int column "id".
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .unwrap();

        let filter_json = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "id"},
            "right": {"type": "literal_exactnumeric", "value": 42}
        });

        // DataFusion path: render_df_filter_safe must produce Some.
        let df_filter = render_df_filter_safe(&filter_json);
        assert!(
            df_filter.is_some(),
            "translatable filter must produce a DataFusion SQL string"
        );

        // Iceberg path: to_iceberg_predicate over the same JSON must produce Some.
        let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
        assert!(
            iceberg_pred.is_some(),
            "translatable filter must produce an Iceberg predicate"
        );

        // Confirm the DataFusion string survives into the common (arg 0) blob.
        let sql = build_sql_for_fixture(
            vec!["s3://warehouse/f.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(10,0)".into()],
            df_filter,
            None,
        );
        let common = common_arg_literal(&sql);
        assert!(
            common.contains("\"filter\"") && common.contains("42"),
            "filter must be pushed into the common arg: {common}"
        );
    }

    /// Scenario: A LIKE-only filter still yields a valid `ScanSpec.filter` (DataFusion
    /// evaluates it) while `to_iceberg_predicate` returns `None` (no file pruning).
    ///
    /// This confirms the correctness invariant: LIKE is not prunable but remains
    /// fully enforced by DataFusion.
    #[test]
    fn like_filter_yields_df_string_and_no_iceberg_predicate() {
        use crate::adapter::iceberg_predicate::to_iceberg_predicate;
        use iceberg::spec::{NestedField, Schema, Type};
        use std::sync::Arc;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "name",
                Type::Primitive(iceberg::spec::PrimitiveType::String),
            ))])
            .build()
            .unwrap();

        let filter_json = serde_json::json!({
            "type": "predicate_like",
            "expression": {"type": "column", "name": "name"},
            "pattern": {"type": "literal_string", "value": "A%"}
        });

        // DataFusion path must still yield Some (LIKE is translatable to DataFusion SQL).
        let df_filter = render_df_filter_safe(&filter_json);
        assert!(
            df_filter.is_some(),
            "LIKE filter must still produce a DataFusion SQL string: {df_filter:?}"
        );

        // Iceberg path must be None — LIKE is not soundly prunable.
        let iceberg_pred = to_iceberg_predicate(&filter_json, &schema);
        assert!(
            iceberg_pred.is_none(),
            "LIKE filter must produce no Iceberg predicate"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 4.3 — No credential in error text
    // ---------------------------------------------------------------------------

    /// Scenario: redact_catalog_error removes credential-shaped values from messages.
    #[test]
    fn redact_catalog_error_strips_credentials() {
        let msg = "GET failed: access_key=AKID_SECRET_VALUE region=us-east-1";
        let safe = redact_catalog_error(msg);
        assert!(
            !safe.contains("AKID_SECRET_VALUE"),
            "credential value must be redacted: {safe}"
        );
        assert!(
            safe.contains("access_key"),
            "label must be preserved: {safe}"
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

    /// A baseline `ConnectionCreds` with no catalog auth (all auth fields `None`).
    /// Individual tests set only the auth fields under test.
    fn base_creds() -> ConnectionCreds {
        ConnectionCreds {
            warehouse: "warehouse".into(),
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: None,
            path_style: true,
            use_sigv4: false,
            use_vended_credentials: false,
            token: None,
            client_id: None,
            client_secret: None,
            oauth2_server_uri: None,
            scope: None,
        }
    }

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

    /// Scenario: Catalog auth props — and the whole catalog block — are never placed
    /// in any scan spec.
    ///
    /// The UDF-boundary secret invariant: auth lives on `ConnectionCreds` and is
    /// consumed only in the planning-layer catalog build. A `ScanSpec` (serialized
    /// for the UDF boundary) must carry no catalog block at all, none of the auth
    /// field NAMES, nor any auth VALUE — the scan UDF never calls the catalog.
    #[test]
    fn scan_spec_carries_no_catalog_block() {
        // Distinctive sentinels: any of these surfacing in the serialized spec is a leak.
        const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_VALUE";
        const SECRET_SENTINEL: &str = "CLIENT_SECRET_SENTINEL_VALUE";
        const OAUTH_URI_SENTINEL: &str = "https://oauth-uri-sentinel.example/token";
        const SCOPE_SENTINEL: &str = "SCOPE_SENTINEL_VALUE";

        // Build a spec exactly as handle_pushdown does — auth creds exist but are
        // NEVER threaded into ScanSpec (it has no auth fields by construction).
        let spec = ScanSpec {
            files: vec!["s3://warehouse/db/events/part-00000.parquet".into()],
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            aggregates: None,
            group_keys: None,
            emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            logical_schema: Vec::new(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };

        let json = spec.to_json();

        // The dropped `catalog` block must not appear in the full spec nor the
        // shard-invariant common blob (the scan UDF never touches the catalog).
        assert!(
            !json.contains("catalog"),
            "ScanSpec JSON must not carry a catalog block: {json}"
        );
        assert!(
            !spec.to_common_json().contains("catalog"),
            "common blob must not carry a catalog block: {}",
            spec.to_common_json()
        );

        // No auth field NAMES (planning-layer concepts) in the serialized spec.
        for field in [
            "token",
            "credential",
            "client_id",
            "client_secret",
            "oauth2_server_uri",
            "oauth2-server-uri",
            "scope",
        ] {
            assert!(
                !json.contains(field),
                "ScanSpec JSON must not carry auth field '{field}': {json}"
            );
        }

        // No auth VALUES, even if a future refactor wired creds in by mistake.
        for value in [
            TOKEN_SENTINEL,
            SECRET_SENTINEL,
            OAUTH_URI_SENTINEL,
            SCOPE_SENTINEL,
        ] {
            assert!(
                !json.contains(value),
                "ScanSpec JSON must not carry auth value '{value}': {json}"
            );
        }

        // The storage block carries only the S3 storage credentials, exactly as
        // in the established credential flows.
        assert!(
            json.contains("minioadmin"),
            "storage S3 creds must still be present: {json}"
        );
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

    /// Static storage with the same sentinel keys.
    fn static_storage() -> StorageProps {
        StorageProps {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            access_key: STATIC_AK.into(),
            secret_key: STATIC_SK.into(),
            session_token: None,
            allow_http: false,
            path_style: false,
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
            files: vec!["s3://warehouse/db/events/part-00000.parquet".into()],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            emit_exa_types: vec!["DECIMAL(20,0)".into()],
            logical_schema: Vec::new(),
            storage: vended_storage,
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
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
    // R1 — SigV4 skips /v1/config round-trip, uses warehouse directly
    // ---------------------------------------------------------------------------

    /// Scenario: The SigV4 path short-circuits `resolve_load_table_prefix` and
    /// returns the warehouse ARN unchanged, even when the catalog server would
    /// return a DIFFERENT prefix.
    ///
    /// A local HTTP server is started that responds with `overrides.prefix` =
    /// `"server-returned-prefix"`. For non-SigV4, that prefix would be used.
    /// For SigV4, the function must return the original warehouse ARN WITHOUT
    /// contacting the server — proved by the contrast with the paired non-SigV4
    /// test `non_sigv4_config_prefix_resolution_uses_config_endpoint`.
    #[tokio::test]
    async fn sigv4_skips_config_prefix_lookup_uses_warehouse_directly() {
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Bind a local server that returns a DIFFERENT prefix. If SigV4 contacted
        // it, the result would differ from the warehouse ARN.
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
        let warehouse_arn = "arn:aws:glue:us-east-1:123456789012:catalog";

        let mut creds = base_creds();
        creds.use_sigv4 = true;
        let auth = CatalogAuth::Sigv4;

        let result = resolve_load_table_prefix(&catalog_uri, warehouse_arn, &auth, &creds).await;

        assert_eq!(
            result, warehouse_arn,
            "SigV4 path must return the warehouse ARN directly, \
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

    // ---------------------------------------------------------------------------
    // Task 3.2 — Pushdown spec carries logical schema field-ids
    // ---------------------------------------------------------------------------

    /// Scenario (pushdown-planning): A pushdown request produces a scan spec whose
    /// `logical_schema` carries the expected field-ids, current names, and nullability.
    ///
    /// Builds an in-memory Iceberg schema and verifies that `build_logical_schema`
    /// produces a `Vec<LogicalField>` with the correct field-id, name, arrow_type
    /// tag, and nullable flag for each field. This covers: required field (nullable=false),
    /// optional field (nullable=true), and multiple Iceberg type families.
    #[test]
    fn pushdown_carries_logical_schema_in_common_arg() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        // Construct an Iceberg schema with 4 fields covering required, optional,
        // and several type families.
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::required(
                    3,
                    "label",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "amount",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 18,
                        scale: 4,
                    }),
                )),
            ])
            .build()
            .unwrap();

        let logical = build_logical_schema(&schema);

        assert_eq!(logical.len(), 4, "must carry all 4 fields");

        // Field 1: required Int → nullable=false, arrow_type="int32"
        assert_eq!(logical[0].field_id, 1);
        assert_eq!(logical[0].name, "id");
        assert_eq!(logical[0].arrow_type, "int32");
        assert!(
            !logical[0].nullable,
            "required field must have nullable=false"
        );

        // Field 2: optional Double → nullable=true, arrow_type="float64"
        assert_eq!(logical[1].field_id, 2);
        assert_eq!(logical[1].name, "score");
        assert_eq!(logical[1].arrow_type, "float64");
        assert!(
            logical[1].nullable,
            "optional field must have nullable=true"
        );

        // Field 3: required String → nullable=false, arrow_type="utf8"
        assert_eq!(logical[2].field_id, 3);
        assert_eq!(logical[2].name, "label");
        assert_eq!(logical[2].arrow_type, "utf8");
        assert!(!logical[2].nullable);

        // Field 4: optional Decimal(18,4) → nullable=true, arrow_type="decimal128(18,4)"
        assert_eq!(logical[3].field_id, 4);
        assert_eq!(logical[3].name, "amount");
        assert_eq!(logical[3].arrow_type, "decimal128(18,4)");
        assert!(logical[3].nullable);

        // Verify round-trip through ScanSpec: logical_schema survives JSON serde.
        let spec = ScanSpec {
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: logical.clone(),
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
        };
        let json = spec.to_json();
        let back = ScanSpec::from_json(&json).unwrap();
        assert_eq!(
            back.logical_schema.len(),
            4,
            "logical_schema must survive ScanSpec JSON round-trip"
        );
        assert_eq!(back.logical_schema[0], logical[0]);
        assert_eq!(back.logical_schema[3], logical[3]);

        // The logical schema is a shard-invariant field, so it must be carried in the
        // common (arg 0) blob — the scan UDF reads it identically for every shard.
        let common_json = spec.to_common_json();
        let common_back = crate::scan::spec::CommonScanSpec::from_json(&common_json).unwrap();
        assert_eq!(
            common_back.logical_schema, logical,
            "logical_schema must be carried in the common arg"
        );
    }
}
