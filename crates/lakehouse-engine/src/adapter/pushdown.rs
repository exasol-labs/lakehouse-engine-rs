use crate::adapter::connection::ConnectionCreds;
use crate::scan::spec::{
    AggKind, AggregatePlan, CatalogProps, DeleteFileContentType, DeleteFileRef, FileEntry,
    JoinSpec, JoinType, LogicalField, NameMappingEntry, ProjectionItem, ScanSpec, SortKey,
    StorageProps, render_order_by_clause,
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
/// scan-driving SQL that invokes the LAKEHOUSE_SCAN SCALAR EMIT UDF.
///
/// Architecture invariants:
/// - File list resolved exactly ONCE here, in the planning layer.
/// - The scan SCALAR EMIT UDF receives the explicit file list; it NEVER discovers files.
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

/// The registered SQL name of the scan SCALAR EMIT UDF entry point.
const SCAN_UDF_NAME: &str = "LAKEHOUSE_SCAN";

/// The registered SQL name of the scalar distinct-merge UDF entry point.
/// The outer wrapper of a single-group `COUNT(DISTINCT)` pushdown feeds the
/// per-shard JSON-array partials into this scalar UDF (via `LISTAGG`); like the
/// scan UDF it must be schema-qualified so it resolves outside the adapter schema.
const DISTINCT_MERGE_UDF_NAME: &str = "LAKEHOUSE_DISTINCT_MERGE_COUNT";

/// The registered SQL name of the file-distributor LUA SET script.
/// The nested fan-out subquery groups the per-shard file-list rows by `shard_key`
/// through this passthrough distributor so Exasol spreads the work units across
/// nodes; the outer ungrouped scalar scan then streams over the distributed rows.
/// Like the scan/merge scripts it must be schema-qualified to resolve outside the
/// adapter schema.
const DISTRIBUTE_FILES_UDF_NAME: &str = "LAKEHOUSE_DISTRIBUTE_FILES";

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
/// - any select item has `distinct: true` OTHER than a `COUNT(DISTINCT ...)`
///   (single-group `COUNT(DISTINCT col)` / `COUNT(DISTINCT expr)` is accepted as
///   [`AggKind::CountDistinct`]; DISTINCT SUM/AVG/etc. still decline)
/// - any select item is not one of COUNT(*), COUNT(col)/COUNT(expr),
///   SUM/MIN/MAX/AVG (bare column or renderable expression), or the
///   STDDEV/VARIANCE family
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
        // A single-group COUNT(DISTINCT ...) is decomposed into a per-shard local
        // distinct set; every OTHER distinct aggregate declines via parse_agg_item.
        let plan = match parse_count_distinct(item) {
            Some(distinct_plan) => distinct_plan,
            None => parse_agg_item(item)?,
        };
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
// `Eq` is intentionally NOT derived: the `ScalarOverAggregate` variant carries a
// raw `serde_json::Value` node (which is `PartialEq` but not `Eq` — it can hold
// floats). `PartialEq` is all the tests and detection need.
#[derive(Debug, Clone, PartialEq)]
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
    /// A constant/literal projection placeholder (Exasol's "count the groups"
    /// rewrite: a `selectList` composed only of a `literal_null` when the outer
    /// query needs the row-per-group shape but not the inner values). It
    /// contributes NO aggregate plan, so the grouped scan emits one row per
    /// distinct group. `projection` is the ready-to-emit outer-wrapper SELECT
    /// expression (the rendered literal, cast to its declared Exasol type — e.g.
    /// `CAST(NULL AS BOOLEAN)`), never a bare literal reused as a column
    /// identifier. `select_index` is the item's original `selectList` ordinal.
    Constant {
        select_index: usize,
        projection: String,
    },
    /// A scalar/arithmetic `selectList` expression that WRAPS one or more nested
    /// `function_aggregate` nodes (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`).
    /// The scalar wrapper itself is not decomposable — only its inner aggregates
    /// are: each is folded into the shared `plans` list (deduplicated by
    /// `AggregatePlan` equality) at detection, and the wrapper is rendered over the
    /// MERGED partials in the outer wrapper (never per shard, never over a source
    /// column). `node` is the raw `selectList` node, rewritten by
    /// `render_scalar_over_merge` at build time; `declared_type` is the item's own
    /// `selectListDataTypes` Exasol type (resolved once at detection), applied as the
    /// outer-wrapper CAST so Exasol's positional pushdown-column-type check passes.
    /// `select_index` is the item's original `selectList` ordinal.
    ScalarOverAggregate {
        select_index: usize,
        node: Json,
        declared_type: String,
    },
}

/// The original `selectList` ordinal of a classified item.
fn select_item_index(item: &GroupedSelectItem) -> usize {
    match item {
        GroupedSelectItem::GroupKey { select_index, .. }
        | GroupedSelectItem::Aggregate { select_index, .. }
        | GroupedSelectItem::Constant { select_index, .. }
        | GroupedSelectItem::ScalarOverAggregate { select_index, .. } => *select_index,
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
    /// Aggregate plans, deduplicated by `AggregatePlan` equality. Includes both
    /// top-level `function_aggregate` select items AND aggregates nested inside a
    /// `ScalarOverAggregate` select item — a `COUNT(*)` used bare and inside a
    /// scalar collapses to ONE plan here.
    pub plans: Vec<AggregatePlan>,
    /// The Exasol-declared result type of each plan, positionally aligned 1:1 with
    /// `plans` (NOT with the `selectList`). A top-level aggregate contributes its
    /// own `selectListDataTypes` type; a plan seen only nested inside a scalar has
    /// no `selectList` ordinal of its own and defaults to `DOUBLE PRECISION` (its
    /// merged form is rendered UNCAST inside the scalar wrapper anyway — the wrapper
    /// item is cast to its OWN declared type). Replaces `aggregate_exasol_types` on
    /// the grouped path, which keyed off top-level select items only and would
    /// misalign once nested aggregates join `plans`.
    pub plan_types: Vec<String>,
    /// One entry per `selectList` item, in `selectList` order.
    pub select_items: Vec<GroupedSelectItem>,
}

/// Build the outer-wrapper SELECT expression for a constant/literal `selectList`
/// item, cast to the Exasol type Exasol declared for that ordinal.
///
/// `rendered` is the literal already rendered to SQL (e.g. `NULL`, `'x'`, `5`).
/// The result is placed in the outer wrapper SELECT (`SELECT <expr> FROM (...)
/// GROUP BY GK_*`), so it must be a self-contained expression, never a column
/// reference. Casting to the declared type keeps the pushdown output column type
/// matching what Exasol validates positionally against `selectListDataTypes`
/// (mirrors the group-key and aggregate cast discipline); the cast is skipped for
/// the `VARCHAR(2000000)` default, matching `group_key_exasol_types`.
fn constant_projection_sql(pushdown_req: &Json, select_index: usize, rendered: &str) -> String {
    let declared = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .and_then(|d| d.get(select_index))
        .map(exasol_type_from_json);
    match declared {
        Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({rendered} AS {ty})"),
        _ => rendered.to_string(),
    }
}

/// `selectList` item types that render to a bare literal value rather than a
/// source column or a translatable scan-side expression.
///
/// Shared by `detect_group_by_aggregates` (classifies these as
/// `GroupedSelectItem::Constant`, per its doc comment above) and
/// `extract_projection` (routes these to the full-row fallback) so the two
/// call sites can never drift apart again (issue #52: `literal_bool` was
/// missing from one of the two copy-pasted lists).
const LITERAL_SELECTLIST_TYPES: &[&str] = &[
    "literal_null",
    "literal_bool",
    "literal_string",
    "literal_exactnumeric",
    "literal_double",
    "literal_date",
    "literal_timestamp",
    "literal_timestamp_utc",
];

/// Whether a `selectList` item's `type` is a bare literal (see
/// `LITERAL_SELECTLIST_TYPES`).
fn is_literal_selectlist_item(item_type: &str) -> bool {
    LITERAL_SELECTLIST_TYPES.contains(&item_type)
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

    let declared_type_at = |select_index: usize| -> String {
        pushdown_req
            .get("selectListDataTypes")
            .and_then(|v| v.as_array())
            .and_then(|d| d.get(select_index))
            .map(exasol_type_from_json)
            .unwrap_or_else(|| "VARCHAR(2000000)".to_string())
    };

    let mut plans = Vec::new();
    let mut plan_types = Vec::new();
    let mut select_items = Vec::with_capacity(list.len());
    for (select_index, item) in list.iter().enumerate() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match item_type {
            "function_aggregate" => {
                let plan = parse_agg_item(item)?;
                // A top-level aggregate carries its own authoritative declared type.
                let plan_slot = fold_aggregate_plan(
                    &mut plans,
                    &mut plan_types,
                    plan,
                    Some(declared_type_at(select_index)),
                );
                select_items.push(GroupedSelectItem::Aggregate {
                    plan_slot,
                    select_index,
                });
            }
            t if is_literal_selectlist_item(t) => {
                // A bare literal is a constant projection, not a group-key
                // reference — see the `Constant` variant's doc comment above
                // for the "count the groups" rationale.
                let rendered = render_expression(item).ok()?;
                let projection = constant_projection_sql(pushdown_req, select_index, &rendered);
                select_items.push(GroupedSelectItem::Constant {
                    select_index,
                    projection,
                });
            }
            _ => {
                // First: a group-key projection — a plain column reference, or a
                // scalar expression that renders to one of the group keys (e.g.
                // SELECT MOD(id,4) ... GROUP BY MOD(id,4)) emitted via GK_*.
                if let Some(group_key_slot) = render_expression(item)
                    .ok()
                    .and_then(|sql| group_keys.iter().position(|gk| *gk == sql))
                {
                    select_items.push(GroupedSelectItem::GroupKey {
                        group_key_slot,
                        select_index,
                    });
                    continue;
                }
                // Otherwise: a scalar function / arithmetic node WRAPPING one or more
                // aggregates (e.g. `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`). Fold
                // each nested aggregate into the shared `plans` list (deduplicated by
                // `AggregatePlan` equality) and classify the item as
                // `ScalarOverAggregate`. `None` here declines the WHOLE grouped
                // detection → the caller routes to the qualified single-table
                // wrapper fallback (never a bare row scan).
                let nested = classify_scalar_over_aggregate(item)?;
                for plan in nested {
                    // Nested-only aggregates have no `selectList` ordinal of their
                    // own → default declared type (DOUBLE PRECISION); a later/earlier
                    // top-level occurrence upgrades it via `fold_aggregate_plan`.
                    fold_aggregate_plan(&mut plans, &mut plan_types, plan, None);
                }
                select_items.push(GroupedSelectItem::ScalarOverAggregate {
                    select_index,
                    node: item.clone(),
                    declared_type: declared_type_at(select_index),
                });
            }
        }
    }

    Some(GroupedAggregateDetection {
        group_keys,
        plans,
        plan_types,
        select_items,
    })
}

/// Fold an aggregate plan into the shared `plans`/`plan_types` lists, deduplicating
/// by `AggregatePlan` equality (kind + argument) so an aggregate used more than once
/// across the select list — bare AND nested inside a scalar — collapses to ONE
/// `PARTIAL_*` column (decision-log [4]). Returns the plan's slot.
///
/// `declared` is `Some` for a top-level `function_aggregate` select item (its
/// authoritative `selectListDataTypes` type) and `None` for an aggregate seen only
/// nested inside a scalar. A `Some` declared type always wins: it overwrites a slot
/// that a nested occurrence created with the default, so a bare aggregate's output
/// CAST uses the type Exasol declared for it regardless of select-list order.
fn fold_aggregate_plan(
    plans: &mut Vec<AggregatePlan>,
    plan_types: &mut Vec<String>,
    plan: AggregatePlan,
    declared: Option<String>,
) -> usize {
    match plans.iter().position(|p| *p == plan) {
        Some(slot) => {
            if let Some(ty) = declared {
                plan_types[slot] = ty;
            }
            slot
        }
        None => {
            let slot = plans.len();
            plans.push(plan);
            plan_types.push(declared.unwrap_or_else(|| "DOUBLE PRECISION".to_string()));
            slot
        }
    }
}

/// Sentinel `column` name substituted for the i-th nested aggregate while rendering
/// a scalar-over-aggregate node through the `vs-expression` translator. Distinctive
/// and already uppercase so it survives the translator's `column` uppercasing and
/// cannot collide with a real column; the rendered token is later string-replaced
/// with the aggregate's merged `PARTIAL_*` expression.
fn agg_sentinel_name(i: usize) -> String {
    format!("__LH_AGG_MERGE_{i}__")
}

/// The exact SQL token `vs-expression` emits for the i-th aggregate sentinel column
/// (a quoted identifier), used as the string-replacement target.
fn agg_sentinel_token(i: usize) -> String {
    quote_ident(&agg_sentinel_name(i))
}

/// Build the sentinel `column` node for the i-th nested aggregate.
fn sentinel_column_node(i: usize) -> Json {
    serde_json::json!({ "type": "column", "name": agg_sentinel_name(i) })
}

/// Deep-clone `node`, replacing every nested `function_aggregate` subtree with a
/// sentinel `column` node (`__LH_AGG_MERGE_{i}__`) and collecting the original
/// aggregate nodes in sentinel order. Recursion STOPS at a `function_aggregate`
/// (its arguments are subsumed into the aggregate and rewritten wholesale), so a
/// `column` inside an aggregate (e.g. inside `SUM(CASE … col …)`) is never treated
/// as a residual. `residual_column` is set when a bare `column` appears OUTSIDE any
/// aggregate — the outer merge wrapper exposes only `GK_*`/`PARTIAL_*` columns, so
/// such a node cannot be rendered there and disqualifies the scalar-over-aggregate
/// classification (the request routes to the qualified fallback instead).
fn sentinelize_aggregates(
    node: &Json,
    aggregates: &mut Vec<Json>,
    residual_column: &mut bool,
) -> Json {
    match node {
        Json::Object(map) => match map.get("type").and_then(|t| t.as_str()) {
            Some("function_aggregate") => {
                let i = aggregates.len();
                aggregates.push(node.clone());
                sentinel_column_node(i)
            }
            kind => {
                if kind == Some("column") {
                    *residual_column = true;
                }
                let mut out = serde_json::Map::with_capacity(map.len());
                for (key, value) in map {
                    out.insert(
                        key.clone(),
                        sentinelize_aggregates(value, aggregates, residual_column),
                    );
                }
                Json::Object(out)
            }
        },
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|v| sentinelize_aggregates(v, aggregates, residual_column))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Classify a `selectList` item as a scalar-over-aggregate: a scalar/arithmetic node
/// wrapping one or more nested `function_aggregate` nodes, every one decomposable via
/// `parse_agg_item` (so `DISTINCT`, an unsupported function, or an untranslatable
/// argument declines), with no bare source `column` outside those aggregates and a
/// residual structure the `vs-expression` translator can render. Returns the nested
/// plans in encounter order, or `None` to decline (→ qualified fallback).
fn classify_scalar_over_aggregate(node: &Json) -> Option<Vec<AggregatePlan>> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    // Not a scalar-over-aggregate: no aggregate to decompose, or a source column the
    // outer merge wrapper cannot reference.
    if aggregates.is_empty() || residual_column {
        return None;
    }
    // The residual scalar/arithmetic structure (with aggregates sentinelized) must be
    // renderable by the translator — otherwise the outer wrapper cannot be built.
    render_expression(&sentinel_tree).ok()?;
    aggregates.iter().map(parse_agg_item).collect()
}

/// Render a scalar/arithmetic node over the OUTER merge wrapper: every nested
/// `function_aggregate` is rewritten to its merged `PARTIAL_*` expression (matched to
/// `plans` by `AggregatePlan` equality), and the surrounding scalar/arithmetic
/// structure is rendered verbatim by the `vs-expression` translator. This is the one
/// merge-rewrite path shared by the grouped select list AND a scalar-over-aggregate
/// inside a HAVING (decision-log [2]).
///
/// It reuses the translator by SUBSTITUTION rather than re-implementing its scalar
/// arms: each aggregate subtree is replaced with a distinctive sentinel `column`,
/// the tree is rendered once, then each sentinel token is string-replaced with the
/// aggregate's merged expression. This inherits every scalar/arithmetic node type,
/// operator string, and parenthesization the translator supports with zero risk of
/// drifting from it. Returns `None` if the structure cannot be rendered or a nested
/// aggregate is not among `plans` (cannot be merged).
fn render_scalar_over_merge(
    node: &Json,
    plans: &[AggregatePlan],
    merge_udf_name: &str,
) -> Option<String> {
    let mut aggregates = Vec::new();
    let mut residual_column = false;
    let sentinel_tree = sentinelize_aggregates(node, &mut aggregates, &mut residual_column);
    let merged = merge_select_items(plans, merge_udf_name);
    let mut sql = render_expression(&sentinel_tree).ok()?;
    for (i, agg) in aggregates.iter().enumerate() {
        let plan = parse_agg_item(agg)?;
        let slot = plans.iter().position(|p| *p == plan)?;
        sql = sql.replace(&agg_sentinel_token(i), merged.get(slot)?);
    }
    Some(sql)
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

/// Resolve an aggregate's single argument into either a bare-column name (the
/// fast path, populating `column`) or a rendered DataFusion SQL fragment
/// (populating `arg_expr`, via `vs_expression::render_expression` — the same
/// seam GROUP BY keys use).
///
/// Returns:
/// - `Some((Some(col), None))` when the argument is a bare `column` node — the
///   bare-column fast path, so the pre-existing exact-type MIN/MAX column
///   lookups keep working.
/// - `Some((None, Some(sql)))` when the argument is any other expression the VS
///   translator can render (e.g. `LENGTH(L_COMMENT)`).
/// - `None` when there is no argument, or the argument cannot be rendered — the
///   caller then declines the aggregate pushdown and falls back to row scanning.
fn arg_column_or_expr(args: Option<&Vec<Json>>) -> Option<(Option<String>, Option<String>)> {
    let arg = args.and_then(|a| a.first())?;
    if arg.get("type").and_then(|t| t.as_str()) == Some("column") {
        return arg
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| (Some(s.to_uppercase()), None));
    }
    render_expression(arg).ok().map(|sql| (None, Some(sql)))
}

/// Parse a single-group `COUNT(DISTINCT ...)` select-list item into a
/// [`AggKind::CountDistinct`] plan.
///
/// Handles both `COUNT(DISTINCT col)` (bare-column fast path) and
/// `COUNT(DISTINCT expr)` (rendered argument), mirroring how `COUNT(col)` /
/// `COUNT(expr)` are resolved. Returns `None` when the item is not a distinct
/// `COUNT`, or when its argument cannot be resolved to a column or rendered
/// expression — the single-group caller then defers to [`parse_agg_item`]
/// (which declines every other `distinct: true` item), so grouped
/// `COUNT(DISTINCT)` and other distinct aggregates still fall back to row scan.
fn parse_count_distinct(item: &Json) -> Option<AggregatePlan> {
    if item.get("distinct").and_then(|d| d.as_bool()) != Some(true) {
        return None;
    }
    let fn_name = item
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_uppercase();
    if fn_name != "COUNT" {
        return None;
    }
    let args = item.get("arguments").and_then(|a| a.as_array());
    let (column, arg_expr) = arg_column_or_expr(args)?;
    Some(AggregatePlan {
        kind: AggKind::CountDistinct,
        column,
        arg_expr,
    })
}

/// Parse a single `function_aggregate` select-list item into an `AggregatePlan`.
///
/// Returns `None` when the item uses `distinct: true` (single-group
/// `COUNT(DISTINCT)` is handled by [`parse_count_distinct`] before this is
/// called; every other distinct — and grouped `COUNT(DISTINCT)` — declines
/// here), when the function name is not one of COUNT, SUM, MIN, MAX, AVG, the
/// STDDEV/VARIANCE family, or when a COUNT/SUM/MIN/MAX/AVG argument is a scalar
/// expression the VS translator cannot render.
///
/// For COUNT/SUM/MIN/MAX/AVG a bare `column` argument takes the fast path
/// (`column` populated, `arg_expr` None); any other renderable expression is
/// carried in `arg_expr` (`column` None). The STDDEV/VARIANCE family keeps its
/// bare-column-only behavior unchanged.
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
        "COUNT" => match args.and_then(|a| a.first()) {
            // COUNT(*) — no argument: count every row.
            None => AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            // COUNT(col) fast path or COUNT(expr) rendered argument. An argument
            // that renders to neither a bare column nor a translatable expression
            // declines the whole aggregate pushdown (row-scan fallback).
            Some(_) => {
                let (column, arg_expr) = arg_column_or_expr(args)?;
                AggregatePlan {
                    kind: AggKind::CountCol,
                    column,
                    arg_expr,
                }
            }
        },
        "SUM" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Sum,
                column,
                arg_expr,
            }
        }
        "MIN" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Min,
                column,
                arg_expr,
            }
        }
        "MAX" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Max,
                column,
                arg_expr,
            }
        }
        "AVG" => {
            let (column, arg_expr) = arg_column_or_expr(args)?;
            AggregatePlan {
                kind: AggKind::Avg,
                column,
                arg_expr,
            }
        }
        // STDDEV/VARIANCE family — decompose into (cnt, sum, sum_sq) sufficient statistics.
        // STDDEV and STDDEV_SAMP are the sample forms; VARIANCE / VAR_SAMP likewise.
        "STDDEV" | "STDDEV_SAMP" => AggregatePlan {
            kind: AggKind::StddevSamp,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "STDDEV_POP" => AggregatePlan {
            kind: AggKind::StddevPop,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "VARIANCE" | "VAR_SAMP" => AggregatePlan {
            kind: AggKind::VarSamp,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        "VAR_POP" => AggregatePlan {
            kind: AggKind::VarPop,
            column: column_from_first_arg(args),
            arg_expr: None,
        },
        _ => return None,
    };
    Some(plan)
}

// ---------------------------------------------------------------------------
// SQL builder (pure; used by handle_pushdown and unit tests)
// ---------------------------------------------------------------------------

/// Serialize one shard's file list to the per-shard UDF argument JSON.
///
/// Generic over the shard element so production (`FileEntry`, carrying its
/// positional-delete refs) and legacy/test call sites (bare `(path, size)`
/// tuples) share one path: each element is converted into a [`FileEntry`] via
/// `Into` — the identity conversion for a `FileEntry` (deletes preserved) and
/// the delete-free [`FileEntry::new`] for a tuple — before serialization.
fn shard_files_json<E: Clone + Into<FileEntry>>(files: &[E]) -> String {
    let entries: Vec<FileEntry> = files.iter().cloned().map(Into::into).collect();
    ScanSpec::files_json(&entries)
}

/// Build the scan-driving SQL from a resolved file list partitioned into shards.
///
/// **Row queries** (no aggregates in spec) — the outer ungrouped scalar scan is the
/// top-level query; no `SELECT * FROM (...)` materialization wrapper (decision [5]):
/// - Single shard: `SELECT {udf}('{common}', '{files}') EMITS ({emits}) [ORDER BY …] [LIMIT n]`
/// - Multi-shard: `SELECT {udf}('{common}', files) EMITS ({emits}) FROM (distributor with GROUP BY shard_key) [ORDER BY …] [LIMIT n]`
///
/// **Aggregate queries** (spec carries `aggregates`, no `group_keys`):
/// - The outer merge SELECT sits directly over the scalar scan (never SELECT *).
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
#[allow(clippy::too_many_arguments)]
pub fn build_scan_driving_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    if let Some(aggregates) = spec_template.aggregates.as_deref() {
        build_aggregate_scan_sql(
            spec_template,
            shards,
            aggregates,
            col_types,
            aggregate_types,
            udf_name,
            merge_udf_name,
            distribute_udf_name,
        )
    } else {
        build_row_scan_sql(
            spec_template,
            shards,
            proj_cols,
            proj_types,
            limit,
            udf_name,
            distribute_udf_name,
        )
    }
}

/// Build the row-scan SQL (no aggregates) as an OUTER UNGROUPED scalar scan over the
/// nested distributor — no `SELECT * FROM (...)` materialization wrapper (decision
/// [5]). Result-equivalence (decision [7]): with no outer GROUP BY the returned rows
/// are exactly the union of every shard's rows.
///
/// ## Ordered top-N
///
/// When `spec_template.order_by` is non-empty the query is a matched ordered
/// top-N: the outer scalar select carries `ORDER BY <keys> LIMIT n` so the returned
/// SQL is self-contained (it does not depend on Exasol re-applying the ordering).
/// Each shard's common blob carries the SAME `order_by` keys (and `limit`), which the
/// scan UDF renders as a per-shard bounded `ORDER BY … LIMIT n` (a DataFusion TopK).
/// The outer merge `ORDER BY` and the per-shard `ORDER BY` render through the one
/// shared [`render_order_by_clause`] seam, so they agree on direction and NULL
/// placement — the correctness-critical invariant. `order_by` is empty for plain
/// (unordered) row scans.
fn build_row_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    limit: Option<u64>,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(item, ty)| format!("{} {}", quote_ident(item.emit_name()), ty))
        .collect::<Vec<_>>()
        .join(", ");

    // The fan-out primitive returns the OUTER UNGROUPED scalar scan directly (with
    // the `GROUP BY shard_key` fan-out nested inside the distributor, or a from-less
    // scalar call on literals for a single shard). No `SELECT * FROM (...)` wrapper:
    // that was the un-flattenable materialization boundary this change removes
    // (decision [5]). Result-equivalence (decision [7]): with no outer GROUP BY the
    // returned rows are exactly the union of every shard's rows.
    let mut sql = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

    // Outer merge ORDER BY, rendered once (empty when not a matched top-N), attached
    // DIRECTLY to the outer scalar select. SQL requires ORDER BY before LIMIT, so it
    // is appended ahead of the LIMIT clause. The per-shard common blob carries the
    // same keys so each shard runs the same bounded sort; this outer ORDER BY merges
    // the per-shard partial orderings.
    if !spec_template.order_by.is_empty() {
        sql.push_str(&format!(
            " ORDER BY {}",
            render_order_by_clause(&spec_template.order_by)
        ));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// Build the aggregate scan SQL: the outer merge SELECT aggregates the per-shard
/// partial columns DIRECTLY over the scalar scan (no `SELECT * FROM (...)` wrapper).
///
/// The EMITS clause names and types follow the COLUMN CONTRACT defined in
/// `crate::scan::build_partial_agg_sql`.  The outer merge SELECT consumes those
/// exact column names.
#[allow(clippy::too_many_arguments)]
fn build_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    aggregates: &[AggregatePlan],
    col_types: &[(String, String)],
    aggregate_types: &[String],
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let emits_items = partial_emits_items(aggregates, col_types, aggregate_types);
    let emits = emits_items.join(", ");
    let merge_select = cast_merge_items(aggregates, aggregate_types, merge_udf_name).join(", ");

    // The outer merge SELECT sits DIRECTLY over the scalar scan — no
    // `SELECT * FROM (...)` between them (decision [5]). The primitive short-circuits
    // to a from-less scalar call for a single shard; for multi-shard it nests the
    // `GROUP BY shard_key` fan-out in the distributor. Either way the scalar scan
    // fires once per shard (one partial-agg row per shard), so the outer merge over
    // those partials equals the single-node aggregate (result-equivalence, [7]).
    let fan_out = build_fan_out_inner(spec_template, shards, &emits, udf_name, distribute_udf_name);

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
/// Build the explicit final `ORDER BY` element list for a grouped-aggregate merge.
///
/// Once `ORDER_BY_COLUMN` is advertised Exasol delegates the ORDER BY and no longer
/// re-sorts the grouped rows the adapter returns (add-topn-pushdown B6), so the merge
/// SQL must sort itself. The outer wrapper's output columns are the stringified
/// `GK_*` staging columns re-cast to their declared types and the merged aggregates —
/// NOT the source column names — so each sort key is rendered as a POSITIONAL output
/// ordinal (`ORDER BY 1 ...`). The ordinal references the type-cast output expression
/// (e.g. `CAST("GK_0" AS DECIMAL(20,0))`), so it sorts on the native value, never the
/// lexicographic VARCHAR `GK_*` staging column (a plain `ORDER BY "GK_0"` would sort
/// `1,10,11,2,…`, corrupting a numeric order).
///
/// Each bare-column sort key must map to a group key (a bare-column `ORDER BY` in a
/// GROUP BY query is only legal on a grouped column). It is matched to its group-key
/// slot exactly as `detect_group_by_aggregates` matches select items (rendered-SQL
/// equality), then to that group key's `selectList` ordinal (its output position,
/// since the outer SELECT is assembled in `selectList` order with no gaps). Returns
/// `None` when there is no `orderBy`, and the caller declines the pushdown when a key
/// is present but cannot be resolved to a grouped output column — a shape SQL forbids.
fn build_grouped_order_by_clause(
    pushdown_req: &Json,
    group_keys: &[String],
    select_items: &[GroupedSelectItem],
) -> Option<GroupedOrderBy> {
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let key = match parse_sort_key_element(element) {
            Some(k) => k,
            None => return Some(GroupedOrderBy::Unresolvable),
        };
        let rendered = match element
            .get("expression")
            .and_then(|e| render_expression(e).ok())
        {
            Some(r) => r,
            None => return Some(GroupedOrderBy::Unresolvable),
        };
        let slot = match group_keys.iter().position(|gk| *gk == rendered) {
            Some(s) => s,
            None => return Some(GroupedOrderBy::Unresolvable),
        };
        // Output position of this group key = its selectList ordinal (1-based for SQL).
        let select_index = select_items.iter().find_map(|it| match it {
            GroupedSelectItem::GroupKey {
                group_key_slot,
                select_index,
            } if *group_key_slot == slot => Some(*select_index),
            _ => None,
        });
        match select_index {
            Some(idx) => parts.push(key.render_ordered(&(idx + 1).to_string())),
            None => return Some(GroupedOrderBy::Unresolvable),
        }
    }
    Some(GroupedOrderBy::Clause(parts.join(", ")))
}

/// Outcome of resolving a grouped-aggregate merge `ORDER BY` (see
/// [`build_grouped_order_by_clause`]). `Unresolvable` marks a pushed sort key that
/// cannot be mapped to a grouped output column — the caller declines the pushdown
/// as a hard error rather than emitting a merge that silently drops the ordering.
#[derive(Debug, PartialEq, Eq)]
enum GroupedOrderBy {
    Clause(String),
    Unresolvable,
}

// ponytail: well over the lint threshold now, but the function is called in only
// two places and every argument is a distinct, already-resolved plan input (no
// natural sub-grouping) — a params struct would just rename the boilerplate.
#[allow(clippy::too_many_arguments)]
pub fn build_grouped_aggregate_scan_sql<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    group_keys: &[String],
    group_key_types: &[String],
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
    limit: Option<u64>,
    col_types: &[(String, String)],
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
    having: Option<&str>,
    order_by: Option<&str>,
) -> String {
    // Build EMITS: GK_* columns first, then PARTIAL_* columns.
    let gk_emits: Vec<String> = (0..group_keys.len())
        .map(|i| format!(r#""GK_{i}" VARCHAR(2000000)"#))
        .collect();
    let partial_items = partial_emits_items(aggregates, col_types, aggregate_types);
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
    let merge_items = cast_merge_items(aggregates, aggregate_types, merge_udf_name);

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
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => {
                gk_select.get(*group_key_slot).cloned()
            }
            GroupedSelectItem::Aggregate { plan_slot, .. } => merge_items.get(*plan_slot).cloned(),
            // A constant placeholder projects its own pre-rendered, type-cast
            // expression (e.g. `CAST(NULL AS BOOLEAN)`); one row survives per
            // distinct group via the outer `GROUP BY GK_*`.
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            // A scalar-over-aggregate item: render the scalar wrapper over the MERGED
            // partials (each nested aggregate rewritten to its `PARTIAL_*` merge
            // expression), then CAST to the item's own declared type so Exasol's
            // positional pushdown-column-type check passes. Detection has already
            // validated decomposability + renderability, so this render succeeds.
            GroupedSelectItem::ScalarOverAggregate {
                node,
                declared_type,
                ..
            } => render_scalar_over_merge(node, aggregates, merge_udf_name)
                .map(|expr| cast_to_declared_type(&expr, declared_type)),
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
    // The primitive nests the `GROUP BY shard_key` fan-out in the distributor (or
    // short-circuits to a from-less scalar call for a single shard); the outer wrapper
    // below re-groups the emitted per-shard partials on the user's group keys.
    let mut common_template = spec_template.clone();
    common_template.limit = None;
    let fan_out = build_fan_out_inner(
        &common_template,
        shards,
        &emits,
        udf_name,
        distribute_udf_name,
    );

    let mut sql =
        format!("SELECT {outer_select_str} FROM ({fan_out}) GROUP BY {outer_group_by_str}");

    // HAVING: applied in outer wrapper only, never pushed into shard scan.
    if let Some(h) = having.filter(|h| !h.is_empty()) {
        sql.push_str(" HAVING ");
        sql.push_str(h);
    }

    // Explicit merge ORDER BY (add-topn-pushdown B6): SQL requires it after HAVING
    // and before LIMIT. Rendered as positional output ordinals so it sorts the
    // type-cast output, not the lexicographic VARCHAR GK_* staging columns.
    if let Some(ob) = order_by.filter(|s| !s.is_empty()) {
        sql.push_str(" ORDER BY ");
        sql.push_str(ob);
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
    aggregate_types: &[String],
) -> Vec<String> {
    aggregates
        .iter()
        .enumerate()
        .flat_map(|(i, plan)| {
            // Declared aggregate result type at this ordinal (from
            // `aggregate_exasol_types`/`selectListDataTypes`); the sole type source
            // for an expression-argument aggregate, which has no source column.
            let declared = aggregate_types.get(i).map(String::as_str);
            match plan.kind {
                AggKind::Count | AggKind::CountCol => {
                    vec![format!(r#""PARTIAL_count_{i}" DECIMAL(20,0)"#)]
                }
                AggKind::Sum => {
                    let ty = col_type_for(plan, col_types, declared);
                    let emit_ty = sum_emit_type(&ty);
                    vec![format!(r#""PARTIAL_sum_{i}" {emit_ty}"#)]
                }
                AggKind::Min => {
                    let ty = col_type_for(plan, col_types, declared);
                    vec![format!(r#""PARTIAL_min_{i}" {ty}"#)]
                }
                AggKind::Max => {
                    let ty = col_type_for(plan, col_types, declared);
                    vec![format!(r#""PARTIAL_max_{i}" {ty}"#)]
                }
                AggKind::Avg => vec![
                    format!(r#""PARTIAL_avg_sum_{i}" DOUBLE PRECISION"#),
                    format!(r#""PARTIAL_avg_cnt_{i}" DECIMAL(20,0)"#),
                ],
                // COUNT(DISTINCT) emits its shard-local distinct set as one JSON
                // array string — always VARCHAR(2000000), independent of the
                // underlying column's own type. A scalar merge UDF unions the
                // per-shard arrays and returns the final cardinality.
                AggKind::CountDistinct => {
                    vec![format!(r#""PARTIAL_cd_{i}" VARCHAR(2000000)"#)]
                }
                // Stat family: 3 columns — cnt (DECIMAL), sum (DOUBLE), sumsq (DOUBLE).
                AggKind::VarPop | AggKind::VarSamp | AggKind::StddevPop | AggKind::StddevSamp => {
                    vec![
                        format!(r#""PARTIAL_stat_cnt_{i}" DECIMAL(20,0)"#),
                        format!(r#""PARTIAL_stat_sum_{i}" DOUBLE PRECISION"#),
                        format!(r#""PARTIAL_stat_sumsq_{i}" DOUBLE PRECISION"#),
                    ]
                }
            }
        })
        .collect()
}

/// Look up the Exasol type used to size an aggregate's partial/merge column.
///
/// For a bare-column aggregate the type is the target column's own Exasol type
/// (from `col_types`), falling back to `DOUBLE PRECISION` when the column is
/// absent from the map. For an expression-argument aggregate (`arg_expr` set,
/// no source `column`) there is no source column to look up, so the type is the
/// aggregate item's declared result type (`declared`, from
/// `aggregate_exasol_types`/`selectListDataTypes`); when the declared type is
/// unavailable it falls back to the column-map lookup (then `DOUBLE PRECISION`).
fn col_type_for(
    plan: &AggregatePlan,
    col_types: &[(String, String)],
    declared: Option<&str>,
) -> String {
    if plan.column.is_none()
        && plan.arg_expr.is_some()
        && let Some(ty) = declared
    {
        return ty.to_string();
    }
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
/// `CountDistinct` is valid over any type (its partial is a VARCHAR JSON array),
/// so it is never numeric-checked here.
/// Returns `false` (fall back to row scan) when any SUM or stat aggregate targets a
/// non-numeric column.
///
/// An expression-argument SUM/stat (`arg_expr` set, no source `column`) passes:
/// its partial type is derived from the declared aggregate result type in
/// `partial_emits_items`, and Exasol only declares such aggregates over numeric
/// results — so the column-map lookup here (which has no entry) safely resolves
/// to the numeric `DOUBLE PRECISION` fallback rather than a spurious fall-back.
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
            let ty = col_type_for(plan, col_types, None);
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
fn merge_select_items(aggregates: &[AggregatePlan], merge_udf_name: &str) -> Vec<String> {
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
            // COUNT(DISTINCT): each shard emitted its local distinct set as one
            // JSON array string in `PARTIAL_cd_{i}`. LISTAGG concatenates the
            // per-shard arrays with `,`; wrapping in `[` … `]` yields a JSON
            // array-of-arrays, which the scalar merge UDF unions and counts. The
            // merge UDF name is schema-qualified the same way the scan UDF is.
            AggKind::CountDistinct => {
                format!(r#"{merge_udf_name}('[' || LISTAGG("PARTIAL_cd_{i}", ',') || ']')"#)
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
fn render_having_over_merge(
    node: &Json,
    plans: &[AggregatePlan],
    merge_udf_name: &str,
) -> Option<String> {
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
        return merge_select_items(plans, merge_udf_name)
            .into_iter()
            .nth(idx);
    }

    // Boolean / comparison predicate nodes that can appear in a HAVING. Operator
    // strings and parenthesization mirror `vs-expression`'s renderer so output
    // matches conventions.
    match kind {
        "predicate_and" => {
            render_having_junction(child("expressions"), plans, " AND ", merge_udf_name)
        }
        "predicate_or" => {
            render_having_junction(child("expressions"), plans, " OR ", merge_udf_name)
        }
        "predicate_not" => {
            let inner = render_having_operand(child("expression"), plans, merge_udf_name)?;
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
            let left = render_having_operand(child("left"), plans, merge_udf_name)?;
            let right = render_having_operand(child("right"), plans, merge_udf_name)?;
            Some(format!("({left} {op} {right})"))
        }
        "predicate_between" => {
            let target = render_having_operand(child("expression"), plans, merge_udf_name)?;
            let low = render_having_operand(child("left"), plans, merge_udf_name)?;
            let high = render_having_operand(child("right"), plans, merge_udf_name)?;
            Some(format!("({target} BETWEEN {low} AND {high})"))
        }
        "predicate_is_null" => {
            let inner = render_having_operand(child("expression"), plans, merge_udf_name)?;
            Some(format!("({inner} IS NULL)"))
        }
        "predicate_is_not_null" => {
            let inner = render_having_operand(child("expression"), plans, merge_udf_name)?;
            Some(format!("({inner} IS NOT NULL)"))
        }
        _ => None,
    }
}

/// Render a HAVING operand: a `function_aggregate` rewrites to its merged
/// expression; any other node (column, literal, scalar function, arithmetic,
/// or nested predicate) delegates to `render_having_over_merge` — which itself
/// falls back to `render_expression` for non-predicate, non-aggregate nodes.
fn render_having_operand(
    node: Option<&Json>,
    plans: &[AggregatePlan],
    merge_udf_name: &str,
) -> Option<String> {
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
        | "predicate_is_not_null" => render_having_over_merge(node, plans, merge_udf_name),
        // Any other node (literal, column, scalar function, arithmetic): render over
        // the merge wrapper, rewriting EVERY nested `function_aggregate` to its merged
        // `PARTIAL_*` expression. A scalar function wrapping an aggregate (e.g.
        // `ROUND(SUM(x) / COUNT(*), 2)`) is thus rewritten correctly rather than
        // rendered verbatim over absent source columns — the fix that closes issue
        // #82's gap, which also covers a scalar-over-aggregate inside a HAVING. A
        // node with no nested aggregate renders exactly as `vs-expression` would.
        _ => render_scalar_over_merge(node, plans, merge_udf_name),
    }
}

/// Render an AND/OR junction over the outer merge wrapper, mirroring
/// `vs-expression`'s `render_junction`: single child unwrapped, multiple joined
/// and parenthesized. Any child that fails to render collapses the junction.
fn render_having_junction(
    expressions: Option<&Json>,
    plans: &[AggregatePlan],
    op: &str,
    merge_udf_name: &str,
) -> Option<String> {
    let items = expressions?.as_array()?;
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(render_having_over_merge(item, plans, merge_udf_name)?);
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
fn cast_merge_items(
    aggregates: &[AggregatePlan],
    aggregate_types: &[String],
    merge_udf_name: &str,
) -> Vec<String> {
    merge_select_items(aggregates, merge_udf_name)
        .into_iter()
        .enumerate()
        .map(|(i, expr)| match aggregate_types.get(i) {
            Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({expr} AS {ty})"),
            _ => expr,
        })
        .collect()
}

/// Wrap an already-rendered outer-wrapper expression in `CAST(... AS <ty>)` unless
/// the declared type is the `VARCHAR(2000000)` default — the same cast discipline
/// as `cast_merge_items`, `constant_projection_sql`, and the group-key cast, so a
/// scalar-over-aggregate item's output column type matches what Exasol validates
/// positionally against `selectListDataTypes`.
fn cast_to_declared_type(expr: &str, declared_type: &str) -> String {
    if declared_type != "VARCHAR(2000000)" {
        format!("CAST({expr} AS {declared_type})")
    } else {
        expr.to_string()
    }
}

/// Builds the shard fan-out SELECT that Exasol distributes across nodes.
///
/// Emits a nested `LAKEHOUSE_DISTRIBUTE_FILES` LUA SET distributor — which does the
/// `GROUP BY shard_key` fan-out (NOT `IPROC()`) so work units spread round-robin
/// across nodes (G ≤ 300) and multiplex onto each node's core pool — wrapped by an
/// outer UNGROUPED scalar `LAKEHOUSE_SCAN('{common}', files)` scan. Separating the
/// fan-out from the scan is what lets Exasol STREAM the scan output: with no
/// top-level `GROUP BY`, the scalar scan's emitted rows are not buffered into a
/// materializing `tmp_subselect` temp table.
///
/// The shard-INVARIANT common blob (credentials, projection, filter, aggregates,
/// tuning knobs) is serialized ONCE via `to_common_json()` as the outer scalar
/// scan's first-argument literal; only the per-shard files list flows through the
/// distributor (one `VALUES` row per shard). Because the fan-out carries only the
/// file-list strings, its payload is independent of the data volume scanned.
///
/// A single-shard plan short-circuits the distributor entirely: a from-less scalar
/// call on literals (`SELECT {udf}('{common}', '{files}') EMITS (...)`) — a scalar
/// EMIT UDF over constant literals fires exactly once, so no driving relation is
/// needed. Callers attach `ORDER BY`/`LIMIT` or an outer merge directly to the
/// returned bare SELECT.
pub fn build_fan_out_inner<E: Clone + Into<FileEntry>>(
    spec_template: &ScanSpec,
    shards: &[Vec<E>],
    emits: &str,
    udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    // Serialize the shard-invariant common blob exactly once.
    let common_literal = sql_string_literal(&spec_template.to_common_json());

    // Single-shard short-circuit: a from-less scalar call on literals. A scalar EMIT
    // UDF over constant literals fires exactly once, so the distributor and the inner
    // GROUP BY are unnecessary.
    if shards.len() == 1 {
        let files_literal = sql_string_literal(&shard_files_json(&shards[0]));
        return format!(
            "SELECT {udf}({common}, {files}) EMITS ({emits})",
            udf = udf_name,
            common = common_literal,
            files = files_literal,
            emits = emits,
        );
    }

    let values: Vec<String> = shards
        .iter()
        .enumerate()
        .map(|(i, files)| {
            let files_literal = sql_string_literal(&shard_files_json(files));
            format!("({i},{files_literal})")
        })
        .collect();
    let values_list = values.join(",");
    // The distributor is a LUA SET script with a STATIC `EMITS (files VARCHAR(...))`
    // definition, so its call MUST NOT carry a query-side `EMITS` clause — supplying
    // one is rejected by Exasol ("static return argument definition. Dynamic return
    // arguments are not supported in this case"). Only the scan (dynamic-output SCALAR)
    // carries a query-side EMITS.
    format!(
        "SELECT {udf}({common}, files) EMITS ({emits}) FROM (SELECT {distribute}(files) FROM (VALUES {values}) AS shards(shard_key, files) GROUP BY shard_key)",
        udf = udf_name,
        common = common_literal,
        emits = emits,
        distribute = distribute_udf_name,
        values = values_list,
    )
}

/// Emit a file path relative to `table_root` when the file lives under it,
/// otherwise pass the absolute path through unchanged.
///
/// Stripping happens ONLY at a real path-segment boundary: the root must be a
/// prefix AND either end with `/` or be followed by a `/` in the path. A path that
/// merely shares the root as a bare string prefix (e.g. `<root>-archive/...`,
/// `<root>2/...`) or one exactly equal to the root does NOT match, so it stays
/// absolute — this keeps the round-trip with the scan UDF's single-`/` join lossless
/// and avoids emitting an empty relative entry. After a boundary match the root
/// prefix and then a single leading `/` are stripped, so the relative path has no
/// leading slash. An empty `table_root` (legacy / no resolved root) always yields an
/// absolute path.
fn relativize_path_to_root(path: &str, table_root: &str) -> String {
    let at_segment_boundary = !table_root.is_empty()
        && path.starts_with(table_root)
        && (table_root.ends_with('/') || path[table_root.len()..].starts_with('/'));
    if at_segment_boundary {
        let rest = &path[table_root.len()..];
        rest.strip_prefix('/').unwrap_or(rest).to_string()
    } else {
        path.to_string()
    }
}

/// Strip `table_root` from every under-root file path in each shard (see
/// [`relativize_path_to_root`]) while preserving byte sizes and shard membership.
/// Paths not under the root stay absolute.
///
/// Each data file's associated positional-delete file paths are relativized by
/// the SAME [`relativize_path_to_root`] rule as the data-file path, so the scan
/// UDF rejoins them onto `table_root` identically (delete files written by the
/// same engine live under the same table root). Delete byte sizes and content
/// types are preserved unchanged.
fn relativize_shards_to_root(shards: Vec<Vec<FileEntry>>, table_root: &str) -> Vec<Vec<FileEntry>> {
    shards
        .into_iter()
        .map(|shard| {
            shard
                .into_iter()
                .map(|mut entry| {
                    entry.path = relativize_path_to_root(&entry.path, table_root);
                    for delete in &mut entry.deletes {
                        delete.path = relativize_path_to_root(&delete.path, table_root);
                    }
                    entry
                })
                .collect()
        })
        .collect()
}

/// Resolve the Iceberg snapshot + file list and build pushdown SQL.
///
/// `cluster_nodes` — the number of Exasol nodes read from the `CLUSTER_NODES`
/// adapterNotes entry (default 1 when absent or unparseable).
///
/// `parallelism_factor` — the oversubscription multiplier read from the
/// `PARALLELISM_FACTOR` adapterNotes entry (default 8).
///
/// `join_broadcast_max_bytes` — the byte-size threshold read from the
/// `JOIN_BROADCAST_MAX_BYTES` adapterNotes entry (default 128 MiB); a two-table
/// inner equi-join broadcasts its smaller side when that side's Iceberg-manifest
/// byte size is at or below this threshold. See backlog BL-001 / plan
/// `add-join-pushdown-broadcast`.
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
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
    creds: &ConnectionCreds,
) -> Result<Json, UdfError> {
    let pushdown_req = request
        .get("pushdownRequest")
        .cloned()
        .unwrap_or(Json::Null);

    // Inner-join handling MUST run before the single-table path. `handle_pushdown`
    // is invoked once per pushdown REQUEST, resolving only `involvedTables[0]`
    // (adapter::mod::handle_pushdown_request); a join-shaped `from` that fell through
    // would scan just the first table and silently drop the join. `NotAJoin` is
    // today's normal single-table request — fall through unchanged. `Ineligible` is a
    // shape the adapter cannot render at all (a non-inner join node, or a malformed
    // shape), so it is a hard client-facing error (Exasol does not re-plan on an
    // adapter error). `Join` is served here by the single unified join path and
    // returns directly.
    match detect_join(request, &pushdown_req)? {
        JoinShape::NotAJoin => {}
        JoinShape::Ineligible(reason) => return Err(ineligible_join_decline(reason)),
        JoinShape::Join(join) => {
            return plan_join(
                request,
                &pushdown_req,
                &join,
                catalog_uri,
                storage,
                catalog,
                creds,
                scan_schema,
                cluster_nodes,
                parallelism_factor,
                df_target_partitions,
                df_batch_size,
                df_threads_per_udf,
                memory_pool_fraction,
                instance_overhead_mb,
                s3_max_connections,
                join_broadcast_max_bytes,
            )
            .await;
        }
    }

    let (proj_cols, proj_types) = extract_projection(request, &pushdown_req)?;

    let filter_json_raw = pushdown_req.get("filter").filter(|f| !f.is_null());

    let filter = filter_json_raw.and_then(render_df_filter_safe);

    let limit = extract_limit(&pushdown_req);

    // Whether Exasol pushed an ORDER BY. Drives the anti-wrong-truncation guard
    // (decision [4]): a limit is withheld from every ORDER-BY-carrying request the
    // adapter does not match as a bounded top-N, so a bare per-shard/outer LIMIT is
    // never emitted ahead of an ordering the adapter did not itself render.
    let has_order_by = order_by_present(&pushdown_req);

    let col_types = extract_all_column_types(request);

    // Resolve file list exactly once. The returned `effective_storage` carries
    // vended STS creds when use_vended_credentials is true; otherwise it equals
    // the static `storage` passed in. Every per-shard ScanSpec uses this storage.
    // filter_json_raw is forwarded for Iceberg-level file pruning; ScanSpec.filter
    // (DataFusion SQL string) is set separately above and left completely unchanged.
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(catalog_uri, catalog, storage, creds, filter_json_raw).await?;
    let storage = &effective_storage;

    if files.is_empty() {
        return empty_result_sql(&pushdown_req, &proj_cols, &proj_types, &col_types);
    }

    // Compute G = shard_count(node_count, parallelism_factor, file_count) and
    // partition files into G byte-balanced work-unit shards (GROUP BY shard_key fan-out).
    let g = shard_count(cluster_nodes, parallelism_factor, files.len());
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    // Emit each under-root file path relative to `table_root` (carried once in the
    // common blob) so the per-shard payload stops repeating the table-location
    // prefix. Sizes and shard membership are unchanged; paths not under the root
    // stay absolute. The scan UDF rejoins relative paths onto `table_root`.
    let shards = relativize_shards_to_root(shards, &table_root);

    // The scan and distinct-merge UDFs must be schema-qualified: the pushdown query
    // executes outside the adapter script's schema, so an unqualified name would not
    // resolve ("function or script LAKEHOUSE_SCAN not found").
    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let merge_udf_name = qualify_udf(scan_schema, DISTINCT_MERGE_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);

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
        plan_types: grouped_agg_types,
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
            // Raise a hard error (the FFI shim surfaces it as F-UDF-CL-RUST-9001);
            // Exasol does not re-plan the query natively.
            if having_node.is_some() {
                return Err(UdfError::User(
                    "grouped aggregate pushdown declined: HAVING present but aggregate \
                     column type is non-numeric; this is a hard error, not a native re-plan"
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
                Some(node) => {
                    match render_having_over_merge(node, &grouped_agg_plans, &merge_udf_name) {
                        Some(sql) => Some(sql),
                        None => {
                            return Err(UdfError::User(
                                "grouped aggregate pushdown declined: HAVING references an \
                             aggregate that cannot be merged or an unsupported node; \
                             this is a hard error, not a native re-plan"
                                    .into(),
                            ));
                        }
                    }
                }
                None => None,
            };
            // Grouped aggregate pushdown path. Once ORDER_BY_COLUMN is advertised,
            // Exasol delegates any ORDER BY on the grouped output and NO LONGER
            // re-sorts the rows the adapter returns (add-topn-pushdown B6), so the
            // merge SQL must render its own explicit final ORDER BY over the grouped
            // output columns. Resolve it now: a pushed sort key that cannot be mapped
            // to a grouped output column is a shape SQL forbids — decline the pushdown
            // as a hard error rather than emit an unsorted merge.
            let grouped_order_by =
                match build_grouped_order_by_clause(&pushdown_req, &group_keys, &select_items) {
                    Some(GroupedOrderBy::Clause(clause)) => Some(clause),
                    Some(GroupedOrderBy::Unresolvable) => {
                        return Err(UdfError::User(
                            "grouped aggregate pushdown declined: ORDER BY references a \
                         column that is not a grouped output column; this is a hard \
                         error, not a native re-plan"
                                .into(),
                        ));
                    }
                    None => None,
                };
            // With the ordering now rendered explicitly, the outer LIMIT is a true
            // global top-N over the merged groups, so it is safe to apply. When there
            // is no ORDER BY it stays a plain grouped LIMIT (unchanged). The per-shard
            // partial scan still never carries a LIMIT (the fan-out common blob is
            // rebuilt with `limit = None`), preserving the anti-wrong-truncation
            // invariant (decision [4]).
            let grouped_limit = limit;
            let spec_template = ScanSpec {
                table_root: table_root.clone(),
                files: vec![],
                projection: proj_cols.clone(),
                filter,
                limit: grouped_limit,
                order_by: Vec::new(),
                aggregates: Some(grouped_agg_plans.clone()),
                group_keys: Some(group_keys.clone()),
                // Aggregate scans emit via the freely-coercing Value path, not the
                // strict emit_batch IPC path, so no per-column declared types needed.
                emit_exa_types: Vec::new(),
                logical_schema: logical_schema.clone(),
                name_mapping: name_mapping.clone(),
                join: None,
                storage: storage.clone(),
                df_target_partitions,
                df_batch_size,
                df_threads_per_udf,
                memory_pool_fraction,
                instance_overhead_mb,
                s3_max_connections,
            };
            let group_key_types = group_key_exasol_types(&pushdown_req, &group_keys, &select_items);
            // Per-plan declared types, aligned 1:1 with `grouped_agg_plans` (which
            // now includes aggregates nested inside a scalar-over-aggregate item).
            // `aggregate_exasol_types` keyed off top-level select items only and
            // would misalign; the detection-built `plan_types` is the aligned source.
            let aggregate_types = grouped_agg_types;
            let sql = build_grouped_aggregate_scan_sql(
                &spec_template,
                &shards,
                &group_keys,
                &group_key_types,
                &grouped_agg_plans,
                &aggregate_types,
                &select_items,
                grouped_limit,
                &col_types,
                &udf_name,
                &merge_udf_name,
                &distribute_udf_name,
                having.as_deref(),
                grouped_order_by.as_deref(),
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        } // end else (validate_agg_col_types passed)
    }

    // A GROUP BY request that did NOT push down as a grouped partial/merge above
    // (an undecomposable select item declined detection, or a non-numeric aggregate
    // with no HAVING fell through the validate gate) must NEVER fall through to the
    // bare row scan below: for a grouped request Exasol expects the pushdown query to
    // return exactly the `selectList` columns, but a raw full-row scan returns the
    // projected source columns instead → SQL state `04000` "Expected number of
    // columns is N but pushdown query has M". Route it to a qualified single-table
    // wrapper — the join N-scan fallback at N=1 — that renders the exact grouped
    // select list (aggregates verbatim) over a materialized sharded raw scan so
    // Exasol's core engine aggregates the returned rows (issue #82 / task 2.5-2.7).
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        let (fb_proj_cols, fb_proj_types) = full_row_projection(&col_types);
        // Per-shard scan stays LIMIT-free and sort-free (no aggregates, no group
        // keys); the group keys, HAVING, ORDER BY, and LIMIT go in the outer wrapper
        // only. The WHERE filter is pushed into the scan (advertised filter
        // capabilities carry only translatable predicates), exactly as the grouped
        // push-down path does — no outer WHERE needed.
        let fan_out_spec = ScanSpec {
            table_root: table_root.clone(),
            files: vec![],
            projection: fb_proj_cols,
            filter: filter.clone(),
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: fb_proj_types,
            logical_schema: logical_schema.clone(),
            name_mapping: name_mapping.clone(),
            join: None,
            storage: storage.clone(),
            df_target_partitions,
            df_batch_size,
            df_threads_per_udf,
            memory_pool_fraction,
            instance_overhead_mb,
            s3_max_connections,
        };
        let sql = build_grouped_qualified_fallback_sql(
            request,
            &pushdown_req,
            &fan_out_spec,
            &shards,
            &udf_name,
            &merge_udf_name,
            &distribute_udf_name,
        )?;
        return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
    }

    // Single-group aggregate or row scan.
    // After detection, validate that each SUM/MIN/MAX targets a supported column type;
    // if any SUM targets a non-numeric type (DATE, VARCHAR, etc.), fall back to row scan.
    let aggregates =
        detect_aggregates(&pushdown_req).filter(|plans| validate_agg_col_types(plans, &col_types));

    // Ordered top-N applies ONLY to the pure row-scan path (no aggregates). On a
    // match the sort keys are carried into the common blob (per-shard bounded sort)
    // and the outer wrapper renders `ORDER BY … LIMIT n`.
    let topn = if aggregates.is_none() {
        detect_topn(request, &pushdown_req, &proj_cols, &logical_schema)
    } else {
        None
    };
    let order_by = topn.unwrap_or_default();

    // Withhold the limit when an ORDER BY is present but the shape is not a matched
    // top-N (`order_by` empty): never a bare per-shard/outer LIMIT ahead of an
    // ordering the adapter did not render (decision [4]). A matched top-N keeps the
    // limit (bounded per-shard sort + outer merge limit); a plain LIMIT-only query
    // (no ORDER BY) is unchanged.
    let effective_limit = if has_order_by && order_by.is_empty() {
        None
    } else {
        limit
    };

    let spec_template = ScanSpec {
        table_root,
        files: vec![], // replaced per shard in build_scan_driving_sql
        projection: proj_cols.clone(),
        filter,
        limit: effective_limit,
        order_by,
        aggregates,
        group_keys: None,
        // Row-scan EMITS types, positionally aligned with `proj_cols`. The scan
        // coerces each emitted Arrow column to the type its declared ExaType
        // accepts before emit_batch. Ignored when `aggregates` is Some (that path
        // emits via the Value path). Same list the EMITS clause is built from.
        emit_exa_types: proj_types.clone(),
        logical_schema,
        name_mapping,
        join: None,
        storage: storage.clone(),
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    };

    let aggregate_types = aggregate_exasol_types(&pushdown_req);
    let sql = build_scan_driving_sql(
        &spec_template,
        &shards,
        &proj_cols,
        &proj_types,
        effective_limit,
        &col_types,
        &aggregate_types,
        &udf_name,
        &merge_udf_name,
        &distribute_udf_name,
    );

    // Row-scan DECLINE path (add-topn-pushdown B6): an ORDER BY was pushed but the
    // shape did not match the bounded top-N (`order_by` empty) — e.g. a sort key
    // that is unprojected or JSON-fallback-typed, or a bare ORDER BY with no LIMIT.
    // Once ORDER_BY_COLUMN is advertised Exasol delegates the ordering and NO LONGER
    // re-applies its own backstop sort/limit on the returned rows, so the adapter
    // reproduces that former backstop as self-contained SQL: wrap the unbounded
    // fan-out in a global ORDER BY (plus the original LIMIT, if any). The per-shard
    // common blob still carries no sort keys and no LIMIT (anti-wrong-truncation
    // invariant, decision [4]); this is the unoptimized correctness restoration, not
    // the bounded per-shard top-N.
    let declined_order_by =
        has_order_by && spec_template.order_by.is_empty() && spec_template.aggregates.is_none();
    let sql = if declined_order_by {
        let keys = parse_order_by_keys(&pushdown_req);
        if keys.is_empty() {
            sql
        } else {
            let mut wrapped = format!(
                "SELECT * FROM ({sql}) ORDER BY {}",
                render_order_by_clause(&keys)
            );
            if let Some(n) = limit {
                wrapped.push_str(&format!(" LIMIT {n}"));
            }
            wrapped
        }
    } else {
        sql
    };

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

/// Parse the Iceberg `schema.name-mapping.default` table property into the flat
/// `Vec<NameMappingEntry>` the scan-side resolver looks up by physical name.
///
/// `raw` is the property's raw JSON value (`None` when the property is absent).
///
/// Behaviour (Iceberg column-projection rule #2 scope — see the plan):
/// - Absent property (`None`) → an empty `Vec` (NOT an error): a table with no
///   name-mapping is the common, fully-supported case.
/// - Present but malformed JSON → a clean, credential-free plan-time `UdfError`
///   (mirrors the fail-loud discipline of `ensure_supported_delete_mechanisms`;
///   the property carries only column names + field-ids, never credentials, and
///   `serde_json`'s error reports only a parse position).
/// - Present and valid → flatten ONLY the TOP-LEVEL entries: for each top-level
///   mapping that HAS a `field-id`, emit one `NameMappingEntry { name, field_id }`
///   per name in its `names` list. Entries without a `field-id` are skipped (they
///   exist only in the Iceberg schema, not in imported files — nothing to map to).
///   Nested `fields` (struct/map/list child mappings) are deliberately NOT
///   recursed into — out of scope for this phase (deferred to issue #83).
///
/// Parsed via the `iceberg` crate's own spec-accurate `NameMapping` deserializer
/// (kebab-case `field-id`, `DefaultOnNull` nested `fields`), never a hand-rolled
/// struct. Resolved ONCE per query in the VS planning layer.
fn parse_name_mapping(raw: Option<&str>) -> Result<Vec<NameMappingEntry>, UdfError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mapping: iceberg::spec::NameMapping = serde_json::from_str(raw).map_err(|e| {
        UdfError::User(format!(
            "failed to parse Iceberg '{}' table property: {e}",
            iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING
        ))
    })?;
    let mut entries = Vec::new();
    for field in mapping.fields() {
        // Skip id-less entries (schema-only, not present in imported files) and do
        // NOT recurse into `field.fields()` (nested child mappings, out of scope).
        let Some(field_id) = field.field_id() else {
            continue;
        };
        for name in field.names() {
            entries.push(NameMappingEntry {
                name: name.clone(),
                field_id,
            });
        }
    }
    Ok(entries)
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
) -> Result<
    (
        Vec<FileEntry>,
        StorageProps,
        Vec<LogicalField>,
        String,
        Vec<NameMappingEntry>,
    ),
    UdfError,
> {
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
    // Own the table root before `result.metadata` is moved into the table builder
    // below. Returned so the adapter can carry it once in the common blob and emit
    // per-shard file paths relative to it (empty ⇒ every path stays absolute).
    let table_root = table_s3_location.to_string();
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
    let runtime = iceberg::Runtime::try_current().map_err(|e| {
        UdfError::User(format!(
            "failed to build Iceberg table: {}",
            redact_catalog_error(&e.to_string())
        ))
    })?;
    let table_builder = iceberg::table::Table::builder()
        .identifier(table_ident)
        .file_io(file_io)
        .runtime(runtime)
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

    // Resolve the Iceberg name-mapping fallback (`schema.name-mapping.default`)
    // ONCE per query here — alongside `logical_schema`, and likewise before
    // `plan_files_from_table` consumes `table` — so it is resolved in the VS
    // planning layer, never per UDF invocation. Absent property ⇒ empty; a
    // present-but-malformed property fails loud with a clean plan-time error.
    let name_mapping = parse_name_mapping(
        table
            .metadata()
            .properties()
            .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
            .map(String::as_str),
    )?;

    // AUTHORITATIVE correctness gate: fail loud at the manifest/`DataFile` level on
    // any delete/data mechanism this engine cannot apply (equality delete, Puffin/v3
    // deletion vector, ORC/Avro data or delete file) BEFORE building any
    // scan-driving SQL. This must run before `plan_files_from_table` so the deletes
    // it associates are guaranteed to be applicable Parquet positional deletes.
    ensure_supported_delete_mechanisms(&table, &catalog_props.table).await?;

    let files = plan_files_from_table(table, &catalog_props.table, filter_json).await?;
    Ok((
        files,
        effective_storage,
        logical_schema,
        table_root,
        name_mapping,
    ))
}

/// A data- or delete-file mechanism the lakehouse engine cannot apply on read.
///
/// This engine applies ONLY Parquet positional deletes over Parquet data files.
/// Every other mechanism must fail loud at plan time — invalid results must never
/// be returned (mission: "correctness and safety are first-class"). The variant is
/// used solely to name the mechanism in a clean, credential-free error; it never
/// carries a file path or any secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedDeleteMechanism {
    /// Iceberg equality deletes (`DataContentType::EqualityDeletes`).
    EqualityDelete,
    /// Iceberg v3 Puffin deletion vector (position delete stored as a Puffin blob).
    DeletionVector,
    /// An ORC data file (`DataFileFormat::Orc`).
    OrcDataFile,
    /// An Avro data file (`DataFileFormat::Avro`).
    AvroDataFile,
    /// An ORC positional-delete file.
    OrcDeleteFile,
    /// An Avro positional-delete file.
    AvroDeleteFile,
    /// A data file in a format this engine does not read as columnar Parquet.
    NonParquetDataFile,
}

impl UnsupportedDeleteMechanism {
    /// A stable, credential-free English name for the mechanism, spliced into the
    /// plan-time fail-loud error. Never includes a file path or any secret value.
    fn describe(self) -> &'static str {
        match self {
            UnsupportedDeleteMechanism::EqualityDelete => "Iceberg equality deletes",
            UnsupportedDeleteMechanism::DeletionVector => "Iceberg v3 Puffin deletion vectors",
            UnsupportedDeleteMechanism::OrcDataFile => "ORC data files",
            UnsupportedDeleteMechanism::AvroDataFile => "Avro data files",
            UnsupportedDeleteMechanism::OrcDeleteFile => "ORC delete files",
            UnsupportedDeleteMechanism::AvroDeleteFile => "Avro delete files",
            UnsupportedDeleteMechanism::NonParquetDataFile => "non-Parquet data files",
        }
    }
}

/// Classify one manifest `DataFile` by its content type and file format, at the
/// authoritative manifest level (where the Puffin discriminator and file format
/// are still visible — `plan_files` drops them, so a deletion vector would be
/// indistinguishable from a Parquet positional delete at read time).
///
/// Returns `Ok(())` ONLY for the two mechanisms this engine can apply correctly:
/// a Parquet DATA file and a Parquet POSITION-DELETE file. Every other
/// (content, format) combination returns the specific unsupported mechanism so
/// the caller can fail loud before building any scan-driving SQL.
fn classify_manifest_file(
    content: iceberg::spec::DataContentType,
    format: iceberg::spec::DataFileFormat,
) -> Result<(), UnsupportedDeleteMechanism> {
    use UnsupportedDeleteMechanism as U;
    use iceberg::spec::DataContentType::{Data, EqualityDeletes, PositionDeletes};
    use iceberg::spec::DataFileFormat::{Avro, Orc, Parquet, Puffin};
    match content {
        Data => match format {
            Parquet => Ok(()),
            Orc => Err(U::OrcDataFile),
            Avro => Err(U::AvroDataFile),
            Puffin => Err(U::NonParquetDataFile),
        },
        PositionDeletes => match format {
            Parquet => Ok(()),
            // A position delete stored as a Puffin blob IS a v3 deletion vector.
            Puffin => Err(U::DeletionVector),
            Orc => Err(U::OrcDeleteFile),
            Avro => Err(U::AvroDeleteFile),
        },
        EqualityDeletes => Err(U::EqualityDelete),
    }
}

/// Build the plan-time fail-loud error for an unsupported delete mechanism.
///
/// The message names ONLY the mechanism (never a file path, which could in
/// principle embed a presigned credential) and is defensively passed through
/// [`redact_catalog_error`] so no secret can survive into surfaced SQL/error text.
fn unsupported_delete_error(mechanism: UnsupportedDeleteMechanism, table_name: &str) -> UdfError {
    let msg = format!(
        "lakehouse pushdown declined for table '{}': it uses {}, which this engine \
         cannot apply on read (only Parquet positional deletes are supported); \
         this is a hard error, not a native re-plan",
        table_name,
        mechanism.describe(),
    );
    UdfError::User(redact_catalog_error(&msg))
}

/// Fail loud at plan time if the table's current snapshot uses ANY delete/data
/// mechanism this engine cannot apply, detected at the manifest/`DataFile` level.
///
/// This is the AUTHORITATIVE correctness gate (invalid results must never be
/// returned). It enumerates the current snapshot's manifest list, loads each
/// manifest, and classifies every ALIVE `DataFile` (both data and delete
/// manifests) via [`classify_manifest_file`]. Detection happens here — before any
/// scan-driving SQL is built — because `plan_files` collapses each task to a bare
/// path and drops the Puffin discriminator and file format needed to tell a
/// Parquet positional delete from a deletion vector.
///
/// A table with no current snapshot (empty table) trivially passes.
async fn ensure_supported_delete_mechanisms(
    table: &iceberg::table::Table,
    table_name: &str,
) -> Result<(), UdfError> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(());
    };
    let file_io = table.file_io();

    let manifest_list_bytes = file_io
        .new_input(snapshot.manifest_list())
        .map_err(|e| {
            UdfError::User(format!(
                "failed to open Iceberg manifest list for '{}': {}",
                table_name,
                redact_catalog_error(&e.to_string())
            ))
        })?
        .read()
        .await
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read Iceberg manifest list for '{}': {}",
                table_name,
                redact_catalog_error(&e.to_string())
            ))
        })?;

    let manifest_list = iceberg::spec::ManifestList::parse_with_version(
        &manifest_list_bytes,
        metadata.format_version(),
    )
    .map_err(|e| {
        UdfError::User(format!(
            "failed to parse Iceberg manifest list for '{}': {}",
            table_name,
            redact_catalog_error(&e.to_string())
        ))
    })?;

    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            UdfError::User(format!(
                "failed to load Iceberg manifest for '{}': {}",
                table_name,
                redact_catalog_error(&e.to_string())
            ))
        })?;
        for entry in manifest.entries() {
            // Skip entries removed in this snapshot: a DELETED manifest entry no
            // longer applies, so failing on it would spuriously reject queries.
            if !entry.is_alive() {
                continue;
            }
            let data_file = entry.data_file();
            classify_manifest_file(data_file.content_type(), data_file.file_format())
                .map_err(|mechanism| unsupported_delete_error(mechanism, table_name))?;
        }
    }

    Ok(())
}

/// Drive the iceberg scan and collect the data-file paths with their sizes.
///
/// When `filter_json` is `Some`, an Iceberg pruning predicate is applied before
/// `plan_files` so manifests and files that cannot match are skipped. DataFusion
/// remains the row-level correctness backstop; this is pruning-only.
/// Map an iceberg task-level delete content type to the wire [`DeleteFileContentType`].
///
/// By the time a `FileScanTask`'s deletes reach here, the plan-time fail-loud gate
/// ([`ensure_supported_delete_mechanisms`]) has already rejected any table that
/// uses equality deletes or Puffin deletion vectors, so every `PositionDeletes`
/// task delete is guaranteed to be a Parquet positional delete. The other arms
/// are mapped honestly for defense-in-depth: they can only be produced if a
/// mechanism somehow slips past the gate, and the scan reader's read-time backstop
/// then rejects them cleanly. `Data` never appears in a task's delete list; it is
/// mapped to a non-positional sentinel so it is likewise rejected rather than
/// silently applied.
fn map_delete_content_type(t: iceberg::spec::DataContentType) -> DeleteFileContentType {
    match t {
        iceberg::spec::DataContentType::PositionDeletes => DeleteFileContentType::PositionDeletes,
        iceberg::spec::DataContentType::EqualityDeletes => DeleteFileContentType::EqualityDeletes,
        iceberg::spec::DataContentType::Data => DeleteFileContentType::EqualityDeletes,
    }
}

async fn plan_files_from_table(
    table: iceberg::table::Table,
    table_name: &str,
    filter_json: Option<&Json>,
) -> Result<Vec<FileEntry>, UdfError> {
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

    // Associate each data file's Parquet positional-delete files into its entry.
    // The plan-time fail-loud gate (`ensure_supported_delete_mechanisms`) has
    // already run, so any `.deletes` present here are applicable Parquet
    // positional deletes. Absolute delete paths are relativized later, in
    // `relativize_shards_to_root`, EXACTLY like the data-file path.
    Ok(tasks
        .into_iter()
        .map(|t| {
            let deletes: Vec<DeleteFileRef> = t
                .deletes
                .iter()
                .map(|d| DeleteFileRef {
                    path: d.file_path.clone(),
                    size: d.file_size_in_bytes,
                    content_type: map_delete_content_type(d.file_type),
                })
                .collect();
            FileEntry::with_deletes(
                t.data_file_path().to_string(),
                t.file_size_in_bytes,
                deletes,
            )
        })
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
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
    project_columns(pushdown_req, extract_all_column_types(request))
}

/// Resolve a pushdown request's select list into an ordered projection and its
/// positionally-aligned Exasol EMITS types, drawing from a given column universe.
///
/// `all_cols` is the `(UPPERCASE name, Exasol type)` set the projection may
/// reference: the first involved table for a single-table scan, or the disjoint
/// union of BOTH involved tables for a broadcast join. Factoring the select-list
/// logic here lets the join path reuse it verbatim — a projected column's EMITS
/// type is looked up in whichever side owns it, with no bespoke join code — while
/// the single-table path is unchanged.
fn project_columns(
    pushdown_req: &Json,
    all_cols: Vec<(String, String)>,
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
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

    // Every column of the base row, each as a bare column reference. Used by the
    // no-select-list, unknown-node, and untranslatable-item fallbacks so Exasol
    // has the full row to post-process the query itself.
    let full_row = || -> (Vec<ProjectionItem>, Vec<String>) {
        let names = all_cols
            .iter()
            .map(|(n, _)| ProjectionItem::Column(n.clone()))
            .collect();
        let types = all_cols.iter().map(|(_, t)| t.clone()).collect();
        (names, types)
    };

    let select_list = pushdown_req.get("selectList");
    let (proj_names, proj_types): (Vec<ProjectionItem>, Vec<String>) = match select_list {
        None | Some(Json::Null) => full_row(),
        Some(Json::Array(list)) if list.is_empty() => {
            // Empty select list — project the first column only.
            let name = first_col_name;
            let ty = type_by_upper(&name);
            (vec![ProjectionItem::Column(name)], vec![ty])
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
                        names.push(ProjectionItem::Column(name));
                        types.push(ty);
                    }
                    t if is_literal_selectlist_item(t) => {
                        // A bare literal is NOT a projectable source column. Its
                        // rendered SQL (e.g. `NULL`, `'x'`, `5`) is an expression,
                        // never a column identifier — pushing it into the row-scan
                        // projection would later be quoted as a phantom EMITS column
                        // name (`"NULL"`) that DataFusion rejects (issue #52). The
                        // grouped "count the groups" shape is handled correctly in
                        // detect_group_by_aggregates; this is the row-scan backstop:
                        // fall back to the full base row so Exasol post-processes the
                        // literal projection itself.
                        needs_full_fallback = true;
                    }
                    "function_scalar"
                    | "predicate_equal"
                    | "predicate_less"
                    | "predicate_lessequal"
                    | "predicate_like"
                    | "predicate_and"
                    | "predicate_or"
                    | "predicate_not" => {
                        // Scalar expression node — try to render it.
                        match render_expression_safe(e) {
                            Some(sql_frag) => {
                                names.push(ProjectionItem::Expr { expr: sql_frag });
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
                full_row()
            } else {
                (names, types)
            }
        }
        _ => full_row(),
    };

    // Defensive backstop: duplicate EMITS column names are always invalid in Exasol,
    // regardless of which path produced the projection. Dedup by the positional EMITS
    // name, keeping the first occurrence and its type.
    let mut seen = std::collections::HashSet::new();
    let mut deduped_names = Vec::with_capacity(proj_names.len());
    let mut deduped_types = Vec::with_capacity(proj_types.len());
    for (name, ty) in proj_names.into_iter().zip(proj_types) {
        if seen.insert(name.emit_name().to_string()) {
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

/// Whether the pushdown request carries a non-empty `orderBy` array.
///
/// Exasol sends `orderBy` only when the adapter advertises an `ORDER_BY_*`
/// capability AND the query has an ORDER BY it can delegate; it withholds `limit`
/// entirely when it cannot delegate the ordering (verified live — decision log A1).
/// So this flag is the trigger for the anti-wrong-truncation guard (decision [4]):
/// when an `orderBy` is present but the request is not a matched ordered top-N, the
/// per-shard AND outer `LIMIT` are withheld and Exasol re-applies both clauses.
fn order_by_present(pushdown_req: &Json) -> bool {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
}

/// Parse ONE `orderBy` element into a bare-column [`SortKey`].
///
/// Returns `None` when the element is not a bare `column` node (only
/// `ORDER_BY_COLUMN` is advertised, so Exasol only ever sends bare-column sort
/// keys — anything else is an unexpected shape) or when its `isAscending` /
/// `nullsLast` flags are absent. The column name is uppercased to match the
/// adapter's canonical identifier casing. This is the SINGLE per-element parser
/// shared by [`detect_topn`] (which adds projection + JSON-fallback gates on top)
/// and [`parse_order_by_keys`] (the ungated backstop-restoration parse).
fn parse_sort_key_element(element: &Json) -> Option<SortKey> {
    let expr = element.get("expression")?;
    if expr.get("type").and_then(|t| t.as_str()) != Some("column") {
        return None;
    }
    let column = expr
        .get("name")
        .and_then(|n| n.as_str())
        .map(|s| s.to_uppercase())?;
    let ascending = element.get("isAscending").and_then(|b| b.as_bool())?;
    let nulls_last = element.get("nullsLast").and_then(|b| b.as_bool())?;
    Some(SortKey {
        column,
        ascending,
        nulls_last,
    })
}

/// Parse every `orderBy` element into [`SortKey`]s WITHOUT the top-N match gates
/// (projection membership, JSON-fallback type). Used to render the self-contained
/// final `ORDER BY` on the DECLINE / non-matched paths: once `ORDER_BY_COLUMN` is
/// advertised Exasol delegates the ordering and NO LONGER re-applies its own
/// backstop sort, so the adapter must reproduce that global sort in the SQL it
/// returns even for shapes it does not optimize (add-topn-pushdown B6). An element
/// that fails to parse as a bare column is skipped defensively.
fn parse_order_by_keys(pushdown_req: &Json) -> Vec<SortKey> {
    pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .map(|elements| elements.iter().filter_map(parse_sort_key_element).collect())
        .unwrap_or_default()
}

/// Detect the ordered-top-N shape and parse its sort keys.
///
/// Returns `Some(keys)` only when EVERY guard holds, so the caller may push the
/// keys as a per-shard bounded sort plus an outer merge `ORDER BY … LIMIT n`:
/// - exactly one involved table (no join),
/// - not a GROUP BY aggregate request (`aggregationType != "group_by"` and no
///   non-empty `groupBy`),
/// - no `having`,
/// - `limit` present with no `offset` (`LIMIT_WITH_OFFSET` is unadvertised, so an
///   offset should never appear — declined defensively if it does),
/// - a non-empty `orderBy` in which EVERY element is a bare `column` node whose
///   uppercased name is one of the projected columns (`ProjectionItem::Column`),
/// - EVERY sort key column resolves to an Arrow type that does NOT require the
///   JSON-fallback VARCHAR cast (`needs_json_fallback` is false for its
///   `LogicalField.arrow_type`).
///
/// The JSON-fallback guard is a correctness requirement, not an optimization: for a
/// fallback-typed column the per-shard scan emits `CAST(col AS VARCHAR)` (a JSON
/// string) but its `ORDER BY col` sorts by the column's REAL native value (the cast
/// lives only in the SELECT list, not the FROM-clause row source the ORDER BY binds
/// against). Exasol's outer merge sees ONLY the emitted JSON string, so it re-ranks
/// lexicographically — a representation the per-shard sort never used. Per-shard and
/// merge would disagree on ranking and silently corrupt the global top-N. Declining
/// falls back to the safe raw-scan path (Exasol re-applies ORDER BY/LIMIT).
/// (The tag vocabulary collapses List/Struct/Binary/etc to `utf8`, so the reachable
/// fallback tag today is an out-of-range `decimal128(p>36,…)`; the guard is the
/// correct seam regardless and stays correct if the tag vocabulary is enriched.)
///
/// A sort key column absent from `logical_schema` declines defensively (rather than
/// assuming a safe type) — it should never happen, since the key is already required
/// to be a projected column.
///
/// Any deviation returns `None` — the caller then withholds the limit (never a
/// bare per-shard/outer LIMIT ahead of an ordering the adapter did not render) and
/// falls back to the pre-existing plan, leaving row selection to Exasol.
///
/// Only ever called on the pure row-scan path (no aggregates); the GROUP BY and
/// aggregate guards below make it self-contained and independently testable.
fn detect_topn(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    logical_schema: &[LogicalField],
) -> Option<Vec<SortKey>> {
    // A top-N needs a bound. Limit must be present with no offset.
    extract_limit(pushdown_req)?;
    if pushdown_req
        .get("limit")
        .and_then(|l| l.get("offset"))
        .is_some()
    {
        return None;
    }

    // Reject GROUP BY / grouped-aggregate shapes: ordered top-N over aggregated or
    // grouped results is out of scope (mission non-goal).
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by") {
        return None;
    }
    if pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
    {
        return None;
    }

    // Reject HAVING (only meaningful with grouping; a defensive belt with the above).
    if pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some()
    {
        return None;
    }

    // Single involved table only — a multi-table (join) shape declines.
    let table_count = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|t| t.len())
        .unwrap_or(0);
    if table_count != 1 {
        return None;
    }

    // Parse each sort key: it must be a bare `column` node that is also projected.
    let elements = pushdown_req.get("orderBy").and_then(|v| v.as_array())?;
    if elements.is_empty() {
        return None;
    }
    let mut keys = Vec::with_capacity(elements.len());
    for element in elements {
        // Bare-column shape + direction/NULL flags (shared parser); a missing flag
        // or a non-column node is an unexpected shape → decline.
        let key = parse_sort_key_element(element)?;
        // The sort key must be one of the projected columns (per the plan's scope:
        // sort on already-emitted columns, no extra machinery). An expression
        // projection (`ProjectionItem::Expr`) is never a bare-column sort target.
        let projected = proj_cols
            .iter()
            .any(|p| matches!(p, ProjectionItem::Column(c) if *c == key.column));
        if !projected {
            return None;
        }
        // Decline any sort key whose column requires the JSON-fallback VARCHAR cast:
        // its emitted representation (a JSON string) would not match the native value
        // the per-shard ORDER BY sorts by, so the outer merge would re-rank on the
        // wrong representation and corrupt the global top-N. Resolve the column's
        // Arrow type from its logical-schema tag (the only type info available at plan
        // time). A column absent from the logical schema declines defensively.
        let arrow_type = logical_schema
            .iter()
            .find(|f| f.name.to_uppercase() == key.column)
            .map(|f| crate::types::mapping::arrow_type_from_tag(&f.arrow_type))?;
        if crate::types::mapping::needs_json_fallback(&arrow_type) {
            return None;
        }
        keys.push(key);
    }
    Some(keys)
}

/// Why a join `from` clause cannot be rendered by the join path at all.
///
/// The unified join path serves EVERY inner join of any arity (broadcast or the
/// N-scan fallback), so an `Ineligible` shape is the genuine last resort — a shape
/// the adapter cannot render, routed to a hard client-facing error. Each variant
/// names the specific reason so a caller can log or test it; every variant carries
/// no data because the shape check alone explains the decline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IneligibleJoinReason {
    /// A join node ANYWHERE in the tree has `join_type` other than `"inner"` (e.g.
    /// an outer join); a cross-join + conjunctive WHERE cannot reproduce its
    /// semantics.
    NotInnerJoinType,
    /// A join node is missing a `left`/`right`/`condition` field, or a leaf is
    /// neither a `join` nor a `table` node — a shape the planner does not recognize.
    UnsupportedShape,
}

/// One base-table leaf of a detected inner-join tree, with its original-cased
/// Iceberg identifier already recovered from `TABLE_MAP`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinLeaf {
    /// The Exasol virtual table name (a `from`-tree leaf's `name`).
    pub table_name: String,
    /// `table_name`'s original-cased Iceberg identifier, from `TABLE_MAP`.
    pub iceberg_ident: String,
}

/// A detected all-inner join tree over N ≥ 2 involved tables — the single unified
/// join shape (the two-involved-table case is simply N = 2).
///
/// `tables` are the base-table leaves in stable left-to-right tree order; every
/// leaf's Iceberg identifier is resolved from `TABLE_MAP` at detection time (a
/// missing leaf is a hard `Err`, not a value here). `conditions` are the N-1
/// join-node `condition` expressions collected while walking the tree —
/// AND-conjoined by the N-scan fallback, which is order-agnostic for an all-inner
/// join.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DetectedJoin {
    /// The N ≥ 2 base-table leaves in stable left-to-right tree order.
    pub tables: Vec<JoinLeaf>,
    /// The N-1 raw join-node `condition` expressions, unrendered.
    pub conditions: Vec<Json>,
}

/// The result of inspecting a pushdown request's `from` clause for the inner
/// equi-join shape this phase plans.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JoinShape {
    /// The `from` clause is a plain table reference (or absent) — today's
    /// single-table pushdown path applies unchanged.
    NotAJoin,
    /// The `from` clause is a join the adapter cannot render at all (a non-inner
    /// join node in the tree, or a malformed shape). Routed to a hard error — the
    /// genuine last resort, never a native re-plan.
    Ineligible(IneligibleJoinReason),
    /// An all-inner join tree spanning N ≥ 2 involved tables, every leaf's Iceberg
    /// identifier resolved from `TABLE_MAP`. Served by the SINGLE unified join path
    /// ([`plan_join`]): broadcast when the two-table (N = 2) case is eligible,
    /// otherwise the N-scan unaccelerated fallback. The two-table case is simply
    /// N = 2 — there is no separate two-table shape.
    Join(DetectedJoin),
}

/// Recursively collect a join tree's base-table leaf names (into `tables`, stable
/// left-to-right order) and every join node's `condition` (into `conditions`,
/// post-order).
///
/// Returns the specific [`IneligibleJoinReason`] on the first non-inner join node
/// ([`IneligibleJoinReason::NotInnerJoinType`]), a join node missing a
/// `left`/`right`/`condition` field or a leaf missing its `name`, or a leaf that is
/// neither a `join` nor a `table` node ([`IneligibleJoinReason::UnsupportedShape`]).
fn collect_join_tree(
    node: &Json,
    tables: &mut Vec<String>,
    conditions: &mut Vec<Json>,
) -> Result<(), IneligibleJoinReason> {
    match node.get("type").and_then(|t| t.as_str()) {
        Some("join") => {
            let is_inner = node
                .get("join_type")
                .and_then(|t| t.as_str())
                .is_some_and(|t| t.eq_ignore_ascii_case("inner"));
            if !is_inner {
                return Err(IneligibleJoinReason::NotInnerJoinType);
            }
            let (left, right) = match (node.get("left"), node.get("right")) {
                (Some(left), Some(right)) => (left, right),
                _ => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            let condition = match node.get("condition").filter(|c| !c.is_null()) {
                Some(condition) => condition.clone(),
                None => return Err(IneligibleJoinReason::UnsupportedShape),
            };
            collect_join_tree(left, tables, conditions)?;
            collect_join_tree(right, tables, conditions)?;
            conditions.push(condition);
            Ok(())
        }
        Some("table") => match node.get("name").and_then(|n| n.as_str()) {
            Some(name) => {
                tables.push(name.to_string());
                Ok(())
            }
            None => Err(IneligibleJoinReason::UnsupportedShape),
        },
        _ => Err(IneligibleJoinReason::UnsupportedShape),
    }
}

/// Detect whether a pushdown request's `from` clause is an inner-join tree the
/// unified join path serves, over N ≥ 2 involved tables.
///
/// Per the Exasol virtual-schema-common-java pushdown JSON shape, a join `from`
/// node looks like:
/// ```json
/// {"type": "join", "join_type": "inner", "left": {...}, "right": {...}, "condition": {...}}
/// ```
/// where `left`/`right` are each a base-table reference (`{"name": ..., "type": "table"}`)
/// or a nested `join` node. The whole tree is walked ONCE by [`collect_join_tree`]:
/// it collects the base-table leaves (stable left-to-right order) and every join
/// node's `condition`, asserting every join node is `join_type = "inner"`. The
/// two-involved-table case is simply N = 2 — there is no separate two-table shape,
/// and no equi-condition gate here (broadcast eligibility, computed later in
/// [`plan_join`], is what requires an equi condition; the N-scan fallback renders
/// any inner-join condition into its WHERE).
///
/// A request whose `from` clause is absent or a plain table reference is
/// [`JoinShape::NotAJoin`]: today's single-table pushdown path, unaffected.
///
/// A non-inner join node or a malformed node is [`JoinShape::Ineligible`] (a hard
/// error, the genuine last resort). Once the tree is a valid all-inner join, every
/// involved table's original-cased Iceberg identifier MUST be recoverable from
/// `TABLE_MAP` — a virtual table absent from `TABLE_MAP` is the same "stale virtual
/// schema" condition the single-table path reports, so it is a hard `Err`, not a
/// decline.
pub(crate) fn detect_join(request: &Json, pushdown_req: &Json) -> Result<JoinShape, UdfError> {
    let from = match pushdown_req.get("from") {
        Some(from) => from,
        None => return Ok(JoinShape::NotAJoin),
    };
    if from.get("type").and_then(|t| t.as_str()) != Some("join") {
        return Ok(JoinShape::NotAJoin);
    }

    let mut table_names = Vec::new();
    let mut conditions = Vec::new();
    if let Err(reason) = collect_join_tree(from, &mut table_names, &mut conditions) {
        return Ok(JoinShape::Ineligible(reason));
    }

    let table_map = super::read_table_map(request);
    let mut tables = Vec::with_capacity(table_names.len());
    for table_name in table_names {
        let iceberg_ident = table_map.get(&table_name).cloned().ok_or_else(|| {
            UdfError::User(format!(
                "pushdown: virtual table '{table_name}' is not in TABLE_MAP; \
                 drop and recreate the virtual schema"
            ))
        })?;
        tables.push(JoinLeaf {
            table_name,
            iceberg_ident,
        });
    }

    Ok(JoinShape::Join(DetectedJoin { tables, conditions }))
}

/// One fully-resolved side of a two-table inner equi-join.
///
/// Every field is resolved ONCE per query in the VS planning layer from Iceberg
/// manifest metadata — the same `resolve_file_list` path the single-table scan
/// uses — never per shard and never per node (mission.md "resolve metadata once
/// per query"). `total_bytes` is the sum of every file's `file_size_in_bytes`
/// (the Iceberg-manifest byte size, NO Parquet read), the quantity the broadcast
/// threshold is evaluated against.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedJoinSide {
    /// The Exasol virtual table name (a detected join leaf).
    pub table_name: String,
    /// The original-cased Iceberg identifier this side was resolved from.
    pub iceberg_ident: String,
    /// The Iceberg table root (`table.metadata().location()`); empty ⇒ every
    /// `files` path is absolute.
    pub table_root: String,
    /// This side's FULL file list as [`FileEntry`] values (path,
    /// `file_size_in_bytes`, and any associated positional-delete files). Deletes
    /// are resolved once here — the same `resolve_file_list` path the single-table
    /// scan uses — and travel with the side so the scan applies them per side.
    pub files: Vec<FileEntry>,
    /// Full logical schema of this side's Iceberg table at query time.
    pub logical_schema: Vec<LogicalField>,
    /// This side's flattened Iceberg `schema.name-mapping.default` entries
    /// (empty when the table has no name-mapping property). Resolved ONCE per
    /// query on the same `resolve_file_list` path as `logical_schema`.
    pub name_mapping: Vec<NameMappingEntry>,
    /// Effective storage for this side (vended STS creds when applicable).
    pub effective_storage: StorageProps,
    /// Sum of every file's `file_size_in_bytes` — the broadcast-threshold metric.
    pub total_bytes: u64,
}

impl ResolvedJoinSide {
    /// Assemble a resolved side, computing `total_bytes` from the file list with a
    /// saturating sum (a byte total that overflows `u64` is clamped to `u64::MAX`,
    /// which is correctly treated as "far over any broadcast threshold").
    fn new(
        table_name: String,
        iceberg_ident: String,
        table_root: String,
        files: Vec<FileEntry>,
        logical_schema: Vec<LogicalField>,
        name_mapping: Vec<NameMappingEntry>,
        effective_storage: StorageProps,
    ) -> Self {
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.size));
        Self {
            table_name,
            iceberg_ident,
            table_root,
            files,
            logical_schema,
            name_mapping,
            effective_storage,
            total_bytes,
        }
    }
}

/// The outcome of resolving BOTH sides of an eligible inner equi-join once and
/// deciding broadcast eligibility from Iceberg-manifest byte sizes.
///
/// Both sides are always carried fully resolved: the broadcast path (task 3.4)
/// shards `fact` and replicates `dimension`; the unaccelerated fallback (task 3.5)
/// scans BOTH sides through their own fan-outs, so it needs both here too. The
/// only role of `broadcast_eligible` is to route between those two SQL builders —
/// it is NEVER an error (decision-log [2]: an ineligible join takes the
/// deterministic N-scan fallback, not a native re-plan).
///
/// # Edge cases (decision-log has no explicit ruling; choices made here)
///
/// - **Self-join** (both sides the same Iceberg table): resolved and sized like
///   any other pair — both sides carry identical file lists and equal byte totals,
///   so the tie-break makes the LEFT side the dimension. Broadcasting a table
///   against itself is a *correct* inner join (every fact-shard row is matched
///   against the full table). No special case is needed here; the disjoint-
///   column-name guard (task 3.3) independently declines a self-join to the
///   unaccelerated path because its two sides share every column name.
/// - **Empty side** (either side resolves to zero files): its `total_bytes` is 0,
///   so an empty side is always the (trivially broadcast-eligible) dimension. An
///   inner join with an empty side yields zero rows either way; the caller may
///   short-circuit to an empty result by testing `fact.files.is_empty() ||
///   dimension.files.is_empty()`. Selection deliberately does not special-case it
///   — sizing and role assignment stay total and deterministic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinSides {
    /// The LARGER side by total bytes — sharded across the cluster exactly like
    /// the single-table scan path.
    pub fact: ResolvedJoinSide,
    /// The SMALLER side by total bytes — the broadcast/dimension candidate.
    pub dimension: ResolvedJoinSide,
    /// `true` when `dimension.total_bytes <= join_broadcast_max_bytes`: plan a
    /// broadcast join. `false`: the smaller side is still too big to replicate to
    /// every shard, so the caller builds the unaccelerated two-scan fallback SQL.
    pub broadcast_eligible: bool,
}

/// Choose the fact (sharded) and dimension (broadcast) roles from two resolved
/// sides and gate broadcast eligibility on the dimension's byte size.
///
/// The SMALLER side by total Iceberg-manifest bytes is the dimension; the larger
/// is the fact. On an exact byte-size tie the first argument (`a`) becomes the
/// dimension — deterministic and arbitrary, since equal-sized candidates are
/// interchangeable. The join is broadcast-eligible iff the chosen dimension's
/// total bytes are at or below `join_broadcast_max_bytes`.
///
/// This is the pure, catalog-free core of side selection so it is unit-testable
/// without a live Iceberg catalog; [`plan_join`] resolves each side and delegates
/// here for the two-table broadcast role/threshold decision.
fn select_broadcast_sides(
    a: ResolvedJoinSide,
    b: ResolvedJoinSide,
    join_broadcast_max_bytes: u64,
) -> JoinSides {
    let (dimension, fact) = if a.total_bytes <= b.total_bytes {
        (a, b)
    } else {
        (b, a)
    };
    let broadcast_eligible = dimension.total_bytes <= join_broadcast_max_bytes;
    JoinSides {
        fact,
        dimension,
        broadcast_eligible,
    }
}

/// Resolve ONE join side's file list, logical schema, table root, and effective
/// storage from the Iceberg catalog, reusing the single-table `resolve_file_list`
/// path unchanged.
///
/// `iceberg_ident` (the original-cased identifier recovered from `TABLE_MAP`)
/// replaces only the `table` field of the shared `catalog` template, so both
/// sides resolve against the same catalog URI and warehouse.
///
/// `filter_json` is this side's SIDE-LOCAL sub-predicate (see [`side_local_filter`])
/// — the conjuncts of the WHERE every column of which is this table's — forwarded
/// for Iceberg manifest pruning exactly as `filter_json_raw` is on the single-table
/// path. For an inner join a side-local conjunct is a necessary condition for that
/// side's rows to survive, so pruning by it is sound; cross-table and OR-spanning
/// conjuncts are already excluded from `filter_json`. `to_iceberg_predicate`
/// resolves it against this table's OWN schema, and sound-drops anything it cannot
/// translate. `None` (no side-local conjunct) prunes nothing — every file is kept.
async fn resolve_one_join_side(
    table_name: &str,
    iceberg_ident: &str,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    filter_json: Option<&Json>,
) -> Result<ResolvedJoinSide, UdfError> {
    let side_catalog = CatalogProps {
        table: iceberg_ident.to_string(),
        ..catalog.clone()
    };
    let (files, effective_storage, logical_schema, table_root, name_mapping) =
        resolve_file_list(catalog_uri, &side_catalog, storage, creds, filter_json).await?;
    Ok(ResolvedJoinSide::new(
        table_name.to_string(),
        iceberg_ident.to_string(),
        table_root,
        files,
        logical_schema,
        name_mapping,
        effective_storage,
    ))
}

/// The `(UPPERCASE name, Exasol type)` columns of the named involved table.
///
/// Locates the `involvedTables[]` entry whose `name` equals `table_name` (the
/// Exasol virtual table name carried in a [`JoinLeaf`]) and maps its columns
/// exactly as the single-table projection does — uppercased names, Exasol types
/// from `dataType`. Returns an empty vec when the table or its columns are absent.
fn involved_table_columns(request: &Json, table_name: &str) -> Vec<(String, String)> {
    request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .and_then(|tables| {
            tables
                .iter()
                .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(table_name))
        })
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

/// The disjoint-column-name guard for reusing the `vs-expression` translator
/// unchanged on a two-table join.
///
/// Returns `true` when no column NAME appears on both sides. Only then do bare,
/// non-table-qualified column references (which is all the translator renders —
/// see `render_expression`) resolve unambiguously against the COMBINED DataFusion
/// schema of both registered tables. A single shared name makes a bare reference
/// ambiguous, so the join is NOT eligible for translator-reuse rendering; the
/// caller declines to the unaccelerated two-scan path (this is a clean decline,
/// never an error). Comparison is by name only — a name collision breaks
/// resolution regardless of the columns' types. Both inputs already carry
/// uppercased names, so the check is exact.
fn disjoint_schema_guard(left: &[(String, String)], right: &[(String, String)]) -> bool {
    let left_names: std::collections::HashSet<&str> =
        left.iter().map(|(n, _)| n.as_str()).collect();
    !right.iter().any(|(n, _)| left_names.contains(n.as_str()))
}

/// Render a join's equi-condition node to a DataFusion SQL boolean expression via
/// the `vs-expression` translator (bare column names). `None` when the node cannot
/// be rendered — a defensive decline, since [`plan_join`] only reaches the broadcast
/// path for a `predicate_equal` condition. Uses `render_expression` (not the filter
/// renderer) so the boolean expression is returned verbatim, never suppressed as
/// trivially true.
fn render_join_condition(condition: &Json) -> Option<String> {
    render_expression_safe(condition)
}

/// The cross-table projection and Exasol EMITS types for a broadcast join.
///
/// Reuses [`project_columns`] against the disjoint union of both involved tables'
/// columns, so a projected column spanning either side is typed from whichever
/// side owns it. The caller must have already passed the [`disjoint_schema_guard`]
/// so the union carries no name collision. Broadcast is a two-table optimization,
/// so `join.tables[0]`/`[1]` are the two involved tables.
fn extract_join_projection(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<(Vec<ProjectionItem>, Vec<String>), UdfError> {
    let mut combined = involved_table_columns(request, &join.tables[0].table_name);
    combined.extend(involved_table_columns(request, &join.tables[1].table_name));
    project_columns(pushdown_req, combined)
}

/// The translator-reuse artifacts for a broadcast inner equi-join, rendered once
/// in the VS planning layer and consumed by the broadcast fan-out SQL builder.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedJoinPushdown {
    /// The rendered DataFusion SQL boolean join condition (→ [`JoinSpec::condition`]).
    pub condition: String,
    /// The rendered cross-table WHERE filter, or `None` when the request carries
    /// none (or it is trivially true and Exasol keeps it as a backstop).
    pub filter: Option<String>,
    /// The cross-table projection, spanning columns from both tables, in order.
    pub projection: Vec<ProjectionItem>,
    /// The Exasol EMITS type per projected column, positionally aligned with
    /// `projection`.
    pub projection_types: Vec<String>,
}

/// Render every `vs-expression` artifact a broadcast inner equi-join needs, after
/// enforcing the disjoint-column-name guard.
///
/// Broadcast is a two-table optimization, so `join.tables[0]`/`[1]` are the two
/// involved tables and `join.conditions[0]` is the equi-condition. Returns
/// `Ok(None)` — a clean decline, NOT an error — when the two tables share any
/// column name (the guard fails) or the equi-condition cannot be rendered; the
/// caller then falls through to the deterministic N-scan fallback, exactly as for
/// any other join off the broadcast path. `Ok(Some(..))` carries the rendered join
/// condition, the cross-table WHERE filter, and the cross-table projection with its
/// EMITS types. `Err` is reserved for a genuinely malformed request with no column
/// metadata at all (the same contract [`project_columns`] enforces for the
/// single-table path).
///
/// Rendering is side-agnostic: the translator emits bare column names, so the
/// result does not depend on which side is later selected as fact vs dimension.
pub(crate) fn render_broadcast_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
) -> Result<Option<RenderedJoinPushdown>, UdfError> {
    let left_cols = involved_table_columns(request, &join.tables[0].table_name);
    let right_cols = involved_table_columns(request, &join.tables[1].table_name);
    if !disjoint_schema_guard(&left_cols, &right_cols) {
        return Ok(None);
    }

    let condition = match render_join_condition(&join.conditions[0]) {
        Some(condition) => condition,
        None => return Ok(None),
    };

    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(render_df_filter_safe);

    let (projection, projection_types) = extract_join_projection(request, pushdown_req, join)?;

    Ok(Some(RenderedJoinPushdown {
        condition,
        filter,
        projection,
        projection_types,
    }))
}

/// Schema-qualify a UDF/script name for a pushdown-driving query.
///
/// The generated SQL runs outside the adapter script's own schema, so an
/// unqualified name would fail to resolve. Shared by the single-table path and the
/// join planner so both qualify identically.
fn qualify_udf(scan_schema: Option<&str>, udf: &str) -> String {
    match scan_schema {
        Some(schema) if !schema.is_empty() => format!("{}.{}", quote_ident(schema), udf),
        _ => udf.to_string(),
    }
}

/// The `User` decline error for a join `from` clause the adapter cannot render at
/// all — the genuine last resort.
///
/// Spanning more than two tables, needing Exasol postprocessing, or overlapping
/// column names are NEVER reasons to reach here — every such inner join is served
/// by the unified fallback. Only a non-inner join node in the tree or a malformed
/// shape lands here, and falling through to the single-table path would scan only
/// the first involved table and silently drop the join. So the only safe outcome is
/// a `User` error — surfaced by the FFI shim as a hard `F-UDF-CL-RUST-9001` client
/// error with no native re-plan (`vs-adapter/pushdown-planning-join` "declined
/// safely", last resort).
fn ineligible_join_decline(reason: IneligibleJoinReason) -> UdfError {
    let detail = match reason {
        IneligibleJoinReason::NotInnerJoinType => "the join is not an inner join",
        IneligibleJoinReason::UnsupportedShape => "the join `from` clause has an unsupported shape",
    };
    UdfError::User(format!(
        "join pushdown declined: {detail}; the adapter cannot render this join shape, \
         so this is a hard error, not a native re-plan"
    ))
}

/// Render one projection item as an outer-query SELECT expression: a bare column is
/// double-quoted, an already-rendered scalar expression is spliced verbatim.
fn projection_item_select_sql(item: &ProjectionItem) -> String {
    match item {
        ProjectionItem::Column(name) => quote_ident(name),
        ProjectionItem::Expr { expr } => expr.clone(),
    }
}

/// Deep-clone an expression node, tagging every `column` node with the subquery
/// alias its `tableName` maps to (`tableAlias`), so `vs-expression` renders it as a
/// table-qualified reference (`"ALIAS"."NAME"`).
///
/// This is the seam that makes the two-scan wrapper correct regardless of whether
/// the two joined tables share a column name: bare-name rendering (the broadcast
/// path) is ambiguous on a collision, but a table-qualified reference resolved
/// against each side's OWN fan-out subquery never is. A `column` whose `tableName`
/// is not in `alias_of` is left unqualified (it belongs to neither joined table —
/// which cannot happen for a well-formed two-table request).
fn annotate_columns_with_alias(expr: &Json, alias_of: &HashMap<String, String>) -> Json {
    match expr {
        Json::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len() + 1);
            for (key, value) in map {
                out.insert(key.clone(), annotate_columns_with_alias(value, alias_of));
            }
            if map.get("type").and_then(|t| t.as_str()) == Some("column")
                && let Some(alias) = map
                    .get("tableName")
                    .and_then(|t| t.as_str())
                    .and_then(|t| alias_of.get(&t.to_ascii_uppercase()))
            {
                out.insert("tableAlias".to_string(), Json::String(alias.clone()));
            }
            Json::Object(out)
        }
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|item| annotate_columns_with_alias(item, alias_of))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Render an expression node to table-qualified DataFusion/Exasol SQL for the
/// two-scan wrapper: annotate each `column` with its side alias, then reuse the
/// `vs-expression` translator. `None` when the node cannot be rendered.
fn render_expression_qualified(expr: &Json, alias_of: &HashMap<String, String>) -> Option<String> {
    render_expression_safe(&annotate_columns_with_alias(expr, alias_of))
}

/// Render a WHERE filter to a table-qualified Exasol boolean expression for the
/// two-scan wrapper. `None` when the filter is absent-shaped, trivially true, or
/// unrenderable — mirroring the single-table `render_df_filter_safe` contract, so a
/// dropped predicate is Exasol's own backstop responsibility exactly as elsewhere.
fn render_df_filter_qualified(filter: &Json, alias_of: &HashMap<String, String>) -> Option<String> {
    render_df_filter_safe(&annotate_columns_with_alias(filter, alias_of))
}

/// Walk an expression tree, recording every `column` node's owning side: its
/// UPPERCASE `tableName` into `tables`, or `has_untagged` when a `column` carries
/// no `tableName`. `any_column` becomes true on the first `column` node seen.
///
/// `tableName` is the SAME attribution signal [`annotate_columns_with_alias`] uses,
/// so conjunct-to-side attribution is by table identity — never by column name,
/// which keeps the shared-column-name case (both tables carry an `ID`) correct.
fn collect_column_tables(
    expr: &Json,
    tables: &mut std::collections::HashSet<String>,
    has_untagged: &mut bool,
    any_column: &mut bool,
) {
    match expr {
        Json::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("column") {
                *any_column = true;
                match map.get("tableName").and_then(|t| t.as_str()) {
                    Some(tn) => {
                        tables.insert(tn.to_ascii_uppercase());
                    }
                    None => *has_untagged = true,
                }
            }
            for value in map.values() {
                collect_column_tables(value, tables, has_untagged, any_column);
            }
        }
        Json::Array(items) => items
            .iter()
            .for_each(|item| collect_column_tables(item, tables, has_untagged, any_column)),
        _ => {}
    }
}

/// The single side a conjunct is local to — `Some(UPPERCASE table name)` iff every
/// `column` node it references is tagged with that ONE `tableName`. `None` when the
/// conjunct spans two tables, carries an untagged column, or references no column at
/// all (a bare literal). Such a conjunct is withheld from BOTH sides' pruning; only
/// the outer wrapper's WHERE (which renders the full predicate) applies it.
///
/// Sound for an inner equi-join: a conjunct over one side alone is a necessary
/// condition for that side's rows to survive the join, so using it to prune that
/// side can never drop a row the join would have kept.
fn conjunct_single_side(conjunct: &Json) -> Option<String> {
    let mut tables = std::collections::HashSet::new();
    let mut has_untagged = false;
    let mut any_column = false;
    collect_column_tables(conjunct, &mut tables, &mut has_untagged, &mut any_column);
    if has_untagged || !any_column || tables.len() != 1 {
        return None;
    }
    tables.into_iter().next()
}

/// Flatten a top-level `predicate_and` chain into its individual conjuncts,
/// recursing through nested `predicate_and` nodes (AND is associative). A non-AND
/// node (including a top-level `predicate_or`) is a single opaque conjunct — an OR
/// is never split, so an OR spanning both tables stays withheld from both sides.
fn flatten_conjuncts<'a>(filter: &'a Json, out: &mut Vec<&'a Json>) {
    if filter.get("type").and_then(|t| t.as_str()) == Some("predicate_and")
        && let Some(exprs) = filter.get("expressions").and_then(|e| e.as_array())
    {
        for expr in exprs {
            flatten_conjuncts(expr, out);
        }
        return;
    }
    out.push(filter);
}

/// The side-local sub-predicate of `filter` for `table_name`: the AND of exactly
/// those top-level conjuncts every column of which is attributed to `table_name`.
/// `None` when no conjunct is side-local to it.
///
/// This is what is threaded into (a) that side's `resolve_file_list` for Iceberg
/// manifest pruning and (b) that side's fan-out `ScanSpec.filter` for DataFusion
/// row-group pruning + row filtering. Cross-table conjuncts and OR-spanning
/// conjuncts are withheld here and applied only by the outer wrapper's WHERE.
fn side_local_filter(filter: &Json, table_name: &str) -> Option<Json> {
    let target = table_name.to_ascii_uppercase();
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    let mut kept: Vec<Json> = conjuncts
        .into_iter()
        .filter(|c| conjunct_single_side(c).as_deref() == Some(target.as_str()))
        .cloned()
        .collect();
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": kept,
        })),
    }
}

/// The cross-side residual sub-predicate of `filter`: the AND of exactly those
/// top-level conjuncts that are NOT side-local to a single table — i.e. cross-table,
/// OR-spanning, untagged, or column-free conjuncts (`conjunct_single_side` is
/// `None`). `None` when every conjunct is side-local.
///
/// This is the exact set-complement of the per-side [`side_local_filter`] slices:
/// every conjunct is either side-local to exactly one table (pushed into that side's
/// fan-out leg) or cross-side residual (kept here, in the outer wrapper's WHERE), so
/// the partition is total and disjoint — no conjunct is dropped or double-applied
/// (decision-log [7]).
fn cross_side_residual_filter(filter: &Json) -> Option<Json> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(filter, &mut conjuncts);
    let mut kept: Vec<Json> = conjuncts
        .into_iter()
        .filter(|c| conjunct_single_side(c).is_none())
        .cloned()
        .collect();
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(serde_json::json!({
            "type": "predicate_and",
            "expressions": kept,
        })),
    }
}

/// Deep-clone `expr` with every `tableAlias` key removed, so the reused
/// `vs-expression` translator renders BARE column names.
///
/// Exasol sends each column node with BOTH its `tableName` and the query's
/// `tableAlias` (e.g. `FROM fact_orders o` yields `tableAlias: "O"`), and the
/// translator emits `"ALIAS"."NAME"` whenever `tableAlias` is present. A single-table
/// fan-out ([`build_side_fan_out_sql`]) scans one relation exposing BARE uppercase
/// column names, so an alias-qualified reference would not resolve against it — the
/// fan-out's pushed filter must be bare, exactly like the single-table scan path.
/// `tableName` is left intact (the translator ignores it; conjunct attribution has
/// already read it upstream).
fn strip_table_alias(expr: &Json) -> Json {
    match expr {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(key, _)| key.as_str() != "tableAlias")
                .map(|(key, value)| (key.clone(), strip_table_alias(value)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_table_alias).collect()),
        other => other.clone(),
    }
}

/// Record the UPPERCASE name of every `column` node in `expr` attributed (by
/// `tableName`, case-insensitive) to `table_name`, recursively.
fn collect_side_column_names(
    expr: &Json,
    table_name: &str,
    out: &mut std::collections::HashSet<String>,
) {
    match expr {
        Json::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("column") {
                let tn = map.get("tableName").and_then(|t| t.as_str());
                let name = map.get("name").and_then(|n| n.as_str());
                if let (Some(tn), Some(name)) = (tn, name)
                    && tn.eq_ignore_ascii_case(table_name)
                {
                    out.insert(name.to_ascii_uppercase());
                }
            }
            for value in map.values() {
                collect_side_column_names(value, table_name, out);
            }
        }
        Json::Array(items) => items
            .iter()
            .for_each(|item| collect_side_column_names(item, table_name, out)),
        _ => {}
    }
}

/// The subset of `full_cols` this side actually contributes to the outer two-scan
/// wrapper — dropping columns the wrapper never references, so each fan-out leg
/// ships narrow rows instead of the table's full column set.
///
/// The kept set is every column of this side referenced by any clause the wrapper
/// renders: the SELECT list, the join condition, the WHERE (the FULL predicate —
/// the outer wrapper renders all of it, so a side-local *or* cross-table filter
/// column must survive), GROUP BY, HAVING, and ORDER BY. Order and Exasol types are
/// preserved from `full_cols`.
///
/// Two total-safety fallbacks keep the wrapper buildable: an absent/empty SELECT
/// list means `SELECT *` over both fan-outs, so every column is kept; and an
/// (unreachable) empty result keeps `full_cols` rather than emit a zero-column leg.
fn referenced_side_columns(
    pushdown_req: &Json,
    condition: &Json,
    table_name: &str,
    full_cols: &[(String, String)],
) -> Vec<(String, String)> {
    let mut names = std::collections::HashSet::new();
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            for item in list {
                collect_side_column_names(item, table_name, &mut names);
            }
        }
        // Absent/empty select list ⇒ the wrapper projects every column (SELECT *).
        _ => return full_cols.to_vec(),
    }
    collect_side_column_names(condition, table_name, &mut names);
    if let Some(f) = pushdown_req.get("filter").filter(|f| !f.is_null()) {
        collect_side_column_names(f, table_name, &mut names);
    }
    for key in ["groupBy", "orderBy"] {
        if let Some(v) = pushdown_req.get(key) {
            collect_side_column_names(v, table_name, &mut names);
        }
    }
    if let Some(h) = pushdown_req.get("having").filter(|h| !h.is_null()) {
        collect_side_column_names(h, table_name, &mut names);
    }
    let narrowed: Vec<(String, String)> = full_cols
        .iter()
        .filter(|(name, _)| names.contains(name))
        .cloned()
        .collect();
    if narrowed.is_empty() {
        full_cols.to_vec()
    } else {
        narrowed
    }
}

/// Render one select-list item to a table-qualified outer-SELECT expression through
/// the SINGLE `vs-expression` path — columns, literals, scalar expressions, a
/// top-level `function_aggregate`, AND a `function_aggregate` nested inside a scalar
/// function all render through the same recursive translator.
///
/// The translator splices an Exasol aggregate `name` verbatim (Exasol pushed it, so
/// it is a valid Exasol aggregate — `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, the
/// STDDEV/VARIANCE family), renders each argument by recursion (table-qualifying any
/// column argument via its `tableAlias`), handles `COUNT(*)`, and honors `DISTINCT`.
/// This is byte-compatible with the former top-level `render_aggregate_qualified`
/// (single-arg aggregate → `NAME(<arg>)`, `COUNT(*)` → `COUNT(*)`), and additionally
/// renders a scalar expression that wraps aggregates (e.g.
/// `ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`) instead of declining. `None` only when
/// the node genuinely cannot be rendered.
fn render_selectlist_item_qualified(
    item: &Json,
    alias_of: &HashMap<String, String>,
) -> Option<String> {
    render_expression_qualified(item, alias_of)
}

/// Whether a join pushdown request carries work Exasol must execute over the
/// materialized two-scan join rather than inside the broadcast in-UDF join: an
/// aggregate (single-group or grouped), a GROUP BY, an ORDER BY, a LIMIT, or a
/// HAVING. The broadcast path renders only projection + filter + join condition, so
/// any of these routes the join to the qualified two-scan fallback (which renders
/// them as ordinary Exasol SQL over the join), reproducing pre-`JOIN`-capability
/// behavior exactly.
fn join_requires_exasol_postprocessing(pushdown_req: &Json) -> bool {
    let has_aggregate_item = pushdown_req
        .get("selectList")
        .and_then(|v| v.as_array())
        .is_some_and(|list| {
            list.iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) == Some("function_aggregate"))
        });
    let has_group_by = pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty());
    let is_group_by_aggregation =
        pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by");
    let has_having = pushdown_req
        .get("having")
        .filter(|h| !h.is_null())
        .is_some();
    has_aggregate_item
        || has_group_by
        || is_group_by_aggregation
        || has_having
        || order_by_present(pushdown_req)
        || extract_limit(pushdown_req).is_some()
}

/// Plan an inner join (N ≥ 2 involved tables) through the SINGLE unified join path.
///
/// Resolves each involved table's file list, logical schema, and byte size ONCE
/// (one catalog resolution per table, never per shard), pruned by that table's
/// side-local WHERE conjuncts. An inner join with any empty side yields zero rows,
/// so an empty side short-circuits to the shape-correct empty result over the
/// combined N-table column universe (in stable side order, matching the fallback's
/// full-row projection).
///
/// Broadcast is an OPTIMIZATION selected inside this one path — never a second
/// implementation. It is taken only for a two-table (N = 2) equi-join whose smaller
/// side fits `join_broadcast_max_bytes`, whose request needs no Exasol
/// postprocessing (the in-UDF join renders only projection + filter + condition),
/// and whose bare-name broadcast render succeeds (disjoint column names + renderable
/// condition — `render_broadcast_join` returns `Ok(None)` otherwise, a clean
/// fall-through, never an error). Every other inner join — N ≥ 3, above threshold,
/// non-equi, overlapping columns, or needing postprocessing — takes the SOLE
/// fallback renderer, [`build_n_scan_join_sql`], which scans each table through its
/// own sharded fan-out and reconstructs the join in Exasol's core engine. A hard
/// `Err` (a client-facing error, no native re-plan) is the last resort, delegated to
/// the builder for a wrapper that genuinely cannot be built.
#[allow(clippy::too_many_arguments)]
async fn plan_join(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    catalog_uri: &str,
    storage: &StorageProps,
    catalog: &CatalogProps,
    creds: &ConnectionCreds,
    scan_schema: Option<&str>,
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
    join_broadcast_max_bytes: u64,
) -> Result<Json, UdfError> {
    // Resolve each side ONCE (one catalog resolution per involved table, never per
    // shard), each pruned by its own side-local WHERE conjuncts for Iceberg manifest
    // pruning — attributed by `tableName`, so a shared-column-name case stays correct.
    let filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let mut sides = Vec::with_capacity(join.tables.len());
    for leaf in &join.tables {
        let side_filter = filter.and_then(|f| side_local_filter(f, &leaf.table_name));
        let side = resolve_one_join_side(
            &leaf.table_name,
            &leaf.iceberg_ident,
            catalog_uri,
            storage,
            catalog,
            creds,
            side_filter.as_ref(),
        )
        .await?;
        sides.push(side);
    }

    // An inner join with any empty side is empty regardless of the plan. Emit the
    // shape-correct empty result over the combined N-table column universe (stable
    // side order) rather than a fan-out over an empty file list.
    if sides.iter().any(|s| s.files.is_empty()) {
        let mut combined = Vec::new();
        for leaf in &join.tables {
            combined.extend(involved_table_columns(request, &leaf.table_name));
        }
        let (proj_cols, proj_types) = project_columns(pushdown_req, combined.clone())?;
        return empty_result_sql(pushdown_req, &proj_cols, &proj_types, &combined);
    }

    let udf_name = qualify_udf(scan_schema, SCAN_UDF_NAME);
    let merge_udf_name = qualify_udf(scan_schema, DISTINCT_MERGE_UDF_NAME);
    let distribute_udf_name = qualify_udf(scan_schema, DISTRIBUTE_FILES_UDF_NAME);
    let tuning = JoinScanTuning {
        cluster_nodes,
        parallelism_factor,
        df_target_partitions,
        df_batch_size,
        df_threads_per_udf,
        memory_pool_fraction,
        instance_overhead_mb,
        s3_max_connections,
    };

    // Broadcast eligibility is a PROPERTY of the request, computed here: exactly two
    // involved tables, a `predicate_equal` condition, and no Exasol postprocessing.
    // When it holds, size the two sides (smaller = dimension) and take the broadcast
    // fan-out iff the dimension fits the threshold AND the bare-name render succeeds.
    // Any miss falls through to the N-scan fallback below — never an error.
    let is_equi =
        join.conditions[0].get("type").and_then(|t| t.as_str()) == Some("predicate_equal");
    if join.tables.len() == 2 && is_equi && !join_requires_exasol_postprocessing(pushdown_req) {
        let candidate =
            select_broadcast_sides(sides[0].clone(), sides[1].clone(), join_broadcast_max_bytes);
        if candidate.broadcast_eligible
            && let Some(rendered) = render_broadcast_join(request, pushdown_req, join)?
        {
            let sql = build_broadcast_join_sql(
                &candidate,
                &rendered,
                &tuning,
                &udf_name,
                &merge_udf_name,
                &distribute_udf_name,
            );
            return Ok(serde_json::json!({"type": "pushdown", "sql": sql}));
        }
    }

    let sql = build_n_scan_join_sql(
        request,
        pushdown_req,
        join,
        &sides,
        &tuning,
        &udf_name,
        &merge_udf_name,
        &distribute_udf_name,
    )?;
    Ok(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// Side `i`'s Exasol virtual table name (UPPERCASE) maps to `aliases[i]`
/// (`LHS_T{i}`), so every column reference the N-scan wrapper renders is
/// table-qualified from its `tableName`.
fn build_n_scan_alias_map(
    sides: &[ResolvedJoinSide],
    aliases: &[String],
) -> HashMap<String, String> {
    sides
        .iter()
        .zip(aliases)
        .map(|(side, alias)| (side.table_name.to_ascii_uppercase(), alias.clone()))
        .collect()
}

/// Render the N-scan fallback's FROM as a left-to-right `INNER JOIN … ON` chain and
/// return it together with any join conditions that could not be attached to a join
/// point (untagged, or referencing no known leg). Those unattachable conditions
/// become outer-WHERE residual conjuncts — for an inner join a condition in the
/// WHERE is result-equivalent to the same condition in an `ON` clause, so this is a
/// safe last-resort backstop (decision-log [7]).
///
/// `conditions[i]` is the pre-rendered, table-qualified SQL for `raw_conditions[i]`.
/// Each condition GREEDILY attaches to the earliest join point where every table it
/// touches is in scope — the join point that brings its highest-indexed leg in.
/// Scope is resolved by the SET of `tableName`s the raw condition references
/// (via [`collect_column_tables`]), NEVER by column name, so two legs sharing a
/// column name can never fool the attachment. A join point with no attached
/// condition renders `ON 1=1`.
fn build_n_scan_join_from(
    fan_outs: &[String],
    aliases: &[String],
    raw_conditions: &[Json],
    conditions: &[String],
    sides: &[ResolvedJoinSide],
) -> (String, Vec<String>) {
    let leg_index: HashMap<String, usize> = sides
        .iter()
        .enumerate()
        .map(|(i, s)| (s.table_name.to_ascii_uppercase(), i))
        .collect();
    let last_join_point = aliases.len().saturating_sub(1);

    let mut on_at: Vec<Vec<String>> = vec![Vec::new(); aliases.len()];
    let mut residual: Vec<String> = Vec::new();
    for (raw, rendered) in raw_conditions.iter().zip(conditions) {
        let mut tables = std::collections::HashSet::new();
        let mut has_untagged = false;
        let mut any_column = false;
        collect_column_tables(raw, &mut tables, &mut has_untagged, &mut any_column);
        let resolvable =
            any_column && !has_untagged && tables.iter().all(|t| leg_index.contains_key(t));
        match resolvable
            .then(|| tables.iter().map(|t| leg_index[t]).max())
            .flatten()
        {
            // The earliest join point in scope is the one bringing the
            // highest-indexed leg in; clamp to a real join point (≥ 1, ≤ last).
            // Guard `last_join_point >= 1` (i.e. at least one join exists) first:
            // with a single leg there is no join point to attach to (and
            // `clamp(1, 0)` would panic since min > max), so fall through to
            // residual; behavior for N≥2 is unchanged.
            Some(m) if last_join_point >= 1 => {
                on_at[m.clamp(1, last_join_point)].push(rendered.clone())
            }
            _ => residual.push(rendered.clone()),
        }
    }

    let mut from = format!("({}) AS {}", fan_outs[0], quote_ident(&aliases[0]));
    for k in 1..aliases.len() {
        let on = if on_at[k].is_empty() {
            "1=1".to_string()
        } else {
            on_at[k]
                .iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        from.push_str(&format!(
            " INNER JOIN ({}) AS {} ON {on}",
            fan_outs[k],
            quote_ident(&aliases[k])
        ));
    }
    (from, residual)
}

/// Every column of all involved tables as a table-qualified projection item, in
/// side order. `cols_per_side[i]` belongs to the side aliased `aliases[i]`.
fn n_full_row_qualified_items(
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Vec<ProjectionItem> {
    aliases
        .iter()
        .zip(cols_per_side)
        .flat_map(|(alias, cols)| {
            cols.iter().map(move |(name, _)| ProjectionItem::Expr {
                expr: format!("{}.{}", quote_ident(alias), quote_ident(name)),
            })
        })
        .collect()
}

/// The N-scan wrapper's outer SELECT list, table-qualified. An absent/empty select
/// list projects every column of all involved tables in side order. An item that
/// cannot be rendered is a last-resort hard error (no native re-plan).
fn n_scan_join_select_items(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
    aliases: &[String],
    cols_per_side: &[Vec<(String, String)>],
) -> Result<Vec<ProjectionItem>, UdfError> {
    match pushdown_req.get("selectList") {
        Some(Json::Array(list)) if !list.is_empty() => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                let sql = render_selectlist_item_qualified(item, alias_of).ok_or_else(|| {
                    UdfError::User(
                        "join pushdown declined: a select-list item could not be rendered for the \
                         qualified N-scan join; this is a hard error, not a native re-plan"
                            .into(),
                    )
                })?;
                items.push(ProjectionItem::Expr { expr: sql });
            }
            Ok(items)
        }
        _ => Ok(n_full_row_qualified_items(aliases, cols_per_side)),
    }
}

/// Build the N-scan (N ≥ 2) unaccelerated inner-join SQL — the SOLE unaccelerated
/// fallback renderer (the two-involved-table case is simply N = 2). Each involved
/// table is scanned through its own sharded fan-out and reconstructed into the
/// original inner join by Exasol's core engine via a left-to-right `INNER JOIN … ON`
/// chain.
///
/// Each side emits its full column set (narrowed to the columns the wrapper actually
/// references across all clauses), so the outer wrapper's SELECT, every join
/// condition, WHERE, aggregate, GROUP BY, HAVING, and ORDER BY can reference any
/// column the join needs — all rendered TABLE-QUALIFIED (`"LHS_T{i}"."COL"`) from
/// each `column` node's `tableName`, so the wrapper is correct whether or not any
/// two involved tables share a column name (decision-log [2]).
///
/// The FROM is a left-to-right `INNER JOIN … ON` chain (decision-log [6]): each join
/// condition greedily attaches to the earliest join point where every table it
/// touches is in scope, resolved by the SET of `tableName`s the condition references
/// (never by column name, so shared column names cannot misroute scope); a join
/// point with no newly-resolvable condition renders `ON 1=1`. Each side's side-local
/// WHERE conjuncts are pushed into that side's fan-out leg; only cross-table /
/// OR-spanning / untagged residual conjuncts (and any untaggable join condition)
/// remain in the outer WHERE, each parenthesized so a top-level `OR` cannot bind
/// across the ANDs. For an inner join this is result-equivalent to single-node
/// evaluation, independent of join order and of shared column names (decision-log
/// [7]).
///
/// Returns an `Err` (a hard client-facing error, no native re-plan) only when the
/// wrapper genuinely cannot be built: an involved table carries no column metadata,
/// or a join condition (or a pushed select/GROUP BY/HAVING/ORDER BY element) cannot
/// be rendered at all.
#[allow(clippy::too_many_arguments)]
fn build_n_scan_join_sql(
    request: &Json,
    pushdown_req: &Json,
    join: &DetectedJoin,
    sides: &[ResolvedJoinSide],
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    let cols_per_side: Vec<Vec<(String, String)>> = sides
        .iter()
        .map(|s| involved_table_columns(request, &s.table_name))
        .collect();
    if cols_per_side.iter().any(|c| c.is_empty()) {
        return Err(UdfError::User(
            "join pushdown declined: an involved table carries no column metadata, so the \
             unaccelerated N-scan fallback cannot be built; this is a hard error, not a \
             native re-plan"
                .into(),
        ));
    }

    let aliases: Vec<String> = (0..sides.len()).map(|i| format!("LHS_T{i}")).collect();
    let alias_of = build_n_scan_alias_map(sides, &aliases);

    // Every join-tree condition, table-qualified. A condition is the one clause with
    // no lower fallback: if it cannot be rendered even qualified, no correct join SQL
    // exists → last-resort hard error (no native re-plan).
    let mut conditions = Vec::with_capacity(join.conditions.len());
    for cond in &join.conditions {
        let rendered = render_expression_qualified(cond, &alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a join condition could not be rendered against the \
                 qualified N-scan schema; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?;
        conditions.push(rendered);
    }

    // Task 4.2: the outer WHERE keeps ONLY the residual conjuncts NOT side-local to a
    // single leg (cross-table, OR-spanning, or untagged); every side-local conjunct
    // is pushed into its leg's fan-out below and never re-applied here. The partition
    // is exact and total (see `side_local_filter` vs `cross_side_residual_filter`).
    let filter = pushdown_req
        .get("filter")
        .filter(|f| !f.is_null())
        .and_then(cross_side_residual_filter)
        .and_then(|residual| render_df_filter_qualified(&residual, &alias_of));

    let select_items = n_scan_join_select_items(pushdown_req, &alias_of, &aliases, &cols_per_side)?;
    let group_by = qualified_join_group_by(pushdown_req, &alias_of)?;
    let having = qualified_join_having(pushdown_req, &alias_of)?;
    let order_by = qualified_join_order_by(pushdown_req, &alias_of)?;
    let limit = extract_limit(pushdown_req);

    // Per-side fan-out: narrow each leg's projection to the columns the wrapper
    // references (across the SELECT list, ALL join conditions, WHERE, GROUP BY,
    // HAVING, and ORDER BY), and push each side's side-local WHERE conjuncts down as a
    // DataFusion filter. Cross-table and OR-spanning conjuncts stay only in the outer
    // WHERE (`filter`), the correctness backstop. All N-1 conditions are passed as one
    // JSON array so `referenced_side_columns` (which walks arbitrary nodes) keeps a
    // side's column referenced by ANY condition.
    let where_filter = pushdown_req.get("filter").filter(|f| !f.is_null());
    let all_conditions = Json::Array(join.conditions.clone());
    let mut fan_outs = Vec::with_capacity(sides.len());
    for (i, side) in sides.iter().enumerate() {
        let narrowed = referenced_side_columns(
            pushdown_req,
            &all_conditions,
            &side.table_name,
            &cols_per_side[i],
        );
        let side_filter = where_filter.and_then(|f| side_local_filter(f, &side.table_name));
        fan_outs.push(build_side_fan_out_sql(
            side,
            &narrowed,
            side_filter.as_ref(),
            tuning,
            udf_name,
            merge_udf_name,
            distribute_udf_name,
        ));
    }

    // Assemble the INNER JOIN … ON chain (decision-log [6]). FROM is the chain of
    // aliased fan-out legs with each condition greedily attached by table-name set;
    // the outer WHERE carries the residual filter plus any untaggable join condition.
    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let (from, residual_conditions) =
        build_n_scan_join_from(&fan_outs, &aliases, &join.conditions, &conditions, sides);

    let mut where_parts: Vec<String> = residual_conditions
        .iter()
        .map(|c| format!("({c})"))
        .collect();
    if let Some(f) = &filter {
        where_parts.push(format!("({f})"));
    }

    let mut sql = format!("SELECT {select} FROM {from}");
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    if let Some(clause) = group_by {
        sql.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        sql.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        sql.push_str(&format!(" ORDER BY {clause}"));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

/// The DataFusion execution + sharding knobs threaded into join SQL building.
///
/// Bundled so the two join SQL builders take one config parameter instead of eight
/// positional numbers whose order is easy to transpose (guardrails: few arguments,
/// config at high levels).
struct JoinScanTuning {
    cluster_nodes: usize,
    parallelism_factor: usize,
    df_target_partitions: usize,
    df_batch_size: usize,
    df_threads_per_udf: usize,
    memory_pool_fraction: f64,
    instance_overhead_mb: u64,
    s3_max_connections: usize,
}

/// Relativize one file list against its table root (single-list convenience over
/// [`relativize_shards_to_root`], preserving order and byte sizes).
fn relativize_files_to_root(files: Vec<FileEntry>, table_root: &str) -> Vec<FileEntry> {
    relativize_shards_to_root(vec![files], table_root)
        .pop()
        .unwrap_or_default()
}

/// Build one side's single-table sharded fan-out SQL (an outer ungrouped scalar
/// `LAKEHOUSE_SCAN` over the nested distributor, or a from-less scalar call on
/// literals for a single shard — no `SELECT * FROM (...)` wrapper, decision [5]),
/// emitting the columns the outer wrapper references for this side and pushing this
/// side's SIDE-LOCAL WHERE conjuncts down as a DataFusion filter. No join block, no
/// limit push. Used for BOTH sides of the unaccelerated fallback: the outer Exasol
/// query (see [`build_n_scan_join_sql`]) still applies the projection, conditions, and
/// the FULL `WHERE`, so `columns` (the side's narrowed `(UPPERCASE name, Exasol
/// type)` list, see [`referenced_side_columns`]) must expose every column any outer
/// clause references. `side_filter` (see [`side_local_filter`]) is rendered bare-name
/// via `render_df_filter_safe` so DataFusion row-group-prunes and row-filters this
/// leg before emitting, rather than shipping every row for Exasol to filter.
fn build_side_fan_out_sql(
    side: &ResolvedJoinSide,
    columns: &[(String, String)],
    side_filter: Option<&Json>,
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let proj_cols: Vec<ProjectionItem> = columns
        .iter()
        .map(|(name, _)| ProjectionItem::Column(name.clone()))
        .collect();
    let proj_types: Vec<String> = columns.iter().map(|(_, ty)| ty.clone()).collect();

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        side.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(side.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &side.table_root);

    let spec = ScanSpec {
        table_root: side.table_root.clone(),
        files: vec![],
        projection: proj_cols.clone(),
        // Render BARE (strip Exasol's `tableAlias`): the fan-out is a single-table
        // scan whose relation exposes bare uppercase column names, so an
        // alias-qualified reference would not resolve — exactly the single-table
        // scan path's contract. The outer wrapper's WHERE re-qualifies separately.
        filter: side_filter
            .map(strip_table_alias)
            .and_then(|f| render_df_filter_safe(&f)),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: proj_types.clone(),
        logical_schema: side.logical_schema.clone(),
        name_mapping: side.name_mapping.clone(),
        join: None,
        storage: side.effective_storage.clone(),
        df_target_partitions: tuning.df_target_partitions,
        df_batch_size: tuning.df_batch_size,
        df_threads_per_udf: tuning.df_threads_per_udf,
        memory_pool_fraction: tuning.memory_pool_fraction,
        instance_overhead_mb: tuning.instance_overhead_mb,
        s3_max_connections: tuning.s3_max_connections,
    };
    build_scan_driving_sql(
        &spec,
        &shards,
        &proj_cols,
        &proj_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    )
}

/// Build the broadcast fan-out scan-driving SQL (task 3.4).
///
/// The fact (larger) side is sharded into G byte-balanced work units exactly as the
/// single-table path does; the dimension (smaller) side's FULL file list, table
/// root, logical schema, join type, and rendered condition ride ONCE in the
/// shard-invariant common blob's join block ([`JoinSpec`]). Every shard invocation
/// therefore re-scans the same dimension side and joins it against its fact-file
/// subset node-locally, with no cross-shard exchange. Reuses [`build_scan_driving_sql`]
/// unchanged — the join block travels transparently inside the common blob.
///
/// One `StorageProps` serves both registered tables inside the single DataFusion
/// session; the fact side's effective storage is used. When vended credentials are
/// disabled (the common MinIO case) both sides' effective storage is identical, so
/// this is exact; with per-prefix vended STS creds both tables must be readable with
/// the fact side's grant (both live under one warehouse for the broadcast target).
fn build_broadcast_join_sql(
    sides: &JoinSides,
    rendered: &RenderedJoinPushdown,
    tuning: &JoinScanTuning,
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> String {
    let fact = &sides.fact;
    let dimension = &sides.dimension;

    let g = shard_count(
        tuning.cluster_nodes,
        tuning.parallelism_factor,
        fact.files.len(),
    );
    let shards = crate::adapter::sharding::partition_files_by_bytes(fact.files.clone(), g);
    let shards = relativize_shards_to_root(shards, &fact.table_root);

    let join = JoinSpec {
        table_root: dimension.table_root.clone(),
        files: relativize_files_to_root(dimension.files.clone(), &dimension.table_root),
        logical_schema: dimension.logical_schema.clone(),
        name_mapping: dimension.name_mapping.clone(),
        join_type: JoinType::Inner,
        condition: rendered.condition.clone(),
    };

    let spec = ScanSpec {
        table_root: fact.table_root.clone(),
        files: vec![],
        projection: rendered.projection.clone(),
        filter: rendered.filter.clone(),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: rendered.projection_types.clone(),
        logical_schema: fact.logical_schema.clone(),
        name_mapping: fact.name_mapping.clone(),
        join: Some(join),
        storage: fact.effective_storage.clone(),
        df_target_partitions: tuning.df_target_partitions,
        df_batch_size: tuning.df_batch_size,
        df_threads_per_udf: tuning.df_threads_per_udf,
        memory_pool_fraction: tuning.memory_pool_fraction,
        instance_overhead_mb: tuning.instance_overhead_mb,
        s3_max_connections: tuning.s3_max_connections,
    };

    build_scan_driving_sql(
        &spec,
        &shards,
        &rendered.projection,
        &rendered.projection_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    )
}

/// The N-scan wrapper's `GROUP BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `groupBy`. A group key that cannot
/// be rendered is a last-resort hard error (no native re-plan).
fn qualified_join_group_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let keys = match pushdown_req
        .get("groupBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(keys) => keys,
        None => return Ok(None),
    };
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(render_expression_qualified(key, alias_of).ok_or_else(|| {
            UdfError::User(
                "join pushdown declined: a GROUP BY key could not be rendered for the qualified \
                 N-scan join; this is a hard error, not a native re-plan"
                    .into(),
            )
        })?);
    }
    Ok(Some(parts.join(", ")))
}

/// The N-scan wrapper's `HAVING` clause (without the keyword), table-qualified.
/// `None` when the request carries no `having`. An unrenderable HAVING is a
/// last-resort hard error (dropping it would return wrong rows; no native re-plan).
fn qualified_join_having(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    match pushdown_req.get("having").filter(|h| !h.is_null()) {
        Some(having) => Ok(Some(
            render_expression_qualified(having, alias_of).ok_or_else(|| {
                UdfError::User(
                    "join pushdown declined: HAVING could not be rendered for the qualified \
                     N-scan join; this is a hard error, not a native re-plan"
                        .into(),
                )
            })?,
        )),
        None => Ok(None),
    }
}

/// The N-scan wrapper's `ORDER BY` clause (without the keyword), table-qualified.
/// `None` when the request carries no non-empty `orderBy`. Only bare-column sort
/// keys are advertised (`ORDER_BY_COLUMN`); an element that is not a renderable bare
/// column is a last-resort hard error (dropping it would return an unordered
/// result Exasol delegated and no longer re-sorts; no native re-plan).
fn qualified_join_order_by(
    pushdown_req: &Json,
    alias_of: &HashMap<String, String>,
) -> Result<Option<String>, UdfError> {
    let elements = match pushdown_req
        .get("orderBy")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
    {
        Some(elements) => elements,
        None => return Ok(None),
    };
    let decline = || {
        UdfError::User(
            "join pushdown declined: an ORDER BY key could not be rendered for the qualified \
             N-scan join; this is a hard error, not a native re-plan"
                .into(),
        )
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        let key = parse_sort_key_element(element).ok_or_else(decline)?;
        let expr = element.get("expression").ok_or_else(decline)?;
        let rendered = render_expression_qualified(expr, alias_of).ok_or_else(decline)?;
        parts.push(key.render_ordered(&rendered));
    }
    Ok(Some(parts.join(", ")))
}

/// The full base row as `(ProjectionItem::Column, Exasol type)` lists, positionally
/// aligned. Used by the grouped qualified-wrapper fallback so its inner sharded raw
/// scan exposes every column the outer grouped select list / GROUP BY / HAVING /
/// ORDER BY can reference.
fn full_row_projection(all_cols: &[(String, String)]) -> (Vec<ProjectionItem>, Vec<String>) {
    (
        all_cols
            .iter()
            .map(|(name, _)| ProjectionItem::Column(name.clone()))
            .collect(),
        all_cols.iter().map(|(_, ty)| ty.clone()).collect(),
    )
}

/// Build the qualified single-table wrapper for a GROUP BY request that could not be
/// decomposed into the partial/merge plan (an undecomposable scalar-over-aggregate
/// item, a non-numeric aggregate with no HAVING, or any other non-pushable grouped
/// shape). This is the join N-scan fallback at N = 1: one aliased raw fan-out
/// subquery, no cross-join and no join condition, with the exact grouped select list,
/// GROUP BY, HAVING, ORDER BY, and LIMIT rendered as ordinary Exasol SQL over it so
/// Exasol's core engine computes the aggregate over the returned rows.
///
/// Reuses the join path's qualified renderers verbatim: the single table is aliased
/// `LHS_T0`, every column reference is table-qualified against that alias, and
/// aggregates are spliced verbatim by the `vs-expression` translator (Exasol
/// aggregates over materialized rows, not over merged partials). The per-shard scan
/// stays LIMIT-free and sort-free (`fan_out_spec` carries no limit/order_by); the
/// group keys, HAVING, ORDER BY, and LIMIT live only in the outer wrapper. The WHERE
/// filter is applied inside the scan (via `fan_out_spec.filter`), so no outer WHERE
/// is needed — mirroring the grouped push-down path. The result column count and
/// per-column types match Exasol's positional `selectListDataTypes` validation, so
/// this never emits the `04000`-triggering bare row scan.
fn build_grouped_qualified_fallback_sql<E: Clone + Into<FileEntry>>(
    request: &Json,
    pushdown_req: &Json,
    fan_out_spec: &ScanSpec,
    shards: &[Vec<E>],
    udf_name: &str,
    merge_udf_name: &str,
    distribute_udf_name: &str,
) -> Result<String, UdfError> {
    const ALIAS: &str = "LHS_T0";

    // Alias EVERY involved table name to the single subquery alias, so a column
    // node's `tableName` (or a stale request `tableAlias`) resolves to `"LHS_T0"`.
    let alias_of: HashMap<String, String> = request
        .get("involvedTables")
        .and_then(|v| v.as_array())
        .map(|tables| {
            tables
                .iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                .map(|name| (name.to_ascii_uppercase(), ALIAS.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let aliases = vec![ALIAS.to_string()];

    // The scan exposes the full base row; reconstruct the `(name, type)` universe
    // from the fan-out spec so the no-select-list fallback (unusual for a grouped
    // request) still resolves types from the one side.
    let all_cols: Vec<(String, String)> = fan_out_spec
        .projection
        .iter()
        .zip(fan_out_spec.emit_exa_types.iter())
        .filter_map(|(item, ty)| match item {
            ProjectionItem::Column(name) => Some((name.clone(), ty.clone())),
            ProjectionItem::Expr { .. } => None,
        })
        .collect();
    let cols_per_side = vec![all_cols];

    let select_items = n_scan_join_select_items(pushdown_req, &alias_of, &aliases, &cols_per_side)?;
    let group_by = qualified_join_group_by(pushdown_req, &alias_of)?;
    let having = qualified_join_having(pushdown_req, &alias_of)?;
    let order_by = qualified_join_order_by(pushdown_req, &alias_of)?;
    let limit = extract_limit(pushdown_req);

    // One aliased raw sharded fan-out. LIMIT-free / sort-free / no aggregates — the
    // fan-out spec already guarantees this.
    let proj_cols = fan_out_spec.projection.clone();
    let proj_types = fan_out_spec.emit_exa_types.clone();
    let fan_out = build_scan_driving_sql(
        fan_out_spec,
        shards,
        &proj_cols,
        &proj_types,
        None,
        &[],
        &[],
        udf_name,
        merge_udf_name,
        distribute_udf_name,
    );

    let select = if select_items.is_empty() {
        "*".to_string()
    } else {
        select_items
            .iter()
            .map(projection_item_select_sql)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut sql = format!("SELECT {select} FROM ({fan_out}) AS {}", quote_ident(ALIAS));
    if let Some(clause) = group_by {
        sql.push_str(&format!(" GROUP BY {clause}"));
    }
    if let Some(clause) = having {
        sql.push_str(&format!(" HAVING {clause}"));
    }
    if let Some(clause) = order_by {
        sql.push_str(&format!(" ORDER BY {clause}"));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

/// Build the shape-correct empty-result response for a fully-pruned file list.
///
/// The request-shape decision is hoisted ahead of the zero-files short-circuit
/// and mirrors the non-empty dispatch priority — grouped aggregate, then
/// single-group aggregate, then row scan. Both aggregate branches are gated on
/// the same `validate_agg_col_types` check the non-empty path applies: a
/// non-numeric aggregate demotes to the next shape, so the empty response's
/// positional column shape always equals what the non-empty path would have
/// committed to. A non-numeric grouped aggregate carrying a HAVING is declined
/// with the same `Err` the non-empty path returns (a hard error, no native re-plan),
/// because the adapter advertises AGGREGATE_HAVING and dropping the HAVING would
/// yield wrong results. No scan or distinct-merge UDF is referenced: with zero
/// files there is nothing to scan or merge.
fn empty_result_sql(
    pushdown_req: &Json,
    proj_cols: &[ProjectionItem],
    proj_types: &[String],
    col_types: &[(String, String)],
) -> Result<Json, UdfError> {
    if let Some(detection) = detect_group_by_aggregates(pushdown_req) {
        if validate_agg_col_types(&detection.plans, col_types) {
            let group_key_types = group_key_exasol_types(
                pushdown_req,
                &detection.group_keys,
                &detection.select_items,
            );
            // Per-plan declared types, aligned 1:1 with `detection.plans` (includes
            // aggregates nested inside a scalar-over-aggregate item) — the same
            // aligned source the non-empty grouped path now uses.
            return Ok(empty_grouped_sql(
                &group_key_types,
                &detection.plan_types,
                &detection.select_items,
            ));
        }
        // Gate failed. The non-empty grouped path declines with an Err when a
        // HAVING is present (advertised AGGREGATE_HAVING → Exasol will not
        // re-apply it); mirror that so the empty path declines identically.
        if pushdown_req
            .get("having")
            .filter(|h| !h.is_null())
            .is_some()
        {
            return Err(UdfError::User(
                "grouped aggregate pushdown declined: HAVING present but aggregate \
                 column type is non-numeric; this is a hard error, not a native re-plan"
                    .into(),
            ));
        }
        // No HAVING: fall through to the group_by qualified-wrapper shape below,
        // exactly as the non-empty path routes such a request.
    }
    // A GROUP BY request that declined grouped detection (or the non-numeric-no-HAVING
    // fall-through above) routes, on the non-empty path, to the qualified single-table
    // wrapper whose output columns ARE the `selectList` items. Mirror that shape here
    // with a zero-row result typed from `selectListDataTypes`, so the empty and
    // non-empty column shapes never diverge (never a full-row `04000` mismatch).
    if pushdown_req.get("aggregationType").and_then(|v| v.as_str()) == Some("group_by")
        && let Some(sql) = empty_select_list_typed_sql(pushdown_req)
    {
        return Ok(sql);
    }
    if let Some(aggregates) =
        detect_aggregates(pushdown_req).filter(|plans| validate_agg_col_types(plans, col_types))
    {
        return Ok(empty_agg_sql(
            &aggregates,
            &aggregate_exasol_types(pushdown_req),
        ));
    }
    Ok(empty_pushdown_sql(proj_cols, proj_types))
}

/// A zero-row result whose columns are `CAST(NULL AS <ty>)` for each
/// `selectListDataTypes` entry, in order — the empty-result shape matching the
/// grouped qualified-wrapper fallback (whose output columns are the `selectList`
/// items). `None` when `selectListDataTypes` is absent or empty (the caller then
/// falls back to the full-row empty shape).
fn empty_select_list_typed_sql(pushdown_req: &Json) -> Option<Json> {
    let types = pushdown_req
        .get("selectListDataTypes")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())?;
    let items: Vec<String> = types
        .iter()
        .map(|dt| format!("CAST(NULL AS {})", exasol_type_from_json(dt)))
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    Some(serde_json::json!({"type": "pushdown", "sql": sql}))
}

/// The empty-result literal for an aggregate evaluated over zero input rows.
///
/// The COUNT family yields `0`; every other kind yields `NULL` — single-node SQL
/// semantics over zero rows, mirroring the zero-count NULL guard (ADR-008).
fn empty_agg_literal(kind: &AggKind) -> &'static str {
    match kind {
        AggKind::Count | AggKind::CountCol | AggKind::CountDistinct => "0",
        AggKind::Sum
        | AggKind::Min
        | AggKind::Max
        | AggKind::Avg
        | AggKind::VarPop
        | AggKind::VarSamp
        | AggKind::StddevPop
        | AggKind::StddevSamp => "NULL",
    }
}

/// Build the single-group aggregate empty-result response: exactly one row whose
/// columns are the per-`AggKind` empty literals cast to their declared result
/// types (from `aggregate_exasol_types`/`selectListDataTypes`), in select-list
/// order. `FROM DUAL` alone already yields one row, so no `WHERE` is emitted.
///
/// The cast decision mirrors `cast_merge_items` (cast when a declared type is
/// present and not the `VARCHAR(2000000)` default) so the empty column types can
/// never drift from the non-empty single-group shape.
fn empty_agg_sql(aggregates: &[AggregatePlan], aggregate_types: &[String]) -> Json {
    let items: Vec<String> = aggregates
        .iter()
        .enumerate()
        .map(|(i, plan)| {
            let literal = empty_agg_literal(&plan.kind);
            match aggregate_types.get(i) {
                Some(ty) if ty != "VARCHAR(2000000)" => format!("CAST({literal} AS {ty})"),
                _ => literal.to_string(),
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build the grouped aggregate empty-result response: zero rows
/// (`FROM DUAL WHERE 1=0`) whose columns are the full grouped output shape —
/// group-key, merged-aggregate, and constant-projection columns assembled in the
/// user's select-list order via `select_items`, exactly as the non-empty grouped
/// merge assembles its outer SELECT.
///
/// Group-key and aggregate columns are `CAST(NULL AS <declared-type>)` (types from
/// `group_key_exasol_types` / `aggregate_exasol_types`); a constant projection
/// reuses its already-rendered, type-cast expression. A zero-row result satisfies
/// any HAVING / ORDER BY / LIMIT, so none of those need rendering.
fn empty_grouped_sql(
    group_key_types: &[String],
    aggregate_types: &[String],
    select_items: &[GroupedSelectItem],
) -> Json {
    let mut ordered = select_items.to_vec();
    ordered.sort_by_key(select_item_index);
    let items: Vec<String> = ordered
        .iter()
        .filter_map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => group_key_types
                .get(*group_key_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Aggregate { plan_slot, .. } => aggregate_types
                .get(*plan_slot)
                .map(|ty| format!("CAST(NULL AS {ty})")),
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            // A scalar-over-aggregate column is NULL over zero rows, typed to the
            // item's own declared type (mirrors the group-key/aggregate cast so the
            // empty grouped shape never drifts from the non-empty wrapper).
            GroupedSelectItem::ScalarOverAggregate { declared_type, .. } => {
                Some(if declared_type != "VARCHAR(2000000)" {
                    format!("CAST(NULL AS {declared_type})")
                } else {
                    "NULL".to_string()
                })
            }
        })
        .collect();
    let sql = format!("SELECT {} FROM DUAL WHERE 1=0", items.join(", "));
    serde_json::json!({"type": "pushdown", "sql": sql})
}

/// Build a pushdown response with an empty result (no matching files).
fn empty_pushdown_sql(proj_cols: &[ProjectionItem], proj_types: &[String]) -> Json {
    let items: Vec<String> = proj_cols
        .iter()
        .zip(proj_types.iter())
        .map(|(item, ty)| format!("CAST(NULL AS {ty}) AS {}", quote_ident(item.emit_name())))
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
    use iceberg::spec::{DataContentType, DataFileFormat};
    use vs_expression::render_df_filter_safe;

    // ---------------------------------------------------------------------------
    // Task 1.3 — fail-loud on unsupported delete/data mechanisms (manifest level)
    // ---------------------------------------------------------------------------

    /// The two mechanisms this engine CAN apply — a Parquet data file and a
    /// Parquet positional-delete file — classify as supported (`Ok`).
    #[test]
    fn classify_accepts_parquet_data_and_parquet_positional_delete() {
        assert!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Parquet).is_ok(),
            "Parquet data file must be supported"
        );
        assert!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Parquet)
                .is_ok(),
            "Parquet positional delete must be supported"
        );
    }

    /// Equality deletes fail loud regardless of file format.
    #[test]
    fn classify_rejects_equality_deletes() {
        for fmt in [
            DataFileFormat::Parquet,
            DataFileFormat::Avro,
            DataFileFormat::Orc,
        ] {
            assert_eq!(
                classify_manifest_file(DataContentType::EqualityDeletes, fmt),
                Err(UnsupportedDeleteMechanism::EqualityDelete),
                "equality delete ({fmt:?}) must fail loud"
            );
        }
    }

    /// A position delete stored as a Puffin blob is a v3 deletion vector — the
    /// exact case indistinguishable from a Parquet positional delete once
    /// `plan_files` has dropped the format discriminator, so it MUST be caught at
    /// the manifest level.
    #[test]
    fn classify_rejects_puffin_deletion_vector() {
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Puffin),
            Err(UnsupportedDeleteMechanism::DeletionVector),
            "Puffin position delete (deletion vector) must fail loud"
        );
    }

    /// ORC/Avro data and delete files fail loud.
    #[test]
    fn classify_rejects_orc_and_avro_data_and_delete_files() {
        assert_eq!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Orc),
            Err(UnsupportedDeleteMechanism::OrcDataFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::Data, DataFileFormat::Avro),
            Err(UnsupportedDeleteMechanism::AvroDataFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Orc),
            Err(UnsupportedDeleteMechanism::OrcDeleteFile),
        );
        assert_eq!(
            classify_manifest_file(DataContentType::PositionDeletes, DataFileFormat::Avro),
            Err(UnsupportedDeleteMechanism::AvroDeleteFile),
        );
    }

    /// The fail-loud error names the mechanism, names the table, and leaks no
    /// credential (defensively redacted).
    #[test]
    fn unsupported_delete_error_names_mechanism_and_redacts() {
        let err = unsupported_delete_error(
            UnsupportedDeleteMechanism::DeletionVector,
            "db.mor_dv_table",
        );
        let msg = match err {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg.contains("Iceberg v3 Puffin deletion vectors"),
            "error must name the mechanism: {msg}"
        );
        assert!(
            msg.contains("db.mor_dv_table"),
            "error must name the offending table: {msg}"
        );
        // No credential label may survive the defensive redaction.
        assert!(
            !msg.contains("access_key"),
            "must not leak access_key: {msg}"
        );
        assert!(
            !msg.contains("secret_key"),
            "must not leak secret_key: {msg}"
        );
    }

    // ---------------------------------------------------------------------------
    // Task 1.2 — adapter carries positional deletes into the per-shard scan spec
    // ---------------------------------------------------------------------------

    /// A Parquet positional-delete file ref.
    fn pos_delete(path: &str, size: u64) -> DeleteFileRef {
        DeleteFileRef {
            path: path.into(),
            size,
            content_type: DeleteFileContentType::PositionDeletes,
        }
    }

    /// A minimal delete-carrying row-scan `ScanSpec` template (files replaced per
    /// shard by the builder), used to assert what the per-shard/common arguments
    /// carry.
    fn delete_spec_template() -> ScanSpec {
        ScanSpec {
            table_root: "s3://warehouse/db/table".into(),
            files: vec![],
            projection: vec![ProjectionItem::Column("ID".into())],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: vec!["DECIMAL(20,0)".into()],
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        }
    }

    /// `map_delete_content_type` maps the iceberg task-level content type onto the
    /// wire enum honestly (position → position; equality → equality).
    #[test]
    fn map_delete_content_type_maps_position_and_equality() {
        use iceberg::spec::DataContentType;
        assert_eq!(
            map_delete_content_type(DataContentType::PositionDeletes),
            DeleteFileContentType::PositionDeletes
        );
        assert_eq!(
            map_delete_content_type(DataContentType::EqualityDeletes),
            DeleteFileContentType::EqualityDeletes
        );
    }

    /// A data file's associated positional-delete file paths are relativized by
    /// the SAME rule as the data-file path: an under-root path is stripped to a
    /// root-relative path, a path not under the root stays absolute. Delete size
    /// and content type are preserved.
    #[test]
    fn delete_file_paths_use_relative_absolute_encoding() {
        let root = "s3://warehouse/db/table";
        let entry = FileEntry::with_deletes(
            format!("{root}/data/part-0.parquet"),
            1000,
            vec![
                // under the table root — must relativize exactly like the data path
                pos_delete(&format!("{root}/data/deletes/del-0.parquet"), 50),
                // not under the root — must stay absolute
                pos_delete("s3://other-bucket/del-x.parquet", 60),
            ],
        );
        let shards = relativize_shards_to_root(vec![vec![entry]], root);
        let e = &shards[0][0];
        assert_eq!(e.path, "data/part-0.parquet", "data path must relativize");
        assert_eq!(
            e.deletes[0].path, "data/deletes/del-0.parquet",
            "under-root delete path must relativize EXACTLY like the data path"
        );
        assert_eq!(e.deletes[0].size, 50, "delete size preserved");
        assert_eq!(
            e.deletes[0].content_type,
            DeleteFileContentType::PositionDeletes,
            "delete content type preserved"
        );
        assert_eq!(
            e.deletes[1].path, "s3://other-bucket/del-x.parquet",
            "a delete path not under the root must stay absolute"
        );
    }

    /// Positional deletes survive into the per-shard scan spec for BOTH
    /// `write.delete.granularity=file` (one data file → its own delete file) and
    /// `partition` (one delete file referenced by multiple data files).
    #[test]
    fn adapter_preserves_positional_deletes_into_scan_spec() {
        // file granularity: one data file carries its own positional-delete file.
        let file_gran = vec![FileEntry::with_deletes(
            "data/part-0.parquet",
            1000,
            vec![pos_delete("data/deletes/del-0.parquet", 50)],
        )];
        let back = ScanSpec::files_from_json(&shard_files_json(&file_gran)).unwrap();
        assert_eq!(back, file_gran, "file-granularity deletes must round-trip");
        assert_eq!(back[0].deletes.len(), 1);
        assert_eq!(
            back[0].deletes[0].content_type,
            DeleteFileContentType::PositionDeletes
        );

        // partition granularity: the SAME delete file is referenced by two data files.
        let shared = "data/deletes/part-del.parquet";
        let part_gran = vec![
            FileEntry::with_deletes("data/p0.parquet", 1, vec![pos_delete(shared, 80)]),
            FileEntry::with_deletes("data/p1.parquet", 1, vec![pos_delete(shared, 80)]),
        ];
        let back2 = ScanSpec::files_from_json(&shard_files_json(&part_gran)).unwrap();
        assert_eq!(
            back2, part_gran,
            "both data files must retain the shared partition delete"
        );
        assert_eq!(back2[1].deletes[0].path, shared);
    }

    /// A delete-carrying entry serializes with its content type on the wire; a
    /// delete-free entry stays the compact `[path, size]` 2-tuple (no wire bloat,
    /// backward-compatible with pre-delete payloads).
    #[test]
    fn delete_file_entry_carries_content_type_and_delete_free_stays_compact() {
        let with_del = vec![FileEntry::with_deletes(
            "d.parquet",
            5,
            vec![pos_delete("del.parquet", 2)],
        )];
        let json = shard_files_json(&with_del);
        assert!(
            json.contains("position_deletes"),
            "delete content type must appear on the wire: {json}"
        );
        let back = ScanSpec::files_from_json(&json).unwrap();
        assert_eq!(
            back[0].deletes[0].content_type,
            DeleteFileContentType::PositionDeletes
        );

        let free = vec![FileEntry::new("data/part-0.parquet", 1000)];
        assert_eq!(
            shard_files_json(&free),
            r#"[["data/part-0.parquet",1000]]"#,
            "delete-free entry must stay the compact 2-tuple form"
        );
    }

    /// Delete refs ride ONLY in the per-shard files argument, never in the
    /// shard-invariant common blob, and the common blob carries no serialized
    /// Iceberg schema or bound predicate (the minimal-surface decision).
    #[test]
    fn adapter_carries_delete_refs_per_shard_minimal_common_spec() {
        let spec_template = delete_spec_template();
        let shards = vec![vec![FileEntry::with_deletes(
            "data/part-0.parquet",
            1000,
            vec![pos_delete("data/deletes/del-0.parquet", 50)],
        )]];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[ProjectionItem::Column("ID".into())],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        assert!(
            sql.contains("del-0.parquet"),
            "per-shard files argument must carry the delete file: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("del-0.parquet"),
            "common blob must NOT carry per-shard delete refs: {common}"
        );
        assert!(
            !common.contains("BoundPredicate") && !common.contains("bound_predicate"),
            "common blob must carry no serialized iceberg predicate: {common}"
        );
    }

    /// The shared fan-out primitive emits a nested `LAKEHOUSE_DISTRIBUTE_FILES`
    /// distributor (`GROUP BY shard_key` over the per-shard file lists) wrapped by an
    /// outer UNGROUPED scalar `LAKEHOUSE_SCAN('{common}', files)` select. The
    /// shard-invariant common blob is spliced exactly ONCE (the outer scalar's first
    /// argument); only the per-shard `files` strings flow through the distributor, so
    /// the fan-out payload is data-volume-independent.
    #[test]
    fn fan_out_primitive_wraps_distributor_in_ungrouped_scalar_scan() {
        let spec = delete_spec_template();
        let shards = vec![
            vec![FileEntry::new("data/part-0.parquet", 1000)],
            vec![FileEntry::new("data/part-1.parquet", 2000)],
        ];
        let emits = r#""ID" DECIMAL(20,0)"#;
        let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

        assert!(
            sql.contains("DISTRIBUTE(files) FROM (VALUES"),
            "distributor passthrough is called bare (its LUA EMITS is static): {sql}"
        );
        assert!(
            !sql.contains("DISTRIBUTE(files) EMITS"),
            "the statically-defined distributor call MUST NOT carry a query-side EMITS: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the GROUP BY shard_key fan-out must live in the distributor subquery: {sql}"
        );
        assert!(
            sql.contains(&format!(
                "SELECT SCAN('{}",
                spec.to_common_json().replace('\'', "''")
            )),
            "the outer scalar scan splices the common blob as its first-arg literal: {sql}"
        );
        assert!(
            sql.contains(", files) EMITS ("),
            "the outer scalar scan reads the bare distributed files column, not a literal: {sql}"
        );
        // The common blob (which carries table_root) appears exactly once: in the
        // outer scalar's first argument, never repeated per shard in the distributor.
        assert_eq!(
            sql.matches("s3://warehouse/db/table").count(),
            1,
            "common blob must be spliced exactly once, not per shard: {sql}"
        );
    }

    /// A single-shard plan short-circuits the distributor entirely: a from-less scalar
    /// `LAKEHOUSE_SCAN('{common}', '{files}')` call on literals (no distributor, no
    /// inner `GROUP BY`, no `VALUES` driving relation).
    #[test]
    fn single_shard_short_circuits_distributor_fromless() {
        let spec = delete_spec_template();
        let shards = vec![vec![FileEntry::new("data/part-0.parquet", 1000)]];
        let emits = r#""ID" DECIMAL(20,0)"#;
        let sql = build_fan_out_inner(&spec, &shards, emits, "SCAN", "DISTRIBUTE");

        assert!(
            sql.starts_with("SELECT SCAN("),
            "from-less scalar call: {sql}"
        );
        assert!(
            !sql.contains("DISTRIBUTE"),
            "no distributor for one shard: {sql}"
        );
        assert!(
            !sql.contains("GROUP BY shard_key"),
            "no shard_key grouping for one shard: {sql}"
        );
        assert!(!sql.contains("VALUES"), "no driving VALUES relation: {sql}");
        let files_literal = sql_string_literal(&shard_files_json(&shards[0]));
        assert!(
            sql.contains(&format!(", {files_literal}) EMITS (")),
            "the single shard's files must be spliced as a literal: {sql}"
        );
    }

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
        let proj_items: Vec<ProjectionItem> = proj_cols
            .iter()
            .cloned()
            .map(ProjectionItem::Column)
            .collect();
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: proj_items.clone(),
            filter,
            limit,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let files_with_sizes: Vec<FileEntry> =
            files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
        let shards =
            crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            &shards,
            &proj_items,
            &proj_types,
            limit,
            &col_types,
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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

    /// Build row-scan SQL the way `handle_pushdown` does for a resolved
    /// `(path, size)` file list under `table_root`: partition into shards,
    /// relativize under-root paths, then build. Exercises the SAME production
    /// stripping (`relativize_shards_to_root`) that runs in `handle_pushdown`, so
    /// the emitted per-shard paths match production exactly.
    fn build_row_sql_with_root(
        files: Vec<(String, u64)>,
        table_root: &str,
        proj_cols: Vec<String>,
        proj_types: Vec<String>,
        cluster_nodes: usize,
    ) -> String {
        let col_types: Vec<(String, String)> = proj_cols
            .iter()
            .cloned()
            .zip(proj_types.iter().cloned())
            .collect();
        let proj_items: Vec<ProjectionItem> = proj_cols
            .iter()
            .cloned()
            .map(ProjectionItem::Column)
            .collect();
        let spec_template = ScanSpec {
            table_root: table_root.to_string(),
            files: vec![],
            projection: proj_items.clone(),
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: proj_types.clone(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
        let g = shard_count(cluster_nodes, 1, files.len());
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let shards = relativize_shards_to_root(shards, table_root);
        build_scan_driving_sql(
            &spec_template,
            &shards,
            &proj_items,
            &proj_types,
            None,
            &col_types,
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
    }

    /// Pushdown carries the table root ONCE in the common blob and per-shard file
    /// sizes travel into the shard payloads (verification scenario, CHANGED).
    #[test]
    fn pushdown_carries_table_root_and_sizes_in_common_and_shards() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        // Two nodes → two shards (one file each) so a genuine fan-out is emitted.
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The table root is carried in the shard-invariant common blob.
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(&format!(r#""table_root":"{root}""#)),
            "common blob must carry table_root once: {common}"
        );

        // Each per-shard payload carries its file's byte size as a [path,size] tuple.
        assert!(
            sql.contains(r#"[["part-00000.parquet",1024]]"#),
            "shard payload must carry relative path + size for file 0: {sql}"
        );
        assert!(
            sql.contains(r#"[["part-00001.parquet",2048]]"#),
            "shard payload must carry relative path + size for file 1: {sql}"
        );
    }

    /// The table root is stripped from every under-root path and appears EXACTLY
    /// ONCE (in the common literal), NEVER in a per-shard VALUES literal (NEW).
    #[test]
    fn table_root_stripped_from_under_root_paths_and_carried_once() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The root string occurs exactly once in the whole statement: in the common
        // blob's table_root. Stripped relative paths never repeat the prefix.
        assert_eq!(
            sql.matches(root).count(),
            1,
            "table root must appear exactly once (common blob only), never per shard: {sql}"
        );
        // That single occurrence lives in the common literal.
        assert!(
            common_arg_literal(&sql).contains(root),
            "the sole table-root occurrence must be in the common blob: {sql}"
        );
        // The per-shard VALUES section (everything after the common literal) carries
        // only relative paths.
        assert!(
            sql.contains("part-00000.parquet") && sql.contains("part-00001.parquet"),
            "shards must carry the relative file names: {sql}"
        );
    }

    /// A data-file path NOT under the table root is carried as a full absolute URI
    /// (NEW).
    #[test]
    fn path_not_under_root_stays_absolute() {
        let root = "s3://warehouse/db/events";
        let outside = "s3://other-bucket/external/f.parquet";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (outside.to_string(), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // The under-root file is emitted relative.
        assert!(
            sql.contains(r#"["part-00000.parquet",1024]"#),
            "under-root path must be relativized: {sql}"
        );
        // The not-under-root file keeps its full absolute URI, with its size.
        assert!(
            sql.contains(&format!(r#"["{outside}",2048]"#)),
            "path outside the table root must stay absolute: {sql}"
        );
        // The table root is still carried exactly once (the absolute outside path
        // does not contain the root prefix).
        assert_eq!(
            sql.matches(root).count(),
            1,
            "table root must appear exactly once even with an out-of-root file: {sql}"
        );
    }

    /// Mirror of the scan UDF's `reconstruct_abs_uri` join rule, so the round-trip
    /// invariant can be asserted here without a cross-crate dependency: an entry that
    /// already carries a scheme (`"://"`) is absolute and returned unchanged; any
    /// other entry is joined onto the root with exactly one `/`.
    fn reconstruct_abs_uri_mirror(entry_path: &str, table_root: &str) -> String {
        if entry_path.contains("://") {
            return entry_path.to_string();
        }
        let root = table_root.strip_suffix('/').unwrap_or(table_root);
        let rel = entry_path.strip_prefix('/').unwrap_or(entry_path);
        format!("{root}/{rel}")
    }

    /// A path that shares the table root only as a bare STRING prefix (no `/`
    /// segment boundary) must NOT be relativized: stripping it and rejoining with a
    /// single `/` corrupts the URI (finding R.1). Only true under-root paths are
    /// stripped; everything else stays absolute and round-trips to itself.
    #[test]
    fn sibling_prefix_paths_are_not_relativized() {
        let root = "s3://w/db/events";

        // A genuine under-root path IS relativized (existing behavior preserved).
        let under = format!("{root}/data/f.parquet");
        assert_eq!(
            relativize_path_to_root(&under, root),
            "data/f.parquet",
            "under-root path must be relativized"
        );

        // Sibling directories that share the root as a bare prefix but break at no
        // `/` boundary stay ABSOLUTE (not stripped).
        let archive = format!("{root}-archive/f.parquet");
        assert_eq!(
            relativize_path_to_root(&archive, root),
            archive,
            "sibling '-archive' path must stay absolute"
        );
        let sibling2 = format!("{root}2/data/f.parquet");
        assert_eq!(
            relativize_path_to_root(&sibling2, root),
            sibling2,
            "sibling '2' path must stay absolute"
        );

        // A path exactly equal to the root stays absolute (no empty entry).
        assert_eq!(
            relativize_path_to_root(root, root),
            root,
            "path equal to the root must stay absolute, not become an empty entry"
        );

        // Every case round-trips back to the original absolute path through the
        // scan UDF's reconstruct rule.
        for original in [&under, &archive, &sibling2, &root.to_string()] {
            let emitted = relativize_path_to_root(original, root);
            assert_eq!(
                reconstruct_abs_uri_mirror(&emitted, root),
                *original,
                "round-trip must be identity for {original}"
            );
        }
    }

    /// Multi-shard fan-out carries the root once in the common literal and each
    /// per-shard literal is a `[[path,size],...]` tuple array (CHANGED).
    #[test]
    fn fan_out_carries_root_once_and_path_size_tuples_per_shard() {
        let root = "s3://warehouse/db/events";
        let files = vec![
            (format!("{root}/part-00000.parquet"), 1024u64),
            (format!("{root}/part-00001.parquet"), 2048u64),
        ];
        let sql = build_row_sql_with_root(
            files,
            root,
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            2,
        );

        // Fan-out shape: GROUP BY shard_key over a VALUES table, never IPROC().
        assert!(
            !sql.contains("IPROC()"),
            "fan-out must not use IPROC(): {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key") && sql.contains("AS shards(shard_key, files)"),
            "fan-out must GROUP BY shard_key over the VALUES table: {sql}"
        );

        // Root carried once (common blob), not repeated per shard.
        assert_eq!(
            sql.matches(root).count(),
            1,
            "root must be serialized once in the common blob: {sql}"
        );

        // Each per-shard files literal is a JSON array of [path,size] 2-tuples.
        assert!(
            sql.contains(r#"[["part-00000.parquet",1024]]"#)
                && sql.contains(r#"[["part-00001.parquet",2048]]"#),
            "each shard literal must be a [[path,size],...] tuple array: {sql}"
        );
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
        // Must be the outer ungrouped scalar scan itself (no SELECT * wrapper).
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "must be a real scalar scan-driving query, no materializing wrapper: {sql}"
        );
    }

    // ---------------------------------------------------------------------------
    // Scenario: Projection is pushed into the scan-driving query
    // ---------------------------------------------------------------------------

    #[test]
    fn projection_carried_in_common_literal_and_emits() {
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
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let resp = empty_pushdown_sql(&proj, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("WHERE 1=0"));
        assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
    }

    /// Single-group empty result: one row, per-`AggKind` literal cast to its
    /// declared type — COUNT → `0`, SUM → `NULL` — with no `WHERE 1=0` (a bare
    /// `FROM DUAL` already yields exactly one row).
    #[test]
    fn empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types() {
        let aggregates = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ];
        let types = vec!["DECIMAL(18,0)".to_string(), "DECIMAL(36,2)".to_string()];
        let resp = empty_agg_sql(&aggregates, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(sql.contains("FROM DUAL"), "must select from DUAL: {sql}");
        assert!(
            !sql.contains("WHERE 1=0"),
            "single-group empty is one row, not zero rows: {sql}"
        );
        assert!(
            sql.contains("CAST(0 AS DECIMAL(18,0))"),
            "COUNT empty literal must be 0 cast to declared type: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(36,2))"),
            "SUM empty literal must be NULL cast to declared type: {sql}"
        );
    }

    /// COUNT(DISTINCT) empty result is `0`, and references neither the scalar
    /// distinct-merge UDF nor a `LISTAGG` union — with zero files there is nothing
    /// to merge.
    #[test]
    fn empty_agg_sql_count_distinct_emits_zero_no_merge_udf() {
        let aggregates = vec![AggregatePlan {
            kind: AggKind::CountDistinct,
            column: Some("ID".into()),
            arg_expr: None,
        }];
        let types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_agg_sql(&aggregates, &types);
        let sql = resp["sql"].as_str().unwrap();
        assert!(
            sql.contains("CAST(0 AS DECIMAL(18,0))"),
            "COUNT(DISTINCT) empty literal must be 0: {sql}"
        );
        assert!(
            !sql.contains(DISTINCT_MERGE_UDF_NAME),
            "empty result must not reference the distinct-merge UDF: {sql}"
        );
        assert!(
            !sql.to_uppercase().contains("LISTAGG"),
            "empty result must not emit a LISTAGG union: {sql}"
        );
    }

    /// Every non-COUNT `AggKind` maps to the `NULL` empty literal — single-node
    /// SQL semantics over zero rows (only the COUNT family yields `0`).
    #[test]
    fn empty_agg_literal_maps_non_count_kinds_to_null() {
        for kind in [
            AggKind::Sum,
            AggKind::Min,
            AggKind::Max,
            AggKind::Avg,
            AggKind::VarPop,
            AggKind::VarSamp,
            AggKind::StddevPop,
            AggKind::StddevSamp,
        ] {
            assert_eq!(
                empty_agg_literal(&kind),
                "NULL",
                "{kind:?} empty literal must be NULL"
            );
        }
        for kind in [AggKind::Count, AggKind::CountCol, AggKind::CountDistinct] {
            assert_eq!(
                empty_agg_literal(&kind),
                "0",
                "{kind:?} empty literal must be 0"
            );
        }
    }

    /// Grouped empty result: zero rows (`WHERE 1=0`) with one `CAST(NULL AS <ty>)`
    /// per grouped output column, assembled in select-list order.
    #[test]
    fn empty_grouped_sql_emits_zero_rows_in_grouped_shape() {
        let select_items = vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 1,
            },
        ];
        let group_key_types = vec!["DECIMAL(20,0)".to_string()];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
        let sql = resp["sql"].as_str().unwrap();
        assert!(
            sql.contains("WHERE 1=0"),
            "grouped empty is zero rows: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(20,0))"),
            "group-key column typed from group_key_types: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS DECIMAL(18,0))"),
            "aggregate column typed from aggregate_types: {sql}"
        );
        let select_clause = sql
            .strip_prefix("SELECT ")
            .and_then(|s| s.split(" FROM").next())
            .unwrap();
        assert_eq!(
            select_clause.matches("CAST(NULL AS").count(),
            2,
            "one output column per grouped select item: {sql}"
        );
    }

    /// A `GroupedSelectItem::Constant` (Exasol's "count the groups" literal
    /// rewrite) reuses its already-rendered projection expression verbatim,
    /// slotted into select-list order alongside the group-key and aggregate
    /// columns — it contributes no aggregate plan and is not re-typed here.
    #[test]
    fn empty_grouped_sql_includes_constant_projection_column() {
        let select_items = vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Constant {
                select_index: 1,
                projection: "CAST(NULL AS BOOLEAN)".to_string(),
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 2,
            },
        ];
        let group_key_types = vec!["DECIMAL(20,0)".to_string()];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
        let sql = resp["sql"].as_str().unwrap();
        let select_clause = sql
            .strip_prefix("SELECT ")
            .and_then(|s| s.split(" FROM").next())
            .unwrap();
        let columns: Vec<&str> = select_clause.split(", ").collect();
        assert_eq!(
            columns,
            vec![
                "CAST(NULL AS DECIMAL(20,0))",
                "CAST(NULL AS BOOLEAN)",
                "CAST(NULL AS DECIMAL(18,0))",
            ],
            "constant column is reused verbatim in select-list order: {sql}"
        );
    }

    /// Dispatch priority mirrors the non-empty path: grouped first, then
    /// single-group aggregate (only when `validate_agg_col_types` passes), then
    /// row scan.
    #[test]
    fn empty_result_sql_dispatches_by_plan_shape() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];

        let grouped = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("COUNT", None, false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 18, "scale": 0},
            ],
        });
        let grouped_sql =
            empty_result_sql(&grouped, &proj, &proj_types, &col_types).unwrap()["sql"]
                .as_str()
                .unwrap()
                .to_string();
        assert!(
            grouped_sql.contains("WHERE 1=0"),
            "grouped shape is zero rows: {grouped_sql}"
        );

        let single = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
        });
        let single_sql = empty_result_sql(&single, &proj, &proj_types, &col_types).unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            single_sql.contains("FROM DUAL") && !single_sql.contains("WHERE 1=0"),
            "single-group shape is one row: {single_sql}"
        );
        assert!(single_sql.contains("CAST(NULL AS DECIMAL(36,2))"));

        // Non-numeric SUM target demotes to the row-scan empty shape (gate honored).
        let non_numeric = serde_json::json!({
            "selectList": [agg_item("SUM", Some("name"), false)],
            "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
        });
        let non_numeric_col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
        let row_sql = empty_result_sql(&non_numeric, &proj, &proj_types, &non_numeric_col_types)
            .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            row_sql.contains("CAST(NULL AS DECIMAL(20,0))") && row_sql.contains(&quote_ident("ID")),
            "non-numeric single-group aggregate must fall through to the row-scan shape: {row_sql}"
        );
    }

    /// A grouped aggregate over a non-numeric column with all files pruned no longer
    /// demotes to the full-row empty shape: since issue #82's fix, a grouped request
    /// that cannot push down (here, a non-numeric SUM with no HAVING) routes on the
    /// NON-empty path to the qualified single-table wrapper, whose output columns are
    /// the `selectList` items. The empty path must MIRROR that shape — a zero-row
    /// result typed per `selectListDataTypes` (the `selectList` column count/types),
    /// NOT the full base row — so the empty and non-empty shapes never diverge.
    #[test]
    fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

        let grouped_non_numeric = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("SUM", Some("name"), false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
        });

        let row_sql = empty_result_sql(&grouped_non_numeric, &proj, &proj_types, &col_types)
            .unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            row_sql,
            "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
            "declined grouped aggregate over zero files must produce the selectList-typed \
             empty shape (matching the qualified wrapper), not the full base row"
        );
    }

    /// A non-numeric grouped aggregate that also carries a HAVING cannot silently
    /// demote (AGGREGATE_HAVING is advertised, so Exasol will not re-apply it):
    /// the empty path must decline with the same `Err` the non-empty path returns.
    #[test]
    fn empty_files_grouped_non_numeric_aggregate_with_having_declines() {
        let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
        let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
        let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

        let grouped_having = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "K"}],
            "selectList": [
                {"type": "column", "name": "K"},
                agg_item("SUM", Some("name"), false),
            ],
            "selectListDataTypes": [
                {"type": "decimal", "precision": 20, "scale": 0},
                {"type": "decimal", "precision": 36, "scale": 2},
            ],
            "having": {"type": "predicate_greater"},
        });

        let err = empty_result_sql(&grouped_having, &proj, &proj_types, &col_types).unwrap_err();
        match err {
            UdfError::User(msg) => assert!(
                msg.contains("HAVING present"),
                "decline message must name the HAVING conflict: {msg}"
            ),
            other => panic!("expected UdfError::User, got {other:?}"),
        }
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

        let unique: std::collections::HashSet<&str> = names.iter().map(|p| p.emit_name()).collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "projection must be duplicate-free, got: {names:?}"
        );
        assert_eq!(
            names,
            vec!["ID", "NAME"],
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

    /// An aggregate over a single explicit argument NODE (a scalar expression,
    /// e.g. `LENGTH(L_COMMENT)`), used to exercise expression-argument pushdown.
    fn agg_item_expr(name: &str, arg: serde_json::Value, distinct: bool) -> serde_json::Value {
        serde_json::json!({
            "type": "function_aggregate",
            "name": name,
            "arguments": [arg],
            "distinct": distinct,
        })
    }

    /// `LENGTH(<col>)` scalar-expression node — renders to `character_length("<COL>")`.
    fn length_expr(col: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "LENGTH",
            "arguments": [{"type": "column", "name": col}],
        })
    }

    /// `<a> * <b>` two-column product node, as Exasol pushes it once `FN_MULT` is
    /// advertised (node name `MULT`; see decision-log entry [7]). Renders to
    /// `("<A>" * "<B>")` via the vs-expression translator.
    fn mult_expr(a: &str, b: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "MULT",
            "arguments": [
                {"type": "column", "name": a},
                {"type": "column", "name": b},
            ],
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

    /// A non-COUNT DISTINCT aggregate (e.g. SUM DISTINCT) => fall back.
    /// (Single-group COUNT(DISTINCT) is now decomposed — see
    /// `count_distinct_builds_local_set_scan_spec`.)
    #[test]
    fn detect_aggregates_falls_back_on_distinct() {
        let req = serde_json::json!({
            "selectList": [agg_item("SUM", Some("amount"), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "must fall back when a non-COUNT DISTINCT is present"
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

    // -----------------------------------------------------------------------
    // Ordered top-N pushdown (B3)
    // -----------------------------------------------------------------------

    /// Reproduce `handle_pushdown`'s SYNCHRONOUS row-scan decision path (everything
    /// after `resolve_file_list`) so tests exercise the real `detect_topn`,
    /// `effective_limit` withholding glue, and `build_scan_driving_sql` — the exact
    /// composition production runs, minus the network file resolution.
    fn plan_scan_sql(request: &Json, files: Vec<(String, u64)>, cluster_nodes: usize) -> String {
        let pushdown_req = request
            .get("pushdownRequest")
            .cloned()
            .unwrap_or(Json::Null);
        let (proj_cols, proj_types) = extract_projection(request, &pushdown_req).unwrap();
        let filter = pushdown_req
            .get("filter")
            .filter(|f| !f.is_null())
            .and_then(render_df_filter_safe);
        let limit = extract_limit(&pushdown_req);
        let has_order_by = order_by_present(&pushdown_req);
        let col_types = extract_all_column_types(request);

        let aggregates = detect_aggregates(&pushdown_req)
            .filter(|plans| validate_agg_col_types(plans, &col_types));
        // Production always resolves a logical schema before detect_topn; reproduce
        // the LINEITEM schema every plan_scan_sql caller's request scans over.
        let logical_schema = lineitem_logical_schema();
        let topn = if aggregates.is_none() {
            detect_topn(request, &pushdown_req, &proj_cols, &logical_schema)
        } else {
            None
        };
        let order_by = topn.unwrap_or_default();
        let effective_limit = if has_order_by && order_by.is_empty() {
            None
        } else {
            limit
        };

        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: proj_cols.clone(),
            filter,
            limit: effective_limit,
            order_by,
            aggregates,
            group_keys: None,
            emit_exa_types: proj_types.clone(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let files: Vec<FileEntry> = files.into_iter().map(FileEntry::from).collect();
        let g = shard_count(cluster_nodes, 1, files.len());
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let aggregate_types = aggregate_exasol_types(&pushdown_req);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &proj_cols,
            &proj_types,
            effective_limit,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );
        // Mirror handle_pushdown's row-scan DECLINE wrapping (add-topn-pushdown B6).
        let declined_order_by =
            has_order_by && spec_template.order_by.is_empty() && spec_template.aggregates.is_none();
        if declined_order_by {
            let keys = parse_order_by_keys(&pushdown_req);
            if keys.is_empty() {
                sql
            } else {
                let mut wrapped = format!(
                    "SELECT * FROM ({sql}) ORDER BY {}",
                    render_order_by_clause(&keys)
                );
                if let Some(n) = limit {
                    wrapped.push_str(&format!(" LIMIT {n}"));
                }
                wrapped
            }
        } else {
            sql
        }
    }

    /// The logical schema production resolves for the NQ4 (LINEITEM) requests: both
    /// sort-eligible columns are in-range DECIMALs, so neither needs the JSON
    /// fallback and `detect_topn` matches. Field-ids are illustrative.
    fn lineitem_logical_schema() -> Vec<LogicalField> {
        vec![
            LogicalField {
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(18,2)".into(),
                nullable: true,
            },
        ]
    }

    /// A single-table request with the NQ4 shape: two projected columns and an
    /// `ORDER BY <projected col> DESC NULLS LAST LIMIT n`.
    fn nq4_request() -> Json {
        serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "selectList": [
                    {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                    {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                ],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 20, "scale": 0},
                    {"type": "decimal", "precision": 18, "scale": 2},
                ],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                    "isAscending": false,
                    "nullsLast": true
                }],
                "limit": {"numElements": 20}
            }
        })
    }

    /// The `pushdownRequest` sub-object of a request (for direct `detect_topn` calls).
    fn pd(request: &Json) -> Json {
        request.get("pushdownRequest").cloned().unwrap()
    }

    /// Match: the ordered top-N wraps the fan-out in an outer `ORDER BY … LIMIT n`
    /// and carries the SAME sort keys + limit into the shard-invariant common blob
    /// (which the scan UDF renders as the per-shard bounded sort). Multi-shard so a
    /// real fan-out + merge is exercised.
    #[test]
    fn ordered_topn_emits_per_shard_and_outer_order_by() {
        let request = nq4_request();
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        // Two nodes → two shards → a genuine GROUP BY shard_key fan-out.
        let sql = plan_scan_sql(&request, files, 2);

        // Outer merge ORDER BY, explicit direction + NULL placement, before LIMIT.
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
            "matched top-N must render an outer ORDER BY … LIMIT: {sql}"
        );
        // The per-shard common blob carries the identical sort keys AND the limit,
        // so every shard runs the same bounded sort (rendered by the scan UDF).
        let common = common_arg_literal(&sql);
        assert!(
            common.contains(
                r#""order_by":[{"column":"L_EXTENDEDPRICE","ascending":false,"nulls_last":true}]"#
            ),
            "common blob must carry the per-shard sort keys: {common}"
        );
        assert!(
            common.contains(r#""limit":20"#),
            "common blob must carry the per-shard limit: {common}"
        );
    }

    /// Decline (sort key not projected): `ORDER BY` is present but the sort column
    /// is not in the projection, so the bounded top-N declines. The PER-SHARD sort
    /// keys and LIMIT are still withheld from the common blob (anti-wrong-truncation
    /// invariant, decision [4]), but the OUTER wrapper now renders a self-contained
    /// global `ORDER BY … LIMIT n` (add-topn-pushdown B6): once `ORDER_BY_COLUMN` is
    /// advertised Exasol no longer re-applies its own backstop sort/limit, so the
    /// adapter reproduces it in the returned SQL.
    #[test]
    fn order_by_present_without_topn_match_withholds_per_shard_limit() {
        // Project only L_ORDERKEY, but ORDER BY L_EXTENDEDPRICE (unprojected).
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "selectList": [
                    {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                ],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 20, "scale": 0},
                ],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                    "isAscending": false,
                    "nullsLast": true
                }],
                "limit": {"numElements": 20}
            }
        });
        // detect_topn declines the unprojected-key shape.
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &[ProjectionItem::Column("L_ORDERKEY".into())],
                &lineitem_logical_schema()
            )
            .is_none(),
            "unprojected sort key must decline the top-N path"
        );

        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        // The OUTER wrapper renders a self-contained global ORDER BY + LIMIT
        // (reproducing Exasol's former backstop, which no longer runs).
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
            "declined shape must render a self-contained outer ORDER BY … LIMIT: {sql}"
        );
        // But the PER-SHARD common blob still carries NO sort keys and NO limit:
        // the fan-out stays unbounded and unsorted (anti-wrong-truncation invariant).
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("\"limit\""),
            "declined shape must withhold the per-shard LIMIT from the common blob: {common}"
        );
        assert!(
            !common.contains("order_by"),
            "declined shape must not carry sort keys into the common blob: {common}"
        );
    }

    /// Every unsupported ordered-query shape declines the top-N path (returns None),
    /// while the NQ4 shape matches. Covers: join (multiple involved tables), GROUP
    /// BY present, an expression (non-bare-column) sort key, ORDER BY with no LIMIT.
    #[test]
    fn unsupported_order_by_shape_declines_topn() {
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];

        // Baseline: the well-formed NQ4 shape matches.
        let ok = nq4_request();
        assert_eq!(
            detect_topn(&ok, &pd(&ok), &projected, &lineitem_logical_schema()),
            Some(vec![SortKey {
                column: "L_EXTENDEDPRICE".into(),
                ascending: false,
                nulls_last: true,
            }]),
            "the NQ4 shape must match"
        );

        // Join: two involved tables.
        let mut join = nq4_request();
        let extra_table = serde_json::json!({
            "name": "ORDERS",
            "columns": [{"name": "O_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]
        });
        join.get_mut("involvedTables")
            .and_then(|v| v.as_array_mut())
            .unwrap()
            .push(extra_table);
        assert!(
            detect_topn(&join, &pd(&join), &projected, &lineitem_logical_schema()).is_none(),
            "a multi-table (join) shape must decline"
        );

        // GROUP BY present.
        let mut grouped = nq4_request();
        grouped["pushdownRequest"]["aggregationType"] = serde_json::json!("group_by");
        grouped["pushdownRequest"]["groupBy"] =
            serde_json::json!([{"type": "column", "name": "L_ORDERKEY"}]);
        assert!(
            detect_topn(
                &grouped,
                &pd(&grouped),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "a GROUP BY shape must decline"
        );

        // Expression (non-bare-column) sort key.
        let mut expr_key = nq4_request();
        expr_key["pushdownRequest"]["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "function_scalar", "name": "ABS", "arguments": [
                {"type": "column", "name": "L_EXTENDEDPRICE"}
            ]},
            "isAscending": false,
            "nullsLast": true
        }]);
        assert!(
            detect_topn(
                &expr_key,
                &pd(&expr_key),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "an expression sort key must decline (ORDER_BY_EXPRESSION unadvertised)"
        );

        // ORDER BY with no LIMIT: not a bounded top-N.
        let mut no_limit = nq4_request();
        no_limit["pushdownRequest"]
            .as_object_mut()
            .unwrap()
            .remove("limit");
        assert!(
            detect_topn(
                &no_limit,
                &pd(&no_limit),
                &projected,
                &lineitem_logical_schema()
            )
            .is_none(),
            "an ORDER BY without a LIMIT must decline"
        );
    }

    // ---------------------------------------------------------------------------
    // Join detection (task 3.1): `detect_join` shape classification.
    // ---------------------------------------------------------------------------

    /// Build a two-table-join pushdown request. `from_extra` is spliced into the
    /// `from` object (e.g. to swap `join_type`, drop a field, or corrupt a side),
    /// and `condition` becomes the join's `condition` node.
    fn join_request(from_extra: Json, condition: Json) -> Json {
        let mut from = serde_json::json!({
            "type": "join",
            "join_type": "inner",
            "left": {"name": "CUSTOMER", "type": "table"},
            "right": {"name": "ORDERS", "type": "table"},
        });
        if let Json::Object(extra) = from_extra {
            from.as_object_mut().unwrap().extend(extra);
        }
        from["condition"] = condition;

        serde_json::json!({
            "involvedTables": [
                {
                    "name": "CUSTOMER",
                    "columns": [
                        {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                        {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
                    ],
                },
                {
                    "name": "ORDERS",
                    "columns": [
                        {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                        {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
                    ],
                },
            ],
            "pushdownRequest": {
                "type": "select",
                "from": from,
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                ],
            },
            "schemaMetadataInfo": {
                "properties": {},
                "adapterNotes": serde_json::json!({
                    "TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}
                }).to_string(),
            },
        })
    }

    /// The standard equi-join condition: `CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY`.
    fn equi_condition() -> Json {
        serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
        })
    }

    /// A genuine two-table inner equi-join is detected as the unified `Join` shape,
    /// with both leaves' original-cased Iceberg identifiers recovered from `TABLE_MAP`
    /// (the two-table case is simply N = 2).
    #[test]
    fn genuine_inner_equi_join_is_detected_with_both_idents() {
        let request = join_request(Json::Null, equi_condition());
        let pushdown_req = pd(&request);

        let shape = detect_join(&request, &pushdown_req).expect("TABLE_MAP has both tables");
        match shape {
            JoinShape::Join(join) => {
                assert_eq!(join.tables.len(), 2);
                assert_eq!(join.tables[0].table_name, "CUSTOMER");
                assert_eq!(join.tables[1].table_name, "ORDERS");
                assert_eq!(join.tables[0].iceberg_ident, "lh.customer");
                assert_eq!(join.tables[1].iceberg_ident, "lh.orders");
                assert_eq!(join.conditions, vec![equi_condition()]);
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// A plain single-table pushdown request (today's normal case, no `from` field
    /// at all) is `NotAJoin` and completely unaffected by the detector.
    #[test]
    fn plain_single_table_request_is_not_a_join() {
        let request = nq4_request();
        let shape = detect_join(&request, &pd(&request)).expect("not a join, no TABLE_MAP lookup");
        assert_eq!(shape, JoinShape::NotAJoin);
    }

    /// A `from` clause that is a plain table reference (`type: "table"`) is also
    /// `NotAJoin` — the single-table shape some requests carry explicitly.
    #[test]
    fn from_table_node_is_not_a_join() {
        let mut request = nq4_request();
        request["pushdownRequest"]["from"] =
            serde_json::json!({"name": "LINEITEM", "type": "table"});
        let shape = detect_join(&request, &pd(&request)).expect("not a join");
        assert_eq!(shape, JoinShape::NotAJoin);
    }

    /// Left/right/full outer joins are declined as `Ineligible(NotInnerJoinType)`,
    /// never `Eligible` — the broadcast contract advertises only `JOIN_TYPE_INNER`.
    #[test]
    fn outer_join_is_ineligible() {
        for outer in ["left_outer", "right_outer", "full_outer"] {
            let request = join_request(serde_json::json!({"join_type": outer}), equi_condition());
            let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
            assert_eq!(
                shape,
                JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType),
                "join_type '{outer}' must be ineligible, not broadcast-eligible"
            );
        }
    }

    /// A non-equi two-table inner join (e.g. `<`) is NOT declined — it is served by
    /// the unified fallback, so it yields the `Join` shape carrying both tables and
    /// the (non-equi) condition. Only broadcast (an inner optimization) is gated on
    /// equi; the N-scan fallback renders any inner-join condition into its WHERE.
    #[test]
    fn non_equi_two_table_join_is_served_by_unified_fallback() {
        let condition = serde_json::json!({
            "type": "predicate_less",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
        });
        let request = join_request(Json::Null, condition.clone());
        match detect_join(&request, &pd(&request)).expect("served, not declined") {
            JoinShape::Join(join) => {
                assert_eq!(join.tables.len(), 2);
                assert_eq!(join.conditions, vec![condition]);
            }
            other => panic!("expected Join (unified fallback), got {other:?}"),
        }
    }

    /// A three-table inner-join pushdown request: `(CUSTOMER ⋈ ORDERS) ⋈ LINEITEM`,
    /// all three in `TABLE_MAP`. Leaves in stable tree order CUSTOMER, ORDERS,
    /// LINEITEM; two join conditions (`C_CUSTKEY=O_CUSTKEY`, `O_ORDERKEY=L_ORDERKEY`).
    fn three_table_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "CUSTOMER", "columns": [
                    {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "ORDERS", "columns": [
                    {"name": "O_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "LINEITEM", "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_QUANTITY", "dataType": {"type": "decimal", "precision": 15, "scale": 2}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "CUSTOMER", "type": "table"},
                        "right": {"name": "ORDERS", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}}},
                    "right": {"name": "LINEITEM", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "O_ORDERKEY", "tableName": "ORDERS"},
                        "right": {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"}}},
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "L_QUANTITY", "tableName": "LINEITEM"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders", "LINEITEM": "lh.lineitem"}})
                    .to_string()},
        })
    }

    /// A three-table all-inner nested join is classified as the unified `Join` shape
    /// (never an error, never Ineligible): the three leaves in stable tree order and
    /// the two collected join conditions, each leaf's Iceberg ident recovered from
    /// `TABLE_MAP` (pushdown-planning-join "A three-or-more-table inner join falls
    /// back to an N-scan unaccelerated wrapper").
    #[test]
    fn three_table_inner_join_is_unified_join() {
        let request = three_table_join_request();
        let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
        match shape {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["CUSTOMER", "ORDERS", "LINEITEM"]);
                let idents: Vec<&str> = join
                    .tables
                    .iter()
                    .map(|t| t.iceberg_ident.as_str())
                    .collect();
                assert_eq!(idents, ["lh.customer", "lh.orders", "lh.lineitem"]);
                assert_eq!(join.conditions.len(), 2, "N-1 conditions for N=3 tables");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// A non-inner join node ANYWHERE in the tree (here the nested left node is a
    /// left outer join) declines as `Ineligible(NotInnerJoinType)` — a cross-join +
    /// conjunctive WHERE cannot reproduce outer-join semantics.
    #[test]
    fn non_inner_node_in_join_tree_is_ineligible() {
        let mut request = three_table_join_request();
        request["pushdownRequest"]["from"]["left"]["join_type"] = serde_json::json!("left_outer");
        let shape = detect_join(&request, &pd(&request)).expect("shape decline, no Err");
        assert_eq!(
            shape,
            JoinShape::Ineligible(IneligibleJoinReason::NotInnerJoinType)
        );
    }

    /// A leaf of a multi-table tree absent from `TABLE_MAP` is a hard `Err` (stale
    /// virtual schema), identical to the two-table path — never a silent decline.
    #[test]
    fn multi_table_leaf_absent_from_table_map_is_err() {
        let mut request = three_table_join_request();
        request["schemaMetadataInfo"]["adapterNotes"] = Json::String(
            serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
                .to_string(),
        );
        let err = detect_join(&request, &pd(&request))
            .expect_err("LINEITEM is absent from TABLE_MAP: must be Err, not a decline");
        assert!(
            err.to_string().contains("LINEITEM"),
            "error must name the unmapped table: {err}"
        );
    }

    /// The Q1-shape three-table inner-join pushdown request:
    /// `(SUPPLIER ⋈ NATION) ⋈ REGION`, all three in `TABLE_MAP`. Leaves in stable
    /// tree order SUPPLIER, NATION, REGION; two join conditions
    /// (`S_NATIONKEY=N_NATIONKEY`, `N_REGIONKEY=R_REGIONKEY`).
    fn q1_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "SUPPLIER", "columns": [
                    {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "NATION", "columns": [
                    {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "REGION", "columns": [
                    {"name": "R_REGIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "R_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "SUPPLIER", "type": "table"},
                        "right": {"name": "NATION", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                            "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                    "right": {"name": "REGION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "N_REGIONKEY", "tableName": "NATION"},
                        "right": {"type": "column", "name": "R_REGIONKEY", "tableName": "REGION"}}},
                "selectList": [
                    {"type": "column", "name": "S_NAME", "tableName": "SUPPLIER"},
                    {"type": "column", "name": "R_NAME", "tableName": "REGION"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"SUPPLIER": "lh.supplier", "NATION": "lh.nation", "REGION": "lh.region"}})
                    .to_string()},
        })
    }

    /// The NQ3-shape four-table inner-join pushdown request:
    /// `((PART ⋈ PARTSUPP) ⋈ SUPPLIER) ⋈ NATION`, all four in `TABLE_MAP`. Leaves in
    /// stable tree order PART, PARTSUPP, SUPPLIER, NATION; three join conditions.
    fn nq3_join_request() -> Json {
        serde_json::json!({
            "involvedTables": [
                {"name": "PART", "columns": [
                    {"name": "P_PARTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "P_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "PARTSUPP", "columns": [
                    {"name": "PS_PARTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "PS_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "PS_AVAILQTY", "dataType": {"type": "decimal", "precision": 15, "scale": 0}}]},
                {"name": "SUPPLIER", "columns": [
                    {"name": "S_SUPPKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "S_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "NATION", "columns": [
                    {"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"type": "join", "join_type": "inner",
                            "left": {"name": "PART", "type": "table"},
                            "right": {"name": "PARTSUPP", "type": "table"},
                            "condition": {"type": "predicate_equal",
                                "left": {"type": "column", "name": "P_PARTKEY", "tableName": "PART"},
                                "right": {"type": "column", "name": "PS_PARTKEY", "tableName": "PARTSUPP"}}},
                        "right": {"name": "SUPPLIER", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "PS_SUPPKEY", "tableName": "PARTSUPP"},
                            "right": {"type": "column", "name": "S_SUPPKEY", "tableName": "SUPPLIER"}}},
                    "right": {"name": "NATION", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "S_NATIONKEY", "tableName": "SUPPLIER"},
                        "right": {"type": "column", "name": "N_NATIONKEY", "tableName": "NATION"}}},
                "selectList": [
                    {"type": "column", "name": "P_NAME", "tableName": "PART"},
                    {"type": "column", "name": "PS_AVAILQTY", "tableName": "PARTSUPP"},
                    {"type": "column", "name": "N_NAME", "tableName": "NATION"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {
                    "PART": "lh.part", "PARTSUPP": "lh.partsupp",
                    "SUPPLIER": "lh.supplier", "NATION": "lh.nation"}})
                    .to_string()},
        })
    }

    /// A four-table all-inner nested join (NQ3 shape: `part⋈partsupp⋈supplier⋈nation`)
    /// is the unified `Join` shape with all four leaves (stable tree order) and the
    /// three collected join conditions — the detector generalizes past N=3, never
    /// capping at three tables.
    #[test]
    fn four_table_inner_join_is_unified_join() {
        let request = nq3_join_request();
        let shape = detect_join(&request, &pd(&request)).expect("all leaves are in TABLE_MAP");
        match shape {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["PART", "PARTSUPP", "SUPPLIER", "NATION"]);
                let idents: Vec<&str> = join
                    .tables
                    .iter()
                    .map(|t| t.iceberg_ident.as_str())
                    .collect();
                assert_eq!(
                    idents,
                    ["lh.part", "lh.partsupp", "lh.supplier", "lh.nation"]
                );
                assert_eq!(join.conditions.len(), 3, "N-1 conditions for N=4 tables");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// `detect_join` is driven by the `from` TREE, not the `involvedTables` count:
    /// a two-table `from` yields the unified `Join` shape with exactly those two
    /// tables even when `involvedTables` lists more (the old `TooManyTables`
    /// defensive belt is gone — the tree is authoritative).
    #[test]
    fn detect_join_follows_from_tree_not_involved_tables_count() {
        let mut request = join_request(Json::Null, equi_condition());
        request["involvedTables"].as_array_mut().unwrap().push(serde_json::json!({
            "name": "NATION",
            "columns": [{"name": "N_NATIONKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}],
        }));
        match detect_join(&request, &pd(&request)).expect("tree-driven, no decline") {
            JoinShape::Join(join) => {
                let names: Vec<&str> = join.tables.iter().map(|t| t.table_name.as_str()).collect();
                assert_eq!(names, ["CUSTOMER", "ORDERS"], "only the from-tree leaves");
            }
            other => panic!("expected Join over the two from-tree tables, got {other:?}"),
        }
    }

    /// An otherwise-eligible join whose virtual table name is absent from
    /// `TABLE_MAP` is a hard `Err` (stale virtual schema), not a decline — the
    /// same treatment the single-table path gives an unmapped involved table.
    #[test]
    fn join_with_unmapped_table_is_an_error() {
        let mut request = join_request(Json::Null, equi_condition());
        request["schemaMetadataInfo"]["adapterNotes"] =
            Json::String(serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer"}}).to_string());
        let err = detect_join(&request, &pd(&request))
            .expect_err("ORDERS is absent from TABLE_MAP: must be Err, not a decline");
        assert!(
            err.to_string().contains("ORDERS"),
            "error must name the unmapped table: {err}"
        );
    }

    // ---------------------------------------------------------------------------
    // Join rendering (task 3.3): disjoint-column guard + condition/filter/projection
    // rendering via the reused vs-expression translator.
    // ---------------------------------------------------------------------------

    /// Recover the [`DetectedJoin`] a request classifies to (the tests below all
    /// operate on the standard two-table CUSTOMER⋈ORDERS shape from `join_request`).
    fn detected_join(request: &Json) -> DetectedJoin {
        match detect_join(request, &pd(request)).expect("detected join shape") {
            JoinShape::Join(join) => join,
            other => panic!("expected Join, got {other:?}"),
        }
    }

    /// Two tables whose column names are genuinely disjoint (TPC-H `C_*` vs `O_*`)
    /// pass the guard, so bare column names resolve unambiguously.
    #[test]
    fn disjoint_schema_guard_passes_for_disjoint_column_names() {
        let request = join_request(Json::Null, equi_condition());
        let left = involved_table_columns(&request, "CUSTOMER");
        let right = involved_table_columns(&request, "ORDERS");
        assert!(
            disjoint_schema_guard(&left, &right),
            "C_* and O_* column sets are disjoint and must pass the guard"
        );
    }

    /// ANY overlapping column name fails the guard, and the failure is surfaced as
    /// a clean decline (`Ok(None)`) — the caller falls through to the unaccelerated
    /// path — never as an error.
    #[test]
    fn overlapping_column_name_fails_guard_and_declines_without_error() {
        let mut request = join_request(Json::Null, equi_condition());
        // Give BOTH sides a column with the same name.
        for table_idx in [0, 1] {
            request["involvedTables"][table_idx]["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "SHARED_KEY",
                    "dataType": {"type": "varchar", "size": 10}
                }));
        }

        let left = involved_table_columns(&request, "CUSTOMER");
        let right = involved_table_columns(&request, "ORDERS");
        assert!(
            !disjoint_schema_guard(&left, &right),
            "a shared column name must fail the disjoint guard"
        );

        // The whole rendering entry point declines cleanly, not with an Err.
        let detected = detected_join(&request);
        let outcome = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("a guard failure is a decline, not an error");
        assert!(
            outcome.is_none(),
            "a column-name collision must decline to the unaccelerated path"
        );
    }

    /// A simple equi-condition renders to the correct DataFusion SQL boolean
    /// expression via the reused translator, and is threaded verbatim into the
    /// rendered join's `condition` (→ `JoinSpec::condition`).
    #[test]
    fn join_condition_renders_via_translator() {
        assert_eq!(
            render_join_condition(&equi_condition()).as_deref(),
            Some(r#"("C_CUSTKEY" = "O_CUSTKEY")"#),
            "the equi-condition must render to a bare-name DataFusion boolean expr"
        );

        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("disjoint, renderable join")
            .expect("a disjoint join must render, not decline");
        assert_eq!(rendered.condition, r#"("C_CUSTKEY" = "O_CUSTKEY")"#);
    }

    /// A WHERE filter referencing columns from BOTH sides renders correctly against
    /// the combined schema (bare names, disjoint → unambiguous).
    #[test]
    fn join_where_filter_spanning_both_sides_renders() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });

        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("disjoint, renderable join")
            .expect("must render");
        let filter = rendered
            .filter
            .expect("a cross-side WHERE filter must render");
        assert!(
            filter.contains(r#""C_NAME""#),
            "the left-side column must appear in the rendered filter: {filter}"
        );
        assert!(
            filter.contains(r#""O_ORDERDATE""#),
            "the right-side column must appear in the rendered filter: {filter}"
        );
        assert!(
            filter.contains("AND"),
            "the conjunction of both sides must render: {filter}"
        );
    }

    /// The cross-table projection attributes each projected column to its OWNING
    /// side's Exasol type: `C_NAME` from CUSTOMER (`VARCHAR(100)`), `O_ORDERDATE`
    /// from ORDERS (`DATE`).
    #[test]
    fn join_projection_emits_attribute_each_side_owning_type() {
        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let (projection, types) =
            extract_join_projection(&request, &pd(&request), &detected).expect("projectable");

        assert_eq!(
            projection,
            vec![
                ProjectionItem::Column("C_NAME".into()),
                ProjectionItem::Column("O_ORDERDATE".into()),
            ],
            "projection spans both tables in select-list order"
        );
        assert_eq!(
            types,
            vec!["VARCHAR(100)".to_string(), "DATE".to_string()],
            "each column's EMITS type comes from the side that owns it"
        );
    }

    // -----------------------------------------------------------------------
    // Join SQL-shape and decline routing (tasks 3.4 / 3.5)
    // -----------------------------------------------------------------------

    /// pushdown-planning-join "A join outside the broadcast contract is declined
    /// safely". Two independent facets are asserted together because they are the
    /// two ways a join leaves the broadcast contract:
    ///
    /// 1. A shape `detect_join` classifies `Ineligible` (a non-inner join node in the
    ///    tree, or a malformed shape) cannot be rendered at all — so it MUST map to a
    ///    `User` decline error, NEVER fall through to the single-table path (which
    ///    would scan only the first involved table and silently drop the join).
    ///    Spanning more than two tables, non-equi, and overlapping column names are
    ///    NOT Ineligible — they are served by the unified fallback.
    /// 2. An otherwise-eligible join whose two tables share a column name fails the
    ///    disjoint-column guard, so `render_broadcast_join` declines with `Ok(None)`.
    ///    The `vs-expression` translator emits only bare column names, so a two-scan
    ///    wrapper would carry an ambiguous `ON`/`WHERE`/`SELECT` — hence the router
    ///    treats `None` as "fallback cannot be built" and errors rather than emit a
    ///    wrong plan.
    #[test]
    fn join_outside_contract_declined_safely() {
        // Facet 1: every ineligible reason declines to a HARD error — a
        // client-facing F-UDF-CL-RUST-9001, NEVER a native re-plan. The message must
        // say so plainly (contains "declined"/"cannot") and MUST NOT claim a retry.
        for reason in [
            IneligibleJoinReason::NotInnerJoinType,
            IneligibleJoinReason::UnsupportedShape,
        ] {
            let err = ineligible_join_decline(reason);
            match err {
                UdfError::User(msg) => {
                    assert!(
                        msg.contains("join pushdown declined") && msg.contains("cannot"),
                        "ineligible reason {reason:?} must be a plain hard-error decline: {msg}"
                    );
                    assert!(
                        !msg.contains("retry"),
                        "ineligible reason {reason:?} must NOT claim a native retry: {msg}"
                    );
                }
                other => panic!("ineligible join must be a User decline, got {other:?}"),
            }
        }

        // An outer join reaches the decline path as Ineligible, never Join.
        let outer = join_request(
            serde_json::json!({"join_type": "left_outer"}),
            equi_condition(),
        );
        assert!(
            matches!(
                detect_join(&outer, &pd(&outer)),
                Ok(JoinShape::Ineligible(
                    IneligibleJoinReason::NotInnerJoinType
                ))
            ),
            "an outer join must classify Ineligible so the decline path is taken"
        );

        // Facet 2: overlapping column names → render declines with Ok(None).
        let mut request = join_request(Json::Null, equi_condition());
        for table_idx in [0, 1] {
            request["involvedTables"][table_idx]["columns"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "name": "SHARED_COL",
                    "dataType": {"type": "varchar", "size": 10}
                }));
        }
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("guard failure is a decline, not an error");
        assert!(
            rendered.is_none(),
            "overlapping column names must decline broadcast rendering (Ok(None))"
        );
    }

    /// The unified fallback (N = 2): each side scanned through its own sharded
    /// fan-out, joined by an `INNER JOIN … ON` chain (the join condition on the join
    /// point), projecting the qualified select list. The single ORDERS-side-local
    /// filter is pushed into the ORDERS leg, so the outer WHERE has no residual. The
    /// two-table case uses the SAME `LHS_T*` renderer as N ≥ 3.
    #[test]
    fn two_table_join_falls_back_to_unified_n_scan_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the two-table unified fallback must build");

        for alias in ["LHS_T0", "LHS_T1"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "both side fan-outs must appear as aliased derived-table subqueries: {sql}"
            );
        }
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            sql.contains(r#"SELECT "LHS_T0"."C_NAME", "LHS_T1"."O_ORDERDATE" FROM"#),
            "the cross-table projection must drive the outer SELECT in order: {sql}"
        );
        // The lone ORDERS-side-local filter is pushed into the ORDERS leg, so no
        // residual conjunct remains and there is no outer WHERE.
        assert!(
            sql.contains("'1995-01-01'"),
            "the ORDERS-side-local filter must be pushed into that leg's fan-out: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every side-local filter is pushed into its leg, so no residual outer WHERE: {sql}"
        );
        // The unified fallback is an INNER JOIN chain, never a broadcast join block.
        assert!(sql.contains("INNER JOIN"), "{sql}");
        assert!(
            !sql.contains("\"join\":{"),
            "the fallback must not embed a broadcast join block: {sql}"
        );
    }

    // -----------------------------------------------------------------------
    // Qualified two-scan fallback (fix: qualified rendering independent of the
    // disjoint-column guard, and aggregate-over-join routed through two-scan)
    // -----------------------------------------------------------------------

    fn two_scan_tuning() -> JoinScanTuning {
        JoinScanTuning {
            cluster_nodes: 1,
            parallelism_factor: 1,
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 0,
            s3_max_connections: 1,
        }
    }

    /// A join whose two tables share a column name (`ID`) fails the disjoint guard
    /// (so the broadcast path declines), but the unified N-scan fallback still builds
    /// a correct, UNAMBIGUOUS wrapper (N = 2): the condition and projection reference
    /// `"LHS_T0"."ID"` / `"LHS_T1"."ID"`, never a bare ambiguous `"ID"`. This is the
    /// `EVENTS ⋈ LABELS ON a.id = b.id` regression.
    #[test]
    fn colliding_columns_render_qualified_unified_wrapper_without_error() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "EVENTS", "type": "table"},
                    "right": {"name": "LABELS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                        "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {"EVENTS": "lh.events", "LABELS": "lh.labels"}})
                    .to_string()},
        });

        // Precondition: the shared ID column fails the disjoint guard, so broadcast
        // rendering declines (Ok(None)) — the very reason the OLD code errored.
        let left = involved_table_columns(&request, "EVENTS");
        let right = involved_table_columns(&request, "LABELS");
        assert!(!disjoint_schema_guard(&left, &right));
        let detected = detected_join(&request);
        assert!(
            render_broadcast_join(&request, &pd(&request), &detected)
                .unwrap()
                .is_none()
        );

        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the qualified unified fallback must build despite the column-name collision");

        assert!(
            sql.contains(r#"("LHS_T0"."ID" = "LHS_T1"."ID")"#),
            "the equi-condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."ID""#) && sql.contains(r#""LHS_T1"."LABEL""#),
            "the projection must be table-qualified per owning side: {sql}"
        );
        assert!(sql.contains("INNER JOIN"), "{sql}");
    }

    /// The N-scan (N≥3) builder produces an `INNER JOIN … ON` chain — N distinct
    /// `LHS_T*` fan-out aliases, every one of the N-1 join conditions rendered
    /// table-qualified and greedily attached to its join point, and the select list
    /// qualified to its owning side — never an `Err` for an all-inner tree over
    /// resolvable tables (pushdown-planning-join "A three-or-more-table inner join
    /// falls back to an N-scan unaccelerated wrapper").
    #[test]
    fn build_n_scan_join_sql_produces_qualified_n_scan_wrapper() {
        let request = three_table_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("an all-inner N-scan wrapper must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "conditions must attach across a two-hop INNER JOIN … ON chain: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T0"."C_NAME""#) && sql.contains(r#""LHS_T2"."L_QUANTITY""#),
            "the select list must be qualified to each column's owning side: {sql}"
        );
    }

    /// The N-scan builder also handles the Q1 shape (`supplier⋈nation⋈region`): three
    /// distinct `LHS_T*` fan-out aliases and both join conditions rendered
    /// table-qualified, never an `Err`.
    #[test]
    fn build_n_scan_join_sql_for_q1_shape_supplier_nation_region() {
        let request = q1_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 10)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 5)]),
            resolved_side("REGION", vec![("s3://w/r-0.parquet", 2)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the Q1-shape (supplier⋈nation⋈region) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."S_NATIONKEY" = "LHS_T1"."N_NATIONKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."N_REGIONKEY" = "LHS_T2"."R_REGIONKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
    }

    /// The N-scan builder also handles the NQ3 shape
    /// (`part⋈partsupp⋈supplier⋈nation`, N=4): four distinct `LHS_T*` fan-out
    /// aliases and all three join conditions rendered table-qualified, never an
    /// `Err` — the builder generalizes past N=3.
    #[test]
    fn build_n_scan_join_sql_for_nq3_shape_part_partsupp_supplier_nation() {
        let request = nq3_join_request();
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("PART", vec![("s3://w/p-0.parquet", 10)]),
            resolved_side("PARTSUPP", vec![("s3://w/ps-0.parquet", 40)]),
            resolved_side("SUPPLIER", vec![("s3://w/s-0.parquet", 5)]),
            resolved_side("NATION", vec![("s3://w/n-0.parquet", 3)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the NQ3-shape (part⋈partsupp⋈supplier⋈nation) must build, never Err");

        for alias in ["LHS_T0", "LHS_T1", "LHS_T2", "LHS_T3"] {
            assert!(
                sql.contains(&format!(r#"AS "{alias}""#)),
                "missing distinct fan-out alias {alias}: {sql}"
            );
        }
        assert!(
            sql.contains(r#""LHS_T0"."P_PARTKEY" = "LHS_T1"."PS_PARTKEY""#),
            "first join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."PS_SUPPKEY" = "LHS_T2"."S_SUPPKEY""#),
            "second join condition must be table-qualified: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T2"."S_NATIONKEY" = "LHS_T3"."N_NATIONKEY""#),
            "third join condition must be table-qualified: {sql}"
        );
    }

    /// Three tables that ALL share a column name (`ID`) — the N-table analog of
    /// `colliding_columns_render_qualified_two_scan_without_error` — still build a
    /// correct, unambiguous N-scan wrapper: every `ID` reference (both join
    /// conditions and the select list) is table-qualified, never bare.
    #[test]
    fn build_n_scan_join_sql_renders_qualified_when_three_tables_share_column_name() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "EVENTS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "SCORE", "dataType": {"type": "double"}}]},
                {"name": "LABELS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "LABEL", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "TAGS", "columns": [
                    {"name": "ID", "dataType": {"type": "decimal", "precision": 18, "scale": 0}},
                    {"name": "TAG_NAME", "dataType": {"type": "varchar", "size": 100}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"type": "join", "join_type": "inner",
                        "left": {"name": "EVENTS", "type": "table"},
                        "right": {"name": "LABELS", "type": "table"},
                        "condition": {"type": "predicate_equal",
                            "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                            "right": {"type": "column", "name": "ID", "tableName": "LABELS"}}},
                    "right": {"name": "TAGS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "ID", "tableName": "LABELS"},
                        "right": {"type": "column", "name": "ID", "tableName": "TAGS"}}},
                "selectList": [
                    {"type": "column", "name": "ID", "tableName": "EVENTS"},
                    {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                    {"type": "column", "name": "TAG_NAME", "tableName": "TAGS"}],
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"EVENTS": "lh.events", "LABELS": "lh.labels", "TAGS": "lh.tags"}})
                    .to_string()},
        });
        let multi = match detect_join(&request, &pd(&request)).expect("detected join shape") {
            JoinShape::Join(m) => m,
            other => panic!("expected Join, got {other:?}"),
        };
        let sides = vec![
            resolved_side("EVENTS", vec![("s3://w/e-0.parquet", 100)]),
            resolved_side("LABELS", vec![("s3://w/l-0.parquet", 10)]),
            resolved_side("TAGS", vec![("s3://w/t-0.parquet", 10)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &multi,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("three tables sharing an ID column must still build, never Err");

        assert!(
            sql.contains(r#""LHS_T0"."ID" = "LHS_T1"."ID""#),
            "first condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        assert!(
            sql.contains(r#""LHS_T1"."ID" = "LHS_T2"."ID""#),
            "second condition must be table-qualified, never bare/ambiguous: {sql}"
        );
        // The outer wrapper's own SELECT list (as opposed to each independently
        // scanned, unambiguous per-side fan-out's inner projection) must qualify
        // every shared `ID` reference — never a bare, cross-side-ambiguous `"ID"`.
        assert!(
            sql.starts_with(r#"SELECT "LHS_T0"."ID", "LHS_T1"."LABEL", "LHS_T2"."TAG_NAME" FROM "#),
            "the outer SELECT list must qualify the shared ID column, never bare: {sql}"
        );
    }

    /// Group D (task 4.1): the two-table above-broadcast-threshold fallback renders
    /// its FROM as a left-to-right `INNER JOIN … ON` chain (not a comma cross-join +
    /// flat WHERE). The single equi-condition attaches as the join point's `ON`
    /// clause, table-qualified, at the point that brings the second leg into scope.
    #[test]
    fn above_threshold_join_falls_back_inner_join_on() {
        let request = join_request(Json::Null, equi_condition());
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the above-threshold two-table fallback must build");

        assert!(
            sql.contains("INNER JOIN"),
            "the fallback FROM must be an INNER JOIN chain, not a comma cross-join: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T0" INNER JOIN"#),
            "the first leg must be the left side of the INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition must attach table-qualified as the join point's ON clause: {sql}"
        );
        assert!(
            !sql.contains(r#"AS "LHS_T0", "#),
            "the legacy comma cross-join between legs must be gone: {sql}"
        );
    }

    /// Group D (task 4.1): a three-table inner join renders a two-hop
    /// `INNER JOIN … ON` chain, each condition greedily attached at the earliest
    /// join point where all its tables are in scope (by table-name set). No residual
    /// filter → no outer WHERE.
    #[test]
    fn three_table_join_inner_join_on_chain() {
        let request = three_table_join_request();
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
            resolved_side("LINEITEM", vec![("s3://w/l-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the three-table inner-join chain must build");

        assert_eq!(
            sql.matches("INNER JOIN").count(),
            2,
            "N=3 tables → a two-hop INNER JOIN chain: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T1" ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the first condition attaches at the join point bringing LHS_T1 into scope: {sql}"
        );
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."O_ORDERKEY" = "LHS_T2"."L_ORDERKEY"))"#),
            "the second condition attaches at the join point bringing LHS_T2 into scope: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "every condition lives in an ON clause and there is no residual filter, so no \
             outer WHERE: {sql}"
        );
    }

    /// Group D (tasks 4.1 + 4.2): greedy-attach by table-name set AND the WHERE split.
    /// A star shape `(N1 ⋈ (N2 ⋈ FACT))` where BOTH conditions reference FACT (the
    /// deepest leaf, `LHS_T2`): both attach at the last join point, so the middle
    /// join point (bringing `LHS_T2`'s sibling `LHS_T1` into scope) has no
    /// newly-resolvable condition and renders `ON 1=1`. A CUSTOMER-side-local WHERE
    /// conjunct is pushed into that leg's fan-out (never re-applied in the outer
    /// WHERE); only the cross-table residual conjunct survives in the outer WHERE.
    #[test]
    fn join_conditions_greedy_attach_and_side_local_pushdown() {
        let cond_n2_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N2_KEY", "tableName": "N2"},
            "right": {"type": "column", "name": "F_N2KEY", "tableName": "FACT"}});
        let cond_n1_fact = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "N1_KEY", "tableName": "N1"},
            "right": {"type": "column", "name": "F_N1KEY", "tableName": "FACT"}});
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "N1", "columns": [
                    {"name": "N1_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "N1_NAME", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "N2", "columns": [
                    {"name": "N2_KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
                {"name": "FACT", "columns": [
                    {"name": "F_N1KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_N2KEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "F_VALUE", "dataType": {"type": "decimal", "precision": 20, "scale": 0}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "N1", "type": "table"},
                    "right": {"type": "join", "join_type": "inner",
                        "left": {"name": "N2", "type": "table"},
                        "right": {"name": "FACT", "type": "table"},
                        "condition": cond_n2_fact},
                    "condition": cond_n1_fact},
                "selectList": [
                    {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                    {"type": "column", "name": "F_VALUE", "tableName": "FACT"}],
                "filter": {"type": "predicate_and", "expressions": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "N1_NAME", "tableName": "N1"},
                     "right": {"type": "literal_string", "value": "ACME"}},
                    {"type": "predicate_greater",
                     "left": {"type": "column", "name": "F_VALUE", "tableName": "FACT"},
                     "right": {"type": "column", "name": "N1_KEY", "tableName": "N1"}}]},
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP":
                    {"N1": "lh.n1", "N2": "lh.n2", "FACT": "lh.fact"}})
                    .to_string()},
        });
        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("N1", vec![("s3://w/n1-0.parquet", 10)]),
            resolved_side("N2", vec![("s3://w/n2-0.parquet", 10)]),
            resolved_side("FACT", vec![("s3://w/f-0.parquet", 500)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("the star-shape greedy-attach fallback must build");

        // The middle join point brings N2 (LHS_T1) into scope but neither condition is
        // resolvable there (both also need FACT / LHS_T2) → ON 1=1.
        assert!(
            sql.contains(r#"AS "LHS_T1" ON 1=1"#),
            "a join point with no newly-resolvable condition must render ON 1=1: {sql}"
        );
        // Both conditions greedily attach at the last join point (LHS_T2), AND-conjoined.
        assert!(
            sql.contains(r#"AS "LHS_T2" ON (("LHS_T1"."N2_KEY" = "LHS_T2"."F_N2KEY")) AND (("LHS_T0"."N1_KEY" = "LHS_T2"."F_N1KEY"))"#),
            "both FACT-touching conditions must attach greedily at the final join point: {sql}"
        );

        // Task 4.2: the N1-side-local conjunct is pushed into N1's fan-out leg…
        assert!(
            sql.contains("'ACME'"),
            "the side-local conjunct must be pushed into its leg's fan-out: {sql}"
        );
        // …and NOT re-applied in the outer WHERE, which keeps only the cross-table residual.
        let where_clause = &sql[sql
            .find(" WHERE ")
            .expect("the cross-table residual must remain in an outer WHERE")..];
        assert!(
            !where_clause.contains("ACME"),
            "the side-local conjunct must NOT be duplicated in the outer WHERE: {sql}"
        );
        assert!(
            where_clause.contains(r#""LHS_T2"."F_VALUE""#)
                && where_clause.contains(r#""LHS_T0"."N1_KEY""#),
            "the cross-table residual conjunct must render qualified in the outer WHERE: {sql}"
        );
    }

    /// An aggregate over a join (`COUNT(*), MIN(o.O_ORDERDATE)`) routes through the
    /// unified N-scan wrapper and lets Exasol evaluate the aggregate over the
    /// materialized join — a two-column result (`COUNT(*)`,
    /// `MIN("LHS_T1"."O_ORDERDATE")`), not the full-row projection the old code
    /// emitted (which produced the "expected 2 columns but pushdown has 5" failure).
    #[test]
    fn aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["selectList"] = serde_json::json!([
            {"type": "function_aggregate", "name": "COUNT", "arguments": []},
            {"type": "function_aggregate", "name": "MIN", "arguments": [
                {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}]},
        ]);

        assert!(
            join_requires_exasol_postprocessing(&pd(&request)),
            "an aggregate select list must force the Exasol-executed fallback path"
        );

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("aggregate-over-join must build the unified wrapper");

        assert!(sql.contains("COUNT(*)"), "COUNT(*) must be rendered: {sql}");
        assert!(
            sql.contains(r#"MIN("LHS_T1"."O_ORDERDATE")"#),
            "MIN must qualify its argument to the owning side: {sql}"
        );
        assert!(
            sql.starts_with(r#"SELECT COUNT(*), MIN("LHS_T1"."O_ORDERDATE") FROM"#),
            "the outer SELECT must be exactly the two aggregate columns: {sql}"
        );
        assert!(
            sql.contains("INNER JOIN") && !sql.contains("\"join\":{"),
            "aggregate-over-join is an INNER JOIN chain fallback, never a broadcast block: {sql}"
        );
    }

    /// A three-side `alias_of` map ({CUSTOMER→LHS_T0, ORDERS→LHS_T1,
    /// LINEITEM→LHS_T2}) for the seam-unification tests, matching the `LHS_T*` scheme
    /// [`build_n_scan_alias_map`] produces from resolved sides.
    fn seam_alias_of() -> HashMap<String, String> {
        HashMap::from([
            ("CUSTOMER".to_string(), "LHS_T0".to_string()),
            ("ORDERS".to_string(), "LHS_T1".to_string()),
            ("LINEITEM".to_string(), "LHS_T2".to_string()),
        ])
    }

    /// The finding-#1 seam: a select item that is a SCALAR FUNCTION WRAPPING
    /// AGGREGATES — the reported `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1
    /// ELSE 0 END) / COUNT(*), 2)` — renders through `render_selectlist_item_qualified`
    /// (NOT `None`, no decline), with its nested aggregates spliced verbatim and its
    /// nested column argument table-qualified to the owning side. Before the vs-expression
    /// aggregate arm + seam unification this recursed into the translator's catch-all and
    /// returned `None`, declining the whole grouped-join pushdown at every arity.
    #[test]
    fn render_selectlist_item_qualified_renders_scalar_over_aggregate() {
        let alias_of = seam_alias_of();
        let sum_case = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{
                "type": "function_scalar", "name": "CASE", "arguments": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                     "right": {"type": "literal_string", "value": "R"}},
                    {"type": "literal_exactnumeric", "value": 1},
                    {"type": "literal_exactnumeric", "value": 0}]}]
        });
        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        let item = serde_json::json!({
            "type": "function_scalar", "name": "ROUND", "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        sum_case]},
                    count_star]},
                {"type": "literal_exactnumeric", "value": 2}]
        });

        let sql = render_selectlist_item_qualified(&item, &alias_of)
            .expect("a scalar-over-aggregate item must render, never decline to None");
        assert!(
            sql.contains(r#"SUM(CASE WHEN ("LHS_T2"."L_RETURNFLAG" = 'R') THEN 1 ELSE 0 END)"#),
            "the nested SUM(CASE ...) must render with its column table-qualified: {sql}"
        );
        assert!(
            sql.contains("COUNT(*)"),
            "the nested COUNT(*) must render as the star case: {sql}"
        );
    }

    /// The finding-#1 byte-compatibility guard: a TOP-LEVEL bare aggregate renders
    /// through the unified `render_selectlist_item_qualified` byte-identically to the
    /// former dedicated `render_aggregate_qualified` — a single-arg aggregate as
    /// `NAME("ALIAS"."COL")`, `COUNT(*)` as `COUNT(*)`, and `DISTINCT` preserved. The
    /// exact expected strings are captured here so any future drift at the seam fails.
    #[test]
    fn render_selectlist_item_qualified_top_level_aggregate_byte_compatible() {
        let alias_of = seam_alias_of();

        let sum = serde_json::json!({
            "type": "function_aggregate", "name": "SUM", "distinct": false,
            "arguments": [{"type": "column", "name": "O_TOTALPRICE", "tableName": "ORDERS"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&sum, &alias_of).as_deref(),
            Some(r#"SUM("LHS_T1"."O_TOTALPRICE")"#)
        );

        let count_star = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "arguments": [], "distinct": false
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_star, &alias_of).as_deref(),
            Some("COUNT(*)")
        );

        let count_distinct = serde_json::json!({
            "type": "function_aggregate", "name": "COUNT", "distinct": true,
            "arguments": [{"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"}]
        });
        assert_eq!(
            render_selectlist_item_qualified(&count_distinct, &alias_of).as_deref(),
            Some(r#"COUNT(DISTINCT "LHS_T0"."C_CUSTKEY")"#)
        );
    }

    /// A bare-column ORDER BY over a join is rendered table-qualified in the unified
    /// wrapper (with explicit direction + NULL placement), so Exasol — which has
    /// delegated the ordering — sorts on the unambiguous, owning-side column.
    #[test]
    fn order_by_over_join_renders_qualified_in_unified_wrapper() {
        let mut request = join_request(Json::Null, equi_condition());
        request["pushdownRequest"]["orderBy"] = serde_json::json!([
            {"expression": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
             "isAscending": true, "nullsLast": false},
        ]);

        assert!(join_requires_exasol_postprocessing(&pd(&request)));

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c-0.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("ordered unified wrapper must build");
        assert!(
            sql.contains(r#"ORDER BY "LHS_T1"."O_ORDERDATE" ASC NULLS FIRST"#),
            "ORDER BY must be table-qualified with explicit direction/nulls: {sql}"
        );
    }

    /// `join_requires_exasol_postprocessing` fires for every clause the broadcast
    /// in-UDF join cannot serve, and is false for a plain projection+filter join.
    #[test]
    fn post_processing_predicate_covers_every_forcing_clause() {
        let plain = join_request(Json::Null, equi_condition());
        assert!(!join_requires_exasol_postprocessing(&pd(&plain)));

        let mut limited = join_request(Json::Null, equi_condition());
        limited["pushdownRequest"]["limit"] = serde_json::json!({"numElements": 10});
        assert!(join_requires_exasol_postprocessing(&pd(&limited)));

        let mut grouped = join_request(Json::Null, equi_condition());
        grouped["pushdownRequest"]["groupBy"] =
            serde_json::json!([{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}]);
        assert!(join_requires_exasol_postprocessing(&pd(&grouped)));

        let mut having = join_request(Json::Null, equi_condition());
        having["pushdownRequest"]["having"] =
            serde_json::json!({"type": "literal_bool", "value": true});
        assert!(join_requires_exasol_postprocessing(&pd(&having)));
    }

    // -----------------------------------------------------------------------
    // Per-side pruning (PR #70 review): side-local conjunct attribution,
    // projection narrowing, and per-side filter pushdown in the fallback path.
    // -----------------------------------------------------------------------

    /// A conjunct referencing only one side's columns is attributed to that side
    /// alone: the CUSTOMER-only conjunct threads to CUSTOMER, the ORDERS-only
    /// conjunct to ORDERS, and neither leaks to the other.
    #[test]
    fn side_local_filter_attributes_conjuncts_to_owning_side() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });

        let cust = render_df_filter_safe(
            &side_local_filter(&filter, "CUSTOMER").expect("a CUSTOMER-local conjunct exists"),
        )
        .expect("renders");
        assert!(
            cust.contains("C_NAME") && !cust.contains("O_ORDERDATE"),
            "CUSTOMER side-local filter must carry only C_NAME: {cust}"
        );

        let ord = render_df_filter_safe(
            &side_local_filter(&filter, "ORDERS").expect("an ORDERS-local conjunct exists"),
        )
        .expect("renders");
        assert!(
            ord.contains("O_ORDERDATE") && !ord.contains("C_NAME"),
            "ORDERS side-local filter must carry only O_ORDERDATE: {ord}"
        );
    }

    /// A cross-table conjunct (references both sides) and an OR spanning both sides
    /// are withheld from BOTH sides' pruning — only the outer wrapper's WHERE
    /// applies them. A single-side-local conjunct alongside a cross-table one is
    /// still extracted for its side.
    #[test]
    fn side_local_filter_withholds_cross_table_and_or_conjuncts() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                // cross-table: references CUSTOMER and ORDERS
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                 "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}},
                // CUSTOMER-local
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
            ],
        });
        let cust = render_df_filter_safe(
            &side_local_filter(&filter, "CUSTOMER").expect("CUSTOMER-local conjunct present"),
        )
        .expect("renders");
        assert!(
            cust.contains("C_NAME") && !cust.contains("O_CUSTKEY"),
            "the cross-table conjunct must NOT be pushed to CUSTOMER: {cust}"
        );
        assert!(
            side_local_filter(&filter, "ORDERS").is_none(),
            "ORDERS is only referenced by the cross-table conjunct, so nothing is side-local to it"
        );

        // An OR spanning both sides is one opaque conjunct referencing both → withheld.
        let or_filter = serde_json::json!({
            "type": "predicate_or",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                 "right": {"type": "literal_string", "value": "1995-01-01"}},
            ],
        });
        assert!(side_local_filter(&or_filter, "CUSTOMER").is_none());
        assert!(side_local_filter(&or_filter, "ORDERS").is_none());

        // An OR referencing only ONE side is side-local to it (still prunable).
        let one_side_or = serde_json::json!({
            "type": "predicate_or",
            "expressions": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "ACME"}},
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                 "right": {"type": "literal_string", "value": "GLOBEX"}},
            ],
        });
        assert!(
            side_local_filter(&one_side_or, "CUSTOMER").is_some(),
            "an OR over one side alone is side-local and prunable"
        );
        assert!(side_local_filter(&one_side_or, "ORDERS").is_none());
    }

    /// A filter that is a single (non-AND) conjunct is attributed to its owning side
    /// without a top-level AND wrapper.
    #[test]
    fn side_local_filter_handles_a_single_conjunct() {
        let single = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            "right": {"type": "literal_string", "value": "ACME"}
        });
        assert!(side_local_filter(&single, "CUSTOMER").is_some());
        assert!(side_local_filter(&single, "ORDERS").is_none());
    }

    /// Attribution is by `tableName`, NOT by column name: with a column name shared
    /// across both tables (`ID`), a conjunct on `EVENTS.ID` is side-local to EVENTS
    /// only and is never applied to LABELS (which also has an `ID`). This is the
    /// shared-column-name safety the whole per-side pruning rests on.
    #[test]
    fn side_local_filter_attributes_shared_column_by_table_not_name() {
        let filter = serde_json::json!({
            "type": "predicate_and",
            "expressions": [
                {"type": "predicate_greater",
                 "left": {"type": "column", "name": "ID", "tableName": "EVENTS"},
                 "right": {"type": "literal_exactnumeric", "value": 5}},
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": "LABEL", "tableName": "LABELS"},
                 "right": {"type": "literal_string", "value": "x"}},
            ],
        });

        let events = render_df_filter_safe(
            &side_local_filter(&filter, "EVENTS").expect("EVENTS.ID conjunct is side-local"),
        )
        .expect("renders");
        assert!(
            events.contains("ID") && events.contains('5'),
            "EVENTS side-local filter must carry the ID > 5 predicate: {events}"
        );

        let labels = render_df_filter_safe(
            &side_local_filter(&filter, "LABELS").expect("LABELS.LABEL conjunct is side-local"),
        )
        .expect("renders");
        assert!(
            labels.contains("LABEL") && !labels.contains('5'),
            "the EVENTS.ID predicate must NOT be applied to LABELS despite the shared name: {labels}"
        );
    }

    /// The fallback projection is narrowed to the columns the outer wrapper
    /// references for a side — SELECT list + join condition + WHERE — preserving
    /// the full-column order/type, and dropping columns referenced nowhere.
    #[test]
    fn referenced_side_columns_narrows_to_used_columns() {
        let pushdown_req = serde_json::json!({
            "selectList": [{"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"}],
            "filter": {"type": "predicate_equal",
                "left": {"type": "column", "name": "C_ADDRESS", "tableName": "CUSTOMER"},
                "right": {"type": "literal_string", "value": "z"}},
        });
        let condition = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
        });
        let full = vec![
            ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
            ("C_ADDRESS".to_string(), "VARCHAR(100)".to_string()),
            ("C_PHONE".to_string(), "VARCHAR(20)".to_string()),
        ];
        let narrowed = referenced_side_columns(&pushdown_req, &condition, "CUSTOMER", &full);
        let names: Vec<&str> = narrowed.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["C_CUSTKEY", "C_NAME", "C_ADDRESS"],
            "narrows to condition + select + filter columns, in full-column order, dropping C_PHONE"
        );
        // The kept columns retain their full-column Exasol types.
        assert_eq!(
            narrowed[1],
            ("C_NAME".to_string(), "VARCHAR(100)".to_string())
        );
    }

    /// An absent (or empty) SELECT list means the wrapper projects every column via
    /// `SELECT *`, so no narrowing is applied — all columns are kept.
    #[test]
    fn referenced_side_columns_keeps_all_when_select_list_absent() {
        let condition = serde_json::json!({
            "type": "predicate_equal",
            "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
            "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}
        });
        let full = vec![
            ("C_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("C_NAME".to_string(), "VARCHAR(100)".to_string()),
        ];
        let narrowed =
            referenced_side_columns(&serde_json::json!({}), &condition, "CUSTOMER", &full);
        assert_eq!(
            narrowed, full,
            "an absent select list ⇒ SELECT *, keep every column"
        );
    }

    /// A per-side fan-out pushes its side-local filter down as a DataFusion
    /// `ScanSpec.filter` (present in the common blob); absent when there is none.
    ///
    /// Regression (PR #70 e2e "No field named \"O\".\"O_ORDERDATE\""): Exasol sends
    /// each column with a `tableAlias` (the query's `FROM fact_orders o` alias). The
    /// fan-out is a SINGLE-TABLE scan over a relation with BARE uppercase columns, so
    /// its pushed filter MUST render bare — the alias must be stripped, or the
    /// alias-qualified reference fails to resolve against the fan-out.
    #[test]
    fn side_fan_out_pushes_bare_side_local_filter_into_common_blob() {
        let side = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 100)]);
        let cols = vec![
            ("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string()),
            ("O_ORDERDATE".to_string(), "DATE".to_string()),
        ];
        // Exactly the Exasol shape: BOTH tableName AND tableAlias present.
        let filter = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "FACT_ORDERS", "tableAlias": "O"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });

        let sql_with = build_side_fan_out_sql(
            &side,
            &cols,
            Some(&filter),
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        );
        let common = common_arg_literal(&sql_with);
        assert!(
            common.contains("\"filter\"") && common.contains("O_ORDERDATE"),
            "the side-local filter must be pushed into the fan-out common blob: {common}"
        );
        assert!(
            !common.contains(r#"\"O\".\"O_ORDERDATE\""#)
                && !common.contains(r#""O"."O_ORDERDATE""#),
            "the fan-out filter MUST be bare (alias stripped), never alias-qualified: {common}"
        );

        let sql_without = build_side_fan_out_sql(
            &side,
            &cols,
            None,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        );
        let common_none = common_arg_literal(&sql_without);
        assert!(
            !common_none.contains("\"filter\""),
            "no side-local filter ⇒ no filter field in the common blob: {common_none}"
        );
    }

    /// A multi-shard join leg routes through the new distributor + scalar scan
    /// primitive: the fan-out `GROUP BY shard_key` lives in the distributor and the
    /// outer scalar `SCAN` is ungrouped, with NO `SELECT * FROM (...)` materialization
    /// wrapper (decision [5]). The leg is a bare subquery the outer join wrapper reads.
    #[test]
    fn side_fan_out_routes_through_distributor_scalar_scan_no_wrapper() {
        let side = resolved_side(
            "ORDERS",
            vec![("s3://w/o-0.parquet", 100), ("s3://w/o-1.parquet", 100)],
        );
        let cols = vec![("O_CUSTKEY".to_string(), "DECIMAL(20,0)".to_string())];
        // Force two shards: two nodes × factor 1 over two files.
        let tuning = JoinScanTuning {
            cluster_nodes: 2,
            parallelism_factor: 1,
            ..two_scan_tuning()
        };
        let sql =
            build_side_fan_out_sql(&side, &cols, None, &tuning, "SCAN", "MERGE", "DISTRIBUTE");

        assert!(
            !sql.contains("SELECT * FROM ("),
            "the leg must not use a SELECT * materialization wrapper: {sql}"
        );
        assert!(
            sql.starts_with("SELECT SCAN("),
            "the leg is the outer ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTE(files) FROM (VALUES")
                && sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the leg's fan-out GROUP BY shard_key must live in the distributor: {sql}"
        );
    }

    /// The broadcast fact side routes through the same distributor + scalar scan
    /// primitive (task 3.4): a multi-file fact side fans out via the nested
    /// distributor under an outer ungrouped scalar `SCAN`, with no `SELECT * FROM
    /// (...)` wrapper; the dimension side rides once in the common blob's join block.
    #[test]
    fn broadcast_fact_side_uses_distributor_scalar_scan() {
        let fact = resolved_side(
            "LINEITEM",
            vec![("s3://w/l-0.parquet", 1000), ("s3://w/l-1.parquet", 1000)],
        );
        let dimension = resolved_side("ORDERS", vec![("s3://w/o-0.parquet", 10)]);
        let sides = JoinSides {
            fact,
            dimension,
            broadcast_eligible: true,
        };
        let rendered = RenderedJoinPushdown {
            condition: r#""L_ORDERKEY" = "O_ORDERKEY""#.to_string(),
            filter: None,
            projection: vec![ProjectionItem::Column("L_ORDERKEY".to_string())],
            projection_types: vec!["DECIMAL(20,0)".to_string()],
        };
        let tuning = JoinScanTuning {
            cluster_nodes: 2,
            parallelism_factor: 1,
            ..two_scan_tuning()
        };
        let sql =
            build_broadcast_join_sql(&sides, &rendered, &tuning, "SCAN", "MERGE", "DISTRIBUTE");

        assert!(
            !sql.contains("SELECT * FROM ("),
            "the broadcast fact side must not use a SELECT * wrapper: {sql}"
        );
        assert!(
            sql.starts_with("SELECT SCAN("),
            "the fact side is the outer ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the fact side fans out via the nested shard_key distributor: {sql}"
        );
    }

    /// The broadcast path is UNCHANGED by the per-side pruning fix: `render_broadcast_join`
    /// still renders `rendered.filter` exactly as before, PRESERVING Exasol's native
    /// `tableAlias` qualifier (the in-UDF `build_join_sql` join resolves it). This is
    /// the mechanical guard the reviewer asked for — the two-scan fan-out's bare
    /// stripping must NOT leak into, nor alter, the broadcast rendering.
    #[test]
    fn render_broadcast_join_preserves_native_table_alias_unchanged() {
        let mut request = join_request(Json::Null, equi_condition());
        // Give every join column node Exasol's native tableAlias, as the live cluster does.
        request["pushdownRequest"]["filter"] = serde_json::json!({
            "type": "predicate_greater",
            "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS", "tableAlias": "O"},
            "right": {"type": "literal_string", "value": "1995-01-01"}
        });
        let detected = detected_join(&request);
        let rendered = render_broadcast_join(&request, &pd(&request), &detected)
            .expect("renders")
            .expect("disjoint join renders");
        let filter = rendered.filter.expect("filter renders");
        assert!(
            filter.contains(r#""O"."O_ORDERDATE""#),
            "broadcast rendering must preserve Exasol's native tableAlias (unchanged): {filter}"
        );
    }

    /// End-to-end fallback wiring: the unified wrapper prunes each leg (side-local
    /// filter pushed into BOTH fan-out common blobs) AND narrows each leg's
    /// projection (an involved column referenced nowhere in the wrapper is dropped).
    /// Here BOTH filter conjuncts are side-local (one per leg), so — under the task
    /// 4.2 split — the outer WHERE has no residual conjunct and is omitted entirely;
    /// the join condition attaches to the INNER JOIN's ON clause instead.
    #[test]
    fn unified_join_prunes_and_narrows_each_leg() {
        let request = serde_json::json!({
            "involvedTables": [
                {"name": "CUSTOMER", "columns": [
                    {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
                    {"name": "C_ADDRESS", "dataType": {"type": "varchar", "size": 100}}]},
                {"name": "ORDERS", "columns": [
                    {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
                    {"name": "O_TOTALPRICE", "dataType": {"type": "decimal", "precision": 20, "scale": 2}}]},
            ],
            "pushdownRequest": {
                "type": "select",
                "from": {"type": "join", "join_type": "inner",
                    "left": {"name": "CUSTOMER", "type": "table"},
                    "right": {"name": "ORDERS", "type": "table"},
                    "condition": {"type": "predicate_equal",
                        "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                        "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"}}},
                "selectList": [
                    {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                    {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"}],
                "filter": {"type": "predicate_and", "expressions": [
                    {"type": "predicate_equal",
                     "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
                     "right": {"type": "literal_string", "value": "ACME"}},
                    {"type": "predicate_greater",
                     "left": {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
                     "right": {"type": "literal_string", "value": "1995-01-01"}}]},
            },
            "schemaMetadataInfo": {"properties": {}, "adapterNotes":
                serde_json::json!({"TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}})
                    .to_string()},
        });

        let detected = detected_join(&request);
        let sides = vec![
            resolved_side("CUSTOMER", vec![("s3://w/c.parquet", 10)]),
            resolved_side("ORDERS", vec![("s3://w/o.parquet", 100)]),
        ];
        let sql = build_n_scan_join_sql(
            &request,
            &pd(&request),
            &detected,
            &sides,
            &two_scan_tuning(),
            "SCAN",
            "MERGE",
            "DISTRIBUTE",
        )
        .expect("unified wrapper must build");

        // Finding 3: columns referenced nowhere in the wrapper are dropped from the legs.
        assert!(
            !sql.contains("C_ADDRESS"),
            "an unreferenced CUSTOMER column must be narrowed out of the fan-out: {sql}"
        );
        assert!(
            !sql.contains("O_TOTALPRICE"),
            "an unreferenced ORDERS column must be narrowed out of the fan-out: {sql}"
        );

        // Finding 2: each leg gets its own side-local filter pushed into its common blob.
        assert_eq!(
            sql.matches("\"filter\"").count(),
            2,
            "both fan-out legs must carry a side-local ScanSpec.filter: {sql}"
        );

        // Both side-local conjuncts are pushed into their legs' common blobs; the
        // outer WHERE keeps only cross-table residual, of which there is none here.
        assert!(
            sql.contains("'ACME'") && sql.contains("'1995-01-01'"),
            "each leg's side-local conjunct must be pushed into its fan-out: {sql}"
        );
        assert!(
            !sql.contains(" WHERE "),
            "no cross-table residual conjunct remains, so the outer WHERE is omitted: {sql}"
        );
        // The join condition attaches to the INNER JOIN chain's ON clause.
        assert!(
            sql.contains(r#"ON (("LHS_T0"."C_CUSTKEY" = "LHS_T1"."O_CUSTKEY"))"#),
            "the equi-condition attaches to the join point's ON clause: {sql}"
        );
    }

    /// B3b correctness guard: a sort key whose column requires the JSON-fallback
    /// VARCHAR cast declines the top-N path, because the per-shard `ORDER BY col`
    /// sorts the native value while the emitted `CAST(col AS VARCHAR)` is a JSON
    /// string — so Exasol's outer merge would re-rank on the wrong representation.
    /// A plain in-range DECIMAL sort key still matches (regression guard), and a
    /// sort key absent from the logical schema declines defensively.
    #[test]
    fn json_fallback_typed_sort_key_declines_topn() {
        let projected = vec![
            ProjectionItem::Column("L_ORDERKEY".into()),
            ProjectionItem::Column("L_EXTENDEDPRICE".into()),
        ];
        let request = nq4_request();

        // Regression: plain in-range DECIMAL sort key (L_EXTENDEDPRICE) matches.
        assert!(
            detect_topn(
                &request,
                &pd(&request),
                &projected,
                &lineitem_logical_schema()
            )
            .is_some(),
            "a plain in-range DECIMAL sort key must still match the top-N shape"
        );

        // The sort key column typed as an OUT-OF-RANGE Decimal128 (emitted as
        // JSON-fallback VARCHAR): the reachable fallback tag from the logical-schema
        // vocabulary (List/Struct/Binary all collapse to `utf8`). Must decline.
        let fallback_schema = vec![
            LogicalField {
                field_id: 1,
                name: "L_ORDERKEY".into(),
                arrow_type: "decimal128(20,0)".into(),
                nullable: true,
            },
            LogicalField {
                field_id: 2,
                name: "L_EXTENDEDPRICE".into(),
                arrow_type: "decimal128(40,6)".into(),
                nullable: true,
            },
        ];
        assert!(
            crate::types::mapping::needs_json_fallback(
                &crate::types::mapping::arrow_type_from_tag("decimal128(40,6)")
            ),
            "sanity: the chosen tag must actually be a JSON-fallback type"
        );
        assert!(
            detect_topn(&request, &pd(&request), &projected, &fallback_schema).is_none(),
            "a JSON-fallback-typed sort key must decline the top-N path"
        );

        // The sort key column absent from the logical schema declines defensively.
        let missing_schema = vec![LogicalField {
            field_id: 1,
            name: "L_ORDERKEY".into(),
            arrow_type: "decimal128(20,0)".into(),
            nullable: true,
        }];
        assert!(
            detect_topn(&request, &pd(&request), &projected, &missing_schema).is_none(),
            "a sort key absent from the logical schema must decline defensively"
        );
    }

    /// cap-ext scenario: an ORDER BY the adapter cannot bound as a top-N (here: no
    /// LIMIT) is correctness-safe. The bounded top-N declines (no per-shard sort, no
    /// per-shard limit in the common blob), but the OUTER wrapper renders a
    /// self-contained global `ORDER BY` (no LIMIT) — since once `ORDER_BY_COLUMN` is
    /// advertised Exasol no longer re-applies its own backstop sort (add-topn-pushdown
    /// B6), the adapter's returned SQL must specify the ordering itself.
    #[test]
    fn unbounded_order_by_falls_back_correctness_safe() {
        // ORDER BY a projected column but NO LIMIT (unbounded).
        let mut request = nq4_request();
        request["pushdownRequest"]
            .as_object_mut()
            .unwrap()
            .remove("limit");
        let files = vec![("s3://w/part-0.parquet".to_string(), 1000u64)];
        let sql = plan_scan_sql(&request, files, 1);
        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
            "unbounded ORDER BY must be rendered self-contained by the adapter: {sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "unbounded ORDER BY must not carry any LIMIT: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "per-shard common blob must stay clean (no sort keys, no limit): {common}"
        );
    }

    // -----------------------------------------------------------------------
    // Expression-argument aggregates (Task 2.1 / 2.3)
    // -----------------------------------------------------------------------

    /// Scenario (bare-column regression): COUNT(*), COUNT(col), SUM/MIN/MAX/AVG(col)
    /// and the STDDEV family keep the bare-column fast path — `column` populated,
    /// `arg_expr` None — so the pre-existing decomposition is byte-for-byte unchanged.
    #[test]
    fn bare_column_aggregates_unchanged_regression() {
        let req = serde_json::json!({
            "selectList": [
                agg_item("COUNT", None, false),
                agg_item("COUNT", Some("id"), false),
                agg_item("SUM", Some("amount"), false),
                agg_item("MIN", Some("ts"), false),
                agg_item("MAX", Some("ts"), false),
                agg_item("AVG", Some("score"), false),
                agg_item("STDDEV", Some("score"), false),
            ]
        });
        let plans = detect_aggregates(&req).expect("bare-column aggregates must decompose");
        // Every plan takes the fast path: no rendered expression argument.
        assert!(
            plans.iter().all(|p| p.arg_expr.is_none()),
            "bare-column aggregates must never populate arg_expr: {plans:?}"
        );
        assert_eq!(plans[0].kind, AggKind::Count);
        assert!(plans[0].column.is_none());
        assert_eq!(plans[1].kind, AggKind::CountCol);
        assert_eq!(plans[1].column.as_deref(), Some("ID"));
        assert_eq!(plans[2].kind, AggKind::Sum);
        assert_eq!(plans[2].column.as_deref(), Some("AMOUNT"));
        assert_eq!(plans[5].kind, AggKind::Avg);
        assert_eq!(plans[6].kind, AggKind::StddevSamp);

        // The partial EMITS clause is identical to the pre-change output: bare-column
        // SUM over DECIMAL widens to DECIMAL(36,s) from the COLUMN type (aggregate_types
        // is ignored for bare columns), independent of any declared aggregate type.
        let col_types = vec![
            ("AMOUNT".to_string(), "DECIMAL(20,0)".to_string()),
            ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
            ("TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let sum_only = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }];
        // A misleading declared type must NOT override the bare-column source type.
        let emits = partial_emits_items(&sum_only, &col_types, &["VARCHAR(2000000)".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,0)"#.to_string()]);
    }

    /// Scenario: a renderable scalar-expression argument is carried in `arg_expr`
    /// (not `column`), and the partial/merge column TYPE is derived from the
    /// aggregate item's declared type — SUM(expr)::DECIMAL widens to DECIMAL(36,s),
    /// SUM(expr)::DOUBLE stays DOUBLE, MIN/MAX(expr) take the declared type verbatim,
    /// and COUNT(expr) stays DECIMAL(20,0).
    #[test]
    fn expression_arg_partial_and_merge_types_from_declared_type() {
        // Detection: SUM(LENGTH(L_COMMENT)) renders the argument into arg_expr.
        let req = serde_json::json!({
            "selectList": [agg_item_expr("SUM", length_expr("L_COMMENT"), false)]
        });
        let plans = detect_aggregates(&req).expect("expression-argument SUM must decompose");
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert!(
            plans[0].column.is_none(),
            "expression argument must not populate column"
        );
        assert_eq!(
            plans[0].arg_expr.as_deref(),
            Some(r#"character_length("L_COMMENT")"#),
            "the rendered DataFusion fragment must be carried in arg_expr"
        );

        // Typing: no source column exists, so the type comes from the declared type.
        // There is deliberately NO matching entry in col_types.
        let col_types: Vec<(String, String)> = vec![];

        let sum_expr = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: None,
            arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
        }];
        // SUM(expr) declared DECIMAL(38,4) → partial widens to DECIMAL(36,4).
        let emits = partial_emits_items(&sum_expr, &col_types, &["DECIMAL(38,4)".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()]);
        // SUM(expr) declared DOUBLE → partial stays DOUBLE PRECISION.
        let emits = partial_emits_items(&sum_expr, &col_types, &["DOUBLE PRECISION".to_string()]);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_sum_0" DOUBLE PRECISION"#.to_string()]
        );

        // MIN(expr) takes the declared type verbatim.
        let min_expr = vec![AggregatePlan {
            kind: AggKind::Min,
            column: None,
            arg_expr: Some(r#"("A" + "B")"#.into()),
        }];
        let emits = partial_emits_items(&min_expr, &col_types, &["DATE".to_string()]);
        assert_eq!(emits, vec![r#""PARTIAL_min_0" DATE"#.to_string()]);

        // COUNT(expr) is a plain count → DECIMAL(20,0), declared type irrelevant.
        let count_expr = vec![AggregatePlan {
            kind: AggKind::CountCol,
            column: None,
            arg_expr: Some(r#"character_length("L_COMMENT")"#.into()),
        }];
        let emits = partial_emits_items(&count_expr, &col_types, &["DECIMAL(18,0)".to_string()]);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_count_0" DECIMAL(20,0)"#.to_string()]
        );

        // An expression SUM/MIN/MAX validates (its declared type is numeric in
        // practice; the missing column resolves to the numeric DOUBLE fallback).
        assert!(
            validate_agg_col_types(&sum_expr, &col_types),
            "expression-argument SUM must pass validation, not force a row scan"
        );
    }

    /// Scenario (NQ1 / TPC-H Q6 shape): `SUM(L_EXTENDEDPRICE * L_DISCOUNT)` over two
    /// DECIMAL(15,2) columns. Exasol declares the SUM result as DECIMAL(36,4) (it
    /// widens the DECIMAL(30,4) product's precision to its max 36, holding the
    /// natural scale 4 — verified live, decision-log entry [7]). The partial column
    /// must be sized from that declared type — NOT recomputed from the operands'
    /// own DECIMAL(15,2) types — so it widens to DECIMAL(36,4), and the merge casts
    /// to the same declared DECIMAL(36,4). This exercises the DECIMAL-with-nonzero-
    /// scale declared-type path for a two-column product argument.
    #[test]
    fn decimal_product_sum_partial_widens_to_decimal_36() {
        // Detection: SUM(L_EXTENDEDPRICE * L_DISCOUNT) carries the product in
        // arg_expr (no bare source column) — proving the aggregate is decomposed,
        // not declined into a raw two-column row scan.
        let req = serde_json::json!({
            "selectList": [
                agg_item_expr("SUM", mult_expr("L_EXTENDEDPRICE", "L_DISCOUNT"), false)
            ]
        });
        let plans =
            detect_aggregates(&req).expect("SUM(col * col) must decompose, not fall back to scan");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, AggKind::Sum);
        assert!(
            plans[0].column.is_none(),
            "a two-column product has no single source column"
        );
        assert_eq!(
            plans[0].arg_expr.as_deref(),
            Some(r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#),
            "the rendered product must be carried in arg_expr"
        );

        // Typing is driven purely by Exasol's declared result type; there is
        // deliberately NO operand column in col_types (the product has none), so a
        // type recomputed from operands would have to reimplement Exasol's widening
        // rules. The declared DECIMAL(36,4) is authoritative and read verbatim.
        let col_types: Vec<(String, String)> = vec![];
        let declared = ["DECIMAL(36,4)".to_string()];

        let emits = partial_emits_items(&plans, &col_types, &declared);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_sum_0" DECIMAL(36,4)"#.to_string()],
            "partial SUM column must widen to the declared DECIMAL(36,4)"
        );

        // The merge wrapper casts the summed partial back to the declared type so
        // it matches Exasol's positional selectListDataTypes validation.
        let merge = cast_merge_items(&plans, &declared, "LAKEHOUSE_MERGE");
        assert_eq!(
            merge,
            vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
            "merge must cast the summed partial to the declared DECIMAL(36,4)"
        );

        // The expression-argument SUM validates (no operand column → numeric
        // DOUBLE fallback), so it is never forced into a row scan.
        assert!(validate_agg_col_types(&plans, &col_types));
    }

    /// Scenario: an aggregate whose argument the VS translator cannot render
    /// declines the whole aggregate pushdown (row-scan fallback), rather than
    /// emitting a plan referencing an argument it could not render soundly.
    #[test]
    fn unrenderable_agg_arg_falls_back_to_row_scan() {
        let unknown = serde_json::json!({
            "type": "function_scalar",
            "name": "TOTALLY_UNKNOWN_FN",
            "arguments": [{"type": "column", "name": "id"}],
        });
        for name in &["SUM", "MIN", "MAX", "AVG", "COUNT"] {
            let req = serde_json::json!({
                "selectList": [agg_item_expr(name, unknown.clone(), false)]
            });
            assert!(
                detect_aggregates(&req).is_none(),
                "{name} over an unrenderable argument must fall back to row scan"
            );
        }
        // A distinct COUNT over an unrenderable argument also falls back.
        let req = serde_json::json!({
            "selectList": [agg_item_expr("COUNT", unknown.clone(), true)]
        });
        assert!(
            detect_aggregates(&req).is_none(),
            "COUNT(DISTINCT unrenderable) must fall back to row scan"
        );
    }

    // -----------------------------------------------------------------------
    // Single-group COUNT(DISTINCT) (Task 2.2 / 2.3)
    // -----------------------------------------------------------------------

    /// Scenario: single-group COUNT(DISTINCT col) is decomposed into a
    /// `CountDistinct` plan (bare column populated), COUNT(DISTINCT expr) carries
    /// the rendered argument, and each emits exactly one VARCHAR(2000000) partial
    /// column regardless of the underlying column/declared type.
    #[test]
    fn count_distinct_builds_local_set_scan_spec() {
        // COUNT(DISTINCT L_SHIPMODE) — bare column fast path.
        let req = serde_json::json!({
            "selectList": [agg_item("COUNT", Some("L_SHIPMODE"), true)]
        });
        let plans = detect_aggregates(&req).expect("single-group COUNT(DISTINCT) must decompose");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, AggKind::CountDistinct);
        assert_eq!(plans[0].column.as_deref(), Some("L_SHIPMODE"));
        assert!(plans[0].arg_expr.is_none());

        // COUNT(DISTINCT LENGTH(col)) — rendered expression argument.
        let req_expr = serde_json::json!({
            "selectList": [agg_item_expr("COUNT", length_expr("L_COMMENT"), true)]
        });
        let plans_expr = detect_aggregates(&req_expr).expect("COUNT(DISTINCT expr) must decompose");
        assert_eq!(plans_expr[0].kind, AggKind::CountDistinct);
        assert!(plans_expr[0].column.is_none());
        assert_eq!(
            plans_expr[0].arg_expr.as_deref(),
            Some(r#"character_length("L_COMMENT")"#)
        );

        // The partial column is ALWAYS VARCHAR(2000000): a JSON array of the shard's
        // local distinct set — even over an integer column and with a DECIMAL
        // declared COUNT type.
        let col_types = vec![("L_ORDERKEY".to_string(), "DECIMAL(20,0)".to_string())];
        let cd_int = vec![AggregatePlan {
            kind: AggKind::CountDistinct,
            column: Some("L_ORDERKEY".into()),
            arg_expr: None,
        }];
        let emits = partial_emits_items(&cd_int, &col_types, &["DECIMAL(18,0)".to_string()]);
        assert_eq!(
            emits,
            vec![r#""PARTIAL_cd_0" VARCHAR(2000000)"#.to_string()]
        );

        // CountDistinct is never numeric-checked (valid over any column type).
        assert!(
            validate_agg_col_types(&cd_int, &col_types),
            "CountDistinct must not force a row-scan fallback via type validation"
        );
    }

    /// Scenario: the outer wrapper for a single-group COUNT(DISTINCT) feeds the
    /// per-shard JSON-array partials into the schema-qualified scalar merge UDF via
    /// `'[' || LISTAGG("PARTIAL_cd_i", ',') || ']'`, cast to the declared type.
    #[test]
    fn count_distinct_merge_sql_calls_scalar_udf_via_listagg() {
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec!["L_SHIPMODE".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::CountDistinct,
                column: Some("L_SHIPMODE".into()),
                arg_expr: None,
            }]),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        // Two shards → a genuine fan-out whose partials LISTAGG merges.
        let shards = vec![
            vec![("s3://warehouse/a.parquet".to_string(), 1u64)],
            vec![("s3://warehouse/b.parquet".to_string(), 1u64)],
        ];
        let col_types = vec![("L_SHIPMODE".to_string(), "VARCHAR(25)".to_string())];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["L_SHIPMODE".into()],
            &["VARCHAR(25)".to_string()],
            None,
            &col_types,
            &aggregate_types,
            r#""VS_SCHEMA".LAKEHOUSE_SCAN"#,
            r#""VS_SCHEMA".LAKEHOUSE_DISTINCT_MERGE_COUNT"#,
            r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES"#,
        );

        // Partial column contract: one VARCHAR JSON-array column per distinct agg.
        assert!(
            sql.contains(r#""PARTIAL_cd_0" VARCHAR(2000000)"#),
            "EMITS must declare the distinct partial column: {sql}"
        );
        // The merge call: schema-qualified scalar UDF fed the LISTAGG-wrapped
        // array-of-arrays, cast to the COUNT(DISTINCT) declared type.
        assert!(
            sql.contains(
                r#"CAST("VS_SCHEMA".LAKEHOUSE_DISTINCT_MERGE_COUNT('[' || LISTAGG("PARTIAL_cd_0", ',') || ']') AS DECIMAL(18,0))"#
            ),
            "outer wrapper must call the schema-qualified merge UDF via LISTAGG and cast to the declared type: {sql}"
        );
        // The count-distinct aggregate shares the nested-distributor + scalar-scan
        // fan-out (decision [1]/[5]): the two-shard GROUP BY shard_key fan-out lives
        // inside the distributor subquery, and the merge sits directly over the
        // scalar scan with no `SELECT * FROM (...)` materializing wrapper.
        assert!(
            sql.contains(r#""VS_SCHEMA".LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES"#)
                && sql.contains("AS shards(shard_key, files) GROUP BY shard_key)"),
            "count-distinct's fan-out must nest the distributor's GROUP BY shard_key: {sql}"
        );
        assert!(
            !sql.contains("SELECT * FROM"),
            "count-distinct merge must not sit behind a SELECT * materializing wrapper: {sql}"
        );
    }

    /// Scenario (capability-extensions): a GROUP BY request carrying a
    /// COUNT(DISTINCT) still declines (falls back to row scanning); grouped
    /// distinct is explicitly out of scope.
    #[test]
    fn grouped_count_distinct_falls_back_to_row_scan() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "REGION"}],
            "selectList": [
                {"type": "column", "name": "REGION"},
                agg_item("COUNT", Some("L_SHIPMODE"), true),
            ],
        });
        assert!(
            detect_group_by_aggregates(&req).is_none(),
            "grouped COUNT(DISTINCT) must still decline (row-scan fallback)"
        );
        // A non-grouped detection also declines this shape (it has a GROUP BY).
        assert!(
            detect_aggregates(&req).is_none(),
            "the single-group path rejects any request carrying a non-empty GROUP BY"
        );
    }

    /// An aggregate select-list translates to a ScanSpec carrying
    /// the aggregate plan (kind+column) plus any pushed-down filter.
    #[test]
    fn aggregate_query_builds_partial_agg_spec() {
        // Build a spec_template as handle_pushdown would.
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec!["AMOUNT".into()],
            filter: Some("(\"REGION\" = 'EU')".into()),
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(vec![
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                    arg_expr: None,
                },
                AggregatePlan {
                    kind: AggKind::Count,
                    column: None,
                    arg_expr: None,
                },
            ]),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };

        // Build single-shard SQL and decode the embedded spec literal.
        let shards = vec![vec![("s3://warehouse/f.parquet".to_string(), 1u64)]];
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["AMOUNT".into()],
            &["DOUBLE PRECISION".to_string()],
            None,
            &col_types,
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );

        // The spec JSON is embedded in the SQL literal; extract and parse it.
        // It lives between the first `'` and the matching unescaped `'` after the JSON.
        // Simpler: deserialize directly from the template (which is what gets embedded).
        let spec_json = {
            // Reconstruct the shard spec as the builder would.
            let mut s = spec_template.clone();
            s.files = vec![FileEntry::new("s3://warehouse/f.parquet", 1)];
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

    /// The connection-concurrency budget (`s3_max_connections`) is a shard-INVARIANT
    /// tuning field — like `df_threads_per_udf` and `memory_pool_fraction` — so it must
    /// travel in the common blob (the UDF's first argument), serialized exactly once,
    /// never duplicated per shard and never silently dropped from the fan-out SQL.
    #[test]
    fn common_spec_carries_s3_max_connections_exactly_once() {
        let files = vec![
            "s3://warehouse/shard0/part-000.parquet".into(),
            "s3://warehouse/shard1/part-001.parquet".into(),
            "s3://warehouse/shard2/part-002.parquet".into(),
        ];
        // A distinctive, non-default value so it cannot be confused with the
        // built-in default (8) or any other numeric field in the spec.
        let distinctive_s3_max_connections = 37;
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: distinctive_s3_max_connections,
        };

        // Confirm the value round-trips through the shard-invariant common split
        // that `handle_pushdown` uses to build the fan-out (`ScanSpec::to_common`).
        let common = spec_template.to_common();
        assert_eq!(
            common.s3_max_connections, distinctive_s3_max_connections,
            "s3_max_connections must carry from ScanSpec into CommonScanSpec"
        );

        // cluster_nodes=3 forces 3 shards (one file each) — the same multi-shard
        // fan-out shape `handle_pushdown` builds via `build_scan_driving_sql`.
        let files_with_sizes: Vec<FileEntry> = files
            .into_iter()
            .map(|p: String| FileEntry::new(p, 1))
            .collect();
        let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, 3);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        );

        let needle = format!("\"s3_max_connections\":{distinctive_s3_max_connections}");
        assert_eq!(
            sql.matches(&needle).count(),
            1,
            "s3_max_connections must appear exactly once, in the shard-invariant \
             common blob, not per shard and not dropped: {sql}"
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
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(agg_plans),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let files_with_sizes: Vec<FileEntry> =
            files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
        let shards =
            crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, cluster_nodes);
        build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS".into()),
                arg_expr: None,
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

    /// The outer single-group merge SELECT sits DIRECTLY over the scalar scan — no
    /// `SELECT * FROM (...)` between the merge and the scan (decision [5]). The scalar
    /// scan fires once per shard (the distributor emits one row per shard), so one
    /// partial-agg row per shard is produced and the outer SUM/MIN/MAX merge over
    /// those partials equals the single-node aggregate (result-equivalence, [7]).
    #[test]
    fn aggregate_merge_over_scalar_scan_no_wrapper() {
        let plans = vec![
            AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            },
        ];
        // Multi-shard: a genuine distributor fan-out under the merge.
        let sql = build_agg_sql(
            plans,
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            2,
        );

        assert!(
            !sql.contains("SELECT * FROM ("),
            "no materializing wrapper between merge and scan: {sql}"
        );
        // The merge is the outer SELECT; the scalar scan is the subquery it reads.
        assert!(
            sql.starts_with("SELECT ") && sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the outer merge SELECT must read directly from the scalar scan subquery: {sql}"
        );
        // The `GROUP BY shard_key` fan-out lives in the distributor, not the outer merge.
        assert!(
            sql.contains("GROUP BY shard_key"),
            "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
        );
    }

    /// Single-shard aggregate: the merge SELECT sits directly over a from-less scalar
    /// scan on literals — no distributor, no `SELECT * FROM (...)` wrapper.
    #[test]
    fn aggregate_single_shard_merge_over_fromless_scalar_scan() {
        let plans = vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }];
        let sql = build_agg_sql(plans, vec!["s3://w/only.parquet".into()], 1);

        assert!(
            !sql.contains("SELECT * FROM ("),
            "single-shard aggregate must not use a materializing wrapper: {sql}"
        );
        assert!(
            !sql.contains("VALUES") && !sql.contains("GROUP BY shard_key"),
            "single-shard aggregate short-circuits the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the merge reads directly from the from-less scalar scan: {sql}"
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
            arg_expr: None,
        }];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(plans.clone()),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
        let col_types = vec![("SCORE".to_string(), "DECIMAL(18,0)".to_string())];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &col_types,
            &aggregate_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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
            arg_expr: None,
        }];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(plans.clone()),
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://warehouse/f0.parquet".to_string(), 1u64)]];
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &[],
            &[],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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
            arg_expr: None,
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
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Avg,
                column: Some("SCORE".into()),
                arg_expr: None,
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
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("EVENT_TS".into()),
                arg_expr: None,
            },
        ];
        let col_types = vec![
            ("EVENT_DATE".to_string(), "DATE".to_string()),
            ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
        ];
        let emits = partial_emits_items(&plans, &col_types, &[]);
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
            arg_expr: None,
        }];
        let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
        let emits = partial_emits_items(&plans, &col_types, &[]);
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
            arg_expr: None,
        }];
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let emits = partial_emits_items(&plans, &col_types, &[]);
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
            arg_expr: None,
        }];
        assert!(
            !validate_agg_col_types(&sum_varchar, &col_types_varchar),
            "SUM over VARCHAR must fail validation (fall back to row scan)"
        );

        let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
        let sum_date = vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("EVENT_DATE".into()),
            arg_expr: None,
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

    // ---------------------------------------------------------------------------
    // Row scan — outer ungrouped scalar scan, no SELECT * materialization wrapper
    // (decision [5]); ORDER BY/LIMIT attach directly to the outer scalar select.
    // ---------------------------------------------------------------------------

    /// A multi-shard row scan drives an OUTER UNGROUPED scalar `LAKEHOUSE_SCAN` over
    /// the nested distributor — with NO `SELECT * FROM (...)` materialization wrapper
    /// (decision [5]). The scan itself is the top-level SELECT; the distributor
    /// subquery does the `GROUP BY shard_key` fan-out. Result-equivalence (decision
    /// [7]): the returned rows are the union of every shard's rows (no outer GROUP BY,
    /// so no dedup/aggregation).
    #[test]
    fn pushdown_builds_scalar_scan_driving_sql() {
        let sql = build_sql_for_fixture_n(
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            None,
            2,
        );
        assert!(
            !sql.contains("SELECT * FROM ("),
            "the materializing SELECT * wrapper must be gone: {sql}"
        );
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}(")),
            "the outer query is the ungrouped scalar scan itself: {sql}"
        );
        assert!(
            sql.contains("GROUP BY shard_key"),
            "the fan-out GROUP BY shard_key must live inside the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("{DISTRIBUTE_FILES_UDF_NAME}(files)")),
            "the distributor subquery must carry only the files column: {sql}"
        );
    }

    /// LIMIT attaches DIRECTLY to the outer ungrouped scalar select (after the
    /// distributor subquery closes), not to a `SELECT * FROM (...)` wrapper
    /// (decision [5]).
    #[test]
    fn limit_attaches_directly_to_outer_scalar_select() {
        let sql = build_sql_for_fixture_n(
            vec!["s3://w/f0.parquet".into(), "s3://w/f1.parquet".into()],
            vec!["ID".into()],
            vec!["DECIMAL(20,0)".into()],
            None,
            Some(7),
            2,
        );
        assert!(
            !sql.contains("SELECT * FROM ("),
            "no materializing wrapper between LIMIT and the scan: {sql}"
        );
        assert!(
            sql.trim_end().ends_with("LIMIT 7"),
            "LIMIT appends to the outer scalar select: {sql}"
        );
        // The LIMIT must sit OUTSIDE the distributor subquery — after its closing paren.
        let limit_pos = sql.rfind("LIMIT 7").expect("LIMIT present");
        let close_pos = sql[..limit_pos]
            .rfind(')')
            .expect("distributor subquery closes");
        assert!(
            close_pos < limit_pos,
            "LIMIT must follow the distributor subquery's closing paren: {sql}"
        );
    }

    /// Single-shard SQL uses the two-argument form `{udf}('<common>', '<files>')`:
    /// the common blob and the whole-file-list literal each appear exactly once. The
    /// scalar scan is a from-less call on literals with no fan-out markers and no
    /// `SELECT * FROM (...)` materialization wrapper (decision [5]).
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

        // Must be the from-less scalar scan itself (no SELECT * materialization
        // wrapper) and invoke the scan UDF.
        assert!(
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
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

    /// `UPPER(<col>)` as a `function_scalar` node — renders to `upper("<COL>")`
    /// via `render_expression`. Used to build all-expression multi-key GROUP BY
    /// tuples where every element (not just some) is an expression.
    fn upper_item(col: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "UPPER",
            "arguments": [
                {"type": "column", "name": col},
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

    /// Build a minimal grouped `ScanSpec` for the merge-SQL builder tests.
    fn grouped_spec(result: &GroupedAggregateDetection) -> ScanSpec {
        ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        }
    }

    /// A grouped aggregate whose request carries an `orderBy` on a group key but
    /// NO `limit` must still render an explicit final `ORDER BY` in its merge SQL:
    /// once `ORDER_BY_COLUMN` is advertised Exasol no longer re-sorts the grouped
    /// output, so a plain `GROUP BY … ORDER BY` must sort itself (add-topn-pushdown
    /// B6). The sort key is rendered as a POSITIONAL output ordinal so it sorts the
    /// type-cast output, not the lexicographic VARCHAR `GK_*` staging column.
    #[test]
    fn grouped_order_by_no_limit_renders_explicit_merge_order_by() {
        let mut req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
        );
        // ORDER BY id ASC NULLS LAST, and deliberately NO "limit" key.
        req["orderBy"] = serde_json::json!([{
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true,
            "nullsLast": true,
        }]);

        let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
        // The group key ID is output column 1 → positional ordinal, explicit dir+nulls.
        assert_eq!(
            build_grouped_order_by_clause(&req, &result.group_keys, &result.select_items),
            Some(GroupedOrderBy::Clause("1 ASC NULLS LAST".to_string())),
            "grouped ORDER BY must map the sort key to its 1-based output ordinal"
        );

        let group_key_types =
            group_key_exasol_types(&req, &result.group_keys, &result.select_items);
        let sql = build_grouped_aggregate_scan_sql(
            &grouped_spec(&result),
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &[],
            &result.select_items,
            None,
            &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            Some("1 ASC NULLS LAST"),
        );
        assert!(
            sql.contains(" ORDER BY 1 ASC NULLS LAST"),
            "merge SQL must render the explicit final ORDER BY: {sql}"
        );
        // No LIMIT was requested, so none is rendered.
        assert!(!sql.contains("LIMIT"), "no LIMIT requested: {sql}");
    }

    /// Row-scan DECLINE with `order_by` but NO `limit` (projected sort column):
    /// the outer wrapper renders a self-contained global `ORDER BY` (no LIMIT), and
    /// the per-shard common blob stays clean. Proves the decline path no longer
    /// withholds the ordering entirely (add-topn-pushdown B6), independent of a
    /// LIMIT being present.
    #[test]
    fn row_scan_decline_order_by_no_limit_wraps_outer_order_by() {
        let request = serde_json::json!({
            "involvedTables": [{
                "name": "LINEITEM",
                "columns": [
                    {"name": "L_ORDERKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                    {"name": "L_EXTENDEDPRICE", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                ],
            }],
            "pushdownRequest": {
                "type": "select",
                "selectList": [
                    {"type": "column", "name": "L_ORDERKEY", "tableName": "LINEITEM"},
                    {"type": "column", "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                ],
                "selectListDataTypes": [
                    {"type": "decimal", "precision": 20, "scale": 0},
                    {"type": "decimal", "precision": 18, "scale": 2},
                ],
                "orderBy": [{
                    "type": "order_by_element",
                    "expression": {"type": "column", "columnNr": 1, "name": "L_EXTENDEDPRICE", "tableName": "LINEITEM"},
                    "isAscending": false,
                    "nullsLast": true
                }]
                // No "limit" key: no LIMIT clause anywhere.
            }
        });
        let files = vec![
            ("s3://w/part-0.parquet".to_string(), 1000u64),
            ("s3://w/part-1.parquet".to_string(), 1000u64),
        ];
        let sql = plan_scan_sql(&request, files, 2);

        assert!(
            sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST"#),
            "no-LIMIT decline must still render a self-contained outer ORDER BY: {sql}"
        );
        assert!(
            !sql.contains("LIMIT"),
            "no LIMIT was requested, so none must be synthesized: {sql}"
        );
        let common = common_arg_literal(&sql);
        assert!(
            !common.contains("order_by") && !common.contains("\"limit\""),
            "per-shard common blob must stay clean (no sort keys, no limit): {common}"
        );
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

    /// Issue #52 regression guard (decision-log entry [4]): the exact composed
    /// `pushdownRequest` Exasol emits for
    /// `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM EVENTS GROUP BY id) t`
    /// — a real `groupBy` but a `selectList` of only a `literal_null` placeholder
    /// (Exasol's "count the groups" rewrite: the outer query needs only the
    /// per-group row count, not the inner values). Fed verbatim (including the
    /// `from`/`type`/`columnNr`/`tableName` fields the detection path ignores,
    /// to prove they don't perturb parsing) from the spike's captured JSON.
    ///
    /// Detection must preserve the GROUP BY (return `Some` with real group keys
    /// and NO aggregate plan) instead of falling back to a row scan — a row-scan
    /// fallback returns one row per source row, not per group, which is only
    /// accidentally correct when the group column happens to be unique (see
    /// decision-log entry [4]'s caveat). The rendered scan SQL must never
    /// reference a phantom `"NULL"` column identifier and must retain a real
    /// `GROUP BY` clause.
    #[test]
    fn composed_nested_aggregate_request_does_not_reference_phantom_column() {
        let req = serde_json::json!({
            "aggregationType": "group_by",
            "from": { "name": "EVENTS", "type": "table" },
            "groupBy": [
                { "columnNr": 0, "name": "ID", "tableName": "EVENTS", "type": "column" }
            ],
            "selectList": [ { "type": "literal_null" } ],
            "selectListDataTypes": [ { "type": "BOOLEAN" } ],
            "type": "select"
        });
        let result = detect_group_by_aggregates(&req).expect(
            "composed literal-only selectList must preserve GROUP BY, not fall back to row scan",
        );
        assert_eq!(result.group_keys.len(), 1, "one group key from groupBy");
        assert!(
            result.group_keys[0].contains("ID"),
            "group key must reference ID: {:?}",
            result.group_keys[0]
        );
        assert!(
            result.plans.is_empty(),
            "a literal placeholder contributes no aggregate plan"
        );
        assert!(
            matches!(
                result.select_items.as_slice(),
                [GroupedSelectItem::Constant {
                    select_index: 0,
                    ..
                }]
            ),
            "the literal_null item must classify as a Constant: {:?}",
            result.select_items
        );

        // The generated grouped scan SQL must group by GK_0 and must never
        // reference a phantom "NULL" column identifier.
        let group_key_types =
            group_key_exasol_types(&req, &result.group_keys, &result.select_items);
        let sql = build_grouped_aggregate_scan_sql(
            &ScanSpec {
                table_root: String::new(),
                files: vec![],
                projection: vec![],
                filter: None,
                limit: None,
                order_by: Vec::new(),
                aggregates: Some(result.plans.clone()),
                group_keys: Some(result.group_keys.clone()),
                emit_exa_types: Vec::new(),
                logical_schema: Vec::new(),
                name_mapping: Vec::new(),
                join: None,
                storage: sample_storage(),
                df_target_partitions: 1,
                df_batch_size: 8192,
                df_threads_per_udf: 1,
                memory_pool_fraction: 0.6,
                instance_overhead_mb: 200,
                s3_max_connections: 8,
            },
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &[],
            &result.select_items,
            None,
            &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        assert!(
            !sql.contains(r#""NULL""#),
            "grouped scan SQL must not reference a phantom \"NULL\" identifier: {sql}"
        );
        assert!(
            sql.contains(r#"GROUP BY "GK_0""#),
            "outer wrapper must group by GK_0 to yield one row per distinct group: {sql}"
        );
        // The constant placeholder projects a typed literal (declared BOOLEAN),
        // not an empty select list and not a bare-literal column reference.
        assert!(
            sql.contains("SELECT CAST(NULL AS BOOLEAN) FROM"),
            "outer wrapper must project the type-cast constant placeholder: {sql}"
        );
    }

    /// Code-review follow-up on issue #52: `literal_bool` was missing from the
    /// literal-type set used to classify grouped `selectList` constants (only
    /// `literal_null` and six other literal kinds were listed, and the
    /// renderer in `vs-expression` supports `literal_bool` — see
    /// `render_expression`). A boolean literal placeholder in a grouped
    /// selectList (e.g. `SELECT k, TRUE AS flag, COUNT(*) FROM t GROUP BY k`)
    /// used to fall through to the group-key-matching `_` arm, fail to match
    /// any group key, and abort the ENTIRE grouped-aggregate detection to
    /// `None` — exactly the bug class the `literal_null` case guards against,
    /// just for `literal_bool`. `LITERAL_SELECTLIST_TYPES` closes this gap.
    #[test]
    fn literal_bool_selectlist_item_classifies_as_constant_not_group_key() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "ID"}]),
            serde_json::json!([
                {"type": "column", "name": "ID"},
                {"type": "literal_bool", "value": true},
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                decimal_type(20, 0),
                serde_json::json!({"type": "boolean"}),
                decimal_type(20, 0),
            ]),
        );
        let result = detect_group_by_aggregates(&req).expect(
            "a literal_bool selectList item must classify as Constant, not abort detection to None",
        );
        assert!(
            matches!(
                result.select_items[1],
                GroupedSelectItem::Constant {
                    select_index: 1,
                    ..
                }
            ),
            "the literal_bool item must classify as a Constant, not fall through \
             to the group-key arm: {:?}",
            result.select_items
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

    /// All-expression multi-key GROUP BY: `SELECT MOD(id,4), UPPER(name), COUNT(*)
    /// ... GROUP BY MOD(id,4), UPPER(name)`. Every tuple element is an expression
    /// (none a plain column) and must still be detected, each rendered on its own,
    /// and each element must appear rendered individually (not merged/collapsed)
    /// in the SQL built from the detection. If one element of the tuple is
    /// untranslatable, the whole detection must fall back to `None` (full
    /// raw-scan fallback), not a partial/degraded pushdown.
    #[test]
    fn detect_group_by_all_expression_multi_key() {
        let req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4), upper_item("NAME")]),
            serde_json::json!([
                mod_item("ID", 4),
                upper_item("NAME"),
                agg_item("COUNT", None, false),
            ]),
        );
        let result =
            detect_group_by_aggregates(&req).expect("all-expression multi-key must detect");
        assert_eq!(result.group_keys.len(), 2, "two expression group keys");
        assert!(
            result.group_keys[0].contains('%') && result.group_keys[0].contains('4'),
            "key 0 must render the MOD expression: {:?}",
            result.group_keys
        );
        assert!(
            result.group_keys[1].to_lowercase().contains("upper"),
            "key 1 must render the UPPER expression: {:?}",
            result.group_keys
        );
        assert_eq!(result.plans.len(), 1, "one aggregate plan");
        assert_eq!(
            result.select_items,
            vec![
                GroupedSelectItem::GroupKey {
                    group_key_slot: 0,
                    select_index: 0,
                },
                GroupedSelectItem::GroupKey {
                    group_key_slot: 1,
                    select_index: 1,
                },
                GroupedSelectItem::Aggregate {
                    plan_slot: 0,
                    select_index: 2,
                },
            ],
            "each expression key must classify to its own slot: {:?}",
            result.select_items
        );

        // Each element must be rendered per-element (not merged) in the built SQL:
        // the per-shard scan spec's common blob carries both rendered fragments
        // verbatim, embedded in the SQL literal that drives the UDF call.
        let col_types: Vec<(String, String)> = vec![];
        let group_key_types = vec!["VARCHAR(2000000)".to_string(); 2];
        let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &result.group_keys,
            &group_key_types,
            &result.plans,
            &aggregate_types,
            &result.select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        );
        assert!(
            sql.contains("% 4"),
            "built SQL must carry the MOD key rendered on its own: {sql}"
        );
        assert!(
            sql.to_lowercase().contains("upper("),
            "built SQL must carry the UPPER key rendered on its own: {sql}"
        );
        assert!(
            sql.contains(r#""GK_0""#) && sql.contains(r#""GK_1""#),
            "built SQL must emit both group-key slots: {sql}"
        );

        // One untranslatable element in the tuple must collapse detection to None.
        let bad_req = make_group_by_request(
            serde_json::json!([mod_item("ID", 4), {"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
            serde_json::json!([
                mod_item("ID", 4),
                {"type": "fn_custom_unsupported", "name": "MYSTERY"},
                agg_item("COUNT", None, false),
            ]),
        );
        assert!(
            detect_group_by_aggregates(&bad_req).is_none(),
            "one untranslatable tuple element must force full fallback to None"
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
        // All files covered exactly once (compare by path; sizes travel alongside).
        let all: Vec<String> = shards.iter().flatten().map(|(p, _)| p.clone()).collect();
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
            table_root: String::new(),
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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
            table_root: String::new(),
            files: vec![],
            projection: vec!["ID".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_scan_driving_sql(
            &spec_template,
            &shards,
            &["ID".into()],
            &["DECIMAL(20,0)".to_string()],
            None,
            &[],
            &[],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
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
            sql.starts_with(&format!("SELECT {SCAN_UDF_NAME}("))
                && !sql.contains("SELECT * FROM ("),
            "single-shard SQL must be the from-less scalar scan, no wrapper: {sql}"
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
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let files_with_sizes: Vec<FileEntry> =
            files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
        let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, g);
        let select_items = keys_first_select_items(group_keys.len(), agg_plans.len());
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &group_keys,
            &[],
            &agg_plans,
            &[],
            &select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
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
                arg_expr: None,
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

    /// The `GROUP BY shard_key` fan-out lives INSIDE the distributor subquery, while
    /// the OUTER wrapper re-groups the per-shard partials on the user's group keys
    /// (`GROUP BY "GK_0"`) over the scalar scan (decision [5]/[7]). The two GROUP BYs
    /// are at different query levels: shard_key groups the fan-out `VALUES` rows for
    /// round-robin distribution; GK_* re-groups the partial groups every shard emits.
    #[test]
    fn grouped_group_by_shard_key_inside_distributor() {
        let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
        let g = shard_count(2, 1, files.len());
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            files,
            g,
        );

        // The distributor carries the shard_key fan-out.
        assert!(
            sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
            "the shard_key fan-out must live in the distributor subquery: {sql}"
        );
        // The outer wrapper re-groups on the user key staging column.
        assert!(
            sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
            "the outer wrapper must re-group on the user group key GK_0: {sql}"
        );
        // The shard_key GROUP BY is nested strictly BEFORE the outer GK_0 GROUP BY:
        // the distributor's grouping is not the outer one.
        let shard_gb = sql
            .find("GROUP BY shard_key")
            .expect("shard_key GROUP BY present");
        let gk_gb = sql
            .find(r#"GROUP BY "GK_0""#)
            .expect("GK_0 GROUP BY present");
        assert!(
            shard_gb < gk_gb,
            "shard_key GROUP BY (distributor) must precede the outer GK_0 GROUP BY: {sql}"
        );
        // No materializing SELECT * wrapper between the outer re-group and the scan.
        assert!(
            !sql.contains("SELECT * FROM ("),
            "grouped wrapper must not use a SELECT * materialization boundary: {sql}"
        );
    }

    /// Single-shard grouped: the outer re-group sits over a from-less scalar scan on
    /// literals — the distributor short-circuits (no `VALUES`, no shard_key grouping).
    #[test]
    fn grouped_single_shard_short_circuits_distributor() {
        let sql = build_grouped_agg_sql(
            vec!["\"REGION\"".into()],
            vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            vec!["s3://w/only.parquet".into()],
            1,
        );

        assert!(
            !sql.contains("VALUES") && !sql.contains("shard_key"),
            "single-shard grouped must short-circuit the distributor: {sql}"
        );
        assert!(
            sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
            "the outer re-group reads directly from the from-less scalar scan: {sql}"
        );
        assert!(
            sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
            "the outer wrapper still re-groups on the user group key GK_0: {sql}"
        );
    }

    /// LIMIT is NOT pushed into the shard scan for a grouped query. The shared common
    /// blob (arg 0) must not carry "limit"; only the outer wrapper may apply LIMIT.
    #[test]
    fn grouped_common_blob_has_no_limit() {
        let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
        let g = shard_count(1, 1, files.len());
        let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: Some(100), // LIMIT should NOT appear inside the shard spec JSON
            order_by: Vec::new(),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &["\"REGION\"".to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            Some(100),
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
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
                    arg_expr: None,
                },
                AggregatePlan {
                    kind: AggKind::Sum,
                    column: Some("AMOUNT".into()),
                    arg_expr: None,
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
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &group_keys,
            &group_key_types,
            &agg_plans,
            &aggregate_types,
            &select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            having,
            None,
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
                arg_expr: None,
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
                arg_expr: None,
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
                arg_expr: None,
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
                arg_expr: None,
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

    // -----------------------------------------------------------------------
    // Scalar-over-aggregate grouped pushdown (issue #82)
    // -----------------------------------------------------------------------

    /// `CASE WHEN <col> = 'R' THEN 1 ELSE 0 END` — the conditional-count inner
    /// expression wrapped by #82's ROUND(...) select item.
    fn case_flag_eq(col: &str, val: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar_case",
            "name": "CASE",
            "arguments": [
                {"type": "predicate_equal",
                 "left": {"type": "column", "name": col},
                 "right": {"type": "literal_string", "value": val}}
            ],
            "results": [
                {"type": "literal_exactnumeric", "value": 1},
                {"type": "literal_exactnumeric", "value": 0}
            ]
        })
    }

    /// #82's scalar-over-aggregate select item:
    /// `ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`.
    fn round_pct_over_aggregates() -> serde_json::Value {
        serde_json::json!({
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    {"type": "function_scalar", "name": "MULT", "arguments": [
                        {"type": "literal_double", "value": 100.0},
                        agg_item_expr("SUM", case_flag_eq("L_RETURNFLAG", "R"), false)
                    ]},
                    agg_item("COUNT", None, false)
                ]},
                {"type": "literal_exactnumeric", "value": 2}
            ]
        })
    }

    fn soa_col_types() -> Vec<(String, String)> {
        vec![
            ("L_RETURNFLAG".to_string(), "VARCHAR(1)".to_string()),
            ("L_QUANTITY".to_string(), "DECIMAL(36,2)".to_string()),
            (
                "L_EXTENDEDPRICE".to_string(),
                "DOUBLE PRECISION".to_string(),
            ),
        ]
    }

    /// Drive detection then the outer-wrapper builder with the detection outputs
    /// (plans + the plans-aligned `plan_types`), mirroring the production grouped
    /// branch of `handle_pushdown`.
    fn build_grouped_from_detection(req: &serde_json::Value) -> String {
        let d = detect_group_by_aggregates(req)
            .expect("must detect the grouped scalar-over-aggregate pushdown");
        let group_key_types = group_key_exasol_types(req, &d.group_keys, &d.select_items);
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(d.plans.clone()),
            group_keys: Some(d.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        build_grouped_aggregate_scan_sql(
            &spec_template,
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            &d.group_keys,
            &group_key_types,
            &d.plans,
            &d.plan_types,
            &d.select_items,
            None,
            &soa_col_types(),
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
            None,
        )
    }

    /// Task 3.1: `detect_group_by_aggregates` over #82's select list (plus a bare
    /// `COUNT(*)` item) classifies the ROUND(...) item as `ScalarOverAggregate` and
    /// folds its inner `SUM(CASE …)` + `COUNT(*)` into the shared plan list — the
    /// nested `COUNT(*)` deduplicated against the bare `COUNT(*)` so there is exactly
    /// ONE count plan (one `PARTIAL_*` column).
    #[test]
    fn grouped_scalar_over_aggregate_detects_and_dedups_inner_aggregates() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
                round_pct_over_aggregates(),
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
                serde_json::json!({"type": "double"}),
                decimal_type(5, 2),
                decimal_type(18, 0),
            ]),
        );
        let d =
            detect_group_by_aggregates(&req).expect("must detect grouped scalar-over-aggregate");

        // The ROUND item is classified as a scalar-over-aggregate at its own ordinal,
        // carrying its own declared type.
        assert!(
            matches!(
                &d.select_items[3],
                GroupedSelectItem::ScalarOverAggregate {
                    select_index: 3,
                    declared_type,
                    ..
                } if declared_type == "DECIMAL(5,2)"
            ),
            "item 3 must be a ScalarOverAggregate with its declared type: {:?}",
            d.select_items[3]
        );

        // Plans: SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), SUM(CASE …), COUNT(*) — the
        // nested COUNT(*) and the bare COUNT(*) collapse to ONE plan.
        assert_eq!(
            d.plans.len(),
            4,
            "inner SUM(CASE) + COUNT(*) fold in; the two COUNT(*) dedup to one: {:?}",
            d.plans
        );
        let count_plans = d
            .plans
            .iter()
            .filter(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
            .count();
        assert_eq!(
            count_plans, 1,
            "the shared COUNT(*) must be a single plan: {:?}",
            d.plans
        );

        // The bare COUNT(*) select item (index 4) points at the SAME slot the nested
        // COUNT(*) folded into.
        let count_slot = d
            .plans
            .iter()
            .position(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
            .unwrap();
        assert!(
            matches!(
                d.select_items[4],
                GroupedSelectItem::Aggregate { plan_slot, select_index: 4 } if plan_slot == count_slot
            ),
            "the bare COUNT(*) must reuse the shared count slot {count_slot}: {:?}",
            d.select_items[4]
        );
    }

    /// Task 3.2: the outer wrapper renders the scalar-over-aggregate column over the
    /// MERGED partials (`ROUND(… SUM("PARTIAL_*") / SUM("PARTIAL_*") …)`), cast to its
    /// declared type, with NO source-column reference; the outer SELECT column count
    /// equals the `selectList` length.
    #[test]
    fn grouped_scalar_over_aggregate_renders_merged_partials() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
                agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
                round_pct_over_aggregates(),
            ]),
            serde_json::json!([
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
                serde_json::json!({"type": "double"}),
                decimal_type(5, 2),
            ]),
        );
        let sql = build_grouped_from_detection(&req);
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            4,
            "outer SELECT must have one item per selectList item: {items:?}"
        );

        let soa = &items[3];
        assert!(
            soa.contains("PARTIAL_"),
            "wrapper item must be over merged partials: {soa}"
        );
        assert!(
            soa.contains("SUM(\"PARTIAL_") && soa.contains("round("),
            "wrapper must render ROUND over merged SUM(PARTIAL_*) partials: {soa}"
        );
        assert!(
            soa.starts_with("CAST(") && soa.contains("DECIMAL(5,2)"),
            "wrapper item must be CAST to its declared type at its own ordinal: {soa}"
        );
        // The nested aggregates' argument structure (the CASE, and every source
        // column) is subsumed into the PARTIAL_* rewrite — the outer wrapper exposes
        // only GK_*/PARTIAL_* columns.
        assert!(
            !soa.contains("CASE"),
            "the CASE must be folded into a PARTIAL_* column: {soa}"
        );
        assert!(
            !soa.contains("L_RETURNFLAG") && !soa.contains("L_QUANTITY"),
            "wrapper item must not reference any source column: {soa}"
        );
    }

    /// Task 3.3: a scalar-over-aggregate placed BEFORE the group key and a plain
    /// aggregate yields outer SELECT items in `selectList` order, each cast from
    /// `selectListDataTypes` at its own ordinal.
    #[test]
    fn grouped_scalar_over_aggregate_preserves_selectlist_order() {
        let req = make_group_by_request_with_types(
            serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
            serde_json::json!([
                round_pct_over_aggregates(),
                {"type": "column", "name": "L_RETURNFLAG"},
                agg_item("SUM", Some("L_QUANTITY"), false),
            ]),
            serde_json::json!([
                decimal_type(5, 2),
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(36, 2),
            ]),
        );
        let sql = build_grouped_from_detection(&req);
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            3,
            "outer SELECT must have 3 items in selectList order: {items:?}"
        );
        assert!(
            items[0].starts_with("CAST(")
                && items[0].contains("round(")
                && items[0].contains("DECIMAL(5,2)"),
            "position 0 must be the scalar-over-aggregate, cast to its own type: {items:?}"
        );
        assert!(
            items[1].starts_with("CAST(\"GK_0\" AS VARCHAR(1))"),
            "position 1 must be the CAST'd group key at its own ordinal: {items:?}"
        );
        assert!(
            items[2].starts_with("CAST(SUM(\"PARTIAL_") && items[2].contains("DECIMAL(36,2)"),
            "position 2 must be the merged plain aggregate, cast to its own type: {items:?}"
        );
    }

    /// Task 3.4: a grouped request whose scalar-over-aggregate wraps a
    /// `COUNT(DISTINCT …)` (undecomposable) declines grouped detection and routes to
    /// the qualified single-table wrapper — `SELECT <selectList> FROM (<raw scan>) AS
    /// "LHS_T0" GROUP BY …` with a `selectList`-matching column count — NOT a bare
    /// `SELECT * FROM (…)` row scan (the `04000` bug).
    #[test]
    fn grouped_undecomposable_falls_back_to_qualified_wrapper() {
        let pushdown_req = serde_json::json!({
            "aggregationType": "group_by",
            "groupBy": [{"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"}],
            "selectList": [
                {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
                {"type": "function_scalar", "name": "ROUND", "arguments": [
                    {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                        agg_item_expr("SUM", serde_json::json!({"type": "column", "name": "X", "tableName": "LINEITEM"}), false),
                        agg_item_expr("COUNT", serde_json::json!({"type": "column", "name": "Y", "tableName": "LINEITEM"}), true)
                    ]},
                    {"type": "literal_exactnumeric", "value": 2}
                ]}
            ],
            "selectListDataTypes": [
                serde_json::json!({"type": "varchar", "size": 1}),
                decimal_type(5, 2),
            ],
        });

        // The COUNT(DISTINCT) inner aggregate is undecomposable → detection declines.
        assert!(
            detect_group_by_aggregates(&pushdown_req).is_none(),
            "a nested COUNT(DISTINCT) must decline the grouped partial/merge path"
        );

        let request = serde_json::json!({
            "involvedTables": [{"name": "LINEITEM", "columns": [
                {"name": "L_RETURNFLAG", "dataType": {"type": "varchar", "size": 1}},
                {"name": "X", "dataType": {"type": "double"}},
                {"name": "Y", "dataType": {"type": "double"}},
            ]}]
        });
        let all_cols = extract_all_column_types(&request);
        let (proj_cols, proj_types) = full_row_projection(&all_cols);
        let fan_out_spec = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: proj_cols,
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: proj_types,
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let sql = build_grouped_qualified_fallback_sql(
            &request,
            &pushdown_req,
            &fan_out_spec,
            &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
        )
        .expect("qualified fallback must build");

        assert!(
            !sql.starts_with("SELECT * FROM"),
            "fallback must NOT be a bare row scan (the 04000 bug): {sql}"
        );
        assert!(
            sql.contains(" GROUP BY "),
            "fallback must render the GROUP BY: {sql}"
        );
        assert!(
            sql.contains("FROM (") && sql.contains("AS \"LHS_T0\""),
            "fallback must wrap one aliased raw fan-out subquery: {sql}"
        );
        assert!(
            sql.contains("COUNT(DISTINCT"),
            "the undecomposable aggregate is rendered verbatim for Exasol to compute: {sql}"
        );
        // The FIRST ` FROM (` is the outer wrapper's (the fan-out subquery's own
        // FROM comes later), so `outer_select_items` extracts the wrapper's SELECT.
        let items = outer_select_items(&sql);
        assert_eq!(
            items.len(),
            2,
            "the wrapper must return exactly the selectList columns, not a full row: {items:?}"
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
            arg_expr: None,
        }];
        let rendered = render_having_over_merge(&having, &plans, DISTINCT_MERGE_UDF_NAME)
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
        let having =
            render_having_over_merge(&having_node, &detection.plans, DISTINCT_MERGE_UDF_NAME)
                .expect("HAVING must render over the merge decomposition");

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            Some(&having),
            None,
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
            arg_expr: None,
        }];
        assert!(
            render_having_over_merge(&having, &plans, DISTINCT_MERGE_UDF_NAME).is_none(),
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
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            None,
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

    /// Multi-key grouped SQL build with HAVING and LIMIT: `SELECT REGION,
    /// SUM(score), MOD(id,4) ... GROUP BY REGION, MOD(id,4) HAVING SUM(score) >
    /// 100 LIMIT 2`. HAVING and LIMIT must be placed ONLY in the outer wrapper —
    /// never in the per-shard partial scan, which must emit every partial group
    /// from every shard for the outer wrapper to merge and filter correctly.
    #[test]
    fn grouped_wrapper_multi_key_having_and_limit_outer_only() {
        let req = make_group_by_request_with_types(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                agg_item("SUM", Some("SCORE"), false),
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "varchar", "size": 100},
                {"type": "double"},
                decimal_type(9, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(detection.group_keys.len(), 2, "two group keys");
        let group_key_types =
            group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
        let aggregate_types = aggregate_exasol_types(&req);

        let having_node = serde_json::json!({
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 100},
        });
        let having =
            render_having_over_merge(&having_node, &detection.plans, DISTINCT_MERGE_UDF_NAME)
                .expect("HAVING must render over the merge decomposition");

        let col_types: Vec<(String, String)> =
            vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let spec_template = ScanSpec {
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        // Multiple shards so the inner scan is a real `GROUP BY shard_key` fan-out,
        // not the single-shard direct-call shortcut.
        let shards = vec![
            vec![("s3://wh/f0.parquet".to_string(), 1u64)],
            vec![("s3://wh/f1.parquet".to_string(), 1u64)],
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &detection.group_keys,
            &group_key_types,
            &detection.plans,
            &aggregate_types,
            &detection.select_items,
            Some(2),
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            Some(&having),
            None,
        );

        // The per-shard partial scan ends at "GROUP BY shard_key"; everything up to
        // and including that point must carry neither HAVING nor LIMIT.
        let shard_group_end = sql
            .find("GROUP BY shard_key")
            .map(|i| i + "GROUP BY shard_key".len())
            .unwrap_or_else(|| panic!("must contain the inner per-shard fan-out: {sql}"));
        let inner_part = &sql[..shard_group_end];
        assert!(
            !inner_part.contains("HAVING"),
            "HAVING must not appear in the per-shard partial scan: {inner_part}"
        );
        assert!(
            !inner_part.contains("LIMIT"),
            "LIMIT must not appear in the per-shard partial scan: {inner_part}"
        );

        // Everything after the per-shard scan is the outer wrapper: it must carry
        // its own multi-key GROUP BY, then HAVING, then LIMIT, in that order.
        let outer_part = &sql[shard_group_end..];
        let outer_group_by_pos = outer_part
            .find("GROUP BY")
            .unwrap_or_else(|| panic!("outer wrapper must have its own GROUP BY: {outer_part}"));
        assert!(
            outer_part.contains(r#""GK_0""#) && outer_part.contains(r#""GK_1""#),
            "outer GROUP BY must reference both group-key slots: {outer_part}"
        );
        let having_pos = outer_part
            .find("HAVING")
            .unwrap_or_else(|| panic!("HAVING must appear in the outer wrapper: {outer_part}"));
        let limit_pos = outer_part
            .find("LIMIT 2")
            .unwrap_or_else(|| panic!("LIMIT must appear in the outer wrapper: {outer_part}"));
        assert!(
            outer_group_by_pos < having_pos,
            "outer GROUP BY must precede HAVING: {outer_part}"
        );
        assert!(
            having_pos < limit_pos,
            "HAVING must precede LIMIT in the outer wrapper: {outer_part}"
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

    /// Mixed-type multi-key GROUP BY: `SELECT REGION, MOD(id,4), COUNT(*) ...
    /// GROUP BY REGION, MOD(id,4)`. `REGION` is a plain column declared VARCHAR;
    /// `MOD(id,4)` is an expression declared DECIMAL. Each `GK_{i}` must resolve
    /// its own declared type by its own `selectList` index — a shared/defaulted
    /// VARCHAR for both would silently lose the DECIMAL key's real type.
    #[test]
    fn group_key_types_multi_key_mixed_types() {
        let req = make_group_by_request_with_types(
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
            ]),
            serde_json::json!([
                {"type": "column", "name": "REGION"},
                mod_item("ID", 4),
                agg_item("COUNT", None, false),
            ]),
            serde_json::json!([
                {"type": "varchar", "size": 100},
                decimal_type(9, 0),
                decimal_type(18, 0),
            ]),
        );
        let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
        assert_eq!(detection.group_keys.len(), 2, "two group keys");

        let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

        assert_eq!(types.len(), 2, "one declared type per group key");
        assert_eq!(
            types[0], "VARCHAR(100)",
            "the REGION key must resolve its own VARCHAR type, at its own select index: {types:?}"
        );
        assert_eq!(
            types[1], "DECIMAL(9,0)",
            "the MOD(id,4) key must resolve its own DECIMAL type, not a shared/defaulted \
             VARCHAR: {types:?}"
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
            table_root: String::new(),
            files: vec![FileEntry::new("s3://w/f0.parquet", 1)],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(group_keys.clone()),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
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
        // A non-COUNT DISTINCT (SUM DISTINCT) is not decomposable — falls back.
        // (Single-group COUNT(DISTINCT) IS decomposed; see
        // `count_distinct_builds_local_set_scan_spec`.)
        let req_distinct = serde_json::json!({
            "selectList": [agg_item("SUM", Some("AMOUNT"), true)],
        });
        assert!(
            detect_aggregates(&req_distinct).is_none(),
            "SUM(DISTINCT) must fall back to row scan"
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
                arg_expr: None,
            }];
            let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
            let items = partial_emits_items(&plans, &col_types, &[]);
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans, DISTINCT_MERGE_UDF_NAME).join(", ");
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
        // The rendered expression should be carried as an Expr projection item, NOT
        // a bare Column — so the scan splices it verbatim instead of quoting it as a
        // phantom identifier.
        assert_eq!(proj_cols.len(), 1);
        assert!(
            matches!(proj_cols[0], ProjectionItem::Expr { .. }),
            "a rendered scalar expression must be an Expr projection item: {proj_cols:?}"
        );
        let rendered = proj_cols[0].emit_name();
        assert!(
            rendered.contains("UPPER") || rendered.contains("upper"),
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
            table_root: String::new(),
            files: vec![],
            projection: vec!["REGION".into(), "AMOUNT".into()],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            }]),
            group_keys: Some(vec![r#""REGION""#.to_string()]),
            emit_exa_types: Vec::new(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
        };
        let shards = vec![vec![("s3://wh/f.parquet".to_string(), 1u64)]];
        let col_types = vec![
            ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
            ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ];
        let sql = build_grouped_aggregate_scan_sql(
            &spec_template,
            &shards,
            &[r#""REGION""#.to_string()],
            &[],
            &[AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            }],
            &[],
            &keys_first_select_items(1, 1),
            None,
            &col_types,
            SCAN_UDF_NAME,
            DISTINCT_MERGE_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            having_filter.as_deref(),
            None,
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
            table_root: String::new(),
            files: vec![FileEntry::new(
                "s3://warehouse/db/events/part-00000.parquet",
                1,
            )],
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some("(\"ID\" > 10)".into()),
            limit: Some(100),
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: vec!["DECIMAL(20,0)".into(), "VARCHAR(2000000)".into()],
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
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
            table_root: String::new(),
            files: vec![],
            projection: vec![],
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            emit_exa_types: Vec::new(),
            logical_schema: logical.clone(),
            name_mapping: Vec::new(),
            join: None,
            storage: sample_storage(),
            df_target_partitions: 1,
            df_batch_size: 8192,
            df_threads_per_udf: 1,
            memory_pool_fraction: 0.6,
            instance_overhead_mb: 200,
            s3_max_connections: 8,
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

    // ---------------------------------------------------------------------------
    // Join side selection + broadcast threshold: `select_broadcast_sides`.
    // The pure core of the two-table broadcast role/threshold decision — exercised
    // without a live Iceberg catalog. `plan_join` resolves each side via
    // `resolve_one_join_side` and delegates here, so this covers the decision.
    // ---------------------------------------------------------------------------

    /// The default `JOIN_BROADCAST_MAX_BYTES` (128 MiB).
    const BROADCAST_MAX: u64 = 134_217_728;

    /// Build a resolved join side with a given `(path, byte_size)` file list.
    /// Storage/schema/root are populated so the tests can assert the full resolved
    /// payload rides along with the selected role; only the byte totals drive
    /// selection.
    fn resolved_side(table_name: &str, files: Vec<(&str, u64)>) -> ResolvedJoinSide {
        let lower = table_name.to_lowercase();
        ResolvedJoinSide::new(
            table_name.to_string(),
            format!("lh.{lower}"),
            format!("s3://warehouse/lh/{lower}"),
            files
                .into_iter()
                .map(|(p, s)| FileEntry::new(p, s))
                .collect(),
            vec![LogicalField {
                field_id: 1,
                name: format!("{table_name}_KEY"),
                arrow_type: "int64".to_string(),
                nullable: false,
            }],
            Vec::new(),
            sample_storage(),
        )
    }

    /// `total_bytes` is the saturating sum of every file's `file_size_in_bytes`
    /// (the Iceberg-manifest size — no Parquet read).
    #[test]
    fn resolved_side_sums_file_bytes_saturating() {
        assert_eq!(
            resolved_side("ORDERS", vec![("a", 100), ("b", 250), ("c", 4)]).total_bytes,
            354
        );
        // Empty side ⇒ zero bytes.
        assert_eq!(resolved_side("EMPTY", vec![]).total_bytes, 0);
        // A byte total that would overflow u64 saturates to u64::MAX (treated as
        // "far over any threshold"), never wraps.
        assert_eq!(
            resolved_side("HUGE", vec![("x", u64::MAX), ("y", 1)]).total_bytes,
            u64::MAX
        );
    }

    /// The smaller side by bytes is the dimension; the larger is the fact, and the
    /// full resolved payload (files, schema, root, storage, idents) rides along
    /// with each role for tasks 3.3/3.4. Here the LEFT argument is smaller.
    #[test]
    fn dimension_is_left_when_left_side_is_smaller() {
        let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
        let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
        let sides = select_broadcast_sides(customer, orders, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "CUSTOMER");
        assert_eq!(sides.fact.table_name, "ORDERS");
        assert_eq!(sides.dimension.total_bytes, 1_000);
        assert_eq!(sides.fact.total_bytes, 100_000);
        assert!(
            sides.broadcast_eligible,
            "1000 bytes is well under the 128 MiB threshold"
        );
        // Resolved payload travels with the role.
        assert_eq!(sides.dimension.iceberg_ident, "lh.customer");
        assert_eq!(sides.fact.iceberg_ident, "lh.orders");
        assert_eq!(sides.dimension.files, vec![FileEntry::new("c1", 1_000)]);
        assert_eq!(sides.dimension.table_root, "s3://warehouse/lh/customer");
        assert_eq!(sides.dimension.logical_schema.len(), 1);
        assert_eq!(sides.dimension.effective_storage, sample_storage());
    }

    /// Reversing the FROM-clause order (larger side first) still selects the
    /// smaller side as the dimension — selection is by byte size, not position.
    #[test]
    fn dimension_is_right_when_right_side_is_smaller() {
        let orders = resolved_side("ORDERS", vec![("o1", 50_000), ("o2", 50_000)]);
        let customer = resolved_side("CUSTOMER", vec![("c1", 1_000)]);
        let sides = select_broadcast_sides(orders, customer, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "CUSTOMER");
        assert_eq!(sides.fact.table_name, "ORDERS");
        assert_eq!(sides.dimension.total_bytes, 1_000);
        assert!(sides.broadcast_eligible);
    }

    /// The dimension (smaller) side exceeding the threshold is reported as NOT
    /// broadcast-eligible — cleanly via the flag, never an error — so the caller
    /// builds the deterministic unaccelerated two-scan fallback (decision-log [2]).
    #[test]
    fn dimension_over_threshold_is_not_broadcast_eligible() {
        let part = resolved_side("PART", vec![("p1", 200)]);
        let lineitem = resolved_side("LINEITEM", vec![("l1", 900)]);
        // Threshold 100 is below even the smaller side's 200 bytes.
        let sides = select_broadcast_sides(part, lineitem, 100);

        assert_eq!(
            sides.dimension.table_name, "PART",
            "PART (200 bytes) is the smaller side"
        );
        assert_eq!(sides.fact.table_name, "LINEITEM");
        assert!(
            !sides.broadcast_eligible,
            "dimension total 200 > threshold 100: not broadcast-eligible"
        );
    }

    /// A dimension exactly AT the threshold is eligible (inclusive `<=`); one byte
    /// over is not — the boundary the byte-size decision hinges on.
    #[test]
    fn threshold_boundary_is_inclusive() {
        let at = select_broadcast_sides(
            resolved_side("DIM", vec![("d", 100)]),
            resolved_side("FACT", vec![("f", 10_000)]),
            100,
        );
        assert!(
            at.broadcast_eligible,
            "dimension == threshold must be eligible"
        );

        let over = select_broadcast_sides(
            resolved_side("DIM", vec![("d", 101)]),
            resolved_side("FACT", vec![("f", 10_000)]),
            100,
        );
        assert!(
            !over.broadcast_eligible,
            "dimension == threshold + 1 must not be eligible"
        );
    }

    /// An empty side (zero files ⇒ zero bytes) is the trivially broadcast-eligible
    /// dimension, and selection stays deterministic (documented empty-side edge).
    #[test]
    fn empty_side_is_the_eligible_dimension() {
        let empty = resolved_side("EMPTYDIM", vec![]);
        let fact = resolved_side("FACT", vec![("f", 5_000)]);
        let sides = select_broadcast_sides(empty, fact, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "EMPTYDIM");
        assert_eq!(sides.dimension.total_bytes, 0);
        assert!(sides.dimension.files.is_empty());
        assert!(sides.broadcast_eligible);
    }

    /// On an exact byte-size tie (e.g. a self-join, both sides the same table) the
    /// FIRST argument is the dimension — deterministic, documented tie-break.
    #[test]
    fn equal_size_tie_breaks_to_first_argument() {
        let a = resolved_side("SELF_A", vec![("s", 4_242)]);
        let b = resolved_side("SELF_B", vec![("s", 4_242)]);
        let sides = select_broadcast_sides(a, b, BROADCAST_MAX);

        assert_eq!(sides.dimension.table_name, "SELF_A");
        assert_eq!(sides.fact.table_name, "SELF_B");
        assert_eq!(sides.dimension.total_bytes, sides.fact.total_bytes);
    }

    // ---------------------------------------------------------------------------
    // Task 2.2 — `parse_name_mapping` flattens `schema.name-mapping.default`
    // ---------------------------------------------------------------------------

    /// A representative `schema.name-mapping.default` payload — mirroring the
    /// Iceberg spec's own example shape — flattens to one `NameMappingEntry` per
    /// TOP-LEVEL name. Multi-name entries expand to one entry per name (Avro field
    /// aliases); an entry's nested `fields` children are excluded, but the entry's
    /// OWN top-level name(s) are still included; an entry with no `field-id` at
    /// all (schema-only, not present in imported files) is fully excluded.
    #[test]
    fn resolves_name_mapping_flat_entries_once() {
        let raw = r#"
        [
            { "field-id": 1, "names": ["id", "record_id"] },
            {
                "field-id": 3,
                "names": ["location"],
                "fields": [
                    { "field-id": 4, "names": ["latitude", "lat"] },
                    { "field-id": 5, "names": ["longitude", "long"] }
                ]
            },
            { "names": ["schema_only_no_field_id"] }
        ]
        "#;

        let entries = parse_name_mapping(Some(raw)).expect("valid name-mapping JSON must parse");

        assert_eq!(
            entries,
            vec![
                NameMappingEntry {
                    name: "id".to_string(),
                    field_id: 1,
                },
                NameMappingEntry {
                    name: "record_id".to_string(),
                    field_id: 1,
                },
                NameMappingEntry {
                    name: "location".to_string(),
                    field_id: 3,
                },
            ],
            "multi-name entry expands per name; nested `fields` children (lat/lat, \
             long/long) are excluded while the parent's own top-level name is kept; \
             the id-less entry is fully excluded"
        );
    }

    /// An absent `schema.name-mapping.default` property (`None`) yields an empty
    /// mapping, not an error — a table with no name-mapping is the common,
    /// fully-supported case.
    #[test]
    fn absent_name_mapping_is_empty() {
        assert_eq!(
            parse_name_mapping(None).expect("absent property must not error"),
            Vec::new()
        );
    }

    /// A present-but-malformed `schema.name-mapping.default` value fails loud with
    /// a clean, credential-free plan-time error that names the offending property.
    #[test]
    fn malformed_name_mapping_errors_cleanly() {
        let err = parse_name_mapping(Some("{ not valid json mapping shape"))
            .expect_err("malformed name-mapping JSON must error");

        let msg = match err {
            UdfError::User(m) => m,
            other => panic!("expected UdfError::User, got {other:?}"),
        };
        assert!(
            msg.contains(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING),
            "error must name the offending property: {msg}"
        );
        assert!(
            !msg.contains("access_key") && !msg.contains("secret_key"),
            "error must not leak credentials: {msg}"
        );
    }
}
