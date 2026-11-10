use crate::auth::{CatalogAuth, resolve_catalog_auth};
use crate::catalog_http::{CatalogHttp, CatalogRequestError, RepeatSafety, quote_body};
use crate::expiring::system_clock;
use crate::iceberg_io::{authed_get_json, build_signed_request, redact_catalog_text};
use crate::namespace::parse_table_ident;
use crate::redaction::redact_credentials;
use crate::vended::{covering_credential, s3_credentials};
use crate::{CatalogProps, ConnectionCreds, VendedS3Credentials};
use exasol_udf_sdk::error::UdfError;
use iceberg::spec::TableMetadata;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{Endpoint, LoadTableResult, StorageCredential};
use reqwest::{Method, Request};
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

/// `prefix` is the already-resolved REST prefix, not the raw CONNECTION warehouse. It is
/// inserted verbatim (not URL-encoded) so a multi-segment prefix like Glue's
/// `catalogs/{account-id}` keeps its `/`.
pub(crate) fn catalog_url(catalog_uri: &str, prefix: &str, path: &str) -> String {
    let base = catalog_uri.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{base}/v1/{path}")
    } else {
        format!("{base}/v1/{prefix}/{path}")
    }
}

fn tables_path(namespace: &str) -> String {
    format!("namespaces/{namespace}/tables")
}

/// A table name is one path segment, so a reserved character such as `#`, `?`, or `/` is
/// percent-encoded instead of changing the route.
fn table_path(namespace: &str, table_name: &str) -> String {
    let segment: String = url::form_urlencoded::byte_serialize(table_name.as_bytes())
        .collect::<String>()
        .replace('+', "%20");
    format!("{}/{segment}", tables_path(namespace))
}

/// The Iceberg REST route of the tables in `namespace` (`namespaces/{namespace}/tables`), relative
/// to the session's prefix: where a table is created or staged.
pub fn namespace_tables_path(namespace: &NamespaceIdent) -> String {
    tables_path(&namespace.to_url_string())
}

/// The Iceberg REST route of `table` (`namespaces/{namespace}/tables/{table}`), relative to the
/// session's prefix: where it is loaded, checked, and committed to. The table name is
/// percent-encoded as one path segment.
pub fn table_ident_path(table: &TableIdent) -> String {
    table_path(&table.namespace.to_url_string(), &table.name)
}

/// Which snapshots a `loadTable` answer lists.
#[derive(Clone, Copy)]
enum Snapshots {
    /// Only those a branch or tag references; the current one is enough to read a table.
    Referenced,
    /// Every snapshot, so a writer can find its own tagged commit among the ancestors.
    All,
}

fn build_load_table_url(
    catalog_uri: &str,
    prefix: &str,
    ns: &str,
    table_name: &str,
    snapshots: Snapshots,
) -> String {
    let mode = match snapshots {
        Snapshots::Referenced => "refs",
        Snapshots::All => "all",
    };
    catalog_url(
        catalog_uri,
        prefix,
        &format!("{}?snapshots={mode}", table_path(ns, table_name)),
    )
}

/// AWS Glue requires the REST prefix `catalogs/{catalogId}`, while the
/// user-facing `warehouse` is the bare account id. SigV4 is exclusively the Glue
/// path, so callers apply this unconditionally on SigV4.
pub(crate) fn glue_catalog_prefix(warehouse: &str) -> String {
    format!("catalogs/{warehouse}")
}

/// Per the Iceberg REST spec `overrides` take precedence over `defaults`, and
/// `prefix` may be served in either: Databricks uses `overrides`, Lakekeeper
/// `defaults`.
fn prefix_from_config(config: &serde_json::Value) -> String {
    let read = |map: &str| {
        config
            .get(map)
            .and_then(|m| m.get("prefix"))
            .and_then(|p| p.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };
    read("overrides")
        .or_else(|| read("defaults"))
        .unwrap_or_default()
}

/// The endpoints a `/v1/config` answer lists, or `None` when it lists none. An entry that is no
/// `<method> <path>` route offers nothing.
fn endpoints_from_config(config: &serde_json::Value) -> Option<Vec<Endpoint>> {
    let listed = config.get("endpoints")?.as_array()?;
    Some(
        listed
            .iter()
            .filter_map(serde_json::Value::as_str)
            .filter_map(|route| route.parse().ok())
            .collect(),
    )
}

/// What a session knows of the endpoints its catalog offers.
enum ServedEndpoints {
    /// The session took its prefix from a stored note and never called `/v1/config`.
    NotAsked,
    /// `/v1/config` answered without an `endpoints` list, so only the spec's default set is served.
    Unlisted,
    Listed(Vec<Endpoint>),
}

/// What `/v1/config` tells a session: its REST prefix and the endpoints the catalog lists.
struct ServedConfig {
    prefix: String,
    endpoints: ServedEndpoints,
}

/// A config without a prefix yields an EMPTY prefix, never the warehouse: a warehouse like
/// `s3://…` as a path segment gives HTTP 400.
async fn resolve_served_config(
    http: &CatalogHttp,
    catalog_uri: &str,
    warehouse: &str,
    auth: &CatalogAuth,
    creds: &ConnectionCreds,
) -> Result<ServedConfig, UdfError> {
    if let CatalogAuth::Sigv4 { .. } = auth {
        return Ok(ServedConfig {
            prefix: glue_catalog_prefix(warehouse),
            endpoints: ServedEndpoints::NotAsked,
        });
    }
    let encoded_warehouse: String =
        url::form_urlencoded::byte_serialize(warehouse.as_bytes()).collect();
    let config_url = catalog_url(
        catalog_uri,
        "",
        &format!("config?warehouse={encoded_warehouse}"),
    );
    authed_get_json::<serde_json::Value>(http, &config_url, auth, false, creds)
        .await
        .map(|config| ServedConfig {
            prefix: prefix_from_config(&config),
            endpoints: endpoints_from_config(&config)
                .map_or(ServedEndpoints::Unlisted, ServedEndpoints::Listed),
        })
        .map_err(|e| {
            UdfError::User(format!(
                "failed to read the catalog config for warehouse '{warehouse}': {}",
                redact_credentials(&e.to_string())
            ))
        })
}

/// Catalog HTTP state resolved once per statement and reused by every request of it, so the
/// OAuth2 grant and the config lookup run once. Its OAuth2 token is granted anew only when it
/// reaches its renewal point, so one session also serves a writer that outlives a token.
pub struct CatalogSession {
    http: CatalogHttp,
    catalog_uri: String,
    auth: CatalogAuth,
    prefix: String,
    endpoints: ServedEndpoints,
    creds: ConnectionCreds,
}

/// A catalog answer of any HTTP status with its whole body. It derives no `Debug`, because a
/// body can carry vended credentials.
pub struct CatalogResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// The bearer token the request carried, which [`CatalogSession::quote`] strips.
    bearer: Option<String>,
}

/// Why [`CatalogSession::post`] has no answer to return. Each message is redacted.
#[derive(Debug)]
pub enum CatalogSendError {
    /// Nothing reached the catalog: the request could not be built, given a token, or connected.
    Unsent(String),
    /// The request was sent and no complete answer came back, so the catalog may have applied it.
    OutcomeUnknown(String),
}

impl fmt::Display for CatalogSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsent(message) | Self::OutcomeUnknown(message) => f.write_str(message),
        }
    }
}

impl From<CatalogSendError> for UdfError {
    fn from(error: CatalogSendError) -> Self {
        UdfError::User(error.to_string())
    }
}

impl CatalogSession {
    /// A `catalog_prefix` that an earlier session found spares the `/v1/config` lookup. A failed
    /// OAuth2 grant or config lookup is an error. Credential values never appear in any returned
    /// error. The session keeps `creds` for the auth, access delegation, and redaction of every
    /// request it sends.
    pub async fn resolve(
        catalog_uri: &str,
        warehouse: &str,
        catalog_prefix: Option<String>,
        creds: &ConnectionCreds,
    ) -> Result<CatalogSession, UdfError> {
        let http = CatalogHttp::new();
        let auth = resolve_catalog_auth(&http, catalog_uri, creds, system_clock()).await?;
        let (prefix, endpoints) = match catalog_prefix {
            Some(prefix) => (prefix, ServedEndpoints::NotAsked),
            None => {
                let config =
                    resolve_served_config(&http, catalog_uri, warehouse, &auth, creds).await?;
                (config.prefix, config.endpoints)
            }
        };
        Ok(CatalogSession {
            http,
            catalog_uri: catalog_uri.to_string(),
            auth,
            prefix,
            endpoints,
            creds: creds.clone(),
        })
    }

    pub(crate) fn http(&self) -> &CatalogHttp {
        &self.http
    }

    /// The catalog address, for an error that names the catalog.
    pub fn catalog_uri(&self) -> &str {
        &self.catalog_uri
    }

    pub(crate) fn auth(&self) -> &CatalogAuth {
        &self.auth
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether the catalog's `/v1/config` answer lists `route`, such as
    /// `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`. `None` means the
    /// session never read that answer, because it took its prefix from a stored note, so the
    /// catalog may offer the route. Without an `endpoints` list the answer is `Some(false)`, so
    /// ask only about a route outside the spec's default set.
    pub fn offers_endpoint(&self, route: &str) -> Option<bool> {
        let route = route.parse::<Endpoint>().ok();
        match &self.endpoints {
            ServedEndpoints::NotAsked => None,
            ServedEndpoints::Unlisted => Some(false),
            ServedEndpoints::Listed(endpoints) => {
                Some(route.is_some_and(|route| endpoints.contains(&route)))
            }
        }
    }

    /// POSTs a state-changing request, such as a namespace or table creation, a stage-create, or a
    /// commit, to `{uri}/v1/{prefix}/{path}` with the session's auth, `body` as JSON, and the
    /// access-delegation header when the CONNECTION vends. Every HTTP answer comes back as a value
    /// with its whole body, so the caller decides what a status such as 409 means. `safety` states
    /// whether the client may send the request again after a transient failure. Reads stay behind
    /// the session's own read methods, so this is no back door for them.
    pub async fn post(
        &self,
        path: &str,
        body: &serde_json::Value,
        safety: RepeatSafety,
    ) -> Result<CatalogResponse, CatalogSendError> {
        self.send(Method::POST, path, Some(body), safety).await
    }

    pub(crate) async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
        safety: RepeatSafety,
    ) -> Result<CatalogResponse, CatalogSendError> {
        let (request, bearer) = self
            .build_request(method, path, body)
            .await
            .map_err(|error| CatalogSendError::Unsent(error.to_string()))?;
        let target = format!("{} {}", request.method(), request.url());
        let answer = self.http.send(request, safety).await.map_err(|error| {
            let message = format!(
                "the catalog request {target} failed: {}",
                error.render(|msg| redact_catalog_text(msg, &self.creds, bearer.as_deref()))
            );
            if error.is_unsent() {
                CatalogSendError::Unsent(message)
            } else {
                CatalogSendError::OutcomeUnknown(message)
            }
        })?;
        Ok(CatalogResponse {
            status: answer.status.as_u16(),
            body: answer.body,
            bearer,
        })
    }

    /// The body of `answer` as an error may show it: stripped of every catalog secret of the
    /// session and of the bearer token the request carried, then cut.
    pub fn quote(&self, answer: &CatalogResponse) -> String {
        quote_body(&answer.body, |msg| {
            redact_catalog_text(msg, &self.creds, answer.bearer.as_deref())
        })
    }

    /// Whether the catalog holds table `ident`: a 2xx answer to `HEAD` on the table is yes, 404
    /// is no, and any other answer is an error.
    pub async fn table_exists(&self, ident: &TableIdent) -> Result<bool, UdfError> {
        let answer = self
            .send(
                Method::HEAD,
                &table_ident_path(ident),
                None,
                RepeatSafety::Safe,
            )
            .await?;
        table_existence(answer.status).ok_or_else(|| {
            UdfError::User(format!(
                "checking whether table '{}' exists failed: the catalog {} answered HTTP {}",
                ident, self.catalog_uri, answer.status
            ))
        })
    }

    /// Loads table `ident` with every snapshot, not only the referenced ones, so a writer can
    /// find its own tagged commit among the ancestors.
    pub async fn load_table_with_all_snapshots(
        &self,
        ident: &TableIdent,
    ) -> Result<LoadTableResult, UdfError> {
        self.load_table(
            &ident.namespace,
            &ident.name,
            self.creds.use_vended_credentials,
            Snapshots::All,
        )
        .await
        .map_err(UdfError::from)
    }

    /// The S3 credentials that the catalog's `loadCredentials` endpoint vends for table `ident`:
    /// those of the `storage-credentials` entry that covers `location`. No error quotes the
    /// answer, because it carries credentials.
    pub async fn load_credentials(
        &self,
        ident: &TableIdent,
        location: &str,
    ) -> Result<VendedS3Credentials, UdfError> {
        let path = format!("{}/credentials", table_ident_path(ident));
        let answer = self
            .send(Method::GET, &path, None, RepeatSafety::Safe)
            .await?;
        if !(200..300).contains(&answer.status) {
            return Err(UdfError::User(format!(
                "loadCredentials of table '{ident}' answered HTTP {}",
                answer.status
            )));
        }
        let config = read_credentials(&answer.body, location).map_err(|cause| {
            UdfError::User(format!("loadCredentials of table '{ident}' {cause}"))
        })?;
        s3_credentials(&config, location).map_err(|error| {
            UdfError::User(format!(
                "loadCredentials of table '{ident}' failed: {error}"
            ))
        })
    }

    /// The request and the bearer token it carries, which an error of the request redacts.
    async fn build_request(
        &self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<(Request, Option<String>), UdfError> {
        let url = catalog_url(&self.catalog_uri, &self.prefix, path);
        let mut builder = self
            .http
            .request(method, url)
            .header("accept", "application/json");
        if self.creds.use_vended_credentials {
            builder = builder.header("X-Iceberg-Access-Delegation", "vended-credentials");
        }
        if let Some(body) = body {
            builder = builder.json(body);
        }
        build_signed_request(builder, &self.auth, &self.creds).await
    }

    /// Names the table in every failure, so the enumeration load, `load_table_any_auth`, and a
    /// writer's load all report which table could not be loaded.
    async fn load_table(
        &self,
        namespace: &NamespaceIdent,
        table_name: &str,
        send_access_delegation: bool,
        snapshots: Snapshots,
    ) -> Result<LoadTableResult, CatalogRequestError> {
        let url = build_load_table_url(
            &self.catalog_uri,
            &self.prefix,
            &namespace.to_url_string(),
            table_name,
            snapshots,
        );
        let identifier = format!("{}.{table_name}", namespace.join("."));
        authed_get_json::<LoadTableResult>(
            &self.http,
            &url,
            &self.auth,
            send_access_delegation,
            &self.creds,
        )
        .await
        .map_err(|error| error.named(&identifier))
    }
}

fn table_existence(status: u16) -> Option<bool> {
    match status {
        200..=299 => Some(true),
        404 => Some(false),
        _ => None,
    }
}

#[derive(Deserialize)]
struct LoadCredentialsAnswer {
    #[serde(rename = "storage-credentials")]
    storage_credentials: Vec<StorageCredential>,
}

/// The config of the entry that covers `location` in a `loadCredentials` answer, or why there is
/// none. The reason quotes no part of the answer.
fn read_credentials(body: &[u8], location: &str) -> Result<HashMap<String, String>, String> {
    let answer: LoadCredentialsAnswer = serde_json::from_slice(body).map_err(|error| {
        format!(
            "answered no credentials answer: invalid JSON at line {}, column {}",
            error.line(),
            error.column()
        )
    })?;
    covering_credential(&answer.storage_credentials, location)
        .cloned()
        .ok_or_else(|| format!("vended no storage credentials whose prefix covers {location}"))
}

/// Self-issued because `RestCatalog::load_table` drops the response
/// `config`/`storage_credentials` needed for vended credentials. Sends
/// `X-Iceberg-Access-Delegation: vended-credentials` only when the session's CONNECTION sets
/// `use_vended_credentials`. Credential values never appear in the error.
pub async fn load_table_any_auth(
    session: &CatalogSession,
    catalog: &CatalogProps,
) -> Result<LoadTableResult, UdfError> {
    let (ns_ident, table_name) = parse_table_ident(&catalog.table)?;
    session
        .load_table(
            &ns_ident,
            &table_name,
            session.creds.use_vended_credentials,
            Snapshots::Referenced,
        )
        .await
        .map_err(UdfError::from)
}

/// The enumeration reads only the current schema, so it asks for no delegated access.
pub(crate) async fn load_table_schema_metadata(
    session: &CatalogSession,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> Result<TableMetadata, CatalogRequestError> {
    session
        .load_table(namespace, table_name, false, Snapshots::Referenced)
        .await
        .map(|result| result.metadata)
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
