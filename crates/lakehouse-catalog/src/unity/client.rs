//! The Unity Catalog REST session and its catalog-neutral operations.
//!
//! One `UnityCatalogSession` holds a pooled `reqwest` client, the base URL
//! derived from the CONNECTION address, and the resolved authentication
//! strategy. The wire types it deserializes stay private to this module — the
//! engine consumes only the catalog-neutral types the shared `CatalogClient`
//! trait returns, so no Unity Catalog request shape crosses the crate boundary.
//! One session serves both OSS and Databricks-managed Unity Catalog: request
//! construction never branches on the host, only the base URL and the resolved
//! authentication strategy differ.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::redaction::redact_error_text;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, ConnectionCreds, SkipReason, SkippedTable, TableFormat,
};

use super::auth::{UnityAuth, resolve_unity_auth};
use super::vended::TemporaryTableCredentials;

/// The standard Unity Catalog REST base path appended to the CONNECTION address —
/// identical on OSS Unity Catalog and Databricks-managed Unity Catalog, and never
/// the Iceberg-REST compatibility endpoint or the `delta/v1` Delta Tables API.
const UNITY_REST_BASE_PATH: &str = "/api/2.1/unity-catalog";

/// A per-request Unity Catalog REST session.
///
/// Deep by design: it hides HOW columns are sourced (a single inline list sweep,
/// no per-table fan-out), how pagination is followed, and how each request is
/// authenticated, exposing only the catalog-neutral operations plus the
/// scan-path credential-vending POST that #319/#320 consumes.
pub struct UnityCatalogSession {
    client: reqwest::Client,
    base_url: String,
    auth: UnityAuth,
    creds: ConnectionCreds,
}

impl UnityCatalogSession {
    /// Build a session against the Unity Catalog REST API rooted at `address`,
    /// deriving the standard `{address}/api/2.1/unity-catalog` base URL and
    /// resolving the authentication strategy from `creds`. Issues no request: an
    /// OAuth grant, if any, is deferred to the first request.
    pub fn new(address: &str, creds: ConnectionCreds) -> Self {
        let client = reqwest::Client::new();
        let base_url = format!("{}{UNITY_REST_BASE_PATH}", address.trim_end_matches('/'));
        let auth = resolve_unity_auth(&client, address, &creds);
        Self {
            client,
            base_url,
            auth,
            creds,
        }
    }

    /// Request per-table, short-lived, scoped storage credentials. The scan path
    /// (#319/#320) terminates the response in a `StorageBackend` through
    /// [`super::resolve_uc_vended_storage`]; in this plan the POST is unit-tested
    /// but reached by no production caller.
    pub async fn temporary_table_credentials(
        &self,
        table_id: &str,
        operation: &str,
    ) -> Result<TemporaryTableCredentials, UdfError> {
        let url = format!("{}/temporary-table-credentials", self.base_url);
        let body = serde_json::json!({ "table_id": table_id, "operation": operation });
        let builder = self
            .client
            .post(&url)
            .header("accept", "application/json")
            .json(&body);
        self.send_json::<TemporaryTableCredentials>(builder, "temporary-credentials")
            .await
    }

    async fn list_table_infos(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<TableInfo>, UdfError> {
        let url = format!("{}/tables", self.base_url);
        // `omit_columns` is deliberately left unset: the inline `columns[]` the
        // list response carries by default IS the createVirtualSchema column
        // source, so setting it would force a per-table get-table to recover them.
        self.collect_pages::<TablesPage>(
            &url,
            &[("catalog_name", catalog), ("schema_name", schema)],
            "list tables",
        )
        .await
    }

    async fn get_table_info(&self, full_name: &str) -> Result<TableInfo, UdfError> {
        // Build the path via the URL crate so a reserved or non-ASCII character in
        // a `catalog.schema.table` segment is percent-encoded into one path
        // segment rather than interpolated raw into a malformed path.
        let mut url = url::Url::parse(&self.base_url)
            .map_err(|e| UdfError::User(format!("invalid Unity Catalog base URL: {e}")))?;
        url.path_segments_mut()
            .map_err(|_| UdfError::User("Unity Catalog base URL cannot be a base".into()))?
            .push("tables")
            .push(full_name);
        let builder = self.client.get(url).header("accept", "application/json");
        self.send_json::<TableInfo>(builder, "load table").await
    }

    /// Follow `page_token`/`next_page_token` pagination to completion, returning
    /// every page's entries in page order — never only the first page, which would
    /// silently hide tables from the virtual schema.
    async fn collect_pages<P: PagedResponse>(
        &self,
        url: &str,
        query: &[(&str, &str)],
        kind: &str,
    ) -> Result<Vec<P::Item>, UdfError> {
        let mut items = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut builder = self
                .client
                .get(url)
                .header("accept", "application/json")
                .query(query);
            if let Some(token) = page_token.as_deref() {
                builder = builder.query(&[("page_token", token)]);
            }
            let page: P = self.send_json(builder, kind).await?;
            let next = page.next_page_token();
            items.extend(page.into_items());
            match next {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => return Ok(items),
            }
        }
    }

    /// Apply the auth strategy, send the request, and deserialize a success body,
    /// translating a transport error, a non-success status, or an unparseable body
    /// into a credential-safe [`UdfError`] naming the request `kind`. The resolved
    /// bearer, the OAuth client secret, and the static token are stripped from
    /// every returned error.
    async fn send_json<T: DeserializeOwned>(
        &self,
        builder: reqwest::RequestBuilder,
        kind: &str,
    ) -> Result<T, UdfError> {
        let (builder, bearer) = self.auth.apply(builder).await?;
        let redact = |msg: &str| {
            let mut secrets: Vec<&str> = Vec::new();
            for candidate in [
                bearer.as_deref(),
                self.creds.client_secret.as_deref(),
                self.creds.token.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if !candidate.is_empty() {
                    secrets.push(candidate);
                }
            }
            redact_error_text(msg, &secrets)
        };
        let response = builder.send().await.map_err(|e| {
            UdfError::User(format!(
                "Unity Catalog {kind} request failed: {}",
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
                "Unity Catalog {kind} request failed with HTTP {}: {}",
                status.as_u16(),
                redact(&body)
            )));
        }
        response.json::<T>().await.map_err(|e| {
            UdfError::User(format!(
                "Unity Catalog {kind} request returned an unparseable response: {}",
                redact(&e.to_string())
            ))
        })
    }
}

impl CatalogClient for UnityCatalogSession {
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        // Own the segments before the future is built: the returned future is
        // bound to `&self`, not to the caller's slice borrow.
        let namespace = namespace.to_vec();
        Box::pin(async move {
            let (catalog, schema) = unity_namespace(&namespace)?;
            let infos = self.list_table_infos(catalog, schema).await?;
            let mut tables = Vec::new();
            let mut skipped = Vec::new();
            for info in infos {
                let ident = CatalogTableIdent {
                    namespace: namespace.clone(),
                    name: info.name.clone(),
                };
                let skip_reason =
                    delta_base_skip_reason(&info.table_type, info.data_source_format.as_deref());
                match skip_reason {
                    Some(reason) => skipped.push(SkippedTable { ident, reason }),
                    // An admitted entry passed the DELTA admission filter above,
                    // so the tag restates that outcome rather than re-deciding it.
                    None => tables.push(neutral_table(ident, info, TableFormat::Delta)),
                }
            }
            Ok(CatalogListing { tables, skipped })
        })
    }

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let full_name = full_name(&ident);
            let info = self.get_table_info(&full_name).await?;
            let format = neutral_table_format(info.data_source_format.as_deref(), &full_name)?;
            Ok(neutral_table(ident, info, format))
        })
    }
}

/// The catalog and schema segments a Unity Catalog namespace must carry — a
/// native Unity Catalog is addressed as `catalog.schema.table`.
fn unity_namespace(namespace: &[String]) -> Result<(&str, &str), UdfError> {
    match namespace {
        [catalog, schema] => Ok((catalog.as_str(), schema.as_str())),
        _ => Err(UdfError::User(format!(
            "a Unity Catalog namespace must name a catalog and a schema (two segments), but {} were given",
            namespace.len()
        ))),
    }
}

/// The dotted `catalog.schema.table` full name the get-table endpoint addresses.
fn full_name(ident: &CatalogTableIdent) -> String {
    let mut parts: Vec<&str> = ident.namespace.iter().map(String::as_str).collect();
    parts.push(&ident.name);
    parts.join(".")
}

/// Convert one deserialized Unity Catalog table entry into the neutral shape,
/// carrying the requested identifier, the neutral table type, the storage
/// location (absent when the entry omits it, as a view does), the `format` its
/// CALLER decided, its credential-vending key, and its columns in declared
/// position order — each column left unmapped, since the engine owns the single
/// Exasol type-mapping home.
///
/// The format tag is a parameter rather than derived here because the two callers
/// reach it differently and only one of them can fail: the listing has already
/// admitted Delta base tables only, while the single-table load must MAP the
/// reported value and refuse one it cannot name (see [`neutral_table_format`]).
///
/// An empty OR whitespace-only vending key projects to an ABSENT one, so a caller
/// that requires one fails naming the table rather than requesting credentials
/// against an empty scope.
fn neutral_table(ident: CatalogTableIdent, info: TableInfo, format: TableFormat) -> CatalogTable {
    CatalogTable {
        ident,
        table_type: neutral_table_type(&info.table_type),
        storage_location: info
            .storage_location
            .filter(|location| !location.is_empty()),
        format,
        vended_credential_key: info.table_id.filter(|key| !key.trim().is_empty()),
        columns: info.columns.into_iter().map(neutral_column).collect(),
    }
}

fn neutral_column(column: ColumnInfo) -> CatalogColumn {
    CatalogColumn {
        name: column.name,
        source_type: ColumnSourceType::Unity {
            type_name: column.type_name,
            precision: column.type_precision.unwrap_or(0),
            scale: column.type_scale.unwrap_or(0),
        },
    }
}

/// Map a Unity Catalog `table_type` onto the neutral kind: a base table (managed
/// or external) is a `Table`, a `VIEW` is a `View` (with no storage location), and
/// any other kind is carried verbatim so it is never silently reported as a base
/// table.
fn neutral_table_type(raw: &str) -> CatalogTableType {
    match raw {
        "MANAGED" | "EXTERNAL" => CatalogTableType::Table,
        "VIEW" => CatalogTableType::View,
        other => CatalogTableType::Other(other.to_string()),
    }
}

/// The `data_source_format` Delta tables report, compared case-sensitively
/// against the uppercase vocabulary Unity Catalog emits. The listing admits ONLY
/// this value; the single-table load, which applies no admission filter, also
/// matches it to map the reported format.
const DELTA_DATA_SOURCE_FORMAT: &str = "DELTA";

/// The `data_source_format` of a Unity Catalog UniForm table, compared
/// case-sensitively against the same uppercase vocabulary. The listing does NOT
/// admit it; only the single-table load, which applies no admission filter, names
/// it.
const ICEBERG_DATA_SOURCE_FORMAT: &str = "ICEBERG";

/// How a missing or null `data_source_format` is named in a skip reason or a
/// format refusal.
const ABSENT_DATA_SOURCE_FORMAT: &str = "absent";

/// Why a listed entry is not a Delta base table, or `None` when it is one: an
/// entry is admitted iff its neutral type is a base table AND its
/// `data_source_format` is exactly `DELTA`. A disqualifying type is reported
/// ahead of the format, so a view — which carries no format — is reported by its
/// `table_type`. Takes the raw wire `table_type` rather than the already-lossy
/// neutral kind, so the returned detail names the offending wire value verbatim
/// and this module keeps a single home for Unity's `table_type` vocabulary.
fn delta_base_skip_reason(
    raw_table_type: &str,
    data_source_format: Option<&str>,
) -> Option<SkipReason> {
    let detail = match neutral_table_type(raw_table_type) {
        CatalogTableType::Table if data_source_format == Some(DELTA_DATA_SOURCE_FORMAT) => {
            return None;
        }
        CatalogTableType::Table => format!(
            "data_source_format={}",
            data_source_format.unwrap_or(ABSENT_DATA_SOURCE_FORMAT)
        ),
        _ => format!("table_type={raw_table_type}"),
    };
    Some(SkipReason::NotDeltaBaseTable { detail })
}

fn neutral_table_format(
    data_source_format: Option<&str>,
    table: &str,
) -> Result<TableFormat, UdfError> {
    match data_source_format.filter(|format| !format.trim().is_empty()) {
        Some(DELTA_DATA_SOURCE_FORMAT) => Ok(TableFormat::Delta),
        Some(ICEBERG_DATA_SOURCE_FORMAT) => Ok(TableFormat::Iceberg),
        unrecognized => Err(UdfError::User(format!(
            "Unity Catalog table {table} reports data_source_format={}, which names no table \
             format this engine can plan (expected {DELTA_DATA_SOURCE_FORMAT} or \
             {ICEBERG_DATA_SOURCE_FORMAT})",
            unrecognized.unwrap_or(ABSENT_DATA_SOURCE_FORMAT)
        ))),
    }
}

/// A paginated Unity Catalog list response: its entries and the token for the
/// next page, so [`UnityCatalogSession::collect_pages`] follows every page through
/// one shape.
trait PagedResponse: DeserializeOwned {
    type Item;
    fn into_items(self) -> Vec<Self::Item>;
    fn next_page_token(&self) -> Option<String>;
}

#[derive(Deserialize)]
struct TablesPage {
    #[serde(default)]
    tables: Vec<TableInfo>,
    #[serde(default)]
    next_page_token: Option<String>,
}

impl PagedResponse for TablesPage {
    type Item = TableInfo;
    fn into_items(self) -> Vec<TableInfo> {
        self.tables
    }
    fn next_page_token(&self) -> Option<String> {
        self.next_page_token.clone()
    }
}

/// One Unity Catalog table entry, modeling only the fields this client consumes.
/// `storage_location` and `data_source_format` are absent-tolerant because a VIEW
/// carries neither, so a VIEW list entry deserializes without failing. `table_id`
/// is absent-tolerant for a different reason — defensive tolerance of a catalog
/// response that omits it, since Unity assigns a `table_id` to views too. Every
/// other wire field this client has no use for, `full_name` among them, is simply
/// not modeled here, and serde ignores it.
#[derive(Deserialize)]
struct TableInfo {
    name: String,
    table_type: String,
    #[serde(default)]
    storage_location: Option<String>,
    #[serde(default)]
    data_source_format: Option<String>,
    /// The catalog-assigned key a temporary-table-credentials request is scoped
    /// against, projected onto [`CatalogTable::vended_credential_key`].
    #[serde(default)]
    table_id: Option<String>,
    #[serde(default)]
    columns: Vec<ColumnInfo>,
}

/// One column entry, carrying the FULL parameterized Unity Catalog Spark type: the
/// type name plus the `DECIMAL(p, s)` precision and scale, absent (and read as 0)
/// for a type taking none.
#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    type_name: String,
    #[serde(default)]
    type_precision: Option<u32>,
    #[serde(default)]
    type_scale: Option<u32>,
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
