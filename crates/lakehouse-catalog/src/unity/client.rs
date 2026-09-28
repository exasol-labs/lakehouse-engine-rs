use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::client::LOAD_CONCURRENCY;
use crate::http::{build_client, execute_with_retry};
use crate::redaction::redact_error_text;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, ConnectionCreds, SkipReason, SkippedTable, TableFormat,
};

use super::auth::{UnityAuth, resolve_unity_auth};
use super::vended::TemporaryTableCredentials;

/// Identical on OSS and Databricks-managed Unity Catalog; deliberately not the
/// Iceberg-REST compatibility endpoint or the `delta/v1` API.
const UNITY_REST_BASE_PATH: &str = "/api/2.1/unity-catalog";

pub struct UnityCatalogSession {
    client: reqwest::Client,
    base_url: String,
    auth: UnityAuth,
    creds: ConnectionCreds,
}

impl UnityCatalogSession {
    /// Issues no request: an OAuth grant, if any, is deferred to the first request.
    pub fn new(address: &str, creds: ConnectionCreds) -> Self {
        let client = build_client();
        let base_url = format!("{}{UNITY_REST_BASE_PATH}", address.trim_end_matches('/'));
        let auth = resolve_unity_auth(&client, address, &creds);
        Self {
            client,
            base_url,
            auth,
            creds,
        }
    }

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
        // `omit_columns` stays unset: the inline `columns[]` is the column source,
        // avoiding a per-table get-table.
        self.collect_pages::<TablesPage>(
            &url,
            &[("catalog_name", catalog), ("schema_name", schema)],
            "list tables",
        )
        .await
    }

    async fn get_table_info(&self, full_name: &str) -> Result<TableInfo, UdfError> {
        // Percent-encodes reserved/non-ASCII characters into one path segment.
        let mut url = url::Url::parse(&self.base_url)
            .map_err(|e| UdfError::User(format!("invalid Unity Catalog base URL: {e}")))?;
        url.path_segments_mut()
            .map_err(|_| UdfError::User("Unity Catalog base URL cannot be a base".into()))?
            .push("tables")
            .push(full_name);
        let builder = self.client.get(url).header("accept", "application/json");
        self.send_json::<TableInfo>(builder, "load table").await
    }

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
        let failed = |e: reqwest::Error| {
            UdfError::User(format!(
                "Unity Catalog {kind} request failed: {}",
                redact(&e.to_string())
            ))
        };
        let request = builder.build().map_err(failed)?;
        let response = execute_with_retry(&self.client, request)
            .await
            .map_err(failed)?;
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
                match admission(&info.table_type, info.data_source_format.as_deref()) {
                    Err(reason) => skipped.push(SkippedTable { ident, reason }),
                    Ok(format) => tables.push(neutral_table(ident, info, format)),
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

    fn load_tables<'a>(
        &'a self,
        idents: &'a [CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            stream::iter(idents.to_vec())
                .map(|ident| self.load_admitted(ident))
                .buffered(LOAD_CONCURRENCY)
                .try_collect()
                .await
        })
    }
}

impl UnityCatalogSession {
    async fn load_admitted(
        &self,
        ident: CatalogTableIdent,
    ) -> Result<Result<CatalogTable, SkippedTable>, UdfError> {
        let info = match self.get_table_info(&full_name(&ident)).await {
            Ok(info) => info,
            Err(err) if is_not_found(&err) => {
                return Ok(Err(SkippedTable {
                    ident,
                    reason: SkipReason::NotFound,
                }));
            }
            Err(err) => return Err(err),
        };
        Ok(
            match admission(&info.table_type, info.data_source_format.as_deref()) {
                Err(reason) => Err(SkippedTable { ident, reason }),
                Ok(format) => Ok(neutral_table(ident, info, format)),
            },
        )
    }
}

/// Matches the prefix `send_json` mints, including `": "`, so a body merely
/// containing `404` cannot false-match.
fn is_not_found(err: &UdfError) -> bool {
    matches!(err, UdfError::User(msg) if msg.starts_with("Unity Catalog load table request failed with HTTP 404: "))
}

fn unity_namespace(namespace: &[String]) -> Result<(&str, &str), UdfError> {
    match namespace {
        [catalog, schema] => Ok((catalog.as_str(), schema.as_str())),
        _ => Err(UdfError::User(format!(
            "a Unity Catalog namespace must name a catalog and a schema (two segments), but {} were given",
            namespace.len()
        ))),
    }
}

fn full_name(ident: &CatalogTableIdent) -> String {
    let mut parts: Vec<&str> = ident.namespace.iter().map(String::as_str).collect();
    parts.push(&ident.name);
    parts.join(".")
}

/// `format` is decided by the caller: the listing has already admitted it, while
/// the single-table load maps and may refuse the reported value.
///
/// A blank vending key projects to `None`, so a caller that needs one fails
/// naming the table rather than requesting credentials against an empty scope.
fn neutral_table(ident: CatalogTableIdent, info: TableInfo, format: TableFormat) -> CatalogTable {
    let mut indexed: Vec<(u32, String)> = info
        .columns
        .iter()
        .filter_map(|column| {
            column
                .partition_index
                .map(|index| (index, column.name.clone()))
        })
        .collect();
    indexed.sort_by_key(|(index, _)| *index);
    let partition_columns = indexed.into_iter().map(|(_, name)| name).collect();

    CatalogTable {
        ident,
        table_type: neutral_table_type(&info.table_type),
        storage_location: info
            .storage_location
            .filter(|location| !location.is_empty()),
        format,
        vended_credential_key: info.table_id.filter(|key| !key.trim().is_empty()),
        partition_columns,
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
            type_json: column.type_json,
        },
    }
}

fn neutral_table_type(raw: &str) -> CatalogTableType {
    match raw {
        "MANAGED" | "EXTERNAL" => CatalogTableType::Table,
        "VIEW" => CatalogTableType::View,
        other => CatalogTableType::Other(other.to_string()),
    }
}

/// Compared case-sensitively: Unity Catalog emits uppercase format names.
const DELTA_DATA_SOURCE_FORMAT: &str = "DELTA";

/// UniForm tables; not admitted by the listing, only named by the single-table load.
const ICEBERG_DATA_SOURCE_FORMAT: &str = "ICEBERG";

const PARQUET_DATA_SOURCE_FORMAT: &str = "PARQUET";

const ABSENT_DATA_SOURCE_FORMAT: &str = "absent";

/// Takes the raw wire `table_type` so the skip detail names it verbatim; the type
/// is checked before the format because a view carries no format.
fn admission(
    raw_table_type: &str,
    data_source_format: Option<&str>,
) -> Result<TableFormat, SkipReason> {
    let detail = match neutral_table_type(raw_table_type) {
        CatalogTableType::Table => match format_of(data_source_format) {
            Some(format @ (TableFormat::Delta | TableFormat::Parquet)) => return Ok(format),
            _ => format!(
                "data_source_format={}",
                data_source_format.unwrap_or(ABSENT_DATA_SOURCE_FORMAT)
            ),
        },
        _ => format!("table_type={raw_table_type}"),
    };
    Err(SkipReason::NotDeltaBaseTable { detail })
}

fn neutral_table_format(
    data_source_format: Option<&str>,
    table: &str,
) -> Result<TableFormat, UdfError> {
    format_of(data_source_format).ok_or_else(|| {
        UdfError::User(format!(
            "Unity Catalog table {table} reports data_source_format={}, which names no table \
             format this engine can plan (expected {DELTA_DATA_SOURCE_FORMAT}, \
             {ICEBERG_DATA_SOURCE_FORMAT}, or {PARQUET_DATA_SOURCE_FORMAT})",
            data_source_format
                .filter(|format| !format.trim().is_empty())
                .unwrap_or(ABSENT_DATA_SOURCE_FORMAT)
        ))
    })
}

fn format_of(data_source_format: Option<&str>) -> Option<TableFormat> {
    match data_source_format? {
        DELTA_DATA_SOURCE_FORMAT => Some(TableFormat::Delta),
        ICEBERG_DATA_SOURCE_FORMAT => Some(TableFormat::Iceberg),
        PARQUET_DATA_SOURCE_FORMAT => Some(TableFormat::Parquet),
        _ => None,
    }
}

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

/// `storage_location` and `data_source_format` are optional because a VIEW
/// carries neither.
#[derive(Deserialize)]
struct TableInfo {
    name: String,
    table_type: String,
    #[serde(default)]
    storage_location: Option<String>,
    #[serde(default)]
    data_source_format: Option<String>,
    #[serde(default)]
    table_id: Option<String>,
    #[serde(default)]
    columns: Vec<ColumnInfo>,
}

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    type_name: String,
    #[serde(default)]
    type_precision: Option<u32>,
    #[serde(default)]
    type_scale: Option<u32>,
    #[serde(default)]
    type_json: Option<String>,
    #[serde(default)]
    partition_index: Option<u32>,
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
