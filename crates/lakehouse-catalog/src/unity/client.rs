use crate::catalog_http::CatalogHttp;
use crate::error_chain;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::redaction::redact_error_text;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogTable, CatalogTableIdent,
    CatalogTableType, ColumnSourceType, ConnectionCreds, SkipReason, SkippedTable, TableAdmission,
    TableFormat,
};

use super::auth::{UnityAuth, resolve_unity_auth};
use super::vended::TemporaryTableCredentials;

/// Identical on OSS and Databricks-managed Unity Catalog; deliberately not the
/// Iceberg-REST compatibility endpoint or the `delta/v1` API.
const UNITY_REST_BASE_PATH: &str = "/api/2.1/unity-catalog";

pub struct UnityCatalogSession {
    http: CatalogHttp,
    base_url: String,
    auth: UnityAuth,
    creds: ConnectionCreds,
}

impl UnityCatalogSession {
    /// Issues no request: an OAuth grant, if any, is deferred to the first request.
    pub fn new(address: &str, creds: ConnectionCreds) -> Self {
        let http = CatalogHttp::new();
        let base_url = format!("{}{UNITY_REST_BASE_PATH}", address.trim_end_matches('/'));
        let auth = resolve_unity_auth(&http, address, &creds);
        Self {
            http,
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
            .http
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
        // The full listing leaves `omit_columns` unset: its inline `columns[]` is the column
        // source, avoiding a per-table get-table.
        self.collect_pages::<TablesPage>(
            &url,
            &[("catalog_name", catalog), ("schema_name", schema)],
            "list tables",
        )
        .await
    }

    async fn list_table_names(&self, catalog: &str, schema: &str) -> Result<Vec<String>, UdfError> {
        let url = format!("{}/tables", self.base_url);
        let names = self
            .collect_pages::<TableNamesPage>(
                &url,
                &identifier_listing_query(catalog, schema),
                "list table names",
            )
            .await?;
        Ok(names.into_iter().map(|entry| entry.name).collect())
    }

    async fn get_table_info(&self, full_name: &str) -> Result<TableInfo, UdfError> {
        self.fetch_table_info(full_name).await.map_err(|e| {
            UdfError::User(format!(
                "failed to load Unity Catalog table '{full_name}': {e}"
            ))
        })
    }

    async fn fetch_table_info(&self, full_name: &str) -> Result<TableInfo, UdfError> {
        let mut url = url::Url::parse(&self.base_url)
            .map_err(|e| UdfError::User(format!("invalid Unity Catalog base URL: {e}")))?;
        // Percent-encodes reserved/non-ASCII characters into one path segment.
        url.path_segments_mut()
            .map_err(|_| UdfError::User("Unity Catalog base URL cannot be a base".into()))?
            .push("tables")
            .push(full_name);
        let builder = self.http.get(url).header("accept", "application/json");
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
                .http
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
        let request = builder.build().map_err(|e| {
            UdfError::User(format!(
                "Unity Catalog {kind} request failed: {}",
                redact(&error_chain(&e))
            ))
        })?;
        let answer = self.http.send(request).await.map_err(|e| {
            UdfError::User(format!(
                "Unity Catalog {kind} request failed: {}",
                e.render(redact)
            ))
        })?;
        if !answer.status.is_success() {
            let body = String::from_utf8_lossy(&answer.body);
            return Err(UdfError::User(format!(
                "Unity Catalog {kind} request failed with HTTP {}: {}",
                answer.status.as_u16(),
                redact(&body)
            )));
        }
        serde_json::from_slice::<T>(&answer.body).map_err(|e| {
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
            Ok(infos
                .into_iter()
                .map(|info| {
                    let ident = CatalogTableIdent {
                        namespace: namespace.clone(),
                        name: info.name.clone(),
                    };
                    admit(ident, info)
                })
                .collect())
        })
    }

    fn list_table_idents(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CatalogTableIdent>, UdfError>> + Send + '_>> {
        let namespace = namespace.to_vec();
        Box::pin(async move {
            let (catalog, schema) = unity_namespace(&namespace)?;
            let names = self.list_table_names(catalog, schema).await?;
            Ok(names
                .into_iter()
                .map(|name| CatalogTableIdent {
                    namespace: namespace.clone(),
                    name,
                })
                .collect())
        })
    }

    fn load_listed_tables(
        &self,
        idents: &[CatalogTableIdent],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        let idents = idents.to_vec();
        Box::pin(async move {
            let mut admissions = Vec::with_capacity(idents.len());
            for ident in idents {
                let info = self.get_table_info(&full_name(&ident)).await?;
                admissions.push(admit(ident, info));
            }
            Ok(admissions.into_iter().collect())
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

fn unity_namespace(namespace: &[String]) -> Result<(&str, &str), UdfError> {
    match namespace {
        [catalog, schema] => Ok((catalog.as_str(), schema.as_str())),
        _ => Err(UdfError::User(format!(
            "a Unity Catalog namespace must name a catalog and a schema (two segments), but {} were given",
            namespace.len()
        ))),
    }
}

/// Databricks and OSS Unity Catalog honor all three omissions and keep each entry's `name`.
fn identifier_listing_query<'a>(catalog: &'a str, schema: &'a str) -> [(&'static str, &'a str); 5] {
    [
        ("catalog_name", catalog),
        ("schema_name", schema),
        ("omit_columns", "true"),
        ("omit_properties", "true"),
        ("omit_username", "true"),
    ]
}

/// The one admission rule both listings share, so `SKIPPED_TABLES` reads the same after either.
fn admit(ident: CatalogTableIdent, info: TableInfo) -> TableAdmission {
    match admission(&info.table_type, info.data_source_format.as_deref()) {
        Err(reason) => TableAdmission::Skipped(SkippedTable { ident, reason }),
        Ok(format) => TableAdmission::Admitted(neutral_table(ident, info, format)),
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
        metadata_location: None,
        properties: info.properties.unwrap_or_default(),
    }
}

fn neutral_column(column: ColumnInfo) -> CatalogColumn {
    CatalogColumn::new(
        column.name,
        ColumnSourceType::Unity {
            type_json: column.type_json,
        },
    )
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

#[derive(Deserialize)]
struct TableNamesPage {
    #[serde(default)]
    tables: Vec<TableName>,
    #[serde(default)]
    next_page_token: Option<String>,
}

impl PagedResponse for TableNamesPage {
    type Item = TableName;
    fn into_items(self) -> Vec<TableName> {
        self.tables
    }
    fn next_page_token(&self) -> Option<String> {
        self.next_page_token.clone()
    }
}

#[derive(Deserialize)]
struct TableName {
    name: String,
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
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(default)]
    type_json: Option<String>,
    #[serde(default)]
    partition_index: Option<u32>,
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
