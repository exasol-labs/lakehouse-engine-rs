use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use aws_sdk_glue::config::retry::RetryConfig;
use aws_sdk_glue::config::timeout::TimeoutConfig;
use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_glue::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_glue::types::{Column, Table};
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::spec::TableMetadata;

use crate::client::{dotted_identifier, iceberg_catalog_table};
use crate::redaction::redact_secret_values;
use crate::sigv4::required_signing_region;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogPartition, CatalogTable,
    CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds, SkipReason,
    SkippedTable, StorageBackend, TableFormat,
};

use super::partitions::neutral_partition;
use super::routing::{Route, route};
use super::trim_location;

pub(super) const MAX_ATTEMPTS: u32 = 5;
/// Bounds each call, retries included, because it runs inside a pushdown.
pub(super) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_READ_CONCURRENCY: usize = 16;
const CREDENTIALS_PROVIDER: &str = "exasol-connection";
const NOT_FOUND_CODE: &str = "EntityNotFoundException";

/// Glue Data Catalog metadata as neutral tables and partitions. It signs only with the
/// CONNECTION's static key, never an environment, profile, or instance-metadata identity.
pub struct GlueCatalogSession {
    client: aws_sdk_glue::Client,
    storage: StorageBackend,
    catalog_id: Option<String>,
    secrets: Vec<String>,
}

impl GlueCatalogSession {
    /// Issues no request. Fails when no signing region resolves for `address`.
    pub fn new(
        address: &str,
        storage: StorageBackend,
        creds: ConnectionCreds,
    ) -> Result<Self, UdfError> {
        let region = required_signing_region(&creds, address)?;
        let config = glue_config(address, &creds, region).build();
        Ok(Self::from_config(config, storage, &creds))
    }

    fn from_config(
        config: aws_sdk_glue::Config,
        storage: StorageBackend,
        creds: &ConnectionCreds,
    ) -> Self {
        let secrets = [&creds.access_key, &creds.secret_key]
            .into_iter()
            .chain(creds.session_token.as_ref())
            .cloned()
            .chain(storage.secret_values().into_iter().map(str::to_string))
            .filter(|secret| !secret.is_empty())
            .collect();
        Self {
            client: aws_sdk_glue::Client::from_conf(config),
            catalog_id: Some(creds.warehouse.clone()).filter(|id| !id.trim().is_empty()),
            storage,
            secrets,
        }
    }

    /// The table as a pushdown plans it, from `GetTable` alone: an Iceberg table carries its
    /// metadata location and no column, because the Iceberg planner reads the metadata file.
    pub async fn load_table_for_planning(
        &self,
        ident: &CatalogTableIdent,
    ) -> Result<CatalogTable, UdfError> {
        let table = self.get_table(ident).await?;
        match route_of(&table) {
            Route::Iceberg { metadata_location } => Ok(CatalogTable {
                ident: ident.clone(),
                table_type: CatalogTableType::Table,
                storage_location: table_location(&table),
                format: TableFormat::Iceberg,
                vended_credential_key: None,
                partition_columns: Vec::new(),
                columns: Vec::new(),
                metadata_location: Some(metadata_location),
            }),
            Route::Parquet => Ok(parquet_table(ident.clone(), &table)),
            Route::Skip(detail) => Err(not_plannable(ident, &detail)),
        }
    }

    /// The registered partitions of a Parquet table, each value keyed by the matching entry
    /// of `partition_columns`, the table's partition keys in Glue order.
    pub async fn partitions(
        &self,
        ident: &CatalogTableIdent,
        partition_columns: &[String],
    ) -> Result<Vec<CatalogPartition>, UdfError> {
        let database = glue_database(&ident.namespace)?;
        let table = dotted_identifier(ident);
        let mut partitions = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .client
                .get_partitions()
                .set_catalog_id(self.catalog_id.clone())
                .database_name(database)
                .table_name(&ident.name)
                .exclude_column_schema(true)
                .set_next_token(token.take())
                .send()
                .await
                .map_err(|error| self.glue_error("GetPartitions", &table_subject(ident), error))?;
            token = next_page_token(page.next_token(), page.partitions().len());
            for partition in page.partitions() {
                let neutral = neutral_partition(&table, partition_columns, partition)
                    .map_err(|message| UdfError::User(self.redact(&message)))?;
                partitions.push(neutral);
            }
            if token.is_none() {
                return Ok(partitions);
            }
        }
    }

    async fn database_tables(&self, database: &str) -> Result<Vec<Table>, UdfError> {
        let mut tables = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .client
                .get_tables()
                .set_catalog_id(self.catalog_id.clone())
                .database_name(database)
                .set_next_token(token.take())
                .send()
                .await
                .map_err(|error| {
                    self.glue_error("GetTables", &format!("database '{database}'"), error)
                })?;
            token = next_page_token(page.next_token(), page.table_list().len());
            tables.extend(page.table_list.unwrap_or_default());
            if token.is_none() {
                return Ok(tables);
            }
        }
    }

    async fn get_table(&self, ident: &CatalogTableIdent) -> Result<Table, UdfError> {
        let database = glue_database(&ident.namespace)?;
        let output = self
            .client
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .name(&ident.name)
            .send()
            .await
            .map_err(|error| self.glue_error("GetTable", &table_subject(ident), error))?;
        output.table.ok_or_else(|| {
            UdfError::User(format!(
                "Glue GetTable returned no table for {}",
                table_subject(ident)
            ))
        })
    }

    async fn listing_table(
        &self,
        ident: CatalogTableIdent,
        table: Table,
        route: Route,
    ) -> Result<CatalogTable, UdfError> {
        match route {
            Route::Iceberg { metadata_location } => {
                self.iceberg_table(ident, metadata_location).await
            }
            Route::Parquet => Ok(parquet_table(ident, &table)),
            Route::Skip(detail) => Err(not_plannable(&ident, &detail)),
        }
    }

    /// `metadata.json` is the schema authority; Glue's Hive-string column copies are ignored.
    async fn iceberg_table(
        &self,
        ident: CatalogTableIdent,
        metadata_location: String,
    ) -> Result<CatalogTable, UdfError> {
        let metadata = TableMetadata::read_from(&self.storage.file_io(), &metadata_location)
            .await
            .map_err(|error| {
                UdfError::User(self.redact(&format!(
                    "Glue Iceberg table '{}': cannot read its metadata file \
                     '{metadata_location}': {error}",
                    dotted_identifier(&ident)
                )))
            })?;
        Ok(iceberg_catalog_table(
            ident,
            &metadata,
            Some(metadata_location),
        ))
    }

    fn glue_error<E>(&self, operation: &str, subject: &str, error: SdkError<E>) -> UdfError
    where
        E: ProvideErrorMetadata + std::error::Error + 'static,
    {
        let text = match &error {
            SdkError::ServiceError(context) => service_error_text(
                operation,
                subject,
                context.err(),
                context.raw().status().as_u16(),
            ),
            SdkError::TimeoutError(_) => format!(
                "Glue {operation} for {subject} did not complete within {} seconds, retries \
                 included",
                OPERATION_TIMEOUT.as_secs()
            ),
            other => format!(
                "Glue {operation} request for {subject} failed: {}",
                DisplayErrorContext(other)
            ),
        };
        UdfError::User(self.redact(&text))
    }

    fn redact(&self, text: &str) -> String {
        let secrets: Vec<&str> = self.secrets.iter().map(String::as_str).collect();
        redact_secret_values(text, &secrets)
    }
}

impl CatalogClient for GlueCatalogSession {
    fn list_tables(
        &self,
        namespace: &[String],
    ) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>> {
        let namespace = namespace.to_vec();
        Box::pin(async move {
            let database = glue_database(&namespace)?;
            let mut admitted = Vec::new();
            let mut skipped = Vec::new();
            for table in self.database_tables(database).await? {
                let ident = CatalogTableIdent {
                    namespace: namespace.clone(),
                    name: table.name().to_string(),
                };
                match route_of(&table) {
                    Route::Skip(detail) => skipped.push(SkippedTable {
                        ident,
                        reason: SkipReason::NotPlannableGlueTable { detail },
                    }),
                    route => admitted.push(self.listing_table(ident, table, route)),
                }
            }
            let tables = stream::iter(admitted)
                .buffered(METADATA_READ_CONCURRENCY)
                .try_collect()
                .await?;
            Ok(CatalogListing { tables, skipped })
        })
    }

    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let table = self.get_table(&ident).await?;
            let route = route_of(&table);
            self.listing_table(ident, table, route).await
        })
    }
}

fn glue_config(
    address: &str,
    creds: &ConnectionCreds,
    region: String,
) -> aws_sdk_glue::config::Builder {
    aws_sdk_glue::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            creds.access_key.clone(),
            creds.secret_key.clone(),
            creds.session_token.clone(),
            None,
            CREDENTIALS_PROVIDER,
        ))
        .region(Region::new(region))
        .endpoint_url(address)
        .retry_config(glue_retry_config())
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(OPERATION_TIMEOUT)
                .build(),
        )
}

fn glue_retry_config() -> RetryConfig {
    RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS)
}

fn glue_database(namespace: &[String]) -> Result<&str, UdfError> {
    match namespace {
        [database] if !database.is_empty() => Ok(database),
        _ => Err(UdfError::User(format!(
            "a Glue NAMESPACE names exactly one database, but '{}' does not",
            namespace.join(".")
        ))),
    }
}

fn table_subject(ident: &CatalogTableIdent) -> String {
    format!("table '{}'", dotted_identifier(ident))
}

fn not_plannable(ident: &CatalogTableIdent, detail: &str) -> UdfError {
    UdfError::User(format!(
        "Glue table '{}' cannot be planned: {detail}",
        dotted_identifier(ident)
    ))
}

/// Glue can return a non-null token after the last page, so an empty page also ends a listing.
fn next_page_token(token: Option<&str>, page_len: usize) -> Option<String> {
    if page_len == 0 {
        return None;
    }
    token.filter(|token| !token.is_empty()).map(str::to_string)
}

fn route_of(table: &Table) -> Route {
    route(
        table.table_type(),
        table.parameters(),
        table
            .storage_descriptor()
            .and_then(|descriptor| descriptor.input_format()),
    )
}

fn table_location(table: &Table) -> Option<String> {
    table
        .storage_descriptor()
        .and_then(|descriptor| descriptor.location())
        .filter(|location| !location.is_empty())
        .map(|location| trim_location(location).to_string())
}

fn parquet_table(ident: CatalogTableIdent, table: &Table) -> CatalogTable {
    let partition_keys = table.partition_keys();
    let storage_columns = table
        .storage_descriptor()
        .map(|descriptor| descriptor.columns())
        .unwrap_or_default();
    CatalogTable {
        ident,
        table_type: CatalogTableType::Table,
        storage_location: table_location(table),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: partition_keys
            .iter()
            .map(|key| key.name().to_string())
            .collect(),
        columns: storage_columns
            .iter()
            .chain(partition_keys)
            .map(glue_column)
            .collect(),
        metadata_location: None,
    }
}

fn glue_column(column: &Column) -> CatalogColumn {
    CatalogColumn {
        name: column.name().to_string(),
        source_type: ColumnSourceType::Glue {
            hive_type: column.r#type().unwrap_or_default().to_string(),
        },
    }
}

fn service_error_text(
    operation: &str,
    subject: &str,
    error: &impl ProvideErrorMetadata,
    status: u16,
) -> String {
    let message = error.message().unwrap_or("(no message)");
    match error.code() {
        Some(NOT_FOUND_CODE) => format!(
            "Glue {operation} failed: {subject} does not exist ({NOT_FOUND_CODE}: {message})"
        ),
        Some(code) if is_signing_time_rejection(code, message) => format!(
            "Glue {operation} failed for {subject}: the Exasol node's clock differs from AWS \
             time, so AWS rejected the request's signing time; synchronize the node's clock \
             ({code}: {message})"
        ),
        Some(code) => format!("Glue {operation} failed for {subject}: {code}: {message}"),
        None => format!("Glue {operation} failed for {subject}: HTTP {status}: {message}"),
    }
}

/// AWS's own codes and texts for a signature outside its time window. One that reaches here
/// survived the SDK's skew-corrected retries.
fn is_signing_time_rejection(code: &str, message: &str) -> bool {
    matches!(code, "RequestTimeTooSkewed" | "RequestExpired")
        || (code == "InvalidSignatureException"
            && (message.starts_with("Signature expired")
                || message.starts_with("Signature not yet current")))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
