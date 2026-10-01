use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};

use crate::client::{dotted_identifier, iceberg_catalog_table};
use crate::redaction::redact_error_text;
use crate::sigv4::required_signing_region;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogPartition, CatalogTable,
    CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds, SkipReason,
    SkippedTable, StorageBackend, TableFormat,
};

use super::partitions::neutral_partition;
use super::routing::{PARQUET_INPUT_FORMAT, Route, route};
use super::sdk::SdkGlueSource;
use super::source::{GlueColumn, GlueFailure, GlueSource, GlueTable};
use super::trim_location;

const METADATA_READ_CONCURRENCY: usize = 16;

/// Glue Data Catalog metadata as neutral tables and partitions.
pub struct GlueCatalogSession {
    source: Box<dyn GlueSource>,
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
        let secrets = [&creds.access_key, &creds.secret_key]
            .into_iter()
            .chain(creds.session_token.as_ref())
            .cloned()
            .chain(storage.secret_values().into_iter().map(str::to_string))
            .filter(|secret| !secret.is_empty())
            .collect();
        Ok(Self {
            source: Box::new(SdkGlueSource::new(address, storage, &creds, region)),
            secrets,
        })
    }

    /// The table as a pushdown plans it, from `GetTable` alone, without reading a metadata file.
    pub async fn load_table_for_planning(
        &self,
        ident: &CatalogTableIdent,
    ) -> Result<CatalogTable, UdfError> {
        let table = self.get_table(ident).await?;
        planning_table(ident, &table)
    }

    /// The registered partitions of a Parquet table, each value keyed by its partition column.
    /// An unpartitioned table registers none, so it reads as one partition at its own location.
    pub async fn partitions(
        &self,
        table: &CatalogTable,
    ) -> Result<Vec<CatalogPartition>, UdfError> {
        let ident = &table.ident;
        if table.partition_columns.is_empty() {
            let location = table.storage_location.clone().ok_or_else(|| {
                UdfError::User(format!(
                    "Glue table '{}' declares no storage location",
                    dotted_identifier(ident)
                ))
            })?;
            return Ok(vec![CatalogPartition {
                values: BTreeMap::new(),
                location,
                input_format: PARQUET_INPUT_FORMAT.to_string(),
            }]);
        }
        let database = glue_database(&ident.namespace)?;
        let name = dotted_identifier(ident);
        self.source
            .partitions(database, &ident.name)
            .await
            .map_err(|failure| self.glue_error("GetPartitions", &table_subject(ident), &failure))?
            .iter()
            .map(|partition| {
                neutral_partition(&name, &table.partition_columns, partition)
                    .map_err(|message| UdfError::User(self.redact(&message)))
            })
            .collect()
    }

    async fn get_table(&self, ident: &CatalogTableIdent) -> Result<GlueTable, UdfError> {
        let database = glue_database(&ident.namespace)?;
        let subject = table_subject(ident);
        self.source
            .table(database, &ident.name)
            .await
            .map_err(|failure| self.glue_error("GetTable", &subject, &failure))?
            .ok_or_else(|| UdfError::User(format!("Glue GetTable returned no table for {subject}")))
    }

    async fn listing_table(
        &self,
        ident: CatalogTableIdent,
        table: GlueTable,
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
        let metadata = self
            .source
            .iceberg_metadata(&metadata_location)
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

    fn glue_error(&self, operation: &str, subject: &str, failure: &GlueFailure) -> UdfError {
        UdfError::User(self.redact(&failure.text(operation, subject)))
    }

    fn redact(&self, text: &str) -> String {
        let secrets: Vec<&str> = self.secrets.iter().map(String::as_str).collect();
        redact_error_text(text, &secrets)
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
            let tables = self.source.tables(database).await.map_err(|failure| {
                self.glue_error("GetTables", &format!("database '{database}'"), &failure)
            })?;
            let mut admitted = Vec::new();
            let mut skipped = Vec::new();
            for table in tables {
                let ident = CatalogTableIdent {
                    namespace: namespace.clone(),
                    name: table.name.clone(),
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

fn route_of(table: &GlueTable) -> Route {
    route(
        table.table_type.as_deref(),
        &table.parameters,
        table.input_format.as_deref(),
    )
}

/// An Iceberg table carries its metadata location and no column, because the Iceberg
/// planner reads the metadata file.
fn planning_table(ident: &CatalogTableIdent, table: &GlueTable) -> Result<CatalogTable, UdfError> {
    match route_of(table) {
        Route::Iceberg { metadata_location } => Ok(CatalogTable {
            ident: ident.clone(),
            table_type: CatalogTableType::Table,
            storage_location: table_location(table),
            format: TableFormat::Iceberg,
            vended_credential_key: None,
            partition_columns: Vec::new(),
            columns: Vec::new(),
            metadata_location: Some(metadata_location),
        }),
        Route::Parquet => Ok(parquet_table(ident.clone(), table)),
        Route::Skip(detail) => Err(not_plannable(ident, &detail)),
    }
}

fn table_location(table: &GlueTable) -> Option<String> {
    table
        .location
        .as_deref()
        .filter(|location| !location.is_empty())
        .map(|location| trim_location(location).to_string())
}

fn parquet_table(ident: CatalogTableIdent, table: &GlueTable) -> CatalogTable {
    CatalogTable {
        ident,
        table_type: CatalogTableType::Table,
        storage_location: table_location(table),
        format: TableFormat::Parquet,
        vended_credential_key: None,
        partition_columns: table
            .partition_keys
            .iter()
            .map(|key| key.name.clone())
            .collect(),
        columns: table
            .columns
            .iter()
            .chain(&table.partition_keys)
            .map(catalog_column)
            .collect(),
        metadata_location: None,
    }
}

fn catalog_column(column: &GlueColumn) -> CatalogColumn {
    CatalogColumn {
        name: column.name.clone(),
        source_type: ColumnSourceType::Glue {
            hive_type: column.hive_type.clone(),
        },
    }
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
