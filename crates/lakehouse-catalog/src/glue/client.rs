use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use aws_sdk_glue::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_glue::types::{Column, Table};
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};

use crate::client::iceberg_catalog_table;
use crate::redaction::redact_error_text;
use crate::sigv4::required_signing_region;
use crate::{
    CatalogClient, CatalogColumn, CatalogListing, CatalogPartition, CatalogTable,
    CatalogTableIdent, CatalogTableType, ColumnSourceType, ConnectionCreds, PartitionFormat,
    SkipReason, SkippedTable, StorageBackend, TableFormat, catalog_identifier_string,
    read_iceberg_metadata_file,
};

use super::partitions::neutral_partition;
use super::routing::{Route, route};
use super::sdk::{catalog_id, error_text, glue_client, next_page_token};
use super::trim_location;

const METADATA_READ_CONCURRENCY: usize = 16;

/// Glue Data Catalog metadata as neutral tables and partitions.
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
        let secrets = creds
            .key_secret_values()
            .into_iter()
            .chain(storage.secret_values())
            .map(str::to_string)
            .collect();
        Ok(Self {
            client: glue_client(address, &creds, region),
            storage,
            catalog_id: catalog_id(&creds.warehouse),
            secrets,
        })
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
                    catalog_identifier_string(ident)
                ))
            })?;
            return Ok(vec![CatalogPartition {
                values: BTreeMap::new(),
                location,
                format: PartitionFormat::Parquet,
            }]);
        }
        let database = glue_database(&ident.namespace)?;
        let name = catalog_identifier_string(ident);
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
                .map_err(|error| self.glue_error("GetPartitions", &table_subject(ident), &error))?;
            for partition in page.partitions() {
                partitions.push(
                    neutral_partition(&name, &table.partition_columns, partition)
                        .map_err(|message| UdfError::User(self.redact(&message)))?,
                );
            }
            token = next_page_token(page.next_token(), page.partitions().len());
            if token.is_none() {
                return Ok(partitions);
            }
        }
    }

    async fn tables(&self, database: &str) -> Result<Vec<Table>, UdfError> {
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
                    self.glue_error("GetTables", &format!("database '{database}'"), &error)
                })?;
            token = next_page_token(page.next_token(), page.table_list().len());
            tables.extend(page.table_list.unwrap_or_default());
            if token.is_none() {
                return Ok(tables);
            }
        }
    }

    /// `metadata.json` is the schema authority; Glue's Hive-string column copies are ignored.
    async fn listed_table(
        &self,
        ident: CatalogTableIdent,
        table: Table,
        route: Route,
    ) -> Result<CatalogTable, UdfError> {
        let Route::Iceberg { metadata_location } = route else {
            return planned_table(ident, &table, route);
        };
        let metadata = read_iceberg_metadata_file(
            &self.storage,
            &metadata_location,
            &catalog_identifier_string(&ident),
        )
        .await
        .map_err(|error| UdfError::User(self.redact(&error.to_string())))?;
        Ok(iceberg_catalog_table(
            ident,
            &metadata,
            Some(metadata_location),
        ))
    }

    fn glue_error<E: ProvideErrorMetadata>(
        &self,
        operation: &str,
        subject: &str,
        error: &SdkError<E>,
    ) -> UdfError {
        UdfError::User(self.redact(&error_text(error, operation, subject)))
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
            let mut admitted = Vec::new();
            let mut skipped = Vec::new();
            for table in self.tables(database).await? {
                let ident = CatalogTableIdent {
                    namespace: namespace.clone(),
                    name: table.name().to_string(),
                };
                match route_of(&table) {
                    Route::Skip(detail) => skipped.push(SkippedTable {
                        ident,
                        reason: SkipReason::NotPlannableGlueTable { detail },
                    }),
                    route => admitted.push(self.listed_table(ident, table, route)),
                }
            }
            let tables = stream::iter(admitted)
                .buffered(METADATA_READ_CONCURRENCY)
                .try_collect()
                .await?;
            Ok(CatalogListing { tables, skipped })
        })
    }

    /// The table as a pushdown plans it, from `GetTable` alone: an Iceberg table carries its
    /// metadata location and no column, because the Iceberg planner reads the metadata file.
    fn load_table(
        &self,
        ident: &CatalogTableIdent,
    ) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>> {
        let ident = ident.clone();
        Box::pin(async move {
            let database = glue_database(&ident.namespace)?;
            let subject = table_subject(&ident);
            let table = self
                .client
                .get_table()
                .set_catalog_id(self.catalog_id.clone())
                .database_name(database)
                .name(&ident.name)
                .send()
                .await
                .map_err(|error| self.glue_error("GetTable", &subject, &error))?
                .table
                .ok_or_else(|| {
                    UdfError::User(format!("Glue GetTable returned no table for {subject}"))
                })?;
            let route = route_of(&table);
            planned_table(ident, &table, route)
        })
    }
}

/// Split at the first dot: a Glue identifier is `database.table`, and a database name holds no dot.
pub fn parse_glue_table_ident(table_identifier: &str) -> Result<CatalogTableIdent, UdfError> {
    match table_identifier.split_once('.') {
        Some((database, name)) if !database.trim().is_empty() && !name.trim().is_empty() => {
            Ok(CatalogTableIdent {
                namespace: vec![database.to_string()],
                name: name.to_string(),
            })
        }
        _ => Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no Glue table \
             — a Glue table is addressed as 'database.table'; drop and recreate the virtual \
             schema"
        ))),
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
    format!("table '{}'", catalog_identifier_string(ident))
}

fn route_of(table: &Table) -> Route {
    route(
        table.table_type(),
        table.parameters().unwrap_or(&Default::default()),
        table
            .storage_descriptor()
            .and_then(|descriptor| descriptor.input_format()),
    )
}

fn planned_table(
    ident: CatalogTableIdent,
    table: &Table,
    route: Route,
) -> Result<CatalogTable, UdfError> {
    let descriptor = table.storage_descriptor();
    let (format, metadata_location, columns, partition_keys): (_, _, &[Column], &[Column]) =
        match route {
            Route::Iceberg { metadata_location } => {
                (TableFormat::Iceberg, Some(metadata_location), &[], &[])
            }
            Route::Parquet => (
                TableFormat::Parquet,
                None,
                descriptor.map_or(&[], |descriptor| descriptor.columns()),
                table.partition_keys(),
            ),
            Route::Skip(detail) => {
                return Err(UdfError::User(format!(
                    "Glue table '{}' cannot be planned: {detail}",
                    catalog_identifier_string(&ident)
                )));
            }
        };
    Ok(CatalogTable {
        ident,
        table_type: CatalogTableType::Table,
        storage_location: descriptor
            .and_then(|descriptor| descriptor.location())
            .filter(|location| !location.is_empty())
            .map(|location| trim_location(location).to_string()),
        format,
        vended_credential_key: None,
        partition_columns: partition_keys
            .iter()
            .map(|key| key.name().to_string())
            .collect(),
        columns: columns
            .iter()
            .chain(partition_keys)
            .map(|column| CatalogColumn {
                name: column.name().to_string(),
                source_type: ColumnSourceType::Glue {
                    hive_type: column.r#type().unwrap_or_default().to_string(),
                },
            })
            .collect(),
        metadata_location,
    })
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
