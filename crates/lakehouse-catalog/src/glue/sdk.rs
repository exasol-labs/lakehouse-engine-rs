use std::time::Duration;

use aws_sdk_glue::config::retry::RetryConfig;
use aws_sdk_glue::config::timeout::TimeoutConfig;
use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_glue::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_glue::types::{Column, Partition, Table};
use iceberg::spec::TableMetadata;

use crate::{ConnectionCreds, StorageBackend};

use super::source::{
    GlueColumn, GlueFailure, GluePartition, GlueSource, GlueTable, SourceFuture,
    classify_service_error,
};

const MAX_ATTEMPTS: u32 = 5;
/// Bounds each call, retries included, because it runs inside a pushdown.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const CREDENTIALS_PROVIDER: &str = "exasol-connection";

/// The `aws-sdk-glue` client and the CONNECTION's storage behind [`GlueSource`].
pub(super) struct SdkGlueSource {
    client: aws_sdk_glue::Client,
    storage: StorageBackend,
    catalog_id: Option<String>,
}

impl SdkGlueSource {
    pub(super) fn new(
        address: &str,
        storage: StorageBackend,
        creds: &ConnectionCreds,
        region: String,
    ) -> Self {
        Self {
            client: aws_sdk_glue::Client::from_conf(glue_config(address, creds, region).build()),
            storage,
            catalog_id: catalog_id(&creds.warehouse),
        }
    }
}

impl GlueSource for SdkGlueSource {
    fn tables<'a>(
        &'a self,
        database: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GlueTable>, GlueFailure>> {
        Box::pin(async move {
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
                    .map_err(failure)?;
                token = next_page_token(page.next_token(), page.table_list().len());
                tables.extend(page.table_list().iter().map(glue_table));
                if token.is_none() {
                    return Ok(tables);
                }
            }
        })
    }

    fn table<'a>(
        &'a self,
        database: &'a str,
        name: &'a str,
    ) -> SourceFuture<'a, Result<Option<GlueTable>, GlueFailure>> {
        Box::pin(async move {
            let output = self
                .client
                .get_table()
                .set_catalog_id(self.catalog_id.clone())
                .database_name(database)
                .name(name)
                .send()
                .await
                .map_err(failure)?;
            Ok(output.table().map(glue_table))
        })
    }

    fn partitions<'a>(
        &'a self,
        database: &'a str,
        table: &'a str,
    ) -> SourceFuture<'a, Result<Vec<GluePartition>, GlueFailure>> {
        Box::pin(async move {
            let mut partitions = Vec::new();
            let mut token = None;
            loop {
                let page = self
                    .client
                    .get_partitions()
                    .set_catalog_id(self.catalog_id.clone())
                    .database_name(database)
                    .table_name(table)
                    .exclude_column_schema(true)
                    .set_next_token(token.take())
                    .send()
                    .await
                    .map_err(failure)?;
                token = next_page_token(page.next_token(), page.partitions().len());
                partitions.extend(page.partitions().iter().map(glue_partition));
                if token.is_none() {
                    return Ok(partitions);
                }
            }
        })
    }

    fn iceberg_metadata<'a>(
        &'a self,
        location: &'a str,
    ) -> SourceFuture<'a, Result<TableMetadata, String>> {
        Box::pin(async move {
            TableMetadata::read_from(&self.storage.file_io(), location)
                .await
                .map_err(|error| error.to_string())
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
        .retry_config(RetryConfig::standard().with_max_attempts(MAX_ATTEMPTS))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(OPERATION_TIMEOUT)
                .build(),
        )
}

/// Glue rejects an empty `CatalogId`, so a blank `warehouse` sends none and Glue uses the
/// caller's account.
fn catalog_id(warehouse: &str) -> Option<String> {
    Some(warehouse.to_string()).filter(|id| !id.trim().is_empty())
}

/// Glue can return a non-null token after the last page, so an empty page also ends a listing.
fn next_page_token(token: Option<&str>, page_len: usize) -> Option<String> {
    if page_len == 0 {
        return None;
    }
    token.filter(|token| !token.is_empty()).map(str::to_string)
}

fn failure<E>(error: SdkError<E>) -> GlueFailure
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    match &error {
        SdkError::ServiceError(context) => classify_service_error(
            context.err().code(),
            context.err().message(),
            context.raw().status().as_u16(),
        ),
        SdkError::TimeoutError(_) => GlueFailure::TimedOut {
            seconds: OPERATION_TIMEOUT.as_secs(),
        },
        other => GlueFailure::Request(DisplayErrorContext(other).to_string()),
    }
}

fn glue_table(table: &Table) -> GlueTable {
    let descriptor = table.storage_descriptor();
    GlueTable {
        name: table.name().to_string(),
        table_type: table.table_type().map(str::to_string),
        parameters: table.parameters().cloned().unwrap_or_default(),
        input_format: descriptor
            .and_then(|descriptor| descriptor.input_format())
            .map(str::to_string),
        location: descriptor
            .and_then(|descriptor| descriptor.location())
            .map(str::to_string),
        columns: descriptor
            .map(|descriptor| descriptor.columns().iter().map(glue_column).collect())
            .unwrap_or_default(),
        partition_keys: table.partition_keys().iter().map(glue_column).collect(),
    }
}

fn glue_column(column: &Column) -> GlueColumn {
    GlueColumn {
        name: column.name().to_string(),
        hive_type: column.r#type().unwrap_or_default().to_string(),
    }
}

fn glue_partition(partition: &Partition) -> GluePartition {
    let descriptor = partition.storage_descriptor();
    GluePartition {
        values: partition.values().to_vec(),
        location: descriptor
            .and_then(|descriptor| descriptor.location())
            .map(str::to_string),
        input_format: descriptor
            .and_then(|descriptor| descriptor.input_format())
            .map(str::to_string),
    }
}

#[cfg(test)]
#[path = "sdk_tests.rs"]
mod tests;
