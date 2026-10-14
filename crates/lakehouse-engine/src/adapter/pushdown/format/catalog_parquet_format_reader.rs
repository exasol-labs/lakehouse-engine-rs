use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowType;
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};
use lakehouse_catalog::{
    CatalogPartition, CatalogTable, GlueCatalogSession, PartitionFormat, StorageBackend,
};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde_json::Value as Json;

use super::parquet_format_reader::file_entry;
use super::partition_predicate::PartitionPredicate;
use super::unity_table_storage::{UnityTableStorage, redacted};
use super::{FormatReader, PlannedFiles, checked_storage_location};
use crate::adapter::column_notes::DeclaredSchema;
use crate::adapter::parquet_directory::{
    FilePattern, PartitionKeepPredicate, list_location_files, list_parquet_files,
    raw_location_prefix, store_prefix,
};
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField, encode_file_path};
use crate::scan::{build_table_root_store, store_root_url};
use crate::types::mapping::arrow_type_from_tag;

#[cfg(test)]
#[path = "catalog_parquet_format_reader_tests.rs"]
mod tests;

/// The catalog is the schema authority, declared in the column notes at CREATE; files supply only
/// the listing and partition values (`glue/glue-table-planning`).
pub(super) struct CatalogParquetFormatReader<'a> {
    pub(super) files: ParquetFileSource<'a>,
}

/// Where a catalog-declared Parquet table's data files and partition values come from.
pub(super) enum ParquetFileSource<'a> {
    /// Unity Catalog: every `.parquet` object below the table directory, valued by its
    /// `key=value` path segments.
    TableDirectory(UnityTableStorage<'a>),
    /// Glue: each registered partition's location, valued by the partition's Glue values and
    /// read through the CONNECTION's static storage.
    GluePartitions {
        session: &'a GlueCatalogSession,
        storage: &'a StorageBackend,
        table: &'a CatalogTable,
    },
}

impl ParquetFileSource<'_> {
    async fn plan(
        &self,
        partition_columns: &[String],
        keep: &PartitionKeepPredicate,
    ) -> Result<PlannedFiles, UdfError> {
        match self {
            Self::TableDirectory(storage) => {
                plan_table_directory(storage, partition_columns, keep).await
            }
            Self::GluePartitions {
                session,
                storage,
                table,
            } => plan_glue_table(session, storage, table, keep).await,
        }
    }
}

async fn plan_table_directory(
    storage: &UnityTableStorage<'_>,
    partition_columns: &[String],
    keep: &PartitionKeepPredicate,
) -> Result<PlannedFiles, UdfError> {
    let (table_root, effective_storage) = storage.resolve().await?;
    let secrets = effective_storage.secret_values();
    let files = list_table_directory(
        table_root,
        &effective_storage,
        &secrets,
        partition_columns,
        keep,
    )
    .await
    .map_err(|error| redacted(error, &secrets))?;
    Ok(PlannedFiles {
        table_root: table_root.to_string(),
        effective_storage,
        files,
        name_mapping: Vec::new(),
    })
}

async fn plan_glue_table(
    session: &GlueCatalogSession,
    storage: &StorageBackend,
    table: &CatalogTable,
    keep: &PartitionKeepPredicate,
) -> Result<PlannedFiles, UdfError> {
    let location = checked_storage_location(table, "Glue")?;
    let secrets = storage.secret_values();
    let planned = async {
        let (store_root, table_prefix) = raw_location_prefix(location)?;
        let table_root = scan_table_root(&store_root, &table_prefix);
        let partitions = session.partitions(table).await?;
        let store =
            build_table_root_store(storage, &table_root, DEFAULT_S3_MAX_CONNECTIONS, &secrets)?;
        let files =
            plan_glue_partitions(&store, &store_root, &table_prefix, partitions, keep).await?;
        Ok((table_root, files))
    };
    let (table_root, files) = planned.await.map_err(|error| redacted(error, &secrets))?;
    Ok(PlannedFiles {
        table_root,
        effective_storage: storage.clone(),
        files,
        name_mapping: Vec::new(),
    })
}

/// The scan parses its table root as a URL, so the raw key's `%`, `#`, and `?` are escaped, and
/// `s3a` reads as `s3` so every file of the table addresses one store.
fn scan_table_root(store_root: &str, prefix: &StorePath) -> String {
    format!(
        "{store_root}/{}",
        encode_file_path(prefix.parts(), prefix.as_ref().len())
    )
}

/// Only direct children are listed, so a nested partition location is never read twice. Values
/// are Glue's, never parsed from the path; a file outside `table_prefix` keeps an absolute path.
async fn plan_glue_partitions(
    store: &Arc<dyn ObjectStore>,
    store_root: &str,
    table_prefix: &StorePath,
    partitions: Vec<CatalogPartition>,
    keep: &PartitionKeepPredicate,
) -> Result<Vec<FileEntry>, UdfError> {
    let kept = partitions
        .into_iter()
        .filter(|partition| keep(&partition.values))
        .map(|partition| {
            Ok((
                readable_partition_prefix(&partition, store_root)?,
                partition.values,
            ))
        })
        .collect::<Result<Vec<_>, UdfError>>()?;

    let mut files: Vec<FileEntry> = stream::iter(kept)
        .map(|(prefix, values)| async move {
            let listed = list_location_files(store, &prefix, FilePattern::AnyDirectChild).await?;
            let count = listed.len();
            Ok::<_, UdfError>(
                listed
                    .into_iter()
                    .zip(iter::repeat_n(values, count))
                    .map(|(file, values)| {
                        file_entry(file.with_partition_values(values), table_prefix, store_root)
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .buffer_unordered(DEFAULT_S3_MAX_CONNECTIONS)
        .try_concat()
        .await?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// A kept partition this reader cannot read faithfully fails the query: skipping it would return
/// wrong rows.
fn readable_partition_prefix(
    partition: &CatalogPartition,
    table_store_root: &str,
) -> Result<StorePath, UdfError> {
    let refused = |cause: String| {
        let values = partition
            .values
            .iter()
            .map(|(key, value)| format!("{key}={}", value.as_deref().unwrap_or("NULL")))
            .collect::<Vec<_>>()
            .join(", ");
        UdfError::User(format!(
            "Glue partition ({values}) at '{}' cannot be read: {cause}; skipping it would \
             return wrong rows",
            partition.location
        ))
    };
    if let PartitionFormat::Unsupported { input_format } = &partition.format {
        return Err(refused(format!(
            "its input format '{input_format}' is not Parquet"
        )));
    }
    let (store_root, prefix) = raw_location_prefix(&partition.location)?;
    if store_root != table_store_root {
        return Err(refused(format!(
            "its bucket '{store_root}' is not the table's bucket '{table_store_root}'"
        )));
    }
    Ok(prefix)
}

async fn list_table_directory(
    table_root: &str,
    storage: &StorageBackend,
    secrets: &[&str],
    partition_columns: &[String],
    keep: &PartitionKeepPredicate,
) -> Result<Vec<FileEntry>, UdfError> {
    let store = build_table_root_store(storage, table_root, DEFAULT_S3_MAX_CONNECTIONS, secrets)?;
    let prefix = store_prefix(table_root)?;
    let store_root = store_root_url(table_root)?;
    Ok(list_parquet_files(&store, &prefix, partition_columns, keep)
        .await?
        .into_iter()
        .map(|file| file_entry(file, &prefix, store_root.as_str()))
        .collect())
}

impl FormatReader for CatalogParquetFormatReader<'_> {
    /// Prunes on every partition column under its declared type; every predicate still applies
    /// above the scan.
    fn resolve_scan<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let partition_columns = &declared.partition_columns;
            let predicate = PartitionPredicate::from_filter(
                filter_json,
                &partition_column_types(&declared.logical_schema, partition_columns),
            );
            let keep = move |values: &_| predicate.keeps(values);
            self.files.plan(partition_columns, &keep).await
        })
    }
}

/// Typed from the logical schema's tags, exactly as the scan types each partition column.
fn partition_column_types(
    logical_schema: &[LogicalField],
    partition_columns: &[String],
) -> Vec<(String, ArrowType)> {
    logical_schema
        .iter()
        .filter(|field| partition_columns.contains(&field.name))
        .map(|field| (field.name.clone(), arrow_type_from_tag(&field.arrow_type)))
        .collect()
}
