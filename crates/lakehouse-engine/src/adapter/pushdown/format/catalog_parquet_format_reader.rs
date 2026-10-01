use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowType;
use delta_kernel::schema::{StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;
use futures::{StreamExt, TryStreamExt, stream};
use lakehouse_catalog::{
    CatalogColumn, CatalogPartition, CatalogTable, ColumnSourceType, GlueCatalogSession,
    StorageBackend,
};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde_json::Value as Json;

use super::delta_schema::classify_spark_schema;
use super::parquet_format_reader::file_entry;
use super::partition_predicate::PartitionPredicate;
use super::unity_table_storage::{UnityTableStorage, redacted};
use super::{
    FormatReader, RefusedColumn, ResolvedScan, checked_storage_location,
    ensure_table_has_a_mappable_column,
};
use crate::adapter::parquet_directory::{
    FilePattern, ParquetFile, PartitionKeepPredicate, list_location_files, list_parquet_files,
    raw_location_prefix, store_prefix,
};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField, encode_file_path};
use crate::scan::{build_table_root_store, store_root_url};
use crate::types::hive_type::parse_hive_type;
use crate::types::mapping::arrow_type_from_tag;

#[cfg(test)]
#[path = "catalog_parquet_format_reader_tests.rs"]
mod tests;

/// The catalog is the schema authority; files supply only the listing and partition values. Every
/// Spark type goes through the one Delta classifier (`vs-adapter/glue-table-planning`).
pub(super) struct CatalogParquetFormatReader<'a> {
    pub(super) table: &'a CatalogTable,
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
    },
}

/// The listed files and the storage they were listed through and must be read through.
struct PlannedFiles {
    table_root: String,
    effective_storage: StorageBackend,
    files: Vec<FileEntry>,
}

impl ParquetFileSource<'_> {
    /// Names the source in every refusal and planning error.
    fn catalog_label(&self) -> &'static str {
        match self {
            Self::TableDirectory(_) => "Unity Parquet",
            Self::GluePartitions { .. } => "Glue",
        }
    }

    async fn plan(
        &self,
        table: &CatalogTable,
        keep: &PartitionKeepPredicate,
    ) -> Result<PlannedFiles, UdfError> {
        match self {
            Self::TableDirectory(storage) => {
                let (table_root, effective_storage) = storage.resolve().await?;
                let secrets = effective_storage.secret_values();
                let files = list_table_directory(
                    table_root,
                    &effective_storage,
                    &secrets,
                    &table.partition_columns,
                    keep,
                )
                .await
                .map_err(|error| redacted(error, &secrets))?;
                Ok(PlannedFiles {
                    table_root: table_root.to_string(),
                    effective_storage,
                    files,
                })
            }
            Self::GluePartitions { session, storage } => {
                plan_glue_table(session, storage, table, keep).await
            }
        }
    }
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
        let table_root = scan_table_root(location)?;
        let partitions = session.partitions(table).await?;
        let store =
            build_table_root_store(storage, &table_root, DEFAULT_S3_MAX_CONNECTIONS, &secrets)?;
        let files = plan_glue_partitions(&store, location, partitions, keep).await?;
        Ok((table_root, files))
    };
    let (table_root, files) = planned.await.map_err(|error| redacted(error, &secrets))?;
    Ok(PlannedFiles {
        table_root,
        effective_storage: storage.clone(),
        files,
    })
}

/// The scan parses its table root as a URL, so the raw key's `%`, `#`, and `?` are escaped, and
/// `s3a` reads as `s3` so every file of the table addresses one store.
fn scan_table_root(location: &str) -> Result<String, UdfError> {
    let (store_root, prefix) = raw_location_prefix(location)?;
    Ok(format!(
        "{store_root}/{}",
        encode_file_path(prefix.parts(), location.len())
    ))
}

/// Only direct children are listed, so a nested partition location is never read twice. Values
/// are Glue's, never parsed from the path; a file outside `table_location` keeps an absolute path.
async fn plan_glue_partitions(
    store: &Arc<dyn ObjectStore>,
    table_location: &str,
    partitions: Vec<CatalogPartition>,
    keep: &PartitionKeepPredicate,
) -> Result<Vec<FileEntry>, UdfError> {
    let (store_root, table_prefix) = raw_location_prefix(table_location)?;
    let kept = partitions
        .into_iter()
        .filter(|partition| keep(&partition.values))
        .map(|partition| {
            Ok((
                readable_partition_prefix(&partition, &store_root)?,
                partition.values,
            ))
        })
        .collect::<Result<Vec<_>, UdfError>>()?;

    let (store_root, table_prefix) = (store_root.as_str(), &table_prefix);
    let listed: Vec<Vec<FileEntry>> = stream::iter(kept)
        .map(|(prefix, values)| async move {
            let files = list_location_files(store, &prefix, FilePattern::AnyDirectChild).await?;
            Ok::<_, UdfError>(
                files
                    .into_iter()
                    .map(|file| {
                        let file = ParquetFile {
                            path: file.path,
                            size: file.size,
                            partition_values: values.clone(),
                            footer: None,
                        };
                        file_entry(file, table_prefix, store_root)
                    })
                    .collect(),
            )
        })
        .buffer_unordered(DEFAULT_S3_MAX_CONNECTIONS)
        .try_collect()
        .await?;

    let mut files: Vec<FileEntry> = listed.into_iter().flatten().collect();
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
    if !partition.is_parquet() {
        return Err(refused(format!(
            "its input format '{}' is not Parquet",
            partition.input_format
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
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let CatalogSchema {
                logical_schema,
                partition_columns,
                refused_columns,
            } = catalog_schema(self.table, self.files.catalog_label())?;
            let predicate = PartitionPredicate::from_filter(
                filter_json,
                &partition_column_types(&logical_schema, &partition_columns),
            );

            let PlannedFiles {
                table_root,
                effective_storage,
                files,
            } = self
                .files
                .plan(self.table, &move |values| predicate.keeps(values))
                .await?;

            Ok(ResolvedScan {
                files,
                effective_storage,
                logical_schema,
                table_root,
                name_mapping: Vec::new(),
                partition_columns,
                refused_columns,
            })
        })
    }
}

struct CatalogSchema {
    logical_schema: Vec<LogicalField>,
    partition_columns: Vec<String>,
    refused_columns: Vec<RefusedColumn>,
}

fn catalog_schema(table: &CatalogTable, label: &str) -> Result<CatalogSchema, UdfError> {
    let mut fields = Vec::with_capacity(table.columns.len());
    let mut refused_columns = Vec::new();
    for column in &table.columns {
        match spark_field(column) {
            Ok(field) => fields.push(field),
            Err(reason) => refused_columns.push(RefusedColumn {
                column_name: column.name.clone(),
                reason: format!("{label} column '{}': {reason}", column.name),
            }),
        }
    }
    let schema = StructType::try_new(fields).map_err(|error| {
        UdfError::User(format!(
            "the {label} columns of table {} do not form one Spark schema: {error}",
            catalog_identifier_string(&table.ident)
        ))
    })?;
    let (logical_schema, partition_columns, classified_refusals) = classify_spark_schema(
        &schema,
        ColumnMappingMode::None,
        table.partition_columns.clone(),
        label,
    )?;
    refused_columns.extend(classified_refusals);
    refused_columns.sort_by_key(|refused| {
        table
            .columns
            .iter()
            .position(|column| column.name == refused.column_name)
    });

    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns, label)?;
    ensure_no_partition_column_is_refused(table, &refused_columns, label)?;
    Ok(CatalogSchema {
        logical_schema,
        partition_columns,
        refused_columns,
    })
}

/// Forced nullable: a file missing the column reads NULL, which a required declaration would fail.
fn spark_field(column: &CatalogColumn) -> Result<StructField, String> {
    match &column.source_type {
        ColumnSourceType::Unity {
            type_json: Some(type_json),
            ..
        } => {
            let field: StructField = serde_json::from_str(type_json).map_err(|error| {
                format!("its type_json descriptor does not parse as a Spark StructField ({error})")
            })?;
            Ok(StructField {
                name: column.name.clone(),
                nullable: true,
                ..field
            })
        }
        ColumnSourceType::Unity {
            type_json: None, ..
        } => Err(
            "Unity Catalog reports no type_json descriptor for it, so its Spark type is unknown"
                .to_string(),
        ),
        ColumnSourceType::Glue { hive_type } => parse_hive_type(hive_type)
            .map(|data_type| StructField::nullable(column.name.clone(), data_type)),
        ColumnSourceType::Iceberg(_) | ColumnSourceType::Parquet(_) => {
            Err("its catalog declares no Spark or Hive type for it".to_string())
        }
    }
}

fn ensure_no_partition_column_is_refused(
    table: &CatalogTable,
    refused_columns: &[RefusedColumn],
    label: &str,
) -> Result<(), UdfError> {
    let Some(refused) = refused_columns
        .iter()
        .find(|column| table.partition_columns.contains(&column.column_name))
    else {
        return Ok(());
    };
    Err(UdfError::User(format!(
        "{label} table {} declares '{}' as a partition column, but that column is refused \
         ({}); the scan cannot materialize a partition column absent from the logical schema",
        catalog_identifier_string(&table.ident),
        refused.column_name,
        refused.reason
    )))
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
