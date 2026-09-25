use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogColumn, CatalogTable, ColumnSourceType, StorageBackend, UnityCatalogSession,
};
use serde_json::Value as Json;

use super::delta_format_reader::ensure_table_has_a_mappable_column;
use super::delta_schema::build_delta_table_schema;
use super::parquet_format_reader::file_entry;
use super::partition_predicate::PartitionPredicate;
use super::unity_table_storage::{UnityTableStorage, redacted};
use super::{ConnectionStorage, FormatReader, RefusedColumn, ResolvedScan};
use crate::adapter::parquet_directory::{list_parquet_files, store_prefix};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField};
use crate::scan::{build_table_root_store, store_root_url};

#[cfg(test)]
#[path = "unity_parquet_format_reader_tests.rs"]
mod tests;

/// The Unity Catalog Parquet table reader: the catalog is the schema authority, and the files
/// supply only the listing and the partition values. Each column's `type_json` is the Spark
/// `StructField` JSON the Delta log also records, so the Delta reader's classifier types it and
/// no third Spark-type mapping can drift from the other two. No footer is read at plan time.
pub(super) struct UnityParquetFormatReader<'a> {
    storage: UnityTableStorage<'a>,
    table: &'a CatalogTable,
}

impl<'a> UnityParquetFormatReader<'a> {
    /// `connection` is the CONNECTION's static storage decision, see
    /// [`UnityTableStorage::new`].
    pub(super) fn new(
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
        connection: &ConnectionStorage<'a>,
    ) -> Self {
        Self {
            storage: UnityTableStorage::new(session, table, connection),
            table,
        }
    }

    async fn plan(
        &self,
        table_root: &str,
        storage: &StorageBackend,
        filter_json: Option<&Json>,
    ) -> Result<(Vec<FileEntry>, CatalogSchema), UdfError> {
        let secrets = storage.secret_values();
        let schema = catalog_schema(self.table)?;
        let keep = string_partition_keep(filter_json, schema.string_partition_columns.clone());

        let store =
            build_table_root_store(storage, table_root, DEFAULT_S3_MAX_CONNECTIONS, &secrets)?;
        let prefix = store_prefix(table_root)?;
        let store_root = store_root_url(table_root)?;
        let files = list_parquet_files(&store, &prefix, &schema.partition_columns, &keep)
            .await?
            .into_iter()
            .map(|file| file_entry(file, &prefix, store_root.as_str()))
            .collect();
        Ok((files, schema))
    }
}

impl FormatReader for UnityParquetFormatReader<'_> {
    /// Prunes files only by predicates on `string` partition columns: the partition predicate
    /// compares values as strings, and string order is not the order of an integer, date, or
    /// timestamp column. Every other predicate still applies above the scan.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let (table_root, effective_storage) = self.storage.resolve().await?;
            let secrets = effective_storage.secret_values();

            let (files, schema) = self
                .plan(table_root, &effective_storage, filter_json)
                .await
                .map_err(|error| redacted(error, &secrets))?;

            Ok(ResolvedScan {
                files,
                effective_storage,
                logical_schema: schema.logical_schema,
                table_root: table_root.to_string(),
                name_mapping: Vec::new(),
                partition_columns: schema.partition_columns,
                refused_columns: schema.refused_columns,
            })
        })
    }
}

/// The catalog-declared schema a Unity Parquet scan binds against.
struct CatalogSchema {
    logical_schema: Vec<LogicalField>,
    partition_columns: Vec<String>,
    refused_columns: Vec<RefusedColumn>,
    string_partition_columns: HashSet<String>,
}

fn catalog_schema(table: &CatalogTable) -> Result<CatalogSchema, UdfError> {
    let mut fields = Vec::with_capacity(table.columns.len());
    let mut refused_columns = Vec::new();
    for column in &table.columns {
        match spark_field(column) {
            Ok(field) => fields.push(field),
            Err(reason) => refused_columns.push(RefusedColumn {
                column_name: column.name.clone(),
                reason,
            }),
        }
    }
    let string_partition_columns = fields
        .iter()
        .filter(|field| field.data_type == DataType::STRING)
        .filter(|field| table.partition_columns.contains(&field.name))
        .map(|field| field.name.clone())
        .collect();

    let schema = StructType::try_new(fields).map_err(|error| {
        UdfError::User(format!(
            "the Unity Catalog columns of table {} do not form one Spark schema: {error}",
            catalog_identifier_string(&table.ident)
        ))
    })?;
    let (logical_schema, partition_columns, classified_refusals) = build_delta_table_schema(
        &schema,
        ColumnMappingMode::None,
        table.partition_columns.clone(),
    )?;
    refused_columns.extend(classified_refusals);
    refused_columns.sort_by_key(|refused| {
        table
            .columns
            .iter()
            .position(|column| column.name == refused.column_name)
    });

    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns, "Unity Parquet")?;
    ensure_no_partition_column_is_refused(table, &refused_columns)?;
    Ok(CatalogSchema {
        logical_schema,
        partition_columns,
        refused_columns,
        string_partition_columns,
    })
}

/// Forced nullable: a file missing the column reads NULL, which a required declaration would fail.
fn spark_field(column: &CatalogColumn) -> Result<StructField, String> {
    let ColumnSourceType::Unity {
        type_json: Some(type_json),
        ..
    } = &column.source_type
    else {
        return Err(
            "Unity Catalog reports no type_json descriptor for it, so its Spark type is unknown"
                .to_string(),
        );
    };
    let field: StructField = serde_json::from_str(type_json).map_err(|error| {
        format!("its type_json descriptor does not parse as a Spark StructField ({error})")
    })?;
    Ok(StructField {
        name: column.name.clone(),
        nullable: true,
        ..field
    })
}

fn ensure_no_partition_column_is_refused(
    table: &CatalogTable,
    refused_columns: &[RefusedColumn],
) -> Result<(), UdfError> {
    let Some(refused) = refused_columns
        .iter()
        .find(|column| table.partition_columns.contains(&column.column_name))
    else {
        return Ok(());
    };
    Err(UdfError::User(format!(
        "Unity Parquet table {} declares '{}' as a partition column, but that column is refused \
         ({}); the scan cannot materialize a partition column absent from the logical schema",
        catalog_identifier_string(&table.ident),
        refused.column_name,
        refused.reason
    )))
}

fn string_partition_keep(
    filter_json: Option<&Json>,
    string_columns: HashSet<String>,
) -> impl Fn(&BTreeMap<String, Option<String>>) -> bool + Send + Sync + 'static {
    let predicate = PartitionPredicate::from_filter(filter_json);
    move |values| {
        let compared: BTreeMap<String, Option<String>> = values
            .iter()
            .filter(|(name, _)| string_columns.contains(*name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        predicate.keeps(&compared)
    }
}
