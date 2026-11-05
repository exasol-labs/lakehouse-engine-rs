//! Table formats: each format's loaded table, which every reference to the table prunes with its
//! own filter and declaration, and the checks the formats share.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogTable, ColumnSourceType, ConnectionCreds, StorageBackend, redact_error_text,
};
use serde_json::Value as Json;

use crate::adapter::column_notes::{ColumnDeclaration, DeclaredSchema};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry};
use crate::types::mapping::arrow_type_from_tag;

pub use crate::adapter::column_notes::RefusedColumn;

mod catalog_declaration;
mod catalog_parquet_table;
mod delta_predicate;
mod delta_protocol;
mod delta_replay;
mod delta_schema;
mod delta_table;
pub(super) mod filter_json;
mod iceberg;
mod parquet_directory_table;
mod partition_predicate;
mod unity_table_storage;

pub(super) use catalog_parquet_table::CatalogParquetTable;
pub(super) use delta_table::DeltaTable;
pub(super) use iceberg::IcebergTable;
pub(super) use parquet_directory_table::ParquetDirectoryTable;
pub(super) use unity_table_storage::UnityTableStorage;

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;

/// The one CREATE-time declaration dispatch: each column's note, `None` for a column no source
/// declares. An engine-side client's slot already holds the encoded note. Never fails: a column
/// whose metadata cannot be declared is recorded with its refusal.
pub(crate) fn declare_columns(table: &CatalogTable) -> Vec<Option<String>> {
    let catalog_declared = catalog_declaration::declare(table);
    table
        .columns
        .iter()
        .enumerate()
        .map(
            |(index, column)| match (&column.declaration, &column.source_type) {
                (Some(note), _) => Some(note.clone()),
                (None, ColumnSourceType::Iceberg(field)) => {
                    Some(iceberg::declaration(field).encode())
                }
                (None, ColumnSourceType::Unity { .. } | ColumnSourceType::Glue { .. }) => {
                    catalog_declared.get(index).map(ColumnDeclaration::encode)
                }
                (None, ColumnSourceType::Parquet(_)) => None,
            },
        )
        .collect()
}

/// Checked before any credential or storage access; neither the catalog URI nor the
/// CONNECTION endpoint may substitute for an empty location.
fn checked_storage_location<'t>(
    table: &'t CatalogTable,
    catalog: &str,
) -> Result<&'t str, UdfError> {
    match table.storage_location.as_deref() {
        Some(location) if !location.trim().is_empty() => Ok(location),
        _ => Err(UdfError::User(format!(
            "the {catalog} metadata for table {} carries an EMPTY storage location; the catalog \
             URI and the CONNECTION endpoint name no table location and are not valid substitutes",
            catalog_identifier_string(&table.ident)
        ))),
    }
}

/// Collapses to [`UdfError::User`] via `Display`, so the variant prefix survives while no
/// future variant payload can carry a secret unmasked.
pub(super) fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}

/// A Delta or Hive partition value is a string, so a binary column's value travels as the lowercase
/// hex of its UTF-8 bytes, the form the Iceberg reader writes and `scan::partition_scalar` decodes.
fn hex_binary_partition_values(files: &mut [FileEntry], logical_schema: &[LogicalField]) {
    let binary: Vec<&str> = logical_schema
        .iter()
        .filter(|field| arrow_type_from_tag(&field.arrow_type).is_binary())
        .map(|field| field.name.as_str())
        .collect();
    if binary.is_empty() {
        return;
    }
    for file in files {
        for (column, value) in &mut file.partition_values {
            if let Some(value) = value.as_mut().filter(|_| binary.contains(&column.as_str())) {
                *value = hex::encode(value.as_bytes());
            }
        }
    }
}

/// A table's planned files with its declared schema: the scan a pushdown renders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedScan {
    pub files: Vec<FileEntry>,
    pub effective_storage: StorageBackend,
    pub logical_schema: Vec<LogicalField>,
    pub table_root: String,
    pub name_mapping: Vec<NameMappingEntry>,
    pub partition_columns: Vec<String>,
    pub refused_columns: Vec<RefusedColumn>,
}

impl ResolvedScan {
    /// Prunes `table`'s files against `declared`, the reference's parsed column notes.
    pub async fn resolve(
        table: &dyn LoadedTable,
        declared: DeclaredSchema,
        filter_json: Option<&Json>,
    ) -> Result<Self, UdfError> {
        let planned = table.prune(&declared, filter_json).await?;
        Ok(Self::new(declared, planned))
    }

    /// The schema half comes from the column notes alone, so no table can substitute its own.
    fn new(declared: DeclaredSchema, planned: PlannedFiles) -> Self {
        Self {
            files: planned.files,
            effective_storage: planned.effective_storage,
            logical_schema: declared.logical_schema,
            table_root: planned.table_root,
            name_mapping: planned.name_mapping,
            partition_columns: declared.partition_columns,
            refused_columns: declared.refused_columns,
        }
    }
}

/// Boxed, because `async fn` in a trait is not dyn-compatible.
pub(super) type UdfFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, UdfError>> + Send + 'a>>;

/// What one reference's prune resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFiles {
    pub files: Vec<FileEntry>,
    /// The storage the files were resolved through and must be read through (vended under
    /// vending, the CONNECTION's own otherwise).
    pub effective_storage: StorageBackend,
    pub table_root: String,
    /// Physical-name-to-field-id entries for data files carrying no embedded id.
    pub name_mapping: Vec<NameMappingEntry>,
}

/// A table's filter-independent state (catalog answer, credential, metadata, log, or listing),
/// opened once so that every reference to the table prunes the same state with its own filter
/// and declaration. Owned, so it outlives the session that opened it.
pub trait LoadedTable: Send + Sync {
    /// `None` disables format-level pruning. Fails a declaration that disagrees with the table.
    fn prune<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> UdfFuture<'a, PlannedFiles>;
}

/// Under vending, `allow_http` is still the operator's consent gate for plaintext transport.
#[derive(Clone, Copy)]
pub struct ConnectionStorage<'a> {
    pub storage: &'a StorageBackend,
    pub creds: &'a ConnectionCreds,
    pub allow_http: bool,
}
