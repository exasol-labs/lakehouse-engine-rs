//! Table-format selection: the one site pairing a resolved catalog session with its table
//! and answering the reader that plans the scan.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, CatalogTable, ColumnSourceType, ConnectionCreds,
    GlueCatalogSession, StorageBackend, TableFormat, UnityCatalogSession,
};
use object_store::ObjectStore;
use serde_json::Value as Json;

use crate::adapter::column_notes::{ColumnDeclaration, DeclaredSchema};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry};

pub use crate::adapter::column_notes::RefusedColumn;

mod catalog_declaration;
mod catalog_parquet_format_reader;
mod delta_format_reader;
mod delta_predicate;
mod delta_protocol;
mod delta_replay;
mod delta_schema;
mod filter_json;
mod iceberg;
mod parquet_format_reader;
mod partition_predicate;
mod unity_table_storage;

use catalog_parquet_format_reader::{CatalogParquetFormatReader, ParquetFileSource};
use delta_format_reader::DeltaFormatReader;
use iceberg::{IcebergFormatReader, IcebergMetadataSource};
use parquet_format_reader::ParquetFormatReader;
use unity_table_storage::UnityTableStorage;

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
    /// Plans `reader`'s files against `declared`, the table's parsed column notes.
    pub async fn resolve(
        reader: &dyn FormatReader,
        declared: DeclaredSchema,
        filter_json: Option<&Json>,
    ) -> Result<Self, UdfError> {
        let planned = reader.resolve_scan(&declared, filter_json).await?;
        Ok(Self::new(declared, planned))
    }

    /// The schema half comes from the column notes alone, so no reader can substitute its own.
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

/// What a format reader resolves per query.
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

/// Each implementation owns its whole file resolution (catalog request, credential, file
/// discovery) because formats need different inputs to reach their file list; the schema is
/// the declaration every reader receives. Returns a boxed future because `async fn` in a trait
/// is not dyn-compatible.
pub trait FormatReader: Send + Sync {
    /// `None` disables format-level pruning.
    fn resolve_scan<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>>;
}

/// Deliberately not the catalog kind: it carries an already-resolved session (and, for
/// Unity and Glue, the loaded table), so format selection needs no second kind match site.
pub enum ScanSource<'a> {
    Iceberg {
        session: &'a CatalogSession,
        catalog_props: &'a CatalogProps,
    },
    Unity {
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
    },
    Glue {
        session: &'a GlueCatalogSession,
        table: &'a CatalogTable,
    },
    DirectParquet {
        store: &'a Arc<dyn ObjectStore>,
        table_root: &'a str,
    },
}

/// Under vending, `allow_http` is still the operator's consent gate for plaintext transport.
#[derive(Clone, Copy)]
pub struct ConnectionStorage<'a> {
    pub storage: &'a StorageBackend,
    pub creds: &'a ConnectionCreds,
    pub allow_http: bool,
}

/// The one site matching a [`ScanSource`], so a new format or catalog kind is a compile
/// error here. A catalog's format tag is checked here because the single-table load applies
/// no listing filter.
pub fn format_reader<'a>(
    source: ScanSource<'a>,
    connection: &ConnectionStorage<'a>,
) -> Result<Box<dyn FormatReader + 'a>, UdfError> {
    match source {
        ScanSource::Iceberg {
            session,
            catalog_props,
        } => Ok(Box::new(IcebergFormatReader {
            metadata: IcebergMetadataSource::RestLoadTable {
                session,
                catalog_props,
            },
            connection: *connection,
        })),
        ScanSource::Unity { session, table } => match table.format {
            TableFormat::Delta => Ok(Box::new(DeltaFormatReader {
                storage: UnityTableStorage::new(session, table, connection),
            })),
            TableFormat::Parquet => Ok(Box::new(CatalogParquetFormatReader {
                files: ParquetFileSource::TableDirectory(UnityTableStorage::new(
                    session, table, connection,
                )),
            })),
            TableFormat::Iceberg => Err(UdfError::User(format!(
                "Unity Catalog table {} reports the {:?} table format, which no Unity Catalog \
                 reader plans",
                catalog_identifier_string(&table.ident),
                table.format
            ))),
        },
        ScanSource::Glue { session, table } => match table.format {
            TableFormat::Iceberg => Ok(Box::new(IcebergFormatReader {
                metadata: IcebergMetadataSource::MetadataFile { table },
                connection: *connection,
            })),
            TableFormat::Parquet => Ok(Box::new(CatalogParquetFormatReader {
                files: ParquetFileSource::GluePartitions {
                    session,
                    storage: connection.storage,
                    table,
                },
            })),
            TableFormat::Delta => Err(UdfError::User(format!(
                "Glue table {} reports the {:?} table format, which no Glue reader plans",
                catalog_identifier_string(&table.ident),
                table.format
            ))),
        },
        ScanSource::DirectParquet { store, table_root } => Ok(Box::new(ParquetFormatReader {
            store,
            table_root,
            storage: connection.storage,
        })),
    }
}
