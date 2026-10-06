//! Table-format selection: the one site pairing a resolved catalog session with its table
//! and answering the reader that plans the scan.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, CatalogTable, ConnectionCreds, GlueCatalogSession,
    StorageBackend, TableFormat, UnityCatalogSession,
};
use object_store::ObjectStore;
use serde_json::Value as Json;

use crate::adapter::parquet_directory::DirectoryOptions;
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry};

mod catalog_parquet_format_reader;
mod delta_format_reader;
mod delta_predicate;
mod delta_protocol;
mod delta_replay;
mod delta_schema;
mod filter_json;
mod footer_statistics;
mod iceberg;
mod parquet_format_reader;
mod partition_predicate;
mod unity_table_storage;

use catalog_parquet_format_reader::{CatalogParquetFormatReader, ParquetFileSource};
use delta_format_reader::DeltaFormatReader;
#[cfg(test)]
pub(crate) use iceberg::build_logical_schema;
use iceberg::{IcebergFormatReader, IcebergMetadataSource};
use parquet_format_reader::ParquetFormatReader;
use unity_table_storage::UnityTableStorage;

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;

/// Never a `ScanSpec` field: only the pre-scan pushdown-resolution gate reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedColumn {
    pub column_name: String,
    pub reason: String,
}

/// `raw_scan` cannot scan an empty schema.
fn ensure_table_has_a_mappable_column(
    logical_schema: &[LogicalField],
    refused_columns: &[RefusedColumn],
    table_kind: &str,
) -> Result<(), UdfError> {
    if !logical_schema.is_empty() || refused_columns.is_empty() {
        return Ok(());
    }

    let reasons = refused_columns
        .iter()
        .map(|column| format!("'{}': {}", column.column_name, column.reason))
        .collect::<Vec<_>>()
        .join("; ");
    Err(UdfError::User(format!(
        "{table_kind} table has no mappable column; every column is refused: {reasons}"
    )))
}

/// Shared by every source, so each binary refusal reads alike
/// (`vs-adapter/binary-column-refusal`).
fn binary_cause(declared: &str) -> String {
    format!(
        "has type '{declared}', which this engine refuses: rendering binary data is tracked as \
         issue #351"
    )
}

/// `member_path` is `None` when the column's own type is the binary one.
pub(super) fn binary_refusal(
    label: &str,
    column: &str,
    member_path: Option<&str>,
    declared: &str,
) -> RefusedColumn {
    let subject = match member_path {
        Some(member_path) => format!("{label} column '{column}', whose member '{member_path}'"),
        None => format!("{label} column '{column}'"),
    };
    RefusedColumn {
        column_name: column.to_string(),
        reason: format!("{subject} {}", binary_cause(declared)),
    }
}

/// The listing still declares a refused column, so only a request reading one fails.
fn without_refused_columns(
    mut logical_schema: Vec<LogicalField>,
    refused_columns: &[RefusedColumn],
    table_kind: &str,
) -> Result<Vec<LogicalField>, UdfError> {
    logical_schema.retain(|field| {
        refused_columns
            .iter()
            .all(|refused| refused.column_name != field.name)
    });
    ensure_table_has_a_mappable_column(&logical_schema, refused_columns, table_kind)?;
    Ok(logical_schema)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedScan {
    pub files: Vec<FileEntry>,
    /// The storage the files were resolved through and must be read through (vended under
    /// vending, the CONNECTION's own otherwise).
    pub effective_storage: StorageBackend,
    pub logical_schema: Vec<LogicalField>,
    pub table_root: String,
    /// Physical-name-to-field-id entries for data files carrying no embedded id.
    pub name_mapping: Vec<NameMappingEntry>,
    /// Always empty for Iceberg.
    pub partition_columns: Vec<String>,
    pub refused_columns: Vec<RefusedColumn>,
}

/// Each implementation owns its whole resolution (catalog request, credential, file
/// discovery) because formats need different inputs to reach their file list. Returns a
/// boxed future because `async fn` in a trait is not dyn-compatible.
pub trait FormatReader: Send + Sync {
    /// `None` disables format-level pruning.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>>;
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
    /// `declared_columns` is the request's `(Exasol name, Exasol type)` declaration, so a column
    /// no kept file carries still resolves. `statistics_filter` is the part of the filter the
    /// scan evaluates, the only part footer statistics may prune on.
    DirectParquet {
        store: &'a Arc<dyn ObjectStore>,
        table_root: &'a str,
        options: DirectoryOptions,
        declared_columns: &'a [(String, String)],
        statistics_filter: Option<&'a Json>,
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
            TableFormat::Delta => Ok(Box::new(DeltaFormatReader::new(session, table, connection))),
            TableFormat::Parquet => Ok(Box::new(CatalogParquetFormatReader {
                table,
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
                table,
                files: ParquetFileSource::GluePartitions {
                    session,
                    storage: connection.storage,
                },
            })),
            TableFormat::Delta => Err(UdfError::User(format!(
                "Glue table {} reports the {:?} table format, which no Glue reader plans",
                catalog_identifier_string(&table.ident),
                table.format
            ))),
        },
        ScanSource::DirectParquet {
            store,
            table_root,
            options,
            declared_columns,
            statistics_filter,
        } => Ok(Box::new(ParquetFormatReader {
            store,
            table_root,
            options,
            declared_columns,
            statistics_filter,
            storage: connection.storage,
        })),
    }
}
