//! Table-format selection: the one site pairing a resolved catalog session with its table
//! and answering the reader that plans the scan.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, CatalogTable, ConnectionCreds, StorageBackend, TableFormat,
    UnityCatalogSession,
};
use object_store::ObjectStore;
use serde_json::Value as Json;

use crate::adapter::parquet_directory::DirectoryOptions;
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry};

mod delta_format_reader;
mod delta_predicate;
mod delta_protocol;
mod delta_replay;
mod delta_schema;
mod filter_json;
mod iceberg;
mod parquet_format_reader;
mod partition_predicate;
mod unity_parquet_format_reader;
mod unity_table_storage;

use delta_format_reader::DeltaFormatReader;
use iceberg::IcebergFormatReader;
#[cfg(test)]
pub(crate) use iceberg::build_logical_schema;
use parquet_format_reader::ParquetFormatReader;
use unity_parquet_format_reader::UnityParquetFormatReader;

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
/// Unity, the loaded table), so format selection needs no second kind match site.
pub enum ScanSource<'a> {
    Iceberg {
        session: &'a CatalogSession,
        catalog_props: &'a CatalogProps,
    },
    Unity {
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
    },
    /// `declared_columns` is the request's `(Exasol name, Exasol type)` declaration, so a column
    /// no kept file carries still resolves.
    DirectParquet {
        store: &'a Arc<dyn ObjectStore>,
        table_root: &'a str,
        options: DirectoryOptions,
        declared_columns: &'a [(String, String)],
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
/// error here. The Unity format tag is checked here because the single-table load applies
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
            session,
            catalog_props,
            connection: *connection,
        })),
        ScanSource::Unity { session, table } => match table.format {
            TableFormat::Delta => Ok(Box::new(DeltaFormatReader::new(session, table, connection))),
            TableFormat::Parquet => Ok(Box::new(UnityParquetFormatReader::new(
                session, table, connection,
            ))),
            TableFormat::Iceberg => Err(UdfError::User(format!(
                "Unity Catalog table {} reports the {:?} table format, which no Unity Catalog \
                 reader plans",
                catalog_identifier_string(&table.ident),
                table.format
            ))),
        },
        ScanSource::DirectParquet {
            store,
            table_root,
            options,
            declared_columns,
        } => Ok(Box::new(ParquetFormatReader {
            store,
            table_root,
            options,
            declared_columns,
            storage: connection.storage,
        })),
    }
}
