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

use delta_format_reader::DeltaFormatReader;
use iceberg::IcebergFormatReader;
#[cfg(test)]
pub(crate) use iceberg::build_logical_schema;
use parquet_format_reader::ParquetFormatReader;

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;

/// Never a `ScanSpec` field: only the pre-scan pushdown-resolution gate reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedColumn {
    pub column_name: String,
    pub reason: String,
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
    UnityDelta {
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
/// no listing filter: a non-Delta table would otherwise surface as a missing transaction log.
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
        ScanSource::UnityDelta { session, table } => {
            if table.format != TableFormat::Delta {
                return Err(UdfError::User(format!(
                    "Unity Catalog table {} reports the {:?} table format, which the Delta \
                     reader this source selects cannot plan",
                    catalog_identifier_string(&table.ident),
                    table.format
                )));
            }
            Ok(Box::new(DeltaFormatReader::new(session, table, connection)))
        }
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
