//! Table-format selection: the ONE site that pairs a resolved catalog session
//! with the table it reads and answers the reader that plans that table's scan.

use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogProps, CatalogSession, CatalogTable, ConnectionCreds, StorageBackend, TableFormat,
    UnityCatalogSession,
};
use serde_json::Value as Json;

use crate::adapter::tables::catalog_identifier_string;
use crate::scan::spec::{FileEntry, LogicalField, NameMappingEntry};

mod delta_format_reader;
mod delta_replay;
mod delta_schema;
mod iceberg;

use delta_format_reader::DeltaFormatReader;
use iceberg::IcebergFormatReader;

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;

/// One table's resolved scan, in the shape every table format answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedScan {
    /// The data files active at the table's current version.
    pub files: Vec<FileEntry>,
    /// The storage the files were resolved THROUGH and must be read through: the
    /// vended backend under vending, the CONNECTION's own otherwise.
    pub effective_storage: StorageBackend,
    /// The table's logical schema, in declared column order.
    pub logical_schema: Vec<LogicalField>,
    /// The table's own storage root, carried once per fan-out.
    pub table_root: String,
    /// Physical-name-to-field-id entries for data files carrying no embedded id.
    pub name_mapping: Vec<NameMappingEntry>,
    /// The table's ordered partition-column names. Empty on every Iceberg scan,
    /// which is what keeps an Iceberg spec's encoding byte-identical to its
    /// pre-Delta form.
    pub partition_columns: Vec<String>,
}

/// Resolving ONE table into the scan the pushdown layer plans against, for one
/// table format.
///
/// Each implementation owns its WHOLE resolution — catalog request, storage
/// credential, and file discovery — because no shared caller can pre-fetch what
/// every format needs to reach its file list: the Iceberg path needs the catalog's
/// own table metadata, the Delta path only a table root and a credentialed store.
///
/// The method answers a boxed future rather than being an `async fn`, because a
/// native `async fn` in a trait is not dyn-compatible and [`format_reader`]
/// answers one `Box<dyn FormatReader>`.
pub trait FormatReader: Send + Sync {
    /// This reader's table as it stands now, pruned by `filter_json` wherever the
    /// format's own planning can apply it. `None` disables that pruning.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>>;
}

/// One table paired with the live catalog session that reads it.
///
/// Deliberately NOT the catalog kind: the kind is a parsed virtual-schema property
/// whose match sites are frozen at their own construction seam, while this carries
/// a session already resolved and, for Unity Catalog, the table metadata that
/// session already loaded. Matching this is what keeps format selection from
/// needing a second kind match site.
pub enum ScanSource<'a> {
    /// A table in an Iceberg REST catalog, named by the request's catalog
    /// properties.
    Iceberg {
        session: &'a CatalogSession,
        catalog_props: &'a CatalogProps,
    },
    /// A Delta table in a Unity Catalog, paired with the metadata that catalog
    /// loaded for it — whose format tag [`format_reader`] checks.
    UnityDelta {
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
    },
}

/// The CONNECTION's static storage decision, threaded together because its three
/// parts always travel together: the static storage backend, the resolved
/// credentials it was built from, and the resolved `ALLOW_HTTP` consent gate for
/// plaintext transport (the operator's consent gate under vending too).
#[derive(Clone, Copy)]
pub struct ConnectionStorage<'a> {
    pub storage: &'a StorageBackend,
    pub creds: &'a ConnectionCreds,
    pub allow_http: bool,
}

/// The reader that plans `source`'s scan.
///
/// The ONE site that matches a [`ScanSource`], so a third table format or a third
/// catalog kind is a compile error here rather than a silent fall-through. It
/// matches the source rather than the catalog kind, which is what leaves that
/// enum's frozen match-site baseline intact.
///
/// The Unity Catalog source's format tag is checked HERE because the single-table
/// load applies no listing filter: a non-Delta table routed into the Delta reader
/// would surface as a missing transaction log instead of a format refusal.
///
/// `connection` is the CONNECTION's static storage decision this source reads
/// through: the static storage backend, resolved credentials, and the resolved
/// `ALLOW_HTTP` consent gate.
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
    }
}
