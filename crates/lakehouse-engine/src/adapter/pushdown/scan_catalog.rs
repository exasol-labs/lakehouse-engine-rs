//! The pushdown face of each catalog kind's client: the identifier rule, the permission check,
//! and the one scan load that opens a table for its format.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogClient, CatalogProps, CatalogTable, CatalogTableIdent, ConnectionCreds,
    GlueCatalogSession, IcebergRestCatalogClient, StorageBackend, TableFormat, UnityCatalogSession,
    parse_glue_table_ident, parse_table_ident,
};

use super::format::{
    CatalogParquetTable, DeltaTable, IcebergTable, ParquetDirectoryTable, UnityTableStorage,
};
use super::{ConnectionStorage, LoadedTable};
use crate::adapter::direct_storage::DirectStorageCatalogClient;
use crate::adapter::permission::PermissionGate;
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::ObjectLocation;

#[cfg(test)]
#[path = "scan_catalog_tests.rs"]
mod tests;

type CatalogFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, UdfError>> + Send + 'a>>;

/// A catalog client as pushdown uses it. Each kind's client loads a table on its own concrete
/// session and then matches only the format tag of the loaded table, so the catalog kind is
/// matched once, where the client is built.
pub(crate) trait ScanCatalog: CatalogClient {
    /// Applies the kind's identifier rule without I/O, so a malformed identifier is refused
    /// before any session or store is contacted.
    fn check_identifier(&self, table_identifier: &str) -> Result<(), UdfError>;

    /// Batch-checks `identifiers` under `gate` and returns those it admits. A kind that runs no
    /// check admits none, so a gated request under it reads no table.
    fn authorize<'a>(
        &'a self,
        _gate: &'a PermissionGate,
        _identifiers: &'a [&'a str],
        _creds: &'a ConnectionCreds,
    ) -> CatalogFuture<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Opens the table's filter-independent state, which every reference to it then prunes.
    fn load_for_scan<'a>(
        &'a self,
        table_identifier: &'a str,
        connection: ConnectionStorage<'a>,
    ) -> CatalogFuture<'a, Arc<dyn LoadedTable>>;
}

impl ScanCatalog for IcebergRestCatalogClient {
    fn check_identifier(&self, table_identifier: &str) -> Result<(), UdfError> {
        parse_table_ident(table_identifier).map(drop)
    }

    fn authorize<'a>(
        &'a self,
        gate: &'a PermissionGate,
        identifiers: &'a [&'a str],
        creds: &'a ConnectionCreds,
    ) -> CatalogFuture<'a, Vec<String>> {
        Box::pin(async move {
            gate.authorize(self.session().await?, identifiers, creds)
                .await?;
            Ok(identifiers.iter().map(ToString::to_string).collect())
        })
    }

    fn load_for_scan<'a>(
        &'a self,
        table_identifier: &'a str,
        connection: ConnectionStorage<'a>,
    ) -> CatalogFuture<'a, Arc<dyn LoadedTable>> {
        Box::pin(async move {
            let catalog_props = CatalogProps {
                warehouse: connection.creds.warehouse.clone(),
                table: table_identifier.to_string(),
            };
            let table =
                IcebergTable::from_rest(self.session().await?, &catalog_props, &connection).await?;
            Ok(Arc::new(table) as Arc<dyn LoadedTable>)
        })
    }
}

impl ScanCatalog for UnityCatalogSession {
    fn check_identifier(&self, table_identifier: &str) -> Result<(), UdfError> {
        unity_table_ident(table_identifier).map(drop)
    }

    fn load_for_scan<'a>(
        &'a self,
        table_identifier: &'a str,
        connection: ConnectionStorage<'a>,
    ) -> CatalogFuture<'a, Arc<dyn LoadedTable>> {
        Box::pin(async move {
            let table = self
                .load_table(&unity_table_ident(table_identifier)?)
                .await?;
            open_unity_table(self, &table, &connection).await
        })
    }
}

impl ScanCatalog for GlueCatalogSession {
    fn check_identifier(&self, table_identifier: &str) -> Result<(), UdfError> {
        parse_glue_table_ident(table_identifier).map(drop)
    }

    fn load_for_scan<'a>(
        &'a self,
        table_identifier: &'a str,
        connection: ConnectionStorage<'a>,
    ) -> CatalogFuture<'a, Arc<dyn LoadedTable>> {
        Box::pin(async move {
            let table = self
                .load_table(&parse_glue_table_ident(table_identifier)?)
                .await?;
            open_glue_table(self, &table, connection.storage).await
        })
    }
}

impl ScanCatalog for DirectStorageCatalogClient {
    fn check_identifier(&self, table_identifier: &str) -> Result<(), UdfError> {
        direct_storage_directory(table_identifier).map(drop)
    }

    fn load_for_scan<'a>(
        &'a self,
        table_identifier: &'a str,
        connection: ConnectionStorage<'a>,
    ) -> CatalogFuture<'a, Arc<dyn LoadedTable>> {
        Box::pin(async move {
            let table_root = direct_storage_table_root(self.root(), table_identifier)?;
            let table =
                ParquetDirectoryTable::open(self.store(), &table_root, connection.storage).await?;
            Ok(Arc::new(table) as Arc<dyn LoadedTable>)
        })
    }
}

/// The single-table load applies no listing filter, so the format its answer reports is
/// checked here, before any storage request.
async fn open_unity_table(
    session: &UnityCatalogSession,
    table: &CatalogTable,
    connection: &ConnectionStorage<'_>,
) -> Result<Arc<dyn LoadedTable>, UdfError> {
    let storage = UnityTableStorage::new(session, table, connection);
    Ok(match table.format {
        TableFormat::Delta => Arc::new(DeltaTable::open(storage).await?),
        TableFormat::Parquet => Arc::new(CatalogParquetTable::from_unity(storage).await?),
        TableFormat::Iceberg => return Err(unplanned_format("Unity Catalog", table)),
    })
}

async fn open_glue_table(
    session: &GlueCatalogSession,
    table: &CatalogTable,
    storage: &StorageBackend,
) -> Result<Arc<dyn LoadedTable>, UdfError> {
    Ok(match table.format {
        TableFormat::Iceberg => Arc::new(IcebergTable::from_metadata_file(table, storage).await?),
        TableFormat::Parquet => {
            Arc::new(CatalogParquetTable::from_glue(session, table, storage).await?)
        }
        // The Glue client tags no table Delta; the arm keeps the match exhaustive.
        TableFormat::Delta => return Err(unplanned_format("Glue", table)),
    })
}

/// A table planned as another format surfaces a missing log or a wrong schema, so it is refused
/// with the format its catalog reported.
fn unplanned_format(catalog: &str, table: &CatalogTable) -> UdfError {
    UdfError::User(format!(
        "{catalog} table {} reports the {:?} table format, which no {catalog} reader plans",
        catalog_identifier_string(&table.ident),
        table.format
    ))
}

/// `root` is the CONNECTION address already joined with `NAMESPACE`, as enumeration composes it.
fn direct_storage_table_root(
    root: &ObjectLocation,
    table_identifier: &str,
) -> Result<ObjectLocation, UdfError> {
    root.child(direct_storage_directory(table_identifier)?)
}

/// Refuses empty, separator-carrying, or relative-path values, which would compose
/// a table root outside the storage base path.
fn direct_storage_directory(table_identifier: &str) -> Result<&str, UdfError> {
    let refusal = |reason: &str| {
        Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no \
             first-level directory under the storage base path — {reason}; drop and \
             recreate the virtual schema"
        )))
    };
    if table_identifier.trim().is_empty() {
        return refusal("it is empty");
    }
    if table_identifier.contains('/') || table_identifier.contains('\\') {
        return refusal("it carries a path separator");
    }
    if table_identifier == "." || table_identifier == ".." {
        return refusal("it names a relative path rather than a directory");
    }
    Ok(table_identifier)
}

/// An identifier with no separator (empty namespace) or an empty last segment is
/// refused rather than sent to the catalog: falling back to an earlier segment
/// would address a different table.
fn unity_table_ident(table_identifier: &str) -> Result<CatalogTableIdent, UdfError> {
    let Some((namespace, name)) = table_identifier.rsplit_once('.') else {
        return Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no Unity Catalog \
             table — a Unity Catalog table is addressed as 'catalog.schema.table'; drop and \
             recreate the virtual schema"
        )));
    };
    if name.trim().is_empty() {
        return Err(UdfError::User(format!(
            "pushdown: the recorded catalog identifier '{table_identifier}' names no table — \
             its last dot-separated segment is empty; drop and recreate the virtual schema"
        )));
    }
    Ok(CatalogTableIdent {
        namespace: namespace.split('.').map(String::from).collect(),
        name: name.to_string(),
    })
}
