use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogTable, ConnectionCreds, StaticStoreAddress, StorageBackend, UnityCatalogSession,
    redact_error_text, resolve_uc_vended_storage,
};
use serde_json::Value as Json;

use super::delta_predicate::to_delta_predicate;
use super::delta_replay::DeltaSnapshot;
use super::delta_schema::build_delta_table_schema;
use super::{ConnectionStorage, FormatReader, RefusedColumn, ResolvedScan};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::build_table_root_store;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField};

#[cfg(test)]
#[path = "delta_format_reader_tests.rs"]
mod tests;

/// Planning never writes; a write-scoped credential would over-grant the scan.
const READ_OPERATION: &str = "READ";

/// Owns the whole resolution, including the storage-credential decision: under vending
/// the credential is scoped to this table's catalog-assigned key and the log is read
/// through it, so credential and file list are one step. The scan side must read files
/// through the same backend, hence it leaves with the resolved scan.
pub(super) struct DeltaFormatReader<'a> {
    session: &'a UnityCatalogSession,
    table: &'a CatalogTable,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
}

impl<'a> DeltaFormatReader<'a> {
    /// Under vending, `connection.allow_http` is the operator's consent gate for plaintext
    /// transport.
    pub(super) fn new(
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
        connection: &ConnectionStorage<'a>,
    ) -> Self {
        Self {
            session,
            table,
            storage: connection.storage,
            creds: connection.creds,
            allow_http: connection.allow_http,
        }
    }

    /// Runs before the vended/static split so both modes report identical text and a
    /// malformed catalog response costs no object-storage access. No CONNECTION-derived
    /// value may stand in for an empty location: the catalog URI names a REST service and
    /// the endpoint names the operator's own store.
    fn checked_table_root(&self) -> Result<&'a str, UdfError> {
        match self.table.storage_location.as_deref() {
            Some(location) if !location.trim().is_empty() => Ok(location),
            _ => Err(UdfError::User(format!(
                "the Unity Catalog metadata for table {} carries an EMPTY storage location; \
                 the catalog URI and the CONNECTION endpoint name no table location and are \
                 not valid substitutes",
                self.table_name()
            ))),
        }
    }

    /// Vending is credentials-only: a table with no (or an empty) vending key fails rather
    /// than falling back to the static credential the operator did not select for it; an
    /// empty scope would let the catalog choose the table.
    async fn effective_storage(&self, table_root: &str) -> Result<StorageBackend, UdfError> {
        if !self.creds.use_vended_credentials {
            return Ok(self.storage.clone());
        }

        let vending_key = self
            .table
            .vended_credential_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                UdfError::User(format!(
                    "USE_VENDED_CREDENTIALS is enabled, but Unity Catalog reported no \
                     storage-credential vending key for table {}; reading it through the \
                     CONNECTION's static credential instead would use a credential the \
                     operator did not select for this table",
                    self.table_name()
                ))
            })?;

        let vended = self
            .session
            .temporary_table_credentials(vending_key, READ_OPERATION)
            .await?;
        resolve_uc_vended_storage(
            &vended,
            table_root,
            self.allow_http,
            &StaticStoreAddress::from(self.creds),
        )
    }

    fn table_name(&self) -> String {
        catalog_identifier_string(&self.table.ident)
    }
}

impl FormatReader for DeltaFormatReader<'_> {
    /// `name_mapping` is always empty: a physical-name binding lives on its own
    /// [`LogicalField`], which the scan side consults before any table-level mapping, so a
    /// mapping here would be unreachable and a second home for the same decision.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let table_root = self.checked_table_root()?;
            let effective_storage = self.effective_storage(table_root).await?;
            let secrets = effective_storage.secret_values();

            let (files, logical_schema, partition_columns, refused_columns) =
                read_delta_log(&effective_storage, table_root, &secrets, filter_json)?;

            Ok(ResolvedScan {
                files,
                effective_storage,
                logical_schema,
                table_root: table_root.to_string(),
                name_mapping: Vec::new(),
                partition_columns,
                refused_columns,
            })
        })
    }
}

type DeltaLogContents = (
    Vec<FileEntry>,
    Vec<LogicalField>,
    Vec<String>,
    Vec<RefusedColumn>,
);

/// Blocks: `delta_kernel`'s read path is synchronous and drives its own runtime.
///
/// Errors are redacted here because this layer made the credential decision and knows
/// which secrets an object-store error could echo; replay and schema know none.
fn read_delta_log(
    storage: &StorageBackend,
    table_root: &str,
    secrets: &[&str],
    filter_json: Option<&Json>,
) -> Result<DeltaLogContents, UdfError> {
    let store = build_table_root_store(storage, table_root, DEFAULT_S3_MAX_CONNECTIONS, secrets)
        .map_err(|error| redacted(error, secrets))?;
    let snapshot =
        DeltaSnapshot::open(store, table_root).map_err(|error| redacted(error, secrets))?;

    let (logical_schema, partition_columns, refused_columns) = build_delta_table_schema(
        &snapshot.schema(),
        snapshot.column_mapping_mode(),
        snapshot.partition_columns(),
    )
    .map_err(|error| redacted(error, secrets))?;
    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns)?;

    let prune = filter_json
        .and_then(|filter| to_delta_predicate(filter, &snapshot.schema()))
        .map(Arc::new);

    let files = snapshot
        .active_files(prune)
        .map_err(|error| redacted(error, secrets))?;

    Ok((files, logical_schema, partition_columns, refused_columns))
}

/// An empty logical schema is not scannable, and falling back to a data file's own schema
/// would bind by physical order and name, which column mapping exists to prevent. A table
/// with at least one mappable column stays queryable; only requests touching a refused
/// column are turned away elsewhere.
fn ensure_table_has_a_mappable_column(
    logical_schema: &[LogicalField],
    refused_columns: &[RefusedColumn],
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
        "Delta table has no mappable column; every column is refused: {reasons}"
    )))
}

/// Collapses to [`UdfError::User`] via `Display`, so the variant prefix survives while no
/// future variant payload can carry a secret unmasked.
fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}
