use std::future::Future;
use std::pin::Pin;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{
    CatalogTable, ConnectionCreds, StaticStoreAddress, StorageBackend, UnityCatalogSession,
    redact_error_text, resolve_uc_vended_storage,
};
use serde_json::Value as Json;

use super::delta_replay::DeltaSnapshot;
use super::delta_schema::build_delta_table_schema;
use super::{ConnectionStorage, FormatReader, RefusedColumn, ResolvedScan};
use crate::adapter::tables::catalog_identifier_string;
use crate::scan::build_table_root_store;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField};

#[cfg(test)]
#[path = "delta_format_reader_tests.rs"]
mod tests;

/// The Unity Catalog credential-vending operation a plan-time log read asks for.
/// Planning never writes, and a write-scoped credential would grant the scan more
/// than it needs.
const READ_OPERATION: &str = "READ";

/// The Delta table reader: one Unity Catalog table's transaction log resolved into
/// the scan the pushdown layer plans against.
///
/// Deep by design — it owns the WHOLE resolution behind one call, including the
/// storage-credential decision. That decision cannot be hoisted to a shared caller:
/// under vending it is scoped to THIS table's catalog-assigned key, and the log is
/// read through its result, so the credential and the file list are one indivisible
/// step. The effective backend leaves with the resolved scan precisely because the
/// scan side must read the files through the same backend the log was read through.
pub(super) struct DeltaFormatReader<'a> {
    session: &'a UnityCatalogSession,
    table: &'a CatalogTable,
    storage: &'a StorageBackend,
    creds: &'a ConnectionCreds,
    allow_http: bool,
}

impl<'a> DeltaFormatReader<'a> {
    /// `connection` is the CONNECTION's static storage decision: its static storage
    /// backend and resolved credentials, plus the resolved `ALLOW_HTTP` property,
    /// which under vending is the operator's consent gate for plaintext transport.
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

    /// This table's own catalog-reported storage location.
    ///
    /// The ONE check that runs before the vended/static split, so both values of
    /// `use_vended_credentials` report identical text and a malformed catalog
    /// response costs zero object-storage access. Nothing else denotes the table's
    /// object store — the catalog URI names a REST service and the CONNECTION
    /// endpoint names the operator's own store address — so no CONNECTION-derived
    /// value may stand in for a location the catalog left empty.
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

    /// The storage backend this table's log is read THROUGH: the vended backend under
    /// vending, the CONNECTION's static one otherwise.
    ///
    /// Vending is credentials-only: a table whose catalog assigned no vending key
    /// fails here rather than falling back, because the fallback would read object
    /// storage with a credential the operator did not select for this table. An empty
    /// key counts as none — requesting against an empty scope asks the catalog to
    /// choose the table for us.
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
    /// `filter_json` prunes nothing: Delta-level file pruning needs the per-file
    /// statistics this plan deliberately carries none of (issue #321), and partition
    /// pruning is the scan side's once it reconstructs partition columns (issue #320).
    ///
    /// `name_mapping` is always empty: a column binding by physical name declares that
    /// name on its own [`LogicalField`], which the scan-side binding consults BEFORE
    /// any table-level mapping. An Iceberg-shaped name mapping here could therefore
    /// never be reached, and would be a second home for one decision, free to drift
    /// from it.
    fn resolve_scan<'a>(
        &'a self,
        _filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let table_root = self.checked_table_root()?;
            let effective_storage = self.effective_storage(table_root).await?;
            let secrets = effective_storage.secret_values();

            let (files, logical_schema, partition_columns, refused_columns) =
                read_delta_log(&effective_storage, table_root, &secrets)?;

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

/// The four values a Delta log read contributes to a [`ResolvedScan`]: the active
/// data files, the logical schema, the table's ordered partition columns, and the
/// columns the schema step declined to map.
type DeltaLogContents = (
    Vec<FileEntry>,
    Vec<LogicalField>,
    Vec<String>,
    Vec<RefusedColumn>,
);

/// Read `table_root`'s Delta log through a store built from `storage`, answering the
/// active file list, the logical schema, the table's ordered partition columns, and
/// the columns the schema step declined to map.
///
/// Blocks: `delta_kernel`'s read path is synchronous and drives its own runtime on its
/// own thread, so this stalls only the caller's own executor, which has nothing else
/// to make progress on while the log is being read.
///
/// Every error is redacted here rather than where it was raised: the replay and schema
/// steps know nothing about credentials by design, and this is the layer that made the
/// credential decision and therefore knows the value set an object-store error could
/// echo back.
fn read_delta_log(
    storage: &StorageBackend,
    table_root: &str,
    secrets: &[&str],
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

    let files = snapshot
        .active_files()
        .map_err(|error| redacted(error, secrets))?;

    Ok((files, logical_schema, partition_columns, refused_columns))
}

/// Refuses the WHOLE table when NO column classified as mappable: `raw_scan` registers
/// `logical_schema` as the DataFusion table's own schema, and an EMPTY logical schema is not a
/// table this engine can scan. Falling back to the first data file's own schema — the only other
/// source such a table could offer — would bind columns by physical file order and by physical
/// file name, exactly the unauthorized binding the column-mapping binding key exists to prevent.
///
/// A table with at least one mappable column is left fully alone here: it stays queryable on its
/// mappable columns, and only a request that reads or emits a refused one is turned away — this
/// function refuses a whole TABLE, never a single request.
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

/// Re-raise `error` with every value in `secrets` masked.
///
/// Collapses onto [`UdfError::User`] deliberately: every error reaching here is a
/// plan-time refusal a user must read, and rendering the error through `Display`
/// keeps a variant's own prefix in the text while leaving no payload a future SDK
/// variant could smuggle a secret through unmasked.
fn redacted(error: UdfError, secrets: &[&str]) -> UdfError {
    UdfError::User(redact_error_text(&error.to_string(), secrets))
}
