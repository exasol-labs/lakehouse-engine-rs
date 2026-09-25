use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::{CatalogTable, StorageBackend, UnityCatalogSession};
use serde_json::Value as Json;

use super::delta_predicate::to_delta_predicate;
use super::delta_replay::DeltaSnapshot;
use super::delta_schema::build_delta_table_schema;
use super::unity_table_storage::{UnityTableStorage, redacted};
use super::{
    ConnectionStorage, FormatReader, RefusedColumn, ResolvedScan,
    ensure_table_has_a_mappable_column,
};
use crate::scan::build_table_root_store;
use crate::scan::spec::{DEFAULT_S3_MAX_CONNECTIONS, FileEntry, LogicalField};

#[cfg(test)]
#[path = "delta_format_reader_tests.rs"]
mod tests;

/// The Delta table reader: one Unity Catalog table's transaction log resolved into
/// the scan the pushdown layer plans against.
///
/// The effective backend leaves with the resolved scan: the scan must read files
/// through the same backend the log was read through.
pub(super) struct DeltaFormatReader<'a> {
    storage: UnityTableStorage<'a>,
}

impl<'a> DeltaFormatReader<'a> {
    pub(super) fn new(
        session: &'a UnityCatalogSession,
        table: &'a CatalogTable,
        connection: &ConnectionStorage<'a>,
    ) -> Self {
        Self {
            storage: UnityTableStorage::new(session, table, connection),
        }
    }
}

impl FormatReader for DeltaFormatReader<'_> {
    /// Forwards `filter_json` to [`read_delta_log`], which builds a `delta_kernel`
    /// predicate from it and applies it during log replay to prune files by partition
    /// value and per-file statistics before scanning.
    ///
    /// `name_mapping` is always empty: a column binding by physical name declares that
    /// name on its own [`LogicalField`], which the scan-side binding consults BEFORE
    /// any table-level mapping. An Iceberg-shaped name mapping here could therefore
    /// never be reached, and would be a second home for one decision, free to drift
    /// from it.
    fn resolve_scan<'a>(
        &'a self,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedScan, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let (table_root, effective_storage) = self.storage.resolve().await?;
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
    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns, "Delta")?;

    let prune = filter_json
        .and_then(|filter| to_delta_predicate(filter, &snapshot.schema()))
        .map(Arc::new);

    let files = snapshot
        .active_files(prune)
        .map_err(|error| redacted(error, secrets))?;

    Ok((files, logical_schema, partition_columns, refused_columns))
}
