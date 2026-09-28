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
    /// `name_mapping` is always empty: a physical-name binding lives on its own
    /// [`LogicalField`], which the scan side consults before any table-level mapping, so a
    /// mapping here would be unreachable and a second home for the same decision.
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
    ensure_table_has_a_mappable_column(&logical_schema, &refused_columns, "Delta")?;

    let prune = filter_json
        .and_then(|filter| to_delta_predicate(filter, &snapshot.schema()))
        .map(Arc::new);

    let files = snapshot
        .active_files(prune)
        .map_err(|error| redacted(error, secrets))?;

    Ok((files, logical_schema, partition_columns, refused_columns))
}
