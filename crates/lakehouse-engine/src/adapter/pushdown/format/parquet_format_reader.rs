use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use serde_json::Value as Json;

use super::partition_predicate::PartitionPredicate;
use super::{FormatReader, PlannedFiles};
use crate::adapter::column_notes::DeclaredSchema;
use crate::adapter::parquet_directory::{ParquetFile, list_parquet_files};
use crate::scan::ObjectLocation;
use crate::scan::spec::FileEntry;

#[cfg(test)]
#[path = "parquet_format_reader_tests.rs"]
mod tests;

/// Lists files against the declared partition columns and reads no footer.
pub(super) struct ParquetFormatReader<'a> {
    /// Shared across every leg of the request (one admission limiter).
    pub(super) store: &'a Arc<dyn ObjectStore>,
    pub(super) table_root: &'a ObjectLocation,
    /// The CONNECTION's static backend; no credential-vending catalog overrides it.
    pub(super) storage: &'a StorageBackend,
}

impl FormatReader for ParquetFormatReader<'_> {
    /// Prunes files by partition-column predicates; other predicates never prune (no file
    /// statistics).
    fn resolve_scan<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let predicate = PartitionPredicate::from_filter(filter_json, &[]);
            let keep = move |values: &_| predicate.keeps(values);
            let root = self.table_root;
            let files =
                list_parquet_files(self.store, root.key(), &declared.partition_columns, &keep)
                    .await?
                    .into_iter()
                    .map(|file| file_entry(file, root))
                    .collect();
            Ok(PlannedFiles {
                files,
                effective_storage: self.storage.clone(),
                table_root: root.to_wire(),
                name_mapping: Vec::new(),
            })
        })
    }
}

/// Size comes from the listing (no extra HEAD); a path outside the table root stays absolute.
pub(super) fn file_entry(file: ParquetFile, table_root: &ObjectLocation) -> FileEntry {
    let location = table_root.at(file.path);
    FileEntry {
        path: location
            .relative_to(table_root)
            .unwrap_or_else(|| location.to_wire()),
        size: file.size,
        deletes: Vec::new(),
        partition_values: file.partition_values,
    }
}
