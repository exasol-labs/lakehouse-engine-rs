use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use serde_json::Value as Json;

use super::partition_predicate::PartitionPredicate;
use super::{LoadedTable, PlannedFiles};
use crate::adapter::column_notes::DeclaredSchema;
use crate::adapter::parquet_directory::{ParquetFile, ParquetListing};
use crate::scan::ObjectLocation;
use crate::scan::spec::FileEntry;

#[cfg(test)]
#[path = "parquet_format_reader_tests.rs"]
mod tests;

/// A directory's full listing, against which each reference declares its partition columns; no
/// footer is read.
pub(in crate::adapter::pushdown) struct ParquetDirectoryTable {
    listing: ParquetListing,
    table_root: ObjectLocation,
    /// The CONNECTION's static backend; no credential-vending catalog overrides it.
    storage: StorageBackend,
}

impl ParquetDirectoryTable {
    /// `store` is shared by every load of the virtual schema's statement (one admission limiter).
    pub(in crate::adapter::pushdown) async fn open(
        store: &Arc<dyn ObjectStore>,
        table_root: &ObjectLocation,
        storage: &StorageBackend,
    ) -> Result<Self, UdfError> {
        Ok(Self {
            listing: ParquetListing::list(store, table_root.key()).await?,
            table_root: table_root.clone(),
            storage: storage.clone(),
        })
    }
}

impl LoadedTable for ParquetDirectoryTable {
    /// Prunes files by partition-column predicates; other predicates never prune (no file
    /// statistics).
    fn prune<'a>(
        &'a self,
        declared: &'a DeclaredSchema,
        filter_json: Option<&'a Json>,
    ) -> Pin<Box<dyn Future<Output = Result<PlannedFiles, UdfError>> + Send + 'a>> {
        Box::pin(async move {
            let predicate = PartitionPredicate::from_filter(filter_json, &[]);
            let keep = move |values: &_| predicate.keeps(values);
            let root = &self.table_root;
            let files = self
                .listing
                .kept_files(&declared.partition_columns, &keep)?
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
