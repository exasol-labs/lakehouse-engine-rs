use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use exasol_udf_sdk::error::UdfError;
use lakehouse_catalog::StorageBackend;
use object_store::ObjectStore;
use serde_json::Value as Json;

use super::partition_predicate::PartitionPredicate;
use super::unity_table_storage::redacted;
use super::{LoadedTable, PlannedFiles};
use crate::adapter::column_notes::DeclaredSchema;
use crate::adapter::parquet_directory::{ParquetFile, ParquetListing, PartitionKeepPredicate};
use crate::scan::ObjectLocation;
use crate::scan::spec::FileEntry;

#[cfg(test)]
#[path = "parquet_directory_table_tests.rs"]
mod tests;

/// A directory's full listing, against which each reference declares its partition columns; no
/// footer is read. Serves a direct-storage table and a Unity Catalog Parquet table alike.
pub(in crate::adapter::pushdown) struct ParquetDirectoryTable {
    listing: ParquetListing,
    table_root: ObjectLocation,
    /// The backend the listing was read through, which the scan reads the files through.
    storage: StorageBackend,
}

impl ParquetDirectoryTable {
    /// Errors are redacted against `storage`'s secrets, because `store` reads through it.
    pub(in crate::adapter::pushdown) async fn open(
        store: &Arc<dyn ObjectStore>,
        table_root: &ObjectLocation,
        storage: &StorageBackend,
    ) -> Result<Self, UdfError> {
        let listing = ParquetListing::list(store, table_root.key())
            .await
            .map_err(|error| redacted(error, &storage.secret_values()))?;
        Ok(Self {
            listing,
            table_root: table_root.clone(),
            storage: storage.clone(),
        })
    }

    /// The listed files whose `partition_columns` values `keep` admits.
    pub(super) fn plan(
        &self,
        partition_columns: &[String],
        keep: &PartitionKeepPredicate,
    ) -> Result<PlannedFiles, UdfError> {
        let files = self
            .listing
            .kept_files(partition_columns, keep)
            .map_err(|error| redacted(error, &self.storage.secret_values()))?
            .into_iter()
            .map(|file| file_entry(file, &self.table_root))
            .collect();
        Ok(PlannedFiles {
            files,
            effective_storage: self.storage.clone(),
            table_root: self.table_root.to_wire(),
            name_mapping: Vec::new(),
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
            self.plan(&declared.partition_columns, &keep)
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
