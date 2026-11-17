use crate::adapter::ResolvedConnectionConfig;
use crate::adapter::sharding::partition_files_by_bytes;
use crate::adapter::tuning::Tuning;
use crate::scan::spec::{CommonScanSpec, FileEntry, ScanStorage, StorageBackend};
use exasol_udf_sdk::error::UdfError;
use vs_expression::ExasolVersion;

use super::ResolvedScan;
use super::shard_paths::relativize_shards_to_root;
use super::support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, quote_ident, scan_storage_for, shard_count,
};

/// Every setting a pushdown request plans with besides its tables, resolved once per request, so
/// no path plans with a setting another path lacks (#563).
pub(crate) struct PushdownContext<'a> {
    pub(crate) connection: &'a ResolvedConnectionConfig,
    pub(crate) udfs: ScanUdfs,
    pub(crate) cluster_nodes: usize,
    pub(crate) tuning: Tuning,
    pub(crate) binary_as_string: bool,
    pub(crate) version: ExasolVersion,
}

impl PushdownContext<'_> {
    /// `files` split into the request's shards, each path relative to `table_root`.
    pub(super) fn shards(&self, files: Vec<FileEntry>, table_root: &str) -> Shards {
        let g = shard_count(
            self.cluster_nodes,
            self.tuning.parallelism_factor,
            files.len(),
        );
        Shards {
            files: relativize_shards_to_root(partition_files_by_bytes(files, g), table_root),
            udfs: self.udfs.clone(),
        }
    }

    pub(super) fn scan_storage(&self, effective: &StorageBackend) -> Result<ScanStorage, UdfError> {
        let connection = self.connection;
        scan_storage_for(
            &connection.creds,
            &connection.connection_name,
            connection.allow_http,
            effective,
            connection.sealed_storage_key.as_ref(),
        )
    }

    /// A scan of `table` with no clause, carrying the request's tuning. Fails as
    /// [`Self::scan_storage`], so before any use of the table's files.
    pub(super) fn common_spec(&self, table: &ResolvedScan) -> Result<CommonScanSpec, UdfError> {
        Ok(CommonScanSpec {
            table_root: table.table_root.clone(),
            projection: Vec::new(),
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            logical_schema: table.logical_schema.clone(),
            name_mapping: table.name_mapping.clone(),
            join: None,
            partition_columns: table.partition_columns.clone(),
            storage: self.scan_storage(&table.effective_storage)?,
            tuning: self.tuning.scan,
            binary_as_string: self.binary_as_string,
        })
    }
}

/// The scan and distributor UDFs a fan-out calls, schema-qualified: the pushdown query runs
/// outside the adapter script's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanUdfs {
    pub(super) scan: String,
    pub(super) distribute: String,
}

impl ScanUdfs {
    /// An absent or empty `schema` leaves the names unqualified.
    pub fn in_schema(schema: Option<&str>) -> Self {
        let qualified = |udf: &str| match schema {
            Some(schema) if !schema.is_empty() => format!("{}.{udf}", quote_ident(schema)),
            _ => udf.to_string(),
        };
        Self {
            scan: qualified(SCAN_UDF_NAME),
            distribute: qualified(DISTRIBUTE_FILES_UDF_NAME),
        }
    }
}

/// A table's files split into shards, with the UDFs that scan them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shards<E = FileEntry> {
    pub files: Vec<Vec<E>>,
    pub udfs: ScanUdfs,
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
