use crate::adapter::ResolvedConnectionConfig;
use crate::adapter::sharding::partition_files_by_bytes;
use crate::adapter::tuning::Tuning;
use crate::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NameMappingEntry, ScanStorage, StorageBackend,
};
use exasol_udf_sdk::error::UdfError;
use vs_expression::ExasolVersion;

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

    /// A scan of one table with no clause, carrying the request's tuning.
    pub(super) fn common_spec(
        &self,
        table_root: String,
        logical_schema: Vec<LogicalField>,
        name_mapping: Vec<NameMappingEntry>,
        partition_columns: Vec<String>,
        storage: ScanStorage,
    ) -> CommonScanSpec {
        let tuning = &self.tuning;
        CommonScanSpec {
            table_root,
            projection: Vec::new(),
            filter: None,
            limit: None,
            order_by: Vec::new(),
            aggregates: None,
            group_keys: None,
            distinct: false,
            logical_schema,
            name_mapping,
            join: None,
            partition_columns,
            storage,
            df_target_partitions: tuning.df_target_partitions,
            df_batch_size: tuning.df_batch_size,
            df_threads_per_udf: tuning.df_threads_per_udf,
            memory_pool_fraction: tuning.memory_pool_fraction,
            instance_overhead_mb: tuning.instance_overhead_mb,
            s3_max_connections: tuning.s3_max_connections,
            binary_as_string: self.binary_as_string,
        }
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
