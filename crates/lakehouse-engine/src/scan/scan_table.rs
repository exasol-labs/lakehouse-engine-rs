//! The table provider every Parquet scan registers, whatever format planned it. A delete-free
//! file and a delete-carrying one go through the same provider.

use crate::scan::delete_source::DeleteApplyingSource;
use crate::scan::parquet_reader::object_meta_for;
use crate::scan::partition_values::PartitionedScanGroups;
use crate::scan::raw_scan::scan_table_parquet_format;
use crate::scan::row_deletes::{ShardDeletes, redact};
use crate::scan::shared_files::{SMALL_FILE_BYTES, ScanFiles, SharedReaderFactory};
use crate::scan::spec::{FileEntry, StorageBackend};
use crate::scan::{FieldIdExprAdapterFactory, FieldIdResolution, reconstruct_abs_uri};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use exasol_udf_sdk::error::UdfError;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// DataFusion cuts no file below this many bytes per range (`repartition_file_min_size`).
const MIN_RANGE_BYTES: u64 = 1024 * 1024;

/// Files with the same partition columns form one file scan. The provider builds each scan's
/// file groups itself and reads every file through one [`ScanFiles`]. `scan()` issues no request.
#[derive(Debug)]
pub(crate) struct ParquetScanTable {
    object_store_url: ObjectStoreUrl,
    schema: PartitionedScanGroups,
    use_field_id_adapter: bool,
    field_id_resolution: FieldIdResolution,
    files: Vec<FileEntry>,
    table_root: String,
    secrets: Arc<[String]>,
    format: Arc<ParquetFormat>,
    /// Shared by every provider of one invocation (both join sides included), so the whole
    /// instance stays within one N-permit budget.
    delete_path_read_limiter: Arc<Semaphore>,
}

/// At most `target` groups, one per stream. A file up to [`SMALL_FILE_BYTES`] stays whole, and a
/// larger file is cut into byte ranges of about an even share of the scan's bytes.
fn file_groups(files: Vec<PartitionedFile>, target: usize) -> Vec<FileGroup> {
    let target = target.max(1) as u64;
    let total: u64 = files.iter().map(|file| file.object_meta.size).sum();
    let range_bytes = total.div_ceil(target).max(MIN_RANGE_BYTES);
    let mut pieces = Vec::new();
    for file in files {
        let size = file.object_meta.size;
        if size <= SMALL_FILE_BYTES {
            pieces.push(file);
            continue;
        }
        let ranges = size.div_ceil(range_bytes).min(target);
        let step = size.div_ceil(ranges);
        for start in (0..size).step_by(step as usize) {
            let end = (start + step).min(size);
            pieces.push(file.clone().with_range(start as i64, end as i64));
        }
    }
    let mut groups = vec![Vec::new(); (target as usize).min(pieces.len()).max(1)];
    let count = groups.len();
    for (index, piece) in pieces.into_iter().enumerate() {
        groups[index % count].push(piece);
    }
    groups.into_iter().map(FileGroup::new).collect()
}

impl ParquetScanTable {
    /// `use_field_id_adapter` is false only for legacy specs that fell back to first-file
    /// inference. `field_id_resolution`'s nested member trees also decide this table's Parquet
    /// read options via [`scan_table_parquet_format`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        object_store_url: ObjectStoreUrl,
        schema: PartitionedScanGroups,
        use_field_id_adapter: bool,
        field_id_resolution: FieldIdResolution,
        files: Vec<FileEntry>,
        table_root: String,
        storage: &StorageBackend,
        delete_path_read_limiter: Arc<Semaphore>,
    ) -> Self {
        let secrets = storage
            .secret_values()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let format = Arc::new(scan_table_parquet_format(&field_id_resolution));
        Self {
            object_store_url,
            schema,
            use_field_id_adapter,
            field_id_resolution,
            files,
            table_root,
            secrets,
            format,
            delete_path_read_limiter,
        }
    }

    fn partitioned_files(&self) -> Result<Vec<PartitionedFile>, UdfError> {
        self.files
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let values = self.schema.groups()[self.schema.group_of(index)]
                    .partition_values_for(entry)
                    .map_err(|e| UdfError::User(redact(e, &self.secrets)))?;
                let meta = object_meta_for(
                    &reconstruct_abs_uri(&entry.path, &self.table_root),
                    entry.size,
                )?;
                Ok(PartitionedFile::from(meta).with_partition_values(values))
            })
            .collect()
    }

    /// The scan's shared file state, with the shard's delete payloads planned but not read.
    fn scan_files(
        &self,
        state: &dyn Session,
        files: &[PartitionedFile],
        projection: Option<&Vec<usize>>,
    ) -> Result<ScanFiles, UdfError> {
        let runtime = state.runtime_env();
        let store = runtime.object_store(&self.object_store_url).map_err(|e| {
            UdfError::User(redact(
                format!("scan object store unavailable: {e}"),
                &self.secrets,
            ))
        })?;
        let metadata_size_hint = self.format.metadata_size_hint();
        let deletes = ShardDeletes {
            files: &self.files,
            table_root: &self.table_root,
            secrets: Arc::clone(&self.secrets),
            limiter: Arc::clone(&self.delete_path_read_limiter),
            reservation: Arc::new(
                MemoryConsumer::new("row-position deletes").register(&runtime.memory_pool),
            ),
            metadata_size_hint,
        }
        .plan(&store)?;
        let shared = files.iter().zip(&self.files).map(|(file, entry)| {
            let delete_key = (!entry.deletes.is_empty())
                .then(|| reconstruct_abs_uri(&entry.path, &self.table_root));
            (
                file.object_meta.location.clone(),
                file.object_meta.size,
                delete_key,
            )
        });
        // Below half of the columns, a whole-file fetch moves more unread bytes than the
        // requests it saves are worth.
        let columns = self.schema.declared_schema().fields().len();
        let whole_small_files = projection.is_none_or(|p| p.len() * 2 >= columns);
        Ok(ScanFiles::new(
            store,
            metadata_size_hint,
            whole_small_files,
            &runtime.memory_pool,
            shared,
            deletes,
        ))
    }
}

#[async_trait]
impl TableProvider for ParquetScanTable {
    /// DECLARED column order; the `file ++ partition` split stays inside [`Self::scan`].
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.schema.declared_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let external = |e: UdfError| DataFusionError::External(Box::new(e));
        let files = self.partitioned_files().map_err(external)?;
        let scan_files = Arc::new(
            self.scan_files(state, &files, projection)
                .map_err(external)?,
        );

        let mut files_by_group = vec![Vec::new(); self.schema.groups().len()];
        for (index, file) in files.into_iter().enumerate() {
            files_by_group[self.schema.group_of(index)].push(file);
        }

        let target_partitions = state.config().target_partitions();
        let mut plans = Vec::with_capacity(files_by_group.len());
        for (schema, files) in self.schema.groups().iter().zip(files_by_group) {
            let source: Arc<dyn FileSource> = Arc::new(
                ParquetSource::new(schema.file_source_schema().clone())
                    .with_table_parquet_options(self.format.options().clone())
                    .with_parquet_file_reader_factory(Arc::new(SharedReaderFactory(Arc::clone(
                        &scan_files,
                    )))),
            );
            let source = if scan_files.applies_deletes() {
                DeleteApplyingSource::wrap(source, &scan_files)
            } else {
                source
            };
            let expr_adapter = self.use_field_id_adapter.then(|| {
                Arc::new(FieldIdExprAdapterFactory {
                    resolution: self.field_id_resolution.clone(),
                    table_root: self.table_root.clone(),
                }) as Arc<_>
            });

            let config = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_file_groups(file_groups(files, target_partitions))
                .with_projection_indices(schema.remap_projection(projection))?
                .with_limit(limit)
                .with_expr_adapter(expr_adapter)
                .build();
            plans.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
        }
        // A single group returns its own plan, so a scan without unlisted keys is unchanged.
        UnionExec::try_new(plans)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact, like `ListingTable`: the scan prunes with it, but a `FilterExec` above keeps
        // correctness independent of the scan.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
#[path = "scan_table_tests.rs"]
mod tests;
