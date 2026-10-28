//! The table provider every Parquet scan registers, whatever format planned it. It attaches each
//! data file's row-position deletes from [`row_deletes`](super::row_deletes) as a base access
//! plan, so a delete-free file and a delete-carrying one go through the same provider.

use crate::scan::diagnostics;
use crate::scan::parquet_reader::object_meta_for;
use crate::scan::partition_values::PartitionedScanGroups;
use crate::scan::raw_scan::scan_table_parquet_format;
use crate::scan::row_deletes::{
    ShardDeletes, access_plan_bytes, build_access_plan, join_spawned, redact, reserve,
};
use crate::scan::spec::{FileEntry, StorageBackend};
use crate::scan::{FieldIdExprAdapterFactory, FieldIdResolution, reconstruct_abs_uri};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use exasol_udf_sdk::error::UdfError;
use parquet::file::metadata::PageIndexPolicy;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

/// Files with the same partition columns go into ONE `FileGroup`, which DataFusion's file-scan
/// repartitioning splits into byte ranges up to `target_partitions`. Each plan is built
/// through [`ParquetFormat::create_physical_plan`], which applies THIS provider's Parquet
/// options (making row-filter pushdown per table) and a `CachedParquetFileReaderFactory` over
/// the session [`FileMetadataCache`]. Access-plan construction reads through that same cache
/// with the same [`ParquetFormat::metadata_size_hint`], so a footer parses once. That holds only
/// because access-plan construction is the FIRST footer reader: the adapter always supplies a
/// `logical_schema`, keeping `register_file_list` off the `infer_schema` fallback.
///
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
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
    /// Held as long as the provider, because the access plans live until the scan finishes.
    delete_state_reservations: Mutex<Vec<Arc<MemoryReservation>>>,
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
            delete_state_reservations: Mutex::new(Vec::new()),
        }
    }

    /// Partition values convert first, so a spec-content failure precedes every fetch.
    ///
    /// Only delete-carrying entries take a limiter permit, fetching their own footer through the
    /// shared session [`FileMetadataCache`] with the opener's size hint and page index skipped
    /// ([`build_access_plan`] needs only row counts), so the footer parses once for both. Each
    /// fetched footer is recorded via [`diagnostics::record_access_plan_cached_footer`] so a
    /// cache eviction is observable.
    ///
    /// The position sets are reserved in the session memory pool as they decode, and the access
    /// plans as they are built, so a shard whose delete state outgrows the pool fails mid-build.
    ///
    /// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
    async fn partitioned_files(
        &self,
        state: &dyn Session,
    ) -> Result<Vec<PartitionedFile>, UdfError> {
        let partition_values = self
            .files
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                self.schema.groups()[self.schema.group_of(index)].partition_values_for(entry)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| UdfError::User(redact(e, &self.secrets)))?;

        let store = state
            .runtime_env()
            .object_store(&self.object_store_url)
            .map_err(|e| {
                UdfError::User(redact(
                    format!("scan object store unavailable: {e}"),
                    &self.secrets,
                ))
            })?;
        let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

        let abs_paths: Vec<String> = self
            .files
            .iter()
            .map(|entry| reconstruct_abs_uri(&entry.path, &self.table_root))
            .collect();
        let mut files = self
            .files
            .iter()
            .zip(&abs_paths)
            .zip(partition_values)
            .map(|((entry, abs), values)| {
                let meta = object_meta_for(abs, entry.size)?;
                Ok(PartitionedFile::from(meta).with_partition_values(values))
            })
            .collect::<Result<Vec<_>, UdfError>>()?;

        if self.files.iter().all(|entry| entry.deletes.is_empty()) {
            return Ok(files);
        }
        let reservation = Arc::new(
            MemoryConsumer::new("row-position deletes").register(&state.runtime_env().memory_pool),
        );
        let size_hint = self.format.metadata_size_hint();
        let delete_positions = ShardDeletes {
            files: &self.files,
            table_root: &self.table_root,
            secrets: Arc::clone(&self.secrets),
            limiter: Arc::clone(&self.delete_path_read_limiter),
            reservation: Arc::clone(&reservation),
            metadata_size_hint: size_hint,
        }
        .collect_positions(&store)
        .await?;
        if delete_positions.is_empty() {
            return Ok(files);
        }

        let positions_bytes = reservation.size();
        let delete_positions: HashMap<String, Arc<RoaringTreemap>> = delete_positions
            .into_iter()
            .filter(|(_, positions)| !positions.is_empty())
            .map(|(path, positions)| (path, Arc::new(positions)))
            .collect();

        let builds = files
            .iter()
            .zip(&abs_paths)
            .enumerate()
            .filter_map(|(idx, (file, abs))| {
                let deletes = Arc::clone(delete_positions.get(abs.as_str())?);
                let abs = abs.clone();
                let meta = file.object_meta.clone();
                let store = Arc::clone(&store);
                let metadata_cache = Arc::clone(&metadata_cache);
                let limiter = Arc::clone(&self.delete_path_read_limiter);
                let secrets = Arc::clone(&self.secrets);
                let reservation = Arc::clone(&reservation);
                Some(async move {
                    let permit = limiter.acquire_owned().await.map_err(|e| {
                        UdfError::User(redact(
                            format!(
                                "delete_path_read_limiter permit unavailable for data-file footer fetch of {abs}: {e}"
                            ),
                            &secrets,
                        ))
                    })?;
                    let parquet_metadata = DFParquetMetadata::new(store.as_ref(), &meta)
                        .with_file_metadata_cache(Some(metadata_cache))
                        .with_metadata_size_hint(size_hint)
                        .with_page_index_policy(Some(PageIndexPolicy::Skip))
                        .fetch_metadata()
                        .await
                        .map_err(|e| {
                            UdfError::User(redact(
                                format!(
                                    "failed to read data-file metadata for delete application: {e}"
                                ),
                                &secrets,
                            ))
                        })?;
                    diagnostics::record_access_plan_cached_footer(&meta.location);
                    drop(permit);

                    let access_plan = build_access_plan(parquet_metadata.row_groups(), &deletes);
                    reserve(&reservation, access_plan_bytes(&access_plan), &secrets)?;
                    Ok((idx, access_plan))
                })
            })
            .collect::<Vec<_>>();
        for (idx, access_plan) in join_spawned(builds).await? {
            files[idx].extensions.insert(access_plan);
        }

        drop(delete_positions);
        reservation.shrink(positions_bytes);
        if let Ok(mut held) = self.delete_state_reservations.lock() {
            held.push(reservation);
        }
        Ok(files)
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
        let files = self
            .partitioned_files(state)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut files_by_group = vec![Vec::new(); self.schema.groups().len()];
        for (index, file) in files.into_iter().enumerate() {
            files_by_group[self.schema.group_of(index)].push(file);
        }

        let mut plans = Vec::with_capacity(files_by_group.len());
        for (schema, files) in self.schema.groups().iter().zip(files_by_group) {
            let file_source = self.format.file_source(schema.file_source_schema().clone());
            let expr_adapter = self.use_field_id_adapter.then(|| {
                Arc::new(FieldIdExprAdapterFactory {
                    resolution: self.field_id_resolution.clone(),
                    table_root: self.table_root.clone(),
                }) as Arc<_>
            });

            // DataFusion splits each file group into `target_partitions` byte ranges, and each
            // range keeps its file's base access plan.
            let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
                .with_file_group(FileGroup::new(files))
                .with_projection_indices(schema.remap_projection(projection))?
                .with_limit(limit)
                .with_expr_adapter(expr_adapter)
                .build();
            plans.push(self.format.create_physical_plan(state, config).await?);
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
