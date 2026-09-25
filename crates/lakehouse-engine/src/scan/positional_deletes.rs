//! Applies row-position deletes (Iceberg positional-delete files and Delta deletion vectors).
//! Phase A reads every unique delete payload once within one shared connection budget and
//! merges positions into one `HashMap<data_file_path, RoaringTreemap>`, mechanism-agnostic.
//! Phase B turns each set into a base [`ParquetAccessPlan`] on the data file's
//! `PartitionedFile`, so DataFusion's opener intersects predicate/row-group/page pruning on
//! top and deletes compose with pushdown.

use crate::scan::deletion_vectors::{DeletionVector, LoggedDeletionVector};
use crate::scan::diagnostics;
use crate::scan::partition_values::PartitionedScanSchema;
use crate::scan::raw_scan::scan_table_parquet_format;
use crate::scan::spec::{DeleteMechanism, FileEntry, StorageBackend};
use crate::scan::{FieldIdExprAdapterFactory, FieldIdResolution, reconstruct_abs_uri};
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::TimeZone;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use futures::future::try_join_all;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::{PageIndexPolicy, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// `selected_row_groups` lists surviving row groups; deletes in pruned ones are stepped over.
///
/// Vendored from apache/iceberg-rust
/// (`crates/iceberg/src/arrow/reader/positional_deletes.rs::build_deletes_row_selection`, tag
/// `v0.10.0`), where it is not importable. Algorithmically identical, but takes a
/// [`RoaringTreemap`] directly instead of iceberg's private `DeleteVector`. Upstream tracking: #344.
pub(crate) fn build_deletes_row_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    selected_row_groups: &Option<Vec<usize>>,
    positional_deletes: &RoaringTreemap,
) -> RowSelection {
    let mut results: Vec<RowSelector> = Vec::new();
    let mut selected_row_groups_idx = 0;
    let mut current_row_group_base_idx: u64 = 0;
    let mut delete_vector_iter = positional_deletes.iter();
    let mut next_deleted_row_idx_opt = delete_vector_iter.next();

    for (idx, row_group_metadata) in row_group_metadata_list.iter().enumerate() {
        let row_group_num_rows = row_group_metadata.num_rows() as u64;
        let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

        if let Some(selected_row_groups) = selected_row_groups {
            if selected_row_groups_idx == selected_row_groups.len() {
                break;
            }

            if idx == selected_row_groups[selected_row_groups_idx] {
                selected_row_groups_idx += 1;
            } else {
                // `advance_to` repositions the iterator, but a cached value in the skipped
                // range is stale and must be refreshed with `next()`.
                delete_vector_iter.advance_to(next_row_group_base_idx);
                if let Some(cached_idx) = next_deleted_row_idx_opt
                    && cached_idx < next_row_group_base_idx
                {
                    next_deleted_row_idx_opt = delete_vector_iter.next();
                }

                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        }

        let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
            Some(next_deleted_row_idx) => {
                if next_deleted_row_idx >= next_row_group_base_idx {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }

                next_deleted_row_idx
            }

            _ => {
                results.push(RowSelector::select(row_group_num_rows as usize));
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        };

        let mut current_idx = current_row_group_base_idx;
        'chunks: while next_deleted_row_idx < next_row_group_base_idx {
            if current_idx < next_deleted_row_idx {
                let run_length = next_deleted_row_idx - current_idx;
                results.push(RowSelector::select(run_length as usize));
                current_idx += run_length;
            }

            let mut run_length = 0;
            while next_deleted_row_idx == current_idx
                && next_deleted_row_idx < next_row_group_base_idx
            {
                run_length += 1;
                current_idx += 1;

                next_deleted_row_idx_opt = delete_vector_iter.next();
                next_deleted_row_idx = match next_deleted_row_idx_opt {
                    Some(next_deleted_row_idx) => next_deleted_row_idx,
                    _ => {
                        // Final delete: conclude the skip, then select the row group's remainder.
                        results.push(RowSelector::skip(run_length));
                        break 'chunks;
                    }
                };
            }
            if run_length > 0 {
                results.push(RowSelector::skip(run_length));
            }
        }

        if current_idx < next_row_group_base_idx {
            results.push(RowSelector::select(
                (next_row_group_base_idx - current_idx) as usize,
            ));
        }

        current_row_group_base_idx += row_group_num_rows;
    }

    results.into()
}

fn redact(msg: String, secrets: &[String]) -> String {
    let borrowed: Vec<&str> = secrets.iter().map(String::as_str).collect();
    let stripped = crate::scan::emit::redact_secret_values(&msg, &borrowed);
    crate::scan::emit::redact_credentials(&stripped)
}

/// Built without a HEAD; the caller supplies the size.
fn object_meta_for(abs_uri: &str, size: u64) -> Result<ObjectMeta, UdfError> {
    let url = ListingTableUrl::parse(abs_uri)
        .map_err(|e| UdfError::User(format!("invalid file URL '{abs_uri}': {e}")))?;
    Ok(ObjectMeta {
        location: url.prefix().clone(),
        last_modified: chrono::Utc.timestamp_nanos(0),
        size,
        e_tag: None,
        version: None,
    })
}

/// Both variants feed ONE position map, so nothing downstream learns which mechanism produced it.
#[derive(Debug)]
enum ApplicableDelete<'a> {
    /// One such file may carry deletes for many data files.
    PositionalDeleteFile { path: &'a str, size: u64 },
    /// `sidecar` is `None` for an inline vector. Positions index the ONE data file carrying it.
    DeletionVector(DeletionVector),
}

/// Read-time backstop behind the plan-time adapter gate: an unapplicable mechanism is refused
/// before any row of its data file is emitted, and the payload is reachable only through here,
/// so it can neither be read nor silently skipped. `data_file_path` is what a deletion-vector
/// refusal names, since its `path_or_inline_dv` is opaque.
fn applicable_delete_mechanism<'a>(
    delete: &'a DeleteMechanism,
    data_file_path: &str,
    table_root: &str,
    secrets: &[String],
) -> Result<ApplicableDelete<'a>, UdfError> {
    let (path, mechanism) = match delete {
        DeleteMechanism::IcebergPositionalDelete { path, size } => {
            return Ok(ApplicableDelete::PositionalDeleteFile {
                path: path.as_str(),
                size: *size,
            });
        }
        DeleteMechanism::DeltaDeletionVector {
            storage,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        } => {
            let vector = DeletionVector::resolve(
                LoggedDeletionVector {
                    storage: *storage,
                    path_or_inline_dv,
                    offset: *offset,
                    size_in_bytes: *size_in_bytes,
                    cardinality: *cardinality,
                },
                table_root,
                data_file_path,
                secrets,
            )?;
            return Ok(ApplicableDelete::DeletionVector(vector));
        }
        DeleteMechanism::IcebergEqualityDelete { path, .. } => (path, "an Iceberg equality delete"),
        DeleteMechanism::IcebergPuffinDeletionVector { path, .. } => {
            (path, "a Puffin deletion vector")
        }
    };
    let path = redact(path.clone(), secrets);
    Err(UdfError::User(format!(
        "assigned delete file '{path}' is {mechanism}, which this engine cannot apply on read \
         (only Iceberg Parquet positional deletes and Delta deletion vectors are supported); \
         refusing to emit rows for the affected data file"
    )))
}

/// Field-id is authoritative; the spec column names are the fallback.
fn locate_delete_columns(schema: &SchemaRef) -> Result<(usize, usize), UdfError> {
    let by_field_id = |target: i32| {
        schema.fields().iter().position(|f| {
            f.metadata()
                .get(super::PARQUET_FIELD_ID_META_KEY)
                .and_then(|v| v.parse::<i32>().ok())
                == Some(target)
        })
    };
    let by_name = |name: &str| schema.fields().iter().position(|f| f.name() == name);

    let file_path_idx = by_field_id(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)
        .or_else(|| by_name("file_path"))
        .ok_or_else(|| {
            UdfError::User(
                "positional-delete file has no file_path column (field-id 2147483546)".into(),
            )
        })?;
    let pos_idx = by_field_id(FIELD_ID_POSITIONAL_DELETE_POS)
        .or_else(|| by_name("pos"))
        .ok_or_else(|| {
            UdfError::User("positional-delete file has no pos column (field-id 2147483545)".into())
        })?;
    Ok((file_path_idx, pos_idx))
}

/// Range-based: Parquet truncates string statistics (min down, max up), so `[min, max]` is a
/// superset and a byte-wise range test never prunes a possible match. An equality shortcut would
/// wrongly prune truncated bounds bracketing a longer path. Absent or partial stats never prune.
fn delete_row_group_may_match(
    row_group: &RowGroupMetaData,
    file_path_idx: usize,
    assigned: &HashSet<String>,
) -> bool {
    let Some(column) = row_group.columns().get(file_path_idx) else {
        return true;
    };
    let Some(Statistics::ByteArray(stats)) = column.statistics() else {
        return true;
    };
    let (Some(min), Some(max)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
        return true;
    };
    assigned.iter().any(|path| {
        let bytes = path.as_bytes();
        min <= bytes && bytes <= max
    })
}

/// Restricted to `assigned` because one partition-granularity delete file references many data
/// files. Row groups whose `file_path` range cannot overlap are skipped, exploiting Iceberg's
/// required (`file_path`, `pos`) sort. No HEAD is issued.
async fn read_delete_file_positions(
    store: Arc<dyn ObjectStore>,
    delete_meta: ObjectMeta,
    assigned: &HashSet<String>,
    secrets: &[String],
) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
    let reader = ParquetObjectReader::new(store, delete_meta.location.clone())
        .with_file_size(delete_meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| UdfError::User(redact(format!("failed to open delete file: {e}"), secrets)))?;
    let schema = Arc::clone(builder.schema());
    let (file_path_idx, pos_idx) = locate_delete_columns(&schema)?;

    let selected: Vec<usize> = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, row_group)| delete_row_group_may_match(row_group, file_path_idx, assigned))
        .map(|(idx, _)| idx)
        .collect();

    let mut positions_by_data_file: HashMap<String, Vec<u64>> = HashMap::new();

    if !selected.is_empty() {
        let mut stream = builder.with_row_groups(selected).build().map_err(|e| {
            UdfError::User(redact(format!("failed to read delete file: {e}"), secrets))
        })?;

        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| {
                UdfError::User(redact(format!("error decoding delete file: {e}"), secrets))
            })?;
            let file_paths = batch.column(file_path_idx);
            let positions = batch
                .column(pos_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    UdfError::User("positional-delete pos column is not Int64".into())
                })?;

            // Fail loud on any other type: a silent `None` would drop every positional delete.
            let utf8 = file_paths.as_any().downcast_ref::<StringArray>();
            let large_utf8 = file_paths.as_any().downcast_ref::<LargeStringArray>();
            if utf8.is_none() && large_utf8.is_none() {
                return Err(UdfError::User(format!(
                    "positional-delete file_path column has unexpected type {:?} \
                     (expected Utf8 or LargeUtf8)",
                    file_paths.data_type()
                )));
            }
            let path_at = |row: usize| -> Option<&str> {
                if file_paths.is_null(row) {
                    return None;
                }
                match (utf8, large_utf8) {
                    (Some(a), _) => Some(a.value(row)),
                    (_, Some(a)) => Some(a.value(row)),
                    _ => None,
                }
            };

            for row in 0..batch.num_rows() {
                if positions.is_null(row) {
                    continue;
                }
                let Some(path) = path_at(row) else { continue };
                if !assigned.contains(path) {
                    continue;
                }
                let pos = positions.value(row);
                // Casting a negative `pos` to u64 would wrap and silently drop the delete.
                if pos < 0 {
                    return Err(UdfError::User(format!(
                        "positional-delete file has a negative pos ({pos}); refusing to \
                         apply a malformed delete"
                    )));
                }
                // `entry` would allocate `path.to_string()` on every matching row.
                if let Some(bucket) = positions_by_data_file.get_mut(path) {
                    bucket.push(pos as u64);
                } else {
                    positions_by_data_file.insert(path.to_string(), vec![pos as u64]);
                }
            }
        }
    }

    // Positions normally arrive sorted per the Iceberg spec; sort + dedup keeps the bulk
    // `from_sorted_iter` build robust to deviations.
    let mut result: HashMap<String, RoaringTreemap> =
        HashMap::with_capacity(positions_by_data_file.len());
    for (path, mut positions) in positions_by_data_file {
        positions.sort_unstable();
        positions.dedup();
        let treemap = RoaringTreemap::from_sorted_iter(positions).map_err(|e| {
            UdfError::User(format!(
                "failed to build positional-delete set for a data file: {e}"
            ))
        })?;
        result.insert(path, treemap);
    }
    Ok(result)
}

/// Splits the whole-file selection at row-group boundaries; untouched row groups stay `Scan`.
fn build_access_plan(
    row_groups: &[RowGroupMetaData],
    deletes: &RoaringTreemap,
) -> ParquetAccessPlan {
    let mut whole = build_deletes_row_selection(row_groups, &None, deletes);
    let mut plan = ParquetAccessPlan::new_all(row_groups.len());
    for (idx, rg) in row_groups.iter().enumerate() {
        let num_rows = rg.num_rows() as usize;
        let per_row_group = whole.split_off(num_rows);
        if per_row_group.iter().any(|selector| selector.skip) {
            plan.scan_selection(idx, per_row_group);
        }
    }
    plan
}

/// All files go into ONE `FileGroup` (one output partition, no repartition). The plan is built
/// through [`ParquetFormat::create_physical_plan`], which applies THIS provider's Parquet
/// options (making row-filter pushdown per table) and a `CachedParquetFileReaderFactory` over
/// the session [`FileMetadataCache`]. Access-plan construction reads through that same cache
/// with the same [`ParquetFormat::metadata_size_hint`], so a footer parses once. That holds only
/// because access-plan construction is the FIRST footer reader: the adapter always supplies a
/// `logical_schema`, keeping `register_file_list` off the `infer_schema` fallback.
///
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
#[derive(Debug)]
pub(crate) struct PositionalDeleteScanTable {
    object_store_url: ObjectStoreUrl,
    schema: PartitionedScanSchema,
    use_field_id_adapter: bool,
    field_id_resolution: FieldIdResolution,
    files: Vec<FileEntry>,
    table_root: String,
    secrets: Vec<String>,
    format: Arc<ParquetFormat>,
    /// Shared by every provider of one invocation (both join sides included), so the whole
    /// instance stays within one N-permit budget.
    delete_path_read_limiter: Arc<Semaphore>,
}

impl PositionalDeleteScanTable {
    /// `use_field_id_adapter` is false only for legacy specs that fell back to first-file
    /// inference. `field_id_resolution`'s nested member trees also decide this table's Parquet
    /// read options via [`scan_table_parquet_format`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        object_store_url: ObjectStoreUrl,
        schema: PartitionedScanSchema,
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

    /// Every mechanism passes [`applicable_delete_mechanism`] BEFORE any I/O, so an unapplicable
    /// one fails before anything is fetched. The two fan-outs share one limiter; `RoaringTreemap`
    /// union is commutative, so completion order cannot change the result.
    async fn collect_delete_positions(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let assigned: HashSet<String> = self
            .files
            .iter()
            .map(|entry| reconstruct_abs_uri(&entry.path, &self.table_root))
            .collect();

        let mut delete_files: HashMap<&str, u64> = HashMap::new();
        let mut vectors: Vec<(&str, DeletionVector)> = Vec::new();
        for entry in &self.files {
            for delete in &entry.deletes {
                match applicable_delete_mechanism(
                    delete,
                    &entry.path,
                    &self.table_root,
                    &self.secrets,
                )? {
                    ApplicableDelete::PositionalDeleteFile { path, size } => {
                        delete_files.entry(path).or_insert(size);
                    }
                    ApplicableDelete::DeletionVector(vector) => {
                        vectors.push((entry.path.as_str(), vector));
                    }
                }
            }
        }

        if delete_files.is_empty() && vectors.is_empty() {
            return Ok(HashMap::new());
        }

        let (per_delete_file, sidecars) = futures::future::try_join(
            self.read_delete_files(store, delete_files, &assigned),
            self.fetch_deletion_vector_sidecars(store, &vectors),
        )
        .await?;

        let mut merged: HashMap<String, RoaringTreemap> = HashMap::new();
        for map in per_delete_file {
            for (path, positions) in map {
                *merged.entry(path).or_default() |= positions;
            }
        }
        for (data_file, vector) in &vectors {
            let body = vector
                .sidecar_url()
                .and_then(|url| sidecars.get(url))
                .cloned();
            let positions = vector.decode(body, data_file, &self.secrets)?;
            let abs = reconstruct_abs_uri(data_file, &self.table_root);
            *merged.entry(abs).or_default() |= positions;
        }
        Ok(merged)
    }

    /// No HEAD: each [`ObjectMeta`] is built from the spec-supplied size.
    async fn read_delete_files(
        &self,
        store: &Arc<dyn ObjectStore>,
        delete_files: HashMap<&str, u64>,
        assigned: &HashSet<String>,
    ) -> Result<Vec<HashMap<String, RoaringTreemap>>, UdfError> {
        let reads = delete_files.into_iter().map(|(delete_path, delete_size)| {
            let store = Arc::clone(store);
            let limiter = Arc::clone(&self.delete_path_read_limiter);
            let secrets = self.secrets.as_slice();
            let table_root = self.table_root.as_str();
            async move {
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .map_err(|e| UdfError::User(format!("delete-read limiter unavailable: {e}")))?;
                let delete_abs = reconstruct_abs_uri(delete_path, table_root);
                let delete_meta = object_meta_for(&delete_abs, delete_size)?;
                read_delete_file_positions(store, delete_meta, assigned, secrets).await
            }
        });
        try_join_all(reads).await
    }

    /// Fetched WHOLE, not by the descriptor's range: the decoder validates the version byte at
    /// position 0, and one body can then serve every descriptor naming it. No HEAD: descriptors
    /// carry the vector's size, never the sidecar's.
    async fn fetch_deletion_vector_sidecars(
        &self,
        store: &Arc<dyn ObjectStore>,
        vectors: &[(&str, DeletionVector)],
    ) -> Result<HashMap<Url, Bytes>, UdfError> {
        // A failed fetch must name a DATA file; a shared sidecar is named by its first referrer.
        let mut unique: HashMap<&Url, &str> = HashMap::new();
        for (data_file, vector) in vectors {
            if let Some(url) = vector.sidecar_url() {
                unique.entry(url).or_insert(data_file);
            }
        }

        let reads = unique.into_iter().map(|(url, data_file)| {
            let store = Arc::clone(store);
            let limiter = Arc::clone(&self.delete_path_read_limiter);
            let secrets = self.secrets.as_slice();
            async move {
                let unreadable = |e: String| {
                    UdfError::User(redact(
                        format!(
                            "data file '{data_file}': its deletion vector could not be read: {e}"
                        ),
                        secrets,
                    ))
                };
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .map_err(|e| unreadable(format!("delete-read limiter unavailable: {e}")))?;
                let location = ListingTableUrl::parse(url.as_str())
                    .map_err(|e| unreadable(format!("invalid sidecar URL: {e}")))?
                    .prefix()
                    .clone();
                let body = store
                    .get(&location)
                    .await
                    .map_err(|e| unreadable(e.to_string()))?
                    .bytes()
                    .await
                    .map_err(|e| unreadable(e.to_string()))?;
                Ok::<_, UdfError>((url.clone(), body))
            }
        });
        Ok(try_join_all(reads).await?.into_iter().collect())
    }

    /// Partition values convert first, so a spec-content failure precedes every fetch.
    ///
    /// Only delete-carrying entries take a limiter permit, fetching their own footer through the
    /// shared session [`FileMetadataCache`] with the opener's size hint and page index skipped
    /// ([`build_access_plan`] needs only row counts), so the footer parses once for both. Each
    /// fetched footer is recorded via [`diagnostics::record_access_plan_cached_footer`] so a
    /// cache eviction is observable.
    ///
    /// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
    async fn partitioned_files(
        &self,
        state: &dyn Session,
    ) -> Result<Vec<PartitionedFile>, UdfError> {
        let partition_values = self
            .files
            .iter()
            .map(|entry| self.schema.partition_values_for(entry))
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

        let delete_positions = self.collect_delete_positions(&store).await?;

        let size_hint = self.format.metadata_size_hint();
        let delete_positions_ref = &delete_positions;
        let builds = self.files.iter().zip(partition_values).map(|(entry, values)| {
            let store = Arc::clone(&store);
            let metadata_cache = Arc::clone(&metadata_cache);
            let limiter = Arc::clone(&self.delete_path_read_limiter);
            let secrets = self.secrets.as_slice();
            let table_root = self.table_root.as_str();
            async move {
                let abs = reconstruct_abs_uri(&entry.path, table_root);
                let meta = object_meta_for(&abs, entry.size)?;
                let partitioned = PartitionedFile::from(meta.clone()).with_partition_values(values);

                let Some(deletes) = delete_positions_ref
                    .get(abs.as_str())
                    .filter(|positions| !positions.is_empty())
                else {
                    return Ok(partitioned);
                };

                let permit = limiter.acquire_owned().await.map_err(|e| {
                    UdfError::User(redact(
                        format!(
                            "delete_path_read_limiter permit unavailable for data-file footer fetch of {abs}: {e}"
                        ),
                        secrets,
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
                            secrets,
                        ))
                    })?;
                diagnostics::record_access_plan_cached_footer(&meta.location);
                drop(permit);

                let access_plan = build_access_plan(parquet_metadata.row_groups(), deletes);
                Ok(partitioned.with_extension(access_plan))
            }
        });
        try_join_all(builds).await
    }
}

#[async_trait]
impl TableProvider for PositionalDeleteScanTable {
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

        let file_source = self
            .format
            .file_source(self.schema.file_source_schema().clone());

        let expr_adapter = self.use_field_id_adapter.then(|| {
            Arc::new(FieldIdExprAdapterFactory {
                resolution: self.field_id_resolution.clone(),
            }) as Arc<_>
        });

        // One file group ⇒ one output partition; with `target_partitions = 1` no repartition.
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file_group(FileGroup::new(files))
            .with_projection_indices(self.schema.remap_projection(projection))?
            .with_limit(limit)
            .with_expr_adapter(expr_adapter)
            .build();

        self.format.create_physical_plan(state, config).await
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
#[path = "positional_deletes_tests.rs"]
mod tests;
