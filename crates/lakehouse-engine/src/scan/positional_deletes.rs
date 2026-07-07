//! Scan-side application of Iceberg merge-on-read **Parquet positional deletes**.
//!
//! The adapter resolves each data file's associated positional-delete files once
//! per query and carries them in the per-shard files argument (see
//! [`crate::scan::spec::FileEntry`]). At read time this module:
//!
//! 1. reads each associated positional-delete Parquet file (`file_path` / `pos`
//!    columns, Iceberg reserved field-ids 2147483546 / 2147483545), filters its
//!    rows to the data file being read (required for `partition` granularity,
//!    where one delete file references many data files), and unions the `pos`
//!    values into a per-data-file [`RoaringTreemap`];
//! 2. converts that set plus the data file's per-row-group row counts into a
//!    per-row-group [`RowSelection`] via [`build_deletes_row_selection`];
//! 3. attaches it as a base [`ParquetAccessPlan`] on the data file's
//!    `PartitionedFile`, so DataFusion's Parquet opener reads it as the base
//!    plan and intersects predicate / row-group / page pruning ON TOP — deletes
//!    compose with pushdown rather than defeating it.
//!
//! The scan engine stays DataFusion's own `ParquetSource`; this module only adds
//! a base access plan and a thin custom `TableProvider` around it (replacing the
//! previous `ListingTable`), preserving projection/filter/LIMIT pushdown,
//! row-group + page pruning, statistics, streaming, and the existing
//! `FieldIdExprAdapter`.

use crate::scan::spec::{DeleteFileContentType, DeleteFileRef, FileEntry, StorageProps};
use crate::scan::{FieldIdExprAdapterFactory, reconstruct_abs_uri};
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
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
use datafusion::datasource::table_schema::TableSchema;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::RowGroupMetaData;
use roaring::RoaringTreemap;
use std::sync::Arc;

/// Iceberg reserved field-id for the `file_path` column of a positional-delete file.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
/// Iceberg reserved field-id for the `pos` column of a positional-delete file.
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// Compute a whole-file [`RowSelection`] that rejects the deleted rows.
///
/// Given the data file's per-row-group metadata and an ascending set of deleted
/// row positions (file-global 0-based indices), produce a `RowSelection` that
/// `skip`s the deleted rows and `select`s the rest, honoring row-group
/// boundaries and any `selected_row_groups` restriction (row groups already
/// pruned away, whose deletes must be stepped over without disturbing the
/// deletes that belong to the surviving row groups).
///
/// # Attribution
///
/// Vendored from apache/iceberg-rust
/// (`crates/iceberg/src/arrow/reader/positional_deletes.rs::build_deletes_row_selection`,
/// tag `v0.10.0-rc.2`), where it is `pub(super)` on `ArrowReader` and therefore
/// not importable. Kept algorithmically identical to reuse its verified
/// row-group-boundary handling (including the multi-row-group and skipped-row-group
/// bug fixes upstream added), differing only in taking the delete set as a
/// [`RoaringTreemap`] directly rather than iceberg's private `DeleteVector`
/// wrapper — roaring 0.11's `RoaringTreemap::iter()` already exposes the
/// `advance_to` the wrapper existed to provide.
///
/// Upstream tracking: apache/iceberg-rust #340 (a native Rust position-delete
/// writer). Once iceberg-rust exposes this routine (or a position-delete writer
/// lets us drop the Spark fixtures), this vendored copy can be reconsidered.
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

        // if row group selection is enabled,
        if let Some(selected_row_groups) = selected_row_groups {
            // if we've consumed all the selected row groups, we're done
            if selected_row_groups_idx == selected_row_groups.len() {
                break;
            }

            if idx == selected_row_groups[selected_row_groups_idx] {
                // we're in a selected row group. Increment selected_row_groups_idx
                // so that next time around the for loop we're looking for the next
                // selected row group
                selected_row_groups_idx += 1;
            } else {
                // Advance iterator past all deletes in the skipped row group.
                // advance_to() positions the iterator to the first delete >= next_row_group_base_idx.
                // However, if our cached next_deleted_row_idx_opt is in the skipped range,
                // we need to call next() to update the cache with the newly positioned value.
                delete_vector_iter.advance_to(next_row_group_base_idx);
                // Only update the cache if the cached value is stale (in the skipped range)
                if let Some(cached_idx) = next_deleted_row_idx_opt
                    && cached_idx < next_row_group_base_idx
                {
                    next_deleted_row_idx_opt = delete_vector_iter.next();
                }

                // still increment the current page base index but then skip to the next row group
                // in the file
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        }

        let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
            Some(next_deleted_row_idx) => {
                // if the index of the next deleted row is beyond this row group, add a selection for
                // the remainder of this row group and skip to the next row group
                if next_deleted_row_idx >= next_row_group_base_idx {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }

                next_deleted_row_idx
            }

            // If there are no more pos deletes, add a selector for the entirety of this row group.
            _ => {
                results.push(RowSelector::select(row_group_num_rows as usize));
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        };

        let mut current_idx = current_row_group_base_idx;
        'chunks: while next_deleted_row_idx < next_row_group_base_idx {
            // `select` all rows that precede the next delete index
            if current_idx < next_deleted_row_idx {
                let run_length = next_deleted_row_idx - current_idx;
                results.push(RowSelector::select(run_length as usize));
                current_idx += run_length;
            }

            // `skip` all consecutive deleted rows in the current row group
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
                        // We've processed the final positional delete.
                        // Conclude the skip and then break so that we select the remaining
                        // rows in the row group and move on to the next row group
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

/// Redact any credential fragments from an error string before surfacing it.
fn redact(msg: String, secrets: &[String]) -> String {
    let borrowed: Vec<&str> = secrets.iter().map(String::as_str).collect();
    let stripped = crate::scan::emit::redact_secret_values(&msg, &borrowed);
    crate::scan::emit::redact_credentials(&stripped)
}

/// The object-store [`ObjectMeta`] for an absolute file URI and its known byte
/// size, keyed by the same `Path` the store observes — built without any
/// object-store HEAD (the size is supplied by the caller).
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

/// Read-time backstop (task 2.6): reject any assigned delete file this engine
/// cannot apply as a Parquet positional delete, with a clean,
/// credential-redacted error, BEFORE any row of the affected data file is
/// emitted. The plan-time gate (adapter) is the authoritative filter; this is
/// cheap defense-in-depth against a non-positional delete slipping through.
fn ensure_positional_delete(delete: &DeleteFileRef, secrets: &[String]) -> Result<(), UdfError> {
    if delete.content_type == DeleteFileContentType::PositionDeletes {
        return Ok(());
    }
    let mechanism = match delete.content_type {
        DeleteFileContentType::PositionDeletes => unreachable!(),
        DeleteFileContentType::EqualityDeletes => "an Iceberg equality delete",
        DeleteFileContentType::PuffinDeletionVector => "a Puffin deletion vector",
    };
    let path = redact(delete.path.clone(), secrets);
    Err(UdfError::User(format!(
        "assigned delete file '{path}' is {mechanism}, which this engine cannot apply on read \
         (only Parquet positional deletes are supported); refusing to emit rows for the affected \
         data file"
    )))
}

/// Locate the `file_path` and `pos` columns of a positional-delete file by
/// Iceberg reserved field-id (authoritative), falling back to the spec column
/// names. Returns `(file_path_idx, pos_idx)`.
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

/// Read one positional-delete Parquet file (task 2.3), keep only the rows whose
/// `file_path` equals `data_file_abs` (required for `partition` granularity,
/// where one delete file references many data files), and union its `pos`
/// values into `out`. Streams row groups; never issues an object-store HEAD
/// (the file size is supplied). Unioning across multiple delete files is the
/// caller's responsibility (it reuses the same `out`).
async fn union_delete_positions(
    store: Arc<dyn ObjectStore>,
    delete_meta: ObjectMeta,
    data_file_abs: &str,
    out: &mut RoaringTreemap,
    secrets: &[String],
) -> Result<(), UdfError> {
    let reader = ParquetObjectReader::new(store, delete_meta.location.clone())
        .with_file_size(delete_meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| UdfError::User(redact(format!("failed to open delete file: {e}"), secrets)))?;
    let schema = Arc::clone(builder.schema());
    let (file_path_idx, pos_idx) = locate_delete_columns(&schema)?;

    let mut stream = builder
        .build()
        .map_err(|e| UdfError::User(redact(format!("failed to read delete file: {e}"), secrets)))?;

    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| {
            UdfError::User(redact(format!("error decoding delete file: {e}"), secrets))
        })?;
        let file_paths = batch.column(file_path_idx);
        let positions = batch
            .column(pos_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| UdfError::User("positional-delete pos column is not Int64".into()))?;

        // Downcast the `file_path` column once per batch (tolerating `Utf8`/`LargeUtf8`)
        // and borrow each cell in place — no per-row downcast or allocation. Fail
        // loud on any other type: a silent `None`-for-every-row fallback would drop
        // ALL positional deletes without error — exactly the silent-correctness
        // failure mode this feature exists to eliminate.
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
            if path_at(row) == Some(data_file_abs) {
                let pos = positions.value(row);
                // A negative `pos` is malformed: casting it to u64 would wrap to a
                // huge index and silently drop the delete. Reject it loudly rather
                // than emit a row Iceberg intended to delete.
                if pos < 0 {
                    return Err(UdfError::User(format!(
                        "positional-delete file has a negative pos ({pos}); refusing to \
                         apply a malformed delete"
                    )));
                }
                out.insert(pos as u64);
            }
        }
    }
    Ok(())
}

/// Build a base [`ParquetAccessPlan`] for a delete-carrying data file (task 2.4).
///
/// Converts the whole-file [`RowSelection`] (from [`build_deletes_row_selection`],
/// with no row-group restriction — the opener applies pruning ON TOP) into a
/// per-row-group `Selection` on the access plan by splitting it at row-group
/// boundaries. Row groups the deletes do not touch stay `Scan`. The opener seeds
/// its `RowGroupAccessPlanFilter` with this plan and intersects predicate /
/// row-group / page pruning on top, so deletes compose with pushdown.
fn build_access_plan(
    row_groups: &[RowGroupMetaData],
    deletes: &RoaringTreemap,
) -> ParquetAccessPlan {
    let mut whole = build_deletes_row_selection(row_groups, &None, deletes);
    let mut plan = ParquetAccessPlan::new_all(row_groups.len());
    for (idx, rg) in row_groups.iter().enumerate() {
        let num_rows = rg.num_rows() as usize;
        let per_row_group = whole.split_off(num_rows);
        // Only attach a Selection when this row group actually loses rows; an
        // all-select row group is left as `Scan` (equivalent, and lets the
        // opener treat it as a plain full scan).
        if per_row_group.iter().any(|selector| selector.skip) {
            plan.scan_selection(idx, per_row_group);
        }
    }
    plan
}

/// Resolve the deleted-row set for one data file and turn it into a base
/// [`ParquetAccessPlan`], or `None` when the file's deletes remove no rows.
///
/// Fetches the data file's Parquet metadata through the shared session
/// [`FileMetadataCache`] (task 2.5): the same cache the opener's
/// `CachedParquetFileReaderFactory` reads, so the footer parses ONCE for both
/// access-plan construction here and the scan. Never issues an object-store HEAD.
///
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
async fn access_plan_for_data_file(
    entry: &FileEntry,
    data_file_abs: &str,
    data_file_meta: &ObjectMeta,
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadataCache>,
    table_root: &str,
    secrets: &[String],
) -> Result<Option<ParquetAccessPlan>, UdfError> {
    let mut deletes = RoaringTreemap::new();
    for delete in &entry.deletes {
        ensure_positional_delete(delete, secrets)?;
        let delete_abs = reconstruct_abs_uri(&delete.path, table_root);
        let delete_meta = object_meta_for(&delete_abs, delete.size)?;
        union_delete_positions(
            Arc::clone(&store),
            delete_meta,
            data_file_abs,
            &mut deletes,
            secrets,
        )
        .await?;
    }

    if deletes.is_empty() {
        // Deletes reference no row of THIS data file (e.g. a partition-granularity
        // delete file that only touches sibling files): read it as-is.
        return Ok(None);
    }

    let parquet_metadata = DFParquetMetadata::new(store.as_ref(), data_file_meta)
        .with_file_metadata_cache(Some(metadata_cache))
        .with_metadata_size_hint(None)
        .fetch_metadata()
        .await
        .map_err(|e| {
            UdfError::User(redact(
                format!("failed to read data-file metadata for delete application: {e}"),
                secrets,
            ))
        })?;

    Ok(Some(build_access_plan(
        parquet_metadata.row_groups(),
        &deletes,
    )))
}

/// Custom [`TableProvider`] over DataFusion's `ParquetSource` (task 2.1),
/// replacing the previous `ListingTable`.
///
/// It registers ONLY the assigned files (no catalog discovery), builds a
/// [`FileScanConfig`] directly so each delete-carrying data file can carry a
/// base [`ParquetAccessPlan`] on its `PartitionedFile` extensions, and preserves
/// exactly: the logical schema, the [`FieldIdExprAdapterFactory`], and the lean
/// single-partition plan (all files in ONE `FileGroup` ⇒ one output partition,
/// no repartition/coalesce). Delete-free files take the identical path with no
/// access plan attached, so the change is unified across all scans.
///
/// The physical plan is produced through [`ParquetFormat::create_physical_plan`]
/// — the same seam `ListingTable` uses — which applies the session's Parquet
/// options and installs a `CachedParquetFileReaderFactory` backed by the session
/// [`FileMetadataCache`]; access-plan construction reads through that SAME cache,
/// so a delete-carrying file's footer parses once (task 2.5).
///
/// [`FileScanConfig`]: datafusion::datasource::physical_plan::FileScanConfig
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
#[derive(Debug)]
pub(crate) struct PositionalDeleteScanTable {
    object_store_url: ObjectStoreUrl,
    table_schema: SchemaRef,
    use_field_id_adapter: bool,
    files: Vec<FileEntry>,
    table_root: String,
    secrets: Vec<String>,
    format: Arc<ParquetFormat>,
}

impl PositionalDeleteScanTable {
    /// Construct the provider from the resolved logical Arrow schema and the
    /// per-shard file list. `use_field_id_adapter` mirrors the previous
    /// `register_files` behavior: the [`FieldIdExprAdapterFactory`] is attached
    /// only when the adapter supplied a logical schema (field-id binding);
    /// legacy specs that fell back to first-file inference bind by name.
    pub(crate) fn new(
        object_store_url: ObjectStoreUrl,
        table_schema: SchemaRef,
        use_field_id_adapter: bool,
        files: Vec<FileEntry>,
        table_root: String,
        storage: &StorageProps,
    ) -> Self {
        let secrets = storage
            .secret_values()
            .iter()
            .map(|s| s.to_string())
            .collect();
        Self {
            object_store_url,
            table_schema,
            use_field_id_adapter,
            files,
            table_root,
            secrets,
            format: Arc::new(ParquetFormat::default()),
        }
    }

    /// Build one `PartitionedFile` per assigned data file, attaching a base
    /// `ParquetAccessPlan` (task 2.4) to each delete-carrying file.
    async fn partitioned_files(
        &self,
        state: &dyn Session,
    ) -> Result<Vec<PartitionedFile>, UdfError> {
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

        let mut files = Vec::with_capacity(self.files.len());
        for entry in &self.files {
            let abs = reconstruct_abs_uri(&entry.path, &self.table_root);
            let meta = object_meta_for(&abs, entry.size)?;
            let mut partitioned = PartitionedFile::from(meta.clone());

            if !entry.deletes.is_empty()
                && let Some(access_plan) = access_plan_for_data_file(
                    entry,
                    &abs,
                    &meta,
                    Arc::clone(&store),
                    Arc::clone(&metadata_cache),
                    &self.table_root,
                    &self.secrets,
                )
                .await?
            {
                partitioned = partitioned.with_extension(access_plan);
            }

            files.push(partitioned);
        }
        Ok(files)
    }
}

#[async_trait]
impl TableProvider for PositionalDeleteScanTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
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

        let table_schema = TableSchema::from_file_schema(Arc::clone(&self.table_schema));
        let file_source = self.format.file_source(table_schema);

        let expr_adapter = self
            .use_field_id_adapter
            .then(|| Arc::new(FieldIdExprAdapterFactory) as Arc<_>);

        // All assigned files go into ONE file group ⇒ one output partition. With
        // `target_partitions = 1` (the scan default) the plan stays lean: no
        // repartition/coalesce is inserted.
        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
            .with_file_group(FileGroup::new(files))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .with_expr_adapter(expr_adapter)
            .build();

        self.format.create_physical_plan(state, config).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Mirror `ListingTable`'s non-partition behavior: the filter is pushed to
        // the Parquet scan for row-group/page pruning, but DataFusion keeps a
        // `FilterExec` above the scan (Inexact) so correctness never depends on
        // the scan fully applying it.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use std::sync::Arc;

    fn build_test_row_group_meta(
        schema_descr: SchemaDescPtr,
        columns: Vec<ColumnChunkMetaData>,
        num_rows: i64,
        ordinal: i16,
    ) -> RowGroupMetaData {
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(num_rows)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(ordinal)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::schema::types::Type as SchemaType;

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    /// Vendored verbatim from apache/iceberg-rust's own unit test for
    /// `build_deletes_row_selection` (tag `v0.10.0-rc.2`), adapted only to feed a
    /// `RoaringTreemap` directly. It is the correctness oracle for the vendored
    /// row-group-boundary algorithm: it exercises skip/select runs at the first,
    /// intermediate, and last positions of skipped and selected row groups, both
    /// with row-group selection enabled (`Some([1, 3])`) and disabled (`None`).
    #[test]
    fn build_deletes_row_selection_matches_upstream() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }

        let row_groups_metadata = vec![
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 0),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 1),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 2),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 3),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 4),
        ];

        let selected_row_groups = Some(vec![1, 3]);

        let positional_deletes = RoaringTreemap::from_iter(&[
            1, 3, 4, 5, 998, 999, 1000, 1010, 1011, 1012, 1498, 1499, 1500, 1501, 1600, 1999, 2000,
            2001, 2100, 2200, 2201, 2202, 2999, 3000,
        ]);

        // using selected row groups 1 and 3
        let result = build_deletes_row_selection(
            &row_groups_metadata,
            &selected_row_groups,
            &positional_deletes,
        );

        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(1),
        ]);

        assert_eq!(result, expected);

        // selecting all row groups
        let result = build_deletes_row_selection(&row_groups_metadata, &None, &positional_deletes);

        let expected = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(3),
            RowSelector::select(992),
            RowSelector::skip(3),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(398),
            RowSelector::skip(3),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(2),
            RowSelector::select(499),
        ]);

        assert_eq!(result, expected);
    }

    /// Task 2.4: the base `ParquetAccessPlan` built from the whole-file
    /// `RowSelection` recombines (via `into_overall_row_selection`) to EXACTLY
    /// the whole-file selection — i.e. splitting per row group then letting the
    /// opener recombine is lossless. Row groups the deletes don't touch stay
    /// `Scan`; touched row groups become `Selection`.
    #[test]
    fn access_plan_round_trips_to_whole_file_selection() {
        use datafusion::datasource::physical_plan::parquet::RowGroupAccess;

        let schema_descr = get_test_schema_descr();
        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            columns.push(ColumnChunkMetaData::builder(ptr.clone()).build().unwrap());
        }
        let metas = vec![
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 0),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 1),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 2),
        ];

        // Deletes touch row group 0 (pos 0, 999) and row group 2 (pos 1500), but
        // NOT row group 1 (rows 1000..1500).
        let deletes = RoaringTreemap::from_iter([0u64, 999, 1500]);

        let plan = build_access_plan(&metas, &deletes);

        // Row group 1 is untouched ⇒ left as Scan; 0 and 2 carry a Selection.
        assert!(matches!(plan.inner()[0], RowGroupAccess::Selection(_)));
        assert!(matches!(plan.inner()[1], RowGroupAccess::Scan));
        assert!(matches!(plan.inner()[2], RowGroupAccess::Selection(_)));

        // Recombining the per-row-group plan yields the whole-file selection.
        let whole = build_deletes_row_selection(&metas, &None, &deletes);
        let recombined = plan
            .into_overall_row_selection(&metas)
            .unwrap()
            .expect("a plan with Selections yields an overall selection");
        assert_eq!(recombined, whole);
    }

    /// Task 2.4: a fully-deleted file — every row of every row group deleted —
    /// yields an access plan whose overall selection skips all rows.
    #[test]
    fn access_plan_fully_deleted_file_selects_no_rows() {
        let schema_descr = get_test_schema_descr();
        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            columns.push(ColumnChunkMetaData::builder(ptr.clone()).build().unwrap());
        }
        let metas = vec![build_test_row_group_meta(schema_descr, columns, 4, 0)];
        let deletes = RoaringTreemap::from_iter([0u64, 1, 2, 3]);

        let plan = build_access_plan(&metas, &deletes);
        let recombined = plan.into_overall_row_selection(&metas).unwrap().unwrap();
        assert_eq!(
            recombined.row_count(),
            0,
            "no rows survive a fully-deleted file"
        );
    }

    /// Task 2.3: a positional-delete file whose `file_path` column references
    /// TWO data files (the `partition` granularity shape) is filtered to only
    /// the data file being read; only its `pos` values are unioned. Columns are
    /// located by Iceberg reserved field-id, and the read never issues a HEAD
    /// (the size is supplied on the `ObjectMeta`).
    #[test]
    fn reads_and_filters_delete_positions_by_file_path() {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use object_store::memory::InMemory;
        use object_store::path::Path as StorePath;
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;

        let field_id_meta = |id: i32| {
            HashMap::from([(
                super::super::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false)
                .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)),
            Field::new("pos", DataType::Int64, false)
                .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_POS)),
        ]));

        let target = "s3://bucket/db/t/data/f1.parquet";
        let other = "s3://bucket/db/t/data/f2.parquet";
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![target, target, other, target])),
                Arc::new(Int64Array::from(vec![3_i64, 7, 5, 9])),
            ],
        )
        .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let deletes = rt.block_on(async move {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let location = StorePath::from("db/t/deletes/d.parquet");
            let size = buf.len() as u64;
            store.put(&location, PutPayload::from(buf)).await.unwrap();
            let meta = ObjectMeta {
                location,
                last_modified: chrono::Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            };
            let mut out = RoaringTreemap::new();
            union_delete_positions(store, meta, target, &mut out, &[])
                .await
                .unwrap();
            out
        });

        // Only f1's positions (3, 7, 9) are unioned; f2's position (5) is filtered out.
        assert_eq!(deletes.iter().collect::<Vec<u64>>(), vec![3, 7, 9]);
    }

    /// Task 2.6: the read-time backstop rejects a non-positional delete file with
    /// a clean error that names the mechanism and does not leak credentials.
    #[test]
    fn backstop_rejects_equality_delete() {
        let delete = DeleteFileRef {
            path: "s3://bucket/db/t/data/eq-delete.parquet".to_string(),
            size: 10,
            content_type: DeleteFileContentType::EqualityDeletes,
        };
        let err = ensure_positional_delete(&delete, &["SECRETKEY".to_string()])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("equality delete"),
            "names the mechanism: {err}"
        );
        assert!(
            !err.contains("SECRETKEY"),
            "must not leak credentials: {err}"
        );

        let ok = DeleteFileRef {
            path: "s3://bucket/db/t/data/pos-delete.parquet".to_string(),
            size: 10,
            content_type: DeleteFileContentType::PositionDeletes,
        };
        assert!(ensure_positional_delete(&ok, &[]).is_ok());
    }
}
