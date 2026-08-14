//! Scan-side application of Iceberg merge-on-read **Parquet positional deletes**.
//!
//! The adapter resolves each data file's associated positional-delete files once
//! per query and carries them in the per-shard files argument (see
//! [`crate::scan::spec::FileEntry`]). At read time this module runs a two-phase
//! pipeline in [`PositionalDeleteScanTable::partitioned_files`]:
//!
//! 1. **Phase A** reads each of the shard's UNIQUE positional-delete Parquet
//!    files exactly once (`file_path` / `pos` columns, Iceberg reserved
//!    field-ids 2147483546 / 2147483545), concurrently within a shared
//!    connection budget, buckets each surviving `pos` value under its
//!    `file_path` (restricted to the assigned data files — the shape required
//!    for `partition` granularity, where one delete file references many data
//!    files), and unions across delete files into a merged
//!    `HashMap<data_file_path, `[`RoaringTreemap`]`>`;
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

use crate::scan::diagnostics;
use crate::scan::spec::{DeleteFileContentType, DeleteFileRef, FileEntry, StorageBackend};
use crate::scan::{
    FieldIdExprAdapterFactory, FieldIdResolution, int96_coerced_parquet_format, reconstruct_abs_uri,
};
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
use futures::future::try_join_all;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::{PageIndexPolicy, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;

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
/// tag `v0.10.0`), where it is `pub(super)` on `ArrowReader` and therefore
/// not importable. Kept algorithmically identical to reuse its verified
/// row-group-boundary handling (including the multi-row-group and skipped-row-group
/// bug fixes upstream added), differing only in taking the delete set as a
/// [`RoaringTreemap`] directly rather than iceberg's private `DeleteVector`
/// wrapper — roaring 0.11's `RoaringTreemap::iter()` already exposes the
/// `advance_to` the wrapper existed to provide.
///
/// Upstream tracking: #344. Once iceberg-rust exposes this routine publicly,
/// this vendored copy can be reconsidered.
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

/// Whether a delete-file row group can hold deletes for any assigned data file,
/// judged from its `file_path` column min/max statistics.
///
/// Pruning is RANGE-based: a row group is skipped ONLY when every assigned path
/// sorts strictly outside the `[min, max]` byte range (before `min` or after
/// `max`). Parquet truncates string statistics — min DOWN and max UP — so the
/// stored `[min, max]` is a superset of the true range; a byte-wise range test
/// therefore stays valid on truncated bounds and never prunes a row group that
/// could contain a match. An equality shortcut (`min == max == target`) is NOT
/// used: it would wrongly prune a row group whose truncated bounds bracket a
/// longer real path. A row group whose `file_path` statistics are absent,
/// partial (min or max unset), or not a byte-array is never pruned — overlap
/// cannot be ruled out.
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

/// Read one positional-delete Parquet file exactly once (Phase A), bucketing
/// each surviving `pos` value under its `file_path` — restricted to `assigned`,
/// the set of this shard's data-file absolute paths (required for `partition`
/// granularity, where one delete file references many data files, only some of
/// which this shard reads). Returns a per-delete-file
/// `HashMap<data_file_path, `[`RoaringTreemap`]`>`; the caller unions these
/// across delete files.
///
/// Delete-file row groups whose `file_path` min/max statistics cannot overlap
/// any assigned data-file path are pruned via [`delete_row_group_may_match`], so
/// only the surviving row groups' data pages are decoded (exploiting Iceberg's
/// required (`file_path`, `pos`) sort). Each data file's set is bulk-built from
/// its collected positions rather than one insert per row. Never issues an
/// object-store HEAD (the file size is supplied).
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

    // Keep only the row groups whose `file_path` range can overlap an assigned
    // data file; the rest are skipped so their data pages are never fetched.
    let selected: Vec<usize> = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, row_group)| delete_row_group_may_match(row_group, file_path_idx, assigned))
        .map(|(idx, _)| idx)
        .collect();

    // Collect matching positions per data file, then bulk-build each set below.
    let mut positions_by_data_file: HashMap<String, Vec<u64>> = HashMap::new();

    // When every row group is pruned there is nothing to decode; skip building
    // the stream so no data pages are read at all.
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
                let Some(path) = path_at(row) else { continue };
                // Only bucket deletes for data files this shard is reading; a
                // partition-granularity delete file referencing sibling files
                // contributes nothing here.
                if !assigned.contains(path) {
                    continue;
                }
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
                // Probe with the borrowed `&str` and only allocate an owned key
                // when the bucket is new. `entry` would force `path.to_string()`
                // on every matching row — one heap allocation per deleted
                // position in the dominant single-data-file case.
                if let Some(bucket) = positions_by_data_file.get_mut(path) {
                    bucket.push(pos as u64);
                } else {
                    positions_by_data_file.insert(path.to_string(), vec![pos as u64]);
                }
            }
        }
    }

    // Bulk-build each data file's set. The Iceberg spec sorts a delete file by
    // (`file_path`, `pos`), so per data file the positions arrive ascending;
    // sort + dedup makes the bulk build robust to any deviation without changing
    // the resulting set (`RoaringTreemap` is a set, so order and duplicates do
    // not affect the outcome — only the efficient sorted build path).
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
/// [`FileMetadataCache`]; access-plan construction reads through that SAME cache
/// with the SAME metadata size hint, so a delete-carrying file's footer parses
/// ONCE for both access-plan construction and the scan. The hint has exactly one
/// owner — [`ParquetFormat::metadata_size_hint`] on the `format` this provider
/// already holds — read back rather than duplicated as a second constant, so the
/// two readers cannot disagree on request shape. The once-per-footer property
/// holds on the production path only because access-plan construction is the
/// FIRST reader of the footer: the adapter always supplies a non-empty
/// `logical_schema`, which keeps `register_file_list` off the
/// `ParquetFormat::infer_schema` fallback that would otherwise populate the
/// cache first, under a request shape the hinted fetch does not match.
///
/// [`FileScanConfig`]: datafusion::datasource::physical_plan::FileScanConfig
/// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
#[derive(Debug)]
pub(crate) struct PositionalDeleteScanTable {
    object_store_url: ObjectStoreUrl,
    table_schema: SchemaRef,
    use_field_id_adapter: bool,
    field_id_resolution: FieldIdResolution,
    files: Vec<FileEntry>,
    table_root: String,
    secrets: Vec<String>,
    format: Arc<ParquetFormat>,
    /// Shared instance-level bound on every object-store read the delete path
    /// issues while preparing a scan — Phase A delete-file bodies and Phase B
    /// data-file footers alike, one permit per read — sized `s3_max_connections`
    /// and constructed once per scan invocation. Every provider registered for
    /// the same invocation (including both sides of a broadcast join) holds a
    /// clone of the SAME `Arc`, so the whole instance stays within one
    /// N-permit budget rather than each provider getting its own N.
    delete_path_read_limiter: Arc<Semaphore>,
}

impl PositionalDeleteScanTable {
    /// Construct the provider from the resolved logical Arrow schema and the
    /// per-shard file list. `use_field_id_adapter` mirrors the previous
    /// `register_files` behavior: the [`FieldIdExprAdapterFactory`] is attached
    /// only when the adapter supplied a logical schema (field-id binding);
    /// legacy specs that fell back to first-file inference bind by name.
    /// `field_id_resolution` groups the query's flattened
    /// `schema.name-mapping.default` entries for this side (fact or
    /// dimension) together with the reconstructed Iceberg `initial-default`
    /// values keyed by field-id — both resolved once in the VS alongside the
    /// logical schema, and empty when the table has neither. It is carried
    /// through unchanged to the [`FieldIdExprAdapterFactory`] installed in
    /// [`Self::scan`] for name-mapping resolution and the absent-with-default
    /// fill respectively. `delete_path_read_limiter` is the shared
    /// instance-level semaphore bounding every object-store read the delete
    /// path issues while preparing a scan — Phase A delete-file bodies and
    /// Phase B data-file footers alike, one permit per read — sized
    /// `s3_max_connections`, constructed once per scan invocation and shared
    /// across every registered provider — see the struct-level doc comment).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        object_store_url: ObjectStoreUrl,
        table_schema: SchemaRef,
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
        Self {
            object_store_url,
            table_schema,
            use_field_id_adapter,
            field_id_resolution,
            files,
            table_root,
            secrets,
            format: Arc::new(int96_coerced_parquet_format()),
            delete_path_read_limiter,
        }
    }

    /// Phase A: read each of the shard's UNIQUE positional-delete files exactly
    /// once, concurrently within the shared [`Self::delete_path_read_limiter`] budget,
    /// and merge the surviving positions into a
    /// `HashMap<data_file_path, `[`RoaringTreemap`]`>` keyed by the data-file
    /// absolute path each delete row targets.
    ///
    /// Every unique delete file is backstop-validated with
    /// [`ensure_positional_delete`] BEFORE any I/O, so an unapplicable delete
    /// anywhere in the shard fails loud before a single row is read. Each read
    /// holds a permit from the shared limiter, so the whole instance — both
    /// sides of a broadcast join included — stays within one connection budget.
    /// The per-delete-file maps are unioned; [`RoaringTreemap`] union is
    /// commutative and associative, so the non-deterministic completion order of
    /// the concurrent reads cannot change the result.
    ///
    /// No object-store HEAD is issued: each delete file's [`ObjectMeta`] is built
    /// from its spec-supplied [`DeleteFileRef::size`] via [`object_meta_for`].
    ///
    /// [`DeleteFileRef::size`]: crate::scan::spec::DeleteFileRef::size
    async fn collect_delete_positions(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let assigned: HashSet<String> = self
            .files
            .iter()
            .map(|entry| reconstruct_abs_uri(&entry.path, &self.table_root))
            .collect();

        let mut unique_deletes: HashMap<&str, &DeleteFileRef> = HashMap::new();
        for entry in &self.files {
            for delete in &entry.deletes {
                ensure_positional_delete(delete, &self.secrets)?;
                unique_deletes.entry(delete.path.as_str()).or_insert(delete);
            }
        }

        if unique_deletes.is_empty() {
            return Ok(HashMap::new());
        }

        let assigned_ref = &assigned;
        let reads = unique_deletes.into_values().map(|delete| {
            let store = Arc::clone(store);
            let limiter = Arc::clone(&self.delete_path_read_limiter);
            let secrets = self.secrets.as_slice();
            let table_root = self.table_root.as_str();
            async move {
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .map_err(|e| UdfError::User(format!("delete-read limiter unavailable: {e}")))?;
                let delete_abs = reconstruct_abs_uri(&delete.path, table_root);
                let delete_meta = object_meta_for(&delete_abs, delete.size)?;
                read_delete_file_positions(store, delete_meta, assigned_ref, secrets).await
            }
        });
        let per_file_maps = try_join_all(reads).await?;

        let mut merged: HashMap<String, RoaringTreemap> = HashMap::new();
        for map in per_file_maps {
            for (path, positions) in map {
                *merged.entry(path).or_default() |= positions;
            }
        }
        Ok(merged)
    }

    /// Build one `PartitionedFile` per assigned data file, attaching a base
    /// `ParquetAccessPlan` (task 2.4) to each delete-carrying file.
    ///
    /// Phase A ([`Self::collect_delete_positions`]) performs all delete-file
    /// I/O up front. Phase B (this method) is a bounded-concurrent,
    /// order-preserving fan-out (`try_join_all` over ALL assigned files,
    /// preserving input order in the returned `Vec<PartitionedFile>`) that
    /// performs no DELETE-file I/O of its own: a delete-free entry takes no
    /// permit from the shared `delete_path_read_limiter` and issues no read. A
    /// delete-carrying entry instead fetches ITS OWN data file's Parquet footer
    /// — one object-store round-trip, under one permit from that same shared
    /// limiter — through the shared session [`FileMetadataCache`], the same
    /// cache the opener's `CachedParquetFileReaderFactory` reads. The fetch
    /// supplies the metadata size hint [`ParquetFormat::metadata_size_hint`]
    /// already governs for the opener, collapsing it to ONE hinted range GET,
    /// and explicitly skips the page index, since [`build_access_plan`] reads
    /// only each row group's row count and never the page index. The footer
    /// therefore parses ONCE for both access-plan construction and the scan,
    /// and no object-store HEAD is issued — then builds the base
    /// [`ParquetAccessPlan`] via [`build_access_plan`]. Every footer fetched
    /// here is also recorded via
    /// [`diagnostics::record_access_plan_cached_footer`], so a metadata-cache
    /// eviction that costs the opener a second fetch is observable rather than
    /// silent (task 1.7b).
    ///
    /// [`FileMetadataCache`]: datafusion::execution::cache::cache_manager::FileMetadataCache
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

        let delete_positions = self.collect_delete_positions(&store).await?;

        let size_hint = self.format.metadata_size_hint();
        let delete_positions_ref = &delete_positions;
        let builds = self.files.iter().map(|entry| {
            let store = Arc::clone(&store);
            let metadata_cache = Arc::clone(&metadata_cache);
            let limiter = Arc::clone(&self.delete_path_read_limiter);
            let secrets = self.secrets.as_slice();
            let table_root = self.table_root.as_str();
            async move {
                let abs = reconstruct_abs_uri(&entry.path, table_root);
                let meta = object_meta_for(&abs, entry.size)?;
                let partitioned = PartitionedFile::from(meta.clone());

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

        let expr_adapter = self.use_field_id_adapter.then(|| {
            Arc::new(FieldIdExprAdapterFactory {
                name_mapping: self.field_id_resolution.name_mapping.clone(),
                defaults: self.field_id_resolution.defaults.clone(),
            }) as Arc<_>
        });

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
#[path = "positional_deletes_tests.rs"]
mod tests;
