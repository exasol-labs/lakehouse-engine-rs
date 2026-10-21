//! Applies row-position deletes: Iceberg positional-delete files, Iceberg deletion vectors, and
//! Delta deletion vectors.
//! Phase A ([`ShardDeletes::collect_positions`]) reads every unique delete payload once within one
//! shared connection budget and merges positions into one `HashMap<data_file_path,
//! RoaringTreemap>`, mechanism-agnostic. Phase B ([`build_access_plan`]) turns each set into a base
//! [`ParquetAccessPlan`], so DataFusion's opener intersects predicate/row-group/page pruning on top
//! and deletes compose with pushdown.

use crate::scan::deletion_vectors::{
    DeltaDeletionVector, IcebergDeletionVector, LoggedDeletionVector, ManifestDeletionVector,
};
use crate::scan::parquet_reader::{SizedParquetReader, object_meta_for};
use crate::scan::reconstruct_abs_uri;
use crate::scan::spec::{DeleteMechanism, FileEntry};
use arrow::array::{Array, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use exasol_udf_sdk::error::UdfError;
use futures::StreamExt;
use futures::future::try_join_all;
use object_store::path::Path as ObjectPath;
use object_store::{
    OBJECT_STORE_COALESCE_DEFAULT, ObjectMeta, ObjectStore, ObjectStoreExt, coalesce_ranges,
};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

/// `selected_row_groups` lists surviving row groups; deletes in pruned ones are stepped over.
///
/// Vendored from apache/iceberg-rust
/// (`crates/iceberg/src/arrow/reader/positional_deletes.rs::build_deletes_row_selection`), where
/// it is `pub(super)`. Algorithmically identical, but takes the mechanism-agnostic
/// [`RoaringTreemap`] that every delete mechanism merges into. #344.
fn build_deletes_row_selection(
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

pub(super) fn redact(msg: String, secrets: &[String]) -> String {
    let borrowed: Vec<&str> = secrets.iter().map(String::as_str).collect();
    let stripped = crate::scan::emit::redact_secret_values(&msg, &borrowed);
    crate::scan::emit::redact_credentials(&stripped)
}

/// Every variant feeds ONE position map, so nothing downstream learns which mechanism produced
/// it. A deletion vector's positions index the ONE data file carrying it.
#[derive(Debug)]
enum ApplicableDelete<'a> {
    /// One such file may carry deletes for many data files.
    PositionalDeleteFile {
        path: &'a str,
        size: u64,
    },
    DeltaDeletionVector(DeltaDeletionVector),
    IcebergDeletionVector(IcebergDeletionVector<'a>),
}

/// Read-time backstop behind the plan-time adapter gate: an unapplicable mechanism is refused
/// before any row of its data file is emitted, and the payload is reachable only through here,
/// so it can neither be read nor silently skipped. A deletion vector's refusal names
/// `data_file_path`: a Delta vector may be inline, and a Puffin file holds many data files' vectors.
fn applicable_delete_mechanism<'a>(
    delete: &'a DeleteMechanism,
    data_file_path: &str,
    table_root: &str,
    secrets: &[String],
) -> Result<ApplicableDelete<'a>, UdfError> {
    match delete {
        DeleteMechanism::IcebergPositionalDelete { path, size } => {
            Ok(ApplicableDelete::PositionalDeleteFile {
                path: path.as_str(),
                size: *size,
            })
        }
        DeleteMechanism::DeltaDeletionVector {
            storage,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        } => DeltaDeletionVector::resolve(
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
        )
        .map(ApplicableDelete::DeltaDeletionVector),
        DeleteMechanism::IcebergPuffinDeletionVector {
            path,
            size,
            content_offset,
            content_size_in_bytes,
            record_count,
        } => IcebergDeletionVector::resolve(
            ManifestDeletionVector {
                path,
                size: *size,
                content_offset: *content_offset,
                content_size_in_bytes: *content_size_in_bytes,
                record_count: *record_count,
            },
            data_file_path,
            secrets,
        )
        .map(ApplicableDelete::IcebergDeletionVector),
        DeleteMechanism::IcebergEqualityDelete { path, .. } => {
            let path = redact(path.clone(), secrets);
            Err(UdfError::User(format!(
                "assigned delete file '{path}' is an Iceberg equality delete, which this engine \
                 cannot apply on read (only Iceberg Parquet positional deletes and deletion \
                 vectors are supported); refusing to emit rows for the affected data file"
            )))
        }
    }
}

/// Runs `read` against `url` under one permit of the shared limiter. A failure names
/// `data_file`, because a deletion vector has no identity of its own.
async fn read_deletion_vector_payload<'a, T, Fut>(
    store: &'a dyn ObjectStore,
    limiter: &Semaphore,
    url: &str,
    data_file: &str,
    secrets: &[String],
    read: impl FnOnce(&'a dyn ObjectStore, ObjectPath) -> Fut,
) -> Result<T, UdfError>
where
    Fut: Future<Output = object_store::Result<T>>,
{
    let unreadable = |e: String| {
        UdfError::User(redact(
            format!("data file '{data_file}': its deletion vector could not be read: {e}"),
            secrets,
        ))
    };
    let _permit = limiter
        .acquire()
        .await
        .map_err(|e| unreadable(format!("delete-read limiter unavailable: {e}")))?;
    let location = ListingTableUrl::parse(url)
        .map_err(|e| unreadable(format!("invalid deletion-vector URL: {e}")))?
        .prefix()
        .clone();
    read(store, location)
        .await
        .map_err(|e| unreadable(e.to_string()))
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
    let reader = SizedParquetReader::new(store, delete_meta.location.clone(), delete_meta.size);
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
pub(super) fn build_access_plan(
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

/// One shard's delete mechanisms, read within the invocation's shared connection budget.
pub(super) struct ShardDeletes<'a> {
    pub(super) files: &'a [FileEntry],
    pub(super) table_root: &'a str,
    pub(super) secrets: &'a [String],
    /// Shared by every provider of one invocation (both join sides included), so the whole
    /// instance stays within one N-permit budget.
    pub(super) limiter: &'a Semaphore,
}

impl ShardDeletes<'_> {
    /// Every mechanism passes [`applicable_delete_mechanism`] BEFORE any I/O, so an unapplicable
    /// one fails before anything is fetched. The three fan-outs share one limiter; `RoaringTreemap`
    /// union is commutative, so completion order cannot change the result. Keyed by absolute
    /// data-file URI.
    pub(super) async fn collect_positions(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let assigned: HashSet<String> = self
            .files
            .iter()
            .map(|entry| reconstruct_abs_uri(&entry.path, self.table_root))
            .collect();

        let mut delete_files: HashMap<&str, u64> = HashMap::new();
        let mut delta_vectors: Vec<(&str, DeltaDeletionVector)> = Vec::new();
        let mut iceberg_vectors: Vec<(&str, IcebergDeletionVector)> = Vec::new();
        for entry in self.files {
            for delete in &entry.deletes {
                match applicable_delete_mechanism(
                    delete,
                    &entry.path,
                    self.table_root,
                    self.secrets,
                )? {
                    ApplicableDelete::PositionalDeleteFile { path, size } => {
                        delete_files.entry(path).or_insert(size);
                    }
                    ApplicableDelete::DeltaDeletionVector(vector) => {
                        delta_vectors.push((entry.path.as_str(), vector));
                    }
                    ApplicableDelete::IcebergDeletionVector(vector) => {
                        iceberg_vectors.push((entry.path.as_str(), vector));
                    }
                }
            }
        }

        if delete_files.is_empty() && delta_vectors.is_empty() && iceberg_vectors.is_empty() {
            return Ok(HashMap::new());
        }

        let (per_delete_file, sidecars, per_iceberg_vector) = futures::future::try_join3(
            self.read_delete_files(store, delete_files, &assigned),
            self.fetch_delta_sidecars(store.as_ref(), &delta_vectors),
            self.read_iceberg_deletion_vectors(store.as_ref(), &iceberg_vectors),
        )
        .await?;

        let mut merged: HashMap<String, RoaringTreemap> = HashMap::new();
        for map in per_delete_file {
            for (path, positions) in map {
                *merged.entry(path).or_default() |= positions;
            }
        }
        for (data_file, vector) in &delta_vectors {
            let body = vector
                .sidecar_url()
                .and_then(|url| sidecars.get(url))
                .cloned();
            let positions = vector.decode(body, data_file, self.secrets)?;
            let abs = reconstruct_abs_uri(data_file, self.table_root);
            *merged.entry(abs).or_default() |= positions;
        }
        for (abs, positions) in per_iceberg_vector {
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
        let reads = delete_files
            .into_iter()
            .map(|(delete_path, delete_size)| async move {
                let _permit =
                    self.limiter.acquire().await.map_err(|e| {
                        UdfError::User(format!("delete-read limiter unavailable: {e}"))
                    })?;
                let delete_abs = reconstruct_abs_uri(delete_path, self.table_root);
                let delete_meta = object_meta_for(&delete_abs, delete_size)?;
                read_delete_file_positions(Arc::clone(store), delete_meta, assigned, self.secrets)
                    .await
            });
        try_join_all(reads).await
    }

    /// Fetched WHOLE, not by the descriptor's range: the decoder validates the version byte at
    /// position 0, and one body can then serve every descriptor naming it. No HEAD: descriptors
    /// carry the vector's size, never the sidecar's.
    async fn fetch_delta_sidecars(
        &self,
        store: &dyn ObjectStore,
        vectors: &[(&str, DeltaDeletionVector)],
    ) -> Result<HashMap<Url, Bytes>, UdfError> {
        // A failed fetch must name a DATA file; a shared sidecar is named by its first referrer.
        let mut unique: HashMap<&Url, &str> = HashMap::new();
        for (data_file, vector) in vectors {
            if let Some(url) = vector.sidecar_url() {
                unique.entry(url).or_insert(data_file);
            }
        }

        let reads = unique.into_iter().map(|(url, data_file)| async move {
            let body = read_deletion_vector_payload(
                store,
                self.limiter,
                url.as_str(),
                data_file,
                self.secrets,
                |store, location| async move { store.get(&location).await?.bytes().await },
            )
            .await?;
            Ok::<_, UdfError>((url.clone(), body))
        });
        Ok(try_join_all(reads).await?.into_iter().collect())
    }

    /// Fetches only the blob ranges this shard's data files need, because one Puffin file may hold
    /// the vectors of a whole table. Ranges of one Puffin file are merged into as few requests as
    /// the gaps allow, each under its own permit. Returns positions keyed by absolute data-file
    /// URI.
    async fn read_iceberg_deletion_vectors(
        &self,
        store: &dyn ObjectStore,
        vectors: &[(&str, IcebergDeletionVector<'_>)],
    ) -> Result<Vec<(String, RoaringTreemap)>, UdfError> {
        let mut by_puffin_file: HashMap<&str, Vec<&(&str, IcebergDeletionVector<'_>)>> =
            HashMap::new();
        for vector in vectors {
            by_puffin_file
                .entry(vector.1.path)
                .or_default()
                .push(vector);
        }

        let reads = by_puffin_file
            .into_iter()
            .map(|(puffin_path, members)| async move {
                let url = reconstruct_abs_uri(puffin_path, self.table_root);
                let ranges: Vec<Range<u64>> =
                    members.iter().map(|(_, v)| v.range.clone()).collect();
                // A shared Puffin file is named by its first referrer. `get_ranges` would run its
                // merged requests concurrently under the caller's single permit.
                let fetch = |range| {
                    read_deletion_vector_payload(
                        store,
                        self.limiter,
                        &url,
                        members[0].0,
                        self.secrets,
                        |store, location| async move { store.get_range(&location, range).await },
                    )
                };
                let blobs = coalesce_ranges(&ranges, fetch, OBJECT_STORE_COALESCE_DEFAULT).await?;
                members
                    .iter()
                    .zip(blobs)
                    .map(|((data_file, vector), blob)| {
                        let positions = vector.decode(&blob, data_file, self.secrets)?;
                        Ok((reconstruct_abs_uri(data_file, self.table_root), positions))
                    })
                    .collect::<Result<Vec<_>, UdfError>>()
            });
        Ok(try_join_all(reads).await?.into_iter().flatten().collect())
    }
}

#[cfg(test)]
#[path = "row_deletes_tests.rs"]
mod tests;
