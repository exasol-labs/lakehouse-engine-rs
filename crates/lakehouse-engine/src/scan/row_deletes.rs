//! Applies row-position deletes: Iceberg positional-delete files, Iceberg deletion vectors, and
//! Delta deletion vectors.
//! Phase A ([`ShardDeletes::collect_positions`]) reads every unique delete payload once within one
//! shared connection budget and merges positions into one `HashMap<data_file_path,
//! RoaringTreemap>`, mechanism-agnostic. Phase B ([`build_access_plan`]) turns each set into a base
//! [`ParquetAccessPlan`], so DataFusion's opener intersects predicate/row-group/page pruning on top
//! and deletes compose with pushdown. Every read and decode is a top-level [`join_spawned`] task,
//! so a multi-thread runtime spreads the work across its workers. Only the orchestrating call awaits
//! tasks, so no permit holder ever waits on another task. Phase A reserves each set in the memory
//! pool as it grows, so no unreserved copy of the positions outlives one decoded batch.

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
use datafusion::execution::memory_pool::MemoryReservation;
use exasol_udf_sdk::error::UdfError;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use object_store::{OBJECT_STORE_COALESCE_DEFAULT, ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::errors::ParquetError;
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use roaring::RoaringTreemap;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinSet;
use url::Url;

const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2_147_483_546;
const FIELD_ID_POSITIONAL_DELETE_POS: i32 = 2_147_483_545;

const DELETE_FILE_BATCH_ROWS: usize = 8192;

/// Without a cap, many adjacent blobs of one Puffin file would collapse into one unbounded GET.
const PUFFIN_MAX_MERGED_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

type Positions = Vec<(String, RoaringTreemap)>;

type Read<T> = BoxFuture<'static, Result<T, UdfError>>;

pub(super) fn redact(msg: String, secrets: &[String]) -> String {
    let borrowed: Vec<&str> = secrets.iter().map(String::as_str).collect();
    crate::scan::emit::redact_error_text(&msg, &borrowed)
}

/// Results keep input order. Dropping the set on the first failure aborts the remaining tasks.
pub(super) async fn join_spawned<T, F>(
    futures: impl IntoIterator<Item = F>,
) -> Result<Vec<T>, UdfError>
where
    F: Future<Output = Result<T, UdfError>> + Send + 'static,
    T: Send + 'static,
{
    let mut tasks = JoinSet::new();
    for (idx, future) in futures.into_iter().enumerate() {
        tasks.spawn(async move { future.await.map(|value| (idx, value)) });
    }
    let mut results: Vec<Option<T>> = std::iter::repeat_with(|| None).take(tasks.len()).collect();
    while let Some(joined) = tasks.join_next().await {
        let (idx, value) = joined.map_err(|e| {
            let outcome = if e.is_panic() {
                "panicked"
            } else {
                "was cancelled"
            };
            UdfError::User(format!("a row-position delete task {outcome}"))
        })??;
        results[idx] = Some(value);
    }
    Ok(results.into_iter().flatten().collect())
}

async fn acquire(limiter: &Semaphore) -> Result<SemaphorePermit<'_>, UdfError> {
    limiter
        .acquire()
        .await
        .map_err(|e| UdfError::User(format!("delete-read limiter unavailable: {e}")))
}

pub(super) fn reserve(
    reservation: &MemoryReservation,
    bytes: usize,
    secrets: &[String],
) -> Result<(), UdfError> {
    reservation.try_grow(bytes).map_err(|e| {
        UdfError::User(redact(
            format!("the row-position delete state does not fit the memory pool: {e}"),
            secrets,
        ))
    })
}

/// Moves `reservation` from the `before` bytes it holds for a set to the set's `after` size.
fn track(
    reservation: &MemoryReservation,
    before: usize,
    after: usize,
    secrets: &[String],
) -> Result<(), UdfError> {
    if after > before {
        reserve(reservation, after - before, secrets)
    } else {
        reservation.shrink(before - after);
        Ok(())
    }
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
    IcebergDeletionVector(IcebergDeletionVector),
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

/// A positional-delete file whose footer has been read. The row groups inside the footer GET's
/// bytes are already in `decoded`; `row_groups` lists the others that can hold deletes of an
/// assigned data file.
struct OpenedDeleteFile {
    reader: SizedParquetReader,
    metadata: ArrowReaderMetadata,
    projection: ProjectionMask,
    row_groups: Vec<usize>,
    decoded: HashMap<String, RoaringTreemap>,
}

/// Serves reads from the footer GET's bytes only, so decoding from them can never issue a request
/// outside the connection budget.
struct FooterBytes {
    start: u64,
    bytes: Bytes,
}

impl FooterBytes {
    fn slice(&self, range: &Range<u64>) -> parquet::errors::Result<Bytes> {
        let end = self.start + self.bytes.len() as u64;
        if range.start < self.start || range.end > end {
            return Err(ParquetError::General(format!(
                "range {range:?} lies outside the delete file's footer bytes"
            )));
        }
        Ok(self
            .bytes
            .slice((range.start - self.start) as usize..(range.end - self.start) as usize))
    }
}

impl AsyncFileReader for FooterBytes {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        futures::future::ready(self.slice(&range)).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        futures::future::ready(ranges.iter().map(|range| self.slice(range)).collect()).boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        futures::future::ready(Err(ParquetError::General(
            "the footer is parsed before its bytes are decoded".into(),
        )))
        .boxed()
    }
}

impl OpenedDeleteFile {
    /// One read per remaining row group, each under its own permit. The positions decoded from
    /// the footer bytes lead as a ready read, so callers merge them like any row group's.
    fn row_group_reads(
        self,
        ctx: &Arc<DeleteReadContext>,
    ) -> Vec<Read<HashMap<String, RoaringTreemap>>> {
        let Self {
            reader,
            metadata,
            projection,
            row_groups,
            decoded,
        } = self;
        let decoded: Option<Read<_>> =
            (!decoded.is_empty()).then(|| futures::future::ready(Ok(decoded)).boxed());
        decoded
            .into_iter()
            .chain(row_groups.into_iter().map(|row_group| {
                let ctx = Arc::clone(ctx);
                let (reader, metadata, projection) =
                    (reader.clone(), metadata.clone(), projection.clone());
                let read: Read<_> = Box::pin(async move {
                    let _permit = acquire(&ctx.limiter).await?;
                    ctx.decode_row_groups(reader, metadata, projection, vec![row_group])
                        .await
                });
                read
            }))
            .collect()
    }
}

/// Drains `positions` into `path`'s set. Positions normally arrive sorted per the Iceberg spec and
/// append in bulk; sort + dedup keeps a deviating file correct.
fn add_positions(
    sets: &mut HashMap<String, RoaringTreemap>,
    path: &str,
    positions: &mut Vec<u64>,
    reservation: &MemoryReservation,
    secrets: &[String],
) -> Result<(), UdfError> {
    if positions.is_empty() {
        return Ok(());
    }
    positions.sort_unstable();
    positions.dedup();
    // A new set starts from zero reserved bytes, so the reservation equals the sets' sizes.
    let (set, before) = match sets.entry(path.to_string()) {
        Entry::Occupied(slot) => {
            let set = slot.into_mut();
            let before = set.serialized_size();
            (set, before)
        }
        Entry::Vacant(slot) => (slot.insert(RoaringTreemap::new()), 0),
    };
    if set.max().is_none_or(|max| positions[0] > max) {
        set.append(positions.drain(..)).map_err(|e| {
            UdfError::User(format!(
                "failed to build positional-delete set for a data file: {e}"
            ))
        })?;
    } else {
        set.extend(positions.drain(..));
    }
    track(reservation, before, set.serialized_size(), secrets)
}

/// One pass over `deletes`, cut at row-group boundaries as it goes, so the cost does not grow
/// with the row-group count. Untouched row groups stay `Scan`, fully deleted ones are skipped.
pub(super) fn build_access_plan(
    row_groups: &[RowGroupMetaData],
    deletes: &RoaringTreemap,
) -> ParquetAccessPlan {
    let mut plan = ParquetAccessPlan::new_all(row_groups.len());
    let mut deleted = deletes.iter().peekable();
    let mut start: u64 = 0;
    for (idx, row_group) in row_groups.iter().enumerate() {
        let end = start + row_group.num_rows() as u64;
        let mut selectors = Vec::new();
        let mut cursor = start;
        while let Some(first) = deleted.next_if(|pos| *pos < end) {
            let mut run_end = first + 1;
            while run_end < end && deleted.next_if_eq(&run_end).is_some() {
                run_end += 1;
            }
            if cursor < first {
                selectors.push(RowSelector::select((first - cursor) as usize));
            }
            selectors.push(RowSelector::skip((run_end - first) as usize));
            cursor = run_end;
        }
        if cursor == end && selectors.len() == 1 {
            plan.skip(idx);
        } else if !selectors.is_empty() {
            if cursor < end {
                selectors.push(RowSelector::select((end - cursor) as usize));
            }
            plan.scan_selection(idx, RowSelection::from(selectors));
        }
        start = end;
    }
    plan
}

/// Bytes the plan's row selections hold, for memory-pool accounting.
pub(super) fn access_plan_bytes(plan: &ParquetAccessPlan) -> usize {
    use datafusion::datasource::physical_plan::parquet::RowGroupAccess;
    plan.inner()
        .iter()
        .map(|access| match access {
            RowGroupAccess::Selection(selection) => {
                selection.iter().count() * size_of::<RowSelector>()
            }
            _ => 0,
        })
        .sum()
}

/// Groups blob ranges, sorted by start, into requests, each with the count of consecutive blobs
/// it covers. A blob joins the open request while the gap stays within `max_gap` and the request
/// within `max_len`.
fn merge_blob_ranges(
    ranges: &[Range<u64>],
    max_gap: u64,
    max_len: u64,
) -> Vec<(Range<u64>, usize)> {
    let mut requests: Vec<(Range<u64>, usize)> = Vec::new();
    for range in ranges {
        if let Some((request, blob_count)) = requests.last_mut()
            && range.start.saturating_sub(request.end) <= max_gap
            && range.end.max(request.end) - request.start <= max_len
        {
            request.end = request.end.max(range.end);
            *blob_count += 1;
        } else {
            requests.push((range.clone(), 1));
        }
    }
    requests
}

/// One shard's delete mechanisms, read within the invocation's shared connection budget.
pub(super) struct ShardDeletes<'a> {
    pub(super) files: &'a [FileEntry],
    pub(super) table_root: &'a str,
    pub(super) secrets: Arc<[String]>,
    /// Shared by every provider of one invocation (both join sides included), so the whole
    /// instance stays within one N-permit budget.
    pub(super) limiter: Arc<Semaphore>,
    /// Grown as positions decode, so it holds the returned sets' serialized size.
    pub(super) reservation: Arc<MemoryReservation>,
    pub(super) metadata_size_hint: Option<usize>,
}

impl ShardDeletes<'_> {
    /// Every mechanism passes [`applicable_delete_mechanism`] BEFORE any I/O, so an unapplicable
    /// one fails before anything is fetched. Delete-file footers are read before their row groups,
    /// alongside the deletion vectors. Every read shares one limiter; `RoaringTreemap` union is
    /// commutative, so completion order cannot change the result. Keyed by absolute data-file URI.
    /// On return, [`Self::reservation`] holds the serialized size of every returned set.
    pub(super) async fn collect_positions(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let mut delete_files: HashMap<&str, u64> = HashMap::new();
        let mut delta_vectors: Vec<(&str, DeltaDeletionVector)> = Vec::new();
        let mut iceberg_vectors: Vec<(&str, IcebergDeletionVector)> = Vec::new();
        for entry in self.files {
            for delete in &entry.deletes {
                match applicable_delete_mechanism(
                    delete,
                    &entry.path,
                    self.table_root,
                    &self.secrets,
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

        let ctx = Arc::new(DeleteReadContext {
            store: Arc::clone(store),
            table_root: self.table_root.to_string(),
            assigned: self
                .files
                .iter()
                .map(|entry| reconstruct_abs_uri(&entry.path, self.table_root))
                .collect(),
            secrets: Arc::clone(&self.secrets),
            limiter: Arc::clone(&self.limiter),
            reservation: Arc::clone(&self.reservation),
            metadata_size_hint: self.metadata_size_hint,
        });
        let mut vector_reads = ctx.delta_vector_reads(delta_vectors);
        vector_reads.extend(ctx.iceberg_vector_reads(iceberg_vectors));
        let (opened, vector_positions) = futures::future::try_join(
            join_spawned(ctx.delete_file_opens(delete_files)?),
            join_spawned(vector_reads),
        )
        .await?;
        let row_groups = join_spawned(
            opened
                .into_iter()
                .flat_map(|file| file.row_group_reads(&ctx)),
        )
        .await?;

        ctx.union_by_data_file(
            vector_positions
                .into_iter()
                .flatten()
                .chain(row_groups.into_iter().flatten()),
        )
        .await
    }
}

/// What every delete-read task of one shard shares, so each task holds one `Arc` of it.
struct DeleteReadContext {
    store: Arc<dyn ObjectStore>,
    table_root: String,
    /// The shard's data files, as absolute URIs.
    assigned: HashSet<String>,
    secrets: Arc<[String]>,
    limiter: Arc<Semaphore>,
    reservation: Arc<MemoryReservation>,
    metadata_size_hint: Option<usize>,
}

impl DeleteReadContext {
    /// Restricted to `assigned` because one partition-granularity delete file references many
    /// data files. Row groups whose `file_path` range cannot overlap are skipped, exploiting
    /// Iceberg's required (`file_path`, `pos`) sort. No HEAD is issued. The footer is one GET of
    /// the file's last `metadata_size_hint` bytes, and the row groups inside them decode here, so
    /// a delete file within the hint costs one request in total.
    async fn open_delete_file(
        &self,
        delete_meta: ObjectMeta,
    ) -> Result<OpenedDeleteFile, UdfError> {
        let failed = |e: String| {
            UdfError::User(redact(
                format!("failed to open delete file: {e}"),
                &self.secrets,
            ))
        };
        let size = delete_meta.size;
        let (tail_start, tail, parquet_metadata) = {
            let _permit = acquire(&self.limiter).await?;
            let mut want = self.metadata_size_hint.unwrap_or(0).max(FOOTER_SIZE) as u64;
            loop {
                let start = size.saturating_sub(want);
                let tail = self
                    .store
                    .get_range(&delete_meta.location, start..size)
                    .await
                    .map_err(|e| failed(e.to_string()))?;
                let mut parser = ParquetMetaDataReader::new();
                match parser.try_parse_sized(&tail, size) {
                    Ok(()) => {
                        let parsed = parser.finish().map_err(|e| failed(e.to_string()))?;
                        break (start, tail, parsed);
                    }
                    // A footer larger than the hint costs one more GET, still under this permit.
                    Err(ParquetError::NeedMoreData(needed)) if needed as u64 > want => {
                        want = needed as u64;
                    }
                    Err(e) => return Err(failed(e.to_string())),
                }
            }
        };
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), ArrowReaderOptions::new())
                .map_err(|e| failed(e.to_string()))?;

        let (file_path_idx, pos_idx) = locate_delete_columns(metadata.schema())?;
        let parquet_schema = metadata.parquet_schema();
        let projection = ProjectionMask::roots(parquet_schema, [file_path_idx, pos_idx]);
        let projected_leaves: Vec<usize> = (0..parquet_schema.num_columns())
            .filter(|&leaf| {
                let root = parquet_schema.get_column_root_idx(leaf);
                root == file_path_idx || root == pos_idx
            })
            .collect();
        let (in_tail, row_groups): (Vec<usize>, Vec<usize>) = metadata
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter(|(_, row_group)| {
                delete_row_group_may_match(row_group, file_path_idx, &self.assigned)
            })
            .map(|(idx, _)| idx)
            .partition(|&idx| {
                let row_group = metadata.metadata().row_group(idx);
                projected_leaves
                    .iter()
                    .all(|&leaf| row_group.column(leaf).byte_range().0 >= tail_start)
            });
        // Decoded before the bytes drop, so they never outlive this task, stay a transient read
        // buffer like any decode buffer, and cannot crowd the pool while other files load.
        let decoded = if in_tail.is_empty() {
            HashMap::new()
        } else {
            let footer_bytes = FooterBytes {
                start: tail_start,
                bytes: tail,
            };
            self.decode_row_groups(footer_bytes, metadata.clone(), projection.clone(), in_tail)
                .await?
        };
        Ok(OpenedDeleteFile {
            reader: SizedParquetReader::new(Arc::clone(&self.store), delete_meta.location, size),
            metadata,
            projection,
            row_groups,
            decoded,
        })
    }

    /// The file is sorted by `file_path`, so one run of equal paths costs one set lookup. Each
    /// batch's positions join their set before the next batch decodes, so the reservation follows
    /// the sets as they grow and an oversized delete set fails mid-read.
    async fn decode_row_groups<R: AsyncFileReader + Unpin + Send + 'static>(
        &self,
        reader: R,
        metadata: ArrowReaderMetadata,
        projection: ProjectionMask,
        row_groups: Vec<usize>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
            .with_row_groups(row_groups)
            .with_projection(projection)
            .with_batch_size(DELETE_FILE_BATCH_ROWS)
            .build()
            .map_err(|e| {
                UdfError::User(redact(
                    format!("failed to read delete file: {e}"),
                    &self.secrets,
                ))
            })?;
        let (file_path_idx, pos_idx) = locate_delete_columns(stream.schema())?;
        let mut positions_by_data_file: HashMap<String, RoaringTreemap> = HashMap::new();
        let mut run: Option<(String, Option<Vec<u64>>)> = None;

        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| {
                UdfError::User(redact(
                    format!("error decoding delete file: {e}"),
                    &self.secrets,
                ))
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
                if run.as_ref().is_none_or(|(current, _)| current != path) {
                    if let Some((ended, Some(bucket))) = run.as_mut() {
                        add_positions(
                            &mut positions_by_data_file,
                            ended,
                            bucket,
                            &self.reservation,
                            &self.secrets,
                        )?;
                    }
                    run = Some((
                        path.to_string(),
                        self.assigned.contains(path).then(Vec::new),
                    ));
                }
                let Some((_, Some(bucket))) = run.as_mut() else {
                    continue;
                };
                let pos = positions.value(row);
                // Casting a negative `pos` to u64 would wrap and silently drop the delete.
                if pos < 0 {
                    return Err(UdfError::User(format!(
                        "positional-delete file has a negative pos ({pos}); refusing to \
                         apply a malformed delete"
                    )));
                }
                bucket.push(pos as u64);
            }
            if let Some((path, Some(bucket))) = run.as_mut() {
                add_positions(
                    &mut positions_by_data_file,
                    path,
                    bucket,
                    &self.reservation,
                    &self.secrets,
                )?;
            }
        }
        Ok(positions_by_data_file)
    }

    /// Unions each data file's sets, one task per data file.
    async fn union_by_data_file(
        self: &Arc<Self>,
        sets: impl IntoIterator<Item = (String, RoaringTreemap)>,
    ) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
        let mut sets_by_data_file: HashMap<String, Vec<RoaringTreemap>> = HashMap::new();
        for (path, positions) in sets {
            sets_by_data_file.entry(path).or_default().push(positions);
        }
        let unions = join_spawned(sets_by_data_file.into_iter().map(|(path, sets)| {
            let ctx = Arc::clone(self);
            async move {
                let before = sets.iter().map(RoaringTreemap::serialized_size).sum();
                let union = sets
                    .into_iter()
                    .fold(RoaringTreemap::new(), |mut union, set| {
                        union |= set;
                        union
                    });
                track(
                    &ctx.reservation,
                    before,
                    union.serialized_size(),
                    &ctx.secrets,
                )?;
                Ok((path, union))
            }
        }))
        .await?;
        Ok(unions.into_iter().collect())
    }

    /// No HEAD: each [`ObjectMeta`] is built from the spec-supplied size.
    fn delete_file_opens(
        self: &Arc<Self>,
        delete_files: HashMap<&str, u64>,
    ) -> Result<Vec<Read<OpenedDeleteFile>>, UdfError> {
        delete_files
            .into_iter()
            .map(|(delete_path, delete_size)| {
                let delete_meta = object_meta_for(
                    &reconstruct_abs_uri(delete_path, &self.table_root),
                    delete_size,
                )?;
                let ctx = Arc::clone(self);
                let open: Read<_> =
                    Box::pin(async move { ctx.open_delete_file(delete_meta).await });
                Ok(open)
            })
            .collect()
    }

    /// Reads `url` whole, or only `range` of it, under one permit of the shared limiter. A failure
    /// names `data_file`, because a deletion vector has no identity of its own.
    async fn read_deletion_vector_payload(
        &self,
        url: &str,
        range: Option<Range<u64>>,
        data_file: &str,
    ) -> Result<Bytes, UdfError> {
        let unreadable = |e: String| {
            UdfError::User(redact(
                format!("data file '{data_file}': its deletion vector could not be read: {e}"),
                &self.secrets,
            ))
        };
        let _permit = acquire(&self.limiter)
            .await
            .map_err(|e| unreadable(e.to_string()))?;
        let location = ListingTableUrl::parse(url)
            .map_err(|e| unreadable(format!("invalid deletion-vector URL: {e}")))?
            .prefix()
            .clone();
        let body = match range {
            Some(range) => self.store.get_range(&location, range).await,
            None => match self.store.get(&location).await {
                Ok(result) => result.bytes().await,
                Err(e) => Err(e),
            },
        };
        body.map_err(|e| unreadable(e.to_string()))
    }

    /// Reserves each member's decoded set and keys it by absolute data-file URI.
    fn decode_vectors<V>(
        &self,
        members: &[(String, V)],
        decode: impl Fn(&str, &V) -> Result<RoaringTreemap, UdfError>,
    ) -> Result<Positions, UdfError> {
        members
            .iter()
            .map(|(data_file, vector)| {
                let positions = decode(data_file, vector)?;
                reserve(
                    &self.reservation,
                    positions.serialized_size(),
                    &self.secrets,
                )?;
                Ok((reconstruct_abs_uri(data_file, &self.table_root), positions))
            })
            .collect()
    }

    /// A sidecar is fetched WHOLE, not by the descriptor's range: the decoder validates the
    /// version byte at position 0, and one body then serves every descriptor naming it. No HEAD:
    /// descriptors carry the vector's size, never the sidecar's. An inline vector is decoded in
    /// its own task.
    fn delta_vector_reads(
        self: &Arc<Self>,
        vectors: Vec<(&str, DeltaDeletionVector)>,
    ) -> Vec<Read<Positions>> {
        let mut by_sidecar: HashMap<Url, Vec<(String, DeltaDeletionVector)>> = HashMap::new();
        let mut inline = Vec::new();
        for (data_file, vector) in vectors {
            let member = (data_file.to_string(), vector);
            match member.1.sidecar_url() {
                Some(url) => by_sidecar.entry(url.clone()).or_default().push(member),
                None => inline.push(member),
            }
        }

        by_sidecar
            .into_iter()
            .map(|(sidecar, members)| (Some(sidecar), members))
            .chain(inline.into_iter().map(|member| (None, vec![member])))
            .map(|(sidecar, members)| {
                let ctx = Arc::clone(self);
                let read: Read<_> = Box::pin(async move {
                    // A shared sidecar is named by its first referrer.
                    let body = match &sidecar {
                        Some(url) => Some(
                            ctx.read_deletion_vector_payload(url.as_str(), None, &members[0].0)
                                .await?,
                        ),
                        None => None,
                    };
                    ctx.decode_vectors(&members, |data_file, vector| {
                        vector.decode(body.clone(), data_file, &ctx.secrets)
                    })
                });
                read
            })
            .collect()
    }

    /// Fetches only the blob ranges this shard's data files need, because one Puffin file may hold
    /// the vectors of a whole table. Each merged request is its own task under its own permit and
    /// decodes its blobs as soon as its bytes arrive.
    fn iceberg_vector_reads(
        self: &Arc<Self>,
        vectors: Vec<(&str, IcebergDeletionVector)>,
    ) -> Vec<Read<Positions>> {
        let mut by_puffin_file: HashMap<String, Vec<(String, IcebergDeletionVector)>> =
            HashMap::new();
        for (data_file, vector) in vectors {
            by_puffin_file
                .entry(vector.path.clone())
                .or_default()
                .push((data_file.to_string(), vector));
        }

        let mut reads = Vec::new();
        for (puffin_path, mut members) in by_puffin_file {
            members.sort_by_key(|(_, vector)| vector.range.start);
            let url = reconstruct_abs_uri(&puffin_path, &self.table_root);
            let ranges: Vec<Range<u64>> = members.iter().map(|(_, v)| v.range.clone()).collect();
            let mut members = members.into_iter();
            for (request, blob_count) in merge_blob_ranges(
                &ranges,
                OBJECT_STORE_COALESCE_DEFAULT,
                PUFFIN_MAX_MERGED_REQUEST_BYTES,
            ) {
                let blobs: Vec<(String, IcebergDeletionVector)> =
                    members.by_ref().take(blob_count).collect();
                let ctx = Arc::clone(self);
                let url = url.clone();
                reads.push(Box::pin(async move {
                    // A shared Puffin request is named by its first referrer.
                    let body = ctx
                        .read_deletion_vector_payload(&url, Some(request.clone()), &blobs[0].0)
                        .await?;
                    ctx.decode_vectors(&blobs, |data_file, vector| {
                        let blob = body.slice(
                            (vector.range.start - request.start) as usize
                                ..(vector.range.end - request.start) as usize,
                        );
                        vector.decode(&blob, data_file, &ctx.secrets)
                    })
                }) as Read<_>);
            }
        }
        reads
    }
}

#[cfg(test)]
#[path = "row_deletes_tests.rs"]
mod tests;
