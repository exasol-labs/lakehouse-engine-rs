//! One scan's data files, shared by every byte range that reads them: a file's footer, the bytes
//! fetched with it, its page index, and its access plan each load once.

use crate::scan::row_deletes::DeleteLoads;
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// A contiguous part of a file held in memory.
#[derive(Debug)]
pub(super) struct HeldBytes {
    pub(super) start: u64,
    pub(super) bytes: Bytes,
}

impl HeldBytes {
    pub(super) fn slice(&self, range: &Range<u64>) -> Option<Bytes> {
        let end = self.start + self.bytes.len() as u64;
        (range.start >= self.start && range.end <= end).then(|| {
            self.bytes
                .slice((range.start - self.start) as usize..(range.end - self.start) as usize)
        })
    }
}

/// `bytes` of `reservation`'s consumer, reserved in the memory pool until the result drops.
pub(super) fn reserved(
    reservation: &MemoryReservation,
    bytes: usize,
) -> datafusion::error::Result<MemoryReservation> {
    let reserved = reservation.new_empty();
    reserved.try_grow(bytes)?;
    Ok(reserved)
}

fn external(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

fn external_df(e: exasol_udf_sdk::error::UdfError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// One GET of the file's last `want` bytes. A footer larger than that costs a second GET, for the
/// missing prefix only. No HEAD: the caller supplies `size`.
pub(super) async fn read_footer(
    store: &dyn ObjectStore,
    location: &Path,
    size: u64,
    want: u64,
) -> Result<(HeldBytes, ParquetMetaData)> {
    let mut start = size.saturating_sub(want.max(FOOTER_SIZE as u64));
    let mut bytes = store
        .get_range(location, start..size)
        .await
        .map_err(external)?;
    loop {
        let mut parser = ParquetMetaDataReader::new();
        match parser.try_parse_sized(&bytes, size) {
            Ok(()) => return Ok((HeldBytes { start, bytes }, parser.finish()?)),
            Err(ParquetError::NeedMoreData(needed))
                if size.saturating_sub(needed as u64) < start =>
            {
                let prefix_start = size.saturating_sub(needed as u64);
                let prefix = store
                    .get_range(location, prefix_start..start)
                    .await
                    .map_err(external)?;
                bytes = [prefix, bytes].concat().into();
                start = prefix_start;
            }
            Err(e) => return Err(e),
        }
    }
}

/// The span of every column and offset index, which the opener reads in one request.
fn page_index_range(metadata: &ParquetMetaData) -> Option<Range<u64>> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .flat_map(|column| [column.column_index_range(), column.offset_index_range()])
        .flatten()
        .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end))
}

#[derive(Debug)]
struct Footer {
    metadata: Arc<ParquetMetaData>,
    held: Option<(HeldBytes, MemoryReservation)>,
    /// `None` when the held bytes already cover it.
    page_index: Option<Range<u64>>,
}

/// What one file's readers share while the file is being read.
#[derive(Debug, Default)]
struct FileLoads {
    footer: OnceCell<Footer>,
    page_index: OnceCell<Option<(HeldBytes, MemoryReservation)>>,
    access_plan: OnceCell<(ParquetAccessPlan, MemoryReservation)>,
}

#[derive(Debug, Default)]
struct FileState {
    open_readers: usize,
    finished: Vec<Range<u64>>,
    loads: Option<Arc<FileLoads>>,
}

#[derive(Debug)]
struct SharedFile {
    location: Path,
    size: u64,
    /// The key of this file's deletes in [`DeleteLoads`], when it carries any.
    delete_key: Option<String>,
    state: Mutex<FileState>,
}

impl SharedFile {
    fn state(&self) -> std::sync::MutexGuard<'_, FileState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn loads(&self) -> Arc<FileLoads> {
        Arc::clone(self.state().loads.get_or_insert_default())
    }

    /// Drops the shared loads once no reader is open and the finished ranges cover the file. A
    /// reader that opens later loads them again, which costs requests but never changes rows.
    fn reader_closed(&self, range: Range<u64>) {
        let mut state = self.state();
        state.open_readers -= 1;
        state.finished.push(range);
        if state.open_readers > 0 {
            return;
        }
        state.finished.sort_unstable_by_key(|r| r.start);
        let mut covered = 0;
        for range in &state.finished {
            if range.start > covered {
                return;
            }
            covered = covered.max(range.end);
        }
        if covered >= self.size {
            state.loads = None;
            state.finished.clear();
        }
    }
}

pub(super) struct ScanFiles {
    store: Arc<dyn ObjectStore>,
    metadata_size_hint: u64,
    held_bytes: MemoryReservation,
    files: HashMap<Path, SharedFile>,
    deletes: Option<DeleteLoads>,
}

impl std::fmt::Debug for ScanFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanFiles")
            .field("files", &self.files.len())
            .finish_non_exhaustive()
    }
}

impl ScanFiles {
    /// `files` pairs each data file with its [`DeleteLoads`] key, if it carries deletes.
    pub(super) fn new(
        store: Arc<dyn ObjectStore>,
        metadata_size_hint: Option<usize>,
        pool: &Arc<dyn MemoryPool>,
        files: impl IntoIterator<Item = (Path, u64, Option<String>)>,
        deletes: Option<DeleteLoads>,
    ) -> Self {
        Self {
            store,
            metadata_size_hint: metadata_size_hint.unwrap_or(0) as u64,
            held_bytes: MemoryConsumer::new("scan file bytes").register(pool),
            files: files
                .into_iter()
                .map(|(location, size, delete_key)| {
                    let file = SharedFile {
                        location: location.clone(),
                        size,
                        delete_key,
                        state: Mutex::default(),
                    };
                    (location, file)
                })
                .collect(),
            deletes,
        }
    }

    pub(super) fn applies_deletes(&self) -> bool {
        self.deletes.is_some()
    }

    pub(super) fn carries_deletes(&self, location: &Path) -> bool {
        self.files
            .get(location)
            .is_some_and(|file| file.delete_key.is_some())
    }

    fn file(&self, location: &Path) -> datafusion::error::Result<&SharedFile> {
        self.files.get(location).ok_or_else(|| {
            DataFusionError::Internal(format!("data file {location} is not part of this scan"))
        })
    }

    /// One GET of the file's last `metadata_size_hint` bytes, which stay held while they fit the
    /// pool. Bytes that do not fit are parsed and dropped.
    async fn footer<'a>(&self, file: &SharedFile, loads: &'a FileLoads) -> Result<&'a Footer> {
        loads
            .footer
            .get_or_try_init(|| async {
                let (tail, metadata) = read_footer(
                    self.store.as_ref(),
                    &file.location,
                    file.size,
                    self.metadata_size_hint,
                )
                .await?;
                let page_index =
                    page_index_range(&metadata).filter(|range| tail.slice(range).is_none());
                let reserved = reserved(&self.held_bytes, tail.bytes.len()).ok();
                Ok(Footer {
                    metadata: Arc::new(metadata),
                    held: reserved.map(|reserved| (tail, reserved)),
                    page_index,
                })
            })
            .await
    }

    /// Bytes of `range` already in memory: the footer's held bytes, or the page index.
    async fn held(
        &self,
        file: &SharedFile,
        loads: &FileLoads,
        range: &Range<u64>,
    ) -> Result<Option<Bytes>> {
        let Some(footer) = loads.footer.get() else {
            return Ok(None);
        };
        if let Some(bytes) = footer.held.as_ref().and_then(|(held, _)| held.slice(range)) {
            return Ok(Some(bytes));
        }
        let Some(index) = footer
            .page_index
            .as_ref()
            .filter(|index| index.start <= range.start && range.end <= index.end)
        else {
            return Ok(None);
        };
        let held = loads
            .page_index
            .get_or_try_init(|| async {
                let length = (index.end - index.start) as usize;
                let Ok(reserved) = reserved(&self.held_bytes, length) else {
                    return Ok(None);
                };
                let bytes = self
                    .store
                    .get_range(&file.location, index.clone())
                    .await
                    .map_err(external)?;
                let held = HeldBytes {
                    start: index.start,
                    bytes,
                };
                Ok::<_, ParquetError>(Some((held, reserved)))
            })
            .await?;
        Ok(held.as_ref().and_then(|(held, _)| held.slice(range)))
    }

    /// Attaches the file's access plan, built once from its footer and its deletes, which load
    /// together. A file without deletes is returned as is.
    pub(super) async fn with_access_plan(
        &self,
        mut part: PartitionedFile,
    ) -> datafusion::error::Result<PartitionedFile> {
        let file = self.file(&part.object_meta.location)?;
        let (Some(deletes), Some(key)) = (&self.deletes, &file.delete_key) else {
            return Ok(part);
        };
        let loads = file.loads();
        let (plan, _) = loads
            .access_plan
            .get_or_try_init(|| async {
                let (footer, loaded) = futures::future::try_join(
                    async { Ok(self.footer(file, &loads).await?) },
                    async { deletes.load(key).await.map_err(external_df) },
                )
                .await?;
                deletes
                    .access_plan(key, &loaded, footer.metadata.row_groups())
                    .map_err(external_df)
            })
            .await?;
        part.extensions.insert(plan.clone());
        Ok(part)
    }
}

/// The reader factory of every file scan of one [`ScanFiles`].
#[derive(Debug)]
pub(super) struct SharedReaderFactory(pub(super) Arc<ScanFiles>);

impl ParquetFileReaderFactory for SharedReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let location = partitioned_file.object_meta.location;
        let file = self.0.file(&location)?;
        // Fails closed: a plan rewrite that drops the delete-applying source must not return
        // deleted rows.
        if file.delete_key.is_some()
            && partitioned_file
                .extensions
                .get::<ParquetAccessPlan>()
                .is_none()
        {
            return Err(DataFusionError::Internal(format!(
                "data file {location} carries row-position deletes but reached the reader \
                 without its access plan"
            )));
        }
        let range = match partitioned_file.range {
            Some(range) => range.start as u64..range.end as u64,
            None => 0..file.size,
        };
        let loads = {
            let mut state = file.state();
            state.open_readers += 1;
            Arc::clone(state.loads.get_or_insert_default())
        };
        Ok(Box::new(SharedFileReader {
            files: Arc::clone(&self.0),
            location,
            loads,
            range,
        }))
    }
}

struct SharedFileReader {
    files: Arc<ScanFiles>,
    location: Path,
    loads: Arc<FileLoads>,
    range: Range<u64>,
}

impl SharedFileReader {
    fn file(&self) -> &SharedFile {
        &self.files.files[&self.location]
    }
}

impl Drop for SharedFileReader {
    fn drop(&mut self) {
        self.file().reader_closed(self.range.clone());
    }
}

impl AsyncFileReader for SharedFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            let file = self.file();
            if let Some(bytes) = self.files.held(file, &self.loads, &range).await? {
                return Ok(bytes);
            }
            self.files
                .store
                .get_range(&file.location, range)
                .await
                .map_err(external)
        }
        .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            let file = self.file();
            let mut held = Vec::with_capacity(ranges.len());
            for range in &ranges {
                held.push(self.files.held(file, &self.loads, range).await?);
            }
            let missing: Vec<Range<u64>> = ranges
                .iter()
                .zip(&held)
                .filter(|(_, held)| held.is_none())
                .map(|(range, _)| range.clone())
                .collect();
            let mut fetched = if missing.is_empty() {
                Vec::new()
            } else {
                self.files
                    .store
                    .get_ranges(&file.location, &missing)
                    .await
                    .map_err(external)?
            }
            .into_iter();
            Ok(held
                .into_iter()
                .map(|held| held.or_else(|| fetched.next()).unwrap_or_default())
                .collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let footer = self.files.footer(self.file(), &self.loads).await?;
            Ok(Arc::clone(&footer.metadata))
        }
        .boxed()
    }
}

#[cfg(test)]
#[path = "shared_files_tests.rs"]
mod tests;
