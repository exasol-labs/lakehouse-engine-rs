use super::*;
use crate::scan::test_support::{CountingObjectStore, parquet_bytes};
use arrow::array::{Int64Array, RecordBatch};
use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
use object_store::memory::InMemory;
use object_store::{GetRange, PutPayload};

const NAME: &str = "data/a.parquet";

/// Two `Int64` columns of 20,000 rows in ten row groups, about 330 KB with a page index.
fn data_file() -> Bytes {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from_iter_values(0..20_000)) as _),
        (
            "value",
            Arc::new(Int64Array::from_iter_values((0..20_000).map(|i| i * 7919))) as _,
        ),
    ])
    .unwrap();
    parquet_bytes(batch, Some(2_000)).into()
}

struct Fixture {
    bytes: Bytes,
    store: Arc<CountingObjectStore>,
    pool: Arc<dyn MemoryPool>,
    files: Arc<ScanFiles>,
}

impl Fixture {
    async fn new(pool: Arc<dyn MemoryPool>, metadata_size_hint: usize) -> Self {
        Self::with(pool, metadata_size_hint, None, true).await
    }

    async fn with(
        pool: Arc<dyn MemoryPool>,
        metadata_size_hint: usize,
        delete_key: Option<String>,
        whole_small_files: bool,
    ) -> Self {
        let bytes = data_file();
        let store = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
        store
            .put(&Path::from(NAME), PutPayload::from(bytes.clone()))
            .await
            .unwrap();
        let files = Arc::new(ScanFiles::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            Some(metadata_size_hint),
            whole_small_files,
            &pool,
            [(Path::from(NAME), bytes.len() as u64, delete_key)],
            None,
        ));
        Self {
            bytes,
            store,
            pool,
            files,
        }
    }

    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn part(&self, range: Option<Range<u64>>) -> PartitionedFile {
        let part = PartitionedFile::new(NAME, self.size());
        match range {
            Some(range) => part.with_range(range.start as i64, range.end as i64),
            None => part,
        }
    }

    fn reader(&self, range: Option<Range<u64>>) -> Box<dyn AsyncFileReader + Send> {
        SharedReaderFactory(Arc::clone(&self.files))
            .create_reader(0, self.part(range), None, &ExecutionPlanMetricsSet::new())
            .unwrap()
    }

    fn requests(&self) -> Vec<Range<u64>> {
        self.store
            .ranges_of(NAME)
            .into_iter()
            .map(|range| match range {
                Some(GetRange::Bounded(range)) => range,
                other => panic!("every read is a bounded range, got {other:?}"),
            })
            .collect()
    }
}

fn unbounded() -> Arc<dyn MemoryPool> {
    Arc::new(UnboundedMemoryPool::default())
}

/// Room for a footer suffix and the page index, but not for the whole file.
fn pool_below_the_file() -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(64 * 1024))
}

fn column_chunks(metadata: &ParquetMetaData) -> Vec<Range<u64>> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .map(|column| {
            let (start, length) = column.byte_range();
            start..start + length
        })
        .collect()
}

/// Scenario: A data file's footer is fetched once for all its byte ranges
#[tokio::test]
async fn the_ranges_of_one_file_share_one_footer_request() {
    const HINT: u64 = 16 * 1024;
    let fixture = Fixture::new(pool_below_the_file(), HINT as usize).await;
    let size = fixture.size();
    let step = size.div_ceil(16);
    let mut readers: Vec<_> = (0..16)
        .map(|i| fixture.reader(Some(i * step..((i + 1) * step).min(size))))
        .collect();

    let footers =
        futures::future::try_join_all(readers.iter_mut().map(|reader| reader.get_metadata(None)))
            .await
            .unwrap();

    assert_eq!(
        fixture.requests(),
        [Range {
            start: size - HINT,
            end: size
        }]
    );
    assert!(footers.iter().all(|f| Arc::ptr_eq(f, &footers[0])));
}

/// Scenario: A small data file is read in one request
#[tokio::test]
async fn a_small_file_costs_one_request_under_and_above_the_size_hint() {
    for hint in [512 * 1024, 1024] {
        let fixture = Fixture::new(unbounded(), hint).await;
        let mut reader = fixture.reader(None);

        let metadata = reader.get_metadata(None).await.unwrap();
        let chunks = column_chunks(&metadata);
        let read = reader.get_byte_ranges(chunks.clone()).await.unwrap();

        let whole = Range {
            start: 0,
            end: fixture.size(),
        };
        assert_eq!(fixture.requests(), [whole], "hint {hint}");
        for (range, bytes) in chunks.iter().zip(read) {
            assert_eq!(
                bytes,
                fixture
                    .bytes
                    .slice(range.start as usize..range.end as usize)
            );
        }
    }
}

/// Scenario: A footer larger than the size hint costs one more request
#[tokio::test]
async fn a_footer_larger_than_the_hint_fetches_only_the_missing_prefix() {
    let fixture = Fixture::new(unbounded(), 0).await;
    let size = fixture.size();

    let (tail, metadata) = read_footer(fixture.store.as_ref(), &Path::from(NAME), size, 64)
        .await
        .unwrap();

    assert_eq!(metadata.num_row_groups(), 10);
    assert_eq!(fixture.requests(), [size - 64..size, tail.start..size - 64]);
    assert_eq!(tail.bytes, fixture.bytes.slice(tail.start as usize..));
}

/// Scenario: A data file's page index is fetched once for all its byte ranges
#[tokio::test]
async fn the_page_index_is_fetched_once_for_all_ranges() {
    // An 8-byte hint holds the footer alone, so the page index lies outside the held bytes.
    let fixture = Fixture::new(pool_below_the_file(), 8).await;
    let half = fixture.size() / 2;
    let mut readers = [
        fixture.reader(Some(0..half)),
        fixture.reader(Some(half..fixture.size())),
    ];

    let mut index = None;
    for reader in &mut readers {
        let metadata = reader.get_metadata(None).await.unwrap();
        let range = page_index_range(&metadata).expect("the fixture has a page index");
        let bytes = reader.get_bytes(range.clone()).await.unwrap();
        assert_eq!(
            bytes,
            fixture
                .bytes
                .slice(range.start as usize..range.end as usize)
        );
        index = Some(range);
    }

    let index = index.unwrap();
    let index_requests = fixture.requests().into_iter().filter(|r| *r == index);
    assert_eq!(index_requests.count(), 1);
    assert_eq!(
        fixture.requests().len(),
        3,
        "footer length, footer, page index"
    );
}

/// Scenario: Held file bytes are reserved in the memory pool
#[tokio::test]
async fn held_bytes_are_released_once_the_readers_cover_the_file() {
    let fixture = Fixture::new(unbounded(), 512 * 1024).await;
    let half = fixture.size() / 2;
    let mut first = fixture.reader(Some(0..half));
    let second = fixture.reader(Some(half..fixture.size()));

    first.get_metadata(None).await.unwrap();
    assert_eq!(fixture.pool.reserved(), fixture.bytes.len());

    drop(first);
    assert_eq!(
        fixture.pool.reserved(),
        fixture.bytes.len(),
        "an open range keeps the bytes"
    );
    drop(second);
    assert_eq!(fixture.pool.reserved(), 0);

    fixture.reader(None).get_metadata(None).await.unwrap();
    assert_eq!(
        fixture.requests().len(),
        2,
        "a later reader loads the file again"
    );
}

/// Scenario: Held file bytes are reserved in the memory pool
#[tokio::test]
async fn bytes_beyond_the_pool_are_not_held_and_are_read_from_the_store() {
    let fixture = Fixture::new(Arc::new(GreedyMemoryPool::new(0)), 16 * 1024).await;
    let mut reader = fixture.reader(None);

    let metadata = reader.get_metadata(None).await.unwrap();
    let chunks = column_chunks(&metadata);
    let last = chunks.last().unwrap().clone();
    let read = reader.get_bytes(last.clone()).await.unwrap();

    assert_eq!(
        read,
        fixture.bytes.slice(last.start as usize..last.end as usize)
    );
    assert_eq!(fixture.pool.reserved(), 0);
    assert_eq!(
        fixture.requests().len(),
        2,
        "the footer, then the column chunk"
    );
}

#[tokio::test]
async fn a_read_mixes_held_and_fetched_ranges_in_request_order() {
    const HINT: u64 = 16 * 1024;
    let fixture = Fixture::new(pool_below_the_file(), HINT as usize).await;
    let size = fixture.size();
    let mut reader = fixture.reader(None);
    reader.get_metadata(None).await.unwrap();

    let ranges = vec![size - 100..size - 50, 4..104, size - 40..size - 8, 200..300];
    let read = reader.get_byte_ranges(ranges.clone()).await.unwrap();

    for (range, bytes) in ranges.iter().zip(read) {
        assert_eq!(
            bytes,
            fixture
                .bytes
                .slice(range.start as usize..range.end as usize)
        );
    }
    assert_eq!(
        fixture.requests(),
        [size - HINT..size, 4..300],
        "only the ranges outside the held bytes are fetched, coalesced by the store"
    );
}

/// Scenario: A delete-carrying data file is never read without its access plan
#[tokio::test]
async fn a_delete_carrying_file_without_its_access_plan_is_refused() {
    let fixture = Fixture::with(
        unbounded(),
        0,
        Some("memory:///data/a.parquet".into()),
        true,
    )
    .await;
    let factory = SharedReaderFactory(Arc::clone(&fixture.files));
    let metrics = ExecutionPlanMetricsSet::new();

    let Err(error) = factory.create_reader(0, fixture.part(None), None, &metrics) else {
        panic!("a delete-carrying file without an access plan must be refused");
    };
    assert!(
        error.to_string().contains("without its access plan"),
        "{error}"
    );

    let mut planned = fixture.part(None);
    planned.extensions.insert(ParquetAccessPlan::new_all(10));
    assert!(factory.create_reader(0, planned, None, &metrics).is_ok());
}

/// Scenario: A small data file read for few columns is read by ranges
#[tokio::test]
async fn a_small_file_read_for_few_columns_fetches_its_footer_and_the_columns_it_reads() {
    let fixture = Fixture::with(unbounded(), 16 * 1024, None, false).await;
    let mut reader = fixture.reader(None);
    let chunks = column_chunks(&reader.get_metadata(None).await.unwrap());

    reader.get_bytes(chunks[0].clone()).await.unwrap();
    assert_eq!(
        fixture.requests(),
        [
            fixture.size() - 16 * 1024..fixture.size(),
            chunks[0].clone()
        ]
    );
}
