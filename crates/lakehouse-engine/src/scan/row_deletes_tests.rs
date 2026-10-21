use super::*;
use crate::scan::spec::DeltaDeletionVectorStorage;
use crate::scan::test_support::{current_thread_runtime, parquet_bytes};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use chrono::TimeZone;
use object_store::GetRange;
use object_store::PutPayload;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

fn row_groups_of(row_counts: &[i64]) -> Vec<RowGroupMetaData> {
    let schema_descr = get_test_schema_descr();
    let columns: Vec<ColumnChunkMetaData> = schema_descr
        .columns()
        .iter()
        .map(|ptr| ColumnChunkMetaData::builder(ptr.clone()).build().unwrap())
        .collect();
    row_counts
        .iter()
        .enumerate()
        .map(|(ordinal, &rows)| {
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), rows, ordinal as i16)
        })
        .collect()
}

/// Expected selection is upstream iceberg-rust's `build_deletes_row_selection` output.
#[test]
fn access_plan_matches_upstream_whole_file_selection() {
    let metas = row_groups_of(&[1000, 500, 500, 1000, 500]);
    let deletes = RoaringTreemap::from_iter([
        1, 3, 4, 5, 998, 999, 1000, 1010, 1011, 1012, 1498, 1499, 1500, 1501, 1600, 1999, 2000,
        2001, 2100, 2200, 2201, 2202, 2999, 3000,
    ]);

    let recombined = build_access_plan(&metas, &deletes)
        .into_overall_row_selection(&metas)
        .unwrap()
        .unwrap();

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
    assert_eq!(recombined, expected);
}

#[test]
fn access_plan_leaves_untouched_row_groups_unselected() {
    use datafusion::datasource::physical_plan::parquet::RowGroupAccess;

    let metas = row_groups_of(&[1000, 500, 500]);
    // Deletes touch row groups 0 and 2 but not row group 1 (rows 1000..1500).
    let deletes = RoaringTreemap::from_iter([0u64, 999, 1500]);

    let plan = build_access_plan(&metas, &deletes);

    assert!(matches!(plan.inner()[0], RowGroupAccess::Selection(_)));
    assert!(matches!(plan.inner()[1], RowGroupAccess::Scan));
    assert!(matches!(plan.inner()[2], RowGroupAccess::Selection(_)));
}

/// Scenario: Access-plan construction cost does not grow with the row-group count
#[test]
fn access_plan_skips_a_fully_deleted_row_group_and_splits_runs_at_its_bounds() {
    use datafusion::datasource::physical_plan::parquet::RowGroupAccess;

    let metas = row_groups_of(&[4, 4, 4]);
    // One run spans the end of row group 0, all of row group 1, and the start of row group 2.
    let deletes = RoaringTreemap::from_iter(2u64..9);

    let plan = build_access_plan(&metas, &deletes);

    assert!(matches!(plan.inner()[1], RowGroupAccess::Skip));
    // The overall selection spans only the scanned row groups 0 and 2.
    let recombined = plan.into_overall_row_selection(&metas).unwrap().unwrap();
    assert_eq!(
        recombined,
        RowSelection::from(vec![
            RowSelector::select(2),
            RowSelector::skip(3),
            RowSelector::select(3),
        ])
    );
}

#[test]
fn access_plan_fully_deleted_file_selects_no_rows() {
    let metas = row_groups_of(&[4]);
    let deletes = RoaringTreemap::from_iter([0u64, 1, 2, 3]);

    let plan = build_access_plan(&metas, &deletes);
    assert!(
        plan.row_group_indexes().is_empty(),
        "no row group of a fully-deleted file is scanned"
    );
}

/// Scenario: Access-plan construction cost does not grow with the row-group count
#[test]
fn access_plan_cost_does_not_grow_with_the_row_group_count() {
    const ROWS: i64 = 3_000_000;
    let deletes: RoaringTreemap = (0..ROWS as u64).step_by(3).collect();
    let fastest_build = |row_group_count: i64| {
        let metas = row_groups_of(&vec![ROWS / row_group_count; row_group_count as usize]);
        (0..3)
            .map(|_| {
                let started = std::time::Instant::now();
                std::hint::black_box(build_access_plan(&metas, &deletes));
                started.elapsed()
            })
            .min()
            .unwrap()
    };

    let one = fastest_build(1);
    let thousand = fastest_build(1000);
    assert!(
        thousand < one * 2,
        "1000 row groups took {thousand:?}, one row group {one:?}"
    );
}

#[test]
fn reads_and_filters_delete_positions_by_file_path() {
    let target = "s3://bucket/db/t/data/f1.parquet";
    let other = "s3://bucket/db/t/data/f2.parquet";
    let bytes =
        positional_delete_file_bytes(&[(target, 3), (target, 7), (other, 5), (target, 9)], None);

    let by_data_file = current_thread_runtime().block_on(async move {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = put_object(store.as_ref(), "db/t/deletes/d.parquet", bytes).await;
        read_positions(store, meta, Some(DEFAULT_METADATA_SIZE_HINT), &[target], 1).await
    });

    assert_eq!(
        by_data_file
            .get(target)
            .unwrap()
            .iter()
            .collect::<Vec<u64>>(),
        vec![3, 7, 9]
    );
    assert!(
        !by_data_file.contains_key(other),
        "an unassigned sibling file must not appear in the position map"
    );
}

/// Scenario: An unapplicable delete file is rejected with a clean error (read-time backstop)
#[test]
fn only_the_iceberg_equality_delete_mechanism_is_refused() {
    const SECRET: &str = "SECRETKEY";
    let secrets = [SECRET.to_string()];
    let table_root = format!("s3://{SECRET}@bucket/db/t");
    let data_file = "data/f1.parquet";

    let positional = DeleteMechanism::IcebergPositionalDelete {
        path: "data/pos-delete.parquet".to_string(),
        size: 10,
    };
    let applied =
        applicable_delete_mechanism(&positional, data_file, &table_root, &secrets).unwrap();
    assert!(
        matches!(
            applied,
            ApplicableDelete::PositionalDeleteFile {
                path: "data/pos-delete.parquet",
                size: 10
            }
        ),
        "the positional-delete variant yields the path and size the delete read consumes"
    );

    let delta = DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    };
    let applied = applicable_delete_mechanism(&delta, data_file, &table_root, &secrets).unwrap();
    let ApplicableDelete::DeltaDeletionVector(vector) = applied else {
        panic!("a Delta deletion vector reaches the delete-application pipeline, not a refusal");
    };
    assert!(
        vector.sidecar_url().is_some(),
        "the vector arrives already resolved to the sidecar the shard must fetch"
    );

    let equality = DeleteMechanism::IcebergEqualityDelete {
        path: format!("s3://{SECRET}@bucket/db/t/data/eq-delete.parquet"),
        size: 10,
    };
    let err = applicable_delete_mechanism(&equality, data_file, &table_root, &secrets)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("equality delete"),
        "names the mechanism: {err}"
    );
    assert!(
        err.contains("eq-delete.parquet"),
        "names the offending delete file: {err}"
    );
    assert!(!err.contains(SECRET), "must not leak credentials: {err}");

    let puffin = DeleteMechanism::IcebergPuffinDeletionVector {
        path: "data/dv.puffin".to_string(),
        size: 100,
        content_offset: 4,
        content_size_in_bytes: 44,
        record_count: 2,
    };
    let applied = applicable_delete_mechanism(&puffin, data_file, &table_root, &secrets).unwrap();
    let ApplicableDelete::IcebergDeletionVector(vector) = applied else {
        panic!("an Iceberg deletion vector reaches the delete-application pipeline, not a refusal");
    };
    assert_eq!(
        (vector.path.as_str(), vector.range),
        ("data/dv.puffin", 4..48),
        "the vector arrives resolved to the blob range the shard must fetch"
    );
}

/// Statistics are omitted entirely when `with_stats` is false.
fn row_group_with_file_path_stats(
    min: Option<&str>,
    max: Option<&str>,
    with_stats: bool,
) -> RowGroupMetaData {
    use parquet::data_type::ByteArray;
    let schema_descr = get_test_schema_descr();
    let ptrs = schema_descr.columns();
    let col0_builder = ColumnChunkMetaData::builder(ptrs[0].clone());
    let col0_builder = if with_stats {
        col0_builder.set_statistics(Statistics::byte_array(
            min.map(ByteArray::from),
            max.map(ByteArray::from),
            None,
            Some(0),
            false,
        ))
    } else {
        col0_builder
    };
    let col0 = col0_builder.build().unwrap();
    let col1 = ColumnChunkMetaData::builder(ptrs[1].clone())
        .build()
        .unwrap();
    build_test_row_group_meta(schema_descr.clone(), vec![col0, col1], 4, 0)
}

#[test]
fn pruning_is_range_based() {
    let rg = row_group_with_file_path_stats(
        Some("s3://b/data/f2.parquet"),
        Some("s3://b/data/f5.parquet"),
        true,
    );

    let inside = HashSet::from(["s3://b/data/f3.parquet".to_string()]);
    assert!(
        delete_row_group_may_match(&rg, 0, &inside),
        "a path within [min, max] must be decoded"
    );

    let before = HashSet::from(["s3://b/data/f1.parquet".to_string()]);
    assert!(
        !delete_row_group_may_match(&rg, 0, &before),
        "a path strictly before min must be pruned"
    );

    let after = HashSet::from(["s3://b/data/f9.parquet".to_string()]);
    assert!(
        !delete_row_group_may_match(&rg, 0, &after),
        "a path strictly after max must be pruned"
    );

    // Straddling but neither inside: pruned.
    let straddle = HashSet::from([
        "s3://b/data/f1.parquet".to_string(),
        "s3://b/data/f9.parquet".to_string(),
    ]);
    assert!(
        !delete_row_group_may_match(&rg, 0, &straddle),
        "paths straddling but outside the range must be pruned"
    );
}

#[test]
fn absent_statistics_are_never_pruned() {
    let assigned = HashSet::from(["s3://b/data/f1.parquet".to_string()]);

    let no_stats = row_group_with_file_path_stats(None, None, false);
    assert!(
        delete_row_group_may_match(&no_stats, 0, &assigned),
        "a row group with no file_path statistics must be decoded"
    );

    let empty_bounds = row_group_with_file_path_stats(None, None, true);
    assert!(
        delete_row_group_may_match(&empty_bounds, 0, &assigned),
        "a row group whose min/max are unset must be decoded"
    );
}

#[test]
fn truncated_bounds_keep_range_valid() {
    let rg = row_group_with_file_path_stats(
        Some("s3://bucket/data/f"),
        Some("s3://bucket/data/g"),
        true,
    );
    let assigned = HashSet::from(["s3://bucket/data/file_00042.parquet".to_string()]);
    assert!(
        delete_row_group_may_match(&rg, 0, &assigned),
        "a path inside truncated bounds must be decoded (range, not equality)"
    );
}

#[test]
fn reads_multi_row_group_delete_file_correctly() {
    // Sorted by (file_path, pos) as Iceberg requires, one data file per row group.
    let f1 = "s3://bucket/db/t/data/f1.parquet";
    let f2 = "s3://bucket/db/t/data/f2.parquet";
    let f3 = "s3://bucket/db/t/data/f3.parquet";
    let bytes = positional_delete_file_bytes(
        &[(f1, 10), (f1, 20), (f2, 5), (f2, 15), (f3, 7), (f3, 30)],
        Some(2),
    );

    let by_data_file = current_thread_runtime().block_on(async move {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = put_object(store.as_ref(), "db/t/deletes/multi.parquet", bytes).await;
        read_positions(store, meta, Some(DEFAULT_METADATA_SIZE_HINT), &[f2], 1).await
    });

    assert_eq!(
        by_data_file.get(f2).unwrap().iter().collect::<Vec<u64>>(),
        vec![5, 15],
        "the assigned file's positions survive the pruned read"
    );
    assert!(!by_data_file.contains_key(f1));
    assert!(!by_data_file.contains_key(f3));
}

/// Scenario: A delete file's row groups are read as separate requests
#[test]
fn delete_file_row_groups_are_read_concurrently_without_the_row_column() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let mut fields = delete_file_fields();
    fields
        .push(Field::new("row", DataType::Utf8, true).with_metadata(field_id_meta(2_147_483_544)));
    let f1 = "s3://bucket/db/t/data/f1.parquet";
    let f2 = "s3://bucket/db/t/data/f2.parquet";
    let f3 = "s3://bucket/db/t/data/f3.parquet";
    let payload = "x".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(vec![f1, f1, f2, f2, f3, f3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 5, 15, 7, 30])),
            Arc::new(StringArray::from(vec![payload.as_str(); 6])),
        ],
    )
    .unwrap();
    let buf = parquet_bytes(batch, Some(2));
    let row_column_chunks: Vec<Range<u64>> = SerializedFileReader::new(Bytes::from(buf.clone()))
        .unwrap()
        .metadata()
        .row_groups()
        .iter()
        .map(|row_group| {
            let (start, len) = row_group.column(2).byte_range();
            start..start + len
        })
        .collect();
    assert_eq!(row_column_chunks.len(), 3);
    let file_size = buf.len() as u64;
    // A hint covering exactly the footer leaves every row group outside the footer bytes.
    let metadata_len = u32::from_le_bytes(buf[buf.len() - 8..buf.len() - 4].try_into().unwrap());
    let footer_len = metadata_len as usize + 8;

    let (by_data_file, ranges, max_in_flight) = current_thread_runtime().block_on(async move {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = put_object(inner.as_ref(), "db/t/deletes/multi.parquet", buf).await;
        let counting = Arc::new(CountingObjectStore::new(inner));
        let store = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let by_data_file = read_positions(store, meta, Some(footer_len), &[f1, f2, f3], 2).await;
        (
            by_data_file,
            counting.ranges_of("multi.parquet"),
            counting.max_in_flight(),
        )
    });

    let positions_of = |path: &str| by_data_file[path].iter().collect::<Vec<u64>>();
    assert_eq!(positions_of(f1), vec![10, 20]);
    assert_eq!(positions_of(f2), vec![5, 15]);
    assert_eq!(positions_of(f3), vec![7, 30]);
    assert_eq!(
        max_in_flight, 2,
        "row groups are read concurrently, one permit each, up to the two-permit budget"
    );
    let (footer, row_group_reads) = ranges.split_first().expect("the footer is read first");
    assert!(
        matches!(footer, Some(GetRange::Bounded(range)) if range.end == file_size),
        "a footer within the size hint is one ranged GET: {footer:?}"
    );
    assert_eq!(
        row_group_reads.len(),
        3,
        "one GET per row group: {row_group_reads:?}"
    );
    for range in row_group_reads.iter().cloned() {
        let Some(GetRange::Bounded(range)) = range else {
            continue;
        };
        assert!(
            row_column_chunks
                .iter()
                .all(|chunk| range.end <= chunk.start || chunk.end <= range.start),
            "read {range:?} overlaps a `row` column chunk {row_column_chunks:?}"
        );
    }
}

/// Scenario: A delete file's row groups are read as separate requests
#[test]
fn a_delete_file_within_the_size_hint_costs_one_get_in_total() {
    let f1 = "s3://bucket/db/t/data/f1.parquet";
    let f2 = "s3://bucket/db/t/data/f2.parquet";
    let bytes = positional_delete_file_bytes(&[(f1, 1), (f1, 4), (f2, 2), (f2, 8)], Some(2));

    let (by_data_file, reads) = current_thread_runtime().block_on(async move {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = put_object(inner.as_ref(), "db/t/deletes/small.parquet", bytes).await;
        let counting = Arc::new(CountingObjectStore::new(inner));
        let store = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let by_data_file =
            read_positions(store, meta, Some(DEFAULT_METADATA_SIZE_HINT), &[f1, f2], 1).await;
        (by_data_file, counting.reads_of("small.parquet"))
    });

    assert_eq!(by_data_file[f1].iter().collect::<Vec<u64>>(), vec![1, 4]);
    assert_eq!(by_data_file[f2].iter().collect::<Vec<u64>>(), vec![2, 8]);
    assert_eq!(
        reads, 1,
        "the footer GET returns the whole file, so both row groups decode from it"
    );
}

/// Scenario: Delete preparation spreads across the scan runtime's workers
#[test]
fn spawned_delete_tasks_run_on_more_than_one_worker() {
    use std::time::{Duration, Instant};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let running = Arc::new(AtomicUsize::new(0));
    let both_ran_together = rt
        .block_on(join_spawned((0..2).map(|_| {
            let running = Arc::clone(&running);
            async move {
                running.fetch_add(1, Ordering::SeqCst);
                // Spins without yielding, like CPU-bound decoding.
                let deadline = Instant::now() + Duration::from_secs(5);
                while running.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
                    std::hint::spin_loop();
                }
                Ok(running.load(Ordering::SeqCst) == 2)
            }
        })))
        .unwrap();
    assert_eq!(both_ran_together, vec![true, true]);
}

/// Scenario: A shard fetches only its own blobs of a shared Puffin file within the connection budget
#[test]
fn merged_puffin_requests_respect_the_gap_and_the_size_cap() {
    let ranges = [0..10, 15..25, 30..40, 100..110, 115..200];
    assert_eq!(
        merge_blob_ranges(&ranges, 10, 30),
        vec![(0..25, 2), (30..40, 1), (100..110, 1), (115..200, 1)]
    );
}

/// Scenario: Row-position delete state is reserved in the memory pool
#[test]
fn phase_a_reserves_the_serialized_size_of_the_sets_it_returns() {
    let data_file = "memory:///db/t/data/x.parquet";
    let first = positional_delete_file_bytes(&[(data_file, 3), (data_file, 7)], None);
    let second = positional_delete_file_bytes(&[(data_file, 7), (data_file, 9)], None);
    let (puffin_file, [puffin_a, _]) = puffin_file_with_two_vectors();
    // Two delete files and a deletion vector for one data file exercise both set merges.
    let files = vec![entry_with(
        "data/x.parquet",
        vec![
            DeleteMechanism::IcebergPositionalDelete {
                path: "deletes/d1.parquet".to_string(),
                size: first.len() as u64,
            },
            DeleteMechanism::IcebergPositionalDelete {
                path: "deletes/d2.parquet".to_string(),
                size: second.len() as u64,
            },
            puffin_a,
        ],
    )];
    let reservation = unbounded_reservation();
    let rt = current_thread_runtime();
    let positions = rt.block_on(async {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_object(store.as_ref(), "db/t/deletes/d1.parquet", first).await;
        put_object(store.as_ref(), "db/t/deletes/d2.parquet", second).await;
        put_object(store.as_ref(), "db/t/deletes/dv.puffin", puffin_file).await;
        collect_positions_within(
            &files,
            "memory:///db/t",
            2,
            &store,
            Arc::clone(&reservation),
        )
        .await
        .unwrap()
    });

    let set = &positions[data_file];
    assert!(
        [3, 7, 9].into_iter().all(|pos| set.contains(pos)),
        "{set:?}"
    );
    assert_eq!(
        reservation.size(),
        set.serialized_size(),
        "the reservation equals the merged set's size, with no per-position copy left in it"
    );
}

/// Scenario: Row-position delete state is reserved in the memory pool
#[test]
fn phase_a_fails_as_soon_as_its_sets_outgrow_the_pool() {
    const ROWS: i64 = 300_000;
    let data_file = "memory:///db/t/data/x.parquet";
    let rows: Vec<(&str, i64)> = (0..ROWS).map(|pos| (data_file, pos)).collect();
    let bytes = positional_delete_file_bytes(&rows, None);
    let files = vec![entry_with(
        "data/x.parquet",
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: "deletes/d.parquet".to_string(),
            size: bytes.len() as u64,
        }],
    )];
    let set_bytes = (0..ROWS as u64)
        .collect::<RoaringTreemap>()
        .serialized_size();
    let rt = current_thread_runtime();
    let collect_within = |pool_bytes: usize| {
        rt.block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            put_object(store.as_ref(), "db/t/deletes/d.parquet", bytes.clone()).await;
            collect_positions_within(
                &files,
                "memory:///db/t",
                2,
                &store,
                reservation_within(pool_bytes),
            )
            .await
        })
    };

    assert!(
        collect_within(set_bytes).is_ok(),
        "decoding {} bytes of positions fits a pool of the final set's {set_bytes} bytes",
        ROWS * 8
    );
    let error = collect_within(set_bytes / 2).expect_err("the set outgrows half its size");
    assert!(
        error.to_string().contains("does not fit the memory pool"),
        "{error}"
    );
}

/// Scenario: A delete file's row groups are read as separate requests
#[test]
fn footer_bytes_of_many_small_delete_files_leave_the_pool_to_the_sets() {
    const DELETE_FILES: usize = 40;
    let mut files = Vec::new();
    let mut payloads = Vec::new();
    for i in 0..DELETE_FILES {
        let data_file = format!("memory:///db/t/data/f{i:02}.parquet");
        let bytes = positional_delete_file_bytes(&[(data_file.as_str(), 5)], None);
        files.push(entry_with(
            &format!("data/f{i:02}.parquet"),
            vec![DeleteMechanism::IcebergPositionalDelete {
                path: format!("deletes/d{i:02}.parquet"),
                size: bytes.len() as u64,
            }],
        ));
        payloads.push((format!("db/t/deletes/d{i:02}.parquet"), bytes));
    }
    let set_bytes = [5_u64]
        .into_iter()
        .collect::<RoaringTreemap>()
        .serialized_size();
    // Half the files' footer bytes would fill this pool, while all the sets fit it many times.
    let pool_bytes = (DELETE_FILES / 2) * payloads[0].1.len() + 1;
    assert!(DELETE_FILES * set_bytes * 5 < pool_bytes);

    let rt = current_thread_runtime();
    let positions = rt
        .block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            for (path, bytes) in &payloads {
                put_object(store.as_ref(), path, bytes.clone()).await;
            }
            collect_positions_within(
                &files,
                "memory:///db/t",
                4,
                &store,
                reservation_within(pool_bytes),
            )
            .await
        })
        .expect("footer bytes never hold pool room the position sets need");
    assert_eq!(positions.len(), DELETE_FILES);
}

/// Phase A's positional-delete steps over one delete file.
async fn read_positions(
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metadata_size_hint: Option<usize>,
    assigned: &[&str],
    permits: usize,
) -> HashMap<String, RoaringTreemap> {
    let ctx = Arc::new(DeleteReadContext {
        store,
        table_root: String::new(),
        assigned: assigned.iter().map(|path| path.to_string()).collect(),
        secrets: Arc::from([]),
        limiter: Arc::new(Semaphore::new(permits)),
        reservation: unbounded_reservation(),
        metadata_size_hint,
    });
    let opened = ctx.open_delete_file(meta).await.unwrap();
    let row_groups = join_spawned(opened.row_group_reads(&ctx)).await.unwrap();
    let positions = ctx
        .union_by_data_file(row_groups.into_iter().flatten())
        .await
        .unwrap();
    assert_eq!(
        ctx.reservation.size(),
        positions
            .values()
            .map(RoaringTreemap::serialized_size)
            .sum::<usize>(),
        "the reservation holds the position sets and nothing else"
    );
    positions
}

/// Stores `payload` at `path` and returns the metadata a delete read is handed.
async fn put_object(
    store: &dyn ObjectStore,
    path: &str,
    payload: impl Into<PutPayload>,
) -> ObjectMeta {
    let location = StorePath::from(path);
    let payload = payload.into();
    let size = payload.content_length() as u64;
    store.put(&location, payload).await.unwrap();
    ObjectMeta {
        location,
        last_modified: chrono::Utc.timestamp_nanos(0),
        size,
        e_tag: None,
        version: None,
    }
}

/// DataFusion's default `metadata_size_hint`, which the scan's Parquet format keeps.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;

fn unbounded_reservation() -> Arc<MemoryReservation> {
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    Arc::new(MemoryConsumer::new("test").register(&pool))
}

fn reservation_within(pool_bytes: usize) -> Arc<MemoryReservation> {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
    Arc::new(MemoryConsumer::new("test").register(&pool))
}

fn entry_with(path: &str, deletes: Vec<DeleteMechanism>) -> FileEntry {
    FileEntry {
        path: path.to_string(),
        size: 1,
        deletes,
        partition_values: Default::default(),
    }
}

/// Phase A over `files`, under a limiter of `permits`.
async fn collect_positions(
    files: &[FileEntry],
    table_root: &str,
    permits: usize,
    store: &Arc<dyn ObjectStore>,
) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
    collect_positions_within(files, table_root, permits, store, unbounded_reservation()).await
}

async fn collect_positions_within(
    files: &[FileEntry],
    table_root: &str,
    permits: usize,
    store: &Arc<dyn ObjectStore>,
    reservation: Arc<MemoryReservation>,
) -> Result<HashMap<String, RoaringTreemap>, UdfError> {
    ShardDeletes {
        files,
        table_root,
        secrets: Arc::from([]),
        limiter: Arc::new(Semaphore::new(permits)),
        reservation,
        metadata_size_hint: Some(DEFAULT_METADATA_SIZE_HINT),
    }
    .collect_positions(store)
    .await
}

fn field_id_meta(id: i32) -> HashMap<String, String> {
    HashMap::from([(
        super::super::PARQUET_FIELD_ID_META_KEY.to_string(),
        id.to_string(),
    )])
}

fn delete_file_fields() -> Vec<Field> {
    vec![
        Field::new("file_path", DataType::Utf8, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_FILE_PATH)),
        Field::new("pos", DataType::Int64, false)
            .with_metadata(field_id_meta(FIELD_ID_POSITIONAL_DELETE_POS)),
    ]
}

fn positional_delete_file_bytes(
    rows: &[(&str, i64)],
    rows_per_row_group: Option<usize>,
) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(delete_file_fields())),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(path, _)| *path).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, pos)| *pos).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    parquet_bytes(batch, rows_per_row_group)
}

/// Real Delta writer output for the `table-with-dv-small` fixture.
const DV_SIDECAR_BODY: &[u8] = include_bytes!(
    "../../../../scripts/unity/fixtures/table-with-dv-small/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin"
);
const DV_LOGGED_PATH: &str = "vBn[lx{q8@P<9BNH/isA";
const DV_SIDECAR_NAME: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

fn logged_deletion_vector() -> DeleteMechanism {
    DeleteMechanism::DeltaDeletionVector {
        storage: DeltaDeletionVectorStorage::UuidRelative,
        path_or_inline_dv: DV_LOGGED_PATH.to_string(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality: 2,
    }
}

/// The sidecar's one vector is also a valid Iceberg `deletion-vector-v1` blob: both formats share
/// the layout, so a Puffin file of two copies after the 4-byte magic holds two vectors.
fn puffin_file_with_two_vectors() -> (Vec<u8>, [DeleteMechanism; 2]) {
    let blob = &DV_SIDECAR_BODY[1..45];
    let file = [b"PFA1".as_slice(), blob, blob].concat();
    let vector = |content_offset| DeleteMechanism::IcebergPuffinDeletionVector {
        path: "deletes/dv.puffin".to_string(),
        size: file.len() as u64,
        content_offset,
        content_size_in_bytes: blob.len() as u64,
        record_count: 2,
    };
    let vectors = [vector(4), vector(4 + blob.len() as u64)];
    (file, vectors)
}

/// Scenario: All delete mechanisms converge on one position map and one access-plan pipeline
/// Scenario: A shard fetches only its own blobs of a shared Puffin file within the connection budget
#[test]
fn all_delete_mechanisms_converge_on_one_position_map() {
    const TABLE_ROOT: &str = "memory:///db/t";
    let iceberg_file = "memory:///db/t/data/iceberg.parquet";
    let delete_file_bytes =
        positional_delete_file_bytes(&[(iceberg_file, 3), (iceberg_file, 7)], None);
    let delete_file_size = delete_file_bytes.len() as u64;
    let (puffin_file, [puffin_a, puffin_b]) = puffin_file_with_two_vectors();

    let files = vec![
        entry_with(
            "data/iceberg.parquet",
            vec![DeleteMechanism::IcebergPositionalDelete {
                path: "deletes/d.parquet".to_string(),
                size: delete_file_size,
            }],
        ),
        entry_with("data/delta_a.parquet", vec![logged_deletion_vector()]),
        entry_with("data/delta_b.parquet", vec![logged_deletion_vector()]),
        entry_with("data/puffin_a.parquet", vec![puffin_a]),
        entry_with("data/puffin_b.parquet", vec![puffin_b]),
        entry_with("data/no_deletes.parquet", Vec::new()),
    ];

    let rt = current_thread_runtime();
    let (positions, reads, puffin_reads) = rt.block_on(async move {
        let inner = Arc::new(InMemory::new());
        put_object(inner.as_ref(), "db/t/deletes/dv.puffin", puffin_file).await;
        put_object(inner.as_ref(), "db/t/deletes/d.parquet", delete_file_bytes).await;
        let sidecar = format!("db/t/{DV_SIDECAR_NAME}");
        put_object(inner.as_ref(), &sidecar, DV_SIDECAR_BODY).await;
        let counting = Arc::new(CountingObjectStore::new(inner));
        let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let positions = collect_positions(&files, TABLE_ROOT, 2, &store)
            .await
            .unwrap();
        (
            positions,
            counting.reads_of(DV_SIDECAR_NAME),
            counting.reads_of("dv.puffin"),
        )
    });

    assert_eq!(
        positions
            .get(iceberg_file)
            .unwrap()
            .iter()
            .collect::<Vec<u64>>(),
        vec![3, 7],
        "the Iceberg mechanism's positions land in the shared map"
    );
    let delta_a = positions
        .get("memory:///db/t/data/delta_a.parquet")
        .unwrap();
    let delta_b = positions
        .get("memory:///db/t/data/delta_b.parquet")
        .unwrap();
    assert_eq!(
        delta_a.len(),
        2,
        "the decoded vector deletes exactly the rows its cardinality declares"
    );
    assert_eq!(
        delta_a, delta_b,
        "each data file sharing a sidecar gets that sidecar's own positions"
    );
    assert!(
        !positions.contains_key("memory:///db/t/data/no_deletes.parquet"),
        "a delete-free entry contributes nothing to the map"
    );
    assert_eq!(
        reads, 1,
        "a sidecar shared by several data files is fetched once for the whole shard"
    );
    for data_file in ["puffin_a", "puffin_b"] {
        assert_eq!(
            positions.get(&format!("memory:///db/t/data/{data_file}.parquet")),
            Some(delta_a),
            "{data_file} gets its own blob's positions, which equal the sidecar's"
        );
    }
    assert_eq!(
        puffin_reads, 1,
        "adjacent blobs of one Puffin file are fetched in one request"
    );
}

/// Scenario: A shard fetches only its own blobs of a shared Puffin file within the connection budget
#[test]
fn distant_puffin_blobs_are_fetched_by_range_each_under_its_own_permit() {
    const TABLE_ROOT: &str = "memory:///db/t";
    let blob = &DV_SIDECAR_BODY[1..45];
    let gap = vec![0_u8; 2 * OBJECT_STORE_COALESCE_DEFAULT as usize];
    let puffin_file = [b"PFA1".as_slice(), blob, &gap, blob].concat();
    let blob_len = blob.len() as u64;
    let first = 4..4 + blob_len;
    let second_offset = 4 + blob_len + gap.len() as u64;
    let second = second_offset..second_offset + blob_len;
    let vector = |content_offset| DeleteMechanism::IcebergPuffinDeletionVector {
        path: "deletes/dv.puffin".to_string(),
        size: puffin_file.len() as u64,
        content_offset,
        content_size_in_bytes: blob_len,
        record_count: 2,
    };
    let files = vec![
        entry_with("data/a.parquet", vec![vector(first.start)]),
        entry_with("data/b.parquet", vec![vector(second.start)]),
    ];

    let rt = current_thread_runtime();
    let (positions, mut ranges, max_in_flight) = rt.block_on(async move {
        let inner = Arc::new(InMemory::new());
        put_object(inner.as_ref(), "db/t/deletes/dv.puffin", puffin_file).await;
        let counting = Arc::new(CountingObjectStore::new(inner));
        let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let positions = collect_positions(&files, TABLE_ROOT, 1, &store)
            .await
            .unwrap();
        (
            positions,
            counting.ranges_of("dv.puffin"),
            counting.max_in_flight(),
        )
    });

    for data_file in ["a", "b"] {
        assert_eq!(
            positions[&format!("{TABLE_ROOT}/data/{data_file}.parquet")].len(),
            2,
            "{data_file} gets its own blob's positions"
        );
    }
    ranges.sort_by_key(|range| match range {
        Some(GetRange::Bounded(range)) => range.start,
        _ => u64::MAX,
    });
    assert_eq!(
        ranges,
        vec![
            Some(GetRange::Bounded(first)),
            Some(GetRange::Bounded(second))
        ],
        "only the two blob ranges are fetched, never the gap between them"
    );
    assert_eq!(
        max_in_flight, 1,
        "a one-permit budget admits one Puffin range request at a time"
    );
}

#[test]
fn an_unapplicable_mechanism_fails_the_shard_before_any_read() {
    let files = vec![
        entry_with("data/delta.parquet", vec![logged_deletion_vector()]),
        entry_with(
            "data/iceberg.parquet",
            vec![DeleteMechanism::IcebergEqualityDelete {
                path: "deletes/eq.parquet".to_string(),
                size: 10,
            }],
        ),
    ];

    let rt = current_thread_runtime();
    let (err, reads) = rt.block_on(async move {
        let counting = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let err = collect_positions(&files, "memory:///db/t", 2, &store)
            .await
            .unwrap_err();
        (err.to_string(), counting.total_reads())
    });

    assert!(
        err.contains("equality delete"),
        "the shard fails on the unapplicable mechanism: {err}"
    );
    assert_eq!(reads, 0, "no body is fetched once the backstop has refused");
}

/// Records every forwarded data read, with its range, so a test can count storage trips per
/// object. Each read yields once while in flight, so concurrent reads overlap observably.
#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    reads: std::sync::Mutex<Vec<(String, Option<GetRange>)>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: std::sync::Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        }
    }

    fn reads_of(&self, name: &str) -> usize {
        self.ranges_of(name).len()
    }

    fn ranges_of(&self, name: &str) -> Vec<Option<GetRange>> {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(location, _)| location.ends_with(name))
            .map(|(_, range)| range.clone())
            .collect()
    }

    fn total_reads(&self) -> usize {
        self.reads.lock().unwrap().len()
    }

    fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if options.head {
            return self.inner.get_opts(location, options).await;
        }
        self.reads
            .lock()
            .unwrap()
            .push((location.to_string(), options.range.clone()));
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let result = self.inner.get_opts(location, options).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
}
