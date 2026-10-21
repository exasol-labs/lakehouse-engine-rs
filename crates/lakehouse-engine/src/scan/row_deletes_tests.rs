use super::*;
use crate::scan::spec::DeltaDeletionVectorStorage;
use async_trait::async_trait;
use chrono::TimeZone;
use object_store::GetRange;
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
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use parquet::arrow::ArrowWriter;

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
    let by_data_file = rt.block_on(async move {
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
        read_positions(store, meta, &[target], 1).await
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
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

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

    // Sorted by (file_path, pos) as Iceberg requires, one data file per row group.
    let f1 = "s3://bucket/db/t/data/f1.parquet";
    let f2 = "s3://bucket/db/t/data/f2.parquet";
    let f3 = "s3://bucket/db/t/data/f3.parquet";
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![f1, f1, f2, f2, f3, f3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 5, 15, 7, 30])),
        ],
    )
    .unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let by_data_file = rt.block_on(async move {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = StorePath::from("db/t/deletes/multi.parquet");
        let size = buf.len() as u64;
        store.put(&location, PutPayload::from(buf)).await.unwrap();
        let meta = ObjectMeta {
            location,
            last_modified: chrono::Utc.timestamp_nanos(0),
            size,
            e_tag: None,
            version: None,
        };
        read_positions(store, meta, &[f2], 1).await
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
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

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
        Field::new("row", DataType::Utf8, true).with_metadata(field_id_meta(2_147_483_544)),
    ]));
    let f1 = "s3://bucket/db/t/data/f1.parquet";
    let f2 = "s3://bucket/db/t/data/f2.parquet";
    let f3 = "s3://bucket/db/t/data/f3.parquet";
    let payload = "x".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![f1, f1, f2, f2, f3, f3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 5, 15, 7, 30])),
            Arc::new(StringArray::from(vec![payload.as_str(); 6])),
        ],
    )
    .unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (by_data_file, ranges, max_in_flight) = rt.block_on(async move {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = StorePath::from("db/t/deletes/multi.parquet");
        let size = buf.len() as u64;
        inner.put(&location, PutPayload::from(buf)).await.unwrap();
        let counting = Arc::new(CountingObjectStore::new(inner));
        let meta = ObjectMeta {
            location,
            last_modified: chrono::Utc.timestamp_nanos(0),
            size,
            e_tag: None,
            version: None,
        };
        let store = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let by_data_file = read_positions(store, meta, &[f1, f2, f3], 2).await;
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
    for range in ranges {
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

/// Phase A's positional-delete steps over one delete file.
async fn read_positions(
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    assigned: &[&str],
    permits: usize,
) -> HashMap<String, RoaringTreemap> {
    let assigned: Arc<HashSet<String>> =
        Arc::new(assigned.iter().map(|path| path.to_string()).collect());
    let secrets: Arc<[String]> = Arc::from([]);
    let limiter = Arc::new(Semaphore::new(permits));
    let opened = open_delete_file(store, meta, &assigned, &secrets, &limiter)
        .await
        .unwrap();
    let row_groups = join_spawned(opened.row_group_reads(&assigned, &secrets, &limiter))
        .await
        .unwrap();
    delete_file_positions(row_groups)
        .await
        .unwrap()
        .into_iter()
        .collect()
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
    ShardDeletes {
        files,
        table_root,
        secrets: Arc::from([]),
        limiter: Arc::new(Semaphore::new(permits)),
    }
    .collect_positions(store)
    .await
}

fn positional_delete_file_bytes(rows: &[(&str, i64)]) -> Vec<u8> {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

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
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
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

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
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
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    const TABLE_ROOT: &str = "memory:///db/t";
    let iceberg_file = "memory:///db/t/data/iceberg.parquet";
    let delete_file_bytes = positional_delete_file_bytes(&[(iceberg_file, 3), (iceberg_file, 7)]);
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (positions, reads, puffin_reads) = rt.block_on(async move {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &StorePath::from("db/t/deletes/dv.puffin"),
                PutPayload::from(puffin_file),
            )
            .await
            .unwrap();
        inner
            .put(
                &StorePath::from("db/t/deletes/d.parquet"),
                PutPayload::from(delete_file_bytes),
            )
            .await
            .unwrap();
        inner
            .put(
                &StorePath::from(format!("db/t/{DV_SIDECAR_NAME}")),
                PutPayload::from_static(DV_SIDECAR_BODY),
            )
            .await
            .unwrap();
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
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStoreExt, PutPayload};

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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (positions, mut ranges, max_in_flight) = rt.block_on(async move {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &StorePath::from("db/t/deletes/dv.puffin"),
                PutPayload::from(puffin_file),
            )
            .await
            .unwrap();
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
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
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
