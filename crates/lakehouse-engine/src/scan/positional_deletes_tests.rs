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
/// `build_deletes_row_selection` (tag `v0.10.0`), adapted only to feed a
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
/// TWO data files (the `partition` granularity shape) is bucketed by
/// `file_path`, restricted to the assigned data files. Only the assigned
/// file's `pos` values survive; the sibling file (absent from the assigned
/// set) is filtered out. Columns are located by Iceberg reserved field-id,
/// and the read never issues a HEAD (the size is supplied on the `ObjectMeta`).
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
        // Only `target` is assigned to this shard; `other` is a sibling file.
        let assigned = HashSet::from([target.to_string()]);
        read_delete_file_positions(store, meta, &assigned, &[])
            .await
            .unwrap()
    });

    // Only f1's positions (3, 7, 9) are bucketed; f2's position (5) is
    // filtered out because f2 is not in the assigned set.
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

/// Build a single-row-group meta whose `file_path` column (index 0) carries
/// the given byte-array min/max statistics (or no statistics when the tuple
/// is `(None, None)` AND `with_stats` is false).
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

/// Task 3: pruning is range-based. A row group is decoded when an assigned
/// path falls within the `[min, max]` byte range, and pruned only when EVERY
/// assigned path sorts strictly outside it — on either side.
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

    // Two assigned paths straddling the range but neither inside it: the row
    // group cannot hold either file's deletes, so it is pruned.
    let straddle = HashSet::from([
        "s3://b/data/f1.parquet".to_string(),
        "s3://b/data/f9.parquet".to_string(),
    ]);
    assert!(
        !delete_row_group_may_match(&rg, 0, &straddle),
        "paths straddling but outside the range must be pruned"
    );
}

/// Task 3: a row group with absent statistics — no `Statistics` at all, or a
/// `Statistics` whose min/max are unset — MUST be decoded (overlap cannot be
/// ruled out).
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

/// Task 3: Parquet truncates string statistics (min DOWN, max UP), so a
/// row group's real paths can be strictly inside its truncated `[min, max]`.
/// A byte-wise RANGE test still decodes it correctly, where an
/// `min == max == target` equality shortcut would wrongly prune it (min, max
/// and target are three distinct strings here).
#[test]
fn truncated_bounds_keep_range_valid() {
    // Truncated min "…/f" sorts below every real "…/file_*"; truncated max
    // "…/g" sorts above every real "…/file_*".
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

/// Task 3: reading a multi-row-group delete file whose row groups carry
/// disjoint `file_path` ranges yields EXACTLY the assigned file's positions —
/// the pruned read equals an unpruned read (correctness-preserving).
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

    // Rows sorted by (file_path, pos) as Iceberg requires; two rows per row
    // group ⇒ each data file lands in its own row group with a tight range.
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
        // Only f2 is assigned: its row group must be decoded, the f1 and f3
        // row groups pruned. The result must still be exactly f2's positions.
        let assigned = HashSet::from([f2.to_string()]);
        read_delete_file_positions(store, meta, &assigned, &[])
            .await
            .unwrap()
    });

    assert_eq!(
        by_data_file.get(f2).unwrap().iter().collect::<Vec<u64>>(),
        vec![5, 15],
        "the assigned file's positions survive the pruned read"
    );
    assert!(!by_data_file.contains_key(f1));
    assert!(!by_data_file.contains_key(f3));
}
