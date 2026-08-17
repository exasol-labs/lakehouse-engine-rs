use super::*;
use crate::scan::spec::DeltaDeletionVectorStorage;
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

/// The read-time backstop dispatches on the delete mechanism's own variant, and
/// exactly two reach the delete-application pipeline: an Iceberg positional-delete
/// file yields the payload the delete read needs, a Delta deletion vector yields a
/// resolved vector, and the two Iceberg mechanisms this engine never applies are
/// refused with a clean, credential-redacted error naming the offending delete FILE.
#[test]
fn only_iceberg_equality_and_puffin_delete_mechanisms_are_refused() {
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
    let ApplicableDelete::DeletionVector(vector) = applied else {
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
        path: format!("s3://{SECRET}@bucket/db/t/data/dv.puffin"),
        size: 10,
    };
    let err = applicable_delete_mechanism(&puffin, data_file, &table_root, &secrets)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Puffin deletion vector"),
        "names the mechanism: {err}"
    );
    assert!(
        err.contains("dv.puffin"),
        "names the offending delete file: {err}"
    );
    assert!(!err.contains(SECRET), "must not leak credentials: {err}");
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

/// A shard's data-file entry carrying the given delete mechanisms.
fn entry_with(path: &str, deletes: Vec<DeleteMechanism>) -> FileEntry {
    FileEntry {
        path: path.to_string(),
        size: 1,
        deletes,
        partition_values: Default::default(),
    }
}

/// A provider over `files` rooted at `table_root`, holding an N-permit read budget.
/// Built field-by-field because Phase A needs none of the session-bound state
/// [`PositionalDeleteScanTable::new`] derives from a storage backend.
fn scan_table_over(
    files: Vec<FileEntry>,
    table_root: &str,
    permits: usize,
) -> PositionalDeleteScanTable {
    PositionalDeleteScanTable {
        object_store_url: ObjectStoreUrl::parse("memory://").unwrap(),
        schema: PartitionedScanSchema::split(Arc::new(arrow::datatypes::Schema::empty()), &[])
            .expect("an empty schema declares no partition column"),
        use_field_id_adapter: false,
        field_id_resolution: FieldIdResolution {
            name_mapping: Vec::new(),
            declared_physical_names: HashMap::new(),
            defaults: HashMap::new(),
        },
        files,
        table_root: table_root.to_string(),
        secrets: Vec::new(),
        format: Arc::new(int96_coerced_parquet_format()),
        delete_path_read_limiter: Arc::new(Semaphore::new(permits)),
    }
}

/// Serialize an Iceberg positional-delete file naming `(data file, position)` pairs.
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

/// The 45-byte deletion-vector container Delta wrote for the vendored
/// `table-with-dv-small` fixture, and the descriptor its commit logs for it.
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

/// A shard mixing Iceberg positional-delete files with Delta deletion vectors
/// accumulates BOTH into one map from data-file path to deleted positions, and the
/// sidecar two data files share is fetched exactly once for the whole shard.
#[test]
fn both_delete_mechanisms_converge_on_one_position_map() {
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    const TABLE_ROOT: &str = "memory:///db/t";
    let iceberg_file = "memory:///db/t/data/iceberg.parquet";
    let delete_file_bytes = positional_delete_file_bytes(&[(iceberg_file, 3), (iceberg_file, 7)]);
    let delete_file_size = delete_file_bytes.len() as u64;

    let table = scan_table_over(
        vec![
            entry_with(
                "data/iceberg.parquet",
                vec![DeleteMechanism::IcebergPositionalDelete {
                    path: "deletes/d.parquet".to_string(),
                    size: delete_file_size,
                }],
            ),
            entry_with("data/delta_a.parquet", vec![logged_deletion_vector()]),
            entry_with("data/delta_b.parquet", vec![logged_deletion_vector()]),
            entry_with("data/no_deletes.parquet", Vec::new()),
        ],
        TABLE_ROOT,
        2,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (positions, reads) = rt.block_on(async move {
        let inner = Arc::new(InMemory::new());
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
        let positions = table.collect_delete_positions(&store).await.unwrap();
        (positions, counting.reads_of(DV_SIDECAR_NAME))
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
}

/// An unapplicable mechanism anywhere in the shard fails the whole shard before a
/// single delete-file body or deletion-vector sidecar is fetched.
#[test]
fn an_unapplicable_mechanism_fails_the_shard_before_any_read() {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    let table = scan_table_over(
        vec![
            entry_with("data/delta.parquet", vec![logged_deletion_vector()]),
            entry_with(
                "data/iceberg.parquet",
                vec![DeleteMechanism::IcebergEqualityDelete {
                    path: "deletes/eq.parquet".to_string(),
                    size: 10,
                }],
            ),
        ],
        "memory:///db/t",
        2,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (err, reads) = rt.block_on(async move {
        let counting = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&counting) as Arc<dyn ObjectStore>;
        let err = table.collect_delete_positions(&store).await.unwrap_err();
        (err.to_string(), counting.total_reads())
    });

    assert!(
        err.contains("equality delete"),
        "the shard fails on the unapplicable mechanism: {err}"
    );
    assert_eq!(reads, 0, "no body is fetched once the backstop has refused");
}

/// An [`ObjectStore`] that records the location of every data read it forwards, so a
/// test can assert how many times a shard actually went to storage for one object.
#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    reads: std::sync::Mutex<Vec<String>>,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn reads_of(&self, name: &str) -> usize {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|location| location.ends_with(name))
            .count()
    }

    fn total_reads(&self) -> usize {
        self.reads.lock().unwrap().len()
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
        if !options.head {
            self.reads.lock().unwrap().push(location.to_string());
        }
        self.inner.get_opts(location, options).await
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

/// The provider from [`scan_table_over`] re-registered under a declared schema and
/// its partition columns, so a test can exercise the split without restating every
/// session-independent field.
fn scan_table_partitioned_by(
    files: Vec<FileEntry>,
    declared: SchemaRef,
    partition_columns: &[String],
) -> PositionalDeleteScanTable {
    PositionalDeleteScanTable {
        schema: PartitionedScanSchema::split(declared, partition_columns)
            .expect("every partition column is declared"),
        ..scan_table_over(files, "memory:///db/t", 1)
    }
}

fn partitioned_declared_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("part", DataType::Int32, true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn entry_in_partition(path: &str, part: Option<&str>) -> FileEntry {
    FileEntry::with_partition_values(
        path,
        1,
        std::collections::BTreeMap::from([("part".to_string(), part.map(str::to_string))]),
    )
}

/// A session whose runtime resolves the `memory://` store the test provider scans.
fn memory_session() -> datafusion::execution::session_state::SessionState {
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    ctx.register_object_store(
        &url::Url::parse("memory://").unwrap(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    ctx.state()
}

/// The registered table's schema is what a query sees, so it stays in DECLARED
/// order even though the scan reads through `file ++ partition` order.
#[test]
fn the_provider_reports_the_declared_schema_while_scanning_the_split_one() {
    let declared = partitioned_declared_schema();
    let table = scan_table_partitioned_by(
        vec![entry_in_partition("data/a.parquet", Some("1"))],
        Arc::clone(&declared),
        &["part".to_string()],
    );

    assert_eq!(TableProvider::schema(&table).as_ref(), declared.as_ref());

    let scan_schema = table.schema.file_source_schema();
    let file_names: Vec<&str> = scan_schema
        .file_schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        file_names,
        ["id", "payload"],
        "the partition column is not read from any data file"
    );
    assert_eq!(
        table.schema.remap_projection(None),
        Some(vec![0, 2, 1]),
        "declared order is restored by the projection alone"
    );
}

/// Each assigned file carries ITS OWN partition value, converted to the column's
/// declared type and positioned to line up with `table_partition_cols`.
#[test]
fn each_partitioned_file_carries_its_own_logged_partition_values() {
    let table = scan_table_partitioned_by(
        vec![
            entry_in_partition("data/a.parquet", Some("1")),
            entry_in_partition("data/b.parquet", Some("2")),
            entry_in_partition("data/default.parquet", None),
        ],
        partitioned_declared_schema(),
        &["part".to_string()],
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let files = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect("no delete needs reading");

    let values: Vec<&datafusion::scalar::ScalarValue> = files
        .iter()
        .map(|f| {
            assert_eq!(
                f.partition_values.len(),
                1,
                "one value per partition column"
            );
            &f.partition_values[0]
        })
        .collect();
    assert_eq!(
        values,
        vec![
            &datafusion::scalar::ScalarValue::Int32(Some(1)),
            &datafusion::scalar::ScalarValue::Int32(Some(2)),
            &datafusion::scalar::ScalarValue::Int32(None),
        ]
    );
}

/// An unpartitioned scan attaches no partition value at all, so its
/// `PartitionedFile`s are what they were before partition materialization existed.
#[test]
fn an_unpartitioned_scan_attaches_no_partition_values() {
    let table = scan_table_over(
        vec![entry_with("data/a.parquet", Vec::new())],
        "memory:///db/t",
        1,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let files = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect("no delete needs reading");

    assert!(files[0].partition_values.is_empty());
}

/// A value the declared type cannot represent fails the scan before any
/// object-store read, on the same terms an unapplicable delete mechanism does.
#[test]
fn an_unrepresentable_partition_value_fails_the_scan() {
    let table = scan_table_partitioned_by(
        vec![entry_in_partition("data/a.parquet", Some("not-a-number"))],
        partitioned_declared_schema(),
        &["part".to_string()],
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = rt
        .block_on(async { table.partitioned_files(&memory_session()).await })
        .expect_err("an unrepresentable partition value must never reach the query");

    let message = error.to_string();
    assert!(message.contains("part"), "{message}");
    assert!(message.contains("Int32"), "{message}");
    assert!(message.contains("not-a-number"), "{message}");
}
