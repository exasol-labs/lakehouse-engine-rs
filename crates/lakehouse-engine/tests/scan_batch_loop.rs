//! Exasol drives a scalar `run()` once per row and `ctx.next()` is an error in scalar
//! context, so "no dropped shards" holds only if the union of per-row [`run_scan_one`]
//! calls covers every shard. This harness mirrors `run_scan` but does not call it.

mod scan_fixture;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{
    Array, Decimal128Array, Int64Array, ListBuilder, StringArray, StringBuilder, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::{NextPolicy, TestContext};
use exasol_udf_sdk::value::{ExaType, Value};
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NestedMembers, ScanSpec, ScanStorage, StorageBackend,
    StorageProps,
};
use lakehouse_engine::scan::{
    ResolvedScanStorage, build_scan_runtime, read_scan_spec, run_raw_scan_with_session,
    run_scan_one, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn write_parquet_ids(dir: &std::path::Path, name: &str, start: i64, count: i64) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let ids: Vec<i64> = (start..start + count).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn file_size(file_url: &str) -> u64 {
    std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(file_url))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn id_name_emits() -> Vec<ExaType> {
    vec![scan_fixture::decimal(20, 0), scan_fixture::varchar()]
}

fn spec_for_file(file_url: String) -> ScanSpec {
    let size = file_size(&file_url);
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
    }
}

fn write_parquet_categories(dir: &std::path::Path, name: &str, values: &[&str]) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "category",
        DataType::Utf8,
        false,
    )]));
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
        .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn write_parquet_tags(dir: &std::path::Path, name: &str) -> String {
    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("hello");
    tags_builder.values().append_value("world");
    tags_builder.append(true);
    let tags = tags_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags.data_type().clone(), true),
    ]));
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![0i64])), Arc::new(tags)],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn nested_spec_for_file(file_url: String) -> ScanSpec {
    let size = file_size(&file_url);
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "TAGS".into()],
            logical_schema: vec![
                LogicalField {
                    field_id: None,
                    name: "id".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                    initial_default: None,
                    nested: None,
                    physical_name: None,
                },
                LogicalField {
                    field_id: None,
                    name: "tags".to_string(),
                    arrow_type: "utf8".to_string(),
                    nullable: true,
                    initial_default: None,
                    nested: Some(NestedMembers::List { element: None }),
                    physical_name: None,
                },
            ],
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
    }
}

/// The `COUNT(DISTINCT col)` fan-out shape, minus the NULL filter (the fixture has no NULLs).
fn distinct_spec_for_file(file_url: String) -> ScanSpec {
    let size = file_size(&file_url);
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["CATEGORY".into()],
            distinct: true,
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
    }
}

fn row_for_spec(spec: &ScanSpec) -> Vec<Value> {
    vec![
        Value::String(spec.to_common_json()),
        Value::String(ScanSpec::files_json(&spec.files)),
    ]
}

fn local_session(
    spec: &ScanSpec,
    _storage: &ResolvedScanStorage,
    _memory_limit_bytes: u64,
) -> Result<SessionContext, UdfError> {
    Ok(SessionContext::new_with_config(session_config_for_spec(
        spec,
    )))
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        if let Some(dec) = col.as_any().downcast_ref::<Decimal128Array>() {
            for i in 0..b.num_rows() {
                out.push(dec.value(i) as i64);
            }
        } else if let Some(ints) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..b.num_rows() {
                out.push(ints.value(i));
            }
        } else {
            panic!("unexpected id column type: {:?}", col.data_type());
        }
    }
    out.sort_unstable();
    out
}

fn categories_of(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column(0);
        if let Some(v) = col.as_any().downcast_ref::<StringViewArray>() {
            for i in 0..b.num_rows() {
                out.push(v.value(i).to_string());
            }
        } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..b.num_rows() {
                out.push(s.value(i).to_string());
            }
        } else {
            panic!("unexpected category column type: {:?}", col.data_type());
        }
    }
    out.sort_unstable();
    out
}

/// Counts this harness's own runtime builds; a runtime cached inside `run_scan` would not show.
fn counting_build_runtime(threads: usize, built: &AtomicUsize) -> tokio::runtime::Runtime {
    built.fetch_add(1, Ordering::SeqCst);
    build_scan_runtime(threads).expect("build per-row runtime")
}

fn run_one_row(spec: &ScanSpec, emits: &[ExaType], built: &AtomicUsize) -> Vec<RecordBatch> {
    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(row_for_spec(spec)).with_next_policy(NextPolicy::Reject(
            UdfError::User(
                "scalar run() handles exactly one row; ctx.next() must never be called".into(),
            ),
        )),
        emits,
    );
    let reconstituted = read_scan_spec(&ctx).expect("reconstitute row spec");
    let rt = counting_build_runtime(reconstituted.common.df_threads_per_udf, built);
    let storage = scan_fixture::resolved_storage(&reconstituted);
    let result = rt.block_on(run_scan_one(
        &mut ctx,
        reconstituted,
        &storage,
        local_session,
    ));
    result.expect("per-row scan");
    rt.shutdown_timeout(std::time::Duration::from_secs(5));
    ctx.into_batches()
}

fn run_all_rows(specs: &[ScanSpec], emits: &[ExaType], built: &AtomicUsize) -> Vec<RecordBatch> {
    specs
        .iter()
        .flat_map(|s| run_one_row(s, emits, built))
        .collect()
}

/// Scenario: N per-row calls emit the union of every shard, not just the first row's
#[test]
fn per_row_calls_emit_union_of_all_shards() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_multi_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let specs = vec![
        spec_for_file(write_parquet_ids(&dir, "f0.parquet", 0, 10)),
        spec_for_file(write_parquet_ids(&dir, "f1.parquet", 100, 10)),
        spec_for_file(write_parquet_ids(&dir, "f2.parquet", 200, 10)),
    ];

    let built = AtomicUsize::new(0);
    let emitted = run_all_rows(&specs, &id_name_emits(), &built);

    assert_eq!(
        total_rows(&emitted),
        30,
        "every shard is scanned by its own run() call (10 rows each); dropping any \
         shard's call would leave fewer than 30 rows"
    );
    let mut expected: Vec<i64> = (0..10).chain(100..110).chain(200..210).collect();
    expected.sort_unstable();
    assert_eq!(
        ids_of(&emitted),
        expected,
        "emitted ids must be the UNION of every per-row call's file contents"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: each per-row call builds and tears down its own runtime
#[test]
fn run_scan_one_builds_and_tears_down_runtime_per_call() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_rt_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let specs = vec![
        spec_for_file(write_parquet_ids(&dir, "r0.parquet", 0, 10)),
        spec_for_file(write_parquet_ids(&dir, "r1.parquet", 50, 10)),
        spec_for_file(write_parquet_ids(&dir, "r2.parquet", 100, 10)),
    ];

    let built = AtomicUsize::new(0);
    let emitted = run_all_rows(&specs, &id_name_emits(), &built);

    assert_eq!(
        built.load(Ordering::SeqCst),
        specs.len(),
        "harness must build one fresh runtime per row (this checks the harness's own \
         call discipline, not run_scan's)"
    );
    assert_eq!(
        total_rows(&emitted),
        30,
        "every per-row call scans its shard even though its runtime is torn down \
         before the next call builds a fresh one"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a single per-row call is byte-identical to the direct raw-scan path
#[test]
fn single_row_call_is_byte_identical_to_direct_raw_scan() {
    let dir = std::env::temp_dir().join(format!("lh_perrow_single_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = spec_for_file(write_parquet_ids(&dir, "only.parquet", 0, 200));

    let built = AtomicUsize::new(0);
    let per_row = run_one_row(&spec, &id_name_emits(), &built);

    let reference = block_on(async {
        let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
            TestContext::scalar(row_for_spec(&spec)).with_next_policy(NextPolicy::Reject(
                UdfError::User(
                    "scalar run() handles exactly one row; ctx.next() must never be called".into(),
                ),
            )),
            &id_name_emits(),
        );
        let session =
            local_session(&spec, &scan_fixture::resolved_storage(&spec), 0).expect("session");
        let mut timers = PhaseTimers::start();
        run_raw_scan_with_session(
            &mut ctx,
            &session,
            &spec,
            &scan_fixture::resolved_storage(&spec),
            &mut timers,
        )
        .await
        .expect("reference raw scan");
        ctx.into_batches()
    });

    assert_eq!(total_rows(&per_row), 200, "single-row call scans all rows");
    assert_eq!(
        per_row, reference,
        "a single per-row call must emit rows byte-for-byte identical to the direct \
         raw-scan path"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a `distinct: true` row scan streams one row per shard-local distinct value
#[test]
fn distinct_row_scan_streams_one_row_per_distinct_value() {
    let dir = std::env::temp_dir().join(format!("lh_distinct_row_scan_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let values = ["a", "b", "a", "c", "b", "a", "c", "b", "a", "c"];
    let file_url = write_parquet_categories(&dir, "categories.parquet", &values);
    let spec = distinct_spec_for_file(file_url);

    let built = AtomicUsize::new(0);
    let emitted = run_one_row(&spec, &[scan_fixture::varchar()], &built);

    assert_eq!(
        total_rows(&emitted),
        3,
        "distinct: true must collapse 10 duplicate-laden rows down to the 3 \
         distinct values, not stream all 10"
    );
    assert_eq!(
        categories_of(&emitted),
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        "emitted rows must be exactly the distinct value set, no duplicates"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: every emitted column carries the Arrow type its declared `EMITS` type requires
#[test]
fn raw_scan_coerces_every_column_to_its_declared_output_type() {
    let dir = std::env::temp_dir().join(format!("lh_emit_coercion_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let spec = spec_for_file(write_parquet_ids(&dir, "coerce.parquet", 0, 10));
    let built = AtomicUsize::new(0);
    let emitted = run_one_row(&spec, &id_name_emits(), &built);

    assert_eq!(total_rows(&emitted), 10, "all 10 rows must be emitted");
    for batch in &emitted {
        let schema = batch.schema();
        assert_eq!(
            schema
                .field(schema.index_of("ID").expect("ID present"))
                .data_type(),
            &DataType::Decimal128(20, 0),
            "ID must reach emit_batch as the Decimal128 its DECIMAL(20,0) declaration requires"
        );
        assert_eq!(
            schema
                .field(schema.index_of("NAME").expect("NAME present"))
                .data_type(),
            &DataType::Utf8,
            "NAME must reach emit_batch as the Utf8 its VARCHAR declaration requires"
        );
    }

    assert_eq!(
        ids_of(&emitted),
        (0..10).collect::<Vec<i64>>(),
        "the ID values must round-trip through the cast unchanged"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a nested column rendered to JSON upstream crosses the emit coercion unchanged
#[test]
fn rendered_nested_column_passes_the_emit_coercion_unchanged() {
    let dir = std::env::temp_dir().join(format!("lh_emit_nested_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file_url = write_parquet_tags(&dir, "tags.parquet");

    let spec = nested_spec_for_file(file_url);

    let built = AtomicUsize::new(0);
    let emitted = run_one_row(&spec, &id_name_emits(), &built);

    let mut rendered: Vec<Option<String>> = Vec::new();
    for batch in &emitted {
        let index = batch.schema().index_of("TAGS").expect("TAGS present");
        let col = batch.column(index);
        let text = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("unexpected TAGS column type: {:?}", col.data_type()));
        for i in 0..batch.num_rows() {
            rendered.push((!text.is_null(i)).then(|| text.value(i).to_string()));
        }
    }

    assert_eq!(
        rendered,
        vec![Some(r#"["hello","world"]"#.to_string())],
        "the rendered nested column must cross the emit boundary as valid JSON, not display text"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
