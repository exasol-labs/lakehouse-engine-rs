//! Integration tests (Task 3.4, plan `add-delta-scan-execution`) for **partition-value
//! materialization at scan time**, over the vendored `basic_partitioned` fixture on a
//! local filesystem store.
//!
//! The fixture is real Delta writer output: a `letter` (Utf8), `number` (Int64),
//! `a_float` (Float64) table partitioned on `letter`, with six data files — two under
//! `letter=a`, one each under `letter=b`, `letter=c`, `letter=e`, and one under the
//! Hive-convention default-partition directory `letter=__HIVE_DEFAULT_PARTITION__`
//! whose commit logs `"letter": null`, not the directory text. Each file carries
//! exactly one row (`_delta_log` `numRecords: 1`), so `number` uniquely identifies the
//! originating file: `1`/`4` → `a`, `2` → `b`, `3` → `c`, `5` → `e`, `6` → the
//! default-partition file.
//!
//! Every test drives the production raw-scan pipeline (`register_files` /
//! `run_raw_scan_with_session` / `build_raw_scan_physical_plan` →
//! `PositionalDeleteScanTable` → `crate::scan::partition_values`) against a
//! temp-directory copy of the vendored bytes, so nothing here mutates the checked-in
//! fixture.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, ScanSpec, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_raw_scan_physical_plan, register_files, run_raw_scan_with_session,
    session_config_for_spec,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;
use url::Url;

const LETTER_A_FILE_1: &str = "part-00000-a08d296a-d2c5-4a99-bea9-afcea42ba2e9.c000.snappy.parquet";
const LETTER_B_FILE: &str = "part-00000-41954fb0-ef91-47e5-bd41-b75169c41c17.c000.snappy.parquet";
const LETTER_C_FILE: &str = "part-00000-27a17b8f-be68-485c-9c49-70c742be30c0.c000.snappy.parquet";
const LETTER_A_FILE_2: &str = "part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet";
const LETTER_E_FILE: &str = "part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet";
const HIVE_DEFAULT_FILE: &str =
    "part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet";

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/unity/fixtures/basic_partitioned")
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("lh_partition_values_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn file_url(path: &Path) -> String {
    Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

fn dir_url(path: &Path) -> String {
    let mut url = file_url(path);
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .len()
}

/// Copy a vendored fixture data file into `dir/partition_dir/`, returning its
/// table-root-relative path and absolute size.
fn copy_partition_file(dir: &Path, partition_dir: &str, file_name: &str) -> (String, u64) {
    let sub = dir.join(partition_dir);
    std::fs::create_dir_all(&sub).expect("create partition subdir");
    let dest = sub.join(file_name);
    std::fs::copy(fixture_dir().join(partition_dir).join(file_name), &dest)
        .unwrap_or_else(|e| panic!("copy fixture file: {e}"));
    let size = file_size(&dest);
    (format!("{partition_dir}/{file_name}"), size)
}

fn letter_value(letter: Option<&str>) -> BTreeMap<String, Option<String>> {
    BTreeMap::from([("letter".to_string(), letter.map(str::to_string))])
}

/// Copy all six vendored data files into `dir`, each carrying the SAME logged
/// partition value its own `_delta_log` commit records — including the
/// default-partition file's logged `null`, never the `__HIVE_DEFAULT_PARTITION__`
/// directory text.
fn all_six_entries(dir: &Path) -> Vec<FileEntry> {
    let files: [(&str, &str, Option<&str>); 6] = [
        ("letter=a", LETTER_A_FILE_1, Some("a")),
        ("letter=b", LETTER_B_FILE, Some("b")),
        ("letter=c", LETTER_C_FILE, Some("c")),
        ("letter=a", LETTER_A_FILE_2, Some("a")),
        ("letter=e", LETTER_E_FILE, Some("e")),
        ("letter=__HIVE_DEFAULT_PARTITION__", HIVE_DEFAULT_FILE, None),
    ];
    files
        .iter()
        .map(|(subdir, name, letter)| {
            let (rel, size) = copy_partition_file(dir, subdir, name);
            FileEntry::with_partition_values(rel, size, letter_value(*letter))
        })
        .collect()
}

fn basic_partitioned_logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: None,
            name: "letter".into(),
            arrow_type: "utf8".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "number".into(),
            arrow_type: "int64".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "a_float".into(),
            arrow_type: "float64".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ]
}

/// Storage props are never dialed for a local `file://` scan; a placeholder keeps
/// the spec well-formed.
fn dummy_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "SECRETKEY".into(),
        allow_http: true,
        ..Default::default()
    })
}

fn basic_partitioned_spec(
    files: Vec<FileEntry>,
    table_root: &str,
    filter: Option<String>,
    limit: Option<u64>,
) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.to_string(),
            logical_schema: basic_partitioned_logical_schema(),
            partition_columns: vec!["letter".to_string()],
            filter,
            limit,
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// A fake `UdfContext` serving one input row and decoding every emitted Arrow IPC
/// batch — the same capture pattern `scan_deletion_vectors.rs` uses.
struct FakeCtx {
    served: bool,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new() -> Self {
        Self {
            served: false,
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        0
    }
    fn get(&self, _col: usize) -> Result<&Value, UdfError> {
        Err(UdfError::User("FakeCtx has no input columns".into()))
    }
    fn get_string(&self, _col: usize) -> Result<Option<&str>, UdfError> {
        Ok(None)
    }
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("raw path must use emit_batch".into()))
    }
    fn next(&mut self) -> Result<bool, UdfError> {
        if self.served {
            Ok(false)
        } else {
            self.served = true;
            Ok(true)
        }
    }
    fn debug_level(&self) -> tracing::Level {
        tracing::Level::INFO
    }
    fn emit_record_batch_ipc(&mut self, ipc: &[u8]) -> Result<(), UdfError> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let reader = StreamReader::try_new(Cursor::new(ipc), None)
            .map_err(|e| UdfError::User(format!("ipc decode: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| UdfError::User(format!("ipc batch: {e}")))?;
            self.emitted.push(batch);
        }
        Ok(())
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

/// Run the production raw scan for `spec` against a session registering `store` for
/// `register_url`'s scheme/authority. Returns the decoded emitted batches, or the
/// scan's error.
async fn try_run_scan_with_store(
    spec: &ScanSpec,
    register_url: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<Vec<RecordBatch>, UdfError> {
    let session = SessionContext::new_with_config(session_config_for_spec(spec));
    session
        .runtime_env()
        .register_object_store(&Url::parse(register_url).expect("register url"), store);
    let mut ctx = FakeCtx::new();
    let mut timers = PhaseTimers::start();
    run_raw_scan_with_session(&mut ctx, &session, spec, &mut timers).await?;
    Ok(ctx.emitted)
}

/// Run the production raw scan over a plain `LocalFileSystem`, panicking on scan
/// failure.
fn run_scan(spec: &ScanSpec, register_url: &str) -> Vec<RecordBatch> {
    block_on(try_run_scan_with_store(
        spec,
        register_url,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect("raw scan must succeed")
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Each row's `(letter, number)` pair, decoded from the batches' first two
/// columns — the declared column order this fixture's logical schema always
/// projects in when no projection narrows it.
fn letter_number_rows(batches: &[RecordBatch]) -> Vec<(Option<String>, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let letters = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("letter col");
        let numbers = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("number col");
        for i in 0..b.num_rows() {
            let letter = if letters.is_null(i) {
                None
            } else {
                Some(letters.value(i).to_string())
            };
            out.push((letter, numbers.value(i)));
        }
    }
    out.sort_by_key(|(_, n)| *n);
    out
}

/// Scenario: a partition column absent from every data file is materialized per
/// file, from that file's own logged value — never another file's.
#[test]
fn absent_partition_column_is_materialized_per_file() {
    let dir = temp_dir("absent_per_file");
    let entries = all_six_entries(&dir);
    let table_root = dir_url(&dir);
    let spec = basic_partitioned_spec(entries, &table_root, None, None);

    let rows = run_scan(&spec, &table_root);

    assert_eq!(total_rows(&rows), 6, "one row per file across six files");
    let got = letter_number_rows(&rows);
    let mut expected = vec![
        (Some("a".to_string()), 1),
        (Some("b".to_string()), 2),
        (Some("c".to_string()), 3),
        (Some("a".to_string()), 4),
        (Some("e".to_string()), 5),
        (None, 6),
    ];
    expected.sort_by_key(|(_, n)| *n);
    assert_eq!(
        got, expected,
        "each file's own logged letter must appear on its own row, never another file's value"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: an absent logged value (the default-partition file's logged `null`)
/// AND an explicit empty-string logged value both materialize SQL NULL — never the
/// `__HIVE_DEFAULT_PARTITION__` directory text and never an empty string.
#[test]
fn absent_and_empty_partition_values_materialize_null() {
    let dir = temp_dir("absent_and_empty");
    let (hive_rel, hive_size) =
        copy_partition_file(&dir, "letter=__HIVE_DEFAULT_PARTITION__", HIVE_DEFAULT_FILE);
    let hive_entry = FileEntry::with_partition_values(hive_rel, hive_size, letter_value(None));

    let (empty_rel, empty_size) = copy_partition_file(&dir, "letter=b", LETTER_B_FILE);
    let empty_entry = FileEntry::with_partition_values(
        empty_rel,
        empty_size,
        BTreeMap::from([("letter".to_string(), Some(String::new()))]),
    );

    let table_root = dir_url(&dir);
    let spec = basic_partitioned_spec(vec![hive_entry, empty_entry], &table_root, None, None);
    let rows = run_scan(&spec, &table_root);

    assert_eq!(total_rows(&rows), 2);
    let got = letter_number_rows(&rows);
    for (letter, number) in &got {
        assert!(
            letter.is_none(),
            "row for number={number} must materialize NULL, not a directory name or empty string: {got:?}"
        );
    }
    let numbers: Vec<i64> = got.iter().map(|(_, n)| *n).collect();
    assert!(
        numbers.contains(&6),
        "the default-partition file's own row must be present: {numbers:?}"
    );
    assert!(
        numbers.contains(&2),
        "the empty-string-partition file's own row must be present: {numbers:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

fn logical_schema_with_flag() -> Vec<LogicalField> {
    let mut fields = basic_partitioned_logical_schema();
    fields.push(LogicalField {
        field_id: None,
        name: "flag".into(),
        arrow_type: "int32".into(),
        nullable: true,
        initial_default: None,
        nested: None,
        physical_name: None,
    });
    fields
}

fn spec_with_flag(files: Vec<FileEntry>, table_root: &str) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.to_string(),
            logical_schema: logical_schema_with_flag(),
            partition_columns: vec!["letter".to_string(), "flag".to_string()],
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files,
    }
}

/// Scenario: a logged partition value converts to its column's declared Arrow
/// type end to end through the production scan; a value that type cannot
/// represent is refused cleanly through the SAME pipeline, never coerced,
/// truncated, or silently nulled.
#[test]
fn partition_values_convert_to_their_declared_type_or_fail_cleanly() {
    let dir = temp_dir("type_conversion");
    let (rel, size) = copy_partition_file(&dir, "letter=a", LETTER_A_FILE_1);
    let table_root = dir_url(&dir);

    let ok_entry = FileEntry::with_partition_values(
        rel.clone(),
        size,
        BTreeMap::from([
            ("letter".to_string(), Some("a".to_string())),
            ("flag".to_string(), Some("7".to_string())),
        ]),
    );
    let ok_spec = spec_with_flag(vec![ok_entry], &table_root);
    let rows = run_scan(&ok_spec, &table_root);
    assert_eq!(total_rows(&rows), 1);
    let flags = rows[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("flag col");
    assert_eq!(
        flags.value(0),
        7,
        "a well-formed value converts to its declared Int32 type"
    );

    let bad_entry = FileEntry::with_partition_values(
        rel,
        size,
        BTreeMap::from([
            ("letter".to_string(), Some("a".to_string())),
            ("flag".to_string(), Some("not-a-number".to_string())),
        ]),
    );
    let bad_spec = spec_with_flag(vec![bad_entry], &table_root);
    let err = block_on(try_run_scan_with_store(
        &bad_spec,
        &table_root,
        Arc::new(LocalFileSystem::new()),
    ))
    .expect_err("an unrepresentable partition value must be refused, not applied");
    let msg = err.to_string();
    assert!(msg.contains("flag"), "{msg}");
    assert!(msg.contains("Int32"), "{msg}");
    assert!(msg.contains("not-a-number"), "{msg}");

    let _ = std::fs::remove_dir_all(&dir);
}

fn write_parquet_with_physical_letter(
    dir: &Path,
    relative: &str,
    physical_letter: &str,
    number: i64,
) -> (String, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("letter", DataType::Utf8, true),
        Field::new("number", DataType::Int64, true),
    ]));
    let path = dir.join(relative);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![physical_letter])),
            Arc::new(Int64Array::from(vec![number])),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    (relative.to_string(), file_size(&path))
}

fn one_off_logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: None,
            name: "letter".into(),
            arrow_type: "utf8".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "number".into(),
            arrow_type: "int64".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ]
}

/// Scenario: a data file that physically carries its own column of the SAME name
/// as a declared partition column is still read through the logged value — the
/// physical column never wins, and the split leaves the file schema unable to see
/// it at all.
#[test]
fn logged_partition_value_wins_over_a_physical_partition_column() {
    let dir = temp_dir("logged_wins");
    let (rel, size) =
        write_parquet_with_physical_letter(&dir, "physical_letter.parquet", "z_physical", 99);
    let table_root = dir_url(&dir);

    let entry = FileEntry::with_partition_values(
        rel,
        size,
        BTreeMap::from([("letter".to_string(), Some("a_logged".to_string()))]),
    );
    let spec = ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.clone(),
            logical_schema: one_off_logical_schema(),
            partition_columns: vec!["letter".to_string()],
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![entry],
    };

    let rows = run_scan(&spec, &table_root);
    assert_eq!(total_rows(&rows), 1);
    let letters = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("letter col");
    let numbers = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("number col");
    assert_eq!(
        letters.value(0),
        "a_logged",
        "the logged partition value must win over the file's own physical column of the same name"
    );
    assert_ne!(letters.value(0), "z_physical");
    assert_eq!(numbers.value(0), 99);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: a materialized partition column is a first-class scan column —
/// projection can drop it or keep it, filter pushdown narrows on it, and GROUP BY
/// aggregates over it, including its own NULL group.
#[test]
fn materialized_partition_column_serves_projection_filter_and_group_by() {
    let dir = temp_dir("proj_filter_group");
    let entries = all_six_entries(&dir);
    let table_root = dir_url(&dir);

    let mut proj_spec = basic_partitioned_spec(entries.clone(), &table_root, None, None);
    proj_spec.common.projection = vec!["LETTER".into(), "NUMBER".into()];
    let proj_rows = run_scan(&proj_spec, &table_root);
    assert_eq!(total_rows(&proj_rows), 6);
    for b in &proj_rows {
        assert_eq!(b.num_columns(), 2, "projection must drop A_FLOAT");
    }

    let filter_spec = basic_partitioned_spec(
        entries.clone(),
        &table_root,
        Some("\"LETTER\" = 'a'".to_string()),
        None,
    );
    let filter_rows = run_scan(&filter_spec, &table_root);
    let mut numbers: Vec<i64> = letter_number_rows(&filter_rows)
        .into_iter()
        .map(|(_, n)| n)
        .collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        vec![1, 4],
        "filter pushdown over the partition column must see only its own files"
    );

    let group_spec = basic_partitioned_spec(entries, &table_root, None, None);
    let counts = block_on(async {
        let session = SessionContext::new_with_config(session_config_for_spec(&group_spec));
        session.runtime_env().register_object_store(
            &Url::parse(&table_root).expect("register url"),
            Arc::new(LocalFileSystem::new()),
        );
        register_files(&session, "scan_target", &group_spec)
            .await
            .expect("register_files must succeed");
        let df = session
            .sql(r#"SELECT "letter", COUNT(*) AS c FROM scan_target GROUP BY "letter" ORDER BY "letter""#)
            .await
            .expect("group-by SQL must plan over the materialized partition column");
        df.collect().await.expect("group-by must execute")
    });
    let total: i64 = counts
        .iter()
        .map(|b| {
            let c = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count col");
            (0..b.num_rows()).map(|i| c.value(i)).sum::<i64>()
        })
        .sum();
    assert_eq!(
        total, 6,
        "every row must land in exactly one group, including the NULL group"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Recurse the physical plan, collecting every leaf node (mirrors
/// `scan_positional_deletes.rs`'s `collect_leaf_execs`).
fn collect_leaf_execs(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    let children = plan.children();
    if children.is_empty() {
        out.push(Arc::clone(plan));
    } else {
        for child in children {
            collect_leaf_execs(child, out);
        }
    }
}

/// The raw scan plan's single leaf, downcast to its `FileScanConfig` (mirrors
/// `scan_positional_deletes.rs`'s `leaf_file_scan_config`).
fn leaf_file_scan_config(
    plan: &Arc<dyn ExecutionPlan>,
) -> datafusion::datasource::physical_plan::FileScanConfig {
    let mut leaves = Vec::new();
    collect_leaf_execs(plan, &mut leaves);
    assert_eq!(leaves.len(), 1, "raw scan plan must have exactly one leaf");
    let (file_scan_config, _parquet_source) = leaves[0]
        .downcast_ref::<DataSourceExec>()
        .expect("leaf must be a DataSourceExec")
        .downcast_to_file_source::<ParquetSource>()
        .expect("leaf must be backed by a ParquetSource");
    file_scan_config.clone()
}

fn unpartitioned_logical_schema() -> Vec<LogicalField> {
    vec![
        LogicalField {
            field_id: None,
            name: "number".into(),
            arrow_type: "int64".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
        LogicalField {
            field_id: None,
            name: "a_float".into(),
            arrow_type: "float64".into(),
            nullable: true,
            initial_default: None,
            nested: None,
            physical_name: None,
        },
    ]
}

/// Scenario: a scan with NO partition columns is unchanged — the (empty) split
/// leaves the file schema, the table_partition_cols, and the projected output
/// schema exactly as they were before `PartitionedScanSchema` existed.
#[test]
fn scan_without_partition_columns_is_byte_identical() {
    let dir = temp_dir("unpartitioned");
    let (rel, size) = copy_partition_file(&dir, "letter=a", LETTER_A_FILE_1);
    let table_root = dir_url(&dir);

    let spec = ScanSpec {
        common: CommonScanSpec {
            table_root: table_root.clone(),
            logical_schema: unpartitioned_logical_schema(),
            storage: dummy_storage(),
            df_batch_size: 64,
            ..Default::default()
        },
        files: vec![FileEntry::new(rel, size)],
    };
    assert!(
        spec.common.partition_columns.is_empty(),
        "control case carries no partition columns"
    );

    let file_scan_config = block_on(async {
        let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
        ctx.runtime_env().register_object_store(
            &Url::parse(&table_root).expect("register url"),
            Arc::new(LocalFileSystem::new()),
        );
        register_files(&ctx, "scan_target", &spec)
            .await
            .expect("register_files must succeed");
        let plan = build_raw_scan_physical_plan(&ctx, &spec)
            .await
            .expect("build physical plan");
        leaf_file_scan_config(&plan)
    });

    let expected_file_schema = Schema::new(vec![
        Field::new("number", DataType::Int64, true),
        Field::new("a_float", DataType::Float64, true),
    ]);
    assert_eq!(
        file_scan_config.file_schema().as_ref(),
        &expected_file_schema,
        "the file schema must be exactly the declared schema, unperturbed by the (empty) partition split"
    );
    assert!(
        file_scan_config.table_partition_cols().is_empty(),
        "an unpartitioned scan must attach no table_partition_cols"
    );
    // `build_scan_sql` unconditionally wraps every scan (partitioned or not) in an
    // uppercase-aliasing SELECT (Exasol identifier casing); DataFusion pushes that
    // rename into the leaf's own projected schema regardless of partitioning. The
    // partition-column dimension this scenario proves byte-identical is declared
    // column ORDER and COUNT, not this pre-existing casing behavior.
    let expected_projected_schema = Schema::new(vec![
        Field::new("NUMBER", DataType::Int64, true),
        Field::new("A_FLOAT", DataType::Float64, true),
    ]);
    let projected = file_scan_config
        .projected_schema()
        .expect("projected schema must be derivable");
    assert_eq!(
        projected.as_ref(),
        &expected_projected_schema,
        "the projected output schema must keep the declared column order and count — proving \
         the partition split introduced no plan-shape change for an unpartitioned table"
    );

    let rows = run_scan(&spec, &table_root);
    assert_eq!(total_rows(&rows), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
