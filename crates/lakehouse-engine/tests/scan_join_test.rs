//! Host DataFusion join-execution tests over local Parquet (Group D, tasks 4.1–4.3).
//!
//! Drives the EXACT production broadcast-join execution path
//! ([`run_join_scan_with_session`] / [`build_join_physical_plan`]) against local
//! `file://` Parquet fixtures — no S3 / MinIO, no Iceberg catalog. A join `ScanSpec`
//! carries the sharded fact file list in `files` and the full dimension file list in
//! `join.files`; the scan registers both in one session, wraps each in an aliased
//! sub-SELECT exposing uppercase Exasol-facing names, runs the inner equi-join, and
//! streams the joined batches as Arrow IPC via `emit_batch`.
//!
//! Covers the `datafusion-scan/scan-execution-join` scenarios:
//! - `join_spec_reconstitutes_two_file_lists`
//! - `join_executes_inner_equi`
//! - `join_projection_filter_limit_streamed`
//! - `join_build_side_is_dimension`
//! - `join_unreadable_file_errors_without_secrets`

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{FileEntry, JoinSpec, JoinType, ScanSpec, StorageProps};
use lakehouse_engine::scan::{
    build_join_physical_plan, run_join_scan_with_session, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;

/// A fake `UdfContext` serving one input row and capturing every `emit_batch` as a
/// decoded `RecordBatch`. The raw/join streaming path must use `emit_batch`, never
/// row-by-row `emit`, so `emit` is a trap.
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
    fn emit(&mut self, _values: &[Value]) -> Result<(), UdfError> {
        Err(UdfError::User("join path must use emit_batch".into()))
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

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn file_url(path: &std::path::Path) -> String {
    url::Url::from_file_path(path)
        .expect("absolute path")
        .to_string()
}

fn sized(url: String) -> (String, u64) {
    let len = std::fs::metadata(url.strip_prefix("file://").unwrap_or(&url))
        .map(|m| m.len())
        .unwrap_or(0);
    (url, len)
}

/// Write the fact (orders) Parquet fixture and return its sized `(url, bytes)`.
/// Disjoint column names from the dimension side (VS disjoint-column guarantee).
fn write_orders(dir: &std::path::Path) -> (String, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_totalprice", DataType::Float64, false),
    ]));
    let path = dir.join("orders.parquet");
    let file = std::fs::File::create(&path).expect("create orders parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5, 6])),
            // custkey 999 has NO matching customer -> excluded by the inner join.
            Arc::new(Int64Array::from(vec![10i64, 20, 30, 10, 20, 999])),
            Arc::new(Float64Array::from(vec![
                100.0, 200.0, 300.0, 400.0, 500.0, 600.0,
            ])),
        ],
    )
    .expect("orders batch");
    writer.write(&batch).expect("write orders");
    writer.close().expect("close orders");
    sized(file_url(&path))
}

/// Write the dimension (customer) Parquet fixture and return its sized `(url, bytes)`.
fn write_customer(dir: &std::path::Path) -> (String, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
    ]));
    let path = dir.join("customer.parquet");
    let file = std::fs::File::create(&path).expect("create customer parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )
    .expect("customer batch");
    writer.write(&batch).expect("write customer");
    writer.close().expect("close customer");
    sized(file_url(&path))
}

fn storage() -> StorageProps {
    StorageProps {
        endpoint: "http://localhost:9000".into(),
        region: "us-east-1".into(),
        access_key: "test-access-key".into(),
        secret_key: "TOPSECRETVALUE".into(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// A join `ScanSpec`: `fact_files` is the sharded fact side (`files`); `dim_files`
/// is the full dimension side (`join.files`). `projection` is uppercase, spanning
/// both tables; `condition` is a rendered DataFusion equi-join predicate.
fn join_spec(
    fact_files: Vec<(String, u64)>,
    dim_files: Vec<(String, u64)>,
    projection: Vec<&str>,
    filter: Option<&str>,
    limit: Option<u64>,
) -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files: fact_files.into_iter().map(FileEntry::from).collect(),
        projection: projection.into_iter().map(Into::into).collect(),
        filter: filter.map(Into::into),
        limit,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join: Some(JoinSpec {
            table_root: String::new(),
            files: dim_files.into_iter().map(FileEntry::from).collect(),
            logical_schema: Vec::new(),
            name_mapping: Vec::new(),
            join_type: JoinType::Inner,
            condition: "\"C_CUSTKEY\" = \"O_CUSTKEY\"".into(),
        }),
        storage: storage(),
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

/// Run the production join scan for `spec` against a capturing context, returning
/// the decoded emitted batches.
fn run_join(spec: &ScanSpec) -> Vec<RecordBatch> {
    block_on(async {
        let mut ctx = FakeCtx::new();
        let session = SessionContext::new_with_config(session_config_for_spec(spec));
        let mut timers = PhaseTimers::start();
        run_join_scan_with_session(&mut ctx, &session, spec, &mut timers)
            .await
            .expect("join scan must succeed");
        ctx.emitted
    })
}

/// Scenario: Scan reconstitutes a join scan spec carrying two file lists.
///
/// The two-argument split (common blob + per-shard files) round-trips a join spec:
/// the fact file list stays in `files`, the dimension file list rides in the
/// shard-invariant `join` block, and the two lists are distinct.
#[test]
fn join_spec_reconstitutes_two_file_lists() {
    let fact = vec![
        ("s3://w/orders/f0.parquet".to_string(), 111u64),
        ("s3://w/orders/f1.parquet".to_string(), 222),
    ];
    let dim = vec![("s3://w/customer/c0.parquet".to_string(), 42u64)];
    let spec = join_spec(
        fact.clone(),
        dim.clone(),
        vec!["O_ORDERKEY", "C_NAME"],
        None,
        None,
    );

    // Split the way the adapter does: common blob (shard-invariant, carries the
    // join block) serialized once, fact files as a separate per-shard array.
    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);

    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");

    // Fact side: the per-shard files list.
    assert_eq!(
        reconstituted.files,
        fact.into_iter().map(FileEntry::from).collect::<Vec<_>>(),
        "fact file list must round-trip"
    );

    // Dimension side: the shard-invariant join block's full file list.
    let join = reconstituted
        .join
        .expect("join block must survive the split/merge");
    assert_eq!(
        join.files,
        dim.into_iter().map(FileEntry::from).collect::<Vec<_>>(),
        "dimension file list must round-trip"
    );
    assert_eq!(join.join_type, JoinType::Inner);
    assert_eq!(join.condition, "\"C_CUSTKEY\" = \"O_CUSTKEY\"");

    // The two file lists are genuinely distinct — no collision between the sharded
    // fact side and the replicated dimension side.
    assert_ne!(
        reconstituted.files, join.files,
        "fact and dimension file lists must be distinct"
    );
}

/// Scenario: Scan registers both tables and executes the inner equi-join.
///
/// Orders (fact) inner-joined to customer (dimension) on custkey. The order whose
/// custkey has no matching customer (999) is dropped; every matched order pairs
/// with its customer name.
#[test]
fn join_executes_inner_equi() {
    let dir = std::env::temp_dir().join(format!("lh_join_inner_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    let customer = write_customer(&dir);

    let spec = join_spec(
        vec![orders],
        vec![customer],
        vec!["O_ORDERKEY", "C_NAME"],
        None,
        None,
    );
    let batches = run_join(&spec);

    // 5 of 6 orders match a customer (custkey 999 is unmatched).
    assert_eq!(
        total_rows(&batches),
        5,
        "inner join must drop the unmatched order"
    );

    // Build orderkey -> customer name from the emitted (O_ORDERKEY, C_NAME) rows.
    let mut got: HashMap<i64, String> = HashMap::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 2, "projection is exactly two columns");
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("O_ORDERKEY must be Int64");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("C_NAME must be Utf8");
        for i in 0..batch.num_rows() {
            got.insert(keys.value(i), names.value(i).to_string());
        }
    }

    let expected: HashMap<i64, String> = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Alice"),
        (5, "Bob"),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.to_string()))
    .collect();
    assert_eq!(
        got, expected,
        "each matched order pairs with its customer name"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: Join projection, filter, and LIMIT are applied and rows streamed as
/// Arrow IPC.
///
/// The projection spans both tables; the WHERE filter references a fact column that
/// is NOT projected (the aliased sub-SELECT still exposes it); LIMIT bounds the
/// output. Rows arrive as decoded Arrow IPC batches (never row-by-row `emit`).
#[test]
fn join_projection_filter_limit_streamed() {
    let dir = std::env::temp_dir().join(format!("lh_join_pfl_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    let customer = write_customer(&dir);

    // custkey 10 matches orders 1 and 4 (both customer "Alice"); LIMIT 1 keeps one.
    let spec = join_spec(
        vec![orders],
        vec![customer],
        vec!["O_ORDERKEY", "O_TOTALPRICE", "C_NAME"],
        Some("\"O_CUSTKEY\" = 10"),
        Some(1),
    );
    let batches = run_join(&spec);

    assert_eq!(
        total_rows(&batches),
        1,
        "filter (custkey=10) yields 2 rows, LIMIT 1 keeps exactly one"
    );
    let batch = &batches[0];
    assert_eq!(
        batch.num_columns(),
        3,
        "projection spans both tables: O_ORDERKEY, O_TOTALPRICE, C_NAME"
    );
    // Column types follow the projection order.
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("O_ORDERKEY must be Int64");
    batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("O_TOTALPRICE must be Float64");
    let name = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("C_NAME must be Utf8");
    // Both custkey-10 orders belong to Alice, so the surviving row is deterministic
    // in the dimension column even though which order survives is not.
    assert_eq!(name.value(0), "Alice", "the custkey-10 customer is Alice");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: The bounded dimension side is the hash-join build side.
///
/// The physical plan's `HashJoinExec` builds its hash table from the LEFT child.
/// The scan places the bounded dimension on the left and disables join reordering,
/// so the dimension is deterministically the build side — its columns appear on the
/// left input, and the fact columns do not.
#[test]
fn join_build_side_is_dimension() {
    let dir = std::env::temp_dir().join(format!("lh_join_build_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    let customer = write_customer(&dir);

    let spec = join_spec(
        vec![orders],
        vec![customer],
        vec!["C_NAME", "O_ORDERKEY", "O_TOTALPRICE"],
        None,
        None,
    );

    let plan = block_on(async {
        let session = SessionContext::new_with_config(session_config_for_spec(&spec));
        build_join_physical_plan(&session, &spec)
            .await
            .expect("physical plan must build")
    });

    let hash_join = find_hash_join(&plan).expect("plan must contain a HashJoinExec");
    let hj_any: &dyn Any = hash_join.as_ref();
    let hj = hj_any
        .downcast_ref::<HashJoinExec>()
        .expect("downcast HashJoinExec");

    let build_side_cols: Vec<String> = hj
        .left()
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_uppercase())
        .collect();

    // The build (left) side is the dimension: it carries a dimension-only column
    // (C_NAME) and none of the fact-only columns (O_ORDERKEY / O_TOTALPRICE).
    assert!(
        build_side_cols.iter().any(|c| c == "C_NAME"),
        "build side must carry the dimension column C_NAME; got {build_side_cols:?}"
    );
    assert!(
        !build_side_cols
            .iter()
            .any(|c| c == "O_ORDERKEY" || c == "O_TOTALPRICE"),
        "build side must NOT carry fact columns; got {build_side_cols:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Recursively locate the first `HashJoinExec` in a physical plan tree.
fn find_hash_join(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let any: &dyn Any = plan.as_ref();
    if any.is::<HashJoinExec>() {
        return Some(Arc::clone(plan));
    }
    for child in plan.children() {
        if let Some(found) = find_hash_join(child) {
            return Some(found);
        }
    }
    None
}

/// Scenario: Scan reports a clear error when an assigned join file is unreadable.
///
/// A nonexistent dimension file surfaces through the secret-redacting
/// `classify_scan_error` path: the error names the read failure and NEVER contains
/// a storage credential value.
#[test]
fn join_unreadable_file_errors_without_secrets() {
    let dir = std::env::temp_dir().join(format!("lh_join_err_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    // Dimension points at a file that does not exist.
    let missing = file_url(&dir.join("does_not_exist_customer.parquet"));

    let spec = join_spec(
        vec![orders],
        vec![(missing, 4096)],
        vec!["O_ORDERKEY", "C_NAME"],
        None,
        None,
    );

    let err = block_on(async {
        let mut ctx = FakeCtx::new();
        let session = SessionContext::new_with_config(session_config_for_spec(&spec));
        let mut timers = PhaseTimers::start();
        run_join_scan_with_session(&mut ctx, &session, &spec, &mut timers)
            .await
            .expect_err("an unreadable dimension file must error")
    });

    let text = err.to_string();
    assert!(
        text.contains("could not be read"),
        "error must route through the storage-read classifier: {text}"
    );
    assert!(
        !text.contains("TOPSECRETVALUE"),
        "error must not leak the secret_key value: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
