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
use std::collections::{BTreeMap, HashMap};
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
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, LogicalField, ScanSpec, StorageBackend,
    StorageProps,
};
use lakehouse_engine::scan::{
    build_join_physical_plan, run_join_scan_with_session, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;

struct FakeCtx {
    served: bool,
    args: Vec<Value>,
    emitted: Vec<RecordBatch>,
}

impl FakeCtx {
    fn new() -> Self {
        Self {
            served: false,
            args: Vec::new(),
            emitted: Vec::new(),
        }
    }

    /// A context serving the TWO production scan-UDF input arguments for `spec` —
    /// the shard-invariant common blob and this shard's files JSON — so a test can
    /// drive `run_scan`, the entry point that builds the real object stores.
    fn with_spec_args(spec: &ScanSpec) -> Self {
        Self {
            served: false,
            args: vec![
                Value::String(spec.to_common_json()),
                Value::String(ScanSpec::files_json(&spec.files)),
            ],
            emitted: Vec::new(),
        }
    }
}

impl UdfContext for FakeCtx {
    fn num_columns(&self) -> usize {
        self.args.len()
    }
    fn get(&self, col: usize) -> Result<&Value, UdfError> {
        self.args
            .get(col)
            .ok_or_else(|| UdfError::User(format!("FakeCtx has no input column {col}")))
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

/// Write a fact fixture whose FIRST two rows match no dimension row (custkey 999)
/// and whose remaining three rows each match a distinct customer. Distinguishes a
/// post-join cap (applied to the JOINED output) from a pre-join cap (applied to the
/// fact scan before the join runs): with `post_join_limit = 2`, a post-join cap
/// truncates the 3 matching joined rows to 2, while a pre-join cap would instead
/// truncate the fact scan to its first 2 (unmatched) rows and emit zero.
fn write_orders_leading_unmatched(dir: &std::path::Path) -> (String, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_totalprice", DataType::Float64, false),
    ]));
    let path = dir.join("orders_leading_unmatched.parquet");
    let file = std::fs::File::create(&path).expect("create orders parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
            // Rows 1-2: custkey 999 matches no customer. Rows 3-5: match 10/20/30.
            Arc::new(Int64Array::from(vec![999i64, 999, 10, 20, 30])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 400.0, 500.0])),
        ],
    )
    .expect("orders batch");
    writer.write(&batch).expect("write orders");
    writer.close().expect("close orders");
    sized(file_url(&path))
}

/// An S3 backend reaching `endpoint` and carrying `secret` as its secret key —
/// the two fields a test tells one side's store, and one side's redaction set,
/// from the other's by. Path-style (the `StorageProps` default) is what makes a
/// bespoke `endpoint` reachable at all.
fn s3_backend(endpoint: &str, secret: &str) -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: endpoint.into(),
        region: "us-east-1".into(),
        access_key: "test-access-key".into(),
        secret_key: secret.into(),
        allow_http: true,
        ..Default::default()
    })
}

fn storage() -> StorageBackend {
    s3_backend("http://localhost:9000", "TOPSECRETVALUE")
}

/// The dimension side's backend — deliberately DISTINCT from `storage()`'s
/// `secret_key`, so every test built through `join_spec` runs credential-divergent:
/// the fact side and the dimension side never share a secret.
fn dim_storage() -> StorageBackend {
    s3_backend("http://localhost:9000", "DIMSECRETVALUE")
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
        common: CommonScanSpec {
            projection: projection.into_iter().map(Into::into).collect(),
            filter: filter.map(Into::into),
            join: Some(JoinSpec {
                table_root: String::new(),
                files: dim_files.into_iter().map(FileEntry::from).collect(),
                logical_schema: Vec::new(),
                name_mapping: Vec::new(),
                join_type: JoinType::Inner,
                condition: "\"C_CUSTKEY\" = \"O_CUSTKEY\"".into(),
                post_join_limit: limit,
                partition_columns: Vec::new(),
                storage: dim_storage(),
            }),
            storage: storage(),
            ..Default::default()
        },
        files: fact_files.into_iter().map(FileEntry::from).collect(),
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
        .common
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
/// Each side is registered against its OWN backend — the fact side under
/// `storage()`, the dimension side under `dim_storage()` (via `join_spec`) — and
/// the join still executes correctly. Orders (fact) inner-joined to customer
/// (dimension) on custkey. The order whose custkey has no matching customer (999)
/// is dropped; every matched order pairs with its customer name. Per-side
/// registration narrows WHICH backend guards each side's read; it never changes
/// WHICH ROWS come back — that correctness is what the row assertions below still
/// characterize.
#[test]
fn join_registers_each_side_against_its_own_backend() {
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

/// Scenario: Each side of a broadcast join materializes its own partition columns.
///
/// The fact side is partitioned by `o_region`, the dimension side by `c_country` —
/// disjoint columns with distinct values — so a wiring bug that dropped, swapped, or
/// crossed the two sides' `partition_columns` lists shows up as either a wrong value
/// or a scan failure, never a silent pass.
#[test]
fn each_join_side_materializes_its_own_partition_columns() {
    let dir = std::env::temp_dir().join(format!("lh_join_partition_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (orders_url, orders_size) = write_orders(&dir);
    let (customer_url, customer_size) = write_customer(&dir);

    let fact_file = FileEntry::with_partition_values(
        orders_url,
        orders_size,
        BTreeMap::from([("o_region".to_string(), Some("US".to_string()))]),
    );
    let dim_file = FileEntry::with_partition_values(
        customer_url,
        customer_size,
        BTreeMap::from([("c_country".to_string(), Some("CA".to_string()))]),
    );

    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec![
                "O_ORDERKEY".into(),
                "O_REGION".into(),
                "C_NAME".into(),
                "C_COUNTRY".into(),
            ],
            logical_schema: vec![
                logical_field(1, "o_orderkey", "int64"),
                logical_field(2, "o_custkey", "int64"),
                logical_field(3, "o_totalprice", "float64"),
                logical_field(4, "o_region", "utf8"),
            ],
            partition_columns: vec!["o_region".to_string()],
            join: Some(JoinSpec {
                table_root: String::new(),
                files: vec![dim_file],
                logical_schema: vec![
                    logical_field(1, "c_custkey", "int64"),
                    logical_field(2, "c_name", "utf8"),
                    logical_field(3, "c_country", "utf8"),
                ],
                name_mapping: Vec::new(),
                join_type: JoinType::Inner,
                condition: "\"C_CUSTKEY\" = \"O_CUSTKEY\"".into(),
                post_join_limit: None,
                partition_columns: vec!["c_country".to_string()],
                storage: dim_storage(),
            }),
            storage: storage(),
            ..Default::default()
        },
        files: vec![fact_file],
    };

    let batches = run_join(&spec);

    assert_eq!(
        total_rows(&batches),
        5,
        "inner join must drop the one order (custkey 999) with no matching customer"
    );

    for batch in &batches {
        assert_eq!(batch.num_columns(), 4);
        let region = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("O_REGION must be Utf8");
        let country = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("C_COUNTRY must be Utf8");
        for i in 0..batch.num_rows() {
            assert_eq!(
                region.value(i),
                "US",
                "fact-side partition value must reach the joined row unswapped"
            );
            assert_eq!(
                country.value(i),
                "CA",
                "dimension-side partition value must reach the joined row unswapped"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: LIMIT bounds the JOINED output, never the scanned input.
///
/// The fact fixture's FIRST two rows match no dimension row; its remaining three
/// rows each match a distinct customer. With `post_join_limit = 2`, the correct
/// (post-join) cap truncates the 3 matching joined rows to exactly 2. A pre-join
/// cap would instead truncate the fact scan to its first 2 rows — both unmatched —
/// and emit zero. The physical plan additionally carries no `fetch` below the
/// `HashJoinExec` on either input, confirming DataFusion did not turn the rendered
/// post-join `LIMIT` into a cap on either side's scan.
#[test]
fn join_limit_bounds_joined_output_not_scanned_input() {
    let dir = std::env::temp_dir().join(format!("lh_join_limit_bound_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders_leading_unmatched(&dir);
    let customer = write_customer(&dir);

    let spec = join_spec(
        vec![orders],
        vec![customer],
        vec!["O_ORDERKEY", "C_NAME"],
        None,
        Some(2),
    );

    let batches = run_join(&spec);
    assert_eq!(
        total_rows(&batches),
        2,
        "a post-join cap truncates the 3 matching joined rows to 2; a pre-join cap \
         would instead truncate the fact scan to its first 2 (unmatched) rows and \
         emit zero"
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

    assert!(
        has_no_fetch_below(hj.left()),
        "the post-join cap must not appear as a fetch on the dimension input"
    );
    assert!(
        has_no_fetch_below(hj.right()),
        "the post-join cap must not appear as a fetch on the fact input"
    );

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

/// True if no node in `plan`'s subtree carries a `fetch` (a `LIMIT`/`TopK` window,
/// or a limit pushed down into a scan). Used to confirm a post-join `LIMIT` never
/// turns into a cap on either join input.
fn has_no_fetch_below(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.fetch().is_none() && plan.children().into_iter().all(has_no_fetch_below)
}

/// Scenario: Scan reports a clear error when an assigned join file is unreadable.
///
/// A nonexistent dimension file surfaces through the secret-redacting
/// `classify_scan_error` path: the error names the read failure and NEVER contains
/// EITHER side's storage credential value (`storage()`'s `TOPSECRETVALUE` for the
/// fact side, `dim_storage()`'s `DIMSECRETVALUE` for the dimension side).
///
/// This test does NOT positively demonstrate that a credential was redacted out of
/// a message that would otherwise contain it: `run_join_scan_with_session` here
/// runs over a plain local-file session (no S3 store is ever registered), so
/// neither literal could appear in the error text regardless of redaction. What it
/// pins is that a MISSING file on the dimension side still routes through the
/// classifier at all. The falsifiable proof that the dimension side's credential is
/// genuinely stripped from a message that would otherwise carry it lives in
/// [`a_dimension_side_read_failure_redacts_the_dimension_sides_credential`].
#[test]
fn unreadable_join_file_error_redacts_both_sides_credentials() {
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
        "error must not leak the fact side's secret_key value: {text}"
    );
    assert!(
        !text.contains("DIMSECRETVALUE"),
        "error must not leak the dimension side's secret_key value: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A loopback endpoint refusing every request with a 403 whose XML body echoes
/// `message`, and the URL to reach it at.
///
/// Modelled on `object_store.rs`'s `RecordingEndpoint`, but the refusal BODY is
/// what matters here: `object_store` folds a non-2xx response body into the error
/// it surfaces, so an endpoint quoting a credential in its refusal — the real shape
/// of an S3 `SignatureDoesNotMatch` — is what makes value-based redaction
/// observable rather than vacuous. A 4xx is never retried, so each read reaches the
/// endpoint exactly once and fails fast.
fn refusing_endpoint(message: &str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback endpoint");
    let url = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("bound endpoint has an address")
    );
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>SignatureDoesNotMatch</Code><Message>{message}</Message></Error>"
    );
    let response = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut request_head = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut request_head);
            let _ = std::io::Write::write_all(&mut stream, response.as_bytes());
        }
    });
    url
}

fn logical_field(field_id: i32, name: &str, arrow_type: &str) -> LogicalField {
    LogicalField {
        field_id: Some(field_id),
        name: name.into(),
        arrow_type: arrow_type.into(),
        nullable: false,
        initial_default: None,
        physical_name: None,
    }
}

/// Scenario: A dimension-side read failure never surfaces the dimension side's
/// credential — the FALSIFIABLE counterpart to
/// [`unreadable_join_file_error_redacts_both_sides_credentials`].
///
/// Both sides share one bucket (the same-warehouse norm, so one DataFusion registry
/// key serves two credentials through the prefix router) but reach DIFFERENT
/// loopback endpoints, each refusing with an XML body that quotes ITS OWN secret
/// key next to a plain marker. The dimension side is the hash-join build side, so
/// its read is the first to touch an endpoint and the one that fails.
///
/// The marker assertion is what makes this test discriminate: it proves the
/// dimension endpoint's refusal body genuinely reached the surfaced message, hence
/// that the secret sitting beside it WOULD have leaked had the redaction set not
/// covered the dimension side. A redaction set built from the fact side's
/// `common.storage` alone fails this test.
///
/// Both sides carry a `logical_schema` so neither registration infers a schema:
/// inference reads through `register_file_list`, which redacts per-side, and would
/// mask the union rule under test.
#[test]
fn a_dimension_side_read_failure_redacts_the_dimension_sides_credential() {
    const DIM_MARKER: &str = "dimension-side-refusal";
    const FACT_MARKER: &str = "fact-side-refusal";

    let fact_endpoint = refusing_endpoint(&format!("{FACT_MARKER} TOPSECRETVALUE"));
    let dim_endpoint = refusing_endpoint(&format!("{DIM_MARKER} DIMSECRETVALUE"));

    let spec = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["O_ORDERKEY".into(), "C_NAME".into()],
            logical_schema: vec![
                logical_field(1, "o_orderkey", "int64"),
                logical_field(2, "o_custkey", "int64"),
            ],
            join: Some(JoinSpec {
                table_root: "s3://test-bucket/db/dim".into(),
                files: vec![FileEntry::new("data/dim-0.parquet", 4096)],
                logical_schema: vec![
                    logical_field(1, "c_custkey", "int64"),
                    logical_field(2, "c_name", "utf8"),
                ],
                name_mapping: Vec::new(),
                join_type: JoinType::Inner,
                condition: "\"C_CUSTKEY\" = \"O_CUSTKEY\"".into(),
                post_join_limit: None,
                partition_columns: Vec::new(),
                storage: s3_backend(&dim_endpoint, "DIMSECRETVALUE"),
            }),
            storage: s3_backend(&fact_endpoint, "TOPSECRETVALUE"),
            ..Default::default()
        },
        files: vec![FileEntry::new("s3://test-bucket/data/part-0.parquet", 4096)],
    };

    let mut ctx = FakeCtx::with_spec_args(&spec);
    let err = lakehouse_engine::scan::run_scan(&mut ctx)
        .expect_err("both sides' endpoints refuse every read, so the scan must fail");
    let text = err.to_string();

    assert!(
        text.contains("could not be read"),
        "error must route through the storage-read classifier: {text}"
    );
    assert!(
        text.contains(DIM_MARKER),
        "the DIMENSION endpoint's refusal body must be the one that reached the error \
         (otherwise the secret assertions below are vacuous): {text}"
    );
    assert!(
        !text.contains("DIMSECRETVALUE"),
        "error must not leak the dimension side's secret_key value: {text}"
    );
    assert!(
        !text.contains("TOPSECRETVALUE"),
        "error must not leak the fact side's secret_key value: {text}"
    );
}
