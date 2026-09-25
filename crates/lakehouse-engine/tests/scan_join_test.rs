mod scan_fixture;

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::test_support::{EmitPolicy, TestContext};
use exasol_udf_sdk::value::{ExaType, Value};
use lakehouse_engine::scan::diagnostics::PhaseTimers;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, JoinSpec, JoinType, LogicalField, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_join_physical_plan, run_join_scan_with_session, session_config_for_spec,
};

use parquet::arrow::ArrowWriter;

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

/// Column names are disjoint from the dimension side (the VS disjoint-column guarantee).
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

/// The first two rows match no dimension row, so a pre-join `LIMIT 2` would emit zero rows.
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
            Arc::new(Int64Array::from(vec![999i64, 999, 10, 20, 30])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0, 400.0, 500.0])),
        ],
    )
    .expect("orders batch");
    writer.write(&batch).expect("write orders");
    writer.close().expect("close orders");
    sized(file_url(&path))
}

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

/// A secret distinct from `storage()`'s, so the two join sides never share a credential.
fn dim_storage() -> StorageBackend {
    s3_backend("http://localhost:9000", "DIMSECRETVALUE")
}

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
                storage: ScanStorage::Inline(dim_storage()),
            }),
            storage: ScanStorage::Inline(storage()),
            ..Default::default()
        },
        files: fact_files.into_iter().map(FileEntry::from).collect(),
    }
}

fn run_join(spec: &ScanSpec, emits: &[ExaType]) -> Vec<RecordBatch> {
    block_on(async {
        let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
            TestContext::scalar(vec![]).with_emit_policy(EmitPolicy::Reject(UdfError::User(
                "join path must use emit_batch".into(),
            ))),
            emits,
        );
        let session = SessionContext::new_with_config(session_config_for_spec(spec));
        let mut timers = PhaseTimers::start();
        run_join_scan_with_session(
            &mut ctx,
            &session,
            spec,
            &scan_fixture::resolved_storage(spec),
            &mut timers,
        )
        .await
        .expect("join scan must succeed");
        ctx.into_batches()
    })
}

/// Scenario: Scan reconstitutes a join scan spec carrying two file lists.
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

    let common_json = spec.to_common_json();
    let files_json = ScanSpec::files_json(&spec.files);

    let reconstituted =
        ScanSpec::from_parts_json(&common_json, &files_json).expect("from_parts_json");

    assert_eq!(
        reconstituted.files,
        fact.into_iter().map(FileEntry::from).collect::<Vec<_>>(),
        "fact file list must round-trip"
    );

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

    assert_ne!(
        reconstituted.files, join.files,
        "fact and dimension file lists must be distinct"
    );
}

/// Scenario: Scan registers both tables and executes the inner equi-join.
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
    let batches = run_join(&spec, &[ExaType::Int64, scan_fixture::varchar()]);

    assert_eq!(
        total_rows(&batches),
        5,
        "inner join must drop the unmatched order"
    );

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

/// Scenario: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC.
#[test]
fn join_projection_filter_limit_streamed() {
    let dir = std::env::temp_dir().join(format!("lh_join_pfl_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    let customer = write_customer(&dir);

    let spec = join_spec(
        vec![orders],
        vec![customer],
        vec!["O_ORDERKEY", "O_TOTALPRICE", "C_NAME"],
        Some("\"O_CUSTKEY\" = 10"),
        Some(1),
    );
    let batches = run_join(
        &spec,
        &[ExaType::Int64, ExaType::Double, scan_fixture::varchar()],
    );

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
    // Which order survives is nondeterministic, but both belong to Alice.
    assert_eq!(name.value(0), "Alice", "the custkey-10 customer is Alice");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: Each side of a broadcast join materializes its own partition columns.
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
                storage: ScanStorage::Inline(dim_storage()),
            }),
            storage: ScanStorage::Inline(storage()),
            ..Default::default()
        },
        files: vec![fact_file],
    };

    let batches = run_join(
        &spec,
        &[
            ExaType::Int64,
            scan_fixture::varchar(),
            scan_fixture::varchar(),
            scan_fixture::varchar(),
        ],
    );

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

    let batches = run_join(&spec, &[ExaType::Int64, scan_fixture::varchar()]);
    assert_eq!(
        total_rows(&batches),
        2,
        "a post-join cap truncates the 3 matching joined rows to 2; a pre-join cap \
         would instead truncate the fact scan to its first 2 (unmatched) rows and \
         emit zero"
    );

    let plan = block_on(async {
        let session = SessionContext::new_with_config(session_config_for_spec(&spec));
        build_join_physical_plan(&session, &spec, &scan_fixture::resolved_storage(&spec))
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
        build_join_physical_plan(&session, &spec, &scan_fixture::resolved_storage(&spec))
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

    // `HashJoinExec` builds from its left child.
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

fn has_no_fetch_below(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.fetch().is_none() && plan.children().into_iter().all(has_no_fetch_below)
}

/// Scenario: Scan reports a clear error when an assigned join file is unreadable.
#[test]
fn unreadable_join_file_error_redacts_both_sides_credentials() {
    // No S3 store is registered, so neither secret could appear anyway; the falsifiable
    // redaction proof is [`a_dimension_side_read_failure_redacts_the_dimension_sides_credential`].
    let dir = std::env::temp_dir().join(format!("lh_join_err_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let orders = write_orders(&dir);
    let missing = file_url(&dir.join("does_not_exist_customer.parquet"));

    let spec = join_spec(
        vec![orders],
        vec![(missing, 4096)],
        vec!["O_ORDERKEY", "C_NAME"],
        None,
        None,
    );

    let err = block_on(async {
        let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
            TestContext::scalar(vec![]).with_emit_policy(EmitPolicy::Reject(UdfError::User(
                "join path must use emit_batch".into(),
            ))),
            &[ExaType::Int64, scan_fixture::varchar()],
        );
        let session = SessionContext::new_with_config(session_config_for_spec(&spec));
        let mut timers = PhaseTimers::start();
        run_join_scan_with_session(
            &mut ctx,
            &session,
            &spec,
            &scan_fixture::resolved_storage(&spec),
            &mut timers,
        )
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

/// A loopback endpoint refusing every request with a 403 whose XML body echoes `message`.
///
/// `object_store` folds a non-2xx body into its error, so a body quoting a credential (like
/// S3's `SignatureDoesNotMatch`) makes redaction observable. A 4xx is never retried.
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
        nested: None,
        physical_name: None,
    }
}

/// Scenario: A dimension-side read failure never surfaces the dimension side's credential.
#[test]
fn a_dimension_side_read_failure_redacts_the_dimension_sides_credential() {
    // The marker proves the refusal body reached the message, so the secret beside it would
    // have leaked without dimension-side redaction. Both sides carry a `logical_schema` because
    // schema inference redacts per-side and would mask the union rule under test.
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
                storage: ScanStorage::Inline(s3_backend(&dim_endpoint, "DIMSECRETVALUE")),
            }),
            storage: ScanStorage::Inline(s3_backend(&fact_endpoint, "TOPSECRETVALUE")),
            ..Default::default()
        },
        files: vec![FileEntry::new("s3://test-bucket/data/part-0.parquet", 4096)],
    };

    let mut ctx = scan_fixture::BatchCapturingCtx::declaring(
        TestContext::scalar(vec![
            Value::String(spec.to_common_json()),
            Value::String(ScanSpec::files_json(&spec.files)),
        ])
        .with_emit_policy(EmitPolicy::Reject(UdfError::User(
            "join path must use emit_batch".into(),
        ))),
        &[ExaType::Int64, scan_fixture::varchar()],
    );
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
