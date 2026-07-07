//! Integration test for Task 2 — repartition-free raw-scan pipeline.
//!
//! Spec scenario "Raw-scan physical plan carries no needless repartition or
//! coalesce-partitions stage": with `df_target_partitions == 1` the committed
//! raw-row pipeline is `ParquetExec → FilterExec → ProjectionExec →
//! CoalesceBatchesExec` and contains NO `RepartitionExec`,
//! `CoalescePartitionsExec`, global `SortExec`, or global aggregate.
//!
//! Host-runnable: writes a local Parquet file and inspects the displayable
//! physical plan the production raw-scan path builds.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use lakehouse_engine::adapter::pushdown::{build_scan_driving_sql, detect_aggregates};
use lakehouse_engine::scan::spec::{
    AggKind, DeleteFileContentType, DeleteFileRef, FileEntry, JoinSpec, JoinType, ProjectionItem,
    ScanSpec, SortKey, StorageProps,
};
use lakehouse_engine::scan::{
    build_raw_scan_physical_plan, register_files, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

fn write_local_parquet(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join("plan_shape.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (0..100).collect();
    let names: Vec<String> = (0..100).map(|i| format!("row-{i}")).collect();
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

fn single_partition_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some(r#""ID" >= 10"#.into()),
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        join: None,
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_scan_plan_has_no_repartition_stage() {
    let dir = std::env::temp_dir().join(format!("lh_plan_shape_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);
    let spec = single_partition_spec(file_url);

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    ctx.register_parquet("scan_target", &spec.files[0].path, Default::default())
        .await
        .expect("register local parquet");

    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("build physical plan");

    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    eprintln!("=== raw-scan physical plan ===\n{rendered}\n=============================");

    // No stage may redistribute or re-buffer rows beyond projection / filter /
    // batch coalescing on the single-partition raw-scan path.
    for forbidden in [
        "RepartitionExec",
        "CoalescePartitionsExec",
        "SortExec",
        "AggregateExec",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "single-partition raw-scan plan must not contain {forbidden}:\n{rendered}"
        );
    }

    // The lean pipeline scans Parquet (DataFusion 54 renamed `ParquetExec` to a
    // `DataSourceExec` over a ParquetSource).
    assert!(
        rendered.contains("DataSourceExec") || rendered.contains("ParquetExec"),
        "plan must scan Parquet:\n{rendered}"
    );
    // The pushed-down predicate is carried — either as a standalone `FilterExec`
    // or, when `pushdown_filters` fuses it into the scan (the leaner outcome of
    // Task 3), as a `predicate=` clause on the Parquet source. Either form
    // satisfies "no stage re-buffers beyond what filter requires"; the predicate
    // applying inside the scan is strictly better than a separate FilterExec.
    assert!(
        rendered.contains("FilterExec") || rendered.contains("predicate="),
        "plan must carry the pushed-down filter (as FilterExec or scan predicate):\n{rendered}"
    );
    // The projection (uppercase SELECT list) is present — either as a
    // standalone `ProjectionExec` or fused into the scan `projection=`.
    assert!(
        rendered.contains("ProjectionExec") || rendered.contains("projection="),
        "plan must carry the projection:\n{rendered}"
    );

    // Result parity: the lean single-partition plan returns the same rows as a
    // baseline plan with the optimizations turned off (multi-partition,
    // pushdown disabled). Pruning / repartition-elision narrow what is read and
    // how rows flow, never the result set.
    let rows_lean = collect_rows(&ctx, &spec).await;

    let baseline_config = SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(4)
        .with_batch_size(8192)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false)
        .set_bool("datafusion.execution.parquet.pushdown_filters", false);
    let baseline_ctx = SessionContext::new_with_config(baseline_config);
    baseline_ctx
        .register_parquet("scan_target", &spec.files[0].path, Default::default())
        .await
        .expect("register baseline parquet");
    let rows_baseline = collect_rows(&baseline_ctx, &spec).await;

    assert_eq!(
        rows_lean, rows_baseline,
        "lean repartition-free plan must produce the same rows as the un-optimized plan"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Write a Parquet file with `id` (Int64), `score` (Float64), `name` (Utf8) and
/// return its `file://` URL. Backs the mixed-projection regression test.
fn write_local_parquet_with_score(dir: &std::path::Path) -> String {
    use arrow::array::Float64Array;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join("mixed_projection.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let ids: Vec<i64> = (1..=3).collect();
    let scores: Vec<f64> = vec![5.0, 10.0, 15.0];
    let names: Vec<String> = (1..=3).map(|i| format!("event-{i:02}")).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(scores)),
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

/// Regression (host-runnable) for the raw-scan expression-projection bug: a
/// projection that mixes a bare column with rendered scalar expressions must be
/// spliced into the scan SELECT correctly — the `Expr` items VERBATIM, the
/// `Column` item quoted as an identifier. Before the fix, `build_scan_sql`
/// quoted every projection entry as an identifier, so `("SCORE" * 2)` became a
/// phantom column name and DataFusion rejected the plan with
/// `No field named "(""SCORE"" * 2)"`.
///
/// Mirrors the E2E `e2e_selectlist_expression_pushdown`
/// (`SELECT id, score * 2.0, UPPER(name) ...`) without needing the Exasol stack.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_scan_projects_mixed_column_and_expression_items() {
    let dir = std::env::temp_dir().join(format!("lh_mixed_proj_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet_with_score(&dir);

    let mut spec = single_partition_spec(file_url);
    spec.filter = None;
    // id (bare column), score * 2 (expression), UPPER(name) (expression) — the
    // expressions reference the uppercase-aliased inner columns, exactly as the
    // adapter's `render_expression` emits them.
    spec.projection = vec![
        ProjectionItem::Column("ID".into()),
        ProjectionItem::Expr {
            expr: r#"("SCORE" * 2)"#.into(),
        },
        ProjectionItem::Expr {
            expr: r#"UPPER("NAME")"#.into(),
        },
    ];

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    ctx.register_parquet("scan_target", &spec.files[0].path, Default::default())
        .await
        .expect("register local parquet");

    // Before the fix this errored at plan build with the phantom-identifier
    // schema error; it must now build and evaluate the expressions.
    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("mixed column+expression projection must build a valid scan plan");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("collect");

    let mut rows: Vec<(i64, f64, String)> = Vec::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 3, "projection must emit 3 columns");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col 0 Int64 (ID)");
        let doubled = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("col 1 Float64 (score * 2)");
        let upper_col = batch.column(2);
        let upper_at: Box<dyn Fn(usize) -> String> =
            if let Some(v) = upper_col.as_any().downcast_ref::<StringViewArray>() {
                Box::new(move |i| v.value(i).to_string())
            } else if let Some(s) = upper_col.as_any().downcast_ref::<StringArray>() {
                Box::new(move |i| s.value(i).to_string())
            } else {
                panic!(
                    "col 2 must be a string array, got {:?}",
                    upper_col.data_type()
                );
            };
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), doubled.value(i), upper_at(i)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, 10.0, "EVENT-01".to_string()),
            (2, 20.0, "EVENT-02".to_string()),
            (3, 30.0, "EVENT-03".to_string()),
        ],
        "the expression columns must evaluate: score*2 and UPPER(name)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Build and collect the raw-scan plan's rows as sorted `(id, name)` tuples.
async fn collect_rows(ctx: &SessionContext, spec: &ScanSpec) -> Vec<(i64, String)> {
    let plan = build_raw_scan_physical_plan(ctx, spec)
        .await
        .expect("build physical plan");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("collect");
    let mut rows: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col 0 Int64");
        let name_col = batch.column(1);
        let name_at: Box<dyn Fn(usize) -> String> =
            if let Some(v) = name_col.as_any().downcast_ref::<StringViewArray>() {
                Box::new(move |i| v.value(i).to_string())
            } else if let Some(s) = name_col.as_any().downcast_ref::<StringArray>() {
                Box::new(move |i| s.value(i).to_string())
            } else {
                panic!(
                    "col 1 must be a string array, got {:?}",
                    name_col.data_type()
                );
            };
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), name_at(i)));
        }
    }
    rows.sort();
    rows
}

/// Scenario `scan-exec: Scan emits a bounded local top-N when the spec carries an
/// order-by`: with `order_by` + `limit` set, the production raw-scan pipeline folds
/// `ORDER BY <col> LIMIT n` into a bounded, fetch-limited `SortExec` — a TopK — NOT
/// an unbounded global sort that materializes and sorts every row.
///
/// Per decision-log A3, BOTH the bounded and unbounded `SortExec` display forms
/// contain the bare substring `"SortExec"`, so a blanket `!contains("SortExec")`
/// check is insufficient. The discriminating assertions are the TopK-specific
/// substring `"TopK(fetch="` (present) and the unbounded-form prefix
/// `"SortExec: expr=["` (absent).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_spec_emits_bounded_topk_not_global_sort() {
    let dir = std::env::temp_dir().join(format!("lh_topn_shape_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);

    let mut spec = single_partition_spec(file_url);
    spec.filter = None;
    // ORDER BY ID DESC NULLS LAST LIMIT 5 over the 100-row fixture.
    spec.order_by = vec![SortKey {
        column: "ID".into(),
        ascending: false,
        nulls_last: true,
    }];
    spec.limit = Some(5);

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    ctx.register_parquet("scan_target", &spec.files[0].path, Default::default())
        .await
        .expect("register local parquet");

    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("build physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    eprintln!(
        "=== ordered top-N physical plan ===\n{rendered}\n==================================="
    );

    assert!(
        rendered.contains("TopK(fetch="),
        "ordered top-N plan must fold ORDER BY + LIMIT into a bounded TopK:\n{rendered}"
    );
    assert!(
        !rendered.contains("SortExec: expr=["),
        "ordered top-N plan must not contain an unbounded global SortExec:\n{rendered}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A minimal aggregate-carrying spec template (no files/storage detail matters
/// for the SQL-shape assertion; `aggregates` drives the aggregate branch).
fn aggregate_spec(aggregates: Vec<lakehouse_engine::scan::spec::AggregatePlan>) -> ScanSpec {
    ScanSpec {
        table_root: String::new(),
        files: Vec::new(),
        projection: Vec::new(),
        filter: Some(
            r#""L_SHIPDATE" >= DATE '1994-01-01' AND "L_SHIPDATE" < DATE '1995-01-01'"#.into(),
        ),
        limit: None,
        order_by: Vec::new(),
        aggregates: Some(aggregates),
        group_keys: None,
        emit_exa_types: Vec::new(),
        logical_schema: Vec::new(),
        join: None,
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    }
}

/// Plan-shape (host-runnable): the NQ1 shape `SUM(L_EXTENDEDPRICE * L_DISCOUNT)`
/// pushes down as a decomposed partial/merge aggregate — the driving SQL carries
/// the `aggregates` plan with the product in `arg_expr` and a `PARTIAL_sum_0`
/// partial column, NOT a raw two-column row-scan fallback. The partial column and
/// the merge CAST are both sized from Exasol's declared DECIMAL(36,4) result type
/// (decision-log entry [7]), verifying the DECIMAL-with-nonzero-scale path.
#[test]
fn sum_two_column_product_emits_aggregates_not_raw_scan() {
    // The pushdown request Exasol sends once FN_MULT is advertised: SUM over a
    // MULT function_scalar of two columns (no GROUP BY → single-group aggregate).
    let req = serde_json::json!({
        "selectList": [{
            "type": "function_aggregate",
            "name": "SUM",
            "distinct": false,
            "arguments": [{
                "type": "function_scalar",
                "name": "MULT",
                "arguments": [
                    {"type": "column", "name": "L_EXTENDEDPRICE"},
                    {"type": "column", "name": "L_DISCOUNT"},
                ],
            }],
        }]
    });

    // Detection must decompose the aggregate — `None` here would mean the raw
    // two-column row-scan fallback (Exasol would aggregate itself).
    let plans = detect_aggregates(&req)
        .expect("SUM(col * col) must decompose to an aggregate plan, not a row scan");
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].kind, AggKind::Sum);
    assert!(
        plans[0].column.is_none() && plans[0].arg_expr.is_some(),
        "the two-column product must be carried in arg_expr, not as a bare column"
    );
    assert_eq!(
        plans[0].arg_expr.as_deref(),
        Some(r#"("L_EXTENDEDPRICE" * "L_DISCOUNT")"#)
    );

    // Build the driving SQL through the real single-group aggregate path, with
    // Exasol's declared DECIMAL(36,4) result type for the SUM ordinal.
    let spec = aggregate_spec(plans);
    let shards = vec![vec![("lineitem/data/f0.parquet".to_string(), 4096u64)]];
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &[],                            // proj cols — unused on the aggregate path
        &[],                            // proj types — unused on the aggregate path
        None,                           // limit
        &[],                            // col_types — a product has no source column
        &["DECIMAL(36,4)".to_string()], // Exasol's declared SUM result type
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_MERGE",
    );

    // Partial column widened from the declared type (NOT recomputed from operands).
    assert!(
        sql.contains(r#""PARTIAL_sum_0" DECIMAL(36,4)"#),
        "partial SUM column must be the declared DECIMAL(36,4):\n{sql}"
    );
    // Merge casts the summed partial back to the declared DECIMAL(36,4).
    assert!(
        sql.contains(r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#),
        "merge must cast to the declared DECIMAL(36,4):\n{sql}"
    );
    // The rendered product travels in the scan spec's serialized aggregate plan.
    assert!(
        sql.contains("arg_expr"),
        "the aggregate plan must carry the product argument (arg_expr):\n{sql}"
    );
    // NOT a raw row-scan fallback: the aggregate path wraps the fan-out in an
    // outer merge SELECT, never `SELECT * FROM (SELECT ...)` over raw columns.
    assert!(
        !sql.contains("SELECT * FROM"),
        "must not be a raw two-column row-scan fallback:\n{sql}"
    );
}

/// Minimal MinIO-style storage for spec construction (no secrets asserted here).
fn test_storage() -> StorageProps {
    StorageProps {
        endpoint: "http://minio:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        session_token: None,
        allow_http: true,
        path_style: true,
    }
}

/// pushdown-planning-join "Broadcast-eligible inner equi-join is planned as a
/// broadcast fan-out". The broadcast plan shards ONLY the fact side and carries the
/// dimension side's FULL file list once in the shard-invariant common blob's join
/// block (`ScanSpec.join`), so the generated fan-out is exactly the single-table
/// `GROUP BY shard_key` fan-out with the join block riding along in the common blob:
/// every shard invocation re-scans the same dimension side and joins it node-locally.
#[test]
fn join_broadcast_fan_out_sql_shape() {
    // Dimension side: full file list, carried once (shard-invariant) in the join block.
    let join = JoinSpec {
        table_root: "s3://warehouse/lh/customer".to_string(),
        files: vec![FileEntry::new("data/cust-0.parquet", 4096)],
        logical_schema: Vec::new(),
        join_type: JoinType::Inner,
        condition: r#"("C_CUSTKEY" = "O_CUSTKEY")"#.to_string(),
    };

    let spec = ScanSpec {
        table_root: "s3://warehouse/lh/orders".to_string(),
        files: vec![], // replaced per shard
        projection: vec![
            ProjectionItem::Column("C_NAME".into()),
            ProjectionItem::Column("O_ORDERDATE".into()),
        ],
        filter: None,
        limit: None,
        order_by: Vec::new(),
        aggregates: None,
        group_keys: None,
        emit_exa_types: vec!["VARCHAR(100)".to_string(), "DATE".to_string()],
        logical_schema: Vec::new(),
        join: Some(join),
        storage: test_storage(),
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
        s3_max_connections: 8,
    };

    // Fact side sharded into two byte-balanced work units → a real GROUP BY fan-out.
    let shards = vec![
        vec![("data/ord-0.parquet".to_string(), 8192u64)],
        vec![("data/ord-1.parquet".to_string(), 8192u64)],
    ];
    let proj = spec.projection.clone();
    let types = spec.emit_exa_types.clone();
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &proj,
        &types,
        None,
        &[],
        &[],
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_MERGE",
    );

    // The fan-out is the single-table GROUP BY shard_key shape.
    assert!(
        sql.contains("GROUP BY shard_key") && sql.contains("AS shards(shard_key, files)"),
        "broadcast join must drive the fact side through the GROUP BY shard_key fan-out:\n{sql}"
    );
    // EMITS the cross-table projection in order and type.
    assert!(
        sql.contains(r#"EMITS ("C_NAME" VARCHAR(100), "O_ORDERDATE" DATE)"#),
        "the EMITS clause must span both tables in projection order:\n{sql}"
    );
    // The join block rides in the shard-invariant common blob (serialized once).
    // The common blob is a single-quoted SQL literal, so JSON object keys appear
    // with raw (unescaped) double quotes.
    assert!(
        sql.contains(r#""join":{"#),
        "the common blob must carry a join block:\n{sql}"
    );
    // The dimension side's full file list and the rendered condition are carried once.
    assert!(
        sql.contains("data/cust-0.parquet"),
        "the dimension side's file list must ride in the common blob:\n{sql}"
    );
    assert!(
        sql.contains(r#"(\"C_CUSTKEY\" = \"O_CUSTKEY\")"#),
        "the rendered join condition must ride in the common blob:\n{sql}"
    );
    assert!(
        sql.contains(r#""join_type":"inner""#),
        "the join block must declare an inner join:\n{sql}"
    );
    // Only the fact side is sharded per work unit; the dimension side is NOT
    // partitioned into the per-shard VALUES rows.
    assert!(
        sql.contains("data/ord-0.parquet") && sql.contains("data/ord-1.parquet"),
        "the fact side must be sharded across the VALUES work units:\n{sql}"
    );
}

/// Write a data Parquet with ten tight, disjoint row groups (100 rows each,
/// `id` monotonically increasing 0..1000) so row-group statistics pruning has
/// something to skip, and return its `file://` URL. Mirrors the fixture the
/// dedicated pruning test uses, so the two tests prove pruning fires on the
/// SAME shape — this one additionally with a base `ParquetAccessPlan` attached.
fn write_multi_row_group_parquet(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let path = dir.join("access_plan_pruning.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let total = 1_000i64;
    let ids: Vec<i64> = (0..total).collect();
    let names: Vec<String> = (0..total).map(|i| format!("row-{i:04}")).collect();
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

/// Write an Iceberg-shaped positional-delete Parquet (`file_path`/`pos` columns,
/// located by the reserved field-ids 2147483546 / 2147483545) that deletes the
/// single row `pos = 250` of `data_file_url`, and return its `file://` URL.
///
/// `pos = 250` lands inside row group 2 (rows 200..300) — a row group the
/// pruning predicate KEEPS — so the delete is applied precisely where pruning
/// does NOT remove the group, proving deletes compose WITH (never replace)
/// pruning. `file_path` carries the full data-file URL because the provider
/// filters delete rows by exact `file_path` equality (partition-granularity).
fn write_positional_delete_parquet(dir: &std::path::Path, data_file_url: &str) -> String {
    use std::collections::HashMap;
    let field_id_meta =
        |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(field_id_meta(2_147_483_546)),
        Field::new("pos", DataType::Int64, false).with_metadata(field_id_meta(2_147_483_545)),
    ]));
    let path = dir.join("access_plan_pruning-delete.parquet");
    let file = std::fs::File::create(&path).expect("create delete parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![data_file_url])),
            Arc::new(Int64Array::from(vec![250_i64])),
        ],
    )
    .expect("delete record batch");
    writer.write(&batch).expect("write delete batch");
    writer.close().expect("close delete writer");
    url::Url::from_file_path(&path)
        .expect("absolute path")
        .to_string()
}

fn local_size(file_url: &str) -> u64 {
    std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(file_url))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Sum the `row_groups_pruned_statistics` pruned count across every node of the
/// executed physical plan. A non-zero total is direct evidence that Parquet
/// row-group statistics pruning fired.
fn sum_row_groups_pruned(plan: &dyn ExecutionPlan) -> usize {
    let mut total = 0;
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } = metric.value()
                && name == "row_groups_pruned_statistics"
            {
                total += pruning_metrics.pruned();
            }
        }
    }
    for child in plan.children() {
        total += sum_row_groups_pruned(child.as_ref());
    }
    total
}

/// GATE test (plan Task 4.2) for the unified-vs-conditional provider decision
/// (Task 2.1). It asserts BOTH halves of the gate on the SAME scan — a
/// delete-carrying data file driven through the real production
/// `PositionalDeleteScanTable` provider (registered via `register_files`, the
/// exact production seam; the built-in `register_parquet` shortcut the other
/// plan-shape tests use never attaches a base `ParquetAccessPlan`):
///
/// 1. **Lean shape preserved** — the raw-scan physical plan has exactly ONE
///    output partition and contains NO `RepartitionExec` and NO
///    `CoalescePartitionsExec`, even with the access plan attached.
/// 2. **Pruning preserved WITH a base access plan** — row-group statistics
///    pruning STILL fires (`row_groups_pruned_statistics` > 0) when a base
///    `ParquetAccessPlan` (from the positional delete) is attached, i.e. the
///    opener intersects pruning ON TOP of the delete's access plan rather than
///    the access plan defeating pruning.
///
/// The result set proves the two compose correctly: the predicate keeps ids
/// 200..=399 (row groups 2 and 3; the other eight are pruned) and the delete
/// removes id 250 from the KEPT row group 2 — so exactly 199 rows survive and
/// 250 is absent. Were the access plan silently dropped, 250 would remain and
/// the count would be 200; were pruning defeated, the pruned metric would be 0.
///
/// If this test fails, the unified provider path is NOT safe and the plan's
/// conditional fallback (`ListingTable` for delete-free files, custom provider
/// only when deletes are present) must be adopted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_plan_lean_and_prunes_with_access_plan() {
    let dir = std::env::temp_dir().join(format!("lh_gate_access_plan_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let data_url = write_multi_row_group_parquet(&dir);
    let delete_url = write_positional_delete_parquet(&dir, &data_url);

    // A single delete-carrying data file: the provider will read the delete
    // file, build a base `ParquetAccessPlan` skipping pos 250, and attach it.
    let mut spec = single_partition_spec(data_url.clone());
    spec.files = vec![FileEntry::with_deletes(
        data_url.clone(),
        local_size(&data_url),
        vec![DeleteFileRef {
            path: delete_url.clone(),
            size: local_size(&delete_url),
            content_type: DeleteFileContentType::PositionDeletes,
        }],
    )];
    // Predicate keeps only ids 200..=399 → row groups 2 and 3; prunes the other
    // eight tight, disjoint row groups.
    spec.filter = Some(r#""ID" >= 200 AND "ID" < 400"#.into());

    // Register the REAL production provider (attaches the access plan), then ask
    // for the exact committed raw-scan pipeline.
    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    register_files(&ctx, "scan_target", &spec)
        .await
        .expect("register production positional-delete provider");
    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("build physical plan through the delete-carrying provider");

    // GATE part 1: the lean single-partition shape survives the access plan.
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    eprintln!(
        "=== gate raw-scan physical plan ===\n{rendered}\n==================================="
    );
    for forbidden in ["RepartitionExec", "CoalescePartitionsExec"] {
        assert!(
            !rendered.contains(forbidden),
            "delete-carrying raw-scan plan must stay lean (no {forbidden}):\n{rendered}"
        );
    }
    assert_eq!(
        plan.output_partitioning().partition_count(),
        1,
        "delete-carrying raw-scan plan must have exactly one output partition:\n{rendered}"
    );

    // Execute; pruning + delete metrics accumulate on the plan tree.
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect delete-carrying scan");

    // GATE part 2: row-group statistics pruning STILL fires with the base
    // access plan attached (the novel assertion this gate exists for).
    let pruned = sum_row_groups_pruned(plan.as_ref());
    eprintln!("row_groups_pruned_statistics (pruned) = {pruned}");
    assert!(
        pruned > 0,
        "row-group pruning must STILL occur with a base ParquetAccessPlan attached \
         (deletes must compose WITH pruning, not defeat it); pruned = {pruned}\n{rendered}"
    );

    // Composition correctness: the kept predicate range minus the deleted row.
    let mut rows: Vec<i64> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col 0 Int64");
        for i in 0..batch.num_rows() {
            rows.push(ids.value(i));
        }
    }
    rows.sort_unstable();

    assert!(
        !rows.contains(&250),
        "the positional delete must remove id 250 (the access plan WAS applied)"
    );
    assert_eq!(
        rows.len(),
        199,
        "predicate keeps ids 200..=399 (200 rows) and the delete removes id 250 → 199 rows"
    );
    assert_eq!(*rows.first().unwrap(), 200, "lowest surviving id is 200");
    assert_eq!(*rows.last().unwrap(), 399, "highest surviving id is 399");
    let expected: Vec<i64> = (200..=399).filter(|&id| id != 250).collect();
    assert_eq!(
        rows, expected,
        "surviving rows must be exactly ids 200..=399 except the deleted 250"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
