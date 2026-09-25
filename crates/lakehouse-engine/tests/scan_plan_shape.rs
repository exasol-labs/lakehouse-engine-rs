mod scan_fixture;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use lakehouse_engine::adapter::pushdown::{
    AggregateMergeInputs, build_scan_driving_sql, detect_aggregates, ordinary_plans,
};
use lakehouse_engine::scan::spec::{
    AggKind, CommonScanSpec, DeleteMechanism, FileEntry, JoinSpec, JoinType, ProjectionItem,
    ScanSpec, ScanStorage, SortKey, StorageBackend, StorageProps,
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
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some(r#""ID" >= 10"#.into()),
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
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

    assert!(
        rendered.contains("DataSourceExec") || rendered.contains("ParquetExec"),
        "plan must scan Parquet:\n{rendered}"
    );
    // `pushdown_filters` may fuse the predicate into the scan as `predicate=`.
    assert!(
        rendered.contains("FilterExec") || rendered.contains("predicate="),
        "plan must carry the pushed-down filter (as FilterExec or scan predicate):\n{rendered}"
    );
    assert!(
        rendered.contains("ProjectionExec") || rendered.contains("projection="),
        "plan must carry the projection:\n{rendered}"
    );

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

/// Scenario: a projection mixing a bare column with expressions splices expressions verbatim and quotes the column
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_scan_projects_mixed_column_and_expression_items() {
    let dir = std::env::temp_dir().join(format!("lh_mixed_proj_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet_with_score(&dir);

    let mut spec = single_partition_spec(file_url);
    spec.common.filter = None;
    spec.common.projection = vec![
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

/// Scenario: scan emits a bounded local top-N when the spec carries an order-by
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_by_spec_emits_bounded_topk_not_global_sort() {
    // Both SortExec display forms contain "SortExec", so the check discriminates on
    // `TopK(fetch=` versus `SortExec: expr=[`.
    let dir = std::env::temp_dir().join(format!("lh_topn_shape_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);

    let mut spec = single_partition_spec(file_url);
    spec.common.filter = None;
    spec.common.order_by = vec![SortKey {
        column: "ID".into(),
        ascending: false,
        nulls_last: true,
    }];
    spec.common.limit = Some(5);

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

fn aggregate_spec(aggregates: Vec<lakehouse_engine::scan::spec::AggregatePlan>) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            filter: Some(
                r#""L_SHIPDATE" >= DATE '1994-01-01' AND "L_SHIPDATE" < DATE '1995-01-01'"#.into(),
            ),
            aggregates: Some(aggregates),
            storage: ScanStorage::Inline(StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            })),
            ..Default::default()
        },
        files: Vec::new(),
    }
}

/// Scenario: `SUM(col * col)` pushes down as a partial/merge aggregate sized from the declared type
#[test]
fn sum_two_column_product_emits_aggregates_not_raw_scan() {
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

    let items = detect_aggregates(&req)
        .expect("SUM(col * col) must decompose to an aggregate plan, not a row scan");
    let plans = ordinary_plans(&items);
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

    let spec = aggregate_spec(plans);
    let shards = vec![vec![("lineitem/data/f0.parquet".to_string(), 4096u64)]];
    let merge_inputs = AggregateMergeInputs::new(
        vec!["DECIMAL(36,4)".to_string()],
        vec![r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,4))"#.to_string()],
        None,
    )
    .expect("one merge item for one select-list item");
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &[],
        &[],
        None,
        &[],
        Some(&merge_inputs),
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_DISTRIBUTE_FILES",
    );

    assert!(
        sql.contains(r#""PARTIAL_sum_0" DECIMAL(36,4)"#),
        "partial SUM column must be the declared DECIMAL(36,4):\n{sql}"
    );
    assert!(
        sql.contains("arg_expr"),
        "the aggregate plan must carry the product argument (arg_expr):\n{sql}"
    );
    assert!(
        !sql.contains("SELECT * FROM"),
        "must not be a raw two-column row-scan fallback:\n{sql}"
    );
}

fn row_scan_spec() -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            storage: ScanStorage::Inline(test_storage()),
            ..Default::default()
        },
        files: Vec::new(),
    }
}

/// Scenario: scan-driving query fans out via a nested distributor over a scalar scan UDF
#[test]
fn row_scan_fans_out_via_nested_distributor_over_scalar_scan() {
    let proj = vec![ProjectionItem::Column("L_ORDERKEY".into())];
    let types = vec!["DECIMAL(20,0)".to_string()];
    let spec = row_scan_spec();

    let shards = vec![
        vec![("data/part-0.parquet".to_string(), 1000u64)],
        vec![("data/part-1.parquet".to_string(), 1000u64)],
    ];
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &proj,
        &types,
        None,
        &[],
        None,
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_DISTRIBUTE_FILES",
    );

    assert!(
        sql.starts_with("SELECT LAKEHOUSE_SCAN("),
        "row scan must be driven directly by the outer scalar scan:\n{sql}"
    );
    assert!(
        sql.contains("FROM (SELECT LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES"),
        "the distributor subquery must be nested inside the outer scan's FROM:\n{sql}"
    );
    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key)"),
        "GROUP BY shard_key must be nested inside the distributor, not top-level:\n{sql}"
    );
    assert!(
        !sql.contains("SELECT * FROM"),
        "row scan must not have a SELECT * materializing wrapper:\n{sql}"
    );
    assert!(
        sql.contains("data/part-0.parquet") && sql.contains("data/part-1.parquet"),
        "both shards' files must appear in the distributor's VALUES list:\n{sql}"
    );
}

/// Scenario: ordered top-N attaches ORDER BY … LIMIT to the outer scalar select, after the fan-out
#[test]
fn topn_order_by_limit_attaches_to_outer_scalar_select() {
    let proj = vec![ProjectionItem::Column("L_EXTENDEDPRICE".into())];
    let types = vec!["DECIMAL(18,2)".to_string()];
    let mut spec = row_scan_spec();
    spec.common.order_by = vec![SortKey {
        column: "L_EXTENDEDPRICE".into(),
        ascending: false,
        nulls_last: true,
    }];
    spec.common.limit = Some(20);

    let shards = vec![
        vec![("data/part-0.parquet".to_string(), 1000u64)],
        vec![("data/part-1.parquet".to_string(), 1000u64)],
    ];
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &proj,
        &types,
        Some(20),
        &[],
        None,
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_DISTRIBUTE_FILES",
    );

    let fan_out_close = sql
        .find("GROUP BY shard_key)")
        .expect("the distributor's GROUP BY shard_key fan-out must close before ORDER BY");
    let order_by_pos = sql
        .find("ORDER BY")
        .expect("the outer scalar select must carry an ORDER BY");
    assert!(
        order_by_pos > fan_out_close,
        "ORDER BY must attach AFTER the nested distributor's fan-out closes, not inside it:\n{sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "L_EXTENDEDPRICE" DESC NULLS LAST LIMIT 20"#),
        "ORDER BY must render direction + NULL placement and precede LIMIT:\n{sql}"
    );
    assert!(
        sql.trim_end().ends_with("LIMIT 20"),
        "LIMIT must be the final clause of the outer scalar select:\n{sql}"
    );
    assert!(
        !sql.contains("SELECT * FROM"),
        "ordered top-N must not have a SELECT * materializing wrapper:\n{sql}"
    );
}

fn test_storage() -> StorageBackend {
    StorageBackend::S3(StorageProps {
        endpoint: "http://minio:9000".to_string(),
        region: "us-east-1".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        allow_http: true,
        ..Default::default()
    })
}

/// Scenario: broadcast-eligible inner equi-join is planned as a broadcast fan-out
#[test]
fn broadcast_fact_side_uses_distributor_scalar_scan() {
    let join = JoinSpec {
        table_root: "s3://warehouse/lh/customer".to_string(),
        files: vec![FileEntry::new("data/cust-0.parquet", 4096)],
        logical_schema: Vec::new(),
        name_mapping: Vec::new(),
        join_type: JoinType::Inner,
        condition: r#"("C_CUSTKEY" = "O_CUSTKEY")"#.to_string(),
        post_join_limit: None,
        partition_columns: Vec::new(),
        storage: ScanStorage::Inline(test_storage()),
    };

    let spec = ScanSpec {
        common: CommonScanSpec {
            table_root: "s3://warehouse/lh/orders".to_string(),
            projection: vec![
                ProjectionItem::Column("C_NAME".into()),
                ProjectionItem::Column("O_ORDERDATE".into()),
            ],
            join: Some(join),
            storage: ScanStorage::Inline(test_storage()),
            ..Default::default()
        },
        files: vec![],
    };

    let shards = vec![
        vec![("data/ord-0.parquet".to_string(), 8192u64)],
        vec![("data/ord-1.parquet".to_string(), 8192u64)],
    ];
    let proj = spec.common.projection.clone();
    let types = vec!["VARCHAR(100)".to_string(), "DATE".to_string()];
    let sql = build_scan_driving_sql(
        &spec,
        &shards,
        &proj,
        &types,
        None,
        &[],
        None,
        "LAKEHOUSE_SCAN",
        "LAKEHOUSE_DISTRIBUTE_FILES",
    );

    assert!(
        sql.starts_with("SELECT LAKEHOUSE_SCAN("),
        "broadcast join must drive the fact side through the outer ungrouped scalar scan:\n{sql}"
    );
    assert!(
        sql.contains("FROM (SELECT LAKEHOUSE_DISTRIBUTE_FILES(files) FROM (VALUES")
            && sql.contains("AS shards(shard_key, files) GROUP BY shard_key)"),
        "the fact side's GROUP BY shard_key fan-out must be nested inside the distributor subquery:\n{sql}"
    );
    assert!(
        !sql.contains("SELECT * FROM"),
        "no materializing SELECT * wrapper over the broadcast fan-out:\n{sql}"
    );
    assert!(
        sql.contains(r#"EMITS ("C_NAME" VARCHAR(100), "O_ORDERDATE" DATE)"#),
        "the EMITS clause must span both tables in projection order:\n{sql}"
    );
    // The common blob is a single-quoted SQL literal, so JSON keys keep raw double quotes.
    assert!(
        sql.contains(r#""join":{"#),
        "the common blob must carry a join block:\n{sql}"
    );
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
    assert!(
        sql.contains("data/ord-0.parquet") && sql.contains("data/ord-1.parquet"),
        "the fact side must be sharded across the VALUES work units:\n{sql}"
    );
}

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

/// Deletes `pos = 250`, inside a row group the pruning predicate keeps.
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

/// Scenario: a delete-carrying scan stays single-partition and still prunes row groups with an access plan attached
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_plan_lean_and_prunes_with_access_plan() {
    // Uses `register_files`: `register_parquet` never attaches a base `ParquetAccessPlan`.
    let dir = std::env::temp_dir().join(format!("lh_gate_access_plan_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let data_url = write_multi_row_group_parquet(&dir);
    let delete_url = write_positional_delete_parquet(&dir, &data_url);

    let mut spec = single_partition_spec(data_url.clone());
    spec.files = vec![FileEntry::with_deletes(
        data_url.clone(),
        local_size(&data_url),
        vec![DeleteMechanism::IcebergPositionalDelete {
            path: delete_url.clone(),
            size: local_size(&delete_url),
        }],
    )];
    spec.common.filter = Some(r#""ID" >= 200 AND "ID" < 400"#.into());

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    let storage = scan_fixture::resolved_storage(&spec);
    register_files(&ctx, "scan_target", &spec, &storage)
        .await
        .expect("register production positional-delete provider");
    let plan = build_raw_scan_physical_plan(&ctx, &spec)
        .await
        .expect("build physical plan through the delete-carrying provider");

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

    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect delete-carrying scan");

    let pruned = sum_row_groups_pruned(plan.as_ref());
    eprintln!("row_groups_pruned_statistics (pruned) = {pruned}");
    assert!(
        pruned > 0,
        "row-group pruning must STILL occur with a base ParquetAccessPlan attached \
         (deletes must compose WITH pruning, not defeat it); pruned = {pruned}\n{rendered}"
    );

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
