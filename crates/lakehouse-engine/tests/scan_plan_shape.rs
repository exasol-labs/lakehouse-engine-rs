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
use datafusion::prelude::SessionConfig;
use lakehouse_engine::scan::spec::{CatalogProps, ScanSpec, StorageProps};
use lakehouse_engine::scan::{build_raw_scan_physical_plan, session_config_for_spec};
use parquet::arrow::ArrowWriter;

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
    ScanSpec {
        files: vec![file_url],
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some(r#""ID" >= 10"#.into()),
        limit: None,
        aggregates: None,
        group_keys: None,
        emit_exa_types: Vec::new(),
        storage: StorageProps {
            endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
            allow_http: true,
            path_style: true,
        },
        catalog: CatalogProps {
            uri: "http://localhost:8181".into(),
            warehouse: "wh".into(),
            table: "db.tbl".into(),
        },
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 200,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_scan_plan_has_no_repartition_stage() {
    let dir = std::env::temp_dir().join(format!("lh_plan_shape_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);
    let spec = single_partition_spec(file_url);

    let ctx = SessionContext::new_with_config(session_config_for_spec(&spec));
    ctx.register_parquet("scan_target", &spec.files[0], Default::default())
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
        .register_parquet("scan_target", &spec.files[0], Default::default())
        .await
        .expect("register baseline parquet");
    let rows_baseline = collect_rows(&baseline_ctx, &spec).await;

    assert_eq!(
        rows_lean, rows_baseline,
        "lean repartition-free plan must produce the same rows as the un-optimized plan"
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
