//! Integration test for Task 3 — Parquet row-group & page pruning.
//!
//! Asserts two things the spec scenario "Scan enables Parquet row-group and
//! page pruning so the reader skips non-matching data" requires:
//!   1. The session config the scan UDF builds enables predicate pushdown,
//!      row-group statistics pruning, and page-index pruning (not the
//!      DataFusion defaults — `pushdown_filters` defaults OFF).
//!   2. Pruning narrows what is read, never the result set: a filtered scan
//!      returns byte-identical rows with pruning ON vs OFF.
//!
//! Host-runnable: writes a local Parquet file (multiple row groups so row-group
//! pruning is actually exercisable) and registers it through DataFusion's local
//! object store — no S3 / MinIO stack required.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use lakehouse_engine::scan::spec::{FileEntry, ScanSpec, StorageProps};
use lakehouse_engine::scan::{build_raw_scan_physical_plan, session_config_for_spec};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Write a Parquet file with several small row groups (so row-group statistics
/// pruning has something to skip) and return its `file://` URL.
fn write_local_parquet(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let path = dir.join("pruning_data.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");

    // Small row groups: 100 rows each, ids monotonically increasing, so each
    // row group's id min/max are tight and disjoint — exactly what row-group
    // statistics pruning skips when a predicate excludes a group's range.
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
        .expect("file path must be absolute")
        .to_string()
}

/// Build a ScanSpec for a single local file with a filter that excludes most
/// row groups (id range 0..1000, predicate keeps only 200..=399).
fn pruning_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        table_root: String::new(),
        files: vec![FileEntry::new(file_url, size)],
        projection: vec!["ID".into(), "NAME".into()],
        filter: Some(r#""ID" >= 200 AND "ID" < 400"#.into()),
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

async fn collect_rows(
    config: SessionConfig,
    spec: &ScanSpec,
    file_url: &str,
) -> Vec<(i64, String)> {
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet("scan_target", file_url, Default::default())
        .await
        .expect("register local parquet");
    let plan = build_raw_scan_physical_plan(&ctx, spec)
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
        // DataFusion 54's Parquet reader defaults `schema_force_view_types=true`,
        // so the string column arrives as Utf8View on the raw-scan plan (the
        // production emit path coerces it to Utf8 later). Accept either.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_enables_rowgroup_and_page_pruning() {
    let dir = std::env::temp_dir().join(format!("lh_pruning_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);
    let spec = pruning_spec(file_url.clone());

    // 1. The flags the scan UDF sets are the pruning flags, not the defaults.
    let on = session_config_for_spec(&spec);
    let parquet = &on.options().execution.parquet;
    assert!(parquet.pruning, "row-group statistics pruning must be ON");
    assert!(parquet.enable_page_index, "page-index pruning must be ON");
    assert!(
        parquet.pushdown_filters,
        "predicate pushdown must be ON (DataFusion defaults it off)"
    );

    // 2. Result parity: the same filtered scan with pruning explicitly DISABLED
    //    must produce the identical row set. Pruning changes what is read, not
    //    what is returned.
    let off = SessionConfig::new()
        .with_information_schema(false)
        .with_target_partitions(1)
        .with_batch_size(8192)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false)
        .set_bool("datafusion.execution.parquet.pushdown_filters", false);

    let rows_on = collect_rows(on, &spec, &file_url).await;
    let rows_off = collect_rows(off, &spec, &file_url).await;

    assert_eq!(
        rows_on, rows_off,
        "pruning must not change the result set (read-narrowing only)"
    );
    // The predicate keeps ids 200..=399 → exactly 200 rows.
    assert_eq!(rows_on.len(), 200, "predicate must keep exactly 200 rows");
    assert_eq!(rows_on.first().unwrap().0, 200);
    assert_eq!(rows_on.last().unwrap().0, 399);

    let _ = std::fs::remove_dir_all(&dir);
}
