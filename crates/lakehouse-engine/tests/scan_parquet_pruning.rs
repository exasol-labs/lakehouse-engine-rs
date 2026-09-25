mod scan_fixture;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::prelude::SessionConfig;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NestedMembers, ProjectionItem, ScanSpec, ScanStorage,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_raw_scan_physical_plan, register_files, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn write_local_parquet(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let path = dir.join("pruning_data.parquet");
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
        .expect("file path must be absolute")
        .to_string()
}

fn pruning_spec(file_url: String) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    ScanSpec {
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some(r#""ID" >= 200 AND "ID" < 400"#.into()),
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
        // `schema_force_view_types` defaults on, so strings may arrive as Utf8View.
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

    let on = session_config_for_spec(&spec);
    let parquet = &on.options().execution.parquet;
    assert!(parquet.pruning, "row-group statistics pruning must be ON");
    assert!(parquet.enable_page_index, "page-index pruning must be ON");
    assert!(
        parquet.pushdown_filters,
        "predicate pushdown must be ON (DataFusion defaults it off)"
    );

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
    assert_eq!(rows_on.len(), 200, "predicate must keep exactly 200 rows");
    assert_eq!(rows_on.first().unwrap().0, 200);
    assert_eq!(rows_on.last().unwrap().0, 399);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Row 1's leaf stats are `min = "hello"`, `max = "world"`, which a min/max check would use to
/// falsely exclude the rendered `["hello","world"]` (`[` sorts below `h`). Page stats and a
/// bloom filter are written so zero-pruned results are evidence, not a missing index.
fn write_nested_parquet(dir: &std::path::Path, rows_per_group: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let path = dir.join(format!("nested_{rows_per_group}.parquet"));
    let file = std::fs::File::create(&path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows_per_group))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");

    let mut tags = ListBuilder::new(StringBuilder::new());
    for row in [vec!["hello", "world"], vec!["zzz"]] {
        for value in row {
            tags.values().append_value(value);
        }
        tags.append(true);
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(tags.finish()),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    url::Url::from_file_path(&path)
        .expect("file path must be absolute")
        .to_string()
}

fn nested_spec(file_url: String, filter: &str) -> ScanSpec {
    let size = std::fs::metadata(file_url.strip_prefix("file://").unwrap_or(&file_url))
        .map(|m| m.len())
        .unwrap_or(0);
    let field = |name: &str, arrow_type: &str, nested: Option<NestedMembers>| LogicalField {
        field_id: None,
        name: name.into(),
        arrow_type: arrow_type.into(),
        nullable: true,
        initial_default: None,
        nested,
        physical_name: None,
    };
    ScanSpec {
        common: CommonScanSpec {
            projection: vec![
                ProjectionItem::Column("ID".into()),
                ProjectionItem::Column("TAGS".into()),
            ],
            filter: Some(filter.into()),
            logical_schema: vec![
                field("id", "int64", None),
                field("tags", "utf8", Some(NestedMembers::List { element: None })),
            ],
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

async fn run_nested_scan(spec: &ScanSpec) -> (Vec<(i64, String)>, Arc<dyn ExecutionPlan>) {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file://").expect("file scheme"),
        Arc::new(LocalFileSystem::new()),
    );
    register_files(
        &ctx,
        "scan_target",
        spec,
        &scan_fixture::resolved_storage(spec),
    )
    .await
    .expect("register production nested-column provider");
    let plan = build_raw_scan_physical_plan(&ctx, spec)
        .await
        .expect("build physical plan");
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect nested scan");

    let mut rows: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col 0 Int64");
        let rendered = batch.column(1);
        let tag_at: Box<dyn Fn(usize) -> String> =
            if let Some(v) = rendered.as_any().downcast_ref::<StringViewArray>() {
                Box::new(move |i| v.value(i).to_string())
            } else if let Some(s) = rendered.as_any().downcast_ref::<StringArray>() {
                Box::new(move |i| s.value(i).to_string())
            } else {
                panic!(
                    "the rendered nested column must be a string array, got {:?}",
                    rendered.data_type()
                );
            };
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), tag_at(i)));
        }
    }
    rows.sort();
    (rows, plan)
}

/// `None` when no node reports the metric: a renamed metric must not read as zero pruned.
fn sum_pruned(plan: &dyn ExecutionPlan, metric_name: &str) -> Option<usize> {
    let mut total: Option<usize> = None;
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } = metric.value()
                && name == metric_name
            {
                total = Some(total.unwrap_or(0) + pruning_metrics.pruned());
            }
        }
    }
    for child in plan.children() {
        if let Some(pruned) = sum_pruned(child.as_ref(), metric_name) {
            total = Some(total.unwrap_or(0) + pruned);
        }
    }
    total
}

/// A stage whose input is absent from the file prunes nothing, so zero-pruned is only
/// evidence once these facts hold.
struct LeafChunkFacts {
    min: String,
    max: String,
    has_page_index: bool,
    has_bloom_filter: bool,
}

fn leaf_chunk_facts(file_url: &str, row_group: usize, leaf_path: &str) -> LeafChunkFacts {
    use parquet::file::reader::FileReader;

    let path = file_url
        .strip_prefix("file://")
        .expect("the fixture returns a file:// URL");
    let reader = parquet::file::reader::SerializedFileReader::new(
        std::fs::File::open(path).expect("open the written parquet file"),
    )
    .expect("read the parquet footer");
    let group = reader.metadata().row_group(row_group);
    let column = group
        .columns()
        .iter()
        .find(|column| column.column_descr().path().string() == leaf_path)
        .unwrap_or_else(|| panic!("row group {row_group} carries no leaf column {leaf_path}"));
    let statistics = column
        .statistics()
        .unwrap_or_else(|| panic!("leaf column {leaf_path} carries no chunk statistics"));
    let bound = |bytes: Option<&[u8]>| {
        String::from_utf8(bytes.expect("the leaf statistics carry a bound").to_vec())
            .expect("a Utf8 leaf bound")
    };
    LeafChunkFacts {
        min: bound(statistics.min_bytes_opt()),
        max: bound(statistics.max_bytes_opt()),
        has_page_index: column.column_index_offset().is_some()
            && column.offset_index_offset().is_some(),
        has_bloom_filter: column.bloom_filter_offset().is_some(),
    }
}

/// Scenario: a predicate over a rendered nested column is evaluated, never silently dropped
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn predicate_over_a_rendered_nested_column_is_applied_not_dropped() {
    let dir = std::env::temp_dir().join(format!("lh_nested_pushdown_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_nested_parquet(&dir, 8);

    let (matching, _) = run_nested_scan(&nested_spec(
        file_url.clone(),
        r#""TAGS" = '["hello","world"]'"#,
    ))
    .await;
    assert_eq!(
        matching,
        vec![(1i64, r#"["hello","world"]"#.to_string())],
        "a predicate over a rendered nested column must return ONLY the matching row"
    );

    let (compound, _) = run_nested_scan(&nested_spec(
        file_url.clone(),
        r#""ID" = 2 AND "TAGS" = '["hello","world"]'"#,
    ))
    .await;
    assert!(
        compound.is_empty(),
        "a conjunction whose nested half matches no row must return no row, got {compound:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scenario: no pruning stage drops a row group holding a rendered nested-column match
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statistics_pruning_cannot_drop_a_row_group_holding_a_rendered_nested_match() {
    // The primitive predicate at the end proves statistics pruning is enabled, so the
    // zero-pruned result is evidence rather than a stage that never ran.
    let dir = std::env::temp_dir().join(format!("lh_nested_pruning_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_nested_parquet(&dir, 1);
    let facts = leaf_chunk_facts(&file_url, 0, "tags.list.item");
    assert_eq!(
        (facts.min.as_str(), facts.max.as_str()),
        ("hello", "world"),
        "the premise this test rests on must be IN the file: row group 0's nested leaf \
         statistics have to be the bounds that would falsely exclude the rendered document"
    );
    assert!(
        facts.has_page_index && facts.has_bloom_filter,
        "the page index and the bloom filter must be written for the nested leaf, or the \
         zero-pruned assertions on those two stages prove only that their input was missing"
    );

    let (rows, plan) = run_nested_scan(&nested_spec(
        file_url.clone(),
        r#""TAGS" = '["hello","world"]'"#,
    ))
    .await;

    assert_eq!(
        rows,
        vec![(1i64, r#"["hello","world"]"#.to_string())],
        "the row group holding the match must not be pruned on leaf statistics"
    );
    for stage in [
        "row_groups_pruned_statistics",
        "row_groups_pruned_bloom_filter",
        "page_index_rows_pruned",
        "files_ranges_pruned_statistics",
    ] {
        assert_eq!(
            sum_pruned(plan.as_ref(), stage),
            Some(0),
            "the {stage} stage must RUN and prune nothing on a predicate over a rendered \
             nested column; `None` means no plan node reports that metric under this name, \
             so the stage is unproven rather than proven harmless"
        );
    }

    let (primitive, primitive_plan) =
        run_nested_scan(&nested_spec(file_url.clone(), r#""ID" = 1"#)).await;
    assert_eq!(
        primitive,
        vec![(1i64, r#"["hello","world"]"#.to_string())],
        "the primitive predicate must return its one matching row"
    );
    assert_eq!(
        sum_pruned(primitive_plan.as_ref(), "row_groups_pruned_statistics"),
        Some(1),
        "statistics pruning must stay ENABLED for a table carrying a nested column — \
         otherwise the zero-pruned assertions above prove nothing"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
