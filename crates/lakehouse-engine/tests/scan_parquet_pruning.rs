//! Integration test for Parquet row-group & page pruning, and for what a
//! predicate over a JSON-RENDERED NESTED column does to both pruning stages.
//!
//! Asserts two things the spec scenario "Scan enables Parquet row-group and
//! page pruning so the reader skips non-matching data" requires:
//!   1. The session config the scan UDF builds enables predicate pushdown,
//!      row-group statistics pruning, and page-index pruning (not the
//!      DataFusion defaults — `pushdown_filters` defaults OFF).
//!   2. Pruning narrows what is read, never the result set: a filtered scan
//!      returns byte-identical rows with pruning ON vs OFF.
//!
//! Then two things `datafusion-scan/nested-json-rendering` requires of a
//! predicate over a list, struct, or map column:
//!   3. It is EVALUATED, never silently dropped — the wrong-rows bug that
//!      returned every row while the pushdown was approved against the `utf8`
//!      logical tag and then dropped against the physical nested type.
//!   4. No Parquet pruning stage drops the row group that holds the match,
//!      proven positively against a MULTI-row-group file whose per-group LEAF
//!      statistics (`min = "hello"`, `max = "world"`) would falsely exclude the
//!      rendered document `["hello","world"]`.
//!
//! Host-runnable: writes a local Parquet file (multiple row groups so row-group
//! pruning is actually exercisable) and registers it through DataFusion's local
//! object store — no S3 / MinIO stack required.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, ListBuilder, StringArray, StringBuilder, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::prelude::SessionConfig;
use lakehouse_engine::scan::spec::{
    CommonScanSpec, FileEntry, LogicalField, NestedMembers, ProjectionItem, ScanSpec,
    StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_raw_scan_physical_plan, register_files, session_config_for_spec,
};
use object_store::local::LocalFileSystem;
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
        common: CommonScanSpec {
            projection: vec!["ID".into(), "NAME".into()],
            filter: Some(r#""ID" >= 200 AND "ID" < 400"#.into()),
            storage: StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            }),
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

/// Write a Parquet file carrying `id: Int64` and `tags: List<Utf8>`, one row per
/// row group so each group's leaf statistics describe exactly one document.
///
/// Row 1's tags are `["hello", "world"]`, so its row group's LEAF statistics are
/// `min = "hello"`, `max = "world"` — the shape that would falsely exclude the
/// rendered document `["hello","world"]` under a min/max range check, because
/// `[` (0x5B) sorts below `h` (0x68).
///
/// Page-level statistics and a bloom filter are written too, so the page-index and
/// bloom-filter pruning stages have real input and their zero-pruned results are
/// evidence about the predicate rather than about a missing index.
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

/// A spec over the nested fixture whose logical schema declares `tags` as the
/// `utf8` tag every list, struct, and map column carries, with the nested member
/// descriptor a `list<string>` produces. Both fields bind by identity.
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
            storage: StorageBackend::S3(StorageProps {
                endpoint: "http://localhost:9000".into(),
                region: "us-east-1".into(),
                access_key: "k".into(),
                secret_key: "s".into(),
                allow_http: true,
                ..Default::default()
            }),
            ..Default::default()
        },
        files: vec![FileEntry::new(file_url, size)],
    }
}

/// Run one nested-column scan through the production registration seam and
/// return both the `(id, rendered tags)` rows and the executed plan.
async fn run_nested_scan(spec: &ScanSpec) -> (Vec<(i64, String)>, Arc<dyn ExecutionPlan>) {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    ctx.runtime_env().register_object_store(
        &url::Url::parse("file://").expect("file scheme"),
        Arc::new(LocalFileSystem::new()),
    );
    register_files(&ctx, "scan_target", spec)
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

/// One Parquet pruning metric's pruned count summed across the executed plan, or
/// `None` when no node reports a metric under that name at all.
///
/// An ABSENT metric and a metric reporting zero are different answers and only the
/// second is evidence: were DataFusion to rename a stage's metric, a summing
/// function that folded both into `0` would turn every zero-pruned assertion into a
/// silent no-op.
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

/// What one row group's leaf column chunk carries in the footer: the min/max bounds
/// a range check would read, and whether the page index and bloom filter the two
/// other pruning stages consume were written at all.
///
/// A stage whose input is absent from the file prunes nothing no matter what the
/// scan asks of it, so "nothing was pruned" is only evidence once these are true.
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

/// Scenario "A predicate over a rendered nested column is evaluated, never
/// silently dropped": DataFusion approves the Parquet row-filter pushdown
/// against the LOGICAL schema (where the column is `Utf8`, so "supported"),
/// removes the `FilterExec`, and then drops the conjunct at file open because it
/// does not match the PHYSICAL nested schema — applying it nowhere and returning
/// EVERY row. Both assertions here returned wrong rows before the fix.
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

/// Scenario "Every pushdown shape treats a nested column as the VARCHAR Exasol
/// declared", pruning clause: proven POSITIVELY against a MULTI-row-group file
/// whose per-group LEAF statistics would falsely exclude the rendered document.
/// Row 1 sits alone in a row group whose `tags` leaf statistics are
/// `min = "hello"`, `max = "world"`, and the predicate compares against the
/// rendered text `["hello","world"]`, which sorts BELOW both — so a min/max range
/// check over those statistics would conclude the group cannot match and skip the
/// row that does. That premise is read out of the written footer rather than
/// asserted in prose, together with the page index and the bloom filter the other
/// two stages consume — a stage whose input the writer never emitted prunes nothing
/// for reasons that have nothing to do with this fix.
///
/// The second half is what makes the first half evidence rather than an
/// accident: statistics pruning is left ENABLED for the table, and a PRIMITIVE
/// predicate over the very same file prunes a row group. So "nothing was pruned"
/// cannot be read as "the stage never ran" — the stage runs, prunes when it can,
/// and still cannot prune the rendered nested column. Every stage is required to
/// REPORT its metric as well as to prune nothing, so a DataFusion rename breaks the
/// test rather than quietly satisfying it. If a future DataFusion or parquet-rs
/// release ever did resolve a nested column's leaf statistics into this predicate,
/// the first half fails loudly rather than silently returning fewer rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statistics_pruning_cannot_drop_a_row_group_holding_a_rendered_nested_match() {
    let dir = std::env::temp_dir().join(format!("lh_nested_pruning_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    // One row per row group: row 1's group carries min "hello" / max "world".
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
