//! Integration test (Task 2.3, issue #145) — aggregate / grouped PHYSICAL
//! projection pruning.
//!
//! This is the empirical proof underpinning the whole `fix-aggregate-projection-field`
//! plan: emptying the diagnostic `ScanSpec.projection` field for aggregate scan
//! specs (Task 2.1) has NO effect on the actual physical Parquet read. DataFusion's
//! own projection pushdown prunes the read, driven by the `aggregates` / `group_keys`
//! query text — not by `ScanSpec.projection`.
//!
//! Both tests drive the EXACT production aggregate path:
//!   - `register_files` registers the assigned files through the real
//!     `PositionalDeleteScanTable` provider (the production seam);
//!   - `build_alias_items` builds the uppercase-aliased inner SELECT that wraps the
//!     scan, exactly as `run_partial_aggregate` / `run_grouped_partial_aggregate`
//!     do (`scan/mod.rs`) — NOT a hand-simplified `SELECT SUM(col) FROM table`, so
//!     the test exercises the real projection shape;
//!   - the real `build_partial_agg_sql_filtered` / `build_grouped_partial_agg_sql`
//!     builders produce the aggregate SQL;
//!   - `session_config_for_spec` supplies the session config.
//!
//! Then it builds the DataFusion physical plan and asserts the leaf Parquet scan's
//! output schema is EXACTLY the aggregate / group-key referenced column set, over a
//! THREE-column table so "exact" is a stronger claim than merely "fewer columns
//! than the table total".
//!
//! Host-runnable: writes a local Parquet file and inspects the physical plan; no
//! S3 / MinIO / Exasol stack required.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use lakehouse_engine::scan::spec::{
    AggKind, AggregatePlan, CommonScanSpec, FileEntry, ScanSpec, StorageBackend, StorageProps,
};
use lakehouse_engine::scan::{
    build_alias_items, build_grouped_partial_agg_sql, build_partial_agg_sql_filtered,
    register_files, session_config_for_spec,
};
use parquet::arrow::ArrowWriter;

/// Physical (Iceberg-style lowercase) column names of the test table. The adapter
/// speaks uppercase identifiers; the aggregate / group-key references below use the
/// uppercase form, and `build_alias_items` bridges the two.
const TABLE_COLUMNS: [&str; 3] = ["region", "score", "category"];

/// Write a local Parquet file with THREE columns (`region`, `score`, `category`)
/// and return its `file://` URL. Three columns make an "exactly one column" or
/// "exactly two columns" pruning assertion meaningfully stronger than "fewer than
/// the table total".
fn write_local_parquet(dir: &std::path::Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let path = dir.join("agg_pruning_data.parquet");
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("arrow writer");

    let regions = vec!["north", "south", "north", "east", "south", "west"];
    let scores: Vec<i64> = vec![10, 20, 30, 40, 50, 60];
    let categories = vec!["a", "b", "a", "c", "b", "a"];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(scores)),
            Arc::new(StringArray::from(categories)),
        ],
    )
    .expect("record batch");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    url::Url::from_file_path(&path)
        .expect("file path must be absolute")
        .to_string()
}

/// Build a base aggregate `ScanSpec` for a single local file. Per the fix (Task
/// 2.1) `projection` is left empty on aggregate specs — proving it plays no part in
/// the physical read is precisely the point of this test. `aggregates` and
/// `group_keys` are set by each test.
fn agg_spec(file_url: String) -> ScanSpec {
    let path = file_url.strip_prefix("file://").unwrap_or(&file_url);
    let size = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat test parquet file {path}: {e}"))
        .len();
    ScanSpec {
        common: CommonScanSpec {
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

/// Build the DataFusion physical plan for an aggregate scan, following the EXACT
/// production path: register the files, wrap the scan in the `build_alias_items`
/// uppercase-aliased inner SELECT, hand that subquery to the caller's SQL builder,
/// then plan it. `build_sql` receives the aliased-table subquery string and returns
/// the full aggregate SQL (via `build_partial_agg_sql_filtered` or
/// `build_grouped_partial_agg_sql`).
async fn build_agg_physical_plan(
    spec: &ScanSpec,
    build_sql: impl FnOnce(&str) -> String,
) -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    register_files(&ctx, "scan_target", spec)
        .await
        .expect("register_files must succeed on the local file");

    // Mirror run_partial_aggregate / run_grouped_partial_aggregate exactly: resolve
    // the registered table's schema and build the uppercase-aliased inner SELECT.
    let table = ctx
        .table("scan_target")
        .await
        .expect("resolve registered table");
    let alias_items = build_alias_items(table.schema());
    let aliased_table = format!("SELECT {} FROM scan_target", alias_items.join(", "));

    let sql = build_sql(&aliased_table);
    let df = ctx.sql(&sql).await.expect("aggregate SQL must plan");
    df.create_physical_plan()
        .await
        .expect("physical plan must build")
}

/// Recurse the physical plan, collecting each leaf node's one-line label and output
/// schema. Owned clones (not borrows) are collected so the recursion does not fight
/// the borrow checker over the temporary `children()` vecs.
fn collect_leaf_scans(plan: &dyn ExecutionPlan, out: &mut Vec<(String, SchemaRef)>) {
    let children = plan.children();
    if children.is_empty() {
        let label = displayable(plan).one_line().to_string();
        out.push((label, plan.schema()));
    } else {
        for child in children {
            collect_leaf_scans(child.as_ref(), out);
        }
    }
}

/// The set of columns the leaf Parquet scan physically projects, normalized to
/// uppercase. Asserts the plan has exactly ONE leaf and that it is the Parquet
/// `DataSourceExec` — so the returned set is unambiguously "what the scan reads".
///
/// A scan node's OUTPUT schema is the projected column set: DataFusion's projection
/// pushdown sets `FileScanConfig.projection` (see
/// `PositionalDeleteScanTable::scan` → `with_projection_indices`), which narrows the
/// `DataSourceExec` output schema to exactly the read columns. Reading that schema
/// is the exact-set mechanism — no fragile string-parsing of the projection list.
fn leaf_scan_projected_columns(plan: &Arc<dyn ExecutionPlan>) -> BTreeSet<String> {
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let mut leaves = Vec::new();
    collect_leaf_scans(plan.as_ref(), &mut leaves);

    assert_eq!(
        leaves.len(),
        1,
        "aggregate plan must have exactly one leaf scan node:\n{rendered}"
    );
    let (label, schema) = &leaves[0];
    assert!(
        label.contains("DataSourceExec"),
        "leaf must be the Parquet DataSourceExec, got `{label}`:\n{rendered}"
    );

    schema
        .fields()
        .iter()
        .map(|f| f.name().to_uppercase())
        .collect()
}

/// `SUM(score)` over the three-column table must physically read ONLY `score`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_group_agg_scan_prunes_to_referenced_columns() {
    let dir = std::env::temp_dir().join(format!("lh_agg_prune_single_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);

    let mut spec = agg_spec(file_url);
    let aggregates = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    spec.common.aggregates = Some(aggregates.clone());

    let plan = build_agg_physical_plan(&spec, |aliased_table| {
        build_partial_agg_sql_filtered(&aggregates, aliased_table, spec.common.filter.as_deref())
    })
    .await;

    let projected = leaf_scan_projected_columns(&plan);
    let expected: BTreeSet<String> = ["SCORE"].into_iter().map(String::from).collect();

    assert_eq!(
        projected,
        expected,
        "SUM(score) must physically read EXACTLY {{SCORE}} (not {:?}); \
         the {}-column table's other columns must be pruned by DataFusion",
        projected,
        TABLE_COLUMNS.len()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// `GROUP BY region, SUM(score)` must physically read ONLY `region` and `score`;
/// the unreferenced `category` column must be pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grouped_agg_scan_prunes_to_referenced_columns() {
    let dir = std::env::temp_dir().join(format!("lh_agg_prune_grouped_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);

    let mut spec = agg_spec(file_url);
    // Group-key fragments are already-rendered, quoted, uppercase DataFusion SQL —
    // exactly what the adapter emits (see `build_grouped_partial_agg_sql` doc and
    // `adapter/pushdown/mod.rs`).
    let group_keys = vec![r#""REGION""#.to_string()];
    let aggregates = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    spec.common.group_keys = Some(group_keys.clone());
    spec.common.aggregates = Some(aggregates.clone());

    let plan = build_agg_physical_plan(&spec, |aliased_table| {
        build_grouped_partial_agg_sql(
            &group_keys,
            &aggregates,
            aliased_table,
            spec.common.filter.as_deref(),
        )
    })
    .await;

    let projected = leaf_scan_projected_columns(&plan);
    let expected: BTreeSet<String> = ["REGION", "SCORE"].into_iter().map(String::from).collect();

    assert_eq!(
        projected, expected,
        "GROUP BY region, SUM(score) must physically read EXACTLY {{REGION, SCORE}} \
         (not {:?}); the unreferenced `category` column must be pruned",
        projected
    );
    assert!(
        !projected.contains("CATEGORY"),
        "the unreferenced `category` column must be pruned, but the scan read it: {projected:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
