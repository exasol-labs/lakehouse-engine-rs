//! Aggregate scans are pruned by DataFusion's projection pushdown from the aggregate /
//! group-key SQL, not by `ScanSpec.projection` (#145).

mod scan_fixture;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use lakehouse_engine::scan::spec::{
    AggKind, AggregatePlan, CommonScanSpec, FileEntry, ScanSpec, ScanStorage, StorageBackend,
    StorageProps,
};
use lakehouse_engine::scan::{
    build_alias_items, build_grouped_partial_agg_sql, build_partial_agg_sql_filtered,
    register_files, session_config_for_spec,
};

use parquet::arrow::ArrowWriter;

const TABLE_COLUMNS: [&str; 3] = ["region", "score", "category"];

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

/// `projection` stays empty: proving it plays no part in the physical read is the point.
fn agg_spec(file_url: String) -> ScanSpec {
    let path = file_url.strip_prefix("file://").unwrap_or(&file_url);
    let size = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat test parquet file {path}: {e}"))
        .len();
    ScanSpec {
        common: CommonScanSpec {
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

async fn build_agg_physical_plan(
    spec: &ScanSpec,
    build_sql: impl FnOnce(&str) -> String,
) -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new_with_config(session_config_for_spec(spec));
    let storage = scan_fixture::resolved_storage(spec);
    register_files(&ctx, "scan_target", spec, &storage)
        .await
        .expect("register_files must succeed on the local file");

    // Mirrors run_partial_aggregate / run_grouped_partial_aggregate.
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

/// The leaf `DataSourceExec`'s output schema is exactly the columns it reads.
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

/// Scenario: `SUM(score)` over a three-column table physically reads only `score`
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

/// Scenario: `GROUP BY region, SUM(score)` physically reads only `region` and `score`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grouped_agg_scan_prunes_to_referenced_columns() {
    let dir = std::env::temp_dir().join(format!("lh_agg_prune_grouped_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file_url = write_local_parquet(&dir);

    let mut spec = agg_spec(file_url);
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
