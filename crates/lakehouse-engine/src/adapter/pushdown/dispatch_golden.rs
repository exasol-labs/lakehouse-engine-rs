//! Golden dispatch-SQL baseline (issue #175 / plan
//! `refactor-scan-spec-dispatch-dedup`, task 1.2).
//!
//! Nine committed fixtures under `testdata/dispatch_golden/` — five non-empty
//! dispatch shapes rendered through the production [`build_dispatch_sql`] seam,
//! and four empty shapes rendered through [`empty_result_sql`] — captured from
//! the pre-dedup code. Every subsequent dedup task (2-5) MUST leave these nine
//! fixtures byte-identical; a diff here is a regression, never an expected
//! update. Every assertion is a full-string `assert_eq!` against the committed
//! file — never `.contains(...)` or `.matches(...).count()`.

use super::test_support::{agg_item, pd, sample_storage};
use super::*;

/// The fixed three-shard file list every non-empty golden dispatches over.
fn fixed_shards() -> Vec<Vec<FileEntry>> {
    vec![
        vec![FileEntry::new("data/part-0.parquet", 1_000)],
        vec![FileEntry::new("data/part-1.parquet", 2_000)],
        vec![FileEntry::new("data/part-2.parquet", 1_500)],
    ]
}

/// The fixed four-column universe (`EVENTS`) every golden fixture projects
/// against: two VARCHAR columns, one numeric DECIMAL, one DECIMAL id.
fn base_col_types() -> Vec<(String, String)> {
    vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
    ]
}

/// Wrap a `pushdownRequest` body with the fixed `EVENTS` `involvedTables` block.
fn events_request(pushdown_req: Json) -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "EVENTS",
            "columns": [
                {"name": "REGION", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "NAME", "dataType": {"type": "varchar", "size": 2000000}},
                {"name": "AMOUNT", "dataType": {"type": "decimal", "precision": 18, "scale": 2}},
                {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
            ],
        }],
        "pushdownRequest": pushdown_req,
    })
}

/// Grouped-aggregate shape: `GROUP BY REGION`, `SUM(AMOUNT)` — decomposes into
/// the partial/merge grouped scan.
fn grouped_request() -> Json {
    events_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("AMOUNT"), false),
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    }))
}

/// Group-by fallback shape: `GROUP BY REGION`, `SUM(NAME)` where `NAME` is
/// VARCHAR — declines grouped decomposition (non-numeric SUM target, no
/// HAVING) and routes to the qualified single-table wrapper. Carries
/// `selectListDataTypes` so the empty counterpart exercises the
/// `GroupByWrapper` typed-empty shape.
fn group_by_fallback_request() -> Json {
    events_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("NAME"), false),
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 30, "scale": 4},
        ],
    }))
}

/// Lone `COUNT(DISTINCT ID)` shape: the only count-distinct shape that fans
/// out to its own DISTINCT row-scan counted by a native `COUNT(DISTINCT "V")`.
fn lone_count_distinct_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [agg_item("COUNT", Some("ID"), true)],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    }))
}

/// Multi/mixed `COUNT(DISTINCT)` decline shape: two distinct items — declines
/// the fan-out and routes to the qualified single-table wrapper.
fn multi_count_distinct_decline_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [
            agg_item("COUNT", Some("ID"), true),
            agg_item("COUNT", Some("NAME"), true),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    }))
}

/// Plain row-scan shape: two projected columns, no aggregate, no GROUP BY.
fn row_scan_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [
            {"type": "column", "name": "REGION"},
            {"type": "column", "name": "AMOUNT"},
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 18, "scale": 2},
        ],
    }))
}

/// The row-scan fixture's own projection (matching `row_scan_request`'s
/// `selectList`), reused for both the non-empty and empty row-scan goldens.
fn row_scan_projection() -> (Vec<ProjectionItem>, Vec<String>) {
    (
        vec![
            ProjectionItem::Column("REGION".into()),
            ProjectionItem::Column("AMOUNT".into()),
        ],
        vec!["VARCHAR(2000000)".into(), "DECIMAL(18,2)".into()],
    )
}

/// Plain single-group aggregate shape (`SUM(AMOUNT)`, no distinct, no GROUP
/// BY) — used only for the empty single-group-aggregate golden (its non-empty
/// counterpart is not one of the five listed dispatch shapes).
fn single_group_agg_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    }))
}

/// Render a non-empty dispatch SQL for `request` through the production
/// [`build_dispatch_sql`] seam, over the fixed three-shard fixture and a fixed
/// tuning/storage/schema common blob. `has_order_by` is always `false`: none
/// of the five golden shapes exercise the ordered top-N or declined-order-by
/// paths.
fn dispatch_sql(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    let pushdown_req = pd(request);
    let result = build_dispatch_sql(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        base_col_types(),
        filter,
        limit,
        false,
        &fixed_shards(),
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        &sample_storage(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        4,
        8192,
        2,
        0.6,
        200,
        8,
    )
    .expect("build_dispatch_sql must succeed for this golden fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// Render the empty-result SQL for `request` through [`empty_result_sql`],
/// over the same fixed column universe every non-empty golden uses.
fn empty_sql(request: &Json, proj_cols: &[ProjectionItem], proj_types: &[String]) -> String {
    let pushdown_req = pd(request);
    let result = empty_result_sql(&pushdown_req, proj_cols, proj_types, &base_col_types())
        .expect("empty_result_sql must succeed for this golden fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// Grouped-aggregate dispatch SQL stays byte-identical to the captured
/// pre-dedup golden.
#[test]
fn grouped_aggregate_matches_golden() {
    let actual = dispatch_sql(
        &grouped_request(),
        Vec::new(),
        Vec::new(),
        Some(r#"("AMOUNT" > 100)"#.to_string()),
        Some(50),
    );
    let expected = include_str!("testdata/dispatch_golden/grouped_aggregate.sql");
    assert_eq!(actual, expected);
}

/// Group-by fallback (declined decomposition) dispatch SQL stays
/// byte-identical to the captured pre-dedup golden.
#[test]
fn group_by_fallback_matches_golden() {
    let actual = dispatch_sql(
        &group_by_fallback_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/group_by_fallback.sql");
    assert_eq!(actual, expected);
}

/// Lone `COUNT(DISTINCT)` dispatch SQL stays byte-identical to the captured
/// pre-dedup golden.
#[test]
fn lone_count_distinct_matches_golden() {
    let actual = dispatch_sql(
        &lone_count_distinct_request(),
        Vec::new(),
        Vec::new(),
        None,
        Some(10),
    );
    let expected = include_str!("testdata/dispatch_golden/lone_count_distinct.sql");
    assert_eq!(actual, expected);
}

/// Multi/mixed `COUNT(DISTINCT)` decline dispatch SQL stays byte-identical to
/// the captured pre-dedup golden.
#[test]
fn multi_count_distinct_decline_matches_golden() {
    let actual = dispatch_sql(
        &multi_count_distinct_decline_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/multi_count_distinct_decline.sql");
    assert_eq!(actual, expected);
}

/// Single-group / row-scan dispatch SQL stays byte-identical to the captured
/// pre-dedup golden.
#[test]
fn single_group_row_scan_matches_golden() {
    let (proj_cols, proj_types) = row_scan_projection();
    let actual = dispatch_sql(
        &row_scan_request(),
        proj_cols,
        proj_types,
        Some(r#"("REGION" = 'EU')"#.to_string()),
        Some(100),
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_row_scan.sql");
    assert_eq!(actual, expected);
}

/// Empty-grouped result SQL stays byte-identical to the captured pre-dedup
/// golden.
#[test]
fn empty_grouped_matches_golden() {
    let actual = empty_sql(&grouped_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_grouped.sql");
    assert_eq!(actual, expected);
}

/// Empty `GroupByWrapper` (typed from `selectListDataTypes`) result SQL stays
/// byte-identical to the captured pre-dedup golden.
#[test]
fn empty_group_by_wrapper_matches_golden() {
    let actual = empty_sql(&group_by_fallback_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_group_by_wrapper.sql");
    assert_eq!(actual, expected);
}

/// Empty single-group-aggregate result SQL stays byte-identical to the
/// captured pre-dedup golden.
#[test]
fn empty_single_group_agg_matches_golden() {
    let actual = empty_sql(&single_group_agg_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_single_group_agg.sql");
    assert_eq!(actual, expected);
}

/// Empty row-scan result SQL stays byte-identical to the captured pre-dedup
/// golden.
#[test]
fn empty_row_scan_matches_golden() {
    let (proj_cols, proj_types) = row_scan_projection();
    let actual = empty_sql(&row_scan_request(), &proj_cols, &proj_types);
    let expected = include_str!("testdata/dispatch_golden/empty_row_scan.sql");
    assert_eq!(actual, expected);
}
