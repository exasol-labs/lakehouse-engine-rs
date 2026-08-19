//! Golden dispatch-SQL baseline (issue #175 / plan
//! `refactor-scan-spec-dispatch-dedup`, task 1.2).
//!
//! Ten committed fixtures under `testdata/dispatch_golden/` — five non-empty
//! dispatch shapes rendered through the production [`build_dispatch_sql`] seam,
//! and five empty shapes rendered through [`empty_result_sql`] — captured from
//! the pre-dedup code. Every subsequent dedup task (2-5) MUST leave these ten
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

/// `grouped_request()` plus a HAVING (`COUNT(*) > 0`) that matches no plan in
/// its select list (only `SUM(AMOUNT)` is projected) — unrenderable over the
/// merge, so it falls through to `RequestShape::GroupByWrapper` rather than
/// staying `Grouped` (issue #195).
fn unmergeable_having_request() -> Json {
    let mut req = grouped_request();
    req["pushdownRequest"]["having"] = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("COUNT", None, false),
        "right": {"type": "literal_exactnumeric", "value": 0},
    });
    req
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
/// paths. The widening flag is always `false` too — every golden shape derives
/// one projection item per select-list item, so none routes to the qualified
/// single-table wrapper.
fn dispatch_sql(
    request: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    let pushdown_req = pd(request);
    dispatch_sql_with_pushdown_req(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        base_col_types(),
        filter,
        limit,
    )
}

/// Like [`dispatch_sql`], but takes the `pushdownRequest` body explicitly
/// instead of deriving it (unstripped) from `request` — so a caller can drive
/// [`build_dispatch_sql`] with a deliberately stripped OR deliberately
/// alias-carrying `pushdown_req`, to pin the alias-leak fix (issue #193) at
/// the dispatch level without touching the frozen golden fixtures above.
fn dispatch_sql_with_pushdown_req(
    request: &Json,
    pushdown_req: &Json,
    proj_cols: Vec<ProjectionItem>,
    proj_types: Vec<String>,
    col_types: Vec<(String, String)>,
    filter: Option<String>,
    limit: Option<u64>,
) -> String {
    let result = build_dispatch_sql(
        request,
        pushdown_req,
        proj_cols,
        proj_types,
        false,
        col_types,
        filter,
        None,
        limit,
        false,
        &fixed_shards(),
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
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
    .expect("build_dispatch_sql must succeed for this fixture");
    result["sql"]
        .as_str()
        .expect("pushdown response must carry a sql field")
        .to_string()
}

/// Render the empty-result SQL for `request` through [`empty_result_sql`],
/// over the same fixed column universe every non-empty golden uses. Every captured
/// fixture is a non-widened projection, so the widening signal is `false` here.
fn empty_sql(request: &Json, proj_cols: &[ProjectionItem], proj_types: &[String]) -> String {
    let pushdown_req = pd(request);
    let result = empty_result_sql(
        &pushdown_req,
        proj_cols,
        proj_types,
        false,
        &base_col_types(),
    )
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

/// An unmergeable HAVING (`COUNT(*) > 0` over a select list that only
/// projects `SUM(AMOUNT)`) routes the empty path to the SAME `GroupByWrapper`
/// shape the non-empty path commits to (issue #195), not the plain `Grouped`
/// empty shape its HAVING-less sibling `grouped_request()` produces.
///
/// Both empty renderers type every column from `selectListDataTypes` at the
/// same select-list index, so for this fixture (a plain group-key + one
/// aggregate select list) their output is byte-identical — the golden text
/// alone cannot distinguish `Grouped` from `GroupByWrapper`. The
/// `classify_request_shape` assertion below is the actual regression guard.
#[test]
fn empty_unmergeable_having_matches_group_by_wrapper_golden() {
    let request = unmergeable_having_request();
    let actual = empty_sql(&request, &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_unmergeable_having.sql");
    assert_eq!(actual, expected);

    let pushdown_req = pd(&request);
    let shape = classify_request_shape(&pushdown_req, &base_col_types());
    assert!(
        matches!(shape, RequestShape::GroupByWrapper),
        "an unmergeable HAVING must route to GroupByWrapper, not stay classified as \
         Grouped: {shape:?}"
    );
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

/// `grouped_request`'s `GROUP BY REGION` / `SUM(AMOUNT)` shape, but with every
/// column node stamped `tableName: "EVENTS"` / `tableAlias: "E"` — the shape
/// Exasol sends for an aliased single-table query (`FROM EVENTS e`, issue
/// #193).
fn aliased_grouped_request() -> Json {
    let column = |name: &str| serde_json::json!({"type": "column", "name": name, "tableName": "EVENTS", "tableAlias": "E"});
    events_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [column("REGION")],
        "selectList": [
            column("REGION"),
            {
                "type": "function_aggregate",
                "name": "SUM",
                "arguments": [column("AMOUNT")],
                "distinct": false,
            },
        ],
        "selectListDataTypes": [
            {"type": "varchar", "size": 2000000},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    }))
}

/// Regression for issue #193: an ALIASED single-table `GROUP BY` request,
/// fed through `strip_table_alias` (the `handle_pushdown` chokepoint) into
/// `build_dispatch_sql`, must render the GROUP BY key and the select-list
/// aggregate argument as BARE `"REGION"` / `"AMOUNT"` — never
/// `"E"."REGION"` / `"E"."AMOUNT"` — because the scan target this dispatches
/// over exposes bare column names and does not resolve an alias-qualified
/// reference. Stripping tableAlias is a no-op on rendering otherwise (the
/// translator ignores the surviving `tableName`), so the output must be
/// byte-identical to the unaliased `grouped_aggregate` golden fixture.
#[test]
fn aliased_single_table_group_by_renders_bare_group_key_and_select_expr() {
    let request = aliased_grouped_request();
    let raw_pushdown_req = pd(&request);
    let stripped_pushdown_req = strip_table_alias(&raw_pushdown_req);

    let actual = dispatch_sql_with_pushdown_req(
        &request,
        &stripped_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        Some(r#"("AMOUNT" > 100)"#.to_string()),
        Some(50),
    );

    assert!(
        actual.contains(r#""group_keys":["\"REGION\""]"#),
        "GROUP BY key must render the bare column name, never alias-qualified: {actual}"
    );
    assert!(
        actual.contains(r#"{"kind":"sum","column":"AMOUNT"}"#),
        "the SUM aggregate argument must render the bare column name: {actual}"
    );
    assert!(
        !actual.contains(r#""E"."#),
        "no clause may carry the stale Exasol alias qualifier \"E\": {actual}"
    );
    let expected = include_str!("testdata/dispatch_golden/grouped_aggregate.sql");
    assert_eq!(
        actual, expected,
        "an aliased request must render byte-identical to the unaliased golden once \
         tableAlias is stripped upstream — the grouped partial/merge path never reads \
         tableName either, so its surviving presence changes nothing"
    );
}

/// `multi_count_distinct_decline_request`'s two-`COUNT(DISTINCT)` shape, but
/// with every column node stamped `tableName: "EVENTS"` / `tableAlias: "E"` —
/// the shape that routes to `qualified_single_table_fallback_pushdown`.
fn aliased_multi_count_distinct_decline_request() -> Json {
    let column = |name: &str| serde_json::json!({"type": "column", "name": name, "tableName": "EVENTS", "tableAlias": "E"});
    events_request(serde_json::json!({
        "selectList": [
            {
                "type": "function_aggregate",
                "name": "COUNT",
                "arguments": [column("ID")],
                "distinct": true,
            },
            {
                "type": "function_aggregate",
                "name": "COUNT",
                "arguments": [column("NAME")],
                "distinct": true,
            },
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    }))
}

/// Regression for issue #193's qualified-fallback guarantee: the multi-
/// `COUNT(DISTINCT)` decline routes to `qualified_single_table_fallback_pushdown`
/// (`build_qualified_single_table_fallback_sql`), which re-derives its
/// `"LHS_T0"` qualification from each column's `tableName` via
/// `annotate_columns_with_alias` — unconditionally overwriting any incoming
/// `tableAlias`. So the wrapper must render identically qualified SQL whether
/// the request carries a stale `tableAlias` or has already been stripped,
/// and in both cases every reference must be qualified `"LHS_T0"."…"`.
///
/// Unlike the other golden requests, `tableName` is present here (the real
/// Exasol wire shape always carries it), so this is deliberately NOT compared
/// against the frozen `multi_count_distinct_decline` golden fixture: that
/// fixture's columns carry no `tableName` at all, which is what makes ITS
/// render come out bare rather than `"LHS_T0"`-qualified (`annotate_columns_
/// with_alias` only qualifies a column whose `tableName` resolves).
#[test]
fn aliased_multi_count_distinct_fallback_qualifies_lhs_t0_regardless_of_alias_presence() {
    let request = aliased_multi_count_distinct_decline_request();
    let raw_pushdown_req = pd(&request);
    let stripped_pushdown_req = strip_table_alias(&raw_pushdown_req);

    let sql_with_alias = dispatch_sql_with_pushdown_req(
        &request,
        &raw_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        None,
        None,
    );
    let sql_stripped = dispatch_sql_with_pushdown_req(
        &request,
        &stripped_pushdown_req,
        Vec::new(),
        Vec::new(),
        base_col_types(),
        None,
        None,
    );

    assert_eq!(
        sql_with_alias, sql_stripped,
        "a caller-supplied stale tableAlias must have no effect on the fallback \
         wrapper's rendered SQL — it re-qualifies from tableName unconditionally"
    );
    assert!(
        sql_with_alias.contains(r#""LHS_T0"."ID""#)
            && sql_with_alias.contains(r#""LHS_T0"."NAME""#),
        "the wrapper must qualify every column reference to its own subquery alias \
         LHS_T0, whether or not the request carried a tableAlias: {sql_with_alias}"
    );
}

// --- All-agg-kinds fixtures (plan `refactor-pushdown-agg-dedup`, task 1.1) ---
//
// These two fixtures cover every `AggKind` variant and every partial-column
// arity (1, 2, and 3 columns) in one request, so the pre-refactor column
// contract (`PARTIAL_*` names, `EMITS` types, merge SELECT) is pinned before
// task 1.3 rewires the five sites that encode it. The mixed arities are
// deliberate: they exercise the plan-ordinal-versus-column-ordinal
// distinction that is the drift risk the refactor must not disturb.
//
// A dedicated column universe (ID/SCORE/TS/REGION), not the shared
// `base_col_types` REGION/NAME/AMOUNT/ID one every other golden fixture
// above uses: SCORE must be numeric (SUM/AVG/the four statistical kinds all
// require it), TS is a MIN/MAX target of any comparable type, and neither
// fits the existing universe.

/// The ID/SCORE/TS/REGION column universe both all-agg-kinds fixtures
/// dispatch over: SCORE numeric (required by SUM/AVG/the statistical
/// family), TS a MIN/MAX target, ID a COUNT target, REGION the group key.
fn all_agg_kinds_col_types() -> Vec<(String, String)> {
    vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ("TS".to_string(), "TIMESTAMP".to_string()),
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
    ]
}

/// Wrap a `pushdownRequest` body with the `involvedTables` block matching
/// [`all_agg_kinds_col_types`].
fn all_agg_kinds_request(pushdown_req: Json) -> Json {
    serde_json::json!({
        "involvedTables": [{
            "name": "EVENTS",
            "columns": [
                {"name": "ID", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "SCORE", "dataType": {"type": "double"}},
                {"name": "TS", "dataType": {"type": "timestamp"}},
                {"name": "REGION", "dataType": {"type": "varchar", "size": 2000000}},
            ],
        }],
        "pushdownRequest": pushdown_req,
    })
}

/// The select-list items exercising all ten `AggKind` variants, in the same
/// order the plan names them: `Count`, `CountCol`, `Sum`, `Min`, `Max`,
/// `Avg` (arity 1 then 1 then 1 then 1 then 1 then 2), then the four
/// statistical kinds `StddevSamp`, `StddevPop`, `VarSamp`, `VarPop` (arity 3
/// each) — `STDDEV`/`STDDEV_POP`/`VARIANCE`/`VAR_POP` map onto them per
/// `parse_agg_item`.
fn all_agg_kinds_select_list() -> Vec<Json> {
    vec![
        agg_item("COUNT", None, false),
        agg_item("COUNT", Some("id"), false),
        agg_item("SUM", Some("score"), false),
        agg_item("MIN", Some("ts"), false),
        agg_item("MAX", Some("ts"), false),
        agg_item("AVG", Some("score"), false),
        agg_item("STDDEV", Some("score"), false),
        agg_item("STDDEV_POP", Some("score"), false),
        agg_item("VARIANCE", Some("score"), false),
        agg_item("VAR_POP", Some("score"), false),
    ]
}

/// The declared Exasol result type Exasol would assign each item in
/// [`all_agg_kinds_select_list`], at the same index: `DECIMAL(18,0)` for the
/// two COUNTs, `TIMESTAMP` for MIN/MAX(ts), `DOUBLE PRECISION` for
/// SUM/AVG(score) and every statistical kind over a DOUBLE column.
fn all_agg_kinds_declared_types() -> Vec<Json> {
    vec![
        serde_json::json!({"type": "decimal", "precision": 18, "scale": 0}),
        serde_json::json!({"type": "decimal", "precision": 18, "scale": 0}),
        serde_json::json!({"type": "double"}),
        serde_json::json!({"type": "timestamp"}),
        serde_json::json!({"type": "timestamp"}),
        serde_json::json!({"type": "double"}),
        serde_json::json!({"type": "double"}),
        serde_json::json!({"type": "double"}),
        serde_json::json!({"type": "double"}),
        serde_json::json!({"type": "double"}),
    ]
}

/// Single-group (ungrouped) shape: all ten `AggKind` variants, no GROUP BY.
fn single_group_all_agg_kinds_request() -> Json {
    all_agg_kinds_request(serde_json::json!({
        "selectList": all_agg_kinds_select_list(),
        "selectListDataTypes": all_agg_kinds_declared_types(),
    }))
}

/// Grouped shape: the same ten `AggKind` variants, plus `GROUP BY region` —
/// the region key occupies select-list ordinal 0, so the ten aggregates sit
/// at ordinals 1..=10, one higher than in the single-group sibling above.
/// That shift is exactly the plan-ordinal-versus-column-ordinal distinction
/// the mixed arities are meant to exercise.
fn grouped_all_agg_kinds_request() -> Json {
    let mut select_list = vec![serde_json::json!({"type": "column", "name": "REGION"})];
    select_list.extend(all_agg_kinds_select_list());
    let mut declared_types = vec![serde_json::json!({"type": "varchar", "size": 2000000})];
    declared_types.extend(all_agg_kinds_declared_types());
    all_agg_kinds_request(serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": select_list,
        "selectListDataTypes": declared_types,
    }))
}

/// Like [`dispatch_sql`], but takes `col_types` explicitly instead of the
/// shared `base_col_types()` — the two all-agg-kinds fixtures dispatch over
/// [`all_agg_kinds_col_types`], not the REGION/NAME/AMOUNT/ID universe every
/// other golden fixture in this module shares.
fn dispatch_sql_with_col_types(request: &Json, col_types: Vec<(String, String)>) -> String {
    dispatch_sql_with_pushdown_req(
        request,
        &pd(request),
        Vec::new(),
        Vec::new(),
        col_types,
        None,
        None,
    )
}

/// Single-group all-agg-kinds dispatch SQL stays byte-identical to the
/// captured pre-refactor golden — the only baseline over the `EMITS` clause
/// and outer merge SELECT for every `AggKind` at once (plan
/// `refactor-pushdown-agg-dedup`, task 1.1).
#[test]
fn single_group_all_agg_kinds_matches_golden() {
    let actual = dispatch_sql_with_col_types(
        &single_group_all_agg_kinds_request(),
        all_agg_kinds_col_types(),
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

/// Grouped all-agg-kinds dispatch SQL stays byte-identical to the captured
/// pre-refactor golden — the grouped-path sibling of
/// `single_group_all_agg_kinds_matches_golden`.
#[test]
fn grouped_all_agg_kinds_matches_golden() {
    let actual =
        dispatch_sql_with_col_types(&grouped_all_agg_kinds_request(), all_agg_kinds_col_types());
    let expected = include_str!("testdata/dispatch_golden/grouped_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

// --- Cross-site fixtures (plan `fix-declined-filter-self-apply`, task 2.6) ---
//
// The ten fixtures above pin the single-table dispatch seam alone. The two
// fixtures below additionally drive the broadcast-join and N-scan-join render
// sites (`render_broadcast_join` + `build_broadcast_join_sql`, and
// `build_n_scan_join_sql`) so a single pair of tests proves ALL THREE sites
// named in the plan's Design > Context table stay byte-identical for a
// filterless request and for a request whose filter renders cleanly — the
// only two cases the fix is required to leave unchanged.

use super::joins::{
    JoinScanTuning, JoinWindowPlan, build_broadcast_join_sql, build_n_scan_join_sql,
};

/// A minimal CUSTOMER ⋈ ORDERS inner equi-join pushdown request (disjoint
/// column names, a broadcast-eligible shape), with an optional WHERE filter —
/// the join-side counterpart of `row_scan_request`.
fn two_table_join_request(filter: Option<Json>) -> Json {
    let mut pushdown_req = serde_json::json!({
        "type": "select",
        "from": {
            "type": "join",
            "join_type": "inner",
            "left": {"name": "CUSTOMER", "type": "table"},
            "right": {"name": "ORDERS", "type": "table"},
            "condition": {
                "type": "predicate_equal",
                "left": {"type": "column", "name": "C_CUSTKEY", "tableName": "CUSTOMER"},
                "right": {"type": "column", "name": "O_CUSTKEY", "tableName": "ORDERS"},
            },
        },
        "selectList": [
            {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
            {"type": "column", "name": "O_ORDERDATE", "tableName": "ORDERS"},
        ],
    });
    if let Some(f) = filter {
        pushdown_req["filter"] = f;
    }
    serde_json::json!({
        "involvedTables": [
            {"name": "CUSTOMER", "columns": [
                {"name": "C_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "C_NAME", "dataType": {"type": "varchar", "size": 100}},
            ]},
            {"name": "ORDERS", "columns": [
                {"name": "O_CUSTKEY", "dataType": {"type": "decimal", "precision": 20, "scale": 0}},
                {"name": "O_ORDERDATE", "dataType": {"type": "date"}},
            ]},
        ],
        "pushdownRequest": pushdown_req,
        "schemaMetadataInfo": {
            "properties": {},
            "adapterNotes": serde_json::json!({
                "TABLE_MAP": {"CUSTOMER": "lh.customer", "ORDERS": "lh.orders"}
            }).to_string(),
        },
    })
}

/// The detected join shape for [`two_table_join_request`], resolved through
/// the production [`detect_join`] seam rather than hand-built.
fn two_table_detected_join(request: &Json) -> DetectedJoin {
    match detect_join(request, &pd(request)).expect("two-table join must be detected") {
        JoinShape::Join(join) => join,
        other => panic!("expected a detected join, got {other:?}"),
    }
}

/// CUSTOMER's resolved join side: one small file, columns disjoint from ORDERS.
fn resolved_customer_side() -> ResolvedJoinSide {
    ResolvedJoinSide {
        table_name: "CUSTOMER".to_string(),
        table_identifier: "lh.customer".to_string(),
        table_root: "s3://warehouse/lh/customer".to_string(),
        files: vec![FileEntry::new("s3://w/c-0.parquet", 10)],
        logical_schema: vec![LogicalField {
            field_id: Some(1),
            name: "CUSTOMER_KEY".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: Vec::new(),
        total_bytes: 10,
        refused_columns: Vec::new(),
    }
}

/// ORDERS' resolved join side: one larger file, columns disjoint from CUSTOMER.
fn resolved_orders_side() -> ResolvedJoinSide {
    ResolvedJoinSide {
        table_name: "ORDERS".to_string(),
        table_identifier: "lh.orders".to_string(),
        table_root: "s3://warehouse/lh/orders".to_string(),
        files: vec![FileEntry::new("s3://w/o-0.parquet", 100)],
        logical_schema: vec![LogicalField {
            field_id: Some(1),
            name: "ORDERS_KEY".to_string(),
            arrow_type: "int64".to_string(),
            nullable: false,
            initial_default: None,
            nested: None,
            physical_name: None,
        }],
        name_mapping: Vec::new(),
        effective_storage: sample_storage(),
        partition_columns: Vec::new(),
        total_bytes: 100,
        refused_columns: Vec::new(),
    }
}

/// The fixed join tuning knobs both join-site goldens dispatch over.
fn join_scan_tuning() -> JoinScanTuning {
    JoinScanTuning {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 0,
        s3_max_connections: 1,
    }
}

/// A filterless request — single-table row scan, and CUSTOMER ⋈ ORDERS at the
/// broadcast and N-scan join sites — must emit byte-identical SQL at all
/// three pushdown render sites named in the plan's Design > Context table:
/// `handle_pushdown`'s `build_dispatch_sql` seam, `render_broadcast_join` +
/// `build_broadcast_join_sql`, and `build_n_scan_join_sql`. Proves tasks
/// 2.1-2.5 changed nothing for the "no filter in the request" case, which was
/// always correct to omit.
#[test]
fn filterless_request_emits_unchanged_sql_at_all_three_sites() {
    // Site 1: single-table WHERE (`handle_pushdown` / `build_dispatch_sql`).
    let (proj_cols, proj_types) = row_scan_projection();
    let single_table_sql = dispatch_sql(&row_scan_request(), proj_cols, proj_types, None, None);
    assert_eq!(
        single_table_sql,
        include_str!("testdata/dispatch_golden/filterless_single_table.sql")
    );

    // Site 2: broadcast join (`render_broadcast_join` + `build_broadcast_join_sql`).
    let request = two_table_join_request(None);
    let pushdown_req = pd(&request);
    let join = two_table_detected_join(&request);
    let rendered = render_broadcast_join(&request, &pushdown_req, &join)
        .expect("render_broadcast_join must not error for a well-formed request")
        .expect("a disjoint-column, filterless equi-join must stay broadcast-eligible");
    let sides = JoinSides {
        fact: resolved_orders_side(),
        dimension: resolved_customer_side(),
        broadcast_eligible: true,
    };
    let broadcast_sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        JoinWindowPlan::Unbounded,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/filterless_broadcast_join.sql")
    );

    // Site 3: N-scan per-leg fallback (`build_n_scan_join_sql`).
    let sides = vec![resolved_customer_side(), resolved_orders_side()];
    let n_scan_sql = build_n_scan_join_sql(
        &request,
        &pushdown_req,
        &join,
        &sides,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("build_n_scan_join_sql must succeed for this fixture");
    assert_eq!(
        n_scan_sql,
        include_str!("testdata/dispatch_golden/filterless_n_scan_join.sql")
    );
}

/// A filter that RENDERS cleanly in DataFusion dialect must ALSO emit
/// byte-identical SQL at all three sites — the fix changes behavior only for
/// a DECLINED filter, never a rendering one. Proves the single-table path
/// stays on its wrapper-free fast scan (no `LHS_T0` qualified fallback), the
/// broadcast join keeps its accelerated shape, and the N-scan fallback keeps
/// pushing the rendering conjunct into its owning leg rather than the outer
/// WHERE.
#[test]
fn rendering_filter_emits_unchanged_wrapper_free_scan() {
    // Site 1: single-table WHERE.
    let (proj_cols, proj_types) = row_scan_projection();
    let single_table_sql = dispatch_sql(
        &row_scan_request(),
        proj_cols,
        proj_types,
        Some(r#"("REGION" = 'EU')"#.to_string()),
        None,
    );
    assert_eq!(
        single_table_sql,
        include_str!("testdata/dispatch_golden/rendering_single_table.sql")
    );

    // Site 2: broadcast join.
    let renderable_filter = serde_json::json!({
        "type": "predicate_equal",
        "left": {"type": "column", "name": "C_NAME", "tableName": "CUSTOMER"},
        "right": {"type": "literal_string", "value": "ACME"},
    });
    let request = two_table_join_request(Some(renderable_filter));
    let pushdown_req = pd(&request);
    let join = two_table_detected_join(&request);
    let rendered = render_broadcast_join(&request, &pushdown_req, &join)
        .expect("render_broadcast_join must not error for a well-formed request")
        .expect("a disjoint-column equi-join with a rendering filter must stay broadcast-eligible");
    let sides = JoinSides {
        fact: resolved_orders_side(),
        dimension: resolved_customer_side(),
        broadcast_eligible: true,
    };
    let broadcast_sql = build_broadcast_join_sql(
        &sides,
        &rendered,
        JoinWindowPlan::Unbounded,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/rendering_broadcast_join.sql")
    );

    // Site 3: N-scan per-leg fallback.
    let sides = vec![resolved_customer_side(), resolved_orders_side()];
    let n_scan_sql = build_n_scan_join_sql(
        &request,
        &pushdown_req,
        &join,
        &sides,
        &join_scan_tuning(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
    )
    .expect("build_n_scan_join_sql must succeed for this fixture");
    assert_eq!(
        n_scan_sql,
        include_str!("testdata/dispatch_golden/rendering_n_scan_join.sql")
    );
}
