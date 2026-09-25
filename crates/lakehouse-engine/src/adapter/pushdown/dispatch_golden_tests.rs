//! Golden dispatch-SQL fixtures under `testdata/dispatch_golden/`, compared by
//! full-string `assert_eq!`. The `empty_*` fixtures carry no `storage` value, so a
//! diff in one of them is always a regression, never an expected update.

use super::test_support::{agg_item, pd, sample_scan_storage, sample_storage};
use super::*;

fn fixed_shards() -> Vec<Vec<FileEntry>> {
    vec![
        vec![FileEntry::new("data/part-0.parquet", 1_000)],
        vec![FileEntry::new("data/part-1.parquet", 2_000)],
        vec![FileEntry::new("data/part-2.parquet", 1_500)],
    ]
}

fn base_col_types() -> Vec<(String, String)> {
    vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DECIMAL(18,2)".to_string()),
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
    ]
}

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

/// HAVING matches no projected plan, so it routes to `GroupByWrapper` (#195).
fn unmergeable_having_request() -> Json {
    let mut req = grouped_request();
    req["pushdownRequest"]["having"] = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("COUNT", None, false),
        "right": {"type": "literal_exactnumeric", "value": 0},
    });
    req
}

/// Non-numeric SUM target: declines grouped decomposition to the qualified wrapper.
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

fn lone_count_distinct_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [agg_item("COUNT", Some("ID"), true)],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    }))
}

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

fn row_scan_projection() -> (Vec<ProjectionItem>, Vec<String>) {
    (
        vec![
            ProjectionItem::Column("REGION".into()),
            ProjectionItem::Column("AMOUNT".into()),
        ],
        vec!["VARCHAR(2000000)".into(), "DECIMAL(18,2)".into()],
    )
}

fn single_group_agg_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    }))
}

/// An inner `DISTINCT` declines the decomposition, so the projection widens (#194).
fn nested_aggregate_decline_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [ {
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                agg_item("SUM", Some("AMOUNT"), true),
                {"type": "literal_exactnumeric", "value": 2}
            ]
        } ],
        "selectListDataTypes": [ {"type": "decimal", "precision": 36, "scale": 2} ],
    }))
}

fn single_group_scalar_over_aggregate_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [ {
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                agg_item("SUM", Some("AMOUNT"), false),
                {"type": "literal_exactnumeric", "value": 2}
            ]
        } ],
        "selectListDataTypes": [ {"type": "decimal", "precision": 36, "scale": 2} ],
    }))
}

/// The nested `COUNT(*)` must dedup against the bare item's partial column.
fn single_group_scalar_over_aggregate_dedup_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [
            agg_item("COUNT", None, false),
            {
                "type": "function_scalar",
                "name": "ROUND",
                "arguments": [
                    {
                        "type": "function_scalar",
                        "name": "FLOAT_DIV",
                        "arguments": [
                            agg_item("SUM", Some("AMOUNT"), false),
                            agg_item("COUNT", None, false),
                        ]
                    },
                    {"type": "literal_exactnumeric", "value": 2}
                ]
            }
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2}
        ],
    }))
}

fn single_group_scalar_over_aggregate_interleaved_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [
            agg_item("SUM", Some("AMOUNT"), false),
            {
                "type": "function_scalar",
                "name": "ROUND",
                "arguments": [
                    {
                        "type": "function_scalar",
                        "name": "FLOAT_DIV",
                        "arguments": [
                            agg_item("SUM", Some("AMOUNT"), false),
                            agg_item("COUNT", None, false),
                        ]
                    },
                    {"type": "literal_exactnumeric", "value": 2}
                ]
            },
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 36, "scale": 2},
            {"type": "decimal", "precision": 9, "scale": 4},
            {"type": "decimal", "precision": 18, "scale": 0}
        ],
    }))
}

fn single_group_scalar_over_variance_request() -> Json {
    events_request(serde_json::json!({
        "selectList": [ {
            "type": "function_scalar",
            "name": "ROUND",
            "arguments": [
                agg_item("VARIANCE", Some("AMOUNT"), false),
                {"type": "literal_exactnumeric", "value": 4}
            ]
        } ],
        "selectListDataTypes": [ {"type": "decimal", "precision": 36, "scale": 4} ],
    }))
}

/// Every golden shape has no ORDER BY and derives one projection item per
/// select-list item, so `has_order_by` and the widening flag are always `false`.
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

/// Derives the projection through `project_columns`, so the golden proves the
/// fixture itself widens.
fn dispatch_sql_widened(request: &Json) -> String {
    let pushdown_req = pd(request);
    let (proj_cols, proj_types, widened) =
        super::support::project_columns(&pushdown_req, base_col_types())
            .expect("the fixture must project");
    assert!(
        widened,
        "this fixture must exercise the widening guard, not a per-item projection"
    );
    let result = build_dispatch_sql(
        request,
        &pushdown_req,
        proj_cols,
        proj_types,
        widened,
        base_col_types(),
        None,
        None,
        None,
        false,
        &fixed_shards(),
        "s3://warehouse/db/events".to_string(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &sample_scan_storage(),
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

/// Takes `pushdown_req` explicitly so callers can pass a stripped or alias-carrying
/// body (#193).
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
        &sample_scan_storage(),
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

/// Scenario: grouped-aggregate dispatch SQL matches its golden
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

/// Scenario: group-by fallback dispatch SQL matches its golden
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

/// Scenario: lone COUNT(DISTINCT) dispatch SQL matches its golden
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

/// Scenario: multi/mixed COUNT(DISTINCT) decline dispatch SQL matches its golden
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

/// Scenario: single-group row-scan dispatch SQL matches its golden
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

/// Scenario: a declined nested aggregate widens to the qualified wrapper (#194)
#[test]
fn nested_aggregate_decline_matches_qualified_wrapper_golden() {
    let actual = dispatch_sql_widened(&nested_aggregate_decline_request());
    let expected = include_str!("testdata/dispatch_golden/nested_aggregate_decline.sql");
    assert_eq!(actual, expected);
}

/// Scenario: a lone scalar-over-aggregate item matches its golden
#[test]
fn single_group_scalar_over_aggregate_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate.sql");
    assert_eq!(actual, expected);
}

/// Scenario: a nested COUNT(*) dedups against the bare COUNT(*) partial column
#[test]
fn single_group_scalar_over_aggregate_dedup_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_dedup_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected =
        include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate_dedup.sql");
    assert_eq!(actual, expected);
}

/// Scenario: interleaved scalar-over-aggregate items keep selectList order and own casts
#[test]
fn single_group_scalar_over_aggregate_interleaved_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_aggregate_interleaved_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected =
        include_str!("testdata/dispatch_golden/single_group_scalar_over_aggregate_interleaved.sql");
    assert_eq!(actual, expected);
}

/// Scenario: a scalar-wrapped statistical aggregate matches its golden
#[test]
fn single_group_scalar_over_variance_matches_golden() {
    let actual = dispatch_sql(
        &single_group_scalar_over_variance_request(),
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_scalar_over_variance.sql");
    assert_eq!(actual, expected);
}

/// Scenario: empty grouped result SQL matches its golden
#[test]
fn empty_grouped_matches_golden() {
    let actual = empty_sql(&grouped_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_grouped.sql");
    assert_eq!(actual, expected);
}

/// Scenario: empty GroupByWrapper result SQL matches its golden
#[test]
fn empty_group_by_wrapper_matches_golden() {
    let actual = empty_sql(&group_by_fallback_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_group_by_wrapper.sql");
    assert_eq!(actual, expected);
}

/// Scenario: an empty unmergeable HAVING routes to GroupByWrapper (#195)
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

/// Scenario: empty single-group aggregate result SQL matches its golden
#[test]
fn empty_single_group_agg_matches_golden() {
    let actual = empty_sql(&single_group_agg_request(), &[], &[]);
    let expected = include_str!("testdata/dispatch_golden/empty_single_group_agg.sql");
    assert_eq!(actual, expected);
}

/// Scenario: empty row-scan result SQL matches its golden
#[test]
fn empty_row_scan_matches_golden() {
    let (proj_cols, proj_types) = row_scan_projection();
    let actual = empty_sql(&row_scan_request(), &proj_cols, &proj_types);
    let expected = include_str!("testdata/dispatch_golden/empty_row_scan.sql");
    assert_eq!(actual, expected);
}

/// Scenario: an empty scalar-over-aggregate row casts NULL to the item's declared type
#[test]
fn empty_single_group_scalar_over_aggregate_matches_golden() {
    let actual = empty_sql(&single_group_scalar_over_aggregate_request(), &[], &[]);
    let expected =
        include_str!("testdata/dispatch_golden/empty_single_group_scalar_over_aggregate.sql");
    assert_eq!(actual, expected);
}

/// Every column node carries `tableAlias: "E"`, as for `FROM EVENTS e` (#193).
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

/// Scenario: an aliased single-table GROUP BY renders bare column names (#193)
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

/// Scenario: the multi-COUNT(DISTINCT) fallback qualifies every column as LHS_T0 regardless of alias
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

// Covers every `AggKind` and every partial-column arity (1, 2, 3) in one request,
// over a dedicated column universe with a numeric SCORE and a MIN/MAX target TS.

fn all_agg_kinds_col_types() -> Vec<(String, String)> {
    vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
        ("TS".to_string(), "TIMESTAMP".to_string()),
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
    ]
}

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

fn single_group_all_agg_kinds_request() -> Json {
    all_agg_kinds_request(serde_json::json!({
        "selectList": all_agg_kinds_select_list(),
        "selectListDataTypes": all_agg_kinds_declared_types(),
    }))
}

/// The group key at ordinal 0 shifts the aggregates by one, exercising the
/// plan-ordinal versus column-ordinal distinction.
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

/// Scenario: single-group all-agg-kinds dispatch SQL matches its golden
#[test]
fn single_group_all_agg_kinds_matches_golden() {
    let actual = dispatch_sql_with_col_types(
        &single_group_all_agg_kinds_request(),
        all_agg_kinds_col_types(),
    );
    let expected = include_str!("testdata/dispatch_golden/single_group_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

/// Scenario: grouped all-agg-kinds dispatch SQL matches its golden
#[test]
fn grouped_all_agg_kinds_matches_golden() {
    let actual =
        dispatch_sql_with_col_types(&grouped_all_agg_kinds_request(), all_agg_kinds_col_types());
    let expected = include_str!("testdata/dispatch_golden/grouped_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

use super::joins::{
    JoinScanRequestConfig, JoinWindowPlan, build_broadcast_join_sql, build_n_scan_join_sql,
};

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

fn two_table_detected_join(request: &Json) -> DetectedJoin {
    match detect_join(request, &pd(request)).expect("two-table join must be detected") {
        JoinShape::Join(join) => join,
        other => panic!("expected a detected join, got {other:?}"),
    }
}

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

fn join_scan_tuning() -> JoinScanRequestConfig<'static> {
    JoinScanRequestConfig {
        cluster_nodes: 1,
        parallelism_factor: 1,
        df_target_partitions: 1,
        df_batch_size: 8192,
        df_threads_per_udf: 1,
        memory_pool_fraction: 0.6,
        instance_overhead_mb: 0,
        s3_max_connections: 1,
        connection: &super::test_support::TEST_CONNECTION,
    }
}

/// Scenario: a filterless request emits unchanged SQL at the single-table, broadcast, and N-scan sites
#[test]
fn filterless_request_emits_unchanged_sql_at_all_three_sites() {
    let (proj_cols, proj_types) = row_scan_projection();
    let single_table_sql = dispatch_sql(&row_scan_request(), proj_cols, proj_types, None, None);
    assert_eq!(
        single_table_sql,
        include_str!("testdata/dispatch_golden/filterless_single_table.sql")
    );

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
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/filterless_broadcast_join.sql")
    );

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

/// Scenario: a rendering filter emits unchanged, wrapper-free SQL at all three sites
#[test]
fn rendering_filter_emits_unchanged_wrapper_free_scan() {
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
    .expect("selecting the wire storage must succeed")
    .expect("an unbounded broadcast join must build");
    assert_eq!(
        broadcast_sql,
        include_str!("testdata/dispatch_golden/rendering_broadcast_join.sql")
    );

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
