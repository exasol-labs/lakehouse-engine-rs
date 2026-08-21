use super::super::detect_aggregates;
use super::super::single_group_agg::DistinctCount;
use super::super::support::quote_ident;
use super::super::test_support::*;
use super::*;
use crate::scan::spec::AggregatePlan;

#[test]
fn empty_file_list_returns_empty_select() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let resp = empty_pushdown_sql(&proj, &types);
    let sql = resp["sql"].as_str().unwrap();
    assert!(sql.contains("WHERE 1=0"));
    assert!(sql.contains("CAST(NULL AS DECIMAL(20,0))"));
}

/// A pruned query with repeated literals in the projection (e.g.
/// `SELECT 1, name, 1 ... WHERE <all files pruned>`) keeps unique EMITS
/// aliases via `emits_ident`: the two `Expr` positions get distinct
/// positional synthetic names, never a duplicated `AS "1"` collision
/// (issue #190).
#[test]
fn empty_pushdown_sql_repeated_literals_unique_aliases() {
    let proj_cols: Vec<ProjectionItem> = vec![
        ProjectionItem::Expr { expr: "1".into() },
        ProjectionItem::Column("NAME".into()),
        ProjectionItem::Expr { expr: "1".into() },
    ];
    let proj_types = vec![
        "DECIMAL(18,0)".to_string(),
        "VARCHAR(2000000)".to_string(),
        "DECIMAL(18,0)".to_string(),
    ];
    let resp = empty_pushdown_sql(&proj_cols, &proj_types);
    let sql = resp["sql"].as_str().unwrap();

    assert_eq!(
        sql.matches("CAST(NULL AS").count(),
        3,
        "must emit three CAST(NULL AS ...) items, one per select-list item: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_0"))),
        "position 0's literal must get a positional-unique alias: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("NAME"))),
        "the column item must keep its real quoted name: {sql}"
    );
    assert!(
        sql.contains(&format!("AS {}", quote_ident("_LH_PROJ_2"))),
        "position 2's literal must get a distinct positional-unique alias: {sql}"
    );
    assert!(
        !sql.contains(&format!("AS {}", quote_ident("1"))),
        "must never alias a literal by its rendered value text (would collide): {sql}"
    );
}

/// Single-group empty result: one row, per-`AggKind` literal cast to its
/// declared type — COUNT → `0`, SUM → `NULL` — with no `WHERE 1=0` (a bare
/// `FROM DUAL` already yields exactly one row).
#[test]
fn empty_agg_sql_emits_zero_and_null_row_cast_to_declared_types() {
    let items = vec![
        SingleGroupItem::Aggregate(AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }),
        SingleGroupItem::Aggregate(AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }),
    ];
    let pushdown_req = serde_json::json!({
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });
    let resp = empty_agg_sql(&items, &pushdown_req, &[]);
    let sql = resp["sql"].as_str().unwrap();
    assert!(sql.contains("FROM DUAL"), "must select from DUAL: {sql}");
    assert!(
        !sql.contains("WHERE 1=0"),
        "single-group empty is one row, not zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(0 AS DECIMAL(18,0))"),
        "COUNT empty literal must be 0 cast to declared type: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(36,2))"),
        "SUM empty literal must be NULL cast to declared type: {sql}"
    );
}

/// A `ScalarOverAggregate` item followed by a bare `Aggregate` item must not
/// shift the bare item's declared-type lookup: each item's cast type comes from
/// its OWN `selectList` index, never from a list compacted down to only the
/// `function_aggregate`-typed items (which the `ScalarOverAggregate` item, typed
/// `function_scalar`, is absent from — a type list compacted that way shifts
/// every index after a `ScalarOverAggregate` item, so each item's cast type must
/// be looked up at its own `selectList` index).
#[test]
fn empty_agg_sql_scalar_over_aggregate_item_does_not_shift_a_later_bare_aggregate_type() {
    let items = vec![
        SingleGroupItem::ScalarOverAggregate {
            node: round_of(count_star(), 2),
            declared_type: "DECIMAL(18,2)".to_string(),
        },
        SingleGroupItem::Aggregate(AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }),
    ];
    let pushdown_req = serde_json::json!({
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 2},
            {"type": "decimal", "precision": 36, "scale": 4},
        ],
    });
    let resp = empty_agg_sql(&items, &pushdown_req, &[]);
    let sql = resp["sql"].as_str().unwrap();
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(36,4))"),
        "the bare SUM at select-list index 1 must be cast to ITS OWN declared \
         type, not left uncast by a compacted-index lookup: {sql}"
    );
}

/// COUNT(DISTINCT) over zero files yields a plain `0` literal row — no distinct
/// fan-out, no scan, and no merge step (with zero files there is nothing to scan
/// or deduplicate).
#[test]
fn empty_agg_sql_count_distinct_emits_zero_no_merge_udf() {
    let items = vec![SingleGroupItem::Distinct(DistinctCount {
        column: Some("ID".into()),
        arg_expr: None,
    })];
    let pushdown_req = serde_json::json!({
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    });
    let resp = empty_agg_sql(&items, &pushdown_req, &[]);
    let sql = resp["sql"].as_str().unwrap();
    assert_eq!(
        sql, "SELECT CAST(0 AS DECIMAL(18,0)) FROM DUAL",
        "COUNT(DISTINCT) over zero files must be a plain 0 literal row with no fan-out \
         or merge step: {sql}"
    );
}

fn count_star() -> Json {
    serde_json::json!({"type": "function_aggregate", "name": "COUNT", "arguments": []})
}

fn sum_of(col: &str) -> Json {
    serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{"type": "column", "name": col}]
    })
}

fn round_of(inner: Json, digits: i64) -> Json {
    serde_json::json!({
        "type": "function_scalar", "name": "ROUND",
        "arguments": [inner, {"type": "literal_exactnumeric", "value": digits}]
    })
}

/// A fully-pruned file list yields one shape-correct empty row for a
/// scalar-over-aggregate select list: each nested aggregate contributes its OWN
/// zero-row literal (COUNT -> `0`, SUM -> a NULL typed from its argument column)
/// substituted into the scalar structure, not a bare `NULL` for the whole item —
/// and the result is cast to the item's own declared type, independent of
/// `selectListDataTypes` (here absent from `pushdown_req` entirely).
///
/// The absent value MUST carry a type: Exasol rejects `ROUND(NULL, 2)` outright
/// with `Feature not supported: Round with wrong type` (SQL state `0A000`).
#[test]
fn empty_single_group_scalar_over_aggregate_emits_one_typed_row() {
    let items = vec![
        SingleGroupItem::ScalarOverAggregate {
            node: round_of(count_star(), 2),
            declared_type: "DECIMAL(18,2)".to_string(),
        },
        SingleGroupItem::ScalarOverAggregate {
            node: round_of(sum_of("AMOUNT"), 2),
            declared_type: "DECIMAL(36,2)".to_string(),
        },
    ];
    let pushdown_req = serde_json::json!({});
    let col_types = [("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];
    let resp = empty_agg_sql(&items, &pushdown_req, &col_types);
    let sql = resp["sql"].as_str().unwrap();

    assert!(sql.contains("FROM DUAL"), "must select from DUAL: {sql}");
    assert!(
        !sql.contains("WHERE 1=0"),
        "single-group empty is one row, not zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(ROUND(0, 2) AS DECIMAL(18,2))"),
        "ROUND(COUNT(*), 2) zero-row value must substitute COUNT's own zero \
         literal, not a bare NULL: {sql}"
    );
    assert!(
        sql.contains("CAST(ROUND(CAST(NULL AS DECIMAL(18,2)), 2) AS DECIMAL(36,2))"),
        "ROUND(SUM(x), 2) zero-row value must substitute SUM's absent value typed \
         from its own argument column — Exasol rejects a bare ROUND(NULL, 2) with \
         SQL state 0A000: {sql}"
    );
}

/// An absent nested aggregate with no argument column to type it from — here a
/// `SUM` over a scalar expression — falls back to `DOUBLE PRECISION`, so the
/// rendered scalar still receives a well-typed argument.
#[test]
fn empty_single_group_scalar_over_expression_aggregate_types_its_null() {
    let items = vec![SingleGroupItem::ScalarOverAggregate {
        node: round_of(sum_of_length("COMMENT"), 2),
        declared_type: "DECIMAL(36,2)".to_string(),
    }];
    let resp = empty_agg_sql(&items, &serde_json::json!({}), &[]);
    let sql = resp["sql"].as_str().unwrap();

    assert!(
        sql.contains("CAST(ROUND(CAST(NULL AS DOUBLE PRECISION), 2) AS DECIMAL(36,2))"),
        "an expression-argument aggregate has no column to read a type from, so \
         its absent value must still be typed: {sql}"
    );
}

fn sum_of_length(col: &str) -> Json {
    serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{
            "type": "function_scalar", "name": "LENGTH",
            "arguments": [{"type": "column", "name": col}]
        }]
    })
}

/// Issue #57 shape-consistency (task 6.7): when EVERY file is pruned, a Case 2/3
/// single-group request (more than one `COUNT(DISTINCT)`, or a distinct mixed with
/// an ordinary aggregate) must return the SAME N-aggregate-column shape
/// (`empty_agg_sql`, one column per select item) that the non-empty qualified
/// single-table wrapper returns — NEVER the full-row empty shape
/// (`empty_pushdown_sql`), whose different column count trips Exasol's positional
/// pushdown validation (`sqlCode 04000`, "Expected number of columns is N but
/// pushdown query has M"), since Exasol never re-aggregates a declined pushdown.
#[test]
fn empty_case_2_3_matches_non_empty_aggregate_shape() {
    fn count_top_level_cols(select_span: &str) -> usize {
        let mut depth = 0i32;
        let mut cols = 1usize;
        for ch in select_span.chars() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => cols += 1,
                _ => {}
            }
        }
        cols
    }

    // Case 3: two COUNT(DISTINCT) + one ordinary SUM → N = 3 output columns.
    let pushdown_req = serde_json::json!({
        "selectList": [
            agg_item("COUNT", Some("A"), true),
            agg_item("COUNT", Some("B"), true),
            agg_item("SUM", Some("C"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });
    let col_types = vec![
        ("A".to_string(), "DECIMAL(18,0)".to_string()),
        ("B".to_string(), "DECIMAL(18,0)".to_string()),
        ("C".to_string(), "DECIMAL(36,2)".to_string()),
    ];

    // The fixture must be a Case 2/3 shape: distinct present, but not a lone one
    // (so the non-empty path declines the fan-out and routes to the wrapper).
    let items = detect_aggregates(&pushdown_req).expect("a Case 3 select list detects");
    assert!(
        super::super::single_group_agg::has_distinct(&items)
            && !super::super::single_group_agg::is_lone_count_distinct(&items),
        "the fixture must be a Case 2/3 shape"
    );
    let n = pushdown_req["selectList"].as_array().unwrap().len();

    // A deliberately WIDER full-row projection (5 columns): if the empty dispatch
    // wrongly returned the full-row shape, its column count would be 5, not N = 3.
    let proj_cols: Vec<ProjectionItem> = ["A", "B", "C", "D", "E"]
        .iter()
        .map(|c| ProjectionItem::from(*c))
        .collect();
    let proj_types = vec![
        "DECIMAL(18,0)".to_string(),
        "DECIMAL(18,0)".to_string(),
        "DECIMAL(36,2)".to_string(),
        "VARCHAR(10)".to_string(),
        "VARCHAR(10)".to_string(),
    ];

    let empty = empty_result_sql(&pushdown_req, &proj_cols, &proj_types, false, &col_types)
        .expect("empty Case 2/3 result must build");
    let empty_sql = empty["sql"].as_str().unwrap();

    // Routes to the N-aggregate-column shape (empty_agg_sql), NOT the full-row shape.
    let direct = empty_agg_sql(&items, &pushdown_req, &[]);
    assert_eq!(
        empty_sql,
        direct["sql"].as_str().unwrap(),
        "the empty Case 2/3 dispatch must route to empty_agg_sql: {empty_sql}"
    );
    assert_ne!(
        empty_sql,
        empty_pushdown_sql(&proj_cols, &proj_types)["sql"]
            .as_str()
            .unwrap(),
        "the empty Case 2/3 dispatch must NOT return the full-row empty shape (#57): {empty_sql}"
    );

    // Exactly N columns — the same one-per-select-item shape the non-empty wrapper
    // returns, so empty and non-empty column shapes never diverge.
    let select_span = &empty_sql["SELECT ".len()..empty_sql.find(" FROM").expect("has FROM")];
    assert_eq!(
        count_top_level_cols(select_span),
        n,
        "the empty shape must have exactly N={n} aggregate columns (one per select \
         item): {empty_sql}"
    );
    // COUNT(DISTINCT) over zero files → 0; the ordinary SUM → NULL, each cast to
    // its declared type.
    assert!(
        empty_sql.contains("CAST(0 AS DECIMAL(18,0))")
            && empty_sql.contains("CAST(NULL AS DECIMAL(36,2))"),
        "COUNT(DISTINCT) empties to 0 and the ordinary SUM to NULL: {empty_sql}"
    );
}

/// Every non-COUNT `AggKind` maps to the `NULL` empty literal — single-node
/// SQL semantics over zero rows (only the COUNT family yields `0`).
#[test]
fn empty_agg_literal_maps_non_count_kinds_to_null() {
    for kind in [
        AggKind::Sum,
        AggKind::Min,
        AggKind::Max,
        AggKind::Avg,
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ] {
        assert_eq!(
            empty_agg_literal(&kind),
            "NULL",
            "{kind:?} empty literal must be NULL"
        );
    }
    for kind in [AggKind::Count, AggKind::CountCol] {
        assert_eq!(
            empty_agg_literal(&kind),
            "0",
            "{kind:?} empty literal must be 0"
        );
    }
}

/// Grouped empty result: zero rows (`WHERE 1=0`) with one `CAST(NULL AS <ty>)`
/// per grouped output column, assembled in select-list order.
#[test]
fn empty_grouped_sql_emits_zero_rows_in_grouped_shape() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 1,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    assert!(
        sql.contains("WHERE 1=0"),
        "grouped empty is zero rows: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(20,0))"),
        "group-key column typed from group_key_types: {sql}"
    );
    assert!(
        sql.contains("CAST(NULL AS DECIMAL(18,0))"),
        "aggregate column typed from aggregate_types: {sql}"
    );
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    assert_eq!(
        select_clause.matches("CAST(NULL AS").count(),
        2,
        "one output column per grouped select item: {sql}"
    );
}

/// A `GroupedSelectItem::Constant` (Exasol's "count the groups" literal
/// rewrite) reuses its already-rendered projection expression verbatim,
/// slotted into select-list order alongside the group-key and aggregate
/// columns — it contributes no aggregate plan and is not re-typed here.
#[test]
fn empty_grouped_sql_includes_constant_projection_column() {
    let select_items = vec![
        GroupedSelectItem::GroupKey {
            group_key_slot: 0,
            select_index: 0,
        },
        GroupedSelectItem::Constant {
            select_index: 1,
            projection: "CAST(NULL AS BOOLEAN)".to_string(),
        },
        GroupedSelectItem::Aggregate {
            plan_slot: 0,
            select_index: 2,
        },
    ];
    let group_key_types = vec!["DECIMAL(20,0)".to_string()];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let resp = empty_grouped_sql(&group_key_types, &aggregate_types, &select_items);
    let sql = resp["sql"].as_str().unwrap();
    let select_clause = sql
        .strip_prefix("SELECT ")
        .and_then(|s| s.split(" FROM").next())
        .unwrap();
    let columns: Vec<&str> = select_clause.split(", ").collect();
    assert_eq!(
        columns,
        vec![
            "CAST(NULL AS DECIMAL(20,0))",
            "CAST(NULL AS BOOLEAN)",
            "CAST(NULL AS DECIMAL(18,0))",
        ],
        "constant column is reused verbatim in select-list order: {sql}"
    );
}

/// Dispatch priority mirrors the non-empty path: grouped first, then
/// single-group aggregate (only when `validate_agg_col_types` passes), then
/// row scan.
#[test]
fn empty_result_sql_dispatches_by_plan_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(18,2)".to_string())];

    let grouped = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("COUNT", None, false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 18, "scale": 0},
        ],
    });
    let grouped_sql =
        empty_result_sql(&grouped, &proj, &proj_types, false, &col_types).unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
    assert!(
        grouped_sql.contains("WHERE 1=0"),
        "grouped shape is zero rows: {grouped_sql}"
    );

    let single = serde_json::json!({
        "selectList": [agg_item("SUM", Some("amount"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    });
    let single_sql =
        empty_result_sql(&single, &proj, &proj_types, false, &col_types).unwrap()["sql"]
            .as_str()
            .unwrap()
            .to_string();
    assert!(
        single_sql.contains("FROM DUAL") && !single_sql.contains("WHERE 1=0"),
        "single-group shape is one row: {single_sql}"
    );
    assert!(single_sql.contains("CAST(NULL AS DECIMAL(36,2))"));

    // Non-numeric SUM target demotes to the row-scan empty shape (gate honored).
    let non_numeric = serde_json::json!({
        "selectList": [agg_item("SUM", Some("name"), false)],
        "selectListDataTypes": [{"type": "decimal", "precision": 36, "scale": 2}],
    });
    let non_numeric_col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
    let row_sql = empty_result_sql(
        &non_numeric,
        &proj,
        &proj_types,
        false,
        &non_numeric_col_types,
    )
    .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        row_sql.contains("CAST(NULL AS DECIMAL(20,0))") && row_sql.contains(&quote_ident("ID")),
        "non-numeric single-group aggregate must fall through to the row-scan shape: {row_sql}"
    );
}

/// A grouped aggregate over a non-numeric column with all files pruned no longer
/// demotes to the full-row empty shape: since issue #82's fix, a grouped request
/// that cannot push down (here, a non-numeric SUM with no HAVING) routes on the
/// NON-empty path to the qualified single-table wrapper, whose output columns are
/// the `selectList` items. The empty path must MIRROR that shape — a zero-row
/// result typed per `selectListDataTypes` (the `selectList` column count/types),
/// NOT the full base row — so the empty and non-empty shapes never diverge.
#[test]
fn empty_files_grouped_non_numeric_aggregate_uses_selectlist_shape() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_non_numeric = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
    });

    let row_sql = empty_result_sql(&grouped_non_numeric, &proj, &proj_types, false, &col_types)
        .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate over zero files must produce the selectList-typed \
         empty shape (matching the qualified wrapper), not the full base row"
    );
}

/// A non-numeric grouped aggregate that also carries a HAVING no longer hard
/// errors: the classifier routes it to `GroupByWrapper` (the HAVING renders
/// natively over the wrapper rather than being dropped), so the empty path must
/// mirror the SAME selectList-typed empty shape as the no-HAVING sibling above,
/// not an `Err`.
#[test]
fn empty_files_grouped_non_numeric_aggregate_with_having_yields_typed_empty() {
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into()];
    let proj_types = vec!["DECIMAL(20,0)".to_string(), "VARCHAR(2000000)".to_string()];
    let col_types = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];

    let grouped_having = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "K"}],
        "selectList": [
            {"type": "column", "name": "K"},
            agg_item("SUM", Some("name"), false),
        ],
        "selectListDataTypes": [
            {"type": "decimal", "precision": 20, "scale": 0},
            {"type": "decimal", "precision": 36, "scale": 2},
        ],
        "having": {"type": "predicate_greater"},
    });

    let row_sql = empty_result_sql(&grouped_having, &proj, &proj_types, false, &col_types).unwrap()
        ["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        row_sql,
        "SELECT CAST(NULL AS DECIMAL(20,0)), CAST(NULL AS DECIMAL(36,2)) FROM DUAL WHERE 1=0",
        "declined grouped aggregate with HAVING over zero files must produce the same \
         selectList-typed empty shape as the wrapper it now falls through to, not an error"
    );
}

/// A row-scan request whose derived projection WIDENED to the full base row is
/// routed on the non-empty path to the qualified single-table wrapper, whose
/// output columns are the `selectList` items (#196). The empty path must mirror
/// that shape — one `selectListDataTypes`-typed zero-row column — never the
/// wider full base row, whose column count trips Exasol's positional `04000`
/// check. The widening signal alone decides this: the identical request with a
/// non-widened projection still gets the full-row shape.
#[test]
fn empty_result_sql_widened_row_scan_uses_select_list_types() {
    let pushdown_req = serde_json::json!({
        "selectList": [
            {"type": "function_scalar", "name": "LENGTH", "arguments": [
                {"type": "column", "name": "SCORE", "tableName": "T"}]},
        ],
        "selectListDataTypes": [{"type": "decimal", "precision": 18, "scale": 0}],
    });
    let col_types = vec![
        ("ID".to_string(), "DECIMAL(20,0)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    // No aggregate anywhere, so the shared classifier picks `RowScan` — the arm
    // under test, not the `GroupByWrapper` arm that already emits this shape.
    assert!(
        matches!(
            classify_request_shape(&pushdown_req, &col_types),
            RequestShape::RowScan
        ),
        "the fixture must classify as RowScan for this test to exercise its arm"
    );

    // The widened projection IS the full base row: three columns for one item.
    let proj: Vec<ProjectionItem> = vec!["ID".into(), "NAME".into(), "SCORE".into()];
    let proj_types: Vec<String> = col_types.iter().map(|(_, t)| t.clone()).collect();

    let widened = empty_result_sql(&pushdown_req, &proj, &proj_types, true, &col_types)
        .expect("the widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        widened, "SELECT CAST(NULL AS DECIMAL(18,0)) FROM DUAL WHERE 1=0",
        "a widened row-scan projection over zero files must produce ONE \
         selectListDataTypes-typed column, not the 3-column base row: {widened}"
    );

    let not_widened = empty_result_sql(&pushdown_req, &proj, &proj_types, false, &col_types)
        .expect("the non-widened empty row-scan result must build")["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        not_widened,
        empty_pushdown_sql(&proj, &proj_types)["sql"]
            .as_str()
            .unwrap(),
        "the non-widened path must stay byte-identical to the full-row empty \
         shape: {not_widened}"
    );
}
