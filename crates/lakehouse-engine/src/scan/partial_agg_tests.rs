use super::*;
use crate::scan::spec::AggKind;

/// Test-only no-filter wrapper over `build_partial_agg_sql_filtered`
/// (`filter = None`); also reached by `grouped_agg_tests` and
/// `scan_surface_probe` via `crate::scan::`.
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// One `AggregatePlan` per `AggKind` variant, in the same order as
/// `testdata/dispatch_golden/single_group_all_agg_kinds.sql` /
/// `grouped_all_agg_kinds.sql` (plan `refactor-pushdown-agg-dedup`, task
/// 1.1): `Count`, `CountCol`, `Sum`, `Min`, `Max`, `Avg` (arity 1 then 1
/// then 1 then 1 then 1 then 2), then the four statistical kinds
/// `StddevSamp`, `StddevPop`, `VarSamp`, `VarPop` (arity 3 each) — the
/// mixed arities exercise the plan-ordinal-versus-column-ordinal
/// distinction the refactor must not disturb.
fn all_agg_kinds_plans() -> Vec<AggregatePlan> {
    vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::CountCol,
            column: Some("ID".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Min,
            column: Some("TS".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Max,
            column: Some("TS".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::StddevSamp,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::StddevPop,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::VarSamp,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::VarPop,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
    ]
}

/// The scan's own single-group partial-aggregate SQL over every `AggKind`
/// stays byte-identical to the captured pre-refactor golden — the only
/// baseline over `partial_select_items`' output (plan
/// `refactor-pushdown-agg-dedup`, task 1.1), which no `dispatch_golden`
/// fixture can reach: the scan's DataFusion SELECT list is built here, at
/// runtime, not by `build_dispatch_sql`.
#[test]
fn partial_agg_sql_all_agg_kinds_matches_golden() {
    let actual = build_partial_agg_sql(&all_agg_kinds_plans(), "aliased");
    let expected = include_str!("testdata/partial_agg_golden/partial_agg_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

/// The scan's own grouped partial-aggregate SQL (one group key, no
/// filter) over every `AggKind` stays byte-identical to the captured
/// pre-refactor golden — the grouped-path sibling of
/// `partial_agg_sql_all_agg_kinds_matches_golden`, and equally
/// unreachable from any `dispatch_golden` fixture.
#[test]
fn grouped_partial_agg_sql_all_agg_kinds_matches_golden() {
    let actual = build_grouped_partial_agg_sql(
        &[r#""REGION""#.to_string()],
        &all_agg_kinds_plans(),
        "aliased",
        None,
    );
    let expected =
        include_str!("testdata/partial_agg_golden/grouped_partial_agg_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

fn sample_plans_count_sum_min_max() -> Vec<AggregatePlan> {
    vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Min,
            column: Some("TS".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Max,
            column: Some("TS".into()),
            arg_expr: None,
        },
    ]
}

/// Column order: COUNT(*) first, then SUM, MIN, MAX — each one column.
#[test]
fn partial_agg_sql_count_star_uses_count_star() {
    let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
    assert!(
        sql.contains("COUNT(*) AS"),
        "COUNT(*) plan must use COUNT(*): {sql}"
    );
    assert!(
        sql.contains("PARTIAL_count_0"),
        "COUNT(*) partial column must be PARTIAL_count_0: {sql}"
    );
}

/// COUNT(col) plan uses COUNT("COL"), not COUNT(*).
#[test]
fn partial_agg_sql_count_col_uses_count_col() {
    let plans = vec![AggregatePlan {
        kind: AggKind::CountCol,
        column: Some("ID".into()),
        arg_expr: None,
    }];
    let sql = build_partial_agg_sql(&plans, "aliased");
    assert!(
        sql.contains(r#"COUNT("ID")"#),
        "COUNT(col) must use COUNT(\"ID\"): {sql}"
    );
    assert!(
        sql.contains("PARTIAL_count_0"),
        "COUNT(col) partial must be PARTIAL_count_0: {sql}"
    );
    assert!(
        !sql.contains("COUNT(*)"),
        "COUNT(col) must not use COUNT(*): {sql}"
    );
}

/// SUM plan uses SUM("COL") at index 1.
#[test]
fn partial_agg_sql_sum_uses_sum_col() {
    let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
    assert!(
        sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_1""#),
        "SUM plan must use SUM(\"AMOUNT\") as PARTIAL_sum_1: {sql}"
    );
}

/// MIN/MAX plans use MIN/MAX("COL").
#[test]
fn partial_agg_sql_min_max_use_min_max_col() {
    let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
    assert!(
        sql.contains(r#"MIN("TS") AS "PARTIAL_min_2""#),
        "MIN plan must use MIN at index 2: {sql}"
    );
    assert!(
        sql.contains(r#"MAX("TS") AS "PARTIAL_max_3""#),
        "MAX plan must use MAX at index 3: {sql}"
    );
}

/// AVG plan emits TWO columns: sum first, count second.
#[test]
fn partial_agg_sql_avg_emits_sum_count_pair() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Avg,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let sql = build_partial_agg_sql(&plans, "aliased");
    // Must NOT emit an AVG() function.
    assert!(
        !sql.contains("AVG("),
        "must not use AVG() for partial avg: {sql}"
    );
    // Must emit SUM for the sum part.
    assert!(
        sql.contains(r#"SUM("SCORE") AS "PARTIAL_avg_sum_0""#),
        "AVG plan must emit SUM as PARTIAL_avg_sum_0: {sql}"
    );
    // Must emit COUNT(col) for the count part (not COUNT(*)).
    assert!(
        sql.contains(r#"COUNT("SCORE") AS "PARTIAL_avg_cnt_0""#),
        "AVG plan must emit COUNT(col) as PARTIAL_avg_cnt_0: {sql}"
    );
}

/// Mixed: COUNT/SUM/AVG — AVG contributes two columns at indices 2 (sum) and 2 (cnt),
/// i.e., each plan item is indexed by its position in the aggregates vec.
#[test]
fn partial_agg_sql_mixed_column_order_and_indices() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Avg,
            column: Some("SCORE".into()),
            arg_expr: None,
        },
    ];
    let sql = build_partial_agg_sql(&plans, "aliased");
    // COUNT at index 0.
    assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
    // SUM at index 1.
    assert!(sql.contains("PARTIAL_sum_1"), "sum at index 1: {sql}");
    // AVG at index 2 -> both sum and cnt use index 2.
    assert!(
        sql.contains("PARTIAL_avg_sum_2"),
        "avg sum at index 2: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_cnt_2"),
        "avg cnt at index 2: {sql}"
    );
}

/// Filter is applied when present.
#[test]
fn partial_agg_sql_applies_filter() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let sql = build_partial_agg_sql_filtered(&plans, "aliased", Some("\"ID\" > 5"));
    assert!(
        sql.contains("WHERE"),
        "filter must produce WHERE clause: {sql}"
    );
    assert!(
        sql.contains("\"ID\" > 5"),
        "filter expression must appear: {sql}"
    );
}

/// No filter: no WHERE clause.
#[test]
fn partial_agg_sql_no_filter_no_where() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let sql = build_partial_agg_sql(&plans, "aliased");
    assert!(
        !sql.contains("WHERE"),
        "no filter must produce no WHERE: {sql}"
    );
}

/// A partial aggregate over a rendered scalar expression argument substitutes
/// that fragment VERBATIM as the DataFusion function argument — it is NOT
/// re-quoted as an identifier — while a bare-column plan is unchanged.
#[test]
fn partial_sql_uses_rendered_expression_argument() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Sum,
            column: None,
            arg_expr: Some(r#"LENGTH("L_COMMENT")"#.into()),
        },
        AggregatePlan {
            kind: AggKind::Avg,
            column: None,
            arg_expr: Some(r#"("A" + "B")"#.into()),
        },
        // A bare-column plan alongside the expression ones stays quoted-identifier.
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
    ];
    let sql = build_partial_agg_sql(&plans, "aliased");

    // Expression argument is substituted raw (no identifier quoting of the whole expr).
    assert!(
        sql.contains(r#"SUM(LENGTH("L_COMMENT")) AS "PARTIAL_sum_0""#),
        "SUM over an expression must render the expression verbatim: {sql}"
    );
    // The rendered expression must NOT be wrapped as a single quoted identifier.
    assert!(
        !sql.contains(r#"SUM("LENGTH("#),
        "expression argument must not be re-quoted as an identifier: {sql}"
    );
    // AVG over an expression emits the sum/count pair over the same fragment.
    assert!(
        sql.contains(r#"SUM(("A" + "B")) AS "PARTIAL_avg_sum_1""#)
            && sql.contains(r#"COUNT(("A" + "B")) AS "PARTIAL_avg_cnt_1""#),
        "AVG over an expression must decompose over the rendered fragment: {sql}"
    );
    // The bare-column plan is unchanged.
    assert!(
        sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_2""#),
        "bare-column aggregate must remain quoted-identifier: {sql}"
    );
}

/// Single group key with COUNT(*): SELECT includes the key and COUNT(*).
#[test]
fn grouped_partial_agg_sql_single_key_count() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let sql = build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
    assert!(
        sql.contains(r#""REGION""#),
        "group key must appear in SQL: {sql}"
    );
    assert!(sql.contains("COUNT(*) AS"), "COUNT(*) must appear: {sql}");
    assert!(
        sql.contains("PARTIAL_count_0"),
        "partial count column at index 0: {sql}"
    );
    assert!(sql.contains("GROUP BY"), "must have GROUP BY clause: {sql}");
}

/// The emitted SELECT layout matches the GK_* then PARTIAL_* adapter contract:
/// group keys appear before partial aggregate columns in the SELECT list.
#[test]
fn grouped_partial_agg_sql_layout_matches_emits() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
    ];
    let sql = build_grouped_partial_agg_sql(
        &[r#""REGION""#.to_string(), r#""CATEGORY""#.to_string()],
        &plans,
        "aliased",
        None,
    );
    // Verify ordering: group key positions come before partial aggregate positions.
    let region_pos = sql.find(r#""REGION""#).expect("REGION must appear");
    let partial_count_pos = sql
        .find("PARTIAL_count_0")
        .expect("PARTIAL_count_0 must appear");
    assert!(
        region_pos < partial_count_pos,
        "group key must precede partial columns: {sql}"
    );
    let category_pos = sql.find(r#""CATEGORY""#).expect("CATEGORY must appear");
    assert!(
        category_pos < partial_count_pos,
        "second group key must precede partial columns: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_sum_1"),
        "SUM at index 1 must appear: {sql}"
    );
}

/// No LIMIT is ever added to a grouped partial aggregate SQL.
#[test]
fn grouped_partial_agg_sql_no_limit() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];
    let sql = build_grouped_partial_agg_sql(&[r#""REGION""#.to_string()], &plans, "aliased", None);
    assert!(
        !sql.contains("LIMIT"),
        "grouped partial SQL must not contain LIMIT: {sql}"
    );
}

/// Expression group keys (e.g. YEAR("DATE")) are inserted verbatim into the
/// DataFusion GROUP BY clause without any quoting or transformation.
#[test]
fn grouped_partial_agg_sql_expression_key_verbatim() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }];
    let expr_key = r#"YEAR("ORDER_DATE")"#.to_string();
    let sql =
        build_grouped_partial_agg_sql(std::slice::from_ref(&expr_key), &plans, "aliased", None);
    assert!(
        sql.contains(&expr_key),
        "expression key must appear verbatim in SQL: {sql}"
    );
    // Must appear in both SELECT and GROUP BY.
    let first_pos = sql.find(&expr_key).unwrap();
    let second_pos = sql[first_pos + 1..]
        .find(&expr_key)
        .map(|p| p + first_pos + 1);
    assert!(
        second_pos.is_some(),
        "expression key must appear in both SELECT and GROUP BY: {sql}"
    );
}

/// Stat aggregate partial emits COUNT(col), SUM(col), SUM(col*col) at index 0.
#[test]
fn partial_agg_sql_stat_emits_cnt_sum_sumsq() {
    for kind in &[
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ] {
        let plans = vec![AggregatePlan {
            kind: kind.clone(),
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let sql = build_partial_agg_sql(&plans, "aliased");
        assert!(
            sql.contains(r#"COUNT("SCORE") AS "PARTIAL_stat_cnt_0""#),
            "{kind:?} must emit COUNT(col) as PARTIAL_stat_cnt_0: {sql}"
        );
        assert!(
            sql.contains(r#"SUM("SCORE") AS "PARTIAL_stat_sum_0""#),
            "{kind:?} must emit SUM(col) as PARTIAL_stat_sum_0: {sql}"
        );
        assert!(
            sql.contains(r#"SUM("SCORE" * "SCORE") AS "PARTIAL_stat_sumsq_0""#),
            "{kind:?} must emit SUM(col*col) as PARTIAL_stat_sumsq_0: {sql}"
        );
        // Must NOT use AVG or STDDEV directly — only sufficient statistics
        assert!(
            !sql.contains("STDDEV"),
            "{kind:?} must not emit STDDEV: {sql}"
        );
        assert!(
            !sql.contains("VARIANCE"),
            "{kind:?} must not emit VARIANCE: {sql}"
        );
    }
}

/// Stat aggregate null fallback row has 3 values: cnt=0, sum=NULL, sumsq=NULL.
#[test]
fn stat_aggregate_null_fallback_row_has_three_values() {
    use exasol_udf_sdk::value::Value;
    for kind in &[
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ] {
        let plans = vec![AggregatePlan {
            kind: kind.clone(),
            column: Some("X".into()),
            arg_expr: None,
        }];
        let row = emit_null_partial_row(&plans);
        assert_eq!(row.len(), 3, "{kind:?} fallback row must have 3 values");
        assert_eq!(row[0], Value::Int64(0), "{kind:?} cnt must be 0");
        assert_eq!(row[1], Value::Null, "{kind:?} sum must be NULL");
        assert_eq!(row[2], Value::Null, "{kind:?} sumsq must be NULL");
    }
}

/// Mixed stat + count: stat at index 1 uses PARTIAL_stat_*_1 names.
#[test]
fn stat_aggregate_index_follows_plan_order() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::VarPop,
            column: Some("X".into()),
            arg_expr: None,
        },
    ];
    let sql = build_partial_agg_sql(&plans, "aliased");
    assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
    assert!(
        sql.contains("PARTIAL_stat_cnt_1"),
        "stat at index 1 must use suffix _1: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_stat_sum_1"),
        "stat sum at index 1: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_stat_sumsq_1"),
        "stat sumsq at index 1: {sql}"
    );
}

/// R2: ResourcesExhausted on the grouped/ungrouped partial-aggregate paths surfaces
/// as a memory-exhaustion error, not a storage error, and leaks no credentials.
///
/// This test exercises classify_scan_error directly (the same function now called
/// at all five mod.rs error sites) to confirm the classification is correct for
/// the DataFusion error shapes that aggregation and execution produce.
#[test]
fn resources_exhausted_on_partial_aggregate_path_surfaces_as_memory_error() {
    use crate::scan::emit::classify_scan_error;
    use datafusion::error::DataFusionError;

    let secret = "my-secret-key-value";
    let secrets = [secret];

    // 1. Direct ResourcesExhausted (e.g., from HashAggregateExec OOM).
    let direct = DataFusionError::ResourcesExhausted(
        "Failed to allocate additional 512 MiB for HashAggregateExec".to_string(),
    );
    let err = classify_scan_error(direct, &secrets);
    let text = err.to_string();
    assert!(
        text.contains("memory exhausted"),
        "direct ResourcesExhausted must surface as memory error: {text}"
    );
    assert!(
        !text.contains("assigned data could not be read"),
        "must NOT be classified as storage error: {text}"
    );
    assert!(!text.contains(secret), "must not leak credentials: {text}");

    // 2. Context-wrapped ResourcesExhausted (DataFusion sort wraps with .context()).
    let ctx_wrapped = DataFusionError::ResourcesExhausted("pool limit hit".to_string())
        .context(format!("External sort failed secret={secret}"));
    let err_ctx = classify_scan_error(ctx_wrapped, &secrets);
    let text_ctx = err_ctx.to_string();
    assert!(
        text_ctx.contains("memory exhausted"),
        "context-wrapped must surface as memory error: {text_ctx}"
    );
    assert!(
        !text_ctx.contains("assigned data could not be read"),
        "must NOT be classified as storage error: {text_ctx}"
    );
    assert!(
        !text_ctx.contains(secret),
        "context-wrapped must not leak credentials: {text_ctx}"
    );

    // 3. Non-ResourcesExhausted errors still route to the storage-error path.
    let storage_err = DataFusionError::Execution("S3 403 Forbidden".to_string());
    let err_storage = classify_scan_error(storage_err, &[]);
    let text_storage = err_storage.to_string();
    assert!(
        text_storage.contains("assigned data could not be read"),
        "non-OOM error must use the storage path: {text_storage}"
    );
    assert!(
        !text_storage.contains("memory exhausted"),
        "non-OOM error must NOT look like a memory error: {text_storage}"
    );
}
