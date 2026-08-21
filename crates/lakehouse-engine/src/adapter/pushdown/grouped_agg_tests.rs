use super::super::detect_aggregates;
use super::super::joins::{
    build_qualified_single_table_fallback_sql, referenced_column_projection,
};
use super::super::support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, extract_all_column_types, shard_count,
};
use super::super::test_support::*;
use super::*;
use crate::scan::spec::CommonScanSpec;

// NOTE on the `sum_emit_type` tests below: routing `sum_emit_type` through the
// canonical `parse_decimal_args` makes it GAIN a whitespace-trimming step it did
// not have before, because `parse_decimal_args` trims each argument before
// parsing. `DECIMAL(10, 2)` therefore yields `DECIMAL(36,2)` where it used to
// yield `DECIMAL(36, 2)` — the raw scale slice echoed verbatim. That is an
// INTENDED consequence of consolidation, not an incidental one, and it is
// unreachable from every producer of `col_ty` in this repo (each emits a
// canonical, already-trimmed `DECIMAL(p,s)` under a `p,s <= 36` guard).

/// The one representative neither invariant generates: with no comma there is no
/// scale text to diverge. The move comes solely from `parse_decimal_args`
/// defaulting an absent scale to `0`, where `sum_emit_type` used to require a
/// comma and decline the input entirely.
#[test]
fn sum_emit_type_absent_scale_widens_to_a_scale_zero_decimal() {
    assert_eq!(sum_emit_type("DECIMAL(10)"), "DECIMAL(36,0)");
}

/// Invariant (a) as a property over an OPEN input set: for every scale text that
/// is not already the canonical `i8` rendering, the answer is never the raw echo
/// the pre-consolidation parser produced. Only a canonical rendering — or the
/// numeric fallback — can emerge from a parsed `i8`. An open set is the right
/// shape here because the pre-consolidation parser echoed the raw scale text
/// without reading it, so the diverging input set has no closed enumeration.
///
/// The rows cover one divergence class each: untrimmed whitespace (the gained
/// trimming step); a leading `+` or a leading zero, which `i8` parsing accepts
/// and which therefore can only re-emerge canonically; a non-numeric scale, which
/// used to be interpolated verbatim into an EMITS type Exasol cannot parse and now
/// declines to the numeric fallback; a scale outside `i8`; and a further comma,
/// where the old `split_once(',')` kept `2,3` as the scale text while
/// `parse_decimal_args` rejects a third argument outright.
#[test]
fn sum_emit_type_never_echoes_a_non_canonical_scale_text() {
    // (raw scale text, canonical answer once parsed) — `None` = the parser
    // rejects the text, so the answer is the numeric fallback.
    let non_canonical: &[(&str, Option<&str>)] = &[
        (" 2", Some("DECIMAL(36,2)")),
        ("2 ", Some("DECIMAL(36,2)")),
        ("+2", Some("DECIMAL(36,2)")),
        ("02", Some("DECIMAL(36,2)")),
        ("-02", Some("DECIMAL(36,-2)")),
        ("X", None),
        ("2,3", None),
        ("200", None),
        ("", None),
    ];
    for (raw_scale, canonical) in non_canonical {
        let answer = sum_emit_type(&format!("DECIMAL(10,{raw_scale})"));
        assert_ne!(
            answer,
            format!("DECIMAL(36,{raw_scale})"),
            "a non-canonical scale text must never be echoed verbatim"
        );
        assert_eq!(
            answer,
            canonical.unwrap_or("DOUBLE PRECISION"),
            "wrong answer for scale text {raw_scale:?}"
        );
    }
}

/// Invariant (b) as a property over an OPEN input set: every precision
/// `parse_decimal_args` rejects now declines to the numeric fallback, where it
/// used to yield `DECIMAL(36,2)` regardless — the pre-consolidation parser bound
/// the precision as `_p` and never read it, so even an unrepresentable precision
/// borrowed a `DECIMAL(36,…)` width. That non-reading is also why the diverging
/// set is open rather than a closed enumeration. The rows cover one rejection
/// class each: a precision outside `u8`, a negative one, a non-numeric one, and
/// an empty or whitespace-only one.
#[test]
fn sum_emit_type_declines_every_precision_the_parser_rejects() {
    for rejected_precision in ["300", "256", "-1", "X", "", " "] {
        assert_eq!(
            sum_emit_type(&format!("DECIMAL({rejected_precision},2)")),
            "DOUBLE PRECISION",
            "precision {rejected_precision:?} is rejected by the parser, so the \
                 aggregate must fall back rather than borrow a DECIMAL(36,…) width"
        );
    }
}

/// A grouped-aggregate merge item that CASTs a scalar-over-aggregate to a
/// CHAR target must render that target as the declared, LENGTH-QUALIFIED
/// `CHAR(20) ASCII`: `render_scalar_over_merge`'s output is spliced into the
/// OUTER merge wrapper that Exasol's own engine parses and type-checks, where
/// a bare length-less `VARCHAR` is the "unexpected ')', expecting '('" parse
/// error and a collapsed `VARCHAR(20)` is the #192 "Data type mismatch"
/// rejection. Guards the grouped-merge one of the three Exasol-dialect CAST
/// consumers; the DataFusion-side renderability check in
/// `classify_scalar_over_aggregate` deliberately keeps bare `VARCHAR`.
#[test]
fn scalar_over_merge_casts_to_exasol_char_target() {
    let sum_node = serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{"type": "column", "name": "x"}]
    });
    let plans = vec![parse_agg_item(&sum_node).expect("SUM(x) must parse to a plan")];
    let node = serde_json::json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [sum_node],
        "dataType": {"type": "CHAR", "size": 20, "characterSet": "ASCII"}
    });
    let sql = render_scalar_over_merge(&node, &plans, &merge_select_items(&plans))
        .expect("CAST over a mergeable aggregate must render");
    assert!(
        sql.contains("CHAR(20) ASCII"),
        "Exasol-parsed merge wrapper needs the declared length-qualified CHAR \
             CAST target: {sql}"
    );
    assert!(
        !sql.contains("AS VARCHAR)"),
        "must NOT emit a bare length-less VARCHAR (Exasol rejects it): {sql}"
    );
    assert!(
        !sql.contains("VARCHAR(20)"),
        "must NOT collapse the declared CHAR target to VARCHAR(20) (#192): {sql}"
    );
}

/// A CAST-to-CHAR wrapping another CAST-to-CHAR over the same merged
/// aggregate must render `CHAR(20) ASCII` at BOTH levels: the Exasol-dialect
/// CHAR case is reached recursively through the translator, so a case that
/// only fired at the outermost level would leave the inner target collapsed
/// to `VARCHAR(20)` and reintroduce the #192 mismatch one level down.
#[test]
fn scalar_over_merge_nested_char_cast_renders_char_at_both_levels() {
    let sum_node = serde_json::json!({
        "type": "function_aggregate", "name": "SUM", "distinct": false,
        "arguments": [{"type": "column", "name": "x"}]
    });
    let plans = vec![parse_agg_item(&sum_node).expect("SUM(x) must parse to a plan")];
    let char_type = serde_json::json!({"type": "CHAR", "size": 20, "characterSet": "ASCII"});
    let inner = serde_json::json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [sum_node],
        "dataType": char_type,
    });
    let node = serde_json::json!({
        "type": "function_scalar_cast", "name": "CAST",
        "arguments": [inner],
        "dataType": char_type,
    });
    let sql = render_scalar_over_merge(&node, &plans, &merge_select_items(&plans))
        .expect("a nested CAST over a mergeable aggregate must render");
    assert_eq!(
        sql.matches("CHAR(20) ASCII").count(),
        2,
        "both CAST levels must declare the CHAR target: {sql}"
    );
    assert!(
        !sql.contains("VARCHAR(20)"),
        "neither level may collapse the declared CHAR target to VARCHAR(20): {sql}"
    );
}

/// Scenario (capability-extensions): a GROUP BY request carrying a
/// COUNT(DISTINCT) still declines (falls back to row scanning); grouped
/// distinct is explicitly out of scope.
#[test]
fn grouped_count_distinct_falls_back_to_row_scan() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [
            {"type": "column", "name": "REGION"},
            agg_item("COUNT", Some("L_SHIPMODE"), true),
        ],
    });
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "grouped COUNT(DISTINCT) must still decline (row-scan fallback)"
    );
    // A non-grouped detection also declines this shape (it has a GROUP BY).
    assert!(
        detect_aggregates(&req).is_none(),
        "the single-group path rejects any request carrying a non-empty GROUP BY"
    );
}

/// R.1: MIN/MAX over a DATE column must EMIT DATE, not DOUBLE PRECISION.
#[test]
fn partial_emits_min_max_preserve_date_timestamp_type() {
    let plans = vec![
        AggregatePlan {
            kind: AggKind::Min,
            column: Some("EVENT_DATE".into()),
            arg_expr: None,
        },
        AggregatePlan {
            kind: AggKind::Max,
            column: Some("EVENT_TS".into()),
            arg_expr: None,
        },
    ];
    let col_types = vec![
        ("EVENT_DATE".to_string(), "DATE".to_string()),
        ("EVENT_TS".to_string(), "TIMESTAMP".to_string()),
    ];
    let emits = partial_emits_items(&plans, &col_types, &[]);
    assert!(
        emits[0].contains("DATE") && !emits[0].contains("DOUBLE"),
        "MIN over DATE must emit DATE, not DOUBLE: {:?}",
        emits[0]
    );
    assert!(
        emits[1].contains("TIMESTAMP") && !emits[1].contains("DOUBLE"),
        "MAX over TIMESTAMP must emit TIMESTAMP, not DOUBLE: {:?}",
        emits[1]
    );
}

/// R.1: SUM over a DECIMAL(20,0) integer column must emit DECIMAL(36,0), not DOUBLE.
#[test]
fn partial_emits_sum_integer_stays_decimal() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }];
    let col_types = vec![("AMOUNT".to_string(), "DECIMAL(20,0)".to_string())];
    let emits = partial_emits_items(&plans, &col_types, &[]);
    assert!(
        emits[0].contains("DECIMAL") && !emits[0].contains("DOUBLE"),
        "SUM over DECIMAL integer must emit DECIMAL, not DOUBLE: {:?}",
        emits[0]
    );
    // Scale must be 0 (preserved from original DECIMAL(20,0)).
    assert!(
        emits[0].contains("DECIMAL(36,0)"),
        "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
        emits[0]
    );
}

/// R.1: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
#[test]
fn partial_emits_sum_double_stays_double() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
    let emits = partial_emits_items(&plans, &col_types, &[]);
    assert!(
        emits[0].contains("DOUBLE PRECISION"),
        "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
        emits[0]
    );
}

/// R.1: SUM over a VARCHAR/DATE column => validate_agg_col_types returns false (fall back).
#[test]
fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
    let col_types_varchar = vec![("NAME".to_string(), "VARCHAR(2000000)".to_string())];
    let sum_varchar = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("NAME".into()),
        arg_expr: None,
    }];
    assert!(
        !validate_agg_col_types(&sum_varchar, &col_types_varchar),
        "SUM over VARCHAR must fail validation (fall back to row scan)"
    );

    let col_types_date = vec![("EVENT_DATE".to_string(), "DATE".to_string())];
    let sum_date = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("EVENT_DATE".into()),
        arg_expr: None,
    }];
    assert!(
        !validate_agg_col_types(&sum_date, &col_types_date),
        "SUM over DATE must fail validation (fall back to row scan)"
    );
}

/// A grouped aggregate whose SUM targets a VARCHAR column must fall back to row
/// scan (return None from detect_group_by_aggregates + validate_agg_col_types) —
/// the same guard as the single-group path — rather than producing grouped scan SQL
/// that would generate an opaque UDF error at execution time.
#[test]
fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
    // Simulate the detection + validation sequence that handle_pushdown runs.
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("NAME"), false), // NAME is VARCHAR — invalid for SUM
        ],
    });

    // detect_group_by_aggregates must accept the shape (it doesn't know types).
    let detected = detect_group_by_aggregates(&req);
    assert!(
        detected.is_some(),
        "detect_group_by_aggregates must accept the shape: {req}"
    );
    let agg_plans = detected.unwrap().plans;

    // Validation with VARCHAR col_types must fail — triggering fall-back.
    let col_types = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &col_types),
        "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
    );

    // Confirm that a DATE column also fails for SUM.
    let col_types_date = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "DATE".to_string()),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &col_types_date),
        "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
    );

    // Confirm a numeric type passes (no fall back).
    let col_types_numeric = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    assert!(
        validate_agg_col_types(&agg_plans, &col_types_numeric),
        "validate_agg_col_types must pass for SUM over DOUBLE PRECISION"
    );
}

fn make_group_by_request(
    group_by: serde_json::Value,
    select_list: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": group_by,
        "selectList": select_list,
    })
}

/// Like `make_group_by_request`, but also carries `selectListDataTypes` so
/// ordering + type-position assertions are possible (positional matching
/// against the outer wrapper SELECT and group-key type resolution).
fn make_group_by_request_with_types(
    group_by: serde_json::Value,
    select_list: serde_json::Value,
    select_list_data_types: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": group_by,
        "selectList": select_list,
        "selectListDataTypes": select_list_data_types,
    })
}

/// `CAST(NAME AS CHAR(size))` as a `function_scalar_cast` node. Its own
/// `dataType` is the group key's declared result type, which is the only place
/// that type appears when the key is not also in the select list.
fn char_cast_key(size: u64, character_set: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "CHAR", "size": size, "characterSet": character_set},
        "arguments": [{"type": "column", "name": "NAME"}],
    })
}

/// `CAST(NAME AS VARCHAR(size))` — the VARCHAR control for `char_cast_key`.
fn varchar_cast_key(size: u64) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR", "size": size},
        "arguments": [{"type": "column", "name": "NAME"}],
    })
}

/// `MOD(<col>, <divisor>)` as a `function_scalar` node — renders to
/// `("<COL>" % <divisor>)` via `render_expression`. Used to build the #33
/// repro (`SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`) and its
/// interleaved/HAVING variants.
fn mod_item(col: &str, divisor: i64) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "MOD",
        "arguments": [
            {"type": "column", "name": col},
            {"type": "literal_exactnumeric", "value": divisor},
        ],
    })
}

/// `UPPER(<col>)` as a `function_scalar` node — renders to `upper("<COL>")`
/// via `render_expression`. Used to build all-expression multi-key GROUP BY
/// tuples where every element (not just some) is an expression.
fn upper_item(col: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "UPPER",
        "arguments": [
            {"type": "column", "name": col},
        ],
    })
}

/// A DECIMAL `selectListDataTypes` entry, per the `exasol_type_from_json` shape.
fn decimal_type(precision: u64, scale: u64) -> serde_json::Value {
    serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
}

/// Column reference in GROUP BY renders to a quoted identifier.
#[test]
fn detect_group_by_aggregates_column_key() {
    let req = make_group_by_request(
        serde_json::json!([{"type": "column", "name": "REGION"}]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            agg_item("COUNT", None, false),
        ]),
    );
    let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    let GroupedAggregateDetection {
        group_keys: keys,
        plans,
        ..
    } = result;
    assert_eq!(keys.len(), 1, "one group key");
    assert!(
        keys[0].contains("REGION"),
        "group key must reference REGION: {:?}",
        keys[0]
    );
    assert_eq!(plans.len(), 1, "one aggregate plan");
    assert_eq!(plans[0].kind, AggKind::Count);
}

/// Build a minimal grouped `ScanSpec` for the merge-SQL builder tests.
fn grouped_spec(result: &GroupedAggregateDetection) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    }
}

/// A grouped aggregate whose request carries an `orderBy` on a group key but
/// NO `limit` must still render an explicit final `ORDER BY` in its merge SQL:
/// once `ORDER_BY_COLUMN` is advertised Exasol no longer re-sorts the grouped
/// output, so a plain `GROUP BY … ORDER BY` must sort itself (add-topn-pushdown
/// B6). The sort key is rendered as a POSITIONAL output ordinal so it sorts the
/// type-cast output, not the lexicographic VARCHAR `GK_*` staging column.
#[test]
fn grouped_order_by_no_limit_renders_explicit_merge_order_by() {
    let mut req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "ID"}]),
        serde_json::json!([
            {"type": "column", "name": "ID"},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
    );
    // ORDER BY id ASC NULLS LAST, and deliberately NO "limit" key.
    req["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": {"type": "column", "name": "ID"},
        "isAscending": true,
        "nullsLast": true,
    }]);

    let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
    // The group key ID is output column 1 → positional ordinal, explicit dir+nulls.
    assert_eq!(
        build_grouped_order_by_clause(&req, &result),
        Some(GroupedOrderBy::Clause("1 ASC NULLS LAST".to_string())),
        "grouped ORDER BY must map the sort key to its 1-based output ordinal"
    );

    let group_key_types = group_key_exasol_types(&req, &result.group_keys, &result.select_items);
    let sql = build_grouped_aggregate_scan_sql(
        &grouped_spec(&result),
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &[],
        &result.select_items,
        None,
        0,
        &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        Some("1 ASC NULLS LAST"),
    );
    assert!(
        sql.contains(" ORDER BY 1 ASC NULLS LAST"),
        "merge SQL must render the explicit final ORDER BY: {sql}"
    );
    // No LIMIT was requested, so none is rendered.
    assert!(!sql.contains("LIMIT"), "no LIMIT requested: {sql}");
}

/// An `ORDER BY` on an aggregate that IS among the detected select-list plans
/// resolves to that aggregate's MERGED expression over the `PARTIAL_*` columns —
/// the same rewrite, by the same `AggregatePlan`-equality match, the merged
/// HAVING uses (issue #198). A group-key element mixed into the same `orderBy`
/// still renders as its positional output ordinal, unchanged.
#[test]
fn grouped_order_by_select_list_aggregate_renders_merged_partial() {
    let mut req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "ID"}]),
        serde_json::json!([
            {"type": "column", "name": "ID"},
            agg_item("SUM", Some("AMOUNT"), false),
        ]),
        serde_json::json!([decimal_type(20, 0), decimal_type(36, 2)]),
    );
    req["orderBy"] = serde_json::json!([
        {
            "type": "order_by_element",
            "expression": agg_item("SUM", Some("AMOUNT"), false),
            "isAscending": false,
            "nullsLast": true,
        },
        {
            "type": "order_by_element",
            "expression": {"type": "column", "name": "ID"},
            "isAscending": true,
            "nullsLast": false,
        },
    ]);

    let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
    assert_eq!(
        build_grouped_order_by_clause(&req, &detection),
        Some(GroupedOrderBy::Clause(
            r#"SUM("PARTIAL_sum_0") DESC NULLS LAST, 1 ASC NULLS FIRST"#.to_string()
        )),
        "an aggregate sort key must render as its merged partial, a group key as its ordinal"
    );
}

/// An `ORDER BY` on an aggregate ABSENT from the detected plans has no
/// `PARTIAL_*` column to merge over, and the adapter does not fabricate one:
/// the resolution reports `Unresolvable`, which `classify_request_shape` turns
/// into a `GroupByWrapper` route (issue #198).
#[test]
fn grouped_order_by_aggregate_absent_from_plans_is_unresolvable() {
    let mut req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "ID"}]),
        serde_json::json!([
            {"type": "column", "name": "ID"},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
    );
    req["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": agg_item("SUM", Some("AMOUNT"), false),
        "isAscending": false,
        "nullsLast": true,
    }]);

    let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
    assert_eq!(
        build_grouped_order_by_clause(&req, &detection),
        Some(GroupedOrderBy::Unresolvable),
        "an aggregate with no matching plan must not resolve to a fabricated partial"
    );
}

/// Scalar expression in GROUP BY (e.g., function_scalar YEAR) renders via render_expression.
#[test]
fn detect_group_by_aggregates_expression_key() {
    // A predicate_equal used as an expression key — render_expression can handle it.
    let req = make_group_by_request(
        serde_json::json!([{
            "type": "predicate_equal",
            "left": {"type": "column", "name": "STATUS"},
            "right": {"type": "literal_string", "value": "active"},
        }]),
        serde_json::json!([agg_item("SUM", Some("AMOUNT"), false),]),
    );
    let result = detect_group_by_aggregates(&req);
    // predicate_equal renders to (STATUS = 'active'), so it should succeed.
    assert!(result.is_some(), "renderable expression key must succeed");
    let GroupedAggregateDetection {
        group_keys: keys,
        plans,
        ..
    } = result.unwrap();
    assert_eq!(keys.len(), 1);
    assert!(keys[0].contains("="), "rendered expression must contain =");
    assert_eq!(plans[0].kind, AggKind::Sum);
}

/// An unsupported expression in GROUP BY causes the whole function to return None.
#[test]
fn detect_group_by_unsupported_expression_falls_back() {
    let req = make_group_by_request(
        serde_json::json!([{"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
        serde_json::json!([agg_item("COUNT", None, false)]),
    );
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "unsupported expression must fall back to None"
    );
}

/// Select list with a non-aggregate, non-column item causes fallback.
#[test]
fn detect_group_by_mixed_select_falls_back() {
    // function_scalar in selectList is not an aggregate and not a plain column.
    let req = make_group_by_request(
        serde_json::json!([{"type": "column", "name": "REGION"}]),
        serde_json::json!([
            {"type": "function_scalar", "name": "YEAR", "arguments": [{"type": "column", "name": "TS"}]},
            agg_item("COUNT", None, false),
        ]),
    );
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "non-aggregate non-column in selectList must fall back"
    );
}

/// Issue #52 regression guard (decision-log entry [4]): the exact composed
/// `pushdownRequest` Exasol emits for
/// `SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM EVENTS GROUP BY id) t`
/// — a real `groupBy` but a `selectList` of only a `literal_null` placeholder
/// (Exasol's "count the groups" rewrite: the outer query needs only the
/// per-group row count, not the inner values). Fed verbatim (including the
/// `from`/`type`/`columnNr`/`tableName` fields the detection path ignores,
/// to prove they don't perturb parsing) from the spike's captured JSON.
///
/// Detection must preserve the GROUP BY (return `Some` with real group keys
/// and NO aggregate plan) instead of falling back to a row scan — a row-scan
/// fallback returns one row per source row, not per group, which is only
/// accidentally correct when the group column happens to be unique (see
/// decision-log entry [4]'s caveat). The rendered scan SQL must never
/// reference a phantom `"NULL"` column identifier and must retain a real
/// `GROUP BY` clause.
#[test]
fn composed_nested_aggregate_request_does_not_reference_phantom_column() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "from": { "name": "EVENTS", "type": "table" },
        "groupBy": [
            { "columnNr": 0, "name": "ID", "tableName": "EVENTS", "type": "column" }
        ],
        "selectList": [ { "type": "literal_null" } ],
        "selectListDataTypes": [ { "type": "BOOLEAN" } ],
        "type": "select"
    });
    let result = detect_group_by_aggregates(&req).expect(
        "composed literal-only selectList must preserve GROUP BY, not fall back to row scan",
    );
    assert_eq!(result.group_keys.len(), 1, "one group key from groupBy");
    assert!(
        result.group_keys[0].contains("ID"),
        "group key must reference ID: {:?}",
        result.group_keys[0]
    );
    assert!(
        result.plans.is_empty(),
        "a literal placeholder contributes no aggregate plan"
    );
    assert!(
        matches!(
            result.select_items.as_slice(),
            [GroupedSelectItem::Constant {
                select_index: 0,
                ..
            }]
        ),
        "the literal_null item must classify as a Constant: {:?}",
        result.select_items
    );

    // The generated grouped scan SQL must group by GK_0 and must never
    // reference a phantom "NULL" column identifier.
    let group_key_types = group_key_exasol_types(&req, &result.group_keys, &result.select_items);
    let sql = build_grouped_aggregate_scan_sql(
        &ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(result.plans.clone()),
                group_keys: Some(result.group_keys.clone()),
                storage: sample_storage(),
                ..Default::default()
            },
            files: vec![],
        },
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &[],
        &result.select_items,
        None,
        0,
        &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    );
    assert!(
        !sql.contains(r#""NULL""#),
        "grouped scan SQL must not reference a phantom \"NULL\" identifier: {sql}"
    );
    assert!(
        sql.contains(r#"GROUP BY "GK_0""#),
        "outer wrapper must group by GK_0 to yield one row per distinct group: {sql}"
    );
    // The constant placeholder projects a typed literal (declared BOOLEAN),
    // not an empty select list and not a bare-literal column reference.
    assert!(
        sql.contains("SELECT CAST(NULL AS BOOLEAN) FROM"),
        "outer wrapper must project the type-cast constant placeholder: {sql}"
    );
}

/// Code-review follow-up on issue #52: `literal_bool` was missing from the
/// literal-type set used to classify grouped `selectList` constants (only
/// `literal_null` and six other literal kinds were listed, and the
/// renderer in `vs-expression` supports `literal_bool` — see
/// `render_expression`). A boolean literal placeholder in a grouped
/// selectList (e.g. `SELECT k, TRUE AS flag, COUNT(*) FROM t GROUP BY k`)
/// used to fall through to the group-key-matching `_` arm, fail to match
/// any group key, and abort the ENTIRE grouped-aggregate detection to
/// `None` — exactly the bug class the `literal_null` case guards against,
/// just for `literal_bool`. `LITERAL_SELECTLIST_TYPES` closes this gap.
#[test]
fn literal_bool_selectlist_item_classifies_as_constant_not_group_key() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "ID"}]),
        serde_json::json!([
            {"type": "column", "name": "ID"},
            {"type": "literal_bool", "value": true},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([
            decimal_type(20, 0),
            serde_json::json!({"type": "boolean"}),
            decimal_type(20, 0),
        ]),
    );
    let result = detect_group_by_aggregates(&req).expect(
        "a literal_bool selectList item must classify as Constant, not abort detection to None",
    );
    assert!(
        matches!(
            result.select_items[1],
            GroupedSelectItem::Constant {
                select_index: 1,
                ..
            }
        ),
        "the literal_bool item must classify as a Constant, not fall through \
             to the group-key arm: {:?}",
        result.select_items
    );
}

/// #33 repro: an aggregate placed before the single group key in the
/// selectList must classify with `select_index` 0 for the aggregate and 1
/// for the group key — the original ordinals, not a keys-first reorder.
#[test]
fn detect_group_by_aggregates_preserves_select_list_order() {
    // SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)
    let req = make_group_by_request(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
    );
    let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(result.group_keys.len(), 1, "one group key");
    assert_eq!(result.plans.len(), 1, "one aggregate plan");
    assert_eq!(
        result.select_items,
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        "classification must preserve original select-list ordinals: {:?}",
        result.select_items
    );
}

/// Interleaved multi-key GROUP BY: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
/// Each classified item must carry its own selectList ordinal and the
/// correct group-key slot (k1 → slot 0, k2 → slot 1), even though the
/// aggregate sits between them in the select list.
#[test]
fn detect_group_by_aggregates_interleaved_multi_key_preserves_order() {
    let req = make_group_by_request(
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            {"type": "column", "name": "YEAR"},
        ]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("SCORE"), false),
            {"type": "column", "name": "YEAR"},
        ]),
    );
    let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(result.group_keys.len(), 2, "two group keys");
    assert_eq!(result.plans.len(), 1, "one aggregate plan");
    assert_eq!(
        result.select_items,
        vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 1,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 1,
                select_index: 2,
            },
        ],
        "classification must preserve interleaved ordinals: {:?}",
        result.select_items
    );
}

/// Expression group key placed after an aggregate:
/// `SELECT COUNT(*), MOD(id,4) ... GROUP BY MOD(id,4)`.
#[test]
fn detect_group_by_aggregates_expr_key_after_agg_preserves_order() {
    let req = make_group_by_request(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([agg_item("COUNT", None, false), mod_item("ID", 4)]),
    );
    let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(
        result.select_items,
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        "expression key after aggregate must classify by original ordinal: {:?}",
        result.select_items
    );
}

/// Aggregate-first GROUP BY with HAVING present: HAVING does not change
/// selectList classification, but this exercises the same aggregate-first
/// shape that flows into the HAVING-present outer-wrapper path.
#[test]
fn detect_group_by_aggregates_aggregate_first_with_having_preserves_order() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [mod_item("ID", 4)],
        "selectList": [agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)],
        "having": {
            "type": "predicate_greater",
            "left": agg_item("SUM", Some("SCORE"), false),
            "right": {"type": "literal_exactnumeric", "value": 100},
        },
    });
    let result = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(
        result.select_items,
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        "HAVING presence must not affect selectList classification order: {:?}",
        result.select_items
    );
}

/// All-expression multi-key GROUP BY: `SELECT MOD(id,4), UPPER(name), COUNT(*)
/// ... GROUP BY MOD(id,4), UPPER(name)`. Every tuple element is an expression
/// (none a plain column) and must still be detected, each rendered on its own,
/// and each element must appear rendered individually (not merged/collapsed)
/// in the SQL built from the detection. If one element of the tuple is
/// untranslatable, the whole detection must fall back to `None` (full
/// raw-scan fallback), not a partial/degraded pushdown.
#[test]
fn detect_group_by_all_expression_multi_key() {
    let req = make_group_by_request(
        serde_json::json!([mod_item("ID", 4), upper_item("NAME")]),
        serde_json::json!([
            mod_item("ID", 4),
            upper_item("NAME"),
            agg_item("COUNT", None, false),
        ]),
    );
    let result = detect_group_by_aggregates(&req).expect("all-expression multi-key must detect");
    assert_eq!(result.group_keys.len(), 2, "two expression group keys");
    assert!(
        result.group_keys[0].contains('%') && result.group_keys[0].contains('4'),
        "key 0 must render the MOD expression: {:?}",
        result.group_keys
    );
    assert!(
        result.group_keys[1].to_lowercase().contains("upper"),
        "key 1 must render the UPPER expression: {:?}",
        result.group_keys
    );
    assert_eq!(result.plans.len(), 1, "one aggregate plan");
    assert_eq!(
        result.select_items,
        vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 1,
                select_index: 1,
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 2,
            },
        ],
        "each expression key must classify to its own slot: {:?}",
        result.select_items
    );

    // Each element must be rendered per-element (not merged) in the built SQL:
    // the per-shard scan spec's common blob carries both rendered fragments
    // verbatim, embedded in the SQL literal that drives the UDF call.
    let col_types: Vec<(String, String)> = vec![];
    let group_key_types = vec!["VARCHAR(2000000)".to_string(); 2];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &aggregate_types,
        &result.select_items,
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    );
    assert!(
        sql.contains("% 4"),
        "built SQL must carry the MOD key rendered on its own: {sql}"
    );
    assert!(
        sql.to_lowercase().contains("upper("),
        "built SQL must carry the UPPER key rendered on its own: {sql}"
    );
    assert!(
        sql.contains(r#""GK_0""#) && sql.contains(r#""GK_1""#),
        "built SQL must emit both group-key slots: {sql}"
    );

    // One untranslatable element in the tuple must collapse detection to None.
    let bad_req = make_group_by_request(
        serde_json::json!([mod_item("ID", 4), {"type": "fn_custom_unsupported", "name": "MYSTERY"}]),
        serde_json::json!([
            mod_item("ID", 4),
            {"type": "fn_custom_unsupported", "name": "MYSTERY"},
            agg_item("COUNT", None, false),
        ]),
    );
    assert!(
        detect_group_by_aggregates(&bad_req).is_none(),
        "one untranslatable tuple element must force full fallback to None"
    );
}

/// Helper: build grouped aggregate scan SQL.
/// Keys-first classification: group keys at ordinals 0..m, aggregates after.
fn keys_first_select_items(group_keys: usize, aggregates: usize) -> Vec<GroupedSelectItem> {
    let mut items = Vec::with_capacity(group_keys + aggregates);
    for slot in 0..group_keys {
        items.push(GroupedSelectItem::GroupKey {
            group_key_slot: slot,
            select_index: slot,
        });
    }
    for slot in 0..aggregates {
        items.push(GroupedSelectItem::Aggregate {
            plan_slot: slot,
            select_index: group_keys + slot,
        });
    }
    items
}

fn build_grouped_agg_sql(
    group_keys: Vec<String>,
    agg_plans: Vec<AggregatePlan>,
    files: Vec<String>,
    g: usize,
) -> String {
    let col_types: Vec<(String, String)> = vec![
        ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let files_with_sizes: Vec<FileEntry> =
        files.into_iter().map(|p| FileEntry::new(p, 1)).collect();
    let shards = crate::adapter::sharding::partition_files_by_bytes(files_with_sizes, g);
    let select_items = keys_first_select_items(group_keys.len(), agg_plans.len());
    build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &group_keys,
        &[],
        &agg_plans,
        &[],
        &select_items,
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    )
}

/// Grouped scan-driving SQL fans out via GROUP BY shard_key over G work units,
/// serializing the common blob once and one files literal per shard.
#[test]
fn grouped_fan_out_common_once_files_per_shard() {
    // Two distinct files, forced onto two shards (2 nodes × factor 1).
    let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
    let g = shard_count(2, 1, files.len());
    let sql = build_grouped_agg_sql(
        vec!["\"REGION\"".into()],
        vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        files,
        g,
    );
    assert!(
        !sql.contains("IPROC()"),
        "grouped SQL must NOT contain IPROC(): {sql}"
    );
    assert!(
        sql.contains("GROUP BY shard_key"),
        "grouped SQL inner must GROUP BY shard_key: {sql}"
    );
    assert!(
        sql.contains("AS shards(shard_key, files)"),
        "grouped fan-out must alias the VALUES table as shards(shard_key, files): {sql}"
    );

    // Common blob (credentials + tuning) serialized once, not per shard.
    assert_eq!(
        sql.matches("http://minio:9000").count(),
        1,
        "grouped common blob (endpoint) must appear exactly once: {sql}"
    );
    assert_eq!(
        sql.matches("memory_pool_fraction").count(),
        1,
        "grouped common blob (tuning payload) must appear exactly once: {sql}"
    );

    // Each shard's file appears exactly once, in its own VALUES row.
    for file in ["f0.parquet", "f1.parquet"] {
        assert_eq!(
            sql.matches(file).count(),
            1,
            "grouped shard file {file} must appear exactly once: {sql}"
        );
    }
}

/// The `GROUP BY shard_key` fan-out lives INSIDE the distributor subquery, while
/// the OUTER wrapper re-groups the per-shard partials on the user's group keys
/// (`GROUP BY "GK_0"`) over the scalar scan (decision [5]/[7]). The two GROUP BYs
/// are at different query levels: shard_key groups the fan-out `VALUES` rows for
/// round-robin distribution; GK_* re-groups the partial groups every shard emits.
#[test]
fn grouped_group_by_shard_key_inside_distributor() {
    let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
    let g = shard_count(2, 1, files.len());
    let sql = build_grouped_agg_sql(
        vec!["\"REGION\"".into()],
        vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        files,
        g,
    );

    // The distributor carries the shard_key fan-out.
    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the shard_key fan-out must live in the distributor subquery: {sql}"
    );
    // The outer wrapper re-groups on the user key staging column.
    assert!(
        sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
        "the outer wrapper must re-group on the user group key GK_0: {sql}"
    );
    // The shard_key GROUP BY is nested strictly BEFORE the outer GK_0 GROUP BY:
    // the distributor's grouping is not the outer one.
    let shard_gb = sql
        .find("GROUP BY shard_key")
        .expect("shard_key GROUP BY present");
    let gk_gb = sql
        .find(r#"GROUP BY "GK_0""#)
        .expect("GK_0 GROUP BY present");
    assert!(
        shard_gb < gk_gb,
        "shard_key GROUP BY (distributor) must precede the outer GK_0 GROUP BY: {sql}"
    );
    // No materializing SELECT * wrapper between the outer re-group and the scan.
    assert!(
        !sql.contains("SELECT * FROM ("),
        "grouped wrapper must not use a SELECT * materialization boundary: {sql}"
    );
}

/// Single-shard grouped: the outer re-group sits over a from-less scalar scan on
/// literals — the distributor short-circuits (no `VALUES`, no shard_key grouping).
#[test]
fn grouped_single_shard_short_circuits_distributor() {
    let sql = build_grouped_agg_sql(
        vec!["\"REGION\"".into()],
        vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        vec!["s3://w/only.parquet".into()],
        1,
    );

    assert!(
        !sql.contains("VALUES") && !sql.contains("shard_key"),
        "single-shard grouped must short-circuit the distributor: {sql}"
    );
    assert!(
        sql.contains(&format!("FROM (SELECT {SCAN_UDF_NAME}(")),
        "the outer re-group reads directly from the from-less scalar scan: {sql}"
    );
    assert!(
        sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
        "the outer wrapper still re-groups on the user group key GK_0: {sql}"
    );
}

/// LIMIT is NOT pushed into the shard scan for a grouped query. The shared common
/// blob (arg 0) must not carry "limit"; only the outer wrapper may apply LIMIT.
#[test]
fn grouped_common_blob_has_no_limit() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            limit: Some(100), // LIMIT should NOT appear inside the shard spec JSON
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &["\"REGION\"".to_string()],
        &[],
        &[AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        &[],
        &keys_first_select_items(1, 1),
        Some(100),
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    );
    // The shared common blob (arg 0) is built once with limit = None, so it must
    // NOT carry a "limit" key — this is the structural LIMIT-exclusion invariant.
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\""),
        "grouped common blob must NOT carry limit: {common}"
    );
    // The outer wrapper may still apply the final LIMIT.
    assert!(
        sql.contains("LIMIT 100"),
        "outer wrapper should still apply the final LIMIT: {sql}"
    );
}

/// A nonzero offset must never reach the per-shard fan-out spec: the common
/// blob shared by every shard carries neither "limit" nor an "offset" key —
/// there is no offset field on `CommonScanSpec` at all (design invariant: no
/// `ScanSpec`/UDF wire change), so this also pins that no such field leaks into
/// the shared JSON. The outer wrapper is the only place the offset renders
/// (fix-191-order-by-offset).
#[test]
fn grouped_merge_offset_never_reaches_per_shard_spec() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            limit: Some(100),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &["\"REGION\"".to_string()],
        &[],
        &[AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        &[],
        &keys_first_select_items(1, 1),
        Some(100),
        3,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        Some("1 ASC NULLS LAST"),
    );
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\"") && !common.contains("\"offset\""),
        "grouped common blob must NOT carry limit or offset: {common}"
    );
    assert!(
        sql.contains("ORDER BY 1 ASC NULLS LAST LIMIT 100 OFFSET 3"),
        "outer wrapper applies the final ORDER BY ... LIMIT ... OFFSET: {sql}"
    );
}

/// Byte-identical requirement (fix-191-order-by-offset): a zero offset renders
/// the exact pre-change ` LIMIT {n}` string with no OFFSET token, so every
/// already-correct SQL-shape assertion for the grouped-agg path keeps passing
/// unchanged.
#[test]
fn grouped_merge_zero_offset_is_byte_identical_to_bare_limit() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            limit: Some(100),
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Count,
                column: None,
                arg_expr: None,
            }]),
            group_keys: Some(vec!["\"REGION\"".into()]),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = crate::adapter::sharding::partition_files_by_bytes(files, g);
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &["\"REGION\"".to_string()],
        &[],
        &[AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        &[],
        &keys_first_select_items(1, 1),
        Some(100),
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    );
    assert!(
        sql.ends_with(" LIMIT 100"),
        "zero offset must render the bare pre-offset LIMIT clause: {sql}"
    );
    assert!(
        !sql.contains("OFFSET"),
        "zero offset must never render an OFFSET token: {sql}"
    );
}

/// The grouped merge renders `GROUP BY … ORDER BY … LIMIT n OFFSET m` in that
/// exact clause order (fix-191-order-by-offset, capture rows 5-8):
/// `render_limit_offset` is the shared seam every reachable wrapper calls, and
/// this pins the grouped merge's wiring into it.
#[test]
fn grouped_merge_renders_limit_offset_in_clause_order() {
    let mut req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "ID"}]),
        serde_json::json!([
            {"type": "column", "name": "ID"},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([decimal_type(20, 0), decimal_type(20, 0)]),
    );
    req["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": {"type": "column", "name": "ID"},
        "isAscending": true,
        "nullsLast": true,
    }]);

    let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
    let group_key_types = group_key_exasol_types(&req, &result.group_keys, &result.select_items);
    let sql = build_grouped_aggregate_scan_sql(
        &grouped_spec(&result),
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &[],
        &result.select_items,
        Some(2),
        1,
        &[("ID".to_string(), "DECIMAL(20,0)".to_string())],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        Some("1 ASC NULLS LAST"),
    );
    assert!(
        sql.ends_with(" ORDER BY 1 ASC NULLS LAST LIMIT 2 OFFSET 1"),
        "merge SQL must render GROUP BY … ORDER BY … LIMIT n OFFSET m in that order: {sql}"
    );
    let group_by_pos = sql.find("GROUP BY").expect("must contain GROUP BY");
    let order_by_pos = sql.find(" ORDER BY").expect("must contain ORDER BY");
    let limit_pos = sql.find(" LIMIT").expect("must contain LIMIT");
    let offset_pos = sql.find(" OFFSET").expect("must contain OFFSET");
    assert!(
        group_by_pos < order_by_pos && order_by_pos < limit_pos && limit_pos < offset_pos,
        "clauses must appear in GROUP BY, ORDER BY, LIMIT, OFFSET order: {sql}"
    );
}

/// Grouped aggregate wrapper SQL re-groups partial results per user group key.
#[test]
fn grouped_aggregate_wrapper_sql_groups_by_user_key_cols() {
    let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
    let g = shard_count(2, 1, files.len());
    let sql = build_grouped_agg_sql(
        vec!["\"REGION\"".into(), "\"YEAR\"".into()],
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
        ],
        files,
        g,
    );
    // Outer wrapper must GROUP BY GK_0, GK_1 (the group key columns).
    assert!(
        sql.contains("GK_0"),
        "wrapper SQL must reference GK_0: {sql}"
    );
    assert!(
        sql.contains("GK_1"),
        "wrapper SQL must reference GK_1: {sql}"
    );
    // Outer GROUP BY must merge partial aggregates.
    assert!(
        sql.contains("SUM("),
        "wrapper must contain SUM for merge: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_count_0"),
        "wrapper must reference PARTIAL_count_0: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_sum_1"),
        "wrapper must reference PARTIAL_sum_1: {sql}"
    );
    // Outer must have GROUP BY GK_0, GK_1.
    let outer_group_by = sql
        .rfind("GROUP BY")
        .expect("must have GROUP BY in outer wrapper");
    let outer_group_by_clause = &sql[outer_group_by..];
    assert!(
        outer_group_by_clause.contains("GK_0"),
        "outer GROUP BY must include GK_0: {outer_group_by_clause}"
    );
    assert!(
        outer_group_by_clause.contains("GK_1"),
        "outer GROUP BY must include GK_1: {outer_group_by_clause}"
    );
}

/// Extract the outer wrapper's SELECT list (between the leading `SELECT `
/// and the `FROM (` that opens the fan-out subselect), split on the
/// top-level commas of each column expression. Aggregate expressions and
/// CAST(...) fragments never contain a bare `, ` outside of nested
/// parens/quotes for the shapes used in these tests (SUM/COUNT merges and
/// CAST("GK_i" AS ...)), so a paren-depth-aware split is sufficient.
fn outer_select_items(sql: &str) -> Vec<String> {
    let from_pos = sql
        .find(" FROM (")
        .expect("must have outer FROM (: sql={sql}");
    let select_str = &sql["SELECT ".len()..from_pos];
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for ch in select_str.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                items.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

/// Build grouped aggregate scan SQL with explicit (non-keys-first) `select_items`
/// and declared group-key types, so ordering + CAST type can be asserted.
fn build_grouped_agg_sql_with_select_items(
    group_keys: Vec<String>,
    group_key_types: Vec<String>,
    agg_plans: Vec<AggregatePlan>,
    aggregate_types: Vec<String>,
    select_items: Vec<GroupedSelectItem>,
    having: Option<&str>,
) -> String {
    let col_types: Vec<(String, String)> = vec![
        ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
        ("SCORE".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(agg_plans.clone()),
            group_keys: Some(group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
    build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &group_keys,
        &group_key_types,
        &agg_plans,
        &aggregate_types,
        &select_items,
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        having,
        None,
    )
}

/// #33 repro: `SELECT SUM(score), MOD(id,4) ... GROUP BY MOD(id,4)`.
/// The outer wrapper SELECT must place the merged SUM at position 0 and
/// the CAST'd group key at position 1 — matching the user's selectList
/// order, not the inner fan-out's keys-first shape.
#[test]
fn grouped_wrapper_agg_before_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec!["DECIMAL(9,0)".to_string()],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec!["DOUBLE PRECISION".to_string()],
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        None,
    );
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        2,
        "outer SELECT must have exactly 2 items: {items:?}"
    );
    assert!(
        items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
        "position 0 must be the merged aggregate: {items:?}"
    );
    assert!(
        items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
        "position 1 must be the CAST'd group key with its declared type: {items:?}"
    );
}

/// Interleaved multi-key: `SELECT k1, SUM(score), k2 ... GROUP BY k1, k2`.
/// Outer SELECT order must be [key0, aggregate, key1], matching selectList.
#[test]
fn grouped_wrapper_interleaved_multi_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#""REGION""#.to_string(), r#""YEAR""#.to_string()],
        vec!["VARCHAR(100)".to_string(), "DECIMAL(4,0)".to_string()],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec!["DOUBLE PRECISION".to_string()],
        vec![
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 1,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 1,
                select_index: 2,
            },
        ],
        None,
    );
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        3,
        "outer SELECT must have exactly 3 items: {items:?}"
    );
    assert!(
        items[0].starts_with("CAST(\"GK_0\" AS VARCHAR(100))"),
        "position 0 must be key0's CAST: {items:?}"
    );
    assert!(
        items[1].contains("PARTIAL_sum_0") && items[1].starts_with("CAST(SUM("),
        "position 1 must be the merged aggregate: {items:?}"
    );
    assert!(
        items[2].starts_with("CAST(\"GK_1\" AS DECIMAL(4,0))"),
        "position 2 must be key1's CAST: {items:?}"
    );
}

/// Expression group key after an aggregate: `SELECT COUNT(*), MOD(id,4) ...
/// GROUP BY MOD(id,4)`. The key's declared type (DECIMAL, from
/// selectListDataTypes at its own select_index) must be preserved — this
/// is what stops the silent VARCHAR(2000000) fallback for #33 sub-case 3.
#[test]
fn grouped_wrapper_expr_key_after_agg_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec!["DECIMAL(9,0)".to_string()],
        vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        vec!["DECIMAL(18,0)".to_string()],
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        None,
    );
    let items = outer_select_items(&sql);
    assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
    assert!(
        items[0].contains("PARTIAL_count_0") && items[0].starts_with("CAST(SUM("),
        "position 0 must be the merged COUNT: {items:?}"
    );
    assert!(
        items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
        "position 1 must be the CAST'd group key, not a VARCHAR fallback: {items:?}"
    );
}

/// Aggregate-first GROUP BY with HAVING: `SELECT SUM(score), MOD(id,4) ...
/// GROUP BY MOD(id,4) HAVING SUM(score) > n`. Outer SELECT order must still
/// follow selectList (aggregate first) and HAVING must be appended after
/// GROUP BY, exercising the HAVING-present outer-wrapper path together with
/// non-keys-first ordering.
#[test]
fn grouped_wrapper_agg_first_with_having_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec!["DECIMAL(9,0)".to_string()],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec!["DOUBLE PRECISION".to_string()],
        vec![
            GroupedSelectItem::Aggregate {
                plan_slot: 0,
                select_index: 0,
            },
            GroupedSelectItem::GroupKey {
                group_key_slot: 0,
                select_index: 1,
            },
        ],
        Some(r#"(SUM("PARTIAL_sum_0") > 100)"#),
    );
    let having_pos = sql.find("HAVING").expect("must contain HAVING: {sql}");
    let group_by_pos = sql.find("GROUP BY").expect("must contain GROUP BY: {sql}");
    assert!(
        having_pos > group_by_pos,
        "HAVING must appear after GROUP BY: {sql}"
    );
    let select_only = &sql[..group_by_pos];
    let items = outer_select_items(select_only);
    assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
    assert!(
        items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
        "position 0 must be the merged aggregate even with HAVING present: {items:?}"
    );
    assert!(
        items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
        "position 1 must be the CAST'd group key even with HAVING present: {items:?}"
    );
}

/// `CASE WHEN <col> = 'R' THEN 1 ELSE 0 END` — the conditional-count inner
/// expression wrapped by #82's ROUND(...) select item.
fn case_flag_eq(col: &str, val: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_equal",
             "left": {"type": "column", "name": col},
             "right": {"type": "literal_string", "value": val}}
        ],
        "results": [
            {"type": "literal_exactnumeric", "value": 1},
            {"type": "literal_exactnumeric", "value": 0}
        ]
    })
}

/// #82's scalar-over-aggregate select item:
/// `ROUND(100.0 * SUM(CASE WHEN L_RETURNFLAG='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`.
fn round_pct_over_aggregates() -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "ROUND",
        "arguments": [
            {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                {"type": "function_scalar", "name": "MULT", "arguments": [
                    {"type": "literal_double", "value": 100.0},
                    agg_item_expr("SUM", case_flag_eq("L_RETURNFLAG", "R"), false)
                ]},
                agg_item("COUNT", None, false)
            ]},
            {"type": "literal_exactnumeric", "value": 2}
        ]
    })
}

fn soa_col_types() -> Vec<(String, String)> {
    vec![
        ("L_RETURNFLAG".to_string(), "VARCHAR(1)".to_string()),
        ("L_QUANTITY".to_string(), "DECIMAL(36,2)".to_string()),
        (
            "L_EXTENDEDPRICE".to_string(),
            "DOUBLE PRECISION".to_string(),
        ),
    ]
}

/// Drive detection then the outer-wrapper builder with the detection outputs
/// (plans + the plans-aligned `plan_types`), mirroring the production grouped
/// branch of `handle_pushdown`.
fn build_grouped_from_detection(req: &serde_json::Value) -> String {
    let d = detect_group_by_aggregates(req)
        .expect("must detect the grouped scalar-over-aggregate pushdown");
    let group_key_types = group_key_exasol_types(req, &d.group_keys, &d.select_items);
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(d.plans.clone()),
            group_keys: Some(d.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    build_grouped_aggregate_scan_sql(
        &spec_template,
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &d.group_keys,
        &group_key_types,
        &d.plans,
        &d.plan_types,
        &d.select_items,
        None,
        0,
        &soa_col_types(),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    )
}

/// Task 3.1: `detect_group_by_aggregates` over #82's select list (plus a bare
/// `COUNT(*)` item) classifies the ROUND(...) item as `ScalarOverAggregate` and
/// folds its inner `SUM(CASE …)` + `COUNT(*)` into the shared plan list — the
/// nested `COUNT(*)` deduplicated against the bare `COUNT(*)` so there is exactly
/// ONE count plan (one `PARTIAL_*` column).
#[test]
fn grouped_scalar_over_aggregate_detects_and_dedups_inner_aggregates() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
        serde_json::json!([
            {"type": "column", "name": "L_RETURNFLAG"},
            agg_item("SUM", Some("L_QUANTITY"), false),
            agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
            round_pct_over_aggregates(),
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([
            serde_json::json!({"type": "varchar", "size": 1}),
            decimal_type(36, 2),
            serde_json::json!({"type": "double"}),
            decimal_type(5, 2),
            decimal_type(18, 0),
        ]),
    );
    let d = detect_group_by_aggregates(&req).expect("must detect grouped scalar-over-aggregate");

    // The ROUND item is classified as a scalar-over-aggregate at its own ordinal,
    // carrying its own declared type.
    assert!(
        matches!(
            &d.select_items[3],
            GroupedSelectItem::ScalarOverAggregate {
                select_index: 3,
                declared_type,
                ..
            } if declared_type == "DECIMAL(5,2)"
        ),
        "item 3 must be a ScalarOverAggregate with its declared type: {:?}",
        d.select_items[3]
    );

    // Plans: SUM(L_QUANTITY), AVG(L_EXTENDEDPRICE), SUM(CASE …), COUNT(*) — the
    // nested COUNT(*) and the bare COUNT(*) collapse to ONE plan.
    assert_eq!(
        d.plans.len(),
        4,
        "inner SUM(CASE) + COUNT(*) fold in; the two COUNT(*) dedup to one: {:?}",
        d.plans
    );
    let count_plans = d
        .plans
        .iter()
        .filter(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
        .count();
    assert_eq!(
        count_plans, 1,
        "the shared COUNT(*) must be a single plan: {:?}",
        d.plans
    );

    // The bare COUNT(*) select item (index 4) points at the SAME slot the nested
    // COUNT(*) folded into.
    let count_slot = d
        .plans
        .iter()
        .position(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
        .unwrap();
    assert!(
        matches!(
            d.select_items[4],
            GroupedSelectItem::Aggregate { plan_slot, select_index: 4 } if plan_slot == count_slot
        ),
        "the bare COUNT(*) must reuse the shared count slot {count_slot}: {:?}",
        d.select_items[4]
    );
}

/// Task 3.2: the outer wrapper renders the scalar-over-aggregate column over the
/// MERGED partials (`ROUND(… SUM("PARTIAL_*") / SUM("PARTIAL_*") …)`), cast to its
/// declared type, with NO source-column reference; the outer SELECT column count
/// equals the `selectList` length.
#[test]
fn grouped_scalar_over_aggregate_renders_merged_partials() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
        serde_json::json!([
            {"type": "column", "name": "L_RETURNFLAG"},
            agg_item("SUM", Some("L_QUANTITY"), false),
            agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
            round_pct_over_aggregates(),
        ]),
        serde_json::json!([
            serde_json::json!({"type": "varchar", "size": 1}),
            decimal_type(36, 2),
            serde_json::json!({"type": "double"}),
            decimal_type(5, 2),
        ]),
    );
    let sql = build_grouped_from_detection(&req);
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        4,
        "outer SELECT must have one item per selectList item: {items:?}"
    );

    let soa = &items[3];
    assert!(
        soa.contains("PARTIAL_"),
        "wrapper item must be over merged partials: {soa}"
    );
    assert!(
        soa.contains("SUM(\"PARTIAL_") && soa.contains("ROUND("),
        "wrapper must render ROUND over merged SUM(PARTIAL_*) partials: {soa}"
    );
    assert!(
        soa.starts_with("CAST(") && soa.contains("DECIMAL(5,2)"),
        "wrapper item must be CAST to its declared type at its own ordinal: {soa}"
    );
    // The nested aggregates' argument structure (the CASE, and every source
    // column) is subsumed into the PARTIAL_* rewrite — the outer wrapper exposes
    // only GK_*/PARTIAL_* columns.
    assert!(
        !soa.contains("CASE"),
        "the CASE must be folded into a PARTIAL_* column: {soa}"
    );
    assert!(
        !soa.contains("L_RETURNFLAG") && !soa.contains("L_QUANTITY"),
        "wrapper item must not reference any source column: {soa}"
    );
}

/// Task 3.3: a scalar-over-aggregate placed BEFORE the group key and a plain
/// aggregate yields outer SELECT items in `selectList` order, each cast from
/// `selectListDataTypes` at its own ordinal.
#[test]
fn grouped_scalar_over_aggregate_preserves_selectlist_order() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "L_RETURNFLAG"}]),
        serde_json::json!([
            round_pct_over_aggregates(),
            {"type": "column", "name": "L_RETURNFLAG"},
            agg_item("SUM", Some("L_QUANTITY"), false),
        ]),
        serde_json::json!([
            decimal_type(5, 2),
            serde_json::json!({"type": "varchar", "size": 1}),
            decimal_type(36, 2),
        ]),
    );
    let sql = build_grouped_from_detection(&req);
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        3,
        "outer SELECT must have 3 items in selectList order: {items:?}"
    );
    assert!(
        items[0].starts_with("CAST(")
            && items[0].contains("ROUND(")
            && items[0].contains("DECIMAL(5,2)"),
        "position 0 must be the scalar-over-aggregate, cast to its own type: {items:?}"
    );
    assert!(
        items[1].starts_with("CAST(\"GK_0\" AS VARCHAR(1))"),
        "position 1 must be the CAST'd group key at its own ordinal: {items:?}"
    );
    assert!(
        items[2].starts_with("CAST(SUM(\"PARTIAL_") && items[2].contains("DECIMAL(36,2)"),
        "position 2 must be the merged plain aggregate, cast to its own type: {items:?}"
    );
}

/// Task 3.4: a grouped request whose scalar-over-aggregate wraps a
/// `COUNT(DISTINCT …)` (undecomposable) declines grouped detection and routes to
/// the qualified single-table wrapper — `SELECT <selectList> FROM (<raw scan>) AS
/// "LHS_T0" GROUP BY …` with a `selectList`-matching column count — NOT a bare
/// `SELECT * FROM (…)` row scan (the `04000` bug).
#[test]
fn grouped_undecomposable_falls_back_to_qualified_wrapper() {
    let pushdown_req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"}],
        "selectList": [
            {"type": "column", "name": "L_RETURNFLAG", "tableName": "LINEITEM"},
            {"type": "function_scalar", "name": "ROUND", "arguments": [
                {"type": "function_scalar", "name": "FLOAT_DIV", "arguments": [
                    agg_item_expr("SUM", serde_json::json!({"type": "column", "name": "X", "tableName": "LINEITEM"}), false),
                    agg_item_expr("COUNT", serde_json::json!({"type": "column", "name": "Y", "tableName": "LINEITEM"}), true)
                ]},
                {"type": "literal_exactnumeric", "value": 2}
            ]}
        ],
        "selectListDataTypes": [
            serde_json::json!({"type": "varchar", "size": 1}),
            decimal_type(5, 2),
        ],
    });

    // The COUNT(DISTINCT) inner aggregate is undecomposable → detection declines.
    assert!(
        detect_group_by_aggregates(&pushdown_req).is_none(),
        "a nested COUNT(DISTINCT) must decline the grouped partial/merge path"
    );

    let request = serde_json::json!({
        "involvedTables": [{"name": "LINEITEM", "columns": [
            {"name": "L_RETURNFLAG", "dataType": {"type": "varchar", "size": 1}},
            {"name": "X", "dataType": {"type": "double"}},
            {"name": "Y", "dataType": {"type": "double"}},
        ]}]
    });
    let all_cols = extract_all_column_types(&request);
    // The shared referenced-column helper (issue #160) narrows the inner scan to
    // only the columns the wrapper references — here L_RETURNFLAG (GROUP BY +
    // select) and X, Y (nested inside the SUM/COUNT aggregate arguments), which is
    // the whole table, so the wrapper shape is identical to the old full-row scan.
    let (proj_cols, proj_types) = referenced_column_projection(&pushdown_req, &all_cols);
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: proj_cols,
            emit_exa_types: proj_types,
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out_spec,
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
    )
    .expect("qualified fallback must build");

    assert!(
        !sql.starts_with("SELECT * FROM"),
        "fallback must NOT be a bare row scan (the 04000 bug): {sql}"
    );
    assert!(
        sql.contains(" GROUP BY "),
        "fallback must render the GROUP BY: {sql}"
    );
    assert!(
        sql.contains("FROM (") && sql.contains("AS \"LHS_T0\""),
        "fallback must wrap one aliased raw fan-out subquery: {sql}"
    );
    assert!(
        sql.contains("COUNT(DISTINCT"),
        "the undecomposable aggregate is rendered verbatim for Exasol to compute: {sql}"
    );
    // The FIRST ` FROM (` is the outer wrapper's (the fan-out subquery's own
    // FROM comes later), so `outer_select_items` extracts the wrapper's SELECT.
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        2,
        "the wrapper must return exactly the selectList columns, not a full row: {items:?}"
    );
}

/// A HAVING `SUM(score) > literal` node built as Exasol sends it (a
/// `predicate_greater` whose `left` is a `function_aggregate`) must render
/// against the MERGE decomposition: the aggregate reference becomes the
/// merged partial expression `SUM("PARTIAL_sum_0")`, NOT the source column
/// `SUM("SCORE")` (which does not exist in the outer wrapper). This is the
/// #33 HAVING repro (`... GROUP BY MOD(id,4) HAVING SUM(score) > 250`).
#[test]
fn render_having_over_merge_rewrites_aggregate_to_partial() {
    let having = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("SUM", Some("SCORE"), false),
        "right": {"type": "literal_exactnumeric", "value": 250},
    });
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let rendered = render_having_over_merge(&having, &plans)
        .expect("HAVING over a known aggregate must render");
    assert_eq!(
        rendered, r#"(SUM("PARTIAL_sum_0") > 250)"#,
        "HAVING must reference the merged partial, not the source column: {rendered}"
    );
    assert!(
        !rendered.contains(r#""SCORE""#) && !rendered.contains("SUM(\"SCORE\")"),
        "HAVING must NOT reference the source column SCORE: {rendered}"
    );
}

/// The full outer-wrapper SQL for the #33 HAVING repro must carry the merged
/// HAVING `SUM("PARTIAL_sum_0") > 250` and must not reference the source
/// `SCORE` column in the HAVING clause.
#[test]
fn grouped_wrapper_having_over_aggregate_uses_merge_expression() {
    let req = make_group_by_request_with_types(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
        serde_json::json!([
            {"type": "double"},
            decimal_type(9, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
    let aggregate_types = detection.plan_types.clone();

    let having_node = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("SUM", Some("SCORE"), false),
        "right": {"type": "literal_exactnumeric", "value": 250},
    });
    let having = render_having_over_merge(&having_node, &detection.plans)
        .expect("HAVING must render over the merge decomposition");

    let col_types: Vec<(String, String)> =
        vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &detection.group_keys,
        &group_key_types,
        &detection.plans,
        &aggregate_types,
        &detection.select_items,
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        Some(&having),
        None,
    );
    let having_pos = sql.find("HAVING").expect("must contain HAVING");
    let having_clause = &sql[having_pos..];
    assert!(
        having_clause.contains(r#"SUM("PARTIAL_sum_0") > 250"#),
        "HAVING clause must use the merge expression: {having_clause}"
    );
    assert!(
        !having_clause.contains(r#""SCORE""#) && !having_clause.contains("SUM(\"SCORE\")"),
        "HAVING clause must NOT reference the source SCORE column: {having_clause}"
    );
}

/// A HAVING referencing an aggregate that is NOT present among the plans
/// (e.g. `COUNT(*)` when only `SUM(score)` was projected) cannot be merged,
/// so `render_having_over_merge` returns None — the signal for
/// `classify_request_shape` to route the request to `RequestShape::GroupByWrapper`
/// rather than drop the HAVING.
#[test]
fn render_having_over_merge_declines_unknown_aggregate() {
    let having = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("COUNT", None, false),
        "right": {"type": "literal_exactnumeric", "value": 10},
    });
    // Only SUM(score) was projected — COUNT(*) has no matching plan.
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    assert!(
        render_having_over_merge(&having, &plans).is_none(),
        "HAVING over an aggregate absent from the plans must not render"
    );
}

/// End-to-end wiring: `detect_group_by_aggregates`'s classification output
/// feeds directly into `build_grouped_aggregate_scan_sql` and the outer
/// wrapper SELECT follows the original selectList order (#33 repro, driven
/// through both functions together rather than a hand-built select_items).
#[test]
fn grouped_wrapper_outer_select_follows_select_list_order() {
    let req = make_group_by_request_with_types(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([agg_item("SUM", Some("SCORE"), false), mod_item("ID", 4)]),
        serde_json::json!([
            {"type": "double"},
            decimal_type(9, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
    let aggregate_types = detection.plan_types.clone();

    let col_types: Vec<(String, String)> =
        vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://wh/f0.parquet".to_string(), 1u64)]];
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &detection.group_keys,
        &group_key_types,
        &detection.plans,
        &aggregate_types,
        &detection.select_items,
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    );

    let items = outer_select_items(&sql);
    assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
    assert!(
        items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
        "position 0 must be the merged SUM (selectList order): {items:?}"
    );
    assert!(
        items[1].starts_with("CAST(\"GK_0\" AS DECIMAL(9,0))"),
        "position 1 must be the CAST'd group key with its declared type: {items:?}"
    );
}

/// Multi-key grouped SQL build with HAVING and LIMIT: `SELECT REGION,
/// SUM(score), MOD(id,4) ... GROUP BY REGION, MOD(id,4) HAVING SUM(score) >
/// 100 LIMIT 2`. HAVING and LIMIT must be placed ONLY in the outer wrapper —
/// never in the per-shard partial scan, which must emit every partial group
/// from every shard for the outer wrapper to merge and filter correctly.
#[test]
fn grouped_wrapper_multi_key_having_and_limit_outer_only() {
    let req = make_group_by_request_with_types(
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            mod_item("ID", 4),
        ]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("SCORE"), false),
            mod_item("ID", 4),
        ]),
        serde_json::json!([
            {"type": "varchar", "size": 100},
            {"type": "double"},
            decimal_type(9, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);
    let aggregate_types = detection.plan_types.clone();

    let having_node = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("SUM", Some("SCORE"), false),
        "right": {"type": "literal_exactnumeric", "value": 100},
    });
    let having = render_having_over_merge(&having_node, &detection.plans)
        .expect("HAVING must render over the merge decomposition");

    let col_types: Vec<(String, String)> =
        vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(detection.plans.clone()),
            group_keys: Some(detection.group_keys.clone()),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    // Multiple shards so the inner scan is a real `GROUP BY shard_key` fan-out,
    // not the single-shard direct-call shortcut.
    let shards = vec![
        vec![("s3://wh/f0.parquet".to_string(), 1u64)],
        vec![("s3://wh/f1.parquet".to_string(), 1u64)],
    ];
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &detection.group_keys,
        &group_key_types,
        &detection.plans,
        &aggregate_types,
        &detection.select_items,
        Some(2),
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        Some(&having),
        None,
    );

    // The per-shard partial scan ends at "GROUP BY shard_key"; everything up to
    // and including that point must carry neither HAVING nor LIMIT.
    let shard_group_end = sql
        .find("GROUP BY shard_key")
        .map(|i| i + "GROUP BY shard_key".len())
        .unwrap_or_else(|| panic!("must contain the inner per-shard fan-out: {sql}"));
    let inner_part = &sql[..shard_group_end];
    assert!(
        !inner_part.contains("HAVING"),
        "HAVING must not appear in the per-shard partial scan: {inner_part}"
    );
    assert!(
        !inner_part.contains("LIMIT"),
        "LIMIT must not appear in the per-shard partial scan: {inner_part}"
    );

    // Everything after the per-shard scan is the outer wrapper: it must carry
    // its own multi-key GROUP BY, then HAVING, then LIMIT, in that order.
    let outer_part = &sql[shard_group_end..];
    let outer_group_by_pos = outer_part
        .find("GROUP BY")
        .unwrap_or_else(|| panic!("outer wrapper must have its own GROUP BY: {outer_part}"));
    assert!(
        outer_part.contains(r#""GK_0""#) && outer_part.contains(r#""GK_1""#),
        "outer GROUP BY must reference both group-key slots: {outer_part}"
    );
    let having_pos = outer_part
        .find("HAVING")
        .unwrap_or_else(|| panic!("HAVING must appear in the outer wrapper: {outer_part}"));
    let limit_pos = outer_part
        .find("LIMIT 2")
        .unwrap_or_else(|| panic!("LIMIT must appear in the outer wrapper: {outer_part}"));
    assert!(
        outer_group_by_pos < having_pos,
        "outer GROUP BY must precede HAVING: {outer_part}"
    );
    assert!(
        having_pos < limit_pos,
        "HAVING must precede LIMIT in the outer wrapper: {outer_part}"
    );
}

/// An expression group key whose `groupBy` and `selectList` renderings
/// differ only by whitespace/casing must still resolve its declared type
/// by index (via `select_items`), not by comparing rendered SQL strings —
/// which would silently fall back to VARCHAR(2000000) on any drift.
#[test]
fn group_key_type_resolved_by_index_not_string_match() {
    // groupBy renders "(\"ID\" % 4)" (see MOD rendering); simulate a
    // whitespace/casing-drifted selectList rendering by using a
    // hand-built classification whose select_index points at a
    // selectListDataTypes slot the rendered-string form would never find.
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [mod_item("ID", 4)],
        "selectList": [
            agg_item("SUM", Some("SCORE"), false),
            mod_item("ID", 4),
        ],
        "selectListDataTypes": [
            {"type": "double"},
            decimal_type(9, 0),
        ],
    });
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    // Sanity: the real detection path already resolves this correctly by
    // index. Now prove the mechanism is index-based, not string-based, by
    // building a classification where the rendered groupBy fragment would
    // NOT string-match the (hypothetically drifted) selectList rendering,
    // yet the index-based lookup still finds DECIMAL(9,0) because it reads
    // selectListDataTypes[select_index] directly.
    let group_keys = vec![r#"("id" % 4)"#.to_string()]; // lowercase drift vs GK render
    let select_items = detection.select_items.clone();
    let types = group_key_exasol_types(&req, &group_keys, &select_items);

    assert_eq!(
        types,
        vec!["DECIMAL(9,0)".to_string()],
        "type must resolve via select_index, not via string-matching the (drifted) \
             rendered group key: {types:?}"
    );
}

/// Mixed-type multi-key GROUP BY: `SELECT REGION, MOD(id,4), COUNT(*) ...
/// GROUP BY REGION, MOD(id,4)`. `REGION` is a plain column declared VARCHAR;
/// `MOD(id,4)` is an expression declared DECIMAL. Each `GK_{i}` must resolve
/// its own declared type by its own `selectList` index — a shared/defaulted
/// VARCHAR for both would silently lose the DECIMAL key's real type.
#[test]
fn group_key_types_multi_key_mixed_types() {
    let req = make_group_by_request_with_types(
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            mod_item("ID", 4),
        ]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            mod_item("ID", 4),
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([
            {"type": "varchar", "size": 100},
            decimal_type(9, 0),
            decimal_type(18, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(types.len(), 2, "one declared type per group key");
    assert_eq!(
        types[0], "VARCHAR(100)",
        "the REGION key must resolve its own VARCHAR type, at its own select index: {types:?}"
    );
    assert_eq!(
        types[1], "DECIMAL(9,0)",
        "the MOD(id,4) key must resolve its own DECIMAL type, not a shared/defaulted \
             VARCHAR: {types:?}"
    );
}

/// An equal-length CASE group key (`CASE WHEN c_decimal_a < 0 THEN 'NEG' ELSE
/// 'POS' END`, the #192 primary shape) must resolve to `CHAR(3) ASCII` through
/// `group_key_exasol_types`, driven through the real `detect_group_by_aggregates`
/// entry point — not the bare `exasol_type_from_json` function. Exasol declares
/// this expression `CHAR(3) ASCII` (live-verified) because both branches are the
/// same length; the current catch-all renders it `VARCHAR(3) ASCII` instead,
/// which Exasol's type checker rejects with "Data type mismatch" (facet A).
#[test]
fn group_key_exasol_types_resolves_char_case_key() {
    let case_key = serde_json::json!({
        "type": "function_scalar_case",
        "name": "CASE",
        "arguments": [
            {"type": "predicate_less",
             "left": {"type": "column", "name": "C_DECIMAL_A"},
             "right": {"type": "literal_exactnumeric", "value": 0}}
        ],
        "results": [
            {"type": "literal_string", "value": "NEG"},
            {"type": "literal_string", "value": "POS"}
        ]
    });
    let req = make_group_by_request_with_types(
        serde_json::json!([case_key.clone()]),
        serde_json::json!([case_key, agg_item("COUNT", None, false)]),
        serde_json::json!([
            {"type": "CHAR", "size": 3, "characterSet": "ASCII"},
            decimal_type(18, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req)
        .expect("equal-length CASE group key must be detected as a grouped aggregate");
    assert_eq!(detection.group_keys.len(), 1, "one group key");

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(
        types,
        vec!["CHAR(3) ASCII".to_string()],
        "an equal-length CASE group key must resolve to CHAR(3) ASCII, not \
             VARCHAR(3) ASCII: {types:?}"
    );
}

/// CONTROL: a plain VARCHAR-declared group key (`REGION`) must keep resolving
/// to `VARCHAR(10)`, unaffected by the CHAR-type-declaration fix. MUST pass
/// both before and after that fix.
#[test]
fn group_key_exasol_types_resolves_varchar_key_unchanged() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "REGION"}]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([
            {"type": "varchar", "size": 10},
            decimal_type(18, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(
        types,
        vec!["VARCHAR(10)".to_string()],
        "a VARCHAR-declared group key must be unaffected: {types:?}"
    );
}

/// A group key that is NOT in the select list (`SELECT COUNT(*) … GROUP BY
/// CAST(NAME AS CHAR(20))`) carries no `selectListDataTypes` ordinal, so its
/// declared type is only readable from its own `groupBy` node. Without that
/// fallback the slot keeps the `VARCHAR(2000000)` "unknown width" default,
/// `blank_pad_char_group_keys` finds no CHAR width, and the key reaches
/// DataFusion unpadded — `'ab'` and `'ab   '` stay two groups where Exasol
/// returns one, with no outer `CAST("GK_0" AS CHAR(n))` on this path to
/// surface the divergence as a type error (#192 review finding).
#[test]
fn group_key_exasol_types_resolves_char_type_for_unprojected_group_key() {
    let req = make_group_by_request_with_types(
        serde_json::json!([char_cast_key(20, "UTF8")]),
        serde_json::json!([agg_item("COUNT", None, false)]),
        serde_json::json!([decimal_type(18, 0)]),
    );
    let detection = detect_group_by_aggregates(&req)
        .expect("an unprojected group key must still detect as a grouped aggregate");
    assert_eq!(detection.group_keys.len(), 1, "one group key");
    assert!(
        !detection
            .select_items
            .iter()
            .any(|item| matches!(item, GroupedSelectItem::GroupKey { .. })),
        "fixture precondition: the group key must NOT appear in the select list"
    );

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(
        types,
        vec!["CHAR(20)".to_string()],
        "an unprojected group key must resolve CHAR(20) from its own groupBy dataType: \
             {types:?}"
    );
}

/// CONTROL for the `groupBy` fallback: an unprojected group key whose own
/// `groupBy` node declares VARCHAR must resolve VARCHAR, never a CHAR width.
/// The fallback fires here (the node carries a `dataType`), so this pins that
/// it resolves the declared type rather than assuming CHAR — a VARCHAR key
/// blank-padded to a width would change grouping semantics for every ordinary
/// string GROUP BY that omits its key from the select list.
#[test]
fn group_key_exasol_types_resolves_varchar_type_for_unprojected_group_key() {
    let req = make_group_by_request_with_types(
        serde_json::json!([varchar_cast_key(10)]),
        serde_json::json!([agg_item("COUNT", None, false)]),
        serde_json::json!([decimal_type(18, 0)]),
    );
    let detection = detect_group_by_aggregates(&req)
        .expect("an unprojected group key must still detect as a grouped aggregate");

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(
        types,
        vec!["VARCHAR(10)".to_string()],
        "an unprojected VARCHAR-declared group key must resolve VARCHAR(10): {types:?}"
    );
}

/// PRECEDENCE: when a group key is BOTH projected and carries a `dataType` on
/// its `groupBy` node, the `selectListDataTypes` entry wins. Exasol validates
/// the outer wrapper SELECT positionally against `selectListDataTypes`, so a
/// `groupBy`-derived type that disagreed would make the outer
/// `CAST("GK_0" AS …)` contradict the column type Exasol is checking.
#[test]
fn group_key_exasol_types_prefers_select_list_type_over_group_by_type() {
    let req = make_group_by_request_with_types(
        serde_json::json!([char_cast_key(20, "UTF8")]),
        serde_json::json!([char_cast_key(20, "UTF8"), agg_item("COUNT", None, false),]),
        serde_json::json!([{"type": "varchar", "size": 30}, decimal_type(18, 0)]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let types = group_key_exasol_types(&req, &detection.group_keys, &detection.select_items);

    assert_eq!(
        types,
        vec!["VARCHAR(30)".to_string()],
        "the selectListDataTypes entry must win over the groupBy node's own dataType: \
             {types:?}"
    );
}

/// A bare string-literal select item (`'X'`, the #192 constant-projection
/// shape — `SELECT 'X' G, COUNT(*) ... GROUP BY 1`) declared `CHAR(1) ASCII`
/// must render `CAST('X' AS CHAR(1) ASCII)` through `constant_projection_sql`,
/// driven through the real `detect_group_by_aggregates` entry point (facet C).
/// Exasol declares a bare string literal `CHAR(1) ASCII` (live-verified); the
/// current catch-all renders `VARCHAR(1) ASCII` instead.
#[test]
fn constant_projection_casts_literal_to_char() {
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "REGION"}]),
        serde_json::json!([
            {"type": "literal_string", "value": "X"},
            agg_item("COUNT", None, false),
        ]),
        serde_json::json!([
            {"type": "CHAR", "size": 1, "characterSet": "ASCII"},
            decimal_type(18, 0),
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let projection = detection
        .select_items
        .iter()
        .find_map(|item| match item {
            GroupedSelectItem::Constant { projection, .. } => Some(projection.clone()),
            _ => None,
        })
        .expect("the literal_string item must classify as Constant");

    assert_eq!(
        projection, "CAST('X' AS CHAR(1) ASCII)",
        "a CHAR(1)-declared literal must cast to CHAR(1) ASCII, not VARCHAR(1) ASCII: \
             {projection}"
    );
}

/// `MIN(CAST(<col> AS CHAR(20)))` (an expression-argument aggregate — no source
/// `column`, so its partial/merge type comes solely from its own declared
/// `selectListDataTypes` entry) must declare its partial EMITS column
/// `"PARTIAL_min_0" CHAR(20)` and cast its outer merge item to `CHAR(20)` — not
/// VARCHAR(20) — driven through the real `detect_group_by_aggregates` entry
/// point rather than hand-built `AggregatePlan`s.
#[test]
fn min_over_char_expression_declares_char_partial_and_merge_cast() {
    let cast_arg = serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "arguments": [{"type": "column", "name": "C_VARCHAR"}],
        "dataType": {"type": "CHAR", "size": 20, "characterSet": "UTF8"}
    });
    let req = make_group_by_request_with_types(
        serde_json::json!([{"type": "column", "name": "REGION"}]),
        serde_json::json!([
            {"type": "column", "name": "REGION"},
            agg_item_expr("MIN", cast_arg, false),
        ]),
        serde_json::json!([
            {"type": "varchar", "size": 100},
            {"type": "CHAR", "size": 20, "characterSet": "UTF8"},
        ]),
    );
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.plans.len(), 1, "one aggregate plan (MIN)");
    assert_eq!(
        detection.plan_types,
        vec!["CHAR(20)".to_string()],
        "the MIN plan's declared type must be CHAR(20), not VARCHAR(20): {:?}",
        detection.plan_types
    );

    let partial_emits = partial_emits_items(&detection.plans, &[], &detection.plan_types);
    assert_eq!(
        partial_emits,
        vec![r#""PARTIAL_min_0" CHAR(20)"#.to_string()],
        "MIN over a CHAR-declared expression must declare its partial column \
             CHAR(20), not VARCHAR(20): {partial_emits:?}"
    );

    let merge_items = cast_merge_items(&detection.plans, &detection.plan_types);
    assert_eq!(
        merge_items,
        vec![r#"CAST(MIN("PARTIAL_min_0") AS CHAR(20))"#.to_string()],
        "the merge item must cast to CHAR(20), not VARCHAR(20): {merge_items:?}"
    );
}

/// aggregationType missing or not "group_by" returns None.
#[test]
fn detect_group_by_aggregates_no_group_by_type_returns_none() {
    // No aggregationType.
    let req1 = serde_json::json!({
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [agg_item("COUNT", None, false)],
    });
    assert!(detect_group_by_aggregates(&req1).is_none());

    // aggregationType is "single_group".
    let req2 = serde_json::json!({
        "aggregationType": "single_group",
        "selectList": [agg_item("COUNT", None, false)],
    });
    assert!(detect_group_by_aggregates(&req2).is_none());
}

/// Empty groupBy array returns None.
#[test]
fn detect_group_by_aggregates_empty_group_by_returns_none() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [],
        "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
    });
    assert!(detect_group_by_aggregates(&req).is_none());
}

/// partial_emits_items produces 3 columns for stat aggregates.
#[test]
fn stat_aggregate_emits_three_partial_columns() {
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
        let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];
        let items = partial_emits_items(&plans, &col_types, &[]);
        assert_eq!(
            items.len(),
            3,
            "{kind:?} must emit 3 partial columns, got: {items:?}"
        );
        assert!(
            items[0].contains("PARTIAL_stat_cnt_0"),
            "first column must be cnt: {items:?}"
        );
        assert!(
            items[1].contains("PARTIAL_stat_sum_0"),
            "second column must be sum: {items:?}"
        );
        assert!(
            items[2].contains("PARTIAL_stat_sumsq_0"),
            "third column must be sumsq: {items:?}"
        );
    }
}

/// The scan's partial SELECT list and the adapter's `EMITS` clause name the
/// same partial columns, in the same order, for every `AggKind`.
///
/// The two lists are built in different modules and are otherwise only
/// validated against each other at query time inside Exasol, where a
/// mismatch surfaces as a wrong value or an `EMITS` arity error rather than
/// as a test failure. The variant list below is an explicit literal: a
/// variant added later that it omits is caught by the compile error
/// `AggKind::partial_columns` raises, not here, so this test asserts
/// alignment and never doubles as an exhaustiveness check it cannot enforce.
#[test]
fn scan_select_list_and_emits_agree_per_agg_kind() {
    /// Every `PARTIAL_…` name in `text`, in order of appearance. Both sides
    /// terminate the name with a double quote — the scan as
    /// `AS "PARTIAL_…"`, the `EMITS` item as `"PARTIAL_…" <type>`.
    fn partial_names_in(text: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut rest = text;
        while let Some(start) = rest.find("PARTIAL_") {
            let tail = &rest[start..];
            let end = tail
                .find('"')
                .expect("a PARTIAL_ name is always double-quote terminated");
            names.push(tail[..end].to_string());
            rest = &tail[end..];
        }
        names
    }

    let all_kinds = [
        AggKind::Count,
        AggKind::CountCol,
        AggKind::Sum,
        AggKind::Min,
        AggKind::Max,
        AggKind::Avg,
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::StddevPop,
        AggKind::StddevSamp,
    ];
    let col_types = vec![("SCORE".to_string(), "DOUBLE PRECISION".to_string())];

    let plan_for = |kind: &AggKind| AggregatePlan {
        kind: kind.clone(),
        column: match kind {
            AggKind::Count => None,
            _ => Some("SCORE".to_string()),
        },
        arg_expr: None,
    };

    for kind in &all_kinds {
        let plans = vec![plan_for(kind)];
        let scan_names = partial_names_in(&crate::scan::build_partial_agg_sql(&plans, "aliased"));
        let emits_names =
            partial_names_in(&partial_emits_items(&plans, &col_types, &[]).join(", "));
        assert_eq!(
            scan_names, emits_names,
            "{kind:?}: scan SELECT list and EMITS clause disagree"
        );
    }

    // The same agreement under mixed arities, where a plan ordinal and a
    // column ordinal diverge — the shape a per-kind check cannot reach.
    let mixed: Vec<AggregatePlan> = all_kinds.iter().map(plan_for).collect();
    assert_eq!(
        partial_names_in(&crate::scan::build_partial_agg_sql(&mixed, "aliased")),
        partial_names_in(&partial_emits_items(&mixed, &col_types, &[]).join(", ")),
        "mixed-arity plan list: scan SELECT list and EMITS clause disagree"
    );
}

/// merge_select_items produces the correct reconstruction SQL for VAR_POP.
#[test]
fn var_pop_merge_formula_divides_by_n() {
    let plans = vec![AggregatePlan {
        kind: AggKind::VarPop,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    // Must contain NULLIF(..., 0) guard on the count
    assert!(
        sql.contains("NULLIF"),
        "var_pop merge must guard zero count: {sql}"
    );
    // Must NOT divide by (count - 1)
    assert!(
        !sql.contains("- 1"),
        "var_pop must not subtract 1 from count: {sql}"
    );
}

/// merge_select_items for VAR_SAMP divides by N-1 and guards N<=1 → NULL.
#[test]
fn var_samp_merge_formula_divides_by_n_minus_1() {
    let plans = vec![AggregatePlan {
        kind: AggKind::VarSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    // Must use CASE WHEN … <= 1 THEN NULL to guard count<=1 → NULL.
    // Checking both `<= 1` and `CASE` ensures the N-1 sample divisor guard
    // is specifically present — not just any CASE or NULLIF in the expression.
    assert!(
        sql.contains("<= 1"),
        "var_samp merge must guard count<=1 with '<= 1': {sql}"
    );
    assert!(
        sql.contains("CASE"),
        "var_samp merge must use CASE for N<=1 guard: {sql}"
    );
}

/// STDDEV_POP merge formula wraps variance in SQRT.
#[test]
fn stddev_pop_merge_formula_uses_sqrt() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevPop,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(sql.contains("SQRT("), "stddev_pop must use SQRT: {sql}");
    assert!(
        !sql.contains("- 1"),
        "stddev_pop must not subtract 1: {sql}"
    );
}

/// STDDEV_SAMP merge formula wraps variance-samp in SQRT.
#[test]
fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(sql.contains("SQRT("), "stddev_samp must use SQRT: {sql}");
    // N-1 guard: removing the N<=1 CASE would break this assertion.
    assert!(
        sql.contains("<= 1"),
        "stddev_samp must guard N<=1 (sample divisor): {sql}"
    );
    assert!(
        sql.contains("CASE"),
        "stddev_samp must use CASE for N<=1 guard: {sql}"
    );
}

/// StddevPop merge SQL passes NULL through (N=0 → var_pop is NULL → stddev_pop NULL).
///
/// Exasol's GREATEST returns NULL if any argument is NULL, so a bare
/// SQRT(GREATEST(...)) already yields NULL when cnt=0. The CASE WHEN IS NULL
/// THEN NULL guard is redundant under that contract but retained for
/// pinned golden-fixture SQL and explicitness at the merge site.
#[test]
fn stddev_pop_merge_null_passthrough_for_n_zero() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevPop,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    // Must contain a NULL guard (CASE … IS NULL) that wraps the whole expression.
    assert!(
        sql.contains("IS NULL"),
        "stddev_pop must pass NULL through for N=0 via IS NULL guard: {sql}"
    );
    // The GREATEST guard against tiny-negative float rounding must still be present.
    assert!(
        sql.contains("GREATEST"),
        "stddev_pop must keep GREATEST rounding guard: {sql}"
    );
}

/// StddevSamp merge SQL passes NULL through for N=0 and N=1.
///
/// var_samp is NULL when cnt<=1 (CASE guard). Exasol's GREATEST returns NULL
/// if any argument is NULL, so SQRT already receives NULL there; the CASE
/// WHEN IS NULL wrapper is redundant under that contract but retained for
/// pinned golden-fixture SQL and explicitness at the merge site.
#[test]
fn stddev_samp_merge_null_passthrough_for_n_zero_and_n_one() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    // Must contain a NULL guard that wraps the whole expression.
    assert!(
        sql.contains("IS NULL"),
        "stddev_samp must pass NULL through for N<=1 via IS NULL guard: {sql}"
    );
    // The GREATEST guard against tiny-negative float rounding must still be present.
    assert!(
        sql.contains("GREATEST"),
        "stddev_samp must keep GREATEST rounding guard: {sql}"
    );
}

/// HAVING is rendered and appears in the outer GROUP BY wrapper SQL.
#[test]
fn having_clause_appears_in_outer_wrapper_only() {
    // Build a grouped aggregate SQL with a HAVING predicate.
    let having_filter = Some(r#"(SUM("AMOUNT") > 100)"#.to_string());
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            projection: vec!["REGION".into(), "AMOUNT".into()],
            aggregates: Some(vec![AggregatePlan {
                kind: AggKind::Sum,
                column: Some("AMOUNT".into()),
                arg_expr: None,
            }]),
            group_keys: Some(vec![r#""REGION""#.to_string()]),
            storage: sample_storage(),
            ..Default::default()
        },
        files: vec![],
    };
    let shards = vec![vec![("s3://wh/f.parquet".to_string(), 1u64)]];
    let col_types = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("AMOUNT".to_string(), "DOUBLE PRECISION".to_string()),
    ];
    let sql = build_grouped_aggregate_scan_sql(
        &spec_template,
        &shards,
        &[r#""REGION""#.to_string()],
        &[],
        &[AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }],
        &[],
        &keys_first_select_items(1, 1),
        None,
        0,
        &col_types,
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        having_filter.as_deref(),
        None,
    );
    // HAVING must appear in the outer wrapper (after GROUP BY)
    assert!(
        sql.contains("HAVING"),
        "outer wrapper must contain HAVING: {sql}"
    );
    assert!(
        sql.contains("100"),
        "HAVING predicate value must be in SQL: {sql}"
    );
    // HAVING must come after GROUP BY
    let having_pos = sql.find("HAVING").unwrap();
    let group_by_pos = sql.find("GROUP BY").unwrap();
    assert!(
        having_pos > group_by_pos,
        "HAVING must appear after GROUP BY: {sql}"
    );
}

// -----------------------------------------------------------------------
// CHAR-declared group-key blank padding (issue #192, facet A)
// -----------------------------------------------------------------------

/// A value wider than the `CHAR(20)` group keys are padded to. 25 characters,
/// matching the over-length row of the `char_pad_probe` seed table.
const OVER_LENGTH_VALUE: &str = "over-length-value-abcdefg";

/// The pad shape this fix commits to, spelled out independently of the
/// production formatter so a silent change of construct fails the test.
fn expected_pad(fragment: &str, width: u32) -> String {
    format!(
        "CASE WHEN character_length({fragment}) < {width} \
             THEN rpad({fragment}, {width}) ELSE {fragment} END"
    )
}

/// A `CHAR(20)`-declared group key must reach the DataFusion side wrapped in
/// the blank pad, so two values differing only in trailing blanks emit the
/// SAME `GK_0` staging value and the outer merge collapses them into one
/// group — exactly as Exasol's own `CAST(x AS CHAR(20))` does natively.
#[test]
fn char_declared_group_key_is_blank_padded_to_its_declared_width() {
    let fragment = r#"CAST("NAME" AS VARCHAR)"#.to_string();

    let padded =
        blank_pad_char_group_keys(std::slice::from_ref(&fragment), &["CHAR(20)".to_string()]);

    assert_eq!(
        padded,
        vec![expected_pad(&fragment, 20)],
        "a CHAR(20)-declared group key must be blank-padded to 20 characters on the \
             DataFusion side: {padded:?}"
    );
}

/// The width must be read from between the parentheses, NOT by trimming a
/// trailing `)` off the declared type: Exasol declares the #192 primary shape
/// (an equal-length CASE) `CHAR(3) ASCII`, and a suffix-intolerant parse would
/// silently skip padding on every ASCII-declared CHAR key.
#[test]
fn ascii_suffixed_char_group_key_width_is_parsed_before_the_suffix() {
    let fragment = "CASE WHEN \"C_DECIMAL_A\" < 0 THEN 'NEG' ELSE 'POS' END".to_string();

    let padded = blank_pad_char_group_keys(
        std::slice::from_ref(&fragment),
        &["CHAR(3) ASCII".to_string()],
    );

    assert_eq!(
        padded,
        vec![expected_pad(&fragment, 3)],
        "a `CHAR(3) ASCII` group key must be padded to 3, not left unpadded because of \
             the character-set suffix: {padded:?}"
    );
}

/// The pad must be guarded by a length test rather than applied as a bare
/// `rpad(x, n)`: `rpad` TRUNCATES an over-length value, which would merge a
/// too-wide key into a wrong group and return rows where Exasol raises 22001.
/// The `ELSE` branch must therefore hand the value on byte-identical.
#[test]
fn char_pad_leaves_an_over_length_value_unmodified() {
    let fragment = r#""NAME""#.to_string();

    let padded =
        blank_pad_char_group_keys(std::slice::from_ref(&fragment), &["CHAR(20)".to_string()])
            .pop()
            .expect("one padded group key");

    assert!(
        padded.starts_with(&format!("CASE WHEN character_length({fragment}) < 20 THEN")),
        "the pad must be guarded by a shorter-than-width test, never unconditional: {padded}"
    );
    assert!(
        padded.ends_with(&format!("ELSE {fragment} END")),
        "an over-length value must pass through the ELSE branch unmodified: {padded}"
    );
    assert_eq!(
        padded.matches("rpad(").count(),
        1,
        "rpad must appear exactly once, inside the guarded THEN branch: {padded}"
    );
    for truncating in ["substr", "substring", "left("] {
        assert!(
            !padded.contains(truncating),
            "the pad must contain no truncating construct ({truncating}): {padded}"
        );
    }
}

/// CONTROL: a VARCHAR-declared group key must be handed on untouched. Also
/// guards the prefix match — `VARCHAR(10)` contains `CHAR(` and would be
/// wrongly padded by a substring test instead of a prefix test.
#[test]
fn varchar_declared_group_key_is_left_unpadded() {
    let keys = vec![r#""REGION""#.to_string()];

    let padded = blank_pad_char_group_keys(&keys, &["VARCHAR(10)".to_string()]);

    assert_eq!(
        padded, keys,
        "a VARCHAR-declared group key must be left unpadded: {padded:?}"
    );
}

/// Padding is decided per group-key slot: a mixed VARCHAR + CHAR multi-key
/// GROUP BY must pad only the CHAR slot, and at that slot's own width.
#[test]
fn multi_key_pad_applies_only_to_the_char_slot() {
    let keys = vec![
        r#""REGION""#.to_string(),
        r#"CAST("NAME" AS VARCHAR)"#.to_string(),
    ];

    let padded = blank_pad_char_group_keys(
        &keys,
        &["VARCHAR(10)".to_string(), "CHAR(5) ASCII".to_string()],
    );

    assert_eq!(
        padded,
        vec![keys[0].clone(), expected_pad(&keys[1], 5)],
        "only the CHAR-declared slot may be padded, at its own width: {padded:?}"
    );
}

/// The padded fragment is spliced into three positions of one DataFusion SQL
/// statement, so it must PLAN and EVALUATE there — not merely look right.
/// Proves the whole #192 facet-A contract on the real engine: trailing-blank
/// variants merge into one group, an over-length value survives at full width
/// (so the outer Exasol cast can still raise 22001), and a NULL key stays NULL.
#[tokio::test]
async fn padded_group_key_merges_trailing_blank_variants_without_truncating() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("V", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            Some("ab"),
            Some("ab   "),
            Some("cd"),
            Some(OVER_LENGTH_VALUE),
            None,
        ]))],
    )
    .expect("fixture batch");
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)
        .expect("fixture table registers");

    let padded = blank_pad_char_group_keys(&[r#""V""#.to_string()], &["CHAR(20)".to_string()])
        .pop()
        .expect("one padded group key");
    // Same shape `build_grouped_partial_agg_sql` emits: the identical fragment
    // in the SELECT list and in the GROUP BY.
    let sql = format!(r#"SELECT {padded}, COUNT(*) FROM (SELECT "V" FROM t) GROUP BY {padded}"#);

    let batches = ctx
        .sql(&sql)
        .await
        .expect("the padded group key must plan in DataFusion")
        .collect()
        .await
        .expect("the padded group key must evaluate in DataFusion");

    let mut groups: Vec<(Option<String>, i64)> = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("group key column is Utf8");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column is Int64");
        for row in 0..batch.num_rows() {
            let key = if keys.is_null(row) {
                None
            } else {
                Some(keys.value(row).to_string())
            };
            groups.push((key, counts.value(row)));
        }
    }
    groups.sort();

    assert_eq!(
        groups.len(),
        4,
        "'ab' and 'ab   ' must merge into ONE group, leaving 4 groups: {groups:?}"
    );
    let merged = format!("{:<20}", "ab");
    assert_eq!(
        groups
            .iter()
            .find(|(k, _)| k.as_deref() == Some(merged.as_str()))
            .map(|(_, c)| *c),
        Some(2),
        "'ab' and 'ab   ' must both pad to {merged:?} and count 2: {groups:?}"
    );
    assert_eq!(
        groups
            .iter()
            .find(|(k, _)| k.as_deref() == Some(OVER_LENGTH_VALUE))
            .map(|(_, c)| *c),
        Some(1),
        "the {} -character value must survive unmodified, never truncated to 20: {groups:?}",
        OVER_LENGTH_VALUE.len()
    );
    assert_eq!(
        groups.iter().find(|(k, _)| k.is_none()).map(|(_, c)| *c),
        Some(1),
        "a NULL group key must stay NULL through the pad: {groups:?}"
    );
}

/// The fragment can itself be a `CASE` expression (the #192 primary shape), so
/// the triple splice nests a `CASE` inside `character_length(...)`, inside
/// `rpad(...)`, and — the one that could plausibly not parse — directly after
/// the outer `ELSE`. Prove DataFusion parses and evaluates that nesting.
#[tokio::test]
async fn padded_case_fragment_plans_and_evaluates_in_datafusion() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("V", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![Some("ab"), Some("cd")]))],
    )
    .expect("fixture batch");
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)
        .expect("fixture table registers");

    let case_fragment = r#"CASE WHEN "V" = 'ab' THEN 'NEG' ELSE 'POS' END"#.to_string();
    let padded = blank_pad_char_group_keys(&[case_fragment], &["CHAR(3) ASCII".to_string()])
        .pop()
        .expect("one padded group key");
    let sql = format!(r#"SELECT {padded}, COUNT(*) FROM (SELECT "V" FROM t) GROUP BY {padded}"#);

    let batches = ctx
        .sql(&sql)
        .await
        .expect("a padded CASE fragment must plan in DataFusion")
        .collect()
        .await
        .expect("a padded CASE fragment must evaluate in DataFusion");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 2,
        "the two equal-length CASE results must stay two groups: {batches:?}"
    );
}

/// Grouped path: a `function_aggregate` select item whose statistical aggregate
/// takes an expression argument declines the WHOLE grouped detection, so the
/// request routes to the Tier 1b qualified single-table wrapper and Exasol
/// computes the statistic over its rows.
///
/// Measured 2026-07-31 against the Docker Exasol container: `SELECT MOD(id, 4),
/// STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` is PUSHED by
/// Exasol and fails with `sqlCode 22002`, `grouped partial aggregate SQL error:
/// Schema error: No field named .`
#[test]
fn grouped_stat_aggregate_over_expression_argument_declines() {
    let req = make_group_by_request_with_types(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([
            mod_item("ID", 4),
            agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
        ]),
        serde_json::json!([decimal_type(9, 0), {"type": "double"}]),
    );
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "a grouped STDDEV over an expression argument must decline the grouped \
             partial/merge path"
    );
}

/// Grouped scalar-over-aggregate path: a select item WRAPPING a statistical
/// aggregate over an expression argument (`SQRT(STDDEV(<expr>))`) does not
/// classify, which declines the whole grouped detection and routes the request
/// to the qualified single-table wrapper.
///
/// Measured 2026-07-31: `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM
/// MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` is PUSHED by Exasol as a
/// scalar-over-aggregate — the merge wrapper renders — and fails with `sqlCode
/// 22002`, `grouped partial aggregate SQL error: Schema error: No field named .`
#[test]
fn scalar_over_stat_aggregate_with_expression_argument_declines() {
    let sqrt_over_stat = serde_json::json!({
        "type": "function_scalar",
        "name": "SQRT",
        "arguments": [agg_item_expr("STDDEV", mod_item("SCORE", 4), false)],
    });
    assert!(
        classify_scalar_over_aggregate(&sqrt_over_stat).is_none(),
        "a scalar wrapping a stat aggregate over an expression must not classify"
    );

    let req = make_group_by_request_with_types(
        serde_json::json!([mod_item("ID", 4)]),
        serde_json::json!([mod_item("ID", 4), sqrt_over_stat]),
        serde_json::json!([decimal_type(9, 0), {"type": "double"}]),
    );
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "an unclassifiable scalar-over-aggregate item must decline the whole \
             grouped detection"
    );
}

/// HAVING path: a HAVING comparing a statistical aggregate over an expression
/// argument does not render over the merge wrapper, so `classify_request_shape`
/// routes the request to the qualified single-table wrapper rather than emit a
/// HAVING over a partial column no scan produces.
///
/// The `plans` slot here is the shape the pre-decline parse produced — a
/// statistical kind carrying neither a source column nor a rendered argument.
/// Matching that slot is what the decline now prevents; the realistic route,
/// where the select list carries the same shape, is declined earlier by
/// `grouped_stat_aggregate_over_expression_argument_declines`.
#[test]
fn having_over_stat_aggregate_with_expression_argument_declines() {
    let having = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
        "right": {"type": "literal_double", "value": 5.0},
    });
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevSamp,
        column: None,
        arg_expr: None,
    }];
    assert!(
        render_having_over_merge(&having, &plans).is_none(),
        "a HAVING over a stat aggregate with an expression argument must not \
             render over the merge wrapper"
    );
}
