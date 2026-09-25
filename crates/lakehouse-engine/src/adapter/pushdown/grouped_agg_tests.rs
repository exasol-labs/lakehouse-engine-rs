use super::super::detect_aggregates;
use super::super::joins::{
    FanOutProjection, build_qualified_single_table_fallback_sql, referenced_column_projection,
};
use super::super::support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, extract_all_column_types, shard_count,
};
use super::super::test_support::*;
use super::*;
use crate::scan::spec::{CommonScanSpec, ScanStorage};

/// Scenario: An absent scale defaults to `0`, so `DECIMAL(10)` widens to `DECIMAL(36,0)`.
#[test]
fn sum_emit_type_absent_scale_widens_to_a_scale_zero_decimal() {
    assert_eq!(sum_emit_type("DECIMAL(10)"), "DECIMAL(36,0)");
}

/// Scenario: A non-canonical scale text re-emerges canonically or falls back, never echoed raw.
#[test]
fn sum_emit_type_never_echoes_a_non_canonical_scale_text() {
    // `None` = the parser rejects the text, so the answer is the numeric fallback.
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

/// Scenario: Every precision `parse_decimal_args` rejects declines to the numeric fallback.
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

/// Scenario: A grouped merge CAST to CHAR renders the length-qualified `CHAR(20) ASCII` Exasol parses (#192).
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

/// Scenario: A nested CAST to CHAR renders `CHAR(20) ASCII` at both levels (#192).
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

/// Scenario: A GROUP BY request carrying COUNT(DISTINCT) declines to row scanning.
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
    assert!(
        detect_aggregates(&req).is_none(),
        "the single-group path rejects any request carrying a non-empty GROUP BY"
    );
}

/// Scenario: MIN/MAX over a DATE column emits DATE, not DOUBLE PRECISION.
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

/// Scenario: SUM over a DECIMAL(20,0) column emits DECIMAL(36,0), not DOUBLE.
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
    assert!(
        emits[0].contains("DECIMAL(36,0)"),
        "SUM over DECIMAL(20,0) must widen to DECIMAL(36,0): {:?}",
        emits[0]
    );
}

/// Scenario: SUM over a DOUBLE PRECISION column stays DOUBLE PRECISION.
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

/// Scenario: SUM over a VARCHAR or DATE column fails `validate_agg_col_types`.
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

/// Scenario: A grouped SUM over VARCHAR falls back to row scan instead of an opaque UDF error.
#[test]
fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [
            {"type": "column", "name": "REGION"},
            agg_item("SUM", Some("NAME"), false), // NAME is VARCHAR — invalid for SUM
        ],
    });

    let detected = detect_group_by_aggregates(&req);
    assert!(
        detected.is_some(),
        "detect_group_by_aggregates must accept the shape: {req}"
    );
    let agg_plans = detected.unwrap().plans;

    let col_types = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "VARCHAR(2000000)".to_string()),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &col_types),
        "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
    );

    let col_types_date = vec![
        ("REGION".to_string(), "VARCHAR(2000000)".to_string()),
        ("NAME".to_string(), "DATE".to_string()),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &col_types_date),
        "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
    );

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

/// Its own `dataType` is the only place the key's type appears when the key is not
/// also in the select list.
fn char_cast_key(size: u64, character_set: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "CHAR", "size": size, "characterSet": character_set},
        "arguments": [{"type": "column", "name": "NAME"}],
    })
}

fn varchar_cast_key(size: u64) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar_cast",
        "name": "CAST",
        "dataType": {"type": "VARCHAR", "size": size},
        "arguments": [{"type": "column", "name": "NAME"}],
    })
}

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

fn upper_item(col: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "function_scalar",
        "name": "UPPER",
        "arguments": [
            {"type": "column", "name": col},
        ],
    })
}

fn decimal_type(precision: u64, scale: u64) -> serde_json::Value {
    serde_json::json!({"type": "decimal", "precision": precision, "scale": scale})
}

/// Scenario: A column reference in GROUP BY renders to a quoted identifier.
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

fn grouped_spec(result: &GroupedAggregateDetection) -> ScanSpec {
    ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    }
}

/// Scenario: A grouped ORDER BY without LIMIT renders a positional final ORDER BY, sorting typed output.
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
    req["orderBy"] = serde_json::json!([{
        "type": "order_by_element",
        "expression": {"type": "column", "name": "ID"},
        "isAscending": true,
        "nullsLast": true,
    }]);

    let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
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
    assert!(!sql.contains("LIMIT"), "no LIMIT requested: {sql}");
}

/// Scenario: An ORDER BY on a detected aggregate resolves to its merged expression (#198).
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

/// Scenario: An ORDER BY on an undetected aggregate is `Unresolvable`, routing to a wrapper (#198).
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

/// Scenario: A scalar expression in GROUP BY renders via `render_expression`.
#[test]
fn detect_group_by_aggregates_expression_key() {
    let req = make_group_by_request(
        serde_json::json!([{
            "type": "predicate_equal",
            "left": {"type": "column", "name": "STATUS"},
            "right": {"type": "literal_string", "value": "active"},
        }]),
        serde_json::json!([agg_item("SUM", Some("AMOUNT"), false),]),
    );
    let result = detect_group_by_aggregates(&req);
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

/// Scenario: An unsupported GROUP BY expression makes detection return None.
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

/// Scenario: A non-aggregate, non-column select-list item causes fallback.
#[test]
fn detect_group_by_mixed_select_falls_back() {
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

/// Scenario: A `literal_null`-only grouped select list keeps its GROUP BY, never a row-scan fallback (#52).
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

    // A row-scan fallback would return one row per source row, not per group.
    let group_key_types = group_key_exasol_types(&req, &result.group_keys, &result.select_items);
    let sql = build_grouped_aggregate_scan_sql(
        &ScanSpec {
            common: CommonScanSpec {
                aggregates: Some(result.plans.clone()),
                group_keys: Some(result.group_keys.clone()),
                storage: ScanStorage::Inline(sample_storage()),
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
    assert!(
        sql.contains("SELECT CAST(NULL AS BOOLEAN) FROM"),
        "outer wrapper must project the type-cast constant placeholder: {sql}"
    );
}

/// Scenario: A `literal_bool` placeholder in a grouped select list does not abort detection (#52).
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

/// Scenario: An aggregate before the group key keeps its original select-list ordinal (#33).
#[test]
fn detect_group_by_aggregates_preserves_select_list_order() {
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

/// Scenario: Interleaved multi-key GROUP BY items keep their own ordinals and key slots.
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

/// Scenario: An expression group key after an aggregate keeps its original ordinal.
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

/// Scenario: HAVING does not change aggregate-first select-list classification.
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

/// Scenario: All-expression multi-key GROUP BY renders each key; one untranslatable key declines all.
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

    let col_types: Vec<(String, String)> = vec![];
    let group_key_types = vec!["VARCHAR(2000000)".to_string(); 2];
    let aggregate_types = vec!["DECIMAL(18,0)".to_string()];
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            storage: ScanStorage::Inline(sample_storage()),
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: Grouped SQL fans out via GROUP BY shard_key, one common blob and one files literal per shard.
#[test]
fn grouped_fan_out_common_once_files_per_shard() {
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

    for file in ["f0.parquet", "f1.parquet"] {
        assert_eq!(
            sql.matches(file).count(),
            1,
            "grouped shard file {file} must appear exactly once: {sql}"
        );
    }
}

/// Scenario: The shard_key GROUP BY sits inside the distributor; the outer wrapper re-groups on GK_*.
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

    assert!(
        sql.contains("AS shards(shard_key, files) GROUP BY shard_key"),
        "the shard_key fan-out must live in the distributor subquery: {sql}"
    );
    assert!(
        sql.trim_end().ends_with(r#"GROUP BY "GK_0""#),
        "the outer wrapper must re-group on the user group key GK_0: {sql}"
    );
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
    assert!(
        !sql.contains("SELECT * FROM ("),
        "grouped wrapper must not use a SELECT * materialization boundary: {sql}"
    );
}

/// Scenario: Single-shard grouped SQL re-groups over a from-less scan, with no distributor.
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

/// Scenario: A grouped LIMIT applies only on the outer wrapper, never in the shard blob.
#[test]
fn grouped_common_blob_has_no_limit() {
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
            storage: ScanStorage::Inline(sample_storage()),
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
    let common = common_arg_literal(&sql);
    assert!(
        !common.contains("\"limit\""),
        "grouped common blob must NOT carry limit: {common}"
    );
    assert!(
        sql.contains("LIMIT 100"),
        "outer wrapper should still apply the final LIMIT: {sql}"
    );
}

/// Scenario: A grouped offset renders only on the outer wrapper, never in the shared blob.
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: A zero offset renders exactly ` LIMIT {n}` with no OFFSET token (#191).
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: The grouped merge renders `GROUP BY … ORDER BY … LIMIT n OFFSET m` in that order (#191).
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

/// Scenario: The grouped wrapper re-groups partial results per user group key.
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
    assert!(
        sql.contains("GK_0"),
        "wrapper SQL must reference GK_0: {sql}"
    );
    assert!(
        sql.contains("GK_1"),
        "wrapper SQL must reference GK_1: {sql}"
    );
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

/// A paren-depth-aware split suffices: the merge and CAST shapes used here carry no
/// top-level `, ` inside quotes.
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: The outer SELECT follows select-list order, merged SUM before the cast key (#33).
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

/// Scenario: Interleaved multi-key outer SELECT order is [key0, aggregate, key1].
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

/// Scenario: An expression key after an aggregate keeps its declared DECIMAL type, not VARCHAR (#33).
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

/// Scenario: Aggregate-first GROUP BY with HAVING keeps select-list order and appends HAVING after GROUP BY.
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

/// Mirrors the production grouped branch of `handle_pushdown`.
fn build_grouped_from_detection(req: &serde_json::Value) -> String {
    let d = detect_group_by_aggregates(req)
        .expect("must detect the grouped scalar-over-aggregate pushdown");
    let group_key_types = group_key_exasol_types(req, &d.group_keys, &d.select_items);
    let spec_template = ScanSpec {
        common: CommonScanSpec {
            aggregates: Some(d.plans.clone()),
            group_keys: Some(d.group_keys.clone()),
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: A scalar-over-aggregate folds its inner aggregates into the plan list, deduping COUNT(*) (#82).
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

/// Scenario: The wrapper renders a scalar-over-aggregate over merged partials with no source column.
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
    assert!(
        !soa.contains("CASE"),
        "the CASE must be folded into a PARTIAL_* column: {soa}"
    );
    assert!(
        !soa.contains("L_RETURNFLAG") && !soa.contains("L_QUANTITY"),
        "wrapper item must not reference any source column: {soa}"
    );
}

/// Scenario: A scalar-over-aggregate before the key keeps select-list order and its declared cast.
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

/// Scenario: A scalar over COUNT(DISTINCT) routes to the qualified wrapper, not a bare row scan.
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
    let (proj_cols, proj_types) = referenced_column_projection(&pushdown_req, &all_cols);
    let fan_out_spec = ScanSpec {
        common: CommonScanSpec {
            projection: proj_cols,
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
    let fan_out = FanOutProjection::new(&fan_out_spec, &proj_types).expect("aligned fan-out");
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown_req,
        &fan_out,
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
    // The first ` FROM (` is the outer wrapper's; the fan-out subquery's comes later.
    let items = outer_select_items(&sql);
    assert_eq!(
        items.len(),
        2,
        "the wrapper must return exactly the selectList columns, not a full row: {items:?}"
    );
}

/// Scenario: A HAVING aggregate renders against the merged partial, not the source column (#33).
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

/// Scenario: The #33 HAVING wrapper carries the merged HAVING and no source `SCORE` reference.
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: A HAVING on an unplanned aggregate returns None so the request routes to a wrapper.
#[test]
fn render_having_over_merge_declines_unknown_aggregate() {
    let having = serde_json::json!({
        "type": "predicate_greater",
        "left": agg_item("COUNT", None, false),
        "right": {"type": "literal_exactnumeric", "value": 10},
    });
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

/// Scenario: Detection output feeds the SQL builder and the wrapper follows select-list order (#33).
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
            storage: ScanStorage::Inline(sample_storage()),
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

/// Scenario: Multi-key HAVING and LIMIT render only in the outer wrapper, never per shard.
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
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        },
        files: vec![],
    };
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

/// Scenario: An expression key resolves its declared type by index, not by rendered-string match.
#[test]
fn group_key_type_resolved_by_index_not_string_match() {
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

    let group_keys = vec![r#"("id" % 4)"#.to_string()];
    let select_items = detection.select_items.clone();
    let types = group_key_exasol_types(&req, &group_keys, &select_items);

    assert_eq!(
        types,
        vec!["DECIMAL(9,0)".to_string()],
        "type must resolve via select_index, not via string-matching the (drifted) \
             rendered group key: {types:?}"
    );
}

/// Scenario: Mixed-type multi-key GROUP BY resolves each key's own declared type.
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

/// Scenario: An equal-length CASE group key resolves to `CHAR(3) ASCII`, as Exasol declares it (#192).
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

/// Scenario: A VARCHAR-declared group key keeps resolving to `VARCHAR(10)`.
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

/// Scenario: An unprojected group key reads its declared CHAR type from its `groupBy` node (#192).
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

/// Scenario: An unprojected VARCHAR group key resolves VARCHAR, never a padded CHAR width.
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

/// Scenario: A projected key's `selectListDataTypes` entry wins over its `groupBy` type.
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

/// Scenario: A bare string-literal select item renders `CAST('X' AS CHAR(1) ASCII)` (#192).
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

/// Scenario: `MIN(CAST(<col> AS CHAR(20)))` emits and merges as `CHAR(20)`, not VARCHAR.
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

/// Scenario: A missing or non-"group_by" aggregationType returns None.
#[test]
fn detect_group_by_aggregates_no_group_by_type_returns_none() {
    let req1 = serde_json::json!({
        "groupBy": [{"type": "column", "name": "REGION"}],
        "selectList": [agg_item("COUNT", None, false)],
    });
    assert!(detect_group_by_aggregates(&req1).is_none());

    let req2 = serde_json::json!({
        "aggregationType": "single_group",
        "selectList": [agg_item("COUNT", None, false)],
    });
    assert!(detect_group_by_aggregates(&req2).is_none());
}

/// Scenario: An empty groupBy array returns None.
#[test]
fn detect_group_by_aggregates_empty_group_by_returns_none() {
    let req = serde_json::json!({
        "aggregationType": "group_by",
        "groupBy": [],
        "selectList": [agg_item("SUM", Some("AMOUNT"), false)],
    });
    assert!(detect_group_by_aggregates(&req).is_none());
}

/// Scenario: `partial_emits_items` produces 3 columns for stat aggregates.
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

/// Scenario: The scan's partial SELECT and the `EMITS` clause name the same partial columns in order.
#[test]
fn scan_select_list_and_emits_agree_per_agg_kind() {
    /// Both sides terminate the name with a double quote.
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

    // Mixed arities make a plan ordinal and a column ordinal diverge.
    let mixed: Vec<AggregatePlan> = all_kinds.iter().map(plan_for).collect();
    assert_eq!(
        partial_names_in(&crate::scan::build_partial_agg_sql(&mixed, "aliased")),
        partial_names_in(&partial_emits_items(&mixed, &col_types, &[]).join(", ")),
        "mixed-arity plan list: scan SELECT list and EMITS clause disagree"
    );
}

/// Scenario: The VAR_POP merge guards the count with NULLIF and does not divide by N-1.
#[test]
fn var_pop_merge_formula_divides_by_n() {
    let plans = vec![AggregatePlan {
        kind: AggKind::VarPop,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(
        sql.contains("NULLIF"),
        "var_pop merge must guard zero count: {sql}"
    );
    assert!(
        !sql.contains("- 1"),
        "var_pop must not subtract 1 from count: {sql}"
    );
}

/// Scenario: The VAR_SAMP merge divides by N-1 and maps N<=1 to NULL.
#[test]
fn var_samp_merge_formula_divides_by_n_minus_1() {
    let plans = vec![AggregatePlan {
        kind: AggKind::VarSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(
        sql.contains("<= 1"),
        "var_samp merge must guard count<=1 with '<= 1': {sql}"
    );
    assert!(
        sql.contains("CASE"),
        "var_samp merge must use CASE for N<=1 guard: {sql}"
    );
}

/// Scenario: The STDDEV_POP merge wraps the variance in SQRT.
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

/// Scenario: The STDDEV_SAMP merge wraps the sample variance in SQRT.
#[test]
fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(sql.contains("SQRT("), "stddev_samp must use SQRT: {sql}");
    assert!(
        sql.contains("<= 1"),
        "stddev_samp must guard N<=1 (sample divisor): {sql}"
    );
    assert!(
        sql.contains("CASE"),
        "stddev_samp must use CASE for N<=1 guard: {sql}"
    );
}

/// Scenario: The STDDEV_POP merge passes NULL through for N=0.
#[test]
fn stddev_pop_merge_null_passthrough_for_n_zero() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevPop,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(
        sql.contains("IS NULL"),
        "stddev_pop must pass NULL through for N=0 via IS NULL guard: {sql}"
    );
    // Guards against tiny-negative float rounding.
    assert!(
        sql.contains("GREATEST"),
        "stddev_pop must keep GREATEST rounding guard: {sql}"
    );
}

/// Scenario: The STDDEV_SAMP merge passes NULL through for N=0 and N=1.
#[test]
fn stddev_samp_merge_null_passthrough_for_n_zero_and_n_one() {
    let plans = vec![AggregatePlan {
        kind: AggKind::StddevSamp,
        column: Some("X".into()),
        arg_expr: None,
    }];
    let sql = merge_select_items(&plans).join(", ");
    assert!(
        sql.contains("IS NULL"),
        "stddev_samp must pass NULL through for N<=1 via IS NULL guard: {sql}"
    );
    // Guards against tiny-negative float rounding.
    assert!(
        sql.contains("GREATEST"),
        "stddev_samp must keep GREATEST rounding guard: {sql}"
    );
}

/// Scenario: HAVING renders after GROUP BY in the outer wrapper.
#[test]
fn having_clause_appears_in_outer_wrapper_only() {
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
            storage: ScanStorage::Inline(sample_storage()),
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
    assert!(
        sql.contains("HAVING"),
        "outer wrapper must contain HAVING: {sql}"
    );
    assert!(
        sql.contains("100"),
        "HAVING predicate value must be in SQL: {sql}"
    );
    let having_pos = sql.find("HAVING").unwrap();
    let group_by_pos = sql.find("GROUP BY").unwrap();
    assert!(
        having_pos > group_by_pos,
        "HAVING must appear after GROUP BY: {sql}"
    );
}

const OVER_LENGTH_VALUE: &str = "over-length-value-abcdefg";

/// Spelled out independently of the production formatter so a changed construct fails.
fn expected_pad(fragment: &str, width: u32) -> String {
    format!(
        "CASE WHEN character_length({fragment}) < {width} \
             THEN rpad({fragment}, {width}) ELSE {fragment} END"
    )
}

/// Scenario: A CHAR(20) group key is blank-padded so trailing-blank variants form one group (#192).
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

/// Scenario: The pad width parses from inside the parentheses, so `CHAR(3) ASCII` still pads.
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

/// Scenario: An over-length value passes through untruncated, since `rpad` would merge it into a wrong group.
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

/// Scenario: A VARCHAR group key is untouched, proving a prefix rather than substring match.
#[test]
fn varchar_declared_group_key_is_left_unpadded() {
    let keys = vec![r#""REGION""#.to_string()];

    let padded = blank_pad_char_group_keys(&keys, &["VARCHAR(10)".to_string()]);

    assert_eq!(
        padded, keys,
        "a VARCHAR-declared group key must be left unpadded: {padded:?}"
    );
}

/// Scenario: A mixed VARCHAR + CHAR multi-key GROUP BY pads only the CHAR slot at its own width.
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

/// Scenario: The padded fragment plans and evaluates in DataFusion with the #192 semantics.
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

/// Scenario: A CASE fragment nests inside the pad splice and still parses in DataFusion.
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

/// Scenario: A grouped statistical aggregate over an expression argument declines to the wrapper.
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

/// Scenario: A grouped `SQRT(STDDEV(<expr>))` declines to the qualified single-table wrapper.
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

/// Scenario: A HAVING over a statistical aggregate of an expression does not render over the merge.
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
