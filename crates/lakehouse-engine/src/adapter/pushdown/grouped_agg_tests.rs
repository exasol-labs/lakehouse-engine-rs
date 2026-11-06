use super::super::detect_aggregates;
use super::super::joins::{
    FanOutProjection, build_qualified_single_table_fallback_sql, referenced_columns,
};
use super::super::support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, involved_columns, shard_count,
};
use super::super::test_support::*;
use super::*;
use crate::scan::spec::{CommonScanSpec, ScanStorage};
use crate::scan::test_support::WithFiles;

#[test]
fn scalar_over_merge_casts_to_exasol_char_target() {
    let sum_node = agg_item("SUM", Some("x"), false);
    let plans = vec![parse_agg_item(&sum_node).expect("SUM(x) must parse to a plan")];
    let node = cast_to(
        ExasolType::Char {
            size: 20,
            ascii: true,
        },
        sum_node,
    );
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

#[test]
fn scalar_over_merge_nested_char_cast_renders_char_at_both_levels() {
    let sum_node = agg_item("SUM", Some("x"), false);
    let plans = vec![parse_agg_item(&sum_node).expect("SUM(x) must parse to a plan")];
    let char_type = ExasolType::Char {
        size: 20,
        ascii: true,
    };
    let node = cast_to(char_type, cast_to(char_type, sum_node));
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

#[test]
fn grouped_count_distinct_falls_back_to_row_scan() {
    let req = pushdown_request("T", &[("REGION", ExasolType::WIDEST_VARCHAR)])
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        .item(
            agg_item("COUNT", Some("L_SHIPMODE"), true),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "grouped COUNT(DISTINCT) must still decline (row-scan fallback)"
    );
    assert!(
        detect_aggregates(&req).is_none(),
        "the single-group path rejects any request carrying a non-empty GROUP BY"
    );
}

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
        ("EVENT_DATE".to_string(), ExasolType::Date),
        ("EVENT_TS".to_string(), ExasolType::BARE_TIMESTAMP),
    ];
    let emits = partial_emits_items(&plans, &column_types(&col_types), &[]);
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

#[test]
fn partial_emits_sum_integer_stays_decimal() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("AMOUNT".into()),
        arg_expr: None,
    }];
    let col_types = vec![("AMOUNT".to_string(), ExasolType::decimal(20, 0))];
    let emits = partial_emits_items(&plans, &column_types(&col_types), &[]);
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

#[test]
fn partial_emits_sum_double_stays_double() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let col_types = vec![("SCORE".to_string(), ExasolType::DoublePrecision)];
    let emits = partial_emits_items(&plans, &column_types(&col_types), &[]);
    assert!(
        emits[0].contains("DOUBLE PRECISION"),
        "SUM over DOUBLE must emit DOUBLE PRECISION: {:?}",
        emits[0]
    );
}

#[test]
fn aggregate_falls_back_to_row_scan_for_sum_of_non_numeric() {
    let col_types_varchar = vec![("NAME".to_string(), ExasolType::WIDEST_VARCHAR)];
    let sum_varchar = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("NAME".into()),
        arg_expr: None,
    }];
    assert!(
        !validate_agg_col_types(&sum_varchar, &column_types(&col_types_varchar)),
        "SUM over VARCHAR must fail validation (fall back to row scan)"
    );

    let col_types_date = vec![("EVENT_DATE".to_string(), ExasolType::Date)];
    let sum_date = vec![AggregatePlan {
        kind: AggKind::Sum,
        column: Some("EVENT_DATE".into()),
        arg_expr: None,
    }];
    assert!(
        !validate_agg_col_types(&sum_date, &column_types(&col_types_date)),
        "SUM over DATE must fail validation (fall back to row scan)"
    );
}

#[test]
fn grouped_aggregate_sum_over_varchar_falls_back_via_type_validation() {
    let req = pushdown_request("T", &[("REGION", ExasolType::WIDEST_VARCHAR)])
        .group_by(vec![col("REGION")])
        .column(col("REGION"))
        // NAME is VARCHAR — invalid for SUM
        .item(
            agg_item("SUM", Some("NAME"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();

    let detected = detect_group_by_aggregates(&req);
    assert!(
        detected.is_some(),
        "detect_group_by_aggregates must accept the shape: {}",
        req.json()
    );
    let agg_plans = detected.unwrap().plans;

    let col_types = vec![
        ("REGION".to_string(), ExasolType::WIDEST_VARCHAR),
        ("NAME".to_string(), ExasolType::WIDEST_VARCHAR),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &column_types(&col_types)),
        "validate_agg_col_types must fail for SUM over VARCHAR (fall back to row scan)"
    );

    let col_types_date = vec![
        ("REGION".to_string(), ExasolType::WIDEST_VARCHAR),
        ("NAME".to_string(), ExasolType::Date),
    ];
    assert!(
        !validate_agg_col_types(&agg_plans, &column_types(&col_types_date)),
        "validate_agg_col_types must fail for SUM over DATE (fall back to row scan)"
    );

    let col_types_numeric = vec![
        ("REGION".to_string(), ExasolType::WIDEST_VARCHAR),
        ("NAME".to_string(), ExasolType::DoublePrecision),
    ];
    assert!(
        validate_agg_col_types(&agg_plans, &column_types(&col_types_numeric)),
        "validate_agg_col_types must pass for SUM over DOUBLE PRECISION"
    );
}

/// A grouped request over table `T`, which declares the columns the select list projects.
fn grouped_request(
    columns: &[(&str, ExasolType)],
    group_by: Vec<serde_json::Value>,
) -> PushdownRequestBuilder {
    pushdown_request("T", columns).group_by(group_by)
}

fn mod_item(column: &str, divisor: i64) -> serde_json::Value {
    scalar_fn("MOD", [col(column), num_lit(divisor)])
}

fn upper_item(column: &str) -> serde_json::Value {
    scalar_fn("UPPER", [col(column)])
}

#[test]
fn detect_group_by_aggregates_column_key() {
    let req = grouped_request(
        &[("REGION", ExasolType::WIDEST_VARCHAR)],
        vec![col("REGION")],
    )
    .column(col("REGION"))
    .count_star()
    .build_pushdown();
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
    CommonScanSpec {
        aggregates: Some(result.plans.clone()),
        group_keys: Some(result.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![])
}

#[test]
fn grouped_order_by_no_limit_renders_explicit_merge_order_by() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(count_star(), ExasolType::decimal(20, 0))
        .order_by(serde_json::json!([order_by_element(col("ID"), true, true)]))
        .build_pushdown();

    let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
    assert_eq!(
        build_grouped_order_by_clause(req.json(), &result),
        Some(GroupedOrderBy::Clause("1 ASC NULLS LAST".to_string())),
        "grouped ORDER BY must map the sort key to its 1-based output ordinal"
    );

    let group_key_types =
        group_key_exasol_types(&req, &result.group_keys, &result.select_items).unwrap();
    let sql = build_grouped_aggregate_scan_sql(
        &grouped_spec(&result),
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &result.plan_types,
        &result.select_items,
        None,
        0,
        &column_types(&[("ID", ExasolType::decimal(20, 0))]),
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

#[test]
fn grouped_order_by_select_list_aggregate_renders_merged_partial() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::decimal(36, 2),
        )
        .order_by(serde_json::json!([
            order_by_element(agg_item("SUM", Some("AMOUNT"), false), false, true),
            order_by_element(col("ID"), true, false),
        ]))
        .build_pushdown();

    let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
    assert_eq!(
        build_grouped_order_by_clause(req.json(), &detection),
        Some(GroupedOrderBy::Clause(
            r#"SUM("PARTIAL_sum_0") DESC NULLS LAST, 1 ASC NULLS FIRST"#.to_string()
        )),
        "an aggregate sort key must render as its merged partial, a group key as its ordinal"
    );
}

#[test]
fn grouped_order_by_aggregate_absent_from_plans_is_unresolvable() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(count_star(), ExasolType::decimal(20, 0))
        .order_by(serde_json::json!([order_by_element(
            agg_item("SUM", Some("AMOUNT"), false),
            false,
            true
        )]))
        .build_pushdown();

    let detection = detect_group_by_aggregates(&req).expect("grouped aggregate");
    assert_eq!(
        build_grouped_order_by_clause(req.json(), &detection),
        Some(GroupedOrderBy::Unresolvable),
        "an aggregate with no matching plan must not resolve to a fabricated partial"
    );
}

#[test]
fn detect_group_by_aggregates_expression_key() {
    let req = grouped_request(
        NO_COLUMNS,
        vec![predicate_equal(col("STATUS"), str_lit("active"))],
    )
    .item(
        agg_item("SUM", Some("AMOUNT"), false),
        ExasolType::WIDEST_VARCHAR,
    )
    .build_pushdown();
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

#[test]
fn detect_group_by_unsupported_expression_falls_back() {
    let req = grouped_request(
        NO_COLUMNS,
        vec![serde_json::json!({"type": "fn_custom_unsupported", "name": "MYSTERY"})],
    )
    .count_star()
    .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "unsupported expression must fall back to None"
    );
}

#[test]
fn detect_group_by_mixed_select_falls_back() {
    let year = scalar_fn("YEAR", [col("TS")]);
    let req = grouped_request(NO_COLUMNS, vec![col("REGION")])
        .item(year, ExasolType::WIDEST_VARCHAR)
        .count_star()
        .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "non-aggregate non-column in selectList must fall back"
    );
}

#[test]
fn composed_nested_aggregate_request_does_not_reference_phantom_column() {
    let group_key = numbered_col("EVENTS", 0, "ID");
    let req = pushdown_request("T", NO_COLUMNS)
        .field("from", table_ref("EVENTS"))
        .group_by(vec![group_key])
        .field("type", "select")
        .item(null_lit(), ExasolType::Boolean)
        .build_pushdown();
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
    let group_key_types =
        group_key_exasol_types(&req, &result.group_keys, &result.select_items).unwrap();
    let sql = build_grouped_aggregate_scan_sql(
        &CommonScanSpec {
            aggregates: Some(result.plans.clone()),
            group_keys: Some(result.group_keys.clone()),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        }
        .with_files(vec![]),
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &result.plan_types,
        &result.select_items,
        None,
        0,
        &column_types(&[("ID", ExasolType::decimal(20, 0))]),
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

#[test]
fn literal_bool_selectlist_item_classifies_as_constant_not_group_key() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(bool_lit(true), ExasolType::Boolean)
        .item(count_star(), ExasolType::decimal(20, 0))
        .build_pushdown();
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

#[test]
fn detect_group_by_aggregates_preserves_select_list_order() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(
            agg_item("SUM", Some("SCORE"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(mod_item("ID", 4), ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
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

#[test]
fn detect_group_by_aggregates_interleaved_multi_key_preserves_order() {
    let req = grouped_request(
        &[
            ("REGION", ExasolType::WIDEST_VARCHAR),
            ("YEAR", ExasolType::WIDEST_VARCHAR),
        ],
        vec![col("REGION"), col("YEAR")],
    )
    .column(col("REGION"))
    .item(
        agg_item("SUM", Some("SCORE"), false),
        ExasolType::WIDEST_VARCHAR,
    )
    .column(col("YEAR"))
    .build_pushdown();
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

#[test]
fn detect_group_by_aggregates_expr_key_after_agg_preserves_order() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .count_star()
        .item(mod_item("ID", 4), ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
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

#[test]
fn detect_group_by_aggregates_aggregate_first_with_having_preserves_order() {
    let req = pushdown_request("T", NO_COLUMNS)
        .group_by(vec![mod_item("ID", 4)])
        .field(
            "having",
            compare(
                "predicate_greater",
                agg_item("SUM", Some("SCORE"), false),
                num_lit(100),
            ),
        )
        .item(
            agg_item("SUM", Some("SCORE"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .item(mod_item("ID", 4), ExasolType::WIDEST_VARCHAR)
        .build_pushdown();
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

#[test]
fn detect_group_by_all_expression_multi_key() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4), upper_item("NAME")])
        .item(mod_item("ID", 4), ExasolType::WIDEST_VARCHAR)
        .item(upper_item("NAME"), ExasolType::WIDEST_VARCHAR)
        .count_star()
        .build_pushdown();
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

    let col_types: Vec<(String, ExasolType)> = vec![];
    let group_key_types = vec![ExasolType::WIDEST_VARCHAR; 2];
    let aggregate_types = vec![ExasolType::decimal(18, 0)];
    let spec_template = CommonScanSpec {
        aggregates: Some(result.plans.clone()),
        group_keys: Some(result.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&col_types),
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

    let bad_req = grouped_request(
        NO_COLUMNS,
        vec![
            mod_item("ID", 4),
            serde_json::json!({"type": "fn_custom_unsupported", "name": "MYSTERY"}),
        ],
    )
    .item(mod_item("ID", 4), ExasolType::WIDEST_VARCHAR)
    .item(
        serde_json::json!({"type": "fn_custom_unsupported", "name": "MYSTERY"}),
        ExasolType::WIDEST_VARCHAR,
    )
    .count_star()
    .build_pushdown();
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
    let col_types: Vec<(String, ExasolType)> = vec![
        ("AMOUNT".to_string(), ExasolType::DoublePrecision),
        ("SCORE".to_string(), ExasolType::DoublePrecision),
    ];
    let spec_template = CommonScanSpec {
        aggregates: Some(agg_plans.clone()),
        group_keys: Some(group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &vec![ExasolType::DoublePrecision; agg_plans.len()],
        &select_items,
        None,
        0,
        &column_types(&col_types),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    )
}

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

#[test]
fn grouped_common_blob_has_no_limit() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        limit: Some(100),
        aggregates: Some(vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }]),
        group_keys: Some(vec!["\"REGION\"".into()]),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &[ExasolType::decimal(18, 0)],
        &keys_first_select_items(1, 1),
        Some(100),
        0,
        &column_types(&col_types),
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

#[test]
fn grouped_merge_offset_never_reaches_per_shard_spec() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        limit: Some(100),
        aggregates: Some(vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }]),
        group_keys: Some(vec!["\"REGION\"".into()]),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &[ExasolType::decimal(18, 0)],
        &keys_first_select_items(1, 1),
        Some(100),
        3,
        &column_types(&col_types),
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

#[test]
fn grouped_merge_zero_offset_is_byte_identical_to_bare_limit() {
    let files = vec![("s3://w/f0.parquet".to_string(), 200u64)];
    let g = shard_count(1, 1, files.len());
    let col_types = vec![("AMOUNT".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        limit: Some(100),
        aggregates: Some(vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }]),
        group_keys: Some(vec!["\"REGION\"".into()]),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &[ExasolType::decimal(18, 0)],
        &keys_first_select_items(1, 1),
        Some(100),
        0,
        &column_types(&col_types),
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

#[test]
fn grouped_merge_renders_limit_offset_in_clause_order() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(count_star(), ExasolType::decimal(20, 0))
        .order_by(serde_json::json!([order_by_element(col("ID"), true, true)]))
        .build_pushdown();

    let result = detect_group_by_aggregates(&req).expect("grouped aggregate");
    let group_key_types =
        group_key_exasol_types(&req, &result.group_keys, &result.select_items).unwrap();
    let sql = build_grouped_aggregate_scan_sql(
        &grouped_spec(&result),
        &[vec![("s3://wh/f0.parquet".to_string(), 1u64)]],
        &result.group_keys,
        &group_key_types,
        &result.plans,
        &result.plan_types,
        &result.select_items,
        Some(2),
        1,
        &column_types(&[("ID", ExasolType::decimal(20, 0))]),
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
    group_key_types: Vec<ExasolType>,
    agg_plans: Vec<AggregatePlan>,
    aggregate_types: Vec<ExasolType>,
    select_items: Vec<GroupedSelectItem>,
    having: Option<&str>,
) -> String {
    let col_types: Vec<(String, ExasolType)> = vec![
        ("AMOUNT".to_string(), ExasolType::DoublePrecision),
        ("SCORE".to_string(), ExasolType::DoublePrecision),
    ];
    let spec_template = CommonScanSpec {
        aggregates: Some(agg_plans.clone()),
        group_keys: Some(group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&col_types),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        having,
        None,
    )
}

#[test]
fn grouped_wrapper_agg_before_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec![ExasolType::DoublePrecision],
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

#[test]
fn grouped_wrapper_interleaved_multi_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#""REGION""#.to_string(), r#""YEAR""#.to_string()],
        vec![ExasolType::varchar(100), ExasolType::decimal(4, 0)],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec![ExasolType::DoublePrecision],
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

#[test]
fn grouped_wrapper_expr_key_after_agg_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![AggregatePlan {
            kind: AggKind::Count,
            column: None,
            arg_expr: None,
        }],
        vec![ExasolType::decimal(18, 0)],
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

#[test]
fn grouped_wrapper_agg_first_with_having_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("SCORE".into()),
            arg_expr: None,
        }],
        vec![ExasolType::DoublePrecision],
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

fn case_flag_eq(column: &str, val: &str) -> serde_json::Value {
    case_when(
        [predicate_equal(col(column), str_lit(val))],
        [num_lit(1), num_lit(0)],
    )
}

fn round_pct_over_aggregates() -> serde_json::Value {
    let returned = agg_item_expr("SUM", case_flag_eq("L_RETURNFLAG", "R"), false);
    scalar_fn(
        "ROUND",
        [
            scalar_fn(
                "FLOAT_DIV",
                [
                    scalar_fn("MULT", [lit("literal_double", 100.0), returned]),
                    count_star(),
                ],
            ),
            num_lit(2),
        ],
    )
}

fn soa_col_types() -> Vec<(String, ExasolType)> {
    vec![
        ("L_RETURNFLAG".to_string(), ExasolType::varchar(1)),
        ("L_QUANTITY".to_string(), ExasolType::decimal(36, 2)),
        ("L_EXTENDEDPRICE".to_string(), ExasolType::DoublePrecision),
    ]
}

/// Mirrors the production grouped branch of `handle_pushdown`.
fn build_grouped_from_detection(req: &PushdownRequest) -> String {
    let d = detect_group_by_aggregates(req)
        .expect("must detect the grouped scalar-over-aggregate pushdown");
    let group_key_types = group_key_exasol_types(req, &d.group_keys, &d.select_items).unwrap();
    let spec_template = CommonScanSpec {
        aggregates: Some(d.plans.clone()),
        group_keys: Some(d.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&soa_col_types()),
        SCAN_UDF_NAME,
        DISTRIBUTE_FILES_UDF_NAME,
        None,
        None,
    )
}

#[test]
fn grouped_scalar_over_aggregate_detects_and_dedups_inner_aggregates() {
    let req = grouped_request(
        &[("L_RETURNFLAG", ExasolType::varchar(1))],
        vec![col("L_RETURNFLAG")],
    )
    .column(col("L_RETURNFLAG"))
    .item(
        agg_item("SUM", Some("L_QUANTITY"), false),
        ExasolType::decimal(36, 2),
    )
    .item(
        agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
        ExasolType::DoublePrecision,
    )
    .item(round_pct_over_aggregates(), ExasolType::decimal(5, 2))
    .count_star()
    .build_pushdown();
    let d = detect_group_by_aggregates(&req).expect("must detect grouped scalar-over-aggregate");

    assert!(
        matches!(
            &d.select_items[3],
            GroupedSelectItem::ScalarOverAggregate {
                select_index: 3,
                declared_type,
                ..
            } if *declared_type == ExasolType::decimal(5, 2)
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

#[test]
fn grouped_scalar_over_aggregate_renders_merged_partials() {
    let req = grouped_request(
        &[("L_RETURNFLAG", ExasolType::varchar(1))],
        vec![col("L_RETURNFLAG")],
    )
    .column(col("L_RETURNFLAG"))
    .item(
        agg_item("SUM", Some("L_QUANTITY"), false),
        ExasolType::decimal(36, 2),
    )
    .item(
        agg_item("AVG", Some("L_EXTENDEDPRICE"), false),
        ExasolType::DoublePrecision,
    )
    .item(round_pct_over_aggregates(), ExasolType::decimal(5, 2))
    .build_pushdown();
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

#[test]
fn grouped_scalar_over_aggregate_preserves_selectlist_order() {
    let req = grouped_request(
        &[("L_RETURNFLAG", ExasolType::varchar(1))],
        vec![col("L_RETURNFLAG")],
    )
    .item(round_pct_over_aggregates(), ExasolType::decimal(5, 2))
    .column(col("L_RETURNFLAG"))
    .item(
        agg_item("SUM", Some("L_QUANTITY"), false),
        ExasolType::decimal(36, 2),
    )
    .build_pushdown();
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

#[test]
fn grouped_undecomposable_falls_back_to_qualified_wrapper() {
    let (request, _) = pushdown_request(
        "LINEITEM",
        &[
            ("L_RETURNFLAG", ExasolType::varchar(1)),
            ("X", ExasolType::DoublePrecision),
            ("Y", ExasolType::DoublePrecision),
        ],
    )
    .group_by(vec![table_col("LINEITEM", "L_RETURNFLAG")])
    .column(table_col("LINEITEM", "L_RETURNFLAG"))
    .item(
        scalar_fn(
            "ROUND",
            [
                scalar_fn(
                    "FLOAT_DIV",
                    [
                        agg_item_expr("SUM", table_col("LINEITEM", "X"), false),
                        agg_item_expr("COUNT", table_col("LINEITEM", "Y"), true),
                    ],
                ),
                num_lit(2),
            ],
        ),
        ExasolType::decimal(5, 2),
    )
    .build();
    let pushdown = parsed_pushdown(&request);

    assert!(
        detect_group_by_aggregates(&pushdown).is_none(),
        "a nested COUNT(DISTINCT) must decline the grouped partial/merge path"
    );

    let all_cols = involved_columns(&request, |tables| tables.first())
        .unwrap()
        .types;
    let referenced = referenced_columns(pushdown.json(), &column_types(&all_cols));
    let fan_out_spec = CommonScanSpec {
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    };
    let fan_out = FanOutProjection::new(fan_out_spec, &referenced);
    let sql = build_qualified_single_table_fallback_sql(
        &request,
        &pushdown,
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

#[test]
fn render_having_over_merge_rewrites_aggregate_to_partial() {
    let having = compare(
        "predicate_greater",
        agg_item("SUM", Some("SCORE"), false),
        num_lit(250),
    );
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

#[test]
fn grouped_wrapper_having_over_aggregate_uses_merge_expression() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(
            agg_item("SUM", Some("SCORE"), false),
            ExasolType::DoublePrecision,
        )
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();
    let aggregate_types = detection.plan_types.clone();

    let having_node = compare(
        "predicate_greater",
        agg_item("SUM", Some("SCORE"), false),
        num_lit(250),
    );
    let having = render_having_over_merge(&having_node, &detection.plans)
        .expect("HAVING must render over the merge decomposition");

    let col_types: Vec<(String, ExasolType)> =
        vec![("SCORE".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        aggregates: Some(detection.plans.clone()),
        group_keys: Some(detection.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&col_types),
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

#[test]
fn render_having_over_merge_declines_unknown_aggregate() {
    let having = compare("predicate_greater", count_star(), num_lit(10));
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

#[test]
fn grouped_wrapper_outer_select_follows_select_list_order() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(
            agg_item("SUM", Some("SCORE"), false),
            ExasolType::DoublePrecision,
        )
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();
    let aggregate_types = detection.plan_types.clone();

    let col_types: Vec<(String, ExasolType)> =
        vec![("SCORE".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        aggregates: Some(detection.plans.clone()),
        group_keys: Some(detection.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&col_types),
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

#[test]
fn grouped_wrapper_multi_key_having_and_limit_outer_only() {
    let req = grouped_request(
        &[("REGION", ExasolType::varchar(100))],
        vec![col("REGION"), mod_item("ID", 4)],
    )
    .column(col("REGION"))
    .item(
        agg_item("SUM", Some("SCORE"), false),
        ExasolType::DoublePrecision,
    )
    .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
    .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");
    let group_key_types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();
    let aggregate_types = detection.plan_types.clone();

    let having_node = compare(
        "predicate_greater",
        agg_item("SUM", Some("SCORE"), false),
        num_lit(100),
    );
    let having = render_having_over_merge(&having_node, &detection.plans)
        .expect("HAVING must render over the merge decomposition");

    let col_types: Vec<(String, ExasolType)> =
        vec![("SCORE".to_string(), ExasolType::DoublePrecision)];
    let spec_template = CommonScanSpec {
        aggregates: Some(detection.plans.clone()),
        group_keys: Some(detection.group_keys.clone()),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
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
        &column_types(&col_types),
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

#[test]
fn group_key_type_resolved_by_index_not_string_match() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(
            agg_item("SUM", Some("SCORE"), false),
            ExasolType::DoublePrecision,
        )
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let group_keys = vec![r#"("id" % 4)"#.to_string()];
    let select_items = detection.select_items.clone();
    let types = group_key_exasol_types(&req, &group_keys, &select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::decimal(9, 0)],
        "type must resolve via select_index, not via string-matching the (drifted) \
             rendered group key: {types:?}"
    );
}

#[test]
fn group_key_types_multi_key_mixed_types() {
    let req = grouped_request(
        &[("REGION", ExasolType::varchar(100))],
        vec![col("REGION"), mod_item("ID", 4)],
    )
    .column(col("REGION"))
    .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
    .count_star()
    .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(types.len(), 2, "one declared type per group key");
    assert_eq!(
        types[0],
        ExasolType::varchar(100),
        "the REGION key must resolve its own VARCHAR type, at its own select index: {types:?}"
    );
    assert_eq!(
        types[1],
        ExasolType::decimal(9, 0),
        "the MOD(id,4) key must resolve its own DECIMAL type, not a shared/defaulted \
             VARCHAR: {types:?}"
    );
}

#[test]
fn group_key_exasol_types_resolves_char_case_key() {
    let case_key = case_when(
        [compare("predicate_less", col("C_DECIMAL_A"), num_lit(0))],
        [str_lit("NEG"), str_lit("POS")],
    );
    let req = grouped_request(NO_COLUMNS, vec![case_key.clone()])
        .item(
            case_key,
            ExasolType::Char {
                size: 3,
                ascii: true,
            },
        )
        .count_star()
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req)
        .expect("equal-length CASE group key must be detected as a grouped aggregate");
    assert_eq!(detection.group_keys.len(), 1, "one group key");

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::Char {
            size: 3,
            ascii: true
        }],
        "an equal-length CASE group key must resolve to CHAR(3) ASCII, not \
             VARCHAR(3) ASCII: {types:?}"
    );
}

/// Scenario: A VARCHAR dataType renders as VARCHAR(n) and its group key is not padded
#[test]
fn group_key_exasol_types_resolves_varchar_key_unchanged() {
    let req = grouped_request(&[("REGION", ExasolType::varchar(10))], vec![col("REGION")])
        .column(col("REGION"))
        .count_star()
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::varchar(10)],
        "a VARCHAR-declared group key must be unaffected: {types:?}"
    );
}

#[test]
fn group_key_exasol_types_resolves_char_type_for_unprojected_group_key() {
    let req = grouped_request(NO_COLUMNS, vec![cast_to(ExasolType::char(20), col("NAME"))])
        .count_star()
        .build_pushdown();
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

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::char(20)],
        "an unprojected group key must resolve CHAR(20) from its own groupBy dataType: \
             {types:?}"
    );
}

#[test]
fn group_key_exasol_types_resolves_varchar_type_for_unprojected_group_key() {
    let req = grouped_request(
        NO_COLUMNS,
        vec![cast_to(ExasolType::varchar(10), col("NAME"))],
    )
    .count_star()
    .build_pushdown();
    let detection = detect_group_by_aggregates(&req)
        .expect("an unprojected group key must still detect as a grouped aggregate");

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::varchar(10)],
        "an unprojected VARCHAR-declared group key must resolve VARCHAR(10): {types:?}"
    );
}

#[test]
fn group_key_exasol_types_prefers_select_list_type_over_group_by_type() {
    let req = grouped_request(NO_COLUMNS, vec![cast_to(ExasolType::char(20), col("NAME"))])
        .item(
            cast_to(ExasolType::char(20), col("NAME")),
            ExasolType::varchar(30),
        )
        .count_star()
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");

    let types =
        group_key_exasol_types(&req, &detection.group_keys, &detection.select_items).unwrap();

    assert_eq!(
        types,
        vec![ExasolType::varchar(30)],
        "the selectListDataTypes entry must win over the groupBy node's own dataType: \
             {types:?}"
    );
}

#[test]
fn constant_projection_casts_literal_to_char() {
    let req = grouped_request(NO_COLUMNS, vec![col("REGION")])
        .item(
            str_lit("X"),
            ExasolType::Char {
                size: 1,
                ascii: true,
            },
        )
        .count_star()
        .build_pushdown();
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

#[test]
fn min_over_char_expression_declares_char_partial_and_an_uncast_merge() {
    let cast_arg = cast_to(ExasolType::char(20), col("C_VARCHAR"));
    let req = grouped_request(&[("REGION", ExasolType::varchar(100))], vec![col("REGION")])
        .column(col("REGION"))
        .item(agg_item_expr("MIN", cast_arg, false), ExasolType::char(20))
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req).expect("must detect grouped aggregate");
    assert_eq!(detection.plans.len(), 1, "one aggregate plan (MIN)");
    assert_eq!(
        detection.plan_types,
        vec![ExasolType::char(20)],
        "the MIN plan's declared type must be CHAR(20), not VARCHAR(20): {:?}",
        detection.plan_types
    );

    let partial_emits = partial_emits_items(
        &detection.plans,
        &ColumnTypes::default(),
        &detection.plan_types,
    );
    assert_eq!(
        partial_emits,
        vec![r#""PARTIAL_min_0" CHAR(20)"#.to_string()],
        "MIN over a CHAR-declared expression must declare its partial column \
             CHAR(20), not VARCHAR(20): {partial_emits:?}"
    );

    let merge_items = cast_merge_items(
        &detection.plans,
        &detection.plan_types,
        &ColumnTypes::default(),
    );
    assert_eq!(
        merge_items,
        vec![r#"MIN("PARTIAL_min_0")"#.to_string()],
        "MIN returns its CHAR(20) partial's type, so the merge item needs no cast: \
         {merge_items:?}"
    );
}

#[test]
fn detect_group_by_aggregates_no_group_by_type_returns_none() {
    let req1 = pushdown_request("T", NO_COLUMNS)
        .field("groupBy", serde_json::json!([col("REGION")]))
        .count_star()
        .build_pushdown();
    assert!(detect_group_by_aggregates(&req1).is_none());

    let req2 = pushdown_request("T", NO_COLUMNS)
        .single_group()
        .count_star()
        .build_pushdown();
    assert!(detect_group_by_aggregates(&req2).is_none());
}

#[test]
fn detect_group_by_aggregates_empty_group_by_returns_none() {
    let req = pushdown_request("T", NO_COLUMNS)
        .group_by(vec![])
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    assert!(detect_group_by_aggregates(&req).is_none());
}

#[test]
fn stat_aggregate_emits_three_partial_columns() {
    for kind in &[
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::Variance,
        AggKind::StddevPop,
        AggKind::StddevSamp,
        AggKind::Stddev,
    ] {
        let plans = vec![AggregatePlan {
            kind: kind.clone(),
            column: Some("SCORE".into()),
            arg_expr: None,
        }];
        let col_types = vec![("SCORE".to_string(), ExasolType::DoublePrecision)];
        let items = partial_emits_items(&plans, &column_types(&col_types), &[]);
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
            items[1].contains("PARTIAL_stat_mean_0"),
            "second column must be mean: {items:?}"
        );
        assert!(
            items[2].contains("PARTIAL_stat_m2_0"),
            "third column must be m2: {items:?}"
        );
    }
}

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
    let col_types = vec![("SCORE".to_string(), ExasolType::DoublePrecision)];

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
        let emits_names = partial_names_in(
            &partial_emits_items(&plans, &column_types(&col_types), &[]).join(", "),
        );
        assert_eq!(
            scan_names, emits_names,
            "{kind:?}: scan SELECT list and EMITS clause disagree"
        );
    }

    // Mixed arities make a plan ordinal and a column ordinal diverge.
    let mixed: Vec<AggregatePlan> = all_kinds.iter().map(plan_for).collect();
    assert_eq!(
        partial_names_in(&crate::scan::build_partial_agg_sql(&mixed, "aliased")),
        partial_names_in(&partial_emits_items(&mixed, &column_types(&col_types), &[]).join(", ")),
        "mixed-arity plan list: scan SELECT list and EMITS clause disagree"
    );
}

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

#[test]
fn variance_and_stddev_merge_return_zero_for_one_value() {
    for kind in [AggKind::Variance, AggKind::Stddev] {
        let plans = vec![AggregatePlan {
            kind: kind.clone(),
            column: Some("X".into()),
            arg_expr: None,
        }];
        let sql = merge_select_items(&plans).join(", ");
        assert!(
            sql.contains(r#"CASE WHEN SUM("PARTIAL_stat_cnt_0") = 1 THEN 0 ELSE"#),
            "{kind:?} must return 0 for one non-NULL value: {sql}"
        );
        assert!(
            sql.contains(r#""STAT_GMEAN_0""#),
            "{kind:?} must merge around the grand mean: {sql}"
        );
    }
}

#[test]
fn having_clause_appears_in_outer_wrapper_only() {
    let having_filter = Some(r#"(SUM("AMOUNT") > 100)"#.to_string());
    let spec_template = CommonScanSpec {
        projection: vec!["REGION".into(), "AMOUNT".into()],
        aggregates: Some(vec![AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }]),
        group_keys: Some(vec![r#""REGION""#.to_string()]),
        storage: ScanStorage::Inline(sample_storage()),
        ..Default::default()
    }
    .with_files(vec![]);
    let shards = vec![vec![("s3://wh/f.parquet".to_string(), 1u64)]];
    let col_types = vec![
        ("REGION".to_string(), ExasolType::WIDEST_VARCHAR),
        ("AMOUNT".to_string(), ExasolType::DoublePrecision),
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
        &[ExasolType::DoublePrecision],
        &keys_first_select_items(1, 1),
        None,
        0,
        &column_types(&col_types),
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

#[test]
fn char_declared_group_key_is_blank_padded_to_its_declared_width() {
    let fragment = r#"CAST("NAME" AS VARCHAR)"#.to_string();

    let padded =
        blank_pad_char_group_keys(std::slice::from_ref(&fragment), &[ExasolType::char(20)]);

    assert_eq!(
        padded,
        vec![expected_pad(&fragment, 20)],
        "a CHAR(20)-declared group key must be blank-padded to 20 characters on the \
             DataFusion side: {padded:?}"
    );
}

#[test]
fn ascii_suffixed_char_group_key_width_is_parsed_before_the_suffix() {
    let fragment = "CASE WHEN \"C_DECIMAL_A\" < 0 THEN 'NEG' ELSE 'POS' END".to_string();

    let padded = blank_pad_char_group_keys(
        std::slice::from_ref(&fragment),
        &[ExasolType::Char {
            size: 3,
            ascii: true,
        }],
    );

    assert_eq!(
        padded,
        vec![expected_pad(&fragment, 3)],
        "a `CHAR(3) ASCII` group key must be padded to 3, not left unpadded because of \
             the character-set suffix: {padded:?}"
    );
}

#[test]
fn char_pad_leaves_an_over_length_value_unmodified() {
    let fragment = r#""NAME""#.to_string();

    let padded =
        blank_pad_char_group_keys(std::slice::from_ref(&fragment), &[ExasolType::char(20)])
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

/// Scenario: A VARCHAR dataType renders as VARCHAR(n) and its group key is not padded
#[test]
fn varchar_declared_group_key_is_left_unpadded() {
    let keys = vec![r#""REGION""#.to_string()];

    let padded = blank_pad_char_group_keys(&keys, &[ExasolType::varchar(10)]);

    assert_eq!(
        padded, keys,
        "a VARCHAR-declared group key must be left unpadded: {padded:?}"
    );
}

#[test]
fn multi_key_pad_applies_only_to_the_char_slot() {
    let keys = vec![
        r#""REGION""#.to_string(),
        r#"CAST("NAME" AS VARCHAR)"#.to_string(),
    ];

    let padded = blank_pad_char_group_keys(
        &keys,
        &[
            ExasolType::varchar(10),
            ExasolType::Char {
                size: 5,
                ascii: true,
            },
        ],
    );

    assert_eq!(
        padded,
        vec![keys[0].clone(), expected_pad(&keys[1], 5)],
        "only the CHAR-declared slot may be padded, at its own width: {padded:?}"
    );
}

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

    let padded = blank_pad_char_group_keys(&[r#""V""#.to_string()], &[ExasolType::char(20)])
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
    let padded = blank_pad_char_group_keys(
        &[case_fragment],
        &[ExasolType::Char {
            size: 3,
            ascii: true,
        }],
    )
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

#[test]
fn grouped_stat_aggregate_over_expression_argument_declines() {
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .item(
            agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
            ExasolType::DoublePrecision,
        )
        .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "a grouped STDDEV over an expression argument must decline the grouped \
             partial/merge path"
    );
}

#[test]
fn scalar_over_stat_aggregate_with_expression_argument_declines() {
    let sqrt_over_stat = scalar_fn(
        "SQRT",
        [agg_item_expr("STDDEV", mod_item("SCORE", 4), false)],
    );
    assert!(
        classify_scalar_over_aggregate(&sqrt_over_stat).is_none(),
        "a scalar wrapping a stat aggregate over an expression must not classify"
    );

    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .item(sqrt_over_stat, ExasolType::DoublePrecision)
        .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req).is_none(),
        "an unclassifiable scalar-over-aggregate item must decline the whole \
             grouped detection"
    );
}

#[test]
fn having_over_stat_aggregate_with_expression_argument_declines() {
    let having = compare(
        "predicate_greater",
        agg_item_expr("STDDEV", mod_item("SCORE", 4), false),
        lit("literal_double", 5.0),
    );
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

#[test]
fn only_a_char_group_key_has_a_pad_width() {
    for exa_type in EXASOL_TYPE_SPELLINGS {
        let expected = match exa_type {
            ExasolType::Char { size, .. } => Some(size),
            ExasolType::Boolean
            | ExasolType::Decimal(_)
            | ExasolType::DoublePrecision
            | ExasolType::Date
            | ExasolType::Timestamp { .. }
            | ExasolType::TimestampWithLocalTimeZone { .. }
            | ExasolType::IntervalYearToMonth { .. }
            | ExasolType::IntervalDayToSecond { .. }
            | ExasolType::Geometry { .. }
            | ExasolType::Hashtype { .. }
            | ExasolType::Varchar { .. } => None,
        };
        assert_eq!(char_width(&exa_type), expected, "{exa_type}");
    }
}

#[test]
fn sum_and_stat_aggregates_accept_only_double_and_decimal_columns() {
    let plans = |kind: AggKind| {
        vec![AggregatePlan {
            kind,
            column: Some("X".to_string()),
            arg_expr: None,
        }]
    };
    for exa_type in EXASOL_TYPE_SPELLINGS {
        let col_types = vec![("X".to_string(), exa_type)];
        let expected = match exa_type {
            ExasolType::DoublePrecision | ExasolType::Decimal(_) => true,
            ExasolType::Boolean
            | ExasolType::Date
            | ExasolType::Timestamp { .. }
            | ExasolType::TimestampWithLocalTimeZone { .. }
            | ExasolType::IntervalYearToMonth { .. }
            | ExasolType::IntervalDayToSecond { .. }
            | ExasolType::Geometry { .. }
            | ExasolType::Hashtype { .. }
            | ExasolType::Char { .. }
            | ExasolType::Varchar { .. } => false,
        };
        for kind in [AggKind::Sum, AggKind::Stddev] {
            assert_eq!(
                validate_agg_col_types(&plans(kind.clone()), &column_types(&col_types)),
                expected,
                "{kind:?} over {exa_type}"
            );
        }
        assert!(
            validate_agg_col_types(&plans(AggKind::Min), &column_types(&col_types)),
            "MIN over {exa_type} needs no numeric type"
        );
    }
}

/// Scenario: A pushdown request with a missing or invalid Exasol type fails the query
#[test]
fn unprojected_group_key_with_an_invalid_cast_type_fails() {
    let invalid = serde_json::json!({"type": "CHAR", "characterSet": "ASCII"});
    // The engine's `cast_to` takes an `ExasolType`, which cannot hold this invalid type.
    let key = cast_json(invalid.clone(), col("NAME"));
    let req = grouped_request(NO_COLUMNS, vec![key])
        .count_star()
        .build_pushdown();
    let unprojected = [GroupedSelectItem::Aggregate {
        plan_slot: 0,
        select_index: 0,
    }];

    match group_key_exasol_types(&req, &[r#""NAME""#.to_string()], &unprojected) {
        Err(UdfError::User(message)) => {
            assert!(message.contains(&invalid.to_string()), "{message}")
        }
        other => panic!("the invalid key type must fail the request, got {other:?}"),
    }
}
