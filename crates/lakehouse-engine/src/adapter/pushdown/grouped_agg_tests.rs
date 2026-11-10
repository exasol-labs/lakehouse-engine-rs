use super::super::detect_aggregates;
use super::super::joins::{
    FanOutProjection, build_qualified_single_table_fallback_sql, referenced_columns,
};
use super::super::scalar_over_agg::merge_select_items;
use super::super::support::{
    DISTRIBUTE_FILES_UDF_NAME, SCAN_UDF_NAME, involved_columns, shard_count,
};
use super::super::test_support::*;
use super::*;
use crate::scan::spec::{CommonScanSpec, ScanStorage};
use crate::scan::test_support::WithFiles;
use vs_expression::SqlFragment;

#[test]
fn scalar_over_merge_casts_to_exasol_char_target() {
    let sum_node = agg_item("SUM", Some("x"), false);
    let plans =
        vec![parse_agg_item(&sum_node, untyped_scan()).expect("SUM(x) must parse to a plan")];
    let node = cast_to(
        ExasolType::Char {
            size: 20,
            ascii: true,
        },
        sum_node,
    );
    let sql = render_scalar_over_merge(&node, &plans, &merge_select_items(&plans), untyped_scan())
        .expect("CAST over a mergeable aggregate must render")
        .sql;
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
    let plans =
        vec![parse_agg_item(&sum_node, untyped_scan()).expect("SUM(x) must parse to a plan")];
    let char_type = ExasolType::Char {
        size: 20,
        ascii: true,
    };
    let node = cast_to(char_type, cast_to(char_type, sum_node));
    let sql = render_scalar_over_merge(&node, &plans, &merge_select_items(&plans), untyped_scan())
        .expect("a nested CAST over a mergeable aggregate must render")
        .sql;
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
        detect_group_by_aggregates(&req, untyped_scan()).is_none(),
        "grouped COUNT(DISTINCT) must still decline (row-scan fallback)"
    );
    assert!(
        detect_aggregates(&req, untyped_scan()).is_none(),
        "the single-group path rejects any request carrying a non-empty GROUP BY"
    );
}

#[test]
fn partial_emits_min_max_preserve_date_timestamp_type() {
    let plans = vec![
        column_plan(AggKind::Min, "EVENT_DATE"),
        column_plan(AggKind::Max, "EVENT_TS"),
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
    let plans = vec![column_plan(AggKind::Sum, "AMOUNT")];
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
    let plans = vec![column_plan(AggKind::Sum, "SCORE")];
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
    let sum_varchar = vec![column_plan(AggKind::Sum, "NAME")];
    assert!(
        !validate_agg_col_types(&sum_varchar, &column_types(&col_types_varchar)),
        "SUM over VARCHAR must fail validation (fall back to row scan)"
    );

    let col_types_date = vec![("EVENT_DATE".to_string(), ExasolType::Date)];
    let sum_date = vec![column_plan(AggKind::Sum, "EVENT_DATE")];
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

    let detected = detect_group_by_aggregates(&req, untyped_scan());
    assert!(
        detected.is_some(),
        "detect_group_by_aggregates must accept the shape: {}",
        req.json()
    );
    let agg_plans = detected.unwrap().aggregates.plans;

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
    let result =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    let keys = result.group_keys;
    let plans = result.aggregates.plans;
    assert_eq!(keys.len(), 1, "one group key");
    assert!(
        keys[0].contains("REGION"),
        "group key must reference REGION: {:?}",
        keys[0]
    );
    assert_eq!(plans.len(), 1, "one aggregate plan");
    assert_eq!(plans[0].kind, AggKind::Count);
}

/// The merge SQL of a grouped aggregate, built as the dispatcher builds it: one shard, and no
/// LIMIT, OFFSET, HAVING, or ORDER BY until a test sets one.
struct GroupedSql {
    spec: ScanSpec,
    shards: Vec<Vec<FileEntry>>,
    group_key_types: Vec<ExasolType>,
    aggregates: TypedPlans,
    select_items: Vec<GroupedSelectItem>,
    select_types: Vec<ExasolType>,
    columns: ColumnTypes,
    limit: Option<u64>,
    offset: u64,
    having: Option<String>,
    order_by: Option<String>,
}

impl GroupedSql {
    /// Over `detection`, with no column type known to the merge render.
    fn of(detection: &GroupedAggregateDetection, req: &PushdownRequest) -> Self {
        Self::with(
            detection.group_keys.clone(),
            detection.group_key_types.clone(),
            detection.aggregates.clone(),
            detection.select_items.clone(),
            req.select_types().to_vec(),
        )
    }

    /// Keys first, then one aggregate per plan, each output in its own type. The merge render
    /// knows `AMOUNT` and `SCORE` as `DOUBLE PRECISION`.
    fn keys_first(keys: &[(&str, ExasolType)], plans: Vec<(AggregatePlan, ExasolType)>) -> Self {
        let columns = column_types(&[
            ("AMOUNT", ExasolType::DoublePrecision),
            ("SCORE", ExasolType::DoublePrecision),
        ]);
        let (plans, types): (Vec<_>, Vec<_>) =
            plans.into_iter().map(|(plan, ty)| (plan, Some(ty))).unzip();
        let aggregates =
            TypedPlans::new(plans, types, &columns).expect("every aggregate has a type");
        let group_key_types: Vec<ExasolType> = keys.iter().map(|(_, ty)| *ty).collect();
        let select_items = keys_first_select_items(keys.len(), aggregates.plans.len());
        let select_types = select_types_of(&select_items, &group_key_types, &aggregates.types);
        Self {
            columns,
            ..Self::with(
                keys.iter().map(|(key, _)| key.to_string()).collect(),
                group_key_types,
                aggregates,
                select_items,
                select_types,
            )
        }
    }

    fn with(
        group_keys: Vec<String>,
        group_key_types: Vec<ExasolType>,
        aggregates: TypedPlans,
        select_items: Vec<GroupedSelectItem>,
        select_types: Vec<ExasolType>,
    ) -> Self {
        let spec = CommonScanSpec {
            aggregates: Some(aggregates.plans.clone()),
            group_keys: Some(group_keys),
            storage: ScanStorage::Inline(sample_storage()),
            ..Default::default()
        }
        .with_files(vec![]);
        Self {
            spec,
            shards: vec![vec![FileEntry::new("s3://wh/f0.parquet", 1)]],
            group_key_types,
            aggregates,
            select_items,
            select_types,
            columns: ColumnTypes::default(),
            limit: None,
            offset: 0,
            having: None,
            order_by: None,
        }
    }

    /// Reorders the select list; each item keeps its own type.
    fn select_items(mut self, select_items: Vec<GroupedSelectItem>) -> Self {
        self.select_types =
            select_types_of(&select_items, &self.group_key_types, &self.aggregates.types);
        self.select_items = select_items;
        self
    }

    fn columns(mut self, columns: ColumnTypes) -> Self {
        self.columns = columns;
        self
    }

    /// `files` of one byte each, partitioned into `g` shards.
    fn files(mut self, files: Vec<String>, g: usize) -> Self {
        let entries = files
            .into_iter()
            .map(|path| FileEntry::new(path, 1))
            .collect();
        self.shards = crate::adapter::sharding::partition_files_by_bytes(entries, g);
        self
    }

    fn shards(mut self, shards: Vec<Vec<FileEntry>>) -> Self {
        self.shards = shards;
        self
    }

    /// A LIMIT on the scan template, which the per-shard partial must never carry.
    fn template_limit(mut self, limit: u64) -> Self {
        self.spec.common.limit = Some(limit);
        self
    }

    fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    fn having(mut self, having: &str) -> Self {
        self.having = Some(having.to_string());
        self
    }

    fn order_by(mut self, order_by: &str) -> Self {
        self.order_by = Some(order_by.to_string());
        self
    }

    fn build(&self) -> String {
        build_grouped_aggregate_scan_sql(
            &self.spec,
            &self.shards,
            &self.group_key_types,
            &self.aggregates,
            &self.select_items,
            &self.select_types,
            self.limit,
            self.offset,
            ScanTranslator::new(&self.columns),
            SCAN_UDF_NAME,
            DISTRIBUTE_FILES_UDF_NAME,
            self.having.as_deref(),
            self.order_by.as_deref(),
        )
        .expect("the merge select list renders")
    }
}

/// The grouped ORDER BY over `detection`'s merge, scoped as `classify_request_shape` scopes it.
fn order_by_over_merge(
    req: &PushdownRequest,
    detection: &GroupedAggregateDetection,
    col_types: &ColumnTypes,
) -> Option<GroupedOrderBy> {
    let scope = MergeScope {
        aggregates: &detection.aggregates,
        group_keys: &detection.group_keys,
    };
    build_grouped_order_by_clause(req.json(), &scope, ScanTranslator::new(col_types))
}

/// A HAVING over `detection`'s merge, scoped as `classify_request_shape` scopes it.
fn having_over_merge(
    node: &Json,
    detection: &GroupedAggregateDetection,
    col_types: &ColumnTypes,
) -> Option<String> {
    let scope = MergeScope {
        aggregates: &detection.aggregates,
        group_keys: &detection.group_keys,
    };
    render_having_over_merge(node, &scope, ScanTranslator::new(col_types))
}

/// A HAVING over `plans` alone, each merged by its uncast partial merge, with no group key.
fn having_over_plans(node: &Json, plans: &[AggregatePlan]) -> Option<String> {
    // The HAVING rewrite reads only the plans and their merges, never the types.
    let aggregates = TypedPlans {
        plans: plans.to_vec(),
        types: Vec::new(),
        merged: merge_select_items(plans),
    };
    let scope = MergeScope {
        aggregates: &aggregates,
        group_keys: &[],
    };
    render_having_over_merge(node, &scope, untyped_scan())
}

#[test]
fn grouped_order_by_no_limit_renders_explicit_merge_order_by() {
    let req = grouped_request(&[("ID", ExasolType::decimal(20, 0))], vec![col("ID")])
        .column(col("ID"))
        .item(count_star(), ExasolType::decimal(20, 0))
        .order_by(serde_json::json!([order_by_element(col("ID"), true, true)]))
        .build_pushdown();

    let result = detect_group_by_aggregates(&req, untyped_scan()).expect("grouped aggregate");
    assert_eq!(
        order_by_over_merge(&req, &result, &ColumnTypes::default()),
        Some(GroupedOrderBy::Clause(
            r#""GK_0" ASC NULLS LAST"#.to_string()
        )),
        "grouped ORDER BY must sort a group key on its typed GK_* column"
    );

    let sql = GroupedSql::of(&result, &req)
        .columns(column_types(&[("ID", ExasolType::decimal(20, 0))]))
        .order_by(r#""GK_0" ASC NULLS LAST"#)
        .build();
    assert!(
        sql.contains(r#" ORDER BY "GK_0" ASC NULLS LAST"#),
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

    let detection = detect_group_by_aggregates(&req, untyped_scan()).expect("grouped aggregate");
    assert_eq!(
        order_by_over_merge(&req, &detection, &ColumnTypes::default()),
        Some(GroupedOrderBy::Clause(
            r#"CAST(SUM("PARTIAL_sum_0") AS DECIMAL(36,2)) DESC NULLS LAST, "GK_0" ASC NULLS FIRST"#
                .to_string()
        )),
        "an aggregate sort key must render as its typed merged partial, a group key as its \
         GK_* column"
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

    let detection = detect_group_by_aggregates(&req, untyped_scan()).expect("grouped aggregate");
    assert_eq!(
        order_by_over_merge(&req, &detection, &ColumnTypes::default()),
        Some(GroupedOrderBy::Unresolvable),
        "an aggregate with no matching plan must not resolve to a fabricated partial"
    );
}

#[test]
fn detect_group_by_aggregates_expression_key() {
    let key = predicate_equal(col("STATUS"), str_lit("active"));
    let req = grouped_request(NO_COLUMNS, vec![key.clone()])
        .item(key, ExasolType::Boolean)
        .item(
            agg_item("SUM", Some("AMOUNT"), false),
            ExasolType::WIDEST_VARCHAR,
        )
        .build_pushdown();
    let col_types = column_types(&[
        ("STATUS", ExasolType::WIDEST_VARCHAR),
        ("AMOUNT", ExasolType::decimal(18, 2)),
    ]);
    let result = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types));
    assert!(result.is_some(), "renderable expression key must succeed");
    let GroupedAggregateDetection {
        group_keys: keys,
        aggregates,
        ..
    } = result.unwrap();
    assert_eq!(keys.len(), 1);
    assert!(keys[0].contains("="), "rendered expression must contain =");
    assert_eq!(aggregates.plans[0].kind, AggKind::Sum);
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
        detect_group_by_aggregates(&req, untyped_scan()).is_none(),
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
        detect_group_by_aggregates(&req, untyped_scan()).is_none(),
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
    let col_types = column_types(&[("ID", ExasolType::decimal(20, 0))]);
    let result = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types)).expect(
        "composed literal-only selectList must preserve GROUP BY, not fall back to row scan",
    );
    assert_eq!(result.group_keys.len(), 1, "one group key from groupBy");
    assert!(
        result.group_keys[0].contains("ID"),
        "group key must reference ID: {:?}",
        result.group_keys[0]
    );
    assert!(
        result.aggregates.plans.is_empty(),
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
    let sql = GroupedSql::of(&result, &req).columns(col_types).build();
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
    let result = detect_group_by_aggregates(&req, untyped_scan()).expect(
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
    let result =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    assert_eq!(result.group_keys.len(), 1, "one group key");
    assert_eq!(result.aggregates.plans.len(), 1, "one aggregate plan");
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
    let result =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    assert_eq!(result.group_keys.len(), 2, "two group keys");
    assert_eq!(result.aggregates.plans.len(), 1, "one aggregate plan");
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
    let result =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
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
    let result =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
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
    let col_types = column_types(&[
        ("ID", ExasolType::decimal(18, 0)),
        ("NAME", ExasolType::WIDEST_VARCHAR),
    ]);
    let result = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("all-expression multi-key must detect");
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
    assert_eq!(result.aggregates.plans.len(), 1, "one aggregate plan");
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

    let sql = GroupedSql::of(&result, &req).build();
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
        detect_group_by_aggregates(&bad_req, untyped_scan()).is_none(),
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

/// Keys output as `VARCHAR(2000000)`, aggregates as `DOUBLE PRECISION`, over `files` in `g` shards.
fn build_grouped_agg_sql(
    group_keys: &[&str],
    agg_plans: Vec<AggregatePlan>,
    files: Vec<String>,
    g: usize,
) -> String {
    let keys: Vec<(&str, ExasolType)> = group_keys
        .iter()
        .map(|key| (*key, ExasolType::WIDEST_VARCHAR))
        .collect();
    let plans = agg_plans
        .into_iter()
        .map(|plan| (plan, ExasolType::DoublePrecision))
        .collect();
    GroupedSql::keys_first(&keys, plans).files(files, g).build()
}

#[test]
fn grouped_fan_out_common_once_files_per_shard() {
    let files: Vec<String> = (0..2).map(|i| format!("s3://w/f{i}.parquet")).collect();
    let g = shard_count(2, 1, files.len());
    let sql = build_grouped_agg_sql(&[r#""REGION""#], vec![count_plan()], files, g);
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
    let sql = build_grouped_agg_sql(&[r#""REGION""#], vec![count_plan()], files, g);

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
        &[r#""REGION""#],
        vec![count_plan()],
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

/// `SELECT "REGION", COUNT(*) ... GROUP BY "REGION"`.
fn region_count_sql() -> GroupedSql {
    GroupedSql::keys_first(
        &[(r#""REGION""#, ExasolType::WIDEST_VARCHAR)],
        vec![(count_plan(), ExasolType::decimal(18, 0))],
    )
}

#[test]
fn grouped_common_blob_has_no_limit() {
    let sql = region_count_sql().template_limit(100).limit(100).build();
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
    let sql = region_count_sql()
        .template_limit(100)
        .limit(100)
        .offset(3)
        .order_by("1 ASC NULLS LAST")
        .build();
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
    let sql = region_count_sql().template_limit(100).limit(100).build();
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

    let result = detect_group_by_aggregates(&req, untyped_scan()).expect("grouped aggregate");
    let sql = GroupedSql::of(&result, &req)
        .columns(column_types(&[("ID", ExasolType::decimal(20, 0))]))
        .limit(2)
        .offset(1)
        .order_by("1 ASC NULLS LAST")
        .build();
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
        &[r#""REGION""#, r#""YEAR""#],
        vec![count_plan(), column_plan(AggKind::Sum, "AMOUNT")],
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
    let keys: Vec<(&str, ExasolType)> = group_keys
        .iter()
        .map(String::as_str)
        .zip(group_key_types)
        .collect();
    let sql = GroupedSql::keys_first(&keys, agg_plans.into_iter().zip(aggregate_types).collect())
        .select_items(select_items);
    match having {
        Some(having) => sql.having(having),
        None => sql,
    }
    .build()
}

#[test]
fn grouped_wrapper_agg_before_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![column_plan(AggKind::Sum, "SCORE")],
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
    assert_eq!(
        items[1], "\"GK_0\"",
        "position 1 must be the typed group key, uncast: {items:?}"
    );
}

#[test]
fn grouped_wrapper_interleaved_multi_key_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#""REGION""#.to_string(), r#""YEAR""#.to_string()],
        vec![ExasolType::varchar(100), ExasolType::decimal(4, 0)],
        vec![column_plan(AggKind::Sum, "SCORE")],
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
    assert_eq!(
        items[0], "\"GK_0\"",
        "position 0 must be key0, uncast: {items:?}"
    );
    assert!(
        items[1].contains("PARTIAL_sum_0") && items[1].starts_with("CAST(SUM("),
        "position 1 must be the merged aggregate: {items:?}"
    );
    assert_eq!(
        items[2], "\"GK_1\"",
        "position 2 must be key1, uncast: {items:?}"
    );
}

#[test]
fn grouped_wrapper_expr_key_after_agg_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![count_plan()],
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
    assert_eq!(
        items[1], "\"GK_0\"",
        "position 1 must be the typed, uncast group key, not a VARCHAR fallback: {items:?}"
    );
}

#[test]
fn grouped_wrapper_agg_first_with_having_ordering() {
    let sql = build_grouped_agg_sql_with_select_items(
        vec![r#"("ID" % 4)"#.to_string()],
        vec![ExasolType::decimal(9, 0)],
        vec![column_plan(AggKind::Sum, "SCORE")],
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
    assert_eq!(
        items[1], "\"GK_0\"",
        "position 1 must be the typed, uncast group key even with HAVING present: {items:?}"
    );
}

fn case_flag_eq(column: &str, val: &str) -> serde_json::Value {
    case_when(
        [predicate_equal(col(column), str_lit(val))],
        [num_lit(1), num_lit(0)],
    )
}

fn round_pct_over_aggregates() -> serde_json::Value {
    let returned = agg_item_expr(
        "SUM",
        cast_to(ExasolType::decimal(1, 0), case_flag_eq("L_RETURNFLAG", "R")),
        false,
    );
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
    let columns = column_types(&soa_col_types());
    let d = detect_group_by_aggregates(req, ScanTranslator::new(&columns))
        .expect("must detect the grouped scalar-over-aggregate pushdown");
    GroupedSql::of(&d, req).columns(columns).build()
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
    let d = detect_group_by_aggregates(&req, ScanTranslator::new(&column_types(&soa_col_types())))
        .expect("must detect grouped scalar-over-aggregate");

    assert!(
        matches!(
            &d.select_items[3],
            GroupedSelectItem::ScalarOverAggregate {
                select_index: 3,
                ..
            }
        ),
        "item 3 must be a ScalarOverAggregate: {:?}",
        d.select_items[3]
    );

    assert_eq!(
        d.aggregates.plans.len(),
        4,
        "inner SUM(CASE) + COUNT(*) fold in; the two COUNT(*) dedup to one: {:?}",
        d.aggregates.plans
    );
    let count_plans = d
        .aggregates
        .plans
        .iter()
        .filter(|p| matches!(p.kind, AggKind::Count | AggKind::CountCol))
        .count();
    assert_eq!(
        count_plans, 1,
        "the shared COUNT(*) must be a single plan: {:?}",
        d.aggregates.plans
    );

    let count_slot = d
        .aggregates
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
    assert_eq!(
        items[1], "\"GK_0\"",
        "position 1 must be the typed, uncast group key at its own ordinal: {items:?}"
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
        detect_group_by_aggregates(&pushdown, untyped_scan()).is_none(),
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
    let plans = vec![column_plan(AggKind::Sum, "SCORE")];
    let rendered =
        having_over_plans(&having, &plans).expect("HAVING over a known aggregate must render");
    assert_eq!(
        rendered, r#"(SUM("PARTIAL_sum_0") > 250)"#,
        "HAVING must reference the merged partial, not the source column: {rendered}"
    );
    assert!(
        !rendered.contains(r#""SCORE""#) && !rendered.contains("SUM(\"SCORE\")"),
        "HAVING must NOT reference the source column SCORE: {rendered}"
    );
}

fn score_double() -> ColumnTypes {
    column_types(&[("SCORE", ExasolType::DoublePrecision)])
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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");

    let having_node = compare(
        "predicate_greater",
        agg_item("SUM", Some("SCORE"), false),
        num_lit(250),
    );
    let having = having_over_merge(&having_node, &detection, &ColumnTypes::default())
        .expect("HAVING must render over the merge decomposition");

    let sql = GroupedSql::of(&detection, &req)
        .columns(score_double())
        .having(&having)
        .build();
    let having_pos = sql.find("HAVING").expect("must contain HAVING");
    let having_clause = &sql[having_pos..];
    assert!(
        having_clause.contains(r#"CAST(SUM("PARTIAL_sum_0") AS DOUBLE PRECISION) > 250"#),
        "HAVING clause must use the typed merge expression: {having_clause}"
    );
    assert!(
        !having_clause.contains(r#""SCORE""#) && !having_clause.contains("SUM(\"SCORE\")"),
        "HAVING clause must NOT reference the source SCORE column: {having_clause}"
    );
}

#[test]
fn render_having_over_merge_declines_unknown_aggregate() {
    let having = compare("predicate_greater", count_star(), num_lit(10));
    let plans = vec![column_plan(AggKind::Sum, "SCORE")];
    assert!(
        having_over_plans(&having, &plans).is_none(),
        "HAVING over an aggregate absent from the plans must not render"
    );
}

#[test]
fn having_over_a_group_key_reads_its_gk_column() {
    let columns = [("REGION", ExasolType::varchar(100))];
    let col_types = column_types(&columns);
    let req = grouped_request(&columns, vec![col("REGION")])
        .column(col("REGION"))
        .count_star()
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("grouped aggregate");
    let having = predicate_equal(col("REGION"), str_lit("EU"));

    assert_eq!(
        having_over_merge(&having, &detection, &col_types),
        Some(r#"("GK_0" = 'EU')"#.to_string()),
        "the merge has no source column, so a group key operand reads GK_0 (#551)"
    );
}

#[test]
fn having_over_a_key_inside_a_scalar_with_an_aggregate_reads_both_merged() {
    let columns = [
        ("K", ExasolType::decimal(9, 0)),
        ("V", ExasolType::decimal(9, 0)),
    ];
    let col_types = column_types(&columns);
    let req = grouped_request(&columns, vec![col("K")])
        .column(col("K"))
        .item(
            agg_item("SUM", Some("V"), false),
            ExasolType::decimal(20, 0),
        )
        .build_pushdown();
    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("grouped aggregate");
    let having = compare(
        "predicate_greater",
        scalar_fn("ADD", [col("K"), agg_item("SUM", Some("V"), false)]),
        num_lit(10),
    );

    assert_eq!(
        having_over_merge(&having, &detection, &col_types),
        Some(r#"(("GK_0" + CAST(SUM("PARTIAL_sum_0") AS DECIMAL(20,0))) > 10)"#.to_string()),
        "HAVING k + SUM(v) > 10 reads GK_0 and the merged SUM (#551)"
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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    let sql = GroupedSql::of(&detection, &req)
        .columns(score_double())
        .build();

    let items = outer_select_items(&sql);
    assert_eq!(items.len(), 2, "outer SELECT must have 2 items: {items:?}");
    assert!(
        items[0].contains("PARTIAL_sum_0") && items[0].starts_with("CAST(SUM("),
        "position 0 must be the merged SUM (selectList order): {items:?}"
    );
    assert_eq!(
        items[1], "\"GK_0\"",
        "position 1 must be the typed group key, uncast: {items:?}"
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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");

    let having_node = compare(
        "predicate_greater",
        agg_item("SUM", Some("SCORE"), false),
        num_lit(100),
    );
    let having = having_over_merge(&having_node, &detection, &ColumnTypes::default())
        .expect("HAVING must render over the merge decomposition");

    let sql = GroupedSql::of(&detection, &req)
        .columns(score_double())
        .shards(vec![
            vec![FileEntry::new("s3://wh/f0.parquet", 1)],
            vec![FileEntry::new("s3://wh/f1.parquet", 1)],
        ])
        .limit(2)
        .having(&having)
        .build();

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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");

    let group_keys = vec![SqlFragment::new(r#"("id" % 4)"#, None)];
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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");
    assert_eq!(detection.group_keys.len(), 2, "two group keys");

    let types = detection.group_key_types.clone();

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
    let detection = detect_group_by_aggregates(&req, untyped_scan())
        .expect("equal-length CASE group key must be detected as a grouped aggregate");
    assert_eq!(detection.group_keys.len(), 1, "one group key");

    let types = detection.group_key_types.clone();

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
    let detection =
        detect_group_by_aggregates(&req, untyped_scan()).expect("must detect grouped aggregate");

    let types = detection.group_key_types.clone();

    assert_eq!(
        types,
        vec![ExasolType::varchar(10)],
        "a VARCHAR-declared group key must be unaffected: {types:?}"
    );
}

#[test]
fn a_cast_to_char_declines_the_split() {
    let columns = [
        ("NAME", ExasolType::WIDEST_VARCHAR),
        ("REGION", ExasolType::varchar(100)),
    ];
    let char_name = || cast_to(ExasolType::char(20), col("NAME"));
    let cases = [
        (
            "unprojected key",
            grouped_request(&columns, vec![char_name()]).count_star(),
        ),
        (
            "MIN argument",
            grouped_request(&columns, vec![col("REGION")])
                .column(col("REGION"))
                .item(
                    agg_item_expr("MIN", char_name(), false),
                    ExasolType::char(20),
                ),
        ),
    ];
    for (label, req) in cases {
        let detection = detect_group_by_aggregates(
            &req.build_pushdown(),
            ScanTranslator::new(&column_types(&columns)),
        );
        assert!(detection.is_none(), "{label}: the scan does not blank-pad");
    }
}

#[test]
fn an_unprojected_sized_varchar_key_travels_as_any_text() {
    let req = grouped_request(
        NO_COLUMNS,
        vec![cast_to(ExasolType::varchar(10), col("NAME"))],
    )
    .count_star()
    .build_pushdown();
    let col_types = column_types(&[("NAME", ExasolType::WIDEST_VARCHAR)]);
    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("an unprojected group key must still detect as a grouped aggregate");

    assert_eq!(
        detection.group_key_types,
        vec![ExasolType::WIDEST_VARCHAR],
        "the scan's unchecked cast emits any text, not VARCHAR(10)"
    );
}

#[test]
fn unprojected_computed_group_key_without_a_cast_declines_the_split() {
    let col_types = column_types(&[("ID", ExasolType::decimal(18, 0))]);
    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .count_star()
        .build_pushdown();

    assert!(
        detect_group_by_aggregates(&req, ScanTranslator::new(&col_types)).is_none(),
        "an unprojected MOD(ID, 4) key has no exact type to travel in (#476)"
    );
}

#[test]
fn unprojected_cast_group_key_keeps_its_cast_type() {
    let col_types = column_types(&[("ID", ExasolType::decimal(18, 0))]);
    let key = cast_to(ExasolType::decimal(9, 0), mod_item("ID", 4));
    let req = grouped_request(NO_COLUMNS, vec![key])
        .count_star()
        .build_pushdown();

    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("a CAST key states its exact type");

    assert_eq!(detection.group_key_types, vec![ExasolType::decimal(9, 0)]);
}

#[test]
fn group_key_exasol_types_prefers_select_list_type_over_group_by_type() {
    let key = || cast_to(ExasolType::varchar(20), col("NAME"));
    let req = grouped_request(NO_COLUMNS, vec![key()])
        .item(key(), ExasolType::varchar(30))
        .count_star()
        .build_pushdown();
    let col_types = column_types(&[("NAME", ExasolType::WIDEST_VARCHAR)]);
    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("must detect grouped aggregate");

    let types = detection.group_key_types.clone();

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
    let col_types = column_types(&[("REGION", ExasolType::varchar(100))]);
    let detection = detect_group_by_aggregates(&req, ScanTranslator::new(&col_types))
        .expect("must detect grouped aggregate");

    let projection = detection
        .select_items
        .iter()
        .find_map(|item| match item {
            GroupedSelectItem::Constant {
                projection,
                select_index,
            } => Some(projection.as_type(req.select_types()[*select_index])),
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
fn detect_group_by_aggregates_no_group_by_type_returns_none() {
    let req1 = pushdown_request("T", NO_COLUMNS)
        .field("groupBy", serde_json::json!([col("REGION")]))
        .count_star()
        .build_pushdown();
    assert!(detect_group_by_aggregates(&req1, untyped_scan()).is_none());

    let req2 = pushdown_request("T", NO_COLUMNS)
        .single_group()
        .count_star()
        .build_pushdown();
    assert!(detect_group_by_aggregates(&req2, untyped_scan()).is_none());
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
    assert!(detect_group_by_aggregates(&req, untyped_scan()).is_none());
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
        let plans = vec![column_plan(kind.clone(), "SCORE")];
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

    let plan_for = |kind: &AggKind| match kind {
        AggKind::Count => count_plan(),
        _ => column_plan(kind.clone(), "SCORE"),
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
    let plans = vec![column_plan(AggKind::VarPop, "X")];
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
    let plans = vec![column_plan(AggKind::VarSamp, "X")];
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
    let plans = vec![column_plan(AggKind::StddevPop, "X")];
    let sql = merge_select_items(&plans).join(", ");
    assert!(sql.contains("SQRT("), "stddev_pop must use SQRT: {sql}");
    assert!(
        !sql.contains("- 1"),
        "stddev_pop must not subtract 1: {sql}"
    );
}

#[test]
fn stddev_samp_merge_formula_uses_sqrt_and_n_minus_1() {
    let plans = vec![column_plan(AggKind::StddevSamp, "X")];
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
        let plans = vec![column_plan(kind.clone(), "X")];
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
    let sql = GroupedSql::keys_first(
        &[(r#""REGION""#, ExasolType::WIDEST_VARCHAR)],
        vec![(
            column_plan(AggKind::Sum, "AMOUNT"),
            ExasolType::DoublePrecision,
        )],
    )
    .having(r#"(SUM("AMOUNT") > 100)"#)
    .build();
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
        detect_group_by_aggregates(&req, untyped_scan()).is_none(),
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
        classify_scalar_over_aggregate(&sqrt_over_stat, untyped_scan()).is_none(),
        "a scalar wrapping a stat aggregate over an expression must not classify"
    );

    let req = grouped_request(NO_COLUMNS, vec![mod_item("ID", 4)])
        .item(mod_item("ID", 4), ExasolType::decimal(9, 0))
        .item(sqrt_over_stat, ExasolType::DoublePrecision)
        .build_pushdown();
    assert!(
        detect_group_by_aggregates(&req, untyped_scan()).is_none(),
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
        having_over_plans(&having, &plans).is_none(),
        "a HAVING over a stat aggregate with an expression argument must not \
             render over the merge wrapper"
    );
}

#[test]
fn sum_and_stat_aggregates_accept_only_double_and_decimal_columns() {
    let plans = |kind: AggKind| vec![column_plan(kind, "X")];
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

/// The types Exasol declares for a select list of only keys and aggregates: each one's own type.
fn select_types_of(
    items: &[GroupedSelectItem],
    key_types: &[ExasolType],
    plan_types: &[ExasolType],
) -> Vec<ExasolType> {
    let mut ordered = items.to_vec();
    ordered.sort_by_key(select_item_index);
    ordered
        .iter()
        .map(|item| match item {
            GroupedSelectItem::GroupKey { group_key_slot, .. } => key_types[*group_key_slot],
            GroupedSelectItem::Aggregate { plan_slot, .. } => plan_types[*plan_slot],
            other => panic!("only a key or an aggregate has its own type: {other:?}"),
        })
        .collect()
}
