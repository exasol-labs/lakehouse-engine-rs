use super::*;
use crate::scan::emit::declared_columns_test_support::{declared, numeric, varchar};
use crate::scan::spec::AggKind;
use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use exasol_udf_sdk::value::ExaType;
use std::sync::Arc;

/// Also reached by `grouped_agg_tests` and `scan_surface_probe` via `crate::scan::`.
pub fn build_partial_agg_sql(aggregates: &[AggregatePlan], aliased_table: &str) -> String {
    build_partial_agg_sql_filtered(aggregates, aliased_table, None)
}

/// Same order as the `dispatch_golden` all-agg-kinds fixtures; the mixed arities (1, 1, 1, 1, 1,
/// 2, then 3 × 4) exercise plan-ordinal versus column-ordinal naming.
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

/// Scenario: single-group partial-aggregate SQL over every AggKind matches the golden byte for byte.
#[test]
fn partial_agg_sql_all_agg_kinds_matches_golden() {
    let actual = build_partial_agg_sql(&all_agg_kinds_plans(), "aliased");
    let expected = include_str!("testdata/partial_agg_golden/partial_agg_all_agg_kinds.sql");
    assert_eq!(actual, expected);
}

/// Scenario: grouped partial-aggregate SQL over every AggKind matches the golden byte for byte.
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

/// Scenario: COUNT(*), SUM, MIN, MAX each yield one column in order.
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

/// Scenario: a COUNT(col) plan uses COUNT("COL"), not COUNT(*).
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

/// Scenario: a SUM plan uses SUM("COL") at index 1.
#[test]
fn partial_agg_sql_sum_uses_sum_col() {
    let sql = build_partial_agg_sql(&sample_plans_count_sum_min_max(), "aliased");
    assert!(
        sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_1""#),
        "SUM plan must use SUM(\"AMOUNT\") as PARTIAL_sum_1: {sql}"
    );
}

/// Scenario: MIN/MAX plans use MIN/MAX("COL").
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

/// Scenario: an AVG plan emits two columns, sum then count.
#[test]
fn partial_agg_sql_avg_emits_sum_count_pair() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Avg,
        column: Some("SCORE".into()),
        arg_expr: None,
    }];
    let sql = build_partial_agg_sql(&plans, "aliased");
    assert!(
        !sql.contains("AVG("),
        "must not use AVG() for partial avg: {sql}"
    );
    assert!(
        sql.contains(r#"SUM("SCORE") AS "PARTIAL_avg_sum_0""#),
        "AVG plan must emit SUM as PARTIAL_avg_sum_0: {sql}"
    );
    assert!(
        sql.contains(r#"COUNT("SCORE") AS "PARTIAL_avg_cnt_0""#),
        "AVG plan must emit COUNT(col) as PARTIAL_avg_cnt_0: {sql}"
    );
}

/// Scenario: each plan item is indexed by its position in the aggregates vec, AVG's pair sharing one index.
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
    assert!(sql.contains("PARTIAL_count_0"), "count at index 0: {sql}");
    assert!(sql.contains("PARTIAL_sum_1"), "sum at index 1: {sql}");
    assert!(
        sql.contains("PARTIAL_avg_sum_2"),
        "avg sum at index 2: {sql}"
    );
    assert!(
        sql.contains("PARTIAL_avg_cnt_2"),
        "avg cnt at index 2: {sql}"
    );
}

/// Scenario: a present filter is applied.
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

/// Scenario: no filter yields no WHERE clause.
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

/// Scenario: a rendered expression argument is substituted verbatim while a bare column stays quoted.
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
        AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        },
    ];
    let sql = build_partial_agg_sql(&plans, "aliased");

    assert!(
        sql.contains(r#"SUM(LENGTH("L_COMMENT")) AS "PARTIAL_sum_0""#),
        "SUM over an expression must render the expression verbatim: {sql}"
    );
    assert!(
        !sql.contains(r#"SUM("LENGTH("#),
        "expression argument must not be re-quoted as an identifier: {sql}"
    );
    assert!(
        sql.contains(r#"SUM(("A" + "B")) AS "PARTIAL_avg_sum_1""#)
            && sql.contains(r#"COUNT(("A" + "B")) AS "PARTIAL_avg_cnt_1""#),
        "AVG over an expression must decompose over the rendered fragment: {sql}"
    );
    assert!(
        sql.contains(r#"SUM("AMOUNT") AS "PARTIAL_sum_2""#),
        "bare-column aggregate must remain quoted-identifier: {sql}"
    );
}

/// Scenario: a single group key with COUNT(*) appears in the SELECT.
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

/// Scenario: group keys precede partial aggregate columns in the SELECT list.
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

/// Scenario: no LIMIT is ever added to a grouped partial aggregate.
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

/// Scenario: expression group keys are inserted verbatim into SELECT and GROUP BY.
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
    let first_pos = sql.find(&expr_key).unwrap();
    let second_pos = sql[first_pos + 1..]
        .find(&expr_key)
        .map(|p| p + first_pos + 1);
    assert!(
        second_pos.is_some(),
        "expression key must appear in both SELECT and GROUP BY: {sql}"
    );
}

/// Scenario: a stat aggregate emits COUNT(col), SUM(col), SUM(col*col) at index 0.
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

/// Scenario: a stat aggregate's null fallback row is cnt=0 (as `Value::Numeric`), sum=NULL, sumsq=NULL.
#[test]
fn stat_aggregate_null_fallback_row_has_three_values() {
    use exasol_udf_sdk::value::{Decimal, Value};
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
        let row = emit_null_partial_row(
            &plans,
            &declared(&[
                ("PARTIAL_statcnt_0", numeric(20, 0)),
                ("PARTIAL_statsum_0", ExaType::Double),
                ("PARTIAL_statsumsq_0", ExaType::Double),
            ]),
        )
        .expect("the stat fallback row must build against its declared columns");
        assert_eq!(row.len(), 3, "{kind:?} fallback row must have 3 values");
        assert_eq!(
            row[0],
            Value::Numeric(Decimal {
                unscaled: 0,
                scale: 0
            }),
            "{kind:?} cnt must be 0 at its declared DECIMAL(20,0)"
        );
        assert_eq!(row[1], Value::Null, "{kind:?} sum must be NULL");
        assert_eq!(row[2], Value::Null, "{kind:?} sumsq must be NULL");
    }
}

/// Scenario: an empty shard's fallback row carries each counter's zero at its declared variant, NULL elsewhere.
#[test]
fn null_partial_row_conforms_to_declared_output_columns() {
    use exasol_udf_sdk::value::{Decimal, Value};

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

    let row = emit_null_partial_row(
        &plans,
        &declared(&[
            ("PARTIAL_cnt_0", numeric(20, 0)),
            ("PARTIAL_sum_1", ExaType::Double),
        ]),
    )
    .expect("an empty shard must build its row against the declared columns");

    assert_eq!(
        row,
        vec![
            Value::Numeric(Decimal {
                unscaled: 0,
                scale: 0
            }),
            Value::Null
        ],
        "a DECIMAL(20,0)-declared counter must contribute Value::Numeric(0), not Value::Int64(0)"
    );
}

/// Scenario: a declared list not covering the fallback row fails naming both counts.
#[test]
fn null_partial_row_fails_when_the_declared_list_is_short() {
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

    let err = emit_null_partial_row(&plans, &declared(&[("PARTIAL_cnt_0", numeric(20, 0))]))
        .expect_err("a declared list shorter than the row must fail the call");
    let text = err.to_string();
    assert!(
        text.contains('1') && text.contains('2'),
        "the error must name both counts: {text}"
    );
}

/// Scenario: a counter declared a type no row count can inhabit fails naming the column.
#[test]
fn null_partial_row_fails_when_a_counter_is_declared_non_numeric() {
    let plans = vec![AggregatePlan {
        kind: AggKind::Count,
        column: None,
        arg_expr: None,
    }];

    let err = emit_null_partial_row(&plans, &declared(&[("BAD_COUNTER", ExaType::Date)]))
        .expect_err("a counter declared DATE must fail the call");
    assert!(
        err.to_string().contains("BAD_COUNTER"),
        "the error must name the offending counter column: {err}"
    );
}

/// Scenario: a stat aggregate at index 1 uses PARTIAL_stat_*_1 names.
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

/// Scenario: ResourcesExhausted on the partial-aggregate paths surfaces as memory exhaustion without credentials.
#[test]
fn resources_exhausted_on_partial_aggregate_path_surfaces_as_memory_error() {
    use crate::scan::emit::classify_scan_error;
    use datafusion::error::DataFusionError;

    let secret = "my-secret-key-value";
    let secrets = [secret];

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

    // DataFusion's sort wraps with .context().
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

fn decimal_column(values: Vec<i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .expect("decimal column"),
    )
}

fn one_row_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, col)| Field::new(*name, col.data_type().clone(), true))
        .collect();
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, col)| col).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("partial batch")
}

/// Scenario: each partial-aggregate cell is coerced to its declared column's Arrow type.
#[test]
fn partial_cells_conform_to_declared_output_columns() {
    use exasol_udf_sdk::value::Value;

    // AVG over an Iceberg long: SUM stays Int64 under DOUBLE PRECISION.
    let avg_long = one_row_batch(vec![
        ("PARTIAL_avg_cnt_0", Arc::new(Int64Array::from(vec![7i64]))),
        ("PARTIAL_avg_sum_0", Arc::new(Int64Array::from(vec![42i64]))),
    ]);
    let row = partial_row_from_batch(
        &[AggregatePlan {
            kind: AggKind::Avg,
            column: Some("QTY".into()),
            arg_expr: None,
        }],
        &avg_long,
        &declared(&[
            ("PARTIAL_avg_cnt_0", numeric(20, 0)),
            ("PARTIAL_avg_sum_0", ExaType::Double),
        ]),
    )
    .expect("AVG over a long must conform to its declared columns");
    assert_eq!(
        row,
        vec![
            Value::Numeric(exasol_udf_sdk::value::Decimal {
                unscaled: 7,
                scale: 0
            }),
            Value::Double(42.0)
        ],
        "an Int64 SUM must reach a DOUBLE PRECISION column as Value::Double"
    );

    let avg_decimal = one_row_batch(vec![
        ("PARTIAL_avg_cnt_0", Arc::new(Int64Array::from(vec![4i64]))),
        ("PARTIAL_avg_sum_0", decimal_column(vec![12_550], 10, 2)),
    ]);
    let row = partial_row_from_batch(
        &[AggregatePlan {
            kind: AggKind::Avg,
            column: Some("PRICE".into()),
            arg_expr: None,
        }],
        &avg_decimal,
        &declared(&[
            ("PARTIAL_avg_cnt_0", numeric(20, 0)),
            ("PARTIAL_avg_sum_0", ExaType::Double),
        ]),
    )
    .expect("AVG over a decimal must conform to its declared columns");
    assert_eq!(
        row[1],
        Value::Double(125.50),
        "a Decimal128 SUM must reach a DOUBLE PRECISION column as Value::Double"
    );

    // DataFusion's sum type exceeds DECIMAL(36,s); arrow_value_at would stringify it.
    let wide_sum = one_row_batch(vec![(
        "PARTIAL_sum_0",
        decimal_column(vec![123_456_789_012_345], 38, 2),
    )]);
    let row = partial_row_from_batch(
        &[AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }],
        &wide_sum,
        &declared(&[("PARTIAL_sum_0", numeric(36, 2))]),
    )
    .expect("a wide-decimal SUM must conform to its declared column");
    assert_eq!(
        row[0],
        Value::Numeric(exasol_udf_sdk::value::Decimal {
            unscaled: 123_456_789_012_345,
            scale: 2
        }),
        "a Decimal128(38,2) SUM must reach DECIMAL(36,2) as Value::Numeric, never a string"
    );

    // MIN/MAX over decimal(p≤18,0): Exasol binned the column to Int64.
    let minmax = one_row_batch(vec![
        ("PARTIAL_min_0", decimal_column(vec![11], 18, 0)),
        ("PARTIAL_max_0", decimal_column(vec![99], 18, 0)),
    ]);
    let row = partial_row_from_batch(
        &[
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("N".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("N".into()),
                arg_expr: None,
            },
        ],
        &minmax,
        &declared(&[
            ("PARTIAL_min_0", ExaType::Int64),
            ("PARTIAL_max_0", ExaType::Int64),
        ]),
    )
    .expect("MIN/MAX over a narrow decimal must conform to its declared columns");
    assert_eq!(
        row,
        vec![Value::Int64(11), Value::Int64(99)],
        "a Decimal128(18,0) extremum must reach an Int64-binned column as Value::Int64"
    );

    // NESTED_AGGREGATE_PLAN_TYPE declares DOUBLE PRECISION; DataFusion yields Decimal128.
    let nested = one_row_batch(vec![("PARTIAL_sum_0", decimal_column(vec![2_500], 20, 4))]);
    let row = partial_row_from_batch(
        &[AggregatePlan {
            kind: AggKind::Sum,
            column: None,
            arg_expr: Some(r#""DEC_A" * "DEC_B""#.into()),
        }],
        &nested,
        &declared(&[("PARTIAL_sum_0", ExaType::Double)]),
    )
    .expect("a nested-only aggregate must conform to its declared column");
    assert_eq!(
        row[0],
        Value::Double(0.25),
        "a Decimal128 nested aggregate must reach DOUBLE PRECISION as Value::Double"
    );
}

/// Scenario: a MIN/MAX cell over TIMESTAMP(9) keeps all nine digits in its `Value::Timestamp`.
#[test]
fn partial_agg_minmax_over_a_nanosecond_timestamp_keeps_every_digit() {
    use arrow::array::TimestampNanosecondArray;

    let lowest = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_nano_opt(0, 0, 0, 123_456_789)
        .unwrap();
    let highest = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_nano_opt(23, 59, 59, 987_654_321)
        .unwrap();
    let cell = |instant: chrono::NaiveDateTime| -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(vec![Some(
            instant.and_utc().timestamp_nanos_opt().unwrap(),
        )]))
    };
    let batch = one_row_batch(vec![
        ("PARTIAL_min_0", cell(lowest)),
        ("PARTIAL_max_0", cell(highest)),
    ]);

    let row = partial_row_from_batch(
        &[
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("TS_NS".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("TS_NS".into()),
                arg_expr: None,
            },
        ],
        &batch,
        &declared(&[
            ("PARTIAL_min_0", ExaType::Timestamp { precision: 9 }),
            ("PARTIAL_max_0", ExaType::Timestamp { precision: 9 }),
        ]),
    )
    .expect("MIN/MAX over a nanosecond timestamp must conform to its declared columns");

    assert_eq!(
        row,
        vec![Value::Timestamp(lowest), Value::Timestamp(highest)],
        "a nanosecond extremum must keep all nine digits under a TIMESTAMP(9) declaration"
    );
}

/// Scenario: an out-of-range partial-aggregate `Numeric` column fails naming it.
#[test]
fn partial_agg_fails_on_numeric_with_out_of_range_payload() {
    let drifted = vec![
        ("precision above Decimal128", numeric(39, 0)),
        ("scale above precision", numeric(10, 12)),
    ];

    for (label, typ) in drifted {
        let batch = one_row_batch(vec![("PARTIAL_sum_0", decimal_column(vec![1], 10, 0))]);
        let err = partial_row_from_batch(
            &[AggregatePlan {
                kind: AggKind::Sum,
                column: Some("N".into()),
                arg_expr: None,
            }],
            &batch,
            &declared(&[("OFFENDING_PARTIAL", typ)]),
        )
        .expect_err("a drifted NUMERIC declaration must fail the call");
        assert!(
            err.to_string().contains("OFFENDING_PARTIAL"),
            "{label}: the error must name the offending column: {err}"
        );
    }
}

/// Scenario: a partial-aggregate value its declared column cannot represent fails naming the column.
#[test]
fn partial_agg_fails_when_a_value_does_not_fit_its_declared_target() {
    // 10^36 needs 37 digits: one more than DECIMAL(36,2) holds.
    let too_wide: i128 = 10i128.pow(36);
    let batch = one_row_batch(vec![(
        "PARTIAL_sum_0",
        decimal_column(vec![too_wide], 38, 2),
    )]);

    let err = partial_row_from_batch(
        &[AggregatePlan {
            kind: AggKind::Sum,
            column: Some("AMOUNT".into()),
            arg_expr: None,
        }],
        &batch,
        &declared(&[("WIDE_PARTIAL", numeric(36, 2))]),
    )
    .expect_err("a SUM wider than its declared DECIMAL(36,2) must fail the call");
    assert!(
        err.to_string().contains("WIDE_PARTIAL"),
        "the error must name the column that could not be coerced: {err}"
    );
}

/// Scenario: the grouped path coerces only the partial-aggregate columns, never group keys.
#[test]
fn grouped_coercion_leaves_the_group_key_columns_untouched() {
    use arrow::array::Date32Array;

    let batch = one_row_batch(vec![
        ("GK_0", Arc::new(Date32Array::from(vec![19_000i32]))),
        ("PARTIAL_sum_0", decimal_column(vec![5], 10, 0)),
    ]);
    let columns = coerce_partial_agg_columns(
        &batch,
        &declared(&[("GK_0", varchar()), ("PARTIAL_sum_0", ExaType::Int64)]),
        1,
    )
    .expect("the grouped coercion must succeed");

    assert_eq!(
        columns[0].data_type(),
        &DataType::Date32,
        "the group-key column must keep its source type, uncoerced"
    );
    assert_eq!(
        columns[1].data_type(),
        &DataType::Int64,
        "the partial-aggregate column must be coerced to its declared target"
    );
}

/// Scenario: a declared list not covering the produced partial row fails the call.
#[test]
fn partial_agg_fails_when_the_declared_column_count_disagrees() {
    let batch = one_row_batch(vec![
        ("PARTIAL_min_0", decimal_column(vec![1], 10, 0)),
        ("PARTIAL_max_0", decimal_column(vec![2], 10, 0)),
    ]);
    let err = partial_row_from_batch(
        &[
            AggregatePlan {
                kind: AggKind::Min,
                column: Some("N".into()),
                arg_expr: None,
            },
            AggregatePlan {
                kind: AggKind::Max,
                column: Some("N".into()),
                arg_expr: None,
            },
        ],
        &batch,
        &declared(&[("PARTIAL_min_0", ExaType::Int64)]),
    )
    .expect_err("a short declaration must fail the call");
    assert!(
        err.to_string().contains('1') && err.to_string().contains('2'),
        "the error must name both counts: {err}"
    );
}
